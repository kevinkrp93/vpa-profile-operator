/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	"github.com/kevinkrp93/vpa-profile-operator/api"
	"github.com/kevinkrp93/vpa-profile-operator/controller"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/client-go/dynamic"
	dynamicinformer "k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/informers"
	coreinformers "k8s.io/client-go/informers/core/v1"
	kubeclient "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/flowcontrol"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
)

var (
	profileNamespace = flag.String("profile-configmap-namespace", "vpa-system", "Namespace containing the central profile ConfigMap")
	profileName      = flag.String("profile-configmap-name", "vpa-profiles", "Name of the central profile ConfigMap")
	profileKey       = flag.String("profile-configmap-key", "profiles.yaml", "ConfigMap data key containing the profile document")
)

func main() {
	klog.InitFlags(nil)
	flag.Parse()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := run(ctx); err != nil {
		klog.ErrorS(err, "Operator stopped with an error")
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	config, err := rest.InClusterConfig()
	if err != nil {
		config, err = clientcmd.BuildConfigFromFlags("", os.Getenv("KUBECONFIG"))
		if err != nil {
			return fmt.Errorf("load Kubernetes configuration: %w", err)
		}
	}
	config.RateLimiter = flowcontrol.NewTokenBucketRateLimiter(rest.DefaultQPS, rest.DefaultBurst)
	kube, err := kubeclient.NewForConfig(config)
	if err != nil {
		return fmt.Errorf("create Kubernetes client: %w", err)
	}
	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		return fmt.Errorf("create dynamic Kubernetes client: %w", err)
	}

	factory := informers.NewSharedInformerFactory(kube, 0)
	deployments := factory.Apps().V1().Deployments().Informer()
	statefulSets := factory.Apps().V1().StatefulSets().Informer()
	configMaps := newProfileConfigMapInformer(kube, *profileNamespace, *profileName)
	vpas := dynamicinformer.NewFilteredDynamicInformer(
		dynamicClient, controller.VPAResource(), metav1.NamespaceAll, 0, cache.Indexers{}, nil).Informer()
	queue := workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())
	defer queue.ShutDown()
	reconciler := &controller.Reconciler{
		DynamicClient: dynamicClient,
		ProfileNS:     *profileNamespace,
		ProfileName:   *profileName,
		ProfileKey:    *profileKey,
		Deployments:   deployments.GetStore(),
		StatefulSets:  statefulSets.GetStore(),
		ConfigMaps:    configMaps.GetStore(),
		VPAs:          vpas.GetStore(),
	}

	registerWorkloadHandlers(deployments, controller.DeploymentKind, queue)
	registerWorkloadHandlers(statefulSets, controller.StatefulSetKind, queue)
	registerVPAHandlers(vpas, queue)
	configMaps.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(object any) {
			if isProfileConfigMap(object, *profileNamespace, *profileName) {
				enqueueOptedIn(deployments, statefulSets, queue)
			}
		},
		UpdateFunc: func(oldObject, newObject any) {
			if profileConfigMapUpdated(oldObject, newObject, *profileNamespace, *profileName) {
				enqueueOptedIn(deployments, statefulSets, queue)
			}
		},
		DeleteFunc: func(object any) {
			if profileConfigMapDeleted(object, *profileNamespace, *profileName) {
				enqueueOptedIn(deployments, statefulSets, queue)
			}
		},
	})

	factory.Start(ctx.Done())
	go configMaps.Run(ctx.Done())
	go vpas.Run(ctx.Done())
	if !cache.WaitForCacheSync(ctx.Done(), deployments.HasSynced, statefulSets.HasSynced, configMaps.HasSynced, vpas.HasSynced) {
		if ctx.Err() != nil {
			return nil
		}
		return errors.New("informer caches failed to synchronize")
	}
	enqueueOptedIn(deployments, statefulSets, queue)

	var workers sync.WaitGroup
	workers.Add(1)
	go func() {
		defer workers.Done()
		runWorker(ctx, queue, reconciler)
	}()
	<-ctx.Done()
	queue.ShutDown()
	workers.Wait()
	return nil
}

func newProfileConfigMapInformer(kube kubeclient.Interface, namespace, name string) cache.SharedIndexInformer {
	return coreinformers.NewFilteredConfigMapInformer(kube, namespace, 0, cache.Indexers{},
		func(options *metav1.ListOptions) {
			options.FieldSelector = fields.OneTermEqualSelector("metadata.name", name).String()
		})
}

func registerWorkloadHandlers(informer cache.SharedIndexInformer, kind string, queue workqueue.RateLimitingInterface) {
	enqueue := func(object any) {
		key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(object)
		if err != nil {
			klog.ErrorS(err, "Could not get workload key", "kind", kind)
			return
		}
		namespace, name, found := strings.Cut(key, "/")
		if !found {
			klog.ErrorS(nil, "Malformed workload cache key", "key", key)
			return
		}
		queue.Add(controller.WorkloadKey(kind, namespace, name))
	}
	informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: enqueue,
		UpdateFunc: func(previous, current any) {
			if workloadChanged(previous, current) {
				enqueue(current)
			}
		},
		DeleteFunc: enqueue,
	})
}

func workloadChanged(previous, current any) bool {
	oldWorkload, oldOK := previous.(metav1.Object)
	newWorkload, newOK := current.(metav1.Object)
	return oldOK && newOK && (oldWorkload.GetUID() != newWorkload.GetUID() ||
		oldWorkload.GetGeneration() != newWorkload.GetGeneration() ||
		oldWorkload.GetAnnotations()[api.ProfileAnnotation] != newWorkload.GetAnnotations()[api.ProfileAnnotation] ||
		!apiequality.Semantic.DeepEqual(oldWorkload.GetDeletionTimestamp(), newWorkload.GetDeletionTimestamp()))
}

func registerVPAHandlers(informer cache.SharedIndexInformer, queue workqueue.RateLimitingInterface) {
	informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(object any) { enqueueManagedVPA(object, queue) },
		UpdateFunc: func(previous, current any) {
			if vpaChanged(previous, current) {
				enqueueManagedVPA(previous, queue)
				enqueueManagedVPA(current, queue)
			}
		},
		DeleteFunc: func(object any) { enqueueManagedVPA(object, queue) },
	})
}

func vpaChanged(previous, current any) bool {
	oldVPA, oldOK := previous.(*unstructured.Unstructured)
	newVPA, newOK := current.(*unstructured.Unstructured)
	return oldOK && newOK && (oldVPA.GetUID() != newVPA.GetUID() ||
		!apiequality.Semantic.DeepEqual(oldVPA.Object["spec"], newVPA.Object["spec"]) ||
		!apiequality.Semantic.DeepEqual(oldVPA.GetOwnerReferences(), newVPA.GetOwnerReferences()) ||
		!apiequality.Semantic.DeepEqual(oldVPA.GetLabels(), newVPA.GetLabels()) ||
		!apiequality.Semantic.DeepEqual(oldVPA.GetDeletionTimestamp(), newVPA.GetDeletionTimestamp()))
}

func enqueueManagedVPA(object any, queue workqueue.RateLimitingInterface) {
	if tombstone, ok := object.(cache.DeletedFinalStateUnknown); ok {
		object = tombstone.Obj
	}
	vpa, ok := object.(*unstructured.Unstructured)
	if !ok {
		return
	}
	kind, name := "", ""
	for _, owner := range vpa.GetOwnerReferences() {
		if owner.APIVersion != "apps/v1" || owner.Controller == nil || !*owner.Controller ||
			(owner.Kind != controller.DeploymentKind && owner.Kind != controller.StatefulSetKind) {
			continue
		}
		kind, name = owner.Kind, owner.Name
		break
	}
	if kind == "" {
		targetKind, _, _ := unstructured.NestedString(vpa.Object, "spec", "targetRef", "kind")
		targetName, _, _ := unstructured.NestedString(vpa.Object, "spec", "targetRef", "name")
		if targetKind == controller.DeploymentKind || targetKind == controller.StatefulSetKind {
			kind, name = targetKind, targetName
		}
	}
	if kind == "" || name == "" {
		return
	}
	owned := controller.IsManagedVPA(vpa, kind, name)
	if !owned && controller.VPAName(kind, name) != vpa.GetName() {
		return
	}
	queue.Add(controller.WorkloadKey(kind, vpa.GetNamespace(), name))
}

func runWorker(ctx context.Context, queue workqueue.RateLimitingInterface, reconciler *controller.Reconciler) {
	for {
		item, shutdown := queue.Get()
		if shutdown {
			return
		}
		key, ok := item.(string)
		if !ok {
			queue.Forget(item)
			queue.Done(item)
			continue
		}
		if err := reconciler.Reconcile(ctx, key); err != nil {
			controller.LogReconcileError(key, err)
			queue.AddRateLimited(key)
		} else {
			queue.Forget(item)
		}
		queue.Done(item)
	}
}

func enqueueOptedIn(deployments, statefulSets cache.SharedIndexInformer, queue workqueue.RateLimitingInterface) {
	for _, entry := range []struct {
		informer cache.SharedIndexInformer
		kind     string
	}{
		{deployments, controller.DeploymentKind},
		{statefulSets, controller.StatefulSetKind},
	} {
		for _, object := range entry.informer.GetStore().List() {
			workload, ok := object.(metav1.Object)
			if ok && workload.GetAnnotations()[api.ProfileAnnotation] != "" {
				queue.Add(controller.WorkloadKey(entry.kind, workload.GetNamespace(), workload.GetName()))
			}
		}
	}
}

func isProfileConfigMap(object any, namespace, name string) bool {
	if tombstone, ok := object.(cache.DeletedFinalStateUnknown); ok {
		object = tombstone.Obj
	}
	configMap, ok := object.(*corev1.ConfigMap)
	return ok && configMap.Namespace == namespace && configMap.Name == name
}

func profileConfigMapUpdated(oldObject, newObject any, namespace, name string) bool {
	oldMap, oldOK := oldObject.(*corev1.ConfigMap)
	newMap, newOK := newObject.(*corev1.ConfigMap)
	return oldOK && newOK && isProfileConfigMap(newMap, namespace, name) &&
		configMapDataChanged(oldMap, newMap)
}

func profileConfigMapDeleted(object any, namespace, name string) bool {
	return isProfileConfigMap(object, namespace, name)
}

func mapsEqual[K comparable, V comparable](a, b map[K]V) bool {
	if len(a) != len(b) {
		return false
	}
	for key, value := range a {
		if b[key] != value {
			return false
		}
	}
	return true
}

func configMapDataChanged(oldMap, newMap *corev1.ConfigMap) bool {
	if !mapsEqual(oldMap.Data, newMap.Data) || len(oldMap.BinaryData) != len(newMap.BinaryData) {
		return true
	}
	for key, value := range oldMap.BinaryData {
		if !bytes.Equal(value, newMap.BinaryData[key]) {
			return true
		}
	}
	return false
}
