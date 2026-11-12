package main

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/kevinkrp93/vpa-profile-operator/api"
	"github.com/kevinkrp93/vpa-profile-operator/controller"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	kubefake "k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
)

type recordingInformer struct {
	cache.SharedIndexInformer
	handler cache.ResourceEventHandler
}

func (informer *recordingInformer) AddEventHandler(handler cache.ResourceEventHandler) (cache.ResourceEventHandlerRegistration, error) {
	informer.handler = handler
	return nil, nil
}

func TestWorkloadHandlersIgnoreIrrelevantUpdates(t *testing.T) {
	for _, workload := range []struct {
		kind   string
		object runtime.Object
	}{
		{controller.DeploymentKind, &appsv1.Deployment{}},
		{controller.StatefulSetKind, &appsv1.StatefulSet{}},
	} {
		t.Run(workload.kind, func(t *testing.T) {
			original := workload.object.(metav1.Object)
			original.SetNamespace("apps")
			original.SetName("checkout")
			original.SetUID("workload-uid")
			original.SetGeneration(1)
			original.SetAnnotations(map[string]string{api.ProfileAnnotation: "baseline"})
			for _, test := range []struct {
				name    string
				mutate  func(runtime.Object)
				enqueue bool
			}{
				{"unchanged", func(runtime.Object) {}, false},
				{"status-only", func(object runtime.Object) {
					object.(metav1.Object).SetResourceVersion("2")
					switch typed := object.(type) {
					case *appsv1.Deployment:
						typed.Status.Replicas = 3
					case *appsv1.StatefulSet:
						typed.Status.Replicas = 3
					}
				}, false},
				{"unrelated-annotation", func(object runtime.Object) {
					object.(metav1.Object).SetAnnotations(map[string]string{api.ProfileAnnotation: "baseline", "unrelated": "changed"})
				}, false},
				{"profile-change", func(object runtime.Object) {
					object.(metav1.Object).SetAnnotations(map[string]string{api.ProfileAnnotation: "aggressive"})
				}, true},
				{"profile-removal", func(object runtime.Object) {
					object.(metav1.Object).SetAnnotations(nil)
				}, true},
				{"spec-generation", func(object runtime.Object) {
					object.(metav1.Object).SetGeneration(2)
				}, true},
				{"replacement", func(object runtime.Object) {
					object.(metav1.Object).SetUID("new-uid")
				}, true},
				{"deletion-started", func(object runtime.Object) {
					now := metav1.Now()
					object.(metav1.Object).SetDeletionTimestamp(&now)
				}, true},
			} {
				t.Run(test.name, func(t *testing.T) {
					queue := workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())
					defer queue.ShutDown()
					informer := &recordingInformer{}
					registerWorkloadHandlers(informer, workload.kind, queue)
					current := workload.object.DeepCopyObject()
					test.mutate(current)
					informer.handler.OnUpdate(workload.object, current)
					expected := 0
					if test.enqueue {
						expected = 1
					}
					if queue.Len() != expected {
						t.Fatalf("expected %d queued items, got %d", expected, queue.Len())
					}
				})
			}
		})
	}
}

func TestVPAHandlersIgnoreStatusButReconcileDrift(t *testing.T) {
	original, err := controller.DesiredVPA("apps", controller.VPAName(controller.DeploymentKind, "checkout"),
		controller.DeploymentKind, "checkout", "workload-uid", api.ResolvedProfile{})
	if err != nil {
		t.Fatal(err)
	}
	original.SetUID("vpa-uid")
	for _, test := range []struct {
		name    string
		mutate  func(*unstructured.Unstructured)
		enqueue bool
	}{
		{"unchanged", func(*unstructured.Unstructured) {}, false},
		{"status-only", func(object *unstructured.Unstructured) {
			object.SetResourceVersion("2")
			object.Object["status"] = map[string]any{"recommendation": map[string]any{"containerRecommendations": []any{}}}
		}, false},
		{"annotation-only", func(object *unstructured.Unstructured) {
			object.SetAnnotations(map[string]string{"unrelated": "changed"})
		}, false},
		{"spec-drift", func(object *unstructured.Unstructured) {
			object.Object["spec"] = map[string]any{}
		}, true},
		{"ownership-drift", func(object *unstructured.Unstructured) {
			object.SetOwnerReferences(nil)
		}, true},
		{"labels-removed", func(object *unstructured.Unstructured) {
			object.SetLabels(nil)
		}, true},
		{"identity-removed", func(object *unstructured.Unstructured) {
			object.SetOwnerReferences(nil)
			object.SetLabels(nil)
			object.Object["spec"] = map[string]any{}
		}, true},
		{"replacement", func(object *unstructured.Unstructured) {
			object.SetUID("new-vpa-uid")
		}, true},
		{"deletion-started", func(object *unstructured.Unstructured) {
			now := metav1.Now()
			object.SetDeletionTimestamp(&now)
		}, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			queue := workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())
			defer queue.ShutDown()
			informer := &recordingInformer{}
			registerVPAHandlers(informer, queue)
			current := original.DeepCopy()
			test.mutate(current)
			informer.handler.OnUpdate(original, current)
			expected := 0
			if test.enqueue {
				expected = 1
			}
			if queue.Len() != expected {
				t.Fatalf("expected %d queued items, got %d", expected, queue.Len())
			}
			if expected == 1 {
				item, _ := queue.Get()
				queue.Done(item)
				if item != controller.WorkloadKey(controller.DeploymentKind, "apps", "checkout") {
					t.Fatalf("drift must reconcile the original workload, got %v", item)
				}
			}
		})
	}
}

func TestProfileConfigMapInformerScopesListAndWatch(t *testing.T) {
	client := kubefake.NewSimpleClientset()
	watcher := watch.NewRaceFreeFake()
	watchStarted := make(chan struct{})
	problems := make(chan error, 4)
	client.PrependReactor("list", "configmaps", func(action clienttesting.Action) (bool, runtime.Object, error) {
		selector := action.(clienttesting.ListAction).GetListRestrictions().Fields
		if action.GetNamespace() != "vpa-system" || selector.String() != "metadata.name=vpa-profiles" {
			problems <- fmt.Errorf("unscoped list: namespace=%q selector=%q", action.GetNamespace(), selector)
		}
		return true, &corev1.ConfigMapList{Items: []corev1.ConfigMap{{
			ObjectMeta: metav1.ObjectMeta{Namespace: "vpa-system", Name: "vpa-profiles"},
			Data:       map[string]string{"profiles.yaml": "initial"},
		}}}, nil
	})
	var once sync.Once
	client.PrependWatchReactor("configmaps", func(action clienttesting.Action) (bool, watch.Interface, error) {
		selector := action.(clienttesting.WatchAction).GetWatchRestrictions().Fields
		if action.GetNamespace() != "vpa-system" || selector.String() != "metadata.name=vpa-profiles" {
			problems <- fmt.Errorf("unscoped watch: namespace=%q selector=%q", action.GetNamespace(), selector)
		}
		once.Do(func() { close(watchStarted) })
		return true, watcher, nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	informer := newProfileConfigMapInformer(client, "vpa-system", "vpa-profiles")
	updated := make(chan struct{}, 1)
	informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		UpdateFunc: func(_, current any) {
			if current.(*corev1.ConfigMap).Data["profiles.yaml"] == "updated" {
				updated <- struct{}{}
			}
		},
	})
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		informer.Run(ctx.Done())
	}()
	defer func() {
		cancel()
		<-stopped
	}()
	if !cache.WaitForCacheSync(ctx.Done(), informer.HasSynced) {
		t.Fatal("profile cache failed to synchronize")
	}
	select {
	case <-watchStarted:
	case <-ctx.Done():
		t.Fatal("profile watch was not established")
	}
	watcher.Modify(&corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Namespace: "vpa-system", Name: "vpa-profiles", ResourceVersion: "2"},
		Data:       map[string]string{"profiles.yaml": "updated"},
	})
	select {
	case <-updated:
	case <-ctx.Done():
		t.Fatal("watch update did not reach the local profile cache")
	}
	select {
	case err := <-problems:
		t.Fatal(err)
	default:
	}
	actions := client.Actions()
	if len(actions) != 2 || actions[0].GetVerb() != "list" || actions[1].GetVerb() != "watch" {
		t.Fatalf("expected one initialization list and one watch, got %v", actions)
	}
}

func TestWorkerRetriesWriteErrorsUsingRefreshedCache(t *testing.T) {
	for _, response := range []string{"AlreadyExists", "Conflict", "NotFound"} {
		t.Run(response, func(t *testing.T) {
			client := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme())
			reconciler := &controller.Reconciler{
				DynamicClient: client,
				ProfileNS:     "vpa-system",
				ProfileName:   "vpa-profiles",
				ProfileKey:    "profiles.yaml",
				Deployments:   cache.NewStore(cache.MetaNamespaceKeyFunc),
				StatefulSets:  cache.NewStore(cache.MetaNamespaceKeyFunc),
				ConfigMaps:    cache.NewStore(cache.MetaNamespaceKeyFunc),
				VPAs:          cache.NewStore(cache.MetaNamespaceKeyFunc),
			}
			deployment := &appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "apps", Name: "checkout", UID: "workload-uid",
					Annotations: map[string]string{api.ProfileAnnotation: "baseline"},
				},
				Spec: appsv1.DeploymentSpec{Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "checkout"}}},
			}
			if err := reconciler.Deployments.Add(deployment); err != nil {
				t.Fatal(err)
			}
			if err := reconciler.ConfigMaps.Add(&corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{Namespace: "vpa-system", Name: "vpa-profiles"},
				Data:       map[string]string{"profiles.yaml": "profiles:\n  baseline:\n    cpuHistogramDecayHalfLife: 1h\n    podUpdateThreshold: 0.1\n"},
			}); err != nil {
				t.Fatal(err)
			}
			vpa, err := controller.DesiredVPA("apps", controller.VPAName(controller.DeploymentKind, "checkout"),
				controller.DeploymentKind, "checkout", deployment.UID, api.ResolvedProfile{})
			if err != nil {
				t.Fatal(err)
			}
			vpa.SetUID("vpa-uid")
			vpa.SetResourceVersion("1")
			if response != "AlreadyExists" {
				if err := reconciler.VPAs.Add(vpa); err != nil {
					t.Fatal(err)
				}
			}
			fresh := vpa.DeepCopy()
			fresh.SetResourceVersion("2")
			if response == "NotFound" {
				fresh.SetUID("replacement-vpa-uid")
			}
			if err := client.Tracker().Create(controller.VPAResource(), fresh, "apps"); err != nil {
				t.Fatal(err)
			}
			var requests int
			writeCompleted := make(chan struct{})
			client.PrependReactor("*", "verticalpodautoscalers", func(action clienttesting.Action) (bool, runtime.Object, error) {
				requests++
				if requests == 1 {
					expectedVerb := "update"
					if response == "AlreadyExists" {
						expectedVerb = "create"
					}
					if action.GetVerb() != expectedVerb {
						t.Errorf("expected %s, got %s", expectedVerb, action.GetVerb())
					}
					if err := reconciler.VPAs.Update(fresh); err != nil {
						return true, nil, err
					}
					resource := controller.VPAResource().GroupResource()
					switch response {
					case "AlreadyExists":
						return true, nil, apierrors.NewAlreadyExists(resource, vpa.GetName())
					case "Conflict":
						return true, nil, apierrors.NewConflict(resource, vpa.GetName(), nil)
					default:
						return true, nil, apierrors.NewNotFound(resource, vpa.GetName())
					}
				}
				if action.GetVerb() != "update" {
					return true, nil, fmt.Errorf("unexpected retry request %s", action.GetVerb())
				}
				updated := action.(clienttesting.UpdateAction).GetObject().(*unstructured.Unstructured)
				if updated.GetUID() != fresh.GetUID() || updated.GetResourceVersion() != "2" {
					t.Errorf("retry used stale cached identity/version: %v", updated)
				}
				close(writeCompleted)
				return false, nil, nil
			})
			queue := workqueue.NewRateLimitingQueue(workqueue.NewItemExponentialFailureRateLimiter(time.Millisecond, time.Second))
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			stopped := make(chan struct{})
			go func() {
				defer close(stopped)
				runWorker(ctx, queue, reconciler)
			}()
			defer func() {
				cancel()
				queue.ShutDown()
				<-stopped
			}()
			key := controller.WorkloadKey(controller.DeploymentKind, "apps", "checkout")
			queue.Add(key)
			select {
			case <-writeCompleted:
			case <-ctx.Done():
				t.Fatal("rate-limited worker did not retry the failed write")
			}
			if err := wait.PollUntilContextCancel(ctx, time.Millisecond, true, func(context.Context) (bool, error) {
				return queue.NumRequeues(key) == 0, nil
			}); err != nil {
				t.Fatal("successful retry did not clear the rate limiter")
			}
			if actions := client.Actions(); len(actions) != 2 {
				t.Fatalf("expected failed write and one retry without GET/LIST requests, got %v", actions)
			}
		})
	}
}

func TestEnqueueOptedInSeparatesSameNameWorkloadKinds(t *testing.T) {
	deployments := cache.NewSharedIndexInformer(&cache.ListWatch{}, &appsv1.Deployment{}, 0, cache.Indexers{})
	statefulSets := cache.NewSharedIndexInformer(&cache.ListWatch{}, &appsv1.StatefulSet{}, 0, cache.Indexers{})
	if err := deployments.GetStore().Add(&appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Namespace: "apps", Name: "checkout", Annotations: map[string]string{api.ProfileAnnotation: "baseline"}},
	}); err != nil {
		t.Fatal(err)
	}

	if err := statefulSets.GetStore().Add(&appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Namespace: "apps", Name: "checkout", Annotations: map[string]string{api.ProfileAnnotation: "baseline"}},
	}); err != nil {
		t.Fatal(err)
	}

	queue := workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())
	defer queue.ShutDown()
	enqueueOptedIn(deployments, statefulSets, queue)
	if queue.Len() != 2 {
		t.Fatalf("expected one key per workload kind, got %d", queue.Len())
	}
	keys := map[string]bool{}
	for range 2 {
		item, shutdown := queue.Get()
		if shutdown {
			t.Fatal("queue shut down before returning workload key")
		}
		keys[item.(string)] = true
		queue.Done(item)
	}
	if !keys[controller.WorkloadKey(controller.DeploymentKind, "apps", "checkout")] ||
		!keys[controller.WorkloadKey(controller.StatefulSetKind, "apps", "checkout")] {
		t.Fatalf("expected kind-specific queue keys, got %v", keys)
	}
}

func TestEnqueueManagedVPAReconcilesItsWorkload(t *testing.T) {
	vpa, err := controller.DesiredVPA("apps", controller.VPAName(controller.DeploymentKind, "checkout"),
		controller.DeploymentKind, "checkout", types.UID("workload-uid"), api.ResolvedProfile{})
	if err != nil {
		t.Fatal(err)
	}
	queue := workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())
	defer queue.ShutDown()
	enqueueManagedVPA(vpa, queue)
	tombstone := cache.DeletedFinalStateUnknown{Key: "apps/" + vpa.GetName(), Obj: vpa}
	enqueueManagedVPA(tombstone, queue)
	if queue.Len() != 1 {
		t.Fatalf("expected VPA add/delete events to enqueue a deduplicated reconciliation, got %d", queue.Len())
	}
	item, shutdown := queue.Get()
	if shutdown {
		t.Fatal("queue shut down before returning VPA workload key")
	}
	queue.Done(item)
	if item != controller.WorkloadKey(controller.DeploymentKind, "apps", "checkout") {
		t.Fatalf("unexpected queued workload key %v", item)
	}

	unmanaged := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "autoscaling.k8s.io/v1",
		"kind":       "VerticalPodAutoscaler",
		"metadata": map[string]any{
			"name": "unmanaged", "namespace": "apps",
			"ownerReferences": []any{map[string]any{
				"apiVersion": "apps/v1", "kind": controller.DeploymentKind,
				"name": "checkout", "uid": "workload-uid", "controller": true,
			}},
		},
	}}
	unmanaged.SetKind("VerticalPodAutoscaler")
	enqueueManagedVPA(runtime.Object(unmanaged), queue)
	if queue.Len() != 0 {
		t.Fatal("unmanaged VPA should not trigger profile reconciliation")
	}

	drifted := vpa.DeepCopy()
	drifted.SetOwnerReferences(nil)
	enqueueManagedVPA(drifted, queue)
	if queue.Len() != 1 {
		t.Fatal("managed VPA with owner-reference drift should still enqueue via its targetRef and labels")
	}
}

func TestProfileConfigMapDetectionAndChange(t *testing.T) {
	oldMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Namespace: "vpa-system", Name: "vpa-profiles"},
		Data:       map[string]string{"profiles.yaml": "profiles: {}"},
		BinaryData: map[string][]byte{"other": []byte{1, 2}},
	}
	newMap := oldMap.DeepCopy()
	if !isProfileConfigMap(newMap, "vpa-system", "vpa-profiles") || profileConfigMapUpdated(oldMap, newMap, "vpa-system", "vpa-profiles") {
		t.Fatal("identical profile ConfigMap was not recognized")
	}
	oldOther := newMap.DeepCopy()
	oldOther.Data["profiles.yaml"] = "profiles: {old: {}}"
	newMap.Data["profiles.yaml"] = "profiles: {changed: {}}"
	if !isProfileConfigMap(newMap, "vpa-system", "vpa-profiles") ||
		!configMapDataChanged(oldOther, newMap) ||
		!profileConfigMapUpdated(oldOther, newMap, "vpa-system", "vpa-profiles") {
		t.Fatal("profile ConfigMap data update was not detected")
	}
	if profileConfigMapUpdated(oldOther, newMap, "other-system", "vpa-profiles") {
		t.Fatal("unconfigured ConfigMap namespace was matched")
	}
	tombstone := cache.DeletedFinalStateUnknown{Key: "vpa-system/vpa-profiles", Obj: newMap}
	if !profileConfigMapDeleted(tombstone, "vpa-system", "vpa-profiles") {
		t.Fatal("deleted ConfigMap tombstone was not recognized")
	}
}
