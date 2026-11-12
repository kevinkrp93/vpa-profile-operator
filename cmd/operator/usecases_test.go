package main

import (
	"context"
	"testing"

	"github.com/kevinkrp93/vpa-profile-operator/api"
	"github.com/kevinkrp93/vpa-profile-operator/controller"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
)

func TestVPAEventsTriggerSelfHealingForSupportedWorkloads(t *testing.T) {
	for _, kind := range []string{controller.DeploymentKind, controller.StatefulSetKind} {
		for _, event := range []string{"deleted", "spec-drift"} {
			t.Run(kind+"/"+event, func(t *testing.T) {
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
				metadata := metav1.ObjectMeta{
					Namespace: "apps", Name: "checkout", UID: "workload-uid",
					Annotations: map[string]string{api.ProfileAnnotation: "baseline"},
				}
				selector := &metav1.LabelSelector{MatchLabels: map[string]string{"app": "checkout"}}
				var workload any = &appsv1.Deployment{ObjectMeta: metadata, Spec: appsv1.DeploymentSpec{Selector: selector}}
				store := reconciler.Deployments
				if kind == controller.StatefulSetKind {
					workload = &appsv1.StatefulSet{ObjectMeta: metadata, Spec: appsv1.StatefulSetSpec{Selector: selector}}
					store = reconciler.StatefulSets
				}
				if err := store.Add(workload); err != nil {
					t.Fatal(err)
				}
				if err := reconciler.ConfigMaps.Add(&corev1.ConfigMap{
					ObjectMeta: metav1.ObjectMeta{Namespace: "vpa-system", Name: "vpa-profiles"},
					Data:       map[string]string{"profiles.yaml": "profiles:\n  baseline:\n    cpuHistogramDecayHalfLife: 1h\n    podUpdateThreshold: 0.1\n"},
				}); err != nil {
					t.Fatal(err)
				}
				key, name := controller.WorkloadKey(kind, "apps", "checkout"), controller.VPAName(kind, "checkout")
				if err := reconciler.Reconcile(context.Background(), key); err != nil {
					t.Fatal(err)
				}
				object, err := client.Tracker().Get(controller.VPAResource(), "apps", name)
				if err != nil {
					t.Fatal(err)
				}
				original := object.(*unstructured.Unstructured)
				original.SetUID("vpa-uid")
				original.SetResourceVersion("1")
				current := original.DeepCopy()
				queue := workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())
				defer queue.ShutDown()
				informer := &recordingInformer{}
				registerVPAHandlers(informer, queue)
				expectedVerb := "update"
				if event == "deleted" {
					expectedVerb = "create"
					if err := client.Tracker().Delete(controller.VPAResource(), "apps", name); err != nil {
						t.Fatal(err)
					}
					informer.handler.OnDelete(cache.DeletedFinalStateUnknown{Key: "apps/" + name, Obj: original})
				} else {
					current.Object["spec"] = map[string]any{"targetRef": map[string]any{"kind": "Deployment", "name": "wrong-target"}}
					if err := client.Tracker().Update(controller.VPAResource(), current, "apps"); err != nil {
						t.Fatal(err)
					}
					if err := reconciler.VPAs.Add(current); err != nil {
						t.Fatal(err)
					}
					informer.handler.OnUpdate(original, current)
				}
				if queue.Len() != 1 {
					t.Fatalf("VPA event did not enqueue its original workload: %d items", queue.Len())
				}
				item, _ := queue.Get()
				if item != key {
					t.Fatalf("event queued %v, want %s", item, key)
				}
				client.ClearActions()
				err = reconciler.Reconcile(context.Background(), item.(string))
				queue.Done(item)
				if err != nil {
					t.Fatal(err)
				}
				actions := client.Actions()
				if len(actions) != 1 || actions[0].GetVerb() != expectedVerb {
					t.Fatalf("self-healing should issue one %s and no GET/LIST, got %v", expectedVerb, actions)
				}
				object, err = client.Tracker().Get(controller.VPAResource(), "apps", name)
				if err != nil {
					t.Fatal(err)
				}
				repaired := object.(*unstructured.Unstructured)
				repairedSpec, _, err := unstructured.NestedMap(repaired.Object, "spec")
				if err != nil {
					t.Fatal(err)
				}
				target, _, err := unstructured.NestedString(repairedSpec, "targetRef", "name")
				if err != nil || target != "checkout" {
					t.Fatalf("self-healing did not restore the workload target: %v", repairedSpec)
				}
				threshold, _, err := unstructured.NestedFloat64(repairedSpec, "updatePolicy", "podUpdateThreshold")
				if err != nil || threshold != 0.1 {
					t.Fatalf("self-healing did not restore the selected profile: %v", repairedSpec)
				}
			})
		}
	}
}

func TestWorkloadDeleteEventsLeaveGarbageCollectionToKubernetes(t *testing.T) {
	for _, kind := range []string{controller.DeploymentKind, controller.StatefulSetKind} {
		t.Run(kind, func(t *testing.T) {
			client := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme())
			name := controller.VPAName(kind, "checkout")
			vpa, err := controller.DesiredVPA("apps", name, kind, "checkout", "replacement-workload-uid", api.ResolvedProfile{})
			if err != nil {
				t.Fatal(err)
			}
			if err := client.Tracker().Create(controller.VPAResource(), vpa, "apps"); err != nil {
				t.Fatal(err)
			}
			reconciler := &controller.Reconciler{
				DynamicClient: client,
				Deployments:   cache.NewStore(cache.MetaNamespaceKeyFunc),
				StatefulSets:  cache.NewStore(cache.MetaNamespaceKeyFunc),
				VPAs:          cache.NewStore(cache.MetaNamespaceKeyFunc),
			}
			queue := workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())
			defer queue.ShutDown()
			informer := &recordingInformer{}
			registerWorkloadHandlers(informer, kind, queue)
			deleted := &metav1.PartialObjectMetadata{ObjectMeta: metav1.ObjectMeta{Namespace: "apps", Name: "checkout", UID: "deleted-workload-uid"}}
			informer.handler.OnDelete(cache.DeletedFinalStateUnknown{Key: "apps/checkout", Obj: deleted})
			if queue.Len() != 1 {
				t.Fatal("workload tombstone did not enqueue reconciliation")
			}
			item, _ := queue.Get()
			err = reconciler.Reconcile(context.Background(), item.(string))
			queue.Done(item)
			if err != nil || len(client.Actions()) != 0 {
				t.Fatalf("cache absence must not cause a replacement VPA to be deleted: err=%v actions=%v", err, client.Actions())
			}
			if _, err := client.Tracker().Get(controller.VPAResource(), "apps", name); err != nil {
				t.Fatalf("replacement-owned VPA was removed: %v", err)
			}
		})
	}
}

func TestProfileConfigMapDataChangeMatrix(t *testing.T) {
	for _, test := range []struct {
		name    string
		mutate  func(*corev1.ConfigMap, *corev1.ConfigMap)
		changed bool
	}{
		{"identical", func(_, _ *corev1.ConfigMap) {}, false},
		{"metadata-only", func(_, current *corev1.ConfigMap) {
			current.ResourceVersion = "2"
			current.Labels = map[string]string{"unrelated": "changed"}
		}, false},
		{"profile-updated", func(_, current *corev1.ConfigMap) {
			current.Data["profiles.yaml"] = "profiles: {updated: {}}"
		}, true},
		{"profile-removed", func(_, current *corev1.ConfigMap) {
			current.Data = nil
		}, true},
		{"empty-key-renamed", func(previous, current *corev1.ConfigMap) {
			previous.Data = map[string]string{"unused": ""}
			current.Data = map[string]string{"profiles.yaml": ""}
		}, true},
		{"binary-added", func(_, current *corev1.ConfigMap) {
			current.BinaryData = map[string][]byte{"other": {1}}
		}, true},
		{"binary-updated", func(previous, current *corev1.ConfigMap) {
			previous.BinaryData = map[string][]byte{"other": {1}}
			current.BinaryData = map[string][]byte{"other": {2}}
		}, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			previous := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{Namespace: "vpa-system", Name: "vpa-profiles"},
				Data:       map[string]string{"profiles.yaml": "profiles: {baseline: {}}"},
			}
			current := previous.DeepCopy()
			test.mutate(previous, current)
			if changed := profileConfigMapUpdated(previous, current, "vpa-system", "vpa-profiles"); changed != test.changed {
				t.Fatalf("ConfigMap change detection returned %t, want %t", changed, test.changed)
			}
		})
	}
}
