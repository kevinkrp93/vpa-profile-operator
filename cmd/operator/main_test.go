package main

import (
	"testing"

	"github.com/kevinkrp93/vpa-profile-operator/api"
	"github.com/kevinkrp93/vpa-profile-operator/controller"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
)

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
