package controller

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/kevinkrp93/vpa-profile-operator/api"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	clienttesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
)

func TestReconcileCreatesUpdatesAndDeletesOnlyOwnedVPA(t *testing.T) {
	ctx := context.Background()
	client := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme())
	deployment := testDeployment("checkout", "deployment-uid", "aggressive")
	configMap := testConfigMap(`
profiles:
  aggressive:
    cpuHistogramDecayHalfLife: 5m
    podUpdateThreshold: 0.15
`)
	deployments := cache.NewStore(cache.MetaNamespaceKeyFunc)
	statefulSets := cache.NewStore(cache.MetaNamespaceKeyFunc)
	configMaps := cache.NewStore(cache.MetaNamespaceKeyFunc)
	for _, storeObject := range []struct {
		store  cache.Store
		object any
	}{{deployments, deployment}, {configMaps, configMap}} {
		if err := storeObject.store.Add(storeObject.object); err != nil {
			t.Fatal(err)
		}
	}
	reconciler := testReconciler(client, deployments, statefulSets, configMaps)
	key := WorkloadKey(DeploymentKind, "apps", "checkout")
	if err := reconciler.Reconcile(ctx, key); err != nil {
		t.Fatal(err)
	}
	vpaName := VPAName(DeploymentKind, "checkout")
	vpa, err := client.Resource(vpaGVR).Namespace("apps").Get(ctx, vpaName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	assertVPAValues(t, vpa, DeploymentKind, "checkout", "5m0s", 0.15)
	assertOwnerUID(t, vpa, "deployment-uid")
	vpa.SetUID(types.UID("managed-vpa-uid"))
	vpa.SetResourceVersion("1")
	if _, err := client.Resource(vpaGVR).Namespace("apps").Update(ctx, vpa, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := reconciler.VPAs.Add(vpa); err != nil {
		t.Fatal(err)
	}

	client.ClearActions()
	if err := reconciler.Reconcile(ctx, key); err != nil {
		t.Fatal(err)
	}
	if len(client.Actions()) != 0 {
		t.Fatalf("identical reconciliation contacted the API server: %v", client.Actions())
	}

	configMap = testConfigMap("profiles:\n  aggressive:\n    cpuHistogramDecayHalfLife: 10m\n    podUpdateThreshold: 0.15\n")
	if err := configMaps.Update(configMap); err != nil {
		t.Fatal(err)
	}
	if err := reconciler.Reconcile(ctx, key); err != nil {
		t.Fatal(err)
	}
	vpa, err = client.Resource(vpaGVR).Namespace("apps").Get(ctx, vpaName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	assertVPAValues(t, vpa, DeploymentKind, "checkout", "10m0s", -1)
	if err := reconciler.VPAs.Update(vpa); err != nil {
		t.Fatal(err)
	}

	deployment.Annotations = nil
	if err := deployments.Update(deployment); err != nil {
		t.Fatal(err)
	}
	if err := reconciler.Reconcile(ctx, key); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Resource(vpaGVR).Namespace("apps").Get(ctx, vpaName, metav1.GetOptions{}); err == nil {
		t.Fatal("owned VPA was not deleted after annotation removal")
	}
}

func TestCoNamedWorkloadsHaveIndependentVPAs(t *testing.T) {
	ctx := context.Background()
	client := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme())
	deployments := cache.NewStore(cache.MetaNamespaceKeyFunc)
	statefulSets := cache.NewStore(cache.MetaNamespaceKeyFunc)
	configMaps := cache.NewStore(cache.MetaNamespaceKeyFunc)
	deployment := testDeployment("same-name", "deployment-uid", "baseline")
	statefulSet := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "apps", Name: "same-name", UID: types.UID("statefulset-uid"),
			Annotations: map[string]string{api.ProfileAnnotation: "baseline"},
		},
		Spec: appsv1.StatefulSetSpec{Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "same"}}},
	}
	_ = deployments.Add(deployment)
	_ = statefulSets.Add(statefulSet)
	_ = configMaps.Add(testConfigMap("profiles:\n  baseline:\n    cpuHistogramDecayHalfLife: 1h\n    podUpdateThreshold: 0.1\n"))
	reconciler := testReconciler(client, deployments, statefulSets, configMaps)
	for _, key := range []string{
		WorkloadKey(DeploymentKind, "apps", "same-name"),
		WorkloadKey(StatefulSetKind, "apps", "same-name"),
	} {
		if err := reconciler.Reconcile(ctx, key); err != nil {
			t.Fatal(err)
		}
	}
	for _, kind := range []string{DeploymentKind, StatefulSetKind} {
		name := VPAName(kind, "same-name")
		if _, err := client.Resource(vpaGVR).Namespace("apps").Get(ctx, name, metav1.GetOptions{}); err != nil {
			t.Fatalf("missing VPA %s: %v", name, err)
		}
	}
	if VPAName(DeploymentKind, "same-name") == VPAName(StatefulSetKind, "same-name") {
		t.Fatal("co-named workload kinds received the same VPA name")
	}
}

func TestReconcileProtectsUIDAndUnmanagedVPAs(t *testing.T) {
	ctx := context.Background()
	deployment := testDeployment("checkout", "new-uid", "baseline")
	deployments := cache.NewStore(cache.MetaNamespaceKeyFunc)
	_ = deployments.Add(deployment)
	configMaps := cache.NewStore(cache.MetaNamespaceKeyFunc)
	_ = configMaps.Add(testConfigMap("profiles:\n  baseline:\n    cpuHistogramDecayHalfLife: 1h\n    podUpdateThreshold: 0.1\n"))

	t.Run("different owner UID", func(t *testing.T) {
		client := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme())
		existing, _ := DesiredVPA("apps", VPAName(DeploymentKind, "checkout"), DeploymentKind, "checkout", types.UID("old-uid"), api.ResolvedProfile{})
		_, _ = client.Resource(vpaGVR).Namespace("apps").Create(ctx, existing, metav1.CreateOptions{})
		reconciler := testReconciler(client, deployments, cache.NewStore(cache.MetaNamespaceKeyFunc), configMaps)
		if err := reconciler.VPAs.Add(existing); err != nil {
			t.Fatal(err)
		}
		if err := reconciler.Reconcile(ctx, WorkloadKey(DeploymentKind, "apps", "checkout")); err == nil {
			t.Fatal("expected UID ownership conflict")
		}
	})
	t.Run("unmanaged name collision", func(t *testing.T) {
		client := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme())
		unmanaged := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "autoscaling.k8s.io/v1", "kind": "VerticalPodAutoscaler",
			"metadata": map[string]any{"name": VPAName(DeploymentKind, "checkout"), "namespace": "apps"},
		}}
		_, _ = client.Resource(vpaGVR).Namespace("apps").Create(ctx, unmanaged, metav1.CreateOptions{})
		reconciler := testReconciler(client, deployments, cache.NewStore(cache.MetaNamespaceKeyFunc), configMaps)
		if err := reconciler.VPAs.Add(unmanaged); err != nil {
			t.Fatal(err)
		}
		if err := reconciler.Reconcile(ctx, WorkloadKey(DeploymentKind, "apps", "checkout")); err == nil {
			t.Fatal("expected unmanaged VPA collision")
		}
	})
}

func TestReconcileReportsMissingProfiles(t *testing.T) {
	client := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme())
	deployments := cache.NewStore(cache.MetaNamespaceKeyFunc)
	_ = deployments.Add(testDeployment("checkout", "uid", "missing"))
	reconciler := testReconciler(client, deployments, cache.NewStore(cache.MetaNamespaceKeyFunc), cache.NewStore(cache.MetaNamespaceKeyFunc))
	if err := reconciler.Reconcile(context.Background(), WorkloadKey(DeploymentKind, "apps", "checkout")); err == nil {
		t.Fatal("expected missing ConfigMap error")
	}
}

func TestMissingWorkloadFromStaleCacheDoesNotDeleteReplacementVPAs(t *testing.T) {
	ctx := context.Background()
	client := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme())
	replacement := testDeployment("checkout", "replacement-uid", "baseline")
	vpa, err := DesiredVPA("apps", VPAName(DeploymentKind, "checkout"), DeploymentKind, "checkout", replacement.UID, api.ResolvedProfile{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Resource(vpaGVR).Namespace("apps").Create(ctx, vpa, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	// The informer cache can briefly miss an object while a same-name
	// replacement's owned VPA already exists. Absence provides no UID proof.
	reconciler := testReconciler(client, cache.NewStore(cache.MetaNamespaceKeyFunc), cache.NewStore(cache.MetaNamespaceKeyFunc), cache.NewStore(cache.MetaNamespaceKeyFunc))
	if err := reconciler.Reconcile(ctx, WorkloadKey(DeploymentKind, "apps", "checkout")); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Resource(vpaGVR).Namespace("apps").Get(ctx, VPAName(DeploymentKind, "checkout"), metav1.GetOptions{}); err != nil {
		t.Fatalf("VPA owned by replacement workload was deleted during cache gap: %v", err)
	}
}

func TestDesiredVPALabelsAreValidForMaximumLengthWorkloadName(t *testing.T) {
	workloadName := strings.Join([]string{
		strings.Repeat("a", 63),
		strings.Repeat("b", 63),
		strings.Repeat("c", 63),
		strings.Repeat("d", 61),
	}, ".")
	if problems := validation.IsDNS1123Subdomain(workloadName); len(problems) != 0 {
		t.Fatalf("test workload name must be Kubernetes-valid: %v", problems)
	}
	vpaName := VPAName(DeploymentKind, workloadName)
	vpa, err := DesiredVPA("apps", vpaName, DeploymentKind, workloadName, types.UID("workload-uid"), api.ResolvedProfile{})
	if err != nil {
		t.Fatal(err)
	}
	if problems := validation.IsDNS1123Subdomain(vpa.GetName()); len(problems) != 0 {
		t.Fatalf("generated VPA name is invalid: %v", problems)
	}
	for label, value := range vpa.GetLabels() {
		if problems := validation.IsQualifiedName(label); len(problems) != 0 {
			t.Errorf("invalid VPA label key %q: %v", label, problems)
		}
		if problems := validation.IsValidLabelValue(value); len(problems) != 0 {
			t.Errorf("invalid VPA label value %q=%q: %v", label, value, problems)
		}
	}
	if vpa.GetLabels()[nameLabel] == workloadName || len(vpa.GetLabels()[nameLabel]) > 63 {
		t.Fatalf("expected a bounded identifier instead of the full workload name: %q", vpa.GetLabels()[nameLabel])
	}
	owners := vpa.GetOwnerReferences()
	if len(owners) != 1 || owners[0].Name != workloadName {
		t.Fatalf("full workload name must be verified by owner reference: %+v", owners)
	}
}

func TestReconcileSwitchesProfilesAndRejectsInvalidProfile(t *testing.T) {
	ctx := context.Background()
	client := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme())
	deployment := testDeployment("checkout", "uid", "conservative")
	deployments := cache.NewStore(cache.MetaNamespaceKeyFunc)
	_ = deployments.Add(deployment)
	configMaps := cache.NewStore(cache.MetaNamespaceKeyFunc)
	_ = configMaps.Add(testConfigMap(`
profiles:
  conservative:
    cpuHistogramDecayHalfLife: 24h
    podUpdateThreshold: 0.2
  aggressive:
    cpuHistogramDecayHalfLife: 4h
    podUpdateThreshold: 0.05
`))
	reconciler := testReconciler(client, deployments, cache.NewStore(cache.MetaNamespaceKeyFunc), configMaps)
	key := WorkloadKey(DeploymentKind, "apps", "checkout")
	if err := reconciler.Reconcile(ctx, key); err != nil {
		t.Fatal(err)
	}

	created, err := client.Resource(vpaGVR).Namespace("apps").Get(ctx, VPAName(DeploymentKind, "checkout"), metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	created.SetUID("vpa-uid")
	created.SetResourceVersion("1")
	if err := reconciler.VPAs.Add(created); err != nil {
		t.Fatal(err)
	}
	deployment.Annotations[api.ProfileAnnotation] = "aggressive"
	_ = deployments.Update(deployment)
	if err := reconciler.Reconcile(ctx, key); err != nil {
		t.Fatal(err)
	}
	vpa, err := client.Resource(vpaGVR).Namespace("apps").Get(ctx, VPAName(DeploymentKind, "checkout"), metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	assertVPAValues(t, vpa, DeploymentKind, "checkout", "4h0m0s", 0.05)

	deployment.Annotations[api.ProfileAnnotation] = "not-defined"
	_ = deployments.Update(deployment)
	if err := reconciler.Reconcile(ctx, key); err == nil {
		t.Fatal("expected unknown profile to be reported")
	}
}

func TestAnnotationRemovalDoesNotDeleteVPAOwnedByDifferentUID(t *testing.T) {
	ctx := context.Background()
	client := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme())
	deployment := testDeployment("checkout", "current-uid", "")
	deployments := cache.NewStore(cache.MetaNamespaceKeyFunc)
	_ = deployments.Add(deployment)
	existing, err := DesiredVPA("apps", VPAName(DeploymentKind, "checkout"), DeploymentKind, "checkout", types.UID("previous-uid"), api.ResolvedProfile{})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = client.Resource(vpaGVR).Namespace("apps").Create(ctx, existing, metav1.CreateOptions{})
	reconciler := testReconciler(client, deployments, cache.NewStore(cache.MetaNamespaceKeyFunc), cache.NewStore(cache.MetaNamespaceKeyFunc))
	if err := reconciler.VPAs.Add(existing); err != nil {
		t.Fatal(err)
	}
	if err := reconciler.Reconcile(ctx, WorkloadKey(DeploymentKind, "apps", "checkout")); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Resource(vpaGVR).Namespace("apps").Get(ctx, VPAName(DeploymentKind, "checkout"), metav1.GetOptions{}); err != nil {
		t.Fatalf("VPA owned by a different UID was deleted: %v", err)
	}
}

func TestDeleteUsesUIDAndResourceVersionPreconditions(t *testing.T) {
	ctx := context.Background()
	client := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme())
	existing, err := DesiredVPA("apps", VPAName(DeploymentKind, "checkout"), DeploymentKind, "checkout",
		types.UID("workload-uid"), api.ResolvedProfile{})
	if err != nil {
		t.Fatal(err)
	}
	existing.SetUID("vpa-uid-old")
	existing.SetResourceVersion("17")
	if _, err := client.Resource(vpaGVR).Namespace("apps").Create(ctx, existing, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	client.PrependReactor("delete", "verticalpodautoscalers", func(action clienttesting.Action) (bool, runtime.Object, error) {
		options := action.(clienttesting.DeleteAction).GetDeleteOptions()
		if options.Preconditions == nil || options.Preconditions.UID == nil ||
			*options.Preconditions.UID != types.UID("vpa-uid-old") ||
			options.Preconditions.ResourceVersion == nil || *options.Preconditions.ResourceVersion != "17" {
			t.Fatalf("delete lacked the fetched VPA UID/resourceVersion preconditions: %+v", options.Preconditions)
		}
		replacement := existing.DeepCopy()
		replacement.SetUID("vpa-uid-replacement")
		replacement.SetResourceVersion("18")
		if err := client.Tracker().Delete(vpaGVR, "apps", existing.GetName()); err != nil {
			t.Fatal(err)
		}
		if err := client.Tracker().Create(vpaGVR, replacement, "apps"); err != nil {
			t.Fatal(err)
		}
		return true, nil, apierrors.NewConflict(vpaGVR.GroupResource(), existing.GetName(), nil)
	})
	reconciler := &Reconciler{DynamicClient: client, VPAs: cache.NewStore(cache.MetaNamespaceKeyFunc)}
	if err := reconciler.VPAs.Add(existing); err != nil {
		t.Fatal(err)
	}
	client.ClearActions()
	if err := reconciler.deleteOwnedVPA(ctx, "apps", existing.GetName(), DeploymentKind, "checkout", "workload-uid"); !apierrors.IsConflict(err) {
		t.Fatal("expected delete conflict after a concurrent replacement")
	}
	if actions := client.Actions(); len(actions) != 1 || actions[0].GetVerb() != "delete" {
		t.Fatalf("delete must use the cache without API reads: %v", actions)
	}
	replacement, err := client.Resource(vpaGVR).Namespace("apps").Get(ctx, existing.GetName(), metav1.GetOptions{})
	if err != nil {
		t.Fatalf("concurrent VPA replacement was removed: %v", err)
	}
	if replacement.GetUID() != types.UID("vpa-uid-replacement") {
		t.Fatalf("unexpected remaining VPA UID: %q", replacement.GetUID())
	}
}

func TestReconcileReadsOnlyCacheAndDoesNotMutateIt(t *testing.T) {
	ctx := context.Background()
	client := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme())
	deployments := cache.NewStore(cache.MetaNamespaceKeyFunc)
	configMaps := cache.NewStore(cache.MetaNamespaceKeyFunc)
	if err := deployments.Add(testDeployment("checkout", "workload-uid", "baseline")); err != nil {
		t.Fatal(err)
	}
	if err := configMaps.Add(testConfigMap("profiles:\n  baseline:\n    cpuHistogramDecayHalfLife: 1h\n    podUpdateThreshold: 0.1\n")); err != nil {
		t.Fatal(err)
	}
	reconciler := testReconciler(client, deployments, cache.NewStore(cache.MetaNamespaceKeyFunc), configMaps)
	key := WorkloadKey(DeploymentKind, "apps", "checkout")
	if err := reconciler.Reconcile(ctx, key); err != nil {
		t.Fatal(err)
	}
	if actions := client.Actions(); len(actions) != 1 || actions[0].GetVerb() != "create" {
		t.Fatalf("cache miss should issue only a create, got %v", actions)
	}
	name := VPAName(DeploymentKind, "checkout")
	object, err := client.Tracker().Get(vpaGVR, "apps", name)
	if err != nil {
		t.Fatal(err)
	}
	cached := object.(*unstructured.Unstructured)
	cached.SetUID("vpa-uid")
	cached.SetResourceVersion("17")
	cached.Object["status"] = map[string]any{"recommendation": map[string]any{"containerRecommendations": []any{}}}
	cached.SetAnnotations(map[string]string{"unrelated": "keep"})
	if err := reconciler.VPAs.Add(cached); err != nil {
		t.Fatal(err)
	}
	before := cached.DeepCopy()
	client.ClearActions()
	for range 1000 {
		if err := reconciler.Reconcile(ctx, key); err != nil {
			t.Fatal(err)
		}
	}
	if len(client.Actions()) != 0 {
		t.Fatalf("no-op reconciliation must not contact the API server: %v", client.Actions())
	}
	if err := configMaps.Update(testConfigMap("profiles:\n  baseline:\n    cpuHistogramDecayHalfLife: 2h\n    podUpdateThreshold: 0.1\n")); err != nil {
		t.Fatal(err)
	}
	if err := reconciler.Reconcile(ctx, key); err != nil {
		t.Fatal(err)
	}
	actions := client.Actions()
	if len(actions) != 1 || actions[0].GetVerb() != "update" {
		t.Fatalf("profile change should issue only an update, got %v", actions)
	}
	updated := actions[0].(clienttesting.UpdateAction).GetObject().(*unstructured.Unstructured)
	if updated.GetUID() != cached.GetUID() || updated.GetResourceVersion() != "17" ||
		updated.GetAnnotations()["unrelated"] != "keep" ||
		!apiequality.Semantic.DeepEqual(updated.Object["status"], before.Object["status"]) {
		t.Fatalf("update did not preserve cached identity, version, or unrelated fields: %v", updated)
	}
	if !apiequality.Semantic.DeepEqual(cached, before) {
		t.Fatal("reconciliation mutated the shared informer object")
	}
	assertVPAValues(t, updated, DeploymentKind, "checkout", "2h0m0s", 0.1)
}

func TestCacheMissCreateCollisionRechecksOwnership(t *testing.T) {
	client := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme())
	deployments := cache.NewStore(cache.MetaNamespaceKeyFunc)
	configMaps := cache.NewStore(cache.MetaNamespaceKeyFunc)
	if err := deployments.Add(testDeployment("checkout", "workload-uid", "baseline")); err != nil {
		t.Fatal(err)
	}
	if err := configMaps.Add(testConfigMap("profiles:\n  baseline:\n    cpuHistogramDecayHalfLife: 1h\n    podUpdateThreshold: 0.1\n")); err != nil {
		t.Fatal(err)
	}
	existing := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "autoscaling.k8s.io/v1", "kind": "VerticalPodAutoscaler",
		"metadata": map[string]any{"namespace": "apps", "name": VPAName(DeploymentKind, "checkout"), "uid": "unmanaged-uid"},
	}}
	if err := client.Tracker().Create(vpaGVR, existing, "apps"); err != nil {
		t.Fatal(err)
	}
	reconciler := testReconciler(client, deployments, cache.NewStore(cache.MetaNamespaceKeyFunc), configMaps)
	key := WorkloadKey(DeploymentKind, "apps", "checkout")
	if err := reconciler.Reconcile(context.Background(), key); !apierrors.IsAlreadyExists(err) {
		t.Fatalf("cache miss should surface AlreadyExists for a queue retry, got %v", err)
	}
	if actions := client.Actions(); len(actions) != 1 || actions[0].GetVerb() != "create" {
		t.Fatalf("collision must not fall back to API reads or overwrite: %v", actions)
	}
	if err := reconciler.VPAs.Add(existing); err != nil {
		t.Fatal(err)
	}
	client.ClearActions()
	if err := reconciler.Reconcile(context.Background(), key); err == nil || !strings.Contains(err.Error(), "not owned") {
		t.Fatalf("retry must recheck ownership against the refreshed cache, got %v", err)
	}
	if len(client.Actions()) != 0 {
		t.Fatalf("unmanaged object must not be changed: %v", client.Actions())
	}
}

func TestUpdateConflictUsesRefreshedCacheAndOwnership(t *testing.T) {
	for _, owned := range []bool{false, true} {
		t.Run(fmt.Sprintf("replacement-owned-%t", owned), func(t *testing.T) {
			ctx := context.Background()
			client := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme())
			deployments := cache.NewStore(cache.MetaNamespaceKeyFunc)
			configMaps := cache.NewStore(cache.MetaNamespaceKeyFunc)
			if err := deployments.Add(testDeployment("checkout", "workload-uid", "baseline")); err != nil {
				t.Fatal(err)
			}
			if err := configMaps.Add(testConfigMap("profiles:\n  baseline:\n    cpuHistogramDecayHalfLife: 1h\n    podUpdateThreshold: 0.1\n")); err != nil {
				t.Fatal(err)
			}
			cached, err := DesiredVPA("apps", VPAName(DeploymentKind, "checkout"), DeploymentKind, "checkout", "workload-uid", api.ResolvedProfile{})
			if err != nil {
				t.Fatal(err)
			}
			cached.SetUID("old-vpa-uid")
			cached.SetResourceVersion("17")
			replacement := cached.DeepCopy()
			replacement.SetUID("new-vpa-uid")
			replacement.SetResourceVersion("18")
			if !owned {
				replacement.SetLabels(nil)
				replacement.SetOwnerReferences(nil)
			}
			if err := client.Tracker().Create(vpaGVR, replacement, "apps"); err != nil {
				t.Fatal(err)
			}
			reconciler := testReconciler(client, deployments, cache.NewStore(cache.MetaNamespaceKeyFunc), configMaps)
			if err := reconciler.VPAs.Add(cached); err != nil {
				t.Fatal(err)
			}
			client.PrependReactor("update", "verticalpodautoscalers", func(action clienttesting.Action) (bool, runtime.Object, error) {
				object := action.(clienttesting.UpdateAction).GetObject().(*unstructured.Unstructured)
				if object.GetResourceVersion() == "17" {
					if object.GetUID() != cached.GetUID() {
						t.Error("update did not preserve cached UID")
					}
					return true, nil, apierrors.NewConflict(vpaGVR.GroupResource(), cached.GetName(), nil)
				}
				return false, nil, nil
			})
			key := WorkloadKey(DeploymentKind, "apps", "checkout")
			if err := reconciler.Reconcile(ctx, key); !apierrors.IsConflict(err) {
				t.Fatalf("stale update should return Conflict for a queue retry, got %v", err)
			}
			if actions := client.Actions(); len(actions) != 1 || actions[0].GetVerb() != "update" {
				t.Fatalf("conflict should not trigger a GET or unconditional write: %v", actions)
			}
			if !apiequality.Semantic.DeepEqual(cached.Object["spec"], replacement.Object["spec"]) {
				t.Fatal("conflicting update mutated the cache")
			}
			if err := reconciler.VPAs.Update(replacement); err != nil {
				t.Fatal(err)
			}
			client.ClearActions()
			err = reconciler.Reconcile(ctx, key)
			if !owned {
				if err == nil || len(client.Actions()) != 0 {
					t.Fatalf("retry must protect the unowned replacement, err=%v actions=%v", err, client.Actions())
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			actions := client.Actions()
			if len(actions) != 1 || actions[0].GetVerb() != "update" {
				t.Fatalf("expected one update from the refreshed cache, got %v", actions)
			}
			updated := actions[0].(clienttesting.UpdateAction).GetObject().(*unstructured.Unstructured)
			if updated.GetResourceVersion() != "18" || updated.GetUID() != replacement.GetUID() {
				t.Fatalf("retry used stale identity/version: %v", updated)
			}
		})
	}
}

func TestWritesRejectMissingCachedIdentityOrVersion(t *testing.T) {
	for _, missing := range []string{"uid", "resourceVersion"} {
		for _, operation := range []string{"update", "delete"} {
			t.Run(operation+"-missing-"+missing, func(t *testing.T) {
				client := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme())
				reconciler := &Reconciler{DynamicClient: client, VPAs: cache.NewStore(cache.MetaNamespaceKeyFunc)}
				existing, err := DesiredVPA("apps", VPAName(DeploymentKind, "checkout"), DeploymentKind, "checkout", "workload-uid", api.ResolvedProfile{})
				if err != nil {
					t.Fatal(err)
				}
				existing.SetUID("vpa-uid")
				existing.SetResourceVersion("17")
				unstructured.RemoveNestedField(existing.Object, "metadata", missing)
				if err := reconciler.VPAs.Add(existing); err != nil {
					t.Fatal(err)
				}
				if operation == "update" {
					desired := existing.DeepCopy()
					desired.Object["spec"] = map[string]any{"changed": true}
					err = reconciler.applyVPA(context.Background(), "apps", existing.GetName(), desired, DeploymentKind, "checkout", "workload-uid")
				} else {
					err = reconciler.deleteOwnedVPA(context.Background(), "apps", existing.GetName(), DeploymentKind, "checkout", "workload-uid")
				}
				if err == nil || !strings.Contains(err.Error(), "without UID and resourceVersion") {
					t.Fatalf("expected explicit missing metadata error, got %v", err)
				}
				if len(client.Actions()) != 0 {
					t.Fatalf("unsafe write reached the API server: %v", client.Actions())
				}
			})
		}
	}
}

func TestVPANameIsBoundedAndDeterministic(t *testing.T) {
	longName := strings.Join([]string{strings.Repeat("a", 63), strings.Repeat("b", 63), strings.Repeat("c", 63), strings.Repeat("d", 61)}, ".")
	first := VPAName(StatefulSetKind, longName)
	if len(first) > 253 || len(strings.Split(first, ".")[0]) > 63 ||
		first != VPAName(StatefulSetKind, longName) {
		t.Fatalf("invalid or non-deterministic VPA name %q", first)
	}
}

func TestWorkloadKeyPreservesKindAndRejectsMalformedKeys(t *testing.T) {
	key := WorkloadKey(StatefulSetKind, "team-a", "checkout")
	kind, namespace, name, err := ParseWorkloadKey(key)
	if err != nil || kind != StatefulSetKind || namespace != "team-a" || name != "checkout" {
		t.Fatalf("unexpected parsed key %q: %s %s %s %v", key, kind, namespace, name, err)
	}
	if _, _, _, err := ParseWorkloadKey("team-a/checkout"); err == nil {
		t.Fatal("expected legacy ambiguous key to be rejected")
	}
}

func testReconciler(client *dynamicfake.FakeDynamicClient, deployments, statefulSets, configMaps cache.Store) *Reconciler {
	return &Reconciler{
		DynamicClient: client,
		ProfileNS:     "vpa-system",
		ProfileName:   "vpa-profiles",
		ProfileKey:    "profiles.yaml",
		Deployments:   deployments,
		StatefulSets:  statefulSets,
		ConfigMaps:    configMaps,
		VPAs:          cache.NewStore(cache.MetaNamespaceKeyFunc),
	}
}

func testDeployment(name, uid, profile string) *appsv1.Deployment {
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "apps", Name: name, UID: types.UID(uid),
			Annotations: map[string]string{api.ProfileAnnotation: profile},
		},
		Spec: appsv1.DeploymentSpec{Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": name}}},
	}
}

func testConfigMap(profiles string) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Namespace: "vpa-system", Name: "vpa-profiles"},
		Data:       map[string]string{"profiles.yaml": profiles},
	}
}

func assertVPAValues(t *testing.T, vpa *unstructured.Unstructured, kind, target, halfLife string, threshold float64) {
	t.Helper()
	updateMode, found, err := unstructured.NestedString(vpa.Object, "spec", "updatePolicy", "updateMode")
	if err != nil || !found || updateMode != "Recreate" {
		t.Fatalf("unexpected VPA update mode: %q found=%v err=%v", updateMode, found, err)
	}
	targetRef, found, err := unstructured.NestedMap(vpa.Object, "spec", "targetRef")
	if err != nil || !found || targetRef["kind"] != kind || targetRef["name"] != target {
		t.Fatalf("unexpected targetRef: %v, found=%v err=%v", targetRef, found, err)
	}
	policies, found, err := unstructured.NestedSlice(vpa.Object, "spec", "resourcePolicy", "containerPolicies")
	if err != nil || !found || len(policies) != 1 {
		t.Fatalf("unexpected container policies: %v, found=%v err=%v", policies, found, err)
	}
	policy := policies[0].(map[string]any)
	if policy["containerName"] != "*" {
		t.Fatalf("expected wildcard container policy, got %v", policy["containerName"])
	}
	if halfLife != "" && policy["cpuHistogramDecayHalfLife"] != halfLife {
		t.Fatalf("unexpected CPU decay half-life: %v", policy["cpuHistogramDecayHalfLife"])
	}
	if threshold >= 0 {
		value, found, err := unstructured.NestedFloat64(vpa.Object, "spec", "updatePolicy", "podUpdateThreshold")
		if err != nil || !found || value != threshold {
			t.Fatalf("unexpected pod update threshold: %v found=%v err=%v", value, found, err)
		}
	}
}

func assertOwnerUID(t *testing.T, vpa *unstructured.Unstructured, uid string) {
	t.Helper()
	owners := vpa.GetOwnerReferences()
	if len(owners) != 1 || string(owners[0].UID) != uid {
		t.Fatalf("unexpected VPA owner references: %+v", owners)
	}
}
