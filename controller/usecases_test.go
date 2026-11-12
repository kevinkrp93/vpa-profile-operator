package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/kevinkrp93/vpa-profile-operator/api"
	appsv1 "k8s.io/api/apps/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	clienttesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
)

const useCaseProfiles = `
profiles:
  conservative:
    cpuHistogramDecayHalfLife: 24h
    podUpdateThreshold: 0.2
  aggressive:
    cpuHistogramDecayHalfLife: 5m
    podUpdateThreshold: 0
`

func TestSupportedWorkloadProfileLifecycle(t *testing.T) {
	for _, kind := range []string{DeploymentKind, StatefulSetKind} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			reconciler, client := newUseCaseReconciler(t)
			workload := addUseCaseWorkload(t, reconciler, kind, "apps", "checkout", "")
			key, name := WorkloadKey(kind, "apps", "checkout"), VPAName(kind, "checkout")
			if err := reconciler.Reconcile(ctx, key); err != nil {
				t.Fatal(err)
			}
			assertNoAPIRequests(t, client)

			workload = addUseCaseWorkload(t, reconciler, kind, "apps", "checkout", "conservative")
			before := workload.(runtime.Object).DeepCopyObject()
			if err := reconciler.Reconcile(ctx, key); err != nil {
				t.Fatal(err)
			}
			assertOnlyVPAWrite(t, client, "create", "apps", name)
			vpa := cacheAPIVPA(t, reconciler, client, "apps", name, "original-vpa-uid", "1")
			assertVPAValues(t, vpa, kind, "checkout", "24h0m0s", 0.2)
			assertOwnerUID(t, vpa, string(workload.GetUID()))
			if !apiequality.Semantic.DeepEqual(workload, before) {
				t.Fatal("reconciliation modified the workload or its pod template")
			}
			client.ClearActions()
			if err := reconciler.Reconcile(ctx, key); err != nil {
				t.Fatal(err)
			}
			assertNoAPIRequests(t, client)

			addUseCaseWorkload(t, reconciler, kind, "apps", "checkout", "aggressive")
			if err := reconciler.Reconcile(ctx, key); err != nil {
				t.Fatal(err)
			}
			assertOnlyVPAWrite(t, client, "update", "apps", name)
			vpa = cacheAPIVPA(t, reconciler, client, "apps", name, "original-vpa-uid", "2")
			assertVPAValues(t, vpa, kind, "checkout", "5m0s", 0)

			drifted := vpa.DeepCopy()
			if err := unstructured.SetNestedField(drifted.Object, float64(0.9), "spec", "updatePolicy", "podUpdateThreshold"); err != nil {
				t.Fatal(err)
			}
			if err := client.Tracker().Update(vpaGVR, drifted, "apps"); err != nil {
				t.Fatal(err)
			}
			if err := reconciler.VPAs.Update(drifted); err != nil {
				t.Fatal(err)
			}
			client.ClearActions()
			if err := reconciler.Reconcile(ctx, key); err != nil {
				t.Fatal(err)
			}
			assertOnlyVPAWrite(t, client, "update", "apps", name)
			vpa = cacheAPIVPA(t, reconciler, client, "apps", name, "original-vpa-uid", "3")
			assertVPAValues(t, vpa, kind, "checkout", "5m0s", 0)

			if err := client.Tracker().Delete(vpaGVR, "apps", name); err != nil {
				t.Fatal(err)
			}
			if err := reconciler.VPAs.Delete(vpa); err != nil {
				t.Fatal(err)
			}
			client.ClearActions()
			if err := reconciler.Reconcile(ctx, key); err != nil {
				t.Fatal(err)
			}
			assertOnlyVPAWrite(t, client, "create", "apps", name)
			vpa = cacheAPIVPA(t, reconciler, client, "apps", name, "recreated-vpa-uid", "4")
			assertVPAValues(t, vpa, kind, "checkout", "5m0s", 0)

			addUseCaseWorkload(t, reconciler, kind, "apps", "checkout", "")
			client.ClearActions()
			if err := reconciler.Reconcile(ctx, key); err != nil {
				t.Fatal(err)
			}
			assertOnlyVPAWrite(t, client, "delete", "apps", name)
			options := client.Actions()[0].(clienttesting.DeleteAction).GetDeleteOptions()
			if options.Preconditions == nil || options.Preconditions.UID == nil ||
				*options.Preconditions.UID != vpa.GetUID() || options.Preconditions.ResourceVersion == nil ||
				*options.Preconditions.ResourceVersion != "4" {
				t.Fatalf("annotation removal did not guard the recreated VPA's identity/version: %+v", options)
			}
			if _, err := client.Tracker().Get(vpaGVR, "apps", name); !apierrors.IsNotFound(err) {
				t.Fatalf("owned VPA still exists after annotation removal: %v", err)
			}
			if err := reconciler.VPAs.Delete(vpa); err != nil {
				t.Fatal(err)
			}
			client.ClearActions()
			if err := reconciler.Reconcile(ctx, key); err != nil {
				t.Fatal(err)
			}
			assertNoAPIRequests(t, client)
		})
	}
}

func TestCentralProfileChangeIsolatesKindsNamespacesAndSelections(t *testing.T) {
	ctx := context.Background()
	reconciler, client := newUseCaseReconciler(t)
	for _, workload := range []struct {
		kind, namespace, profile string
	}{
		{DeploymentKind, "apps", "conservative"},
		{StatefulSetKind, "apps", "aggressive"},
		{DeploymentKind, "reports", "conservative"},
		{StatefulSetKind, "reports", ""},
	} {
		addUseCaseWorkload(t, reconciler, workload.kind, workload.namespace, "same-name", workload.profile)
		key := WorkloadKey(workload.kind, workload.namespace, "same-name")
		if err := reconciler.Reconcile(ctx, key); err != nil {
			t.Fatal(err)
		}
		if workload.profile != "" {
			cacheAPIVPA(t, reconciler, client, workload.namespace, VPAName(workload.kind, "same-name"),
				types.UID(workload.kind+"-"+workload.namespace), "1")
		}
	}
	if err := reconciler.ConfigMaps.Update(testConfigMap(strings.ReplaceAll(
		strings.ReplaceAll(useCaseProfiles, "24h", "48h"), "0.2", "0.3"))); err != nil {
		t.Fatal(err)
	}
	client.ClearActions()
	for _, identity := range []struct{ kind, namespace string }{
		{DeploymentKind, "apps"}, {StatefulSetKind, "apps"},
		{DeploymentKind, "reports"}, {StatefulSetKind, "reports"},
	} {
		if err := reconciler.Reconcile(ctx, WorkloadKey(identity.kind, identity.namespace, "same-name")); err != nil {
			t.Fatal(err)
		}
	}
	actions := client.Actions()
	if len(actions) != 2 {
		t.Fatalf("only the two workloads selecting the changed profile should write, got %v", actions)
	}
	for _, action := range actions {
		if action.GetVerb() != "update" {
			t.Fatalf("profile change must update, not recreate: %v", action)
		}
		vpa := action.(clienttesting.UpdateAction).GetObject().(*unstructured.Unstructured)
		assertVPAValues(t, vpa, DeploymentKind, "same-name", "48h0m0s", 0.3)
		if vpa.GetUID() != types.UID(DeploymentKind+"-"+action.GetNamespace()) {
			t.Fatalf("updated another workload's VPA: %v", vpa)
		}
	}
	object, err := client.Tracker().Get(vpaGVR, "apps", VPAName(StatefulSetKind, "same-name"))
	if err != nil {
		t.Fatal(err)
	}
	assertVPAValues(t, object.(*unstructured.Unstructured), StatefulSetKind, "same-name", "5m0s", 0)
	if _, err := client.Tracker().Get(vpaGVR, "reports", VPAName(StatefulSetKind, "same-name")); !apierrors.IsNotFound(err) {
		t.Fatalf("unannotated workload acquired a VPA: %v", err)
	}
}

func TestInvalidConfigurationPreservesExistingVPAAndRecovers(t *testing.T) {
	for _, test := range []struct {
		name, message string
		mutate        func(*testing.T, *Reconciler, metav1.Object)
	}{
		{"missing-configmap", "not found", func(t *testing.T, reconciler *Reconciler, _ metav1.Object) {
			if err := reconciler.ConfigMaps.Delete(testConfigMap("")); err != nil {
				t.Fatal(err)
			}
		}},
		{"missing-data-key", "missing key", func(t *testing.T, reconciler *Reconciler, _ metav1.Object) {
			configMap := testConfigMap("")
			configMap.Data = nil
			if err := reconciler.ConfigMaps.Update(configMap); err != nil {
				t.Fatal(err)
			}
		}},
		{"malformed-yaml", "decode profiles", func(t *testing.T, reconciler *Reconciler, _ metav1.Object) {
			if err := reconciler.ConfigMaps.Update(testConfigMap("profiles: [")); err != nil {
				t.Fatal(err)
			}
		}},
		{"negative-threshold", "non-negative", func(t *testing.T, reconciler *Reconciler, _ metav1.Object) {
			if err := reconciler.ConfigMaps.Update(testConfigMap(strings.ReplaceAll(useCaseProfiles, "0.2", "-0.2"))); err != nil {
				t.Fatal(err)
			}
		}},
		{"unknown-selection", "does not exist", func(_ *testing.T, _ *Reconciler, workload metav1.Object) {
			workload.SetAnnotations(map[string]string{api.ProfileAnnotation: "undefined"})
		}},
		{"missing-workload-uid", "has no UID", func(_ *testing.T, _ *Reconciler, workload metav1.Object) {
			workload.SetUID("")
		}},
		{"missing-selector", "no pod selector", func(_ *testing.T, _ *Reconciler, workload metav1.Object) {
			workload.(*appsv1.Deployment).Spec.Selector = nil
		}},
	} {
		for _, existing := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/existing-%t", test.name, existing), func(t *testing.T) {
				reconciler, client := newUseCaseReconciler(t)
				workload := addUseCaseWorkload(t, reconciler, DeploymentKind, "apps", "checkout", "conservative")
				key, name := WorkloadKey(DeploymentKind, "apps", "checkout"), VPAName(DeploymentKind, "checkout")
				var before *unstructured.Unstructured
				if existing {
					if err := reconciler.Reconcile(context.Background(), key); err != nil {
						t.Fatal(err)
					}
					before = cacheAPIVPA(t, reconciler, client, "apps", name, "vpa-uid", "1").DeepCopy()
				}
				workload = workload.(runtime.Object).DeepCopyObject().(metav1.Object)
				test.mutate(t, reconciler, workload)
				if err := reconciler.Deployments.Update(workload); err != nil {
					t.Fatal(err)
				}
				client.ClearActions()
				if err := reconciler.Reconcile(context.Background(), key); err == nil || !strings.Contains(err.Error(), test.message) {
					t.Fatalf("expected error containing %q, got %v", test.message, err)
				}
				assertNoAPIRequests(t, client)
				if existing {
					object, err := client.Tracker().Get(vpaGVR, "apps", name)
					if err != nil || !apiequality.Semantic.DeepEqual(object, before) {
						t.Fatalf("invalid configuration modified or deleted the last valid VPA: %v", err)
					}
				}
				if err := reconciler.ConfigMaps.Update(testConfigMap(useCaseProfiles)); err != nil {
					t.Fatal(err)
				}
				addUseCaseWorkload(t, reconciler, DeploymentKind, "apps", "checkout", "conservative")
				if err := reconciler.Reconcile(context.Background(), key); err != nil {
					t.Fatalf("reconciliation did not recover after configuration repair: %v", err)
				}
				if existing {
					assertNoAPIRequests(t, client)
				} else {
					assertOnlyVPAWrite(t, client, "create", "apps", name)
				}
			})
		}
	}
}

func TestOwnershipDriftPreventsUpdateAndDelete(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*unstructured.Unstructured)
	}{
		{"manager-label", func(vpa *unstructured.Unstructured) { vpa.SetLabels(nil) }},
		{"workload-kind-label", func(vpa *unstructured.Unstructured) {
			labels := vpa.GetLabels()
			labels[kindLabel] = StatefulSetKind
			vpa.SetLabels(labels)
		}},
		{"workload-name-label", func(vpa *unstructured.Unstructured) {
			labels := vpa.GetLabels()
			labels[nameLabel] = workloadNameLabel("someone-else")
			vpa.SetLabels(labels)
		}},
		{"owner-missing", func(vpa *unstructured.Unstructured) { vpa.SetOwnerReferences(nil) }},
		{"owner-uid", func(vpa *unstructured.Unstructured) {
			owners := vpa.GetOwnerReferences()
			owners[0].UID = "other-workload-uid"
			vpa.SetOwnerReferences(owners)
		}},
		{"owner-name", func(vpa *unstructured.Unstructured) {
			owners := vpa.GetOwnerReferences()
			owners[0].Name = "someone-else"
			vpa.SetOwnerReferences(owners)
		}},
		{"owner-version", func(vpa *unstructured.Unstructured) {
			owners := vpa.GetOwnerReferences()
			owners[0].APIVersion = "unrelated/v1"
			vpa.SetOwnerReferences(owners)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			reconciler, client := newUseCaseReconciler(t)
			addUseCaseWorkload(t, reconciler, DeploymentKind, "apps", "checkout", "conservative")
			key, name := WorkloadKey(DeploymentKind, "apps", "checkout"), VPAName(DeploymentKind, "checkout")
			if err := reconciler.Reconcile(context.Background(), key); err != nil {
				t.Fatal(err)
			}
			vpa := cacheAPIVPA(t, reconciler, client, "apps", name, "vpa-uid", "1").DeepCopy()
			test.mutate(vpa)
			if err := reconciler.VPAs.Update(vpa); err != nil {
				t.Fatal(err)
			}
			client.ClearActions()
			if err := reconciler.Reconcile(context.Background(), key); err == nil || !strings.Contains(err.Error(), "not owned") {
				t.Fatalf("ownership drift must be reported, got %v", err)
			}
			addUseCaseWorkload(t, reconciler, DeploymentKind, "apps", "checkout", "")
			if err := reconciler.Reconcile(context.Background(), key); err != nil {
				t.Fatal(err)
			}
			assertNoAPIRequests(t, client)
		})
	}
}

func TestReconcilePropagatesWriteFailures(t *testing.T) {
	for _, operation := range []string{"create", "update", "delete"} {
		t.Run(operation, func(t *testing.T) {
			reconciler, client := newUseCaseReconciler(t)
			addUseCaseWorkload(t, reconciler, DeploymentKind, "apps", "checkout", "conservative")
			key, name := WorkloadKey(DeploymentKind, "apps", "checkout"), VPAName(DeploymentKind, "checkout")
			if operation != "create" {
				if err := reconciler.Reconcile(context.Background(), key); err != nil {
					t.Fatal(err)
				}
				cacheAPIVPA(t, reconciler, client, "apps", name, "vpa-uid", "1")
				if operation == "update" {
					addUseCaseWorkload(t, reconciler, DeploymentKind, "apps", "checkout", "aggressive")
				} else {
					addUseCaseWorkload(t, reconciler, DeploymentKind, "apps", "checkout", "")
				}
			}
			rejected := apierrors.NewForbidden(vpaGVR.GroupResource(), name, errors.New("write denied"))
			client.PrependReactor(operation, "verticalpodautoscalers", func(clienttesting.Action) (bool, runtime.Object, error) {
				return true, nil, rejected
			})
			client.ClearActions()
			if err := reconciler.Reconcile(context.Background(), key); !apierrors.IsForbidden(err) {
				t.Fatalf("write failure must be returned to the worker for logging/retry, got %v", err)
			}
			assertOnlyVPAWrite(t, client, operation, "apps", name)
		})
	}
}

func TestDesiredVPARejectsUnsupportedKindsAndMissingUID(t *testing.T) {
	for _, kind := range []string{"Pod", "ReplicaSet", "Job", "DaemonSet"} {
		if _, err := DesiredVPA("apps", "example", kind, "example", "uid", api.ResolvedProfile{}); err == nil {
			t.Errorf("unsupported workload kind %s was silently accepted", kind)
		}
	}
	for _, kind := range []string{DeploymentKind, StatefulSetKind} {
		if _, err := DesiredVPA("apps", "example", kind, "example", "", api.ResolvedProfile{}); err == nil {
			t.Errorf("missing owner UID was accepted for %s", kind)
		}
	}
}

type failedLookupStore struct {
	cache.Store
	err error
}

func (store failedLookupStore) GetByKey(string) (any, bool, error) {
	return nil, false, store.err
}

func TestCacheLookupFailuresDoNotFallBackToAPIReadsOrWrites(t *testing.T) {
	unavailable := errors.New("cache temporarily unavailable")
	for _, test := range []struct {
		name, message string
		mutate        func(*testing.T, *Reconciler)
	}{
		{"workload-cache-error", unavailable.Error(), func(_ *testing.T, reconciler *Reconciler) {
			reconciler.Deployments = failedLookupStore{err: unavailable}
		}},
		{"profile-cache-error", unavailable.Error(), func(_ *testing.T, reconciler *Reconciler) {
			reconciler.ConfigMaps = failedLookupStore{err: unavailable}
		}},
		{"vpa-cache-error", unavailable.Error(), func(_ *testing.T, reconciler *Reconciler) {
			reconciler.VPAs = failedLookupStore{err: unavailable}
		}},
		{"unconfigured-vpa-cache", "not configured", func(_ *testing.T, reconciler *Reconciler) {
			reconciler.VPAs = nil
		}},
		{"unexpected-vpa-object", "unexpected VPA cache object", func(t *testing.T, reconciler *Reconciler) {
			if err := reconciler.VPAs.Add(&appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{Namespace: "apps", Name: VPAName(DeploymentKind, "checkout")},
			}); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			reconciler, client := newUseCaseReconciler(t)
			addUseCaseWorkload(t, reconciler, DeploymentKind, "apps", "checkout", "conservative")
			test.mutate(t, reconciler)
			err := reconciler.Reconcile(context.Background(), WorkloadKey(DeploymentKind, "apps", "checkout"))
			if err == nil || !strings.Contains(err.Error(), test.message) {
				t.Fatalf("cache failure must be surfaced, got %v", err)
			}
			assertNoAPIRequests(t, client)
		})
	}
}

func TestAnnotationRemovalToleratesAnAlreadyDeletedVPA(t *testing.T) {
	reconciler, client := newUseCaseReconciler(t)
	workload := addUseCaseWorkload(t, reconciler, StatefulSetKind, "apps", "checkout", "")
	name := VPAName(StatefulSetKind, "checkout")
	stale, err := DesiredVPA("apps", name, StatefulSetKind, "checkout", workload.GetUID(), api.ResolvedProfile{})
	if err != nil {
		t.Fatal(err)
	}
	stale.SetUID("already-deleted-uid")
	stale.SetResourceVersion("1")
	if err := reconciler.VPAs.Add(stale); err != nil {
		t.Fatal(err)
	}
	if err := reconciler.Reconcile(context.Background(), WorkloadKey(StatefulSetKind, "apps", "checkout")); err != nil {
		t.Fatalf("NotFound from an already-completed deletion must not be retried: %v", err)
	}
	assertOnlyVPAWrite(t, client, "delete", "apps", name)
}

func newUseCaseReconciler(t *testing.T) (*Reconciler, *dynamicfake.FakeDynamicClient) {
	t.Helper()
	client := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme())
	reconciler := testReconciler(client, cache.NewStore(cache.MetaNamespaceKeyFunc),
		cache.NewStore(cache.MetaNamespaceKeyFunc), cache.NewStore(cache.MetaNamespaceKeyFunc))
	if err := reconciler.ConfigMaps.Add(testConfigMap(useCaseProfiles)); err != nil {
		t.Fatal(err)
	}
	return reconciler, client
}

func addUseCaseWorkload(t *testing.T, reconciler *Reconciler, kind, namespace, name, profile string) metav1.Object {
	t.Helper()
	deployment := testDeployment(name, kind+"-"+namespace+"-"+name, profile)
	deployment.Namespace = namespace
	var object metav1.Object = deployment
	store := reconciler.Deployments
	if kind == StatefulSetKind {
		object = &appsv1.StatefulSet{
			ObjectMeta: deployment.ObjectMeta,
			Spec:       appsv1.StatefulSetSpec{Selector: deployment.Spec.Selector},
		}
		store = reconciler.StatefulSets
	}
	if err := store.Update(object); err != nil {
		t.Fatal(err)
	}
	return object
}

func cacheAPIVPA(t *testing.T, reconciler *Reconciler, client *dynamicfake.FakeDynamicClient, namespace, name string, uid types.UID, version string) *unstructured.Unstructured {
	t.Helper()
	object, err := client.Tracker().Get(vpaGVR, namespace, name)
	if err != nil {
		t.Fatal(err)
	}
	vpa := object.(*unstructured.Unstructured).DeepCopy()
	// Fake clients do not assign server metadata or deliver informer events.
	vpa.SetUID(uid)
	vpa.SetResourceVersion(version)
	if err := client.Tracker().Update(vpaGVR, vpa, namespace); err != nil {
		t.Fatal(err)
	}
	if err := reconciler.VPAs.Update(vpa); err != nil {
		t.Fatal(err)
	}
	return vpa
}

func assertNoAPIRequests(t *testing.T, client *dynamicfake.FakeDynamicClient) {
	t.Helper()
	if actions := client.Actions(); len(actions) != 0 {
		t.Fatalf("expected no API requests, got %v", actions)
	}
}

func assertOnlyVPAWrite(t *testing.T, client *dynamicfake.FakeDynamicClient, verb, namespace, name string) {
	t.Helper()
	actions := client.Actions()
	if len(actions) != 1 || actions[0].GetVerb() != verb || actions[0].GetNamespace() != namespace ||
		actions[0].GetResource() != vpaGVR {
		t.Fatalf("expected one VPA %s for %s/%s and no API reads, got %v", verb, namespace, name, actions)
	}
	var actualName string
	switch action := actions[0].(type) {
	case clienttesting.CreateAction:
		actualName = action.GetObject().(metav1.Object).GetName()
	case clienttesting.UpdateAction:
		actualName = action.GetObject().(metav1.Object).GetName()
	case clienttesting.DeleteAction:
		actualName = action.GetName()
	}
	if actualName != name {
		t.Fatalf("write targeted VPA %q, want %q", actualName, name)
	}
}
