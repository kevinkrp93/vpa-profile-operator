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

package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/kevinkrp93/vpa-profile-operator/api"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
)

const (
	DeploymentKind  = "Deployment"
	StatefulSetKind = "StatefulSet"
	kindLabel       = "vpa-profile-operator/workload-kind"
	nameLabel       = "vpa-profile-operator/workload-name"
)

var vpaGVR = schema.GroupVersionResource{
	Group: "autoscaling.k8s.io", Version: "v1", Resource: "verticalpodautoscalers",
}

type Reconciler struct {
	DynamicClient dynamic.Interface
	ProfileNS     string
	ProfileName   string
	ProfileKey    string
	Deployments   cache.Store
	StatefulSets  cache.Store
	ConfigMaps    cache.Store
}

// WorkloadKey makes workloads with the same namespace/name but different kinds
// independent queue items.
func WorkloadKey(kind, namespace, name string) string {
	return kind + "/" + namespace + "/" + name
}

func ParseWorkloadKey(key string) (kind, namespace, name string, err error) {
	parts := strings.SplitN(key, "/", 3)
	if len(parts) != 3 || (parts[0] != DeploymentKind && parts[0] != StatefulSetKind) || parts[1] == "" || parts[2] == "" {
		return "", "", "", fmt.Errorf("invalid workload key %q", key)
	}
	return parts[0], parts[1], parts[2], nil
}

func (r *Reconciler) Reconcile(ctx context.Context, key string) error {
	kind, namespace, name, err := ParseWorkloadKey(key)
	if err != nil {
		return err
	}

	workload, selector, exists, err := r.getWorkload(kind, namespace, name)
	if err != nil {
		return err
	}
	vpaName := VPAName(kind, name)
	if !exists {
		klog.InfoS("Workload is absent from informer cache; leaving VPA cleanup to Kubernetes owner-reference garbage collection",
			"kind", kind, "namespace", namespace, "name", name)
		return nil
	}

	profileName := workload.GetAnnotations()[api.ProfileAnnotation]
	if profileName == "" {
		return r.deleteOwnedVPA(ctx, namespace, vpaName, kind, name, string(workload.GetUID()))
	}
	if selector == nil {
		return fmt.Errorf("%s %s/%s has no pod selector", kind, namespace, name)
	}
	profiles, err := r.getProfiles()
	if err != nil {
		return err
	}
	profile, err := profiles.Resolve(profileName)
	if err != nil {
		return err
	}
	desired, err := DesiredVPA(namespace, vpaName, kind, name, workload.GetUID(), profile)
	if err != nil {
		return err
	}
	return r.applyVPA(ctx, namespace, vpaName, desired, kind, name, string(workload.GetUID()))
}

func (r *Reconciler) getWorkload(kind, namespace, name string) (metav1.Object, *metav1.LabelSelector, bool, error) {
	var store cache.Store
	switch kind {
	case DeploymentKind:
		store = r.Deployments
	case StatefulSetKind:
		store = r.StatefulSets
	default:
		return nil, nil, false, fmt.Errorf("unsupported workload kind %q", kind)
	}
	item, exists, err := store.GetByKey(namespace + "/" + name)
	if err != nil || !exists {
		return nil, nil, exists, err
	}
	switch workload := item.(type) {
	case *appsv1.Deployment:
		return workload, workload.Spec.Selector, true, nil
	case *appsv1.StatefulSet:
		return workload, workload.Spec.Selector, true, nil
	default:
		return nil, nil, false, fmt.Errorf("unexpected %s cache object %T", kind, item)
	}
}

func (r *Reconciler) getProfiles() (api.ProfileSet, error) {
	key := r.ProfileNS + "/" + r.ProfileName
	item, exists, err := r.ConfigMaps.GetByKey(key)
	if err != nil {
		return api.ProfileSet{}, err
	}
	if !exists {
		return api.ProfileSet{}, fmt.Errorf("profile ConfigMap %s not found", key)
	}
	configMap, ok := item.(*corev1.ConfigMap)
	if !ok {
		return api.ProfileSet{}, fmt.Errorf("unexpected ConfigMap cache object %T", item)
	}
	data, exists := configMap.Data[r.ProfileKey]
	if !exists {
		return api.ProfileSet{}, fmt.Errorf("profile ConfigMap %s is missing key %q", key, r.ProfileKey)
	}
	return api.ParseProfiles(data)
}

// VPAName includes kind in the identity so co-named Deployments and
// StatefulSets can each have an independent VPA. Long names are shortened
// deterministically while retaining a hash to avoid collisions.
func VPAName(kind, workloadName string) string {
	suffix := "-deployment"
	if kind == StatefulSetKind {
		suffix = "-statefulset"
	}
	candidate := workloadName + suffix
	lastLabel := candidate
	if lastDot := strings.LastIndexByte(lastLabel, '.'); lastDot >= 0 {
		lastLabel = lastLabel[lastDot+1:]
	}
	if len(candidate) <= 253 && len(lastLabel) <= 63 {
		return candidate
	}
	digest := sha256.Sum256([]byte(candidate))
	firstLabel := strings.SplitN(workloadName, ".", 2)[0]
	hashSuffix := "-" + hex.EncodeToString(digest[:8])
	prefixLength := 63 - len(suffix) - len(hashSuffix)
	if len(firstLabel) > prefixLength {
		firstLabel = firstLabel[:prefixLength]
	}
	return strings.TrimRight(firstLabel, "-") + hashSuffix + suffix
}

func DesiredVPA(namespace, vpaName, targetKind, targetName string, uid types.UID, profile api.ResolvedProfile) (*unstructured.Unstructured, error) {
	if targetKind != DeploymentKind && targetKind != StatefulSetKind {
		return nil, fmt.Errorf("unsupported target kind %q", targetKind)
	}
	if uid == "" {
		return nil, fmt.Errorf("%s %s/%s has no UID", targetKind, namespace, targetName)
	}
	policy := map[string]any{"containerName": "*"}
	if profile.CPUHistogramDecayHalfLife > 0 {
		policy["cpuHistogramDecayHalfLife"] = profile.CPUHistogramDecayHalfLife.String()
	}
	updatePolicy := map[string]any{"updateMode": "Recreate"}
	if profile.PodUpdateThreshold != nil {
		updatePolicy["podUpdateThreshold"] = *profile.PodUpdateThreshold
	}
	vpa := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "autoscaling.k8s.io/v1",
		"kind":       "VerticalPodAutoscaler",
		"metadata": map[string]any{
			"name":      vpaName,
			"namespace": namespace,
			"labels": map[string]any{
				api.ManagedByLabel: api.ManagedByValue,
				kindLabel:          targetKind,
				nameLabel:          workloadNameLabel(targetName),
			},
			"ownerReferences": []any{map[string]any{
				"apiVersion": "apps/v1",
				"kind":       targetKind,
				"name":       targetName,
				"uid":        string(uid),
				"controller": true,
			}},
		},
		"spec": map[string]any{
			"targetRef": map[string]any{
				"apiVersion": "apps/v1",
				"kind":       targetKind,
				"name":       targetName,
			},
			"updatePolicy": updatePolicy,
			"resourcePolicy": map[string]any{
				"containerPolicies": []any{policy},
			},
		},
	}}
	return vpa, nil
}

func (r *Reconciler) applyVPA(ctx context.Context, namespace, name string, desired *unstructured.Unstructured, kind, targetName, uid string) error {
	resource := r.DynamicClient.Resource(vpaGVR).Namespace(namespace)
	current, err := resource.Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		_, err = resource.Create(ctx, desired, metav1.CreateOptions{})
		return err
	}
	if err != nil {
		return err
	}
	if !isOwnedBy(current, kind, targetName, uid) {
		return fmt.Errorf("VPA %s/%s exists but is not owned by %s %s/%s UID %q", namespace, name, kind, namespace, targetName, uid)
	}
	currentSpec, _, _ := unstructured.NestedFieldNoCopy(current.Object, "spec")
	desiredSpec, _, _ := unstructured.NestedFieldNoCopy(desired.Object, "spec")
	currentSpecJSON, _ := json.Marshal(currentSpec)
	desiredSpecJSON, _ := json.Marshal(desiredSpec)
	if string(currentSpecJSON) == string(desiredSpecJSON) {
		return nil
	}
	current.Object["spec"] = desired.Object["spec"]
	_, err = resource.Update(ctx, current, metav1.UpdateOptions{})
	return err
}

func (r *Reconciler) deleteOwnedVPA(ctx context.Context, namespace, name, kind, targetName, uid string) error {
	resource := r.DynamicClient.Resource(vpaGVR).Namespace(namespace)
	current, err := resource.Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if !isOwnedBy(current, kind, targetName, uid) {
		return nil
	}
	err = resource.Delete(ctx, name, metav1.DeleteOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	return err
}

func isOwnedBy(vpa *unstructured.Unstructured, kind, name, uid string) bool {
	if uid == "" {
		return false
	}
	if vpa.GetLabels()[api.ManagedByLabel] != api.ManagedByValue ||
		vpa.GetLabels()[kindLabel] != kind || vpa.GetLabels()[nameLabel] != workloadNameLabel(name) {
		return false
	}
	for _, owner := range vpa.GetOwnerReferences() {
		if owner.APIVersion == "apps/v1" && owner.Kind == kind && owner.Name == name &&
			owner.UID != "" && string(owner.UID) == uid {
			return true
		}
	}
	return false
}

func workloadNameLabel(name string) string {
	digest := sha256.Sum256([]byte(name))
	return "name-" + hex.EncodeToString(digest[:8])
}

func LogReconcileError(key string, err error) {
	klog.ErrorS(err, "Failed reconciling VPA profile", "workload", key)
}
