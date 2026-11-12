package deploy

import (
	"bytes"
	"io"
	"os"
	"slices"
	"testing"

	rbacv1 "k8s.io/api/rbac/v1"
	yamlutil "k8s.io/apimachinery/pkg/util/yaml"
)

func TestOperatorRoleAllowsInformerReadsAndVPAWrites(t *testing.T) {
	manifest, err := os.ReadFile("operator.yaml")
	if err != nil {
		t.Fatal(err)
	}
	decoder := yamlutil.NewYAMLOrJSONDecoder(bytes.NewReader(manifest), 4096)
	for {
		var role rbacv1.ClusterRole
		if err := decoder.Decode(&role); err == io.EOF {
			t.Fatal("operator ClusterRole not found")
		} else if err != nil {
			t.Fatal(err)
		}
		if role.Kind != "ClusterRole" || role.Name != "vpa-profile-operator" {
			continue
		}
		for _, required := range []struct {
			group    string
			resource string
			verbs    []string
		}{
			{"apps", "deployments", []string{"list", "watch"}},
			{"apps", "statefulsets", []string{"list", "watch"}},
			{"", "configmaps", []string{"list", "watch"}},
			{"autoscaling.k8s.io", "verticalpodautoscalers", []string{"list", "watch", "create", "update", "delete"}},
		} {
			for _, verb := range required.verbs {
				allowed := false
				for _, rule := range role.Rules {
					if slices.Contains(rule.APIGroups, required.group) &&
						slices.Contains(rule.Resources, required.resource) &&
						slices.Contains(rule.Verbs, verb) {
						allowed = true
					}
				}
				if !allowed {
					t.Errorf("operator role lacks %s on %s/%s", verb, required.group, required.resource)
				}
			}
		}
		return
	}
}
