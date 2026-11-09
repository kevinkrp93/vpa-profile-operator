package api

import "testing"

func TestParseAndResolveProfiles(t *testing.T) {
	profiles, err := ParseProfiles(`
profiles:
  aggressive:
    cpuHistogramDecayHalfLife: 5m
    podUpdateThreshold: 0.1
`)
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := profiles.Resolve("aggressive")
	if err != nil {
		t.Fatal(err)
	}
	if resolved.CPUHistogramDecayHalfLife.String() != "5m0s" {
		t.Fatalf("unexpected half-life: %s", resolved.CPUHistogramDecayHalfLife)
	}
	if resolved.PodUpdateThreshold == nil || *resolved.PodUpdateThreshold != 0.1 {
		t.Fatalf("unexpected update threshold: %v", resolved.PodUpdateThreshold)
	}
}

func TestParseProfilesRejectsInvalidValues(t *testing.T) {
	for _, document := range []string{
		"profiles: {}",
		"profiles:\n  invalid:\n    cpuHistogramDecayHalfLife: never",
		"profiles:\n  invalid:\n    cpuHistogramDecayHalfLife: 0s",
		"profiles:\n  invalid:\n    podUpdateThreshold: -0.1",
		"profiles:\n  invalid:\n    unknownField: 10",
	} {
		if _, err := ParseProfiles(document); err == nil {
			t.Errorf("expected invalid document to fail: %s", document)
		}
	}
}
