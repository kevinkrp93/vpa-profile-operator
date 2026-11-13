package api

import (
	"strings"
	"testing"
	"time"
)

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
	if resolved.InRecommendationBoundsEvictionLifetimeThreshold != nil {
		t.Fatal("omitted lifetime must remain absent to use the updater's global flag")
	}
}

func TestProfileLifetimeOverrides(t *testing.T) {
	for _, tc := range []struct {
		value string
		valid bool
		want  time.Duration
	}{
		{value: "0s", valid: true},
		{value: "10m", valid: true, want: 10 * time.Minute},
		{value: "-1s"},
		{value: "invalid"},
		{value: ""},
	} {
		t.Run(tc.value, func(t *testing.T) {
			profiles, err := ParseProfiles("profiles:\n  test:\n    cpuHistogramDecayHalfLife: 5m\n    podUpdateThreshold: 0.1\n    inRecommendationBoundsEvictionLifetimeThreshold: \"" + tc.value + "\"\n")
			if !tc.valid {
				if err == nil {
					t.Fatal("expected invalid duration error")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			resolved, err := profiles.Resolve("test")
			if err != nil || resolved.InRecommendationBoundsEvictionLifetimeThreshold == nil ||
				*resolved.InRecommendationBoundsEvictionLifetimeThreshold != tc.want {
				t.Fatalf("unexpected lifetime: %+v, %v", resolved, err)
			}
			*resolved.InRecommendationBoundsEvictionLifetimeThreshold = time.Hour
			again, err := profiles.Resolve("test")
			if err != nil || *again.InRecommendationBoundsEvictionLifetimeThreshold != tc.want {
				t.Fatalf("resolved lifetime changed the profile: %+v, %v", again, err)
			}
		})
	}
}

func TestParseProfilesRejectsInvalidValues(t *testing.T) {
	for _, test := range []struct {
		name     string
		document string
		message  string
	}{
		{"empty-document", "", "at least one"},
		{"empty-profiles", "profiles: {}", "at least one"},
		{"malformed-yaml", "profiles: [", "decode profiles"},
		{"unknown-field", "profiles:\n  invalid:\n    unknownField: 10", "decode profiles"},
		{"empty-profile-name", "profiles:\n  \"\":\n    cpuHistogramDecayHalfLife: 1h\n    podUpdateThreshold: 0.1", "name must not be empty"},
		{"missing-half-life", "profiles:\n  invalid:\n    podUpdateThreshold: 0.1", "must set cpuHistogramDecayHalfLife"},
		{"invalid-half-life", "profiles:\n  invalid:\n    cpuHistogramDecayHalfLife: never\n    podUpdateThreshold: 0.1", "invalid cpuHistogramDecayHalfLife"},
		{"zero-half-life", "profiles:\n  invalid:\n    cpuHistogramDecayHalfLife: 0s\n    podUpdateThreshold: 0.1", "invalid cpuHistogramDecayHalfLife"},
		{"negative-half-life", "profiles:\n  invalid:\n    cpuHistogramDecayHalfLife: -1h\n    podUpdateThreshold: 0.1", "invalid cpuHistogramDecayHalfLife"},
		{"missing-threshold", "profiles:\n  invalid:\n    cpuHistogramDecayHalfLife: 1h", "must set podUpdateThreshold"},
		{"null-threshold", "profiles:\n  invalid:\n    cpuHistogramDecayHalfLife: 1h\n    podUpdateThreshold: null", "must set podUpdateThreshold"},
		{"negative-threshold", "profiles:\n  invalid:\n    cpuHistogramDecayHalfLife: 1h\n    podUpdateThreshold: -0.1", "must be non-negative"},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := ParseProfiles(test.document)
			if err == nil || !strings.Contains(err.Error(), test.message) {
				t.Fatalf("expected error containing %q, got %v", test.message, err)
			}
		})
	}
}

func TestResolveProfilesKeepsSelectionsIndependent(t *testing.T) {
	profiles, err := ParseProfiles(`
profiles:
  conservative:
    cpuHistogramDecayHalfLife: 24h
    podUpdateThreshold: 0.2
  aggressive:
    cpuHistogramDecayHalfLife: 5m
    podUpdateThreshold: 0
`)
	if err != nil {
		t.Fatal(err)
	}
	aggressive, err := profiles.Resolve("aggressive")
	if err != nil {
		t.Fatal(err)
	}
	if aggressive.CPUHistogramDecayHalfLife != 5*time.Minute ||
		aggressive.PodUpdateThreshold == nil || *aggressive.PodUpdateThreshold != 0 {
		t.Fatalf("explicit zero threshold must remain present: %+v", aggressive)
	}
	*aggressive.PodUpdateThreshold = 0.9
	again, err := profiles.Resolve("aggressive")
	if err != nil {
		t.Fatal(err)
	}
	if *again.PodUpdateThreshold != 0 {
		t.Fatal("modifying a resolved profile changed the central profile")
	}
	conservative, err := profiles.Resolve("conservative")
	if err != nil {
		t.Fatal(err)
	}
	if conservative.CPUHistogramDecayHalfLife != 24*time.Hour || *conservative.PodUpdateThreshold != 0.2 {
		t.Fatalf("another profile's values were affected: %+v", conservative)
	}
	if _, err := profiles.Resolve("missing"); err == nil {
		t.Fatal("unknown selection must not silently fall back to another profile")
	}
}

func TestResolveReportsInvalidDurationInConstructedProfile(t *testing.T) {
	profiles := ProfileSet{Profiles: map[string]Profile{
		"invalid": {CPUHistogramDecayHalfLife: "not-a-duration"},
	}}
	if _, err := profiles.Resolve("invalid"); err == nil || !strings.Contains(err.Error(), "invalid cpuHistogramDecayHalfLife") {
		t.Fatalf("expected an explicit duration error, got %v", err)
	}
}
