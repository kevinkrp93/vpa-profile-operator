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

package api

import (
	"fmt"
	"time"

	"sigs.k8s.io/yaml"
)

const (
	ProfileAnnotation = "vpa-scale-profile"
	ManagedByLabel    = "app.kubernetes.io/managed-by"
	ManagedByValue    = "vpa-profile-operator"
)

type Profile struct {
	CPUHistogramDecayHalfLife string   `json:"cpuHistogramDecayHalfLife"`
	PodUpdateThreshold        *float64 `json:"podUpdateThreshold"`
}

type ProfileSet struct {
	Profiles map[string]Profile `json:"profiles"`
}

type ResolvedProfile struct {
	CPUHistogramDecayHalfLife time.Duration
	PodUpdateThreshold        *float64
}

func ParseProfiles(content string) (ProfileSet, error) {
	var result ProfileSet
	if err := yaml.UnmarshalStrict([]byte(content), &result); err != nil {
		return ProfileSet{}, fmt.Errorf("decode profiles: %w", err)
	}
	if len(result.Profiles) == 0 {
		return ProfileSet{}, fmt.Errorf("profiles must contain at least one entry")
	}
	for name, profile := range result.Profiles {
		if name == "" {
			return ProfileSet{}, fmt.Errorf("profile name must not be empty")
		}
		if profile.CPUHistogramDecayHalfLife == "" {
			return ProfileSet{}, fmt.Errorf("profile %q must set cpuHistogramDecayHalfLife", name)
		}
		halfLife, err := time.ParseDuration(profile.CPUHistogramDecayHalfLife)
		if err != nil || halfLife <= 0 {
			return ProfileSet{}, fmt.Errorf("profile %q has invalid cpuHistogramDecayHalfLife %q", name, profile.CPUHistogramDecayHalfLife)
		}
		if profile.PodUpdateThreshold == nil {
			return ProfileSet{}, fmt.Errorf("profile %q must set podUpdateThreshold", name)
		}
		if profile.PodUpdateThreshold != nil && *profile.PodUpdateThreshold < 0 {
			return ProfileSet{}, fmt.Errorf("profile %q podUpdateThreshold must be non-negative", name)
		}
	}
	return result, nil
}

func (profiles ProfileSet) Resolve(name string) (ResolvedProfile, error) {
	profile, ok := profiles.Profiles[name]
	if !ok {
		return ResolvedProfile{}, fmt.Errorf("profile %q does not exist", name)
	}
	var result ResolvedProfile
	halfLife, err := time.ParseDuration(profile.CPUHistogramDecayHalfLife)
	if err != nil {
		return ResolvedProfile{}, fmt.Errorf("profile %q has invalid cpuHistogramDecayHalfLife: %w", name, err)
	}
	result.CPUHistogramDecayHalfLife = halfLife
	if profile.PodUpdateThreshold != nil {
		threshold := *profile.PodUpdateThreshold
		result.PodUpdateThreshold = &threshold
	}
	return result, nil
}
