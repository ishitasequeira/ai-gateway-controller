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

package tenant

import (
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// aitenantFixture builds an unstructured AITenant with status.phase and
// status.gatewayRef. AnnotationPayloadProcessingType is no longer read from
// AITenant (see maastenantconfig_test.go for that), so this fixture no
// longer sets it.
func aitenantFixture(phase, gatewayName, gatewayNamespace string) *unstructured.Unstructured {
	u := NewAITenant()
	status := map[string]any{}
	if phase != "" {
		status["phase"] = phase
	}
	if gatewayName != "" || gatewayNamespace != "" {
		status["gatewayRef"] = map[string]any{"name": gatewayName, "namespace": gatewayNamespace}
	}
	u.Object["status"] = status
	return u
}

func TestNewAITenantSetsGVK(t *testing.T) {
	u := NewAITenant()
	if got := u.GroupVersionKind(); got != AITenantGVK {
		t.Fatalf("GVK = %v, want %v", got, AITenantGVK)
	}
}

func TestIsActive(t *testing.T) {
	cases := []struct {
		name  string
		phase string
		want  bool
	}{
		{"absent phase", "", false},
		{"Pending", "Pending", false},
		{"Active", "Active", true},
		{"Failed", "Failed", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := IsActive(aitenantFixture(c.phase, "", "")); got != c.want {
				t.Errorf("IsActive(phase=%q) = %v, want %v", c.phase, got, c.want)
			}
		})
	}
}

func TestPlatformPrerequisitesReady(t *testing.T) {
	current := NewAITenant()
	current.SetGeneration(3)
	status := map[string]any{
		"conditions": []any{map[string]any{
			"type":               AITenantConditionPlatformPrerequisitesReady,
			"status":             "True",
			"observedGeneration": int64(3),
		}},
	}
	current.Object["status"] = status
	if !PlatformPrerequisitesReady(current) {
		t.Fatal("PlatformPrerequisitesReady = false, want true for a current True condition")
	}

	status["conditions"] = []any{map[string]any{
		"type":               AITenantConditionPlatformPrerequisitesReady,
		"status":             "True",
		"observedGeneration": int64(2),
	}}
	if PlatformPrerequisitesReady(current) {
		t.Fatal("PlatformPrerequisitesReady = true, want false for a stale condition")
	}

	status["conditions"] = []any{map[string]any{
		"type":               AITenantConditionPlatformPrerequisitesReady,
		"status":             "False",
		"observedGeneration": int64(3),
	}}
	if PlatformPrerequisitesReady(current) {
		t.Fatal("PlatformPrerequisitesReady = true, want false for a False condition")
	}
}

// aitenantWithReady builds an AITenant carrying the AITenantConditionReady
// condition maas-controller writes alongside status.phase, with an explicit
// metadata.generation and the condition's observedGeneration, so the
// generation fence can be exercised directly.
func aitenantWithReady(generation, observedGeneration int64, readyStatus string) *unstructured.Unstructured {
	u := NewAITenant()
	u.SetGeneration(generation)
	u.Object["status"] = map[string]any{
		"phase": "Active",
		"conditions": []any{
			map[string]any{
				"type":               AITenantConditionReady,
				"status":             readyStatus,
				"observedGeneration": observedGeneration,
			},
		},
	}
	return u
}

func TestStatusIsCurrent(t *testing.T) {
	t.Run("no conditions", func(t *testing.T) {
		if StatusIsCurrent(aitenantFixture("Active", "", "")) {
			t.Fatal("StatusIsCurrent = true, want false when there are no conditions")
		}
	})

	t.Run("ready observedGeneration matches generation", func(t *testing.T) {
		if !StatusIsCurrent(aitenantWithReady(3, 3, "True")) {
			t.Fatal("StatusIsCurrent = false, want true when Ready observedGeneration == generation")
		}
	})

	t.Run("ready observedGeneration lags generation", func(t *testing.T) {
		if StatusIsCurrent(aitenantWithReady(4, 3, "True")) {
			t.Fatal("StatusIsCurrent = true, want false when Ready observedGeneration < generation")
		}
	})

	t.Run("generation currency is independent of ready status", func(t *testing.T) {
		// StatusIsCurrent gates on generation only; IsActive (phase) covers
		// the True/False dimension. A current-but-False condition is still
		// "current".
		if !StatusIsCurrent(aitenantWithReady(2, 2, "False")) {
			t.Fatal("StatusIsCurrent = false, want true when observedGeneration matches, regardless of condition status")
		}
	})

	t.Run("only a non-ready condition present", func(t *testing.T) {
		u := NewAITenant()
		u.SetGeneration(2)
		u.Object["status"] = map[string]any{
			"conditions": []any{
				map[string]any{"type": "SomethingElse", "observedGeneration": int64(2)},
			},
		}
		if StatusIsCurrent(u) {
			t.Fatal("StatusIsCurrent = true, want false when there is no Ready condition")
		}
	})

	t.Run("observedGeneration decoded as float64", func(t *testing.T) {
		u := NewAITenant()
		u.SetGeneration(5)
		u.Object["status"] = map[string]any{
			"phase": "Active",
			"conditions": []any{
				map[string]any{
					"type":               AITenantConditionReady,
					"status":             "True",
					"observedGeneration": float64(5),
				},
			},
		}
		if !StatusIsCurrent(u) {
			t.Fatal("StatusIsCurrent = false, want true when observedGeneration is a float64 matching generation")
		}
	})
}

func TestGatewayRefNotReadyWhenUnset(t *testing.T) {
	u := aitenantFixture("Active", "", "")
	if _, _, ok := GatewayRef(u); ok {
		t.Fatal("GatewayRef ok = true, want false when status.gatewayRef is unset")
	}
}

func TestGatewayRefNotReadyWhenPartiallySet(t *testing.T) {
	u := aitenantFixture("Active", "my-gateway", "")
	if _, _, ok := GatewayRef(u); ok {
		t.Fatal("GatewayRef ok = true, want false when namespace is missing")
	}
}

func TestGatewayRefReady(t *testing.T) {
	u := aitenantFixture("Active", "my-gateway", "my-namespace")
	name, namespace, ok := GatewayRef(u)
	if !ok {
		t.Fatal("GatewayRef ok = false, want true")
	}
	if name != "my-gateway" || namespace != "my-namespace" {
		t.Fatalf("GatewayRef = (%q, %q), want (%q, %q)", name, namespace, "my-gateway", "my-namespace")
	}
}

func TestConfigNamespace(t *testing.T) {
	t.Run("absent", func(t *testing.T) {
		u := NewAITenant()
		u.Object["status"] = map[string]any{}
		if _, ok := ConfigNamespace(u); ok {
			t.Fatal("ok = true, want false when status.tenantNamespace is unset")
		}
	})

	t.Run("present", func(t *testing.T) {
		u := NewAITenant()
		u.Object["status"] = map[string]any{"tenantNamespace": "ai-tenant-redteam"}
		ns, ok := ConfigNamespace(u)
		if !ok {
			t.Fatal("ok = false, want true")
		}
		if ns != "ai-tenant-redteam" {
			t.Fatalf("namespace = %q, want %q", ns, "ai-tenant-redteam")
		}
	})
}
