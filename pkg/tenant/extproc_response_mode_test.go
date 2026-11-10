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

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestResolveExtProcResponseBodyMode(t *testing.T) {
	cases := []struct {
		name string
		set  bool
		raw  string
		want string
	}{
		{name: "absent", want: DefaultExtProcResponseBodyMode},
		{name: "empty", set: true, raw: "  ", want: DefaultExtProcResponseBodyMode},
		{name: "streamed", set: true, raw: "STREAMED", want: "STREAMED"},
		{name: "streamed lower", set: true, raw: "streamed", want: "STREAMED"},
		{name: "none", set: true, raw: "NONE", want: "NONE"},
		{name: "buffered", set: true, raw: "BUFFERED", want: "BUFFERED"},
		{name: "invalid", set: true, raw: "FULL_DUPLEX", want: DefaultExtProcResponseBodyMode},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var mtc *unstructured.Unstructured
			if tc.set || tc.name == "absent" {
				mtc = &unstructured.Unstructured{Object: map[string]any{
					"apiVersion": "maas.opendatahub.io/v1alpha1",
					"kind":       "MaasTenantConfig",
					"metadata":   map[string]any{"name": "default-tenant", "namespace": "models-as-a-service"},
				}}
				if tc.set {
					mtc.SetAnnotations(map[string]string{AnnotationExtProcResponseBodyMode: tc.raw})
				}
			}
			if got := resolveExtProcResponseBodyMode(mtc, logr.Discard()); got != tc.want {
				t.Fatalf("resolveExtProcResponseBodyMode() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestConstrainExtProcResponseBodyMode(t *testing.T) {
	cases := []struct {
		name              string
		mode              string
		apiTranslation    bool
		needsResponseBody bool
		want              string
	}{
		// Default / annotation path with no Output callouts: leave STREAMED alone.
		{name: "default streamed without output callouts", mode: "STREAMED", want: "STREAMED"},
		{name: "buffered unchanged", mode: "BUFFERED", want: "BUFFERED"},
		{name: "none without constraints", mode: "NONE", want: "NONE"},
		{name: "none with apiTranslation falls back to streamed", mode: "NONE", apiTranslation: true, want: DefaultExtProcResponseBodyMode},
		// Output guardrails force BUFFERED even over the STREAMED default / NONE.
		{name: "none with output callouts forces buffered", mode: "NONE", needsResponseBody: true, want: ExtProcResponseBodyModeBuffered},
		{name: "streamed with output callouts forces buffered", mode: "STREAMED", needsResponseBody: true, want: ExtProcResponseBodyModeBuffered},
		{name: "buffered with output callouts stays buffered", mode: "BUFFERED", needsResponseBody: true, want: ExtProcResponseBodyModeBuffered},
		// Output requirement wins if both flags are set (apiTranslation normally
		// rejects Output checks before they remain as response hooks).
		{name: "output callouts win over apiTranslation none fallback", mode: "NONE", apiTranslation: true, needsResponseBody: true, want: ExtProcResponseBodyModeBuffered},
		{name: "apiTranslation alone does not force buffered", mode: "STREAMED", apiTranslation: true, want: "STREAMED"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := constrainExtProcResponseBodyMode(tc.mode, tc.apiTranslation, tc.needsResponseBody, logr.Discard())
			if got != tc.want {
				t.Fatalf("constrainExtProcResponseBodyMode() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestDefaultResponseBodyModeIsStreamedNotBuffered(t *testing.T) {
	if DefaultExtProcResponseBodyMode != "STREAMED" {
		t.Fatalf("DefaultExtProcResponseBodyMode = %q, want STREAMED (BUFFERED only with Output callouts)", DefaultExtProcResponseBodyMode)
	}
	if got := resolveExtProcResponseBodyMode(nil, logr.Discard()); got != "STREAMED" {
		t.Fatalf("resolveExtProcResponseBodyMode(nil) = %q, want STREAMED", got)
	}
	if got := constrainExtProcResponseBodyMode(DefaultExtProcResponseBodyMode, false, false, logr.Discard()); got != "STREAMED" {
		t.Fatalf("constrain without output callouts = %q, want STREAMED", got)
	}
}

func TestCalloutsNeedResponseBody(t *testing.T) {
	if calloutsNeedResponseBody(nil) {
		t.Fatal("empty callouts need response body")
	}
	if calloutsNeedResponseBody([]nemoCallout{{checks: []nemoCheck{{request: true}}}}) {
		t.Fatal("request-only callouts need response body")
	}
	if !calloutsNeedResponseBody([]nemoCallout{{checks: []nemoCheck{{response: true}}}}) {
		t.Fatal("output callouts do not need response body")
	}
}

func TestConfigureSharedExtProcHonorsResponseBodyModeAnnotation(t *testing.T) {
	tenantID := "redteam"
	gatewayNamespace := "openshift-ingress"
	for _, mode := range []string{"NONE", "BUFFERED", "STREAMED"} {
		t.Run(mode, func(t *testing.T) {
			resources := []unstructured.Unstructured{
				{Object: map[string]any{
					"apiVersion": "v1", "kind": "ConfigMap",
					"metadata": map[string]any{"name": PayloadProcessingPluginsConfigMapForTenant(tenantID), "namespace": gatewayNamespace},
					"data":     map[string]any{"extproc.yaml": "old"},
				}},
				{Object: map[string]any{
					"apiVersion": "apps/v1", "kind": "Deployment",
					"metadata": map[string]any{"name": PayloadProcessingDeploymentName(tenantID), "namespace": gatewayNamespace},
					"spec": map[string]any{"template": map[string]any{
						"metadata": map[string]any{"annotations": map[string]any{}},
						"spec":     map[string]any{"containers": []any{map[string]any{"name": "payload-processing"}}},
					}},
				}},
				sharedEnvoyFilterFixture(tenantID, gatewayNamespace),
			}
			if err := configureSharedExtProc(resources, tenantID, gatewayNamespace, false, DefaultExtProcMaxBodyBytes, mode); err != nil {
				t.Fatal(err)
			}
			assertSharedResponseMode(t, resources[2], mode, false)
		})
	}
}

func TestConfigureSharedExtProcRejectsNoneWithAPITranslation(t *testing.T) {
	tenantID := "redteam"
	gatewayNamespace := "openshift-ingress"
	resources := []unstructured.Unstructured{
		{Object: map[string]any{
			"apiVersion": "v1", "kind": "ConfigMap",
			"metadata": map[string]any{"name": PayloadProcessingPluginsConfigMapForTenant(tenantID), "namespace": gatewayNamespace},
			"data":     map[string]any{"extproc.yaml": "old"},
		}},
		{Object: map[string]any{
			"apiVersion": "apps/v1", "kind": "Deployment",
			"metadata": map[string]any{"name": PayloadProcessingDeploymentName(tenantID), "namespace": gatewayNamespace},
			"spec": map[string]any{"template": map[string]any{
				"metadata": map[string]any{"annotations": map[string]any{}},
				"spec":     map[string]any{"containers": []any{map[string]any{"name": "payload-processing"}}},
			}},
		}},
		sharedEnvoyFilterFixture(tenantID, gatewayNamespace),
	}
	mode := constrainExtProcResponseBodyMode("NONE", true, false, logr.Discard())
	if err := configureSharedExtProc(resources, tenantID, gatewayNamespace, true, DefaultExtProcMaxBodyBytes, mode); err != nil {
		t.Fatal(err)
	}
	assertSharedResponseMode(t, resources[2], DefaultExtProcResponseBodyMode, true)
}

func TestConfigureSharedExtProcForcesBufferedWithOutputCallouts(t *testing.T) {
	tenantID := "redteam"
	gatewayNamespace := "openshift-ingress"
	needsBody := calloutsNeedResponseBody([]nemoCallout{
		{checks: []nemoCheck{{name: "jail", response: true}}},
	})
	for _, requested := range []string{"NONE", "STREAMED", DefaultExtProcResponseBodyMode} {
		t.Run(requested, func(t *testing.T) {
			resources := []unstructured.Unstructured{sharedEnvoyFilterFixture(tenantID, gatewayNamespace)}
			mode := constrainExtProcResponseBodyMode(requested, false, needsBody, logr.Discard())
			if err := configureSharedExtProcResponseMode(resources, tenantID, mode, false); err != nil {
				t.Fatal(err)
			}
			assertSharedResponseMode(t, resources[0], ExtProcResponseBodyModeBuffered, false)
		})
	}
}

func TestConfigureSharedExtProcKeepsStreamedDefaultWithoutOutputCallouts(t *testing.T) {
	tenantID := "redteam"
	gatewayNamespace := "openshift-ingress"
	resources := []unstructured.Unstructured{sharedEnvoyFilterFixture(tenantID, gatewayNamespace)}
	mode := constrainExtProcResponseBodyMode(
		DefaultExtProcResponseBodyMode, false, calloutsNeedResponseBody(nil), logr.Discard())
	if err := configureSharedExtProcResponseMode(resources, tenantID, mode, false); err != nil {
		t.Fatal(err)
	}
	assertSharedResponseMode(t, resources[0], DefaultExtProcResponseBodyMode, false)
}
