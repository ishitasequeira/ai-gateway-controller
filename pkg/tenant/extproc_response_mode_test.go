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
