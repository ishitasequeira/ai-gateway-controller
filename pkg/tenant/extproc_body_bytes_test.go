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
	"fmt"
	"strings"
	"testing"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestResolveExtProcMaxBodyBytes(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		raw  string
		want int64
		set  bool
	}{
		{name: "absent", want: DefaultExtProcMaxBodyBytes},
		{name: "empty", set: true, raw: "  ", want: DefaultExtProcMaxBodyBytes},
		{name: "valid", set: true, raw: "2097152", want: 2097152},
		{name: "zero", set: true, raw: "0", want: DefaultExtProcMaxBodyBytes},
		{name: "too large", set: true, raw: "33554433", want: DefaultExtProcMaxBodyBytes},
		{name: "not an int", set: true, raw: "10Mi", want: DefaultExtProcMaxBodyBytes},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mtc := &unstructured.Unstructured{Object: map[string]any{
				"apiVersion": "maas.opendatahub.io/v1alpha1",
				"kind":       "MaasTenantConfig",
				"metadata":   map[string]any{"name": "default-tenant", "namespace": "ns"},
			}}
			if tc.set {
				mtc.SetAnnotations(map[string]string{AnnotationExtProcMaxBodyBytes: tc.raw})
			}
			if got := resolveExtProcMaxBodyBytes(mtc, logr.Discard()); got != tc.want {
				t.Fatalf("resolveExtProcMaxBodyBytes() = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestConfigureExtProcMaxBodyBytes(t *testing.T) {
	t.Parallel()
	const oldBody = int64(33554432)
	const wantBody = int64(2097152)
	resources := []unstructured.Unstructured{{
		Object: map[string]any{
			"apiVersion": "v1",
			"kind":       "ConfigMap",
			"metadata":   map[string]any{"name": PayloadProcessingPluginsConfigMapForTenant("red"), "namespace": "gw"},
			"data": map[string]any{
				"pre-extproc.yaml": fmt.Sprintf("server:\n  grpc_address: \"0.0.0.0:9004\"\n  max_body_bytes: %d\nfilter_chains:\n  - filters:\n      - filter: stream_usage_inject\n        max_body_bytes: %d\n",
					oldBody, oldBody),
				"extproc.yaml": fmt.Sprintf("server:\n  max_body_bytes: %d\n", oldBody),
			},
		},
	}}
	if err := configureExtProcMaxBodyBytes(resources, wantBody); err != nil {
		t.Fatalf("configureExtProcMaxBodyBytes: %v", err)
	}
	data, _, _ := unstructured.NestedStringMap(resources[0].Object, "data")
	if !strings.Contains(data["pre-extproc.yaml"], fmt.Sprintf("  max_body_bytes: %d\n", wantBody)) {
		t.Fatalf("pre-extproc.yaml missing server max_body_bytes %d:\n%s", wantBody, data["pre-extproc.yaml"])
	}
	// Filter-level (8-space) max_body_bytes must stay untouched.
	if !strings.Contains(data["pre-extproc.yaml"], fmt.Sprintf("        max_body_bytes: %d\n", oldBody)) {
		t.Fatalf("filter-level max_body_bytes was rewritten:\n%s", data["pre-extproc.yaml"])
	}
	// Post-auth extproc.yaml is owned by the render helpers; leave it alone.
	if !strings.Contains(data["extproc.yaml"], fmt.Sprintf("  max_body_bytes: %d\n", oldBody)) {
		t.Fatalf("extproc.yaml should not be patched by configureExtProcMaxBodyBytes:\n%s", data["extproc.yaml"])
	}
}

func TestConfigureExtProcMaxBodyBytesRequiresPluginConfigMap(t *testing.T) {
	t.Parallel()
	err := configureExtProcMaxBodyBytes([]unstructured.Unstructured{{
		Object: map[string]any{
			"apiVersion": "v1",
			"kind":       "ConfigMap",
			"metadata":   map[string]any{"name": "unrelated", "namespace": "gw"},
			"data":       map[string]any{"extproc.yaml": "server:\n  max_body_bytes: 1\n"},
		},
	}}, DefaultExtProcMaxBodyBytes)
	if err == nil {
		t.Fatal("expected error when no owned plugin ConfigMap is present")
	}
}
