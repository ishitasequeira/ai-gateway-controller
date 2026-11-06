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

func TestResolveExtProcMaxRequests(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		raw  string
		want int64
		set  bool
	}{
		{name: "absent", want: DefaultExtProcMaxRequests},
		{name: "empty", set: true, raw: "  ", want: DefaultExtProcMaxRequests},
		{name: "valid", set: true, raw: "64", want: 64},
		{name: "zero", set: true, raw: "0", want: DefaultExtProcMaxRequests},
		{name: "too large", set: true, raw: "2048", want: DefaultExtProcMaxRequests},
		{name: "not an int", set: true, raw: "32Mi", want: DefaultExtProcMaxRequests},
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
				mtc.SetAnnotations(map[string]string{AnnotationExtProcMaxRequests: tc.raw})
			}
			if got := resolveExtProcMaxRequests(mtc, logr.Discard()); got != tc.want {
				t.Fatalf("resolveExtProcMaxRequests() = %d, want %d", got, tc.want)
			}
		})
	}
}

func concurrencyClusterPatch(name string) map[string]any {
	return map[string]any{
		"applyTo": "CLUSTER",
		"patch": map[string]any{
			"operation": "ADD",
			"value": map[string]any{
				"name": name,
				"circuit_breakers": map[string]any{
					"thresholds": []any{
						map[string]any{
							"max_connections":      int64(32),
							"max_pending_requests": int64(32),
							"max_requests":         int64(32),
							"max_retries":          int64(3),
						},
					},
				},
				"typed_extension_protocol_options": map[string]any{
					"envoy.extensions.upstreams.http.v3.HttpProtocolOptions": map[string]any{
						"@type": "type.googleapis.com/envoy.extensions.upstreams.http.v3.HttpProtocolOptions",
						"explicit_http_config": map[string]any{
							"http2_protocol_options": map[string]any{
								"max_concurrent_streams": int64(32),
							},
						},
					},
				},
			},
		},
	}
}

func TestConfigureExtProcConcurrencyOverridesBudgetAndDropsMaxRetries(t *testing.T) {
	t.Parallel()
	shared := unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "networking.istio.io/v1alpha3",
		"kind":       "EnvoyFilter",
		"metadata":   map[string]any{"name": PayloadProcessingEnvoyFilterName("redteam"), "namespace": "gw"},
		"spec": map[string]any{
			"configPatches": []any{
				map[string]any{"applyTo": "HTTP_FILTER"},
				concurrencyClusterPatch("payload-pre-processing-extproc"),
				concurrencyClusterPatch("payload-processing-extproc"),
			},
		},
	}}
	external := unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "networking.istio.io/v1alpha3",
		"kind":       "EnvoyFilter",
		"metadata":   map[string]any{"name": PayloadProcessingExternalModelFilterNameForTenant("redteam"), "namespace": "gw"},
		"spec": map[string]any{
			"configPatches": []any{concurrencyClusterPatch("payload-processing-external-model-extproc")},
		},
	}}

	resources := []unstructured.Unstructured{shared, external}
	if err := configureExtProcConcurrency(resources, 64); err != nil {
		t.Fatalf("configureExtProcConcurrency: %v", err)
	}

	assertClusterConcurrency(t, resources[0], "payload-pre-processing-extproc", 64)
	assertClusterConcurrency(t, resources[0], "payload-processing-extproc", 64)
	assertClusterConcurrency(t, resources[1], "payload-processing-external-model-extproc", 64)
}

func assertClusterConcurrency(t *testing.T, filter unstructured.Unstructured, clusterName string, want int64) {
	t.Helper()
	patches, found, err := unstructured.NestedSlice(filter.Object, "spec", "configPatches")
	if err != nil || !found {
		t.Fatalf("configPatches: found=%t err=%v", found, err)
	}
	for _, raw := range patches {
		patch, ok := raw.(map[string]any)
		if !ok || patch["applyTo"] != "CLUSTER" {
			continue
		}
		value := mustMap(t, mustMap(t, patch["patch"])["value"])
		if value["name"] != clusterName {
			continue
		}
		thresholds, _, _ := unstructured.NestedSlice(value, "circuit_breakers", "thresholds")
		if len(thresholds) != 1 {
			t.Fatalf("%s thresholds len = %d, want 1", clusterName, len(thresholds))
		}
		th := mustMap(t, thresholds[0])
		for _, key := range []string{"max_connections", "max_pending_requests", "max_requests"} {
			got, ok := nestedInt64(th, key)
			if !ok || got != want {
				t.Fatalf("%s.%s = %v (ok=%t), want %d", clusterName, key, th[key], ok, want)
			}
		}
		if _, exists := th["max_retries"]; exists {
			t.Fatalf("%s.max_retries still set; want omitted (no retry_policy)", clusterName)
		}
		streams, found, err := unstructured.NestedInt64(value,
			"typed_extension_protocol_options",
			"envoy.extensions.upstreams.http.v3.HttpProtocolOptions",
			"explicit_http_config",
			"http2_protocol_options",
			"max_concurrent_streams",
		)
		if err != nil || !found || streams != want {
			t.Fatalf("%s max_concurrent_streams = %d found=%t err=%v, want %d", clusterName, streams, found, err, want)
		}
		return
	}
	t.Fatalf("cluster %s not found", clusterName)
}
