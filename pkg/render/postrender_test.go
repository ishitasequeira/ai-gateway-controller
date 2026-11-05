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

package render

import (
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func testParams() Params {
	return Params{
		Namespace:        "istio-system",
		GatewayName:      "my-gateway",
		Image:            "quay.io/example/praxis-extproc:v1.2.3",
		MaaSAPIRouteName: "maas-api-route",
	}
}

func namespacedResource(kind, name, namespace string) unstructured.Unstructured {
	metadata := map[string]any{"name": name}
	if namespace != "" {
		metadata["namespace"] = namespace
	}
	return unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1",
		"kind":       kind,
		"metadata":   metadata,
	}}
}

// nestedSliceElem reads obj[path...] as a slice and returns element index as
// a map, failing the test if either step is not the expected shape. Kept as
// a helper because unstructured.NestedXxx only walks map keys, not slice
// indices, and the vendored EnvoyFilter fixtures nest lists of patches.
func nestedSliceElem(t *testing.T, obj map[string]any, index int, path ...string) map[string]any {
	t.Helper()
	slice, found, err := unstructured.NestedSlice(obj, path...)
	if err != nil || !found {
		t.Fatalf("NestedSlice(%v) not found: %v", path, err)
	}
	elem, ok := slice[index].(map[string]any)
	if !ok {
		t.Fatalf("element %d at %v is %T, want map[string]any", index, path, slice[index])
	}
	return elem
}

func mustNestedString(t *testing.T, obj map[string]any, path ...string) string {
	t.Helper()
	got, found, err := unstructured.NestedString(obj, path...)
	if err != nil || !found {
		t.Fatalf("NestedString(%v) not found: %v", path, err)
	}
	return got
}

func TestPostRenderDefaultsNamespaceOnUnsetNamespacedResource(t *testing.T) {
	in := []unstructured.Unstructured{namespacedResource("Deployment", "payload-processing", "")}

	out := PostRender(in, testParams())

	if got := out[0].GetNamespace(); got != "istio-system" {
		t.Fatalf("namespace = %q, want %q", got, "istio-system")
	}
}

func TestPostRenderLeavesClusterScopedResourceWithoutNamespace(t *testing.T) {
	in := []unstructured.Unstructured{namespacedResource("ClusterRole", "payload-processing-reader", "")}

	out := PostRender(in, testParams())

	if got := out[0].GetNamespace(); got != "" {
		t.Fatalf("namespace = %q, want empty for cluster-scoped kind", got)
	}
}

func TestPostRenderPreservesAlreadySetNamespace(t *testing.T) {
	in := []unstructured.Unstructured{namespacedResource("Service", "payload-processing", "explicit-ns")}

	out := PostRender(in, testParams())

	if got := out[0].GetNamespace(); got != "explicit-ns" {
		t.Fatalf("namespace = %q, want the pre-set value to survive untouched", got)
	}
}

func TestPostRenderRewritesCompoundKuadrantSubfilterName(t *testing.T) {
	in := []unstructured.Unstructured{{Object: map[string]any{
		"apiVersion": "networking.istio.io/v1alpha3",
		"kind":       "EnvoyFilter",
		"metadata":   map[string]any{"name": "payload-processing"},
		"spec": map[string]any{
			"configPatches": []any{
				map[string]any{
					"patch": map[string]any{
						"match": map[string]any{
							"subFilter": map[string]any{
								"name": "extensions.istio.io/wasmplugin/openshift-ingress.kuadrant-maas-default-gateway",
							},
						},
					},
				},
			},
		},
	}}}

	out := PostRender(in, testParams())

	patch0 := nestedSliceElem(t, out[0].Object, 0, "spec", "configPatches")
	got := mustNestedString(t, patch0, "patch", "match", "subFilter", "name")
	want := "extensions.istio.io/wasmplugin/istio-system.kuadrant-my-gateway"
	if got != want {
		t.Fatalf("subFilter name = %q, want %q", got, want)
	}
}

func TestPostRenderRewritesFQDNSuffix(t *testing.T) {
	in := []unstructured.Unstructured{{Object: map[string]any{
		"apiVersion": "networking.istio.io/v1",
		"kind":       "DestinationRule",
		"metadata":   map[string]any{"name": "payload-processing"},
		"spec": map[string]any{
			"host": "payload-processing.openshift-ingress.svc.cluster.local",
		},
	}}}

	out := PostRender(in, testParams())

	got := mustNestedString(t, out[0].Object, "spec", "host")
	want := "payload-processing.istio-system.svc.cluster.local"
	if got != want {
		t.Fatalf("host = %q, want %q", got, want)
	}
}

func TestPostRenderRewritesGatewayNameLabel(t *testing.T) {
	in := []unstructured.Unstructured{{Object: map[string]any{
		"apiVersion": "networking.istio.io/v1alpha3",
		"kind":       "EnvoyFilter",
		"metadata":   map[string]any{"name": "payload-processing"},
		"spec": map[string]any{
			"workloadSelector": map[string]any{
				"labels": map[string]any{
					"gateway.networking.k8s.io/gateway-name": "maas-default-gateway",
				},
			},
		},
	}}}

	out := PostRender(in, testParams())

	got := mustNestedString(t, out[0].Object, "spec", "workloadSelector", "labels", "gateway.networking.k8s.io/gateway-name")
	if got != "my-gateway" {
		t.Fatalf("gateway-name label = %q, want %q", got, "my-gateway")
	}
}

func TestPostRenderRewritesClusterRoleBindingSubjectNamespace(t *testing.T) {
	in := []unstructured.Unstructured{{Object: map[string]any{
		"apiVersion": "rbac.authorization.k8s.io/v1",
		"kind":       "ClusterRoleBinding",
		"metadata":   map[string]any{"name": "payload-processing-reader"},
		"subjects": []any{
			map[string]any{"kind": "ServiceAccount", "name": "payload-processing", "namespace": "openshift-ingress"},
		},
	}}}

	out := PostRender(in, testParams())

	subject0 := nestedSliceElem(t, out[0].Object, 0, "subjects")
	got := mustNestedString(t, subject0, "namespace")
	if got != "istio-system" {
		t.Fatalf("subject namespace = %q, want %q", got, "istio-system")
	}
	if ns := out[0].GetNamespace(); ns != "" {
		t.Fatalf("ClusterRoleBinding itself must stay cluster-scoped, got namespace %q", ns)
	}
}

func TestPostRenderRewritesRoutePlaceholderPrefix(t *testing.T) {
	in := []unstructured.Unstructured{{Object: map[string]any{
		"apiVersion": "networking.istio.io/v1alpha3",
		"kind":       "EnvoyFilter",
		"metadata":   map[string]any{"name": "payload-processing"},
		"spec": map[string]any{
			"configPatches": []any{
				map[string]any{"match": map[string]any{"routeConfiguration": map[string]any{"vhost": map[string]any{"route": map[string]any{
					"name": "PLACEHOLDER.maas-api-route.0",
				}}}}},
			},
		},
	}}}

	out := PostRender(in, Params{Namespace: "ns", GatewayName: "gw", Image: "img", MaaSAPIRouteName: "custom-route"})

	patch0 := nestedSliceElem(t, out[0].Object, 0, "spec", "configPatches")
	got := mustNestedString(t, patch0, "match", "routeConfiguration", "vhost", "route", "name")
	if got != "custom-route.0" {
		t.Fatalf("route name = %q, want %q", got, "custom-route.0")
	}
}

func TestPostRenderRewritesContainerImage(t *testing.T) {
	in := []unstructured.Unstructured{{Object: map[string]any{
		"apiVersion": "apps/v1",
		"kind":       "Deployment",
		"metadata":   map[string]any{"name": "payload-processing"},
		"spec": map[string]any{
			"template": map[string]any{
				"spec": map[string]any{
					"containers": []any{
						map[string]any{"name": "payload-processing", "image": "praxis-extproc:dev"},
					},
				},
			},
		},
	}}}

	out := PostRender(in, testParams())

	container0 := nestedSliceElem(t, out[0].Object, 0, "spec", "template", "spec", "containers")
	got := mustNestedString(t, container0, "image")
	if got != "quay.io/example/praxis-extproc:v1.2.3" {
		t.Fatalf("image = %q, want %q", got, "quay.io/example/praxis-extproc:v1.2.3")
	}
}

func TestPostRenderDoesNotMutateInputSlice(t *testing.T) {
	in := []unstructured.Unstructured{namespacedResource("Deployment", "payload-processing", "")}

	PostRender(in, testParams())

	if got := in[0].GetNamespace(); got != "" {
		t.Fatalf("input resource was mutated in place: namespace = %q, want empty", got)
	}
}

// TestPostRenderLeavesNoKnownPlaceholders renders the real vendored overlay
// (fetched by hack/scripts/get-manifests.sh) and asserts none of the known
// placeholder literals survive PostRender. It fails loudly if a future
// praxis-extproc manifest revision introduces a placeholder this package
// does not yet know how to rewrite.
func TestPostRenderLeavesNoKnownPlaceholders(t *testing.T) {
	const manifestPath = "../../config/manifests/external-model/overlays/odh"

	rendered, err := Build(manifestPath)
	if err != nil {
		t.Skipf("vendored manifests not present at %s; run hack/scripts/get-manifests.sh first: %v", manifestPath, err)
	}

	out := PostRender(rendered, testParams())

	leftover := []string{
		"openshift-ingress",
		"maas-default-gateway",
		"PLACEHOLDER.maas-api-route",
		"praxis-extproc:dev",
	}
	for i := range out {
		b, err := out[i].MarshalJSON()
		if err != nil {
			t.Fatalf("marshal resource %d: %v", i, err)
		}
		for _, placeholder := range leftover {
			if strings.Contains(string(b), placeholder) {
				t.Errorf("%s %s/%s still contains placeholder %q after PostRender",
					out[i].GetKind(), out[i].GetNamespace(), out[i].GetName(), placeholder)
			}
		}
	}
}
