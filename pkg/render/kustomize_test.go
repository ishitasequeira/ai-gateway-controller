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
	"os"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestNormalizeJSONTypesConvertsIntToInt64(t *testing.T) {
	obj := map[string]any{
		"replicas": 3,
		"nested": map[string]any{
			"port": 9004,
		},
		"list": []any{1, 2, map[string]any{"n": 5}},
	}

	normalizeJSONTypes(obj)

	if _, ok := obj["replicas"].(int64); !ok {
		t.Fatalf("replicas = %T, want int64", obj["replicas"])
	}

	nested, ok := obj["nested"].(map[string]any)
	if !ok {
		t.Fatalf("nested = %T, want map[string]any", obj["nested"])
	}
	if _, ok := nested["port"].(int64); !ok {
		t.Fatalf("nested.port = %T, want int64", nested["port"])
	}

	list, ok := obj["list"].([]any)
	if !ok {
		t.Fatalf("list = %T, want []any", obj["list"])
	}
	if _, ok := list[0].(int64); !ok {
		t.Fatalf("list[0] = %T, want int64", list[0])
	}
	listItem, ok := list[2].(map[string]any)
	if !ok {
		t.Fatalf("list[2] = %T, want map[string]any", list[2])
	}
	if _, ok := listItem["n"].(int64); !ok {
		t.Fatalf("list[2].n = %T, want int64", listItem["n"])
	}
}

// TestRenderKustomizeBuildsVendoredOverlay is a smoke test against the real
// controller-owned composition of the vendored manifest tree and its
// ExternalModel patches. It is skipped, not
// failed, when the manifests have not been fetched yet (e.g. a fresh clone
// before "make get-manifests").
func TestRenderKustomizeBuildsVendoredOverlay(t *testing.T) {
	const manifestPath = "../../config/manifests/external-model/overlays/odh"

	resources, err := Build(manifestPath)
	if err != nil {
		t.Skipf("vendored manifests not present at %s; run hack/scripts/get-manifests.sh first: %v", manifestPath, err)
	}

	wantKinds := map[string]int{
		"ServiceAccount":     1,
		"ClusterRole":        1,
		"ClusterRoleBinding": 1,
		"ConfigMap":          1,
		"Service":            2,
		"Deployment":         2,
		"DestinationRule":    2,
		"EnvoyFilter":        2,
		"NetworkPolicy":      1,
	}
	gotKinds := map[string]int{}
	for _, r := range resources {
		gotKinds[r.GetKind()]++
	}
	for kind, want := range wantKinds {
		if gotKinds[kind] != want {
			t.Errorf("kind %s: got %d resources, want %d (full count map: %v)", kind, gotKinds[kind], want, gotKinds)
		}
	}
}

func TestRenderedExternalModelGatewayHasTenMiBRequestHeadroom(t *testing.T) {
	const manifestPath = "../../config/manifests/external-model/overlays/odh"
	requireVendoredManifests(t, manifestPath)
	resources, err := Build(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	found := 0
	for _, resource := range resources {
		if resource.GetKind() != "EnvoyFilter" || resource.GetName() != "payload-processing-external-model-filters" {
			continue
		}
		patches, _, err := unstructured.NestedSlice(resource.Object, "spec", "configPatches")
		if err != nil {
			t.Fatal(err)
		}
		for _, raw := range patches {
			patch, ok := raw.(map[string]any)
			if !ok {
				continue
			}
			if patch["applyTo"] != "LISTENER" {
				continue
			}
			found++
			context, _, _ := unstructured.NestedString(patch, "match", "context")
			operation, _, _ := unstructured.NestedString(patch, "patch", "operation")
			limit, _, _ := unstructured.NestedInt64(patch, "patch", "value", "per_connection_buffer_limit_bytes")
			if context != "GATEWAY" || operation != "MERGE" || limit != 10*1024*1024+64*1024 {
				t.Fatalf("listener patch = %s/%s/%d, want GATEWAY/MERGE/10551296", context, operation, limit)
			}
		}
	}
	if found != 1 {
		t.Fatalf("listener buffer patches = %d, want exactly one", found)
	}
}

func assertRenderedExtProcResource(t *testing.T, resource map[string]any) string {
	t.Helper()
	name, ok := resource["name"].(string)
	if !ok {
		return ""
	}
	switch name {
	case "envoy.filters.http.ext_proc.ipp", "envoy.filters.http.ext_proc.external-model", "envoy.filters.http.ext_proc.external-model-pre":
	default:
		return ""
	}
	mode, found, err := unstructured.NestedString(resource, "typed_config", "processing_mode", "request_body_mode")
	if err != nil || !found {
		t.Fatalf("%s request_body_mode missing: found=%v err=%v", name, found, err)
	}
	responseMode, _, _ := unstructured.NestedString(resource, "typed_config", "processing_mode", "response_body_mode")
	sendBodyBeforeHeader, sendBodyFound, sendBodyErr := unstructured.NestedBool(resource, "typed_config", "send_body_without_waiting_for_header_response")
	if sendBodyErr != nil {
		t.Fatalf("%s send_body_without_waiting_for_header_response: %v", name, sendBodyErr)
	}
	switch name {
	case "envoy.filters.http.ext_proc.ipp":
		if routeCacheAction, found, err := unstructured.NestedString(resource, "typed_config", "route_cache_action"); err != nil || found {
			t.Fatalf("tenant-independent shared filter must leave route_cache_action unset: value=%q found=%t err=%v", routeCacheAction, found, err)
		}
		if mode != "BUFFERED" || responseMode != "BUFFERED" {
			t.Fatalf("rendered shared post-auth modes = request %q response %q, want main-compatible BUFFERED/BUFFERED defaults", mode, responseMode)
		}
		return "post"
	case "envoy.filters.http.ext_proc.external-model":
		if mode != "NONE" || responseMode != "NONE" {
			t.Fatalf("ExternalModel post-auth modes = request %q response %q, want NONE/NONE", mode, responseMode)
		}
		if sendBodyFound || sendBodyBeforeHeader {
			t.Fatal("default ExternalModel post-auth filter must retain main request ordering with early body delivery unset")
		}
		return "external"
	default:
		failureModeAllow, found, err := unstructured.NestedBool(resource, "typed_config", "failure_mode_allow")
		if mode != "BUFFERED" || err != nil || !found || failureModeAllow {
			t.Fatalf("ExternalModel pre-auth mode=%q failure_mode_allow=%t found=%t err=%v, want BUFFERED/false", mode, failureModeAllow, found, err)
		}
		return "external-pre"
	}
}

func TestRenderedExtProcPreservesBufferedMaaSAndAddsHeaderPhaseExternalModel(t *testing.T) {
	const manifestPath = "../../config/manifests/external-model/overlays/odh"

	requireVendoredManifests(t, manifestPath)
	resources, err := Build(manifestPath)
	if err != nil {
		t.Fatalf("build vendored manifests at %s: %v", manifestPath, err)
	}

	var patches []any
	for i := range resources {
		if resources[i].GetKind() != "EnvoyFilter" {
			continue
		}
		filterPatches, found, err := unstructured.NestedSlice(resources[i].Object, "spec", "configPatches")
		if err != nil || !found {
			t.Fatalf("EnvoyFilter %q configPatches missing: found=%v err=%v", resources[i].GetName(), found, err)
		}
		patches = append(patches, filterPatches...)
	}
	if len(patches) == 0 {
		t.Fatal("rendered overlay has no EnvoyFilter patches")
	}
	var postAuth, externalModel, preAuth, externalModelPre, externalModelDefaultDisabled int
	for _, raw := range patches {
		patch, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		value, ok := patch["patch"].(map[string]any)
		if !ok {
			continue
		}
		if patch["applyTo"] == "HTTP_ROUTE" {
			typed := map[string]any{}
			if value, ok := value["value"].(map[string]any); ok {
				typed, _, _ = unstructured.NestedMap(value, "typed_per_filter_config")
			}
			if disabled, found, _ := unstructured.NestedBool(typed, "envoy.filters.http.ext_proc.external-model", "disabled"); found && disabled {
				vhost, found, err := unstructured.NestedMap(patch, "match", "routeConfiguration", "vhost")
				if err != nil || !found {
					t.Fatalf("ExternalModel default-disable patch missing route match: found=%v err=%v", found, err)
				}
				if _, found := vhost["name"]; found {
					t.Fatalf("ExternalModel default-disable patch must match all virtual hosts: %#v", vhost)
				}
				if action, found, _ := unstructured.NestedString(vhost, "route", "action"); !found || action != "ANY" {
					t.Fatalf("ExternalModel default-disable patch must match every route: %#v", vhost)
				}
			}
			for _, filterName := range []string{
				"envoy.filters.http.ext_proc.external-model",
				"envoy.filters.http.ext_proc.external-model-pre",
			} {
				if disabled, found, _ := unstructured.NestedBool(typed, filterName, "disabled"); found && disabled {
					externalModelDefaultDisabled++
				}
			}
			continue
		}
		resource, ok := value["value"].(map[string]any)
		if !ok {
			continue
		}
		switch assertRenderedExtProcResource(t, resource) {
		case "post":
			postAuth++
		case "external":
			externalModel++
		case "external-pre":
			externalModelPre++
		}
	}
	preAuthResponseNone := assertRenderedIPPPreResponseNone(t, patches)

	for _, raw := range patches {
		patch, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		value, ok := patch["patch"].(map[string]any)
		if !ok {
			continue
		}
		resource, ok := value["value"].(map[string]any)
		if !ok || resource["name"] != "envoy.filters.http.ext_proc.ipp-pre" {
			continue
		}
		mode, found, err := unstructured.NestedString(resource, "typed_config", "processing_mode", "request_body_mode")
		if err != nil || !found {
			t.Fatalf("pre-auth processing mode missing: found=%v err=%v", found, err)
		}
		if mode != "BUFFERED" {
			t.Fatalf("pre-auth request_body_mode = %q, want BUFFERED", mode)
		}
		preAuth++
	}

	if postAuth != 4 || preAuthResponseNone != 4 || externalModel == 0 || preAuth == 0 || externalModelPre == 0 || externalModelDefaultDisabled < 2 {
		t.Fatalf(
			"expected four default-buffered shared post-auth and four ipp-pre response NONE patches, plus the ExternalModel phases and default-disabled patches; "+
				"post=%d ippPreNone=%d external=%d pre=%d externalPre=%d disabled=%d",
			postAuth, preAuthResponseNone, externalModel, preAuth, externalModelPre, externalModelDefaultDisabled)
	}
}

func assertRenderedIPPPreResponseNone(t *testing.T, patches []any) int {
	t.Helper()
	count := 0
	for _, raw := range patches {
		patch, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		value, ok := patch["patch"].(map[string]any)
		if !ok {
			continue
		}
		resource, ok := value["value"].(map[string]any)
		if !ok || resource["name"] != "envoy.filters.http.ext_proc.ipp-pre" {
			continue
		}
		mode, found, err := unstructured.NestedString(resource, "typed_config", "processing_mode", "response_body_mode")
		if err != nil || !found || mode != "NONE" {
			t.Fatalf("pre-auth response_body_mode = %q, found=%t err=%v, want NONE", mode, found, err)
		}
		count++
	}
	return count
}

func requireVendoredManifests(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Stat(path); os.IsNotExist(err) {
		t.Skipf("vendored manifests not present at %s", path)
	} else if err != nil {
		t.Fatalf("inspect vendored manifests: %v", err)
	}
}
