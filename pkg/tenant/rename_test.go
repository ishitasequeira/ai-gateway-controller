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
	"reflect"
	"strings"
	"testing"

	"github.com/go-openapi/testify/v2/assert"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func deploymentFixture(name, appLabel, serviceAccountName, configMapName string) unstructured.Unstructured {
	return unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "apps/v1",
		"kind":       "Deployment",
		"metadata":   map[string]any{"name": name},
		"spec": map[string]any{
			"selector": map[string]any{
				"matchLabels": map[string]any{"app": appLabel},
			},
			"template": map[string]any{
				"metadata": map[string]any{
					"labels": map[string]any{"app": appLabel},
				},
				"spec": map[string]any{
					"serviceAccountName": serviceAccountName,
					"volumes": []any{
						map[string]any{
							"name":      "plugins-config-volume",
							"configMap": map[string]any{"name": configMapName},
						},
					},
				},
			},
		},
	}}
}

func serviceFixture(name, appLabel string) unstructured.Unstructured {
	return unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1",
		"kind":       "Service",
		"metadata":   map[string]any{"name": name},
		"spec": map[string]any{
			"selector": map[string]any{"app": appLabel},
		},
	}}
}

func TestRenameDefaultTenantDoesNotMutateDeploymentSelector(t *testing.T) {
	in := []unstructured.Unstructured{deploymentFixture(PayloadProcessingName, PayloadProcessingName, PayloadProcessingName, PayloadProcessingPluginsConfigMapName)}

	out, err := Rename(in, "", "istio-system")
	if err != nil {
		t.Fatalf("Rename: %v", err)
	}

	if got := out[0].GetName(); got != PayloadProcessingName {
		t.Fatalf("name = %q, want unchanged %q", got, PayloadProcessingName)
	}
	labels, _, _ := unstructured.NestedStringMap(out[0].Object, "spec", "selector", "matchLabels")
	if len(labels) != 1 || labels["app"] != PayloadProcessingName {
		t.Fatalf("selector.matchLabels = %v, want untouched {app: %q} (immutable on the default Deployment)", labels, PayloadProcessingName)
	}
	// The pod template label is still added even for the default tenant.
	podLabels, _, _ := unstructured.NestedStringMap(out[0].Object, "spec", "template", "metadata", "labels")
	if podLabels[LabelTenantInstance] != PayloadProcessingName {
		t.Fatalf("pod template tenant-instance label = %q, want %q", podLabels[LabelTenantInstance], PayloadProcessingName)
	}
}

func TestRenameNonDefaultTenantPayloadProcessingDeployment(t *testing.T) {
	in := []unstructured.Unstructured{deploymentFixture(PayloadProcessingName, PayloadProcessingName, PayloadProcessingName, PayloadProcessingPluginsConfigMapName)}

	out, err := Rename(in, "redteam", "istio-system")
	if err != nil {
		t.Fatalf("Rename: %v", err)
	}

	const wantName = "payload-processing-redteam"
	if got := out[0].GetName(); got != wantName {
		t.Fatalf("name = %q, want %q", got, wantName)
	}

	selector, _, _ := unstructured.NestedStringMap(out[0].Object, "spec", "selector", "matchLabels")
	if selector["app"] != PayloadProcessingName || selector[LabelTenantInstance] != wantName {
		t.Fatalf("selector.matchLabels = %v, want {app: %q, %s: %q}", selector, PayloadProcessingName, LabelTenantInstance, wantName)
	}

	podLabels, _, _ := unstructured.NestedStringMap(out[0].Object, "spec", "template", "metadata", "labels")
	if podLabels["app"] != PayloadProcessingName || podLabels[LabelTenantInstance] != wantName {
		t.Fatalf("pod template labels = %v, want app preserved plus %s=%q", podLabels, LabelTenantInstance, wantName)
	}

	sa, _, _ := unstructured.NestedString(out[0].Object, "spec", "template", "spec", "serviceAccountName")
	if want := "payload-processing-post-redteam"; sa != want {
		t.Fatalf("serviceAccountName = %q, want the dedicated post-auth %q", sa, want)
	}

	volumes, _, _ := unstructured.NestedSlice(out[0].Object, "spec", "template", "spec", "volumes")
	vol0, ok := volumes[0].(map[string]any)
	if !ok {
		t.Fatalf("volumes[0] is %T, want map[string]any", volumes[0])
	}
	cmRef, ok := vol0["configMap"].(map[string]any)
	if !ok {
		t.Fatalf("volumes[0].configMap is %T, want map[string]any", vol0["configMap"])
	}
	if cmRef["name"] != "payload-processing-plugins-redteam" {
		t.Fatalf("plugins ConfigMap volume ref = %q, want %q", cmRef["name"], "payload-processing-plugins-redteam")
	}
}

func TestRenameNonDefaultTenantPreProcessingDeploymentSharesServiceAccount(t *testing.T) {
	in := []unstructured.Unstructured{deploymentFixture(PayloadPreProcessingName, PayloadPreProcessingName, PayloadProcessingName, PayloadProcessingPluginsConfigMapName)}

	out, err := Rename(in, "redteam", "istio-system")
	if err != nil {
		t.Fatalf("Rename: %v", err)
	}

	if got := out[0].GetName(); got != "payload-pre-processing-redteam" {
		t.Fatalf("name = %q, want %q", got, "payload-pre-processing-redteam")
	}
	sa, _, _ := unstructured.NestedString(out[0].Object, "spec", "template", "spec", "serviceAccountName")
	if sa != "payload-processing-redteam" {
		t.Fatalf("serviceAccountName = %q, want the shared payload-processing SA %q", sa, "payload-processing-redteam")
	}
}

func TestRenameServiceAlwaysGetsTenantInstanceSelector(t *testing.T) {
	in := []unstructured.Unstructured{serviceFixture(PayloadProcessingName, PayloadProcessingName)}

	out, err := Rename(in, "", "istio-system")
	if err != nil {
		t.Fatalf("Rename: %v", err)
	}

	selector, _, _ := unstructured.NestedStringMap(out[0].Object, "spec", "selector")
	if selector["app"] != PayloadProcessingName || selector[LabelTenantInstance] != PayloadProcessingName {
		t.Fatalf("selector = %v, want tenant-instance set even for the default tenant", selector)
	}
}

func TestRenameServiceNonDefaultTenant(t *testing.T) {
	in := []unstructured.Unstructured{serviceFixture(PayloadPreProcessingName, PayloadPreProcessingName)}

	out, err := Rename(in, "redteam", "istio-system")
	if err != nil {
		t.Fatalf("Rename: %v", err)
	}

	if got := out[0].GetName(); got != "payload-pre-processing-redteam" {
		t.Fatalf("name = %q, want %q", got, "payload-pre-processing-redteam")
	}
	selector, _, _ := unstructured.NestedStringMap(out[0].Object, "spec", "selector")
	if selector[LabelTenantInstance] != "payload-pre-processing-redteam" {
		t.Fatalf("selector tenant-instance = %q, want %q", selector[LabelTenantInstance], "payload-pre-processing-redteam")
	}
}

func TestRenameConfigMap(t *testing.T) {
	in := []unstructured.Unstructured{{Object: map[string]any{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata":   map[string]any{"name": PayloadProcessingPluginsConfigMapName},
	}}}

	out, err := Rename(in, "redteam", "istio-system")
	if err != nil {
		t.Fatalf("Rename: %v", err)
	}
	if got := out[0].GetName(); got != "payload-processing-plugins-redteam" {
		t.Fatalf("name = %q, want %q", got, "payload-processing-plugins-redteam")
	}
}

func TestRenameServiceAccount(t *testing.T) {
	in := []unstructured.Unstructured{{Object: map[string]any{
		"apiVersion": "v1",
		"kind":       "ServiceAccount",
		"metadata":   map[string]any{"name": PayloadProcessingName, "namespace": "istio-system"},
		// Anything MaaS sets on the shared account must not reach the
		// post-auth one.
		"secrets": []any{map[string]any{"name": "shared-token"}},
	}}}

	out, err := Rename(in, "redteam", "istio-system")
	if err != nil {
		t.Fatalf("Rename: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("Rename returned %d resources, want the shared and the post-auth ServiceAccount", len(out))
	}
	if got := out[0].GetName(); got != "payload-processing-redteam" {
		t.Fatalf("name = %q, want %q", got, "payload-processing-redteam")
	}
	post := out[1]
	want := map[string]any{
		"apiVersion":                   "v1",
		"kind":                         "ServiceAccount",
		"metadata":                     map[string]any{"name": "payload-processing-post-redteam", "namespace": "istio-system"},
		"automountServiceAccountToken": false,
	}
	if !reflect.DeepEqual(post.Object, want) {
		t.Fatalf("post-auth ServiceAccount = %v, want %v", post.Object, want)
	}
}

func TestRenameNetworkPolicyReplacesPodSelector(t *testing.T) {
	in := []unstructured.Unstructured{{Object: map[string]any{
		"apiVersion": "networking.k8s.io/v1",
		"kind":       "NetworkPolicy",
		"metadata":   map[string]any{"name": PayloadProcessingName},
		"spec": map[string]any{
			"podSelector": map[string]any{
				"matchExpressions": []any{
					map[string]any{"key": "app", "operator": "In", "values": []any{PayloadProcessingName, PayloadPreProcessingName}},
				},
			},
		},
	}}}

	out, err := Rename(in, "redteam", "istio-system")
	if err != nil {
		t.Fatalf("Rename: %v", err)
	}
	if got := out[0].GetName(); got != "payload-processing-redteam" {
		t.Fatalf("name = %q, want %q", got, "payload-processing-redteam")
	}

	expr, found, err := unstructured.NestedSlice(out[0].Object, "spec", "podSelector", "matchExpressions")
	if err != nil || !found {
		t.Fatalf("podSelector.matchExpressions not found: %v", err)
	}
	e0, ok := expr[0].(map[string]any)
	if !ok {
		t.Fatalf("matchExpressions[0] is %T, want map[string]any", expr[0])
	}
	if e0["key"] != LabelTenantInstance {
		t.Fatalf("podSelector matchExpressions[0].key = %v, want %q", e0["key"], LabelTenantInstance)
	}
	values, ok := e0["values"].([]any)
	if !ok {
		t.Fatalf("matchExpressions[0].values is %T, want []any", e0["values"])
	}
	if values[0] != "payload-processing-redteam" || values[1] != "payload-pre-processing-redteam" {
		t.Fatalf("podSelector matchExpressions[0].values = %v, want tenant-scoped deployment names", values)
	}
}

func TestRenameClusterRoleBinding(t *testing.T) {
	in := []unstructured.Unstructured{{Object: map[string]any{
		"apiVersion": "rbac.authorization.k8s.io/v1",
		"kind":       "ClusterRoleBinding",
		"metadata":   map[string]any{"name": PayloadProcessingReaderClusterRoleBindingName},
		"subjects": []any{
			map[string]any{"kind": "ServiceAccount", "name": PayloadProcessingName, "namespace": "istio-system"},
		},
	}}}

	out, err := Rename(in, "redteam", "istio-system")
	if err != nil {
		t.Fatalf("Rename: %v", err)
	}
	if got := out[0].GetName(); got != "payload-processing-reader-redteam" {
		t.Fatalf("name = %q, want %q", got, "payload-processing-reader-redteam")
	}
	subjects, _, _ := unstructured.NestedSlice(out[0].Object, "subjects")
	subject0, ok := subjects[0].(map[string]any)
	if !ok {
		t.Fatalf("subjects[0] is %T, want map[string]any", subjects[0])
	}
	if subject0["name"] != "payload-processing-redteam" {
		t.Fatalf("subjects[0].name = %v, want %q", subject0["name"], "payload-processing-redteam")
	}
}

func TestRenameLeavesClusterRoleUnchanged(t *testing.T) {
	in := []unstructured.Unstructured{{Object: map[string]any{
		"apiVersion": "rbac.authorization.k8s.io/v1",
		"kind":       "ClusterRole",
		"metadata":   map[string]any{"name": PayloadProcessingReaderClusterRoleBindingName},
	}}}

	out, err := Rename(in, "redteam", "istio-system")
	if err != nil {
		t.Fatalf("Rename: %v", err)
	}
	if got := out[0].GetName(); got != PayloadProcessingReaderClusterRoleBindingName {
		t.Fatalf("ClusterRole name = %q, want unchanged %q (shared across tenants)", got, PayloadProcessingReaderClusterRoleBindingName)
	}
}

func destinationRuleFixture(name string) unstructured.Unstructured {
	return unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "networking.istio.io/v1",
		"kind":       "DestinationRule",
		"metadata":   map[string]any{"name": name},
		"spec": map[string]any{
			"host": name + ".istio-system.svc.cluster.local",
			"trafficPolicy": map[string]any{
				"tls": map[string]any{"sni": name + ".istio-system.svc.cluster.local"},
			},
		},
	}}
}

func TestRenameDestinationRulePayloadProcessing(t *testing.T) {
	in := []unstructured.Unstructured{destinationRuleFixture(PayloadProcessingName)}

	out, err := Rename(in, "redteam", "istio-system")
	if err != nil {
		t.Fatalf("Rename: %v", err)
	}
	const want = "payload-processing-redteam"
	if got := out[0].GetName(); got != want {
		t.Fatalf("name = %q, want %q", got, want)
	}
	host, _, _ := unstructured.NestedString(out[0].Object, "spec", "host")
	sni, _, _ := unstructured.NestedString(out[0].Object, "spec", "trafficPolicy", "tls", "sni")
	wantFQDN := want + ".istio-system.svc.cluster.local"
	if host != wantFQDN || sni != wantFQDN {
		t.Fatalf("host/sni = %q/%q, want %q", host, sni, wantFQDN)
	}
}

func TestRenameDestinationRulePreProcessing(t *testing.T) {
	in := []unstructured.Unstructured{destinationRuleFixture(PayloadPreProcessingName)}

	out, err := Rename(in, "redteam", "istio-system")
	if err != nil {
		t.Fatalf("Rename: %v", err)
	}
	const want = "payload-pre-processing-redteam"
	if got := out[0].GetName(); got != want {
		t.Fatalf("name = %q, want %q", got, want)
	}
}

// clusterPatch builds one EnvoyFilter CLUSTER-applyTo configPatch entry for
// clusterName, matching the shape of the vendored envoy-filter.yaml's
// "Dedicated ExtProc upstream clusters" patches.
func clusterPatch(clusterName, address string) map[string]any {
	return map[string]any{
		"applyTo": "CLUSTER",
		"patch": map[string]any{
			"operation": "ADD",
			"value": map[string]any{
				"name": clusterName,
				"transport_socket": map[string]any{
					"typed_config": map[string]any{"sni": address},
				},
				"load_assignment": map[string]any{
					"cluster_name": clusterName,
					"endpoints": []any{
						map[string]any{
							"lb_endpoints": []any{
								map[string]any{
									"endpoint": map[string]any{
										"address": map[string]any{
											"socket_address": map[string]any{
												"address":    address,
												"port_value": int64(9004),
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}
}

func envoyFilterFixture() unstructured.Unstructured {
	return unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "networking.istio.io/v1alpha3",
		"kind":       "EnvoyFilter",
		"metadata":   map[string]any{"name": PayloadProcessingName},
		"spec": map[string]any{
			"configPatches": []any{
				map[string]any{"applyTo": "HTTP_FILTER"}, // unrelated patch, must be left alone
				clusterPatch("payload-pre-processing-extproc", "payload-pre-processing.istio-system.svc.cluster.local"),
				clusterPatch("payload-processing-extproc", "payload-processing.istio-system.svc.cluster.local"),
			},
		},
	}}
}

func TestRenameEnvoyFilterRepointsClusterAddresses(t *testing.T) {
	in := []unstructured.Unstructured{envoyFilterFixture()}

	out, err := Rename(in, "redteam", "istio-system")
	if err != nil {
		t.Fatalf("Rename: %v", err)
	}
	if got := out[0].GetName(); got != "payload-processing-redteam" {
		t.Fatalf("name = %q, want %q", got, "payload-processing-redteam")
	}

	configPatches, _, _ := unstructured.NestedSlice(out[0].Object, "spec", "configPatches")
	if len(configPatches) != 3 {
		t.Fatalf("configPatches length = %d, want 3 (unrelated patch preserved)", len(configPatches))
	}

	preValue := mustMap(t, mustMap(t, mustMap(t, configPatches[1])["patch"])["value"])
	postValue := mustMap(t, mustMap(t, mustMap(t, configPatches[2])["patch"])["value"])

	wantPreFQDN := "payload-pre-processing-redteam.istio-system.svc.cluster.local"
	wantPostFQDN := "payload-processing-redteam.istio-system.svc.cluster.local"

	assertClusterAddress(t, preValue, wantPreFQDN)
	assertClusterAddress(t, postValue, wantPostFQDN)

	// cluster_name / filter literals are intentionally left unsuffixed.
	if preValue["name"] != "payload-pre-processing-extproc" {
		t.Fatalf("pre cluster name = %v, want unsuffixed literal", preValue["name"])
	}
}

func externalModelEnvoyFilterFixture() unstructured.Unstructured {
	return unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "networking.istio.io/v1alpha3",
		"kind":       "EnvoyFilter",
		"metadata":   map[string]any{"name": PayloadProcessingExternalModelFilterName},
		"spec": map[string]any{
			"configPatches": []any{clusterPatch("payload-processing-external-model-extproc", "payload-processing-external-model.istio-system.svc.cluster.local")},
		},
	}}
}

func TestRenameExternalModelEnvoyFilterRepointsClusterAddress(t *testing.T) {
	out, err := Rename([]unstructured.Unstructured{externalModelEnvoyFilterFixture()}, "redteam", "istio-system")
	if err != nil {
		t.Fatalf("Rename: %v", err)
	}
	if got := out[0].GetName(); got != PayloadProcessingExternalModelFilterNameForTenant("redteam") {
		t.Fatalf("name = %q, want %q", got, PayloadProcessingExternalModelFilterNameForTenant("redteam"))
	}
	patches, _, _ := unstructured.NestedSlice(out[0].Object, "spec", "configPatches")
	value := mustMap(t, mustMap(t, mustMap(t, patches[0])["patch"])["value"])
	assertClusterAddress(t, value, "payload-processing-external-model-redteam.istio-system.svc.cluster.local")
}

func assertClusterAddress(t *testing.T, value map[string]any, wantFQDN string) {
	t.Helper()
	sni, _, err := unstructured.NestedString(value, "transport_socket", "typed_config", "sni")
	if err != nil || sni != wantFQDN {
		t.Fatalf("sni = %q (err=%v), want %q", sni, err, wantFQDN)
	}

	loadAssignment := mustMap(t, value["load_assignment"])
	endpoints := mustSlice(t, loadAssignment["endpoints"])
	endpoint0 := mustMap(t, endpoints[0])
	lbEndpoints := mustSlice(t, endpoint0["lb_endpoints"])
	lbEndpoint0 := mustMap(t, lbEndpoints[0])
	ep := mustMap(t, lbEndpoint0["endpoint"])
	address := mustMap(t, ep["address"])
	socketAddress := mustMap(t, address["socket_address"])
	if socketAddress["address"] != wantFQDN {
		t.Fatalf("socket_address.address = %v, want %q", socketAddress["address"], wantFQDN)
	}
}

// mustMap and mustSlice type-assert v, failing the test (rather than
// silently ignoring a bad assertion) if the underlying value has a
// different shape than expected.
func mustMap(t *testing.T, v any) map[string]any {
	t.Helper()
	m, ok := v.(map[string]any)
	if !ok {
		t.Fatalf("value is %T, want map[string]any", v)
	}
	return m
}

func mustSlice(t *testing.T, v any) []any {
	t.Helper()
	s, ok := v.([]any)
	if !ok {
		t.Fatalf("value is %T, want []any", v)
	}
	return s
}

func TestRenameEnvoyFilterMissingClusterPatchErrors(t *testing.T) {
	in := []unstructured.Unstructured{{Object: map[string]any{
		"apiVersion": "networking.istio.io/v1alpha3",
		"kind":       "EnvoyFilter",
		"metadata":   map[string]any{"name": PayloadProcessingName},
		"spec": map[string]any{
			"configPatches": []any{
				clusterPatch("payload-processing-extproc", "payload-processing.istio-system.svc.cluster.local"),
			},
		},
	}}}

	if _, err := Rename(in, "redteam", "istio-system"); err == nil {
		t.Fatal("expected an error when the pre-processing CLUSTER patch is missing")
	}
}

func TestRenameShortensTo63Characters(t *testing.T) {
	// "payload-processing-plugins-" (28 chars) + a 40-char tenant ID = 68,
	// past the Kubernetes 63-character object name limit.
	longTenantID := strings.Repeat("a", 40)
	in := []unstructured.Unstructured{{Object: map[string]any{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata":   map[string]any{"name": PayloadProcessingPluginsConfigMapName},
	}}}

	out, err := Rename(in, longTenantID, "istio-system")
	assert.NoError(t, err, "Rename returned an unexpected error", "err", err)
	assert.Len(t, out, 1, "Rename output isn't of the expected length")
	assert.Len(t, out[0].GetName(), 63, "Rename didn't shorten to 63 characters")
}

func TestRenameDoesNotMutateInputSlice(t *testing.T) {
	in := []unstructured.Unstructured{deploymentFixture(PayloadProcessingName, PayloadProcessingName, PayloadProcessingName, PayloadProcessingPluginsConfigMapName)}

	if _, err := Rename(in, "redteam", "istio-system"); err != nil {
		t.Fatalf("Rename: %v", err)
	}

	if got := in[0].GetName(); got != PayloadProcessingName {
		t.Fatalf("input resource was mutated in place: name = %q, want unchanged %q", got, PayloadProcessingName)
	}
}
