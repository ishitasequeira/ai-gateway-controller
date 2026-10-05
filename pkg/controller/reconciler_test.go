package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	v1alpha1 "github.com/opendatahub-io/ai-gateway-controller/api/inference/v1alpha1"
	"github.com/opendatahub-io/ai-gateway-controller/pkg/envelope"
	"github.com/opendatahub-io/ai-gateway-controller/pkg/publisher"
	"github.com/opendatahub-io/ai-gateway-controller/pkg/render"
	"github.com/opendatahub-io/ai-gateway-controller/pkg/resolver"
	"github.com/opendatahub-io/ai-gateway-controller/pkg/tenant"
)

func controllerTestClient(t *testing.T, objects ...client.Object) *Reconciler {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	scheme.AddKnownTypeWithName(schema.GroupVersionKind{Group: "networking.istio.io", Version: "v1alpha3", Kind: "EnvoyFilter"}, &unstructured.Unstructured{})
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&v1alpha1.ExternalModel{}, &v1alpha1.ExternalProvider{}).
		WithObjects(objects...).Build()
	return &Reconciler{Client: fakeClient, APIReader: fakeClient}
}

func TestEnableExternalModelRoutesScopesHeaderPhaseFilterToGeneratedRoutes(t *testing.T) {
	filter := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "networking.istio.io/v1alpha3",
		"kind":       "EnvoyFilter",
		"metadata":   map[string]any{"name": "payload-processing-tenant-a", "namespace": "gateway-system"},
		"spec": map[string]any{"configPatches": []any{
			map[string]any{
				"applyTo": "VIRTUAL_HOST",
				"patch": map[string]any{"value": map[string]any{"typed_per_filter_config": map[string]any{
					externalModelExtProcFilter: map[string]any{"disabled": true},
				}}},
			},
			map[string]any{
				"applyTo": "HTTP_ROUTE",
				"patch": map[string]any{"value": map[string]any{"typed_per_filter_config": map[string]any{
					externalModelExtProcFilter: map[string]any{"overrides": map[string]any{"processing_mode": map[string]any{"request_header_mode": "SEND", "request_body_mode": "NONE"}}},
				}}},
			},
		}},
	}}
	r := controllerTestClient(t, filter)
	var applied unstructured.Unstructured
	r.ApplyResource = func(_ context.Context, _ client.Client, object unstructured.Unstructured) error {
		applied = *object.DeepCopy()
		return nil
	}
	routes := []resolver.Route{{Model: "demo-model", ClientName: "demo", Provider: "provider-a"}}
	if err := r.enableExternalModelRoutes(context.Background(), "tenant-a", "tenant-a", "test-gateway", "gateway-system", routes); err != nil {
		t.Fatal(err)
	}
	if applied.GetName() != tenant.PayloadProcessingExternalModelEnvoyFilterName("tenant-a") {
		t.Fatalf("route EnvoyFilter name = %q, want %q", applied.GetName(), tenant.PayloadProcessingExternalModelEnvoyFilterName("tenant-a"))
	}
	if applied.GetLabels()["app.kubernetes.io/managed-by"] != "ai-gateway-controller" {
		t.Fatalf("route EnvoyFilter labels = %#v, want controller ownership", applied.GetLabels())
	}
	selector, found, err := unstructured.NestedStringMap(applied.Object, "spec", "workloadSelector", "labels")
	if err != nil || !found || selector["gateway.networking.k8s.io/gateway-name"] != "test-gateway" {
		t.Fatalf("route EnvoyFilter workloadSelector = %#v found=%t err=%v, want selected Gateway", selector, found, err)
	}
	patches := nestedSlice(t, applied.Object, "spec", "configPatches")
	for _, raw := range patches {
		patch, ok := raw.(map[string]any)
		if !ok || patch["applyTo"] != "HTTP_ROUTE" {
			continue
		}
		match, _, _ := unstructured.NestedString(patch, "match", "routeConfiguration", "vhost", "route", "name")
		if match == "" {
			continue
		}
		if match != "tenant-a.external-model-demo-model.0" && match != "tenant-a.external-model-demo-model.1" && match != "tenant-a.external-model-demo-model.2" && match != "tenant-a.external-model-demo-model.3" {
			t.Fatalf("unexpected ExternalModel route patch name %q", match)
		}
		overrides, found, err := unstructured.NestedMap(patch, "patch", "value", "typed_per_filter_config", externalModelExtProcFilter, "overrides")
		if err != nil || !found {
			t.Fatalf("route %q overrides=%#v found=%t err=%v, want supported enable override", match, overrides, found, err)
		}
		mode, found, err := unstructured.NestedMap(overrides, "processing_mode")
		if err != nil || !found || mode["request_header_mode"] != "SEND" ||
			mode["request_body_mode"] != "STREAMED" || mode["response_body_mode"] != "NONE" {
			t.Fatalf("route %q processing mode=%#v found=%t err=%v, want SEND/STREAMED", match, mode, found, err)
		}
		sharedDisabled, found, err := unstructured.NestedBool(patch, "patch", "value", "typed_per_filter_config", "envoy.filters.http.ext_proc.ipp", "disabled")
		if err != nil || !found || !sharedDisabled {
			t.Fatalf("route %q shared ipp disabled=%t found=%t err=%v, want disabled", match, sharedDisabled, found, err)
		}
		preOverrides, found, err := unstructured.NestedMap(patch, "patch", "value", "typed_per_filter_config", externalModelPreExtProcFilter, "overrides")
		if err != nil || !found {
			t.Fatalf("route %q pre-auth overrides=%#v found=%t err=%v, want fail-closed ExternalModel pre-auth", match, preOverrides, found, err)
		}
		preMode, found, err := unstructured.NestedMap(preOverrides, "processing_mode")
		if err != nil || !found || preMode["request_body_mode"] != "BUFFERED" {
			t.Fatalf("route %q pre-auth processing mode=%#v found=%t err=%v, want BUFFERED", match, preMode, found, err)
		}
		preDisabled, found, err := unstructured.NestedBool(patch, "patch", "value", "typed_per_filter_config", "envoy.filters.http.ext_proc.ipp-pre", "disabled")
		if err != nil || !found || !preDisabled {
			t.Fatalf("route %q shared pre-auth disabled=%t found=%t err=%v, want disabled", match, preDisabled, found, err)
		}
	}
	// A stale route-level enablement is removed before new route patches are
	// added; otherwise a deleted ExternalModel could leave the header-phase
	// filter active on an unrelated route.
	for _, raw := range patches {
		patch, ok := raw.(map[string]any)
		if !ok || patch["applyTo"] != "HTTP_ROUTE" {
			continue
		}
		name, _, _ := unstructured.NestedString(patch, "match", "routeConfiguration", "vhost", "route", "name")
		if name == "stale.route.0" {
			t.Fatal("stale ExternalModel route patch was retained")
		}
	}
}

func TestEnableExternalModelRoutesDeletesOnlyOwnedRouteFilterWhenEmpty(t *testing.T) {
	owned := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "networking.istio.io/v1alpha3",
		"kind":       "EnvoyFilter",
		"metadata": map[string]any{
			"name":      tenant.PayloadProcessingExternalModelEnvoyFilterName("tenant-a"),
			"namespace": "gateway-system",
			"labels":    map[string]any{"app.kubernetes.io/managed-by": "ai-gateway-controller"},
		},
	}}
	foreign := owned.DeepCopy()
	foreign.SetName("foreign-routes")
	foreign.SetLabels(map[string]string{"app.kubernetes.io/managed-by": "other-controller"})
	r := controllerTestClient(t, owned, foreign)

	if err := r.enableExternalModelRoutes(context.Background(), "tenant-a", "tenant-a", "test-gateway", "gateway-system", nil); err != nil {
		t.Fatal(err)
	}
	var got unstructured.Unstructured
	got.SetGroupVersionKind(schema.GroupVersionKind{Group: "networking.istio.io", Version: "v1alpha3", Kind: "EnvoyFilter"})
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: "gateway-system", Name: owned.GetName()}, &got); !apierrors.IsNotFound(err) {
		t.Fatalf("owned route EnvoyFilter lookup = %v, want NotFound", err)
	}
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: "gateway-system", Name: foreign.GetName()}, &got); err != nil {
		t.Fatalf("foreign route EnvoyFilter was removed: %v", err)
	}
}

func nestedSlice(t *testing.T, object map[string]any, fields ...string) []any {
	t.Helper()
	values, found, err := unstructured.NestedSlice(object, fields...)
	if err != nil || !found {
		t.Fatalf("missing %v: found=%v err=%v", fields, found, err)
	}
	return values
}

func nestedString(t *testing.T, object map[string]any, fields ...string) string {
	t.Helper()
	value, found, err := unstructured.NestedString(object, fields...)
	if err != nil || !found {
		t.Fatalf("missing %v: found=%v err=%v", fields, found, err)
	}
	return value
}

func nestedMapAt(t *testing.T, values []any, index int) map[string]any {
	t.Helper()
	if index < 0 || index >= len(values) {
		t.Fatalf("index %d out of range for %d values", index, len(values))
	}
	value, ok := values[index].(map[string]any)
	if !ok {
		t.Fatalf("value %d has type %T, want map[string]any", index, values[index])
	}
	return value
}

func TestModelHTTPRoutePreservesPathAndBodyRouting(t *testing.T) {
	route := resolver.Route{Model: "model", ClientName: "client-model", Provider: "openai", Endpoint: "api.example.com"}
	obj := modelHTTPRoute(route, "tenant-a", "gateway", "maas-system")
	parentRefs := nestedSlice(t, obj.Object, "spec", "parentRefs")
	parent := nestedMapAt(t, parentRefs, 0)
	if nestedString(t, parent, "namespace") != "maas-system" {
		t.Fatalf("route parent namespace = %q, want maas-system", nestedString(t, parent, "namespace"))
	}
	rules, found, err := unstructured.NestedSlice(obj.Object, "spec", "rules")
	if err != nil || !found || len(rules) != 4 {
		t.Fatalf("expected trusted canonical/body rules and fail-closed fallbacks, got found=%v len=%d err=%v", found, len(rules), err)
	}
	selectedMatches := nestedSlice(t, nestedMapAt(t, rules, 0), "matches")
	selectedHeader := nestedMapAt(t, nestedSlice(t, nestedMapAt(t, selectedMatches, 0), "headers"), 0)
	if nestedString(t, selectedHeader, "name") != "X-AI-Routing-Candidate" || nestedString(t, selectedHeader, "value") != envelope.CandidateStableID("model", "openai") {
		t.Fatalf("trusted provider header = %q=%q", nestedString(t, selectedHeader, "name"), nestedString(t, selectedHeader, "value"))
	}
	pathMatches := nestedSlice(t, nestedMapAt(t, rules, 2), "matches")
	path := nestedString(t, nestedMapAt(t, pathMatches, 0), "path", "value")
	if path != "/tenant-a/client-model" {
		t.Fatalf("path route = %q", path)
	}
	backendRefs := nestedSlice(t, nestedMapAt(t, rules, 2), "backendRefs")
	backend := nestedMapAt(t, backendRefs, 0)
	if nestedString(t, backend, "name") != "provider-selection-required-model" {
		t.Fatalf("backend name = %q, want fail-closed sink", nestedString(t, backend, "name"))
	}
	if port, _, _ := unstructured.NestedInt64(backend, "port"); port != 443 {
		t.Fatalf("backend port = %d, want 443", port)
	}
	if _, found, err := unstructured.NestedString(backend, "namespace"); err != nil || found {
		t.Fatal("route backend must remain in the tenant namespace; unexpected cross-namespace backend reference")
	}
	bodyMatches := nestedSlice(t, nestedMapAt(t, rules, 3), "matches")
	headers := nestedSlice(t, nestedMapAt(t, bodyMatches, 0), "headers")
	name := nestedString(t, nestedMapAt(t, headers, 0), "name")
	value := nestedString(t, nestedMapAt(t, headers, 0), "value")
	if name != "X-Gateway-Model-Name" || value != "client-model" {
		t.Fatalf("body route header = %q=%q", name, value)
	}
	if _, found, err := unstructured.NestedMap(nestedMapAt(t, bodyMatches, 0), "path"); err != nil || found {
		t.Fatalf("body route must intentionally be path-independent: found=%v err=%v", found, err)
	}
}

func TestModelHTTPRouteHasOneTrustedRulePerProviderBeforeFallback(t *testing.T) {
	routes := []resolver.Route{
		{Model: "model", ClientName: "chat", Provider: "provider-b"},
		{Model: "model", ClientName: "chat", Provider: "provider-a"},
	}
	obj := modelHTTPRouteSet(routes, "tenant-a", "gateway", "gateway-system")
	rules := nestedSlice(t, obj.Object, "spec", "rules")
	if len(rules) != 6 {
		t.Fatalf("rules = %d, want two trusted entry rules per provider plus path and body fallback", len(rules))
	}
	for i, providerName := range []string{"provider-b", "provider-a"} {
		wantStableID := envelope.CandidateStableID("model", providerName)
		ruleIndex := i * 2
		matches := nestedSlice(t, nestedMapAt(t, rules, ruleIndex), "matches")
		header := nestedMapAt(t, nestedSlice(t, nestedMapAt(t, matches, 0), "headers"), 0)
		if got := nestedString(t, header, "name"); got != selectedProviderHeader {
			t.Fatalf("rule %d header name = %q, want %q", i, got, selectedProviderHeader)
		}
		if got := nestedString(t, header, "value"); got != wantStableID {
			t.Fatalf("rule %d header value = %q, want %q", i, got, wantStableID)
		}
		backend := nestedMapAt(t, nestedSlice(t, nestedMapAt(t, rules, ruleIndex), "backendRefs"), 0)
		if got := nestedString(t, backend, "name"); got != providerServicePrefix+providerName {
			t.Fatalf("rule %d backend = %q, want %q", i, got, providerServicePrefix+providerName)
		}
	}
}

func TestModelHTTPRouteFailsClosedWithoutTrustedSelection(t *testing.T) {
	routes := []resolver.Route{
		{Model: "model", ClientName: "chat", Provider: "provider-a", Endpoint: "a.example.com"},
		{Model: "model", ClientName: "chat", Provider: "provider-b", Endpoint: "b.example.com"},
	}
	obj := modelHTTPRouteSet(routes, "tenant-a", "gateway", "gateway-system")
	rules := nestedSlice(t, obj.Object, "spec", "rules")
	sinkName := providerSelectionSinkName("model")
	if len(rules) != 6 {
		t.Fatalf("rules = %d, want two selected rules per provider and two sink rules", len(rules))
	}
	for i := 0; i < len(rules)-2; i++ {
		matches := nestedSlice(t, nestedMapAt(t, rules, i), "matches")
		foundSelection := false
		for _, rawMatch := range matches {
			match, ok := rawMatch.(map[string]any)
			if !ok {
				t.Fatalf("provider rule %d match has type %T", i, rawMatch)
			}
			for _, rawHeader := range nestedSlice(t, match, "headers") {
				header, ok := rawHeader.(map[string]any)
				if !ok {
					t.Fatalf("provider rule %d header has type %T", i, rawHeader)
				}
				if header["name"] == selectedProviderHeader {
					foundSelection = true
				}
			}
		}
		if !foundSelection {
			t.Fatalf("provider rule %d lacks an exact trusted selection match", i)
		}
		filters := nestedSlice(t, nestedMapAt(t, rules, i), "filters")
		removed := map[string]bool{}
		for _, rawFilter := range filters {
			filter, ok := rawFilter.(map[string]any)
			if !ok {
				t.Fatalf("provider rule %d filter has type %T", i, rawFilter)
			}
			if filter["type"] != "RequestHeaderModifier" {
				continue
			}
			for _, rawName := range nestedSlice(t, filter, "requestHeaderModifier", "remove") {
				if name, ok := rawName.(string); ok {
					removed[name] = true
				}
			}
		}
		for _, name := range []string{"x-ai-routing-candidate", "x-ai-routing-request-id", "x-ai-routing-revision"} {
			if !removed[name] {
				t.Fatalf("provider rule %d does not remove %s before forwarding", i, name)
			}
		}
	}
	for i := len(rules) - 2; i < len(rules); i++ {
		backend := nestedMapAt(t, nestedSlice(t, nestedMapAt(t, rules, i), "backendRefs"), 0)
		if got := nestedString(t, backend, "name"); got != sinkName {
			t.Fatalf("fallback rule %d backend = %q, want endpoint-less sink %q", i, got, sinkName)
		}
	}
	sink := providerSelectionSink(routes[0], "tenant-a")
	if _, found, err := unstructured.NestedFieldNoCopy(sink.Object, "spec", "selector"); err != nil || found {
		t.Fatalf("sink selector = found=%v err=%v, want no selector and therefore no provider endpoint", found, err)
	}
}

func TestProviderTransportPreservesAuthorityPortAndHostnameSNI(t *testing.T) {
	route := resolver.Route{Provider: "provider", Endpoint: "provider.example.com:8443"}
	service := providerService(route, "tenant-a")
	if got := nestedString(t, service.Object, "spec", "externalName"); got != "provider.example.com" {
		t.Fatalf("ExternalName = %q, want hostname only", got)
	}
	if got := nestedMapAt(t, nestedSlice(t, service.Object, "spec", "ports"), 0)["port"]; got != int64(8443) {
		t.Fatalf("Service port = %v, want 8443", got)
	}
	entry := providerServiceEntry(route, "tenant-a")
	hosts := nestedSlice(t, entry.Object, "spec", "hosts")
	if got, ok := hosts[0].(string); !ok || got != "provider.example.com" {
		t.Fatalf("ServiceEntry host = %q, want hostname only", got)
	}
	destination := providerDestinationRule(route, "tenant-a")
	exportTo := nestedSlice(t, destination.Object, "spec", "exportTo")
	if len(exportTo) != 1 || exportTo[0] != "*" {
		t.Fatalf("DestinationRule exportTo = %#v, want [*]", exportTo)
	}
	if got := nestedString(t, destination.Object, "spec", "host"); got != "provider.example.com" {
		t.Fatalf("DestinationRule host = %q, want hostname only", got)
	}
	if got := nestedString(t, destination.Object, "spec", "trafficPolicy", "tls", "sni"); got != "provider.example.com" {
		t.Fatalf("DestinationRule SNI = %q, want hostname only", got)
	}
}

func TestProviderTransportUsesConfiguredCAFileWithoutDisablingVerification(t *testing.T) {
	route := resolver.Route{Provider: "provider", Endpoint: "provider.example.com", TLSCACertificates: "/etc/istio/provider-ca/ca.crt"}
	destination := providerDestinationRule(route, "tenant-a")
	if got := nestedString(t, destination.Object, "spec", "trafficPolicy", "tls", "caCertificates"); got != "/etc/istio/provider-ca/ca.crt" {
		t.Fatalf("DestinationRule CA file = %q", got)
	}
	if got, found, err := unstructured.NestedBool(destination.Object, "spec", "trafficPolicy", "tls", "insecureSkipVerify"); err != nil || found && got {
		t.Fatalf("provider TLS must retain verification: found=%v value=%v err=%v", found, got, err)
	}
}

func TestProviderDestinationRuleIsolatedPerTenantInSharedGatewayNamespace(t *testing.T) {
	route := resolver.Route{Provider: "provider-a", Endpoint: "provider-a-tenant-b.example.com"}
	defaultRule := providerDestinationRuleForTenant(route, "gateway-system", "models-as-a-service")
	tenantRule := providerDestinationRuleForTenant(route, "gateway-system", "tenant-b")
	if got := defaultRule.GetName(); got != "provider-provider-a" {
		t.Fatalf("default DestinationRule name = %q, want historical name", got)
	}
	if got := tenantRule.GetName(); got != "provider-provider-a-tenant-b" {
		t.Fatalf("tenant DestinationRule name = %q, want tenant-qualified name", got)
	}
	if got := nestedString(t, tenantRule.Object, "spec", "host"); got != "provider-a-tenant-b.example.com" {
		t.Fatalf("tenant DestinationRule host = %q, want tenant endpoint", got)
	}
	if got := nestedString(t, tenantRule.Object, "spec", "trafficPolicy", "tls", "sni"); got != "provider-a-tenant-b.example.com" {
		t.Fatalf("tenant DestinationRule SNI = %q, want hostname-only tenant endpoint", got)
	}
}

func TestProviderEndpointRejectsURLsAndInvalidPorts(t *testing.T) {
	for _, endpoint := range []string{"https://provider.example.com", "provider.example.com:0", "provider.example.com:65536"} {
		if err := validateProviderEndpoint(endpoint); err == nil {
			t.Errorf("validateProviderEndpoint(%q) succeeded, want error", endpoint)
		}
	}
}

func TestCandidateIdentityUsesClientModelName(t *testing.T) {
	route := resolver.Route{
		Model: "external-model-name", ClientName: "client-visible-model",
		Cluster: "provider-provider", Namespace: "tenant-a", Provider: "provider",
		ProviderType: "openai", Endpoint: "api.example.com", APIFormat: "openai-chat",
		AuthType: "apikey", SecretName: "credentials", SecretKey: "api-key",
	}
	set := &resolver.ResolvedRouteSet{Models: []resolver.ModelRoutes{{ModelRef: "tenant-a/external-model-name", Routes: []resolver.Route{route}}}}
	env, err := envelope.Render(set, envelope.Scope{Network: "network", Gateway: "gateway", Namespace: "tenant-a", LocalSite: "local"}, envelope.Revision{}, envelope.Options{KnownClusters: []string{"provider-provider"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(env.Overlay.Candidates) != 1 || env.Overlay.Candidates[0].Name != "client-visible-model" {
		t.Fatalf("candidate identity = %#v, want client-visible-model", env.Overlay.Candidates)
	}
	wantStableID := envelope.CandidateStableID("external-model-name", "provider")
	if env.Overlay.Candidates[0].StableID != wantStableID {
		t.Fatalf("candidate StableID = %q, want %q", env.Overlay.Candidates[0].StableID, wantStableID)
	}
	obj := modelHTTPRoute(route, "tenant-a", "gateway", "tenant-a")
	rules := nestedSlice(t, obj.Object, "spec", "rules")
	path := nestedString(t, nestedMapAt(t, nestedSlice(t, nestedMapAt(t, rules, 2), "matches"), 0), "path", "value")
	if path != "/tenant-a/client-visible-model" {
		t.Fatalf("HTTPRoute path = %q, want client-visible-model path", path)
	}
}

func TestTwoModelsSharedProviderProduceDistinctOverlayCandidates(t *testing.T) {
	routeA := resolver.Route{
		Model: "model-a", ClientName: "model-a", Namespace: "tenant-a",
		Provider: "shared-provider", ProviderType: "openai",
		Cluster: "provider-shared-provider", Endpoint: "api.example.com",
		APIFormat: "openai-chat", AuthType: "apikey",
		SecretName: "credentials", SecretKey: "api-key",
	}
	routeB := resolver.Route{
		Model: "model-b", ClientName: "model-b", Namespace: "tenant-a",
		Provider: "shared-provider", ProviderType: "openai",
		Cluster: "provider-shared-provider", Endpoint: "api.example.com",
		APIFormat: "openai-chat", AuthType: "apikey",
		SecretName: "credentials", SecretKey: "api-key",
	}
	set := &resolver.ResolvedRouteSet{Models: []resolver.ModelRoutes{
		{ModelRef: "tenant-a/model-a", Routes: []resolver.Route{routeA}},
		{ModelRef: "tenant-a/model-b", Routes: []resolver.Route{routeB}},
	}}
	env, err := envelope.Render(set, envelope.Scope{
		Network: "network", Gateway: "gateway", Namespace: "tenant-a", LocalSite: "local",
	}, envelope.Revision{}, envelope.Options{KnownClusters: []string{"provider-shared-provider"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(env.Overlay.Candidates) != 2 {
		t.Fatalf("candidates = %d, want 2", len(env.Overlay.Candidates))
	}
	idA := env.Overlay.Candidates[0].StableID
	idB := env.Overlay.Candidates[1].StableID
	if idA == idB {
		t.Fatalf("two candidates sharing a provider have the same stable_id %q; Praxis will reject the overlay", idA)
	}
	if env.Overlay.Candidates[0].Cluster != env.Overlay.Candidates[1].Cluster {
		t.Fatalf("candidates should share the same Cluster (transport); got %q and %q",
			env.Overlay.Candidates[0].Cluster, env.Overlay.Candidates[1].Cluster)
	}

	// Verify HTTPRoute header matches use the model-scoped stable_id, not
	// the bare provider name.
	objA := modelHTTPRoute(routeA, "tenant-a", "gateway", "tenant-a")
	rulesA := nestedSlice(t, objA.Object, "spec", "rules")
	headerA := nestedMapAt(t, nestedSlice(t, nestedMapAt(t, nestedSlice(t, nestedMapAt(t, rulesA, 0), "matches"), 0), "headers"), 0)
	if got := nestedString(t, headerA, "value"); got != idA {
		t.Fatalf("model-a HTTPRoute header value = %q, want overlay stable_id %q", got, idA)
	}
	objB := modelHTTPRoute(routeB, "tenant-a", "gateway", "tenant-a")
	rulesB := nestedSlice(t, objB.Object, "spec", "rules")
	headerB := nestedMapAt(t, nestedSlice(t, nestedMapAt(t, nestedSlice(t, nestedMapAt(t, rulesB, 0), "matches"), 0), "headers"), 0)
	if got := nestedString(t, headerB, "value"); got != idB {
		t.Fatalf("model-b HTTPRoute header value = %q, want overlay stable_id %q", got, idB)
	}
}

func TestValidateProviderAuthenticationStrategies(t *testing.T) {
	tests := []struct {
		name       string
		authType   string
		secretData map[string][]byte
		want       string
	}{
		{name: "apikey", authType: "apikey", secretData: map[string][]byte{"api-key": []byte("fixture")}},
		{name: "sigv4 unsupported", authType: "sigv4", secretData: map[string][]byte{"api-key": []byte("fixture")}, want: "unsupported authentication strategy"},
		{name: "oauth2 unsupported", authType: "oauth2", secretData: map[string][]byte{"api-key": []byte("fixture")}, want: "unsupported authentication strategy"},
		{name: "unknown unsupported", authType: "custom", secretData: map[string][]byte{"api-key": []byte("fixture")}, want: "unsupported authentication strategy"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			provider := &v1alpha1.ExternalProvider{ObjectMeta: metav1.ObjectMeta{Name: "provider", Namespace: "tenant-a"}, Spec: v1alpha1.ExternalProviderSpec{Endpoint: "provider.example.com",
				Auth: v1alpha1.AuthConfig{Type: tc.authType, SecretRef: v1alpha1.NameReference{Name: "credentials"}},
			}}
			secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "credentials", Namespace: "tenant-a"}, Data: tc.secretData}
			r := controllerTestClient(t, provider, secret)
			err := r.validateProvider(context.Background(), provider)
			if tc.want == "" {
				if err != nil {
					t.Fatalf("validateProvider() = %v, want nil", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("validateProvider() = %v, want error containing %q", err, tc.want)
			}
		})
	}
}

func TestDependentEventsEnqueueOnlyAffectedNamespaceModels(t *testing.T) {
	modelA := &v1alpha1.ExternalModel{
		ObjectMeta: metav1.ObjectMeta{Name: "a", Namespace: "tenant-a"},
		Spec:       v1alpha1.ExternalModelSpec{ExternalProviderRefs: []v1alpha1.ExternalProviderRef{{Ref: v1alpha1.NameReference{Name: "provider"}}}},
	}
	modelC := &v1alpha1.ExternalModel{ObjectMeta: metav1.ObjectMeta{Name: "c", Namespace: "tenant-a"}}
	modelB := &v1alpha1.ExternalModel{ObjectMeta: metav1.ObjectMeta{Name: "b", Namespace: "tenant-b"}}
	provider := &v1alpha1.ExternalProvider{ObjectMeta: metav1.ObjectMeta{Name: "provider", Namespace: "tenant-a"}}
	r := controllerTestClient(t, modelA, modelB, modelC, provider)
	requests := r.providerModels(context.Background(), provider)
	if len(requests) != 1 || requests[0].NamespacedName != client.ObjectKeyFromObject(modelA) {
		t.Fatalf("provider event enqueued %#v", requests)
	}

	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "credential", Namespace: "tenant-a"}}
	provider.Spec.Auth.SecretRef.Name = secret.Name
	r = controllerTestClient(t, modelA, modelB, modelC, provider, secret)
	requests = r.secretModels(context.Background(), secret)
	if len(requests) != 1 || requests[0].NamespacedName != client.ObjectKeyFromObject(modelA) {
		t.Fatalf("secret event enqueued %#v", requests)
	}
}

func TestDependentEventsRespectExternalModelNamespaceScope(t *testing.T) {
	modelA := &v1alpha1.ExternalModel{ObjectMeta: metav1.ObjectMeta{Name: "a", Namespace: "tenant-a"}}
	modelB := &v1alpha1.ExternalModel{ObjectMeta: metav1.ObjectMeta{Name: "b", Namespace: "tenant-b"}}
	providerA := &v1alpha1.ExternalProvider{ObjectMeta: metav1.ObjectMeta{Name: "provider", Namespace: "tenant-a"}}
	providerB := &v1alpha1.ExternalProvider{ObjectMeta: metav1.ObjectMeta{Name: "provider", Namespace: "tenant-b"}}
	secretA := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "credentials", Namespace: "tenant-a"}}
	secretB := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "credentials", Namespace: "tenant-b"}}
	serviceB := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "praxis", Namespace: "tenant-b"}}
	aitenantB := tenant.NewAITenant()
	aitenantB.SetName("tenant-b")
	aitenantB.SetNamespace("ai-tenants")
	aitenantB.Object["status"] = map[string]any{"tenantNamespace": "tenant-b"}
	r := controllerTestClient(t, modelA, modelB, providerA, providerB, secretA, secretB, serviceB, aitenantB)
	r.Namespace = "tenant-a"
	if got := r.providerModels(context.Background(), providerB); len(got) != 0 {
		t.Fatalf("out-of-scope provider event enqueued %#v", got)
	}
	if got := r.secretModels(context.Background(), secretB); len(got) != 0 {
		t.Fatalf("out-of-scope Secret event enqueued %#v", got)
	}
	if got := r.modelsInNamespace(context.Background(), "tenant-b"); len(got) != 0 {
		t.Fatalf("out-of-scope model list returned %#v", got)
	}
	if got := r.serviceModels(context.Background(), serviceB); len(got) != 0 {
		t.Fatalf("out-of-scope Service event enqueued %#v", got)
	}
	if got := r.tenantModels(context.Background(), aitenantB); len(got) != 0 {
		t.Fatalf("out-of-scope AITenant event enqueued %#v", got)
	}
	if _, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(modelB)}); err != nil {
		t.Fatalf("out-of-scope reconcile returned error: %v", err)
	}
}

func TestPraxisTenantUsesAnnotation(t *testing.T) {
	ait := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "maas.opendatahub.io/v1alpha1", "kind": "AITenant",
		"metadata": map[string]any{"name": "tenant", "namespace": "models-as-a-service", "annotations": map[string]any{tenant.AnnotationPayloadProcessingType: "praxis"}},
		"status":   map[string]any{"tenantNamespace": "tenant-a", "phase": "Active"},
	}}
	ait.SetGroupVersionKind(tenant.AITenantGVK)
	r := controllerTestClient(t)
	if err := r.Create(context.Background(), ait); err != nil {
		t.Fatal(err)
	}
	got, found, err := r.praxisTenantForNamespace(context.Background(), "tenant-a")
	if err != nil || !found || got.GetName() != "tenant" {
		t.Fatalf("annotation tenant lookup = %v, %v, %v", got, found, err)
	}
}

func TestTenantModelsMapsStatusNamespace(t *testing.T) {
	ait := tenant.NewAITenant()
	ait.SetNamespace("models-as-a-service")
	ait.SetName("tenant")
	ait.Object["status"] = map[string]any{"tenantNamespace": "tenant-a"}
	model := &v1alpha1.ExternalModel{ObjectMeta: metav1.ObjectMeta{Name: "a", Namespace: "tenant-a"}}
	r := controllerTestClient(t, model)
	requests := r.tenantModels(context.Background(), ait)
	want := reconcile.Request{NamespacedName: client.ObjectKeyFromObject(model)}
	if len(requests) != 1 || requests[0] != want {
		t.Fatalf("tenant event enqueued %#v", requests)
	}
}

func TestReconcileInactivePraxisTenantDoesNotPublish(t *testing.T) {
	model := &v1alpha1.ExternalModel{
		ObjectMeta: metav1.ObjectMeta{Name: "model", Namespace: "tenant-a"},
		Spec:       v1alpha1.ExternalModelSpec{ModelName: "client-model"},
	}
	ait := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "maas.opendatahub.io/v1alpha1", "kind": "AITenant",
		"metadata": map[string]any{"name": "tenant", "namespace": "models-as-a-service", "annotations": map[string]any{tenant.AnnotationPayloadProcessingType: "praxis"}},
		"status":   map[string]any{"tenantNamespace": "tenant-a", "phase": "Pending"},
	}}
	ait.SetGroupVersionKind(tenant.AITenantGVK)
	r := controllerTestClient(t, model)
	if err := r.Create(context.Background(), ait); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(model)}); err != nil {
		t.Fatal(err)
	}
	var got v1alpha1.ExternalModel
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(model), &got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != "Failed" {
		t.Fatalf("phase = %q, want Failed", got.Status.Phase)
	}
	var foundTenantNotReady bool
	for _, condition := range got.Status.Conditions {
		if condition.Reason == reasonTenantNotReady {
			foundTenantNotReady = true
		}
	}
	if !foundTenantNotReady {
		t.Fatalf("conditions = %#v, want reason %q", got.Status.Conditions, reasonTenantNotReady)
	}
	for _, kind := range []string{"Service", "ServiceEntry", "DestinationRule", "HTTPRoute"} {
		obj := &unstructured.Unstructured{}
		groups := map[string]string{
			"Service": "", "ServiceEntry": "networking.istio.io",
			"DestinationRule": "networking.istio.io", "HTTPRoute": "gateway.networking.k8s.io",
		}
		obj.SetGroupVersionKind(schema.GroupVersionKind{Group: groups[kind], Version: "v1", Kind: kind})
		if err := r.Get(context.Background(), client.ObjectKey{Namespace: "tenant-a", Name: "external-model-model"}, obj); err == nil {
			t.Fatalf("inactive tenant published %s", kind)
		}
	}
	var overlay corev1.ConfigMap
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: "tenant-a", Name: "routing-overlay"}, &overlay); !apierrors.IsNotFound(err) {
		t.Fatalf("inactive tenant overlay lookup error = %v, want NotFound", err)
	}
}

func TestReconcileCreatesTransportAndOverlayFromOneRouteSet(t *testing.T) {
	provider := &v1alpha1.ExternalProvider{
		ObjectMeta: metav1.ObjectMeta{Name: "provider", Namespace: "tenant-a", UID: "provider-uid"},
		Spec: v1alpha1.ExternalProviderSpec{
			Provider: "openai", Endpoint: "api.example.com",
			Auth: v1alpha1.AuthConfig{Type: "apikey", SecretRef: v1alpha1.NameReference{Name: "credentials"}},
		},
	}
	model := &v1alpha1.ExternalModel{
		ObjectMeta: metav1.ObjectMeta{Name: "model", Namespace: "tenant-a", UID: "model-uid"},
		Spec: v1alpha1.ExternalModelSpec{ModelName: "client-model", ExternalProviderRefs: []v1alpha1.ExternalProviderRef{{
			Ref: v1alpha1.NameReference{Name: provider.Name}, TargetModel: "gpt", APIFormat: "openai-chat", Path: "/v1/chat/completions",
		}}},
	}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "credentials", Namespace: "tenant-a"}, Data: map[string][]byte{"api-key": []byte("must-not-be-published")}}
	ait := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "maas.opendatahub.io/v1alpha1", "kind": "AITenant",
		"metadata": map[string]any{"name": "tenant", "namespace": "models-as-a-service", "annotations": map[string]any{tenant.AnnotationPayloadProcessingType: "praxis"}},
		"status":   map[string]any{"tenantNamespace": "tenant-a", "phase": "Active"},
	}}
	ait.SetGroupVersionKind(tenant.AITenantGVK)
	r := controllerTestClient(t, provider, model, secret)
	if err := r.Create(context.Background(), ait); err != nil {
		t.Fatal(err)
	}
	r.Namespace, r.GatewayName, r.GatewayNamespace, r.Network = "tenant-a", "gateway", "gateway-system", "external-model"
	r.KnownClusters = []string{"provider-provider"}
	req := reconcile.Request{NamespacedName: client.ObjectKeyFromObject(model)}
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	var firstOverlay corev1.ConfigMap
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: "tenant-a", Name: "routing-overlay"}, &firstOverlay); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatal(err)
	}

	for _, object := range []struct{ kind, name string }{{"Service", "provider-provider"}, {"ServiceEntry", "provider-provider"}, {"DestinationRule", "provider-provider-tenant"}, {"HTTPRoute", "external-model-model"}} {
		got := &unstructured.Unstructured{}
		groups := map[string]string{
			"Service": "", "ServiceEntry": "networking.istio.io",
			"DestinationRule": "networking.istio.io", "HTTPRoute": "gateway.networking.k8s.io",
		}
		got.SetGroupVersionKind(schema.GroupVersionKind{Group: groups[object.kind], Version: "v1", Kind: object.kind})
		objectNamespace := "tenant-a"
		if object.kind == "DestinationRule" {
			objectNamespace = "gateway-system"
		}
		if err := r.Get(context.Background(), client.ObjectKey{Namespace: objectNamespace, Name: object.name}, got); err != nil {
			t.Fatalf("get %s: %v", object.kind, err)
		}
		if object.kind == "DestinationRule" {
			if len(got.GetOwnerReferences()) != 0 {
				t.Fatalf("cross-namespace DestinationRule owner references = %#v", got.GetOwnerReferences())
			}
			if got.GetLabels()["inference.opendatahub.io/external-provider"] != "provider" {
				t.Fatalf("DestinationRule provider label = %q", got.GetLabels()["inference.opendatahub.io/external-provider"])
			}
			continue
		}
		if len(got.GetOwnerReferences()) != 1 {
			t.Fatalf("%s owner references = %#v", object.kind, got.GetOwnerReferences())
		}
		wantOwner := "provider"
		if object.kind == "HTTPRoute" {
			wantOwner = "model"
		}
		if got.GetOwnerReferences()[0].Name != wantOwner {
			t.Fatalf("%s owner = %q, want %q", object.kind, got.GetOwnerReferences()[0].Name, wantOwner)
		}
	}
	var overlay corev1.ConfigMap
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: "tenant-a", Name: "routing-overlay"}, &overlay); err != nil {
		t.Fatal(err)
	}
	if overlay.Data["routing-overlay.json"] != firstOverlay.Data["routing-overlay.json"] {
		t.Fatal("semantic no-op rewrote overlay bytes")
	}
	if strings.Contains(overlay.Data["routing-overlay.json"], "must-not-be-published") {
		t.Fatal("Secret bytes entered overlay")
	}
	if !strings.Contains(overlay.Data["routing-overlay.json"], "provider-provider") {
		t.Fatal("provider cluster missing from overlay")
	}
	var gotModel v1alpha1.ExternalModel
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(model), &gotModel); err != nil {
		t.Fatal(err)
	}
	if gotModel.Status.Phase != resolver.PhaseReady || gotModel.Status.HTTPRouteName != "external-model-model" {
		t.Fatalf("model status = %#v", gotModel.Status)
	}
	var gotProvider v1alpha1.ExternalProvider
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(provider), &gotProvider); err != nil {
		t.Fatal(err)
	}
	if gotProvider.Status.Phase != resolver.PhaseReady || gotProvider.Status.ObservedGeneration != provider.Generation {
		t.Fatalf("provider status = %#v", gotProvider.Status)
	}
	if len(gotModel.Status.Conditions) < 2 {
		t.Fatalf("expected Ready and OverlayDistributed conditions: %#v", gotModel.Status.Conditions)
	}
	ait.SetAnnotations(map[string]string{tenant.AnnotationPayloadProcessingType: tenant.PayloadProcessingBackendIPP})
	if err := r.Update(context.Background(), ait); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	removedRoute := &unstructured.Unstructured{}
	removedRoute.SetGroupVersionKind(schema.GroupVersionKind{Group: "gateway.networking.k8s.io", Version: "v1", Kind: "HTTPRoute"})
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: "tenant-a", Name: "external-model-model"}, removedRoute); !apierrors.IsNotFound(err) {
		t.Fatalf("switch-away route error = %v, want NotFound", err)
	}
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: "tenant-a", Name: "routing-overlay"}, &overlay); !apierrors.IsNotFound(err) {
		t.Fatalf("switch-away overlay error = %v, want NotFound", err)
	}
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(model), &gotModel); err != nil {
		t.Fatal(err)
	}
	if controllerutil.ContainsFinalizer(&gotModel, externalModelFinalizer) {
		t.Fatalf("switch-away retained controller finalizer: %v", gotModel.Finalizers)
	}
}

func TestValidateResolvedCredentialsUsesEffectiveModelOverride(t *testing.T) {
	base := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "provider-default", Namespace: "tenant-a"}, Data: map[string][]byte{"api-key": []byte("default")}}
	r := controllerTestClient(t, base)
	route := resolver.Route{Model: "model", Provider: "provider", Namespace: "tenant-a", AuthType: "apikey", SecretName: "model-override", SecretKey: "api-key"}
	if err := r.validateResolvedCredentials(context.Background(), []resolver.Route{route}); err == nil || !strings.Contains(err.Error(), "model-override") {
		t.Fatalf("validateResolvedCredentials() error = %v, want missing effective override Secret", err)
	}
	override := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "model-override", Namespace: "tenant-a"}, Data: map[string][]byte{"api-key": []byte("override")}}
	if err := r.Create(context.Background(), override); err != nil {
		t.Fatal(err)
	}
	if err := r.validateResolvedCredentials(context.Background(), []resolver.Route{route}); err != nil {
		t.Fatalf("validateResolvedCredentials() = %v, want effective override accepted", err)
	}
}

func TestReconcileRecoversProviderAfterSecretDeletionAndRestoration(t *testing.T) {
	provider := &v1alpha1.ExternalProvider{
		ObjectMeta: metav1.ObjectMeta{Name: "provider", Namespace: "tenant-a", UID: "provider-uid"},
		Spec: v1alpha1.ExternalProviderSpec{
			Provider: "openai", Endpoint: "api.example.com",
			Auth: v1alpha1.AuthConfig{Type: "apikey", SecretRef: v1alpha1.NameReference{Name: "credentials"}},
		},
	}
	model := &v1alpha1.ExternalModel{
		ObjectMeta: metav1.ObjectMeta{Name: "model", Namespace: "tenant-a", UID: "model-uid"},
		Spec: v1alpha1.ExternalModelSpec{ExternalProviderRefs: []v1alpha1.ExternalProviderRef{{
			Ref: v1alpha1.NameReference{Name: provider.Name}, TargetModel: "gpt", APIFormat: "openai-chat", Path: "/v1/chat/completions",
		}}},
	}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "credentials", Namespace: "tenant-a"}, Data: map[string][]byte{"api-key": []byte("secret")}}
	ait := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "maas.opendatahub.io/v1alpha1", "kind": "AITenant",
		"metadata": map[string]any{"name": "tenant", "namespace": "models-as-a-service", "annotations": map[string]any{tenant.AnnotationPayloadProcessingType: "praxis"}},
		"status":   map[string]any{"tenantNamespace": "tenant-a", "phase": "Active"},
	}}
	ait.SetGroupVersionKind(tenant.AITenantGVK)
	r := controllerTestClient(t, provider, model, secret)
	if err := r.Create(context.Background(), ait); err != nil {
		t.Fatal(err)
	}
	r.Namespace, r.GatewayName, r.GatewayNamespace, r.Network = "tenant-a", "gateway", "tenant-a", "external-model"
	r.KnownClusters = []string{"provider-provider"}
	req := reconcile.Request{NamespacedName: client.ObjectKeyFromObject(model)}
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	var before corev1.ConfigMap
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: "tenant-a", Name: "routing-overlay"}, &before); err != nil {
		t.Fatal(err)
	}

	if err := r.Delete(context.Background(), secret); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(context.Background(), req); !errors.Is(err, errCredentialNotReady) {
		t.Fatalf("missing-secret reconcile error = %v, want credential-not-ready", err)
	}
	var after corev1.ConfigMap
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: "tenant-a", Name: "routing-overlay"}, &after); err != nil {
		t.Fatal(err)
	}
	if after.Data["routing-overlay.json"] != before.Data["routing-overlay.json"] {
		t.Fatal("readiness loss replaced the last-known-good overlay")
	}
	var failed v1alpha1.ExternalModel
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(model), &failed); err != nil {
		t.Fatal(err)
	}
	if failed.Status.Phase != "Failed" || !hasConditionReason(failed.Status.Conditions, conditionReady, reasonProviderNotReady) {
		t.Fatalf("missing-secret status = %#v", failed.Status)
	}

	secret.ResourceVersion = ""
	if err := r.Create(context.Background(), secret); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	var recovered v1alpha1.ExternalModel
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(model), &recovered); err != nil {
		t.Fatal(err)
	}
	if recovered.Status.Phase != resolver.PhaseReady || recovered.Status.OverlayDigest == "" || recovered.Status.OverlayGeneration == 0 {
		t.Fatalf("recovery status = %#v", recovered.Status)
	}
}

func TestReconcileRecoversProviderAfterTransientTransportFailure(t *testing.T) {
	r, model := reconcilerFixture(t)
	req := reconcile.Request{NamespacedName: client.ObjectKeyFromObject(model)}
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	r.ApplyResource = func(context.Context, client.Client, unstructured.Unstructured) error {
		return errors.New("transient transport failure")
	}
	if _, err := r.Reconcile(context.Background(), req); err == nil || !strings.Contains(err.Error(), "transient transport failure") {
		t.Fatalf("transport failure = %v", err)
	}
	var failedProvider v1alpha1.ExternalProvider
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: "tenant-a", Name: "provider"}, &failedProvider); err != nil {
		t.Fatal(err)
	}
	if failedProvider.Status.Phase != "Failed" {
		t.Fatalf("provider phase after transport failure = %q, want Failed", failedProvider.Status.Phase)
	}
	r.ApplyResource = nil
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("recovery reconcile = %v", err)
	}
	var provider v1alpha1.ExternalProvider
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: "tenant-a", Name: "provider"}, &provider); err != nil {
		t.Fatal(err)
	}
	if provider.Status.Phase != resolver.PhaseReady {
		t.Fatalf("provider phase after transport recovery = %q, want Ready", provider.Status.Phase)
	}
}

func TestExternalModelDeletionRepublishesRemainingRoutes(t *testing.T) {
	r, model := reconcilerFixture(t)
	if _, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(model)}); err != nil {
		t.Fatal(err)
	}
	sibling := model.DeepCopy()
	sibling.Name = "sibling"
	sibling.UID = "sibling-uid"
	sibling.ResourceVersion = ""
	sibling.Finalizers = []string{externalModelFinalizer}
	sibling.Status.Phase = resolver.PhaseReady
	if err := r.Create(context.Background(), sibling); err != nil {
		t.Fatal(err)
	}
	if err := r.Delete(context.Background(), model); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(model)}); err != nil {
		t.Fatal(err)
	}
	deletedRoute := &unstructured.Unstructured{}
	deletedRoute.SetGroupVersionKind(schema.GroupVersionKind{Group: "gateway.networking.k8s.io", Version: "v1", Kind: "HTTPRoute"})
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: "tenant-a", Name: modelRouteName(model.Name)}, deletedRoute); !apierrors.IsNotFound(err) {
		t.Fatalf("deleted model route = %v, want NotFound", err)
	}
	remainingRoute := &unstructured.Unstructured{}
	remainingRoute.SetGroupVersionKind(schema.GroupVersionKind{Group: "gateway.networking.k8s.io", Version: "v1", Kind: "HTTPRoute"})
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: "tenant-a", Name: modelRouteName(sibling.Name)}, remainingRoute); err != nil {
		t.Fatalf("remaining sibling route = %v", err)
	}
	var overlay corev1.ConfigMap
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: "tenant-a", Name: "routing-overlay"}, &overlay); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(overlay.Data["routing-overlay.json"], "sibling") {
		t.Fatal("remaining overlay does not contain sibling model")
	}
}

func TestFinalExternalModelDeletionRemovesOwnedTransportAndOverlay(t *testing.T) {
	r, model := reconcilerFixture(t)
	if _, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(model)}); err != nil {
		t.Fatal(err)
	}
	if err := r.Delete(context.Background(), model); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(model)}); err != nil {
		t.Fatal(err)
	}
	var overlay corev1.ConfigMap
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: "tenant-a", Name: "routing-overlay"}, &overlay); !apierrors.IsNotFound(err) {
		t.Fatalf("final overlay = %v, want NotFound", err)
	}
	for _, key := range []client.ObjectKey{{Namespace: "tenant-a", Name: "provider-provider"}, {Namespace: "tenant-a", Name: "external-model-model"}} {
		for _, gvk := range []schema.GroupVersionKind{{Version: "v1", Kind: "Service"}, {Group: "gateway.networking.k8s.io", Version: "v1", Kind: "HTTPRoute"}} {
			obj := &unstructured.Unstructured{}
			obj.SetGroupVersionKind(gvk)
			if err := r.Get(context.Background(), key, obj); !apierrors.IsNotFound(err) {
				t.Fatalf("final deletion left %s/%s: %v", gvk.Kind, key.Name, err)
			}
		}
	}
}

func TestConcurrentExternalModelDeletionCleansNamespaceState(t *testing.T) {
	r, first := reconcilerFixture(t)
	firstKey := client.ObjectKeyFromObject(first)
	if _, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: firstKey}); err != nil {
		t.Fatal(err)
	}

	second := first.DeepCopy()
	second.Name = "second"
	second.UID = "second-uid"
	second.ResourceVersion = ""
	second.ManagedFields = nil
	second.Finalizers = []string{externalModelFinalizer}
	second.Status = v1alpha1.ExternalModelStatus{}
	if err := r.Create(context.Background(), second); err != nil {
		t.Fatal(err)
	}
	secondKey := client.ObjectKeyFromObject(second)

	if err := r.Delete(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	if err := r.Delete(context.Background(), second); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: firstKey}); err != nil {
		t.Fatal(err)
	}

	var overlay corev1.ConfigMap
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: "tenant-a", Name: "routing-overlay"}, &overlay); !apierrors.IsNotFound(err) {
		t.Fatalf("concurrent deletion overlay = %v, want NotFound", err)
	}
	for _, key := range []client.ObjectKey{
		{Namespace: "tenant-a", Name: "provider-provider"},
		{Namespace: "tenant-a", Name: "external-model-model"},
		{Namespace: "tenant-a", Name: "external-model-second"},
	} {
		for _, gvk := range []schema.GroupVersionKind{{Version: "v1", Kind: "Service"}, {Group: "gateway.networking.k8s.io", Version: "v1", Kind: "HTTPRoute"}} {
			obj := &unstructured.Unstructured{}
			obj.SetGroupVersionKind(gvk)
			if err := r.Get(context.Background(), key, obj); !apierrors.IsNotFound(err) {
				t.Fatalf("concurrent deletion left %s/%s: %v", gvk.Kind, key.Name, err)
			}
		}
	}

	var pending v1alpha1.ExternalModel
	if err := r.Get(context.Background(), secondKey, &pending); err != nil {
		t.Fatal(err)
	}
	if !controllerutil.ContainsFinalizer(&pending, externalModelFinalizer) {
		t.Fatalf("sibling deletion unexpectedly removed by another reconcile: %v", pending.Finalizers)
	}
	if _, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: secondKey}); err != nil {
		t.Fatal(err)
	}
	for _, key := range []client.ObjectKey{firstKey, secondKey} {
		var deleted v1alpha1.ExternalModel
		if err := r.Get(context.Background(), key, &deleted); !apierrors.IsNotFound(err) {
			t.Fatalf("deleted ExternalModel %s remains: %v", key.Name, err)
		}
	}
}

type failingDeleteClient struct {
	client.Client
	err error
}

func (c failingDeleteClient) Delete(context.Context, client.Object, ...client.DeleteOption) error {
	return c.err
}

func TestExternalModelDeletionRetainsFinalizerWhenCleanupFails(t *testing.T) {
	r, model := reconcilerFixture(t)
	if _, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(model)}); err != nil {
		t.Fatal(err)
	}
	if err := r.Delete(context.Background(), model); err != nil {
		t.Fatal(err)
	}
	r.Client = failingDeleteClient{Client: r.Client, err: errors.New("injected cleanup delete failure")}
	if _, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(model)}); err == nil || !strings.Contains(err.Error(), "injected cleanup delete failure") {
		t.Fatalf("cleanup failure = %v", err)
	}
	var pending v1alpha1.ExternalModel
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(model), &pending); err != nil {
		t.Fatal(err)
	}
	if !controllerutil.ContainsFinalizer(&pending, externalModelFinalizer) {
		t.Fatalf("cleanup failure removed finalizer: %v", pending.Finalizers)
	}
}

func hasConditionReason(conditions []metav1.Condition, typ, reason string) bool {
	for _, condition := range conditions {
		if condition.Type == typ && condition.Reason == reason && condition.Status == metav1.ConditionFalse {
			return true
		}
	}
	return false
}

func TestReconcileFailureBoundariesRetainPublishedOverlay(t *testing.T) {
	r, model := reconcilerFixture(t)
	req := reconcile.Request{NamespacedName: client.ObjectKeyFromObject(model)}
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	var before corev1.ConfigMap
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: "tenant-a", Name: "routing-overlay"}, &before); err != nil {
		t.Fatal(err)
	}

	r.ApplyResource = func(context.Context, client.Client, unstructured.Unstructured) error {
		return errors.New("injected transport apply failure")
	}
	if _, err := r.Reconcile(context.Background(), req); err == nil || !strings.Contains(err.Error(), "injected transport") {
		t.Fatalf("transport failure = %v", err)
	}
	var afterTransport corev1.ConfigMap
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: "tenant-a", Name: "routing-overlay"}, &afterTransport); err != nil {
		t.Fatal(err)
	}
	if afterTransport.Data["routing-overlay.json"] != before.Data["routing-overlay.json"] {
		t.Fatal("transport failure published a new overlay")
	}

	r.ApplyResource = nil
	var provider v1alpha1.ExternalProvider
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: "tenant-a", Name: "provider"}, &provider); err != nil {
		t.Fatal(err)
	}
	provider.Status.Phase = resolver.PhaseReady
	if err := r.Status().Update(context.Background(), &provider); err != nil {
		t.Fatal(err)
	}
	r.PublishOverlay = func(context.Context, *resolver.ResolvedRouteSet, envelope.Scope, envelope.Options) (publisher.Result, error) {
		return publisher.Result{}, errors.New("injected overlay publication failure")
	}
	if _, err := r.Reconcile(context.Background(), req); err == nil || !strings.Contains(err.Error(), "injected overlay") {
		t.Fatalf("publication failure = %v", err)
	}
	var afterPublish corev1.ConfigMap
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: "tenant-a", Name: "routing-overlay"}, &afterPublish); err != nil {
		t.Fatal(err)
	}
	if afterPublish.Data["routing-overlay.json"] != before.Data["routing-overlay.json"] {
		t.Fatal("publication failure changed the last-known-good overlay")
	}
}

func TestReconcileCanFailEachSSAApplyBoundary(t *testing.T) {
	for _, boundary := range []string{"Service", "ServiceEntry", "DestinationRule", "HTTPRoute"} {
		t.Run(boundary, func(t *testing.T) {
			r, model := reconcilerFixture(t)
			req := reconcile.Request{NamespacedName: client.ObjectKeyFromObject(model)}
			if _, err := r.Reconcile(context.Background(), req); err != nil {
				t.Fatal(err)
			}
			var before corev1.ConfigMap
			if err := r.Get(context.Background(), client.ObjectKey{Namespace: "tenant-a", Name: "routing-overlay"}, &before); err != nil {
				t.Fatal(err)
			}
			r.ApplyResource = func(ctx context.Context, _ client.Client, object unstructured.Unstructured) error {
				if object.GetKind() == boundary {
					return fmt.Errorf("injected %s apply failure", boundary)
				}
				return render.Apply(ctx, r.Client, []unstructured.Unstructured{object})
			}
			if _, err := r.Reconcile(context.Background(), req); err == nil || !strings.Contains(err.Error(), "injected "+boundary) {
				t.Fatalf("%s failure = %v", boundary, err)
			}
			var after corev1.ConfigMap
			if err := r.Get(context.Background(), client.ObjectKey{Namespace: "tenant-a", Name: "routing-overlay"}, &after); err != nil {
				t.Fatal(err)
			}
			if after.Data["routing-overlay.json"] != before.Data["routing-overlay.json"] {
				t.Fatalf("%s failure changed the published overlay", boundary)
			}
		})
	}
}

type failingStatusClient struct {
	client.Client
	err error
}

//nolint:ireturn // client.Status() is intentionally an interface boundary.
func (c failingStatusClient) Status() client.StatusWriter {
	return failingStatusWriter{SubResourceWriter: c.Client.Status(), err: c.err}
}

type failingStatusWriter struct {
	client.SubResourceWriter
	err error
}

func (w failingStatusWriter) Patch(context.Context, client.Object, client.Patch, ...client.SubResourcePatchOption) error {
	return w.err
}

func TestStatusWriteErrorsAreReturned(t *testing.T) {
	errorsToTest := []error{
		apierrors.NewConflict(schema.GroupResource{Group: v1alpha1.GroupVersion.Group, Resource: "externalmodels"}, "model", errors.New("conflict")),
		apierrors.NewForbidden(schema.GroupResource{Group: v1alpha1.GroupVersion.Group, Resource: "externalmodels"}, "model", errors.New("forbidden")),
		errors.New("transient API failure"),
	}
	for _, injected := range errorsToTest {
		t.Run(injected.Error(), func(t *testing.T) {
			model := &v1alpha1.ExternalModel{ObjectMeta: metav1.ObjectMeta{Name: "model", Namespace: "tenant-a"}}
			r := controllerTestClient(t, model)
			r.Client = failingStatusClient{Client: r.Client, err: injected}
			if err := r.updateModelStatus(context.Background(), model, true, reasonReconciled, "status test", nil); err == nil {
				t.Fatalf("status update returned nil for %v", injected)
			}
		})
	}
}

func reconcilerFixture(t *testing.T) (*Reconciler, *v1alpha1.ExternalModel) {
	t.Helper()
	provider := &v1alpha1.ExternalProvider{
		ObjectMeta: metav1.ObjectMeta{Name: "provider", Namespace: "tenant-a", UID: "provider-uid"},
		Spec:       v1alpha1.ExternalProviderSpec{Provider: "openai", Endpoint: "api.example.com", Auth: v1alpha1.AuthConfig{Type: "apikey", SecretRef: v1alpha1.NameReference{Name: "credentials"}}},
	}
	model := &v1alpha1.ExternalModel{
		ObjectMeta: metav1.ObjectMeta{Name: "model", Namespace: "tenant-a", UID: "model-uid"},
		Spec: v1alpha1.ExternalModelSpec{ExternalProviderRefs: []v1alpha1.ExternalProviderRef{{
			Ref: v1alpha1.NameReference{Name: provider.Name}, TargetModel: "gpt",
			APIFormat: "openai-chat", Path: "/v1/chat/completions",
		}}},
	}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "credentials", Namespace: "tenant-a"}, Data: map[string][]byte{"api-key": []byte("fixture-only-secret")}}
	ait := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "maas.opendatahub.io/v1alpha1", "kind": "AITenant",
		"metadata": map[string]any{"name": "tenant", "namespace": "models-as-a-service", "annotations": map[string]any{tenant.AnnotationPayloadProcessingType: "praxis"}},
		"status":   map[string]any{"tenantNamespace": "tenant-a", "phase": "Active"},
	}}
	ait.SetGroupVersionKind(tenant.AITenantGVK)
	r := controllerTestClient(t, provider, model, secret)
	if err := r.Create(context.Background(), ait); err != nil {
		t.Fatal(err)
	}
	r.Namespace, r.GatewayName, r.GatewayNamespace, r.Network = "tenant-a", "gateway", "tenant-a", "external-model"
	r.KnownClusters = []string{"provider-provider"}
	return r, model
}

func TestReconcileRevalidatesPendingProviderDuringIPPHandoff(t *testing.T) {
	r, model := reconcilerFixture(t)
	var provider v1alpha1.ExternalProvider
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: "tenant-a", Name: "provider"}, &provider); err != nil {
		t.Fatal(err)
	}
	provider.Status.Phase = "Pending"
	provider.Status.Conditions = []metav1.Condition{{
		Type: conditionReady, Status: metav1.ConditionFalse, Reason: "Reconciling",
		Message: "handoff in progress", LastTransitionTime: metav1.Now(),
	}}
	if err := r.Status().Update(context.Background(), &provider); err != nil {
		t.Fatal(err)
	}

	if _, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(model)}); err != nil {
		t.Fatalf("reconcile with pending handoff provider = %v", err)
	}
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(&provider), &provider); err != nil {
		t.Fatal(err)
	}
	if provider.Status.Phase != resolver.PhaseReady {
		t.Fatalf("provider phase = %q, want Ready", provider.Status.Phase)
	}
	var overlay corev1.ConfigMap
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: "tenant-a", Name: "routing-overlay"}, &overlay); err != nil {
		t.Fatalf("routing overlay = %v", err)
	}
}
