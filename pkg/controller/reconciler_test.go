package controller

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"reflect"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	aigatewayv1alpha1 "github.com/opendatahub-io/ai-gateway-controller/api/aigateway/v1alpha1"
	v1alpha1 "github.com/opendatahub-io/ai-gateway-controller/api/inference/v1alpha1"
	"github.com/opendatahub-io/ai-gateway-controller/pkg/envelope"
	"github.com/opendatahub-io/ai-gateway-controller/pkg/guardrail"
	"github.com/opendatahub-io/ai-gateway-controller/pkg/publisher"
	"github.com/opendatahub-io/ai-gateway-controller/pkg/render"
	"github.com/opendatahub-io/ai-gateway-controller/pkg/resolver"
	"github.com/opendatahub-io/ai-gateway-controller/pkg/tenant"
)

// TestEnableExternalModelRoutesKeepsPostAuthForCompiledChecks pins that a
// tenant with an attached NeMo check keeps the shared post-auth filter on its
// ExternalModel routes, so those requests are checked too, and that an
// unattached policy leaves it off.
func TestEnableExternalModelRoutesKeepsPostAuthForCompiledChecks(t *testing.T) {
	policy := &aigatewayv1alpha1.AIGuardrail{
		ObjectMeta: metav1.ObjectMeta{Name: "safety", Namespace: "tenant-a", Generation: 1},
		Spec: aigatewayv1alpha1.AIGuardrailSpec{
			Provider: aigatewayv1alpha1.AIGuardrailProvider{
				Nemo: aigatewayv1alpha1.AIGuardrailNemoProvider{Ref: aigatewayv1alpha1.AIGuardrailNamespacedReference{Name: "nemo"}},
			},
			Checks: []aigatewayv1alpha1.AIGuardrailCheck{{
				Name: "pii", ConfigID: "pii-v1", Phases: []aigatewayv1alpha1.GuardrailPhase{aigatewayv1alpha1.GuardrailPhaseInput},
			}},
		},
		Status: aigatewayv1alpha1.AIGuardrailStatus{
			BindingRevision:  "rev",
			ProviderEndpoint: "http://nemo.nemo-infra.svc.cluster.local",
			Conditions: []metav1.Condition{{
				Type: guardrail.ConditionAccepted, Status: metav1.ConditionTrue, ObservedGeneration: 1,
			}},
		},
	}
	mtc := tenant.NewMaasTenantConfig()
	mtc.SetName(tenant.MaasTenantConfigInstanceName)
	mtc.SetNamespace("tenant-a")
	mtc.Object["spec"] = map[string]any{"guardrails": []any{map[string]any{"ref": map[string]any{"name": "safety"}}}}

	for _, c := range []struct {
		name     string
		objects  []client.Object
		postAuth bool
	}{
		{name: "attached", objects: []client.Object{policy, mtc}, postAuth: true},
		{name: "not attached", objects: []client.Object{policy}},
	} {
		t.Run(c.name, func(t *testing.T) {
			r := controllerTestClient(t, c.objects...)
			var applied unstructured.Unstructured
			r.ApplyResource = func(_ context.Context, _ client.Client, object unstructured.Unstructured) error {
				applied = *object.DeepCopy()
				return nil
			}
			routes := []resolver.Route{{Model: "demo-model", ClientName: "demo", Provider: "provider-a"}}
			testTenant := attachmentTenant{
				namespace: "tenant-a", tenantID: "tenant-a",
				gateway: v1alpha1.NamespacedObjectReference{Name: "test-gateway", Namespace: "gateway-system"},
			}
			if _, err := r.enableExternalModelRoutes(context.Background(), testTenant, routes); err != nil {
				t.Fatal(err)
			}
			patches := nestedSlice(t, applied.Object, "spec", "configPatches")
			if len(patches) == 0 {
				t.Fatal("no route patches were written")
			}
			for _, raw := range patches {
				patch, _ := raw.(map[string]any)
				disabled, _, _ := unstructured.NestedBool(patch, "patch", "value", "typed_per_filter_config", "envoy.filters.http.ext_proc.ipp", "disabled")
				if disabled == c.postAuth {
					t.Fatalf("route patch %v post-auth filter disabled = %v, want %v", patch["match"], disabled, !c.postAuth)
				}
				if disabled, _, _ := unstructured.NestedBool(patch, "patch", "value", "typed_per_filter_config", "envoy.filters.http.ext_proc.ipp-pre", "disabled"); !disabled {
					t.Fatalf("route patch %v left the shared pre-auth filter on", patch["match"])
				}
			}
		})
	}
}

func controllerTestClient(t *testing.T, objects ...client.Object) *Reconciler {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := aigatewayv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	scheme.AddKnownTypeWithName(schema.GroupVersionKind{Group: "networking.istio.io", Version: "v1alpha3", Kind: "EnvoyFilter"}, &unstructured.Unstructured{})
	scheme.AddKnownTypeWithName(schema.GroupVersionKind{Group: "networking.istio.io", Version: "v1alpha3", Kind: "EnvoyFilterList"}, &unstructured.UnstructuredList{})
	scheme.AddKnownTypeWithName(tenant.AITenantGVK, &unstructured.Unstructured{})
	scheme.AddKnownTypeWithName(tenant.AITenantGVK.GroupVersion().WithKind("AITenantList"), &unstructured.UnstructuredList{})
	scheme.AddKnownTypeWithName(tenant.MaasTenantConfigGVK, &unstructured.Unstructured{})
	scheme.AddKnownTypeWithName(tenant.MaasTenantConfigGVK.GroupVersion().WithKind("MaasTenantConfigList"), &unstructured.UnstructuredList{})
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&v1alpha1.ExternalModel{}, &v1alpha1.ExternalProvider{}).
		WithIndex(&v1alpha1.ExternalProvider{}, providerCredentialNameIndex, indexProviderByCredentialName).
		WithObjects(objects...).Build()
	r := &Reconciler{Client: fakeClient, APIReader: fakeClient}
	r.readPodLogs = func(ctx context.Context, namespace, _, container string) (string, error) {
		if container != externalModelExtProcContainer {
			return "", fmt.Errorf("unexpected ExtProc container %q", container)
		}
		env, err := r.readPublishedEnvelope(ctx, namespace)
		if err != nil || env == nil {
			return "", err
		}
		revision := env.Revision.Value
		return `accepted_revision="` + revision + `" serving_revision="` + revision + `"`, nil
	}
	return r
}

func legacyExternalModelRouteFilter(name, namespace, managedBy string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "networking.istio.io/v1alpha3",
		"kind":       "EnvoyFilter",
		"metadata": map[string]any{
			"name": name, "namespace": namespace,
			"labels": map[string]any{"app.kubernetes.io/managed-by": managedBy},
		},
	}}
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
	routes := []resolver.Route{
		{Model: "demo-model", ClientName: "demo", Provider: "provider-a", APIFormat: "openai-chat", Path: "/v1/chat/completions"},
		{Model: "demo-model", ClientName: "demo", Provider: "provider-b", APIFormat: "messages", Path: "/v1/messages", APITranslationEnabled: true},
	}
	rendered := modelHTTPRouteSet(routes, "tenant-a", "test-gateway", "gateway-system")
	renderedRules := nestedSlice(t, rendered.Object, "spec", "rules")
	expectedRoutes := make(map[string]bool, len(renderedRules))
	if len(renderedRules) != 6 {
		t.Fatalf("rendered route rules = %d, want four provider rules and two sinks", len(renderedRules))
	}
	expectedResponseModes := []string{"STREAMED", "STREAMED", "STREAMED", "STREAMED", "STREAMED", "STREAMED"}
	expectedResponseModeByRoute := make(map[string]string, len(renderedRules))
	for index := range renderedRules {
		name := fmt.Sprintf("tenant-a.external-model-demo-model.%d", index)
		expectedRoutes[name] = false
		expectedResponseModeByRoute[name] = expectedResponseModes[index]
	}
	testTenant := attachmentTenant{namespace: "tenant-a", tenantID: "tenant-a",
		gateway: v1alpha1.NamespacedObjectReference{Name: "test-gateway", Namespace: "gateway-system"}}
	if _, err := r.enableExternalModelRoutes(context.Background(), testTenant, routes); err != nil {
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
	assertExternalModelRoutePatches(t, patches, expectedRoutes, expectedResponseModeByRoute, len(renderedRules))
	if _, err := r.enableExternalModelRoutes(context.Background(), testTenant, routes[:1]); err != nil {
		t.Fatal(err)
	}
	// stream_usage_inject needs the complete body with translation off too.
	assertOpenAIOnlyRouteModes(t, nestedSlice(t, applied.Object, "spec", "configPatches"), "BUFFERED")
	routes[0].APITranslationEnabled = true
	if _, err := r.enableExternalModelRoutes(context.Background(), testTenant, routes[:1]); err != nil {
		t.Fatal(err)
	}
	assertOpenAIOnlyRouteModes(t, nestedSlice(t, applied.Object, "spec", "configPatches"), "BUFFERED")
}

func assertExternalModelRoutePatches(
	t *testing.T,
	patches []any,
	expectedRoutes map[string]bool,
	expectedResponseModeByRoute map[string]string,
	expectedCount int,
) {
	t.Helper()
	routePatchCount := 0
	for _, raw := range patches {
		patch, ok := raw.(map[string]any)
		if !ok || patch["applyTo"] != "HTTP_ROUTE" {
			continue
		}
		match, _, _ := unstructured.NestedString(patch, "match", "routeConfiguration", "vhost", "route", "name")
		if match == "" {
			continue
		}
		if _, exists := expectedRoutes[match]; !exists {
			t.Fatalf("unexpected ExternalModel route patch name %q", match)
		}
		if expectedRoutes[match] {
			t.Fatalf("duplicate ExternalModel route patch %q", match)
		}
		expectedRoutes[match] = true
		routePatchCount++
		assertExternalModelRouteExtProcPatch(t, patch, match, "BUFFERED", expectedResponseModeByRoute[match])
	}
	if routePatchCount != expectedCount {
		t.Fatalf("ExternalModel route patches = %d, generated HTTPRoute rules = %d", routePatchCount, expectedCount)
	}
	for name, found := range expectedRoutes {
		if !found {
			t.Errorf("generated ExternalModel route %q has no ExtProc enablement patch", name)
		}
	}
	// A stale route-level enablement must not survive when current routes are patched.
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

func assertOpenAIOnlyRouteModes(t *testing.T, patches []any, wantRequestMode string) {
	t.Helper()
	for _, raw := range patches {
		patch, ok := raw.(map[string]any)
		if !ok || patch["applyTo"] != "HTTP_ROUTE" {
			continue
		}
		mode, found, err := unstructured.NestedString(patch, "patch", "value", "typed_per_filter_config", externalModelExtProcFilter, "overrides", "processing_mode", "response_body_mode")
		if err != nil || !found || mode != "NONE" {
			t.Fatalf("OpenAI-only route response mode = %q found=%t err=%v, want NONE", mode, found, err)
		}
		requestMode, found, err := unstructured.NestedString(patch, "patch", "value", "typed_per_filter_config", externalModelExtProcFilter, "overrides", "processing_mode", "request_body_mode")
		if err != nil || !found || requestMode != wantRequestMode {
			t.Fatalf("OpenAI request mode = %q found=%t err=%v, want %s", requestMode, found, err, wantRequestMode)
		}
	}
}

func assertExternalModelRouteExtProcPatch(t *testing.T, patch map[string]any, routeName, requestMode, responseMode string) {
	t.Helper()
	overrides, found, err := unstructured.NestedMap(patch, "patch", "value", "typed_per_filter_config", externalModelExtProcFilter, "overrides")
	if err != nil || !found {
		t.Fatalf("route %q overrides=%#v found=%t err=%v, want supported enable override", routeName, overrides, found, err)
	}
	mode, found, err := unstructured.NestedMap(overrides, "processing_mode")
	if err != nil || !found || mode["request_header_mode"] != "SEND" ||
		mode["request_body_mode"] != requestMode || mode["response_body_mode"] != responseMode {
		t.Fatalf("route %q processing mode=%#v found=%t err=%v, want request=%s response=%s", routeName, mode, found, err, requestMode, responseMode)
	}
	sharedDisabled, found, err := unstructured.NestedBool(patch, "patch", "value", "typed_per_filter_config", "envoy.filters.http.ext_proc.ipp", "disabled")
	if err != nil || !found || !sharedDisabled {
		t.Fatalf("route %q shared ipp disabled=%t found=%t err=%v, want disabled", routeName, sharedDisabled, found, err)
	}
	preOverrides, found, err := unstructured.NestedMap(patch, "patch", "value", "typed_per_filter_config", externalModelPreExtProcFilter, "overrides")
	if err != nil || !found {
		t.Fatalf("route %q pre-auth overrides=%#v found=%t err=%v, want fail-closed ExternalModel pre-auth", routeName, preOverrides, found, err)
	}
	preMode, found, err := unstructured.NestedMap(preOverrides, "processing_mode")
	if err != nil || !found || preMode["request_body_mode"] != "BUFFERED" {
		t.Fatalf("route %q pre-auth processing mode=%#v found=%t err=%v, want BUFFERED for model extraction", routeName, preMode, found, err)
	}
	preDisabled, found, err := unstructured.NestedBool(patch, "patch", "value", "typed_per_filter_config", "envoy.filters.http.ext_proc.ipp-pre", "disabled")
	if err != nil || !found || !preDisabled {
		t.Fatalf("route %q shared pre-auth disabled=%t found=%t err=%v, want disabled", routeName, preDisabled, found, err)
	}
}

func TestEnableExternalModelRoutesDeletesOnlyOwnedRouteFilterWhenEmpty(t *testing.T) {
	owned := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "networking.istio.io/v1alpha3",
		"kind":       "EnvoyFilter",
		"metadata": map[string]any{
			"name":      tenant.PayloadProcessingExternalModelEnvoyFilterName("tenant-a"),
			"namespace": "gateway-system",
			"labels":    map[string]any{"app.kubernetes.io/managed-by": "ai-gateway-controller", externalTenantLabel: "tenant-a"},
		},
	}}
	foreign := owned.DeepCopy()
	foreign.SetName("foreign-routes")
	foreign.SetLabels(map[string]string{"app.kubernetes.io/managed-by": "other-controller"})
	neighbor := owned.DeepCopy()
	neighbor.SetNamespace("other-gateway-system")
	neighbor.SetLabels(map[string]string{"app.kubernetes.io/managed-by": "ai-gateway-controller", externalTenantLabel: "tenant-b"})
	r := controllerTestClient(t, owned, foreign, neighbor)

	testTenant := attachmentTenant{namespace: "tenant-a", tenantID: "tenant-a",
		gateway: v1alpha1.NamespacedObjectReference{Name: "test-gateway", Namespace: "gateway-system"}}
	if _, err := r.enableExternalModelRoutes(context.Background(), testTenant, nil); err != nil {
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
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(neighbor), &got); err != nil {
		t.Fatalf("neighbor route EnvoyFilter with the same name was removed: %v", err)
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
	route := resolver.Route{Model: "model", ClientName: "client-model", Provider: "anthropic", Endpoint: "api.example.com", APIFormat: "messages", Path: "/v1/messages", APITranslationEnabled: true}
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
	if nestedString(t, selectedHeader, "name") != "X-AI-Routing-Candidate" || nestedString(t, selectedHeader, "value") != envelope.CandidateStableID("model", "anthropic") {
		t.Fatalf("trusted provider header = %q=%q", nestedString(t, selectedHeader, "name"), nestedString(t, selectedHeader, "value"))
	}
	// The exact target path is another match on the body-routing rule. Its
	// candidate header makes it more specific than a shared exact-path route.
	bodyMatches := nestedSlice(t, nestedMapAt(t, rules, 1), "matches")
	if len(bodyMatches) != 2 {
		t.Fatalf("body rule matches = %d, want exact target-path and model-header matches", len(bodyMatches))
	}
	var foundTarget, foundModelHeader bool
	for _, raw := range bodyMatches {
		match, ok := raw.(map[string]any)
		if !ok {
			t.Fatalf("body match has type %T, want map[string]any", raw)
		}
		pathType, hasPath, pathErr := unstructured.NestedString(match, "path", "type")
		pathValue, _, valueErr := unstructured.NestedString(match, "path", "value")
		if pathErr != nil || valueErr != nil {
			t.Fatalf("read target path match: type error=%v value error=%v", pathErr, valueErr)
		}
		matchHeaders := nestedSlice(t, match, "headers")
		if hasPath && pathType == "Exact" && pathValue == "/v1/messages" {
			foundTarget = len(matchHeaders) == 1 &&
				nestedString(t, nestedMapAt(t, matchHeaders, 0), "name") == selectedProviderHeader &&
				nestedString(t, nestedMapAt(t, matchHeaders, 0), "value") == envelope.CandidateStableID("model", "anthropic")
		}
		if len(matchHeaders) == 2 && nestedString(t, nestedMapAt(t, matchHeaders, 0), "name") == "X-Gateway-Model-Name" {
			foundModelHeader = nestedString(t, nestedMapAt(t, matchHeaders, 0), "value") == "client-model" &&
				nestedString(t, nestedMapAt(t, matchHeaders, 1), "name") == selectedProviderHeader &&
				nestedString(t, nestedMapAt(t, matchHeaders, 1), "value") == envelope.CandidateStableID("model", "anthropic")
		}
	}
	if !foundTarget || !foundModelHeader {
		t.Fatalf("body rule matches = %#v, want exact target path and model-header candidate matches", bodyMatches)
	}
	targetBackend := nestedMapAt(t, nestedSlice(t, nestedMapAt(t, rules, 1), "backendRefs"), 0)
	if got := nestedString(t, targetBackend, "name"); got != providerServicePrefix+"anthropic" {
		t.Fatalf("body route backend = %q, want provider service", got)
	}
	pathMatches := nestedSlice(t, nestedMapAt(t, rules, 2), "matches")
	path := nestedString(t, nestedMapAt(t, pathMatches, 0), "path", "value")
	if path != "/tenant-a/client-model" {
		t.Fatalf("path route = %q", path)
	}
	backendRefs := nestedSlice(t, nestedMapAt(t, rules, 3), "backendRefs")
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
	modelHeaderMatch := nestedMapAt(t, bodyMatches, 0)
	modelHeaders := nestedSlice(t, modelHeaderMatch, "headers")
	if nestedString(t, nestedMapAt(t, modelHeaders, 0), "name") != "X-Gateway-Model-Name" ||
		nestedString(t, nestedMapAt(t, modelHeaders, 0), "value") != "client-model" {
		t.Fatalf("body route model header = %#v", modelHeaders)
	}
	if _, found, err := unstructured.NestedString(modelHeaderMatch, "path", "value"); err != nil || found {
		t.Fatalf("body route model-header match must remain path-independent: found=%v err=%v", found, err)
	}
}

func TestModelHTTPRouteUsesMainMatchSetForDefaultOpenAI(t *testing.T) {
	route := resolver.Route{Model: "model", ClientName: "client-model", Provider: "openai", Endpoint: "api.example.com", APIFormat: "openai-chat", Path: "/v1/chat/completions"}
	obj := modelHTTPRoute(route, "tenant-a", "gateway", "maas-system")
	rules := nestedSlice(t, obj.Object, "spec", "rules")
	if len(rules) != 4 {
		t.Fatalf("OpenAI route rules = %d, want main's two provider rules and two fail-closed fallbacks", len(rules))
	}
	bodyRule := nestedMapAt(t, rules, 1)
	matches := nestedSlice(t, bodyRule, "matches")
	if len(matches) != 1 {
		t.Fatalf("default OpenAI body rule matches = %#v, want only main's model-header match", matches)
	}
	bodyHeaders := nestedSlice(t, nestedMapAt(t, matches, 0), "headers")
	if len(bodyHeaders) != 2 ||
		nestedString(t, nestedMapAt(t, bodyHeaders, 0), "name") != "X-Gateway-Model-Name" ||
		nestedString(t, nestedMapAt(t, bodyHeaders, 0), "value") != "client-model" ||
		nestedString(t, nestedMapAt(t, bodyHeaders, 1), "name") != selectedProviderHeader ||
		nestedString(t, nestedMapAt(t, bodyHeaders, 1), "value") != envelope.CandidateStableID("model", "openai") {
		t.Fatalf("default OpenAI body match headers = %#v, want model plus trusted selected-provider headers", bodyHeaders)
	}
	if _, found, err := unstructured.NestedString(nestedMapAt(t, matches, 0), "path", "value"); err != nil || found {
		t.Fatalf("default OpenAI main-compatible body match unexpectedly constrains path: found=%v err=%v", found, err)
	}
	backend := nestedMapAt(t, nestedSlice(t, bodyRule, "backendRefs"), 0)
	if got := nestedString(t, backend, "name"); got != providerServicePrefix+"openai" {
		t.Fatalf("OpenAI body-route backend = %q, want selected provider", got)
	}
}

func TestOpenAIOnlyMultipleProvidersKeepMainMatchSet(t *testing.T) {
	routes := []resolver.Route{
		{Model: "model", ClientName: "chat", Provider: "provider-b", APIFormat: "openai-chat", Path: "/v1/chat/completions"},
		{Model: "model", ClientName: "chat", Provider: "provider-a", APIFormat: "openai-chat", Path: "/v1/chat/completions"},
	}
	obj := modelHTTPRouteSet(routes, "tenant-a", "gateway", "gateway-system")
	rules := nestedSlice(t, obj.Object, "spec", "rules")
	if len(rules) != 6 {
		t.Fatalf("OpenAI-only rules = %d, want two rules per provider plus two fallbacks", len(rules))
	}
	for i, route := range routes {
		bodyMatches := nestedSlice(t, nestedMapAt(t, rules, i*2+1), "matches")
		if len(bodyMatches) != 1 {
			t.Fatalf("provider %s body matches = %#v, want only main's model-header match", route.Provider, bodyMatches)
		}
		match := nestedMapAt(t, bodyMatches, 0)
		if _, found, err := unstructured.NestedString(match, "path", "value"); err != nil || found {
			t.Fatalf("provider %s unexpectedly adds a target-path match: found=%v err=%v", route.Provider, found, err)
		}
		headers := nestedSlice(t, match, "headers")
		if len(headers) != 2 || nestedString(t, nestedMapAt(t, headers, 1), "value") != envelope.CandidateStableID(route.Model, route.Provider) {
			t.Fatalf("provider %s match headers = %#v, want its trusted candidate", route.Provider, headers)
		}
	}
}

func TestTranslationEnabledOpenAIExactTargetPathUsesSelectedProvider(t *testing.T) {
	routes := []resolver.Route{
		{Model: "model", ClientName: "chat", Provider: "provider-b", APIFormat: "openai-chat", Path: "/v1/chat/completions", APITranslationEnabled: true},
		{Model: "model", ClientName: "chat", Provider: "provider-a", APIFormat: "openai-chat", Path: "/v1/chat/completions", APITranslationEnabled: true},
	}
	obj := modelHTTPRouteSet(routes, "tenant-a", "gateway", "gateway-system")
	rules := nestedSlice(t, obj.Object, "spec", "rules")
	if len(rules) != 6 {
		t.Fatalf("translation-enabled OpenAI rules = %d, want two rules per provider plus two fallbacks", len(rules))
	}
	for i, route := range routes {
		bodyMatches := nestedSlice(t, nestedMapAt(t, rules, i*2+1), "matches")
		if len(bodyMatches) != 2 {
			t.Fatalf("provider %s body matches = %#v, want model-header and exact target-path matches", route.Provider, bodyMatches)
		}
		stableID := envelope.CandidateStableID(route.Model, route.Provider)
		foundExactTargetPath := false
		for _, raw := range bodyMatches {
			match, ok := raw.(map[string]any)
			if !ok {
				t.Fatalf("body match has type %T, want map[string]any", raw)
			}
			pathType, hasPath, pathTypeErr := unstructured.NestedString(match, "path", "type")
			pathValue, _, pathValueErr := unstructured.NestedString(match, "path", "value")
			if pathTypeErr != nil || pathValueErr != nil {
				t.Fatalf("read exact target path: type error=%v value error=%v", pathTypeErr, pathValueErr)
			}
			if hasPath && pathType == "Exact" && pathValue == route.Path {
				headers := nestedSlice(t, match, "headers")
				foundExactTargetPath = len(headers) == 1 &&
					nestedString(t, nestedMapAt(t, headers, 0), "name") == selectedProviderHeader &&
					nestedString(t, nestedMapAt(t, headers, 0), "value") == stableID
			}
		}
		if !foundExactTargetPath {
			t.Fatalf("provider %s lacks exact target path %q gated by selected candidate %q: %#v",
				route.Provider, route.Path, stableID, bodyMatches)
		}
	}
}

func TestProviderTargetPathMatchRequiresTranslationPermission(t *testing.T) {
	route := resolver.Route{
		Model: "model", ClientName: "chat", Provider: "openai", APIFormat: "openai-chat",
		Path: "/v1/responses", TargetModel: "deployed-model",
	}
	var initialFlagOff []byte
	for index, tc := range []struct {
		name                string
		translationEnabled  bool
		wantTargetPathMatch bool
	}{
		{name: "flag off preserves main match set"},
		{name: "flag on routes rewritten provider path", translationEnabled: true, wantTargetPathMatch: true},
		{name: "flag off after disable removes stale path match"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			route.APITranslationEnabled = tc.translationEnabled
			obj := modelHTTPRouteSet([]resolver.Route{route}, "tenant-a", "gateway", "gateway-system")
			switch index {
			case 0:
				initialJSON, err := json.Marshal(obj.Object["spec"])
				if err != nil {
					t.Fatalf("marshal initial flag-off HTTPRoute spec: %v", err)
				}
				initialFlagOff = append([]byte(nil), initialJSON...)
			case 2:
				finalFlagOff, err := json.Marshal(obj.Object["spec"])
				if err != nil {
					t.Fatalf("marshal final flag-off HTTPRoute spec: %v", err)
				}
				if !bytes.Equal(finalFlagOff, initialFlagOff) {
					t.Fatalf("flag-off HTTPRoute after toggle differs from initial main-compatible spec:\n got %s\nwant %s", finalFlagOff, initialFlagOff)
				}
			}
			rules := nestedSlice(t, obj.Object, "spec", "rules")
			if len(rules) != 4 {
				t.Fatalf("HTTPRoute rules = %d, want four rules for one provider", len(rules))
			}
			bodyMatches := nestedSlice(t, nestedMapAt(t, rules, 1), "matches")
			wantMatches := 1
			if tc.wantTargetPathMatch {
				wantMatches++
			}
			if len(bodyMatches) != wantMatches {
				t.Fatalf("body-routing matches = %#v, want %d match alternatives", bodyMatches, wantMatches)
			}
			foundTargetPath := false
			for _, raw := range bodyMatches {
				match, ok := raw.(map[string]any)
				if !ok {
					t.Fatalf("body match has type %T, want map[string]any", raw)
				}
				path, found, err := unstructured.NestedString(match, "path", "value")
				if err != nil {
					t.Fatalf("read body match path: %v", err)
				}
				if found && path == route.Path {
					foundTargetPath = true
				}
			}
			if foundTargetPath != tc.wantTargetPathMatch {
				t.Fatalf("target path match present=%t, want %t: %#v", foundTargetPath, tc.wantTargetPathMatch, bodyMatches)
			}
		})
	}
}

func TestModelHTTPRouteHasOneTrustedRulePerProviderBeforeFallback(t *testing.T) {
	routes := []resolver.Route{
		{Model: "model", ClientName: "chat", Provider: "provider-b", APIFormat: "messages", Path: "/v1/messages", APITranslationEnabled: true},
		{Model: "model", ClientName: "chat", Provider: "provider-a", APIFormat: "openai-chat", Path: "/v1/chat/completions", APITranslationEnabled: true},
	}
	obj := modelHTTPRouteSet(routes, "tenant-a", "gateway", "gateway-system")
	rules := nestedSlice(t, obj.Object, "spec", "rules")
	if len(rules) != 6 {
		t.Fatalf("rules = %d, want two provider rules per candidate plus path and body fallback", len(rules))
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
		targetMatches := nestedSlice(t, nestedMapAt(t, rules, ruleIndex+1), "matches")
		if routes[i].Path == "/v1/chat/completions" && !routes[i].APITranslationEnabled {
			if len(targetMatches) != 1 {
				t.Fatalf("provider %s flag-off default OpenAI path matches = %#v, want only the model-header match", providerName, targetMatches)
			}
			continue
		}
		var targetMatch map[string]any
		for _, rawMatch := range targetMatches {
			match, ok := rawMatch.(map[string]any)
			if !ok {
				continue
			}
			pathType, hasPath, pathErr := unstructured.NestedString(match, "path", "type")
			pathValue, _, valueErr := unstructured.NestedString(match, "path", "value")
			if pathErr != nil || valueErr != nil {
				t.Fatalf("read target path for provider %s: type error=%v value error=%v", providerName, pathErr, valueErr)
			}
			if hasPath && pathType == "Exact" && pathValue == routes[i].Path {
				targetMatch = match
				break
			}
		}
		if targetMatch == nil {
			t.Fatalf("provider %s body rule lacks exact target path %q: %#v", providerName, routes[i].Path, targetMatches)
		}
		targetHeader := nestedMapAt(t, nestedSlice(t, targetMatch, "headers"), 0)
		if got := nestedString(t, targetHeader, "value"); got != wantStableID {
			t.Fatalf("provider %s target path candidate = %q, want %q", providerName, got, wantStableID)
		}
	}
}

func TestModelHTTPRouteFailsClosedWithoutTrustedSelection(t *testing.T) {
	routes := []resolver.Route{
		{Model: "model", ClientName: "chat", Provider: "provider-a", Endpoint: "a.example.com", APIFormat: "messages", Path: "/v1/messages", APITranslationEnabled: true},
		{Model: "model", ClientName: "chat", Provider: "provider-b", Endpoint: "b.example.com", APIFormat: "messages", Path: "/v1/messages", APITranslationEnabled: true},
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

func TestExternalModelHTTPRouteRuleLimitByFormat(t *testing.T) {
	for _, tc := range []struct {
		name, apiFormat, path string
		providers             int
		wantRules             int
		wantRejected          bool
	}{
		{"seven OpenAI providers", "openai-chat", "/v1/chat/completions", 7, 16, false},
		{"seven Messages providers fit limit", "messages", "/v1/messages", 7, 16, false},
		{"eight Messages providers exceed limit", "messages", "/v1/messages", 8, 18, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			routes := make([]resolver.Route, 0, tc.providers)
			for i := range tc.providers {
				routes = append(routes, resolver.Route{Model: "model", ClientName: "client", Provider: fmt.Sprintf("provider-%d", i), APIFormat: tc.apiFormat, Path: tc.path, Weight: 1})
			}
			rendered := modelHTTPRouteSet(routes, "tenant-a", "gateway", "gateway-system")
			rules := nestedSlice(t, rendered.Object, "spec", "rules")
			if got := modelHTTPRouteRuleCount(routes); got != tc.wantRules || len(rules) != got {
				t.Fatalf("rule count = %d, rendered rules = %d, want %d", got, len(rules), tc.wantRules)
			}
			set := &resolver.ResolvedRouteSet{Models: []resolver.ModelRoutes{{ModelRef: "tenant-a/model", Routes: routes}}}
			_, rejected := excludeUnsupportedModelSelection(set)
			if (rejected["tenant-a/model"] != nil) != tc.wantRejected {
				t.Fatalf("rejected = %v, want %t", rejected["tenant-a/model"], tc.wantRejected)
			}
		})
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

func TestProviderTransportUsesResolvedPort(t *testing.T) {
	for _, tc := range []struct {
		name string
		port int64
		want int64
	}{
		{name: "annotated port", port: 8000, want: 8000},
		{name: "default HTTPS port", want: 443},
	} {
		t.Run(tc.name, func(t *testing.T) {
			route := resolver.Route{Provider: "provider", Endpoint: "provider.example.com", Port: tc.port}
			service := providerService(route, "tenant-a")
			if got := nestedMapAt(t, nestedSlice(t, service.Object, "spec", "ports"), 0)["port"]; got != tc.want {
				t.Fatalf("Service port = %v, want %d", got, tc.want)
			}
			entry := providerServiceEntry(route, "tenant-a")
			if got := nestedMapAt(t, nestedSlice(t, entry.Object, "spec", "ports"), 0)["number"]; got != tc.want {
				t.Fatalf("ServiceEntry port = %v, want %d", got, tc.want)
			}
			httpRoute := modelHTTPRoute(route, "tenant-a", "gateway", "gateway-system")
			rules := nestedSlice(t, httpRoute.Object, "spec", "rules")
			if got := nestedMapAt(t, nestedSlice(t, nestedMapAt(t, rules, 0), "backendRefs"), 0)["port"]; got != tc.want {
				t.Fatalf("HTTPRoute backend port = %v, want %d", got, tc.want)
			}
		})
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

func TestProviderTransportUsesCredentialNameForCATrust(t *testing.T) {
	route := resolver.Route{Provider: "provider", Endpoint: "provider.example.com", TLSCredentialName: "provider-ca"}
	destination := providerDestinationRuleAtGateway(route, "gateway", "gateway-system", "models-as-a-service")
	tls, _, _ := unstructured.NestedMap(destination.Object, "spec", "trafficPolicy", "tls")
	want := map[string]any{"mode": "SIMPLE", "sni": "provider.example.com", "credentialName": "provider-ca"}
	if !reflect.DeepEqual(tls, want) {
		t.Fatalf("DestinationRule TLS = %#v, want %#v", tls, want)
	}
}

func TestValidateProviderTLSConfig(t *testing.T) {
	tests := []struct {
		name   string
		config map[string]string
		want   string
	}{
		{name: "credential name", config: map[string]string{"tls.credentialName": "provider-ca"}},
		{name: "CA file", config: map[string]string{"tls.caCertificates": "/etc/provider-ca/ca.crt"}},
		{name: "invalid Secret name", config: map[string]string{"tls.credentialName": "Provider_CA"}, want: "config tls.credentialName must be a valid Secret name"},
		{name: "both", config: map[string]string{"tls.caCertificates": "/etc/provider-ca/ca.crt", "tls.credentialName": "provider-ca"}, want: "config tls.caCertificates and tls.credentialName are mutually exclusive"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			provider := &v1alpha1.ExternalProvider{ObjectMeta: metav1.ObjectMeta{Name: "provider", Namespace: "tenant-a"}, Spec: v1alpha1.ExternalProviderSpec{
				Endpoint: "provider.example.com", Config: tc.config,
				Auth: v1alpha1.AuthConfig{Type: "apikey", SecretRef: v1alpha1.NameReference{Name: "credentials"}},
			}}
			err := (&Reconciler{}).validateProvider(context.Background(), provider)
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
		{name: "simple is apikey", authType: "simple", secretData: map[string][]byte{"api-key": []byte("fixture")}},
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
		Spec:       v1alpha1.ExternalModelSpec{ExternalProviderRefs: []v1alpha1.ExternalProviderRef{{Ref: v1alpha1.ExternalProviderReference{Name: "provider"}}}},
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

func TestMaasTenantConfigEventsFanOutOnlyWithinConfigNamespace(t *testing.T) {
	modelA := &v1alpha1.ExternalModel{ObjectMeta: metav1.ObjectMeta{Name: "a", Namespace: "tenant-a"}}
	modelB := &v1alpha1.ExternalModel{ObjectMeta: metav1.ObjectMeta{Name: "b", Namespace: "tenant-b"}}
	r := controllerTestClient(t, modelA, modelB)
	if got := r.maasTenantConfigModels(context.Background(), controllerMaasTenantConfig("tenant-a", "tenant", "models-as-a-service", "praxis")); len(got) != 1 || got[0].NamespacedName != client.ObjectKeyFromObject(modelA) {
		t.Fatalf("MaaS TenantConfig event enqueued %#v, want only tenant-a model", got)
	}
	other := controllerMaasTenantConfig("tenant-b", "tenant-b", "models-as-a-service", "praxis")
	if got := r.maasTenantConfigModels(context.Background(), other); len(got) != 1 || got[0].NamespacedName != client.ObjectKeyFromObject(modelB) {
		t.Fatalf("second MaaS TenantConfig event enqueued %#v, want only tenant-b model", got)
	}
	r.Namespace = "tenant-a"
	if got := r.maasTenantConfigModels(context.Background(), other); len(got) != 0 {
		t.Fatalf("out-of-scope MaaS TenantConfig event enqueued %#v", got)
	}
}

func TestAIGuardrailEventsEnqueueTenantAndGatewayAttachedModels(t *testing.T) {
	tenantGateway := v1alpha1.NamespacedObjectReference{Name: "gateway", Namespace: "gateway-system"}
	tenantModel := &v1alpha1.ExternalModel{ObjectMeta: metav1.ObjectMeta{Name: "tenant", Namespace: "tenant-a"}, Spec: v1alpha1.ExternalModelSpec{GatewayRefs: []v1alpha1.NamespacedObjectReference{tenantGateway}}}
	attachedModel := &v1alpha1.ExternalModel{ObjectMeta: metav1.ObjectMeta{Name: "attached", Namespace: "team-a"}, Spec: v1alpha1.ExternalModelSpec{GatewayRefs: []v1alpha1.NamespacedObjectReference{tenantGateway}}}
	unrelatedModel := &v1alpha1.ExternalModel{ObjectMeta: metav1.ObjectMeta{Name: "unrelated", Namespace: "team-b"}, Spec: v1alpha1.ExternalModelSpec{GatewayRefs: []v1alpha1.NamespacedObjectReference{gatewayB}}}
	guardrail := &aigatewayv1alpha1.AIGuardrail{ObjectMeta: metav1.ObjectMeta{Name: "safety", Namespace: "tenant-a"}}
	ait := controllerAITenant("tenant-a", "models-as-a-service", "tenant-a", tenantGateway.Name, tenantGateway.Namespace)
	mtc := controllerMaasTenantConfig("tenant-a", "tenant-a", "models-as-a-service", "praxis")
	r := controllerTestClient(t, tenantModel, attachedModel, unrelatedModel, guardrail, ait, mtc)

	want := map[types.NamespacedName]bool{
		client.ObjectKeyFromObject(tenantModel):   true,
		client.ObjectKeyFromObject(attachedModel): true,
	}
	got, err := r.postAuthFilterModels(context.Background(), guardrail.Namespace)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(want) {
		t.Fatalf("AIGuardrail event enqueued %#v, want %#v", got, want)
	}
	for _, request := range got {
		if !want[request.NamespacedName] {
			t.Fatalf("AIGuardrail event unexpectedly enqueued %s", request.NamespacedName)
		}
		delete(want, request.NamespacedName)
	}
	if len(want) != 0 {
		t.Fatalf("AIGuardrail event missed %#v", want)
	}
}

func TestDefaultTenantAIGuardrailEventsEnqueueImplicitModels(t *testing.T) {
	implicitModel := &v1alpha1.ExternalModel{ObjectMeta: metav1.ObjectMeta{Name: "implicit", Namespace: "team-a"}}
	guardrail := &aigatewayv1alpha1.AIGuardrail{ObjectMeta: metav1.ObjectMeta{Name: "safety", Namespace: "models-as-a-service"}}
	defaultTenant := controllerAITenant(tenant.DefaultAITenantName, "models-as-a-service", "models-as-a-service", defaultGateway.Name, defaultGateway.Namespace)
	mtc := controllerMaasTenantConfig("models-as-a-service", tenant.DefaultAITenantName, "models-as-a-service", "praxis")
	r := controllerTestClient(t, implicitModel, guardrail, defaultTenant, mtc)

	got, err := r.postAuthFilterModels(context.Background(), guardrail.Namespace)
	want := reconcile.Request{NamespacedName: client.ObjectKeyFromObject(implicitModel)}
	if err != nil || len(got) != 1 || got[0] != want {
		t.Fatalf("default tenant AIGuardrail event enqueued %#v, %v; want %#v", got, err, want)
	}
}

func TestPostAuthFilterEventRetriesATransientTenantReadFailure(t *testing.T) {
	setPostAuthFilterRetryDelays(t, time.Millisecond)
	tenantGateway := v1alpha1.NamespacedObjectReference{Name: "gateway", Namespace: "gateway-system"}
	tenantModel := &v1alpha1.ExternalModel{ObjectMeta: metav1.ObjectMeta{Name: "tenant", Namespace: "tenant-a"}, Spec: v1alpha1.ExternalModelSpec{GatewayRefs: []v1alpha1.NamespacedObjectReference{tenantGateway}}}
	attachedModel := &v1alpha1.ExternalModel{ObjectMeta: metav1.ObjectMeta{Name: "attached", Namespace: "team-a"}, Spec: v1alpha1.ExternalModelSpec{GatewayRefs: []v1alpha1.NamespacedObjectReference{tenantGateway}}}
	ait := controllerAITenant("tenant-a", "models-as-a-service", "tenant-a", tenantGateway.Name, tenantGateway.Namespace)
	mtc := controllerMaasTenantConfig("tenant-a", "tenant-a", "models-as-a-service", "praxis")
	r := controllerTestClient(t, tenantModel, attachedModel, ait, mtc)
	var failures atomic.Int32
	failures.Store(2)
	base, ok := r.Client.(client.WithWatch)
	if !ok {
		t.Fatal("the test client must support watches")
	}
	r.Client = interceptor.NewClient(base, interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if obj.GetObjectKind().GroupVersionKind().Kind == tenant.MaasTenantConfigGVK.Kind && failures.Add(-1) >= 0 {
				return apierrors.NewServiceUnavailable("etcd is unavailable")
			}
			return c.Get(ctx, key, obj, opts...)
		},
	})

	q := newTestQueue(t)
	r.enqueuePostAuthFilterModels(context.Background(), q, "tenant-a", 0)
	// The model in the tenant's namespace is queued even though the lookup failed.
	if got := drainQueue(q); !reflect.DeepEqual(got, []reconcile.Request{{NamespacedName: client.ObjectKeyFromObject(tenantModel)}}) {
		t.Fatalf("first attempt enqueued %v, want only the namespace-local model", got)
	}
	want := reconcile.Request{NamespacedName: client.ObjectKeyFromObject(attachedModel)}
	deadline := time.Now().Add(5 * time.Second)
	for !slices.Contains(drainQueue(q), want) {
		if time.Now().After(deadline) {
			t.Fatal("gateway-attached model was never enqueued after the tenant read recovered")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestPostAuthFilterEventDoesNotRetryAPermanentTenantFailure(t *testing.T) {
	setPostAuthFilterRetryDelays(t, time.Millisecond)
	tenantModel := &v1alpha1.ExternalModel{ObjectMeta: metav1.ObjectMeta{Name: "tenant", Namespace: "tenant-a"}}
	// This AITenant points at another namespace, so it doesn't own the MaasTenantConfig.
	ait := controllerAITenant("tenant", "models-as-a-service", "other-tenant", "gateway", "gateway-system")
	mtc := controllerMaasTenantConfig("tenant-a", "tenant", "models-as-a-service", "praxis")
	r := controllerTestClient(t, tenantModel, ait, mtc)
	var reads atomic.Int32
	base, ok := r.Client.(client.WithWatch)
	if !ok {
		t.Fatal("the test client must support watches")
	}
	r.Client = interceptor.NewClient(base, interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if obj.GetObjectKind().GroupVersionKind().Kind == tenant.MaasTenantConfigGVK.Kind {
				reads.Add(1)
			}
			return c.Get(ctx, key, obj, opts...)
		},
	})

	q := newTestQueue(t)
	r.enqueuePostAuthFilterModels(context.Background(), q, "tenant-a", 0)
	if got := drainQueue(q); !reflect.DeepEqual(got, []reconcile.Request{{NamespacedName: client.ObjectKeyFromObject(tenantModel)}}) {
		t.Fatalf("enqueued %v, want the namespace-local model kept", got)
	}
	time.Sleep(50 * time.Millisecond)
	if n := reads.Load(); n != 1 {
		t.Fatalf("MaasTenantConfig read %d times, want 1: an ownership error must not be retried", n)
	}
}

func setPostAuthFilterRetryDelays(t *testing.T, delay time.Duration) {
	t.Helper()
	saved := postAuthFilterRetryDelays
	postAuthFilterRetryDelays = []time.Duration{delay, delay, delay}
	t.Cleanup(func() { postAuthFilterRetryDelays = saved })
}

func newTestQueue(t *testing.T) workqueue.TypedRateLimitingInterface[reconcile.Request] {
	t.Helper()
	q := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[reconcile.Request]())
	t.Cleanup(q.ShutDown)
	return q
}

// drainQueue returns the requests queued so far without blocking.
func drainQueue(q workqueue.TypedRateLimitingInterface[reconcile.Request]) []reconcile.Request {
	var out []reconcile.Request
	for q.Len() > 0 {
		request, _ := q.Get()
		q.Done(request)
		out = append(out, request)
	}
	return out
}

func TestPraxisTenantUsesMaasTenantConfigSelector(t *testing.T) {
	ait := controllerAITenant("tenant", "models-as-a-service", "tenant-a", "gateway", "gateway-system")
	mtc := controllerMaasTenantConfig("tenant-a", "tenant", "models-as-a-service", "praxis")
	r := controllerTestClient(t, ait, mtc)
	got, found, err := r.praxisTenantForNamespace(context.Background(), "tenant-a")
	if err != nil || !found || got.GetName() != "tenant" {
		t.Fatalf("MaaS tenant config lookup = %v, %v, %v", got, found, err)
	}
	ait.SetAnnotations(map[string]string{tenant.AnnotationPayloadProcessingType: tenant.PayloadProcessingBackendIPP})
	if err := r.Update(context.Background(), ait); err != nil {
		t.Fatal(err)
	}
	got, found, err = r.praxisTenantForNamespace(context.Background(), "tenant-a")
	if err != nil || !found || got.GetName() != "tenant" {
		t.Fatalf("AITenant selector unexpectedly changed selection: %v, %v, %v", got, found, err)
	}
	mtc.SetAnnotations(map[string]string{tenant.AnnotationPayloadProcessingType: tenant.PayloadProcessingBackendIPP, tenant.AnnotationAITenantName: "tenant", tenant.AnnotationAITenantNamespace: "models-as-a-service"})
	if err := r.Update(context.Background(), mtc); err != nil {
		t.Fatal(err)
	}
	if _, selected, err := r.praxisTenantForNamespace(context.Background(), "tenant-a"); err != nil || selected {
		t.Fatalf("MaaS TenantConfig ipp selection = %v, want false", err)
	}
}

func controllerAITenant(name, namespace, tenantNamespace, gatewayName, gatewayNamespace string) *unstructured.Unstructured {
	u := tenant.NewAITenant()
	u.SetName(name)
	u.SetNamespace(namespace)
	u.SetGeneration(1)
	u.Object["status"] = map[string]any{
		"tenantNamespace": tenantNamespace,
		"phase":           tenant.AITenantPhaseActive,
		"gatewayRef":      map[string]any{"name": gatewayName, "namespace": gatewayNamespace},
		"conditions": []any{map[string]any{
			"type":               tenant.AITenantConditionReady,
			"status":             string(metav1.ConditionTrue),
			"observedGeneration": int64(1),
		}},
	}
	return u
}

func controllerMaasTenantConfig(namespace, aitenantName, aitenantNamespace, selector string) *unstructured.Unstructured {
	u := tenant.NewMaasTenantConfig()
	u.SetName(tenant.MaasTenantConfigInstanceName)
	u.SetNamespace(namespace)
	u.SetAnnotations(map[string]string{
		tenant.AnnotationPayloadProcessingType:   selector,
		tenant.AnnotationAITenantName:            aitenantName,
		tenant.AnnotationAITenantNamespace:       aitenantNamespace,
		tenant.AnnotationPayloadProcessingStatus: tenant.PayloadProcessingStatusSteady,
	})
	return u
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

// TestReconcilePendingPraxisTenantWithGatewayRefPublishes asserts ExternalModel
// publish does not wait for AITenant Active: Pending + current Ready stamp +
// gatewayRef + steady is enough (DeployReady).
func TestReconcilePendingPraxisTenantWithGatewayRefPublishes(t *testing.T) {
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
			Ref: v1alpha1.ExternalProviderReference{Name: provider.Name}, TargetModel: "gpt", APIFormat: "openai-chat", Path: "/v1/chat/completions",
		}}},
	}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "credentials", Namespace: "tenant-a"}, Data: map[string][]byte{"api-key": []byte("secret")}}
	ait := controllerAITenant("tenant", "models-as-a-service", "tenant-a", "gateway", "gateway-system")
	status, ok := ait.Object["status"].(map[string]any)
	if !ok {
		t.Fatal("AITenant fixture status is not an object")
	}
	status["phase"] = "Pending"
	if conditions, ok := status["conditions"].([]any); ok && len(conditions) > 0 {
		if cond, ok := conditions[0].(map[string]any); ok {
			cond["status"] = string(metav1.ConditionFalse)
		}
	}
	mtc := controllerMaasTenantConfig("tenant-a", "tenant", "models-as-a-service", "praxis")
	r := controllerTestClient(t, provider, model, secret, ait, mtc)
	r.Namespace, r.GatewayName, r.GatewayNamespace, r.Network = "tenant-a", "gateway", "gateway-system", "external-model"
	r.KnownClusters = []string{"provider-provider"}
	// ExternalModel publish only routes through Ready providers (#128).
	if _, err := r.ReconcileProvider(context.Background(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(provider)}); err != nil {
		t.Fatalf("reconcile provider: %v", err)
	}
	if _, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(model)}); err != nil {
		t.Fatal(err)
	}
	var overlay corev1.ConfigMap
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: "tenant-a", Name: "routing-overlay"}, &overlay); err != nil {
		t.Fatalf("Pending AITenant with gatewayRef must publish overlay: %v", err)
	}
	var got v1alpha1.ExternalModel
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(model), &got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != resolver.PhaseReady {
		t.Fatalf("phase = %q, want %q while AITenant is Pending", got.Status.Phase, resolver.PhaseReady)
	}
	for _, condition := range got.Status.Conditions {
		if condition.Reason == reasonTenantNotReady {
			t.Fatalf("conditions = %#v, must not report %q when DeployReady", got.Status.Conditions, reasonTenantNotReady)
		}
	}
}

func TestReconcileFailedAITenantDoesNotPublish(t *testing.T) {
	model := &v1alpha1.ExternalModel{
		ObjectMeta: metav1.ObjectMeta{Name: "model", Namespace: "tenant-a"},
		Spec:       v1alpha1.ExternalModelSpec{ModelName: "client-model"},
	}
	ait := controllerAITenant("tenant", "models-as-a-service", "tenant-a", "gateway", "gateway-system")
	status, ok := ait.Object["status"].(map[string]any)
	if !ok {
		t.Fatal("AITenant fixture status is not an object")
	}
	status["phase"] = tenant.AITenantPhaseFailed
	if conditions, ok := status["conditions"].([]any); ok && len(conditions) > 0 {
		if cond, ok := conditions[0].(map[string]any); ok {
			cond["status"] = string(metav1.ConditionFalse)
			cond["reason"] = "GatewayClaimFailed"
		}
	}
	mtc := controllerMaasTenantConfig("tenant-a", "tenant", "models-as-a-service", "praxis")
	r := controllerTestClient(t, model, ait, mtc)
	if _, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(model)}); err != nil {
		t.Fatal(err)
	}
	var overlay corev1.ConfigMap
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: "tenant-a", Name: "routing-overlay"}, &overlay); !apierrors.IsNotFound(err) {
		t.Fatalf("Failed AITenant overlay lookup error = %v, want NotFound", err)
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
}

func TestReconcileStaleAITenantStatusDoesNotPublish(t *testing.T) {
	model := &v1alpha1.ExternalModel{
		ObjectMeta: metav1.ObjectMeta{Name: "model", Namespace: "tenant-a"},
		Spec:       v1alpha1.ExternalModelSpec{ModelName: "client-model"},
	}
	ait := controllerAITenant("tenant", "models-as-a-service", "tenant-a", "gateway", "gateway-system")
	ait.SetGeneration(2)
	status, ok := ait.Object["status"].(map[string]any)
	if !ok {
		t.Fatal("AITenant fixture status is not an object")
	}
	if conditions, ok := status["conditions"].([]any); ok && len(conditions) > 0 {
		if cond, ok := conditions[0].(map[string]any); ok {
			cond["observedGeneration"] = int64(1)
		}
	}
	mtc := controllerMaasTenantConfig("tenant-a", "tenant", "models-as-a-service", "praxis")
	r := controllerTestClient(t, model, ait, mtc)
	if _, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(model)}); err != nil {
		t.Fatal(err)
	}
	var overlay corev1.ConfigMap
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: "tenant-a", Name: "routing-overlay"}, &overlay); !apierrors.IsNotFound(err) {
		t.Fatalf("stale AITenant overlay lookup error = %v, want NotFound", err)
	}
}

func TestReconcileWithoutGatewayRefDoesNotPublish(t *testing.T) {
	model := &v1alpha1.ExternalModel{
		ObjectMeta: metav1.ObjectMeta{Name: "model", Namespace: "tenant-a"},
		Spec:       v1alpha1.ExternalModelSpec{ModelName: "client-model"},
	}
	ait := controllerAITenant("tenant", "models-as-a-service", "tenant-a", "", "")
	mtc := controllerMaasTenantConfig("tenant-a", "tenant", "models-as-a-service", "praxis")
	r := controllerTestClient(t, model, ait, mtc)
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
	var overlay corev1.ConfigMap
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: "tenant-a", Name: "routing-overlay"}, &overlay); !apierrors.IsNotFound(err) {
		t.Fatalf("missing gatewayRef overlay lookup error = %v, want NotFound", err)
	}
}

func TestReconcileDoesNotPublishWithoutMaasTenantConfig(t *testing.T) {
	for _, withAITenant := range []bool{true, false} {
		t.Run(map[bool]string{true: "owner-present", false: "owner-absent"}[withAITenant], func(t *testing.T) {
			model := &v1alpha1.ExternalModel{
				ObjectMeta: metav1.ObjectMeta{Name: "model", Namespace: "tenant-a"},
			}
			controllerutil.AddFinalizer(model, externalModelFinalizer)
			overlay := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{
				Name:      "routing-overlay",
				Namespace: "tenant-a",
				Labels:    map[string]string{"app.kubernetes.io/managed-by": "ai-gateway-controller"},
			}}
			filter := &unstructured.Unstructured{Object: map[string]any{
				"apiVersion": "networking.istio.io/v1alpha3",
				"kind":       "EnvoyFilter",
				"metadata": map[string]any{
					"name":      tenant.PayloadProcessingExternalModelEnvoyFilterName("tenant"),
					"namespace": "gateway-system",
					"labels":    map[string]any{"app.kubernetes.io/managed-by": "ai-gateway-controller", externalTenantLabel: "tenant-a"},
				},
			}}
			objects := []client.Object{model, overlay, filter}
			if withAITenant {
				objects = append(objects, controllerAITenant("tenant", "models-as-a-service", "tenant-a", "gateway", "gateway-system"))
			}
			r := controllerTestClient(t, objects...)
			r.Namespace, r.GatewayName, r.GatewayNamespace = "tenant-a", "gateway", "gateway-system"
			if _, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(model)}); err != nil {
				t.Fatal(err)
			}
			var gotOverlay corev1.ConfigMap
			if err := r.Get(context.Background(), client.ObjectKeyFromObject(overlay), &gotOverlay); err != nil {
				t.Fatalf("missing MaasTenantConfig must retain serving overlay: %v", err)
			}
			var gotFilter unstructured.Unstructured
			gotFilter.SetGroupVersionKind(schema.GroupVersionKind{Group: "networking.istio.io", Version: "v1alpha3", Kind: "EnvoyFilter"})
			if err := r.Get(context.Background(), client.ObjectKeyFromObject(filter), &gotFilter); err != nil {
				t.Fatalf("missing MaasTenantConfig must retain route filter: %v", err)
			}
			var gotModel v1alpha1.ExternalModel
			if err := r.Get(context.Background(), client.ObjectKeyFromObject(model), &gotModel); err != nil {
				t.Fatal(err)
			}
			if !controllerutil.ContainsFinalizer(&gotModel, externalModelFinalizer) {
				t.Fatal("missing MaasTenantConfig must retain the ExternalModel cleanup finalizer")
			}
		})
	}
}

func TestExplicitIPPSwitchCleansOwnedResourcesWithoutGatewayStatus(t *testing.T) {
	model := &v1alpha1.ExternalModel{
		ObjectMeta: metav1.ObjectMeta{Name: "model", Namespace: "tenant-a"},
	}
	controllerutil.AddFinalizer(model, externalModelFinalizer)
	ait := controllerAITenant("tenant", "models-as-a-service", "tenant-a", "gateway", "gateway-system")
	status, ok := ait.Object["status"].(map[string]any)
	if !ok {
		t.Fatal("AITenant fixture status is not an object")
	}
	delete(status, "gatewayRef")
	mtc := controllerMaasTenantConfig("tenant-a", "tenant", "models-as-a-service", "ipp")
	overlay := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{
		Name: "routing-overlay", Namespace: "tenant-a",
		Labels: map[string]string{"app.kubernetes.io/managed-by": "ai-gateway-controller"},
	}}
	filter := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "networking.istio.io/v1alpha3",
		"kind":       "EnvoyFilter",
		"metadata": map[string]any{
			"name":      tenant.PayloadProcessingExternalModelEnvoyFilterName("tenant"),
			"namespace": "gateway-system",
			"labels":    map[string]any{"app.kubernetes.io/managed-by": "ai-gateway-controller", externalTenantLabel: "tenant-a"},
		},
	}}
	legacy := legacyExternalModelRouteFilter(tenant.PayloadProcessingExternalModelEnvoyFilterName("tenant"), "old-gateway-system", "ai-gateway-controller")
	neighborLegacy := legacyExternalModelRouteFilter(tenant.PayloadProcessingExternalModelEnvoyFilterName("neighbor"), "old-gateway-system", "ai-gateway-controller")
	foreignLegacy := legacyExternalModelRouteFilter(tenant.PayloadProcessingExternalModelEnvoyFilterName("tenant"), "foreign-gateway-system", "other-controller")
	r := controllerTestClient(t, model, ait, mtc, overlay, filter, legacy, neighborLegacy, foreignLegacy)
	r.Namespace, r.GatewayNamespace = "tenant-a", "gateway-system"
	if _, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(model)}); err != nil {
		t.Fatal(err)
	}
	var gotFilter unstructured.Unstructured
	gotFilter.SetGroupVersionKind(schema.GroupVersionKind{Group: "networking.istio.io", Version: "v1alpha3", Kind: "EnvoyFilter"})
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(filter), &gotFilter); !apierrors.IsNotFound(err) {
		t.Fatalf("route filter cleanup error = %v, want NotFound", err)
	}
	for _, preserved := range []*unstructured.Unstructured{neighborLegacy, foreignLegacy} {
		got := &unstructured.Unstructured{}
		got.SetGroupVersionKind(schema.GroupVersionKind{Group: "networking.istio.io", Version: "v1alpha3", Kind: "EnvoyFilter"})
		if err := r.Get(context.Background(), client.ObjectKeyFromObject(preserved), got); err != nil {
			t.Fatalf("legacy neighboring/foreign filter %s/%s was removed: %v", preserved.GetNamespace(), preserved.GetName(), err)
		}
	}
	gotFilter = unstructured.Unstructured{}
	gotFilter.SetGroupVersionKind(schema.GroupVersionKind{Group: "networking.istio.io", Version: "v1alpha3", Kind: "EnvoyFilter"})
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(legacy), &gotFilter); !apierrors.IsNotFound(err) {
		t.Fatalf("legacy route filter cleanup error = %v, want NotFound", err)
	}
	var gotOverlay corev1.ConfigMap
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(overlay), &gotOverlay); !apierrors.IsNotFound(err) {
		t.Fatalf("overlay cleanup error = %v, want NotFound", err)
	}
	var gotModel v1alpha1.ExternalModel
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(model), &gotModel); err != nil {
		t.Fatal(err)
	}
	if controllerutil.ContainsFinalizer(&gotModel, externalModelFinalizer) {
		t.Fatal("explicit ipp cleanup retained the ExternalModel finalizer")
	}
}

func TestExplicitIPPSwitchCleansOnlyTenantDestinationRulesAcrossGatewayMove(t *testing.T) {
	model := &v1alpha1.ExternalModel{ObjectMeta: metav1.ObjectMeta{Name: "model", Namespace: "tenant-a"}}
	controllerutil.AddFinalizer(model, externalModelFinalizer)
	ait := controllerAITenant("tenant", "models-as-a-service", "tenant-a", "new-gateway", "new-gateway-system")
	mtc := controllerMaasTenantConfig("tenant-a", "tenant", "models-as-a-service", "ipp")
	ownedOld := providerDestinationRuleForTenant(resolver.Route{Provider: "provider-a", Namespace: "tenant-a", Endpoint: "old.example.com"}, "old-gateway-system", "tenant")
	ownedCurrent := providerDestinationRuleForTenant(resolver.Route{Provider: "provider-b", Namespace: "tenant-a", Endpoint: "current.example.com"}, "new-gateway-system", "tenant")
	neighbor := providerDestinationRuleForTenant(resolver.Route{Provider: "provider-a", Namespace: "tenant-b", Endpoint: "neighbor.example.com"}, "new-gateway-system", "tenant-b")
	foreign := providerDestinationRule(resolver.Route{Provider: "provider-a", Endpoint: "foreign.example.com"}, "old-gateway-system")
	providerA := &v1alpha1.ExternalProvider{ObjectMeta: metav1.ObjectMeta{Name: "provider-a", Namespace: "tenant-a", Finalizers: []string{externalProviderFinalizer}}}
	providerB := &v1alpha1.ExternalProvider{ObjectMeta: metav1.ObjectMeta{Name: "provider-b", Namespace: "tenant-a", Finalizers: []string{externalProviderFinalizer}}}
	r := controllerTestClient(t, model, providerA, providerB, ait, mtc, &ownedOld, &ownedCurrent, &neighbor, &foreign)
	r.Namespace, r.GatewayNamespace = "tenant-a", "new-gateway-system"
	if _, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(model)}); err != nil {
		t.Fatal(err)
	}
	for _, provider := range []*v1alpha1.ExternalProvider{providerA, providerB} {
		if _, err := r.ReconcileProvider(context.Background(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(provider)}); err != nil {
			t.Fatalf("provider %s cleanup after IPP switch: %v", provider.Name, err)
		}
	}
	for _, owned := range []*unstructured.Unstructured{&ownedOld, &ownedCurrent} {
		got := &unstructured.Unstructured{}
		got.SetGroupVersionKind(schema.GroupVersionKind{Group: "networking.istio.io", Version: "v1", Kind: "DestinationRule"})
		if err := r.Get(context.Background(), client.ObjectKeyFromObject(owned), got); !apierrors.IsNotFound(err) {
			t.Fatalf("owned DestinationRule %s/%s cleanup error = %v, want NotFound", owned.GetNamespace(), owned.GetName(), err)
		}
	}
	for _, preserved := range []*unstructured.Unstructured{&neighbor, &foreign} {
		got := &unstructured.Unstructured{}
		got.SetGroupVersionKind(schema.GroupVersionKind{Group: "networking.istio.io", Version: "v1", Kind: "DestinationRule"})
		if err := r.Get(context.Background(), client.ObjectKeyFromObject(preserved), got); err != nil {
			t.Fatalf("foreign DestinationRule %s/%s was removed: %v", preserved.GetNamespace(), preserved.GetName(), err)
		}
	}
}

func TestProviderCleanupPreservesNeighborDefaultTenantDestinationRule(t *testing.T) {
	defaultRule := providerDestinationRuleForTenant(resolver.Route{Provider: "provider-a", Namespace: "models-as-a-service", Endpoint: "default.example.com"}, "gateway-system", "")
	neighbor := providerDestinationRuleForTenant(resolver.Route{Provider: "provider-a", Namespace: "tenant-b", Endpoint: "neighbor.example.com"}, "gateway-system", "tenant-b")
	if got := defaultRule.GetLabels()[externalTenantLabel]; got != "models-as-a-service" {
		t.Fatalf("default DestinationRule tenant label = %q, want models-as-a-service", got)
	}
	r := controllerTestClient(t, &defaultRule, &neighbor)
	if err := r.cleanupProviderTransport(context.Background(), "models-as-a-service", "provider-a"); err != nil {
		t.Fatal(err)
	}
	got := &unstructured.Unstructured{}
	got.SetGroupVersionKind(schema.GroupVersionKind{Group: "networking.istio.io", Version: "v1", Kind: "DestinationRule"})
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(&defaultRule), got); !apierrors.IsNotFound(err) {
		t.Fatalf("default DestinationRule cleanup error = %v, want NotFound", err)
	}
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(&neighbor), got); err != nil {
		t.Fatalf("neighbor DestinationRule was removed: %v", err)
	}
}

func TestResolveTenantRejectsWrongOwnerAndStatusNamespace(t *testing.T) {
	ait := controllerAITenant("tenant", "models-as-a-service", "other-tenant", "gateway", "gateway-system")
	mtc := controllerMaasTenantConfig("tenant-a", "tenant", "models-as-a-service", "praxis")
	r := controllerTestClient(t, ait, mtc)
	if _, err := r.resolveTenantForNamespace(context.Background(), "tenant-a"); err == nil || !strings.Contains(err.Error(), "does not own") {
		t.Fatalf("wrong status namespace resolution error = %v, want ownership error", err)
	}
	mtc.SetAnnotations(map[string]string{tenant.AnnotationPayloadProcessingType: "praxis", tenant.AnnotationAITenantName: "other", tenant.AnnotationAITenantNamespace: "models-as-a-service"})
	if err := r.Update(context.Background(), mtc); err != nil {
		t.Fatal(err)
	}
	resolution, err := r.resolveTenantForNamespace(context.Background(), "tenant-a")
	if err != nil || resolution.aitenant != nil || !resolution.selected {
		t.Fatalf("missing owner resolution = %#v, %v, want selected with no owner", resolution, err)
	}
}

func TestReconcileStandaloneProviderThenModelRoutes(t *testing.T) {
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
			Ref: v1alpha1.ExternalProviderReference{Name: provider.Name}, TargetModel: "gpt", APIFormat: "openai-chat", Path: "/v1/chat/completions",
		}}},
	}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "credentials", Namespace: "tenant-a"}, Data: map[string][]byte{"api-key": []byte("must-not-be-published")}}
	ait := controllerAITenant("tenant", "models-as-a-service", "tenant-a", "gateway", "gateway-system")
	mtc := controllerMaasTenantConfig("tenant-a", "tenant", "models-as-a-service", "praxis")
	r := controllerTestClient(t, provider, secret, ait, mtc)
	r.Namespace, r.GatewayName, r.GatewayNamespace, r.Network = "tenant-a", "gateway", "gateway-system", "external-model"
	r.KnownClusters = []string{"provider-provider"}
	providerReq := reconcile.Request{NamespacedName: client.ObjectKeyFromObject(provider)}
	if _, err := r.ReconcileProvider(context.Background(), providerReq); err != nil {
		t.Fatalf("reconcile standalone provider: %v", err)
	}
	var gotProvider v1alpha1.ExternalProvider
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(provider), &gotProvider); err != nil {
		t.Fatal(err)
	}
	if gotProvider.Status.Phase != resolver.PhaseReady || gotProvider.Status.ObservedGeneration != provider.Generation {
		t.Fatalf("standalone provider status = %#v", gotProvider.Status)
	}
	if err := r.Create(context.Background(), model); err != nil {
		t.Fatalf("create model after standalone provider reconcile: %v", err)
	}
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
			if got.GetLabels()[externalTenantLabel] != "tenant-a" {
				t.Fatalf("DestinationRule tenant label = %q, want tenant-a", got.GetLabels()[externalTenantLabel])
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
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(provider), &gotProvider); err != nil {
		t.Fatal(err)
	}
	if gotProvider.Status.Phase != resolver.PhaseReady || gotProvider.Status.ObservedGeneration != provider.Generation {
		t.Fatalf("provider status = %#v", gotProvider.Status)
	}
	if len(gotModel.Status.Conditions) < 2 {
		t.Fatalf("expected Ready and OverlayDistributed conditions: %#v", gotModel.Status.Conditions)
	}
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(mtc), mtc); err != nil {
		t.Fatal(err)
	}
	mtc.SetAnnotations(map[string]string{tenant.AnnotationPayloadProcessingType: tenant.PayloadProcessingBackendIPP, tenant.AnnotationAITenantName: "tenant", tenant.AnnotationAITenantNamespace: "models-as-a-service"})
	if err := r.Update(context.Background(), mtc); err != nil {
		t.Fatal(err)
	}
	if _, err := r.ReconcileProvider(context.Background(), providerReq); err != nil {
		t.Fatalf("provider cleanup after IPP handoff: %v", err)
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
	routeFilter := &unstructured.Unstructured{}
	routeFilter.SetGroupVersionKind(schema.GroupVersionKind{Group: "networking.istio.io", Version: "v1alpha3", Kind: "EnvoyFilter"})
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: "gateway-system", Name: tenant.PayloadProcessingExternalModelEnvoyFilterName("")}, routeFilter); !apierrors.IsNotFound(err) {
		t.Fatalf("switch-away route EnvoyFilter error = %v, want NotFound", err)
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
			Ref: v1alpha1.ExternalProviderReference{Name: provider.Name}, TargetModel: "gpt", APIFormat: "openai-chat", Path: "/v1/chat/completions",
		}}},
	}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "credentials", Namespace: "tenant-a"}, Data: map[string][]byte{"api-key": []byte("secret")}}
	ait := controllerAITenant("tenant", "models-as-a-service", "tenant-a", "gateway", "tenant-a")
	mtc := controllerMaasTenantConfig("tenant-a", "tenant", "models-as-a-service", "praxis")
	r := controllerTestClient(t, provider, model, secret, ait, mtc)
	r.Namespace, r.GatewayName, r.GatewayNamespace, r.Network = "tenant-a", "gateway", "tenant-a", "external-model"
	r.KnownClusters = []string{"provider-provider"}
	req := reconcile.Request{NamespacedName: client.ObjectKeyFromObject(model)}
	providerReq := reconcile.Request{NamespacedName: client.ObjectKeyFromObject(provider)}
	if _, err := r.ReconcileProvider(context.Background(), providerReq); err != nil {
		t.Fatalf("initial provider reconcile = %v", err)
	}
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
	if _, err := r.ReconcileProvider(context.Background(), providerReq); err != nil {
		t.Fatalf("provider reconcile after Secret deletion = %v", err)
	}
	if _, err := r.Reconcile(context.Background(), req); !errors.Is(err, resolver.ErrNoRoutes) {
		t.Fatalf("missing-Secret model reconcile error = %v, want no routes while Provider is failed", err)
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
	var failedProvider v1alpha1.ExternalProvider
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(provider), &failedProvider); err != nil {
		t.Fatal(err)
	}
	if failedProvider.Status.Phase != "Failed" {
		t.Fatalf("provider status after Secret deletion = %#v", failedProvider.Status)
	}

	secret.ResourceVersion = ""
	if err := r.Create(context.Background(), secret); err != nil {
		t.Fatal(err)
	}
	if _, err := r.ReconcileProvider(context.Background(), providerReq); err != nil {
		t.Fatalf("provider reconcile after Secret restoration = %v", err)
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

// testCACertificatePEM returns a self-signed CA certificate in PEM form.
func testCACertificatePEM(t *testing.T) []byte {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "provider-ca"},
		NotBefore: time.Now(), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

// providerCASecret returns a provider-ca Secret holding a CA in namespace.
func providerCASecret(t *testing.T, namespace string) *corev1.Secret {
	t.Helper()
	return &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "provider-ca", Namespace: namespace}, Data: map[string][]byte{"cacert": testCACertificatePEM(t)}}
}

func TestReconcileProviderReportsTLSCredentialSecretInGatewayNamespace(t *testing.T) {
	provider := &v1alpha1.ExternalProvider{
		ObjectMeta: metav1.ObjectMeta{Name: "provider", Namespace: "tenant-a", UID: "provider-uid"},
		Spec: v1alpha1.ExternalProviderSpec{
			Provider: "openai", Endpoint: "api.example.com",
			Auth:   v1alpha1.AuthConfig{Type: "apikey", SecretRef: v1alpha1.NameReference{Name: "credentials"}},
			Config: map[string]string{"tls.credentialName": "provider-ca"},
		},
	}
	credentials := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "credentials", Namespace: "tenant-a"}, Data: map[string][]byte{"api-key": []byte("secret")}}
	// A same-named Secret in the provider namespace is not the one Istio reads.
	decoy := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "provider-ca", Namespace: "tenant-a"}, Data: map[string][]byte{"cacert": testCACertificatePEM(t)}}
	ait := controllerAITenant("tenant", "models-as-a-service", "tenant-a", "gateway", "gateway-system")
	mtc := controllerMaasTenantConfig("tenant-a", "tenant", "models-as-a-service", "praxis")
	r := controllerTestClient(t, provider, credentials, decoy, ait, mtc)
	ctx := context.Background()
	req := reconcile.Request{NamespacedName: client.ObjectKeyFromObject(provider)}
	getRule := func() (*unstructured.Unstructured, error) {
		rule := &unstructured.Unstructured{}
		rule.SetGroupVersionKind(destinationRuleGVK)
		return rule, r.Get(ctx, client.ObjectKey{Namespace: "gateway-system", Name: "provider-provider-tenant"}, rule)
	}

	reconcileStatus := func() v1alpha1.ExternalProviderStatus {
		t.Helper()
		if _, err := r.ReconcileProvider(ctx, req); err != nil {
			t.Fatalf("ReconcileProvider() = %v", err)
		}
		var got v1alpha1.ExternalProvider
		if err := r.Get(ctx, req.NamespacedName, &got); err != nil {
			t.Fatal(err)
		}
		return got.Status
	}
	// A missing Secret and one without a CA read the same, so the tenant's
	// status does not disclose what the Gateway namespace holds.
	assertFailed := func() {
		t.Helper()
		status := reconcileStatus()
		ready := apiMeta.FindStatusCondition(status.Conditions, conditionReady)
		if status.Phase != providerPhaseFailed || ready == nil || ready.Status != metav1.ConditionFalse || ready.Message != errProviderCANotReady.Error() {
			t.Fatalf("provider status = %#v, want Failed with %q", status, errProviderCANotReady)
		}
		if _, err := getRule(); !apierrors.IsNotFound(err) {
			t.Fatalf("DestinationRule without its CA Secret: err = %v, want NotFound", err)
		}
	}

	assertFailed()

	caSecret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "provider-ca", Namespace: "gateway-system"}, Data: map[string][]byte{"tls.crt": testCACertificatePEM(t)}}
	if err := r.Create(ctx, caSecret); err != nil {
		t.Fatal(err)
	}
	assertFailed()

	caSecret.Data = map[string][]byte{"cacert": testCACertificatePEM(t)}
	if err := r.Update(ctx, caSecret); err != nil {
		t.Fatal(err)
	}
	if status := reconcileStatus(); status.Phase != resolver.PhaseReady {
		t.Fatalf("provider status with CA Secret = %#v, want Ready", status)
	}
	destination, err := getRule()
	if err != nil {
		t.Fatal(err)
	}
	if got := nestedString(t, destination.Object, "spec", "trafficPolicy", "tls", "credentialName"); got != "provider-ca" {
		t.Fatalf("DestinationRule credentialName = %q, want provider-ca", got)
	}
}

func TestSharedProviderReportsTLSCredentialSecretInGatewayNamespace(t *testing.T) {
	provider, secret := attachmentProvider("team")
	provider.Spec.Config = map[string]string{"tls.credentialName": "provider-ca"}
	r := controllerTestClient(t, provider, secret)
	r.GatewayNamespace = "openshift-ingress"
	ctx := context.Background()
	reconcileStatus := func() (string, *metav1.Condition) {
		t.Helper()
		if _, err := r.ReconcileProvider(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(provider)}); err != nil {
			t.Fatal(err)
		}
		var got v1alpha1.ExternalProvider
		if err := r.Get(ctx, client.ObjectKeyFromObject(provider), &got); err != nil {
			t.Fatal(err)
		}
		return got.Status.Phase, apiMeta.FindStatusCondition(got.Status.Conditions, conditionReady)
	}

	// No MaasTenantConfig serves this namespace; attachments still render the
	// credential, so its Secret is checked in the Gateway namespace.
	if phase, ready := reconcileStatus(); phase != providerPhaseFailed || ready == nil || ready.Message != errProviderCANotReady.Error() {
		t.Fatalf("provider = %s/%#v, want Failed with %q", phase, ready, errProviderCANotReady)
	}
	caSecret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "provider-ca", Namespace: "openshift-ingress"}, Data: map[string][]byte{"cacert": testCACertificatePEM(t)}}
	if err := r.Create(ctx, caSecret); err != nil {
		t.Fatal(err)
	}
	if phase, ready := reconcileStatus(); phase != resolver.PhaseReady || ready == nil || ready.Reason != reasonValidated {
		t.Fatalf("provider with its CA Secret = %s/%#v, want Ready/%s", phase, ready, reasonValidated)
	}
}

// failingCASecretReader fails reads of the provider-ca Secrets in namespace.
type failingCASecretReader struct {
	client.Reader
	namespace string
}

func (r failingCASecretReader) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if _, ok := obj.(*corev1.Secret); ok && key.Namespace == r.namespace && strings.HasPrefix(key.Name, "provider-ca") {
		return apierrors.NewServiceUnavailable("etcd is unavailable")
	}
	return r.Reader.Get(ctx, key, obj, opts...)
}

func TestReconcileProviderRetriesTransientCASecretRead(t *testing.T) {
	provider := &v1alpha1.ExternalProvider{
		ObjectMeta: metav1.ObjectMeta{Name: "provider", Namespace: "tenant-a", UID: "provider-uid"},
		Spec: v1alpha1.ExternalProviderSpec{
			Provider: "openai", Endpoint: "api.example.com",
			Auth:   v1alpha1.AuthConfig{Type: "apikey", SecretRef: v1alpha1.NameReference{Name: "credentials"}},
			Config: map[string]string{"tls.credentialName": "provider-ca"},
		},
	}
	credentials := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "credentials", Namespace: "tenant-a"}, Data: map[string][]byte{"api-key": []byte("secret")}}
	caSecret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "provider-ca", Namespace: "gateway-system"}, Data: map[string][]byte{"cacert": testCACertificatePEM(t)}}
	ait := controllerAITenant("tenant", "models-as-a-service", "tenant-a", "gateway", "gateway-system")
	mtc := controllerMaasTenantConfig("tenant-a", "tenant", "models-as-a-service", "praxis")
	r := controllerTestClient(t, provider, credentials, caSecret, ait, mtc)
	ctx := context.Background()
	req := reconcile.Request{NamespacedName: client.ObjectKeyFromObject(provider)}
	if _, err := r.ReconcileProvider(ctx, req); err != nil {
		t.Fatal(err)
	}

	reader := r.APIReader
	r.APIReader = failingCASecretReader{Reader: reader, namespace: "gateway-system"}
	if _, err := r.ReconcileProvider(ctx, req); !apierrors.IsServiceUnavailable(err) {
		t.Fatalf("ReconcileProvider() on a failed CA Secret read = %v, want ServiceUnavailable to retry", err)
	}
	var got v1alpha1.ExternalProvider
	if err := r.Get(ctx, req.NamespacedName, &got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != resolver.PhaseReady {
		t.Fatalf("provider phase after a failed CA Secret read = %q, want Ready", got.Status.Phase)
	}
	rule := &unstructured.Unstructured{}
	rule.SetGroupVersionKind(destinationRuleGVK)
	if err := r.Get(ctx, client.ObjectKey{Namespace: "gateway-system", Name: "provider-provider-tenant"}, rule); err != nil {
		t.Fatalf("DestinationRule after a failed CA Secret read: %v", err)
	}

	r.APIReader = reader
	if _, err := r.ReconcileProvider(ctx, req); err != nil {
		t.Fatalf("recovery reconcile = %v", err)
	}
}

func TestSharedProviderRetriesTransientCASecretRead(t *testing.T) {
	provider, secret := attachmentProvider("team")
	provider.Spec.Config = map[string]string{"tls.credentialName": "provider-ca"}
	caSecret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "provider-ca", Namespace: "openshift-ingress"}, Data: map[string][]byte{"cacert": testCACertificatePEM(t)}}
	r := controllerTestClient(t, provider, secret, caSecret)
	r.GatewayNamespace = "openshift-ingress"
	ctx := context.Background()
	req := reconcile.Request{NamespacedName: client.ObjectKeyFromObject(provider)}
	if _, err := r.ReconcileProvider(ctx, req); err != nil {
		t.Fatal(err)
	}

	r.APIReader = failingCASecretReader{Reader: r.APIReader, namespace: "openshift-ingress"}
	if _, err := r.ReconcileProvider(ctx, req); !apierrors.IsServiceUnavailable(err) {
		t.Fatalf("ReconcileProvider() on a failed CA Secret read = %v, want ServiceUnavailable to retry", err)
	}
	var got v1alpha1.ExternalProvider
	if err := r.Get(ctx, req.NamespacedName, &got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != resolver.PhaseReady {
		t.Fatalf("provider phase after a failed CA Secret read = %q, want Ready", got.Status.Phase)
	}
}

func TestValidateProviderCACredentialFollowsIstioLookup(t *testing.T) {
	provider := &v1alpha1.ExternalProvider{Spec: v1alpha1.ExternalProviderSpec{Config: map[string]string{"tls.credentialName": "provider-ca"}}}
	ca := map[string][]byte{"cacert": testCACertificatePEM(t)}
	noCA := map[string][]byte{"tls.crt": testCACertificatePEM(t)}
	secret := func(name string, data map[string][]byte) client.Object {
		return &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "gateway-system"}, Data: data}
	}
	elsewhere := secret("provider-ca", ca)
	elsewhere.SetNamespace("tenant-a")
	for name, tc := range map[string]struct {
		secrets []client.Object
		wantErr error
	}{
		"single Secret":             {secrets: []client.Object{secret("provider-ca", ca)}},
		"ca.crt key":                {secrets: []client.Object{secret("provider-ca", map[string][]byte{"ca.crt": ca["cacert"]})}},
		"separate CA Secret":        {secrets: []client.Object{secret("provider-ca-cacert", ca)}},
		"separate CA Secret first":  {secrets: []client.Object{secret("provider-ca", ca), secret("provider-ca-cacert", noCA)}, wantErr: errProviderCANotReady},
		"neither Secret":            {wantErr: errProviderCANotReady},
		"only in another namespace": {secrets: []client.Object{elsewhere}, wantErr: errProviderCANotReady},
	} {
		t.Run(name, func(t *testing.T) {
			r := controllerTestClient(t, tc.secrets...)
			if err := r.validateProviderCACredential(context.Background(), provider, "gateway-system"); !errors.Is(err, tc.wantErr) {
				t.Fatalf("validateProviderCACredential() = %v, want %v", err, tc.wantErr)
			}
		})
	}
}

func TestCACredentialSecretEventsEnqueueTrustingProviders(t *testing.T) {
	trusting := &v1alpha1.ExternalProvider{ObjectMeta: metav1.ObjectMeta{Name: "trusting", Namespace: "tenant-a"}, Spec: v1alpha1.ExternalProviderSpec{
		Config: map[string]string{"tls.credentialName": "provider-ca"},
	}}
	other := &v1alpha1.ExternalProvider{ObjectMeta: metav1.ObjectMeta{Name: "other", Namespace: "tenant-b"}, Spec: v1alpha1.ExternalProviderSpec{
		Config: map[string]string{"tls.credentialName": "other-ca"},
	}}
	r := controllerTestClient(t, trusting, other)
	secret := &metav1.PartialObjectMetadata{ObjectMeta: metav1.ObjectMeta{Name: "provider-ca", Namespace: "gateway-system"}}
	want := []reconcile.Request{{NamespacedName: client.ObjectKeyFromObject(trusting)}}
	if got := r.secretProviders(context.Background(), secret); !reflect.DeepEqual(got, want) {
		t.Fatalf("secretProviders() = %v, want %v", got, want)
	}
	// Istio prefers a separate <name>-cacert Secret, so its events count too.
	separate := &metav1.PartialObjectMetadata{ObjectMeta: metav1.ObjectMeta{Name: "provider-ca-cacert", Namespace: "gateway-system"}}
	if got := r.secretProviders(context.Background(), separate); !reflect.DeepEqual(got, want) {
		t.Fatalf("secretProviders(%s) = %v, want %v", separate.Name, got, want)
	}
	r.Namespace = "tenant-b"
	if got := r.secretProviders(context.Background(), secret); len(got) != 0 {
		t.Fatalf("secretProviders() outside the namespace scope = %v, want none", got)
	}
}

func TestCACredentialSecretEventsEnqueueModelsOfTrustingProviders(t *testing.T) {
	provider, secret := attachmentProvider("team")
	provider.Spec.Config = map[string]string{"tls.credentialName": "provider-ca"}
	attached := attachmentModel("chat", "team", "chat-model", time.Unix(100, 0), gatewayB)
	// A same-named provider in another namespace does not trust the Secret.
	unrelated := attachmentModel("other", "elsewhere", "other-model", time.Unix(100, 0), gatewayB)
	r := controllerTestClient(t, provider, secret, attached, unrelated)
	want := []reconcile.Request{{NamespacedName: client.ObjectKeyFromObject(attached)}}
	for _, name := range []string{"provider-ca", "provider-ca-cacert"} {
		event := &metav1.PartialObjectMetadata{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: gatewayB.Namespace}}
		if got := r.secretModels(context.Background(), event); !reflect.DeepEqual(got, want) {
			t.Fatalf("secretModels(%s) = %v, want %v", name, got, want)
		}
	}
}

func TestModelTransportFailureDoesNotRewriteProviderReadiness(t *testing.T) {
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
	if failedProvider.Status.Phase != resolver.PhaseReady {
		t.Fatalf("provider phase after model transport failure = %q, want Ready", failedProvider.Status.Phase)
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
		t.Fatalf("provider phase after model transport recovery = %q, want Ready", provider.Status.Phase)
	}
}

func TestExternalModelDeletionRepublishesRemainingRoutes(t *testing.T) {
	r, model := reconcilerFixture(t)
	r.KnownClusters = append(r.KnownClusters, "provider-provider-b", "provider-provider-c")
	model.Spec.ModelName = "shared-model"
	if err := r.Update(context.Background(), model); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(model)}); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"provider-b", "provider-c"} {
		provider := &v1alpha1.ExternalProvider{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: model.Namespace, UID: types.UID(name + "-uid")},
			Spec: v1alpha1.ExternalProviderSpec{
				Provider: "openai", Endpoint: name + ".example.com",
				Auth: v1alpha1.AuthConfig{Type: "apikey", SecretRef: v1alpha1.NameReference{Name: name + "-credentials"}},
			},
			Status: v1alpha1.ExternalProviderStatus{Phase: resolver.PhaseReady},
		}
		if err := r.Create(context.Background(), provider); err != nil {
			t.Fatal(err)
		}
		secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: name + "-credentials", Namespace: model.Namespace}, Data: map[string][]byte{"api-key": []byte("fixture-only-secret")}}
		if err := r.Create(context.Background(), secret); err != nil {
			t.Fatal(err)
		}
		if _, err := r.ReconcileProvider(context.Background(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(provider)}); err != nil {
			t.Fatalf("ReconcileProvider %s: %v", name, err)
		}
	}
	sibling := &v1alpha1.ExternalModel{
		ObjectMeta: metav1.ObjectMeta{Name: "sibling", Namespace: model.Namespace, UID: "sibling-uid", Finalizers: []string{externalModelFinalizer}},
		Spec: v1alpha1.ExternalModelSpec{ModelName: "shared-model", ExternalProviderRefs: []v1alpha1.ExternalProviderRef{
			{Ref: v1alpha1.ExternalProviderReference{Name: "provider-b"}, TargetModel: "gpt-b", APIFormat: "openai-chat", Path: "/v1/chat/completions"},
			{Ref: v1alpha1.ExternalProviderReference{Name: "provider-c"}, TargetModel: "gpt-c", APIFormat: "openai-chat", Path: "/v1/chat/completions"},
		}},
	}
	if err := r.Create(context.Background(), sibling); err != nil {
		t.Fatal(err)
	}
	if err := r.Delete(context.Background(), model); err != nil {
		t.Fatal(err)
	}
	result, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(model)})
	if err != nil {
		t.Fatal(err)
	}
	if result.RequeueAfter <= 0 {
		t.Fatalf("deleted model withdrawal requeue = %s, want old route drain", result.RequeueAfter)
	}
	deletedRoute := &unstructured.Unstructured{}
	deletedRoute.SetGroupVersionKind(schema.GroupVersionKind{Group: "gateway.networking.k8s.io", Version: "v1", Kind: "HTTPRoute"})
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: "tenant-a", Name: modelRouteName(model.Name)}, deletedRoute); err != nil {
		t.Fatalf("deleted model route should remain until the old ExtProc drains: %v", err)
	}
	seedSafeExternalModelRuntime(t, r, "tenant", model.Namespace, []*v1alpha1.ExternalModel{sibling})
	result, err = r.Reconcile(context.Background(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(model)})
	if err != nil {
		t.Fatal(err)
	}
	if result.RequeueAfter != withdrawalDrainDuration {
		t.Fatalf("deleted model drain requeue = %s, want %s", result.RequeueAfter, withdrawalDrainDuration)
	}
	var drainOverlay corev1.ConfigMap
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: model.Namespace, Name: "routing-overlay"}, &drainOverlay); err != nil {
		t.Fatal(err)
	}
	var drainState withdrawalDrainState
	if err := json.Unmarshal([]byte(drainOverlay.Annotations[withdrawalDrainAnnotation]), &drainState); err != nil {
		t.Fatalf("decode drain state: %v", err)
	}
	drainState.StartedAt = time.Now().Add(-withdrawalDrainDuration - time.Second)
	drainStateRaw, err := json.Marshal(drainState)
	if err != nil {
		t.Fatal(err)
	}
	drainOverlay.Annotations[withdrawalDrainAnnotation] = string(drainStateRaw)
	if err := r.Update(context.Background(), &drainOverlay); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(model)}); err != nil {
		t.Fatal(err)
	}
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
	var published envelope.Envelope
	if err := json.Unmarshal([]byte(overlay.Data["routing-overlay.json"]), &published); err != nil {
		t.Fatalf("decode republished overlay: %v", err)
	}
	var policy struct {
		Mode string `json:"mode"`
	}
	if err := json.Unmarshal(published.Overlay.SelectionPolicy, &policy); err != nil || policy.Mode != "random" {
		t.Fatalf("deletion-time selection policy = %s, want random (err=%v)", published.Overlay.SelectionPolicy, err)
	}
	if len(published.Overlay.Candidates) != 2 {
		t.Fatalf("deletion-time candidates = %d, want both remaining providers", len(published.Overlay.Candidates))
	}
	for _, candidate := range published.Overlay.Candidates {
		if candidate.Name != "shared-model" || candidate.SelectionGroup == nil || *candidate.SelectionGroup != 0 {
			t.Fatalf("deletion-time candidate did not retain shared random group: %+v", candidate)
		}
	}
	deletionDigest := published.Revision.Value
	deletionGeneration := published.Provenance.SourceGeneration
	remainingProviders := make([]*v1alpha1.ExternalProvider, 0, 2)
	for _, name := range []string{"provider-b", "provider-c"} {
		provider := &v1alpha1.ExternalProvider{}
		if err := r.Get(context.Background(), client.ObjectKey{Namespace: model.Namespace, Name: name}, provider); err != nil {
			t.Fatal(err)
		}
		provider.Status.Phase = resolver.PhaseReady
		remainingProviders = append(remainingProviders, provider)
	}
	remainingSet, err := resolver.Resolve([]*v1alpha1.ExternalModel{sibling}, remainingProviders)
	if err != nil {
		t.Fatalf("resolve normal remaining routes: %v", err)
	}
	normal, err := envelope.Render(remainingSet, envelope.Scope{
		Network: r.Network, Gateway: r.GatewayName, Namespace: sibling.Namespace, LocalSite: r.localSite(),
	}, envelope.Revision{Generation: deletionGeneration, Digest: deletionDigest}, envelope.Options{
		KnownClusters: r.KnownClusters, SourceUID: string(sibling.UID), ProducerVersion: r.ProducerVersion,
	})
	if err != nil {
		t.Fatalf("render normal remaining routes: %v", err)
	}
	if normal.Revision.Value != deletionDigest || normal.Provenance.SourceGeneration != deletionGeneration {
		t.Fatalf("normal publication revision %s/%d differs from deletion-time %s/%d",
			normal.Revision.Value, normal.Provenance.SourceGeneration, deletionDigest, deletionGeneration)
	}
	var normalPolicy struct {
		Mode string `json:"mode"`
	}
	if err := json.Unmarshal(normal.Overlay.SelectionPolicy, &normalPolicy); err != nil {
		t.Fatalf("decode normal selection policy: %v", err)
	}
	if normalPolicy.Mode != policy.Mode {
		t.Fatalf("normal selection policy %s differs from deletion-time %s",
			normalPolicy.Mode, policy.Mode)
	}
}

func TestReconcileAppliesCandidateTransportBeforePublishingRandomSelection(t *testing.T) {
	r, model := reconcilerFixture(t)
	var providerA v1alpha1.ExternalProvider
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: model.Namespace, Name: "provider"}, &providerA); err != nil {
		t.Fatal(err)
	}
	providerA.Status.Phase = resolver.PhaseReady
	if err := r.Status().Update(context.Background(), &providerA); err != nil {
		t.Fatal(err)
	}
	providerB := &v1alpha1.ExternalProvider{
		ObjectMeta: metav1.ObjectMeta{Name: "provider-b", Namespace: model.Namespace, UID: "provider-b-uid"},
		Spec: v1alpha1.ExternalProviderSpec{Provider: "openai", Endpoint: "b.example.com",
			Auth: v1alpha1.AuthConfig{Type: "apikey", SecretRef: v1alpha1.NameReference{Name: "provider-b-credentials"}}},
		Status: v1alpha1.ExternalProviderStatus{Phase: resolver.PhaseReady},
	}
	if err := r.Create(context.Background(), providerB); err != nil {
		t.Fatal(err)
	}
	if err := r.Create(context.Background(), &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "provider-b-credentials", Namespace: model.Namespace},
		Data:       map[string][]byte{"api-key": []byte("fixture-only-secret")},
	}); err != nil {
		t.Fatal(err)
	}
	model.Spec.ExternalProviderRefs = append(model.Spec.ExternalProviderRefs, v1alpha1.ExternalProviderRef{
		Ref: v1alpha1.ExternalProviderReference{Name: "provider-b"}, TargetModel: "gpt-b", APIFormat: "openai-chat", Path: "/v1/chat/completions",
	})
	if err := r.Update(context.Background(), model); err != nil {
		t.Fatal(err)
	}
	r.KnownClusters = append(r.KnownClusters, "provider-provider-b")

	providerTransport := map[string]map[string]bool{}
	routedProviders := map[string]bool{}
	routeApplied, filterApplied, publishCalled := false, false, false
	r.ApplyResource = func(ctx context.Context, _ client.Client, object unstructured.Unstructured) error {
		labels := object.GetLabels()
		providerName := labels["inference.opendatahub.io/external-provider"]
		if providerName != "" {
			if providerTransport[providerName] == nil {
				providerTransport[providerName] = map[string]bool{}
			}
			providerTransport[providerName][object.GetKind()] = true
		}
		if object.GetKind() == "HTTPRoute" && labels["inference.opendatahub.io/external-model"] == model.Name {
			routeApplied = true
			for _, rawRule := range nestedSlice(t, object.Object, "spec", "rules") {
				rule, ok := rawRule.(map[string]any)
				if !ok {
					continue
				}
				for _, rawBackend := range nestedSlice(t, rule, "backendRefs") {
					backend, ok := rawBackend.(map[string]any)
					if !ok {
						continue
					}
					if name, ok := backend["name"].(string); ok {
						routedProviders[name] = true
					}
				}
			}
		}
		if object.GetKind() == "EnvoyFilter" {
			filterApplied = true
		}
		return render.Apply(ctx, r.Client, []unstructured.Unstructured{object})
	}
	// Provider transport is owned by ReconcileProvider (#128); model reconcile
	// only publishes once those Services/ServiceEntries/DestinationRules exist.
	for _, name := range []string{"provider", "provider-b"} {
		if _, err := r.ReconcileProvider(context.Background(), reconcile.Request{
			NamespacedName: client.ObjectKey{Namespace: model.Namespace, Name: name},
		}); err != nil {
			t.Fatalf("ReconcileProvider %s: %v", name, err)
		}
	}
	pub, err := publisher.New(r.Client, publisher.Config{Namespace: model.Namespace, Name: "routing-overlay"})
	if err != nil {
		t.Fatal(err)
	}
	r.PublishOverlay = func(ctx context.Context, set *resolver.ResolvedRouteSet, scope envelope.Scope,
		options envelope.Options) (publisher.Result, error) {
		for _, provider := range []string{"provider", "provider-b"} {
			for _, kind := range []string{"Service", "ServiceEntry", "DestinationRule"} {
				if !providerTransport[provider][kind] {
					return publisher.Result{}, fmt.Errorf("publish called before %s transport for %s was applied", kind, provider)
				}
			}
		}
		if !routeApplied || !filterApplied {
			return publisher.Result{}, errors.New("publish called before candidate HTTPRoute and EnvoyFilter were applied")
		}
		for _, provider := range []string{"provider-provider", "provider-provider-b"} {
			if !routedProviders[provider] {
				return publisher.Result{}, fmt.Errorf("candidate HTTPRoute lacks backend %s", provider)
			}
		}
		preview, err := envelope.Render(set, scope, envelope.Revision{}, options)
		if err != nil {
			return publisher.Result{}, err
		}
		if string(preview.Overlay.SelectionPolicy) != `{"mode":"random"}` || len(preview.Overlay.Candidates) != 2 {
			return publisher.Result{}, fmt.Errorf("published candidates/policy = %d/%s, want two grouped random candidates",
				len(preview.Overlay.Candidates), preview.Overlay.SelectionPolicy)
		}
		for _, candidate := range preview.Overlay.Candidates {
			if candidate.SelectionGroup == nil || *candidate.SelectionGroup != 0 {
				return publisher.Result{}, fmt.Errorf("candidate %s lacks numeric group zero", candidate.Cluster)
			}
		}
		publishCalled = true
		return pub.Publish(ctx, set, scope, options)
	}
	if _, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(model)}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if !publishCalled {
		t.Fatal("overlay selection was not published")
	}
}

func TestReconcileAcceptsUnequalWeightsAndPublishesWeightedRandom(t *testing.T) {
	r, model := reconcilerFixture(t)
	providerB := &v1alpha1.ExternalProvider{
		ObjectMeta: metav1.ObjectMeta{Name: "provider-b", Namespace: model.Namespace, UID: "provider-b-uid"},
		Spec: v1alpha1.ExternalProviderSpec{Provider: "openai", Endpoint: "b.example.com",
			Auth: v1alpha1.AuthConfig{Type: "apikey", SecretRef: v1alpha1.NameReference{Name: "provider-b-credentials"}}},
		Status: v1alpha1.ExternalProviderStatus{Phase: resolver.PhaseReady},
	}
	if err := r.Create(context.Background(), providerB); err != nil {
		t.Fatal(err)
	}
	if err := r.Create(context.Background(), &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "provider-b-credentials", Namespace: model.Namespace},
		Data:       map[string][]byte{"api-key": []byte("fixture-only-secret")},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := r.ReconcileProvider(context.Background(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(providerB)}); err != nil {
		t.Fatalf("ReconcileProvider provider-b: %v", err)
	}
	weightEighty, weightTwenty := 80, 20
	model.Spec.ExternalProviderRefs = []v1alpha1.ExternalProviderRef{
		{Ref: v1alpha1.ExternalProviderReference{Name: "provider"}, TargetModel: "gpt", APIFormat: "openai-chat",
			Path: "/v1/chat/completions", Weight: &weightEighty},
		{Ref: v1alpha1.ExternalProviderReference{Name: "provider-b"}, TargetModel: "gpt-b", APIFormat: "openai-chat",
			Path: "/v1/chat/completions", Weight: &weightTwenty},
	}
	if err := r.Update(context.Background(), model); err != nil {
		t.Fatal(err)
	}
	r.KnownClusters = append(r.KnownClusters, "provider-provider-b")
	if _, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(model)}); err != nil {
		t.Fatalf("Reconcile error = %v, want success for unequal weights", err)
	}
	var published corev1.ConfigMap
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: model.Namespace, Name: "routing-overlay"}, &published); err != nil {
		t.Fatal(err)
	}
	var env envelope.Envelope
	if err := json.Unmarshal([]byte(published.Data["routing-overlay.json"]), &env); err != nil {
		t.Fatal(err)
	}
	var policy struct {
		Mode string `json:"mode"`
	}
	if err := json.Unmarshal(env.Overlay.SelectionPolicy, &policy); err != nil || policy.Mode != "weightedRandom" || len(env.Overlay.Candidates) != 2 {
		t.Fatalf("overlay = policy %s candidates %#v, want weightedRandom with two candidates (err=%v)",
			env.Overlay.SelectionPolicy, env.Overlay.Candidates, err)
	}
	want := map[string]uint32{"provider-provider": 80, "provider-provider-b": 20}
	for _, candidate := range env.Overlay.Candidates {
		if candidate.SelectionGroup == nil || *candidate.SelectionGroup != 0 {
			t.Fatalf("candidate %s selection_group = %v, want 0", candidate.Cluster, candidate.SelectionGroup)
		}
		if candidate.TrafficWeight == nil || *candidate.TrafficWeight != want[candidate.Cluster] {
			t.Fatalf("candidate %s traffic_weight = %v, want %d", candidate.Cluster, candidate.TrafficWeight, want[candidate.Cluster])
		}
	}
	var got v1alpha1.ExternalModel
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(model), &got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != resolver.PhaseReady {
		t.Fatalf("model phase = %q, want Ready", got.Status.Phase)
	}
}

func TestReconcileAllZeroWeightsWithdrawsPreviouslyPublishedRoute(t *testing.T) {
	ctx := context.Background()
	r, model := reconcilerFixture(t)
	req := reconcile.Request{NamespacedName: client.ObjectKeyFromObject(model)}
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("initial serving reconcile: %v", err)
	}
	var overlay corev1.ConfigMap
	if err := r.Get(ctx, client.ObjectKey{Namespace: model.Namespace, Name: "routing-overlay"}, &overlay); err != nil {
		t.Fatalf("initial serving overlay: %v", err)
	}
	route := &unstructured.Unstructured{}
	route.SetGroupVersionKind(schema.GroupVersionKind{Group: "gateway.networking.k8s.io", Version: "v1", Kind: "HTTPRoute"})
	if err := r.Get(ctx, client.ObjectKey{Namespace: model.Namespace, Name: modelRouteName(model.Name)}, route); err != nil {
		t.Fatalf("initial serving HTTPRoute: %v", err)
	}
	var current v1alpha1.ExternalModel
	if err := r.Get(ctx, client.ObjectKeyFromObject(model), &current); err != nil {
		t.Fatal(err)
	}
	zero := 0
	current.Spec.ExternalProviderRefs[0].Weight = &zero
	if err := r.Update(ctx, &current); err != nil {
		t.Fatalf("set all provider weights to zero: %v", err)
	}
	if _, err := r.Reconcile(ctx, req); !errors.Is(err, resolver.ErrNoRoutes) {
		t.Fatalf("all-zero reconcile = %v, want ErrNoRoutes", err)
	}
	var residualOverlay corev1.ConfigMap
	if err := r.Get(ctx, client.ObjectKey{Namespace: model.Namespace, Name: "routing-overlay"}, &residualOverlay); !apierrors.IsNotFound(err) {
		t.Fatalf("overlay after all-zero update = err %v, labels %v, annotations %v, data %v; want NotFound",
			err, residualOverlay.Labels, residualOverlay.Annotations, residualOverlay.Data)
	}
	if err := r.Get(ctx, client.ObjectKey{Namespace: model.Namespace, Name: modelRouteName(model.Name)}, route); !apierrors.IsNotFound(err) {
		t.Fatalf("HTTPRoute after all-zero update = %v, want NotFound", err)
	}
	if err := r.Get(ctx, client.ObjectKeyFromObject(model), &current); err != nil {
		t.Fatal(err)
	}
	if current.Status.Phase != "Failed" || current.Status.ObservedGeneration != current.Generation ||
		!hasConditionReason(current.Status.Conditions, conditionReady, reasonNoRoutes) {
		t.Fatalf("all-zero ExternalModel status = %#v, want current generation Failed with Ready=False/NoRoutes", current.Status)
	}
}

func TestReconcileAllZeroWeightsWithdrawsDespiteProviderFailure(t *testing.T) {
	for _, tc := range []struct {
		name         string
		providerGone bool
	}{
		{name: "unready provider"},
		{name: "missing provider", providerGone: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			r, model := reconcilerFixture(t)
			req := reconcile.Request{NamespacedName: client.ObjectKeyFromObject(model)}
			if _, err := r.Reconcile(ctx, req); err != nil {
				t.Fatalf("initial serving reconcile: %v", err)
			}
			routeKey := client.ObjectKey{Namespace: model.Namespace, Name: modelRouteName(model.Name)}
			if _, err := getUnstructured(r, httpRouteGVK, routeKey.Namespace, routeKey.Name); err != nil {
				t.Fatalf("initial serving HTTPRoute: %v", err)
			}

			var provider v1alpha1.ExternalProvider
			providerKey := client.ObjectKey{Namespace: model.Namespace, Name: "provider"}
			if err := r.Get(ctx, providerKey, &provider); err != nil {
				t.Fatal(err)
			}
			if tc.providerGone {
				if err := r.Delete(ctx, &provider); err != nil {
					t.Fatalf("delete provider: %v", err)
				}
			} else {
				provider.Status.Phase = "Pending"
				provider.Status.Conditions = []metav1.Condition{{
					Type: conditionReady, Status: metav1.ConditionFalse, Reason: "Reconciling",
					Message: "test provider is unavailable", LastTransitionTime: metav1.Now(),
				}}
				if err := r.Status().Update(ctx, &provider); err != nil {
					t.Fatalf("mark provider unready: %v", err)
				}
			}
			var current v1alpha1.ExternalModel
			if err := r.Get(ctx, client.ObjectKeyFromObject(model), &current); err != nil {
				t.Fatal(err)
			}
			zero := 0
			current.Spec.ExternalProviderRefs[0].Weight = &zero
			if err := r.Update(ctx, &current); err != nil {
				t.Fatalf("set explicit zero weight: %v", err)
			}
			if _, err := r.Reconcile(ctx, req); !errors.Is(err, resolver.ErrNoRoutes) {
				t.Fatalf("all-zero reconcile = %v, want ErrNoRoutes", err)
			}
			if _, err := getUnstructured(r, httpRouteGVK, routeKey.Namespace, routeKey.Name); !apierrors.IsNotFound(err) {
				t.Fatalf("HTTPRoute after all-zero update = %v, want NotFound", err)
			}
			var residual corev1.ConfigMap
			if err := r.Get(ctx, client.ObjectKey{Namespace: model.Namespace, Name: "routing-overlay"}, &residual); !apierrors.IsNotFound(err) {
				t.Fatalf("routing overlay after all-zero update = %v, want NotFound", err)
			}
			if err := r.Get(ctx, client.ObjectKeyFromObject(model), &current); err != nil {
				t.Fatal(err)
			}
			if current.Status.ObservedGeneration != current.Generation || current.Status.Phase != "Failed" ||
				!hasConditionReason(current.Status.Conditions, conditionReady, reasonNoRoutes) {
				t.Fatalf("all-zero ExternalModel status = %#v, want current-generation Failed/NoRoutes", current.Status)
			}
		})
	}
}

func reconcilerWithSiblingProvider(t *testing.T) (*Reconciler, *v1alpha1.ExternalModel, *v1alpha1.ExternalModel, *v1alpha1.ExternalProvider) {
	t.Helper()
	r, model := reconcilerFixture(t)
	providerB := &v1alpha1.ExternalProvider{
		ObjectMeta: metav1.ObjectMeta{Name: "provider-b", Namespace: model.Namespace, UID: "provider-b-uid"},
		Spec: v1alpha1.ExternalProviderSpec{Provider: "openai", Endpoint: "b.example.com",
			Auth: v1alpha1.AuthConfig{Type: "apikey", SecretRef: v1alpha1.NameReference{Name: "provider-b-credentials"}}},
		Status: v1alpha1.ExternalProviderStatus{Phase: resolver.PhaseReady},
	}
	if err := r.Create(context.Background(), providerB); err != nil {
		t.Fatal(err)
	}
	if err := r.Create(context.Background(), &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "provider-b-credentials", Namespace: model.Namespace},
		Data:       map[string][]byte{"api-key": []byte("fixture-only-provider-b-secret")},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := r.ReconcileProvider(context.Background(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(providerB)}); err != nil {
		t.Fatalf("ReconcileProvider provider-b: %v", err)
	}
	sibling := model.DeepCopy()
	sibling.Name, sibling.UID, sibling.ResourceVersion = "sibling", "sibling-uid", ""
	sibling.Status = v1alpha1.ExternalModelStatus{}
	sibling.Spec.ExternalProviderRefs = []v1alpha1.ExternalProviderRef{{
		Ref: v1alpha1.ExternalProviderReference{Name: providerB.Name}, TargetModel: "gpt-sibling",
		APIFormat: "openai-chat", Path: "/v1/chat/completions",
	}}
	if err := r.Create(context.Background(), sibling); err != nil {
		t.Fatal(err)
	}
	r.KnownClusters = append(r.KnownClusters, "provider-provider-b")
	return r, model, sibling, providerB
}

func TestAllZeroModelWithdrawsWithoutAffectingHealthySibling(t *testing.T) {
	ctx := context.Background()
	r, model, sibling, _ := reconcilerWithSiblingProvider(t)
	for _, current := range []*v1alpha1.ExternalModel{model, sibling} {
		if _, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(current)}); err != nil {
			t.Fatalf("initial reconcile %s: %v", current.Name, err)
		}
	}
	zero := 0
	var current v1alpha1.ExternalModel
	if err := r.Get(ctx, client.ObjectKeyFromObject(model), &current); err != nil {
		t.Fatal(err)
	}
	current.Spec.ExternalProviderRefs[0].Weight = &zero
	if err := r.Update(ctx, &current); err != nil {
		t.Fatalf("set model weight to zero: %v", err)
	}
	result, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(model)})
	if err != nil || result.RequeueAfter <= 0 {
		t.Fatalf("reconcile all-zero model beside healthy sibling: %v", err)
	}
	if _, err := getUnstructured(r, httpRouteGVK, model.Namespace, modelRouteName(model.Name)); err != nil {
		t.Fatalf("disabled model HTTPRoute should remain during the serving-revision drain: %v", err)
	}
	if _, err := getUnstructured(r, httpRouteGVK, sibling.Namespace, modelRouteName(sibling.Name)); err != nil {
		t.Fatalf("healthy sibling HTTPRoute was removed: %v", err)
	}
	var overlay corev1.ConfigMap
	if err := r.Get(ctx, client.ObjectKey{Namespace: model.Namespace, Name: "routing-overlay"}, &overlay); err != nil {
		t.Fatalf("sibling routing overlay: %v", err)
	}
	var published envelope.Envelope
	if err := json.Unmarshal([]byte(overlay.Data["routing-overlay.json"]), &published); err != nil {
		t.Fatal(err)
	}
	if len(published.Overlay.Candidates) != 1 || published.Overlay.Candidates[0].StableID != "sibling/provider-provider-b" {
		t.Fatalf("overlay candidates after model withdrawal = %#v, want only sibling provider", published.Overlay.Candidates)
	}
	if _, err := getUnstructured(r, serviceGVK, model.Namespace, providerServicePrefix+"provider-b"); err != nil {
		t.Fatalf("healthy sibling provider Service was removed: %v", err)
	}
	if err := r.Get(ctx, client.ObjectKeyFromObject(model), &current); err != nil {
		t.Fatal(err)
	}
	if current.Status.Phase != "Failed" || current.Status.ObservedGeneration != current.Generation ||
		!hasConditionReason(current.Status.Conditions, conditionReady, reasonNoRoutes) {
		t.Fatalf("disabled model status = %#v, want current-generation Failed/NoRoutes", current.Status)
	}
	seedSafeExternalModelRuntime(t, r, "tenant", model.Namespace, []*v1alpha1.ExternalModel{&current, sibling})
	result, err = r.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(model)})
	if err != nil || result.RequeueAfter != withdrawalDrainDuration {
		t.Fatalf("start all-zero model drain: result=%#v err=%v", result, err)
	}
	markWithdrawalDrainElapsed(t, r, model.Namespace)
	if _, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(model)}); err != nil {
		t.Fatalf("finish all-zero model drain: %v", err)
	}
	if _, err := getUnstructured(r, httpRouteGVK, model.Namespace, modelRouteName(model.Name)); !apierrors.IsNotFound(err) {
		t.Fatalf("disabled model HTTPRoute after serving revision drained = %v, want NotFound", err)
	}
}

func TestAllZeroModelWithdrawalPreservesSiblingLastKnownGoodOnResolutionFailure(t *testing.T) {
	ctx := context.Background()
	r, model, sibling, providerB := reconcilerWithSiblingProvider(t)
	for _, current := range []*v1alpha1.ExternalModel{model, sibling} {
		if _, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(current)}); err != nil {
			t.Fatalf("initial reconcile %s: %v", current.Name, err)
		}
	}
	if err := r.Get(ctx, client.ObjectKeyFromObject(providerB), providerB); err != nil {
		t.Fatal(err)
	}
	providerB.Status.Phase = "Pending"
	providerB.Status.Conditions = []metav1.Condition{{
		Type: conditionReady, Status: metav1.ConditionFalse, Reason: "Reconciling",
		Message: "temporary test failure", LastTransitionTime: metav1.Now(),
	}}
	if err := r.Status().Update(ctx, providerB); err != nil {
		t.Fatalf("mark sibling provider unready: %v", err)
	}
	zero := 0
	var current v1alpha1.ExternalModel
	if err := r.Get(ctx, client.ObjectKeyFromObject(model), &current); err != nil {
		t.Fatal(err)
	}
	current.Spec.ExternalProviderRefs[0].Weight = &zero
	if err := r.Update(ctx, &current); err != nil {
		t.Fatalf("set disabled model weight to zero: %v", err)
	}
	if _, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(model)}); !errors.Is(err, resolver.ErrNoRoutes) {
		t.Fatalf("reconcile with unready sibling = %v, want ErrNoRoutes", err)
	}
	if _, err := getUnstructured(r, httpRouteGVK, model.Namespace, modelRouteName(model.Name)); !apierrors.IsNotFound(err) {
		t.Fatalf("disabled model HTTPRoute = %v, want NotFound", err)
	}
	if _, err := getUnstructured(r, httpRouteGVK, sibling.Namespace, modelRouteName(sibling.Name)); err != nil {
		t.Fatalf("sibling last-known HTTPRoute was removed: %v", err)
	}
	var overlay corev1.ConfigMap
	if err := r.Get(ctx, client.ObjectKey{Namespace: model.Namespace, Name: "routing-overlay"}, &overlay); err != nil {
		t.Fatalf("last-known overlay: %v", err)
	}
	var published envelope.Envelope
	if err := json.Unmarshal([]byte(overlay.Data["routing-overlay.json"]), &published); err != nil {
		t.Fatal(err)
	}
	if len(published.Overlay.Candidates) != 1 || published.Overlay.Candidates[0].StableID != "sibling/provider-provider-b" {
		t.Fatalf("overlay candidates = %#v, want only sibling last-known candidate", published.Overlay.Candidates)
	}
	if _, err := getUnstructured(r, serviceGVK, model.Namespace, providerServicePrefix+"provider-b"); err != nil {
		t.Fatalf("sibling last-known provider Service was removed: %v", err)
	}
	if err := r.Get(ctx, client.ObjectKeyFromObject(model), &current); err != nil {
		t.Fatal(err)
	}
	if current.Status.Phase != "Failed" || current.Status.ObservedGeneration != current.Generation ||
		!hasConditionReason(current.Status.Conditions, conditionReady, reasonNoRoutes) {
		t.Fatalf("disabled model status = %#v, want current-generation Failed/NoRoutes", current.Status)
	}
}

func TestUnequalWeightModelStaysReadyBesideHealthySibling(t *testing.T) {
	ctx := context.Background()
	r, healthy := reconcilerFixture(t)
	providerB := &v1alpha1.ExternalProvider{
		ObjectMeta: metav1.ObjectMeta{Name: "provider-b", Namespace: healthy.Namespace, UID: "provider-b-uid"},
		Spec: v1alpha1.ExternalProviderSpec{Provider: "openai", Endpoint: "b.example.com",
			Auth: v1alpha1.AuthConfig{Type: "apikey", SecretRef: v1alpha1.NameReference{Name: "provider-b-credentials"}}},
		Status: v1alpha1.ExternalProviderStatus{Phase: resolver.PhaseReady},
	}
	if err := r.Create(ctx, providerB); err != nil {
		t.Fatal(err)
	}
	if err := r.Create(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "provider-b-credentials", Namespace: healthy.Namespace},
		Data: map[string][]byte{"api-key": []byte("fixture-only-secret")}}); err != nil {
		t.Fatal(err)
	}
	if _, err := r.ReconcileProvider(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(providerB)}); err != nil {
		t.Fatalf("ReconcileProvider provider-b: %v", err)
	}
	weightOne, weightTwo := 1, 2
	weighted := healthy.DeepCopy()
	weighted.Name, weighted.UID, weighted.ResourceVersion = "weighted", "weighted-uid", ""
	weighted.Status = v1alpha1.ExternalModelStatus{}
	weighted.Spec.ExternalProviderRefs = []v1alpha1.ExternalProviderRef{
		{Ref: v1alpha1.ExternalProviderReference{Name: "provider"}, TargetModel: "gpt", APIFormat: "openai-chat", Path: "/v1/chat/completions", Weight: &weightOne},
		{Ref: v1alpha1.ExternalProviderReference{Name: "provider-b"}, TargetModel: "gpt-b", APIFormat: "openai-chat", Path: "/v1/chat/completions", Weight: &weightTwo},
	}
	if err := r.Create(ctx, weighted); err != nil {
		t.Fatal(err)
	}
	r.KnownClusters = append(r.KnownClusters, "provider-provider-b")
	if _, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(weighted)}); err != nil {
		t.Fatalf("unequal-weight model reconciliation failed: %v", err)
	}
	if _, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(healthy)}); err != nil {
		t.Fatalf("healthy sibling reconciliation failed: %v", err)
	}
	var published corev1.ConfigMap
	if err := r.Get(ctx, client.ObjectKey{Namespace: healthy.Namespace, Name: "routing-overlay"}, &published); err != nil {
		t.Fatal(err)
	}
	var overlay envelope.Envelope
	if err := json.Unmarshal([]byte(published.Data["routing-overlay.json"]), &overlay); err != nil {
		t.Fatal(err)
	}
	if len(overlay.Overlay.Candidates) < 2 {
		t.Fatalf("published candidates = %#v, want both models", overlay.Overlay.Candidates)
	}
	for _, model := range []*v1alpha1.ExternalModel{healthy, weighted} {
		var got v1alpha1.ExternalModel
		if err := r.Get(ctx, client.ObjectKeyFromObject(model), &got); err != nil {
			t.Fatal(err)
		}
		if got.Status.Phase != resolver.PhaseReady {
			t.Fatalf("model %s phase = %q, want Ready", model.Name, got.Status.Phase)
		}
	}
	if err := r.Delete(ctx, healthy); err != nil {
		t.Fatal(err)
	}
	result, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(healthy)})
	if err != nil || result.RequeueAfter <= 0 {
		t.Fatalf("stage healthy model deletion: result=%#v err=%v", result, err)
	}
	seedSafeExternalModelRuntime(t, r, "tenant", healthy.Namespace, []*v1alpha1.ExternalModel{weighted})
	result, err = r.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(healthy)})
	if err != nil || result.RequeueAfter != withdrawalDrainDuration {
		t.Fatalf("start healthy model deletion drain: result=%#v err=%v", result, err)
	}
	markWithdrawalDrainElapsed(t, r, healthy.Namespace)
	if _, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(healthy)}); err != nil {
		t.Fatalf("finish healthy model deletion: %v", err)
	}
	if err := r.Get(ctx, client.ObjectKeyFromObject(healthy), &v1alpha1.ExternalModel{}); !apierrors.IsNotFound(err) {
		t.Fatalf("deleted healthy model still exists: %v", err)
	}
}

func TestCrossModelDuplicateProviderDoesNotBlockUnrelatedModel(t *testing.T) {
	ctx := context.Background()
	r, healthy := reconcilerFixture(t)
	providerB := &v1alpha1.ExternalProvider{ObjectMeta: metav1.ObjectMeta{Name: "provider-b", Namespace: healthy.Namespace, UID: "provider-b-uid"},
		Spec: v1alpha1.ExternalProviderSpec{Provider: "openai", Endpoint: "b.example.com",
			Auth: v1alpha1.AuthConfig{Type: "apikey", SecretRef: v1alpha1.NameReference{Name: "provider-b-credentials"}}},
		Status: v1alpha1.ExternalProviderStatus{Phase: resolver.PhaseReady}}
	if err := r.Create(ctx, providerB); err != nil {
		t.Fatal(err)
	}
	if err := r.Create(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "provider-b-credentials", Namespace: healthy.Namespace},
		Data: map[string][]byte{"api-key": []byte("fixture-only-secret")}}); err != nil {
		t.Fatal(err)
	}
	if _, err := r.ReconcileProvider(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(providerB)}); err != nil {
		t.Fatalf("ReconcileProvider provider-b: %v", err)
	}
	newAlias := func(name string, refs []v1alpha1.ExternalProviderRef) *v1alpha1.ExternalModel {
		for i := range refs {
			refs[i].TargetModel, refs[i].APIFormat, refs[i].Path = "gpt", "openai-chat", "/v1/chat/completions"
		}
		return &v1alpha1.ExternalModel{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: healthy.Namespace, UID: types.UID(name + "-uid")},
			Spec: v1alpha1.ExternalModelSpec{ModelName: "shared", ExternalProviderRefs: refs}}
	}
	first := newAlias("first", []v1alpha1.ExternalProviderRef{
		{Ref: v1alpha1.ExternalProviderReference{Name: "provider"}},
		{Ref: v1alpha1.ExternalProviderReference{Name: "provider-b"}},
	})
	second := newAlias("second", []v1alpha1.ExternalProviderRef{
		{Ref: v1alpha1.ExternalProviderReference{Name: "provider"}},
	})
	for _, model := range []*v1alpha1.ExternalModel{first, second} {
		if err := r.Create(ctx, model); err != nil {
			t.Fatal(err)
		}
	}
	r.KnownClusters = append(r.KnownClusters, "provider-provider-b")
	if _, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(healthy)}); err != nil {
		t.Fatalf("unrelated model reconciliation failed: %v", err)
	}
	var overlay corev1.ConfigMap
	if err := r.Get(ctx, client.ObjectKey{Namespace: healthy.Namespace, Name: "routing-overlay"}, &overlay); err != nil {
		t.Fatal(err)
	}
	var published envelope.Envelope
	if err := json.Unmarshal([]byte(overlay.Data["routing-overlay.json"]), &published); err != nil {
		t.Fatal(err)
	}
	if len(published.Overlay.Candidates) != 1 || published.Overlay.Candidates[0].Name != healthy.Name {
		t.Fatalf("unrelated model overlay candidates = %#v", published.Overlay.Candidates)
	}
	for _, model := range []*v1alpha1.ExternalModel{first, second} {
		var got v1alpha1.ExternalModel
		if err := r.Get(ctx, client.ObjectKeyFromObject(model), &got); err != nil {
			t.Fatal(err)
		}
		if got.Status.Phase != "Failed" {
			t.Fatalf("conflicting model status = %#v, want Failed", got.Status)
		}
	}
	if _, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(first)}); !errors.Is(err, envelope.ErrDuplicateProvider) {
		t.Fatalf("conflicting model error = %v, want ErrDuplicateProvider", err)
	}
	var current v1alpha1.ExternalModel
	if err := r.Get(ctx, client.ObjectKeyFromObject(healthy), &current); err != nil {
		t.Fatal(err)
	}
	if current.Status.Phase != resolver.PhaseReady {
		t.Fatalf("unrelated model phase = %q, want Ready", current.Status.Phase)
	}
	if err := r.Delete(ctx, &current); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(healthy)}); err != nil {
		t.Fatalf("unrelated model deletion blocked: %v", err)
	}
	if err := r.Get(ctx, client.ObjectKeyFromObject(healthy), &current); !apierrors.IsNotFound(err) {
		t.Fatalf("unrelated model remains after deletion: %v", err)
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
	boundaries := []struct {
		name, kind string
		provider   bool
	}{
		{name: "ModelService", kind: "Service"},
		{name: "HTTPRoute", kind: "HTTPRoute"},
		{name: "ProviderService", kind: "Service", provider: true},
		{name: "ServiceEntry", kind: "ServiceEntry", provider: true},
		{name: "DestinationRule", kind: "DestinationRule", provider: true},
	}
	for _, boundary := range boundaries {
		t.Run(boundary.name, func(t *testing.T) {
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
				if object.GetKind() == boundary.kind {
					return fmt.Errorf("injected %s apply failure", boundary.name)
				}
				return render.Apply(ctx, r.Client, []unstructured.Unstructured{object})
			}
			var err error
			if boundary.provider {
				providerReq := reconcile.Request{NamespacedName: client.ObjectKey{Namespace: model.Namespace, Name: "provider"}}
				_, err = r.ReconcileProvider(context.Background(), providerReq)
			} else {
				_, err = r.Reconcile(context.Background(), req)
			}
			if err == nil || !strings.Contains(err.Error(), "injected "+boundary.name) {
				t.Fatalf("%s failure = %v", boundary.name, err)
			}
			var after corev1.ConfigMap
			if err := r.Get(context.Background(), client.ObjectKey{Namespace: "tenant-a", Name: "routing-overlay"}, &after); err != nil {
				t.Fatal(err)
			}
			if after.Data["routing-overlay.json"] != before.Data["routing-overlay.json"] {
				t.Fatalf("%s failure changed the published overlay", boundary.name)
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
		Status:     v1alpha1.ExternalProviderStatus{Phase: resolver.PhaseReady},
	}
	model := &v1alpha1.ExternalModel{
		ObjectMeta: metav1.ObjectMeta{Name: "model", Namespace: "tenant-a", UID: "model-uid"},
		Spec: v1alpha1.ExternalModelSpec{ExternalProviderRefs: []v1alpha1.ExternalProviderRef{{
			Ref: v1alpha1.ExternalProviderReference{Name: provider.Name}, TargetModel: "gpt",
			APIFormat: "openai-chat", Path: "/v1/chat/completions",
		}}},
	}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "credentials", Namespace: "tenant-a"}, Data: map[string][]byte{"api-key": []byte("fixture-only-secret")}}
	ait := controllerAITenant("tenant", "models-as-a-service", "tenant-a", "gateway", "tenant-a")
	mtc := controllerMaasTenantConfig("tenant-a", "tenant", "models-as-a-service", "praxis")
	r := controllerTestClient(t, provider, model, secret, ait, mtc)
	r.Namespace, r.GatewayName, r.GatewayNamespace, r.Network = "tenant-a", "gateway", "tenant-a", "external-model"
	r.KnownClusters = []string{"provider-provider"}
	return r, model
}

func TestProviderReadinessIsReconciledIndependentlyFromModels(t *testing.T) {
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

	modelReq := reconcile.Request{NamespacedName: client.ObjectKeyFromObject(model)}
	if _, err := r.Reconcile(context.Background(), modelReq); !errors.Is(err, resolver.ErrNoRoutes) {
		t.Fatalf("model reconcile with pending provider = %v, want no routes", err)
	}
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(&provider), &provider); err != nil {
		t.Fatal(err)
	}
	if provider.Status.Phase != "Pending" {
		t.Fatalf("model reconcile changed provider phase to %q, want Pending", provider.Status.Phase)
	}
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: "tenant-a", Name: "routing-overlay"}, &corev1.ConfigMap{}); !apierrors.IsNotFound(err) {
		t.Fatalf("routing overlay before provider transport is ready = %v, want NotFound", err)
	}
	if _, err := r.ReconcileProvider(context.Background(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&provider)}); err != nil {
		t.Fatalf("provider reconcile = %v", err)
	}
	if _, err := r.Reconcile(context.Background(), modelReq); err != nil {
		t.Fatalf("model reconcile after provider readiness = %v", err)
	}
	var overlay corev1.ConfigMap
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: "tenant-a", Name: "routing-overlay"}, &overlay); err != nil {
		t.Fatalf("routing overlay after provider transport is ready = %v", err)
	}
}

func TestUnsupportedActiveProviderPreservesLastKnownServingState(t *testing.T) {
	r, model := reconcilerFixture(t)
	ctx := context.Background()
	req := reconcile.Request{NamespacedName: client.ObjectKeyFromObject(model)}
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("initial serving reconcile: %v", err)
	}

	var beforeOverlay corev1.ConfigMap
	if err := r.Get(ctx, client.ObjectKey{Namespace: model.Namespace, Name: "routing-overlay"}, &beforeOverlay); err != nil {
		t.Fatalf("get initial overlay: %v", err)
	}
	beforeRoute := &unstructured.Unstructured{}
	beforeRoute.SetGroupVersionKind(schema.GroupVersionKind{Group: "gateway.networking.k8s.io", Version: "v1", Kind: "HTTPRoute"})
	routeKey := client.ObjectKey{Namespace: model.Namespace, Name: modelRouteName(model.Name)}
	if err := r.Get(ctx, routeKey, beforeRoute); err != nil {
		t.Fatalf("get initial route: %v", err)
	}
	beforeRouteSpec, found, err := unstructured.NestedMap(beforeRoute.Object, "spec")
	if err != nil || !found {
		t.Fatalf("initial HTTPRoute spec = %#v, found=%t, err=%v", beforeRouteSpec, found, err)
	}
	var provider v1alpha1.ExternalProvider
	providerKey := client.ObjectKey{Namespace: model.Namespace, Name: "provider"}
	if err := r.Get(ctx, providerKey, &provider); err != nil {
		t.Fatal(err)
	}
	provider.Spec.Provider = "vertex-openai"
	if err := r.Update(ctx, &provider); err != nil {
		t.Fatal(err)
	}

	applyCalls, publishCalls := 0, 0
	r.ApplyResource = func(context.Context, client.Client, unstructured.Unstructured) error {
		applyCalls++
		return nil
	}
	r.PublishOverlay = func(context.Context, *resolver.ResolvedRouteSet, envelope.Scope, envelope.Options) (publisher.Result, error) {
		publishCalls++
		return publisher.Result{}, nil
	}
	_, err = r.Reconcile(ctx, req)
	if !errors.Is(err, resolver.ErrProviderCapabilityUnsupported) {
		t.Fatalf("reconcile error = %v, want ErrProviderCapabilityUnsupported", err)
	}
	if !strings.Contains(err.Error(), "vertex-openai") || !strings.Contains(err.Error(), "openai-chat") {
		t.Fatalf("unsupported capability error lacks provider/API format: %v", err)
	}
	if applyCalls != 0 || publishCalls != 0 {
		t.Fatalf("unsupported binding wrote serving resources: applies=%d overlayPublishes=%d", applyCalls, publishCalls)
	}

	var afterOverlay corev1.ConfigMap
	if err := r.Get(ctx, client.ObjectKeyFromObject(&beforeOverlay), &afterOverlay); err != nil {
		t.Fatalf("get preserved overlay: %v", err)
	}
	if afterOverlay.Data["routing-overlay.json"] != beforeOverlay.Data["routing-overlay.json"] {
		t.Fatal("unsupported active binding changed the last-known serving overlay")
	}
	afterRoute := &unstructured.Unstructured{}
	afterRoute.SetGroupVersionKind(schema.GroupVersionKind{Group: "gateway.networking.k8s.io", Version: "v1", Kind: "HTTPRoute"})
	if err := r.Get(ctx, routeKey, afterRoute); err != nil {
		t.Fatalf("get preserved route: %v", err)
	}
	afterRouteSpec, found, err := unstructured.NestedMap(afterRoute.Object, "spec")
	if err != nil || !found || !reflect.DeepEqual(afterRouteSpec, beforeRouteSpec) {
		t.Fatalf("unsupported active binding changed last-known HTTPRoute spec: got=%#v want=%#v err=%v", afterRouteSpec, beforeRouteSpec, err)
	}
	var failed v1alpha1.ExternalModel
	if err := r.Get(ctx, client.ObjectKeyFromObject(model), &failed); err != nil {
		t.Fatal(err)
	}
	if failed.Status.Phase != "Failed" || !hasConditionReason(failed.Status.Conditions, conditionReady, reasonReconcileFailed) {
		t.Fatalf("unsupported binding status = phase %q conditions %#v, want a clear reconcile failure", failed.Status.Phase, failed.Status.Conditions)
	}
	statusReportsUnsupported := false
	for _, condition := range failed.Status.Conditions {
		if strings.Contains(condition.Message, string(resolver.SkipAPIFormatUnsupported)) {
			statusReportsUnsupported = true
			break
		}
	}
	if !statusReportsUnsupported {
		t.Fatalf("unsupported binding status does not report %s: %#v", resolver.SkipAPIFormatUnsupported, failed.Status.Conditions)
	}
	for _, condition := range failed.Status.Conditions {
		if strings.Contains(condition.Message, string(resolver.SkipTranslationDisabled)) {
			t.Fatalf("unsupported binding status was mislabeled as operator-disabled translation: %#v", failed.Status.Conditions)
		}
	}
}

type servingStateSnapshot struct {
	routeKey   client.ObjectKey
	serviceKey client.ObjectKey
	secretKey  client.ObjectKey
	routeSpec  map[string]any
	overlay    string
	service    corev1.ServiceSpec
}

func captureServingStateSnapshot(t *testing.T, r *Reconciler, model *v1alpha1.ExternalModel) servingStateSnapshot {
	t.Helper()
	snapshot := servingStateSnapshot{
		routeKey:   client.ObjectKey{Namespace: model.Namespace, Name: modelRouteName(model.Name)},
		serviceKey: client.ObjectKey{Namespace: model.Namespace, Name: providerServicePrefix + "provider"},
		secretKey:  client.ObjectKey{Namespace: model.Namespace, Name: "credentials"},
	}
	var service corev1.Service
	if err := r.Get(context.Background(), snapshot.serviceKey, &service); err != nil {
		t.Fatalf("get initial provider Service: %v", err)
	}
	snapshot.service = *service.Spec.DeepCopy()
	var overlay corev1.ConfigMap
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: model.Namespace, Name: "routing-overlay"}, &overlay); err != nil {
		t.Fatalf("get initial overlay: %v", err)
	}
	snapshot.overlay = overlay.Data["routing-overlay.json"]
	route := &unstructured.Unstructured{}
	route.SetGroupVersionKind(schema.GroupVersionKind{Group: "gateway.networking.k8s.io", Version: "v1", Kind: "HTTPRoute"})
	if err := r.Get(context.Background(), snapshot.routeKey, route); err != nil {
		t.Fatalf("get initial route: %v", err)
	}
	var found bool
	var err error
	snapshot.routeSpec, found, err = unstructured.NestedMap(route.Object, "spec")
	if err != nil || !found {
		t.Fatalf("initial HTTPRoute spec = %#v, found=%t, err=%v", snapshot.routeSpec, found, err)
	}
	rules, found, err := unstructured.NestedSlice(snapshot.routeSpec, "rules")
	if err != nil || !found || len(rules) == 0 {
		t.Fatalf("initial HTTPRoute has no serving rules: rules=%#v found=%t err=%v", rules, found, err)
	}
	firstRule, ok := rules[0].(map[string]any)
	if !ok {
		t.Fatalf("initial HTTPRoute rule has unexpected type %T", rules[0])
	}
	backends, found, err := unstructured.NestedSlice(firstRule, "backendRefs")
	if err != nil || !found || len(backends) != 1 {
		t.Fatalf("initial HTTPRoute backend refs = %#v found=%t err=%v", backends, found, err)
	}
	backend, ok := backends[0].(map[string]any)
	if !ok {
		t.Fatalf("initial HTTPRoute backend has unexpected type %T", backends[0])
	}
	backendName, found, err := unstructured.NestedString(backend, "name")
	if err != nil || !found || backendName != snapshot.serviceKey.Name {
		t.Fatalf("initial HTTPRoute backend = %q, found=%t, want serving provider Service %q (err=%v)", backendName, found, snapshot.serviceKey.Name, err)
	}
	return snapshot
}

func assertServingStateSnapshotRetained(t *testing.T, r *Reconciler, snapshot servingStateSnapshot) {
	t.Helper()
	var overlay corev1.ConfigMap
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: snapshot.routeKey.Namespace, Name: "routing-overlay"}, &overlay); err != nil {
		t.Fatalf("get preserved overlay: %v", err)
	}
	if overlay.Data["routing-overlay.json"] != snapshot.overlay {
		t.Fatal("unsupported active binding changed the last-known serving overlay")
	}
	route := &unstructured.Unstructured{}
	route.SetGroupVersionKind(schema.GroupVersionKind{Group: "gateway.networking.k8s.io", Version: "v1", Kind: "HTTPRoute"})
	if err := r.Get(context.Background(), snapshot.routeKey, route); err != nil {
		t.Fatalf("get preserved route: %v", err)
	}
	afterRouteSpec, found, err := unstructured.NestedMap(route.Object, "spec")
	if err != nil || !found || !reflect.DeepEqual(afterRouteSpec, snapshot.routeSpec) {
		t.Fatalf("unsupported active binding changed last-known HTTPRoute spec: got=%#v want=%#v err=%v", afterRouteSpec, snapshot.routeSpec, err)
	}
	var service corev1.Service
	if err := r.Get(context.Background(), snapshot.serviceKey, &service); err != nil {
		t.Fatalf("unsupported binding removed the serving provider Service: %v", err)
	}
	if !reflect.DeepEqual(service.Spec, snapshot.service) {
		t.Fatalf("unsupported binding changed serving provider Service spec: got=%#v want=%#v", service.Spec, snapshot.service)
	}
	var credential corev1.Secret
	if err := r.Get(context.Background(), snapshot.secretKey, &credential); err != nil {
		t.Fatalf("unsupported binding removed the serving provider credential reference: %v", err)
	}
}

func configureAnthropicMessagesBinding(t *testing.T, r *Reconciler, model *v1alpha1.ExternalModel) {
	t.Helper()
	ctx := context.Background()
	providerKey := client.ObjectKey{Namespace: model.Namespace, Name: "provider"}
	var provider v1alpha1.ExternalProvider
	if err := r.Get(ctx, providerKey, &provider); err != nil {
		t.Fatal(err)
	}
	provider.Spec.Provider = "anthropic"
	if err := r.Update(ctx, &provider); err != nil {
		t.Fatal(err)
	}
	var changedModel v1alpha1.ExternalModel
	if err := r.Get(ctx, client.ObjectKeyFromObject(model), &changedModel); err != nil {
		t.Fatal(err)
	}
	changedModel.Spec.ExternalProviderRefs[0].APIFormat = "messages"
	changedModel.Spec.ExternalProviderRefs[0].Path = "/v1/messages"
	if err := r.Update(ctx, &changedModel); err != nil {
		t.Fatal(err)
	}
	setControllerTranslationEnabled(t, r, model.Namespace, true)
}

func setControllerTranslationEnabled(t *testing.T, r *Reconciler, namespace string, enabled bool) {
	t.Helper()
	var mtc unstructured.Unstructured
	mtc.SetGroupVersionKind(tenant.MaasTenantConfigGVK)
	mtcKey := client.ObjectKey{Namespace: namespace, Name: tenant.MaasTenantConfigInstanceName}
	if err := r.Get(context.Background(), mtcKey, &mtc); err != nil {
		t.Fatal(err)
	}
	if err := unstructured.SetNestedMap(mtc.Object, map[string]any{"enabled": enabled}, "spec", "apiTranslation"); err != nil {
		t.Fatal(err)
	}
	if err := r.Update(context.Background(), &mtc); err != nil {
		t.Fatal(err)
	}
}

func unsetControllerTranslationSetting(t *testing.T, r *Reconciler, namespace string) {
	t.Helper()
	var mtc unstructured.Unstructured
	mtc.SetGroupVersionKind(tenant.MaasTenantConfigGVK)
	mtcKey := client.ObjectKey{Namespace: namespace, Name: tenant.MaasTenantConfigInstanceName}
	if err := r.Get(context.Background(), mtcKey, &mtc); err != nil {
		t.Fatal(err)
	}
	unstructured.RemoveNestedField(mtc.Object, "spec", "apiTranslation")
	if err := r.Update(context.Background(), &mtc); err != nil {
		t.Fatal(err)
	}
}

func assertAnthropicMessagesServingState(t *testing.T, r *Reconciler, model *v1alpha1.ExternalModel, snapshot servingStateSnapshot) {
	t.Helper()
	ctx := context.Background()
	var provider v1alpha1.ExternalProvider
	if err := r.Get(ctx, client.ObjectKey{Namespace: model.Namespace, Name: "provider"}, &provider); err != nil {
		t.Fatal(err)
	}
	if provider.Spec.Provider != "anthropic" {
		t.Fatalf("serving provider type = %q, want anthropic", provider.Spec.Provider)
	}
	var mtc unstructured.Unstructured
	mtc.SetGroupVersionKind(tenant.MaasTenantConfigGVK)
	if err := r.Get(ctx, client.ObjectKey{Namespace: model.Namespace, Name: tenant.MaasTenantConfigInstanceName}, &mtc); err != nil {
		t.Fatal(err)
	}
	enabled, found, err := unstructured.NestedBool(mtc.Object, "spec", "apiTranslation", "enabled")
	if err != nil || !found || !enabled {
		t.Fatalf("translation permission before disable = %t found=%t err=%v, want enabled", enabled, found, err)
	}
	if !strings.Contains(snapshot.overlay, `"api_format": "messages"`) || !strings.Contains(snapshot.overlay, `"path": "/v1/messages"`) {
		t.Fatalf("initial serving overlay does not identify Anthropic Messages profile: %s", snapshot.overlay)
	}
	routeSpec, err := json.Marshal(snapshot.routeSpec)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(routeSpec), "/v1/messages") {
		t.Fatalf("initial serving HTTPRoute does not include Messages path: %s", routeSpec)
	}
}

func assertUnsupportedOpenAIMessagesFailure(t *testing.T, r *Reconciler, model *v1alpha1.ExternalModel, err error) {
	t.Helper()
	if !errors.Is(err, resolver.ErrProviderCapabilityUnsupported) {
		t.Fatalf("reconcile error = %v, want ErrProviderCapabilityUnsupported", err)
	}
	if !strings.Contains(err.Error(), "APIFormatUnsupported") || !strings.Contains(err.Error(), `provider "openai" with APIFormat "messages"`) {
		t.Fatalf("unsupported openai/messages binding error is not specific: %v", err)
	}
	if strings.Contains(err.Error(), string(resolver.SkipTranslationDisabled)) {
		t.Fatalf("unsupported openai/messages binding was mislabeled as operator-disabled translation: %v", err)
	}
	var failed v1alpha1.ExternalModel
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(model), &failed); err != nil {
		t.Fatal(err)
	}
	if failed.Status.Phase != "Failed" || !hasConditionReason(failed.Status.Conditions, conditionReady, reasonReconcileFailed) {
		t.Fatalf("unsupported binding status = phase %q conditions %#v, want a clear reconcile failure", failed.Status.Phase, failed.Status.Conditions)
	}
	unsupportedReasonReported, disabledReasonReported := false, false
	for _, condition := range failed.Status.Conditions {
		unsupportedReasonReported = unsupportedReasonReported || strings.Contains(condition.Message, string(resolver.SkipAPIFormatUnsupported))
		disabledReasonReported = disabledReasonReported || strings.Contains(condition.Message, string(resolver.SkipTranslationDisabled))
	}
	if !unsupportedReasonReported || disabledReasonReported {
		t.Fatalf("unsupported status reason mismatch: conditions=%#v", failed.Status.Conditions)
	}
}

func TestUnsupportedMessagesBindingWithTranslationOmittedPreservesLastKnownServingState(t *testing.T) {
	r, model := reconcilerFixture(t)
	ctx := context.Background()
	req := reconcile.Request{NamespacedName: client.ObjectKeyFromObject(model)}
	providerKey := client.ObjectKey{Namespace: model.Namespace, Name: "provider"}
	if _, err := r.ReconcileProvider(ctx, reconcile.Request{NamespacedName: providerKey}); err != nil {
		t.Fatalf("initial provider transport reconcile: %v", err)
	}
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("initial serving reconcile: %v", err)
	}
	before := captureServingStateSnapshot(t, r, model)
	var changedModel v1alpha1.ExternalModel
	if err := r.Get(ctx, client.ObjectKeyFromObject(model), &changedModel); err != nil {
		t.Fatal(err)
	}
	changedModel.Spec.ExternalProviderRefs[0].APIFormat = "messages"
	changedModel.Spec.ExternalProviderRefs[0].Path = "/v1/messages"
	if err := r.Update(ctx, &changedModel); err != nil {
		t.Fatal(err)
	}
	applyCalls, publishCalls := 0, 0
	r.ApplyResource = func(context.Context, client.Client, unstructured.Unstructured) error {
		applyCalls++
		return nil
	}
	r.PublishOverlay = func(context.Context, *resolver.ResolvedRouteSet, envelope.Scope, envelope.Options) (publisher.Result, error) {
		publishCalls++
		return publisher.Result{}, nil
	}
	_, err := r.Reconcile(ctx, req)
	assertUnsupportedOpenAIMessagesFailure(t, r, model, err)
	if applyCalls != 0 || publishCalls != 0 {
		t.Fatalf("unsupported binding wrote serving resources: applies=%d overlayPublishes=%d", applyCalls, publishCalls)
	}
	assertServingStateSnapshotRetained(t, r, before)
}

func TestTranslationOffPolicyRemovesSupportedMessagesServingRoute(t *testing.T) {
	for _, tc := range []struct {
		name    string
		omitted bool
	}{
		{name: "apiTranslation omitted", omitted: true},
		{name: "apiTranslation explicitly false"},
	} {
		t.Run(tc.name, func(t *testing.T) { testTranslationOffRemovesMessagesServingRoute(t, tc.omitted) })
	}
}

func TestTranslationOffWithdrawsMessagesRouteWhenItsProviderIsUnavailable(t *testing.T) {
	for _, tc := range []struct {
		name    string
		missing bool
	}{
		{name: "provider missing", missing: true},
		{name: "provider not ready"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, model := reconcilerFixture(t)
			ctx := context.Background()
			configureAnthropicMessagesBinding(t, r, model)
			modelReq := reconcile.Request{NamespacedName: client.ObjectKeyFromObject(model)}
			if _, err := r.Reconcile(ctx, modelReq); err != nil {
				t.Fatalf("initial Messages reconcile: %v", err)
			}
			var provider v1alpha1.ExternalProvider
			if err := r.Get(ctx, client.ObjectKey{Namespace: model.Namespace, Name: "provider"}, &provider); err != nil {
				t.Fatal(err)
			}
			if tc.missing {
				if err := r.Delete(ctx, &provider); err != nil {
					t.Fatalf("delete Messages provider: %v", err)
				}
			} else {
				provider.Status.Phase = "Pending"
				if err := r.Status().Update(ctx, &provider); err != nil {
					t.Fatalf("mark Messages provider not Ready: %v", err)
				}
			}
			setControllerTranslationEnabled(t, r, model.Namespace, false)
			if _, err := r.Reconcile(ctx, modelReq); !errors.Is(err, resolver.ErrNoRoutes) {
				t.Fatalf("translation-off reconcile = %v, want ErrNoRoutes", err)
			}
			var route unstructured.Unstructured
			route.SetGroupVersionKind(schema.GroupVersionKind{Group: "gateway.networking.k8s.io", Version: "v1", Kind: "HTTPRoute"})
			if err := r.Get(ctx, client.ObjectKey{Namespace: model.Namespace, Name: modelRouteName(model.Name)}, &route); !apierrors.IsNotFound(err) {
				t.Fatalf("Messages route after disabling translation = %v, want NotFound", err)
			}
			var overlay corev1.ConfigMap
			if err := r.Get(ctx, client.ObjectKey{Namespace: model.Namespace, Name: "routing-overlay"}, &overlay); !apierrors.IsNotFound(err) {
				t.Fatalf("Messages overlay after disabling translation = %v, want NotFound", err)
			}
		})
	}
}

func testTranslationOffRemovesMessagesServingRoute(t *testing.T, omitted bool) {
	t.Helper()
	r, model := reconcilerFixture(t)
	ctx := context.Background()
	providerKey := client.ObjectKey{Namespace: model.Namespace, Name: "provider"}
	modelReq := reconcile.Request{NamespacedName: client.ObjectKeyFromObject(model)}
	configureAnthropicMessagesBinding(t, r, model)
	if _, err := r.ReconcileProvider(ctx, reconcile.Request{NamespacedName: providerKey}); err != nil {
		t.Fatalf("initial provider transport reconcile: %v", err)
	}
	if _, err := r.Reconcile(ctx, modelReq); err != nil {
		t.Fatalf("initial serving reconcile: %v", err)
	}
	before := captureServingStateSnapshot(t, r, model)
	assertAnthropicMessagesServingState(t, r, model, before)
	if omitted {
		unsetControllerTranslationSetting(t, r, model.Namespace)
	} else {
		setControllerTranslationEnabled(t, r, model.Namespace, false)
	}

	_, err := r.Reconcile(ctx, modelReq)
	if !errors.Is(err, resolver.ErrNoRoutes) {
		t.Fatalf("reconcile with translation-off policy = %v, want ErrNoRoutes", err)
	}
	var route unstructured.Unstructured
	route.SetGroupVersionKind(schema.GroupVersionKind{Group: "gateway.networking.k8s.io", Version: "v1", Kind: "HTTPRoute"})
	routeKey := client.ObjectKey{Namespace: model.Namespace, Name: modelRouteName(model.Name)}
	if err := r.Get(ctx, routeKey, &route); !apierrors.IsNotFound(err) {
		t.Fatalf("supported Messages route after explicit translation disable = %v, want NotFound", err)
	}
	var overlay corev1.ConfigMap
	if err := r.Get(ctx, client.ObjectKey{Namespace: model.Namespace, Name: "routing-overlay"}, &overlay); !apierrors.IsNotFound(err) {
		t.Fatalf("routing overlay after explicit translation disable = %v, want NotFound", err)
	}
	var failed v1alpha1.ExternalModel
	if err := r.Get(ctx, client.ObjectKeyFromObject(model), &failed); err != nil {
		t.Fatal(err)
	}
	if failed.Status.Phase != "Failed" || !hasConditionReason(failed.Status.Conditions, conditionReady, reasonTranslationDisabled) {
		t.Fatalf("translation-disabled Messages model status = phase %q conditions %#v, want the policy-excluded route to be Failed", failed.Status.Phase, failed.Status.Conditions)
	}
	messageExplainsPermission := false
	for _, condition := range failed.Status.Conditions {
		if strings.Contains(condition.Message, "apiTranslation.enabled must be true") {
			messageExplainsPermission = true
		}
	}
	if !messageExplainsPermission {
		t.Fatalf("translation-off status does not explain the required setting: %#v", failed.Status.Conditions)
	}
	var providerService corev1.Service
	if err := r.Get(ctx, before.serviceKey, &providerService); err != nil {
		t.Fatalf("disabling translation removed the provider-owned Service: %v", err)
	}
	var credential corev1.Secret
	if err := r.Get(ctx, client.ObjectKey{Namespace: model.Namespace, Name: "credentials"}, &credential); err != nil {
		t.Fatalf("disabling translation removed provider credentials: %v", err)
	}
}

func TestTranslationOffCleansDisabledRouteWhenSiblingProviderIsMissing(t *testing.T) {
	r, messagesModel := reconcilerFixture(t)
	ctx := context.Background()
	if err := r.Delete(ctx, messagesModel); err != nil {
		t.Fatalf("remove default model fixture: %v", err)
	}
	messagesModel.Name = "a"
	messagesModel.ResourceVersion = ""
	if err := r.Create(ctx, messagesModel); err != nil {
		t.Fatalf("create Messages model a: %v", err)
	}
	configureAnthropicMessagesBinding(t, r, messagesModel)
	missingProvider := &v1alpha1.ExternalProvider{
		ObjectMeta: metav1.ObjectMeta{Name: "second-provider", Namespace: messagesModel.Namespace, UID: "second-provider-uid"},
		Spec: v1alpha1.ExternalProviderSpec{
			Provider: "openai", Endpoint: "second.example.com",
			Auth: v1alpha1.AuthConfig{Type: "apikey", SecretRef: v1alpha1.NameReference{Name: "second-credentials"}},
		},
		Status: v1alpha1.ExternalProviderStatus{Phase: resolver.PhaseReady},
	}
	secondSecret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "second-credentials", Namespace: messagesModel.Namespace}, Data: map[string][]byte{"api-key": []byte("fixture-only-second-secret")}}
	secondModel := &v1alpha1.ExternalModel{
		ObjectMeta: metav1.ObjectMeta{Name: "a.b", Namespace: messagesModel.Namespace, UID: "second-model-uid"},
		Spec: v1alpha1.ExternalModelSpec{ModelName: "second-client", ExternalProviderRefs: []v1alpha1.ExternalProviderRef{{
			Ref: v1alpha1.ExternalProviderReference{Name: missingProvider.Name}, TargetModel: "second-target",
			APIFormat: "openai-chat", Path: "/v1/chat/completions",
		}}},
	}
	for _, object := range []client.Object{missingProvider, secondSecret, secondModel} {
		if err := r.Create(ctx, object); err != nil {
			t.Fatalf("create sibling fixture %T %s/%s: %v", object, object.GetNamespace(), object.GetName(), err)
		}
	}
	r.KnownClusters = []string{"provider-provider", "provider-second-provider"}
	modelReq := reconcile.Request{NamespacedName: client.ObjectKeyFromObject(messagesModel)}
	if _, err := r.Reconcile(ctx, modelReq); err != nil {
		t.Fatalf("initial serving reconcile with both providers ready: %v", err)
	}
	var before corev1.ConfigMap
	if err := r.Get(ctx, client.ObjectKey{Namespace: messagesModel.Namespace, Name: "routing-overlay"}, &before); err != nil {
		t.Fatalf("get initial routing overlay: %v", err)
	}
	var beforeEnvelope envelope.Envelope
	if err := json.Unmarshal([]byte(before.Data[publisher.DefaultDataKey]), &beforeEnvelope); err != nil {
		t.Fatalf("decode initial routing overlay: %v", err)
	}
	if len(beforeEnvelope.Overlay.Candidates) != 2 {
		t.Fatalf("initial candidates = %d, want Messages and OpenAI sibling candidates", len(beforeEnvelope.Overlay.Candidates))
	}

	if err := r.Delete(ctx, missingProvider); err != nil {
		t.Fatalf("remove sibling provider to simulate provider-not-found: %v", err)
	}
	setControllerTranslationEnabled(t, r, messagesModel.Namespace, false)
	if _, err := r.Reconcile(ctx, modelReq); !errors.Is(err, resolver.ErrNoRoutes) {
		t.Fatalf("translation-off reconcile with missing sibling provider = %v, want ErrNoRoutes", err)
	}

	messageRoute := &unstructured.Unstructured{}
	messageRoute.SetGroupVersionKind(schema.GroupVersionKind{Group: "gateway.networking.k8s.io", Version: "v1", Kind: "HTTPRoute"})
	if err := r.Get(ctx, client.ObjectKey{Namespace: messagesModel.Namespace, Name: modelRouteName(messagesModel.Name)}, messageRoute); !apierrors.IsNotFound(err) {
		t.Fatalf("translation-disabled Messages route remains after sibling resolution failure: %v", err)
	}
	secondRoute := &unstructured.Unstructured{}
	secondRoute.SetGroupVersionKind(schema.GroupVersionKind{Group: "gateway.networking.k8s.io", Version: "v1", Kind: "HTTPRoute"})
	if err := r.Get(ctx, client.ObjectKey{Namespace: secondModel.Namespace, Name: modelRouteName(secondModel.Name)}, secondRoute); err != nil {
		t.Fatalf("unrelated sibling's last-known route was removed: %v", err)
	}
	var after corev1.ConfigMap
	if err := r.Get(ctx, client.ObjectKeyFromObject(&before), &after); err != nil {
		t.Fatalf("get routing overlay after policy cleanup: %v", err)
	}
	var afterEnvelope envelope.Envelope
	if err := json.Unmarshal([]byte(after.Data[publisher.DefaultDataKey]), &afterEnvelope); err != nil {
		t.Fatalf("decode routing overlay after policy cleanup: %v", err)
	}
	if len(afterEnvelope.Overlay.Candidates) != 1 || afterEnvelope.Overlay.Candidates[0].StableID != envelope.CandidateStableID(secondModel.Name, missingProvider.Name) {
		t.Fatalf("candidates after translation-policy cleanup = %#v, want only the missing-provider sibling's last-known candidate", afterEnvelope.Overlay.Candidates)
	}

	var routeFilter unstructured.Unstructured
	routeFilter.SetGroupVersionKind(schema.GroupVersionKind{Group: "networking.istio.io", Version: "v1alpha3", Kind: "EnvoyFilter"})
	filterKey := client.ObjectKey{Namespace: r.GatewayNamespace, Name: tenant.PayloadProcessingExternalModelEnvoyFilterName("tenant")}
	if err := r.Get(ctx, filterKey, &routeFilter); err != nil {
		t.Fatalf("get route EnvoyFilter after policy cleanup: %v", err)
	}
	patches := nestedSlice(t, routeFilter.Object, "spec", "configPatches")
	var messagePatches, secondPatches int
	for _, raw := range patches {
		patch, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		name, _, _ := unstructured.NestedString(patch, "match", "routeConfiguration", "vhost", "route", "name")
		if strings.HasPrefix(name, secondModel.Namespace+"."+modelRouteName(secondModel.Name)+".") {
			secondPatches++
		} else if strings.HasPrefix(name, messagesModel.Namespace+"."+modelRouteName(messagesModel.Name)+".") {
			messagePatches++
		}
	}
	if messagePatches != 0 || secondPatches != 4 {
		t.Fatalf("route patches after policy cleanup = Messages:%d sibling:%d, want four sibling patches only", messagePatches, secondPatches)
	}
}

func TestAllRoutesExcludedByPolicyOnlyAcceptsTranslationOff(t *testing.T) {
	cases := []struct {
		name string
		set  *resolver.ResolvedRouteSet
		want bool
	}{
		{
			name: "only translation disabled",
			set:  &resolver.ResolvedRouteSet{Models: []resolver.ModelRoutes{{Skips: []resolver.Skip{{Reason: resolver.SkipTranslationDisabled}}}}},
			want: true,
		},
		{
			name: "unsupported format is not policy",
			set:  &resolver.ResolvedRouteSet{Models: []resolver.ModelRoutes{{Skips: []resolver.Skip{{Reason: resolver.SkipAPIFormatUnsupported}}}}},
			want: false,
		},
		{
			name: "translation disabled with unrelated provider missing is not wholly policy excluded",
			set: &resolver.ResolvedRouteSet{Models: []resolver.ModelRoutes{
				{ModelRef: "tenant-a/messages", Skips: []resolver.Skip{{Reason: resolver.SkipTranslationDisabled}}},
				{ModelRef: "tenant-a/openai", Skips: []resolver.Skip{{Reason: resolver.SkipRefNotFound}}},
			}},
			want: false,
		},
		{
			name: "unexplained empty model is not policy",
			set:  &resolver.ResolvedRouteSet{Models: []resolver.ModelRoutes{{}}},
			want: false,
		},
		{
			name: "empty model set is not policy",
			set:  &resolver.ResolvedRouteSet{},
			want: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := allRoutesExcludedByPolicy(tc.set); got != tc.want {
				t.Fatalf("allRoutesExcludedByPolicy() = %t, want %t", got, tc.want)
			}
		})
	}
}

func TestHomeRouteFailureReportsOnlyCurrentModelSkips(t *testing.T) {
	set := &resolver.ResolvedRouteSet{Models: []resolver.ModelRoutes{
		{ModelRef: "tenant-a/current", Skips: []resolver.Skip{{ProviderRef: "current-provider", Reason: resolver.SkipRefNotFound, Message: "missing"}}},
		{ModelRef: "tenant-a/other", Skips: []resolver.Skip{{ProviderRef: "other-provider", Reason: resolver.SkipRefNotFound, Message: "missing"}}},
	}}
	var reason, message string
	r := &Reconciler{}
	_, err := r.handleHomeRouteResolutionError(context.Background(), "tenant-a", modelTenantContext{}, set, "current", "uid", resolver.ErrNoRoutes,
		func(_ bool, gotReason, gotMessage string, _ *envelope.Envelope) error {
			reason, message = gotReason, gotMessage
			return nil
		})
	if !errors.Is(err, resolver.ErrNoRoutes) || reason != reasonProviderNotReady ||
		!strings.Contains(message, "current-provider") || strings.Contains(message, "other-provider") {
		t.Fatalf("current model status reason=%q message=%q err=%v", reason, message, err)
	}
}

func TestProviderReportsTenantHandoffReadinessCondition(t *testing.T) {
	r, _ := reconcilerFixture(t)
	ctx := context.Background()

	mtc := tenant.NewMaasTenantConfig()
	mtcKey := client.ObjectKey{Namespace: "tenant-a", Name: tenant.MaasTenantConfigInstanceName}
	if err := r.Get(ctx, mtcKey, mtc); err != nil {
		t.Fatal(err)
	}
	annotations := mtc.GetAnnotations()
	annotations[tenant.AnnotationPayloadProcessingStatus] = tenant.PayloadProcessingStatusCleanupComplete
	mtc.SetAnnotations(annotations)
	if err := r.Update(ctx, mtc); err != nil {
		t.Fatal(err)
	}

	providerKey := client.ObjectKey{Namespace: "tenant-a", Name: "provider"}
	if _, err := r.ReconcileProvider(ctx, reconcile.Request{NamespacedName: providerKey}); err != nil {
		t.Fatalf("reconcile provider during tenant handoff: %v", err)
	}

	var got v1alpha1.ExternalProvider
	if err := r.Get(ctx, providerKey, &got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != providerPhasePending || got.Status.ObservedGeneration != got.Generation {
		t.Fatalf("provider status = %#v, want %s at current generation", got.Status, providerPhasePending)
	}
	var ready *metav1.Condition
	for i := range got.Status.Conditions {
		if got.Status.Conditions[i].Type == conditionReady {
			ready = &got.Status.Conditions[i]
			break
		}
	}
	if ready == nil {
		t.Fatalf("provider Ready condition missing: %#v", got.Status.Conditions)
	}
	if ready.Status != metav1.ConditionFalse || ready.Reason != reasonTenantNotReady {
		t.Fatalf("provider Ready condition = %#v, want False with reason %q", *ready, reasonTenantNotReady)
	}
	if !strings.Contains(ready.Message, tenant.PayloadProcessingStatusCleanupComplete) || !strings.Contains(ready.Message, tenant.PayloadProcessingStatusSteady) {
		t.Fatalf("provider Ready condition message = %q, want actual and required handoff states", ready.Message)
	}
}

func TestModelRoutesRequireCurrentProviderGeneration(t *testing.T) {
	provider := &v1alpha1.ExternalProvider{
		ObjectMeta: metav1.ObjectMeta{Name: "provider", Namespace: "tenant-a", UID: "provider-uid", Generation: 2},
		Spec:       v1alpha1.ExternalProviderSpec{Provider: "openai", Endpoint: "api.example.com", Auth: v1alpha1.AuthConfig{Type: "apikey", SecretRef: v1alpha1.NameReference{Name: "credentials"}}},
		Status:     v1alpha1.ExternalProviderStatus{Phase: resolver.PhaseReady, ObservedGeneration: 1},
	}
	model := &v1alpha1.ExternalModel{
		ObjectMeta: metav1.ObjectMeta{Name: "model", Namespace: "tenant-a", UID: "model-uid"},
		Spec: v1alpha1.ExternalModelSpec{ExternalProviderRefs: []v1alpha1.ExternalProviderRef{{
			Ref: v1alpha1.ExternalProviderReference{Name: provider.Name}, TargetModel: "gpt",
			APIFormat: "openai-chat", Path: "/v1/chat/completions",
		}}},
	}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "credentials", Namespace: "tenant-a"}, Data: map[string][]byte{"api-key": []byte("fixture-only-secret")}}
	ait := controllerAITenant("tenant", "models-as-a-service", "tenant-a", "gateway", "tenant-a")
	mtc := controllerMaasTenantConfig("tenant-a", "tenant", "models-as-a-service", "praxis")
	r := controllerTestClient(t, provider, model, secret, ait, mtc)
	r.Namespace, r.GatewayName, r.GatewayNamespace, r.Network = "tenant-a", "gateway", "tenant-a", "external-model"
	r.KnownClusters = []string{"provider-provider"}

	req := reconcile.Request{NamespacedName: client.ObjectKeyFromObject(model)}
	if _, err := r.Reconcile(context.Background(), req); !errors.Is(err, resolver.ErrNoRoutes) {
		t.Fatalf("model reconcile with stale Provider generation = %v, want no routes", err)
	}
	route := &unstructured.Unstructured{}
	route.SetGroupVersionKind(schema.GroupVersionKind{Group: "gateway.networking.k8s.io", Version: "v1", Kind: "HTTPRoute"})
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: "tenant-a", Name: "external-model-model"}, route); !apierrors.IsNotFound(err) {
		t.Fatalf("HTTPRoute for stale Provider generation error = %v, want NotFound", err)
	}
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: "tenant-a", Name: "routing-overlay"}, &corev1.ConfigMap{}); !apierrors.IsNotFound(err) {
		t.Fatalf("routing overlay for stale Provider generation error = %v, want NotFound", err)
	}
}

func TestReconcileRequiresSteadyPayloadProcessingHandoff(t *testing.T) {
	for _, status := range []string{"", tenant.PayloadProcessingStatusCleanupComplete, tenant.PayloadProcessingStatusSteady} {
		t.Run(map[string]string{"": "absent", tenant.PayloadProcessingStatusCleanupComplete: "cleanup-complete", tenant.PayloadProcessingStatusSteady: "steady"}[status], func(t *testing.T) {
			r, model := reconcilerFixture(t)
			var mtc unstructured.Unstructured
			mtc.SetGroupVersionKind(tenant.MaasTenantConfigGVK)
			if err := r.Get(context.Background(), client.ObjectKey{Namespace: model.Namespace, Name: tenant.MaasTenantConfigInstanceName}, &mtc); err != nil {
				t.Fatal(err)
			}
			annotations := mtc.GetAnnotations()
			if status == "" {
				delete(annotations, tenant.AnnotationPayloadProcessingStatus)
			} else {
				annotations[tenant.AnnotationPayloadProcessingStatus] = status
			}
			mtc.SetAnnotations(annotations)
			if err := r.Update(context.Background(), &mtc); err != nil {
				t.Fatal(err)
			}
			if _, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(model)}); err != nil {
				t.Fatal(err)
			}
			var overlay corev1.ConfigMap
			overlayErr := r.Get(context.Background(), client.ObjectKey{Namespace: model.Namespace, Name: "routing-overlay"}, &overlay)
			if status == tenant.PayloadProcessingStatusSteady {
				if overlayErr != nil {
					t.Fatalf("steady handoff did not publish serving state: %v", overlayErr)
				}
			} else if !apierrors.IsNotFound(overlayErr) {
				t.Fatalf("status %q published serving state: %v", status, overlayErr)
			}
		})
	}
}

func TestExplicitIPPSwitchCleansWithoutSteadyStatus(t *testing.T) {
	for _, status := range []string{"", tenant.PayloadProcessingStatusCleanupComplete} {
		t.Run(map[string]string{"": "absent", tenant.PayloadProcessingStatusCleanupComplete: "cleanup-complete"}[status], func(t *testing.T) {
			r, model := reconcilerFixture(t)
			if _, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(model)}); err != nil {
				t.Fatal(err)
			}
			var mtc unstructured.Unstructured
			mtc.SetGroupVersionKind(tenant.MaasTenantConfigGVK)
			if err := r.Get(context.Background(), client.ObjectKey{Namespace: model.Namespace, Name: tenant.MaasTenantConfigInstanceName}, &mtc); err != nil {
				t.Fatal(err)
			}
			annotations := mtc.GetAnnotations()
			annotations[tenant.AnnotationPayloadProcessingType] = tenant.PayloadProcessingBackendIPP
			if status == "" {
				delete(annotations, tenant.AnnotationPayloadProcessingStatus)
			} else {
				annotations[tenant.AnnotationPayloadProcessingStatus] = status
			}
			mtc.SetAnnotations(annotations)
			if err := r.Update(context.Background(), &mtc); err != nil {
				t.Fatal(err)
			}
			if _, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(model)}); err != nil {
				t.Fatal(err)
			}
			var overlay corev1.ConfigMap
			if err := r.Get(context.Background(), client.ObjectKey{Namespace: model.Namespace, Name: "routing-overlay"}, &overlay); !apierrors.IsNotFound(err) {
				t.Fatalf("IPP switch with status %q left overlay: %v", status, err)
			}
		})
	}
}

type overridingTenantConfigReader struct {
	client.Reader
	mtc *unstructured.Unstructured
}

func (r overridingTenantConfigReader) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if u, ok := obj.(*unstructured.Unstructured); ok && u.GroupVersionKind() == tenant.MaasTenantConfigGVK && key.Name == tenant.MaasTenantConfigInstanceName {
		*u = *r.mtc.DeepCopy()
		return nil
	}
	return r.Reader.Get(ctx, key, obj, opts...)
}

func TestStalePraxisDecisionDoesNotPublishAfterLiveIPPSwitch(t *testing.T) {
	r, model := reconcilerFixture(t)
	ipp := controllerMaasTenantConfig(model.Namespace, "tenant", "models-as-a-service", tenant.PayloadProcessingBackendIPP)
	r.APIReader = overridingTenantConfigReader{Reader: r.APIReader, mtc: ipp}
	published := false
	r.PublishOverlay = func(context.Context, *resolver.ResolvedRouteSet, envelope.Scope, envelope.Options) (publisher.Result, error) {
		published = true
		return publisher.Result{}, nil
	}
	if _, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(model)}); err != nil {
		t.Fatal(err)
	}
	if published {
		t.Fatal("stale Praxis decision published after live IPP switch")
	}
}

func TestDeletingModelAndMissingProviderCleanOwnedResourcesWithoutMTCOrAITenant(t *testing.T) {
	for _, withMTC := range []bool{false, true} {
		t.Run(map[bool]string{false: "missing-mtc", true: "missing-aitenant"}[withMTC], func(t *testing.T) {
			model := &v1alpha1.ExternalModel{ObjectMeta: metav1.ObjectMeta{
				Name: "model", Namespace: "tenant-a", Finalizers: []string{externalModelFinalizer},
				DeletionTimestamp: &metav1.Time{Time: time.Now().Add(-time.Minute)},
			}}
			labels := map[string]any{"app.kubernetes.io/managed-by": "ai-gateway-controller", externalTenantLabel: "tenant-a"}
			filter := &unstructured.Unstructured{Object: map[string]any{
				"apiVersion": "networking.istio.io/v1alpha3", "kind": "EnvoyFilter",
				"metadata": map[string]any{"name": tenant.PayloadProcessingExternalModelEnvoyFilterName("tenant"), "namespace": "gateway-old", "labels": labels},
			}}
			route := resolver.Route{Model: "model", Provider: "provider", Namespace: "tenant-a", Endpoint: "provider.example.com"}
			dr := providerDestinationRuleAtGateway(route, "gateway-old", "gateway-old", "tenant")
			svc := providerService(route, "tenant-a")
			entry := providerServiceEntry(route, "tenant-a")
			httpRoute := modelHTTPRouteSet([]resolver.Route{route}, "tenant-a", "gateway", "gateway-old")
			overlay := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "routing-overlay", Namespace: "tenant-a", Labels: map[string]string{"app.kubernetes.io/managed-by": "ai-gateway-controller"}}}
			objects := []client.Object{model, filter, &dr, &svc, &entry, &httpRoute, overlay}
			if withMTC {
				objects = append(objects, controllerMaasTenantConfig("tenant-a", "missing", "models-as-a-service", tenant.PayloadProcessingBackendPraxis))
			}
			r := controllerTestClient(t, objects...)
			r.Namespace = "tenant-a"
			if _, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(model)}); err != nil {
				t.Fatal(err)
			}
			if _, err := r.ReconcileProvider(context.Background(), reconcile.Request{NamespacedName: client.ObjectKey{Namespace: model.Namespace, Name: "provider"}}); err != nil {
				t.Fatalf("cleanup resources from missing provider: %v", err)
			}
			var gotModel v1alpha1.ExternalModel
			if err := r.Get(context.Background(), client.ObjectKeyFromObject(model), &gotModel); err != nil && !apierrors.IsNotFound(err) {
				t.Fatal(err)
			}
			if controllerutil.ContainsFinalizer(&gotModel, externalModelFinalizer) {
				t.Fatal("deleting model retained finalizer after ownerless cleanup")
			}
			for _, object := range []struct {
				obj       client.Object
				group     string
				version   string
				kind      string
				namespace string
			}{
				{filter, "networking.istio.io", "v1alpha3", "EnvoyFilter", "gateway-old"},
				{&dr, "networking.istio.io", "v1", "DestinationRule", "gateway-old"},
				{&svc, "", "v1", "Service", "tenant-a"},
				{&entry, "networking.istio.io", "v1", "ServiceEntry", "tenant-a"},
				{&httpRoute, "gateway.networking.k8s.io", "v1", "HTTPRoute", "tenant-a"},
				{overlay, "", "v1", "ConfigMap", "tenant-a"},
			} {
				got := &unstructured.Unstructured{}
				got.SetGroupVersionKind(schema.GroupVersionKind{Group: object.group, Version: object.version, Kind: object.kind})
				if err := r.Get(context.Background(), client.ObjectKey{Namespace: object.namespace, Name: object.obj.GetName()}, got); !apierrors.IsNotFound(err) {
					t.Fatalf("owned %s/%s remained: %v", object.namespace, object.obj.GetName(), err)
				}
			}
		})
	}
}

func TestReconcileDeletingModelHonorsIPPSwitchWithSurvivingSibling(t *testing.T) {
	deleted := &v1alpha1.ExternalModel{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "deleted-model",
			Namespace:         "tenant-a",
			Finalizers:        []string{externalModelFinalizer},
			DeletionTimestamp: &metav1.Time{Time: time.Now().Add(-time.Minute)},
		},
	}
	sibling := &v1alpha1.ExternalModel{
		ObjectMeta: metav1.ObjectMeta{Name: "surviving-model", Namespace: "tenant-a", UID: "sibling-uid"},
		Spec: v1alpha1.ExternalModelSpec{ExternalProviderRefs: []v1alpha1.ExternalProviderRef{{
			Ref: v1alpha1.ExternalProviderReference{Name: "provider"}, TargetModel: "gpt",
			APIFormat: "openai-chat", Path: "/v1/chat/completions",
		}}},
	}
	provider := &v1alpha1.ExternalProvider{
		ObjectMeta: metav1.ObjectMeta{Name: "provider", Namespace: "tenant-a", UID: "provider-uid"},
		Spec: v1alpha1.ExternalProviderSpec{
			Provider: "openai", Endpoint: "api.example.com",
			Auth: v1alpha1.AuthConfig{Type: "apikey", SecretRef: v1alpha1.NameReference{Name: "credentials"}},
		},
	}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "credentials", Namespace: "tenant-a"}, Data: map[string][]byte{"api-key": []byte("fixture")}}
	ait := controllerAITenant("tenant", "models-as-a-service", "tenant-a", "gateway", "gateway-system")
	mtc := controllerMaasTenantConfig("tenant-a", "tenant", "models-as-a-service", tenant.PayloadProcessingBackendIPP)
	overlay := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "routing-overlay", Namespace: "tenant-a", Labels: map[string]string{"app.kubernetes.io/managed-by": "ai-gateway-controller"}},
		Data:       map[string]string{"routing-overlay.json": `{"serving":"last-known-good"}`},
	}
	r := controllerTestClient(t, deleted, sibling, provider, secret, ait, mtc, overlay)
	r.Namespace, r.GatewayName, r.GatewayNamespace = "tenant-a", "gateway", "gateway-system"
	published := false
	r.PublishOverlay = func(context.Context, *resolver.ResolvedRouteSet, envelope.Scope, envelope.Options) (publisher.Result, error) {
		published = true
		return publisher.Result{}, nil
	}
	if _, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(deleted)}); err != nil {
		t.Fatal(err)
	}
	if published {
		t.Fatal("deleting model on explicit IPP switch republished a Praxis overlay")
	}
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(overlay), &corev1.ConfigMap{}); !apierrors.IsNotFound(err) {
		t.Fatalf("IPP cleanup left the routing overlay: %v", err)
	}
	var got v1alpha1.ExternalModel
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(deleted), &got); !apierrors.IsNotFound(err) {
		t.Fatalf("IPP cleanup left deleting model present with its finalizer: %v", err)
	}
}

func TestOwnerlessDeletingModelWithSiblingRetainsFinalizerWhenIdentityIsAmbiguous(t *testing.T) {
	deleted := &v1alpha1.ExternalModel{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "deleted-model",
			Namespace:         "tenant-a",
			Finalizers:        []string{externalModelFinalizer},
			DeletionTimestamp: &metav1.Time{Time: time.Now().Add(-time.Minute)},
		},
	}
	sibling := &v1alpha1.ExternalModel{ObjectMeta: metav1.ObjectMeta{Name: "surviving-model", Namespace: "tenant-a"}}
	r := controllerTestClient(t, deleted, sibling)
	r.Namespace = "tenant-a"
	if _, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(deleted)}); err == nil {
		t.Fatal("ownerless deletion with an ambiguous sibling unexpectedly succeeded")
	}
	var got v1alpha1.ExternalModel
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(deleted), &got); err != nil {
		t.Fatal(err)
	}
	if !controllerutil.ContainsFinalizer(&got, externalModelFinalizer) {
		t.Fatal("ambiguous ownerless deletion dropped the finalizer")
	}
}

func TestDeletingModelDoesNotRebuildSiblingRoutesBeforeSteadyHandoff(t *testing.T) {
	for _, status := range []string{"missing-mtc", "absent", tenant.PayloadProcessingStatusCleanupComplete} {
		t.Run(status, func(t *testing.T) {
			deleted := &v1alpha1.ExternalModel{ObjectMeta: metav1.ObjectMeta{
				Name: "deleted-model", Namespace: "tenant-a", Finalizers: []string{externalModelFinalizer},
				DeletionTimestamp: &metav1.Time{Time: time.Now().Add(-time.Minute)},
			}}
			sibling := &v1alpha1.ExternalModel{
				ObjectMeta: metav1.ObjectMeta{Name: "sibling", Namespace: "tenant-a", UID: "sibling-uid"},
				Spec: v1alpha1.ExternalModelSpec{ExternalProviderRefs: []v1alpha1.ExternalProviderRef{{
					Ref: v1alpha1.ExternalProviderReference{Name: "provider"}, TargetModel: "gpt",
					APIFormat: "openai-chat", Path: "/v1/chat/completions",
				}}},
			}
			provider := &v1alpha1.ExternalProvider{
				ObjectMeta: metav1.ObjectMeta{Name: "provider", Namespace: "tenant-a", UID: "provider-uid"},
				Spec: v1alpha1.ExternalProviderSpec{
					Provider: "openai", Endpoint: "api.example.com",
					Auth: v1alpha1.AuthConfig{Type: "apikey", SecretRef: v1alpha1.NameReference{Name: "credentials"}},
				},
			}
			ait := controllerAITenant("tenant", "models-as-a-service", "tenant-a", "gateway", "gateway-system")
			filter := &unstructured.Unstructured{Object: map[string]any{
				"apiVersion": "networking.istio.io/v1alpha3", "kind": "EnvoyFilter",
				"metadata": map[string]any{
					"name": tenant.PayloadProcessingExternalModelEnvoyFilterName("tenant"), "namespace": "gateway-system",
					"labels": map[string]any{
						"app.kubernetes.io/managed-by": "ai-gateway-controller", externalTenantLabel: "tenant-a",
						externalTenantIDLabel: "tenant", externalGatewayNameLabel: "gateway",
						externalGatewayNamespaceLabel: "gateway-system",
					},
				},
			}}
			objects := []client.Object{deleted, sibling, provider, ait, filter}
			if status != "missing-mtc" {
				mtc := controllerMaasTenantConfig("tenant-a", "tenant", "models-as-a-service", tenant.PayloadProcessingBackendPraxis)
				annotations := mtc.GetAnnotations()
				if status == "absent" {
					delete(annotations, tenant.AnnotationPayloadProcessingStatus)
				} else {
					annotations[tenant.AnnotationPayloadProcessingStatus] = status
				}
				mtc.SetAnnotations(annotations)
				objects = append(objects, mtc)
			}
			r := controllerTestClient(t, objects...)
			r.Namespace, r.GatewayName, r.GatewayNamespace = "tenant-a", "gateway", "gateway-system"
			published := false
			r.PublishOverlay = func(context.Context, *resolver.ResolvedRouteSet, envelope.Scope, envelope.Options) (publisher.Result, error) {
				published = true
				return publisher.Result{}, nil
			}
			if _, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(deleted)}); err == nil {
				t.Fatal("deletion rebuilt sibling routes before the Praxis handoff became steady")
			}
			if published {
				t.Fatal("deletion published sibling routes before the Praxis handoff became steady")
			}
			var got v1alpha1.ExternalModel
			if err := r.Get(context.Background(), client.ObjectKeyFromObject(deleted), &got); err != nil {
				t.Fatal(err)
			}
			if !controllerutil.ContainsFinalizer(&got, externalModelFinalizer) {
				t.Fatal("deletion released finalizer before sibling serving state could be reconciled")
			}
		})
	}
}

func TestActiveProviderGatewayMoveRemovesOnlyPriorTenantCopies(t *testing.T) {
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
			Ref: v1alpha1.ExternalProviderReference{Name: "provider"}, TargetModel: "gpt",
			APIFormat: "openai-chat", Path: "/v1/chat/completions",
		}}},
	}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "credentials", Namespace: "tenant-a"}, Data: map[string][]byte{"api-key": []byte("fixture")}}
	ait := controllerAITenant("tenant", "models-as-a-service", "tenant-a", "new-gateway", "new-gateway-system")
	mtc := controllerMaasTenantConfig("tenant-a", "tenant", "models-as-a-service", tenant.PayloadProcessingBackendPraxis)
	route := resolver.Route{Model: "model", Provider: "provider", Namespace: "tenant-a", Endpoint: "api.example.com"}
	oldRule := providerDestinationRuleAtGateway(route, "old-gateway", "old-gateway-system", "tenant")
	neighborRule := providerDestinationRuleAtGateway(resolver.Route{Provider: "provider", Namespace: "tenant-b", Endpoint: "neighbor.example.com"}, "old-gateway", "old-gateway-system", "neighbor")
	oldFilter := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "networking.istio.io/v1alpha3", "kind": "EnvoyFilter",
		"metadata": map[string]any{
			"name": tenant.PayloadProcessingExternalModelEnvoyFilterName("tenant"), "namespace": "old-gateway-system",
			"labels": map[string]any{
				"app.kubernetes.io/managed-by": "ai-gateway-controller", externalTenantLabel: "tenant-a",
				externalTenantIDLabel: "tenant", externalGatewayNameLabel: "old-gateway",
				externalGatewayNamespaceLabel: "old-gateway-system",
			},
		},
	}}
	legacy := legacyExternalModelRouteFilter(tenant.PayloadProcessingExternalModelEnvoyFilterName("tenant"), "legacy-gateway-system", "ai-gateway-controller")
	neighborLegacy := legacyExternalModelRouteFilter(tenant.PayloadProcessingExternalModelEnvoyFilterName("neighbor"), "legacy-gateway-system", "ai-gateway-controller")
	foreignLegacy := legacyExternalModelRouteFilter(tenant.PayloadProcessingExternalModelEnvoyFilterName("tenant"), "foreign-gateway-system", "other-controller")
	r := controllerTestClient(t, provider, model, secret, ait, mtc, &oldRule, &neighborRule, oldFilter, legacy, neighborLegacy, foreignLegacy)
	r.Namespace, r.GatewayName, r.GatewayNamespace, r.Network = "tenant-a", "new-gateway", "new-gateway-system", "external-model"
	r.KnownClusters = []string{"provider-provider"}
	if _, err := r.ReconcileProvider(context.Background(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(provider)}); err != nil {
		t.Fatalf("reconcile provider after Gateway move: %v", err)
	}
	if _, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(model)}); err != nil {
		t.Fatal(err)
	}
	for _, object := range []client.Object{&oldRule, oldFilter, legacy} {
		got := &unstructured.Unstructured{}
		got.SetGroupVersionKind(object.GetObjectKind().GroupVersionKind())
		if err := r.Get(context.Background(), client.ObjectKeyFromObject(object), got); !apierrors.IsNotFound(err) {
			t.Fatalf("prior Gateway resource %s/%s remained: %v", object.GetNamespace(), object.GetName(), err)
		}
	}
	for _, object := range []client.Object{&neighborRule} {
		got := &unstructured.Unstructured{}
		got.SetGroupVersionKind(object.GetObjectKind().GroupVersionKind())
		if err := r.Get(context.Background(), client.ObjectKeyFromObject(object), got); err != nil {
			t.Fatalf("neighbor resource %s/%s was removed: %v", object.GetNamespace(), object.GetName(), err)
		}
	}
	for _, object := range []client.Object{neighborLegacy, foreignLegacy} {
		got := &unstructured.Unstructured{}
		got.SetGroupVersionKind(object.GetObjectKind().GroupVersionKind())
		if err := r.Get(context.Background(), client.ObjectKeyFromObject(object), got); err != nil {
			t.Fatalf("legacy neighboring/foreign filter %s/%s was removed: %v", object.GetNamespace(), object.GetName(), err)
		}
	}
	currentRule := &unstructured.Unstructured{}
	currentRule.SetGroupVersionKind(schema.GroupVersionKind{Group: "networking.istio.io", Version: "v1", Kind: "DestinationRule"})
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: "new-gateway-system", Name: "provider-provider-tenant"}, currentRule); err != nil {
		t.Fatalf("current Gateway DestinationRule missing: %v", err)
	}
	if currentRule.GetLabels()[externalGatewayNameLabel] != "new-gateway" {
		t.Fatalf("current Gateway ownership labels = %#v", currentRule.GetLabels())
	}
}

func TestAmbiguousPrePRDestinationRuleIsPreserved(t *testing.T) {
	// origin/main rendered provider-<provider> in the Gateway namespace with
	// only managed-by and external-provider labels. It has no tenant identity,
	// owner reference, or Gateway label, so automatic deletion would risk a
	// neighboring tenant. Leave it for the documented manual migration.
	legacy := unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "networking.istio.io/v1", "kind": "DestinationRule",
		"metadata": map[string]any{
			"name": "provider-provider-a", "namespace": "gateway-system",
			"labels": map[string]any{
				"app.kubernetes.io/managed-by":               "ai-gateway-controller",
				"inference.opendatahub.io/external-provider": "provider-a",
			},
		},
		"spec": map[string]any{"host": "legacy.example.com"},
	}}
	owned := providerDestinationRuleAtGateway(resolver.Route{Provider: "provider-a", Namespace: "tenant-a", Endpoint: "current.example.com"}, "gateway", "gateway-system", "tenant")
	r := controllerTestClient(t, &legacy, &owned)
	if err := r.cleanupProviderTransport(context.Background(), "tenant-a", "provider-a"); err != nil {
		t.Fatal(err)
	}
	gotLegacy := &unstructured.Unstructured{}
	gotLegacy.SetGroupVersionKind(schema.GroupVersionKind{Group: "networking.istio.io", Version: "v1", Kind: "DestinationRule"})
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(&legacy), gotLegacy); err != nil {
		t.Fatalf("ambiguous pre-PR DestinationRule was removed: %v", err)
	}
	gotOwned := &unstructured.Unstructured{}
	gotOwned.SetGroupVersionKind(schema.GroupVersionKind{Group: "networking.istio.io", Version: "v1", Kind: "DestinationRule"})
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(&owned), gotOwned); !apierrors.IsNotFound(err) {
		t.Fatalf("owned DestinationRule cleanup error = %v, want NotFound", err)
	}
}

func TestBlockedHomeTenantMigrationLeavesModelAndProviderStatusToIPP(t *testing.T) {
	r, model := reconcilerFixture(t)
	ctx := context.Background()
	mtc := tenant.NewMaasTenantConfig()
	if err := r.Get(ctx, client.ObjectKey{Namespace: "tenant-a", Name: tenant.MaasTenantConfigInstanceName}, mtc); err != nil {
		t.Fatal(err)
	}
	annotations := mtc.GetAnnotations()
	delete(annotations, tenant.AnnotationPayloadProcessingStatus)
	mtc.SetAnnotations(annotations)
	if err := r.Update(ctx, mtc); err != nil {
		t.Fatal(err)
	}
	if _, err := r.ReconcileProvider(ctx, reconcile.Request{NamespacedName: client.ObjectKey{Namespace: "tenant-a", Name: "provider"}}); err != nil {
		t.Fatalf("ReconcileProvider: %v", err)
	}
	if _, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(model)}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	var provider v1alpha1.ExternalProvider
	if err := r.Get(ctx, client.ObjectKey{Namespace: "tenant-a", Name: "provider"}, &provider); err != nil {
		t.Fatal(err)
	}
	if len(provider.Status.Conditions) > 0 || len(provider.Finalizers) > 0 {
		t.Fatalf("provider IPP still serves was changed: finalizers %v, status %#v", provider.Finalizers, provider.Status)
	}
	var got v1alpha1.ExternalModel
	if err := r.Get(ctx, client.ObjectKeyFromObject(model), &got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != "" || len(got.Status.Conditions) > 0 || len(got.Finalizers) > 0 {
		t.Fatalf("model IPP still serves was changed: finalizers %v, status %#v", got.Finalizers, got.Status)
	}
}

func TestHomeModelWithoutResolvedProviderIsNotReadyWhileSiblingsServe(t *testing.T) {
	r, model := reconcilerFixture(t)
	ctx := context.Background()
	orphan := &v1alpha1.ExternalModel{
		ObjectMeta: metav1.ObjectMeta{Name: "orphan", Namespace: "tenant-a", UID: "orphan-uid"},
		Spec: v1alpha1.ExternalModelSpec{ExternalProviderRefs: []v1alpha1.ExternalProviderRef{{
			Ref: v1alpha1.ExternalProviderReference{Name: "ghost"}, TargetModel: "gpt", APIFormat: "openai-chat", Path: "/v1/chat/completions",
		}}},
	}
	if err := r.Create(ctx, orphan); err != nil {
		t.Fatal(err)
	}
	// As if it had been served before its provider was deleted.
	orphan.Status.HTTPRouteName = modelRouteName(orphan.Name)
	if err := r.Status().Update(ctx, orphan); err != nil {
		t.Fatal(err)
	}
	for _, m := range []*v1alpha1.ExternalModel{model, orphan} {
		if _, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(m)}); err != nil {
			t.Fatalf("Reconcile %s: %v", m.Name, err)
		}
	}
	var got v1alpha1.ExternalModel
	if err := r.Get(ctx, client.ObjectKeyFromObject(orphan), &got); err != nil {
		t.Fatal(err)
	}
	ready := apiMeta.FindStatusCondition(got.Status.Conditions, conditionReady)
	if got.Status.Phase != "Failed" || ready == nil || ready.Reason != reasonProviderNotReady ||
		!strings.Contains(ready.Message, "provider ghost: external provider not found") || got.Status.HTTPRouteName != "" {
		t.Fatalf("model without a resolved provider = %#v", got.Status)
	}
	if err := r.Get(ctx, client.ObjectKeyFromObject(model), &got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != resolver.PhaseReady || got.Status.HTTPRouteName != modelRouteName(model.Name) {
		t.Fatalf("sibling model = %#v", got.Status)
	}
	for _, candidate := range overlayEnvelope(t, r, "tenant-a").Overlay.Candidates {
		if strings.Contains(candidate.StableID, "orphan") {
			t.Fatalf("overlay carries a candidate for the model without a provider: %#v", candidate)
		}
	}
}

func TestProviderRequestingPlaintextTransportFails(t *testing.T) {
	r, _ := reconcilerFixture(t)
	ctx := context.Background()
	var provider v1alpha1.ExternalProvider
	if err := r.Get(ctx, client.ObjectKey{Namespace: "tenant-a", Name: "provider"}, &provider); err != nil {
		t.Fatal(err)
	}
	provider.Annotations = map[string]string{resolver.ProviderTLSAnnotation: "false", resolver.ProviderPortAnnotation: "8080"}
	if err := r.Update(ctx, &provider); err != nil {
		t.Fatal(err)
	}
	if _, err := r.ReconcileProvider(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&provider)}); err != nil {
		t.Fatalf("ReconcileProvider: %v", err)
	}
	if err := r.Get(ctx, client.ObjectKeyFromObject(&provider), &provider); err != nil {
		t.Fatal(err)
	}
	ready := apiMeta.FindStatusCondition(provider.Status.Conditions, conditionReady)
	if provider.Status.Phase != providerPhaseFailed || ready == nil || !strings.Contains(ready.Message, "plaintext transport") {
		t.Fatalf("plaintext provider status = %#v", provider.Status)
	}
}

func TestSortModelsByName(t *testing.T) {
	models := []v1alpha1.ExternalModel{
		{ObjectMeta: metav1.ObjectMeta{Name: "c"}}, {ObjectMeta: metav1.ObjectMeta{Name: "a"}}, {ObjectMeta: metav1.ObjectMeta{Name: "b"}},
	}
	sortModelsByName(models)
	if models[0].Name != "a" || models[1].Name != "b" || models[2].Name != "c" {
		t.Fatalf("sorted models = %s, %s, %s", models[0].Name, models[1].Name, models[2].Name)
	}
}

func TestReconcileProviderClaimsOwnershipWhenMaasTenantConfigMissing(t *testing.T) {
	provider := &v1alpha1.ExternalProvider{
		ObjectMeta: metav1.ObjectMeta{Name: "provider", Namespace: "tenant-a"},
		Spec: v1alpha1.ExternalProviderSpec{
			Provider: "openai", Endpoint: "api.example.com",
			Auth: v1alpha1.AuthConfig{Type: "apikey", SecretRef: v1alpha1.NameReference{Name: "credentials"}},
		},
	}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "credentials", Namespace: "tenant-a"}, Data: map[string][]byte{"api-key": []byte("secret")}}
	r := controllerTestClient(t, provider, secret)
	r.Namespace = "tenant-a"

	if _, err := r.ReconcileProvider(context.Background(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(provider)}); err != nil {
		t.Fatalf("ReconcileProvider: %v", err)
	}
	var got v1alpha1.ExternalProvider
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(provider), &got); err != nil {
		t.Fatal(err)
	}
	if !controllerutil.ContainsFinalizer(&got, externalProviderFinalizer) {
		t.Fatal("expected ExternalProvider cleanup finalizer when MaasTenantConfig is absent (Praxis default)")
	}
	if got.Status.Phase != providerPhasePending {
		t.Fatalf("status phase = %q, want Pending while waiting for MaasTenantConfig", got.Status.Phase)
	}
	if !strings.Contains(got.Status.Conditions[0].Message, "MaasTenantConfig/default-tenant is missing") {
		t.Fatalf("status message = %q", got.Status.Conditions[0].Message)
	}
}

// A tenant claims its namespace before maas-controller creates the
// MaasTenantConfig there, so a provider in a multi-namespace deployment is
// claimed as Praxis-default rather than reported as a standalone provider.
func TestReconcileProviderClaimsOwnershipInTenantNamespaceAwaitingMaasTenantConfig(t *testing.T) {
	provider := &v1alpha1.ExternalProvider{
		ObjectMeta: metav1.ObjectMeta{Name: "provider", Namespace: "tenant-a"},
		Spec: v1alpha1.ExternalProviderSpec{
			Provider: "openai", Endpoint: "api.example.com",
			Auth: v1alpha1.AuthConfig{Type: "apikey", SecretRef: v1alpha1.NameReference{Name: "credentials"}},
		},
	}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "credentials", Namespace: "tenant-a"}, Data: map[string][]byte{"api-key": []byte("secret")}}
	ait := controllerAITenant("tenant", "models-as-a-service", "tenant-a", "gateway", "gateway-system")
	r := controllerTestClient(t, provider, secret, ait)

	if _, err := r.ReconcileProvider(context.Background(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(provider)}); err != nil {
		t.Fatalf("ReconcileProvider: %v", err)
	}
	var got v1alpha1.ExternalProvider
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(provider), &got); err != nil {
		t.Fatal(err)
	}
	if !controllerutil.ContainsFinalizer(&got, externalProviderFinalizer) {
		t.Fatal("expected ExternalProvider cleanup finalizer in a claimed tenant namespace")
	}
	if got.Status.Phase != providerPhasePending {
		t.Fatalf("status phase = %q, want Pending while waiting for MaasTenantConfig", got.Status.Phase)
	}
	if !strings.Contains(got.Status.Conditions[0].Message, "waiting for maas-controller") {
		t.Fatalf("status message = %q", got.Status.Conditions[0].Message)
	}
}

// A tenant resolution failure must not leave the provider unclaimed and
// stateless: the provider keeps our finalizer and a Pending condition while the
// reconcile error still asks for a retry.
func TestReconcileProviderClaimsAndReportsPendingWhenTenantResolutionFails(t *testing.T) {
	for name, tc := range map[string]struct {
		objects []client.Object
		reader  func(*Reconciler)
		message string
	}{
		"owning AITenant does not own the namespace": {
			objects: []client.Object{
				controllerAITenant("tenant", "models-as-a-service", "tenant-b", "gateway", "gateway-system"),
				controllerMaasTenantConfig("tenant-a", "tenant", "models-as-a-service", tenant.PayloadProcessingBackendPraxis),
			},
			message: "does not own MaasTenantConfig",
		},
		"owning AITenant is unreadable": {
			objects: []client.Object{
				controllerMaasTenantConfig("tenant-a", "tenant", "models-as-a-service", tenant.PayloadProcessingBackendPraxis),
			},
			reader:  func(r *Reconciler) { r.APIReader = failingAITenantReader{Reader: r.APIReader} },
			message: "transient AITenant read failure",
		},
	} {
		t.Run(name, func(t *testing.T) {
			provider := &v1alpha1.ExternalProvider{
				ObjectMeta: metav1.ObjectMeta{Name: "provider", Namespace: "tenant-a"},
				Spec: v1alpha1.ExternalProviderSpec{
					Provider: "openai", Endpoint: "api.example.com",
					Auth: v1alpha1.AuthConfig{Type: "apikey", SecretRef: v1alpha1.NameReference{Name: "credentials"}},
				},
			}
			secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "credentials", Namespace: "tenant-a"}, Data: map[string][]byte{"api-key": []byte("secret")}}
			r := controllerTestClient(t, append(tc.objects, provider, secret)...)
			r.Namespace = "tenant-a"
			if tc.reader != nil {
				tc.reader(r)
			}

			if _, err := r.ReconcileProvider(context.Background(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(provider)}); err == nil {
				t.Fatal("ReconcileProvider succeeded despite an unresolved tenant owner")
			}
			var got v1alpha1.ExternalProvider
			if err := r.Get(context.Background(), client.ObjectKeyFromObject(provider), &got); err != nil {
				t.Fatal(err)
			}
			if !controllerutil.ContainsFinalizer(&got, externalProviderFinalizer) {
				t.Fatal("provider was left unclaimed after a tenant resolution failure")
			}
			if got.Status.Phase != providerPhasePending {
				t.Fatalf("status phase = %q, want Pending", got.Status.Phase)
			}
			if !hasConditionReason(got.Status.Conditions, conditionReady, reasonTenantNotReady) {
				t.Fatalf("provider conditions = %#v, want Ready with reason %q", got.Status.Conditions, reasonTenantNotReady)
			}
			if !strings.Contains(got.Status.Conditions[0].Message, tc.message) {
				t.Fatalf("status message = %q, want it to explain the failure", got.Status.Conditions[0].Message)
			}
		})
	}
}

type failingAITenantReader struct {
	client.Reader
}

func (r failingAITenantReader) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if u, ok := obj.(*unstructured.Unstructured); ok && u.GroupVersionKind() == tenant.AITenantGVK {
		return errors.New("transient AITenant read failure")
	}
	return r.Reader.Get(ctx, key, obj, opts...)
}

func TestProviderTenantReadinessMessageAbsentMaasTenantConfig(t *testing.T) {
	msg := providerTenantReadinessMessage(tenantResolution{missingConfig: true, selected: true})
	if msg == "" {
		t.Fatal("expected pending message while MaasTenantConfig is absent")
	}
	if !strings.Contains(msg, "missing") {
		t.Fatalf("message = %q", msg)
	}
}

func TestAllZeroScopedModelWithdrawalPreservesSameAliasSiblingLastKnownGood(t *testing.T) {
	ctx := context.Background()
	r, model, sibling, providerB := reconcilerWithSiblingProvider(t)
	r.Namespace = ""
	shared, secret := attachmentProvider("shared")
	r.KnownClusters = append(r.KnownClusters, (resolver.Route{Namespace: model.Namespace, ProviderNamespace: shared.Namespace, Provider: shared.Name}).ServiceName())
	grant := referenceGrant("shared", model.Namespace, shared.Name)
	for _, obj := range []client.Object{shared, secret, grant} {
		if err := r.Create(ctx, obj); err != nil {
			t.Fatal(err)
		}
	}
	model.Spec.ModelName = "shared-client-alias"
	model.Spec.ExternalProviderRefs = []v1alpha1.ExternalProviderRef{{
		Ref:         v1alpha1.ExternalProviderReference{Name: shared.Name, Namespace: shared.Namespace},
		TargetModel: "gpt", APIFormat: "openai-chat", Path: "/v1/chat/completions",
	}}
	sibling.Spec.ModelName = model.Spec.ModelName
	for _, m := range []*v1alpha1.ExternalModel{model, sibling} {
		if err := r.Update(ctx, m); err != nil {
			t.Fatal(err)
		}
	}
	for _, m := range []*v1alpha1.ExternalModel{model, sibling} {
		reconcileModel(t, r, m)
	}
	expectedScoped := envelope.ScopedStableID(model.Namespace, model.Name, shared.Namespace, shared.Name)
	before := overlayEnvelope(t, r, model.Namespace)
	found := false
	for _, candidate := range before.Overlay.Candidates {
		found = found || candidate.StableID == expectedScoped
	}
	if !found || len(before.Overlay.Candidates) != 2 {
		t.Fatalf("initial overlay must include scoped model and same-alias sibling: %#v", before.Overlay.Candidates)
	}
	if err := r.Get(ctx, client.ObjectKeyFromObject(providerB), providerB); err != nil {
		t.Fatal(err)
	}
	providerB.Status.Phase = "Pending"
	providerB.Status.Conditions = []metav1.Condition{{Type: conditionReady, Status: metav1.ConditionFalse, Reason: "Reconciling", Message: "temporary failure", LastTransitionTime: metav1.Now()}}
	if err := r.Status().Update(ctx, providerB); err != nil {
		t.Fatal(err)
	}
	var current v1alpha1.ExternalModel
	if err := r.Get(ctx, client.ObjectKeyFromObject(model), &current); err != nil {
		t.Fatal(err)
	}
	zero := 0
	current.Spec.ExternalProviderRefs[0].Weight = &zero
	if err := r.Update(ctx, &current); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(model)}); !errors.Is(err, resolver.ErrNoRoutes) {
		t.Fatalf("withdrawal error=%v, want ErrNoRoutes", err)
	}
	after := overlayEnvelope(t, r, model.Namespace)
	if len(after.Overlay.Candidates) != 1 || after.Overlay.Candidates[0].StableID != envelope.CandidateStableID(sibling.Name, providerB.Name) {
		t.Fatalf("expected only sibling last-known candidate: %#v", after.Overlay.Candidates)
	}
	if _, err := getUnstructured(r, httpRouteGVK, model.Namespace, modelRouteName(model.Name)); !apierrors.IsNotFound(err) {
		t.Fatalf("disabled route remains: %v", err)
	}
	if _, err := getUnstructured(r, httpRouteGVK, sibling.Namespace, modelRouteName(sibling.Name)); err != nil {
		t.Fatalf("sibling route removed: %v", err)
	}
	if err := r.Get(ctx, client.ObjectKeyFromObject(model), &current); err != nil {
		t.Fatal(err)
	}
	if current.Status.Phase != "Failed" || current.Status.ObservedGeneration != current.Generation || !hasConditionReason(current.Status.Conditions, conditionReady, reasonNoRoutes) {
		t.Fatalf("withdrawal status=%#v", current.Status)
	}
}

func TestExcludedScopedCandidateIDsSupportHashedIdentitiesAndFailOnUnreadableModel(t *testing.T) {
	ctx := context.Background()
	model := &v1alpha1.ExternalModel{
		ObjectMeta: metav1.ObjectMeta{Namespace: "tenant-a", Name: strings.Repeat("m", 220)},
		Spec: v1alpha1.ExternalModelSpec{ModelName: "alias", ExternalProviderRefs: []v1alpha1.ExternalProviderRef{{
			Ref: v1alpha1.ExternalProviderReference{Namespace: "shared", Name: strings.Repeat("p", 60)},
		}}},
	}
	r := controllerTestClient(t, model)
	excluded := map[string]bool{model.Namespace + "/" + model.Name: true}
	ids, err := r.excludedScopedCandidateIDs(ctx, model.Namespace, excluded)
	if err != nil {
		t.Fatal(err)
	}
	expected := envelope.ScopedStableID(model.Namespace, model.Name, "shared", strings.Repeat("p", 60))
	if !strings.HasPrefix(expected, "sha256:") || len(ids) != 1 || !ids[expected] {
		t.Fatalf("hashed candidate identity was not preserved: %#v", ids)
	}
	excluded[model.Namespace+"/missing"] = true
	if _, err := r.excludedScopedCandidateIDs(ctx, model.Namespace, excluded); !apierrors.IsNotFound(err) {
		t.Fatalf("unreadable excluded model must fail closed: %v", err)
	}
}

func TestHomeModelPublicationReportsOneStatus(t *testing.T) {
	for _, tc := range []struct {
		name        string
		publication homeModelPublication
		wantReason  string
	}{
		{name: "weight disabled takes precedence", publication: homeModelPublication{weightDisabled: true, denied: "denied", unserved: "unready", unroutedReason: reasonNoRoutes}, wantReason: reasonNoRoutes},
		{name: "denied reference", publication: homeModelPublication{denied: "denied", unserved: "unready", unroutedReason: reasonNoRoutes}, wantReason: reasonRefNotPermitted},
		{name: "unready credential", publication: homeModelPublication{unserved: "unready", unroutedReason: reasonNoRoutes}, wantReason: reasonProviderNotReady},
		{name: "unrouted", publication: homeModelPublication{unroutedReason: reasonNoRoutes}, wantReason: reasonNoRoutes},
		{name: "ready", wantReason: reasonReconciled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, reportErr := range []error{nil, errors.New("status write failed")} {
				calls := 0
				publication := tc.publication
				publication.report = func(ready bool, reason, _ string, _ *envelope.Envelope) error {
					calls++
					if reason != tc.wantReason || ready != (reason == reasonReconciled) {
						t.Errorf("status ready=%v reason=%s; want %s", ready, reason, tc.wantReason)
					}
					return reportErr
				}
				_, err := (&Reconciler{}).finishHomeModelPublication(context.Background(), publication)
				if calls != 1 || !errors.Is(err, reportErr) {
					t.Fatalf("status calls=%d err=%v; want one call and %v", calls, err, reportErr)
				}
			}
		})
	}
}
