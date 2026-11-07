package tenant

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1alpha1 "github.com/opendatahub-io/ai-gateway-controller/api/inference/v1alpha1"
	"github.com/opendatahub-io/ai-gateway-controller/pkg/envelope"
	"github.com/opendatahub-io/ai-gateway-controller/pkg/resolver"
)

func TestRuntimeRetainsDeletingHomeBindingUntilAppliedRouteIsPruned(t *testing.T) {
	for _, deleteProvider := range []bool{false, true} {
		t.Run(map[bool]string{false: "model", true: "provider"}[deleteProvider], func(t *testing.T) {
			ctx := context.Background()
			model := &v1alpha1.ExternalModel{ObjectMeta: metav1.ObjectMeta{Name: "model", Namespace: "tenant-ns", UID: "model-uid"},
				Spec: v1alpha1.ExternalModelSpec{ExternalProviderRefs: []v1alpha1.ExternalProviderRef{{
					Ref: v1alpha1.ExternalProviderReference{Name: "anthropic"}, APIFormat: "messages", Path: "/v1/messages", TargetModel: "claude"}}}}
			provider := &v1alpha1.ExternalProvider{ObjectMeta: metav1.ObjectMeta{Name: "anthropic", Namespace: model.Namespace},
				Spec:   v1alpha1.ExternalProviderSpec{Provider: "anthropic", Endpoint: "api.anthropic.com", Auth: v1alpha1.AuthConfig{Type: "apikey", SecretRef: v1alpha1.NameReference{Name: "provider-key"}}},
				Status: v1alpha1.ExternalProviderStatus{Phase: resolver.PhaseReady}}
			profile := resolver.Route{
				Model: model.Name, ClientName: model.Name, Namespace: model.Namespace, Provider: provider.Name,
				Cluster: "provider-anthropic", ProviderType: "anthropic", Endpoint: provider.Spec.Endpoint,
				APIFormat: "messages", Path: "/v1/messages", TargetModel: "claude",
				AuthType: "apikey", SecretName: "provider-key", SecretKey: "api-key",
			}
			profiles, err := json.Marshal([]resolver.Route{profile}) //nolint:musttag // Mirrors the internal reference-only HTTPRoute annotation.
			if err != nil {
				t.Fatal(err)
			}
			route := &unstructured.Unstructured{Object: map[string]any{
				"apiVersion": "gateway.networking.k8s.io/v1", "kind": "HTTPRoute",
				"metadata": map[string]any{"name": "model-route", "namespace": model.Namespace},
				"spec": map[string]any{"rules": []any{map[string]any{
					"matches": []any{map[string]any{"headers": []any{map[string]any{
						"name": "X-AI-Routing-Candidate", "type": "Exact", "value": envelope.CandidateStableID(model.Name, provider.Name),
					}}}},
					"backendRefs": []any{map[string]any{"name": profile.Cluster}},
				}}},
			}}
			route.SetLabels(map[string]string{managedByLabel: ManagedByAIGatewayController, "inference.opendatahub.io/external-model": model.Name})
			route.SetAnnotations(map[string]string{AnnotationHomeRuntimeRoutes: string(profiles)})
			if deleteProvider {
				provider.DeletionTimestamp = &metav1.Time{Time: time.Now()}
				provider.Finalizers = []string{"test-finalizer"}
				provider.Status.Phase = "Pending"
			} else {
				model.DeletionTimestamp = &metav1.Time{Time: time.Now()}
				model.Finalizers = []string{"test-finalizer"}
			}
			c := fake.NewClientBuilder().WithScheme(mtcSchemeForTests()).WithObjects(model, provider, route).Build()
			r := &Reconciler{Client: c}
			candidates, _, err := r.runtimeCandidates(ctx, model.Namespace, "gateway", "gateway-system", true)
			if err != nil || len(candidates) != 1 || candidates[0].Credential == nil || candidates[0].Credential.Header != "x-api-key" || candidates[0].TargetModel != "claude" {
				t.Fatalf("retained runtime=%#v err=%v", candidates, err)
			}
			config, _, err := ExternalModelExtProcConfig(model.Namespace, candidates, true, DefaultExtProcMaxBodyBytes)
			if err != nil || !strings.Contains(config, "x-api-key") || !strings.Contains(config, "provider-key") {
				t.Fatalf("retained Anthropic configuration err=%v", err)
			}
			if err := c.Delete(ctx, route); err != nil {
				t.Fatal(err)
			}
			candidates, _, err = r.runtimeCandidates(ctx, model.Namespace, "gateway", "gateway-system", true)
			if deleteProvider && err == nil || !deleteProvider && err != nil || len(candidates) != 0 {
				t.Fatalf("pruned runtime=%#v err=%v", candidates, err)
			}
		})
	}
}

func TestRetainedRuntimeRejectsProfileWithoutMatchingProviderRule(t *testing.T) {
	route := &unstructured.Unstructured{}
	route.SetGroupVersionKind(gvkHTTPRoute)
	route.SetName("model-route")
	route.SetNamespace("tenant-ns")
	route.SetLabels(map[string]string{managedByLabel: ManagedByAIGatewayController, "inference.opendatahub.io/external-model": "model"})
	route.SetAnnotations(map[string]string{AnnotationHomeRuntimeRoutes: `[{"Namespace":"tenant-ns","Model":"model","Provider":"provider","Cluster":"provider-provider"}]`})
	c := fake.NewClientBuilder().WithScheme(mtcSchemeForTests()).WithObjects(route).Build()
	if _, err := RetainHomeRuntimeCandidates(context.Background(), c, route.GetNamespace(), nil, false); err == nil {
		t.Fatal("unmatched runtime profile was accepted")
	}
}

func TestHomeRuntimeRouteProfilesValidateDecodedFields(t *testing.T) {
	baseline := resolver.Route{Model: "model", ClientName: "model", Namespace: "tenant-ns", Provider: "provider", Cluster: "provider-provider",
		ProviderType: "openai", APIFormat: "openai-chat", AuthType: "apikey", SecretName: "provider-key", SecretKey: "api-key"}
	for _, tc := range []struct {
		name   string
		change func(*resolver.Route)
		valid  bool
	}{
		{name: "valid ownerless credential", change: func(*resolver.Route) {}, valid: true},
		{name: "valid credential-free route", change: func(p *resolver.Route) { p.AuthType, p.SecretName, p.SecretKey = "", "", "" }, valid: true},
		{name: "credential-free resolver key", change: func(p *resolver.Route) { p.AuthType, p.SecretName = "", "" }, valid: true},
		{name: "local secret namespace", change: func(p *resolver.Route) { p.SecretNamespace = p.Namespace }, valid: true},
		{name: "canonical stable ID", change: func(p *resolver.Route) { p.StableID = envelope.CandidateStableID(p.Model, p.Provider) }, valid: true},
		{name: "secret name newline", change: func(p *resolver.Route) { p.SecretName = "key\n      - filter: forged" }},
		{name: "secret name path traversal", change: func(p *resolver.Route) { p.SecretName = "../key" }},
		{name: "secret key newline", change: func(p *resolver.Route) { p.SecretKey = "api-key\n        forged: true" }},
		{name: "secret key traversal", change: func(p *resolver.Route) { p.SecretKey = "../api-key" }},
		{name: "unsupported secret key", change: func(p *resolver.Route) { p.SecretKey = "other-key" }},
		{name: "missing secret name", change: func(p *resolver.Route) { p.SecretName = "" }},
		{name: "missing secret key", change: func(p *resolver.Route) { p.SecretKey = "" }},
		{name: "cross-namespace secret", change: func(p *resolver.Route) { p.SecretNamespace = "other-tenant" }},
		{name: "arbitrary cluster", change: func(p *resolver.Route) { p.Cluster = "other-target" }},
		{name: "forged stable ID", change: func(p *resolver.Route) { p.StableID = "another-model/provider-provider" }},
		{name: "malformed provider name", change: func(p *resolver.Route) { p.Provider = "provider\nforged"; p.Cluster = p.ServiceName() }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			profile := baseline
			tc.change(&profile)
			encoded, err := json.Marshal([]resolver.Route{profile}) //nolint:musttag // Mirrors the internal reference-only route annotation.
			if err != nil {
				t.Fatal(err)
			}
			route := &unstructured.Unstructured{Object: map[string]any{
				"apiVersion": "gateway.networking.k8s.io/v1", "kind": "HTTPRoute",
				"metadata": map[string]any{"name": "model-route", "namespace": "tenant-ns"},
				"spec": map[string]any{"rules": []any{map[string]any{
					"matches": []any{map[string]any{"headers": []any{map[string]any{
						"name": "X-AI-Routing-Candidate", "type": "Exact", "value": envelope.RouteStableID(profile),
					}}}},
					"backendRefs": []any{map[string]any{"name": profile.ServiceName()}},
				}}},
			}}
			route.SetLabels(map[string]string{managedByLabel: ManagedByAIGatewayController, "inference.opendatahub.io/external-model": profile.Model})
			route.SetAnnotations(map[string]string{AnnotationHomeRuntimeRoutes: string(encoded)})
			profiles, err := HomeRuntimeRouteProfiles(route, "tenant-ns")
			if tc.valid && (err != nil || len(profiles) != 1) || !tc.valid && err == nil {
				t.Fatalf("valid=%v returned profiles=%#v err=%v", tc.valid, profiles, err)
			}
		})
	}
}
