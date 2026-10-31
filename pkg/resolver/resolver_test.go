package resolver

import (
	"errors"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1alpha1 "github.com/opendatahub-io/ai-gateway-controller/api/inference/v1alpha1"
)

func provider(ns, name, phase, endpoint string, cfg map[string]string) *v1alpha1.ExternalProvider {
	return &v1alpha1.ExternalProvider{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Spec: v1alpha1.ExternalProviderSpec{
			Provider: "openai",
			Endpoint: endpoint,
			Auth: v1alpha1.AuthConfig{
				Type:      "apikey",
				SecretRef: v1alpha1.NameReference{Name: name + "-key"},
			},
			Config: cfg,
		},
		Status: v1alpha1.ExternalProviderStatus{Phase: phase},
	}
}

func model(ns, name string, refs ...v1alpha1.ExternalProviderRef) *v1alpha1.ExternalModel {
	return &v1alpha1.ExternalModel{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Spec:       v1alpha1.ExternalModelSpec{ExternalProviderRefs: refs},
	}
}

func ref(name, target, path string) v1alpha1.ExternalProviderRef {
	return v1alpha1.ExternalProviderRef{
		Ref:         v1alpha1.ExternalProviderReference{Name: name},
		TargetModel: target,
		APIFormat:   "openai-chat",
		Path:        path,
	}
}

func intptr(v int) *int { return &v }

func TestResolve_SingleHappyPath(t *testing.T) {
	m := model("ns1", "my-model", ref("prov-a", "gpt-4o", "/v1/chat/completions"))
	p := provider("ns1", "prov-a", PhaseReady, "api.openai.com", nil)

	set, err := Resolve([]*v1alpha1.ExternalModel{m}, []*v1alpha1.ExternalProvider{p})
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	rs := set.Routes()
	if len(rs) != 1 {
		t.Fatalf("want 1 route, got %d", len(rs))
	}
	r := rs[0]
	if r.Model != "my-model" || r.ClientName != "my-model" {
		t.Errorf("identity: model=%q client=%q", r.Model, r.ClientName)
	}
	if r.Cluster != "provider-prov-a" {
		t.Errorf("cluster convention: got %q, want provider-prov-a", r.Cluster)
	}
	if r.Weight != 1 {
		t.Errorf("weight default: got %d, want 1", r.Weight)
	}
	if r.Path != "/v1/chat/completions" {
		t.Errorf("path: got %q", r.Path)
	}
	if r.SecretName != "prov-a-key" || r.SecretKey != "api-key" {
		t.Errorf("credential refs: %+v", r)
	}
	if r.AuthType != "apikey" {
		t.Errorf("auth type: got %q", r.AuthType)
	}
}

func TestResolve_UsesAnnotatedProviderPort(t *testing.T) {
	m := model("ns1", "my-model", ref("prov-a", "gpt-4o", "/v1/chat/completions"))
	p := provider("ns1", "prov-a", PhaseReady, "provider.example.com", nil)
	p.Annotations = map[string]string{ProviderPortAnnotation: "8000"}

	set, err := Resolve([]*v1alpha1.ExternalModel{m}, []*v1alpha1.ExternalProvider{p})
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if got := set.Routes()[0].Port; got != 8000 {
		t.Fatalf("resolved port = %d, want 8000", got)
	}
}

func TestProviderPortDefaultsAndRejectsInvalidAnnotation(t *testing.T) {
	p := provider("ns1", "prov-a", PhaseReady, "provider.example.com", nil)
	if got, err := ProviderPort(p); err != nil || got != 443 {
		t.Fatalf("ProviderPort() = (%d, %v), want (443, nil)", got, err)
	}
	p.Annotations = map[string]string{ProviderPortAnnotation: "65536"}
	if _, err := ProviderPort(p); err == nil {
		t.Fatal("ProviderPort() succeeded for invalid port annotation")
	}
}

func TestResolveRejectsDuplicateActiveProviderBindings(t *testing.T) {
	m := model("ns1", "my-model",
		ref("prov-a", "gpt-4o-mini", "/v1/chat/completions"),
		ref("prov-a", "gpt-4o", "/v1/responses"),
	)
	p := provider("ns1", "prov-a", PhaseReady, "api.openai.com", nil)

	_, err := Resolve([]*v1alpha1.ExternalModel{m}, []*v1alpha1.ExternalProvider{p})
	if err == nil || !strings.Contains(err.Error(), "multiple active references") {
		t.Fatalf("Resolve() error = %v, want duplicate active provider rejection", err)
	}
}

func TestResolveGatesAnthropicMessagesOnTenantPermission(t *testing.T) {
	modelRef := ref("prov-anthropic", "claude", "/v1/messages")
	modelRef.APIFormat = "messages"
	model := model("ns1", "my-model", modelRef)
	provider := provider("ns1", "prov-anthropic", PhaseReady, "api.anthropic.com", nil)
	provider.Spec.Provider = "anthropic"
	set, err := Resolve(
		[]*v1alpha1.ExternalModel{model}, []*v1alpha1.ExternalProvider{provider},
	)
	if !errors.Is(err, ErrNoRoutes) || len(set.Models) != 1 || len(set.Models[0].Routes) != 0 || len(set.Models[0].Skips) != 1 || set.Models[0].Skips[0].Reason != SkipTranslationDisabled {
		t.Fatalf("Resolve() = (%+v, %v), want no route with APITranslationDisabled skip", set, err)
	}
	enabled, err := ResolveWithAPITranslation([]*v1alpha1.ExternalModel{model}, []*v1alpha1.ExternalProvider{provider}, true)
	if err != nil || len(enabled.Routes()) != 1 || enabled.Routes()[0].APIFormat != "messages" {
		t.Fatalf("ResolveWithAPITranslation() = (%+v, %v), want one Messages candidate", enabled, err)
	}
}

func TestAPITranslationPermissionPreservesOpenAIAndGatesAnthropicSelection(t *testing.T) {
	openAIRef := ref("openai", "gpt-mock", "/v1/chat/completions")
	messageRef := ref("anthropic", "claude-mock", "/v1/messages")
	messageRef.APIFormat = "messages"
	model := model("ns1", "model", openAIRef, messageRef)
	providers := []*v1alpha1.ExternalProvider{
		provider("ns1", "openai", PhaseReady, "openai.example.com", nil),
		provider("ns1", "anthropic", PhaseReady, "anthropic.example.com", nil),
	}
	providers[1].Spec.Provider = "anthropic"

	defaultOff, err := ResolveWithAPITranslation([]*v1alpha1.ExternalModel{model}, providers, false)
	if err != nil || len(defaultOff.Routes()) != 1 || defaultOff.Routes()[0].Provider != "openai" {
		t.Fatalf("default-off routes = %+v, err=%v; want OpenAI pass-through only", defaultOff, err)
	}
	if len(defaultOff.Models[0].Skips) != 1 || defaultOff.Models[0].Skips[0].Reason != SkipTranslationDisabled {
		t.Fatalf("default-off skips = %+v, want the Messages candidate gated by permission", defaultOff.Models[0].Skips)
	}

	enabled, err := ResolveWithAPITranslation([]*v1alpha1.ExternalModel{model}, providers, true)
	if err != nil || len(enabled.Routes()) != 2 {
		t.Fatalf("enabled routes = %+v, err=%v; want OpenAI and Anthropic candidates", enabled, err)
	}
	if enabled.Routes()[0].Provider != "openai" || enabled.Routes()[1].Provider != "anthropic" {
		t.Fatalf("enabled route order = %+v; permission must not choose a translator", enabled.Routes())
	}
}

func TestResolveRejectsUnqualifiedActiveProviderBindings(t *testing.T) {
	cases := []struct {
		name         string
		providerType string
		apiFormat    string
	}{
		{name: "Vertex OpenAI", providerType: "vertex-openai", apiFormat: "openai-chat"},
		{name: "Bedrock OpenAI", providerType: "bedrock-openai", apiFormat: "openai-chat"},
		{name: "custom provider", providerType: "custom", apiFormat: "openai-chat"},
		{name: "custom format", providerType: "openai", apiFormat: "vendor-chat"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			modelRef := ref("provider", "target", "/v1/chat/completions")
			modelRef.APIFormat = tc.apiFormat
			model := model("ns1", "model", modelRef)
			provider := provider("ns1", "provider", PhaseReady, "provider.example.com", nil)
			provider.Spec.Provider = tc.providerType

			_, err := ResolveWithAPITranslation([]*v1alpha1.ExternalModel{model}, []*v1alpha1.ExternalProvider{provider}, true)
			if !errors.Is(err, ErrProviderCapabilityUnsupported) {
				t.Fatalf("ResolveWithAPITranslation() error = %v, want ErrProviderCapabilityUnsupported", err)
			}
			for _, want := range []string{tc.providerType, tc.apiFormat, "provider ref provider"} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("error %q does not identify %q", err, want)
				}
			}
		})
	}
}

func TestUnsupportedMessagesBindingFailsClosedBeforeTranslationPermission(t *testing.T) {
	cases := []struct {
		name         string
		providerType string
	}{
		{name: "Azure OpenAI", providerType: "azure-openai"},
		{name: "Vertex OpenAI", providerType: "vertex-openai"},
		{name: "Bedrock OpenAI", providerType: "bedrock-openai"},
		{name: "custom provider", providerType: "custom"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			modelRef := ref("provider", "target", "/v1/messages")
			modelRef.APIFormat = "messages"
			model := model("ns1", "model", modelRef)
			provider := provider("ns1", "provider", PhaseReady, "provider.example.com", nil)
			provider.Spec.Provider = tc.providerType

			set, err := ResolveWithAPITranslation([]*v1alpha1.ExternalModel{model}, []*v1alpha1.ExternalProvider{provider}, false)
			if !errors.Is(err, ErrProviderCapabilityUnsupported) {
				t.Fatalf("ResolveWithAPITranslation(default-off) error = %v, want ErrProviderCapabilityUnsupported", err)
			}
			if len(set.Models) != 1 || len(set.Models[0].Routes) != 0 || len(set.Models[0].Skips) != 1 || set.Models[0].Skips[0].Reason != SkipAPIFormatUnsupported {
				t.Fatalf("ResolveWithAPITranslation(default-off) result = %+v, want APIFormatUnsupported diagnostic without a publishable route", set)
			}
			if !strings.Contains(err.Error(), string(SkipAPIFormatUnsupported)) {
				t.Fatalf("ResolveWithAPITranslation(default-off) error = %v, want reported %s reason", err, SkipAPIFormatUnsupported)
			}
		})
	}
}

func TestResolveAllReportsUnsupportedMessagesSeparatelyFromTranslationDisabled(t *testing.T) {
	modelRef := withWeight(ref("provider", "target", "/v1/messages"), 0)
	modelRef.APIFormat = "messages"
	model := model("ns1", "model", modelRef)
	provider := provider("ns1", "provider", PhaseReady, "provider.example.com", nil)
	provider.Spec.Provider = "custom"

	set, err := ResolveAllWithAPITranslation([]*v1alpha1.ExternalModel{model}, []*v1alpha1.ExternalProvider{provider}, false)
	if !errors.Is(err, ErrNoRoutes) || len(set.Models) != 1 || len(set.Models[0].Routes) != 0 || len(set.Models[0].Skips) != 1 {
		t.Fatalf("ResolveAllWithAPITranslation(default-off) = (%+v, %v), want one skipped unsupported candidate", set, err)
	}
	if got := set.Models[0].Skips[0].Reason; got != SkipAPIFormatUnsupported {
		t.Fatalf("unsupported Messages candidate skip reason = %q, want %q", got, SkipAPIFormatUnsupported)
	}
}

func TestResolveAllSkipsUnqualifiedDisabledBinding(t *testing.T) {
	active := ref("active", "target-a", "/v1/chat/completions")
	disabled := withWeight(ref("vertex", "target-b", "/v1/chat/completions"), 0)
	disabled.APIFormat = "openai-chat"
	model := model("ns1", "model", active, disabled)
	providers := []*v1alpha1.ExternalProvider{
		provider("ns1", "active", PhaseReady, "openai.example.com", nil),
		provider("ns1", "vertex", PhaseReady, "vertex.example.com", nil),
	}
	providers[1].Spec.Provider = "vertex-openai"

	set, err := ResolveAllWithAPITranslation([]*v1alpha1.ExternalModel{model}, providers, true)
	if err != nil {
		t.Fatalf("ResolveAllWithAPITranslation() error = %v, want inactive unqualified binding to be omitted", err)
	}
	if got := len(set.Routes()); got != 1 || set.Routes()[0].Provider != "active" {
		t.Fatalf("preloaded routes = %+v, want only active qualified provider", set.Routes())
	}
	if len(set.Models[0].Skips) != 1 || set.Models[0].Skips[0].Reason != SkipAPIFormatUnsupported {
		t.Fatalf("inactive unqualified binding skips = %+v, want APIFormatUnsupported", set.Models[0].Skips)
	}
}

func TestResolveAllSkipsNegativeWeightUnsupportedBinding(t *testing.T) {
	active := ref("active", "target-a", "/v1/chat/completions")
	disabled := withWeight(ref("vertex", "target-b", "/v1/chat/completions"), -1)
	model := model("ns1", "model", active, disabled)
	providers := []*v1alpha1.ExternalProvider{
		provider("ns1", "active", PhaseReady, "openai.example.com", nil),
		provider("ns1", "vertex", PhaseReady, "vertex.example.com", nil),
	}
	providers[1].Spec.Provider = "vertex-openai"

	set, err := ResolveAllWithAPITranslation([]*v1alpha1.ExternalModel{model}, providers, true)
	if err != nil {
		t.Fatalf("ResolveAllWithAPITranslation() error = %v, want negative-weight unsupported binding treated as disabled", err)
	}
	if got := len(set.Routes()); got != 1 || set.Routes()[0].Provider != "active" {
		t.Fatalf("preloaded routes = %+v, want only the active qualified provider", set.Routes())
	}
	if len(set.Models[0].Skips) != 1 || set.Models[0].Skips[0].Reason != SkipAPIFormatUnsupported {
		t.Fatalf("negative-weight unsupported binding skips = %+v, want APIFormatUnsupported", set.Models[0].Skips)
	}
}

func TestResolveAllDoesNotPreloadMessagesWhenTenantPermissionIsOff(t *testing.T) {
	modelRef := withWeight(ref("prov-anthropic", "claude", "/v1/messages"), 0)
	modelRef.APIFormat = "messages"
	model := model("ns1", "my-model", modelRef)
	provider := provider("ns1", "prov-anthropic", PhaseReady, "api.anthropic.com", nil)
	provider.Spec.Provider = "anthropic"

	set, err := ResolveAllWithAPITranslation([]*v1alpha1.ExternalModel{model}, []*v1alpha1.ExternalProvider{provider}, false)
	if !errors.Is(err, ErrNoRoutes) || len(set.Models) != 1 || len(set.Models[0].Routes) != 0 || len(set.Models[0].Skips) != 1 {
		t.Fatalf("ResolveAllWithAPITranslation() = (%+v, %v), want no preloaded Messages candidate", set, err)
	}
	if set.Models[0].Skips[0].Reason != SkipTranslationDisabled {
		t.Fatalf("Messages candidate skip reason = %q, want %q", set.Models[0].Skips[0].Reason, SkipTranslationDisabled)
	}
}

func TestResolveAllIncludesUnselectedProviderBindings(t *testing.T) {
	m := model("ns1", "my-model",
		withWeight(ref("prov-a", "a", "/v1"), 1),
		withWeight(ref("prov-b", "b", "/v1"), 0),
	)
	provs := []*v1alpha1.ExternalProvider{
		provider("ns1", "prov-a", PhaseReady, "a.example.com", nil),
		provider("ns1", "prov-b", PhaseReady, "b.example.com", nil),
	}
	set, err := ResolveAll([]*v1alpha1.ExternalModel{m}, provs)
	if err != nil {
		t.Fatalf("ResolveAll: %v", err)
	}
	if got := len(set.Routes()); got != 2 {
		t.Fatalf("ResolveAll routes = %d, want 2", got)
	}
	if set.Routes()[1].Provider != "prov-b" || set.Routes()[1].Weight != 0 {
		t.Fatalf("ResolveAll did not preserve the unselected binding: %+v", set.Routes()[1])
	}

	selected, err := Resolve([]*v1alpha1.ExternalModel{m}, provs)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if got := len(selected.Routes()); got != 1 || selected.Routes()[0].Provider != "prov-a" {
		t.Fatalf("Resolve serving routes = %+v, want only prov-a", selected.Routes())
	}
}

func TestResolve_ModelNameAlias(t *testing.T) {
	m := model("ns1", "my-model", ref("prov-a", "gpt-4o", "/v1"))
	m.Spec.ModelName = "gpt-4o.prod"
	p := provider("ns1", "prov-a", PhaseReady, "api.openai.com", nil)

	set, err := Resolve([]*v1alpha1.ExternalModel{m}, []*v1alpha1.ExternalProvider{p})
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	r := set.Routes()[0]
	if r.Model != "my-model" {
		t.Errorf("candidate identity must stay CR name (#425): got %q", r.Model)
	}
	if r.ClientName != "gpt-4o.prod" {
		t.Errorf("client name: got %q, want gpt-4o.prod", r.ClientName)
	}
}

func TestResolve_SkipReasons(t *testing.T) {
	pReady := provider("ns1", "ready", PhaseReady, "a.example.com", nil)
	pPend := provider("ns1", "pending", "Pending", "b.example.com", nil)
	pZero := provider("ns1", "zero", PhaseReady, "c.example.com", nil)

	cases := []struct {
		name   string
		ref    v1alpha1.ExternalProviderRef
		provs  []*v1alpha1.ExternalProvider
		reason SkipReason
	}{
		{
			name:   "missing provider",
			ref:    ref("ghost", "t", "/v1"),
			provs:  []*v1alpha1.ExternalProvider{pReady},
			reason: SkipRefNotFound,
		},
		{
			name:   "provider not ready",
			ref:    ref("pending", "t", "/v1"),
			provs:  []*v1alpha1.ExternalProvider{pPend},
			reason: SkipProviderNotReady,
		},
		{
			name:   "weight zero disables",
			ref:    withWeight(ref("zero", "t", "/v1"), 0),
			provs:  []*v1alpha1.ExternalProvider{pZero},
			reason: SkipWeightDisabled,
		},
		{
			name:   "negative weight disables",
			ref:    withWeight(ref("zero", "t", "/v1"), -3),
			provs:  []*v1alpha1.ExternalProvider{pZero},
			reason: SkipWeightDisabled,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			set, err := Resolve(
				[]*v1alpha1.ExternalModel{model("ns1", "m", tc.ref)}, tc.provs)
			if !errors.Is(err, ErrNoRoutes) {
				t.Fatalf("all-skipped must return ErrNoRoutes, got %v", err)
			}
			if len(set.Models) != 1 || len(set.Models[0].Skips) != 1 {
				t.Fatalf("want exactly one skip, got %+v", set.Models)
			}
			if set.Models[0].Skips[0].Reason != tc.reason {
				t.Errorf("reason: got %q, want %q", set.Models[0].Skips[0].Reason, tc.reason)
			}
			if set.Models[0].Skips[0].ModelRef != "ns1/m" {
				t.Errorf("skip must carry model ref: %q", set.Models[0].Skips[0].ModelRef)
			}
		})
	}
}

func TestResolve_SkipAggregationKeepsGoodRefs(t *testing.T) {
	// IPP parity: bad refs are skipped, reconciliation fails only at zero.
	m := model("ns1", "m",
		ref("ghost", "t1", "/v1"),
		ref("good", "t2", "/v1"),
		withWeight(ref("good2", "t3", "/v1"), 0),
	)
	provs := []*v1alpha1.ExternalProvider{
		provider("ns1", "good", PhaseReady, "g.example.com", nil),
		provider("ns1", "good2", PhaseReady, "g2.example.com", nil),
	}
	set, err := Resolve([]*v1alpha1.ExternalModel{m}, provs)
	if err != nil {
		t.Fatalf("partial resolution must not error: %v", err)
	}
	rs := set.Routes()
	if len(rs) != 1 || rs[0].Provider != "good" {
		t.Fatalf("want only the good ref resolved, got %+v", rs)
	}
	if len(set.Models[0].Skips) != 2 {
		t.Errorf("want 2 aggregated skips, got %+v", set.Models[0].Skips)
	}
}

func withWeight(r v1alpha1.ExternalProviderRef, w int) v1alpha1.ExternalProviderRef {
	r.Weight = intptr(w)
	return r
}

func TestResolve_ProviderNamespace(t *testing.T) {
	for name, resolve := range map[string]func([]*v1alpha1.ExternalModel, []*v1alpha1.ExternalProvider) (*ResolvedRouteSet, error){
		"serving": Resolve, "preload": ResolveAll,
	} {
		t.Run(name, func(t *testing.T) {
			for _, namespace := range []string{"", "models", "shared"} {
				t.Run("namespace="+namespace, func(t *testing.T) {
					r := ref("provider", "gpt", "/v1/chat/completions")
					r.Ref.Namespace = namespace
					set, err := resolve([]*v1alpha1.ExternalModel{model("models", "model", r)}, []*v1alpha1.ExternalProvider{
						provider("models", "provider", PhaseReady, "local.example.com", nil),
						provider("shared", "provider", PhaseReady, "foreign.example.com", nil),
					})
					if err != nil || len(set.Routes()) != 1 {
						t.Fatalf("set=%#v err=%v", set, err)
					}
					route := set.Routes()[0]
					if namespace != "shared" {
						if route.Endpoint != "local.example.com" || route.CrossNamespace() || route.Cluster != "provider-provider" || route.SecretRefNamespace() != "models" {
							t.Fatalf("local reference = %#v", route)
						}
						return
					}
					// Never the same-named local provider; the inherited
					// credential stays in the provider namespace.
					if route.Endpoint != "foreign.example.com" || route.ProviderNamespace != "shared" || route.SecretRefNamespace() != "shared" {
						t.Fatalf("foreign reference = %#v", route)
					}
					if route.Cluster != route.ServiceName() || !strings.HasPrefix(route.Cluster, "provider-provider-") || len(route.Cluster) != len("provider-provider-")+12 {
						t.Fatalf("foreign cluster = %q, want the hashed Service name", route.Cluster)
					}
				})
			}
		})
	}
}

func TestResolve_UnpermittedNamespaceKeepsLocalRefs(t *testing.T) {
	foreign := ref("provider", "foreign-target", "/v1/chat/completions")
	foreign.Ref.Namespace = "shared"
	m := model("models", "model", foreign, ref("provider", "local-target", "/v1/chat/completions"))
	p := provider("models", "provider", PhaseReady, "local.example.com", nil)
	for _, resolve := range []func([]*v1alpha1.ExternalModel, []*v1alpha1.ExternalProvider) (*ResolvedRouteSet, error){Resolve, ResolveAll} {
		set, err := resolve([]*v1alpha1.ExternalModel{m}, []*v1alpha1.ExternalProvider{p})
		if err != nil || len(set.Routes()) != 1 || set.Routes()[0].TargetModel != "local-target" {
			t.Fatalf("mixed references: set=%#v err=%v", set, err)
		}
		skips := set.Models[0].Skips
		if len(skips) != 1 || skips[0].Reason != SkipRefNotPermitted || !strings.Contains(skips[0].Message, "shared/provider is not found or not permitted") {
			t.Fatalf("unpermitted reference skips = %#v", skips)
		}
	}
}

func TestResolve_SameNamedProvidersFromTwoNamespaces(t *testing.T) {
	a, b := ref("openai", "gpt", "/v1/chat/completions"), ref("openai", "gpt", "/v1/chat/completions")
	a.Ref.Namespace, b.Ref.Namespace = "ns-a", "ns-b"
	overridden := v1alpha1.AuthConfig{Type: "apikey", SecretRef: v1alpha1.NameReference{Name: "model-key"}}
	b.Auth = &overridden
	set, err := Resolve([]*v1alpha1.ExternalModel{model("models", "model", a, b)}, []*v1alpha1.ExternalProvider{
		provider("ns-a", "openai", PhaseReady, "a.example.com", nil),
		provider("ns-b", "openai", PhaseReady, "b.example.com", nil),
	})
	if err != nil || len(set.Routes()) != 2 {
		t.Fatalf("set=%#v err=%v", set, err)
	}
	routeA, routeB := set.Routes()[0], set.Routes()[1]
	if routeA.Cluster == routeB.Cluster || routeA.Endpoint != "a.example.com" || routeB.Endpoint != "b.example.com" {
		t.Fatalf("same-named providers are not distinct: %#v %#v", routeA, routeB)
	}
	// Inherited auth uses the provider namespace; an override the model namespace.
	if routeA.SecretName != "openai-key" || routeA.SecretRefNamespace() != "ns-a" || routeB.SecretName != "model-key" || routeB.SecretRefNamespace() != "models" {
		t.Fatalf("credential sources = %s/%s and %s/%s", routeA.SecretRefNamespace(), routeA.SecretName, routeB.SecretRefNamespace(), routeB.SecretName)
	}
}

func TestResolve_SimpleAuthIsAPIKey(t *testing.T) {
	inherited, overridden := ref("prov-a", "gpt", "/v1/chat/completions"), ref("prov-b", "gpt", "/v1/chat/completions")
	overridden.Auth = &v1alpha1.AuthConfig{Type: "simple", SecretRef: v1alpha1.NameReference{Name: "model-key"}}
	providerA := provider("ns1", "prov-a", PhaseReady, "a.example.com", nil)
	providerA.Spec.Auth.Type = "simple"
	set, err := Resolve([]*v1alpha1.ExternalModel{model("ns1", "my-model", inherited, overridden)},
		[]*v1alpha1.ExternalProvider{providerA, provider("ns1", "prov-b", PhaseReady, "b.example.com", nil)})
	if err != nil || len(set.Routes()) != 2 {
		t.Fatalf("set=%#v err=%v", set, err)
	}
	for _, route := range set.Routes() {
		if route.AuthType != "apikey" {
			t.Fatalf("route %s auth type = %q, want apikey", route.Provider, route.AuthType)
		}
	}
}

func TestRouteServiceNameFitsServiceNameLimit(t *testing.T) {
	route := Route{Namespace: "models", ProviderNamespace: "shared", Provider: strings.Repeat("p", 253)}
	if name := route.ServiceName(); len(name) != 63 || !strings.HasPrefix(name, "provider-ppp") {
		t.Fatalf("ServiceName() = %q (%d characters)", name, len(name))
	}
	route.ProviderNamespace = "other"
	if other := route.ServiceName(); other == (Route{Namespace: "models", ProviderNamespace: "shared", Provider: route.Provider}).ServiceName() {
		t.Fatalf("providers from different namespaces share Service name %q", other)
	}
}

func TestResolve_NamespaceIsolation(t *testing.T) {
	// A provider with the right name in another namespace must NOT resolve.
	m := model("ns1", "m", ref("prov-a", "t", "/v1"))
	foreign := provider("ns2", "prov-a", PhaseReady, "x.example.com", nil)

	set, err := Resolve([]*v1alpha1.ExternalModel{m}, []*v1alpha1.ExternalProvider{foreign})
	if !errors.Is(err, ErrNoRoutes) {
		t.Fatalf("cross-namespace ref must skip, got err=%v", err)
	}
	if set.Models[0].Skips[0].Reason != SkipRefNotFound {
		t.Errorf("reason: got %q", set.Models[0].Skips[0].Reason)
	}
}

func TestResolve_MergedConfigAndPathTemplates(t *testing.T) {
	m := model("ns1", "m", ref("vx", "claude-x", "/v1/projects/{project}/locations/{location}/models/{model}:raw"))
	p := provider("ns1", "vx", PhaseReady, "aiplatform.googleapis.com", map[string]string{
		"project":  "prov-project",
		"location": "us-central1",
	})
	// ref config overrides provider config (IPP merged-config precedence)
	m.Spec.ExternalProviderRefs[0].Config = map[string]string{"project": "ref-project"}

	set, err := Resolve([]*v1alpha1.ExternalModel{m}, []*v1alpha1.ExternalProvider{p})
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	want := "/v1/projects/ref-project/locations/us-central1/models/claude-x:raw"
	if got := set.Routes()[0].Path; got != want {
		t.Errorf("path:\n got %s\nwant %s", got, want)
	}
}

func TestResolve_MergedConfigCarriesMountedTLSCAPath(t *testing.T) {
	m := model("ns1", "m", ref("p", "target", "/v1"))
	m.Spec.ExternalProviderRefs[0].Config = map[string]string{
		"tls.caCertificates": "/etc/istio/provider-ca/ca.crt",
	}
	p := provider("ns1", "p", PhaseReady, "provider.example.com", map[string]string{
		"tls.caCertificates": "/etc/istio/provider-ca/ca.crt",
	})
	set, err := Resolve([]*v1alpha1.ExternalModel{m}, []*v1alpha1.ExternalProvider{p})
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if got := set.Routes()[0].TLSCACertificates; got != "/etc/istio/provider-ca/ca.crt" {
		t.Fatalf("TLS CA path = %q", got)
	}
}

func TestResolve_RejectsModelSpecificTLSCAOverride(t *testing.T) {
	m := model("ns1", "m", ref("p", "target", "/v1"))
	m.Spec.ExternalProviderRefs[0].Config = map[string]string{
		"tls.caCertificates": "/etc/istio/model-ca/ca.crt",
	}
	p := provider("ns1", "p", PhaseReady, "provider.example.com", map[string]string{
		"tls.caCertificates": "/etc/istio/provider-ca/ca.crt",
	})

	_, err := Resolve([]*v1alpha1.ExternalModel{m}, []*v1alpha1.ExternalProvider{p})
	if err == nil || !strings.Contains(err.Error(), "tls.caCertificates cannot override ExternalProvider p") {
		t.Fatalf("Resolve error = %v, want explicit shared-transport CA override error", err)
	}
}

func TestResolve_RejectsUnsafeTLSCAPath(t *testing.T) {
	m := model("ns1", "m", ref("p", "target", "/v1"))
	p := provider("ns1", "p", PhaseReady, "provider.example.com", map[string]string{"tls.caCertificates": "../ca.crt"})
	if _, err := Resolve([]*v1alpha1.ExternalModel{m}, []*v1alpha1.ExternalProvider{p}); err == nil {
		t.Fatal("unsafe TLS CA path must be rejected")
	}
}

func TestResolve_PathErrors(t *testing.T) {
	cases := []struct {
		name string
		path string
		cfg  map[string]string
	}{
		{"missing key", "/v1/{model}?key={secret}", map[string]string{"model": "ok"}},
		{"unclosed brace followed by closed one", "/v1/{model/{oops}", nil},
		{"all keys listed", "/{a}/{b}", nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := model("ns1", "m", ref("p", "target", tc.path))
			p := provider("ns1", "p", PhaseReady, "e.example.com", tc.cfg)
			_, err := Resolve([]*v1alpha1.ExternalModel{m}, []*v1alpha1.ExternalProvider{p})
			if err == nil {
				t.Fatal("path errors must hard-fail the reconcile (IPP #368), got nil")
			}
		})
	}
}

func TestResolve_Ordering(t *testing.T) {
	// Ref order is the deterministic first-admitted fallback order; models
	// keep input order. A partially-bad model keeps relative order.
	m1 := model("ns1", "b-model",
		ref("p2", "t2", "/v2"), ref("ghost", "t", "/v1"), ref("p1", "t1", "/v1"))
	m2 := model("ns1", "a-model", ref("p1", "t1", "/v1"))
	provs := []*v1alpha1.ExternalProvider{
		provider("ns1", "p1", PhaseReady, "p1.example.com", nil),
		provider("ns1", "p2", PhaseReady, "p2.example.com", nil),
	}
	set, err := Resolve([]*v1alpha1.ExternalModel{m1, m2}, provs)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if set.Models[0].ModelRef != "ns1/b-model" || set.Models[1].ModelRef != "ns1/a-model" {
		t.Errorf("model order must follow input: %+v", set.Models)
	}
	got := []string{set.Models[0].Routes[0].Provider, set.Models[0].Routes[1].Provider}
	if got[0] != "p2" || got[1] != "p1" {
		t.Errorf("ref order within model must survive skips: got %v", got)
	}
}

func TestResolve_AuthOverridePerRef(t *testing.T) {
	m := model("ns1", "m", ref("p", "t", "/v1"))
	m.Spec.ExternalProviderRefs[0].Auth = &v1alpha1.AuthConfig{
		Type:      "oauth2",
		SecretRef: v1alpha1.NameReference{Name: "oauth-secret"},
	}
	p := provider("ns1", "p", PhaseReady, "e.example.com", nil)

	set, err := Resolve([]*v1alpha1.ExternalModel{m}, []*v1alpha1.ExternalProvider{p})
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	r := set.Routes()[0]
	if r.AuthType != "oauth2" || r.SecretName != "oauth-secret" {
		t.Errorf("ref auth override failed: %+v", r)
	}
}

func TestResolve_NilAndEmptyInputs(t *testing.T) {
	set, err := Resolve(nil, nil)
	if !errors.Is(err, ErrNoRoutes) || set == nil {
		t.Fatalf("nil inputs: want ErrNoRoutes + non-nil set, got %v / %v", set, err)
	}
	// nil entries are tolerated, not panicked on
	set, err = Resolve(
		[]*v1alpha1.ExternalModel{nil, model("ns", "m", ref("p", "t", "/v1"))},
		[]*v1alpha1.ExternalProvider{nil, provider("ns", "p", PhaseReady, "e.example.com", nil)})
	if err != nil || len(set.Routes()) != 1 {
		t.Fatalf("nil entries must be skipped: %v %v", set, err)
	}
}

// IPP parity (path.go doc comment, verbatim): config substitutes first, so
// an explicit "model" config key takes precedence; {model} is the fallback.
func TestResolvePath_ExplicitModelKeyPrecedence(t *testing.T) {
	got, err := resolvePath("/v1/{model}", "target-model", map[string]string{"model": "config-wins"})
	if err != nil {
		t.Fatalf("resolvePath: %v", err)
	}
	if got != "/v1/config-wins" {
		t.Errorf("explicit config key must win: got %q, want /v1/config-wins", got)
	}
	got, err = resolvePath("/v1/{model}", "target-model", map[string]string{"other": "x"})
	if err != nil {
		t.Fatalf("resolvePath fallback: %v", err)
	}
	if got != "/v1/target-model" {
		t.Errorf("reserved fallback: got %q, want /v1/target-model", got)
	}
}

// Documented deviation from IPP: a config VALUE containing another key's
// placeholder is order-sensitive. IPP iterates its config map in Go's
// randomized order, so its outcome for this input coin-flips between
// "/v1/xzy/z" (a substituted first) and a PathUnresolved error on {b}
// (b first). We substitute in sorted key order, so the outcome is fixed.
// On configs whose values contain no placeholder tokens — the real-world
// case — sorted order produces byte-identical output to IPP.
func TestResolvePath_ChainedPlaceholderIsDeterministic(t *testing.T) {
	const want = "/v1/xzy/z"
	for i := 0; i < 50; i++ { // Go randomizes map iteration; 50 runs
		// would catch any regression to randomized order.
		got, err := resolvePath("/v1/{a}/{b}", "t", map[string]string{"a": "x{b}y", "b": "z"})
		if err != nil {
			t.Fatalf("run %d: want deterministic success, got error %v", i, err)
		}
		if got != want {
			t.Fatalf("run %d: got %q, want %q (sorted substitution)", i, got, want)
		}
	}
}

// IPP parity quirks, preserved deliberately so IPP's table tests port
// without edits: `{}` (empty key) and a stray `{` with no closing `}` do
// not match the placeholder regex and pass through unresolved. Tightening
// either would deviate from the parity bar; flag as a 3.7 candidate.
func TestResolvePath_MalformedBracesPassThrough(t *testing.T) {
	for _, path := range []string{"/v1/{}", "/v1/{unterminated"} {
		got, err := resolvePath(path, "t", nil)
		if err != nil {
			t.Fatalf("IPP does not error on %q: %v", path, err)
		}
		if got != path {
			t.Errorf("got %q, want %q (IPP pass-through parity)", got, path)
		}
	}
}

// TestResolveTwoModelsOneProvider verifies that two distinct ExternalModels
// referencing the same ExternalProvider both resolve cleanly. The resolver
// must not reject this topology; the envelope layer is responsible for
// giving each candidate a unique stable_id.
func TestResolveTwoModelsOneProvider(t *testing.T) {
	shared := provider("ns1", "shared-prov", PhaseReady, "api.example.com", nil)
	modelA := model("ns1", "model-a", ref("shared-prov", "gpt-4o", "/v1/chat/completions"))
	modelB := model("ns1", "model-b", ref("shared-prov", "gpt-4o-mini", "/v1/chat/completions"))

	set, err := Resolve([]*v1alpha1.ExternalModel{modelA, modelB}, []*v1alpha1.ExternalProvider{shared})
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	routes := set.Routes()
	if len(routes) != 2 {
		t.Fatalf("routes = %d, want 2", len(routes))
	}
	if routes[0].Model != "model-a" || routes[1].Model != "model-b" {
		t.Fatalf("models = [%q, %q], want [model-a, model-b]", routes[0].Model, routes[1].Model)
	}
	if routes[0].Cluster != routes[1].Cluster {
		t.Fatalf("two models sharing a provider must have the same Cluster; got %q and %q",
			routes[0].Cluster, routes[1].Cluster)
	}
	if routes[0].Cluster != "provider-shared-prov" {
		t.Fatalf("cluster = %q, want provider-shared-prov", routes[0].Cluster)
	}
}

func TestValidateProviderTLSRejectsPlaintext(t *testing.T) {
	for value, wantErr := range map[string]string{
		"":      "",
		"true":  "",
		"false": "plaintext transport",
		"no":    `must be "true" or "false"`,
	} {
		p := provider("ns1", "p", PhaseReady, "p.example.com", nil)
		if value != "" {
			p.Annotations = map[string]string{ProviderTLSAnnotation: value}
		}
		err := ValidateProviderTLS(p)
		if wantErr == "" && err != nil || wantErr != "" && (err == nil || !strings.Contains(err.Error(), wantErr)) {
			t.Errorf("ValidateProviderTLS(%q) = %v, want %q", value, err, wantErr)
		}
	}
}

func TestResolve_NotReadySkipCarriesProviderReason(t *testing.T) {
	p := provider("ns1", "p", "Failed", "p.example.com", nil)
	p.Status.Conditions = []metav1.Condition{{Type: "Ready", Status: metav1.ConditionFalse, Reason: "ReconcileFailed", Message: "endpoint is invalid"}}
	set, err := Resolve([]*v1alpha1.ExternalModel{model("ns1", "m", ref("p", "t", "/v1"))}, []*v1alpha1.ExternalProvider{p})
	if !errors.Is(err, ErrNoRoutes) {
		t.Fatalf("Resolve error = %v, want ErrNoRoutes", err)
	}
	if skips := set.Models[0].Skips; len(skips) != 1 || skips[0].Reason != SkipProviderNotReady ||
		skips[0].Message != `provider phase "Failed" is not Ready: endpoint is invalid` {
		t.Fatalf("skips = %#v", skips)
	}
}
