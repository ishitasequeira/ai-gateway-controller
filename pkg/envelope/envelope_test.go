package envelope

import (
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1alpha1 "github.com/opendatahub-io/ai-gateway-controller/api/inference/v1alpha1"
	"github.com/opendatahub-io/ai-gateway-controller/pkg/resolver"
)

func scope() Scope {
	return Scope{Network: "net1", Gateway: "gw1", Namespace: "ns1", LocalSite: "site-a"}
}

func route(model, provider string, weight int) resolver.Route {
	return resolver.Route{
		Model: model, ClientName: model, Namespace: "ns1", Provider: provider, ProviderType: "openai",
		Cluster: "provider-" + provider, Endpoint: provider + ".example.com",
		TargetModel: "t", APIFormat: "openai-chat", Path: "/v1/chat/completions",
		Weight: weight, AuthType: "apikey", SecretName: provider + "-key", SecretKey: "api-key",
	}
}

func routeSet(models ...resolver.ModelRoutes) *resolver.ResolvedRouteSet {
	return &resolver.ResolvedRouteSet{Models: models}
}

func TestRender_HappyPath(t *testing.T) {
	set := routeSet(resolver.ModelRoutes{
		ModelRef: "ns1/m1",
		Routes:   []resolver.Route{route("m1", "p1", 1)},
	})

	env, err := Render(set, scope(), Revision{}, Options{
		KnownClusters: []string{"provider-p1"}, SourceUID: "uid-1",
		ProducerVersion: "0.1.0", RenderedAt: "2026-09-03T00:00:00Z", IncludeProviderProfiles: true,
	})
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	if env.SchemaVersion != "1.0.0" {
		t.Errorf("schema_version: %q", env.SchemaVersion)
	}
	if len(env.Overlay.Candidates) != 1 {
		t.Fatalf("candidates: %+v", env.Overlay.Candidates)
	}
	c := env.Overlay.Candidates[0]
	if c.Name != "m1" || c.Cluster != "provider-p1" || c.Kind != "inference_model" || c.Site != "site-a" || !c.Fresh {
		t.Errorf("candidate mapping: %+v", c)
	}
	if c.APIFormat != "openai-chat" || c.Path != "/v1/chat/completions" || c.TargetModel != "t" {
		t.Fatalf("trusted provider profile missing from candidate: %+v", c)
	}
	// Wire shape per praxis validate_revision_shape (overlay.rs):
	// revision = {kind: "content_addressed", algorithm: "sha256", value}.
	if env.Revision.Kind != "content_addressed" || env.Revision.Algorithm != "sha256" {
		t.Errorf("revision shape: %+v", env.Revision)
	}
	if env.Revision.Value != env.ContentDigest.Value || len(env.Revision.Value) != 64 {
		t.Errorf("revision/content_digest must match, 64 hex: %q vs %q", env.Revision.Value, env.ContentDigest.Value)
	}
	if env.Provenance.SourceGeneration != 1 {
		t.Errorf("first distribution source_generation: %d, want 1", env.Provenance.SourceGeneration)
	}
}

func TestRenderPublishesDistinctProfilesForMixedProviderSelection(t *testing.T) {
	openai := route("client-model", "openai", 1)
	openai.Model, openai.ClientName = "external-model", "client-model"
	openai.TargetModel, openai.Path, openai.APIFormat = "gpt-deployed", "/v1/chat/completions", "openai-chat"
	anthropic := route("client-model", "anthropic", 1)
	anthropic.Model, anthropic.ClientName = "external-model", "client-model"
	anthropic.ProviderType, anthropic.TargetModel = "anthropic", "claude-deployed"
	anthropic.APIFormat, anthropic.Path = "messages", "/v1/messages"
	set := routeSet(resolver.ModelRoutes{ModelRef: "ns1/external-model", Routes: []resolver.Route{openai, anthropic}})
	env, err := Render(set, scope(), Revision{}, Options{IncludeProviderProfiles: true})
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	if len(env.Overlay.Candidates) != 2 {
		t.Fatalf("candidate count = %d, want 2", len(env.Overlay.Candidates))
	}
	profiles := map[string]Candidate{}
	for _, candidate := range env.Overlay.Candidates {
		profiles[candidate.StableID] = candidate
	}
	if got := profiles[CandidateStableID("external-model", "openai")]; got.APIFormat != "openai-chat" || got.Path != "/v1/chat/completions" || got.TargetModel != "gpt-deployed" {
		t.Fatalf("OpenAI candidate profile = %+v", got)
	}
	if got := profiles[CandidateStableID("external-model", "anthropic")]; got.APIFormat != "messages" || got.Path != "/v1/messages" || got.TargetModel != "claude-deployed" {
		t.Fatalf("Anthropic candidate profile = %+v", got)
	}
}

func TestRenderProviderProfilesAreOptInAndClearedWhenTranslationTurnsOff(t *testing.T) {
	r := route("client-model", "openai", 1)
	r.Model = "external-model"
	r.TargetModel = "gpt-deployed"
	r.Path = "/v1/responses"
	set := routeSet(resolver.ModelRoutes{ModelRef: "ns1/external-model", Routes: []resolver.Route{r}})

	offBefore, err := Render(set, scope(), Revision{}, Options{})
	if err != nil {
		t.Fatalf("Render flag-off overlay: %v", err)
	}
	assertProviderProfile(t, offBefore.Overlay.Candidates[0], "", "", "")

	on, err := Render(set, scope(), revisionForEnvelope(offBefore), Options{IncludeProviderProfiles: true})
	if err != nil {
		t.Fatalf("Render flag-on overlay: %v", err)
	}
	assertProviderProfile(t, on.Overlay.Candidates[0], "openai-chat", "/v1/responses", "gpt-deployed")

	offAfter, err := Render(&resolver.ResolvedRouteSet{}, scope(), revisionForEnvelope(on), Options{
		PreservedCandidates:     on.Overlay.Candidates,
		IncludeProviderProfiles: false,
	})
	if err != nil {
		t.Fatalf("Render flag-off overlay after toggle: %v", err)
	}
	assertProviderProfile(t, offAfter.Overlay.Candidates[0], "", "", "")
	if offAfter.ContentDigest.Value != offBefore.ContentDigest.Value {
		t.Fatalf("flag-off digest after toggle = %s, want original flag-off digest %s", offAfter.ContentDigest.Value, offBefore.ContentDigest.Value)
	}
}

func revisionForEnvelope(env Envelope) Revision {
	return Revision{Generation: env.Provenance.SourceGeneration, Digest: env.ContentDigest.Value}
}

func assertProviderProfile(t *testing.T, candidate Candidate, apiFormat, path, targetModel string) {
	t.Helper()
	if candidate.APIFormat != apiFormat || candidate.Path != path || candidate.TargetModel != targetModel {
		t.Fatalf("provider profile = (%q, %q, %q), want (%q, %q, %q)",
			candidate.APIFormat, candidate.Path, candidate.TargetModel, apiFormat, path, targetModel)
	}
	raw, err := json.Marshal(candidate)
	if err != nil {
		t.Fatalf("marshal candidate: %v", err)
	}
	if apiFormat == "" && (strings.Contains(string(raw), `"api_format"`) || strings.Contains(string(raw), `"target_model"`) || strings.Contains(string(raw), `"path"`)) {
		t.Fatalf("flag-off candidate serializes provider profile fields: %s", raw)
	}
}

func TestRender_CredentialWireShapeExact(t *testing.T) {
	// The consumer rejects unknown fields inside credential objects — the
	// emitted shape must be exactly {strategy, secretRef{name,namespace,key}},
	// apikey is translated to the wire-level bearer_token strategy.
	r := route("m1", "p1", 1)
	set := routeSet(resolver.ModelRoutes{ModelRef: "ns1/m1", Routes: []resolver.Route{r}})
	env, err := Render(set, scope(), Revision{}, Options{})
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	if env.Overlay.Candidates[0].Credential == nil {
		t.Fatal("apikey must render a credential reference")
	}
	raw, err := json.Marshal(env.Overlay.Candidates[0].Credential)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"strategy":"bearer_token","secretRef":{"name":"p1-key","namespace":"ns1","key":"api-key"}}`
	if string(raw) != want {
		t.Errorf("credential JSON:\n got %s\nwant %s", raw, want)
	}
}

func TestRender_APIKeyRejectsUnqualifiedCombinations(t *testing.T) {
	// Only explicitly qualified provider/API-format pairs earn a wire
	// strategy; every other combination — including the qualified formats
	// under the wrong provider — must fail closed loudly.
	tests := []struct {
		name         string
		providerType string
		apiFormat    string
	}{
		{"anthropic with openai chat format", "anthropic", "openai-chat"},
		{"openai with anthropic messages format", "openai", "messages"},
		{"azure with anthropic messages format", "azure", "messages"},
		{"aws-bedrock with openai chat format", "aws-bedrock", "openai-chat"},
		{"vertex with anthropic messages format", "vertex", "messages"},
		{"anthropic with unknown format", "anthropic", "completions"},
		{"unknown provider with known format", "mistral", "openai-chat"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := route("m1", "p1", 1)
			r.ProviderType = tc.providerType
			r.APIFormat = tc.apiFormat
			_, err := Render(routeSet(resolver.ModelRoutes{ModelRef: "ns1/m1", Routes: []resolver.Route{r}}), scope(), Revision{}, Options{})
			if err == nil || !strings.Contains(err.Error(), "provider credential semantics are not supported") {
				t.Fatalf("expected provider-specific credential error for %s/%s, got %v", tc.providerType, tc.apiFormat, err)
			}
			if !strings.Contains(err.Error(), strconv.Quote(tc.providerType)) ||
				!strings.Contains(err.Error(), strconv.Quote(tc.apiFormat)) {
				t.Errorf("error must name both the provider and the API format, got %v", err)
			}
		})
	}
}

func TestRender_AnthropicMessagesMapsToApikeyReference(t *testing.T) {
	// The anthropic messages contract injects the Secret value through the
	// credential_inject filter's configured header, so the wire carries the
	// apikey strategy with the same reference-only shape as bearer_token.
	r := route("m1", "p1", 1)
	r.ProviderType = "anthropic"
	r.APIFormat = "messages"
	env, err := Render(routeSet(resolver.ModelRoutes{ModelRef: "ns1/m1", Routes: []resolver.Route{r}}), scope(), Revision{}, Options{})
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	raw, err := json.Marshal(env.Overlay.Candidates[0].Credential)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"strategy":"apikey","secretRef":{"name":"p1-key","namespace":"ns1","key":"api-key"}}`
	if string(raw) != want {
		t.Errorf("credential JSON:\n got %s\nwant %s", raw, want)
	}
	if strings.Contains(string(raw), "fixture-only-secret") {
		t.Error("credential value must never reach the wire")
	}
}

func TestRender_AzureOpenAIChatMapsToApikeyWithHeaderOverride(t *testing.T) {
	// Azure OpenAI uses the api-key header, diverging from the credential_inject
	// filter's x-api-key default. The wire stays reference-only with the apikey
	// strategy; the header override rides in-memory (json:"-") into the ExtProc
	// credential entry, never onto the overlay.
	for _, providerType := range []string{"azure", "azure-openai"} {
		t.Run(providerType, func(t *testing.T) {
			r := route("m1", "p1", 1)
			r.ProviderType = providerType
			r.APIFormat = "openai-chat"
			env, err := Render(routeSet(resolver.ModelRoutes{ModelRef: "ns1/m1", Routes: []resolver.Route{r}}), scope(), Revision{}, Options{})
			if err != nil {
				t.Fatalf("Render: %v", err)
			}
			cred := env.Overlay.Candidates[0].Credential
			if cred == nil || cred.Strategy != "apikey" {
				t.Fatalf("expected apikey strategy, got %+v", cred)
			}
			if cred.Header != "api-key" {
				t.Errorf("header = %q, want api-key", cred.Header)
			}
			raw, err := json.Marshal(cred)
			if err != nil {
				t.Fatal(err)
			}
			want := `{"strategy":"apikey","secretRef":{"name":"p1-key","namespace":"ns1","key":"api-key"}}`
			if string(raw) != want {
				t.Errorf("credential wire JSON must stay reference-only without header:\n got %s\nwant %s", raw, want)
			}
		})
	}
}

func TestIsQualifiedAuthHeader(t *testing.T) {
	// Exactly the non-empty headers StrategyFor emits are qualified; the empty
	// header (bearer_token, none injected) and any other value are not, so a
	// consumer can reject an untrusted or unrecognized header annotation.
	for header, want := range map[string]bool{
		"api-key":         true,
		"x-api-key":       true,
		"":                false,
		"authorization":   false,
		"Authorization":   false,
		"x-forwarded-for": false,
	} {
		if got := IsQualifiedAuthHeader(header); got != want {
			t.Errorf("IsQualifiedAuthHeader(%q) = %t, want %t", header, got, want)
		}
	}
}

func TestRender_CredentialOmittedForUnmappedAuth(t *testing.T) {
	// SigV4 and OAuth2 have no supported wire mapping and must fail closed.
	for _, authType := range []string{"sigv4", "oauth2"} {
		r := route("m1", "p1", 1)
		r.AuthType = authType
		set := routeSet(resolver.ModelRoutes{ModelRef: "ns1/m1", Routes: []resolver.Route{r}})
		_, err := Render(set, scope(), Revision{}, Options{})
		if err == nil || !strings.Contains(err.Error(), "not supported") {
			t.Errorf("%s: expected loud unsupported-auth error, got %v", authType, err)
		}
	}
}

func TestRender_APIKeyMapsToReferenceOnlyBearerCredential(t *testing.T) {
	r := route("m1", "p1", 1)
	r.AuthType = "apikey"
	env, err := Render(routeSet(resolver.ModelRoutes{ModelRef: "ns1/m1", Routes: []resolver.Route{r}}), scope(), Revision{}, Options{})
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	raw, err := json.Marshal(env.Overlay.Candidates[0])
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), `"strategy":"bearer_token"`) ||
		!strings.Contains(string(raw), `"secretRef":{"name":"p1-key","namespace":"ns1","key":"api-key"}`) {
		t.Fatalf("apikey reference mapping missing: %s", raw)
	}
	if strings.Contains(string(raw), "fixture-only-secret") {
		t.Fatal("credential value appeared in reference-only overlay")
	}
}

func TestRender_WeightSelection(t *testing.T) {
	cases := []struct {
		name       string
		weights    []int
		wantPolicy string
		wantWeight bool
	}{
		{"uniform nil-normalized", []int{1, 1}, `{"mode":"random"}`, false},
		{"uniform equal nonzero", []int{2, 2}, `{"mode":"random"}`, false},
		{"single provider any weight", []int{7}, "", false},
		{"non-uniform 70/30", []int{70, 30}, `{"mode":"weightedRandom"}`, true},
		{"non-uniform 2/1", []int{2, 1}, `{"mode":"weightedRandom"}`, true},
		{"non-uniform 80/20", []int{80, 20}, `{"mode":"weightedRandom"}`, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var routes []resolver.Route
			for i, w := range tc.weights {
				routes = append(routes, route("m1", string(rune('a'+i)), w))
			}
			env, err := Render(routeSet(resolver.ModelRoutes{ModelRef: "ns1/m1", Routes: routes}), scope(), Revision{}, Options{})
			if err != nil {
				t.Fatalf("weights %v: unexpected error %v", tc.weights, err)
			}
			if string(env.Overlay.SelectionPolicy) != tc.wantPolicy {
				t.Fatalf("weights %v: selection_policy = %s, want %s", tc.weights, env.Overlay.SelectionPolicy, tc.wantPolicy)
			}
			for i, candidate := range env.Overlay.Candidates {
				if tc.wantWeight {
					if candidate.SelectionGroup == nil || *candidate.SelectionGroup != 0 {
						t.Fatalf("candidate %d selection_group = %v, want 0", i, candidate.SelectionGroup)
					}
					if candidate.TrafficWeight == nil || int(*candidate.TrafficWeight) != tc.weights[i] {
						t.Fatalf("candidate %d traffic_weight = %v, want %d", i, candidate.TrafficWeight, tc.weights[i])
					}
				} else if candidate.TrafficWeight != nil {
					t.Fatalf("candidate %d traffic_weight = %v, want omitted", i, candidate.TrafficWeight)
				}
			}
		})
	}
}

func TestRender_UnequalWeightForcesWeightedRandomOnSiblingModels(t *testing.T) {
	// Model A uniform, model B unequal → whole overlay uses weightedRandom
	// because Praxis allows only one selection_policy.mode.
	set := routeSet(
		resolver.ModelRoutes{ModelRef: "ns1/a", Routes: []resolver.Route{route("a", "p1", 1), route("a", "p2", 1)}},
		resolver.ModelRoutes{ModelRef: "ns1/b", Routes: []resolver.Route{route("b", "p1", 7), route("b", "p2", 3)}},
	)
	env, err := Render(set, scope(), Revision{}, Options{})
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	if string(env.Overlay.SelectionPolicy) != `{"mode":"weightedRandom"}` {
		t.Fatalf("selection_policy = %s, want weightedRandom", env.Overlay.SelectionPolicy)
	}
	for _, candidate := range env.Overlay.Candidates {
		if candidate.SelectionGroup == nil || candidate.TrafficWeight == nil {
			t.Fatalf("weightedRandom candidate missing group/weight: %+v", candidate)
		}
	}
}

func TestRender_WeightedRandomNormalizesMixedCapabilitiesAndPreservesContributions(t *testing.T) {
	group := uint32(0)
	weightNine, weightThree := uint32(9), uint32(3)
	preserved := []Candidate{
		{Cluster: "preserved-equal-a", StableID: "other/equal-a", Kind: "inference_model", Name: "preserved-equal", Site: "site-a", Fresh: true, SelectionGroup: &group},
		{Cluster: "preserved-equal-b", StableID: "other/equal-b", Kind: "inference_model", Name: "preserved-equal", Site: "site-a", Fresh: true, SelectionGroup: &group},
		{Cluster: "preserved-weighted-a", StableID: "other/weighted-a", Kind: "inference_model", Name: "preserved-weighted", Site: "site-a", Fresh: true, SelectionGroup: &group, TrafficWeight: &weightNine},
		{Cluster: "preserved-weighted-b", StableID: "other/weighted-b", Kind: "inference_model", Name: "preserved-weighted", Site: "site-a", Fresh: true, SelectionGroup: &group, TrafficWeight: &weightThree},
	}
	routes := routeSet(
		resolver.ModelRoutes{ModelRef: "ns1/weighted", Routes: []resolver.Route{route("weighted", "a", 50), route("weighted", "b", 25), route("weighted", "c", 25)}},
		resolver.ModelRoutes{ModelRef: "ns1/equal", Routes: []resolver.Route{route("equal", "d", 7), route("equal", "e", 7)}},
		resolver.ModelRoutes{ModelRef: "ns1/single", Routes: []resolver.Route{route("single", "f", 4)}},
	)
	env, err := Render(routes, scope(), Revision{}, Options{PreservedCandidates: preserved})
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	if string(env.Overlay.SelectionPolicy) != `{"mode":"weightedRandom"}` {
		t.Fatalf("selection_policy = %s, want weightedRandom", env.Overlay.SelectionPolicy)
	}
	weightsByStableID := map[string]uint32{
		"weighted/provider-a": 50, "weighted/provider-b": 25, "weighted/provider-c": 25,
		"equal/provider-d": 7, "equal/provider-e": 7, "single/provider-f": 4,
		"other/equal-a": 1, "other/equal-b": 1,
		"other/weighted-a": 9, "other/weighted-b": 3,
	}
	if len(env.Overlay.Candidates) != len(weightsByStableID) {
		t.Fatalf("candidate count = %d, want %d", len(env.Overlay.Candidates), len(weightsByStableID))
	}
	for _, candidate := range env.Overlay.Candidates {
		want, ok := weightsByStableID[candidate.StableID]
		if !ok {
			t.Fatalf("unexpected candidate: %+v", candidate)
		}
		if candidate.SelectionGroup == nil || *candidate.SelectionGroup != 0 {
			t.Errorf("candidate %s selection_group = %v, want 0", candidate.StableID, candidate.SelectionGroup)
		}
		if candidate.TrafficWeight == nil || *candidate.TrafficWeight != want {
			got := "<nil>"
			if candidate.TrafficWeight != nil {
				got = strconv.FormatUint(uint64(*candidate.TrafficWeight), 10)
			}
			t.Errorf("candidate %s traffic_weight = %s, want %d", candidate.StableID, got, want)
		}
	}
}

func TestRender_SelectionPolicyTransitionsOutOfWeightedRandom(t *testing.T) {
	weighted := routeSet(resolver.ModelRoutes{ModelRef: "ns1/model", Routes: []resolver.Route{
		route("model", "a", 80), route("model", "b", 20),
	}})
	first, err := Render(weighted, scope(), Revision{}, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if string(first.Overlay.SelectionPolicy) != `{"mode":"weightedRandom"}` {
		t.Fatalf("initial policy = %s, want weightedRandom", first.Overlay.SelectionPolicy)
	}
	equal := routeSet(resolver.ModelRoutes{ModelRef: "ns1/model", Routes: []resolver.Route{
		route("model", "a", 4), route("model", "b", 4),
	}})
	second, err := Render(equal, scope(), Revision{Generation: first.Provenance.SourceGeneration, Digest: first.Revision.Value}, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if string(second.Overlay.SelectionPolicy) != `{"mode":"random"}` || second.Provenance.SourceGeneration != first.Provenance.SourceGeneration+1 {
		t.Fatalf("weighted->equal policy/generation = %s/%d, want random/%d", second.Overlay.SelectionPolicy,
			second.Provenance.SourceGeneration, first.Provenance.SourceGeneration+1)
	}
	for _, candidate := range second.Overlay.Candidates {
		if candidate.SelectionGroup == nil || candidate.TrafficWeight != nil {
			t.Fatalf("equal candidate retained weighted metadata after policy transition: %+v", candidate)
		}
	}
	single := routeSet(resolver.ModelRoutes{ModelRef: "ns1/model", Routes: []resolver.Route{route("model", "a", 4)}})
	third, err := Render(single, scope(), Revision{Generation: second.Provenance.SourceGeneration, Digest: second.Revision.Value}, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if len(third.Overlay.SelectionPolicy) != 0 || third.Provenance.SourceGeneration != second.Provenance.SourceGeneration+1 {
		t.Fatalf("equal->singleton policy/generation = %s/%d, want no policy/%d", third.Overlay.SelectionPolicy,
			third.Provenance.SourceGeneration, second.Provenance.SourceGeneration+1)
	}
	if third.Overlay.Candidates[0].SelectionGroup != nil || third.Overlay.Candidates[0].TrafficWeight != nil {
		t.Fatalf("singleton retained selection metadata: %+v", third.Overlay.Candidates[0])
	}
}

func TestRender_EqualProvidersUseRandomGroupZero(t *testing.T) {
	for _, count := range []int{1, 2, 3} {
		t.Run(strconv.Itoa(count)+"-providers", func(t *testing.T) {
			routes := make([]resolver.Route, 0, count)
			for i := 0; i < count; i++ {
				routes = append(routes, route("m1", "p"+strconv.Itoa(i+1), 1))
			}
			env, err := Render(routeSet(resolver.ModelRoutes{ModelRef: "ns1/m1", Routes: routes}), scope(), Revision{}, Options{})
			if err != nil {
				t.Fatalf("Render: %v", err)
			}
			if count == 1 {
				if len(env.Overlay.SelectionPolicy) != 0 || env.Overlay.Candidates[0].SelectionGroup != nil {
					t.Fatalf("singleton must remain deterministic and ungrouped: policy=%s candidate=%+v", env.Overlay.SelectionPolicy, env.Overlay.Candidates[0])
				}
				return
			}
			if string(env.Overlay.SelectionPolicy) != `{"mode":"random"}` {
				t.Fatalf("selection_policy = %s, want random", env.Overlay.SelectionPolicy)
			}
			for i, candidate := range env.Overlay.Candidates {
				if candidate.SelectionGroup == nil || *candidate.SelectionGroup != 0 {
					t.Fatalf("candidate %d selection_group = %v, want numeric zero", i, candidate.SelectionGroup)
				}
			}
			wire, err := json.Marshal(env.Overlay)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(string(wire), `"selection_group":0`) {
				t.Fatalf("group zero missing from wire: %s", wire)
			}
		})
	}
}

func TestRender_ResolvedOmittedAndExplicitWeightsSelectTogether(t *testing.T) {
	weightOne, weightZero := 1, 0
	model := &v1alpha1.ExternalModel{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns1", Name: "m1"},
		Spec: v1alpha1.ExternalModelSpec{ExternalProviderRefs: []v1alpha1.ExternalProviderRef{
			{Ref: v1alpha1.ExternalProviderReference{Name: "p1"}, TargetModel: "target", APIFormat: "openai-chat", Path: "/v1/chat/completions"},
			{Ref: v1alpha1.ExternalProviderReference{Name: "p2"}, TargetModel: "target", APIFormat: "openai-chat", Path: "/v1/chat/completions", Weight: &weightOne},
			{Ref: v1alpha1.ExternalProviderReference{Name: "disabled"}, TargetModel: "target", APIFormat: "openai-chat", Path: "/v1/chat/completions", Weight: &weightZero},
		}},
	}
	providers := make([]*v1alpha1.ExternalProvider, 0, 3)
	for _, name := range []string{"p1", "p2", "disabled"} {
		providers = append(providers, &v1alpha1.ExternalProvider{
			ObjectMeta: metav1.ObjectMeta{Namespace: "ns1", Name: name},
			Spec: v1alpha1.ExternalProviderSpec{
				Provider: "openai", Endpoint: name + ".example.com",
				Auth: v1alpha1.AuthConfig{Type: "apikey", SecretRef: v1alpha1.NameReference{Name: name + "-key"}},
			},
			Status: v1alpha1.ExternalProviderStatus{Phase: resolver.PhaseReady},
		})
	}

	set, err := resolver.Resolve([]*v1alpha1.ExternalModel{model}, providers)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if len(set.Routes()) != 2 || len(set.Models[0].Skips) != 1 || set.Models[0].Skips[0].Reason != resolver.SkipWeightDisabled {
		t.Fatalf("resolved routes/skips = %+v, want two active routes and one disabled zero-weight ref", set.Models)
	}
	for _, route := range set.Routes() {
		if route.Weight != 1 {
			t.Fatalf("provider %s normalized weight = %d, want 1", route.Provider, route.Weight)
		}
	}

	env, err := Render(set, scope(), Revision{}, Options{})
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	if string(env.Overlay.SelectionPolicy) != `{"mode":"random"}` || len(env.Overlay.Candidates) != 2 {
		t.Fatalf("selection policy/candidates = %s/%d, want random and two eligible providers", env.Overlay.SelectionPolicy, len(env.Overlay.Candidates))
	}
	for _, candidate := range env.Overlay.Candidates {
		if candidate.SelectionGroup == nil || *candidate.SelectionGroup != 0 {
			t.Fatalf("candidate %s selection_group = %v, want numeric zero", candidate.Cluster, candidate.SelectionGroup)
		}
	}
}

func TestRender_RandomGroupsAreScopedPerClientModel(t *testing.T) {
	set := routeSet(
		resolver.ModelRoutes{ModelRef: "ns1/a", Routes: []resolver.Route{route("a", "p1", 2), route("a", "p2", 2)}},
		resolver.ModelRoutes{ModelRef: "ns1/b", Routes: []resolver.Route{route("b", "p3", 7)}},
	)
	env, err := Render(set, scope(), Revision{}, Options{})
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	if len(env.Overlay.Candidates) != 3 || string(env.Overlay.SelectionPolicy) != `{"mode":"random"}` {
		t.Fatalf("overlay metadata = policy %s candidates %#v", env.Overlay.SelectionPolicy, env.Overlay.Candidates)
	}
	for i := range env.Overlay.Candidates[:2] {
		if env.Overlay.Candidates[i].SelectionGroup == nil || *env.Overlay.Candidates[i].SelectionGroup != 0 {
			t.Fatalf("multi-provider model candidate %d is not in group zero: %+v", i, env.Overlay.Candidates[i])
		}
	}
	if env.Overlay.Candidates[2].SelectionGroup != nil {
		t.Fatalf("singleton model must remain ungrouped: %+v", env.Overlay.Candidates[2])
	}
}

func TestRender_UnequalWeightsAcrossSameClientModelUseWeightedRandom(t *testing.T) {
	set := routeSet(
		resolver.ModelRoutes{ModelRef: "ns1/a", Routes: []resolver.Route{route("shared", "p1", 2)}},
		resolver.ModelRoutes{ModelRef: "ns1/b", Routes: []resolver.Route{route("shared", "p2", 1)}},
	)
	env, err := Render(set, scope(), Revision{}, Options{})
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	if string(env.Overlay.SelectionPolicy) != `{"mode":"weightedRandom"}` {
		t.Fatalf("selection_policy = %s, want weightedRandom", env.Overlay.SelectionPolicy)
	}
	want := map[string]uint32{"provider-p1": 2, "provider-p2": 1}
	for _, candidate := range env.Overlay.Candidates {
		if candidate.SelectionGroup == nil || *candidate.SelectionGroup != 0 {
			t.Fatalf("candidate %s selection_group = %v, want 0", candidate.Cluster, candidate.SelectionGroup)
		}
		if candidate.TrafficWeight == nil || *candidate.TrafficWeight != want[candidate.Cluster] {
			t.Fatalf("candidate %s traffic_weight = %v, want %d", candidate.Cluster, candidate.TrafficWeight, want[candidate.Cluster])
		}
	}
}

func TestRender_WeightChangeBumpsDigest(t *testing.T) {
	equal := routeSet(resolver.ModelRoutes{ModelRef: "ns1/m1", Routes: []resolver.Route{route("m1", "p1", 1), route("m1", "p2", 1)}})
	first, err := Render(equal, scope(), Revision{}, Options{})
	if err != nil {
		t.Fatal(err)
	}
	unequal := routeSet(resolver.ModelRoutes{ModelRef: "ns1/m1", Routes: []resolver.Route{route("m1", "p1", 80), route("m1", "p2", 20)}})
	changed, err := Render(unequal, scope(), Revision{Generation: first.Provenance.SourceGeneration, Digest: first.Revision.Value}, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if changed.Revision.Value == first.Revision.Value {
		t.Fatal("weight change must bump content digest")
	}
	if changed.Provenance.SourceGeneration != first.Provenance.SourceGeneration+1 {
		t.Fatalf("generation = %d, want %d", changed.Provenance.SourceGeneration, first.Provenance.SourceGeneration+1)
	}
	noop, err := Render(unequal, scope(), Revision{Generation: changed.Provenance.SourceGeneration, Digest: changed.Revision.Value}, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if noop.Provenance.SourceGeneration != changed.Provenance.SourceGeneration || noop.Revision.Value != changed.Revision.Value {
		t.Fatalf("identical weighted overlay must keep generation/digest: got %d/%s want %d/%s",
			noop.Provenance.SourceGeneration, noop.Revision.Value, changed.Provenance.SourceGeneration, changed.Revision.Value)
	}
}

func TestRender_RejectsRepeatedProviderAcrossSameClientModel(t *testing.T) {
	set := routeSet(
		resolver.ModelRoutes{ModelRef: "ns1/a", Routes: []resolver.Route{route("shared", "p1", 1), route("shared", "p2", 1)}},
		resolver.ModelRoutes{ModelRef: "ns1/b", Routes: []resolver.Route{route("shared", "p1", 1)}},
	)
	if _, err := Render(set, scope(), Revision{}, Options{}); !errors.Is(err, ErrDuplicateProvider) {
		t.Fatalf("Render error = %v, want ErrDuplicateProvider", err)
	}
}

func TestRender_AllowsSharedProviderAcrossDifferentClientModels(t *testing.T) {
	set := routeSet(
		resolver.ModelRoutes{ModelRef: "ns1/a", Routes: []resolver.Route{route("a", "p1", 1)}},
		resolver.ModelRoutes{ModelRef: "ns1/b", Routes: []resolver.Route{route("b", "p1", 1)}},
	)
	if _, err := Render(set, scope(), Revision{}, Options{}); err != nil {
		t.Fatalf("different client models may share a provider: %v", err)
	}
}

func TestRender_SelectionMetadataChangesDigestAndNoopKeepsGeneration(t *testing.T) {
	single := routeSet(resolver.ModelRoutes{ModelRef: "ns1/m1", Routes: []resolver.Route{route("m1", "p1", 1)}})
	first, err := Render(single, scope(), Revision{}, Options{})
	if err != nil {
		t.Fatal(err)
	}
	multiple := routeSet(resolver.ModelRoutes{ModelRef: "ns1/m1", Routes: []resolver.Route{route("m1", "p1", 1), route("m1", "p2", 1)}})
	changed, err := Render(multiple, scope(), Revision{Generation: first.Provenance.SourceGeneration, Digest: first.Revision.Value}, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if changed.Revision.Value == first.Revision.Value || changed.Provenance.SourceGeneration != first.Provenance.SourceGeneration+1 {
		t.Fatalf("selection change revision = %s generation %d; prior=%s generation %d", changed.Revision.Value, changed.Provenance.SourceGeneration, first.Revision.Value, first.Provenance.SourceGeneration)
	}
	noop, err := Render(multiple, scope(), Revision{Generation: changed.Provenance.SourceGeneration, Digest: changed.Revision.Value}, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if noop.Revision.Value != changed.Revision.Value || noop.Provenance.SourceGeneration != changed.Provenance.SourceGeneration {
		t.Fatalf("semantic no-op changed revision: prior=%s/%d next=%s/%d", changed.Revision.Value, changed.Provenance.SourceGeneration, noop.Revision.Value, noop.Provenance.SourceGeneration)
	}
}

func TestRender_UnknownCluster(t *testing.T) {
	set := routeSet(resolver.ModelRoutes{ModelRef: "ns1/m1", Routes: []resolver.Route{route("m1", "p1", 1)}})
	_, err := Render(set, scope(), Revision{}, Options{KnownClusters: []string{"provider-other"}})
	if !errors.Is(err, ErrUnknownCluster) {
		t.Fatalf("want ErrUnknownCluster, got %v", err)
	}
	// Empty allowlist = check disabled (M0 stub posture; prod must pass lists).
	if _, err := Render(set, scope(), Revision{}, Options{}); err != nil {
		t.Fatalf("empty allowlist must skip check: %v", err)
	}
}

func TestRender_ScopeAndNilGuards(t *testing.T) {
	set := routeSet(resolver.ModelRoutes{ModelRef: "ns1/m1", Routes: []resolver.Route{route("m1", "p1", 1)}})
	bad := scope()
	bad.Gateway = ""
	if _, err := Render(set, bad, Revision{}, Options{}); !errors.Is(err, ErrScopeMismatch) {
		t.Errorf("empty gateway: want ErrScopeMismatch, got %v", err)
	}
	if _, err := Render(nil, scope(), Revision{}, Options{}); !errors.Is(err, ErrScopeMismatch) {
		t.Errorf("nil routes: want ErrScopeMismatch, got %v", err)
	}
}

func TestRender_RevisionLifecycle(t *testing.T) {
	set := routeSet(resolver.ModelRoutes{ModelRef: "ns1/m1", Routes: []resolver.Route{route("m1", "p1", 1)}})

	firstRev := Revision{Generation: 0}
	first, err := Render(set, scope(), firstRev, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if first.Provenance.SourceGeneration != 1 {
		t.Fatalf("gen: %d", first.Provenance.SourceGeneration)
	}
	// Feed Render the revision it just produced (what the reconciler reads
	// back from the ConfigMap annotation).
	firstDist := Revision{Generation: 1, Digest: first.Revision.Value}
	// Same content → same digest, no bump.
	again, err := Render(set, scope(), firstDist, Options{RenderedAt: "later"})
	if err != nil {
		t.Fatal(err)
	}
	if again.Revision.Value != first.Revision.Value || again.Provenance.SourceGeneration != 1 {
		t.Errorf("unchanged content must not bump generation: %s/%d vs %s/%d",
			again.Revision.Value, again.Provenance.SourceGeneration,
			first.Revision.Value, first.Provenance.SourceGeneration)
	}
	// Changed content → +1, new digest.
	set2 := routeSet(resolver.ModelRoutes{ModelRef: "ns1/m1", Routes: []resolver.Route{route("m1", "p1", 1), route("m1", "p2", 1)}})
	second, err := Render(set2, scope(), firstDist, Options{KnownClusters: []string{"provider-p1", "provider-p2"}})
	if err != nil {
		t.Fatal(err)
	}
	if second.Provenance.SourceGeneration != 2 || second.Revision.Value == first.Revision.Value {
		t.Errorf("content change must bump gen and change digest: gen=%d value=%s",
			second.Provenance.SourceGeneration, second.Revision.Value)
	}
	if _, err := Render(set2, scope(), Revision{Generation: 1<<63 - 1, Digest: first.Revision.Value}, Options{}); !errors.Is(err, ErrGenerationRegression) {
		t.Errorf("generation overflow error = %v, want ErrGenerationRegression", err)
	}
}

func ov(candidates []Candidate, policy string) Overlay {
	o := Overlay{Network: "net", LocalSite: "site", Candidates: candidates}
	if policy != "" {
		o.SelectionPolicy = json.RawMessage(policy)
	}
	return o
}

func TestComputeDigest_StableAndOrderSensitive(t *testing.T) {
	c1 := Candidate{Cluster: "a", Kind: "inference_model", Name: "m", Site: "s", Fresh: true, Credential: &Credential{Strategy: "apikey", SecretRef: SecretRef{Name: "n", Namespace: "ns", Key: "api-key"}}}
	c2 := Candidate{Cluster: "b", Kind: "inference_model", Name: "m", Site: "s", Fresh: true, Credential: &Credential{Strategy: "apikey", SecretRef: SecretRef{Name: "n", Namespace: "ns", Key: "api-key"}}}

	d1a, _ := ComputeDigest(ov([]Candidate{c1}, ""))
	d1b, _ := ComputeDigest(ov([]Candidate{c1}, ""))
	if d1a != d1b {
		t.Fatal("digest must be deterministic")
	}
	d2, _ := ComputeDigest(ov([]Candidate{c2}, ""))
	if d1a == d2 {
		t.Fatal("different candidates must change digest")
	}
	// order is part of the contract (first-admitted fallback)
	d3, _ := ComputeDigest(ov([]Candidate{c2, c1}, ""))
	if d3 == d2 || d3 == d1a {
		t.Error("candidate order must affect digest")
	}
	// selection_policy participates in the digest iff present (overlay.rs)
	d4, _ := ComputeDigest(ov([]Candidate{c1}, `{"mode":"random"}`))
	if d4 == d1a {
		t.Error("selection_policy must affect digest when present")
	}
	group := uint32(0)
	c1Grouped := c1
	c1Grouped.SelectionGroup = &group
	d7, err := ComputeDigest(ov([]Candidate{c1Grouped}, ""))
	if err != nil {
		t.Fatal(err)
	}
	if d7 == d1a {
		t.Error("selection_group zero must affect digest when present")
	}
	// nil vs empty candidates marshal identically ([]), never null
	d5, _ := ComputeDigest(ov(nil, ""))
	d6, _ := ComputeDigest(ov([]Candidate{}, ""))
	if d5 != d6 {
		t.Error("nil and empty candidate lists must digest identically")
	}
}

// Digest regression vectors: each expected value was computed with a
// second implementation (Python json.dumps with sort_keys, compact
// separators, ensure_ascii=False). That reference is RFC-8785-equivalent
// ONLY for these payload shapes: ASCII object keys (there code-point and
// UTF-16 code-unit ordering coincide), integer-only numbers, no exotic
// escapes. So these vectors pin our Go digest path against regressions
// and catch gross JCS bugs — they do NOT establish byte-compatibility
// with the Rust consumer (serde_json_canonicalizer in praxis); that gate
// is the M1 golden vectors extracted from praxis fixtures (port plan R3).
// Non-ASCII *keys* are deliberately absent: the two implementations can
// order them differently, so they are exactly the case this pair cannot
// witness. U+2028/U+2029, non-BMP emoji, and empty-vs-absent fields are
// present because both sides demonstrably agree on them.
func TestComputeDigest_KnownVectors(t *testing.T) {
	cases := []struct {
		name    string
		overlay Overlay
		want    string
	}{
		{
			name: "single candidate with selection policy",
			overlay: Overlay{
				Network: "net-a", LocalSite: "site-a",
				Candidates: []Candidate{{
					Cluster: "local-inference", Kind: "inference_model", Name: "llama-3",
					Site: "site-a", Fresh: true,
					Credential: &Credential{Strategy: "apikey", SecretRef: SecretRef{Name: "llama-key", Namespace: "tenant-a", Key: "api-key"}},
				}},
				SelectionPolicy: json.RawMessage(`{"mode":"random","groups":["g1","g2"]}`),
			},
			want: "126c4edda5743fad8aff1659251801c083e0ea978846cad0b0f42a2b6825c888",
		},
		{
			name: "unicode, line separators, empty fields, weighted policy",
			overlay: Overlay{
				Network: "netz-ünïcode-日本", LocalSite: "site‑a\u2028line\u2029sep", //nolint:gosmopolitan // deliberate non-ASCII round-trip fixture
				Candidates: []Candidate{
					{
						Cluster: "cls", Kind: "mcp_tool", Name: "tügel — 名称", //nolint:gosmopolitan // deliberate non-ASCII round-trip fixture
						Site: "s", Fresh: false,
						Credential: &Credential{Strategy: "oauth2", SecretRef: SecretRef{Name: "n/a?b&c=d", Namespace: "", Key: ""}},
					},
					{
						Cluster: "", Kind: "", Name: "🚀", Site: "s2", Fresh: true,
						Credential: &Credential{Strategy: "", SecretRef: SecretRef{Name: "x", Namespace: "y", Key: "z"}},
					},
				},
				SelectionPolicy: json.RawMessage(`{"groups":["b","a"],"mode":"weighted","weights":{"a":10,"b":2}}`),
			},
			want: "87ba1cb6a473cff5dccd31740498e008ce83ead15f6a7a366017a6e758900d49",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ComputeDigest(tc.overlay)
			if err != nil {
				t.Fatal(err)
			}
			if got != tc.want {
				t.Errorf("digest mismatch — byte-compat with praxis is broken:\n got %s\nwant %s", got, tc.want)
			}
		})
	}
}

func TestRender_SelectionPolicyOnWire(t *testing.T) {
	set := routeSet(resolver.ModelRoutes{ModelRef: "ns1/m1", Routes: []resolver.Route{
		route("m1", "p1", 1),
		route("m1", "p2", 1),
	}})
	env, err := Render(set, scope(), Revision{}, Options{})
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	raw, err := json.Marshal(env.Overlay)
	if err != nil {
		t.Fatalf("marshal overlay: %v", err)
	}
	if !strings.Contains(string(raw), `"selection_policy":{"mode":"random"}`) {
		t.Errorf("equal-provider policy must serialize: %s", raw)
	}
	for i, candidate := range env.Overlay.Candidates {
		if candidate.SelectionGroup == nil || *candidate.SelectionGroup != 0 {
			t.Errorf("candidate %d selection_group = %v, want numeric zero", i, candidate.SelectionGroup)
		}
	}
	// A singleton derives no policy and remains deterministic.
	singleton := routeSet(resolver.ModelRoutes{ModelRef: "ns1/m1", Routes: []resolver.Route{route("m1", "p1", 1)}})
	env2, err := Render(singleton, scope(), Revision{}, Options{})
	if err != nil {
		t.Fatalf("Render singleton: %v", err)
	}
	raw2, err := json.Marshal(env2.Overlay)
	if err != nil {
		t.Fatalf("marshal overlay: %v", err)
	}
	if strings.Contains(string(raw2), "selection_policy") {
		t.Errorf("empty policy must be omitted: %s", raw2)
	}
}

func TestCheckRevisionTransition(t *testing.T) {
	cases := []struct {
		name    string
		prev    Revision
		next    Revision
		wantErr bool
	}{
		{"first distribution", Revision{}, Revision{Generation: 1, Digest: "a"}, false},
		{"stable", Revision{Generation: 3, Digest: "a"}, Revision{Generation: 3, Digest: "a"}, false},
		{"valid bump", Revision{Generation: 3, Digest: "a"}, Revision{Generation: 4, Digest: "b"}, false},
		{"zero generation", Revision{Generation: 3, Digest: "a"}, Revision{Generation: 0, Digest: "b"}, true},
		{"negative previous generation", Revision{Generation: -1, Digest: "a"}, Revision{Generation: 1, Digest: "b"}, true},
		{"negative next generation", Revision{Generation: 1, Digest: "a"}, Revision{Generation: -1, Digest: "b"}, true},
		{"same digest re-bumped", Revision{Generation: 3, Digest: "a"}, Revision{Generation: 4, Digest: "a"}, true},
		{"changed digest flat", Revision{Generation: 3, Digest: "a"}, Revision{Generation: 3, Digest: "b"}, true},
		{"changed digest regressed", Revision{Generation: 3, Digest: "a"}, Revision{Generation: 2, Digest: "b"}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := CheckRevisionTransition(tc.prev, tc.next)
			if tc.wantErr && !errors.Is(err, ErrGenerationRegression) {
				t.Fatalf("want ErrGenerationRegression, got %v", err)
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected: %v", err)
			}
		})
	}
}

// Round-trip: the emitted document must marshal with the exact top-level
// field names the praxis consumer parses.
func TestRender_TopLevelWireFields(t *testing.T) {
	set := routeSet(resolver.ModelRoutes{ModelRef: "ns1/m1", Routes: []resolver.Route{route("m1", "p1", 1)}})
	env, err := Render(set, scope(), Revision{}, Options{RenderedAt: "2026-09-03T00:00:00Z"})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}
	var generic map[string]json.RawMessage
	if err := json.Unmarshal(raw, &generic); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"schema_version", "revision", "content_digest", "scope", "provenance", "overlay"} {
		if _, ok := generic[k]; !ok {
			t.Errorf("missing top-level field %q in %s", k, raw)
		}
	}
}
