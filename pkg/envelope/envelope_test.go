package envelope

import (
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"testing"

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
		ProducerVersion: "0.1.0", RenderedAt: "2026-09-03T00:00:00Z",
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
		{"azure with openai chat format", "azure", "openai-chat"},
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

func TestRender_WeightGuard(t *testing.T) {
	cases := []struct {
		name    string
		weights []int
		wantErr bool
	}{
		{"uniform nil-normalized", []int{1, 1}, false},
		{"uniform equal nonzero", []int{2, 2}, false},
		{"single provider any weight", []int{7}, false},
		{"non-uniform 70/30", []int{70, 30}, true},
		{"non-uniform 2/1", []int{2, 1}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var routes []resolver.Route
			for i, w := range tc.weights {
				routes = append(routes, route("m1", string(rune('a'+i)), w))
			}
			_, err := Render(routeSet(resolver.ModelRoutes{ModelRef: "ns1/m1", Routes: routes}), scope(), Revision{}, Options{})
			if tc.wantErr {
				if !errors.Is(err, ErrWeightUnsupported) {
					t.Fatalf("weights %v: want ErrWeightUnsupported, got %v", tc.weights, err)
				}
				if !strings.Contains(err.Error(), "ns1/m1") {
					t.Errorf("error must name the model: %v", err)
				}
			} else if err != nil {
				t.Fatalf("weights %v: unexpected error %v", tc.weights, err)
			}
		})
	}
}

func TestRender_WeightGuardIsPerModel(t *testing.T) {
	// Model A uniform, model B non-uniform → whole render refused (one
	// envelope, one source — a bad model must not ship a partial envelope).
	set := routeSet(
		resolver.ModelRoutes{ModelRef: "ns1/a", Routes: []resolver.Route{route("a", "p1", 1), route("a", "p2", 1)}},
		resolver.ModelRoutes{ModelRef: "ns1/b", Routes: []resolver.Route{route("b", "p1", 7), route("b", "p2", 3)}},
	)
	_, err := Render(set, scope(), Revision{}, Options{})
	if !errors.Is(err, ErrWeightUnsupported) {
		t.Fatalf("want ErrWeightUnsupported from model b, got %v", err)
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
	set := routeSet(resolver.ModelRoutes{ModelRef: "ns1/m1", Routes: []resolver.Route{route("m1", "p1", 1)}})
	env, err := Render(set, scope(), Revision{}, Options{SelectionPolicy: json.RawMessage(`{"mode":"random"}`)})
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	raw, err := json.Marshal(env.Overlay)
	if err != nil {
		t.Fatalf("marshal overlay: %v", err)
	}
	if !strings.Contains(string(raw), `"selection_policy":{"mode":"random"}`) {
		t.Errorf("selection_policy must serialize verbatim: %s", raw)
	}
	// absent by default
	env2, _ := Render(set, scope(), Revision{}, Options{})
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
