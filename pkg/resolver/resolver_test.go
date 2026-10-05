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
		Ref:         v1alpha1.NameReference{Name: name},
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
