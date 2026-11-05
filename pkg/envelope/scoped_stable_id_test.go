package envelope

import (
	"strings"
	"testing"

	"github.com/opendatahub-io/ai-gateway-controller/pkg/resolver"
)

func TestScopedStableIDFormsAreDisjointFromExistingForm(t *testing.T) {
	existing := CandidateStableID("model", "openai")
	scoped := ScopedStableID("team", "model", "team", "openai")
	if scoped != "team/model/team/openai" {
		t.Fatalf("scoped stable_id = %q", scoped)
	}
	long := strings.Repeat("m", 253)
	hashed := ScopedStableID(strings.Repeat("n", 63), long, strings.Repeat("p", 63), long)
	if !strings.HasPrefix(hashed, "sha256:") || len(hashed) != len("sha256:")+64 || len(hashed) > maxStableIDBytes {
		t.Fatalf("over-limit stable_id = %q", hashed)
	}
	if IsScopedStableID(existing) || !IsScopedStableID(scoped) || !IsScopedStableID(hashed) {
		t.Fatalf("form detection: existing=%t scoped=%t hashed=%t", IsScopedStableID(existing), IsScopedStableID(scoped), IsScopedStableID(hashed))
	}
	// Length prefixes keep field boundaries in the hash input.
	if ScopedStableID("a", long+"b", "c", long) == ScopedStableID("a", long, "bc", long) {
		t.Fatal("hash form must not depend on field concatenation only")
	}
}

func TestRenderUsesRouteStableIDOverride(t *testing.T) {
	scoped := route("model", "openai", 1)
	scoped.StableID = ScopedStableID("team", "model", "team", "openai")
	existing := route("other", "openai", 1)
	env, err := Render(routeSet(
		resolver.ModelRoutes{ModelRef: "team/model", Routes: []resolver.Route{scoped}},
		resolver.ModelRoutes{ModelRef: "team/other", Routes: []resolver.Route{existing}},
	), scope(), Revision{}, Options{})
	if err != nil {
		t.Fatal(err)
	}
	candidates := env.Overlay.Candidates
	if len(candidates) != 2 || candidates[0].StableID != scoped.StableID || candidates[1].StableID != CandidateStableID("other", "openai") {
		t.Fatalf("candidates = %#v, want the scoped override and the existing form", candidates)
	}
}

func TestRenderUsesCredentialCopiesAndPreservedCandidates(t *testing.T) {
	r := route("model", "openai", 1)
	r.StableID = ScopedStableID("team", "model", "team", "openai")
	r.SecretName, r.SecretNamespace = "agc-cred-0123456789ab", "tenant-b"
	preserved := Candidate{Cluster: "provider-own", StableID: "own/provider-own", Kind: "inference_model", Name: "own", Site: "site-a", Fresh: true}
	env, err := Render(routeSet(resolver.ModelRoutes{ModelRef: "team/model", Routes: []resolver.Route{r}}), scope(), Revision{},
		Options{PreservedCandidates: []Candidate{preserved}})
	if err != nil {
		t.Fatal(err)
	}
	candidates := env.Overlay.Candidates
	if len(candidates) != 2 || candidates[0] != preserved {
		t.Fatalf("candidates = %#v, want the preserved candidate first", candidates)
	}
	if candidates[1].Credential.SecretRef != (SecretRef{Name: r.SecretName, Namespace: "tenant-b", Key: "api-key"}) {
		t.Fatalf("rendered credential = %#v, want the tenant-namespace copy", candidates[1].Credential)
	}
}

func TestPreservedSelectionGroupsKeepTheirPolicy(t *testing.T) {
	group := uint32(0)
	preserved := []Candidate{
		{Cluster: "provider-a", StableID: "own/provider-a", Kind: "inference_model", Name: "own", Site: "site-a", Fresh: true, SelectionGroup: &group},
		{Cluster: "provider-b", StableID: "own/provider-b", Kind: "inference_model", Name: "own", Site: "site-a", Fresh: true, SelectionGroup: &group},
	}
	attached := route("model", "openai", 1)
	attached.StableID = ScopedStableID("team", "model", "team", "openai")
	env, err := Render(routeSet(resolver.ModelRoutes{ModelRef: "team/model", Routes: []resolver.Route{attached}}), scope(), Revision{},
		Options{PreservedCandidates: preserved})
	if err != nil {
		t.Fatal(err)
	}
	if string(env.Overlay.SelectionPolicy) != `{"mode":"random"}` {
		t.Fatalf("selection policy = %s, want the preserved groups' random policy", env.Overlay.SelectionPolicy)
	}
}

func TestValidateModelMatchesRenderRejections(t *testing.T) {
	unqualified := route("model", "openai", 1)
	unqualified.APIFormat = "messages"
	for name, m := range map[string]resolver.ModelRoutes{
		"credential": {ModelRef: "team/model", Routes: []resolver.Route{unqualified}},
		"duplicate":  {ModelRef: "team/model", Routes: []resolver.Route{route("model", "a", 1), route("model", "a", 1)}},
	} {
		if err := ValidateModel(m, Options{}); err == nil {
			t.Fatalf("%s: ValidateModel accepted a model Render rejects", name)
		}
		if _, err := Render(routeSet(m), scope(), Revision{}, Options{}); err == nil {
			t.Fatalf("%s: Render accepted the model", name)
		}
	}
	unequal := resolver.ModelRoutes{ModelRef: "team/model", Routes: []resolver.Route{route("model", "a", 1), route("model", "b", 2)}}
	if err := ValidateModel(unequal, Options{}); err != nil {
		t.Fatalf("unequal weights must be accepted: %v", err)
	}
	if _, err := Render(routeSet(unequal), scope(), Revision{}, Options{}); err != nil {
		t.Fatalf("unequal weights must render: %v", err)
	}
	if err := ValidateModel(resolver.ModelRoutes{Routes: []resolver.Route{route("model", "a", 1)}}, Options{KnownClusters: []string{"provider-b"}}); err == nil {
		t.Fatal("ValidateModel accepted an unknown cluster")
	}
}

func TestPreservedCandidatesNormalizeUnderWeightedRandom(t *testing.T) {
	group := uint32(0)
	preserved := []Candidate{
		{Cluster: "provider-a", StableID: "own/provider-a", Kind: "inference_model", Name: "own", Site: "site-a", Fresh: true, SelectionGroup: &group},
		{Cluster: "provider-b", StableID: "own/provider-b", Kind: "inference_model", Name: "own", Site: "site-a", Fresh: true, SelectionGroup: &group},
	}
	attached := route("model", "openai", 80)
	attached.StableID = ScopedStableID("team", "model", "team", "openai")
	second := route("model", "azure", 20)
	second.StableID = ScopedStableID("team", "model", "team", "azure")
	env, err := Render(routeSet(resolver.ModelRoutes{ModelRef: "team/model", Routes: []resolver.Route{attached, second}}), scope(), Revision{},
		Options{PreservedCandidates: preserved})
	if err != nil {
		t.Fatal(err)
	}
	if string(env.Overlay.SelectionPolicy) != `{"mode":"weightedRandom"}` {
		t.Fatalf("selection_policy = %s, want weightedRandom", env.Overlay.SelectionPolicy)
	}
	for i, candidate := range env.Overlay.Candidates {
		if candidate.SelectionGroup == nil || candidate.TrafficWeight == nil {
			t.Fatalf("candidate %d missing weightedRandom fields: %+v", i, candidate)
		}
	}
	if *env.Overlay.Candidates[0].TrafficWeight != 1 || *env.Overlay.Candidates[1].TrafficWeight != 1 {
		t.Fatalf("preserved equal-weight candidates default traffic_weight = %v/%v, want 1/1",
			env.Overlay.Candidates[0].TrafficWeight, env.Overlay.Candidates[1].TrafficWeight)
	}
}
