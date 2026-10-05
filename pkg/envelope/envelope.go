// Package envelope renders the praxis routing overlay (routing-overlay.json
// v1) from a resolved route set. Pure functions: no Kubernetes clients, no
// clocks unless injected, no hidden state — the reconciler owns persistence.
//
// The wire contract is pinned from praxis-ai origin/main (#540 envelope +
// #731 selection groups + #386 trust boundary). The revision is content
// addressed: RFC 8785 (JCS) SHA-256 over network + local_site + ordered
// candidates; praxis recomputes it and rejects mismatches, so the digest
// computation here must stay byte-compatible with the Rust side — enforced
// by cross-language golden vectors (port plan R3/M1).
package envelope

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/gowebpki/jcs"

	"github.com/opendatahub-io/ai-gateway-controller/pkg/resolver"
)

// SchemaVersion is the envelope shape this renderer emits.
const SchemaVersion = "1.0.0"

// Scope identifies the serving target; it must satisfy the consuming
// pipeline's expected_overlay_scope (we render to it, never widen it).
type Scope struct {
	Network   string `json:"network"`
	Gateway   string `json:"gateway"`
	Namespace string `json:"namespace"`
	LocalSite string `json:"local_site"`
}

// Revision is the content-addressed revision of a distributed envelope —
// the internal distribution state the reconciler tracks (ConfigMap
// annotation), not the wire shape. The monotonicity obligation ships with
// the type: Generation is strictly positive and strictly increases whenever
// Digest changes. On the wire it decomposes into RevisionField (value) and
// provenance.source_generation (generation) — see overlay.rs in praxis-ai.
type Revision struct {
	Generation uint64
	Digest     string
}

// RevisionField is the wire revision object. Praxis validates the shape:
// kind must be "content_addressed", algorithm "sha256", value 64 lowercase
// hex, and value must equal content_digest.value.
type RevisionField struct {
	Kind      string `json:"kind"`
	Algorithm string `json:"algorithm"`
	Value     string `json:"value"`
}

// SecretRef points at the credential Secret; refs never bytes (praxis #386).
type SecretRef struct {
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
	Key       string `json:"key"`
}

// Credential is the final-hop credential reference. Field set is exact:
// the consumer rejects unknown fields inside credential objects.
type Credential struct {
	Strategy  string    `json:"strategy"`
	SecretRef SecretRef `json:"secretRef"`
}

// Candidate is one routable capability instance.
//
// Credential is optional: the overlay consumer treats an absent credential as
// "no reference" (validate_credential in praxis-ai descriptor.rs), and the
// wire vocabulary cannot yet express every CRD auth type (see StrategyFor),
// so mislabeling would be a lie the digest happily hashes.
type Candidate struct {
	Cluster string `json:"cluster"`
	// StableID is the trusted provider-handoff value emitted by ExtProc.
	// It is explicit so the Envoy route match and the overlay use one
	// content-addressed identity rather than separate derivations.
	StableID   string      `json:"stable_id,omitempty"`
	Kind       string      `json:"kind"` // inference_model | mcp_tool
	Name       string      `json:"name"`
	Site       string      `json:"site"`
	Fresh      bool        `json:"fresh"`
	Credential *Credential `json:"credential,omitempty"`
}

// Provenance records who rendered this envelope and from what source state.
type Provenance struct {
	Producer         string `json:"producer"`
	ProducerVersion  string `json:"producer_version"`
	SourceName       string `json:"source_name"`
	SourceUID        string `json:"source_uid"`
	SourceGeneration uint64 `json:"source_generation"`
	RenderedAt       string `json:"rendered_at"` // RFC3339
}

// Envelope is the routing-overlay.json v1 document.
type Envelope struct {
	SchemaVersion string        `json:"schema_version"`
	Revision      RevisionField `json:"revision"`
	ContentDigest Digest        `json:"content_digest"`
	Scope         Scope         `json:"scope"`
	Provenance    Provenance    `json:"provenance"`
	Overlay       Overlay       `json:"overlay"`
}

// Digest names the algorithm so consumers can refuse unknown hash schemes.
type Digest struct {
	Algorithm string `json:"algorithm"`
	Value     string `json:"value"`
}

// Overlay is the swappable payload: scope-independent serving facts.
// SelectionPolicy is the picker policy for selection groups (praxis #731);
// its exact wire shape is pinned with the M1 selection-group work (Q3
// lands there), so it rides as raw JSON. When present it participates in
// the digest — matching compute_semantic_digest in praxis-ai overlay.rs,
// which hashes it iff present.
type Overlay struct {
	Network         string          `json:"network"`
	LocalSite       string          `json:"local_site"`
	Candidates      []Candidate     `json:"candidates"`
	SelectionPolicy json.RawMessage `json:"selection_policy,omitempty"`
}

// Frozen error surface (port plan §6): each maps to a Ready=False condition
// with a stable reason, and is what E2E failure-injection asserts against.
var (
	ErrWeightUnsupported     = errors.New("envelope: non-uniform (proportional) weights are not supported in 3.6; use uniform weights or a single provider (proportional canaries: known 3.6 limitation, see ADR 0001 D5)")
	ErrUnknownCluster        = errors.New("envelope: candidate references cluster absent from load_balancer config")
	ErrScopeMismatch         = errors.New("envelope: scope fields must be non-empty")
	ErrGenerationRegression  = errors.New("envelope: source_generation must strictly increase on content change")
	ErrUnsupportedCredential = errors.New("envelope: provider credential semantics are not supported")
)

// Options carries the non-derivable inputs so Render stays pure.
type Options struct {
	// KnownClusters is an optional administrative load_balancer cluster
	// allowlist. Empty derives the cluster set from the resolved provider
	// routes; when supplied, it remains an upper-bound validation check.
	KnownClusters []string
	// SourceUID uniquely identifies the source of truth (e.g. cluster+ns).
	SourceUID string
	// ProducerVersion is stamped by the binary (ldflags), default "dev".
	ProducerVersion string
	// RenderedAt is injected (RFC3339) to keep Render deterministic under test.
	RenderedAt string
	// SelectionPolicy stamps the overlay picker policy (praxis #731). Its
	// exact shape is pinned with the M1 selection-group work; nil omits
	// the field (deterministic first-admitted default).
	SelectionPolicy json.RawMessage
}

// Render validates the resolved route set and emits the envelope with its
// revision. Weight guard (port plan R1): within a model, after resolver
// dropped weight<=0 refs, all surviving weights must be pairwise equal
// (nil normalized to 1 upstream) — a non-uniform group is refused loudly
// with ErrWeightUnsupported so a weighted canary fails at apply time
// instead of silently routing evenly.
//
// Revision rule: digest equal to prev → prev returned unchanged (no bump);
// digest changed → Generation = prev.Generation + 1.
func Render(routes *resolver.ResolvedRouteSet, scope Scope, prev Revision, opts Options) (Envelope, error) {
	if scope.Network == "" || scope.Gateway == "" || scope.Namespace == "" || scope.LocalSite == "" {
		return Envelope{}, fmt.Errorf("%w: %+v", ErrScopeMismatch, scope)
	}
	if routes == nil {
		return Envelope{}, fmt.Errorf("%w: nil route set", ErrScopeMismatch)
	}

	candidates := make([]Candidate, 0)
	known := make(map[string]bool, len(opts.KnownClusters))
	for _, c := range opts.KnownClusters {
		known[c] = true
	}

	for _, m := range routes.Models {
		if err := checkUniformWeights(m); err != nil {
			return Envelope{}, err
		}
		for _, r := range m.Routes {
			if len(known) > 0 && !known[r.Cluster] {
				return Envelope{}, fmt.Errorf("%w: %s (model %s)", ErrUnknownCluster, r.Cluster, r.Model)
			}
			// Candidate names are the client-visible model identity.  This must
			// match resolver.Route.ClientName and the HTTPRoute body/header
			// matches; the ExternalModel object name is control-plane identity.
			//
			// StableID is scoped to the model-provider pair so two models
			// sharing the same provider produce distinct overlay candidates.
			// Praxis validates stable_id uniqueness and rejects overlays with
			// duplicates, which would cause the second model to get 404/503.
			// The Cluster stays shared (one transport per provider).
			cand := Candidate{
				Cluster:  r.Cluster,
				StableID: CandidateStableID(r.Model, r.Provider),
				Kind:     "inference_model",
				Name:     r.ClientName,
				Site:     scope.LocalSite,
				Fresh:    true,
			}
			strategy, err := StrategyFor(r)
			if err != nil {
				return Envelope{}, fmt.Errorf("model %s provider %s: %w", r.Model, r.Provider, err)
			}
			if strategy != "" {
				cand.Credential = &Credential{
					Strategy: strategy,
					SecretRef: SecretRef{
						Name:      r.SecretName,
						Namespace: r.Namespace,
						Key:       r.SecretKey,
					},
				}
			}
			candidates = append(candidates, cand)
		}
	}

	overlay := Overlay{
		Network:         scope.Network,
		LocalSite:       scope.LocalSite,
		Candidates:      candidates,
		SelectionPolicy: opts.SelectionPolicy,
	}
	digest, err := ComputeDigest(overlay)
	if err != nil {
		return Envelope{}, err
	}

	rev := Revision{Generation: 1, Digest: digest}
	if prev.Digest == digest && prev.Generation > 0 {
		rev = prev // no content change, no generation bump
	} else if prev.Generation > 0 {
		rev = Revision{Generation: prev.Generation + 1, Digest: digest}
	}

	pv := opts.ProducerVersion
	if pv == "" {
		pv = "dev"
	}
	return Envelope{
		SchemaVersion: SchemaVersion,
		Revision:      RevisionField{Kind: "content_addressed", Algorithm: "sha256", Value: digest},
		ContentDigest: Digest{Algorithm: "sha256", Value: digest},
		Scope:         scope,
		Provenance: Provenance{
			Producer:         "ai-gateway-controller",
			ProducerVersion:  pv,
			SourceName:       scope.Network,
			SourceUID:        opts.SourceUID,
			SourceGeneration: rev.Generation,
			RenderedAt:       opts.RenderedAt,
		},
		Overlay: overlay,
	}, nil
}

// StrategyFor maps only the currently qualified provider API-key contracts to
// Praxis wire strategies: openai + openai-chat maps to bearer_token
// (Authorization: Bearer at the provider boundary) and anthropic + messages
// maps to apikey (the credential_inject filter injects the Secret value into
// its configured header, x-api-key by default). The wire stays reference-only
// either way. Provider API keys are not interchangeable: other provider/
// API-format combinations fail closed until their header semantics are
// explicitly supported.
//
// This is the single qualification authority: Render stamps overlay
// candidates with the strategy this function returns, and the tenant
// reconciler preloads unselected bindings into the ExtProc runtime
// configuration through it as well. Both consumers must agree because the
// data plane rejects a candidate whose strategy differs from the configured
// credential entry (praxis-ai #1172); disagreement fails closed with a 503.
func StrategyFor(route resolver.Route) (string, error) {
	switch route.AuthType {
	case "":
		return "", nil
	case "apikey":
		if route.ProviderType == "openai" && route.APIFormat == "openai-chat" {
			return "bearer_token", nil
		}
		if route.ProviderType == "anthropic" && route.APIFormat == "messages" {
			return "apikey", nil
		}
		return "", fmt.Errorf("%w: auth.type apikey for provider %q and API format %q", ErrUnsupportedCredential, route.ProviderType, route.APIFormat)
	case "sigv4", "oauth2":
		return "", fmt.Errorf("%w: auth.type %q", ErrUnsupportedCredential, route.AuthType)
	default:
		return "", fmt.Errorf("envelope: unknown auth.type %q", route.AuthType)
	}
}

// CandidateStableID returns the canonical stable_id for a model-provider
// pair. It is the single derivation authority: the overlay renderer, the
// tenant ExtProc runtime preload, and the HTTPRoute header match must all
// use this function to stay in agreement. The value is scoped to the model
// so two models sharing the same provider produce distinct candidates.
func CandidateStableID(model, provider string) string {
	return model + "/provider-" + provider
}

// checkUniformWeights enforces the R1 guard for one model group.
func checkUniformWeights(m resolver.ModelRoutes) error {
	if len(m.Routes) == 0 {
		return nil // model renders no candidates; skips are the record
	}
	first := m.Routes[0].Weight
	for _, r := range m.Routes[1:] {
		if r.Weight != first {
			return fmt.Errorf("%w: model %s has weights %d and %d across providers",
				ErrWeightUnsupported, m.ModelRef, first, r.Weight)
		}
	}
	return nil
}

// ComputeDigest returns the RFC 8785 (JCS) SHA-256 hex digest praxis
// recomputes on load — a port of compute_semantic_digest in praxis-ai
// overlay.rs: take {candidates, local_site, network} plus selection_policy
// iff present, canonicalize per RFC 8785, SHA-256, lowercase hex.
// Byte-compatibility with the Rust consumer is pinned by cross-language
// golden vectors (port plan R3, M1 acceptance).
func ComputeDigest(overlay Overlay) (string, error) {
	digestInput := struct {
		Candidates      []Candidate     `json:"candidates"`
		LocalSite       string          `json:"local_site"`
		Network         string          `json:"network"`
		SelectionPolicy json.RawMessage `json:"selection_policy,omitempty"`
	}{overlay.Candidates, overlay.LocalSite, overlay.Network, overlay.SelectionPolicy}
	if digestInput.Candidates == nil {
		digestInput.Candidates = []Candidate{}
	}
	payload, err := json.Marshal(digestInput)
	if err != nil {
		return "", fmt.Errorf("envelope: marshal digest input: %w", err)
	}
	canon, err := jcs.Transform(payload)
	if err != nil {
		return "", fmt.Errorf("envelope: JCS canonicalize: %w", err)
	}
	sum := sha256.Sum256(canon)
	return hex.EncodeToString(sum[:]), nil
}

// ComputeDigestFromWire recomputes the semantic digest of a routing-overlay
// wire document as the praxis consumer does — a port of compute_semantic_
// digest in praxis-ai overlay.rs operating on the RAW parsed value, filtered
// to candidates + local_site + network (+ selection_policy iff present).
//
// Unlike ComputeDigest it sees fields our structs do not model: unknown
// additive candidate fields (stable_id, rank, ...) participate in the digest
// praxis hashes, and overlay-level fields outside the four keys (e.g.
// generated_at) do not. It is the independent verifier: audit and read-back
// paths recompute digests over bytes they did not render, and the M1 golden
// vectors (test/testdata/overlay-contract/v1, pinned from praxis fixtures) assert
// this path agrees with the Rust consumer byte for byte.
//
// Shape detection mirrors praxis: a document with a schema_version field is
// an envelope (digest computed over its overlay section); without one it is
// a legacy bare overlay payload.
func ComputeDigestFromWire(raw []byte) (string, error) {
	var doc map[string]json.RawMessage
	if err := json.Unmarshal(raw, &doc); err != nil {
		return "", fmt.Errorf("envelope: wire document is not a JSON object: %w", err)
	}
	overlay := doc
	if _, isEnvelope := doc["schema_version"]; isEnvelope {
		inner, ok := doc["overlay"]
		if !ok {
			return "", errors.New("envelope: envelope document has no overlay section")
		}
		if err := json.Unmarshal(inner, &overlay); err != nil {
			return "", fmt.Errorf("envelope: overlay section is not a JSON object: %w", err)
		}
	}
	filtered := make(map[string]json.RawMessage, 4)
	for _, k := range []string{"candidates", "local_site", "network", "selection_policy"} {
		if v, ok := overlay[k]; ok {
			filtered[k] = v
		}
	}
	payload, err := json.Marshal(filtered)
	if err != nil {
		return "", fmt.Errorf("envelope: marshal wire digest input: %w", err)
	}
	canon, err := jcs.Transform(payload)
	if err != nil {
		return "", fmt.Errorf("envelope: JCS canonicalize: %w", err)
	}
	sum := sha256.Sum256(canon)
	return hex.EncodeToString(sum[:]), nil
}

// CheckRevisionTransition validates a candidate revision against the one
// currently distributed (read from the ConfigMap annotation by the caller).
// It enforces the monotonicity obligation that Render relies on:
// same digest → same generation; changed digest → strictly greater.
func CheckRevisionTransition(prev, next Revision) error {
	switch {
	case next.Generation == 0:
		return fmt.Errorf("%w: generation must be positive", ErrGenerationRegression)
	case prev.Generation == 0:
		return nil // first distribution
	case next.Digest == prev.Digest && next.Generation != prev.Generation:
		return fmt.Errorf("%w: same digest, generation %d -> %d", ErrGenerationRegression, prev.Generation, next.Generation)
	case next.Digest != prev.Digest && next.Generation <= prev.Generation:
		return fmt.Errorf("%w: %d -> %d on content change", ErrGenerationRegression, prev.Generation, next.Generation)
	}
	return nil
}
