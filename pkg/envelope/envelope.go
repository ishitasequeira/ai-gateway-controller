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
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"

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
	Generation int64
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

// Credential is the final-hop credential reference. The wire field set is
// exact: the consumer rejects unknown fields inside credential objects, so
// Header never serializes (json:"-"). Header is a controller-internal carrier
// for the provider-boundary request header the credential_inject filter must
// inject into (see defaultAuthHeaders); it threads from StrategyFor into the
// ExtProc credential entry (pkg/tenant/extproc.go), never onto the overlay.
type Credential struct {
	Strategy  string    `json:"strategy"`
	Header    string    `json:"-"`
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
	StableID string `json:"stable_id,omitempty"`
	Kind     string `json:"kind"` // inference_model | mcp_tool
	Name     string `json:"name"`
	Site     string `json:"site"`
	Fresh    bool   `json:"fresh"`
	// SelectionGroup is scoped by (kind, name). A pointer is required so
	// group zero is emitted instead of being omitted by encoding/json.
	SelectionGroup *uint32 `json:"selection_group,omitempty"`
	// TrafficWeight is the Praxis weightedRandom candidate weight (1–1000).
	// CRD ExternalProviderRef.weight is 0–100; AGC pass-throughs positive
	// values. Omitted unless selection_policy.mode is weightedRandom — Praxis
	// rejects traffic_weight with any other mode, and requires it on every
	// candidate when mode is weightedRandom.
	TrafficWeight *uint32     `json:"traffic_weight,omitempty"`
	Credential    *Credential `json:"credential,omitempty"`
	APIFormat     string      `json:"api_format,omitempty"`
	Path          string      `json:"path,omitempty"`
	TargetModel   string      `json:"target_model,omitempty"`
}

// Provenance records who rendered this envelope and from what source state.
type Provenance struct {
	Producer         string `json:"producer"`
	ProducerVersion  string `json:"producer_version"`
	SourceName       string `json:"source_name"`
	SourceUID        string `json:"source_uid"`
	SourceGeneration int64  `json:"source_generation"`
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
// SelectionPolicy is the picker policy for selection groups (praxis #731).
// When present it participates in the digest — matching
// compute_semantic_digest in praxis-ai overlay.rs, which hashes it iff
// present.
type Overlay struct {
	Network         string          `json:"network"`
	LocalSite       string          `json:"local_site"`
	Candidates      []Candidate     `json:"candidates"`
	SelectionPolicy json.RawMessage `json:"selection_policy,omitempty"`
}

// Frozen error surface (port plan §6): each maps to a Ready=False condition
// with a stable reason, and is what E2E failure-injection asserts against.
var (
	ErrDuplicateProvider     = errors.New("envelope: a client model cannot bind the same provider more than once")
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
	// IncludeProviderProfiles enables the newer Praxis AI profile fields used
	// for API translation. Keep this false for flag-off overlays so older
	// intelligent_route consumers receive the same candidate shape as main.
	IncludeProviderProfiles bool
	// PreservedCandidates are already-published candidates kept verbatim
	// ahead of the rendered routes. A tenant's own candidates are preserved
	// this way when only contributions from other namespaces change.
	PreservedCandidates []Candidate
}

// Render validates the resolved route set and emits the envelope with its
// revision. Distinct providers with equal positive weights sharing a
// client-visible model name are assigned selection group zero and use
// Praxis' global random policy. Unequal positive weights use weightedRandom
// with per-candidate traffic_weight (CRD weight pass-through). Singleton
// model capabilities remain ungrouped and deterministic unless the overlay
// must use weightedRandom (Praxis requires traffic_weight and selection_group
// on every candidate in that mode). Repeated providers for one client model
// are rejected.
//
// Revision rule: digest equal to prev → prev returned unchanged (no bump);
// digest changed → Generation = prev.Generation + 1.
func Render(routes *resolver.ResolvedRouteSet, scope Scope, prev Revision, opts Options) (Envelope, error) {
	if prev.Generation < 0 {
		return Envelope{}, fmt.Errorf("%w: previous generation cannot be negative: %d", ErrGenerationRegression, prev.Generation)
	}
	if scope.Network == "" || scope.Gateway == "" || scope.Namespace == "" || scope.LocalSite == "" {
		return Envelope{}, fmt.Errorf("%w: %+v", ErrScopeMismatch, scope)
	}
	if routes == nil {
		return Envelope{}, fmt.Errorf("%w: nil route set", ErrScopeMismatch)
	}
	selection, err := deriveSelectionMetadata(routes)
	if err != nil {
		return Envelope{}, err
	}
	preservedNeedsWeighted := slices.ContainsFunc(opts.PreservedCandidates, func(c Candidate) bool { return c.TrafficWeight != nil })
	preservedNeedsRandom := slices.ContainsFunc(opts.PreservedCandidates, func(c Candidate) bool { return c.SelectionGroup != nil })
	if selection.weighted || preservedNeedsWeighted {
		selection.policy = weightedRandomSelectionPolicy
		selection.weighted = true
	} else if selection.policy == nil && preservedNeedsRandom {
		selection.policy = randomSelectionPolicy
	}

	candidates := append(make([]Candidate, 0, len(opts.PreservedCandidates)), opts.PreservedCandidates...)
	for _, m := range routes.Models {
		if err := ValidateModel(m, opts); err != nil {
			return Envelope{}, err
		}
		for _, r := range m.Routes {
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
				StableID: RouteStableID(r),
				Kind:     "inference_model",
				Name:     r.ClientName,
				Site:     scope.LocalSite,
				Fresh:    true,
			}
			if opts.IncludeProviderProfiles {
				cand.APIFormat = r.APIFormat
				cand.Path = r.Path
				cand.TargetModel = r.TargetModel
			}
			key := selectionRouteKey{modelRef: m.ModelRef, model: r.Model, provider: r.Provider, cluster: r.Cluster}
			if group, ok := selection.groups[key]; ok {
				group := group
				cand.SelectionGroup = &group
			}
			if weight, ok := selection.weights[key]; ok {
				weight := weight
				cand.TrafficWeight = &weight
			}
			// ValidateModel has already qualified the strategy.
			if strategy, header, _ := StrategyFor(r); strategy != "" {
				cand.Credential = &Credential{
					Strategy: strategy,
					Header:   header,
					SecretRef: SecretRef{
						Name:      r.SecretName,
						Namespace: r.SecretRefNamespace(),
						Key:       r.SecretKey,
					},
				}
			}
			candidates = append(candidates, cand)
		}
	}
	if selection.weighted {
		// Praxis weightedRandom requires selection_group and traffic_weight on
		// every candidate, including preserved foreign entries and singletons.
		normalizeWeightedCandidates(candidates)
	}
	if !opts.IncludeProviderProfiles {
		// Preserved candidates may have been published while translation was
		// enabled. Strip the versioned profile fields before hashing a flag-off
		// overlay so disabling the feature returns to the main wire contract.
		for i := range candidates {
			candidates[i].APIFormat = ""
			candidates[i].Path = ""
			candidates[i].TargetModel = ""
		}
	}

	overlay := Overlay{
		Network:         scope.Network,
		LocalSite:       scope.LocalSite,
		Candidates:      candidates,
		SelectionPolicy: selection.policy,
	}
	digest, err := ComputeDigest(overlay)
	if err != nil {
		return Envelope{}, err
	}

	rev := Revision{Generation: 1, Digest: digest}
	if prev.Digest == digest && prev.Generation > 0 {
		rev = prev // no content change, no generation bump
	} else if prev.Generation > 0 {
		if prev.Generation == math.MaxInt64 {
			return Envelope{}, fmt.Errorf("%w: generation exhausted at %d", ErrGenerationRegression, prev.Generation)
		}
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

type selectionRouteKey struct {
	modelRef string
	model    string
	provider string
	cluster  string
}

type selectionCapability struct {
	count       int
	firstWeight int
	unequal     bool
}

type selectionMetadata struct {
	policy   json.RawMessage
	weighted bool
	groups   map[selectionRouteKey]uint32
	weights  map[selectionRouteKey]uint32
}

// deriveSelectionMetadata is the single policy derivation used by both
// normal publication and deletion-time republishing (both pass through
// Render). Praxis scopes groups by (kind, name), so provider candidates for
// distinct client-visible names never share a group.
//
// Equal multi-provider capabilities keep mode random (no traffic_weight).
// Any unequal positive weights force weightedRandom for the whole overlay:
// Praxis allows only one selection_policy.mode and requires traffic_weight
// plus selection_group on every candidate in that mode.
func deriveSelectionMetadata(routes *resolver.ResolvedRouteSet) (selectionMetadata, error) {
	capabilities := map[string]*selectionCapability{}
	providerBindings := map[string]bool{}
	for _, model := range routes.Models {
		for _, route := range model.Routes {
			key := "inference_model\x00" + route.ClientName
			binding := key + "\x00" + route.ProviderNamespace + "/" + route.Provider
			if providerBindings[binding] {
				return selectionMetadata{}, fmt.Errorf("%w: client model %s provider %s", ErrDuplicateProvider, route.ClientName, route.Provider)
			}
			providerBindings[binding] = true
			capability := capabilities[key]
			if capability == nil {
				capability = &selectionCapability{firstWeight: route.Weight}
				capabilities[key] = capability
			} else if route.Weight != capability.firstWeight {
				capability.unequal = true
			}
			capability.count++
		}
	}
	needsWeighted := false
	needsRandom := false
	for _, capability := range capabilities {
		if capability.count <= 1 {
			continue
		}
		if capability.unequal {
			needsWeighted = true
		} else {
			needsRandom = true
		}
	}
	metadata := selectionMetadata{
		groups:  make(map[selectionRouteKey]uint32),
		weights: make(map[selectionRouteKey]uint32),
	}
	if needsWeighted {
		metadata.policy = weightedRandomSelectionPolicy
		metadata.weighted = true
		for _, model := range routes.Models {
			for _, route := range model.Routes {
				key := selectionRouteKey{modelRef: model.ModelRef, model: route.Model, provider: route.Provider, cluster: route.Cluster}
				metadata.groups[key] = 0
				metadata.weights[key] = trafficWeightFor(route.Weight)
			}
		}
		return metadata, nil
	}
	if needsRandom {
		metadata.policy = randomSelectionPolicy
		for _, model := range routes.Models {
			for _, route := range model.Routes {
				if capabilities["inference_model\x00"+route.ClientName].count > 1 {
					metadata.groups[selectionRouteKey{modelRef: model.ModelRef, model: route.Model, provider: route.Provider, cluster: route.Cluster}] = 0
				}
			}
		}
	}
	return metadata, nil
}

// trafficWeightFor maps a positive CRD/resolver weight onto Praxis
// traffic_weight. CRD max is 100; Praxis accepts 1–1000. Non-positive values
// should already be filtered by the resolver; clamp defensively.
func trafficWeightFor(weight int) uint32 {
	if weight < 1 {
		return 1
	}
	if weight > 1000 {
		return 1000
	}
	return uint32(weight)
}

// normalizeWeightedCandidates stamps the Praxis weightedRandom contract onto
// every candidate: selection_group and traffic_weight are required on all of
// them, including preserved foreign entries that previously used mode random.
func normalizeWeightedCandidates(candidates []Candidate) {
	for i := range candidates {
		if candidates[i].SelectionGroup == nil {
			group := uint32(0)
			candidates[i].SelectionGroup = &group
		}
		if candidates[i].TrafficWeight == nil {
			weight := uint32(1)
			candidates[i].TrafficWeight = &weight
		}
	}
}

// ValidateSelectionMetadata rejects invalid selection bindings (today:
// duplicate providers for one client-visible model). Render uses the same
// derivation for wire metadata; the controller calls this before applying
// transport resources so invalid configs do not change serving state.
func ValidateSelectionMetadata(routes *resolver.ResolvedRouteSet) error {
	if routes == nil {
		return fmt.Errorf("%w: nil route set", ErrScopeMismatch)
	}
	_, err := deriveSelectionMetadata(routes)
	return err
}

// defaultAuthHeaders maps a qualified api-key provider to the provider-boundary
// request header its credential_inject entry must target. It mirrors the praxis
// credential_inject filter's defaultAuthHeaders map so the controller renders
// the same header the data plane would otherwise default to, and overrides that
// default where a provider diverges: anthropic uses x-api-key (the filter's own
// apikey default, rendered explicitly so the entry is self-describing) while
// Azure OpenAI uses api-key. Providers are absent here only when their strategy
// fixes the header on the filter side (bearer_token always targets
// Authorization: Bearer, so openai carries no override).
var defaultAuthHeaders = map[string]string{
	"anthropic":    "x-api-key",
	"azure":        "api-key",
	"azure-openai": "api-key",
}

// IsQualifiedAuthHeader reports whether header is a provider-boundary request
// header the controller qualifies for apikey credential injection — one of the
// values StrategyFor emits (see defaultAuthHeaders). The empty header is not
// qualified here: it means "no injected header" (bearer_token fixes
// Authorization filter-side), which callers treat as the absence of a header
// rather than a value to validate. It lets a consumer reject an untrusted or
// unrecognized header annotation before it drives credential_inject.
func IsQualifiedAuthHeader(header string) bool {
	for _, qualified := range defaultAuthHeaders {
		if qualified == header {
			return true
		}
	}
	return false
}

// StrategyFor maps only the currently qualified provider API-key contracts to
// Praxis wire strategies, returning the strategy and — for apikey strategies —
// the provider-boundary header the credential_inject filter must inject into
// (see defaultAuthHeaders): openai + openai-chat maps to bearer_token
// (Authorization: Bearer at the provider boundary, header fixed filter-side so
// none is returned); anthropic + messages and azure/azure-openai + openai-chat
// map to apikey, injected into x-api-key and api-key respectively. The wire
// stays reference-only either way. Provider API keys are not interchangeable:
// other provider/API-format combinations fail closed until their header
// semantics are explicitly supported.
//
// This is the single qualification authority: Render stamps overlay
// candidates with the strategy this function returns, and the tenant
// reconciler preloads unselected bindings into the ExtProc runtime
// configuration through it as well. Both consumers must agree because the
// data plane rejects a candidate whose strategy differs from the configured
// credential entry (praxis-ai #1172); disagreement fails closed with a 503.
func StrategyFor(route resolver.Route) (strategy, header string, err error) {
	switch route.AuthType {
	case "":
		return "", "", nil
	case "apikey":
		switch {
		case route.ProviderType == "openai" && route.APIFormat == "openai-chat":
			return "bearer_token", "", nil
		case route.ProviderType == "anthropic" && route.APIFormat == "messages":
			return "apikey", defaultAuthHeaders["anthropic"], nil
		case (route.ProviderType == "azure" || route.ProviderType == "azure-openai") && route.APIFormat == "openai-chat":
			return "apikey", defaultAuthHeaders[route.ProviderType], nil
		}
		return "", "", fmt.Errorf("%w: auth.type apikey for provider %q and API format %q", ErrUnsupportedCredential, route.ProviderType, route.APIFormat)
	case "sigv4", "oauth2":
		return "", "", fmt.Errorf("%w: auth.type %q", ErrUnsupportedCredential, route.AuthType)
	default:
		return "", "", fmt.Errorf("envelope: unknown auth.type %q", route.AuthType)
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

// randomSelectionPolicy is the Praxis picker policy for equal-weight groups.
var randomSelectionPolicy = json.RawMessage(`{"mode":"random"}`)

// weightedRandomSelectionPolicy is the Praxis picker for proportional weights.
var weightedRandomSelectionPolicy = json.RawMessage(`{"mode":"weightedRandom"}`)

// maxStableIDBytes is the Praxis stable_id length limit.
const maxStableIDBytes = 256

// ScopedStableID is the stable_id for a candidate whose model or provider is
// outside the tenant namespace: <modelNamespace>/<model>/<providerNamespace>/<provider>,
// or sha256:<hex> over the length-prefixed fields when that exceeds the
// Praxis limit. Kubernetes names contain no "/", so the existing form (one
// slash), this form (three) and the hash form (none) never collide.
func ScopedStableID(modelNamespace, model, providerNamespace, provider string) string {
	fields := []string{modelNamespace, model, providerNamespace, provider}
	if id := strings.Join(fields, "/"); len(id) <= maxStableIDBytes {
		return id
	}
	h := sha256.New()
	for _, field := range fields {
		var size [8]byte
		binary.BigEndian.PutUint64(size[:], uint64(len(field)))
		h.Write(size[:])
		h.Write([]byte(field))
	}
	return "sha256:" + hex.EncodeToString(h.Sum(nil))
}

// RouteStableID returns the route's stable_id: its explicit scoped override,
// otherwise the existing CandidateStableID form.
func RouteStableID(route resolver.Route) string {
	if route.StableID != "" {
		return route.StableID
	}
	return CandidateStableID(route.Model, route.Provider)
}

// IsScopedStableID reports whether id is a ScopedStableID (three slashes or
// the hash form) rather than the existing tenant-local form.
func IsScopedStableID(id string) bool {
	return strings.Count(id, "/") != 1
}

// ValidateModel applies Render's per-model checks: provider selection (no
// repeated provider), known clusters and qualified credential strategies. The
// controller also runs it on a contribution from another namespace in
// isolation, so a model Render would reject is excluded before it can fail
// the whole tenant overlay.
func ValidateModel(m resolver.ModelRoutes, opts Options) error {
	if _, err := deriveSelectionMetadata(&resolver.ResolvedRouteSet{Models: []resolver.ModelRoutes{m}}); err != nil {
		return err
	}
	known := make(map[string]bool, len(opts.KnownClusters))
	for _, c := range opts.KnownClusters {
		known[c] = true
	}
	for _, r := range m.Routes {
		if len(known) > 0 && !known[r.Cluster] {
			return fmt.Errorf("%w: %s (model %s)", ErrUnknownCluster, r.Cluster, r.Model)
		}
		if _, _, err := StrategyFor(r); err != nil {
			return fmt.Errorf("model %s provider %s: %w", r.Model, r.Provider, err)
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
	case prev.Generation < 0 || next.Generation < 0:
		return fmt.Errorf("%w: generations cannot be negative: %d -> %d", ErrGenerationRegression, prev.Generation, next.Generation)
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
