// Package resolver merges ExternalModel/ExternalProvider pairs into a
// resolved route set — pure functions, no Kubernetes client access.
//
// It reproduces the merged-config semantics of the IPP model-provider-resolver
// (opendatahub-io/ai-gateway-payload-processing): model-ref config overrides
// provider config, refs are skipped (not failed) when individually bad, and
// reconciliation fails only when a model resolves zero routes.
package resolver

import (
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"

	v1alpha1 "github.com/opendatahub-io/ai-gateway-controller/api/inference/v1alpha1"
)

// SkipReason classifies why a single provider ref was excluded from a
// model's route set. Frozen surface (port plan §6): values are stable and
// may be surfaced in model status messages, so they are data, not prose.
type SkipReason string

const (
	// SkipRefNotFound: the referenced ExternalProvider does not exist in
	// the model's namespace.
	SkipRefNotFound SkipReason = "RefNotFound"
	// SkipRefNamespaceUnsupported: cross-namespace provider resolution and
	// authorization are not implemented; never substitute a local provider.
	SkipRefNamespaceUnsupported SkipReason = "RefNamespaceUnsupported"
	// SkipProviderNotReady: the provider exists but its status.phase is
	// not Ready. Blocking gate ported from IPP: a ref may not route to a
	// provider whose networking (Service/SE/DR) is not confirmed.
	SkipProviderNotReady SkipReason = "ProviderNotReady"
	// SkipWeightDisabled: weight <= 0 disables the ref (IPP semantics:
	// weight 0 means "no traffic", default 1 when unset).
	SkipWeightDisabled SkipReason = "WeightDisabled"
	// SkipPathUnresolved: the ref's path template contains a {key}
	// placeholder with no entry in the merged config.
	SkipPathUnresolved SkipReason = "PathUnresolved"
)

// PhaseReady mirrors the CRD enum (status.phase == "Ready").
const PhaseReady = "Ready"

// Skip records one excluded ref and why.
type Skip struct {
	// ModelRef is "namespace/name" of the model that owns the ref.
	ModelRef string
	// ProviderRef is the ref's provider name (as written in the spec).
	ProviderRef string
	Reason      SkipReason
	Message     string
}

// Route is one resolved (model x provider) pair — the atom both output
// planes are rendered from: the Envoy plane (HTTPRoute/SE/DR) and the
// ExtProc routing overlay envelope.
type Route struct {
	// Model is the ExternalModel CR name. Overlay candidate.name and the
	// HTTPRoute `X-Gateway-Model-Name` match both key on this (#425).
	Model string
	// ClientName is the body `model` value clients send (spec.modelName,
	// defaulting to the CR name). Alias-candidate expansion is an open
	// design question (port plan Q3) — carried, not yet expanded.
	ClientName string
	Namespace  string
	// Provider is the ExternalProvider CR name.
	Provider string
	// ProviderType is ExternalProvider.spec.provider and is used for
	// provider-specific credential semantics.
	ProviderType string
	// Cluster is the pre-provisioned extproc load_balancer cluster name
	// for this provider (fixed convention: "provider-<crName>").
	Cluster string
	// Endpoint is the provider FQDN (for HTTPRoute Host rewrite).
	Endpoint string
	// TLSCACertificates is an optional absolute path already mounted in the
	// calling proxy. It is a file path only; the controller never reads or
	// stores certificate bytes. This supports private test/provider CAs while
	// keeping TLS verification enabled.
	TLSCACertificates string
	TargetModel       string
	APIFormat         string
	Path              string // fully resolved, no placeholders
	Weight            int    // normalized: unset == 1; only positive weights survive
	AuthType          string // merged auth type: apikey|sigv4|oauth2
	SecretName        string // credential secret (same namespace as provider)
	SecretKey         string // fixed "api-key" per the CRD contract
}

// ModelRoutes is the per-model outcome: resolved routes in ref order (the
// deterministic first-admitted fallback order — IPP's refs[0] semantics)
// plus every ref skipped along the way.
type ModelRoutes struct {
	ModelRef string // "namespace/name"
	Routes   []Route
	Skips    []Skip
}

// ResolvedRouteSet is the single input both render planes consume in one
// reconcile (port plan §4.4: one source, two planes, no divergence window).
type ResolvedRouteSet struct {
	Models []ModelRoutes
}

// Routes flattens the set in model/ref order.
func (s *ResolvedRouteSet) Routes() []Route {
	var out []Route
	for _, m := range s.Models {
		out = append(out, m.Routes...)
	}
	return out
}

// ErrNoRoutes means no model resolved a single route; the caller must mark
// models Failed (IPP fails only when zero refs resolve).
var ErrNoRoutes = errors.New("resolver: no provider refs resolved to routes")

// Resolve merges model/provider pairs into a route set. Ordering guarantees:
// models keep input order, routes keep ref order. A model whose refs all
// skip is not an error by itself — its skips are reported and only a wholly
// empty set returns ErrNoRoutes.
func Resolve(models []*v1alpha1.ExternalModel, providers []*v1alpha1.ExternalProvider) (*ResolvedRouteSet, error) {
	return resolve(models, providers, false)
}

// ResolveAll resolves every valid provider binding, including weight-zero
// references. The serving overlay uses Resolve and therefore excludes those
// references from selection. The ExtProc runtime uses ResolveAll to preload
// the complete referenced provider/credential set, so changing the selected
// provider is an overlay-only update and does not require a pod rollout.
func ResolveAll(models []*v1alpha1.ExternalModel, providers []*v1alpha1.ExternalProvider) (*ResolvedRouteSet, error) {
	return resolve(models, providers, true)
}

func resolve(models []*v1alpha1.ExternalModel, providers []*v1alpha1.ExternalProvider, includeDisabled bool) (*ResolvedRouteSet, error) {
	type provKey struct{ ns, name string }
	byKey := make(map[provKey]*v1alpha1.ExternalProvider, len(providers))
	for _, p := range providers {
		if p == nil {
			continue
		}
		byKey[provKey{p.Namespace, p.Name}] = p
	}

	set := &ResolvedRouteSet{}
	resolved := 0
	for _, m := range models {
		if m == nil {
			continue
		}
		mref := m.Namespace + "/" + m.Name
		mr := ModelRoutes{ModelRef: mref}
		activeProviders := map[string]bool{}
		clientName := m.Name
		if m.Spec.ModelName != "" {
			clientName = m.Spec.ModelName
		}
		for _, ref := range m.Spec.ExternalProviderRefs {
			base := Skip{ModelRef: mref, ProviderRef: ref.Ref.Name}
			if ref.Ref.Namespace != "" && ref.Ref.Namespace != m.Namespace {
				mr.Skips = append(mr.Skips, newSkip(base, SkipRefNamespaceUnsupported,
					fmt.Sprintf("cross-namespace ExternalProvider reference %q is not supported", ref.Ref.Namespace+"/"+ref.Ref.Name)))
				continue
			}

			prov, ok := byKey[provKey{m.Namespace, ref.Ref.Name}]
			if !ok {
				mr.Skips = append(mr.Skips, newSkip(base,
					SkipRefNotFound, "external provider not found in model namespace"))
				continue
			}
			if prov.Status.Phase != PhaseReady {
				mr.Skips = append(mr.Skips, newSkip(base,
					SkipProviderNotReady, fmt.Sprintf("provider phase %q is not Ready", prov.Status.Phase)))
				continue
			}

			weight := 1
			if ref.Weight != nil {
				weight = *ref.Weight
			}
			if weight <= 0 && !includeDisabled {
				mr.Skips = append(mr.Skips, newSkip(base,
					SkipWeightDisabled, fmt.Sprintf("weight %d disables this ref", weight)))
				continue
			}
			if weight > 0 && activeProviders[prov.Name] {
				return nil, fmt.Errorf("resolver: model %s has multiple active references to provider %s", mref, prov.Name)
			}
			if weight > 0 {
				activeProviders[prov.Name] = true
			}

			cfg := mergeConfig(prov.Spec.Config, ref.Config)
			caCertificates := strings.TrimSpace(cfg["tls.caCertificates"])
			if caCertificates != "" && (!strings.HasPrefix(caCertificates, "/") || strings.Contains(caCertificates, "..")) {
				return nil, fmt.Errorf("resolver: model %s ref %s: tls.caCertificates must be an absolute mounted file path", mref, ref.Ref.Name)
			}
			path, err := resolvePath(ref.Path, ref.TargetModel, cfg)
			if err != nil {
				// IPP #368: unresolved placeholders are a reconcile-time
				// error, never a request-time surprise. Hard-fail the call.
				return nil, fmt.Errorf("resolver: model %s ref %s: %w", mref, ref.Ref.Name, err)
			}

			auth := prov.Spec.Auth
			if ref.Auth != nil {
				auth = *ref.Auth
			}
			mr.Routes = append(mr.Routes, Route{
				Model:             m.Name,
				ClientName:        clientName,
				Namespace:         m.Namespace,
				Provider:          prov.Name,
				ProviderType:      prov.Spec.Provider,
				Cluster:           "provider-" + prov.Name,
				Endpoint:          prov.Spec.Endpoint,
				TLSCACertificates: caCertificates,
				TargetModel:       ref.TargetModel,
				APIFormat:         ref.APIFormat,
				Path:              path,
				Weight:            weight,
				AuthType:          auth.Type,
				SecretName:        auth.SecretRef.Name,
				SecretKey:         "api-key",
			})
			resolved++
		}
		set.Models = append(set.Models, mr)
	}
	if resolved == 0 {
		return set, ErrNoRoutes
	}
	return set, nil
}

// newSkip returns a copy of base with a reason and message attached.
func newSkip(s Skip, reason SkipReason, message string) Skip {
	s.Reason = reason
	s.Message = message
	return s
}

// mergeConfig overlays ref config on provider config (ref wins per key).
func mergeConfig(provCfg, refCfg map[string]string) map[string]string {
	merged := make(map[string]string, len(provCfg)+len(refCfg))
	for k, v := range provCfg {
		merged[k] = v
	}
	for k, v := range refCfg {
		merged[k] = v
	}
	return merged
}

// placeholderRe matches {key} with a non-empty key, mirroring IPP.
var placeholderRe = regexp.MustCompile(`\{([^}]+)\}`)

// resolvePath is the port of IPP pkg/controller/common/path.go ResolvePath:
// every merged-config key substitutes first (so an explicit "model" config
// key takes precedence), then the reserved {model} placeholder falls back
// to targetModel; anything left over is an error listing ALL unresolved
// keys (IPP #368 — reconcile-time, never request-time). One deliberate
// deviation from verbatim: config keys substitute in sorted order, where
// IPP's map iteration is randomized — see the comment in the body and the
// ChainedPlaceholder parity test.
func resolvePath(tmpl, targetModel string, cfg map[string]string) (string, error) {
	if tmpl == "" || !strings.Contains(tmpl, "{") {
		return tmpl, nil
	}
	// Config substitutes first (IPP semantics), in SORTED key order. IPP
	// iterates its config map in Go's randomized order, so a config value
	// containing another key's placeholder (cfg{"a":"x{b}y","b":"z"} on
	// "/{a}/{b}") makes IPP's result — success or PathUnresolved error —
	// coin-flip between reconciles. Nondeterminism is a defect, not a
	// quirk to port: sorted order is deterministic and identical to IPP
	// on every config whose values contain no placeholder tokens (the
	// real-world case). Documented deviation, see the parity test.
	keys := make([]string, 0, len(cfg))
	for k := range cfg {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	path := tmpl
	for _, k := range keys {
		path = strings.ReplaceAll(path, "{"+k+"}", cfg[k])
	}
	path = strings.ReplaceAll(path, "{model}", targetModel)
	if matches := placeholderRe.FindAllStringSubmatch(path, -1); len(matches) > 0 {
		keys := make([]string, 0, len(matches))
		for _, m := range matches {
			keys = append(keys, m[1])
		}
		return "", fmt.Errorf("path %q has unresolved placeholders %v — add these keys to the provider or model config", tmpl, keys)
	}
	return path, nil
}
