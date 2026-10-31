// Package resolver merges ExternalModel/ExternalProvider pairs into a
// resolved route set — pure functions, no Kubernetes client access.
//
// It reproduces the merged-config semantics of the IPP model-provider-resolver
// (opendatahub-io/ai-gateway-payload-processing): model-ref config overrides
// provider config except for tls.caCertificates, which configures shared
// provider transport. Missing, unready, disabled, or policy-excluded refs are
// skipped; an active binding with no qualified provider/API/credential profile
// is a hard error so reconciliation cannot publish a silently reduced route.
package resolver

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"

	apimeta "k8s.io/apimachinery/pkg/api/meta"

	v1alpha1 "github.com/opendatahub-io/ai-gateway-controller/api/inference/v1alpha1"
)

const (
	// ProviderPortAnnotation configures the TLS port for an ExternalProvider
	// endpoint.
	ProviderPortAnnotation = "inference.opendatahub.io/port"
	defaultProviderPort    = int64(443)

	// ProviderTLSAnnotation is the ExternalProvider annotation that IPP reads
	// to disable TLS origination with "false". Provider transport is always
	// TLS, and every provider sends a credential, so plaintext is rejected,
	// as IPP rejects it for a provider with auth.
	ProviderTLSAnnotation = "inference.opendatahub.io/tls"
)

// SkipReason classifies why a single provider ref was excluded from a
// model's route set. Frozen surface (port plan §6): values are stable and
// may be surfaced in model status messages, so they are data, not prose.
type SkipReason string

const (
	// SkipRefNotFound: the referenced ExternalProvider does not exist in
	// the model's namespace.
	SkipRefNotFound SkipReason = "RefNotFound"
	// SkipRefNotPermitted: the ExternalProvider in another namespace is not
	// among the providers a ReferenceGrant permits. One reason covers a
	// missing and an unauthorized provider, so its existence is not
	// disclosed; a local provider is never substituted.
	SkipRefNotPermitted SkipReason = "RefNotPermitted"
	// SkipProviderNotReady: the provider exists but its status.phase is
	// not Ready. Blocking gate ported from IPP: a ref may not route to a
	// provider whose networking (Service/SE/DR) is not confirmed.
	SkipProviderNotReady SkipReason = "ProviderNotReady"
	// SkipWeightDisabled: weight <= 0 disables the ref (IPP semantics:
	// weight 0 means "no traffic", default 1 when unset).
	SkipWeightDisabled SkipReason = "WeightDisabled"
	// SkipTranslationDisabled prevents Anthropic Messages requests when the
	// tenant has not enabled API translation.
	SkipTranslationDisabled SkipReason = "APITranslationDisabled"
	// SkipAPIFormatUnsupported records an unsupported disabled/preloaded
	// provider/API binding. An active binding returns
	// ErrProviderCapabilityUnsupported instead of being silently removed.
	SkipAPIFormatUnsupported SkipReason = "APIFormatUnsupported"
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
	// ProviderNamespace is the ExternalProvider namespace. Empty means
	// Namespace.
	ProviderNamespace string
	// ProviderType is ExternalProvider.spec.provider and is used for
	// provider-specific credential semantics.
	ProviderType string
	// Cluster is the pre-provisioned extproc load_balancer cluster name
	// for this provider: its ServiceName.
	Cluster string
	// Endpoint is the provider FQDN (for HTTPRoute Host rewrite).
	Endpoint string
	// Port is the validated provider transport port. Zero means unset:
	// ProviderPort never returns 0, so this only occurs in hand-built
	// Route literals, and renders as the endpoint/default port.
	Port int64
	// TLSCACertificates is an optional absolute path already mounted in the
	// calling proxy. It is a file path only; the controller never reads or
	// stores certificate bytes. This supports private test/provider CAs while
	// keeping TLS verification enabled.
	TLSCACertificates string
	TargetModel       string
	APIFormat         string
	Path              string // fully resolved, no placeholders
	// APITranslationEnabled is the resolved tenant permission. It gates
	// translation-only HTTPRoute alternatives that exist for rewritten paths.
	APITranslationEnabled bool
	Weight                int    // normalized: unset == 1; only positive weights survive
	AuthType              string // merged auth type: apikey|sigv4|oauth2
	SecretName            string // credential secret, see SecretRefNamespace
	SecretKey             string // fixed "api-key" per the CRD contract
	// StableID overrides the existing stable_id derivation for a route
	// contributed to a tenant from another namespace. Empty keeps the
	// existing <model>/provider-<provider> form.
	StableID string
	// SecretNamespace is SecretName's namespace: the provider namespace for
	// inherited auth of a provider in another namespace, or the tenant
	// namespace for a controller-owned credential copy. Empty means Namespace.
	SecretNamespace string
}

// ProviderPort returns the validated ExternalProvider transport port. An
// omitted annotation retains the HTTPS default of 443.
func ProviderPort(provider *v1alpha1.ExternalProvider) (int64, error) {
	if provider == nil || provider.Annotations == nil {
		return defaultProviderPort, nil
	}
	raw, found := provider.Annotations[ProviderPortAnnotation]
	if !found {
		return defaultProviderPort, nil
	}
	port, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
	if err != nil || port < 1 || port > 65535 {
		return 0, fmt.Errorf("%s must be an integer from 1 to 65535", ProviderPortAnnotation)
	}
	return port, nil
}

// ValidateProviderTLS rejects an ExternalProvider that asks for plaintext
// transport. Serving it over TLS anyway would fail every request while the
// model reports Ready.
func ValidateProviderTLS(provider *v1alpha1.ExternalProvider) error {
	if provider == nil || provider.Annotations == nil {
		return nil
	}
	raw, found := provider.Annotations[ProviderTLSAnnotation]
	if !found {
		return nil
	}
	switch strings.TrimSpace(raw) {
	case "true":
		return nil
	case "false":
		return fmt.Errorf("%s: \"false\" is not supported: plaintext transport would send the provider credential in cleartext; "+
			"serve the endpoint over TLS (%s selects a port other than 443)", ProviderTLSAnnotation, ProviderPortAnnotation)
	default:
		return fmt.Errorf("%s must be \"true\" or \"false\"", ProviderTLSAnnotation)
	}
}

// AuthType returns the auth strategy of a CRD auth.type. "simple", which the
// installed schema retains from 3.5, is the API key strategy, as IPP serves it.
func AuthType(crdType string) string {
	if crdType == "simple" {
		return "apikey"
	}
	return crdType
}

// CrossNamespace reports whether the route's provider is in another
// namespace than its model.
func (r Route) CrossNamespace() bool {
	return r.ProviderNamespace != "" && r.ProviderNamespace != r.Namespace
}

// ServiceName names the provider's transport Service and ServiceEntry in the
// model namespace: provider-<provider>, or provider-<provider>-<hash12> within
// 63 characters for a provider in another namespace, so it never collides
// with a same-named local provider.
func (r Route) ServiceName() string {
	name := "provider-" + r.Provider
	if !r.CrossNamespace() {
		return name
	}
	sum := sha256.Sum256([]byte(r.ProviderNamespace + "/" + r.Provider))
	hash := hex.EncodeToString(sum[:])[:12]
	if limit := 63 - len(hash) - 1; len(name) > limit {
		name = strings.TrimRight(name[:limit], "-.")
	}
	return name + "-" + hash
}

// SecretRefNamespace returns SecretName's namespace.
func (r Route) SecretRefNamespace() string {
	if r.SecretNamespace != "" {
		return r.SecretNamespace
	}
	return r.Namespace
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

// ErrProviderCapabilityUnsupported means an active provider binding cannot be
// served by the installed Praxis translation and credential runtime. The
// caller must retain its last-known serving resources rather than publish a
// reduced route set that silently drops this candidate.
var ErrProviderCapabilityUnsupported = errors.New("resolver: provider capability is unsupported by the installed Praxis runtime")

// Resolve merges model/provider pairs into a route set. Ordering guarantees:
// models keep input order, routes keep ref order. A model whose refs all
// skip is not an error by itself — its skips are reported and only a wholly
// empty set returns ErrNoRoutes. A ref to another namespace resolves only to
// a provider of that namespace in providers, so callers pass only the
// providers in other namespaces that the model namespace may reference.
func Resolve(models []*v1alpha1.ExternalModel, providers []*v1alpha1.ExternalProvider) (*ResolvedRouteSet, error) {
	return resolve(models, providers, false, false)
}

// ResolveWithAPITranslation resolves routes when the tenant explicitly permits
// API translation. The permission allows supported Messages candidates and
// enables trusted provider profiles for request conversion, including
// targetModel rewriting on OpenAI-compatible routes. It does not choose which
// provider is selected; the selected candidate's profile controls conversion.
func ResolveWithAPITranslation(models []*v1alpha1.ExternalModel, providers []*v1alpha1.ExternalProvider, enabled bool) (*ResolvedRouteSet, error) {
	return resolve(models, providers, false, enabled)
}

// ResolveAll resolves every valid provider binding, including weight-zero
// references. The serving overlay uses Resolve and therefore excludes those
// references from selection. The ExtProc runtime uses ResolveAll to preload
// the complete referenced provider/credential set, so changing the selected
// provider is an overlay-only update and does not require a pod rollout.
func ResolveAll(models []*v1alpha1.ExternalModel, providers []*v1alpha1.ExternalProvider) (*ResolvedRouteSet, error) {
	return resolve(models, providers, true, false)
}

// ResolveAllWithAPITranslation retains weight-disabled bindings so ExtProc
// can preload references before selection changes. The tenant permission
// still gates Messages candidates, including weight-disabled references.
func ResolveAllWithAPITranslation(models []*v1alpha1.ExternalModel, providers []*v1alpha1.ExternalProvider, enabled bool) (*ResolvedRouteSet, error) {
	return resolve(models, providers, true, enabled)
}

// resolveRefTransport validates and resolves the transport-level config for
// one ExternalProviderRef: the shared provider TLS CA, the templated
// request path, and the provider's transport port.
func resolveRefTransport(prov *v1alpha1.ExternalProvider, ref v1alpha1.ExternalProviderRef) (path, caCertificates string, port int64, err error) {
	providerCA := strings.TrimSpace(prov.Spec.Config["tls.caCertificates"])
	if modelCA, configured := ref.Config["tls.caCertificates"]; configured && strings.TrimSpace(modelCA) != providerCA {
		return "", "", 0, fmt.Errorf("tls.caCertificates cannot override ExternalProvider %s because provider transport is shared; configure it on the ExternalProvider", prov.Name)
	}

	cfg := mergeConfig(prov.Spec.Config, ref.Config)
	caCertificates = strings.TrimSpace(cfg["tls.caCertificates"])
	if caCertificates != "" && (!strings.HasPrefix(caCertificates, "/") || strings.Contains(caCertificates, "..")) {
		return "", "", 0, errors.New("tls.caCertificates must be an absolute mounted file path")
	}
	path, err = resolvePath(ref.Path, ref.TargetModel, cfg)
	if err != nil {
		// IPP #368: unresolved placeholders are a reconcile-time
		// error, never a request-time surprise. Hard-fail the call.
		return "", "", 0, err
	}
	if strings.TrimSpace(ref.TargetModel) == "" || len(ref.TargetModel) > 256 || strings.ContainsAny(ref.TargetModel, "\r\n\x00") {
		return "", "", 0, errors.New("targetModel must be 1-256 printable bytes")
	}
	if path == "" || len(path) > 2048 || !strings.HasPrefix(path, "/") || strings.ContainsAny(path, "?#\r\n\x00") {
		return "", "", 0, errors.New("resolved path must be a valid absolute path")
	}
	port, err = ProviderPort(prov)
	if err != nil {
		return "", "", 0, err
	}
	return path, caCertificates, port, nil
}

func resolve(models []*v1alpha1.ExternalModel, providers []*v1alpha1.ExternalProvider, includeDisabled, translationEnabled bool) (*ResolvedRouteSet, error) {
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
		activeProviders := map[provKey]bool{}
		clientName := m.Name
		if m.Spec.ModelName != "" {
			clientName = m.Spec.ModelName
		}
		for _, ref := range m.Spec.ExternalProviderRefs {
			base := Skip{ModelRef: mref, ProviderRef: ref.Ref.Name}
			key := provKey{m.Namespace, ref.Ref.Name}
			if ref.Ref.Namespace != "" {
				key.ns = ref.Ref.Namespace
			}
			prov, ok := byKey[key]
			if !ok && key.ns != m.Namespace {
				mr.Skips = append(mr.Skips, newSkip(base, SkipRefNotPermitted,
					fmt.Sprintf("ExternalProvider %s/%s is not found or not permitted by a ReferenceGrant in namespace %s", key.ns, key.name, key.ns)))
				continue
			}
			// A Messages route cannot serve while translation is disabled, even
			// when its previously serving Anthropic provider has disappeared or
			// become unready. Classify that policy exclusion before availability
			// so reconciliation withdraws the old route and candidate. A known
			// non-Anthropic provider still reaches capability validation below.
			if ref.APIFormat == "messages" && !translationEnabled && (!ok || prov.Spec.Provider == "anthropic") {
				mr.Skips = append(mr.Skips, newSkip(base, SkipTranslationDisabled,
					"MaasTenantConfig.spec.apiTranslation.enabled must be true for an Anthropic Messages provider"))
				continue
			}
			if !ok {
				mr.Skips = append(mr.Skips, newSkip(base,
					SkipRefNotFound, "external provider not found in model namespace"))
				continue
			}
			if prov.Status.Phase != PhaseReady {
				message := fmt.Sprintf("provider phase %q is not Ready", prov.Status.Phase)
				if ready := apimeta.FindStatusCondition(prov.Status.Conditions, "Ready"); ready != nil && ready.Message != "" {
					// The provider's own reason, so the model status explains it.
					message += ": " + ready.Message
				}
				mr.Skips = append(mr.Skips, newSkip(base, SkipProviderNotReady, message))
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
			if !supportedProviderFormat(prov.Spec.Provider, ref.APIFormat) {
				message := fmt.Sprintf("provider %q with APIFormat %q has no qualified route and credential profile", prov.Spec.Provider, ref.APIFormat)
				if weight <= 0 {
					// Disabled candidates are preloaded by ResolveAll, but cannot
					// serve traffic. Keep them out of the runtime until qualified.
					mr.Skips = append(mr.Skips, newSkip(base, SkipAPIFormatUnsupported, message))
					continue
				}
				mr.Skips = append(mr.Skips, newSkip(base, SkipAPIFormatUnsupported, message))
				set.Models = append(set.Models, mr)
				return set, fmt.Errorf("%w: %s: model %s provider ref %s: %s", ErrProviderCapabilityUnsupported, SkipAPIFormatUnsupported, mref, ref.Ref.Name, message)
			}
			if weight > 0 && activeProviders[key] {
				return nil, fmt.Errorf("resolver: model %s has multiple active references to provider %s/%s", mref, prov.Namespace, prov.Name)
			}
			if weight > 0 {
				activeProviders[key] = true
			}

			path, caCertificates, port, err := resolveRefTransport(prov, ref)
			if err != nil {
				return nil, fmt.Errorf("resolver: model %s ref %s: %w", mref, ref.Ref.Name, err)
			}

			// Inherited auth uses the provider-local Secret; a model override
			// uses the model-local Secret, even for a provider elsewhere.
			auth, secretNamespace := prov.Spec.Auth, ""
			if ref.Auth != nil {
				auth = *ref.Auth
			} else if prov.Namespace != m.Namespace {
				secretNamespace = prov.Namespace
			}
			route := Route{
				Model:                 m.Name,
				ClientName:            clientName,
				Namespace:             m.Namespace,
				Provider:              prov.Name,
				ProviderNamespace:     prov.Namespace,
				ProviderType:          prov.Spec.Provider,
				Endpoint:              prov.Spec.Endpoint,
				Port:                  port,
				TLSCACertificates:     caCertificates,
				TargetModel:           ref.TargetModel,
				APIFormat:             ref.APIFormat,
				Path:                  path,
				APITranslationEnabled: translationEnabled,
				Weight:                weight,
				AuthType:              AuthType(auth.Type),
				SecretName:            auth.SecretRef.Name,
				SecretKey:             "api-key",
				SecretNamespace:       secretNamespace,
			}
			route.Cluster = route.ServiceName()
			mr.Routes = append(mr.Routes, route)
			resolved++
		}
		set.Models = append(set.Models, mr)
	}
	if resolved == 0 {
		return set, ErrNoRoutes
	}
	return set, nil
}

func supportedProviderFormat(providerType, apiFormat string) bool {
	switch {
	case providerType == "openai" && apiFormat == "openai-chat":
		return true
	case providerType == "anthropic" && apiFormat == "messages":
		return true
	case (providerType == "azure" || providerType == "azure-openai") && apiFormat == "openai-chat":
		return true
	default:
		return false
	}
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
