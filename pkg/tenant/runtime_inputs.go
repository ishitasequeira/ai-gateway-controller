package tenant

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/opendatahub-io/ai-gateway-controller/api/inference/v1alpha1"
	"github.com/opendatahub-io/ai-gateway-controller/pkg/envelope"
	"github.com/opendatahub-io/ai-gateway-controller/pkg/resolver"
)

// AnnotationHomeRuntimeRoutes stores reference-only provider profiles alongside
// their applied home HTTPRoute. They survive removal from the routing overlay
// until the route writer prunes the corresponding rules after the drain.
const AnnotationHomeRuntimeRoutes = "inference.opendatahub.io/home-runtime-routes"

// RetainHomeRuntimeCandidates keeps runtime-only credentials and provider hops
// for applied routes while older serving revisions drain. It never publishes
// these candidates into the routing overlay or changes provider readiness.
func RetainHomeRuntimeCandidates(ctx context.Context, reader client.Reader, namespace string, current []envelope.Candidate, apiTranslation bool) ([]envelope.Candidate, error) {
	routes := &unstructured.UnstructuredList{}
	routes.SetGroupVersionKind(gvkHTTPRoute.GroupVersion().WithKind("HTTPRouteList"))
	if err := reader.List(ctx, routes, client.InNamespace(namespace), client.MatchingLabels{managedByLabel: ManagedByAIGatewayController}); err != nil {
		return nil, fmt.Errorf("list retained home HTTPRoutes: %w", err)
	}
	result := slices.Clone(current)
	seen := map[string]bool{}
	for _, candidate := range current {
		seen[candidate.StableID] = true
	}
	for i := range routes.Items {
		route := &routes.Items[i]
		profiles, err := HomeRuntimeRouteProfiles(route, namespace)
		if err != nil {
			return nil, err
		}
		for _, profile := range profiles {
			if seen[envelope.CandidateStableID(profile.Model, profile.Provider)] {
				continue
			}
			set := &resolver.ResolvedRouteSet{Models: []resolver.ModelRoutes{{Routes: []resolver.Route{profile}}}}
			candidates, err := ExternalModelRuntimeCandidates(set, apiTranslation)
			if err != nil {
				return nil, err
			}
			for _, candidate := range candidates {
				if !seen[candidate.StableID] {
					result = append(result, candidate)
					seen[candidate.StableID] = true
				}
			}
		}
	}
	return result, nil
}

// HomeRuntimeRouteProfiles validates the reference-only profiles against the
// controller-owned home HTTPRoute that keeps their provider transport alive.
func HomeRuntimeRouteProfiles(route *unstructured.Unstructured, namespace string) ([]resolver.Route, error) {
	modelName := route.GetLabels()["inference.opendatahub.io/external-model"]
	owner := metav1.GetControllerOf(route)
	if route.GetNamespace() != namespace || route.GetLabels()[managedByLabel] != ManagedByAIGatewayController || modelName == "" {
		return nil, nil
	}
	// A staged rebuild can retain the rule after its model leaves the owner
	// input set. Reject a conflicting owner, but keep that ownerless rule.
	if owner != nil && (owner.Kind != "ExternalModel" || owner.Name != modelName || owner.APIVersion != v1alpha1.GroupVersion.String()) {
		return nil, nil
	}
	raw := route.GetAnnotations()[AnnotationHomeRuntimeRoutes]
	if raw == "" {
		return nil, nil
	}
	var profiles []resolver.Route
	if err := json.Unmarshal([]byte(raw), &profiles); err != nil { //nolint:musttag // Internal resolved routes use Go field names in this reference-only snapshot.
		return nil, fmt.Errorf("decode retained runtime profiles on HTTPRoute %s/%s: %w", namespace, route.GetName(), err)
	}
	for _, profile := range profiles {
		if !validHomeRuntimeProfile(profile, namespace) {
			return nil, fmt.Errorf("retained runtime profile on HTTPRoute %s/%s has invalid fields", namespace, route.GetName())
		}
		if profile.Namespace != namespace || profile.Model != modelName || profile.CrossNamespace() || !homeRouteHasRuntimeBinding(route, profile) {
			return nil, fmt.Errorf("retained runtime profile does not match HTTPRoute %s/%s", namespace, route.GetName())
		}
	}
	return profiles, nil
}

// validHomeRuntimeProfile bounds fields decoded from route annotations before
// they can become credential paths, projected Secret references, or routing
// targets. This validates configuration data, not the writer's authorization.
func validHomeRuntimeProfile(profile resolver.Route, namespace string) bool {
	if len(validation.IsDNS1123Subdomain(profile.Model)) > 0 || len(validation.IsDNS1123Subdomain(profile.Provider)) > 0 ||
		profile.Cluster != profile.ServiceName() || profile.SecretNamespace != "" && profile.SecretNamespace != namespace {
		return false
	}
	if profile.StableID != "" && profile.StableID != envelope.CandidateStableID(profile.Model, profile.Provider) {
		return false
	}
	if profile.SecretName != "" && len(validation.IsDNS1123Subdomain(profile.SecretName)) > 0 {
		return false
	}
	if profile.SecretKey != "" && profile.SecretKey != "api-key" {
		return false
	}
	if profile.AuthType != "" && (profile.SecretName == "" || profile.SecretKey != "api-key") {
		return false
	}
	return true
}

// homeRouteHasRuntimeBinding verifies the profile's exact provider handoff and
// backend rather than treating an annotation alone as an applied route.
func homeRouteHasRuntimeBinding(route *unstructured.Unstructured, profile resolver.Route) bool {
	rules, _, _ := unstructured.NestedSlice(route.Object, "spec", "rules")
	for _, rawRule := range rules {
		rule, ok := rawRule.(map[string]any)
		if !ok {
			continue
		}
		backends, _, _ := unstructured.NestedSlice(rule, "backendRefs")
		matches, _, _ := unstructured.NestedSlice(rule, "matches")
		for _, rawBackend := range backends {
			backend, ok := rawBackend.(map[string]any)
			if !ok || backend["name"] != profile.ServiceName() {
				continue
			}
			if matchesRuntimeIdentity(matches, envelope.RouteStableID(profile)) {
				return true
			}
		}
	}
	return false
}

func matchesRuntimeIdentity(matches []any, stableID string) bool {
	for _, rawMatch := range matches {
		match, ok := rawMatch.(map[string]any)
		if !ok {
			continue
		}
		headers, _, _ := unstructured.NestedSlice(match, "headers")
		for _, rawHeader := range headers {
			header, ok := rawHeader.(map[string]any)
			if ok && header["name"] == "X-AI-Routing-Candidate" && header["value"] == stableID && header["type"] == "Exact" {
				return true
			}
		}
	}
	return false
}
