/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package render

import (
	"strings"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// Params controls the placeholder substitution PostRender applies to the
// composed upstream praxis-extproc and controller-owned ExternalModel
// manifests. The upstream kustomization documents the contract this mirrors:
// "Namespace is supplied by the consumer ... Placeholder FQDNs / gateway
// names ... are rewritten by the controller."
type Params struct {
	// Namespace is the initial render and Gateway namespace. The tenant
	// reconciler subsequently moves the post-auth workload and its
	// namespace-scoped configuration to the resolved tenant namespace while
	// keeping EnvoyFilter and Gateway-facing mesh resources here. Replaces
	// every "openshift-ingress" placeholder.
	Namespace string
	// GatewayName replaces the "maas-default-gateway" placeholder.
	GatewayName string
	// Image replaces the "praxis-extproc:dev" placeholder image reference on
	// the payload-processing and payload-pre-processing containers.
	Image string
	// MaaSAPIRouteName replaces the "PLACEHOLDER.maas-api-route" prefix used
	// by the EnvoyFilter to disable ext_proc on maas-api's own HTTPRoute
	// rules. Exact fidelity depends on maas-api's HTTPRoute name and the
	// Istio version's internal route-naming scheme, which this controller
	// does not observe in Phase 1 (see DESIGN.md, "Out of scope"); treat the
	// default as a best-effort placeholder until that integration lands.
	MaaSAPIRouteName string
}

// clusterScopedKinds lists the Kinds present in the vendored overlay that
// must never receive a namespace. Everything else is namespace-scoped and
// gets defaulted to Params.Namespace by PostRender if kustomize left it
// unset (the overlay intentionally ships with no "namespace:" field; see
// Params.Namespace doc).
var clusterScopedKinds = map[string]bool{
	"ClusterRole":              true,
	"ClusterRoleBinding":       true,
	"CustomResourceDefinition": true,
	"Namespace":                true,
}

// placeholderReplacement is one literal substring substitution applied
// across every string value in a rendered resource.
type placeholderReplacement struct {
	old, new string
}

// placeholderReplacements returns the substitutions PostRender applies, in
// order. Order matters: compound placeholders that embed a shorter one (the
// gateway name inside the Kuadrant subFilter name, the namespace inside the
// FQDN suffix) must be replaced before the shorter, standalone pattern, or
// the shorter replacement would fire first and leave the compound pattern
// half-rewritten.
func placeholderReplacements(p Params) []placeholderReplacement {
	return []placeholderReplacement{
		// Kuadrant WasmPlugin/TrafficExtension subFilter names embed
		// "<gateway namespace>.kuadrant-<gateway name>" as one token, e.g.
		// "extensions.istio.io/wasmplugin/openshift-ingress.kuadrant-maas-default-gateway".
		{old: "openshift-ingress.kuadrant-maas-default-gateway", new: p.Namespace + ".kuadrant-" + p.GatewayName},
		// DestinationRule host/sni and EnvoyFilter CLUSTER address/sni FQDNs.
		{old: ".openshift-ingress.svc.cluster.local", new: "." + p.Namespace + ".svc.cluster.local"},
		// Remaining standalone occurrence: EnvoyFilter workloadSelector label value.
		{old: "maas-default-gateway", new: p.GatewayName},
		// Remaining standalone occurrences: ClusterRoleBinding subject
		// namespace, and the NetworkPolicy namespaceSelector value that
		// networkpolicy.yaml's own comment calls out as a placeholder.
		{old: "openshift-ingress", new: p.Namespace},
		// EnvoyFilter HTTP_ROUTE match names, e.g. "PLACEHOLDER.maas-api-route.0".
		{old: "PLACEHOLDER.maas-api-route", new: p.MaaSAPIRouteName},
		// Deployment container images (payload-processing and payload-pre-processing).
		{old: "praxis-extproc:dev", new: p.Image},
	}
}

// PostRender rewrites the placeholder namespace, gateway name, image, and
// route-name values baked into the vendored overlay, and defaults
// metadata.namespace on every namespace-scoped resource that kustomize left
// unset. It does not mutate the input slice.
func PostRender(resources []unstructured.Unstructured, p Params) []unstructured.Unstructured {
	replacements := placeholderReplacements(p)

	out := make([]unstructured.Unstructured, len(resources))
	for i := range resources {
		u := resources[i].DeepCopy()

		rewritten, ok := replaceStrings(u.Object, replacements).(map[string]any)
		if ok {
			u.Object = rewritten
		}

		if !clusterScopedKinds[u.GetKind()] && u.GetNamespace() == "" {
			u.SetNamespace(p.Namespace)
		}

		out[i] = *u
	}
	return out
}

// replaceStrings walks an unstructured value tree (the shapes produced by
// unstructured.Unstructured: map[string]any, []any, and scalars) and applies
// every replacement to each string leaf, returning the (possibly new) value.
func replaceStrings(v any, replacements []placeholderReplacement) any {
	switch val := v.(type) {
	case map[string]any:
		for k, child := range val {
			val[k] = replaceStrings(child, replacements)
		}
		return val
	case []any:
		for i, child := range val {
			val[i] = replaceStrings(child, replacements)
		}
		return val
	case string:
		for _, r := range replacements {
			val = strings.ReplaceAll(val, r.old, r.new)
		}
		return val
	default:
		return v
	}
}
