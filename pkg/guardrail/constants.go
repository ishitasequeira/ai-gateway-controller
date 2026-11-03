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

// Package guardrail reconciles AIGuardrail resources: it resolves each
// policy's NeMo provider, evaluates the provider's allowedConsumers
// authorization, verifies provider readiness, and publishes an accepted
// binding on the AIGuardrail status. It does not compile the per-tenant
// Praxis check catalog — that is the catalog reconciler's job (RHAI-2520).
//
// This package deliberately reads NemoGuardrails via unstructured +
// schema.GroupVersionKind rather than importing the TrustyAI operator's Go
// types, for the same reason pkg/tenant reads AITenant unstructured (see
// pkg/tenant/constants.go): pulling in that module's dependency graph is
// unnecessary when the only contract this controller relies on is the
// resource's on-wire JSON shape — spec.allowedConsumers for authorization,
// spec.nemoConfigs and status for readiness and config discovery.
package guardrail

import "k8s.io/apimachinery/pkg/runtime/schema"

// NemoGuardrailsGVK identifies TrustyAI's NemoGuardrails CRD
// (trustyai-service-operator api/nemo_guardrails/v1alpha1.NemoGuardrails).
// It is the provider an AIGuardrail binds to, and the owner of the
// spec.allowedConsumers rule that decides whether that binding is permitted.
var NemoGuardrailsGVK = schema.GroupVersionKind{
	Group:   "trustyai.opendatahub.io",
	Version: "v1alpha1",
	Kind:    "NemoGuardrails",
}

// Condition types published on AIGuardrail.status, per the guardrails
// proposal (models-as-a-service .../guardrails-responses/
// 02-guardrails-low-level-details.md).
//
// These are exported so the per-tenant catalog compiler can gate on them by
// symbol instead of re-declaring the strings. A consumer must treat a
// condition as authoritative only when its ObservedGeneration equals the
// AIGuardrail's metadata.generation: a True verdict left over from an earlier
// generation says nothing about the current spec, so a plain
// apimeta.IsStatusConditionTrue check would admit a stale acceptance.
const (
	ConditionAccepted      = "Accepted"
	ConditionResolvedRefs  = "ResolvedRefs"
	ConditionProviderReady = "ProviderReady"
	ConditionCompatible    = "Compatible"
)

// Reference-resolution condition reasons, published on ResolvedRefs and
// mirrored onto Accepted when they deny the binding.
//
// Every one of these except reasonReferencesAuthorized is a refusal, and a
// refusal always leaves the policy and its checks in place: a guardrail that
// cannot be authorized has to keep failing closed downstream, never quietly
// stop being enforced.
const (
	reasonReferencesAuthorized  = "ReferencesAuthorized"
	reasonProviderNotFound      = "ProviderNotFound"
	reasonInvalidPolicy         = "InvalidAllowedConsumers"
	reasonConsumerNotAuthorized = "ConsumerNotAuthorized"
	// reasonProviderCRDNotInstalled is reported when TrustyAI is absent, so
	// no NemoGuardrails can exist for a policy to resolve against. Re-asked
	// on every reconcile, so a policy is accepted once TrustyAI is installed
	// without this controller being restarted; see providerCRDInstalled.
	reasonProviderCRDNotInstalled = "ProviderCRDNotInstalled"
)

// Provider readiness condition reasons.
//
// reasonProviderAvailable is the canonical success reason from the guardrails
// proposal's binding status fragment. The proposal names no failure reason for
// missing discovery, so reasonEndpointDiscoveryUnavailable and
// reasonProviderNotReady are this controller's own.
const (
	reasonProviderAvailable            = "ProviderAvailable"
	reasonEndpointDiscoveryUnavailable = "EndpointDiscoveryUnavailable"
	reasonProviderNotReady             = "ProviderNotReady"
)

// Compatibility condition reasons, published on Compatible and mirrored onto
// Accepted when they refuse the binding.
//
// Compatibility is the desired-side check only: each spec.checks[].configId
// must name a configuration the provider declares in spec.nemoConfigs[].name.
// Whether the running NeMo server actually loaded that configuration is not
// observable from the CR and needs the same endpoint-discovery contract as
// provider readiness (see evaluateNemoReady).
//
// The proposal names no reasons for this condition, so all four are this
// controller's own.
const (
	reasonChecksSatisfiable          = "ChecksSatisfiable"
	reasonUnknownCheckConfig         = "UnknownCheckConfig"
	reasonProviderConfigsUnavailable = "ProviderConfigsUnavailable"
	reasonInvalidProviderConfigs     = "InvalidProviderConfigs"
)

// reasonPolicyAccepted is the canonical success reason on Accepted, from the
// guardrails proposal's binding status fragment.
const reasonPolicyAccepted = "PolicyAccepted"

// reasonTimeoutBelowMinimum refuses a policy whose provider timeout is
// positive and shorter than 1ms. Praxis records that timeout as timeout_ms
// and rejects zero, so rendering the duration would stall the post-auth rollout.
const reasonTimeoutBelowMinimum = "TimeoutBelowMinimum"

// providerIndexKey indexes each AIGuardrail by the NemoGuardrails it
// references, formatted as "<namespace>/<name>".
//

// AIGuardrail indexes NemoGuardrails by the namespace and name of the
// referenced resource, formatted as "<namespace>/<name>".
const providerIndexKey = ".spec.provider.nemo.ref"

// consumerMode is a NemoGuardrails spec.allowedConsumers.namespaces.from
// value: the provider-owned rule for which namespaces may reference it,
// analogous to Gateway API's target-owned allowedRoutes.
type consumerMode string

const (
	// consumerModeSame allows only AIGuardrail resources in the
	// NemoGuardrails resource's own namespace. This is the default whenever
	// allowedConsumers, namespaces or from is omitted.
	consumerModeSame consumerMode = "Same"
	// consumerModeSelector allows only AIGuardrail resources whose Namespace
	// object's labels match the selector. Same-namespace consumers must match
	// too — the selector is not additive to Same.
	consumerModeSelector consumerMode = "Selector"
	// consumerModeAll allows any namespace, including the provider's own.
	consumerModeAll consumerMode = "All"
)
