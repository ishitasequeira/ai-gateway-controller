/*
Copyright 2026 The opendatahub.io Authors.

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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// GuardrailPhase identifies when a guardrail check is applied.
//
// +kubebuilder:validation:Enum=Input;Output
type GuardrailPhase string

const (
	GuardrailPhaseInput  GuardrailPhase = "Input"
	GuardrailPhaseOutput GuardrailPhase = "Output"
)

// Printer columns show the acceptance verdict with its Reason beside it: the
// status alone does not say why a policy is not enforcing, and a guardrail
// that silently fails to bind is the failure mode operators most need to see.
// There is deliberately no Ready column — this type publishes no Ready
// condition, so one would render permanently empty, which is what the
// original column did.
//
// This block is detached from the doc comment on purpose. controller-gen
// copies the doc comment verbatim into the CRD's user-facing description, and
// implementation notes do not belong in a published API schema.

// AIGuardrail is a reusable, ordered collection of checks backed by a
// TrustyAI NemoGuardrails service.
//
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Provider",type="string",JSONPath=".spec.provider.nemo.ref.name"
// +kubebuilder:printcolumn:name="Accepted",type="string",JSONPath=".status.conditions[?(@.type==\"Accepted\")].status"
// +kubebuilder:printcolumn:name="Reason",type="string",JSONPath=".status.conditions[?(@.type==\"Accepted\")].reason"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"
type AIGuardrail struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   AIGuardrailSpec   `json:"spec"`
	Status AIGuardrailStatus `json:"status,omitempty"`
}

// AIGuardrailSpec defines a guardrail provider and its ordered checks.
type AIGuardrailSpec struct {
	// Provider identifies the service used to execute the checks.
	// +kubebuilder:validation:Required
	Provider AIGuardrailProvider `json:"provider"`

	// Checks is the ordered list of independent checks. All selected checks
	// must pass. Check names are unique within this list.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=64
	// +kubebuilder:validation:XValidation:rule="self.all(x, self.exists_one(y, y.name == x.name))",message="check names must be unique"
	// +listType=atomic
	Checks []AIGuardrailCheck `json:"checks"`
}

// AIGuardrailProvider configures the TrustyAI provider used by a guardrail.
type AIGuardrailProvider struct {
	// Nemo references a NemoGuardrails resource. If Namespace is omitted,
	// the reference resolves in the AIGuardrail namespace.
	// +kubebuilder:validation:Required
	Nemo AIGuardrailNemoProvider `json:"nemo"`

	// Format=duration is omitted because it admits strings metav1.Duration cannot
	// decode. Kept apart from the field doc so it stays out of the CRD description.

	// Timeout is the maximum duration for an individual provider call.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Type=string
	// +kubebuilder:validation:MaxLength=64
	// +kubebuilder:validation:XValidation:rule="duration(self) > duration('0s')",message="timeout must be a valid positive duration"
	// +kubebuilder:validation:XValidation:rule="duration(self) >= duration('1ms')",message="timeout must be at least 1ms"
	Timeout metav1.Duration `json:"timeout"`
}

// AIGuardrailNemoProvider is the typed NeMo provider binding.
type AIGuardrailNemoProvider struct {
	// Ref references a NemoGuardrails resource.
	// +kubebuilder:validation:Required
	Ref AIGuardrailNamespacedReference `json:"ref"`
}

// AIGuardrailNamespacedReference references a namespaced provider resource.
// Namespace is optional and defaults to the AIGuardrail namespace.
type AIGuardrailNamespacedReference struct {
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$`
	Name string `json:"name"`

	// +optional
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
	Namespace string `json:"namespace,omitempty"`
}

// AIGuardrailCheck defines one named NeMo configuration and the phases in
// which it is evaluated.
type AIGuardrailCheck struct {
	// Name is the stable name used by attachment resources to select this
	// check.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
	Name string `json:"name"`

	// ConfigID identifies the configuration loaded by NemoGuardrails.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	ConfigID string `json:"configId"`

	// Phases is the non-empty set of request phases for this check.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=2
	// +listType=set
	Phases []GuardrailPhase `json:"phases"`
}

// AIGuardrailStatus defines the observed provider binding and readiness.
type AIGuardrailStatus struct {
	// ObservedGeneration is the latest spec generation processed by the
	// controller.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// ProviderEndpoint is the in-cluster base URL of the NeMo Guardrails
	// server, read verbatim from NemoGuardrails.status.endpoint.
	//
	// It is published so a consumer compiles against the same endpoint this
	// controller accepted, rather than re-deriving one from the provider's
	// name. It is set only while Accepted is True and cleared whenever the
	// policy is refused.
	//
	// A published endpoint means the server is addressable and
	// authenticated, not that it is serving: TrustyAI writes this field
	// whenever the server is auth-protected, including while its pods are
	// still coming up.
	// +optional
	ProviderEndpoint string `json:"providerEndpoint,omitempty"`

	// BindingRevision is a content digest of the provider identity, endpoint,
	// configuration and permission dependencies behind an accepted binding:
	// the inputs that can change without changing this policy's
	// metadata.generation. Equal revisions mean equal dependencies, so a
	// consumer compares it for equality rather than ordering it. It is set
	// only while Accepted is True and cleared whenever the policy is refused,
	// so a consumer gating on it never matches a binding that is no longer
	// accepted.
	//
	// It does not detect every change. The content of a ConfigMap behind a
	// provider configuration name is not observable from these resources, so
	// editing one can change effective safety while this digest stays
	// constant. Version configuration resources immutably where that
	// distinction matters.
	// +optional
	BindingRevision string `json:"bindingRevision,omitempty"`

	// Conditions report sanitized acceptance, reference, provider and
	// compatibility state. Credentials and provider configuration are never
	// exposed here.
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true

// AIGuardrailList contains a list of AIGuardrail resources.
type AIGuardrailList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []AIGuardrail `json:"items"`
}
