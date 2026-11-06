//go:build envtest

package envtest_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/opendatahub-io/ai-gateway-controller/pkg/guardrail"
)

// enableAuthAnnotation is what makes TrustyAI publish status.endpoint on a
// real cluster. Nothing here reads it — these scenarios write the status
// directly — but the provider fixtures carry it so they match what an operator
// applies.
const enableAuthAnnotation = "security.opendatahub.io/enable-auth"

// nemoGuardrailsCRDName is the resource the controller's RESTMapper looks for
// to decide whether TrustyAI is installed at all.
const nemoGuardrailsCRDName = "nemoguardrails.trustyai.opendatahub.io"

// nemoGuardrailsCRD defines TrustyAI's NemoGuardrails well enough for this
// controller, which reads it unstructured by GVK and depends only on
// spec.allowedConsumers, spec.nemoConfigs and status.workloadNamespace.
//
// The schema preserves unknown fields rather than restating TrustyAI's. Two
// reasons, and neither is brevity. A copy of their schema would drift silently
// and start failing for reasons that have nothing to do with this controller.
// More importantly, spec.allowedConsumers is not upstream yet, so there is no
// published schema to copy — and the InvalidAllowedConsumers verdict can only
// be reached if the API server admits a value the controller then refuses. A
// validating schema would reject it at admission and the refusal path would
// never run.
func nemoGuardrailsCRD() *apiextensionsv1.CustomResourceDefinition {
	openAPI := func() *apiextensionsv1.JSONSchemaProps {
		return &apiextensionsv1.JSONSchemaProps{
			Type:                   "object",
			XPreserveUnknownFields: ptr.To(true),
		}
	}
	return &apiextensionsv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: nemoGuardrailsCRDName},
		Spec: apiextensionsv1.CustomResourceDefinitionSpec{
			Group: guardrail.NemoGuardrailsGVK.Group,
			Scope: apiextensionsv1.NamespaceScoped,
			Names: apiextensionsv1.CustomResourceDefinitionNames{
				Plural:   "nemoguardrails",
				Singular: "nemoguardrails",
				Kind:     guardrail.NemoGuardrailsGVK.Kind,
				ListKind: guardrail.NemoGuardrailsGVK.Kind + "List",
			},
			Versions: []apiextensionsv1.CustomResourceDefinitionVersion{{
				Name:    guardrail.NemoGuardrailsGVK.Version,
				Served:  true,
				Storage: true,
				Subresources: &apiextensionsv1.CustomResourceSubresources{
					Status: &apiextensionsv1.CustomResourceSubresourceStatus{},
				},
				Schema: &apiextensionsv1.CustomResourceValidation{
					OpenAPIV3Schema: &apiextensionsv1.JSONSchemaProps{
						Type: "object",
						Properties: map[string]apiextensionsv1.JSONSchemaProps{
							"spec":   *openAPI(),
							"status": *openAPI(),
						},
					},
				},
			}},
		},
	}
}

// siblingCRDName is a second kind in TrustyAI's group.
const siblingCRDName = "trustyaiservices.trustyai.opendatahub.io"

// trustyAIServiceCRD stands in for the other kinds TrustyAI ships alongside
// NemoGuardrails, in the same group and version.
//
// Nothing reads it. It exists so a scenario can remove the NemoGuardrails CRD
// and leave trustyai.opendatahub.io/v1alpha1 still served, which is what
// uninstalling that one CRD looks like on a real cluster. With only the one
// CRD in the group, deleting it takes the whole group/version with it, and the
// controller's discovery check never has to find the Kind missing from a path
// that still answers.
func trustyAIServiceCRD() *apiextensionsv1.CustomResourceDefinition {
	return &apiextensionsv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: siblingCRDName},
		Spec: apiextensionsv1.CustomResourceDefinitionSpec{
			Group: guardrail.NemoGuardrailsGVK.Group,
			Scope: apiextensionsv1.NamespaceScoped,
			Names: apiextensionsv1.CustomResourceDefinitionNames{
				Plural:   "trustyaiservices",
				Singular: "trustyaiservice",
				Kind:     "TrustyAIService",
				ListKind: "TrustyAIServiceList",
			},
			Versions: []apiextensionsv1.CustomResourceDefinitionVersion{{
				Name:    guardrail.NemoGuardrailsGVK.Version,
				Served:  true,
				Storage: true,
				Schema: &apiextensionsv1.CustomResourceValidation{
					OpenAPIV3Schema: &apiextensionsv1.JSONSchemaProps{
						Type:                   "object",
						XPreserveUnknownFields: ptr.To(true),
					},
				},
			}},
		},
	}
}

// providerOption edits a provider before it is created.
type providerOption func(*unstructured.Unstructured)

// nemoProvider builds a provider declaring configID, composed of the named
// ConfigMaps. No ConfigMap is created: this controller reads their names off
// the provider and never fetches them, which is the whole reason a changed
// ConfigMap is invisible to bindingRevision.
func nemoProvider(namespace, name, configID string, configMaps []string, options ...providerOption) *unstructured.Unstructured {
	mounted := make([]any, 0, len(configMaps))
	for _, configMap := range configMaps {
		mounted = append(mounted, configMap)
	}

	provider := guardrail.NewNemoGuardrails()
	provider.SetNamespace(namespace)
	provider.SetName(name)
	provider.SetAnnotations(map[string]string{enableAuthAnnotation: "true"})
	provider.Object["spec"] = map[string]any{
		"nemoConfigs": []any{
			map[string]any{"name": configID, "default": true, "configMaps": mounted},
		},
	}
	for _, option := range options {
		option(provider)
	}
	return provider
}

// withAllowedConsumers sets the provider-owned permission rule. from is
// written verbatim so a scenario can plant a value the controller must refuse.
func withAllowedConsumers(from string, selector map[string]any) providerOption {
	return func(provider *unstructured.Unstructured) {
		namespaces := map[string]any{"from": from}
		if selector != nil {
			namespaces["selector"] = selector
		}
		spec, _ := provider.Object["spec"].(map[string]any)
		spec["allowedConsumers"] = map[string]any{"namespaces": namespaces}
	}
}

// createReadyProvider creates a provider and publishes the workload namespace
// TrustyAI would publish once its server is addressable.
func createReadyProvider(t *testing.T, c client.Client, provider *unstructured.Unstructured) *unstructured.Unstructured {
	t.Helper()
	require.NoError(t, c.Create(t.Context(), provider))
	publishWorkloadNamespace(t, c, provider)
	return provider
}

// publishWorkloadNamespace writes status.workloadNamespace, standing in for
// TrustyAI. It is the only status field this controller reads for provider
// readiness: the Service behind it is derived, not published. The controller
// accepts only an AI Gateway infrastructure namespace, so the workload runs in
// infraNamespace, the namespace every fixture tenant publishes.
func publishWorkloadNamespace(t *testing.T, c client.Client, provider *unstructured.Unstructured) {
	t.Helper()
	require.NoError(t, unstructured.SetNestedField(provider.Object, infraNamespace, "status", "workloadNamespace"))
	require.NoError(t, c.Status().Update(t.Context(), provider))
}
