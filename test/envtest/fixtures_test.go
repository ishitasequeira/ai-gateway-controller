//go:build envtest

package envtest_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"

	aigatewayv1alpha1 "github.com/opendatahub-io/ai-gateway-controller/api/aigateway/v1alpha1"
	inferencev1alpha1 "github.com/opendatahub-io/ai-gateway-controller/api/inference/v1alpha1"
	"github.com/opendatahub-io/ai-gateway-controller/pkg/envelope"
	"github.com/opendatahub-io/ai-gateway-controller/pkg/publisher"
)

// externalModelFinalizer is pinned rather than imported: it is stored on live
// objects, so renaming it in the controller must fail this suite.
const externalModelFinalizer = "inference.opendatahub.io/external-model-cleanup"

func createNamespace(t *testing.T, c client.Client, name string) {
	t.Helper()
	require.NoError(t, c.Create(t.Context(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}))
}

func createProvider(t *testing.T, c client.Client, namespace string) *inferencev1alpha1.ExternalProvider {
	t.Helper()
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "credentials", Namespace: namespace},
		Data:       map[string][]byte{"api-key": []byte("fixture-only")},
	}
	provider := &inferencev1alpha1.ExternalProvider{
		ObjectMeta: metav1.ObjectMeta{Name: "provider", Namespace: namespace},
		Spec: inferencev1alpha1.ExternalProviderSpec{
			Provider: "openai", Endpoint: "api.example.com",
			Auth: inferencev1alpha1.AuthConfig{Type: "apikey", SecretRef: inferencev1alpha1.NameReference{Name: secret.Name}},
		},
	}
	for _, obj := range []client.Object{secret, provider} {
		require.NoError(t, c.Create(t.Context(), obj))
	}
	return provider
}

func aiGuardrail(name, namespace string) *aigatewayv1alpha1.AIGuardrail {
	return &aigatewayv1alpha1.AIGuardrail{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: aigatewayv1alpha1.AIGuardrailSpec{
			Provider: aigatewayv1alpha1.AIGuardrailProvider{
				Nemo: aigatewayv1alpha1.AIGuardrailNemoProvider{
					Ref: aigatewayv1alpha1.AIGuardrailNamespacedReference{Name: "guardrails"},
				},
				Timeout: metav1.Duration{Duration: 30 * time.Second},
			},
			Checks: []aigatewayv1alpha1.AIGuardrailCheck{{
				Name: "safety", ConfigID: "safety",
				Phases: []aigatewayv1alpha1.GuardrailPhase{aigatewayv1alpha1.GuardrailPhaseInput},
			}},
		},
	}
}

type externalModelOption func(*inferencev1alpha1.ExternalModel)

func externalModel(name, namespace string, options ...externalModelOption) *inferencev1alpha1.ExternalModel {
	model := &inferencev1alpha1.ExternalModel{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace}}
	for _, option := range options {
		option(model)
	}
	return model
}

func withModelName(name string) externalModelOption {
	return func(model *inferencev1alpha1.ExternalModel) { model.Spec.ModelName = name }
}

func withProviderModel(provider, targetModel string) externalModelOption {
	return func(model *inferencev1alpha1.ExternalModel) {
		model.Spec.ExternalProviderRefs = append(model.Spec.ExternalProviderRefs, inferencev1alpha1.ExternalProviderRef{
			Ref: inferencev1alpha1.ExternalProviderReference{Name: provider}, TargetModel: targetModel,
			APIFormat: "openai-chat", Path: "/v1/chat/completions",
		})
	}
}

func awaitPraxisInstalled(t *testing.T, c client.Client, namespace string) {
	t.Helper()
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		var deployment appsv1.Deployment
		require.NoError(collect, c.Get(t.Context(), client.ObjectKey{Namespace: gatewayNamespace, Name: "payload-processing-" + namespace}, &deployment))
		require.NotEmpty(collect, deployment.Spec.Template.Spec.Containers)
		require.Equal(collect, "quay.io/example/praxis-extproc:test", deployment.Spec.Template.Spec.Containers[0].Image)
		var binding rbacv1.ClusterRoleBinding
		require.NoError(collect, c.Get(t.Context(), client.ObjectKey{Name: "payload-processing-reader-" + namespace}, &binding))
		require.Equal(collect, "payload-processing-reader", binding.RoleRef.Name)
	}, eventuallyTimeout, pollInterval, "the existing tenant must have its Praxis installation")
}

// awaitModelPublished returns the name of the HTTPRoute published for model.
func awaitModelPublished(t *testing.T, c client.Client, model *inferencev1alpha1.ExternalModel, provider *inferencev1alpha1.ExternalProvider) string {
	t.Helper()
	var routeName string
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		var actual inferencev1alpha1.ExternalModel
		require.NoError(collect, c.Get(t.Context(), client.ObjectKeyFromObject(model), &actual))
		require.Contains(collect, actual.Finalizers, externalModelFinalizer)
		require.Equal(collect, model.Generation, actual.Status.ObservedGeneration)
		require.True(collect, apimeta.IsStatusConditionTrue(actual.Status.Conditions, "Ready"), "%+v", actual.Status.Conditions)
		var overlay corev1.ConfigMap
		require.NoError(collect, c.Get(t.Context(), client.ObjectKey{Namespace: model.Namespace, Name: publisher.DefaultName}, &overlay))
		var published envelope.Envelope
		require.NoError(collect, json.Unmarshal([]byte(overlay.Data[publisher.DefaultDataKey]), &published))
		require.Equal(collect, gatewayName, published.Scope.Gateway, "the overlay must target the tenant's gateway")
		require.Len(collect, published.Overlay.Candidates, 1)
		require.Equal(collect, model.Spec.ModelName, published.Overlay.Candidates[0].Name)
		require.Equal(collect, "provider-"+provider.Name, published.Overlay.Candidates[0].Cluster)
		route := &unstructured.Unstructured{}
		route.SetAPIVersion("gateway.networking.k8s.io/v1")
		route.SetKind("HTTPRoute")
		require.NoError(collect, c.Get(t.Context(), client.ObjectKey{Namespace: model.Namespace, Name: actual.Status.HTTPRouteName}, route))
		parentRefs, _, err := unstructured.NestedSlice(route.Object, "spec", "parentRefs")
		require.NoError(collect, err)
		require.Equal(collect, []any{map[string]any{
			"group": "gateway.networking.k8s.io", "kind": "Gateway", "name": gatewayName, "namespace": gatewayNamespace,
		}}, parentRefs, "the route must attach to the tenant's gateway")
		// The tenant controller must consume the published overlay to configure
		// the workload, exercising both controllers through their watches.
		var deployment appsv1.Deployment
		require.NoError(collect, c.Get(t.Context(), client.ObjectKey{
			Namespace: model.Namespace, Name: "payload-processing-external-model-" + model.Namespace,
		}, &deployment))
		require.Contains(collect, projectedSecretNames(deployment), provider.Spec.Auth.SecretRef.Name,
			"the ExtProc workload must mount the provider credentials")
		routeName = actual.Status.HTTPRouteName
	}, eventuallyTimeout, pollInterval, "the requested model and provider must be published with their workload")
	return routeName
}

func awaitModelRemoved(t *testing.T, c client.Client, model *inferencev1alpha1.ExternalModel, routeName string) {
	t.Helper()
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		err := c.Get(t.Context(), client.ObjectKeyFromObject(model), &inferencev1alpha1.ExternalModel{})
		require.True(collect, apierrors.IsNotFound(err), "the model must be released by its finalizer, got %v", err)
		key := client.ObjectKey{Namespace: model.Namespace, Name: publisher.DefaultName}
		require.True(collect, apierrors.IsNotFound(c.Get(t.Context(), key, &corev1.ConfigMap{})), "routing overlay remains")
		route := &unstructured.Unstructured{}
		route.SetAPIVersion("gateway.networking.k8s.io/v1")
		route.SetKind("HTTPRoute")
		key.Name = routeName
		require.True(collect, apierrors.IsNotFound(c.Get(t.Context(), key, route)), "model route remains")
		key.Name = "payload-processing-external-model-" + model.Namespace
		require.True(collect, apierrors.IsNotFound(c.Get(t.Context(), key, &appsv1.Deployment{})), "ExtProc workload remains")
	}, eventuallyTimeout, pollInterval, "deleting the last model must remove its routing and workload")
}

func projectedSecretNames(deployment appsv1.Deployment) []string {
	var names []string
	for _, volume := range deployment.Spec.Template.Spec.Volumes {
		if volume.Projected == nil {
			continue
		}
		for _, source := range volume.Projected.Sources {
			if source.Secret != nil {
				names = append(names, source.Secret.Name)
			}
		}
	}
	return names
}
