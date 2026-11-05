//go:build envtest

package envtest_test

import (
	"context"
	"encoding/json"
	"strings"
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
	"github.com/opendatahub-io/ai-gateway-controller/pkg/guardrail"
	"github.com/opendatahub-io/ai-gateway-controller/pkg/publisher"
	"github.com/opendatahub-io/ai-gateway-controller/pkg/tenant"
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
		// The model is Ready only once its tenant's ExtProc is available.
		// envtest runs no Deployment controller, so mark it available as its
		// pods would make it.
		var extProc appsv1.Deployment
		require.NoError(collect, c.Get(t.Context(), client.ObjectKey{
			Namespace: model.Namespace, Name: "payload-processing-external-model-" + model.Namespace,
		}, &extProc))
		require.NoError(collect, markAvailable(t.Context(), c, &extProc))
		// It also waits for the Gateway to accept its route; envtest runs no
		// Gateway controller either.
		require.NoError(collect, markRouteAccepted(t.Context(), c, model.Namespace, "external-model-"+model.Name))
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

// awaitModelFailed waits until model reports Failed with reason and names no
// HTTPRoute.
func awaitModelFailed(t *testing.T, c client.Client, model *inferencev1alpha1.ExternalModel, reason string) {
	t.Helper()
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		var actual inferencev1alpha1.ExternalModel
		require.NoError(collect, c.Get(t.Context(), client.ObjectKeyFromObject(model), &actual))
		require.Equal(collect, "Failed", actual.Status.Phase)
		ready := apimeta.FindStatusCondition(actual.Status.Conditions, "Ready")
		require.NotNil(collect, ready)
		require.Equal(collect, reason, ready.Reason, ready.Message)
		require.Empty(collect, actual.Status.HTTPRouteName)
	}, eventuallyTimeout, pollInterval, "the model must report %s", reason)
}

// awaitGuardrailEnforced accepts one guardrail, attaches it through a
// subscription, and waits until the tenant post-auth config calls it and the
// ExternalModel route filter leaves that config enabled. It returns the
// subscription. Envtest has no Envoy or NeMo, so this checks the objects the
// dataplane would enforce, not a live blocked request.
func awaitGuardrailEnforced(t *testing.T, c client.Client, namespace string) *unstructured.Unstructured {
	t.Helper()
	subscription := &unstructured.Unstructured{Object: map[string]any{
		"spec": map[string]any{"guardrails": []any{map[string]any{"ref": map[string]any{"name": "safety"}}}},
	}}
	subscription.SetGroupVersionKind(tenant.MaaSSubscriptionGVK)
	subscription.SetName("team")
	subscription.SetNamespace(namespace)
	require.NoError(t, c.Create(t.Context(), subscription))

	policy := aiGuardrail("safety", namespace)
	require.NoError(t, c.Create(t.Context(), policy))
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(policy), policy))
	// The tenant compiler calls NeMo only in the MaasTenantConfig's
	// status.infraNamespace, which these fixtures set to namespace.
	policy.Status = aigatewayv1alpha1.AIGuardrailStatus{
		BindingRevision:  "rev",
		ProviderEndpoint: "http://guardrails." + namespace + ".svc",
		Conditions: []metav1.Condition{{
			Type:               guardrail.ConditionAccepted,
			Status:             metav1.ConditionTrue,
			Reason:             "Accepted",
			Message:            "provider binding accepted",
			ObservedGeneration: policy.Generation,
			LastTransitionTime: metav1.Now(),
		}},
	}
	require.NoError(t, c.Status().Update(t.Context(), policy))

	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		var config corev1.ConfigMap
		require.NoError(collect, c.Get(t.Context(), client.ObjectKey{
			Namespace: gatewayNamespace, Name: tenant.PayloadProcessingPluginsConfigMapForTenant(namespace),
		}, &config))
		extproc := config.Data["extproc.yaml"]
		require.Contains(collect, extproc, "filter: ai_guardrails")
		require.Contains(collect, extproc, "safety")

		filter := &unstructured.Unstructured{}
		filter.SetAPIVersion("networking.istio.io/v1alpha3")
		filter.SetKind("EnvoyFilter")
		require.NoError(collect, c.Get(t.Context(), client.ObjectKey{
			Namespace: gatewayNamespace, Name: tenant.PayloadProcessingExternalModelEnvoyFilterName(namespace),
		}, filter))
		patches, _, err := unstructured.NestedSlice(filter.Object, "spec", "configPatches")
		require.NoError(collect, err)
		require.NotEmpty(collect, patches)
		for _, raw := range patches {
			patch, ok := raw.(map[string]any)
			require.True(collect, ok, "route patch = %T, want a map", raw)
			disabled, found, nestedErr := unstructured.NestedBool(patch, "patch", "value", "typed_per_filter_config", "envoy.filters.http.ext_proc.ipp", "disabled")
			require.NoError(collect, nestedErr)
			require.False(collect, found && disabled, "ExternalModel route disabled the guardrail filter")
		}
		require.False(collect, strings.Contains(extproc, "static_response"), "the accepted check must be called, not rejected")
	}, eventuallyTimeout, pollInterval, "the accepted guardrail must be compiled and left on for ExternalModel routes")
	return subscription
}

// awaitGuardrailDetached removes the guardrail's only attachment and waits
// until the post-auth config no longer calls it and the ExternalModel routes
// turn the post-auth filter off.
func awaitGuardrailDetached(t *testing.T, c client.Client, namespace string, attachment *unstructured.Unstructured) {
	t.Helper()
	require.NoError(t, c.Delete(t.Context(), attachment))
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		var config corev1.ConfigMap
		require.NoError(collect, c.Get(t.Context(), client.ObjectKey{
			Namespace: gatewayNamespace, Name: tenant.PayloadProcessingPluginsConfigMapForTenant(namespace),
		}, &config))
		require.NotContains(collect, config.Data["extproc.yaml"], "filter: ai_guardrails")

		filter := &unstructured.Unstructured{}
		filter.SetAPIVersion("networking.istio.io/v1alpha3")
		filter.SetKind("EnvoyFilter")
		require.NoError(collect, c.Get(t.Context(), client.ObjectKey{
			Namespace: gatewayNamespace, Name: tenant.PayloadProcessingExternalModelEnvoyFilterName(namespace),
		}, filter))
		patches, _, err := unstructured.NestedSlice(filter.Object, "spec", "configPatches")
		require.NoError(collect, err)
		require.NotEmpty(collect, patches)
		for _, raw := range patches {
			patch, ok := raw.(map[string]any)
			require.True(collect, ok, "route patch = %T, want a map", raw)
			disabled, _, nestedErr := unstructured.NestedBool(patch, "patch", "value", "typed_per_filter_config", "envoy.filters.http.ext_proc.ipp", "disabled")
			require.NoError(collect, nestedErr)
			require.True(collect, disabled, "ExternalModel route kept the post-auth filter on for a detached guardrail")
		}
	}, eventuallyTimeout, pollInterval, "a detached guardrail must stop being compiled and enforced")
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

// markAvailable sets the Deployment's Available condition.
func markAvailable(ctx context.Context, c client.Client, deployment *appsv1.Deployment) error {
	for _, condition := range deployment.Status.Conditions {
		if condition.Type == appsv1.DeploymentAvailable && condition.Status == corev1.ConditionTrue {
			return nil
		}
	}
	deployment.Status.Conditions = append(deployment.Status.Conditions, appsv1.DeploymentCondition{
		Type: appsv1.DeploymentAvailable, Status: corev1.ConditionTrue, Reason: "MinimumReplicasAvailable",
	})
	return c.Status().Update(ctx, deployment)
}

// markRouteAccepted reports the HTTPRoute Accepted by the tenant's gateway, as
// its Gateway controller would.
func markRouteAccepted(ctx context.Context, c client.Client, namespace, name string) error {
	route := &unstructured.Unstructured{}
	route.SetAPIVersion("gateway.networking.k8s.io/v1")
	route.SetKind("HTTPRoute")
	if err := c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, route); err != nil {
		return err
	}
	// Accepted already, unless for an earlier generation of the route.
	if parents, _, _ := unstructured.NestedSlice(route.Object, "status", "parents"); len(parents) == 1 {
		parent, _ := parents[0].(map[string]any)
		if conditions, _, _ := unstructured.NestedSlice(parent, "conditions"); len(conditions) == 1 {
			if condition, _ := conditions[0].(map[string]any); condition["observedGeneration"] == route.GetGeneration() {
				return nil
			}
		}
	}
	parents := []any{map[string]any{
		"parentRef":      map[string]any{"group": "gateway.networking.k8s.io", "kind": "Gateway", "name": gatewayName, "namespace": gatewayNamespace},
		"controllerName": "istio.io/gateway-controller",
		"conditions": []any{map[string]any{
			"type": "Accepted", "status": string(metav1.ConditionTrue), "reason": "Accepted", "message": "Route was valid",
			"observedGeneration": route.GetGeneration(), "lastTransitionTime": metav1.Now().UTC().Format(time.RFC3339),
		}},
	}}
	if err := unstructured.SetNestedSlice(route.Object, parents, "status", "parents"); err != nil {
		return err
	}
	return c.Status().Update(ctx, route)
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
