//go:build envtest

package envtest_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"

	aigatewayv1alpha1 "github.com/opendatahub-io/ai-gateway-controller/api/aigateway/v1alpha1"
	"github.com/opendatahub-io/ai-gateway-controller/pkg/guardrail"
)

// Accepted condition reasons, spelled out rather than imported. They are
// unexported in pkg/guardrail, and these are the strings a cluster publishes
// to consumers — pinning them here means a rename has to be deliberate
// instead of agreeing with the controller by construction.
const (
	reasonPolicyAccepted               = "PolicyAccepted"
	reasonProviderNotFound             = "ProviderNotFound"
	reasonConsumerNotAuthorized        = "ConsumerNotAuthorized"
	reasonInvalidAllowedConsumers      = "InvalidAllowedConsumers"
	reasonUnknownCheckConfig           = "UnknownCheckConfig"
	reasonProviderNotReady             = "ProviderNotReady"
	reasonEndpointDiscoveryUnavailable = "EndpointDiscoveryUnavailable"
	reasonProviderCRDNotInstalled      = "ProviderCRDNotInstalled"
)

// defaultConfigID is the configuration the fixtures declare and the baseline
// policy selects.
const defaultConfigID = "default-all"

// guardrailPolicy builds an AIGuardrail selecting configID on a provider.
// providerNamespace is empty for a same-namespace reference, which is the case
// the default Same permission authorizes.
func guardrailPolicy(namespace, providerName, providerNamespace, configID string) *aigatewayv1alpha1.AIGuardrail {
	return &aigatewayv1alpha1.AIGuardrail{
		ObjectMeta: metav1.ObjectMeta{Name: "policy", Namespace: namespace},
		Spec: aigatewayv1alpha1.AIGuardrailSpec{
			Provider: aigatewayv1alpha1.AIGuardrailProvider{
				Timeout: metav1.Duration{Duration: 5 * time.Second},
				Nemo: aigatewayv1alpha1.AIGuardrailNemoProvider{
					Ref: aigatewayv1alpha1.AIGuardrailNamespacedReference{
						Name: providerName, Namespace: providerNamespace,
					},
				},
			},
			Checks: []aigatewayv1alpha1.AIGuardrailCheck{{
				Name: "content-safety", ConfigID: configID,
				Phases: []aigatewayv1alpha1.GuardrailPhase{aigatewayv1alpha1.GuardrailPhaseInput},
			}},
		},
	}
}

// awaitAcceptedReason waits for the Accepted condition to carry wantReason for
// the policy's current generation, and returns the status as it stood then.
//
// The generation gate is what makes this an assertion rather than a race. A
// verdict left over from an earlier spec says nothing about the edit a
// scenario just made, and without the gate an unchanged stale condition would
// satisfy several of these cases the instant they start polling.
func awaitAcceptedReason(t *testing.T, c client.Client, policy client.Object, wantReason string) aigatewayv1alpha1.AIGuardrailStatus {
	t.Helper()
	var observed aigatewayv1alpha1.AIGuardrailStatus
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		var current aigatewayv1alpha1.AIGuardrail
		if !assert.NoError(collect, c.Get(t.Context(), client.ObjectKeyFromObject(policy), &current)) {
			return
		}
		accepted := guardrailCondition(current, guardrail.ConditionAccepted)
		if !assert.NotNil(collect, accepted, "no Accepted condition published yet") {
			return
		}
		assert.Equal(collect, current.Generation, accepted.ObservedGeneration,
			"Accepted still reports on an older generation")
		assert.Equal(collect, wantReason, accepted.Reason, "Accepted says: %s", accepted.Message)
		assert.NotEmpty(collect, accepted.Message, "the verdict has to say why")
		observed = current.Status
	}, eventuallyTimeout, pollInterval)
	return observed
}

// guardrailCondition returns the named condition, or nil while the controller
// has not published it.
func guardrailCondition(policy aigatewayv1alpha1.AIGuardrail, conditionType string) *metav1.Condition {
	for i := range policy.Status.Conditions {
		if policy.Status.Conditions[i].Type == conditionType {
			return &policy.Status.Conditions[i]
		}
	}
	return nil
}

// requireBindingPublished asserts the two fields a consumer compiles against
// are set. Both are published only while the binding is accepted.
func requireBindingPublished(t *testing.T, status aigatewayv1alpha1.AIGuardrailStatus) {
	t.Helper()
	require.NotEmpty(t, status.ProviderEndpoint, "an accepted binding must publish the endpoint it accepted")
	require.NotEmpty(t, status.BindingRevision, "an accepted binding must publish its revision")
}

// requireBindingWithdrawn asserts a refusal leaves nothing to gate on. A
// surviving endpoint or revision would let a consumer keep compiling against a
// binding that is no longer accepted.
func requireBindingWithdrawn(t *testing.T, status aigatewayv1alpha1.AIGuardrailStatus) {
	t.Helper()
	require.Empty(t, status.ProviderEndpoint, "a refused policy must not publish an endpoint")
	require.Empty(t, status.BindingRevision, "a refused policy must not publish a revision")
}

// testGuardrailBinding covers the provider binding verdicts the AIGuardrail
// reconciler publishes. Every scenario works in its own namespaces, so none
// inherits another's objects.
//
//nolint:maintidx // one scenario per sub-test; splitting them hides the table.
func testGuardrailBinding(t *testing.T, c client.Client) {
	t.Helper()

	t.Run("a policy binding a ready provider in its own namespace is accepted", func(t *testing.T) {
		const namespace = "guardrail-baseline"
		createNamespace(t, c, namespace)
		createReadyProvider(t, c, nemoProvider(namespace, "nemo", defaultConfigID, []string{"base-rails"}))

		policy := guardrailPolicy(namespace, "nemo", "", defaultConfigID)
		require.NoError(t, c.Create(t.Context(), policy))

		status := awaitAcceptedReason(t, c, policy, reasonPolicyAccepted)
		requireBindingPublished(t, status)
		require.Equal(t, "ChecksSatisfiable", guardrailCondition(
			aigatewayv1alpha1.AIGuardrail{Status: status}, guardrail.ConditionCompatible).Reason)
		require.Equal(t, "ProviderAvailable", guardrailCondition(
			aigatewayv1alpha1.AIGuardrail{Status: status}, guardrail.ConditionProviderReady).Reason)
	})

	t.Run("deleting the provider withdraws the accepted binding", func(t *testing.T) {
		const namespace = "guardrail-provider-deleted"
		createNamespace(t, c, namespace)
		provider := createReadyProvider(t, c, nemoProvider(namespace, "nemo", defaultConfigID, []string{"base-rails"}))

		policy := guardrailPolicy(namespace, "nemo", "", defaultConfigID)
		require.NoError(t, c.Create(t.Context(), policy))
		requireBindingPublished(t, awaitAcceptedReason(t, c, policy, reasonPolicyAccepted))

		require.NoError(t, c.Delete(t.Context(), provider))

		// The refusal has to withdraw the binding, not just restate the
		// reason: a consumer gating on bindingRevision would otherwise keep
		// compiling against a provider that no longer exists.
		requireBindingWithdrawn(t, awaitAcceptedReason(t, c, policy, reasonProviderNotFound))
	})

	t.Run("a consumer in another namespace is refused under the default rule", func(t *testing.T) {
		const providerNamespace, consumerNamespace = "guardrail-x-provider", "guardrail-x-consumer"
		createNamespace(t, c, providerNamespace)
		createNamespace(t, c, consumerNamespace)
		createReadyProvider(t, c, nemoProvider(providerNamespace, "nemo", defaultConfigID, []string{"base-rails"}))

		policy := guardrailPolicy(consumerNamespace, "nemo", providerNamespace, defaultConfigID)
		require.NoError(t, c.Create(t.Context(), policy))

		requireBindingWithdrawn(t, awaitAcceptedReason(t, c, policy, reasonConsumerNotAuthorized))
	})

	t.Run("a provider opening itself to All accepts a consumer in another namespace", func(t *testing.T) {
		const providerNamespace, consumerNamespace = "guardrail-all-provider", "guardrail-all-consumer"
		createNamespace(t, c, providerNamespace)
		createNamespace(t, c, consumerNamespace)
		createReadyProvider(t, c, nemoProvider(providerNamespace, "nemo", defaultConfigID, []string{"base-rails"},
			withAllowedConsumers("All", nil)))

		policy := guardrailPolicy(consumerNamespace, "nemo", providerNamespace, defaultConfigID)
		require.NoError(t, c.Create(t.Context(), policy))

		requireBindingPublished(t, awaitAcceptedReason(t, c, policy, reasonPolicyAccepted))
	})

	t.Run("a Selector rule follows the consumer namespace's labels", func(t *testing.T) {
		const providerNamespace, consumerNamespace = "guardrail-sel-provider", "guardrail-sel-consumer"
		createNamespace(t, c, providerNamespace)
		createNamespace(t, c, consumerNamespace)
		createReadyProvider(t, c, nemoProvider(providerNamespace, "nemo", defaultConfigID, []string{"base-rails"},
			withAllowedConsumers("Selector", map[string]any{
				"matchLabels": map[string]any{"guardrails": "allowed"},
			})))

		policy := guardrailPolicy(consumerNamespace, "nemo", providerNamespace, defaultConfigID)
		require.NoError(t, c.Create(t.Context(), policy))
		requireBindingWithdrawn(t, awaitAcceptedReason(t, c, policy, reasonConsumerNotAuthorized))

		accepted := labelNamespace(t, c, consumerNamespace, "guardrails", "allowed")
		acceptedStatus := awaitAcceptedReason(t, c, policy, reasonPolicyAccepted)
		requireBindingPublished(t, acceptedStatus)

		// Revoking the label touches neither CR. The policy's generation does
		// not move, so nothing but the Namespace watch can carry this, which
		// is the whole reason that watch exists.
		labelNamespace(t, c, consumerNamespace, "guardrails", "")
		requireBindingWithdrawn(t, awaitAcceptedReason(t, c, policy, reasonConsumerNotAuthorized))
		require.NotEmpty(t, accepted, "the labelled namespace must have been observed")
	})

	t.Run("an unrecognised permission rule is refused rather than guessed at", func(t *testing.T) {
		const providerNamespace, consumerNamespace = "guardrail-bad-provider", "guardrail-bad-consumer"
		createNamespace(t, c, providerNamespace)
		createNamespace(t, c, consumerNamespace)
		createReadyProvider(t, c, nemoProvider(providerNamespace, "nemo", defaultConfigID, []string{"base-rails"},
			withAllowedConsumers("Nope", nil)))

		policy := guardrailPolicy(consumerNamespace, "nemo", providerNamespace, defaultConfigID)
		require.NoError(t, c.Create(t.Context(), policy))

		// Failing closed is the point: a rule this controller cannot read is
		// not evidence that the binding is permitted.
		requireBindingWithdrawn(t, awaitAcceptedReason(t, c, policy, reasonInvalidAllowedConsumers))
	})

	t.Run("a check selecting an undeclared configuration is refused", func(t *testing.T) {
		const namespace = "guardrail-unknown-config"
		createNamespace(t, c, namespace)
		createReadyProvider(t, c, nemoProvider(namespace, "nemo", defaultConfigID, []string{"base-rails"}))

		policy := guardrailPolicy(namespace, "nemo", "", "no-such-config")
		require.NoError(t, c.Create(t.Context(), policy))

		status := awaitAcceptedReason(t, c, policy, reasonUnknownCheckConfig)
		requireBindingWithdrawn(t, status)
		compatible := guardrailCondition(aigatewayv1alpha1.AIGuardrail{Status: status}, guardrail.ConditionCompatible)
		require.Equal(t, metav1.ConditionFalse, compatible.Status)
		require.Equal(t, reasonUnknownCheckConfig, compatible.Reason)
		require.Contains(t, compatible.Message, "no-such-config",
			"the message must name the configId to fix, not just report a mismatch")
	})

	t.Run("a provider publishing no endpoint is not ready", func(t *testing.T) {
		const namespace = "guardrail-no-endpoint"
		createNamespace(t, c, namespace)
		// Created without a status: status.workloadNamespace is what the
		// controller derives the Service from, so nothing is discoverable
		// until TrustyAI publishes it. The annotation is dropped with it
		// because an unprotected server is the shape that pairs with a
		// workload whose address is not settled.
		provider := nemoProvider(namespace, "nemo", defaultConfigID, []string{"base-rails"})
		provider.SetAnnotations(nil)
		require.NoError(t, c.Create(t.Context(), provider))

		policy := guardrailPolicy(namespace, "nemo", "", defaultConfigID)
		require.NoError(t, c.Create(t.Context(), policy))

		status := awaitAcceptedReason(t, c, policy, reasonProviderNotReady)
		requireBindingWithdrawn(t, status)
		ready := guardrailCondition(aigatewayv1alpha1.AIGuardrail{Status: status}, guardrail.ConditionProviderReady)
		require.Equal(t, metav1.ConditionFalse, ready.Status)
		require.Equal(t, reasonEndpointDiscoveryUnavailable, ready.Reason)
	})

	t.Run("editing which ConfigMaps compose a configuration moves the binding revision", func(t *testing.T) {
		const namespace = "guardrail-configmaps"
		createNamespace(t, c, namespace)
		provider := createReadyProvider(t, c, nemoProvider(namespace, "nemo", defaultConfigID, []string{"base-rails"}))

		policy := guardrailPolicy(namespace, "nemo", "", defaultConfigID)
		require.NoError(t, c.Create(t.Context(), policy))
		before := awaitAcceptedReason(t, c, policy, reasonPolicyAccepted)
		requireBindingPublished(t, before)

		// The policy is untouched, so its generation does not move and the
		// configId still resolves. The revision is the only signal a consumer
		// has that the rules behind that id changed.
		setConfigMaps(t, c, provider, []string{"base-rails", "pii-rails"})

		require.EventuallyWithT(t, func(collect *assert.CollectT) {
			var current aigatewayv1alpha1.AIGuardrail
			if !assert.NoError(collect, c.Get(t.Context(), client.ObjectKeyFromObject(policy), &current)) {
				return
			}
			assert.NotEqual(collect, before.BindingRevision, current.Status.BindingRevision,
				"the ConfigMaps composing the configuration changed, but the revision did not")
			assert.NotEmpty(collect, current.Status.BindingRevision)
		}, eventuallyTimeout, pollInterval)
	})

	t.Run("removing the provider CRD is reported as a missing platform component", func(t *testing.T) {
		// A missing CRD means the platform is incomplete, while a missing
		// object means the reference is wrong, and an operator fixes those in
		// different places.
		//
		// The controller is not restarted, so its RESTMapper still carries the
		// mapping it resolved while the CRD existed. Reaching the right reason
		// from here is what the discovery check on the refusal path is for.
		const namespace = "guardrail-crd-missing"
		createNamespace(t, c, namespace)
		createReadyProvider(t, c, nemoProvider(namespace, "nemo", defaultConfigID, []string{"base-rails"}))

		policy := guardrailPolicy(namespace, "nemo", "", defaultConfigID)
		require.NoError(t, c.Create(t.Context(), policy))
		requireBindingPublished(t, awaitAcceptedReason(t, c, policy, reasonPolicyAccepted))

		require.NoError(t, c.Delete(t.Context(), nemoGuardrailsCRD()))
		t.Cleanup(func() { restoreProviderCRD(t, c) })
		awaitCRDGone(t, c)

		requireBindingWithdrawn(t, awaitAcceptedReason(t, c, policy, reasonProviderCRDNotInstalled))
	})

	t.Run("removing only the provider CRD leaves its group served and is still reported as missing", func(t *testing.T) {
		// The scenario above removes the last CRD in TrustyAI's group, so the
		// whole group/version stops resolving. Uninstalling NemoGuardrails on
		// a real cluster does not look like that: the operator's other kinds
		// keep the group/version answering, and only the Kind goes away.
		//
		// That is the case a check trusting the group/version to disappear
		// would get wrong, and it is the one this suite could not produce
		// while NemoGuardrails was the only CRD in its group.
		const namespace = "guardrail-crd-kind-missing"

		require.NoError(t, c.Create(t.Context(), trustyAIServiceCRD()))
		t.Cleanup(func() {
			// Removed so the scenario after this one still sees the group
			// disappear along with the CRD.
			require.NoError(t, client.IgnoreNotFound(c.Delete(context.Background(), trustyAIServiceCRD())))
		})

		createNamespace(t, c, namespace)
		createReadyProvider(t, c, nemoProvider(namespace, "nemo", defaultConfigID, []string{"base-rails"}))

		policy := guardrailPolicy(namespace, "nemo", "", defaultConfigID)
		require.NoError(t, c.Create(t.Context(), policy))
		requireBindingPublished(t, awaitAcceptedReason(t, c, policy, reasonPolicyAccepted))

		require.NoError(t, c.Delete(t.Context(), nemoGuardrailsCRD()))
		t.Cleanup(func() { restoreProviderCRD(t, c) })
		awaitCRDGone(t, c)

		requireBindingWithdrawn(t, awaitAcceptedReason(t, c, policy, reasonProviderCRDNotInstalled))
	})

	// Last on purpose: it removes a cluster-scoped CRD the other scenarios
	// depend on, and restores it before returning.
	t.Run("policies recover on their own once the provider CRD is reinstalled", func(t *testing.T) {
		const namespace = "guardrail-crd-absent"
		createNamespace(t, c, namespace)
		createReadyProvider(t, c, nemoProvider(namespace, "nemo", defaultConfigID, []string{"base-rails"}))

		policy := guardrailPolicy(namespace, "nemo", "", defaultConfigID)
		require.NoError(t, c.Create(t.Context(), policy))
		requireBindingPublished(t, awaitAcceptedReason(t, c, policy, reasonPolicyAccepted))

		require.NoError(t, c.Delete(t.Context(), nemoGuardrailsCRD()))
		// The CRD is cluster-scoped, so a failure anywhere between here and
		// the reinstall below would leave it deleted for whatever runs next.
		t.Cleanup(func() { restoreProviderCRD(t, c) })
		awaitCRDGone(t, c)

		// Which reason the refusal carries is the subject of the scenario
		// above. What matters here is that the binding is withdrawn rather
		// than left standing against a provider that cannot be read.
		requireBindingWithdrawn(t, awaitBindingRefused(t, c, policy))

		restoreProviderCRD(t, c)
		createReadyProvider(t, c, nemoProvider(namespace, "nemo", defaultConfigID, []string{"base-rails"}))

		// The controller is never restarted, and it re-reads the RESTMapper on
		// every reconcile. Recovery is awaited on the resync cadence rather
		// than on the provider watch, whose informer lost the resource it was
		// watching when the CRD went away.
		requireBindingPublished(t, awaitAcceptedReason(t, c, policy, reasonPolicyAccepted))
	})
}

// awaitBindingRefused waits for the policy to stop being accepted for its
// current generation, without pinning which refusal reason it carries.
func awaitBindingRefused(t *testing.T, c client.Client, policy client.Object) aigatewayv1alpha1.AIGuardrailStatus {
	t.Helper()
	var observed aigatewayv1alpha1.AIGuardrailStatus
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		var current aigatewayv1alpha1.AIGuardrail
		if !assert.NoError(collect, c.Get(t.Context(), client.ObjectKeyFromObject(policy), &current)) {
			return
		}
		accepted := guardrailCondition(current, guardrail.ConditionAccepted)
		if !assert.NotNil(collect, accepted, "no Accepted condition published yet") {
			return
		}
		assert.Equal(collect, current.Generation, accepted.ObservedGeneration,
			"Accepted still reports on an older generation")
		assert.Equal(collect, metav1.ConditionFalse, accepted.Status,
			"the policy is still accepted: %s", accepted.Message)
		observed = current.Status
	}, eventuallyTimeout, pollInterval)
	return observed
}

// labelNamespace sets or, for an empty value, removes a label on a namespace
// and returns the labels as they stand afterwards.
func labelNamespace(t *testing.T, c client.Client, name, key, value string) map[string]string {
	t.Helper()
	var namespace corev1.Namespace
	require.NoError(t, c.Get(t.Context(), client.ObjectKey{Name: name}, &namespace))
	if namespace.Labels == nil {
		namespace.Labels = map[string]string{}
	}
	if value == "" {
		delete(namespace.Labels, key)
	} else {
		namespace.Labels[key] = value
	}
	require.NoError(t, c.Update(t.Context(), &namespace))
	return namespace.Labels
}

// setConfigMaps rewrites which ConfigMaps compose the provider's one declared
// configuration, leaving its name alone so the policy's configId still
// resolves.
func setConfigMaps(t *testing.T, c client.Client, provider *unstructured.Unstructured, configMaps []string) {
	t.Helper()
	mounted := make([]any, 0, len(configMaps))
	for _, configMap := range configMaps {
		mounted = append(mounted, configMap)
	}

	current := guardrail.NewNemoGuardrails()
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(provider), current))
	configs, found, err := unstructured.NestedSlice(current.Object, "spec", "nemoConfigs")
	require.NoError(t, err)
	require.True(t, found)
	require.NotEmpty(t, configs)

	config, ok := configs[0].(map[string]any)
	require.True(t, ok)
	config["configMaps"] = mounted
	require.NoError(t, unstructured.SetNestedSlice(current.Object, configs, "spec", "nemoConfigs"))
	require.NoError(t, c.Update(t.Context(), current))
}

// awaitCRDGone waits for the provider CRD to stop resolving, so the scenario
// asserts the controller's verdict rather than racing the deletion.
func awaitCRDGone(t *testing.T, c client.Client) {
	t.Helper()
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		err := c.List(t.Context(), newProviderList())
		assert.True(collect, apierrors.IsNotFound(err) || apimeta.IsNoMatchError(err),
			"the provider CRD still resolves: %v", err)
	}, eventuallyTimeout, pollInterval)
}

// restoreProviderCRD reinstalls the provider CRD and waits for it to answer
// requests again. The API server admits the CRD object before it establishes
// the resource behind it, so creating a provider straight after reinstalling
// races that.
//
// A scenario that deletes the CRD registers this as its cleanup and also calls
// it inline, so it tolerates the CRD already being back. It carries its own
// context because t.Context is cancelled just before cleanups run.
func restoreProviderCRD(t *testing.T, c client.Client) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*eventuallyTimeout)
	defer cancel()

	err := c.Create(ctx, nemoGuardrailsCRD())
	require.True(t, err == nil || apierrors.IsAlreadyExists(err), "reinstalling the provider CRD: %v", err)

	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		assert.NoError(collect, c.List(ctx, newProviderList()))
	}, eventuallyTimeout, pollInterval)
}

// newProviderList is the list type the provider GVK resolves through. There is
// no generated list helper in pkg/guardrail, which reads providers one at a time.
func newProviderList() *unstructured.UnstructuredList {
	providers := &unstructured.UnstructuredList{}
	providers.SetGroupVersionKind(guardrail.NemoGuardrailsGVK.GroupVersion().WithKind(
		guardrail.NemoGuardrailsGVK.Kind + "List"))
	return providers
}
