//go:build envtest

package envtest_test

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestControlPlane(t *testing.T) {
	c := startControlPlane(t)

	t.Run("guardrail timeouts remain readable by typed clients", func(t *testing.T) {
		testGuardrailTimeoutAdmission(t, c)
	})

	t.Run("shipped permissions cover credential copies and reference grants", func(t *testing.T) {
		testShippedPermissions(t, c)
	})

	t.Run("adding and deleting an external model manages its routing", func(t *testing.T) {
		// Given an existing Praxis tenant with a configured provider.
		const namespace = "model-publication"
		maas := maasFixture{client: c}
		maas.existingPraxisTenant(t, namespace)
		awaitPraxisInstalled(t, c, namespace)
		provider := createProvider(t, c, namespace)

		// When a user adds a model using that provider.
		model := externalModel("model", namespace,
			withModelName("chat"),
			withProviderModel(provider.Name, "provider-model"),
		)
		require.NoError(t, c.Create(t.Context(), model))

		// Then the requested model/provider mapping and its workload are configured.
		routeName := awaitModelPublished(t, c, model, provider)

		// When the user deletes that model.
		require.NoError(t, c.Delete(t.Context(), model))

		// Then its routing and workload are removed and the finalizer releases it.
		awaitModelRemoved(t, c, model, routeName)
	})

	t.Run("an attached guardrail is enforced on ExternalModel routes until it is detached", func(t *testing.T) {
		const namespace = "guardrail-enforcement"
		maas := maasFixture{client: c}
		maas.existingPraxisTenant(t, namespace)
		awaitPraxisInstalled(t, c, namespace)
		provider := createProvider(t, c, namespace)
		model := externalModel("model", namespace,
			withModelName("chat"),
			withProviderModel(provider.Name, "provider-model"),
		)
		require.NoError(t, c.Create(t.Context(), model))
		awaitModelPublished(t, c, model, provider)
		attachment := awaitGuardrailEnforced(t, c, namespace)
		awaitGuardrailDetached(t, c, namespace, attachment)
	})
}
