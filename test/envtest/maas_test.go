//go:build envtest

package envtest_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/opendatahub-io/ai-gateway-controller/pkg/tenant"
)

func TestMaasTenantConfigSchemaPreservesAPITranslation(t *testing.T) {
	admin := startEnvironment(t, func(ctrl.Manager) {})
	createNamespace(t, admin, "translation-schema")
	mtc := tenant.NewMaasTenantConfig()
	mtc.SetName(tenant.MaasTenantConfigInstanceName)
	mtc.SetNamespace("translation-schema")
	mtc.Object["spec"] = map[string]any{"apiTranslation": map[string]any{"enabled": true}}
	require.NoError(t, admin.Create(t.Context(), mtc))
	stored := tenant.NewMaasTenantConfig()
	require.NoError(t, admin.Get(t.Context(), client.ObjectKeyFromObject(mtc), stored))
	enabled, err := tenant.APITranslationEnabled(stored)
	require.NoError(t, err)
	require.True(t, enabled, "API server pruned spec.apiTranslation.enabled")
}

func TestAITenantSchemaPreservesCookieExpire(t *testing.T) {
	admin := startEnvironment(t, func(ctrl.Manager) {})
	createNamespace(t, admin, "cookie-expire-schema")
	ait := tenant.NewAITenant()
	ait.SetName("cookie-expire-tenant")
	ait.SetNamespace(maasNamespace)
	ait.Object["spec"] = map[string]any{
		"oidc": map[string]any{
			"issuerUrl":    "https://keycloak.example.com/realms/maas",
			"clientId":     "maas-consumer-portal",
			"cookieExpire": "8h",
		},
	}
	require.NoError(t, admin.Create(t.Context(), ait))
	stored := tenant.NewAITenant()
	require.NoError(t, admin.Get(t.Context(), client.ObjectKeyFromObject(ait), stored))
	expire, _, err := unstructured.NestedString(stored.Object, "spec", "oidc", "cookieExpire")
	require.NoError(t, err)
	require.Equal(t, "8h", expire, "API server pruned spec.oidc.cookieExpire")

	// The cross-field rule compares effective defaults: a cookie lifetime at
	// or below the 4m default refresh interval must be rejected.
	ait.Object["spec"] = map[string]any{
		"oidc": map[string]any{
			"issuerUrl":    "https://keycloak.example.com/realms/maas",
			"clientId":     "maas-consumer-portal",
			"cookieExpire": "2m",
		},
	}
	err = admin.Update(t.Context(), ait)
	require.ErrorContains(t, err, "cookieExpire must be longer than refreshInterval")
}

// maasFixture supplies the external tenant state consumed by this controller.
// It emits API objects; it does not run or simulate MaaS reconciliation.
type maasFixture struct {
	client client.Client
}

// existingPraxisTenant establishes the currently supported selection precondition.
// Backend opt-in and migration are outside the model-publication scenario.
func (m maasFixture) existingPraxisTenant(t *testing.T, namespace string) {
	t.Helper()
	createNamespace(t, m.client, namespace)
	// gatewayExists looks up the Gateway API object named by status.gatewayRef;
	// the CRD is vendored in testdata/crds and the namespace is created in suite setup.
	gateway := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "gateway.networking.k8s.io/v1",
		"kind":       "Gateway",
		"metadata":   map[string]any{"name": gatewayName, "namespace": gatewayNamespace},
		"spec": map[string]any{
			"gatewayClassName": "istio",
			"listeners": []any{map[string]any{
				"name": "http", "protocol": "HTTP", "port": int64(80),
				"allowedRoutes": map[string]any{"namespaces": map[string]any{"from": "All"}},
			}},
		},
	}}
	require.NoError(t, m.client.Create(t.Context(), gateway))

	ait := tenant.NewAITenant()
	ait.SetName(namespace)
	ait.SetNamespace(maasNamespace)
	// TODO: Remove the legacy AITenant selector when model selection reads MTC.
	// This is an explicit existing-tenant precondition, not an MTC-only opt-in test.
	ait.SetAnnotations(map[string]string{tenant.AnnotationPayloadProcessingType: tenant.PayloadProcessingBackendPraxis})
	ait.Object["spec"] = map[string]any{}
	require.NoError(t, m.client.Create(t.Context(), ait))
	ait.Object["status"] = map[string]any{
		"phase": tenant.AITenantPhaseActive, "tenantNamespace": namespace,
		"gatewayRef": map[string]any{"name": gatewayName, "namespace": gatewayNamespace},
		// DeployReady requires a Ready stamp for the current generation.
		"conditions": []any{map[string]any{
			"type": tenant.AITenantConditionReady, "status": "True",
			"reason": "Reconciled", "message": "AITenant bootstrap resources are reconciled",
			"observedGeneration": ait.GetGeneration(), "lastTransitionTime": time.Now().UTC().Format(time.RFC3339),
		}},
	}
	require.NoError(t, m.client.Status().Update(t.Context(), ait))

	mtc := tenant.NewMaasTenantConfig()
	mtc.SetName(tenant.MaasTenantConfigInstanceName)
	mtc.SetNamespace(namespace)
	mtc.SetLabels(map[string]string{tenant.LabelManagedByAITenant: "true", tenant.LabelTenantName: namespace})
	mtc.SetAnnotations(map[string]string{
		tenant.AnnotationPayloadProcessingType:   tenant.PayloadProcessingBackendPraxis,
		tenant.AnnotationPayloadProcessingStatus: tenant.PayloadProcessingStatusCleanupComplete,
		tenant.AnnotationAITenantName:            namespace,
		tenant.AnnotationAITenantNamespace:       maasNamespace,
	})
	mtc.Object["spec"] = map[string]any{}
	require.NoError(t, m.client.Create(t.Context(), mtc))
}
