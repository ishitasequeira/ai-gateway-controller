//go:build envtest

package envtest_test

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/opendatahub-io/ai-gateway-controller/pkg/controller"
	"github.com/opendatahub-io/ai-gateway-controller/pkg/guardrail"
	"github.com/opendatahub-io/ai-gateway-controller/pkg/tenant"
)

// guardrailResyncInterval is short enough that scenarios driven by the resync
// loop rather than by a watch still finish inside eventuallyTimeout. Scenario
// 9 is the one that needs it: the suite installs the provider CRD before the
// manager starts, so the provider watch is registered, but that scenario then
// deletes the CRD underneath it. Whether the informer behind the watch resumes
// once the CRD is reinstalled is not something to assert on, so the resync is
// the path recovery is allowed to take.
const guardrailResyncInterval = time.Second

const (
	repoRoot            = "../.."
	controllerNamespace = "opendatahub"
	maasNamespace       = "ai-tenants"
	eventuallyTimeout   = 15 * time.Second
	pollInterval        = 50 * time.Millisecond
)

// The AITenant's gateway differs from the model controller's default flags,
// so published routes must follow status.gatewayRef.
const (
	gatewayName      = "tenant-gateway"
	gatewayNamespace = "tenant-gateways"
)

// startControlPlane registers both controllers as cmd/manager does. Scenarios use
// the returned admin client to act as users and as the external MaaS controller.
//
//nolint:ireturn // controller-runtime exposes clients through client.Client.
func startControlPlane(t *testing.T) client.Client {
	t.Helper()
	return startEnvironment(t, func(mgr ctrl.Manager) {
		tenantController := &tenant.Reconciler{
			Client:               mgr.GetClient(),
			APIReader:            mgr.GetAPIReader(),
			ManifestPath:         filepath.Join(repoRoot, "config/manifests/external-model/overlays/odh"),
			Image:                "quay.io/example/praxis-extproc:test",
			MaaSAPIRouteNameBase: "maas-api-route",
			ResyncInterval:       time.Minute,
			Log:                  mgr.GetLogger().WithName("tenant"),
		}
		require.NoError(t, tenantController.SetupWithManager(mgr))
		modelController := &controller.Reconciler{
			Client:           mgr.GetClient(),
			APIReader:        mgr.GetAPIReader(),
			Scheme:           mgr.GetScheme(),
			GatewayName:      "maas-default-gateway",
			GatewayNamespace: "openshift-ingress",
			Network:          "external-model",
			Log:              mgr.GetLogger().WithName("external-model"),
		}
		require.NoError(t, modelController.SetupWithManager(mgr))
		guardrailController := &guardrail.Reconciler{
			Client:         mgr.GetClient(),
			APIReader:      mgr.GetAPIReader(),
			Scheme:         mgr.GetScheme(),
			ResyncInterval: guardrailResyncInterval,
			Log:            mgr.GetLogger().WithName("aiguardrail"),
			Recorder:       mgr.GetEventRecorder("aiguardrail"),
		}
		require.NoError(t, guardrailController.SetupWithManager(mgr))
	})
}
