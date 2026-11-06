//go:build envtest

package envtest_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-logr/logr/testr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	controllerconfig "sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	aigatewayv1alpha1 "github.com/opendatahub-io/ai-gateway-controller/api/aigateway/v1alpha1"
	inferencev1alpha1 "github.com/opendatahub-io/ai-gateway-controller/api/inference/v1alpha1"
	"github.com/opendatahub-io/ai-gateway-controller/pkg/render"
)

// controllerIdentity is the ServiceAccount bound by the shipped ClusterRoleBinding.
var controllerIdentity = envtest.User{
	Name:   "system:serviceaccount:opendatahub:ai-gateway-controller",
	Groups: []string{"system:serviceaccounts", "system:serviceaccounts:opendatahub", "system:authenticated"},
}

func TestMain(m *testing.M) {
	// SetLogger only takes effect once per process, so its sink must outlive each test.
	ctrl.SetLogger(zap.New())
	os.Exit(m.Run())
}

// startEnvironment owns the API-server and manager lifecycle and applies the
// shipped controller permissions. The suite supplies controller registration.
//
//nolint:ireturn // controller-runtime exposes clients through client.Client.
func startEnvironment(t *testing.T, setupControllers func(ctrl.Manager)) client.Client {
	t.Helper()
	assets := os.Getenv("KUBEBUILDER_ASSETS")
	require.NotEmpty(t, assets, "run make test-envtest or set KUBEBUILDER_ASSETS")

	environment := &envtest.Environment{
		BinaryAssetsDirectory: assets,
		UseExistingCluster:    ptr.To(false),
		// Two bases dirs because generation is split by API group: the aigateway
		// group ships in the self bundle, the inference group does not.
		CRDDirectoryPaths: []string{
			"testdata/crds",
			filepath.Join(repoRoot, "config/self/crd/bases"),
			filepath.Join(repoRoot, "config/crd/bases"),
		},
		ErrorIfCRDPathMissing: true,
		// Installed before the manager starts, because the guardrail
		// reconciler only registers its provider watch when the CRD already
		// resolves.
		CRDs: []*apiextensionsv1.CustomResourceDefinition{nemoGuardrailsCRD()},
	}
	apiServer := environment.ControlPlane.GetAPIServer().Configure()
	apiServer.Set("authorization-mode", "RBAC")
	// OpenShift enables this; without it, blockOwnerDeletion passes without the owner's finalizers permission.
	apiServer.Append("enable-admission-plugins", "OwnerReferencesPermissionEnforcement")
	t.Cleanup(func() { require.NoError(t, environment.Stop()) })
	config, err := environment.Start()
	require.NoError(t, err)
	scheme := runtime.NewScheme()
	schemeBuilder := runtime.NewSchemeBuilder(
		clientgoscheme.AddToScheme,
		inferencev1alpha1.AddToScheme,
		aigatewayv1alpha1.AddToScheme,
		// So a scenario can remove and reinstall the provider CRD and watch
		// the controller recover.
		apiextensionsv1.AddToScheme,
	)
	require.NoError(t, schemeBuilder.AddToScheme(scheme))
	admin, err := client.New(config, client.Options{Scheme: scheme})
	require.NoError(t, err)
	for _, namespace := range []string{controllerNamespace, gatewayNamespace, maasNamespace} {
		createNamespace(t, admin, namespace)
	}
	resources, err := render.Build(filepath.Join(repoRoot, "config/self/default"))
	require.NoError(t, err)
	for _, resource := range resources {
		switch resource.GetKind() {
		case "ServiceAccount", "ClusterRole", "ClusterRoleBinding":
			require.NoError(t, admin.Create(t.Context(), &resource))
		}
	}
	controllerUser, err := environment.AddUser(controllerIdentity, &rest.Config{QPS: -1})
	require.NoError(t, err)
	mgr, err := ctrl.NewManager(controllerUser.Config(), ctrl.Options{
		Scheme:                  scheme,
		Logger:                  testr.New(t),
		Metrics:                 metricsserver.Options{BindAddress: "0"},
		GracefulShutdownTimeout: ptr.To(5 * time.Second),
		// Controller names persist globally; allow go test -count to start a fresh manager.
		Controller: controllerconfig.Controller{SkipNameValidation: ptr.To(true)},
	})
	require.NoError(t, err)
	// Wait for the binding to propagate before starting the controller watches.
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		key := client.ObjectKey{Namespace: controllerNamespace, Name: "ai-gateway-controller"}
		require.NoError(c, mgr.GetAPIReader().Get(t.Context(), key, &corev1.ServiceAccount{}))
	}, eventuallyTimeout, pollInterval)

	setupControllers(mgr)

	// t.Context is canceled before cleanup; wait for the manager before stopping envtest.
	done := make(chan error, 1)
	go func() { done <- mgr.Start(t.Context()) }()
	t.Cleanup(func() {
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(10 * time.Second):
			t.Error("controller manager did not stop")
		}
	})
	cacheCtx, cacheCancel := context.WithTimeout(t.Context(), eventuallyTimeout)
	defer cacheCancel()
	require.True(t, mgr.GetCache().WaitForCacheSync(cacheCtx), "controller cache did not sync")
	return admin
}
