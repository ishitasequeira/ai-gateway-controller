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

package main

import (
	"cmp"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"

	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	aigatewayv1alpha1 "github.com/opendatahub-io/ai-gateway-controller/api/aigateway/v1alpha1"
	inferencev1alpha1 "github.com/opendatahub-io/ai-gateway-controller/api/inference/v1alpha1"
	"github.com/opendatahub-io/ai-gateway-controller/pkg/controller"
	"github.com/opendatahub-io/ai-gateway-controller/pkg/tenant"
)

var setupLog = ctrl.Log.WithName("setup")

// buildVersion is replaced by release/container builds with -ldflags
// -X main.buildVersion=...; local builds retain the explicit development
// value rather than claiming an unknown release version.
var buildVersion = "dev"

func main() {
	var (
		metricsAddr          string
		probeAddr            string
		enableLeaderElection bool
		image                string
		manifestPath         string
		maasAPIRouteName     string
		resyncInterval       time.Duration
		deletionTimeout      time.Duration
		externalNamespace    string
		gatewayName          string
		gatewayNamespace     string
		network              string
		localSite            string
		knownClusters        []string
		skipNetworkPolicy    bool
	)

	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "The address the metrics endpoint binds to.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	flag.BoolVar(&enableLeaderElection, "leader-elect", false,
		"Enable leader election for controller manager. Enable this when running multiple replicas.")
	flag.StringVar(&image, "image", resolveExtprocImage(),
		"Container image for the payload-processing and payload-pre-processing Deployments.")
	flag.StringVar(&manifestPath, "manifest-path", "/config/manifests/external-model/overlays/odh",
		"Path to the controller-owned overlay that composes vendored praxis-extproc manifests and ExternalModel patches.")
	flag.StringVar(&maasAPIRouteName, "maas-api-route-name", "maas-api-route",
		"Base name of maas-api's HTTPRoute, used to disable ext_proc on its own routes. "+
			"Exact fidelity depends on the Istio version's route-naming scheme; see DESIGN.md.")
	flag.DurationVar(&resyncInterval, "resync-interval", 5*time.Minute,
		"RequeueAfter used once a tenant's praxis-extproc resources have been applied, so drift "+
			"gets corrected periodically even without a new AITenant watch event.")
	flag.DurationVar(&deletionTimeout, "deletion-timeout", 10*time.Minute,
		"Maximum time to retry praxis-extproc cleanup for a tenant switching away from praxis or "+
			"being deleted before force-removing this controller's cleanup finalizer without "+
			"confirming cleanup succeeded. Zero disables the timeout and retries indefinitely.")
	flag.StringVar(&externalNamespace, "external-model-namespace", "", "Optional namespace scope for ExternalModels; empty watches all namespaces and publishes each overlay in its model namespace.")
	flag.StringVar(&gatewayName, "gateway-name", "maas-default-gateway", "Gateway parent name for ExternalModel HTTPRoutes.")
	flag.StringVar(&gatewayNamespace, "gateway-namespace", "openshift-ingress", "Gateway parent namespace for ExternalModel HTTPRoutes.")
	flag.StringVar(&network, "routing-network", "external-model", "Routing overlay network scope.")
	flag.StringVar(&localSite, "routing-local-site", "local", "Routing overlay local-site scope.")
	flag.Func("known-cluster", "Optional administrative upper bound for a rendered provider cluster; repeat for each cluster.", func(value string) error {
		knownClusters = append(knownClusters, value)
		return nil
	})
	flag.BoolVar(&skipNetworkPolicy, "skip-network-policy", false,
		"Omit controller-managed payload-processing NetworkPolicies when the installation supplies equivalent networking. Default false.")

	opts := zap.Options{}
	if err := applyLogDevelopment(&opts, os.Stderr); err != nil {
		os.Exit(1)
	}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))
	if err := inferencev1alpha1.AddToScheme(clientgoscheme.Scheme); err != nil {
		setupLog.Error(err, "unable to register inference API scheme")
		os.Exit(1)
	}
	if err := aigatewayv1alpha1.AddToScheme(clientgoscheme.Scheme); err != nil {
		setupLog.Error(err, "unable to register AI gateway API scheme")
		os.Exit(1)
	}

	if image == "" {
		setupLog.Error(errors.New("missing required flag"), "--image must be non-empty")
		os.Exit(1)
	}

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme: clientgoscheme.Scheme,
		Metrics: metricsserver.Options{
			BindAddress: metricsAddr,
		},
		HealthProbeBindAddress: probeAddr,
		LeaderElection:         enableLeaderElection,
		LeaderElectionID:       "ai-gateway-controller-leader.opendatahub.io",
	})
	if err != nil {
		setupLog.Error(err, "unable to start manager")
		os.Exit(1)
	}

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up ready check")
		os.Exit(1)
	}

	reconciler := &tenant.Reconciler{
		Client:               mgr.GetClient(),
		APIReader:            mgr.GetAPIReader(),
		ManifestPath:         manifestPath,
		Image:                image,
		SkipNetworkPolicy:    skipNetworkPolicy,
		MaaSAPIRouteNameBase: maasAPIRouteName,
		ResyncInterval:       resyncInterval,
		DeletionTimeout:      deletionTimeout,
		Log:                  ctrl.Log.WithName("tenant"),
	}
	if err := reconciler.SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to set up AITenant reconciler")
		os.Exit(1)
	}

	modelReconciler := &controller.Reconciler{
		Client: mgr.GetClient(), APIReader: mgr.GetAPIReader(), Scheme: mgr.GetScheme(), Namespace: externalNamespace,
		GatewayName: gatewayName, GatewayNamespace: gatewayNamespace, Network: network,
		LocalSite: localSite, KnownClusters: knownClusters, ProducerVersion: buildVersion,
		Log: ctrl.Log.WithName("external-model"),
	}
	if err := modelReconciler.SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to set up ExternalModel and ExternalProvider reconcilers")
		os.Exit(1)
	}

	setupLog.Info("starting ai-gateway-controller", "manifestPath", manifestPath, "image", image)
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "problem running manager")
		os.Exit(1)
	}
}

// resolveExtprocImage returns the praxis-extproc container image.
// In operator-managed deployments the RELATED_IMAGE_ODH_PRAXIS_EXTPROC_IMAGE
// env var carries a digest-pinned reference suitable for disconnected clusters.
// When the variable is unset (local development), the mutable tag fallback is
// returned instead.
func resolveExtprocImage() string {
	return cmp.Or(
		os.Getenv("RELATED_IMAGE_ODH_PRAXIS_EXTPROC_IMAGE"),
		"quay.io/opendatahub/odh-praxis-extproc:odh-stable",
	)
}

// applyLogDevelopment reads LOG_DEVELOPMENT into opts. Invalid values are
// written to errOut: ctrl.Log is a NullLogSink until SetLogger runs.
func applyLogDevelopment(opts *zap.Options, errOut io.Writer) error {
	v, ok := os.LookupEnv("LOG_DEVELOPMENT")
	if !ok {
		return nil
	}
	dev, err := strconv.ParseBool(v)
	if err != nil {
		fmt.Fprintf(errOut, "invalid LOG_DEVELOPMENT value %q: %v\n", v, err)
		return err
	}
	opts.Development = dev
	return nil
}
