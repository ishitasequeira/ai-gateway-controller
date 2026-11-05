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

package consumerportal

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	netwv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"github.com/opendatahub-io/ai-gateway-controller/pkg/constants"
	"github.com/opendatahub-io/ai-gateway-controller/pkg/tenant"
)

const (
	testTenantNamespace  = "models-as-a-service"
	testInfraNamespace   = "redhat-ai-gateway-infra"
	testAITenantNS       = "ai-tenants"
	testGatewayName      = "maas-default-gateway"
	testGatewayNamespace = "openshift-ingress"
	testHostname         = "maas.apps.example.com"
	testIssuer           = "https://keycloak.apps.example.com/realms/tenant-a"
	testManifestPath     = "../../config/manifests/consumer-portal/base"
)

func testScheme() *runtime.Scheme {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = netwv1.AddToScheme(scheme)
	for _, gvk := range []struct{ group, version, kind string }{
		{tenant.MaasTenantConfigGVK.Group, tenant.MaasTenantConfigGVK.Version, tenant.MaasTenantConfigGVK.Kind},
		{tenant.AITenantGVK.Group, tenant.AITenantGVK.Version, tenant.AITenantGVK.Kind},
		{gvkGateway.Group, gvkGateway.Version, gvkGateway.Kind},
		{gvkHTTPRoute.Group, gvkHTTPRoute.Version, gvkHTTPRoute.Kind},
		{gvkReferenceGrant.Group, gvkReferenceGrant.Version, gvkReferenceGrant.Kind},
		{gvkAuthPolicy.Group, gvkAuthPolicy.Version, gvkAuthPolicy.Kind},
		{gvkDestinationRule.Group, gvkDestinationRule.Version, gvkDestinationRule.Kind},
	} {
		gv := tenant.AITenantGVK.GroupVersion()
		gv.Group, gv.Version = gvk.group, gvk.version
		scheme.AddKnownTypeWithName(gv.WithKind(gvk.kind), &unstructured.Unstructured{})
		scheme.AddKnownTypeWithName(gv.WithKind(gvk.kind+"List"), &unstructured.UnstructuredList{})
	}
	return scheme
}

func newMTC(tenantName, infraNamespace string) *unstructured.Unstructured {
	u := tenant.NewMaasTenantConfig()
	u.SetName(tenant.MaasTenantConfigInstanceName)
	u.SetNamespace(testTenantNamespace)
	u.SetLabels(map[string]string{
		tenant.LabelManagedByAITenant: "true",
		tenant.LabelTenantName:        tenantName,
	})
	u.SetAnnotations(map[string]string{
		tenant.AnnotationAITenantName:      tenantName,
		tenant.AnnotationAITenantNamespace: testAITenantNS,
	})
	if infraNamespace != "" {
		u.Object["status"] = map[string]any{"infraNamespace": infraNamespace}
	}
	return u
}

type aitenantOpts struct {
	notReady       bool
	issuer         string
	portalDisabled bool
}

func newAITenant(opts aitenantOpts) *unstructured.Unstructured {
	u := tenant.NewAITenant()
	u.SetName(tenant.DefaultAITenantName)
	u.SetNamespace(testAITenantNS)
	phase, ready := tenant.AITenantPhaseActive, "True"
	if opts.notReady {
		phase, ready = tenant.AITenantPhaseFailed, "False"
	}
	spec := map[string]any{}
	if opts.issuer != "" {
		spec["oidc"] = map[string]any{"issuerUrl": opts.issuer, "clientId": "test-client"}
	}
	if opts.portalDisabled {
		spec["consumerPortal"] = map[string]any{"enabled": false}
	}
	u.Object["spec"] = spec
	u.Object["status"] = map[string]any{
		"phase":           phase,
		"tenantNamespace": testTenantNamespace,
		"gatewayRef":      map[string]any{"name": testGatewayName, "namespace": testGatewayNamespace},
		"conditions": []any{map[string]any{
			"type":               tenant.AITenantConditionReady,
			"status":             ready,
			"observedGeneration": u.GetGeneration(),
		}},
	}
	return u
}

func newGateway(listeners ...map[string]any) *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(gvkGateway)
	u.SetName(testGatewayName)
	u.SetNamespace(testGatewayNamespace)
	items := make([]any, 0, len(listeners))
	for _, l := range listeners {
		items = append(items, l)
	}
	u.Object["spec"] = map[string]any{"listeners": items}
	return u
}

func httpsListener(hostname string) map[string]any {
	return map[string]any{"name": "https", "protocol": "HTTPS", "port": int64(443), "hostname": hostname}
}

func adminCredentialsSecret() *corev1.Secret {
	return &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: CredentialsSecretName, Namespace: testInfraNamespace}}
}

func dashboardService() *corev1.Service {
	return &corev1.Service{ObjectMeta: metav1.ObjectMeta{
		Name: PortalServiceName, Namespace: DefaultDashboardNamespace,
	}}
}

func newConsumerPortalObject(gvk schema.GroupVersionKind, name, namespace string, managed bool) *unstructured.Unstructured {
	object := &unstructured.Unstructured{}
	object.SetGroupVersionKind(gvk)
	object.SetName(name)
	object.SetNamespace(namespace)
	labels := map[string]string{"app.kubernetes.io/part-of": "maas-consumer-portal"}
	if managed {
		labels[tenant.LabelManagedBy] = tenant.ManagedByAIGatewayController
		labels[LabelPortalInstance] = testInfraNamespace
	}
	object.SetLabels(labels)
	return object
}

func managedProxyDeployment() *appsv1.Deployment {
	return &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{
		Name:      AuthProxyName,
		Namespace: testInfraNamespace,
		Labels:    map[string]string{tenant.LabelManagedBy: tenant.ManagedByAIGatewayController},
	}}
}

// recorder records which kinds were applied, read, and deleted while letting
// the fake client persist every operation.
type recorder struct {
	mu      sync.Mutex
	objects map[string]*unstructured.Unstructured
	applied []string
	gets    []string
	deleted []string
}

func setRouteReady(route *unstructured.Unstructured) {
	setRouteConditions(route, true, true)
}

func setRouteConditions(route *unstructured.Unstructured, accepted, resolved bool) {
	status := func(value bool) string {
		if value {
			return "True"
		}
		return "False"
	}
	route.Object["status"] = map[string]any{"parents": []any{map[string]any{
		"conditions": []any{
			map[string]any{"type": "Accepted", "status": status(accepted)},
			map[string]any{"type": "ResolvedRefs", "status": status(resolved)},
		},
	}}}
}

func (r *recorder) funcs() interceptor.Funcs {
	return interceptor.Funcs{
		Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			if patch.Type() == types.ApplyPatchType {
				r.mu.Lock()
				r.applied = append(r.applied, obj.GetObjectKind().GroupVersionKind().Kind+"/"+obj.GetNamespace()+"/"+obj.GetName())
				if u, ok := obj.(*unstructured.Unstructured); ok {
					if r.objects == nil {
						r.objects = map[string]*unstructured.Unstructured{}
					}
					r.objects[u.GetKind()] = u.DeepCopy()
				}
				r.mu.Unlock()
				existing, ok := obj.DeepCopyObject().(client.Object)
				if !ok {
					return fmt.Errorf("applied object %T is not a client object", obj)
				}
				err := c.Get(ctx, client.ObjectKeyFromObject(obj), existing)
				if apierrors.IsNotFound(err) {
					if route, ok := obj.(*unstructured.Unstructured); ok && route.GetKind() == "HTTPRoute" {
						setRouteReady(route)
					}
					return c.Create(ctx, obj)
				}
				if err != nil {
					return err
				}
				return nil
			}
			return c.Patch(ctx, obj, patch, opts...)
		},
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			r.mu.Lock()
			r.gets = append(r.gets, obj.GetObjectKind().GroupVersionKind().Kind)
			r.mu.Unlock()
			return c.Get(ctx, key, obj, opts...)
		},
		Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			r.mu.Lock()
			r.deleted = append(r.deleted, obj.GetObjectKind().GroupVersionKind().Kind)
			r.mu.Unlock()
			return c.Delete(ctx, obj, opts...)
		},
	}
}

func newReconciler(t *testing.T, objects ...client.Object) (*Reconciler, *recorder) {
	t.Helper()
	rec := &recorder{}
	c := fake.NewClientBuilder().WithScheme(testScheme()).WithObjects(objects...).WithInterceptorFuncs(rec.funcs()).Build()
	return &Reconciler{
		Client:         c,
		APIReader:      c,
		Log:            logr.Discard(),
		ManifestPath:   testManifestPath,
		Image:          "quay.io/example/kube-auth-proxy:test",
		ResyncInterval: 5 * time.Minute,
	}, rec
}

func reconcileMTC(t *testing.T, r *Reconciler) ctrl.Result {
	t.Helper()
	res, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{
		Namespace: testTenantNamespace, Name: tenant.MaasTenantConfigInstanceName,
	}})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	return res
}

func getMTC(t *testing.T, c client.Client) *unstructured.Unstructured {
	t.Helper()
	mtc := tenant.NewMaasTenantConfig()
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: testTenantNamespace, Name: tenant.MaasTenantConfigInstanceName}, mtc); err != nil {
		t.Fatalf("get MaasTenantConfig: %v", err)
	}
	return mtc
}

func TestReconcileAppliesAuthProxyForDefaultTenant(t *testing.T) {
	r, rec := newReconciler(t,
		newMTC(tenant.DefaultAITenantName, testInfraNamespace),
		newAITenant(aitenantOpts{issuer: testIssuer}),
		newGateway(httpsListener(testHostname)),
		adminCredentialsSecret(),
		dashboardService(),
	)

	res := reconcileMTC(t, r)

	if res.RequeueAfter != r.ResyncInterval {
		t.Errorf("RequeueAfter = %v, want resync interval %v", res.RequeueAfter, r.ResyncInterval)
	}
	sort.Strings(rec.applied)
	want := []string{
		"AuthPolicy/" + testInfraNamespace + "/maas-consumer-portal-oauth2-anonymous",
		"AuthPolicy/" + testInfraNamespace + "/maas-consumer-portal-session",
		"Deployment/" + testInfraNamespace + "/" + AuthProxyName,
		"DestinationRule/" + testGatewayNamespace + "/maas-consumer-portal-auth-proxy-backend-tls",
		"DestinationRule/" + testGatewayNamespace + "/maas-consumer-portal-backend-tls",
		"HTTPRoute/" + testInfraNamespace + "/" + PortalRouteName,
		"HTTPRoute/" + testInfraNamespace + "/" + OAuthRouteName,
		"NetworkPolicy/" + testInfraNamespace + "/" + AuthProxyName,
		"ReferenceGrant/" + DefaultDashboardNamespace + "/maas-consumer-portal",
		"Service/" + testInfraNamespace + "/" + AuthProxyName,
		"ServiceAccount/" + testInfraNamespace + "/" + AuthProxyName,
	}
	if len(rec.applied) != len(want) {
		t.Fatalf("applied = %v, want %v", rec.applied, want)
	}
	for i := range want {
		if rec.applied[i] != want[i] {
			t.Errorf("applied[%d] = %q, want %q", i, rec.applied[i], want[i])
		}
	}
	if !controllerutil.ContainsFinalizer(getMTC(t, r.Client), CleanupFinalizer) {
		t.Error("cleanup finalizer was not added to the MaasTenantConfig")
	}
	for _, kind := range rec.gets {
		if kind == "GatewayConfig" {
			t.Error("reconciler read a GatewayConfig; the auth proxy must not depend on it")
		}
	}
}

func TestReconcileWaitsForCredentialsSecret(t *testing.T) {
	r, rec := newReconciler(t,
		newMTC(tenant.DefaultAITenantName, testInfraNamespace),
		newAITenant(aitenantOpts{issuer: testIssuer}),
		newGateway(httpsListener(testHostname)),
		dashboardService(),
	)

	res := reconcileMTC(t, r)

	if res.RequeueAfter != constants.NotReadyRequeueInterval {
		t.Errorf("RequeueAfter = %v, want %v", res.RequeueAfter, constants.NotReadyRequeueInterval)
	}
	if len(rec.applied) != 0 {
		t.Errorf("applied %v without a credentials Secret", rec.applied)
	}
	if !controllerutil.ContainsFinalizer(getMTC(t, r.Client), CleanupFinalizer) {
		t.Error("cleanup finalizer was not added while credentials were pending")
	}
}

func TestRefreshIntervalFallsBackAtCookieLifetime(t *testing.T) {
	aitenant := newAITenant(aitenantOpts{issuer: testIssuer})
	for _, value := range []string{"6h", "7h", "590h"} {
		_ = unstructured.SetNestedField(aitenant.Object, value, "spec", "oidc", "refreshInterval")
		if got := RefreshInterval(aitenant); got != DefaultRefreshInterval {
			t.Errorf("RefreshInterval(%q) = %v, want %v", value, got, DefaultRefreshInterval)
		}
	}
	// A longer cookieExpire admits refresh intervals above the default lifetime.
	_ = unstructured.SetNestedField(aitenant.Object, "2h", "spec", "oidc", "refreshInterval")
	_ = unstructured.SetNestedField(aitenant.Object, "4h", "spec", "oidc", "cookieExpire")
	if got := RefreshInterval(aitenant); got != 2*time.Hour {
		t.Errorf("RefreshInterval with cookieExpire=4h = %v, want 2h", got)
	}
	// A refresh interval at or above the configured cookieExpire falls back.
	_ = unstructured.SetNestedField(aitenant.Object, "4h", "spec", "oidc", "refreshInterval")
	if got := RefreshInterval(aitenant); got != DefaultRefreshInterval {
		t.Errorf("RefreshInterval at cookieExpire = %v, want %v", got, DefaultRefreshInterval)
	}
}

func TestRefreshIntervalClampedBelowShortCookieExpire(t *testing.T) {
	aitenant := newAITenant(aitenantOpts{issuer: testIssuer})
	// A cookieExpire at or below the default refresh interval would stop the
	// auth proxy from starting; the fallback must clamp below it.
	_ = unstructured.SetNestedField(aitenant.Object, "2m", "spec", "oidc", "cookieExpire")
	if got := RefreshInterval(aitenant); got >= 2*time.Minute {
		t.Errorf("RefreshInterval with cookieExpire=2m = %v, want < 2m", got)
	}
	_ = unstructured.SetNestedField(aitenant.Object, "4m", "spec", "oidc", "cookieExpire")
	if got := RefreshInterval(aitenant); got >= 4*time.Minute {
		t.Errorf("RefreshInterval with cookieExpire=4m = %v, want < 4m", got)
	}
	// An explicit refresh interval below a short cookieExpire is honored.
	_ = unstructured.SetNestedField(aitenant.Object, "1m", "spec", "oidc", "refreshInterval")
	if got := RefreshInterval(aitenant); got != time.Minute {
		t.Errorf("RefreshInterval with cookieExpire=4m, refreshInterval=1m = %v, want 1m", got)
	}
}

func TestCookieExpire(t *testing.T) {
	cases := map[string]time.Duration{
		"":               DefaultCookieExpire,
		"not-a-duration": DefaultCookieExpire,
		"-1h":            DefaultCookieExpire,
		"0s":             DefaultCookieExpire,
		"30m":            30 * time.Minute,
		"168h":           168 * time.Hour,
	}
	for value, want := range cases {
		aitenant := newAITenant(aitenantOpts{issuer: testIssuer})
		if value != "" {
			_ = unstructured.SetNestedField(aitenant.Object, value, "spec", "oidc", "cookieExpire")
		}
		if got := CookieExpire(aitenant); got != want {
			t.Errorf("CookieExpire(%q) = %v, want %v", value, got, want)
		}
	}
}

func TestReconcileWaitsForDashboardService(t *testing.T) {
	r, rec := newReconciler(t,
		newMTC(tenant.DefaultAITenantName, testInfraNamespace),
		newAITenant(aitenantOpts{issuer: testIssuer}),
		newGateway(httpsListener(testHostname)),
		adminCredentialsSecret(),
	)

	res := reconcileMTC(t, r)

	if res.RequeueAfter != constants.NotReadyRequeueInterval {
		t.Errorf("RequeueAfter = %v, want %v", res.RequeueAfter, constants.NotReadyRequeueInterval)
	}
	if len(rec.applied) != 0 {
		t.Errorf("applied %v without the dashboard Service", rec.applied)
	}
	if controllerutil.ContainsFinalizer(getMTC(t, r.Client), CleanupFinalizer) {
		t.Error("finalizer added before the dashboard Service existed")
	}
}

// dsci returns a DSCInitialization singleton fixture carrying the given
// applicationsNamespace.
func dsci(applicationsNamespace string) *unstructured.Unstructured {
	object := &unstructured.Unstructured{}
	object.SetGroupVersionKind(gvkDSCI)
	object.SetName("default-dsci")
	object.SetNamespace("opendatahub")
	_ = unstructured.SetNestedField(object.Object, applicationsNamespace, "spec", "applicationsNamespace")
	return object
}

func TestDashboardNamespaceFollowsDSCI(t *testing.T) {
	// A DSCI with a non-default applicationsNamespace relocates the Service
	// gate: the dashboard Service in DefaultDashboardNamespace no longer
	// satisfies it.
	r, rec := newReconciler(t,
		newMTC(tenant.DefaultAITenantName, testInfraNamespace),
		newAITenant(aitenantOpts{issuer: testIssuer}),
		newGateway(httpsListener(testHostname)),
		adminCredentialsSecret(),
		dashboardService(),
		dsci("odh-applications"),
	)
	res := reconcileMTC(t, r)
	if res.RequeueAfter != constants.NotReadyRequeueInterval {
		t.Errorf("RequeueAfter = %v, want %v (Service only exists in the default namespace)", res.RequeueAfter, constants.NotReadyRequeueInterval)
	}
	if len(rec.applied) != 0 {
		t.Errorf("applied %v without the dashboard Service in the DSCI namespace", rec.applied)
	}

	// With the Service in the DSCI namespace the gate passes.
	dsciService := dashboardService()
	dsciService.Namespace = "odh-applications"
	r, rec = newReconciler(t,
		newMTC(tenant.DefaultAITenantName, testInfraNamespace),
		newAITenant(aitenantOpts{issuer: testIssuer}),
		newGateway(httpsListener(testHostname)),
		adminCredentialsSecret(),
		dsciService,
		dsci("odh-applications"),
	)
	res = reconcileMTC(t, r)
	if res.RequeueAfter == constants.NotReadyRequeueInterval && len(rec.applied) == 0 {
		t.Error("still requeuing with the dashboard Service present in the DSCI namespace")
	}
}

func TestDashboardNamespaceFlagOverridesDSCI(t *testing.T) {
	r, _ := newReconciler(t,
		newMTC(tenant.DefaultAITenantName, testInfraNamespace),
		newAITenant(aitenantOpts{issuer: testIssuer}),
		newGateway(httpsListener(testHostname)),
		adminCredentialsSecret(),
		dashboardService(),
		dsci("odh-applications"),
	)
	r.DashboardNamespace = DefaultDashboardNamespace
	ns, err := r.dashboardNamespace(context.Background())
	if err != nil {
		t.Fatalf("dashboardNamespace: %v", err)
	}
	if ns != DefaultDashboardNamespace {
		t.Errorf("dashboardNamespace = %q with the flag set, want %q", ns, DefaultDashboardNamespace)
	}
}

func TestReconcileRequeuesUntilPrerequisitesExist(t *testing.T) {
	cases := map[string][]client.Object{
		"AITenant not ready": {
			newMTC(tenant.DefaultAITenantName, testInfraNamespace),
			newAITenant(aitenantOpts{issuer: testIssuer, notReady: true}),
		},
		"infra namespace not published": {
			newMTC(tenant.DefaultAITenantName, ""),
			newAITenant(aitenantOpts{issuer: testIssuer}),
		},
		"Gateway without concrete HTTPS hostname": {
			newMTC(tenant.DefaultAITenantName, testInfraNamespace),
			newAITenant(aitenantOpts{issuer: testIssuer}),
			newGateway(httpsListener("*.apps.example.com")),
			adminCredentialsSecret(),
			dashboardService(),
		},
	}
	for name, objects := range cases {
		t.Run(name, func(t *testing.T) {
			r, rec := newReconciler(t, objects...)
			if res := reconcileMTC(t, r); res.RequeueAfter != constants.NotReadyRequeueInterval {
				t.Errorf("RequeueAfter = %v, want %v", res.RequeueAfter, constants.NotReadyRequeueInterval)
			}
			if len(rec.applied) != 0 {
				t.Errorf("applied %v before prerequisites exist", rec.applied)
			}
		})
	}
}

func TestReconcileRemovesAuthProxyWhenNotWanted(t *testing.T) {
	withFinalizer := func(mtc *unstructured.Unstructured) *unstructured.Unstructured {
		controllerutil.AddFinalizer(mtc, CleanupFinalizer)
		return mtc
	}
	cases := map[string]struct {
		mtc      *unstructured.Unstructured
		aitenant *unstructured.Unstructured
	}{
		"no OIDC issuer (OpenShift mode)": {
			mtc:      withFinalizer(newMTC(tenant.DefaultAITenantName, testInfraNamespace)),
			aitenant: newAITenant(aitenantOpts{}),
		},
		"consumer portal disabled": {
			mtc:      withFinalizer(newMTC(tenant.DefaultAITenantName, testInfraNamespace)),
			aitenant: newAITenant(aitenantOpts{issuer: testIssuer, portalDisabled: true}),
		},
		"non-default tenant": {
			mtc:      withFinalizer(newMTC("team-a", testInfraNamespace)),
			aitenant: newAITenant(aitenantOpts{issuer: testIssuer}),
		},
		// Once deployed, the dashboard Service's removal is the off-signal:
		// the portal is torn down instead of left serving errors.
		"dashboard Service deleted after deploy": {
			mtc:      withFinalizer(newMTC(tenant.DefaultAITenantName, testInfraNamespace)),
			aitenant: newAITenant(aitenantOpts{issuer: testIssuer}),
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			r, rec := newReconciler(t, tc.mtc, tc.aitenant, newGateway(httpsListener(testHostname)),
				adminCredentialsSecret(), managedProxyDeployment())

			reconcileMTC(t, r)

			if len(rec.applied) != 0 {
				t.Errorf("applied %v when the auth proxy is not wanted", rec.applied)
			}
			err := r.Client.Get(context.Background(), types.NamespacedName{Namespace: testInfraNamespace, Name: AuthProxyName}, &appsv1.Deployment{})
			if !apierrors.IsNotFound(err) {
				t.Errorf("auth proxy Deployment still exists (err=%v)", err)
			}
			if controllerutil.ContainsFinalizer(getMTC(t, r.Client), CleanupFinalizer) {
				t.Error("cleanup finalizer was not removed")
			}
		})
	}
}

func TestReconcileDeletionCleansUpAndReleasesFinalizer(t *testing.T) {
	mtc := newMTC(tenant.DefaultAITenantName, testInfraNamespace)
	controllerutil.AddFinalizer(mtc, CleanupFinalizer)
	now := metav1.Now()
	mtc.SetDeletionTimestamp(&now)
	r, _ := newReconciler(t, mtc, managedProxyDeployment())

	reconcileMTC(t, r)

	err := r.Client.Get(context.Background(), types.NamespacedName{Namespace: testInfraNamespace, Name: AuthProxyName}, &appsv1.Deployment{})
	if !apierrors.IsNotFound(err) {
		t.Errorf("auth proxy Deployment still exists after MaasTenantConfig deletion (err=%v)", err)
	}
	// Releasing the last finalizer lets the fake client complete the delete.
	err = r.Client.Get(context.Background(), types.NamespacedName{Namespace: testTenantNamespace, Name: tenant.MaasTenantConfigInstanceName}, tenant.NewMaasTenantConfig())
	if !apierrors.IsNotFound(err) {
		t.Errorf("MaasTenantConfig still exists after finalizer release (err=%v)", err)
	}
}

func TestCleanupLeavesUnmanagedObjects(t *testing.T) {
	foreign := managedProxyDeployment()
	foreign.Labels = map[string]string{tenant.LabelManagedBy: "someone-else"}
	r, _ := newReconciler(t, foreign)

	if err := r.cleanup(context.Background(), testInfraNamespace, testInfraNamespace); err != nil {
		t.Fatalf("cleanup: %v", err)
	}
	if err := r.Client.Get(context.Background(), types.NamespacedName{Namespace: testInfraNamespace, Name: AuthProxyName}, &appsv1.Deployment{}); err != nil {
		t.Errorf("unmanaged Deployment was deleted: %v", err)
	}
}

func TestRoutesReadyRequiresAcceptedAndResolvedRoutes(t *testing.T) {
	newRoute := func(name string, accepted, resolved bool) *unstructured.Unstructured {
		route := newConsumerPortalObject(gvkHTTPRoute, name, testInfraNamespace, true)
		setRouteConditions(route, accepted, resolved)
		return route
	}
	cases := map[string]struct {
		accepted  bool
		resolved  bool
		wantReady bool
	}{
		"ready":            {accepted: true, resolved: true, wantReady: true},
		"not accepted":     {accepted: false, resolved: true},
		"unresolved pipes": {accepted: true, resolved: false},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			r, _ := newReconciler(t,
				newRoute(PortalRouteName, tc.accepted, tc.resolved),
				newRoute(OAuthRouteName, true, true),
			)
			ready, err := r.routesReady(context.Background(), testInfraNamespace)
			if err != nil {
				t.Fatalf("routesReady: %v", err)
			}
			if ready != tc.wantReady {
				t.Errorf("routesReady = %v, want %v", ready, tc.wantReady)
			}
		})
	}
}

func TestRoutesReadyTreatsMissingRouteAsNotReady(t *testing.T) {
	// No HTTPRoutes exist: right after the first apply the route can be
	// absent from the API server (or a stale cache), which must requeue on
	// the not-ready interval, not fail the reconcile with an error.
	r, _ := newReconciler(t,
		newMTC(tenant.DefaultAITenantName, testInfraNamespace),
	)
	ready, err := r.routesReady(context.Background(), testInfraNamespace)
	if err != nil {
		t.Fatalf("routesReady: %v", err)
	}
	if ready {
		t.Error("routesReady = true with no routes, want false")
	}
}

func TestRoutesReadyRejectsStaleGenerationConditions(t *testing.T) {
	// After a spec change, the previous True conditions remain until the
	// Gateway controller processes the new generation; they must not satisfy
	// the readiness check.
	route := newConsumerPortalObject(gvkHTTPRoute, PortalRouteName, testInfraNamespace, true)
	route.SetGeneration(2)
	setRouteConditions(route, true, true)
	parents, found, err := unstructured.NestedSlice(route.Object, "status", "parents")
	if err != nil || !found || len(parents) == 0 {
		t.Fatalf("route has no status parents (found=%v err=%v)", found, err)
	}
	parent, ok := parents[0].(map[string]any)
	if !ok {
		t.Fatal("status parent is not a map")
	}
	conditions, _, _ := unstructured.NestedSlice(parent, "conditions")
	if len(conditions) == 0 {
		t.Fatal("status parent has no conditions")
	}
	condition, ok := conditions[0].(map[string]any)
	if !ok {
		t.Fatal("condition is not a map")
	}
	condition["observedGeneration"] = int64(1)
	r, _ := newReconciler(t, route)
	ready, err := r.routesReady(context.Background(), testInfraNamespace)
	if err != nil {
		t.Fatalf("routesReady: %v", err)
	}
	if ready {
		t.Error("routesReady = true with conditions from an older generation, want false")
	}
}

func TestCleanupKeepsOtherPortalInstances(t *testing.T) {
	// The portal labels every object with its MaasTenantConfig namespace;
	// disabling one instance must not delete another instance's objects,
	// which can live in the same or cross namespaces.
	ours := newConsumerPortalObject(gvkReferenceGrant, PortalRouteName, DefaultDashboardNamespace, true)
	theirs := newConsumerPortalObject(gvkReferenceGrant, "other-instance-grant", DefaultDashboardNamespace, true)
	theirs.SetLabels(map[string]string{
		"app.kubernetes.io/part-of": "maas-consumer-portal",
		tenant.LabelManagedBy:       tenant.ManagedByAIGatewayController,
		LabelPortalInstance:         "other-tenant",
	})
	r, _ := newReconciler(t, ours, theirs)
	if err := r.cleanup(context.Background(), testInfraNamespace, testInfraNamespace); err != nil {
		t.Fatalf("cleanup: %v", err)
	}
	current := &unstructured.Unstructured{}
	current.SetGroupVersionKind(gvkReferenceGrant)
	if err := r.Client.Get(context.Background(), client.ObjectKeyFromObject(ours), current); !apierrors.IsNotFound(err) {
		t.Errorf("own ReferenceGrant still exists (err=%v)", err)
	}
	if err := r.Client.Get(context.Background(), client.ObjectKeyFromObject(theirs), current); err != nil {
		t.Errorf("other instance's ReferenceGrant deleted: %v", err)
	}
}

func TestPruneStaleGrantsRemovesOnlyOldNamespace(t *testing.T) {
	current := newConsumerPortalObject(gvkReferenceGrant, PortalRouteName, "new-dashboard-ns", true)
	stale := newConsumerPortalObject(gvkReferenceGrant, PortalRouteName, "old-dashboard-ns", true)
	theirs := newConsumerPortalObject(gvkReferenceGrant, "other-instance-grant", "old-dashboard-ns", true)
	theirs.SetLabels(map[string]string{
		"app.kubernetes.io/part-of": "maas-consumer-portal",
		tenant.LabelManagedBy:       tenant.ManagedByAIGatewayController,
		LabelPortalInstance:         "other-tenant",
	})
	r, _ := newReconciler(t, current, stale, theirs)
	if err := r.pruneStaleGrants(context.Background(), testInfraNamespace, "new-dashboard-ns"); err != nil {
		t.Fatalf("pruneStaleGrants: %v", err)
	}
	check := func(obj *unstructured.Unstructured, wantGone bool) {
		got := &unstructured.Unstructured{}
		got.SetGroupVersionKind(gvkReferenceGrant)
		err := r.Client.Get(context.Background(), client.ObjectKeyFromObject(obj), got)
		if wantGone && !apierrors.IsNotFound(err) {
			t.Errorf("ReferenceGrant in %s still exists (err=%v)", obj.GetNamespace(), err)
		}
		if !wantGone && err != nil {
			t.Errorf("ReferenceGrant in %s deleted: %v", obj.GetNamespace(), err)
		}
	}
	check(stale, true)
	check(current, false)
	check(theirs, false)
}

func TestCleanupDeletesManagedConsumerPortalObjects(t *testing.T) {
	objects := []client.Object{
		newConsumerPortalObject(gvkHTTPRoute, PortalRouteName, testInfraNamespace, true),
		newConsumerPortalObject(gvkHTTPRoute, OAuthRouteName, testInfraNamespace, true),
		newConsumerPortalObject(gvkReferenceGrant, PortalRouteName, DefaultDashboardNamespace, true),
		newConsumerPortalObject(gvkAuthPolicy, PortalRouteName, testInfraNamespace, true),
		newConsumerPortalObject(gvkAuthPolicy, OAuthRouteName, testInfraNamespace, true),
		newConsumerPortalObject(gvkDestinationRule, PortalRouteName, testGatewayNamespace, true),
		newConsumerPortalObject(gvkDestinationRule, AuthProxyName, testGatewayNamespace, true),
		newConsumerPortalObject(gvkHTTPRoute, "unmanaged", testInfraNamespace, false),
	}
	r, _ := newReconciler(t, objects...)

	if err := r.cleanup(context.Background(), testInfraNamespace, testInfraNamespace); err != nil {
		t.Fatalf("cleanup: %v", err)
	}
	for _, object := range objects[:len(objects)-1] {
		current := &unstructured.Unstructured{}
		source, ok := object.(*unstructured.Unstructured)
		if !ok {
			t.Fatalf("cleanup object is %T, want unstructured", object)
		}
		current.SetGroupVersionKind(source.GroupVersionKind())
		err := r.Client.Get(context.Background(), client.ObjectKeyFromObject(object), current)
		if !apierrors.IsNotFound(err) {
			t.Errorf("managed %s %s still exists (err=%v)", current.GetKind(), client.ObjectKeyFromObject(object), err)
		}
	}
	current := &unstructured.Unstructured{}
	current.SetGroupVersionKind(gvkHTTPRoute)
	if err := r.Client.Get(context.Background(), types.NamespacedName{
		Namespace: testInfraNamespace, Name: "unmanaged",
	}, current); err != nil {
		t.Errorf("unmanaged HTTPRoute was deleted: %v", err)
	}
}

func TestMaasTenantConfigForAITenantOnlyMapsDefaultTenant(t *testing.T) {
	r := &Reconciler{}
	if reqs := r.maasTenantConfigForAITenant(context.Background(), newAITenant(aitenantOpts{})); len(reqs) != 1 ||
		reqs[0].Namespace != testTenantNamespace || reqs[0].Name != tenant.MaasTenantConfigInstanceName {
		t.Errorf("default AITenant mapped to %v", reqs)
	}
	other := newAITenant(aitenantOpts{})
	other.SetName("team-a")
	if reqs := r.maasTenantConfigForAITenant(context.Background(), other); len(reqs) != 0 {
		t.Errorf("non-default AITenant mapped to %v", reqs)
	}
}

func TestProviderCAKeysSkipsEmptyBundleKeys(t *testing.T) {
	bundle := trustedCABundle("one")
	r, _ := newReconciler(t, bundle)
	keys, hash, err := r.providerCAKeys(context.Background(), testInfraNamespace)
	if err != nil {
		t.Fatalf("providerCAKeys: %v", err)
	}
	if len(keys) != 1 || keys[0] != "ca-bundle.crt" {
		t.Errorf("keys = %v, want [ca-bundle.crt]", keys)
	}
	if hash == "" {
		t.Error("providerCAKeys returned no hash for a non-empty CA bundle")
	}

	r, _ = newReconciler(t)
	if keys, hash, err := r.providerCAKeys(context.Background(), testInfraNamespace); err != nil || len(keys) != 0 || hash != "" {
		t.Errorf("missing bundle: keys = %v, hash = %q, err = %v", keys, hash, err)
	}
}

func trustedCABundle(content string) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: TrustedCABundleConfigMap, Namespace: testInfraNamespace},
		Data: map[string]string{
			"ca-bundle.crt":     "-----BEGIN CERTIFICATE-----\n" + content + "\n-----END CERTIFICATE-----\n",
			"odh-ca-bundle.crt": "\n",
		},
	}
}

func TestReconcileRollsProxyWhenTrustedCABundleChanges(t *testing.T) {
	objects := func(content string) []client.Object {
		return []client.Object{
			newMTC(tenant.DefaultAITenantName, testInfraNamespace),
			newAITenant(aitenantOpts{issuer: testIssuer}),
			newGateway(httpsListener(testHostname)),
			adminCredentialsSecret(),
			dashboardService(),
			trustedCABundle(content),
		}
	}

	r, rec := newReconciler(t, objects("one")...)
	reconcileMTC(t, r)
	first, _, _ := unstructured.NestedString(rec.objects["Deployment"].Object,
		"spec", "template", "metadata", "annotations", CABundleHashAnnotation)
	if first == "" {
		t.Fatal("Deployment pod template has no CA bundle hash")
	}

	r, rec = newReconciler(t, objects("two")...)
	reconcileMTC(t, r)
	second, _, _ := unstructured.NestedString(rec.objects["Deployment"].Object,
		"spec", "template", "metadata", "annotations", CABundleHashAnnotation)
	if second == first {
		t.Errorf("CA bundle hash did not change: %q", second)
	}
}

func TestReconcileOmitsCABundleHashWithoutBundleContent(t *testing.T) {
	emptyBundle := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{
		Name: TrustedCABundleConfigMap, Namespace: testInfraNamespace,
	}}
	cases := map[string][]client.Object{
		"missing bundle": {
			newMTC(tenant.DefaultAITenantName, testInfraNamespace),
			newAITenant(aitenantOpts{issuer: testIssuer}),
			newGateway(httpsListener(testHostname)),
			adminCredentialsSecret(),
			dashboardService(),
		},
		"empty bundle": {
			newMTC(tenant.DefaultAITenantName, testInfraNamespace),
			newAITenant(aitenantOpts{issuer: testIssuer}),
			newGateway(httpsListener(testHostname)),
			adminCredentialsSecret(),
			dashboardService(),
			emptyBundle,
		},
	}
	for name, objects := range cases {
		t.Run(name, func(t *testing.T) {
			r, rec := newReconciler(t, objects...)
			reconcileMTC(t, r)
			hash, _, _ := unstructured.NestedString(rec.objects["Deployment"].Object,
				"spec", "template", "metadata", "annotations", CABundleHashAnnotation)
			if hash != "" {
				t.Errorf("CA bundle hash = %q, want empty", hash)
			}
		})
	}
}

func TestMaasTenantConfigForCABundleFiltersConfigMaps(t *testing.T) {
	r, _ := newReconciler(t, newMTC(tenant.DefaultAITenantName, testInfraNamespace))
	bundle := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{
		Name: TrustedCABundleConfigMap, Namespace: testInfraNamespace,
	}}

	reqs := r.maasTenantConfigForCABundle(context.Background(), bundle)
	if len(reqs) != 1 || reqs[0].Namespace != testTenantNamespace || reqs[0].Name != tenant.MaasTenantConfigInstanceName {
		t.Fatalf("trusted CA bundle mapped to %v", reqs)
	}

	otherName := bundle.DeepCopy()
	otherName.SetName("other-configmap")
	if reqs := r.maasTenantConfigForCABundle(context.Background(), otherName); len(reqs) != 0 {
		t.Errorf("other ConfigMap name mapped to %v", reqs)
	}
	otherNamespace := bundle.DeepCopy()
	otherNamespace.SetNamespace("other-namespace")
	if reqs := r.maasTenantConfigForCABundle(context.Background(), otherNamespace); len(reqs) != 0 {
		t.Errorf("other ConfigMap namespace mapped to %v", reqs)
	}
}

func TestMaasTenantConfigForPortalServiceFiltersNames(t *testing.T) {
	r, _ := newReconciler(t, newMTC(tenant.DefaultAITenantName, testInfraNamespace))
	service := &corev1.Service{ObjectMeta: metav1.ObjectMeta{
		Name: PortalServiceName, Namespace: DefaultDashboardNamespace,
	}}
	if reqs := r.maasTenantConfigForPortalService(context.Background(), service); len(reqs) != 1 {
		t.Fatalf("maas-portal Service mapped to %v, want the MaasTenantConfig", reqs)
	}
	other := service.DeepCopy()
	other.SetName("other-service")
	if reqs := r.maasTenantConfigForPortalService(context.Background(), other); len(reqs) != 0 {
		t.Errorf("other Service name mapped to %v", reqs)
	}
}

func TestMaasTenantConfigForDSCIEnqueuesAllConfigs(t *testing.T) {
	r, _ := newReconciler(t, newMTC(tenant.DefaultAITenantName, testInfraNamespace))
	if reqs := r.maasTenantConfigForDSCI(context.Background(), dsci("odh-applications")); len(reqs) != 1 {
		t.Fatalf("DSCI mapped to %v, want the MaasTenantConfig", reqs)
	}
}

func TestMaasTenantConfigForPortalRouteFiltersNamesAndNamespace(t *testing.T) {
	r, _ := newReconciler(t, newMTC(tenant.DefaultAITenantName, testInfraNamespace))
	route := newConsumerPortalObject(gvkHTTPRoute, PortalRouteName, testInfraNamespace, false)

	if reqs := r.maasTenantConfigForPortalRoute(context.Background(), route); len(reqs) != 1 {
		t.Fatalf("portal route mapped to %v, want the MaasTenantConfig", reqs)
	}
	otherName := route.DeepCopy()
	otherName.SetName("other-route")
	if reqs := r.maasTenantConfigForPortalRoute(context.Background(), otherName); len(reqs) != 0 {
		t.Errorf("other route name mapped to %v", reqs)
	}
	otherNamespace := route.DeepCopy()
	otherNamespace.SetNamespace("other-namespace")
	if reqs := r.maasTenantConfigForPortalRoute(context.Background(), otherNamespace); len(reqs) != 0 {
		t.Errorf("route in a foreign namespace mapped to %v", reqs)
	}
}

func withClientSecretRef(aitenant *unstructured.Unstructured) *unstructured.Unstructured {
	_ = unstructured.SetNestedField(aitenant.Object, map[string]any{"name": "portal-oidc", "key": "client-secret"}, "spec", "oidc", "clientSecretRef")
	_ = unstructured.SetNestedField(aitenant.Object, "2m", "spec", "oidc", "refreshInterval")
	_ = unstructured.SetNestedField(aitenant.Object, "8h", "spec", "oidc", "cookieExpire")
	return aitenant
}

func tenantClientSecret(value string) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "portal-oidc", Namespace: testTenantNamespace},
		Data:       map[string][]byte{"client-secret": []byte(value)},
	}
}

func appliedSecretData(t *testing.T, rec *recorder) map[string]string {
	t.Helper()
	secret := rec.objects["Secret"]
	if secret == nil {
		t.Fatal("credentials Secret was not applied")
	}
	encoded, _, _ := unstructured.NestedStringMap(secret.Object, "data")
	out := map[string]string{}
	for k, v := range encoded {
		raw, err := base64.StdEncoding.DecodeString(v)
		if err != nil {
			t.Fatalf("decode %s: %v", k, err)
		}
		out[k] = string(raw)
	}
	return out
}

func deploymentHash(rec *recorder) string {
	hash, _, _ := unstructured.NestedString(rec.objects["Deployment"].Object, "spec", "template", "metadata", "annotations", CredentialsHashAnnotation)
	return hash
}

func TestReconcileBuildsCredentialsFromTenantClientSecret(t *testing.T) {
	r, rec := newReconciler(t,
		newMTC(tenant.DefaultAITenantName, testInfraNamespace),
		withClientSecretRef(newAITenant(aitenantOpts{issuer: testIssuer})),
		newGateway(httpsListener(testHostname)),
		tenantClientSecret("s3cret"),
		dashboardService(),
	)

	reconcileMTC(t, r)

	data := appliedSecretData(t, rec)
	if data[keyClientID] != "test-client" || data[keyClientSecret] != "s3cret" || len(data[keyCookieSecret]) == 0 {
		t.Errorf("credentials = %v", data)
	}
	if rec.objects["Secret"].GetLabels()[tenant.LabelManagedBy] != tenant.ManagedByAIGatewayController {
		t.Error("credentials Secret is not labeled as managed by this controller")
	}
	if deploymentHash(rec) == "" {
		t.Error("Deployment pod template has no credentials hash")
	}
	args, err := json.Marshal(rec.objects["Deployment"].Object)
	if err != nil {
		t.Fatalf("marshal Deployment: %v", err)
	}
	if !strings.Contains(string(args), "--cookie-refresh=2m0s") {
		t.Error("refreshInterval was not applied as --cookie-refresh")
	}
	if !strings.Contains(string(args), "--cookie-expire=8h0m0s") {
		t.Error("cookieExpire was not applied as --cookie-expire")
	}
}

func TestReconcileKeepsCookieSecretAndRestartsOnRotation(t *testing.T) {
	existing := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: CredentialsSecretName, Namespace: testInfraNamespace},
		Data:       map[string][]byte{keyCookieSecret: []byte("keep-me")},
	}
	objects := func(secret string) []client.Object {
		return []client.Object{
			newMTC(tenant.DefaultAITenantName, testInfraNamespace),
			withClientSecretRef(newAITenant(aitenantOpts{issuer: testIssuer})),
			newGateway(httpsListener(testHostname)),
			tenantClientSecret(secret), existing.DeepCopy(),
			dashboardService(),
		}
	}

	r, rec := newReconciler(t, objects("one")...)
	reconcileMTC(t, r)
	if got := appliedSecretData(t, rec)[keyCookieSecret]; got != "keep-me" {
		t.Errorf("cookie secret = %q, want preserved value", got)
	}
	first := deploymentHash(rec)

	r, rec = newReconciler(t, objects("two")...)
	reconcileMTC(t, r)
	if second := deploymentHash(rec); second == first {
		t.Error("credentials hash did not change after the client secret rotated")
	}
}

func TestReconcileWaitsForTenantClientSecret(t *testing.T) {
	r, rec := newReconciler(t,
		newMTC(tenant.DefaultAITenantName, testInfraNamespace),
		withClientSecretRef(newAITenant(aitenantOpts{issuer: testIssuer})),
		newGateway(httpsListener(testHostname)),
	)
	if res := reconcileMTC(t, r); res.RequeueAfter != constants.NotReadyRequeueInterval {
		t.Errorf("RequeueAfter = %v", res.RequeueAfter)
	}
	if len(rec.applied) != 0 {
		t.Errorf("applied %v without the tenant client Secret", rec.applied)
	}
}

func TestCleanupDeletesOnlyManagedCredentials(t *testing.T) {
	managed := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{
		Name: CredentialsSecretName, Namespace: testInfraNamespace,
		Labels: map[string]string{tenant.LabelManagedBy: tenant.ManagedByAIGatewayController},
	}}
	r, _ := newReconciler(t, managed)
	if err := r.cleanup(context.Background(), testInfraNamespace, testInfraNamespace); err != nil {
		t.Fatalf("cleanup: %v", err)
	}
	if err := r.Client.Get(context.Background(), types.NamespacedName{Namespace: testInfraNamespace, Name: CredentialsSecretName}, &corev1.Secret{}); !apierrors.IsNotFound(err) {
		t.Errorf("managed credentials Secret still exists (err=%v)", err)
	}

	r, _ = newReconciler(t, adminCredentialsSecret())
	if err := r.cleanup(context.Background(), testInfraNamespace, testInfraNamespace); err != nil {
		t.Fatalf("cleanup: %v", err)
	}
	if err := r.Client.Get(context.Background(), types.NamespacedName{Namespace: testInfraNamespace, Name: CredentialsSecretName}, &corev1.Secret{}); err != nil {
		t.Errorf("administrator credentials Secret was deleted: %v", err)
	}
}
