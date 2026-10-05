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

package tenant

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	v1alpha1 "github.com/opendatahub-io/ai-gateway-controller/api/inference/v1alpha1"
	"github.com/opendatahub-io/ai-gateway-controller/pkg/envelope"
	"github.com/opendatahub-io/ai-gateway-controller/pkg/render"
	"github.com/opendatahub-io/ai-gateway-controller/pkg/resolver"
)

// notReadyRequeueInterval is used when a MaasTenantConfig has opted into
// praxis but is not yet ready for it (see IsActive / GatewayRef), or when a
// transition-in is blocked on the IPP migration marker. This is a "come
// back shortly" wait, distinct from ResyncInterval's steady-state resync.
const notReadyRequeueInterval = 10 * time.Second

// Reconciler primarily watches MaasTenantConfig CRs (owned by
// maas-controller) — mirroring maas-controller's own TenantReconciler — and,
// for every tenant that UsesPraxis (default when AnnotationPayloadProcessingType
// is absent or "praxis"), renders and SSA-applies a per-tenant copy of the
// vendored praxis-extproc manifests into that tenant's Gateway namespace.
// Tenants annotated with PayloadProcessingBackendIPP are ignored:
// maas-controller's own TenantReconciler owns their legacy IPP deployment.
//
// Reconciler still Gets a tenant's owning AITenant (see
// OwningAITenantRef / GatewayRef / IsActive) — status.gatewayRef and
// status.phase live there, not on MaasTenantConfig — but no longer watches
// AITenant as its primary trigger; it only watches AITenant secondarily, to
// react to gatewayRef/phase changes that AnnotationPayloadProcessingType
// alone would miss.
//
// Reconciler does not write AITenant at all (status or otherwise). It does
// track, via PraxisCleanupFinalizer on MaasTenantConfig, whether it has (or
// may have) applied resources for a tenant, so it can delete them again
// when the tenant switches away from praxis or the MaasTenantConfig is
// deleted — SSA only ever upserts the current render set, it never deletes
// what falls out of it.
type Reconciler struct {
	// Client applies the rendered resources and reads/updates MaasTenantConfig
	// (and reads AITenant).
	Client client.Client
	// APIReader reads directly from the API server, bypassing the informer
	// cache. The AITenant identity/ownership/readiness decision must be made
	// against live state: a cache-backed read can lag a delete+recreate during
	// a watch disruption, letting the UID fence compare stale-to-stale and
	// adopt a replaced tenant.
	APIReader client.Reader
	// ManifestPath is the controller-owned kustomize entrypoint, e.g.
	// config/manifests/external-model/overlays/odh. It composes the pinned
	// upstream praxis-extproc tree with controller-specific patches.
	ManifestPath string
	// Image replaces the vendored overlay's placeholder container image.
	Image string
	// SkipNetworkPolicy omits the controller-managed payload-processing
	// NetworkPolicy when an installation supplies equivalent networking and the
	// target namespace disallows this controller from creating policies. It is
	// false by default and must be set explicitly by the installer.
	SkipNetworkPolicy bool
	// MaaSAPIRouteNameBase is the base name used to disable ext_proc on
	// maas-api's own HTTPRoute rules; suffixed per tenant like every other
	// resource this package renames.
	MaaSAPIRouteNameBase string
	// ResyncInterval is the RequeueAfter used once a tenant's resources have
	// been successfully applied, so drift gets corrected periodically even
	// without a new watch event (mirrors maas-controller's TenantReconciler
	// setFinalStatus, and this controller's own former --resync-interval
	// ticker). Must be positive.
	ResyncInterval time.Duration
	// DeletionTimeout bounds how long Reconcile keeps retrying cleanup
	// before force-removing PraxisCleanupFinalizer without confirming
	// cleanup succeeded (mirrors maas-controller's
	// AITenantReconciler.DeletionTimeout / forceRemoveAITenantFinalizer):
	// without this, a persistent cleanup failure (or this controller being
	// down) would block MaasTenantConfig (and therefore AITenant) deletion
	// forever. Zero disables the timeout and retries indefinitely.
	DeletionTimeout time.Duration
	// Log receives one entry per reconcile, plus any render/apply/cleanup
	// error.
	Log logr.Logger
}

// SetupWithManager registers the MaasTenantConfig watch (primary trigger)
// and a secondary AITenant watch mapped back to the owning MaasTenantConfig,
// mirroring maas-controller's own TenantReconciler.SetupWithManager shape.
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(NewMaasTenantConfig()).
		Watches(
			NewAITenant(),
			handler.EnqueueRequestsFromMapFunc(r.enqueueMaasTenantConfigForAITenant),
		).
		Watches(&v1alpha1.ExternalModel{}, handler.EnqueueRequestsFromMapFunc(r.maasTenantConfigForNamespace)).
		Watches(&v1alpha1.ExternalProvider{}, handler.EnqueueRequestsFromMapFunc(r.maasTenantConfigForNamespace)).
		WatchesMetadata(&corev1.Secret{}, handler.EnqueueRequestsFromMapFunc(r.maasTenantConfigForNamespace)).
		Watches(&corev1.ConfigMap{}, handler.EnqueueRequestsFromMapFunc(r.maasTenantConfigForRoutingOverlay)).
		Complete(r)
}

func (r *Reconciler) maasTenantConfigForRoutingOverlay(ctx context.Context, obj client.Object) []reconcile.Request {
	if obj.GetName() != praxisOverlayName {
		return nil
	}
	return r.maasTenantConfigForNamespace(ctx, obj)
}

func (r *Reconciler) maasTenantConfigForNamespace(_ context.Context, obj client.Object) []reconcile.Request {
	if obj.GetNamespace() == "" {
		return nil
	}
	return []reconcile.Request{{NamespacedName: types.NamespacedName{
		Namespace: obj.GetNamespace(),
		Name:      MaasTenantConfigInstanceName,
	}}}
}

// enqueueMaasTenantConfigForAITenant maps an AITenant event to the
// MaasTenantConfig it owns, via status.tenantNamespace (see
// ConfigNamespace) — avoiding any need to duplicate maas-controller's
// TenantNamespaceForAITenant naming convention, which depends on a
// configurable default tenant namespace this controller does not know.
func (r *Reconciler) enqueueMaasTenantConfigForAITenant(_ context.Context, obj client.Object) []reconcile.Request {
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return nil
	}
	namespace, ok := ConfigNamespace(u)
	if !ok {
		return nil
	}
	return []reconcile.Request{{NamespacedName: types.NamespacedName{
		Namespace: namespace,
		Name:      MaasTenantConfigInstanceName,
	}}}
}

// Reconcile implements the logic documented on Reconciler.
func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := r.Log.WithValues("maastenantconfig", req.NamespacedName)

	mtc := NewMaasTenantConfig()
	if err := r.Client.Get(ctx, req.NamespacedName, mtc); err != nil {
		if apierrors.IsNotFound(err) {
			// Already fully gone: our finalizer (if we ever added one)
			// must have already been cleared, or we never added one.
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("get MaasTenantConfig %s: %w", req.NamespacedName, err)
	}

	tenantID := IdentifierFor(mtc)

	if !mtc.GetDeletionTimestamp().IsZero() {
		return r.reconcileDelete(ctx, log, mtc, tenantID)
	}

	if !UsesPraxis(mtc) {
		return r.reconcileNotPraxis(ctx, log, mtc, tenantID)
	}

	return r.reconcilePraxis(ctx, log, mtc, tenantID)
}

// resolveOwnedAITenant Gets the AITenant named by OwningAITenantRef and
// verifies status.tenantNamespace owns mtc. It does not require phase Active:
// cleanup/delete must still use status.gatewayRef while the AITenant is
// Terminating (otherwise PraxisCleanupFinalizer blocks MTC deletion until
// DeletionTimeout). A nil aitenant with a nil error means annotations aren't
// populated yet or the AITenant is gone — a normal transient state.
func (r *Reconciler) resolveOwnedAITenant(ctx context.Context, mtc *unstructured.Unstructured) (aitenant *unstructured.Unstructured, err error) {
	name, namespace, ok := OwningAITenantRef(mtc)
	if !ok {
		return nil, nil
	}
	aitenant = NewAITenant()
	if err := r.Client.Get(ctx, client.ObjectKey{Name: name, Namespace: namespace}, aitenant); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("get owning AITenant %s/%s: %w", namespace, name, err)
	}
	ownedNS, ownedOK := ConfigNamespace(aitenant)
	if !ownedOK || ownedNS != mtc.GetNamespace() {
		return nil, fmt.Errorf(
			"AITenant %s/%s status.tenantNamespace %q does not own MaasTenantConfig in %q; refusing spoofed owning-AITenant annotations",
			namespace, name, ownedNS, mtc.GetNamespace())
	}
	return aitenant, nil
}

// resolveOwningAITenant is resolveOwnedAITenant plus the Active readiness
// gate used by the praxis apply path.
func (r *Reconciler) resolveOwningAITenant(ctx context.Context, mtc *unstructured.Unstructured) (aitenant *unstructured.Unstructured, ready bool, err error) {
	aitenant, err = r.resolveOwnedAITenant(ctx, mtc)
	if err != nil || aitenant == nil {
		return aitenant, false, err
	}
	// Require both an Active phase and status observed for the current spec
	// generation: an Active phase whose gatewayRef still reflects a superseded
	// generation (maas-controller mid-reconcile after a spec change) must not
	// drive a praxis-extproc install against a stale Gateway.
	if !IsActive(aitenant) || !StatusIsCurrent(aitenant) {
		return aitenant, false, nil
	}
	return aitenant, true, nil
}

// aiTenantStillValidForApply re-reads the owning AITenant live (via APIReader,
// not the informer cache) immediately before the praxis-extproc apply and
// reports whether it is still the exact object, in the exact state, that this
// reconcile resolved and rendered against. It is the TOCTOU (Time-of-Check to
// Time-of-Use) fence for the apply path (RHAI-2514): between the cached resolve
// and the write, the AITenant may have been deleted, recreated with a new UID,
// re-homed to a different gateway, or had its spec bumped ahead of maas. Any of
// those means the resources computed from the resolved copy would target the
// wrong tenant/gateway or a superseded generation, so it must requeue.
//
// It returns false (transient ⇒ requeue, not an error) when, read live, the
// AITenant: is gone; has a different metadata.uid than wantUID; no longer binds
// this MaasTenantConfig (status.tenantNamespace); is not Active; is not current
// for its generation (StatusIsCurrent); or no longer publishes the same
// status.gatewayRef this reconcile rendered against. An empty wantUID disables
// the check, so a live object always carries a UID and only tests can opt out.
// This guards the apply path only: delete/cleanup must run regardless of
// identity (see resolveOwnedAITenant).
func (r *Reconciler) aiTenantStillValidForApply(ctx context.Context, mtc *unstructured.Unstructured, wantUID types.UID, wantGatewayName, wantGatewayNamespace string) (bool, error) {
	if wantUID == "" {
		return true, nil
	}
	name, namespace, ok := OwningAITenantRef(mtc)
	if !ok {
		return false, nil
	}
	aitenant := NewAITenant()
	// Read live from the API server, not the informer cache: a cache-backed
	// re-read can lag a delete+recreate or a spec update during a watch
	// disruption, defeating this fence. APIReader must be wired (see
	// Reconciler.APIReader); reads fail rather than silently degrade.
	if err := r.APIReader.Get(ctx, client.ObjectKey{Name: name, Namespace: namespace}, aitenant); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("re-read owning AITenant %s/%s: %w", namespace, name, err)
	}
	// Terminating: the owner's delete/cleanup is already in flight, so a fresh
	// apply would provision for a tenant on its way out. deletionTimestamp is the
	// only signal that flips here — uid/bind/active/current/gatewayRef can all
	// still pass while the object is finalizing.
	if !aitenant.GetDeletionTimestamp().IsZero() {
		return false, nil
	}
	// Identity: the same object, not a delete+recreate that reused the name.
	if aitenant.GetUID() != wantUID {
		return false, nil
	}
	// Ownership bind still holds live (anti-spoof): re-checked because the
	// resolve above read the possibly-stale cache.
	if ns, ok := ConfigNamespace(aitenant); !ok || ns != mtc.GetNamespace() {
		return false, nil
	}
	// Readiness + generation currency against live state: an Active tenant
	// whose status lags a spec change (StatusIsCurrent false) must not drive an
	// install against a superseded generation.
	if !IsActive(aitenant) || !StatusIsCurrent(aitenant) {
		return false, nil
	}
	// The gateway we rendered against must still be the live one: a re-home
	// (new status.gatewayRef for the same object) must not be applied against
	// the old gateway.
	gwName, gwNamespace, gwReady := GatewayRef(aitenant)
	if !gwReady || gwName != wantGatewayName || gwNamespace != wantGatewayNamespace {
		return false, nil
	}
	return true, nil
}

// reconcilePraxis is the steady-state path for a tenant that currently
// opts into praxis: ensure the cleanup finalizer is present (before doing
// anything else, so even a partially-applied tenant is guaranteed a
// cleanup pass later), wait for readiness, then render/apply.
func (r *Reconciler) reconcilePraxis(ctx context.Context, log logr.Logger, mtc *unstructured.Unstructured, tenantID string) (ctrl.Result, error) {
	if err := r.ensureFinalizer(ctx, mtc); err != nil {
		return ctrl.Result{}, fmt.Errorf("ensure finalizer: %w", err)
	}

	aitenant, ready, err := r.resolveOwningAITenant(ctx, mtc)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !ready {
		log.Info("MaasTenantConfig opted into praxis but owning AITenant is not Active for its current generation yet; will retry")
		return ctrl.Result{RequeueAfter: notReadyRequeueInterval}, nil
	}

	gatewayName, gatewayNamespace, gwReady := GatewayRef(aitenant)
	if !gwReady {
		log.Info("owning AITenant is Active but status.gatewayRef is not populated; will retry")
		return ctrl.Result{RequeueAfter: notReadyRequeueInterval}, nil
	}

	// Handshake: claim cleanup-complete → steady. Absent means wait (existing
	// tenants are assumed to still run legacy IPP until they signal
	// cleanup-complete). Status itself is the durable claim — no bundleExists gate.
	ready, err = EnsurePraxisMayDeploy(ctx, r.Client, mtc)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("ensure praxis may deploy: %w", err)
	}
	if !ready {
		log.Info("waiting for the legacy IPP cleanup to finish before applying praxis-extproc")
		return ctrl.Result{RequeueAfter: notReadyRequeueInterval}, nil
	}

	rendered, err := render.Build(r.ManifestPath)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("render: %w", err)
	}

	resources := render.PostRender(rendered, render.Params{
		Namespace:        gatewayNamespace,
		GatewayName:      gatewayName,
		Image:            r.Image,
		MaaSAPIRouteName: ResourceName(r.MaaSAPIRouteNameBase, tenantID),
	})

	resources, err = Rename(resources, tenantID, gatewayNamespace)
	if err != nil {
		// Not retryable until the tenant's name changes: don't requeue, or
		// every resync would fail identically for the same reason. A future
		// change re-triggers reconciliation via the watch.
		log.Error(err, "cannot render praxis-extproc resources for this tenant name; will not retry until the tenant changes")
		return ctrl.Result{}, nil
	}
	tenantNamespace := mtc.GetNamespace()
	hasModels, err := r.hasActiveExternalModels(ctx, tenantNamespace)
	if err != nil {
		return ctrl.Result{}, err
	}
	var runtimeCandidates []envelope.Candidate
	if hasModels {
		_, requiredProviders, providerErr := r.providersForTenant(ctx, tenantNamespace)
		if providerErr != nil {
			return ctrl.Result{}, providerErr
		}
		ready, reason, overlayErr := r.routingOverlayReady(ctx, tenantNamespace, requiredProviders)
		if overlayErr != nil {
			return ctrl.Result{}, fmt.Errorf("check routing overlay: %w", overlayErr)
		}
		if !ready {
			log.Info("routing overlay is not ready for ExternalModel ExtProc", "reason", reason, "namespace", tenantNamespace)
			return ctrl.Result{RequeueAfter: notReadyRequeueInterval}, nil
		}
		runtimeCandidates, err = r.runtimeCandidates(ctx, tenantNamespace)
		if err != nil {
			return ctrl.Result{}, err
		}
	} else if err := r.cleanupExternalModelResources(ctx, tenantID, gatewayNamespace, tenantNamespace); err != nil {
		return ctrl.Result{}, fmt.Errorf("clean up ExternalModel ExtProc resources: %w", err)
	}

	resources, err = SplitPostAuthResources(resources, tenantID, gatewayNamespace, tenantNamespace)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("split ExternalModel ExtProc resources: %w", err)
	}
	if hasModels {
		if err := configureExternalModelExtProc(resources, tenantNamespace, runtimeCandidates); err != nil {
			return ctrl.Result{}, fmt.Errorf("configure ExternalModel ExtProc: %w", err)
		}
	} else {
		resources = RemoveExternalModelResources(resources, tenantID)
	}
	if r.SkipNetworkPolicy {
		filtered := make([]unstructured.Unstructured, 0, len(resources))
		for _, resource := range resources {
			if resource.GetKind() != "NetworkPolicy" {
				filtered = append(filtered, resource)
			}
		}
		resources = filtered
		log.Info("omitting controller-managed NetworkPolicy by explicit configuration", "namespace", gatewayNamespace)
	}
	for i := range resources {
		labels := resources[i].GetLabels()
		if labels == nil {
			labels = make(map[string]string)
		}
		labels[managedByLabel] = render.FieldOwner
		resources[i].SetLabels(labels)
	}
	if err := r.waitForForeignOwnership(ctx, resources); err != nil {
		log.Info("praxis-extproc resources are still owned by another controller; waiting for handoff", "error", err)
		return ctrl.Result{RequeueAfter: notReadyRequeueInterval}, nil
	}

	// Re-validate the owning AITenant live, immediately before writing cluster
	// resources: the resolve above read the informer cache, which can lag a
	// delete+recreate (new UID), a re-home (new gatewayRef), or a spec bump
	// (generation ahead of status) during a watch disruption. Everything
	// rendered — gateway namespace, tenant identifier — was computed from the
	// resolved copy, so if the live object no longer matches on identity,
	// ownership, readiness, generation, or gateway, it must not be applied
	// (RHAI-2514 apply-path fence). Transient ⇒ requeue.
	if valid, err := r.aiTenantStillValidForApply(ctx, mtc, aitenant.GetUID(), gatewayName, gatewayNamespace); err != nil {
		return ctrl.Result{}, err
	} else if !valid {
		log.Info("owning AITenant changed (uid, ownership, readiness, generation, or gatewayRef) before praxis-extproc apply; will re-resolve")
		return ctrl.Result{RequeueAfter: notReadyRequeueInterval}, nil
	}

	if err := render.Apply(ctx, r.Client, resources); err != nil {
		log.Error(err, "praxis-extproc apply failed for tenant; will retry")
		return ctrl.Result{}, fmt.Errorf("apply: %w", err)
	}

	// Drop any leftover tenant-scoped standalone Praxis hop from earlier
	// releases. ExtProc is the dataplane; ExternalModel routes backend to
	// provider ExternalName Services directly.
	if err := r.deleteStandalonePraxis(ctx, tenantID, tenantNamespace); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.deleteResourceIfOwned(ctx, gvkClusterRoleBinding, PayloadProcessingReaderClusterRoleBindingPostNameForTenant(tenantID), ""); err != nil {
		return ctrl.Result{}, err
	}

	log.Info("praxis-extproc install applied",
		"tenantID", tenantID, "namespace", gatewayNamespace, "gatewayName", gatewayName)
	return ctrl.Result{RequeueAfter: r.ResyncInterval}, nil
}

// deleteStandalonePraxis removes controller-owned standalone Praxis resources
// left from releases that deployed a tenant-scoped praxis-ai hop.
func (r *Reconciler) deleteStandalonePraxis(ctx context.Context, tenantID, tenantNamespace string) error {
	for _, t := range []struct {
		gvk  schema.GroupVersionKind
		name string
	}{
		{gvkServiceAccount, ResourceName(praxisServiceAccount, tenantID)},
		{gvkConfigMap, ResourceName(praxisConfigMapName, tenantID)},
		{gvkService, ResourceName(praxisServiceName, tenantID)},
		{gvkDeployment, ResourceName(praxisDeploymentName, tenantID)},
	} {
		if err := r.deleteResourceIfOwned(ctx, t.gvk, t.name, tenantNamespace); err != nil {
			return err
		}
	}
	return nil
}

func (r *Reconciler) hasActiveExternalModels(ctx context.Context, namespace string) (bool, error) {
	models := &v1alpha1.ExternalModelList{}
	if err := r.Client.List(ctx, models, client.InNamespace(namespace)); err != nil {
		return false, fmt.Errorf("list ExternalModels for cleanup: %w", err)
	}
	for i := range models.Items {
		if models.Items[i].GetDeletionTimestamp().IsZero() {
			return true, nil
		}
	}
	return false, nil
}

func (r *Reconciler) cleanupExternalModelResources(ctx context.Context, tenantID, gatewayNamespace, tenantNamespace string) error {
	targets := []struct {
		gvk             schema.GroupVersionKind
		name, namespace string
	}{
		{gvkDeployment, PayloadProcessingExternalModelDeploymentName(tenantID), tenantNamespace},
		{gvkService, PayloadProcessingExternalModelServiceName(tenantID), tenantNamespace},
		{gvkConfigMap, PayloadProcessingExternalModelPluginsConfigMapForTenant(tenantID), tenantNamespace},
		{gvkServiceAccount, PayloadProcessingExternalModelServiceAccountName(tenantID), tenantNamespace},
		{gvkDestinationRule, PayloadProcessingExternalModelServiceName(tenantID), tenantNamespace},
		{gvkDestinationRule, PayloadProcessingExternalModelServiceName(tenantID), gatewayNamespace},
		{gvkEnvoyFilter, PayloadProcessingExternalModelFilterNameForTenant(tenantID), gatewayNamespace},
		{gvkEnvoyFilter, PayloadProcessingExternalModelEnvoyFilterName(tenantID), gatewayNamespace},
	}
	for _, target := range targets {
		if err := r.deleteResourceIfOwned(ctx, target.gvk, target.name, target.namespace); err != nil {
			return err
		}
	}
	return nil
}

func (r *Reconciler) providersForTenant(ctx context.Context, namespace string) ([]v1alpha1.ExternalProvider, map[string]bool, error) {
	var providers v1alpha1.ExternalProviderList
	if err := r.Client.List(ctx, &providers, client.InNamespace(namespace)); err != nil {
		return nil, nil, fmt.Errorf("list tenant ExternalProviders: %w", err)
	}
	var models v1alpha1.ExternalModelList
	if err := r.Client.List(ctx, &models, client.InNamespace(namespace)); err != nil {
		return nil, nil, fmt.Errorf("list tenant ExternalModels: %w", err)
	}
	referenced := map[string]bool{}
	required := map[string]bool{}
	for i := range models.Items {
		for _, ref := range models.Items[i].Spec.ExternalProviderRefs {
			name := ref.Ref.Name
			if name == "" {
				continue
			}
			referenced[name] = true
			if ref.Weight == nil || *ref.Weight > 0 {
				required[name] = true
			}
		}
	}
	result := make([]v1alpha1.ExternalProvider, 0, len(providers.Items))
	for i := range providers.Items {
		if referenced[providers.Items[i].Name] {
			result = append(result, providers.Items[i])
		}
	}
	return result, required, nil
}

func (r *Reconciler) runtimeCandidates(ctx context.Context, namespace string) ([]envelope.Candidate, error) {
	var models v1alpha1.ExternalModelList
	if err := r.Client.List(ctx, &models, client.InNamespace(namespace)); err != nil {
		return nil, fmt.Errorf("list ExternalModels for ExtProc runtime configuration: %w", err)
	}
	var providers v1alpha1.ExternalProviderList
	if err := r.Client.List(ctx, &providers, client.InNamespace(namespace)); err != nil {
		return nil, fmt.Errorf("list ExternalProviders for ExtProc runtime configuration: %w", err)
	}
	modelPtrs := make([]*v1alpha1.ExternalModel, 0, len(models.Items))
	for i := range models.Items {
		modelPtrs = append(modelPtrs, &models.Items[i])
	}
	providerPtrs := make([]*v1alpha1.ExternalProvider, 0, len(providers.Items))
	for i := range providers.Items {
		providerPtrs = append(providerPtrs, &providers.Items[i])
	}
	runtimeSet, err := resolver.ResolveAll(modelPtrs, providerPtrs)
	if err != nil {
		return nil, fmt.Errorf("resolve ExtProc runtime provider set: %w", err)
	}
	candidates := make([]envelope.Candidate, 0, len(runtimeSet.Routes()))
	for _, route := range runtimeSet.Routes() {
		strategy, err := envelope.StrategyFor(route)
		if err != nil {
			return nil, fmt.Errorf("resolve ExtProc runtime credential for model %s provider %s: %w", route.Model, route.Provider, err)
		}
		candidate := envelope.Candidate{
			Cluster: route.Cluster, StableID: envelope.CandidateStableID(route.Model, route.Provider),
			Kind: "inference_model", Name: route.ClientName, Fresh: true,
		}
		if strategy != "" {
			candidate.Credential = &envelope.Credential{
				Strategy:  strategy,
				SecretRef: envelope.SecretRef{Name: route.SecretName, Namespace: route.Namespace, Key: route.SecretKey},
			}
		}
		candidates = append(candidates, candidate)
	}
	return candidates, nil
}

func (r *Reconciler) routingOverlayReady(ctx context.Context, namespace string, requiredProviders map[string]bool) (bool, string, error) {
	var configMap corev1.ConfigMap
	if err := r.Client.Get(ctx, client.ObjectKey{Namespace: namespace, Name: praxisOverlayName}, &configMap); err != nil {
		if apierrors.IsNotFound(err) {
			return false, "routing overlay is absent", nil
		}
		return false, "", fmt.Errorf("get routing overlay %s/%s: %w", namespace, praxisOverlayName, err)
	}
	if configMap.GetLabels()[managedByLabel] != render.FieldOwner {
		return false, "routing overlay is not owned by ai-gateway-controller", nil
	}
	raw := configMap.Data[praxisOverlayDataKey]
	if raw == "" {
		return false, "routing overlay has no envelope data", nil
	}
	var env envelope.Envelope
	if err := json.Unmarshal([]byte(raw), &env); err != nil {
		return overlayNotReady("routing overlay is not valid JSON")
	}
	if env.SchemaVersion != envelope.SchemaVersion || env.Revision.Value == "" || env.ContentDigest.Value != env.Revision.Value || env.Scope.Namespace != namespace {
		return false, "routing overlay envelope metadata is invalid", nil
	}
	digest, err := envelope.ComputeDigestFromWire([]byte(raw))
	if err != nil || digest != env.Revision.Value {
		return overlayNotReady("routing overlay digest does not match its declared revision")
	}
	available := make(map[string]bool, len(env.Overlay.Candidates))
	for _, candidate := range env.Overlay.Candidates {
		available[candidate.Cluster] = true
	}
	for provider := range requiredProviders {
		if !available["provider-"+provider] {
			return false, fmt.Sprintf("routing overlay does not contain provider %s", provider), nil
		}
	}
	return true, "routing overlay is controller-owned and digest-valid", nil
}

func overlayNotReady(reason string) (bool, string, error) {
	return false, reason, nil
}

const managedByLabel = "app.kubernetes.io/managed-by"

func (r *Reconciler) waitForForeignOwnership(ctx context.Context, resources []unstructured.Unstructured) error {
	for i := range resources {
		desired := &resources[i]
		if desired.GetKind() == "ClusterRole" && desired.GetName() == "payload-processing-reader" {
			continue
		}
		current := &unstructured.Unstructured{}
		current.SetGroupVersionKind(desired.GroupVersionKind())
		if err := r.Client.Get(ctx, client.ObjectKeyFromObject(desired), current); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return fmt.Errorf("inspect %s %s/%s: %w", desired.GetKind(), desired.GetNamespace(), desired.GetName(), err)
		}
		if isReleasedPluginConfigMap(desired) && current.GetAnnotations()["opendatahub.io/managed"] == "false" {
			continue
		}
		if current.GetLabels()[managedByLabel] != render.FieldOwner {
			return fmt.Errorf("%s %s/%s is managed by %q", desired.GetKind(), desired.GetNamespace(), desired.GetName(), current.GetLabels()[managedByLabel])
		}
	}
	return nil
}

func isReleasedPluginConfigMap(obj *unstructured.Unstructured) bool {
	name := obj.GetName()
	return name == PayloadProcessingPluginsConfigMapName || strings.HasPrefix(name, PayloadProcessingPluginsConfigMapName+"-")
}

// reconcileNotPraxis handles a tenant that currently does not opt into
// praxis. If our finalizer is present, this tenant previously opted in and
// has since switched away (or dropped the annotation): clean up whatever
// was applied, signal maas-controller that it may now (re)deploy legacy IPP
// (see MarkPayloadProcessingCleanupComplete), then release the finalizer.
// Otherwise this tenant never used praxis and there is nothing to do.
func (r *Reconciler) reconcileNotPraxis(ctx context.Context, log logr.Logger, mtc *unstructured.Unstructured, tenantID string) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(mtc, PraxisCleanupFinalizer) {
		log.V(1).Info("MaasTenantConfig does not use the praxis payload processing backend; nothing to do")
		return ctrl.Result{}, nil
	}

	aitenant, err := r.resolveOwnedAITenant(ctx, mtc)
	if err != nil {
		return ctrl.Result{}, err
	}
	if aitenant == nil {
		log.Info("waiting for owning AITenant before praxis-extproc cleanup after switch-away")
		return ctrl.Result{RequeueAfter: notReadyRequeueInterval}, nil
	}
	_, gatewayNamespace, gwReady := GatewayRef(aitenant)
	if !gwReady {
		log.Info("waiting for status.gatewayRef before praxis-extproc cleanup after switch-away")
		return ctrl.Result{RequeueAfter: notReadyRequeueInterval}, nil
	}

	if err := r.cleanup(ctx, tenantID, gatewayNamespace, mtc.GetNamespace()); err != nil {
		log.Error(err, "praxis-extproc cleanup failed after switching away from praxis; will retry", "namespace", gatewayNamespace)
		return ctrl.Result{}, fmt.Errorf("cleanup: %w", err)
	}
	if tenantNamespace, ok := ConfigNamespace(aitenant); ok {
		if err := r.deleteStandalonePraxis(ctx, tenantID, tenantNamespace); err != nil {
			return ctrl.Result{}, fmt.Errorf("cleanup standalone praxis: %w", err)
		}
	}
	if err := MarkPayloadProcessingCleanupComplete(ctx, r.Client, mtc); err != nil {
		return ctrl.Result{}, fmt.Errorf("mark payload-processing cleanup complete: %w", err)
	}
	log.Info("praxis-extproc resources cleaned up after switching away from praxis", "tenantID", tenantID, "namespace", gatewayNamespace)

	if err := r.removeFinalizer(ctx, mtc); err != nil {
		return ctrl.Result{}, fmt.Errorf("remove finalizer: %w", err)
	}
	return ctrl.Result{}, nil
}

// reconcileDelete handles a tenant whose MaasTenantConfig is being deleted.
func (r *Reconciler) reconcileDelete(ctx context.Context, log logr.Logger, mtc *unstructured.Unstructured, tenantID string) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(mtc, PraxisCleanupFinalizer) {
		// We never opted this tenant in (or already finished cleanup):
		// nothing for us to do, and no finalizer of ours blocking deletion.
		return ctrl.Result{}, nil
	}

	if r.DeletionTimeout > 0 && time.Since(mtc.GetDeletionTimestamp().Time) >= r.DeletionTimeout {
		log.Error(nil, "praxis-extproc cleanup exceeded deletion timeout; force-removing finalizer without confirming cleanup succeeded",
			"tenantID", tenantID, "timeout", r.DeletionTimeout)
		return ctrl.Result{}, r.removeFinalizer(ctx, mtc)
	}

	aitenant, err := r.resolveOwnedAITenant(ctx, mtc)
	if err != nil {
		return ctrl.Result{}, err
	}
	if aitenant == nil {
		log.Info("MaasTenantConfig deleting but owning AITenant is not resolvable yet; will retry cleanup")
		return ctrl.Result{RequeueAfter: notReadyRequeueInterval}, nil
	}

	_, gatewayNamespace, gwReady := GatewayRef(aitenant)
	if !gwReady {
		log.Info("MaasTenantConfig deleting but status.gatewayRef is not populated; will retry cleanup")
		return ctrl.Result{RequeueAfter: notReadyRequeueInterval}, nil
	}

	if err := r.cleanup(ctx, tenantID, gatewayNamespace, mtc.GetNamespace()); err != nil {
		log.Error(err, "praxis-extproc cleanup failed for deleted tenant; will retry", "namespace", gatewayNamespace)
		return ctrl.Result{}, fmt.Errorf("cleanup: %w", err)
	}
	if tenantNamespace, ok := ConfigNamespace(aitenant); ok {
		if err := r.deleteStandalonePraxis(ctx, tenantID, tenantNamespace); err != nil {
			return ctrl.Result{}, fmt.Errorf("cleanup standalone praxis: %w", err)
		}
	}

	log.Info("praxis-extproc resources cleaned up for deleted tenant", "tenantID", tenantID, "namespace", gatewayNamespace)
	return ctrl.Result{}, r.removeFinalizer(ctx, mtc)
}

// ensureFinalizer adds PraxisCleanupFinalizer if not already present.
func (r *Reconciler) ensureFinalizer(ctx context.Context, mtc *unstructured.Unstructured) error {
	if controllerutil.ContainsFinalizer(mtc, PraxisCleanupFinalizer) {
		return nil
	}
	base := mtc.DeepCopy()
	controllerutil.AddFinalizer(mtc, PraxisCleanupFinalizer)
	if err := r.Client.Patch(ctx, mtc, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{})); err != nil {
		return err
	}
	return nil
}

// removeFinalizer removes PraxisCleanupFinalizer if present.
func (r *Reconciler) removeFinalizer(ctx context.Context, mtc *unstructured.Unstructured) error {
	if !controllerutil.ContainsFinalizer(mtc, PraxisCleanupFinalizer) {
		return nil
	}
	base := mtc.DeepCopy()
	controllerutil.RemoveFinalizer(mtc, PraxisCleanupFinalizer)
	if err := r.Client.Patch(ctx, mtc, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{})); err != nil {
		return err
	}
	return nil
}

// cleanup deletes (ignoring not-found) every resource this controller
// would have applied for tenantID in namespace, mirroring
// tenantreconcile.cleanupTenantResources / cleanupPayloadProcessingHPA in
// maas-controller: SSA only ever upserts the current render set, it never
// deletes what falls out of it. The shared payload-processing-reader
// ClusterRole is never deleted here: every tenant's ClusterRoleBinding
// references that one role, so deleting it would break every other
// praxis tenant sharing the cluster.
func (r *Reconciler) cleanup(ctx context.Context, tenantID, gatewayNamespace, tenantNamespace string) error {
	type target struct {
		gvk       schema.GroupVersionKind
		name      string
		namespace string
	}

	targets := []target{
		{gvkDeployment, PayloadProcessingDeploymentName(tenantID), gatewayNamespace},
		{gvkDeployment, PayloadPreProcessingDeploymentName(tenantID), gatewayNamespace},
		{gvkDeployment, PayloadProcessingDeploymentName(tenantID), tenantNamespace},
		{gvkDeployment, PayloadProcessingExternalModelDeploymentName(tenantID), tenantNamespace},
		{gvkService, PayloadProcessingServiceName(tenantID), gatewayNamespace},
		{gvkService, PayloadPreProcessingServiceName(tenantID), gatewayNamespace},
		{gvkService, PayloadProcessingServiceName(tenantID), tenantNamespace},
		{gvkService, PayloadProcessingExternalModelServiceName(tenantID), tenantNamespace},
		{gvkConfigMap, PayloadProcessingPluginsConfigMapForTenant(tenantID), gatewayNamespace},
		{gvkConfigMap, PayloadProcessingPluginsConfigMapForTenant(tenantID), tenantNamespace},
		{gvkConfigMap, PayloadProcessingExternalModelPluginsConfigMapForTenant(tenantID), tenantNamespace},
		{gvkServiceAccount, PayloadProcessingServiceAccountName(tenantID), gatewayNamespace},
		{gvkServiceAccount, PayloadProcessingServiceAccountName(tenantID), tenantNamespace},
		{gvkServiceAccount, PayloadProcessingPostServiceAccountName(tenantID), tenantNamespace},
		{gvkServiceAccount, PayloadProcessingExternalModelServiceAccountName(tenantID), tenantNamespace},
		{gvkNetworkPolicy, PayloadProcessingNetworkPolicyName(tenantID), gatewayNamespace},
		{gvkNetworkPolicy, PayloadProcessingNetworkPolicyName(tenantID), tenantNamespace},
		{gvkEnvoyFilter, PayloadProcessingEnvoyFilterName(tenantID), gatewayNamespace},
		{gvkEnvoyFilter, PayloadProcessingExternalModelFilterNameForTenant(tenantID), gatewayNamespace},
		{gvkEnvoyFilter, PayloadProcessingExternalModelEnvoyFilterName(tenantID), gatewayNamespace},
		{gvkDestinationRule, PayloadProcessingServiceName(tenantID), gatewayNamespace},
		{gvkDestinationRule, PayloadPreProcessingServiceName(tenantID), gatewayNamespace},
		{gvkDestinationRule, PayloadProcessingServiceName(tenantID), tenantNamespace},
		{gvkDestinationRule, PayloadProcessingExternalModelServiceName(tenantID), gatewayNamespace},
		{gvkDestinationRule, PayloadProcessingExternalModelServiceName(tenantID), tenantNamespace},
		{gvkClusterRoleBinding, PayloadProcessingReaderClusterRoleBindingNameForTenant(tenantID), ""},
		{gvkClusterRoleBinding, PayloadProcessingReaderClusterRoleBindingPostNameForTenant(tenantID), ""},
	}

	for _, t := range targets {
		if err := r.deleteResourceIfOwned(ctx, t.gvk, t.name, t.namespace); err != nil {
			return err
		}
	}
	return nil
}

func (r *Reconciler) deleteResourceIfOwned(ctx context.Context, gvk schema.GroupVersionKind, name, namespace string) error {
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(gvk)
	obj.SetName(name)
	obj.SetNamespace(namespace)
	if err := r.Client.Get(ctx, client.ObjectKeyFromObject(obj), obj); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("get %s %s/%s: %w", gvk.Kind, namespace, name, err)
	}
	if !shouldDeletePraxisResource(obj) {
		return nil
	}
	if err := client.IgnoreNotFound(r.Client.Delete(ctx, obj)); err != nil {
		return fmt.Errorf("delete %s %s/%s: %w", gvk.Kind, namespace, name, err)
	}
	return nil
}
