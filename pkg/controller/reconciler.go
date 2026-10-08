// Package controller contains the ExternalModel and ExternalProvider control loop.
package controller

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	v1alpha1 "github.com/opendatahub-io/ai-gateway-controller/api/inference/v1alpha1"
	"github.com/opendatahub-io/ai-gateway-controller/pkg/envelope"
	"github.com/opendatahub-io/ai-gateway-controller/pkg/publisher"
	"github.com/opendatahub-io/ai-gateway-controller/pkg/render"
	"github.com/opendatahub-io/ai-gateway-controller/pkg/resolver"
	"github.com/opendatahub-io/ai-gateway-controller/pkg/tenant"
)

const (
	conditionReady                = "Ready"
	conditionOverlayDistributed   = "OverlayDistributed"
	reasonReconciled              = "Reconciled"
	reasonReconcileFailed         = "ReconcileFailed"
	reasonNotPraxis               = "NotPraxisTenant"
	reasonTenantNotReady          = "TenantNotReady"
	reasonNoRoutes                = "NoRoutes"
	reasonProviderNotReady        = "ProviderNotReady"
	providerServicePrefix         = "provider-"
	providerSelectionSinkPrefix   = "provider-selection-required-"
	selectedProviderHeader        = "X-AI-Routing-Candidate"
	externalTenantLabel           = "inference.opendatahub.io/external-tenant"
	externalTenantIDLabel         = "inference.opendatahub.io/external-tenant-id"
	externalGatewayNameLabel      = "inference.opendatahub.io/external-gateway"
	externalGatewayNamespaceLabel = "inference.opendatahub.io/external-gateway-namespace"
	externalModelPreExtProcFilter = "envoy.filters.http.ext_proc.external-model-pre"
	externalModelExtProcFilter    = "envoy.filters.http.ext_proc.external-model"
	modelRoutePrefix              = "external-model-"
	externalModelFinalizer        = "inference.opendatahub.io/external-model-cleanup"
)

var errCredentialNotReady = errors.New("effective provider credential is not ready")

// Reconciler is the sole writer for the ExternalModel transport plane and the
// routing overlay. It resolves one namespace-scoped route set and renders both
// planes from that same result.
type Reconciler struct {
	client.Client
	// APIReader reads referenced Secret data directly from the API server. The
	// manager cache only watches Secret metadata to avoid caching credentials.
	APIReader        client.Reader
	Scheme           *runtime.Scheme
	Namespace        string
	GatewayName      string
	GatewayNamespace string
	Network          string
	LocalSite        string
	ConfigMap        string
	KnownClusters    []string
	// ProducerVersion is supplied by build metadata and recorded in the
	// content-addressed overlay provenance.
	ProducerVersion string
	Log             logr.Logger
	// ApplyResource and PublishOverlay are test seams. Production leaves them
	// nil, selecting the real SSA renderer and publisher below. Applying one
	// object at a time preserves the production order and lets tests inject a
	// failure at each SSA boundary without production-only flags.
	ApplyResource  func(context.Context, client.Client, unstructured.Unstructured) error
	PublishOverlay func(context.Context, *resolver.ResolvedRouteSet, envelope.Scope, envelope.Options) (publisher.Result, error)
}

// SetupWithManager registers model, provider, Secret, MaasTenantConfig, and
// AITenant watches. Provider, Secret, and MaasTenantConfig changes fan out to
// affected models in the same namespace; AITenant changes fan out to models
// in its resolved tenant namespace because status and Gateway identity remain
// owned by AITenant.
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	inScope := predicate.NewPredicateFuncs(func(obj client.Object) bool { return r.namespaceAllowed(obj.GetNamespace()) })
	return builder.ControllerManagedBy(mgr).
		For(&v1alpha1.ExternalModel{}, builder.WithPredicates(inScope)).
		Watches(&v1alpha1.ExternalProvider{}, handler.EnqueueRequestsFromMapFunc(r.providerModels), builder.WithPredicates(inScope)).
		WatchesMetadata(&corev1.Secret{}, handler.EnqueueRequestsFromMapFunc(r.secretModels), builder.WithPredicates(inScope)).
		// The tenant reconciler creates the tenant-local Praxis Service. A
		// model may otherwise publish an HTTPRoute before that Service exists;
		// Istio can retain ResolvedRefs=False until the route is reconciled
		// again. Service events are namespace-scoped and therefore only fan out
		// to models in the affected tenant.
		Watches(&corev1.Service{}, handler.EnqueueRequestsFromMapFunc(r.serviceModels), builder.WithPredicates(inScope)).
		WatchesRawSource(source.Kind(mgr.GetCache(), tenant.NewMaasTenantConfig(), handler.TypedEnqueueRequestsFromMapFunc[*unstructured.Unstructured, reconcile.Request](r.maasTenantConfigModels))).
		WatchesRawSource(source.Kind(mgr.GetCache(), tenant.NewAITenant(), handler.TypedEnqueueRequestsFromMapFunc[*unstructured.Unstructured, reconcile.Request](r.tenantModels))).
		Complete(r)
}

func (r *Reconciler) providerModels(ctx context.Context, obj client.Object) []reconcile.Request {
	if !r.namespaceAllowed(obj.GetNamespace()) {
		return nil
	}
	return r.modelsReferencingProviders(ctx, obj.GetNamespace(), map[string]bool{obj.GetName(): true})
}

func (r *Reconciler) secretModels(ctx context.Context, obj client.Object) []reconcile.Request {
	if !r.namespaceAllowed(obj.GetNamespace()) {
		return nil
	}
	var providers v1alpha1.ExternalProviderList
	if err := r.List(ctx, &providers, client.InNamespace(obj.GetNamespace())); err != nil {
		r.Log.Error(err, "list providers for Secret event", "secret", obj.GetName())
		return nil
	}
	providerNames := map[string]bool{}
	for _, p := range providers.Items {
		if p.Spec.Auth.SecretRef.Name == obj.GetName() {
			providerNames[p.Name] = true
		}
	}
	return r.modelsReferencingProviders(ctx, obj.GetNamespace(), providerNames)
}

func (r *Reconciler) serviceModels(ctx context.Context, obj client.Object) []reconcile.Request {
	if !r.namespaceAllowed(obj.GetNamespace()) {
		return nil
	}
	return r.modelsInNamespace(ctx, obj.GetNamespace())
}

func (r *Reconciler) tenantModels(ctx context.Context, ait *unstructured.Unstructured) []reconcile.Request {
	ns, _, _ := unstructured.NestedString(ait.Object, "status", "tenantNamespace")
	if ns == "" {
		ns = ait.GetNamespace()
	}
	if !r.namespaceAllowed(ns) {
		return nil
	}
	return r.modelsInNamespace(ctx, ns)
}

func (r *Reconciler) maasTenantConfigModels(ctx context.Context, mtc *unstructured.Unstructured) []reconcile.Request {
	if mtc.GetName() != tenant.MaasTenantConfigInstanceName || !r.namespaceAllowed(mtc.GetNamespace()) {
		return nil
	}
	return r.modelsInNamespace(ctx, mtc.GetNamespace())
}

func (r *Reconciler) modelsInNamespace(ctx context.Context, namespace string) []reconcile.Request {
	if !r.namespaceAllowed(namespace) {
		return nil
	}
	var models v1alpha1.ExternalModelList
	if err := r.List(ctx, &models, client.InNamespace(namespace)); err != nil {
		r.Log.Error(err, "list models for dependent event", "namespace", namespace)
		return nil
	}
	requests := make([]reconcile.Request, 0, len(models.Items))
	for i := range models.Items {
		requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&models.Items[i])})
	}
	return requests
}

func (r *Reconciler) modelsReferencingProviders(ctx context.Context, namespace string, providers map[string]bool) []reconcile.Request {
	if len(providers) == 0 || !r.namespaceAllowed(namespace) {
		return nil
	}
	var models v1alpha1.ExternalModelList
	if err := r.List(ctx, &models, client.InNamespace(namespace)); err != nil {
		r.Log.Error(err, "list models for provider event", "namespace", namespace)
		return nil
	}
	requests := make([]reconcile.Request, 0, len(models.Items))
	for i := range models.Items {
		referencesProvider := false
		for _, ref := range models.Items[i].Spec.ExternalProviderRefs {
			if providers[ref.Ref.Name] {
				referencesProvider = true
				break
			}
		}
		if referencesProvider {
			requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&models.Items[i])})
		}
	}
	return requests
}

// providerInputs validates providers before building the route set. The
// persisted phase is informational and may have been written by the
// handoff-side IPP controller; the Secret and transport reconciliation below
// are the authoritative gates for Praxis. Invalid providers remain excluded
// and fail closed.
func (r *Reconciler) providerInputs(ctx context.Context, namespace string) (
	v1alpha1.ExternalProviderList, []*v1alpha1.ExternalProvider,
	[]*v1alpha1.ExternalProvider, error,
) {
	var providers v1alpha1.ExternalProviderList
	if err := r.List(ctx, &providers, client.InNamespace(namespace)); err != nil {
		return providers, nil, nil, err
	}
	providerPtrs := make([]*v1alpha1.ExternalProvider, 0, len(providers.Items))
	validProviders := make([]*v1alpha1.ExternalProvider, 0, len(providers.Items))
	for i := range providers.Items {
		p := &providers.Items[i]
		if err := r.validateProvider(ctx, p); err != nil {
			if statusErr := r.updateProviderStatus(ctx, p, false, reasonReconcileFailed, err.Error()); statusErr != nil {
				return providers, nil, nil, statusErr
			}
			continue
		}
		ready := p.DeepCopy()
		if ready.Status.Phase != resolver.PhaseReady {
			ready.Status.Phase = resolver.PhaseReady
		}
		providerPtrs = append(providerPtrs, ready)
		validProviders = append(validProviders, p)
	}
	return providers, providerPtrs, validProviders, nil
}

// Reconcile applies provider resources, then model routes, then the overlay.
// The overlay is never published when an earlier stage fails.
func (r *Reconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	if !r.namespaceAllowed(req.Namespace) {
		return reconcile.Result{}, nil
	}
	var model v1alpha1.ExternalModel
	if err := r.Get(ctx, req.NamespacedName, &model); err != nil {
		if apierrors.IsNotFound(err) {
			return reconcile.Result{}, nil
		}
		return reconcile.Result{}, err
	}
	// Deletion must not depend on MaasTenantConfig or its AITenant still being
	// present. Those objects can disappear first during a tenant teardown.
	// The deletion path uses the ExternalModel namespace and controller-owned
	// resource labels as its independent ownership proof.
	if !model.DeletionTimestamp.IsZero() {
		if err := r.reconcileDeletingModel(ctx, &model); err != nil {
			return reconcile.Result{}, err
		}
		return reconcile.Result{}, nil
	}
	modelTenant, proceed, err := r.prepareModelTenant(ctx, &model, req.Namespace)
	if err != nil {
		return reconcile.Result{}, err
	}
	if !proceed {
		return reconcile.Result{}, nil
	}
	gatewayName, gatewayNamespace := modelTenant.gatewayName, modelTenant.gatewayNamespace
	var models v1alpha1.ExternalModelList
	if err := r.List(ctx, &models, client.InNamespace(req.Namespace)); err != nil {
		return reconcile.Result{}, err
	}
	providers, providerPtrs, validProviders, err := r.providerInputs(ctx, req.Namespace)
	if err != nil {
		return reconcile.Result{}, err
	}

	modelPtrs := make([]*v1alpha1.ExternalModel, 0, len(models.Items))
	modelOwners := make(map[string]*v1alpha1.ExternalModel, len(models.Items))
	for i := range models.Items {
		modelPtrs = append(modelPtrs, &models.Items[i])
		modelOwners[models.Items[i].Name] = &models.Items[i]
	}
	providerOwners := make(map[string]*v1alpha1.ExternalProvider, len(providers.Items))
	for i := range providers.Items {
		providerOwners[providers.Items[i].Name] = &providers.Items[i]
	}
	set, err := resolver.Resolve(modelPtrs, providerPtrs)
	if err != nil {
		reason := reasonReconcileFailed
		if errors.Is(err, resolver.ErrNoRoutes) {
			reason = reasonProviderNotReady
		}
		if statusErr := r.updateModelStatus(ctx, &model, false, reason, err.Error(), nil); statusErr != nil {
			return reconcile.Result{}, statusErr
		}
		return reconcile.Result{}, err
	}
	if err := r.validateResolvedCredentials(ctx, set.Routes()); err != nil {
		reason := reasonReconcileFailed
		if errors.Is(err, errCredentialNotReady) {
			reason = reasonProviderNotReady
		}
		if statusErr := r.updateModelStatus(ctx, &model, false, reason, err.Error(), nil); statusErr != nil {
			return reconcile.Result{}, statusErr
		}
		return reconcile.Result{}, err
	}
	// The selector/status/owner may have changed while provider inputs and
	// credential references were being resolved. Do not publish or delete
	// serving state from a stale cached decision.
	if valid, err := r.livePraxisBeforeWrite(ctx, req.Namespace, modelTenant); err != nil {
		return reconcile.Result{}, err
	} else if !valid {
		return reconcile.Result{Requeue: true}, nil
	}
	if len(set.Routes()) == 0 {
		if err := r.enableExternalModelRoutes(ctx, tenant.ID(modelTenant.aitenant.GetName()), req.Namespace, gatewayName, gatewayNamespace, nil); err != nil {
			return reconcile.Result{}, err
		}
		if err := r.deleteLegacyExternalModelRouteFilter(ctx, tenant.ID(modelTenant.aitenant.GetName())); err != nil {
			return reconcile.Result{}, err
		}
		if cleanupErr := r.cleanupTransport(ctx, req.Namespace, gatewayName, gatewayNamespace, nil); cleanupErr != nil {
			return reconcile.Result{}, cleanupErr
		}
		if cleanupErr := r.cleanupOverlay(ctx, req.Namespace); cleanupErr != nil {
			return reconcile.Result{}, cleanupErr
		}
		message := "no ExternalModel provider references resolved to a Ready provider"
		if statusErr := r.updateModelStatus(ctx, &model, false, reasonNoRoutes, message, nil); statusErr != nil {
			return reconcile.Result{}, statusErr
		}
		return reconcile.Result{}, resolver.ErrNoRoutes
	}
	if err := r.applyTransport(ctx, set.Routes(), req.Namespace, tenant.ID(modelTenant.aitenant.GetName()), gatewayName, gatewayNamespace, modelOwners, providerOwners); err != nil {
		for _, p := range validProviders {
			if statusErr := r.updateProviderStatus(ctx, p, false, reasonReconcileFailed, err.Error()); statusErr != nil {
				return reconcile.Result{}, statusErr
			}
		}
		if statusErr := r.updateModelStatus(ctx, &model, false, reasonReconcileFailed, err.Error(), nil); statusErr != nil {
			return reconcile.Result{}, statusErr
		}
		return reconcile.Result{}, err
	}
	if err := r.cleanupExternalModelRouteFilters(ctx, req.Namespace, tenant.ID(modelTenant.aitenant.GetName()), gatewayName, gatewayNamespace); err != nil {
		return reconcile.Result{}, err
	}
	if err := r.cleanupTransport(ctx, req.Namespace, gatewayName, gatewayNamespace, set.Routes()); err != nil {
		if statusErr := r.updateModelStatus(ctx, &model, false, reasonReconcileFailed, err.Error(), nil); statusErr != nil {
			return reconcile.Result{}, statusErr
		}
		return reconcile.Result{}, err
	}
	for _, p := range validProviders {
		if statusErr := r.updateProviderStatus(ctx, p, true, reasonReconciled, "provider Secret reference and transport resources are ready"); statusErr != nil {
			return reconcile.Result{}, statusErr
		}
	}

	pub, err := publisher.New(r.Client, publisher.Config{Namespace: req.Namespace, Name: r.ConfigMap})
	if err != nil {
		return reconcile.Result{}, err
	}
	publish := r.PublishOverlay
	if publish == nil {
		publish = pub.Publish
	}
	result, err := publish(ctx, set, envelope.Scope{
		Network: r.Network, Gateway: gatewayName, Namespace: req.Namespace, LocalSite: r.localSite(),
	}, envelope.Options{
		KnownClusters: r.KnownClusters, SourceUID: string(model.UID), ProducerVersion: r.ProducerVersion,
	})
	if err != nil {
		if statusErr := r.updateModelStatus(ctx, &model, false, reasonReconcileFailed, err.Error(), nil); statusErr != nil {
			return reconcile.Result{}, statusErr
		}
		return reconcile.Result{}, err
	}

	message := fmt.Sprintf("distributed routing overlay digest %s generation %d", result.Envelope.Revision.Value, result.Envelope.Provenance.SourceGeneration)
	if err := r.updateModelStatus(ctx, &model, true, reasonReconciled, message, &result.Envelope); err != nil {
		return reconcile.Result{}, err
	}
	return reconcile.Result{}, nil
}

type modelTenantContext struct {
	aitenant         *unstructured.Unstructured
	aitenantUID      types.UID
	mtcUID           types.UID
	gatewayName      string
	gatewayNamespace string
}

// prepareModelTenant resolves the MaasTenantConfig selector and the owning
// AITenant before any ExternalModel serving state is published. A false
// proceed result means status/cleanup work was completed and the caller must
// stop this reconcile.
func (r *Reconciler) prepareModelTenant(ctx context.Context, model *v1alpha1.ExternalModel, namespace string) (modelTenantContext, bool, error) {
	// Re-read the selector, handoff status, deletion state, and owner from the
	// API server immediately before making the cleanup/apply decision. The
	// informer copy can be behind an IPP switch or an AITenant replacement.
	live, err := r.liveTenantResolution(ctx, namespace)
	if err != nil {
		return modelTenantContext{}, false, err
	}
	if live.missingConfig {
		if err := r.updateModelStatus(ctx, model, false, reasonTenantNotReady, "MaasTenantConfig/default-tenant is missing; retaining serving state and waiting for the owner to resolve", nil); err != nil {
			return modelTenantContext{}, false, err
		}
		return modelTenantContext{}, false, nil
	}
	if !live.selected {
		// Explicit IPP selection is a cleanup command. It does not wait for
		// cleanup-complete or steady, and it does not require a ready GatewayRef.
		if err := r.cleanupUnselectedModel(ctx, model); err != nil {
			return modelTenantContext{}, false, err
		}
		return modelTenantContext{}, false, nil
	}
	if live.aitenant == nil {
		if err := r.updateModelStatus(ctx, model, false, reasonTenantNotReady, "owning AITenant is not yet resolvable for MaasTenantConfig", nil); err != nil {
			return modelTenantContext{}, false, err
		}
		return modelTenantContext{}, false, nil
	}
	if live.status != tenant.PayloadProcessingStatusSteady {
		if err := r.updateModelStatus(ctx, model, false, reasonTenantNotReady, "MaasTenantConfig selects Praxis but payload-processing-status is not steady", nil); err != nil {
			return modelTenantContext{}, false, err
		}
		return modelTenantContext{}, false, nil
	}
	ait := live.aitenant
	gatewayName, gatewayNamespace, gatewayReady := tenant.GatewayRef(ait)
	if !gatewayReady {
		if err := r.updateModelStatus(ctx, model, false, reasonTenantNotReady, "AITenant Gateway reference is not ready", nil); err != nil {
			return modelTenantContext{}, false, err
		}
		return modelTenantContext{}, false, nil
	}
	if !tenant.IsActive(ait) || !tenant.StatusIsCurrent(ait) {
		if err := r.updateModelStatus(ctx, model, false, reasonTenantNotReady, "AITenant is selected for Praxis but is not Active", nil); err != nil {
			return modelTenantContext{}, false, err
		}
		return modelTenantContext{}, false, nil
	}
	if handled, err := r.handleModelLifecycle(ctx, model, ait); handled {
		return modelTenantContext{}, false, err
	} else if err != nil {
		return modelTenantContext{}, false, err
	}
	return modelTenantContext{aitenant: ait, aitenantUID: ait.GetUID(), mtcUID: live.mtc.GetUID(), gatewayName: gatewayName, gatewayNamespace: gatewayNamespace}, true, nil
}

// cleanupUnselectedModel releases only resources owned by this controller when
// the tenant no longer selects Praxis. The default IPP path is not targeted.
func (r *Reconciler) cleanupUnselectedModel(ctx context.Context, model *v1alpha1.ExternalModel) error {
	// Re-check immediately before deleting. A cached IPP decision must not
	// tear down a tenant that has already switched back to Praxis.
	live, err := r.liveTenantResolution(ctx, model.Namespace)
	if err != nil {
		return err
	}
	if live.missingConfig || live.selected || live.mtc == nil || !live.mtc.GetDeletionTimestamp().IsZero() {
		return nil
	}
	if err := r.deleteExternalModelRouteFiltersForTenant(ctx, model.Namespace); err != nil {
		return err
	}
	if live.aitenant != nil {
		if err := r.deleteLegacyExternalModelRouteFilter(ctx, tenant.ID(live.aitenant.GetName())); err != nil {
			return err
		}
	}
	if err := r.cleanupTransport(ctx, model.Namespace, "", "", nil); err != nil {
		return err
	}
	if err := r.cleanupOverlay(ctx, model.Namespace); err != nil {
		return err
	}
	if controllerutil.ContainsFinalizer(model, externalModelFinalizer) {
		return r.removeExternalModelFinalizer(ctx, model)
	}
	return nil
}

// reconcileDeletingModel is intentionally independent of the MTC/AITenant
// handoff. A model finalizer must not strand credential-bearing or routing
// resources merely because the owner was deleted first.
func (r *Reconciler) reconcileDeletingModel(ctx context.Context, model *v1alpha1.ExternalModel) error {
	// An explicit IPP selector is a cleanup command even when the AITenant
	// still exists. Do this live check before the normal deletion rebuild path;
	// otherwise a surviving sibling can cause us to republish its Praxis
	// overlay while the tenant is handing control back to IPP.
	live, err := r.liveTenantResolution(ctx, model.Namespace)
	if err != nil {
		return err
	}
	if !live.missingConfig && !live.selected && live.mtc != nil && live.mtc.GetDeletionTimestamp().IsZero() {
		return r.cleanupUnselectedModel(ctx, model)
	}
	if live.missingConfig || live.mtc == nil || live.aitenant == nil ||
		!live.mtc.GetDeletionTimestamp().IsZero() || !live.aitenant.GetDeletionTimestamp().IsZero() ||
		live.status != tenant.PayloadProcessingStatusSteady || !tenant.IsActive(live.aitenant) ||
		!tenant.StatusIsCurrent(live.aitenant) {
		return r.reconcileDeletedModelWithoutOwner(ctx, model)
	}
	if _, _, ready := tenant.GatewayRef(live.aitenant); !ready {
		return r.reconcileDeletedModelWithoutOwner(ctx, model)
	}
	return r.reconcileDeletedModel(ctx, model, live.aitenant)
}

// reconcileDeletedModelWithoutOwner performs the final-model cleanup using
// only the model namespace and controller ownership labels. If siblings remain,
// rebuilding their routes requires a live, steady Praxis tenant; the finalizer
// remains until that authorization is restored.
func (r *Reconciler) reconcileDeletedModelWithoutOwner(ctx context.Context, deleted *v1alpha1.ExternalModel) error {
	var models v1alpha1.ExternalModelList
	if err := r.List(ctx, &models, client.InNamespace(deleted.Namespace)); err != nil {
		return fmt.Errorf("list remaining ExternalModels for ownerless deletion: %w", err)
	}
	for i := range models.Items {
		if models.Items[i].Name != deleted.Name && models.Items[i].DeletionTimestamp.IsZero() {
			return fmt.Errorf("cannot rebuild routes for deleting ExternalModel %s/%s while sibling ExternalModels remain and the Praxis handoff is not steady", deleted.Namespace, deleted.Name)
		}
	}
	if err := r.deleteExternalModelRouteFiltersForTenant(ctx, deleted.Namespace); err != nil {
		return err
	}
	if err := r.cleanupTransport(ctx, deleted.Namespace, "", "", nil); err != nil {
		return err
	}
	if err := r.cleanupOverlay(ctx, deleted.Namespace); err != nil {
		return err
	}
	return r.removeExternalModelFinalizer(ctx, deleted)
}

func ownershipTenantID(tenantID string) string {
	if tenantID == "" {
		return tenant.DefaultAITenantName
	}
	return tenantID
}

func (r *Reconciler) handleModelLifecycle(ctx context.Context, model *v1alpha1.ExternalModel, ait *unstructured.Unstructured) (bool, error) {
	if !controllerutil.ContainsFinalizer(model, externalModelFinalizer) {
		if !model.DeletionTimestamp.IsZero() {
			return true, nil
		}
		base := model.DeepCopy()
		controllerutil.AddFinalizer(model, externalModelFinalizer)
		if err := r.Patch(ctx, model, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{})); err != nil {
			return true, fmt.Errorf("add ExternalModel cleanup finalizer: %w", err)
		}
	}
	if model.DeletionTimestamp.IsZero() {
		return false, nil
	}
	return true, r.reconcileDeletedModel(ctx, model, ait)
}

// reconcileDeletedModel republishes the namespace's remaining route set before
// releasing the ExternalModel finalizer. Deletion therefore does not depend on
// an unrelated sibling watch to remove stale transport or overlay state.
func (r *Reconciler) reconcileDeletedModel(ctx context.Context, deleted *v1alpha1.ExternalModel, ait *unstructured.Unstructured) error {
	gatewayName := r.gatewayName()
	gatewayNamespace := r.gatewayNamespace()
	tenantID := ""
	if ait != nil {
		tenantID = tenant.ID(ait.GetName())
	}
	if selectedGateway, selectedNamespace, ok := tenant.GatewayRef(ait); ok {
		gatewayName, gatewayNamespace = selectedGateway, selectedNamespace
	}
	return r.reconcileDeletedModelWithIdentity(ctx, deleted, tenantID, gatewayName, gatewayNamespace)
}

func (r *Reconciler) reconcileDeletedModelWithIdentity(ctx context.Context, deleted *v1alpha1.ExternalModel, tenantID, gatewayName, gatewayNamespace string) error {
	var models v1alpha1.ExternalModelList
	if err := r.List(ctx, &models, client.InNamespace(deleted.Namespace)); err != nil {
		return fmt.Errorf("list remaining ExternalModels for deletion: %w", err)
	}
	modelPtrs := make([]*v1alpha1.ExternalModel, 0, len(models.Items))
	modelOwners := make(map[string]*v1alpha1.ExternalModel, len(models.Items))
	for i := range models.Items {
		m := &models.Items[i]
		if m.Name == deleted.Name || !m.DeletionTimestamp.IsZero() {
			continue
		}
		modelPtrs = append(modelPtrs, m)
		modelOwners[m.Name] = m
	}
	providers, providerPtrs, _, err := r.providerInputs(ctx, deleted.Namespace)
	if err != nil {
		return err
	}
	providerOwners := make(map[string]*v1alpha1.ExternalProvider, len(providers.Items))
	for i := range providers.Items {
		providerOwners[providers.Items[i].Name] = &providers.Items[i]
	}
	set, err := resolver.Resolve(modelPtrs, providerPtrs)
	if errors.Is(err, resolver.ErrNoRoutes) {
		// A sibling still exists but is temporarily unroutable: retain the
		// last-known-good transport and overlay and retry. Only final-model
		// deletion is allowed to remove the shared serving state.
		if len(modelPtrs) > 0 {
			return err
		}
		if err := r.deleteExternalModelRouteFiltersForTenant(ctx, deleted.Namespace); err != nil {
			return err
		}
		if tenantID != "" {
			if err := r.deleteLegacyExternalModelRouteFilter(ctx, tenantID); err != nil {
				return err
			}
		}
		if err := r.cleanupTransport(ctx, deleted.Namespace, gatewayName, gatewayNamespace, nil); err != nil {
			return err
		}
		if err := r.cleanupOverlay(ctx, deleted.Namespace); err != nil {
			return err
		}
		return r.removeExternalModelFinalizer(ctx, deleted)
	}
	if err != nil {
		return fmt.Errorf("resolve remaining ExternalModels after deletion: %w", err)
	}
	if err := r.validateResolvedCredentials(ctx, set.Routes()); err != nil {
		return fmt.Errorf("validate remaining ExternalModel credentials after deletion: %w", err)
	}
	if err := r.applyTransport(ctx, set.Routes(), deleted.Namespace, tenantID, gatewayName, gatewayNamespace, modelOwners, providerOwners); err != nil {
		return fmt.Errorf("rebuild transport after ExternalModel deletion: %w", err)
	}
	if err := r.cleanupExternalModelRouteFilters(ctx, deleted.Namespace, tenantID, gatewayName, gatewayNamespace); err != nil {
		return err
	}
	if err := r.deleteLegacyExternalModelRouteFilter(ctx, tenantID); err != nil {
		return err
	}
	if err := r.cleanupTransport(ctx, deleted.Namespace, gatewayName, gatewayNamespace, set.Routes()); err != nil {
		return err
	}
	pub, err := publisher.New(r.Client, publisher.Config{Namespace: deleted.Namespace, Name: r.ConfigMap})
	if err != nil {
		return err
	}
	publish := r.PublishOverlay
	if publish == nil {
		publish = pub.Publish
	}
	if _, err := publish(ctx, set, envelope.Scope{
		Network: r.Network, Gateway: gatewayName, Namespace: deleted.Namespace, LocalSite: r.localSite(),
	}, envelope.Options{
		KnownClusters: r.KnownClusters, SourceUID: string(modelPtrs[0].UID), ProducerVersion: r.ProducerVersion,
	}); err != nil {
		return fmt.Errorf("publish remaining overlay after ExternalModel deletion: %w", err)
	}
	return r.removeExternalModelFinalizer(ctx, deleted)
}

func (r *Reconciler) removeExternalModelFinalizer(ctx context.Context, model *v1alpha1.ExternalModel) error {
	base := model.DeepCopy()
	if !controllerutil.RemoveFinalizer(model, externalModelFinalizer) {
		return nil
	}
	if err := r.Patch(ctx, model, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{})); err != nil {
		return fmt.Errorf("remove ExternalModel cleanup finalizer: %w", err)
	}
	return nil
}

func (r *Reconciler) cleanupTransport(ctx context.Context, namespace, gatewayName, gatewayNamespace string, routes []resolver.Route) error {
	providers := map[string]bool{}
	models := map[string]bool{}
	for _, route := range routes {
		providers[route.Provider] = true
		models[route.Model] = true
	}
	resources := []struct {
		kind, group, version, label string
		keep                        map[string]bool
	}{
		{"Service", "", "v1", "inference.opendatahub.io/external-provider", providers},
		{"Service", "", "v1", "inference.opendatahub.io/external-model", models},
		{"ServiceEntry", "networking.istio.io", "v1", "inference.opendatahub.io/external-provider", providers},
		{"DestinationRule", "networking.istio.io", "v1", "inference.opendatahub.io/external-provider", providers},
		{"HTTPRoute", "gateway.networking.k8s.io", "v1", "inference.opendatahub.io/external-model", models},
	}
	for _, resource := range resources {
		namespaces := []string{namespace}
		listOptions := []client.ListOption{client.MatchingLabels{"app.kubernetes.io/managed-by": "ai-gateway-controller"}}
		if resource.kind == "DestinationRule" {
			// DestinationRules are Gateway-local and may survive in a
			// previous Gateway namespace. The tenant ownership label is
			// required before listing cluster-wide; provider name and
			// managed-by alone are not a safe ownership key in a shared
			// Gateway namespace.
			namespaces = []string{""}
			listOptions = append(listOptions, client.MatchingLabels{externalTenantLabel: namespace})
		}
		for _, resourceNamespace := range namespaces {
			list := &unstructured.UnstructuredList{}
			list.SetGroupVersionKind(schema.GroupVersionKind{Group: resource.group, Version: resource.version, Kind: resource.kind + "List"})
			if err := r.List(ctx, list, append([]client.ListOption{client.InNamespace(resourceNamespace)}, listOptions...)...); err != nil {
				return fmt.Errorf("list stale %s resources: %w", resource.kind, err)
			}
			for i := range list.Items {
				name := list.Items[i].GetLabels()[resource.label]
				keep := resource.keep[name]
				if resource.kind == "DestinationRule" {
					labels := list.Items[i].GetLabels()
					// A provider name is not a sufficient keep key in a
					// shared Gateway namespace. Keep only the copy rendered
					// for this tenant's current Gateway. Empty routes are a
					// full cleanup request for this tenant's labeled rules.
					keep = keep && gatewayName != "" && gatewayNamespace != "" &&
						labels[externalGatewayNameLabel] == gatewayName &&
						labels[externalGatewayNamespaceLabel] == gatewayNamespace
				}
				if name != "" && !keep {
					if err := r.Delete(ctx, &list.Items[i]); client.IgnoreNotFound(err) != nil {
						return fmt.Errorf("delete stale %s %s/%s: %w", resource.kind, resourceNamespace, list.Items[i].GetName(), err)
					}
				}
			}
		}
	}
	return nil
}

// cleanupOverlay removes only the routing ConfigMap owned by this controller
// when a namespace leaves Praxis selection. It is intentionally label-gated:
// the ConfigMap is shared by models in one tenant namespace, but resources in
// other namespaces and any non-controller ConfigMap are never touched.
func (r *Reconciler) cleanupOverlay(ctx context.Context, namespace string) error {
	var cm corev1.ConfigMap
	key := client.ObjectKey{Namespace: namespace, Name: r.configMapName()}
	if err := r.Get(ctx, key, &cm); err != nil {
		return client.IgnoreNotFound(err)
	}
	if cm.Labels["app.kubernetes.io/managed-by"] != "ai-gateway-controller" {
		return nil
	}
	return client.IgnoreNotFound(r.Delete(ctx, &cm))
}

type tenantResolution struct {
	mtc           *unstructured.Unstructured
	aitenant      *unstructured.Unstructured
	selected      bool
	missingConfig bool
	status        string
}

func (r *Reconciler) resolveTenantForNamespace(ctx context.Context, namespace string) (tenantResolution, error) {
	mtc := tenant.NewMaasTenantConfig()
	err := r.Get(ctx, client.ObjectKey{Namespace: namespace, Name: tenant.MaasTenantConfigInstanceName}, mtc)
	if apierrors.IsNotFound(err) {
		// The MaasTenantConfig is the sole selector. If it disappears, do
		// not infer ownership from an AITenant and do not delete serving
		// state: the owner may be recreating or handing off the config.
		return tenantResolution{missingConfig: true}, nil
	}
	if err != nil {
		return tenantResolution{}, fmt.Errorf("get MaasTenantConfig %s/%s: %w", namespace, tenant.MaasTenantConfigInstanceName, err)
	}
	ait, err := r.resolveOwningAITenant(ctx, mtc)
	if err != nil {
		return tenantResolution{}, err
	}
	return tenantResolution{mtc: mtc, aitenant: ait, selected: tenant.UsesPraxis(mtc), status: tenant.PayloadProcessingStatus(mtc)}, nil
}

func (r *Reconciler) resolveOwningAITenant(ctx context.Context, mtc *unstructured.Unstructured) (*unstructured.Unstructured, error) {
	name, namespace, ok := tenant.OwningAITenantRef(mtc)
	if !ok {
		return nil, fmt.Errorf("MaasTenantConfig %s/%s has no owning AITenant reference", mtc.GetNamespace(), mtc.GetName())
	}
	ait := tenant.NewAITenant()
	if err := r.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, ait); err != nil {
		if apierrors.IsNotFound(err) {
			// A missing owner is a normal teardown state. Callers may still
			// perform namespace/label-proven cleanup for a deleting model.
			return nil, nil
		}
		return nil, fmt.Errorf("get owning AITenant %s/%s: %w", namespace, name, err)
	}
	resolvedNamespace, ok := tenant.ConfigNamespace(ait)
	if !ok || resolvedNamespace != mtc.GetNamespace() {
		return nil, fmt.Errorf("AITenant %s/%s does not own MaasTenantConfig %s/%s", namespace, name, mtc.GetNamespace(), mtc.GetName())
	}
	return ait, nil
}

// liveTenantResolution reads the MTC and its owner from APIReader rather than
// the informer cache. This is the handoff fence for ExternalModel serving.
func (r *Reconciler) liveTenantResolution(ctx context.Context, namespace string) (tenantResolution, error) {
	reader := r.APIReader
	if reader == nil {
		return tenantResolution{}, errors.New("APIReader is required for live MaasTenantConfig validation")
	}
	mtc := tenant.NewMaasTenantConfig()
	if err := reader.Get(ctx, client.ObjectKey{Namespace: namespace, Name: tenant.MaasTenantConfigInstanceName}, mtc); err != nil {
		if apierrors.IsNotFound(err) {
			return tenantResolution{missingConfig: true}, nil
		}
		return tenantResolution{}, fmt.Errorf("live get MaasTenantConfig %s/%s: %w", namespace, tenant.MaasTenantConfigInstanceName, err)
	}
	resolution := tenantResolution{mtc: mtc, selected: tenant.UsesPraxis(mtc), status: tenant.PayloadProcessingStatus(mtc)}
	name, ownerNamespace, ok := tenant.OwningAITenantRef(mtc)
	if !ok {
		return resolution, nil
	}
	ait := tenant.NewAITenant()
	if err := reader.Get(ctx, client.ObjectKey{Name: name, Namespace: ownerNamespace}, ait); err != nil {
		if apierrors.IsNotFound(err) {
			return resolution, nil
		}
		return tenantResolution{}, fmt.Errorf("live get owning AITenant %s/%s: %w", ownerNamespace, name, err)
	}
	if ownedNamespace, ok := tenant.ConfigNamespace(ait); !ok || ownedNamespace != namespace {
		return tenantResolution{}, fmt.Errorf("live AITenant %s/%s does not own MaasTenantConfig %s/%s", ownerNamespace, name, namespace, mtc.GetName())
	}
	resolution.aitenant = ait
	return resolution, nil
}

func (r *Reconciler) livePraxisBeforeWrite(ctx context.Context, namespace string, want modelTenantContext) (bool, error) {
	live, err := r.liveTenantResolution(ctx, namespace)
	if err != nil {
		return false, err
	}
	if live.missingConfig || !live.selected || live.status != tenant.PayloadProcessingStatusSteady || live.mtc == nil || live.aitenant == nil {
		return false, nil
	}
	if !live.mtc.GetDeletionTimestamp().IsZero() || !live.aitenant.GetDeletionTimestamp().IsZero() {
		return false, nil
	}
	if want.mtcUID != "" && live.mtc.GetUID() != want.mtcUID {
		return false, nil
	}
	if want.aitenantUID != "" && live.aitenant.GetUID() != want.aitenantUID {
		return false, nil
	}
	if !tenant.IsActive(live.aitenant) || !tenant.StatusIsCurrent(live.aitenant) {
		return false, nil
	}
	gatewayName, gatewayNamespace, ok := tenant.GatewayRef(live.aitenant)
	return ok && gatewayName == want.gatewayName && gatewayNamespace == want.gatewayNamespace, nil
}

// praxisTenantForNamespace is retained for callers/tests that need the
// resolved owner. Selection itself is deliberately read only from the
// tenant-local MaasTenantConfig.
func (r *Reconciler) praxisTenantForNamespace(ctx context.Context, namespace string) (*unstructured.Unstructured, bool, error) {
	resolution, err := r.resolveTenantForNamespace(ctx, namespace)
	if err != nil {
		return nil, false, err
	}
	return resolution.aitenant, resolution.selected, nil
}

func (r *Reconciler) validateProvider(_ context.Context, p *v1alpha1.ExternalProvider) error {
	if err := validateProviderEndpoint(p.Spec.Endpoint); err != nil {
		return err
	}
	authType := strings.ToLower(strings.TrimSpace(p.Spec.Auth.Type))
	if authType != "apikey" {
		if authType == "" {
			return errors.New("auth.type is required; supported strategy is apikey")
		}
		return fmt.Errorf("unsupported authentication strategy %q; only apikey is supported", p.Spec.Auth.Type)
	}
	if p.Spec.Auth.SecretRef.Name == "" {
		return errors.New("auth.secretRef.name is required")
	}
	return nil
}

// validateResolvedCredentials checks the effective credential after applying
// any ExternalModel ref override. This is intentionally route-based: the same
// resolved reference is published in the overlay and projected into ExtProc.
func (r *Reconciler) validateResolvedCredentials(ctx context.Context, routes []resolver.Route) error {
	if r.APIReader == nil {
		return errors.New("APIReader is required for credential Secret reads")
	}
	seen := map[client.ObjectKey]bool{}
	for _, route := range routes {
		if route.AuthType == "" {
			continue
		}
		if route.AuthType != "apikey" {
			return fmt.Errorf("model %s provider %s uses unsupported authentication strategy %q", route.Model, route.Provider, route.AuthType)
		}
		if route.SecretName == "" || route.SecretKey == "" {
			return fmt.Errorf("model %s provider %s has an incomplete effective credential reference", route.Model, route.Provider)
		}
		key := client.ObjectKey{Namespace: route.Namespace, Name: route.SecretName}
		if seen[key] {
			continue
		}
		seen[key] = true
		var secret corev1.Secret
		if err := r.APIReader.Get(ctx, key, &secret); err != nil {
			return fmt.Errorf("%w: Secret %s/%s: %w", errCredentialNotReady, key.Namespace, key.Name, err)
		}
		if _, ok := secret.Data[route.SecretKey]; !ok {
			return fmt.Errorf("%w: Secret %s/%s is missing key %s", errCredentialNotReady, key.Namespace, key.Name, route.SecretKey)
		}
	}
	return nil
}

func (r *Reconciler) namespaceAllowed(namespace string) bool {
	return namespace != "" && (r.Namespace == "" || r.Namespace == namespace)
}

func (r *Reconciler) applyTransport(ctx context.Context, routes []resolver.Route, modelNamespace, tenantID, gatewayName,
	gatewayNamespace string, modelOwners map[string]*v1alpha1.ExternalModel,
	providerOwners map[string]*v1alpha1.ExternalProvider) error {
	resources := make([]unstructured.Unstructured, 0, len(routes)*3+len(routes))
	seen := map[string]bool{}
	for _, route := range routes {
		for _, obj := range []unstructured.Unstructured{
			providerService(route, modelNamespace),
			providerServiceEntry(route, modelNamespace),
			providerDestinationRuleAtGateway(route, gatewayName, gatewayNamespace, tenantID),
		} {
			if owner := providerOwners[route.Provider]; owner != nil && owner.GetNamespace() == obj.GetNamespace() {
				setOwnerReference(&obj, owner)
			} else if obj.GetKind() == "DestinationRule" {
				// Gateway-local rules cannot be owned by the tenant-local
				// ExternalProvider. Explicitly clear any owner reference from
				// an older reconciliation so Kubernetes does not garbage-collect
				// the cross-namespace rule.
				obj.SetOwnerReferences([]metav1.OwnerReference{})
			}
			key := obj.GetKind() + "/" + obj.GetNamespace() + "/" + obj.GetName()
			if !seen[key] {
				resources = append(resources, obj)
				seen[key] = true
			}
		}
	}
	models := map[string][]resolver.Route{}
	for _, route := range routes {
		models[route.Model] = append(models[route.Model], route)
	}
	modelNames := make([]string, 0, len(models))
	for modelName := range models {
		modelNames = append(modelNames, modelName)
	}
	sort.Strings(modelNames)
	for _, modelName := range modelNames {
		modelRoutes := models[modelName]
		sink := providerSelectionSink(modelRoutes[0], modelNamespace)
		if owner := modelOwners[modelRoutes[0].Model]; owner != nil {
			setOwnerReference(&sink, owner)
		}
		if key := sink.GetKind() + "/" + sink.GetNamespace() + "/" + sink.GetName(); !seen[key] {
			resources = append(resources, sink)
			seen[key] = true
		}
		obj := modelHTTPRouteSet(modelRoutes, modelNamespace, gatewayName, gatewayNamespace)
		if owner := modelOwners[modelRoutes[0].Model]; owner != nil {
			setOwnerReference(&obj, owner)
		}
		key := obj.GetKind() + "/" + obj.GetNamespace() + "/" + obj.GetName()
		if !seen[key] {
			resources = append(resources, obj)
		}
	}
	for _, resource := range resources {
		if r.ApplyResource != nil {
			if err := r.ApplyResource(ctx, r.Client, resource); err != nil {
				return err
			}
			continue
		}
		if err := render.Apply(ctx, r.Client, []unstructured.Unstructured{resource}); err != nil {
			return err
		}
	}
	if err := r.enableExternalModelRoutes(ctx, tenantID, modelNamespace, gatewayName, gatewayNamespace, routes); err != nil {
		return err
	}
	return nil
}

// enableExternalModelRoutes owns a separate EnvoyFilter containing only the
// route-specific ExternalModel filter overrides. The tenant reconciler owns
// the base EnvoyFilter; keeping these objects separate prevents concurrent
// tenant/model reconciles from overwriting configPatches.
func (r *Reconciler) enableExternalModelRoutes(ctx context.Context, tenantID, modelNamespace, gatewayName, gatewayNamespace string, routes []resolver.Route) error {
	name := tenant.PayloadProcessingExternalModelEnvoyFilterName(tenantID)
	if len(routes) == 0 {
		return r.deleteExternalModelRouteFilters(ctx, name, modelNamespace)
	}
	filter := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "networking.istio.io/v1alpha3",
		"kind":       "EnvoyFilter",
		"metadata": map[string]any{
			"name":      name,
			"namespace": gatewayNamespace,
			"labels": map[string]any{
				"app.kubernetes.io/managed-by": "ai-gateway-controller",
				externalTenantLabel:            modelNamespace,
				externalTenantIDLabel:          ownershipTenantID(tenantID),
				externalGatewayNameLabel:       gatewayName,
				externalGatewayNamespaceLabel:  gatewayNamespace,
			},
		},
		"spec": map[string]any{
			"priority": int64(20),
			"workloadSelector": map[string]any{
				"labels": map[string]any{
					"gateway.networking.k8s.io/gateway-name": gatewayName,
				},
			},
		},
	}}
	kept := make([]any, 0, len(routes)*2+2)

	byModel := map[string]int{}
	for _, route := range routes {
		byModel[route.Model]++
	}
	models := make([]string, 0, len(byModel))
	for model := range byModel {
		models = append(models, model)
	}
	sort.Strings(models)
	for _, model := range models {
		// modelHTTPRouteSet emits two provider rules per route plus the
		// canonical and header-only fail-closed sink rules.
		count := byModel[model]*2 + 2
		for index := 0; index < count; index++ {
			kept = append(kept, map[string]any{
				"applyTo": "HTTP_ROUTE",
				"match": map[string]any{
					"context": "GATEWAY",
					"routeConfiguration": map[string]any{"vhost": map[string]any{
						"route": map[string]any{
							"name": fmt.Sprintf("%s.%s.%d", modelNamespace, modelRouteName(model), index),
						},
					}},
				},
				"patch": map[string]any{
					"operation": "MERGE",
					"value": map[string]any{"typed_per_filter_config": map[string]any{
						externalModelPreExtProcFilter: map[string]any{
							"@type": "type.googleapis.com/envoy.extensions.filters.http.ext_proc.v3.ExtProcPerRoute",
							"overrides": map[string]any{
								"processing_mode": map[string]any{
									"request_header_mode":   "SEND",
									"request_body_mode":     "BUFFERED",
									"response_header_mode":  "SKIP",
									"response_body_mode":    "NONE",
									"request_trailer_mode":  "SKIP",
									"response_trailer_mode": "SKIP",
								},
							},
						},
						externalModelExtProcFilter: map[string]any{
							"@type": "type.googleapis.com/envoy.extensions.filters.http.ext_proc.v3.ExtProcPerRoute",
							// ExtProcPerRoute is disable-only when the
							// vhost config is disabled. An overrides
							// object is the supported way for this
							// more-specific ExternalModel route to
							// re-enable the filter.
							"overrides": map[string]any{
								"processing_mode": map[string]any{
									"request_header_mode": "SEND",
									// Praxis treats the omitted NONE enum as BUFFERED; STREAMED runs selection at headers.
									"request_body_mode":     "STREAMED",
									"response_header_mode":  "SEND",
									"response_body_mode":    "NONE",
									"request_trailer_mode":  "SKIP",
									"response_trailer_mode": "SKIP",
								},
							},
						},
						"envoy.filters.http.ext_proc.ipp-pre": map[string]any{
							"@type":    "type.googleapis.com/envoy.extensions.filters.http.ext_proc.v3.ExtProcPerRoute",
							"disabled": true,
						},
						"envoy.filters.http.ext_proc.ipp": map[string]any{
							"@type":    "type.googleapis.com/envoy.extensions.filters.http.ext_proc.v3.ExtProcPerRoute",
							"disabled": true,
						},
					}},
				},
			})
		}
	}
	if err := unstructured.SetNestedSlice(filter.Object, kept, "spec", "configPatches"); err != nil {
		return fmt.Errorf("write ExternalModel EnvoyFilter route patches: %w", err)
	}
	if r.ApplyResource != nil {
		return r.ApplyResource(ctx, r.Client, *filter)
	}
	return render.Apply(ctx, r.Client, []unstructured.Unstructured{*filter})
}

// deleteExternalModelRouteFilters removes only the controller-owned route
// filter with the deterministic tenant name. Listing cluster-wide is
// intentional: during an AITenant handoff its status.gatewayRef may already
// be empty or may point at a new Gateway while the old filter still exists.
// The tenant namespace label, exact name, and managed-by label are the
// ownership guard.
func (r *Reconciler) deleteExternalModelRouteFilters(ctx context.Context, name, modelNamespace string) error {
	filters := &unstructured.UnstructuredList{}
	filters.SetGroupVersionKind(schema.GroupVersionKind{Group: "networking.istio.io", Version: "v1alpha3", Kind: "EnvoyFilterList"})
	if err := r.List(ctx, filters, client.MatchingLabels{"app.kubernetes.io/managed-by": "ai-gateway-controller", externalTenantLabel: modelNamespace}); err != nil {
		return fmt.Errorf("list controller-owned ExternalModel route EnvoyFilters: %w", err)
	}
	for i := range filters.Items {
		filter := &filters.Items[i]
		if filter.GetName() != name || filter.GetLabels()["app.kubernetes.io/managed-by"] != "ai-gateway-controller" || filter.GetLabels()[externalTenantLabel] != modelNamespace {
			continue
		}
		if err := client.IgnoreNotFound(r.Delete(ctx, filter)); err != nil {
			return fmt.Errorf("delete ExternalModel route EnvoyFilter %s/%s: %w", filter.GetNamespace(), filter.GetName(), err)
		}
	}
	return nil
}

// deleteExternalModelRouteFiltersForTenant removes every controller-owned
// route filter proven to belong to the model namespace. It is used for
// switch-away and ownerless final-model deletion, where the AITenant name or
// current Gateway may already be gone.
func (r *Reconciler) deleteExternalModelRouteFiltersForTenant(ctx context.Context, modelNamespace string) error {
	filters := &unstructured.UnstructuredList{}
	filters.SetGroupVersionKind(schema.GroupVersionKind{Group: "networking.istio.io", Version: "v1alpha3", Kind: "EnvoyFilterList"})
	if err := r.List(ctx, filters, client.MatchingLabels{"app.kubernetes.io/managed-by": "ai-gateway-controller", externalTenantLabel: modelNamespace}); err != nil {
		return fmt.Errorf("list controller-owned ExternalModel route EnvoyFilters for tenant cleanup: %w", err)
	}
	for i := range filters.Items {
		filter := &filters.Items[i]
		if err := client.IgnoreNotFound(r.Delete(ctx, filter)); err != nil {
			return fmt.Errorf("delete ExternalModel route EnvoyFilter %s/%s: %w", filter.GetNamespace(), filter.GetName(), err)
		}
	}
	return nil
}

// deleteLegacyExternalModelRouteFilter removes the pre-#105 route filter only
// when the tenant identity is known. Before the ownership labels were added,
// the deterministic name and controller managed-by label are the remaining
// ownership proof. The exact-name check is mandatory: never sweep filters by
// managed-by alone, because a shared Gateway namespace may contain other
// tenants or foreign resources.
func (r *Reconciler) deleteLegacyExternalModelRouteFilter(ctx context.Context, tenantID string) error {
	name := tenant.PayloadProcessingExternalModelEnvoyFilterName(tenantID)
	filters := &unstructured.UnstructuredList{}
	filters.SetGroupVersionKind(schema.GroupVersionKind{Group: "networking.istio.io", Version: "v1alpha3", Kind: "EnvoyFilterList"})
	if err := r.List(ctx, filters, client.MatchingLabels{"app.kubernetes.io/managed-by": "ai-gateway-controller"}); err != nil {
		return fmt.Errorf("list legacy ExternalModel route EnvoyFilters: %w", err)
	}
	for i := range filters.Items {
		filter := &filters.Items[i]
		labels := filter.GetLabels()
		if filter.GetName() != name || labels["app.kubernetes.io/managed-by"] != "ai-gateway-controller" {
			continue
		}
		if _, labeledForTenant := labels[externalTenantLabel]; labeledForTenant {
			continue
		}
		if err := client.IgnoreNotFound(r.Delete(ctx, filter)); err != nil {
			return fmt.Errorf("delete legacy ExternalModel route EnvoyFilter %s/%s: %w", filter.GetNamespace(), filter.GetName(), err)
		}
	}
	return nil
}

// cleanupExternalModelRouteFilters removes stale Gateway-local copies after
// the new copy has been applied. The deterministic name plus tenant label is
// the ownership proof; Gateway labels decide which copy is current.
func (r *Reconciler) cleanupExternalModelRouteFilters(ctx context.Context, modelNamespace, tenantID, gatewayName, gatewayNamespace string) error {
	filters := &unstructured.UnstructuredList{}
	filters.SetGroupVersionKind(schema.GroupVersionKind{Group: "networking.istio.io", Version: "v1alpha3", Kind: "EnvoyFilterList"})
	if err := r.List(ctx, filters, client.MatchingLabels{"app.kubernetes.io/managed-by": "ai-gateway-controller", externalTenantLabel: modelNamespace}); err != nil {
		return fmt.Errorf("list stale ExternalModel route EnvoyFilters: %w", err)
	}
	wantName := tenant.PayloadProcessingExternalModelEnvoyFilterName(tenantID)
	for i := range filters.Items {
		filter := &filters.Items[i]
		labels := filter.GetLabels()
		if filter.GetName() != wantName || labels[externalTenantLabel] != modelNamespace {
			continue
		}
		if labels[externalGatewayNameLabel] == gatewayName && labels[externalGatewayNamespaceLabel] == gatewayNamespace && filter.GetNamespace() == gatewayNamespace {
			continue
		}
		if err := client.IgnoreNotFound(r.Delete(ctx, filter)); err != nil {
			return fmt.Errorf("delete stale ExternalModel route EnvoyFilter %s/%s: %w", filter.GetNamespace(), filter.GetName(), err)
		}
	}
	return r.deleteLegacyExternalModelRouteFilter(ctx, tenantID)
}

func setOwnerReference(obj *unstructured.Unstructured, owner client.Object) {
	uid := owner.GetUID()
	if uid == "" {
		return
	}
	controller := true
	blockOwnerDeletion := true
	apiVersion, kind := v1alpha1.GroupVersion.String(), "ExternalModel"
	if _, ok := owner.(*v1alpha1.ExternalProvider); ok {
		kind = "ExternalProvider"
	}
	obj.SetOwnerReferences([]metav1.OwnerReference{{
		APIVersion: apiVersion,
		Kind:       kind,
		Name:       owner.GetName(), UID: uid,
		Controller: &controller, BlockOwnerDeletion: &blockOwnerDeletion,
	}})
}

func (r *Reconciler) updateProviderStatus(ctx context.Context, p *v1alpha1.ExternalProvider, ready bool, reason, message string) error {
	old := p.DeepCopy()
	p.Status.ObservedGeneration = p.Generation
	if ready {
		p.Status.Phase = resolver.PhaseReady
	} else {
		p.Status.Phase = "Failed"
	}
	setCondition(&p.Status.Conditions, conditionReady, boolStatus(ready), reason, message)
	if equality.Semantic.DeepEqual(old.Status, p.Status) {
		return nil
	}
	if err := r.Status().Patch(ctx, p, client.MergeFrom(old)); err != nil {
		if !apierrors.IsConflict(err) {
			r.Log.Error(err, "update provider status", "provider", p.Name)
		}
		return fmt.Errorf("update provider status %s/%s: %w", p.Namespace, p.Name, err)
	}
	return nil
}

func (r *Reconciler) updateModelStatus(ctx context.Context, m *v1alpha1.ExternalModel, ready bool, reason, message string, env *envelope.Envelope) error {
	old := m.DeepCopy()
	m.Status.ObservedGeneration = m.Generation
	if ready {
		m.Status.Phase = resolver.PhaseReady
		m.Status.HTTPRouteName = modelRouteName(m.Name)
		if env != nil {
			m.Status.OverlayDigest = env.Revision.Value
			m.Status.OverlayGeneration = env.Provenance.SourceGeneration
			setCondition(&m.Status.Conditions, conditionOverlayDistributed, metav1.ConditionTrue, reasonReconciled, message)
		}
	} else {
		m.Status.Phase = "Failed"
		setCondition(&m.Status.Conditions, conditionOverlayDistributed, metav1.ConditionFalse, reason, message)
	}
	setCondition(&m.Status.Conditions, conditionReady, boolStatus(ready), reason, message)
	if equality.Semantic.DeepEqual(old.Status, m.Status) {
		return nil
	}
	if err := r.Status().Patch(ctx, m, client.MergeFrom(old)); err != nil {
		if !apierrors.IsConflict(err) {
			r.Log.Error(err, "update model status", "model", m.Name)
		}
		return fmt.Errorf("update model status %s/%s: %w", m.Namespace, m.Name, err)
	}
	return nil
}

func setCondition(conditions *[]metav1.Condition, typ string, status metav1.ConditionStatus, reason, message string) {
	apiMeta.SetStatusCondition(conditions, metav1.Condition{Type: typ, Status: status, Reason: reason, Message: message})
}

func boolStatus(ready bool) metav1.ConditionStatus {
	if ready {
		return metav1.ConditionTrue
	}
	return metav1.ConditionFalse
}
func modelRouteName(model string) string {
	return modelRoutePrefix + strings.ToLower(strings.ReplaceAll(model, "/", "-"))
}
func (r *Reconciler) gatewayName() string {
	if r.GatewayName != "" {
		return r.GatewayName
	}
	return "maas-default-gateway"
}
func (r *Reconciler) gatewayNamespace() string {
	if r.GatewayNamespace != "" {
		return r.GatewayNamespace
	}
	return r.Namespace
}
func (r *Reconciler) configMapName() string {
	if r.ConfigMap != "" {
		return r.ConfigMap
	}
	return publisher.DefaultName
}
func (r *Reconciler) localSite() string {
	if r.LocalSite != "" {
		return r.LocalSite
	}
	return "local"
}
func metadata(name, namespace string) map[string]any {
	return map[string]any{"name": name, "namespace": namespace, "labels": map[string]any{"app.kubernetes.io/managed-by": "ai-gateway-controller"}}
}

func labelledMetadata(name, namespace, key, value string) map[string]any {
	m := metadata(name, namespace)
	labels := map[string]any{"app.kubernetes.io/managed-by": "ai-gateway-controller", key: value}
	m["labels"] = labels
	return m
}

func providerService(route resolver.Route, ns string) unstructured.Unstructured {
	endpoint := providerEndpointForRoute(route)
	return unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1", "kind": "Service",
		"metadata": labelledMetadata(providerServicePrefix+route.Provider, ns, "inference.opendatahub.io/external-provider", route.Provider),
		"spec": map[string]any{
			"type": "ExternalName", "externalName": endpoint.host,
			"ports": []any{map[string]any{"name": "https", "port": endpoint.port, "targetPort": endpoint.port}},
		},
	}}
}
func providerServiceEntry(route resolver.Route, ns string) unstructured.Unstructured {
	endpoint := providerEndpointForRoute(route)
	return unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "networking.istio.io/v1", "kind": "ServiceEntry",
		"metadata": labelledMetadata(providerServicePrefix+route.Provider, ns, "inference.opendatahub.io/external-provider", route.Provider),
		"spec": map[string]any{
			"hosts": []any{endpoint.host}, "location": "MESH_EXTERNAL", "resolution": "DNS",
			"ports": []any{map[string]any{"name": "https", "number": endpoint.port, "protocol": "HTTPS"}},
		},
	}}
}
func providerDestinationRule(route resolver.Route, ns string) unstructured.Unstructured {
	return providerDestinationRuleForTenant(route, ns, "")
}

// providerDestinationRuleForTenant names Gateway-local provider policies per
// tenant. Multiple tenant reconcilers share a Gateway namespace, so the
// provider-only name is insufficient: a later reconcile could otherwise
// overwrite another tenant's endpoint/SNI policy. Preserve the historical
// default-tenant name for compatibility with existing installations.
func providerDestinationRuleForTenant(route resolver.Route, ns, tenantID string) unstructured.Unstructured {
	return providerDestinationRuleAtGateway(route, "", ns, tenantID)
}

func providerDestinationRuleAtGateway(route resolver.Route, gatewayName, ns, tenantID string) unstructured.Unstructured {
	endpoint := providerEndpointForRoute(route)
	tls := map[string]any{"mode": "SIMPLE", "sni": endpoint.host}
	if route.TLSCACertificates != "" {
		tls["caCertificates"] = route.TLSCACertificates
	}
	name := providerServicePrefix + route.Provider
	if tenantID != "" && tenantID != "models-as-a-service" {
		name += "-" + tenantID
	}
	labels := map[string]any{
		"app.kubernetes.io/managed-by":               "ai-gateway-controller",
		"inference.opendatahub.io/external-provider": route.Provider,
	}
	labels[externalTenantLabel] = route.Namespace
	labels[externalTenantIDLabel] = ownershipTenantID(tenantID)
	labels[externalGatewayNameLabel] = gatewayName
	labels[externalGatewayNamespaceLabel] = ns
	return unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "networking.istio.io/v1", "kind": "DestinationRule",
		"metadata": map[string]any{"name": name, "namespace": ns, "labels": labels},
		"spec": map[string]any{
			// The Gateway workload may be in a different namespace from the
			// tenant-owned transport resources. Export the rule explicitly so
			// Envoy receives the provider TLS/SNI policy across that boundary.
			"exportTo":      []any{"*"},
			"host":          endpoint.host,
			"trafficPolicy": map[string]any{"tls": tls},
		},
	}}
}

func providerPort(route resolver.Route) int64 {
	return providerEndpointForRoute(route).port
}

func providerEndpointForRoute(route resolver.Route) providerEndpoint {
	endpoint, err := parseProviderEndpoint(route.Endpoint)
	if err != nil {
		// Reconciliation validates provider endpoints before rendering. This
		// defensive value keeps render-only unit fixtures with an omitted
		// endpoint structurally inspectable without creating a new fallback in
		// the live reconcile path.
		return providerEndpoint{authority: route.Endpoint, host: route.Endpoint, port: 443}
	}
	return endpoint
}

func modelHTTPRoute(route resolver.Route, ns, gateway, gatewayNS string) unstructured.Unstructured {
	return modelHTTPRouteSet([]resolver.Route{route}, ns, gateway, gatewayNS)
}

func modelHTTPRouteSet(routes []resolver.Route, ns, gateway, gatewayNS string) unstructured.Unstructured {
	if len(routes) == 0 {
		return unstructured.Unstructured{}
	}
	route := routes[0]
	parent := map[string]any{"name": gateway}
	if gatewayNS != "" && gatewayNS != ns {
		parent["namespace"] = gatewayNS
	}
	path := "/" + ns + "/" + route.ClientName
	rules := make([]any, 0, len(routes)*2+2)
	seenProviders := map[string]bool{}
	for _, candidate := range routes {
		if seenProviders[candidate.Provider] {
			continue
		}
		seenProviders[candidate.Provider] = true
		stableID := envelope.CandidateStableID(route.Model, candidate.Provider)
		rules = append(rules, map[string]any{
			"matches": []any{map[string]any{
				"path":    map[string]any{"type": "PathPrefix", "value": path},
				"headers": []any{map[string]any{"name": selectedProviderHeader, "type": "Exact", "value": stableID}},
			}},
			"backendRefs": []any{map[string]any{"name": providerServicePrefix + candidate.Provider, "port": providerPort(candidate)}},
			"filters":     []any{providerURLRewrite(providerEndpointForRoute(candidate)), removeInternalRoutingHeaders()},
			"timeouts":    map[string]any{"request": "300s"},
		})
		// Body-routed requests enter on a model header rather than the
		// canonical path. The selected-provider header is still authoritative
		// after post-auth ExtProc clears the route cache.
		rules = append(rules, map[string]any{
			"matches": []any{map[string]any{"headers": []any{
				map[string]any{"name": "X-Gateway-Model-Name", "type": "Exact", "value": route.ClientName},
				map[string]any{"name": selectedProviderHeader, "type": "Exact", "value": stableID},
			}}},
			"backendRefs": []any{map[string]any{"name": providerServicePrefix + candidate.Provider, "port": providerPort(candidate)}},
			"filters":     []any{providerHostnameRewrite(providerEndpointForRoute(candidate)), removeInternalRoutingHeaders()},
			"timeouts":    map[string]any{"request": "300s"},
		})
	}
	sinkName := providerSelectionSinkName(route.Model)
	rules = append(rules, map[string]any{
		"matches":     []any{map[string]any{"path": map[string]any{"type": "PathPrefix", "value": path}}},
		"backendRefs": []any{map[string]any{"name": sinkName, "port": int64(443)}},
		"timeouts":    map[string]any{"request": "300s"},
	})
	// Keep the body-routing rule for requests whose path is not the canonical
	// model path. The post-auth ExtProc selection header is authoritative for
	// provider choice and the route-cache clear causes Envoy to reselect one of
	// the header rules above.
	rules = append(rules, map[string]any{
		"matches":     []any{map[string]any{"headers": []any{map[string]any{"name": "X-Gateway-Model-Name", "type": "Exact", "value": route.ClientName}}}},
		"backendRefs": []any{map[string]any{"name": sinkName, "port": int64(443)}},
		"timeouts":    map[string]any{"request": "300s"},
	})
	return unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "gateway.networking.k8s.io/v1", "kind": "HTTPRoute",
		"metadata": labelledMetadata(modelRouteName(route.Model), ns, "inference.opendatahub.io/external-model", route.Model),
		"spec": map[string]any{
			"parentRefs": []any{parent},
			"rules":      rules,
		},
	}}
}

func providerURLRewrite(endpoint providerEndpoint) map[string]any {
	return map[string]any{"type": "URLRewrite", "urlRewrite": map[string]any{
		"hostname": endpoint.host,
		"path":     map[string]any{"type": "ReplacePrefixMatch", "replacePrefixMatch": "/"},
	}}
}

func providerHostnameRewrite(endpoint providerEndpoint) map[string]any {
	return map[string]any{"type": "URLRewrite", "urlRewrite": map[string]any{"hostname": endpoint.host}}
}

func removeInternalRoutingHeaders() map[string]any {
	return map[string]any{"type": "RequestHeaderModifier", "requestHeaderModifier": map[string]any{
		"remove": []any{"x-ai-routing-candidate", "x-ai-routing-request-id", "x-ai-routing-revision"},
	}}
}

func providerSelectionSinkName(model string) string {
	return providerSelectionSinkPrefix + strings.ToLower(strings.ReplaceAll(model, "/", "-"))
}

func providerSelectionSink(route resolver.Route, namespace string) unstructured.Unstructured {
	return unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1", "kind": "Service",
		"metadata": labelledMetadata(providerSelectionSinkName(route.Model), namespace, "inference.opendatahub.io/external-model", route.Model),
		"spec": map[string]any{
			"ports": []any{map[string]any{"name": "https", "port": int64(443), "targetPort": int64(443)}},
		},
	}}
}

var _ client.Object = (*v1alpha1.ExternalModel)(nil)
var _ client.Object = (*v1alpha1.ExternalProvider)(nil)
