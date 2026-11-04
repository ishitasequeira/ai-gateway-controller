// Package controller contains the ExternalModel and ExternalProvider control loop.
package controller

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

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
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
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
	reasonValidated               = "Validated"
	reasonReconcileFailed         = "ReconcileFailed"
	reasonNotPraxis               = "NotPraxisTenant"
	reasonTenantNotReady          = "TenantNotReady"
	reasonNoRoutes                = "NoRoutes"
	reasonProviderNotReady        = "ProviderNotReady"
	providerPhasePending          = "Pending"
	providerPhaseFailed           = "Failed"
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
	externalProviderFinalizer     = "inference.opendatahub.io/external-provider-cleanup"
)

var errCredentialNotReady = errors.New("effective provider credential is not ready")

// errProviderCANotReady reports a tls.credentialName Secret that is missing or
// has no CA, without saying which (see validateProviderCACredential).
var errProviderCANotReady = errors.New("tls.credentialName must name a Secret in the Gateway namespace with a PEM CA certificate in key cacert or ca.crt")

// Reconciler coordinates Praxis ExternalModel routes and ExternalProvider
// transport. The model loop resolves one namespace-scoped route set and
// renders the serving resources and routing overlay from that result; the
// provider loop provisions provider transport even when no models exist.
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
	// readPodLogs verifies that each current ExternalModel ExtProc process has
	// accepted and is serving the published overlay revision before withdrawal
	// cleanup begins. Tests inject deterministic process-reported revisions.
	readPodLogs func(context.Context, string, string, string) (string, error)
	// ApplyResource and PublishOverlay are test seams. Production leaves them
	// nil, selecting the real SSA renderer and publisher below. Applying one
	// object at a time preserves the production order and lets tests inject a
	// failure at each SSA boundary without production-only flags.
	ApplyResource  func(context.Context, client.Client, unstructured.Unstructured) error
	PublishOverlay func(context.Context, *resolver.ResolvedRouteSet, envelope.Scope, envelope.Options) (publisher.Result, error)
}

// SetupWithManager registers the ExternalModel and ExternalProvider loops,
// along with their shared tenant and Secret watches. Provider and Secret
// changes fan out to models that reference the provider, in any namespace, and
// ReferenceGrant changes to models referencing a provider in their namespace;
// Secret and tenant changes also reconcile providers independently of any model.
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.readPodLogs == nil {
		kubeClient, err := kubernetes.NewForConfig(mgr.GetConfig())
		if err != nil {
			return fmt.Errorf("create Kubernetes client for ExternalModel serving-state checks: %w", err)
		}
		r.readPodLogs = func(ctx context.Context, namespace, podName, container string) (string, error) {
			ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()
			tailLines := int64(300)
			limitBytes := int64(1 << 20)
			body, err := kubeClient.CoreV1().Pods(namespace).GetLogs(podName, &corev1.PodLogOptions{
				Container:  container,
				TailLines:  &tailLines,
				LimitBytes: &limitBytes,
			}).Do(ctx).Raw()
			return string(body), err
		}
	}
	inScope := predicate.NewPredicateFuncs(func(obj client.Object) bool { return r.namespaceAllowed(obj.GetNamespace()) })
	models := builder.ControllerManagedBy(mgr).
		For(&v1alpha1.ExternalModel{}, builder.WithPredicates(inScope)).
		Watches(&v1alpha1.ExternalProvider{}, handler.EnqueueRequestsFromMapFunc(r.providerModels), builder.WithPredicates(inScope)).
		WatchesMetadata(&corev1.Secret{}, handler.EnqueueRequestsFromMapFunc(r.secretModels), builder.WithPredicates(inScope)).
		// The tenant reconciler creates the tenant-local Praxis Service. Service events are namespace-scoped and therefore only affect
		// models in the affected tenant.
		Watches(&corev1.Service{}, handler.EnqueueRequestsFromMapFunc(r.serviceModels), builder.WithPredicates(inScope))
	// Model routes enable the post-auth filter only while the tenant has
	// post-auth filters, so re-check them when one of those inputs changes.
	for _, src := range tenant.PostAuthFilterSources() {
		models = models.Watches(src.Object, r.postAuthFilterHandler(src), builder.WithPredicates(src.Predicate))
	}
	if err := models.
		WatchesRawSource(source.Kind(mgr.GetCache(), tenant.NewMaasTenantConfig(), handler.TypedEnqueueRequestsFromMapFunc[*unstructured.Unstructured, reconcile.Request](r.maasTenantConfigModels))).
		WatchesRawSource(source.Kind(mgr.GetCache(), tenant.NewAITenant(), handler.TypedEnqueueRequestsFromMapFunc[*unstructured.Unstructured, reconcile.Request](r.tenantModels))).
		// Gateway attachments: route acceptance is reported in HTTPRoute
		// status, and a model-name change can decide a conflict on another
		// namespace's attachment.
		WatchesRawSource(source.Kind(mgr.GetCache(), newHTTPRoute(), handler.TypedEnqueueRequestsFromMapFunc[*unstructured.Unstructured, reconcile.Request](r.attachmentRouteModels))).
		Watches(&v1alpha1.ExternalModel{}, handler.EnqueueRequestsFromMapFunc(r.sameNameModels), builder.WithPredicates(inScope)).
		WatchesRawSource(source.Kind(mgr.GetCache(), newReferenceGrant(), handler.TypedEnqueueRequestsFromMapFunc[*unstructured.Unstructured, reconcile.Request](r.referenceGrantModels))).
		Complete(r); err != nil {
		return err
	}
	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &v1alpha1.ExternalProvider{}, providerCredentialNameIndex, indexProviderByCredentialName); err != nil {
		return fmt.Errorf("index ExternalProviders by tls.credentialName: %w", err)
	}
	return builder.ControllerManagedBy(mgr).
		For(&v1alpha1.ExternalProvider{}, builder.WithPredicates(inScope)).
		// Unfiltered: a tls.credentialName Secret is in the Gateway namespace,
		// which can be outside the scope; secretProviders applies the scope.
		WatchesMetadata(&corev1.Secret{}, handler.EnqueueRequestsFromMapFunc(r.secretProviders)).
		WatchesRawSource(source.Kind(mgr.GetCache(), tenant.NewMaasTenantConfig(), handler.TypedEnqueueRequestsFromMapFunc[*unstructured.Unstructured, reconcile.Request](r.tenantProviders))).
		WatchesRawSource(source.Kind(mgr.GetCache(), tenant.NewAITenant(), handler.TypedEnqueueRequestsFromMapFunc[*unstructured.Unstructured, reconcile.Request](r.aitenantProviders))).
		Complete(&providerReconciler{Reconciler: r})
}

type providerReconciler struct{ *Reconciler }

func (r *providerReconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	return r.ReconcileProvider(ctx, req)
}

// providerCredentialNameIndex indexes ExternalProviders by the Secret names
// Istio reads for tls.credentialName, so a CA Secret event in a Gateway
// namespace finds providers in any tenant.
const providerCredentialNameIndex = "spec.config.tls.credentialName" //nolint:gosec // field index name, not a credential

// istioCASuffix names the separate CA Secret Istio prefers for a credential.
const istioCASuffix = "-cacert"

func indexProviderByCredentialName(obj client.Object) []string {
	provider, ok := obj.(*v1alpha1.ExternalProvider)
	if !ok {
		return nil
	}
	if name := strings.TrimSpace(provider.Spec.Config["tls.credentialName"]); name != "" {
		return []string{name, name + istioCASuffix}
	}
	return nil
}

func (r *Reconciler) secretProviders(ctx context.Context, obj client.Object) []reconcile.Request {
	var requests []reconcile.Request
	if r.namespaceAllowed(obj.GetNamespace()) {
		var providers v1alpha1.ExternalProviderList
		if err := r.List(ctx, &providers, client.InNamespace(obj.GetNamespace())); err != nil {
			r.Log.Error(err, "list providers for Secret event", "secret", obj.GetName())
			return nil
		}
		for i := range providers.Items {
			if providers.Items[i].Spec.Auth.SecretRef.Name == obj.GetName() {
				requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&providers.Items[i])})
			}
		}
	}
	// The name alone matches: which Gateway namespace a provider uses is
	// decided by its tenant, and a redundant reconcile is harmless.
	var trusting v1alpha1.ExternalProviderList
	if err := r.List(ctx, &trusting, client.MatchingFields{providerCredentialNameIndex: obj.GetName()}); err != nil {
		r.Log.Error(err, "list providers for CA Secret event", "secret", obj.GetName())
		return requests
	}
	for i := range trusting.Items {
		if r.namespaceAllowed(trusting.Items[i].Namespace) {
			requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&trusting.Items[i])})
		}
	}
	return requests
}

func (r *Reconciler) tenantProviders(ctx context.Context, mtc *unstructured.Unstructured) []reconcile.Request {
	requests := r.providersInNamespace(ctx, mtc.GetNamespace())
	if name, _, ok := tenant.OwningAITenantRef(mtc); ok && name == tenant.DefaultAITenantName {
		requests = append(requests, r.providersOutsideTenants(ctx)...)
	}
	return requests
}

func (r *Reconciler) aitenantProviders(ctx context.Context, ait *unstructured.Unstructured) []reconcile.Request {
	ns, _, _ := unstructured.NestedString(ait.Object, "status", "tenantNamespace")
	if ns == "" {
		ns = ait.GetNamespace()
	}
	requests := r.providersInNamespace(ctx, ns)
	if ait.GetName() == tenant.DefaultAITenantName {
		requests = append(requests, r.providersOutsideTenants(ctx)...)
	}
	return requests
}

// providersOutsideTenants enqueues the providers of namespaces without a
// MaasTenantConfig. Whether their status is reported follows the default
// tenant, which serves their models through implicit attachments.
func (r *Reconciler) providersOutsideTenants(ctx context.Context) []reconcile.Request {
	if r.Namespace != "" {
		return nil
	}
	configs := &unstructured.UnstructuredList{}
	configs.SetGroupVersionKind(tenant.MaasTenantConfigGVK.GroupVersion().WithKind("MaasTenantConfigList"))
	if err := r.List(ctx, configs); err != nil {
		r.Log.Error(err, "list MaasTenantConfigs for default tenant event")
		return nil
	}
	tenantNamespaces := make(map[string]bool, len(configs.Items))
	for i := range configs.Items {
		tenantNamespaces[configs.Items[i].GetNamespace()] = true
	}
	var providers v1alpha1.ExternalProviderList
	if err := r.List(ctx, &providers); err != nil {
		r.Log.Error(err, "list providers for default tenant event")
		return nil
	}
	var requests []reconcile.Request
	for i := range providers.Items {
		if ns := providers.Items[i].Namespace; r.namespaceAllowed(ns) && !tenantNamespaces[ns] {
			requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&providers.Items[i])})
		}
	}
	return requests
}

func (r *Reconciler) providersInNamespace(ctx context.Context, namespace string) []reconcile.Request {
	if !r.namespaceAllowed(namespace) {
		return nil
	}
	var providers v1alpha1.ExternalProviderList
	if err := r.List(ctx, &providers, client.InNamespace(namespace)); err != nil {
		r.Log.Error(err, "list providers for tenant event", "namespace", namespace)
		return nil
	}
	requests := make([]reconcile.Request, 0, len(providers.Items))
	for i := range providers.Items {
		requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&providers.Items[i])})
	}
	return requests
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
	requests := r.modelsReferencingProviders(ctx, obj.GetNamespace(), providerNames)
	// Attachments and cross-namespace references check a provider's CA Secret
	// in their own Gateway's namespace, so its events reach their models.
	var trusting v1alpha1.ExternalProviderList
	if err := r.List(ctx, &trusting, client.MatchingFields{providerCredentialNameIndex: obj.GetName()}); err != nil {
		r.Log.Error(err, "list providers for CA Secret event", "secret", obj.GetName())
		return requests
	}
	for i := range trusting.Items {
		requests = append(requests, r.modelsReferencingProviders(ctx, trusting.Items[i].Namespace, map[string]bool{trusting.Items[i].Name: true})...)
	}
	// Models re-validate an auth override Secret, and credential copies
	// follow it, whether or not they attach to other tenants.
	var models v1alpha1.ExternalModelList
	if err := r.List(ctx, &models, client.InNamespace(obj.GetNamespace())); err != nil {
		r.Log.Error(err, "list models for Secret event", "secret", obj.GetName())
		return requests
	}
	for i := range models.Items {
		if slices.ContainsFunc(models.Items[i].Spec.ExternalProviderRefs, func(ref v1alpha1.ExternalProviderRef) bool {
			return ref.Auth != nil && ref.Auth.SecretRef.Name == obj.GetName()
		}) {
			requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&models.Items[i])})
		}
	}
	return requests
}

func (r *Reconciler) serviceModels(ctx context.Context, obj client.Object) []reconcile.Request {
	if !r.namespaceAllowed(obj.GetNamespace()) {
		return nil
	}
	return r.modelsInNamespace(ctx, obj.GetNamespace())
}

// postAuthFilterRetryDelays are the waits between retries when the tenant
// lookup fails temporarily. The event won't fire again, so without retries
// models attached to the tenant's Gateway would be missed.
var postAuthFilterRetryDelays = []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second}

// postAuthFilterHandler queues the models of each tenant affected by a
// change. It holds the queue so it can retry later.
//
//nolint:ireturn // controller-runtime watches take a handler.EventHandler.
func (r *Reconciler) postAuthFilterHandler(src tenant.PostAuthFilterSource) handler.EventHandler {
	enqueue := func(ctx context.Context, q workqueue.TypedRateLimitingInterface[reconcile.Request], objects ...client.Object) {
		for _, obj := range objects {
			for _, namespace := range src.Tenants(ctx, r.Client, obj) {
				r.enqueuePostAuthFilterModels(ctx, q, namespace, 0)
			}
		}
	}
	return handler.Funcs{
		CreateFunc: func(ctx context.Context, e event.CreateEvent, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
			enqueue(ctx, q, e.Object)
		},
		UpdateFunc: func(ctx context.Context, e event.UpdateEvent, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
			enqueue(ctx, q, e.ObjectOld, e.ObjectNew)
		},
		DeleteFunc: func(ctx context.Context, e event.DeleteEvent, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
			enqueue(ctx, q, e.Object)
		},
		GenericFunc: func(ctx context.Context, e event.GenericEvent, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
			enqueue(ctx, q, e.Object)
		},
	}
}

// enqueuePostAuthFilterModels queues the tenant's models and retries if the
// tenant lookup failed temporarily. Other failures are only logged; fixing
// the MaasTenantConfig or AITenant triggers a new pass.
func (r *Reconciler) enqueuePostAuthFilterModels(ctx context.Context, q workqueue.TypedRateLimitingInterface[reconcile.Request], namespace string, attempt int) {
	if ctx.Err() != nil || q.ShuttingDown() {
		return
	}
	requests, err := r.postAuthFilterModels(ctx, namespace)
	for _, request := range requests {
		q.Add(request)
	}
	if err == nil {
		return
	}
	if !transientReadError(err) || attempt >= len(postAuthFilterRetryDelays) {
		r.Log.Error(err, "resolve tenant for post-auth filter event; gateway-attached models were not enqueued",
			"namespace", namespace, "attempts", attempt+1)
		return
	}
	time.AfterFunc(postAuthFilterRetryDelays[attempt], func() {
		r.enqueuePostAuthFilterModels(ctx, q, namespace, attempt+1)
	})
}

// postAuthFilterModels returns the tenant's models: those in its namespace
// and those attached to its Gateway. If the tenant lookup fails, it still
// returns the ones in its namespace, along with the error.
func (r *Reconciler) postAuthFilterModels(ctx context.Context, tenantNamespace string) ([]reconcile.Request, error) {
	if !r.namespaceAllowed(tenantNamespace) {
		return nil, nil
	}
	requests := r.modelsInNamespace(ctx, tenantNamespace)
	resolution, err := r.resolveTenantForNamespace(ctx, tenantNamespace)
	if err != nil {
		return requests, err
	}
	if resolution.aitenant == nil {
		return requests, nil
	}
	seen := make(map[types.NamespacedName]struct{}, len(requests))
	for _, request := range requests {
		seen[request.NamespacedName] = struct{}{}
	}
	for _, request := range r.gatewayAttachedModels(ctx, resolution.aitenant) {
		if _, ok := seen[request.NamespacedName]; ok {
			continue
		}
		seen[request.NamespacedName] = struct{}{}
		requests = append(requests, request)
	}
	return requests, nil
}

func (r *Reconciler) tenantModels(ctx context.Context, ait *unstructured.Unstructured) []reconcile.Request {
	ns, _, _ := unstructured.NestedString(ait.Object, "status", "tenantNamespace")
	if ns == "" {
		ns = ait.GetNamespace()
	}
	if !r.namespaceAllowed(ns) {
		return nil
	}
	return append(r.modelsInNamespace(ctx, ns), r.gatewayAttachedModels(ctx, ait)...)
}

func (r *Reconciler) maasTenantConfigModels(ctx context.Context, mtc *unstructured.Unstructured) []reconcile.Request {
	if mtc.GetName() != tenant.MaasTenantConfigInstanceName || !r.namespaceAllowed(mtc.GetNamespace()) {
		return nil
	}
	requests := r.modelsInNamespace(ctx, mtc.GetNamespace())
	if name, namespace, ok := tenant.OwningAITenantRef(mtc); ok {
		ait := tenant.NewAITenant()
		if err := r.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, ait); err == nil {
			requests = append(requests, r.gatewayAttachedModels(ctx, ait)...)
		}
	}
	return requests
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

// modelsReferencingProviders enqueues the models, in any namespace, that
// reference the named providers of the namespace.
func (r *Reconciler) modelsReferencingProviders(ctx context.Context, namespace string, providers map[string]bool) []reconcile.Request {
	if len(providers) == 0 || !r.namespaceAllowed(namespace) {
		return nil
	}
	var models v1alpha1.ExternalModelList
	if err := r.List(ctx, &models); err != nil {
		r.Log.Error(err, "list models for provider event", "namespace", namespace)
		return nil
	}
	requests := make([]reconcile.Request, 0, len(models.Items))
	for i := range models.Items {
		m := &models.Items[i]
		if r.namespaceAllowed(m.Namespace) && slices.ContainsFunc(m.Spec.ExternalProviderRefs, func(ref v1alpha1.ExternalProviderRef) bool {
			return (ref.Ref.Namespace == namespace || (ref.Ref.Namespace == "" && m.Namespace == namespace)) && providers[ref.Ref.Name]
		}) {
			requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(m)})
		}
	}
	return requests
}

// providerInputs returns only providers whose persisted status covers their
// current spec; the resolver routes only the Ready ones. ExternalProvider
// reconciliation is the sole writer of that status and of provider transport
// resources. A provider only Validated outside tenant namespaces has no
// tenant-local transport.
func (r *Reconciler) providerInputs(ctx context.Context, namespace string) ([]*v1alpha1.ExternalProvider, error) {
	var providers v1alpha1.ExternalProviderList
	if err := r.List(ctx, &providers, client.InNamespace(namespace)); err != nil {
		return nil, err
	}
	providerPtrs := make([]*v1alpha1.ExternalProvider, 0, len(providers.Items))
	for i := range providers.Items {
		p := &providers.Items[i]
		ready := apiMeta.FindStatusCondition(p.Status.Conditions, conditionReady)
		if !p.DeletionTimestamp.IsZero() || p.Status.ObservedGeneration != p.Generation ||
			p.Status.Phase == resolver.PhaseReady && ready != nil && ready.Reason == reasonValidated {
			continue
		}
		// A current status that is not Ready is kept: the resolver skips the
		// provider and reports that status, rather than a missing provider.
		providerPtrs = append(providerPtrs, p)
	}
	return providerPtrs, nil
}

// ReconcileProvider owns provider-level transport and persisted readiness. It
// deliberately does not require an ExternalModel: provider creation and
// credential rotation must be observable before any model references it.
func (r *Reconciler) ReconcileProvider(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	if !r.namespaceAllowed(req.Namespace) {
		return reconcile.Result{}, nil
	}
	var provider v1alpha1.ExternalProvider
	if err := r.Get(ctx, req.NamespacedName, &provider); err != nil {
		if apierrors.IsNotFound(err) {
			// Clean resources left by a provider that predated the provider
			// finalizer. All cleanup remains label-gated to this provider.
			inUse, useErr := r.providerTransportStillInUse(ctx, req.Namespace, req.Name)
			if useErr != nil {
				return reconcile.Result{}, useErr
			}
			if inUse {
				return reconcile.Result{RequeueAfter: withdrawalRequeueInterval}, nil
			}
			return reconcile.Result{}, r.cleanupProviderTransport(ctx, req.Namespace, req.Name)
		}
		return reconcile.Result{}, err
	}
	if !provider.DeletionTimestamp.IsZero() {
		return r.reconcileDeletingProvider(ctx, &provider)
	}

	live, err := r.liveTenantResolution(ctx, provider.Namespace)
	if err != nil {
		// Tenant resolution fails on an unreadable or inconsistent AITenant
		// owner. Claim the provider and report Pending before returning: a bare
		// error would leave it with neither our finalizer nor a status writer,
		// while IPP remains free to write status for this namespace.
		if finalizerErr := r.ensureExternalProviderFinalizer(ctx, &provider); finalizerErr != nil {
			return reconcile.Result{}, errors.Join(err, finalizerErr)
		}
		if statusErr := r.updateProviderStatus(ctx, &provider, providerPhasePending, reasonTenantNotReady, err.Error()); statusErr != nil {
			return reconcile.Result{}, errors.Join(err, statusErr)
		}
		return reconcile.Result{}, err
	}
	// An explicit IPP selection is a handoff command. Remove only resources
	// owned by Praxis and release our finalizer; IPP remains the status owner.
	// Attachments from this namespace still serve Praxis tenants through the
	// provider Service and ServiceEntry; removing the attachment collects them.
	// Routes are read live and applied before their transport, so a route
	// created a moment ago is never missed.
	if !praxisSelected(live) {
		routes, err := r.listAttachmentRoutes(ctx, r.APIReader, client.InNamespace(provider.Namespace))
		if err != nil {
			return reconcile.Result{}, err
		}
		if err := r.cleanupProviderTransportExcept(ctx, provider.Namespace, provider.Name, routeBackends(routes)); err != nil {
			return reconcile.Result{}, err
		}
		if controllerutil.ContainsFinalizer(&provider, externalProviderFinalizer) {
			return reconcile.Result{}, r.removeExternalProviderFinalizer(ctx, &provider)
		}
		return reconcile.Result{}, nil
	}
	if !live.missingConfig && live.status == "" {
		// Praxis is selected but the handoff has not started (see
		// tenant.LegacyServes): IPP still serves this namespace and owns
		// provider status until maas-controller writes cleanup-complete.
		return reconcile.Result{}, nil
	}
	validationErr := r.validateProvider(ctx, &provider)
	if validationErr == nil {
		validationErr = r.validateProviderCredential(ctx, &provider)
	}
	if live.missingConfig {
		// A namespace no tenant claims is served only through Gateway
		// attachments: its provider has no tenant-local transport, so only its
		// own configuration is reported. While IPP serves the default tenant,
		// it serves this namespace's models and reports their providers'
		// status instead. Every tenant Gateway is in the configured Gateway
		// namespace, where its CA Secret must be. A tenant namespace whose
		// MaasTenantConfig maas-controller has not created yet falls through
		// and is claimed below.
		tenantNamespace, err := r.tenantNamespace(ctx, provider.Namespace)
		if err != nil {
			return reconcile.Result{}, err
		}
		if !tenantNamespace {
			return r.reconcileAttachmentOnlyProvider(ctx, &provider, validationErr)
		}
	}
	// Claim the provider before any tenant gate: an absent MaasTenantConfig is
	// Praxis-default, and IPP must not write status for a provider we own.
	if err := r.ensureExternalProviderFinalizer(ctx, &provider); err != nil {
		return reconcile.Result{}, err
	}
	if message := providerTenantReadinessMessage(live); message != "" {
		return reconcile.Result{}, r.updateProviderStatus(ctx, &provider, providerPhasePending, reasonTenantNotReady, message)
	}
	gatewayName, gatewayNamespace, gatewayReady := tenant.GatewayRef(live.aitenant)
	if !gatewayReady {
		return reconcile.Result{}, r.updateProviderStatus(ctx, &provider, providerPhasePending, reasonTenantNotReady, "AITenant Gateway reference is not ready")
	}
	want := modelTenantContext{
		aitenantUID: live.aitenant.GetUID(), mtcUID: live.mtc.GetUID(),
		gatewayName: gatewayName, gatewayNamespace: gatewayNamespace,
		apiTranslationEnabled: live.apiTranslationEnabled,
	}

	if validationErr == nil {
		validationErr = r.validateProviderCACredential(ctx, &provider, gatewayNamespace)
		if transientReadError(validationErr) {
			// A failed Secret read is not the provider's own failure: retry
			// instead of withdrawing its transport and reporting it Failed.
			return reconcile.Result{}, validationErr
		}
	}
	if validationErr != nil {
		cleanupErr := r.cleanupProviderTransport(ctx, provider.Namespace, provider.Name)
		if statusErr := r.updateProviderStatus(ctx, &provider, providerPhaseFailed, reasonReconcileFailed, validationErr.Error()); statusErr != nil {
			return reconcile.Result{}, statusErr
		}
		if cleanupErr != nil {
			return reconcile.Result{}, cleanupErr
		}
		return reconcile.Result{}, nil
	}

	valid, err := r.livePraxisBeforeWrite(ctx, provider.Namespace, want)
	if err != nil {
		return reconcile.Result{}, err
	}
	if !valid {
		return reconcile.Result{Requeue: true}, nil
	}
	err = r.applyProviderTransport(ctx, &provider, tenant.ID(live.aitenant.GetName()), gatewayName, gatewayNamespace)
	if err == nil {
		err = r.cleanupStaleProviderDestinationRules(ctx, &provider, gatewayName, gatewayNamespace)
	}
	if err != nil {
		cleanupErr := r.cleanupProviderTransport(ctx, provider.Namespace, provider.Name)
		if statusErr := r.updateProviderStatus(ctx, &provider, providerPhaseFailed, reasonReconcileFailed, err.Error()); statusErr != nil {
			return reconcile.Result{}, statusErr
		}
		if cleanupErr != nil {
			return reconcile.Result{}, fmt.Errorf("%w; clean up partially applied provider transport: %w", err, cleanupErr)
		}
		return reconcile.Result{}, err
	}
	if err := r.updateProviderStatus(ctx, &provider, resolver.PhaseReady, reasonReconciled, "provider Secret reference and transport resources are ready"); err != nil {
		return reconcile.Result{}, err
	}
	return reconcile.Result{}, nil
}

// reconcileAttachmentOnlyProvider reports the status of a provider in a
// namespace no tenant claims, given the result of its own validation, and
// releases its deletion protection once no attachment uses it.
func (r *Reconciler) reconcileAttachmentOnlyProvider(ctx context.Context, provider *v1alpha1.ExternalProvider, validationErr error) (reconcile.Result, error) {
	if err := r.releaseUnusedAttachmentProvider(ctx, provider); err != nil {
		return reconcile.Result{}, err
	}
	if _, _, ipp, err := r.defaultTenantGateway(ctx); err != nil || ipp {
		return reconcile.Result{}, err
	}
	if validationErr == nil {
		validationErr = r.validateProviderCACredential(ctx, provider, r.gatewayNamespace())
		if transientReadError(validationErr) {
			// A failed Secret read is not the provider's own failure: retry
			// instead of reporting it Failed.
			return reconcile.Result{}, validationErr
		}
	}
	result := reconcile.Result{}
	if controllerutil.ContainsFinalizer(provider, externalProviderFinalizer) {
		result.RequeueAfter = 30 * time.Second
	}
	if validationErr != nil {
		return result, r.updateProviderStatus(ctx, provider, providerPhaseFailed, reasonReconcileFailed, validationErr.Error())
	}
	return result, r.updateProviderStatus(ctx, provider, resolver.PhaseReady, reasonValidated, "provider Secret reference is ready; Gateway attachments apply its transport")
}

func (r *Reconciler) ensureExternalProviderFinalizer(ctx context.Context, provider *v1alpha1.ExternalProvider) error {
	if controllerutil.ContainsFinalizer(provider, externalProviderFinalizer) {
		return nil
	}
	base := provider.DeepCopy()
	controllerutil.AddFinalizer(provider, externalProviderFinalizer)
	if err := r.Patch(ctx, provider, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{})); err != nil {
		return fmt.Errorf("add ExternalProvider cleanup finalizer: %w", err)
	}
	return nil
}

// Reconcile applies model routes, then the routing overlay. The overlay is
// never published when an earlier stage fails.
func (r *Reconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	if !r.namespaceAllowed(req.Namespace) {
		return reconcile.Result{}, nil
	}
	var model v1alpha1.ExternalModel
	if err := r.Get(ctx, req.NamespacedName, &model); err != nil {
		if apierrors.IsNotFound(err) {
			// A deleted model's attachment routes are garbage collected with
			// it, and each route deletion requeues it here.
			return reconcile.Result{}, errors.Join(r.collectDeletedAttachments(ctx), r.collectProviderTransport(ctx, req.Namespace))
		}
		return reconcile.Result{}, err
	}
	// Deletion must not depend on MaasTenantConfig or its AITenant still being
	// present. Those objects can disappear first during a tenant teardown.
	// The deletion path uses the ExternalModel namespace and controller-owned
	// resource labels as its independent ownership proof.
	if !model.DeletionTimestamp.IsZero() {
		if controllerutil.ContainsFinalizer(&model, attachmentFinalizer) {
			// Extra attachments remove only their own objects: they never
			// wait for a tenant or rebuild its shared state.
			if err := r.releaseAttachments(ctx, &model); err != nil {
				return reconcile.Result{}, err
			}
			if !controllerutil.ContainsFinalizer(&model, externalModelFinalizer) {
				// No home attachment to clean up.
				return reconcile.Result{}, nil
			}
		}
		if err := r.reconcileDeletingModel(ctx, &model); err != nil {
			var pending *pendingWithdrawalError
			if errors.As(err, &pending) {
				return reconcile.Result{RequeueAfter: pending.After}, nil
			}
			return reconcile.Result{}, err
		}
		return reconcile.Result{}, nil
	}
	result, err := r.reconcileModel(ctx, req, &model)
	var pending *pendingWithdrawalError
	if errors.As(err, &pending) {
		return reconcile.Result{RequeueAfter: pending.After}, nil
	}
	// Provider transport is collected once the routes that used it are withdrawn.
	if collectErr := r.collectProviderTransport(ctx, model.Namespace); collectErr != nil {
		err = errors.Join(err, collectErr)
	}
	return result, err
}

func (r *Reconciler) reconcileModel(ctx context.Context, req reconcile.Request, model *v1alpha1.ExternalModel) (reconcile.Result, error) {
	gateways, err := r.modelGateways(ctx, model)
	if err != nil {
		return reconcile.Result{}, err
	}
	if len(gateways) > 0 {
		return r.reconcileAttachedModel(ctx, model, gateways)
	}
	// With every gatewayRef removed, or an implicit attachment replaced by a
	// MaasTenantConfig in the namespace, left to IPP or without a default
	// tenant Gateway, no attachment route, attachment finalizer or per-gateway
	// result remains.
	if err := r.releaseAttachments(ctx, model); err != nil {
		return reconcile.Result{}, err
	}
	if err := r.clearGatewayStatus(ctx, model); err != nil {
		return reconcile.Result{}, err
	}
	return r.reconcileHomeModel(ctx, req, model, func(ready bool, reason, message string, env *envelope.Envelope) error {
		return r.updateModelStatus(ctx, model, ready, reason, message, env)
	})
}

// modelStatusReporter receives the home path's model status. Models with
// gatewayRefs aggregate it with their other attachments into one write.
type modelStatusReporter func(ready bool, reason, message string, env *envelope.Envelope) error

type homeModelPublication struct {
	result            publisher.Result
	namespace         string
	gateway           v1alpha1.NamespacedObjectReference
	denied            string
	unserved          string
	weightDisabled    bool
	unroutedReason    string
	unroutedMessage   string
	attached          []attachedModel
	state             map[string]attachedModel
	remaining         time.Duration
	withdrawalPending bool
	report            modelStatusReporter
}

// reconcileHomeModel serves the model through its own tenant: the tenant
// whose MaasTenantConfig is in the model namespace. This is the existing
// tenant-local path; it publishes every model the tenant serves from its own
// namespace.
func (r *Reconciler) reconcileHomeModel(ctx context.Context, req reconcile.Request, model *v1alpha1.ExternalModel, report modelStatusReporter) (reconcile.Result, error) {
	modelTenant, proceed, err := r.prepareModelTenant(ctx, model, req.Namespace, report)
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
	sortModelsByName(models.Items)
	providerPtrs, err := r.providerInputs(ctx, req.Namespace)
	if err != nil {
		return reconcile.Result{}, err
	}

	modelPtrs, modelOwners := homeModelInputs(models.Items, gatewayName, gatewayNamespace)
	foreign, unready, err := r.foreignProviders(ctx, req.Namespace, gatewayNamespace, modelPtrs)
	if err != nil {
		return reconcile.Result{}, err
	}
	set, err := resolver.ResolveWithAPITranslation(modelPtrs, append(providerPtrs, foreign...), modelTenant.apiTranslationEnabled)
	set, unserved, err := skipUnreadyCredentials(set, err, unready)
	denied := deniedReferences(set)
	if errors.Is(err, resolver.ErrNoRoutes) {
		// A served reference that is no longer permitted must stop serving,
		// so its last serving state is not retained.
		if revoked, servesErr := r.servesDeniedReference(ctx, req.Namespace, modelPtrs, foreign); servesErr != nil {
			return reconcile.Result{}, servesErr
		} else if revoked {
			err = nil
		}
		// An explicit all-zero weight update is an administrative withdrawal,
		// not a provider-readiness failure. Let the no-route cleanup below
		// remove the previous selection instead of preserving it as LKG.
		if errors.Is(err, resolver.ErrNoRoutes) && allRoutesExplicitlyWeightDisabled(set) {
			err = nil
		}
	}
	if err != nil {
		return r.handleHomeRouteResolutionError(ctx, req.Namespace, modelTenant, set, model.Name, string(model.UID), err, report)
	}
	set, err = r.validateModelRouteSet(ctx, set, modelOwners, model, report)
	if err != nil {
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
	home := attachmentTenant{namespace: req.Namespace, tenantID: tenant.ID(modelTenant.aitenant.GetName()),
		gateway:               v1alpha1.NamespacedObjectReference{Name: gatewayName, Namespace: gatewayNamespace},
		apiTranslationEnabled: modelTenant.apiTranslationEnabled}
	if len(set.Routes()) == 0 {
		// Attachments from other namespaces keep the tenant's overlay and
		// route filter; without one the existing cleanup applies.
		if served, err := r.keepTenantAttachments(ctx, home, string(model.UID)); err != nil {
			return reconcile.Result{}, err
		} else if !served {
			if _, err := r.enableExternalModelRoutes(ctx, home, nil); err != nil {
				return reconcile.Result{}, err
			}
			if err := r.deleteLegacyExternalModelRouteFilter(ctx, tenant.ID(modelTenant.aitenant.GetName())); err != nil {
				return reconcile.Result{}, err
			}
			if cleanupErr := r.cleanupModelTransport(ctx, req.Namespace, nil); cleanupErr != nil {
				return reconcile.Result{}, cleanupErr
			}
			if cleanupErr := r.cleanupOverlay(ctx, req.Namespace); cleanupErr != nil {
				return reconcile.Result{}, cleanupErr
			}
		}
		reason, message := reasonNoRoutes, "no ExternalModel provider references resolved to a Ready provider"
		if denial := denied[req.String()]; denial != "" {
			reason, message = reasonRefNotPermitted, denial
		}
		if statusErr := report(false, reason, message, nil); statusErr != nil {
			return reconcile.Result{}, statusErr
		}
		return reconcile.Result{}, resolver.ErrNoRoutes
	}
	state := foreignState(req.Namespace, set)
	attached, err := r.admittedAttachments(ctx, home, len(set.Routes()))
	if err != nil {
		return reconcile.Result{}, err
	}
	previousOverlay, err := r.readPublishedEnvelope(ctx, req.Namespace)
	if err != nil {
		return reconcile.Result{}, err
	}
	transportRoutes, withdrawalPending, err := r.transportRoutesForPublication(ctx, req.Namespace, set.Routes(), previousOverlay, modelTenant.apiTranslationEnabled)
	if err != nil {
		return reconcile.Result{}, err
	}
	if err := r.applyTransport(ctx, transportRoutes, req.Namespace, tenant.ID(modelTenant.aitenant.GetName()), gatewayName, gatewayNamespace, modelOwners, attached, state, modelTenant.apiTranslationEnabled); err != nil {
		if statusErr := report(false, reasonReconcileFailed, err.Error(), nil); statusErr != nil {
			return reconcile.Result{}, statusErr
		}
		return reconcile.Result{}, err
	}
	if err := r.cleanupExternalModelRouteFilters(ctx, req.Namespace, tenant.ID(modelTenant.aitenant.GetName()), gatewayName, gatewayNamespace); err != nil {
		return reconcile.Result{}, err
	}
	if err := r.cleanupModelTransport(ctx, req.Namespace, transportRoutes); err != nil {
		if statusErr := report(false, reasonReconcileFailed, err.Error(), nil); statusErr != nil {
			return reconcile.Result{}, statusErr
		}
		return reconcile.Result{}, err
	}
	result, err := r.publishTenantOverlay(ctx, req.Namespace, gatewayName, set, attached, nil, string(model.UID), modelTenant.apiTranslationEnabled)
	if err != nil {
		if statusErr := report(false, reasonReconcileFailed, err.Error(), nil); statusErr != nil {
			return reconcile.Result{}, statusErr
		}
		return reconcile.Result{}, err
	}
	// Attachments the tenant's own candidates pushed over the overlay
	// capacity, and providers in other namespaces its own routes no longer
	// use, lose their DestinationRules and credential copies.
	if !withdrawalPending {
		if err := r.gcTenantState(ctx, req.Namespace, excludedRoutes(attached), stateAnnotations(home.gateway, state)); err != nil {
			return reconcile.Result{}, err
		}
	}
	remaining, err := r.finishProviderWithdrawal(ctx, req.Namespace, tenant.ID(modelTenant.aitenant.GetName()),
		gatewayName, gatewayNamespace, modelPtrs, modelOwners, set.Routes(), attached, result.Envelope.Revision.Value, withdrawalPending,
		modelTenant.apiTranslationEnabled, state)
	if err != nil {
		return reconcile.Result{}, err
	}
	unroutedReason, unroutedMessage, unrouted := unroutedStatus(set, req.String())
	if !unrouted {
		unroutedReason, unroutedMessage = "", ""
	}
	return r.finishHomeModelPublication(ctx, homeModelPublication{
		result: result, namespace: req.Namespace,
		gateway: home.gateway, denied: denied[req.String()],
		unserved:       unserved[req.String()],
		weightDisabled: explicitlyWeightDisabledModels(set)[req.String()],
		unroutedReason: unroutedReason, unroutedMessage: unroutedMessage,
		attached: attached, state: state,
		remaining: remaining, withdrawalPending: withdrawalPending, report: report,
	})
}

// homeModelInputs selects live models owned by this tenant without republishing
// terminating models whose staged withdrawal is handled independently.
func homeModelInputs(models []v1alpha1.ExternalModel, gatewayName, gatewayNamespace string) ([]*v1alpha1.ExternalModel, map[string]*v1alpha1.ExternalModel) {
	modelPtrs := make([]*v1alpha1.ExternalModel, 0, len(models))
	modelOwners := make(map[string]*v1alpha1.ExternalModel, len(models))
	for i := range models {
		// A terminating model is handled by its own staged-withdrawal
		// reconcile. Including it here lets a sibling event republish its
		// providers and replace the drain revision before its finalizer can
		// finish.
		if !models[i].DeletionTimestamp.IsZero() || !tenant.ServedByTenantGateway(&models[i], gatewayName, gatewayNamespace) {
			continue
		}
		modelPtrs = append(modelPtrs, &models[i])
		modelOwners[models[i].Name] = &models[i]
	}
	return modelPtrs, modelOwners
}

func (r *Reconciler) finishHomeModelPublication(ctx context.Context, publication homeModelPublication) (reconcile.Result, error) {
	var statusErr error
	switch {
	case publication.weightDisabled:
		statusErr = publication.report(false, reasonNoRoutes, "all ExternalModel provider references are explicitly disabled by weight", nil)
	case publication.denied != "":
		statusErr = publication.report(false, reasonRefNotPermitted, publication.denied, nil)
	case publication.unserved != "":
		statusErr = publication.report(false, reasonProviderNotReady, publication.unserved, nil)
	case publication.unroutedReason != "":
		statusErr = publication.report(false, publication.unroutedReason, publication.unroutedMessage, nil)
	default:
		message := fmt.Sprintf("distributed routing overlay digest %s generation %d", publication.result.Envelope.Revision.Value, publication.result.Envelope.Provenance.SourceGeneration)
		statusErr = publication.report(true, reasonReconciled, message, &publication.result.Envelope)
	}
	if publication.remaining > 0 {
		if statusErr != nil {
			return reconcile.Result{}, statusErr
		}
		return reconcile.Result{RequeueAfter: publication.remaining}, nil
	}
	if publication.withdrawalPending {
		if err := r.gcTenantState(ctx, publication.namespace, excludedRoutes(publication.attached), stateAnnotations(publication.gateway, publication.state)); err != nil {
			return reconcile.Result{}, err
		}
	}
	return reconcile.Result{}, statusErr
}

func (r *Reconciler) handleHomeRouteResolutionError(
	ctx context.Context,
	namespace string,
	modelTenant modelTenantContext,
	set *resolver.ResolvedRouteSet,
	modelName string,
	sourceUID string,
	routeErr error,
	report modelStatusReporter,
) (reconcile.Result, error) {
	reason := reasonReconcileFailed
	message := routeErr.Error()
	if errors.Is(routeErr, resolver.ErrNoRoutes) {
		reason = reasonProviderNotReady
		if unroutedReason, unroutedMessage, ok := unroutedStatus(set, namespace+"/"+modelName); ok {
			reason, message = unroutedReason, unroutedMessage
		}
		excluded := translationPolicyExcludedModels(set)
		disabledByWeight := explicitlyWeightDisabledModels(set)
		for modelRef := range disabledByWeight {
			excluded[modelRef] = true
		}
		if len(excluded) > 0 {
			if disabledByWeight[namespace+"/"+modelName] {
				reason = reasonNoRoutes
			} else if excluded[namespace+"/"+modelName] {
				reason = reasonTranslationDisabled
			}
			valid, err := r.livePraxisBeforeWrite(ctx, namespace, modelTenant)
			if err != nil {
				return reconcile.Result{}, err
			}
			if !valid {
				return reconcile.Result{Requeue: true}, nil
			}
			// Report the policy failure before cleanup so a cleanup error cannot
			// leave a previously Ready model status in place.
			if err := report(false, reason, message, nil); err != nil {
				return reconcile.Result{}, err
			}
			home := attachmentTenant{namespace: namespace, tenantID: tenant.ID(modelTenant.aitenant.GetName()),
				gateway:               v1alpha1.NamespacedObjectReference{Name: modelTenant.gatewayName, Namespace: modelTenant.gatewayNamespace},
				apiTranslationEnabled: modelTenant.apiTranslationEnabled}
			if err := r.cleanupExcludedHomeModels(ctx, home, set, excluded, sourceUID); err != nil {
				return reconcile.Result{}, err
			}
			return reconcile.Result{}, routeErr
		}
	}
	if err := report(false, reason, message, nil); err != nil {
		return reconcile.Result{}, err
	}
	return reconcile.Result{}, routeErr
}

// unroutedStatus explains a model the route set leaves with no route: every
// skipped provider reference with the resolver's message. ok is false when
// the model has a route, or when no reference was skipped.
func unroutedStatus(set *resolver.ResolvedRouteSet, modelRef string) (reason, message string, ok bool) {
	if set == nil {
		return "", "", false
	}
	i := slices.IndexFunc(set.Models, func(m resolver.ModelRoutes) bool { return m.ModelRef == modelRef })
	if i < 0 || len(set.Models[i].Routes) > 0 || len(set.Models[i].Skips) == 0 {
		return "", "", false
	}
	reason = reasonProviderNotReady
	skips := make([]string, 0, len(set.Models[i].Skips))
	for _, skip := range set.Models[i].Skips {
		skips = append(skips, fmt.Sprintf("provider %s: %s", skip.ProviderRef, skip.Message))
		if skip.Reason == resolver.SkipRefNotPermitted {
			reason = reasonRefNotPermitted
		}
	}
	if translationPolicyExcludedModels(set)[modelRef] {
		reason = reasonTranslationDisabled
	}
	return reason, "no routable ExternalModel provider remains: " + strings.Join(skips, "; "), true
}

type modelTenantContext struct {
	aitenant              *unstructured.Unstructured
	aitenantUID           types.UID
	mtcUID                types.UID
	gatewayName           string
	gatewayNamespace      string
	apiTranslationEnabled bool
}

// prepareModelTenant resolves the MaasTenantConfig selector and the owning
// AITenant before any ExternalModel serving state is published. A false
// proceed result means status/cleanup work was completed and the caller must
// stop this reconcile.
func (r *Reconciler) prepareModelTenant(ctx context.Context, model *v1alpha1.ExternalModel, namespace string, report modelStatusReporter) (modelTenantContext, bool, error) {
	// Re-read the selector, handoff status, deletion state, and owner from the
	// API server immediately before making the cleanup/apply decision. The
	// informer copy can be behind an IPP switch or an AITenant replacement.
	live, err := r.liveTenantResolution(ctx, namespace)
	if err != nil {
		return modelTenantContext{}, false, err
	}
	if live.missingConfig {
		if len(model.Spec.GatewayRefs) == 0 && !controllerutil.ContainsFinalizer(model, externalModelFinalizer) {
			// Never served here, nor through an implicit attachment to the
			// default tenant's Gateway (see implicitlyAttached).
			message := fmt.Sprintf("namespace %s has no MaasTenantConfig; set spec.gatewayRefs to attach this model to a tenant gateway", namespace)
			implicit, err := r.implicitlyAttached(ctx, model)
			if err != nil {
				return modelTenantContext{}, false, err
			}
			if implicit {
				_, ok, ipp, err := r.defaultTenantGateway(ctx)
				if err != nil || ipp {
					// The default tenant's IPP serves it and owns its status.
					return modelTenantContext{}, false, err
				}
				if !ok {
					message = fmt.Sprintf("namespace %s has no MaasTenantConfig and the default tenant (AITenant %s) has no resolvable Gateway; "+
						"set spec.gatewayRefs to attach this model to a tenant gateway", namespace, tenant.DefaultAITenantName)
				}
			}
			if err := r.setModelStatus(ctx, model, modelPhasePending, false, reasonTenantNotResolved, message, ""); err != nil {
				return modelTenantContext{}, false, err
			}
			return modelTenantContext{}, false, nil
		}
		if err := report(false, reasonTenantNotReady, "MaasTenantConfig/default-tenant is missing; retaining serving state and waiting for the owner to resolve", nil); err != nil {
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
	if live.status == "" && len(model.Spec.GatewayRefs) == 0 {
		// Praxis is selected but the handoff has not started (see
		// tenant.LegacyServes): IPP still serves the model and reports its
		// status. A Pending status here would compete with IPP's, and MaaS
		// treats a model that is not Ready as unhealthy.
		return modelTenantContext{}, false, nil
	}
	if live.aitenant == nil {
		if err := report(false, reasonTenantNotReady, "owning AITenant is not yet resolvable for MaasTenantConfig", nil); err != nil {
			return modelTenantContext{}, false, err
		}
		return modelTenantContext{}, false, nil
	}
	if live.status != tenant.PayloadProcessingStatusSteady {
		if err := report(false, reasonTenantNotReady, "MaasTenantConfig selects Praxis but payload-processing-status is not steady", nil); err != nil {
			return modelTenantContext{}, false, err
		}
		return modelTenantContext{}, false, nil
	}
	ait := live.aitenant
	gatewayName, gatewayNamespace, gatewayReady := tenant.GatewayRef(ait)
	if !gatewayReady {
		if err := report(false, reasonTenantNotReady, "AITenant Gateway reference is not ready", nil); err != nil {
			return modelTenantContext{}, false, err
		}
		return modelTenantContext{}, false, nil
	}
	// DeployReady (current Ready stamp, not Failed) — not Active. maas writes
	// gatewayRef before the exclusive gateway claim; Failed/GatewayClaimFailed
	// still publishes the contested ref. Pending with a current stamp is enough
	// so ExternalModel publish does not wait on Active↔MTC Ready.
	if !tenant.DeployReady(ait) {
		if err := report(false, reasonTenantNotReady, "AITenant is not ready for ExternalModel publish (Failed or status not current for this generation)", nil); err != nil {
			return modelTenantContext{}, false, err
		}
		return modelTenantContext{}, false, nil
	}
	if handled, err := r.handleModelLifecycle(ctx, model, ait); handled {
		return modelTenantContext{}, false, err
	} else if err != nil {
		return modelTenantContext{}, false, err
	}
	return modelTenantContext{
		aitenant: ait, aitenantUID: ait.GetUID(), mtcUID: live.mtc.GetUID(),
		gatewayName: gatewayName, gatewayNamespace: gatewayNamespace,
		apiTranslationEnabled: live.apiTranslationEnabled,
	}, true, nil
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
	if err := r.cleanupModelTransport(ctx, model.Namespace, nil); err != nil {
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
		live.status != tenant.PayloadProcessingStatusSteady {
		return r.reconcileDeletedModelWithoutOwner(ctx, model)
	}
	if _, _, ready := tenant.GatewayRef(live.aitenant); !ready || !tenant.DeployReady(live.aitenant) {
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
	if err := r.cleanupModelTransport(ctx, deleted.Namespace, nil); err != nil {
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
	if err := r.republishHomeWithout(ctx, deleted, tenantID, gatewayName, gatewayNamespace); err != nil {
		return err
	}
	return r.removeExternalModelFinalizer(ctx, deleted)
}

// republishHomeWithout republishes the tenant's own serving state without the
// given model: its deletion, or its gatewayRefs no longer naming its own
// tenant. Only the final served model may remove the shared serving state.
func (r *Reconciler) republishHomeWithout(ctx context.Context, deleted *v1alpha1.ExternalModel, tenantID, gatewayName, gatewayNamespace string) error {
	live, err := r.liveTenantResolution(ctx, deleted.Namespace)
	if err != nil {
		return err
	}
	if live.missingConfig || !live.selected || live.mtc == nil || live.aitenant == nil ||
		live.status != tenant.PayloadProcessingStatusSteady || !tenant.DeployReady(live.aitenant) {
		return errors.New("cannot republish remaining ExternalModels while the tenant handoff is not steady")
	}
	set, modelPtrs, modelOwners, foreign, err := r.resolveRemainingHomeModels(ctx, deleted, gatewayName, gatewayNamespace, live.apiTranslationEnabled)
	home := attachmentTenant{namespace: deleted.Namespace, tenantID: tenantID,
		gateway:               v1alpha1.NamespacedObjectReference{Name: gatewayName, Namespace: gatewayNamespace},
		apiTranslationEnabled: live.apiTranslationEnabled}
	if errors.Is(err, resolver.ErrNoRoutes) {
		// Preserve serving state during provider/readiness failures. Policy-excluded
		// candidates are not eligible to serve and may be removed safely, and a
		// served reference that is no longer permitted must stop serving.
		revoked, servesErr := r.servesDeniedReference(ctx, deleted.Namespace, modelPtrs, foreign)
		if servesErr != nil {
			return servesErr
		}
		if len(modelPtrs) > 0 && !allRoutesExcludedByPolicy(set) && !revoked {
			return err
		}
		if err := r.ensureHomeTenantBeforeWrite(ctx, deleted.Namespace, live, gatewayName, gatewayNamespace); err != nil {
			return err
		}
		if served, err := r.keepTenantAttachments(ctx, home, string(deleted.UID)); err != nil || served {
			return err
		}
		if err := r.cleanupHomeModelServingState(ctx, deleted.Namespace, tenantID); err != nil {
			return err
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("resolve remaining ExternalModels after deletion: %w", err)
	}
	set, rejected := excludeUnsupportedModelSelection(set)
	if len(set.Routes()) == 0 && len(rejected) > 0 {
		if err := r.ensureHomeTenantBeforeWrite(ctx, deleted.Namespace, live, gatewayName, gatewayNamespace); err != nil {
			return err
		}
		if served, err := r.keepTenantAttachments(ctx, home, string(deleted.UID)); err != nil || served {
			return err
		}
		if err := r.cleanupHomeModelServingState(ctx, deleted.Namespace, tenantID); err != nil {
			return err
		}
		return r.removeExternalModelFinalizer(ctx, deleted)
	}
	if err := r.validateResolvedRouteSet(ctx, set); err != nil {
		return fmt.Errorf("validate remaining ExternalModel routes after deletion: %w", err)
	}
	if err := r.ensureHomeTenantBeforeWrite(ctx, deleted.Namespace, live, gatewayName, gatewayNamespace); err != nil {
		return err
	}
	return r.applyRemainingHomeModels(ctx, deleted, tenantID, gatewayName, gatewayNamespace,
		live.apiTranslationEnabled, set, modelPtrs, modelOwners)
}

func homeModelTenantContext(live tenantResolution, gatewayName, gatewayNamespace string) modelTenantContext {
	return modelTenantContext{
		aitenantUID: live.aitenant.GetUID(), mtcUID: live.mtc.GetUID(),
		gatewayName: gatewayName, gatewayNamespace: gatewayNamespace,
		apiTranslationEnabled: live.apiTranslationEnabled,
	}
}

func (r *Reconciler) ensureHomeTenantBeforeWrite(
	ctx context.Context,
	namespace string,
	live tenantResolution,
	gatewayName, gatewayNamespace string,
) error {
	valid, err := r.livePraxisBeforeWrite(ctx, namespace, homeModelTenantContext(live, gatewayName, gatewayNamespace))
	if err != nil {
		return err
	}
	if !valid {
		return errors.New("tenant changed before remaining ExternalModel state could be published; retrying")
	}
	return nil
}

func (r *Reconciler) resolveRemainingHomeModels(
	ctx context.Context,
	deleted *v1alpha1.ExternalModel,
	gatewayName, gatewayNamespace string,
	apiTranslationEnabled bool,
) (*resolver.ResolvedRouteSet, []*v1alpha1.ExternalModel, map[string]*v1alpha1.ExternalModel, []*v1alpha1.ExternalProvider, error) {
	var models v1alpha1.ExternalModelList
	if err := r.List(ctx, &models, client.InNamespace(deleted.Namespace)); err != nil {
		return nil, nil, nil, nil, fmt.Errorf("list remaining ExternalModels for deletion: %w", err)
	}
	sortModelsByName(models.Items)
	modelPtrs := make([]*v1alpha1.ExternalModel, 0, len(models.Items))
	modelOwners := make(map[string]*v1alpha1.ExternalModel, len(models.Items))
	for i := range models.Items {
		m := &models.Items[i]
		if m.Name == deleted.Name || !m.DeletionTimestamp.IsZero() || !tenant.ServedByTenantGateway(m, gatewayName, gatewayNamespace) {
			continue
		}
		modelPtrs = append(modelPtrs, m)
		modelOwners[m.Name] = m
	}
	providerPtrs, err := r.providerInputs(ctx, deleted.Namespace)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	foreign, unready, err := r.foreignProviders(ctx, deleted.Namespace, gatewayNamespace, modelPtrs)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	set, err := resolver.ResolveWithAPITranslation(modelPtrs, append(providerPtrs, foreign...), apiTranslationEnabled)
	set, _, err = skipUnreadyCredentials(set, err, unready)
	return set, modelPtrs, modelOwners, foreign, err
}

func (r *Reconciler) cleanupHomeModelServingState(ctx context.Context, namespace, tenantID string) error {
	if err := r.deleteExternalModelRouteFiltersForTenant(ctx, namespace); err != nil {
		return err
	}
	if tenantID != "" {
		if err := r.deleteLegacyExternalModelRouteFilter(ctx, tenantID); err != nil {
			return err
		}
	}
	if err := r.cleanupModelTransport(ctx, namespace, nil); err != nil {
		return err
	}
	return r.cleanupOverlay(ctx, namespace)
}

// applyRemainingHomeModels reconciles the home tenant's remaining routes and
// admitted attachments after a model is deleted or moved away from the tenant.
func (r *Reconciler) applyRemainingHomeModels(
	ctx context.Context,
	deleted *v1alpha1.ExternalModel,
	tenantID, gatewayName, gatewayNamespace string,
	apiTranslationEnabled bool,
	set *resolver.ResolvedRouteSet,
	modelPtrs []*v1alpha1.ExternalModel,
	modelOwners map[string]*v1alpha1.ExternalModel,
) error {
	home := attachmentTenant{namespace: deleted.Namespace, tenantID: tenantID,
		gateway:               v1alpha1.NamespacedObjectReference{Name: gatewayName, Namespace: gatewayNamespace},
		apiTranslationEnabled: apiTranslationEnabled}
	state := foreignState(deleted.Namespace, set)
	attached, err := r.admittedAttachments(ctx, home, len(set.Routes()))
	if err != nil {
		return err
	}
	previousOverlay, err := r.readPublishedEnvelope(ctx, deleted.Namespace)
	if err != nil {
		return err
	}
	transportRoutes, withdrawalPending, err := r.transportRoutesForPublication(ctx, deleted.Namespace, set.Routes(), previousOverlay, apiTranslationEnabled)
	if err != nil {
		return err
	}
	if err := r.applyTransport(ctx, transportRoutes, deleted.Namespace, tenantID, gatewayName, gatewayNamespace, modelOwners, attached, state, apiTranslationEnabled); err != nil {
		return fmt.Errorf("rebuild transport after ExternalModel deletion: %w", err)
	}
	if err := r.cleanupExternalModelRouteFilters(ctx, deleted.Namespace, tenantID, gatewayName, gatewayNamespace); err != nil {
		return err
	}
	if err := r.deleteLegacyExternalModelRouteFilter(ctx, tenantID); err != nil {
		return err
	}
	if err := r.cleanupModelTransport(ctx, deleted.Namespace, transportRoutes); err != nil {
		return err
	}
	sourceUID := string(deleted.UID)
	if len(modelPtrs) > 0 {
		sourceUID = string(modelPtrs[0].UID)
	}
	result, err := r.publishTenantOverlay(ctx, deleted.Namespace, gatewayName, set, attached, nil, sourceUID, apiTranslationEnabled)
	if err != nil {
		return fmt.Errorf("publish remaining overlay after ExternalModel deletion: %w", err)
	}
	remaining, err := r.finishProviderWithdrawal(ctx, deleted.Namespace, tenantID, gatewayName, gatewayNamespace,
		modelPtrs, modelOwners, set.Routes(), attached, result.Envelope.Revision.Value, withdrawalPending,
		apiTranslationEnabled, state)
	if err != nil {
		return err
	}
	if remaining > 0 {
		return &pendingWithdrawalError{After: remaining}
	}
	return r.gcTenantState(ctx, deleted.Namespace, excludedRoutes(attached), stateAnnotations(home.gateway, state))
}

func allRoutesExcludedByPolicy(set *resolver.ResolvedRouteSet) bool {
	return set != nil && len(set.Models) > 0 && len(translationPolicyExcludedModels(set)) == len(set.Models)
}

func translationPolicyExcludedModels(set *resolver.ResolvedRouteSet) map[string]bool {
	excluded := make(map[string]bool)
	if set == nil {
		return excluded
	}
	for _, model := range set.Models {
		if len(model.Routes) > 0 || len(model.Skips) == 0 {
			continue
		}
		policyOnly := true
		for _, skip := range model.Skips {
			if skip.Reason != resolver.SkipTranslationDisabled {
				policyOnly = false
				break
			}
		}
		if policyOnly {
			excluded[model.ModelRef] = true
		}
	}
	return excluded
}

// cleanupExcludedHomeModels removes only candidates made ineligible by an
// explicit model-local policy, such as disabled API translation or an
// all-zero provider weight. Other models with provider/readiness errors keep
// their last-known route patches and overlay candidates.
func (r *Reconciler) cleanupExcludedHomeModels(
	ctx context.Context,
	home attachmentTenant,
	set *resolver.ResolvedRouteSet,
	excluded map[string]bool,
	sourceUID string,
) error {
	candidates, err := r.ownOverlayCandidates(ctx, home.namespace)
	if err != nil {
		return fmt.Errorf("read current overlay candidates before translation-policy cleanup: %w", err)
	}
	excludedScopedIDs, err := r.excludedScopedCandidateIDs(ctx, home.namespace, excluded)
	if err != nil {
		return err
	}
	keptCandidates := make([]envelope.Candidate, 0, len(candidates))
	for _, candidate := range candidates {
		policyExcluded := excludedScopedIDs[candidate.StableID]
		for modelRef := range excluded {
			modelNamespace, modelName, found := strings.Cut(modelRef, "/")
			if found && modelNamespace == home.namespace && strings.HasPrefix(candidate.StableID, modelName+"/provider-") {
				policyExcluded = true
				break
			}
		}
		if !policyExcluded {
			keptCandidates = append(keptCandidates, candidate)
		}
	}

	patches, err := r.ownRoutePatches(ctx, home)
	if err != nil {
		return fmt.Errorf("read current route patches before translation-policy cleanup: %w", err)
	}
	keptPatches := make([]any, 0, len(patches))
	for _, raw := range patches {
		patch, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		routeName, _, _ := unstructured.NestedString(patch, "match", "routeConfiguration", "vhost", "route", "name")
		policyExcluded := false
		for modelRef := range excluded {
			modelNamespace, modelName, found := strings.Cut(modelRef, "/")
			if !found || modelNamespace != home.namespace {
				continue
			}
			ruleIndex, matchesModel := strings.CutPrefix(routeName, modelNamespace+"."+modelRouteName(modelName)+".")
			if _, err := strconv.Atoi(ruleIndex); matchesModel && err == nil {
				policyExcluded = true
				break
			}
		}
		if !policyExcluded {
			keptPatches = append(keptPatches, raw)
		}
	}

	attachments, err := r.admittedAttachments(ctx, home, len(keptCandidates))
	if err != nil {
		return fmt.Errorf("resolve served attachments during translation-policy cleanup: %w", err)
	}
	if _, err := r.writeTenantExternalModelRoutes(ctx, home, set.Routes(), keptPatches, attachments, nil, nil); err != nil {
		return fmt.Errorf("remove translation-disabled model route patches: %w", err)
	}
	if err := r.cleanupExternalModelRouteFilters(ctx, home.namespace, home.tenantID, home.gateway.Name, home.gateway.Namespace); err != nil {
		return err
	}
	if err := r.deleteLegacyExternalModelRouteFilter(ctx, home.tenantID); err != nil {
		return err
	}

	keepRoutes := append([]resolver.Route(nil), set.Routes()...)
	for _, model := range set.Models {
		if excluded[model.ModelRef] {
			continue
		}
		modelNamespace, modelName, found := strings.Cut(model.ModelRef, "/")
		if found && modelNamespace == home.namespace && modelName != "" {
			keepRoutes = append(keepRoutes, resolver.Route{Model: modelName})
		}
	}
	if err := r.cleanupModelTransport(ctx, home.namespace, keepRoutes); err != nil {
		return err
	}
	if len(keptCandidates) == 0 && len(servedAttachments(attachments)) == 0 {
		if err := r.cleanupOverlay(ctx, home.namespace); err != nil {
			return err
		}
	} else if _, err := r.publishTenantOverlay(ctx, home.namespace, home.gateway.Name, nil, attachments, keptCandidates, sourceUID, home.apiTranslationEnabled); err != nil {
		return fmt.Errorf("publish overlay without translation-disabled candidates: %w", err)
	}
	if stale := excludedRoutes(attachments); len(stale) > 0 {
		return r.gcAttachmentState(ctx, home.namespace, stale)
	}
	return nil
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

func (r *Reconciler) cleanupModelTransport(ctx context.Context, namespace string, routes []resolver.Route) error {
	models := map[string]bool{}
	for _, route := range routes {
		models[route.Model] = true
	}
	// Attachment routes from this namespace use their models' selection sinks.
	sinks, err := r.attachedModelSinks(ctx, namespace)
	if err != nil {
		return err
	}
	maps.Copy(sinks, models)
	resources := []struct {
		kind, group, version, label string
		keep                        map[string]bool
	}{
		{"Service", "", "v1", "inference.opendatahub.io/external-model", sinks},
		{"HTTPRoute", "gateway.networking.k8s.io", "v1", "inference.opendatahub.io/external-model", models},
	}
	for _, resource := range resources {
		list := &unstructured.UnstructuredList{}
		list.SetGroupVersionKind(schema.GroupVersionKind{Group: resource.group, Version: resource.version, Kind: resource.kind + "List"})
		if err := r.List(ctx, list, client.InNamespace(namespace), client.MatchingLabels{"app.kubernetes.io/managed-by": "ai-gateway-controller"}); err != nil {
			return fmt.Errorf("list stale %s resources: %w", resource.kind, err)
		}
		for i := range list.Items {
			name := list.Items[i].GetLabels()[resource.label]
			if name != "" && !resource.keep[name] {
				if err := r.Delete(ctx, &list.Items[i]); client.IgnoreNotFound(err) != nil {
					return fmt.Errorf("delete stale %s %s/%s: %w", resource.kind, namespace, list.Items[i].GetName(), err)
				}
			}
		}
	}
	return nil
}

func (r *Reconciler) applyProviderTransport(ctx context.Context, provider *v1alpha1.ExternalProvider, tenantID, gatewayName, gatewayNamespace string) error {
	port, err := resolver.ProviderPort(provider)
	if err != nil {
		return fmt.Errorf("resolve provider %s transport port: %w", provider.Name, err)
	}
	route := resolver.Route{
		Namespace: provider.Namespace, Provider: provider.Name, ProviderType: provider.Spec.Provider,
		Endpoint: provider.Spec.Endpoint, Port: port, TLSCACertificates: strings.TrimSpace(provider.Spec.Config["tls.caCertificates"]),
		TLSCredentialName: strings.TrimSpace(provider.Spec.Config["tls.credentialName"]),
	}
	service := providerService(route, provider.Namespace)
	serviceEntry := providerServiceEntry(route, provider.Namespace)
	setOwnerReference(&service, provider)
	setOwnerReference(&serviceEntry, provider)
	resources := []unstructured.Unstructured{
		service,
		serviceEntry,
		providerDestinationRuleAtGateway(route, gatewayName, gatewayNamespace, tenantID),
	}
	if err := r.verifyProviderTransport(ctx, resources); err != nil {
		return fmt.Errorf("apply provider %s: %w", provider.Name, err)
	}
	for _, resource := range resources {
		if r.ApplyResource != nil {
			if err := r.ApplyResource(ctx, r.Client, resource); err != nil {
				return fmt.Errorf("apply provider %s %s/%s: %w", provider.Name, resource.GetKind(), resource.GetNamespace(), err)
			}
			continue
		}
		if err := render.Apply(ctx, r.Client, []unstructured.Unstructured{resource}); err != nil {
			return fmt.Errorf("apply provider %s %s/%s: %w", provider.Name, resource.GetKind(), resource.GetNamespace(), err)
		}
	}
	return nil
}

func (r *Reconciler) cleanupProviderTransport(ctx context.Context, namespace, providerName string) error {
	return r.cleanupProviderTransportExcept(ctx, namespace, providerName, nil)
}

// cleanupProviderTransportExcept keeps the provider Service and ServiceEntry
// named in used.
func (r *Reconciler) cleanupProviderTransportExcept(ctx context.Context, namespace, providerName string, used map[string]bool) error {
	for _, resource := range []struct {
		group, version, kind string
	}{
		{group: "", version: "v1", kind: "Service"},
		{group: "networking.istio.io", version: "v1", kind: "ServiceEntry"},
	} {
		list := &unstructured.UnstructuredList{}
		list.SetGroupVersionKind(schema.GroupVersionKind{Group: resource.group, Version: resource.version, Kind: resource.kind + "List"})
		if err := r.List(ctx, list, client.InNamespace(namespace), client.MatchingLabels{
			"app.kubernetes.io/managed-by":               "ai-gateway-controller",
			"inference.opendatahub.io/external-provider": providerName,
		}); err != nil {
			return fmt.Errorf("list provider %s %s resources for cleanup: %w", providerName, resource.kind, err)
		}
		for i := range list.Items {
			if used[list.Items[i].GetName()] {
				continue
			}
			if err := client.IgnoreNotFound(r.Delete(ctx, &list.Items[i])); err != nil {
				return fmt.Errorf("delete provider %s %s %s/%s: %w", providerName, resource.kind, namespace, list.Items[i].GetName(), err)
			}
		}
	}
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(schema.GroupVersionKind{Group: "networking.istio.io", Version: "v1", Kind: "DestinationRuleList"})
	if err := r.List(ctx, list, client.MatchingLabels{
		"app.kubernetes.io/managed-by":               "ai-gateway-controller",
		externalTenantLabel:                          namespace,
		"inference.opendatahub.io/external-provider": providerName,
	}); err != nil {
		return fmt.Errorf("list provider %s DestinationRules for cleanup: %w", providerName, err)
	}
	for i := range list.Items {
		if err := client.IgnoreNotFound(r.Delete(ctx, &list.Items[i])); err != nil {
			return fmt.Errorf("delete provider %s DestinationRule %s/%s: %w", providerName, list.Items[i].GetNamespace(), list.Items[i].GetName(), err)
		}
	}
	return nil
}

func (r *Reconciler) cleanupStaleProviderDestinationRules(ctx context.Context, provider *v1alpha1.ExternalProvider, gatewayName, gatewayNamespace string) error {
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(schema.GroupVersionKind{Group: "networking.istio.io", Version: "v1", Kind: "DestinationRuleList"})
	if err := r.List(ctx, list, client.MatchingLabels{
		"app.kubernetes.io/managed-by":               "ai-gateway-controller",
		externalTenantLabel:                          provider.Namespace,
		"inference.opendatahub.io/external-provider": provider.Name,
	}); err != nil {
		return fmt.Errorf("list stale provider DestinationRules: %w", err)
	}
	for i := range list.Items {
		obj := &list.Items[i]
		labels := obj.GetLabels()
		if labels[externalGatewayNameLabel] == gatewayName && labels[externalGatewayNamespaceLabel] == gatewayNamespace && obj.GetNamespace() == gatewayNamespace {
			continue
		}
		if err := client.IgnoreNotFound(r.Delete(ctx, obj)); err != nil {
			return fmt.Errorf("delete stale provider DestinationRule %s/%s: %w", obj.GetNamespace(), obj.GetName(), err)
		}
	}
	return nil
}

func (r *Reconciler) removeExternalProviderFinalizer(ctx context.Context, provider *v1alpha1.ExternalProvider) error {
	base := provider.DeepCopy()
	if !controllerutil.RemoveFinalizer(provider, externalProviderFinalizer) {
		return nil
	}
	if err := r.Patch(ctx, provider, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{})); err != nil {
		return fmt.Errorf("remove ExternalProvider cleanup finalizer: %w", err)
	}
	return nil
}

// cleanupOverlay removes only the routing ConfigMap owned by this controller
// when a namespace leaves Praxis selection. It is intentionally label-gated:
// the ConfigMap is shared by models in one tenant namespace, but resources in
// other namespaces and any non-controller ConfigMap are never touched.
// Without the overlay, the tenant's own routes need none of its
// DestinationRules and credential copies.
func (r *Reconciler) cleanupOverlay(ctx context.Context, namespace string) error {
	var cm corev1.ConfigMap
	key := client.ObjectKey{Namespace: namespace, Name: r.configMapName()}
	if err := r.Get(ctx, key, &cm); client.IgnoreNotFound(err) != nil {
		return err
	} else if err == nil && cm.Labels["app.kubernetes.io/managed-by"] == "ai-gateway-controller" {
		if err := client.IgnoreNotFound(r.Delete(ctx, &cm)); err != nil {
			return err
		}
	}
	return r.gcTenantState(ctx, namespace, nil, nil)
}

type tenantResolution struct {
	mtc                   *unstructured.Unstructured
	aitenant              *unstructured.Unstructured
	selected              bool
	missingConfig         bool
	status                string
	apiTranslationEnabled bool
}

// praxisSelected mirrors maas-controller: absent MaasTenantConfig defaults to
// Praxis (3.6 default dataplane). Only an explicit IPP annotation opts out.
func praxisSelected(live tenantResolution) bool {
	return live.missingConfig || live.selected
}

// tenantNamespace reports whether namespace belongs to a tenant. A
// single-namespace controller serves only tenant namespaces, and a namespace an
// AITenant claims is a tenant's even before maas-controller creates its
// MaasTenantConfig. A namespace no tenant claims is served only through Gateway
// attachments.
func (r *Reconciler) tenantNamespace(ctx context.Context, namespace string) (bool, error) {
	if r.Namespace != "" {
		return true, nil
	}
	return r.namespaceClaimedByAITenant(ctx, namespace)
}

// providerTenantReadinessMessage explains why provider transport is waiting
// for the Praxis tenant handoff to become ready. An empty message means the
// tenant gate is open.
func providerTenantReadinessMessage(live tenantResolution) string {
	switch {
	case live.mtc == nil:
		if live.missingConfig {
			return "MaasTenantConfig/default-tenant is missing; waiting for maas-controller to create it"
		}
		return "MaasTenantConfig/default-tenant is not resolvable"
	case !live.mtc.GetDeletionTimestamp().IsZero():
		return "MaasTenantConfig/default-tenant is being deleted"
	case live.aitenant == nil:
		return "owning AITenant is not yet resolvable for MaasTenantConfig"
	case !live.aitenant.GetDeletionTimestamp().IsZero():
		return "owning AITenant is being deleted"
	case live.status != tenant.PayloadProcessingStatusSteady:
		return fmt.Sprintf("MaasTenantConfig selects Praxis but payload-processing-status is %q, expected %q", live.status, tenant.PayloadProcessingStatusSteady)
	case !tenant.DeployReady(live.aitenant):
		return "owning AITenant is not DeployReady (Failed or Ready stamp not current for this generation)"
	default:
		return ""
	}
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
	apiTranslationEnabled, err := tenant.APITranslationEnabled(mtc)
	if err != nil {
		return tenantResolution{}, fmt.Errorf("read MaasTenantConfig.spec.apiTranslation: %w", err)
	}
	return tenantResolution{
		mtc: mtc, aitenant: ait, selected: tenant.UsesPraxis(mtc),
		status: tenant.PayloadProcessingStatus(mtc), apiTranslationEnabled: apiTranslationEnabled,
	}, nil
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
			return tenantResolution{missingConfig: true, selected: true}, nil
		}
		return tenantResolution{}, fmt.Errorf("live get MaasTenantConfig %s/%s: %w", namespace, tenant.MaasTenantConfigInstanceName, err)
	}
	apiTranslationEnabled, err := tenant.APITranslationEnabled(mtc)
	if err != nil {
		return tenantResolution{}, fmt.Errorf("read live MaasTenantConfig.spec.apiTranslation: %w", err)
	}
	resolution := tenantResolution{
		mtc: mtc, selected: tenant.UsesPraxis(mtc), status: tenant.PayloadProcessingStatus(mtc),
		apiTranslationEnabled: apiTranslationEnabled,
	}
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
	if !praxisSelected(live) || live.status != tenant.PayloadProcessingStatusSteady || live.mtc == nil || live.aitenant == nil {
		return false, nil
	}
	if !live.mtc.GetDeletionTimestamp().IsZero() || !live.aitenant.GetDeletionTimestamp().IsZero() {
		return false, nil
	}
	if want.mtcUID != "" && live.mtc.GetUID() != want.mtcUID {
		return false, nil
	}
	if want.apiTranslationEnabled != live.apiTranslationEnabled {
		return false, nil
	}
	if want.aitenantUID != "" && live.aitenant.GetUID() != want.aitenantUID {
		return false, nil
	}
	gatewayName, gatewayNamespace, ok := tenant.GatewayRef(live.aitenant)
	return ok && tenant.DeployReady(live.aitenant) && gatewayName == want.gatewayName && gatewayNamespace == want.gatewayNamespace, nil
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
	if _, err := resolver.ProviderPort(p); err != nil {
		return err
	}
	if err := resolver.ValidateProviderTLS(p); err != nil {
		return err
	}
	authType := resolver.AuthType(strings.ToLower(strings.TrimSpace(p.Spec.Auth.Type)))
	if authType != "apikey" {
		if authType == "" {
			return errors.New("auth.type is required; supported strategy is apikey")
		}
		return fmt.Errorf("unsupported authentication strategy %q; only apikey is supported", p.Spec.Auth.Type)
	}
	if p.Spec.Auth.SecretRef.Name == "" {
		return errors.New("auth.secretRef.name is required")
	}
	if err := resolver.ValidateProviderTrust(p.Spec.Config); err != nil {
		return fmt.Errorf("config %w", err)
	}
	return nil
}

// validateProviderCACredential checks the tls.credentialName Secret in the
// Gateway namespace, where Istio reads it. Without it the Gateway cannot verify
// the provider, so this is reported on the provider instead of failing TLS.
// Like Istio, it reads <name>-cacert and falls back to <name> only when that
// Secret does not exist, taking the CA from the cacert key, or from ca.crt.
// Tenants cannot read the Gateway namespace, so a missing Secret and one
// without a CA return the same errProviderCANotReady; a failed read is
// returned for retry.
func (r *Reconciler) validateProviderCACredential(ctx context.Context, p *v1alpha1.ExternalProvider, gatewayNamespace string) error {
	name := strings.TrimSpace(p.Spec.Config["tls.credentialName"])
	if name == "" {
		return nil
	}
	if r.APIReader == nil {
		return errors.New("APIReader is required for CA Secret reads")
	}
	var secret corev1.Secret
	err := r.APIReader.Get(ctx, client.ObjectKey{Namespace: gatewayNamespace, Name: name + istioCASuffix}, &secret)
	if apierrors.IsNotFound(err) {
		err = r.APIReader.Get(ctx, client.ObjectKey{Namespace: gatewayNamespace, Name: name}, &secret)
	}
	if apierrors.IsNotFound(err) {
		return errProviderCANotReady
	}
	if err != nil {
		return fmt.Errorf("read tls.credentialName Secret: %w", err)
	}
	ca := secret.Data["cacert"]
	if len(ca) == 0 {
		ca = secret.Data["ca.crt"]
	}
	if !x509.NewCertPool().AppendCertsFromPEM(ca) {
		return errProviderCANotReady
	}
	return nil
}

func (r *Reconciler) validateProviderCredential(ctx context.Context, p *v1alpha1.ExternalProvider) error {
	if r.APIReader == nil {
		return errors.New("APIReader is required for credential Secret reads")
	}
	key := client.ObjectKey{Namespace: p.Namespace, Name: p.Spec.Auth.SecretRef.Name}
	var secret corev1.Secret
	if err := r.APIReader.Get(ctx, key, &secret); err != nil {
		return fmt.Errorf("provider credential Secret %s/%s is not ready: %w", key.Namespace, key.Name, err)
	}
	if _, ok := secret.Data["api-key"]; !ok {
		return fmt.Errorf("provider credential Secret %s/%s is missing key api-key", key.Namespace, key.Name)
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
		key := client.ObjectKey{Namespace: route.SecretRefNamespace(), Name: route.SecretName}
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

func (r *Reconciler) validateResolvedRouteSet(ctx context.Context, set *resolver.ResolvedRouteSet) error {
	if err := r.validateResolvedCredentials(ctx, set.Routes()); err != nil {
		return err
	}
	return envelope.ValidateSelectionMetadata(set)
}

// excludeUnsupportedModelSelection excludes every model contributing to a
// conflicting client-visible capability. The overlay selects candidates by
// client name, so repeated providers across model objects must not affect
// unrelated models in the namespace. Unequal positive weights are valid and
// render as Praxis weightedRandom.
func excludeUnsupportedModelSelection(set *resolver.ResolvedRouteSet) (*resolver.ResolvedRouteSet, map[string]error) {
	valid := &resolver.ResolvedRouteSet{Models: make([]resolver.ModelRoutes, 0, len(set.Models))}
	rejected := make(map[string]error)
	bindings := make(map[string]bool)
	duplicateProviders := make(map[string]bool)
	for _, model := range set.Models {
		if count := modelHTTPRouteRuleCount(model.Routes); count > maxHTTPRouteRules {
			rejected[model.ModelRef] = fmt.Errorf("%d HTTPRoute rules exceed the Gateway API limit of %d", count, maxHTTPRouteRules)
		}
		for _, route := range model.Routes {
			binding := route.ClientName + "\x00" + route.ProviderNamespace + "/" + route.Provider
			if bindings[binding] {
				duplicateProviders[route.ClientName] = true
			}
			bindings[binding] = true
		}
	}
	for _, model := range set.Models {
		for _, route := range model.Routes {
			if duplicateProviders[route.ClientName] {
				rejected[model.ModelRef] = fmt.Errorf("%w: client model %s repeats a provider", envelope.ErrDuplicateProvider, route.ClientName)
				break
			}
		}
		if rejected[model.ModelRef] == nil {
			valid.Models = append(valid.Models, model)
		}
	}
	return valid, rejected
}

func allRoutesExplicitlyWeightDisabled(set *resolver.ResolvedRouteSet) bool {
	disabled := explicitlyWeightDisabledModels(set)
	return set != nil && len(set.Models) > 0 && len(disabled) == len(set.Models)
}

func explicitlyWeightDisabledModels(set *resolver.ResolvedRouteSet) map[string]bool {
	disabled := make(map[string]bool)
	if set == nil {
		return disabled
	}
	for _, model := range set.Models {
		if len(model.Routes) != 0 || len(model.Skips) == 0 {
			continue
		}
		allDisabled := true
		for _, skip := range model.Skips {
			if skip.Reason != resolver.SkipWeightDisabled {
				allDisabled = false
				break
			}
		}
		if allDisabled {
			disabled[model.ModelRef] = true
		}
	}
	return disabled
}

func (r *Reconciler) validateModelRouteSet(ctx context.Context, set *resolver.ResolvedRouteSet,
	owners map[string]*v1alpha1.ExternalModel, current *v1alpha1.ExternalModel, report modelStatusReporter) (*resolver.ResolvedRouteSet, error) {
	valid, rejected := excludeUnsupportedModelSelection(set)
	for name, rejection := range rejected {
		if owner := owners[name[strings.LastIndexByte(name, '/')+1:]]; owner != nil {
			if owner.Namespace == current.Namespace && owner.Name == current.Name {
				continue
			}
			if err := r.updateModelStatus(ctx, owner, false, reasonReconcileFailed, rejection.Error(), nil); err != nil {
				return nil, err
			}
		}
	}
	if err := rejected[current.Namespace+"/"+current.Name]; err != nil {
		if statusErr := report(false, reasonReconcileFailed, err.Error(), nil); statusErr != nil {
			return nil, statusErr
		}
		return nil, err
	}
	if err := r.validateResolvedRouteSet(ctx, valid); err != nil {
		reason := reasonReconcileFailed
		if errors.Is(err, errCredentialNotReady) {
			reason = reasonProviderNotReady
		}
		if statusErr := report(false, reason, err.Error(), nil); statusErr != nil {
			return nil, statusErr
		}
		return nil, err
	}
	return valid, nil
}

func (r *Reconciler) namespaceAllowed(namespace string) bool {
	return namespace != "" && (r.Namespace == "" || r.Namespace == namespace)
}

// applyTransport applies the tenant's own model routes. Their providers in
// other namespaces (foreign, by model) also get hashed transport here, and,
// as for an attachment, Gateway DestinationRules and credential copies that
// their HTTPRoute records.
func (r *Reconciler) applyTransport(ctx context.Context, routes []resolver.Route, modelNamespace, tenantID, gatewayName,
	gatewayNamespace string, modelOwners map[string]*v1alpha1.ExternalModel, attached []attachedModel, foreign map[string]attachedModel,
	apiTranslationEnabled bool) error {
	home := attachmentTenant{namespace: modelNamespace, tenantID: tenantID,
		gateway:               v1alpha1.NamespacedObjectReference{Name: gatewayName, Namespace: gatewayNamespace},
		apiTranslationEnabled: apiTranslationEnabled}
	resources := make([]unstructured.Unstructured, 0, len(routes))
	seen := map[string]bool{}
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
		state, hasForeign := foreign[modelName]
		for _, route := range state.routes {
			for _, transport := range []unstructured.Unstructured{providerService(route, modelNamespace), providerServiceEntry(route, modelNamespace)} {
				if key := transport.GetKind() + "/" + transport.GetName(); !seen[key] {
					resources = append(resources, transport)
					seen[key] = true
				}
			}
		}
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
		if hasForeign {
			annotations := obj.GetAnnotations()
			for key, value := range routeStateAnnotations(home.gateway, state) {
				annotations[key] = value
			}
			obj.SetAnnotations(annotations)
		}
		key := obj.GetKind() + "/" + obj.GetNamespace() + "/" + obj.GetName()
		if !seen[key] {
			resources = append(resources, obj)
		}
	}
	if err := r.verifyProviderTransport(ctx, resources); err != nil {
		return err
	}
	for _, modelName := range modelNames {
		if state, ok := foreign[modelName]; ok {
			if err := r.applyAttachmentDestinationRules(ctx, state, home); err != nil {
				return err
			}
			if err := r.applyCredentialCopies(ctx, state, home); err != nil {
				return err
			}
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
	if _, err := r.writeTenantExternalModelRoutes(ctx, home, routes, nil, attached, nil, nil); err != nil {
		return err
	}
	return nil
}

// enableExternalModelRoutes owns a separate EnvoyFilter containing only the
// route-specific ExternalModel filter overrides. The tenant reconciler owns
// the base EnvoyFilter; keeping these objects separate prevents concurrent
// tenant/model reconciles from overwriting configPatches.
func (r *Reconciler) enableExternalModelRoutes(ctx context.Context, t attachmentTenant, routes []resolver.Route) (bool, error) {
	return r.writeTenantExternalModelRoutes(ctx, t, routes, nil, nil, nil, nil)
}

// writeExternalModelRoutes enables the tenant's own routes, then its served
// attachments' routes; preserved patches are kept verbatim ahead of both.
func (r *Reconciler) writeExternalModelRoutes(ctx context.Context, tenantID, modelNamespace, gatewayName, gatewayNamespace string, routes []resolver.Route,
	preserved []any, attached []attachedModel) error {
	name := tenant.PayloadProcessingExternalModelEnvoyFilterName(tenantID)
	attached = servedAttachments(attached)
	if len(routes) == 0 && len(preserved) == 0 && len(attached) == 0 {
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
	postAuth, err := tenant.HasPostAuthFilters(ctx, r.Client, modelNamespace)
	if err != nil {
		return err
	}
	kept := append(make([]any, 0, len(preserved)+len(routes)*3+len(attached)*3+2), preserved...)

	byModel := map[string][]resolver.Route{}
	for _, route := range routes {
		byModel[route.Model] = append(byModel[route.Model], route)
	}
	models := make([]string, 0, len(byModel))
	for model := range byModel {
		models = append(models, model)
	}
	sort.Strings(models)
	// Istio names Gateway routes <route namespace>.<route name>.<rule index>.
	type routeRules struct {
		namespace, name string
		count           int
		streamResponse  bool
	}
	enabled := make([]routeRules, 0, len(models)+len(attached))
	for _, model := range models {
		// Keep per-route ExtProc enablement aligned with the same renderer that
		// creates the HTTPRoute rules. Translation and non-default provider paths
		// add target-path matches; attachments use the same count as their route.
		httpRoute := modelHTTPRouteSet(byModel[model], modelNamespace, gatewayName, gatewayNamespace)
		rules, found, err := unstructured.NestedSlice(httpRoute.Object, "spec", "rules")
		if err != nil || !found {
			return fmt.Errorf("ExternalModel %s route renderer did not produce HTTPRoute rules", model)
		}
		enabled = append(enabled, routeRules{modelNamespace, modelRouteName(model), len(rules), routesNeedTranslation(byModel[model])})
	}
	for _, a := range attached {
		httpRoute := modelHTTPRouteSet(a.routes, a.model.Namespace, gatewayName, gatewayNamespace)
		rules, found, err := unstructured.NestedSlice(httpRoute.Object, "spec", "rules")
		if err != nil || !found {
			return fmt.Errorf("attached ExternalModel %s/%s route renderer did not produce HTTPRoute rules", a.model.Namespace, a.model.Name)
		}
		enabled = append(enabled, routeRules{a.model.Namespace, a.routeName, len(rules), routesNeedTranslation(a.routes)})
	}
	for _, route := range enabled {
		responseMode := "NONE"
		if route.streamResponse {
			responseMode = "STREAMED"
		}
		for index := 0; index < route.count; index++ {
			perFilter := map[string]any{
				externalModelPreExtProcFilter: map[string]any{
					"@type": "type.googleapis.com/envoy.extensions.filters.http.ext_proc.v3.ExtProcPerRoute",
					"overrides": map[string]any{
						"processing_mode": map[string]any{
							"request_header_mode": "SEND",
							// The pre-auth processor extracts the model from JSON before
							// authentication. Give its body-field-to-header filter the
							// complete request body; only post-auth Praxis needs streamed
							// chunks for selected-provider translation.
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
							// Buffer every request so Envoy sends one complete body message,
							// including when trailers follow: stream_usage_inject and the
							// translation filters need the whole JSON body. The pre-auth
							// processor already buffers it.
							"request_body_mode":    "BUFFERED",
							"response_header_mode": "SEND",
							// Envoy chooses the response processing mode before the
							// provider is selected. Stream all rules for a model that
							// can select Messages; retain NONE for OpenAI-only models.
							"response_body_mode":    responseMode,
							"request_trailer_mode":  "SKIP",
							"response_trailer_mode": "SKIP",
						},
					},
				},
				"envoy.filters.http.ext_proc.ipp-pre": map[string]any{
					"@type":    "type.googleapis.com/envoy.extensions.filters.http.ext_proc.v3.ExtProcPerRoute",
					"disabled": true,
				},
			}
			// The "ipp" filter runs the tenant's post-auth filters. Leave it
			// on when the tenant has any; otherwise turn it off.
			if !postAuth {
				perFilter["envoy.filters.http.ext_proc.ipp"] = map[string]any{
					"@type":    "type.googleapis.com/envoy.extensions.filters.http.ext_proc.v3.ExtProcPerRoute",
					"disabled": true,
				}
			}
			kept = append(kept, map[string]any{
				"applyTo": "HTTP_ROUTE",
				"match": map[string]any{
					"context": "GATEWAY",
					"routeConfiguration": map[string]any{"vhost": map[string]any{
						"route": map[string]any{
							"name": fmt.Sprintf("%s.%s.%d", route.namespace, route.name, index),
						},
					}},
				},
				"patch": map[string]any{
					"operation": "MERGE",
					"value":     map[string]any{"typed_per_filter_config": perFilter},
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

func (r *Reconciler) updateProviderStatus(ctx context.Context, p *v1alpha1.ExternalProvider, phase, reason, message string) error {
	old := p.DeepCopy()
	p.Status.ObservedGeneration = p.Generation
	p.Status.Phase = phase
	setCondition(&p.Status.Conditions, conditionReady, boolStatus(phase == resolver.PhaseReady), reason, message)
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
		// httpRouteName names the route of a single attachment only.
		m.Status.HTTPRouteName = ""
		if len(m.Spec.GatewayRefs) <= 1 {
			m.Status.HTTPRouteName = modelRouteName(m.Name)
		}
		if env != nil {
			m.Status.OverlayDigest = env.Revision.Value
			m.Status.OverlayGeneration = env.Provenance.SourceGeneration
			setCondition(&m.Status.Conditions, conditionOverlayDistributed, metav1.ConditionTrue, reasonReconciled, message)
		}
	} else {
		m.Status.Phase = "Failed"
		setCondition(&m.Status.Conditions, conditionOverlayDistributed, metav1.ConditionFalse, reason, message)
		if r.staleRouteName(ctx, m) {
			m.Status.HTTPRouteName = ""
		}
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

// staleRouteName reports whether a model that is not Ready names an HTTPRoute
// that does not serve it: any route but its home route, such as the route of
// a removed IPP or of a withdrawn attachment, or a home route that was
// removed. A home route this controller retains keeps its name. The route is
// read live: it may have been deleted earlier in this reconcile.
func (r *Reconciler) staleRouteName(ctx context.Context, m *v1alpha1.ExternalModel) bool {
	name := m.Status.HTTPRouteName
	if name == "" {
		return false
	}
	if name != modelRouteName(m.Name) {
		return true
	}
	var reader client.Reader = r.Client
	if r.APIReader != nil {
		reader = r.APIReader
	}
	err := reader.Get(ctx, client.ObjectKey{Namespace: m.Namespace, Name: name}, newHTTPRoute())
	if err != nil && !apierrors.IsNotFound(err) {
		r.Log.Error(err, "check home HTTPRoute for model status", "model", client.ObjectKeyFromObject(m))
	}
	return apierrors.IsNotFound(err)
}

// sortModelsByName orders a namespace's models by name before they are
// resolved. The informer cache lists them in no stable order and the resolver
// keeps its input order, so equal inputs would otherwise publish the same
// overlay content under a new digest, and each rewrite re-triggers every model
// of the tenant.
func sortModelsByName(models []v1alpha1.ExternalModel) {
	slices.SortFunc(models, func(a, b v1alpha1.ExternalModel) int { return strings.Compare(a.Name, b.Name) })
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

// providerTransportMetadata names and labels a provider's model-namespace
// transport. Transport for a provider in another namespace carries its
// identity instead of the local provider label.
func providerTransportMetadata(route resolver.Route, ns string) map[string]any {
	if !route.CrossNamespace() {
		return labelledMetadata(route.ServiceName(), ns, "inference.opendatahub.io/external-provider", route.Provider)
	}
	m := labelledMetadata(route.ServiceName(), ns, labelProviderNamespace, route.ProviderNamespace)
	m["annotations"] = map[string]any{annotationAttachmentIdentity: providerIdentity(route)}
	return m
}

func providerService(route resolver.Route, ns string) unstructured.Unstructured {
	endpoint := providerEndpointForRoute(route)
	return unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1", "kind": "Service",
		"metadata": providerTransportMetadata(route, ns),
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
		"metadata": providerTransportMetadata(route, ns),
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
	if route.TLSCredentialName != "" {
		tls["credentialName"] = route.TLSCredentialName
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
	if route.Port != 0 {
		endpoint.port = route.Port
	}
	return endpoint
}

func modelHTTPRoute(route resolver.Route, ns, gateway, gatewayNS string) unstructured.Unstructured {
	return modelHTTPRouteSet([]resolver.Route{route}, ns, gateway, gatewayNS)
}

func modelHTTPRouteRuleCount(routes []resolver.Route) int {
	if len(routes) == 0 {
		return 0
	}
	count := 2 // Canonical path and model-header fallbacks.
	seenProviders := map[string]bool{}
	for _, route := range routes {
		// One rule pair per provider namespace and name, as modelHTTPRouteSet emits.
		if seenProviders[route.ServiceName()] {
			continue
		}
		seenProviders[route.ServiceName()] = true
		// The body-routing rule also matches the selected provider's exact
		// target path, so rewritten requests retain the selected backend without
		// consuming another Gateway API rule.
		count += 2
	}
	return count
}

func routesNeedTranslation(routes []resolver.Route) bool {
	for _, route := range routes {
		if route.APIFormat == "messages" {
			return true
		}
	}
	return false
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
	// Keyed by provider namespace and name: same-named providers from two
	// namespaces each keep their rules.
	seenProviders := map[string]bool{}
	for _, candidate := range routes {
		service := candidate.ServiceName()
		if seenProviders[service] {
			continue
		}
		seenProviders[service] = true
		stableID := envelope.RouteStableID(candidate)
		rules = append(rules, map[string]any{
			"matches": []any{map[string]any{
				"path":    map[string]any{"type": "PathPrefix", "value": path},
				"headers": []any{map[string]any{"name": selectedProviderHeader, "type": "Exact", "value": stableID}},
			}},
			"backendRefs": []any{map[string]any{"name": service, "port": providerPort(candidate)}},
			"filters":     []any{providerURLRewrite(providerEndpointForRoute(candidate)), removeInternalRoutingHeaders()},
			"timeouts":    map[string]any{"request": fmt.Sprintf("%ds", withdrawalRequestTimeoutSeconds)},
		})
		// Body-routed requests enter on a model header rather than the
		// canonical path. When translation is enabled, the selected profile can
		// rewrite the path to candidate.Path before Envoy reselects its route.
		// Include that exact path with the trusted candidate, even for the
		// canonical OpenAI path, so a shared exact-path route cannot win after
		// rewriting. Flag-off routes keep main's match set. Gateway API matches
		// in one rule are alternatives and share the same backend.
		bodyMatches := []any{map[string]any{"headers": []any{
			map[string]any{"name": "X-Gateway-Model-Name", "type": "Exact", "value": route.ClientName},
			map[string]any{"name": selectedProviderHeader, "type": "Exact", "value": stableID},
		}}}
		profilePathRewriteEnabled := candidate.APITranslationEnabled
		if profilePathRewriteEnabled && candidate.Path != "" {
			bodyMatches = append(bodyMatches, map[string]any{
				"path":    map[string]any{"type": "Exact", "value": candidate.Path},
				"headers": []any{map[string]any{"name": selectedProviderHeader, "type": "Exact", "value": stableID}},
			})
		}
		rules = append(rules, map[string]any{
			"matches":     bodyMatches,
			"backendRefs": []any{map[string]any{"name": service, "port": providerPort(candidate)}},
			"filters":     []any{providerHostnameRewrite(providerEndpointForRoute(candidate)), removeInternalRoutingHeaders()},
			"timeouts":    map[string]any{"request": fmt.Sprintf("%ds", withdrawalRequestTimeoutSeconds)},
		})
	}
	sinkName := providerSelectionSinkName(route.Model)
	rules = append(rules, map[string]any{
		"matches":     []any{map[string]any{"path": map[string]any{"type": "PathPrefix", "value": path}}},
		"backendRefs": []any{map[string]any{"name": sinkName, "port": int64(443)}},
		"timeouts":    map[string]any{"request": fmt.Sprintf("%ds", withdrawalRequestTimeoutSeconds)},
	})
	// Keep the body-routing rule for requests whose path is not the canonical
	// model path. The post-auth ExtProc selection header is authoritative for
	// provider choice and the route-cache clear causes Envoy to reselect one of
	// the header rules above.
	rules = append(rules, map[string]any{
		"matches":     []any{map[string]any{"headers": []any{map[string]any{"name": "X-Gateway-Model-Name", "type": "Exact", "value": route.ClientName}}}},
		"backendRefs": []any{map[string]any{"name": sinkName, "port": int64(443)}},
		"timeouts":    map[string]any{"request": fmt.Sprintf("%ds", withdrawalRequestTimeoutSeconds)},
	})
	metadata := labelledMetadata(modelRouteName(route.Model), ns, "inference.opendatahub.io/external-model", route.Model)
	localProfiles := make([]resolver.Route, 0, len(routes))
	for _, profile := range routes {
		if !profile.CrossNamespace() {
			localProfiles = append(localProfiles, profile)
		}
	}
	// Route contains only scalar fields and Secret references, never key data.
	profiles, err := json.Marshal(localProfiles) //nolint:musttag // Internal resolved routes use Go field names in this reference-only snapshot.
	if err != nil {
		return unstructured.Unstructured{}
	}
	metadata["annotations"] = map[string]any{tenant.AnnotationHomeRuntimeRoutes: string(profiles)}
	return unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "gateway.networking.k8s.io/v1", "kind": "HTTPRoute",
		"metadata": metadata,
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

// excludedScopedCandidateIDs derives scoped identities from provider references,
// including hashed identities, without conflating shared client-visible names.
func (r *Reconciler) excludedScopedCandidateIDs(ctx context.Context, namespace string, excluded map[string]bool) (map[string]bool, error) {
	ids := make(map[string]bool)
	for modelRef := range excluded {
		modelNamespace, modelName, found := strings.Cut(modelRef, "/")
		if !found || modelNamespace != namespace {
			continue
		}
		var model v1alpha1.ExternalModel
		if err := r.APIReader.Get(ctx, client.ObjectKey{Namespace: modelNamespace, Name: modelName}, &model); err != nil {
			return nil, fmt.Errorf("read excluded model %s to identify scoped candidates: %w", modelRef, err)
		}
		for _, ref := range model.Spec.ExternalProviderRefs {
			if ref.Ref.Namespace != "" && ref.Ref.Namespace != modelNamespace {
				ids[envelope.ScopedStableID(modelNamespace, modelName, ref.Ref.Namespace, ref.Ref.Name)] = true
			}
		}
	}
	return ids, nil
}

// reconcileDeletingProvider retains in-use transport until serving requests drain.
func (r *Reconciler) reconcileDeletingProvider(ctx context.Context, provider *v1alpha1.ExternalProvider) (reconcile.Result, error) {
	inUse, err := r.providerTransportStillInUse(ctx, provider.Namespace, provider.Name)
	if err != nil {
		return reconcile.Result{}, err
	}
	if inUse {
		if controllerutil.ContainsFinalizer(provider, externalProviderFinalizer) {
			message, messageErr := r.providerWithdrawalWaitMessage(ctx, provider.Namespace, provider.Name)
			if messageErr != nil {
				return reconcile.Result{}, messageErr
			}
			if statusErr := r.updateProviderStatus(ctx, provider, providerPhasePending, reasonProviderInUse, message); statusErr != nil {
				return reconcile.Result{}, statusErr
			}
		}
		return reconcile.Result{RequeueAfter: withdrawalRequeueInterval}, nil
	}
	if !controllerutil.ContainsFinalizer(provider, externalProviderFinalizer) {
		return reconcile.Result{}, r.cleanupProviderTransport(ctx, provider.Namespace, provider.Name)
	}
	if err := r.cleanupProviderTransport(ctx, provider.Namespace, provider.Name); err != nil {
		return reconcile.Result{}, err
	}
	return reconcile.Result{}, r.removeExternalProviderFinalizer(ctx, provider)
}

// releaseUnusedAttachmentProvider releases attachment-only deletion protection
// without changing the status ownership of a namespace claimed by a tenant.
func (r *Reconciler) releaseUnusedAttachmentProvider(ctx context.Context, provider *v1alpha1.ExternalProvider) error {
	if !controllerutil.ContainsFinalizer(provider, externalProviderFinalizer) {
		return nil
	}
	claimed, err := r.tenantNamespace(ctx, provider.Namespace)
	if err != nil || claimed {
		return err
	}
	mtc := tenant.NewMaasTenantConfig()
	if err := r.APIReader.Get(ctx, client.ObjectKey{Namespace: provider.Namespace, Name: "default-tenant"}, mtc); err == nil {
		return nil
	} else if !apierrors.IsNotFound(err) {
		return err
	}
	inUse, err := r.providerTransportStillInUse(ctx, provider.Namespace, provider.Name)
	if err != nil || inUse {
		return err
	}
	if err := r.cleanupProviderTransport(ctx, provider.Namespace, provider.Name); err != nil {
		return err
	}
	return r.removeExternalProviderFinalizer(ctx, provider)
}
