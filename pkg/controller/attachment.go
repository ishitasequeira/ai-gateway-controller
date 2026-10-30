package controller

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/url"
	"slices"
	"sort"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	v1alpha1 "github.com/opendatahub-io/ai-gateway-controller/api/inference/v1alpha1"
	"github.com/opendatahub-io/ai-gateway-controller/pkg/envelope"
	"github.com/opendatahub-io/ai-gateway-controller/pkg/publisher"
	"github.com/opendatahub-io/ai-gateway-controller/pkg/render"
	"github.com/opendatahub-io/ai-gateway-controller/pkg/resolver"
	"github.com/opendatahub-io/ai-gateway-controller/pkg/tenant"
)

// Gateway attachments (spec.gatewayRefs). The home attachment is the one
// whose tenant's MaasTenantConfig is in the model namespace; it keeps the
// existing tenant-local path unchanged. Every other attachment contributes the
// model to a tenant from another namespace: an HTTPRoute in the model
// namespace, and, only after the Gateway accepts that route, a hashed
// DestinationRule in the Gateway namespace, credential copies in the tenant
// namespace and the tenant's overlay entries. A model naming no Gateway in a
// namespace with no MaasTenantConfig attaches implicitly to the default
// tenant's Gateway, as the default tenant served every namespace in 3.5.
const (
	reasonTenantNotResolved       = "TenantNotResolved"
	reasonGatewayNotManaged       = "GatewayNotManaged"
	reasonAmbiguousTenant         = "AmbiguousTenant"
	reasonTenantNotPraxis         = "TenantNotPraxis"
	reasonTenantNotSteady         = "TenantNotSteady"
	reasonModelNameConflict       = "ModelNameConflict"
	reasonTooManyProviders        = "TooManyProviders"
	reasonTranslationDisabled     = "APITranslationDisabled"
	reasonOverlayCapacityExceeded = "OverlayCapacityExceeded"
	reasonAttachmentPending       = "AttachmentPending"

	modelPhasePending = "Pending"
	modelPhaseFailed  = "Failed"

	// Gateway API limits each HTTPRoute to 16 rules. Every provider uses two
	// rules; the body-routing rule also matches its exact target path.
	maxHTTPRouteRules = 16
	// maxOverlayCandidates is the Praxis per-overlay candidate limit.
	maxOverlayCandidates = 1024

	attachmentRequeueInterval = 5 * time.Second

	credentialCopyPrefix = "agc-cred-" //nolint:gosec // Secret name prefix, not a credential

	// attachmentFinalizer holds a deleting model until its extra attachments'
	// own objects are removed; the home attachment keeps externalModelFinalizer.
	attachmentFinalizer = "inference.opendatahub.io/external-model-attachment-cleanup"

	annotationAttachmentModel       = "inference.opendatahub.io/attachment-model"
	annotationAttachmentGateway     = "inference.opendatahub.io/attachment-gateway"
	annotationAttachmentRules       = "inference.opendatahub.io/attachment-destination-rules"
	annotationAttachmentCredentials = "inference.opendatahub.io/attachment-credentials"
	annotationAttachmentIdentity    = "inference.opendatahub.io/attachment-identity"
	annotationCredentialSource      = "inference.opendatahub.io/credential-source"
)

var httpRouteGVK = schema.GroupVersionKind{Group: "gateway.networking.k8s.io", Version: "v1", Kind: "HTTPRoute"}

// attachmentTenant is the Praxis tenant that owns an attached Gateway.
type attachmentTenant struct {
	gateway               v1alpha1.NamespacedObjectReference
	namespace             string // MaasTenantConfig namespace
	tenantID              string
	aitenantUID           types.UID
	mtcUID                types.UID
	apiTranslationEnabled bool
}

// handoff is the tenant handoff state the attachment was gated on, re-checked
// live before writing into the tenant.
func (t attachmentTenant) handoff() modelTenantContext {
	return modelTenantContext{
		aitenantUID: t.aitenantUID, mtcUID: t.mtcUID,
		gatewayName: t.gateway.Name, gatewayNamespace: t.gateway.Namespace,
		apiTranslationEnabled: t.apiTranslationEnabled,
	}
}

// attachmentResult is the outcome of one gateway attachment.
type attachmentResult struct {
	gateway   v1alpha1.NamespacedObjectReference
	ready     bool
	pending   bool
	reason    string
	message   string
	routeName string
	// env is the overlay the home attachment distributed.
	env *envelope.Envelope
}

// credentialCopy is a credential Secret from outside the tenant namespace
// copied into it, referenced key only. header is the provider-boundary request
// header for the credential (envelope.Credential.Header), carried onto the copy
// Secret so the tenant reconciler can restore it when reading attached
// candidates back from the published overlay (where Header does not serialize).
type credentialCopy struct {
	name, sourceNamespace, sourceName, key, header string
}

func (c credentialCopy) source() string {
	return c.sourceNamespace + "/" + c.sourceName + "/" + c.key
}

// attachedModel is one model from another namespace attached to a tenant.
// A non-empty reason excludes it from the tenant.
type attachedModel struct {
	model     *v1alpha1.ExternalModel
	routeName string
	routes    []resolver.Route // stable IDs and credentials rewritten for the tenant
	copies    []credentialCopy
	reason    string
	message   string
	// denied reports a provider reference that is not permitted; the
	// permitted ones are still served.
	denied string
	// Set on admission: whether the credential copies are in place.
	ready bool
}

func gatewayKey(gateway v1alpha1.NamespacedObjectReference) string {
	return gateway.Namespace + "/" + gateway.Name
}

func hasGatewayRef(model *v1alpha1.ExternalModel, gateway v1alpha1.NamespacedObjectReference) bool {
	return slices.Contains(model.Spec.GatewayRefs, gateway)
}

// implicitlyAttached reports whether the model is eligible for an implicit
// attachment to the default tenant's Gateway: it names no Gateway and its
// namespace is no tenant's, holding no MaasTenantConfig and claimed by no
// AITenant. A model with the home finalizer keeps waiting for its own tenant's
// MaasTenantConfig instead, and a single-namespace controller serves no
// attachments.
func (r *Reconciler) implicitlyAttached(ctx context.Context, model *v1alpha1.ExternalModel) (bool, error) {
	if r.Namespace != "" || len(model.Spec.GatewayRefs) > 0 || controllerutil.ContainsFinalizer(model, externalModelFinalizer) {
		return false, nil
	}
	err := r.Get(ctx, client.ObjectKey{Namespace: model.Namespace, Name: tenant.MaasTenantConfigInstanceName}, tenant.NewMaasTenantConfig())
	if !apierrors.IsNotFound(err) {
		if err != nil {
			return false, fmt.Errorf("get MaasTenantConfig %s/%s: %w", model.Namespace, tenant.MaasTenantConfigInstanceName, err)
		}
		return false, nil
	}
	// A tenant namespace whose MaasTenantConfig is not created yet, or is being
	// recreated, is never served by another tenant.
	claimed, err := r.namespaceClaimedByAITenant(ctx, model.Namespace)
	if err != nil {
		return false, err
	}
	return !claimed, nil
}

// namespaceClaimedByAITenant reports whether an AITenant claims the namespace.
// A tenant claims its namespace before maas-controller creates the
// MaasTenantConfig there, and keeps the claim while the config is recreated.
func (r *Reconciler) namespaceClaimedByAITenant(ctx context.Context, namespace string) (bool, error) {
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(tenant.AITenantGVK.GroupVersion().WithKind("AITenantList"))
	if err := r.List(ctx, list); err != nil {
		return false, fmt.Errorf("list AITenants for namespace %s: %w", namespace, err)
	}
	for i := range list.Items {
		if claimed, ok := tenant.ConfigNamespace(&list.Items[i]); ok && claimed == namespace {
			return true, nil
		}
	}
	return false, nil
}

// mayAttachImplicitly is implicitlyAttached for event fan-out, which logs a
// failed read instead of returning it.
func (r *Reconciler) mayAttachImplicitly(ctx context.Context, model *v1alpha1.ExternalModel) bool {
	implicit, err := r.implicitlyAttached(ctx, model)
	if err != nil {
		r.Log.Error(err, "check implicit Gateway attachment", "model", client.ObjectKeyFromObject(model))
	}
	return implicit
}

// defaultTenantGateway returns the Gateway of the default tenant (AITenant
// models-as-a-service), which serves implicit attachments. ok is false unless
// exactly one default AITenant publishes a Gateway. ipp reports a default
// tenant that IPP still serves (tenant.LegacyServes): it selects IPP, or its
// Praxis handoff has not started, for example while the MaaS migration gate
// blocks it. IPP serves models of every namespace itself and reports their
// status, so implicit attachments are left to it.
func (r *Reconciler) defaultTenantGateway(ctx context.Context) (gateway v1alpha1.NamespacedObjectReference, ok, ipp bool, err error) {
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(tenant.AITenantGVK.GroupVersion().WithKind("AITenantList"))
	if err := r.List(ctx, list); err != nil {
		return gateway, false, false, fmt.Errorf("list AITenants for the default tenant: %w", err)
	}
	var owner *unstructured.Unstructured
	for i := range list.Items {
		name, namespace, published := tenant.GatewayRef(&list.Items[i])
		if !published || list.Items[i].GetName() != tenant.DefaultAITenantName {
			continue
		}
		if owner != nil {
			return v1alpha1.NamespacedObjectReference{}, false, false, nil
		}
		owner, gateway = &list.Items[i], v1alpha1.NamespacedObjectReference{Name: name, Namespace: namespace}
	}
	if owner == nil {
		return gateway, false, false, nil
	}
	// A missing MaasTenantConfig is reported by the attachment's tenant gate.
	if tenantNamespace, found := tenant.ConfigNamespace(owner); found {
		mtc := tenant.NewMaasTenantConfig()
		if err := r.Get(ctx, client.ObjectKey{Namespace: tenantNamespace, Name: tenant.MaasTenantConfigInstanceName}, mtc); err == nil {
			ipp = tenant.LegacyServes(mtc)
		} else if !apierrors.IsNotFound(err) {
			return gateway, false, false, fmt.Errorf("get MaasTenantConfig %s/%s: %w", tenantNamespace, tenant.MaasTenantConfigInstanceName, err)
		}
	}
	return gateway, true, ipp, nil
}

// modelGateways returns the Gateways the model attaches to: its gatewayRefs
// or, for an implicit attachment, the Gateway of a default tenant selecting
// Praxis.
func (r *Reconciler) modelGateways(ctx context.Context, model *v1alpha1.ExternalModel) ([]v1alpha1.NamespacedObjectReference, error) {
	if len(model.Spec.GatewayRefs) > 0 {
		return model.Spec.GatewayRefs, nil
	}
	if implicit, err := r.implicitlyAttached(ctx, model); err != nil || !implicit {
		return nil, err
	}
	gateway, ok, ipp, err := r.defaultTenantGateway(ctx)
	if err != nil || !ok || ipp {
		return nil, err
	}
	return []v1alpha1.NamespacedObjectReference{gateway}, nil
}

func clientModelName(model *v1alpha1.ExternalModel) string {
	if model.Spec.ModelName != "" {
		return model.Spec.ModelName
	}
	return model.Name
}

func shortHash(parts ...string) string {
	sum := sha256.Sum256([]byte(strings.Join(parts, "/")))
	return hex.EncodeToString(sum[:])[:12]
}

// attachmentRouteName names an extra attachment's HTTPRoute in the model
// namespace. The home attachment keeps modelRouteName.
func attachmentRouteName(model *v1alpha1.ExternalModel, gateway v1alpha1.NamespacedObjectReference) string {
	hash := shortHash(model.Namespace, model.Name, gateway.Namespace, gateway.Name)
	if name := modelRouteName(model.Name) + "-" + hash; len(name) <= 253 {
		return name
	}
	// ExternalModel names start with an alphanumeric, so the double dash
	// never matches a home route.
	return modelRoutePrefix + "-" + hash
}

// attachmentDestinationRuleName names the Gateway-namespace DestinationRule
// for a provider outside the tenant namespace, within 63 characters.
func attachmentDestinationRuleName(gateway v1alpha1.NamespacedObjectReference, providerNamespace, provider string) string {
	hash := shortHash(gateway.Namespace, gateway.Name, providerNamespace, provider)
	base := providerServicePrefix + provider
	if limit := 63 - len(hash) - 1; len(base) > limit {
		base = strings.TrimRight(base[:limit], "-.")
	}
	return base + "-" + hash
}

// credentialCopyName derives the tenant-namespace copy name for a source
// credential Secret. The provider-boundary header is part of the identity so a
// single source Secret qualified under two different headers (e.g. Azure's
// api-key and Anthropic's x-api-key) yields two distinct copies, each carrying
// its own AnnotationCredentialHeader. Without it a later attached model could
// apply the same copy name with a different header and overwrite the annotation
// the tenant reconciler restores for every candidate referencing that copy.
func credentialCopyName(tenantNamespace, sourceNamespace, sourceName, key, header string) string {
	return credentialCopyPrefix + shortHash(tenantNamespace, sourceNamespace, sourceName, key, header)
}

func newHTTPRoute() *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(httpRouteGVK)
	return u
}

// gatewayAttachedModels enqueues models attached to the AITenant's Gateway.
// Implicit attachments follow the default tenant, including its switch to IPP.
func (r *Reconciler) gatewayAttachedModels(ctx context.Context, ait *unstructured.Unstructured) []reconcile.Request {
	name, namespace, ok := tenant.GatewayRef(ait)
	if !ok {
		return nil
	}
	gateway := v1alpha1.NamespacedObjectReference{Name: name, Namespace: namespace}
	var models v1alpha1.ExternalModelList
	if err := r.List(ctx, &models); err != nil {
		r.Log.Error(err, "list models for Gateway attachment event", "gateway", gatewayKey(gateway))
		return nil
	}
	var requests []reconcile.Request
	for i := range models.Items {
		if r.namespaceAllowed(models.Items[i].Namespace) && (hasGatewayRef(&models.Items[i], gateway) ||
			ait.GetName() == tenant.DefaultAITenantName && r.mayAttachImplicitly(ctx, &models.Items[i])) {
			requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&models.Items[i])})
		}
	}
	return requests
}

// attachmentRouteModels maps an attachment HTTPRoute to its model.
func (r *Reconciler) attachmentRouteModels(_ context.Context, route *unstructured.Unstructured) []reconcile.Request {
	if _, ok := route.GetLabels()[tenant.LabelAttachmentTenant]; !ok {
		return nil
	}
	model := route.GetAnnotations()[annotationAttachmentModel]
	if model == "" || !r.namespaceAllowed(route.GetNamespace()) {
		return nil
	}
	return []reconcile.Request{{NamespacedName: types.NamespacedName{Namespace: route.GetNamespace(), Name: model}}}
}

// sameNameModels enqueues attached models with the same client-facing name,
// in any namespace, so a model-name conflict is re-decided for them. Updates
// map both the old and the new object, so a rename also re-decides the name
// it releases.
func (r *Reconciler) sameNameModels(ctx context.Context, obj client.Object) []reconcile.Request {
	changed, ok := obj.(*v1alpha1.ExternalModel)
	if !ok {
		return nil
	}
	var models v1alpha1.ExternalModelList
	if err := r.List(ctx, &models); err != nil {
		r.Log.Error(err, "list models for model-name conflict", "model", changed.Name)
		return nil
	}
	var requests []reconcile.Request
	for i := range models.Items {
		m := &models.Items[i]
		if r.namespaceAllowed(m.Namespace) && clientModelName(m) == clientModelName(changed) &&
			(len(m.Spec.GatewayRefs) > 0 || r.mayAttachImplicitly(ctx, m)) {
			requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(m)})
		}
	}
	return requests
}

// resolveGatewayTenant resolves a gatewayRef to its owning AITenant through
// status.gatewayRef and returns that tenant's namespace. An empty namespace
// means the attachment is unsupported; result says why.
func (r *Reconciler) resolveGatewayTenant(ctx context.Context, gateway v1alpha1.NamespacedObjectReference) (*unstructured.Unstructured, string, attachmentResult, error) {
	result := attachmentResult{gateway: gateway}
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(tenant.AITenantGVK.GroupVersion().WithKind("AITenantList"))
	if err := r.List(ctx, list); err != nil {
		return nil, "", result, fmt.Errorf("list AITenants for Gateway %s: %w", gatewayKey(gateway), err)
	}
	var owners []*unstructured.Unstructured
	for i := range list.Items {
		if name, namespace, ok := tenant.GatewayRef(&list.Items[i]); ok && name == gateway.Name && namespace == gateway.Namespace {
			owners = append(owners, &list.Items[i])
		}
	}
	switch {
	case len(owners) == 0:
		result.reason, result.message = reasonGatewayNotManaged, "no AITenant manages this Gateway"
		return nil, "", result, nil
	case len(owners) > 1:
		result.reason, result.message = reasonAmbiguousTenant, fmt.Sprintf("%d AITenants reference this Gateway", len(owners))
		return nil, "", result, nil
	}
	tenantNamespace, ok := tenant.ConfigNamespace(owners[0])
	if !ok {
		result.reason, result.message = reasonTenantNotSteady, "the owning AITenant has no tenant namespace yet"
		return nil, "", result, nil
	}
	if !r.namespaceAllowed(tenantNamespace) {
		result.reason, result.message = reasonGatewayNotManaged, fmt.Sprintf("tenant namespace %s is outside this controller's namespace scope", tenantNamespace)
		return nil, "", result, nil
	}
	return owners[0], tenantNamespace, result, nil
}

// gateAttachmentTenant applies the existing backend handshake gate to the
// attached tenant: it must select Praxis and be steady. cleanup reports an
// explicit non-Praxis selection, the existing cleanup command.
func (r *Reconciler) gateAttachmentTenant(ctx context.Context, ait *unstructured.Unstructured, tenantNamespace string, result attachmentResult) (attachmentTenant, attachmentResult, bool, error) {
	t := attachmentTenant{gateway: result.gateway, namespace: tenantNamespace, tenantID: tenant.ID(ait.GetName())}
	live, err := r.liveTenantResolution(ctx, tenantNamespace)
	if err != nil {
		return t, result, false, err
	}
	switch {
	case live.missingConfig:
		result.reason, result.message = reasonTenantNotSteady, "the tenant's MaasTenantConfig is missing; retaining existing serving state"
	case !live.selected:
		result.reason, result.message = reasonTenantNotPraxis, "the tenant does not select Praxis"
		return t, result, live.mtc.GetDeletionTimestamp().IsZero(), nil
	case live.aitenant == nil || live.aitenant.GetUID() != ait.GetUID() ||
		!live.mtc.GetDeletionTimestamp().IsZero() || !live.aitenant.GetDeletionTimestamp().IsZero():
		result.reason, result.message = reasonTenantNotSteady, "the tenant's owning AITenant is changing"
	case live.status != tenant.PayloadProcessingStatusSteady:
		result.reason, result.message = reasonTenantNotSteady, "the tenant's payload-processing-status is not steady"
	case !tenant.DeployReady(live.aitenant):
		result.reason, result.message = reasonTenantNotSteady, "the tenant's AITenant is not DeployReady"
	default:
		if name, namespace, ok := tenant.GatewayRef(live.aitenant); !ok || name != t.gateway.Name || namespace != t.gateway.Namespace {
			result.reason, result.message = reasonTenantNotSteady, "the tenant's Gateway reference changed"
			break
		}
		t.aitenantUID, t.mtcUID = live.aitenant.GetUID(), live.mtc.GetUID()
		t.apiTranslationEnabled = live.apiTranslationEnabled
		result.ready = true
	}
	return t, result, false, nil
}

// reconcileAttachedModel reconciles a model attached to the given Gateways,
// its gatewayRefs or implicit attachment: the home attachment through the
// existing path, every other attachment on its own, then one aggregated status
// write.
func (r *Reconciler) reconcileAttachedModel(ctx context.Context, model *v1alpha1.ExternalModel, gateways []v1alpha1.NamespacedObjectReference) (reconcile.Result, error) {
	type target struct {
		ait             *unstructured.Unstructured
		tenantNamespace string
		result          attachmentResult
	}
	targets := make([]target, 0, len(gateways))
	home := -1
	for _, gateway := range gateways {
		ait, tenantNamespace, result, err := r.resolveGatewayTenant(ctx, gateway)
		if err != nil {
			return reconcile.Result{}, err
		}
		if tenantNamespace == model.Namespace {
			home = len(targets)
		}
		targets = append(targets, target{ait: ait, tenantNamespace: tenantNamespace, result: result})
	}
	extra := home < 0 || len(targets) > 1
	if extra {
		if err := r.setAttachmentFinalizer(ctx, model, true); err != nil {
			return reconcile.Result{}, err
		}
	}

	var result reconcile.Result
	var errs []error
	var homeErr error
	results := make([]attachmentResult, len(targets))
	homeReported := false
	if home >= 0 {
		result, homeErr = r.reconcileHomeModel(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(model)}, model, func(ready bool, reason, message string, env *envelope.Envelope) error {
			homeReported = true
			results[home] = attachmentResult{gateway: targets[home].result.gateway, ready: ready, reason: reason, message: message, routeName: modelRouteName(model.Name), env: env}
			return nil
		})
		if homeErr != nil {
			errs = append(errs, homeErr)
		}
	} else if err := r.leaveHomeTenant(ctx, model); err != nil {
		errs = append(errs, err)
	}

	for i, target := range targets {
		if i == home {
			continue
		}
		// Unsupported attachments retain existing state and never fall back
		// to another Gateway.
		results[i] = target.result
		if target.tenantNamespace == "" {
			continue
		}
		t, gated, cleanup, err := r.gateAttachmentTenant(ctx, target.ait, target.tenantNamespace, target.result)
		results[i] = gated
		switch {
		case err != nil:
			errs = append(errs, err)
			results[i] = failedAttachment(gated, err)
		case cleanup:
			// An explicit non-Praxis selection is the existing cleanup
			// command: the attachment's route is removed.
			if err := r.removeAttachmentRoute(ctx, model, gated.gateway); err != nil {
				errs = append(errs, err)
			}
		case gated.ready:
			if results[i], err = r.reconcileAttachment(ctx, model, t); err != nil {
				errs = append(errs, err)
				results[i] = failedAttachment(results[i], err)
			}
		}
	}
	if err := r.removeDetachedRoutes(ctx, model, gateways); err != nil {
		errs = append(errs, err)
	} else if !extra {
		if err := r.setAttachmentFinalizer(ctx, model, false); err != nil {
			errs = append(errs, err)
		}
	}

	if home >= 0 && !homeReported {
		// The home path stopped without a status: an explicit non-Praxis
		// selection (cleanup), a requeue for a changing tenant, or an error.
		results[home] = attachmentResult{gateway: targets[home].result.gateway, reason: reasonTenantNotPraxis,
			message: "the model's own tenant does not select Praxis"}
		switch {
		case homeErr != nil:
			// Only the home path's own error; other attachments report theirs.
			results[home] = failedAttachment(attachmentResult{gateway: targets[home].result.gateway}, homeErr)
		case result.Requeue || result.RequeueAfter > 0: //nolint:staticcheck // the home path's existing requeue signal
			results[home].pending, results[home].reason = true, reasonTenantNotSteady
			results[home].message = "the model's own tenant is not steady"
		}
	}
	if len(results) > 0 {
		if err := r.writeAttachedStatus(ctx, model, results); err != nil {
			errs = append(errs, err)
		}
	}
	for _, res := range results {
		if res.pending {
			// Route acceptance and credential copies are observed through
			// the cache; recheck instead of waiting for an unrelated event.
			result.RequeueAfter = attachmentRequeueInterval
		}
	}
	return result, errors.Join(errs...)
}

// failedAttachment reports an attachment whose reconcile returned an error and
// is retried. Status conditions require a reason.
func failedAttachment(result attachmentResult, err error) attachmentResult {
	if result.reason == "" {
		result.ready, result.reason, result.message = false, reasonReconcileFailed, err.Error()
	}
	return result
}

// writeAttachedStatus writes every attachment's result to status.gateways and
// aggregates them in the same write: Ready requires every attachment,
// otherwise the first failing attachment in gatewayRefs order is reported.
func (r *Reconciler) writeAttachedStatus(ctx context.Context, model *v1alpha1.ExternalModel, results []attachmentResult) error {
	old := model.DeepCopy()
	model.Status.Gateways = gatewayStatuses(model, results)
	for _, res := range results {
		if res.ready {
			continue
		}
		phase := modelPhaseFailed
		if res.pending || res.reason == reasonTenantNotSteady {
			// A tenant that is not steady is waited for, not a failure.
			phase = modelPhasePending
		}
		message := fmt.Sprintf("Gateway %s: %s", gatewayKey(res.gateway), res.message)
		applyModelStatus(model, phase, false, res.reason, message, "")
		// A failed model also drops a route another writer named, such as a
		// removed IPP route; a pending one keeps it, as that writer may still
		// serve the model.
		if phase == modelPhaseFailed && r.staleRouteName(ctx, model) {
			model.Status.HTTPRouteName = ""
		}
		return r.patchModelStatus(ctx, old, model)
	}
	routeName := ""
	if len(results) == 1 {
		routeName = results[0].routeName
	}
	message := fmt.Sprintf("served through %d Gateway attachment(s)", len(results))
	for _, res := range results {
		if res.env != nil {
			// The home attachment keeps its existing overlay attestation.
			model.Status.OverlayDigest = res.env.Revision.Value
			model.Status.OverlayGeneration = res.env.Provenance.SourceGeneration
			message = res.message
		}
	}
	applyModelStatus(model, resolver.PhaseReady, true, reasonReconciled, message, routeName)
	return r.patchModelStatus(ctx, old, model)
}

// gatewayStatuses returns one result per attachment in gatewayRefs order. Only
// a Ready result names the attachment's HTTPRoute, so a failed or pending
// attachment never implies a usable route.
func gatewayStatuses(model *v1alpha1.ExternalModel, results []attachmentResult) []v1alpha1.ExternalModelGatewayStatus {
	statuses := make([]v1alpha1.ExternalModelGatewayStatus, 0, len(results))
	for _, res := range results {
		status := v1alpha1.ExternalModelGatewayStatus{NamespacedObjectReference: res.gateway}
		if i := slices.IndexFunc(model.Status.Gateways, func(g v1alpha1.ExternalModelGatewayStatus) bool {
			return g.NamespacedObjectReference == res.gateway
		}); i >= 0 {
			// Keeps the transition time of an unchanged condition.
			status.Conditions = slices.Clone(model.Status.Gateways[i].Conditions)
		}
		if res.ready {
			status.HTTPRouteRef = &v1alpha1.NamespacedObjectReference{Name: res.routeName, Namespace: model.Namespace}
		}
		apiMeta.SetStatusCondition(&status.Conditions, metav1.Condition{
			Type: conditionReady, Status: boolStatus(res.ready), Reason: res.reason, Message: res.message, ObservedGeneration: model.Generation,
		})
		statuses = append(statuses, status)
	}
	return statuses
}

// leaveHomeTenant republishes the model's own tenant without it when its
// gatewayRefs no longer name that tenant's Gateway. As on the home path, an
// explicit non-Praxis selection cleans up; otherwise the home state is kept
// until the tenant is steady.
func (r *Reconciler) leaveHomeTenant(ctx context.Context, model *v1alpha1.ExternalModel) error {
	if hasHome, err := r.hasHomeRoute(ctx, model); err != nil || !hasHome {
		return err
	}
	live, err := r.liveTenantResolution(ctx, model.Namespace)
	if err != nil {
		return err
	}
	if live.missingConfig {
		return nil
	}
	if !live.selected {
		// An explicit non-Praxis selection is the home path's cleanup
		// command; it does not wait for a steady tenant.
		return r.cleanupUnselectedModel(ctx, model)
	}
	if live.aitenant == nil || live.status != tenant.PayloadProcessingStatusSteady ||
		!live.mtc.GetDeletionTimestamp().IsZero() || !live.aitenant.GetDeletionTimestamp().IsZero() ||
		!tenant.DeployReady(live.aitenant) {
		return nil
	}
	gatewayName, gatewayNamespace, ok := tenant.GatewayRef(live.aitenant)
	if !ok || tenant.ServedByTenantGateway(model, gatewayName, gatewayNamespace) {
		// Still bound to its own tenant; the Gateway lookup was unresolved.
		return nil
	}
	return r.republishHomeWithout(ctx, model, tenant.ID(live.aitenant.GetName()), gatewayName, gatewayNamespace)
}

// hasHomeRoute reports whether the model has its tenant-local HTTPRoute.
func (r *Reconciler) hasHomeRoute(ctx context.Context, model *v1alpha1.ExternalModel) (bool, error) {
	route := newHTTPRoute()
	if err := r.Get(ctx, client.ObjectKey{Namespace: model.Namespace, Name: modelRouteName(model.Name)}, route); err != nil {
		return false, client.IgnoreNotFound(err)
	}
	return route.GetLabels()["inference.opendatahub.io/external-model"] == model.Name, nil
}

// setModelStatus writes the model status with an explicit phase.
func (r *Reconciler) setModelStatus(ctx context.Context, m *v1alpha1.ExternalModel, phase string, ready bool, reason, message, routeName string) error {
	old := m.DeepCopy()
	applyModelStatus(m, phase, ready, reason, message, routeName)
	return r.patchModelStatus(ctx, old, m)
}

// applyModelStatus sets the top-level model status. The route name is recorded
// only when ready, matching updateModelStatus, and is always empty for several
// attachments: it names a single attachment's route. An attachment route name
// is cleared when the model is not ready; a home route keeps its existing
// behavior.
func applyModelStatus(m *v1alpha1.ExternalModel, phase string, ready bool, reason, message, routeName string) {
	m.Status.ObservedGeneration = m.Generation
	m.Status.Phase = phase
	if ready || len(m.Spec.GatewayRefs) > 1 || namesAttachmentRoute(m) {
		m.Status.HTTPRouteName = routeName
	}
	setCondition(&m.Status.Conditions, conditionOverlayDistributed, boolStatus(ready), reason, message)
	setCondition(&m.Status.Conditions, conditionReady, boolStatus(ready), reason, message)
}

// patchModelStatus patches the model status changed since old.
func (r *Reconciler) patchModelStatus(ctx context.Context, old, m *v1alpha1.ExternalModel) error {
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

// reconcileAttachment serves the model through a steady Praxis tenant of
// another namespace.
func (r *Reconciler) reconcileAttachment(ctx context.Context, model *v1alpha1.ExternalModel, t attachmentTenant) (attachmentResult, error) {
	result := attachmentResult{gateway: t.gateway, routeName: attachmentRouteName(model, t.gateway)}
	candidates, err := r.attachmentCandidates(ctx, t)
	if err != nil {
		return result, err
	}
	idx := slices.IndexFunc(candidates, func(c attachedModel) bool { return c.model.UID == model.UID })
	if idx < 0 {
		result.pending, result.reason, result.message = true, reasonAttachmentPending, "waiting for the model to be observed"
		return result, nil
	}
	self := candidates[idx]
	if self.reason != "" {
		// A model-name conflict, a resolution or credential failure (such
		// as a deleted source Secret) and too many providers all exclude the
		// model: no route, overlay entry or transport on this Gateway.
		result.reason, result.message = self.reason, self.message
		return result, r.excludeAttachment(ctx, model, t)
	}
	var providers v1alpha1.ExternalProviderList
	if err := r.List(ctx, &providers, client.InNamespace(model.Namespace)); err != nil {
		return result, fmt.Errorf("list ExternalProviders in %s: %w", model.Namespace, err)
	}
	providerOwners := make(map[string]*v1alpha1.ExternalProvider, len(providers.Items))
	for i := range providers.Items {
		providerOwners[providers.Items[i].Name] = &providers.Items[i]
	}
	if err := r.applyAttachmentRoute(ctx, self, t, providerOwners); err != nil {
		return result, err
	}

	route, err := r.getAttachmentRoute(ctx, model.Namespace, self.routeName)
	if err != nil {
		return result, err
	}
	if accepted, reason, message := routeAccepted(route, t.gateway); !accepted {
		result.reason, result.message = reason, message
		result.pending = reason == reasonAttachmentPending
		// A revoked acceptance withdraws the overlay entry, copies and
		// DestinationRules; publishing excludes routes not accepted.
		return result, r.withdrawAttachment(ctx, model, t)
	}
	// Before writing into the tenant, re-check the handoff live, as the
	// tenant-local path does before applying its transport, and decide the
	// overlay capacity: a model over it receives no DestinationRule or copy.
	if valid, err := r.livePraxisBeforeWrite(ctx, t.namespace, t.handoff()); err != nil || !valid {
		if err == nil {
			result.pending, result.reason, result.message = true, reasonTenantNotSteady, "the tenant's handoff changed; retrying"
		}
		return result, err
	}
	own, err := r.ownOverlayCandidates(ctx, t.namespace)
	if err != nil {
		return result, err
	}
	admission, err := r.admitAttachments(ctx, t, candidates, len(own))
	if err != nil {
		return result, err
	}
	if i := slices.IndexFunc(admission, func(c attachedModel) bool { return c.model.UID == model.UID }); i >= 0 && admission[i].reason != "" {
		result.reason, result.message = admission[i].reason, admission[i].message
		return result, r.withdrawAttachment(ctx, model, t)
	}
	if err := r.applyAttachmentDestinationRules(ctx, self, t); err != nil {
		return result, err
	}
	if err := r.applyCredentialCopies(ctx, self, t); err != nil {
		return result, err
	}
	admitted, err := r.publishAttachmentTenant(ctx, t, model)
	if err != nil {
		return result, err
	}
	if err := r.gcAttachmentState(ctx, t.namespace, excludedRoutes(admitted)); err != nil {
		return result, err
	}
	idx = slices.IndexFunc(admitted, func(c attachedModel) bool { return c.model.UID == model.UID })
	switch {
	case idx >= 0 && admitted[idx].reason != "":
		result.reason, result.message = admitted[idx].reason, admitted[idx].message
	case idx < 0 || !admitted[idx].ready:
		result.pending, result.reason, result.message = true, reasonAttachmentPending, "the tenant has not admitted this attachment yet"
	case self.denied != "":
		// The model's permitted references are served; the others are reported.
		result.reason, result.message = reasonRefNotPermitted, self.denied
	default:
		result.ready, result.reason, result.message = true, reasonReconciled, "attached and distributed to the tenant overlay"
	}
	return result, nil
}

// excludeAttachment removes an excluded model from the tenant: its HTTPRoute,
// the DestinationRules and credential copies no other attachment references,
// and its overlay entry and route filter patches.
func (r *Reconciler) excludeAttachment(ctx context.Context, model *v1alpha1.ExternalModel, t attachmentTenant) error {
	if err := r.removeAttachmentRoute(ctx, model, t.gateway); err != nil {
		return err
	}
	_, err := r.publishAttachmentTenant(ctx, t, model)
	return err
}

// withdrawAttachment stops publishing an attachment that keeps its HTTPRoute
// (not accepted by the Gateway, or over the overlay capacity): the tenant
// republishes without it and its DestinationRules and credential copies are
// collected.
func (r *Reconciler) withdrawAttachment(ctx context.Context, model *v1alpha1.ExternalModel, t attachmentTenant) error {
	admitted, err := r.publishAttachmentTenant(ctx, t, model)
	if err != nil {
		return err
	}
	return r.gcAttachmentState(ctx, t.namespace, excludedRoutes(admitted))
}

// ownServedModels lists the models the tenant serves from its own namespace.
func (r *Reconciler) ownServedModels(ctx context.Context, t attachmentTenant) ([]*v1alpha1.ExternalModel, error) {
	var models v1alpha1.ExternalModelList
	if err := r.List(ctx, &models, client.InNamespace(t.namespace)); err != nil {
		return nil, fmt.Errorf("list tenant ExternalModels in %s: %w", t.namespace, err)
	}
	own := make([]*v1alpha1.ExternalModel, 0, len(models.Items))
	for i := range models.Items {
		if tenant.ServedByTenantGateway(&models.Items[i], t.gateway.Name, t.gateway.Namespace) {
			own = append(own, &models.Items[i])
		}
	}
	return own, nil
}

// attachmentCandidates returns every model from another namespace attached
// to the tenant's Gateway, oldest first, with model-name conflicts and
// per-model resolution decided. Failures exclude only that model.
func (r *Reconciler) attachmentCandidates(ctx context.Context, t attachmentTenant) ([]attachedModel, error) {
	if r.Namespace != "" {
		// A single-namespace controller serves no other namespaces.
		return nil, nil
	}
	own, err := r.ownServedModels(ctx, t)
	if err != nil {
		return nil, err
	}
	// A model name is held by one namespace. Conflicts apply across
	// namespaces only: models in the holder's namespace share the name, as
	// they do today.
	type holder struct{ namespace, model string }
	winners := map[string]holder{}
	for _, m := range own {
		if m.DeletionTimestamp.IsZero() {
			// The tenant's own models always keep their model name.
			winners[clientModelName(m)] = holder{m.Namespace, m.Namespace + "/" + m.Name}
		}
	}
	var models v1alpha1.ExternalModelList
	if err := r.List(ctx, &models); err != nil {
		return nil, fmt.Errorf("list ExternalModels attached to Gateway %s: %w", gatewayKey(t.gateway), err)
	}
	attached := make([]*v1alpha1.ExternalModel, 0)
	for i := range models.Items {
		m := &models.Items[i]
		if m.Namespace == t.namespace || !m.DeletionTimestamp.IsZero() {
			continue
		}
		gateways, err := r.modelGateways(ctx, m)
		if err != nil {
			return nil, err
		}
		if slices.Contains(gateways, t.gateway) {
			attached = append(attached, m)
		}
	}
	sort.SliceStable(attached, func(i, j int) bool {
		if !attached[i].CreationTimestamp.Equal(&attached[j].CreationTimestamp) {
			return attached[i].CreationTimestamp.Before(&attached[j].CreationTimestamp)
		}
		return attached[i].Namespace+"/"+attached[i].Name < attached[j].Namespace+"/"+attached[j].Name
	})
	candidates := make([]attachedModel, 0, len(attached))
	for _, m := range attached {
		candidate := attachedModel{model: m, routeName: attachmentRouteName(m, t.gateway)}
		name := clientModelName(m)
		if winner, taken := winners[name]; taken && winner.namespace != m.Namespace {
			candidate.reason = reasonModelNameConflict
			candidate.message = fmt.Sprintf("model name %q is served on this Gateway by ExternalModel %s", name, winner.model)
		} else {
			if !taken {
				winners[name] = holder{m.Namespace, m.Namespace + "/" + m.Name}
			}
			if err := r.resolveAttachedModel(ctx, t, &candidate); err != nil {
				return nil, err
			}
		}
		candidates = append(candidates, candidate)
	}
	// Same-namespace models sharing a name form one selection group, so the
	// group is validated together, as on the tenant-local path, and a
	// rejection excludes only the group's own models.
	group := &resolver.ResolvedRouteSet{}
	for _, c := range candidates {
		if c.reason == "" {
			group.Models = append(group.Models, resolver.ModelRoutes{ModelRef: c.model.Namespace + "/" + c.model.Name, Routes: c.routes})
		}
	}
	if _, rejected := excludeUnsupportedModelSelection(group); len(rejected) > 0 {
		for i := range candidates {
			if err := rejected[candidates[i].model.Namespace+"/"+candidates[i].model.Name]; err != nil {
				candidates[i].reason, candidates[i].message = reasonReconcileFailed, err.Error()
			}
		}
	}
	return candidates, nil
}

// resolveAttachedModel resolves one attached model against the providers of
// its own namespace and the permitted providers of other namespaces, and
// rewrites its routes for the tenant (see localizeRoutes).
func (r *Reconciler) resolveAttachedModel(ctx context.Context, t attachmentTenant, candidate *attachedModel) error {
	m := candidate.model
	var providers v1alpha1.ExternalProviderList
	if err := r.List(ctx, &providers, client.InNamespace(m.Namespace)); err != nil {
		return fmt.Errorf("list ExternalProviders in %s: %w", m.Namespace, err)
	}
	providerPtrs := make([]*v1alpha1.ExternalProvider, 0, len(providers.Items))
	for i := range providers.Items {
		if !providers.Items[i].DeletionTimestamp.IsZero() {
			continue
		}
		// The provider's own configuration decides, not the status another
		// writer may have left. An invalid provider is kept as Failed, so the
		// model reports why it cannot be used rather than that it is missing.
		checked := providers.Items[i].DeepCopy()
		checked.Status.Phase, checked.Status.Conditions = resolver.PhaseReady, nil
		if err := r.validateProvider(ctx, checked); err != nil {
			checked.Status.Phase = providerPhaseFailed
			setCondition(&checked.Status.Conditions, conditionReady, metav1.ConditionFalse, reasonReconcileFailed, err.Error())
		}
		providerPtrs = append(providerPtrs, checked)
	}
	foreign, unready, err := r.foreignProviders(ctx, m.Namespace, []*v1alpha1.ExternalModel{m})
	if err != nil {
		return err
	}
	set, err := resolver.ResolveWithAPITranslation([]*v1alpha1.ExternalModel{m}, append(providerPtrs, foreign...), t.apiTranslationEnabled)
	set, _, err = skipUnreadyCredentials(set, err, unready)
	if err == nil {
		err = r.validateResolvedCredentials(ctx, set.Routes())
	}
	if transientReadError(err) {
		// A failed Secret read is not the model's own failure: retry
		// instead of excluding, and so withdrawing, a served attachment.
		return err
	}
	denied := deniedReferences(set)[m.Namespace+"/"+m.Name]
	if err != nil {
		candidate.reason, candidate.message = reasonReconcileFailed, err.Error()
		switch {
		case errors.Is(err, resolver.ErrNoRoutes) && denied != "":
			candidate.reason, candidate.message = reasonRefNotPermitted, denied
		case errors.Is(err, resolver.ErrNoRoutes):
			candidate.reason = reasonProviderNotReady
			if reason, message, ok := unroutedStatus(set, m.Namespace+"/"+m.Name); ok {
				// The skipped references, not only that none resolved.
				candidate.reason, candidate.message = reason, message
			}
		case errors.Is(err, errCredentialNotReady):
			candidate.reason = reasonProviderNotReady
		}
		return nil
	}
	routes := set.Routes()
	if count := modelHTTPRouteRuleCount(routes); count > maxHTTPRouteRules {
		candidate.reason = reasonTooManyProviders
		candidate.message = fmt.Sprintf("%d HTTPRoute rules exceed the Gateway API limit of %d for an attached model", count, maxHTTPRouteRules)
		return nil
	}
	if err := envelope.ValidateModel(set.Models[0], envelope.Options{KnownClusters: r.KnownClusters}); err != nil {
		candidate.reason, candidate.message = reasonReconcileFailed, err.Error()
		return nil //nolint:nilerr // excludes only this model
	}
	candidate.copies = localizeRoutes(t.namespace, routes)
	candidate.routes, candidate.denied = routes, denied
	return nil
}

// localizeRoutes rewrites routes for the tenant namespace: a route whose model
// or provider is outside it gets a scoped stable ID, and a credential Secret
// outside it is replaced by a controller-owned copy there. A Secret in the
// tenant namespace is mounted directly. It returns the copies.
func localizeRoutes(tenantNamespace string, routes []resolver.Route) []credentialCopy {
	var copies []credentialCopy
	for i := range routes {
		route := &routes[i]
		if route.Namespace == tenantNamespace && !route.CrossNamespace() {
			continue
		}
		route.StableID = envelope.ScopedStableID(route.Namespace, route.Model, route.ProviderNamespace, route.Provider)
		if route.AuthType == "" {
			continue
		}
		// ValidateModel above has already qualified the strategy, so deriving the
		// provider-boundary header here cannot fail. It is carried onto the copy
		// Secret because Credential.Header does not survive the overlay round-trip.
		_, header, _ := envelope.StrategyFor(*route)
		source := route.SecretRefNamespace()
		// A Secret already in the tenant namespace is mounted directly — unless it
		// carries a provider-boundary header (Azure api-key, Anthropic x-api-key).
		// That header survives the overlay round-trip only on the copy Secret's
		// annotation, which attachedOverlayCandidates reads back, so a headered
		// credential still needs a copy even when its Secret is tenant-local (e.g.
		// a model-local override Secret for a cross-namespace Azure provider).
		if source == tenantNamespace && header == "" {
			continue
		}
		copied := credentialCopy{
			// The header is part of the copy identity (credentialCopyName), so two
			// providers that share a source Secret but diverge on the injected
			// header get distinct copies, each with its own header annotation. This
			// holds across separate attached models sharing one source Secret: each
			// candidate references the copy for its own header, so no apply can
			// overwrite another provider's annotation and attachedOverlayCandidates
			// always restores the correct one.
			name:            credentialCopyName(tenantNamespace, source, route.SecretName, route.SecretKey, header),
			sourceNamespace: source, sourceName: route.SecretName, key: route.SecretKey,
			header: header,
		}
		if !slices.Contains(copies, copied) {
			copies = append(copies, copied)
		}
		route.SecretName, route.SecretNamespace = copied.name, tenantNamespace
	}
	return copies
}

// foreignState localizes the tenant's own routes and returns, by model, the
// state of those to providers in other namespaces: hashed transport, Gateway
// DestinationRules and credential copies, as for an attachment.
func foreignState(tenantNamespace string, set *resolver.ResolvedRouteSet) map[string]attachedModel {
	state := map[string]attachedModel{}
	for _, model := range set.Models {
		copies := localizeRoutes(tenantNamespace, model.Routes)
		var routes []resolver.Route
		for _, route := range model.Routes {
			if route.CrossNamespace() {
				routes = append(routes, route)
			}
		}
		if len(routes) > 0 {
			state[routes[0].Model] = attachedModel{routes: routes, copies: copies}
		}
	}
	return state
}

// routeStateAnnotations records on an HTTPRoute the DestinationRules and
// credential copies its routes need; collection keeps those.
func routeStateAnnotations(gateway v1alpha1.NamespacedObjectReference, a attachedModel) map[string]string {
	rules := make([]string, 0, len(a.routes))
	for _, route := range a.routes {
		if name := attachmentDestinationRuleName(gateway, route.ProviderNamespace, route.Provider); !slices.Contains(rules, name) {
			rules = append(rules, name)
		}
	}
	copies := make([]string, 0, len(a.copies))
	for _, copied := range a.copies {
		copies = append(copies, copied.name)
	}
	return map[string]string{annotationAttachmentRules: strings.Join(rules, ","), annotationAttachmentCredentials: strings.Join(copies, ",")}
}

// stateAnnotations returns the state annotations of the tenant's own
// HTTPRoutes for the given foreign state.
func stateAnnotations(gateway v1alpha1.NamespacedObjectReference, state map[string]attachedModel) []map[string]string {
	own := make([]map[string]string, 0, len(state))
	for _, a := range state {
		own = append(own, routeStateAnnotations(gateway, a))
	}
	return own
}

// transientReadError reports an API read that failed for a reason other than
// the object being absent.
func transientReadError(err error) bool {
	if err == nil || apierrors.IsNotFound(err) {
		return false
	}
	var status apierrors.APIStatus
	var urlErr *url.Error
	return errors.As(err, &status) || errors.As(err, &urlErr) ||
		errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

// admittedAttachments returns the tenant's attachments in admission order;
// see admitAttachments.
func (r *Reconciler) admittedAttachments(ctx context.Context, t attachmentTenant, ownCandidates int) ([]attachedModel, error) {
	candidates, err := r.attachmentCandidates(ctx, t)
	if err != nil {
		return nil, err
	}
	return r.admitAttachments(ctx, t, candidates, ownCandidates)
}

// admitAttachments admits, oldest first, the candidates whose route the
// Gateway accepted, within the overlay capacity left after the tenant's own
// candidates; those over it carry OverlayCapacityExceeded. Capacity is
// reserved whether or not the credential copies exist yet, so it is decided
// before anything is written into the tenant. Only ready attachments, with
// their copies in place, are published.
func (r *Reconciler) admitAttachments(ctx context.Context, t attachmentTenant, candidates []attachedModel, ownCandidates int) ([]attachedModel, error) {
	admitted := make([]attachedModel, 0, len(candidates))
	capacity := maxOverlayCandidates - ownCandidates
	for _, candidate := range candidates {
		if candidate.reason != "" {
			continue
		}
		route, err := r.getAttachmentRoute(ctx, candidate.model.Namespace, candidate.routeName)
		if err != nil {
			return nil, err
		}
		if accepted, _, _ := routeAccepted(route, t.gateway); !accepted {
			continue
		}
		if len(candidate.routes) > capacity {
			candidate.reason = reasonOverlayCapacityExceeded
			candidate.message = fmt.Sprintf("the tenant overlay has no capacity for %d more candidates (limit %d)", len(candidate.routes), maxOverlayCandidates)
			admitted = append(admitted, candidate)
			continue
		}
		capacity -= len(candidate.routes)
		candidate.ready = true
		for _, copied := range candidate.copies {
			_, owned, err := r.credentialCopyOwned(ctx, t.namespace, copied)
			if err != nil {
				return nil, err
			}
			candidate.ready = candidate.ready && owned
		}
		admitted = append(admitted, candidate)
	}
	return admitted, nil
}

// servedAttachments drops attachments excluded by capacity or still waiting
// for their credential copies.
func servedAttachments(attached []attachedModel) []attachedModel {
	served := make([]attachedModel, 0, len(attached))
	for _, a := range attached {
		if a.reason == "" && a.ready {
			served = append(served, a)
		}
	}
	return served
}

// excludedRoutes returns the routes of attachments excluded by the overlay
// capacity. They keep their routes, so their DestinationRules and credential
// copies are collected as if the routes were gone.
func excludedRoutes(attached []attachedModel) map[types.NamespacedName]bool {
	routes := map[types.NamespacedName]bool{}
	for _, a := range attached {
		if a.reason != "" {
			routes[types.NamespacedName{Namespace: a.model.Namespace, Name: a.routeName}] = true
		}
	}
	return routes
}

// publishTenantOverlay publishes the tenant's own route set followed by its
// served attachments, or, with preserved candidates, the already-published
// own candidates followed by the attachments. Without served attachments the
// own route set is published unchanged.
func (r *Reconciler) publishTenantOverlay(ctx context.Context, namespace, gatewayName string, own *resolver.ResolvedRouteSet,
	attached []attachedModel, preserved []envelope.Candidate, sourceUID string, apiTranslationEnabled bool) (publisher.Result, error) {
	set := own
	if served := servedAttachments(attached); len(served) > 0 || own == nil {
		set = &resolver.ResolvedRouteSet{}
		if own != nil {
			set.Models = append(set.Models, own.Models...)
		}
		for _, a := range served {
			set.Models = append(set.Models, resolver.ModelRoutes{ModelRef: a.model.Namespace + "/" + a.model.Name, Routes: a.routes})
		}
	}
	pub, err := publisher.New(r.Client, publisher.Config{Namespace: namespace, Name: r.ConfigMap})
	if err != nil {
		return publisher.Result{}, err
	}
	publish := r.PublishOverlay
	if publish == nil {
		publish = pub.Publish
	}
	return publish(ctx, set, envelope.Scope{
		Network: r.Network, Gateway: gatewayName, Namespace: namespace, LocalSite: r.localSite(),
	}, envelope.Options{
		KnownClusters: r.KnownClusters, SourceUID: sourceUID, ProducerVersion: r.ProducerVersion,
		PreservedCandidates: preserved, IncludeProviderProfiles: apiTranslationEnabled,
	})
}

// keepTenantAttachments keeps a tenant whose own models serve nothing
// publishing its attachments: their route patches and overlay entries, with
// only the tenant's own transport removed. It reports whether any attachment
// is served; without one the caller keeps its existing cleanup.
func (r *Reconciler) keepTenantAttachments(ctx context.Context, t attachmentTenant, sourceUID string) (bool, error) {
	attached, err := r.admittedAttachments(ctx, t, 0)
	if err != nil || len(servedAttachments(attached)) == 0 {
		return false, err
	}
	if err := r.writeExternalModelRoutes(ctx, t.tenantID, t.namespace, t.gateway.Name, t.gateway.Namespace, nil, nil, attached); err != nil {
		return true, err
	}
	if err := r.cleanupExternalModelRouteFilters(ctx, t.namespace, t.tenantID, t.gateway.Name, t.gateway.Namespace); err != nil {
		return true, err
	}
	if err := r.cleanupModelTransport(ctx, t.namespace, nil); err != nil {
		return true, err
	}
	if _, err = r.publishTenantOverlay(ctx, t.namespace, t.gateway.Name, nil, attached, nil, sourceUID, t.apiTranslationEnabled); err != nil {
		return true, err
	}
	// The tenant's own routes are gone, and so is their state.
	return true, r.gcTenantState(ctx, t.namespace, excludedRoutes(attached), nil)
}

// publishAttachmentTenant republishes the tenant's attachments. The tenant's
// own overlay candidates and route patches are preserved verbatim from what
// its own path last published, so attachments never change them. This
// read-modify-write is safe only because ExternalModel reconciles run one at
// a time (the default single worker): the tenant-local path and attachments
// from every namespace write the same overlay and route filter.
func (r *Reconciler) publishAttachmentTenant(ctx context.Context, t attachmentTenant, subject *v1alpha1.ExternalModel) ([]attachedModel, error) {
	if valid, err := r.livePraxisBeforeWrite(ctx, t.namespace, t.handoff()); err != nil || !valid {
		return nil, err
	}
	preserved, err := r.ownOverlayCandidates(ctx, t.namespace)
	if err != nil {
		return nil, err
	}
	ownPatches, err := r.ownRoutePatches(ctx, t)
	if err != nil {
		return nil, err
	}
	admitted, err := r.admittedAttachments(ctx, t, len(preserved))
	if err != nil {
		return nil, err
	}
	if err := r.writeExternalModelRoutes(ctx, t.tenantID, t.namespace, t.gateway.Name, t.gateway.Namespace, nil, ownPatches, admitted); err != nil {
		return nil, err
	}
	if len(preserved) == 0 && len(servedAttachments(admitted)) == 0 {
		return admitted, r.cleanupOverlay(ctx, t.namespace)
	}
	if _, err := r.publishTenantOverlay(ctx, t.namespace, t.gateway.Name, nil, admitted, preserved, string(subject.UID), t.apiTranslationEnabled); err != nil {
		return nil, err
	}
	return admitted, nil
}

// ownOverlayCandidates returns the tenant's own published candidates: those
// with the existing tenant-local stable_id form, and the scoped stable IDs of
// its own models' providers in other namespaces. An attachment's scoped
// stable ID starts with, or hashes, another model namespace.
func (r *Reconciler) ownOverlayCandidates(ctx context.Context, namespace string) ([]envelope.Candidate, error) {
	var models v1alpha1.ExternalModelList
	if err := r.List(ctx, &models, client.InNamespace(namespace)); err != nil {
		return nil, fmt.Errorf("list tenant ExternalModels in %s: %w", namespace, err)
	}
	ownScoped := map[string]bool{}
	for i := range models.Items {
		for _, ref := range models.Items[i].Spec.ExternalProviderRefs {
			if ref.Ref.Namespace != "" && ref.Ref.Namespace != namespace {
				ownScoped[envelope.ScopedStableID(namespace, models.Items[i].Name, ref.Ref.Namespace, ref.Ref.Name)] = true
			}
		}
	}
	var cm corev1.ConfigMap
	// Live read: the tenant-local path may have published in this same
	// event batch, and preserving a stale copy would drop its candidates.
	if err := r.APIReader.Get(ctx, client.ObjectKey{Namespace: namespace, Name: r.configMapName()}, &cm); err != nil {
		return nil, client.IgnoreNotFound(err)
	}
	raw, ok := cm.Data[publisher.DefaultDataKey]
	if !ok {
		return nil, nil
	}
	var env envelope.Envelope
	if err := json.Unmarshal([]byte(raw), &env); err != nil {
		return nil, fmt.Errorf("%w: %s/%s: %w", publisher.ErrEnvelopeTampered, namespace, cm.Name, err)
	}
	var own []envelope.Candidate
	for _, candidate := range env.Overlay.Candidates {
		if !envelope.IsScopedStableID(candidate.StableID) || ownScoped[candidate.StableID] {
			own = append(own, candidate)
		}
	}
	return own, nil
}

// ownRoutePatches returns the route filter patches for HTTPRoutes in the
// tenant namespace. Istio route names start with the route namespace, and
// attached routes always live in another namespace.
func (r *Reconciler) ownRoutePatches(ctx context.Context, t attachmentTenant) ([]any, error) {
	filter := &unstructured.Unstructured{}
	filter.SetGroupVersionKind(schema.GroupVersionKind{Group: "networking.istio.io", Version: "v1alpha3", Kind: "EnvoyFilter"})
	key := client.ObjectKey{Namespace: t.gateway.Namespace, Name: tenant.PayloadProcessingExternalModelEnvoyFilterName(t.tenantID)}
	if err := r.APIReader.Get(ctx, key, filter); err != nil {
		return nil, client.IgnoreNotFound(err)
	}
	if filter.GetLabels()[externalTenantLabel] != t.namespace {
		return nil, nil
	}
	patches, _, err := unstructured.NestedSlice(filter.Object, "spec", "configPatches")
	if err != nil {
		return nil, fmt.Errorf("read ExternalModel route EnvoyFilter %s: %w", key, err)
	}
	own := make([]any, 0, len(patches))
	for _, raw := range patches {
		patch, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		name, _, _ := unstructured.NestedString(patch, "match", "routeConfiguration", "vhost", "route", "name")
		if strings.HasPrefix(name, t.namespace+".") {
			own = append(own, patch)
		}
	}
	return own, nil
}

// applyAttachmentRoute applies the attachment's HTTPRoute and then the
// model-namespace transport. Provider Services, ServiceEntries and the
// selection sink are shared with the model's other attachments. The route
// goes first, so the provider reconciler's IPP cleanup, which keeps the
// transport of the routes it reads live, never sees a Service without it.
func (r *Reconciler) applyAttachmentRoute(ctx context.Context, a attachedModel, t attachmentTenant, providerOwners map[string]*v1alpha1.ExternalProvider) error {
	m := a.model
	route := modelHTTPRouteSet(a.routes, m.Namespace, t.gateway.Name, t.gateway.Namespace)
	providers := make([]string, 0, len(a.routes))
	var resources []unstructured.Unstructured
	for _, provider := range a.routes {
		if slices.Contains(providers, provider.ServiceName()) {
			continue
		}
		providers = append(providers, provider.ServiceName())
		for _, obj := range []unstructured.Unstructured{providerService(provider, m.Namespace), providerServiceEntry(provider, m.Namespace)} {
			if owner := providerOwners[provider.Provider]; owner != nil && !provider.CrossNamespace() {
				setOwnerReference(&obj, owner)
			}
			resources = append(resources, obj)
		}
	}
	if err := r.verifyProviderTransport(ctx, resources); err != nil {
		return err
	}
	sink := providerSelectionSink(a.routes[0], m.Namespace)
	setOwnerReference(&sink, m)
	route.SetName(a.routeName)
	route.SetLabels(map[string]string{"app.kubernetes.io/managed-by": "ai-gateway-controller", tenant.LabelAttachmentTenant: t.namespace})
	// The DestinationRules and credential copies the route needs; collection
	// keeps those an accepted route still names.
	annotations := routeStateAnnotations(t.gateway, a)
	annotations[annotationAttachmentModel], annotations[annotationAttachmentGateway] = m.Name, gatewayKey(t.gateway)
	route.SetAnnotations(annotations)
	setOwnerReference(&route, m)
	if existing, err := r.getAttachmentRoute(ctx, m.Namespace, a.routeName); err != nil {
		return err
	} else if existing != nil && !isAttachmentRoute(existing, m, t.gateway) {
		return fmt.Errorf("HTTPRoute %s/%s exists with a different identity; refusing to adopt it", m.Namespace, a.routeName)
	}
	return r.applyResources(ctx, append([]unstructured.Unstructured{route}, append(resources, sink)...))
}

func (r *Reconciler) applyResources(ctx context.Context, resources []unstructured.Unstructured) error {
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
	return nil
}

// getAttachmentRoute returns the HTTPRoute at an attachment route name, or nil.
func (r *Reconciler) getAttachmentRoute(ctx context.Context, namespace, name string) (*unstructured.Unstructured, error) {
	route := newHTTPRoute()
	if err := r.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, route); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("get attachment HTTPRoute %s/%s: %w", namespace, name, err)
	}
	return route, nil
}

// isAttachmentRoute verifies a hashed route name against its identity
// annotations before AGC adopts or deletes the route.
func isAttachmentRoute(route *unstructured.Unstructured, model *v1alpha1.ExternalModel, gateway v1alpha1.NamespacedObjectReference) bool {
	annotations := route.GetAnnotations()
	return annotations[annotationAttachmentModel] == model.Name && annotations[annotationAttachmentGateway] == gatewayKey(gateway)
}

// routeAccepted reads the route's parent status for the Gateway. The Gateway
// enforces listener allowedRoutes; its Accepted=False reason is reported
// unchanged.
func routeAccepted(route *unstructured.Unstructured, gateway v1alpha1.NamespacedObjectReference) (bool, string, string) {
	if route == nil {
		return false, reasonAttachmentPending, "waiting for the attachment HTTPRoute"
	}
	namespace := route.GetNamespace()
	parents, _, _ := unstructured.NestedSlice(route.Object, "status", "parents")
	reason, message := reasonAttachmentPending, "waiting for the Gateway to accept the attachment HTTPRoute"
	for _, raw := range parents {
		parent, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		parentName, _, _ := unstructured.NestedString(parent, "parentRef", "name")
		parentNamespace, _, _ := unstructured.NestedString(parent, "parentRef", "namespace")
		if parentNamespace == "" {
			parentNamespace = namespace
		}
		if parentName != gateway.Name || parentNamespace != gateway.Namespace {
			continue
		}
		conditions, _, _ := unstructured.NestedSlice(parent, "conditions")
		for _, rawCondition := range conditions {
			condition, ok := rawCondition.(map[string]any)
			if !ok || condition["type"] != "Accepted" {
				continue
			}
			if condition["status"] == string(metav1.ConditionTrue) {
				return true, "", ""
			}
			if gatewayReason, _ := condition["reason"].(string); gatewayReason != "" {
				reason = gatewayReason
			}
			if gatewayMessage, _ := condition["message"].(string); gatewayMessage != "" {
				message = gatewayMessage
			} else {
				message = "the Gateway did not accept the attachment HTTPRoute"
			}
		}
	}
	return false, reason, message
}

// applyAttachmentDestinationRules applies the Gateway-namespace provider TLS
// policy under hashed, identity-verified names. The rules carry the
// attachment-tenant label instead of the provider-namespace tenant label, so
// the provider's own reconciler never adopts or collects them.
func (r *Reconciler) applyAttachmentDestinationRules(ctx context.Context, a attachedModel, t attachmentTenant) error {
	seen := map[string]bool{}
	var rules []unstructured.Unstructured
	for _, route := range a.routes {
		name := attachmentDestinationRuleName(t.gateway, route.ProviderNamespace, route.Provider)
		if seen[name] {
			continue
		}
		seen[name] = true
		identity := gatewayKey(t.gateway) + "/" + providerIdentity(route)
		existing := &unstructured.Unstructured{}
		existing.SetGroupVersionKind(schema.GroupVersionKind{Group: "networking.istio.io", Version: "v1", Kind: "DestinationRule"})
		if err := r.Get(ctx, client.ObjectKey{Namespace: t.gateway.Namespace, Name: name}, existing); err == nil {
			if existing.GetAnnotations()[annotationAttachmentIdentity] != identity {
				return fmt.Errorf("DestinationRule %s/%s exists with a different identity; refusing to adopt it", t.gateway.Namespace, name)
			}
		} else if !apierrors.IsNotFound(err) {
			return fmt.Errorf("get DestinationRule %s/%s: %w", t.gateway.Namespace, name, err)
		}
		rule := providerDestinationRuleAtGateway(route, t.gateway.Name, t.gateway.Namespace, t.tenantID)
		rule.SetName(name)
		labels := rule.GetLabels()
		delete(labels, externalTenantLabel)
		labels[tenant.LabelAttachmentTenant] = t.namespace
		rule.SetLabels(labels)
		rule.SetAnnotations(map[string]string{annotationAttachmentIdentity: identity})
		rules = append(rules, rule)
	}
	return r.applyResources(ctx, rules)
}

// applyCredentialCopies copies the referenced key of each credential Secret
// from outside the tenant namespace into a controller-owned Secret there.
func (r *Reconciler) applyCredentialCopies(ctx context.Context, a attachedModel, t attachmentTenant) error {
	if r.APIReader == nil {
		return errors.New("APIReader is required for credential Secret reads")
	}
	copies := make([]unstructured.Unstructured, 0, len(a.copies))
	for _, copied := range a.copies {
		if exists, owned, err := r.credentialCopyOwned(ctx, t.namespace, copied); err != nil {
			return err
		} else if exists && !owned {
			return fmt.Errorf("credential copy Secret %s/%s exists with a different identity; refusing to adopt it", t.namespace, copied.name)
		}
		var source corev1.Secret
		if err := r.APIReader.Get(ctx, client.ObjectKey{Namespace: copied.sourceNamespace, Name: copied.sourceName}, &source); err != nil {
			return fmt.Errorf("%w: Secret %s/%s: %w", errCredentialNotReady, copied.sourceNamespace, copied.sourceName, err)
		}
		value, ok := source.Data[copied.key]
		if !ok {
			return fmt.Errorf("%w: Secret %s/%s is missing key %s", errCredentialNotReady, copied.sourceNamespace, copied.sourceName, copied.key)
		}
		annotations := map[string]any{annotationCredentialSource: copied.source()}
		if copied.header != "" {
			annotations[tenant.AnnotationCredentialHeader] = copied.header
		}
		copies = append(copies, unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "v1", "kind": "Secret",
			"metadata": map[string]any{
				"name": copied.name, "namespace": t.namespace,
				"labels": map[string]any{
					"app.kubernetes.io/managed-by": "ai-gateway-controller",
					tenant.LabelCredentialCopy:     "true",
				},
				"annotations": annotations,
			},
			"type": string(corev1.SecretTypeOpaque),
			"data": map[string]any{copied.key: base64.StdEncoding.EncodeToString(value)},
		}})
	}
	return r.applyResources(ctx, copies)
}

// credentialCopyOwned reports whether a Secret exists at the copy name and
// whether it is the controller-owned copy of the same source. Secret metadata
// is read from the metadata-only cache.
func (r *Reconciler) credentialCopyOwned(ctx context.Context, namespace string, copied credentialCopy) (bool, bool, error) {
	existing := &metav1.PartialObjectMetadata{}
	existing.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("Secret"))
	if err := r.Get(ctx, client.ObjectKey{Namespace: namespace, Name: copied.name}, existing); err != nil {
		if apierrors.IsNotFound(err) {
			return false, false, nil
		}
		return false, false, fmt.Errorf("get credential copy %s/%s: %w", namespace, copied.name, err)
	}
	return true, existing.GetLabels()[tenant.LabelCredentialCopy] == "true" &&
		existing.GetAnnotations()[annotationCredentialSource] == copied.source(), nil
}

// listAttachmentRoutes lists controller-owned attachment HTTPRoutes.
func (r *Reconciler) listAttachmentRoutes(ctx context.Context, reader client.Reader, opts ...client.ListOption) ([]unstructured.Unstructured, error) {
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(httpRouteGVK.GroupVersion().WithKind("HTTPRouteList"))
	opts = append(opts, client.MatchingLabels{"app.kubernetes.io/managed-by": "ai-gateway-controller"}, client.HasLabels{tenant.LabelAttachmentTenant})
	if err := reader.List(ctx, list, opts...); err != nil {
		return nil, fmt.Errorf("list attachment HTTPRoutes: %w", err)
	}
	return list.Items, nil
}

// removeAttachmentRoute deletes the model's identity-verified HTTPRoute for
// one Gateway, then the tenant's DestinationRules and credential copies and
// the model namespace's transport no remaining attachment references. The
// route goes first, so no accepted route is left without its backends. The
// tenant's overlay drops the entry on its next publish; until then the entry
// is inert because no route reaches it.
func (r *Reconciler) removeAttachmentRoute(ctx context.Context, model *v1alpha1.ExternalModel, gateway v1alpha1.NamespacedObjectReference) error {
	route, err := r.getAttachmentRoute(ctx, model.Namespace, attachmentRouteName(model, gateway))
	if err != nil || route == nil || !isAttachmentRoute(route, model, gateway) {
		return err
	}
	if err := client.IgnoreNotFound(r.Delete(ctx, route)); err != nil {
		return fmt.Errorf("delete attachment HTTPRoute %s/%s: %w", route.GetNamespace(), route.GetName(), err)
	}
	// The informer cache may still list the deleted route.
	if err := r.gcAttachmentState(ctx, route.GetLabels()[tenant.LabelAttachmentTenant], map[types.NamespacedName]bool{client.ObjectKeyFromObject(route): true}); err != nil {
		return err
	}
	return r.gcAttachmentTransport(ctx, model.Namespace)
}

// gcAttachmentTransport deletes the provider Services and ServiceEntries and
// the selection sinks no HTTPRoute in the namespace uses any more. It collects
// only namespaces no Praxis tenant serves, where only attachments create them;
// in a Praxis tenant namespace the provider reconciler and the tenant-local
// path own them. Routes are read live, so a route just deleted never counts.
func (r *Reconciler) gcAttachmentTransport(ctx context.Context, namespace string) error {
	mtc := tenant.NewMaasTenantConfig()
	if err := r.Get(ctx, client.ObjectKey{Namespace: namespace, Name: tenant.MaasTenantConfigInstanceName}, mtc); err == nil {
		if tenant.UsesPraxis(mtc) {
			return nil
		}
	} else if !apierrors.IsNotFound(err) {
		return fmt.Errorf("get MaasTenantConfig %s/%s: %w", namespace, tenant.MaasTenantConfigInstanceName, err)
	}
	routes := &unstructured.UnstructuredList{}
	routes.SetGroupVersionKind(httpRouteGVK.GroupVersion().WithKind("HTTPRouteList"))
	if err := r.APIReader.List(ctx, routes, client.InNamespace(namespace)); err != nil {
		return fmt.Errorf("list HTTPRoutes in %s: %w", namespace, err)
	}
	used := routeBackends(routes.Items)
	for _, gvk := range []schema.GroupVersionKind{{Version: "v1", Kind: "ServiceList"}, {Group: "networking.istio.io", Version: "v1", Kind: "ServiceEntryList"}} {
		list := &unstructured.UnstructuredList{}
		list.SetGroupVersionKind(gvk)
		if err := r.List(ctx, list, client.InNamespace(namespace), client.MatchingLabels{"app.kubernetes.io/managed-by": "ai-gateway-controller"}); err != nil {
			return fmt.Errorf("list attachment transport %s in %s: %w", gvk.Kind, namespace, err)
		}
		for i := range list.Items {
			obj := &list.Items[i]
			labels := obj.GetLabels()
			if used[obj.GetName()] || (labels["inference.opendatahub.io/external-provider"] == "" && labels["inference.opendatahub.io/external-model"] == "") {
				continue
			}
			if err := client.IgnoreNotFound(r.Delete(ctx, obj)); err != nil {
				return fmt.Errorf("delete attachment transport %s %s/%s: %w", obj.GetKind(), namespace, obj.GetName(), err)
			}
		}
	}
	return nil
}

// routeBackends returns the backend names the routes not being deleted use.
func routeBackends(routes []unstructured.Unstructured) map[string]bool {
	used := map[string]bool{}
	for i := range routes {
		if !routes[i].GetDeletionTimestamp().IsZero() {
			continue
		}
		rules, _, _ := unstructured.NestedSlice(routes[i].Object, "spec", "rules")
		for _, rawRule := range rules {
			rule, _ := rawRule.(map[string]any)
			backends, _, _ := unstructured.NestedSlice(rule, "backendRefs")
			for _, rawBackend := range backends {
				if backend, ok := rawBackend.(map[string]any); ok {
					name, _ := backend["name"].(string)
					used[name] = true
				}
			}
		}
	}
	return used
}

// gcAttachmentState deletes the tenant's attachment DestinationRules and
// credential copies that neither an accepted attachment HTTPRoute, other than
// the given routes (just deleted, or excluded by the overlay capacity), nor
// the tenant's own HTTPRoutes reference.
func (r *Reconciler) gcAttachmentState(ctx context.Context, tenantNamespace string, withdrawn map[types.NamespacedName]bool) error {
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(httpRouteGVK.GroupVersion().WithKind("HTTPRouteList"))
	if err := r.List(ctx, list, client.InNamespace(tenantNamespace), client.MatchingLabels{"app.kubernetes.io/managed-by": "ai-gateway-controller"},
		client.HasLabels{"inference.opendatahub.io/external-model"}); err != nil {
		return fmt.Errorf("list tenant HTTPRoutes in %s: %w", tenantNamespace, err)
	}
	own := make([]map[string]string, 0, len(list.Items))
	for i := range list.Items {
		own = append(own, list.Items[i].GetAnnotations())
	}
	return r.gcTenantState(ctx, tenantNamespace, withdrawn, own)
}

// gcTenantState is gcAttachmentState with the state annotations of the
// tenant's own HTTPRoutes given: the tenant-local path knows the routes it
// applies before the cache does.
func (r *Reconciler) gcTenantState(ctx context.Context, tenantNamespace string, withdrawn map[types.NamespacedName]bool, own []map[string]string) error {
	if tenantNamespace == "" {
		// Never collect across namespaces.
		return nil
	}
	routes, err := r.listAttachmentRoutes(ctx, r.Client, client.MatchingLabels{tenant.LabelAttachmentTenant: tenantNamespace})
	if err != nil {
		return err
	}
	keep := slices.Clone(own)
	for i := range routes {
		annotations := routes[i].GetAnnotations()
		gatewayNamespace, gatewayName, _ := strings.Cut(annotations[annotationAttachmentGateway], "/")
		accepted, _, _ := routeAccepted(&routes[i], v1alpha1.NamespacedObjectReference{Name: gatewayName, Namespace: gatewayNamespace})
		if !withdrawn[client.ObjectKeyFromObject(&routes[i])] && accepted {
			keep = append(keep, annotations)
		}
	}
	keepRules, keepCopies := map[string]bool{}, map[string]bool{}
	for _, annotations := range keep {
		for _, name := range strings.Split(annotations[annotationAttachmentRules], ",") {
			keepRules[name] = true
		}
		for _, name := range strings.Split(annotations[annotationAttachmentCredentials], ",") {
			keepCopies[name] = true
		}
	}
	owned := client.MatchingLabels{"app.kubernetes.io/managed-by": "ai-gateway-controller"}
	rules := &unstructured.UnstructuredList{}
	rules.SetGroupVersionKind(schema.GroupVersionKind{Group: "networking.istio.io", Version: "v1", Kind: "DestinationRuleList"})
	if err := r.List(ctx, rules, owned, client.MatchingLabels{tenant.LabelAttachmentTenant: tenantNamespace}); err != nil {
		return fmt.Errorf("list attachment DestinationRules: %w", err)
	}
	for i := range rules.Items {
		rule := &rules.Items[i]
		if keepRules[rule.GetName()] || rule.GetAnnotations()[annotationAttachmentIdentity] == "" {
			continue
		}
		if err := client.IgnoreNotFound(r.Delete(ctx, rule)); err != nil {
			return fmt.Errorf("delete attachment DestinationRule %s/%s: %w", rule.GetNamespace(), rule.GetName(), err)
		}
	}
	copies := &metav1.PartialObjectMetadataList{}
	copies.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("SecretList"))
	if err := r.List(ctx, copies, client.InNamespace(tenantNamespace), owned, client.MatchingLabels{tenant.LabelCredentialCopy: "true"}); err != nil {
		return fmt.Errorf("list credential copies: %w", err)
	}
	for i := range copies.Items {
		copied := &copies.Items[i]
		if keepCopies[copied.GetName()] || copied.GetAnnotations()[annotationCredentialSource] == "" {
			continue
		}
		if err := client.IgnoreNotFound(r.Delete(ctx, copied)); err != nil {
			return fmt.Errorf("delete credential copy %s/%s: %w", tenantNamespace, copied.GetName(), err)
		}
	}
	return nil
}

// collectDeletedAttachments deletes the credential copies and DestinationRules
// of every tenant that no accepted attachment HTTPRoute references any more;
// see releaseAttachments. It also runs once a model is gone, for a model
// deleted without the attachment finalizer, such as one created before it
// existed. The shared tenant overlay and route filter are not
// rebuilt: their entries are inert once no route reaches them, and the
// tenant's next publish drops them.
func (r *Reconciler) collectDeletedAttachments(ctx context.Context) error {
	if r.Namespace != "" {
		// A single-namespace controller serves no attachments.
		return nil
	}
	owned := client.MatchingLabels{"app.kubernetes.io/managed-by": "ai-gateway-controller"}
	tenants := map[string]bool{}
	rules := &unstructured.UnstructuredList{}
	rules.SetGroupVersionKind(schema.GroupVersionKind{Group: "networking.istio.io", Version: "v1", Kind: "DestinationRuleList"})
	if err := r.List(ctx, rules, owned, client.HasLabels{tenant.LabelAttachmentTenant}); err != nil {
		return fmt.Errorf("list attachment DestinationRules: %w", err)
	}
	for i := range rules.Items {
		tenants[rules.Items[i].GetLabels()[tenant.LabelAttachmentTenant]] = true
	}
	copies := &metav1.PartialObjectMetadataList{}
	copies.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("SecretList"))
	if err := r.List(ctx, copies, owned, client.MatchingLabels{tenant.LabelCredentialCopy: "true"}); err != nil {
		return fmt.Errorf("list credential copies: %w", err)
	}
	for i := range copies.Items {
		tenants[copies.Items[i].GetNamespace()] = true
	}
	names := slices.Sorted(maps.Keys(tenants))
	for _, namespace := range names {
		if err := r.gcAttachmentState(ctx, namespace, nil); err != nil {
			return err
		}
	}
	return nil
}

// removeDetachedRoutes deletes the model's attachment HTTPRoutes for Gateways
// other than the ones it still attaches to.
func (r *Reconciler) removeDetachedRoutes(ctx context.Context, model *v1alpha1.ExternalModel, gateways []v1alpha1.NamespacedObjectReference) error {
	routes, err := r.listAttachmentRoutes(ctx, r.Client, client.InNamespace(model.Namespace))
	if err != nil {
		return err
	}
	for i := range routes {
		gatewayNamespace, gatewayName, ok := strings.Cut(routes[i].GetAnnotations()[annotationAttachmentGateway], "/")
		gateway := v1alpha1.NamespacedObjectReference{Name: gatewayName, Namespace: gatewayNamespace}
		if !ok || routes[i].GetAnnotations()[annotationAttachmentModel] != model.Name || slices.Contains(gateways, gateway) {
			continue
		}
		if err := r.removeAttachmentRoute(ctx, model, gateway); err != nil {
			return err
		}
	}
	return nil
}

// releaseAttachments removes every attachment of a deleting model, or of one
// that no longer attaches to any Gateway, then releases its attachment
// finalizer. The routes go first; the sweeps then collect what they used,
// including what routes already removed by namespace or foreground deletion
// left behind. The finalizer stays until both succeed, so a failed cleanup is
// retried without needing the route.
func (r *Reconciler) releaseAttachments(ctx context.Context, model *v1alpha1.ExternalModel) error {
	if err := r.removeDetachedRoutes(ctx, model, nil); err != nil {
		return err
	}
	if !controllerutil.ContainsFinalizer(model, attachmentFinalizer) {
		return nil
	}
	if err := r.collectDeletedAttachments(ctx); err != nil {
		return err
	}
	if err := r.gcAttachmentTransport(ctx, model.Namespace); err != nil {
		return err
	}
	return r.setAttachmentFinalizer(ctx, model, false)
}

// setAttachmentFinalizer adds or removes the attachment finalizer.
func (r *Reconciler) setAttachmentFinalizer(ctx context.Context, model *v1alpha1.ExternalModel, attached bool) error {
	base := model.DeepCopy()
	var updated bool
	if attached {
		updated = controllerutil.AddFinalizer(model, attachmentFinalizer)
	} else {
		updated = controllerutil.RemoveFinalizer(model, attachmentFinalizer)
	}
	if !updated {
		return nil
	}
	if err := r.Patch(ctx, model, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{})); err != nil {
		return fmt.Errorf("update ExternalModel attachment finalizer: %w", err)
	}
	return nil
}

// clearGatewayStatus removes the per-gateway results of a model without
// Gateway attachments.
func (r *Reconciler) clearGatewayStatus(ctx context.Context, model *v1alpha1.ExternalModel) error {
	if len(model.Status.Gateways) == 0 {
		return nil
	}
	old := model.DeepCopy()
	model.Status.Gateways = nil
	return r.patchModelStatus(ctx, old, model)
}

// namesAttachmentRoute reports whether httpRouteName names one of the model's
// attachment routes: not its home route, nor a route another writer named.
func namesAttachmentRoute(m *v1alpha1.ExternalModel) bool {
	name := m.Status.HTTPRouteName
	return strings.HasPrefix(name, modelRoutePrefix) && name != modelRouteName(m.Name)
}

// attachedModelSinks returns the models whose selection sinks the
// namespace's attachment HTTPRoutes use.
func (r *Reconciler) attachedModelSinks(ctx context.Context, namespace string) (map[string]bool, error) {
	routes, err := r.listAttachmentRoutes(ctx, r.Client, client.InNamespace(namespace))
	if err != nil {
		return nil, err
	}
	models := map[string]bool{}
	for i := range routes {
		models[routes[i].GetAnnotations()[annotationAttachmentModel]] = true
	}
	return models, nil
}
