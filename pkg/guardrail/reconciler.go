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

package guardrail

import (
	"context"
	"fmt"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	aigatewayv1alpha1 "github.com/opendatahub-io/ai-gateway-controller/api/aigateway/v1alpha1"
)

// Reconciler reconciles AIGuardrail resources.
type Reconciler struct {
	// Client reads AIGuardrail and patches its status.
	Client client.Client
	// APIReader reads directly from the API server, bypassing the informer
	// cache, for provider references and credential Secrets whose freshness
	// gates the accepted binding (same rationale as pkg/tenant.Reconciler).
	APIReader client.Reader
	// Scheme is used for owner references and typed conversions.
	Scheme *runtime.Scheme
	// ResyncInterval is the RequeueAfter used once a policy has been
	// evaluated, so provider/permission drift is re-checked periodically even
	// without a watch event. Must be positive.
	ResyncInterval time.Duration
	// Log receives one entry per reconcile plus any resolution error.
	Log logr.Logger
	// Recorder publishes a Warning Event whenever a policy stops being
	// accepted, so the reason a guardrail is not enforcing is visible to a
	// namespace owner who can read their own AIGuardrail but not this
	// controller's logs.
	Recorder events.EventRecorder

	// RESTMapper answers whether TrustyAI's NemoGuardrails CRD is installed.
	// SetupWithManager fills it from the manager; it is re-read on every
	// reconcile so a policy recovers once TrustyAI appears, rather than
	// waiting for this controller to restart.
	RESTMapper apimeta.RESTMapper
}

// refuseAction is the machine-readable action every Event this controller
// emits reports. The events API requires one, and there is a single kind of
// event here: a provider binding being refused.
const refuseAction = "RefuseBinding"

// event records a Kubernetes Event on the AIGuardrail.
//
// Callers must gate emission on the bool apimeta.SetStatusCondition returns,
// so exactly one event fires per state transition: an unaccepted policy is
// requeued every resync interval, and an ungated emit would turn that loop
// into an event flood.
func (r *Reconciler) event(guardrail *aigatewayv1alpha1.AIGuardrail, eventType, reason, message string) {
	// related is nil: the refusal is about this AIGuardrail. The message is
	// passed as an argument rather than as the format string, so a percent
	// sign in a provider-supplied value cannot turn into a verb.
	r.Recorder.Eventf(guardrail, nil, eventType, reason, refuseAction, "%s", message)
}

// providerRef resolves the NemoGuardrails an AIGuardrail references. An
// omitted namespace resolves in the AIGuardrail's own namespace (see
// aiguardrail_types.go).
//
// Reconcile and the field index must agree on this exactly. If the index
// derived a different key than the reconcile path resolves, a provider event
// would wake the wrong policies and a revoked permission would go unnoticed
// until the next resync — the window the provider watch exists to close.
func providerRef(guardrail *aigatewayv1alpha1.AIGuardrail) types.NamespacedName {
	ref := guardrail.Spec.Provider.Nemo.Ref
	namespace := ref.Namespace
	if namespace == "" {
		namespace = guardrail.Namespace
	}
	return types.NamespacedName{Namespace: namespace, Name: ref.Name}
}

// indexGuardrailByProvider is the providerIndexKey extractor. It is a named
// function rather than a closure in SetupWithManager so tests can register the
// identical extractor and verify it agrees with what Reconcile resolves.
func indexGuardrailByProvider(obj client.Object) []string {
	guardrail, ok := obj.(*aigatewayv1alpha1.AIGuardrail)
	if !ok {
		return nil
	}
	return []string{providerRef(guardrail).String()}
}

// SetupWithManager registers AIGuardrail as the primary trigger plus the two
// secondary watches that carry authorization inputs.
//
// Both inputs are owned by someone else — the provider's spec.allowedConsumers
// by TrustyAI, the consumer Namespace's labels by the platform — so neither
// touches the AIGuardrail when it changes, and the primary watch stays silent.
// Without these watches a revoked permission would keep being honoured until
// the next ResyncInterval, five minutes by default.
//
// Credential Secrets are deliberately not watched: nothing in this package
// reads one yet, and a watch with no reader is only cache pressure.
//
// Only the provider watch is skipped when TrustyAI's NemoGuardrails CRD is not
// installed (see providerCRDInstalled). The reconciler is registered either
// way: a cluster without TrustyAI is a supported configuration, and a policy
// there still has to be told on its own status why it is not enforcing.
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	r.RESTMapper = mgr.GetRESTMapper()
	installed, err := providerCRDInstalled(r.RESTMapper)
	if err != nil {
		return err
	}

	// IndexField only records the extractor against a cache that has not been
	// started yet; it performs no I/O and there is nothing to cancel, so
	// context.Background is sufficient rather than plumbing a ctx through
	// every SetupWithManager in the repo.
	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &aigatewayv1alpha1.AIGuardrail{}, providerIndexKey, indexGuardrailByProvider); err != nil {
		return fmt.Errorf("indexing AIGuardrail by provider reference: %w", err)
	}

	b := ctrl.NewControllerManagedBy(mgr).
		For(&aigatewayv1alpha1.AIGuardrail{}).
		// Selector mode matches labels on the consumer's Namespace, so a label
		// edit grants or revokes access without touching the policy or the
		// provider. Only label changes can do that, hence the predicate:
		// without it every Namespace write in the cluster would fan out.
		Watches(&corev1.Namespace{}, handler.EnqueueRequestsFromMapFunc(r.guardrailsInNamespace),
			builder.WithPredicates(predicate.LabelChangedPredicate{}))

	if installed {
		// The provider owns the authorization rule, so an edit to
		// spec.allowedConsumers must re-evaluate every policy bound to it.
		// This starts an informer for TrustyAI's CRD, which is why it is the
		// one part of the setup that depends on the CRD being present.
		b = b.Watches(NewNemoGuardrails(), handler.EnqueueRequestsFromMapFunc(r.guardrailsForProvider))
	} else {
		// Logged at the level an operator will actually see, alongside the
		// reasonProviderCRDNotInstalled each policy will carry.
		r.Log.Info("NemoGuardrails CRD not installed; AIGuardrail provider watch disabled",
			"crd", NemoGuardrailsGVK.String(),
			"effect", "every AIGuardrail is refused until TrustyAI is installed; policies recover on their own, "+
				"but until this controller restarts provider edits are picked up at the resync interval rather than immediately")
	}

	return b.Complete(r)
}

// guardrailsForProvider maps a NemoGuardrails event to every AIGuardrail bound1
// to it.
//
// The lookup is cache-backed on purpose, unlike the reads inside Reconcile.
// This only decides which policies to re-examine, and Reconcile re-reads the
// provider live before publishing any verdict, so a stale cache here can cost
// a redundant wakeup but can never produce an incorrect authorization.
func (r *Reconciler) guardrailsForProvider(ctx context.Context, obj client.Object) []reconcile.Request {
	provider := types.NamespacedName{Namespace: obj.GetNamespace(), Name: obj.GetName()}
	var bound aigatewayv1alpha1.AIGuardrailList
	if err := r.Client.List(ctx, &bound, client.MatchingFields{providerIndexKey: provider.String()}); err != nil {
		r.Log.Error(err, "list AIGuardrails for NemoGuardrails event", "provider", provider)
		return nil
	}
	return guardrailRequests(bound.Items)
}

// guardrailsInNamespace maps a Namespace label change to every AIGuardrail in
// that namespace.
//
// All of them are enqueued, not only those whose provider is in Selector mode,
// because establishing which those are would mean reading every referenced
// provider from inside a map function. A redundant reconcile is cheap and
// idempotent; a missed one leaves a revoked binding standing.
func (r *Reconciler) guardrailsInNamespace(ctx context.Context, obj client.Object) []reconcile.Request {
	var inNamespace aigatewayv1alpha1.AIGuardrailList
	if err := r.Client.List(ctx, &inNamespace, client.InNamespace(obj.GetName())); err != nil {
		r.Log.Error(err, "list AIGuardrails for Namespace event", "namespace", obj.GetName())
		return nil
	}
	return guardrailRequests(inNamespace.Items)
}

func guardrailRequests(guardrails []aigatewayv1alpha1.AIGuardrail) []reconcile.Request {
	requests := make([]reconcile.Request, 0, len(guardrails))
	for i := range guardrails {
		requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&guardrails[i])})
	}
	return requests
}

// Reconcile resolves, authorizes and verifies a single AIGuardrail policy. It
// resolves the NemoGuardrails provider reference and evaluates that provider's
// allowedConsumers policy against this AIGuardrail's namespace (ResolvedRefs),
// checks every spec.checks[].configId against the configurations the provider
// declares (Compatible), and resolves the provider's published checks
// endpoint (ProviderReady). Accepted summarises the three.
//
// An accepted policy also publishes status.providerEndpoint, the endpoint the
// verdict was reached against, and status.bindingRevision, the digest of the
// dependencies behind the verdict that can change without a generation bump.
// Together with status.observedGeneration that lets a consumer tell a current
// verdict from a stale one, and an unchanged policy from one whose provider
// or permissions moved underneath it.
func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := r.Log.WithValues("aiguardrail", req.NamespacedName)

	var guardrail aigatewayv1alpha1.AIGuardrail
	if err := r.Client.Get(ctx, req.NamespacedName, &guardrail); err != nil {
		if apierrors.IsNotFound(err) {
			// Deleted; nothing this controller owns to clean up yet.
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	original := guardrail.DeepCopy()
	guardrail.Status.ObservedGeneration = guardrail.Generation

	// Asked again on every reconcile rather than reused from startup, so a
	// policy is accepted once TrustyAI is installed without this controller
	// being restarted. The provider watch cannot be added to a running
	// controller, so until a restart the resync below is what picks provider
	// edits up; the verdict itself is correct either way.
	installed, err := providerCRDInstalled(r.RESTMapper)
	if err != nil {
		// Discovery failed, which is not evidence the CRD is gone. Backing
		// off beats publishing a verdict on a read that did not complete.
		return ctrl.Result{}, fmt.Errorf("checking whether %s is installed: %w", NemoGuardrailsGVK, err)
	}
	if !installed {
		// Reported as a refusal rather than skipped, so the policy says why
		// it is not enforcing instead of carrying no conditions at all.
		log.Info("NemoGuardrails CRD not installed; refusing AIGuardrail", "crd", NemoGuardrailsGVK.String())
		return r.denyBinding(ctx, &guardrail, original, reasonProviderCRDNotInstalled,
			"TrustyAI's NemoGuardrails CRD is not installed on this cluster, so no guardrail provider can be resolved")
	}

	// A sub-millisecond timeout cannot be rendered. Praxis stores timeout_ms and
	//  rejects zero, so accepting the policy would stall the post-auth rollout.
	if ProviderTimeoutTooFine(guardrail.Spec.Provider.Timeout.Duration) {
		log.Info("AIGuardrail timeout is below 1ms", "timeout", guardrail.Spec.Provider.Timeout.Duration)
		return r.refuse(ctx, &guardrail, original, reasonTimeoutBelowMinimum,
			"timeout must be at least 1ms; Praxis records provider timeouts in whole milliseconds and rejects zero")
	}

	// The same resolution the field index uses, so a provider event and this
	// path can never disagree about which provider a policy is bound to.
	provider := providerRef(&guardrail)

	// Live read via APIReader, bypassing the informer cache: the accepted
	// binding's authorization and readiness gate on the provider's current
	// state, so a stale cache read must not accept a policy against a provider
	// that has since changed or gone away (same rationale as
	// pkg/tenant.Reconciler's APIReader use).
	nemo := NewNemoGuardrails()
	if err := r.APIReader.Get(ctx, provider, nemo); err != nil {
		if !apierrors.IsNotFound(err) {
			// The read did not complete, so nothing was learned about the
			// provider. Denying here would retract a working binding on an API
			// server blip — refuse clears BindingRevision and ProviderEndpoint
			// — so this backs off and leaves the last verdict standing, the
			// same choice the CRD check above makes.
			return ctrl.Result{}, fmt.Errorf("reading NemoGuardrails %s: %w", provider, err)
		}
		// Absence, unlike a failed read, is an answer.
		log.Info("AIGuardrail provider unresolved", "provider", provider, "reason", reasonProviderNotFound)
		return r.denyBinding(ctx, &guardrail, original, reasonProviderNotFound,
			"referenced NemoGuardrails provider does not exist")
	}

	// The provider owns the rule for which namespaces may reference it. Parse
	// its spec.allowedConsumers; an absent policy defaults to Same.
	policy, err := parseAllowedConsumers(nemo)
	if err != nil {
		log.Info("AIGuardrail provider has an invalid allowedConsumers policy",
			"provider", provider, "error", err)
		return r.denyBinding(ctx, &guardrail, original, reasonInvalidPolicy,
			"referenced NemoGuardrails provider has an invalid allowedConsumers policy")
	}

	// Selector mode matches labels on the consumer's Namespace object, so read
	// it live too: those labels are an authorization input, and a stale read
	// must never keep a revoked binding alive.
	//
	// Absence and failure are split as they are for the provider read above. A
	// Namespace that does not exist carries no labels, so the selector cannot
	// match and nil is left for policy.allows to deny on. A read that failed
	// says nothing about the labels, and denying on it would retract a binding
	// that was authorized moments ago — revocation never arrives as an error,
	// it is a successful read of a Namespace that no longer carries the label.
	var consumerNS *corev1.Namespace
	if policy.Mode == consumerModeSelector {
		var ns corev1.Namespace
		switch err := r.APIReader.Get(ctx, types.NamespacedName{Name: guardrail.Namespace}, &ns); {
		case err == nil:
			consumerNS = &ns
		case apierrors.IsNotFound(err):
			log.Info("consumer Namespace does not exist for allowedConsumers selector matching",
				"namespace", guardrail.Namespace)
		default:
			return ctrl.Result{}, fmt.Errorf("reading consumer namespace %s: %w", guardrail.Namespace, err)
		}
	}

	// The consumer identity is the AIGuardrail's own namespace — not the
	// tenant object, model, subscription, runtime Pod or inference caller.
	allowed, err := policy.allows(guardrail.Namespace, nemo.GetNamespace(), consumerNS)
	if err != nil {
		log.Info("AIGuardrail provider allowedConsumers evaluation failed",
			"provider", provider, "error", err)
		return r.denyBinding(ctx, &guardrail, original, reasonInvalidPolicy,
			"referenced NemoGuardrails provider has an invalid allowedConsumers policy")
	}
	if !allowed {
		log.Info("AIGuardrail namespace is not an authorized consumer of its provider",
			"provider", provider, "mode", policy.Mode)
		return r.denyBinding(ctx, &guardrail, original, reasonConsumerNotAuthorized,
			"referenced NemoGuardrails provider does not authorize this namespace")
	}

	// Reference resolved and authorized.
	apimeta.SetStatusCondition(&guardrail.Status.Conditions, metav1.Condition{
		Type:               ConditionResolvedRefs,
		Status:             metav1.ConditionTrue,
		ObservedGeneration: guardrail.Generation,
		Reason:             reasonReferencesAuthorized,
		Message:            "referenced NemoGuardrails provider resolved and authorized",
	})

	// Each check's configId must name a configuration the provider declares.
	// This is evaluated before readiness and published regardless of it: it
	// compares two specs, both readable while the server is down, so a
	// configId typo is reported as one instead of being masked by an unready
	// provider.
	compat := evaluateCompatible(&guardrail, nemo)
	if compat.Err != nil {
		log.Info("AIGuardrail provider has malformed spec.nemoConfigs",
			"provider", provider, "error", compat.Err)
	}
	compatibleStatus := metav1.ConditionFalse
	if compat.Compatible {
		compatibleStatus = metav1.ConditionTrue
	}
	apimeta.SetStatusCondition(&guardrail.Status.Conditions, metav1.Condition{
		Type:               ConditionCompatible,
		Status:             compatibleStatus,
		ObservedGeneration: guardrail.Generation,
		Reason:             compat.Reason,
		Message:            compat.Message,
	})

	// Provider readiness resolves the provider's published checks endpoint,
	// never inferring one from the NeMo CR's status.phase or from its name
	// (see evaluateNemoReady).
	readiness := evaluateNemoReady(nemo)
	readyStatus := metav1.ConditionFalse
	if readiness.Ready {
		readyStatus = metav1.ConditionTrue
	}
	apimeta.SetStatusCondition(&guardrail.Status.Conditions, metav1.Condition{
		Type:               ConditionProviderReady,
		Status:             readyStatus,
		ObservedGeneration: guardrail.Generation,
		Reason:             readiness.Reason,
		Message:            readiness.Message,
	})

	// Either failure prevents activation. Incompatibility is refused first: it
	// is a static misconfiguration whose author can fix it, whereas an unready
	// provider is usually transient and says nothing about whether the policy
	// itself is correct.
	if !compat.Compatible {
		log.Info("AIGuardrail checks are not satisfiable by its provider",
			"provider", provider, "reason", compat.Reason)
		return r.refuse(ctx, &guardrail, original, compat.Reason, compat.Message)
	}
	if !readiness.Ready {
		log.Info("AIGuardrail provider not ready", "provider", provider, "reason", readiness.Reason)
		return r.refuse(ctx, &guardrail, original, reasonProviderNotReady, readiness.Message)
	}

	// Resolved, authorized, satisfiable and ready. The revision digests the
	// dependencies behind this verdict that can change without bumping
	// metadata.generation, so a consumer can tell "same policy, same
	// dependencies" from "same policy, different provider or permission".
	//
	// The error is returned rather than published as a refusal: the inputs are
	// strings and maps already read off two API objects, so marshalling them
	// cannot fail in practice, and inventing a user-facing condition reason
	// for an unreachable state would be vocabulary nobody can act on. If it
	// ever does fail, something is wrong beyond this policy, and a backed-off
	// retry that leaves the previous status alone is the right response.
	revision, err := computeBindingRevision(nemo, compat.Configs, readiness.Endpoint, policy, consumerNS)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("computing the binding revision for %s: %w", req.NamespacedName, err)
	}
	guardrail.Status.BindingRevision = revision

	// Published alongside the revision it was digested into, so a consumer
	// compiles against the endpoint this verdict was reached on rather than
	// re-deriving one from the provider's name.
	guardrail.Status.ProviderEndpoint = readiness.Endpoint

	apimeta.SetStatusCondition(&guardrail.Status.Conditions, metav1.Condition{
		Type:               ConditionAccepted,
		Status:             metav1.ConditionTrue,
		ObservedGeneration: guardrail.Generation,
		Reason:             reasonPolicyAccepted,
		Message:            "policy resolved, authorized and satisfiable by a ready provider",
	})

	if err := r.Client.Status().Patch(ctx, &guardrail, client.MergeFrom(original)); err != nil {
		if apierrors.IsConflict(err) {
			return ctrl.Result{Requeue: true}, nil
		}
		return ctrl.Result{}, err
	}

	log.Info("reconciled AIGuardrail", "generation", guardrail.Generation, "provider", provider)
	return ctrl.Result{RequeueAfter: r.ResyncInterval}, nil
}

// denyBinding publishes an unresolved or unauthorized provider binding.
//
// Compatible and ProviderReady are reset to Unknown because this path returns
// before either is evaluated. Both are answered by reading the provider, so
// without a resolved and authorized one there is no verdict to report — and
// the edits that land here, a deleted provider or a revoked permission, leave
// metadata.generation untouched. A True left over from the last accepted
// reconcile would therefore still carry the current generation and pass the
// staleness fence described in constants.go, telling a consumer that a missing
// provider is ready and compatible.
func (r *Reconciler) denyBinding(
	ctx context.Context,
	guardrail, original *aigatewayv1alpha1.AIGuardrail,
	reason, message string,
) (ctrl.Result, error) {
	apimeta.SetStatusCondition(&guardrail.Status.Conditions, metav1.Condition{
		Type:               ConditionResolvedRefs,
		Status:             metav1.ConditionFalse,
		ObservedGeneration: guardrail.Generation,
		Reason:             reason,
		Message:            message,
	})
	for _, conditionType := range []string{ConditionCompatible, ConditionProviderReady} {
		apimeta.SetStatusCondition(&guardrail.Status.Conditions, metav1.Condition{
			Type:               conditionType,
			Status:             metav1.ConditionUnknown,
			ObservedGeneration: guardrail.Generation,
			Reason:             reason,
			Message:            "not evaluated: " + message,
		})
	}
	return r.refuse(ctx, guardrail, original, reason, message)
}

// refuse publishes Accepted=False, persists the status and requeues on the
// resync cadence, leaving whatever other conditions the caller already set in
// place.
//
// The policy itself is never dropped and its checks are never silently
// removed — it stays marked not accepted, so downstream catalog compilation
// keeps failing closed rather than serving traffic with one fewer guardrail
// than the author declared.
//
// The requeue is a drift fallback, not a recovery path, which is why it uses
// the same cadence as an accepted binding rather than a short one. Every
// refusal here is lifted by an edit this controller already watches: the
// policy itself, the provider it binds to, or the consumer Namespace's
// labels. A refused policy therefore recovers on the watch event, and polling
// faster would only repeat the uncached provider read that produced the
// refusal — forever, for a misconfiguration no amount of re-reading fixes.
func (r *Reconciler) refuse(
	ctx context.Context,
	guardrail, original *aigatewayv1alpha1.AIGuardrail,
	reason, message string,
) (ctrl.Result, error) {
	// There is no accepted binding, so there is no accepted binding revision
	// and no accepted endpoint. Clearing both keeps a consumer that gates on
	// them from matching, or compiling against, what was left over from the
	// last time this policy was accepted.
	guardrail.Status.BindingRevision = ""
	guardrail.Status.ProviderEndpoint = ""

	changed := apimeta.SetStatusCondition(&guardrail.Status.Conditions, metav1.Condition{
		Type:               ConditionAccepted,
		Status:             metav1.ConditionFalse,
		ObservedGeneration: guardrail.Generation,
		Reason:             reason,
		Message:            message,
	})

	if err := r.Client.Status().Patch(ctx, guardrail, client.MergeFrom(original)); err != nil {
		if apierrors.IsConflict(err) {
			return ctrl.Result{Requeue: true}, nil
		}
		return ctrl.Result{}, err
	}
	// Emitted only after the patch succeeds, so an event never announces a
	// verdict that failed to persist, and only on a transition, so the requeue
	// loop cannot become an event flood.
	if changed {
		r.event(guardrail, corev1.EventTypeWarning, reason, message)
	}
	return ctrl.Result{RequeueAfter: r.ResyncInterval}, nil
}
