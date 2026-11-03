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
	"errors"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	aigatewayv1alpha1 "github.com/opendatahub-io/ai-gateway-controller/api/aigateway/v1alpha1"
)

const (
	guardrailName   = "pii-and-jailbreak"
	consumerNS      = "consumer-ns"
	providerNS      = "provider-ns"
	nemoName        = "nemo-main"
	testGeneration  = 3
	guardrailConfig = "jailbreak-v1"
)

// guardrailSchemeForTests registers the AIGuardrail Go types plus
// NemoGuardrailsGVK as unstructured, so the fake client can serve a provider
// this package deliberately does not import Go types for (see nemo.go).
func guardrailSchemeForTests() *runtime.Scheme {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = aigatewayv1alpha1.AddToScheme(scheme)
	scheme.AddKnownTypeWithName(NemoGuardrailsGVK, &unstructured.Unstructured{})
	scheme.AddKnownTypeWithName(NemoGuardrailsGVK.GroupVersion().WithKind(NemoGuardrailsGVK.Kind+"List"), &unstructured.UnstructuredList{})
	return scheme
}

// newGuardrail builds an AIGuardrail in namespace referencing a provider.
// An empty providerNamespace exercises the "resolves in the AIGuardrail's own
// namespace" default. Generation is deliberately non-zero so that asserting
// observedGeneration on each published condition is not vacuous.
func newGuardrail(namespace, providerName, providerNamespace string) *aigatewayv1alpha1.AIGuardrail {
	return &aigatewayv1alpha1.AIGuardrail{
		ObjectMeta: metav1.ObjectMeta{Name: guardrailName, Namespace: namespace, Generation: testGeneration},
		Spec: aigatewayv1alpha1.AIGuardrailSpec{
			Provider: aigatewayv1alpha1.AIGuardrailProvider{
				Nemo: aigatewayv1alpha1.AIGuardrailNemoProvider{
					Ref: aigatewayv1alpha1.AIGuardrailNamespacedReference{Name: providerName, Namespace: providerNamespace},
				},
				Timeout: metav1.Duration{Duration: 2 * time.Second},
			},
			Checks: []aigatewayv1alpha1.AIGuardrailCheck{{
				Name:     "jailbreak",
				ConfigID: guardrailConfig,
				Phases:   []aigatewayv1alpha1.GuardrailPhase{aigatewayv1alpha1.GuardrailPhaseInput},
			}},
		},
	}
}

// newNemo builds a NemoGuardrails provider whose
// spec.allowedConsumers.namespaces is namespacesPolicy. A nil policy omits
// allowedConsumers, the shape of every provider TrustyAI ships today.
//
// It declares guardrailConfig, the configuration newGuardrail's check selects,
// so the default pair is compatible and a test that is about authorization or
// readiness is not refused for an unrelated reason. Use withNemoConfigs to
// build a provider that declares something else.
func newNemo(namespace, name string, namespacesPolicy map[string]any) *unstructured.Unstructured {
	u := NewNemoGuardrails()
	u.SetName(name)
	u.SetNamespace(namespace)
	if namespacesPolicy != nil {
		if err := unstructured.SetNestedMap(u.Object, namespacesPolicy, "spec", "allowedConsumers", "namespaces"); err != nil {
			panic(err)
		}
	}
	// Providers are discoverable by default, so every test that is about
	// something other than readiness reaches the verdict it is written for.
	// withoutNemoEndpoint takes it back off.
	if err := unstructured.SetNestedField(u.Object, "https://"+name+"."+namespace+".svc.cluster.local",
		"status", "endpoint"); err != nil {
		panic(err)
	}
	return withNemoConfigs(u, guardrailConfig)
}

// withoutNemoEndpoint clears the provider's published endpoint, the way
// TrustyAI does when the server is not auth-protected. Nothing else about the
// provider changes, so a refusal can only come from discovery failing.
func withoutNemoEndpoint(u *unstructured.Unstructured) *unstructured.Unstructured {
	unstructured.RemoveNestedField(u.Object, "status", "endpoint")
	return u
}

// withNemoConfigs replaces the provider's declared configurations. Passing no
// names declares an empty list.
func withNemoConfigs(u *unstructured.Unstructured, names ...string) *unstructured.Unstructured {
	entries := make([]any, 0, len(names))
	for _, name := range names {
		entries = append(entries, map[string]any{"name": name})
	}
	if err := unstructured.SetNestedSlice(u.Object, entries, "spec", "nemoConfigs"); err != nil {
		panic(err)
	}
	return u
}

func newNamespace(name string, labels map[string]string) *corev1.Namespace {
	return &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels}}
}

func guardrailRequest(namespace string) ctrl.Request {
	return ctrl.Request{NamespacedName: client.ObjectKey{Namespace: namespace, Name: guardrailName}}
}

// fixture wires a Reconciler the way the manager does, with two distinct
// backing stores: Client is the informer cache, APIReader is the live API
// server. Authorization inputs (the provider and the consumer Namespace) go
// only into the live store, so any test that passes is proving the reconciler
// really read them live rather than out of a cache that may be stale about a
// just-revoked permission.
type fixture struct {
	reconciler *Reconciler
	cache      client.Client
	live       client.Client
	events     <-chan string
}

type fixtureOptions struct {
	cacheObjects []client.Object
	liveObjects  []client.Object
	cacheFuncs   interceptor.Funcs
	liveFuncs    interceptor.Funcs
}

func newFixture(opts fixtureOptions) *fixture {
	scheme := guardrailSchemeForTests()
	cache := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(opts.cacheObjects...).
		WithStatusSubresource(&aigatewayv1alpha1.AIGuardrail{}).
		// The same extractor SetupWithManager hands the manager's field
		// indexer, so the mapping tests exercise the production key derivation
		// rather than a test-local imitation of it.
		WithIndex(&aigatewayv1alpha1.AIGuardrail{}, providerIndexKey, indexGuardrailByProvider).
		WithInterceptorFuncs(opts.cacheFuncs).
		Build()
	live := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(opts.liveObjects...).
		WithInterceptorFuncs(opts.liveFuncs).
		Build()
	recorder := events.NewFakeRecorder(16)
	return &fixture{
		reconciler: &Reconciler{
			Client:         cache,
			APIReader:      live,
			Scheme:         scheme,
			ResyncInterval: time.Minute,
			Recorder:       recorder,
			// What SetupWithManager fills in from the manager. A nil mapper
			// is not a usable Reconciler, so it is stated rather than assumed.
			RESTMapper: mapperWithNemo(),
		},
		cache:  cache,
		live:   live,
		events: recorder.Events,
	}
}

// newStandardFixture is the common case: one AIGuardrail in the cache, and
// whatever authorization inputs the test needs live.
func newStandardFixture(guardrail *aigatewayv1alpha1.AIGuardrail, liveObjects ...client.Object) *fixture {
	return newFixture(fixtureOptions{
		cacheObjects: []client.Object{guardrail},
		liveObjects:  liveObjects,
	})
}

func (f *fixture) reconcile(t *testing.T, namespace string) ctrl.Result {
	t.Helper()
	res, err := f.reconciler.Reconcile(context.Background(), guardrailRequest(namespace))
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	return res
}

func (f *fixture) reload(t *testing.T, namespace string) *aigatewayv1alpha1.AIGuardrail {
	t.Helper()
	var got aigatewayv1alpha1.AIGuardrail
	if err := f.cache.Get(context.Background(), guardrailRequest(namespace).NamespacedName, &got); err != nil {
		t.Fatalf("Get AIGuardrail: %v", err)
	}
	return &got
}

// requireCondition asserts a published condition's status and reason, and
// always asserts its observedGeneration too. Every consumer of these
// conditions has to fence on generation — apimeta.IsStatusConditionTrue
// compares only Type and Status, so an unstamped verdict is one a downstream
// catalog compiler would happily read as current for a spec it never saw.
func requireCondition(t *testing.T, guardrail *aigatewayv1alpha1.AIGuardrail, condType string, status metav1.ConditionStatus, reason string) {
	t.Helper()
	cond := apimeta.FindStatusCondition(guardrail.Status.Conditions, condType)
	if cond == nil {
		t.Fatalf("condition %s is absent; conditions are %#v", condType, guardrail.Status.Conditions)
	}
	if cond.Status != status || cond.Reason != reason {
		t.Fatalf("condition %s = %s/%s, want %s/%s", condType, cond.Status, cond.Reason, status, reason)
	}
	if cond.ObservedGeneration != guardrail.Generation {
		t.Fatalf("condition %s observedGeneration = %d, want %d", condType, cond.ObservedGeneration, guardrail.Generation)
	}
}

// requireDenied asserts the full shape of a refused binding: both conditions
// False with the same reason, a drift-fallback requeue, and the policy left
// in place. The checks must never be dropped — a guardrail that cannot be
// authorized has to keep failing closed downstream, not quietly stop existing.
//
// The requeue is the resync cadence, not a short one: every refusal is lifted
// by a watched edit, so polling faster would only repeat the uncached read
// that produced the refusal.
func requireDenied(t *testing.T, f *fixture, res ctrl.Result, namespace, reason string) *aigatewayv1alpha1.AIGuardrail {
	t.Helper()
	if res.RequeueAfter != f.reconciler.ResyncInterval {
		t.Fatalf("RequeueAfter = %v, resync interval %v", res.RequeueAfter, f.reconciler.ResyncInterval)
	}
	got := f.reload(t, namespace)
	requireCondition(t, got, ConditionResolvedRefs, metav1.ConditionFalse, reason)
	requireCondition(t, got, ConditionAccepted, metav1.ConditionFalse, reason)
	if len(got.Spec.Checks) != 1 {
		t.Fatalf("spec.checks = %#v, want the declared checks left untouched", got.Spec.Checks)
	}
	return got
}

func TestProviderRefResolvesTheReferenceNamespace(t *testing.T) {
	cases := []struct {
		name              string
		providerNamespace string
		want              types.NamespacedName
	}{
		{"explicit namespace is used as written", providerNS, types.NamespacedName{Namespace: providerNS, Name: nemoName}},
		{"omitted namespace falls back to the guardrail's own", "", types.NamespacedName{Namespace: consumerNS, Name: nemoName}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := providerRef(newGuardrail(consumerNS, nemoName, c.providerNamespace)); got != c.want {
				t.Fatalf("providerRef = %v, want %v", got, c.want)
			}
		})
	}
}

// TestIndexGuardrailByProviderAgreesWithProviderRef pins the invariant the
// provider watch depends on. If the index key and the key Reconcile resolves
// ever diverge, a spec.allowedConsumers edit silently wakes nothing and the
// revocation waits for the next resync — exactly the failure the watch was
// added to prevent, and one no other test would catch.
func TestIndexGuardrailByProviderAgreesWithProviderRef(t *testing.T) {
	for _, providerNamespace := range []string{providerNS, ""} {
		guardrail := newGuardrail(consumerNS, nemoName, providerNamespace)
		got := indexGuardrailByProvider(guardrail)
		want := []string{providerRef(guardrail).String()}
		if len(got) != 1 || got[0] != want[0] {
			t.Fatalf("indexGuardrailByProvider = %v, want %v", got, want)
		}
	}
}

func TestIndexGuardrailByProviderIgnoresOtherTypes(t *testing.T) {
	// The indexer is registered for AIGuardrail, but a type assertion that
	// panicked would take the manager down rather than skip one object.
	if got := indexGuardrailByProvider(newNamespace(consumerNS, nil)); got != nil {
		t.Fatalf("indexGuardrailByProvider on a Namespace = %v, want nil", got)
	}
}

// TestGuardrailsForProviderEnqueuesOnlyBoundPolicies covers the fan-out that
// closes the revocation window: editing a provider must wake every policy
// bound to it, in whatever namespace, and nothing else.
func TestGuardrailsForProviderEnqueuesOnlyBoundPolicies(t *testing.T) {
	bound := newGuardrail(consumerNS, nemoName, providerNS)
	boundElsewhere := newGuardrail("other-consumer", nemoName, providerNS)
	// Same provider name, different namespace: must not match.
	boundToANamesake := newGuardrail("namesake-ns", nemoName, "")
	// Different provider in the same namespace: must not match.
	boundToAnother := newGuardrail("another-ns", "nemo-other", providerNS)

	f := newFixture(fixtureOptions{
		cacheObjects: []client.Object{bound, boundElsewhere, boundToANamesake, boundToAnother},
	})

	got := f.reconciler.guardrailsForProvider(context.Background(), newNemo(providerNS, nemoName, nil))
	want := map[client.ObjectKey]bool{
		client.ObjectKeyFromObject(bound):          true,
		client.ObjectKeyFromObject(boundElsewhere): true,
	}
	if len(got) != len(want) {
		t.Fatalf("enqueued %d requests (%v), want %d", len(got), got, len(want))
	}
	for _, req := range got {
		if !want[req.NamespacedName] {
			t.Fatalf("enqueued %v, which is not bound to %s/%s", req.NamespacedName, providerNS, nemoName)
		}
	}
}

// TestGuardrailsForProviderMatchesAnOmittedNamespace is the case the index
// makes easy to get wrong: a policy that omits spec.provider.nemo.ref.namespace
// is bound to a provider in its own namespace, and a provider event there must
// still reach it.
func TestGuardrailsForProviderMatchesAnOmittedNamespace(t *testing.T) {
	guardrail := newGuardrail(consumerNS, nemoName, "")
	f := newFixture(fixtureOptions{cacheObjects: []client.Object{guardrail}})

	got := f.reconciler.guardrailsForProvider(context.Background(), newNemo(consumerNS, nemoName, nil))
	if len(got) != 1 || got[0].NamespacedName != client.ObjectKeyFromObject(guardrail) {
		t.Fatalf("enqueued %v, want just %v", got, client.ObjectKeyFromObject(guardrail))
	}
}

func TestGuardrailsForProviderEnqueuesNothingWhenUnreferenced(t *testing.T) {
	f := newFixture(fixtureOptions{cacheObjects: []client.Object{newGuardrail(consumerNS, nemoName, providerNS)}})
	if got := f.reconciler.guardrailsForProvider(context.Background(), newNemo(providerNS, "unreferenced", nil)); len(got) != 0 {
		t.Fatalf("enqueued %v for an unreferenced provider, want nothing", got)
	}
}

// TestGuardrailsInNamespaceEnqueuesEveryPolicyInIt covers the Namespace label
// watch. It deliberately enqueues policies regardless of their provider's
// mode, since determining that would mean reading providers from a map func.
func TestGuardrailsInNamespaceEnqueuesEveryPolicyInIt(t *testing.T) {
	here := newGuardrail(consumerNS, nemoName, providerNS)
	elsewhere := newGuardrail("other-consumer", nemoName, providerNS)
	f := newFixture(fixtureOptions{cacheObjects: []client.Object{here, elsewhere}})

	got := f.reconciler.guardrailsInNamespace(context.Background(), newNamespace(consumerNS, map[string]string{"guardrails": "shared"}))
	if len(got) != 1 || got[0].NamespacedName != client.ObjectKeyFromObject(here) {
		t.Fatalf("enqueued %v, want just %v", got, client.ObjectKeyFromObject(here))
	}
}

func TestGuardrailsInNamespaceEnqueuesNothingWhenEmpty(t *testing.T) {
	f := newFixture(fixtureOptions{cacheObjects: []client.Object{newGuardrail(consumerNS, nemoName, providerNS)}})
	if got := f.reconciler.guardrailsInNamespace(context.Background(), newNamespace("empty-ns", nil)); len(got) != 0 {
		t.Fatalf("enqueued %v for a namespace with no policies, want nothing", got)
	}
}

// TestMapFunctionsTolerateListFailures keeps a cache read error from taking
// the manager down: a map function that panicked or returned junk would be far
// worse than one dropped wakeup, which the resync recovers from anyway.
func TestMapFunctionsTolerateListFailures(t *testing.T) {
	failList := interceptor.Funcs{
		List: func(_ context.Context, _ client.WithWatch, _ client.ObjectList, _ ...client.ListOption) error {
			return apierrors.NewInternalError(errors.New("cache unavailable"))
		},
	}
	f := newFixture(fixtureOptions{
		cacheObjects: []client.Object{newGuardrail(consumerNS, nemoName, providerNS)},
		cacheFuncs:   failList,
	})
	if got := f.reconciler.guardrailsForProvider(context.Background(), newNemo(providerNS, nemoName, nil)); got != nil {
		t.Fatalf("guardrailsForProvider = %v on a list failure, want nil", got)
	}
	if got := f.reconciler.guardrailsInNamespace(context.Background(), newNamespace(consumerNS, nil)); got != nil {
		t.Fatalf("guardrailsInNamespace = %v on a list failure, want nil", got)
	}
}

func TestReconcileIgnoresMissingGuardrail(t *testing.T) {
	f := newFixture(fixtureOptions{})
	res := f.reconcile(t, consumerNS)
	if res != (ctrl.Result{}) {
		t.Fatalf("Result = %#v, want an empty result for a deleted AIGuardrail", res)
	}
}

// TestReconcileDeniesUnauthorizedCrossNamespaceReference is the ticket's
// "cross-namespace reference without permission is denied" case: the provider
// carries no allowedConsumers, so it defaults to Same and a consumer in
// another namespace must not bind to it.
func TestReconcileDeniesUnauthorizedCrossNamespaceReference(t *testing.T) {
	f := newStandardFixture(
		newGuardrail(consumerNS, nemoName, providerNS),
		newNemo(providerNS, nemoName, nil),
	)
	res := f.reconcile(t, consumerNS)
	requireDenied(t, f, res, consumerNS, "ConsumerNotAuthorized")
}

// TestReconcileLeavesSameNamespaceReferencesUnaffected is the ticket's
// "same-namespace references are unaffected" case: the Same default must not
// become collateral damage of the cross-namespace fence.
func TestReconcileLeavesSameNamespaceReferencesUnaffected(t *testing.T) {
	for _, c := range []struct {
		name              string
		providerNamespace string
	}{
		{"explicit namespace", consumerNS},
		{"omitted namespace defaults to the guardrail's own", ""},
	} {
		t.Run(c.name, func(t *testing.T) {
			f := newStandardFixture(
				newGuardrail(consumerNS, nemoName, c.providerNamespace),
				newNemo(consumerNS, nemoName, nil),
			)
			f.reconcile(t, consumerNS)
			requireCondition(t, f.reload(t, consumerNS), ConditionResolvedRefs, metav1.ConditionTrue, "ReferencesAuthorized")
		})
	}
}

// TestReconcileAcceptsPermittedCrossNamespaceReference is the ticket's
// "a permitted cross-namespace reference is accepted" case, and the only test
// that walks the whole accepted path: all four conditions True, a published
// endpoint and binding revision, and the steady resync interval rather than
// the short one.
//
// Acceptance here is now earned rather than assumed: newNemo publishes a
// status.endpoint, which is what ProviderReady gates on, so removing it makes
// these cases refuse (see the readiness case in
// TestReconcileClearsTheBindingRevisionOnRefusal).
func TestReconcileAcceptsPermittedCrossNamespaceReference(t *testing.T) {
	cases := []struct {
		name   string
		policy map[string]any
		// consumerNamespace is the Namespace object Reconcile reads in
		// Selector mode, and nil in every other mode, where it reads none.
		consumerNamespace *corev1.Namespace
	}{
		{
			name:   "All",
			policy: map[string]any{"from": "All"},
		},
		{
			name:              "Selector matching the consumer Namespace labels",
			policy:            map[string]any{"from": "Selector", "selector": map[string]any{"matchLabels": map[string]any{"guardrails": "shared"}}},
			consumerNamespace: newNamespace(consumerNS, map[string]string{"guardrails": "shared"}),
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			provider := newNemo(providerNS, nemoName, c.policy)
			live := []client.Object{provider}
			if c.consumerNamespace != nil {
				live = append(live, c.consumerNamespace)
			}
			f := newStandardFixture(newGuardrail(consumerNS, nemoName, providerNS), live...)

			res := f.reconcile(t, consumerNS)
			got := f.reload(t, consumerNS)
			requireCondition(t, got, ConditionResolvedRefs, metav1.ConditionTrue, reasonReferencesAuthorized)
			if got.Status.ObservedGeneration != testGeneration {
				t.Fatalf("status.observedGeneration = %d, want %d", got.Status.ObservedGeneration, testGeneration)
			}

			requireCondition(t, got, ConditionCompatible, metav1.ConditionTrue, reasonChecksSatisfiable)
			requireCondition(t, got, ConditionProviderReady, metav1.ConditionTrue, reasonProviderAvailable)
			requireCondition(t, got, ConditionAccepted, metav1.ConditionTrue, reasonPolicyAccepted)

			// The revision is the one computed from the dependencies actually
			// observed, not merely some non-empty string: recomputing it from
			// the same inputs is what a consumer does to check its compiled
			// catalog is still current.
			policy, err := parseAllowedConsumers(provider)
			if err != nil {
				t.Fatalf("parseAllowedConsumers on the test's own provider: %v", err)
			}
			wantRevision := mustComputeRevision(t, provider, nemoEndpoint, policy, c.consumerNamespace)
			if got.Status.BindingRevision != wantRevision {
				t.Fatalf("status.bindingRevision = %q, want %q", got.Status.BindingRevision, wantRevision)
			}

			// Published verbatim, so a consumer compiles against the address
			// this verdict was reached on instead of rebuilding one from the
			// provider's name.
			if got.Status.ProviderEndpoint != nemoEndpoint {
				t.Fatalf("status.providerEndpoint = %q, want %q", got.Status.ProviderEndpoint, nemoEndpoint)
			}

			// An accepted policy comes back on the steady resync, not the
			// short not-ready interval, and announces nothing: Events are for
			// a guardrail that stopped enforcing.
			if res.RequeueAfter != f.reconciler.ResyncInterval {
				t.Fatalf("RequeueAfter = %v, want the steady resync %v", res.RequeueAfter, f.reconciler.ResyncInterval)
			}
			if events := len(f.events); events != 0 {
				t.Fatalf("%d events emitted, want 0 on acceptance", events)
			}
		})
	}
}

// mutateLiveProvider applies mutate to the live NemoGuardrails, the way
// TrustyAI or a cluster admin would: a write to somebody else's object that
// never touches the AIGuardrail and cannot bump its generation.
func mutateLiveProvider(t *testing.T, f *fixture, mutate func(*unstructured.Unstructured)) {
	t.Helper()
	current := NewNemoGuardrails()
	key := types.NamespacedName{Namespace: providerNS, Name: nemoName}
	if err := f.live.Get(context.Background(), key, current); err != nil {
		t.Fatalf("Get provider: %v", err)
	}
	mutate(current)
	if err := f.live.Update(context.Background(), current); err != nil {
		t.Fatalf("Update provider: %v", err)
	}
}

// TestReconcileRepublishesTheBindingRevisionWhenDependenciesMove is the
// end-to-end claim status.bindingRevision makes to a consumer.
//
// Every mutation below happens in another object. The AIGuardrail's spec is
// untouched, so metadata.generation — and therefore status.observedGeneration,
// which each case asserts held — is identical across both reconciles. A
// consumer fencing on generation alone would conclude nothing changed. The
// revision is the only thing that can tell it otherwise, which is the whole
// reason the field exists (02-guardrails-low-level-details.md:1392-1394).
//
// The wantChange=false cases are not padding. Over-reacting is as wrong as
// under-reacting here: a revision that moves on an edit the binding cannot see
// invalidates every downstream Praxis generation for nothing.
func TestReconcileRepublishesTheBindingRevisionWhenDependenciesMove(t *testing.T) {
	allFrom := map[string]any{"from": "All"}
	selectorFrom := map[string]any{
		"from":     "Selector",
		"selector": map[string]any{"matchLabels": map[string]any{"guardrails": "shared"}},
	}
	matchingNamespace := func() *corev1.Namespace {
		return newNamespace(consumerNS, map[string]string{"guardrails": "shared"})
	}
	// The UID participates in the digest, so it has to be set explicitly: the
	// fake client leaves it empty, which would make the delete-and-recreate
	// case pass against two providers that are genuinely indistinguishable.
	providerWithUID := func(uid string, policy map[string]any) *unstructured.Unstructured {
		u := newNemo(providerNS, nemoName, policy)
		u.SetUID(types.UID(uid))
		return u
	}

	cases := []struct {
		name              string
		policy            map[string]any
		consumerNamespace *corev1.Namespace
		mutate            func(t *testing.T, f *fixture)
		wantChange        bool
		// why states the dependency change in the consumer's terms, so a
		// failure says what went unnoticed rather than that two hashes differ.
		why string
	}{
		{
			name:   "the provider declares an additional configuration",
			policy: allFrom,
			mutate: func(t *testing.T, f *fixture) {
				t.Helper()
				mutateLiveProvider(t, f, func(u *unstructured.Unstructured) {
					withNemoConfigs(u, guardrailConfig, "pii-v2")
				})
			},
			wantChange: true,
			why:        "the set of configurations a check may select was edited",
		},
		{
			name:   "the provider was deleted and recreated at the same name",
			policy: allFrom,
			mutate: func(t *testing.T, f *fixture) {
				t.Helper()
				if err := f.live.Delete(context.Background(), newNemo(providerNS, nemoName, allFrom)); err != nil {
					t.Fatalf("Delete provider: %v", err)
				}
				replacement := providerWithUID("00000000-0000-0000-0000-000000000000", allFrom)
				if err := f.live.Create(context.Background(), replacement); err != nil {
					t.Fatalf("Create provider: %v", err)
				}
			},
			wantChange: true,
			why:        "a different object now answers to the referenced name",
		},
		{
			name:              "the permission rule widened from Selector to All",
			policy:            selectorFrom,
			consumerNamespace: matchingNamespace(),
			mutate: func(t *testing.T, f *fixture) {
				t.Helper()
				mutateLiveProvider(t, f, func(u *unstructured.Unstructured) {
					if err := unstructured.SetNestedMap(u.Object, allFrom, "spec", "allowedConsumers", "namespaces"); err != nil {
						t.Fatalf("set allowedConsumers: %v", err)
					}
					unstructured.RemoveNestedField(u.Object, "spec", "allowedConsumers", "namespaces", "selector")
				})
			},
			wantChange: true,
			why:        "the binding is now authorized by a different rule",
		},
		{
			// TrustyAI owns the endpoint and rewrites it on its own
			// reconcile, with nothing in the AIGuardrail or the provider's
			// spec moving. A relocated server has to invalidate the binding,
			// because the compiled catalog points at the old address.
			name:   "the provider republishes a different endpoint",
			policy: allFrom,
			mutate: func(t *testing.T, f *fixture) {
				t.Helper()
				mutateLiveProvider(t, f, func(u *unstructured.Unstructured) {
					if err := unstructured.SetNestedField(u.Object,
						"https://"+nemoName+".relocated.svc.cluster.local", "status", "endpoint"); err != nil {
						t.Fatalf("set status.endpoint: %v", err)
					}
				})
			},
			wantChange: true,
			why:        "the server answering checks moved to a different address",
		},
		{
			name:              "a label the selector does not read is added",
			policy:            selectorFrom,
			consumerNamespace: matchingNamespace(),
			mutate: func(t *testing.T, f *fixture) {
				t.Helper()
				var ns corev1.Namespace
				if err := f.live.Get(context.Background(), types.NamespacedName{Name: consumerNS}, &ns); err != nil {
					t.Fatalf("Get consumer Namespace: %v", err)
				}
				ns.Labels["istio-injection"] = "enabled"
				if err := f.live.Update(context.Background(), &ns); err != nil {
					t.Fatalf("Update consumer Namespace: %v", err)
				}
			},
			wantChange: false,
			why:        "nothing the binding depends on changed",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			live := []client.Object{providerWithUID(nemoUID, c.policy)}
			if c.consumerNamespace != nil {
				live = append(live, c.consumerNamespace)
			}
			f := newStandardFixture(newGuardrail(consumerNS, nemoName, providerNS), live...)

			f.reconcile(t, consumerNS)
			before := f.reload(t, consumerNS)
			requireCondition(t, before, ConditionAccepted, metav1.ConditionTrue, reasonPolicyAccepted)
			if before.Status.BindingRevision == "" {
				t.Fatal("no binding revision was published on the first accepted reconcile")
			}

			c.mutate(t, f)
			f.reconcile(t, consumerNS)
			after := f.reload(t, consumerNS)

			// Each mutation is meant to leave the binding accepted, so a
			// changed revision is read as "recompile", never as "stop".
			requireCondition(t, after, ConditionAccepted, metav1.ConditionTrue, reasonPolicyAccepted)
			if after.Generation != before.Generation ||
				after.Status.ObservedGeneration != before.Status.ObservedGeneration {
				t.Fatalf("generation moved from %d/%d to %d/%d; the mutation edited the policy rather than its dependencies, "+
					"so this no longer tests what the revision adds over observedGeneration",
					before.Generation, before.Status.ObservedGeneration,
					after.Generation, after.Status.ObservedGeneration)
			}

			switch {
			case c.wantChange && after.Status.BindingRevision == before.Status.BindingRevision:
				t.Fatalf("status.bindingRevision stayed %q, but %s; a consumer gating on it keeps serving a stale catalog",
					after.Status.BindingRevision, c.why)
			case !c.wantChange && after.Status.BindingRevision != before.Status.BindingRevision:
				t.Fatalf("status.bindingRevision moved from %q to %q, but %s; this rolls every downstream generation for nothing",
					before.Status.BindingRevision, after.Status.BindingRevision, c.why)
			}
		})
	}
}

// TestReconcileDeniesSelectorModeWithoutAMatch covers the ways Selector mode
// must refuse. The unreadable-Namespace case matters most: the reconciler
// logs and carries on with a nil Namespace rather than returning an error, so
// without this test a refactor could turn "could not read the labels" into
// "no labels to fail against" and let the binding through.
func TestReconcileDeniesSelectorModeWithoutAMatch(t *testing.T) {
	selector := map[string]any{"from": "Selector", "selector": map[string]any{"matchLabels": map[string]any{"guardrails": "shared"}}}
	cases := []struct {
		name         string
		namespaceObj client.Object
	}{
		{"consumer Namespace labels do not match", newNamespace(consumerNS, map[string]string{"guardrails": "private"})},
		{"consumer Namespace carries no labels", newNamespace(consumerNS, nil)},
		{"consumer Namespace does not exist", nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			live := []client.Object{newNemo(providerNS, nemoName, selector)}
			if c.namespaceObj != nil {
				live = append(live, c.namespaceObj)
			}
			f := newStandardFixture(newGuardrail(consumerNS, nemoName, providerNS), live...)
			res := f.reconcile(t, consumerNS)
			requireDenied(t, f, res, consumerNS, "ConsumerNotAuthorized")
		})
	}
}

// TestReconcileDeniesSelectorOnLabelsOfTheWrongObject pins the consumer
// identity: the selector matches labels on the consuming Namespace object,
// never labels the author put on their own AIGuardrail. If it matched the
// latter, any namespace could self-authorize with a kubectl label.
func TestReconcileDeniesSelectorOnLabelsOfTheWrongObject(t *testing.T) {
	guardrail := newGuardrail(consumerNS, nemoName, providerNS)
	guardrail.Labels = map[string]string{"guardrails": "shared"}
	f := newStandardFixture(
		guardrail,
		newNemo(providerNS, nemoName, map[string]any{
			"from":     "Selector",
			"selector": map[string]any{"matchLabels": map[string]any{"guardrails": "shared"}},
		}),
		newNamespace(consumerNS, nil),
	)
	res := f.reconcile(t, consumerNS)
	requireDenied(t, f, res, consumerNS, "ConsumerNotAuthorized")
}

func TestReconcileDeniesInvalidAllowedConsumers(t *testing.T) {
	// A policy this controller cannot parse is a policy whose intent it does
	// not know, which has to deny. These are reconcile-level spot checks; the
	// full matrix lives in allowedconsumers_test.go.
	cases := []struct {
		name   string
		policy map[string]any
	}{
		{"Selector with no selector", map[string]any{"from": "Selector"}},
		{"unknown mode", map[string]any{"from": "Everyone"}},
		{"selector forbidden under All", map[string]any{"from": "All", "selector": map[string]any{"matchLabels": map[string]any{"a": "b"}}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := newStandardFixture(
				newGuardrail(consumerNS, nemoName, providerNS),
				newNemo(providerNS, nemoName, c.policy),
			)
			res := f.reconcile(t, consumerNS)
			requireDenied(t, f, res, consumerNS, "InvalidAllowedConsumers")
		})
	}
}

// TestReconcileRefusesSubMillisecondTimeout covers a timeout Praxis would
// render as timeout_ms 0. The provider is ready; the policy is still refused
// before that endpoint is published, and 1ms remains acceptable.
func TestReconcileRefusesSubMillisecondTimeout(t *testing.T) {
	guardrail := newGuardrail(consumerNS, nemoName, providerNS)
	guardrail.Spec.Provider.Timeout.Duration = 500 * time.Microsecond
	f := newStandardFixture(guardrail, newNemo(providerNS, nemoName, map[string]any{"from": "All"}))

	res := f.reconcile(t, consumerNS)
	got := f.reload(t, consumerNS)
	requireCondition(t, got, ConditionAccepted, metav1.ConditionFalse, reasonTimeoutBelowMinimum)
	if got.Status.BindingRevision != "" || got.Status.ProviderEndpoint != "" {
		t.Fatalf("revision %q endpoint %q, want both empty on a refused timeout", got.Status.BindingRevision, got.Status.ProviderEndpoint)
	}
	if res.RequeueAfter != f.reconciler.ResyncInterval {
		t.Fatalf("RequeueAfter = %v, want the resync interval %v", res.RequeueAfter, f.reconciler.ResyncInterval)
	}
	if events := len(f.events); events != 1 {
		t.Fatalf("%d events emitted, want 1 for the transition to refused", events)
	}

	accepted := newGuardrail(consumerNS, nemoName, providerNS)
	accepted.Spec.Provider.Timeout.Duration = time.Millisecond
	f = newStandardFixture(accepted, newNemo(providerNS, nemoName, map[string]any{"from": "All"}))
	f.reconcile(t, consumerNS)
	got = f.reload(t, consumerNS)
	requireCondition(t, got, ConditionAccepted, metav1.ConditionTrue, reasonPolicyAccepted)
	if got.Status.ProviderEndpoint == "" {
		t.Fatal("1ms timeout was refused")
	}
}

// TestReconcileRefusesChecksTheProviderCannotSatisfy covers the Compatible
// gate end to end, and pins two things the unit tests cannot see.
//
// First, the reference still resolves: ResolvedRefs stays True, because the
// provider exists and authorizes this namespace. The proposal reserves
// ResolvedRefs=False for permission denial, so reporting a configId typo there
// would tell the author their binding was refused rather than that their check
// names a configuration nobody declared.
//
// Second, an incompatible policy is refused even though the provider reports
// ready, so the compatibility gate stands on its own rather than riding on a
// readiness failure.
func TestReconcileRefusesChecksTheProviderCannotSatisfy(t *testing.T) {
	f := newStandardFixture(
		newGuardrail(consumerNS, nemoName, providerNS),
		withNemoConfigs(newNemo(providerNS, nemoName, map[string]any{"from": "All"}), "some-other-config"),
	)

	res := f.reconcile(t, consumerNS)
	got := f.reload(t, consumerNS)

	requireCondition(t, got, ConditionResolvedRefs, metav1.ConditionTrue, reasonReferencesAuthorized)
	requireCondition(t, got, ConditionCompatible, metav1.ConditionFalse, reasonUnknownCheckConfig)
	requireCondition(t, got, ConditionAccepted, metav1.ConditionFalse, reasonUnknownCheckConfig)

	// Readiness is published even though the compatibility gate is what
	// refuses, so status reports the provider's full state rather than
	// stopping at the first failure.
	requireCondition(t, got, ConditionProviderReady, metav1.ConditionTrue, reasonProviderAvailable)

	// No revision for a refused binding, however ready the provider is.
	if got.Status.BindingRevision != "" {
		t.Errorf("status.bindingRevision = %q, want it empty on a refused binding", got.Status.BindingRevision)
	}

	// The message has to name the offending configId; "incompatible" alone
	// does not tell an author which of up to 64 checks to fix.
	compatible := apimeta.FindStatusCondition(got.Status.Conditions, ConditionCompatible)
	if !strings.Contains(compatible.Message, guardrailConfig) {
		t.Errorf("Compatible message %q does not name the undeclared configId %q", compatible.Message, guardrailConfig)
	}

	if len(got.Spec.Checks) != 1 {
		t.Fatalf("spec.checks = %#v, want the declared checks left untouched", got.Spec.Checks)
	}
	if res.RequeueAfter != f.reconciler.ResyncInterval {
		t.Fatalf("RequeueAfter = %v, want the resync interval %v", res.RequeueAfter, f.reconciler.ResyncInterval)
	}
	if got := len(f.events); got != 1 {
		t.Fatalf("%d events emitted, want 1 for the transition to refused", got)
	}
	if event := <-f.events; !strings.Contains(event, reasonUnknownCheckConfig) {
		t.Errorf("event = %q, want it to carry reason %q", event, reasonUnknownCheckConfig)
	}
}

// TestReconcileRefusesProvidersWithUnusableConfigs covers the two remaining
// compatibility verdicts at reconcile level: a provider that declares nothing,
// and one whose declarations cannot be parsed. Both leave the reference
// resolved and refuse on Accepted with the compatibility reason.
func TestReconcileRefusesProvidersWithUnusableConfigs(t *testing.T) {
	cases := []struct {
		name     string
		provider func() *unstructured.Unstructured
		want     string
	}{
		{
			name: "provider declares no configurations",
			provider: func() *unstructured.Unstructured {
				return withNemoConfigs(newNemo(providerNS, nemoName, map[string]any{"from": "All"}))
			},
			want: reasonProviderConfigsUnavailable,
		},
		{
			name: "provider configurations cannot be parsed",
			provider: func() *unstructured.Unstructured {
				u := newNemo(providerNS, nemoName, map[string]any{"from": "All"})
				if err := unstructured.SetNestedSlice(u.Object, []any{int64(7)}, "spec", "nemoConfigs"); err != nil {
					t.Fatalf("plant a malformed spec.nemoConfigs: %v", err)
				}
				return u
			},
			want: reasonInvalidProviderConfigs,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := newStandardFixture(newGuardrail(consumerNS, nemoName, providerNS), c.provider())

			res := f.reconcile(t, consumerNS)
			got := f.reload(t, consumerNS)

			requireCondition(t, got, ConditionResolvedRefs, metav1.ConditionTrue, reasonReferencesAuthorized)
			requireCondition(t, got, ConditionCompatible, metav1.ConditionFalse, c.want)
			requireCondition(t, got, ConditionAccepted, metav1.ConditionFalse, c.want)
			if res.RequeueAfter != f.reconciler.ResyncInterval {
				t.Fatalf("RequeueAfter = %v, want the resync interval %v", res.RequeueAfter, f.reconciler.ResyncInterval)
			}
		})
	}
}

// TestReconcileClearsTheBindingRevisionOnRefusal covers the fail-closed half
// of the revision contract. A consumer gates on Accepted=True together with a
// matching binding revision, so a revision left behind from the last time the
// policy was accepted is one it could still match against — exactly when the
// binding has just stopped being valid.
//
// Each case starts from a status that already carries a revision, which is
// how a policy that was accepted and then broke actually looks.
func TestReconcileClearsTheBindingRevisionOnRefusal(t *testing.T) {
	const staleRevision = "9f2c4ae1b73d085c6d4f2a1e8b70c395ad62fe41c0783b9e5d6a2f14cb87e03d"

	cases := []struct {
		name     string
		provider []client.Object
	}{
		{
			name: "the provider is gone",
		},
		{
			name:     "the provider no longer authorizes this namespace",
			provider: []client.Object{newNemo(providerNS, nemoName, map[string]any{"from": "Same"})},
		},
		{
			name: "the provider no longer declares the selected configuration",
			provider: []client.Object{
				withNemoConfigs(newNemo(providerNS, nemoName, map[string]any{"from": "All"}), "some-other-config"),
			},
		},
		{
			// The readiness refusal, and the one most likely to fire in
			// production: the reference resolves, the namespace is
			// authorized and the configuration is declared, so only
			// discovery fails. It doubles as the proof that the acceptance
			// tests above are earned — they share this provider and differ
			// only by having an endpoint.
			name: "the provider no longer publishes an endpoint",
			provider: []client.Object{
				withoutNemoEndpoint(newNemo(providerNS, nemoName, map[string]any{"from": "All"})),
			},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			guardrail := newGuardrail(consumerNS, nemoName, providerNS)
			guardrail.Status.BindingRevision = staleRevision
			guardrail.Status.ProviderEndpoint = nemoEndpoint

			f := newStandardFixture(guardrail, c.provider...)
			f.reconcile(t, consumerNS)

			got := f.reload(t, consumerNS)
			if apimeta.IsStatusConditionTrue(got.Status.Conditions, ConditionAccepted) {
				t.Fatalf("Accepted is True; this case must refuse for the test to mean anything")
			}
			if got.Status.BindingRevision != "" {
				t.Fatalf("status.bindingRevision = %q, want it cleared; a refused binding must not leave "+
					"a revision a consumer can still match", got.Status.BindingRevision)
			}
			if got.Status.ProviderEndpoint != "" {
				t.Fatalf("status.providerEndpoint = %q, want it cleared; a refused binding must not leave "+
					"an address a consumer can still compile against", got.Status.ProviderEndpoint)
			}
		})
	}
}

func TestReconcileDeniesWhenProviderIsMissing(t *testing.T) {
	f := newStandardFixture(newGuardrail(consumerNS, nemoName, providerNS))
	res := f.reconcile(t, consumerNS)
	requireDenied(t, f, res, consumerNS, "ProviderNotFound")
}

// TestReconcileRefusesEveryPolicyWhenTheProviderCRDIsAbsent covers a cluster
// TrustyAI was never installed on. SetupWithManager registers the reconciler
// there anyway: skipping registration leaves every AIGuardrail with no
// conditions at all, which a consumer cannot tell apart from a controller
// that crashed before reaching it.
func TestReconcileRefusesEveryPolicyWhenTheProviderCRDIsAbsent(t *testing.T) {
	// A provider that exists and authorizes this namespace, so the missing
	// CRD is the only thing that can be producing the refusal.
	f := newStandardFixture(
		newGuardrail(consumerNS, nemoName, providerNS),
		newNemo(providerNS, nemoName, map[string]any{"from": "All"}),
	)
	f.reconciler.RESTMapper = mapperWithoutNemo()

	res := f.reconcile(t, consumerNS)
	// The slow cadence, not the ten-second one every other refusal uses:
	// each miss costs the RESTMapper a discovery reload.
	if res.RequeueAfter != f.reconciler.ResyncInterval {
		t.Errorf("RequeueAfter = %v, want the resync interval %v", res.RequeueAfter, f.reconciler.ResyncInterval)
	}

	got := f.reload(t, consumerNS)
	requireCondition(t, got, ConditionResolvedRefs, metav1.ConditionFalse, reasonProviderCRDNotInstalled)
	requireCondition(t, got, ConditionAccepted, metav1.ConditionFalse, reasonProviderCRDNotInstalled)
	requireCondition(t, got, ConditionCompatible, metav1.ConditionUnknown, reasonProviderCRDNotInstalled)
	requireCondition(t, got, ConditionProviderReady, metav1.ConditionUnknown, reasonProviderCRDNotInstalled)
}

// TestReconcileRecoversWhenTheProviderCRDIsInstalledLater is the reason the
// CRD check is re-asked per reconcile instead of being cached at startup.
// Installing TrustyAI after this controller is already running must accept
// the policy on the next pass, not leave it refused until someone notices and
// restarts the pod.
//
// Only the provider watch still needs a restart, and that is a latency
// difference rather than a correctness one: the requeue this refusal schedules
// is what brings the policy back.
func TestReconcileRecoversWhenTheProviderCRDIsInstalledLater(t *testing.T) {
	f := newStandardFixture(
		newGuardrail(consumerNS, nemoName, providerNS),
		newNemo(providerNS, nemoName, map[string]any{"from": "All"}),
	)
	f.reconciler.RESTMapper = mapperWithoutNemo()
	f.reconcile(t, consumerNS)
	requireCondition(t, f.reload(t, consumerNS), ConditionAccepted, metav1.ConditionFalse, reasonProviderCRDNotInstalled)

	// TrustyAI is installed. Nothing else about the cluster changes, and this
	// controller is not restarted.
	f.reconciler.RESTMapper = mapperWithNemo()

	res := f.reconcile(t, consumerNS)
	if res.RequeueAfter != f.reconciler.ResyncInterval {
		t.Errorf("RequeueAfter = %v, want the steady-state resync %v", res.RequeueAfter, f.reconciler.ResyncInterval)
	}
	requireCondition(t, f.reload(t, consumerNS), ConditionAccepted, metav1.ConditionTrue, reasonPolicyAccepted)
}

// TestReconcileDeniesWhenProviderCannotBeRead keeps a transient API failure
// on the deny path rather than the error path: a read failure is not evidence
// that the reference is authorized, so the binding must come down while the
// controller retries.
// TestReconcileKeepsAnAcceptedBindingWhenTheProviderReadFails proves a failed
// read is not a verdict. The provider is unchanged and still authorizes this
// namespace; only the read breaks. Denying would clear bindingRevision and
// providerEndpoint, retracting a working binding on an API server blip, so the
// error is surfaced for backoff and the last verdict is left standing.
func TestReconcileKeepsAnAcceptedBindingWhenTheProviderReadFails(t *testing.T) {
	// Flipped between the two reconciles; both run on this goroutine, with the
	// interceptor called synchronously inside Reconcile.
	failRead := false
	f := newFixture(fixtureOptions{
		cacheObjects: []client.Object{newGuardrail(consumerNS, nemoName, providerNS)},
		liveObjects:  []client.Object{newNemo(providerNS, nemoName, map[string]any{"from": "All"})},
		liveFuncs: interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if failRead && obj.GetObjectKind().GroupVersionKind().Kind == NemoGuardrailsGVK.Kind {
					return apierrors.NewInternalError(errors.New("etcd unavailable"))
				}
				return c.Get(ctx, key, obj, opts...)
			},
		},
	})

	f.reconcile(t, consumerNS)
	accepted := f.reload(t, consumerNS)
	requireCondition(t, accepted, ConditionAccepted, metav1.ConditionTrue, reasonPolicyAccepted)
	if accepted.Status.BindingRevision == "" {
		t.Fatal("precondition: the binding was never accepted")
	}

	failRead = true
	if _, err := f.reconciler.Reconcile(context.Background(), guardrailRequest(consumerNS)); err == nil {
		t.Fatal("Reconcile returned no error, want the failed read surfaced so the manager backs off")
	}

	got := f.reload(t, consumerNS)
	requireCondition(t, got, ConditionAccepted, metav1.ConditionTrue, reasonPolicyAccepted)
	if got.Status.BindingRevision != accepted.Status.BindingRevision {
		t.Errorf("bindingRevision = %q, want the accepted one %q left standing",
			got.Status.BindingRevision, accepted.Status.BindingRevision)
	}
	if got.Status.ProviderEndpoint != accepted.Status.ProviderEndpoint {
		t.Errorf("providerEndpoint = %q, want the accepted one %q left standing",
			got.Status.ProviderEndpoint, accepted.Status.ProviderEndpoint)
	}
}

// TestReconcileKeepsAnAcceptedBindingWhenTheNamespaceReadFails is the same
// argument for the other authorization input: a Namespace whose labels could
// not be read is not a Namespace that stopped matching.
func TestReconcileKeepsAnAcceptedBindingWhenTheNamespaceReadFails(t *testing.T) {
	selector := map[string]any{"from": "Selector", "selector": map[string]any{"matchLabels": map[string]any{"guardrails": "shared"}}}
	failRead := false
	f := newFixture(fixtureOptions{
		cacheObjects: []client.Object{newGuardrail(consumerNS, nemoName, providerNS)},
		liveObjects: []client.Object{
			newNemo(providerNS, nemoName, selector),
			newNamespace(consumerNS, map[string]string{"guardrails": "shared"}),
		},
		liveFuncs: interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if _, isNamespace := obj.(*corev1.Namespace); failRead && isNamespace {
					return apierrors.NewInternalError(errors.New("etcd unavailable"))
				}
				return c.Get(ctx, key, obj, opts...)
			},
		},
	})

	f.reconcile(t, consumerNS)
	accepted := f.reload(t, consumerNS)
	requireCondition(t, accepted, ConditionAccepted, metav1.ConditionTrue, reasonPolicyAccepted)

	failRead = true
	if _, err := f.reconciler.Reconcile(context.Background(), guardrailRequest(consumerNS)); err == nil {
		t.Fatal("Reconcile returned no error, want the failed read surfaced so the manager backs off")
	}

	got := f.reload(t, consumerNS)
	requireCondition(t, got, ConditionAccepted, metav1.ConditionTrue, reasonPolicyAccepted)
	if got.Status.BindingRevision != accepted.Status.BindingRevision {
		t.Errorf("bindingRevision = %q, want the accepted one %q left standing",
			got.Status.BindingRevision, accepted.Status.BindingRevision)
	}
}

// TestReconcileReadsTheProviderLive proves authorization does not come from
// the informer cache. The cache still holds a provider that allowed everyone;
// the API server already holds the revoked one. A cache-backed read would
// keep the binding alive for up to a full resync after revocation.
func TestReconcileReadsTheProviderLive(t *testing.T) {
	f := newFixture(fixtureOptions{
		cacheObjects: []client.Object{
			newGuardrail(consumerNS, nemoName, providerNS),
			newNemo(providerNS, nemoName, map[string]any{"from": "All"}),
		},
		liveObjects: []client.Object{newNemo(providerNS, nemoName, map[string]any{"from": "Same"})},
	})
	res := f.reconcile(t, consumerNS)
	requireDenied(t, f, res, consumerNS, "ConsumerNotAuthorized")
}

// TestReconcileReadsTheConsumerNamespaceLive is the same argument for the
// other authorization input: a Namespace whose label was just removed must
// stop matching immediately, not at the next cache sync.
func TestReconcileReadsTheConsumerNamespaceLive(t *testing.T) {
	selector := map[string]any{"from": "Selector", "selector": map[string]any{"matchLabels": map[string]any{"guardrails": "shared"}}}
	f := newFixture(fixtureOptions{
		cacheObjects: []client.Object{
			newGuardrail(consumerNS, nemoName, providerNS),
			newNamespace(consumerNS, map[string]string{"guardrails": "shared"}),
		},
		liveObjects: []client.Object{
			newNemo(providerNS, nemoName, selector),
			newNamespace(consumerNS, map[string]string{"guardrails": "private"}),
		},
	})
	res := f.reconcile(t, consumerNS)
	requireDenied(t, f, res, consumerNS, "ConsumerNotAuthorized")
}

// TestReconcileEmitsOneEventPerTransition guards the gating on
// SetStatusCondition's changed bool. A denied policy is requeued every resync
// interval, so an ungated emit would post an Event on every pass forever.
func TestReconcileEmitsOneEventPerTransition(t *testing.T) {
	f := newStandardFixture(
		newGuardrail(consumerNS, nemoName, providerNS),
		newNemo(providerNS, nemoName, nil),
	)

	f.reconcile(t, consumerNS)
	if got := len(f.events); got != 1 {
		t.Fatalf("after the first reconcile there are %d events, want 1", got)
	}
	event := <-f.events
	if !strings.Contains(event, "ConsumerNotAuthorized") {
		t.Fatalf("event = %q, want it to name the ConsumerNotAuthorized reason", event)
	}

	// Steady state: same verdict, no new event.
	for i := range 3 {
		f.reconcile(t, consumerNS)
		if got := len(f.events); got != 0 {
			t.Fatalf("requeue %d emitted %d further events, want 0 while the verdict is unchanged", i+1, got)
		}
	}

	// A genuine transition emits again. The replacement provider authorizes
	// every namespace but declares a configuration this policy's check does
	// not select, so Accepted stays False and moves from
	// ConsumerNotAuthorized to UnknownCheckConfig. It has to land on another
	// refusal rather than on acceptance: acceptance emits nothing, which
	// would prove the gating works for the wrong reason.
	if err := f.live.Delete(context.Background(), newNemo(providerNS, nemoName, nil)); err != nil {
		t.Fatalf("Delete provider: %v", err)
	}
	authorizedButIncompatible := withNemoConfigs(
		newNemo(providerNS, nemoName, map[string]any{"from": "All"}), "some-other-config")
	if err := f.live.Create(context.Background(), authorizedButIncompatible); err != nil {
		t.Fatalf("Create provider: %v", err)
	}
	f.reconcile(t, consumerNS)
	if got := len(f.events); got != 1 {
		t.Fatalf("the transition to %s emitted %d events, want 1", reasonUnknownCheckConfig, got)
	}
	if event := <-f.events; !strings.Contains(event, reasonUnknownCheckConfig) {
		t.Fatalf("event = %q, want it to name the %s reason", event, reasonUnknownCheckConfig)
	}
}

// TestReconcileDoesNotEmitWhenTheStatusPatchFails keeps events honest: an
// Event is a claim that a verdict was published, so it must not outlive a
// patch that never landed.
func TestReconcileDoesNotEmitWhenTheStatusPatchFails(t *testing.T) {
	f := newFixture(fixtureOptions{
		cacheObjects: []client.Object{newGuardrail(consumerNS, nemoName, providerNS)},
		liveObjects:  []client.Object{newNemo(providerNS, nemoName, nil)},
		cacheFuncs: interceptor.Funcs{
			SubResourcePatch: func(_ context.Context, _ client.Client, _ string, _ client.Object, _ client.Patch, _ ...client.SubResourcePatchOption) error {
				return apierrors.NewInternalError(errors.New("status write rejected"))
			},
		},
	})
	if _, err := f.reconciler.Reconcile(context.Background(), guardrailRequest(consumerNS)); err == nil {
		t.Fatal("Reconcile succeeded, want the status patch error surfaced for retry")
	}
	if got := len(f.events); got != 0 {
		t.Fatalf("%d events emitted, want 0 when the status patch failed", got)
	}
}

// TestReconcileRequeuesOnStatusConflict covers both patch sites: a conflict
// means someone else wrote the status first, so the right answer is to re-read
// and retry, not to report an error.
func TestReconcileRequeuesOnStatusConflict(t *testing.T) {
	conflict := interceptor.Funcs{
		SubResourcePatch: func(_ context.Context, _ client.Client, _ string, obj client.Object, _ client.Patch, _ ...client.SubResourcePatchOption) error {
			return apierrors.NewConflict(schema.GroupResource{Group: "aigateway.opendatahub.io", Resource: "aiguardrails"}, obj.GetName(), errors.New("object was modified"))
		},
	}
	cases := []struct {
		name   string
		policy map[string]any
	}{
		{"on the deny path", nil},
		{"on the authorized-but-unready path", map[string]any{"from": "All"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := newFixture(fixtureOptions{
				cacheObjects: []client.Object{newGuardrail(consumerNS, nemoName, providerNS)},
				liveObjects:  []client.Object{newNemo(providerNS, nemoName, c.policy)},
				cacheFuncs:   conflict,
			})
			res := f.reconcile(t, consumerNS)
			if res != (ctrl.Result{Requeue: true}) {
				t.Fatalf("Result = %#v, want an immediate requeue on a status conflict", res)
			}
			if got := len(f.events); got != 0 {
				t.Fatalf("%d events emitted, want 0 when the status patch conflicted", got)
			}
		})
	}
}

// TestReconcileStampsEveryConditionWithTheCurrentGeneration re-asserts the
// generation fence against a status left over from an older spec. A verdict
// that still carried generation 2 would be read as current by any consumer
// using apimeta.IsStatusConditionTrue, which compares only Type and Status.
func TestReconcileStampsEveryConditionWithTheCurrentGeneration(t *testing.T) {
	guardrail := newGuardrail(consumerNS, nemoName, providerNS)
	guardrail.Status = aigatewayv1alpha1.AIGuardrailStatus{
		ObservedGeneration: testGeneration - 1,
		Conditions: []metav1.Condition{
			{
				Type:               ConditionResolvedRefs,
				Status:             metav1.ConditionTrue,
				ObservedGeneration: testGeneration - 1,
				Reason:             "ReferencesAuthorized",
				LastTransitionTime: metav1.Now(),
			},
			{
				Type:               ConditionAccepted,
				Status:             metav1.ConditionTrue,
				ObservedGeneration: testGeneration - 1,
				Reason:             "Accepted",
				LastTransitionTime: metav1.Now(),
			},
		},
	}
	f := newStandardFixture(guardrail, newNemo(providerNS, nemoName, nil))

	res := f.reconcile(t, consumerNS)
	got := requireDenied(t, f, res, consumerNS, "ConsumerNotAuthorized")
	if got.Status.ObservedGeneration != testGeneration {
		t.Fatalf("status.observedGeneration = %d, want %d", got.Status.ObservedGeneration, testGeneration)
	}
	for _, cond := range got.Status.Conditions {
		if cond.ObservedGeneration != testGeneration {
			t.Fatalf("condition %s kept observedGeneration %d, want %d", cond.Type, cond.ObservedGeneration, testGeneration)
		}
	}
}

// TestReconcileRetractsCheckVerdictsWhenTheBindingIsDenied covers the case the
// generation fence cannot: a policy accepted at generation N whose provider is
// then deleted or whose permission is revoked. Neither edit touches this
// policy's spec, so metadata.generation stays at N and a leftover
// Compatible=True / ProviderReady=True would still match it — reporting a
// missing or unauthorized provider as ready and compatible to any consumer
// that reads those conditions independently of Accepted.
//
// Unknown rather than False because the deny path returns before either
// verdict is evaluated: there is no provider to judge, which is not the same
// as having judged it and found it wanting.
func TestReconcileRetractsCheckVerdictsWhenTheBindingIsDenied(t *testing.T) {
	accepted := func() []metav1.Condition {
		conditions := make([]metav1.Condition, 0, 4)
		for _, c := range []struct{ conditionType, reason string }{
			{ConditionResolvedRefs, "ReferencesAuthorized"},
			{ConditionCompatible, "ChecksSatisfiable"},
			{ConditionProviderReady, "ProviderAvailable"},
			{ConditionAccepted, "Accepted"},
		} {
			conditions = append(conditions, metav1.Condition{
				Type:   c.conditionType,
				Status: metav1.ConditionTrue,
				// The current generation, not an older one: that is what makes
				// these leftovers invisible to the staleness check.
				ObservedGeneration: testGeneration,
				Reason:             c.reason,
				LastTransitionTime: metav1.Now(),
			})
		}
		return conditions
	}

	cases := []struct {
		name string
		// objects is the cluster state besides the AIGuardrail itself.
		objects []client.Object
		// wantReason is the Accepted reason the denial publishes.
		wantReason string
	}{
		{
			name:       "the provider was deleted",
			wantReason: "ProviderNotFound",
		},
		{
			name:       "the provider revoked this namespace's permission",
			objects:    []client.Object{newNemo(providerNS, nemoName, nil)},
			wantReason: "ConsumerNotAuthorized",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			guardrail := newGuardrail(consumerNS, nemoName, providerNS)
			guardrail.Status = aigatewayv1alpha1.AIGuardrailStatus{
				ObservedGeneration: testGeneration,
				Conditions:         accepted(),
				BindingRevision:    "stale-revision",
				ProviderEndpoint:   nemoEndpoint,
			}

			f := newStandardFixture(guardrail, c.objects...)
			res := f.reconcile(t, consumerNS)
			got := requireDenied(t, f, res, consumerNS, c.wantReason)

			for _, conditionType := range []string{ConditionCompatible, ConditionProviderReady} {
				cond := apimeta.FindStatusCondition(got.Status.Conditions, conditionType)
				if cond == nil {
					t.Fatalf("%s is missing; a denied binding must not leave the last verdict standing", conditionType)
				}
				if cond.Status != metav1.ConditionUnknown {
					t.Errorf("%s = %s with reason %q, want Unknown: the provider was never evaluated on this path",
						conditionType, cond.Status, cond.Reason)
				}
				if cond.ObservedGeneration != testGeneration {
					t.Errorf("%s kept observedGeneration %d, want %d", conditionType, cond.ObservedGeneration, testGeneration)
				}
			}
		})
	}
}

// guardrailCRDPath is the generated CRD for the type this package reconciles.
// It is read rather than imported because the printer columns only exist in
// the generated YAML — the kubebuilder markers that produce them are comments,
// invisible to the compiler and to every other test in the repo.
const guardrailCRDPath = "../../config/self/crd/bases/aigateway.opendatahub.io_aiguardrails.yaml"

// conditionColumnPattern extracts X from a printer column JSONPath of the form
// .status.conditions[?(@.type=="X")].status
var conditionColumnPattern = regexp.MustCompile(`\.status\.conditions\[\?\(@\.type=="([^"]+)"\)\]`)

// publishedConditionTypes is the set of condition types Reconcile actually
// writes to status.
//
// This is deliberately not the set of Condition* constants declared in
// constants.go. A constant may be declared well before anything sets it, and
// checking against the constants would admit a printer column for such a type
// — one that renders empty on every object, the exact defect this guards
// against. Add a type here in the same change that makes Reconcile set it.
var publishedConditionTypes = map[string]bool{
	ConditionAccepted:      true,
	ConditionResolvedRefs:  true,
	ConditionProviderReady: true,
	ConditionCompatible:    true,
}

// TestPrinterColumnsReferenceOnlyPublishedConditions guards a silent failure:
// a printer column may name any condition type at all, and one naming a type
// no controller publishes renders permanently blank in kubectl output. Nothing
// else catches it — controller-gen generates it happily, the API server serves
// it happily, and no other test in the repo reads a generated CRD.
//
// The original Ready column on this type was exactly that.
func TestPrinterColumnsReferenceOnlyPublishedConditions(t *testing.T) {
	crd, err := os.ReadFile(guardrailCRDPath)
	if err != nil {
		t.Fatalf("read the generated AIGuardrail CRD: %v", err)
	}

	matches := conditionColumnPattern.FindAllStringSubmatch(string(crd), -1)
	if len(matches) == 0 {
		// Not "nothing to check": either the columns were dropped or this test
		// is reading the wrong file, and both make it vacuously pass forever.
		t.Fatalf("no condition-based printer columns found in %s; the columns were removed or the path is stale", guardrailCRDPath)
	}

	for _, match := range matches {
		conditionType := match[1]
		if !publishedConditionTypes[conditionType] {
			t.Errorf("printer column selects condition type %q, which Reconcile never sets; "+
				"the column will be empty on every AIGuardrail", conditionType)
		}
	}
}
