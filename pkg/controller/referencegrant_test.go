package controller

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	v1alpha1 "github.com/opendatahub-io/ai-gateway-controller/api/inference/v1alpha1"
	"github.com/opendatahub-io/ai-gateway-controller/pkg/envelope"
	"github.com/opendatahub-io/ai-gateway-controller/pkg/resolver"
	"github.com/opendatahub-io/ai-gateway-controller/pkg/tenant"
)

// referenceGrant permits ExternalModels of fromNamespace to reference the
// named ExternalProvider of namespace, or every one when provider is empty.
func referenceGrant(namespace, fromNamespace, provider string) *unstructured.Unstructured {
	to := map[string]any{"group": "inference.opendatahub.io", "kind": "ExternalProvider"}
	if provider != "" {
		to["name"] = provider
	}
	grant := newReferenceGrant()
	grant.SetName("models-from-" + fromNamespace)
	grant.SetNamespace(namespace)
	grant.Object["spec"] = map[string]any{
		"from": []any{map[string]any{"group": "inference.opendatahub.io", "kind": "ExternalModel", "namespace": fromNamespace}},
		"to":   []any{to},
	}
	return grant
}

func referenceProvider(model *v1alpha1.ExternalModel, namespace string) *v1alpha1.ExternalModel {
	model.Spec.ExternalProviderRefs[0].Ref.Namespace = namespace
	return model
}

func readyMessage(model *v1alpha1.ExternalModel) string {
	if condition := apiMeta.FindStatusCondition(model.Status.Conditions, conditionReady); condition != nil {
		return condition.Message
	}
	return ""
}

func foreignRoute(modelNamespace, providerNamespace string) resolver.Route {
	return resolver.Route{Namespace: modelNamespace, ProviderNamespace: providerNamespace, Provider: "openai"}
}

var serviceGVK = schema.GroupVersionKind{Version: "v1", Kind: "Service"}

func TestReferencePermittedMatchesTheGrantedReference(t *testing.T) {
	other := referenceGrant("shared", "tenant-a", "")
	other.SetName("other-kind")
	if err := unstructured.SetNestedSlice(other.Object, []any{map[string]any{"group": "gateway.networking.k8s.io", "kind": "HTTPRoute", "namespace": "tenant-a"}}, "spec", "from"); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		grants []client.Object
		want   bool
	}{
		{"every provider", []client.Object{referenceGrant("shared", "tenant-a", "")}, true},
		{"the named provider", []client.Object{referenceGrant("shared", "tenant-a", "openai")}, true},
		{"another provider", []client.Object{referenceGrant("shared", "tenant-a", "anthropic")}, false},
		{"another model namespace", []client.Object{referenceGrant("shared", "tenant-b", "")}, false},
		{"another provider namespace", []client.Object{referenceGrant("elsewhere", "tenant-a", "")}, false},
		// An ExternalModel-to-ExternalProvider grant is required; another
		// reference kind's grant does not authorize it.
		{"another referencing kind", []client.Object{other}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := controllerTestClient(t, tc.grants...)
			got, err := r.referencePermitted(context.Background(), "tenant-a", types.NamespacedName{Namespace: "shared", Name: "openai"})
			if err != nil || got != tc.want {
				t.Fatalf("referencePermitted = %t, %v; want %t", got, err, tc.want)
			}
		})
	}
}

func TestTenantModelServesGrantedProviderFromAnotherNamespace(t *testing.T) {
	// The same-named local provider and Secret must never stand in.
	localProvider, localSecret := attachmentProvider("tenant-b")
	sharedProvider, sharedSecret := attachmentProvider("shared")
	model := referenceProvider(attachmentModel("chat", "tenant-b", "chat-model", time.Unix(100, 0)), "shared")
	objects := append([]client.Object{localProvider, localSecret, sharedProvider, sharedSecret, model}, attachmentTenantObjects("tenant-b", "tenant-b", gatewayB, "praxis")...)
	r := attachmentReconciler(t, objects...)

	route := foreignRoute("tenant-b", "shared")
	serviceName := route.ServiceName()
	ruleName := attachmentDestinationRuleName(gatewayB, "shared", "openai")
	copyName := credentialCopyName("tenant-b", "shared", "openai-key", "api-key", "")
	assertNotServed := func(when string) {
		t.Helper()
		if _, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(model)}); !errors.Is(err, resolver.ErrNoRoutes) {
			t.Fatalf("%s: reconcile = %v, want %v", when, err, resolver.ErrNoRoutes)
		}
		got := getModel(t, r, model)
		if got.Status.Phase != modelPhaseFailed || readyReason(got) != reasonRefNotPermitted ||
			!strings.Contains(readyMessage(got), "shared/openai is not found or not permitted by a ReferenceGrant") {
			t.Fatalf("%s: status = %q/%q %q", when, got.Status.Phase, readyReason(got), readyMessage(got))
		}
		if _, err := getUnstructured(r, httpRouteGVK, "tenant-b", modelRouteName("chat")); !apierrors.IsNotFound(err) {
			t.Fatalf("%s: HTTPRoute exists: %v", when, err)
		}
		assertNoTenantOverlay(t, r)
		if _, err := getUnstructured(r, destinationRuleGVK, "gateway-b", ruleName); !apierrors.IsNotFound(err) {
			t.Fatalf("%s: DestinationRule exists: %v", when, err)
		}
		var copied corev1.Secret
		if err := r.Get(context.Background(), client.ObjectKey{Namespace: "tenant-b", Name: copyName}, &copied); !apierrors.IsNotFound(err) {
			t.Fatalf("%s: credential copy exists: %v", when, err)
		}
	}
	assertNotServed("without a grant")

	if err := r.Create(context.Background(), referenceGrant("shared", "tenant-b", "openai")); err != nil {
		t.Fatal(err)
	}
	reconcileModel(t, r, model)
	if got := getModel(t, r, model); got.Status.Phase != resolver.PhaseReady || got.Status.HTTPRouteName != modelRouteName("chat") {
		t.Fatalf("granted status = %#v", got.Status)
	}
	service, err := getUnstructured(r, serviceGVK, "tenant-b", serviceName)
	if err != nil {
		t.Fatalf("hashed provider Service: %v", err)
	}
	if service.GetLabels()["inference.opendatahub.io/external-provider"] != "" || service.GetAnnotations()[annotationAttachmentIdentity] != "shared/openai" {
		t.Fatalf("hashed provider Service metadata = %#v %#v", service.GetLabels(), service.GetAnnotations())
	}
	if _, err := getUnstructured(r, schema.GroupVersionKind{Group: "networking.istio.io", Version: "v1", Kind: "ServiceEntry"}, "tenant-b", serviceName); err != nil {
		t.Fatalf("hashed provider ServiceEntry: %v", err)
	}
	httpRoute, err := getUnstructured(r, httpRouteGVK, "tenant-b", modelRouteName("chat"))
	if err != nil {
		t.Fatal(err)
	}
	rule := nestedMapAt(t, nestedSlice(t, httpRoute.Object, "spec", "rules"), 0)
	if backend := nestedMapAt(t, nestedSlice(t, rule, "backendRefs"), 0); backend["name"] != serviceName {
		t.Fatalf("backendRef = %#v, want %s", backend, serviceName)
	}
	candidates := overlayEnvelope(t, r, "tenant-b").Overlay.Candidates
	if len(candidates) != 1 || candidates[0].StableID != "tenant-b/chat/shared/openai" || candidates[0].Cluster != serviceName {
		t.Fatalf("overlay candidates = %#v", candidates)
	}
	if candidates[0].Credential == nil || candidates[0].Credential.SecretRef != (envelope.SecretRef{Name: copyName, Namespace: "tenant-b", Key: "api-key"}) {
		t.Fatalf("candidate credential = %#v, want the copy of the provider-namespace Secret", candidates[0].Credential)
	}
	var copied corev1.Secret
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: "tenant-b", Name: copyName}, &copied); err != nil || string(copied.Data["api-key"]) != "sk-shared" {
		t.Fatalf("credential copy = %q, %v; want the provider-namespace value", copied.Data["api-key"], err)
	}
	dr, err := getUnstructured(r, destinationRuleGVK, "gateway-b", ruleName)
	if err != nil || dr.GetLabels()[tenant.LabelAttachmentTenant] != "tenant-b" {
		t.Fatalf("DestinationRule %s: %v %#v", ruleName, err, dr.GetLabels())
	}
	// The same-named local provider keeps its own transport.
	if _, err := getUnstructured(r, destinationRuleGVK, "gateway-b", "provider-openai-tenant-b"); err != nil {
		t.Fatalf("local provider DestinationRule: %v", err)
	}

	if err := r.Delete(context.Background(), referenceGrant("shared", "tenant-b", "openai")); err != nil {
		t.Fatal(err)
	}
	assertNotServed("after the grant is revoked")

	// Transport no model references any more is collected.
	got := getModel(t, r, model)
	got.Spec.ExternalProviderRefs[0].Ref.Namespace = ""
	if err := r.Update(context.Background(), got); err != nil {
		t.Fatal(err)
	}
	reconcileModel(t, r, model)
	if _, err := getUnstructured(r, serviceGVK, "tenant-b", serviceName); !apierrors.IsNotFound(err) {
		t.Fatalf("unreferenced provider Service remains: %v", err)
	}
	if candidates := overlayEnvelope(t, r, "tenant-b").Overlay.Candidates; len(candidates) != 1 || candidates[0].StableID != "chat/provider-openai" {
		t.Fatalf("local reference overlay = %#v", candidates)
	}
}

func TestModelServesSameNamedProvidersFromTwoNamespaces(t *testing.T) {
	providerA, secretA := attachmentProvider("ns-a")
	providerB, secretB := attachmentProvider("ns-b")
	model := referenceProvider(attachmentModel("chat", "tenant-b", "chat-model", time.Unix(100, 0)), "ns-a")
	second := model.Spec.ExternalProviderRefs[0]
	second.Ref.Namespace = "ns-b"
	model.Spec.ExternalProviderRefs = append(model.Spec.ExternalProviderRefs, second)
	objects := []client.Object{providerA, secretA, providerB, secretB, model, referenceGrant("ns-a", "tenant-b", ""), referenceGrant("ns-b", "tenant-b", "")}
	r := attachmentReconciler(t, append(objects, attachmentTenantObjects("tenant-b", "tenant-b", gatewayB, "praxis")...)...)
	reconcileModel(t, r, model)

	httpRoute, err := getUnstructured(r, httpRouteGVK, "tenant-b", modelRouteName("chat"))
	if err != nil {
		t.Fatal(err)
	}
	rules := nestedSlice(t, httpRoute.Object, "spec", "rules")
	backends := map[any]bool{}
	for i := range rules {
		backends[nestedMapAt(t, nestedSlice(t, nestedMapAt(t, rules, i), "backendRefs"), 0)["name"]] = true
	}
	serviceA, serviceB := foreignRoute("tenant-b", "ns-a").ServiceName(), foreignRoute("tenant-b", "ns-b").ServiceName()
	if len(rules) != 6 || !backends[serviceA] || !backends[serviceB] {
		t.Fatalf("HTTPRoute rules = %d with backends %v, want both providers' rules", len(rules), backends)
	}
	candidates := overlayEnvelope(t, r, "tenant-b").Overlay.Candidates
	if len(candidates) != 2 || candidates[0].Cluster == candidates[1].Cluster || candidates[0].StableID == candidates[1].StableID {
		t.Fatalf("overlay candidates = %#v, want two distinct candidates", candidates)
	}
	for _, namespace := range []string{"ns-a", "ns-b"} {
		if _, err := getUnstructured(r, destinationRuleGVK, "gateway-b", attachmentDestinationRuleName(gatewayB, namespace, "openai")); err != nil {
			t.Fatalf("DestinationRule for %s/openai: %v", namespace, err)
		}
		var copied corev1.Secret
		if err := r.Get(context.Background(), client.ObjectKey{Namespace: "tenant-b", Name: credentialCopyName("tenant-b", namespace, "openai-key", "api-key", "")}, &copied); err != nil ||
			string(copied.Data["api-key"]) != "sk-"+namespace {
			t.Fatalf("credential copy for %s: %q %v", namespace, copied.Data["api-key"], err)
		}
	}
}

func TestAttachedModelOverrideCredentialIsModelLocalWithoutFallback(t *testing.T) {
	sharedProvider, sharedSecret := attachmentProvider("shared")
	// Same Secret name in the model namespace, with a different value.
	_, modelSecret := attachmentProvider("team")
	model := referenceProvider(attachmentModel("chat", "team", "chat-model", time.Unix(100, 0), gatewayB), "shared")
	model.Spec.ExternalProviderRefs[0].Auth = &v1alpha1.AuthConfig{Type: "apikey", SecretRef: v1alpha1.NameReference{Name: "openai-key"}}
	objects := []client.Object{sharedProvider, sharedSecret, modelSecret, model, referenceGrant("shared", "team", "openai")}
	r := attachmentReconciler(t, append(objects, attachmentTenantObjects("tenant-b", "tenant-b", gatewayB, "praxis")...)...)
	reconcileModel(t, r, model)
	setRouteParentStatus(t, r, model, gatewayB, metav1.ConditionTrue, "Accepted")
	reconcileModel(t, r, model)

	if got := getModel(t, r, model); got.Status.Phase != resolver.PhaseReady {
		t.Fatalf("status = %q/%q %q", got.Status.Phase, readyReason(got), readyMessage(got))
	}
	modelCopy, sharedCopy := credentialCopyName("tenant-b", "team", "openai-key", "api-key", ""), credentialCopyName("tenant-b", "shared", "openai-key", "api-key", "")
	var copied corev1.Secret
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: "tenant-b", Name: modelCopy}, &copied); err != nil || string(copied.Data["api-key"]) != "sk-team" {
		t.Fatalf("override copy = %q, %v; want the model-namespace value", copied.Data["api-key"], err)
	}
	candidates := overlayEnvelope(t, r, "tenant-b").Overlay.Candidates
	if len(candidates) != 1 || candidates[0].StableID != "team/chat/shared/openai" || candidates[0].Credential.SecretRef.Name != modelCopy {
		t.Fatalf("overlay candidates = %#v", candidates)
	}

	// A missing model-local override never falls back to the provider-namespace Secret.
	if err := r.Delete(context.Background(), modelSecret); err != nil {
		t.Fatal(err)
	}
	reconcileModel(t, r, model)
	if got := getModel(t, r, model); readyReason(got) != reasonProviderNotReady || !strings.Contains(readyMessage(got), "team/openai-key") {
		t.Fatalf("status = %q %q, want the missing model-local Secret", readyReason(got), readyMessage(got))
	}
	for _, name := range []string{modelCopy, sharedCopy} {
		if err := r.Get(context.Background(), client.ObjectKey{Namespace: "tenant-b", Name: name}, &copied); !apierrors.IsNotFound(err) {
			t.Fatalf("credential copy %s remains: %v", name, err)
		}
	}
	assertNoTenantOverlay(t, r)
}

func TestAttachedModelReportsDeniedReferenceAndServesTheRest(t *testing.T) {
	provider, secret := attachmentProvider("team")
	model := attachmentModel("chat", "team", "chat-model", time.Unix(100, 0), gatewayB)
	denied := model.Spec.ExternalProviderRefs[0]
	denied.Ref = v1alpha1.ExternalProviderReference{Name: "anthropic", Namespace: "shared"}
	model.Spec.ExternalProviderRefs = append(model.Spec.ExternalProviderRefs, denied)
	objects := append([]client.Object{provider, secret, model}, attachmentTenantObjects("tenant-b", "tenant-b", gatewayB, "praxis")...)
	r := attachmentReconciler(t, objects...)
	reconcileModel(t, r, model)
	setRouteParentStatus(t, r, model, gatewayB, metav1.ConditionTrue, "Accepted")
	reconcileModel(t, r, model)

	if got := getModel(t, r, model); readyReason(got) != reasonRefNotPermitted || !strings.Contains(readyMessage(got), "shared/anthropic") {
		t.Fatalf("status = %q %q", readyReason(got), readyMessage(got))
	}
	if candidates := overlayEnvelope(t, r, "tenant-b").Overlay.Candidates; len(candidates) != 1 || candidates[0].StableID != "team/chat/team/openai" {
		t.Fatalf("overlay candidates = %#v, want only the permitted reference", candidates)
	}
}

func TestProviderGrantAndSecretEventsEnqueueReferencingModels(t *testing.T) {
	sharedProvider, sharedSecret := attachmentProvider("shared")
	localProvider, _ := attachmentProvider("tenant-a")
	foreign := referenceProvider(attachmentModel("chat", "tenant-a", "chat", time.Unix(100, 0)), "shared")
	local := attachmentModel("local", "tenant-a", "local", time.Unix(100, 0))
	r := controllerTestClient(t, sharedProvider, localProvider, foreign, local)
	ctx := context.Background()
	want := func(name string, got []reconcile.Request, model *v1alpha1.ExternalModel) {
		t.Helper()
		if len(got) != 1 || got[0].NamespacedName != client.ObjectKeyFromObject(model) {
			t.Fatalf("%s enqueued %v, want only %s/%s", name, got, model.Namespace, model.Name)
		}
	}
	want("shared provider", r.providerModels(ctx, sharedProvider), foreign)
	want("shared Secret", r.secretModels(ctx, sharedSecret), foreign)
	want("ReferenceGrant", r.referenceGrantModels(ctx, referenceGrant("shared", "tenant-a", "")), foreign)
	// A same-named local provider does not affect the cross-namespace reference.
	want("local provider", r.providerModels(ctx, localProvider), local)
}

func TestUnpermittedSiblingKeepsLastServingState(t *testing.T) {
	provider, secret := attachmentProvider("tenant-b")
	own := attachmentModel("own", "tenant-b", "own-model", time.Unix(100, 0))
	// Never permitted, so never served: it must not cost the namespace its
	// last serving state.
	sibling := referenceProvider(attachmentModel("sibling", "tenant-b", "sibling-model", time.Unix(200, 0)), "shared")
	objects := append([]client.Object{provider, secret, own, sibling}, attachmentTenantObjects("tenant-b", "tenant-b", gatewayB, "praxis")...)
	r := attachmentReconciler(t, objects...)
	reconcileModel(t, r, own)

	// A provider spec edit leaves its readiness briefly stale.
	got := &v1alpha1.ExternalProvider{}
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(provider), got); err != nil {
		t.Fatal(err)
	}
	got.Status.ObservedGeneration = got.Generation + 1
	if err := r.Status().Update(context.Background(), got); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(own)}); !errors.Is(err, resolver.ErrNoRoutes) {
		t.Fatalf("reconcile = %v, want %v", err, resolver.ErrNoRoutes)
	}
	if candidates := overlayEnvelope(t, r, "tenant-b").Overlay.Candidates; len(candidates) != 1 || candidates[0].StableID != "own/provider-openai" {
		t.Fatalf("overlay candidates = %#v, want the retained serving state", candidates)
	}
	if _, err := getUnstructured(r, httpRouteGVK, "tenant-b", modelRouteName("own")); err != nil {
		t.Fatalf("retained HTTPRoute: %v", err)
	}
}

func TestAllZeroCrossNamespaceReferenceWithdrawsAfterGrantRevocation(t *testing.T) {
	ctx := context.Background()
	sharedProvider, sharedSecret := attachmentProvider("shared")
	model := referenceProvider(attachmentModel("chat", "tenant-b", "chat-model", time.Unix(100, 0)), "shared")
	grant := referenceGrant("shared", "tenant-b", "openai")
	objects := append([]client.Object{sharedProvider, sharedSecret, model, grant},
		attachmentTenantObjects("tenant-b", "tenant-b", gatewayB, "praxis")...)
	r := attachmentReconciler(t, objects...)
	request := reconcile.Request{NamespacedName: client.ObjectKeyFromObject(model)}
	if _, err := r.Reconcile(ctx, request); err != nil {
		t.Fatalf("initial granted serving reconcile: %v", err)
	}
	if _, err := getUnstructured(r, httpRouteGVK, model.Namespace, modelRouteName(model.Name)); err != nil {
		t.Fatalf("initial granted route: %v", err)
	}
	if len(overlayEnvelope(t, r, model.Namespace).Overlay.Candidates) != 1 {
		t.Fatal("expected the granted provider to be published before withdrawal")
	}
	var current v1alpha1.ExternalModel
	if err := r.Get(ctx, client.ObjectKeyFromObject(model), &current); err != nil {
		t.Fatal(err)
	}
	zero := 0
	current.Spec.ExternalProviderRefs[0].Weight = &zero
	if err := r.Update(ctx, &current); err != nil {
		t.Fatalf("set cross-namespace provider weight to zero: %v", err)
	}
	if err := r.Delete(ctx, grant); err != nil {
		t.Fatalf("revoke ReferenceGrant: %v", err)
	}
	if _, err := r.Reconcile(ctx, request); !errors.Is(err, resolver.ErrNoRoutes) {
		t.Fatalf("all-zero revoked-reference reconcile = %v, want ErrNoRoutes", err)
	}
	if _, err := getUnstructured(r, httpRouteGVK, model.Namespace, modelRouteName(model.Name)); !apierrors.IsNotFound(err) {
		t.Fatalf("revoked zero-weight route = %v, want NotFound", err)
	}
	assertNoTenantOverlay(t, r)
	got := getModel(t, r, model)
	if got.Status.Phase != modelPhaseFailed || got.Status.ObservedGeneration != got.Generation ||
		readyReason(got) != reasonNoRoutes {
		t.Fatalf("revoked zero-weight model status = %q/%q at %d/%d, want current-generation Failed/NoRoutes",
			got.Status.Phase, readyReason(got), got.Status.ObservedGeneration, got.Generation)
	}
}

func TestLocalProviderCannotTakeOverHashedTransport(t *testing.T) {
	sharedProvider, sharedSecret := attachmentProvider("shared")
	_, localSecret := attachmentProvider("tenant-b")
	model := referenceProvider(attachmentModel("chat", "tenant-b", "chat-model", time.Unix(100, 0)), "shared")
	objects := append([]client.Object{sharedProvider, sharedSecret, localSecret, model, referenceGrant("shared", "tenant-b", "")},
		attachmentTenantObjects("tenant-b", "tenant-b", gatewayB, "praxis")...)
	r := attachmentReconciler(t, objects...)
	reconcileModel(t, r, model)
	serviceName := foreignRoute("tenant-b", "shared").ServiceName()

	// A local provider whose own Service name is the hashed name.
	local, _ := namedProvider("tenant-b", strings.TrimPrefix(serviceName, "provider-"))
	local.Spec.Endpoint = "attacker.example.com"
	local.Spec.Auth.SecretRef.Name = "openai-key"
	if err := r.Create(context.Background(), local); err != nil {
		t.Fatal(err)
	}
	if _, err := r.ReconcileProvider(context.Background(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(local)}); err == nil || !strings.Contains(err.Error(), "different identity") {
		t.Fatalf("local provider reconcile = %v, want a refused takeover", err)
	}
	service, err := getUnstructured(r, serviceGVK, "tenant-b", serviceName)
	if err != nil {
		t.Fatal(err)
	}
	if host, _, _ := unstructured.NestedString(service.Object, "spec", "externalName"); host != "api.openai.com" || service.GetAnnotations()[annotationAttachmentIdentity] != "shared/openai" {
		t.Fatalf("hashed Service = %s %#v, want it unchanged", host, service.GetAnnotations())
	}
}

func TestProviderTransportOutlivesTheRouteThatUsesIt(t *testing.T) {
	localProvider, localSecret := attachmentProvider("tenant-b")
	sharedProvider, sharedSecret := attachmentProvider("shared")
	model := referenceProvider(attachmentModel("chat", "tenant-b", "chat-model", time.Unix(100, 0)), "shared")
	objects := append([]client.Object{localProvider, localSecret, sharedProvider, sharedSecret, model, referenceGrant("shared", "tenant-b", "")},
		attachmentTenantObjects("tenant-b", "tenant-b", gatewayB, "praxis")...)
	r := attachmentReconciler(t, objects...)
	reconcileModel(t, r, model)
	serviceName := foreignRoute("tenant-b", "shared").ServiceName()

	got := getModel(t, r, model)
	got.Spec.ExternalProviderRefs[0].Ref.Namespace = ""
	if err := r.Update(context.Background(), got); err != nil {
		t.Fatal(err)
	}
	// The model no longer references the provider, but its route still does.
	if err := r.collectProviderTransport(context.Background(), "tenant-b"); err != nil {
		t.Fatal(err)
	}
	if _, err := getUnstructured(r, serviceGVK, "tenant-b", serviceName); err != nil {
		t.Fatalf("provider Service deleted while a route uses it: %v", err)
	}
	reconcileModel(t, r, model)
	if _, err := getUnstructured(r, serviceGVK, "tenant-b", serviceName); !apierrors.IsNotFound(err) {
		t.Fatalf("provider Service remains after its route stopped using it: %v", err)
	}
}

func TestOverrideServesWithoutTheProviderSecret(t *testing.T) {
	// The shared provider has no credential of its own; the model brings one.
	sharedProvider, _ := attachmentProvider("shared")
	_, modelSecret := attachmentProvider("team")
	model := referenceProvider(attachmentModel("chat", "team", "chat-model", time.Unix(100, 0), gatewayB), "shared")
	model.Spec.ExternalProviderRefs[0].Auth = &v1alpha1.AuthConfig{Type: "apikey", SecretRef: v1alpha1.NameReference{Name: "openai-key"}}
	objects := []client.Object{sharedProvider, modelSecret, model, referenceGrant("shared", "team", "")}
	r := attachmentReconciler(t, append(objects, attachmentTenantObjects("tenant-b", "tenant-b", gatewayB, "praxis")...)...)
	reconcileModel(t, r, model)
	setRouteParentStatus(t, r, model, gatewayB, metav1.ConditionTrue, "Accepted")
	reconcileModel(t, r, model)

	if got := getModel(t, r, model); got.Status.Phase != resolver.PhaseReady {
		t.Fatalf("status = %q/%q %q", got.Status.Phase, readyReason(got), readyMessage(got))
	}
	candidates := overlayEnvelope(t, r, "tenant-b").Overlay.Candidates
	if len(candidates) != 1 || candidates[0].Credential.SecretRef.Name != credentialCopyName("tenant-b", "team", "openai-key", "api-key", "") {
		t.Fatalf("overlay candidates = %#v, want the model-namespace credential", candidates)
	}
}

func TestUnreadyProviderCredentialSkipsOnlyInheritingReferences(t *testing.T) {
	// The shared provider's own Secret is missing.
	sharedProvider, _ := attachmentProvider("shared")
	_, modelSecret := attachmentProvider("tenant-b")
	inheriting := referenceProvider(attachmentModel("inherit", "tenant-b", "inherit-model", time.Unix(100, 0)), "shared")
	overriding := referenceProvider(attachmentModel("override", "tenant-b", "override-model", time.Unix(200, 0)), "shared")
	overriding.Spec.ExternalProviderRefs[0].Auth = &v1alpha1.AuthConfig{Type: "apikey", SecretRef: v1alpha1.NameReference{Name: "openai-key"}}
	objects := append([]client.Object{sharedProvider, modelSecret, inheriting, overriding, referenceGrant("shared", "tenant-b", "")},
		attachmentTenantObjects("tenant-b", "tenant-b", gatewayB, "praxis")...)
	r := attachmentReconciler(t, objects...)
	for _, model := range []*v1alpha1.ExternalModel{inheriting, overriding} {
		reconcileModel(t, r, model)
	}

	if got := getModel(t, r, overriding); got.Status.Phase != resolver.PhaseReady {
		t.Fatalf("overriding model status = %q/%q %q, want Ready", got.Status.Phase, readyReason(got), readyMessage(got))
	}
	if got := getModel(t, r, inheriting); readyReason(got) != reasonProviderNotReady || !strings.Contains(readyMessage(got), "shared/openai is not ready") {
		t.Fatalf("inheriting model status = %q %q, want ProviderNotReady", readyReason(got), readyMessage(got))
	}
	candidates := overlayEnvelope(t, r, "tenant-b").Overlay.Candidates
	if len(candidates) != 1 || candidates[0].StableID != "tenant-b/override/shared/openai" ||
		candidates[0].Credential.SecretRef != (envelope.SecretRef{Name: "openai-key", Namespace: "tenant-b", Key: "api-key"}) {
		t.Fatalf("overlay candidates = %#v, want only the override", candidates)
	}
}

func TestOverrideSecretEventsReconcileTenantLocalModels(t *testing.T) {
	sharedProvider, sharedSecret := attachmentProvider("shared")
	modelSecret := func() *corev1.Secret {
		return &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "model-key", Namespace: "tenant-b"}, Data: map[string][]byte{"api-key": []byte("sk-model")}}
	}
	model := referenceProvider(attachmentModel("chat", "tenant-b", "chat-model", time.Unix(100, 0)), "shared")
	model.Spec.ExternalProviderRefs[0].Auth = &v1alpha1.AuthConfig{Type: "apikey", SecretRef: v1alpha1.NameReference{Name: "model-key"}}
	objects := append([]client.Object{sharedProvider, sharedSecret, modelSecret(), model, referenceGrant("shared", "tenant-b", "")},
		attachmentTenantObjects("tenant-b", "tenant-b", gatewayB, "praxis")...)
	r := attachmentReconciler(t, objects...)
	reconcileModel(t, r, model)
	// Each override Secret event must reconcile the model, which has no gatewayRefs.
	reconcileOnSecretEvent := func() {
		t.Helper()
		requests := r.secretModels(context.Background(), modelSecret())
		if len(requests) != 1 || requests[0].NamespacedName != client.ObjectKeyFromObject(model) {
			t.Fatalf("override Secret event enqueued %v, want the model", requests)
		}
		_, _ = r.Reconcile(context.Background(), requests[0])
	}

	if err := r.Delete(context.Background(), modelSecret()); err != nil {
		t.Fatal(err)
	}
	reconcileOnSecretEvent()
	if got := getModel(t, r, model); readyReason(got) != reasonProviderNotReady || !strings.Contains(readyMessage(got), "tenant-b/model-key") {
		t.Fatalf("status after deleting the override Secret = %q %q, want ProviderNotReady", readyReason(got), readyMessage(got))
	}
	if err := r.Create(context.Background(), modelSecret()); err != nil {
		t.Fatal(err)
	}
	reconcileOnSecretEvent()
	if got := getModel(t, r, model); got.Status.Phase != resolver.PhaseReady {
		t.Fatalf("status after restoring the override Secret = %q/%q %q, want Ready", got.Status.Phase, readyReason(got), readyMessage(got))
	}
}
