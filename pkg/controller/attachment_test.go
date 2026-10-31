package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
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
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	v1alpha1 "github.com/opendatahub-io/ai-gateway-controller/api/inference/v1alpha1"
	"github.com/opendatahub-io/ai-gateway-controller/pkg/envelope"
	"github.com/opendatahub-io/ai-gateway-controller/pkg/render"
	"github.com/opendatahub-io/ai-gateway-controller/pkg/resolver"
	"github.com/opendatahub-io/ai-gateway-controller/pkg/tenant"
)

var (
	gatewayB = v1alpha1.NamespacedObjectReference{Name: "gw-b", Namespace: "gateway-b"}
	gatewayC = v1alpha1.NamespacedObjectReference{Name: "gw-c", Namespace: "gateway-c"}
)

func attachmentProvider(namespace string) (*v1alpha1.ExternalProvider, *corev1.Secret) {
	provider := &v1alpha1.ExternalProvider{
		ObjectMeta: metav1.ObjectMeta{Name: "openai", Namespace: namespace, UID: types.UID("provider-" + namespace)},
		Spec: v1alpha1.ExternalProviderSpec{
			Provider: "openai", Endpoint: "api.openai.com",
			Auth: v1alpha1.AuthConfig{Type: "apikey", SecretRef: v1alpha1.NameReference{Name: "openai-key"}},
		},
	}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "openai-key", Namespace: namespace}, Data: map[string][]byte{"api-key": []byte("sk-" + namespace)}}
	return provider, secret
}

func attachmentModel(name, namespace, modelName string, created time.Time, gateways ...v1alpha1.NamespacedObjectReference) *v1alpha1.ExternalModel {
	return &v1alpha1.ExternalModel{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace, UID: types.UID("uid-" + namespace + "-" + name), CreationTimestamp: metav1.NewTime(created)},
		Spec: v1alpha1.ExternalModelSpec{
			ModelName: modelName,
			ExternalProviderRefs: []v1alpha1.ExternalProviderRef{{
				Ref: v1alpha1.ExternalProviderReference{Name: "openai"}, TargetModel: "gpt-4o", APIFormat: "openai-chat", Path: "/v1/chat/completions",
			}},
			GatewayRefs: gateways,
		},
	}
}

// attachmentTenantObjects returns a steady Praxis tenant owning gateway.
func attachmentTenantObjects(name, tenantNamespace string, gateway v1alpha1.NamespacedObjectReference, selector string) []client.Object {
	return []client.Object{
		controllerAITenant(name, "models-as-a-service", tenantNamespace, gateway.Name, gateway.Namespace),
		controllerMaasTenantConfig(tenantNamespace, name, "models-as-a-service", selector),
	}
}

func attachmentReconciler(t *testing.T, objects ...client.Object) *Reconciler {
	t.Helper()
	r := controllerTestClient(t, objects...)
	r.Network = "external-model"
	for _, object := range objects {
		provider, ok := object.(*v1alpha1.ExternalProvider)
		if !ok {
			continue
		}
		if _, err := r.ReconcileProvider(context.Background(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(provider)}); err != nil {
			t.Fatalf("reconcile provider %s/%s: %v", provider.Namespace, provider.Name, err)
		}
	}
	return r
}

func reconcileModel(t *testing.T, r *Reconciler, model *v1alpha1.ExternalModel) {
	t.Helper()
	if _, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(model)}); err != nil {
		t.Fatalf("reconcile %s/%s: %v", model.Namespace, model.Name, err)
	}
}

func getModel(t *testing.T, r *Reconciler, model *v1alpha1.ExternalModel) *v1alpha1.ExternalModel {
	t.Helper()
	var got v1alpha1.ExternalModel
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(model), &got); err != nil {
		t.Fatal(err)
	}
	return &got
}

func readyReason(model *v1alpha1.ExternalModel) string {
	if condition := apiMeta.FindStatusCondition(model.Status.Conditions, conditionReady); condition != nil {
		return condition.Reason
	}
	return ""
}

func getUnstructured(r *Reconciler, gvk schema.GroupVersionKind, namespace, name string) (*unstructured.Unstructured, error) {
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(gvk)
	err := r.Get(context.Background(), client.ObjectKey{Namespace: namespace, Name: name}, obj)
	return obj, err
}

var destinationRuleGVK = schema.GroupVersionKind{Group: "networking.istio.io", Version: "v1", Kind: "DestinationRule"}

// gatewayResult returns the model's status.gateways result for gateway.
func gatewayResult(t *testing.T, model *v1alpha1.ExternalModel, gateway v1alpha1.NamespacedObjectReference) v1alpha1.ExternalModelGatewayStatus {
	t.Helper()
	for _, result := range model.Status.Gateways {
		if result.NamespacedObjectReference == gateway {
			return result
		}
	}
	t.Fatalf("no status.gateways result for %s: %#v", gatewayKey(gateway), model.Status.Gateways)
	return v1alpha1.ExternalModelGatewayStatus{}
}

// assertTransportCollected checks that the namespace keeps no provider
// transport or selection sink of the given models.
func assertTransportCollected(t *testing.T, r *Reconciler, namespace string, models ...string) {
	t.Helper()
	if _, err := getUnstructured(r, schema.GroupVersionKind{Group: "networking.istio.io", Version: "v1", Kind: "ServiceEntry"}, namespace, "provider-openai"); !apierrors.IsNotFound(err) {
		t.Fatalf("ServiceEntry %s/provider-openai remains: %v", namespace, err)
	}
	services := []string{"provider-openai"}
	for _, model := range models {
		services = append(services, providerSelectionSinkName(model))
	}
	for _, name := range services {
		if _, err := getUnstructured(r, schema.GroupVersionKind{Version: "v1", Kind: "Service"}, namespace, name); !apierrors.IsNotFound(err) {
			t.Fatalf("Service %s/%s remains: %v", namespace, name, err)
		}
	}
}

// setRouteParentStatus writes the Gateway's parent status on an attachment
// HTTPRoute, as the Gateway controller would.
func setRouteParentStatus(t *testing.T, r *Reconciler, model *v1alpha1.ExternalModel, gateway v1alpha1.NamespacedObjectReference, status metav1.ConditionStatus, reason string) {
	t.Helper()
	route, err := getUnstructured(r, httpRouteGVK, model.Namespace, attachmentRouteName(model, gateway))
	if err != nil {
		t.Fatalf("attachment HTTPRoute: %v", err)
	}
	parents := []any{map[string]any{
		"parentRef":      map[string]any{"name": gateway.Name, "namespace": gateway.Namespace},
		"controllerName": "istio.io/gateway-controller",
		"conditions":     []any{map[string]any{"type": "Accepted", "status": string(status), "reason": reason}},
	}}
	if err := unstructured.SetNestedSlice(route.Object, parents, "status", "parents"); err != nil {
		t.Fatal(err)
	}
	if err := r.Update(context.Background(), route); err != nil {
		t.Fatal(err)
	}
}

func overlayEnvelope(t *testing.T, r *Reconciler, namespace string) envelope.Envelope {
	t.Helper()
	var cm corev1.ConfigMap
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: namespace, Name: "routing-overlay"}, &cm); err != nil {
		t.Fatalf("overlay %s: %v", namespace, err)
	}
	var env envelope.Envelope
	if err := json.Unmarshal([]byte(cm.Data["routing-overlay.json"]), &env); err != nil {
		t.Fatal(err)
	}
	return env
}

func routeFilterNames(t *testing.T, r *Reconciler, tenantID, gatewayNamespace string) []string {
	t.Helper()
	filter, err := getUnstructured(r, schema.GroupVersionKind{Group: "networking.istio.io", Version: "v1alpha3", Kind: "EnvoyFilter"},
		gatewayNamespace, tenant.PayloadProcessingExternalModelEnvoyFilterName(tenantID))
	if err != nil {
		t.Fatalf("route EnvoyFilter: %v", err)
	}
	var names []string
	patches := nestedSlice(t, filter.Object, "spec", "configPatches")
	for i := range patches {
		names = append(names, nestedString(t, nestedMapAt(t, patches, i), "match", "routeConfiguration", "vhost", "route", "name"))
	}
	return names
}

func assertExactSelectedPathRule(t *testing.T, route *unstructured.Unstructured, path, stableID string) {
	t.Helper()
	rules := nestedSlice(t, route.Object, "spec", "rules")
	for ruleIndex := range rules {
		matches := nestedSlice(t, nestedMapAt(t, rules, ruleIndex), "matches")
		for _, rawMatch := range matches {
			match, ok := rawMatch.(map[string]any)
			if !ok {
				continue
			}
			pathType, hasPath, pathErr := unstructured.NestedString(match, "path", "type")
			pathValue, _, valueErr := unstructured.NestedString(match, "path", "value")
			if pathErr != nil || valueErr != nil {
				t.Fatalf("read exact target path match: type error=%v value error=%v", pathErr, valueErr)
			}
			if !hasPath || pathType != "Exact" || pathValue != path {
				continue
			}
			headers := nestedSlice(t, match, "headers")
			if len(headers) == 1 && nestedString(t, nestedMapAt(t, headers, 0), "name") == selectedProviderHeader &&
				nestedString(t, nestedMapAt(t, headers, 0), "value") == stableID {
				return
			}
		}
	}
	t.Fatalf("no exact target-path match for %q gated by %s=%q", path, selectedProviderHeader, stableID)
}

func TestUserNamespaceModelServedOnlyAfterRouteAcceptance(t *testing.T) {
	provider, secret := attachmentProvider("team")
	model := attachmentModel("chat", "team", "chat-model", time.Unix(100, 0), gatewayB)
	objects := append([]client.Object{provider, secret, model}, attachmentTenantObjects("tenant-b", "tenant-b", gatewayB, "praxis")...)
	r := attachmentReconciler(t, objects...)

	reconcileModel(t, r, model)
	routeName := attachmentRouteName(model, gatewayB)
	if !strings.HasPrefix(routeName, "external-model-chat-") || len(routeName) != len("external-model-chat-")+12 {
		t.Fatalf("attachment route name = %q", routeName)
	}
	route, err := getUnstructured(r, httpRouteGVK, "team", routeName)
	if err != nil {
		t.Fatalf("attachment HTTPRoute was not created first: %v", err)
	}
	parent := nestedMapAt(t, nestedSlice(t, route.Object, "spec", "parentRefs"), 0)
	if parent["name"] != "gw-b" || parent["namespace"] != "gateway-b" {
		t.Fatalf("parentRef = %#v, want gateway-b/gw-b", parent)
	}
	if route.GetLabels()[tenant.LabelAttachmentTenant] != "tenant-b" || route.GetLabels()["inference.opendatahub.io/external-model"] != "" {
		t.Fatalf("attachment route labels = %#v", route.GetLabels())
	}
	if _, err := getUnstructured(r, schema.GroupVersionKind{Version: "v1", Kind: "Service"}, "team", "provider-openai"); err != nil {
		t.Fatalf("model-namespace provider Service: %v", err)
	}
	ruleName := attachmentDestinationRuleName(gatewayB, "team", "openai")
	copyName := credentialCopyName("tenant-b", "team", "openai-key", "api-key", "")
	assertAttachmentUnpublished := func() {
		t.Helper()
		if _, err := getUnstructured(r, destinationRuleGVK, "gateway-b", ruleName); !apierrors.IsNotFound(err) {
			t.Fatalf("DestinationRule published before acceptance: %v", err)
		}
		var copied corev1.Secret
		if err := r.Get(context.Background(), client.ObjectKey{Namespace: "tenant-b", Name: copyName}, &copied); !apierrors.IsNotFound(err) {
			t.Fatalf("credential copy published before acceptance: %v", err)
		}
		var overlay corev1.ConfigMap
		if err := r.Get(context.Background(), client.ObjectKey{Namespace: "tenant-b", Name: "routing-overlay"}, &overlay); !apierrors.IsNotFound(err) {
			t.Fatalf("overlay published before acceptance: %v", err)
		}
	}
	assertAttachmentUnpublished()
	if got := getModel(t, r, model); got.Status.Phase != modelPhasePending || readyReason(got) != reasonAttachmentPending {
		t.Fatalf("pending status = %q/%q", got.Status.Phase, readyReason(got))
	}

	setRouteParentStatus(t, r, model, gatewayB, metav1.ConditionFalse, "NotAllowedByListeners")
	reconcileModel(t, r, model)
	assertAttachmentUnpublished()
	if got := getModel(t, r, model); got.Status.Phase != modelPhaseFailed || readyReason(got) != "NotAllowedByListeners" {
		t.Fatalf("rejected status = %q/%q, want the Gateway's reason", got.Status.Phase, readyReason(got))
	}

	setRouteParentStatus(t, r, model, gatewayB, metav1.ConditionTrue, "Accepted")
	reconcileModel(t, r, model)
	rule, err := getUnstructured(r, destinationRuleGVK, "gateway-b", ruleName)
	if err != nil {
		t.Fatalf("attachment DestinationRule: %v", err)
	}
	if len(ruleName) > 63 || rule.GetLabels()[tenant.LabelAttachmentTenant] != "tenant-b" || rule.GetLabels()[externalGatewayNameLabel] != "gw-b" {
		t.Fatalf("attachment DestinationRule %q labels = %#v", ruleName, rule.GetLabels())
	}
	// The provider's own reconciler selects DestinationRules by the
	// provider-namespace tenant label; the attachment rule must not carry it.
	if _, ok := rule.GetLabels()[externalTenantLabel]; ok {
		t.Fatalf("attachment DestinationRule carries %s: %#v", externalTenantLabel, rule.GetLabels())
	}
	var copied corev1.Secret
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: "tenant-b", Name: copyName}, &copied); err != nil {
		t.Fatalf("credential copy: %v", err)
	}
	if string(copied.Data["api-key"]) != "sk-team" || len(copied.Data) != 1 || copied.Labels[tenant.LabelCredentialCopy] != "true" {
		t.Fatalf("credential copy = %#v", copied)
	}
	env := overlayEnvelope(t, r, "tenant-b")
	if len(env.Overlay.Candidates) != 1 {
		t.Fatalf("overlay candidates = %#v", env.Overlay.Candidates)
	}
	candidate := env.Overlay.Candidates[0]
	if candidate.StableID != "team/chat/team/openai" || candidate.Name != "chat-model" || candidate.Cluster != "provider-openai" {
		t.Fatalf("attached candidate = %#v", candidate)
	}
	if candidate.Credential == nil || candidate.Credential.SecretRef != (envelope.SecretRef{Name: copyName, Namespace: "tenant-b", Key: "api-key"}) {
		t.Fatalf("attached candidate credential = %#v, want the tenant-namespace copy", candidate.Credential)
	}
	if env.Scope.Namespace != "tenant-b" || env.Scope.Gateway != "gw-b" {
		t.Fatalf("overlay scope = %#v", env.Scope)
	}
	route, err = getUnstructured(r, httpRouteGVK, "team", routeName)
	if err != nil {
		t.Fatal(err)
	}
	headers := nestedSlice(t, nestedMapAt(t, nestedSlice(t, nestedMapAt(t, nestedSlice(t, route.Object, "spec", "rules"), 0), "matches"), 0), "headers")
	if nestedMapAt(t, headers, 0)["value"] != candidate.StableID {
		t.Fatalf("route candidate header = %#v, want %q", headers, candidate.StableID)
	}
	names := routeFilterNames(t, r, "tenant-b", "gateway-b")
	if len(names) != 4 || names[0] != "team."+routeName+".0" {
		t.Fatalf("route filter patches = %v", names)
	}
	got := getModel(t, r, model)
	if got.Status.Phase != resolver.PhaseReady || got.Status.HTTPRouteName != routeName {
		t.Fatalf("served status = %#v", got.Status)
	}
}

func TestSharedModelAttachesToTenantsInIsolation(t *testing.T) {
	provider, secret := attachmentProvider("team")
	model := attachmentModel("chat", "team", "chat-model", time.Unix(100, 0), gatewayB, gatewayC)
	objects := []client.Object{provider, secret, model}
	objects = append(objects, attachmentTenantObjects("tenant-b", "tenant-b", gatewayB, "praxis")...)
	objects = append(objects, attachmentTenantObjects("tenant-c", "tenant-c", gatewayC, "praxis")...)
	r := attachmentReconciler(t, objects...)
	reconcileModel(t, r, model)
	setRouteParentStatus(t, r, model, gatewayB, metav1.ConditionTrue, "Accepted")
	setRouteParentStatus(t, r, model, gatewayC, metav1.ConditionTrue, "Accepted")
	reconcileModel(t, r, model)

	routeB, routeC := attachmentRouteName(model, gatewayB), attachmentRouteName(model, gatewayC)
	if routeB == routeC {
		t.Fatalf("attachments share HTTPRoute name %q", routeB)
	}
	for _, tc := range []struct {
		tenantNamespace string
		gateway         v1alpha1.NamespacedObjectReference
		route           string
	}{{"tenant-b", gatewayB, routeB}, {"tenant-c", gatewayC, routeC}} {
		route, err := getUnstructured(r, httpRouteGVK, "team", tc.route)
		if err != nil {
			t.Fatal(err)
		}
		if parent := nestedMapAt(t, nestedSlice(t, route.Object, "spec", "parentRefs"), 0); parent["name"] != tc.gateway.Name || parent["namespace"] != tc.gateway.Namespace {
			t.Fatalf("route %s parentRef = %#v", tc.route, parent)
		}
		env := overlayEnvelope(t, r, tc.tenantNamespace)
		if len(env.Overlay.Candidates) != 1 || env.Overlay.Candidates[0].Credential.SecretRef.Namespace != tc.tenantNamespace {
			t.Fatalf("%s overlay = %#v", tc.tenantNamespace, env.Overlay.Candidates)
		}
		for _, name := range routeFilterNames(t, r, tc.tenantNamespace, tc.gateway.Namespace) {
			if !strings.HasPrefix(name, "team."+tc.route+".") {
				t.Fatalf("%s route filter leaks route %q", tc.tenantNamespace, name)
			}
		}
	}
	got := getModel(t, r, model)
	if got.Status.Phase != resolver.PhaseReady || got.Status.HTTPRouteName != "" {
		t.Fatalf("multi-attachment status = %q httpRouteName=%q, want Ready and empty", got.Status.Phase, got.Status.HTTPRouteName)
	}
}

func TestUnsupportedAttachmentDoesNotBlockOtherAttachments(t *testing.T) {
	provider, secret := attachmentProvider("team")
	unmanaged := v1alpha1.NamespacedObjectReference{Name: "unmanaged", Namespace: "elsewhere"}
	model := attachmentModel("chat", "team", "chat-model", time.Unix(100, 0), gatewayC, unmanaged, gatewayB)
	objects := []client.Object{provider, secret, model}
	objects = append(objects, attachmentTenantObjects("tenant-b", "tenant-b", gatewayB, "praxis")...)
	objects = append(objects, attachmentTenantObjects("tenant-c", "tenant-c", gatewayC, "ipp")...)
	r := attachmentReconciler(t, objects...)
	reconcileModel(t, r, model)
	setRouteParentStatus(t, r, model, gatewayB, metav1.ConditionTrue, "Accepted")
	reconcileModel(t, r, model)

	if len(overlayEnvelope(t, r, "tenant-b").Overlay.Candidates) != 1 {
		t.Fatal("the Praxis tenant attachment must be served")
	}
	for _, gateway := range []v1alpha1.NamespacedObjectReference{gatewayC, unmanaged} {
		if _, err := getUnstructured(r, httpRouteGVK, "team", attachmentRouteName(model, gateway)); !apierrors.IsNotFound(err) {
			t.Fatalf("unsupported attachment %s received a route: %v", gatewayKey(gateway), err)
		}
	}
	var overlay corev1.ConfigMap
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: "tenant-c", Name: "routing-overlay"}, &overlay); !apierrors.IsNotFound(err) {
		t.Fatalf("non-Praxis tenant received an overlay: %v", err)
	}
	if got := getModel(t, r, model); readyReason(got) != reasonTenantNotPraxis {
		t.Fatalf("status reason = %q, want the first failing attachment %q", readyReason(got), reasonTenantNotPraxis)
	}

	unmanagedOnly := attachmentModel("other", "team", "other-model", time.Unix(100, 0), unmanaged)
	if err := r.Create(context.Background(), unmanagedOnly); err != nil {
		t.Fatal(err)
	}
	reconcileModel(t, r, unmanagedOnly)
	if got := getModel(t, r, unmanagedOnly); readyReason(got) != reasonGatewayNotManaged {
		t.Fatalf("status reason = %q, want %q", readyReason(got), reasonGatewayNotManaged)
	}
}

func TestModelNameConflictKeepsTenantModelThenOldest(t *testing.T) {
	tenantProvider, tenantSecret := attachmentProvider("tenant-b")
	ownModel := attachmentModel("own", "tenant-b", "chat-model", time.Unix(300, 0))
	providerOne, secretOne := attachmentProvider("team-one")
	providerTwo, secretTwo := attachmentProvider("team-two")
	conflicting := attachmentModel("chat", "team-one", "chat-model", time.Unix(100, 0), gatewayB)
	older := attachmentModel("older", "team-one", "shared-name", time.Unix(100, 0), gatewayB)
	newer := attachmentModel("newer", "team-two", "shared-name", time.Unix(200, 0), gatewayB)
	objects := []client.Object{tenantProvider, tenantSecret, ownModel, providerOne, secretOne, providerTwo, secretTwo, conflicting, older, newer}
	objects = append(objects, attachmentTenantObjects("tenant-b", "tenant-b", gatewayB, "praxis")...)
	r := attachmentReconciler(t, objects...)
	reconcileModel(t, r, ownModel)
	for _, model := range []*v1alpha1.ExternalModel{conflicting, older, newer} {
		reconcileModel(t, r, model)
	}
	setRouteParentStatus(t, r, older, gatewayB, metav1.ConditionTrue, "Accepted")
	reconcileModel(t, r, older)

	for _, loser := range []*v1alpha1.ExternalModel{conflicting, newer} {
		got := getModel(t, r, loser)
		if readyReason(got) != reasonModelNameConflict {
			t.Fatalf("%s reason = %q, want %q", loser.Name, readyReason(got), reasonModelNameConflict)
		}
		if _, err := getUnstructured(r, httpRouteGVK, loser.Namespace, attachmentRouteName(loser, gatewayB)); !apierrors.IsNotFound(err) {
			t.Fatalf("conflict loser %s received a route: %v", loser.Name, err)
		}
	}
	conflict := apiMeta.FindStatusCondition(gatewayResult(t, getModel(t, r, conflicting), gatewayB).Conditions, conditionReady)
	if conflict == nil || conflict.Reason != reasonModelNameConflict || !strings.Contains(conflict.Message, "tenant-b/own") {
		t.Fatalf("conflict result %#v does not name the winner", conflict)
	}
	env := overlayEnvelope(t, r, "tenant-b")
	var ids []string
	for _, candidate := range env.Overlay.Candidates {
		ids = append(ids, candidate.StableID)
	}
	if strings.Join(ids, ",") != "own/provider-openai,team-one/older/team-one/openai" {
		t.Fatalf("overlay candidates = %v, want the tenant's own model first and only the oldest attached winner", ids)
	}
}

// renameModel changes the model's client-facing name and returns the requests
// the model-name watch enqueues for that update.
func renameModel(t *testing.T, r *Reconciler, model *v1alpha1.ExternalModel, modelName string) []reconcile.Request {
	t.Helper()
	old := getModel(t, r, model)
	renamed := old.DeepCopy()
	renamed.Spec.ModelName = modelName
	if err := r.Update(context.Background(), renamed); err != nil {
		t.Fatal(err)
	}
	q := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[reconcile.Request]())
	defer q.ShutDown()
	handler.EnqueueRequestsFromMapFunc(r.sameNameModels).Update(context.Background(), event.UpdateEvent{ObjectOld: old, ObjectNew: renamed}, q)
	var requests []reconcile.Request
	for q.Len() > 0 {
		request, _ := q.Get()
		requests = append(requests, request)
		q.Done(request)
	}
	return requests
}

func attachmentRouteExists(t *testing.T, r *Reconciler, model *v1alpha1.ExternalModel) bool {
	t.Helper()
	_, err := getUnstructured(r, httpRouteGVK, model.Namespace, attachmentRouteName(model, gatewayB))
	if err != nil && !apierrors.IsNotFound(err) {
		t.Fatal(err)
	}
	return err == nil
}

func TestRenamedWinnerReleasesItsNameToTheWaitingModel(t *testing.T) {
	providerOne, secretOne := attachmentProvider("team-one")
	providerTwo, secretTwo := attachmentProvider("team-two")
	winner := attachmentModel("a", "team-one", "foo", time.Unix(100, 0), gatewayB)
	waiting := attachmentModel("b", "team-two", "foo", time.Unix(200, 0), gatewayB)
	objects := []client.Object{providerOne, secretOne, providerTwo, secretTwo, winner, waiting}
	objects = append(objects, attachmentTenantObjects("tenant-b", "tenant-b", gatewayB, "praxis")...)
	r := attachmentReconciler(t, objects...)
	reconcileModel(t, r, winner)
	reconcileModel(t, r, waiting)
	if reason := readyReason(getModel(t, r, waiting)); reason != reasonModelNameConflict || attachmentRouteExists(t, r, waiting) {
		t.Fatalf("waiting model reason = %q, route = %t; want %q without a route", reason, attachmentRouteExists(t, r, waiting), reasonModelNameConflict)
	}

	requests := renameModel(t, r, winner, "bar")
	if !slices.Contains(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(waiting)}) {
		t.Fatalf("renaming %s enqueued %v, want the model waiting on its previous name", winner.Name, requests)
	}
	reconcileModel(t, r, waiting)
	if reason := readyReason(getModel(t, r, waiting)); reason != reasonAttachmentPending || !attachmentRouteExists(t, r, waiting) {
		t.Fatalf("after the rename: reason = %q, route = %t; want %q with a route", reason, attachmentRouteExists(t, r, waiting), reasonAttachmentPending)
	}
}

// namedProvider returns another provider and its credential in namespace.
func namedProvider(namespace, name string) (*v1alpha1.ExternalProvider, *corev1.Secret) {
	provider, secret := attachmentProvider(namespace)
	provider.Name, provider.UID = name, types.UID("provider-"+namespace+"-"+name)
	secret.Name = name + "-key"
	provider.Spec.Auth.SecretRef.Name = secret.Name
	return provider, secret
}

func TestSameNamespaceModelsShareAnAttachedName(t *testing.T) {
	providerOne, secretOne := attachmentProvider("team-one")
	providerAlt, secretAlt := namedProvider("team-one", "alt")
	providerTwo, secretTwo := attachmentProvider("team-two")
	first := attachmentModel("first", "team-one", "foo", time.Unix(100, 0), gatewayB)
	other := attachmentModel("other", "team-two", "foo", time.Unix(200, 0), gatewayB)
	// Same-namespace models sharing a name are one selection group, so the
	// sibling uses another provider, as on the tenant-local path.
	sibling := attachmentModel("sibling", "team-one", "foo", time.Unix(300, 0), gatewayB)
	sibling.Spec.ExternalProviderRefs[0].Ref.Name = providerAlt.Name
	objects := []client.Object{providerOne, secretOne, providerAlt, secretAlt, providerTwo, secretTwo, first, other, sibling}
	objects = append(objects, attachmentTenantObjects("tenant-b", "tenant-b", gatewayB, "praxis")...)
	r := attachmentReconciler(t, objects...)
	for _, model := range []*v1alpha1.ExternalModel{first, other, sibling} {
		reconcileModel(t, r, model)
	}
	want := func(model *v1alpha1.ExternalModel, reason string, route bool) {
		t.Helper()
		if got := readyReason(getModel(t, r, model)); got != reason || attachmentRouteExists(t, r, model) != route {
			t.Fatalf("%s/%s: reason = %q, route = %t; want %q, route = %t", model.Namespace, model.Name, got, attachmentRouteExists(t, r, model), reason, route)
		}
	}
	// Conflicts apply across namespaces only: same-namespace models share the name.
	want(first, reasonAttachmentPending, true)
	want(sibling, reasonAttachmentPending, true)
	want(other, reasonModelNameConflict, false)

	// Once the oldest model releases the name, the other namespace holds it,
	// so the same-namespace sibling must be re-decided too.
	requests := renameModel(t, r, first, "bar")
	for _, model := range []*v1alpha1.ExternalModel{other, sibling} {
		if !slices.Contains(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(model)}) {
			t.Fatalf("renaming %s enqueued %v, want %s/%s", first.Name, requests, model.Namespace, model.Name)
		}
		reconcileModel(t, r, model)
	}
	want(other, reasonAttachmentPending, true)
	want(sibling, reasonModelNameConflict, false)
}

func TestSameNamespaceAliasesRepeatingAProviderExcludeOnlyThemselves(t *testing.T) {
	tenantProvider, tenantSecret := attachmentProvider("tenant-b")
	ownModel := attachmentModel("own", "tenant-b", "own-model", time.Unix(300, 0))
	provider, secret := attachmentProvider("team")
	first := attachmentModel("first", "team", "foo", time.Unix(100, 0), gatewayB)
	second := attachmentModel("second", "team", "foo", time.Unix(200, 0), gatewayB)
	objects := []client.Object{tenantProvider, tenantSecret, ownModel, provider, secret, first, second}
	objects = append(objects, attachmentTenantObjects("tenant-b", "tenant-b", gatewayB, "praxis")...)
	r := attachmentReconciler(t, objects...)
	for _, model := range []*v1alpha1.ExternalModel{first, second, ownModel} {
		reconcileModel(t, r, model)
	}
	for _, model := range []*v1alpha1.ExternalModel{first, second} {
		if got := getModel(t, r, model); readyReason(got) != reasonReconcileFailed || attachmentRouteExists(t, r, model) {
			t.Fatalf("%s: reason = %q, route = %t; want %q without a route", model.Name, readyReason(got), attachmentRouteExists(t, r, model), reasonReconcileFailed)
		}
	}
	// The rejected group never reaches the overlay or blocks the tenant's own model.
	if env := overlayEnvelope(t, r, "tenant-b"); len(env.Overlay.Candidates) != 1 || env.Overlay.Candidates[0].Name != "own-model" {
		t.Fatalf("overlay candidates = %#v, want only the tenant's own model", env.Overlay.Candidates)
	}
}

func TestDeletingProviderExcludesAttachedModel(t *testing.T) {
	provider, secret := attachmentProvider("team")
	provider.Finalizers = []string{"example.com/hold"}
	model := attachmentModel("chat", "team", "chat-model", time.Unix(100, 0), gatewayB)
	objects := []client.Object{provider, secret, model}
	objects = append(objects, attachmentTenantObjects("tenant-b", "tenant-b", gatewayB, "praxis")...)
	r := attachmentReconciler(t, objects...)
	reconcileModel(t, r, model)
	if err := r.Delete(context.Background(), provider); err != nil {
		t.Fatal(err)
	}
	reconcileModel(t, r, model)
	if reason := readyReason(getModel(t, r, model)); reason != reasonProviderNotReady {
		t.Fatalf("reason = %q, want %q", reason, reasonProviderNotReady)
	}
	if _, err := getUnstructured(r, httpRouteGVK, "team", attachmentRouteName(model, gatewayB)); !apierrors.IsNotFound(err) {
		t.Fatalf("a model whose provider is being deleted kept its route: %v", err)
	}
}

func TestOwnTenantGatewayRefChangesNoResources(t *testing.T) {
	render := func(gateways ...v1alpha1.NamespacedObjectReference) (*Reconciler, *v1alpha1.ExternalModel) {
		provider, secret := attachmentProvider("tenant-b")
		model := attachmentModel("chat", "tenant-b", "chat-model", time.Unix(100, 0), gateways...)
		r := attachmentReconciler(t, append([]client.Object{provider, secret, model}, attachmentTenantObjects("tenant-b", "tenant-b", gatewayB, "praxis")...)...)
		r.PublishOverlay = nil
		reconcileModel(t, r, model)
		return r, model
	}
	legacy, _ := render()
	attached, attachedModel := render(gatewayB)
	for _, object := range []struct {
		gvk             schema.GroupVersionKind
		namespace, name string
	}{
		{httpRouteGVK, "tenant-b", "external-model-chat"},
		{schema.GroupVersionKind{Version: "v1", Kind: "Service"}, "tenant-b", "provider-openai"},
		{schema.GroupVersionKind{Group: "networking.istio.io", Version: "v1", Kind: "ServiceEntry"}, "tenant-b", "provider-openai"},
		{destinationRuleGVK, "gateway-b", "provider-openai-tenant-b"},
		{schema.GroupVersionKind{Group: "networking.istio.io", Version: "v1alpha3", Kind: "EnvoyFilter"}, "gateway-b", tenant.PayloadProcessingExternalModelEnvoyFilterName("tenant-b")},
	} {
		want, err := getUnstructured(legacy, object.gvk, object.namespace, object.name)
		if err != nil {
			t.Fatalf("legacy %s: %v", object.gvk.Kind, err)
		}
		got, err := getUnstructured(attached, object.gvk, object.namespace, object.name)
		if err != nil {
			t.Fatalf("own-gateway %s: %v", object.gvk.Kind, err)
		}
		wantSpec, gotSpec := mustJSON(t, want.Object["spec"]), mustJSON(t, got.Object["spec"])
		if wantSpec != gotSpec || !maps.Equal(want.GetLabels(), got.GetLabels()) {
			t.Fatalf("%s differs:\nlegacy %s %v\nown-gateway %s %v", object.gvk.Kind, wantSpec, want.GetLabels(), gotSpec, got.GetLabels())
		}
	}
	if legacyEnv, gotEnv := overlayEnvelope(t, legacy, "tenant-b"), overlayEnvelope(t, attached, "tenant-b"); legacyEnv.Revision != gotEnv.Revision {
		t.Fatalf("overlay digest differs: %s vs %s", legacyEnv.Revision.Value, gotEnv.Revision.Value)
	}
	got := getModel(t, attached, attachedModel)
	if got.Status.Phase != resolver.PhaseReady || got.Status.HTTPRouteName != "external-model-chat" {
		t.Fatalf("own-gateway status = %#v", got.Status)
	}
	// Only the home attachment's result is added; its deletion stays the
	// existing home path.
	result := gatewayResult(t, got, gatewayB)
	if result.HTTPRouteRef == nil || *result.HTTPRouteRef != (v1alpha1.NamespacedObjectReference{Name: "external-model-chat", Namespace: "tenant-b"}) ||
		!apiMeta.IsStatusConditionTrue(result.Conditions, conditionReady) || !slices.Equal(got.Finalizers, []string{externalModelFinalizer}) {
		t.Fatalf("own-gateway result = %#v, finalizers %v", result, got.Finalizers)
	}
}

func TestGatewayStatusReportsEachAttachment(t *testing.T) {
	provider, secret := attachmentProvider("team")
	model := attachmentModel("chat", "team", "chat-model", time.Unix(100, 0), gatewayB)
	objects := []client.Object{provider, secret, model}
	objects = append(objects, attachmentTenantObjects("tenant-b", "tenant-b", gatewayB, "praxis")...)
	objects = append(objects, attachmentTenantObjects("tenant-c", "tenant-c", gatewayC, "praxis")...)
	r := attachmentReconciler(t, objects...)
	reconcileModel(t, r, model)
	setRouteParentStatus(t, r, model, gatewayB, metav1.ConditionTrue, "Accepted")
	reconcileModel(t, r, model)
	setRefs := func(refs ...v1alpha1.NamespacedObjectReference) *v1alpha1.ExternalModel {
		t.Helper()
		got := getModel(t, r, model)
		got.Spec.GatewayRefs = refs
		if err := r.Update(context.Background(), got); err != nil {
			t.Fatal(err)
		}
		reconcileModel(t, r, model)
		return getModel(t, r, model)
	}
	routeB := &v1alpha1.NamespacedObjectReference{Name: attachmentRouteName(model, gatewayB), Namespace: "team"}
	assertResult := func(got *v1alpha1.ExternalModel, gateway v1alpha1.NamespacedObjectReference, reason string, route *v1alpha1.NamespacedObjectReference) {
		t.Helper()
		result := gatewayResult(t, got, gateway)
		ready := apiMeta.FindStatusCondition(result.Conditions, conditionReady)
		routeMatches := (route == nil) == (result.HTTPRouteRef == nil) && (route == nil || *route == *result.HTTPRouteRef)
		if ready == nil || ready.Reason != reason || ready.ObservedGeneration != got.Generation ||
			(ready.Status == metav1.ConditionTrue) != (route != nil) || !routeMatches {
			t.Fatalf("%s result = %#v, want reason %q and route %v", gatewayKey(gateway), result, reason, route)
		}
	}

	got := getModel(t, r, model)
	assertResult(got, gatewayB, reasonReconciled, routeB)
	if got.Status.HTTPRouteName != routeB.Name {
		t.Fatalf("single attachment httpRouteName = %q, want %q", got.Status.HTTPRouteName, routeB.Name)
	}

	// A second attachment: the served one keeps its result while the new one
	// is pending, and the single-attachment alias is cleared.
	got = setRefs(gatewayB, gatewayC)
	if len(got.Status.Gateways) != 2 || got.Status.HTTPRouteName != "" || readyReason(got) != reasonAttachmentPending {
		t.Fatalf("status = %#v, want two results, no httpRouteName and the pending attachment reported", got.Status)
	}
	assertResult(got, gatewayB, reasonReconciled, routeB)
	assertResult(got, gatewayC, reasonAttachmentPending, nil)

	// Partial failure: only the rejected attachment fails.
	setRouteParentStatus(t, r, model, gatewayC, metav1.ConditionFalse, "NotAllowedByListeners")
	reconcileModel(t, r, model)
	got = getModel(t, r, model)
	assertResult(got, gatewayB, reasonReconciled, routeB)
	assertResult(got, gatewayC, "NotAllowedByListeners", nil)

	// Removing an attachment removes its result.
	got = setRefs(gatewayC)
	if len(got.Status.Gateways) != 1 {
		t.Fatalf("status.gateways = %#v, want only gw-c", got.Status.Gateways)
	}
	assertResult(got, gatewayC, "NotAllowedByListeners", nil)

	got = setRefs()
	if len(got.Status.Gateways) != 0 || len(got.Finalizers) != 0 {
		t.Fatalf("detached model keeps results %#v or finalizers %v", got.Status.Gateways, got.Finalizers)
	}
	assertTransportCollected(t, r, "team", "chat")
}

func mustJSON(t *testing.T, v any) string {
	t.Helper()
	data, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func TestModelWithoutTenantOrGatewayRefsIsTenantNotResolved(t *testing.T) {
	provider, secret := attachmentProvider("team")
	model := attachmentModel("chat", "team", "chat-model", time.Unix(100, 0))
	r := attachmentReconciler(t, provider, secret, model)
	// A route name another writer (legacy IPP) published is left alone.
	written := getModel(t, r, model)
	written.Status.HTTPRouteName = "chat"
	if err := r.Status().Update(context.Background(), written); err != nil {
		t.Fatal(err)
	}
	reconcileModel(t, r, model)
	got := getModel(t, r, model)
	condition := apiMeta.FindStatusCondition(got.Status.Conditions, conditionReady)
	if got.Status.Phase != modelPhasePending || condition == nil || condition.Status != metav1.ConditionFalse ||
		condition.Reason != reasonTenantNotResolved || !strings.Contains(condition.Message, "spec.gatewayRefs") || got.Status.HTTPRouteName != "chat" {
		t.Fatalf("status = %#v", got.Status)
	}
	if _, err := getUnstructured(r, httpRouteGVK, "team", "external-model-chat"); !apierrors.IsNotFound(err) {
		t.Fatalf("serving state created without a tenant: %v", err)
	}
}

var defaultGateway = v1alpha1.NamespacedObjectReference{Name: "maas-default-gateway", Namespace: "openshift-ingress"}

// implicitAttachmentFixture reconciles a model without gatewayRefs, from a
// namespace with no MaasTenantConfig, next to the given default tenant.
func implicitAttachmentFixture(t *testing.T, defaultTenant ...client.Object) (*Reconciler, *v1alpha1.ExternalModel) {
	t.Helper()
	provider, secret := attachmentProvider("team")
	model := attachmentModel("chat", "team", "chat-model", time.Unix(100, 0))
	r := attachmentReconciler(t, append([]client.Object{provider, secret, model}, defaultTenant...)...)
	reconcileModel(t, r, model)
	return r, model
}

// servedImplicitFixture serves that model through the default tenant's Gateway.
func servedImplicitFixture(t *testing.T) (*Reconciler, *v1alpha1.ExternalModel) {
	t.Helper()
	r, model := implicitAttachmentFixture(t, attachmentTenantObjects(tenant.DefaultAITenantName, "models-as-a-service", defaultGateway, "praxis")...)
	setRouteParentStatus(t, r, model, defaultGateway, metav1.ConditionTrue, "Accepted")
	reconcileModel(t, r, model)
	return r, model
}

func assertImplicitAttachmentRemoved(t *testing.T, r *Reconciler, model *v1alpha1.ExternalModel) {
	t.Helper()
	if _, err := getUnstructured(r, httpRouteGVK, "team", attachmentRouteName(model, defaultGateway)); !apierrors.IsNotFound(err) {
		t.Fatalf("implicit attachment route remains: %v", err)
	}
	if _, err := getUnstructured(r, destinationRuleGVK, "openshift-ingress", attachmentDestinationRuleName(defaultGateway, "team", "openai")); !apierrors.IsNotFound(err) {
		t.Fatalf("implicit attachment DestinationRule remains: %v", err)
	}
	var copied corev1.Secret
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: "models-as-a-service", Name: credentialCopyName("models-as-a-service", "team", "openai-key", "api-key", "")}, &copied); !apierrors.IsNotFound(err) {
		t.Fatalf("implicit attachment credential copy remains: %v", err)
	}
	if got := getModel(t, r, model); controllerutil.ContainsFinalizer(got, attachmentFinalizer) || len(got.Status.Gateways) > 0 {
		t.Fatalf("model keeps its implicit attachment: finalizers %v, gateways %#v", got.Finalizers, got.Status.Gateways)
	}
}

func TestModelWithoutGatewayRefsIsServedByTheDefaultTenant(t *testing.T) {
	r, model := servedImplicitFixture(t)
	got := getModel(t, r, model)
	routeName := attachmentRouteName(model, defaultGateway)
	if got.Status.Phase != resolver.PhaseReady || got.Status.HTTPRouteName != routeName || len(got.Spec.GatewayRefs) != 0 {
		t.Fatalf("model = %#v", got)
	}
	if ref := gatewayResult(t, got, defaultGateway).HTTPRouteRef; ref == nil || ref.Name != routeName {
		t.Fatalf("default Gateway result routes through %#v, want %s", ref, routeName)
	}
	if !controllerutil.ContainsFinalizer(got, attachmentFinalizer) || controllerutil.ContainsFinalizer(got, externalModelFinalizer) {
		t.Fatalf("finalizers = %v, want only the attachment finalizer", got.Finalizers)
	}
	candidates := overlayEnvelope(t, r, "models-as-a-service").Overlay.Candidates
	if len(candidates) != 1 || candidates[0].StableID != "team/chat/team/openai" ||
		candidates[0].Credential.SecretRef.Name != credentialCopyName("models-as-a-service", "team", "openai-key", "api-key", "") {
		t.Fatalf("default tenant overlay = %#v", candidates)
	}
	if _, err := getUnstructured(r, destinationRuleGVK, "openshift-ingress", attachmentDestinationRuleName(defaultGateway, "team", "openai")); err != nil {
		t.Fatalf("attachment DestinationRule: %v", err)
	}
}

func TestImplicitModelWaitsForASteadyDefaultTenant(t *testing.T) {
	for name, mutate := range map[string]func(ait, mtc *unstructured.Unstructured) []client.Object{
		"not steady": func(ait, mtc *unstructured.Unstructured) []client.Object {
			annotations := mtc.GetAnnotations()
			annotations[tenant.AnnotationPayloadProcessingStatus] = tenant.PayloadProcessingStatusCleanupComplete
			mtc.SetAnnotations(annotations)
			return []client.Object{ait, mtc}
		},
		"deleting": func(ait, mtc *unstructured.Unstructured) []client.Object {
			now := metav1.Now()
			ait.SetFinalizers([]string{"maas.opendatahub.io/test"})
			ait.SetDeletionTimestamp(&now)
			return []client.Object{ait, mtc}
		},
		"config missing": func(ait, _ *unstructured.Unstructured) []client.Object { return []client.Object{ait} },
	} {
		t.Run(name, func(t *testing.T) {
			ait := controllerAITenant(tenant.DefaultAITenantName, "models-as-a-service", "models-as-a-service", defaultGateway.Name, defaultGateway.Namespace)
			mtc := controllerMaasTenantConfig("models-as-a-service", tenant.DefaultAITenantName, "models-as-a-service", "praxis")
			// An explicit attachment to the same tenant waits the same way.
			explicit := attachmentModel("explicit", "team", "explicit-model", time.Unix(100, 0), defaultGateway)
			r, model := implicitAttachmentFixture(t, append(mutate(ait, mtc), explicit)...)
			reconcileModel(t, r, explicit)
			for _, m := range []*v1alpha1.ExternalModel{model, explicit} {
				got := getModel(t, r, m)
				if got.Status.Phase != modelPhasePending || readyReason(got) != reasonTenantNotSteady || gatewayResult(t, got, defaultGateway).HTTPRouteRef != nil {
					t.Fatalf("%s status = %#v", m.Name, got.Status)
				}
				if _, err := getUnstructured(r, httpRouteGVK, "team", attachmentRouteName(m, defaultGateway)); !apierrors.IsNotFound(err) {
					t.Fatalf("%s attached to a default tenant that is not steady: %v", m.Name, err)
				}
			}
		})
	}
}

func TestDefaultTenantOnIPPLeavesImplicitModelAlone(t *testing.T) {
	r, model := implicitAttachmentFixture(t, attachmentTenantObjects(tenant.DefaultAITenantName, "models-as-a-service", defaultGateway, "ipp")...)
	if got := getModel(t, r, model); got.Status.Phase != "" || len(got.Status.Conditions) > 0 || len(got.Finalizers) > 0 {
		t.Fatalf("model left to IPP was changed: finalizers %v, status %#v", got.Finalizers, got.Status)
	}
	if _, err := getUnstructured(r, httpRouteGVK, "team", attachmentRouteName(model, defaultGateway)); !apierrors.IsNotFound(err) {
		t.Fatalf("attached to a default tenant on IPP: %v", err)
	}

	// A switch to IPP removes a served implicit attachment.
	r, model = servedImplicitFixture(t)
	mtc := tenant.NewMaasTenantConfig()
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: "models-as-a-service", Name: tenant.MaasTenantConfigInstanceName}, mtc); err != nil {
		t.Fatal(err)
	}
	annotations := mtc.GetAnnotations()
	annotations[tenant.AnnotationPayloadProcessingType] = tenant.PayloadProcessingBackendIPP
	mtc.SetAnnotations(annotations)
	if err := r.Update(context.Background(), mtc); err != nil {
		t.Fatal(err)
	}
	reconcileModel(t, r, model)
	assertImplicitAttachmentRemoved(t, r, model)
}

func TestImplicitAttachmentIsReplacedByGatewayRefsOrTenantConfig(t *testing.T) {
	t.Run("gatewayRefs", func(t *testing.T) {
		r, model := servedImplicitFixture(t)
		for _, object := range attachmentTenantObjects("tenant-b", "tenant-b", gatewayB, "praxis") {
			if err := r.Create(context.Background(), object); err != nil {
				t.Fatal(err)
			}
		}
		got := getModel(t, r, model)
		got.Spec.GatewayRefs = []v1alpha1.NamespacedObjectReference{gatewayB}
		if err := r.Update(context.Background(), got); err != nil {
			t.Fatal(err)
		}
		reconcileModel(t, r, model)
		if _, err := getUnstructured(r, httpRouteGVK, "team", attachmentRouteName(model, defaultGateway)); !apierrors.IsNotFound(err) {
			t.Fatalf("implicit attachment route remains: %v", err)
		}
		if !attachmentRouteExists(t, r, model) {
			t.Fatal("explicit attachment route missing")
		}
		if _, err := getUnstructured(r, destinationRuleGVK, "openshift-ingress", attachmentDestinationRuleName(defaultGateway, "team", "openai")); !apierrors.IsNotFound(err) {
			t.Fatalf("implicit attachment DestinationRule remains: %v", err)
		}
	})
	t.Run("MaasTenantConfig", func(t *testing.T) {
		r, model := servedImplicitFixture(t)
		for _, object := range attachmentTenantObjects("team", "team", gatewayB, "praxis") {
			if err := r.Create(context.Background(), object); err != nil {
				t.Fatal(err)
			}
		}
		// The provider serves the new tenant's own model only once the
		// tenant has applied its transport.
		if _, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(model)}); !errors.Is(err, resolver.ErrNoRoutes) {
			t.Fatalf("reconcile before the provider's tenant transport = %v, want %v", err, resolver.ErrNoRoutes)
		}
		assertImplicitAttachmentRemoved(t, r, model)
		if _, err := getUnstructured(r, httpRouteGVK, "team", modelRouteName(model.Name)); !apierrors.IsNotFound(err) {
			t.Fatalf("home route published before the provider's tenant transport: %v", err)
		}
		if _, err := r.ReconcileProvider(context.Background(), reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "team", Name: "openai"}}); err != nil {
			t.Fatal(err)
		}
		reconcileModel(t, r, model)
		if _, err := getUnstructured(r, httpRouteGVK, "team", modelRouteName(model.Name)); err != nil {
			t.Fatalf("home route: %v", err)
		}
	})
}

func TestDefaultTenantEventsEnqueueImplicitModels(t *testing.T) {
	implicit := attachmentModel("chat", "team", "chat-model", time.Unix(100, 0))
	implicit.Spec.ExternalProviderRefs[0].Auth = &v1alpha1.AuthConfig{Type: "apikey", SecretRef: v1alpha1.NameReference{Name: "model-key"}}
	home := attachmentModel("chat", "tenant-b", "chat-model", time.Unix(100, 0))
	defaultTenant := controllerAITenant(tenant.DefaultAITenantName, "models-as-a-service", "models-as-a-service", defaultGateway.Name, defaultGateway.Namespace)
	tenantB := controllerAITenant("tenant-b", "models-as-a-service", "tenant-b", gatewayB.Name, gatewayB.Namespace)
	r := controllerTestClient(t, implicit, home, defaultTenant, tenantB,
		controllerMaasTenantConfig("models-as-a-service", tenant.DefaultAITenantName, "models-as-a-service", "ipp"),
		controllerMaasTenantConfig("tenant-b", "tenant-b", "models-as-a-service", "praxis"))
	want := []reconcile.Request{{NamespacedName: client.ObjectKeyFromObject(implicit)}}
	// Enqueued whatever the default tenant selects, so a switch to IPP releases it.
	if got := r.gatewayAttachedModels(context.Background(), defaultTenant); !slices.Equal(got, want) {
		t.Fatalf("default tenant event enqueued %v, want %v", got, want)
	}
	if got := r.gatewayAttachedModels(context.Background(), tenantB); len(got) != 0 {
		t.Fatalf("another tenant's event enqueued %v", got)
	}
	if got := r.sameNameModels(context.Background(), home); !slices.Equal(got, want) {
		t.Fatalf("model-name event enqueued %v, want %v", got, want)
	}
	// Its credential copy follows the override Secret.
	if got := r.secretModels(context.Background(), &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "model-key", Namespace: "team"}}); !slices.Equal(got, want) {
		t.Fatalf("override Secret event enqueued %v, want %v", got, want)
	}
}

func TestTenantNamespacesAndSingleNamespaceControllersAttachNothingImplicitly(t *testing.T) {
	model := attachmentModel("chat", "team", "chat-model", time.Unix(100, 0))
	objects := append(attachmentTenantObjects(tenant.DefaultAITenantName, "models-as-a-service", defaultGateway, "praxis"), model)
	implicit := func(r *Reconciler) bool {
		t.Helper()
		attached, err := r.implicitlyAttached(context.Background(), model)
		if err != nil {
			t.Fatal(err)
		}
		return attached
	}
	if !implicit(controllerTestClient(t, objects...)) {
		t.Fatal("model outside tenant namespaces is not attached implicitly")
	}
	// An AITenant claims the namespace before its MaasTenantConfig exists.
	if implicit(controllerTestClient(t, append(objects, controllerAITenant("team", "models-as-a-service", "team", gatewayB.Name, gatewayB.Namespace))...)) {
		t.Fatal("a tenant namespace without its MaasTenantConfig attached to the default tenant")
	}
	r := controllerTestClient(t, objects...)
	r.Namespace = "team"
	if implicit(r) {
		t.Fatal("a single-namespace controller attached a model")
	}
}

func TestModelIneligibleForImplicitAttachmentIsTenantNotResolved(t *testing.T) {
	for name, setup := range map[string]func() (*Reconciler, *v1alpha1.ExternalModel){
		// The default tenant's IPP does not report a model of a namespace an
		// AITenant claims before its MaasTenantConfig exists.
		"claimed namespace, default tenant on IPP": func() (*Reconciler, *v1alpha1.ExternalModel) {
			provider, secret := attachmentProvider("team")
			model := attachmentModel("chat", "team", "chat-model", time.Unix(100, 0))
			return attachmentReconciler(t, append(attachmentTenantObjects(tenant.DefaultAITenantName, "models-as-a-service", defaultGateway, "ipp"),
				provider, secret, model, controllerAITenant("team", "models-as-a-service", "team", gatewayB.Name, gatewayB.Namespace))...), model
		},
		"single-namespace controller, no default tenant": func() (*Reconciler, *v1alpha1.ExternalModel) {
			provider, secret := attachmentProvider("team")
			model := attachmentModel("chat", "team", "chat-model", time.Unix(100, 0))
			r := attachmentReconciler(t, provider, secret, model)
			r.Namespace = "team"
			return r, model
		},
	} {
		t.Run(name, func(t *testing.T) {
			r, model := setup()
			reconcileModel(t, r, model)
			got := getModel(t, r, model)
			condition := apiMeta.FindStatusCondition(got.Status.Conditions, conditionReady)
			if got.Status.Phase != modelPhasePending || condition == nil || condition.Reason != reasonTenantNotResolved || strings.Contains(condition.Message, "default tenant") {
				t.Fatalf("status = %#v", got.Status)
			}
		})
	}
}

// TestProviderOutsideTenantNamespacesReportsItsOwnConfiguration pins the split
// between the two provider status paths. A namespace no AITenant claims has no
// tenant-local transport, so its provider reports its own configuration and is
// not claimed. Once a tenant claims the namespace the provider becomes the
// controller's to own: it gains the cleanup finalizer before the tenant gate
// and waits for MaasTenantConfig instead of reporting stale local state.
func TestProviderOutsideTenantNamespacesReportsItsOwnConfiguration(t *testing.T) {
	provider, secret := attachmentProvider("team")
	r := attachmentReconciler(t, provider, secret)
	getProvider := func() v1alpha1.ExternalProvider {
		t.Helper()
		var got v1alpha1.ExternalProvider
		if err := r.Get(context.Background(), client.ObjectKeyFromObject(provider), &got); err != nil {
			t.Fatal(err)
		}
		return got
	}
	providerStatus := func() (string, string, string) {
		t.Helper()
		got := getProvider()
		condition := apiMeta.FindStatusCondition(got.Status.Conditions, conditionReady)
		if condition == nil {
			t.Fatalf("provider status = %#v", got.Status)
		}
		return got.Status.Phase, condition.Reason, condition.Message
	}
	reconcileProvider := func() {
		t.Helper()
		if _, err := r.ReconcileProvider(context.Background(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(provider)}); err != nil {
			t.Fatal(err)
		}
	}
	// No AITenant claims "team", so the provider is served only through Gateway
	// attachments: its status describes its own Secret reference, and there is
	// no tenant transport to claim or clean up.
	if phase, reason, _ := providerStatus(); phase != resolver.PhaseReady || reason != reasonValidated {
		t.Fatalf("provider = %s/%s, want Ready/%s", phase, reason, reasonValidated)
	}
	if got := getProvider(); controllerutil.ContainsFinalizer(&got, externalProviderFinalizer) {
		t.Fatalf("standalone provider was claimed: finalizers = %v", got.Finalizers)
	}
	// Without tenant-local transport it is no input to a tenant's own models.
	if inputs, err := r.providerInputs(context.Background(), "team"); err != nil || len(inputs) != 0 {
		t.Fatalf("providerInputs = %v, %v", inputs, err)
	}
	if err := r.Delete(context.Background(), secret); err != nil {
		t.Fatal(err)
	}
	reconcileProvider()
	if phase, _, _ := providerStatus(); phase != providerPhaseFailed {
		t.Fatalf("provider without its Secret = %s, want %s", phase, providerPhaseFailed)
	}
	// A tenant claim moves the same provider to the owned path. maas-controller
	// has not created the MaasTenantConfig yet, which is Praxis-default, so the
	// provider is claimed and waits rather than reporting its own configuration.
	if err := r.Create(context.Background(), controllerAITenant("tenant", "models-as-a-service", "team", "gateway", "gateway-system")); err != nil {
		t.Fatal(err)
	}
	reconcileProvider()
	if got := getProvider(); !controllerutil.ContainsFinalizer(&got, externalProviderFinalizer) {
		t.Fatalf("tenant provider was not claimed: finalizers = %v", got.Finalizers)
	}
	phase, reason, message := providerStatus()
	if phase != providerPhasePending || reason != reasonTenantNotReady {
		t.Fatalf("tenant provider without its MaasTenantConfig = %s/%s, want Pending/%s", phase, reason, reasonTenantNotReady)
	}
	if !strings.Contains(message, "MaasTenantConfig/default-tenant is missing") {
		t.Fatalf("provider message = %q, want it to name the missing MaasTenantConfig", message)
	}
}

func TestTenantModelBoundElsewhereIsNotServedByItsOwnTenant(t *testing.T) {
	gatewayA := v1alpha1.NamespacedObjectReference{Name: "gw-a", Namespace: "gateway-a"}
	provider, secret := attachmentProvider("tenant-a")
	model := attachmentModel("chat", "tenant-a", "chat-model", time.Unix(100, 0), gatewayB)
	objects := append([]client.Object{provider, secret, model}, attachmentTenantObjects("tenant-a", "tenant-a", gatewayA, "praxis")...)
	r := attachmentReconciler(t, objects...)
	reconcileModel(t, r, model)
	if _, err := getUnstructured(r, httpRouteGVK, "tenant-a", "external-model-chat"); !apierrors.IsNotFound(err) {
		t.Fatalf("a model bound to another Gateway fell back to its own tenant: %v", err)
	}
	if got := getModel(t, r, model); readyReason(got) != reasonGatewayNotManaged {
		t.Fatalf("reason = %q, want %q", readyReason(got), reasonGatewayNotManaged)
	}
}

func TestTooManyProvidersExcludesOnlyThatModel(t *testing.T) {
	objects := attachmentTenantObjects("tenant-b", "tenant-b", gatewayB, "praxis")
	model := attachmentModel("wide", "team", "wide-model", time.Unix(100, 0), gatewayB)
	model.Spec.ExternalProviderRefs = nil
	for _, name := range []string{"p1", "p2", "p3", "p4", "p5", "p6", "p7", "p8"} {
		provider, secret := attachmentProvider("team")
		provider.Name, provider.UID = name, types.UID("uid-"+name)
		objects = append(objects, provider)
		if name == "p1" {
			objects = append(objects, secret)
		}
		model.Spec.ExternalProviderRefs = append(model.Spec.ExternalProviderRefs, v1alpha1.ExternalProviderRef{
			Ref: v1alpha1.ExternalProviderReference{Name: name}, TargetModel: "gpt-4o", APIFormat: "openai-chat", Path: "/v1/chat/completions",
		})
	}
	r := attachmentReconciler(t, append(objects, model)...)
	reconcileModel(t, r, model)
	if got := getModel(t, r, model); readyReason(got) != reasonTooManyProviders {
		t.Fatalf("reason = %q, want %q", readyReason(got), reasonTooManyProviders)
	}
	if _, err := getUnstructured(r, httpRouteGVK, "team", attachmentRouteName(model, gatewayB)); !apierrors.IsNotFound(err) {
		t.Fatalf("over-budget model received a route: %v", err)
	}
}

func TestSevenMessagesProvidersFitAttachmentRouteLimit(t *testing.T) {
	objects := attachmentTenantObjects("tenant-b", "tenant-b", gatewayB, "praxis")
	model := attachmentModel("wide", "team", "wide-model", time.Unix(100, 0), gatewayB)
	model.Spec.ExternalProviderRefs = nil
	for _, name := range []string{"p1", "p2", "p3", "p4", "p5", "p6", "p7"} {
		provider, secret := attachmentProvider("team")
		provider.Name, provider.UID = name, types.UID("uid-"+name)
		provider.Spec.Provider = "anthropic"
		objects = append(objects, provider)
		if name == "p1" {
			objects = append(objects, secret)
		}
		model.Spec.ExternalProviderRefs = append(model.Spec.ExternalProviderRefs, v1alpha1.ExternalProviderRef{
			Ref: v1alpha1.ExternalProviderReference{Name: name}, TargetModel: "claude", APIFormat: "messages", Path: "/v1/messages",
		})
	}
	r := attachmentReconciler(t, append(objects, model)...)
	setControllerTranslationEnabled(t, r, "tenant-b", true)
	reconcileModel(t, r, model)
	setRouteParentStatus(t, r, model, gatewayB, metav1.ConditionTrue, "Accepted")
	reconcileModel(t, r, model)
	if got := getModel(t, r, model); got.Status.Phase != resolver.PhaseReady {
		t.Fatalf("seven Messages providers should fit 16 rules: phase=%q reason=%q", got.Status.Phase, readyReason(got))
	}
	if _, err := getUnstructured(r, httpRouteGVK, "team", attachmentRouteName(model, gatewayB)); err != nil {
		t.Fatalf("16-rule Messages attachment has no route: %v", err)
	}
}

func TestEightMessagesProvidersExceedAttachmentRouteLimit(t *testing.T) {
	objects := attachmentTenantObjects("tenant-b", "tenant-b", gatewayB, "praxis")
	model := attachmentModel("wide", "team", "wide-model", time.Unix(100, 0), gatewayB)
	model.Spec.ExternalProviderRefs = nil
	for _, name := range []string{"p1", "p2", "p3", "p4", "p5", "p6", "p7", "p8"} {
		provider, secret := attachmentProvider("team")
		provider.Name, provider.UID = name, types.UID("uid-"+name)
		provider.Spec.Provider = "anthropic"
		objects = append(objects, provider)
		if name == "p1" {
			objects = append(objects, secret)
		}
		model.Spec.ExternalProviderRefs = append(model.Spec.ExternalProviderRefs, v1alpha1.ExternalProviderRef{
			Ref: v1alpha1.ExternalProviderReference{Name: name}, TargetModel: "claude", APIFormat: "messages", Path: "/v1/messages",
		})
	}
	r := attachmentReconciler(t, append(objects, model)...)
	setControllerTranslationEnabled(t, r, "tenant-b", true)
	reconcileModel(t, r, model)
	if got := getModel(t, r, model); readyReason(got) != reasonTooManyProviders {
		t.Fatalf("reason = %q, want %q for 18 HTTPRoute rules", readyReason(got), reasonTooManyProviders)
	}
	if _, err := getUnstructured(r, httpRouteGVK, "team", attachmentRouteName(model, gatewayB)); !apierrors.IsNotFound(err) {
		t.Fatalf("over-budget Messages attachment received a route: %v", err)
	}
}

func TestUnservedHomeAttachmentBlocksAggregateReady(t *testing.T) {
	gatewayA := v1alpha1.NamespacedObjectReference{Name: "gw-a", Namespace: "gateway-a"}
	provider, secret := attachmentProvider("tenant-a")
	model := attachmentModel("chat", "tenant-a", "chat-model", time.Unix(100, 0), gatewayA, gatewayB)
	objects := []client.Object{provider, secret, model}
	objects = append(objects, attachmentTenantObjects("tenant-a", "tenant-a", gatewayA, "ipp")...)
	objects = append(objects, attachmentTenantObjects("tenant-b", "tenant-b", gatewayB, "praxis")...)
	r := attachmentReconciler(t, objects...)
	reconcileModel(t, r, model)
	setRouteParentStatus(t, r, model, gatewayB, metav1.ConditionTrue, "Accepted")
	reconcileModel(t, r, model)
	if len(overlayEnvelope(t, r, "tenant-b").Overlay.Candidates) != 1 {
		t.Fatal("the Praxis attachment must still be served")
	}
	if got := getModel(t, r, model); got.Status.Phase == resolver.PhaseReady || readyReason(got) != reasonTenantNotPraxis {
		t.Fatalf("status = %q/%q, want the unserved home attachment reported", got.Status.Phase, readyReason(got))
	}
	// The provider's IPP cleanup keeps the transport the attachment route uses,
	// read live even while its cache has not seen the route yet.
	cached, ok := r.Client.(client.WithWatch)
	if !ok {
		t.Fatal("the test client must support watches")
	}
	r.Client = interceptor.NewClient(cached, interceptor.Funcs{
		List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if routes, ok := list.(*unstructured.UnstructuredList); ok && routes.GetKind() == "HTTPRouteList" {
				return nil
			}
			return c.List(ctx, list, opts...)
		},
	})
	if _, err := r.ReconcileProvider(context.Background(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(provider)}); err != nil {
		t.Fatal(err)
	}
	r.Client = cached
	if _, err := getUnstructured(r, schema.GroupVersionKind{Version: "v1", Kind: "Service"}, "tenant-a", "provider-openai"); err != nil {
		t.Fatalf("provider Service removed while the attachment route uses it: %v", err)
	}

	// Only attachments create transport in a non-Praxis tenant namespace, so
	// removing the last one collects it.
	got := getModel(t, r, model)
	got.Spec.GatewayRefs = []v1alpha1.NamespacedObjectReference{gatewayA}
	if err := r.Update(context.Background(), got); err != nil {
		t.Fatal(err)
	}
	reconcileModel(t, r, model)
	assertTransportCollected(t, r, "tenant-a", "chat")
}

func TestAttachmentRouteIsAppliedBeforeItsTransport(t *testing.T) {
	provider, secret := attachmentProvider("team")
	model := attachmentModel("chat", "team", "chat-model", time.Unix(100, 0), gatewayB)
	r := attachmentReconciler(t, append([]client.Object{provider, secret, model}, attachmentTenantObjects("tenant-b", "tenant-b", gatewayB, "praxis")...)...)
	var applied []string
	r.ApplyResource = func(ctx context.Context, c client.Client, obj unstructured.Unstructured) error {
		applied = append(applied, obj.GetKind()+"/"+obj.GetName())
		return render.Apply(ctx, c, []unstructured.Unstructured{obj})
	}
	reconcileModel(t, r, model)
	route := slices.Index(applied, "HTTPRoute/"+attachmentRouteName(model, gatewayB))
	for _, backend := range []string{"Service/provider-openai", "ServiceEntry/provider-openai", "Service/" + providerSelectionSinkName("chat")} {
		if i := slices.Index(applied, backend); route < 0 || i < route {
			t.Fatalf("apply order = %v, want the attachment route before %s", applied, backend)
		}
	}
}

// failingTenantReader fails live reads of one tenant's MaasTenantConfig, as
// an unavailable API server would.
type failingTenantReader struct {
	client.Reader
	namespace string
}

func (f failingTenantReader) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if u, ok := obj.(*unstructured.Unstructured); ok && u.GetKind() == tenant.MaasTenantConfigGVK.Kind && key.Namespace == f.namespace {
		return apierrors.NewServiceUnavailable("etcd is unavailable")
	}
	return f.Reader.Get(ctx, key, obj, opts...)
}

func TestHomeAttachmentReportsOnlyItsOwnFailure(t *testing.T) {
	gatewayA := v1alpha1.NamespacedObjectReference{Name: "gw-a", Namespace: "gateway-a"}
	provider, secret := attachmentProvider("tenant-a")
	model := attachmentModel("chat", "tenant-a", "chat-model", time.Unix(100, 0), gatewayA, gatewayB)
	objects := []client.Object{provider, secret, model}
	objects = append(objects, attachmentTenantObjects("tenant-a", "tenant-a", gatewayA, "ipp")...)
	objects = append(objects, attachmentTenantObjects("tenant-b", "tenant-b", gatewayB, "praxis")...)
	r := attachmentReconciler(t, objects...)
	reader := r.APIReader
	request := reconcile.Request{NamespacedName: client.ObjectKeyFromObject(model)}

	// Another attachment's failure does not relabel the home attachment.
	r.APIReader = failingTenantReader{Reader: reader, namespace: "tenant-b"}
	if _, err := r.Reconcile(context.Background(), request); err == nil {
		t.Fatal("a failed tenant read must be retried")
	}
	if got := getModel(t, r, model); readyReason(got) != reasonTenantNotPraxis {
		t.Fatalf("reason = %q, want the home attachment's own %q", readyReason(got), reasonTenantNotPraxis)
	}

	// The home path's own failure is reported as a failure, not a tenant state.
	r.APIReader = failingTenantReader{Reader: reader, namespace: "tenant-a"}
	if _, err := r.Reconcile(context.Background(), request); err == nil {
		t.Fatal("a failed tenant read must be retried")
	}
	got := getModel(t, r, model)
	if readyReason(got) != reasonReconcileFailed || got.Status.Phase != modelPhaseFailed {
		t.Fatalf("status = %q/%q, want %q", got.Status.Phase, readyReason(got), reasonReconcileFailed)
	}
}

func TestHomeAttachmentReportsItsTenantNotReady(t *testing.T) {
	gatewayA := v1alpha1.NamespacedObjectReference{Name: "gw-a", Namespace: "gateway-a"}
	provider, secret := attachmentProvider("tenant-a")
	model := attachmentModel("chat", "tenant-a", "chat-model", time.Unix(100, 0), gatewayA, gatewayB)
	tenantA := attachmentTenantObjects("tenant-a", "tenant-a", gatewayA, "praxis")
	// The AITenant's Ready stamp is for an older generation: not DeployReady.
	tenantA[0].SetGeneration(2)
	objects := append([]client.Object{provider, secret, model}, tenantA...)
	objects = append(objects, attachmentTenantObjects("tenant-b", "tenant-b", gatewayB, "praxis")...)
	r := attachmentReconciler(t, objects...)
	reconcileModel(t, r, model)
	if ready := apiMeta.FindStatusCondition(gatewayResult(t, getModel(t, r, model), gatewayA).Conditions, conditionReady); ready == nil || ready.Reason != reasonTenantNotReady {
		t.Fatalf("home result = %#v, want %q", ready, reasonTenantNotReady)
	}
}

func TestRebindingAwayFromOwnTenantRemovesHomeRoute(t *testing.T) {
	gatewayA := v1alpha1.NamespacedObjectReference{Name: "gw-a", Namespace: "gateway-a"}
	provider, secret := attachmentProvider("tenant-a")
	model := attachmentModel("chat", "tenant-a", "chat-model", time.Unix(100, 0))
	sibling := attachmentModel("other", "tenant-a", "other-model", time.Unix(100, 0))
	objects := []client.Object{provider, secret, model, sibling}
	objects = append(objects, attachmentTenantObjects("tenant-a", "tenant-a", gatewayA, "praxis")...)
	objects = append(objects, attachmentTenantObjects("tenant-b", "tenant-b", gatewayB, "praxis")...)
	r := attachmentReconciler(t, objects...)
	reconcileModel(t, r, model)
	reconcileModel(t, r, sibling)
	if _, err := getUnstructured(r, httpRouteGVK, "tenant-a", "external-model-chat"); err != nil {
		t.Fatalf("model is not served through its own tenant first: %v", err)
	}

	got := getModel(t, r, model)
	got.Spec.GatewayRefs = []v1alpha1.NamespacedObjectReference{gatewayB}
	if err := r.Update(context.Background(), got); err != nil {
		t.Fatal(err)
	}
	reconcileModel(t, r, model)

	if _, err := getUnstructured(r, httpRouteGVK, "tenant-a", "external-model-chat"); !apierrors.IsNotFound(err) {
		t.Fatalf("the old home route survived rebinding to another Gateway: %v", err)
	}
	if _, err := getUnstructured(r, httpRouteGVK, "tenant-a", "external-model-other"); err != nil {
		t.Fatalf("a sibling's route was removed: %v", err)
	}
	if candidates := overlayEnvelope(t, r, "tenant-a").Overlay.Candidates; len(candidates) != 1 || candidates[0].Name != "other-model" {
		t.Fatalf("own tenant overlay = %#v, want only the sibling", candidates)
	}
	if got := getModel(t, r, model); readyReason(got) != reasonAttachmentPending {
		t.Fatalf("reason = %q, want the new attachment pending", readyReason(got))
	}

	// The last model leaving removes the tenant's own serving state.
	got = getModel(t, r, sibling)
	got.Spec.GatewayRefs = []v1alpha1.NamespacedObjectReference{gatewayB}
	if err := r.Update(context.Background(), got); err != nil {
		t.Fatal(err)
	}
	reconcileModel(t, r, sibling)
	if _, err := getUnstructured(r, httpRouteGVK, "tenant-a", "external-model-other"); !apierrors.IsNotFound(err) {
		t.Fatalf("the last model's home route survived: %v", err)
	}
	var overlay corev1.ConfigMap
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: "tenant-a", Name: "routing-overlay"}, &overlay); !apierrors.IsNotFound(err) {
		t.Fatalf("own tenant overlay survived its last model leaving: %v", err)
	}
}

func TestRebindingAwayFromDeselectedTenantCleansHomeRoute(t *testing.T) {
	gatewayA := v1alpha1.NamespacedObjectReference{Name: "gw-a", Namespace: "gateway-a"}
	provider, secret := attachmentProvider("tenant-a")
	model := attachmentModel("chat", "tenant-a", "chat-model", time.Unix(100, 0))
	objects := []client.Object{provider, secret, model}
	objects = append(objects, attachmentTenantObjects("tenant-a", "tenant-a", gatewayA, "praxis")...)
	objects = append(objects, attachmentTenantObjects("tenant-b", "tenant-b", gatewayB, "praxis")...)
	r := attachmentReconciler(t, objects...)
	reconcileModel(t, r, model)
	if _, err := getUnstructured(r, httpRouteGVK, "tenant-a", "external-model-chat"); err != nil {
		t.Fatalf("model is not served through its own tenant first: %v", err)
	}

	// The tenant switches to IPP while the model is rebound elsewhere.
	mtc := tenant.NewMaasTenantConfig()
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: "tenant-a", Name: tenant.MaasTenantConfigInstanceName}, mtc); err != nil {
		t.Fatal(err)
	}
	annotations := mtc.GetAnnotations()
	annotations[tenant.AnnotationPayloadProcessingType] = tenant.PayloadProcessingBackendIPP
	mtc.SetAnnotations(annotations)
	if err := r.Update(context.Background(), mtc); err != nil {
		t.Fatal(err)
	}
	got := getModel(t, r, model)
	got.Spec.GatewayRefs = []v1alpha1.NamespacedObjectReference{gatewayB}
	if err := r.Update(context.Background(), got); err != nil {
		t.Fatal(err)
	}
	reconcileModel(t, r, model)

	if _, err := getUnstructured(r, httpRouteGVK, "tenant-a", "external-model-chat"); !apierrors.IsNotFound(err) {
		t.Fatalf("the home route survived an explicit non-Praxis selection: %v", err)
	}
	var overlay corev1.ConfigMap
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: "tenant-a", Name: "routing-overlay"}, &overlay); !apierrors.IsNotFound(err) {
		t.Fatalf("the deselected tenant's overlay survived: %v", err)
	}
}

func TestAttachmentRouteFallbackNeverMatchesAHomeRoute(t *testing.T) {
	model := attachmentModel(strings.Repeat("m", 240), "team", "chat-model", time.Unix(100, 0), gatewayB)
	// ExternalModel names start with an alphanumeric, so no home route
	// (modelRoutePrefix + name) starts with modelRoutePrefix + "-".
	if name := attachmentRouteName(model, gatewayB); len(name) > 253 || !strings.HasPrefix(name, modelRoutePrefix+"-") {
		t.Fatalf("fallback route name = %q, want %q followed by the hash", name, modelRoutePrefix+"-")
	}
}

func TestRemovedGatewayRefRemovesOnlyItsAttachmentRoute(t *testing.T) {
	provider, secret := attachmentProvider("team")
	model := attachmentModel("chat", "team", "chat-model", time.Unix(100, 0), gatewayB, gatewayC)
	other := attachmentModel("other", "team", "other-model", time.Unix(100, 0), gatewayC)
	objects := []client.Object{provider, secret, model, other}
	objects = append(objects, attachmentTenantObjects("tenant-b", "tenant-b", gatewayB, "praxis")...)
	objects = append(objects, attachmentTenantObjects("tenant-c", "tenant-c", gatewayC, "praxis")...)
	r := attachmentReconciler(t, objects...)
	reconcileModel(t, r, model)
	reconcileModel(t, r, other)
	setRefs := func(refs ...v1alpha1.NamespacedObjectReference) {
		t.Helper()
		got := getModel(t, r, model)
		got.Spec.GatewayRefs = refs
		if err := r.Update(context.Background(), got); err != nil {
			t.Fatal(err)
		}
		reconcileModel(t, r, model)
	}
	routeExists := func(name string) bool {
		t.Helper()
		_, err := getUnstructured(r, httpRouteGVK, "team", name)
		if err != nil && !apierrors.IsNotFound(err) {
			t.Fatal(err)
		}
		return err == nil
	}
	routeB, routeC, otherC := attachmentRouteName(model, gatewayB), attachmentRouteName(model, gatewayC), attachmentRouteName(other, gatewayC)

	setRefs(gatewayB)
	if routeExists(routeC) || !routeExists(routeB) || !routeExists(otherC) {
		t.Fatalf("after removing gw-c: routes b=%v c=%v other=%v, want only the removed attachment gone",
			routeExists(routeB), routeExists(routeC), routeExists(otherC))
	}
	setRefs()
	if routeExists(routeB) || !routeExists(otherC) {
		t.Fatalf("after removing every gatewayRef: routes b=%v other=%v, want the model's last attachment gone",
			routeExists(routeB), routeExists(otherC))
	}
}

func TestAttachmentPreservesTenantOwnOverlayCandidates(t *testing.T) {
	tenantProvider, tenantSecret := attachmentProvider("tenant-b")
	ownModel := attachmentModel("own", "tenant-b", "own-model", time.Unix(300, 0))
	provider, secret := attachmentProvider("team")
	model := attachmentModel("chat", "team", "chat-model", time.Unix(100, 0), gatewayB)
	objects := append([]client.Object{tenantProvider, tenantSecret, ownModel, provider, secret, model}, attachmentTenantObjects("tenant-b", "tenant-b", gatewayB, "praxis")...)
	r := attachmentReconciler(t, objects...)
	reconcileModel(t, r, ownModel)
	before := overlayEnvelope(t, r, "tenant-b").Overlay.Candidates

	reconcileModel(t, r, model)
	setRouteParentStatus(t, r, model, gatewayB, metav1.ConditionTrue, "Accepted")
	reconcileModel(t, r, model)
	after := overlayEnvelope(t, r, "tenant-b").Overlay.Candidates
	if len(after) != 2 || mustJSON(t, after[0]) != mustJSON(t, before[0]) {
		t.Fatalf("own candidate changed: before %#v after %#v", before, after)
	}
	names := routeFilterNames(t, r, "tenant-b", "gateway-b")
	if len(names) != 8 || names[0] != "tenant-b.external-model-own.0" || names[4] != "team."+attachmentRouteName(model, gatewayB)+".0" {
		t.Fatalf("route filter patches = %v, want own patches then the attachment", names)
	}

	// The tenant's own reconcile keeps the admitted attachment.
	reconcileModel(t, r, ownModel)
	if again := overlayEnvelope(t, r, "tenant-b").Overlay.Candidates; len(again) != 2 || again[1].StableID != "team/chat/team/openai" {
		t.Fatalf("own reconcile dropped the attachment: %#v", again)
	}
}

func TestSameNamedProvidersFromNamespacesStayDistinct(t *testing.T) {
	providerOne, secretOne := attachmentProvider("team-one")
	providerTwo, secretTwo := attachmentProvider("team-two")
	modelOne := attachmentModel("chat", "team-one", "chat-one", time.Unix(100, 0), gatewayB)
	modelTwo := attachmentModel("chat", "team-two", "chat-two", time.Unix(200, 0), gatewayB)
	objects := append([]client.Object{providerOne, secretOne, providerTwo, secretTwo, modelOne, modelTwo}, attachmentTenantObjects("tenant-b", "tenant-b", gatewayB, "praxis")...)
	r := attachmentReconciler(t, objects...)
	for _, model := range []*v1alpha1.ExternalModel{modelOne, modelTwo} {
		reconcileModel(t, r, model)
		setRouteParentStatus(t, r, model, gatewayB, metav1.ConditionTrue, "Accepted")
	}
	for _, model := range []*v1alpha1.ExternalModel{modelOne, modelTwo} {
		reconcileModel(t, r, model)
	}
	ruleOne, ruleTwo := attachmentDestinationRuleName(gatewayB, "team-one", "openai"), attachmentDestinationRuleName(gatewayB, "team-two", "openai")
	if ruleOne == ruleTwo {
		t.Fatalf("DestinationRules collide: %s", ruleOne)
	}
	for _, name := range []string{ruleOne, ruleTwo} {
		if _, err := getUnstructured(r, destinationRuleGVK, "gateway-b", name); err != nil {
			t.Fatalf("DestinationRule %s: %v", name, err)
		}
	}
	env := overlayEnvelope(t, r, "tenant-b")
	if len(env.Overlay.Candidates) != 2 || env.Overlay.Candidates[0].StableID != "team-one/chat/team-one/openai" ||
		env.Overlay.Candidates[1].StableID != "team-two/chat/team-two/openai" {
		t.Fatalf("overlay candidates = %#v", env.Overlay.Candidates)
	}
	if env.Overlay.Candidates[0].Credential.SecretRef.Name == env.Overlay.Candidates[1].Credential.SecretRef.Name {
		t.Fatal("credential copies of different sources collide")
	}
}

func servedAttachmentFixture(t *testing.T) (*Reconciler, *v1alpha1.ExternalModel) {
	t.Helper()
	provider, secret := attachmentProvider("team")
	model := attachmentModel("chat", "team", "chat-model", time.Unix(100, 0), gatewayB)
	objects := append([]client.Object{provider, secret, model}, attachmentTenantObjects("tenant-b", "tenant-b", gatewayB, "praxis")...)
	r := attachmentReconciler(t, objects...)
	reconcileModel(t, r, model)
	setRouteParentStatus(t, r, model, gatewayB, metav1.ConditionTrue, "Accepted")
	reconcileModel(t, r, model)
	if len(overlayEnvelope(t, r, "tenant-b").Overlay.Candidates) != 1 {
		t.Fatal("fixture attachment is not served")
	}
	return r, model
}

func assertAttachmentWithdrawn(t *testing.T, r *Reconciler) {
	t.Helper()
	if _, err := getUnstructured(r, destinationRuleGVK, "gateway-b", attachmentDestinationRuleName(gatewayB, "team", "openai")); !apierrors.IsNotFound(err) {
		t.Fatalf("attachment DestinationRule remains: %v", err)
	}
	var copied corev1.Secret
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: "tenant-b", Name: credentialCopyName("tenant-b", "team", "openai-key", "api-key", "")}, &copied); !apierrors.IsNotFound(err) {
		t.Fatalf("credential copy remains: %v", err)
	}
}

func assertNoTenantOverlay(t *testing.T, r *Reconciler) {
	t.Helper()
	var overlay corev1.ConfigMap
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: "tenant-b", Name: "routing-overlay"}, &overlay); !apierrors.IsNotFound(err) {
		t.Fatalf("overlay entry remains: %v", err)
	}
}

func TestRevokedAcceptanceWithdrawsAttachment(t *testing.T) {
	r, model := servedAttachmentFixture(t)
	setRouteParentStatus(t, r, model, gatewayB, metav1.ConditionFalse, "NotAllowedByListeners")
	reconcileModel(t, r, model)
	assertAttachmentWithdrawn(t, r)
	assertNoTenantOverlay(t, r)
	if got := getModel(t, r, model); readyReason(got) != "NotAllowedByListeners" || got.Status.HTTPRouteName != "" {
		t.Fatalf("reason = %q, httpRouteName = %q; want the Gateway's reason and no route", readyReason(got), got.Status.HTTPRouteName)
	}
}

func TestRemovedGatewayRefCollectsAttachmentState(t *testing.T) {
	r, model := servedAttachmentFixture(t)
	got := getModel(t, r, model)
	got.Spec.GatewayRefs = nil
	if err := r.Update(context.Background(), got); err != nil {
		t.Fatal(err)
	}
	reconcileModel(t, r, model)
	if attachmentRouteExists(t, r, model) {
		t.Fatal("detached model keeps its attachment route")
	}
	assertAttachmentWithdrawn(t, r)
	if got := getModel(t, r, model); got.Status.HTTPRouteName != "" {
		t.Fatalf("httpRouteName = %q names the deleted attachment route", got.Status.HTTPRouteName)
	}
}

func TestFailedDetachCleanupIsRetried(t *testing.T) {
	r, model := servedAttachmentFixture(t)
	failed := false
	fakeClient, ok := r.Client.(client.WithWatch)
	if !ok {
		t.Fatal("the test client must support watches")
	}
	r.Client = interceptor.NewClient(fakeClient, interceptor.Funcs{
		Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			if obj.GetObjectKind().GroupVersionKind().Kind == "DestinationRule" && !failed {
				failed = true
				return apierrors.NewServiceUnavailable("etcd is unavailable")
			}
			return c.Delete(ctx, obj, opts...)
		},
	})
	got := getModel(t, r, model)
	got.Spec.GatewayRefs = nil
	if err := r.Update(context.Background(), got); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(model)}); err == nil {
		t.Fatal("a failed cleanup must be retried")
	}
	// The route is withdrawn before its backends.
	if attachmentRouteExists(t, r, model) {
		t.Fatal("the attachment route outlives a failed backend cleanup")
	}
	// The attachment finalizer stays until the cleanup succeeds, so the retry
	// collects what the deleted route used.
	reconcileModel(t, r, model)
	assertAttachmentWithdrawn(t, r)
}

func TestDeletingModelCollectsStateOfItsAlreadyDeletedRoute(t *testing.T) {
	r, model := servedAttachmentFixture(t)
	// Foreground deletion removes the model-owned route and selection sink
	// before the model's finalizer runs; the provider transport is not
	// model-owned.
	route, err := getUnstructured(r, httpRouteGVK, "team", attachmentRouteName(model, gatewayB))
	if err != nil {
		t.Fatal(err)
	}
	sink, err := getUnstructured(r, schema.GroupVersionKind{Version: "v1", Kind: "Service"}, "team", providerSelectionSinkName("chat"))
	if err != nil {
		t.Fatal(err)
	}
	for _, owned := range []client.Object{route, sink} {
		if err := r.Delete(context.Background(), owned); err != nil {
			t.Fatal(err)
		}
	}
	if err := r.Delete(context.Background(), getModel(t, r, model)); err != nil {
		t.Fatal(err)
	}
	reconcileModel(t, r, model)
	assertAttachmentWithdrawn(t, r)
	assertTransportCollected(t, r, "team", "chat")
}

func TestModelDeletedWithoutAttachmentFinalizerCollectsItsState(t *testing.T) {
	r, model := servedAttachmentFixture(t)
	// A model created before the attachment finalizer existed is deleted at
	// once; Kubernetes then garbage collects its route.
	got := getModel(t, r, model)
	got.Finalizers = nil
	if err := r.Update(context.Background(), got); err != nil {
		t.Fatal(err)
	}
	if err := r.Delete(context.Background(), got); err != nil {
		t.Fatal(err)
	}
	route, err := getUnstructured(r, httpRouteGVK, "team", attachmentRouteName(model, gatewayB))
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Delete(context.Background(), route); err != nil {
		t.Fatal(err)
	}
	// The route deletion requeues the deleted model.
	reconcileModel(t, r, model)
	assertAttachmentWithdrawn(t, r)
}

func TestDeletedCredentialSourceWithdrawsAttachment(t *testing.T) {
	r, model := servedAttachmentFixture(t)
	_, secret := attachmentProvider("team")
	if err := r.Delete(context.Background(), secret); err != nil {
		t.Fatal(err)
	}
	reconcileModel(t, r, model)
	if got := getModel(t, r, model); readyReason(got) != reasonProviderNotReady {
		t.Fatalf("reason = %q, want %q", readyReason(got), reasonProviderNotReady)
	}
	// The copy must not keep serving a credential its source no longer holds.
	assertAttachmentWithdrawn(t, r)
	assertNoTenantOverlay(t, r)
	if attachmentRouteExists(t, r, model) {
		t.Fatal("excluded model keeps its route")
	}

	_, secret = attachmentProvider("team")
	if err := r.Create(context.Background(), secret); err != nil {
		t.Fatal(err)
	}
	reconcileModel(t, r, model)
	setRouteParentStatus(t, r, model, gatewayB, metav1.ConditionTrue, "Accepted")
	reconcileModel(t, r, model)
	if len(overlayEnvelope(t, r, "tenant-b").Overlay.Candidates) != 1 {
		t.Fatal("the attachment is not served again after its source Secret was restored")
	}
}

// failingSecretReader fails Secret reads as an unavailable API server would.
type failingSecretReader struct{ client.Reader }

func (f failingSecretReader) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if _, ok := obj.(*corev1.Secret); ok {
		return apierrors.NewServiceUnavailable("etcd is unavailable")
	}
	return f.Reader.Get(ctx, key, obj, opts...)
}

func TestTransientCredentialReadKeepsServedAttachment(t *testing.T) {
	r, model := servedAttachmentFixture(t)
	r.APIReader = failingSecretReader{Reader: r.APIReader}
	if _, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(model)}); err == nil {
		t.Fatal("a failed Secret read must be retried")
	}
	if len(overlayEnvelope(t, r, "tenant-b").Overlay.Candidates) != 1 {
		t.Fatal("a failed Secret read withdrew the served attachment")
	}
	var copied corev1.Secret
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: "tenant-b", Name: credentialCopyName("tenant-b", "team", "openai-key", "api-key", "")}, &copied); err != nil {
		t.Fatalf("a failed Secret read deleted the credential copy: %v", err)
	}
	if got := getModel(t, r, model); readyReason(got) != reasonReconcileFailed {
		t.Fatalf("reason = %q, want %q", readyReason(got), reasonReconcileFailed)
	}
}

func TestServedAzureAttachmentStampsCredentialHeaderAnnotation(t *testing.T) {
	// Azure apikey auth qualifies to strategy apikey + api-key header. The
	// header is json:"-" on Credential and cannot survive the overlay
	// round-trip, so attach time stamps it on the credential copy Secret for
	// the tenant reconciler to restore. See envelope.StrategyFor /
	// tenant.AnnotationCredentialHeader.
	provider := &v1alpha1.ExternalProvider{
		ObjectMeta: metav1.ObjectMeta{Name: "azure", Namespace: "team", UID: types.UID("provider-azure-team")},
		Spec: v1alpha1.ExternalProviderSpec{
			Provider: "azure", Endpoint: "azure.example.com",
			Auth: v1alpha1.AuthConfig{Type: "apikey", SecretRef: v1alpha1.NameReference{Name: "azure-key"}},
		},
	}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "azure-key", Namespace: "team"}, Data: map[string][]byte{"api-key": []byte("azure-secret")}}
	model := &v1alpha1.ExternalModel{
		ObjectMeta: metav1.ObjectMeta{Name: "chat", Namespace: "team", UID: types.UID("uid-team-azure-chat"), CreationTimestamp: metav1.NewTime(time.Unix(100, 0))},
		Spec: v1alpha1.ExternalModelSpec{
			ModelName: "chat-model",
			ExternalProviderRefs: []v1alpha1.ExternalProviderRef{{
				Ref: v1alpha1.ExternalProviderReference{Name: "azure"}, TargetModel: "gpt-4o", APIFormat: "openai-chat", Path: "/v1/chat/completions",
			}},
			GatewayRefs: []v1alpha1.NamespacedObjectReference{gatewayB},
		},
	}
	objects := append([]client.Object{provider, secret, model}, attachmentTenantObjects("tenant-b", "tenant-b", gatewayB, "praxis")...)
	r := attachmentReconciler(t, objects...)
	reconcileModel(t, r, model)
	setRouteParentStatus(t, r, model, gatewayB, metav1.ConditionTrue, "Accepted")
	reconcileModel(t, r, model)
	if len(overlayEnvelope(t, r, "tenant-b").Overlay.Candidates) != 1 {
		t.Fatal("azure attachment is not served")
	}

	var copied corev1.Secret
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: "tenant-b", Name: credentialCopyName("tenant-b", "team", "azure-key", "api-key", "api-key")}, &copied); err != nil {
		t.Fatalf("credential copy: %v", err)
	}
	if got := copied.GetAnnotations()[tenant.AnnotationCredentialHeader]; got != "api-key" {
		t.Fatalf("credential-header annotation = %q, want %q", got, "api-key")
	}
}

func TestSharedSecretWithDivergentHeadersGetsDistinctCopies(t *testing.T) {
	// The injected header is provider-scoped and part of the credential copy
	// identity (credentialCopyName), so a source Secret qualified under two
	// different headers yields two distinct copies, each with its own header
	// annotation, rather than one copy whose single annotation a later apply
	// could overwrite. credentialCopyName is a pure function of (tenant, source
	// Secret, header), so the guarantee holds across separate attached models
	// that share the Secret too. Here one model binds azure (api-key) and openai
	// (bearer_token, no header) to the same Secret; each gets its own copy.
	azure := &v1alpha1.ExternalProvider{
		ObjectMeta: metav1.ObjectMeta{Name: "azure", Namespace: "team", UID: types.UID("provider-azure-team")},
		Spec: v1alpha1.ExternalProviderSpec{Provider: "azure", Endpoint: "azure.example.com",
			Auth: v1alpha1.AuthConfig{Type: "apikey", SecretRef: v1alpha1.NameReference{Name: "shared-key"}}},
	}
	openai := &v1alpha1.ExternalProvider{
		ObjectMeta: metav1.ObjectMeta{Name: "openai", Namespace: "team", UID: types.UID("provider-openai-team")},
		Spec: v1alpha1.ExternalProviderSpec{Provider: "openai", Endpoint: "openai.example.com",
			Auth: v1alpha1.AuthConfig{Type: "apikey", SecretRef: v1alpha1.NameReference{Name: "shared-key"}}},
	}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "shared-key", Namespace: "team"}, Data: map[string][]byte{"api-key": []byte("k")}}
	model := &v1alpha1.ExternalModel{
		ObjectMeta: metav1.ObjectMeta{Name: "chat", Namespace: "team", UID: types.UID("uid-team-shared-chat"), CreationTimestamp: metav1.NewTime(time.Unix(100, 0))},
		Spec: v1alpha1.ExternalModelSpec{
			ModelName: "chat-model",
			ExternalProviderRefs: []v1alpha1.ExternalProviderRef{
				{Ref: v1alpha1.ExternalProviderReference{Name: "azure"}, TargetModel: "gpt-4o", APIFormat: "openai-chat", Path: "/v1/chat/completions"},
				{Ref: v1alpha1.ExternalProviderReference{Name: "openai"}, TargetModel: "gpt-4o", APIFormat: "openai-chat", Path: "/v1/chat/completions"},
			},
			GatewayRefs: []v1alpha1.NamespacedObjectReference{gatewayB},
		},
	}
	objects := append([]client.Object{azure, openai, secret, model}, attachmentTenantObjects("tenant-b", "tenant-b", gatewayB, "praxis")...)
	r := attachmentReconciler(t, objects...)
	reconcileModel(t, r, model)
	setRouteParentStatus(t, r, model, gatewayB, metav1.ConditionTrue, "Accepted")
	reconcileModel(t, r, model)

	if got := getModel(t, r, model); readyReason(got) == reasonReconcileFailed {
		t.Fatalf("model sharing a Secret under distinct headers must be served, got %+v", got.Status.Conditions)
	}

	// azure qualifies to the api-key header; openai to bearer_token with none.
	// The header is part of the identity, so the two copies have distinct names.
	azureCopyName := credentialCopyName("tenant-b", "team", "shared-key", "api-key", "api-key")
	openaiCopyName := credentialCopyName("tenant-b", "team", "shared-key", "api-key", "")
	if azureCopyName == openaiCopyName {
		t.Fatal("divergent headers must produce distinct copy names")
	}
	var azureCopy corev1.Secret
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: "tenant-b", Name: azureCopyName}, &azureCopy); err != nil {
		t.Fatalf("azure credential copy: %v", err)
	}
	if got := azureCopy.GetAnnotations()[tenant.AnnotationCredentialHeader]; got != "api-key" {
		t.Fatalf("azure copy header annotation = %q, want api-key", got)
	}
	var openaiCopy corev1.Secret
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: "tenant-b", Name: openaiCopyName}, &openaiCopy); err != nil {
		t.Fatalf("openai credential copy: %v", err)
	}
	if got, ok := openaiCopy.GetAnnotations()[tenant.AnnotationCredentialHeader]; ok {
		t.Fatalf("bearer_token copy must carry no header annotation, got %q", got)
	}
}

func TestCrossNamespaceAzureOverrideSecretGetsHeaderedCopy(t *testing.T) {
	// A tenant's own model references a cross-namespace Azure provider but brings
	// its own credential with a model-local override Secret. The override Secret
	// lives in the tenant namespace, so it would otherwise be mounted directly —
	// but azure+openai-chat qualifies to the api-key header, which survives the
	// overlay round-trip only on a copy Secret's annotation (Credential.Header is
	// not serialized). localizeRoutes must still copy such a tenant-local Secret
	// so attachedOverlayCandidates can restore the header; skipping the copy would
	// drop the api-key header and break Azure auth.
	azure := &v1alpha1.ExternalProvider{
		ObjectMeta: metav1.ObjectMeta{Name: "azure", Namespace: "shared", UID: types.UID("provider-azure-shared")},
		Spec: v1alpha1.ExternalProviderSpec{Provider: "azure", Endpoint: "azure.example.com",
			Auth: v1alpha1.AuthConfig{Type: "apikey", SecretRef: v1alpha1.NameReference{Name: "provider-key"}}},
	}
	model := referenceProvider(attachmentModel("chat", "tenant-b", "chat-model", time.Unix(100, 0)), "shared")
	model.Spec.ExternalProviderRefs[0].Ref.Name = "azure"
	// A model-local override: the resolver leaves SecretNamespace empty, so the
	// Secret is read from the model (tenant) namespace.
	model.Spec.ExternalProviderRefs[0].Auth = &v1alpha1.AuthConfig{Type: "apikey", SecretRef: v1alpha1.NameReference{Name: "model-key"}}
	modelSecret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "model-key", Namespace: "tenant-b"}, Data: map[string][]byte{"api-key": []byte("sk-model-local")}}
	objects := append([]client.Object{azure, modelSecret, model, referenceGrant("shared", "tenant-b", "azure")},
		attachmentTenantObjects("tenant-b", "tenant-b", gatewayB, "praxis")...)
	r := attachmentReconciler(t, objects...)
	reconcileModel(t, r, model)

	if got := getModel(t, r, model); got.Status.Phase != resolver.PhaseReady {
		t.Fatalf("status = %q/%q %q", got.Status.Phase, readyReason(got), readyMessage(got))
	}

	// Source namespace is the tenant namespace (model-local override), yet the
	// headered credential is still copied, carrying its header annotation.
	copyName := credentialCopyName("tenant-b", "tenant-b", "model-key", "api-key", "api-key")
	var copied corev1.Secret
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: "tenant-b", Name: copyName}, &copied); err != nil {
		t.Fatalf("model-local override copy for a cross-namespace Azure provider: %v", err)
	}
	if got := copied.GetAnnotations()[tenant.AnnotationCredentialHeader]; got != "api-key" {
		t.Fatalf("copy header annotation = %q, want api-key", got)
	}
	if string(copied.Data["api-key"]) != "sk-model-local" {
		t.Fatalf("copy value = %q, want the model-local override", copied.Data["api-key"])
	}
	candidates := overlayEnvelope(t, r, "tenant-b").Overlay.Candidates
	if len(candidates) != 1 || candidates[0].StableID != "tenant-b/chat/shared/azure" ||
		candidates[0].Credential == nil || candidates[0].Credential.SecretRef.Name != copyName {
		t.Fatalf("overlay candidates = %#v, want the headered copy", candidates)
	}
}

func TestServedBearerAttachmentOmitsCredentialHeaderAnnotation(t *testing.T) {
	// openai + openai-chat qualifies to bearer_token, whose header is fixed
	// data-plane side; no header annotation is stamped on the copy Secret.
	r, _ := servedAttachmentFixture(t)
	var copied corev1.Secret
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: "tenant-b", Name: credentialCopyName("tenant-b", "team", "openai-key", "api-key", "")}, &copied); err != nil {
		t.Fatalf("credential copy: %v", err)
	}
	if got, ok := copied.GetAnnotations()[tenant.AnnotationCredentialHeader]; ok {
		t.Fatalf("bearer_token copy must not carry a credential-header annotation, got %q", got)
	}
}

// publishOwnCandidates publishes count tenant-local candidates in the tenant
// overlay, as the tenant-local path does.
func publishOwnCandidates(t *testing.T, r *Reconciler, namespace string, gateway v1alpha1.NamespacedObjectReference, count int) {
	t.Helper()
	set := &resolver.ResolvedRouteSet{}
	for i := range count {
		model := fmt.Sprintf("own-%d", i)
		set.Models = append(set.Models, resolver.ModelRoutes{ModelRef: namespace + "/" + model, Routes: []resolver.Route{{
			Model: model, ClientName: model, Namespace: namespace, Provider: "local", Cluster: "provider-local", Weight: 1,
		}}})
	}
	if _, err := r.publishTenantOverlay(context.Background(), namespace, gateway.Name, set, nil, nil, "tenant-source", false); err != nil {
		t.Fatal(err)
	}
}

func TestAttachmentOverOverlayCapacityReceivesNoTenantState(t *testing.T) {
	provider, secret := attachmentProvider("team")
	model := attachmentModel("chat", "team", "chat-model", time.Unix(100, 0), gatewayB)
	objects := append([]client.Object{provider, secret, model}, attachmentTenantObjects("tenant-b", "tenant-b", gatewayB, "praxis")...)
	r := attachmentReconciler(t, objects...)
	publishOwnCandidates(t, r, "tenant-b", gatewayB, maxOverlayCandidates)
	reconcileModel(t, r, model)
	setRouteParentStatus(t, r, model, gatewayB, metav1.ConditionTrue, "Accepted")
	reconcileModel(t, r, model)

	if got := getModel(t, r, model); readyReason(got) != reasonOverlayCapacityExceeded {
		t.Fatalf("reason = %q, want %q", readyReason(got), reasonOverlayCapacityExceeded)
	}
	assertAttachmentWithdrawn(t, r)
	for _, candidate := range overlayEnvelope(t, r, "tenant-b").Overlay.Candidates {
		if envelope.IsScopedStableID(candidate.StableID) {
			t.Fatalf("over-capacity model entered the overlay: %#v", candidate)
		}
	}
	// It keeps its accepted route, and so its place in admission order.
	if !attachmentRouteExists(t, r, model) {
		t.Fatal("over-capacity model lost its accepted route")
	}
}

func TestAttachmentPushedOverCapacityIsWithdrawn(t *testing.T) {
	r, model := servedAttachmentFixture(t)
	publishOwnCandidates(t, r, "tenant-b", gatewayB, maxOverlayCandidates)
	reconcileModel(t, r, model)
	if got := getModel(t, r, model); readyReason(got) != reasonOverlayCapacityExceeded {
		t.Fatalf("reason = %q, want %q", readyReason(got), reasonOverlayCapacityExceeded)
	}
	assertAttachmentWithdrawn(t, r)
}

func TestProviderReconcileKeepsAttachmentDestinationRules(t *testing.T) {
	gatewayA := v1alpha1.NamespacedObjectReference{Name: "gw-a", Namespace: "gateway-a"}
	provider, secret := attachmentProvider("tenant-a")
	model := attachmentModel("chat", "tenant-a", "chat-model", time.Unix(100, 0), gatewayA, gatewayB)
	objects := []client.Object{provider, secret, model}
	objects = append(objects, attachmentTenantObjects("tenant-a", "tenant-a", gatewayA, "praxis")...)
	objects = append(objects, attachmentTenantObjects("tenant-b", "tenant-b", gatewayB, "praxis")...)
	r := attachmentReconciler(t, objects...)
	reconcileModel(t, r, model)
	setRouteParentStatus(t, r, model, gatewayB, metav1.ConditionTrue, "Accepted")
	reconcileModel(t, r, model)
	ruleName := attachmentDestinationRuleName(gatewayB, "tenant-a", "openai")
	if _, err := getUnstructured(r, destinationRuleGVK, "gateway-b", ruleName); err != nil {
		t.Fatalf("attachment DestinationRule: %v", err)
	}
	// The provider's own reconciler keeps only the rule for its tenant's
	// Gateway; the attachment's rule on another tenant's Gateway is not its.
	if _, err := r.ReconcileProvider(context.Background(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(provider)}); err != nil {
		t.Fatal(err)
	}
	if _, err := getUnstructured(r, destinationRuleGVK, "gateway-b", ruleName); err != nil {
		t.Fatalf("provider reconcile removed the attachment DestinationRule: %v", err)
	}
}

func TestTenantKeepsAttachmentsWhenItsOwnModelsServeNothing(t *testing.T) {
	r, model := servedAttachmentFixture(t)
	// The tenant's own model references a provider that does not exist, so
	// the tenant itself serves nothing.
	own := attachmentModel("own", "tenant-b", "own-model", time.Unix(300, 0))
	if err := r.Create(context.Background(), own); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(own)}); !errors.Is(err, resolver.ErrNoRoutes) {
		t.Fatalf("own reconcile = %v, want %v", err, resolver.ErrNoRoutes)
	}
	assertServed := func(when string) {
		t.Helper()
		if env := overlayEnvelope(t, r, "tenant-b"); len(env.Overlay.Candidates) != 1 || env.Overlay.Candidates[0].StableID != "team/chat/team/openai" {
			t.Fatalf("%s: overlay candidates = %#v, want the attachment", when, env.Overlay.Candidates)
		}
		if names := routeFilterNames(t, r, "tenant-b", "gateway-b"); len(names) != 4 || names[0] != "team."+attachmentRouteName(model, gatewayB)+".0" {
			t.Fatalf("%s: route filter patches = %v", when, names)
		}
	}
	assertServed("own model without routes")

	if err := r.Delete(context.Background(), getModel(t, r, own)); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(own)}); err != nil {
		t.Fatalf("own deletion: %v", err)
	}
	assertServed("last own model deleted")
}

func TestTranslationDisabledHomeModelKeepsAttachmentServing(t *testing.T) {
	r, attachment := servedAttachmentFixture(t)
	provider, secret := attachmentProvider("tenant-b")
	provider.Spec.Provider = "anthropic"
	provider.Spec.Endpoint = "api.anthropic.com"
	if err := r.Create(context.Background(), provider); err != nil {
		t.Fatal(err)
	}
	if err := r.Create(context.Background(), secret); err != nil {
		t.Fatal(err)
	}
	if _, err := r.ReconcileProvider(context.Background(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(provider)}); err != nil {
		t.Fatalf("reconcile Anthropic provider: %v", err)
	}
	own := attachmentModel("messages", "tenant-b", "anthropic-model", time.Unix(300, 0))
	own.Spec.ExternalProviderRefs[0].APIFormat = "messages"
	own.Spec.ExternalProviderRefs[0].Path = "/v1/messages"
	if err := r.Create(context.Background(), own); err != nil {
		t.Fatal(err)
	}
	setControllerTranslationEnabled(t, r, "tenant-b", true)
	reconcileModel(t, r, own)
	if got := overlayEnvelope(t, r, "tenant-b").Overlay.Candidates; len(got) != 2 {
		t.Fatalf("serving candidates before disabling translation = %#v, want home model and attachment", got)
	}

	setControllerTranslationEnabled(t, r, "tenant-b", false)
	if _, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(own)}); !errors.Is(err, resolver.ErrNoRoutes) {
		t.Fatalf("reconcile with translation disabled = %v, want ErrNoRoutes", err)
	}
	if got := overlayEnvelope(t, r, "tenant-b").Overlay.Candidates; len(got) != 1 || got[0].StableID != "team/chat/team/openai" {
		t.Fatalf("remaining candidates = %#v, want the cross-namespace attachment", got)
	}
	if _, err := getUnstructured(r, httpRouteGVK, "tenant-b", modelRouteName(own.Name)); !apierrors.IsNotFound(err) {
		t.Fatalf("home HTTPRoute survived translation disable: %v", err)
	}
	if _, err := getUnstructured(r, httpRouteGVK, "team", attachmentRouteName(attachment, gatewayB)); err != nil {
		t.Fatalf("attachment HTTPRoute was removed with the home route: %v", err)
	}
	if names := routeFilterNames(t, r, "tenant-b", "gateway-b"); len(names) != 4 || names[0] != "team."+attachmentRouteName(attachment, gatewayB)+".0" {
		t.Fatalf("remaining route filter patches = %v, want the attachment's five route rules", names)
	}
}

func TestNewAttachmentCarriesEnabledTranslationInHandoff(t *testing.T) {
	provider, secret := attachmentProvider("team")
	provider.Spec.Provider = "anthropic"
	model := attachmentModel("chat", "team", "chat-model", time.Unix(100, 0), gatewayB)
	model.Spec.ExternalProviderRefs[0].APIFormat = "messages"
	model.Spec.ExternalProviderRefs[0].Path = "/v1/messages"
	objects := []client.Object{provider, secret, model}
	objects = append(objects, attachmentTenantObjects("tenant-b", "tenant-b", gatewayB, "praxis")...)
	r := attachmentReconciler(t, objects...)
	setControllerTranslationEnabled(t, r, "tenant-b", true)

	reconcileModel(t, r, model)
	setRouteParentStatus(t, r, model, gatewayB, metav1.ConditionTrue, "Accepted")
	reconcileModel(t, r, model)

	got := getModel(t, r, model)
	condition := apiMeta.FindStatusCondition(got.Status.Conditions, conditionReady)
	if got.Status.Phase != resolver.PhaseReady || condition == nil || condition.Status != metav1.ConditionTrue {
		t.Fatalf("attached model status = %#v, want Ready after translation-enabled handoff", got.Status)
	}
	env := overlayEnvelope(t, r, "tenant-b")
	if len(env.Overlay.Candidates) != 1 || env.Overlay.Candidates[0].StableID != "team/chat/team/openai" {
		t.Fatalf("overlay candidates = %#v, want the accepted attachment", env.Overlay.Candidates)
	}
	if env.Overlay.Candidates[0].APIFormat != "messages" {
		t.Fatalf("attachment API format = %q, want messages", env.Overlay.Candidates[0].APIFormat)
	}
	route, err := getUnstructured(r, httpRouteGVK, "team", attachmentRouteName(model, gatewayB))
	if err != nil {
		t.Fatal(err)
	}
	assertExactSelectedPathRule(t, route, "/v1/messages", env.Overlay.Candidates[0].StableID)
}

func TestAttachmentKeepsTenantSelectionPolicy(t *testing.T) {
	tenantProvider, tenantSecret := attachmentProvider("tenant-b")
	tenantAlt, tenantAltSecret := namedProvider("tenant-b", "alt")
	// The tenant's own model spreads traffic over two equal-weight providers.
	ownModel := attachmentModel("own", "tenant-b", "own-model", time.Unix(300, 0))
	ownModel.Spec.ExternalProviderRefs = append(ownModel.Spec.ExternalProviderRefs, v1alpha1.ExternalProviderRef{
		Ref: v1alpha1.ExternalProviderReference{Name: tenantAlt.Name}, TargetModel: "gpt-4o", APIFormat: "openai-chat", Path: "/v1/chat/completions",
	})
	provider, secret := attachmentProvider("team")
	model := attachmentModel("chat", "team", "chat-model", time.Unix(100, 0), gatewayB)
	objects := append([]client.Object{tenantProvider, tenantSecret, tenantAlt, tenantAltSecret, ownModel, provider, secret, model},
		attachmentTenantObjects("tenant-b", "tenant-b", gatewayB, "praxis")...)
	r := attachmentReconciler(t, objects...)
	reconcileModel(t, r, ownModel)
	before := overlayEnvelope(t, r, "tenant-b").Overlay
	if mustJSON(t, before.SelectionPolicy) != `{"mode":"random"}` || len(before.Candidates) != 2 {
		t.Fatalf("own overlay = %#v, want two grouped candidates with the random policy", before)
	}

	reconcileModel(t, r, model)
	setRouteParentStatus(t, r, model, gatewayB, metav1.ConditionTrue, "Accepted")
	reconcileModel(t, r, model)
	after := overlayEnvelope(t, r, "tenant-b").Overlay
	if len(after.Candidates) != 3 || mustJSON(t, after.Candidates[:2]) != mustJSON(t, before.Candidates) || mustJSON(t, after.SelectionPolicy) != `{"mode":"random"}` {
		t.Fatalf("attachment publish changed the tenant's selection: before %#v after %#v", before, after)
	}
}

func TestSharedProviderStateStaysWhileAnAttachmentUsesIt(t *testing.T) {
	provider, secret := attachmentProvider("team")
	chat := attachmentModel("chat", "team", "chat-model", time.Unix(100, 0), gatewayB)
	other := attachmentModel("other", "team", "other-model", time.Unix(200, 0), gatewayB)
	objects := append([]client.Object{provider, secret, chat, other}, attachmentTenantObjects("tenant-b", "tenant-b", gatewayB, "praxis")...)
	r := attachmentReconciler(t, objects...)
	for _, model := range []*v1alpha1.ExternalModel{chat, other} {
		reconcileModel(t, r, model)
		setRouteParentStatus(t, r, model, gatewayB, metav1.ConditionTrue, "Accepted")
	}
	for _, model := range []*v1alpha1.ExternalModel{chat, other} {
		reconcileModel(t, r, model)
	}
	detach := func(model *v1alpha1.ExternalModel) {
		t.Helper()
		got := getModel(t, r, model)
		got.Spec.GatewayRefs = nil
		if err := r.Update(context.Background(), got); err != nil {
			t.Fatal(err)
		}
		reconcileModel(t, r, model)
	}
	// Both models use the same provider and credential: one DestinationRule
	// and one copy, kept while either accepted route references them.
	detach(chat)
	if _, err := getUnstructured(r, destinationRuleGVK, "gateway-b", attachmentDestinationRuleName(gatewayB, "team", "openai")); err != nil {
		t.Fatalf("shared DestinationRule removed while another attachment uses it: %v", err)
	}
	if _, err := getUnstructured(r, schema.GroupVersionKind{Version: "v1", Kind: "Service"}, "team", "provider-openai"); err != nil {
		t.Fatalf("shared provider Service removed while another attachment uses it: %v", err)
	}
	var copied corev1.Secret
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: "tenant-b", Name: credentialCopyName("tenant-b", "team", "openai-key", "api-key", "")}, &copied); err != nil {
		t.Fatalf("shared credential copy removed while another attachment uses it: %v", err)
	}
	detach(other)
	assertAttachmentWithdrawn(t, r)
	assertTransportCollected(t, r, "team", "chat", "other")
}

func TestDeletingAttachedOnlyModelCompletesWithoutItsTenant(t *testing.T) {
	r, model := servedAttachmentFixture(t)
	got := getModel(t, r, model)
	if !slices.Equal(got.Finalizers, []string{attachmentFinalizer}) {
		t.Fatalf("finalizers = %v, want only %s", got.Finalizers, attachmentFinalizer)
	}
	// The attached tenant is torn down first.
	for _, object := range attachmentTenantObjects("tenant-b", "tenant-b", gatewayB, "praxis") {
		if err := r.Delete(context.Background(), object); err != nil {
			t.Fatal(err)
		}
	}
	if err := r.Delete(context.Background(), got); err != nil {
		t.Fatal(err)
	}
	reconcileModel(t, r, model)
	if attachmentRouteExists(t, r, model) {
		t.Fatal("deleted model keeps its attachment route")
	}
	assertAttachmentWithdrawn(t, r)
	assertTransportCollected(t, r, "team", "chat")
	var deleted v1alpha1.ExternalModel
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(model), &deleted); !apierrors.IsNotFound(err) {
		t.Fatalf("model deletion did not complete: %v", err)
	}
}

func TestDeletingModelRemovesExtraAttachmentsWhileItsHomeWaits(t *testing.T) {
	gatewayA := v1alpha1.NamespacedObjectReference{Name: "gw-a", Namespace: "gateway-a"}
	provider, secret := attachmentProvider("tenant-a")
	model := attachmentModel("chat", "tenant-a", "chat-model", time.Unix(100, 0), gatewayA, gatewayB)
	sibling := attachmentModel("other", "tenant-a", "other-model", time.Unix(100, 0))
	objects := []client.Object{provider, secret, model, sibling}
	objects = append(objects, attachmentTenantObjects("tenant-a", "tenant-a", gatewayA, "praxis")...)
	objects = append(objects, attachmentTenantObjects("tenant-b", "tenant-b", gatewayB, "praxis")...)
	r := attachmentReconciler(t, objects...)
	reconcileModel(t, r, sibling)
	reconcileModel(t, r, model)
	setRouteParentStatus(t, r, model, gatewayB, metav1.ConditionTrue, "Accepted")
	reconcileModel(t, r, model)
	got := getModel(t, r, model)
	if !controllerutil.ContainsFinalizer(got, externalModelFinalizer) || !controllerutil.ContainsFinalizer(got, attachmentFinalizer) {
		t.Fatalf("finalizers = %v, want the home and attachment finalizers", got.Finalizers)
	}

	// Without its MaasTenantConfig the home path keeps its existing
	// conservative wait while a sibling remains; the extra attachment does not.
	mtc := tenant.NewMaasTenantConfig()
	mtc.SetNamespace("tenant-a")
	mtc.SetName(tenant.MaasTenantConfigInstanceName)
	if err := r.Delete(context.Background(), mtc); err != nil {
		t.Fatal(err)
	}
	if err := r.Delete(context.Background(), got); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(model)}); err == nil {
		t.Fatal("the home path must wait for a steady tenant while a sibling remains")
	}
	if _, err := getUnstructured(r, httpRouteGVK, "tenant-a", attachmentRouteName(model, gatewayB)); !apierrors.IsNotFound(err) {
		t.Fatalf("extra attachment route remains: %v", err)
	}
	if _, err := getUnstructured(r, destinationRuleGVK, "gateway-b", attachmentDestinationRuleName(gatewayB, "tenant-a", "openai")); !apierrors.IsNotFound(err) {
		t.Fatalf("extra attachment DestinationRule remains: %v", err)
	}
	if _, err := getUnstructured(r, httpRouteGVK, "tenant-a", "external-model-chat"); err != nil {
		t.Fatalf("home route removed while the home path waits: %v", err)
	}
	if got := getModel(t, r, model); !slices.Equal(got.Finalizers, []string{externalModelFinalizer}) {
		t.Fatalf("finalizers = %v, want only the home finalizer", got.Finalizers)
	}
}

// blockedDefaultTenantObjects returns a default tenant that selects Praxis by
// default but whose handoff has not started: maas-controller's migration gate
// keeps it on IPP, so it has no payload-processing-status.
func blockedDefaultTenantObjects() (ait, mtc *unstructured.Unstructured) {
	ait = controllerAITenant(tenant.DefaultAITenantName, "models-as-a-service", "models-as-a-service", defaultGateway.Name, defaultGateway.Namespace)
	mtc = controllerMaasTenantConfig("models-as-a-service", tenant.DefaultAITenantName, "models-as-a-service", "")
	annotations := mtc.GetAnnotations()
	delete(annotations, tenant.AnnotationPayloadProcessingType)
	delete(annotations, tenant.AnnotationPayloadProcessingStatus)
	mtc.SetAnnotations(annotations)
	return ait, mtc
}

func TestBlockedDefaultTenantMigrationLeavesIPPServedModelsAlone(t *testing.T) {
	ait, mtc := blockedDefaultTenantObjects()
	r, model := implicitAttachmentFixture(t, ait, mtc)
	if got := getModel(t, r, model); got.Status.Phase != "" || len(got.Status.Conditions) > 0 || len(got.Status.Gateways) > 0 || len(got.Finalizers) > 0 {
		t.Fatalf("model IPP still serves was changed: finalizers %v, status %#v", got.Finalizers, got.Status)
	}
	if _, err := getUnstructured(r, httpRouteGVK, "team", attachmentRouteName(model, defaultGateway)); !apierrors.IsNotFound(err) {
		t.Fatalf("attached to a default tenant IPP still serves: %v", err)
	}
	var provider v1alpha1.ExternalProvider
	if err := r.Get(context.Background(), types.NamespacedName{Namespace: "team", Name: "openai"}, &provider); err != nil {
		t.Fatal(err)
	}
	if provider.Status.Phase != "" || len(provider.Status.Conditions) > 0 {
		t.Fatalf("provider IPP still reports was given status %#v", provider.Status)
	}

	// A not-ready result written earlier is removed, so MaaS reads IPP's
	// route again; IPP's own status is left as it is.
	written := getModel(t, r, model)
	written.Status.Phase, written.Status.HTTPRouteName = resolver.PhaseReady, "chat"
	written.Status.Conditions = []metav1.Condition{{Type: conditionReady, Status: metav1.ConditionTrue, Reason: "Reconciled", LastTransitionTime: metav1.Now()}}
	written.Status.Gateways = []v1alpha1.ExternalModelGatewayStatus{{NamespacedObjectReference: defaultGateway, Conditions: []metav1.Condition{{
		Type: conditionReady, Status: metav1.ConditionFalse, Reason: reasonTenantNotSteady, LastTransitionTime: metav1.Now(),
	}}}}
	if err := r.Status().Update(context.Background(), written); err != nil {
		t.Fatal(err)
	}
	reconcileModel(t, r, model)
	got := getModel(t, r, model)
	if len(got.Status.Gateways) > 0 || got.Status.HTTPRouteName != "chat" || got.Status.Phase != resolver.PhaseReady || readyReason(got) != "Reconciled" {
		t.Fatalf("status after a blocked handoff = %#v", got.Status)
	}
}

func TestDefaultTenantEventsEnqueueProvidersOutsideTenants(t *testing.T) {
	outside, outsideSecret := attachmentProvider("team")
	inside, insideSecret := attachmentProvider("tenant-b")
	ait, mtc := blockedDefaultTenantObjects()
	objects := append([]client.Object{ait, mtc, outside, outsideSecret, inside, insideSecret},
		attachmentTenantObjects("tenant-b", "tenant-b", gatewayB, "praxis")...)
	r := controllerTestClient(t, objects...)
	outsideRequest := reconcile.Request{NamespacedName: client.ObjectKeyFromObject(outside)}
	if got := r.tenantProviders(context.Background(), mtc); !slices.Contains(got, outsideRequest) || slices.Contains(got, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(inside)}) {
		t.Fatalf("default tenant config event enqueued %v, want the provider outside tenant namespaces only", got)
	}
	if got := r.aitenantProviders(context.Background(), ait); !slices.Contains(got, outsideRequest) {
		t.Fatalf("default AITenant event enqueued %v, want %v", got, outsideRequest)
	}
	tenantB := controllerMaasTenantConfig("tenant-b", "tenant-b", "models-as-a-service", "praxis")
	if got := r.tenantProviders(context.Background(), tenantB); slices.Contains(got, outsideRequest) {
		t.Fatalf("another tenant's event enqueued %v", got)
	}
}

func TestAttachedModelReportsWhyNoProviderResolved(t *testing.T) {
	r, model := servedImplicitFixture(t)
	plain, plainSecret := attachmentProvider("team")
	plain.Name, plain.UID = "plain", "provider-plain"
	plain.Annotations = map[string]string{resolver.ProviderTLSAnnotation: "false"}
	for _, object := range []client.Object{plain, plainSecret} {
		if err := r.Create(context.Background(), object); err != nil && !apierrors.IsAlreadyExists(err) {
			t.Fatal(err)
		}
	}
	got := getModel(t, r, model)
	got.Spec.ExternalProviderRefs = []v1alpha1.ExternalProviderRef{
		{Ref: v1alpha1.ExternalProviderReference{Name: "ghost"}, TargetModel: "gpt-4o", APIFormat: "openai-chat", Path: "/v1/chat/completions"},
		{Ref: v1alpha1.ExternalProviderReference{Name: "plain"}, TargetModel: "gpt-4o", APIFormat: "openai-chat", Path: "/v1/chat/completions"},
	}
	if err := r.Update(context.Background(), got); err != nil {
		t.Fatal(err)
	}
	// A route name IPP recorded before the migration does not exist any more.
	got = getModel(t, r, model)
	got.Status.HTTPRouteName = "chat"
	if err := r.Status().Update(context.Background(), got); err != nil {
		t.Fatal(err)
	}
	reconcileModel(t, r, model)
	got = getModel(t, r, model)
	result := apiMeta.FindStatusCondition(gatewayResult(t, got, defaultGateway).Conditions, conditionReady)
	if got.Status.Phase != modelPhaseFailed || result == nil || result.Reason != reasonProviderNotReady ||
		!strings.Contains(result.Message, "provider ghost: external provider not found") ||
		!strings.Contains(result.Message, "provider plain: provider phase \"Failed\" is not Ready: "+resolver.ProviderTLSAnnotation) {
		t.Fatalf("attachment result = %#v, status = %#v", result, got.Status)
	}
	if got.Status.HTTPRouteName != "" {
		t.Fatalf("failed model keeps httpRouteName %q", got.Status.HTTPRouteName)
	}
}
