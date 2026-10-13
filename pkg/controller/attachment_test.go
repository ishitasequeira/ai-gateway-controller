package controller

import (
	"context"
	"encoding/json"
	"maps"
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

func TestUnsupportedAttachmentDoesNotBlockOtherAttachments(t *testing.T) {
	provider, secret := attachmentProvider("team")
	unmanaged := v1alpha1.NamespacedObjectReference{Name: "unmanaged", Namespace: "elsewhere"}
	model := attachmentModel("chat", "team", "chat-model", time.Unix(100, 0), gatewayC, unmanaged, gatewayB)
	objects := []client.Object{provider, secret, model}
	objects = append(objects, attachmentTenantObjects("tenant-b", "tenant-b", gatewayB, "praxis")...)
	objects = append(objects, attachmentTenantObjects("tenant-c", "tenant-c", gatewayC, "ipp")...)
	r := attachmentReconciler(t, objects...)
	reconcileModel(t, r, model)

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
	if got := getModel(t, attached, attachedModel); got.Status.Phase != resolver.PhaseReady || got.Status.HTTPRouteName != "external-model-chat" {
		t.Fatalf("own-gateway status = %#v", got.Status)
	}
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
	reconcileModel(t, r, model)
	got := getModel(t, r, model)
	condition := apiMeta.FindStatusCondition(got.Status.Conditions, conditionReady)
	if got.Status.Phase != modelPhasePending || condition == nil || condition.Status != metav1.ConditionFalse ||
		condition.Reason != reasonTenantNotResolved || !strings.Contains(condition.Message, "spec.gatewayRefs") {
		t.Fatalf("status = %#v", got.Status)
	}
	if _, err := getUnstructured(r, httpRouteGVK, "team", "external-model-chat"); !apierrors.IsNotFound(err) {
		t.Fatalf("serving state created without a tenant: %v", err)
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

func TestUnservedHomeAttachmentBlocksAggregateReady(t *testing.T) {
	gatewayA := v1alpha1.NamespacedObjectReference{Name: "gw-a", Namespace: "gateway-a"}
	provider, secret := attachmentProvider("tenant-a")
	model := attachmentModel("chat", "tenant-a", "chat-model", time.Unix(100, 0), gatewayA, gatewayB)
	objects := []client.Object{provider, secret, model}
	objects = append(objects, attachmentTenantObjects("tenant-a", "tenant-a", gatewayA, "ipp")...)
	objects = append(objects, attachmentTenantObjects("tenant-b", "tenant-b", gatewayB, "praxis")...)
	r := attachmentReconciler(t, objects...)
	reconcileModel(t, r, model)
	if got := getModel(t, r, model); got.Status.Phase == resolver.PhaseReady || readyReason(got) != reasonTenantNotPraxis {
		t.Fatalf("status = %q/%q, want the unserved home attachment reported", got.Status.Phase, readyReason(got))
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
