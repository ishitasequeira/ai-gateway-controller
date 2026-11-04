package tenant

import (
	"context"
	"encoding/json"
	"slices"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	v1alpha1 "github.com/opendatahub-io/ai-gateway-controller/api/inference/v1alpha1"
	"github.com/opendatahub-io/ai-gateway-controller/pkg/constants"
	"github.com/opendatahub-io/ai-gateway-controller/pkg/envelope"
	"github.com/opendatahub-io/ai-gateway-controller/pkg/render"
)

func TestAttachedOverlayCandidatesSkipRemovedCredentialCopies(t *testing.T) {
	credential := func(name string) *envelope.Credential {
		return &envelope.Credential{Strategy: "bearer_token", SecretRef: envelope.SecretRef{Name: name, Namespace: "tenant-b", Key: "api-key"}}
	}
	candidates := []envelope.Candidate{
		{Cluster: "provider-own", StableID: "own/provider-own", Name: "own", Credential: credential("own-key")},
		{Cluster: "provider-openai", StableID: "team/chat/team/openai", Name: "chat", Credential: credential("agc-cred-present")},
		{Cluster: "provider-openai", StableID: "gone/chat/gone/openai", Name: "gone", Credential: credential("agc-cred-removed")},
	}
	data, err := json.Marshal(envelope.Envelope{Overlay: envelope.Overlay{Candidates: candidates}})
	if err != nil {
		t.Fatal(err)
	}
	overlay := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: praxisOverlayName, Namespace: "tenant-b", Labels: map[string]string{managedByLabel: ManagedByAIGatewayController}},
		Data:       map[string]string{praxisOverlayDataKey: string(data)},
	}
	present := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "agc-cred-present", Namespace: "tenant-b"}}
	r := &Reconciler{Client: fake.NewClientBuilder().WithScheme(mtcSchemeForTests()).WithObjects(overlay, present).Build()}
	got, unreadable, err := r.attachedOverlayCandidates(context.Background(), "tenant-b")
	if err != nil || unreadable {
		t.Fatalf("attachedOverlayCandidates: unreadable=%t err=%v", unreadable, err)
	}
	if len(got) != 1 || got[0].StableID != "team/chat/team/openai" {
		t.Fatalf("attached candidates = %#v, want only the attachment whose copy exists", got)
	}
}

func TestAttachedOverlayCandidatesRestoreCredentialHeader(t *testing.T) {
	// Credential.Header is json:"-" and does not survive the overlay round-trip.
	// attachedOverlayCandidates must restore it from the credential-header
	// annotation the attachment reconciler stamps on the copy Secret, so an
	// Azure apikey candidate keeps its api-key header through a reconcile that
	// reads the published overlay back. A bearer_token copy carries no
	// annotation and its header stays empty.
	//
	// The annotation is honored only from a Secret the controller owns as a
	// credential copy, and only when the value is a header the controller
	// qualifies: an annotation on an untrusted Secret in the tenant namespace, or
	// an unrecognized header value, must not drive credential_inject and leaves
	// the header unset.
	copyLabels := map[string]string{LabelManagedBy: ManagedByAIGatewayController, LabelCredentialCopy: "true"}
	candidate := func(stableID, name, secret string) envelope.Candidate {
		return envelope.Candidate{Cluster: "provider-" + name, StableID: stableID, Name: name,
			Credential: &envelope.Credential{Strategy: "apikey", SecretRef: envelope.SecretRef{Name: secret, Namespace: "tenant-b", Key: "api-key"}}}
	}
	candidates := []envelope.Candidate{
		candidate("team/chat/team/azure", "azure", "agc-cred-azure"),
		{Cluster: "provider-openai", StableID: "team/gpt/team/openai", Name: "gpt",
			Credential: &envelope.Credential{Strategy: "bearer_token", SecretRef: envelope.SecretRef{Name: "agc-cred-openai", Namespace: "tenant-b", Key: "api-key"}}},
		candidate("team/forged/team/azure", "forged", "agc-cred-forged"),
		candidate("team/bogus/team/azure", "bogus", "agc-cred-bogus"),
	}
	data, err := json.Marshal(envelope.Envelope{Overlay: envelope.Overlay{Candidates: candidates}})
	if err != nil {
		t.Fatal(err)
	}
	overlay := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: praxisOverlayName, Namespace: "tenant-b", Labels: map[string]string{managedByLabel: ManagedByAIGatewayController}},
		Data:       map[string]string{praxisOverlayDataKey: string(data)},
	}
	azureCopy := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "agc-cred-azure", Namespace: "tenant-b",
		Labels: copyLabels, Annotations: map[string]string{AnnotationCredentialHeader: "api-key"}}}
	openaiCopy := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "agc-cred-openai", Namespace: "tenant-b", Labels: copyLabels}}
	// Not a controller-owned credential copy: a Secret a tenant planted in its
	// own namespace under the copy name, carrying a qualified header annotation.
	forgedCopy := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "agc-cred-forged", Namespace: "tenant-b",
		Annotations: map[string]string{AnnotationCredentialHeader: "api-key"}}}
	// A controller copy, but with a header value the controller does not qualify.
	bogusCopy := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "agc-cred-bogus", Namespace: "tenant-b",
		Labels: copyLabels, Annotations: map[string]string{AnnotationCredentialHeader: "authorization"}}}
	r := &Reconciler{Client: fake.NewClientBuilder().WithScheme(mtcSchemeForTests()).WithObjects(overlay, azureCopy, openaiCopy, forgedCopy, bogusCopy).Build()}
	got, unreadable, err := r.attachedOverlayCandidates(context.Background(), "tenant-b")
	if err != nil || unreadable {
		t.Fatalf("attachedOverlayCandidates: unreadable=%t err=%v", unreadable, err)
	}
	headers := map[string]string{}
	for _, c := range got {
		headers[c.StableID] = c.Credential.Header
	}
	if headers["team/chat/team/azure"] != "api-key" {
		t.Fatalf("azure candidate header = %q, want restored api-key; got %#v", headers["team/chat/team/azure"], got)
	}
	if headers["team/gpt/team/openai"] != "" {
		t.Fatalf("bearer_token candidate header = %q, want empty", headers["team/gpt/team/openai"])
	}
	if headers["team/forged/team/azure"] != "" {
		t.Fatalf("untrusted Secret header = %q, want empty (not a controller-owned credential copy)", headers["team/forged/team/azure"])
	}
	if headers["team/bogus/team/azure"] != "" {
		t.Fatalf("unqualified header value = %q, want empty", headers["team/bogus/team/azure"])
	}
}

func TestAttachedOverlayCandidatesLiveReadFallback(t *testing.T) {
	// The overlay ConfigMap event that triggers a tenant reconcile can arrive
	// before the Secret informer observed the credential copy written
	// alongside it. Simulate that lag — the cached client answers NotFound,
	// the API server serves the Secret — and require the candidate to be
	// admitted instead of silently skipped; a skipped candidate is dropped
	// from the rendered ExtProc mount until the next event.
	copyName := "agc-cred-lagged"
	candidates := []envelope.Candidate{
		{Cluster: "provider-openai", StableID: "team/chat/team/openai", Name: "chat",
			Credential: &envelope.Credential{Strategy: "bearer_token", SecretRef: envelope.SecretRef{Name: copyName, Namespace: "tenant-b", Key: "api-key"}}},
	}
	data, err := json.Marshal(envelope.Envelope{Overlay: envelope.Overlay{Candidates: candidates}})
	if err != nil {
		t.Fatal(err)
	}
	overlay := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: praxisOverlayName, Namespace: "tenant-b", Labels: map[string]string{managedByLabel: ManagedByAIGatewayController}},
		Data:       map[string]string{praxisOverlayDataKey: string(data)},
	}
	live := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: copyName, Namespace: "tenant-b",
		Labels: map[string]string{LabelManagedBy: ManagedByAIGatewayController, LabelCredentialCopy: "true"}}}
	base := fake.NewClientBuilder().WithScheme(mtcSchemeForTests()).WithObjects(overlay, live).Build()
	r := &Reconciler{
		Client: interceptor.NewClient(base, interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if _, meta := obj.(*metav1.PartialObjectMetadata); meta && key.Name == copyName {
					return apierrors.NewNotFound(corev1.Resource("secrets"), copyName)
				}
				return c.Get(ctx, key, obj, opts...)
			},
		}),
		APIReader: base,
	}
	got, unreadable, err := r.attachedOverlayCandidates(context.Background(), "tenant-b")
	if err != nil || unreadable {
		t.Fatalf("attachedOverlayCandidates: unreadable=%t err=%v", unreadable, err)
	}
	if len(got) != 1 || got[0].StableID != "team/chat/team/openai" {
		t.Fatalf("attached candidates = %#v, want the lagged-cache candidate admitted via the live read", got)
	}
}

func TestCleanupRemovesTenantAttachmentState(t *testing.T) {
	owned := map[string]string{LabelManagedBy: ManagedByAIGatewayController}
	copied := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "agc-cred-0123456789ab", Namespace: "tenant-b",
		Labels: map[string]string{LabelManagedBy: ManagedByAIGatewayController, LabelCredentialCopy: "true"}}}
	unrelated := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "provider-key", Namespace: "tenant-b", Labels: owned}}
	rule := func(name, tenantNamespace string) *unstructured.Unstructured {
		u := &unstructured.Unstructured{}
		u.SetGroupVersionKind(gvkDestinationRule)
		u.SetName(name)
		u.SetNamespace("gateway-b")
		u.SetLabels(map[string]string{LabelManagedBy: ManagedByAIGatewayController, LabelAttachmentTenant: tenantNamespace})
		return u
	}
	attachedRule, neighborRule := rule("provider-openai-0123456789ab", "tenant-b"), rule("provider-openai-987654321098", "tenant-c")
	fakeClient := fake.NewClientBuilder().WithScheme(mtcSchemeForTests()).WithObjects(copied, unrelated, attachedRule, neighborRule).Build()
	r := &Reconciler{Client: fakeClient, APIReader: fakeClient}
	if err := r.cleanup(context.Background(), "tenant-b", "gateway-b", "tenant-b"); err != nil {
		t.Fatal(err)
	}
	var secret corev1.Secret
	if err := fakeClient.Get(context.Background(), client.ObjectKeyFromObject(copied), &secret); !apierrors.IsNotFound(err) {
		t.Fatalf("credential copy survived tenant cleanup: %v", err)
	}
	if err := fakeClient.Get(context.Background(), client.ObjectKeyFromObject(unrelated), &secret); err != nil {
		t.Fatalf("cleanup removed a Secret that is not a credential copy: %v", err)
	}
	got := &unstructured.Unstructured{}
	got.SetGroupVersionKind(gvkDestinationRule)
	if err := fakeClient.Get(context.Background(), client.ObjectKeyFromObject(attachedRule), got); !apierrors.IsNotFound(err) {
		t.Fatalf("attachment DestinationRule survived tenant cleanup: %v", err)
	}
	if err := fakeClient.Get(context.Background(), client.ObjectKeyFromObject(neighborRule), got); err != nil {
		t.Fatalf("cleanup removed another tenant's attachment DestinationRule: %v", err)
	}
}

func TestTenantProvidersExcludeModelsBoundToOtherGateways(t *testing.T) {
	ref := func(name string) []v1alpha1.ExternalProviderRef {
		return []v1alpha1.ExternalProviderRef{{Ref: v1alpha1.ExternalProviderReference{Name: name}}}
	}
	local := &v1alpha1.ExternalModel{ObjectMeta: metav1.ObjectMeta{Name: "local", Namespace: "tenant-a"}, Spec: v1alpha1.ExternalModelSpec{ExternalProviderRefs: ref("local")}}
	own := &v1alpha1.ExternalModel{ObjectMeta: metav1.ObjectMeta{Name: "own", Namespace: "tenant-a"}, Spec: v1alpha1.ExternalModelSpec{
		ExternalProviderRefs: ref("own"), GatewayRefs: []v1alpha1.NamespacedObjectReference{{Name: "gw-a", Namespace: "gateway-a"}},
	}}
	elsewhere := &v1alpha1.ExternalModel{ObjectMeta: metav1.ObjectMeta{Name: "elsewhere", Namespace: "tenant-a"}, Spec: v1alpha1.ExternalModelSpec{
		ExternalProviderRefs: ref("elsewhere"), GatewayRefs: []v1alpha1.NamespacedObjectReference{{Name: "gw-b", Namespace: "gateway-b"}},
	}}
	// A provider in another namespace is never the same-named local one.
	foreign := &v1alpha1.ExternalModel{ObjectMeta: metav1.ObjectMeta{Name: "foreign", Namespace: "tenant-a"}, Spec: v1alpha1.ExternalModelSpec{
		ExternalProviderRefs: []v1alpha1.ExternalProviderRef{{Ref: v1alpha1.ExternalProviderReference{Name: "shared", Namespace: "shared"}}},
	}}
	provider := func(name string) *v1alpha1.ExternalProvider {
		return &v1alpha1.ExternalProvider{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "tenant-a"}}
	}
	r := &Reconciler{Client: fake.NewClientBuilder().WithScheme(mtcSchemeForTests()).WithObjects(
		local, own, elsewhere, foreign, provider("local"), provider("own"), provider("elsewhere"), provider("shared"),
	).Build()}
	_, required, err := r.providersForTenant(context.Background(), "tenant-a", "gw-a", "gateway-a", false)
	if err != nil {
		t.Fatal(err)
	}
	if !required["local"] || !required["own"] || required["elsewhere"] || len(required) != 2 {
		t.Fatalf("required providers = %v, want only local providers of models the tenant Gateway serves", required)
	}
}

func TestExternalModelCleanupKeepsRouteFilterForAttachments(t *testing.T) {
	filter := &unstructured.Unstructured{}
	filter.SetGroupVersionKind(gvkEnvoyFilter)
	filter.SetName(PayloadProcessingExternalModelEnvoyFilterName("tenant-b"))
	filter.SetNamespace("gateway-b")
	filter.SetLabels(map[string]string{LabelManagedBy: ManagedByAIGatewayController})
	route := &unstructured.Unstructured{}
	route.SetGroupVersionKind(gvkHTTPRoute)
	route.SetName("external-model-chat-0123456789ab")
	route.SetNamespace("team")
	route.SetLabels(map[string]string{LabelManagedBy: ManagedByAIGatewayController, LabelAttachmentTenant: "tenant-b"})
	fakeClient := fake.NewClientBuilder().WithScheme(mtcSchemeForTests()).WithObjects(filter, route).Build()
	r := &Reconciler{Client: fakeClient, APIReader: fakeClient}
	if err := r.cleanupExternalModelResources(context.Background(), "tenant-b", "gateway-b", "tenant-b"); err != nil {
		t.Fatal(err)
	}
	got := &unstructured.Unstructured{}
	got.SetGroupVersionKind(gvkEnvoyFilter)
	if err := fakeClient.Get(context.Background(), client.ObjectKeyFromObject(filter), got); err != nil {
		t.Fatalf("route filter removed while an attachment route targets the tenant: %v", err)
	}
}

func TestAttachmentOnlyTenantKeepsProcessorWhenOverlayIsUnreadable(t *testing.T) {
	requireManifests(t)
	mtc := withMTCFinalizer(withMarker(newMTC("tenant-ns", "redteam", PayloadProcessingBackendPraxis, "redteam", "ai-tenants"), PayloadProcessingStatusSteady))
	aitenant := newAITenantOwner("redteam", "ai-tenants", AITenantPhaseActive, "my-gateway", "tenant-ns", "tenant-ns")
	processorName := PayloadProcessingExternalModelDeploymentName(IdentifierFor(mtc))
	processor := &unstructured.Unstructured{}
	processor.SetGroupVersionKind(gvkDeployment)
	processor.SetName(processorName)
	processor.SetNamespace("tenant-ns")
	processor.SetLabels(map[string]string{managedByLabel: render.FieldOwner})
	// The tenant has no ExternalModels of its own: it serves only attachments,
	// and its controller-owned overlay cannot be parsed.
	overlay := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: praxisOverlayName, Namespace: "tenant-ns", Labels: map[string]string{managedByLabel: render.FieldOwner}},
		Data:       map[string]string{praxisOverlayDataKey: "{not json"},
	}
	rec := &recorder{}
	fakeClient := fake.NewClientBuilder().WithScheme(mtcSchemeForTests()).
		WithObjects(mtc, aitenant, processor, overlay, newGateway("my-gateway", "tenant-ns")).WithInterceptorFuncs(rec.funcs()).Build()
	r := &Reconciler{Client: fakeClient, APIReader: fakeClient, ManifestPath: manifestPath, Image: "img", MaaSAPIRouteNameBase: "maas-api-route", ResyncInterval: time.Minute}
	res, err := r.Reconcile(context.Background(), mtcRequest("tenant-ns"))
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if res.RequeueAfter != constants.NotReadyRequeueInterval {
		t.Fatalf("RequeueAfter = %v, want %v while the overlay is unreadable", res.RequeueAfter, constants.NotReadyRequeueInterval)
	}
	if _, _, deleted := rec.snapshot(); slices.Contains(deleted, processorName) {
		t.Fatalf("processor %s deleted for an unreadable overlay; deleted = %v", processorName, deleted)
	}
	got := &unstructured.Unstructured{}
	got.SetGroupVersionKind(gvkDeployment)
	if err := fakeClient.Get(context.Background(), client.ObjectKeyFromObject(processor), got); err != nil {
		t.Fatalf("processor: %v", err)
	}
}

func TestRuntimeCandidatesLeaveProvidersInOtherNamespacesToTheOverlay(t *testing.T) {
	foreign := &v1alpha1.ExternalModel{ObjectMeta: metav1.ObjectMeta{Name: "foreign", Namespace: "tenant-a"}, Spec: v1alpha1.ExternalModelSpec{
		ExternalProviderRefs: []v1alpha1.ExternalProviderRef{{Ref: v1alpha1.ExternalProviderReference{Name: "openai", Namespace: "shared"}, APIFormat: "openai-chat"}},
	}}
	// A Ready same-named local provider is not the referenced one.
	local := &v1alpha1.ExternalProvider{ObjectMeta: metav1.ObjectMeta{Name: "openai", Namespace: "tenant-a"},
		Spec: v1alpha1.ExternalProviderSpec{Provider: "openai"}, Status: v1alpha1.ExternalProviderStatus{Phase: "Ready"}}
	r := &Reconciler{Client: fake.NewClientBuilder().WithScheme(mtcSchemeForTests()).WithObjects(foreign, local).Build()}
	candidates, _, err := r.runtimeCandidates(context.Background(), "tenant-a", "gw-a", "gateway-a", false)
	if err != nil || len(candidates) != 0 {
		t.Fatalf("runtime candidates = %#v, err=%v; want none, the overlay carries the reference", candidates, err)
	}
}
