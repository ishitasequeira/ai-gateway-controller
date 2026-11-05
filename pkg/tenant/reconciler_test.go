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
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	v1alpha1 "github.com/opendatahub-io/ai-gateway-controller/api/inference/v1alpha1"
	"github.com/opendatahub-io/ai-gateway-controller/pkg/constants"
	"github.com/opendatahub-io/ai-gateway-controller/pkg/envelope"
	"github.com/opendatahub-io/ai-gateway-controller/pkg/render"
	"github.com/opendatahub-io/ai-gateway-controller/pkg/resolver"
)

// manifestPath points at the controller-owned composition of the pinned
// praxis-extproc overlay and ExternalModel patches. This package is a sibling
// of pkg/render, so the relative depth to the repo root is the same.
const manifestPath = "../../config/manifests/external-model/overlays/odh"

// mtcSchemeForTests registers MaasTenantConfigGVK and AITenantGVK (and their
// List kinds) with a bare scheme so the fake client can Get/List/Patch/
// Update them as unstructured without depending on maas-controller's Go
// types. GVKs used only for freshly-constructed Get/Delete targets (e.g. the
// praxis-extproc resource kinds in cleanup) do not need registration here —
// the fake client handles self-describing unstructured objects for those
// operations without it.
func mtcSchemeForTests() *runtime.Scheme {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = v1alpha1.AddToScheme(scheme)
	scheme.AddKnownTypeWithName(MaasTenantConfigGVK, &unstructured.Unstructured{})
	scheme.AddKnownTypeWithName(MaasTenantConfigGVK.GroupVersion().WithKind("MaasTenantConfigList"), &unstructured.UnstructuredList{})
	scheme.AddKnownTypeWithName(AITenantGVK, &unstructured.Unstructured{})
	scheme.AddKnownTypeWithName(AITenantGVK.GroupVersion().WithKind(AITenantGVK.Kind+"List"), &unstructured.UnstructuredList{})
	scheme.AddKnownTypeWithName(gvkGateway, &unstructured.Unstructured{})
	scheme.AddKnownTypeWithName(gvkGateway.GroupVersion().WithKind("GatewayList"), &unstructured.UnstructuredList{})
	return scheme
}

func newGateway(name, namespace string) *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(gvkGateway)
	u.SetName(name)
	u.SetNamespace(namespace)
	return u
}

func TestMaasTenantConfigForNamespaceMapsExternalModelEvents(t *testing.T) {
	r := &Reconciler{}
	model := &v1alpha1.ExternalModel{ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "tenant-a"}}
	requests := r.maasTenantConfigForNamespace(context.Background(), model)
	want := ctrl.Request{NamespacedName: client.ObjectKey{Namespace: "tenant-a", Name: MaasTenantConfigInstanceName}}
	if len(requests) != 1 || requests[0] != want {
		t.Fatalf("ExternalModel event enqueued %#v, want %#v", requests, want)
	}
}

func TestDeletedProviderReferenceDoesNotBlockTenantOverlayReadiness(t *testing.T) {
	ctx := context.Background()
	const namespace = "tenant-ns"
	model := &v1alpha1.ExternalModel{
		ObjectMeta: metav1.ObjectMeta{Name: "model", Namespace: namespace},
		Spec: v1alpha1.ExternalModelSpec{ExternalProviderRefs: []v1alpha1.ExternalProviderRef{
			{Ref: v1alpha1.ExternalProviderReference{Name: "live"}, TargetModel: "gpt", APIFormat: "openai-chat", Path: "/v1/chat/completions"},
			{Ref: v1alpha1.ExternalProviderReference{Name: "deleted"}, TargetModel: "gpt", APIFormat: "openai-chat", Path: "/v1/chat/completions"},
			{Ref: v1alpha1.ExternalProviderReference{Name: "later"}, TargetModel: "gpt", APIFormat: "openai-chat", Path: "/v1/chat/completions"},
		}},
	}
	provider := func(name string) *v1alpha1.ExternalProvider {
		return &v1alpha1.ExternalProvider{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
			Spec: v1alpha1.ExternalProviderSpec{Provider: "openai", Endpoint: name + ".example.com",
				Auth: v1alpha1.AuthConfig{Type: "apikey", SecretRef: v1alpha1.NameReference{Name: name + "-key"}}},
			Status: v1alpha1.ExternalProviderStatus{Phase: resolver.PhaseReady},
		}
	}
	live, later := provider("live"), provider("later")
	fakeClient := fake.NewClientBuilder().WithScheme(mtcSchemeForTests()).WithObjects(model, live).Build()
	r := &Reconciler{Client: fakeClient}
	writeOverlay := func(providers ...*v1alpha1.ExternalProvider) {
		t.Helper()
		set, err := resolver.Resolve([]*v1alpha1.ExternalModel{model}, providers)
		if err != nil {
			t.Fatal(err)
		}
		env, err := envelope.Render(set, envelope.Scope{Network: "test", Gateway: "gateway", Namespace: namespace, LocalSite: "site"}, envelope.Revision{}, envelope.Options{})
		if err != nil {
			t.Fatal(err)
		}
		raw, err := json.Marshal(env)
		if err != nil {
			t.Fatal(err)
		}
		configMap := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: praxisOverlayName, Namespace: namespace,
			Labels: map[string]string{managedByLabel: render.FieldOwner}}, Data: map[string]string{praxisOverlayDataKey: string(raw)}}
		var existing corev1.ConfigMap
		if err := fakeClient.Get(ctx, client.ObjectKeyFromObject(configMap), &existing); apierrors.IsNotFound(err) {
			if err := fakeClient.Create(ctx, configMap); err != nil {
				t.Fatal(err)
			}
		} else if err != nil {
			t.Fatal(err)
		} else {
			configMap.ResourceVersion = existing.ResourceVersion
			if err := fakeClient.Update(ctx, configMap); err != nil {
				t.Fatal(err)
			}
		}
	}
	writeOverlay(live)
	_, required, err := r.providersForTenant(ctx, namespace, "gateway", "gateway-system", false)
	if err != nil {
		t.Fatal(err)
	}
	if len(required) != 1 || !required["live"] {
		t.Fatalf("required providers with stale refs = %#v, want only live", required)
	}
	if ready, reason, err := r.routingOverlayReady(ctx, namespace, required); err != nil || !ready {
		t.Fatalf("overlay with live provider: ready=%t reason=%q err=%v", ready, reason, err)
	}
	if err := fakeClient.Create(ctx, later); err != nil {
		t.Fatal(err)
	}
	_, required, err = r.providersForTenant(ctx, namespace, "gateway", "gateway-system", false)
	if err != nil {
		t.Fatal(err)
	}
	if len(required) != 2 || !required["live"] || !required["later"] {
		t.Fatalf("required providers after add = %#v, want live and later", required)
	}
	if ready, _, err := r.routingOverlayReady(ctx, namespace, required); err != nil || ready {
		t.Fatalf("overlay before later provider publication: ready=%t err=%v", ready, err)
	}
	writeOverlay(live, later)
	if ready, reason, err := r.routingOverlayReady(ctx, namespace, required); err != nil || !ready {
		t.Fatalf("overlay after later provider publication: ready=%t reason=%q err=%v", ready, reason, err)
	}
	candidates, _, err := r.runtimeCandidates(ctx, namespace, "gateway", "gateway-system", false)
	if err != nil || len(candidates) != 2 {
		t.Fatalf("ExtProc runtime candidates = %#v, err=%v; want live and later", candidates, err)
	}
}

func TestDisabledAnthropicTranslationDoesNotBlockAttachmentOnlyOverlay(t *testing.T) {
	ctx := context.Background()
	const namespace = "tenant-ns"
	model := &v1alpha1.ExternalModel{
		ObjectMeta: metav1.ObjectMeta{Name: "home-model", Namespace: namespace},
		Spec: v1alpha1.ExternalModelSpec{ExternalProviderRefs: []v1alpha1.ExternalProviderRef{{
			Ref:         v1alpha1.ExternalProviderReference{Name: "anthropic"},
			TargetModel: "claude", APIFormat: "messages", Path: "/v1/messages",
		}}},
	}
	provider := &v1alpha1.ExternalProvider{
		ObjectMeta: metav1.ObjectMeta{Name: "anthropic", Namespace: namespace},
		Spec:       v1alpha1.ExternalProviderSpec{Provider: "anthropic", Endpoint: "anthropic.example.com"},
		Status:     v1alpha1.ExternalProviderStatus{Phase: resolver.PhaseReady},
	}
	attachment := envelope.Candidate{
		Cluster: "provider-openai", StableID: "attached/model/provider-openai",
		Kind: "inference_model", Name: "attached", Site: "local", Fresh: true,
		APIFormat: "openai-chat", Path: "/v1/chat/completions", TargetModel: "mock-openai-model",
	}
	rendered, err := envelope.Render(&resolver.ResolvedRouteSet{}, envelope.Scope{
		Network: "external-model", Gateway: "gateway", Namespace: namespace, LocalSite: "local",
	}, envelope.Revision{}, envelope.Options{PreservedCandidates: []envelope.Candidate{attachment}})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(rendered)
	if err != nil {
		t.Fatal(err)
	}
	configMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: praxisOverlayName, Namespace: namespace,
			Labels: map[string]string{managedByLabel: render.FieldOwner}},
		Data: map[string]string{praxisOverlayDataKey: string(raw)},
	}
	fakeClient := fake.NewClientBuilder().WithScheme(mtcSchemeForTests()).WithObjects(model, provider, configMap).Build()
	r := &Reconciler{Client: fakeClient}

	_, required, err := r.providersForTenant(ctx, namespace, "gateway", "gateway-system", false)
	if err != nil {
		t.Fatal(err)
	}
	if len(required) != 0 {
		t.Fatalf("disabled Anthropic binding required in overlay = %#v, want no home providers", required)
	}
	ready, reason, err := r.routingOverlayReady(ctx, namespace, required)
	if err != nil || !ready {
		t.Fatalf("attachment-only overlay readiness with translation disabled: ready=%t reason=%q err=%v", ready, reason, err)
	}

	_, required, err = r.providersForTenant(ctx, namespace, "gateway", "gateway-system", true)
	if err != nil {
		t.Fatal(err)
	}
	if len(required) != 1 || !required["anthropic"] {
		t.Fatalf("enabled Anthropic binding required providers = %#v, want anthropic", required)
	}
	ready, reason, err = r.routingOverlayReady(ctx, namespace, required)
	if err != nil || ready || !strings.Contains(reason, "provider anthropic") {
		t.Fatalf("enabled Anthropic binding without its candidate: ready=%t reason=%q err=%v, want not ready", ready, reason, err)
	}

	// The tenant still has an ExternalModel, so it enters the serving path. Its
	// only home binding is intentionally skipped while translation is disabled;
	// the published attachment must still drive the tenant ExtProc config.
	homeCandidates, policyExcludedOnly, homeErr := r.runtimeCandidates(ctx, namespace, "gateway", "gateway-system", false)
	if !errors.Is(homeErr, resolver.ErrNoRoutes) || !policyExcludedOnly {
		t.Fatalf("disabled Anthropic runtime candidates error = %v, policyExcludedOnly = %t; want ErrNoRoutes from policy exclusion", homeErr, policyExcludedOnly)
	}
	runtimeCandidates, err := combineRuntimeCandidates(homeCandidates, []envelope.Candidate{attachment}, homeErr, policyExcludedOnly)
	if err != nil || len(runtimeCandidates) != 1 || runtimeCandidates[0].StableID != attachment.StableID {
		t.Fatalf("attachment-only ExtProc runtime candidates = %#v, err=%v; want the served attachment", runtimeCandidates, err)
	}
	if candidates, err := combineRuntimeCandidates(homeCandidates, nil, homeErr, policyExcludedOnly); err != nil || len(candidates) != 0 {
		t.Fatalf("disabled home binding without an attachment = %#v, error = %v; want empty candidate set for cleanup", candidates, err)
	}
}

func TestMissingHomeProviderKeepsRuntimeConfiguration(t *testing.T) {
	set, err := resolver.ResolveAllWithAPITranslation([]*v1alpha1.ExternalModel{{
		ObjectMeta: metav1.ObjectMeta{Name: "home", Namespace: "tenant-ns"},
		Spec: v1alpha1.ExternalModelSpec{ExternalProviderRefs: []v1alpha1.ExternalProviderRef{{
			Ref: v1alpha1.ExternalProviderReference{Name: "missing"}, TargetModel: "gpt", APIFormat: "openai-chat", Path: "/v1/chat/completions",
		}}},
	}}, nil, false)
	if !errors.Is(err, resolver.ErrNoRoutes) || allRuntimeRoutesExcludedByPolicy(set) {
		t.Fatalf("missing provider: error = %v, policyExcludedOnly = %t", err, allRuntimeRoutesExcludedByPolicy(set))
	}
	attachment := []envelope.Candidate{{StableID: "attached/model/provider"}}
	if _, err := combineRuntimeCandidates(nil, attachment, err, allRuntimeRoutesExcludedByPolicy(set)); !errors.Is(err, resolver.ErrNoRoutes) {
		t.Fatalf("missing home provider with attachment error = %v, want ErrNoRoutes to retain last config", err)
	}
}

func TestCleanupExternalModelResourcesRemovesCredentialWorkload(t *testing.T) {
	scheme := mtcSchemeForTests()
	const tenantID = "redteam"
	const tenantNamespace = "tenant-ns"
	const gatewayNamespace = "gateway-system"
	owned := func(gvk schema.GroupVersionKind, name, namespace string) client.Object {
		u := &unstructured.Unstructured{}
		u.SetGroupVersionKind(gvk)
		u.SetName(name)
		u.SetNamespace(namespace)
		u.SetLabels(map[string]string{LabelManagedBy: ManagedByAIGatewayController})
		return u
	}
	objects := []client.Object{
		owned(gvkDeployment, PayloadProcessingExternalModelDeploymentName(tenantID), tenantNamespace),
		owned(gvkService, PayloadProcessingExternalModelServiceName(tenantID), tenantNamespace),
		owned(gvkConfigMap, PayloadProcessingExternalModelPluginsConfigMapForTenant(tenantID), tenantNamespace),
		owned(gvkServiceAccount, PayloadProcessingExternalModelServiceAccountName(tenantID), tenantNamespace),
		owned(gvkDestinationRule, PayloadProcessingExternalModelServiceName(tenantID), gatewayNamespace),
		owned(gvkEnvoyFilter, PayloadProcessingExternalModelEnvoyFilterName(tenantID), gatewayNamespace),
	}
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()
	r := &Reconciler{Client: fakeClient, APIReader: fakeClient}
	if err := r.cleanupExternalModelResources(context.Background(), tenantID, gatewayNamespace, tenantNamespace); err != nil {
		t.Fatalf("cleanupExternalModelResources: %v", err)
	}
	for _, target := range objects {
		got := &unstructured.Unstructured{}
		got.SetGroupVersionKind(target.GetObjectKind().GroupVersionKind())
		if err := fakeClient.Get(context.Background(), client.ObjectKeyFromObject(target), got); !apierrors.IsNotFound(err) {
			t.Fatalf("ExternalModel resource %s/%s remains: %v", target.GetNamespace(), target.GetName(), err)
		}
	}
}

// newMTC builds an unstructured MaasTenantConfig fixture in the shape
// AITenantReconciler.applyAITenantMetadata produces: AITenant-managed labels
// always set (tenantName == "" mirrors the default AITenant, whose
// LabelTenantName value is DefaultAITenantName and therefore resolves to the
// empty resource-naming identifier), plus the payload-processing-type and
// owning-AITenant annotations.
func newMTC(namespace, tenantName, payloadProcessingType, aitenantName, aitenantNamespace string) *unstructured.Unstructured {
	u := NewMaasTenantConfig()
	u.SetName(MaasTenantConfigInstanceName)
	u.SetNamespace(namespace)
	annotations := map[string]string{}
	if payloadProcessingType != "" {
		annotations[AnnotationPayloadProcessingType] = payloadProcessingType
	}
	if aitenantName != "" {
		annotations[AnnotationAITenantName] = aitenantName
	}
	if aitenantNamespace != "" {
		annotations[AnnotationAITenantNamespace] = aitenantNamespace
	}
	u.SetAnnotations(annotations)
	if tenantName == "" {
		tenantName = DefaultAITenantName
	}
	u.SetLabels(map[string]string{
		LabelManagedByAITenant: "true",
		LabelTenantName:        tenantName,
	})
	return u
}

func withMarker(u *unstructured.Unstructured, value string) *unstructured.Unstructured {
	annotations := u.GetAnnotations()
	if annotations == nil {
		annotations = map[string]string{}
	}
	annotations[AnnotationPayloadProcessingStatus] = value
	u.SetAnnotations(annotations)
	return u
}

func withMTCFinalizer(u *unstructured.Unstructured) *unstructured.Unstructured {
	controllerutil.AddFinalizer(u, PraxisCleanupFinalizer)
	return u
}

// newAITenantOwner builds an unstructured AITenant fixture that owns a
// MaasTenantConfig in tenantConfigNamespace (status.tenantNamespace —
// controller-authored ownership, distinct from gatewayRef.namespace).
// phase == "" omits status.phase entirely.
//
// When phase is set it also emits the AITenantConditionReady condition
// maas-controller writes alongside status.phase (setAITenantPhase), with
// observedGeneration equal to metadata.generation (0 by default) so
// DeployReady / StatusIsCurrent see a status computed for the current
// generation. Use withStaleReadyGeneration to simulate a lagging status.
func newAITenantOwner(name, namespace, phase, gatewayName, gatewayNamespace, tenantConfigNamespace string) *unstructured.Unstructured {
	u := NewAITenant()
	u.SetName(name)
	u.SetNamespace(namespace)
	status := map[string]any{}
	if phase != "" {
		status["phase"] = phase
		readyStatus := "False"
		if phase == AITenantPhaseActive {
			readyStatus = "True"
		}
		status["conditions"] = []any{
			map[string]any{
				"type":               AITenantConditionReady,
				"status":             readyStatus,
				"observedGeneration": u.GetGeneration(),
			},
		}
	}
	if gatewayName != "" || gatewayNamespace != "" {
		status["gatewayRef"] = map[string]any{"name": gatewayName, "namespace": gatewayNamespace}
	}
	if tenantConfigNamespace != "" {
		status["tenantNamespace"] = tenantConfigNamespace
	}
	u.Object["status"] = status
	return u
}

// withStaleReadyGeneration bumps metadata.generation above the Ready
// condition's observedGeneration, simulating an in-flight spec change
// maas-controller has not reconciled yet (status.phase / gatewayRef still
// reflect the prior generation). StatusIsCurrent / DeployReady treat it as
// not ready.
func withStaleReadyGeneration(u *unstructured.Unstructured) *unstructured.Unstructured {
	u.SetGeneration(u.GetGeneration() + 1)
	return u
}

// withDeletionTimestamp marks the object as terminating (non-zero
// metadata.deletionTimestamp) and adds a finalizer so the fake client admits it
// rather than treating it as an immediate delete. Simulates an owning AITenant
// whose delete is in flight while status.phase / gatewayRef still look valid.
func withDeletionTimestamp(u *unstructured.Unstructured) *unstructured.Unstructured {
	now := metav1.Now()
	u.SetDeletionTimestamp(&now)
	u.SetFinalizers([]string{"test.opendatahub.io/keep"})
	return u
}

// recorder captures what Reconciler did against the fake client, split by
// target so tests can assert on praxis-extproc resource applies,
// MaasTenantConfig finalizer/marker maintenance, and cleanup deletes
// independently.
type recorder struct {
	mu                 sync.Mutex
	resourcePatchNames []string
	resourcePatchKinds []string
	mtcPatches         int
	mtcUpdates         int
	deletedNames       []string
}

func (r *recorder) funcs() interceptor.Funcs {
	return interceptor.Funcs{
		// MaasTenantConfig's finalizer add/remove and marker mark-complete
		// use a real (non-SSA) merge patch, which the fake client supports
		// natively, so delegate to make the change observable on a
		// follow-up Get. Praxis-extproc resource applies use client.Apply
		// (SSA), which the fake client's tracker does not implement, so
		// those are recorded and short-circuited instead.
		Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			if obj.GetObjectKind().GroupVersionKind().Kind == MaasTenantConfigGVK.Kind {
				r.mu.Lock()
				r.mtcPatches++
				r.mu.Unlock()
				return c.Patch(ctx, obj, patch, opts...)
			}
			r.mu.Lock()
			r.resourcePatchNames = append(r.resourcePatchNames, obj.GetName())
			r.resourcePatchKinds = append(r.resourcePatchKinds, obj.GetObjectKind().GroupVersionKind().Kind)
			r.mu.Unlock()
			return nil
		},
		// The payload-processing status claim uses an optimistic-concurrency
		// Update (see claimPraxisSteady), which the fake client
		// supports natively including resourceVersion-conflict detection,
		// so delegate and just count it.
		Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			if obj.GetObjectKind().GroupVersionKind().Kind == MaasTenantConfigGVK.Kind {
				r.mu.Lock()
				r.mtcUpdates++
				r.mu.Unlock()
			}
			return c.Update(ctx, obj, opts...)
		},
		Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			r.mu.Lock()
			r.deletedNames = append(r.deletedNames, obj.GetName())
			r.mu.Unlock()
			return c.Delete(ctx, obj, opts...)
		},
	}
}

func (r *recorder) snapshot() (resourcePatchNames []string, mtcPatches int, deletedNames []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.resourcePatchNames...), r.mtcPatches, append([]string(nil), r.deletedNames...)
}

func (r *recorder) kindSnapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.resourcePatchKinds...)
}

func requireManifests(t *testing.T) {
	t.Helper()
	if _, err := render.Build(manifestPath); err != nil {
		t.Skipf("vendored manifests not present at %s; run hack/scripts/get-manifests.sh first: %v", manifestPath, err)
	}
}

func mtcRequest(namespace string) ctrl.Request {
	return ctrl.Request{NamespacedName: client.ObjectKey{Namespace: namespace, Name: MaasTenantConfigInstanceName}}
}

func TestReconcileSkipsWhenMaasTenantConfigNotFound(t *testing.T) {
	scheme := mtcSchemeForTests()
	rec := &recorder{}
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithInterceptorFuncs(rec.funcs()).Build()

	r := &Reconciler{Client: fakeClient, APIReader: fakeClient, ManifestPath: manifestPath, Image: "img", ResyncInterval: time.Minute}
	res, err := r.Reconcile(context.Background(), mtcRequest("ai-tenant-missing"))
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if res.RequeueAfter != 0 {
		t.Fatalf("RequeueAfter = %v, want 0 for a deleted/missing MaasTenantConfig", res.RequeueAfter)
	}
	patched, mtcPatches, deleted := rec.snapshot()
	if len(patched) != 0 || mtcPatches != 0 || len(deleted) != 0 {
		t.Fatalf("expected no writes, got patched=%v mtcPatches=%d deleted=%v", patched, mtcPatches, deleted)
	}
}

func TestReconcileSkipsWhenNotUsingPraxis(t *testing.T) {
	scheme := mtcSchemeForTests()
	mtc := newMTC("ai-tenant-redteam", "redteam", PayloadProcessingBackendIPP, "redteam", "ai-tenants")
	rec := &recorder{}
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(mtc).WithInterceptorFuncs(rec.funcs()).Build()

	r := &Reconciler{Client: fakeClient, APIReader: fakeClient, ManifestPath: manifestPath, Image: "img", ResyncInterval: time.Minute}
	res, err := r.Reconcile(context.Background(), mtcRequest("ai-tenant-redteam"))
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if res.RequeueAfter != 0 {
		t.Fatalf("RequeueAfter = %v, want 0 for a non-praxis tenant", res.RequeueAfter)
	}
	patched, mtcPatches, deleted := rec.snapshot()
	if len(patched) != 0 || mtcPatches != 0 || len(deleted) != 0 {
		t.Fatalf("expected no writes for a tenant that never used praxis, got patched=%v mtcPatches=%d deleted=%v", patched, mtcPatches, deleted)
	}
}

func TestReconcileAddsFinalizerAndRequeuesWhenGatewayRefMissing(t *testing.T) {
	scheme := mtcSchemeForTests()
	mtc := newMTC("ai-tenant-redteam", "redteam", PayloadProcessingBackendPraxis, "redteam", "ai-tenants")
	// Pending (not Active) with current Ready condition but no gatewayRef yet.
	aitenant := newAITenantOwner("redteam", "ai-tenants", "Pending", "", "", "ai-tenant-redteam")
	rec := &recorder{}
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(mtc, aitenant).WithInterceptorFuncs(rec.funcs()).Build()

	r := &Reconciler{Client: fakeClient, APIReader: fakeClient, ManifestPath: manifestPath, Image: "img", ResyncInterval: time.Hour}
	res, err := r.Reconcile(context.Background(), mtcRequest("ai-tenant-redteam"))
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if res.RequeueAfter != constants.NotReadyRequeueInterval {
		t.Fatalf("RequeueAfter = %v, want the short not-ready interval %v", res.RequeueAfter, constants.NotReadyRequeueInterval)
	}
	patched, mtcPatches, deleted := rec.snapshot()
	if len(patched) != 0 || len(deleted) != 0 {
		t.Fatalf("expected no resource applies/deletes before gatewayRef, got patched=%v deleted=%v", patched, deleted)
	}
	if mtcPatches != 1 {
		t.Fatalf("mtcPatches = %d, want 1 (adding the cleanup finalizer)", mtcPatches)
	}

	var got unstructured.Unstructured
	got.SetGroupVersionKind(MaasTenantConfigGVK)
	if err := fakeClient.Get(context.Background(), client.ObjectKey{Namespace: "ai-tenant-redteam", Name: MaasTenantConfigInstanceName}, &got); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !controllerutil.ContainsFinalizer(&got, PraxisCleanupFinalizer) {
		t.Fatal("expected PraxisCleanupFinalizer to be added even before gatewayRef is ready")
	}
}

// TestReconcileAppliesWhenPendingWithGatewayRef asserts praxis-extproc does
// not wait for AITenant Active: Pending + gatewayRef + Gateway object +
// handshake is enough (parity with legacy IPP deploy gating).
func TestReconcileAppliesWhenPendingWithGatewayRef(t *testing.T) {
	requireManifests(t)
	scheme := mtcSchemeForTests()
	mtc := withMarker(newMTC("tenant-ns", "redteam", PayloadProcessingBackendPraxis, "redteam", "ai-tenants"), PayloadProcessingStatusCleanupComplete)
	aitenant := newAITenantOwner("redteam", "ai-tenants", "Pending", "my-gateway", "tenant-ns", "tenant-ns")
	rec := &recorder{}
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(mtc, aitenant, newGateway("my-gateway", "tenant-ns")).WithInterceptorFuncs(rec.funcs()).Build()

	r := &Reconciler{Client: fakeClient, APIReader: fakeClient, ManifestPath: manifestPath, Image: "img", ResyncInterval: time.Hour}
	res, err := r.Reconcile(context.Background(), mtcRequest("tenant-ns"))
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if res.RequeueAfter != time.Hour {
		t.Fatalf("RequeueAfter = %v, want ResyncInterval after a successful Pending apply", res.RequeueAfter)
	}
	patched, _, deleted := rec.snapshot()
	if len(patched) == 0 {
		t.Fatal("expected praxis-extproc resources to be applied while AITenant is still Pending")
	}
	if len(deleted) != 0 {
		t.Fatalf("unexpected deletes on first Pending apply: %v", deleted)
	}
	got := &unstructured.Unstructured{}
	got.SetGroupVersionKind(MaasTenantConfigGVK)
	if err := fakeClient.Get(context.Background(), client.ObjectKey{Namespace: "tenant-ns", Name: MaasTenantConfigInstanceName}, got); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if PayloadProcessingStatus(got) != PayloadProcessingStatusSteady {
		t.Fatalf("status = %q, want %q after claim from Pending", PayloadProcessingStatus(got), PayloadProcessingStatusSteady)
	}
}

func TestReconcileAppliesSharedResourcesWithOnlyDisabledMessagesModel(t *testing.T) {
	requireManifests(t)
	const namespace = "tenant-ns"
	mtc := withMarker(newMTC(namespace, "redteam", PayloadProcessingBackendPraxis, "redteam", "ai-tenants"), PayloadProcessingStatusCleanupComplete)
	if err := unstructured.SetNestedMap(mtc.Object, map[string]any{"enabled": false}, "spec", "apiTranslation"); err != nil {
		t.Fatal(err)
	}
	aitenant := newAITenantOwner("redteam", "ai-tenants", AITenantPhaseActive, "my-gateway", namespace, namespace)
	model := &v1alpha1.ExternalModel{
		ObjectMeta: metav1.ObjectMeta{Name: "messages-only", Namespace: namespace},
		Spec: v1alpha1.ExternalModelSpec{ExternalProviderRefs: []v1alpha1.ExternalProviderRef{{
			Ref: v1alpha1.ExternalProviderReference{Name: "anthropic"}, TargetModel: "claude", APIFormat: "messages", Path: "/v1/messages",
		}}},
	}
	provider := &v1alpha1.ExternalProvider{
		ObjectMeta: metav1.ObjectMeta{Name: "anthropic", Namespace: namespace},
		Spec:       v1alpha1.ExternalProviderSpec{Provider: "anthropic", Endpoint: "anthropic.example.com"},
		Status:     v1alpha1.ExternalProviderStatus{Phase: resolver.PhaseReady},
	}
	rec := &recorder{}
	fakeClient := fake.NewClientBuilder().WithScheme(mtcSchemeForTests()).
		WithObjects(mtc, aitenant, newGateway("my-gateway", namespace), model, provider).
		WithInterceptorFuncs(rec.funcs()).Build()
	r := &Reconciler{Client: fakeClient, APIReader: fakeClient, ManifestPath: manifestPath, Image: "img", ResyncInterval: time.Hour}
	res, err := r.Reconcile(context.Background(), mtcRequest(namespace))
	if err != nil || res.RequeueAfter != time.Hour {
		t.Fatalf("Messages-only tenant reconcile: result=%#v err=%v, want shared resources applied", res, err)
	}
	patched, _, _ := rec.snapshot()
	if !slices.Contains(patched, PayloadProcessingPluginsConfigMapForTenant("redteam")) {
		t.Fatalf("shared ExtProc config was not applied: %v", patched)
	}
	if slices.Contains(patched, PayloadProcessingExternalModelPluginsConfigMapForTenant("redteam")) {
		t.Fatalf("disabled Messages model unexpectedly installed ExternalModel ExtProc: %v", patched)
	}
}

func TestReconcileRequeuesShortlyWhenActiveButGatewayRefNotReady(t *testing.T) {
	scheme := mtcSchemeForTests()
	mtc := newMTC("ai-tenant-redteam", "redteam", PayloadProcessingBackendPraxis, "redteam", "ai-tenants")
	aitenant := newAITenantOwner("redteam", "ai-tenants", AITenantPhaseActive, "", "", "ai-tenant-redteam")
	rec := &recorder{}
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(mtc, aitenant).WithInterceptorFuncs(rec.funcs()).Build()

	r := &Reconciler{Client: fakeClient, APIReader: fakeClient, ManifestPath: manifestPath, Image: "img", ResyncInterval: time.Hour}
	res, err := r.Reconcile(context.Background(), mtcRequest("ai-tenant-redteam"))
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if res.RequeueAfter != constants.NotReadyRequeueInterval {
		t.Fatalf("RequeueAfter = %v, want the short not-ready interval %v", res.RequeueAfter, constants.NotReadyRequeueInterval)
	}
	patched, _, deleted := rec.snapshot()
	if len(patched) != 0 || len(deleted) != 0 {
		t.Fatalf("expected no resource applies/deletes until status.gatewayRef is populated, got patched=%v deleted=%v", patched, deleted)
	}
}

// TestReconcileRequeuesWhenGatewayObjectMissing mirrors maas-controller's
// validateGatewayExists: gatewayRef alone is not enough; the Gateway CR must
// exist before praxis-extproc apply.
func TestReconcileRequeuesWhenGatewayObjectMissing(t *testing.T) {
	scheme := mtcSchemeForTests()
	mtc := withMarker(newMTC("tenant-ns", "redteam", PayloadProcessingBackendPraxis, "redteam", "ai-tenants"), PayloadProcessingStatusCleanupComplete)
	aitenant := newAITenantOwner("redteam", "ai-tenants", AITenantPhaseActive, "my-gateway", "tenant-ns", "tenant-ns")
	rec := &recorder{}
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(mtc, aitenant).WithInterceptorFuncs(rec.funcs()).Build()

	r := &Reconciler{Client: fakeClient, APIReader: fakeClient, ManifestPath: manifestPath, Image: "img", ResyncInterval: time.Hour}
	res, err := r.Reconcile(context.Background(), mtcRequest("tenant-ns"))
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if res.RequeueAfter != constants.NotReadyRequeueInterval {
		t.Fatalf("RequeueAfter = %v, want the short not-ready interval %v", res.RequeueAfter, constants.NotReadyRequeueInterval)
	}
	patched, _, deleted := rec.snapshot()
	if len(patched) != 0 || len(deleted) != 0 {
		t.Fatalf("expected no resource applies/deletes while Gateway object is missing, got patched=%v deleted=%v", patched, deleted)
	}
}

// TestReconcileRequeuesWhenAITenantStatusIsStale asserts praxis apply waits
// for StatusIsCurrent: a lagged Ready observedGeneration means gatewayRef
// may still describe the prior generation (e.g. mid re-home before claim).
func TestReconcileRequeuesWhenAITenantStatusIsStale(t *testing.T) {
	scheme := mtcSchemeForTests()
	mtc := withMarker(newMTC("tenant-ns", "redteam", PayloadProcessingBackendPraxis, "redteam", "ai-tenants"), PayloadProcessingStatusCleanupComplete)
	aitenant := withStaleReadyGeneration(newAITenantOwner("redteam", "ai-tenants", AITenantPhaseActive, "my-gateway", "tenant-ns", "tenant-ns"))
	rec := &recorder{}
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(mtc, aitenant, newGateway("my-gateway", "tenant-ns")).WithInterceptorFuncs(rec.funcs()).Build()

	r := &Reconciler{Client: fakeClient, APIReader: fakeClient, ManifestPath: manifestPath, Image: "img", ResyncInterval: time.Hour}
	res, err := r.Reconcile(context.Background(), mtcRequest("tenant-ns"))
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if res.RequeueAfter != constants.NotReadyRequeueInterval {
		t.Fatalf("RequeueAfter = %v, want the short not-ready interval %v", res.RequeueAfter, constants.NotReadyRequeueInterval)
	}
	patched, _, deleted := rec.snapshot()
	if len(patched) != 0 || len(deleted) != 0 {
		t.Fatalf("expected no apply while Ready observedGeneration is stale, got patched=%v deleted=%v", patched, deleted)
	}
}

// TestReconcileRequeuesWhenAITenantFailed asserts GatewayClaimFailed (and
// other Failed phases) do not get praxis-extproc even though gatewayRef is
// populated — maas writes the ref before the exclusive claim.
func TestReconcileRequeuesWhenAITenantFailed(t *testing.T) {
	scheme := mtcSchemeForTests()
	mtc := withMarker(newMTC("tenant-ns", "redteam", PayloadProcessingBackendPraxis, "redteam", "ai-tenants"), PayloadProcessingStatusCleanupComplete)
	aitenant := newAITenantOwner("redteam", "ai-tenants", AITenantPhaseFailed, "my-gateway", "tenant-ns", "tenant-ns")
	rec := &recorder{}
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(mtc, aitenant, newGateway("my-gateway", "tenant-ns")).WithInterceptorFuncs(rec.funcs()).Build()

	r := &Reconciler{Client: fakeClient, APIReader: fakeClient, ManifestPath: manifestPath, Image: "img", ResyncInterval: time.Hour}
	res, err := r.Reconcile(context.Background(), mtcRequest("tenant-ns"))
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if res.RequeueAfter != constants.NotReadyRequeueInterval {
		t.Fatalf("RequeueAfter = %v, want the short not-ready interval %v", res.RequeueAfter, constants.NotReadyRequeueInterval)
	}
	patched, _, deleted := rec.snapshot()
	if len(patched) != 0 || len(deleted) != 0 {
		t.Fatalf("expected no apply while AITenant is Failed, got patched=%v deleted=%v", patched, deleted)
	}
}

// TestAITenantStillValidForApply covers the RHAI-2514 apply-path fence helper:
// the live re-read must confirm the owning AITenant is still the same object
// (metadata.uid), still binds this MaasTenantConfig, still publishes the
// gatewayRef this reconcile rendered against, and is still DeployReady. A
// delete+recreate (new UID), a re-home (new gatewayRef), a lost ownership
// bind, an owner whose delete is in flight, Failed, or a stale Ready stamp
// must be rejected. Terminating phase alone (without deletionTimestamp) is
// still accepted if DeployReady holds.
func TestAITenantStillValidForApply(t *testing.T) {
	scheme := mtcSchemeForTests()
	mtc := newMTC("tenant-ns", "redteam", PayloadProcessingBackendPraxis, "redteam", "ai-tenants")
	ownerWithUID := func(uid string) *unstructured.Unstructured {
		u := newAITenantOwner("redteam", "ai-tenants", AITenantPhaseActive, "my-gateway", "tenant-ns", "tenant-ns")
		u.SetUID(types.UID(uid))
		return u
	}
	ownerWith := func(uid, phase, gatewayName string) *unstructured.Unstructured {
		u := newAITenantOwner("redteam", "ai-tenants", phase, gatewayName, "tenant-ns", "tenant-ns")
		u.SetUID(types.UID(uid))
		return u
	}
	withUID := func(u *unstructured.Unstructured, uid string) *unstructured.Unstructured {
		u.SetUID(types.UID(uid))
		return u
	}
	cases := []struct {
		name    string
		objects []client.Object
		mtc     *unstructured.Unstructured
		wantUID types.UID
		want    bool
	}{
		{"empty wantUID disables the check", []client.Object{mtc, ownerWithUID("uid-1")}, mtc, "", true},
		{"matching uid, active, gateway matches", []client.Object{mtc, ownerWithUID("uid-1")}, mtc, "uid-1", true},
		{"matching uid, pending, gateway matches", []client.Object{mtc, ownerWith("uid-1", "Pending", "my-gateway")}, mtc, "uid-1", true},
		{"matching uid, terminating, gateway matches", []client.Object{mtc, ownerWith("uid-1", "Terminating", "my-gateway")}, mtc, "uid-1", true},
		{"matching uid, stale generation, gateway matches", []client.Object{mtc, withStaleReadyGeneration(ownerWithUID("uid-1"))}, mtc, "uid-1", false},
		{"matching uid, failed, gateway matches", []client.Object{mtc, ownerWith("uid-1", AITenantPhaseFailed, "my-gateway")}, mtc, "uid-1", false},
		{"uid changed (recreate)", []client.Object{mtc, ownerWithUID("uid-2")}, mtc, "uid-1", false},
		{"owning AITenant gone", []client.Object{mtc}, mtc, "uid-1", false},
		{"no owning-ref annotations", []client.Object{ownerWithUID("uid-1")}, newMTC("tenant-ns", "redteam", PayloadProcessingBackendPraxis, "", ""), "uid-1", false},
		{"gateway re-homed (live gateway name differs)", []client.Object{mtc, ownerWith("uid-1", AITenantPhaseActive, "new-gateway")}, mtc, "uid-1", false},
		{"gateway re-homed (live gateway namespace differs)", []client.Object{mtc, withUID(newAITenantOwner("redteam", "ai-tenants", AITenantPhaseActive, "my-gateway", "other-ns", "tenant-ns"), "uid-1")}, mtc, "uid-1", false},
		{"gateway dropped (live gatewayRef unset)", []client.Object{mtc, withUID(newAITenantOwner("redteam", "ai-tenants", AITenantPhaseActive, "", "", "tenant-ns"), "uid-1")}, mtc, "uid-1", false},
		{"ownership bind lost (live tenantNamespace differs)", []client.Object{mtc,
			withUID(newAITenantOwner("redteam", "ai-tenants", AITenantPhaseActive, "my-gateway", "tenant-ns", "other-ns"), "uid-1")}, mtc, "uid-1", false},
		// Phase alone does not invalidate: "matching uid, terminating" above
		// stays valid, because the phase only flips once maas observes the
		// delete. Here every signal but deletionTimestamp still reports a
		// healthy owner, which is the window a fresh install could slip through.
		{"owner terminating (live deletionTimestamp set)", []client.Object{mtc, withDeletionTimestamp(ownerWithUID("uid-1"))}, mtc, "uid-1", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(c.objects...).Build()
			r := &Reconciler{Client: fakeClient, APIReader: fakeClient}
			got, err := r.aiTenantStillValidForApply(context.Background(), c.mtc, c.wantUID, "my-gateway", "tenant-ns")
			if err != nil {
				t.Fatalf("aiTenantStillValidForApply: %v", err)
			}
			if got != c.want {
				t.Fatalf("aiTenantStillValidForApply = %v, want %v", got, c.want)
			}
		})
	}
}

// TestReconcileRefusesWhenAITenantReplacedBeforeApply covers the UID
// fence at the reconcile level: an AITenant that resolves Active+current but is
// deleted and recreated (new metadata.uid) between resolution and the apply
// must abort the in-flight reconcile — no praxis-extproc resources may be
// written against a tenant identity that no longer matches. The Get
// interceptor swaps the UID on the second AITenant read (the pre-apply
// re-confirmation), simulating a mid-reconcile recreate.
func TestReconcileRefusesWhenAITenantReplacedBeforeApply(t *testing.T) {
	requireManifests(t)
	scheme := mtcSchemeForTests()
	mtc := withMarker(newMTC(DefaultAITenantName, "", PayloadProcessingBackendPraxis, DefaultAITenantName, "ai-tenants"), PayloadProcessingStatusCleanupComplete)
	aitenant := newAITenantOwner(DefaultAITenantName, "ai-tenants", AITenantPhaseActive, "my-gateway", DefaultAITenantName, DefaultAITenantName)
	aitenant.SetUID(types.UID("uid-1"))

	rec := &recorder{}
	funcs := rec.funcs()
	var aitenantGets int
	funcs.Get = func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
		if err := c.Get(ctx, key, obj, opts...); err != nil {
			return err
		}
		if u, ok := obj.(*unstructured.Unstructured); ok && u.GetKind() == AITenantGVK.Kind {
			aitenantGets++
			if aitenantGets >= 2 {
				u.SetUID(types.UID("uid-2")) // deleted+recreated mid-reconcile
			}
		}
		return nil
	}
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(mtc, aitenant, newGateway("my-gateway", DefaultAITenantName)).WithInterceptorFuncs(funcs).Build()

	r := &Reconciler{Client: fakeClient, APIReader: fakeClient, ManifestPath: manifestPath, Image: "img", MaaSAPIRouteNameBase: "maas-api-route", ResyncInterval: time.Minute}
	res, err := r.Reconcile(context.Background(), mtcRequest(DefaultAITenantName))
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if res.RequeueAfter != constants.NotReadyRequeueInterval {
		t.Fatalf("RequeueAfter = %v, want the short not-ready interval %v (owning AITenant replaced mid-reconcile)", res.RequeueAfter, constants.NotReadyRequeueInterval)
	}
	patched, _, deleted := rec.snapshot()
	if len(patched) != 0 || len(deleted) != 0 {
		t.Fatalf("expected no resource applies/deletes when the owning AITenant was replaced before apply, got patched=%v deleted=%v", patched, deleted)
	}
}

// TestReconcileUsesLiveReadNotStaleCache is the Finding #1 regression guard.
// After a watch drop the informer cache still holds the
// pre-delete AITenant (uid-1, Active), but the object has actually been
// deleted. The pre-apply fence must re-read live (APIReader), see it gone,
// and refuse to apply — proving the fence no longer trusts a stale cache.
func TestReconcileUsesLiveReadNotStaleCache(t *testing.T) {
	requireManifests(t)
	scheme := mtcSchemeForTests()
	mtc := withMarker(newMTC(DefaultAITenantName, "", PayloadProcessingBackendPraxis, DefaultAITenantName, "ai-tenants"), PayloadProcessingStatusCleanupComplete)
	aitenant := newAITenantOwner(DefaultAITenantName, "ai-tenants", AITenantPhaseActive, "my-gateway", DefaultAITenantName, DefaultAITenantName)
	aitenant.SetUID(types.UID("uid-1"))
	gw := newGateway("my-gateway", DefaultAITenantName)

	rec := &recorder{}
	// Cache-backed client (r.Client): MTC + the stale, pre-delete AITenant + Gateway.
	cacheClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(mtc, aitenant, gw).WithInterceptorFuncs(rec.funcs()).Build()
	// Live client (r.APIReader): the AITenant is gone (deleted during the watch drop).
	liveClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(mtc, gw).Build()

	r := &Reconciler{Client: cacheClient, APIReader: liveClient, ManifestPath: manifestPath, Image: "img", MaaSAPIRouteNameBase: "maas-api-route", ResyncInterval: time.Minute}
	res, err := r.Reconcile(context.Background(), mtcRequest(DefaultAITenantName))
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if res.RequeueAfter != constants.NotReadyRequeueInterval {
		t.Fatalf("RequeueAfter = %v, want the short not-ready interval %v (AITenant deleted; only the live read can see it)", res.RequeueAfter, constants.NotReadyRequeueInterval)
	}
	patched, _, deleted := rec.snapshot()
	if len(patched) != 0 || len(deleted) != 0 {
		t.Fatalf("expected no resource applies/deletes when the live read shows the owning AITenant is gone, got patched=%v deleted=%v", patched, deleted)
	}
}

// TestReconcileRequeuesWhenAITenantDeleted covers matrix row 4: the owning
// AITenant is gone, so resolveOwningAITenant returns not-ready and the apply
// path requeues without writing praxis-extproc resources. (The delete/cleanup
// path is exercised by the cleanup tests.)
func TestReconcileRequeuesWhenAITenantDeleted(t *testing.T) {
	requireManifests(t)
	scheme := mtcSchemeForTests()
	mtc := withMarker(newMTC(DefaultAITenantName, "", PayloadProcessingBackendPraxis, DefaultAITenantName, "ai-tenants"), PayloadProcessingStatusCleanupComplete)
	// No AITenant object: it has been deleted.
	rec := &recorder{}
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(mtc).WithInterceptorFuncs(rec.funcs()).Build()

	r := &Reconciler{Client: fakeClient, APIReader: fakeClient, ManifestPath: manifestPath, Image: "img", MaaSAPIRouteNameBase: "maas-api-route", ResyncInterval: time.Minute}
	res, err := r.Reconcile(context.Background(), mtcRequest(DefaultAITenantName))
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if res.RequeueAfter != constants.NotReadyRequeueInterval {
		t.Fatalf("RequeueAfter = %v, want the short not-ready interval %v (owning AITenant deleted)", res.RequeueAfter, constants.NotReadyRequeueInterval)
	}
	patched, _, deleted := rec.snapshot()
	if len(patched) != 0 || len(deleted) != 0 {
		t.Fatalf("expected no resource applies/deletes when the owning AITenant is gone, got patched=%v deleted=%v", patched, deleted)
	}
}

// TestReconcileWaitsForBlockedMigrationMarker covers both ways a
// MaasTenantConfig can have an absent status: a cleanup genuinely in
// flight, and a tenant that has never swapped payload-processing backends
// before (which, absent maas-controller's create-time seeding — see
// AnnotationPayloadProcessingStatus's doc comment — is this fixture's
// case). Both must block a transitioning-in praxis party: proceeding on an
// absent status would race an in-flight legacy IPP cleanup for any tenant
// that has never swapped before.
func TestReconcileWaitsForBlockedMigrationMarker(t *testing.T) {
	scheme := mtcSchemeForTests()
	mtc := newMTC("tenant-ns", "redteam", PayloadProcessingBackendPraxis, "redteam", "ai-tenants") // no status annotation
	aitenant := newAITenantOwner("redteam", "ai-tenants", AITenantPhaseActive, "my-gateway", "tenant-ns", "tenant-ns")
	rec := &recorder{}
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(mtc, aitenant, newGateway("my-gateway", "tenant-ns")).WithInterceptorFuncs(rec.funcs()).Build()

	r := &Reconciler{Client: fakeClient, APIReader: fakeClient, ManifestPath: manifestPath, Image: "img", ResyncInterval: time.Minute}
	res, err := r.Reconcile(context.Background(), mtcRequest("tenant-ns"))
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if res.RequeueAfter != constants.NotReadyRequeueInterval {
		t.Fatalf("RequeueAfter = %v, want %v", res.RequeueAfter, constants.NotReadyRequeueInterval)
	}
	names, _, _ := rec.snapshot()
	if len(names) != 0 {
		t.Fatalf("expected no praxis apply while the migration status is blocked, got %v", names)
	}
}

// TestReconcileProceedsWhenMigrationMarkerClear asserts the brand-new-tenant
// bootstrap case: maas-controller seeds every new MaasTenantConfig with
// PayloadProcessingStatusCleanupComplete at creation time, so the
// initial transition-in is never blocked. Claiming then writes steady.
func TestReconcileProceedsWhenMigrationMarkerClear(t *testing.T) {
	requireManifests(t)
	scheme := mtcSchemeForTests()
	mtc := withMarker(newMTC(DefaultAITenantName, "", PayloadProcessingBackendPraxis, DefaultAITenantName, "ai-tenants"), PayloadProcessingStatusCleanupComplete)
	aitenant := newAITenantOwner(DefaultAITenantName, "ai-tenants", AITenantPhaseActive, "my-gateway", DefaultAITenantName, DefaultAITenantName)
	rec := &recorder{}
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(mtc, aitenant, newGateway("my-gateway", DefaultAITenantName)).WithInterceptorFuncs(rec.funcs()).Build()

	r := &Reconciler{Client: fakeClient, APIReader: fakeClient, ManifestPath: manifestPath, Image: "img", MaaSAPIRouteNameBase: "maas-api-route", ResyncInterval: time.Minute}
	res, err := r.Reconcile(context.Background(), mtcRequest(DefaultAITenantName))
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if res.RequeueAfter != time.Minute {
		t.Fatalf("RequeueAfter = %v, want ResyncInterval", res.RequeueAfter)
	}
	names, _, _ := rec.snapshot()
	assertContains(t, names, "payload-processing")

	var got unstructured.Unstructured
	got.SetGroupVersionKind(MaasTenantConfigGVK)
	if err := fakeClient.Get(context.Background(), client.ObjectKey{Namespace: DefaultAITenantName, Name: MaasTenantConfigInstanceName}, &got); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if PayloadProcessingStatus(&got) != PayloadProcessingStatusSteady {
		t.Fatalf("status = %q, want %q after claim", PayloadProcessingStatus(&got), PayloadProcessingStatusSteady)
	}
}

// TestReconcileSkipsMigrationMarkerCheckWhenBundleAlreadyExists is the
// steady-state / crash-recovery guarantee: once status is steady, subsequent
// reconciles keep applying unconditionally — even without consulting
// cleanup-complete again.
func TestReconcileSkipsMigrationMarkerCheckWhenBundleAlreadyExists(t *testing.T) {
	requireManifests(t)
	scheme := mtcSchemeForTests()
	mtc := withMTCFinalizer(withMarker(newMTC(DefaultAITenantName, "", PayloadProcessingBackendPraxis, DefaultAITenantName, "ai-tenants"), PayloadProcessingStatusSteady))
	aitenant := newAITenantOwner(DefaultAITenantName, "ai-tenants", AITenantPhaseActive, "my-gateway", DefaultAITenantName, DefaultAITenantName)
	existingDeployment := &unstructured.Unstructured{}
	existingDeployment.SetGroupVersionKind(gvkDeployment)
	existingDeployment.SetName(PayloadProcessingDeploymentName(""))
	existingDeployment.SetNamespace(DefaultAITenantName)
	existingDeployment.SetLabels(map[string]string{managedByLabel: render.FieldOwner})
	rec := &recorder{}
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(mtc, aitenant, existingDeployment, newGateway("my-gateway", DefaultAITenantName)).WithInterceptorFuncs(rec.funcs()).Build()

	r := &Reconciler{Client: fakeClient, APIReader: fakeClient, ManifestPath: manifestPath, Image: "img", MaaSAPIRouteNameBase: "maas-api-route", ResyncInterval: time.Minute}
	res, err := r.Reconcile(context.Background(), mtcRequest(DefaultAITenantName))
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if res.RequeueAfter != time.Minute {
		t.Fatalf("RequeueAfter = %v, want ResyncInterval (steady status must not gate an already-claimed deploy)", res.RequeueAfter)
	}
	names, _, _ := rec.snapshot()
	assertContains(t, names, "payload-processing")
}

func TestReconcileAppliesAndRequeuesResyncIntervalForNonDefaultTenant(t *testing.T) {
	requireManifests(t)
	scheme := mtcSchemeForTests()
	mtc := withMarker(newMTC("tenant-ns", "redteam", PayloadProcessingBackendPraxis, "redteam", "ai-tenants"), PayloadProcessingStatusCleanupComplete)
	aitenant := newAITenantOwner("redteam", "ai-tenants", AITenantPhaseActive, "my-gateway", "tenant-ns", "tenant-ns")
	rec := &recorder{}
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(mtc, aitenant, newGateway("my-gateway", "tenant-ns")).WithInterceptorFuncs(rec.funcs()).Build()

	const resync = 5 * time.Minute
	r := &Reconciler{Client: fakeClient, APIReader: fakeClient, ManifestPath: manifestPath, Image: "img", MaaSAPIRouteNameBase: "maas-api-route", ResyncInterval: resync}
	res, err := r.Reconcile(context.Background(), mtcRequest("tenant-ns"))
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if res.RequeueAfter != resync {
		t.Fatalf("RequeueAfter = %v, want ResyncInterval %v", res.RequeueAfter, resync)
	}

	names, mtcPatches, _ := rec.snapshot()
	if len(names) == 0 {
		t.Fatal("resource patches = 0, want at least one applied resource")
	}
	if mtcPatches != 1 {
		t.Fatalf("mtcPatches = %d, want 1 (adding the cleanup finalizer)", mtcPatches)
	}

	assertContains(t, names, "payload-processing-redteam")
	assertContains(t, names, "payload-pre-processing-redteam")
	assertContains(t, names, "payload-processing-plugins-redteam")
	assertContains(t, names, "payload-processing-reader-redteam")
	// The shared ClusterRole must NOT be tenant-suffixed.
	assertContains(t, names, "payload-processing-reader")
	for _, n := range names {
		if n == "payload-processing" || n == "payload-pre-processing" {
			t.Fatalf("found unsuffixed resource name %q while reconciling a non-default tenant", n)
		}
	}
}

func TestReconcileAppliesUnsuffixedNamesForDefaultTenant(t *testing.T) {
	requireManifests(t)
	scheme := mtcSchemeForTests()
	mtc := withMarker(newMTC(DefaultAITenantName, "", PayloadProcessingBackendPraxis, DefaultAITenantName, "ai-tenants"), PayloadProcessingStatusCleanupComplete)
	aitenant := newAITenantOwner(DefaultAITenantName, "ai-tenants", AITenantPhaseActive, "my-gateway", "openshift-ingress", DefaultAITenantName)
	rec := &recorder{}
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(mtc, aitenant, newGateway("my-gateway", "openshift-ingress")).WithInterceptorFuncs(rec.funcs()).Build()

	r := &Reconciler{Client: fakeClient, APIReader: fakeClient, ManifestPath: manifestPath, Image: "img", MaaSAPIRouteNameBase: "maas-api-route", ResyncInterval: time.Minute}
	if _, err := r.Reconcile(context.Background(), mtcRequest(DefaultAITenantName)); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	names, _, _ := rec.snapshot()
	assertContains(t, names, "payload-processing")
	assertContains(t, names, "payload-pre-processing")
	assertContains(t, names, "payload-processing-plugins")
	assertContains(t, names, "payload-processing-reader")
	for _, kind := range rec.kindSnapshot() {
		if kind == "HorizontalPodAutoscaler" {
			t.Fatal("unexpected HPA apply when autoscaling is not configured")
		}
	}
}

func TestReconcileAppliesHPAsWhenAutoscalingConfigured(t *testing.T) {
	requireManifests(t)
	scheme := mtcSchemeForTests()
	mtc := withMarker(newMTC("tenant-ns", "redteam", PayloadProcessingBackendPraxis, "redteam", "ai-tenants"), PayloadProcessingStatusCleanupComplete)
	mtc.Object["spec"] = map[string]any{
		"payloadProcessing": map[string]any{
			"replicas":    int64(2),
			"autoscaling": map[string]any{"maxReplicas": int64(6)},
		},
		"payloadPreProcessing": map[string]any{
			"autoscaling": map[string]any{},
		},
	}
	aitenant := newAITenantOwner("redteam", "ai-tenants", AITenantPhaseActive, "my-gateway", "openshift-ingress", "tenant-ns")
	rec := &recorder{}
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(mtc, aitenant, newGateway("my-gateway", "openshift-ingress")).WithInterceptorFuncs(rec.funcs()).Build()

	r := &Reconciler{Client: fakeClient, APIReader: fakeClient, ManifestPath: manifestPath, Image: "img", MaaSAPIRouteNameBase: "maas-api-route", ResyncInterval: time.Minute}
	if _, err := r.Reconcile(context.Background(), mtcRequest("tenant-ns")); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	names, _, _ := rec.snapshot()
	assertContains(t, names, "payload-processing-redteam")
	assertContains(t, names, "payload-pre-processing-redteam")
	hpaCount := 0
	for _, kind := range rec.kindSnapshot() {
		if kind == "HorizontalPodAutoscaler" {
			hpaCount++
		}
	}
	if hpaCount != 2 {
		t.Fatalf("HPA applies = %d, want 2; kinds=%v names=%v", hpaCount, rec.kindSnapshot(), names)
	}
}

func TestReconcileDeletesOrphanedHPAsWhenAutoscalingRemoved(t *testing.T) {
	requireManifests(t)
	scheme := mtcSchemeForTests()
	mtc := withMarker(newMTC("tenant-ns", "redteam", PayloadProcessingBackendPraxis, "redteam", "ai-tenants"), PayloadProcessingStatusCleanupComplete)
	aitenant := newAITenantOwner("redteam", "ai-tenants", AITenantPhaseActive, "my-gateway", "openshift-ingress", "tenant-ns")
	orphan := &unstructured.Unstructured{}
	orphan.SetGroupVersionKind(gvkHPA)
	orphan.SetName(PayloadProcessingHPAName("redteam"))
	orphan.SetNamespace("openshift-ingress")
	orphan.SetLabels(map[string]string{LabelManagedBy: ManagedByAIGatewayController})
	rec := &recorder{}
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(mtc, aitenant, orphan, newGateway("my-gateway", "openshift-ingress")).WithInterceptorFuncs(rec.funcs()).Build()

	r := &Reconciler{Client: fakeClient, APIReader: fakeClient, ManifestPath: manifestPath, Image: "img", MaaSAPIRouteNameBase: "maas-api-route", ResyncInterval: time.Minute}
	if _, err := r.Reconcile(context.Background(), mtcRequest("tenant-ns")); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	_, _, deleted := rec.snapshot()
	assertContains(t, deleted, PayloadProcessingHPAName("redteam"))
}

// TestReconcileRejectsSpoofedOwningAITenantAnnotations covers CWE-639:
// tenant-admin can patch aitenant-name/aitenant-namespace on their
// MaasTenantConfig, but status.tenantNamespace on the fetched AITenant
// must still equal the MaasTenantConfig namespace before praxis apply or
// cleanup may use that AITenant's gatewayRef.
func TestReconcileRejectsSpoofedOwningAITenantAnnotations(t *testing.T) {
	requireManifests(t)
	scheme := mtcSchemeForTests()
	// Attacker's MTC points annotations at victim's AITenant.
	mtc := withMarker(newMTC("ai-tenant-attacker", "attacker", PayloadProcessingBackendPraxis, "victim", "ai-tenants"), PayloadProcessingStatusCleanupComplete)
	victim := newAITenantOwner("victim", "ai-tenants", AITenantPhaseActive, "victim-gateway", "ai-tenant-victim", "ai-tenant-victim")
	rec := &recorder{}
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(mtc, victim).WithInterceptorFuncs(rec.funcs()).Build()

	r := &Reconciler{Client: fakeClient, APIReader: fakeClient, ManifestPath: manifestPath, Image: "img", MaaSAPIRouteNameBase: "maas-api-route", ResyncInterval: time.Minute}
	_, err := r.Reconcile(context.Background(), mtcRequest("ai-tenant-attacker"))
	if err == nil {
		t.Fatal("Reconcile error = nil, want ownership mismatch error")
	}
	if !strings.Contains(err.Error(), "does not own MaasTenantConfig") {
		t.Fatalf("Reconcile error = %v, want ownership mismatch", err)
	}
	names, _, deleted := rec.snapshot()
	if len(names) != 0 || len(deleted) != 0 {
		t.Fatalf("expected no apply/delete against victim gateway, got patched=%v deleted=%v", names, deleted)
	}
}

func TestReconcileSwitchAwayRejectsSpoofedOwningAITenantAnnotations(t *testing.T) {
	scheme := mtcSchemeForTests()
	mtc := withMTCFinalizer(newMTC("ai-tenant-attacker", "attacker", PayloadProcessingBackendIPP, "victim", "ai-tenants"))
	victim := newAITenantOwner("victim", "ai-tenants", AITenantPhaseActive, "victim-gateway", "ai-tenant-victim", "ai-tenant-victim")
	rec := &recorder{}
	seed := seedPraxisOwnedForCleanup("attacker", "ai-tenant-victim")
	objs := append([]client.Object{mtc, victim}, seed...)
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).WithInterceptorFuncs(rec.funcs()).Build()

	r := &Reconciler{Client: fakeClient, APIReader: fakeClient, ManifestPath: manifestPath, Image: "img", ResyncInterval: time.Minute}
	_, err := r.Reconcile(context.Background(), mtcRequest("ai-tenant-attacker"))
	if err == nil {
		t.Fatal("Reconcile error = nil, want ownership mismatch error")
	}
	if !strings.Contains(err.Error(), "does not own MaasTenantConfig") {
		t.Fatalf("Reconcile error = %v, want ownership mismatch", err)
	}
	_, _, deleted := rec.snapshot()
	if len(deleted) != 0 {
		t.Fatalf("expected no cleanup in victim gateway, got deleted=%v", deleted)
	}
}

func assertContains(t *testing.T, haystack []string, want string) {
	t.Helper()
	for _, v := range haystack {
		if v == want {
			return
		}
	}
	t.Fatalf("expected %q among applied resource names, got %v", want, haystack)
}

func expectedCleanupNames(tenantID string) []string {
	return []string{
		PayloadProcessingDeploymentName(tenantID),
		PayloadPreProcessingDeploymentName(tenantID),
		PayloadProcessingServiceName(tenantID),
		PayloadPreProcessingServiceName(tenantID),
		PayloadProcessingPluginsConfigMapForTenant(tenantID),
		PayloadProcessingServiceAccountName(tenantID),
		PayloadProcessingNetworkPolicyName(tenantID),
		PayloadProcessingEnvoyFilterName(tenantID),
		PayloadProcessingReaderClusterRoleBindingNameForTenant(tenantID),
		PayloadProcessingHPAName(tenantID),
		PayloadPreProcessingHPAName(tenantID),
	}
}

func seedPraxisOwnedForCleanup(tenantID, namespace string) []client.Object {
	setManager := func(u *unstructured.Unstructured) client.Object {
		u.SetLabels(map[string]string{LabelManagedBy: ManagedByAIGatewayController})
		return u
	}
	mk := func(gvk schema.GroupVersionKind, name, ns string) client.Object {
		u := &unstructured.Unstructured{}
		u.SetGroupVersionKind(gvk)
		u.SetName(name)
		u.SetNamespace(ns)
		return setManager(u)
	}
	return []client.Object{
		mk(gvkDeployment, PayloadProcessingDeploymentName(tenantID), namespace),
		mk(gvkDeployment, PayloadPreProcessingDeploymentName(tenantID), namespace),
		mk(gvkService, PayloadProcessingServiceName(tenantID), namespace),
		mk(gvkService, PayloadPreProcessingServiceName(tenantID), namespace),
		mk(gvkConfigMap, PayloadProcessingPluginsConfigMapForTenant(tenantID), namespace),
		mk(gvkServiceAccount, PayloadProcessingServiceAccountName(tenantID), namespace),
		mk(gvkNetworkPolicy, PayloadProcessingNetworkPolicyName(tenantID), namespace),
		mk(gvkEnvoyFilter, PayloadProcessingEnvoyFilterName(tenantID), namespace),
		mk(gvkDestinationRule, PayloadProcessingServiceName(tenantID), namespace),
		mk(gvkDestinationRule, PayloadPreProcessingServiceName(tenantID), namespace),
		mk(gvkClusterRoleBinding, PayloadProcessingReaderClusterRoleBindingNameForTenant(tenantID), ""),
		mk(gvkHPA, PayloadProcessingHPAName(tenantID), namespace),
		mk(gvkHPA, PayloadPreProcessingHPAName(tenantID), namespace),
	}
}

func TestReconcileCleansUpAndRemovesFinalizerWhenSwitchedAwayFromPraxis(t *testing.T) {
	scheme := mtcSchemeForTests()
	// Explicit ipp: this tenant switched back to legacy IPP, but our
	// finalizer from when it was praxis is still present.
	mtc := withMTCFinalizer(newMTC("tenant-ns", "redteam", PayloadProcessingBackendIPP, "redteam", "ai-tenants"))
	aitenant := newAITenantOwner("redteam", "ai-tenants", AITenantPhaseActive, "my-gateway", "tenant-ns", "tenant-ns")
	rec := &recorder{}
	seed := seedPraxisOwnedForCleanup("redteam", "tenant-ns")
	objs := append([]client.Object{mtc, aitenant}, seed...)
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).WithInterceptorFuncs(rec.funcs()).Build()

	r := &Reconciler{Client: fakeClient, APIReader: fakeClient, ManifestPath: manifestPath, Image: "img", ResyncInterval: time.Minute}
	res, err := r.Reconcile(context.Background(), mtcRequest("tenant-ns"))
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if res.RequeueAfter != 0 {
		t.Fatalf("RequeueAfter = %v, want 0", res.RequeueAfter)
	}

	_, mtcPatches, deleted := rec.snapshot()
	if mtcPatches != 2 {
		t.Fatalf("mtcPatches = %d, want 2 (marking IPP migration cleanup complete, then removing the cleanup finalizer)", mtcPatches)
	}
	for _, want := range expectedCleanupNames("redteam") {
		assertContains(t, deleted, want)
	}

	var got unstructured.Unstructured
	got.SetGroupVersionKind(MaasTenantConfigGVK)
	if err := fakeClient.Get(context.Background(), client.ObjectKey{Namespace: "tenant-ns", Name: MaasTenantConfigInstanceName}, &got); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if controllerutil.ContainsFinalizer(&got, PraxisCleanupFinalizer) {
		t.Fatal("expected PraxisCleanupFinalizer to be removed after switch-away cleanup")
	}
	if v := got.GetAnnotations()[AnnotationPayloadProcessingStatus]; v != PayloadProcessingStatusCleanupComplete {
		t.Fatalf("status = %q, want %q so maas-controller may redeploy legacy IPP", v, PayloadProcessingStatusCleanupComplete)
	}
}

func TestReconcileCleanupSkipsMaaSOwnedResources(t *testing.T) {
	scheme := mtcSchemeForTests()
	mtc := withMTCFinalizer(newMTC("tenant-ns", "redteam", PayloadProcessingBackendIPP, "redteam", "ai-tenants"))
	aitenant := newAITenantOwner("redteam", "ai-tenants", AITenantPhaseActive, "my-gateway", "tenant-ns", "tenant-ns")
	legacyDeploy := &unstructured.Unstructured{}
	legacyDeploy.SetGroupVersionKind(gvkDeployment)
	legacyDeploy.SetName(PayloadProcessingDeploymentName("redteam"))
	legacyDeploy.SetNamespace("tenant-ns")
	legacyDeploy.SetLabels(map[string]string{LabelManagedBy: maasControllerFieldOwner})

	rec := &recorder{}
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(mtc, aitenant, legacyDeploy).WithInterceptorFuncs(rec.funcs()).Build()

	r := &Reconciler{Client: fakeClient, APIReader: fakeClient, ManifestPath: manifestPath, Image: "img", ResyncInterval: time.Minute}
	if _, err := r.Reconcile(context.Background(), mtcRequest("tenant-ns")); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	_, _, deleted := rec.snapshot()
	for _, name := range deleted {
		if name == PayloadProcessingDeploymentName("redteam") {
			t.Fatalf("should not delete maas-controller-owned %q, got deletes %v", name, deleted)
		}
	}
}

func TestReconcileSwitchAwayWithoutFinalizerIsNoop(t *testing.T) {
	scheme := mtcSchemeForTests()
	mtc := newMTC("tenant-ns", "redteam", PayloadProcessingBackendIPP, "redteam", "ai-tenants")
	aitenant := newAITenantOwner("redteam", "ai-tenants", AITenantPhaseActive, "my-gateway", "tenant-ns", "tenant-ns")
	rec := &recorder{}
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(mtc, aitenant).WithInterceptorFuncs(rec.funcs()).Build()

	r := &Reconciler{Client: fakeClient, APIReader: fakeClient, ManifestPath: manifestPath, Image: "img", ResyncInterval: time.Minute}
	if _, err := r.Reconcile(context.Background(), mtcRequest("tenant-ns")); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	_, mtcPatches, deleted := rec.snapshot()
	if mtcPatches != 0 || len(deleted) != 0 {
		t.Fatalf("expected no writes for a tenant that never had our finalizer, got mtcPatches=%d deleted=%v", mtcPatches, deleted)
	}
}

func TestReconcileDeleteCleansUpAndRemovesFinalizer(t *testing.T) {
	scheme := mtcSchemeForTests()
	mtc := withMTCFinalizer(newMTC("tenant-ns", "redteam", PayloadProcessingBackendPraxis, "redteam", "ai-tenants"))
	now := metav1.Now()
	mtc.SetDeletionTimestamp(&now)
	aitenant := newAITenantOwner("redteam", "ai-tenants", AITenantPhaseActive, "my-gateway", "tenant-ns", "tenant-ns")
	rec := &recorder{}
	seed := seedPraxisOwnedForCleanup("redteam", "tenant-ns")
	objs := append([]client.Object{mtc, aitenant}, seed...)
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).WithInterceptorFuncs(rec.funcs()).Build()

	r := &Reconciler{Client: fakeClient, APIReader: fakeClient, ManifestPath: manifestPath, Image: "img", ResyncInterval: time.Minute, DeletionTimeout: 10 * time.Minute}
	res, err := r.Reconcile(context.Background(), mtcRequest("tenant-ns"))
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if res.RequeueAfter != 0 {
		t.Fatalf("RequeueAfter = %v, want 0", res.RequeueAfter)
	}

	_, mtcPatches, deleted := rec.snapshot()
	if mtcPatches != 1 {
		t.Fatalf("mtcPatches = %d, want 1 (removing the cleanup finalizer)", mtcPatches)
	}
	for _, want := range expectedCleanupNames("redteam") {
		assertContains(t, deleted, want)
	}
}

// TestReconcileDeleteCleansUpWhenAITenantTerminating covers the live
// swaprace failure: AITenant is already Terminating (not Active) while MTC
// still carries PraxisCleanupFinalizer. Cleanup must use status.gatewayRef
// without waiting for Active, or deletion stalls until DeletionTimeout.
func TestReconcileDeleteCleansUpWhenAITenantTerminating(t *testing.T) {
	scheme := mtcSchemeForTests()
	mtc := withMTCFinalizer(newMTC("tenant-ns", "redteam", PayloadProcessingBackendPraxis, "redteam", "ai-tenants"))
	now := metav1.Now()
	mtc.SetDeletionTimestamp(&now)
	aitenant := newAITenantOwner("redteam", "ai-tenants", "Terminating", "my-gateway", "tenant-ns", "tenant-ns")
	rec := &recorder{}
	seed := seedPraxisOwnedForCleanup("redteam", "tenant-ns")
	objs := append([]client.Object{mtc, aitenant}, seed...)
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).WithInterceptorFuncs(rec.funcs()).Build()

	r := &Reconciler{Client: fakeClient, APIReader: fakeClient, ManifestPath: manifestPath, Image: "img", ResyncInterval: time.Minute, DeletionTimeout: 10 * time.Minute}
	res, err := r.Reconcile(context.Background(), mtcRequest("tenant-ns"))
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if res.RequeueAfter != 0 {
		t.Fatalf("RequeueAfter = %v, want 0 (Terminating owner with gatewayRef must still clean up)", res.RequeueAfter)
	}

	_, mtcPatches, deleted := rec.snapshot()
	if mtcPatches != 1 {
		t.Fatalf("mtcPatches = %d, want 1 (removing the cleanup finalizer)", mtcPatches)
	}
	for _, want := range expectedCleanupNames("redteam") {
		assertContains(t, deleted, want)
	}
}

func TestReconcileDeleteWithoutFinalizerIsNoop(t *testing.T) {
	scheme := mtcSchemeForTests()
	mtc := newMTC("tenant-ns", "redteam", PayloadProcessingBackendPraxis, "redteam", "ai-tenants")
	// A real object with a DeletionTimestamp but no finalizers at all
	// would already be gone; give it maas-controller's own (unrelated)
	// finalizer so it legitimately still exists without ours.
	mtc.SetFinalizers([]string{"maas.opendatahub.io/tenant-cleanup"})
	now := metav1.Now()
	mtc.SetDeletionTimestamp(&now)
	rec := &recorder{}
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(mtc).WithInterceptorFuncs(rec.funcs()).Build()

	r := &Reconciler{Client: fakeClient, APIReader: fakeClient, ManifestPath: manifestPath, Image: "img", ResyncInterval: time.Minute}
	if _, err := r.Reconcile(context.Background(), mtcRequest("tenant-ns")); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	_, mtcPatches, deleted := rec.snapshot()
	if mtcPatches != 0 || len(deleted) != 0 {
		t.Fatalf("expected no writes when our finalizer was never set, got mtcPatches=%d deleted=%v", mtcPatches, deleted)
	}
}

func TestReconcileDeleteRequeuesWhenGatewayRefNeverPopulated(t *testing.T) {
	scheme := mtcSchemeForTests()
	mtc := withMTCFinalizer(newMTC("tenant-ns", "redteam", PayloadProcessingBackendPraxis, "redteam", "ai-tenants"))
	now := metav1.Now()
	mtc.SetDeletionTimestamp(&now)
	aitenant := newAITenantOwner("redteam", "ai-tenants", AITenantPhaseActive, "", "", "tenant-ns") // Active, but gatewayRef never populated
	rec := &recorder{}
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(mtc, aitenant).WithInterceptorFuncs(rec.funcs()).Build()

	r := &Reconciler{Client: fakeClient, APIReader: fakeClient, ManifestPath: manifestPath, Image: "img", ResyncInterval: time.Minute}
	res, err := r.Reconcile(context.Background(), mtcRequest("tenant-ns"))
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if res.RequeueAfter != constants.NotReadyRequeueInterval {
		t.Fatalf("RequeueAfter = %v, want %v (keep finalizer until gatewayRef is known or DeletionTimeout)", res.RequeueAfter, constants.NotReadyRequeueInterval)
	}

	_, mtcPatches, deleted := rec.snapshot()
	if len(deleted) != 0 {
		t.Fatalf("expected no deletes when status.gatewayRef was never populated, got %v", deleted)
	}
	if mtcPatches != 0 {
		t.Fatalf("mtcPatches = %d, want 0 (must not remove the cleanup finalizer yet)", mtcPatches)
	}
	var got unstructured.Unstructured
	got.SetGroupVersionKind(MaasTenantConfigGVK)
	if err := fakeClient.Get(context.Background(), client.ObjectKey{Namespace: "tenant-ns", Name: MaasTenantConfigInstanceName}, &got); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !controllerutil.ContainsFinalizer(&got, PraxisCleanupFinalizer) {
		t.Fatal("expected PraxisCleanupFinalizer to remain while waiting for gatewayRef")
	}
}

func TestReconcileSwitchAwayRequeuesWhenAITenantNotReady(t *testing.T) {
	scheme := mtcSchemeForTests()
	mtc := withMTCFinalizer(newMTC("tenant-ns", "redteam", PayloadProcessingBackendIPP, "redteam", "ai-tenants"))
	aitenant := newAITenantOwner("redteam", "ai-tenants", "", "", "", "tenant-ns") // not Active
	rec := &recorder{}
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(mtc, aitenant).WithInterceptorFuncs(rec.funcs()).Build()

	r := &Reconciler{Client: fakeClient, APIReader: fakeClient, ManifestPath: manifestPath, Image: "img", ResyncInterval: time.Minute}
	res, err := r.Reconcile(context.Background(), mtcRequest("tenant-ns"))
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if res.RequeueAfter != constants.NotReadyRequeueInterval {
		t.Fatalf("RequeueAfter = %v, want %v", res.RequeueAfter, constants.NotReadyRequeueInterval)
	}
	_, mtcPatches, deleted := rec.snapshot()
	if mtcPatches != 0 || len(deleted) != 0 {
		t.Fatalf("expected no cleanup/finalizer drop while AITenant is not ready, got mtcPatches=%d deleted=%v", mtcPatches, deleted)
	}
}

func TestReconcileDeleteForceRemovesFinalizerAfterDeletionTimeout(t *testing.T) {
	scheme := mtcSchemeForTests()
	mtc := withMTCFinalizer(newMTC("tenant-ns", "redteam", PayloadProcessingBackendPraxis, "redteam", "ai-tenants"))
	past := metav1.NewTime(time.Now().Add(-time.Hour))
	mtc.SetDeletionTimestamp(&past)
	aitenant := newAITenantOwner("redteam", "ai-tenants", AITenantPhaseActive, "my-gateway", "tenant-ns", "tenant-ns")
	rec := &recorder{}
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(mtc, aitenant).WithInterceptorFuncs(rec.funcs()).Build()

	r := &Reconciler{Client: fakeClient, APIReader: fakeClient, ManifestPath: manifestPath, Image: "img", ResyncInterval: time.Minute, DeletionTimeout: 10 * time.Minute}
	if _, err := r.Reconcile(context.Background(), mtcRequest("tenant-ns")); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	_, mtcPatches, deleted := rec.snapshot()
	if len(deleted) != 0 {
		t.Fatalf("expected cleanup to be skipped once the deletion timeout is exceeded, got deletes %v", deleted)
	}
	if mtcPatches != 1 {
		t.Fatalf("mtcPatches = %d, want 1 (force-removing the cleanup finalizer)", mtcPatches)
	}
}

func TestEnqueueMaasTenantConfigForAITenant(t *testing.T) {
	r := &Reconciler{}

	t.Run("maps to the owning MaasTenantConfig via status.tenantNamespace", func(t *testing.T) {
		aitenant := newAITenantOwner("redteam", "ai-tenants", AITenantPhaseActive, "my-gateway", "tenant-ns", "tenant-ns")
		reqs := r.enqueueMaasTenantConfigForAITenant(context.Background(), aitenant)
		if len(reqs) != 1 {
			t.Fatalf("len(reqs) = %d, want 1", len(reqs))
		}
		if reqs[0].Namespace != "tenant-ns" || reqs[0].Name != MaasTenantConfigInstanceName {
			t.Fatalf("request = %v, want namespace=tenant-ns name=%s", reqs[0], MaasTenantConfigInstanceName)
		}
	})

	t.Run("no request when status.tenantNamespace is unset", func(t *testing.T) {
		aitenant := newAITenantOwner("redteam", "ai-tenants", "", "", "", "")
		reqs := r.enqueueMaasTenantConfigForAITenant(context.Background(), aitenant)
		if len(reqs) != 0 {
			t.Fatalf("len(reqs) = %d, want 0", len(reqs))
		}
	})
}
