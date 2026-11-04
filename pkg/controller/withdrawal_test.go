/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package controller

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	discv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	v1alpha1 "github.com/opendatahub-io/ai-gateway-controller/api/inference/v1alpha1"
	"github.com/opendatahub-io/ai-gateway-controller/pkg/envelope"
	"github.com/opendatahub-io/ai-gateway-controller/pkg/publisher"
	"github.com/opendatahub-io/ai-gateway-controller/pkg/render"
	"github.com/opendatahub-io/ai-gateway-controller/pkg/resolver"
	"github.com/opendatahub-io/ai-gateway-controller/pkg/tenant"
)

type deploymentReadErrorReader struct {
	client.Reader
}

func (r deploymentReadErrorReader) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if _, ok := obj.(*appsv1.Deployment); ok {
		return errors.New("injected Deployment read failure")
	}
	return r.Reader.Get(ctx, key, obj, opts...)
}

func TestPodServesOverlayRevisionParsesColoredExtProcLogs(t *testing.T) {
	wantRevision := strings.Repeat("a", 64)
	r := &Reconciler{readPodLogs: func(context.Context, string, string, string) (string, error) {
		return "\x1b[2maccepted_revision\x1b[0m=\x1b[0m\"" + wantRevision +
			"\" \x1b[2mserving_revision\x1b[0m=\x1b[0m\"" + wantRevision + "\"", nil
	}}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "tenant", Name: "extproc"}}

	servesExpected, err := r.podServesOverlayRevision(context.Background(), pod, wantRevision)
	if err != nil {
		t.Fatalf("read serving revision: %v", err)
	}
	if !servesExpected {
		t.Fatal("colored ExtProc log did not prove the accepted and serving overlay revisions")
	}
}

func TestStagedProviderWithdrawalPublishesBeforePruningAndSurvivesRetry(t *testing.T) {
	r, model := reconcilerFixture(t)
	r.KnownClusters = append(r.KnownClusters, "provider-provider-b")
	providerB := &v1alpha1.ExternalProvider{
		ObjectMeta: metav1.ObjectMeta{Name: "provider-b", Namespace: model.Namespace, UID: "provider-b-uid"},
		Spec: v1alpha1.ExternalProviderSpec{
			Provider: "openai", Endpoint: "provider-b.example.com",
			Auth: v1alpha1.AuthConfig{Type: "apikey", SecretRef: v1alpha1.NameReference{Name: "provider-b-credentials"}},
		},
		Status: v1alpha1.ExternalProviderStatus{Phase: resolver.PhaseReady},
	}
	secretB := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "provider-b-credentials", Namespace: model.Namespace}, Data: map[string][]byte{"api-key": []byte("fixture-only-secret")}}
	if err := r.Create(context.Background(), providerB); err != nil {
		t.Fatal(err)
	}
	if err := r.Create(context.Background(), secretB); err != nil {
		t.Fatal(err)
	}
	if _, err := r.ReconcileProvider(context.Background(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(providerB)}); err != nil {
		t.Fatalf("reconcile provider B: %v", err)
	}
	model = getModel(t, r, model)
	model.Spec.ExternalProviderRefs = append(model.Spec.ExternalProviderRefs, v1alpha1.ExternalProviderRef{
		Ref: v1alpha1.ExternalProviderReference{Name: providerB.Name}, TargetModel: "gpt-b", APIFormat: "openai-chat", Path: "/v1/chat/completions",
	})
	if err := r.Update(context.Background(), model); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(model)}); err != nil {
		t.Fatalf("publish A+B: %v", err)
	}
	if got := len(overlayEnvelope(t, r, model.Namespace).Overlay.Candidates); got != 2 {
		t.Fatalf("A+B candidates = %d, want 2", got)
	}

	model = getModel(t, r, model)
	model.Spec.ExternalProviderRefs = model.Spec.ExternalProviderRefs[:1]
	if err := r.Update(context.Background(), model); err != nil {
		t.Fatal(err)
	}
	var events []string
	r.ApplyResource = func(ctx context.Context, c client.Client, resource unstructured.Unstructured) error {
		if resource.GetKind() == "HTTPRoute" {
			events = append(events, "apply-route")
		}
		return render.Apply(ctx, c, []unstructured.Unstructured{resource})
	}
	pub, err := publisher.New(r.Client, publisher.Config{Namespace: model.Namespace, Name: r.ConfigMap})
	if err != nil {
		t.Fatal(err)
	}
	r.PublishOverlay = func(ctx context.Context, set *resolver.ResolvedRouteSet, scope envelope.Scope, opts envelope.Options) (publisher.Result, error) {
		events = append(events, "publish-overlay")
		route := newHTTPRoute()
		if err := r.Get(ctx, client.ObjectKey{Namespace: model.Namespace, Name: modelRouteName(model.Name)}, route); err != nil {
			t.Fatalf("withdrawn provider route must exist before the reduced overlay is published: %v", err)
		}
		return pub.Publish(ctx, set, scope, opts)
	}
	result, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(model)})
	if err != nil {
		t.Fatalf("publish A-only while retaining B: %v", err)
	}
	if result.RequeueAfter <= 0 {
		t.Fatalf("withdrawal requeue = %s, want a pending drain", result.RequeueAfter)
	}
	if len(events) < 2 || events[len(events)-1] != "publish-overlay" {
		t.Fatalf("apply/publish order = %#v", events)
	}
	if got := len(overlayEnvelope(t, r, model.Namespace).Overlay.Candidates); got != 1 {
		t.Fatalf("published A-only candidates = %d, want 1", got)
	}
	assertRouteProviders(t, r, model.Namespace, model.Name, "provider", "provider-b")

	// A complete replacement config is not sufficient while the previous pod
	// or an EndpointSlice can still serve. The old route remains available.
	seedSafeExternalModelRuntime(t, r, "tenant", model.Namespace, []*v1alpha1.ExternalModel{model})
	validPodLogs := r.readPodLogs
	staleRevision := strings.Repeat("0", 64)
	currentRevision := overlayEnvelope(t, r, model.Namespace).Revision.Value
	if staleRevision == currentRevision {
		staleRevision = strings.Repeat("1", 64)
	}
	r.readPodLogs = func(context.Context, string, string, string) (string, error) {
		return `accepted_revision="` + staleRevision + `" serving_revision="` + staleRevision + `"`, nil
	}
	result, err = r.Reconcile(context.Background(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(model)})
	if err != nil || result.RequeueAfter != withdrawalRequeueInterval {
		t.Fatalf("stale serving overlay should keep withdrawal pending: result=%#v err=%v", result, err)
	}
	assertRouteProviders(t, r, model.Namespace, model.Name, "provider", "provider-b")
	var staleDrainOverlay corev1.ConfigMap
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: model.Namespace, Name: r.configMapName()}, &staleDrainOverlay); err != nil {
		t.Fatal(err)
	}
	if staleDrainOverlay.Annotations[withdrawalDrainAnnotation] != "" {
		t.Fatalf("stale pod serving revision started the drain timer: %q", staleDrainOverlay.Annotations[withdrawalDrainAnnotation])
	}
	r.readPodLogs = validPodLogs
	result, err = r.Reconcile(context.Background(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(model)})
	if err != nil {
		t.Fatalf("record drain start: %v", err)
	}
	if result.RequeueAfter != withdrawalDrainDuration {
		t.Fatalf("initial drain requeue = %s, want %s", result.RequeueAfter, withdrawalDrainDuration)
	}
	var overlay corev1.ConfigMap
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: model.Namespace, Name: "routing-overlay"}, &overlay); err != nil {
		t.Fatal(err)
	}
	var state withdrawalDrainState
	if err := json.Unmarshal([]byte(overlay.Annotations[withdrawalDrainAnnotation]), &state); err != nil || state.Revision == "" {
		t.Fatalf("persisted drain state = %#v, err=%v", state, err)
	}
	state.StartedAt = time.Now().Add(-withdrawalDrainDuration - time.Second)
	encoded, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	overlay.Annotations[withdrawalDrainAnnotation] = string(encoded)
	if err := r.Update(context.Background(), &overlay); err != nil {
		t.Fatal(err)
	}

	// A fresh reconciler instance resumes from the ConfigMap state and only
	// prunes B after the rollout and request-drain fence both pass.
	r2 := &Reconciler{Client: r.Client, APIReader: r.APIReader, Namespace: r.Namespace, GatewayName: r.GatewayName,
		GatewayNamespace: r.GatewayNamespace, Network: r.Network, KnownClusters: r.KnownClusters, readPodLogs: validPodLogs}
	result, err = r2.Reconcile(context.Background(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(model)})
	if err != nil {
		t.Fatalf("resume withdrawal after restart: %v", err)
	}
	if result.RequeueAfter != 0 {
		t.Fatalf("completed withdrawal requeue = %s, want none", result.RequeueAfter)
	}
	assertRouteProviders(t, r2, model.Namespace, model.Name, "provider")
	if err := r2.Get(context.Background(), client.ObjectKey{Namespace: model.Namespace, Name: "routing-overlay"}, &overlay); err != nil {
		t.Fatal(err)
	}
	if overlay.Annotations[withdrawalDrainAnnotation] != "" {
		t.Fatalf("completed withdrawal retained drain marker %q", overlay.Annotations[withdrawalDrainAnnotation])
	}

	// Provider transport remains owned by the still-present ExternalProvider;
	// deleting a model binding must not delete a provider shared with other
	// models or independently reconciled by the provider loop.
	service := &corev1.Service{}
	if err := r2.Get(context.Background(), client.ObjectKey{Namespace: model.Namespace, Name: "provider-provider-b"}, service); err != nil {
		t.Fatalf("provider B Service was removed with one model binding: %v", err)
	}
}

func TestWithdrawalKeepsSharedProviderForAnotherModel(t *testing.T) {
	ctx := context.Background()
	r, first := reconcilerFixture(t)
	providerB := createWithdrawalProvider(t, r, first.Namespace, "provider-b")
	second := first.DeepCopy()
	second.Name = "second-model"
	second.UID = types.UID("second-model-uid")
	second.ResourceVersion = ""
	second.ManagedFields = nil
	second.Finalizers = nil
	second.Status = v1alpha1.ExternalModelStatus{}
	second.Spec.ModelName = "second-client-model"
	second.Spec.ExternalProviderRefs = []v1alpha1.ExternalProviderRef{withdrawalProviderRef(providerB.Name, "gpt-b")}
	if err := r.Create(ctx, second); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(first)}); err != nil {
		t.Fatalf("publish the two models: %v", err)
	}

	first = getModel(t, r, first)
	first.Spec.ExternalProviderRefs = append(first.Spec.ExternalProviderRefs, withdrawalProviderRef(providerB.Name, "gpt-b"))
	if err := r.Update(ctx, first); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(first)}); err != nil {
		t.Fatalf("publish shared provider on both models: %v", err)
	}
	first = getModel(t, r, first)
	first.Spec.ExternalProviderRefs = first.Spec.ExternalProviderRefs[:1]
	if err := r.Update(ctx, first); err != nil {
		t.Fatal(err)
	}
	result, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(first)})
	if err != nil || result.RequeueAfter <= 0 {
		t.Fatalf("withdraw provider B from first model: result=%#v err=%v", result, err)
	}
	assertRouteProviders(t, r, first.Namespace, first.Name, "provider", "provider-b")
	assertRouteProviders(t, r, second.Namespace, second.Name, "provider-b")
	if got := len(overlayEnvelope(t, r, first.Namespace).Overlay.Candidates); got != 2 {
		t.Fatalf("published candidates = %d, want first/A and second/B", got)
	}

	first = getModel(t, r, first)
	second = getModel(t, r, second)
	seedSafeExternalModelRuntime(t, r, "tenant", first.Namespace, []*v1alpha1.ExternalModel{first, second})
	result, err = r.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(first)})
	if err != nil || result.RequeueAfter != withdrawalDrainDuration {
		t.Fatalf("start shared-provider drain: result=%#v err=%v", result, err)
	}
	markWithdrawalDrainElapsed(t, r, first.Namespace)
	if _, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(first)}); err != nil {
		t.Fatalf("finish first model withdrawal: %v", err)
	}
	assertRouteProviders(t, r, first.Namespace, first.Name, "provider")
	assertRouteProviders(t, r, second.Namespace, second.Name, "provider-b")
	if _, err := getUnstructured(r, schema.GroupVersionKind{Version: "v1", Kind: "Service"}, first.Namespace,
		providerServicePrefix+providerB.Name); err != nil {
		t.Fatalf("provider B transport was removed while the second model still uses it: %v", err)
	}
}

func TestWithdrawalPreservesServedAttachment(t *testing.T) {
	ctx := context.Background()
	r, home := reconcilerFixture(t)
	r.Namespace = "" // Exercise a second namespace on the shared controller.
	r.KnownClusters = append(r.KnownClusters, "provider-openai")
	providerB := createWithdrawalProvider(t, r, home.Namespace, "provider-b")
	remoteProvider, remoteSecret := attachmentProvider("team")
	remote := attachmentModel("remote", "team", "remote-model", time.Unix(100, 0),
		v1alpha1.NamespacedObjectReference{Name: "gateway", Namespace: home.Namespace})
	for _, object := range []client.Object{remoteProvider, remoteSecret, remote} {
		if err := r.Create(ctx, object); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := r.ReconcileProvider(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(remoteProvider)}); err != nil {
		t.Fatalf("reconcile attached provider: %v", err)
	}
	reconcileModel(t, r, home)
	reconcileModel(t, r, remote)
	setRouteParentStatus(t, r, remote, remote.Spec.GatewayRefs[0], metav1.ConditionTrue, "Accepted")
	reconcileModel(t, r, remote)

	home = getModel(t, r, home)
	home.Spec.ExternalProviderRefs = append(home.Spec.ExternalProviderRefs, withdrawalProviderRef(providerB.Name, "gpt-b"))
	if err := r.Update(ctx, home); err != nil {
		t.Fatal(err)
	}
	reconcileModel(t, r, home)
	home = getModel(t, r, home)
	home.Spec.ExternalProviderRefs = home.Spec.ExternalProviderRefs[:1]
	if err := r.Update(ctx, home); err != nil {
		t.Fatal(err)
	}
	result, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(home)})
	if err != nil || result.RequeueAfter <= 0 {
		t.Fatalf("stage home provider withdrawal with attachment: result=%#v err=%v", result, err)
	}
	assertRouteProviders(t, r, home.Namespace, home.Name, "provider", "provider-b")
	wantAttachedID := envelope.ScopedStableID(remote.Namespace, remote.Name, remote.Namespace, remoteProvider.Name)
	wantIDs := map[string]bool{envelope.CandidateStableID(home.Name, "provider"): true, wantAttachedID: true}
	for _, candidate := range overlayEnvelope(t, r, home.Namespace).Overlay.Candidates {
		delete(wantIDs, candidate.StableID)
	}
	if len(wantIDs) != 0 {
		t.Fatalf("published overlay lost candidates: %#v", wantIDs)
	}
	if _, err := getUnstructured(r, httpRouteGVK, remote.Namespace, attachmentRouteName(remote, remote.Spec.GatewayRefs[0])); err != nil {
		t.Fatalf("served attachment route was removed: %v", err)
	}
	home = getModel(t, r, home)
	seedSafeExternalModelRuntime(t, r, "tenant", home.Namespace, []*v1alpha1.ExternalModel{home})
	result, err = r.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(home)})
	if err != nil || result.RequeueAfter != withdrawalDrainDuration {
		t.Fatalf("start withdrawal drain with attachment: result=%#v err=%v", result, err)
	}
	markWithdrawalDrainElapsed(t, r, home.Namespace)
	result, err = r.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(home)})
	if err != nil || result.RequeueAfter != 0 {
		t.Fatalf("finish withdrawal with attachment: result=%#v err=%v", result, err)
	}
	assertRouteProviders(t, r, home.Namespace, home.Name, "provider")
	if _, err := getUnstructured(r, httpRouteGVK, remote.Namespace, attachmentRouteName(remote, remote.Spec.GatewayRefs[0])); err != nil {
		t.Fatalf("served attachment route was removed after drain: %v", err)
	}
	if len(routeFilterNames(t, r, "tenant", home.Namespace)) == 0 {
		t.Fatal("served attachment lost its gateway route filter after drain")
	}
}

func TestRuntimeConfigHashPreservesScopedAnthropicHeaderWithNoHomeRoutes(t *testing.T) {
	ctx := context.Background()
	r, home := reconcilerFixture(t)
	r.Namespace = ""
	r.KnownClusters = append(r.KnownClusters, "provider-anthropic")
	setControllerTranslationEnabled(t, r, home.Namespace, true)
	provider := &v1alpha1.ExternalProvider{
		ObjectMeta: metav1.ObjectMeta{Name: "anthropic", Namespace: "team", UID: "anthropic-provider-uid"},
		Spec: v1alpha1.ExternalProviderSpec{Provider: "anthropic", Endpoint: "api.anthropic.com",
			Auth: v1alpha1.AuthConfig{Type: "apikey", SecretRef: v1alpha1.NameReference{Name: "anthropic-key"}}},
	}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "anthropic-key", Namespace: "team"}, Data: map[string][]byte{"api-key": []byte("fixture-only-anthropic-key")}}
	remote := attachmentModel("messages", "team", "claude", time.Unix(100, 0), v1alpha1.NamespacedObjectReference{Name: "gateway", Namespace: home.Namespace})
	remote.Spec.ExternalProviderRefs = []v1alpha1.ExternalProviderRef{{
		Ref: v1alpha1.ExternalProviderReference{Name: provider.Name}, TargetModel: "claude-3-5-sonnet", APIFormat: "messages", Path: "/v1/messages",
	}}
	for _, object := range []client.Object{provider, secret, remote} {
		if err := r.Create(ctx, object); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := r.ReconcileProvider(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(provider)}); err != nil {
		t.Fatalf("reconcile attached Anthropic provider: %v", err)
	}
	reconcileModel(t, r, home)
	reconcileModel(t, r, remote)
	gateway := v1alpha1.NamespacedObjectReference{Name: "gateway", Namespace: home.Namespace}
	setRouteParentStatus(t, r, remote, gateway, metav1.ConditionTrue, "Accepted")
	reconcileModel(t, r, remote)

	var copySecret corev1.Secret
	copyName := credentialCopyName(home.Namespace, "team", "anthropic-key", "api-key", "x-api-key")
	if err := r.Get(ctx, client.ObjectKey{Namespace: home.Namespace, Name: copyName}, &copySecret); err != nil {
		t.Fatalf("attached Anthropic credential copy: %v", err)
	}
	if copySecret.Labels[tenant.LabelCredentialCopy] != "true" || copySecret.Annotations[tenant.AnnotationCredentialHeader] != "x-api-key" {
		t.Fatalf("credential copy metadata = labels %#v annotations %#v", copySecret.Labels, copySecret.Annotations)
	}

	var scoped envelope.Candidate
	for _, candidate := range overlayEnvelope(t, r, home.Namespace).Overlay.Candidates {
		if candidate.Cluster == "provider-anthropic" {
			scoped = cloneCandidate(candidate)
		}
	}
	if scoped.StableID == "" || scoped.APIFormat != "messages" || scoped.Path != "/v1/messages" || scoped.Credential == nil {
		t.Fatalf("published scoped Anthropic candidate = %#v", scoped)
	}

	// This case intentionally hashes an attachment-only runtime.
	if route, err := getUnstructured(r, httpRouteGVK, home.Namespace, modelRouteName(home.Name)); err != nil {
		t.Fatal(err)
	} else if err := r.Delete(ctx, route); err != nil {
		t.Fatal(err)
	}
	tracker := &secretMetadataReadTracker{Reader: r.APIReader}
	r.APIReader = tracker
	// With no home candidates, ErrNoRoutes is a valid empty local set. The
	// still-serving scoped Anthropic attachment remains in the composed runtime.
	gotHash, err := runtimeConfigHash(ctx, r, nil, home.Namespace, true)
	if err != nil {
		t.Fatalf("hash scoped-only runtime: %v", err)
	}
	complete := cloneCandidate(scoped)
	complete.Credential.Header = "x-api-key"
	wantHash, err := tenant.ExternalModelExtProcConfigHash(home.Namespace, []envelope.Candidate{complete}, true)
	if err != nil {
		t.Fatal(err)
	}
	withoutHeader, err := tenant.ExternalModelExtProcConfigHash(home.Namespace, []envelope.Candidate{scoped}, true)
	if err != nil {
		t.Fatal(err)
	}
	if gotHash != wantHash || gotHash == withoutHeader {
		t.Fatalf("runtime hash = %s, complete x-api-key hash = %s, missing-header hash = %s", gotHash, wantHash, withoutHeader)
	}
	if tracker.metadataReads != 1 || tracker.typedSecretReads != 0 {
		t.Fatalf("APIReader Secret reads: metadata=%d typed=%d, want metadata-only access", tracker.metadataReads, tracker.typedSecretReads)
	}
}

type secretMetadataReadTracker struct {
	client.Reader
	metadataReads    int
	typedSecretReads int
}

func (r *secretMetadataReadTracker) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	switch obj.(type) {
	case *metav1.PartialObjectMetadata:
		if obj.GetObjectKind().GroupVersionKind().Kind == "Secret" {
			r.metadataReads++
		}
	case *corev1.Secret:
		r.typedSecretReads++
	}
	return r.Reader.Get(ctx, key, obj, opts...)
}

func TestRapidProviderWithdrawalAndReadditionKeepsRouteReachable(t *testing.T) {
	ctx := context.Background()
	r, model := reconcilerFixture(t)
	providerB := createWithdrawalProvider(t, r, model.Namespace, "provider-b")
	model = getModel(t, r, model)
	model.Spec.ExternalProviderRefs = append(model.Spec.ExternalProviderRefs, withdrawalProviderRef(providerB.Name, "gpt-b"))
	if err := r.Update(ctx, model); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(model)}); err != nil {
		t.Fatalf("publish A+B: %v", err)
	}

	model = getModel(t, r, model)
	model.Spec.ExternalProviderRefs = model.Spec.ExternalProviderRefs[:1]
	if err := r.Update(ctx, model); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(model)}); err != nil {
		t.Fatalf("publish A-only: %v", err)
	}
	assertRouteProviders(t, r, model.Namespace, model.Name, "provider", "provider-b")
	if got := len(overlayEnvelope(t, r, model.Namespace).Overlay.Candidates); got != 1 {
		t.Fatalf("withdrawal overlay candidates = %d, want A-only", got)
	}

	model = getModel(t, r, model)
	seedSafeExternalModelRuntime(t, r, "tenant", model.Namespace, []*v1alpha1.ExternalModel{model})
	result, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(model)})
	if err != nil || result.RequeueAfter != withdrawalDrainDuration {
		t.Fatalf("start A-only drain: result=%#v err=%v", result, err)
	}
	var overlay corev1.ConfigMap
	if err := r.Get(ctx, client.ObjectKey{Namespace: model.Namespace, Name: r.configMapName()}, &overlay); err != nil {
		t.Fatal(err)
	}
	if overlay.Annotations[withdrawalDrainAnnotation] == "" {
		t.Fatal("safe A-only rollout did not persist its drain state")
	}

	// The provider returns before the drain expires. The reconciler must publish
	// A+B again and cancel the obsolete cleanup timer without dropping B's route.
	model = getModel(t, r, model)
	model.Spec.ExternalProviderRefs = append(model.Spec.ExternalProviderRefs, withdrawalProviderRef(providerB.Name, "gpt-b"))
	if err := r.Update(ctx, model); err != nil {
		t.Fatal(err)
	}
	result, err = r.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(model)})
	if err != nil || result.RequeueAfter != 0 {
		t.Fatalf("restore A+B during drain: result=%#v err=%v", result, err)
	}
	assertRouteProviders(t, r, model.Namespace, model.Name, "provider", "provider-b")
	if got := len(overlayEnvelope(t, r, model.Namespace).Overlay.Candidates); got != 2 {
		t.Fatalf("restored overlay candidates = %d, want A+B", got)
	}
	if err := r.Get(ctx, client.ObjectKey{Namespace: model.Namespace, Name: r.configMapName()}, &overlay); err != nil {
		t.Fatal(err)
	}
	if overlay.Annotations[withdrawalDrainAnnotation] != "" {
		t.Fatalf("restored A+B retained stale drain state %q", overlay.Annotations[withdrawalDrainAnnotation])
	}
}

func TestDeletingModelStagesItsProviderWithdrawalUntilSiblingRuntimeDrains(t *testing.T) {
	ctx := context.Background()
	r, deleting := reconcilerFixture(t)
	providerB := createWithdrawalProvider(t, r, deleting.Namespace, "provider-b")
	deleting = getModel(t, r, deleting)
	deleting.Spec.ExternalProviderRefs = append(deleting.Spec.ExternalProviderRefs, withdrawalProviderRef(providerB.Name, "gpt-b"))
	if err := r.Update(ctx, deleting); err != nil {
		t.Fatal(err)
	}
	sibling := deleting.DeepCopy()
	sibling.Name = "sibling-model"
	sibling.UID = types.UID("sibling-model-uid")
	sibling.ResourceVersion = ""
	sibling.ManagedFields = nil
	sibling.Finalizers = nil
	sibling.Status = v1alpha1.ExternalModelStatus{}
	sibling.Spec.ModelName = "sibling-client-model"
	sibling.Spec.ExternalProviderRefs = sibling.Spec.ExternalProviderRefs[:1]
	if err := r.Create(ctx, sibling); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(deleting)}); err != nil {
		t.Fatalf("publish model and sibling: %v", err)
	}
	if err := r.Delete(ctx, deleting); err != nil {
		t.Fatal(err)
	}
	result, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(deleting)})
	if err != nil || result.RequeueAfter <= 0 {
		t.Fatalf("stage deleting model withdrawal: result=%#v err=%v", result, err)
	}
	assertRouteProviders(t, r, deleting.Namespace, deleting.Name, "provider", "provider-b")
	assertRouteProviders(t, r, sibling.Namespace, sibling.Name, "provider")
	if got := len(overlayEnvelope(t, r, deleting.Namespace).Overlay.Candidates); got != 1 {
		t.Fatalf("deletion overlay candidates = %d, want only sibling A", got)
	}

	sibling = getModel(t, r, sibling)
	seedSafeExternalModelRuntime(t, r, "tenant", deleting.Namespace, []*v1alpha1.ExternalModel{sibling})
	result, err = r.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(deleting)})
	if err != nil || result.RequeueAfter != withdrawalDrainDuration {
		t.Fatalf("start deleted model drain: result=%#v err=%v", result, err)
	}
	var overlayBeforeSibling corev1.ConfigMap
	if err := r.Get(ctx, client.ObjectKey{Namespace: deleting.Namespace, Name: "routing-overlay"}, &overlayBeforeSibling); err != nil {
		t.Fatal(err)
	}
	drainBeforeSibling := overlayBeforeSibling.Annotations[withdrawalDrainAnnotation]
	if drainBeforeSibling == "" {
		t.Fatal("deleting model did not persist the deliberate drain state")
	}
	// The sink Service has owner-reference updates which can enqueue sibling
	// reconciles. A sibling pass must not recompose the terminating model or
	// restart the drain timer.
	if _, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(sibling)}); err != nil {
		t.Fatalf("reconcile sibling while deleted model drains: %v", err)
	}
	if got := overlayEnvelope(t, r, deleting.Namespace).Overlay.Candidates; len(got) != 1 || got[0].Name != "sibling-client-model" {
		t.Fatalf("sibling reconcile republished terminating model: %#v", got)
	}
	var overlayAfterSibling corev1.ConfigMap
	if err := r.Get(ctx, client.ObjectKey{Namespace: deleting.Namespace, Name: "routing-overlay"}, &overlayAfterSibling); err != nil {
		t.Fatal(err)
	}
	if got := overlayAfterSibling.Annotations[withdrawalDrainAnnotation]; got != drainBeforeSibling {
		t.Fatalf("sibling reconcile changed drain state from %q to %q", drainBeforeSibling, got)
	}
	markWithdrawalDrainElapsed(t, r, deleting.Namespace)
	if _, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(deleting)}); err != nil {
		t.Fatalf("finish deleted model withdrawal: %v", err)
	}
	if _, err := getUnstructured(r, httpRouteGVK, deleting.Namespace, modelRouteName(deleting.Name)); !apierrors.IsNotFound(err) {
		t.Fatalf("deleted model route error = %v, want NotFound", err)
	}
	assertRouteProviders(t, r, sibling.Namespace, sibling.Name, "provider")
}

func TestDeletingOnlyAnthropicProviderRetainsMessagesRouteContractUntilDrain(t *testing.T) {
	ctx := context.Background()
	r, model := reconcilerFixture(t)
	provider := createWithdrawalProvider(t, r, model.Namespace, "anthropic")
	if err := r.Get(ctx, client.ObjectKeyFromObject(provider), provider); err != nil {
		t.Fatal(err)
	}
	provider.Spec.Provider = "anthropic"
	provider.Spec.Endpoint = "api.anthropic.com"
	if err := r.Update(ctx, provider); err != nil {
		t.Fatal(err)
	}
	if _, err := r.ReconcileProvider(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(provider)}); err != nil {
		t.Fatalf("reconcile Anthropic provider: %v", err)
	}
	r.KnownClusters = append(r.KnownClusters, "provider-anthropic")
	setControllerTranslationEnabled(t, r, model.Namespace, true)
	model.Spec.ModelName = "claude-client"
	model.Spec.ExternalProviderRefs = []v1alpha1.ExternalProviderRef{{
		Ref: v1alpha1.ExternalProviderReference{Name: provider.Name}, TargetModel: "claude-3-5-sonnet", APIFormat: "messages", Path: "/v1/messages",
	}}
	sibling := model.DeepCopy()
	sibling.Name = "sibling"
	sibling.UID = "sibling-model-uid"
	sibling.ResourceVersion = ""
	sibling.ManagedFields = nil
	sibling.Finalizers = nil
	sibling.Status = v1alpha1.ExternalModelStatus{}
	sibling.Spec.ModelName = "sibling-client"
	sibling.Spec.ExternalProviderRefs = []v1alpha1.ExternalProviderRef{withdrawalProviderRef("provider", "gpt")}
	if err := r.Update(ctx, model); err != nil {
		t.Fatal(err)
	}
	if err := r.Create(ctx, sibling); err != nil {
		t.Fatal(err)
	}
	request := reconcile.Request{NamespacedName: client.ObjectKeyFromObject(model)}
	if _, err := r.Reconcile(ctx, request); err != nil {
		t.Fatalf("publish Anthropic model and sibling: %v", err)
	}
	if _, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(sibling)}); err != nil {
		t.Fatalf("publish sibling model: %v", err)
	}
	prior := overlayEnvelope(t, r, model.Namespace).Overlay.Candidates
	if len(prior) != 2 || prior[0].APIFormat != "messages" && prior[1].APIFormat != "messages" {
		t.Fatalf("pre-delete overlay did not contain translated Anthropic candidate: %#v", prior)
	}
	if err := r.Delete(ctx, model); err != nil {
		t.Fatal(err)
	}
	result, err := r.Reconcile(ctx, request)
	if err != nil || result.RequeueAfter <= 0 {
		t.Fatalf("stage final Anthropic model withdrawal: result=%#v err=%v", result, err)
	}
	assertRouteProviders(t, r, model.Namespace, model.Name, "anthropic")
	if got := len(overlayEnvelope(t, r, model.Namespace).Overlay.Candidates); got != 1 {
		t.Fatalf("reduced overlay has %d candidates, want only sibling", got)
	}
	stableID := envelope.CandidateStableID(model.Name, "anthropic")
	route, err := getUnstructured(r, httpRouteGVK, model.Namespace, modelRouteName(model.Name))
	if err != nil {
		t.Fatal(err)
	}
	routeRules, _, err := unstructured.NestedSlice(route.Object, "spec", "rules")
	if err != nil {
		t.Fatal(err)
	}
	if got := routeClientName(routeRules, model.Namespace, model.Name); got != "claude-client" {
		t.Fatalf("retained route client model = %q", got)
	}
	assertExactSelectedPathRule(t, route, "/v1/messages", stableID)
	filter, err := getUnstructured(r, schema.GroupVersionKind{Group: "networking.istio.io", Version: "v1alpha3", Kind: "EnvoyFilter"},
		model.Namespace, tenant.PayloadProcessingExternalModelEnvoyFilterName("tenant"))
	if err != nil {
		t.Fatal(err)
	}
	expectedRoutes, responseModes := map[string]bool{}, map[string]string{}
	for i := 0; i < 4; i++ {
		name := fmt.Sprintf("tenant-a.%s.%d", modelRouteName(model.Name), i)
		expectedRoutes[name], responseModes[name] = false, "STREAMED"
	}
	for i := 0; i < 4; i++ {
		name := fmt.Sprintf("tenant-a.%s.%d", modelRouteName(sibling.Name), i)
		expectedRoutes[name], responseModes[name] = false, "NONE"
	}
	assertExternalModelRoutePatches(t, nestedSlice(t, filter.Object, "spec", "configPatches"), expectedRoutes, responseModes, 8)
	if err := r.Get(ctx, client.ObjectKey{Namespace: model.Namespace, Name: "provider-anthropic"}, &corev1.Service{}); err != nil {
		t.Fatalf("Anthropic provider transport removed before drain: %v", err)
	}

	sibling = getModel(t, r, sibling)
	seedSafeExternalModelRuntime(t, r, "tenant", model.Namespace, []*v1alpha1.ExternalModel{sibling}, true)
	result, err = r.Reconcile(ctx, request)
	if err != nil || result.RequeueAfter != withdrawalDrainDuration {
		t.Fatalf("start Anthropic request drain: result=%#v err=%v", result, err)
	}
	markWithdrawalDrainElapsed(t, r, model.Namespace)
	result, err = r.Reconcile(ctx, request)
	if err != nil || result.RequeueAfter != 0 {
		t.Fatalf("finish Anthropic withdrawal: result=%#v err=%v", result, err)
	}
	if _, err := getUnstructured(r, httpRouteGVK, model.Namespace, modelRouteName(model.Name)); !apierrors.IsNotFound(err) {
		t.Fatalf("Anthropic route remains after drain: %v", err)
	}
	filter, err = getUnstructured(r, schema.GroupVersionKind{Group: "networking.istio.io", Version: "v1alpha3", Kind: "EnvoyFilter"},
		model.Namespace, tenant.PayloadProcessingExternalModelEnvoyFilterName("tenant"))
	if err != nil {
		t.Fatal(err)
	}
	expectedRoutes, responseModes = map[string]bool{}, map[string]string{}
	for i := 0; i < 4; i++ {
		name := fmt.Sprintf("tenant-a.%s.%d", modelRouteName(sibling.Name), i)
		expectedRoutes[name], responseModes[name] = false, "NONE"
	}
	assertExternalModelRoutePatches(t, nestedSlice(t, filter.Object, "spec", "configPatches"), expectedRoutes, responseModes, 4)
}

func TestGatewayRebindStagesProviderWithdrawalUntilHomeRuntimeDrains(t *testing.T) {
	ctx := context.Background()
	gatewayA := v1alpha1.NamespacedObjectReference{Name: "gw-a", Namespace: "gateway-a"}
	providerA, secretA := attachmentProvider("tenant-a")
	providerB := &v1alpha1.ExternalProvider{
		ObjectMeta: metav1.ObjectMeta{Name: "provider-b", Namespace: "tenant-a", UID: "provider-b-uid"},
		Spec: v1alpha1.ExternalProviderSpec{Provider: "openai", Endpoint: "provider-b.example.com",
			Auth: v1alpha1.AuthConfig{Type: "apikey", SecretRef: v1alpha1.NameReference{Name: "provider-b-credentials"}}},
	}
	secretB := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "provider-b-credentials", Namespace: "tenant-a"}, Data: map[string][]byte{"api-key": []byte("fixture-only-secret")}}
	model := attachmentModel("chat", "tenant-a", "chat-model", time.Unix(100, 0))
	sibling := attachmentModel("other", "tenant-a", "other-model", time.Unix(100, 0))
	objects := []client.Object{providerA, secretA, providerB, secretB, model, sibling}
	objects = append(objects, attachmentTenantObjects("tenant-a", "tenant-a", gatewayA, "praxis")...)
	objects = append(objects, attachmentTenantObjects("tenant-b", "tenant-b", gatewayB, "praxis")...)
	r := attachmentReconciler(t, objects...)
	r.KnownClusters = []string{providerServicePrefix + providerA.Name, providerServicePrefix + providerB.Name}
	reconcileModel(t, r, model)
	reconcileModel(t, r, sibling)

	model = getModel(t, r, model)
	model.Spec.ExternalProviderRefs = append(model.Spec.ExternalProviderRefs, withdrawalProviderRef(providerB.Name, "gpt-b"))
	if err := r.Update(ctx, model); err != nil {
		t.Fatal(err)
	}
	reconcileModel(t, r, model)
	assertRouteProviders(t, r, model.Namespace, model.Name, providerA.Name, providerB.Name)

	model = getModel(t, r, model)
	model.Spec.GatewayRefs = []v1alpha1.NamespacedObjectReference{gatewayB}
	if err := r.Update(ctx, model); err != nil {
		t.Fatal(err)
	}
	result, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(model)})
	if err != nil || result.RequeueAfter <= 0 {
		t.Fatalf("stage home withdrawal during Gateway rebind: result=%#v err=%v", result, err)
	}
	assertRouteProviders(t, r, model.Namespace, model.Name, providerA.Name, providerB.Name)
	assertRouteProviders(t, r, sibling.Namespace, sibling.Name, providerA.Name)
	if got := len(overlayEnvelope(t, r, model.Namespace).Overlay.Candidates); got != 1 {
		t.Fatalf("home overlay candidates after rebind = %d, want only sibling", got)
	}

	sibling = getModel(t, r, sibling)
	seedSafeExternalModelRuntime(t, r, "tenant-a", model.Namespace, []*v1alpha1.ExternalModel{sibling})
	result, err = r.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(model)})
	if err != nil || result.RequeueAfter != withdrawalDrainDuration {
		t.Fatalf("start home rebind drain: result=%#v err=%v", result, err)
	}
	markWithdrawalDrainElapsed(t, r, model.Namespace)
	if _, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(model)}); err != nil {
		t.Fatalf("finish home rebind drain: %v", err)
	}
	if _, err := getUnstructured(r, httpRouteGVK, model.Namespace, modelRouteName(model.Name)); !apierrors.IsNotFound(err) {
		t.Fatalf("old home route after completed rebind = %v, want NotFound", err)
	}
	assertRouteProviders(t, r, sibling.Namespace, sibling.Name, providerA.Name)
}

func TestWithdrawalWaitsForOldReplicaSetsAndEndpointSlices(t *testing.T) {
	r, model := reconcilerFixture(t)
	if _, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(model)}); err != nil {
		t.Fatalf("publish initial routing overlay: %v", err)
	}
	seedSafeExternalModelRuntime(t, r, "tenant", model.Namespace, []*v1alpha1.ExternalModel{model})
	safe, err := r.externalModelRolloutSafe(context.Background(), model.Namespace, "tenant", r.deploymentHashForTest(t, model))
	if err != nil || !safe {
		t.Fatalf("current single-revision runtime safe=%t, err=%v", safe, err)
	}
	validPodLogs := r.readPodLogs
	r.readPodLogs = func(context.Context, string, string, string) (string, error) {
		return "", errors.New("injected pod log read failure")
	}
	safe, err = r.externalModelRolloutSafe(context.Background(), model.Namespace, "tenant", r.deploymentHashForTest(t, model))
	if err == nil || safe {
		t.Fatalf("pod log read failure safe=%t, err=%v; want fail-closed error", safe, err)
	}
	r.readPodLogs = validPodLogs

	var deployment appsv1.Deployment
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: model.Namespace, Name: tenant.PayloadProcessingExternalModelDeploymentName("tenant")}, &deployment); err != nil {
		t.Fatal(err)
	}
	deployment.Status.UpdatedReplicas = 0 // desired Deployment has not converged
	if err := r.Status().Update(context.Background(), &deployment); err != nil {
		t.Fatal(err)
	}
	safe, err = r.externalModelRolloutSafe(context.Background(), model.Namespace, "tenant", r.deploymentHashForTest(t, model))
	if err != nil || safe {
		t.Fatalf("stalled rollout safe=%t, err=%v; want false", safe, err)
	}

	// Restore the current Deployment counts, then model an overlapping rollout:
	// both the new revision and an old revision have ready pods that are still
	// serving through the ExtProc Service at the same time.
	deployment.Status.UpdatedReplicas = 1
	deployment.Status.Replicas = 1
	if err := r.Status().Update(context.Background(), &deployment); err != nil {
		t.Fatal(err)
	}
	oldRS := &appsv1.ReplicaSet{ObjectMeta: metav1.ObjectMeta{Name: "old-extproc", Namespace: model.Namespace, UID: "old-rs-uid",
		Labels:          map[string]string{"app": tenant.PayloadProcessingExternalModelName},
		OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "Deployment", Name: deployment.Name, UID: deployment.UID, Controller: boolPtr(true)}},
	}, Spec: appsv1.ReplicaSetSpec{Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": tenant.PayloadProcessingExternalModelName}}},
		Status: appsv1.ReplicaSetStatus{Replicas: 1, ReadyReplicas: 1, AvailableReplicas: 1}}
	if err := r.Create(context.Background(), oldRS); err != nil {
		t.Fatal(err)
	}
	oldPod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "old-extproc-pod", Namespace: model.Namespace, UID: "old-pod-uid",
		Labels:          map[string]string{"app": tenant.PayloadProcessingExternalModelName},
		OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: oldRS.Name, UID: oldRS.UID, Controller: boolPtr(true)}},
	}, Status: corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}}
	if err := r.Create(context.Background(), oldPod); err != nil {
		t.Fatal(err)
	}
	var slices discv1.EndpointSliceList
	if err := r.List(context.Background(), &slices, client.InNamespace(model.Namespace), client.MatchingLabels{"kubernetes.io/service-name": tenant.PayloadProcessingExternalModelServiceName("tenant")}); err != nil {
		t.Fatal(err)
	}
	if len(slices.Items) != 1 {
		t.Fatalf("EndpointSlices = %d, want 1", len(slices.Items))
	}
	endpointSlice := slices.Items[0]
	if len(endpointSlice.Endpoints) != 1 || endpointSlice.Endpoints[0].TargetRef == nil {
		t.Fatalf("new revision EndpointSlice endpoints = %#v, want its current pod", endpointSlice.Endpoints)
	}
	ready := true
	serving := true
	endpointSlice.Endpoints = append(endpointSlice.Endpoints, discv1.Endpoint{
		Addresses: []string{"10.0.0.2"}, Conditions: discv1.EndpointConditions{Ready: &ready, Serving: &serving},
		TargetRef: &corev1.ObjectReference{APIVersion: "v1", Kind: "Pod", Name: oldPod.Name, Namespace: oldPod.Namespace, UID: oldPod.UID},
	})
	if err := r.Update(context.Background(), &endpointSlice); err != nil {
		t.Fatal(err)
	}
	if len(endpointSlice.Endpoints) != 2 {
		t.Fatalf("overlapping revision endpoint count = %d, want old and new pods", len(endpointSlice.Endpoints))
	}
	safe, err = r.externalModelRolloutSafe(context.Background(), model.Namespace, "tenant", r.deploymentHashForTest(t, model))
	if err != nil || safe {
		t.Fatalf("concurrent old/new serving revisions safe=%t, err=%v; want false", safe, err)
	}
}

func TestWithdrawalRequiresEveryReadyPodToServePublishedOverlay(t *testing.T) {
	r, model := reconcilerFixture(t)
	if _, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(model)}); err != nil {
		t.Fatalf("publish initial routing overlay: %v", err)
	}
	seedSafeExternalModelRuntime(t, r, "tenant", model.Namespace, []*v1alpha1.ExternalModel{model})

	ctx := context.Background()
	var deployment appsv1.Deployment
	deploymentKey := client.ObjectKey{Namespace: model.Namespace, Name: tenant.PayloadProcessingExternalModelDeploymentName("tenant")}
	if err := r.Get(ctx, deploymentKey, &deployment); err != nil {
		t.Fatal(err)
	}
	two := int32(2)
	deployment.Spec.Replicas = &two
	if err := r.Update(ctx, &deployment); err != nil {
		t.Fatal(err)
	}
	if err := r.Get(ctx, deploymentKey, &deployment); err != nil {
		t.Fatal(err)
	}
	deployment.Status.ObservedGeneration = deployment.Generation
	deployment.Status.Replicas = two
	deployment.Status.UpdatedReplicas = two
	deployment.Status.ReadyReplicas = two
	deployment.Status.AvailableReplicas = two
	if err := r.Status().Update(ctx, &deployment); err != nil {
		t.Fatal(err)
	}

	var replicaSets appsv1.ReplicaSetList
	selector := client.MatchingLabels(deployment.Spec.Selector.MatchLabels)
	if err := r.List(ctx, &replicaSets, client.InNamespace(model.Namespace), selector); err != nil {
		t.Fatal(err)
	}
	if len(replicaSets.Items) != 1 {
		t.Fatalf("current ReplicaSets = %d, want 1", len(replicaSets.Items))
	}
	currentRS := replicaSets.Items[0]
	currentRS.Spec.Replicas = &two
	if err := r.Update(ctx, &currentRS); err != nil {
		t.Fatal(err)
	}
	currentRS.Status.Replicas = two
	currentRS.Status.ReadyReplicas = two
	currentRS.Status.AvailableReplicas = two
	if err := r.Status().Update(ctx, &currentRS); err != nil {
		t.Fatal(err)
	}

	var pods corev1.PodList
	if err := r.List(ctx, &pods, client.InNamespace(model.Namespace), selector); err != nil {
		t.Fatal(err)
	}
	if len(pods.Items) != 1 {
		t.Fatalf("initial ExtProc pods = %d, want 1", len(pods.Items))
	}
	secondPod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: "second-extproc-pod", Namespace: model.Namespace, UID: types.UID("second-extproc-pod-uid"),
		Labels: deployment.Spec.Selector.MatchLabels,
		OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: currentRS.Name,
			UID: currentRS.UID, Controller: boolPtr(true)}},
	}, Status: corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}}
	if err := r.Create(ctx, secondPod); err != nil {
		t.Fatal(err)
	}

	var slices discv1.EndpointSliceList
	serviceName := tenant.PayloadProcessingExternalModelServiceName("tenant")
	if err := r.List(ctx, &slices, client.InNamespace(model.Namespace), client.MatchingLabels{"kubernetes.io/service-name": serviceName}); err != nil {
		t.Fatal(err)
	}
	if len(slices.Items) != 1 {
		t.Fatalf("ExtProc EndpointSlices = %d, want 1", len(slices.Items))
	}
	endpointSlice := slices.Items[0]
	ready, serving := true, true
	endpointSlice.Endpoints = append(endpointSlice.Endpoints, discv1.Endpoint{
		Addresses: []string{"10.0.0.2"}, Conditions: discv1.EndpointConditions{Ready: &ready, Serving: &serving},
		TargetRef: &corev1.ObjectReference{APIVersion: "v1", Kind: "Pod", Name: secondPod.Name, Namespace: secondPod.Namespace, UID: secondPod.UID},
	})
	if err := r.Update(ctx, &endpointSlice); err != nil {
		t.Fatal(err)
	}

	validPodLogs := r.readPodLogs
	staleRevision := strings.Repeat("f", 64)
	currentRevision := overlayEnvelope(t, r, model.Namespace).Revision.Value
	if staleRevision == currentRevision {
		staleRevision = strings.Repeat("e", 64)
	}
	r.readPodLogs = func(ctx context.Context, namespace, podName, container string) (string, error) {
		if podName == secondPod.Name {
			return `accepted_revision="` + staleRevision + `" serving_revision="` + staleRevision + `"`, nil
		}
		return validPodLogs(ctx, namespace, podName, container)
	}
	safe, err := r.externalModelRolloutSafe(ctx, model.Namespace, "tenant", r.deploymentHashForTest(t, model))
	if err != nil || safe {
		t.Fatalf("one Ready pod serving a stale overlay safe=%t, err=%v; want false", safe, err)
	}

	r.readPodLogs = validPodLogs
	safe, err = r.externalModelRolloutSafe(ctx, model.Namespace, "tenant", r.deploymentHashForTest(t, model))
	if err != nil || !safe {
		t.Fatalf("both Ready pods serving the published overlay safe=%t, err=%v; want true", safe, err)
	}
}

func TestWithdrawnAppliedRouteRetainsNonDefaultBackendPort(t *testing.T) {
	r, model := reconcilerFixture(t)
	route := modelHTTPRouteSet([]resolver.Route{{
		Model: model.Name, ClientName: model.Name, Namespace: model.Namespace,
		Provider: "provider-b", Cluster: "provider-provider-b", Endpoint: "provider-b.example.com:8443",
	}}, model.Namespace, r.GatewayName, r.GatewayNamespace)
	if err := r.Create(context.Background(), &route); err != nil {
		t.Fatalf("create applied provider route: %v", err)
	}
	applied, err := r.readAppliedModelRoutes(context.Background(), model.Namespace)
	if err != nil {
		t.Fatalf("read applied provider route: %v", err)
	}
	got, found := applied[envelope.CandidateStableID(model.Name, "provider-b")]
	if !found {
		t.Fatal("withdrawn provider route was not reconstructed")
	}
	if got.Endpoint != "provider-b.example.com:8443" {
		t.Fatalf("reconstructed endpoint = %q, want provider-b.example.com:8443", got.Endpoint)
	}
	if port := providerPort(got); port != 8443 {
		t.Fatalf("reconstructed provider route port = %d, want 8443", port)
	}
}

func TestWithdrawalReconciliationErrorKeepsRouteAndRetryResumes(t *testing.T) {
	r, model := reconcilerFixture(t)
	r.KnownClusters = append(r.KnownClusters, "provider-provider-b")
	providerB := &v1alpha1.ExternalProvider{
		ObjectMeta: metav1.ObjectMeta{Name: "provider-b", Namespace: model.Namespace, UID: "provider-b-uid"},
		Spec: v1alpha1.ExternalProviderSpec{
			Provider: "openai", Endpoint: "provider-b.example.com",
			Auth: v1alpha1.AuthConfig{Type: "apikey", SecretRef: v1alpha1.NameReference{Name: "provider-b-credentials"}},
		},
		Status: v1alpha1.ExternalProviderStatus{Phase: resolver.PhaseReady},
	}
	secretB := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "provider-b-credentials", Namespace: model.Namespace}, Data: map[string][]byte{"api-key": []byte("fixture-only-secret")}}
	if err := r.Create(context.Background(), providerB); err != nil {
		t.Fatal(err)
	}
	if err := r.Create(context.Background(), secretB); err != nil {
		t.Fatal(err)
	}
	if _, err := r.ReconcileProvider(context.Background(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(providerB)}); err != nil {
		t.Fatal(err)
	}
	model = getModel(t, r, model)
	model.Spec.ExternalProviderRefs = append(model.Spec.ExternalProviderRefs, v1alpha1.ExternalProviderRef{
		Ref: v1alpha1.ExternalProviderReference{Name: providerB.Name}, TargetModel: "gpt-b", APIFormat: "openai-chat", Path: "/v1/chat/completions",
	})
	if err := r.Update(context.Background(), model); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(model)}); err != nil {
		t.Fatal(err)
	}
	model = getModel(t, r, model)
	model.Spec.ExternalProviderRefs = model.Spec.ExternalProviderRefs[:1]
	if err := r.Update(context.Background(), model); err != nil {
		t.Fatal(err)
	}

	// Let the reduced overlay publish, then fail the live rollout check. B must
	// remain reachable and the next reconcile must resume the drain safely.
	r.APIReader = deploymentReadErrorReader{Reader: r.Client}
	_, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(model)})
	if err == nil || !strings.Contains(err.Error(), "injected Deployment read failure") {
		t.Fatalf("withdrawal error = %v, want injected rollout read failure", err)
	}
	if got := len(overlayEnvelope(t, r, model.Namespace).Overlay.Candidates); got != 1 {
		t.Fatalf("published overlay candidates after rollout error = %d, want reduced A-only overlay", got)
	}
	assertRouteProviders(t, r, model.Namespace, model.Name, "provider", "provider-b")

	r.APIReader = r.Client
	seedSafeExternalModelRuntime(t, r, "tenant", model.Namespace, []*v1alpha1.ExternalModel{model})
	result, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(model)})
	if err != nil {
		t.Fatalf("retry withdrawal after rollout error: %v", err)
	}
	if result.RequeueAfter != withdrawalDrainDuration {
		t.Fatalf("retry drain requeue = %s, want %s", result.RequeueAfter, withdrawalDrainDuration)
	}
	assertRouteProviders(t, r, model.Namespace, model.Name, "provider", "provider-b")
}

func TestProviderDeletionWaitsForTenantOwnedWithdrawalRoute(t *testing.T) {
	r, model := reconcilerFixture(t)
	provider := createWithdrawalProvider(t, r, model.Namespace, "provider-b")
	model = getModel(t, r, model)
	model.Spec.ExternalProviderRefs = []v1alpha1.ExternalProviderRef{withdrawalProviderRef(provider.Name, "gpt-b")}
	if err := r.Update(context.Background(), model); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(model)}); err != nil {
		t.Fatalf("publish final-provider route: %v", err)
	}
	model = getModel(t, r, model)
	var storedProvider v1alpha1.ExternalProvider
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(provider), &storedProvider); err != nil {
		t.Fatal(err)
	}
	provider = &storedProvider
	neighbor := modelHTTPRouteSet([]resolver.Route{{Model: "neighbor-model", ClientName: "neighbor", Namespace: "tenant-b",
		Provider: "provider-b", Cluster: "provider-provider-b", Endpoint: "provider-b.example.com"}}, "tenant-b", "gateway-b", "tenant-b")
	if err := render.Apply(context.Background(), r.Client, []unstructured.Unstructured{neighbor}); err != nil {
		t.Fatal(err)
	}
	if err := r.Delete(context.Background(), provider); err != nil {
		t.Fatal(err)
	}
	result, err := r.ReconcileProvider(context.Background(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(provider)})
	if err != nil {
		t.Fatal(err)
	}
	if result.RequeueAfter != withdrawalRequeueInterval {
		t.Fatalf("provider deletion requeue = %s, want %s", result.RequeueAfter, withdrawalRequeueInterval)
	}
	var pendingProvider v1alpha1.ExternalProvider
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(provider), &pendingProvider); err != nil {
		t.Fatal(err)
	}
	condition := apiMeta.FindStatusCondition(pendingProvider.Status.Conditions, conditionReady)
	if pendingProvider.Status.Phase != providerPhasePending || condition == nil || condition.Status != metav1.ConditionFalse || condition.Reason != reasonProviderInUse ||
		!strings.Contains(condition.Message, model.Namespace+"/"+model.Name) || !strings.Contains(condition.Message, "updated or deleted") {
		t.Fatalf("deleting provider status = %#v, want an explicit in-use blocking state", pendingProvider.Status)
	}
	if _, err := getUnstructured(r, httpRouteGVK, "tenant-b", modelRouteName("neighbor-model")); err != nil {
		t.Fatalf("neighbor tenant route was changed: %v", err)
	}

	// Removing the final model reference is the documented operator action that
	// starts safe staged withdrawal; the provider finalizer remains until the
	// serving revision and request drain have completed.
	model = getModel(t, r, model)
	model.Spec.ExternalProviderRefs = []v1alpha1.ExternalProviderRef{withdrawalProviderRef("provider", "gpt-a")}
	if err := r.Update(context.Background(), model); err != nil {
		t.Fatal(err)
	}
	result, err = r.Reconcile(context.Background(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(model)})
	if err != nil || result.RequeueAfter <= 0 {
		t.Fatalf("stage replacement provider route: result=%#v err=%v", result, err)
	}
	assertRouteProviders(t, r, model.Namespace, model.Name, "provider", "provider-b")
	if got := len(overlayEnvelope(t, r, model.Namespace).Overlay.Candidates); got != 1 || overlayEnvelope(t, r, model.Namespace).Overlay.Candidates[0].Cluster != "provider-provider" {
		t.Fatalf("replacement overlay candidates = %#v, want Provider A only", overlayEnvelope(t, r, model.Namespace).Overlay.Candidates)
	}
	model = getModel(t, r, model)
	seedSafeExternalModelRuntime(t, r, "tenant", model.Namespace, []*v1alpha1.ExternalModel{model})
	result, err = r.Reconcile(context.Background(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(model)})
	if err != nil || result.RequeueAfter != withdrawalDrainDuration {
		t.Fatalf("start provider drain: result=%#v err=%v", result, err)
	}
	markWithdrawalDrainElapsed(t, r, model.Namespace)
	result, err = r.Reconcile(context.Background(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(model)})
	if err != nil || result.RequeueAfter != 0 {
		t.Fatalf("finish provider drain: result=%#v err=%v", result, err)
	}
	assertRouteProviders(t, r, model.Namespace, model.Name, "provider")
	if _, err := getUnstructured(r, httpRouteGVK, "tenant-b", modelRouteName("neighbor-model")); err != nil {
		t.Fatalf("neighbor route did not survive local provider cleanup: %v", err)
	}
	if _, err := r.ReconcileProvider(context.Background(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(provider)}); err != nil {
		t.Fatalf("finish provider finalizer after model drain: %v", err)
	}
	if _, err := getUnstructured(r, schema.GroupVersionKind{Version: "v1", Kind: "Service"}, model.Namespace, "provider-provider-b"); !apierrors.IsNotFound(err) {
		t.Fatalf("withdrawn provider Service remains: %v", err)
	}
}

func createWithdrawalProvider(t *testing.T, r *Reconciler, namespace, name string) *v1alpha1.ExternalProvider {
	t.Helper()
	r.KnownClusters = append(r.KnownClusters, providerServicePrefix+name)
	provider := &v1alpha1.ExternalProvider{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace, UID: types.UID(name + "-uid")},
		Spec: v1alpha1.ExternalProviderSpec{
			Provider: "openai", Endpoint: name + ".example.com",
			Auth: v1alpha1.AuthConfig{Type: "apikey", SecretRef: v1alpha1.NameReference{Name: name + "-credentials"}},
		},
		Status: v1alpha1.ExternalProviderStatus{Phase: resolver.PhaseReady},
	}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: name + "-credentials", Namespace: namespace},
		Data: map[string][]byte{"api-key": []byte("fixture-only-secret")}}
	if err := r.Create(context.Background(), provider); err != nil {
		t.Fatalf("create ExternalProvider %s: %v", name, err)
	}
	if err := r.Create(context.Background(), secret); err != nil {
		t.Fatalf("create credential Secret for %s: %v", name, err)
	}
	if _, err := r.ReconcileProvider(context.Background(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(provider)}); err != nil {
		t.Fatalf("reconcile ExternalProvider %s: %v", name, err)
	}
	return provider
}

func withdrawalProviderRef(provider, target string) v1alpha1.ExternalProviderRef {
	return v1alpha1.ExternalProviderRef{
		Ref: v1alpha1.ExternalProviderReference{Name: provider}, TargetModel: target,
		APIFormat: "openai-chat", Path: "/v1/chat/completions",
	}
}

func markWithdrawalDrainElapsed(t *testing.T, r *Reconciler, namespace string) {
	t.Helper()
	ctx := context.Background()
	var overlay corev1.ConfigMap
	if err := r.Get(ctx, client.ObjectKey{Namespace: namespace, Name: r.configMapName()}, &overlay); err != nil {
		t.Fatal(err)
	}
	var state withdrawalDrainState
	if err := json.Unmarshal([]byte(overlay.Annotations[withdrawalDrainAnnotation]), &state); err != nil || state.Revision == "" {
		t.Fatalf("read withdrawal drain state %#v, err=%v", state, err)
	}
	state.StartedAt = time.Now().Add(-withdrawalDrainDuration - time.Second)
	encoded, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	if overlay.Annotations == nil {
		overlay.Annotations = map[string]string{}
	}
	overlay.Annotations[withdrawalDrainAnnotation] = string(encoded)
	if err := r.Update(ctx, &overlay); err != nil {
		t.Fatal(err)
	}
}

func seedSafeExternalModelRuntime(t *testing.T, r *Reconciler, tenantID, namespace string, models []*v1alpha1.ExternalModel, translation ...bool) {
	t.Helper()
	apiTranslationEnabled := len(translation) > 0 && translation[0]
	r.readPodLogs = func(ctx context.Context, _, _, container string) (string, error) {
		if container != externalModelExtProcContainer {
			t.Fatalf("pod log container = %q, want %q", container, externalModelExtProcContainer)
		}
		env, err := r.readPublishedEnvelope(ctx, namespace)
		if err != nil {
			return "", err
		}
		if env == nil {
			return "", errors.New("routing overlay is missing")
		}
		revision := env.Revision.Value
		return `accepted_revision="` + revision + `" serving_revision="` + revision + `"`, nil
	}
	var providers v1alpha1.ExternalProviderList
	if err := r.List(context.Background(), &providers, client.InNamespace(namespace)); err != nil {
		t.Fatal(err)
	}
	providerPtrs := make([]*v1alpha1.ExternalProvider, 0, len(providers.Items))
	for i := range providers.Items {
		if providers.Items[i].DeletionTimestamp.IsZero() {
			providerPtrs = append(providerPtrs, &providers.Items[i])
		}
	}
	routes, err := resolver.ResolveAllWithAPITranslation(models, providerPtrs, apiTranslationEnabled)
	if errors.Is(err, resolver.ErrNoRoutes) {
		routes, err = &resolver.ResolvedRouteSet{}, nil
	}
	if err != nil {
		t.Fatalf("resolve runtime for safe fixture: %v", err)
	}
	candidates, err := tenant.ExternalModelRuntimeCandidates(routes, apiTranslationEnabled)
	if err != nil {
		t.Fatal(err)
	}
	candidates, err = tenant.RetainHomeRuntimeCandidates(context.Background(), r.Client, namespace, candidates, apiTranslationEnabled)
	if err != nil {
		t.Fatal(err)
	}
	for _, candidate := range overlayEnvelope(t, r, namespace).Overlay.Candidates {
		if envelope.IsScopedStableID(candidate.StableID) {
			candidate = cloneCandidate(candidate)
			if candidate.Credential != nil {
				copySecret := &metav1.PartialObjectMetadata{}
				copySecret.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("Secret"))
				ref := candidate.Credential.SecretRef
				if err := r.APIReader.Get(context.Background(), client.ObjectKey{Namespace: ref.Namespace, Name: ref.Name}, copySecret); err != nil {
					t.Fatalf("read projected credential metadata %s/%s: %v", ref.Namespace, ref.Name, err)
				}
				if copySecret.GetLabels()[tenant.LabelCredentialCopy] == "true" {
					if header := copySecret.GetAnnotations()[tenant.AnnotationCredentialHeader]; envelope.IsQualifiedAuthHeader(header) {
						candidate.Credential.Header = header
					}
				}
			}
			candidates = append(candidates, candidate)
		}
	}
	config, configHash, err := tenant.ExternalModelExtProcConfig(namespace, candidates, apiTranslationEnabled)
	if err != nil {
		t.Fatal(err)
	}
	labels := map[string]string{"app.kubernetes.io/managed-by": "ai-gateway-controller", "app": tenant.PayloadProcessingExternalModelName}
	selector := map[string]string{"app": tenant.PayloadProcessingExternalModelName}
	one := int32(1)
	deploymentName := tenant.PayloadProcessingExternalModelDeploymentName(tenantID)
	serviceName := tenant.PayloadProcessingExternalModelServiceName(tenantID)
	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: deploymentName, Namespace: namespace, UID: types.UID("deployment-" + tenantID), Generation: 1, Labels: labels},
		Spec: appsv1.DeploymentSpec{
			Replicas: &one, Selector: &metav1.LabelSelector{MatchLabels: selector},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: selector, Annotations: map[string]string{extprocConfigHashAnnotation: configHash}},
				Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "extproc", Image: "example.invalid/extproc"}}},
			},
		},
		Status: appsv1.DeploymentStatus{ObservedGeneration: 1, Replicas: 1, UpdatedReplicas: 1, ReadyReplicas: 1, AvailableReplicas: 1},
	}
	rs := &appsv1.ReplicaSet{ObjectMeta: metav1.ObjectMeta{Name: deploymentName + "-new", Namespace: namespace, UID: types.UID("rs-" + tenantID), Labels: selector,
		Annotations: map[string]string{"deployment.kubernetes.io/revision": "1"},
		OwnerReferences: []metav1.OwnerReference{{
			APIVersion: "apps/v1", Kind: "Deployment", Name: deploymentName,
			UID: deployment.UID, Controller: boolPtr(true),
		}},
	},
		Spec:   appsv1.ReplicaSetSpec{Replicas: &one, Selector: &metav1.LabelSelector{MatchLabels: selector}, Template: deployment.Spec.Template},
		Status: appsv1.ReplicaSetStatus{Replicas: 1, ReadyReplicas: 1, AvailableReplicas: 1},
	}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: deploymentName + "-new-0", Namespace: namespace, UID: types.UID("pod-" + tenantID), Labels: selector,
		OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: rs.Name, UID: rs.UID, Controller: boolPtr(true)}}},
		Status: corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}}
	service := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: serviceName, Namespace: namespace, Labels: labels}, Spec: corev1.ServiceSpec{Selector: selector}}
	ready, serving := true, true
	endpointSlice := &discv1.EndpointSlice{ObjectMeta: metav1.ObjectMeta{Name: serviceName + "-safe", Namespace: namespace,
		Labels: map[string]string{"kubernetes.io/service-name": serviceName}}, AddressType: discv1.AddressTypeIPv4,
		Ports: []discv1.EndpointPort{{Name: stringPtr("grpc"), Port: int32Ptr(9004)}},
		Endpoints: []discv1.Endpoint{{Addresses: []string{"10.0.0.1"}, Conditions: discv1.EndpointConditions{Ready: &ready, Serving: &serving},
			TargetRef: &corev1.ObjectReference{APIVersion: "v1", Kind: "Pod", Name: pod.Name, Namespace: namespace, UID: pod.UID}}}}
	configMap := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: tenant.PayloadProcessingExternalModelPluginsConfigMapForTenant(tenantID), Namespace: namespace, Labels: labels},
		Data: map[string]string{"extproc.yaml": config}}
	for _, object := range []client.Object{deployment, rs, pod, service, endpointSlice, configMap} {
		if err := r.Create(context.Background(), object); err != nil {
			t.Fatalf("create safe runtime %T %s/%s: %v", object, object.GetNamespace(), object.GetName(), err)
		}
	}
}

// markSafeExternalModelRuntimeCurrent simulates completion of the rollout
// after an earlier safe runtime was seeded. It updates the exact config,
// Deployment template hash, and current ReplicaSet template without changing
// pod identity or EndpointSlice membership.
func markSafeExternalModelRuntimeCurrent(t *testing.T, r *Reconciler, tenantID, namespace string, apiTranslationEnabled bool, models ...*v1alpha1.ExternalModel) {
	t.Helper()
	ctx := context.Background()
	env := overlayEnvelope(t, r, namespace)
	candidates, err := tenant.RetainHomeRuntimeCandidates(ctx, r.Client, namespace, env.Overlay.Candidates, apiTranslationEnabled)
	if err != nil {
		t.Fatal(err)
	}
	config, hash, err := tenant.ExternalModelExtProcConfig(namespace, candidates, apiTranslationEnabled)
	if err != nil {
		t.Fatalf("render serving ExtProc config from reduced overlay: %v", err)
	}
	deploymentName := tenant.PayloadProcessingExternalModelDeploymentName(tenantID)
	deployment := &appsv1.Deployment{}
	if err := r.Get(ctx, client.ObjectKey{Namespace: namespace, Name: deploymentName}, deployment); err != nil {
		t.Fatalf("get seeded ExternalModel ExtProc Deployment: %v", err)
	}
	if deployment.Spec.Template.Annotations == nil {
		deployment.Spec.Template.Annotations = map[string]string{}
	}
	deployment.Spec.Template.Annotations[extprocConfigHashAnnotation] = hash
	if err := r.Update(ctx, deployment); err != nil {
		t.Fatalf("update ExternalModel ExtProc Deployment revision: %v", err)
	}
	if err := r.Get(ctx, client.ObjectKeyFromObject(deployment), deployment); err != nil {
		t.Fatalf("read updated ExternalModel ExtProc Deployment: %v", err)
	}
	deployment.Status.ObservedGeneration = deployment.Generation
	if err := r.Status().Update(ctx, deployment); err != nil {
		t.Fatalf("mark ExternalModel ExtProc Deployment revision observed: %v", err)
	}
	rs := &appsv1.ReplicaSet{}
	if err := r.Get(ctx, client.ObjectKey{Namespace: namespace, Name: deploymentName + "-new"}, rs); err != nil {
		t.Fatalf("get seeded ExternalModel ExtProc ReplicaSet: %v", err)
	}
	rs.Spec.Template.Annotations[extprocConfigHashAnnotation] = hash
	if err := r.Update(ctx, rs); err != nil {
		t.Fatalf("update ExternalModel ExtProc ReplicaSet revision: %v", err)
	}
	configMap := &corev1.ConfigMap{}
	if err := r.Get(ctx, client.ObjectKey{Namespace: namespace, Name: tenant.PayloadProcessingExternalModelPluginsConfigMapForTenant(tenantID)}, configMap); err != nil {
		t.Fatalf("get seeded ExternalModel ExtProc ConfigMap: %v", err)
	}
	configMap.Data["extproc.yaml"] = config
	if err := r.Update(ctx, configMap); err != nil {
		t.Fatalf("update ExternalModel ExtProc ConfigMap revision: %v", err)
	}
	expectedHash, err := runtimeConfigHash(ctx, r, models, namespace, apiTranslationEnabled)
	if err != nil {
		t.Fatalf("compute current serving config hash: %v", err)
	}
	deployment, replicas, deploymentSafe, err := r.externalModelDeploymentSafe(ctx, namespace, tenantID, expectedHash)
	if err != nil {
		t.Fatalf("check current ExternalModel Deployment: %v", err)
	}
	if !deploymentSafe {
		actual := &appsv1.Deployment{}
		readErr := r.Get(ctx, client.ObjectKey{Namespace: namespace, Name: tenant.PayloadProcessingExternalModelDeploymentName(tenantID)}, actual)
		configMap := &corev1.ConfigMap{}
		cmErr := r.Get(ctx, client.ObjectKey{Namespace: namespace, Name: tenant.PayloadProcessingExternalModelPluginsConfigMapForTenant(tenantID)}, configMap)
		cmDigest := sha256.Sum256([]byte(configMap.Data["extproc.yaml"]))
		t.Fatalf("simulated current Deployment is not safe: expected hash=%s replicas=%d readErr=%v labels=%#v generation=%d observed=%d status=%#v specReplicas=%v template=%#v configMapErr=%v configHash=%x configBytes=%d",
			expectedHash, replicas, readErr, actual.Labels, actual.Generation, actual.Status.ObservedGeneration, actual.Status,
			actual.Spec.Replicas, actual.Spec.Template.Annotations, cmErr, cmDigest, len(configMap.Data["extproc.yaml"]))
	}
	pods, podsSafe, err := r.externalModelPodsSafe(ctx, namespace, deployment, replicas, expectedHash, env.Revision.Value)
	if err != nil || !podsSafe {
		t.Fatalf("simulated current Pods are not safe: err=%v pods=%#v", err, pods)
	}
	if safe, err := r.externalModelEndpointsSafe(ctx, namespace, tenantID, pods); err != nil || !safe {
		t.Fatalf("simulated current endpoints are not safe: safe=%t err=%v pods=%#v", safe, err, pods)
	}
}

func (r *Reconciler) deploymentHashForTest(t *testing.T, model *v1alpha1.ExternalModel) string {
	t.Helper()
	var providers v1alpha1.ExternalProviderList
	if err := r.List(context.Background(), &providers, client.InNamespace(model.Namespace)); err != nil {
		t.Fatal(err)
	}
	providerPtrs := make([]*v1alpha1.ExternalProvider, 0, len(providers.Items))
	for i := range providers.Items {
		if providers.Items[i].DeletionTimestamp.IsZero() {
			providerPtrs = append(providerPtrs, &providers.Items[i])
		}
	}
	routes, err := resolver.ResolveAll([]*v1alpha1.ExternalModel{model}, providerPtrs)
	if err != nil {
		t.Fatal(err)
	}
	candidates, err := tenant.ExternalModelRuntimeCandidates(routes, false)
	if err != nil {
		t.Fatal(err)
	}
	hash, err := tenant.ExternalModelExtProcConfigHash(model.Namespace, candidates, false)
	if err != nil {
		t.Fatal(err)
	}
	return hash
}

func assertRouteProviders(t *testing.T, r *Reconciler, namespace, model string, want ...string) {
	t.Helper()
	route := &unstructured.Unstructured{}
	route.SetGroupVersionKind(schema.GroupVersionKind{Group: "gateway.networking.k8s.io", Version: "v1", Kind: "HTTPRoute"})
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: namespace, Name: modelRouteName(model)}, route); err != nil {
		t.Fatalf("get model route %s/%s: %v", namespace, modelRouteName(model), err)
	}
	routes, _, err := unstructured.NestedSlice(route.Object, "spec", "rules")
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for _, rawRule := range routes {
		rule, ok := rawRule.(map[string]any)
		if !ok {
			continue
		}
		refs, _, _ := unstructured.NestedSlice(rule, "backendRefs")
		if len(refs) == 0 {
			continue
		}
		backend, ok := refs[0].(map[string]any)
		if !ok {
			continue
		}
		name, _ := backend["name"].(string)
		if strings.HasPrefix(name, providerServicePrefix) && !strings.HasPrefix(name, providerSelectionSinkPrefix) {
			got[strings.TrimPrefix(name, providerServicePrefix)] = true
		}
	}
	for _, provider := range want {
		if !got[provider] {
			t.Fatalf("HTTPRoute providers = %#v, missing %q", got, provider)
		}
		delete(got, provider)
	}
	if len(got) != 0 {
		t.Fatalf("HTTPRoute retained unexpected providers %#v", got)
	}
}

func boolPtr(value bool) *bool       { return &value }
func stringPtr(value string) *string { return &value }
func int32Ptr(value int32) *int32    { return &value }

func TestUnknownServingRevisionPreservesDrainButCannotAuthorizeCleanup(t *testing.T) {
	ctx := context.Background()
	r, model := reconcilerFixture(t)
	reconcileModel(t, r, model)
	seedSafeExternalModelRuntime(t, r, "tenant", model.Namespace, []*v1alpha1.ExternalModel{model})
	markSafeExternalModelRuntimeCurrent(t, r, "tenant", model.Namespace, false, model)
	env := overlayEnvelope(t, r, model.Namespace)
	now := time.Now()
	if _, err := r.withdrawalDrainRemaining(ctx, model.Namespace, env.Revision.Value, true, now.Add(-withdrawalDrainDuration)); err != nil {
		t.Fatal(err)
	}
	readState := func() string {
		var cm corev1.ConfigMap
		if err := r.Get(ctx, client.ObjectKey{Namespace: model.Namespace, Name: r.configMapName()}, &cm); err != nil {
			t.Fatal(err)
		}
		return cm.Annotations[withdrawalDrainAnnotation]
	}
	before := readState()
	knownLogs := r.readPodLogs
	r.readPodLogs = func(context.Context, string, string, string) (string, error) {
		return strings.Repeat("request handled\n", 301), nil
	}
	tnt := attachmentTenant{namespace: model.Namespace, tenantID: "tenant"}
	remaining, err := r.attachmentWithdrawalRemaining(ctx, tnt)
	if err != nil || remaining != withdrawalRequeueInterval {
		t.Fatalf("unknown revision must block cleanup: remaining=%v err=%v", remaining, err)
	}
	if readState() != before {
		t.Fatal("unknown revision reset the persisted drain")
	}
	r.readPodLogs = knownLogs
	remaining, err = r.attachmentWithdrawalRemaining(ctx, tnt)
	if err != nil || remaining != 0 {
		t.Fatalf("fresh proof should finish the original elapsed drain: remaining=%v err=%v", remaining, err)
	}
	r.readPodLogs = func(context.Context, string, string, string) (string, error) {
		return `accepted_revision="` + strings.Repeat("b", 64) + `" serving_revision="` + strings.Repeat("b", 64) + `"`, nil
	}
	remaining, err = r.attachmentWithdrawalRemaining(ctx, tnt)
	if err != nil || remaining != withdrawalRequeueInterval {
		t.Fatalf("mismatch must block: %v %v", remaining, err)
	}
	if readState() != "" {
		t.Fatal("confirmed mismatch did not invalidate the drain")
	}
}

func TestTerminalPodsDoNotBlockServingRevision(t *testing.T) {
	ctx := context.Background()
	r, model := reconcilerFixture(t)
	if _, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(model)}); err != nil {
		t.Fatal(err)
	}
	seedSafeExternalModelRuntime(t, r, "tenant", model.Namespace, []*v1alpha1.ExternalModel{model})
	deployment := &appsv1.Deployment{}
	if err := r.Get(ctx, client.ObjectKey{Namespace: model.Namespace, Name: tenant.PayloadProcessingExternalModelDeploymentName("tenant")}, deployment); err != nil {
		t.Fatal(err)
	}
	for _, phase := range []corev1.PodPhase{corev1.PodFailed, corev1.PodSucceeded} {
		pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "terminal-" + strings.ToLower(string(phase)), Namespace: model.Namespace, Labels: deployment.Spec.Selector.MatchLabels}, Status: corev1.PodStatus{Phase: phase}}
		if err := r.Create(ctx, pod); err != nil {
			t.Fatal(err)
		}
	}
	env := overlayEnvelope(t, r, model.Namespace)
	pods, safe, err := r.externalModelPodsSafe(ctx, model.Namespace, deployment, 1, deployment.Spec.Template.Annotations[extprocConfigHashAnnotation], env.Revision.Value)
	if err != nil || !safe || len(pods) != 1 {
		t.Fatalf("terminal Pods blocked the one current serving Pod: safe=%t pods=%v err=%v", safe, pods, err)
	}
	pending := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pending-unowned", Namespace: model.Namespace, Labels: deployment.Spec.Selector.MatchLabels}, Status: corev1.PodStatus{Phase: corev1.PodPending}}
	if err := r.Create(ctx, pending); err != nil {
		t.Fatal(err)
	}
	if _, safe, err := r.externalModelPodsSafe(ctx, model.Namespace, deployment, 1, deployment.Spec.Template.Annotations[extprocConfigHashAnnotation], env.Revision.Value); err != nil || safe {
		t.Fatalf("nonterminal unowned Pod must still block: safe=%t err=%v", safe, err)
	}
}

func TestPodServesOverlayRevisionParsesReloadLogs(t *testing.T) {
	revision := strings.Repeat("a", 64)
	other := strings.Repeat("b", 64)
	for _, tc := range []struct {
		name, logs               string
		wantServing, wantUnknown bool
	}{
		{name: "unquoted reload", logs: "overlay reloaded accepted_revision=" + revision + " serving_revision=" + revision, wantServing: true},
		{name: "quoted startup", logs: `accepted_revision="` + revision + `" serving_revision="` + revision + `"`, wantServing: true},
		{name: "mixed quoting", logs: `accepted_revision="` + revision + `" serving_revision=` + revision + " elapsed=1ms", wantServing: true},
		{name: "newest revision wins", logs: "accepted_revision=" + revision + " serving_revision=" + revision + "\naccepted_revision=" + other + " serving_revision=" + other},
		{name: "accepted is not serving", logs: "accepted_revision=" + revision + " serving_revision=" + other},
		{name: "oversized digest", logs: "accepted_revision=" + revision + "a serving_revision=" + revision, wantUnknown: true},
		{name: "malformed newest line", logs: "accepted_revision=" + revision + " serving_revision=" + revision + "\naccepted_revision=unknown", wantUnknown: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := &Reconciler{readPodLogs: func(context.Context, string, string, string) (string, error) { return tc.logs, nil }}
			serving, err := r.podServesOverlayRevision(context.Background(), &corev1.Pod{}, revision)
			if serving != tc.wantServing || errors.Is(err, errServingRevisionUnknown) != tc.wantUnknown || err != nil && !tc.wantUnknown {
				t.Fatalf("serving=%v err=%v; want serving=%v unknown=%v", serving, err, tc.wantServing, tc.wantUnknown)
			}
		})
	}
}

func TestDeletingHomeBindingRetainsRuntimeProfileThroughDrain(t *testing.T) {
	for _, deleteProvider := range []bool{false, true} {
		t.Run(fmt.Sprintf("delete_provider_%v", deleteProvider), func(t *testing.T) {
			ctx := context.Background()
			r, model := reconcilerFixture(t)
			providerB := createWithdrawalProvider(t, r, model.Namespace, "provider-b")
			sibling := &v1alpha1.ExternalModel{ObjectMeta: metav1.ObjectMeta{Name: "sibling", Namespace: model.Namespace, UID: "sibling-uid"},
				Spec: v1alpha1.ExternalModelSpec{ExternalProviderRefs: []v1alpha1.ExternalProviderRef{withdrawalProviderRef(providerB.Name, "sibling-target")}}}
			if err := r.Create(ctx, sibling); err != nil {
				t.Fatal(err)
			}
			reconcileModel(t, r, model)
			reconcileModel(t, r, sibling)
			models := []*v1alpha1.ExternalModel{getModel(t, r, model), getModel(t, r, sibling)}
			beforeHash, err := runtimeConfigHash(ctx, r, models, model.Namespace, false)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := r.ReconcileProvider(ctx, reconcile.Request{NamespacedName: client.ObjectKey{Namespace: model.Namespace, Name: "provider"}}); err != nil {
				t.Fatal(err)
			}
			seedSafeExternalModelRuntime(t, r, "tenant", model.Namespace, models)
			quotedLogs := r.readPodLogs
			r.readPodLogs = func(ctx context.Context, namespace, pod, container string) (string, error) {
				logs, err := quotedLogs(ctx, namespace, pod, container)
				return strings.ReplaceAll(logs, "\"", ""), err
			}
			model = getModel(t, r, model)
			if deleteProvider {
				var provider v1alpha1.ExternalProvider
				if err := r.Get(ctx, client.ObjectKey{Namespace: model.Namespace, Name: "provider"}, &provider); err != nil {
					t.Fatal(err)
				}
				if err := r.Delete(ctx, &provider); err != nil {
					t.Fatal(err)
				}
				if _, err := r.ReconcileProvider(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&provider)}); err != nil {
					t.Fatal(err)
				}
				zero := 0
				model.Spec.ExternalProviderRefs[0].Weight = &zero
				if err := r.Update(ctx, model); err != nil {
					t.Fatal(err)
				}
			} else if err := r.Delete(ctx, model); err != nil {
				t.Fatal(err)
			}
			assertRetained := func() {
				t.Helper()
				candidates, err := tenant.RetainHomeRuntimeCandidates(ctx, r.Client, model.Namespace, nil, false)
				if err != nil || len(candidates) != 2 {
					t.Fatalf("retained runtime candidates=%#v err=%v", candidates, err)
				}
				config, hash, err := tenant.ExternalModelExtProcConfig(model.Namespace, candidates, false)
				if err != nil || hash != beforeHash || !strings.Contains(config, "provider-provider") || !strings.Contains(config, "credentials") {
					t.Fatalf("retained runtime hash=%s want=%s err=%v", hash, beforeHash, err)
				}
			}
			assertRetained()
			result, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(model)})
			if err != nil || result.RequeueAfter <= 0 {
				t.Fatalf("stage withdrawal: result=%#v err=%v", result, err)
			}
			for _, candidate := range overlayEnvelope(t, r, model.Namespace).Overlay.Candidates {
				if candidate.StableID == envelope.CandidateStableID(model.Name, "provider") {
					t.Fatal("withdrawn provider is still eligible in the overlay")
				}
			}
			assertRetained()
			if _, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(sibling)}); err != nil {
				t.Fatal(err)
			}
			assertRetained()
			markWithdrawalDrainElapsed(t, r, model.Namespace)
			if _, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(model)}); err != nil {
				t.Fatal(err)
			}
			candidates, err := tenant.RetainHomeRuntimeCandidates(ctx, r.Client, model.Namespace, nil, false)
			if err != nil || len(candidates) != 1 || candidates[0].StableID != envelope.CandidateStableID(sibling.Name, providerB.Name) {
				t.Fatalf("runtime after drain=%#v err=%v", candidates, err)
			}
		})
	}
}

func TestMissingWithdrawnProviderUsesAppliedProfileOrSkipsStaleCandidate(t *testing.T) {
	ctx := context.Background()
	r, model := reconcilerFixture(t)
	reconcileModel(t, r, model)
	previous := overlayEnvelope(t, r, model.Namespace)
	base, ok := r.Client.(client.WithWatch)
	if !ok {
		t.Fatal("test client does not support watches")
	}
	lookupErr := errors.New("provider API read failed")
	r.Client = interceptor.NewClient(base, interceptor.Funcs{Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
		if _, provider := obj.(*v1alpha1.ExternalProvider); provider {
			return lookupErr
		}
		return c.Get(ctx, key, obj, opts...)
	}})
	if _, _, err := r.transportRoutesForPublication(ctx, model.Namespace, nil, &previous, false); !errors.Is(err, lookupErr) {
		t.Fatalf("provider read failure was swallowed: %v", err)
	}
	r.Client = base

	if err := r.Delete(ctx, &v1alpha1.ExternalProvider{ObjectMeta: metav1.ObjectMeta{Namespace: model.Namespace, Name: "provider"}}); err != nil {
		t.Fatal(err)
	}
	routes, pending, err := r.transportRoutesForPublication(ctx, model.Namespace, nil, &previous, false)
	if err != nil || !pending || len(routes) != 1 || routes[0].ProviderType != "openai" || routes[0].SecretName != "credentials" || routes[0].Endpoint != "api.example.com" {
		t.Fatalf("applied profile not preserved: routes=%#v pending=%v err=%v", routes, pending, err)
	}
	if route, err := getUnstructured(r, httpRouteGVK, model.Namespace, modelRouteName(model.Name)); err != nil {
		t.Fatal(err)
	} else if err := r.Delete(ctx, route); err != nil {
		t.Fatal(err)
	}
	routes, pending, err = r.transportRoutesForPublication(ctx, model.Namespace, nil, &previous, false)
	if err != nil || pending || len(routes) != 0 {
		t.Fatalf("stale candidate not skipped: routes=%#v pending=%v err=%v", routes, pending, err)
	}
	routes, err = r.routesForAttachmentWithdrawal(ctx, model, attachmentTenant{}, previous.Overlay.Candidates)
	if err != nil || len(routes) != 0 {
		t.Fatalf("stale attachment not skipped: routes=%#v err=%v", routes, err)
	}
}
