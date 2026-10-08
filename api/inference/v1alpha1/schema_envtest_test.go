/*
Copyright 2026 The opendatahub.io Authors.

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

package v1alpha1

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

func schemaModel(name string) *ExternalModel {
	return &ExternalModel{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "models"},
		Spec: ExternalModelSpec{
			ExternalProviderRefs: []ExternalProviderRef{{
				Ref: ExternalProviderReference{Name: "provider"}, TargetModel: "gpt-4o",
				APIFormat: "openai-chat", Path: "/v1/chat/completions",
			}},
		},
	}
}

func rawSchemaModel(t *testing.T, name string) *unstructured.Unstructured {
	t.Helper()
	doc, err := runtime.DefaultUnstructuredConverter.ToUnstructured(schemaModel(name))
	require.NoError(t, err)
	obj := &unstructured.Unstructured{Object: doc}
	obj.SetAPIVersion(GroupVersion.String())
	obj.SetKind("ExternalModel")
	return obj
}

func gateway(name, namespace string) map[string]any {
	return map[string]any{"name": name, "namespace": namespace}
}

func TestInferenceAPISchema(t *testing.T) {
	if testing.Short() {
		t.Skip("envtest starts a local API server; run without -short to validate admission")
	}
	crds := inferenceCRDs(t)
	legacy := installedLegacySchemas(t)
	legacyCRDs := inferenceCRDs(t)
	for _, crd := range legacyCRDs {
		schema := legacy[crd.Spec.Names.Plural]
		crd.Spec.Versions[0].Schema.OpenAPIV3Schema = &schema
	}
	useExistingCluster := false
	env := &envtest.Environment{
		CRDInstallOptions:           envtest.CRDInstallOptions{CRDs: legacyCRDs},
		UseExistingCluster:          &useExistingCluster,
		BinaryAssetsDirectory:       filepath.Join("..", "..", "..", "bin", "envtest"),
		DownloadBinaryAssets:        os.Getenv("KUBEBUILDER_ASSETS") == "",
		DownloadBinaryAssetsVersion: "1.35.0",
	}
	cfg, err := env.Start()
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, env.Stop()) })
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, AddToScheme(scheme))
	kube, err := client.New(cfg, client.Options{Scheme: scheme})
	require.NoError(t, err)
	ctx := t.Context()
	require.NoError(t, kube.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "models"}}))

	// Create real old-shaped resources before replacing the installed schema.
	provider := &ExternalProvider{
		ObjectMeta: metav1.ObjectMeta{Name: "provider", Namespace: "models"},
		Spec: ExternalProviderSpec{
			Provider: "openai", Endpoint: "api.example.com",
			Auth:   AuthConfig{Type: "simple", SecretRef: NameReference{Name: "a..b"}},
			Config: map[string]string{"project": "legacy"},
		},
	}
	model := schemaModel("legacy")
	model.Spec.ExternalProviderRefs[0].Auth = &AuthConfig{Type: "simple", SecretRef: NameReference{Name: "a.-b"}}
	model.Spec.ExternalProviderRefs[0].Weight = new(int)
	model.Spec.ExternalProviderRefs[0].Config = map[string]string{"project": "override"}
	require.NoError(t, kube.Create(ctx, provider))
	require.NoError(t, kube.Create(ctx, model))
	condition := metav1.Condition{
		Type: "Ready", Status: metav1.ConditionTrue, Reason: "Reconciled",
		ObservedGeneration: model.Generation, LastTransitionTime: metav1.NewTime(time.Now().UTC().Truncate(time.Second)),
	}
	provider.Status = ExternalProviderStatus{Phase: "Ready", Conditions: []metav1.Condition{condition}}
	model.Status = ExternalModelStatus{Phase: "Ready", HTTPRouteName: "legacy-route", Conditions: []metav1.Condition{condition}}
	require.NoError(t, kube.Status().Update(ctx, provider))
	require.NoError(t, kube.Status().Update(ctx, model))
	beforeProvider, beforeModel := provider.DeepCopy(), model.DeepCopy()
	_, err = envtest.InstallCRDs(cfg, envtest.CRDInstallOptions{CRDs: crds})
	require.NoError(t, err)

	t.Run("legacy upgrade and round trips", func(t *testing.T) {
		require.NoError(t, kube.Get(ctx, client.ObjectKeyFromObject(provider), provider))
		require.NoError(t, kube.Get(ctx, client.ObjectKeyFromObject(model), model))
		assert.Equal(t, beforeProvider.Spec, provider.Spec)
		assert.Equal(t, beforeProvider.Status, provider.Status)
		assert.Equal(t, beforeModel.Spec, model.Spec)
		assert.Equal(t, beforeModel.Status, model.Status)
		assert.Nil(t, model.Spec.GatewayRefs)
		assert.Nil(t, model.Status.Gateways)
		assert.Empty(t, model.Spec.ExternalProviderRefs[0].Ref.Namespace)
		for _, obj := range []client.Object{provider, model} {
			obj.SetAnnotations(map[string]string{"updated": "true"})
			require.NoError(t, kube.Update(ctx, obj))
		}
		provider.Status.ObservedGeneration = provider.Generation
		model.Status.ObservedGeneration = model.Generation
		model.Status.OverlayDigest = "sha256:test"
		model.Status.OverlayGeneration = 7
		require.NoError(t, kube.Status().Update(ctx, provider))
		require.NoError(t, kube.Status().Update(ctx, model))
		expectedProvider, expectedModel := provider.Status, model.Status
		require.NoError(t, kube.Get(ctx, client.ObjectKeyFromObject(provider), provider))
		require.NoError(t, kube.Get(ctx, client.ObjectKeyFromObject(model), model))
		assert.Equal(t, expectedProvider, provider.Status)
		assert.Equal(t, expectedModel, model.Status)

		// New creates must accept all installed auth values too, without relying on validation ratcheting.
		for _, authType := range []string{"apikey", "simple", "sigv4", "oauth2"} {
			newProvider := beforeProvider.DeepCopy()
			newProvider.ObjectMeta = metav1.ObjectMeta{Name: "provider-" + authType, Namespace: "models"}
			newProvider.Spec.Auth.Type = authType
			require.NoError(t, kube.Create(ctx, newProvider))
			newModel := schemaModel("model-" + authType)
			newModel.Spec.ExternalProviderRefs[0].Auth = &AuthConfig{Type: authType, SecretRef: NameReference{Name: "a-.b"}}
			require.NoError(t, kube.Create(ctx, newModel))
		}
	})

	t.Run("gateway admission", func(t *testing.T) {
		many := func(count int) []any {
			refs := make([]any, count)
			for i := range refs {
				refs[i] = gateway(fmt.Sprintf("gateway-%d", i), "tenant-a")
			}
			return refs
		}
		cases := []struct {
			name    string
			refs    []any
			invalid bool
		}{
			{name: "omitted"},
			{name: "empty", refs: []any{}, invalid: true},
			{name: "one", refs: many(1)},
			{name: "sixteen", refs: many(16)},
			{name: "seventeen", refs: many(17), invalid: true},
			{name: "duplicate", refs: []any{gateway("shared", "tenant-a"), gateway("shared", "tenant-a")}, invalid: true},
			{name: "different-namespaces", refs: []any{gateway("shared", "tenant-a"), gateway("shared", "tenant-b")}},
			{name: "missing-name", refs: []any{map[string]any{"namespace": "tenant-a"}}, invalid: true},
			{name: "missing-namespace", refs: []any{map[string]any{"name": "gateway"}}, invalid: true},
			{name: "empty-name", refs: []any{gateway("", "tenant-a")}, invalid: true},
			{name: "empty-namespace", refs: []any{gateway("gateway", "")}, invalid: true},
			{name: "uppercase-name", refs: []any{gateway("Gateway", "tenant-a")}, invalid: true},
			{name: "invalid-name", refs: []any{gateway("a..b", "tenant-a")}, invalid: true},
			{name: "dotted-name", refs: []any{gateway("a.b", "tenant-a")}},
			{name: "longest-name", refs: []any{gateway(strings.Repeat("a", 253), "tenant-a")}},
			{name: "long-name", refs: []any{gateway(strings.Repeat("a", 254), "tenant-a")}, invalid: true},
			{name: "invalid-namespace", refs: []any{gateway("gateway", "tenant.a")}, invalid: true},
			{name: "uppercase-namespace", refs: []any{gateway("gateway", "Tenant")}, invalid: true},
			{name: "longest-namespace", refs: []any{gateway("gateway", strings.Repeat("a", 63))}},
			{name: "long-namespace", refs: []any{gateway("gateway", strings.Repeat("a", 64))}, invalid: true},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				obj := rawSchemaModel(t, "gateway-"+tc.name)
				if tc.refs != nil {
					require.NoError(t, unstructured.SetNestedSlice(obj.Object, tc.refs, "spec", "gatewayRefs"))
				}
				err := kube.Create(ctx, obj)
				if tc.invalid {
					require.True(t, apierrors.IsInvalid(err), "expected admission rejection, got %v", err)
					assert.Contains(t, err.Error(), "gatewayRefs")
					return
				}
				require.NoError(t, err)
				require.NoError(t, kube.Get(ctx, client.ObjectKeyFromObject(obj), obj))
				refs, found, err := unstructured.NestedSlice(obj.Object, "spec", "gatewayRefs")
				require.NoError(t, err)
				assert.Equal(t, tc.refs != nil, found)
				assert.Equal(t, tc.refs, refs)
			})
		}
		obj := rawSchemaModel(t, "gateway-update")
		require.NoError(t, kube.Create(ctx, obj))
		for _, tc := range cases {
			if tc.invalid {
				require.NoError(t, unstructured.SetNestedSlice(obj.Object, tc.refs, "spec", "gatewayRefs"))
				err := kube.Update(ctx, obj)
				require.True(t, apierrors.IsInvalid(err), "update %s: %v", tc.name, err)
			}
		}
	})

	t.Run("provider namespaces", func(t *testing.T) {
		for i, namespace := range []string{"", "models", "shared-providers"} {
			obj := schemaModel(fmt.Sprintf("provider-namespace-%d", i))
			obj.Spec.ExternalProviderRefs[0].Ref.Namespace = namespace
			require.NoError(t, kube.Create(ctx, obj))
			require.NoError(t, kube.Get(ctx, client.ObjectKeyFromObject(obj), obj))
			assert.Equal(t, namespace, obj.Spec.ExternalProviderRefs[0].Ref.Namespace)
		}
		for i, namespace := range []string{"", "Uppercase", "has.dot", strings.Repeat("a", 64)} {
			obj := rawSchemaModel(t, fmt.Sprintf("invalid-provider-namespace-%d", i))
			refs, _, err := unstructured.NestedSlice(obj.Object, "spec", "externalProviderRefs")
			require.NoError(t, err)
			ref, ok := refs[0].(map[string]any)
			require.True(t, ok)
			require.NoError(t, unstructured.SetNestedField(ref, namespace, "ref", "namespace"))
			require.NoError(t, unstructured.SetNestedSlice(obj.Object, refs, "spec", "externalProviderRefs"))
			err = kube.Create(ctx, obj)
			require.True(t, apierrors.IsInvalid(err), "namespace %q: %v", namespace, err)
		}
	})

	t.Run("gateway status round trips", func(t *testing.T) {
		obj := schemaModel("gateway-status")
		obj.Spec.GatewayRefs = []NamespacedObjectReference{{Name: "shared", Namespace: "tenant-a"}, {Name: "shared", Namespace: "tenant-b"}}
		require.NoError(t, kube.Create(ctx, obj))
		condition.ObservedGeneration = obj.Generation
		failed := condition
		failed.Status, failed.Reason = metav1.ConditionFalse, "AttachmentFailed"
		obj.Status = ExternalModelStatus{
			Phase: "Failed", ObservedGeneration: obj.Generation, OverlayDigest: "sha256:test", OverlayGeneration: 8,
			Conditions: []metav1.Condition{failed},
			Gateways: []ExternalModelGatewayStatus{
				{NamespacedObjectReference: obj.Spec.GatewayRefs[0], HTTPRouteRef: &NamespacedObjectReference{Name: "route", Namespace: "routes"}, Conditions: []metav1.Condition{condition}},
				{NamespacedObjectReference: obj.Spec.GatewayRefs[1], Conditions: []metav1.Condition{failed}},
			},
		}
		require.NoError(t, kube.Status().Update(ctx, obj))
		expected := obj.Status.DeepCopy()
		require.NoError(t, kube.Get(ctx, client.ObjectKeyFromObject(obj), obj))
		assert.Equal(t, *expected, obj.Status)
		// Spec updates must preserve status, including conditions that become stale.
		obj.Spec.ModelName = "new-model-name"
		require.NoError(t, kube.Update(ctx, obj))
		require.NoError(t, kube.Get(ctx, client.ObjectKeyFromObject(obj), obj))
		assert.Greater(t, obj.Generation, expected.ObservedGeneration)
		assert.Equal(t, *expected, obj.Status)
		valid := obj.DeepCopy()
		for _, tc := range []struct {
			name   string
			mutate func(*ExternalModelStatus)
		}{
			{"duplicate gateway", func(status *ExternalModelStatus) { status.Gateways = append(status.Gateways, status.Gateways[0]) }},
			{"duplicate condition", func(status *ExternalModelStatus) {
				status.Gateways[0].Conditions = append(status.Gateways[0].Conditions, condition)
			}},
			{"missing route namespace", func(status *ExternalModelStatus) { status.Gateways[0].HTTPRouteRef.Namespace = "" }},
			{"missing gateway name", func(status *ExternalModelStatus) { status.Gateways[0].Name = "" }},
			{"negative generation", func(status *ExternalModelStatus) { status.Gateways[0].Conditions[0].ObservedGeneration = -1 }},
			{"invalid condition", func(status *ExternalModelStatus) { status.Gateways[0].Conditions[0].Status = "Invalid" }},
		} {
			t.Run(tc.name, func(t *testing.T) {
				invalid := valid.DeepCopy()
				tc.mutate(&invalid.Status)
				err := kube.Status().Update(ctx, invalid)
				require.True(t, apierrors.IsInvalid(err), "expected invalid status, got %v", err)
			})
		}
	})
}
