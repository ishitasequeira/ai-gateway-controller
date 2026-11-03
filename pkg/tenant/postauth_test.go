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
	"slices"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// TestModelRefEventsReachEverySubscribingTenant pins that a MaaSModelRef change
// re-checks its own namespace and each namespace whose subscriptions name it,
// once each, and no namespace whose subscriptions name another model.
func TestModelRefEventsReachEverySubscribingTenant(t *testing.T) {
	model := newMaaSModelRef()
	model.SetName("granite")
	model.SetNamespace("models")

	subscription := func(namespace, name string, modelRefs ...any) *unstructured.Unstructured {
		u := subscriptionWithGuardrails(name, nil, modelRefs)
		u.SetNamespace(namespace)
		return u
	}
	granite := map[string]any{"name": "granite", "namespace": "models"}
	reader := fake.NewClientBuilder().WithScheme(mtcSchemeForTests()).WithObjects(
		model,
		subscription("tenant-a", "team", granite),
		subscription("tenant-b", "first", granite),
		subscription("tenant-b", "second", map[string]any{"name": "other", "namespace": "models"}, granite),
		// Names the model in the subscription's own namespace, which is the
		// model's namespace, so it adds nothing beyond the model's own.
		subscription("models", "local", map[string]any{"name": "granite"}),
		// Same model name, but in the subscription's own namespace.
		subscription("tenant-c", "lookalike", map[string]any{"name": "granite"}),
		subscription("tenant-d", "unrelated", map[string]any{"name": "other", "namespace": "models"}),
	).Build()

	var tenants func(context.Context, client.Reader, client.Object) []string
	for _, src := range PostAuthFilterSources() {
		if u, ok := src.Object.(*unstructured.Unstructured); ok && u.GroupVersionKind() == MaaSModelRefGVK {
			tenants = src.Tenants
		}
	}
	if tenants == nil {
		t.Fatal("PostAuthFilterSources has no MaaSModelRef source")
	}

	got := tenants(context.Background(), reader, model)
	slices.Sort(got)
	if want := []string{"models", "tenant-a", "tenant-b"}; !slices.Equal(got, want) {
		t.Fatalf("tenants = %v, want %v", got, want)
	}
}
