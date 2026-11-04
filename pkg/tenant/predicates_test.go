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
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"

	"github.com/opendatahub-io/ai-gateway-controller/pkg/render"
)

func aitenantForPredicate(t *testing.T, mutate func(*unstructured.Unstructured)) *unstructured.Unstructured {
	t.Helper()
	aitenant := NewAITenant()
	aitenant.SetNamespace("ai-tenants")
	aitenant.SetName("tenant-a")
	if mutate != nil {
		mutate(aitenant)
	}
	return aitenant
}

func mtcForPredicate(mutate func(*unstructured.Unstructured)) *unstructured.Unstructured {
	mtc := NewMaasTenantConfig()
	mtc.SetNamespace("e2e-tenant-a")
	mtc.SetName("maas-tenant-config")
	if mutate != nil {
		mutate(mtc)
	}
	return mtc
}

func updateFor(old, updated client.Object) event.UpdateEvent {
	return event.UpdateEvent{ObjectOld: old, ObjectNew: updated}
}

func TestAITenantPredicate(t *testing.T) {
	preds := tenantEventPredicates()["aitenant"]

	cases := []struct {
		name  string
		old   func(*unstructured.Unstructured)
		new   func(*unstructured.Unstructured)
		admit bool
	}{
		{
			name:  "spec change admits",
			old:   func(u *unstructured.Unstructured) { u.SetGeneration(1) },
			new:   func(u *unstructured.Unstructured) { u.SetGeneration(2) },
			admit: true,
		},
		{
			name: "tenantNamespace transition admits",
			old:  func(*unstructured.Unstructured) {},
			new: func(u *unstructured.Unstructured) {
				if err := unstructured.SetNestedField(u.Object, "e2e-tenant-a", "status", "tenantNamespace"); err != nil {
					t.Fatal(err)
				}
			},
			admit: true,
		},
		{
			name: "gatewayRef change admits",
			old: func(u *unstructured.Unstructured) {
				if err := unstructured.SetNestedField(u.Object, "gw-old", "status", "gatewayRef", "name"); err != nil {
					t.Fatal(err)
				}
			},
			new: func(u *unstructured.Unstructured) {
				if err := unstructured.SetNestedField(u.Object, "gw-new", "status", "gatewayRef", "name"); err != nil {
					t.Fatal(err)
				}
			},
			admit: true,
		},
		{
			name: "unrelated status write is dropped",
			old:  func(*unstructured.Unstructured) {},
			new: func(u *unstructured.Unstructured) {
				if err := unstructured.SetNestedField(u.Object, "something", "status", "unrelated"); err != nil {
					t.Fatal(err)
				}
			},
			admit: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := preds.Update(updateFor(aitenantForPredicate(t, tc.old), aitenantForPredicate(t, tc.new)))
			if got != tc.admit {
				t.Errorf("admitted = %v, want %v", got, tc.admit)
			}
		})
	}
}

func TestMaasTenantConfigPredicate(t *testing.T) {
	preds := tenantEventPredicates()["maastenantconfig"]

	cases := []struct {
		name  string
		old   func(*unstructured.Unstructured)
		new   func(*unstructured.Unstructured)
		admit bool
	}{
		{
			name:  "spec change admits",
			old:   func(u *unstructured.Unstructured) { u.SetGeneration(1) },
			new:   func(u *unstructured.Unstructured) { u.SetGeneration(2) },
			admit: true,
		},
		{
			name: "backend annotation flip admits",
			old: func(u *unstructured.Unstructured) {
				u.SetAnnotations(map[string]string{AnnotationPayloadProcessingType: PayloadProcessingBackendIPP})
			},
			new: func(u *unstructured.Unstructured) {
				u.SetAnnotations(map[string]string{AnnotationPayloadProcessingType: "praxis"})
			},
			admit: true,
		},
		{
			name: "status-only update is dropped",
			old:  func(*unstructured.Unstructured) {},
			new: func(u *unstructured.Unstructured) {
				if err := unstructured.SetNestedField(u.Object, "Ready", "status", "phase"); err != nil {
					t.Fatal(err)
				}
			},
			admit: false,
		},
		{
			name:  "annotation churn on other annotations is dropped",
			old:   func(*unstructured.Unstructured) {},
			new:   func(u *unstructured.Unstructured) { u.SetLabels(map[string]string{"unrelated": "bump"}) },
			admit: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := preds.Update(updateFor(mtcForPredicate(tc.old), mtcForPredicate(tc.new)))
			if got != tc.admit {
				t.Errorf("admitted = %v, want %v", got, tc.admit)
			}
		})
	}
}

func TestNotControllerOwnedSecret(t *testing.T) {
	owned := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "agc-cred-x", Labels: map[string]string{managedByLabel: render.FieldOwner}}}
	foreign := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "source-api-key"}}

	if notControllerOwnedSecret(owned) {
		t.Error("controller-owned secret admitted; want excluded")
	}
	if !notControllerOwnedSecret(foreign) {
		t.Error("foreign secret excluded; want admitted")
	}
}

func TestTenantSecretPredicate(t *testing.T) {
	preds := tenantSecretPredicate()
	owned := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "agc-cred-x", Labels: map[string]string{managedByLabel: render.FieldOwner}}}
	foreign := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "source-api-key"}}

	if preds.Create(event.CreateEvent{Object: owned}) {
		t.Error("create of controller-owned secret admitted; want excluded")
	}
	if !preds.Create(event.CreateEvent{Object: foreign}) {
		t.Error("create of foreign secret excluded; want admitted")
	}
	if preds.Update(updateFor(owned, owned)) {
		t.Error("update of controller-owned secret admitted; want excluded")
	}
	if !preds.Update(updateFor(foreign, foreign)) {
		t.Error("update of foreign secret excluded; want admitted")
	}
	// Deletes are always admitted: a credential copy is removed by the
	// attachment reconciler acting on the model in the model's namespace, so
	// the Secret delete is the only tenant-side trigger to drop the candidate
	// from the rendered stack. A foreign source being deleted must equally
	// reconcile.
	for _, obj := range []client.Object{owned, foreign} {
		if !preds.Delete(event.DeleteEvent{Object: obj}) {
			t.Errorf("delete of %s excluded; want admitted", obj.GetName())
		}
	}
}
