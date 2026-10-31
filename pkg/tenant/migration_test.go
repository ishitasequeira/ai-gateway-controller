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
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func newMTCFixture(namespace string, annotations map[string]string) *unstructured.Unstructured {
	u := NewMaasTenantConfig()
	u.SetName(MaasTenantConfigInstanceName)
	u.SetNamespace(namespace)
	u.SetAnnotations(annotations)
	return u
}

func TestPayloadProcessingStatus(t *testing.T) {
	t.Run("absent", func(t *testing.T) {
		if got := PayloadProcessingStatus(newMTCFixture("ns", nil)); got != "" {
			t.Fatalf("got %q, want empty", got)
		}
	})
	t.Run("cleanup-complete", func(t *testing.T) {
		mtc := newMTCFixture("ns", map[string]string{AnnotationPayloadProcessingStatus: PayloadProcessingStatusCleanupComplete})
		if got := PayloadProcessingStatus(mtc); got != PayloadProcessingStatusCleanupComplete {
			t.Fatalf("got %q, want %q", got, PayloadProcessingStatusCleanupComplete)
		}
	})
	t.Run("steady", func(t *testing.T) {
		mtc := newMTCFixture("ns", map[string]string{AnnotationPayloadProcessingStatus: PayloadProcessingStatusSteady})
		if got := PayloadProcessingStatus(mtc); got != PayloadProcessingStatusSteady {
			t.Fatalf("got %q, want %q", got, PayloadProcessingStatusSteady)
		}
	})
}

func TestEnsurePraxisMayDeploy(t *testing.T) {
	scheme := mtcSchemeForTests()

	t.Run("steady is ready", func(t *testing.T) {
		mtc := newMTCFixture("ns", map[string]string{AnnotationPayloadProcessingStatus: PayloadProcessingStatusSteady})
		cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(mtc).Build()
		ready, err := EnsurePraxisMayDeploy(context.Background(), cl, mtc)
		if err != nil {
			t.Fatalf("EnsurePraxisMayDeploy: %v", err)
		}
		if !ready {
			t.Fatal("ready = false, want true")
		}
	})

	t.Run("cleanup-complete claims to steady", func(t *testing.T) {
		mtc := newMTCFixture("ns-claim", map[string]string{
			AnnotationPayloadProcessingType:   PayloadProcessingBackendPraxis,
			AnnotationPayloadProcessingStatus: PayloadProcessingStatusCleanupComplete,
		})
		cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(mtc).Build()
		ready, err := EnsurePraxisMayDeploy(context.Background(), cl, mtc)
		if err != nil {
			t.Fatalf("EnsurePraxisMayDeploy: %v", err)
		}
		if !ready {
			t.Fatal("ready = false, want true after claim")
		}
		var got unstructured.Unstructured
		got.SetGroupVersionKind(MaasTenantConfigGVK)
		if err := cl.Get(context.Background(), client.ObjectKeyFromObject(mtc), &got); err != nil {
			t.Fatalf("Get: %v", err)
		}
		if PayloadProcessingStatus(&got) != PayloadProcessingStatusSteady {
			t.Fatalf("status = %q, want %q", PayloadProcessingStatus(&got), PayloadProcessingStatusSteady)
		}
	})

	t.Run("absent waits", func(t *testing.T) {
		mtc := newMTCFixture("ns-wait", nil)
		cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(mtc).Build()
		ready, err := EnsurePraxisMayDeploy(context.Background(), cl, mtc)
		if err != nil {
			t.Fatalf("EnsurePraxisMayDeploy: %v", err)
		}
		if ready {
			t.Fatal("ready = true, want false while waiting on legacy cleanup")
		}
	})

	t.Run("concurrent claim attempts", func(t *testing.T) {
		mtc := newMTCFixture("ns-race", map[string]string{
			AnnotationPayloadProcessingType:   PayloadProcessingBackendPraxis,
			AnnotationPayloadProcessingStatus: PayloadProcessingStatusCleanupComplete,
		})
		cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(mtc).Build()

		readerA := mtc.DeepCopy()
		readerB := mtc.DeepCopy()
		claimedA, errA := claimPraxisSteady(context.Background(), cl, readerA)
		claimedB, errB := claimPraxisSteady(context.Background(), cl, readerB)
		if errA != nil || errB != nil {
			t.Fatalf("claim errors: %v %v", errA, errB)
		}
		if claimedA == claimedB {
			t.Fatalf("exactly one claimer should win, got claimedA=%v claimedB=%v", claimedA, claimedB)
		}
	})

	t.Run("refuses claim when backend flipped to legacy before Get", func(t *testing.T) {
		// Caller's reconcile snapshot still looks like praxis + cleanup-complete,
		// but the live object already selected legacy (same RV until claim Update).
		caller := newMTCFixture("ns-flip", map[string]string{
			AnnotationPayloadProcessingType:   PayloadProcessingBackendPraxis,
			AnnotationPayloadProcessingStatus: PayloadProcessingStatusCleanupComplete,
		})
		live := caller.DeepCopy()
		annotations := live.GetAnnotations()
		annotations[AnnotationPayloadProcessingType] = PayloadProcessingBackendIPP // legacy selection
		live.SetAnnotations(annotations)
		cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(live).Build()

		claimed, err := claimPraxisSteady(context.Background(), cl, caller)
		if err != nil {
			t.Fatalf("claimPraxisSteady: %v", err)
		}
		if claimed {
			t.Fatal("claimed = true, want false when live object no longer selects praxis")
		}
		var got unstructured.Unstructured
		got.SetGroupVersionKind(MaasTenantConfigGVK)
		if err := cl.Get(context.Background(), client.ObjectKeyFromObject(live), &got); err != nil {
			t.Fatalf("Get: %v", err)
		}
		if PayloadProcessingStatus(&got) != PayloadProcessingStatusCleanupComplete {
			t.Fatalf("status = %q, want %q (must not write steady for legacy)", PayloadProcessingStatus(&got), PayloadProcessingStatusCleanupComplete)
		}
	})
}

func TestMarkPayloadProcessingCleanupComplete(t *testing.T) {
	scheme := mtcSchemeForTests()
	mtc := newMTCFixture("ns-mark", map[string]string{AnnotationPayloadProcessingStatus: PayloadProcessingStatusSteady})
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(mtc).Build()

	if err := MarkPayloadProcessingCleanupComplete(context.Background(), cl, mtc); err != nil {
		t.Fatalf("MarkPayloadProcessingCleanupComplete: %v", err)
	}
	var got unstructured.Unstructured
	got.SetGroupVersionKind(MaasTenantConfigGVK)
	if err := cl.Get(context.Background(), client.ObjectKeyFromObject(mtc), &got); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if v := PayloadProcessingStatus(&got); v != PayloadProcessingStatusCleanupComplete {
		t.Fatalf("status = %q, want %q", v, PayloadProcessingStatusCleanupComplete)
	}
}

func TestLegacyServes(t *testing.T) {
	for _, tc := range []struct {
		backend, status string
		want            bool
	}{
		{backend: PayloadProcessingBackendIPP, want: true},
		{backend: PayloadProcessingBackendIPP, status: PayloadProcessingStatusCleanupComplete, want: true},
		// Praxis selected, handoff not started: maas-controller's gate may block it.
		{backend: "", want: true},
		{backend: PayloadProcessingBackendPraxis, want: true},
		{backend: PayloadProcessingBackendPraxis, status: PayloadProcessingStatusCleanupComplete, want: false},
		{backend: "", status: PayloadProcessingStatusSteady, want: false},
	} {
		mtc := NewMaasTenantConfig()
		annotations := map[string]string{}
		if tc.backend != "" {
			annotations[AnnotationPayloadProcessingType] = tc.backend
		}
		if tc.status != "" {
			annotations[AnnotationPayloadProcessingStatus] = tc.status
		}
		mtc.SetAnnotations(annotations)
		if got := LegacyServes(mtc); got != tc.want {
			t.Errorf("LegacyServes(type=%q, status=%q) = %t, want %t", tc.backend, tc.status, got, tc.want)
		}
	}
}
