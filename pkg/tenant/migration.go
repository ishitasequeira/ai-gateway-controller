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
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// PayloadProcessingStatus returns AnnotationPayloadProcessingStatus.
// Empty means absent (legacy steady when legacy is selected; blocked when
// praxis is selected — existing tenants are assumed to run legacy IPP).
func PayloadProcessingStatus(mtc *unstructured.Unstructured) string {
	annotations := mtc.GetAnnotations()
	if annotations == nil {
		return ""
	}
	return annotations[AnnotationPayloadProcessingStatus]
}

// LegacyServes reports whether maas-controller's legacy IPP still serves the
// tenant: IPP is selected, or Praxis is selected but the handoff has not
// started. With Praxis selected, an absent status means maas-controller has
// not cleaned up IPP yet, or its migration gate blocks the switch; IPP keeps
// serving the tenant's models and reporting their status meanwhile.
func LegacyServes(mtc *unstructured.Unstructured) bool {
	return !UsesPraxis(mtc) || PayloadProcessingStatus(mtc) == ""
}

// EnsurePraxisMayDeploy decides whether praxis may render/apply for mtc.
//
//	steady           → ready (resume / steady-state)
//	cleanup-complete → CAS-claim to steady, then ready
//	absent / other   → wait (legacy still owns, or cleanup not signaled yet)
//
// A false ready with a nil error means the caller should requeue shortly.
func EnsurePraxisMayDeploy(ctx context.Context, c client.Client, mtc *unstructured.Unstructured) (ready bool, err error) {
	switch PayloadProcessingStatus(mtc) {
	case PayloadProcessingStatusSteady:
		return true, nil
	case PayloadProcessingStatusCleanupComplete:
		return claimPraxisSteady(ctx, c, mtc)
	default:
		return false, nil
	}
}

// claimPraxisSteady atomically consumes cleanup-complete by writing
// AnnotationPayloadProcessingStatus=steady via an optimistic-concurrency
// Update. Re-validates UsesPraxis on the fresh read so a legacy switch that
// landed before this Get (same resourceVersion as the claim write) cannot
// produce a praxis claim. A false return means another party claimed first,
// the backend is no longer praxis, or the status is no longer clear — caller
// must wait.
func claimPraxisSteady(ctx context.Context, c client.Client, mtc *unstructured.Unstructured) (claimed bool, err error) {
	latest := &unstructured.Unstructured{}
	latest.SetGroupVersionKind(mtc.GroupVersionKind())
	if err := c.Get(ctx, client.ObjectKeyFromObject(mtc), latest); err != nil {
		return false, fmt.Errorf("get MaasTenantConfig for payload-processing status claim: %w", err)
	}
	if !UsesPraxis(latest) || PayloadProcessingStatus(latest) != PayloadProcessingStatusCleanupComplete {
		return false, nil
	}
	setPayloadProcessingStatus(latest, PayloadProcessingStatusSteady)
	if err := c.Update(ctx, latest); err != nil {
		if apierrors.IsConflict(err) {
			return false, nil
		}
		return false, fmt.Errorf("claim payload-processing status to steady: %w", err)
	}
	// Keep the caller's copy in sync for subsequent patches in this reconcile.
	setPayloadProcessingStatus(mtc, PayloadProcessingStatusSteady)
	return true, nil
}

// MarkPayloadProcessingCleanupComplete is this controller's switch-off "done"
// signal: after a full praxis-extproc cleanup, write cleanup-complete so
// maas-controller may claim (delete to absent) and redeploy legacy IPP.
func MarkPayloadProcessingCleanupComplete(ctx context.Context, c client.Client, mtc *unstructured.Unstructured) error {
	if PayloadProcessingStatus(mtc) == PayloadProcessingStatusCleanupComplete {
		return nil
	}
	base := mtc.DeepCopy()
	setPayloadProcessingStatus(mtc, PayloadProcessingStatusCleanupComplete)
	return c.Patch(ctx, mtc, client.MergeFrom(base))
}

func setPayloadProcessingStatus(mtc *unstructured.Unstructured, value string) {
	annotations := mtc.GetAnnotations()
	if annotations == nil {
		annotations = make(map[string]string)
	}
	if value == "" {
		delete(annotations, AnnotationPayloadProcessingStatus)
	} else {
		annotations[AnnotationPayloadProcessingStatus] = value
	}
	mtc.SetAnnotations(annotations)
}
