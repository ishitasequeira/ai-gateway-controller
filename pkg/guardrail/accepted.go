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

package guardrail

import (
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	aigatewayv1alpha1 "github.com/opendatahub-io/ai-gateway-controller/api/aigateway/v1alpha1"
)

// BindingAccepted reports whether this generation has an accepted provider
// binding. ObservedGeneration on the condition and a published binding
// revision are both required: a True condition left from an older generation
// is not acceptance.
func BindingAccepted(policy *aigatewayv1alpha1.AIGuardrail) bool {
	condition := acceptedCondition(policy)
	return condition != nil &&
		condition.ObservedGeneration == policy.Generation &&
		policy.Status.BindingRevision != ""
}

// BindingStale reports an accepted policy whose spec has moved on and whose
// status has not. The checks from the last accepted generation are still the
// ones in effect. Dropping them before status catches up would stop enforcing
// a check that is still selected.
func BindingStale(policy *aigatewayv1alpha1.AIGuardrail) bool {
	condition := acceptedCondition(policy)
	return condition != nil && condition.ObservedGeneration != policy.Generation
}

// acceptedCondition returns the Accepted condition when it is True on a policy
// that is not being deleted, whatever generation it was observed at.
func acceptedCondition(policy *aigatewayv1alpha1.AIGuardrail) *metav1.Condition {
	if policy == nil || !policy.DeletionTimestamp.IsZero() {
		return nil
	}
	condition := apimeta.FindStatusCondition(policy.Status.Conditions, ConditionAccepted)
	if condition == nil || condition.Status != metav1.ConditionTrue {
		return nil
	}
	return condition
}
