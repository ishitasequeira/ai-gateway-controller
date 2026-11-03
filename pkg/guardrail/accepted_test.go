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
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	aigatewayv1alpha1 "github.com/opendatahub-io/ai-gateway-controller/api/aigateway/v1alpha1"
)

func TestBindingAcceptedAndStale(t *testing.T) {
	policy := func(status metav1.ConditionStatus, observed int64, revision string) *aigatewayv1alpha1.AIGuardrail {
		p := &aigatewayv1alpha1.AIGuardrail{}
		p.Generation = 2
		p.Status.BindingRevision = revision
		p.Status.Conditions = []metav1.Condition{{Type: ConditionAccepted, Status: status, ObservedGeneration: observed}}
		return p
	}
	deleting := policy(metav1.ConditionTrue, 2, "rev")
	now := metav1.Now()
	deleting.DeletionTimestamp = &now
	noCondition := policy(metav1.ConditionTrue, 2, "rev")
	noCondition.Status.Conditions = nil

	for _, c := range []struct {
		name     string
		policy   *aigatewayv1alpha1.AIGuardrail
		accepted bool
		stale    bool
	}{
		{name: "nil", policy: nil},
		{name: "deleting", policy: deleting},
		{name: "no condition", policy: noCondition},
		{name: "false", policy: policy(metav1.ConditionFalse, 2, "")},
		{name: "unknown", policy: policy(metav1.ConditionUnknown, 2, "rev")},
		{name: "false at an older generation", policy: policy(metav1.ConditionFalse, 1, "")},
		{name: "true at an older generation", policy: policy(metav1.ConditionTrue, 1, "rev"), stale: true},
		{name: "true without a revision", policy: policy(metav1.ConditionTrue, 2, "")},
		{name: "accepted", policy: policy(metav1.ConditionTrue, 2, "rev"), accepted: true},
	} {
		t.Run(c.name, func(t *testing.T) {
			if got := BindingAccepted(c.policy); got != c.accepted {
				t.Errorf("BindingAccepted = %v, want %v", got, c.accepted)
			}
			if got := BindingStale(c.policy); got != c.stale {
				t.Errorf("BindingStale = %v, want %v", got, c.stale)
			}
		})
	}
}
