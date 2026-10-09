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

import "k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

// NewAITenant returns an empty unstructured object with the AITenant GVK
// set, ready for Get. This controller only ever Gets a specific AITenant by
// name/namespace (to resolve status.gatewayRef / status.phase for the
// MaasTenantConfig it primarily watches).
func NewAITenant() *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(AITenantGVK)
	return u
}

// IsActive reports whether maas-controller's AITenant reconciler has
// finished validating and bootstrapping this tenant (status.phase ==
// "Active": its Gateway is validated, and its namespace, MaasTenantConfig,
// and RBAC exist). Used as the readiness gate instead of GatewayRef alone,
// since status.gatewayRef is populated optimistically from spec before any
// of that validation happens.
func IsActive(aitenant *unstructured.Unstructured) bool {
	phase, _, _ := unstructured.NestedString(aitenant.Object, "status", "phase")
	return phase == AITenantPhaseActive
}

// PlatformPrerequisitesReady reports whether maas-controller has validated the
// tenant's base resources for the current AITenant generation. This is weaker
// than Active by design: it is the permission for the selected dataplane
// controller to create its resources, not a claim that the complete tenant
// dataplane is serving yet.
func PlatformPrerequisitesReady(aitenant *unstructured.Unstructured) bool {
	return statusConditionCurrent(aitenant, AITenantConditionPlatformPrerequisitesReady, true)
}

// GatewayRef reads status.gatewayRef.{name,namespace}. ok is false until
// the AITenant reconciler (maas-controller) has resolved and published the
// Gateway reference; callers should requeue rather than treat that as an
// error.
func GatewayRef(aitenant *unstructured.Unstructured) (name, namespace string, ok bool) {
	name, _, _ = unstructured.NestedString(aitenant.Object, "status", "gatewayRef", "name")
	namespace, _, _ = unstructured.NestedString(aitenant.Object, "status", "gatewayRef", "namespace")
	return name, namespace, name != "" && namespace != ""
}

// StatusIsCurrent reports whether the AITenant's Ready condition was computed
// for the object's current spec generation — i.e. status.phase and
// status.gatewayRef reflect the live spec, not an in-flight older one that
// maas-controller has not reconciled yet. It looks for the
// AITenantConditionReady condition and requires its observedGeneration to
// equal metadata.generation; maas-controller stamps that on every phase
// transition (setAITenantPhase), and AITenantStatus exposes no top-level
// observedGeneration to rely on instead.
//
// Used together with IsActive or PlatformPrerequisitesReady as the generation
// fence: acting on status whose gatewayRef still reflects a superseded
// generation would install praxis-extproc against a stale Gateway. A false
// result is a transient not-ready state (requeue), not an error.
func StatusIsCurrent(aitenant *unstructured.Unstructured) bool {
	return statusConditionCurrent(aitenant, AITenantConditionReady, false)
}

// statusConditionCurrent reports whether a status condition exists for the
// object's current spec generation. When requireTrue is set, the condition
// must also carry status=True.
func statusConditionCurrent(aitenant *unstructured.Unstructured, conditionType string, requireTrue bool) bool {
	conditions, _, _ := unstructured.NestedSlice(aitenant.Object, "status", "conditions")
	for _, entry := range conditions {
		condition, ok := entry.(map[string]any)
		if !ok {
			continue
		}
		condType, _, _ := unstructured.NestedString(condition, "type")
		if condType != conditionType {
			continue
		}
		if requireTrue {
			status, _, _ := unstructured.NestedString(condition, "status")
			if status != "True" {
				return false
			}
		}
		observed, ok := nestedInt64(condition, "observedGeneration")
		return ok && observed == aitenant.GetGeneration()
	}
	return false
}

// nestedInt64 reads an integer field that may have been decoded as int64
// (runtime.DefaultUnstructuredConverter) or float64 (plain encoding/json).
func nestedInt64(m map[string]any, key string) (int64, bool) {
	switch v := m[key].(type) {
	case int64:
		return v, true
	case float64:
		return int64(v), true
	}
	return 0, false
}
