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
	"errors"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// NewMaasTenantConfig returns an empty unstructured object with the
// MaasTenantConfig GVK set, ready for Get or for use with the controller-
// runtime builder. This is this controller's primary watch target (mirrors
// maas-controller's TenantReconciler watching the same object).
func NewMaasTenantConfig() *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(MaasTenantConfigGVK)
	return u
}

// PayloadProcessingType reads the AnnotationPayloadProcessingType annotation
// from a MaasTenantConfig.
func PayloadProcessingType(mtc *unstructured.Unstructured) string {
	return mtc.GetAnnotations()[AnnotationPayloadProcessingType]
}

// UsesPraxis reports whether this controller owns payload processing for the
// MaasTenantConfig. Mirrors maas-controller's resolveSkipIPP: absent,
// "praxis", or any unrecognized value defaults to praxis; only
// PayloadProcessingBackendIPP opts into legacy IPP.
func UsesPraxis(mtc *unstructured.Unstructured) bool {
	switch PayloadProcessingType(mtc) {
	case PayloadProcessingBackendIPP:
		return false
	default:
		return true
	}
}

// APITranslationEnabled reads the authoritative MaaS-owned translation
// permission. Missing fields are default-off. Malformed fields fail closed so
// an older or incomplete installed CRD cannot silently enable translation.
func APITranslationEnabled(mtc *unstructured.Unstructured) (bool, error) {
	if mtc == nil {
		return false, errors.New("MaasTenantConfig is required to read apiTranslation")
	}
	value, found, err := unstructured.NestedFieldNoCopy(mtc.Object, "spec", "apiTranslation")
	if err != nil {
		return false, fmt.Errorf("read spec.apiTranslation: %w", err)
	}
	if !found {
		return false, nil
	}
	settings, ok := value.(map[string]any)
	if !ok {
		return false, fmt.Errorf("spec.apiTranslation must be an object, got %T", value)
	}
	enabled, found := settings["enabled"]
	if !found {
		return false, nil
	}
	flag, ok := enabled.(bool)
	if !ok {
		return false, fmt.Errorf("spec.apiTranslation.enabled must be a boolean, got %T", enabled)
	}
	return flag, nil
}

// OutputChecksUnsupported returns why the tenant configured in namespace
// cannot enforce Output-phase guardrail checks, or "" if it can. With API
// translation enabled the shared post-auth ExtProc streams responses, and the
// shipped Praxis image skips response checks in that mode.
func OutputChecksUnsupported(ctx context.Context, reader client.Reader, namespace string) (string, error) {
	mtc := NewMaasTenantConfig()
	if err := reader.Get(ctx, client.ObjectKey{Namespace: namespace, Name: MaasTenantConfigInstanceName}, mtc); err != nil {
		if apierrors.IsNotFound(err) {
			return "", nil
		}
		return "", fmt.Errorf("get MaasTenantConfig %s/%s: %w", namespace, MaasTenantConfigInstanceName, err)
	}
	translated, err := APITranslationEnabled(mtc)
	if err != nil || !translated {
		return "", err
	}
	return "API translation is enabled for this tenant, so its ExtProc streams responses, which the current " +
		"Praxis release does not check; use Input-phase checks only, or disable API translation", nil
}

// IdentifierFor derives the per-tenant resource-naming identifier from
// a MaasTenantConfig's labels, mirroring maas-controller's
// tenantreconcile.TenantIdentifierFor: the default/legacy tenant (unlabeled,
// or labeled with DefaultAITenantName) returns "" so its resources keep the
// unsuffixed names Phase 1 always used; every other AITenant-managed tenant
// returns its tenant name for "{base}-{tenantID}" naming.
func IdentifierFor(mtc *unstructured.Unstructured) string {
	labels := mtc.GetLabels()
	if labels == nil || labels[LabelManagedByAITenant] != "true" {
		return ""
	}
	name := labels[LabelTenantName]
	if name == DefaultAITenantName {
		return ""
	}
	return name
}

// OwningAITenantRef reads the AnnotationAITenantName / AnnotationAITenantNamespace
// annotations maas-controller's AITenantReconciler stamps onto every
// AITenant-managed MaasTenantConfig. ok is false until both are populated.
// Callers must not treat these annotations as proof of ownership: tenant
// users can patch them, so resolveOwningAITenant also checks
// status.tenantNamespace against the MaasTenantConfig's namespace.
func OwningAITenantRef(mtc *unstructured.Unstructured) (name, namespace string, ok bool) {
	annotations := mtc.GetAnnotations()
	if annotations == nil {
		return "", "", false
	}
	name = annotations[AnnotationAITenantName]
	namespace = annotations[AnnotationAITenantNamespace]
	return name, namespace, name != "" && namespace != ""
}

// ConfigNamespace reads status.tenantNamespace from an AITenant —
// the namespace where maas-controller creates/adopts that tenant's
// MaasTenantConfig/default-tenant object (mirrors maas-controller's
// AITenantReconciler setting aitenant.Status.TenantNamespace). Used to map
// an AITenant watch event back to the MaasTenantConfig this controller
// primarily reconciles, without needing to duplicate maas-controller's
// TenantNamespaceForAITenant naming convention (which depends on a
// configurable default tenant namespace this controller does not know).
func ConfigNamespace(aitenant *unstructured.Unstructured) (namespace string, ok bool) {
	namespace, _, _ = unstructured.NestedString(aitenant.Object, "status", "tenantNamespace")
	return namespace, namespace != ""
}
