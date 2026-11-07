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

import "knative.dev/pkg/kmeta"

// ID returns the resource-naming tenant identifier for an AITenant name.
// The default/legacy AITenant keeps the empty identifier so its resources
// stay unsuffixed (mirrors maas-controller's tenantreconcile.TenantIdentifierFor
// treatment of the default tenant); every other AITenant name is used as-is.
func ID(aitenantName string) string {
	if aitenantName == DefaultAITenantName {
		return ""
	}
	return aitenantName
}

// ResourceName returns base for the default tenant ("") and "base-tenantID"
// otherwise (mirrors tenantreconcile.resourceNameForTenant).
func ResourceName(base, tenantID string) string {
	if tenantID == "" {
		return base
	}
	return kmeta.ChildName(base+"-", tenantID)
}

func PayloadProcessingDeploymentName(tenantID string) string {
	return ResourceName(PayloadProcessingName, tenantID)
}

func PayloadPreProcessingDeploymentName(tenantID string) string {
	return ResourceName(PayloadPreProcessingName, tenantID)
}

func PayloadProcessingServiceName(tenantID string) string {
	return ResourceName(PayloadProcessingName, tenantID)
}

func PayloadPreProcessingServiceName(tenantID string) string {
	return ResourceName(PayloadPreProcessingName, tenantID)
}

func PayloadProcessingServiceAccountName(tenantID string) string {
	return ResourceName(PayloadProcessingName, tenantID)
}

// PayloadProcessingPostServiceAccountName is the gateway-namespace post-auth
// ExtProc's ServiceAccount. It is intentionally distinct from MaaS's
// payload-processing ServiceAccount, which the pre-auth ExtProc keeps: MaaS's
// shared IPP reader binding may grant that account Secret access, while the
// post-auth ExtProc only needs projected volumes. It holds no binding but the
// NeMo caller grants, so the token it sends to an authenticated NeMo server
// carries nothing else.
func PayloadProcessingPostServiceAccountName(tenantID string) string {
	return ResourceName(PayloadProcessingName+"-post", tenantID)
}

// PayloadProcessingExternalModelServiceAccountName is the credential-bearing
// account for the ExternalModel-only header-phase ExtProc. It is separate from
// both the pre-auth account and MaaS's existing post-auth account.
func PayloadProcessingExternalModelServiceAccountName(tenantID string) string {
	return ResourceName(PayloadProcessingExternalModelName, tenantID)
}

func PayloadProcessingExternalModelDeploymentName(tenantID string) string {
	return ResourceName(PayloadProcessingExternalModelName, tenantID)
}

func PayloadProcessingExternalModelServiceName(tenantID string) string {
	return ResourceName(PayloadProcessingExternalModelName, tenantID)
}

// PayloadProcessingTLSSecretName is the OpenShift service-ca secret annotated
// onto the shared post-auth ExtProc Service.
func PayloadProcessingTLSSecretName(tenantID string) string {
	return PayloadProcessingServiceName(tenantID) + "-tls"
}

// PayloadPreProcessingTLSSecretName is the OpenShift service-ca secret for the
// pre-auth ExtProc Service.
func PayloadPreProcessingTLSSecretName(tenantID string) string {
	return PayloadPreProcessingServiceName(tenantID) + "-tls"
}

// PayloadProcessingExternalModelTLSSecretName is the OpenShift service-ca
// secret for the ExternalModel ExtProc Service.
func PayloadProcessingExternalModelTLSSecretName(tenantID string) string {
	return PayloadProcessingExternalModelServiceName(tenantID) + "-tls"
}

func PayloadProcessingExternalModelPluginsConfigMapForTenant(tenantID string) string {
	return ResourceName(PayloadProcessingExternalModelName+"-plugins", tenantID)
}

func PayloadProcessingNetworkPolicyName(tenantID string) string {
	return ResourceName(PayloadProcessingName, tenantID)
}

func PayloadProcessingEnvoyFilterName(tenantID string) string {
	return ResourceName(PayloadProcessingName, tenantID)
}

func PayloadProcessingHPAName(tenantID string) string {
	return ResourceName(PayloadProcessingName, tenantID)
}

func PayloadPreProcessingHPAName(tenantID string) string {
	return ResourceName(PayloadPreProcessingName, tenantID)
}

// PayloadProcessingExternalModelEnvoyFilterName is owned exclusively by the
// ExternalModel reconciler. Keeping route-specific patches separate from the
// tenant-owned base EnvoyFilter prevents concurrent reconcilers from losing
// each other's configPatches.
func PayloadProcessingExternalModelEnvoyFilterName(tenantID string) string {
	return ResourceName(PayloadProcessingExternalModelName+"-routes", tenantID)
}

// PayloadProcessingExternalModelFilterNameForTenant returns the name of the
// controller-owned static ExternalModel filter resource for a tenant.
func PayloadProcessingExternalModelFilterNameForTenant(tenantID string) string {
	return ResourceName(PayloadProcessingExternalModelFilterName, tenantID)
}

func PayloadProcessingPluginsConfigMapForTenant(tenantID string) string {
	return ResourceName(PayloadProcessingPluginsConfigMapName, tenantID)
}

func PayloadProcessingReaderClusterRoleBindingNameForTenant(tenantID string) string {
	return ResourceName(PayloadProcessingReaderClusterRoleBindingName, tenantID)
}

func PayloadProcessingReaderClusterRoleBindingPostNameForTenant(tenantID string) string {
	return ResourceName(PayloadProcessingReaderClusterRoleBindingName+"-post", tenantID)
}

func PayloadProcessingMetricsAuthClusterRoleBindingNameForTenant(tenantID string) string {
	return ResourceName(PayloadProcessingMetricsAuthName, tenantID)
}

func PayloadProcessingMetricsServiceNameForTenant(tenantID string) string {
	return ResourceName(PayloadProcessingMetricsServiceName, tenantID)
}

func PayloadProcessingMetricsMonitorNameForTenant(tenantID string) string {
	return ResourceName(PayloadProcessingMetricsMonitorName, tenantID)
}
