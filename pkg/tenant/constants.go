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

// Package tenant watches MaasTenantConfig CRs (owned by maas-controller) and,
// for every tenant whose payload-processing annotation selects Praxis, renders
// and SSA-applies a dedicated per-tenant copy of the vendored praxis-extproc
// manifests into that tenant's Gateway namespace. AITenant is resolved only
// for status, ownership, and Gateway identity.
//
// This package deliberately reads both resources via unstructured +
// schema.GroupVersionKind rather than importing
// models-as-a-service/maas-controller's Go types: that module's go.mod pulls
// in kserve, knative, KEDA, openshift/api, and more, none of which this
// controller needs, and DESIGN.md's stated goal is to keep this repo's
// dependency graph minimal. Their on-wire JSON shapes are the only contracts
// this package relies on.
package tenant

import "k8s.io/apimachinery/pkg/runtime/schema"

// AITenantGVK identifies the AITenant CRD (maas-controller
// api/maas/v1alpha1.AITenant). This controller only Gets a tenant's owning
// AITenant by name/namespace to read status.gatewayRef and status.phase,
// neither of which live on MaasTenantConfig.
var AITenantGVK = schema.GroupVersionKind{
	Group:   "maas.opendatahub.io",
	Version: "v1alpha1",
	Kind:    "AITenant",
}

// MaasTenantConfigGVK identifies the MaasTenantConfig CRD (maas-controller
// api/maas/v1alpha1.MaasTenantConfig). This is this controller's primary
// watch target, mirroring maas-controller's own TenantReconciler: both
// controllers react to the same object, so a payload-processing backend
// swap (see AnnotationPayloadProcessingType) is a single write both sides
// observe from the same watch stream, with no cross-object propagation lag.
var MaasTenantConfigGVK = schema.GroupVersionKind{
	Group:   "maas.opendatahub.io",
	Version: "v1alpha1",
	Kind:    "MaasTenantConfig",
}

const (
	// MaasTenantConfigInstanceName is the singleton resource name enforced by
	// the MaasTenantConfig CRD (mirrors maasv1alpha1.MaasTenantConfigInstanceName).
	MaasTenantConfigInstanceName = "default-tenant"

	// AnnotationPayloadProcessingType selects a tenant's payload-processing
	// dataplane. Both this controller and maas-controller always read the
	// same single source of truth (mirrors maas-controller's
	// tenantreconcile.AnnotationPayloadProcessingType). Default (absent or
	// "praxis") means this controller owns praxis; set to
	// PayloadProcessingBackendIPP to opt back into maas-controller legacy IPP.
	AnnotationPayloadProcessingType = "maas.opendatahub.io/payload-processing-type"

	// AnnotationPayloadProcessingStatus coordinates the payload-processing
	// backend swap handshake with maas-controller. It lives only on
	// MaasTenantConfig (mirrors maas-controller's tenantreconcile.
	// AnnotationPayloadProcessingStatus). Semantics:
	//
	//   - PayloadProcessingStatusCleanupComplete ("cleanup-complete"): clear
	//     to claim. The party currently selected by AnnotationPayloadProcessingType
	//     may CAS-claim and start deploying.
	//   - PayloadProcessingStatusSteady ("steady"): praxis owns / may resume
	//     apply. Legacy must wait until praxis switch-off writes cleanup-complete.
	//   - absent: legacy steady when legacy is selected (existing tenants are
	//     assumed to run legacy IPP); blocked when praxis is selected (wait
	//     for legacy cleanup to write cleanup-complete). New MaasTenantConfigs
	//     are seeded with cleanup-complete at creation time by maas-controller
	//     so a brand-new tenant's first deploy is never blocked by absent.
	AnnotationPayloadProcessingStatus = "maas.opendatahub.io/payload-processing-status"

	// PayloadProcessingStatusCleanupComplete means peer cleanup finished; the
	// selected party may claim.
	PayloadProcessingStatusCleanupComplete = "cleanup-complete"

	// PayloadProcessingStatusSteady means praxis has claimed and may
	// deploy/resume. Legacy treats this as blocked.
	PayloadProcessingStatusSteady = "steady"

	// AnnotationAITenantName and AnnotationAITenantNamespace identify the
	// AITenant that owns a MaasTenantConfig (mirrors maas-controller's
	// tenantreconcile.AnnotationAITenantName / AnnotationAITenantNamespace,
	// set by AITenantReconciler.applyAITenantMetadata). Used to resolve the
	// owning AITenant for status.gatewayRef / status.phase, neither of which
	// live on MaasTenantConfig itself.
	AnnotationAITenantName      = "maas.opendatahub.io/aitenant-name"
	AnnotationAITenantNamespace = "maas.opendatahub.io/aitenant-namespace"

	// LabelManagedByAITenant and LabelTenantName mirror maas-controller's
	// tenantreconcile.LabelManagedByAITenant / LabelTenantName, set by
	// AITenantReconciler.applyAITenantMetadata. Used to derive the per-tenant
	// resource-naming identifier the same way maas-controller's
	// TenantIdentifierFor does.
	LabelManagedByAITenant = "maas.opendatahub.io/managed-by-aitenant"
	LabelTenantName        = "maas.opendatahub.io/tenant-name"

	// PayloadProcessingBackendPraxis is the AnnotationPayloadProcessingType
	// value for the praxis dataplane (also the product default when the
	// annotation is absent; mirrors maas-controller's
	// tenantreconcile.PayloadProcessingTypePraxis).
	PayloadProcessingBackendPraxis = "praxis"

	// PayloadProcessingBackendIPP opts a tenant into maas-controller legacy
	// IPP (mirrors maas-controller's tenantreconcile.PayloadProcessingTypeIPP).
	PayloadProcessingBackendIPP = "ipp"

	// AITenantPhaseActive is the AITenant status.phase value maas-controller's
	// AITenant reconciler sets only after it has validated the tenant's
	// Gateway, and created its namespace, MaasTenantConfig, and RBAC. Used
	// as the readiness gate before installing praxis-extproc: status.gatewayRef
	// alone is not sufficient, since AITenantReconciler populates it
	// optimistically (from spec, unvalidated) before that work happens.
	AITenantPhaseActive = "Active"

	// AITenantConditionReady is the AITenant status condition type
	// maas-controller's AITenant reconciler stamps together with status.phase
	// (mirrors maasv1alpha1.AITenantConditionReady, set by
	// AITenantReconciler.setAITenantPhase), carrying ObservedGeneration set to
	// the AITenant's metadata.generation. It is the only place a per-generation
	// marker is exposed: AITenantStatus has no top-level observedGeneration
	// field. Reads of status.phase / status.gatewayRef are only trustworthy for
	// the current spec when this condition's observedGeneration matches
	// metadata.generation — see StatusIsCurrent.
	AITenantConditionReady = "Ready"

	// AITenantConditionPlatformPrerequisitesReady is the backend-neutral
	// readiness signal maas-controller publishes once the tenant namespace,
	// validated Gateway claim, RBAC, and MaasTenantConfig are safe for a
	// dataplane controller to provision. It may be true before Ready while the
	// selected dataplane controller is creating its Gateway resources.
	AITenantConditionPlatformPrerequisitesReady = "PlatformPrerequisitesReady"

	// PraxisCleanupFinalizer is added to every MaasTenantConfig this controller
	// has applied praxis-extproc resources for, so it can clean them up when
	// the tenant switches away from praxis or the MaasTenantConfig is deleted
	// (SSA alone never deletes resources that fall out of the render set).
	PraxisCleanupFinalizer = "ai-gateway-controller.opendatahub.io/praxis-cleanup"

	// DefaultAITenantName is the AITenant that represents the legacy/default
	// models-as-a-service installation (mirrors
	// maasv1alpha1.DefaultAITenantName / tenantreconcile.DefaultAITenantName).
	// Resources for this tenant keep the unsuffixed names Phase 1 always used,
	// for backward compatibility.
	DefaultAITenantName = "models-as-a-service"

	// Base resource names present in the controller-owned ExternalModel overlay
	// (config/manifests/external-model/overlays/odh), which composes the pinned
	// praxis-extproc manifests with controller-owned filter patches. Per-tenant
	// resources suffix these with "-{tenantID}"; the default tenant keeps them as-is.
	PayloadProcessingName                         = "payload-processing"
	PayloadProcessingExternalModelName            = "payload-processing-external-model"
	PayloadPreProcessingName                      = "payload-pre-processing"
	PayloadProcessingPluginsConfigMapName         = "payload-processing-plugins"
	PayloadProcessingReaderClusterRoleBindingName = "payload-processing-reader"
	PayloadProcessingExternalModelFilterName      = "payload-processing-external-model-filters"

	// Legacy standalone names are retained only so cleanup can remove resources
	// created before the ExtProc dataplane migration.
	praxisDeploymentName = "praxis"
	praxisServiceName    = "praxis"
	praxisConfigMapName  = "praxis-config"
	praxisServiceAccount = "praxis"

	// Active ExtProc overlay names are consumed by intelligent_route. They are
	// not standalone Praxis resources and must remain stable across upgrades.
	praxisOverlayName    = "routing-overlay"
	praxisOverlayDataKey = "routing-overlay.json"

	// LabelTenantInstance distinguishes pods/Services when multiple
	// per-tenant praxis-extproc stacks share a gateway namespace (mirrors
	// maas-controller's tenantreconcile.LabelTenantInstance).
	LabelTenantInstance = "maas.opendatahub.io/tenant-instance"

	// LabelManagedBy identifies resources applied by this controller. maas-controller
	// ensureIPPWritersStopped skips pods with this label when checking legacy IPP writers.
	LabelManagedBy = "app.kubernetes.io/managed-by"

	// ManagedByAIGatewayController is the LabelManagedBy value on praxis resources.
	ManagedByAIGatewayController = "ai-gateway-controller"

	// maxKubernetesNameLength is the Kubernetes object name limit (RFC 1123
	// label / DNS subdomain component). Rename returns an error rather than
	// applying a name that would exceed it.
	maxKubernetesNameLength = 63
)

// GVKs of the resources cleanup deletes by computed name. Kept alongside
// AITenantGVK rather than in render/postrender.go: these are the identity
// half of "what did we apply for this tenant", cleanup's own concern, not
// the kustomize-build/placeholder-substitution concern render.PostRender
// owns.
var (
	gvkDeployment         = schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"}
	gvkService            = schema.GroupVersionKind{Version: "v1", Kind: "Service"}
	gvkConfigMap          = schema.GroupVersionKind{Version: "v1", Kind: "ConfigMap"}
	gvkServiceAccount     = schema.GroupVersionKind{Version: "v1", Kind: "ServiceAccount"}
	gvkNetworkPolicy      = schema.GroupVersionKind{Group: "networking.k8s.io", Version: "v1", Kind: "NetworkPolicy"}
	gvkEnvoyFilter        = schema.GroupVersionKind{Group: "networking.istio.io", Version: "v1alpha3", Kind: "EnvoyFilter"}
	gvkDestinationRule    = schema.GroupVersionKind{Group: "networking.istio.io", Version: "v1", Kind: "DestinationRule"}
	gvkClusterRoleBinding = schema.GroupVersionKind{Group: "rbac.authorization.k8s.io", Version: "v1", Kind: "ClusterRoleBinding"}
)
