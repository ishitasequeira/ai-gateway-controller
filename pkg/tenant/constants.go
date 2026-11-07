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

// MaaSSubscriptionGVK is a subscription. Its guardrails lists are read in the order written.
var MaaSSubscriptionGVK = schema.GroupVersionKind{
	Group:   "maas.opendatahub.io",
	Version: "v1alpha1",
	Kind:    "MaaSSubscription",
}

// MaaSModelRefGVK is a model. Its guardrails list is read after the subscription-wide lists.
var MaaSModelRefGVK = schema.GroupVersionKind{
	Group:   "maas.opendatahub.io",
	Version: "v1alpha1",
	Kind:    "MaaSModelRef",
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

	// AnnotationExtProcMaxRequests overrides the ExtProc upstream Envoy
	// circuit-breaker concurrency budget (max_connections, max_pending_requests,
	// max_requests) and HTTP/2 max_concurrent_streams for this tenant's ExtProc
	// clusters. Default (absent/invalid) is DefaultExtProcMaxRequests.
	//
	// Annotation rather than MaasTenantConfig.spec: Praxis v3 may reshape how
	// ExtProc concurrency is expressed, and we do not want a typed CRD field
	// that would need deprecation during that move. Mirrors the experimental-*
	// annotation pattern used for Envoy body-buffer limits in maas-controller.
	AnnotationExtProcMaxRequests = "maas.opendatahub.io/experimental-extproc-max-requests"

	// DefaultExtProcMaxRequests is the ExtProc upstream concurrency budget when
	// AnnotationExtProcMaxRequests is absent. It is a hard Envoy circuit-breaker
	// cap (not a soft hint) chosen to bound ExtProc memory under large bodies
	// (paired with DefaultExtProcMaxBodyBytes: 8×10 MiB; measured peak is
	// ~50 MiB/request so ODH pre/post pods default to a 512 MiB limit).
	DefaultExtProcMaxRequests int64 = 8

	// MaxExtProcMaxRequests bounds AnnotationExtProcMaxRequests (Envoy's own
	// circuit-breaker default is 1024).
	MaxExtProcMaxRequests int64 = 1024

	// AnnotationExtProcMaxBodyBytes overrides Praxis ExtProc server
	// max_body_bytes (ConfigMap pre-extproc.yaml / rendered extproc.yaml) for
	// this tenant. Default (absent/invalid) is DefaultExtProcMaxBodyBytes.
	// Same experimental-annotation rationale as AnnotationExtProcMaxRequests.
	AnnotationExtProcMaxBodyBytes = "maas.opendatahub.io/experimental-extproc-max-body-bytes"

	// DefaultExtProcMaxBodyBytes is the ExtProc server body bound when
	// AnnotationExtProcMaxBodyBytes is absent (10 MiB).
	DefaultExtProcMaxBodyBytes int64 = 10485760

	// MaxExtProcMaxBodyBytes is the upper bound for AnnotationExtProcMaxBodyBytes
	// (32 MiB). Deployments pair this with GRPC_MAX_RECV_MSG_SIZE = body + 1 MiB
	// framing so tonic rejects oversized ExtProc messages before allocation.
	MaxExtProcMaxBodyBytes int64 = 33554432

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
	// Gateway, and created its namespace, MaasTenantConfig, and RBAC.
	AITenantPhaseActive = "Active"

	// AITenantPhaseFailed is status.phase when maas-controller cannot finish
	// bootstrap (including GatewayClaimFailed). gatewayRef may still name the
	// contested gateway; DeployReady rejects Failed.
	AITenantPhaseFailed = "Failed"

	// AITenantConditionReady is the AITenant status condition type
	// maas-controller's AITenant reconciler stamps together with status.phase
	// (mirrors maasv1alpha1.AITenantConditionReady, set by
	// AITenantReconciler.setAITenantPhase), carrying ObservedGeneration set to
	// the AITenant's metadata.generation. StatusIsCurrent / DeployReady use
	// that stamp so phase and gatewayRef are only trusted for the live spec.
	AITenantConditionReady = "Ready"

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
	PayloadProcessingReaderClusterRoleBindingName = "payload-processing-reader" // retained for leftover binding cleanup
	// Metrics RBAC / scrape resources from praxis-extproc (#57 / #177). The
	// metrics-auth binding is tenant-suffixed because it names the ExtProc SA;
	// ClusterRoles and the metrics-reader binding stay shared/unsuffixed.
	PayloadProcessingMetricsAuthName         = "payload-processing-metrics-auth"
	PayloadProcessingMetricsReaderName       = "payload-processing-metrics-reader"
	PayloadProcessingMetricsServiceName      = "payload-processing-metrics"
	PayloadProcessingMetricsMonitorName      = "payload-processing-metrics"
	PayloadProcessingExternalModelFilterName = "payload-processing-external-model-filters"
	servingCertSecretAnnotation              = "service.beta.openshift.io/serving-cert-secret-name" //nolint:gosec // G101: annotation key name, not a credential
	reloaderSecretAnnotation                 = "secret.reloader.stakater.com/reload"
	grpcMaxRecvMsgSizeEnv                    = "GRPC_MAX_RECV_MSG_SIZE"
	// grpcRecvFramingHeadroomBytes matches praxis-extproc GRPC_RECV_FRAMING_HEADROOM
	// so GRPC_MAX_RECV_MSG_SIZE stays above server max_body_bytes.
	grpcRecvFramingHeadroomBytes int64 = 1024 * 1024

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

	// LabelAttachmentTenant marks the HTTPRoutes an ExternalModel contributes
	// to a tenant from another namespace. Its value is the tenant
	// (MaasTenantConfig) namespace.
	LabelAttachmentTenant = "inference.opendatahub.io/attachment-tenant"

	// LabelCredentialCopy marks controller-owned copies of credential Secrets
	// from outside the tenant namespace. Copies live in the tenant namespace.
	LabelCredentialCopy = "inference.opendatahub.io/credential-copy"

	// AnnotationCredentialHeader carries the provider-boundary request header
	// (envelope.Credential.Header) for an attached candidate's credential. The
	// header is derived at attach time via envelope.StrategyFor, where the
	// provider type and API format are known, and stamped on the credential
	// copy Secret so the tenant reconciler can restore it after reading the
	// published overlay back: Credential.Header is json:"-" and never survives
	// the overlay round-trip. Absent (empty) for strategies whose header is
	// fixed data-plane side, e.g. bearer_token.
	AnnotationCredentialHeader = "inference.opendatahub.io/credential-header"

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
	gvkServiceMonitor     = schema.GroupVersionKind{Group: "monitoring.coreos.com", Version: "v1", Kind: "ServiceMonitor"}
	gvkGateway            = schema.GroupVersionKind{Group: "gateway.networking.k8s.io", Version: "v1", Kind: "Gateway"}
	gvkHTTPRoute          = schema.GroupVersionKind{Group: "gateway.networking.k8s.io", Version: "v1", Kind: "HTTPRoute"}
	gvkHPA                = schema.GroupVersionKind{Group: "autoscaling", Version: "v2", Kind: "HorizontalPodAutoscaler"}
)
