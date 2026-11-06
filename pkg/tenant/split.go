/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package tenant

import (
	"errors"
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// SplitPostAuthResources preserves the rendered MaaS/KServe payload-
// processing resources in the Gateway namespace and adds only the dedicated
// ExternalModel ExtProc copy in the resolved tenant namespace. The shared
// filter, workload, Service, TLS identity, and buffered configuration are
// intentionally not retargeted. The ExternalModel EnvoyFilter is retargeted to
// the tenant-local ExtProc Service.
//
//nolint:gocyclo // Namespace/resource splitting is an explicit compatibility matrix; each branch preserves a distinct ownership contract.
func SplitPostAuthResources(resources []unstructured.Unstructured, tenantID, gatewayNamespace, tenantNamespace string) ([]unstructured.Unstructured, error) {
	if gatewayNamespace == tenantNamespace {
		out := make([]unstructured.Unstructured, 0, len(resources)+1)
		for i := range resources {
			u := resources[i].DeepCopy()
			switch {
			case u.GetKind() == "Deployment" && u.GetName() == PayloadProcessingDeploymentName(tenantID):
				out = append(out, *u)
				external := u.DeepCopy()
				if err := configureExternalModelDeployment(external, tenantID); err != nil {
					return nil, err
				}
				out = append(out, *external)
			case u.GetKind() == "ServiceAccount" && u.GetName() == PayloadProcessingServiceAccountName(tenantID):
				out = append(out, *u)
				external := u.DeepCopy()
				external.SetName(PayloadProcessingExternalModelServiceAccountName(tenantID))
				out = append(out, *external)
			case u.GetKind() == "Service" && u.GetName() == PayloadProcessingServiceName(tenantID):
				out = append(out, *u)
				external := u.DeepCopy()
				external.SetName(PayloadProcessingExternalModelServiceName(tenantID))
				if err := setServiceSelector(external, map[string]string{"app": PayloadProcessingExternalModelName, LabelTenantInstance: PayloadProcessingExternalModelDeploymentName(tenantID)}); err != nil {
					return nil, fmt.Errorf("external-model Service selector: %w", err)
				}
				if err := setServingCertSecretName(external, PayloadProcessingExternalModelTLSSecretName(tenantID)); err != nil {
					return nil, fmt.Errorf("external-model serving-cert: %w", err)
				}
				out = append(out, *external)
			case u.GetKind() == "DestinationRule" && u.GetName() == PayloadProcessingServiceName(tenantID):
				out = append(out, *u)
				external := u.DeepCopy()
				if err := renamePayloadDestinationRule(external, PayloadProcessingExternalModelServiceName(tenantID), tenantNamespace); err != nil {
					return nil, fmt.Errorf("external-model DestinationRule: %w", err)
				}
				out = append(out, *external)
			case u.GetKind() == "ConfigMap" && u.GetName() == PayloadProcessingPluginsConfigMapForTenant(tenantID):
				external := u.DeepCopy()
				external.SetName(PayloadProcessingExternalModelPluginsConfigMapForTenant(tenantID))
				if err := keepExternalModelConfig(external); err != nil {
					return nil, err
				}
				out = append(out, *u, *external)
			case u.GetKind() == "NetworkPolicy":
				if err := filterTenantInstanceSelectorValues(u, []string{PayloadProcessingDeploymentName(tenantID), PayloadProcessingExternalModelDeploymentName(tenantID)}); err != nil {
					return nil, err
				}
				out = append(out, *u)
			case u.GetKind() == "ClusterRoleBinding":
				if isLegacyReaderClusterRoleBinding(u.GetName()) {
					// Drop leftover reader bindings; ExtProc must not hold secrets get.
					continue
				}
				out = append(out, *u)
			default:
				out = append(out, *u)
			}
		}
		return out, nil
	}
	out := make([]unstructured.Unstructured, 0, len(resources)+5)
	for i := range resources {
		u := resources[i].DeepCopy()
		switch {
		case u.GetKind() == "Deployment" && u.GetName() == PayloadProcessingDeploymentName(tenantID):
			out = append(out, *u)
			external := u.DeepCopy()
			external.SetNamespace(tenantNamespace)
			if err := configureExternalModelDeployment(external, tenantID); err != nil {
				return nil, err
			}
			out = append(out, *external)
		case u.GetKind() == "Service" && u.GetName() == PayloadProcessingServiceName(tenantID):
			out = append(out, *u)
			external := u.DeepCopy()
			external.SetNamespace(tenantNamespace)
			external.SetName(PayloadProcessingExternalModelServiceName(tenantID))
			if err := setServiceSelector(external, map[string]string{"app": PayloadProcessingExternalModelName, LabelTenantInstance: PayloadProcessingExternalModelDeploymentName(tenantID)}); err != nil {
				return nil, fmt.Errorf("external-model Service selector: %w", err)
			}
			if err := setServingCertSecretName(external, PayloadProcessingExternalModelTLSSecretName(tenantID)); err != nil {
				return nil, fmt.Errorf("external-model serving-cert: %w", err)
			}
			out = append(out, *external)
		case u.GetKind() == "DestinationRule" && u.GetName() == PayloadProcessingServiceName(tenantID):
			// Rename already left the shared MaaS/KServe DestinationRule
			// pointing at the Gateway-local payload-processing Service.
			out = append(out, *u)
			external := u.DeepCopy()
			if err := renamePayloadDestinationRule(external, PayloadProcessingExternalModelServiceName(tenantID), tenantNamespace); err != nil {
				return nil, fmt.Errorf("external-model DestinationRule: %w", err)
			}
			out = append(out, *external)
		case u.GetKind() == "ConfigMap" && u.GetName() == PayloadProcessingPluginsConfigMapForTenant(tenantID):
			out = append(out, *u)
			external := u.DeepCopy()
			external.SetNamespace(tenantNamespace)
			if err := keepExternalModelConfig(external); err != nil {
				return nil, err
			}
			external.SetName(PayloadProcessingExternalModelPluginsConfigMapForTenant(tenantID))
			out = append(out, *external)
		case u.GetKind() == "ServiceAccount" && u.GetName() == PayloadProcessingServiceAccountName(tenantID):
			out = append(out, *u)
			external := u.DeepCopy()
			external.SetNamespace(tenantNamespace)
			external.SetName(PayloadProcessingExternalModelServiceAccountName(tenantID))
			out = append(out, *external)
		case u.GetKind() == "NetworkPolicy" && u.GetName() == PayloadProcessingNetworkPolicyName(tenantID):
			out = append(out, *u)
			post := u.DeepCopy()
			post.SetNamespace(tenantNamespace)
			if err := filterTenantInstanceSelectorValues(post, []string{PayloadProcessingDeploymentName(tenantID), PayloadProcessingExternalModelDeploymentName(tenantID)}); err != nil {
				return nil, err
			}
			out = append(out, *post)
		case u.GetKind() == "EnvoyFilter" && u.GetName() == PayloadProcessingExternalModelFilterNameForTenant(tenantID):
			if err := renamePayloadProcessingExternalModelFilter(u, tenantID, tenantNamespace); err != nil {
				return nil, fmt.Errorf("external-model EnvoyFilter: %w", err)
			}
			out = append(out, *u)
		case u.GetKind() == "ClusterRoleBinding":
			if isLegacyReaderClusterRoleBinding(u.GetName()) {
				// Drop leftover reader bindings; ExtProc must not hold secrets get.
				continue
			}
			out = append(out, *u)
		default:
			out = append(out, *u)
		}
	}
	return out, nil
}

// isLegacyReaderClusterRoleBinding reports whether name is a leftover
// payload-processing-reader binding (possibly tenant-suffixed / -post).
// Metrics-auth / metrics-reader bindings must be kept.
func isLegacyReaderClusterRoleBinding(name string) bool {
	return strings.HasPrefix(name, PayloadProcessingReaderClusterRoleBindingName)
}

// RemoveExternalModelResources removes the credential-bearing, tenant-local
// ExtProc resources from a tenant render when no ExternalModel remains. The
// shared pre-auth/KServe resources stay in the render and continue serving
// ordinary MaaS traffic.
func RemoveExternalModelResources(resources []unstructured.Unstructured, tenantID string) []unstructured.Unstructured {
	deployment := PayloadProcessingExternalModelDeploymentName(tenantID)
	service := PayloadProcessingExternalModelServiceName(tenantID)
	configMap := PayloadProcessingExternalModelPluginsConfigMapForTenant(tenantID)
	serviceAccount := PayloadProcessingExternalModelServiceAccountName(tenantID)
	filter := PayloadProcessingExternalModelFilterNameForTenant(tenantID)
	result := make([]unstructured.Unstructured, 0, len(resources))
	for i := range resources {
		u := resources[i]
		remove := (u.GetKind() == "Deployment" && u.GetName() == deployment) ||
			(u.GetKind() == "Service" && u.GetName() == service) ||
			(u.GetKind() == "ConfigMap" && u.GetName() == configMap) ||
			(u.GetKind() == "ServiceAccount" && u.GetName() == serviceAccount) ||
			(u.GetKind() == "DestinationRule" && u.GetName() == service) ||
			(u.GetKind() == "EnvoyFilter" && u.GetName() == filter)
		if !remove {
			result = append(result, u)
		}
	}
	return result
}

func keepExternalModelConfig(u *unstructured.Unstructured) error {
	data, found, err := unstructured.NestedStringMap(u.Object, "data")
	if err != nil {
		return fmt.Errorf("read ExternalModel plugin ConfigMap data: %w", err)
	}
	if !found {
		return errors.New("ExternalModel plugin ConfigMap data is missing")
	}
	for key := range data {
		if key != "extproc.yaml" {
			delete(data, key)
		}
	}
	return unstructured.SetNestedStringMap(u.Object, data, "data")
}

func configureExternalModelDeployment(u *unstructured.Unstructured, tenantID string) error {
	name := PayloadProcessingExternalModelDeploymentName(tenantID)
	if err := setName(u, name); err != nil {
		return err
	}
	if err := setObjectLabel(u, "app", PayloadProcessingExternalModelName); err != nil {
		return fmt.Errorf("ExternalModel Deployment label: %w", err)
	}
	if err := addPodTemplateLabel(u, "app", PayloadProcessingExternalModelName); err != nil {
		return fmt.Errorf("ExternalModel pod label: %w", err)
	}
	if err := addPodTemplateLabel(u, LabelTenantInstance, name); err != nil {
		return fmt.Errorf("ExternalModel tenant label: %w", err)
	}
	if err := setSelectorMatchLabels(u, map[string]string{"app": PayloadProcessingExternalModelName, LabelTenantInstance: name}); err != nil {
		return fmt.Errorf("ExternalModel selector: %w", err)
	}
	if err := setServiceAccountName(u, PayloadProcessingExternalModelServiceAccountName(tenantID)); err != nil {
		return fmt.Errorf("ExternalModel ServiceAccount: %w", err)
	}
	if err := setConfigMapVolumeName(u, "plugins-config-volume", PayloadProcessingExternalModelPluginsConfigMapForTenant(tenantID)); err != nil {
		return fmt.Errorf("ExternalModel ConfigMap volume: %w", err)
	}
	tlsSecret := PayloadProcessingExternalModelTLSSecretName(tenantID)
	if err := setTLSSecretVolumeName(u, "tls", tlsSecret); err != nil {
		return fmt.Errorf("ExternalModel TLS secret volume: %w", err)
	}
	if err := setReloaderSecretName(u, tlsSecret); err != nil {
		return fmt.Errorf("ExternalModel reloader annotation: %w", err)
	}
	volumes, _, err := unstructured.NestedSlice(u.Object, "spec", "template", "spec", "volumes")
	if err != nil {
		return fmt.Errorf("read ExternalModel volumes: %w", err)
	}
	volumes = appendOrReplaceNamedVolume(volumes, "routing-overlay-volume", map[string]any{
		"name": "routing-overlay-volume",
		"configMap": map[string]any{
			"name":     praxisOverlayName,
			"optional": true,
		},
	})
	if err := unstructured.SetNestedSlice(u.Object, volumes, "spec", "template", "spec", "volumes"); err != nil {
		return fmt.Errorf("write ExternalModel volumes: %w", err)
	}
	containers, found, err := unstructured.NestedSlice(u.Object, "spec", "template", "spec", "containers")
	if err != nil || !found || len(containers) == 0 {
		if err == nil {
			err = errors.New("containers are missing")
		}
		return fmt.Errorf("read ExternalModel containers: %w", err)
	}
	container, ok := containers[0].(map[string]any)
	if !ok {
		return errors.New("ExternalModel first container is malformed")
	}
	mounts, _, err := unstructured.NestedSlice(container, "volumeMounts")
	if err != nil {
		return fmt.Errorf("read ExternalModel volume mounts: %w", err)
	}
	mounts = appendOrReplaceNamedVolume(mounts, "routing-overlay-volume", map[string]any{
		"name": "routing-overlay-volume", "mountPath": "/etc/praxis/routing", "readOnly": true,
	})
	if err := unstructured.SetNestedSlice(container, mounts, "volumeMounts"); err != nil {
		return fmt.Errorf("write ExternalModel volume mounts: %w", err)
	}
	containers[0] = container
	if err := unstructured.SetNestedSlice(u.Object, containers, "spec", "template", "spec", "containers"); err != nil {
		return fmt.Errorf("write ExternalModel containers: %w", err)
	}
	return nil
}

func filterTenantInstanceSelector(u *unstructured.Unstructured, instance string) error {
	return filterTenantInstanceSelectorValues(u, []string{instance})
}

func filterTenantInstanceSelectorValues(u *unstructured.Unstructured, instances []string) error {
	expressions, found, err := unstructured.NestedSlice(u.Object, "spec", "podSelector", "matchExpressions")
	if err != nil {
		return fmt.Errorf("read NetworkPolicy pod selector: %w", err)
	}
	if !found {
		return errors.New("NetworkPolicy pod selector matchExpressions are missing")
	}
	for _, raw := range expressions {
		expression, ok := raw.(map[string]any)
		if !ok || expression["key"] != LabelTenantInstance {
			continue
		}
		values := make([]any, len(instances))
		for i, instance := range instances {
			values[i] = instance
		}
		expression["values"] = values
	}
	return unstructured.SetNestedSlice(u.Object, expressions, "spec", "podSelector", "matchExpressions")
}

func setBindingSubjectNamespace(u *unstructured.Unstructured, namespace string) error {
	subjects, found, err := unstructured.NestedSlice(u.Object, "subjects")
	if err != nil {
		return fmt.Errorf("read ClusterRoleBinding subjects: %w", err)
	}
	if !found || len(subjects) == 0 {
		return errors.New("ClusterRoleBinding subjects are missing")
	}
	subject, ok := subjects[0].(map[string]any)
	if !ok {
		return errors.New("ClusterRoleBinding subject is malformed")
	}
	subject["namespace"] = namespace
	return unstructured.SetNestedSlice(u.Object, subjects, "subjects")
}
