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
	"errors"
	"fmt"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// Rename fans the fixed-name praxis-extproc resources (as already rendered
// and namespace/placeholder-substituted by render.PostRender) out into a
// per-tenant copy: it renames every resource this controller owns to
// "{base}-{tenantID}" (mirrors tenantreconcile.resourceNameForTenant; the
// default tenant, tenantID == "", keeps the unsuffixed legacy names) and
// patches the internal references (ConfigMap/ServiceAccount volume refs,
// Service/Deployment selectors, EnvoyFilter cluster addresses,
// DestinationRule host/sni, ClusterRoleBinding subject) that would otherwise
// still point at the unsuffixed names.
//
// The shared "payload-processing-reader" ClusterRole is intentionally left
// untouched: every tenant's ClusterRoleBinding binds to that one shared
// role, only the binding (and its subject) is per-tenant, mirroring
// maas-controller's own IPP ClusterRole/ClusterRoleBinding split.
//
// Next to the shared ServiceAccount it adds the post-auth ExtProc's own
// ServiceAccount, which nothing binds.
//
// It does not mutate the input slice.
func Rename(resources []unstructured.Unstructured, tenantID, namespace string) ([]unstructured.Unstructured, error) {
	out := make([]unstructured.Unstructured, 0, len(resources)+1)
	for i := range resources {
		u := resources[i].DeepCopy()
		if err := renameOne(u, tenantID, namespace); err != nil {
			return nil, fmt.Errorf("rename %s %q for tenant %q: %w", u.GetKind(), u.GetName(), tenantID, err)
		}
		out = append(out, *u)
		if u.GetKind() == "ServiceAccount" && u.GetName() == PayloadProcessingServiceAccountName(tenantID) {
			out = append(out, newPostAuthServiceAccount(tenantID, u.GetNamespace()))
		}
	}
	return out, nil
}

// newPostAuthServiceAccount is built rather than copied from the shared
// ServiceAccount so it carries nothing MaaS sets on that one. Praxis does not
// call the Kubernetes API, so the pod gets no API token; the NeMo caller token
// is projected separately when a provider needs it.
func newPostAuthServiceAccount(tenantID, namespace string) unstructured.Unstructured {
	return unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1",
		"kind":       "ServiceAccount",
		"metadata": map[string]any{
			"name":      PayloadProcessingPostServiceAccountName(tenantID),
			"namespace": namespace,
		},
		"automountServiceAccountToken": false,
	}}
}

func renameOne(u *unstructured.Unstructured, tenantID, namespace string) error {
	kind := u.GetKind()
	name := u.GetName()

	switch {
	case kind == "Deployment" && name == PayloadProcessingName:
		return renamePayloadProcessingDeployment(u, tenantID)
	case kind == "Deployment" && name == PayloadPreProcessingName:
		return renamePayloadPreProcessingDeployment(u, tenantID)
	case kind == "Service" && name == PayloadProcessingName:
		return renamePayloadProcessingService(u, tenantID)
	case kind == "Service" && name == PayloadPreProcessingName:
		return renamePayloadPreProcessingService(u, tenantID)
	case kind == "ConfigMap" && name == PayloadProcessingPluginsConfigMapName:
		return setName(u, PayloadProcessingPluginsConfigMapForTenant(tenantID))
	case kind == "ServiceAccount" && name == PayloadProcessingName:
		return setName(u, PayloadProcessingServiceAccountName(tenantID))
	case kind == "NetworkPolicy" && name == PayloadProcessingName:
		return renamePayloadProcessingNetworkPolicy(u, tenantID)
	case kind == "ClusterRoleBinding" && name == PayloadProcessingReaderClusterRoleBindingName:
		return renamePayloadProcessingReaderClusterRoleBinding(u, tenantID)
	case kind == "EnvoyFilter" && name == PayloadProcessingName:
		return renamePayloadProcessingEnvoyFilter(u, tenantID, namespace)
	case kind == "EnvoyFilter" && name == PayloadProcessingExternalModelFilterName:
		return renamePayloadProcessingExternalModelFilter(u, tenantID, namespace)
	case kind == "DestinationRule" && name == PayloadProcessingName:
		return renamePayloadDestinationRule(u, PayloadProcessingServiceName(tenantID), namespace)
	case kind == "DestinationRule" && name == PayloadPreProcessingName:
		return renamePayloadDestinationRule(u, PayloadPreProcessingServiceName(tenantID), namespace)
	}
	// Everything else (notably the shared ClusterRole) passes through
	// unchanged.
	return nil
}

// setName renames u, refusing to produce a name that exceeds the
// Kubernetes 63-character object name limit. The 41-character AITenant
// name limit the CRD enforces is sized against maas-controller's own
// longest base name (maas-api-auth-policy, 21 chars); praxis-extproc's
// longest base name is payload-processing-plugins (27 chars), so a
// legitimately-shaped AITenant name can still overflow here.
func setName(u *unstructured.Unstructured, name string) error {
	if len(name) > maxKubernetesNameLength {
		return fmt.Errorf("computed name %q is %d characters, exceeds the Kubernetes %d-character limit",
			name, len(name), maxKubernetesNameLength)
	}
	u.SetName(name)
	return nil
}

func renamePayloadProcessingDeployment(u *unstructured.Unstructured, tenantID string) error {
	newName := PayloadProcessingDeploymentName(tenantID)
	if err := setName(u, newName); err != nil {
		return err
	}
	if err := setObjectLabel(u, LabelManagedBy, ManagedByAIGatewayController); err != nil {
		return fmt.Errorf("managed-by label: %w", err)
	}
	if err := addPodTemplateLabel(u, LabelManagedBy, ManagedByAIGatewayController); err != nil {
		return fmt.Errorf("pod template managed-by label: %w", err)
	}
	if err := addPodTemplateLabel(u, LabelTenantInstance, newName); err != nil {
		return fmt.Errorf("tenant-instance label: %w", err)
	}
	if tenantID != "" {
		// Never mutate the default Deployment's spec.selector: it is
		// immutable on upgrade, and the default tenant's Service selector
		// (unsuffixed "app" only) already resolves it correctly.
		if err := setSelectorMatchLabels(u, map[string]string{"app": PayloadProcessingName, LabelTenantInstance: newName}); err != nil {
			return fmt.Errorf("selector: %w", err)
		}
	}
	if err := setServiceAccountName(u, PayloadProcessingPostServiceAccountName(tenantID)); err != nil {
		return fmt.Errorf("serviceAccountName: %w", err)
	}
	if err := setConfigMapVolumeName(u, "plugins-config-volume", PayloadProcessingPluginsConfigMapForTenant(tenantID)); err != nil {
		return fmt.Errorf("plugins ConfigMap volume: %w", err)
	}
	return nil
}

func renamePayloadPreProcessingDeployment(u *unstructured.Unstructured, tenantID string) error {
	newName := PayloadPreProcessingDeploymentName(tenantID)
	if err := setName(u, newName); err != nil {
		return err
	}
	if err := setObjectLabel(u, LabelManagedBy, ManagedByAIGatewayController); err != nil {
		return fmt.Errorf("managed-by label: %w", err)
	}
	if err := addPodTemplateLabel(u, LabelManagedBy, ManagedByAIGatewayController); err != nil {
		return fmt.Errorf("pod template managed-by label: %w", err)
	}
	if err := addPodTemplateLabel(u, LabelTenantInstance, newName); err != nil {
		return fmt.Errorf("tenant-instance label: %w", err)
	}
	if tenantID != "" {
		if err := setSelectorMatchLabels(u, map[string]string{"app": PayloadPreProcessingName, LabelTenantInstance: newName}); err != nil {
			return fmt.Errorf("selector: %w", err)
		}
	}
	// pre-processing keeps the shared ServiceAccount the vendored overlay
	// assigns (see overlays/odh/pre-processing/deployment-patch.yaml); only
	// post-processing moves to its own.
	if err := setServiceAccountName(u, PayloadProcessingServiceAccountName(tenantID)); err != nil {
		return fmt.Errorf("serviceAccountName: %w", err)
	}
	if err := setConfigMapVolumeName(u, "plugins-config-volume", PayloadProcessingPluginsConfigMapForTenant(tenantID)); err != nil {
		return fmt.Errorf("plugins ConfigMap volume: %w", err)
	}
	return nil
}

func renamePayloadProcessingService(u *unstructured.Unstructured, tenantID string) error {
	if err := setName(u, PayloadProcessingServiceName(tenantID)); err != nil {
		return err
	}
	deploymentName := PayloadProcessingDeploymentName(tenantID)
	if err := setServiceSelector(u, map[string]string{"app": PayloadProcessingName, LabelTenantInstance: deploymentName}); err != nil {
		return fmt.Errorf("selector: %w", err)
	}
	return nil
}

func renamePayloadPreProcessingService(u *unstructured.Unstructured, tenantID string) error {
	if err := setName(u, PayloadPreProcessingServiceName(tenantID)); err != nil {
		return err
	}
	deploymentName := PayloadPreProcessingDeploymentName(tenantID)
	if err := setServiceSelector(u, map[string]string{"app": PayloadPreProcessingName, LabelTenantInstance: deploymentName}); err != nil {
		return fmt.Errorf("selector: %w", err)
	}
	return nil
}

func renamePayloadProcessingNetworkPolicy(u *unstructured.Unstructured, tenantID string) error {
	if err := setName(u, PayloadProcessingNetworkPolicyName(tenantID)); err != nil {
		return err
	}
	// Replace the base "app in (payload-processing, payload-pre-processing)"
	// podSelector with one scoped to this tenant's instance labels, so this
	// NetworkPolicy only ever selects this tenant's pods when multiple
	// tenants share the gateway namespace (mirrors
	// tenantreconcile.patchPayloadProcessingNetworkPolicy).
	podSelector := map[string]any{
		"matchExpressions": []any{
			map[string]any{
				"key":      LabelTenantInstance,
				"operator": "In",
				"values": []any{
					PayloadProcessingDeploymentName(tenantID),
					PayloadPreProcessingDeploymentName(tenantID),
				},
			},
		},
	}
	if err := unstructured.SetNestedMap(u.Object, podSelector, "spec", "podSelector"); err != nil {
		return fmt.Errorf("podSelector: %w", err)
	}
	return nil
}

func renamePayloadProcessingReaderClusterRoleBinding(u *unstructured.Unstructured, tenantID string) error {
	if err := setName(u, PayloadProcessingReaderClusterRoleBindingNameForTenant(tenantID)); err != nil {
		return err
	}
	subjects, found, err := unstructured.NestedSlice(u.Object, "subjects")
	if err != nil {
		return fmt.Errorf("read subjects: %w", err)
	}
	if !found || len(subjects) == 0 {
		return errors.New("subjects not found")
	}
	subject, ok := subjects[0].(map[string]any)
	if !ok {
		return errors.New("subjects[0] is not an object")
	}
	subject["name"] = PayloadProcessingServiceAccountName(tenantID)
	subjects[0] = subject
	if err := unstructured.SetNestedSlice(u.Object, subjects, "subjects"); err != nil {
		return fmt.Errorf("write subjects: %w", err)
	}
	return nil
}

func renamePayloadDestinationRule(u *unstructured.Unstructured, serviceName, namespace string) error {
	if err := setName(u, serviceName); err != nil {
		return err
	}
	fqdn := serviceFQDN(serviceName, namespace)
	if err := unstructured.SetNestedField(u.Object, fqdn, "spec", "host"); err != nil {
		return fmt.Errorf("host: %w", err)
	}
	if err := unstructured.SetNestedField(u.Object, fqdn, "spec", "trafficPolicy", "tls", "sni"); err != nil {
		return fmt.Errorf("sni: %w", err)
	}
	return nil
}

// renamePayloadProcessingEnvoyFilter renames the EnvoyFilter and repoints
// its dedicated ext_proc upstream CLUSTER definitions (see envoy-filter.yaml,
// "Dedicated ExtProc upstream clusters") at this tenant's Service FQDNs. The
// shared buffered ipp and pre-ipp filter names remain unchanged so the
// upstream KServe/MaaS chain is recognizable and can be disabled only on the
// controller-owned ExternalModel routes. The separate ExternalModel filter
// and cluster use a tenant-qualified Service and are enabled by route-level
// patches added by the ExternalModel controller.
func renamePayloadProcessingEnvoyFilter(u *unstructured.Unstructured, tenantID, namespace string) error {
	if err := setName(u, PayloadProcessingEnvoyFilterName(tenantID)); err != nil {
		return err
	}
	return patchPayloadProcessingEnvoyFilterNamespaces(u, tenantID, namespace, namespace)
}

func patchPayloadProcessingEnvoyFilterNamespaces(u *unstructured.Unstructured, tenantID, gatewayNamespace, tenantNamespace string) error {
	targets := map[string]string{
		"payload-pre-processing-extproc": serviceFQDN(PayloadPreProcessingServiceName(tenantID), gatewayNamespace),
		"payload-processing-extproc":     serviceFQDN(PayloadProcessingServiceName(tenantID), tenantNamespace),
	}

	configPatches, found, err := unstructured.NestedSlice(u.Object, "spec", "configPatches")
	if err != nil {
		return fmt.Errorf("read configPatches: %w", err)
	}
	if !found {
		return errors.New("configPatches not found")
	}

	patched := 0
	for i, raw := range configPatches {
		patch, ok := raw.(map[string]any)
		if !ok || patch["applyTo"] != "CLUSTER" {
			continue
		}
		patchBody, ok := patch["patch"].(map[string]any)
		if !ok {
			continue
		}
		value, ok := patchBody["value"].(map[string]any)
		if !ok {
			continue
		}
		clusterName, _ := value["name"].(string)
		fqdn, ok := targets[clusterName]
		if !ok {
			continue
		}
		if err := setClusterUpstreamAddress(value, fqdn); err != nil {
			return fmt.Errorf("CLUSTER patch %d (%s): %w", i, clusterName, err)
		}
		patched++
	}
	if patched != len(targets) {
		return fmt.Errorf("expected %d CLUSTER patches (%v), found %d", len(targets), targets, patched)
	}
	if err := unstructured.SetNestedSlice(u.Object, configPatches, "spec", "configPatches"); err != nil {
		return fmt.Errorf("write configPatches: %w", err)
	}
	return nil
}

func renamePayloadProcessingExternalModelFilter(u *unstructured.Unstructured, tenantID, namespace string) error {
	if err := setName(u, PayloadProcessingExternalModelFilterNameForTenant(tenantID)); err != nil {
		return err
	}
	configPatches, found, err := unstructured.NestedSlice(u.Object, "spec", "configPatches")
	if err != nil {
		return fmt.Errorf("read ExternalModel configPatches: %w", err)
	}
	if !found {
		return errors.New("ExternalModel configPatches not found")
	}
	target := serviceFQDN(PayloadProcessingExternalModelServiceName(tenantID), namespace)
	patched := 0
	for i, raw := range configPatches {
		patch, ok := raw.(map[string]any)
		if !ok || patch["applyTo"] != "CLUSTER" {
			continue
		}
		body, ok := patch["patch"].(map[string]any)
		if !ok {
			continue
		}
		value, ok := body["value"].(map[string]any)
		if !ok || value["name"] != "payload-processing-external-model-extproc" {
			continue
		}
		if err := setClusterUpstreamAddress(value, target); err != nil {
			return fmt.Errorf("CLUSTER patch %d: %w", i, err)
		}
		patched++
	}
	if patched != 1 {
		return fmt.Errorf("expected one ExternalModel CLUSTER patch, found %d", patched)
	}
	return unstructured.SetNestedSlice(u.Object, configPatches, "spec", "configPatches")
}

// setClusterUpstreamAddress rewrites the SNI and the (sole) endpoint
// address of an ext_proc upstream CLUSTER patch's value to fqdn. value is a
// plain map[string]any obtained by type-asserting into an
// already-deep-copied tree (see renamePayloadProcessingEnvoyFilter), so
// mutating its nested maps in place is safe and requires no separate
// write-back.
func setClusterUpstreamAddress(value map[string]any, fqdn string) error {
	if err := unstructured.SetNestedField(value, fqdn, "transport_socket", "typed_config", "sni"); err != nil {
		return fmt.Errorf("sni: %w", err)
	}

	loadAssignment, ok := value["load_assignment"].(map[string]any)
	if !ok {
		return errors.New("load_assignment not found")
	}
	endpoints, ok := loadAssignment["endpoints"].([]any)
	if !ok || len(endpoints) == 0 {
		return errors.New("load_assignment.endpoints not found")
	}
	endpoint0, ok := endpoints[0].(map[string]any)
	if !ok {
		return errors.New("endpoints[0] is not an object")
	}
	lbEndpoints, ok := endpoint0["lb_endpoints"].([]any)
	if !ok || len(lbEndpoints) == 0 {
		return errors.New("lb_endpoints not found")
	}
	lbEndpoint0, ok := lbEndpoints[0].(map[string]any)
	if !ok {
		return errors.New("lb_endpoints[0] is not an object")
	}
	ep, ok := lbEndpoint0["endpoint"].(map[string]any)
	if !ok {
		return errors.New("endpoint not found")
	}
	address, ok := ep["address"].(map[string]any)
	if !ok {
		return errors.New("address not found")
	}
	socketAddress, ok := address["socket_address"].(map[string]any)
	if !ok {
		return errors.New("socket_address not found")
	}
	socketAddress["address"] = fqdn
	return nil
}

func serviceFQDN(serviceName, namespace string) string {
	return fmt.Sprintf("%s.%s.svc.cluster.local", serviceName, namespace)
}

func setObjectLabel(u *unstructured.Unstructured, key, value string) error {
	labels := u.GetLabels()
	if labels == nil {
		labels = map[string]string{}
	}
	labels[key] = value
	u.SetLabels(labels)
	return nil
}

func addPodTemplateLabel(u *unstructured.Unstructured, key, value string) error {
	labels, _, err := unstructured.NestedStringMap(u.Object, "spec", "template", "metadata", "labels")
	if err != nil {
		return fmt.Errorf("read pod template labels: %w", err)
	}
	if labels == nil {
		labels = map[string]string{}
	}
	labels[key] = value
	if err := unstructured.SetNestedStringMap(u.Object, labels, "spec", "template", "metadata", "labels"); err != nil {
		return fmt.Errorf("write pod template labels: %w", err)
	}
	return nil
}

func setSelectorMatchLabels(u *unstructured.Unstructured, labels map[string]string) error {
	if err := unstructured.SetNestedStringMap(u.Object, labels, "spec", "selector", "matchLabels"); err != nil {
		return fmt.Errorf("write selector.matchLabels: %w", err)
	}
	return nil
}

func setServiceSelector(u *unstructured.Unstructured, labels map[string]string) error {
	if err := unstructured.SetNestedStringMap(u.Object, labels, "spec", "selector"); err != nil {
		return fmt.Errorf("write spec.selector: %w", err)
	}
	return nil
}

func setServiceAccountName(u *unstructured.Unstructured, name string) error {
	if err := unstructured.SetNestedField(u.Object, name, "spec", "template", "spec", "serviceAccountName"); err != nil {
		return fmt.Errorf("write serviceAccountName: %w", err)
	}
	return nil
}

func setConfigMapVolumeName(u *unstructured.Unstructured, volumeName, configMapName string) error {
	volumes, found, err := unstructured.NestedSlice(u.Object, "spec", "template", "spec", "volumes")
	if err != nil {
		return fmt.Errorf("read volumes: %w", err)
	}
	if !found {
		return errors.New("volumes not found")
	}
	for i, raw := range volumes {
		vol, ok := raw.(map[string]any)
		if !ok || vol["name"] != volumeName {
			continue
		}
		cm, ok := vol["configMap"].(map[string]any)
		if !ok {
			return fmt.Errorf("volume %q has no configMap", volumeName)
		}
		cm["name"] = configMapName
		vol["configMap"] = cm
		volumes[i] = vol
		if err := unstructured.SetNestedSlice(u.Object, volumes, "spec", "template", "spec", "volumes"); err != nil {
			return fmt.Errorf("write volumes: %w", err)
		}
		return nil
	}
	return fmt.Errorf("volume %q not found", volumeName)
}
