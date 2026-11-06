package tenant

import (
	"reflect"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/opendatahub-io/ai-gateway-controller/pkg/render"
)

//nolint:gocyclo // The test checks every namespace/ownership boundary in one rendered resource set.
func TestSplitPostAuthResourcesUsesResolvedTenantNamespace(t *testing.T) {
	requireManifests(t)
	rendered, err := render.Build(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	resources := render.PostRender(rendered, render.Params{Namespace: "gateway-system", GatewayName: "gateway", Image: "extproc:dev"})
	resources, err = Rename(resources, "tenant-a", "gateway-system")
	if err != nil {
		t.Fatal(err)
	}
	sharedBeforeSplit := append([]unstructured.Unstructured(nil), resources...)
	resources, err = SplitPostAuthResources(resources, "tenant-a", "gateway-system", "tenant-a")
	if err != nil {
		t.Fatal(err)
	}

	find := func(kind, name, namespace string) *unstructured.Unstructured {
		t.Helper()
		for i := range resources {
			if resources[i].GetKind() == kind && resources[i].GetName() == name && resources[i].GetNamespace() == namespace {
				return &resources[i]
			}
		}
		t.Fatalf("missing %s %s/%s", kind, namespace, name)
		return nil
	}
	findIn := func(items []unstructured.Unstructured, kind, name, namespace string) *unstructured.Unstructured {
		for i := range items {
			if items[i].GetKind() == kind && items[i].GetName() == name && items[i].GetNamespace() == namespace {
				return &items[i]
			}
		}
		return nil
	}
	for _, identity := range []struct{ kind, name, namespace string }{
		{"Deployment", PayloadProcessingDeploymentName("tenant-a"), "gateway-system"},
		{"Service", PayloadProcessingServiceName("tenant-a"), "gateway-system"},
		{"ServiceAccount", PayloadProcessingServiceAccountName("tenant-a"), "gateway-system"},
		{"ConfigMap", PayloadProcessingPluginsConfigMapForTenant("tenant-a"), "gateway-system"},
		{"NetworkPolicy", PayloadProcessingNetworkPolicyName("tenant-a"), "gateway-system"},
		{"DestinationRule", PayloadProcessingServiceName("tenant-a"), "gateway-system"},
		{"EnvoyFilter", PayloadProcessingEnvoyFilterName("tenant-a"), "gateway-system"},
	} {
		before := findIn(sharedBeforeSplit, identity.kind, identity.name, identity.namespace)
		after := findIn(resources, identity.kind, identity.name, identity.namespace)
		if before == nil || after == nil || !reflect.DeepEqual(before.Object, after.Object) {
			t.Fatalf("shared %s %s/%s changed during ExternalModel split", identity.kind, identity.namespace, identity.name)
		}
	}
	find("Deployment", PayloadPreProcessingDeploymentName("tenant-a"), "gateway-system")
	postDeployment := find("Deployment", PayloadProcessingDeploymentName("tenant-a"), "gateway-system")
	if serviceAccount, _, _ := unstructured.NestedString(postDeployment.Object, "spec", "template", "spec", "serviceAccountName"); serviceAccount != PayloadProcessingPostServiceAccountName("tenant-a") {
		t.Fatalf("shared post-auth Deployment ServiceAccount = %q, want %q", serviceAccount, PayloadProcessingPostServiceAccountName("tenant-a"))
	}
	preDeployment := find("Deployment", PayloadPreProcessingDeploymentName("tenant-a"), "gateway-system")
	if serviceAccount, _, _ := unstructured.NestedString(preDeployment.Object, "spec", "template", "spec", "serviceAccountName"); serviceAccount != PayloadProcessingServiceAccountName("tenant-a") {
		t.Fatalf("pre-auth Deployment ServiceAccount = %q, want the shared %q", serviceAccount, PayloadProcessingServiceAccountName("tenant-a"))
	}
	externalDeployment := find("Deployment", PayloadProcessingExternalModelDeploymentName("tenant-a"), "tenant-a")
	if serviceAccount, _, _ := unstructured.NestedString(externalDeployment.Object, "spec", "template", "spec", "serviceAccountName"); serviceAccount != PayloadProcessingExternalModelServiceAccountName("tenant-a") {
		t.Fatalf("ExternalModel Deployment ServiceAccount = %q, want %q", serviceAccount, PayloadProcessingExternalModelServiceAccountName("tenant-a"))
	}
	if selector, _, _ := unstructured.NestedString(externalDeployment.Object, "spec", "selector", "matchLabels", "app"); selector != PayloadProcessingExternalModelName {
		t.Fatalf("ExternalModel Deployment selector app = %q, want %q", selector, PayloadProcessingExternalModelName)
	}
	externalService := find("Service", PayloadProcessingExternalModelServiceName("tenant-a"), "tenant-a")
	if selector, _, _ := unstructured.NestedString(externalService.Object, "spec", "selector", "app"); selector != PayloadProcessingExternalModelName {
		t.Fatalf("ExternalModel Service selector app = %q, want %q", selector, PayloadProcessingExternalModelName)
	}
	externalRule := find("DestinationRule", PayloadProcessingExternalModelServiceName("tenant-a"), "gateway-system")
	externalFQDN := "payload-processing-external-model-tenant-a.tenant-a.svc.cluster.local"
	if host, _, _ := unstructured.NestedString(externalRule.Object, "spec", "host"); host != externalFQDN {
		t.Fatalf("ExternalModel DestinationRule host = %q, want %q", host, externalFQDN)
	}
	if sni, _, _ := unstructured.NestedString(externalRule.Object, "spec", "trafficPolicy", "tls", "sni"); sni != externalFQDN {
		t.Fatalf("ExternalModel DestinationRule SNI = %q, want %q", sni, externalFQDN)
	}
	find("ServiceAccount", PayloadProcessingServiceAccountName("tenant-a"), "gateway-system")
	find("ServiceAccount", PayloadProcessingPostServiceAccountName("tenant-a"), "gateway-system")
	for _, resource := range resources {
		if resource.GetKind() == "ServiceAccount" && resource.GetName() == PayloadProcessingPostServiceAccountName("tenant-a") && resource.GetNamespace() != "gateway-system" {
			t.Fatal("shared post-auth ServiceAccount must not be moved or duplicated into the tenant namespace")
		}
		if resource.GetKind() != "ClusterRoleBinding" {
			continue
		}
		subjects, _, _ := unstructured.NestedSlice(resource.Object, "subjects")
		for _, raw := range subjects {
			if subject, _ := raw.(map[string]any); subject["name"] == PayloadProcessingPostServiceAccountName("tenant-a") {
				t.Fatalf("ClusterRoleBinding %s binds the post-auth ServiceAccount, which must hold no permissions", resource.GetName())
			}
		}
	}
	find("Service", PayloadPreProcessingServiceName("tenant-a"), "gateway-system")
	find("Service", PayloadProcessingServiceName("tenant-a"), "gateway-system")
	postRule := find("DestinationRule", PayloadProcessingServiceName("tenant-a"), "gateway-system")
	postFQDN := "payload-processing-tenant-a.gateway-system.svc.cluster.local"
	preFQDN := "payload-pre-processing-tenant-a.gateway-system.svc.cluster.local"
	if host, _, _ := unstructured.NestedString(postRule.Object, "spec", "host"); host != postFQDN {
		t.Fatalf("shared post-auth DestinationRule host = %q, want Gateway-local Service FQDN", host)
	}
	if sni, _, _ := unstructured.NestedString(postRule.Object, "spec", "trafficPolicy", "tls", "sni"); sni != postFQDN {
		t.Fatalf("shared post-auth DestinationRule SNI = %q, want Gateway-local Service FQDN", sni)
	}
	for _, resource := range resources {
		if resource.GetKind() == "DestinationRule" && resource.GetName() == PayloadProcessingServiceName("tenant-a") && resource.GetNamespace() == "tenant-a" {
			t.Fatal("post-auth DestinationRule must remain in the Gateway namespace")
		}
	}

	gatewayConfig := find("ConfigMap", PayloadProcessingPluginsConfigMapForTenant("tenant-a"), "gateway-system")
	if data, _, _ := unstructured.NestedStringMap(gatewayConfig.Object, "data"); data["extproc.yaml"] == "" || data["pre-extproc.yaml"] == "" {
		t.Fatal("shared Gateway ConfigMap must retain both buffered MaaS/KServe configurations")
	}
	for _, resource := range resources {
		if resource.GetKind() == "ConfigMap" && resource.GetName() == PayloadProcessingPluginsConfigMapForTenant("tenant-a") && resource.GetNamespace() == "tenant-a" {
			t.Fatal("shared payload-processing ConfigMap must not be moved into the tenant namespace")
		}
	}
	externalConfig := find("ConfigMap", PayloadProcessingExternalModelPluginsConfigMapForTenant("tenant-a"), "tenant-a")
	if data, _, _ := unstructured.NestedStringMap(externalConfig.Object, "data"); data["extproc.yaml"] == "" || data["pre-extproc.yaml"] != "" {
		t.Fatal("ExternalModel ConfigMap must contain only the dedicated post-auth configuration")
	}
	externalFilter := find("EnvoyFilter", PayloadProcessingExternalModelFilterNameForTenant("tenant-a"), "gateway-system")
	externalPatches, _, _ := unstructured.NestedSlice(externalFilter.Object, "spec", "configPatches")
	wantExternalFQDN := "payload-processing-external-model-tenant-a.tenant-a.svc.cluster.local"
	foundExternalCluster := false
	for _, raw := range externalPatches {
		patch, ok := raw.(map[string]any)
		if !ok || patch["applyTo"] != "CLUSTER" {
			continue
		}
		body, _ := patch["patch"].(map[string]any)
		value, _ := body["value"].(map[string]any)
		if value["name"] != "payload-processing-external-model-extproc" {
			continue
		}
		assertClusterAddress(t, value, wantExternalFQDN)
		foundExternalCluster = true
	}
	if !foundExternalCluster {
		t.Fatal("ExternalModel EnvoyFilter must target the tenant-local ExtProc Service")
	}

	envoy := find("EnvoyFilter", PayloadProcessingEnvoyFilterName("tenant-a"), "gateway-system")
	patches, _, _ := unstructured.NestedSlice(envoy.Object, "spec", "configPatches")
	foundTenantCluster := false
	foundGatewayCluster := false
	for _, raw := range patches {
		patch, ok := raw.(map[string]any)
		if !ok || patch["applyTo"] != "CLUSTER" {
			continue
		}
		body, _ := patch["patch"].(map[string]any)
		value, _ := body["value"].(map[string]any)
		clusterName, _ := value["name"].(string)
		var wantFQDN string
		switch clusterName {
		case "payload-processing-extproc":
			wantFQDN = postFQDN
		case "payload-pre-processing-extproc":
			wantFQDN = preFQDN
		default:
			continue
		}
		sni, _, _ := unstructured.NestedString(value, "transport_socket", "typed_config", "sni")
		endpoints, _, _ := unstructured.NestedSlice(value, "load_assignment", "endpoints")
		address := ""
		if len(endpoints) > 0 {
			endpoint, _ := endpoints[0].(map[string]any)
			lbEndpoints, _ := endpoint["lb_endpoints"].([]any)
			if len(lbEndpoints) > 0 {
				lbEndpoint, _ := lbEndpoints[0].(map[string]any)
				endpointSpec, _ := lbEndpoint["endpoint"].(map[string]any)
				addressSpec, _ := endpointSpec["address"].(map[string]any)
				socket, _ := addressSpec["socket_address"].(map[string]any)
				address, _ = socket["address"].(string)
			}
		}
		if address != wantFQDN || sni != wantFQDN {
			t.Fatalf("%s cluster address/SNI = %q/%q, want %q/%q", clusterName, address, sni, wantFQDN, wantFQDN)
		}
		if clusterName == "payload-processing-extproc" {
			foundTenantCluster = true
		} else {
			foundGatewayCluster = true
		}
	}
	if !foundTenantCluster {
		t.Fatal("EnvoyFilter shared post-auth cluster must target the Gateway-local Service FQDN")
	}
	if !foundGatewayCluster {
		t.Fatal("EnvoyFilter pre-auth cluster must remain Gateway-local")
	}
	for _, resource := range resources {
		if resource.GetKind() == "ClusterRoleBinding" && resource.GetName() == PayloadProcessingReaderClusterRoleBindingPostNameForTenant("tenant-a") {
			t.Fatal("post-auth ExtProc must not bind the shared MaaS reader ClusterRole")
		}
	}
	metricsAuth := false
	metricsReader := false
	for _, resource := range resources {
		if resource.GetKind() != "ClusterRoleBinding" {
			continue
		}
		switch resource.GetName() {
		case PayloadProcessingMetricsAuthClusterRoleBindingNameForTenant("tenant-a"):
			metricsAuth = true
		case PayloadProcessingMetricsReaderName:
			metricsReader = true
		case PayloadProcessingReaderClusterRoleBindingNameForTenant("tenant-a"),
			PayloadProcessingReaderClusterRoleBindingPostNameForTenant("tenant-a"):
			t.Fatalf("reader ClusterRoleBinding %q must be dropped", resource.GetName())
		}
	}
	if !metricsAuth || !metricsReader {
		t.Fatalf("metrics ClusterRoleBindings missing: auth=%t reader=%t", metricsAuth, metricsReader)
	}
}

func TestRemoveExternalModelResourcesLeavesSharedKServeResources(t *testing.T) {
	requireManifests(t)
	rendered, err := render.Build(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	resources := render.PostRender(rendered, render.Params{Namespace: "gateway-system", GatewayName: "gateway", Image: "extproc:dev"})
	resources, err = Rename(resources, "tenant-a", "gateway-system")
	if err != nil {
		t.Fatal(err)
	}
	resources, err = SplitPostAuthResources(resources, "tenant-a", "gateway-system", "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	resources = RemoveExternalModelResources(resources, "tenant-a")
	sharedFilter, externalFilter := false, false
	for _, resource := range resources {
		if resource.GetKind() != "EnvoyFilter" {
			continue
		}
		switch resource.GetName() {
		case PayloadProcessingEnvoyFilterName("tenant-a"):
			sharedFilter = true
		case PayloadProcessingExternalModelFilterNameForTenant("tenant-a"):
			externalFilter = true
		}
	}
	if !sharedFilter || externalFilter {
		t.Fatalf("shared EnvoyFilter present=%t, ExternalModel EnvoyFilter present=%t", sharedFilter, externalFilter)
	}
}

func TestSplitPostAuthResourcesSeparatesSameNamespaceServiceAccount(t *testing.T) {
	requireManifests(t)
	rendered, err := render.Build(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	resources := render.PostRender(rendered, render.Params{Namespace: "gateway-system", GatewayName: "gateway", Image: "extproc:dev"})
	resources, err = Rename(resources, "tenant-a", "gateway-system")
	if err != nil {
		t.Fatal(err)
	}
	resources, err = SplitPostAuthResources(resources, "tenant-a", "gateway-system", "gateway-system")
	if err != nil {
		t.Fatal(err)
	}
	external := false
	externalService := false
	externalConfig := false
	for _, resource := range resources {
		if resource.GetKind() == "ServiceAccount" && resource.GetName() == PayloadProcessingExternalModelServiceAccountName("tenant-a") {
			external = true
		}
		if resource.GetKind() == "Service" && resource.GetName() == PayloadProcessingExternalModelServiceName("tenant-a") && resource.GetNamespace() == "gateway-system" {
			externalService = true
		}
		if resource.GetKind() == "ConfigMap" && resource.GetName() == PayloadProcessingExternalModelPluginsConfigMapForTenant("tenant-a") && resource.GetNamespace() == "gateway-system" {
			externalConfig = true
		}
		if resource.GetKind() == "ClusterRoleBinding" && resource.GetName() == PayloadProcessingReaderClusterRoleBindingPostNameForTenant("tenant-a") {
			t.Fatal("same-namespace post-auth ExtProc must not bind the shared MaaS reader ClusterRole")
		}
	}
	if !external {
		t.Fatalf("missing same-namespace ExternalModel ServiceAccount %s", PayloadProcessingExternalModelServiceAccountName("tenant-a"))
	}
	if !externalService || !externalConfig {
		t.Fatalf("missing same-namespace ExternalModel Service or ConfigMap: service=%t config=%t", externalService, externalConfig)
	}
}
