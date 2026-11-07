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
	"maps"
	"slices"
	"strings"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

// TestNemoCallerRBACGrantsTheCrossNamespaceService pins the grant to the
// Service kube-rbac-proxy authorizes. In ODH and RHOAI, TrustyAI places the
// workload in a shared infra namespace and names its Service
// <cr-namespace>-<cr-name>; its proxy checks get on that Service, not on the
// NemoGuardrails name.
func TestNemoCallerRBACGrantsTheCrossNamespaceService(t *testing.T) {
	policy := acceptedPolicy("safety", 3, 3, "https://tenant-a-nemo.odh-ai-gateway-infra.svc.cluster.local")
	callouts, err := calloutsFor(t, policy)
	if err != nil {
		t.Fatalf("acceptedNemoCallouts: %v", err)
	}
	rbac := nemoCallerRBAC(callouts, "tenant-a", "gw", PayloadProcessingPostServiceAccountName("tenant-a"))
	if len(rbac) != 2 {
		t.Fatalf("RBAC = %v, want one Role and one RoleBinding", rbac)
	}
	role := rbac[0]
	if role.GetKind() != "Role" || role.GetNamespace() != "odh-ai-gateway-infra" {
		t.Fatalf("Role = %s %s/%s, want a Role in odh-ai-gateway-infra", role.GetKind(), role.GetNamespace(), role.GetName())
	}
	rules, _, _ := unstructured.NestedSlice(role.Object, "rules")
	if len(rules) != 1 {
		t.Fatalf("Role rules = %v, want one rule", rules)
	}
	if names, _, err := unstructured.NestedStringSlice(testObject(t, rules[0]), "resourceNames"); err != nil || !slices.Equal(names, []string{"tenant-a-nemo"}) {
		t.Fatalf("Role resourceNames = %v, %v; want the tenant-a-nemo Service, not the NemoGuardrails name", names, err)
	}
}

// TestConfigureNemoCalloutsAuthenticatedProvider pins the kube-rbac-proxy
// path: the https check carries the pod token and service CA, the pod mounts
// both, egress opens the proxy port, and the pod's ServiceAccount is granted
// get on the NeMo Service in the NeMo namespace. The auth-off check beside it
// stays plain.
func TestConfigureNemoCalloutsAuthenticatedProvider(t *testing.T) {
	resources := nemoCalloutFixtures("red", "gw")
	containers := []any{map[string]any{"name": "payload-processing", "volumeMounts": []any{
		map[string]any{"name": "plugins-config-volume", "mountPath": "/etc/praxis"},
	}}}
	if err := unstructured.SetNestedSlice(resources[0].Object, containers, "spec", "template", "spec", "containers"); err != nil {
		t.Fatal(err)
	}
	callouts := []nemoCallout{
		{name: "secure", endpoint: "https://nemo.infra.svc.cluster.local", serviceName: "nemo", serviceNamespace: "infra", authenticated: true,
			checks: []nemoCheck{{configID: "pii-v1", header: "h-pii", request: true}}},
		{name: "plain", endpoint: "http://open.lab.svc", serviceName: "open", serviceNamespace: "lab",
			checks: []nemoCheck{{configID: "tox-v1", header: "h-tox", request: true}}},
	}
	resources, err := configureNemoCallouts(resources, "red", "gw", "red", callouts)
	if err != nil {
		t.Fatalf("configureNemoCallouts: %v", err)
	}

	data, _, _ := unstructured.NestedStringMap(findResource(resources, "ConfigMap", PayloadProcessingPluginsConfigMapForTenant("red"), "gw").Object, "data")
	_, filters := parseCompiled(t, data["extproc.yaml"])
	var plain, secure map[string]any
	for _, filter := range filters {
		provider, _ := filter.(map[string]any)["provider"].(map[string]any)
		switch provider["endpoint"] {
		case "http://open.lab.svc/v1/checks":
			plain = provider
		case "https://nemo.infra.svc.cluster.local/v1/checks":
			secure = provider
		}
	}
	if plain == nil || secure == nil {
		t.Fatalf("filters = %v, want both checks compiled", filters)
	}
	if _, ok := plain["token_file"]; ok || plain["tls"] != nil {
		t.Fatalf("auth-off check = %v, want no token_file or tls", plain)
	}
	tls, _ := secure["tls"].(map[string]any)
	if secure["endpoint"] != "https://nemo.infra.svc.cluster.local/v1/checks" || secure["token_file"] != nemoTokenFile || tls["ca_file"] != nemoServiceCAFile {
		t.Fatalf("authenticated check = %v, want the https endpoint with token_file and tls.ca_file", secure)
	}

	assertNemoAuthMounted(t, findResource(resources, "Deployment", PayloadProcessingDeploymentName("red"), "gw"))

	egress, _, _ := unstructured.NestedSlice(findResource(resources, "NetworkPolicy", PayloadProcessingNetworkPolicyName("red"), "gw").Object, "spec", "egress")
	if len(egress) != 3 {
		t.Fatalf("egress = %v, want the rendered rule plus one rule per NeMo port", egress)
	}
	for i, want := range []struct {
		port      int64
		namespace string
	}{{nemoServerPort, "lab"}, {nemoProxyPort, "infra"}} {
		rule := testObject(t, egress[i+1])
		ports, _, _ := unstructured.NestedSlice(rule, "ports")
		to, _, _ := unstructured.NestedSlice(rule, "to")
		if len(ports) != 1 || len(to) != 1 {
			t.Fatalf("egress rule %d = %v, want one port and one peer", i, rule)
		}
		peer, _, _ := unstructured.NestedString(testObject(t, to[0]), "namespaceSelector", "matchLabels", "kubernetes.io/metadata.name")
		if testObject(t, ports[0])["port"] != want.port || peer != want.namespace {
			t.Fatalf("egress rule %d = %v, want TCP %d to %s only", i, rule, want.port, want.namespace)
		}
	}

	assertNemoCallerRBAC(t, resources)
}

// assertNemoAuthMounted checks the post-auth pod mounts its ServiceAccount
// token, with the default audience, and the OpenShift service CA.
func assertNemoAuthMounted(t *testing.T, deployment *unstructured.Unstructured) {
	t.Helper()
	volumes, _, _ := unstructured.NestedSlice(deployment.Object, "spec", "template", "spec", "volumes")
	var projected []any
	for _, volume := range volumes {
		if v := testObject(t, volume); v["name"] == nemoAuthVolumeName {
			projected, _, _ = unstructured.NestedSlice(v, "projected", "sources")
		}
	}
	if len(projected) != 2 {
		t.Fatalf("volumes = %v, want a projected token and service CA volume", volumes)
	}
	token, _, _ := unstructured.NestedMap(testObject(t, projected[0]), "serviceAccountToken")
	if _, hasAudience := token["audience"]; hasAudience || token["path"] != "token" {
		t.Fatalf("token source = %v, want the default audience at path token", token)
	}
	if name, _, _ := unstructured.NestedString(testObject(t, projected[1]), "configMap", "name"); name != openShiftServiceCAConfigMap {
		t.Fatalf("CA source = %v, want ConfigMap %s", projected[1], openShiftServiceCAConfigMap)
	}
	containers, _, _ := unstructured.NestedSlice(deployment.Object, "spec", "template", "spec", "containers")
	if len(containers) == 0 {
		t.Fatal("payload-processing has no containers")
	}
	mounts, _, _ := unstructured.NestedSlice(testObject(t, containers[0]), "volumeMounts")
	hasPraxis, hasNemo := false, false
	for _, mount := range mounts {
		switch testObject(t, mount)["mountPath"] {
		case "/etc/praxis":
			hasPraxis = true
		case nemoAuthMountPath:
			hasNemo = true
		}
	}
	if !hasPraxis || !hasNemo {
		t.Fatalf("volumeMounts = %v, want /etc/praxis and %s (TLS mount may also be present)", mounts, nemoAuthMountPath)
	}
}

func TestNemoCallerName(t *testing.T) {
	if got, want := nemoCallerName("openshift-ingress", "payload-processing", "models-as-a-service-nemo-auth"),
		"nemo-caller.openshift-ingress.payload-processing.models-as-a-service-nemo-auth"; got != want {
		t.Fatalf("name = %q, want %q", got, want)
	}
	// Joined with dashes these two callers would collide in the Service's
	// namespace.
	if nemoCallerName("a-b", "c", "nemo") == nemoCallerName("a", "b-c", "nemo") {
		t.Fatal("different callers share a name")
	}
	longest := nemoCallerName(strings.Repeat("g", 63), PayloadProcessingPostServiceAccountName(strings.Repeat("t", 63)), strings.Repeat("s", 63))
	if errs := validation.IsDNS1123Subdomain(longest); len(errs) > 0 {
		t.Fatalf("longest name %q is not a valid Role name: %v", longest, errs)
	}
}

// assertNemoCallerRBAC checks the only grant is get on services/nemo in infra,
// bound to the red tenant's post-auth ServiceAccount in gw.
func assertNemoCallerRBAC(t *testing.T, resources []unstructured.Unstructured) {
	t.Helper()
	name := nemoCallerName("gw", PayloadProcessingPostServiceAccountName("red"), "nemo")
	role := findResource(resources, "Role", name, "infra")
	binding := findResource(resources, "RoleBinding", name, "infra")
	if role == nil || binding == nil || len(resources) != 6 {
		t.Fatalf("resources = %d, want the fixtures plus one Role and RoleBinding in infra", len(resources))
	}
	rules, _, _ := unstructured.NestedSlice(role.Object, "rules")
	if len(rules) != 1 {
		t.Fatalf("Role rules = %v, want one rule", rules)
	}
	rule := testObject(t, rules[0])
	for field, want := range map[string][]string{"resources": {"services"}, "resourceNames": {"nemo"}, "verbs": {"get"}} {
		if got, _, err := unstructured.NestedStringSlice(rule, field); err != nil || !slices.Equal(got, want) {
			t.Fatalf("Role rule %s = %v, %v; want %v", field, got, err, want)
		}
	}
	subjects, _, _ := unstructured.NestedSlice(binding.Object, "subjects")
	if len(subjects) != 1 {
		t.Fatalf("RoleBinding subjects = %v, want one subject", subjects)
	}
	subject := testObject(t, subjects[0])
	if subject["kind"] != "ServiceAccount" || subject["name"] != PayloadProcessingPostServiceAccountName("red") || subject["namespace"] != "gw" {
		t.Fatalf("RoleBinding subjects = %v, want the post-auth ServiceAccount in gw", subjects)
	}
	if ref, _, _ := unstructured.NestedString(binding.Object, "roleRef", "name"); ref != name {
		t.Fatalf("roleRef = %q, want %q", ref, name)
	}
	for _, obj := range []*unstructured.Unstructured{role, binding} {
		if !maps.Equal(obj.GetLabels(), nemoCallerLabels("red")) {
			t.Fatalf("%s labels = %v, want the controller ownership and red tenant labels", obj.GetKind(), obj.GetLabels())
		}
	}
}

func testObject(t *testing.T, v any) map[string]any {
	t.Helper()
	m, ok := v.(map[string]any)
	if !ok {
		t.Fatalf("%v is %T, want an object", v, v)
	}
	return m
}

// TestPruneNemoCallerRBAC pins that a grant that falls out of the render is
// deleted, including one the tenant left on a Gateway it moved away from, the
// current one is kept, and another tenant's is left alone. It reads through
// APIReader, so the controller never caches Roles or RoleBindings.
func TestPruneNemoCallerRBAC(t *testing.T) {
	serviceAccount := PayloadProcessingPostServiceAccountName("red")
	keep := nemoCallerRBAC([]nemoCallout{{serviceName: "nemo", serviceNamespace: "infra", authenticated: true}}, "red", "gw", serviceAccount)
	stale := nemoCallerRBAC([]nemoCallout{{serviceName: "old", serviceNamespace: "elsewhere", authenticated: true}}, "red", "gw", serviceAccount)
	rehomed := nemoCallerRBAC([]nemoCallout{{serviceName: "nemo", serviceNamespace: "infra", authenticated: true}}, "red", "gw-old", serviceAccount)
	other := nemoCallerRBAC([]nemoCallout{{serviceName: "nemo", serviceNamespace: "infra", authenticated: true}}, "blue", "gw", PayloadProcessingPostServiceAccountName("blue"))
	builder := fake.NewClientBuilder().WithScheme(mtcSchemeForTests())
	for _, set := range [][]unstructured.Unstructured{keep, stale, rehomed, other} {
		for i := range set {
			builder = builder.WithObjects(set[i].DeepCopy())
		}
	}
	c := builder.Build()
	r := &Reconciler{Client: failingClient{c}, APIReader: c}
	if err := r.pruneNemoCallerRBAC(context.Background(), "red", keep); err != nil {
		t.Fatalf("pruneNemoCallerRBAC: %v", err)
	}
	for _, c := range []struct {
		set  []unstructured.Unstructured
		want bool
		what string
	}{
		{keep, true, "current"},
		{stale, false, "stale"},
		{rehomed, false, "left-behind Gateway"},
		{other, true, "another tenant's"},
	} {
		for _, obj := range c.set {
			if got := objectExists(t, r.APIReader, obj); got != c.want {
				t.Fatalf("%s %s %s/%s exists = %t, want %t", c.what, obj.GetKind(), obj.GetNamespace(), obj.GetName(), got, c.want)
			}
		}
	}
}

// TestReconcileAppliesAndPrunesNemoCallerGrants drives a whole praxis
// reconcile and then a tenant delete with an authenticated provider attached.
// The reconcile must apply the grant for the dedicated post-auth
// ServiceAccount after everything else, mount its token, and prune the grant
// the tenant left on its previous Gateway; the delete must remove every grant.
func TestReconcileAppliesAndPrunesNemoCallerGrants(t *testing.T) {
	requireManifests(t)
	const (
		tenantNamespace = "tenant-a"
		gatewayNS       = "openshift-ingress"
		nemoNamespace   = "odh-ai-gateway-infra"
		nemoService     = "tenant-a-nemo"
	)
	serviceAccount := PayloadProcessingPostServiceAccountName("redteam")
	mtc := withMarker(newMTC(tenantNamespace, "redteam", PayloadProcessingBackendPraxis, "redteam", "ai-tenants"), PayloadProcessingStatusCleanupComplete)
	mtc.Object["spec"] = map[string]any{"guardrails": []any{map[string]any{"ref": map[string]any{"name": "secure"}}}}
	if err := unstructured.SetNestedField(mtc.Object, nemoNamespace, "status", "infraNamespace"); err != nil {
		t.Fatal(err)
	}
	policy := acceptedPolicy("secure", 3, 3, "https://"+nemoService+"."+nemoNamespace+".svc.cluster.local")
	current := nemoCallerRBAC([]nemoCallout{{serviceName: nemoService, serviceNamespace: nemoNamespace, authenticated: true}}, tenantNamespace, gatewayNS, serviceAccount)
	rehomed := nemoCallerRBAC([]nemoCallout{{serviceName: nemoService, serviceNamespace: nemoNamespace, authenticated: true}}, tenantNamespace, "gw-old", serviceAccount)
	objects := []client.Object{mtc, policy,
		newAITenantOwner("redteam", "ai-tenants", AITenantPhaseActive, "my-gateway", gatewayNS, tenantNamespace),
		newGateway("my-gateway", gatewayNS)}
	for _, set := range [][]unstructured.Unstructured{current, rehomed} {
		for i := range set {
			objects = append(objects, set[i].DeepCopy())
		}
	}

	var applied []unstructured.Unstructured
	fakeClient := fake.NewClientBuilder().WithScheme(mtcSchemeForTests()).WithObjects(objects...).
		WithInterceptorFuncs(interceptor.Funcs{
			// The fake client cannot server-side apply; keep what was applied.
			Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
				if patch.Type() != types.ApplyPatchType {
					return c.Patch(ctx, obj, patch, opts...)
				}
				if u, ok := obj.(*unstructured.Unstructured); ok {
					applied = append(applied, *u.DeepCopy())
				}
				return nil
			},
		}).Build()
	r := &Reconciler{Client: fakeClient, APIReader: fakeClient, ManifestPath: manifestPath, Image: "img", MaaSAPIRouteNameBase: "maas-api-route", ResyncInterval: time.Minute}
	if _, err := r.Reconcile(context.Background(), mtcRequest(tenantNamespace)); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	name := nemoCallerName(gatewayNS, serviceAccount, nemoService)
	if len(applied) < 2 || applied[len(applied)-2].GetKind() != "Role" || applied[len(applied)-1].GetKind() != "RoleBinding" ||
		applied[len(applied)-1].GetName() != name || applied[len(applied)-1].GetNamespace() != nemoNamespace {
		t.Fatalf("last applied = %v, want the Role and RoleBinding %s/%s after the dataplane", appliedKeys(applied), nemoNamespace, name)
	}
	deployment := findResource(applied, "Deployment", PayloadProcessingDeploymentName("redteam"), gatewayNS)
	if deployment == nil {
		t.Fatalf("applied = %v, want the post-auth Deployment", appliedKeys(applied))
	}
	if got, _, _ := unstructured.NestedString(deployment.Object, "spec", "template", "spec", "serviceAccountName"); got != serviceAccount {
		t.Fatalf("post-auth ServiceAccount = %q, want %q", got, serviceAccount)
	}
	assertNemoAuthMounted(t, deployment)
	if findResource(applied, "ServiceAccount", serviceAccount, gatewayNS) == nil {
		t.Fatalf("applied = %v, want ServiceAccount %s/%s", appliedKeys(applied), gatewayNS, serviceAccount)
	}
	for _, obj := range current {
		if !objectExists(t, fakeClient, obj) {
			t.Fatalf("current %s %s was pruned", obj.GetKind(), obj.GetName())
		}
	}
	for _, obj := range rehomed {
		if objectExists(t, fakeClient, obj) {
			t.Fatalf("%s %s/%s from the previous Gateway was kept", obj.GetKind(), obj.GetNamespace(), obj.GetName())
		}
	}

	live := &unstructured.Unstructured{}
	live.SetGroupVersionKind(MaasTenantConfigGVK)
	if err := fakeClient.Get(context.Background(), client.ObjectKeyFromObject(mtc), live); err != nil {
		t.Fatalf("get MaasTenantConfig: %v", err)
	}
	if err := fakeClient.Delete(context.Background(), live); err != nil {
		t.Fatalf("delete MaasTenantConfig: %v", err)
	}
	if _, err := r.Reconcile(context.Background(), mtcRequest(tenantNamespace)); err != nil {
		t.Fatalf("Reconcile after delete: %v", err)
	}
	for _, obj := range current {
		if objectExists(t, fakeClient, obj) {
			t.Fatalf("%s %s survived tenant cleanup", obj.GetKind(), obj.GetName())
		}
	}
}

func appliedKeys(resources []unstructured.Unstructured) []string {
	keys := make([]string, 0, len(resources))
	for i := range resources {
		keys = append(keys, resources[i].GetKind()+" "+resources[i].GetNamespace()+"/"+resources[i].GetName())
	}
	return keys
}

// failingClient fails cached reads, so a test can show a path reads live.
type failingClient struct{ client.Client }

func (failingClient) List(context.Context, client.ObjectList, ...client.ListOption) error {
	return errors.New("cached List used")
}

func objectExists(t *testing.T, reader client.Reader, obj unstructured.Unstructured) bool {
	t.Helper()
	got := &unstructured.Unstructured{}
	got.SetGroupVersionKind(obj.GroupVersionKind())
	err := reader.Get(context.Background(), client.ObjectKeyFromObject(&obj), got)
	if err != nil && !apierrors.IsNotFound(err) {
		t.Fatalf("get %s %s: %v", obj.GetKind(), obj.GetName(), err)
	}
	return err == nil
}
