/*
Copyright 2026 The opendatahub.io Authors.

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
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/opendatahub-io/ai-gateway-controller/pkg/render"
)

const (
	chainTestTenant              = "redteam"
	gatewayNamespaceForChainTest = "openshift-ingress"
)

// TestChainsHoldTheirInvariants pins the two descriptors. The assertions are
// deliberately explicit rather than derived: if someone flips a field, this
// test should say which fact changed, not merely that both sides agree.
func TestChainsHoldTheirInvariants(t *testing.T) {
	for _, c := range Chains() {
		if violations := c.InvariantViolations(); len(violations) > 0 {
			t.Errorf("chain %s: %v", c.Position, violations)
		}
	}

	if !preAuthChain.MountsServiceAccountToken || !preAuthChain.AuthenticatesMetrics || !preAuthChain.MetricsAuthBinding {
		t.Errorf("pre-auth chain lost a leg of its metrics identity: %+v", preAuthChain)
	}
	if postAuthChain.MountsServiceAccountToken {
		t.Error("post-auth chain mounts a ServiceAccount token; it must hold no API identity")
	}
	if postAuthChain.AuthenticatesMetrics || postAuthChain.MetricsAuthBinding {
		t.Errorf("post-auth chain authenticates or is bound for metrics: %+v", postAuthChain)
	}

	if got, err := ChainForPosition(ChainPostAuth); err != nil || got.Position != ChainPostAuth {
		t.Fatalf("ChainForPosition(PostAuth) = %+v, %v", got, err)
	}
	if _, err := ChainForPosition("Sideways"); err == nil {
		t.Fatal("ChainForPosition accepted an unknown position")
	}
}

// TestChainsMatchRenderedResources is the conformance check the four-way fix
// needed and did not have: it walks the real render -> rename -> configure
// pipeline for one tenant and requires every resource that carries a chain's
// identity to agree with that chain's descriptor. A vendor sync, a rename
// helper, or a config generator that reintroduces the token fails here.
func TestChainsMatchRenderedResources(t *testing.T) {
	requireManifests(t)

	const gatewayNamespace = gatewayNamespaceForChainTest
	rendered, err := render.Build(manifestPath)
	if err != nil {
		t.Fatalf("render.Build: %v", err)
	}
	resources := render.PostRender(rendered, render.Params{
		Namespace:             gatewayNamespace,
		GatewayName:           "maas-default-gateway",
		Image:                 "quay.io/example/praxis-extproc:test",
		MaaSAPIRouteName:      "maas-api-route",
		MaaSAPIRouteNamespace: gatewayNamespace,
	})
	resources, err = Rename(resources, chainTestTenant, gatewayNamespace)
	if err != nil {
		t.Fatalf("Rename: %v", err)
	}
	const maxBodyBytes = int64(10 << 20)
	if err := configureSharedExtProc(resources, chainTestTenant, gatewayNamespace, false, maxBodyBytes, DefaultExtProcResponseBodyMode); err != nil {
		t.Fatalf("configureSharedExtProc: %v", err)
	}

	for _, c := range Chains() {
		t.Run(string(c.Position), func(t *testing.T) {
			deploy := requireResource(t, resources, "Deployment", c.DeploymentName(chainTestTenant))
			assertServiceAccountName(t, deploy, c.ServiceAccountName(chainTestTenant))
			assertPodTokenMount(t, deploy, c.MountsServiceAccountToken)

			sa := requireResource(t, resources, "ServiceAccount", c.ServiceAccountName(chainTestTenant))
			assertServiceAccountTokenMount(t, sa, c.MountsServiceAccountToken)
		})
	}

	// One metrics-auth binding per tenant, bound to the chain declared to hold
	// it. The binding is tenant-scoped rather than per-chain, so it is checked
	// once against both descriptors instead of inside the loop.
	binding := findResource(resources, "ClusterRoleBinding", PayloadProcessingMetricsAuthClusterRoleBindingNameForTenant(chainTestTenant), "")
	assertMetricsBinding(t, binding, preAuthChain)
	for _, c := range Chains() {
		if c.MetricsAuthBinding {
			continue
		}
		if names := bindingsNamingSubject(resources, c.ServiceAccountName(chainTestTenant)); len(names) > 0 {
			t.Errorf("%s ServiceAccount is bound by %v; it must hold no API permissions", c.Position, names)
		}
	}

	// The two scrape-wiring facts: the tenant ServiceMonitor sends a
	// credential only to a chain that authenticates, and the chain's own
	// configuration agrees about authenticating at all.
	monitor := requireResource(t, resources, "ServiceMonitor", PayloadProcessingMetricsMonitorNameForTenant(chainTestTenant))
	assertMonitorCredentials(t, monitor, postAuthChain)

	plugins := requireResource(t, resources, "ConfigMap", PayloadProcessingPluginsConfigMapForTenant(chainTestTenant))
	data, found, err := unstructured.NestedStringMap(plugins.Object, "data")
	if err != nil || !found {
		t.Fatalf("plugins ConfigMap data: found=%v err=%v", found, err)
	}
	if cfg := data["extproc.yaml"]; !strings.Contains(cfg, postAuthChain.MetricsAuthYAML()) {
		t.Errorf("generated extproc.yaml disagrees with postAuthChain.MetricsAuthYAML()=%q:\n%s",
			postAuthChain.MetricsAuthYAML(), cfg)
	}
	if cfg := data["pre-extproc.yaml"]; !strings.Contains(cfg, preAuthChain.MetricsAuthYAML()) {
		t.Errorf("vendored pre-extproc.yaml disagrees with preAuthChain.MetricsAuthYAML()=%q:\n%s",
			preAuthChain.MetricsAuthYAML(), cfg)
	}
}

func requireResource(t *testing.T, resources []unstructured.Unstructured, kind, name string) *unstructured.Unstructured {
	t.Helper()
	u := findResource(resources, kind, name, gatewayNamespaceForChainTest)
	if u == nil {
		t.Fatalf("rendered %s %q is missing", kind, name)
	}
	return u
}

func assertServiceAccountName(t *testing.T, deploy *unstructured.Unstructured, want string) {
	t.Helper()
	got, _, err := unstructured.NestedString(deploy.Object, "spec", "template", "spec", "serviceAccountName")
	if err != nil {
		t.Fatalf("read serviceAccountName: %v", err)
	}
	if got != want {
		t.Errorf("Deployment %s runs as ServiceAccount %q, want %q", deploy.GetName(), got, want)
	}
}

// assertPodTokenMount checks the pod-level automount setting. An absent field
// means the ServiceAccount's setting applies, which is true unless that
// ServiceAccount says otherwise; the chain's descriptor states the effective
// answer, so the pipeline must set it explicitly.
func assertPodTokenMount(t *testing.T, deploy *unstructured.Unstructured, want bool) {
	t.Helper()
	got, found, err := unstructured.NestedBool(deploy.Object, "spec", "template", "spec", "automountServiceAccountToken")
	if err != nil {
		t.Fatalf("read automountServiceAccountToken: %v", err)
	}
	if !found {
		t.Fatalf("Deployment %s does not set pod-level automountServiceAccountToken explicitly", deploy.GetName())
	}
	if got != want {
		t.Errorf("Deployment %s pod automountServiceAccountToken = %v, want %v", deploy.GetName(), got, want)
	}
}

func assertServiceAccountTokenMount(t *testing.T, sa *unstructured.Unstructured, want bool) {
	t.Helper()
	got, found, err := unstructured.NestedBool(sa.Object, "automountServiceAccountToken")
	if err != nil {
		t.Fatalf("read automountServiceAccountToken: %v", err)
	}
	// An absent field defaults to true.
	if !found {
		if want {
			return
		}
		t.Errorf("ServiceAccount %s leaves automountServiceAccountToken unset (defaults true), want false", sa.GetName())
		return
	}
	if got != want {
		t.Errorf("ServiceAccount %s automountServiceAccountToken = %v, want %v", sa.GetName(), got, want)
	}
}

// assertMetricsBinding requires the metrics-auth ClusterRoleBinding to exist
// for the chain declared to hold it, and to name that chain's ServiceAccount
// and no other.
func assertMetricsBinding(t *testing.T, binding *unstructured.Unstructured, c Chain) {
	t.Helper()
	if !c.MetricsAuthBinding {
		t.Fatalf("assertMetricsBinding called for the unbound chain %s", c.Position)
	}
	if binding == nil {
		t.Fatalf("metrics-auth binding is missing for %s", c.Position)
	}
	subjects, found, err := unstructured.NestedSlice(binding.Object, "subjects")
	if err != nil || !found || len(subjects) == 0 {
		t.Fatalf("read binding subjects: found=%v err=%v", found, err)
	}
	subject, ok := subjects[0].(map[string]any)
	if !ok {
		t.Fatalf("binding subject is %T, want map[string]any", subjects[0])
	}
	if got, want := subject["name"], c.ServiceAccountName(chainTestTenant); got != want {
		t.Errorf("metrics-auth binding binds %v, want %q", got, want)
	}
}

// bindingsNamingSubject returns the names of every ClusterRoleBinding in the
// rendered set whose subjects include serviceAccountName. Bindings are
// cluster-scoped, so no namespace filter applies.
func bindingsNamingSubject(resources []unstructured.Unstructured, serviceAccountName string) []string {
	var names []string
	for i := range resources {
		u := &resources[i]
		if u.GetKind() != "ClusterRoleBinding" {
			continue
		}
		subjects, found, err := unstructured.NestedSlice(u.Object, "subjects")
		if err != nil || !found {
			continue
		}
		for _, raw := range subjects {
			subject, ok := raw.(map[string]any)
			if !ok {
				continue
			}
			if subject["kind"] == "ServiceAccount" && subject["name"] == serviceAccountName {
				names = append(names, u.GetName())
				break
			}
		}
	}
	return names
}

func assertMonitorCredentials(t *testing.T, monitor *unstructured.Unstructured, c Chain) {
	t.Helper()
	endpoints, found, err := unstructured.NestedSlice(monitor.Object, "spec", "endpoints")
	if err != nil || !found {
		t.Fatalf("read spec.endpoints: found=%v err=%v", found, err)
	}
	for i, raw := range endpoints {
		endpoint, ok := raw.(map[string]any)
		if !ok {
			t.Fatalf("endpoints[%d] is %T, want map[string]any", i, raw)
		}
		for _, field := range serviceMonitorCredentialFields {
			_, present := endpoint[field]
			if present == c.AuthenticatesMetrics {
				continue
			}
			if present {
				t.Errorf("endpoints[%d] sends %q to a chain that does not authenticate", i, field)
			} else {
				t.Errorf("endpoints[%d] is missing %q for a chain that authenticates", i, field)
			}
		}
	}
}
