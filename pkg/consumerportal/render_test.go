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

package consumerportal

import (
	"encoding/json"
	"slices"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/opendatahub-io/ai-gateway-controller/pkg/render"
	"github.com/opendatahub-io/ai-gateway-controller/pkg/tenant"
)

func renderTestResources(t *testing.T) []unstructured.Unstructured {
	t.Helper()
	built, err := render.Build(testManifestPath)
	if err != nil {
		t.Fatalf("render.Build: %v", err)
	}
	return Render(built, Params{
		Namespace:          testInfraNamespace,
		DashboardNamespace: DefaultDashboardNamespace,
		Image:              "quay.io/example/kube-auth-proxy:test",
		OIDCIssuerURL:      testIssuer,
		Hostname:           testHostname,
		GatewayName:        testGatewayName,
		GatewayNamespace:   testGatewayNamespace,
		AuthorinoNamespace: DefaultAuthorinoNamespace,
	})
}

func findResource(t *testing.T, resources []unstructured.Unstructured, kind, name string) unstructured.Unstructured {
	t.Helper()
	for _, resource := range resources {
		if resource.GetKind() == kind && resource.GetName() == name {
			return resource
		}
	}
	t.Fatalf("rendered resources missing %s %s", kind, name)
	return unstructured.Unstructured{}
}

func firstNestedMap(t *testing.T, object map[string]any, fields ...string) map[string]any {
	t.Helper()
	values, found, err := unstructured.NestedSlice(object, fields...)
	if err != nil || !found || len(values) == 0 {
		t.Fatalf("nested field %v not found (found=%t err=%v values=%v)", fields, found, err, values)
	}
	value, ok := values[0].(map[string]any)
	if !ok {
		t.Fatalf("nested field %v is not an object: %#v", fields, values[0])
	}
	return value
}

func TestRenderSubstitutesTenantValues(t *testing.T) {
	resources := renderTestResources(t)
	for _, kind := range []string{"Deployment", "Service", "ServiceAccount", "NetworkPolicy"} {
		u := findResource(t, resources, kind, AuthProxyName)
		if u.GetNamespace() != testInfraNamespace || u.GetName() != AuthProxyName {
			t.Errorf("%s = %s/%s, want %s/%s", kind, u.GetNamespace(), u.GetName(), testInfraNamespace, AuthProxyName)
		}
		if u.GetLabels()[tenant.LabelManagedBy] != tenant.ManagedByAIGatewayController {
			t.Errorf("%s missing managed-by label: %v", kind, u.GetLabels())
		}
		raw, err := json.Marshal(u.Object)
		if err != nil {
			t.Fatalf("marshal %s: %v", kind, err)
		}
		if strings.Contains(string(raw), "CONSUMER_PORTAL_") {
			t.Errorf("%s still contains an unreplaced token: %s", kind, raw)
		}
	}

	deployment := findResource(t, resources, "Deployment", AuthProxyName)
	containers, _, _ := unstructured.NestedSlice(deployment.Object, "spec", "template", "spec", "containers")
	if len(containers) != 1 {
		t.Fatalf("Deployment containers = %d, want 1", len(containers))
	}
	container, ok := containers[0].(map[string]any)
	if !ok {
		t.Fatalf("container is not an object: %#v", containers[0])
	}
	if container["image"] != "quay.io/example/kube-auth-proxy:test" {
		t.Errorf("image = %v", container["image"])
	}
	args, _, _ := unstructured.NestedStringSlice(container, "args")
	for _, want := range []string{
		"--provider=oidc",
		"--oidc-issuer-url=" + testIssuer,
		"--redirect-url=https://" + testHostname + "/oauth2/callback",
		"--cookie-domain=" + testHostname,
		"--https-address=0.0.0.0:4180",
	} {
		if !slices.Contains(args, want) {
			t.Errorf("auth proxy args missing %q: %v", want, args)
		}
	}

	policy := findResource(t, resources, "NetworkPolicy", AuthProxyName)
	raw, err := json.Marshal(policy.Object)
	if err != nil {
		t.Fatalf("marshal NetworkPolicy: %v", err)
	}
	for _, want := range []string{testGatewayNamespace, testGatewayName, DefaultAuthorinoNamespace} {
		if !strings.Contains(string(raw), want) {
			t.Errorf("NetworkPolicy does not reference %q: %s", want, raw)
		}
	}
}

func TestRenderBuildsConsumerPortalRoutes(t *testing.T) {
	resources := renderTestResources(t)
	portal := findResource(t, resources, "HTTPRoute", PortalRouteName)
	hostnames, _, _ := unstructured.NestedStringSlice(portal.Object, "spec", "hostnames")
	if len(hostnames) != 1 || hostnames[0] != testHostname {
		t.Errorf("portal HTTPRoute hostnames = %v, want [%s]", hostnames, testHostname)
	}
	parents, _, _ := unstructured.NestedSlice(portal.Object, "spec", "parentRefs")
	if len(parents) != 1 {
		t.Fatalf("portal parentRefs = %v, want one", parents)
	}
	parent, ok := parents[0].(map[string]any)
	if !ok || parent["name"] != testGatewayName || parent["namespace"] != testGatewayNamespace || parent["sectionName"] != "https" {
		t.Errorf("portal parentRef = %#v", parent)
	}

	rules, _, _ := unstructured.NestedSlice(portal.Object, "spec", "rules")
	if len(rules) != 2 {
		t.Fatalf("portal rules = %d, want 2", len(rules))
	}
	redirect, ok := rules[0].(map[string]any)
	if !ok {
		t.Fatalf("portal redirect rule is not an object: %#v", rules[0])
	}
	redirectPath, _, _ := unstructured.NestedString(firstNestedMap(t, redirect, "matches"), "path", "value")
	redirectTarget, _, _ := unstructured.NestedString(firstNestedMap(t, redirect, "filters"), "requestRedirect", "path", "replaceFullPath")
	if redirectPath != "/maas-portal" || redirectTarget != "/maas-portal/" {
		t.Errorf("bare-prefix redirect = %q -> %q", redirectPath, redirectTarget)
	}
	proxyRule, ok := rules[1].(map[string]any)
	if !ok {
		t.Fatalf("portal backend rule is not an object: %#v", rules[1])
	}
	rewrite, _, _ := unstructured.NestedString(firstNestedMap(t, proxyRule, "filters"), "urlRewrite", "path", "replacePrefixMatch")
	backend := firstNestedMap(t, proxyRule, "backendRefs")
	backendName, _, _ := unstructured.NestedString(backend, "name")
	backendNamespace, _, _ := unstructured.NestedString(backend, "namespace")
	backendPort, _, _ := unstructured.NestedInt64(backend, "port")
	if rewrite != "/" || backendName != PortalServiceName || backendNamespace != DefaultDashboardNamespace || backendPort != 8443 {
		t.Errorf("portal backend = rewrite %q, %s/%s:%d", rewrite, backendNamespace, backendName, backendPort)
	}

	oauth := findResource(t, resources, "HTTPRoute", OAuthRouteName)
	oauthRule := firstNestedMap(t, oauth.Object, "spec", "rules")
	oauthPath, _, _ := unstructured.NestedString(firstNestedMap(t, oauthRule, "matches"), "path", "value")
	oauthBackendRef := firstNestedMap(t, oauthRule, "backendRefs")
	oauthBackend, _, _ := unstructured.NestedString(oauthBackendRef, "name")
	oauthPort, _, _ := unstructured.NestedInt64(oauthBackendRef, "port")
	if oauthPath != "/oauth2" || oauthBackend != AuthProxyName || oauthPort != 4180 {
		t.Errorf("oauth backend = path %q, %s:%d", oauthPath, oauthBackend, oauthPort)
	}

	grant := findResource(t, resources, "ReferenceGrant", PortalRouteName)
	if grant.GetAPIVersion() != "gateway.networking.k8s.io/v1beta1" {
		t.Errorf("ReferenceGrant apiVersion = %s, want gateway.networking.k8s.io/v1beta1", grant.GetAPIVersion())
	}
	fromNamespace, _, _ := unstructured.NestedString(firstNestedMap(t, grant.Object, "spec", "from"), "namespace")
	toName, _, _ := unstructured.NestedString(firstNestedMap(t, grant.Object, "spec", "to"), "name")
	if grant.GetNamespace() != DefaultDashboardNamespace || fromNamespace != testInfraNamespace || toName != PortalServiceName {
		t.Errorf("ReferenceGrant = %s/%s from %s to %s", grant.GetNamespace(), grant.GetName(), fromNamespace, toName)
	}
}

func TestRenderBuildsBackendTLS(t *testing.T) {
	resources := renderTestResources(t)
	cases := []struct {
		name string
		host string
		port int64
	}{
		{name: PortalRouteName, host: PortalServiceName + "." + DefaultDashboardNamespace + ".svc.cluster.local", port: 8443},
		{name: AuthProxyName, host: AuthProxyName + "." + testInfraNamespace + ".svc.cluster.local", port: 4180},
	}
	for _, tc := range cases {
		rule := findResource(t, resources, "DestinationRule", tc.name+"-backend-tls")
		host, _, _ := unstructured.NestedString(rule.Object, "spec", "host")
		portSetting := firstNestedMap(t, rule.Object, "spec", "trafficPolicy", "portLevelSettings")
		port, _, _ := unstructured.NestedInt64(portSetting, "port", "number")
		mode, _, _ := unstructured.NestedString(portSetting, "tls", "mode")
		sni, _, _ := unstructured.NestedString(portSetting, "tls", "sni")
		if rule.GetNamespace() != testGatewayNamespace || host != tc.host || port != tc.port || mode != "SIMPLE" || sni != tc.host {
			t.Errorf("DestinationRule %s = %s/%s host=%q port=%d mode=%q sni=%q",
				tc.name, rule.GetNamespace(), rule.GetName(), host, port, mode, sni)
		}
	}
}

func TestRenderBuildsAuthPolicies(t *testing.T) {
	resources := renderTestResources(t)
	anonymous := findResource(t, resources, "AuthPolicy", OAuthRouteName+"-anonymous")
	anonymousTarget, _, _ := unstructured.NestedString(anonymous.Object, "spec", "targetRef", "name")
	if anonymousTarget != OAuthRouteName || anonymous.GetNamespace() != testInfraNamespace {
		t.Errorf("anonymous AuthPolicy target = %s in %s", anonymousTarget, anonymous.GetNamespace())
	}

	session := findResource(t, resources, "AuthPolicy", "maas-consumer-portal-session")
	sessionTarget, _, _ := unstructured.NestedString(session.Object, "spec", "targetRef", "name")
	rego, _, _ := unstructured.NestedString(session.Object, "spec", "rules", "authorization", "session", "opa", "rego")
	requireSessionPattern := firstNestedMap(t, session.Object,
		"spec", "rules", "authorization", "require-session", "patternMatching", "patterns")
	requireSession, _, _ := unstructured.NestedString(requireSessionPattern, "predicate")
	location, locationFound, locationErr := unstructured.NestedString(session.Object,
		"spec", "rules", "response", "unauthorized", "headers", "Location", "expression")
	raw, err := json.Marshal(session.Object)
	if err != nil {
		t.Fatalf("marshal session AuthPolicy: %v", err)
	}
	if sessionTarget != PortalRouteName || session.GetNamespace() != testInfraNamespace {
		t.Errorf("session AuthPolicy target = %s in %s", sessionTarget, session.GetNamespace())
	}
	wantProxyURL := "https://" + AuthProxyName + "." + testInfraNamespace + ".svc.cluster.local:4180/oauth2/auth"
	if !strings.Contains(rego, `"url": "`+wantProxyURL+`"`) || !strings.Contains(string(raw), `"code":302`) {
		t.Errorf("session AuthPolicy proxy URL or redirect is wrong: url present=%t",
			strings.Contains(rego, wantProxyURL))
	}
	for _, want := range []string{
		"x-forwarded-access-token",
		"/oauth2/start?rd=",
		"allow = true",
		"login_location := sprintf",
		"urlquery.encode(input.context.request.http.path)",
		"{200, 202}[response.status_code]",
		"no-store",
		"/var/run/secrets/kubernetes.io/serviceaccount/service-ca.crt",
	} {
		if !strings.Contains(rego, want) && !strings.Contains(string(raw), want) {
			t.Errorf("session AuthPolicy missing %q", want)
		}
	}
	if requireSession != "auth.authorization.session.identity.access_token != \"\"" {
		t.Errorf("session requirement = %q", requireSession)
	}
	if !locationFound || locationErr != nil || location != "auth.authorization.session.login_location" {
		t.Errorf("session redirect expression = %q found=%t err=%v, policy=%s", location, locationFound, locationErr, raw)
	}
}

func TestListenerHostname(t *testing.T) {
	cases := map[string]struct {
		listeners []map[string]any
		want      string
	}{
		"https listener": {listeners: []map[string]any{httpsListener(testHostname)}, want: testHostname},
		"skips wildcard and http": {listeners: []map[string]any{
			httpsListener("*.apps.example.com"),
			{"name": "http", "protocol": "HTTP", "hostname": "plain.example.com"},
			httpsListener(testHostname),
		}, want: testHostname},
		"no hostname": {listeners: []map[string]any{{"name": "https", "protocol": "HTTPS"}}, want: ""},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if got := ListenerHostname(newGateway(tc.listeners...)); got != tc.want {
				t.Errorf("ListenerHostname = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestPortalEnabledDefaultsTrue(t *testing.T) {
	if !PortalEnabled(newAITenant(aitenantOpts{})) {
		t.Error("consumer portal should default to enabled")
	}
	if PortalEnabled(newAITenant(aitenantOpts{portalDisabled: true})) {
		t.Error("consumerPortal.enabled=false was ignored")
	}
}

func TestRenderAddsProviderCAFilesPerKey(t *testing.T) {
	built, err := render.Build(testManifestPath)
	if err != nil {
		t.Fatalf("render.Build: %v", err)
	}
	argsFor := func(keys []string) []string {
		for _, u := range Render(built, Params{Namespace: testInfraNamespace, ProviderCAKeys: keys}) {
			if u.GetKind() != "Deployment" {
				continue
			}
			containers, _, _ := unstructured.NestedSlice(u.Object, "spec", "template", "spec", "containers")
			container, ok := containers[0].(map[string]any)
			if !ok {
				t.Fatalf("container is not an object: %#v", containers[0])
			}
			args, _, _ := unstructured.NestedStringSlice(container, "args")
			return args
		}
		t.Fatal("no Deployment rendered")
		return nil
	}

	if args := argsFor([]string{"ca-bundle.crt"}); !slices.Contains(args, "--provider-ca-file=/etc/pki/odh-trusted-ca/ca-bundle.crt") ||
		slices.Contains(args, "--provider-ca-file=/etc/pki/odh-trusted-ca/odh-ca-bundle.crt") {
		t.Errorf("args for [ca-bundle.crt] = %v", args)
	}
	for _, arg := range argsFor(nil) {
		if strings.HasPrefix(arg, "--provider-ca-file=") {
			t.Errorf("unexpected %q without CA keys", arg)
		}
	}
}

func TestRenderSetsCABundleHashAnnotation(t *testing.T) {
	built, err := render.Build(testManifestPath)
	if err != nil {
		t.Fatalf("render.Build: %v", err)
	}
	params := Params{
		Namespace:    testInfraNamespace,
		CABundleHash: "ca-bundle-hash",
	}
	deployment := unstructured.Unstructured{}
	for _, u := range Render(built, params) {
		if u.GetKind() == "Deployment" {
			deployment = u
			break
		}
	}
	hash, _, _ := unstructured.NestedString(deployment.Object,
		"spec", "template", "metadata", "annotations", CABundleHashAnnotation)
	if hash != "ca-bundle-hash" {
		t.Errorf("CA bundle hash annotation = %q, want ca-bundle-hash", hash)
	}

	params.CABundleHash = ""
	for _, u := range Render(built, params) {
		if u.GetKind() != "Deployment" {
			continue
		}
		hash, found, _ := unstructured.NestedString(u.Object,
			"spec", "template", "metadata", "annotations", CABundleHashAnnotation)
		if found || hash != "" {
			t.Errorf("empty CA bundle hash rendered annotation %q", hash)
		}
	}
}
