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
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/opendatahub-io/ai-gateway-controller/pkg/tenant"
)

// TrustedCABundleConfigMap is the platform-injected CA bundle mounted into the
// auth proxy at trustedCAMountPath so it can verify the OIDC issuer.
const (
	TrustedCABundleConfigMap = "odh-trusted-ca-bundle"
	trustedCAMountPath       = "/etc/pki/odh-trusted-ca"
	CABundleHashAnnotation   = "ai-gateway-controller.opendatahub.io/ca-bundle-hash"
	// LabelPortalInstance identifies which MaasTenantConfig instance a
	// rendered portal object belongs to. Cross-namespace children (the
	// ReferenceGrant lives in the dashboard namespace, DestinationRules in the
	// gateway namespace) cannot be scoped by namespace alone, so cleanup and
	// grant pruning key on this label instead.
	LabelPortalInstance = "ai-gateway-controller.opendatahub.io/consumer-portal-instance"

	// PortalRouteName is the public consumer portal route.
	PortalRouteName = "maas-consumer-portal"
	// OAuthRouteName is the public kube-auth-proxy sign-in route.
	OAuthRouteName = "maas-consumer-portal-oauth2"
	// PortalServiceName is the dashboard team's consumer portal Service.
	PortalServiceName = "maas-portal"
	// DefaultDashboardNamespace is where the RHOAI dashboard operands run.
	DefaultDashboardNamespace = "redhat-ods-applications"
)

// TrustedCABundleKeys are the odh-trusted-ca-bundle keys the proxy may trust:
// the cluster trust bundle and the DataScienceClusterInitialization custom CA.
var TrustedCABundleKeys = []string{"ca-bundle.crt", "odh-ca-bundle.crt"}

// Params are the per-tenant values substituted into the auth proxy manifests.
type Params struct {
	Namespace          string
	DashboardNamespace string
	// Instance is the MaasTenantConfig namespace; it labels every rendered
	// object so cleanup and grant pruning only touch this instance's objects.
	Instance           string
	Image              string
	OIDCIssuerURL      string
	Hostname           string
	GatewayName        string
	GatewayNamespace   string
	AuthorinoNamespace string
	// ProviderCAKeys are the non-empty odh-trusted-ca-bundle keys passed to the
	// proxy as --provider-ca-file.
	ProviderCAKeys []string
	// CABundleHash is stamped on the pod template so trusted CA bundle changes
	// restart the proxy.
	CABundleHash string
	// CredentialsHash is stamped on the pod template so credential changes
	// restart the proxy.
	CredentialsHash string
	// RefreshInterval is the proxy --cookie-refresh value.
	RefreshInterval time.Duration
	// CookieExpire is the proxy --cookie-expire value.
	CookieExpire time.Duration
}

// Render substitutes the CONSUMER_PORTAL_* tokens, places every resource in
// the infrastructure namespace, and labels it as managed by this controller.
func Render(resources []unstructured.Unstructured, p Params) []unstructured.Unstructured {
	portalServiceFQDN := PortalServiceName + "." + p.DashboardNamespace + ".svc.cluster.local"
	proxyServiceFQDN := AuthProxyName + "." + p.Namespace + ".svc.cluster.local"
	replacer := strings.NewReplacer(
		"CONSUMER_PORTAL_AUTH_PROXY_IMAGE", p.Image,
		"CONSUMER_PORTAL_OIDC_ISSUER_URL", p.OIDCIssuerURL,
		"CONSUMER_PORTAL_REDIRECT_URL", "https://"+p.Hostname+"/oauth2/callback",
		"CONSUMER_PORTAL_COOKIE_DOMAIN", p.Hostname,
		"CONSUMER_PORTAL_HOSTNAME", p.Hostname,
		"CONSUMER_PORTAL_GATEWAY_NAMESPACE", p.GatewayNamespace,
		"CONSUMER_PORTAL_GATEWAY_NAME", p.GatewayName,
		"CONSUMER_PORTAL_AUTHORINO_NAMESPACE", p.AuthorinoNamespace,
		"CONSUMER_PORTAL_COOKIE_REFRESH", p.RefreshInterval.String(),
		"CONSUMER_PORTAL_COOKIE_EXPIRE", p.CookieExpire.String(),
		"CONSUMER_PORTAL_DASHBOARD_NAMESPACE", p.DashboardNamespace,
		"CONSUMER_PORTAL_INFRA_NAMESPACE", p.Namespace,
		"CONSUMER_PORTAL_PORTAL_SERVICE_FQDN", portalServiceFQDN,
		"CONSUMER_PORTAL_PROXY_SERVICE_FQDN", proxyServiceFQDN,
		"CONSUMER_PORTAL_PROXY_URL", "https://"+proxyServiceFQDN+":4180/oauth2/auth",
	)
	out := make([]unstructured.Unstructured, 0, len(resources))
	for i := range resources {
		u := resources[i].DeepCopy()
		replaceStrings(u.Object, replacer)
		switch u.GetKind() {
		case "ReferenceGrant":
			u.SetNamespace(p.DashboardNamespace)
		case "DestinationRule":
			u.SetNamespace(p.GatewayNamespace)
		default:
			u.SetNamespace(p.Namespace)
		}
		labels := u.GetLabels()
		if labels == nil {
			labels = map[string]string{}
		}
		labels[tenant.LabelManagedBy] = tenant.ManagedByAIGatewayController
		if p.Instance != "" {
			labels[LabelPortalInstance] = p.Instance
		}
		u.SetLabels(labels)
		if u.GetKind() == "Deployment" {
			addProviderCAFiles(u, p.ProviderCAKeys)
			if p.CABundleHash != "" {
				_ = unstructured.SetNestedField(u.Object, p.CABundleHash,
					"spec", "template", "metadata", "annotations", CABundleHashAnnotation)
			}
			if p.CredentialsHash != "" {
				_ = unstructured.SetNestedField(u.Object, p.CredentialsHash,
					"spec", "template", "metadata", "annotations", CredentialsHashAnnotation)
			}
		}
		out = append(out, *u)
	}
	return out
}

// addProviderCAFiles appends a --provider-ca-file argument per CA bundle key to
// the auth proxy container.
func addProviderCAFiles(deployment *unstructured.Unstructured, keys []string) {
	if len(keys) == 0 {
		return
	}
	containers, _, _ := unstructured.NestedSlice(deployment.Object, "spec", "template", "spec", "containers")
	if len(containers) == 0 {
		return
	}
	container, ok := containers[0].(map[string]any)
	if !ok {
		return
	}
	args, _, _ := unstructured.NestedStringSlice(container, "args")
	for _, key := range keys {
		args = append(args, "--provider-ca-file="+trustedCAMountPath+"/"+key)
	}
	_ = unstructured.SetNestedStringSlice(container, args, "args")
	containers[0] = container
	_ = unstructured.SetNestedSlice(deployment.Object, containers, "spec", "template", "spec", "containers")
}

// replaceStrings replaces tokens in every string, mutating maps and slices in
// place; the returned value is only meaningful for a top-level string.
func replaceStrings(v any, replacer *strings.Replacer) any {
	switch value := v.(type) {
	case string:
		return replacer.Replace(value)
	case map[string]any:
		for k, item := range value {
			value[k] = replaceStrings(item, replacer)
		}
		return value
	case []any:
		for i, item := range value {
			value[i] = replaceStrings(item, replacer)
		}
		return value
	default:
		return v
	}
}

// IsDefaultTenant reports whether the MaasTenantConfig belongs to the default
// AITenant, the only tenant that serves the consumer portal in the MVP.
func IsDefaultTenant(mtc *unstructured.Unstructured) bool {
	labels := mtc.GetLabels()
	return labels[tenant.LabelManagedByAITenant] == "true" &&
		labels[tenant.LabelTenantName] == tenant.DefaultAITenantName
}

// InfraNamespace returns the namespace where maas-controller runs the
// tenant's maas-api; the auth proxy runs alongside it.
func InfraNamespace(mtc *unstructured.Unstructured) string {
	namespace, _, _ := unstructured.NestedString(mtc.Object, "status", "infraNamespace")
	return namespace
}

// PortalEnabled reads AITenant spec.consumerPortal.enabled, which
// defaults to true when unset.
func PortalEnabled(aitenant *unstructured.Unstructured) bool {
	enabled, found, err := unstructured.NestedBool(aitenant.Object, "spec", "consumerPortal", "enabled")
	if err != nil || !found {
		return true
	}
	return enabled
}

// OIDCIssuerURL returns AITenant spec.oidc.issuerUrl, or "" when OIDC is not
// configured.
func OIDCIssuerURL(aitenant *unstructured.Unstructured) string {
	issuer, _, _ := unstructured.NestedString(aitenant.Object, "spec", "oidc", "issuerUrl")
	return issuer
}

// ListenerHostname returns the hostname of the Gateway's first HTTPS listener
// with a concrete (non-wildcard) hostname, or "" when there is none.
func ListenerHostname(gateway *unstructured.Unstructured) string {
	listeners, _, _ := unstructured.NestedSlice(gateway.Object, "spec", "listeners")
	for _, entry := range listeners {
		listener, ok := entry.(map[string]any)
		if !ok {
			continue
		}
		protocol, _, _ := unstructured.NestedString(listener, "protocol")
		hostname, _, _ := unstructured.NestedString(listener, "hostname")
		if protocol == "HTTPS" && hostname != "" && !strings.HasPrefix(hostname, "*") {
			return hostname
		}
	}
	return ""
}
