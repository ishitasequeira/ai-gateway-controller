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

package guardrail

import (
	"errors"
	"fmt"
	"net/url"
	"strings"

	"k8s.io/apimachinery/pkg/util/validation"
)

// NemoChecksPath is the NeMo server's checks API, relative to its base URL.
const NemoChecksPath = "/v1/checks"

// NemoService is the in-cluster Service a NeMo provider endpoint addresses.
// It is the only endpoint shape this controller accepts and compiles.
type NemoService struct {
	Namespace string
	Name      string
	// Authenticated is the https Service on port 443, fronted by
	// kube-rbac-proxy. Otherwise it is the plain http Service on port 80.
	Authenticated bool
}

// URL is the Service's base URL, the form published on
// status.providerEndpoint.
func (s NemoService) URL() string {
	scheme := "http"
	if s.Authenticated {
		scheme = "https"
	}
	return scheme + "://" + s.Name + "." + s.Namespace + ".svc.cluster.local"
}

// ParseNemoEndpoint returns the Service a NeMo provider endpoint addresses.
// providerNamespace and providerName identify the NemoGuardrails it was
// published for.
//
// Only a base URL, or one ending at NemoChecksPath, is accepted: http on port
// 80 or https on port 443. The host is either <providerName>.<namespace>.svc,
// which TrustyAI publishes in status.endpoint, or <providerNamespace>-<providerName>.<namespace>.svc,
// the Service TrustyAI creates for a workload in another namespace. Either
// may end in .cluster.local.
//
// Errors never quote the endpoint: it comes from another controller's
// status and may carry credentials.
func ParseNemoEndpoint(endpoint, providerNamespace, providerName string) (NemoService, error) {
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.ForceQuery || parsed.Fragment != "" {
		return NemoService{}, errors.New("endpoint is not a plain absolute base URL")
	}
	var service NemoService
	switch parsed.Scheme {
	case "http":
		if port := parsed.Port(); port != "" && port != "80" {
			return NemoService{}, fmt.Errorf("http endpoint uses port %s, not the Service's port 80", port)
		}
	case "https":
		if port := parsed.Port(); port != "" && port != "443" {
			return NemoService{}, fmt.Errorf("https endpoint uses port %s, not the Service's port 443", port)
		}
		service.Authenticated = true
	default:
		return NemoService{}, fmt.Errorf("endpoint scheme %q is not http or https", parsed.Scheme)
	}
	switch strings.TrimSuffix(parsed.Path, "/") {
	case "", NemoChecksPath:
	default:
		return NemoService{}, errors.New("endpoint path is not the NeMo base URL")
	}

	// The provider name is a DNS subdomain, so it may contain dots. Cutting the
	// host at the first dot would read "my.rails.infra.svc" as Service "my".
	// The cross-namespace name is tried first so a host that starts with it is
	// not read as the shorter <name>.<namespace> form.
	host := strings.TrimSuffix(parsed.Hostname(), ".cluster.local")
	var rest string
	crossNamespace := providerNamespace + "-" + providerName
	switch {
	case providerNamespace != "" && strings.HasPrefix(host, crossNamespace+"."):
		service.Name, rest = crossNamespace, strings.TrimPrefix(host, crossNamespace+".")
	case providerName != "" && strings.HasPrefix(host, providerName+"."):
		service.Name, rest = providerName, strings.TrimPrefix(host, providerName+".")
	default:
		return NemoService{}, fmt.Errorf("endpoint host %q is not a Service of NemoGuardrails %q", parsed.Hostname(), providerName)
	}
	namespace, suffix, _ := strings.Cut(rest, ".")
	if suffix != "svc" || len(validation.IsDNS1123Label(namespace)) > 0 {
		return NemoService{}, fmt.Errorf("endpoint host %q is not an in-cluster Service", parsed.Hostname())
	}
	service.Namespace = namespace
	return service, nil
}
