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
	"slices"
	"strings"

	"k8s.io/apimachinery/pkg/util/validation"
)

// NemoChecksPath is the NeMo server's checks API, relative to its base URL.
const NemoChecksPath = "/v1/checks"

// NemoInfraNamespaces are the AI Gateway infrastructure namespaces of ODH
// and RHOAI, the only namespaces a NeMo Service is accepted in. The tenant
// compiler opens egress and writes RBAC for the Service an endpoint names,
// so a provider must not be able to point it anywhere else.
var NemoInfraNamespaces = []string{"odh-ai-gateway-infra", "redhat-ai-gateway-infra"}

// IsNemoInfraNamespace reports whether namespace is one of NemoInfraNamespaces.
func IsNemoInfraNamespace(namespace string) bool {
	return slices.Contains(NemoInfraNamespaces, namespace)
}

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

// NemoServiceFor is the Service TrustyAI creates for the NemoGuardrails
// providerNamespace/providerName when its workload runs in infraNamespace:
// the custom resource name when the custom resource is in infraNamespace
// itself, and <providerNamespace>-<providerName> otherwise. It fails when
// infraNamespace is not one of NemoInfraNamespaces, or when that name is not a
// Service name.
func NemoServiceFor(infraNamespace, providerNamespace, providerName string) (NemoService, error) {
	if !IsNemoInfraNamespace(infraNamespace) {
		return NemoService{}, fmt.Errorf("namespace %q is not an AI Gateway infrastructure namespace %v",
			infraNamespace, NemoInfraNamespaces)
	}
	name := providerName
	if providerNamespace != infraNamespace {
		name = providerNamespace + "-" + providerName
	}
	if providerNamespace == "" || providerName == "" || len(validation.IsDNS1123Label(name)) > 0 {
		return NemoService{}, fmt.Errorf("NemoGuardrails %s/%s has no valid Service name in %q",
			providerNamespace, providerName, infraNamespace)
	}
	return NemoService{Namespace: infraNamespace, Name: name}, nil
}

// ParseNemoEndpoint returns the Service a NeMo provider endpoint addresses.
// providerNamespace and providerName identify the NemoGuardrails the endpoint
// was published for.
//
// Only a base URL, or one ending at NemoChecksPath, is accepted: http on port
// 80 or https on port 443. The host must be <service>.<namespace>.svc,
// optionally ending in .cluster.local, where namespace is one of
// NemoInfraNamespaces and service is NemoServiceFor that namespace. Whether
// it is the infrastructure namespace of a particular tenant is the caller's
// to check.
//
// Errors never quote the endpoint: it comes from another controller's
// status and may carry credentials.
func ParseNemoEndpoint(endpoint, providerNamespace, providerName string) (NemoService, error) {
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.ForceQuery || parsed.Fragment != "" {
		return NemoService{}, errors.New("endpoint is not a plain absolute base URL")
	}
	host := strings.TrimSuffix(parsed.Hostname(), ".cluster.local")
	serviceHost, ok := strings.CutSuffix(host, ".svc")
	dot := strings.LastIndex(serviceHost, ".")
	if !ok || dot < 0 {
		return NemoService{}, fmt.Errorf("endpoint host %q is not an in-cluster Service", parsed.Hostname())
	}
	service, err := NemoServiceFor(serviceHost[dot+1:], providerNamespace, providerName)
	if err != nil {
		return NemoService{}, fmt.Errorf("endpoint host %q: %w", parsed.Hostname(), err)
	}
	if serviceHost[:dot] != service.Name {
		return NemoService{}, fmt.Errorf("endpoint host %q is not Service %s of NemoGuardrails %s/%s",
			parsed.Hostname(), service.Name, providerNamespace, providerName)
	}
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
	return service, nil
}
