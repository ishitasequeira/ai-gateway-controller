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
	"strings"
	"testing"
)

func TestParseNemoEndpoint(t *testing.T) {
	for _, c := range []struct {
		endpoint string
		want     NemoService
		wantErr  bool
	}{
		{endpoint: "http://my.rails.infra.svc.cluster.local", want: NemoService{Namespace: "infra", Name: "my.rails"}},
		{endpoint: "https://my.rails.infra.svc.cluster.local", want: NemoService{Namespace: "infra", Name: "my.rails", Authenticated: true}},
		{endpoint: "http://team-my.rails.infra.svc.cluster.local", want: NemoService{Namespace: "infra", Name: "team-my.rails"}},
		{endpoint: "http://my.rails.infra.svc", want: NemoService{Namespace: "infra", Name: "my.rails"}},
		{endpoint: "http://my.rails.infra.svc:80/", want: NemoService{Namespace: "infra", Name: "my.rails"}},
		{endpoint: "https://my.rails.infra.svc:443/v1/checks", want: NemoService{Namespace: "infra", Name: "my.rails", Authenticated: true}},
		{endpoint: "http://my.other.infra.svc.cluster.local", wantErr: true},
		{endpoint: "http://my.rails.infra.example.com", wantErr: true},
		{endpoint: "https://my.rails.infra.svc:8443", wantErr: true},
		{endpoint: "http://my.rails.infra.svc:8000", wantErr: true},
		{endpoint: "http://my.rails.infra.svc/v2/other", wantErr: true},
		{endpoint: "grpc://my.rails.infra.svc", wantErr: true},
		{endpoint: "https://user@my.rails.infra.svc", wantErr: true},
		{endpoint: "https://my.rails.infra.svc/?debug=1", wantErr: true},
		{endpoint: "my.rails.infra.svc", wantErr: true},
	} {
		t.Run(c.endpoint, func(t *testing.T) {
			got, err := ParseNemoEndpoint(c.endpoint, "team", "my.rails")
			if c.wantErr {
				if err == nil {
					t.Fatalf("ParseNemoEndpoint = %+v, want an error", got)
				}
				return
			}
			if err != nil || got != c.want {
				t.Fatalf("ParseNemoEndpoint = %+v, %v; want %+v", got, err, c.want)
			}
		})
	}
}

// TestNemoReadyEndpointsParse pins that every endpoint evaluateNemoReady
// accepts is one the tenant compiler can parse back to the same Service.
func TestNemoReadyEndpointsParse(t *testing.T) {
	for _, c := range []struct {
		namespace, name string
		auth            bool
		workloadNS      string
	}{
		{providerNS, nemoName, true, providerNS},
		{providerNS, nemoName, false, providerNS},
		{providerNS, nemoName, true, "nemo-workloads"},
		{providerNS, nemoName, false, "nemo-workloads"},
	} {
		provider := NewNemoGuardrails()
		provider.SetName(c.name)
		provider.SetNamespace(c.namespace)
		if c.auth {
			provider.SetAnnotations(map[string]string{nemoAuthAnnotation: "true"})
		}
		provider.Object["status"] = map[string]any{"workloadNamespace": c.workloadNS}
		readiness := evaluateNemoReady(provider)
		if !readiness.Ready {
			t.Fatalf("evaluateNemoReady(%+v) refused: %s", c, readiness.Message)
		}
		service, err := ParseNemoEndpoint(readiness.Endpoint, c.namespace, c.name)
		if err != nil {
			t.Fatalf("accepted endpoint %q does not parse: %v", readiness.Endpoint, err)
		}
		if service.URL() != readiness.Endpoint {
			t.Errorf("Service %+v builds %q, want the accepted %q", service, service.URL(), readiness.Endpoint)
		}
	}
}

// TestEvaluateNemoReadyIgnoresStatusEndpoint pins that status.endpoint never
// reaches the published endpoint or the message: the Service always comes
// from status.workloadNamespace, so another controller's URL cannot steer the
// tenant compiler's egress and RBAC.
func TestEvaluateNemoReadyIgnoresStatusEndpoint(t *testing.T) {
	const secret = "sup3rs3cret"
	for _, endpoint := range []string{
		"https://" + nemoName + ".kube-system.svc.cluster.local",
		"https://nemo.example.com",
		"https://user:" + secret + "@" + nemoName + "." + providerNS + ".svc.cluster.local",
		"http://" + nemoName + "." + providerNS + ".svc.cluster.local:8000",
	} {
		t.Run(endpoint, func(t *testing.T) {
			provider := NewNemoGuardrails()
			provider.SetName(nemoName)
			provider.SetNamespace(providerNS)
			provider.SetAnnotations(map[string]string{nemoAuthAnnotation: "true"})
			provider.Object["status"] = map[string]any{"endpoint": endpoint, "workloadNamespace": providerNS}
			got := evaluateNemoReady(provider)
			if !got.Ready || got.Endpoint != nemoEndpoint {
				t.Fatalf("evaluateNemoReady = %+v, want %q from the workload namespace", got, nemoEndpoint)
			}
			if strings.Contains(got.Message, secret) {
				t.Errorf("message republishes status.endpoint: %q", got.Message)
			}
		})
	}
}
