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
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
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
	for _, status := range []map[string]any{
		{"endpoint": nemoEndpoint},
		{"endpoint": "http://" + nemoName + "." + providerNS + ".svc.cluster.local"},
		{"workloadNamespace": "nemo-workloads"},
	} {
		provider := NewNemoGuardrails()
		provider.SetName(nemoName)
		provider.SetNamespace(providerNS)
		provider.Object["status"] = status
		readiness := evaluateNemoReady(provider)
		if !readiness.Ready {
			t.Fatalf("evaluateNemoReady(%v) refused: %s", status, readiness.Message)
		}
		service, err := ParseNemoEndpoint(readiness.Endpoint, providerNS, nemoName)
		if err != nil {
			t.Fatalf("accepted endpoint %q does not parse: %v", readiness.Endpoint, err)
		}
		if service.URL() != readiness.Endpoint {
			t.Errorf("Service %+v builds %q, want the accepted %q", service, service.URL(), readiness.Endpoint)
		}
	}
}

func TestEvaluateNemoReadyRefusesEndpointsTheCompilerCannotCall(t *testing.T) {
	host := nemoName + "." + providerNS + ".svc.cluster.local"
	for _, endpoint := range []string{
		"https://" + host + ":8443",
		"http://" + host + ":8000",
		"https://" + host + "/v2/other",
		"https://nemo.example.com",
	} {
		provider := &unstructured.Unstructured{}
		provider.SetGroupVersionKind(NemoGuardrailsGVK)
		provider.SetName(nemoName)
		provider.SetNamespace(providerNS)
		provider.Object["status"] = map[string]any{"endpoint": endpoint}
		if got := evaluateNemoReady(provider); got.Ready {
			t.Errorf("evaluateNemoReady accepted %q", endpoint)
		}
	}
}
