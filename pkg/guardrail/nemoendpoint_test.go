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
	const service = "team-rails." + testInfraNS + ".svc"
	for _, c := range []struct {
		endpoint string
		want     NemoService
		wantErr  bool
	}{
		{endpoint: "http://" + service + ".cluster.local", want: NemoService{Namespace: testInfraNS, Name: "team-rails"}},
		{endpoint: "https://" + service + ".cluster.local", want: NemoService{Namespace: testInfraNS, Name: "team-rails", Authenticated: true}},
		{endpoint: "http://" + service, want: NemoService{Namespace: testInfraNS, Name: "team-rails"}},
		{endpoint: "http://" + service + ":80/", want: NemoService{Namespace: testInfraNS, Name: "team-rails"}},
		{endpoint: "https://" + service + ":443/v1/checks", want: NemoService{Namespace: testInfraNS, Name: "team-rails", Authenticated: true}},
		// The bare name is only the Service of a custom resource in the
		// infrastructure namespace itself.
		{endpoint: "http://rails." + testInfraNS + ".svc", wantErr: true},
		{endpoint: "http://rails.team.svc", wantErr: true},
		{endpoint: "http://team-rails.redhat-ai-gateway-infra.svc", want: NemoService{Namespace: "redhat-ai-gateway-infra", Name: "team-rails"}},
		{endpoint: "http://team-rails.kube-system.svc", wantErr: true},
		{endpoint: "http://team-rails.svc", wantErr: true},
		{endpoint: "http://team-rails." + testInfraNS + "-evil.svc", wantErr: true},
		{endpoint: "http://x.team-rails." + testInfraNS + ".svc", wantErr: true},
		{endpoint: "http://team-other." + testInfraNS + ".svc", wantErr: true},
		{endpoint: "http://team-rails." + testInfraNS + ".example.com", wantErr: true},
		{endpoint: "https://" + service + ":8443", wantErr: true},
		{endpoint: "http://" + service + ":8000", wantErr: true},
		{endpoint: "http://" + service + "/v2/other", wantErr: true},
		{endpoint: "grpc://" + service, wantErr: true},
		{endpoint: "https://user@" + service, wantErr: true},
		{endpoint: "https://" + service + "/?debug=1", wantErr: true},
		{endpoint: service, wantErr: true},
	} {
		t.Run(c.endpoint, func(t *testing.T) {
			got, err := ParseNemoEndpoint(c.endpoint, "team", "rails")
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

// TestNemoServiceForMatchesTrustyAI pins the Service name TrustyAI creates for
// a NemoGuardrails, and that only an AI Gateway infrastructure namespace and a
// name that is a Service name yield one.
func TestNemoServiceForMatchesTrustyAI(t *testing.T) {
	for _, c := range []struct {
		infra, namespace, name string
		want                   string
	}{
		{testInfraNS, "team", "rails", "team-rails"},
		{"redhat-ai-gateway-infra", "team", "rails", "team-rails"},
		{testInfraNS, testInfraNS, "rails", "rails"},
		{"infra", "team", "rails", ""},
		{"", "team", "rails", ""},
		{testInfraNS, "team", "my.rails", ""},
		{testInfraNS, "team", strings.Repeat("n", 60), ""},
		{testInfraNS, "", "rails", ""},
	} {
		got, err := NemoServiceFor(c.infra, c.namespace, c.name)
		if c.want == "" {
			if err == nil {
				t.Errorf("NemoServiceFor(%q, %q, %q) = %+v, want an error", c.infra, c.namespace, c.name, got)
			}
			continue
		}
		if want := (NemoService{Namespace: c.infra, Name: c.want}); err != nil || got != want {
			t.Errorf("NemoServiceFor(%q, %q, %q) = %+v, %v; want %+v", c.infra, c.namespace, c.name, got, err, want)
		}
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
		{providerNS, nemoName, true, testInfraNS},
		{providerNS, nemoName, false, testInfraNS},
		{testInfraNS, nemoName, true, testInfraNS},
		{providerNS, nemoName, true, "redhat-ai-gateway-infra"},
		{providerNS, nemoName, false, "redhat-ai-gateway-infra"},
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
		"https://" + providerNS + "-" + nemoName + ".redhat-ai-gateway-infra.svc.cluster.local",
		"https://nemo.example.com",
		"https://user:" + secret + "@" + nemoName + "." + providerNS + ".svc.cluster.local",
		"http://" + nemoName + "." + providerNS + ".svc.cluster.local:8000",
	} {
		t.Run(endpoint, func(t *testing.T) {
			provider := NewNemoGuardrails()
			provider.SetName(nemoName)
			provider.SetNamespace(providerNS)
			provider.SetAnnotations(map[string]string{nemoAuthAnnotation: "true"})
			provider.Object["status"] = map[string]any{"endpoint": endpoint, "workloadNamespace": testInfraNS}
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
