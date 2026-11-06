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

// testInfraNS is the ODH AI Gateway infrastructure namespace, where the
// fixtures' NeMo workloads run.
const testInfraNS = "odh-ai-gateway-infra"

// nemoServiceHost is the Service TrustyAI creates in testInfraNS for the
// standard fixture, and nemoEndpoint its authenticated URL as
// controllers/nemo_guardrails/status.go formats it.
const (
	nemoServiceHost = providerNS + "-" + nemoName + "." + testInfraNS + ".svc.cluster.local"
	nemoEndpoint    = "https://" + nemoServiceHost
)

// TestEvaluateNemoReadyGatesOnTheDiscoveredEndpoint covers discovery:
// status.workloadNamespace is the only status input, it must be an AI Gateway
// infrastructure namespace, the auth annotation picks the scheme, and
// everything else the CR reports is irrelevant to the verdict.
//
// The two cases worth reading carefully are the crossed ones. A provider with
// phase Ready and a True DeploymentReady condition but no workload namespace
// must still be refused, and a provider with one and no phase at all must still
// be accepted. Those pin the rule that nothing here may consult phase or
// DeploymentReady: upstream's CheckDeploymentReady always returns true
// (RHAI-4448 / RHOAIENG-96107), and with spec.exposeRoute unset the phase it
// feeds is Ready unconditionally, so reading either would make this evaluator
// fail open against a dead server.
func TestEvaluateNemoReadyGatesOnTheDiscoveredEndpoint(t *testing.T) {
	provider := func(annotations map[string]string, status map[string]any) *unstructured.Unstructured {
		u := NewNemoGuardrails()
		u.SetName(nemoName)
		u.SetNamespace(providerNS)
		u.SetAnnotations(annotations)
		u.Object["status"] = status
		return u
	}
	auth := map[string]string{nemoAuthAnnotation: "true"}

	cases := []struct {
		name         string
		provider     *unstructured.Unstructured
		wantReady    bool
		wantEndpoint string
	}{
		{name: "no provider object"},
		{name: "no status", provider: NewNemoGuardrails()},
		{
			name:     "workload namespace present but empty",
			provider: provider(auth, map[string]any{"workloadNamespace": ""}),
		},
		{
			name:     "invalid workload namespace",
			provider: provider(auth, map[string]any{"workloadNamespace": "evil.example.com#"}),
		},
		{
			name: "phase Ready and DeploymentReady True, but no workload namespace",
			provider: provider(auth, map[string]any{
				"phase": "Ready",
				"conditions": []any{
					map[string]any{"type": "DeploymentReady", "status": "True"},
					map[string]any{"type": "Route", "status": "True"},
				},
			}),
		},
		{
			name:         "auth on",
			provider:     provider(auth, map[string]any{"workloadNamespace": testInfraNS, "phase": "Ready"}),
			wantReady:    true,
			wantEndpoint: nemoEndpoint,
		},
		{
			// utils.RequiresAuth lowercases the value.
			name:         "auth on, mixed case",
			provider:     provider(map[string]string{nemoAuthAnnotation: "True"}, map[string]any{"workloadNamespace": testInfraNS}),
			wantReady:    true,
			wantEndpoint: nemoEndpoint,
		},
		{
			name:         "auth annotation absent",
			provider:     provider(nil, map[string]any{"workloadNamespace": testInfraNS}),
			wantReady:    true,
			wantEndpoint: "http://" + nemoServiceHost,
		},
		{
			name:         "auth annotation false",
			provider:     provider(map[string]string{nemoAuthAnnotation: "false"}, map[string]any{"workloadNamespace": testInfraNS}),
			wantReady:    true,
			wantEndpoint: "http://" + nemoServiceHost,
		},
		{
			name:         "workload in the RHOAI infrastructure namespace",
			provider:     provider(auth, map[string]any{"workloadNamespace": "redhat-ai-gateway-infra"}),
			wantReady:    true,
			wantEndpoint: "https://" + providerNS + "-" + nemoName + ".redhat-ai-gateway-infra.svc.cluster.local",
		},
		{
			// TrustyAI names the Service for the bare custom resource name
			// only when the custom resource is in the workload namespace.
			name: "custom resource in the infrastructure namespace",
			provider: func() *unstructured.Unstructured {
				u := provider(auth, map[string]any{"workloadNamespace": testInfraNS})
				u.SetNamespace(testInfraNS)
				return u
			}(),
			wantReady:    true,
			wantEndpoint: "https://" + nemoName + "." + testInfraNS + ".svc.cluster.local",
		},
		{
			// The tenant compiler would open egress and write RBAC for it.
			name:     "workload in the custom resource namespace",
			provider: provider(auth, map[string]any{"workloadNamespace": providerNS}),
		},
		{
			name:     "workload outside the infrastructure namespaces",
			provider: provider(auth, map[string]any{"workloadNamespace": "kube-system"}),
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := evaluateNemoReady(c.provider)
			if got.Ready != c.wantReady {
				t.Fatalf("evaluateNemoReady = %v, want %v (reason %q, message %q)", got.Ready, c.wantReady, got.Reason, got.Message)
			}
			if got.Endpoint != c.wantEndpoint {
				t.Errorf("endpoint = %q, want %q", got.Endpoint, c.wantEndpoint)
			}
			wantReason := reasonEndpointDiscoveryUnavailable
			if c.wantReady {
				wantReason = reasonProviderAvailable
			}
			if got.Reason != wantReason {
				t.Errorf("reason = %q, want %q", got.Reason, wantReason)
			}
			if got.Message == "" {
				t.Error("message is empty; status has to say why this verdict was reached")
			}
		})
	}
}
