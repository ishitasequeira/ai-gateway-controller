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

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// nemoEndpoint is the address TrustyAI publishes for the standard fixture,
// formatted the way controllers/nemo_guardrails/status.go does.
const nemoEndpoint = "https://" + nemoName + "." + providerNS + ".svc.cluster.local"

// TestEvaluateNemoReadyGatesOnTheDiscoveredEndpoint covers the contract
// TrustyAI merged in PR #977: status.endpoint is the sole input, and
// everything else the CR reports is irrelevant to the verdict.
//
// The two cases worth reading carefully are the crossed ones. A provider with
// phase Ready and a True DeploymentReady condition but no endpoint must still
// be refused, and a provider with an endpoint and no phase at all must still
// be accepted. Those pin the rule that nothing here may consult phase or
// DeploymentReady: upstream's CheckDeploymentReady always returns true
// (RHAI-4448 / RHOAIENG-96107), and with spec.exposeRoute unset the phase it
// feeds is Ready unconditionally, so reading either would make this evaluator
// fail open against a dead server. PR #977 did not change that.
func TestEvaluateNemoReadyGatesOnTheDiscoveredEndpoint(t *testing.T) {
	withStatus := func(status map[string]any) *unstructured.Unstructured {
		u := NewNemoGuardrails()
		u.SetName(nemoName)
		u.SetNamespace(providerNS)
		u.Object["status"] = status
		return u
	}

	cases := []struct {
		name         string
		provider     *unstructured.Unstructured
		wantReady    bool
		wantEndpoint string
	}{
		{name: "no provider object"},
		{name: "no status", provider: NewNemoGuardrails()},
		{
			// What an auth-disabled server looks like: upstream writes the
			// field only when utils.RequiresAuth holds, and clears it
			// otherwise.
			name:     "endpoint present but empty",
			provider: withStatus(map[string]any{"endpoint": ""}),
		},
		{
			name:     "endpoint is not a URL",
			provider: withStatus(map[string]any{"endpoint": "nemo.provider-ns.svc.cluster.local"}),
		},
		{
			// Auth off: TrustyAI publishes the same Service URL with http.
			name:         "endpoint is http",
			provider:     withStatus(map[string]any{"endpoint": "http://" + nemoName + "." + providerNS + ".svc.cluster.local"}),
			wantReady:    true,
			wantEndpoint: "http://" + nemoName + "." + providerNS + ".svc.cluster.local",
		},
		{
			name:     "endpoint has a scheme but no host",
			provider: withStatus(map[string]any{"endpoint": "https:///v1/checks"}),
		},
		{
			// Callers append their own path to this value, so a query or a
			// fragment would end up in the middle of the resulting URL.
			name:     "endpoint carries a query string",
			provider: withStatus(map[string]any{"endpoint": nemoEndpoint + "/?debug=1"}),
		},
		{
			name:     "endpoint carries an empty forced query",
			provider: withStatus(map[string]any{"endpoint": nemoEndpoint + "?"}),
		},
		{
			name:     "endpoint carries a fragment",
			provider: withStatus(map[string]any{"endpoint": nemoEndpoint + "/#section"}),
		},
		{
			// Accepting this would copy the password into
			// status.providerEndpoint, the binding revision, and the Warning
			// Event refuse emits.
			name:     "endpoint embeds userinfo credentials",
			provider: withStatus(map[string]any{"endpoint": "https://user:pw@" + nemoName + "." + providerNS + ".svc.cluster.local"}),
		},
		{
			name: "phase Ready and DeploymentReady True, but no endpoint",
			provider: withStatus(map[string]any{
				"phase": "Ready",
				"conditions": []any{
					map[string]any{"type": "DeploymentReady", "status": "True"},
					map[string]any{"type": "Route", "status": "True"},
				},
			}),
		},
		{
			name:         "endpoint published",
			provider:     withStatus(map[string]any{"endpoint": nemoEndpoint, "phase": "Ready"}),
			wantReady:    true,
			wantEndpoint: nemoEndpoint,
		},
		{
			// The inverse crossed case. TrustyAI writes the endpoint from its
			// not-ready path too, so discovery succeeding while the server is
			// still coming up is a state that genuinely occurs.
			name:         "endpoint published with no phase and a False DeploymentReady",
			provider:     withStatus(map[string]any{"endpoint": nemoEndpoint, "conditions": []any{map[string]any{"type": "DeploymentReady", "status": "False"}}}),
			wantReady:    true,
			wantEndpoint: nemoEndpoint,
		},
		{
			// No endpoint published: the server is its plain http Service
			// on port 80 in the published workload namespace. TrustyAI names
			// that Service <custom-resource namespace>-<custom-resource name>.
			name:         "workload namespace published, no endpoint",
			provider:     withStatus(map[string]any{"workloadNamespace": "nemo-workloads"}),
			wantReady:    true,
			wantEndpoint: "http://" + providerNS + "-" + nemoName + ".nemo-workloads.svc.cluster.local",
		},
		{
			name:     "invalid workload namespace",
			provider: withStatus(map[string]any{"workloadNamespace": "evil.example.com#"}),
		},
		{
			// A published https endpoint wins over the derived one.
			name:         "endpoint and workload namespace published",
			provider:     withStatus(map[string]any{"endpoint": nemoEndpoint, "workloadNamespace": providerNS}),
			wantReady:    true,
			wantEndpoint: nemoEndpoint,
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

// TestEvaluateNemoReadyDoesNotEchoTheRejectedEndpoint pins that a refusal
// never republishes the raw status.endpoint. The field belongs to another
// controller, so the rejected string is arbitrary input rather than the DNS
// name the success path assumes, and Reconcile puts this message into both a
// condition and a Warning Event that any reader of the consumer namespace can
// see.
func TestEvaluateNemoReadyDoesNotEchoTheRejectedEndpoint(t *testing.T) {
	const secret = "sup3rs3cret"
	host := nemoName + "." + providerNS + ".svc.cluster.local"

	for _, endpoint := range []string{
		"https://user:" + secret + "@" + host,
		"http://user:" + secret + "@" + host,
		"https://" + host + "/?token=" + secret,
		"https://" + host + "/#" + secret,
	} {
		t.Run(endpoint, func(t *testing.T) {
			provider := NewNemoGuardrails()
			provider.SetName(nemoName)
			provider.SetNamespace(providerNS)
			provider.Object["status"] = map[string]any{"endpoint": endpoint}

			got := evaluateNemoReady(provider)
			if got.Ready {
				t.Fatal("accepted an endpoint that is not a plain absolute http or https base URL")
			}
			if got.Endpoint != "" {
				t.Errorf("endpoint = %q, want it withheld on refusal", got.Endpoint)
			}
			if strings.Contains(got.Message, secret) {
				t.Errorf("refusal message republishes the rejected value: %q", got.Message)
			}
		})
	}
}
