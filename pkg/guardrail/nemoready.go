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
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// nemoReadiness is what reading a NemoGuardrails provider's status answers:
// whether a checks endpoint was discovered, which one, and the reason and
// message to publish on the ProviderReady condition.
//
// This is NeMo-specific, like the file it lives in. ProviderReady itself is
// not — if a second guardrails provider is ever supported, it gets its own
// evaluator returning this same shape, and Reconcile keeps publishing one
// condition.
type nemoReadiness struct {
	Ready    bool
	Endpoint string
	Reason   string
	Message  string
}

// evaluateNemoReady resolves the referenced NeMo server's checks endpoint,
// returning it alongside the condition reason and message to publish.
//
// What this establishes is endpoint *discovery*, not liveness. TrustyAI
// publishes status.workloadNamespace once the workload's address is settled,
// from its ready and not-ready status paths alike, so a published namespace
// proves the server is addressable — not that its pods are up. That is
// precisely the gate the guardrails proposal specifies: "Missing endpoint
// discovery reports ProviderReady=False and prevents activation"
// (02-guardrails-low-level-details.md:1339). The success message says what was
// actually established, so ProviderReady=True is not mistaken for a probe.
//
// status.phase and the DeploymentReady condition are deliberately NOT
// consulted. Upstream's controllers/utils/deployment.go CheckDeploymentReady
// passes a nil *appsv1.Deployment to client.Get so nothing is ever populated,
// returns an error from its retry closure on every path including success
// (discarding CheckPodsReady's verdict), and then ends in an unconditional
// "return true, nil" that never inspects the error — so it always reports
// ready. Its caller, controllers/nemo_guardrails/status.go, ANDs that with
// routeReady to set phase, meaning a NemoGuardrails with spec.exposeRoute
// unset or false reports phase Ready unconditionally, whatever its pods are
// doing. That is RHAI-4448 / RHOAIENG-96107, and it is why those two fields
// cannot gate activation.
//
// The OpenShift Route was evaluated as an endpoint source and rejected. The
// Route exists only when the optional spec.exposeRoute is true, it is an
// external ingress path with edge or reencrypt TLS while ExtProc calls the
// server in-cluster, and its condition is set to False with reason
// RouteDisabled when not exposed — which a naive True check would read as
// failure.
//
// status.endpoint is not read. It is another controller's free-form URL, and
// the tenant compiler opens egress and writes RBAC for whatever Service the
// endpoint names, so the Service is derived instead, the way TrustyAI creates
// it: NemoServiceFor status.workloadNamespace, which must be one of
// NemoInfraNamespaces, behind kube-rbac-proxy on https when the custom
// resource carries nemoAuthAnnotation and plain http otherwise.
func evaluateNemoReady(nemo *unstructured.Unstructured) nemoReadiness {
	refuse := func(message string) nemoReadiness {
		return nemoReadiness{
			Reason:  reasonEndpointDiscoveryUnavailable,
			Message: message + ", so no supported checks endpoint could be discovered",
		}
	}
	if nemo == nil {
		return refuse("the referenced NemoGuardrails provider publishes no status.workloadNamespace")
	}
	workloadNamespace, _, _ := unstructured.NestedString(nemo.Object, "status", "workloadNamespace")
	if workloadNamespace == "" {
		return refuse("the referenced NemoGuardrails provider publishes no status.workloadNamespace")
	}
	if !IsNemoInfraNamespace(workloadNamespace) {
		return refuse(fmt.Sprintf("the referenced NemoGuardrails provider runs its workload in namespace %q, "+
			"not an AI Gateway infrastructure namespace %v", workloadNamespace, NemoInfraNamespaces))
	}
	service, err := NemoServiceFor(workloadNamespace, nemo.GetNamespace(), nemo.GetName())
	if err != nil {
		return refuse(fmt.Sprintf("the referenced NemoGuardrails provider's Service cannot be derived (%v)", err))
	}
	service.Authenticated = strings.EqualFold(nemo.GetAnnotations()[nemoAuthAnnotation], "true")

	message := "referenced NemoGuardrails provider is reachable through its in-cluster http Service; " +
		"this confirms discovery, not that the server is serving"
	if service.Authenticated {
		message = "referenced NemoGuardrails provider is reachable through its authenticated in-cluster Service; " +
			"this confirms discovery, not that the server is serving"
	}
	return nemoReadiness{
		Ready:    true,
		Endpoint: service.URL(),
		Reason:   reasonProviderAvailable,
		Message:  message,
	}
}

// nemoAuthAnnotation is TrustyAI's utils.RequiresAuth switch
// (constants.AuthAnnotationKey). Set to "true" in any case, the server is
// fronted by kube-rbac-proxy.
const nemoAuthAnnotation = "security.opendatahub.io/enable-auth"
