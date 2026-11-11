/*
Copyright 2026 The opendatahub.io Authors.

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

package tenant

import "fmt"

// ChainPosition identifies where an ExtProc chain sits relative to Kuadrant
// authentication. Position is the only thing that differs between the two
// chains at the API-server level; everything else below follows from it.
type ChainPosition string

const (
	// ChainPreAuth runs before Kuadrant, so it sees the client's own
	// credentials and must not see the response body.
	ChainPreAuth ChainPosition = "PreAuth"
	// ChainPostAuth runs after Kuadrant, on authenticated and rewritten
	// traffic, and sees both request and response.
	ChainPostAuth ChainPosition = "PostAuth"
)

// Chain is the single description of one ExtProc chain and every identity and
// metrics consequence that follows from who runs it.
//
// Before this type the same chain was described in five unconnected places: the
// ServiceAccount its Deployment runs as, the pod-level and ServiceAccount-level
// automountServiceAccountToken, the metrics_auth block of whichever config the
// chain loads (generated extproc.yaml vs. the vendored pre-extproc.yaml), the
// subject of payload-processing-metrics-auth, and the scrape credentials the
// tenant ServiceMonitor forwards. They were each found and fixed separately
// (RHAI-22253), because nothing made them agree. Anything that depends on these
// facts must ask this type instead of hard-coding one of them, and
// TestChainsMatchRenderedResources fails when a code path disagrees.
//
// This is also the intended input to the Praxis ExtProc chain CR: the fields
// here are what a CR would carry as intent. See
// docs/proposals/praxis-extproc-chain-cr.md.
type Chain struct {
	// Position is where the chain sits relative to Kuadrant.
	Position ChainPosition
	// App is the value of the "app" label on the workload, its Service
	// selector, and the metrics Service selector.
	App string
	// DeploymentName returns the tenant's Deployment name for this chain.
	DeploymentName func(tenantID string) string
	// ServiceAccountName returns the ServiceAccount the Deployment runs as.
	ServiceAccountName func(tenantID string) string
	// MountsServiceAccountToken is the API-server reality: whether this
	// chain's pod receives a projected Kubernetes API token.
	MountsServiceAccountToken bool
	// AuthenticatesMetrics is whether the /metrics server requires
	// TokenReview + SubjectAccessReview for a scrape.
	AuthenticatesMetrics bool
	// MetricsAuthBinding is whether payload-processing-metrics-auth binds this
	// chain's ServiceAccount, which is what makes the TokenReview and
	// SubjectAccessReview calls succeed.
	MetricsAuthBinding bool
}

// The two chains MaaS runs. Declared as package-level values, not built by the
// callers, so there is exactly one answer per position.
var (
	preAuthChain = Chain{
		Position:           ChainPreAuth,
		App:                PayloadPreProcessingName,
		DeploymentName:     PayloadPreProcessingDeploymentName,
		ServiceAccountName: PayloadProcessingServiceAccountName,
		// The shared ServiceAccount, bound by payload-processing-metrics-auth,
		// and the vendored pre-extproc.yaml enables metrics_auth. Those three
		// facts are one fact and must move together.
		MountsServiceAccountToken: true,
		AuthenticatesMetrics:      true,
		MetricsAuthBinding:        true,
	}
	postAuthChain = Chain{
		Position:           ChainPostAuth,
		App:                PayloadProcessingName,
		DeploymentName:     PayloadProcessingDeploymentName,
		ServiceAccountName: PayloadProcessingPostServiceAccountName,
		// This pod handles untrusted inference traffic. It must hold no
		// Kubernetes API identity: no binding, no token, and therefore no
		// TokenReview authenticated /metrics - praxis builds a kube client for
		// the metrics server and exits(1) before opening the gRPC listener
		// when it cannot, which takes tenant inference down.
		MountsServiceAccountToken: false,
		AuthenticatesMetrics:      false,
		MetricsAuthBinding:        false,
	}
)

// Chains returns both ExtProc chains. Callers that need "the other one" derive
// it from Position rather than assuming an order.
func Chains() []Chain {
	return []Chain{preAuthChain, postAuthChain}
}

// ChainForPosition returns the declared chain for position. The set is closed;
// callers pass a constant, so an unknown position is a programming error and is
// reported rather than defaulted.
func ChainForPosition(position ChainPosition) (Chain, error) {
	for _, c := range Chains() {
		if c.Position == position {
			return c, nil
		}
	}
	return Chain{}, fmt.Errorf("unknown ExtProc chain position %q", position)
}

// InvariantViolations reports the ways this chain's identity, metrics
// authentication, and metrics binding contradict each other. A valid chain
// returns a nil slice; TestChainsHoldTheirInvariants checks every declared one.
func (c Chain) InvariantViolations() []string {
	var out []string
	if c.AuthenticatesMetrics && !c.MountsServiceAccountToken {
		out = append(out,
			"authenticates /metrics but the pod carries no ServiceAccount token, so praxis exits before serving gRPC")
	}
	if c.MetricsAuthBinding && !c.MountsServiceAccountToken {
		out = append(out,
			"grants TokenReview/SubjectAccessReview to a ServiceAccount whose pod receives no token")
	}
	if c.AuthenticatesMetrics && !c.MetricsAuthBinding {
		out = append(out,
			"authenticates /metrics but no ClusterRoleBinding lets it review tokens or subjects")
	}
	if c.MountsServiceAccountToken && !c.AuthenticatesMetrics && !c.MetricsAuthBinding {
		// Not a contradiction, but it means a pod is handed an API identity
		// for nothing. Flagging it keeps the two chains' asymmetry deliberate.
		out = append(out,
			"mounts a ServiceAccount token while neither authenticating /metrics nor holding the metrics-auth binding")
	}
	return out
}

// MetricsAuthYAML renders the metrics_auth block for this chain's Praxis
// configuration. Kept here so the two config generators cannot disagree with
// the descriptor, and so the pre chain's vendored config has an explicit
// counterpart to be checked against.
func (c Chain) MetricsAuthYAML() string {
	if c.AuthenticatesMetrics {
		return "  metrics_auth:\n    enabled: true\n"
	}
	return "  metrics_auth:\n    enabled: false\n"
}
