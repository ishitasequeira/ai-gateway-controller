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
	"context"
	"fmt"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// NewNemoGuardrails returns an empty unstructured object with the
// NemoGuardrails GVK set, ready for a Get by name/namespace.
func NewNemoGuardrails() *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(NemoGuardrailsGVK)
	return u
}

// providerCRDInstalled reports whether TrustyAI's NemoGuardrails CRD is
// present on the cluster.
//
// This gates the provider watch, not the reconciler. Watching a Kind whose
// CRD is absent fails the informer and brings the manager down with it, which
// would take the tenant and external-model reconcilers offline too. The
// reconciler itself still runs, so an AIGuardrail on a cluster without
// TrustyAI is refused with reasonProviderCRDNotInstalled instead of being
// left with no conditions.
//
// The manager's RESTMapper is used rather than a separate discovery client:
// it is already built from the same discovery data, and reading it needs no
// RBAC beyond the API discovery every client already performs.
//
// Called twice over: at startup to decide whether to register the provider
// watch, and on every reconcile so a policy is accepted on its own once
// TrustyAI is installed. Only the watch needs a restart, because one cannot
// be added to a running controller — until then provider edits are picked up
// at the resync interval rather than immediately.
//
// The mapper reloads discovery on a miss, so the repeated call is a cache hit
// once the CRD exists and an API round trip while it does not. That cost is
// why the refusal is requeued on the resync cadence rather than the ten-second
// one every other refusal uses.
//
// That reload happens only on a miss, which makes this answer reliable in one
// direction. A CRD that appears is picked up; a CRD that is uninstalled under
// a running controller leaves a resolved mapping behind that nothing
// invalidates, and this keeps reporting it as present. providerCRDServed is
// the live answer for the path where that difference changes the verdict.
func providerCRDInstalled(mapper apimeta.RESTMapper) (bool, error) {
	_, err := mapper.RESTMapping(NemoGuardrailsGVK.GroupKind(), NemoGuardrailsGVK.Version)
	switch {
	case err == nil:
		return true, nil
	case apimeta.IsNoMatchError(err):
		// The CRD is genuinely absent, which is a supported configuration
		// rather than a failure.
		return false, nil
	default:
		// Discovery itself failed. Starting up as though TrustyAI were absent
		// would silently disable guardrails on a cluster that has them, so
		// this is reported and left for the caller to treat as fatal.
		return false, fmt.Errorf("resolving %s: %w", NemoGuardrailsGVK, err)
	}
}

// serverResourceLister is the slice of discovery this package uses: the kinds
// the API server serves for one group/version.
//
// Narrow on purpose. discovery.DiscoveryInterface carries a dozen methods
// nothing here calls, and standing it up in a test would mean stubbing all of
// them to exercise one. *discovery.DiscoveryClient satisfies this.
type serverResourceLister interface {
	ServerResourcesForGroupVersionWithContext(ctx context.Context, groupVersion string) (*metav1.APIResourceList, error)
}

// discoveryTimeout bounds the one discovery request this package makes.
//
// client-go gives a discovery client a 32s per-request timeout, but only when
// it builds the HTTP client itself. SetupWithManager hands it the manager's,
// which takes precedence over the configured transport values, so that default
// never reaches the wire. The reconcile context carries no deadline either, and
// this controller runs a single worker, so an unbounded request holds back
// every other policy.
const discoveryTimeout = 32 * time.Second

// providerCRDServed reports whether the API server serves the NemoGuardrails
// kind right now, asked of discovery rather than of the cached RESTMapper.
//
// This exists because providerCRDInstalled cannot answer it once the mapping
// has resolved. Uninstalling TrustyAI from a running cluster would otherwise
// leave every policy reporting that its provider reference is wrong, when the
// reference is correct and the CRD behind it is gone -- two different repairs.
//
// Only the refusal path calls this, so the cost is one request per policy
// whose provider already could not be read, rather than one per reconcile.
func providerCRDServed(ctx context.Context, discovery serverResourceLister) (bool, error) {
	groupVersion := NemoGuardrailsGVK.GroupVersion().String()

	ctx, cancel := context.WithTimeout(ctx, discoveryTimeout)
	defer cancel()

	resources, err := discovery.ServerResourcesForGroupVersionWithContext(ctx, groupVersion)
	if err != nil {
		if apierrors.IsNotFound(err) {
			// Nothing serves the group/version at all, which is what removing
			// the only CRD that defined it looks like.
			return false, nil
		}
		// A failed lookup is not evidence of absence, and the caller has a
		// verdict riding on this. Same treatment as a broken RESTMapper above.
		return false, fmt.Errorf("discovering %s: %w", groupVersion, err)
	}

	// The group/version outliving the CRD is the ordinary case on a real
	// cluster: TrustyAI ships other kinds under it, so the path keeps
	// answering after NemoGuardrails is removed. The Kind is what settles it.
	for _, resource := range resources.APIResources {
		if resource.Kind == NemoGuardrailsGVK.Kind {
			return true, nil
		}
	}
	return false, nil
}
