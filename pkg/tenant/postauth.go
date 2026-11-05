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

package tenant

import (
	"context"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	aigatewayv1alpha1 "github.com/opendatahub-io/ai-gateway-controller/api/aigateway/v1alpha1"
)

// PostAuthFilterSource is a resource whose changes can change
// HasPostAuthFilters for one or more tenants.
type PostAuthFilterSource struct {
	Object    client.Object
	Predicate predicate.Predicate
	// Tenants returns the tenant namespaces whose answer obj can change.
	Tenants func(ctx context.Context, reader client.Reader, obj client.Object) []string
}

// PostAuthFilterSources lists the resources a route reconciler must watch to
// keep HasPostAuthFilters current, besides the tenant's MaasTenantConfig and
// AITenant, which a route reconciler already re-reads the tenant on.
func PostAuthFilterSources() []PostAuthFilterSource {
	return []PostAuthFilterSource{
		{Object: &aigatewayv1alpha1.AIGuardrail{}, Predicate: nemoCalloutChanged(), Tenants: ownNamespace},
		{Object: newMaaSSubscription(), Predicate: predicate.GenerationChangedPredicate{}, Tenants: ownNamespace},
		{Object: newMaaSModelRef(), Predicate: predicate.GenerationChangedPredicate{}, Tenants: modelRefTenants},
	}
}

// HasPostAuthFilters reports whether the tenant's compiled post-auth config
// has filters that must run on its ExternalModel routes, so those routes keep
// the post-auth ExtProc filter enabled.
func HasPostAuthFilters(ctx context.Context, reader client.Reader, tenantNamespace string) (bool, error) {
	return hasNemoCallouts(ctx, reader, tenantNamespace)
}

func ownNamespace(_ context.Context, _ client.Reader, obj client.Object) []string {
	if obj.GetNamespace() == "" {
		return nil
	}
	return []string{obj.GetNamespace()}
}

// modelRefTenants is the model ref's own namespace plus each namespace whose
// subscriptions point at it.
func modelRefTenants(ctx context.Context, reader client.Reader, obj client.Object) []string {
	namespaces := ownNamespace(ctx, reader, obj)
	naming, err := subscriptionNamespacesNaming(ctx, reader, obj.GetNamespace(), obj.GetName())
	if err != nil {
		return namespaces
	}
	for _, namespace := range naming {
		if namespace != obj.GetNamespace() {
			namespaces = append(namespaces, namespace)
		}
	}
	return namespaces
}
