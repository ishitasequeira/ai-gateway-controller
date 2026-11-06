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
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	aigatewayv1alpha1 "github.com/opendatahub-io/ai-gateway-controller/api/aigateway/v1alpha1"
)

// PostAuthFilterSource is a resource whose changes can change
// HasPostAuthFilters for one or more tenants.
type PostAuthFilterSource struct {
	Object    client.Object
	Predicate predicate.Predicate
	// Tenants returns the tenant namespaces whose answer obj can change. On
	// error the namespaces already found are returned with it.
	Tenants func(ctx context.Context, reader client.Reader, obj client.Object) ([]string, error)
}

// PostAuthFilterSources lists the resources a route reconciler must watch to
// keep HasPostAuthFilters current, besides the tenant's MaasTenantConfig and
// AITenant, which a route reconciler already re-reads the tenant on.
func PostAuthFilterSources() []PostAuthFilterSource {
	return []PostAuthFilterSource{
		{Object: &aigatewayv1alpha1.AIGuardrail{}, Predicate: nemoCalloutChanged(), Tenants: ownNamespace},
		{Object: newMaaSSubscription(), Predicate: predicate.GenerationChangedPredicate{}, Tenants: ownNamespace},
		{Object: newMaaSModelRef(), Predicate: predicate.GenerationChangedPredicate{}, Tenants: ModelRefTenants},
	}
}

// HasPostAuthFilters reports whether the tenant's compiled post-auth config
// has filters that must run on its ExternalModel routes, so those routes keep
// the post-auth ExtProc filter enabled.
func HasPostAuthFilters(ctx context.Context, reader client.Reader, tenantNamespace string) (bool, error) {
	return hasNemoCallouts(ctx, reader, tenantNamespace)
}

func ownNamespace(_ context.Context, _ client.Reader, obj client.Object) ([]string, error) {
	if obj.GetNamespace() == "" {
		return nil, nil
	}
	return []string{obj.GetNamespace()}, nil
}

// ModelRefTenants returns, once each, the tenant namespaces a MaaSModelRef
// change can affect: its own namespace unless that namespace has no
// MaasTenantConfig, and each namespace whose subscriptions point at it. A
// model ref stored alongside the model, outside any tenant, reaches tenants
// only through their subscriptions.
func ModelRefTenants(ctx context.Context, reader client.Reader, obj client.Object) ([]string, error) {
	key := client.ObjectKeyFromObject(obj)
	seen := map[string]bool{}
	var namespaces []string
	add := func(namespace string) {
		if namespace != "" && !seen[namespace] {
			seen[namespace] = true
			namespaces = append(namespaces, namespace)
		}
	}
	if key.Namespace != "" {
		err := reader.Get(ctx, client.ObjectKey{Namespace: key.Namespace, Name: MaasTenantConfigInstanceName}, NewMaasTenantConfig())
		if !apierrors.IsNotFound(err) {
			add(key.Namespace)
		}
	}
	naming, err := subscriptionNamespacesNaming(ctx, reader, key.Namespace, key.Name)
	if err != nil {
		return namespaces, fmt.Errorf("list MaaSSubscriptions naming MaaSModelRef %s: %w", key, err)
	}
	for _, namespace := range naming {
		add(namespace)
	}
	return namespaces, nil
}
