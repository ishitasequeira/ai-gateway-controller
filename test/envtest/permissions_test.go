//go:build envtest

package envtest_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	authv1 "k8s.io/api/authorization/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const gatewayAPIGroup = "gateway.networking.k8s.io"

// Namespaces whose access is checked. A SubjectAccessReview does not need them to exist.
const (
	// tenantNamespace runs a tenant's external-model ExtProc, which mounts the credential copies.
	tenantNamespace = "permissions-tenant"
	// providerNamespace holds a shared ExternalProvider and the ReferenceGrants authorizing it.
	providerNamespace = "permissions-provider"
)

// testShippedPermissions asks the API server what the shipped ClusterRole lets
// the controller do, so packaged permissions are verified even before a
// reconciler exercises them.
func testShippedPermissions(t *testing.T, c client.Client) {
	t.Helper()
	for _, resource := range []struct{ group, name string }{
		{group: "apps", name: "replicasets"},
		{group: "", name: "pods"},
		{group: "discovery.k8s.io", name: "endpointslices"},
	} {
		for _, verb := range []string{"get", "list"} {
			assertAccess(t, c, true, authv1.ResourceAttributes{
				Namespace: tenantNamespace, Group: resource.group, Resource: resource.name, Verb: verb,
			})
		}
		assertAccess(t, c, false, authv1.ResourceAttributes{
			Namespace: tenantNamespace, Group: resource.group, Resource: resource.name, Verb: "watch",
		})
	}
	assertAccess(t, c, true, authv1.ResourceAttributes{
		Namespace: tenantNamespace, Resource: "pods/log", Verb: "get",
	})
	// Controller-owned credential copies are applied and deleted in tenant namespaces.
	for _, verb := range []string{"create", "patch", "delete"} {
		assertAccess(t, c, true, authv1.ResourceAttributes{
			Namespace: tenantNamespace, Resource: "secrets", Verb: verb,
		})
	}
	for _, verb := range []string{"get", "list", "watch"} {
		// ReferenceGrants in the provider namespace authorize cross-namespace
		// ExternalProvider references; the manager cache watches all namespaces.
		for _, namespace := range []string{providerNamespace, metav1.NamespaceAll} {
			assertAccess(t, c, true, authv1.ResourceAttributes{
				Namespace: namespace, Group: gatewayAPIGroup, Resource: "referencegrants", Verb: verb,
			})
		}
		// The tenant reconciler checks that the AITenant's Gateway exists.
		assertAccess(t, c, true, authv1.ResourceAttributes{
			Namespace: gatewayNamespace, Group: gatewayAPIGroup, Resource: "gateways", Verb: verb,
		})
		// AIGuardrail allowedConsumers Selector mode reads Namespace labels as an
		// authorization input (via APIReader, not the informer cache).
		assertAccess(t, c, true, authv1.ResourceAttributes{
			Resource: "namespaces", Verb: verb,
		})
	}
}

// assertAccess also requires an error-free evaluation, so a failed check is
// never mistaken for a denial.
func assertAccess(t *testing.T, c client.Client, want bool, attributes authv1.ResourceAttributes) {
	t.Helper()
	review := &authv1.SubjectAccessReview{Spec: authv1.SubjectAccessReviewSpec{
		User:               controllerIdentity.Name,
		Groups:             controllerIdentity.Groups,
		ResourceAttributes: &attributes,
	}}
	require.NoError(t, c.Create(t.Context(), review))
	request := fmt.Sprintf("%s %s.%s in namespace %q",
		attributes.Verb, attributes.Resource, attributes.Group, attributes.Namespace)
	assert.Empty(t, review.Status.EvaluationError, request)
	assert.Equal(t, want, review.Status.Allowed, "%s: %s", request, review.Status.Reason)
}
