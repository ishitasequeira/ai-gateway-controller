package tenant

import (
	"context"
	"strings"
	"testing"

	"github.com/go-logr/logr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

func TestWaitForForeignOwnershipUsesLiveReads(t *testing.T) {
	scheme := mtcSchemeForTests()
	desired := &unstructured.Unstructured{}
	desired.SetGroupVersionKind(gvkDeployment)
	desired.SetName("payload-processing")
	desired.SetNamespace("gateway-system")

	foreign := desired.DeepCopy()
	foreign.SetUID("live-uid")
	foreign.SetLabels(map[string]string{managedByLabel: "another-controller"})
	cacheClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	liveClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(foreign).Build()

	r := &Reconciler{Client: cacheClient, APIReader: liveClient}
	err := r.waitForForeignOwnership(context.Background(), []unstructured.Unstructured{*desired})
	if err == nil {
		t.Fatal("expected foreign ownership error from live client")
	}
	if !strings.Contains(err.Error(), "another-controller") {
		t.Fatalf("waitForForeignOwnership error = %v, want another-controller ownership error", err)
	}
}

func TestOmitOptionalUnavailableResourcesDropsMissingServiceMonitor(t *testing.T) {
	scheme := mtcSchemeForTests()
	deploy := &unstructured.Unstructured{}
	deploy.SetGroupVersionKind(gvkDeployment)
	deploy.SetName("payload-processing-tenant")
	deploy.SetNamespace("gateway-system")

	monitor := &unstructured.Unstructured{}
	monitor.SetGroupVersionKind(gvkServiceMonitor)
	monitor.SetName("payload-processing-metrics-tenant")
	monitor.SetNamespace("gateway-system")

	liveClient := fake.NewClientBuilder().WithScheme(scheme).WithInterceptorFuncs(interceptor.Funcs{
		Get: func(_ context.Context, _ client.WithWatch, key client.ObjectKey, _ client.Object, _ ...client.GetOption) error {
			if key.Name == monitor.GetName() {
				return &apimeta.NoKindMatchError{
					GroupKind:        gvkServiceMonitor.GroupKind(),
					SearchedVersions: []string{gvkServiceMonitor.Version},
				}
			}
			return apierrors.NewNotFound(schema.GroupResource{Resource: "deployments"}, key.Name)
		},
	}).Build()

	r := &Reconciler{Client: liveClient, APIReader: liveClient}
	out, err := r.omitOptionalUnavailableResources(context.Background(), logr.Discard(), []unstructured.Unstructured{*deploy, *monitor})
	if err != nil {
		t.Fatalf("omitOptionalUnavailableResources: %v", err)
	}
	if len(out) != 1 || out[0].GetKind() != "Deployment" {
		t.Fatalf("got %#v, want only Deployment kept", out)
	}
}

func TestWaitForForeignOwnershipIgnoresNoMatch(t *testing.T) {
	scheme := mtcSchemeForTests()
	monitor := &unstructured.Unstructured{}
	monitor.SetGroupVersionKind(gvkServiceMonitor)
	monitor.SetName("payload-processing-metrics-tenant")
	monitor.SetNamespace("gateway-system")

	liveClient := fake.NewClientBuilder().WithScheme(scheme).WithInterceptorFuncs(interceptor.Funcs{
		Get: func(_ context.Context, _ client.WithWatch, _ client.ObjectKey, _ client.Object, _ ...client.GetOption) error {
			return &apimeta.NoKindMatchError{
				GroupKind:        gvkServiceMonitor.GroupKind(),
				SearchedVersions: []string{gvkServiceMonitor.Version},
			}
		},
	}).Build()

	r := &Reconciler{Client: liveClient, APIReader: liveClient}
	if err := r.waitForForeignOwnership(context.Background(), []unstructured.Unstructured{*monitor}); err != nil {
		t.Fatalf("waitForForeignOwnership: %v", err)
	}
}

func TestDeleteResourceIfOwnedIgnoresNoMatch(t *testing.T) {
	scheme := mtcSchemeForTests()
	liveClient := fake.NewClientBuilder().WithScheme(scheme).WithInterceptorFuncs(interceptor.Funcs{
		Get: func(_ context.Context, _ client.WithWatch, _ client.ObjectKey, _ client.Object, _ ...client.GetOption) error {
			return &apimeta.NoKindMatchError{
				GroupKind:        gvkServiceMonitor.GroupKind(),
				SearchedVersions: []string{gvkServiceMonitor.Version},
			}
		},
	}).Build()

	r := &Reconciler{Client: liveClient, APIReader: liveClient}
	if err := r.deleteResourceIfOwned(context.Background(), gvkServiceMonitor, "payload-processing-metrics-tenant", "gateway-system"); err != nil {
		t.Fatalf("deleteResourceIfOwned: %v", err)
	}
}

func TestDeleteResourceIfOwnedUsesUIDPrecondition(t *testing.T) {
	scheme := mtcSchemeForTests()
	live := &unstructured.Unstructured{}
	live.SetGroupVersionKind(gvkDeployment)
	live.SetName("payload-processing")
	live.SetNamespace("gateway-system")
	live.SetUID("live-uid")
	live.SetLabels(map[string]string{LabelManagedBy: ManagedByAIGatewayController})

	var preconditions *metav1.Preconditions
	cacheClient := fake.NewClientBuilder().WithScheme(scheme).WithInterceptorFuncs(interceptor.Funcs{
		Delete: func(_ context.Context, _ client.WithWatch, _ client.Object, opts ...client.DeleteOption) error {
			for _, opt := range opts {
				if deleteOptions, ok := opt.(*client.DeleteOptions); ok && deleteOptions.Preconditions != nil {
					preconditions = deleteOptions.Preconditions
				}
			}
			return nil
		},
	}).Build()
	liveClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(live).Build()

	r := &Reconciler{Client: cacheClient, APIReader: liveClient}
	if err := r.deleteResourceIfOwned(context.Background(), gvkDeployment, live.GetName(), live.GetNamespace()); err != nil {
		t.Fatalf("deleteResourceIfOwned: %v", err)
	}
	if preconditions == nil || preconditions.UID == nil || *preconditions.UID != live.GetUID() {
		t.Fatalf("delete preconditions = %#v, want UID %s", preconditions, live.GetUID())
	}
}
