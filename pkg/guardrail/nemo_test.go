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
	"errors"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func TestNewNemoGuardrailsSetsGVK(t *testing.T) {
	if got := NewNemoGuardrails().GroupVersionKind(); got != NemoGuardrailsGVK {
		t.Fatalf("GVK = %v, want %v", got, NemoGuardrailsGVK)
	}
}

// mapperWithNemo is a RESTMapper for a cluster where TrustyAI is installed.
func mapperWithNemo() *apimeta.DefaultRESTMapper {
	mapper := apimeta.NewDefaultRESTMapper([]schema.GroupVersion{NemoGuardrailsGVK.GroupVersion()})
	mapper.Add(NemoGuardrailsGVK, apimeta.RESTScopeNamespace)
	return mapper
}

// mapperWithoutNemo is a RESTMapper for a cluster where TrustyAI is not
// installed. A DefaultRESTMapper that knows no kinds returns the same
// NoKindMatchError the manager's mapper returns for an absent CRD.
func mapperWithoutNemo() *apimeta.DefaultRESTMapper {
	return apimeta.NewDefaultRESTMapper(nil)
}

// erroringRESTMapper stands in for discovery being broken rather than the CRD
// being absent. Only RESTMapping is ever called, so the embedded nil interface
// is never dereferenced.
type erroringRESTMapper struct {
	apimeta.RESTMapper
}

func (erroringRESTMapper) RESTMapping(schema.GroupKind, ...string) (*apimeta.RESTMapping, error) {
	return nil, errors.New("discovery unavailable")
}

func TestProviderCRDInstalled(t *testing.T) {
	t.Run("enabled when the CRD is present", func(t *testing.T) {
		installed, err := providerCRDInstalled(mapperWithNemo())
		if err != nil {
			t.Fatalf("providerCRDInstalled: %v", err)
		}
		if !installed {
			t.Fatal("providerCRDInstalled = false with the NemoGuardrails CRD registered, want true")
		}
	})

	t.Run("disabled when the CRD is absent", func(t *testing.T) {
		// An absent CRD must not be an error: the manager has to start
		// anyway so the tenant and external-model reconcilers keep running.
		installed, err := providerCRDInstalled(mapperWithoutNemo())
		if err != nil {
			t.Fatalf("providerCRDInstalled returned an error for an absent CRD: %v", err)
		}
		if installed {
			t.Fatal("providerCRDInstalled = true with no NemoGuardrails CRD, want false")
		}
	})

	t.Run("a broken discovery is an error, not a disable", func(t *testing.T) {
		// Treating this as "absent" would silently disable guardrails on a
		// cluster that has TrustyAI installed, which is the one outcome worse
		// than failing to start.
		installed, err := providerCRDInstalled(erroringRESTMapper{})
		if err == nil {
			t.Fatal("providerCRDInstalled succeeded on a discovery failure, want an error")
		}
		if installed {
			t.Fatal("providerCRDInstalled = true alongside an error, want false")
		}
	})

	t.Run("a different kind in the same group does not count", func(t *testing.T) {
		// Guards against a mapper lookup loose enough to match the group
		// rather than the Kind, which would re-enable the watch that brings
		// the manager down.
		mapper := apimeta.NewDefaultRESTMapper([]schema.GroupVersion{NemoGuardrailsGVK.GroupVersion()})
		mapper.Add(NemoGuardrailsGVK.GroupVersion().WithKind("TrustyAIService"), apimeta.RESTScopeNamespace)
		installed, err := providerCRDInstalled(mapper)
		if err != nil {
			t.Fatalf("providerCRDInstalled: %v", err)
		}
		if installed {
			t.Fatal("providerCRDInstalled = true for a different Kind in the same group, want false")
		}
	})
}

// stubDiscovery answers one group/version lookup with a fixed result, standing
// in for the API server's /apis/<group>/<version> endpoint.
type stubDiscovery struct {
	kinds []string
	err   error
}

func (s stubDiscovery) ServerResourcesForGroupVersionWithContext(_ context.Context, groupVersion string) (*metav1.APIResourceList, error) {
	if s.err != nil {
		return nil, s.err
	}
	list := &metav1.APIResourceList{GroupVersion: groupVersion}
	for _, kind := range s.kinds {
		list.APIResources = append(list.APIResources, metav1.APIResource{Kind: kind})
	}
	return list, nil
}

// discoveryNotFound is what a group/version nothing serves returns: the REST
// client turns the API server's 404 into a NotFound status error.
func discoveryNotFound() error {
	return apierrors.NewGenericServerResponse(404, "get", schema.GroupResource{}, "", "not found", 0, false)
}

func TestProviderCRDServed(t *testing.T) {
	t.Run("served when the group/version carries the Kind", func(t *testing.T) {
		served, err := providerCRDServed(t.Context(), stubDiscovery{
			kinds: []string{"TrustyAIService", NemoGuardrailsGVK.Kind},
		})
		if err != nil {
			t.Fatalf("providerCRDServed: %v", err)
		}
		if !served {
			t.Fatal("providerCRDServed = false with the Kind in discovery, want true")
		}
	})

	t.Run("not served when the group/version is gone", func(t *testing.T) {
		// Removing the only CRD that defined the group/version takes the whole
		// path with it, which is what envtest does.
		served, err := providerCRDServed(t.Context(), stubDiscovery{err: discoveryNotFound()})
		if err != nil {
			t.Fatalf("providerCRDServed returned an error for an absent group/version: %v", err)
		}
		if served {
			t.Fatal("providerCRDServed = true for a group/version nothing serves, want false")
		}
	})

	t.Run("not served when the group/version outlives the Kind", func(t *testing.T) {
		// The real-cluster case: TrustyAI's other kinds keep
		// trustyai.opendatahub.io/v1alpha1 answering after NemoGuardrails is
		// uninstalled, so a check that trusted the path would report it
		// present and send an operator to fix the provider reference.
		served, err := providerCRDServed(t.Context(), stubDiscovery{
			kinds: []string{"TrustyAIService", "LMEvalJob"},
		})
		if err != nil {
			t.Fatalf("providerCRDServed: %v", err)
		}
		if served {
			t.Fatal("providerCRDServed = true with the Kind absent from discovery, want false")
		}
	})

	t.Run("a broken discovery is an error, not an absence", func(t *testing.T) {
		// Reporting absence here would retract a working binding and blame a
		// missing CRD for an API server blip.
		served, err := providerCRDServed(t.Context(), stubDiscovery{err: errors.New("discovery unavailable")})
		if err == nil {
			t.Fatal("providerCRDServed succeeded on a discovery failure, want an error")
		}
		if served {
			t.Fatal("providerCRDServed = true alongside an error, want false")
		}
	})
}
