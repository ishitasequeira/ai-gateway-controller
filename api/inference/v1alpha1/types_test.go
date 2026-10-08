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

package v1alpha1

import (
	"go/ast"
	"go/parser"
	"go/token"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func TestSchemeRegistration(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, AddToScheme(scheme))

	gvk := schema.GroupVersionKind{Group: "inference.opendatahub.io", Version: "v1alpha1", Kind: "ExternalProvider"}
	obj, err := scheme.New(gvk)
	require.NoError(t, err)
	assert.IsType(t, &ExternalProvider{}, obj)

	gvk.Kind = "ExternalModel"
	obj, err = scheme.New(gvk)
	require.NoError(t, err)
	assert.IsType(t, &ExternalModel{}, obj)
}

func TestExternalProviderDeepCopy(t *testing.T) {
	original := &ExternalProvider{
		Spec: ExternalProviderSpec{
			Provider: "openai",
			Endpoint: "api.openai.com",
			Auth: AuthConfig{
				Type:      "apikey",
				SecretRef: NameReference{Name: "key"},
			},
			Config: map[string]string{"project": "my-project", "location": "us-central1"},
		},
	}

	copied := original.DeepCopy()

	assert.Equal(t, original.Spec, copied.Spec)

	// Verify deep copy — mutating the copy must not affect the original
	copied.Spec.Config["project"] = "other-project"
	assert.Equal(t, "my-project", original.Spec.Config["project"])
}

func TestExternalModelDeepCopy(t *testing.T) {
	original := &ExternalModel{
		Spec: ExternalModelSpec{
			GatewayRefs: []NamespacedObjectReference{{Name: "gateway", Namespace: "tenant-a"}},
			ExternalProviderRefs: []ExternalProviderRef{
				{
					Ref:         ExternalProviderReference{Name: "my-openai", Namespace: "providers"},
					TargetModel: "gpt-4o",
					APIFormat:   "openai-chat",
					Path:        "/v1/chat/completions",
				},
			},
		},
		Status: ExternalModelStatus{
			Gateways: []ExternalModelGatewayStatus{{
				NamespacedObjectReference: NamespacedObjectReference{Name: "gateway", Namespace: "tenant-a"},
				HTTPRouteRef:              &NamespacedObjectReference{Name: "route", Namespace: "tenant-a"},
				Conditions:                []metav1.Condition{{Type: "Ready", Reason: "Reconciled"}},
			}},
		},
	}

	copied := original.DeepCopy()

	assert.Equal(t, original, copied)

	// Verify deep copy — mutating the copy must not affect the original
	copied.Spec.ExternalProviderRefs[0].TargetModel = "gpt-3.5"
	assert.Equal(t, "gpt-4o", original.Spec.ExternalProviderRefs[0].TargetModel)
	copied.Spec.ExternalProviderRefs[0].Ref.Namespace = "other-providers"
	assert.Equal(t, "providers", original.Spec.ExternalProviderRefs[0].Ref.Namespace)
	copied.Spec.GatewayRefs[0].Namespace = "tenant-b"
	assert.Equal(t, "tenant-a", original.Spec.GatewayRefs[0].Namespace)
	copied.Status.Gateways[0].HTTPRouteRef.Name = "other-route"
	assert.Equal(t, "route", original.Status.Gateways[0].HTTPRouteRef.Name)
	copied.Status.Gateways[0].Conditions[0].Reason = "Failed"
	assert.Equal(t, "Reconciled", original.Status.Gateways[0].Conditions[0].Reason)
}

// CRD schema validation (patterns) is enforced at admission time by the K8s API server,
// not in Go structs. These tests read the kubebuilder marker from the source
// file directly so the test pattern is always the deployed pattern.

func TestNameReferencePattern(t *testing.T) {
	pattern := regexp.MustCompile(kubebuilderPattern(t, "common_types.go", "NameReference", "Name"))

	valid := []string{
		"my-openai", "a", "openai-key-v2", "a1b2",
		"glm5.1-w8a8-key", "has.dot", "a.b", "a.b.c",
		// These values were accepted by the installed schema and must remain valid.
		"a..b", "a.-b", "a-.b",
	}
	for _, name := range valid {
		assert.True(t, pattern.MatchString(name), "should accept %q", name)
	}

	invalid := []string{
		"My-OpenAI", "UPPERCASE", "-leading-dash", "trailing-", "has/slash", "has space",
		".leading-dot", "trailing-dot.",
	}
	for _, name := range invalid {
		assert.False(t, pattern.MatchString(name), "should reject %q", name)
	}
}

func TestEndpointPattern(t *testing.T) {
	pattern := regexp.MustCompile(kubebuilderPattern(t, "externalprovider_types.go", "ExternalProviderSpec", "Endpoint"))

	valid := []string{"api.openai.com", "bedrock.us-east-1.amazonaws.com", "3-147-232-199.sslip.io", "a.b"}
	for _, ep := range valid {
		assert.True(t, pattern.MatchString(ep), "should accept %q", ep)
	}

	invalid := []string{"openai", "localhost", "-bad.com", "bad-.com", "has space.com", "has/slash.com"}
	for _, ep := range invalid {
		assert.False(t, pattern.MatchString(ep), "should reject %q", ep)
	}
}

// kubebuilderPattern extracts the +kubebuilder:validation:Pattern value from
// the doc comment of a struct field in a Go source file. This is the single
// source of truth: the test reads the actual marker instead of duplicating it.
func kubebuilderPattern(t *testing.T, filename, typeName, fieldName string) string {
	t.Helper()
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, filename, nil, parser.ParseComments)
	require.NoError(t, err)

	for _, decl := range f.Decls {
		gd, ok := decl.(*ast.GenDecl)
		if !ok {
			continue
		}
		for _, spec := range gd.Specs {
			ts, ok := spec.(*ast.TypeSpec)
			if !ok || ts.Name.Name != typeName {
				continue
			}
			st, ok := ts.Type.(*ast.StructType)
			if !ok {
				continue
			}
			for _, field := range st.Fields.List {
				for _, name := range field.Names {
					if name.Name != fieldName {
						continue
					}
					if field.Doc == nil {
						break
					}
					for _, c := range field.Doc.List {
						const prefix = "// +kubebuilder:validation:Pattern=`"
						if strings.HasPrefix(c.Text, prefix) && strings.HasSuffix(c.Text, "`") {
							return c.Text[len(prefix) : len(c.Text)-1]
						}
					}
				}
			}
		}
	}
	t.Fatalf("no +kubebuilder:validation:Pattern marker found for %s.%s in %s", typeName, fieldName, filename)
	return ""
}
