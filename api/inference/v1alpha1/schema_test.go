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
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"sigs.k8s.io/yaml"
)

func inferenceCRDs(t *testing.T) []*apiextensionsv1.CustomResourceDefinition {
	t.Helper()
	var crds []*apiextensionsv1.CustomResourceDefinition
	for _, plural := range []string{"externalmodels", "externalproviders"} {
		data, err := os.ReadFile(filepath.Join("..", "..", "..", "config", "crd", "bases", "inference.opendatahub.io_"+plural+".yaml"))
		require.NoError(t, err)
		crd := &apiextensionsv1.CustomResourceDefinition{}
		require.NoError(t, yaml.UnmarshalStrict(data, crd))
		crds = append(crds, crd)
	}
	return crds
}

// The validation-only fixture comes from MaaS commit
// 353a85e841d8442afc976a539e49e2925b73e017, deployment/base/maas-controller/crd/bases,
// as packaged by ai-gateway-operator. Descriptions are omitted; validation,
// defaulting and list semantics are preserved.
func installedLegacySchemas(t *testing.T) map[string]apiextensionsv1.JSONSchemaProps {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", "installed-legacy-schema.json"))
	require.NoError(t, err)
	var schemas map[string]apiextensionsv1.JSONSchemaProps
	require.NoError(t, json.Unmarshal(data, &schemas))
	return schemas
}

func stripDescriptions(schema *apiextensionsv1.JSONSchemaProps) {
	schema.Description = ""
	for name, property := range schema.Properties {
		stripDescriptions(&property)
		schema.Properties[name] = property
	}
	if schema.Items != nil && schema.Items.Schema != nil {
		stripDescriptions(schema.Items.Schema)
	}
	if schema.AdditionalProperties != nil && schema.AdditionalProperties.Schema != nil {
		stripDescriptions(schema.AdditionalProperties.Schema)
	}
}

func TestInstalledLegacySchemaCompatibility(t *testing.T) {
	legacy := installedLegacySchemas(t)
	for _, crd := range inferenceCRDs(t) {
		t.Run(crd.Spec.Names.Kind, func(t *testing.T) {
			actual := crd.Spec.Versions[0].Schema.OpenAPIV3Schema.DeepCopy()
			stripDescriptions(actual)
			expected := legacy[crd.Spec.Names.Plural]
			status := expected.Properties["status"]
			// Keep AGC's pre-existing status attestations as well as the installed legacy fields.
			status.Properties["observedGeneration"] = apiextensionsv1.JSONSchemaProps{Type: "integer", Format: "int64"}
			if crd.Spec.Names.Kind == "ExternalModel" {
				status.Properties["overlayDigest"] = apiextensionsv1.JSONSchemaProps{Type: "string"}
				status.Properties["overlayGeneration"] = apiextensionsv1.JSONSchemaProps{Type: "integer", Format: "int64"}
				spec := actual.Properties["spec"]
				delete(spec.Properties, "gatewayRefs")
				ref := spec.Properties["externalProviderRefs"].Items.Schema.Properties["ref"]
				delete(ref.Properties, "namespace")
				delete(actual.Properties["status"].Properties, "gateways")

				// Provider names retain the broader name pattern accepted by the local
				// API mirrors and AGC, in addition to the installed name-only pattern.
				expectedRef := expected.Properties["spec"].Properties["externalProviderRefs"].Items.Schema.Properties["ref"]
				name := expectedRef.Properties["name"]
				name.Pattern = expected.Properties["spec"].Properties["externalProviderRefs"].Items.Schema.Properties["auth"].Properties["secretRef"].Properties["name"].Pattern
				expectedRef.Properties["name"] = name
			}
			assert.Equal(t, expected, *actual, "legacy validation/defaults and AGC status must survive the additive extension")
		})
	}
}
