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

import "testing"

func TestIDDefaultTenantIsEmptyString(t *testing.T) {
	if got := ID(DefaultAITenantName); got != "" {
		t.Fatalf("ID(%q) = %q, want empty string", DefaultAITenantName, got)
	}
}

func TestIDNonDefaultTenantIsUnchanged(t *testing.T) {
	if got := ID("redteam"); got != "redteam" {
		t.Fatalf("ID(%q) = %q, want unchanged", "redteam", got)
	}
}

func TestResourceNameDefaultTenantIsUnsuffixed(t *testing.T) {
	if got := ResourceName("payload-processing", ""); got != "payload-processing" {
		t.Fatalf("ResourceName(base, \"\") = %q, want %q", got, "payload-processing")
	}
}

func TestResourceNameNonDefaultTenantIsSuffixed(t *testing.T) {
	if got := ResourceName("payload-processing", "redteam"); got != "payload-processing-redteam" {
		t.Fatalf("ResourceName(base, tenantID) = %q, want %q", got, "payload-processing-redteam")
	}
}

func TestPerResourceNamingFunctions(t *testing.T) {
	const tenantID = "redteam"

	cases := []struct {
		name string
		got  string
		want string
	}{
		{"PayloadProcessingDeploymentName", PayloadProcessingDeploymentName(tenantID), "payload-processing-redteam"},
		{"PayloadPreProcessingDeploymentName", PayloadPreProcessingDeploymentName(tenantID), "payload-pre-processing-redteam"},
		{"PayloadProcessingServiceName", PayloadProcessingServiceName(tenantID), "payload-processing-redteam"},
		{"PayloadProcessingPostServiceAccountName", PayloadProcessingPostServiceAccountName(tenantID), "payload-processing-post-redteam"},
		{"PayloadPreProcessingServiceName", PayloadPreProcessingServiceName(tenantID), "payload-pre-processing-redteam"},
		{"PayloadProcessingServiceAccountName", PayloadProcessingServiceAccountName(tenantID), "payload-processing-redteam"},
		{"PayloadProcessingNetworkPolicyName", PayloadProcessingNetworkPolicyName(tenantID), "payload-processing-redteam"},
		{"PayloadProcessingEnvoyFilterName", PayloadProcessingEnvoyFilterName(tenantID), "payload-processing-redteam"},
		{"PayloadProcessingPluginsConfigMapForTenant", PayloadProcessingPluginsConfigMapForTenant(tenantID), "payload-processing-plugins-redteam"},
		{
			"PayloadProcessingReaderClusterRoleBindingNameForTenant",
			PayloadProcessingReaderClusterRoleBindingNameForTenant(tenantID),
			"payload-processing-reader-redteam",
		},
		{
			"PayloadProcessingMetricsAuthClusterRoleBindingNameForTenant",
			PayloadProcessingMetricsAuthClusterRoleBindingNameForTenant(tenantID),
			"payload-processing-metrics-auth-redteam",
		},
		{
			"PayloadProcessingMetricsServiceNameForTenant",
			PayloadProcessingMetricsServiceNameForTenant(tenantID),
			"payload-processing-metrics-redteam",
		},
	}

	for _, c := range cases {
		if c.got != c.want {
			t.Errorf("%s(%q) = %q, want %q", c.name, tenantID, c.got, c.want)
		}
	}
}

func TestPerResourceNamingFunctionsDefaultTenantAreUnsuffixed(t *testing.T) {
	cases := []struct {
		name string
		got  string
		want string
	}{
		{"PayloadProcessingDeploymentName", PayloadProcessingDeploymentName(""), "payload-processing"},
		{"PayloadPreProcessingDeploymentName", PayloadPreProcessingDeploymentName(""), "payload-pre-processing"},
		{"PayloadProcessingPluginsConfigMapForTenant", PayloadProcessingPluginsConfigMapForTenant(""), "payload-processing-plugins"},
		{"PayloadProcessingPostServiceAccountName", PayloadProcessingPostServiceAccountName(""), "payload-processing-post"},
		{
			"PayloadProcessingReaderClusterRoleBindingNameForTenant",
			PayloadProcessingReaderClusterRoleBindingNameForTenant(""),
			"payload-processing-reader",
		},
	}

	for _, c := range cases {
		if c.got != c.want {
			t.Errorf("%s(\"\") = %q, want %q (unsuffixed, backward-compatible)", c.name, c.got, c.want)
		}
	}
}
