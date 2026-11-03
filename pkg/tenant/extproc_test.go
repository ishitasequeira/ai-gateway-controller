package tenant

import (
	"fmt"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/opendatahub-io/ai-gateway-controller/pkg/envelope"
)

func TestConfigureExternalModelExtProcProjectsReferencesAndTrustedHandoff(t *testing.T) {
	resources := []unstructured.Unstructured{
		{Object: map[string]any{
			"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "payload-processing-external-model-plugins-redteam", "namespace": "tenant-a"},
			"data": map[string]any{"extproc.yaml": "old", "pre-extproc.yaml": "pre"},
		}},
		{Object: map[string]any{
			"apiVersion": "apps/v1", "kind": "Deployment", "metadata": map[string]any{"name": "payload-processing-external-model-redteam", "namespace": "tenant-a"},
			"spec": map[string]any{"template": map[string]any{"spec": map[string]any{
				"volumes":    []any{map[string]any{"name": "routing-overlay-volume"}},
				"containers": []any{map[string]any{"name": "payload-processing", "volumeMounts": []any{map[string]any{"name": "routing-overlay-volume"}}}},
			}}},
		}},
		externalModelEnvoyFilterFixtureForTenant("redteam", "gateway-system"),
	}
	candidates := []envelope.Candidate{
		{
			Cluster:  "provider-provider-b",
			StableID: envelope.CandidateStableID("model-b", "provider-b"),
			Credential: &envelope.Credential{
				Strategy:  "bearer_token",
				SecretRef: envelope.SecretRef{Name: "b-secret", Namespace: "tenant-a", Key: "api-key"},
			},
		},
		{
			Cluster:  "provider-provider-a",
			StableID: envelope.CandidateStableID("model-a", "provider-a"),
			Credential: &envelope.Credential{
				Strategy:  "bearer_token",
				SecretRef: envelope.SecretRef{Name: "a-secret", Namespace: "tenant-a", Key: "api-key"},
			},
		},
	}
	if err := configureExternalModelExtProc(resources, "redteam", "tenant-a", candidates, true); err != nil {
		t.Fatal(err)
	}
	data, found, err := unstructured.NestedStringMap(resources[0].Object, "data")
	if err != nil || !found {
		t.Fatalf("ConfigMap data missing: found=%v err=%v", found, err)
	}
	config, found := data["extproc.yaml"]
	if !found {
		t.Fatal("post-auth extproc.yaml is missing")
	}
	for _, want := range []string{
		"filter: intelligent_route",
		"filter: credential_inject",
		"provider_hop_clusters: [\"provider-provider-a\", \"provider-provider-b\"]",
		"  max_body_bytes: 10485760\n",
		"filter: stream_usage_inject\n        max_body_bytes: 10485760\n",
		"api_translation_enabled: true\n        max_body_bytes: 10485760\n",
		"strategy: bearer_token",
		"a-secret",
		"b-secret",
	} {
		if !strings.Contains(config, want) {
			t.Fatalf("post-auth config missing %q:\n%s", want, config)
		}
	}
	profiled := []string{
		"filter: intelligent_route",
		"filter: stream_usage_inject",
		"filter: openai_chat_completions_to_anthropic_stream",
		"filter: openai_chat_completions_to_anthropic\n",
		"filter: credential_inject",
	}
	positions := make([]int, len(profiled))
	for i, want := range profiled {
		positions[i] = strings.Index(config, want)
		if positions[i] < 0 {
			t.Fatalf("post-auth config missing %q:\n%s", want, config)
		}
		if i > 0 && positions[i] <= positions[i-1] {
			t.Fatalf("provider selection, JSON, SSE, and credential filters are out of request/response order:\n%s", config)
		}
	}
	if strings.Contains(config, "secret-value") {
		t.Fatal("secret value appeared in post-auth configuration")
	}
	if strings.Contains(config, "allow_unbounded_body: true") {
		t.Fatal("dedicated ExternalModel ExtProc must enforce its configured body accumulation ceiling")
	}
	assertExternalModelEarlyBodyDelivery(t, resources[2], false)
	volumes, _, err := unstructured.NestedSlice(resources[1].Object, "spec", "template", "spec", "volumes")
	if err != nil {
		t.Fatal(err)
	}
	if len(volumes) != 2 {
		t.Fatalf("volumes = %d, want routing plus projected credentials", len(volumes))
	}
	annotations, _, err := unstructured.NestedStringMap(resources[1].Object, "spec", "template", "metadata", "annotations")
	if err != nil || annotations["ai-gateway-controller.opendatahub.io/extproc-config-sha256"] == "" {
		t.Fatalf("post-auth config hash missing: %#v (err=%v)", annotations, err)
	}
}

func TestExternalModelConfigMatchesMainWhenTranslationIsOff(t *testing.T) {
	resources := []unstructured.Unstructured{
		{Object: map[string]any{
			"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "payload-processing-external-model-plugins", "namespace": "tenant-a"},
			"data": map[string]any{"pre-extproc.yaml": "pre", "extproc.yaml": "old"},
		}},
		{Object: map[string]any{
			"apiVersion": "apps/v1", "kind": "Deployment", "metadata": map[string]any{"name": "payload-processing-external-model", "namespace": "tenant-a"},
			"spec": map[string]any{"template": map[string]any{"spec": map[string]any{
				"volumes": []any{}, "containers": []any{map[string]any{"name": "payload-processing", "volumeMounts": []any{}}},
			}}},
		}},
		externalModelEnvoyFilterFixtureForTenant("", "gateway-system"),
	}
	if err := configureExternalModelExtProc(resources, "", "tenant-a", []envelope.Candidate{{
		Cluster: "provider-openai", Name: "demo", StableID: envelope.CandidateStableID("demo-model", "openai"),
	}}, false); err != nil {
		t.Fatal(err)
	}
	data, _, err := unstructured.NestedStringMap(resources[0].Object, "data")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(data["extproc.yaml"], "credential_inject") {
		t.Fatalf("credential_inject must be omitted without credential references:\n%s", data["extproc.yaml"])
	}
	config := data["extproc.yaml"]
	const want = `server:
  grpc_address: "0.0.0.0:9004"
  health_address: "0.0.0.0:50052"
  metrics_address: "0.0.0.0:9090"
  tls:
    mode: self_signed

filter_chains:
  - name: post-auth
    filters:
      - filter: headers
        request_remove:
          - authorization
          - x-api-key
          - x-origin-cluster
          - x-maas-username
          - x-maas-username-oc
          - x-maas-username-token
          - x-maas-group
          - x-maas-group-oc
          - x-maas-group-token
          - x-maas-keyname
          - x-maas-key-id
          - x-maas-subscription
          - x-maas-subscription-rate-limit-id
          - x-maas-cost-center
          - x-maas-organization-id
          - x-maas-identity
          - x-maas-admin-scope
          - x-maas-tenant
          - x-maas-invocation-mode
          - x-maas-policy-revision
          - x-maas-responses-owner
          - x-maas-return-all-models
          - x-maas-model
      - filter: intelligent_route
        overlay_file: /etc/praxis/routing/routing-overlay.json
        model_header: X-Gateway-Model-Name
        provider_hop_clusters: ["provider-openai"]
        reload: {enabled: true, debounce_ms: 500}
      - filter: stream_usage_inject
        max_body_bytes: 10485760

insecure_options:
  allow_unbounded_body: true
`
	if config != want {
		t.Fatalf("translation-off ExtProc config differs from pinned main:\n--- got ---\n%s\n--- want ---\n%s", config, want)
	}
	assertExternalModelEarlyBodyDelivery(t, resources[2], false)
	hash, found, err := unstructured.NestedString(resources[1].Object, "spec", "template", "metadata", "annotations", "ai-gateway-controller.opendatahub.io/extproc-config-sha256")
	if err != nil || !found || hash == "" {
		t.Fatal("config hash is empty")
	}
}

func TestExtprocCredentialsRenderPerEntryHeader(t *testing.T) {
	// An apikey provider that diverges from the filter default (Azure → api-key)
	// must render an explicit header on its entry, while a bearer_token entry
	// (header fixed filter-side) must not.
	candidates := []envelope.Candidate{
		{
			Cluster:  "provider-azure",
			StableID: envelope.CandidateStableID("model-azure", "azure"),
			Credential: &envelope.Credential{
				Strategy:  "apikey",
				Header:    "api-key",
				SecretRef: envelope.SecretRef{Name: "azure-secret", Namespace: "tenant-a", Key: "api-key"},
			},
		},
		{
			Cluster:  "provider-openai",
			StableID: envelope.CandidateStableID("model-openai", "openai"),
			Credential: &envelope.Credential{
				Strategy:  "bearer_token",
				SecretRef: envelope.SecretRef{Name: "openai-secret", Namespace: "tenant-a", Key: "api-key"},
			},
		},
	}
	credentials, err := extprocCredentials("tenant-a", candidates)
	if err != nil {
		t.Fatal(err)
	}
	config := extprocPostAuthConfig(candidates, credentials, false)
	if !strings.Contains(config, "strategy: apikey\n            header: api-key\n") {
		t.Fatalf("azure entry must render api-key header between strategy and file:\n%s", config)
	}
	// The bearer_token entry carries no header override.
	if strings.Contains(config, "strategy: bearer_token\n            header:") {
		t.Fatalf("bearer_token entry must not render a header line:\n%s", config)
	}
}

func TestExtprocCredentialsDeterministicWhenSecretSharedWithDifferentHeader(t *testing.T) {
	// Two providers may share one Secret but diverge on the injected header
	// (e.g. an Azure apikey entry and a bearer_token entry keyed to the same
	// Secret). Both survive dedup because header is part of the dedup identity,
	// so the sort key must include it too: otherwise the two entries, equal on
	// name+key+strategy-prefix terms, could order either way and churn the
	// config hash, restarting ExtProc for no functional change.
	azure := envelope.Candidate{
		Cluster:  "provider-azure",
		StableID: envelope.CandidateStableID("model-azure", "azure"),
		Credential: &envelope.Credential{
			Strategy:  "apikey",
			Header:    "api-key",
			SecretRef: envelope.SecretRef{Name: "shared-secret", Namespace: "tenant-a", Key: "api-key"},
		},
	}
	bearer := envelope.Candidate{
		Cluster:  "provider-openai",
		StableID: envelope.CandidateStableID("model-openai", "openai"),
		Credential: &envelope.Credential{
			Strategy:  "bearer_token",
			SecretRef: envelope.SecretRef{Name: "shared-secret", Namespace: "tenant-a", Key: "api-key"},
		},
	}

	forward, err := extprocCredentials("tenant-a", []envelope.Candidate{azure, bearer})
	if err != nil {
		t.Fatal(err)
	}
	reverse, err := extprocCredentials("tenant-a", []envelope.Candidate{bearer, azure})
	if err != nil {
		t.Fatal(err)
	}
	if len(forward) != 2 {
		t.Fatalf("both entries sharing a Secret with different headers must be retained, got %d: %+v", len(forward), forward)
	}
	if forward[0] != reverse[0] || forward[1] != reverse[1] {
		t.Fatalf("credential order must be independent of candidate input order:\nforward=%+v\nreverse=%+v", forward, reverse)
	}
	// Total order resolves the tie on strategy ("apikey" < "bearer_token").
	if forward[0].strategy != "apikey" || forward[1].strategy != "bearer_token" {
		t.Fatalf("expected apikey before bearer_token, got %+v", forward)
	}
}

func TestExtprocPostAuthConfigNeverEmitsHeaderForBearerToken(t *testing.T) {
	// The credential_inject filter hard-rejects the entire config if a
	// bearer_token entry carries a header, taking the tenant's whole ExtProc
	// config down. Even if a header leaks onto a bearer_token credential (e.g. a
	// stale copy-Secret annotation restored on the attached path), the render
	// must not emit it.
	credentials := []extprocCredential{{
		name: "openai-secret", namespace: "tenant-a", key: "api-key",
		strategy: "bearer_token", header: "api-key", // header must be dropped
		file: "/etc/praxis/credentials/openai-secret-abc/api-key",
	}}
	config := extprocPostAuthConfig(nil, credentials, false)
	if strings.Contains(config, "strategy: bearer_token\n            header:") {
		t.Fatalf("bearer_token entry must never render a header line:\n%s", config)
	}
}

func TestExtprocCredentialsRejectCrossNamespaceProvider(t *testing.T) {
	candidates := []envelope.Candidate{{
		StableID: envelope.CandidateStableID("model", "provider"),
		Credential: &envelope.Credential{
			Strategy:  "bearer_token",
			SecretRef: envelope.SecretRef{Name: "secret", Namespace: "other", Key: "api-key"},
		},
	}}
	if _, err := extprocCredentials("tenant-a", candidates); err == nil || !strings.Contains(err.Error(), "cross-namespace") {
		t.Fatalf("expected cross-namespace rejection, got %v", err)
	}
}

func TestExtprocCredentialsRejectsUnsupportedAuth(t *testing.T) {
	candidates := []envelope.Candidate{{
		StableID: envelope.CandidateStableID("model", "provider"),
		Credential: &envelope.Credential{
			Strategy:  "oauth2",
			SecretRef: envelope.SecretRef{Name: "secret", Namespace: "tenant-a", Key: "api-key"},
		},
	}}
	if _, err := extprocCredentials("tenant-a", candidates); err == nil || !strings.Contains(err.Error(), "unsupported ExtProc credential strategy") {
		t.Fatalf("expected unsupported-auth rejection, got %v", err)
	}
}

func TestSharedExtProcConfigDefaultOff(t *testing.T) {
	config := sharedExtProcConfig(false)
	if strings.Contains(config, "anthropic_messages_to_chat_completions") || strings.Contains(config, "path_rewrite") {
		t.Fatalf("default-off shared chain unexpectedly contains translation:\n%s", config)
	}
	if !strings.Contains(config, "filter: request_id") {
		t.Fatalf("default shared chain lost request_id:\n%s", config)
	}
	// Internal models are served through the shared chain: their streams must
	// report usage, as IPP's include_usage injection made them do.
	if !strings.Contains(config, "filter: request_id\n      - filter: stream_usage_inject\n        max_body_bytes: 10485760\n") {
		t.Fatalf("default shared chain does not inject stream usage:\n%s", config)
	}
}

// TestSharedExtProcConfigHasHeadersGuard verifies that API translation does
// not alter the leading shared-chain security boundary.
func TestSharedExtProcConfigHasHeadersGuard(t *testing.T) {
	for _, apiTranslation := range []bool{false, true} {
		t.Run(fmt.Sprintf("apiTranslation=%t", apiTranslation), func(t *testing.T) {
			assertSharedConfigHasLeadingHeadersGuard(t, sharedExtProcConfig(apiTranslation))
		})
	}
}

func TestSharedExtProcConfigUsesRegisteredOrderedMessagesFilters(t *testing.T) {
	config := sharedExtProcConfig(true)
	ordered := []string{
		"filter: headers",
		"filter: request_id",
		"filter: stream_usage_inject",
		"filter: anthropic_messages_request",
		"filter: anthropic_messages_to_chat_completions",
		"filter: anthropic_messages_to_chat_completions_stream",
		"filter: headers",
		"filter: path_rewrite",
	}
	previous := -1
	for _, token := range ordered {
		position := strings.Index(config[previous+1:], token)
		if position < 0 {
			t.Fatalf("filter %q appears out of order (position=%d):\n%s", token, position, config)
		}
		previous += position + 1
	}
	for _, want := range []string{
		"on_invalid: reject",
		"path: \"/v1/messages\"",
		"max_body_bytes: 1048576",
		"max_partial_event_bytes: 10485760",
		"content-type: \"text/event-stream\"",
		"pattern: \"^/v1/messages$\"",
		"replacement: \"/v1/chat/completions\"",
		"- x-api-key",
	} {
		if !strings.Contains(config, want) {
			t.Fatalf("translation chain missing %q:\n%s", want, config)
		}
	}
	for _, filter := range []struct {
		name       string
		occurrence int
	}{
		{name: "headers", occurrence: 1},
		{name: "path_rewrite", occurrence: 0},
	} {
		block := extProcFilterBlock(t, config, filter.name, filter.occurrence)
		if !strings.Contains(block, "path: \"/v1/messages\"") {
			t.Fatalf("translation filter %q must be restricted to the exact Messages path:\n%s", filter.name, block)
		}
	}
}

// assertSharedConfigHasLeadingHeadersGuard verifies that the leading shared ipp
// filter removes every sensitive header outside the maas-api management routes.
//
// t reports test failures, and config is the rendered shared Praxis YAML.
func assertSharedConfigHasLeadingHeadersGuard(t *testing.T, config string) {
	t.Helper()
	guard := extProcFilterBlock(t, config, "headers", 0)
	requestID := strings.Index(config, "filter: request_id")
	if requestID < 0 || strings.Index(config, guard) > requestID {
		t.Fatalf("shared headers guard must precede request_id:\n%s", config)
	}
	for _, exemption := range []string{
		"path_prefix: /maas-api/v1",
		"path_prefix: /maas-api/health",
		"path_prefix: /v1/models",
		"path_prefix: /v1/subscriptions",
		"path_prefix: /v1/api-keys",
		"path: /v1/me",
	} {
		if !strings.Contains(guard, exemption) {
			t.Fatalf("shared headers guard missing exemption %q:\n%s", exemption, guard)
		}
	}
	for _, header := range guardedRequestHeaders {
		if !strings.Contains(guard, "\n          - "+header) {
			t.Fatalf("shared headers guard missing header %q:\n%s", header, guard)
		}
	}
}

// extProcFilterBlock returns one named filter block selected by its occurrence.
// It bounds the block at the next filter entry so tests can distinguish
// multiple headers filters in the translation-enabled shared chain.
//
// t reports malformed configurations, config is rendered Praxis YAML, filter
// is its filter token, and occurrence selects zero-based matching instances.
// The returned value is the complete YAML block for that filter.
func extProcFilterBlock(t *testing.T, config, filter string, occurrence int) string {
	t.Helper()
	token := "filter: " + filter
	start := -1
	searchFrom := 0
	for i := 0; i <= occurrence; i++ {
		index := strings.Index(config[searchFrom:], token)
		if index < 0 {
			t.Fatalf("configuration missing occurrence %d of %q:\n%s", occurrence, token, config)
		}
		start = searchFrom + index
		searchFrom = start + len(token)
	}
	end := strings.Index(config[start+len(token):], "\n      - filter:")
	if end < 0 {
		return config[start:]
	}
	return config[start : start+len(token)+end]
}

func TestConfigureSharedExtProcDoesNotRewriteExternalModelResources(t *testing.T) {
	tenantID := "redteam"
	gatewayNamespace := "openshift-ingress"
	resources := []unstructured.Unstructured{
		{Object: map[string]any{
			"apiVersion": "v1", "kind": "ConfigMap",
			"metadata": map[string]any{"name": PayloadProcessingPluginsConfigMapForTenant(tenantID), "namespace": gatewayNamespace},
			"data":     map[string]any{"pre-extproc.yaml": "pre", "extproc.yaml": "old"},
		}},
		{Object: map[string]any{
			"apiVersion": "apps/v1", "kind": "Deployment",
			"metadata": map[string]any{"name": PayloadProcessingDeploymentName(tenantID), "namespace": gatewayNamespace},
			"spec": map[string]any{"template": map[string]any{
				"metadata": map[string]any{"annotations": map[string]any{}},
				"spec":     map[string]any{"containers": []any{map[string]any{"name": "payload-processing"}}},
			}},
		}},
		sharedEnvoyFilterFixture(tenantID, gatewayNamespace),
		{Object: map[string]any{
			"apiVersion": "v1", "kind": "ConfigMap",
			"metadata": map[string]any{"name": PayloadProcessingExternalModelPluginsConfigMapForTenant(tenantID), "namespace": "tenant-a"},
			"data":     map[string]any{"extproc.yaml": "external-model-chain"},
		}},
	}
	if err := configureSharedExtProc(resources, tenantID, gatewayNamespace, true); err != nil {
		t.Fatal(err)
	}
	data, _, err := unstructured.NestedStringMap(resources[0].Object, "data")
	if err != nil {
		t.Fatal(err)
	}
	if data["pre-extproc.yaml"] != "pre" || !strings.Contains(data["extproc.yaml"], "anthropic_messages_request") {
		t.Fatalf("unexpected shared ExtProc ConfigMap: %#v", data)
	}
	assertSharedConfigHasLeadingHeadersGuard(t, data["extproc.yaml"])
	hash, found, err := unstructured.NestedString(resources[1].Object, "spec", "template", "metadata", "annotations", extprocConfigHashAnnotation)
	if err != nil || !found || hash == "" {
		t.Fatalf("shared config hash missing: found=%v err=%v hash=%q", found, err, hash)
	}
	externalData, _, err := unstructured.NestedStringMap(resources[3].Object, "data")
	if err != nil {
		t.Fatal(err)
	}
	if externalData["extproc.yaml"] != "external-model-chain" {
		t.Fatalf("shared config changed ExternalModel ExtProc: %#v", externalData)
	}
	assertSharedResponseMode(t, resources[2], "STREAMED", true)
}

func TestConfigureSharedExtProcKeepsDefaultResponseModeWhenTranslationIsOff(t *testing.T) {
	tenantID := "redteam"
	gatewayNamespace := "openshift-ingress"
	resources := []unstructured.Unstructured{
		{Object: map[string]any{
			"apiVersion": "v1", "kind": "ConfigMap",
			"metadata": map[string]any{"name": PayloadProcessingPluginsConfigMapForTenant(tenantID), "namespace": gatewayNamespace},
			"data":     map[string]any{"extproc.yaml": "old"},
		}},
		{Object: map[string]any{
			"apiVersion": "apps/v1", "kind": "Deployment",
			"metadata": map[string]any{"name": PayloadProcessingDeploymentName(tenantID), "namespace": gatewayNamespace},
			"spec": map[string]any{"template": map[string]any{
				"metadata": map[string]any{"annotations": map[string]any{}},
				"spec":     map[string]any{"containers": []any{map[string]any{"name": "payload-processing"}}},
			}},
		}},
		sharedEnvoyFilterFixture(tenantID, gatewayNamespace),
	}
	if err := configureSharedExtProc(resources, tenantID, gatewayNamespace, false); err != nil {
		t.Fatal(err)
	}
	assertSharedResponseMode(t, resources[2], "BUFFERED", false)
}

func TestTranslationSettingsToggleWithoutStaleRenderedState(t *testing.T) {
	tenantID := "redteam"
	gatewayNamespace := "openshift-ingress"
	resources := []unstructured.Unstructured{
		{Object: map[string]any{
			"apiVersion": "v1", "kind": "ConfigMap",
			"metadata": map[string]any{"name": PayloadProcessingPluginsConfigMapForTenant(tenantID), "namespace": gatewayNamespace},
			"data":     map[string]any{"extproc.yaml": "old"},
		}},
		{Object: map[string]any{
			"apiVersion": "apps/v1", "kind": "Deployment",
			"metadata": map[string]any{"name": PayloadProcessingDeploymentName(tenantID), "namespace": gatewayNamespace},
			"spec": map[string]any{"template": map[string]any{
				"metadata": map[string]any{"annotations": map[string]any{}},
				"spec":     map[string]any{"containers": []any{map[string]any{"name": "payload-processing"}}},
			}},
		}},
		sharedEnvoyFilterFixture(tenantID, gatewayNamespace),
		{Object: map[string]any{
			"apiVersion": "v1", "kind": "ConfigMap",
			"metadata": map[string]any{"name": PayloadProcessingExternalModelPluginsConfigMapForTenant(tenantID), "namespace": "tenant-a"},
			"data":     map[string]any{"extproc.yaml": "old"},
		}},
		{Object: map[string]any{
			"apiVersion": "apps/v1", "kind": "Deployment",
			"metadata": map[string]any{"name": PayloadProcessingExternalModelDeploymentName(tenantID), "namespace": "tenant-a"},
			"spec": map[string]any{"template": map[string]any{
				"metadata": map[string]any{"annotations": map[string]any{}},
				"spec":     map[string]any{"containers": []any{map[string]any{"name": "payload-processing"}}},
			}},
		}},
		externalModelEnvoyFilterFixtureForTenant(tenantID, gatewayNamespace),
	}
	candidates := []envelope.Candidate{{Cluster: "provider-openai"}}
	for _, enabled := range []bool{false, true, false} {
		if err := configureSharedExtProc(resources, tenantID, gatewayNamespace, enabled); err != nil {
			t.Fatalf("configure shared ExtProc with translation=%t: %v", enabled, err)
		}
		if err := configureExternalModelExtProc(resources, tenantID, "tenant-a", candidates, enabled); err != nil {
			t.Fatalf("configure ExternalModel ExtProc with translation=%t: %v", enabled, err)
		}
		assertSharedResponseMode(t, resources[2], map[bool]string{true: "STREAMED", false: "BUFFERED"}[enabled], enabled)
		sharedData, _, err := unstructured.NestedStringMap(resources[0].Object, "data")
		if err != nil {
			t.Fatal(err)
		}
		externalData, _, err := unstructured.NestedStringMap(resources[3].Object, "data")
		if err != nil {
			t.Fatal(err)
		}
		sharedConfig, externalConfig := sharedData["extproc.yaml"], externalData["extproc.yaml"]
		if strings.Contains(sharedConfig, "anthropic_messages_to_chat_completions") != enabled {
			t.Fatalf("shared translation filters stale for enabled=%t:\n%s", enabled, sharedConfig)
		}
		assertExternalModelEarlyBodyDelivery(t, resources[5], false)
		if strings.Contains(externalConfig, "api_translation_enabled: true") != enabled ||
			strings.Contains(externalConfig, "openai_chat_completions_to_anthropic_stream") != enabled ||
			strings.Contains(externalConfig, "openai_chat_completions_to_anthropic\n") != enabled {
			t.Fatalf("ExternalModel translation filters stale for enabled=%t:\n%s", enabled, externalConfig)
		}
		if !strings.Contains(externalConfig, "filter: stream_usage_inject") {
			t.Fatalf("ExternalModel chain lost stream_usage_inject for enabled=%t:\n%s", enabled, externalConfig)
		}
		if enabled && (!strings.Contains(externalConfig, "\n  max_body_bytes: 10485760\n") || strings.Contains(externalConfig, "allow_unbounded_body: true")) {
			t.Fatalf("translation-on body limits are not rendered for enabled=true:\n%s", externalConfig)
		}
		if !enabled && (strings.Contains(externalConfig, "\n  max_body_bytes:") || !strings.Contains(externalConfig, "allow_unbounded_body: true")) {
			t.Fatalf("translation-off body limits are stale for enabled=false:\n%s", externalConfig)
		}
	}
}

func externalModelEnvoyFilterFixtureForTenant(tenantID, gatewayNamespace string) unstructured.Unstructured {
	return unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "networking.istio.io/v1alpha3",
		"kind":       "EnvoyFilter",
		"metadata": map[string]any{
			"name":      PayloadProcessingExternalModelFilterNameForTenant(tenantID),
			"namespace": gatewayNamespace,
		},
		"spec": map[string]any{
			"configPatches": []any{map[string]any{
				"applyTo": "HTTP_FILTER",
				"patch": map[string]any{"value": map[string]any{
					"name": "envoy.filters.http.ext_proc.external-model",
					"typed_config": map[string]any{
						"processing_mode": map[string]any{"request_body_mode": "NONE"},
					},
				}},
			}},
		},
	}}
}

func assertExternalModelEarlyBodyDelivery(t *testing.T, filter unstructured.Unstructured, want bool) {
	t.Helper()
	patches, found, err := unstructured.NestedSlice(filter.Object, "spec", "configPatches")
	if err != nil || !found {
		t.Fatalf("ExternalModel EnvoyFilter configPatches: found=%t err=%v", found, err)
	}
	matched := 0
	for index, raw := range patches {
		patch, ok := raw.(map[string]any)
		if !ok || patch["applyTo"] != "HTTP_FILTER" {
			continue
		}
		name, _, _ := unstructured.NestedString(patch, "patch", "value", "name")
		if name != "envoy.filters.http.ext_proc.external-model" {
			continue
		}
		value, found, err := unstructured.NestedBool(patch, "patch", "value", "typed_config", "send_body_without_waiting_for_header_response")
		if err != nil || found != want || (found && value != want) {
			t.Errorf("ExternalModel patch %d early body delivery = %t found=%t err=%v, want %t", index, value, found, err, want)
		}
		matched++
	}
	if matched == 0 {
		t.Fatal("ExternalModel EnvoyFilter has no post-auth HTTP_FILTER patch")
	}
}

func sharedEnvoyFilterFixture(tenantID, namespace string) unstructured.Unstructured {
	patches := make([]any, 0, 8)
	for range 4 {
		for _, filter := range []struct {
			name string
			mode string
		}{
			{name: "envoy.filters.http.ext_proc.ipp", mode: "BUFFERED"},
			{name: "envoy.filters.http.ext_proc.ipp-pre", mode: "NONE"},
		} {
			typedConfig := map[string]any{
				"processing_mode": map[string]any{"response_body_mode": filter.mode},
			}
			if filter.name == "envoy.filters.http.ext_proc.ipp" {
				typedConfig["route_cache_action"] = "RETAIN"
			}
			patches = append(patches, map[string]any{
				"applyTo": "HTTP_FILTER",
				"patch": map[string]any{"value": map[string]any{
					"name":         filter.name,
					"typed_config": typedConfig,
				}},
			})
		}
	}
	return unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "networking.istio.io/v1alpha3", "kind": "EnvoyFilter",
		"metadata": map[string]any{"name": PayloadProcessingEnvoyFilterName(tenantID), "namespace": namespace},
		"spec":     map[string]any{"configPatches": patches},
	}}
}

func assertSharedResponseMode(t *testing.T, filter unstructured.Unstructured, want string, wantRouteCacheRetain bool) {
	t.Helper()
	patches, found, err := unstructured.NestedSlice(filter.Object, "spec", "configPatches")
	if err != nil || !found {
		t.Fatalf("shared EnvoyFilter configPatches: found=%t err=%v", found, err)
	}
	var ipp, ippPre int
	for _, raw := range patches {
		patch, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		name, _, _ := unstructured.NestedString(patch, "patch", "value", "name")
		mode, found, err := unstructured.NestedString(patch, "patch", "value", "typed_config", "processing_mode", "response_body_mode")
		if err != nil || !found {
			continue
		}
		switch name {
		case "envoy.filters.http.ext_proc.ipp":
			if mode != want {
				t.Errorf("shared ipp response_body_mode = %q, want %q", mode, want)
			}
			routeCacheAction, found, err := unstructured.NestedString(patch, "patch", "value", "typed_config", "route_cache_action")
			if err != nil || found != wantRouteCacheRetain || (found && routeCacheAction != "RETAIN") {
				t.Errorf("shared ipp route_cache_action = %q found=%t err=%v, want retain=%t", routeCacheAction, found, err, wantRouteCacheRetain)
			}
			ipp++
		case "envoy.filters.http.ext_proc.ipp-pre":
			if mode != "NONE" {
				t.Errorf("shared ipp-pre response_body_mode = %q, want NONE", mode)
			}
			if action, found, err := unstructured.NestedString(patch, "patch", "value", "typed_config", "route_cache_action"); err != nil || found {
				t.Errorf("shared ipp-pre route_cache_action = %q found=%t err=%v, want unset", action, found, err)
			}
			ippPre++
		}
	}
	if ipp != 4 || ippPre != 4 {
		t.Errorf("shared mode patches = ipp:%d ipp-pre:%d, want 4 each", ipp, ippPre)
	}
}
