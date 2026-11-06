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
	"fmt"
	"strconv"
	"strings"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// extProcUpstreamClusterNames are the EnvoyFilter CLUSTER patch names this
// controller owns for ExtProc. Concurrency budgets are applied to each.
var extProcUpstreamClusterNames = map[string]struct{}{
	"payload-pre-processing-extproc":            {},
	"payload-processing-extproc":                {},
	"payload-processing-external-model-extproc": {},
}

// resolveExtProcMaxRequests reads AnnotationExtProcMaxRequests from the
// MaasTenantConfig. Absent or invalid values fall back to
// DefaultExtProcMaxRequests; invalid values also log a warning.
func resolveExtProcMaxRequests(mtc *unstructured.Unstructured, log logr.Logger) int64 {
	if mtc == nil {
		return DefaultExtProcMaxRequests
	}
	raw, ok := mtc.GetAnnotations()[AnnotationExtProcMaxRequests]
	if !ok || strings.TrimSpace(raw) == "" {
		return DefaultExtProcMaxRequests
	}
	value, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
	if err != nil || value < 1 || value > MaxExtProcMaxRequests {
		log.Info("Ignoring invalid ExtProc concurrency annotation; using default",
			"annotation", AnnotationExtProcMaxRequests,
			"value", raw,
			"default", DefaultExtProcMaxRequests,
			"max", MaxExtProcMaxRequests,
		)
		return DefaultExtProcMaxRequests
	}
	return value
}

// configureExtProcConcurrency sets circuit_breakers thresholds and
// max_concurrent_streams on every owned ExtProc upstream CLUSTER patch.
// max_retries is omitted: without a route/cluster retry_policy it does nothing,
// and we intentionally do not enable ExtProc retries here.
func configureExtProcConcurrency(resources []unstructured.Unstructured, maxRequests int64) error {
	if maxRequests < 1 {
		return fmt.Errorf("extproc max requests must be >= 1, got %d", maxRequests)
	}
	patched := 0
	for i := range resources {
		filter := &resources[i]
		if filter.GetKind() != "EnvoyFilter" {
			continue
		}
		n, err := patchExtProcEnvoyFilterConcurrency(filter, maxRequests)
		if err != nil {
			return fmt.Errorf("EnvoyFilter %s/%s: %w", filter.GetNamespace(), filter.GetName(), err)
		}
		patched += n
	}
	if patched == 0 {
		return fmt.Errorf("no ExtProc upstream CLUSTER patches found to apply concurrency budget %d", maxRequests)
	}
	return nil
}

func patchExtProcEnvoyFilterConcurrency(filter *unstructured.Unstructured, maxRequests int64) (int, error) {
	configPatches, found, err := unstructured.NestedSlice(filter.Object, "spec", "configPatches")
	if err != nil {
		return 0, fmt.Errorf("read configPatches: %w", err)
	}
	if !found {
		return 0, nil
	}
	patched := 0
	for i, raw := range configPatches {
		patch, ok := raw.(map[string]any)
		if !ok || patch["applyTo"] != "CLUSTER" {
			continue
		}
		body, ok := patch["patch"].(map[string]any)
		if !ok {
			continue
		}
		value, ok := body["value"].(map[string]any)
		if !ok {
			continue
		}
		clusterName, _ := value["name"].(string)
		if _, owned := extProcUpstreamClusterNames[clusterName]; !owned {
			continue
		}
		if err := setExtProcClusterConcurrency(value, maxRequests); err != nil {
			return patched, fmt.Errorf("CLUSTER patch %d (%s): %w", i, clusterName, err)
		}
		patched++
	}
	if patched > 0 {
		if err := unstructured.SetNestedSlice(filter.Object, configPatches, "spec", "configPatches"); err != nil {
			return patched, fmt.Errorf("write configPatches: %w", err)
		}
	}
	return patched, nil
}

func setExtProcClusterConcurrency(value map[string]any, maxRequests int64) error {
	thresholds := []any{
		map[string]any{
			"max_connections":      maxRequests,
			"max_pending_requests": maxRequests,
			"max_requests":         maxRequests,
		},
	}
	if err := unstructured.SetNestedSlice(value, thresholds, "circuit_breakers", "thresholds"); err != nil {
		return fmt.Errorf("circuit_breakers.thresholds: %w", err)
	}
	if err := unstructured.SetNestedField(value, maxRequests,
		"typed_extension_protocol_options",
		"envoy.extensions.upstreams.http.v3.HttpProtocolOptions",
		"explicit_http_config",
		"http2_protocol_options",
		"max_concurrent_streams",
	); err != nil {
		return fmt.Errorf("max_concurrent_streams: %w", err)
	}
	// Preserve @type if the protocol-options block already existed; otherwise
	// EnvoyFilter merge still needs the type URL for typed_extension_protocol_options.
	protoOpts, found, err := unstructured.NestedMap(value,
		"typed_extension_protocol_options",
		"envoy.extensions.upstreams.http.v3.HttpProtocolOptions",
	)
	if err != nil {
		return fmt.Errorf("read protocol options: %w", err)
	}
	if found {
		if _, ok := protoOpts["@type"]; !ok {
			protoOpts["@type"] = "type.googleapis.com/envoy.extensions.upstreams.http.v3.HttpProtocolOptions"
			if err := unstructured.SetNestedMap(value, protoOpts,
				"typed_extension_protocol_options",
				"envoy.extensions.upstreams.http.v3.HttpProtocolOptions",
			); err != nil {
				return fmt.Errorf("write protocol options @type: %w", err)
			}
		}
	}
	return nil
}
