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
	"regexp"
	"strconv"
	"strings"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// serverMaxBodyBytesLine matches the Praxis server-scoped max_body_bytes field
// (two-space indent under server:). Filter-level max_body_bytes uses deeper
// indentation and must not be rewritten by the annotation patcher.
var serverMaxBodyBytesLine = regexp.MustCompile(`(?m)^(  max_body_bytes: )\d+\s*$`)

// pluginConfigMapBodyBytesKeys are ConfigMap data keys whose server
// max_body_bytes the annotation overrides. Only pre-extproc.yaml: post-auth /
// ExternalModel extproc.yaml is rewritten by configureSharedExtProc /
// configureExternalModelExtProc (including translation accumulation headroom),
// so patching that key here would clobber the rendered server ceiling.
var pluginConfigMapBodyBytesKeys = []string{"pre-extproc.yaml"}

// ExtProcMaxBodyBytesFor returns the effective ExtProc server body budget for
// mtc (annotation or DefaultExtProcMaxBodyBytes). Exported so withdrawal hash
// checks use the same budget the tenant reconciler rendered.
func ExtProcMaxBodyBytesFor(mtc *unstructured.Unstructured) int64 {
	return resolveExtProcMaxBodyBytes(mtc, logr.Discard())
}

// resolveExtProcMaxBodyBytes reads AnnotationExtProcMaxBodyBytes from the
// MaasTenantConfig. Absent or invalid values fall back to
// DefaultExtProcMaxBodyBytes; invalid values also log a warning.
func resolveExtProcMaxBodyBytes(mtc *unstructured.Unstructured, log logr.Logger) int64 {
	if mtc == nil {
		return DefaultExtProcMaxBodyBytes
	}
	raw, ok := mtc.GetAnnotations()[AnnotationExtProcMaxBodyBytes]
	if !ok || strings.TrimSpace(raw) == "" {
		return DefaultExtProcMaxBodyBytes
	}
	value, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
	if err != nil || value < 1 || value > MaxExtProcMaxBodyBytes {
		log.Info("Ignoring invalid ExtProc max body bytes annotation; using default",
			"annotation", AnnotationExtProcMaxBodyBytes,
			"value", raw,
			"default", DefaultExtProcMaxBodyBytes,
			"max", MaxExtProcMaxBodyBytes,
		)
		return DefaultExtProcMaxBodyBytes
	}
	return value
}

// configureExtProcMaxBodyBytes rewrites server-scoped max_body_bytes in owned
// plugin ConfigMaps' pre-extproc.yaml. Rendered post-auth / ExternalModel
// chains receive maxBodyBytes via configureSharedExtProc and
// configureExternalModelExtProc; this pass covers the vendored pre-auth YAML
// those helpers leave untouched.
func configureExtProcMaxBodyBytes(resources []unstructured.Unstructured, maxBodyBytes int64) error {
	if maxBodyBytes < 1 {
		return fmt.Errorf("extproc max body bytes must be >= 1, got %d", maxBodyBytes)
	}
	patched := 0
	for i := range resources {
		cm := &resources[i]
		if cm.GetKind() != "ConfigMap" || !isOwnedPluginConfigMap(cm.GetName()) {
			continue
		}
		n, err := patchPluginConfigMapMaxBodyBytes(cm, maxBodyBytes)
		if err != nil {
			return fmt.Errorf("ConfigMap %s/%s: %w", cm.GetNamespace(), cm.GetName(), err)
		}
		patched += n
	}
	if patched == 0 {
		return fmt.Errorf("no plugin ConfigMap server max_body_bytes lines found to apply body budget %d", maxBodyBytes)
	}
	return nil
}

// configureExtProcBodyLimits applies the shared body budget to plugin ConfigMaps
// and pairs GRPC_MAX_RECV_MSG_SIZE with the same ceiling.
func configureExtProcBodyLimits(resources []unstructured.Unstructured, maxBodyBytes int64) error {
	if err := configureExtProcMaxBodyBytes(resources, maxBodyBytes); err != nil {
		return fmt.Errorf("configure ExtProc max body bytes: %w", err)
	}
	if err := configureExtProcGrpcRecvMsgSize(resources, maxBodyBytes); err != nil {
		return fmt.Errorf("configure ExtProc gRPC recv size: %w", err)
	}
	return nil
}

func isOwnedPluginConfigMap(name string) bool {
	return strings.Contains(name, PayloadProcessingPluginsConfigMapName) ||
		strings.HasPrefix(name, PayloadProcessingExternalModelName+"-plugins")
}

func patchPluginConfigMapMaxBodyBytes(cm *unstructured.Unstructured, maxBodyBytes int64) (int, error) {
	data, found, err := unstructured.NestedStringMap(cm.Object, "data")
	if err != nil {
		return 0, fmt.Errorf("read data: %w", err)
	}
	if !found {
		return 0, nil
	}
	patched := 0
	for _, key := range pluginConfigMapBodyBytesKeys {
		raw, ok := data[key]
		if !ok || !strings.Contains(raw, "max_body_bytes:") {
			continue
		}
		updated, n := rewriteServerMaxBodyBytes(raw, maxBodyBytes)
		if n == 0 {
			continue
		}
		data[key] = updated
		patched += n
	}
	if patched == 0 {
		return 0, nil
	}
	if err := unstructured.SetNestedStringMap(cm.Object, data, "data"); err != nil {
		return patched, fmt.Errorf("write data: %w", err)
	}
	return patched, nil
}

func rewriteServerMaxBodyBytes(yaml string, maxBodyBytes int64) (string, int) {
	replacement := fmt.Sprintf("${1}%d", maxBodyBytes)
	count := 0
	updated := serverMaxBodyBytesLine.ReplaceAllStringFunc(yaml, func(match string) string {
		count++
		return serverMaxBodyBytesLine.ReplaceAllString(match, replacement)
	})
	return updated, count
}
