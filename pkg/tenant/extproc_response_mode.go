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
	"strings"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// allowedExtProcResponseBodyModes are Envoy ExtProc BodySendMode values this
// controller will apply to the shared ipp filter.
var allowedExtProcResponseBodyModes = map[string]struct{}{
	"STREAMED": {},
	"NONE":     {},
	"BUFFERED": {},
}

// resolveExtProcResponseBodyMode reads AnnotationExtProcResponseBodyMode from
// the MaasTenantConfig. Absent or invalid values fall back to
// DefaultExtProcResponseBodyMode; invalid values also log a warning.
func resolveExtProcResponseBodyMode(mtc *unstructured.Unstructured, log logr.Logger) string {
	if mtc == nil {
		return DefaultExtProcResponseBodyMode
	}
	raw, ok := mtc.GetAnnotations()[AnnotationExtProcResponseBodyMode]
	if !ok || strings.TrimSpace(raw) == "" {
		return DefaultExtProcResponseBodyMode
	}
	value := strings.ToUpper(strings.TrimSpace(raw))
	if _, allowed := allowedExtProcResponseBodyModes[value]; !allowed {
		log.Info("Ignoring invalid ExtProc response body mode annotation; using default",
			"annotation", AnnotationExtProcResponseBodyMode,
			"value", raw,
			"default", DefaultExtProcResponseBodyMode,
		)
		return DefaultExtProcResponseBodyMode
	}
	return value
}
