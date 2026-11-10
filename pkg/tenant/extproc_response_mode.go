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

// extProcResponseBodyModeAnnotation returns the raw annotation value used by
// watch predicates so annotation-only flips enqueue a reconcile.
func extProcResponseBodyModeAnnotation(mtc *unstructured.Unstructured) string {
	if mtc == nil {
		return ""
	}
	return mtc.GetAnnotations()[AnnotationExtProcResponseBodyMode]
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
	if value != DefaultExtProcResponseBodyMode {
		log.Info("Applying a non-default ExtProc response body mode",
			"annotation", AnnotationExtProcResponseBodyMode,
			"value", value,
		)
	}
	return value
}

// ExtProcResponseBodyModeBuffered is the Envoy BodySendMode Praxis needs to
// evaluate Output-phase AIGuardrail checks (StreamBuffer on the response path).
const ExtProcResponseBodyModeBuffered = "BUFFERED"

// constrainExtProcResponseBodyMode adjusts an resolved response body mode when
// a stronger requirement applies:
//   - Output-phase callouts force BUFFERED (not the STREAMED default): Praxis
//     only runs Output guards on a buffered body. The tenant-wide default
//     remains STREAMED; BUFFERED is applied only while those callouts remain
//     after infra pinning. With apiTranslation enabled, Output checks are
//     unsupported and should not reach this path.
//   - NONE with apiTranslation falls back to DefaultExtProcResponseBodyMode
//     (STREAMED) so Messages SSE translation still receives response chunks.
func constrainExtProcResponseBodyMode(mode string, apiTranslation, needsResponseBody bool, log logr.Logger) string {
	if needsResponseBody && mode != ExtProcResponseBodyModeBuffered {
		log.Info("Forcing BUFFERED ExtProc response body mode: Output-phase guardrail checks require a buffered body",
			"annotation", AnnotationExtProcResponseBodyMode,
			"requested", mode,
			"effective", ExtProcResponseBodyModeBuffered,
		)
		return ExtProcResponseBodyModeBuffered
	}
	if mode == "NONE" && apiTranslation {
		log.Info("Ignoring NONE ExtProc response body mode: API translation requires response body callbacks",
			"annotation", AnnotationExtProcResponseBodyMode,
			"default", DefaultExtProcResponseBodyMode,
		)
		return DefaultExtProcResponseBodyMode
	}
	return mode
}

// calloutsNeedResponseBody reports whether any accepted callout still has an
// Output-phase check after infra pinning (closed stand-ins are request-only).
func calloutsNeedResponseBody(callouts []nemoCallout) bool {
	for _, c := range callouts {
		for _, check := range c.checks {
			if check.response {
				return true
			}
		}
	}
	return false
}
