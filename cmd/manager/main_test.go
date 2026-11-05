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

package main

import (
	"bytes"
	"io"
	"os"
	"strings"
	"testing"

	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

const wantFallbackImage = "quay.io/opendatahub/odh-praxis-extproc@sha256:a67be44e530961ca7e6dc453220ebe11a6641a2003577f3c4ddb1f426555b874"

func TestResolveExtprocImageUsesEnvVar(t *testing.T) {
	digest := "quay.io/opendatahub/odh-praxis-extproc@sha256:abc123"
	t.Setenv("RELATED_IMAGE_ODH_PRAXIS_EXTPROC_IMAGE", digest)

	got := resolveExtprocImage()
	if got != digest {
		t.Fatalf("resolveExtprocImage() = %q, want %q", got, digest)
	}
}

func TestResolveExtprocImageFallsBackToDefault(t *testing.T) {
	t.Setenv("RELATED_IMAGE_ODH_PRAXIS_EXTPROC_IMAGE", "")
	if err := os.Unsetenv("RELATED_IMAGE_ODH_PRAXIS_EXTPROC_IMAGE"); err != nil {
		t.Fatal(err)
	}

	got := resolveExtprocImage()
	if got != wantFallbackImage {
		t.Fatalf("resolveExtprocImage() = %q, want %q", got, wantFallbackImage)
	}
}

func TestResolveExtprocImageIgnoresEmptyEnvVar(t *testing.T) {
	t.Setenv("RELATED_IMAGE_ODH_PRAXIS_EXTPROC_IMAGE", "")

	got := resolveExtprocImage()
	if got != wantFallbackImage {
		t.Fatalf("resolveExtprocImage() = %q, want %q (empty env should fall back)", got, wantFallbackImage)
	}
}

func TestApplyLogDevelopmentInvalidValueWritesStderr(t *testing.T) {
	t.Setenv("LOG_DEVELOPMENT", "maybe")

	var buf bytes.Buffer
	opts := zap.Options{}
	err := applyLogDevelopment(&opts, &buf)
	if err == nil {
		t.Fatal("expected error for invalid LOG_DEVELOPMENT")
	}
	got := buf.String()
	if !strings.Contains(got, "maybe") {
		t.Fatalf("stderr %q does not mention the invalid value", got)
	}
	if !strings.Contains(got, "invalid LOG_DEVELOPMENT") {
		t.Fatalf("stderr %q does not explain the failure", got)
	}
}

func TestApplyLogDevelopmentTrue(t *testing.T) {
	t.Setenv("LOG_DEVELOPMENT", "true")

	opts := zap.Options{}
	if err := applyLogDevelopment(&opts, io.Discard); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !opts.Development {
		t.Fatal("Development = false, want true")
	}
}

func TestApplyLogDevelopmentFalse(t *testing.T) {
	t.Setenv("LOG_DEVELOPMENT", "false")

	opts := zap.Options{}
	if err := applyLogDevelopment(&opts, io.Discard); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if opts.Development {
		t.Fatal("Development = true, want false")
	}
}

func TestApplyLogDevelopmentUnsetLeavesDefault(t *testing.T) {
	t.Setenv("LOG_DEVELOPMENT", "true")
	if err := os.Unsetenv("LOG_DEVELOPMENT"); err != nil {
		t.Fatal(err)
	}

	opts := zap.Options{}
	var buf bytes.Buffer
	if err := applyLogDevelopment(&opts, &buf); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if opts.Development {
		t.Fatal("Development = true, want false when unset")
	}
	if buf.Len() != 0 {
		t.Fatalf("stderr write on unset: %q", buf.String())
	}
}

func TestBuildMetricsServerOptionsSecure(t *testing.T) {
	opts := buildMetricsServerOptions(":8443", true)
	if opts.BindAddress != ":8443" {
		t.Fatalf("BindAddress = %q, want :8443", opts.BindAddress)
	}
	if !opts.SecureServing {
		t.Fatal("SecureServing = false, want true")
	}
	if opts.CertDir != metricsCertDir {
		t.Fatalf("CertDir = %q, want %q", opts.CertDir, metricsCertDir)
	}
	if opts.FilterProvider == nil {
		t.Fatal("FilterProvider is nil, want non-nil")
	}
}

func TestBuildMetricsServerOptionsInsecure(t *testing.T) {
	opts := buildMetricsServerOptions(":8080", false)
	if opts.BindAddress != ":8080" {
		t.Fatalf("BindAddress = %q, want :8080", opts.BindAddress)
	}
	if opts.SecureServing {
		t.Fatal("SecureServing = true, want false")
	}
	if opts.CertDir != "" {
		t.Fatalf("CertDir = %q, want empty", opts.CertDir)
	}
	if opts.FilterProvider != nil {
		t.Fatal("FilterProvider is set, want nil")
	}
}
