#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
command -v yq >/dev/null || { echo 'yq is required for the static render test' >&2; exit 1; }
TEST_STATE=$(mktemp -d)
OUTPUT="$TEST_STATE/rendered"
trap 'rm -rf "$TEST_STATE"' EXIT

export OPENSHIFT_E2E_CONTROLLER_NAMESPACE=xmp-controller-test
export OPENSHIFT_E2E_BACKEND_NAMESPACE=xmp-backend-test
export OPENSHIFT_E2E_IMAGE_PROJECT=xmp-images-test
export OPENSHIFT_E2E_TENANT_NAMESPACE=xmp-tenant-test
export OPENSHIFT_E2E_RUN_ID=render-test-123
export OPENSHIFT_E2E_GATEWAY_NAME=xmp-gateway
export OPENSHIFT_E2E_GATEWAY_NAMESPACE=xmp-controller-test
export OPENSHIFT_E2E_GATEWAY_TLS_SECRET=xmp-gateway-tls
export OPENSHIFT_E2E_GATEWAY_CLASS=istio
export ROLE_NAME=xmp-controller-role-render-test-123
export CONTROLLER_IMAGE=registry.example.test/controller@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
export EXTPROC_IMAGE=registry.example.test/extproc@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
export KATAN_IMAGE=ghcr.io/nerdalert/llm-katan@sha256:11379a1ec2fd69dc121eada6c544eb423a7c074414507dc1d474f4abba9df75a
export OPENSHIFT_E2E_USER=render-user
export CLIENT_CA_CONFIGMAP=xmp-gateway-ca-render-test
export EXTERNAL_MODEL_NAMESPACE=xmp-tenant-test
export EXTERNAL_MODEL_RUN_ID=render-test-123
export EXTERNAL_MODEL_PROVIDER_A_ENDPOINT=provider-a.xmp-backend-test.svc.cluster.local
export EXTERNAL_MODEL_PROVIDER_B_ENDPOINT=provider-b.xmp-backend-test.svc.cluster.local
export EXTERNAL_MODEL_PROVIDER_CA_PATH=/etc/external-model-e2e/provider-ca/service-ca.crt
export EXTERNAL_MODEL_OPENAI_SECRET=openai-provider-credentials
export EXTERNAL_MODEL_OPENAI_SUBSCRIPTION=openai-e2e-subscription
export EXTERNAL_MODEL_OPENAI_POLICY=openai-e2e-access
export EXTERNAL_MODEL_OPENAI_USER=render-user
export EXTERNAL_MODEL_CLIENT_NAME=xmp-client-render-test
export EXTERNAL_MODEL_CLIENT_NAMESPACE=xmp-tenant-test
export EXTERNAL_MODEL_CLIENT_IMAGE='curlimages/curl:8.10.1@sha256:d9b4541e214bcd85196d6e92e2753ac6d0ea699f0af5741f8c6cccbfcf00ef4b'
export EXTERNAL_MODEL_CLIENT_RUN_AS_NON_ROOT=true
export EXTERNAL_MODEL_CLIENT_VOLUME_MOUNTS='[{name: gateway-ca, mountPath: /etc/xmp/ca, readOnly: true}]'
export EXTERNAL_MODEL_CLIENT_VOLUMES='[{name: gateway-ca, configMap: {name: xmp-gateway-ca-render-test}}]'

"$ROOT/test/openshift-env/render-manifests.sh" "$OUTPUT"
files=("$OUTPUT"/*.yaml)
"$ROOT/test/openshift-env/render-manifests.sh" "$OUTPUT" providers.yaml.tmpl openai.yaml.tmpl client.yaml.tmpl
files=("$OUTPUT"/*.yaml)
(( ${#files[@]} == 10 )) || { echo "expected ten rendered templates including shared resources" >&2; exit 1; }
if grep -R -nE '\$\{[A-Za-z_][A-Za-z0-9_]*\}|(stringData:|data:.*api-key|Authorization:|Bearer |token:)' "$OUTPUT"; then
  echo "rendered output contains an unresolved or credential-shaped value" >&2
  exit 1
fi
grep -R -q 'name: xmp-gateway' "$OUTPUT/10-gateway.yaml"
grep -q 'provider-a.xmp-backend-test.svc.cluster.local' "$OUTPUT/providers.yaml"
grep -q 'tls.caCertificates: /etc/external-model-e2e/provider-ca/service-ca.crt' "$OUTPUT/providers.yaml"
grep -R -q 'namespace: xmp-tenant-test' "$OUTPUT/40-model-fixtures.yaml"
if grep -q 'tls.caCertificates' "$OUTPUT/40-model-fixtures.yaml"; then
  echo 'model fixture sets the shared provider CA at the model binding' >&2
  exit 1
fi
grep -R -q 'external-model-praxis.opendatahub.io/run-id: render-test-123' "$OUTPUT"
grep -q 'name: gpt-4o-mini' "$OUTPUT/openai.yaml"
grep -q 'name: openai-gpt-4o-mini' "$OUTPUT/openai.yaml"
if grep -q 'api-key' "$OUTPUT/openai.yaml"; then
  echo 'shared OpenAI resources embedded a credential key' >&2
  exit 1
fi
grep -R -q 'ghcr.io/nerdalert/llm-katan@sha256:' "$OUTPUT/30-provider-fixtures.yaml"
[[ $(grep -c -- '--validate-keys' "$OUTPUT/30-provider-fixtures.yaml") -eq 2 ]]
grep -q 'apiFormat: openai-chat' "$OUTPUT/40-model-fixtures.yaml"
grep -q 'path: /v1/chat/completions' "$OUTPUT/40-model-fixtures.yaml"
if grep -R -q '/v1/responses\|apiFormat:.*responses' "$OUTPUT"; then
  echo 'Responses API configuration is outside the supported integration contract' >&2
  exit 1
fi
if grep -q '^  labels:.*app: provider-[ab]' "$OUTPUT/30-provider-fixtures.yaml"; then
  echo 'provider metadata gained an unreviewed app label' >&2
  exit 1
fi
grep -R -q 'registry.example.test/controller@sha256:' "$OUTPUT/20-controller.yaml"
grep -R -q 'registry.example.test/extproc@sha256:' "$OUTPUT/20-controller.yaml"
if grep -R -q -- '--praxis-image\|registry.example.test/praxis@sha256:' "$OUTPUT/20-controller.yaml"; then
  echo 'ExtProc-only controller manifest still contains standalone dataplane image configuration' >&2
  exit 1
fi
grep -R -q 'xmp-gateway' "$OUTPUT/20-controller.yaml"
if grep -En 'name: (provider-a|provider-b|demo-model|xmp-client-)|endpoint: provider-' "$ROOT/test/openshift-env/provision.sh"; then
  echo "stable fixture remains inline in provision.sh" >&2
  exit 1
fi
for file in "${files[@]}"; do yq eval '.' "$file" >/dev/null; done
echo 'render-manifests: PASS'
