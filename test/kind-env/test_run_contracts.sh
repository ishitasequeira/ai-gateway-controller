#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
source "$ROOT/test/kind-env/lib/image_refs.sh"
source "$ROOT/test/kind-env/lib/maas_api_image_identity.sh"
source "$ROOT/test/kind-env/lib/registry_image_identity.sh"

run_image_resolution() {
  env -i PATH="$PATH" "$@" bash -c '
    set -e
    source "$1"
    resolve_katan_image_refs || exit $?
    printf "%s\t%s\t%s\n" "$KATAN_EFFECTIVE_IMAGE" "$KATAN_PULL_IMAGE" "$KATAN_KIND_IMAGE"
  ' bash "$ROOT/test/kind-env/lib/image_refs.sh"
}

digest=ghcr.io/example/llm-katan@sha256:0123456789abcdef
resolved=$(run_image_resolution BUILD_KATAN=false KATAN_IMAGE="$digest")
IFS=$'\t' read -r effective pull kind <<<"$resolved"
[[ "$pull" == "$digest" ]] || { echo "digest pull reference changed: $pull" >&2; exit 1; }
[[ "$kind" == "llm-katan:external-model-two-plane" && "$kind" != *@* ]] || {
  echo "digest path produced invalid Kind tag: $kind" >&2
  exit 1
}

tag=registry.example/llm-katan:qualification
resolved=$(run_image_resolution BUILD_KATAN=false KATAN_IMAGE="$tag")
IFS=$'\t' read -r effective pull kind <<<"$resolved"
[[ "$pull" == "$tag" ]] || { echo "ordinary tag pull reference changed: $pull" >&2; exit 1; }
[[ "$kind" == "llm-katan:external-model-two-plane" && "$kind" != *@* ]] || {
  echo "ordinary-tag path produced invalid Kind tag: $kind" >&2
  exit 1
}

if run_image_resolution BUILD_KATAN=true KATAN_IMAGE="$digest" >/dev/null 2>&1; then
  echo "digest was accepted as a Docker build target" >&2
  exit 1
fi
if run_image_resolution BUILD_KATAN=false KATAN_IMAGE="$tag" KATAN_KIND_IMAGE="$digest" >/dev/null 2>&1; then
  echo "digest was accepted as an explicit Kind transport tag" >&2
  exit 1
fi

runtime_image_info='{"status":{"id":"sha256:platform-config","repoDigests":["127.0.0.1:32768/maas-api@sha256:platform","127.0.0.1:32768/maas-api@sha256:index"],"repoTags":["127.0.0.1:32768/maas-api:run"]}}'
containerd_image_matches_pushed_ref "$runtime_image_info" \
  "127.0.0.1:32768/maas-api@sha256:index" "127.0.0.1:32768/maas-api:run" || {
  echo "containerd did not match the pushed digest when it followed the platform digest" >&2
  exit 1
}
if containerd_image_matches_pushed_ref "$runtime_image_info" \
  "127.0.0.1:32768/maas-api@sha256:missing" "127.0.0.1:32768/maas-api:run"; then
  echo "containerd accepted an image with a different pushed digest" >&2
  exit 1
fi
if containerd_image_matches_pushed_ref "$runtime_image_info" \
  "127.0.0.1:32768/maas-api@sha256:index" "127.0.0.1:32768/maas-api:missing"; then
  echo "containerd accepted an image with a different tag" >&2
  exit 1
fi

maas_deployment='{"spec":{"selector":{"matchLabels":{"app.kubernetes.io/name":"maas-api"}},"template":{"metadata":{"labels":{"app.kubernetes.io/name":"maas-api","maas.opendatahub.io/tenant-instance":"maas-api-tenant-b"}}}}}'
maas_selector=$(maas_api_deployment_pod_selector "$maas_deployment")
[[ "$maas_selector" == *'maas.opendatahub.io/tenant-instance=maas-api-tenant-b'* ]] || {
  echo "MaaS API pod selector omitted the unique tenant-instance label: $maas_selector" >&2
  exit 1
}
maas_image_digest='127.0.0.1:32768/maas-api@sha256:run'
maas_pods='{"items":[{"metadata":{"name":"old","deletionTimestamp":null},"spec":{"containers":[{"name":"maas-api","image":"maas-api:old"}]},"status":{"phase":"Running","containerStatuses":[{"name":"maas-api","ready":true,"image":"maas-api:old","imageID":"127.0.0.1:32768/maas-api@sha256:old"}]}},{"metadata":{"name":"terminating-new","deletionTimestamp":"2026-10-19T00:00:00Z"},"spec":{"containers":[{"name":"maas-api","image":"maas-api:run"}]},"status":{"phase":"Running","containerStatuses":[{"name":"maas-api","ready":true,"image":"maas-api:alias","imageID":"127.0.0.1:32768/maas-api@sha256:run"}]}}]}'
if maas_api_pods_have_exact_ready_image "$maas_pods" "maas-api:run" "$maas_image_digest"; then
  echo "MaaS API runtime check accepted only stale or terminating pods" >&2
  exit 1
fi
maas_pods=$(jq -c --arg digest "$maas_image_digest" '.items += [{"metadata":{"name":"current","deletionTimestamp":null},"spec":{"containers":[{"name":"maas-api","image":"maas-api:run"}]},"status":{"phase":"Running","containerStatuses":[{"name":"maas-api","ready":true,"image":"maas-api:alias","imageID":$digest}]}}]' <<<"$maas_pods")
maas_api_pods_have_exact_ready_image "$maas_pods" "maas-api:run" "$maas_image_digest" || {
  echo "MaaS API runtime check rejected the requested run tag and exact digest when status.image reported an older alias" >&2
  exit 1
}
if maas_api_pods_have_exact_ready_image "$(jq -c '.items[2].status.containerStatuses[0].imageID = "127.0.0.1:32768/maas-api@sha256:wrong"' <<<"$maas_pods")" "maas-api:run" "$maas_image_digest"; then
  echo "MaaS API runtime check accepted a pod with a different runtime digest" >&2
  exit 1
fi
if maas_api_pods_have_exact_ready_image "$(jq -c '.items[2].spec.containers[0].image = "maas-api:old"' <<<"$maas_pods")" "maas-api:run" "$maas_image_digest"; then
  echo "MaaS API runtime check accepted a pod that did not request the run tag" >&2
  exit 1
fi

run_source=$(<"$ROOT/test/kind-env/run.sh")
rg -q 'KIND_EXPERIMENTAL_DOCKER_NETWORK="\$REGISTRY_NETWORK" kind create cluster' <<<"$run_source" || {
  echo "Kind cluster creation does not use the run-owned registry network" >&2
  exit 1
}
rg -q 'crictl pull "\$MAAS_API_RUNTIME_IMAGE"' <<<"$run_source" || {
  echo "the Kind node does not verify access to the source-built MaaS API registry image" >&2
  exit 1
}
rg -q 'docker network connect --gw-priority=-1 "\$REGISTRY_NETWORK" "\$node"' <<<"$run_source" || {
  echo "the run-owned registry network may replace the Kind node's existing default route" >&2
  exit 1
}
rg -q 'RELATED_IMAGE_ODH_MAAS_API_IMAGE=\$\{MAAS_API_RUNTIME_IMAGE\}' <<<"$run_source" || {
  echo "the MaaS controller is not configured with the run-owned API registry image" >&2
  exit 1
}
rg -q 'apply_maas_api_translation_schema' <<<"$run_source" || {
  echo "the run harness does not expose the MaaS apiTranslation schema fixture overlay" >&2
  exit 1
}
rg -q 'MAAS_API_TRANSLATION_SCHEMA_REV' <<<"$run_source" || {
  echo "the MaaS apiTranslation schema fixture is not revision-pinned" >&2
  exit 1
}
rg -q 'set image "deployment/\$api_deployment" "maas-api=\$\{MAAS_API_RUNTIME_IMAGE\}"' <<<"$run_source" || {
  echo "tenant API Deployments do not select the run-owned source image" >&2
  exit 1
}
if rg -q 'patch deployment maas-api|patch deployment "\$api_deployment"' <<<"$run_source"; then
  echo "the harness overrides MaaS API imagePullPolicy instead of satisfying its Always pull" >&2
  exit 1
fi

ready_pods=$(jq -r -f "$ROOT/test/kind-env/controller-ready-manager-pods.jq" <<'JSON'
{"items":[
  {"metadata":{"name":"current","deletionTimestamp":null},"spec":{"nodeName":"kind-node"},"status":{"phase":"Running","containerStatuses":[{"name":"manager","ready":true,"imageID":"containerd://sha256:current","image":"controller:tag"}]}},
  {"metadata":{"name":"terminating","deletionTimestamp":"2026-10-18T00:00:00Z"},"spec":{"nodeName":"kind-node"},"status":{"phase":"Running","containerStatuses":[{"name":"manager","ready":true,"imageID":"containerd://sha256:old","image":"controller:old"}]}}
]}
JSON
)
[[ "$(wc -l <<<"$ready_pods")" == 1 && "$ready_pods" == current$'\t'kind-node$'\t'containerd://sha256:current$'\t'controller:tag ]] || {
  echo "controller pod filter did not exclude the terminating pod: $ready_pods" >&2
  exit 1
}

provider_manifest="$ROOT/test/kind-env/manifests/provider-selection-proof.yaml"
provider_source="$ROOT/test/kind-env/provider-recorder/main.go"
for suffix in a b c; do
  deployment="provider-selection-proof-${suffix}"
  yq -e "select(.kind == \"Deployment\" and .metadata.name == \"${deployment}\") | .spec.template.spec.securityContext.fsGroup == 65532" "$provider_manifest" >/dev/null || {
    echo "${deployment} recorder receipt volume lacks its unprivileged writable fsGroup" >&2
    exit 1
  }
  yq -e "select(.kind == \"Deployment\" and .metadata.name == \"${deployment}\") | .spec.template.spec.volumes[] | select(.name == \"observations\") | has(\"emptyDir\")" "$provider_manifest" >/dev/null || {
    echo "${deployment} recorder receipt storage is not ephemeral" >&2
    exit 1
  }
  yq -e "select(.kind == \"Deployment\" and .metadata.name == \"${deployment}\") | .spec.template.spec.containers[] | select(.name == \"recorder\") | .env[] | select(.name == \"ENABLE_TEST_OBSERVATIONS\") | .value == \"true\"" "$provider_manifest" >/dev/null || {
    echo "${deployment} recorder does not enable its test-only receipt endpoint" >&2
    exit 1
  }
done
rg -q '"proof-c":        "proof-c"' "$provider_source" || {
  echo "provider recorder does not recognize the third static-weight fixture" >&2
  exit 1
}
rg -q -- '--known-cluster=provider-provider-proof-c' <<<"$run_source" || {
  echo "Kind controller allowlist omits provider C" >&2
  exit 1
}
rg -q 'make_katan_tls provider-proof-c-tls provider-proof-c-ext' <<<"$run_source" || {
  echo "Kind provisioner does not issue the verified TLS certificate for provider C" >&2
  exit 1
}

echo "Kind run harness contracts passed (image identities, runtime pod filtering, exact MaaS image identity, and static-weight receipt fixtures)."
