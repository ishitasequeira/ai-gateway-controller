#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
source "$ROOT/test/kind-env/lib/image_refs.sh"
source "$ROOT/test/kind-env/lib/maas_api_image_identity.sh"
source "$ROOT/test/kind-env/lib/registry_image_identity.sh"

published_extproc_ref='quay.io/opendatahub/odh-praxis-extproc@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef'
published_extproc_runtime='{"status":{"id":"sha256:config","repoDigests":["quay.io/opendatahub/odh-praxis-extproc@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"]}}'
image_reference_is_immutable_digest "$published_extproc_ref" || {
  echo "published ExtProc digest reference was rejected" >&2
  exit 1
}
if image_reference_is_immutable_digest 'quay.io/opendatahub/odh-praxis-extproc:odh-stable'; then
  echo "mutable ExtProc image tag was accepted as immutable" >&2
  exit 1
fi
extproc_image_mode_is_valid published "$published_extproc_ref" 24f2d003ed303a950b6bd8ab042f0d62a5ab4278 linux/amd64 "" || {
  echo "valid published ExtProc qualification inputs were rejected" >&2
  exit 1
}
for invalid_extproc_mode in \
  "published quay.io/opendatahub/odh-praxis-extproc:odh-stable 24f2d003ed303a950b6bd8ab042f0d62a5ab4278 linux/amd64" \
  "published $published_extproc_ref short linux/amd64" \
  "published $published_extproc_ref 24f2d003ed303a950b6bd8ab042f0d62a5ab4278 linux/ppc64le" \
  "published $published_extproc_ref 24f2d003ed303a950b6bd8ab042f0d62a5ab4278 linux/amd64 /local/praxis-ai" \
  "unknown $published_extproc_ref 24f2d003ed303a950b6bd8ab042f0d62a5ab4278 linux/amd64"; do
  read -r mode image source_sha platform ai_repo <<<"$invalid_extproc_mode"
  if extproc_image_mode_is_valid "$mode" "$image" "$source_sha" "$platform" "$ai_repo"; then
    echo "invalid published ExtProc qualification inputs were accepted: $invalid_extproc_mode" >&2
    exit 1
  fi
done
extproc_image_mode_is_valid source praxis-extproc:local "" linux/amd64 "" || {
  echo "default source-built ExtProc mode was rejected" >&2
  exit 1
}
containerd_image_has_exact_digest_reference "$published_extproc_runtime" "$published_extproc_ref" || {
  echo "containerd image identity rejected the exact requested ExtProc digest" >&2
  exit 1
}
if containerd_image_has_exact_digest_reference "$published_extproc_runtime" 'quay.io/opendatahub/odh-praxis-extproc@sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff'; then
  echo "containerd image identity accepted a different ExtProc digest" >&2
  exit 1
fi
containerd_pod_image_id_matches_runtime "$published_extproc_runtime" 'containerd://sha256:config' || {
  echo "pod config image ID was not correlated with the inspected runtime image" >&2
  exit 1
}
containerd_pod_image_id_matches_runtime "$published_extproc_runtime" 'docker-pullable://quay.io/opendatahub/odh-praxis-extproc@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef' || {
  echo "pod digest image ID was not correlated with the inspected runtime image" >&2
  exit 1
}
if containerd_pod_image_id_matches_runtime "$published_extproc_runtime" 'docker-pullable://quay.io/opendatahub/odh-praxis-extproc@sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff'; then
  echo "pod image ID matched a digest absent from the inspected runtime image" >&2
  exit 1
fi

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
maas_image_digest='127.0.0.1:32768/maas-api@sha256:index'
maas_node_repo_digests='127.0.0.1:32768/maas-api@sha256:platform,127.0.0.1:32768/maas-api@sha256:index'
maas_pods='{"items":[{"metadata":{"name":"old","deletionTimestamp":null},"spec":{"containers":[{"name":"maas-api","image":"maas-api:old"}]},"status":{"phase":"Running","containerStatuses":[{"name":"maas-api","ready":true,"image":"maas-api:old","imageID":"127.0.0.1:32768/maas-api@sha256:old"}]}},{"metadata":{"name":"terminating-new","deletionTimestamp":"2026-10-19T00:00:00Z"},"spec":{"containers":[{"name":"maas-api","image":"maas-api:run"}]},"status":{"phase":"Running","containerStatuses":[{"name":"maas-api","ready":true,"image":"maas-api:alias","imageID":"127.0.0.1:32768/maas-api@sha256:run"}]}}]}'
if maas_api_pods_have_exact_ready_image "$maas_pods" "maas-api:run" "$maas_image_digest" "$maas_node_repo_digests"; then
  echo "MaaS API runtime check accepted only stale or terminating pods" >&2
  exit 1
fi
maas_pods=$(jq -c --arg digest "$maas_image_digest" '.items += [{"metadata":{"name":"current","deletionTimestamp":null},"spec":{"containers":[{"name":"maas-api","image":"maas-api:run"}]},"status":{"phase":"Running","containerStatuses":[{"name":"maas-api","ready":true,"image":"maas-api:alias","imageID":$digest}]}}]' <<<"$maas_pods")
maas_api_pods_have_exact_ready_image "$maas_pods" "maas-api:run" "$maas_image_digest" "$maas_node_repo_digests" || {
  echo "MaaS API runtime check rejected a requested run tag and node-verified digest alias" >&2
  exit 1
}
maas_pods=$(jq -c '.items[2].status.containerStatuses[0].imageID = "127.0.0.1:32768/maas-api@sha256:platform"' <<<"$maas_pods")
maas_api_pods_have_exact_ready_image "$maas_pods" "maas-api:run" "$maas_image_digest" "$maas_node_repo_digests" || {
  echo "MaaS API runtime check rejected a platform-manifest alias from the verified node image" >&2
  exit 1
}
if maas_api_pods_have_exact_ready_image "$(jq -c '.items[2].status.containerStatuses[0].imageID = "127.0.0.1:32768/maas-api@sha256:wrong"' <<<"$maas_pods")" "maas-api:run" "$maas_image_digest" "$maas_node_repo_digests"; then
  echo "MaaS API runtime check accepted a pod with a different runtime digest" >&2
  exit 1
fi
if maas_api_pods_have_exact_ready_image "$(jq -c '.items[2].spec.containers[0].image = "maas-api:old"' <<<"$maas_pods")" "maas-api:run" "$maas_image_digest" "$maas_node_repo_digests"; then
  echo "MaaS API runtime check accepted a pod that did not request the run tag" >&2
  exit 1
fi
if maas_api_pods_have_exact_ready_image "$maas_pods" "maas-api:run" "$maas_image_digest" "127.0.0.1:32768/maas-api@sha256:platform"; then
  echo "MaaS API runtime check accepted node digests without the expected pushed digest" >&2
  exit 1
fi

run_source=$(<"$ROOT/test/kind-env/run.sh")
published_extproc_gate=$(sed -n '/if \[\[ "$label" == extproc && "$EXTPROC_IMAGE_MODE" == published \]\]/,/^  fi/p' \
  "$ROOT/test/kind-env/run.sh")
rg -q 'git -C "\$source" status --porcelain' <<<"$published_extproc_gate" || {
  echo "published ExtProc source cleanliness check does not inspect git status" >&2
  exit 1
}
if rg -q '\[\[ -z "\$source_diff" \]\]' <<<"$published_extproc_gate"; then
  echo "published ExtProc source cleanliness check confuses the content fingerprint with dirty status" >&2
  exit 1
fi
published_extproc_load=$(sed -n '/if \[\[ "\$EXTPROC_IMAGE_MODE" == published \]\]/,/^  fi/p' \
  "$ROOT/test/kind-env/run.sh" | sed -n '/crictl pull "\$EXTPROC_IMAGE"/,$p')
rg -q 'crictl pull "\$EXTPROC_IMAGE"' <<<"$published_extproc_load" || {
  echo "published ExtProc qualification does not pull the digest directly into the Kind node" >&2
  exit 1
}
rg -q 'kind load docker-image "\$EXTPROC_IMAGE" --name "\$CLUSTER"' "$ROOT/test/kind-env/run.sh" || {
  echo "source-built ExtProc qualification no longer uses Kind image loading" >&2
  exit 1
}
authorino_gate=$(sed -n '/authorino_deployment_ready=false/,/set env deployment\/authorino/p' "$ROOT/test/kind-env/run.sh")
rg -q 'authorino_deployment_deadline=\$\(\(SECONDS \+ 180\)\)' <<<"$authorino_gate" || {
  echo "Authorino Deployment readiness wait is not bounded to 180 seconds" >&2
  exit 1
}
rg -q 'maas-api-serving-ca.*authorino-maas-api-ca' <<<"$authorino_gate" || {
  echo "Authorino readiness does not require the run-owned MaaS CA projection" >&2
  exit 1
}
rg -q 'rollout status deployment/authorino --timeout=8s' <<<"$authorino_gate" || {
  echo "Authorino readiness does not wait for its projected-CA Deployment rollout" >&2
  exit 1
}
rg -q 'authorino-readiness-timeout-events.txt' <<<"$authorino_gate" || {
  echo "Authorino readiness timeout does not preserve workload/event diagnostics" >&2
  exit 1
}
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

route_policy_manifest=$(<"$ROOT/test/kind-env/manifests/50-route-policies.yaml")
if rg -q '^kind: AuthPolicy$' <<<"$route_policy_manifest"; then
  echo "ExternalModel route fixture must inherit MaaS Gateway AuthPolicy instead of adding a conflicting AuthPolicy" >&2
  exit 1
fi
rg -q '^kind: TokenRateLimitPolicy$' <<<"$route_policy_manifest" || {
  echo "ExternalModel route fixture lost its token-rate-limit policy" >&2
  exit 1
}
translation_source=$(<"$ROOT/test/kind-env/e2e-api-translation.py")
rg -q 'remove_stale_externalmodel_auth_fixture' <<<"$translation_source" || {
  echo "translation E2E does not safely remove the exact known legacy AuthPolicy fixture" >&2
  exit 1
}
rg -q 'metadata.get\("uid"\) == DIAGNOSTIC_BYPASS_AUTH_UID' <<<"$translation_source" || {
  echo "diagnostic AuthPolicy cleanup does not verify its recorded UID" >&2
  exit 1
}
rg -q 'ConflictingAuthPolicy' <<<"$translation_source" || {
  echo "MaaS AuthPolicy readiness does not reject conflicting route policies" >&2
  exit 1
}
rg -q 'praxis_extproc_body_size_rejections_total' <<<"$translation_source" || {
  echo "request-size qualification does not attribute ExtProc's selected-upstream limit" >&2
  exit 1
}
rg -q 'selected-upstream input guard before body translation' <<<"$translation_source" || {
  echo "request-size evidence does not identify the ExtProc guard accurately" >&2
  exit 1
}
if rg -q 'rejected_by_translation_filter|Praxis AI OpenAI-to-Anthropic request-size check' <<<"$translation_source"; then
  echo "request-limit assertion still attributes ExtProc rejection to the AI translator" >&2
  exit 1
fi
rg -q 'translator_max_request_body_bytes' "$ROOT/test/kind-env/api_translation_contract.py" || {
  echo "mounted configuration parser does not distinguish request and response limits" >&2
  exit 1
}

if rg -q '\[\[ "\$extproc_branch" == main' <<<"$run_source"; then
  echo "ExtProc provenance gate incorrectly requires a branch name instead of matching source revision" >&2
  exit 1
fi
rg -q 'extproc_head.*EXPECTED_EXTPROC_SOURCE_SHA' <<<"$run_source" || {
  echo "ExtProc provenance gate does not require the configured exact source SHA" >&2
  exit 1
}

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


(
  handoff_poll=$(sed -n '/^    handoff_ready=false$/,/^    if \[\[ "$handoff_ready" != true \]\]; then$/p' \
    "$ROOT/test/kind-env/run.sh" | sed '$d')
  [[ -n "$handoff_poll" ]] || { echo 'could not extract the bootstrap Deployment handoff poll' >&2; exit 1; }
  KCTL=(mock_kubectl)
  deployment_name=payload-processing
  seq() { printf '%s\n' 1 2 3; }
  sleep() { :; }
  mock_kubectl() {
    [[ "$*" == '-n maas-system get deployment payload-processing --ignore-not-found -o json' ]] || return 1
    case "$scenario" in
      absent) return 0 ;;
      owned) printf '%s\n' '{"metadata":{"labels":{"app.kubernetes.io/managed-by":"ai-gateway-controller"}}}' ;;
      unowned) printf '%s\n' '{"metadata":{"labels":{}}}' ;;
      denied) return 1 ;;
      timeout) return 124 ;;
      partial) printf '%s\n' '{"metadata":{"labels":{"app.kubernetes.io/managed-by":"ai-gateway-controller"}}}'; return 1 ;;
      recovering)
        if [[ ! -e "$handoff_tmp/retried" ]]; then
          touch "$handoff_tmp/retried"
          return 124
        fi
        printf '%s\n' '{"metadata":{"labels":{"app.kubernetes.io/managed-by":"ai-gateway-controller"}}}'
        ;;
      *) return 1 ;;
    esac
  }
  handoff_tmp=$(mktemp -d)
  trap 'rm -rf "$handoff_tmp"' EXIT
  for scenario in denied timeout partial unowned; do
    eval "$handoff_poll"
    [[ "$handoff_ready" == false ]] || { echo "handoff must not succeed for $scenario Deployment reads" >&2; exit 1; }
  done
  for scenario in absent owned recovering; do
    eval "$handoff_poll"
    [[ "$handoff_ready" == true ]] || { echo "handoff must accept a confirmed $scenario Deployment" >&2; exit 1; }
  done
)


echo "Kind run harness contracts passed (image identity, handoff read errors, and static-weight receipt fixtures)."
# Check the actual Docker RepoDigests expression against multiple entries.
rg -q 'index\(\$d\) != null' "$ROOT/test/kind-env/run.sh"
for digests in '["other@sha256:abc","expected@sha256:def"]' '["expected@sha256:def","other@sha256:abc"]'; do
  [[ "$(jq -r --arg d 'expected@sha256:def' 'index($d) != null' <<<"$digests")" == true ]]
done

if rg -q '^kind: AuthPolicy$' "$ROOT/test/kind-env/manifests/51-route-policies-tenant-b.yaml"; then
  echo "tenant B must inherit the MaaS Gateway policy, as tenant A does" >&2
  exit 1
fi
