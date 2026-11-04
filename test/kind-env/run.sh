#!/usr/bin/env bash
set -Eeuo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
source "$ROOT/test/kind-env/lib/image_refs.sh"
source "$ROOT/test/kind-env/lib/maas_api_image_identity.sh"
source "$ROOT/test/kind-env/lib/registry_image_identity.sh"
resolve_katan_image_refs
CLUSTER=${LOCAL_ENV_CLUSTER:-external-model-two-plane}
WORKSPACE=$(cd "$ROOT/.." && pwd)
DEPS_DIR=${LOCAL_ENV_DEPS_DIR:-"$WORKSPACE/deps"}
LLM_KATAN_REPO=${LLM_KATAN_REPO:-"$DEPS_DIR/llm-katan"}
PROVIDER_RECORDER_IMAGE=${PROVIDER_RECORDER_IMAGE:-provider-recorder:issue28-kind}
MAAS_API_IMAGE=${MAAS_API_IMAGE:-maas-api:external-model-two-plane}
REGISTRY_IMAGE=${LOCAL_ENV_REGISTRY_IMAGE:-registry:2.8.3}
REGISTRY_NETWORK=${LOCAL_ENV_REGISTRY_NETWORK:-${CLUSTER}-registry-net}
REGISTRY_CONTAINER=${LOCAL_ENV_REGISTRY_CONTAINER:-${CLUSTER}-registry}
REGISTRY_PORT=
MAAS_API_RUNTIME_IMAGE=
REGISTRY_OWNER_LABEL=local-env.opendatahub.io/kind-run
MAAS_UPSTREAM_URL=${MAAS_UPSTREAM_URL:-https://github.com/opendatahub-io/models-as-a-service.git}
if [[ -n "${MAAS_CONTROLLER_REPO:-}" ]]; then
  MAAS_SOURCE_MODE=explicit-override
else
  MAAS_CONTROLLER_REPO="$DEPS_DIR/models-as-a-service"
  MAAS_SOURCE_MODE=canonical-main
fi
MAAS_API_TRANSLATION_SCHEMA_REPO=${MAAS_API_TRANSLATION_SCHEMA_REPO:-}
MAAS_API_TRANSLATION_SCHEMA_REV=${MAAS_API_TRANSLATION_SCHEMA_REV:-bce7ddd52657a2ad4c2444757761806a8290db44}
MAAS_API_TRANSLATION_SCHEMA_PATH=deployment/base/maas-controller/crd/bases/maas.opendatahub.io_maastenantconfigs.yaml
APPLY_REAL_OPENAI_FIXTURE=${APPLY_REAL_OPENAI_FIXTURE:-false}
KSERVE_REPO=${KSERVE_REPO:-"$DEPS_DIR/kserve"}
KUADRANT_OPERATOR_REPO=${KUADRANT_OPERATOR_REPO:-"$DEPS_DIR/kuadrant-operator"}
PRAXIS_EXTPROC_REPO=${PRAXIS_EXTPROC_REPO:-"$DEPS_DIR/praxis-extproc"}
PRAXIS_AI_REPO=${PRAXIS_AI_REPO:-}
EXTPROC_IMAGE=${EXTPROC_IMAGE:-praxis-extproc:dev}
ISTIOCTL=${ISTIOCTL:-/tmp/istio-1.27.3/bin/istioctl}
EVIDENCE_ROOT=${LOCAL_ENV_EVIDENCE_ROOT:-"$ROOT/evidence"}
STAMP=$(date -u +%Y%m%dT%H%M%SZ)
EVIDENCE="$EVIDENCE_ROOT/$STAMP"
mkdir -p "$EVIDENCE"
exec > >(tee "$EVIDENCE/provisioner.log") 2>&1

record_provision_error() {
  local rc=$?
  printf 'unexpected_exit=%s line=%s command=%s\n' "$rc" "$LINENO" "$BASH_COMMAND" >>"$EVIDENCE/unexpected-exit.txt"
  return "$rc"
}
trap record_provision_error ERR

KCTL=(kubectl --context "kind-$CLUSTER")
failures=0
fail() { echo "FAIL: $*" | tee -a "$EVIDENCE/failures.txt"; failures=$((failures + 1)); }
check_cmd() { command -v "$1" >/dev/null 2>&1 || fail "missing command: $1"; }
create_run_registry() {
  local network_owner container_owner node network_names expected_id runtime_id runtime_info expected_repo_digest node_repo_digest node_tag_present repository hosts_dir config_before config_after

  if docker network inspect "$REGISTRY_NETWORK" >/dev/null 2>&1; then
    network_owner=$(docker network inspect --format "{{ index .Labels \"$REGISTRY_OWNER_LABEL\" }}" "$REGISTRY_NETWORK")
    [[ "$network_owner" == "$CLUSTER" ]] || { fail "registry network exists without this run's ownership label: $REGISTRY_NETWORK"; return 1; }
  else
    docker network create --driver bridge --label "$REGISTRY_OWNER_LABEL=$CLUSTER" "$REGISTRY_NETWORK" >"$EVIDENCE/registry-network-id.txt"
  fi

  if ! docker image inspect "$REGISTRY_IMAGE" >/dev/null 2>&1; then
    timeout 300s docker pull "$REGISTRY_IMAGE"
  fi
  docker image inspect "$REGISTRY_IMAGE" --format '{{.Id}}' >"$EVIDENCE/registry-image-id.txt"
  if docker container inspect "$REGISTRY_CONTAINER" >/dev/null 2>&1; then
    container_owner=$(docker container inspect --format "{{ index .Config.Labels \"$REGISTRY_OWNER_LABEL\" }}" "$REGISTRY_CONTAINER")
    [[ "$container_owner" == "$CLUSTER" ]] || { fail "registry container exists without this run's ownership label: $REGISTRY_CONTAINER"; return 1; }
    docker start "$REGISTRY_CONTAINER" >/dev/null
  else
    docker run --detach --restart unless-stopped --name "$REGISTRY_CONTAINER" \
      --label "$REGISTRY_OWNER_LABEL=$CLUSTER" \
      --network "$REGISTRY_NETWORK" --network-alias "$REGISTRY_CONTAINER" \
      --publish 127.0.0.1::5000 "$REGISTRY_IMAGE" >"$EVIDENCE/registry-container-id.txt"
  fi
  REGISTRY_PORT=$(docker container inspect --format '{{(index (index .NetworkSettings.Ports "5000/tcp") 0).HostPort}}' "$REGISTRY_CONTAINER")
  [[ "$REGISTRY_PORT" =~ ^[0-9]+$ ]] || { fail "could not resolve the run-owned registry host port"; return 1; }
  MAAS_API_RUNTIME_IMAGE="127.0.0.1:${REGISTRY_PORT}/maas-api:${STAMP}"
  printf 'network=%s\ncontainer=%s\nport=%s\nregistry_image=%s\nregistry_image_id=%s\nmaas_api_runtime_image=%s\n' \
    "$REGISTRY_NETWORK" "$REGISTRY_CONTAINER" "$REGISTRY_PORT" "$REGISTRY_IMAGE" \
    "$(<"$EVIDENCE/registry-image-id.txt")" "$MAAS_API_RUNTIME_IMAGE" >"$EVIDENCE/local-registry.txt"

  docker tag "$MAAS_API_IMAGE" "$MAAS_API_RUNTIME_IMAGE"
  docker image inspect "$MAAS_API_IMAGE" --format '{{.Id}}' >"$EVIDENCE/maas-api-image-id.txt"
  docker image inspect "$MAAS_API_RUNTIME_IMAGE" --format '{{.Id}}' >"$EVIDENCE/maas-api-registry-tag-image-id.txt"
  cmp -s "$EVIDENCE/maas-api-image-id.txt" "$EVIDENCE/maas-api-registry-tag-image-id.txt" || {
    fail "local registry tag does not identify the source-built MaaS API image"; return 1;
  }
  docker push "$MAAS_API_RUNTIME_IMAGE" >"$EVIDENCE/maas-api-registry-push.txt" 2>&1

  kind get clusters | rg -qx "$CLUSTER" || KIND_EXPERIMENTAL_DOCKER_NETWORK="$REGISTRY_NETWORK" kind create cluster --name "$CLUSTER"
  node="${CLUSTER}-control-plane"
  network_names=$(docker inspect --format '{{range $name, $value := .NetworkSettings.Networks}}{{$name}} {{end}}' "$node")
  if [[ " $network_names " != *" $REGISTRY_NETWORK "* ]]; then
    docker network connect --gw-priority=-1 "$REGISTRY_NETWORK" "$node"
  fi

  hosts_dir="/etc/containerd/certs.d/127.0.0.1:${REGISTRY_PORT}"
  printf '[host."http://%s:5000"]\n  capabilities = ["pull", "resolve"]\n' "$REGISTRY_CONTAINER" >"$EVIDENCE/registry-hosts.toml"
  while IFS= read -r node; do
    [[ -n "$node" ]] || continue
    docker exec "$node" mkdir -p "$hosts_dir"
    docker exec -i "$node" sh -c "cat > '$hosts_dir/hosts.toml'" <"$EVIDENCE/registry-hosts.toml"
    config_before="$EVIDENCE/${node}-containerd-config.before.toml"
    config_after="$EVIDENCE/${node}-containerd-config.after.toml"
    docker cp "$node:/etc/containerd/config.toml" "$config_before" >/dev/null
    python3 - "$config_before" "$config_after" <<'PY'
import pathlib
import sys

before, after = map(pathlib.Path, sys.argv[1:])
config = before.read_text()
setting = '  config_path = "/etc/containerd/certs.d"'
if setting not in config:
    section = '[plugins."io.containerd.grpc.v1.cri".registry]'
    if section in config:
        config = config.replace(section, section + "\n" + setting, 1)
    else:
        config = config.rstrip() + "\n\n" + section + "\n" + setting + "\n"
after.write_text(config)
PY
    if ! cmp -s "$config_before" "$config_after"; then
      docker cp "$config_after" "$node:/etc/containerd/config.toml" >/dev/null
      docker exec "$node" systemctl restart containerd
      printf '%s\n' "$node restarted after enabling the run-owned containerd registry host configuration" >>"$EVIDENCE/containerd-registry-config.txt"
    else
      printf '%s\n' "$node already reads registry host configuration" >>"$EVIDENCE/containerd-registry-config.txt"
    fi
    diff -u "$config_before" "$config_after" >"$EVIDENCE/${node}-containerd-config.patch" || true
  done < <(kind get nodes --name "$CLUSTER")
  for _ in $(seq 1 60); do
    if "${KCTL[@]}" wait --for=condition=Ready node "${CLUSTER}-control-plane" --timeout=5s >/dev/null 2>&1; then
      break
    fi
    sleep 2
  done
  "${KCTL[@]}" wait --for=condition=Ready node "${CLUSTER}-control-plane" --timeout=5s >/dev/null
  docker exec "${CLUSTER}-control-plane" crictl pull "$MAAS_API_RUNTIME_IMAGE" >"$EVIDENCE/maas-api-registry-node-pull.txt" 2>&1
  runtime_info=$(docker exec "${CLUSTER}-control-plane" crictl inspecti "$MAAS_API_RUNTIME_IMAGE")
  runtime_id=$(jq -r '.status.id' <<<"$runtime_info")
  expected_id=$(<"$EVIDENCE/maas-api-image-id.txt")
  repository=${MAAS_API_RUNTIME_IMAGE%:*}
  expected_repo_digest=$(docker image inspect "$MAAS_API_RUNTIME_IMAGE" --format '{{json .RepoDigests}}' \
    | jq -r --arg prefix "$repository@" '[.[]? | select(startswith($prefix))][0] // ""')
  node_repo_digests=$(jq -r '.status.repoDigests // [] | join(",")' <<<"$runtime_info")
  node_has_expected_repo_digest=false
  if containerd_image_matches_pushed_ref "$runtime_info" "$expected_repo_digest" "$MAAS_API_RUNTIME_IMAGE"; then
    node_has_expected_repo_digest=true
  fi
  node_tag_present=$(jq -r --arg ref "$MAAS_API_RUNTIME_IMAGE" '(.status.repoTags // []) | index($ref) != null' <<<"$runtime_info")
  printf 'expected_local_image_id=%s\nnode_containerd_image_id=%s\nexpected_registry_repo_digest=%s\nnode_registry_repo_digests=%s\nnode_has_expected_repo_digest=%s\nnode_tag_present=%s\n' \
    "$expected_id" "$runtime_id" "$expected_repo_digest" "$node_repo_digests" "$node_has_expected_repo_digest" "$node_tag_present" \
    >"$EVIDENCE/maas-api-registry-node-image-identity.txt"
  # Containerd may report both the platform manifest and its parent image
  # index in repoDigests, in either order. Match the exact digest pushed from
  # the source-built image by membership rather than treating the first entry
  # as authoritative. The CRI image ID is the platform config digest, while
  # Docker's local image ID can identify the index.
  [[ -n "$expected_repo_digest" && "$node_has_expected_repo_digest" == true && "$node_tag_present" == true ]] || {
    fail "Kind node is missing the source-built MaaS API registry digest or tag"; return 1;
  }
}
cleanup_run_registry() {
  local owner
  if ! command -v docker >/dev/null 2>&1; then
    echo 'registry_cleanup_complete=false reason=docker_command_unavailable' >"$EVIDENCE/registry-cleanup.txt"
    fail "docker is unavailable for run-owned registry cleanup"
    return
  fi
  if docker container inspect "$REGISTRY_CONTAINER" >/dev/null 2>&1; then
    owner=$(docker container inspect --format "{{ index .Config.Labels \"$REGISTRY_OWNER_LABEL\" }}" "$REGISTRY_CONTAINER")
    if [[ "$owner" == "$CLUSTER" ]]; then
      docker rm -f "$REGISTRY_CONTAINER" >/dev/null || fail "could not remove run-owned registry container"
      echo 'registry_container_removed=true' >>"$EVIDENCE/registry-cleanup.txt"
    else
      echo 'registry_container_removed=false reason=ownership_label_mismatch' >>"$EVIDENCE/registry-cleanup.txt"
      fail "refusing to remove registry container without matching ownership label"
    fi
  else
    echo 'registry_container_removed=true reason=already_absent' >>"$EVIDENCE/registry-cleanup.txt"
  fi
  if docker network inspect "$REGISTRY_NETWORK" >/dev/null 2>&1; then
    owner=$(docker network inspect --format "{{ index .Labels \"$REGISTRY_OWNER_LABEL\" }}" "$REGISTRY_NETWORK")
    if [[ "$owner" == "$CLUSTER" ]]; then
      docker network rm "$REGISTRY_NETWORK" >/dev/null || fail "could not remove run-owned registry network"
      echo 'registry_network_removed=true' >>"$EVIDENCE/registry-cleanup.txt"
    else
      echo 'registry_network_removed=false reason=ownership_label_mismatch' >>"$EVIDENCE/registry-cleanup.txt"
      fail "refusing to remove registry network without matching ownership label"
    fi
  else
    echo 'registry_network_removed=true reason=already_absent' >>"$EVIDENCE/registry-cleanup.txt"
  fi
}
apply_maas_api_translation_schema() {
  local base_schema="$EVIDENCE/maas-main-maastenantconfigs.yaml"
  local feature_schema="$EVIDENCE/maas-api-translation-maastenantconfigs.yaml"
  local base_normalized="$EVIDENCE/maas-main-maastenantconfigs.normalized.json"
  local feature_normalized="$EVIDENCE/maas-api-translation-maastenantconfigs.normalized.json"
  local feature_sha feature_branch

  if [[ -z "$MAAS_API_TRANSLATION_SCHEMA_REPO" ]]; then
    printf 'schema_overlay=not_applied reason=MAAS_API_TRANSLATION_SCHEMA_REPO_not_set\n' >"$EVIDENCE/maas-api-translation-schema-source.txt"
    return 0
  fi
  git -C "$MAAS_API_TRANSLATION_SCHEMA_REPO" show "$MAAS_API_TRANSLATION_SCHEMA_REV:$MAAS_API_TRANSLATION_SCHEMA_PATH" >"$feature_schema"
  git -C "$MAAS_CONTROLLER_REPO" show "HEAD:$MAAS_API_TRANSLATION_SCHEMA_PATH" >"$base_schema"
  yq eval -o=json "$base_schema" | jq -S . >"$base_normalized"
  yq eval -o=json "$feature_schema" | jq -S 'del(.spec.versions[] | select(.name == "v1alpha1") | .schema.openAPIV3Schema.properties.spec.properties.apiTranslation)' >"$feature_normalized"
  cmp -s "$base_normalized" "$feature_normalized" || {
    fail "MaaS schema source differs from main beyond spec.apiTranslation"; return 1;
  }
  yq eval -o=json "$feature_schema" | jq -e '
    .spec.versions[] | select(.name == "v1alpha1") |
    .schema.openAPIV3Schema.properties.spec.properties.apiTranslation.properties.enabled.type == "boolean"' >/dev/null || {
      fail "MaaS apiTranslation schema does not expose enabled as a boolean"; return 1;
    }
  feature_sha=$(sha256sum "$feature_schema" | awk '{print $1}')
  feature_branch=$(git -C "$MAAS_API_TRANSLATION_SCHEMA_REPO" branch --show-current)
  printf 'controller_source=%s\ncontroller_commit=%s\nschema_source=%s\nschema_branch=%s\nschema_commit=%s\nschema_file=%s\nschema_file_sha256=%s\n' \
    "$MAAS_CONTROLLER_REPO" "$(git -C "$MAAS_CONTROLLER_REPO" rev-parse HEAD)" \
    "$MAAS_API_TRANSLATION_SCHEMA_REPO" "$feature_branch" "$MAAS_API_TRANSLATION_SCHEMA_REV" \
    "$MAAS_API_TRANSLATION_SCHEMA_PATH" "$feature_sha" >"$EVIDENCE/maas-api-translation-schema-source.txt"
  diff -u "$base_schema" "$feature_schema" >"$EVIDENCE/maas-api-translation-schema.diff" || true
  "${KCTL[@]}" apply -f "$feature_schema"
  "${KCTL[@]}" get crd maastenantconfigs.maas.opendatahub.io -o yaml >"$EVIDENCE/maastenantconfigs-crd-after-schema-overlay.yaml"
  "${KCTL[@]}" get crd maastenantconfigs.maas.opendatahub.io -o json \
    | jq -e '.spec.versions[] | select(.name == "v1alpha1") | .schema.openAPIV3Schema.properties.spec.properties.apiTranslation.properties.enabled.type == "boolean"' >/dev/null
}
wait_for_praxis_handoff() {
  local namespace=$1 status
  for _ in $(seq 1 90); do
    status=$("${KCTL[@]}" -n "$namespace" get maastenantconfig default-tenant -o jsonpath='{.metadata.annotations.maas\.opendatahub\.io/payload-processing-status}' 2>/dev/null || true)
    if [[ "$status" == steady ]]; then
      printf 'tenant=%s status=steady\n' "$namespace" >>"$EVIDENCE/maas-payload-handoff.txt"
      return 0
    fi
    sleep 2
  done
  "${KCTL[@]}" -n "$namespace" get maastenantconfig default-tenant -o json >"$EVIDENCE/maas-payload-handoff-${namespace}.json" 2>&1 || true
  fail "MaaS/Praxis handoff did not reach steady for $namespace"
  return 1
}
check_repo() { local name=$1 path=${!1:-}; [[ -n "$path" ]] && git -C "$path" rev-parse --git-dir >/dev/null 2>&1 || fail "$name checkout missing: ${path:-unset}"; }
dirty_hash() {
  local source=$1 file
  {
    git -C "$source" diff --no-ext-diff
    while IFS= read -r -d '' file; do
      local path="$source/$file"
      printf 'UNTRACKED %s\n' "$file"
      if [[ -f "$path" ]]; then
        sha256sum "$path"
      elif [[ -d "$path" && -d "$path/.git" ]]; then
        printf 'NESTED_REPOSITORY_HEAD %s\n' "$(git -C "$path" rev-parse HEAD 2>/dev/null || true)"
        git -C "$path" diff --no-ext-diff | sha256sum
      else
        printf 'UNTRACKED_DIRECTORY %s\n' "$file"
      fi
    done < <(git -C "$source" ls-files --others --exclude-standard -z | sort -z)
  } | sha256sum | awk '{print $1}'
}

build_extproc_with_local_ai() {
  local extproc_source=$1 ai_source=$2 image=$3 context="$EVIDENCE/extproc-build-context"
  python3 - "$extproc_source" "$ai_source" "$context" <<'PY'
import pathlib
import shutil
import sys

extproc_source, ai_source, context = map(pathlib.Path, sys.argv[1:])
if context.exists():
    shutil.rmtree(context)
context.mkdir(parents=True)

def copy_source(source, destination):
    ignored = shutil.ignore_patterns(".git", "target", "evidence", "node_modules")
    shutil.copytree(source, destination, dirs_exist_ok=True, ignore=ignored)

copy_source(extproc_source, context / "praxis-extproc")
copy_source(ai_source, context / "praxis-ai")

cargo_toml = context / "praxis-extproc" / "Cargo.toml"
cargo = cargo_toml.read_text()
replacements = {
    'praxis-ai-apis = { git = "https://github.com/praxis-proxy/ai.git", tag = "v0.7.0", default-features = false, optional = true }':
        'praxis-ai-apis = { path = "../praxis-ai/apis", default-features = false, optional = true }',
    'praxis-ai-filters = { git = "https://github.com/praxis-proxy/ai.git", tag = "v0.7.0", default-features = false }':
        'praxis-ai-filters = { path = "../praxis-ai/filters", default-features = false }',
}
for old, new in replacements.items():
    if cargo.count(old) != 1:
        raise SystemExit(f"expected one ExtProc dependency pin to replace: {old.split()[0]}")
    cargo = cargo.replace(old, new)
cargo_toml.write_text(cargo)

containerfile = context / "praxis-extproc" / "Containerfile"
recipe = containerfile.read_text()
copy_replacements = {
    "COPY Cargo.toml Cargo.lock ./": "COPY praxis-extproc/Cargo.toml praxis-extproc/Cargo.lock praxis-extproc/",
    "COPY proto proto": "COPY praxis-extproc/proto praxis-extproc/proto",
    "COPY src src": "COPY praxis-extproc/src praxis-extproc/src",
    "COPY xtask xtask": "COPY praxis-extproc/xtask praxis-extproc/xtask",
}
for old, new in copy_replacements.items():
    if recipe.count(old) != 1:
        raise SystemExit(f"expected one ExtProc Containerfile source copy: {old}")
    recipe = recipe.replace(old, new)
anchor = "COPY praxis-extproc/xtask praxis-extproc/xtask\n"
if recipe.count(anchor) != 1:
    raise SystemExit("ExtProc Containerfile source-copy anchor changed")
recipe = recipe.replace(anchor, anchor + "COPY praxis-ai praxis-ai\nWORKDIR /src/praxis-extproc\n")
containerfile.write_text(recipe)
PY

  cargo update --manifest-path "$context/praxis-extproc/Cargo.toml" --offline -p praxis-ai-filters
  cargo update --manifest-path "$context/praxis-extproc/Cargo.toml" --offline -p praxis-ai-apis
  diff -u "$extproc_source/Containerfile" "$context/praxis-extproc/Containerfile" >"$EVIDENCE/extproc-local-ai-containerfile.patch" || true
  diff -u "$extproc_source/Cargo.toml" "$context/praxis-extproc/Cargo.toml" >"$EVIDENCE/extproc-local-ai-cargo.patch" || true
  sha256sum "$context/praxis-extproc/Cargo.lock" >"$EVIDENCE/extproc-local-ai-cargo-lock.sha256"
  timeout 1200s docker build --platform linux/amd64 -t "$image" -f "$context/praxis-extproc/Containerfile" "$context"
}

echo "evidence=$EVIDENCE"
echo "cluster=$CLUSTER"
cat >"$EVIDENCE/namespace-contract.txt" <<EOF
tenant_namespace=models-as-a-service
transition_tenant_namespace=ai-tenant-transition
mock_backend_namespace=maas-system
gateway_namespace=maas-system
aitenant_namespace=ai-tenants
controller_namespace=opendatahub
EOF

# Teardown is intentionally independent of build/provisioning prerequisites.
# A machine with a missing tool (for example istioctl) must still be able to
# remove the exact run-owned cluster and record cleanup evidence.
if [[ "${1:---preflight}" == "--destroy" ]]; then
  prior_evidence="$EVIDENCE"
  if [[ -s "$EVIDENCE_ROOT/.active-run" ]]; then
    prior_evidence=$(<"$EVIDENCE_ROOT/.active-run")
  fi
  if ! command -v kind >/dev/null 2>&1; then
    fail "missing command: kind"
  elif kind get clusters 2>/dev/null | rg -qx "$CLUSTER"; then
    timeout 180s kind delete cluster --name "$CLUSTER" >"$EVIDENCE/destroy.log" 2>&1 || fail "run-owned cluster deletion failed"
  else
    echo "cluster_already_absent=true" >"$EVIDENCE/destroy.log"
  fi
  cleanup_run_registry
  {
    echo "cluster=kind-$CLUSTER"
    echo "run_owned_prior_evidence=$prior_evidence"
    echo "run_owned_ca_artifacts=$prior_evidence/maas-api-ca.crt,$prior_evidence/maas-api-serving.crt"
    echo "cluster_certificate_secrets_removed=true"
    echo "authorino_ca_configmap_removed=true"
    if kind get clusters 2>/dev/null | rg -qx "$CLUSTER"; then
      echo "cluster_removed=false"
      fail "run-owned Kind cluster still exists after teardown"
    else
      echo "cluster_removed=true"
    fi
    echo "unrelated_kind_clusters_preserved=$(kind get clusters 2>/dev/null | tr '\n' ' ' || true)"
    cat "$EVIDENCE/registry-cleanup.txt"
  } >"$EVIDENCE/cleanup-inventory.txt"
  echo "destroyed kind-$CLUSTER"
  (( failures == 0 )) || exit 2
  exit 0
fi

for cmd in docker kind kubectl helm kustomize go git openssl python3 yq envsubst; do check_cmd "$cmd"; done
check_cmd "$ISTIOCTL"
check_repo KSERVE_REPO "$KSERVE_REPO"
if [[ -n "$MAAS_API_TRANSLATION_SCHEMA_REPO" ]]; then
  check_repo MAAS_API_TRANSLATION_SCHEMA_REPO "$MAAS_API_TRANSLATION_SCHEMA_REPO"
fi
mkdir -p "$EVIDENCE_ROOT/.image-inputs"

if [[ "$MAAS_SOURCE_MODE" == canonical-main ]]; then
  if [[ ! -d "$MAAS_CONTROLLER_REPO/.git" ]]; then
    timeout 600s git clone "$MAAS_UPSTREAM_URL" "$MAAS_CONTROLLER_REPO" || fail "canonical MaaS clone failed"
  else
    maas_remote=$(git -C "$MAAS_CONTROLLER_REPO" remote get-url origin 2>/dev/null || true)
    [[ "$maas_remote" == "$MAAS_UPSTREAM_URL" ]] || fail "default MaaS checkout has unexpected origin: ${maas_remote:-missing}"
    if [[ -n "$(git -C "$MAAS_CONTROLLER_REPO" status --porcelain)" ]]; then
      fail "default canonical MaaS checkout is dirty; use MAAS_CONTROLLER_REPO for an explicit checkout"
    else
      timeout 600s git -C "$MAAS_CONTROLLER_REPO" fetch --prune origin main || fail "canonical MaaS fetch failed"
      git -C "$MAAS_CONTROLLER_REPO" checkout --detach origin/main || fail "canonical MaaS checkout failed"
    fi
  fi
fi
if [[ ! -d "$MAAS_CONTROLLER_REPO/deployment/base/maas-controller/default" ]]; then
  fail "selected MaaS source lacks deployment/base/maas-controller/default"
fi
if [[ ! -d "$MAAS_CONTROLLER_REPO/maas-api/deploy/overlays/xks" ]]; then
  fail "selected MaaS source lacks maas-api/deploy/overlays/xks"
fi
if ! rg -q 'aitenants\.maas\.opendatahub\.io' "$MAAS_CONTROLLER_REPO"; then
  fail "selected MaaS source lacks the canonical AITenant CRD"
fi
{
  git -C "$MAAS_CONTROLLER_REPO" remote -v
  printf 'source_mode=%s\n' "$MAAS_SOURCE_MODE"
  printf 'branch=%s\n' "$(git -C "$MAAS_CONTROLLER_REPO" symbolic-ref --short -q HEAD || echo DETACHED)"
  printf 'sha=%s\n' "$(git -C "$MAAS_CONTROLLER_REPO" rev-parse HEAD)"
} >"$EVIDENCE/maas-source.txt"
dirty_hash "$MAAS_CONTROLLER_REPO" >"$EVIDENCE/maas-controller.diff.sha256"
printf '%s\n' "$MAAS_SOURCE_MODE" >"$EVIDENCE/maas-source-mode.txt"
if [[ "$APPLY_REAL_OPENAI_FIXTURE" == true && ",${PRAXIS_EXTRA_KNOWN_CLUSTERS:-}," != *,provider-openai,* ]]; then
  fail "real OpenAI fixture requires PRAXIS_EXTRA_KNOWN_CLUSTERS=provider-openai"
fi

if docker info --format '{{.Architecture}} {{.NCPU}} {{.MemTotal}}' >"$EVIDENCE/docker.txt" 2>&1; then
  read -r arch cpus memory <"$EVIDENCE/docker.txt" || true
  echo "docker_arch=$arch docker_cpus=$cpus docker_memory_bytes=$memory"
else
  fail "Docker daemon is unavailable"
fi

df -P "$ROOT" >"$EVIDENCE/disk.txt" 2>&1 || fail "disk check failed"
free -b >"$EVIDENCE/memory.txt" 2>&1 || fail "memory check failed"
if [[ "${BUILD_KATAN:-false}" == true ]]; then check_repo LLM_KATAN_REPO; fi
check_repo MAAS_CONTROLLER_REPO
check_repo KUADRANT_OPERATOR_REPO
check_repo PRAXIS_EXTPROC_REPO
if [[ -n "$PRAXIS_AI_REPO" ]]; then check_repo PRAXIS_AI_REPO; fi
if [[ -n "${EXPECTED_EXTPROC_MAIN_SHA:-}" ]]; then
  extproc_branch=$(git -C "$PRAXIS_EXTPROC_REPO" branch --show-current)
  extproc_head=$(git -C "$PRAXIS_EXTPROC_REPO" rev-parse HEAD)
  [[ "$extproc_branch" == main && "$extproc_head" == "$EXPECTED_EXTPROC_MAIN_SHA" ]] || fail "ExtProc source must be clean main at $EXPECTED_EXTPROC_MAIN_SHA; got $extproc_branch@$extproc_head"
  [[ -z "$(git -C "$PRAXIS_EXTPROC_REPO" status --porcelain)" ]] || fail "ExtProc main checkout is dirty"
fi

if rg -q 'For\(.*ExternalModel|ExternalModel.*Reconciler|SetupWithManager' "$ROOT/cmd" "$ROOT/pkg" 2>/dev/null; then
  echo "reconciler_source=present"
else
  fail "real ExternalModel/ExternalProvider reconciler is absent from this checkout"
fi
if find "$ROOT/config" -type f -name '*.yaml' -print0 | xargs -0 rg -q 'kind: CustomResourceDefinition' 2>/dev/null; then
  echo "crd_manifests=present"
else
  fail "ExternalModel/ExternalProvider CRD manifests are absent from this checkout"
fi

git -C "$ROOT" rev-parse HEAD >"$EVIDENCE/controller.sha"
git -C "$ROOT" diff --no-ext-diff >"$EVIDENCE/controller.diff" || true
dirty_hash "$ROOT" >"$EVIDENCE/controller.diff.sha256"
git -C "$ROOT" status --short >"$EVIDENCE/controller.status"

for pair in \
  "controller|$ROOT|${AI_CONTROLLER_IMAGE:-ai-gateway-controller:external-model-two-plane}|Dockerfile" \
  "katan|$LLM_KATAN_REPO|$KATAN_EFFECTIVE_IMAGE|Containerfile" \
  "extproc|$PRAXIS_EXTPROC_REPO|$EXTPROC_IMAGE|Containerfile" \
  "maas-api|$MAAS_CONTROLLER_REPO/maas-api|${MAAS_API_IMAGE:-maas-api:external-model-two-plane}|Dockerfile" \
  "maas-controller|$MAAS_CONTROLLER_REPO|${MAAS_CONTROLLER_IMAGE:-maas-controller:external-model-two-plane}|maas-controller/Dockerfile"; do
  IFS='|' read -r label source image dockerfile <<<"$pair"
  if [[ "$label" == katan && "${BUILD_KATAN:-false}" != true ]]; then
    echo "published_image=$image source_commit=a5a47568ac6daf1d4bd8b356e7b350cce9ceca2a"
    continue
  fi
  source_sha=$(git -C "$source" rev-parse HEAD)
  source_diff=$(dirty_hash "$source")
  dockerfile_sha=$(sha256sum "$source/$dockerfile" | awk '{print $1}')
  printf '%s\n%s\n%s\n' "$source_sha" "$source_diff" "$dockerfile_sha" >"$EVIDENCE/${label}.inputs"
  printf '%s\n' "$source_sha" >"$EVIDENCE/${label}.sha"
  printf '%s\n' "$source_diff" >"$EVIDENCE/${label}.diff.sha256"
  if [[ "$label" == extproc ]]; then
    printf 'branch=%s\ncommit=%s\n' "$(git -C "$source" branch --show-current)" "$source_sha" >"$EVIDENCE/extproc-source.txt"
    if [[ -n "$PRAXIS_AI_REPO" ]]; then
      ai_sha=$(git -C "$PRAXIS_AI_REPO" rev-parse HEAD)
      ai_diff=$(dirty_hash "$PRAXIS_AI_REPO")
      printf 'branch=%s\ncommit=%s\nworktree_fingerprint=%s\n' \
        "$(git -C "$PRAXIS_AI_REPO" branch --show-current)" "$ai_sha" "$ai_diff" >"$EVIDENCE/praxis-ai-source.txt"
      printf 'praxis-ai=%s\n%s\n' "$ai_sha" "$ai_diff" >>"$EVIDENCE/${label}.inputs"
    fi
  fi
  cache="$EVIDENCE_ROOT/.image-inputs/$label"
  rebuild=true
  if docker image inspect "$image" >/dev/null 2>&1 && [[ -f "$cache" ]] && cmp -s "$EVIDENCE/${label}.inputs" "$cache"; then
    rebuild=false
  fi
  if [[ "$rebuild" == true ]]; then
    case "$label" in
      controller) timeout 900s docker build --platform linux/amd64 -t "$image" -f "$source/$dockerfile" "$source" ;;
      katan) timeout 900s docker build --platform linux/amd64 -t "$image" -f "$source/$dockerfile" "$source" ;;
      extproc)
        if [[ -n "$PRAXIS_AI_REPO" ]]; then
          build_extproc_with_local_ai "$source" "$PRAXIS_AI_REPO" "$image"
        else
          timeout 1200s docker build --platform linux/amd64 -t "$image" -f "$source/$dockerfile" "$source"
        fi
        ;;
      maas-api) timeout 1200s docker build --platform linux/amd64 -t "$image" -f "$source/$dockerfile" "$source" ;;
      maas-controller) timeout 1200s docker build --platform linux/amd64 -t "$image" -f "$source/$dockerfile" "$source" ;;
    esac
    cp "$EVIDENCE/${label}.inputs" "$cache"
    echo "built_image=$image reason=source-inputs-changed-or-image-missing"
  else
    echo "reused_image=$image reason=source-inputs-unchanged"
  fi
  if [[ "$label" == controller ]]; then
    docker image inspect "$image" --format '{{.Id}}' >"$EVIDENCE/controller-image-id.txt"
    printf '%s\n' "$image" >"$EVIDENCE/controller-image-reference.txt"
  fi
done

git -C "$KSERVE_REPO" rev-parse HEAD >"$EVIDENCE/kserve.sha"
git -C "$KSERVE_REPO" diff --no-ext-diff | sha256sum >"$EVIDENCE/kserve.diff.sha256"

# Build only the HTTPS test backend here. Praxis ExtProc remains selected by
# EXTPROC_IMAGE and is never rebuilt by this fixture setup.
mkdir -p "$EVIDENCE/provider-recorder-build"
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -o "$EVIDENCE/provider-recorder-build/provider-recorder" "$ROOT/test/kind-env/provider-recorder/main.go"
cp "$ROOT/test/kind-env/provider-recorder/Dockerfile" "$EVIDENCE/provider-recorder-build/Dockerfile"
sha256sum "$ROOT/test/kind-env/provider-recorder/main.go" "$ROOT/test/kind-env/provider-recorder/Dockerfile" >"$EVIDENCE/provider-recorder-source.sha256"
timeout 600s docker build --platform linux/amd64 -t "$PROVIDER_RECORDER_IMAGE" -f "$EVIDENCE/provider-recorder-build/Dockerfile" "$EVIDENCE/provider-recorder-build"
docker image inspect "$PROVIDER_RECORDER_IMAGE" --format '{{.Id}}' >"$EVIDENCE/provider-recorder-image-id.txt"

if (( failures )); then
  printf '{\n  "status":"BLOCKED",\n  "failures":%d,\n  "cluster":"kind-%s",\n  "evidence":"%s"\n}\n' "$failures" "$CLUSTER" "$EVIDENCE" >"$EVIDENCE/result.json"
  exit 2
fi

if [[ "${1:---preflight}" == "--provision" ]]; then
  printf '%s\n' "$EVIDENCE" >"$EVIDENCE_ROOT/.active-run"
  create_run_registry
  "${KCTL[@]}" cluster-info >"$EVIDENCE/cluster-info.txt"
  # Stop prior run-owned controller pods before replacing their node image
  # tags; containerd retains images referenced by live containers. A running
  # AIGC pod can keep the previous image ID attached to a reused local tag.
  "${KCTL[@]}" -n maas-system scale deployment/maas-api deployment/maas-controller --replicas=0 >/dev/null 2>&1 || true
  aigc_deployment=$("${KCTL[@]}" -n opendatahub get deployment ai-gateway-controller -o json 2>/dev/null || true)
  if [[ -n "$aigc_deployment" ]]; then
    aigc_selector=$(jq -r '.spec.selector.matchLabels | to_entries | map(.key + "=" + .value) | join(",")' <<<"$aigc_deployment")
    if [[ -z "$aigc_selector" ]]; then
      fail "ai-gateway-controller Deployment has no selector before image replacement"
      exit 2
    fi
    "${KCTL[@]}" -n opendatahub scale deployment/ai-gateway-controller --replicas=0 >/dev/null
    aigc_pods="present"
    for _ in $(seq 1 60); do
      aigc_pods=$("${KCTL[@]}" -n opendatahub get pods -l "$aigc_selector" -o name 2>/dev/null || true)
      [[ -z "$aigc_pods" ]] && break
      sleep 1
    done
    if [[ -n "$aigc_pods" ]]; then
      fail "prior ai-gateway-controller pods did not stop before image replacement"
      exit 2
    fi
  fi
  for selector in 'control-plane=maas-controller'; do
    for _ in $(seq 1 60); do
      [[ -z $("${KCTL[@]}" -n maas-system get pods -l "$selector" -o name 2>/dev/null || true) ]] && break
      sleep 1
    done
  done
  # kind load does not reliably replace an existing docker.io/library tag in
  # the node container. Remove only these run-owned image tags first so the
  # loaded image ID always matches the source-input cache record above.
  for image in \
    "${AI_CONTROLLER_IMAGE:-ai-gateway-controller:external-model-two-plane}" \
    "$KATAN_KIND_IMAGE" \
    "$PROVIDER_RECORDER_IMAGE" \
    "$EXTPROC_IMAGE" \
    "${MAAS_API_IMAGE:-maas-api:external-model-two-plane}" \
    "${MAAS_CONTROLLER_IMAGE:-maas-controller:external-model-two-plane}"; do
    docker exec "${CLUSTER}-control-plane" crictl rmi "docker.io/library/$image" >/dev/null 2>&1 || true
  done
  kind load docker-image "${AI_CONTROLLER_IMAGE:-ai-gateway-controller:external-model-two-plane}" --name "$CLUSTER"
  if [[ "${BUILD_KATAN:-false}" != true ]]; then
    # The published reference is a multi-architecture index. Pull only the
    # node platform before loading Kind; importing the index with all
    # platforms can fail when a registry omits one referenced child blob.
    docker pull --platform linux/amd64 "$KATAN_PULL_IMAGE"
    docker tag "$KATAN_PULL_IMAGE" "$KATAN_KIND_IMAGE"
  fi
  kind load docker-image "$KATAN_KIND_IMAGE" --name "$CLUSTER"
  kind load docker-image "$PROVIDER_RECORDER_IMAGE" --name "$CLUSTER"
  kind load docker-image "$EXTPROC_IMAGE" --name "$CLUSTER"
  kind load docker-image "${MAAS_API_IMAGE:-maas-api:external-model-two-plane}" --name "$CLUSTER"
  kind load docker-image "${MAAS_CONTROLLER_IMAGE:-maas-controller:external-model-two-plane}" --name "$CLUSTER"
  timeout 120s "${KCTL[@]}" apply -f https://github.com/kubernetes-sigs/gateway-api/releases/download/v1.5.1/standard-install.yaml
  timeout 600s helm upgrade --install cert-manager jetstack/cert-manager --namespace cert-manager --create-namespace --version v1.15.3 --set crds.enabled=true --kube-context "kind-$CLUSTER" --wait --timeout 300s
  timeout 600s helm upgrade --install kuadrant-operator kuadrant/kuadrant-operator --namespace kuadrant-system --create-namespace --version 1.3.1 --kube-context "kind-$CLUSTER" --wait --timeout 300s
  timeout 600s "$ISTIOCTL" install --context "kind-$CLUSTER" --set profile=minimal --set components.ingressGateways[0].name=istio-ingressgateway --set components.ingressGateways[0].enabled=true --set values.gateways.istio-ingressgateway.autoscaleEnabled=false --set values.gateways.istio-ingressgateway.replicaCount=1 -y
  # Kuadrant is installed before Istio so its initial provider discovery can
  # miss the Gateway API implementation. Restart its run-owned controller
  # after Istio is ready, matching the documented recovery path.
  "${KCTL[@]}" -n kuadrant-system rollout restart deployment/kuadrant-operator-controller-manager
  "${KCTL[@]}" -n kuadrant-system rollout status deployment/kuadrant-operator-controller-manager --timeout=120s
  "${KCTL[@]}" create namespace maas-system --dry-run=client -o yaml | "${KCTL[@]}" apply -f -
  "${KCTL[@]}" create namespace opendatahub --dry-run=client -o yaml | "${KCTL[@]}" apply -f -
  "${KCTL[@]}" create namespace models-as-a-service --dry-run=client -o yaml | "${KCTL[@]}" apply -f -
  "${KCTL[@]}" create namespace ai-tenant-tenant-b --dry-run=client -o yaml | "${KCTL[@]}" apply -f -
  "${KCTL[@]}" create namespace ai-tenant-transition --dry-run=client -o yaml | "${KCTL[@]}" apply -f -
  "${KCTL[@]}" label namespace models-as-a-service local-env.opendatahub.io/gateway-tenant=models-as-a-service --overwrite
  "${KCTL[@]}" label namespace ai-tenant-tenant-b local-env.opendatahub.io/gateway-tenant=ai-tenant-tenant-b ai-gateway.opendatahub.io/tenant=true maas.opendatahub.io/managed-by-aitenant=true --overwrite
  "${KCTL[@]}" label namespace ai-tenant-transition local-env.opendatahub.io/gateway-tenant=ai-tenant-transition --overwrite
  "${KCTL[@]}" create namespace ai-tenants --dry-run=client -o yaml | "${KCTL[@]}" apply -f -
  "${KCTL[@]}" apply -f "$ROOT/test/kind-env/manifests/05-database.yaml"
  "${KCTL[@]}" apply -f "$ROOT/test/kind-env/manifests/30-gateway.yaml"
  gateway_uid=$("${KCTL[@]}" -n maas-system get gateway maas-default-gateway -o jsonpath='{.metadata.uid}')
  "${KCTL[@]}" -n maas-system patch service maas-default-gateway --type=merge -p="{\"metadata\":{\"ownerReferences\":[{\"apiVersion\":\"gateway.networking.k8s.io/v1\",\"kind\":\"Gateway\",\"name\":\"maas-default-gateway\",\"uid\":\"$gateway_uid\",\"controller\":false,\"blockOwnerDeletion\":false}]}}"
  tenant_b_gateway_uid=$("${KCTL[@]}" -n maas-system get gateway maas-tenant-b-gateway -o jsonpath='{.metadata.uid}')
  "${KCTL[@]}" -n maas-system patch service maas-tenant-b-gateway --type=merge -p="{\"metadata\":{\"ownerReferences\":[{\"apiVersion\":\"gateway.networking.k8s.io/v1\",\"kind\":\"Gateway\",\"name\":\"maas-tenant-b-gateway\",\"uid\":\"$tenant_b_gateway_uid\",\"controller\":false,\"blockOwnerDeletion\":false}]}}"
  transition_gateway_uid=$("${KCTL[@]}" -n maas-system get gateway maas-transition-gateway -o jsonpath='{.metadata.uid}')
  "${KCTL[@]}" -n maas-system patch service maas-transition-gateway --type=merge -p="{\"metadata\":{\"ownerReferences\":[{\"apiVersion\":\"gateway.networking.k8s.io/v1\",\"kind\":\"Gateway\",\"name\":\"maas-transition-gateway\",\"uid\":\"$transition_gateway_uid\",\"controller\":false,\"blockOwnerDeletion\":false}]}}"
  "${KCTL[@]}" apply -f "$MAAS_CONTROLLER_REPO/deployment/base/networking/odh/kuadrant.yaml"
  # MaaS watches KServe LLMInferenceService. Install the checked-out CRDs
  # before its controller so its controller-runtime cache can synchronize.
  "${KCTL[@]}" apply --server-side -f "$KSERVE_REPO/test/crds/serving.kserve.io_all_crds.yaml"
  # Keep both the upstream render and the exact Kind-transformed bundle. This
  # makes a generated maas-api merge distinguishable from a harness patch.
  kustomize build "$MAAS_CONTROLLER_REPO/maas-api/deploy/overlays/xks" >"$EVIDENCE/maas-api-rendered-xks.yaml"
  kustomize build "$MAAS_CONTROLLER_REPO/maas-api/deploy/overlays/odh" >"$EVIDENCE/maas-api-rendered-odh.yaml" 2>"$EVIDENCE/maas-api-rendered-odh.err" || true
  sha256sum "$EVIDENCE/maas-api-rendered-xks.yaml" "$EVIDENCE/maas-api-rendered-odh.yaml" >"$EVIDENCE/maas-api-rendered.sha256" || true
  cp "$ROOT/test/kind-env/manifests/30-gateway.yaml" "$EVIDENCE/kind-patch-30-gateway.yaml"
  cp "$ROOT/test/kind-env/manifests/31-maas-api-kind-rbac.yaml" "$EVIDENCE/kind-patch-31-maas-api-rbac.yaml"
  # Keep the MaaS controller/API HTTPS contract intact on Kind. Only the
  # namespace and local image substitutions are harness concerns; validation
  # URLs, secure ports, and TLS settings must remain those rendered upstream.
  # The canonical MaaS controller base requires the OpenShift/service-ca
  # generated Secrets below. Kind has neither service-ca nor the OpenShift
  # certificate controller, so provision equivalent run-owned certificates
  # before applying the Deployment. This avoids starting a controller whose
  # required projected volumes cannot mount if provisioning is interrupted.
  controller_tls_tmp=$(mktemp -d)
  openssl req -x509 -nodes -newkey rsa:2048 -days 2 \
    -keyout "$controller_tls_tmp/tls.key" -out "$controller_tls_tmp/tls.crt" \
    -subj '/CN=maas-controller-webhook-service.maas-system.svc' \
    -addext 'basicConstraints=critical,CA:FALSE' \
    -addext 'keyUsage=critical,digitalSignature,keyEncipherment' \
    -addext 'extendedKeyUsage=serverAuth' \
    -addext 'subjectAltName=DNS:maas-controller-webhook-service,DNS:maas-controller-webhook-service.maas-system.svc,DNS:maas-controller-webhook-service.maas-system.svc.cluster.local,DNS:maas-controller-metrics,DNS:maas-controller-metrics.maas-system.svc,DNS:maas-controller-metrics.maas-system.svc.cluster.local' >/dev/null 2>&1
  openssl x509 -in "$controller_tls_tmp/tls.crt" -noout -issuer -subject -serial -fingerprint -sha256 -ext subjectAltName \
    >"$EVIDENCE/maas-controller-certificate.txt"
  sha256sum "$controller_tls_tmp/tls.crt" >"$EVIDENCE/maas-controller-certificate.sha256"
  "${KCTL[@]}" -n maas-system create secret tls maas-controller-webhook-cert \
    --cert="$controller_tls_tmp/tls.crt" --key="$controller_tls_tmp/tls.key" \
    --dry-run=client -o yaml | "${KCTL[@]}" apply -f -
  "${KCTL[@]}" -n maas-system create secret tls maas-controller-metrics-tls \
    --cert="$controller_tls_tmp/tls.crt" --key="$controller_tls_tmp/tls.key" \
    --dry-run=client -o yaml | "${KCTL[@]}" apply -f -
  kustomize build "$MAAS_CONTROLLER_REPO/deployment/base/maas-controller/default" | sed -e "s#quay.io/opendatahub/maas-api:latest#${MAAS_API_RUNTIME_IMAGE}#g" -e "s#quay.io/opendatahub/maas-api:odh-stable#${MAAS_API_RUNTIME_IMAGE}#g" -e "s#quay.io/opendatahub/maas-controller:latest#${MAAS_CONTROLLER_IMAGE:-maas-controller:external-model-two-plane}#g" -e "s#quay.io/opendatahub/maas-controller:odh-stable#${MAAS_CONTROLLER_IMAGE:-maas-controller:external-model-two-plane}#g" -e 's#openshift-ingress#maas-system#g' -e 's#namespace: opendatahub#namespace: maas-system#g' -e 's#namespace: system#namespace: maas-system#g' | yq eval 'select(.kind != "ServiceMonitor" and .kind != "ValidatingWebhookConfiguration")' - >"$EVIDENCE/maas-controller-kind-rendered.yaml"
  # This run-owned Deployment may contain the literal image override from an
  # earlier provision. Delete it before applying the upstream valueFrom form;
  # otherwise apply can merge both env representations and reject the spec.
  "${KCTL[@]}" -n maas-system delete deployment maas-controller --ignore-not-found --wait=true
  "${KCTL[@]}" apply -f "$EVIDENCE/maas-controller-kind-rendered.yaml"
  apply_maas_api_translation_schema
  "${KCTL[@]}" -n maas-system set env deployment/maas-controller "RELATED_IMAGE_ODH_MAAS_API_IMAGE=${MAAS_API_RUNTIME_IMAGE}"
  "${KCTL[@]}" -n maas-system patch deployment maas-controller --type=json -p='[{"op":"replace","path":"/spec/template/spec/containers/0/imagePullPolicy","value":"Never"}]'
  # The webhook Secret is created below before the controller is restarted.
  # Run the binary directly: a shell wrapper can race projected Secret mounts
  # and obscure the controller's startup error in Kind.
  "${KCTL[@]}" -n maas-system patch deployment maas-controller --type=json -p='[{"op":"replace","path":"/spec/template/spec/containers/0/args/3","value":"--metrics-secure=false"},{"op":"replace","path":"/spec/template/spec/containers/0/command","value":["/manager"]},{"op":"replace","path":"/spec/template/spec/containers/0/args","value":["--leader-elect","--health-probe-bind-address=:8081","--metrics-bind-address=:8080","--metrics-secure=false","--controller-namespace=maas-system","--gateway-name=maas-default-gateway","--gateway-namespace=maas-system","--infra-namespace=AUTO","--maas-subscription-namespace=models-as-a-service","--aitenant-namespace=ai-tenants","--monitoring-namespace=opendatahub","--enable-tenant-namespace-discovery","--metadata-cache-ttl=60","--authz-cache-ttl=60","--log-format=zap"]}]'
  # Restart the source-built controller after the run-owned serving
  # certificates are available. It will create API Deployments using the
  # source-built image published to the run-owned registry; its Always pull
  # policy remains intact.
  "${KCTL[@]}" -n maas-system rollout restart deployment/maas-controller
  "${KCTL[@]}" -n maas-system rollout restart deployment/maas-api 2>/dev/null || true
  # Kind has no OpenShift service-ca injection. The MaaS webhook is therefore
  # omitted from the rendered Kind bundle; remove any instance left by a
  # previous interrupted provision before applying tenant fixtures.
  "${KCTL[@]}" delete validatingwebhookconfiguration maas-validating-webhook-configuration --ignore-not-found
  db_tmp=$(mktemp -d)
  # Kind has no OpenShift service-ca injection. Create a run-owned CA and use
  # it to sign the MaaS API serving certificate, preserving hostname
  # validation for Authorino's HTTPS metadata evaluator.
  openssl req -x509 -nodes -newkey rsa:2048 -days 2 \
    -keyout "$db_tmp/ca.key" -out "$db_tmp/ca.crt" \
    -subj '/CN=external-model-e2e-ca' \
    -addext 'basicConstraints=critical,CA:TRUE,pathlen:1' \
    -addext 'keyUsage=critical,keyCertSign,cRLSign' >/dev/null 2>&1
  openssl req -new -nodes -newkey rsa:2048 \
    -keyout "$db_tmp/tls.key" -out "$db_tmp/tls.csr" \
    -subj '/CN=maas-api.maas-system.svc' >/dev/null 2>&1
  cat >"$db_tmp/tls.ext" <<'EOF'
basicConstraints=critical,CA:FALSE
keyUsage=critical,digitalSignature,keyEncipherment
extendedKeyUsage=serverAuth
subjectAltName=DNS:maas-api,DNS:maas-api.maas-system,DNS:maas-api.maas-system.svc,DNS:maas-api.maas-system.svc.cluster.local
EOF
  openssl x509 -req -in "$db_tmp/tls.csr" -CA "$db_tmp/ca.crt" -CAkey "$db_tmp/ca.key" \
    -CAcreateserial -out "$db_tmp/tls.crt" -days 2 -sha256 -extfile "$db_tmp/tls.ext" >/dev/null 2>&1
  openssl req -new -nodes -newkey rsa:2048 \
    -keyout "$db_tmp/tenant-b-tls.key" -out "$db_tmp/tenant-b-tls.csr" \
    -subj '/CN=maas-api-tenant-b.maas-system.svc' >/dev/null 2>&1
  sed 's/maas-api/maas-api-tenant-b/g' "$db_tmp/tls.ext" >"$db_tmp/tenant-b-tls.ext"
  openssl x509 -req -in "$db_tmp/tenant-b-tls.csr" -CA "$db_tmp/ca.crt" -CAkey "$db_tmp/ca.key" \
    -CAcreateserial -out "$db_tmp/tenant-b-tls.crt" -days 2 -sha256 -extfile "$db_tmp/tenant-b-tls.ext" >/dev/null 2>&1
  openssl req -new -nodes -newkey rsa:2048 \
    -keyout "$db_tmp/transition-tls.key" -out "$db_tmp/transition-tls.csr" \
    -subj '/CN=maas-api-transition.maas-system.svc' >/dev/null 2>&1
  sed 's/maas-api/maas-api-transition/g' "$db_tmp/tls.ext" >"$db_tmp/transition-tls.ext"
  openssl x509 -req -in "$db_tmp/transition-tls.csr" -CA "$db_tmp/ca.crt" -CAkey "$db_tmp/ca.key" \
    -CAcreateserial -out "$db_tmp/transition-tls.crt" -days 2 -sha256 -extfile "$db_tmp/transition-tls.ext" >/dev/null 2>&1
  openssl x509 -in "$db_tmp/ca.crt" -noout -issuer -subject -serial -fingerprint -sha256 -ext subjectAltName >"$EVIDENCE/maas-api-ca-certificate.txt"
  openssl x509 -in "$db_tmp/tls.crt" -noout -issuer -subject -serial -fingerprint -sha256 -ext subjectAltName >"$EVIDENCE/maas-api-serving-certificate.txt"
  sha256sum "$db_tmp/ca.crt" "$db_tmp/tls.crt" >"$EVIDENCE/maas-api-certificates.sha256"
  cp "$db_tmp/ca.crt" "$EVIDENCE/maas-api-ca.crt"
  cp "$db_tmp/tls.crt" "$EVIDENCE/maas-api-serving.crt"
  "${KCTL[@]}" -n maas-system create secret tls maas-api-serving-cert --cert="$db_tmp/tls.crt" --key="$db_tmp/tls.key" --dry-run=client -o yaml | "${KCTL[@]}" apply -f -
  "${KCTL[@]}" -n maas-system create secret tls maas-api-tenant-b-serving-cert --cert="$db_tmp/tenant-b-tls.crt" --key="$db_tmp/tenant-b-tls.key" --dry-run=client -o yaml | "${KCTL[@]}" apply -f -
  "${KCTL[@]}" -n maas-system create secret tls maas-api-transition-serving-cert --cert="$db_tmp/transition-tls.crt" --key="$db_tmp/transition-tls.key" --dry-run=client -o yaml | "${KCTL[@]}" apply -f -
  "${KCTL[@]}" -n kuadrant-system create configmap authorino-maas-api-ca --from-file=ca.crt="$db_tmp/ca.crt" --dry-run=client -o yaml | "${KCTL[@]}" apply -f -
  # Authorino's operator-supported volume projection mounts this run's CA at
  # /etc/ssl/certs/maas-api-serving-ca.crt. A standalone file in that directory
  # is not added to Go's system trust bundle, so point Authorino explicitly at
  # it. Keep TLS verification enabled; the CA is limited to this run's MaaS API.
  authorino_found=false
  for _ in $(seq 1 60); do
    if "${KCTL[@]}" -n kuadrant-system get authorino authorino >/dev/null 2>&1; then
      authorino_found=true
      break
    fi
    sleep 2
  done
  [[ "$authorino_found" == true ]] || { fail "Kuadrant did not create the run-owned Authorino resource"; exit 2; }
  "${KCTL[@]}" -n kuadrant-system patch authorino authorino --type=merge -p='{"spec":{"volumes":{"defaultMode":420,"items":[{"name":"maas-api-serving-ca","mountPath":"/etc/ssl/certs","configMaps":["authorino-maas-api-ca"],"items":[{"key":"ca.crt","path":"maas-api-serving-ca.crt"}]}]}}}'
  "${KCTL[@]}" -n kuadrant-system set env deployment/authorino \
    SSL_CERT_FILE=/etc/ssl/certs/maas-api-serving-ca.crt \
    REQUESTS_CA_BUNDLE=/etc/ssl/certs/maas-api-serving-ca.crt
  printf '%s\n' 'Authorino callback trust: SSL_CERT_FILE and REQUESTS_CA_BUNDLE point to the run-owned MaaS API CA; TLS verification remains enabled.' \
    >"$EVIDENCE/authorino-callback-trust-config.txt"
  "${KCTL[@]}" -n kuadrant-system get authorino authorino -o yaml >"$EVIDENCE/authorino-ca-config.yaml"
  kustomize build "$MAAS_CONTROLLER_REPO/deployment/base/maas-api/rbac" | sed 's#namespace: opendatahub#namespace: maas-system#g' | "${KCTL[@]}" apply -f -
  "${KCTL[@]}" apply -f "$ROOT/test/kind-env/manifests/31-maas-api-kind-rbac.yaml"
  "${KCTL[@]}" apply -f "$ROOT/test/kind-env/manifests/32-maas-api-kind-network.yaml"
  # The webhook Secret is created after the OpenShift-only bundle is rendered;
  # restart so the projected certificate is present before manager startup.
  "${KCTL[@]}" -n maas-system rollout restart deployment/maas-controller
  # Repeated local runs may leave the old static maas-api object in place.
  # Let the current MaaS tenant pipeline create its canonical object so
  # server-side apply cannot merge stale OpenShift fields into the Kind spec.
  # TenantReconciler watches MaasTenantConfig, not child Deployment deletion,
  # so explicitly enqueue it after removing the stale static objects.
  "${KCTL[@]}" -n maas-system delete deployment maas-api service maas-api --ignore-not-found
  reconcile_trigger="${STAMP}"
  if "${KCTL[@]}" -n models-as-a-service get maastenantconfig default-tenant >/dev/null 2>&1; then
    "${KCTL[@]}" -n models-as-a-service annotate maastenantconfig default-tenant \
      "local-env.opendatahub.io/provision-reconcile=$reconcile_trigger" --overwrite >/dev/null
    printf 'tenant=models-as-a-service/default-tenant annotation=local-env.opendatahub.io/provision-reconcile value=%s reason=child-deployment-deletion_is_not_watched\n' \
      "$reconcile_trigger" >"$EVIDENCE/maas-tenant-reconcile-trigger.txt"
  else
    printf 'tenant=models-as-a-service/default-tenant annotation=local-env.opendatahub.io/provision-reconcile action=deferred reason=MaasTenantConfig_is_created_after_AITenant\n' \
      >"$EVIDENCE/maas-tenant-reconcile-trigger.txt"
  fi
  if [[ "${LOCAL_ENV_SKIP_UNSUPPORTED_AIGUARDRAIL_CRD:-false}" == true ]]; then
    # The focused provider-selection fixture does not use AIGuardrail. Some
    # Kind API server builds reject its isDuration CEL helper, so omit only
    # that unrelated CRD when the caller explicitly opts in.
    kustomize build "$ROOT/config/crd" \
      | yq eval 'select(.metadata.name != "aiguardrails.aigateway.opendatahub.io")' - \
      >"$EVIDENCE/aigc-kind-crds.yaml"
    printf '%s\n' 'aiguardrails.aigateway.opendatahub.io: omitted by explicit Kind compatibility override; not used by this fixture' \
      >"$EVIDENCE/kind-crd-exclusions.txt"
    "${KCTL[@]}" apply --server-side -f "$EVIDENCE/aigc-kind-crds.yaml"
  else
  kustomize build "$ROOT/config/crd" | "${KCTL[@]}" apply --server-side -f -
  fi
  # OpenShift service-ca creates this secure metrics certificate. Kind does
  # not run that controller, so provide a run-owned TLS certificate in memory
  # before creating the Deployment; metrics remain TLS-enabled in the test.
  if ! "${KCTL[@]}" -n opendatahub get secret ai-gateway-controller-metrics-tls >/dev/null 2>&1; then
    python3 - "$EVIDENCE/ai-gateway-controller-metrics-cert.crt" <<'PY' | "${KCTL[@]}" apply -f -
import base64, datetime, pathlib, sys
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "ai-gateway-controller-metrics.opendatahub.svc")])
now = datetime.datetime.now(datetime.timezone.utc)
cert = (x509.CertificateBuilder().subject_name(name).issuer_name(name).public_key(key.public_key())
        .serial_number(x509.random_serial_number()).not_valid_before(now - datetime.timedelta(minutes=1))
        .not_valid_after(now + datetime.timedelta(days=2))
        .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
        .add_extension(x509.KeyUsage(digital_signature=True, content_commitment=False, key_encipherment=True,
                                     data_encipherment=False, key_agreement=False, key_cert_sign=False,
                                     crl_sign=False, encipher_only=None, decipher_only=None), critical=True)
        .add_extension(x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH]), critical=True)
        .add_extension(x509.SubjectAlternativeName([x509.DNSName(dns) for dns in (
            "ai-gateway-controller-metrics", "ai-gateway-controller-metrics.opendatahub.svc",
            "ai-gateway-controller-metrics.opendatahub.svc.cluster.local")]), critical=False)
        .sign(key, hashes.SHA256()))
cert_pem = cert.public_bytes(serialization.Encoding.PEM)
key_pem = key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.TraditionalOpenSSL,
                            serialization.NoEncryption())
pathlib.Path(sys.argv[1]).write_bytes(cert_pem)
sys.stdout.write("apiVersion: v1\nkind: Secret\nmetadata:\n  name: ai-gateway-controller-metrics-tls\n  namespace: opendatahub\ntype: kubernetes.io/tls\ndata:\n  tls.crt: " + base64.b64encode(cert_pem).decode() + "\n  tls.key: " + base64.b64encode(key_pem).decode() + "\n")
PY
    openssl x509 -in "$EVIDENCE/ai-gateway-controller-metrics-cert.crt" \
      -noout -issuer -subject -serial -fingerprint -sha256 -ext subjectAltName \
      >"$EVIDENCE/ai-gateway-controller-metrics-cert.txt"
    sha256sum "$EVIDENCE/ai-gateway-controller-metrics-cert.crt" \
      >"$EVIDENCE/ai-gateway-controller-metrics-cert.sha256"
    printf '%s\n' 'run-owned metrics TLS certificate generated in memory because Kind has no OpenShift service-ca; metrics remain TLS-enabled.' \
      >"$EVIDENCE/ai-gateway-controller-metrics-cert-purpose.txt"
  fi

  kustomize build "$ROOT/config/self/default" >"$EVIDENCE/ai-gateway-controller-self-rendered.yaml"
  if "${KCTL[@]}" get crd servicemonitors.monitoring.coreos.com >/dev/null 2>&1; then
    cp "$EVIDENCE/ai-gateway-controller-self-rendered.yaml" "$EVIDENCE/ai-gateway-controller-self-kind.yaml"
  else
    yq eval 'select(.kind != "ServiceMonitor")' "$EVIDENCE/ai-gateway-controller-self-rendered.yaml" \
      >"$EVIDENCE/ai-gateway-controller-self-kind.yaml"
    printf '%s\n' \
      'ServiceMonitor objects omitted because the run-owned Kind cluster does not install Prometheus Operator CRDs; this monitoring object is not used by translation or routing assertions.' \
      >"$EVIDENCE/kind-servicemonitor-exclusion.txt"
  fi
  "${KCTL[@]}" apply --server-side --force-conflicts -f "$EVIDENCE/ai-gateway-controller-self-kind.yaml"
  if [[ "${LOCAL_ENV_PATCH_EXTERNALMODEL_FINALIZER_RBAC:-false}" == true ]]; then
    # The source ClusterRole lacks patch on ExternalModels even though the
    # current controller adds its cleanup finalizer with Patch. This fixture
    # opt-in unblocks controller tests without changing the product manifest.
    if ! "${KCTL[@]}" get clusterrole ai-gateway-controller-role -o json \
      | jq -e 'any(.rules[]; (.apiGroups | index("inference.opendatahub.io")) and (.resources | index("externalmodels")) and (.verbs | index("patch")))' >/dev/null; then
      "${KCTL[@]}" patch clusterrole ai-gateway-controller-role --type=json \
        -p='[{"op":"add","path":"/rules/-","value":{"apiGroups":["inference.opendatahub.io"],"resources":["externalmodels"],"verbs":["patch"]}}]'
    fi
    printf '%s\n' 'added patch on inference.opendatahub.io/externalmodels to the run-owned Kind ClusterRole; product RBAC manifest unchanged' \
      >"$EVIDENCE/kind-rbac-override.txt"
  fi
  "${KCTL[@]}" -n opendatahub set image deployment/ai-gateway-controller manager="${AI_CONTROLLER_IMAGE:-ai-gateway-controller:external-model-two-plane}"
  extra_known_json='[]'
  if [[ -n "${PRAXIS_EXTRA_KNOWN_CLUSTERS:-}" ]]; then
    IFS=',' read -r -a extra_known_clusters <<<"$PRAXIS_EXTRA_KNOWN_CLUSTERS"
    for extra_cluster in "${extra_known_clusters[@]}"; do
      [[ -n "$extra_cluster" ]] || continue
      extra_known_json=$(jq -c --arg cluster "$extra_cluster" '. + ["--known-cluster=" + $cluster]' <<<"$extra_known_json")
    done
  fi
  controller_patch=$(jq -cn --arg extproc_image "$EXTPROC_IMAGE" --argjson extra_known "$extra_known_json" '[
    {op:"replace",path:"/spec/template/spec/containers/0/imagePullPolicy",value:"Never"},
    {op:"replace",path:"/spec/template/spec/containers/0/args",value:([
      "--leader-elect",
      "--health-probe-bind-address=:8081",
      "--gateway-name=maas-default-gateway",
      "--gateway-namespace=maas-system",
      "--known-cluster=provider-provider-a",
      "--known-cluster=provider-provider-b",
      "--known-cluster=provider-provider-proof-a",
      "--known-cluster=provider-provider-proof-b",
      "--known-cluster=provider-provider-proof-c",
      "--known-cluster=provider-api-translation-openai",
      "--known-cluster=provider-api-translation-anthropic",
      "--known-cluster=provider-transition-provider",
      ("--image=" + $extproc_image)
    ] + $extra_known)}
  ]')
  "${KCTL[@]}" -n opendatahub patch deployment ai-gateway-controller --type=json -p="$controller_patch"
  # Tenant-local ExtProc resources are now rendered and owned by ai-gateway-controller from
  # ExternalProvider references. Do not apply the former static tenant
  # Deployments here; doing so would create an unowned same-name object and
  # correctly block the controller's ownership handoff.
  # Create the Praxis opt-in AITenants before their ExternalModels. This lets
  # MaaS materialize its per-tenant IPP operands, then the handoff below can
  # stop them before an IPP writer ever sees an ExternalModel and creates a
  # competing direct-provider HTTPRoute.
  for manifest in "$ROOT/test/kind-env/manifests/20-fixtures.yaml" "$ROOT/test/kind-env/manifests/21-fixtures-tenant-b.yaml" "$ROOT/test/kind-env/manifests/42-transition-fixtures.yaml"; do
    yq eval 'select(.kind == "AITenant")' "$manifest" | "${KCTL[@]}" apply -f -
  done
  # MaaS owns and creates each tenant-local MaasTenantConfig. Apply the
  # payload-processing selector only after that object exists; the AITenant is
  # deliberately not a second source of backend-selection truth. The
  # explicit-ipp transition tenant remains on the normal IPP path.
  for tenant_namespace in models-as-a-service ai-tenant-tenant-b; do
    mtc_ready=false
    for _ in $(seq 1 90); do
      if "${KCTL[@]}" -n "$tenant_namespace" get maastenantconfig default-tenant >/dev/null 2>&1; then
        mtc_ready=true
        break
      fi
      sleep 2
    done
    [[ "$mtc_ready" == true ]] || { fail "MaaS did not create $tenant_namespace/default-tenant MaasTenantConfig"; exit 2; }
    "${KCTL[@]}" -n "$tenant_namespace" annotate maastenantconfig default-tenant \
      maas.opendatahub.io/payload-processing-type=praxis --overwrite
    wait_for_praxis_handoff "$tenant_namespace" || exit 2
  done
  transition_mtc_ready=false
  for _ in $(seq 1 90); do
    if "${KCTL[@]}" -n ai-tenant-transition get maastenantconfig default-tenant >/dev/null 2>&1; then
      transition_mtc_ready=true
      break
    fi
    sleep 2
  done
  [[ "$transition_mtc_ready" == true ]] || { fail "MaaS did not create ai-tenant-transition/default-tenant MaasTenantConfig"; exit 2; }
  "${KCTL[@]}" -n ai-tenant-transition annotate maastenantconfig default-tenant \
    maas.opendatahub.io/payload-processing-type=ipp --overwrite
  for tenant_id in "" tenant-b; do
    deployment_name=payload-processing${tenant_id:+-$tenant_id}
    pre_deployment_name=payload-pre-processing${tenant_id:+-$tenant_id}
    if [[ -z "$tenant_id" ]]; then
      tenant_namespace=models-as-a-service
      gateway_name=maas-default-gateway
    else
      tenant_namespace=ai-tenant-tenant-b
      gateway_name=maas-tenant-b-gateway
    fi
    if "${KCTL[@]}" -n maas-system get deployment "$deployment_name" >/dev/null 2>&1; then
      "${KCTL[@]}" -n maas-system set env deployment/"$deployment_name" \
        NAMESPACE="$tenant_namespace" \
        TENANT_NAMESPACE="$tenant_namespace" \
        GATEWAY_NAMESPACE=maas-system \
        GATEWAY_NAME="$gateway_name" \
        DISABLE_EXTERNAL_MODEL_CONTROLLER=true
    else
      # Praxis opt-in tenants do not require MaaS to materialize the old IPP
      # payload workloads. Record that state and let the controller create the
      # ExtProc-only workloads after the tenant fixtures are applied.
      printf 'tenant=%s deployment=%s state=absent reason=praxis-opt-in\n' \
        "$tenant_namespace" "$deployment_name" >>"$EVIDENCE/maas-payload-handoff.txt"
    fi
    if "${KCTL[@]}" -n maas-system get deployment "$pre_deployment_name" >/dev/null 2>&1; then
      "${KCTL[@]}" -n maas-system set env deployment/"$pre_deployment_name" \
        NAMESPACE="$tenant_namespace" \
        TENANT_NAMESPACE="$tenant_namespace" \
        GATEWAY_NAMESPACE=maas-system \
        GATEWAY_NAME="$gateway_name" \
        DISABLE_EXTERNAL_MODEL_CONTROLLER=true
    fi
    # These exact, unowned bootstrap operands are the MaaS handoff boundary
    # in the Kind fixture. They are removed only after the tenant writer is
    # disabled; the controller then creates the complete labeled set.
    for resource in \
      "deployment/$deployment_name" "deployment/$pre_deployment_name" \
      "service/$deployment_name" "service/$pre_deployment_name" \
      "configmap/payload-processing-plugins${tenant_id:+-$tenant_id}" \
      "serviceaccount/$deployment_name" "envoyfilter/$deployment_name" \
      "networkpolicy/$deployment_name" "destinationrule/$deployment_name" \
      "destinationrule/$pre_deployment_name" \
      "clusterrolebinding/payload-processing-reader${tenant_id:+-$tenant_id}" \
      "clusterrole/payload-processing-ipp-reader${tenant_id:+-$tenant_id}" \
      "clusterrolebinding/payload-processing-ipp-reader${tenant_id:+-$tenant_id}"; do
      # Some handoff entries are cluster-scoped and some are already absent
      # for Praxis opt-in tenants. Neither state is a provisioning failure.
      # Keep the command explicitly non-fatal under set -e while preserving
      # the later ownership check for any surviving deployment.
      "${KCTL[@]}" -n maas-system delete "$resource" --ignore-not-found --wait=true >/dev/null || true
    done
    if "${KCTL[@]}" -n maas-system get deployment "$deployment_name" -o json 2>/dev/null \
      | jq -e '.metadata.labels["app.kubernetes.io/managed-by"] == "ai-gateway-controller"' >/dev/null; then
      :
    elif "${KCTL[@]}" -n maas-system get deployment "$deployment_name" >/dev/null 2>&1; then
      fail "$deployment_name survived handoff without controller ownership"
      exit 2
    fi
  done
  # The ExtProc-only provider path is HTTPS end to end. Katan is a local
  # fixture, so give each run-owned Service a run-owned certificate whose SAN
  # is the exact provider endpoint rendered by the controller. This keeps the
  # fixture behind the same TLS/SNI contract as a real ExternalProvider.
  make_katan_tls() {
    local secret_name=$1 service_name=$2 alias=${3:-}
    local key="$db_tmp/${secret_name}.key" csr="$db_tmp/${secret_name}.csr" cert="$db_tmp/${secret_name}.crt" ext="$db_tmp/${secret_name}.ext"
    openssl req -new -nodes -newkey rsa:2048 -keyout "$key" -out "$csr" \
      -subj "/CN=${service_name}.maas-system.svc" >/dev/null 2>&1
    local san="DNS:${service_name},DNS:${service_name}.maas-system,DNS:${service_name}.maas-system.svc,DNS:${service_name}.maas-system.svc.cluster.local"
    if [[ -n "$alias" ]]; then
      san+=",DNS:${alias},DNS:${alias}.maas-system,DNS:${alias}.maas-system.svc,DNS:${alias}.maas-system.svc.cluster.local"
    fi
    cat >"$ext" <<EOF
basicConstraints=critical,CA:FALSE
keyUsage=critical,digitalSignature,keyEncipherment
extendedKeyUsage=serverAuth
subjectAltName=${san}
EOF
    openssl x509 -req -in "$csr" -CA "$db_tmp/ca.crt" -CAkey "$db_tmp/ca.key" \
      -CAserial "$db_tmp/provider-ca.srl" -CAcreateserial -out "$cert" -days 2 -sha256 -extfile "$ext" >/dev/null 2>&1
    openssl x509 -in "$cert" -noout -issuer -subject -serial -fingerprint -sha256 -ext subjectAltName \
      >>"$EVIDENCE/katan-serving-certificates.txt"
    "${KCTL[@]}" -n maas-system create secret tls "$secret_name" --cert="$cert" --key="$key" --dry-run=client -o yaml | "${KCTL[@]}" apply -f - >/dev/null
  }
  make_katan_tls provider-a-tls provider-a provider-a-ext
  make_katan_tls provider-b-tls provider-b provider-b-ext
  make_katan_tls provider-proof-a-tls provider-proof-a-ext
  make_katan_tls provider-proof-b-tls provider-proof-b-ext
  make_katan_tls provider-proof-c-tls provider-proof-c-ext
  make_katan_tls provider-translation-tls provider-translation-ext provider-translation-openai-ext
  make_katan_tls provider-a-tenant-b-tls provider-a-tenant-b
  make_katan_tls provider-b-tenant-b-tls provider-b-tenant-b
  make_katan_tls provider-transition-tls provider-a-legacy
  # MaaS creates Gateway API-owned gateway Deployments in maas-system; they
  # are distinct from the standalone istio-ingressgateway Deployment. Mount
  # this run's public provider CA into those run-created gateway workloads so
  # the generated DestinationRules can verify Katan without
  # insecureSkipVerify or a plaintext exception. Never overwrite a preexisting
  # shared fixture ConfigMap on a non-disposable cluster.
  provider_ca_configmap=maas-system/external-model-e2e-provider-ca
  provider_ca_configmap_changed=false
  provider_ca_get_error="$EVIDENCE/provider-ca-configmap-get.err"
  if existing_provider_ca=$("${KCTL[@]}" -n maas-system get configmap external-model-e2e-provider-ca -o json 2>"$provider_ca_get_error"); then
    existing_run_id=$(jq -r '.metadata.labels["external-model-e2e/run-id"] // ""' <<<"$existing_provider_ca")
    existing_manager=$(jq -r '.metadata.labels["app.kubernetes.io/managed-by"] // ""' <<<"$existing_provider_ca")
    if [[ "$existing_run_id" != "$CLUSTER" || "$existing_manager" != external-model-e2e ]]; then
      fail "$provider_ca_configmap exists without matching run ownership; refusing to overwrite shared trust state"
      exit 2
    fi
    existing_provider_ca_sha=$(jq -r '.data["ca.crt"]' <<<"$existing_provider_ca" | sha256sum | awk '{print $1}')
    new_provider_ca_sha=$(sha256sum "$db_tmp/ca.crt" | awk '{print $1}')
    if [[ "$existing_provider_ca_sha" != "$new_provider_ca_sha" ]]; then
      provider_ca_configmap_changed=true
      printf 'configmap=%s run_id=%s previous_ca_sha256=%s new_ca_sha256=%s action=rotate_run_owned_fixture_ca\n' \
        "$provider_ca_configmap" "$CLUSTER" "$existing_provider_ca_sha" "$new_provider_ca_sha" \
        >"$EVIDENCE/provider-ca-configmap-rotation.txt"
    else
      printf 'configmap=%s run_id=%s ca_sha256=%s action=retain_matching_run_owned_fixture_ca\n' \
        "$provider_ca_configmap" "$CLUSTER" "$new_provider_ca_sha" \
        >"$EVIDENCE/provider-ca-configmap-rotation.txt"
    fi
  elif rg -q 'Error from server \(NotFound\): configmaps "external-model-e2e-provider-ca" not found' "$provider_ca_get_error"; then
    provider_ca_configmap_changed=true
    printf 'configmap=%s run_id=%s action=create_run_owned_fixture_ca\n' \
      "$provider_ca_configmap" "$CLUSTER" >"$EVIDENCE/provider-ca-configmap-rotation.txt"
  else
    fail "could not verify ownership of $provider_ca_configmap; see provider-ca-configmap-get.err"
    exit 2
  fi
  if [[ "$provider_ca_configmap_changed" == true ]]; then
    "${KCTL[@]}" -n maas-system create configmap external-model-e2e-provider-ca \
      --from-file=ca.crt="$db_tmp/ca.crt" --dry-run=client -o yaml \
      | "${KCTL[@]}" apply -f - >/dev/null
    "${KCTL[@]}" -n maas-system label configmap external-model-e2e-provider-ca \
      external-model-e2e/run-id="$CLUSTER" app.kubernetes.io/managed-by=external-model-e2e --overwrite >/dev/null
  fi
  for gateway_deployment in maas-default-gateway-istio maas-tenant-b-gateway-istio maas-transition-gateway-istio; do
    if ! "${KCTL[@]}" -n maas-system get deployment "$gateway_deployment" >/dev/null 2>&1; then
      continue
    fi
    if ! "${KCTL[@]}" -n maas-system get deployment "$gateway_deployment" -o json \
      | jq -e '.spec.template.spec.volumes[]? | select(.name == "external-model-e2e-provider-ca")' >/dev/null; then
      "${KCTL[@]}" -n maas-system patch deployment "$gateway_deployment" --type=json -p='[{"op":"add","path":"/spec/template/spec/volumes/-","value":{"name":"external-model-e2e-provider-ca","configMap":{"name":"external-model-e2e-provider-ca","items":[{"key":"ca.crt","path":"ca.crt"}]}}},{"op":"add","path":"/spec/template/spec/containers/0/volumeMounts/-","value":{"name":"external-model-e2e-provider-ca","mountPath":"/etc/external-model-e2e/provider-ca","readOnly":true}}]' >/dev/null
    elif [[ "$provider_ca_configmap_changed" == true ]]; then
      "${KCTL[@]}" -n maas-system rollout restart deployment/"$gateway_deployment" >/dev/null
    fi
    "${KCTL[@]}" -n maas-system rollout status deployment/"$gateway_deployment" --timeout=120s
  done
  printf 'configmap=maas-system/external-model-e2e-provider-ca ca_file=/etc/external-model-e2e/provider-ca/ca.crt\n' \
    >"$EVIDENCE/istio-provider-ca-mount.txt"
  # Apply the remaining fixtures after the writer handoff. In particular, do
  # not let the disabled IPP deployment observe the Praxis ExternalModels.
  for manifest in "$ROOT/test/kind-env/manifests"/*.yaml; do
    case "$(basename "$manifest")" in
      20-fixtures.yaml|21-fixtures-tenant-b.yaml|31-maas-api-kind-rbac.yaml|32-maas-api-kind-network.yaml|40-maas-fixtures.yaml|41-maas-fixtures-tenant-b.yaml|42-transition-fixtures.yaml|provider-proof-external-providers.yaml|provider-selection-proof.yaml|shared-translation-backend.yaml|shared-translation-routes.yaml|api-translation-provider-aware-backend.yaml) continue ;;
      45-real-openai-policies.yaml|60-client-kind-patch.yaml) continue ;;
    esac
    if [[ "$(basename "$manifest")" == 00-backends.yaml ]]; then
      # Kind runs the platform-pulled local image. The immutable public
      # digest remains recorded in the provision evidence and is the source
      # of the loaded image; the local name is only a registry-free transport
      # reference understood by the Kind node.
      sed "s#ghcr.io/nerdalert/llm-katan@sha256:11379a1ec2fd69dc121eada6c544eb423a7c074414507dc1d474f4abba9df75a#$KATAN_KIND_IMAGE#g" "$manifest" | "${KCTL[@]}" apply -f -
    else
      "${KCTL[@]}" apply -f "$manifest"
    fi
  done
  # shellcheck disable=SC2016
  EXTERNAL_MODEL_NAMESPACE=models-as-a-service \
  EXTERNAL_MODEL_RUN_ID="$CLUSTER" \
  EXTERNAL_MODEL_PROVIDER_A_ENDPOINT=provider-a-ext.maas-system.svc.cluster.local \
  EXTERNAL_MODEL_PROVIDER_B_ENDPOINT=provider-b-ext.maas-system.svc.cluster.local \
  EXTERNAL_MODEL_PROVIDER_CA_PATH=/etc/external-model-e2e/provider-ca/ca.crt \
    envsubst '${EXTERNAL_MODEL_NAMESPACE} ${EXTERNAL_MODEL_PROVIDER_A_ENDPOINT} ${EXTERNAL_MODEL_PROVIDER_B_ENDPOINT} ${EXTERNAL_MODEL_PROVIDER_CA_PATH} ${EXTERNAL_MODEL_RUN_ID}' \
      <"$ROOT/test/external-model/providers.yaml.tmpl" | "${KCTL[@]}" apply -f -
  if [[ "$APPLY_REAL_OPENAI_FIXTURE" == true ]]; then
    # This fixture is intentionally explicit: it expands the allowlist with
    # provider-openai and must never be pulled into the ordinary glob above.
    # shellcheck disable=SC2016
    EXTERNAL_MODEL_NAMESPACE=models-as-a-service \
    EXTERNAL_MODEL_RUN_ID="$CLUSTER" \
    EXTERNAL_MODEL_OPENAI_SECRET=openai-provider-credentials \
    EXTERNAL_MODEL_OPENAI_SUBSCRIPTION=openai-e2e-subscription \
    EXTERNAL_MODEL_OPENAI_POLICY=openai-e2e-access \
    EXTERNAL_MODEL_OPENAI_USER=kind-user \
      envsubst '${EXTERNAL_MODEL_NAMESPACE} ${EXTERNAL_MODEL_OPENAI_SECRET} ${EXTERNAL_MODEL_OPENAI_SUBSCRIPTION} ${EXTERNAL_MODEL_OPENAI_POLICY} ${EXTERNAL_MODEL_OPENAI_USER} ${EXTERNAL_MODEL_RUN_ID}' \
        <"$ROOT/test/external-model/openai.yaml.tmpl" | "${KCTL[@]}" apply -f -
    "${KCTL[@]}" apply -f "$ROOT/test/kind-env/manifests/45-real-openai-policies.yaml"
  fi
  # MaaS creates one existing-IPP deployment per tenant. The upstream IPP
  # runner supports a namespace-scoped cache and explicit Gateway settings;
  # provide those only in this Kind fixture.  Keep IPP disabled for tenants
  # already owned by Praxis so it cannot create a competing direct-provider
  # route, and enable it only for the explicit-ipp transition tenant.
  ipp_ready=false
  for _ in $(seq 1 60); do
    # Praxis tenants may already have had their MaaS IPP operands removed by
    # the ownership-gated transition. Only the explicit-ipp transition
    # tenant is required to retain an existing-IPP deployment at this stage.
    if "${KCTL[@]}" -n maas-system get deployment payload-processing-transition >/dev/null 2>&1; then
      ipp_ready=true
      break
    fi
    sleep 2
  done
  [[ "$ipp_ready" == true ]] || { fail "MaaS did not create the transition tenant IPP deployment"; exit 2; }
  if "${KCTL[@]}" -n maas-system get deployment/payload-processing >/dev/null 2>&1; then
    "${KCTL[@]}" -n maas-system set env deployment/payload-processing \
      NAMESPACE=models-as-a-service TENANT_NAMESPACE=models-as-a-service GATEWAY_NAMESPACE=maas-system GATEWAY_NAME=maas-default-gateway DISABLE_EXTERNAL_MODEL_CONTROLLER=true
  fi
  if "${KCTL[@]}" -n maas-system get deployment/payload-processing-tenant-b >/dev/null 2>&1; then
    "${KCTL[@]}" -n maas-system set env deployment/payload-processing-tenant-b \
      NAMESPACE=ai-tenant-tenant-b TENANT_NAMESPACE=ai-tenant-tenant-b GATEWAY_NAMESPACE=maas-system GATEWAY_NAME=maas-tenant-b-gateway DISABLE_EXTERNAL_MODEL_CONTROLLER=true
  fi
  "${KCTL[@]}" -n maas-system set env deployment/payload-processing-transition \
    NAMESPACE=ai-tenant-transition TENANT_NAMESPACE=ai-tenant-transition GATEWAY_NAMESPACE=maas-system GATEWAY_NAME=maas-transition-gateway DISABLE_EXTERNAL_MODEL_CONTROLLER=false
  if "${KCTL[@]}" -n maas-system get deployment/payload-pre-processing >/dev/null 2>&1; then
    "${KCTL[@]}" -n maas-system set env deployment/payload-pre-processing \
      NAMESPACE=models-as-a-service TENANT_NAMESPACE=models-as-a-service GATEWAY_NAMESPACE=maas-system GATEWAY_NAME=maas-default-gateway DISABLE_EXTERNAL_MODEL_CONTROLLER=true
  fi
  if "${KCTL[@]}" -n maas-system get deployment/payload-pre-processing-tenant-b >/dev/null 2>&1; then
    "${KCTL[@]}" -n maas-system set env deployment/payload-pre-processing-tenant-b \
      NAMESPACE=ai-tenant-tenant-b TENANT_NAMESPACE=ai-tenant-tenant-b GATEWAY_NAMESPACE=maas-system GATEWAY_NAME=maas-tenant-b-gateway DISABLE_EXTERNAL_MODEL_CONTROLLER=true
  fi
  "${KCTL[@]}" -n maas-system set env deployment/payload-pre-processing-transition \
    NAMESPACE=ai-tenant-transition TENANT_NAMESPACE=ai-tenant-transition GATEWAY_NAMESPACE=maas-system GATEWAY_NAME=maas-transition-gateway DISABLE_EXTERNAL_MODEL_CONTROLLER=false
  if "${KCTL[@]}" -n maas-system get deployment/payload-processing >/dev/null 2>&1; then "${KCTL[@]}" -n maas-system rollout status deployment/payload-processing --timeout=120s; fi
  if "${KCTL[@]}" -n maas-system get deployment/payload-processing-tenant-b >/dev/null 2>&1; then "${KCTL[@]}" -n maas-system rollout status deployment/payload-processing-tenant-b --timeout=120s; fi
  "${KCTL[@]}" -n maas-system rollout status deployment/payload-processing-transition --timeout=120s
  if "${KCTL[@]}" -n maas-system get deployment/payload-pre-processing >/dev/null 2>&1; then "${KCTL[@]}" -n maas-system rollout status deployment/payload-pre-processing --timeout=120s; fi
  if "${KCTL[@]}" -n maas-system get deployment/payload-pre-processing-tenant-b >/dev/null 2>&1; then "${KCTL[@]}" -n maas-system rollout status deployment/payload-pre-processing-tenant-b --timeout=120s; fi
  "${KCTL[@]}" -n maas-system rollout status deployment/payload-pre-processing-transition --timeout=120s
  # Apply the transition IPP resources only after its writer has the
  # run-owned Gateway settings. Applying the ExternalModel earlier lets the
  # IPP reconciler publish a route against its default OpenShift Gateway.
  yq eval 'select(.kind != "AITenant")' "$ROOT/test/kind-env/manifests/42-transition-fixtures.yaml" | "${KCTL[@]}" apply -f -
  # Apply Praxis-tenant MaaS model references only after their IPP writers have
  # been disabled and rolled out. Otherwise the old IPP ExternalModel watcher
  # can observe the reference first and create a competing direct-provider
  # HTTPRoute before the controller handoff is complete.
  for manifest in "$ROOT/test/kind-env/manifests/40-maas-fixtures.yaml" "$ROOT/test/kind-env/manifests/41-maas-fixtures-tenant-b.yaml"; do
    "${KCTL[@]}" apply -f "$manifest"
  done
  for manifest in "$ROOT/test/kind-env/manifests/20-fixtures.yaml" "$ROOT/test/kind-env/manifests/21-fixtures-tenant-b.yaml"; do
    yq eval 'select(.kind != "AITenant")' "$manifest" | "${KCTL[@]}" apply -f -
  done
  # Do not delete existing IPP HTTPRoutes here. Their owner is the pinned IPP
  # ExternalModel reconciler, and deleting them would hide an ownership or
  # cutover defect. The qualification records any such route explicitly.
  "${KCTL[@]}" -n ai-tenants get aitenant models-as-a-service -o yaml >"$EVIDENCE/aitenant-after-fixtures.yaml" 2>&1 || true
  "${KCTL[@]}" -n maas-system get config default -o yaml >"$EVIDENCE/maas-config-after-fixtures.yaml" 2>&1 || true
  "${KCTL[@]}" -n maas-system get deployment maas-api -o yaml >"$EVIDENCE/maas-api-before-tenant-apply.yaml" 2>&1 || true
  "${KCTL[@]}" -n maas-system rollout status deployment/maas-postgres --timeout=180s
  gateway_ready=false
  for _ in $(seq 1 60); do
    listener_status=$("${KCTL[@]}" -n maas-system get gateway maas-default-gateway -o jsonpath='{.status.listeners[0].conditions[?(@.type=="Programmed")].status}' 2>/dev/null || true)
    endpoint_count=$("${KCTL[@]}" -n maas-system get endpoints maas-default-gateway -o jsonpath='{.subsets[*].addresses[*].ip}' 2>/dev/null || true)
    # Kind's LoadBalancer address remains Pending even when Istio has fully
    # programmed the listener. Use the listener condition plus endpoints as
    # the state-based readiness signal for the run-owned Gateway Service.
    if [[ "$listener_status" == "True" && -n "$endpoint_count" ]]; then
      gateway_ready=true
      break
    fi
    sleep 3
  done
  [[ "$gateway_ready" == true ]] || { fail "Istio Gateway listener or endpoint did not become ready"; exit 2; }
  # Kind's LoadBalancer Service has no external address, while MaaS uses the
  # Gateway address to resolve a model endpoint. Publish the run-owned
  # in-cluster Gateway hostname after the listener is actually programmed;
  # OpenShift's gateway/operator supplies this status in production.
  "${KCTL[@]}" -n maas-system patch gateway maas-default-gateway --subresource=status --type=merge -p='{"status":{"addresses":[{"type":"Hostname","value":"maas-default-gateway-istio.maas-system.svc.cluster.local"}]}}'
  "${KCTL[@]}" -n maas-system patch gateway maas-tenant-b-gateway --subresource=status --type=merge -p='{"status":{"addresses":[{"type":"Hostname","value":"maas-tenant-b-gateway.maas-system.svc.cluster.local"}]}}'
  transition_gateway_ready=false
  for _ in $(seq 1 60); do
    transition_listener=$("${KCTL[@]}" -n maas-system get gateway maas-transition-gateway -o jsonpath='{.status.listeners[0].conditions[?(@.type=="Programmed")].status}' 2>/dev/null || true)
    transition_endpoints=$("${KCTL[@]}" -n maas-system get endpoints maas-transition-gateway -o jsonpath='{.subsets[*].addresses[*].ip}' 2>/dev/null || true)
    if [[ "$transition_listener" == "True" && -n "$transition_endpoints" ]]; then transition_gateway_ready=true; break; fi
    sleep 3
  done
  [[ "$transition_gateway_ready" == true ]] || { fail "Istio transition Gateway listener or endpoint did not become ready"; exit 2; }
  # MaaS may reconcile the generated existing-IPP deployments while the three
  # gateways are becoming ready. Re-apply the transition-only Kind wiring at
  # the settled point, then wait for the pods that read these values at
  # startup. This selects the transition Gateway for the real IPP fixture;
  # it does not remove or rewrite the IPP-owned HTTPRoute.
  for ipp_deployment in payload-processing-transition payload-pre-processing-transition; do
    "${KCTL[@]}" -n maas-system set env deployment/"$ipp_deployment" \
      NAMESPACE=ai-tenant-transition TENANT_NAMESPACE=ai-tenant-transition GATEWAY_NAMESPACE=maas-system GATEWAY_NAME=maas-transition-gateway DISABLE_EXTERNAL_MODEL_CONTROLLER=false
    "${KCTL[@]}" -n maas-system rollout status deployment/"$ipp_deployment" --timeout=120s
  done
  "${KCTL[@]}" -n maas-system patch gateway maas-transition-gateway --subresource=status --type=merge -p='{"status":{"addresses":[{"type":"Hostname","value":"maas-transition-gateway.maas-system.svc.cluster.local"}]}}'
  # The tenant pipeline creates/reconciles one API Deployment per tenant.
  # Wait for its first Deployment, then select the exact source-built image
  # served by the run-owned registry. The controller retains imagePullPolicy
  # Always and can now resolve every tenant API replica.
  maas_api_created=false
  for _ in $(seq 1 60); do
    if "${KCTL[@]}" -n maas-system get deployment maas-api >/dev/null 2>&1; then
      maas_api_created=true
      break
    fi
    sleep 3
  done
  [[ "$maas_api_created" == true ]] || { fail "MaaS tenant pipeline did not create the default maas-api"; exit 2; }
  for api_deployment in maas-api maas-api-tenant-b maas-api-transition; do
    if "${KCTL[@]}" -n maas-system get deployment "$api_deployment" >/dev/null 2>&1; then
      "${KCTL[@]}" -n maas-system set image "deployment/$api_deployment" "maas-api=${MAAS_API_RUNTIME_IMAGE}"
      # MaaS API validates the internal Gateway during startup and must read
      # the serving certificate signed by this run's CA.
      "${KCTL[@]}" -n maas-system rollout restart "deployment/$api_deployment"
      "${KCTL[@]}" -n maas-system rollout status "deployment/$api_deployment" --timeout=180s
    fi
  done
  api_runtime_ready=false
  expected_maas_api_repo_digest=$(sed -n 's/^expected_registry_repo_digest=//p' "$EVIDENCE/maas-api-registry-node-image-identity.txt")
  [[ -n "$expected_maas_api_repo_digest" ]] || {
    fail "source-built MaaS API registry digest is missing from image identity evidence"; exit 2;
  }
  for _ in $(seq 1 90); do
    all_api_runtime_ready=true
    for api_deployment in maas-api maas-api-tenant-b maas-api-transition; do
      if ! api_deployment_json=$("${KCTL[@]}" -n maas-system get deployment "$api_deployment" -o json 2>/dev/null) ||
         ! api_selector=$(maas_api_deployment_pod_selector "$api_deployment_json" 2>/dev/null) ||
         ! api_pods_json=$("${KCTL[@]}" -n maas-system get pods -l "$api_selector" -o json 2>/dev/null); then
        all_api_runtime_ready=false
        break
      fi
      if ! maas_api_pods_have_exact_ready_image "$api_pods_json" "$MAAS_API_RUNTIME_IMAGE" "$expected_maas_api_repo_digest"; then
        all_api_runtime_ready=false
        break
      fi
    done
    if [[ "$all_api_runtime_ready" == true ]]; then
      api_runtime_ready=true
      break
    fi
    sleep 2
  done
  if [[ "$api_runtime_ready" != true ]]; then
    "${KCTL[@]}" -n maas-system get deploy,pods -o json >"$EVIDENCE/maas-api-runtime-wait-failure.json"
    fail "controller-managed MaaS API pods did not become ready on the exact source-built image"
    exit 2
  fi
  api_runtime_records='[]'
  for api_deployment in maas-api maas-api-tenant-b maas-api-transition; do
    api_deployment_json=$("${KCTL[@]}" -n maas-system get deployment "$api_deployment" -o json)
    api_selector=$(maas_api_deployment_pod_selector "$api_deployment_json")
    api_pods_json=$("${KCTL[@]}" -n maas-system get pods -l "$api_selector" -o json)
    api_runtime_row=$(jq -cn --argjson deployment "$api_deployment_json" --argjson pods "$api_pods_json" \
      --arg expected_image "$MAAS_API_RUNTIME_IMAGE" --arg expected_image_digest "$expected_maas_api_repo_digest" '
      [$deployment.spec.template.spec.containers[] | select(.name == "maas-api")][0] as $container |
      {deployment:$deployment.metadata.name,image:$container.image,pullPolicy:$container.imagePullPolicy,
       readyReplicas:($deployment.status.readyReplicas // 0),expectedImage:$expected_image,
       expectedImageDigest:$expected_image_digest,
       exactImage:($container.image == $expected_image),controllerManagedAlwaysPull:($container.imagePullPolicy == "Always"),
       pods:[$pods.items[] | select(.metadata.deletionTimestamp == null) as $pod |
         $pod.status.containerStatuses[]? | select(.name == "maas-api") |
         {name:$pod.metadata.name,image:.image,imageID:.imageID,
          podSpecImage:([$pod.spec.containers[]? | select(.name == "maas-api") | .image] | first),ready:(.ready == true)}],
       exactReadyPodCount:([$pods.items[] |
         select(.metadata.deletionTimestamp == null and .status.phase == "Running") |
         select(any(.spec.containers[]?; .name == "maas-api" and .image == $expected_image)) |
         select(any(.status.containerStatuses[]?;
           .name == "maas-api" and .ready == true and ((.imageID // "" | sub("^containerd://"; "")) == $expected_image_digest)))] | length)}')
    api_runtime_records=$(jq -cn --argjson rows "$api_runtime_records" --argjson row "$api_runtime_row" '$rows + [$row]')
  done
  printf '%s\n' "$api_runtime_records" >"$EVIDENCE/maas-api-runtime-deployments.json"
  jq -e 'length == 3 and all(.[]; .exactImage and .controllerManagedAlwaysPull and .readyReplicas >= 1 and .exactReadyPodCount >= 1)' \
    "$EVIDENCE/maas-api-runtime-deployments.json" >/dev/null || {
    fail "all three controller-managed MaaS API Deployments must run the exact registry image with Always pulls"; exit 2;
    }
  "${KCTL[@]}" -n ai-tenants get aitenant models-as-a-service -o yaml >"$EVIDENCE/aitenant-generated.yaml" 2>&1 || true
  "${KCTL[@]}" -n maas-system get config default -o yaml >"$EVIDENCE/maas-config-generated.yaml" 2>&1 || true
  "${KCTL[@]}" -n maas-system get deployment maas-api -o yaml >"$EVIDENCE/maas-api-generated.yaml" 2>&1 || true
  "${KCTL[@]}" -n maas-system rollout status deployment/maas-api --timeout=180s
  # The shared callback identity must terminate at exactly one real MaaS API
  # target. This is an ownership/selector gate, not a synthetic response
  # adapter: the selected pod is the source-built MaaS API deployment.
  maas_api_service_json=$("${KCTL[@]}" -n maas-system get service maas-api -o json)
  maas_api_selector=$(jq -r '.spec.selector | to_entries | map(.key + "=" + .value) | join(",")' <<<"$maas_api_service_json")
  [[ -n "$maas_api_selector" ]] || { fail "shared maas-api Service has no selector"; exit 2; }
  maas_api_targets=$("${KCTL[@]}" -n maas-system get pods -l "$maas_api_selector" -o json)
  printf '%s\n' "$maas_api_service_json" | jq '{metadata:{name:.metadata.name,namespace:.metadata.namespace,labels:.metadata.labels,ownerReferences:.metadata.ownerReferences},spec:{selector:.spec.selector,ports:.spec.ports}}' >"$EVIDENCE/shared-maas-api-service.json"
  printf '%s\n' "$maas_api_targets" | jq '[.items[] | {name:.metadata.name,uid:.metadata.uid,deletionTimestamp:.metadata.deletionTimestamp,ownerReferences:.metadata.ownerReferences,phase:.status.phase,ready:([.status.conditions[]? | select(.type=="Ready" and .status=="True")] | length == 1),restarts:([.status.containerStatuses[]?.restartCount] | add // 0)}]' >"$EVIDENCE/shared-maas-api-targets.json"
  # A Deployment rollout can leave the old ReplicaSet pod terminating while
  # the replacement is already Ready. It is not an eligible Service target;
  # count only non-deleting pods, while retaining the full inventory above.
  maas_api_target_count=$(jq '[.items[] | select(.metadata.deletionTimestamp == null)] | length' <<<"$maas_api_targets")
  maas_api_ready_count=$(jq '[.items[] | select(.metadata.deletionTimestamp == null) | select([.status.conditions[]? | select(.type=="Ready" and .status=="True")] | length == 1)] | length' <<<"$maas_api_targets")
  [[ "$maas_api_target_count" == 1 && "$maas_api_ready_count" == 1 ]] || { fail "shared maas-api Service target must be exactly one Ready real MaaS API pod (targets=$maas_api_target_count ready=$maas_api_ready_count selector=$maas_api_selector)"; exit 2; }
  "${KCTL[@]}" -n maas-system rollout status deployment/maas-controller --timeout=180s
  "${KCTL[@]}" -n opendatahub rollout status deployment/ai-gateway-controller --timeout=180s
  expected_controller_image=$(<"$EVIDENCE/controller-image-id.txt")
  expected_controller_reference=$(<"$EVIDENCE/controller-image-reference.txt")
  controller_deployment=$("${KCTL[@]}" -n opendatahub get deployment ai-gateway-controller -o json)
  controller_selector=$(jq -r '.spec.selector.matchLabels | to_entries | map(.key + "=" + .value) | join(",")' <<<"$controller_deployment")
  controller_pods=$("${KCTL[@]}" -n opendatahub get pods -l "$controller_selector" -o json)
  expected_controller_cri_reference=$expected_controller_reference
  if [[ "$expected_controller_cri_reference" != */* ]]; then
    expected_controller_cri_reference="docker.io/library/$expected_controller_cri_reference"
  fi
  controller_cri_records='[]'
  mapfile -t controller_ready_pods < <(jq -r -f "$ROOT/test/kind-env/controller-ready-manager-pods.jq" <<<"$controller_pods")
  for controller_pod_row in "${controller_ready_pods[@]}"; do
    IFS=$'\t' read -r controller_pod_name controller_node_name controller_pod_image_id controller_runtime_image <<<"$controller_pod_row"
    controller_cri_json=$(docker exec "$controller_node_name" crictl inspecti "$expected_controller_cri_reference" 2>/dev/null || true)
    if ! jq -e '.status.id and (.status.repoTags | type == "array")' <<<"$controller_cri_json" >/dev/null 2>&1; then
      controller_cri_json='{}'
    fi
    controller_cri_record=$(jq -c \
      --arg pod "$controller_pod_name" \
      --arg node "$controller_node_name" \
      --arg image "$controller_runtime_image" \
      --arg pod_image_id "$controller_pod_image_id" \
      --arg requested_reference "$expected_controller_cri_reference" \
      '{name:$pod,node:$node,image:$image,pod_imageID:$pod_image_id,
        runtime_imageID:(.status.id // null),repoTags:(.status.repoTags // []),
        requested_tag_present:((.status.repoTags // []) | index($requested_reference) != null)}' \
      <<<"$controller_cri_json")
    controller_cri_records=$(jq -cn --argjson records "$controller_cri_records" --argjson record "$controller_cri_record" '$records + [$record]')
  done
  jq -n --arg expected_image "$expected_controller_reference" --arg expected_image_id "$expected_controller_image" \
    --arg expected_cri_reference "$expected_controller_cri_reference" \
    --argjson deployment "$controller_deployment" --argjson cri_records "$controller_cri_records" '
      {expected_image:$expected_image,expected_image_id:$expected_image_id,
       expected_cri_reference:$expected_cri_reference,
       deployment_image:([ $deployment.spec.template.spec.containers[] | select(.name == "manager") | .image ] | first),
       desired_replicas:($deployment.spec.replicas // 1),
       ready_pods:$cri_records}' \
    >"$EVIDENCE/controller-runtime-image-identity.json"
  if ! jq -e '
      . as $identity |
      $identity.deployment_image == $identity.expected_image and
      ($identity.ready_pods | length) == $identity.desired_replicas and
      ([$identity.ready_pods[] |
        .requested_tag_present == true and
        .runtime_imageID == (.pod_imageID | sub("^containerd://"; ""))] | all)
    ' "$EVIDENCE/controller-runtime-image-identity.json" >/dev/null; then
    fail "running ai-gateway-controller pod is not using the source-built tag content; see controller-runtime-image-identity.json"
    exit 2
  fi
  wait_for_extproc() {
    local namespace=$1 name=$2
    for _ in $(seq 1 90); do
      if "${KCTL[@]}" -n "$namespace" get deployment "$name" >/dev/null 2>&1; then
        "${KCTL[@]}" -n "$namespace" rollout status "deployment/$name" --timeout=180s
        return 0
      fi
      sleep 2
    done
    fail "controller did not create tenant ExtProc deployment $namespace/$name"
    return 1
  }
  patch_kind_extproc_identity() {
    local namespace=$1 name=$2
    # Kind's kubelet cannot verify a named image user when runAsNonRoot=true.
    # This is a Kind-only admission compatibility transform. Production
    # manifests deliberately leave UID/GID/fsGroup unset for OpenShift SCC to
    # assign the namespace-safe identity.
    "${KCTL[@]}" -n "$namespace" patch deployment "$name" --type=merge \
      -p='{"spec":{"template":{"spec":{"securityContext":{"runAsUser":65532,"runAsGroup":65532,"fsGroup":65532}}}}}'
  }
  wait_for_extproc maas-system payload-pre-processing
  wait_for_extproc models-as-a-service payload-processing-external-model
  patch_kind_extproc_identity maas-system payload-pre-processing
  patch_kind_extproc_identity models-as-a-service payload-processing-external-model
  for extproc_ref in maas-system/payload-pre-processing models-as-a-service/payload-processing-external-model; do
    extproc_namespace=${extproc_ref%/*}
    extproc_name=${extproc_ref#*/}
    # The Kind identity patch above changes the pod template and can start a
    # second rollout. Wait for that rollout before recording the image, then
    # select only Ready pods owned by the Deployment's current ReplicaSet.
    "${KCTL[@]}" -n "$extproc_namespace" rollout status "deployment/$extproc_name" --timeout=180s
    deployed_image=$("${KCTL[@]}" -n "$extproc_namespace" get deployment "$extproc_name" -o jsonpath='{.spec.template.spec.containers[0].image}')
    [[ "$deployed_image" == "$EXTPROC_IMAGE" ]] || { fail "ExtProc image mismatch for $extproc_ref: requested=$EXTPROC_IMAGE deployed=$deployed_image"; exit 2; }
    deployment_json=$("${KCTL[@]}" -n "$extproc_namespace" get deployment "$extproc_name" -o json)
    deployment_selector=$(jq -r '.spec.selector.matchLabels | to_entries | map(.key + "=" + .value) | join(",")' <<<"$deployment_json")
    [[ -n "$deployment_selector" ]] || { fail "ExtProc Deployment $extproc_ref has no selector"; exit 2; }
    deployment_uid=$(jq -r '.metadata.uid' <<<"$deployment_json")
    extproc_container=$(jq -r '.spec.template.spec.containers[0].name' <<<"$deployment_json")
    generation=$(jq -r '.metadata.generation' <<<"$deployment_json")
    observed_generation=$(jq -r '.status.observedGeneration // 0' <<<"$deployment_json")
    desired_replicas=$(jq -r '.spec.replicas // 1' <<<"$deployment_json")
    updated_replicas=$(jq -r '.status.updatedReplicas // 0' <<<"$deployment_json")
    available_replicas=$(jq -r '.status.availableReplicas // 0' <<<"$deployment_json")
    revision=$(jq -r '.metadata.annotations["deployment.kubernetes.io/revision"] // empty' <<<"$deployment_json")
    [[ "$observed_generation" -ge "$generation" && "$updated_replicas" -eq "$desired_replicas" && "$available_replicas" -ge "$desired_replicas" && -n "$revision" ]] || {
      fail "ExtProc Deployment $extproc_ref has not observed and completed its current generation (generation=$generation observed=$observed_generation updated=$updated_replicas available=$available_replicas desired=$desired_replicas revision=${revision:-absent})"
      exit 2
    }
    replica_sets=$("${KCTL[@]}" -n "$extproc_namespace" get replicasets -l "$deployment_selector" -o json)
    current_replica_sets=$(jq -c --arg revision "$revision" --arg deploymentUID "$deployment_uid" '[.items[] | select(.metadata.annotations["deployment.kubernetes.io/revision"] == $revision) | select(any(.metadata.ownerReferences[]?; .kind == "Deployment" and .uid == $deploymentUID)) | .metadata.name]' <<<"$replica_sets")
    [[ "$(jq 'length' <<<"$current_replica_sets")" -gt 0 ]] || { fail "ExtProc Deployment $extproc_ref has no ReplicaSet for current revision $revision"; exit 2; }
    pods=$("${KCTL[@]}" -n "$extproc_namespace" get pods -l "$deployment_selector" -o json)
    ready_current_pods=$(jq -c \
      --argjson replicaSets "$current_replica_sets" \
      --arg requested "$EXTPROC_IMAGE" \
      --arg container "$extproc_container" \
      '[.items[]
        | select(.metadata.deletionTimestamp == null)
        | select(any(.metadata.ownerReferences[]?; .kind == "ReplicaSet" and (.name as $owner | ($replicaSets | index($owner)) != null)))
        | select(any(.status.conditions[]?; .type == "Ready" and .status == "True"))
        | select(any(.spec.containers[]?; .name == $container and .image == $requested))
        | select(any(.status.containerStatuses[]?; .name == $container and (.imageID // "") != ""))
        | {name:.metadata.name,uid:.metadata.uid,ownerReplicaSet:([.metadata.ownerReferences[]? | select(.kind == "ReplicaSet") | .name] | first),image:([.spec.containers[]? | select(.name == $container) | .image] | first),imageID:([.status.containerStatuses[]? | select(.name == $container) | .imageID] | first),ready:true}]' <<<"$pods")
    ready_current_count=$(jq 'length' <<<"$ready_current_pods")
    [[ "$ready_current_count" -eq "$desired_replicas" ]] || {
      fail "ExtProc Deployment $extproc_ref has $ready_current_count Ready pods with image IDs from current revision $revision; expected $desired_replicas"
      exit 2
    }
    jq -n --arg requestedImage "$EXTPROC_IMAGE" --arg deployment "$extproc_name" --arg revision "$revision" --argjson generation "$generation" --argjson observedGeneration "$observed_generation" --argjson desiredReplicas "$desired_replicas" --argjson pods "$ready_current_pods" '{requestedImage:$requestedImage,deployment:$deployment,generation:$generation,observedGeneration:$observedGeneration,replicaSetRevision:$revision,desiredReplicas:$desiredReplicas,pods:$pods}' >"$EVIDENCE/extproc-image-${extproc_namespace}.json"
  done
  "${KCTL[@]}" -n maas-system rollout status deployment/katan-a-tenant-b --timeout=180s
  "${KCTL[@]}" -n maas-system rollout status deployment/katan-b-tenant-b --timeout=180s
  "${KCTL[@]}" -n maas-system rollout status deployment/katan-a --timeout=180s
  "${KCTL[@]}" -n maas-system rollout status deployment/katan-b --timeout=180s
  "${KCTL[@]}" -n maas-system rollout status deployment/katan-transition --timeout=180s
  "${KCTL[@]}" -n kuadrant-system rollout status deployment/authorino --timeout=180s
  # The MaaS tenant pipeline may reconcile the transition operand while the
  # other tenant resources are becoming ready. Apply the run-owned IPP
  # transition configuration once more at the settled point, then require the
  # actual IPP-owned route to be attached to the transition Gateway before the
  # qualification can issue its initial request. This is setup convergence,
  # not route deletion or fabricated status.
  for ipp_deployment in payload-processing-transition payload-pre-processing-transition; do
    "${KCTL[@]}" -n maas-system set env deployment/"$ipp_deployment" \
      NAMESPACE=ai-tenant-transition TENANT_NAMESPACE=ai-tenant-transition GATEWAY_NAMESPACE=maas-system GATEWAY_NAME=maas-transition-gateway DISABLE_EXTERNAL_MODEL_CONTROLLER=false
    "${KCTL[@]}" -n maas-system rollout status deployment/"$ipp_deployment" --timeout=180s
  done
  # The initial transition fixture may have been applied before the final
  # writer/Gateway rollout. Re-apply the run-owned CRs at this settled point
  # so the real IPP controller receives an event under the final environment;
  # the generated HTTPRoute is never patched by the harness.
  "${KCTL[@]}" apply -f "$ROOT/test/kind-env/manifests/42-transition-fixtures.yaml" >/dev/null
  transition_route_configured=false
  for _ in $(seq 1 60); do
    transition_parent=$("${KCTL[@]}" -n ai-tenant-transition get httproute transition-model -o jsonpath='{.spec.parentRefs[0].namespace}/{.spec.parentRefs[0].name}' 2>/dev/null || true)
    transition_accepted=$("${KCTL[@]}" -n ai-tenant-transition get httproute transition-model -o jsonpath='{range .status.parents[*].conditions[?(@.type=="Accepted")]}{.status}{end}' 2>/dev/null || true)
    transition_refs=$("${KCTL[@]}" -n ai-tenant-transition get httproute transition-model -o jsonpath='{range .status.parents[*].conditions[?(@.type=="ResolvedRefs")]}{.status}{end}' 2>/dev/null || true)
    if [[ "$transition_parent" == "maas-system/maas-transition-gateway" && "$transition_accepted" == *True* && "$transition_refs" == *True* ]]; then
      transition_route_configured=true
      break
    fi
    sleep 2
  done
  if [[ "$transition_route_configured" != true ]]; then
    # Transition assertions are a separate follow-up suite. Do not make
    # routing-increment provisioning depend on an existing IPP route; retain
    # the observed state for the transition report instead.
    echo "transition_route_configured=false parent=${transition_parent:-unknown} accepted=${transition_accepted:-unknown} resolved_refs=${transition_refs:-unknown}" >"$EVIDENCE/transition-deferred.txt"
  fi
  "${KCTL[@]}" -n kuadrant-system get deployment authorino -o yaml >"$EVIDENCE/authorino-deployment.yaml"
  authorino_pod=$("${KCTL[@]}" -n kuadrant-system get pods -l authorino-resource=authorino -o jsonpath='{.items[0].metadata.name}')
  "${KCTL[@]}" -n kuadrant-system exec "$authorino_pod" -- sha256sum /etc/ssl/certs/maas-api-serving-ca.crt >"$EVIDENCE/authorino-mounted-ca.sha256"
  sha256sum "$db_tmp/ca.crt" >"$EVIDENCE/authorino-expected-ca.sha256"
  "${KCTL[@]}" -n models-as-a-service get externalmodel,externalprovider,httproute,serviceentry,destinationrule,configmap,pod -o yaml >"$EVIDENCE/routing-state.yaml"
  "${KCTL[@]}" get events -A --sort-by=.lastTimestamp >"$EVIDENCE/events.txt"
  printf '{\n  "status":"PASS",\n  "cluster":"kind-%s",\n  "evidence":"%s"\n}\n' "$CLUSTER" "$EVIDENCE" >"$EVIDENCE/result.json"
  exit 0
fi

printf '{\n  "status":"PASS",\n  "cluster":"kind-%s",\n  "evidence":"%s"\n}\n' "$CLUSTER" "$EVIDENCE" >"$EVIDENCE/result.json"
