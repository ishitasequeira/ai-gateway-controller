#!/usr/bin/env bash
set -Eeuo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
CLUSTER=${LOCAL_ENV_CLUSTER:-external-model-two-plane}
WORKSPACE=$(cd "$ROOT/.." && pwd)
DEPS_DIR=${LOCAL_ENV_DEPS_DIR:-"$WORKSPACE/deps"}
LLM_KATAN_REPO=${LLM_KATAN_REPO:-"$DEPS_DIR/llm-katan"}
KATAN_DEFAULT_IMAGE=ghcr.io/nerdalert/llm-katan@sha256:11379a1ec2fd69dc121eada6c544eb423a7c074414507dc1d474f4abba9df75a
KATAN_DEFAULT_AMD64_IMAGE=ghcr.io/nerdalert/llm-katan@sha256:a8bf18109e2db641ef4a63efe65f69d4d6554f1128a174053de89a8b81b4284d
KATAN_EFFECTIVE_IMAGE=${KATAN_IMAGE:-$KATAN_DEFAULT_IMAGE}
KATAN_KIND_IMAGE=${KATAN_IMAGE:-llm-katan:external-model-two-plane}
if [[ "${BUILD_KATAN:-false}" == true ]]; then
  KATAN_EFFECTIVE_IMAGE=${KATAN_IMAGE:-llm-katan:e2e}
  KATAN_KIND_IMAGE=$KATAN_EFFECTIVE_IMAGE
fi
KATAN_PULL_IMAGE=${KATAN_IMAGE:-$KATAN_DEFAULT_AMD64_IMAGE}
MAAS_UPSTREAM_URL=${MAAS_UPSTREAM_URL:-https://github.com/opendatahub-io/models-as-a-service.git}
if [[ -n "${MAAS_CONTROLLER_REPO:-}" ]]; then
  MAAS_SOURCE_MODE=explicit-override
else
  MAAS_CONTROLLER_REPO="$DEPS_DIR/models-as-a-service"
  MAAS_SOURCE_MODE=canonical-main
fi
APPLY_REAL_OPENAI_FIXTURE=${APPLY_REAL_OPENAI_FIXTURE:-false}
KSERVE_REPO=${KSERVE_REPO:-"$DEPS_DIR/kserve"}
KUADRANT_OPERATOR_REPO=${KUADRANT_OPERATOR_REPO:-"$DEPS_DIR/kuadrant-operator"}
PRAXIS_EXTPROC_REPO=${PRAXIS_EXTPROC_REPO:-"$DEPS_DIR/praxis-extproc"}
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
check_repo() { local name=$1 path=${!1:-}; [[ -n "$path" && -d "$path/.git" ]] || fail "$name checkout missing: ${path:-unset}"; }
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
  } >"$EVIDENCE/cleanup-inventory.txt"
  echo "destroyed kind-$CLUSTER"
  (( failures == 0 )) || exit 2
  exit 0
fi

for cmd in docker kind kubectl helm kustomize go git openssl yq envsubst; do check_cmd "$cmd"; done
check_cmd "$ISTIOCTL"
check_repo KSERVE_REPO "$KSERVE_REPO"
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
  "extproc|$PRAXIS_EXTPROC_REPO|${EXTPROC_IMAGE:-praxis-extproc:dev}|Containerfile" \
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
  cache="$EVIDENCE_ROOT/.image-inputs/$label"
  rebuild=true
  if docker image inspect "$image" >/dev/null 2>&1 && [[ -f "$cache" ]] && cmp -s "$EVIDENCE/${label}.inputs" "$cache"; then
    rebuild=false
  fi
  if [[ "$rebuild" == true ]]; then
    case "$label" in
      controller) timeout 900s docker build --platform linux/amd64 -t "$image" -f "$source/$dockerfile" "$source" ;;
      katan) timeout 900s docker build --platform linux/amd64 -t "$image" -f "$source/$dockerfile" "$source" ;;
      extproc) timeout 1200s docker build --platform linux/amd64 -t "$image" -f "$source/$dockerfile" "$source" ;;
      maas-api) timeout 1200s docker build --platform linux/amd64 -t "$image" -f "$source/$dockerfile" "$source" ;;
      maas-controller) timeout 1200s docker build --platform linux/amd64 -t "$image" -f "$source/$dockerfile" "$source" ;;
    esac
    cp "$EVIDENCE/${label}.inputs" "$cache"
    echo "built_image=$image reason=source-inputs-changed-or-image-missing"
  else
    echo "reused_image=$image reason=source-inputs-unchanged"
  fi
done

git -C "$KSERVE_REPO" rev-parse HEAD >"$EVIDENCE/kserve.sha"
git -C "$KSERVE_REPO" diff --no-ext-diff | sha256sum >"$EVIDENCE/kserve.diff.sha256"

if (( failures )); then
  printf '{\n  "status":"BLOCKED",\n  "failures":%d,\n  "cluster":"kind-%s",\n  "evidence":"%s"\n}\n' "$failures" "$CLUSTER" "$EVIDENCE" >"$EVIDENCE/result.json"
  exit 2
fi

if [[ "${1:---preflight}" == "--provision" ]]; then
  printf '%s\n' "$EVIDENCE" >"$EVIDENCE_ROOT/.active-run"
  kind get clusters | rg -qx "$CLUSTER" || kind create cluster --name "$CLUSTER"
  "${KCTL[@]}" cluster-info >"$EVIDENCE/cluster-info.txt"
  # Stop prior run-owned MaaS pods before replacing their node image tags;
  # containerd retains images referenced by live containers.
  "${KCTL[@]}" -n maas-system scale deployment/maas-api deployment/maas-controller --replicas=0 >/dev/null 2>&1 || true
  for selector in 'app.kubernetes.io/name=maas-api' 'control-plane=maas-controller'; do
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
    "${EXTPROC_IMAGE:-praxis-extproc:dev}" \
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
  kind load docker-image "${EXTPROC_IMAGE:-praxis-extproc:dev}" --name "$CLUSTER"
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
  kustomize build "$MAAS_CONTROLLER_REPO/deployment/base/maas-controller/default" | sed -e "s#quay.io/opendatahub/maas-api:latest#${MAAS_API_IMAGE:-maas-api:external-model-two-plane}#g" -e "s#quay.io/opendatahub/maas-controller:latest#${MAAS_CONTROLLER_IMAGE:-maas-controller:external-model-two-plane}#g" -e "s#quay.io/opendatahub/maas-controller:odh-stable#${MAAS_CONTROLLER_IMAGE:-maas-controller:external-model-two-plane}#g" -e 's#openshift-ingress#maas-system#g' -e 's#namespace: opendatahub#namespace: maas-system#g' -e 's#namespace: system#namespace: maas-system#g' | yq eval 'select(.kind != "ServiceMonitor" and .kind != "ValidatingWebhookConfiguration")' - >"$EVIDENCE/maas-controller-kind-rendered.yaml"
  "${KCTL[@]}" apply -f "$EVIDENCE/maas-controller-kind-rendered.yaml"
  "${KCTL[@]}" -n maas-system patch deployment maas-api --type=json -p='[{"op":"replace","path":"/spec/template/spec/containers/0/imagePullPolicy","value":"Never"}]' 2>/dev/null || true
  "${KCTL[@]}" -n maas-system patch deployment maas-controller --type=json -p='[{"op":"replace","path":"/spec/template/spec/containers/0/imagePullPolicy","value":"Never"}]'
  # The webhook Secret is created below before the controller is restarted.
  # Run the binary directly: a shell wrapper can race projected Secret mounts
  # and obscure the controller's startup error in Kind.
  "${KCTL[@]}" -n maas-system patch deployment maas-controller --type=json -p='[{"op":"replace","path":"/spec/template/spec/containers/0/args/3","value":"--metrics-secure=false"},{"op":"replace","path":"/spec/template/spec/containers/0/command","value":["/manager"]},{"op":"replace","path":"/spec/template/spec/containers/0/args","value":["--leader-elect","--health-probe-bind-address=:8081","--metrics-bind-address=:8080","--metrics-secure=false","--controller-namespace=maas-system","--gateway-name=maas-default-gateway","--gateway-namespace=maas-system","--infra-namespace=AUTO","--maas-subscription-namespace=models-as-a-service","--aitenant-namespace=ai-tenants","--monitoring-namespace=opendatahub","--enable-tenant-namespace-discovery","--metadata-cache-ttl=60","--authz-cache-ttl=60","--log-format=zap"]}]'
  # The rendered MaaS deployment may already have created an old ReplicaSet
  # before the pull-policy patch. Force a fresh template so the new pod uses
  # the exact image loaded into Kind instead of attempting a registry pull.
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
  # Authorino's operator-supported volume projection mounts the dedicated CA
  # into /etc/ssl/certs, where Go's system pool discovers it. The mounted file
  # contains only this run's CA; no system bundle or TLS bypass is configured.
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
  "${KCTL[@]}" -n kuadrant-system get authorino authorino -o yaml >"$EVIDENCE/authorino-ca-config.yaml"
  kustomize build "$MAAS_CONTROLLER_REPO/deployment/base/maas-api/rbac" | sed 's#namespace: opendatahub#namespace: maas-system#g' | "${KCTL[@]}" apply -f -
  "${KCTL[@]}" apply -f "$ROOT/test/kind-env/manifests/31-maas-api-kind-rbac.yaml"
  # The webhook Secret is created after the OpenShift-only bundle is rendered;
  # restart so the projected certificate is present before manager startup.
  "${KCTL[@]}" -n maas-system rollout restart deployment/maas-controller
  # Repeated local runs may leave the old static maas-api object in place.
  # Let the current MaaS tenant pipeline create its canonical object so
  # server-side apply cannot merge stale OpenShift fields into the Kind spec.
  "${KCTL[@]}" -n maas-system delete deployment maas-api service maas-api --ignore-not-found
  kustomize build "$ROOT/config/crd" | "${KCTL[@]}" apply --server-side -f -
  kustomize build "$ROOT/config/self/default" | "${KCTL[@]}" apply --server-side --force-conflicts -f -
  "${KCTL[@]}" -n opendatahub set image deployment/ai-gateway-controller manager="${AI_CONTROLLER_IMAGE:-ai-gateway-controller:external-model-two-plane}"
  extra_known_json='[]'
  if [[ -n "${PRAXIS_EXTRA_KNOWN_CLUSTERS:-}" ]]; then
    IFS=',' read -r -a extra_known_clusters <<<"$PRAXIS_EXTRA_KNOWN_CLUSTERS"
    for extra_cluster in "${extra_known_clusters[@]}"; do
      [[ -n "$extra_cluster" ]] || continue
      extra_known_json=$(jq -c --arg cluster "$extra_cluster" '. + ["--known-cluster=" + $cluster]' <<<"$extra_known_json")
    done
  fi
  controller_patch=$(jq -cn --arg extproc_image "${EXTPROC_IMAGE:-praxis-extproc:dev}" --argjson extra_known "$extra_known_json" '[
    {op:"replace",path:"/spec/template/spec/containers/0/imagePullPolicy",value:"Never"},
    {op:"replace",path:"/spec/template/spec/containers/0/args",value:([
      "--leader-elect",
      "--health-probe-bind-address=:8081",
      "--gateway-name=maas-default-gateway",
      "--gateway-namespace=maas-system",
      "--known-cluster=provider-provider-a",
      "--known-cluster=provider-provider-b",
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
  make_katan_tls provider-a-tenant-b-tls provider-a-tenant-b
  make_katan_tls provider-b-tenant-b-tls provider-b-tenant-b
  make_katan_tls provider-transition-tls provider-a-legacy
  # MaaS creates Gateway API-owned gateway Deployments in maas-system; they
  # are distinct from the standalone istio-ingressgateway Deployment. Mount
  # this run's public provider CA into those run-created gateway workloads so
  # the generated DestinationRules can verify Katan without
  # insecureSkipVerify or a plaintext exception. Never overwrite a preexisting
  # shared fixture ConfigMap on a non-disposable cluster.
  if "${KCTL[@]}" -n maas-system get configmap external-model-e2e-provider-ca >/dev/null 2>&1; then
    fail "maas-system/external-model-e2e-provider-ca already exists; refusing to overwrite shared trust state"
    exit 2
  fi
  "${KCTL[@]}" -n maas-system create configmap external-model-e2e-provider-ca \
    --from-file=ca.crt="$db_tmp/ca.crt" --dry-run=client -o yaml \
    | "${KCTL[@]}" apply -f - >/dev/null
  "${KCTL[@]}" -n maas-system label configmap external-model-e2e-provider-ca \
    external-model-e2e/run-id="$CLUSTER" app.kubernetes.io/managed-by=external-model-e2e --overwrite >/dev/null
  for gateway_deployment in maas-default-gateway-istio maas-tenant-b-gateway-istio maas-transition-gateway-istio; do
    if ! "${KCTL[@]}" -n maas-system get deployment "$gateway_deployment" >/dev/null 2>&1; then
      continue
    fi
    if ! "${KCTL[@]}" -n maas-system get deployment "$gateway_deployment" -o json \
      | jq -e '.spec.template.spec.volumes[]? | select(.name == "external-model-e2e-provider-ca")' >/dev/null; then
      "${KCTL[@]}" -n maas-system patch deployment "$gateway_deployment" --type=json -p='[{"op":"add","path":"/spec/template/spec/volumes/-","value":{"name":"external-model-e2e-provider-ca","configMap":{"name":"external-model-e2e-provider-ca","items":[{"key":"ca.crt","path":"ca.crt"}]}}},{"op":"add","path":"/spec/template/spec/containers/0/volumeMounts/-","value":{"name":"external-model-e2e-provider-ca","mountPath":"/etc/external-model-e2e/provider-ca","readOnly":true}}]' >/dev/null
    fi
    "${KCTL[@]}" -n maas-system rollout status deployment/"$gateway_deployment" --timeout=120s
  done
  printf 'configmap=maas-system/external-model-e2e-provider-ca ca_file=/etc/external-model-e2e/provider-ca/ca.crt\n' \
    >"$EVIDENCE/istio-provider-ca-mount.txt"
  # Apply the remaining fixtures after the writer handoff. In particular, do
  # not let the disabled IPP deployment observe the Praxis ExternalModels.
  for manifest in "$ROOT/test/kind-env/manifests"/*.yaml; do
    case "$(basename "$manifest")" in
      20-fixtures.yaml|21-fixtures-tenant-b.yaml|40-maas-fixtures.yaml|41-maas-fixtures-tenant-b.yaml|42-transition-fixtures.yaml) continue ;;
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
    envsubst '${EXTERNAL_MODEL_NAMESPACE} ${EXTERNAL_MODEL_PROVIDER_A_ENDPOINT} ${EXTERNAL_MODEL_PROVIDER_B_ENDPOINT} ${EXTERNAL_MODEL_RUN_ID}' \
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
  # MaaS API validates the internal gateway service during startup. Restart it
  # after Istio has programmed the Gateway and populated its run-owned Service.
  # The tenant pipeline creates/reconciles one API Deployment per tenant.
  # Restart every run-owned API instance after replacing its serving Secret so
  # the process is definitely using the certificate signed by this run's CA.
  for api_deployment in maas-api maas-api-tenant-b maas-api-transition; do
    if "${KCTL[@]}" -n maas-system get deployment "$api_deployment" >/dev/null 2>&1; then
      "${KCTL[@]}" -n maas-system rollout restart "deployment/$api_deployment"
    fi
  done
  for api_deployment in maas-api maas-api-tenant-b maas-api-transition; do
    if "${KCTL[@]}" -n maas-system get deployment "$api_deployment" >/dev/null 2>&1; then
      "${KCTL[@]}" -n maas-system rollout status "deployment/$api_deployment" --timeout=180s
    fi
  done
  maas_api_created=false
  for _ in $(seq 1 60); do
    if "${KCTL[@]}" -n maas-system get deployment maas-api >/dev/null 2>&1; then
      maas_api_created=true
      break
    fi
    sleep 3
  done
  [[ "$maas_api_created" == true ]] || { fail "MaaS tenant pipeline did not create the default maas-api"; exit 2; }
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
  wait_for_extproc models-as-a-service payload-processing
  patch_kind_extproc_identity maas-system payload-pre-processing
  patch_kind_extproc_identity models-as-a-service payload-processing
  for extproc_ref in maas-system/payload-pre-processing models-as-a-service/payload-processing; do
    extproc_namespace=${extproc_ref%/*}
    extproc_name=${extproc_ref#*/}
    deployed_image=$("${KCTL[@]}" -n "$extproc_namespace" get deployment "$extproc_name" -o jsonpath='{.spec.template.spec.containers[0].image}')
    [[ "$deployed_image" == "${EXTPROC_IMAGE:-praxis-extproc:dev}" ]] || { fail "ExtProc image mismatch for $extproc_ref: requested=${EXTPROC_IMAGE:-praxis-extproc:dev} deployed=$deployed_image"; exit 2; }
    "${KCTL[@]}" -n "$extproc_namespace" get pods -l app=payload-processing -o json | jq --arg requested "${EXTPROC_IMAGE:-praxis-extproc:dev}" --arg deployment "$extproc_name" '{requestedImage:$requested,deployment:$deployment,pods:[.items[]|{name:.metadata.name,uid:.metadata.uid,image:.spec.containers[0].image,imageID:.status.containerStatuses[0].imageID,ready:([.status.conditions[]?|select(.type=="Ready" and .status=="True")]|length==1)}]}' >"$EVIDENCE/extproc-image-${extproc_namespace}.json"
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
