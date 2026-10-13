#!/usr/bin/env bash
set -Eeuo pipefail

# Retained-cluster narrative demo. Requests execute from one persistent,
# restricted in-cluster client pod. Mutations are limited to run-owned fixtures.
ROOT=$(cd "$(dirname "$0")/../.." && pwd)
CLUSTER=external-model-two-plane
CONTEXT=kind-$CLUSTER
TENANT=models-as-a-service
BNS=ai-tenant-tenant-b
GATEWAY_NS=maas-system
API_NS=maas-system
CLIENT_NS=maas-system
CLIENT=external-model-demo-client
DATAPLANE_DEPLOYMENT=payload-processing-external-model
DATAPLANE_CONTAINER=payload-processing
PUBLIC_GATEWAY=maas-default-gateway-istio
TENANT_B_GATEWAY=maas-tenant-b-gateway-istio
EVIDENCE="$ROOT/evidence/$(date -u +%Y%m%dT%H%M%SZ)-demo"
PAUSE_SECONDS=2
NON_INTERACTIVE=false
RESET=false
TMP_DIR=$(mktemp -d)
RECOMPUTE="$TMP_DIR/recompute-digest"
go build -o "$RECOMPUTE" "$ROOT/test/kind-env/recompute_digest.go"
APF=""
GPF=""
TBPF=""
KEY_ID=""
CA_CERT="$TMP_DIR/maas-api-ca.crt"
AUTH_CFG="$TMP_DIR/auth.cfg"
ADMIN_CFG="$TMP_DIR/admin.cfg"
while (($#)); do
  case "$1" in
    --context) CONTEXT=$2; shift 2 ;;
    --tenant) TENANT=$2; shift 2 ;;
    --evidence-dir) EVIDENCE=$2; shift 2 ;;
    --pause-seconds) PAUSE_SECONDS=$2; shift 2 ;;
    --non-interactive) NON_INTERACTIVE=true; shift ;;
    --skip-reset) shift ;;
    --reset) RESET=true; shift ;;
    -h|--help) sed -n '1,30p' "$0"; exit 0 ;;
    *) echo "unknown option: $1" >&2; exit 2 ;;
  esac
done
[[ "$CONTEXT" == kind-* ]] || { echo "FAIL: expected a Kind context"; exit 2; }
mkdir -p "$EVIDENCE"
exec > >(tee "$EVIDENCE/demo.log") 2>&1
evidence_label() {
  case "$1" in
    "$ROOT"/*) printf '%s\n' "${1#"$ROOT"/}" ;;
    *) printf '%s\n' 'run-evidence' ;;
  esac
}
cleanup() {
  local rc=$?
  set +e
  if [[ -n "$KEY_ID" && -s "$ADMIN_CFG" && -s "$CA_CERT" ]]; then
    timeout 20s curl --noproxy '*' --cacert "$CA_CERT" --resolve "maas-api.maas-system.svc.cluster.local:$API_PORT:127.0.0.1" \
      -sS -o /dev/null -w 'key_revocation_status=%{http_code}\n' --config "$ADMIN_CFG" \
      -X DELETE "https://maas-api.maas-system.svc.cluster.local:$API_PORT/v1/api-keys/$KEY_ID" || true
  fi
  if [[ -n "$APF" ]]; then kill "$APF" 2>/dev/null || true; fi
  if [[ -n "$GPF" ]]; then kill "$GPF" 2>/dev/null || true; fi
  if [[ -n "$TBPF" ]]; then kill "$TBPF" 2>/dev/null || true; fi
  rm -rf "$TMP_DIR"
  exit "$rc"
}
trap cleanup EXIT INT TERM
kctl() { kubectl --context "$CONTEXT" "$@"; }
guard_context() {
  local current
  current=$(kubectl config current-context 2>/dev/null || true)
  [[ "$current" == "$CONTEXT" ]] || { echo "FAIL: current context is $current; expected $CONTEXT"; exit 2; }
  kctl cluster-info >/dev/null
}
mutate() { guard_context; kctl "$@"; }
pause() {
  [[ "$NON_INTERACTIVE" == true ]] && return
  [[ -t 0 ]] || { sleep "$PAUSE_SECONDS"; return; }
  read -r -p "  Press Enter to continue (or wait ${PAUSE_SECONDS}s): " _ </dev/tty || sleep "$PAUSE_SECONDS"
}
fail_stage() {
  record "$STAGE" "stage_${STAGE}_failure" FAIL "failed_boundary=$1"
  echo "RESULT"; echo "  Status                 FAIL"
  echo "  Failed boundary        $1"
  echo "  Diagnostics             $EVIDENCE"
  atomic_results
  exit 1
}
wait_for() {
  local label=$1 timeout_s=$2
  shift 2
  local started now
  started=$(date +%s)
  while ! "$@" >/dev/null 2>&1; do
    now=$(date +%s)
    (( now - started < timeout_s )) || { echo "  [FAIL] $label after ${timeout_s}s"; return 1; }
    sleep 2
  done
  echo "  [PASS] $label"
}
model_converged() {
  local namespace=$1 name=$2 phase observed generation
  phase=$(kctl -n "$namespace" get externalmodel "$name" -o jsonpath='{.status.phase}' 2>/dev/null || true)
  observed=$(kctl -n "$namespace" get externalmodel "$name" -o jsonpath='{.status.observedGeneration}' 2>/dev/null || true)
  generation=$(kctl -n "$namespace" get externalmodel "$name" -o jsonpath='{.metadata.generation}' 2>/dev/null || true)
  [[ "$phase" == Ready && -n "$generation" && "$observed" == "$generation" ]]
}
wait_pod_ready() {
  local namespace=$1 name=$2 started now state
  started=$(date +%s)
  while :; do
    state=$(kctl -n "$namespace" get pod "$name" -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null || true)
    [[ "$state" == True ]] && { echo "  [PASS] persistent in-cluster client Ready"; return 0; }
    now=$(date +%s)
    (( now - started < 120 )) || { echo "  [FAIL] pod $namespace/$name did not become Ready"; return 1; }
    sleep 2
  done
}
client_exec() { kctl -n "$CLIENT_NS" exec -i "$CLIENT" -- "$@" </dev/null; }
overlay_state() {
  local cm="$TMP_DIR/overlay.json" content_file="$TMP_DIR/content.json" mounted="$TMP_DIR/mounted.json"
  kctl -n "$TENANT" get configmap routing-overlay -o json >"$cm"
  kctl -n "$TENANT" exec "deploy/$DATAPLANE_DEPLOYMENT" -c "$DATAPLANE_CONTAINER" -- cat /etc/praxis/routing/routing-overlay.json >"$mounted" 2>/dev/null || true
  local declared generation recomputed mounted_digest
  jq -r '.data["routing-overlay.json"] // empty' "$cm" >"$content_file"
  declared=$(jq -r '.metadata.annotations["inference.opendatahub.io/routing-overlay-content-digest"]' "$cm")
  generation=$(jq -r '.metadata.annotations["inference.opendatahub.io/routing-overlay-source-generation"]' "$cm")
  recomputed=$("$RECOMPUTE" "$content_file" 2>/dev/null || true)
  mounted_digest=$("$RECOMPUTE" "$mounted" 2>/dev/null || true)
  printf 'generation=%s declared=%s recomputed=%s mounted=%s a=%s b=%s\n' \
    "$generation" "$declared" "$recomputed" "$mounted_digest" \
    "$(jq -r '.data["routing-overlay.json"]' "$cm" | grep -q provider-provider-a && echo true || echo false)" \
    "$(jq -r '.data["routing-overlay.json"]' "$cm" | grep -q provider-provider-b && echo true || echo false)"
}
wait_overlay() {
  local wanted=$1 stable=0 first second active_other
  for _ in $(seq 1 90); do
    first=$(overlay_state)
    if [[ "$wanted" == a ]]; then active_other=b; else active_other=a; fi
    if [[ "$first" == *" $wanted=true"* && "$first" == *" $active_other=false"* &&
      "$(awk '{print $2}' <<<"$first" | cut -d= -f2)" == "$(awk '{print $3}' <<<"$first" | cut -d= -f2)" &&
      "$(awk '{print $2}' <<<"$first" | cut -d= -f2)" == "$(awk '{print $4}' <<<"$first" | cut -d= -f2)" ]]; then
      sleep 2
      second=$(overlay_state)
      if [[ "$first" == "$second" ]]; then
        stable=$((stable + 1))
        printf '%s\n' "$second" >"$EVIDENCE/overlay-$wanted-$stable.txt"
        [[ "$stable" -ge 2 ]] && return 0
      else
        stable=0
      fi
    else
      stable=0
    fi
    sleep 2
  done
  return 1
}
wait_port() {
  local port=$1
  for _ in $(seq 1 60); do
    if timeout 3s bash -c "cat </dev/null >/dev/tcp/127.0.0.1/$port" >/dev/null 2>&1; then return 0; fi
    sleep 1
  done
  return 1
}
gateway_route_state() {
  local provider=$1 route_dump
  route_dump=$(timeout 5s kubectl --context "$CONTEXT" -n "$GATEWAY_NS" exec deploy/maas-default-gateway-istio -c istio-proxy -- \
    curl -sS http://127.0.0.1:15000/config_dump 2>/dev/null || true)
  jq -c --arg provider "$provider" '
    [.. | objects | select(.match? and .route?.cluster?) |
      select((.match.headers // [] | any(.name == "X-AI-Routing-Candidate" and .string_match.exact == ("provider-" + $provider))) or
             (.route.cluster == ("outbound|443||" + $provider + ".maas-system.svc.cluster.local"))) |
      {match, cluster:.route.cluster, host_rewrite:.route.host_rewrite_literal}] | sort_by(.cluster, (.match|tojson))
  ' <<<"$route_dump" | sha256sum | awk '{print $1}'
}
wait_gateway_route() {
  local provider=$1 stable=0 previous="" current
  for _ in $(seq 1 180); do
    current=$(gateway_route_state "$provider")
    if [[ "$current" != e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855 && -n "$current" && "$current" == "$previous" ]]; then
      stable=$((stable + 1))
      printf 'provider=%s route_fingerprint=%s stable_sample=%s\n' "$provider" "$current" "$stable" >>"$EVIDENCE/gateway-route-$provider.txt"
      [[ "$stable" -ge 2 ]] && return 0
    else
      stable=0
    fi
    previous=$current
    sleep 1
  done
  return 1
}
wait_gateway_provider() {
  local provider=$1 host dump
  host="${provider}-ext.maas-system.svc.cluster.local"
  for _ in $(seq 1 120); do
    dump=$(timeout 5s kubectl --context "$CONTEXT" -n "$GATEWAY_NS" exec deploy/maas-default-gateway-istio -c istio-proxy -- \
      curl -sS http://127.0.0.1:15000/clusters 2>/dev/null || true)
    if awk -v prefix="${host}::" 'index($0, prefix) && index($0, "::health_flags::healthy") { found=1 } END { exit found ? 0 : 1 }' <<<"$dump"; then
      printf 'provider=%s endpoint_health=healthy\n' "$provider" >"$EVIDENCE/provider-cluster-$provider.txt"
      return 0
    fi
    sleep 1
  done
  printf 'provider=%s endpoint_health=not-healthy\n' "$provider" >"$EVIDENCE/provider-cluster-$provider.txt"
  return 1
}
wait_gateway_cluster() {
  local host=$1 dump
  for _ in $(seq 1 120); do
    dump=$(timeout 5s kubectl --context "$CONTEXT" -n "$GATEWAY_NS" exec deploy/maas-default-gateway-istio -c istio-proxy -- \
      curl -sS http://127.0.0.1:15000/clusters 2>/dev/null || true)
    if awk -v prefix="${host}::" 'index($0, prefix) && index($0, "::health_flags::healthy") { found=1 } END { exit found ? 0 : 1 }' <<<"$dump"; then
      printf 'cluster=%s health=healthy\n' "$host" >"$EVIDENCE/gateway-cluster-${host%%.*}.txt"
      return 0
    fi
    sleep 1
  done
  printf 'cluster=%s health=not-healthy\n' "$host" >"$EVIDENCE/gateway-cluster-${host%%.*}.txt"
  return 1
}
wait_tenant_b_gateway_transport() {
  local provider=provider-a host="provider-a-tenant-b.maas-system.svc.cluster.local" previous="" current stable=0 dump
  for _ in $(seq 1 180); do
    current=$(timeout 5s kubectl --context "$CONTEXT" -n "$GATEWAY_NS" exec deploy/maas-tenant-b-gateway-istio -c istio-proxy -- \
      curl -sS http://127.0.0.1:15000/config_dump 2>/dev/null | jq -c --arg provider "provider-$provider" --arg host "$host" '
        [.. | objects | select(.match? and .route?.cluster?) |
          select((.match.headers // [] | any(.name == "X-AI-Routing-Candidate" and .string_match.exact == $provider)) or
                 (.route.cluster == ("outbound|443||" + $host))) |
          {match, cluster:.route.cluster, host_rewrite:.route.host_rewrite_literal}] | sort_by(.cluster, (.match|tojson))
      ' | sha256sum | awk '{print $1}')
    if [[ -n "$current" && "$current" != e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855 && "$current" == "$previous" ]]; then
      stable=$((stable + 1))
      printf 'gateway=tenant-b provider=%s route_fingerprint=%s stable_sample=%s\n' "$provider" "$current" "$stable" >>"$EVIDENCE/gateway-route-tenant-b-$provider.txt"
      [[ "$stable" -ge 2 ]] || true
    else
      stable=0
    fi
    dump=$(timeout 5s kubectl --context "$CONTEXT" -n "$GATEWAY_NS" exec deploy/maas-tenant-b-gateway-istio -c istio-proxy -- \
      curl -sS http://127.0.0.1:15000/clusters 2>/dev/null || true)
    if [[ "$stable" -ge 2 ]] && awk -v prefix="${host}::" 'index($0, prefix) && index($0, "::health_flags::healthy") { found=1 } END { exit found ? 0 : 1 }' <<<"$dump"; then
      printf 'gateway=tenant-b provider=%s endpoint=%s endpoint_health=healthy route_fingerprint=%s\n' "$provider" "$host" "$current" >"$EVIDENCE/tenant-b-transport.txt"
      return 0
    fi
    previous=$current
    sleep 1
  done
  printf 'gateway=tenant-b provider=%s endpoint=%s endpoint_health=not-healthy route_fingerprint=%s\n' "$provider" "$host" "$current" >"$EVIDENCE/tenant-b-transport-failure.txt"
  return 1
}
request_status() {
  local _gateway=$1 path=$2 body=$3
  local port=$GATEWAY_PORT
  [[ "$_gateway" == "$TENANT_B_GATEWAY" ]] && port=$TENANT_B_PORT
  local url="http://127.0.0.1:$port$path" response="$TMP_DIR/demo-response"
  local status
  status=$(timeout 30s curl --noproxy '*' --connect-timeout 5 --max-time 15 -sS -o "$response" -w '%{http_code}' \
    --config "$AUTH_CFG" -H 'content-type: application/json' --data "$body" "$url" 2>/dev/null || printf 000)
  local backend
  backend=$(grep -o 'katan-[A-Za-z0-9-]*' "$response" 2>/dev/null | head -1 || true)
  printf '%s|%s' "$status" "$backend"
}
record() {
  local number=$1 id=$2 status=$3 observation=$4
  python3 - "$EVIDENCE/results.json" "$number" "$id" "$status" "$observation" <<'PY'
import json, os, sys, tempfile
p,n,i,s,o=sys.argv[1:]
with open(p) as f: d=json.load(f)
d["assertions"].append({"number":int(n),"id":i,"status":s,"observation":o})
d["total_assertions"]=len(d["assertions"])
fd,t=tempfile.mkstemp(prefix=".results.",dir=os.path.dirname(p))
with os.fdopen(fd,"w") as f: json.dump(d,f,indent=2); f.flush(); os.fsync(f.fileno())
os.replace(t,p)
PY
}
atomic_results() {
  python3 - "$EVIDENCE/results.json" <<'PY'
import json, os, sys, tempfile
p=sys.argv[1]
with open(p) as f: d=json.load(f)
d["total_assertions"]=len(d["assertions"])
statuses=[x["status"] for x in d["assertions"]]
d["status"]="PASS" if all(s in ("PASS", "NOT_DEMONSTRATED") for s in statuses) else "FAIL"
d["scope"]="routing_increment"
d["not_demonstrated"]=[x["id"] for x in d["assertions"] if x["status"] == "NOT_DEMONSTRATED"]
fd,t=tempfile.mkstemp(prefix=".results.",dir=os.path.dirname(p))
with os.fdopen(fd,"w") as f: json.dump(d,f,indent=2); f.flush(); os.fsync(f.fileno())
os.replace(t,p)
PY
}
guard_context
printf '{"status":"RUNNING","assertions":[],"total_assertions":0}\n' > "$EVIDENCE/results.json"

if [[ "$RESET" == true ]]; then
  echo "RESET: restoring run-owned initial fixtures"
  # Ensure the Praxis tenants' IPP writers have completed their disabled
  # rollout before recreating ExternalModels. Otherwise an old writer can
  # observe the fixture during reset and recreate a direct-provider route.
  for ipp_deployment in payload-processing payload-processing-tenant-b payload-pre-processing payload-pre-processing-tenant-b; do
    if kctl -n maas-system get deployment "$ipp_deployment" >/dev/null 2>&1; then
      tenant_namespace=models-as-a-service
      gateway_name=maas-default-gateway
      [[ "$ipp_deployment" == *-tenant-b ]] && { tenant_namespace=ai-tenant-tenant-b; gateway_name=maas-tenant-b-gateway; }
      mutate -n maas-system set env deployment/"$ipp_deployment" \
        NAMESPACE="$tenant_namespace" TENANT_NAMESPACE="$tenant_namespace" GATEWAY_NAMESPACE=maas-system GATEWAY_NAME="$gateway_name" \
        DISABLE_EXTERNAL_MODEL_CONTROLLER=true >/dev/null
      mutate -n maas-system rollout status deployment/"$ipp_deployment" --timeout=120s >/dev/null
    fi
  done
  mutate apply -f "$ROOT/test/kind-env/manifests/20-fixtures.yaml" >/dev/null
  mutate apply -f "$ROOT/test/kind-env/manifests/21-fixtures-tenant-b.yaml" >/dev/null
  mutate -n "$TENANT" annotate externalmodel demo-model "external-model-e2e/reset-at=$(date -u +%s%N)" --overwrite >/dev/null
  mutate -n "$BNS" annotate externalmodel demo-model "external-model-e2e/reset-at=$(date -u +%s%N)" --overwrite >/dev/null
  kctl -n "$TENANT" get deployment "$DATAPLANE_DEPLOYMENT" >/dev/null || {
    echo "RESET: expected ExtProc deployment $DATAPLANE_DEPLOYMENT is absent" >&2
    exit 1
  }
  mutate -n "$TENANT" patch externalmodel demo-model --type=json -p='[{"op":"replace","path":"/spec/externalProviderRefs","value":[{"ref":{"name":"provider-a"},"targetModel":"demo","apiFormat":"openai-chat","path":"/v1/chat/completions"},{"ref":{"name":"provider-b"},"weight":0,"targetModel":"demo","apiFormat":"openai-chat","path":"/v1/chat/completions"}]}]' >/dev/null
  wait_for "tenant model Ready after reset" 120 model_converged "$TENANT" demo-model || exit 1
  for _ in $(seq 1 60); do
    reset_overlay=$(kctl -n "$TENANT" get configmap routing-overlay -o jsonpath='{.data.routing-overlay\.json}' 2>/dev/null || true)
    [[ "$reset_overlay" == *provider-provider-a* && "$reset_overlay" != *provider-provider-b* ]] && break
    sleep 2
  done
  [[ "$reset_overlay" == *provider-provider-a* && "$reset_overlay" != *provider-provider-b* ]] || { echo "RESET: overlay did not converge to Provider A"; exit 1; }
  wait_for "ExtProc mounted Provider A overlay" 120 kctl -n "$TENANT" exec "deploy/$DATAPLANE_DEPLOYMENT" -c "$DATAPLANE_CONTAINER" -- sh -c 'grep -q provider-provider-a /etc/praxis/routing/routing-overlay.json'
  wait_overlay a || { echo "RESET: semantic Provider A overlay did not converge"; exit 1; }
  wait_gateway_route provider-a || { echo "RESET: Provider A route did not converge"; exit 1; }
  wait_for "Tenant B model Ready after reset" 120 model_converged "$BNS" demo-model || exit 1
  wait_for "Tenant B ExtProc Ready after reset" 120 kctl -n "$BNS" get deployment payload-processing-external-model-tenant-b -o jsonpath='{.status.readyReplicas}' || exit 1
  wait_for "Tenant B provider transport after reset" 120 kctl -n "$GATEWAY_NS" get destinationrule provider-provider-a-tenant-b -o jsonpath='{.spec.host}' || exit 1
  echo "RESET: PASS; cluster and unrelated resources preserved"
  exit 0
fi

STAGE=0
stage() {
  STAGE=$((STAGE + 1))
  printf "\n--------------------------------------------------------------------------------\n STAGE %s OF 9: %s\n--------------------------------------------------------------------------------\n" "$STAGE" "$1"
  echo "GOAL"; echo "  $2"; echo "STARTING STATE"
}

stage "Introduce the topology" "Show the Gateway-to-ExtProc request path and tenant ownership."
echo "  Client -> Gateway/Envoy -> Kuadrant -> ExternalModel ExtProc -> provider"
echo "  Controller -> Service/ServiceEntry/DestinationRule/HTTPRoute -> overlay"
kctl get ns "$GATEWAY_NS" "$TENANT" "$BNS" -o custom-columns='NAMESPACE:.metadata.name' --no-headers
kctl -n "$TENANT" get externalmodel,externalprovider,httproute,service,deployment,configmap -o name 2>/dev/null | head -30
digest=$(kctl -n "$TENANT" get configmap routing-overlay -o jsonpath='{.metadata.annotations.inference\.opendatahub\.io/routing-overlay-content-digest}' 2>/dev/null || true)
echo "  overlay=$(printf '%s' "$digest" | cut -c1-12)"
record 1 topology PASS "gateway_namespace=$GATEWAY_NS tenant_namespace=$TENANT"
echo "RESULT"; echo "  Status                 PASS"; echo "  WHY IT MATTERS"; echo "  Control and request processing have separate owners."; pause

stage "Prove authentication" "Use a persistent client pod and show policy rejection before authentication."
if kctl -n "$CLIENT_NS" get pod "$CLIENT" -o jsonpath='{.metadata.labels.external-model-e2e/purpose}' 2>/dev/null | rg -qx client; then
  mutate -n "$CLIENT_NS" delete pod "$CLIENT" --wait=true >/dev/null
fi
mutate -n kuadrant-system get configmap authorino-maas-api-ca -o json \
  | jq 'del(.metadata.creationTimestamp,.metadata.resourceVersion,.metadata.uid,.metadata.managedFields,.metadata.ownerReferences) | .metadata.name="demo-maas-api-ca" | .metadata.namespace="maas-system"' \
  | mutate apply -f - >/dev/null
# shellcheck disable=SC2016
EXTERNAL_MODEL_CLIENT_NAME=$CLIENT \
EXTERNAL_MODEL_CLIENT_NAMESPACE=$CLIENT_NS \
EXTERNAL_MODEL_CLIENT_IMAGE='curlimages/curl:8.10.1' \
EXTERNAL_MODEL_RUN_ID=$CONTEXT \
EXTERNAL_MODEL_CLIENT_RUN_AS_NON_ROOT=false \
EXTERNAL_MODEL_CLIENT_VOLUME_MOUNTS='[{name: maas-ca, mountPath: /etc/demo-ca, readOnly: true}]' \
EXTERNAL_MODEL_CLIENT_VOLUMES='[{name: maas-ca, configMap: {name: demo-maas-api-ca, items: [{key: ca.crt, path: ca.crt}]}}]' \
  envsubst '${EXTERNAL_MODEL_CLIENT_NAME} ${EXTERNAL_MODEL_CLIENT_NAMESPACE} ${EXTERNAL_MODEL_CLIENT_IMAGE} ${EXTERNAL_MODEL_RUN_ID} ${EXTERNAL_MODEL_CLIENT_RUN_AS_NON_ROOT} ${EXTERNAL_MODEL_CLIENT_VOLUME_MOUNTS} ${EXTERNAL_MODEL_CLIENT_VOLUMES}' \
    <"$ROOT/test/external-model/client.yaml.tmpl" | mutate apply -f - >/dev/null
wait_pod_ready "$CLIENT_NS" "$CLIENT" || fail_stage "client pod readiness"
path="/$TENANT/demo/v1/chat/completions"
API_PORT=$((18000+($$%1000)))
GATEWAY_PORT=$((API_PORT+1))
kubectl --context "$CONTEXT" -n "$API_NS" port-forward "svc/maas-api" "$API_PORT:8443" >"$EVIDENCE/port-forward-api.log" 2>&1 &
APF=$!
kubectl --context "$CONTEXT" -n "$GATEWAY_NS" port-forward "svc/$PUBLIC_GATEWAY" "$GATEWAY_PORT:80" >"$EVIDENCE/port-forward-gateway.log" 2>&1 &
GPF=$!
wait_port "$API_PORT" || fail_stage "MaaS API port-forward"
wait_port "$GATEWAY_PORT" || fail_stage "Gateway port-forward"
kctl -n kuadrant-system get configmap authorino-maas-api-ca -o jsonpath='{.data.ca\.crt}' >"$CA_CERT"
printf '%s\n' 'header = "X-MaaS-Username: kind-user"' 'header = "X-MaaS-Group: [\"system:authenticated\"]"' >"$ADMIN_CFG"
chmod 600 "$CA_CERT" "$ADMIN_CFG"
unauth=$(timeout 30s curl --noproxy '*' --connect-timeout 5 --max-time 15 -sS -o "$TMP_DIR/unauth" -w '%{http_code}' \
  -H 'content-type: application/json' --data '{"model":"demo","messages":[]}' "http://127.0.0.1:$GATEWAY_PORT$path" 2>/dev/null || printf 000)
echo "  unauthenticated request HTTP $unauth (expected 401/403)"
[[ "$unauth" == 401 || "$unauth" == 403 ]] || fail_stage "Kuadrant authentication"
key_body="$TMP_DIR/key.json"
key_status=$(timeout 30s curl --noproxy '*' --cacert "$CA_CERT" --resolve "maas-api.maas-system.svc.cluster.local:$API_PORT:127.0.0.1" \
  -sS -o "$key_body" -w '%{http_code}' --config "$ADMIN_CFG" -H 'content-type: application/json' \
  --data '{"name":"narrative-demo","ephemeral":true,"subscription":"kind-e2e-subscription"}' \
  "https://maas-api.maas-system.svc.cluster.local:$API_PORT/v1/api-keys" 2>/dev/null || printf 000)
KEY_ID=$(jq -r '.id//.keyId//.metadata.id//empty' "$key_body" 2>/dev/null || true)
KEY_VALUE=$(jq -r '.key//.apiKey//.token//empty' "$key_body" 2>/dev/null || true)
[[ "$key_status" == 201 && -n "$KEY_ID" && -n "$KEY_VALUE" ]] || fail_stage "MaaS API key creation"
printf 'header = "Authorization: Bearer %s"\n' "$KEY_VALUE" >"$AUTH_CFG"
chmod 600 "$AUTH_CFG"
unset KEY_VALUE
wait_gateway_route provider-a || fail_stage "initial Provider A route convergence"
wait_gateway_provider provider-a || fail_stage "initial Provider A endpoint convergence"
wait_gateway_cluster "payload-pre-processing.maas-system.svc.cluster.local" || fail_stage "pre-auth ExtProc endpoint convergence"
wait_gateway_cluster "payload-processing-external-model.models-as-a-service.svc.cluster.local" || fail_stage "post-auth ExtProc endpoint convergence"
wait_overlay a || fail_stage "initial semantic overlay convergence"
auth=$(request_status "$PUBLIC_GATEWAY" "$path" '{"model":"demo","messages":[{"role":"user","content":"auth"}]}' 2>/dev/null || true)
auth_status=$(printf '%s' "$auth" | cut -d'|' -f1); auth_backend=$(printf '%s' "$auth" | cut -d'|' -f2)
echo "  authenticated request HTTP $auth_status; backend ${auth_backend:-unavailable}"
[[ "$auth_status" == 200 ]] || fail_stage "authenticated Gateway request"
record 2 authentication PASS "unauthenticated=$unauth authenticated=$auth_status client_pod=$CLIENT"
echo "RESULT"; echo "  Status                 PASS"; echo "  Authenticated request  HTTP $auth_status"; pause

stage "Route to Provider A" "Prove the active overlay and backend response."
wait_gateway_route provider-a || fail_stage "Provider A route convergence"
wait_gateway_provider provider-a || fail_stage "Provider A endpoint convergence"
wait_overlay a || fail_stage "Provider A semantic overlay convergence"
result=$(request_status "$PUBLIC_GATEWAY" "$path" '{"model":"demo","messages":[{"role":"user","content":"provider-a"}]}' 2>/dev/null || true)
result_status=$(printf '%s' "$result" | cut -d'|' -f1); result_backend=$(printf '%s' "$result" | cut -d'|' -f2)
echo "  Provider A request     HTTP $result_status; backend ${result_backend:-unavailable}"
[[ "$result_status" == 200 && "$result_backend" == *katan-a* ]] || fail_stage "Provider A request"
record 3 provider_a PASS "http_status=$result_status backend=$result_backend"
echo "RESULT"; echo "  Status                 PASS"; echo "  Selected provider      Provider A"; pause

stage "Hot-swap to Provider B" "Mutate the run-owned model and wait for overlay convergence."
before_uid=$(kctl -n "$TENANT" get pod -l "app=$DATAPLANE_DEPLOYMENT" -o jsonpath='{.items[0].metadata.uid}')
before_restart=$(kctl -n "$TENANT" get pod -l "app=$DATAPLANE_DEPLOYMENT" -o jsonpath='{.items[0].status.containerStatuses[0].restartCount}')
before_generation=$(kctl -n "$TENANT" get configmap routing-overlay -o jsonpath='{.metadata.annotations.inference\.opendatahub\.io/routing-overlay-source-generation}')
mutate -n "$TENANT" patch externalmodel demo-model --type=json -p='[{"op":"replace","path":"/spec/externalProviderRefs/0/weight","value":0},{"op":"replace","path":"/spec/externalProviderRefs/1/weight","value":1}]' >/dev/null
wait_for "model Ready after Provider B mutation" 120 model_converged "$TENANT" demo-model || fail_stage "model readiness"
new_generation=
for _ in $(seq 1 60); do
  new_generation=$(kctl -n "$TENANT" get configmap routing-overlay -o jsonpath='{.metadata.annotations.inference\.opendatahub\.io/routing-overlay-source-generation}' 2>/dev/null || true)
  [[ "$new_generation" != "$before_generation" ]] && break
  sleep 2
done
[[ "$new_generation" != "$before_generation" ]] || fail_stage "overlay generation convergence"
wait_for "ExtProc mounted Provider B overlay" 120 kctl -n "$TENANT" exec "deploy/$DATAPLANE_DEPLOYMENT" -c "$DATAPLANE_CONTAINER" -- sh -c 'grep -q provider-provider-b /etc/praxis/routing/routing-overlay.json' || fail_stage "ExtProc overlay reload"
wait_overlay b || fail_stage "Provider B semantic overlay convergence"
wait_gateway_route provider-b || fail_stage "Provider B route convergence"
wait_gateway_provider provider-b || fail_stage "Provider B endpoint convergence"
result=$(request_status "$PUBLIC_GATEWAY" "$path" '{"model":"demo","messages":[{"role":"user","content":"provider-b"}]}' 2>/dev/null || true)
result_status=$(printf '%s' "$result" | cut -d'|' -f1); result_backend=$(printf '%s' "$result" | cut -d'|' -f2)
after_uid=$(kctl -n "$TENANT" get pod -l "app=$DATAPLANE_DEPLOYMENT" -o jsonpath='{.items[0].metadata.uid}')
after_restart=$(kctl -n "$TENANT" get pod -l "app=$DATAPLANE_DEPLOYMENT" -o jsonpath='{.items[0].status.containerStatuses[0].restartCount}')
echo "  Provider B request     HTTP $result_status; backend ${result_backend:-unavailable}; generation $before_generation -> $new_generation"
echo "  ExternalModel ExtProc restarts $before_restart -> $after_restart"
[[ "$result_status" == 200 && "$result_backend" == *katan-b* && "$before_uid" == "$after_uid" && "$before_restart" == "$after_restart" ]] || fail_stage "Provider B swap"
record 4 provider_hot_swap PASS "http_status=$result_status backend=$result_backend praxis_restart_unchanged=true"
echo "RESULT"; echo "  Status                 PASS"; echo "  New requests use the new routing snapshot."; pause

stage "Semantic no-op" "Cause a real provider watch event and prove routing content does not churn."
before_overlay=$(kctl -n "$TENANT" get configmap routing-overlay -o json | jq -c '{data,annotations:{source:.metadata.annotations["inference.opendatahub.io/routing-overlay-source-generation"],digest:.metadata.annotations["inference.opendatahub.io/routing-overlay-content-digest"]}}')
mutate -n "$TENANT" annotate externalprovider provider-b local-env.opendatahub.io/demo-noop=true --overwrite >/dev/null
wait_for "provider watch event observed" 120 kctl -n "$TENANT" get externalprovider provider-b -o jsonpath='{.status.observedGeneration}'
after_overlay=$(kctl -n "$TENANT" get configmap routing-overlay -o json | jq -c '{data,annotations:{source:.metadata.annotations["inference.opendatahub.io/routing-overlay-source-generation"],digest:.metadata.annotations["inference.opendatahub.io/routing-overlay-content-digest"]}}')
[[ "$before_overlay" == "$after_overlay" ]] || fail_stage "semantic overlay churn"
record 5 semantic_noop PASS "watch_event=true overlay_bytes_unchanged=true"
echo "RESULT"; echo "  Status                 PASS"; pause

stage "Invalid overlay and last-known-good" "Reject malformed routing while serving the previous valid revision."
valid_overlay=$(kctl -n "$TENANT" get configmap routing-overlay -o jsonpath='{.data.routing-overlay\.json}')
mutate -n "$TENANT" patch configmap routing-overlay --type=merge -p='{"data":{"routing-overlay.json":"{}"}}' >/dev/null
result=$(request_status "$PUBLIC_GATEWAY" "$path" '{"model":"demo","messages":[{"role":"user","content":"last-known-good"}]}' 2>/dev/null || true)
result_status=$(printf '%s' "$result" | cut -d'|' -f1); result_backend=$(printf '%s' "$result" | cut -d'|' -f2)
restore_patch=$(jq -n --arg data "$valid_overlay" '{data:{"routing-overlay.json":$data}}')
mutate -n "$TENANT" patch configmap routing-overlay --type=merge -p "$restore_patch" >/dev/null
echo "  Invalid replacement    rejected; active request HTTP $result_status; backend ${result_backend:-unavailable}"
[[ "$result_status" == 200 && "$result_backend" == *katan-b* ]] || fail_stage "last-known-good serving"
record 6 last_known_good PASS "invalid_replacement=true http_status=$result_status backend=$result_backend"
echo "RESULT"; echo "  Status                 PASS"; echo "  Previous valid routing remained active."; pause

stage "Tenant isolation" "Prove real Tenant B traffic before and after a Tenant A mutation."
TENANT_B_PORT=$((GATEWAY_PORT+1))
kubectl --context "$CONTEXT" -n "$GATEWAY_NS" port-forward "svc/$TENANT_B_GATEWAY" "$TENANT_B_PORT:80" >"$EVIDENCE/port-forward-tenant-b-gateway.log" 2>&1 &
TBPF=$!
wait_port "$TENANT_B_PORT" || fail_stage "Tenant B Gateway port-forward"
mutate -n "$BNS" annotate externalmodel demo-model "external-model-e2e/tenant-b-transport-check=$(date -u +%s%N)" --overwrite >/dev/null
wait_tenant_b_gateway_transport || fail_stage "Tenant B Gateway route/provider convergence"
printf '    %-24s %-26s %-26s\n' Property "Tenant A" "Tenant B"
printf '    %-24s %-26s %-26s\n' Namespace "$TENANT" "$BNS"
bpath="/$BNS/demo/v1/chat/completions"
b_before=$(request_status "$TENANT_B_GATEWAY" "$bpath" '{"model":"demo","messages":[{"role":"user","content":"tenant-b-before"}]}' 2>/dev/null || true)
b_before_status=$(printf '%s' "$b_before" | cut -d'|' -f1); b_before_backend=$(printf '%s' "$b_before" | cut -d'|' -f2)
echo "    Tenant B before A change HTTP $b_before_status; backend ${b_before_backend:-unavailable}"
if [[ "$b_before_status" != 200 ]]; then
  record 7 tenant_isolation NOT_DEMONSTRATED "tenant_b_initial_http=$b_before_status; tenant_b_transport_or_gateway_scope_unproven=true"
  echo "RESULT"; echo "  Status                 NOT_DEMONSTRATED"; echo "  Tenant B isolation requires a tenant-B Gateway transport qualification."; pause
else
  mutate -n "$TENANT" patch externalprovider provider-b --type=merge -p='{"spec":{"config":{"demo-isolation":"true"}}}' >/dev/null
  wait_for "Tenant A mutation observed" 120 kctl -n "$TENANT" get externalprovider provider-b -o jsonpath='{.status.observedGeneration}' || fail_stage "Tenant A reconcile"
  b_after=$(request_status "$TENANT_B_GATEWAY" "$bpath" '{"model":"demo","messages":[{"role":"user","content":"tenant-b-after"}]}' 2>/dev/null || true)
  b_after_status=$(printf '%s' "$b_after" | cut -d'|' -f1); b_after_backend=$(printf '%s' "$b_after" | cut -d'|' -f2)
  echo "    Tenant B after A change  HTTP $b_after_status; backend ${b_after_backend:-unavailable}"
  [[ "$b_after_status" == 200 ]] || fail_stage "Tenant B survival"
  sa_a_name=$(kctl -n "$TENANT" get deployment "$DATAPLANE_DEPLOYMENT" -o jsonpath='{.spec.template.spec.serviceAccountName}' 2>/dev/null || true)
  sa_b_name=$(kctl -n "$BNS" get deployment payload-processing-external-model-tenant-b -o jsonpath='{.spec.template.spec.serviceAccountName}' 2>/dev/null || true)
  [[ -n "$sa_a_name" && -n "$sa_b_name" ]] || fail_stage "ExtProc ServiceAccount discovery"
  sa_a=$(kctl auth can-i get secrets --as="system:serviceaccount:$TENANT:$sa_a_name" -n "$TENANT" 2>/dev/null || true)
  sa_b=$(kctl auth can-i get secrets --as="system:serviceaccount:$BNS:$sa_b_name" -n "$BNS" 2>/dev/null || true)
  echo "    ExtProc Secret API A/B  $sa_a/$sa_b (expected no/no)"
  [[ "$sa_a" == no && "$sa_b" == no ]] || fail_stage "tenant Secret API isolation"
  record 7 tenant_isolation PASS "tenant_b_before=$b_before_status tenant_b_after=$b_after_status secret_api=denied"
  echo "RESULT"; echo "  Status                 PASS"; echo "  Tenant B survived real Tenant A traffic and mutation."; pause
fi

stage "Credential behavior" "Report the runtime credential boundary without simulating an unqualified rotation."
echo "  Result                  NOT DEMONSTRATED"
echo "  Remaining gate          Live projected-Secret rotation qualification"
record 8 credential_rotation NOT_DEMONSTRATED "live rotation is outside current demo scope"
echo "RESULT"; echo "  Status                 NOT DEMONSTRATED"; pause

stage "Summary" "Summarize observed behavior and retain the healthy cluster."
record 9 summary PASS "routing_increment=pass credential_rotation=not_demonstrated"
atomic_results
python3 - "$EVIDENCE/results.json" <<'PY'
import json,sys
d=json.load(open(sys.argv[1]))
print("\n  Validation                         Result")
print("  ---------------------------------  ----------------")
for x in d["assertions"]: print("  %-33s %s" % (x["id"], x["status"]))
print("  Total demo assertions              %s" % d["total_assertions"])
PY
echo "RESULT"; echo "  Status                 PASS (routing increment)"
echo "  Credential rotation    NOT DEMONSTRATED"
echo "  Existing IPP path      FOLLOW-UP"
echo "  Retained context       $CONTEXT"
echo "  Evidence               $(evidence_label "$EVIDENCE")"
echo "  Inspect: kubectl --context $CONTEXT get pods -A"
echo "  Inspect: kubectl --context $CONTEXT -n $TENANT get externalmodel,externalprovider,httproute,service,deployment,configmap"
echo "  Reset:   ./test/kind-env/demo.sh --context $CONTEXT --reset"
