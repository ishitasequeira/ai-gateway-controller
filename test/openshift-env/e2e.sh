#!/usr/bin/env bash
# shellcheck disable=SC2068,SC2016,SC2015
set -euo pipefail
# Reviewed remote-command array expansions are required for the isolated
# kubeconfig wrapper and stdin-based credential transfer.
# shellcheck disable=SC2068,SC2016
ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
STATE=${OPENSHIFT_E2E_STATE:-"$ROOT/.openshift-state"}
# shellcheck disable=SC1091
source "$STATE/run.env"
# shellcheck disable=SC1091
source "$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)/test/external-model/request.sh"
command -v timeout >/dev/null || { echo "timeout is required" >&2; exit 1; }
OC=(timeout --foreground 45s oc --kubeconfig "${OPENSHIFT_KUBECONFIG:-$STATE/kubeconfig}")
OUT="$OPENSHIFT_E2E_EVIDENCE_ROOT/e2e-$(date -u +%Y%m%dT%H%M%SZ)-$$"
mkdir -p "$OUT"
RECOMPUTE_BIN="$OUT/recompute-digest"
go build -o "$RECOMPUTE_BIN" "$ROOT/test/openshift-env/recompute_digest.go"
AITENANT_NAME="${OPENSHIFT_E2E_AITENANT_NAME:-models-as-a-service}"
POST_AUTH_DEPLOYMENT=payload-processing-external-model
RESULTS="$OUT/results.json"
tmp="$RESULTS.tmp.$$"
printf '{"suite":"openshift-routing","functional":"RUNNING","assertions":[]}' >"$tmp"
mv "$tmp" "$RESULTS"
CURRENT_ASSERTION="initialization"
FINALIZED=false
ADMIN=""
KEY=""
KEY_ID=""
HOST=""
CLIENT=""
revoke_key() {
  [[ -n "$ADMIN" && -n "$KEY_ID" && -n "$HOST" && -n "$CLIENT" ]] || return 0
  local status
  status=$(printf '%s\n' "$ADMIN" | "${OC[@]}" exec -i "$CLIENT" -n "$OPENSHIFT_E2E_TENANT_NAMESPACE" -- sh -c 'read -r token; curl --silent --show-error --output /dev/null --write-out "%{http_code}" --max-time 20 --cacert /etc/xmp/ca/ca.crt -X DELETE "https://'"$HOST"'/v1/api-keys/'"$KEY_ID"'" -H "Authorization: Bearer $token"' 2>/dev/null || true)
  printf '%s\n' "$status" >"$OUT/key-revoke-status.txt"
  [[ "$status" == 200 ]] || return 1
  KEY_ID=""
}
finalize_on_exit() {
  local rc=${1:-$?} current final
  [[ "$FINALIZED" == true ]] && return "$rc"
  FINALIZED=true
  if [[ -n "$KEY_ID" ]] && ! revoke_key; then
    rc=1
    CURRENT_ASSERTION="API-key revocation after failure"
  fi
  [[ -f "$RESULTS" ]] || exit "$rc"
  current=$(jq -r '.functional // "UNKNOWN"' "$RESULTS" 2>/dev/null || printf 'UNKNOWN')
  if [[ "$current" == RUNNING ]]; then
    final="$RESULTS.final.$$.${RANDOM}"
    jq --arg assertion "$CURRENT_ASSERTION" --arg detail "qualification exited before completing this assertion" \
      '.functional="FAIL" | .failure={assertion:$assertion,detail:$detail}' "$RESULTS" >"$final"
    mv "$final" "$RESULTS"
  fi
  return "$rc"
}
trap 'finalize_on_exit "$?"' EXIT
trap 'exit 143' INT TERM HUP
record() {
  local id=$1 name=$2 status=$3 boundary=$4 detail=$5
  CURRENT_ASSERTION="$name"
  jq --arg id "$id" --arg name "$name" --arg status "$status" --arg boundary "$boundary" --arg detail "$detail" '.assertions += [{id:$id,name:$name,status:$status,boundary:$boundary,detail:$detail}]' "$RESULTS" >"$tmp"
  mv "$tmp" "$RESULTS"
}
wait_json() {
  local label=$1 command=$2 timeout=${3:-300} start now
  start=$(date +%s)
  while :; do
    if eval "$command" 2>/dev/null; then return 0; fi
    now=$(date +%s)
    (( now - start >= timeout )) && { echo "$label did not converge after ${timeout}s" >&2; return 1; }
    sleep 2
  done
}
"${OC[@]}" get deployment ai-gateway-controller -n "$OPENSHIFT_E2E_CONTROLLER_NAMESPACE" -o json >"$OUT/controller.json"
wait_json "controller deployment" "${OC[*]} get deployment ai-gateway-controller -n $OPENSHIFT_E2E_CONTROLLER_NAMESPACE -o json | jq -e '.status.availableReplicas == 1' >/dev/null" || { record 1 "controller deployment Ready" FAIL deployment "controller did not become available"; exit 1; }
record 1 "controller deployment Ready" PASS deployment "availableReplicas=1"
"${OC[@]}" get aitenant "$AITENANT_NAME" -n ai-tenants -o json >"$OUT/aitenant.json"
RESOLVED_TENANT_NAMESPACE=$(jq -r '.status.tenantNamespace // empty' "$OUT/aitenant.json")
if [[ -z "$RESOLVED_TENANT_NAMESPACE" || "$RESOLVED_TENANT_NAMESPACE" != "$OPENSHIFT_E2E_TENANT_NAMESPACE" ]]; then
  record 2 "AITenant namespace resolution" FAIL maas "run state namespace does not match live AITenant status"
  exit 1
fi
wait_json "AITenant Ready" "${OC[*]} get aitenant $AITENANT_NAME -n ai-tenants -o json | jq -e '[.status.conditions[]? | select(.type==\"Ready\" and .status==\"True\")] | length == 1' >/dev/null" || { record 2 "AITenant Ready" FAIL maas "MaaS did not report Ready"; exit 1; }
record 2 "AITenant Ready" PASS maas "MaaS reported Ready"
# A provider with no ExternalModel reference must still validate credentials,
# install provider transport, and publish Ready. Keep this resource run-owned
# so the normal destroy path removes it from the shared E2E tenant namespace.
STANDALONE_SUFFIX=${OPENSHIFT_E2E_RUN_ID//-/}
STANDALONE_SUFFIX=${STANDALONE_SUFFIX: -8}
STANDALONE_PROVIDER="provider-only-$STANDALONE_SUFFIX"
STANDALONE_ENDPOINT="standalone-$STANDALONE_SUFFIX.e2e.example.com"
"${OC[@]}" apply -f - <<EOF >"$OUT/provider-only-apply.log"
apiVersion: inference.opendatahub.io/v1alpha1
kind: ExternalProvider
metadata:
  name: $STANDALONE_PROVIDER
  namespace: $OPENSHIFT_E2E_TENANT_NAMESPACE
  labels:
    external-model-praxis.opendatahub.io/run-id: $OPENSHIFT_E2E_RUN_ID
    app.kubernetes.io/managed-by: external-model-praxis-openshift-e2e
spec:
  provider: openai
  endpoint: $STANDALONE_ENDPOINT
  auth:
    type: apikey
    secretRef:
      name: provider-credentials
EOF
wait_json "standalone provider Ready" "${OC[*]} get externalprovider $STANDALONE_PROVIDER -n $OPENSHIFT_E2E_TENANT_NAMESPACE -o json | jq -e '.status.phase == \"Ready\" and .status.observedGeneration == .metadata.generation and ([.status.conditions[]? | select(.type==\"Ready\" and .status==\"True\")] | length == 1)' >/dev/null" || { record provider-only "ExternalProvider Ready without model reference" FAIL controller "provider-only status did not converge"; exit 1; }
if ! "${OC[@]}" get externalmodel -n "$OPENSHIFT_E2E_TENANT_NAMESPACE" -o json |
  jq -e --arg provider "$STANDALONE_PROVIDER" '[.items[] | .spec.externalProviderRefs[]? | select(.ref.name == $provider)] | length == 0' >/dev/null; then
  record provider-only "ExternalProvider Ready without model reference" FAIL controller "an ExternalModel unexpectedly references the provider"
  exit 1
fi
standalone_provider_transport_ready() {
  local service service_entry destination_rule
  service=$("${OC[@]}" get service "provider-$STANDALONE_PROVIDER" -n "$OPENSHIFT_E2E_TENANT_NAMESPACE" -o json) || return 1
  service_entry=$("${OC[@]}" get serviceentry "provider-$STANDALONE_PROVIDER" -n "$OPENSHIFT_E2E_TENANT_NAMESPACE" -o json) || return 1
  destination_rule=$("${OC[@]}" get destinationrules -n "$OPENSHIFT_E2E_GATEWAY_NAMESPACE" \
    -l "app.kubernetes.io/managed-by=ai-gateway-controller,inference.opendatahub.io/external-provider=$STANDALONE_PROVIDER,inference.opendatahub.io/external-tenant=$OPENSHIFT_E2E_TENANT_NAMESPACE" -o json) || return 1
  jq -e --arg endpoint "$STANDALONE_ENDPOINT" '.spec.type == "ExternalName" and .spec.externalName == $endpoint' <<<"$service" >/dev/null &&
    jq -e --arg endpoint "$STANDALONE_ENDPOINT" '.spec.hosts | index($endpoint) != null' <<<"$service_entry" >/dev/null &&
    jq -e --arg endpoint "$STANDALONE_ENDPOINT" '[.items[] | select(.spec.host == $endpoint and .spec.trafficPolicy.tls.sni == $endpoint)] | length == 1' <<<"$destination_rule" >/dev/null
}
wait_json "standalone provider transport" standalone_provider_transport_ready || { record provider-only "ExternalProvider transport without model reference" FAIL transport "provider Service, ServiceEntry, or Gateway DestinationRule did not converge"; exit 1; }
record provider-only "ExternalProvider Ready without model reference" PASS controller "status Ready and Service, ServiceEntry, and Gateway DestinationRule exist with no ExternalModel reference"
CURRENT_ASSERTION="ExternalProvider Ready"
wait_json "provider Ready" "${OC[*]} get externalprovider provider-a -n $OPENSHIFT_E2E_TENANT_NAMESPACE -o json | jq -e '.status.phase == \"Ready\"' >/dev/null" || { record 3 "ExternalProvider Ready" FAIL controller "provider status did not converge"; exit 1; }
record 3 "ExternalProvider Ready" PASS controller "status.phase=Ready"
CURRENT_ASSERTION="ExternalModel Ready"
wait_json "model Ready" "${OC[*]} get externalmodel demo-model -n $OPENSHIFT_E2E_TENANT_NAMESPACE -o json | jq -e '.status.phase == \"Ready\"' >/dev/null" || { record 4 "ExternalModel Ready" FAIL controller "model status did not converge"; exit 1; }
record 4 "ExternalModel Ready" PASS controller "status.phase=Ready"
"${OC[@]}" get httproute -n "$OPENSHIFT_E2E_TENANT_NAMESPACE" -o json >"$OUT/httproutes.json"
wait_json "HTTPRoute acceptance" "${OC[*]} get httproute -n $OPENSHIFT_E2E_TENANT_NAMESPACE -o json | jq -e '[.items[].status.parents[]?.conditions[]? | select(.type==\"Accepted\" and .status==\"True\")] | length > 0' >/dev/null" || { record 5 "HTTPRoute Accepted" FAIL gateway "no Accepted=True parent"; exit 1; }
record 5 "HTTPRoute Accepted" PASS gateway "Accepted=True observed"
wait_json "HTTPRoute references" "${OC[*]} get httproute -n $OPENSHIFT_E2E_TENANT_NAMESPACE -o json | jq -e '[.items[].status.parents[]?.conditions[]? | select(.type==\"ResolvedRefs\" and .status==\"True\")] | length > 0' >/dev/null" || { record 6 "HTTPRoute ResolvedRefs" FAIL gateway "no ResolvedRefs=True parent"; exit 1; }
record 6 "HTTPRoute ResolvedRefs" PASS gateway "ResolvedRefs=True observed"
"${OC[@]}" get deployment,service,serviceaccount,configmap -n "$OPENSHIFT_E2E_TENANT_NAMESPACE" -o json >"$OUT/tenant-resources.json"
wait_json "ExtProc workloads" "${OC[*]} get deployment payload-pre-processing -n $OPENSHIFT_E2E_GATEWAY_NAMESPACE -o json | jq -e '.status.availableReplicas == 1' >/dev/null && ${OC[*]} get deployment $POST_AUTH_DEPLOYMENT -n $OPENSHIFT_E2E_TENANT_NAMESPACE -o json | jq -e '.status.availableReplicas == 1' >/dev/null" || { record 7 "ExtProc-only workloads Ready" FAIL tenant "pre-auth or post-auth ExtProc deployment did not become available"; exit 1; }
if "${OC[@]}" get deployment praxis -n "$OPENSHIFT_E2E_TENANT_NAMESPACE" -o name 2>/dev/null | grep -q .; then
  record 7 "Standalone provider-forwarding workload absent" FAIL tenant "unexpected standalone provider-forwarding deployment exists"
  exit 1
fi
record 7 "ExtProc-only workloads Ready" PASS tenant "Gateway-local pre-auth and tenant-local post-auth ExtProc are available; standalone provider-forwarding workload absent"
extproc_config=$("${OC[@]}" get configmap -n "$OPENSHIFT_E2E_TENANT_NAMESPACE" -l app.kubernetes.io/managed-by=ai-gateway-controller -o json 2>/dev/null | jq -r '[.items[] | .data["extproc.yaml"] // empty] | first // ""' || true)
printf '%s\n' "$extproc_config" >"$OUT/extproc-transport-config.txt"
: >"$OUT/external-https-transport.json"
transport_ok=true
for provider in a b; do
  grep -Fq "provider-provider-$provider" "$OUT/extproc-transport-config.txt" || transport_ok=false
  provider_host="provider-$provider.${OPENSHIFT_E2E_BACKEND_NAMESPACE}.svc.cluster.local"
  service_json=$(${OC[@]} get service "provider-$provider" -n "$OPENSHIFT_E2E_BACKEND_NAMESPACE" -o json 2>/dev/null || true)
  service_entry=$(${OC[@]} get serviceentry "provider-$provider" -n "$OPENSHIFT_E2E_TENANT_NAMESPACE" -o json 2>/dev/null || true)
  destination_rule=$(${OC[@]} get destinationrule "provider-$provider" -n "$OPENSHIFT_E2E_GATEWAY_NAMESPACE" -o json 2>/dev/null || true)
  [[ "$(jq -r '[.spec.ports[]?.port] | any(. == 443)' <<<"$service_json")" == true ]] || transport_ok=false
  [[ "$(jq -r '.spec.host // empty' <<<"$destination_rule")" == "$provider_host" ]] || transport_ok=false
  [[ "$(jq -r '.spec.trafficPolicy.tls.sni // empty' <<<"$destination_rule")" == "$provider_host" ]] || transport_ok=false
  [[ "$(jq -r '[.spec.hosts[]?] | any(. == $host)' --arg host "$provider_host" <<<"$service_entry")" == true ]] || transport_ok=false
  jq -n --arg provider "$provider" --arg host "$provider_host" --argjson service "$service_json" --argjson serviceEntry "$service_entry" --argjson destinationRule "$destination_rule" \
    '{provider:$provider,host:$host,service:{ports:($service.spec.ports // [])},serviceEntry:{hosts:($serviceEntry.spec.hosts // [])},destinationRule:{host:($destinationRule.spec.host // null),sni:($destinationRule.spec.trafficPolicy.tls.sni // null)}}' \
    >>"$OUT/external-https-transport.json" 2>/dev/null || true
done
if [[ "$transport_ok" == true ]]; then
  record 8-transport "External HTTPS transport" PASS transport "provider Services expose 443; ServiceEntry hosts, DestinationRule authority, and TLS SNI match the provider hostnames"
else
  record 8-transport "External HTTPS transport" FAIL transport "provider cluster, Service port 443, ServiceEntry host, or DestinationRule host/SNI contract was not proven"
  exit 1
fi
PRAXIS_SA=$("${OC[@]}" get deployment "$POST_AUTH_DEPLOYMENT" -n "$OPENSHIFT_E2E_TENANT_NAMESPACE" -o jsonpath='{.spec.template.spec.serviceAccountName}' 2>/dev/null || true)
if [[ -z "$PRAXIS_SA" ]]; then
  record 8 "ExtProc Secret API denied" FAIL rbac "post-auth ExtProc ServiceAccount was not found"
  exit 1
fi
printf 'serviceAccount=%s\n' "$PRAXIS_SA" >"$OUT/post-auth-service-account.txt"
if [[ "$PRAXIS_SA" != payload-processing-external-model* ]]; then
  record 8 "ExtProc Secret API denied" FAIL rbac "ExternalModel post-auth ExtProc uses unexpected ServiceAccount: $PRAXIS_SA"
  exit 1
fi
SECRET_API_ACCESS=$("${OC[@]}" auth can-i get secrets --as="system:serviceaccount:$OPENSHIFT_E2E_TENANT_NAMESPACE:$PRAXIS_SA" -n "$OPENSHIFT_E2E_TENANT_NAMESPACE" || true)
if [[ "$SECRET_API_ACCESS" == no ]]; then
  record 8 "ExtProc Secret API denied" PASS rbac "post-auth ExtProc ServiceAccount $PRAXIS_SA denied Secret reads"
else
  record 8 "ExtProc Secret API denied" FAIL rbac "post-auth ExtProc ServiceAccount can read Secrets"
  exit 1
fi
"${OC[@]}" get pods -n "$OPENSHIFT_E2E_TENANT_NAMESPACE" -l app=payload-processing-external-model -o json >"$OUT/post-auth-extproc-pods.json"
"${OC[@]}" get events -n "$OPENSHIFT_E2E_TENANT_NAMESPACE" --sort-by=.lastTimestamp >"$OUT/events.txt" 2>/dev/null || :
jq '.functional="RUNNING" | .note="functional request qualification"' "$RESULTS" >"$tmp"
mv "$tmp" "$RESULTS"
CLIENT="xmp-client-$OPENSHIFT_E2E_RUN_ID"
external_model_client_exec() {
  "${OC[@]}" exec -i "$CLIENT" -n "$OPENSHIFT_E2E_TENANT_NAMESPACE" -- "$@"
}
# The command array is intentionally expanded in several remote-command
# contexts below; keep these reviewed expansions visible to ShellCheck.
# shellcheck disable=SC2068,SC2016
HOST=$(${OC[@]} get gateway "$OPENSHIFT_E2E_GATEWAY_NAME" -n "$OPENSHIFT_E2E_GATEWAY_NAMESPACE" -o jsonpath='{.status.addresses[0].value}')
URL="https://$HOST/$OPENSHIFT_E2E_TENANT_NAMESPACE/demo/v1/chat/completions"
request() {
  local key=$1 url=$2 body=$3
  if [[ "$key" == none ]]; then
    printf '%s' "$body" | ${OC[@]} exec -i "$CLIENT" -n "$OPENSHIFT_E2E_TENANT_NAMESPACE" -- sh -c 'curl --silent --show-error --output /tmp/xmp-request --write-out "%{http_code}" --max-time 30 --cacert /etc/xmp/ca/ca.crt -X POST "$1" -H "Content-Type: application/json" --data-binary @-' sh "$url"
    return
  fi
  EXTERNAL_MODEL_CLIENT_EXEC=external_model_client_exec
  EXTERNAL_MODEL_CLIENT_OUTPUT=/tmp/xmp-request
  export EXTERNAL_MODEL_CLIENT_EXEC EXTERNAL_MODEL_CLIENT_OUTPUT
  printf '%s\n%s\n' "$key" "$body" | external_model_client_post "$url" /etc/xmp/ca/ca.crt
}
request_with_x_api_key_override() {
  local key=$1 url=$2 body=$3
  printf '%s\n%s' "$key" "$body" | ${OC[@]} exec -i "$CLIENT" -n "$OPENSHIFT_E2E_TENANT_NAMESPACE" -- sh -c 'read -r key; printf "Authorization: Bearer %s\n" "$key" > /tmp/xmp-auth-header; curl --silent --show-error --output /tmp/xmp-request --write-out "%{http_code}" --max-time 30 --cacert /etc/xmp/ca/ca.crt -X POST "$1" -H "x-api-key: client-override" -H @/tmp/xmp-auth-header -H "Content-Type: application/json" --data-binary @-; : > /tmp/xmp-auth-header' sh "$url"
}
wait_for_gateway_tls() {
  local deadline=$((SECONDS + 180)) status
  while (( SECONDS < deadline )); do
    status=$(request none "$URL" "$request_body" 2>/dev/null || true)
    if [[ "$status" == 401 ]]; then
      return 0
    fi
    # A response is definitive. Only transport status 000 may be retried
    # while the externally programmed load balancer becomes reachable.
    if [[ "$status" != 000 ]]; then
      printf 'Gateway readiness returned unexpected HTTP status %s\n' "$status" >&2
      return 1
    fi
    sleep 2
  done
  printf 'Gateway TLS endpoint did not become reachable before deadline\n' >&2
  return 1
}
wait_for_extproc_overlay() {
  local expected_provider=$1 expected_generation=$2 expected_digest=$3
  local stable=0 previous="" observation config_content mounted_content static_config generation digest recomputed mounted_digest extproc_identity revision_line accepted_revision serving_revision
  local deadline=$((SECONDS + 180))
  while (( SECONDS < deadline )); do
    observation=""
    config_content=$(${OC[@]} get configmap routing-overlay -n "$OPENSHIFT_E2E_TENANT_NAMESPACE" -o jsonpath='{.data.routing-overlay\.json}' 2>/dev/null || true)
    static_config=$("${OC[@]}" get configmap -n "$OPENSHIFT_E2E_TENANT_NAMESPACE" -l app.kubernetes.io/managed-by=ai-gateway-controller -o json 2>/dev/null | jq -r '[.items[] | .data["extproc.yaml"] // empty] | first // ""' || true)
    generation=$(${OC[@]} get configmap routing-overlay -n "$OPENSHIFT_E2E_TENANT_NAMESPACE" -o jsonpath='{.metadata.annotations.inference\.opendatahub\.io/routing-overlay-source-generation}' 2>/dev/null || true)
    digest=$(${OC[@]} get configmap routing-overlay -n "$OPENSHIFT_E2E_TENANT_NAMESPACE" -o jsonpath='{.metadata.annotations.inference\.opendatahub\.io/routing-overlay-content-digest}' 2>/dev/null || true)
    mounted_content=$("${OC[@]}" exec deploy/"$POST_AUTH_DEPLOYMENT" -n "$OPENSHIFT_E2E_TENANT_NAMESPACE" -- cat /etc/praxis/routing/routing-overlay.json 2>/dev/null || true)
    extproc_identity=$("${OC[@]}" get pods -n "$OPENSHIFT_E2E_TENANT_NAMESPACE" -l app=payload-processing-external-model -o json 2>/dev/null | jq -r '[.items[] | select([.status.conditions[]? | select(.type == "Ready" and .status == "True")] | length == 1)] | if length == 1 then .[0] | [.metadata.uid,([.status.containerStatuses[]?.restartCount]|add // 0)] | @tsv else empty end' || true)
    revision_line=$("${OC[@]}" logs deploy/"$POST_AUTH_DEPLOYMENT" -n "$OPENSHIFT_E2E_TENANT_NAMESPACE" -c payload-processing --tail=200 2>/dev/null | sed $'s/\033\\[[0-9;]*m//g' | rg 'intelligent_route: overlay (snapshot initialized|reloaded)' | tail -1 || true)
    accepted_revision=$(printf '%s\n' "$revision_line" | grep -oE '(^|[[:space:]])accepted_revision="?[0-9a-f]{64}"?' | tail -1 | sed 's/^[[:space:]]*accepted_revision=//; s/"//g' || true)
    serving_revision=$(printf '%s\n' "$revision_line" | grep -oE '(^|[[:space:]])serving_revision="?[0-9a-f]{64}"?' | tail -1 | sed 's/^[[:space:]]*serving_revision=//; s/"//g' || true)
    printf '%s' "$config_content" >"$OUT/config-overlay.json"
    printf '%s' "$mounted_content" >"$OUT/mounted-overlay.json"
    printf 'line=%s\naccepted_revision=%s\nserving_revision=%s\n' "$revision_line" "$accepted_revision" "$serving_revision" >"$OUT/extproc-overlay-revision.txt"
    recomputed=$($RECOMPUTE_BIN "$OUT/config-overlay.json" 2>/dev/null || true)
    mounted_digest=$($RECOMPUTE_BIN "$OUT/mounted-overlay.json" 2>/dev/null || true)
    # Bind the expected revision only after the requested provider is present;
    # otherwise a reset can accidentally bind the previous provider's stable
    # revision and reject the subsequent valid transition.
    if [[ -z "$expected_generation" && -n "$generation" && "$digest" =~ ^[0-9a-f]{64}$ && "$config_content" == *"provider-provider-$expected_provider"* && "$mounted_content" == *"provider-provider-$expected_provider"* ]]; then
      expected_generation="$generation"
      expected_digest="$digest"
    fi
    if [[ -n "$extproc_identity" && "$generation" == "$expected_generation" && "$digest" == "$expected_digest" && "$digest" =~ ^[0-9a-f]{64}$ && "$recomputed" == "$digest" && "$mounted_digest" == "$digest" && "$accepted_revision" == "$digest" && "$serving_revision" == "$digest" && "$config_content" == *"provider-provider-$expected_provider"* && "$mounted_content" == *"provider-provider-$expected_provider"* && "$static_config" == *"provider-provider-a"* && "$static_config" == *"provider-provider-b"* ]]; then
      observation="$generation:$digest:$mounted_digest:$accepted_revision:$serving_revision:$expected_provider:$extproc_identity"
      if [[ "$observation" == "$previous" ]]; then stable=$((stable + 1)); else stable=1; previous="$observation"; fi
      [[ "$stable" -ge 2 ]] && return 0
    else
      stable=0
      previous=""
    fi
    sleep 2
  done
  printf 'overlay convergence failed: expected provider=%s generation=%s digest=%s observed generation=%s digest=%s recomputed=%s mounted=%s accepted=%s serving=%s\n' "$expected_provider" "$expected_generation" "$expected_digest" "$generation" "$digest" "$recomputed" "$mounted_digest" "$accepted_revision" "$serving_revision" >&2
  return 1
}
${OC[@]} patch externalmodel demo-model -n "$OPENSHIFT_E2E_TENANT_NAMESPACE" --type=json -p='[{"op":"replace","path":"/spec/externalProviderRefs/0/weight","value":1},{"op":"replace","path":"/spec/externalProviderRefs/1/weight","value":0}]' >"$OUT/baseline-provider-a.txt"
wait_json "first endpoint controller reconciliation" "${OC[*]} get externalmodel demo-model -n $OPENSHIFT_E2E_TENANT_NAMESPACE -o json | jq -e '.status.phase == \"Ready\"' >/dev/null" 180 || { record baseline "First endpoint baseline" FAIL cleanup "controller did not reconcile first endpoint"; exit 1; }
wait_json "first endpoint ConfigMap" "${OC[*]} get configmap routing-overlay -n $OPENSHIFT_E2E_TENANT_NAMESPACE -o json | jq -e '.data[\"routing-overlay.json\"] | contains(\"provider-provider-a\") and (contains(\"provider-provider-b\") | not)' >/dev/null" 180 || { record baseline "First endpoint baseline" FAIL cleanup "first endpoint ConfigMap did not converge"; exit 1; }
wait_for_extproc_overlay a "" "" || { record baseline "First endpoint baseline" FAIL cleanup "ExternalModel ExtProc projection did not converge"; exit 1; }
request_body='{"model":"demo","messages":[{"role":"user","content":"qualification"}]}'
wait_for_gateway_tls || { record 9 "Gateway TLS endpoint" FAIL authorization "TLS endpoint did not reach the expected unauthenticated response"; exit 1; }
unauth=$(request none "$URL" "$request_body" 2>/dev/null || true)
[[ "$unauth" == 401 ]] && record 9 "Unauthenticated request" PASS authorization "first test endpoint returned HTTP 401" || { record 9 "Unauthenticated request" FAIL authorization "expected 401, observed $unauth"; exit 1; }
ADMIN=$(${OC[@]} whoami -t)
SUB=$(${OC[@]} get maassubscription -n "$OPENSHIFT_E2E_TENANT_NAMESPACE" -o jsonpath='{.items[0].metadata.name}')
printf '%s\n%s' "$ADMIN" "{\"name\":\"xmp-$OPENSHIFT_E2E_RUN_ID\",\"subscription\":\"$SUB\"}" | ${OC[@]} exec -i "$CLIENT" -n "$OPENSHIFT_E2E_TENANT_NAMESPACE" -- sh -c 'read -r token; curl --silent --show-error --output /tmp/xmp-key --write-out "%{http_code}" --max-time 20 --cacert /etc/xmp/ca/ca.crt -X POST "https://'"$HOST"'/v1/api-keys" -H "Authorization: Bearer $token" -H "Content-Type: application/json" --data-binary @-' >"$OUT/key-create-status.txt"
key_json=$(${OC[@]} exec "$CLIENT" -n "$OPENSHIFT_E2E_TENANT_NAMESPACE" -- sh -c 'cat /tmp/xmp-key && : > /tmp/xmp-key'); KEY=$(printf '%s' "$key_json" | sed -n 's/.*"key":"\([^"]*\)".*/\1/p'); KEY_ID=$(printf '%s' "$key_json" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')
[[ $(cat "$OUT/key-create-status.txt") == 201 && -n "$KEY" && -n "$KEY_ID" ]] || { record 10 "API-key creation" FAIL authorization "real MaaS key creation failed"; exit 1; }
record 10 "API-key creation" PASS authorization "HTTP 201; key withheld"
a_status=$(request "$KEY" "$URL" "$request_body" 2>/dev/null || true); a_body=$(${OC[@]} exec "$CLIENT" -n "$OPENSHIFT_E2E_TENANT_NAMESPACE" -- sh -c 'sed -n "s/.*host=\([^,\" ]*\).*/\1/p" /tmp/xmp-request' | head -1 || true)
[[ "$a_status" == 200 && "$a_body" == *provider-a* ]] && record 11 "First test endpoint" PASS routing "HTTP 200; Test Provider A attribution observed" || { record 11 "First test endpoint" FAIL routing "expected HTTP 200 from first endpoint"; exit 1; }
provider_url="https://provider-a.${OPENSHIFT_E2E_BACKEND_NAMESPACE}.svc.cluster.local/v1/chat/completions"
provider_probe() {
  local mode=$1
  case "$mode" in
    missing) printf '%s' "$request_body" | ${OC[@]} exec -i "$CLIENT" -n "$OPENSHIFT_E2E_TENANT_NAMESPACE" -- sh -c 'curl --silent --show-error --output /dev/null --write-out "%{http_code}" --max-time 20 --cacert /etc/xmp/ca/ca.crt -X POST "$1" -H "Content-Type: application/json" --data-binary @-' sh "$provider_url" ;;
    wrong) printf '%s' "$request_body" | ${OC[@]} exec -i "$CLIENT" -n "$OPENSHIFT_E2E_TENANT_NAMESPACE" -- sh -c 'curl --silent --show-error --output /dev/null --write-out "%{http_code}" --max-time 20 --cacert /etc/xmp/ca/ca.crt -X POST "$1" -H "Authorization: Bearer client-override" -H "Content-Type: application/json" --data-binary @-' sh "$provider_url" ;;
  esac
}
provider_missing=$(provider_probe missing 2>/dev/null || true)
printf '%s\n' "$provider_missing" >"$OUT/provider-missing-status.txt"
[[ "$provider_missing" == 401 ]] && record 21 "provider_rejects_missing_credential" PASS provider "direct Provider A request returned HTTP 401" || { record 21 "provider_rejects_missing_credential" FAIL provider "expected HTTP 401, observed $provider_missing"; exit 1; }
provider_wrong=$(provider_probe wrong 2>/dev/null || true)
printf '%s\n' "$provider_wrong" >"$OUT/provider-wrong-status.txt"
[[ "$provider_wrong" == 401 ]] && record 22 "provider_rejects_wrong_credential" PASS provider "direct Provider A request returned HTTP 401" || { record 22 "provider_rejects_wrong_credential" FAIL provider "expected HTTP 401, observed $provider_wrong"; exit 1; }
record 23 "client_authorization_cannot_override_provider_credential" NOT_DEMONSTRATED provider "public MaaS caller authentication consumes the single Authorization header; duplicate headers are intentionally not used"
x_api_override=$(request_with_x_api_key_override "$KEY" "$URL" "$request_body" 2>/dev/null || true)
x_api_override_body=$(${OC[@]} exec "$CLIENT" -n "$OPENSHIFT_E2E_TENANT_NAMESPACE" -- sh -c 'sed -n "s/.*host=\([^,\" ]*\).*/\1/p" /tmp/xmp-request' | head -1 || true)
[[ "$x_api_override" == 200 && "$x_api_override_body" == *provider-a* ]] && record 24 "client_x_api_key_cannot_override_provider_credential" PASS provider "HTTP 200; Provider A attribution observed" || { record 24 "client_x_api_key_cannot_override_provider_credential" FAIL provider "expected attributed HTTP 200, observed $x_api_override"; exit 1; }
if [[ "$a_status" == 200 && "$a_body" == *provider-a* && "$provider_missing" == 401 && "$provider_wrong" == 401 && "$x_api_override" == 200 ]]; then
  record 25 "credential_enforcing_provider_chain" NOT_DEMONSTRATED provider "backend enforcement and x-api-key resistance passed; Authorization override remains unrepresentable on the public path"
else
  record 25 "credential_enforcing_provider_chain" FAIL provider "credential enforcement or x-api-key override resistance failed"
  exit 1
fi
unknown=$(request "$KEY" "https://$HOST/$OPENSHIFT_E2E_TENANT_NAMESPACE/missing-model/v1/chat/completions" '{"model":"missing-model","messages":[{"role":"user","content":"qualification"}]}' 2>/dev/null || true)
[[ "$unknown" == 404 ]] && record 12 "Unknown model" PASS routing "HTTP 404" || { record 12 "Unknown model" FAIL routing "expected HTTP 404, observed $unknown"; exit 1; }
before=$(wait_json "single ready post-auth ExtProc pod" "${OC[*]} get pods -n $OPENSHIFT_E2E_TENANT_NAMESPACE -l app=payload-processing-external-model -o json | jq -e '[.items[] | select([.status.conditions[]? | select(.type==\"Ready\" and .status==\"True\")] | length == 1)] | length == 1' >/dev/null && ${OC[*]} get pods -n $OPENSHIFT_E2E_TENANT_NAMESPACE -l app=payload-processing-external-model -o json | jq -r '[.items[] | select([.status.conditions[]? | select(.type==\"Ready\" and .status==\"True\")] | length == 1)] | .[0] | [.metadata.uid,([.status.containerStatuses[]?.restartCount]|add // 0)] | @tsv'" 180)
${OC[@]} patch externalmodel demo-model -n "$OPENSHIFT_E2E_TENANT_NAMESPACE" --type=json -p='[{"op":"replace","path":"/spec/externalProviderRefs/0/weight","value":0},{"op":"replace","path":"/spec/externalProviderRefs/1/weight","value":1}]' >"$OUT/endpoint-switch.txt"
wait_json "second endpoint overlay" "${OC[*]} get configmap routing-overlay -n $OPENSHIFT_E2E_TENANT_NAMESPACE -o json | jq -e '.data[\"routing-overlay.json\"] | contains(\"provider-provider-b\")' >/dev/null" 180 || { record 13-converge "Endpoint switch convergence" FAIL routing "second endpoint overlay did not converge"; exit 1; }
wait_for_extproc_overlay b "" "" || { record 13-converge "Endpoint switch convergence" FAIL routing "second endpoint projection did not converge"; exit 1; }
b_status=$(request "$KEY" "$URL" "$request_body" 2>/dev/null || true); b_body=$(${OC[@]} exec "$CLIENT" -n "$OPENSHIFT_E2E_TENANT_NAMESPACE" -- sh -c 'sed -n "s/.*host=\([^,\" ]*\).*/\1/p" /tmp/xmp-request' | head -1 || true)
[[ "$b_status" == 200 && "$b_body" == *provider-b* ]] && record 13 "Second test endpoint" PASS routing "HTTP 200; Test Provider B attribution observed" || { record 13 "Second test endpoint" FAIL routing "expected HTTP 200 from second endpoint"; exit 1; }
after=$(wait_json "single ready post-auth ExtProc pod after switch" "${OC[*]} get pods -n $OPENSHIFT_E2E_TENANT_NAMESPACE -l app=payload-processing-external-model -o json | jq -e '[.items[] | select([.status.conditions[]? | select(.type==\"Ready\" and .status==\"True\")] | length == 1)] | length == 1' >/dev/null && ${OC[*]} get pods -n $OPENSHIFT_E2E_TENANT_NAMESPACE -l app=payload-processing-external-model -o json | jq -r '[.items[] | select([.status.conditions[]? | select(.type==\"Ready\" and .status==\"True\")] | length == 1)] | .[0] | [.metadata.uid,([.status.containerStatuses[]?.restartCount]|add // 0)] | @tsv'" 180)
[[ "$before" == "$after" ]] && record 14 "ExternalModel ExtProc identity stable" PASS tenant "UID and restart count unchanged" || { record 14 "ExternalModel ExtProc identity stable" FAIL tenant "before=$before after=$after"; exit 1; }
noop_before=$(${OC[@]} get externalmodel demo-model -n "$OPENSHIFT_E2E_TENANT_NAMESPACE" -o json | jq -r '.metadata.generation')
noop_cm_before=$(${OC[@]} get configmap routing-overlay -n "$OPENSHIFT_E2E_TENANT_NAMESPACE" -o json | jq -r '[.metadata.generation,.metadata.resourceVersion,.metadata.annotations["inference.opendatahub.io/routing-overlay-content-digest"]] | @tsv')
${OC[@]} get externalmodel demo-model -n "$OPENSHIFT_E2E_TENANT_NAMESPACE" -o json | jq 'del(.metadata.creationTimestamp,.metadata.generation,.metadata.managedFields,.metadata.resourceVersion,.metadata.uid,.status)' | ${OC[@]} apply --server-side --field-manager=openshift-e2e-noop -f - >"$OUT/semantic-noop.txt"
noop_after=$(${OC[@]} get externalmodel demo-model -n "$OPENSHIFT_E2E_TENANT_NAMESPACE" -o json | jq -r '.metadata.generation')
noop_cm_after=$(${OC[@]} get configmap routing-overlay -n "$OPENSHIFT_E2E_TENANT_NAMESPACE" -o json | jq -r '[.metadata.generation,.metadata.resourceVersion,.metadata.annotations["inference.opendatahub.io/routing-overlay-content-digest"]] | @tsv')
[[ "$noop_before" == "$noop_after" && "$noop_cm_before" == "$noop_cm_after" ]] && record 15 "Semantic no-op" PASS routing "ExternalModel generation and overlay generation, resourceVersion, and digest unchanged" || { record 15 "Semantic no-op" FAIL routing "before=$noop_before/$noop_cm_before after=$noop_after/$noop_cm_after"; exit 1; }
valid_overlay=$(${OC[@]} get configmap routing-overlay -n "$OPENSHIFT_E2E_TENANT_NAMESPACE" -o jsonpath='{.data.routing-overlay\.json}')
printf '%s' "$valid_overlay" >"$OUT/valid-overlay-before-lkg.json"
${OC[@]} patch configmap routing-overlay -n "$OPENSHIFT_E2E_TENANT_NAMESPACE" --type=merge -p='{"data":{"routing-overlay.json":"{invalid-overlay"}}' >"$OUT/invalid-overlay-injection.txt"
sleep 3
lkg_mounted=$("${OC[@]}" exec deploy/"$POST_AUTH_DEPLOYMENT" -n "$OPENSHIFT_E2E_TENANT_NAMESPACE" -- cat /etc/praxis/routing/routing-overlay.json 2>/dev/null || true)
lkg_status=$(request "$KEY" "$URL" "$request_body" 2>/dev/null || true)
[[ "$lkg_status" == 200 && "$lkg_mounted" == *provider-provider-b* ]] && record 16 "Invalid overlay LKG" PASS routing "malformed replacement retained the accepted Provider B route" || { record 16 "Invalid overlay LKG" FAIL routing "expected HTTP 200 with mounted Provider B last-known-good overlay, observed $lkg_status"; exit 1; }
# The publisher intentionally refuses to chain from a tampered envelope.  The
# documented, run-owned recovery is to remove only that corrupted ConfigMap;
# the next normal ExternalModel reconciliation recreates the valid envelope.
${OC[@]} delete configmap routing-overlay -n "$OPENSHIFT_E2E_TENANT_NAMESPACE" --ignore-not-found >"$OUT/lkg-recovery-delete.txt"
${OC[@]} patch externalmodel demo-model -n "$OPENSHIFT_E2E_TENANT_NAMESPACE" --type=json -p='[{"op":"replace","path":"/spec/externalProviderRefs/0/weight","value":1},{"op":"replace","path":"/spec/externalProviderRefs/1/weight","value":0}]' >"$OUT/reset-provider-a.txt"
wait_for_extproc_overlay a "" "" || { record 18 "Provider A reset" FAIL cleanup "baseline did not restore"; exit 1; }
reset_status=$(request "$KEY" "$URL" "$request_body" 2>/dev/null || true); reset_body=$(${OC[@]} exec "$CLIENT" -n "$OPENSHIFT_E2E_TENANT_NAMESPACE" -- sh -c 'sed -n "s/.*host=\([^,\" ]*\).*/\1/p" /tmp/xmp-request' | head -1 || true)
[[ "$reset_status" == 200 && "$reset_body" == *provider-a* ]] && record 19 "Provider A reset request" PASS routing "HTTP 200; first test endpoint restored" || { record 19 "Provider A reset request" FAIL routing "expected HTTP 200 from first endpoint after reset"; exit 1; }
revoke_key && record 17 "API-key revocation" PASS authorization "HTTP 200" || { record 17 "API-key revocation" FAIL authorization "revocation failed"; exit 1; }
scan_candidates="$OUT/.credential-scan-candidates"
rg -I -i -n 'Authorization:[[:space:]]*(Bearer|Basic)[[:space:]]+[A-Za-z0-9._~+/=-]{20,}|Bearer[[:space:]]+[A-Za-z0-9._~+/=-]{20,}|sk-[A-Za-z0-9]{16,}' "$OUT" >"$scan_candidates" 2>/dev/null || true
if [[ -s "$scan_candidates" ]]; then
  # The generated OpenShift CRD schema contains a documented sha256~ token
  # example in a prose description. Exclude only that exact schema-example
  # shape; runtime request, header, log, and response evidence remains strict.
  filtered_candidates="$OUT/.credential-scan-filtered"
  rg -v '"description": "authorize indicates if the proxied request should contain.*Authorization: Bearer sha256~' "$scan_candidates" >"$filtered_candidates" || true
  if [[ -s "$filtered_candidates" ]]; then
    printf 'status=FAIL\ncandidates=present\n' >"$OUT/credential-scan.txt"
    record 20 "Credential leak scan" FAIL security "credential-shaped runtime evidence found"
  else
    printf 'status=PASS\ncandidates=generated-schema-example-only\n' >"$OUT/credential-scan.txt"
    record 20 "Credential leak scan" PASS security "no credential values in runtime evidence; generated schema example classified separately"
  fi
else
  printf 'status=PASS\ncandidates=none\n' >"$OUT/credential-scan.txt"
  record 20 "Credential leak scan" PASS security "no credential patterns found"
fi
jq '.functional=(if (([.assertions[] | select(.status=="FAIL")] | length) == 0 and ([.assertions[] | select(.status=="NOT_DEMONSTRATED")] | length) == 0) then "PASS" else "PARTIAL" end) | .note="Single-tenant routing; two-tenant MaaS authorization remains issue #23"' "$RESULTS" >"$tmp"
mv "$tmp" "$RESULTS"
printf '%s\n' "$RESULTS"
if [[ "$(jq '[.assertions[] | select(.status=="FAIL" or .status=="NOT_DEMONSTRATED")] | length' "$RESULTS")" -ne 0 ]]; then exit 1; fi
