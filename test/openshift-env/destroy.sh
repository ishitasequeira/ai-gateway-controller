#!/usr/bin/env bash
set -euo pipefail
ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
STATE=${OPENSHIFT_E2E_STATE:-"$ROOT/.openshift-state"}
# shellcheck disable=SC1091
source "$STATE/run.env"
OC=(oc --kubeconfig "${OPENSHIFT_KUBECONFIG:-$STATE/kubeconfig}")
PULL_SECRET="xmp-registry-pull-$OPENSHIFT_E2E_RUN_ID"
GATEWAY_TLS_SECRET="xmp-gateway-tls-$OPENSHIFT_E2E_RUN_ID"
AITENANT="${OPENSHIFT_E2E_AITENANT_NAME:-xmp-$OPENSHIFT_E2E_RUN_ID}"
SHARED_AITENANT=false
AITENANT_RETAINED=false
MTC_NAME=default-tenant
MTC_RETAINED=false
# A recorded original snapshot is authoritative for runs that predate the
# persisted AITENANT_NAME field. Never enter the deletion path for the shared
# shared MaaS AITenant when that snapshot exists.
if [[ "${OPENSHIFT_E2E_AITENANT_NAME:-}" == models-as-a-service || -s "$STATE/aitenant-original.json" ]]; then
  AITENANT=models-as-a-service
  # A partially completed bootstrap may have written the shared-name setting
  # before the MaaS CRD or object existed. Treat that as an absent shared
  # resource; require the snapshot whenever a live shared object is present.
  if [[ -s "$STATE/aitenant-original.json" ]] || "${OC[@]}" get aitenant "$AITENANT" -n ai-tenants >/dev/null 2>&1; then
    SHARED_AITENANT=true
  fi
fi
if "${OC[@]}" get aitenant "$AITENANT" -n ai-tenants -o json >"$STATE/destroy-aitenant.json" 2>/dev/null; then
    if [[ "$SHARED_AITENANT" == true ]]; then
    if ! jq -e --arg run "$OPENSHIFT_E2E_RUN_ID" '.metadata.labels["external-model-praxis.opendatahub.io/run-id"] == $run and .metadata.labels["app.kubernetes.io/managed-by"] == "external-model-praxis-openshift-e2e"' "$STATE/destroy-aitenant.json" >/dev/null; then
      jq -e --slurpfile original "$STATE/aitenant-original.json" '(.metadata.labels // {}) == ($original[0].metadata.labels // {}) and (.metadata.annotations // {}) == ($original[0].metadata.annotations // {}) and (.spec // {}) == ($original[0].spec // {})' "$STATE/destroy-aitenant.json" >/dev/null || { echo "shared AITenant was not opted in by this run or was already restored" >&2; exit 1; }
      SHARED_AITENANT_ALREADY_RESTORED=true
      AITENANT_RETAINED=true
    fi
  else
    jq -e --arg run "$OPENSHIFT_E2E_RUN_ID" '.metadata.labels["external-model-praxis.opendatahub.io/run-id"] == $run and .metadata.labels["app.kubernetes.io/managed-by"] == "external-model-praxis-openshift-e2e"' "$STATE/destroy-aitenant.json" >/dev/null || { echo "AITenant ownership check failed" >&2; exit 1; }
  fi
  resolved=$(jq -r '.status.tenantNamespace // empty' "$STATE/destroy-aitenant.json")
  [[ -z "$resolved" || "$resolved" == models-as-a-service || "$resolved" == ai-tenant-xmp-$OPENSHIFT_E2E_RUN_ID ]] || { echo "unexpected MaaS resolved namespace: $resolved" >&2; exit 1; }
  [[ -z "$resolved" ]] || OPENSHIFT_E2E_TENANT_NAMESPACE=$resolved
fi
for ns in "$OPENSHIFT_E2E_CONTROLLER_NAMESPACE" "$OPENSHIFT_E2E_TENANT_NAMESPACE" "$OPENSHIFT_E2E_BACKEND_NAMESPACE"; do
  if [[ "$SHARED_AITENANT" == true && "$ns" == models-as-a-service ]]; then continue; fi
  [[ "$ns" == xmp-controller-$OPENSHIFT_E2E_RUN_ID || "$ns" == xmp-provider-$OPENSHIFT_E2E_RUN_ID || "$ns" == xmp-tenant-$OPENSHIFT_E2E_RUN_ID || "$ns" == ai-tenant-xmp-$OPENSHIFT_E2E_RUN_ID ]] || { echo "refusing unvalidated namespace: $ns" >&2; exit 1; }
  if "${OC[@]}" get namespace "$ns" -o json >"$STATE/destroy-namespace.json" 2>/dev/null; then
    jq -e --arg run "$OPENSHIFT_E2E_RUN_ID" '(.metadata.labels["external-model-praxis.opendatahub.io/run-id"] == $run) or (.metadata.labels["maas.opendatahub.io/tenant-name"] == ("xmp-" + $run) and .metadata.annotations["maas.opendatahub.io/created-by-aitenant"] == "true")' "$STATE/destroy-namespace.json" >/dev/null || { echo "ownership check failed: $ns" >&2; exit 1; }
  fi
done
if "${OC[@]}" get gateway "$OPENSHIFT_E2E_GATEWAY_NAME" -n "$OPENSHIFT_E2E_GATEWAY_NAMESPACE" -o json >"$STATE/destroy-gateway.json" 2>/dev/null; then
  jq -e --arg run "$OPENSHIFT_E2E_RUN_ID" '.metadata.labels["external-model-praxis.opendatahub.io/run-id"] == $run' "$STATE/destroy-gateway.json" >/dev/null || { echo "gateway ownership check failed" >&2; exit 1; }
fi
if "${OC[@]}" get route "$OPENSHIFT_E2E_REGISTRY_ROUTE" -n openshift-image-registry -o json >"$STATE/destroy-registry-route.json" 2>/dev/null; then
  jq -e --arg run "$OPENSHIFT_E2E_RUN_ID" '.metadata.labels["external-model-praxis.opendatahub.io/run-id"] == $run' "$STATE/destroy-registry-route.json" >/dev/null || { echo "registry route ownership check failed" >&2; exit 1; }
fi
if "${OC[@]}" get secret "$GATEWAY_TLS_SECRET" -n "$OPENSHIFT_E2E_GATEWAY_NAMESPACE" -o json >"$STATE/destroy-gateway-tls-secret.json" 2>/dev/null; then
  jq -e --arg run "$OPENSHIFT_E2E_RUN_ID" '.metadata.labels["external-model-praxis.opendatahub.io/run-id"] == $run and .metadata.labels["app.kubernetes.io/managed-by"] == "external-model-praxis-openshift-e2e"' "$STATE/destroy-gateway-tls-secret.json" >/dev/null || { echo "Gateway TLS Secret ownership check failed" >&2; exit 1; }
fi
OUT="$OPENSHIFT_E2E_EVIDENCE_ROOT/cleanup-$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$OUT"
cleanup_failed=0

# The selector is owned by MaaS's tenant config, not AITenant. A shared
# default tenant must remain in place, so restore only the selector annotation
# captured before this run's opt-in. Unrelated labels, annotations, and
# handoff status belong to MaaS/controller reconciliation and are never
# overwritten by cleanup.
if [[ "$SHARED_AITENANT" == true ]]; then
  mtc_original="$STATE/maastenantconfig-original.json"
  if [[ -s "$mtc_original" ]]; then
    mtc_selector_snapshot="$OUT/maastenantconfig-selector-original.json"
    jq --arg selector 'maas.opendatahub.io/payload-processing-type' \
      'if (.selector? != null) then .selector else {present:((.metadata.annotations // {}) | has($selector)),value:((.metadata.annotations // {})[$selector] // null)} end' \
      "$mtc_original" >"$mtc_selector_snapshot"
    "${OC[@]}" get maastenantconfig "$MTC_NAME" -n "$OPENSHIFT_E2E_TENANT_NAMESPACE" -o json >"$OUT/shared-maastenantconfig-before-restore.json" 2>/dev/null || {
      echo "refusing shared cleanup: MaasTenantConfig snapshot exists but live $MTC_NAME is missing" >&2
      exit 1
    }
    jq -e --slurpfile original "$mtc_original" '.metadata.uid == $original[0].metadata.uid' "$OUT/shared-maastenantconfig-before-restore.json" >/dev/null || {
      echo "refusing shared cleanup: MaasTenantConfig UID changed" >&2
      exit 1
    }
    selector_present=$(jq -r '.present' "$mtc_selector_snapshot")
    selector_value=$(jq -r '.value // ""' "$mtc_selector_snapshot")
    live_selector_present=$(jq -r --arg selector 'maas.opendatahub.io/payload-processing-type' '((.metadata.annotations // {}) | has($selector))' "$OUT/shared-maastenantconfig-before-restore.json")
    live_selector=$(jq -r '.metadata.annotations["maas.opendatahub.io/payload-processing-type"] // ""' "$OUT/shared-maastenantconfig-before-restore.json")
    if [[ "$live_selector_present" == "$selector_present" && "$live_selector" == "$selector_value" ]]; then
      # The previous destroy already restored the recorded selector. This is
      # the harmless idempotent path and performs no mutation.
      MTC_RETAINED=true
    elif [[ "$live_selector_present" == true && "$live_selector" == praxis ]]; then
      # Only restore a value that this run explicitly wrote. Any other live
      # value is a concurrent decision and must not be overwritten.
      :
    else
      echo "refusing shared cleanup: MaasTenantConfig selector changed since this run wrote praxis" >&2
      exit 1
    fi
    if [[ "$MTC_RETAINED" != true ]]; then
      if [[ "$selector_present" == true ]]; then
        "${OC[@]}" annotate maastenantconfig "$MTC_NAME" -n "$OPENSHIFT_E2E_TENANT_NAMESPACE" \
          "maas.opendatahub.io/payload-processing-type=$selector_value" --overwrite >"$OUT/shared-maastenantconfig-restore.log"
      else
        "${OC[@]}" annotate maastenantconfig "$MTC_NAME" -n "$OPENSHIFT_E2E_TENANT_NAMESPACE" \
          maas.opendatahub.io/payload-processing-type- --overwrite >"$OUT/shared-maastenantconfig-restore.log"
      fi
      deadline=$((SECONDS + 180))
      while (( SECONDS < deadline )); do
        if "${OC[@]}" get maastenantconfig "$MTC_NAME" -n "$OPENSHIFT_E2E_TENANT_NAMESPACE" -o json 2>/dev/null |
          jq -e --arg selector 'maas.opendatahub.io/payload-processing-type' --slurpfile original "$mtc_selector_snapshot" '((.metadata.annotations // {}) | has($selector)) == $original[0].present and (((.metadata.annotations // {})[$selector] // null) == ($original[0].value // null))' >/dev/null; then
          MTC_RETAINED=true
          break
        fi
        sleep 3
      done
    fi
    [[ "$MTC_RETAINED" == true ]] || { echo "shared MaasTenantConfig selector did not restore" >&2; exit 1; }
    echo "shared MaaS MaasTenantConfig selector restored; object and namespace retained; unrelated metadata preserved" >"$OUT/shared-maastenantconfig-restored.txt"
  elif "${OC[@]}" get maastenantconfig "$MTC_NAME" -n "$OPENSHIFT_E2E_TENANT_NAMESPACE" -o json >"$OUT/shared-maastenantconfig-untracked.json" 2>/dev/null; then
    if jq -e '.metadata.annotations["maas.opendatahub.io/payload-processing-type"] == "praxis"' "$OUT/shared-maastenantconfig-untracked.json" >/dev/null; then
      echo "refusing shared cleanup: live MaasTenantConfig is selected for praxis but original metadata snapshot is missing" >&2
      exit 1
    fi
    MTC_RETAINED=true
  fi
fi

# The AITenant finalizer is serviced by the run-owned controller.  Keep the
# controller, tenant namespace, and Gateway alive until the object disappears;
# deleting any of them first strands the finalizer and makes cleanup unsafe.
if [[ "$SHARED_AITENANT" == true && "${SHARED_AITENANT_ALREADY_RESTORED:-false}" != true ]]; then
  original="$STATE/aitenant-original.json"
  [[ -s "$original" ]] || { echo "refusing shared AITenant cleanup: original metadata is missing" >&2; exit 1; }
  OUT="$OPENSHIFT_E2E_EVIDENCE_ROOT/cleanup-$(date -u +%Y%m%dT%H%M%SZ)"
  mkdir -p "$OUT"
  "${OC[@]}" get aitenant "$AITENANT" -n ai-tenants -o json >"$OUT/shared-aitenant-before-restore.json"
  jq -n --slurpfile original "$original" '[{op:"replace",path:"/metadata/labels",value:($original[0].metadata.labels // {})},{op:"replace",path:"/metadata/annotations",value:($original[0].metadata.annotations // {})},{op:"replace",path:"/spec",value:($original[0].spec // {})}]' >"$OUT/shared-aitenant-restore-patch.json"
  "${OC[@]}" patch aitenant "$AITENANT" -n ai-tenants --type=json --patch-file "$OUT/shared-aitenant-restore-patch.json" >"$OUT/shared-aitenant-restore.log"
  deadline=$((SECONDS + 180))
  restored=false
  while (( SECONDS < deadline )); do
    if "${OC[@]}" get aitenant "$AITENANT" -n ai-tenants -o json 2>/dev/null |
      jq -e --slurpfile original "$original" '(.metadata.labels // {}) == ($original[0].metadata.labels // {}) and (.metadata.annotations // {}) == ($original[0].metadata.annotations // {}) and (.spec // {}) == ($original[0].spec // {})' >/dev/null; then
      restored=true
      break
    fi
    sleep 3
  done
  [[ "$restored" == true ]] || { echo "shared AITenant metadata did not restore" >&2; exit 1; }
  echo "shared MaaS AITenant metadata restored; tenant and namespace retained" >"$OUT/shared-aitenant-restored.txt"
  AITENANT_RETAINED=true
fi
if [[ "$AITENANT_RETAINED" != true ]] && "${OC[@]}" get aitenant "$AITENANT" -n ai-tenants -o name >/dev/null 2>&1; then
  if ! "${OC[@]}" get deployment ai-gateway-controller -n "$OPENSHIFT_E2E_CONTROLLER_NAMESPACE" -o json >"$OUT/controller-before-finalization.json" 2>/dev/null; then
    echo "refusing cleanup: run-owned controller is unavailable before AITenant finalization" >&2
    echo "PARTIAL: $OUT" >&2
    exit 1
  fi
  if ! "${OC[@]}" get gateway "$OPENSHIFT_E2E_GATEWAY_NAME" -n "$OPENSHIFT_E2E_GATEWAY_NAMESPACE" -o json >"$OUT/gateway-before-finalization.json" 2>/dev/null; then
    echo "refusing cleanup: run-owned Gateway is unavailable before AITenant finalization" >&2
    echo "PARTIAL: $OUT" >&2
    exit 1
  fi
  "${OC[@]}" delete aitenant "$AITENANT" -n ai-tenants --wait=false >/dev/null
  deadline=$((SECONDS + 300))
  finalized=false
  while (( SECONDS < deadline )); do
    "${OC[@]}" get aitenant "$AITENANT" -n ai-tenants -o json >"$OUT/aitenant-finalization.json" 2>/dev/null || {
      finalized=true
      break
    }
    jq -c '{name:.metadata.name,deletionTimestamp:.metadata.deletionTimestamp,finalizers:(.metadata.finalizers // []),conditions:(.status.conditions // [])}' "$OUT/aitenant-finalization.json" >"$OUT/aitenant-finalization-summary.json"
    "${OC[@]}" logs -n "$OPENSHIFT_E2E_CONTROLLER_NAMESPACE" deploy/ai-gateway-controller --tail=80 >"$OUT/controller-finalization.log" 2>&1 || true
    "${OC[@]}" logs -n redhat-ods-applications deploy/maas-controller --tail=80 >"$OUT/maas-finalization.log" 2>&1 || true
    sleep 5
  done
  if [[ "$finalized" != true ]]; then
    echo "AITenant finalization timed out; leaving controller, Gateway, RBAC, and namespaces intact" >&2
    echo "PARTIAL: $OUT" >&2
    exit 1
  fi
  echo "AITenant finalized before secondary cleanup" >"$OUT/finalization-order.txt"
fi

# At this point the AITenant is gone.  Only run-owned residual resources may be
# removed, and every namespace was ownership-checked above.
for ns in "$OPENSHIFT_E2E_CONTROLLER_NAMESPACE" "$OPENSHIFT_E2E_TENANT_NAMESPACE" "$OPENSHIFT_E2E_BACKEND_NAMESPACE"; do
  if [[ "$SHARED_AITENANT" == true && "$ns" == models-as-a-service ]]; then
    "${OC[@]}" get all -n "$ns" -l "external-model-praxis.opendatahub.io/run-id=$OPENSHIFT_E2E_RUN_ID" -o json >"$OUT/$ns-run-owned-before.json" 2>/dev/null || :
    for resource in maasauthpolicy maassubscription maasmodelref externalmodel externalprovider pod configmap secret; do
      "${OC[@]}" delete "$resource" -n "$ns" -l "external-model-praxis.opendatahub.io/run-id=$OPENSHIFT_E2E_RUN_ID" --ignore-not-found >/dev/null 2>&1 || cleanup_failed=1
    done
    continue
  fi
  "${OC[@]}" get all -n "$ns" -o json >"$OUT/$ns-before.json" 2>/dev/null || :
  if ! "${OC[@]}" delete namespace "$ns" --ignore-not-found --wait=true --timeout=5m >/dev/null 2>&1; then
    cleanup_failed=1
    echo "namespace cleanup failed: $ns" >>"$OUT/cleanup-errors.txt"
  fi
done
if ! "${OC[@]}" delete gateway "$OPENSHIFT_E2E_GATEWAY_NAME" -n "$OPENSHIFT_E2E_GATEWAY_NAMESPACE" --ignore-not-found >/dev/null 2>&1; then
  cleanup_failed=1
  echo "Gateway cleanup failed: $OPENSHIFT_E2E_GATEWAY_NAMESPACE/$OPENSHIFT_E2E_GATEWAY_NAME" >>"$OUT/cleanup-errors.txt"
fi
if ! "${OC[@]}" delete secret "$GATEWAY_TLS_SECRET" -n "$OPENSHIFT_E2E_GATEWAY_NAMESPACE" --ignore-not-found >/dev/null 2>&1; then
  cleanup_failed=1
  echo "Gateway TLS Secret cleanup failed: $OPENSHIFT_E2E_GATEWAY_NAMESPACE/$GATEWAY_TLS_SECRET" >>"$OUT/cleanup-errors.txt"
fi
if ! "${OC[@]}" delete route "$OPENSHIFT_E2E_REGISTRY_ROUTE" -n openshift-image-registry --ignore-not-found >/dev/null 2>&1; then
  cleanup_failed=1
  echo "registry Route cleanup failed: openshift-image-registry/$OPENSHIFT_E2E_REGISTRY_ROUTE" >>"$OUT/cleanup-errors.txt"
fi
if "${OC[@]}" get securitycontextconstraints "xmp-istio-$OPENSHIFT_E2E_RUN_ID" -o json >"$STATE/destroy-istio-scc.json" 2>/dev/null; then
  jq -e --arg run "$OPENSHIFT_E2E_RUN_ID" '.metadata.labels["external-model-praxis.opendatahub.io/run-id"] == $run' "$STATE/destroy-istio-scc.json" >/dev/null || { echo "Istio SCC ownership check failed" >&2; exit 1; }
    if ! "${OC[@]}" delete securitycontextconstraints "xmp-istio-$OPENSHIFT_E2E_RUN_ID" >/dev/null; then
      cleanup_failed=1
      echo "Istio SCC cleanup failed" >>"$OUT/cleanup-errors.txt"
    fi
fi
for kind_name in "clusterrole/xmp-controller-role-$OPENSHIFT_E2E_RUN_ID" "clusterrolebinding/xmp-controller-$OPENSHIFT_E2E_RUN_ID"; do
  kind=${kind_name%%/*}; name=${kind_name#*/}
  if "${OC[@]}" get "$kind" "$name" -o json >"$STATE/destroy-$kind-$name.json" 2>/dev/null; then
    jq -e --arg run "$OPENSHIFT_E2E_RUN_ID" '.metadata.labels["external-model-praxis.opendatahub.io/run-id"] == $run' "$STATE/destroy-$kind-$name.json" >/dev/null || { echo "ownership check failed: $kind/$name" >&2; exit 1; }
    if ! "${OC[@]}" delete "$kind" "$name" >/dev/null; then
      cleanup_failed=1
      echo "cleanup failed: $kind/$name" >>"$OUT/cleanup-errors.txt"
    fi
  fi
done

# Restore the shared Authorino configuration before removing the run-owned CA
# bundle. Refuse to patch a replacement Authorino object.
if [[ -s "$STATE/authorino-original-volumes.json" ]]; then
  authorino_uid=$("${OC[@]}" get authorino authorino -n kuadrant-system -o jsonpath='{.metadata.uid}' 2>/dev/null || true)
  original_authorino_uid=$(jq -r '.uid' "$STATE/authorino-original-volumes.json")
  if [[ -z "$authorino_uid" || "$authorino_uid" != "$original_authorino_uid" ]]; then
    cleanup_failed=1
    echo "shared Authorino identity changed; refusing volume restoration" >>"$OUT/cleanup-errors.txt"
  else
    jq -c '{spec:{volumes:.volumes}}' "$STATE/authorino-original-volumes.json" >"$OUT/authorino-volume-restore-patch.json"
    if ! "${OC[@]}" patch authorino authorino -n kuadrant-system --type=merge --patch-file "$OUT/authorino-volume-restore-patch.json" >"$OUT/authorino-volume-restore.log"; then
      cleanup_failed=1
      echo "shared Authorino volume restoration failed" >>"$OUT/cleanup-errors.txt"
    fi
  fi
fi
if "${OC[@]}" get configmap "xmp-service-ca-$OPENSHIFT_E2E_RUN_ID" -n kuadrant-system -o json >"$OUT/authorino-service-ca-configmap.json" 2>/dev/null; then
  if jq -e --arg run "$OPENSHIFT_E2E_RUN_ID" '.metadata.labels["external-model-praxis.opendatahub.io/run-id"] == $run' "$OUT/authorino-service-ca-configmap.json" >/dev/null; then
    "${OC[@]}" delete configmap "xmp-service-ca-$OPENSHIFT_E2E_RUN_ID" -n kuadrant-system >/dev/null || cleanup_failed=1
  else
    cleanup_failed=1
    echo "Authorino CA ConfigMap ownership check failed" >>"$OUT/cleanup-errors.txt"
  fi
fi
if "${OC[@]}" get configmap "xmp-provider-ca-$OPENSHIFT_E2E_RUN_ID" -n "$OPENSHIFT_E2E_GATEWAY_NAMESPACE" -o json >"$OUT/provider-ca-configmap.json" 2>/dev/null; then
  if jq -e --arg run "$OPENSHIFT_E2E_RUN_ID" '.metadata.labels["external-model-praxis.opendatahub.io/run-id"] == $run and .metadata.labels["app.kubernetes.io/managed-by"] == "external-model-praxis-openshift-e2e"' "$OUT/provider-ca-configmap.json" >/dev/null; then
    "${OC[@]}" delete configmap "xmp-provider-ca-$OPENSHIFT_E2E_RUN_ID" -n "$OPENSHIFT_E2E_GATEWAY_NAMESPACE" >/dev/null || cleanup_failed=1
  else
    cleanup_failed=1
    echo "provider CA ConfigMap ownership check failed" >>"$OUT/cleanup-errors.txt"
  fi
fi
for resource in service/maas-api deployment/maas-api-callback-proxy configmap/maas-api-callback-proxy; do
  filename=${resource//\//-}
  if "${OC[@]}" get "$resource" -n maas-system -o json >"$OUT/$filename.json" 2>/dev/null; then
    if jq -e --arg run "$OPENSHIFT_E2E_RUN_ID" '.metadata.labels["external-model-praxis.opendatahub.io/run-id"] == $run' "$OUT/$filename.json" >/dev/null; then
      "${OC[@]}" delete "$resource" -n maas-system >/dev/null || cleanup_failed=1
    elif [[ "$resource" != service/maas-api ]]; then
      cleanup_failed=1
      echo "compatibility resource ownership check failed: $resource" >>"$OUT/cleanup-errors.txt"
    fi
  fi
done
for snapshot in "$STATE"/serviceaccount-original-maas-system-*.json; do
  [[ -s "$snapshot" ]] || continue
  sa=$(jq -r '.name' "$snapshot")
  expected_uid=$(jq -r '.uid' "$snapshot")
  current_uid=$("${OC[@]}" get serviceaccount "$sa" -n maas-system -o jsonpath='{.metadata.uid}' 2>/dev/null || true)
  if [[ -z "$current_uid" || "$current_uid" != "$expected_uid" ]]; then
    cleanup_failed=1
    echo "shared ServiceAccount identity changed; refusing restoration: maas-system/$sa" >>"$OUT/cleanup-errors.txt"
    continue
  fi
  jq -c '{imagePullSecrets:.imagePullSecrets}' "$snapshot" >"$OUT/serviceaccount-$sa-restore-patch.json"
  if ! "${OC[@]}" patch serviceaccount "$sa" -n maas-system --type=merge --patch-file "$OUT/serviceaccount-$sa-restore-patch.json" >"$OUT/serviceaccount-$sa-restore.log"; then
    cleanup_failed=1
    echo "shared ServiceAccount restoration failed: maas-system/$sa" >>"$OUT/cleanup-errors.txt"
  fi
done
if "${OC[@]}" get secret "$PULL_SECRET" -n maas-system -o json >"$OUT/maas-system-registry-pull.json" 2>/dev/null; then
  if jq -e --arg run "$OPENSHIFT_E2E_RUN_ID" '.metadata.labels["external-model-praxis.opendatahub.io/run-id"] == $run' "$OUT/maas-system-registry-pull.json" >/dev/null; then
    "${OC[@]}" delete secret "$PULL_SECRET" -n maas-system >/dev/null || cleanup_failed=1
  else
    cleanup_failed=1
    echo "shared registry pull Secret ownership check failed" >>"$OUT/cleanup-errors.txt"
  fi
fi
"${OC[@]}" get namespace "$OPENSHIFT_E2E_CONTROLLER_NAMESPACE" "$OPENSHIFT_E2E_TENANT_NAMESPACE" "$OPENSHIFT_E2E_BACKEND_NAMESPACE" -o name >"$OUT/remaining.txt" 2>/dev/null || :
if "${OC[@]}" get gateway "$OPENSHIFT_E2E_GATEWAY_NAME" -n "$OPENSHIFT_E2E_GATEWAY_NAMESPACE" >/dev/null 2>&1; then
  cleanup_failed=1
  echo "run-owned resource remains: gateway/$OPENSHIFT_E2E_GATEWAY_NAMESPACE/$OPENSHIFT_E2E_GATEWAY_NAME" >>"$OUT/cleanup-errors.txt"
fi
if "${OC[@]}" get route "$OPENSHIFT_E2E_REGISTRY_ROUTE" -n openshift-image-registry >/dev/null 2>&1; then
  cleanup_failed=1
  echo "run-owned resource remains: route/openshift-image-registry/$OPENSHIFT_E2E_REGISTRY_ROUTE" >>"$OUT/cleanup-errors.txt"
fi
for resource in \
  "securitycontextconstraints/xmp-istio-$OPENSHIFT_E2E_RUN_ID" \
  "clusterrole/xmp-controller-role-$OPENSHIFT_E2E_RUN_ID" \
  "clusterrolebinding/xmp-controller-$OPENSHIFT_E2E_RUN_ID"; do
  if "${OC[@]}" get "$resource" >/dev/null 2>&1; then
    cleanup_failed=1
    echo "run-owned resource remains: $resource" >>"$OUT/cleanup-errors.txt"
  fi
done
if [[ "$SHARED_AITENANT" == true ]]; then
  # The shared MaaS AITenant namespace is intentionally retained; only
  # dedicated run namespaces count as cleanup residue in this mode.
  sed -i '/^namespace\/models-as-a-service$/d' "$OUT/remaining.txt"
fi
if [[ -s "$OUT/remaining.txt" || "$cleanup_failed" -ne 0 ]]; then
  echo "PARTIAL: run-owned resources remain or cleanup failed; diagnostics: $OUT" >&2
  exit 1
fi
printf '%s\n' "$OUT"
