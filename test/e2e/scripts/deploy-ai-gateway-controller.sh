#!/usr/bin/env bash
# Deploy (or upgrade) ai-gateway-controller on an existing MaaS cluster for e2e.
#
# Required:
#   AI_GATEWAY_CONTROLLER_IMAGE — manager image to run (Konflux PR image in CI)
#
# Optional:
#   AI_GATEWAY_CONTROLLER_NAMESPACE — default opendatahub
#   GATEWAY_NAMESPACE               — default openshift-ingress
#   GATEWAY_NAME                    — default maas-default-gateway
#   PRAXIS_EXTPROC_IMAGE            — praxis dataplane image (default from params.env)
#   REMOVE_MAAS_IPP                 — default true; delete maas-controller legacy IPP
#                                     and hand ext_proc to ai-gateway-controller
#   PRAXIS_INSTALL_TIMEOUT          — default 180s
#   SCALE_DOWN_PAYLOAD_PROCESSING   — deprecated alias for REMOVE_MAAS_IPP

set -euo pipefail

_find_project_root() {
  local dir="${1:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)}"
  while [[ "$dir" != "/" && ! -e "$dir/.git" ]]; do dir="$(dirname "$dir")"; done
  [[ -e "$dir/.git" ]] && printf '%s\n' "$dir" || return 1
}

PROJECT_ROOT="$(_find_project_root)"
AI_GATEWAY_CONTROLLER_IMAGE="${AI_GATEWAY_CONTROLLER_IMAGE:-}"
AI_GATEWAY_CONTROLLER_NAMESPACE="${AI_GATEWAY_CONTROLLER_NAMESPACE:-opendatahub}"
GATEWAY_NAMESPACE="${GATEWAY_NAMESPACE:-openshift-ingress}"
GATEWAY_NAME="${GATEWAY_NAME:-maas-default-gateway}"
DEPLOYMENT_NAMESPACE="${DEPLOYMENT_NAMESPACE:-opendatahub}"
MAAS_CONTROLLER_DEPLOYMENT="${MAAS_CONTROLLER_DEPLOYMENT:-maas-controller}"
MAAS_CONTROLLER_PRIOR_REPLICAS=""
MAAS_CONTROLLER_PAUSED=false
MAAS_CONTROLLER_RESUME_REPLICAS="${MAAS_CONTROLLER_RESUME_REPLICAS:-1}"

REMOVE_MAAS_IPP="${REMOVE_MAAS_IPP:-${SCALE_DOWN_PAYLOAD_PROCESSING:-true}}"
PRAXIS_INSTALL_TIMEOUT="${PRAXIS_INSTALL_TIMEOUT:-300}"
MANAGED_FALSE_ANNOTATION="${MANAGED_FALSE_ANNOTATION:-opendatahub.io/managed=false}"
AITENANT_NAME="${AITENANT_NAME:-models-as-a-service}"
AITENANT_NAMESPACE="${AITENANT_NAMESPACE:-ai-tenants}"
# Praxis opt-in and PraxisCleanupFinalizer live on MaasTenantConfig, not AITenant.
MAAS_TENANT_CONFIG_NAME="${MAAS_TENANT_CONFIG_NAME:-default-tenant}"
MAAS_SUBSCRIPTION_NAMESPACE="${MAAS_SUBSCRIPTION_NAMESPACE:-models-as-a-service}"

_default_praxis_image() {
  local params="${PROJECT_ROOT}/config/self/default/params.env"
  if [[ -f "${params}" ]]; then
    grep -E '^praxis-extproc-image=' "${params}" | cut -d= -f2- || true
  fi
}
_derive_praxis_image_from_controller() {
  # When CI passes AI_GATEWAY_CONTROLLER_IMAGE@digest with a companion tag (AIGC_TAG /
  # image_tag in the Konflux snapshot), prefer the matching praxis PR tag over odh-stable.
  local controller_image="${AI_GATEWAY_CONTROLLER_IMAGE:-}"
  local explicit_tag="${AIGC_IMAGE_TAG:-${PRAXIS_EXTPROC_TAG:-}}"
  if [[ -n "$explicit_tag" ]]; then
    echo "quay.io/opendatahub/odh-praxis-extproc:${explicit_tag}"
    return
  fi
  if [[ "$controller_image" == *@sha256:* ]]; then
    local ref="${controller_image%%@*}"
    local tag="${ref##*:}"
    if [[ "$tag" != "$ref" && "$tag" != odh-stable ]]; then
      echo "quay.io/opendatahub/odh-praxis-extproc:${tag}"
    fi
  fi
}
# Prow/Tekton may set PRAXIS_EXTPROC_IMAGE via maas-image-defaults.sh or Konflux snapshot.
# When unset, fall back to params.env, controller tag alignment, then odh-stable.
PRAXIS_EXTPROC_IMAGE="${PRAXIS_EXTPROC_IMAGE:-$(_default_praxis_image)}"
PRAXIS_EXTPROC_IMAGE="${PRAXIS_EXTPROC_IMAGE:-$(_derive_praxis_image_from_controller)}"
PRAXIS_EXTPROC_IMAGE="${PRAXIS_EXTPROC_IMAGE:-quay.io/opendatahub/odh-praxis-extproc:odh-stable}"

# Default-tenant IPP object names in the gateway namespace (maas-controller + praxis share these).
IPP_NAMES=(
  payload-processing
  payload-pre-processing
)

if [[ -z "${AI_GATEWAY_CONTROLLER_IMAGE}" ]]; then
  echo "ERROR: AI_GATEWAY_CONTROLLER_IMAGE is required" >&2
  exit 1
fi

_on_deploy_exit() {
  local code=$?
  if [[ $code -ne 0 && -n "${MAAS_CONTROLLER_PRIOR_REPLICAS:-}" && "${MAAS_CONTROLLER_PAUSED:-}" == "true" ]]; then
    echo "Deploy failed; attempting to resume maas-controller ..."
    _resume_maas_controller || true
  fi
}
trap _on_deploy_exit EXIT

_pause_maas_controller() {
  local replicas
  replicas="$(oc get deployment "${MAAS_CONTROLLER_DEPLOYMENT}" -n "${DEPLOYMENT_NAMESPACE}" \
    -o jsonpath='{.spec.replicas}' 2>/dev/null || echo "")"
  if [[ -z "${replicas}" ]]; then
    echo "WARN: ${MAAS_CONTROLLER_DEPLOYMENT} not found in ${DEPLOYMENT_NAMESPACE}; skipping pause"
    MAAS_CONTROLLER_PRIOR_REPLICAS=""
    return 0
  fi
  MAAS_CONTROLLER_PRIOR_REPLICAS="${replicas}"
  if [[ "${replicas}" == "0" ]]; then
    MAAS_CONTROLLER_PRIOR_REPLICAS="${MAAS_CONTROLLER_RESUME_REPLICAS}"
    echo "maas-controller already scaled to 0; will resume to ${MAAS_CONTROLLER_PRIOR_REPLICAS} after handoff"
    return 0
  fi
  echo "Pausing maas-controller (scale ${replicas} -> 0) to avoid IPP reconcile during handoff ..."
  oc scale deployment "${MAAS_CONTROLLER_DEPLOYMENT}" -n "${DEPLOYMENT_NAMESPACE}" --replicas=0
  oc rollout status deployment/"${MAAS_CONTROLLER_DEPLOYMENT}" -n "${DEPLOYMENT_NAMESPACE}" --timeout=180s
  MAAS_CONTROLLER_PAUSED=true
}

_resume_maas_controller() {
  [[ -n "${MAAS_CONTROLLER_PRIOR_REPLICAS:-}" ]] || return 0
  if [[ "${MAAS_CONTROLLER_PRIOR_REPLICAS}" == "0" ]]; then
    return 0
  fi
  echo "Resuming maas-controller (scale -> ${MAAS_CONTROLLER_PRIOR_REPLICAS}) ..."
  oc scale deployment "${MAAS_CONTROLLER_DEPLOYMENT}" -n "${DEPLOYMENT_NAMESPACE}" \
    --replicas="${MAAS_CONTROLLER_PRIOR_REPLICAS}"
  oc rollout status deployment/"${MAAS_CONTROLLER_DEPLOYMENT}" -n "${DEPLOYMENT_NAMESPACE}" --timeout=180s || true
  MAAS_CONTROLLER_PAUSED=false
}

# PraxisCleanupFinalizer is written on MaasTenantConfig. Keep maas-controller
# running so its webhook can admit that update and so it can finish legacy IPP
# cleanup (payload-processing-status=cleanup-complete) before aigc deploys.
_resume_maas_controller_for_aitenant_webhook() {
  if [[ "${MAAS_CONTROLLER_PAUSED:-}" != "true" ]]; then
    return 0
  fi
  echo "Resuming maas-controller so the MaasTenantConfig webhook stays available ..."
  _resume_maas_controller
}

_tenant_config_namespace() {
  local ns
  ns="$(oc get aitenant "${AITENANT_NAME}" -n "${AITENANT_NAMESPACE}" \
    -o jsonpath='{.status.tenantNamespace}' 2>/dev/null || true)"
  if [[ -n "${ns}" ]]; then
    printf '%s\n' "${ns}"
  else
    printf '%s\n' "${MAAS_SUBSCRIPTION_NAMESPACE}"
  fi
}

_delete_legacy_ipp_in_gateway_namespace() {
  echo "Removing maas-controller legacy IPP in ${GATEWAY_NAMESPACE} ..."
  local name
  for name in "${IPP_NAMES[@]}"; do
    oc delete deployment,service,destinationrule "${name}" -n "${GATEWAY_NAMESPACE}" --ignore-not-found --wait=false 2>/dev/null || true
  done
  oc delete envoyfilter payload-processing -n "${GATEWAY_NAMESPACE}" --ignore-not-found --wait=false 2>/dev/null || true
  oc delete networkpolicy payload-processing -n "${GATEWAY_NAMESPACE}" --ignore-not-found --wait=false 2>/dev/null || true
  oc delete serviceaccount payload-processing -n "${GATEWAY_NAMESPACE}" --ignore-not-found --wait=false 2>/dev/null || true
  oc delete configmap payload-processing-plugins -n "${GATEWAY_NAMESPACE}" --ignore-not-found --wait=false 2>/dev/null || true
  oc delete clusterrolebinding payload-processing-reader --ignore-not-found --wait=false 2>/dev/null || true
  oc delete hpa -n "${GATEWAY_NAMESPACE}" -l app.kubernetes.io/name=payload-processing --ignore-not-found 2>/dev/null || true

  local deadline=$((SECONDS + 120))
  while [[ $SECONDS -lt $deadline ]]; do
    if ! oc get deployment payload-processing -n "${GATEWAY_NAMESPACE}" &>/dev/null \
      && ! oc get deployment payload-pre-processing -n "${GATEWAY_NAMESPACE}" &>/dev/null \
      && ! oc get serviceaccount payload-processing -n "${GATEWAY_NAMESPACE}" &>/dev/null \
      && ! oc get configmap payload-processing-plugins -n "${GATEWAY_NAMESPACE}" &>/dev/null \
      && ! oc get clusterrolebinding payload-processing-reader &>/dev/null; then
      echo "Legacy IPP resources removed from ${GATEWAY_NAMESPACE}"
      return 0
    fi
    sleep 2
  done
  echo "ERROR: legacy IPP resources still present after delete" >&2
  oc get deployment,service,serviceaccount,configmap,envoyfilter,destinationrule,networkpolicy \
    -n "${GATEWAY_NAMESPACE}" 2>/dev/null | grep payload || true
  oc get clusterrolebinding payload-processing-reader 2>/dev/null || true
  return 1
}

_apply_ai_gateway_controller() {
  local work_dir
  work_dir="$(mktemp -d -t aigc-kustomize.XXXXXXXXXX)"

  cp -a "${PROJECT_ROOT}/config/self" "${work_dir}/self"
  sed -i "s|^ai-gateway-controller-image=.*|ai-gateway-controller-image=${AI_GATEWAY_CONTROLLER_IMAGE}|" \
    "${work_dir}/self/default/params.env"
  sed -i "s|^praxis-extproc-image=.*|praxis-extproc-image=${PRAXIS_EXTPROC_IMAGE}|" \
    "${work_dir}/self/default/params.env"

  if command -v kustomize >/dev/null 2>&1; then
    kustomize build "${work_dir}/self/default"
  elif kubectl kustomize "${work_dir}/self/default" >/dev/null 2>&1; then
    kubectl kustomize "${work_dir}/self/default"
  else
    rm -rf "${work_dir}"
    echo "ERROR: kustomize or kubectl kustomize required" >&2
    return 1
  fi | \
    sed \
      -e "s|value: maas-default-gateway|value: ${GATEWAY_NAME}|g" \
      -e "s|value: openshift-ingress|value: ${GATEWAY_NAMESPACE}|g" \
    | oc apply -f -

  rm -rf "${work_dir}"

  oc delete configmap ai-gateway-controller-parameters -n default --ignore-not-found 2>/dev/null || true

  oc rollout status deployment/ai-gateway-controller \
    -n "${AI_GATEWAY_CONTROLLER_NAMESPACE}" --timeout=180s

  echo "Triggering immediate praxis-extproc install ..."
  oc rollout restart deployment/ai-gateway-controller -n "${AI_GATEWAY_CONTROLLER_NAMESPACE}"
  oc rollout status deployment/ai-gateway-controller \
    -n "${AI_GATEWAY_CONTROLLER_NAMESPACE}" --timeout=180s
}

_wait_for_aigc_praxis_reconcile() {
  local ns
  ns="$(_tenant_config_namespace)"
  echo "Waiting for ai-gateway-controller to attach praxis finalizer on MaasTenantConfig ${ns}/${MAAS_TENANT_CONFIG_NAME} ..."
  local deadline=$((SECONDS + 120))
  while [[ $SECONDS -lt $deadline ]]; do
    if oc get maastenantconfig "${MAAS_TENANT_CONFIG_NAME}" -n "${ns}" \
      -o jsonpath='{.metadata.finalizers}' 2>/dev/null | grep -q 'ai-gateway-controller.opendatahub.io/praxis-cleanup'; then
      echo "ai-gateway-controller praxis reconcile started (finalizer present on MaasTenantConfig)"
      return 0
    fi
    sleep 3
  done
  echo "WARN: praxis finalizer not observed on MaasTenantConfig ${ns}/${MAAS_TENANT_CONFIG_NAME} within 120s; continuing praxis wait anyway" >&2
  oc get maastenantconfig "${MAAS_TENANT_CONFIG_NAME}" -n "${ns}" \
    -o jsonpath='type={.metadata.annotations.maas\.opendatahub\.io/payload-processing-type} status={.metadata.annotations.maas\.opendatahub\.io/payload-processing-status}{"\n"}' \
    2>/dev/null || true
}

_remove_stale_payload_processing_before_wait() {
  if ! oc get deployment payload-processing -n "${GATEWAY_NAMESPACE}" &>/dev/null; then
    return 0
  fi
  local image args
  image="$(oc get deployment payload-processing -n "${GATEWAY_NAMESPACE}" \
    -o jsonpath='{.spec.template.spec.containers[0].image}' 2>/dev/null || true)"
  args="$(oc get deployment payload-processing -n "${GATEWAY_NAMESPACE}" \
    -o jsonpath='{.spec.template.spec.containers[0].args}' 2>/dev/null || true)"
  if [[ "${image}" == "${PRAXIS_EXTPROC_IMAGE}" ]] \
    && [[ "${args}" == *"/etc/praxis/extproc.yaml"* ]]; then
    return 0
  fi
  echo "Removing stale payload-processing (image=${image:-<none>}) before praxis install ..."
  _delete_legacy_ipp_in_gateway_namespace
  local ns
  ns="$(_tenant_config_namespace)"
  oc annotate maastenantconfig "${MAAS_TENANT_CONFIG_NAME}" -n "${ns}" \
    "reconcile-trigger=$(date +%s)" --overwrite 2>/dev/null || true
}

_wait_for_praxis_extproc() {
  echo "Waiting for shared praxis-extproc (${PRAXIS_EXTPROC_IMAGE}) in ${GATEWAY_NAMESPACE}: payload-processing and payload-pre-processing (timeout: ${PRAXIS_INSTALL_TIMEOUT}s) ..."
  local deadline=$((SECONDS + PRAXIS_INSTALL_TIMEOUT))
  while [[ $SECONDS -lt $deadline ]]; do
    local post_image post_args post_ready pre_image pre_args pre_ready
    if ! oc get deployment payload-processing -n "${GATEWAY_NAMESPACE}" &>/dev/null \
      || ! oc get deployment payload-pre-processing -n "${GATEWAY_NAMESPACE}" &>/dev/null; then
      local ns status
      ns="$(_tenant_config_namespace)"
      status="$(oc get maastenantconfig "${MAAS_TENANT_CONFIG_NAME}" -n "${ns}" \
        -o jsonpath='type={.metadata.annotations.maas\.opendatahub\.io/payload-processing-type} status={.metadata.annotations.maas\.opendatahub\.io/payload-processing-status}' \
        2>/dev/null || true)"
      echo "  Waiting... shared payload-processing deployments not created in ${GATEWAY_NAMESPACE} yet (${status:-MaasTenantConfig unread})"
      sleep 5
      continue
    fi
    post_image="$(oc get deployment payload-processing -n "${GATEWAY_NAMESPACE}" \
      -o jsonpath='{.spec.template.spec.containers[0].image}' 2>/dev/null || true)"
    post_args="$(oc get deployment payload-processing -n "${GATEWAY_NAMESPACE}" \
      -o jsonpath='{.spec.template.spec.containers[0].args}' 2>/dev/null || true)"
    post_ready="$(oc get deployment payload-processing -n "${GATEWAY_NAMESPACE}" \
      -o jsonpath='{.status.readyReplicas}' 2>/dev/null || echo "0")"
    pre_image="$(oc get deployment payload-pre-processing -n "${GATEWAY_NAMESPACE}" \
      -o jsonpath='{.spec.template.spec.containers[0].image}' 2>/dev/null || true)"
    pre_args="$(oc get deployment payload-pre-processing -n "${GATEWAY_NAMESPACE}" \
      -o jsonpath='{.spec.template.spec.containers[0].args}' 2>/dev/null || true)"
    pre_ready="$(oc get deployment payload-pre-processing -n "${GATEWAY_NAMESPACE}" \
      -o jsonpath='{.status.readyReplicas}' 2>/dev/null || echo "0")"

    if [[ "${post_image}" == "${PRAXIS_EXTPROC_IMAGE}" ]] \
      && [[ "${post_args}" == *"/etc/praxis/extproc.yaml"* ]] \
      && [[ "${post_ready:-0}" -ge 1 ]] \
      && [[ "${pre_image}" == "${PRAXIS_EXTPROC_IMAGE}" ]] \
      && [[ "${pre_args}" == *"/etc/praxis/pre-extproc.yaml"* ]] \
      && [[ "${pre_ready:-0}" -ge 1 ]]; then
      echo "✅ shared praxis-extproc ready: ${GATEWAY_NAMESPACE}/payload-processing and ${GATEWAY_NAMESPACE}/payload-pre-processing"
      return 0
    fi
    echo "  Waiting... payload-processing image=${post_image:-<none>} ready=${post_ready:-0}; payload-pre-processing image=${pre_image:-<none>} ready=${pre_ready:-0}"
    sleep 5
  done

  echo "ERROR: shared praxis-extproc deployments not ready after ${PRAXIS_INSTALL_TIMEOUT}s" >&2
  oc get deployment -n "${GATEWAY_NAMESPACE}" | grep payload || true
  oc get pods -n "${GATEWAY_NAMESPACE}" | grep payload || true
  local pod
  pod="$(oc get pods -n "${GATEWAY_NAMESPACE}" -o name 2>/dev/null | grep payload-processing | head -1 || true)"
  if [[ -n "${pod}" ]]; then
    oc describe "${pod}" -n "${GATEWAY_NAMESPACE}" 2>&1 | tail -20 || true
  fi
  oc logs deployment/ai-gateway-controller -n "${AI_GATEWAY_CONTROLLER_NAMESPACE}" --tail=30 2>&1 || true
  return 1
}

_select_praxis_on_default_tenant() {
  local ns
  ns="$(_tenant_config_namespace)"
  if ! oc get maastenantconfig "${MAAS_TENANT_CONFIG_NAME}" -n "${ns}" &>/dev/null; then
    echo "ERROR: MaasTenantConfig ${ns}/${MAAS_TENANT_CONFIG_NAME} not found; cannot opt into praxis" >&2
    oc get maastenantconfig -A 2>/dev/null || true
    return 1
  fi

  # aigc only reads these from MaasTenantConfig (never AITenant). Select ExtProc
  # with type=praxis and clear payload-processing-status in the same write:
  # EnsurePraxisMayDeploy only keeps the deploy gate closed while the status is
  # absent — both cleanup-complete and steady release aigc. A re-run (or any
  # prior handoff) leaves the annotation behind, so without this clear aigc
  # would create its own (same-named) payload-processing while
  # _delete_legacy_ipp_in_gateway_namespace is still waiting for the legacy one
  # to disappear. type=praxis is also the signal for maas-controller to stop
  # managing the legacy IPP, so it will not recreate it during the delete.
  echo "Annotating MaasTenantConfig ${ns}/${MAAS_TENANT_CONFIG_NAME}: type=praxis, clearing payload-processing-status ..."
  oc annotate maastenantconfig "${MAAS_TENANT_CONFIG_NAME}" -n "${ns}" \
    maas.opendatahub.io/payload-processing-type=praxis \
    maas.opendatahub.io/payload-processing-status- \
    --overwrite

  local type status
  type="$(oc get maastenantconfig "${MAAS_TENANT_CONFIG_NAME}" -n "${ns}" \
    -o jsonpath='{.metadata.annotations.maas\.opendatahub\.io/payload-processing-type}' 2>/dev/null || true)"
  if [[ "${type}" != "praxis" ]]; then
    echo "ERROR: payload-processing-type did not stick (got '${type}')" >&2
    return 1
  fi
  status="$(oc get maastenantconfig "${MAAS_TENANT_CONFIG_NAME}" -n "${ns}" \
    -o jsonpath='{.metadata.annotations.maas\.opendatahub\.io/payload-processing-status}' 2>/dev/null || true)"
  if [[ -n "${status}" ]]; then
    echo "ERROR: payload-processing-status is still '${status}'; the praxis deploy gate would be open during legacy IPP removal" >&2
    return 1
  fi
  echo "MaasTenantConfig praxis selected: type=${type}, status=<absent> (deploy gate closed)"
}

_clear_praxis_cleanup_on_default_tenant() {
  local ns
  ns="$(_tenant_config_namespace)"

  # #1508 clear-to-claim handoff: only now — after the legacy IPP delete has
  # converged — do we release aigc to claim steady and create the shared
  # payload-processing. In e2e we set cleanup-complete ourselves because
  # maas-controller may not write it before our wait times out.
  echo "Annotating MaasTenantConfig ${ns}/${MAAS_TENANT_CONFIG_NAME}: status=cleanup-complete ..."
  oc annotate maastenantconfig "${MAAS_TENANT_CONFIG_NAME}" -n "${ns}" \
    maas.opendatahub.io/payload-processing-status=cleanup-complete \
    --overwrite

  local status
  status="$(oc get maastenantconfig "${MAAS_TENANT_CONFIG_NAME}" -n "${ns}" \
    -o jsonpath='{.metadata.annotations.maas\.opendatahub\.io/payload-processing-status}' 2>/dev/null || true)"
  if [[ "${status}" != "cleanup-complete" && "${status}" != "steady" ]]; then
    echo "ERROR: payload-processing-status did not stick (got '${status}')" >&2
    return 1
  fi
  echo "MaasTenantConfig cleanup cleared: status=${status}"
}

_protect_praxis_from_maas_reconcile() {
  echo "Annotating praxis IPP resources ${MANAGED_FALSE_ANNOTATION} so maas-controller skips them ..."
  local name kind
  for name in "${IPP_NAMES[@]}"; do
    for kind in deployment service destinationrule; do
      oc annotate "${kind}" "${name}" -n "${GATEWAY_NAMESPACE}" \
        "${MANAGED_FALSE_ANNOTATION}" --overwrite 2>/dev/null || true
    done
  done
  oc annotate envoyfilter payload-processing -n "${GATEWAY_NAMESPACE}" \
    "${MANAGED_FALSE_ANNOTATION}" --overwrite 2>/dev/null || true
  oc annotate networkpolicy payload-processing -n "${GATEWAY_NAMESPACE}" \
    "${MANAGED_FALSE_ANNOTATION}" --overwrite 2>/dev/null || true
}

assert_praxis_extproc_image() {
  local image
  image="$(oc get deployment payload-processing -n "${GATEWAY_NAMESPACE}" \
    -o jsonpath='{.spec.template.spec.containers[0].image}')"
  if [[ "${image}" != "${PRAXIS_EXTPROC_IMAGE}" ]]; then
    echo "ERROR: expected shared payload-processing image ${PRAXIS_EXTPROC_IMAGE}, got ${image}" >&2
    return 1
  fi
  if [[ "${image}" == *"odh-ai-gateway-payload-processing"* ]]; then
    echo "ERROR: legacy IPP image still in use: ${image}" >&2
    return 1
  fi
  echo "Verified shared ext_proc dataplane image: ${GATEWAY_NAMESPACE}/payload-processing=${image}"
}

echo "Deploying ai-gateway-controller image: ${AI_GATEWAY_CONTROLLER_IMAGE}"
echo "  praxis-extproc image: ${PRAXIS_EXTPROC_IMAGE}"
echo "  namespace: ${AI_GATEWAY_CONTROLLER_NAMESPACE}"
echo "  gateway: ${GATEWAY_NAMESPACE}/${GATEWAY_NAME}"
echo "  remove maas IPP: ${REMOVE_MAAS_IPP}"

# Apply the controller first so it is watching when the tenant opts in.
# Then select praxis (type=praxis) with payload-processing-status cleared — that
# absent status is what holds aigc's deploy gate closed — and remove the legacy
# IPP; only after the delete converges do we set cleanup-complete, releasing
# aigc to create the shared payload-processing. Leaving a carried-over status in
# place races aigc's (same-named) create against the legacy-removal
# convergence check.
_apply_ai_gateway_controller

if [[ "${REMOVE_MAAS_IPP}" == "true" ]]; then
  _select_praxis_on_default_tenant
  _delete_legacy_ipp_in_gateway_namespace
  _clear_praxis_cleanup_on_default_tenant
  _wait_for_aigc_praxis_reconcile
  _wait_for_praxis_extproc
  _protect_praxis_from_maas_reconcile
fi

assert_praxis_extproc_image
echo "ai-gateway-controller rollout complete (praxis-extproc handoff done)"
