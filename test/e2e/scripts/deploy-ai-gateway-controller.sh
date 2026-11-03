#!/usr/bin/env bash
# Deploy (or upgrade) ai-gateway-controller on an existing MaaS cluster for e2e.
#
# MaaS defaults the payload-processing dataplane to praxis, so this script only
# installs/upgrades ai-gateway-controller and waits for the shared praxis
# ext_proc Deployments — no legacy IPP swap / annotation handoff.
#
# Required:
#   AI_GATEWAY_CONTROLLER_IMAGE — manager image to run (Konflux PR image in CI)
#
# Optional:
#   AI_GATEWAY_CONTROLLER_NAMESPACE — default opendatahub
#   GATEWAY_NAMESPACE               — default openshift-ingress
#   GATEWAY_NAME                    — default maas-default-gateway
#   PRAXIS_EXTPROC_IMAGE            — praxis dataplane image (default from params.env)
#   PRAXIS_INSTALL_TIMEOUT          — default 300s

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
PRAXIS_INSTALL_TIMEOUT="${PRAXIS_INSTALL_TIMEOUT:-300}"
MANAGED_FALSE_ANNOTATION="${MANAGED_FALSE_ANNOTATION:-opendatahub.io/managed=false}"
AITENANT_NAME="${AITENANT_NAME:-models-as-a-service}"
AITENANT_NAMESPACE="${AITENANT_NAMESPACE:-ai-tenants}"
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

if [[ -z "${AI_GATEWAY_CONTROLLER_IMAGE}" ]]; then
  echo "ERROR: AI_GATEWAY_CONTROLLER_IMAGE is required" >&2
  exit 1
fi

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

_apply_ai_gateway_controller() {
  local work_dir params rendered
  work_dir="$(mktemp -d -t aigc-kustomize.XXXXXXXXXX)"

  cp -a "${PROJECT_ROOT}/config/self" "${work_dir}/self"
  # BSD sed (macOS) requires a backup suffix for -i; write a new file instead.
  params="${work_dir}/self/default/params.env"
  rendered="$(mktemp)"
  sed \
    -e "s|^ai-gateway-controller-image=.*|ai-gateway-controller-image=${AI_GATEWAY_CONTROLLER_IMAGE}|" \
    -e "s|^praxis-extproc-image=.*|praxis-extproc-image=${PRAXIS_EXTPROC_IMAGE}|" \
    "${params}" > "${rendered}"
  mv "${rendered}" "${params}"

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

  echo "Triggering immediate praxis-extproc reconcile ..."
  oc rollout restart deployment/ai-gateway-controller -n "${AI_GATEWAY_CONTROLLER_NAMESPACE}"
  oc rollout status deployment/ai-gateway-controller \
    -n "${AI_GATEWAY_CONTROLLER_NAMESPACE}" --timeout=180s
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

_protect_praxis_from_maas_reconcile() {
  echo "Annotating praxis IPP resources ${MANAGED_FALSE_ANNOTATION} so maas-controller skips them ..."
  local name kind
  for name in payload-processing payload-pre-processing; do
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

_apply_ai_gateway_controller
_wait_for_praxis_extproc
_protect_praxis_from_maas_reconcile
assert_praxis_extproc_image
echo "ai-gateway-controller rollout complete"
