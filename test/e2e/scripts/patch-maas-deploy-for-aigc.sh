#!/usr/bin/env bash
# Patch fetched MaaS deploy.sh for ai-gateway-controller e2e (no local maas-controller/ tree).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="${PROJECT_ROOT:-$(cd "${SCRIPT_DIR}/../../.." && pwd)}"
MAAS_CHECKOUT_ROOT="${MAAS_CHECKOUT_ROOT:-${PROJECT_ROOT}/test/maas-e2e}"
DEPLOY_SH="${MAAS_CHECKOUT_ROOT}/scripts/deploy.sh"
MARKER="ai-gateway-controller: no local maas-controller/ tree"
OPTIONAL_OPS_MARKER="ai-gateway-controller: skip optional operators when already installed"

if [[ ! -f "${DEPLOY_SH}" ]]; then
  echo "ERROR: ${DEPLOY_SH} not found — run fetch-maas-e2e.sh first" >&2
  exit 1
fi

_deploy_sh_already_patched=false
if grep -qF "${MARKER}" "${DEPLOY_SH}" && grep -qF "${OPTIONAL_OPS_MARKER}" "${DEPLOY_SH}"; then
  echo "deploy.sh already patched for ai-gateway-controller"
  _deploy_sh_already_patched=true
fi

if [[ "${_deploy_sh_already_patched}" != "true" ]] && ! grep -qF "${MARKER}" "${DEPLOY_SH}"; then
  python3 - <<'PY' "${DEPLOY_SH}" "${MARKER}"
import sys
path, marker = sys.argv[1], sys.argv[2]
text = open(path).read()
old = '''  if [[ ! -d "$controller_dir" ]]; then
    log_error "maas-controller directory not found at $controller_dir — controller is required"
    return 1
  fi'''
new = f'''  if [[ ! -d "$controller_dir" ]]; then
    # {marker}
    if [[ "$DEPLOYMENT_MODE" == "operator" ]]; then
      log_info "  maas-controller source tree not present; relying on operator-managed deployment"
    elif [[ -d "${{project_root}}/deployment/base/maas-controller" ]]; then
      log_info "  maas-controller source tree not present; using synced deployment/ manifests (kustomize mode)"
    else
      log_error "maas-controller directory not found at $controller_dir — fetch MaaS checkout (needs deployment/)"
      return 1
    fi
  fi'''
if old not in text:
    if marker in text:
        sys.exit(0)
    print("ERROR: deploy.sh controller_dir block not found", file=sys.stderr)
    sys.exit(1)
open(path, "w").write(text.replace(old, new, 1))
PY
fi

if [[ "${_deploy_sh_already_patched}" != "true" ]] && ! grep -qF "${OPTIONAL_OPS_MARKER}" "${DEPLOY_SH}"; then
  if grep -qF 'install_optional_operators() {' "${DEPLOY_SH}"; then
    awk -v marker="${OPTIONAL_OPS_MARKER}" '
      /install_optional_operators\(\) \{/ { print; getline; print; getline; print; print ""; print "  local data_dir=\"${SCRIPT_DIR}/data\""; print ""; print "  # " marker; print "  if kubectl get deployment -n cert-manager-operator cert-manager-operator-controller-manager -o jsonpath='"'"'{.status.availableReplicas}'"'"' 2>/dev/null | grep -q '"'"'[1-9]'"'"' \\"; print "    && kubectl get csv -n openshift-lws-operator leader-worker-set.v1.0.0 -o jsonpath='"'"'{.status.phase}'"'"' 2>/dev/null | grep -q Succeeded; then"; print "    log_info \"cert-manager and LWS already installed; skipping subscription apply\""; print "    log_info \"Activating LeaderWorkerSet API...\""; print "    kubectl apply -f \"${data_dir}/lws-operator-cr.yaml\""; print "    log_info \"Optional operators installed\""; print "    return 0"; print "  fi"; print ""; skip=2; next }
      skip>0 { skip--; if (/local data_dir=/) next; if (/^$/) next; }
      { print }
    ' "${DEPLOY_SH}" > "${DEPLOY_SH}.tmp" && mv "${DEPLOY_SH}.tmp" "${DEPLOY_SH}"
  fi
fi

if [[ "${_deploy_sh_already_patched}" != "true" ]]; then
  if ! grep -qF "${MARKER}" "${DEPLOY_SH}"; then
    echo "ERROR: failed to patch ${DEPLOY_SH}" >&2
    exit 1
  fi
  echo "Patched ${DEPLOY_SH}"
fi

DEPLOY_MODELS_SH="${MAAS_CHECKOUT_ROOT}/test/e2e/scripts/deploy-models.sh"
FIXTURES_MARKER="ai-gateway-controller: resolve fixture root from MaaS checkout"
if [[ -f "${DEPLOY_MODELS_SH}" ]] && ! grep -qF "${FIXTURES_MARKER}" "${DEPLOY_MODELS_SH}"; then
  python3 - <<'PY' "${DEPLOY_MODELS_SH}" "${FIXTURES_MARKER}"
import sys
path, marker = sys.argv[1], sys.argv[2]
text = open(path).read()
replacements = [
    (
        '[[ "$(type -t find_project_root 2>/dev/null)" == "function" ]] || source "$PROJECT_ROOT/scripts/deployment-helpers.sh"',
        f'# {marker}\n[[ "$(type -t find_project_root 2>/dev/null)" == "function" ]] || source "${{MAAS_CHECKOUT_ROOT:-$PROJECT_ROOT}}/scripts/deployment-helpers.sh"',
    ),
    (
        '(cd "$PROJECT_ROOT" && kustomize build test/e2e/fixtures/',
        '(cd "${MAAS_CHECKOUT_ROOT:-$PROJECT_ROOT}" && kustomize build test/e2e/fixtures/',
    ),
]
for old, new in replacements:
    if old not in text:
        if marker in text:
            sys.exit(0)
        print(f"WARN: deploy-models.sh block not found for patch: {old[:60]}...", file=sys.stderr)
        sys.exit(0)
    text = text.replace(old, new, 1)
open(path, "w").write(text)
PY
  echo "Patched ${DEPLOY_MODELS_SH} (MaaS checkout fixture root)"
fi

