#!/usr/bin/env bash
# Fetch models-as-a-service checkout for ai-gateway-controller e2e (runtime, not vendored).
#
# Usage:
#   source test/e2e/scripts/fetch-maas-e2e.sh
#   MAAS_COMMIT=<sha> source test/e2e/scripts/fetch-maas-e2e.sh
#   MAAS_UPDATE_LOCK=true bash test/e2e/scripts/fetch-maas-e2e.sh
#
# Pin: default fetch ref is maas_commit from test/maas-e2e.lock (not rolling main).
# Set MAAS_COMMIT to override for a single run without editing the lock file.
#
# Container runtime: Podman (Docker compatible) — not required; uses git fetch only.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="${PROJECT_ROOT:-$(cd "${SCRIPT_DIR}/../../.." && pwd)}"

MAAS_REF="${MAAS_REF:-main}"
MAAS_CHECKOUT_ROOT="${MAAS_CHECKOUT_ROOT:-${PROJECT_ROOT}/test/maas-e2e}"
LOCK_FILE="${MAAS_LOCK_FILE:-${PROJECT_ROOT}/test/maas-e2e.lock}"
MAAS_UPDATE_LOCK="${MAAS_UPDATE_LOCK:-false}"

_read_lock_value() {
  local key="$1"
  [[ -f "${LOCK_FILE}" ]] || return 1
  local line
  line="$(grep -E "^${key}=" "${LOCK_FILE}" | tail -1 || true)"
  [[ -n "${line}" ]] || return 1
  printf '%s\n' "${line#${key}=}"
}

_read_lock_commit() {
  _read_lock_value maas_commit
}

MAAS_REPO="${MAAS_REPO:-$(_read_lock_value maas_repo 2>/dev/null || echo https://github.com/opendatahub-io/models-as-a-service)}"

_resolve_fetch_ref() {
  if [[ -n "${MAAS_COMMIT:-}" ]]; then
    printf '%s\n' "${MAAS_COMMIT}"
    return 0
  fi
  local locked
  if locked="$(_read_lock_commit)"; then
    printf '%s\n' "${locked}"
    return 0
  fi
  printf '%s\n' "${MAAS_REF}"
}

_fetch_maas_checkout() {
  local fetch_ref="$1"
  echo "Fetching ${MAAS_REPO}@${fetch_ref} into ${MAAS_CHECKOUT_ROOT} ..."

  rm -rf "${MAAS_CHECKOUT_ROOT}"
  mkdir -p "$(dirname "${MAAS_CHECKOUT_ROOT}")"

  local tmp_dir
  tmp_dir="$(mktemp -d -t maas-e2e-fetch.XXXXXXXXXX)"

  git -C "${tmp_dir}" init -q
  git -C "${tmp_dir}" remote add origin "${MAAS_REPO}"
  if ! git -C "${tmp_dir}" fetch --depth 1 -q origin "${fetch_ref}"; then
    echo "ERROR: git fetch failed for ${MAAS_REPO}@${fetch_ref}" >&2
    rm -rf "${tmp_dir}"
    exit 1
  fi
  git -C "${tmp_dir}" checkout -q FETCH_HEAD
  mv "${tmp_dir}" "${MAAS_CHECKOUT_ROOT}"

  local resolved_commit
  resolved_commit="$(git -C "${MAAS_CHECKOUT_ROOT}" rev-parse HEAD)"
  echo "MaaS checkout at: ${resolved_commit}"

  if [[ "${MAAS_UPDATE_LOCK}" == "true" ]]; then
    cat >"${LOCK_FILE}" <<EOF
# Pin for ai-gateway-controller e2e (models-as-a-service). Update with MAAS_UPDATE_LOCK=true.
maas_repo=${MAAS_REPO}
maas_commit=${resolved_commit}
maas_ref=${MAAS_REF}
updated_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)
EOF
    echo "Updated ${LOCK_FILE}"
  fi

  export MAAS_COMMIT_RESOLVED="${resolved_commit}"
}

_ensure_maas_checkout() {
  local fetch_ref
  fetch_ref="$(_resolve_fetch_ref)"

  if [[ -z "${MAAS_FORCE_FETCH:-}" && -f "${MAAS_CHECKOUT_ROOT}/scripts/deploy.sh" ]]; then
    local current
    current="$(git -C "${MAAS_CHECKOUT_ROOT}" rev-parse HEAD 2>/dev/null || true)"
    if [[ -n "${current}" && "${current}" == "${fetch_ref}" ]]; then
      echo "Reusing MaaS checkout at ${current}"
      export MAAS_COMMIT_RESOLVED="${current}"
      return 0
    fi
  fi

  _fetch_maas_checkout "${fetch_ref}"
}

_ensure_maas_script_perms() {
  # Shallow fetch and Python patches rewrite scripts without the executable bit.
  # .github/hack/ existed until the 7879675 pin and is gone upstream; scripts/
  # covers its replacements (scripts/installers/).
  find "${MAAS_CHECKOUT_ROOT}/scripts" \
    "${MAAS_CHECKOUT_ROOT}/test/e2e/scripts" \
    -name '*.sh' -exec chmod +x {} + 2>/dev/null || true
}

_ensure_maas_checkout

export MAAS_CHECKOUT_ROOT
export MAAS_E2E_DIR="${MAAS_CHECKOUT_ROOT}/test/e2e"

for req in \
  "${MAAS_CHECKOUT_ROOT}/scripts/deployment-helpers.sh" \
  "${MAAS_CHECKOUT_ROOT}/scripts/deploy.sh" \
  "${MAAS_E2E_DIR}/requirements.txt" \
  "${MAAS_E2E_DIR}/pyproject.toml" \
  "${MAAS_E2E_DIR}/scripts/ensure_e2e_python.sh"
do
  if [[ ! -f "${req}" ]]; then
    echo "ERROR: MaaS checkout missing required file: ${req}" >&2
    exit 1
  fi
done

# shellcheck disable=SC1091
source "${SCRIPT_DIR}/patch-maas-deploy-for-aigc.sh"
_ensure_maas_script_perms
