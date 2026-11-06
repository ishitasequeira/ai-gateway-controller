#!/usr/bin/env bash
set -euo pipefail

# Vendors the praxis-extproc ODH kustomize overlay at a pinned commit into
# config/manifests/praxis-extproc/.
#
# To upgrade: change PRAXIS_EXTPROC_COMMIT below and run "make get-manifests".

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

PRAXIS_EXTPROC_REPO="https://github.com/opendatahub-io/praxis-extproc"
PRAXIS_EXTPROC_COMMIT="1632a345668940cb1cf415f8a1982c9723e02d9e"

# The "odh" overlay references "../../base/..." paths that resolve relative to
# deploy/, so we vendor deploy/base and deploy/overlays/odh together, preserving
# their relative layout (minus the "deploy/" prefix) so those references still
# resolve. The controller-owned kustomize entrypoint is
# config/manifests/external-model/overlays/odh; it composes this exact upstream
# tree with controller-specific ExternalModel patches.
DST_ROOT="${PROJECT_ROOT}/config/manifests/praxis-extproc"

fetch_praxis_extproc() {
    # Always wipe the destination before copy so stale files from a previous
    # pin never linger.
    if [[ "${USE_LOCAL:-}" == "true" ]] && [[ -d "${PROJECT_ROOT}/../praxis-extproc" ]]; then
        echo "Copying manifests from adjacent praxis-extproc checkout"
        rm -rf "${DST_ROOT}"
        mkdir -p "${DST_ROOT}/base" "${DST_ROOT}/overlays/odh"
        cp -a "${PROJECT_ROOT}/../praxis-extproc/deploy/base/." "${DST_ROOT}/base/"
        cp -a "${PROJECT_ROOT}/../praxis-extproc/deploy/overlays/odh/." "${DST_ROOT}/overlays/odh/"
        echo "Manifests copied to ${DST_ROOT}"
        return
    fi

    local tmp_dir
    tmp_dir=$(mktemp -d -t "praxis-extproc-manifests.XXXXXXXXXX")

    git -C "${tmp_dir}" init -q
    git -C "${tmp_dir}" remote add origin "${PRAXIS_EXTPROC_REPO}"
    git -C "${tmp_dir}" fetch --depth 1 -q origin "${PRAXIS_EXTPROC_COMMIT}"
    git -C "${tmp_dir}" reset -q --hard "${PRAXIS_EXTPROC_COMMIT}"

    rm -rf "${DST_ROOT}"
    mkdir -p "${DST_ROOT}/base" "${DST_ROOT}/overlays/odh"
    cp -a "${tmp_dir}/deploy/base/." "${DST_ROOT}/base/"
    cp -a "${tmp_dir}/deploy/overlays/odh/." "${DST_ROOT}/overlays/odh/"

    rm -rf "${tmp_dir}"

    echo "[praxis-extproc] Manifests ready at ${DST_ROOT} (commit ${PRAXIS_EXTPROC_COMMIT})"
}

# The vendored ExtProc workload reads routing ConfigMaps and inference CRs but
# does not consume provider Secrets. Credentials are projected only into the
# tenant-local credential-bearing ExtProc workload. Keep this downstream least-privilege
# adjustment structural and fail closed so regeneration cannot silently restore
# Secret API access to ExtProc.
normalize_extproc_cluster_role() {
    local cluster_role=$1
    local expected_rule='(.apiGroups[0] == "" and (.apiGroups | length) == 1 and .resources[0] == "secrets" and (.resources | length) == 1 and .verbs[0] == "get" and (.verbs | length) == 1)'
    local rule_count secret_permission_count remaining_secret_permissions

    command -v yq >/dev/null 2>&1 || { echo "yq is required to edit ${cluster_role}" >&2; return 1; }
    rule_count=$(yq eval "[.rules[]? | select(${expected_rule})] | length" "${cluster_role}")
    secret_permission_count=$(yq eval '[.rules[]? | select(((.resources // []) | contains(["secrets"])) or ((.resources // []) | contains(["*"])))] | length' "${cluster_role}")

    if [[ "${rule_count}" == 0 ]]; then
        [[ "${secret_permission_count}" == 0 ]] || {
            echo "refusing RBAC rewrite: unexpected Secret permission exists in an already-normalized manifest" >&2
            return 1
        }
        return 0
    fi
    [[ "${rule_count}" == 1 ]] || {
        echo "refusing RBAC rewrite: expected exactly one core Secret get rule, found ${rule_count}" >&2
        return 1
    }

    RBAC_TMP_FILE=$(mktemp "${cluster_role}.XXXXXX")
    trap 'rm -f "${RBAC_TMP_FILE:-}"' EXIT
    yq eval "del(.rules[] | select(${expected_rule}))" "${cluster_role}" >"${RBAC_TMP_FILE}"
    remaining_secret_permissions=$(yq eval '[.rules[]? | select(((.resources // []) | contains(["secrets"])) or ((.resources // []) | contains(["*"])))] | length' "${RBAC_TMP_FILE}")
    [[ "${remaining_secret_permissions}" == 0 ]] || {
        echo "refusing RBAC rewrite: Secret permission remains after structural edit" >&2
        return 1
    }
    mv "${RBAC_TMP_FILE}" "${cluster_role}"
    trap - EXIT
    rm -f "${RBAC_TMP_FILE}"
    RBAC_TMP_FILE=
}

if [[ "${GET_MANIFESTS_TEST_ONLY:-false}" != true ]]; then
    fetch_praxis_extproc
    # Upstream ODH no longer ships payload-processing-reader; keep the
    # structural Secret-strip for older pins / accidental reintroduction.
    cluster_role="${DST_ROOT}/overlays/odh/rbac/clusterrole.yaml"
    if [[ -f "${cluster_role}" ]]; then
        normalize_extproc_cluster_role "${cluster_role}"
    else
        echo "[praxis-extproc] no overlays/odh/rbac/clusterrole.yaml (already Secret-free upstream); skipping normalize"
    fi
fi
