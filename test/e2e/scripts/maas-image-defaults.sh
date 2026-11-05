#!/usr/bin/env bash
# Default container images for ai-gateway-controller e2e (no local maas-api/maas-controller trees).
#
# MaaS main-branch Konflux pushes publish :latest and a tag for each commit SHA (see
# models-as-a-service .tekton/odh-maas-api-push.yaml and odh-maas-controller-push.yaml).
# Override with MAAS_API_IMAGE / MAAS_CONTROLLER_IMAGE when testing specific builds.

set -euo pipefail

# Deploy images built from the MaaS commit pinned in test/maas-e2e.lock, so the deployed
# MaaS matches the fetched tests. Set MAAS_IMAGE_TAG as well when overriding MAAS_COMMIT.
maas_lock_file="${MAAS_LOCK_FILE:-$(dirname "${BASH_SOURCE[0]}")/../../maas-e2e.lock}"
if [[ -z "${MAAS_IMAGE_TAG:-}" ]]; then
  MAAS_IMAGE_TAG="$(sed -n 's/^maas_commit=//p' "${maas_lock_file}" 2>/dev/null | tail -1)" || true
fi
if [[ -z "${MAAS_IMAGE_TAG}" ]]; then
  echo "ERROR: no maas_commit in ${maas_lock_file}; set MAAS_IMAGE_TAG" >&2
  exit 1
fi

# The shared Konflux group-test pipeline exports maas-*:latest, which drifts from the pinned
# tests whenever MaaS main changes. Use the pinned images instead unless
# MAAS_ALLOW_LATEST_IMAGES=true.
if [[ "${MAAS_ALLOW_LATEST_IMAGES:-false}" != "true" ]]; then
  if [[ "${MAAS_API_IMAGE:-}" == *:latest ]]; then
    unset MAAS_API_IMAGE
  fi
  if [[ "${MAAS_CONTROLLER_IMAGE:-}" == *:latest ]]; then
    unset MAAS_CONTROLLER_IMAGE
  fi
fi

export MAAS_API_IMAGE="${MAAS_API_IMAGE:-quay.io/opendatahub/maas-api:${MAAS_IMAGE_TAG}}"
export MAAS_CONTROLLER_IMAGE="${MAAS_CONTROLLER_IMAGE:-quay.io/opendatahub/maas-controller:${MAAS_IMAGE_TAG}}"

# ai-gateway-controller is always supplied by the caller (Konflux snapshot or local override).
if [[ -z "${AI_GATEWAY_CONTROLLER_IMAGE:-}" ]]; then
  echo "ERROR: AI_GATEWAY_CONTROLLER_IMAGE must be set (PR under test)" >&2
  exit 1
fi

# Konflux group-test may export the snapshot digest as AI_GATEWAY_CONTROLLER_IMAGE.
# That digest can lag the floating odh-pr-<N> tag and roll out a stale manager while
# e2e manifests come from the PR head (e.g. --metrics-secure). Prefer a tag from the
# SNAPSHOT JSON or odh-pr-<PR_NUMBER> (see hack/tekton/pr-group-testing-pipeline.yaml).
if [[ "${AI_GATEWAY_CONTROLLER_IMAGE}" == *@sha256:* ]]; then
  aigc_repo="${AI_GATEWAY_CONTROLLER_IMAGE%%@*}"
  resolved_tag=""

  if [[ -n "${SNAPSHOT:-}" ]]; then
    resolved_tag="$(jq -r '."odh-ai-gateway-controller-ci".image_tag // empty' <<< "${SNAPSHOT}" 2>/dev/null || true)"
    if [[ "${resolved_tag}" == "null" || "${resolved_tag}" == *@sha256:* ]]; then
      resolved_tag=""
    fi
  fi

  if [[ -z "${resolved_tag}" ]]; then
    pr_num="${PR_NUMBER:-${PR_NUMBER_RESULT:-}}"
    if [[ -n "${pr_num}" && "${pr_num}" != "null" ]]; then
      resolved_tag="odh-pr-${pr_num}"
    fi
  fi

  if [[ -n "${resolved_tag}" ]]; then
    echo "NOTE: rewriting AI_GATEWAY_CONTROLLER_IMAGE from snapshot digest to tag ${resolved_tag}" >&2
    export AI_GATEWAY_CONTROLLER_IMAGE="${aigc_repo}:${resolved_tag}"
  else
    echo "ERROR: refusing snapshot digest ${AI_GATEWAY_CONTROLLER_IMAGE}; set SNAPSHOT image_tag or PR_NUMBER" >&2
    exit 1
  fi
fi

# Konflux group-test may export PRAXIS_EXTPROC_IMAGE from the odh-praxis-extproc-ci snapshot.
export PRAXIS_EXTPROC_IMAGE="${PRAXIS_EXTPROC_IMAGE:-quay.io/opendatahub/odh-praxis-extproc@sha256:a67be44e530961ca7e6dc453220ebe11a6641a2003577f3c4ddb1f426555b874}"

echo "MaaS platform images:"
echo "  MAAS_API_IMAGE=${MAAS_API_IMAGE}"
echo "  MAAS_CONTROLLER_IMAGE=${MAAS_CONTROLLER_IMAGE}"
echo "  AI_GATEWAY_CONTROLLER_IMAGE=${AI_GATEWAY_CONTROLLER_IMAGE}"
echo "  PRAXIS_EXTPROC_IMAGE=${PRAXIS_EXTPROC_IMAGE}"
