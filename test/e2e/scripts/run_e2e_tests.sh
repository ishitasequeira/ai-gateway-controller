#!/bin/bash
# ai-gateway-controller e2e runner — MaaS pytest suite (dynamic checkout).
# Excluded modules: see test/e2e/TODO.md

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
if [[ -z "${MAAS_E2E_DIR:-}" ]]; then
  # shellcheck disable=SC1091
  source "${SCRIPT_DIR}/fetch-maas-e2e.sh"
fi
TEST_DIR="${MAAS_E2E_DIR}"

serial_only=false
extra_pytest_args=()
while [[ $# -gt 0 ]]; do
    case "$1" in
        --serial-only) serial_only=true; shift ;;
        --) shift; extra_pytest_args=("$@"); break ;;
        *) extra_pytest_args+=("$1"); shift ;;
    esac
done

export E2E_RECONCILE_WAIT="${E2E_RECONCILE_WAIT:-4}"
E2E_PARALLEL_WORKERS="${E2E_PARALLEL_WORKERS:-7}"
if ! [[ "$E2E_PARALLEL_WORKERS" =~ ^[1-9][0-9]*$ ]]; then
    echo "ERROR: E2E_PARALLEL_WORKERS must be a positive integer (>= 1), got '$E2E_PARALLEL_WORKERS'" >&2
    exit 1
fi

ARTIFACTS_DIR="${ARTIFACTS_DIR:-${ARTIFACT_DIR:-${ARTIFACTS:-${LOG_DIR:-$PROJECT_ROOT/test/e2e/reports}}}}"
mkdir -p "$ARTIFACTS_DIR"

if [[ "$E2E_PARALLEL_WORKERS" -gt 1 ]]; then
    export E2E_AUTHPOLICY_PHASE_TIMEOUT="${E2E_AUTHPOLICY_PHASE_TIMEOUT:-120}"
    export E2E_MAAS_SUBSCRIPTION_PHASE_TIMEOUT="${E2E_MAAS_SUBSCRIPTION_PHASE_TIMEOUT:-120}"
    export E2E_GATEWAY_ENFORCED_TIMEOUT="${E2E_GATEWAY_ENFORCED_TIMEOUT:-240}"
    export E2E_MULTITENANCY_PHASE_TIMEOUT="${E2E_MULTITENANCY_PHASE_TIMEOUT:-180}"
fi

# MaaS pytest 9 needs Python >= 3.10. Group-test UBI9 still ships python3=3.9;
# ensure_e2e_python.sh (from the pinned checkout) installs/selects 3.11 when needed.
# shellcheck source=/dev/null
source "${TEST_DIR}/scripts/ensure_e2e_python.sh"

VENV_DIR="${PROJECT_ROOT}/test/e2e/.venv"
if [[ -d "$VENV_DIR" ]] \
    && ! "$VENV_DIR/bin/python" -c 'import sys; raise SystemExit(0 if sys.version_info >= (3, 10) else 1)' 2>/dev/null; then
    echo "Recreating e2e venv (existing interpreter is older than Python 3.10)..."
    rm -rf "$VENV_DIR"
fi
if [[ ! -d "$VENV_DIR" ]]; then
    echo "Creating Python venv for e2e tests ($E2E_PYTHON)..."
    "$E2E_PYTHON" -m venv "$VENV_DIR" --upgrade-deps
fi
# shellcheck disable=SC1091
source "$VENV_DIR/bin/activate"
python -m pip install --upgrade pip --quiet
python -m pip install -r "$TEST_DIR/requirements.txt" --quiet

user="$(oc whoami 2>/dev/null || echo 'unknown')"
html="$ARTIFACTS_DIR/e2e-${user}.html"
xml="$ARTIFACTS_DIR/e2e-${user}.xml"
xml_serial="${xml%.xml}-serial.xml"

e2e_test_files=(
    "$TEST_DIR/tests/test_api_keys.py"
    "$TEST_DIR/tests/test_namespace_scoping.py"
    "$TEST_DIR/tests/test_negative_security.py"
    "$TEST_DIR/tests/test_subscription.py"
    "$TEST_DIR/tests/test_subscription_list_endpoints.py"
    "$TEST_DIR/tests/test_models_endpoint.py"
    "$TEST_DIR/tests/test_smoke.py"
    "$TEST_DIR/tests/test_tenant.py"
    "$TEST_DIR/tests/test_config_tenant.py"
    "$TEST_DIR/tests/test_tenant_discovery.py"
    "$TEST_DIR/tests/test_aitenant_lifecycle.py"
    "$TEST_DIR/tests/test_tenant_namespace_discovery.py"
    "$TEST_DIR/tests/test_tenant_discovery_isolation.py"
    "$TEST_DIR/tests/test_gateway_scoped_authpolicy.py"
    "$TEST_DIR/tests/test_multi_tenant_integration.py"
    "$TEST_DIR/tests/test_multi_tenant_maas_api.py"
    "$TEST_DIR/tests/test_tenant_model_inference.py"
    "$TEST_DIR/tests/test_tenant_auth_isolation.py"
    "$TEST_DIR/tests/test_tenant_subscription_isolation.py"
    "$TEST_DIR/tests/test_tenant_rate_limit_isolation.py"
    "$TEST_DIR/tests/test_per_tenant_ipp_isolation.py"
    "$TEST_DIR/tests/test_embedding_inference.py"
)

resolved_extra_args=()
has_path_arg=false
for arg in "${extra_pytest_args[@]}"; do
    if [[ -e "$TEST_DIR/$arg" ]]; then
        resolved_extra_args+=("$TEST_DIR/$arg")
        has_path_arg=true
    elif [[ -e "$arg" ]]; then
        resolved_extra_args+=("$arg")
        has_path_arg=true
    else
        resolved_extra_args+=("$arg")
    fi
done

if $has_path_arg; then
    pytest_common_args=(
        -v --disable-warnings
        --capture=tee-sys --show-capture=all --log-level=INFO
        "${resolved_extra_args[@]}"
    )
else
    pytest_common_args=(
        -v --disable-warnings
        --capture=tee-sys --show-capture=all --log-level=INFO
        "${e2e_test_files[@]}"
        "${extra_pytest_args[@]}"
    )
fi

parallel_rc=0
serial_rc=0

if [[ "$serial_only" == "true" || "$E2E_PARALLEL_WORKERS" -le 1 ]]; then
    echo "Running E2E tests serially (E2E_PARALLEL_WORKERS=${E2E_PARALLEL_WORKERS})"
    if ! PYTHONPATH="$TEST_DIR:${PYTHONPATH:-}" pytest \
        --maxfail=5 \
        --junitxml="$xml" \
        --html="$html" --self-contained-html \
        "${pytest_common_args[@]}"; then
        parallel_rc=1
    fi
else
    echo "Running E2E pass 1/2: parallel (E2E_PARALLEL_WORKERS=${E2E_PARALLEL_WORKERS}, --dist=loadgroup, -m 'not serial')"
    if ! PYTHONPATH="$TEST_DIR:${PYTHONPATH:-}" pytest \
        --maxfail=5 \
        -n "$E2E_PARALLEL_WORKERS" --dist=loadgroup \
        -m "not serial" \
        --junitxml="$xml" \
        --html="$html" --self-contained-html \
        "${pytest_common_args[@]}"; then
        parallel_rc=1
    fi

    echo "Running E2E pass 2/2: serial cluster mutators (-m serial, single worker)"
    if ! PYTHONPATH="$TEST_DIR:${PYTHONPATH:-}" pytest \
        --maxfail=5 \
        -m serial \
        --junitxml="$xml_serial" \
        --html="${html%.html}-serial.html" --self-contained-html \
        "${pytest_common_args[@]}"; then
        serial_rc=1
    fi
fi

if [[ "$parallel_rc" -ne 0 || "$serial_rc" -ne 0 ]]; then
    echo "ERROR: E2E tests failed (parallel_rc=${parallel_rc}, serial_rc=${serial_rc})"
    exit 1
fi

echo "E2E tests completed"
echo " - JUnit XML : ${xml}"
if [[ -f "$xml_serial" ]]; then
    echo " - JUnit XML (serial pass): ${xml_serial}"
fi
echo " - HTML      : ${html}"
