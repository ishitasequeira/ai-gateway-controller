# ai-gateway-controller E2E

MaaS pytest suite is **not vendored** in this repo. `prow_run_ai_gateway_controller_test.sh` fetches [models-as-a-service](https://github.com/opendatahub-io/models-as-a-service) into `test/maas-e2e/` (gitignored) at the commit in `test/maas-e2e.lock` by default. Set `MAAS_COMMIT` to override the lock for a single run.

Pinned MaaS e2e requires **Python ≥ 3.10** (pytest 9). `run_e2e_tests.sh` sources MaaS `ensure_e2e_python.sh`, which prefers `python3.11` and can install it on UBI9 group-test runners where system `python3` is still 3.9.

**Orchestration (this repo):** `test/e2e/scripts/`  
**Tests + fixtures (MaaS checkout):** `test/maas-e2e/test/e2e/`

`E2E_SMOKE_ONLY` defaults **false** (full allowlist). Group-test Tekton sets `E2E_SMOKE_ONLY=true` so CI runs pytest `-m 'smoke and …'` within the AIGC module allowlist. See [TODO.md](TODO.md) for excluded modules and upstream gaps.

```bash
# Full allowlist (script default):
AI_GATEWAY_CONTROLLER_IMAGE=quay.io/opendatahub/odh-ai-gateway-controller:odh-pr \
  ./test/e2e/scripts/prow_run_ai_gateway_controller_test.sh

# Smoke-only (matches group-test):
E2E_SMOKE_ONLY=true \
AI_GATEWAY_CONTROLLER_IMAGE=quay.io/opendatahub/odh-ai-gateway-controller:odh-pr \
  ./test/e2e/scripts/prow_run_ai_gateway_controller_test.sh
```

For a one-off run against a different MaaS source revision, set `MAAS_COMMIT` to its full Git SHA. This overrides the locked commit for that run only and does not update `test/maas-e2e.lock`:

```bash
MAAS_UPDATE_LOCK=false \
MAAS_COMMIT="<full-commit-sha>" \
AI_GATEWAY_CONTROLLER_IMAGE=quay.io/opendatahub/odh-ai-gateway-controller:odh-pr \
  ./test/e2e/scripts/prow_run_ai_gateway_controller_test.sh
```
