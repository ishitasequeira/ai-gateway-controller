# ai-gateway-controller E2E — dynamic MaaS checkout

Tests and deploy tooling come from [models-as-a-service](https://github.com/opendatahub-io/models-as-a-service) at runtime (`test/e2e/scripts/fetch-maas-e2e.sh`).

**Pin policy:** `test/maas-e2e.lock` holds a fixed commit SHA. Prow/CI fetch that source revision rather than rolling `main`. The MaaS runtime image is configured separately.

**Repo:** `fetch-maas-e2e.sh` reads `maas_repo` from the lock file (default `opendatahub-io/models-as-a-service`).

```bash
# Bump pin to latest main (updates test/maas-e2e.lock after fetch):
MAAS_UPDATE_LOCK=true bash test/e2e/scripts/fetch-maas-e2e.sh

AI_GATEWAY_CONTROLLER_IMAGE=quay.io/opendatahub/odh-ai-gateway-controller:odh-pr \
  ./test/e2e/scripts/prow_run_ai_gateway_controller_test.sh
```

---

## Excluded tests (not in `run_e2e_tests.sh` allowlist)

Re-enable when the **Requirement to re-introduce** is met. Update the allowlist in `test/e2e/scripts/run_e2e_tests.sh`.

| Test module | Why excluded | Requirement to re-introduce |
|-------------|--------------|------------------------------|
| `test_external_models.py` | Marked `legacy_ipp` upstream; assumes pre-#136 / legacy IPP ExternalModel serving | Praxis-aligned ExternalModel e2e (home path / `gatewayRefs`) + drop `legacy_ipp` or add a Praxis suite |
| `test_external_oidc.py` | External OIDC gateway path not in aigc CI scope | Partner/OIDC gateway deployments + Keycloak fixtures in prow; `EXTERNAL_OIDC=true` path |
| `test_x_api_key_auth.py` | Depends on IPP ExternalModel (`apiFormat=messages`) identity source | Praxis/IPP ExternalModel wiring in aigc; gateway AuthPolicy `api-keys-x-api-key` source |
| `test_networkpolicy.py` | NetworkPolicy assertions assume full MaaS networkpolicy bundle | Confirm aigc deploy applies same NP manifests; no CNI conflicts on Konflux clusters |
| `test_model_identity_conflict.py` | Cross-CR identity conflict scenarios need full controller surface | Validate against aigc + praxis dataplane; may need MaaS upstream fixes |
| `test_tenant_auto_resolve.py` | Tenant auto-resolve depends on MaaS controller features not validated on aigc yet | Confirm `maas-controller` + aigc AITenant wiring; run on dedicated cluster |
| `test_crd_watch_resilience.py` | Deletes KServe CRD / restarts controller — destructive serial test | Safe on ephemeral CI only; add serial pass gating + KServe module present |
| `test_authpolicy_generation_stability.py` | Long stability window; sensitive to parallel AuthPolicy churn | Run serial-only or increase isolation; confirm no aigc-specific AuthPolicy regressions |

---

## Post-fetch test patches vs upstream MaaS PRs

Some e2e fixes from the vendored branch (`ci/maas-e2e-konflux-group-test`) are **not** in MaaS `main` at the pinned commit. Two ways to close that gap:

| Approach | When to use | Trade-off |
|----------|-------------|-----------|
| **Upstream MaaS PR** (preferred) | Fix belongs in shared MaaS tests (praxis log skip, `_poll_status` flakes, duplicate-header warmup) | All MaaS consumers benefit; slower until merged |
| **aigc post-fetch patch** (`patch-maas-tests-for-aigc.sh`, not added yet) | Short-term CI unblock while upstream PR is open | Duplicated logic; must re-apply after every lock bump |

**Current choice:** MaaS source @ `7879675` (see `test/maas-e2e.lock`), including the [#1546](https://github.com/opendatahub-io/models-as-a-service/pull/1546) short TRLP subscription IDs and the [#1537](https://github.com/opendatahub-io/models-as-a-service/pull/1537) rate-grouping test. Deploy patches stay in `patch-maas-deploy-for-aigc.sh`. IPP migration **workarounds remain removed** from aigc (no pod labeling / managed-by stamp / MaasTenantConfig retry loop).

---

## Upstream MaaS changes needed (or carry aigc patches)

Some fixes landed in upstream MaaS `main` @ `5ece7d3` (#1493); IPP backend-swap coordination is on #1508 (`5c36d49`). Others still need `patch-maas-deploy-for-aigc.sh` / aigc-only scripts.

| Area | Issue | Fix (upstream or aigc) |
|------|--------|-------------------------|
| **Praxis default dataplane** | `test_per_tenant_ipp_isolation` expects Go IPP log markers (`handlers/server.go`, `x-request-id`) | **Upstream (#1493):** skip log check when deployment is `odh-praxis-extproc`; rely on HTTP 200 |
| **Praxis image / BBR** | Default tenant uses praxis; BBR needs `model_to_header` + `llmisvc_model_provider_resolver` on **pre-extproc** chain | Manifest: [praxis-extproc#82](https://github.com/opendatahub-io/praxis-extproc/pull/82) @ `d030ea0`; image: `odh-praxis-extproc:odh-stable` ([#79](https://github.com/opendatahub-io/praxis-extproc/pull/79)) |
| **`_poll_status`** | Parallel workers churn `maas-gateway-auth` → empty 401/403 flakes | **Upstream (#1493):** re-check gateway AuthPolicy on transient empty 401/403 during poll |
| **Duplicate subscription headers** | `test_duplicate_subscription_headers_ignored` warmup 403 under load | **Upstream (#1493):** post-mint delay + warmup poll hardening |
| **`deploy.sh`** | Kustomize e2e has no `maas-controller/` source tree | **aigc:** `patch-maas-deploy-for-aigc.sh` after fetch (keep until upstream accepts `deployment/` only) |
| **`deploy-models.sh`** | Waits for all Kuadrant AuthPolicies (flakes when aigc adds policies) | **Upstream (#1759):** waits only for AuthPolicies targeting `$gateway_name`; the aigc patch that scoped it to the `managed-by=maas-controller` label was retired when the pin moved to `7879675` |
| **llmisvc controller CPU** | Since [#1594](https://github.com/opendatahub-io/models-as-a-service/pull/1594) the parallel pass queues new LLMInferenceServices behind the shipped 100m `llmisvc-controller-manager`; models stay `WaitingForGateway` past test timeouts | **aigc:** `deploy-platform.sh` raises its CPU like upstream [#1598](https://github.com/opendatahub-io/models-as-a-service/pull/1598) (aigc keeps its own copy of the script) |
| **`validate-deployment.sh`** | BBR model URL is gateway-root; path-based HTTPRoute needs path prefix | **Upstream (maas-billing):** `E2E_MODEL_PATH` / `E2E_MODEL_REF` in `validate-deployment.sh`; **aigc:** `ensure_gateway_allows_model_namespace` in prow runner |
| **`prow_run_*` prerequisites** | Empty `PRAXIS_EXTPROC_IMAGE` + `set -e` silent exit | **aigc-only** in `prow_run_ai_gateway_controller_test.sh` |
| **Must-gather** | CI artifacts for HTTPRoute/LLMIS debugging | **aigc:** `collect-maas-must-gather.sh` dumps all `maas.opendatahub.io` + `inference.opendatahub.io` kinds, Gateway API HTTPRoutes/Gateways (cluster + per-namespace), Kuadrant policies, Istio gateway networking; Tekton step writes `gather-maas/` + `gather-openshift/` |
| **Webhook handoff** | Pausing `maas-controller` breaks AITenant webhook during praxis install | **aigc-only** in `deploy-ai-gateway-controller.sh` (annotate → pause → delete IPP → **resume** → apply aigc) |
| **IPP migration / backend swap** | Legacy↔praxis handoff races / `MaasTenantConfig` blocked during cleanup | **Upstream (#1508):** `ipp-migration-cleanup-complete` annotation as handoff boundary. aigc workarounds removed on `ci/e2e-maas-pr-1508`. |

---

## ai-gateway-controller repo gaps (not MaaS)

- [ ] `RELATED_IMAGE_ODH_AI_GATEWAY_CONTROLLER_IMAGE` — operator still installs via kustomize in CI
- [x] Merge [odh-konflux-central](https://github.com/opendatahub-io/odh-konflux-central) group-test pipeline upstream
- [x] Praxis stable default (`quay.io/opendatahub/odh-praxis-extproc:odh-stable`, praxis-extproc#79 @ `9872fc9`)
- [x] Point `.tekton/ai-gateway-controller-group-test.yaml` at upstream konflux-central after merge
