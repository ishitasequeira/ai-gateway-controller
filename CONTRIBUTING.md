# Contributing to ai-gateway-controller

Thanks for your interest in contributing. This guide explains the conventions
this repo enforces and how to work with it.

Read [DESIGN.md](./DESIGN.md) first for scope and architecture rationale —
this file only covers process (PR/CI conventions), not design decisions.

## Table of contents

- [Contributing to ai-gateway-controller](#contributing-to-ai-gateway-controller)
  - [Table of contents](#table-of-contents)
  - [Getting started](#getting-started)
  - [Development setup](#development-setup)
  - [Pull request process](#pull-request-process)
  - [CI and checks](#ci-and-checks)
  - [Testing](#testing)
    - [Test ownership boundaries: AIGO / AIGC / MaaS](#test-ownership-boundaries-aigo--aigc--maas)
  - [Repository layout](#repository-layout)
  - [Getting help](#getting-help)

## Getting started

1. **Fork** the repository on GitHub.
2. **Clone** your fork and add the upstream remote:
   ```bash
   git clone git@github.com:YOUR_USERNAME/ai-gateway-controller.git
   cd ai-gateway-controller
   git remote add upstream https://github.com/opendatahub-io/ai-gateway-controller.git
   ```
3. **Create a branch** from `main` for your work:
   ```bash
   git fetch upstream
   git checkout -b your-feature upstream/main
   ```
4. Go toolchain version is pinned in [go.mod](./go.mod).

## Development setup

- `make build` — full local build: `tidy`, `lint`, `test`, `binary`.
- `make run` — build and run the manager locally (see `cmd/manager/main.go` for flags).
- `make get-manifests` — re-vendor the `praxis-extproc` overlay pinned in
  `hack/scripts/get-manifests.sh` into the exact upstream tree at
  `config/manifests/praxis-extproc/`. Controller-owned ExternalModel patches
  live separately under `config/manifests/external-model/` and are not part
  of the generated tree.
  Run this and commit the diff whenever you bump `PRAXIS_EXTPROC_COMMIT`.
- `make install` / `make uninstall` — apply or remove `config/self/default`
  (SA, RBAC, Deployment) standalone, namespace `opendatahub`, for local testing
  ahead of `ai-gateway-operator` vendoring this repo as a sibling of
  `maas-controller`.
- `make build-image` / `make push-image` — build/push the container image
  (`REPO=`/`TAG=` to override); `build-image` vendors manifests first.

## Pull request process

1. **Push** your branch to your fork and open a pull request against `main`.
2. **Use a conventional commit PR title**: `type: subject`, subject must not
   start with a capital letter. Allowed types: `feat`, `fix`, `docs`, `style`,
   `refactor`, `perf`, `test`, `build`, `ci`, `chore`, `revert`.
   Example: `fix: correct namespace default for praxis-extproc install`.
3. **Sign off every commit** (DCO): `git commit -s`. CI checks every commit in
   the PR for a `Signed-off-by` trailer; apply the `skip/dco` label to bypass
   for an exception.
4. **Keep PRs under the 750-line size cap.** CI counts added production lines
   (excluding `*_test.go`, `*.md`, `go.sum`, and `config/manifests/**`
   vendored content) and fails above 750. Split into a stack of smaller PRs,
   or apply the `skip/pr-conventions` label if a maintainer approves an
   exception.
5. **Keep changes focused** and make sure CI passes (see below) before
   requesting review.

## CI and checks

| Workflow | Trigger | What it checks |
|---|---|---|
| `ci.yml` / `lint` | PR + push to `main` | `golangci-lint`, version kept in sync with `tools.mk` |
| `ci.yml` / `govulncheck` | PR + push to `main` | Known-CVE scan via `govulncheck` |
| `ci.yml` / `test` | PR + push to `main` | `make test`; uploads coverage as an artifact |
| `ci.yml` / `build` | PR + push to `main` | `make binary` compiles |
| `ci.yml` / `verify-manifests` | PR + push to `main` | Re-runs `hack/scripts/get-manifests.sh` and fails if `config/manifests/praxis-extproc` drifts from the pinned commit — see "Development setup" |
| `ci.yml` / `typos` | PR + push to `main` | `crate-ci/typos` spell check |
| `ci.yml` / `shellcheck` | PR + push to `main` | `shellcheck` on `hack/` scripts |
| `pr-conventions.yml` | PR events | PR title format, DCO, PR size cap — see "Pull request process" |
| `zizmor.yml` | PR + push to `main` | Security-focused SAST on the GitHub Actions workflow files themselves (template injection, unpinned actions, excessive permissions); results go to the repo's Security tab |

**Run locally before pushing:**

- `make lint` (or `make lint LINT_FIX=true` to auto-fix)
- `make test`
- `make build` (runs `tidy`, `lint`, `test`, `binary` together)
- `shellcheck hack/scripts/*.sh` if you touched vendoring scripts

## Testing

New functionality should include tests. `pkg/render` is the reference for
coverage expectations in this repo — its test suite runs against the real
vendored `praxis-extproc` manifest, not just fixtures, so regressions in the
vendored overlay's shape are caught here too.

### Test ownership boundaries: AIGO / AIGC / MaaS

AI Gateway now spans three repos: [ai-gateway-operator](https://github.com/opendatahub-io/ai-gateway-operator) (AIGO), this repo (AIGC), and [models-as-a-service](https://github.com/opendatahub-io/models-as-a-service) (MaaS). Where a test belongs depends on what it needs to observe, not which repo you happen to be changing:

| Repo | Owns | What it tests | Test suite |
|------|------|----------------|------------|
| **AIGO** | Component setup & dependency management — deploy/status only | `AIGateway` CR reconciles to `Ready`; sibling controller Deployments become `Available`; aggregate status (`ModelsAsAServiceReady`, etc.) rolls up correctly. Never the request path. | `test/e2e/*_test.go` (Go — deploy prereqs, create CR, assert status) |
| **AIGC** (this repo) | Deploying the Praxis-backed stack + AIGC's own control-plane logic | Its own reconciliation logic (`pkg/tenant`, `pkg/render`, `pkg/controller` for ExternalModel/ExternalProvider) via real fixtures — see [`test/kind-env/README.md`](test/kind-env/README.md) and [`test/openshift-env/README.md`](test/openshift-env/README.md). For MaaS-level/request-path behavior (subscription enforcement, auth, route matching, identity headers), this repo does **not** write new test content of its own — it fetches and re-runs **MaaS's own pytest suite** against its Praxis-backed deployment, pinned via `test/maas-e2e.lock` (see [`test/e2e/README.md`](test/e2e/README.md)). | `test/kind-env`, `test/openshift-env` + vendored `test/maas-e2e/` (MaaS pytest, fetched at a pinned commit) |
| **MaaS** | The single source of truth for MaaS-level resource/request behavior | Subscription enforcement, auth policy, rate limiting, API-key lifecycle, and anything visible at the MaaS API/Gateway boundary — including OpenAI resource-API routing and identity-header propagation. Written once, run twice: in MaaS against IPP, and here via the vendored fetch against Praxis. | MaaS's `test/e2e/tests/*.py` (pytest) |

**The upstream-first rule:** if MaaS-level behavior has a coverage gap, add the test in MaaS's `test/e2e/tests/` first (skip-gated with `pytest.mark.skipif` if genuinely Praxis-only), then bump this repo's `test/maas-e2e.lock` to pick it up. Prefer an upstream MaaS PR over a post-fetch patch in this repo — see the "Post-fetch test patches vs upstream MaaS PRs" table in [`test/e2e/README.md`](test/e2e/README.md). [`#37`](https://github.com/opendatahub-io/ai-gateway-controller/pull/37) is a worked example of the full lifecycle: a gap was fixed upstream in MaaS ([models-as-a-service#1508](https://github.com/opendatahub-io/models-as-a-service/pull/1508)); once it landed, this repo removed its own side-workarounds and re-pinned the lock.

Don't add request-path Go tests (route matching, header forwarding) directly to AIGO, and don't accept a bespoke copy of MaaS-level test content here that duplicates what MaaS's pytest suite already covers or should cover.

## Repository layout

| Area | Purpose |
|---|---|
| `cmd/manager/` | Flags, manager bootstrap, registers `pkg/tenant.Reconciler` |
| `pkg/render/` | Kustomize build, placeholder post-render, SSA apply (tenant-agnostic primitives) |
| `pkg/tenant/` | Primarily watches `MaasTenantConfig` (mirroring maas-controller's own `TenantReconciler`); per opted-in (`maas.opendatahub.io/payload-processing-type: praxis` annotation) tenant, renames/patches and applies its own copy of the rendered resources, and cleans them up again via `PraxisCleanupFinalizer` (on `MaasTenantConfig`) on switch-away/deletion. Also Gets the tenant's owning `AITenant` for `status.gatewayRef`/`status.phase`. |
| `config/self/` | This repo's own deploy manifest (SA/ClusterRole/ClusterRoleBinding/Deployment), vendored by `ai-gateway-operator` |
| `config/manifests/praxis-extproc/` | Exact pinned `praxis-extproc` manifests; never add controller-specific patches here |
| `config/manifests/external-model/` | Controller-owned Kustomize composition and ExternalModel EnvoyFilter patches |
| `hack/scripts/get-manifests.sh` | Pinned-commit vendoring for `config/manifests/praxis-extproc/` |
| `Makefile`, `tools.mk` | Build/test/lint/tidy/get-manifests/build-image targets |

See [DESIGN.md](./DESIGN.md) for why `config/` is split this way and how this
repo fits into the `ai-gateway-operator` → `ai-gateway-controller` →
`praxis-extproc` deployment chain.

## Getting help

- **Open an issue** on GitHub for bugs or feature ideas.
- **Design questions:** see [DESIGN.md](./DESIGN.md) first; open a discussion
  or issue if it doesn't answer your question.
