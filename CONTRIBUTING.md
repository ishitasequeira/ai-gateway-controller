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
   (excluding `*_test.go`, `*.md`, `go.sum`, `config/manifests/**`, and
   vendored YAML under `test/envtest/testdata/crds/`) and fails above 750.
   Split into a stack of smaller PRs,
   or apply the `skip/pr-conventions` label if a maintainer approves an
   exception.
5. **Keep changes focused** and make sure CI passes (see below) before
   requesting review.

## CI and checks

| Workflow | Trigger | What it checks |
|---|---|---|
| `ci.yml` / `lint` | PR + push to `main` | `golangci-lint`, version kept in sync with `tools.mk` |
| `ci.yml` / `govulncheck` | PR + push to `main` | Known-CVE scan via `govulncheck` |
| `ci.yml` / `test` | PR + push to `main` | `make test-unit`; uploads coverage as an artifact |
| `ci.yml` / `test-envtest` | PR + push to `main` | Runs controller scenarios through a real manager with shipped CRDs and RBAC |
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

`make test` runs both unit tests and control-plane scenarios. Use `make test-unit`
for the fast loop, or `make test-envtest` to exercise controller watches and
reconciliation with shipped permissions. The latter uses vendored dependency
CRDs and downloads pinned control-plane binaries; it needs no cluster or
container runtime. Sources and
refresh instructions live beside the [vendored schemas](test/envtest/testdata/crds/README.md).
See [the envtest guide](test/envtest/README.md) for focused runs and fixtures.

New functionality should include tests. `pkg/render` is the reference for
coverage expectations in this repo — its test suite runs against the real
vendored `praxis-extproc` manifest, not just fixtures, so regressions in the
vendored overlay's shape are caught here too.

## Repository layout

| Area | Purpose |
|---|---|
| `cmd/manager/` | Flags, manager bootstrap, registers `pkg/tenant.Reconciler` |
| `pkg/render/` | Kustomize build, placeholder post-render, SSA apply (tenant-agnostic primitives) |
| `pkg/tenant/` | Primarily watches `MaasTenantConfig` (mirroring maas-controller's own `TenantReconciler`); per config whose `maas.opendatahub.io/payload-processing-type` is absent or `praxis`, renames/patches and applies its own copy of the rendered resources, and cleans them up again via `PraxisCleanupFinalizer` (on `MaasTenantConfig`) on switch-away/deletion. Explicit `ipp` leaves payload processing to MaaS. It also Gets the owning `AITenant` for `status.gatewayRef`/`status.phase`. |
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
