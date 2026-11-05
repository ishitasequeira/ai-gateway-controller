# Honor explicit all-zero provider weights

## Behavior

When every `ExternalModel` provider reference has an explicit weight of zero, the configuration has no eligible destination. AIGC should withdraw that model's previously published serving route and overlay candidate rather than continue serving its last-known-good provider selection.

This rule applies only to an explicit all-zero configuration. Provider-resolution and readiness failures continue to preserve last-known-good serving state. Setting a provider's weight back above zero reactivates it through ordinary reconciliation.

## Acceptance checks

- Reconcile a serving model, set all provider weights to zero, and verify the current generation reports no eligible route, with no model HTTPRoute or serving overlay candidate.
- Send one request after withdrawal and verify it is not attributed to a provider and produces no backend receipt.
- Re-enable Provider A and verify the route and overlay return, then verify an authenticated request returns HTTP 200 with Provider A attribution and one matching receipt.
- Keep the zero-weight regression scoped to this model; do not change provider-readiness last-known-good handling.

## Validation

The controller regression test verifies that changing a previously serving model
to all-zero removes its HTTPRoute and overlay, and reports the current
generation as Failed with `Ready=False/NoRoutes`. A live Kind check confirmed
one post-withdrawal request returned 404 with no provider attribution or backend
receipt; after Provider A was re-enabled, one authenticated request returned
200 with exactly one matching receipt. Provider A was restored and the
temporary MaaS key was revoked.
