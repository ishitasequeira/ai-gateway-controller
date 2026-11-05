# Static ExternalModel provider weights

## Scope and routing owner

`ExternalProviderRef.weight` already exists in the installed MaaS CRD and the
AIGC API. AIGC resolves the eligible provider bindings and publishes their
selection metadata in the versioned, content-addressed Praxis routing overlay.
Praxis `intelligent_route` performs the single destination selection; Envoy
forwards to the provider selected by the trusted handoff. AIGC does not also
weight Envoy destinations.

```text
ExternalModel refs and weights
  -> AIGC resolver and overlay renderer
  -> Praxis intelligent_route selects one eligible provider
  -> trusted selected-provider header
  -> Envoy forwards to that provider
```

The selected route retains its own provider, API format, target model, path,
and credential reference. `Resolve` excludes zero-weight providers from the
serving overlay. `ResolveAll` keeps valid referenced providers and credentials
preloaded so an administrative weight change does not require a dataplane
rollout. Preloading does not make an inactive provider selectable.

## What PR #184 supplies

The current #184 implementation supplies the control-plane behavior:

- Omitted weight defaults to `1`; the API range is `0..100`.
- Equal positive candidates use Praxis `random`.
- Unequal positive candidates use `weightedRandom`, sharing the model's
  selection group and carrying their configured `traffic_weight` values.
- A zero-weight binding is absent from the serving candidate set while its
  valid provider configuration can remain preloaded.
- Duplicate active bindings for one client-visible model are rejected.
- Selection metadata participates in overlay digest and generation
  publication. Equal unchanged content remains a no-op.
- API/design documentation and two-provider Kind checks cover proportional
  selection and zero-weight exclusion.

The weights are relative. For example, `50:25:25` and `2:1:1` express the same
intended shares. AIGC passes the configured values through, so changing from
one representation to the other publishes a new overlay revision even though
the allocation is equivalent. The existing CRD maximum is 100; larger vectors
must be scaled proportionally before use. Zero is omitted rather than emitted
as an invalid Praxis `traffic_weight`.

## Additional qualification

- Add three-provider Kind qualification for equal shares, `50:25:25`,
  equivalent `2:1:1`, explicit `100:0` to `0:100` switching, and `95:5`.
- Match every Gateway response to exactly one independent provider receipt by
  unique request ID. Verify provider attribution, HTTPS, SNI, authority,
  backend credential acceptance, method, path, and body model.
- Require stable published, recomputed, mounted, accepted, and serving overlay
  revisions before sampling; verify configuration and ExtProc identity do not
  change during each sampling window.
- Make recorder receipt storage writable only through an ephemeral volume under
  its unprivileged security context, and verify receipt storage/retrieval with
  a smoke request before any large sample.
- Add focused renderer coverage for mixed weighted, equal, and singleton
  capabilities; preserved candidates; and transitions out of `weightedRandom`.
- Extend live qualification to prove an explicit all-zero update withdraws
  serving state and produces no provider receipt, then reactivating Provider A
  restores a successful request with exactly one verified Provider A receipt.
  A controller regression also covers this behavior.

## Review findings and remaining qualification

Review of #184 found no need for a second selection implementation. It did
leave two complementary gaps: the existing runtime test samples only two
providers and does not exercise the broader allocation cases; and its
provider-recorder fixture does not provide writable storage for the independent
receipt endpoint used by this suite.

The fresh Kind qualification measures 50:50, 50:25:25, and 95:5 with 500
requests each, and checks 100:0 then 0:100 with 50 requests per direction.
The 2:1:1 case checks the published candidate weights and ratio equivalence
without another traffic sample. Distribution checks use predeclared Pearson
goodness-of-fit thresholds at alpha 0.001 per window, bounded to eight
concurrent requests. Qualification also requires zero receipts for inactive
providers, credential and attribution checks, stable configuration during
samples, all-zero withdrawal with no provider receipt, Provider A reactivation,
final Provider A restoration, key revocation, evidence scanning, and the
narrative demo. It does not claim metric-driven routing, automatic failover,
health-based promotion, or a ten-provider distribution.

On the fresh Kind run, the 500-request windows produced 249/251 for 50:50,
246/144/110 for 50:25:25, and 471/29 for 95:5. Both 50-request
active/passive windows sent every request to the sole positive-weight
provider. Each response had exactly one matching backend receipt, inactive
providers received zero receipts, configuration remained stable, and all
three statistical checks passed their predeclared thresholds. The 2:1:1
configuration matched the 50:25:25 ratio signature without another traffic
sample. Explicit all-zero withdrawal returned HTTP 404 with no attribution or
provider receipt; reactivating Provider A returned HTTP 200 with one verified
receipt and no ExtProc restart.

A subsequent 50:50 run had eight HTTP 500 responses and no receipts for those
requests. The sampling split itself was within bounds, but MaaS API logs at the
same time recorded eight key-validation failures caused by canceled DNS/DB
lookups to Postgres. After Postgres readiness and DNS resolution recovered, a
single targeted 500-request 50:50 sample passed 259/241 with 500 successful
responses and receipts. The failed attempt remains preserved and is not
reclassified as a pass.

The base Kind suite finished 34/35; its pre-auth-unavailable trust-boundary
assertion observed `stopped=true http=200 provider_contact=true`. This remains
an unresolved validation limitation; the weighting tests do not change the
pre-auth filter configuration or this assertion.
