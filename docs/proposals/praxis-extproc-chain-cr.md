# Praxis ExtProc chain CR — making the controller/operand seam a contract

## The problem this replaces

Today ai-gateway-controller (AIGC) produces the Praxis ExtProc deployment by
composing a vendored manifest bundle and then rewriting it in Go:

```text
config/manifests/praxis-extproc/overlays/odh   (vendored kustomize overlay)
  -> render.Build            (kustomize build -> []unstructured.Unstructured)
  -> render.PostRender       (literal string substitution of placeholders)
  -> tenant.Rename           (per-tenant names/labels/selectors, token removal)
  -> tenant.SplitPostAuthResources (duplicate the post-auth copy per tenant)
  -> render.Apply            (server-side apply)
```

On top of that, `pkg/tenant/extproc.go` generates `extproc.yaml` in Go and
stomps the vendored one, while `pre-extproc.yaml` is left as vendored, and the
Praxis image is injected by a fourth mechanism (a post-render `--image` arg
substitution sourced from a ConfigMap key).

That is three separate ways for AIGC to say one thing, and the cost is real:

- **No schema.** A field AIGC writes is validated by nothing. `metrics_auth`
  against an image predating it produced `config: server: unknown field
  metrics_auth` and `exit(1)` **before the gRPC server was constructed** — a
  CrashLoop and a total MaaS inference outage, from a config typo, in every
  tenant at once.
- **No invariant ownership.** "The post-auth pod holds no API identity" is
  enforced in four disconnected places (pod `automountServiceAccountToken`,
  generated `extproc.yaml` `metrics_auth`, the ClusterRoleBinding subject, and
  the tenant ServiceMonitor's scrape credentials). Each was found — and fixed
  — separately, and a vendor sync can undo any of them silently.
- **Split config ownership.** Which of `pre-extproc.yaml` / `extproc.yaml` /
  plugin ConfigMap wins is decided by call order inside `rename.go`.
- **Duplicated knowledge.** The ExtProc Service name and port appear both in
  the Service AIGC applies and in the EnvoyFilter that points at it, so the two
  can only be kept consistent by constructing them from the same place-literal.

## The proposal

Make the seam between controller and operand an **API object** instead of a
manifest.

AIGC writes one small CR per chain per tenant describing **intent only**.
Praxis-extproc watches it, renders its own Deployment / Service(s) /
ConfigMap / metrics wiring / RBAC, and publishes the addresses the gateway
needs in status. The EnvoyFilter author (AIGC) reads addresses from status
instead of constructing them.

```text
AIGC: tenant intent ──► ExtProcChain CR ──► praxis-extproc ──► workload + Service
                                    ▲                                 │
                                    └──── status.endpoints ◄──────────┘
AIGC: EnvoyFilter ── reads status.endpoints ─────────────────────────────┘
```

### The CR

```yaml
apiVersion: praxis.opendatahub.io/v1alpha1
kind: ExtProcChain
metadata:
  name: maas-payload-processing       # one per chain per tenant, from naming.go
  namespace: <tenant namespace>
spec:
  # Where in the Envoy filter chain this sits. Drives the generated config's
  # request/response direction and whether the pre-auth model-name hook is on.
  position: PostAuth                  # PreAuth | PostAuth
  image: quay.io/opendatahub/odh-praxis-extproc@sha256:...

  # The identity the operand runs as. create/mountToken are intent; AIGC keeps
  # owning the ServiceAccount only where another controller already does.
  serviceAccount:
    name: payload-processing-post
    createInstances: true
    mountToken: false

  metrics:
    expose: true
    requireAuth: false                # must be false when mountToken is false
    serviceMonitor: true
    monitorNamespaceSelector: []      # optional override

  # Inputs the operand configures itself from. Content, not shape: AIGC does
  # not tell praxis what fields to write in its own YAML.
  pluginsConfigMapRef:
    name: payload-processing-plugins
  routing:
    # Versioned, content-addressed overlay published by AIGC's resolver.
    overlaysConfigMapRef:
      name: payload-processing-routing
    overlayRevision: "sha256:..."

  externalModel:
    enabled: true                     # secondary in-namespace chain

status:
  conditions: []                      # Ready, Accepted, ConfigRejected
  endpoints:
    - name: grpc
      address: 10.128.2.11
      port: 9004
    - name: metrics
      address: 10.128.2.11
      port: 9090
  configRevision: "sha256:..."        # what the process actually loaded
  observedGeneration: 7
```

### Rules the design depends on

1. **Intent, not manifests.** Nothing in `spec` is a pod template fragment.
   If AIGC starts putting `resources:`, `env:`, or container args in the CR,
   the proposal has failed and we rebuilt the same coupling with more YAML.
2. **Invariants are validated at admission, not at startup.** The
   `mountToken`/`requireAuth` contradiction is a CEL rule on the CRD, so a bad
   combination is rejected by the API server when AIGC applies the CR. The
   blast radius becomes one rejected write by one controller, not a
   CrashLooping dataplane. Admission rejection also names the field, which
   today's `exit(1)` does not.
3. **Praxis owns the shape of its own config.** AIGC passes *what* (plugins
   ConfigMap, routing overlay revision, position); praxis decides *how* it
   serializes that into `extproc.yaml`. The schema of that file stops being a
   cross-repo handshake hidden in `sharedExtProcConfig`.
4. **Addresses flow back through status.** This is the piece that removes
   duplicated knowledge: the EnvoyFilter is written against
   `status.endpoints[grpc]`, so the Service name/port is declared once.
   It also gives AIGC a truthful readiness signal for the filter it applies —
   apply the EnvoyFilter only once the endpoint exists.
5. **Praxis must stop being strict about unknown fields.** The CRD schema is
   the validator now. Keeping `deny_unknown_fields` on the generated config
   would re-arm the exact failure this design removes.
6. **One image, one place.** `spec.image` is the only pin AIGC writes. Today
   four pins disagree (CSV `ea.2`, AIGC main fallback, AiGO `params.env`, and
   whatever the running Deployment resolved from a ConfigMap key). Leaving
   `image` empty means "the praxis-extproc that ships with the operator", which
   is what most deployments actually want.

### Ownership after the change

| Resource | Owner |
| --- | --- |
| `ExtProcChain` CR | AIGC (per tenant, per chain) |
| Deployment, Service, metrics Service, ServiceMonitor | praxis-extproc |
| ClusterRoles / ClusterRoleBindings for metrics auth | praxis-extproc (it knows whether it authenticates) |
| `extproc.yaml` / `pre-extproc.yaml`, plugins merge | praxis-extproc |
| NetworkPolicy on the metrics and gRPC ports | praxis-extproc (it owns the ports) |
| EnvoyFilter, DestinationRule | AIGC (Envoy config; it owns the gateway side) |
| Routing overlay ConfigMap | AIGC resolver (content-addressed) |
| Plugins ConfigMap | whoever owns the tenant's customization; AIGC writes the migration gate |

The vendored `config/manifests/praxis-extproc/` bundle shrinks to what is
genuinely Envoy-side plus the standalone-installability manifest that does not
go through the controller at all.

### Migration

- The CRD ships from praxis-extproc; AIGC adds the Go type to its scheme and
  the RBAC to write it.
- Feature gate `--praxis-extproc-chain-cr` (default off). With it off, AIGC
  behaves exactly as today.
- With it on, AIGC stops applying the workload resources and starts applying
  CRs. Server-side apply field ownership does the handover:
  `aigateway-controller` relinquishes, `praxis-extproc` takes. The two
  mechanisms must not both run for one chain — the reconciler asserts that.
- Rollback is flipping the gate back; praxis, left with an unowned CR, keeps
  the workload and AIGC reclaims it on the next reconcile. Deletion of the
  CR must be blocked by a finalizer on the CR, not on AIGC's bookkeeping.
- `Render` → `Rename` → `PostRender` keeps its current tests while the gate is
  off, so the change is additive and reviewable in slices.

### What praxis-extproc has to add

1. The CRD (`ExtProcChain`), with CEL validation for rule 2.
2. A controller: watch the CR in tenant namespaces, render, apply, publish
   status and conditions, honor `spec.routing.overlayRevision` by reloading
   config without restarting the pod where it can.
3. A published JSON Schema (or the CRD itself) for its own config, so AIGC's
   golden configs can be parsed in CI — cheap, and it catches the
   `metrics_auth` class of break before it ships.
4. Drop `deny_unknown_fields` for the fields AIGC writes.

### Non-goals

- Standard mode (Praxis as the tenant-facing gateway). That is
  [praxis-dataplane-evolution.md](../praxis-dataplane-evolution.md)'s post-3.6
  draft, a different problem with a different owner.
- IPP. The IPP path keeps its own lifecycle until it is removed.
- Multi-cluster fan-out. The CR is a per-cluster object; federation stays out
  of scope.

### Risks

- **Praxis becomes privileged.** Creating ClusterRoleBindings per tenant from
  the operand expands its RBAC and needs a separate review; the alternative
  (AIGC keeps RBAC, praxis only reports what it needs in status) is the
  fallback if that review blocks.
- **Two reconcilers on one namespace.** Handover windows need the field-owner
  assertion above or resources will flap.
- **Sequencing.** None of this helps until praxis ships the CRD. The cheap
  slice of this proposal — the schema publication and dropping
  `deny_unknown_fields` — is worth doing first and independently.

## Cheap, independent steps (do these regardless)

1. praxis-extproc: tolerate unknown config fields; publish its config schema.
2. AIGC: a per-chain descriptor struct in Go that derives *all* of
   deployment identity, token mounting, metrics auth and monitor credentials
   from one place. This removes the four-way desync without waiting for the
   CRD and is a strict prerequisite for knowing what belongs in the CR.
3. AIGC: `ServiceMonitor` credentials and the `extproc.yaml` `metrics_auth`
   value come from that descriptor, not from two literals.
