# Kind ExternalModel ExtProc environment

This directory is the bounded entrypoint for the local Kind environment
described by ADR 0001. It creates and uses only the named Kind context,
records provenance and cluster state, and supports `--preflight`,
`--provision`, and `--destroy`.

MaaS tenant opt-in uses the `MaasTenantConfig/default-tenant` annotation
contract. The AITenant supplies only ownership, status, and Gateway identity:

```yaml
metadata:
  name: default-tenant
  namespace: <resolved-tenant-namespace>
  annotations:
    maas.opendatahub.io/payload-processing-type: praxis
```

The Kind provisioner applies the AITenant first, waits for MaaS to create this
config in the resolved tenant namespace, and then annotates the config. An
absent, empty, or unrecognized value follows the controller's current Praxis
default; explicit `ipp` selects the MaaS-owned IPP path. The ExternalModel
controller never reads a backend selector from AITenant and never claims
MaaS-owned IPP resources.

The controller watches ExternalModel and ExternalProvider, publishes transport
resources before the content-addressed overlay, and the local manifests
provide two Katan backends plus tenant-local ExternalModel ExtProc. The default
qualification entrypoint is `e2e.sh`, which executes the ExtProc-only suite.
The transition implementation is not part of this checkout's executable
qualification. Istio, Kuadrant, and the MaaS platform remain
explicit prerequisites for the full authenticated chain; the provisioner
records a failure rather than silently substituting them.

The executable qualification records numbered routing assertions. In addition to the
core transport, routing, hot-reload, digest, and last-known-good checks, it
proves semantic no-op stability after a real provider watch event and provider
status-gate loss/recovery. An
explicitly non-Ready provider is excluded from the next resolved route set;
the previously distributed overlay remains intact until a valid replacement
is published. A newly-created provider with an empty phase is admitted once
for bootstrap so its transport resources can establish Ready.

The local configuration now provisions two independently serving tenant
stacks. Tenant A is `models-as-a-service`; tenant B is
`ai-tenant-tenant-b`. Their Gateway, ExtProc Service, overlay ConfigMap,
ExternalModel, ExternalProvider, and transport resources are distinct. Katan
backends remain in `maas-system`, and evidence records that backend namespace
separately. The extended run proves positive tenant-B traffic after a bounded
tenant-B Gateway route/provider convergence gate; this is workload and route
isolation evidence, not proof of multi-tenant MaaS authorization. Gateway-local
provider `DestinationRule` objects carry a tenant ownership label and are
tenant-qualified when more than one tenant shares `maas-system`, so endpoint
and SNI policy from one tenant cannot overwrite another tenant's policy. A
separate `ai-tenant-transition` fixture is explicitly set to `ipp` and
reserved for real MaaS IPP cutover/rollback qualification.

### Shared External Model resources

The reusable Provider A/B identities, optional `gpt-4o-mini` OpenAI CR chain,
persistent in-cluster client base, and credential-safe request helper live in
`test/external-model/`. The Kind harness renders these resources with explicit
test-only values. Katan remains a Kind-only plaintext fixture and the Gateway
client uses the Kind HTTP Service; OpenShift keeps its service-ca-backed Katan
HTTPS and public Gateway TLS configuration.

The OpenAI CR chain is opt-in only and is never included by the base manifest
application glob. It contains Secret references, not credential values.

For a fresh cold validation, use a unique cluster and evidence root:

```sh
LOCAL_ENV_CLUSTER=external-model-kind-<run-id> \
LOCAL_ENV_EVIDENCE_ROOT="$PWD/evidence/<run-id>" ./run.sh --destroy || true
LOCAL_ENV_CLUSTER=external-model-kind-<run-id> \
LOCAL_ENV_EVIDENCE_ROOT="$PWD/evidence/<run-id>" ./run.sh --provision
LOCAL_ENV_CLUSTER=external-model-kind-<run-id> ./e2e.sh
./demo.sh --context kind-external-model-kind-<run-id> --non-interactive
LOCAL_ENV_CLUSTER=external-model-kind-<run-id> \
LOCAL_ENV_EVIDENCE_ROOT="$PWD/evidence/<run-id>" ./run.sh --destroy
```

Replace `<run-id>` consistently. The normal qualification does not apply the
optional OpenAI resources; that workflow must opt in explicitly through its
documented OpenAI command path.

### Shared External Model resources

The reusable Provider A/B identities, optional `gpt-4o-mini` OpenAI CR chain,
persistent in-cluster client base, and credential-safe request helper live in
`test/external-model/`. The Kind harness renders these resources with explicit
test-only values. Katan remains a Kind-only plaintext fixture and the Gateway
client uses the Kind HTTP Service; OpenShift keeps its service-ca-backed Katan
HTTPS and public Gateway TLS configuration.

The OpenAI CR chain is opt-in only and is never included by the base manifest
application glob. It contains Secret references, not credential values.

### Namespace boundaries

The production-shaped split is deliberate:

```text
client -> Gateway/Envoy + Kuadrant + ExtProc       (maas-system)
                              |
                              v
                 HTTPRoute parent reference
                              |
       tenant HTTPRoute -> tenant-local ExternalModel ExtProc (tenant namespace)
                              |
                              v
                 provider Service/mesh transport    (tenant namespace)
                              |
                              v
                 external or fixture backend        (maas-system)
```

The HTTPRoute is created in the resolved tenant namespace and attaches to a
Gateway in `maas-system`. Gateway listeners explicitly allow routes from the
tenant namespaces. The route backend is the same-namespace controller-owned
transport Service, so
the route does not need a `ReferenceGrant`; no broad cross-namespace backend
grant is installed. If a future design sends a backendRef to another
namespace, that design must add a ReferenceGrant in the backend namespace
limited to this Gateway API Service reference.

Provider backend fixtures are separate from tenant state: they live in
`maas-system`. The controller-created ExternalName Service, ServiceEntry,
HTTPRoute, overlay, and projected Secret volume live in the resolved tenant
namespace; Gateway-local `DestinationRule` resources live in `maas-system`
and use tenant-qualified names when needed. Kubernetes Secret projection is namespace-bound;
each ExternalModel ExtProc ServiceAccount has token automount disabled and no Secret API
permission. The E2E captures route parent/backend namespaces, Gateway
`allowedRoutes`, ReferenceGrant inventory, projected Secret identities, and
`kubectl auth can-i` denial for ExtProc cross-tenant Secret reads. Production
manifests contain no Kind-only certificate, callback identity, or image-policy
adaptation.

### Shared MaaS authorization callback

The Kind workflow does not install a callback proxy or response-faking adapter.
Authorino calls the `maas-api` Service directly, and that Service selects
exactly one Ready source-built MaaS API pod. The real MaaS API performs key
validation and subscription selection. The Kind-only certificate and CA
reproduce the trusted TLS relationship that OpenShift normally supplies; they
do not authorize requests, choose a tenant, rewrite callback bodies, or
manufacture responses.

The qualification attempts to record the real
`/internal/v1/api-keys/validate` and
`/internal/v1/subscriptions/select` access-log observations during an
authenticated Gateway request, plus selected tenant/subscription data when
the real API emits it and TLS fingerprints. Direct API key creation is only
fixture setup and is not counted as callback proof. No key or Authorization
value is written to evidence.

The current route-scoped Kind fixture observes API-key validation and the full
authorized ExtProc request. The generated AuthPolicy is inspected before
classifying subscription-selection callback evidence: if the policy requires
`/internal/v1/subscriptions/select`, an absent callback fails the routing
qualification; if it does not, the behavior is recorded separately as
`NOT_DEMONSTRATED` and is excluded from the functional routing total. No
canned response or synthetic MaaS service is used.

This proves single-tenant MaaS authorization callback compatibility. It does
not prove tenant-aware dispatch for multiple tenant-qualified MaaS APIs; that
broader shared-URL behavior remains `NOT_DEMONSTRATED` and is a separate MaaS
design issue affecting both the existing IPP path and ExtProc mode. Production
manifests contain none of these Kind-only certificate or callback adaptations.

The Kind certificate and CA fixture can be removed when the Kind deployment
provides the same trusted `maas-api` Service identity and CA relationship as
the target platform. The direct Service selector and one-Ready-pod assertion
should remain: they verify that the test reaches the real MaaS API rather than
an adapter.

For the Katan fixture, the provisioner records the public multi-architecture
digest
`ghcr.io/nerdalert/llm-katan@sha256:11379a1ec2fd69dc121eada6c544eb423a7c074414507dc1d474f4abba9df75a`
and pulls its `linux/amd64` child digest
`sha256:a8bf18109e2db641ef4a63efe65f69d4d6554f1128a174053de89a8b81b4284d`
before loading that platform image under a local Kind name. The local name is
a container-runtime transport reference only; the published digest and source
commit are recorded in provisioning evidence, and the fixture is still the
published Katan image rather than a mock service.
`KATAN_IMAGE` overrides the image and `BUILD_KATAN=true` is the explicit local
source-build path.

### Kind-only security compatibility

The production ExtProc workload leaves pod UID, GID, and fsGroup unset so an
OpenShift restricted SCC can assign the namespace-safe identity. Kind does not
perform that admission mutation and rejects the image's named non-root user
when `runAsNonRoot` is set. After the controller creates each tenant ExtProc
Deployment, the Kind provisioner applies the fixture-only numeric identity
`65532` and records that transformation in the provision evidence. This
transform is not in production manifests and is not used by the OpenShift
workflow.
The run-owned Katan Services expose verified TLS on port 443. The controller
uses the declared provider endpoint and its TLS configuration; it does not
infer plaintext from a Kubernetes Service name or silently downgrade an
endpoint. Any plaintext fixture must be an explicit test-only configuration,
not a production transport behavior.

The Katan backends are credential-enforcing fixtures, not permissive traffic
sinks. ExtProc providers use the run-only `kind-only-dummy` value and the
explicit-`ipp` IPP transition fixture uses its separate
`transition-provider-key` value through the dedicated `katan-transition`
Deployment. These values are qualification fixtures only and are never
production credentials. The E2E first proves direct requests without a
credential or with the wrong credential receive HTTP 401. The authenticated
Gateway request carries the MaaS API key in `Authorization`, while the backend
accepts only the distinct projected provider credential; its attributed HTTP
200 proves the ExtProc credential-injection chain replaced the caller
credential. A separate request proves a
client-supplied `x-api-key` cannot replace it either. Duplicate `Authorization`
header ordering is outside this claim. The expected provider credential is kept
out of logs and evidence.

Run the declarative fixture regression check with:

```console
./test/kind-env/test-provider-credentials.sh
```

To validate the retained cluster as well, pass its explicit Kind context:

```console
./test/kind-env/test-provider-credentials.sh \
  --context kind-external-model-two-plane
```

The live check verifies that every active Katan Deployment still contains
`--validate-keys` and its expected `--api-keys openai=...` mapping, then probes
Provider A, Provider B, and the separate IPP transition Service. For each it
requires HTTP 401 for no credential and a wrong Bearer credential, HTTP 200 for
the matching fixture credential, and HTTP 200 when a wrong `x-api-key` is
added alongside the matching Bearer credential. It prints status codes only;
fixture credentials and response bodies are not written to evidence. The
Gateway qualification remains authoritative for proving that the MaaS caller
credential is replaced by the projected provider credential.

Credential injection is implemented in the ExtProc `credential_inject` filter.
The controller renders a tenant-local ExternalModel ExtProc Deployment, its
reference-only filter configuration, and a projected provider Secret volume.
The generated ServiceAccount has token automount disabled. Existing Secret
content rotation does not alter the pod template; changing the referenced
Secret set intentionally changes the template and may roll out the tenant
ExtProc pod.

The shared MaaS/KServe post-auth ExtProc filter remains BUFFERED. The
ExternalModel-only filter is a separate SEND/NONE chain, disabled by default
and enabled only by controller-owned route patches. Ordinary KServe routes on
the same Gateway therefore retain their upstream body-processing behavior.

The transition fixture uses a real `MaasTenantConfig` explicitly set to
`ipp` and MaaS model
resources. It reached real IPP ownership in the fresh run, but the existing IPP
request/cutover path was blocked by a stale IPP ExternalModel-owned HTTPRoute.
The narrowly scoped MaaS change now deletes that route only after the IPP writer
stops and only when its labels and exact owner UID prove ownership; ambiguous,
user-owned, or controller-owned routes remain untouched. The current annotation
contract and controller-owned cleanup are preserved; the selector must not be
reintroduced on AITenant.

The Kind harness writes only the `payload-processing-type` selector. It does
not synthesize `payload-processing-status=cleanup-complete`; MaaS and the
selected controller own that handoff signal. Provisioning waits until the
Praxis-selected configs reach `steady` before creating ExternalModel fixtures.
If a config is temporarily missing, the ExternalModel controller retains
serving state and waits rather than inferring selection from AITenant or
deleting resources. An explicit `ipp` selector is required for switch-away
cleanup.

The controller still uses the persisted ExternalProvider phase as its
reconciliation gate. Before each live provider request, the Kind E2E also
waits for the selected Envoy provider cluster to report a healthy Endpoint
through the Envoy admin interface, with a bounded timeout. The semantic
overlay gate additionally requires the published ConfigMap digest, a fresh
recomputed digest, the mounted ExtProc file, and the ExtProc accepted and
serving revisions to agree for two consecutive samples. It is used before
initial Provider A traffic, the A-to-B switch, the Provider A reset, and the
last-known-good request after an invalid overlay. This separates resource
reconciliation from request-path endpoint convergence; it does not claim
health-based provider selection or failover.

The corrected qualification uses the run-owned CA with `curl --cacert`; TLS
verification is enabled and no HTTP downgrade or insecure flag is used. Evidence
records only CA/certificate fingerprints and paths, never keys or credentials.

Required source checkouts are supplied through environment variables:

* `LLM_KATAN_REPO`
* `PRAXIS_REPO`
* `MAAS_CONTROLLER_REPO`
* `KUADRANT_OPERATOR_REPO`
* `PRAXIS_EXTPROC_REPO`

Source selection and provenance are explicit. `PRAXIS_REPO` and
`PRAXIS_EXTPROC_REPO` point at the checked-out integration sources; the
expected revisions are recorded before each build. `MAAS_CONTROLLER_REPO`
must point at the merged `main` branch for new runs (the harness verifies that
the checkout is clean but does not enforce the branch name). `KUADRANT_OPERATOR_REPO`
is the pinned dependency checkout used for platform fixtures. `LLM_KATAN_REPO`
is optional in the normal workflow because the default provider image is the
public digest-pinned artifact; a source checkout is used only for explicit
development builds.

Each run records repository path, remote, branch or detached state, HEAD,
tracked diff hash, and untracked-content hash before building. After
publication, the image reference is resolved to a registry digest and the
deployed digest is recorded separately. The digest is the executable artifact;
the source revision is provenance. A dirty dependency or unexpected source
state is not silently changed by the harness.

No kubeconfig context is read implicitly. Provisioning uses only
`kind-${LOCAL_ENV_CLUSTER}`. Images are built from these checkouts when absent
and loaded into Kind with `imagePullPolicy: Never`.

## Mock external provider

The E2E currently builds its mock provider from
[`yossiovadia/llm-katan`](https://github.com/yossiovadia/llm-katan) at commit
`a5a47568ac6daf1d4bd8b356e7b350cce9ceca2a`. LLM-Katan is used only to provide
deterministic provider-compatible responses for routing, hot-swap, and
last-known-good assertions. It is not deployed as part of the production
architecture. It may eventually be replaced by a smaller purpose-built mock
that satisfies the same E2E contract.
## Optional real OpenAI qualification

This is an opt-in, single-provider diagnostic for a run-owned Kind cluster. It
is separate from the fixture-backed qualification and makes one minimal,
paid request through the public MaaS Gateway. Do not run it in automated
qualification or with a production credential.

The provider credential and the MaaS client key are different credentials. The
OpenAI key belongs only in a tenant-local Secret created from a protected local
file; the MaaS client key is minted through `POST /v1/api-keys` and is revoked
after the request. Disable shell tracing before handling either credential, do
not put a key value in an argument or evidence, and never add either value to
a ConfigMap or overlay.

The temporary control-plane objects must use these identities:

* `ExternalProvider` may have an operational name such as `openai` and points
  to `api.openai.com` with `provider: openai`, `auth.type: apikey`, and the
  tenant-local Secret reference.
* `ExternalModel.metadata.name` is an infrastructure name such as
  `openai-gpt-4o-mini`, while `spec.modelName` is the client-visible
  `gpt-4o-mini` and `targetModel` is also `gpt-4o-mini`.
* `MaaSModelRef.metadata.name` must be exactly `gpt-4o-mini`; its
  `spec.modelRef.name` points to the ExternalModel name. The temporary
  subscription must reference that MaaSModelRef. MaaS authorization matches
  the client-visible model identity, not the ExternalModel resource name.

Create the provider Secret without printing its contents:

```console
export OPENAI_KEY_FILE=/path/to/protected/openai-key
export OPENAI_RUN_ID=external-model-openai-$(date -u +%Y%m%dT%H%M%SZ)
export OPENAI_EVIDENCE="$PWD/evidence/$OPENAI_RUN_ID"
export OPENAI_CLUSTER="external-model-openai-${OPENAI_RUN_ID##*-}"
export KUBECTX="kind-$OPENAI_CLUSTER"

# Start from the ordinary fixture stack. The OpenAI resources are not part of
# base provisioning and are never selected by a manifest glob.
LOCAL_ENV_CLUSTER="$OPENAI_CLUSTER" \
LOCAL_ENV_EVIDENCE_ROOT="$OPENAI_EVIDENCE" \
PRAXIS_EXTRA_KNOWN_CLUSTERS=provider-openai \
APPLY_REAL_OPENAI_FIXTURE=false \
  ./run.sh --provision

set +x
kubectl --context "$KUBECTX" -n models-as-a-service create secret generic openai-provider-credentials \
  --from-file=api-key="$OPENAI_KEY_FILE"
```

Apply the shared CR chain and the Kind-only policy explicitly. Do not patch an
overlay or generated AuthPolicy:

```console
set +x
EXTERNAL_MODEL_NAMESPACE=models-as-a-service \
EXTERNAL_MODEL_RUN_ID="$OPENAI_RUN_ID" \
EXTERNAL_MODEL_OPENAI_SECRET=openai-provider-credentials \
EXTERNAL_MODEL_OPENAI_SUBSCRIPTION=openai-e2e-subscription \
EXTERNAL_MODEL_OPENAI_POLICY=openai-e2e-access \
EXTERNAL_MODEL_OPENAI_USER=kind-user \
  envsubst '${EXTERNAL_MODEL_NAMESPACE} ${EXTERNAL_MODEL_RUN_ID} ${EXTERNAL_MODEL_OPENAI_SECRET} ${EXTERNAL_MODEL_OPENAI_SUBSCRIPTION} ${EXTERNAL_MODEL_OPENAI_POLICY} ${EXTERNAL_MODEL_OPENAI_USER}' \
    <test/external-model/openai.yaml.tmpl | kubectl --context "$KUBECTX" apply -f -
kubectl --context "$KUBECTX" apply -f test/kind-env/manifests/45-real-openai-policies.yaml
set +x
```

Create a run-owned in-cluster curl client. Kind uses the HTTP Gateway listener;
TLS remains mandatory on the ExtProc-to-OpenAI connection. The client has no
ServiceAccount token and no embedded credentials:

```console
docker pull curlimages/curl:8.10.1
kind load docker-image curlimages/curl:8.10.1 --name "$OPENAI_CLUSTER"
export MAAS_CA="$(cat "$OPENAI_EVIDENCE/.active-run")/maas-api-ca.crt"
kubectl --context "$KUBECTX" -n models-as-a-service create configmap openai-client-ca \
  --from-file=ca.crt="$MAAS_CA" --dry-run=client -o yaml | \
  kubectl --context "$KUBECTX" apply -f -
EXTERNAL_MODEL_CLIENT_NAME=openai-client \
EXTERNAL_MODEL_CLIENT_NAMESPACE=models-as-a-service \
EXTERNAL_MODEL_CLIENT_IMAGE=curlimages/curl:8.10.1 \
EXTERNAL_MODEL_RUN_ID="$OPENAI_RUN_ID" \
EXTERNAL_MODEL_CLIENT_VOLUMES='[{"name":"maas-ca","configMap":{"name":"openai-client-ca"}}]' \
EXTERNAL_MODEL_CLIENT_VOLUME_MOUNTS='[{"name":"maas-ca","mountPath":"/etc/maas-ca","readOnly":true}]' \
  envsubst '${EXTERNAL_MODEL_CLIENT_NAME} ${EXTERNAL_MODEL_CLIENT_NAMESPACE} ${EXTERNAL_MODEL_CLIENT_IMAGE} ${EXTERNAL_MODEL_RUN_ID} ${EXTERNAL_MODEL_CLIENT_VOLUMES} ${EXTERNAL_MODEL_CLIENT_VOLUME_MOUNTS}' \
    >"$OPENAI_EVIDENCE/client-base.yaml"
yq eval-all 'select(fileIndex == 0) * select(fileIndex == 1)' \
  "$OPENAI_EVIDENCE/client-base.yaml" test/kind-env/manifests/60-client-kind-patch.yaml \
  | kubectl --context "$KUBECTX" apply -f -
kubectl --context "$KUBECTX" -n models-as-a-service wait --for=condition=Ready pod/openai-client --timeout=120s
```

Before sending traffic, wait for `ExternalProvider`, `ExternalModel`,
`MaaSModelRef`, subscription, `HTTPRoute`, and ExtProc readiness. Inspect the
controller-generated overlay and require exactly one OpenAI cluster with
`api.openai.com:443`, `http.authority: api.openai.com`, TLS enabled,
`tls.sni: api.openai.com`, certificate verification enabled, and no random
selection policy. The overlay contains only the Secret reference. Plaintext is
permitted only for explicitly configured test-only Katan fixture endpoints;
public provider endpoints otherwise use verified TLS. The qualified API
contract is `openai-chat` with `/v1/chat/completions`; `/v1/responses` is not
supported by this integration.

Start a short-lived verified port-forward only for MaaS key administration;
the OpenAI request itself is sent from the in-cluster client and never uses a
port-forward. Keep the response in a shell variable while tracing is disabled,
and never print it:

```console
set +x
kubectl --context "$KUBECTX" -n maas-system port-forward svc/maas-api 18443:8443 >"$OPENAI_EVIDENCE/maas-api-admin-port-forward.log" 2>&1 &
MAAS_PF=$!
for _ in $(seq 1 20); do rg -q 'Forwarding from' "$OPENAI_EVIDENCE/maas-api-admin-port-forward.log" && break; sleep 1; done
MAAS_KEY_RESPONSE=$(curl --cacert "$MAAS_CA" --resolve "maas-api.maas-system.svc.cluster.local:18443:127.0.0.1" \
  -sS -H 'content-type: application/json' \
  -H 'X-MaaS-Username: kind-user' -H 'X-MaaS-Group: ["system:authenticated"]' \
  --data '{"name":"openai-kind-e2e","ephemeral":true,"subscription":"openai-e2e-subscription"}' \
  https://maas-api.maas-system.svc.cluster.local:18443/v1/api-keys)
MAAS_KEY=$(jq -er '.key' <<<"$MAAS_KEY_RESPONSE")
MAAS_KEY_ID=$(jq -er '.id' <<<"$MAAS_KEY_RESPONSE")
unset MAAS_KEY_RESPONSE
kill "$MAAS_PF" 2>/dev/null || true
wait "$MAAS_PF" 2>/dev/null || true
```

Send exactly one request from the client Pod. This is the only paid request;
do not retry a received HTTP response:

```console
BODY='{"model":"gpt-4o-mini","messages":[{"role":"user","content":"Hello"}]}'
printf '%s\n%s\n' "$MAAS_KEY" "$BODY" | \
  kubectl --context "$KUBECTX" -n models-as-a-service exec -i openai-client -- \
    sh -c 'read -r key && read -r body && curl --silent --show-error --max-time 60 \
      -D /tmp/headers -o /tmp/body -w "%{http_code}" \
      -H "Authorization: Bearer $key" -H "Content-Type: application/json" \
      --data-binary "$body" \
      "http://maas-default-gateway.maas-system.svc.cluster.local/models-as-a-service/gpt-4o-mini/v1/chat/completions"'
```

Capture only the HTTP status, response model/ID, and allowlisted OpenAI
response headers. Never retain Authorization, API-key, cookie, Secret, or
private-key values. If the provider returns an error, retain only
`error.message`, `error.type`, `error.code`, and `error.param` in sanitized
evidence, then delete the raw response.

Revoke the temporary MaaS key and remove the run-owned resources in dependency
order. The API key is passed through the Pod stdin and is not a command
argument:

```console
set +x
kubectl --context "$KUBECTX" -n maas-system port-forward svc/maas-api 18443:8443 >"$OPENAI_EVIDENCE/maas-api-admin-port-forward.log" 2>&1 &
MAAS_PF=$!
for _ in $(seq 1 20); do rg -q 'Forwarding from' "$OPENAI_EVIDENCE/maas-api-admin-port-forward.log" && break; sleep 1; done
curl --cacert "$MAAS_CA" --resolve "maas-api.maas-system.svc.cluster.local:18443:127.0.0.1" \
  --silent --show-error --output /dev/null --write-out '%{http_code}' \
  -X DELETE "https://maas-api.maas-system.svc.cluster.local:18443/v1/api-keys/$MAAS_KEY_ID" \
  -H "Authorization: Bearer $MAAS_KEY"
kill "$MAAS_PF" 2>/dev/null || true
wait "$MAAS_PF" 2>/dev/null || true
kubectl --context "$KUBECTX" -n models-as-a-service delete externalmodel openai-gpt-4o-mini --ignore-not-found
kubectl --context "$KUBECTX" -n models-as-a-service delete externalprovider openai --ignore-not-found
kubectl --context "$KUBECTX" -n models-as-a-service delete maasmodelref gpt-4o-mini maassubscription openai-e2e-subscription maasauthpolicy openai-e2e-access --ignore-not-found
kubectl --context "$KUBECTX" -n models-as-a-service delete secret openai-provider-credentials --ignore-not-found
kubectl --context "$KUBECTX" -n models-as-a-service delete configmap openai-client-ca --ignore-not-found
kubectl --context "$KUBECTX" -n models-as-a-service delete pod openai-client --ignore-not-found
unset MAAS_KEY MAAS_KEY_ID MAAS_ADMIN_TOKEN
```

Restore Provider A and prove one fixture request returns HTTP 200 before
retaining or destroying the cluster. Scan the evidence and worktree for
credential-shaped values. The normal fixture qualification must be run before
the opt-in request; its cleanup must restore Provider A rather than deleting
the baseline model or subscription.

Send one request through the Gateway, without retries or optional fields:

```json
{"model":"gpt-4o-mini","messages":[{"role":"user","content":"Hello"}]}
```

Capture only status and an allowlist of non-secret response headers. If the
provider returns an error, copy only `error.message`, `error.type`,
`error.code`, and `error.param` into a sanitized artifact, then immediately
truncate the restricted temporary response file. On success, retain only the
response model, response ID, and a short summary. Never save Authorization,
API-key, cookie, Secret, or private-key values.

Cleanup is ownership-ordered: revoke temporary MaaS keys, remove the
ExternalModel and ExternalProvider, wait for their routes and transports to
disappear, remove the MaaSModelRef/subscription/policy additions, then remove
the provider Secret. Restore the normal Provider A fixture and verify one
fixture request returns HTTP 200 before retaining or destroying the cluster.
Scan the evidence and worktree for credential-shaped values. This workflow
proves one real provider request; it does not prove multi-provider
authorization dispatch, credential rotation, OAuth2, SigV4, or IPP
transition/rollback.
