# Amazon Bedrock Mantle ExternalModels

AIGC can route an `openai-chat` ExternalModel binding to an Amazon Bedrock
Mantle endpoint that implements the OpenAI Chat Completions API.

Use the Mantle endpoint family, such as
`bedrock-mantle.us-east-2.api.aws`, with the `/v1/chat/completions` path. Set
`ExternalProvider.spec.provider` to `bedrock-openai`, configure
`ExternalModel.spec.externalProviderRefs[].apiFormat` as `openai-chat`, and set
`targetModel` to the model identifier accepted by Mantle. Configure
`auth.type: apikey` and reference a same-namespace Kubernetes Secret whose
`api-key` entry contains the Bedrock API key. AIGC injects that key as a Bearer
token at the provider hop. Configure the provider endpoint with HTTPS; the
controller uses the endpoint host for TLS verification, SNI, and HTTP
authority.

The OpenAI-compatible request and response bodies pass through the existing
Praxis `openai-chat` path. JSON and SSE use the same provider binding. This
support does not cover Bedrock Runtime's SigV4 API, `/openai/v1/chat/completions`,
or the Bedrock Converse API.

This describes a newly configured Praxis ExternalModel binding. It does not
claim automatic migration of an existing IPP `bedrock-openai` binding or prove
that a historical RHOAI image served it. Release qualification also depends on
the Praxis AI and ExtProc changes used by AIGC #151 being released and tested
with their published images.

The Kind harness also has an opt-in live check through the deployed Gateway:
`python3 test/kind-env/e2e-api-translation.py --scope externalmodel --live-provider bedrock`.
It reads a single `Bedrock - <API key>` entry from the developer's local
`~/rhoai/provider-keys.md`. The default Mantle region is `us-east-1` and the
default model is `openai.gpt-oss-20b`; set `BEDROCK_MANTLE_REGION` or
`BEDROCK_MANTLE_MODEL` to override them. The normal deterministic Kind run
uses only the local credential-checking recorder. The opt-in live check saves
no provider key or response content in its evidence.
