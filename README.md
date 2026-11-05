# ai-gateway-controller

Installs the `praxis-extproc` dataplane manifests. Deployed by
`ai-gateway-operator` as a sibling of `maas-controller`.

See [DESIGN.md](./DESIGN.md) for scope, architecture rationale, and what is
explicitly deferred. Read `DESIGN.md` before adding code.

See [CONTRIBUTING.md](./CONTRIBUTING.md) for the PR process, CI checks, and
development setup.

## Production image contract

The ODH operator and release packaging own the ExtProc dataplane image used by
an installed controller. They provide `praxis-extproc-image` through
`config/self/default/params.env`; the controller Deployment passes that value
to `--image`. There is no standalone `praxis-ai` hop: Gateway → ExtProc is the
dataplane, and ExternalModel HTTPRoutes backend to provider ExternalName
Services.

## Kind integration environment

The [Kind environment](./test/kind-env/README.md) provides reproducible
functional integration and routing validation using pinned source revisions
and locally loaded images. It is isolated from production manifests and
records evidence for every qualification run.

See [Amazon Bedrock Mantle ExternalModels](./docs/external-model-bedrock-mantle.md)
for the provider identifier, authentication, endpoint, and API scope.
