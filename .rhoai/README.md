# API contract metadata

This repository has no checked-in OpenAPI documents. GitHub Actions runs
[API contract CRD compatibility checks](../.github/workflows/api-contract.yml)
in CRD-only mode (no `.rhoai/contracts.yaml`).

If REST or gRPC OpenAPI specs are added later, introduce a `contracts.yaml`
descriptor here per
[api-contract-tests onboarding](https://github.com/red-hat-data-services/api-contract-tests/blob/main/docs/onboarding/README.md).