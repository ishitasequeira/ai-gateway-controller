# API contract metadata

This repository has no OpenAPI provider contracts. GitHub Actions runs
[API contract CRD compatibility checks](../.github/workflows/api-contract.yml)
in CRD-only mode.

[`contracts.yaml`](contracts.yaml) is a CRD-only `ContractSet` (`spec.contracts: []`)
so the working group can register the descriptor path. Add OpenAPI `spec.contracts`
entries here when a checked-in API spec becomes a provider contract.