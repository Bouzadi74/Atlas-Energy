# PostgreSQL optimization-result publication

## Contract

`atlas-publish-results` is a separate serving-boundary command. It does not rerun or modify the
optimizer. It reads one immutable run directory, verifies the manifest and every declared SHA-256
checksum, validates table schemas and natural keys, repeats energy/cost/emissions reconciliation,
and only then opens a PostgreSQL transaction.

The publisher writes:

- `optimization.optimization_runs`: run status, scenario years, KPIs, solver/model/input versions,
  artifact root, manifest checksum, and publication lifecycle;
- `optimization.scenario_inputs`: the canonical scenario assumptions and typed high-use fields;
- `optimization.run_artifacts`: format, relative location, checksum, byte count, and row count;
- `optimization.capacity_results`: existing, optimized, and new MW plus storage MWh;
- `optimization.dispatch_hourly`: generator/import/reliability dispatch MW;
- `optimization.storage_hourly`: signed storage dispatch MW and state of charge MWh;
- `optimization.cost_components`: asset-level capital and operating cost;
- `optimization.run_provenance`: input, software, validation, and output-contract JSON records.

The current optimizer artifacts expose capital and combined operating/marginal costs. The publisher
does not invent separate fuel and carbon components. That split requires a future optimizer output
contract change with an exact objective reconciliation.

## Atomicity and idempotency

All file validation happens before publication. All database inserts happen in one transaction;
any failure rolls back the run and all child rows. Repeating a run with the same manifest checksum
returns `reused=True`. Reusing a run ID with different bytes is rejected. A newer publication for
the same scenario, run mode, and calendar marks the previous row `superseded` without deleting its
audit history.

Validated pandas frames act as the staging boundary before the atomic PostgreSQL load. Separate
permanent staging tables are intentionally avoided because they would add cleanup and concurrent
publication state without improving visibility: uncommitted final rows are already invisible to
readers under PostgreSQL transactions.

## Apply migrations and publish

```powershell
Get-Content -Raw .\infra\postgres\migrations\003_optimization_results.sql |
  docker compose exec -T postgres psql -v ON_ERROR_STOP=1 -U atlas -d atlas

Get-Content -Raw .\infra\postgres\migrations\004_result_publication_lifecycle.sql |
  docker compose exec -T postgres psql -v ON_ERROR_STOP=1 -U atlas -d atlas

Get-Content -Raw .\infra\postgres\migrations\005_result_annualization.sql |
  docker compose exec -T postgres psql -v ON_ERROR_STOP=1 -U atlas -d atlas
Get-Content -Raw .\infra\postgres\migrations\006_scenario_worker_execution.sql |
  docker compose exec -T postgres psql -v ON_ERROR_STOP=1 -U atlas -d atlas

uv run atlas-publish-results `
  --run-path data/optimization/scenario_id=64c2ce05800db046/run_id=7ce9e36bc50405b2
```

The corrected annual run publishes 15 capacity rows, 105,120 dispatch rows, 26,280 storage rows,
30 cost rows, 11 artifact records, and four grouped provenance records. A second execution reuses
the publication and creates no duplicates.

## CI and testing

CI starts PostgreSQL, applies all migrations, and sets `ATLAS_RUN_INTEGRATION_TESTS=1`. The result
integration test forces a failure after some inserts and confirms full rollback, then verifies a
successful load, objective reconciliation, row counts, and idempotent reuse.

The original Parquet, CSV, and JSON files remain the immutable system of record for model output.
PostgreSQL is the queryable application-serving projection; a future Snowflake adapter can consume
the same validated contract without changing optimization mathematics.
