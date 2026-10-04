# Result and provenance API

FastAPI serves read-only, typed projections of the tested `analytics_marts` dbt models. It does
not query lakehouse files, recalculate KPIs, or contain optimization mathematics. Obtain the
optimizer `run_id` from `GET /v1/runs/{scenario_id}` after the asynchronous job completes.

For API-submitted jobs, the result mart's `scenario_id` is the application's `scn_...` ID and
matches the job and scenario endpoints. The optimizer's separately derived scenario identifier
remains in `optimization.optimization_runs.scenario_id` and is also exposed as
`analytics_marts.dim_scenario.optimizer_scenario_id`. Standalone optimizer runs without an API
job retain their optimizer-derived ID in the result marts.
An API job that exhausts retries after optimizer publication is excluded from current KPI, fact,
comparison, and provenance marts on the next dbt build. Its dimension row is marked non-current;
the immutable optimizer artifact and PostgreSQL publication remain available for diagnosis.

## Endpoints

- `GET /v1/results/{run_id}/kpis`: executive cost, energy, emissions, reliability, and version KPIs;
- `GET /v1/results/{run_id}/capacity`: existing, new, and optimized capacity by asset;
- `GET /v1/results/{run_id}/dispatch`: hourly generator/import/reliability dispatch;
- `GET /v1/results/{run_id}/storage`: hourly storage net dispatch and state of charge;
- `GET /v1/results/{run_id}/costs`: annual capital and operating cost components;
- `GET /v1/results/{run_id}/provenance`: input, software, validation, and artifact contracts;
- `GET /v1/comparisons`: all canonical current-scenario comparisons;
- `GET /v1/comparisons/{base_run_id}/{comparison_run_id}`: one comparison oriented to the
  requested base, including correctly signed deltas when the stored canonical order is reversed.

Unknown or non-current run IDs return `404`. Comparison endpoints expose only pairs materialized by
dbt from current, published annual results.

## Hourly query contract

Dispatch and storage accept optional ISO-8601 `start` and `end` parameters, `asset`, `carrier`,
`limit`, and `offset`. `start` is inclusive and `end` is exclusive. Naive timestamps are treated
as UTC; timezone-aware values are normalized to UTC. `limit` defaults to 1,000 and cannot exceed
5,000. Responses include `returned` and `has_more` so clients do not accidentally retrieve an
entire annual fact table in one request.

Example:

```powershell
$runId = "7ce9e36bc50405b2"
Invoke-RestMethod "http://localhost:8000/v1/results/$runId/kpis"
Invoke-RestMethod "http://localhost:8000/v1/results/$runId/dispatch?carrier=solar&limit=168"
Invoke-RestMethod "http://localhost:8000/v1/results/$runId/provenance"
```

The OpenAPI explorer at `http://localhost:8000/docs` contains the complete response schemas.

## Serving boundary

The API process currently uses the same PostgreSQL credentials as other local services. A deployed
environment should give its identity read-only access to `analytics_marts`, plus only the narrow
scenario/job writes required by submission. PostgreSQL is the implemented local serving warehouse;
the response contracts remain portable to a future Snowflake-backed repository adapter.
