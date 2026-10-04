# Atlas Energy

For a concise public repository overview, see [GITHUB.md](GITHUB.md).

Atlas Energy is a reproducible energy-transition decision platform for Morocco. The MVP turns public and transparently synthetic-calibrated data into hourly demand and renewable profiles, capacity/dispatch optimization scenarios, governed analytical outputs, and a typed API.

The architecture follows the supplied enterprise blueprint, with one deliberate change: the default implementation uses only zero-license-cost local components. Paid cloud services remain optional future adapters, not runtime requirements.

## Current foundation

- FastAPI service with scenario lifecycle plus KPI, capacity, dispatch, storage, cost, provenance,
  and comparison result contracts backed by dbt marts.
- PostgreSQL-backed scenario persistence with deterministic idempotency.
- Transactional outbox records ready for Kafka publication.
- Kafka outbox publisher with delivery acknowledgements and persisted retry state.
- Kafka scenario worker connected to PyPSA, atomic result publication, and dbt refresh.
- Granular asynchronous job lifecycle and `GET /v1/jobs/{job_id}` polling contract.
- Five versioned academic policy, cost, and demand scenario presets exposed through the API.
- Responsive Next.js Scenario Builder, executive KPI/capacity/cost dashboard, governed comparison
  view, bounded hourly dispatch/storage explorer, and model-transparency view.
- Opt-in read/write API keys, server-side frontend proxying, request IDs, structured request logs,
  database readiness checks, Prometheus metrics, and a provisioned local Grafana dashboard.
- Versioned Spark/Delta weather and renewable capacity-factor pipelines.
- Synthetic Kafka telemetry capture with replayable Bronze bytes, validated Delta Silver events,
  event-time deduplication, and quarantine for invalid messages.
- Reproducible PyPSA/HiGHS national-node optimization with toy, week, and annual modes.
- Pydantic validation for the blueprint's scenario parameters.
- PostgreSQL and Apache Kafka local services through Docker Compose.
- Versioned Kafka event schemas, local Airflow DAG suite, tests, and CI.
- Versioned scenario catalog and documentation for provenance, assumptions, limitations, architecture, and integrations.
- Machine-readable registry for eleven approved public sources plus immutable Bronze manifests and an integrity audit.

## Quick start (Windows PowerShell)

Prerequisites: Python 3.12 or 3.13, `uv`, Node.js 20+, Docker Compose, and Git.

```powershell
Copy-Item .env.example .env
uv sync --extra dev
docker compose up -d postgres kafka
uv run uvicorn atlas.api.main:app --reload
```

Open:

- API health: <http://localhost:8000/health>
- API readiness: <http://localhost:8000/ready>
- API documentation: <http://localhost:8000/docs>

Run checks:

```powershell
uv run ruff check .
uv run pytest
```

Run the PostgreSQL integration test after applying the migration:

```powershell
$env:ATLAS_RUN_INTEGRATION_TESTS='1'
uv run pytest tests/integration/test_postgres_repository.py
Remove-Item Env:ATLAS_RUN_INTEGRATION_TESTS
```

Publish one pending outbox batch:

```powershell
uv run atlas-outbox-publisher --once
```

Run the publisher continuously until stopped with `Ctrl+C`:

```powershell
uv run atlas-outbox-publisher
```

In another terminal, run the scenario worker:

```powershell
uv sync --extra dev --extra data --extra optimization --extra analytics
uv run atlas-scenario-worker
```

The worker validates the requested model/data versions, runs or reuses the deterministic full-year
PyPSA result, publishes it atomically to PostgreSQL, refreshes dbt marts, links the application job
to the optimizer run, and emits `scenario.completed` through the transactional outbox. Failed work
is retried at most three times before its Kafka offset is committed as a terminal failure.

Download and validate one year of hourly NASA POWER weather data into the Bronze layer:

```powershell
uv run atlas-ingest-nasa-power --location ouarzazate --latitude 30.9335 --longitude -6.9370 --year 2024
```

The command preserves the raw JSON, writes provenance metadata and a SHA-256 checksum, validates
the expected number of hours, reports missing values, and safely reuses an identical existing file.

Download the versioned set of representative Morocco energy zones sequentially:

```powershell
uv run atlas-ingest-nasa-power --config configs/locations/morocco.yml --year 2024
```

The configuration is a zonal modeling approximation rather than an administrative-boundary map.
The project currently holds complete 2020-2024 hourly coverage for all 11 configured locations
from both NASA POWER and Open-Meteo. To reproduce the historical backfill:

```powershell
2020..2024 | ForEach-Object {
  uv run atlas-ingest-nasa-power --config configs/locations/morocco.yml --year $_
  uv run atlas-ingest-open-meteo --config configs/locations/morocco.yml --year $_
}
```

Build out the wider raw-data foundation before further optimization work:

```powershell
uv run atlas-ingest-open-meteo --config configs/locations/morocco.yml --year 2024
uv run atlas-ingest-world-bank --config configs/indicators/world_bank.yml
uv run atlas-ingest-files --config configs/downloads/owid.yml
uv run atlas-ingest-files --config configs/downloads/ember.yml
uv run atlas-ingest-files --config configs/downloads/technology_data.yml
uv run atlas-audit-bronze
```

Source access, licensing, coverage, and Bronze locations are defined under `configs/sources/`.
See `docs/data/SOURCE_CATALOG.md` before adding or transforming a source.

Build the conformed hourly Silver Delta table after installing the separate data environment:

```powershell
uv sync --extra dev --extra data
$env:JAVA_HOME = "C:\Program Files\Eclipse Adoptium\jdk-21.0.7.6-hotspot"
$env:PATH = "$env:JAVA_HOME\bin;$env:PATH"
$env:PYSPARK_SUBMIT_ARGS = "--driver-memory 3g pyspark-shell"
uv run atlas-build-weather-silver --year 2024
uv run atlas-build-structured-silver
uv run atlas-build-report-silver
uv run atlas-build-zenodo-silver
```

Build or refresh all historical weather partitions with:

```powershell
2020..2024 | ForEach-Object { uv run atlas-build-weather-silver --year $_ }
```

The command selects the latest complete Bronze partition, verifies every checksum, enforces the
expected location-hour keys and writes `data/silver/weather` as a partitioned Delta table.
The structured builder independently preserves World Bank, OWID, Ember, IRENA, and PyPSA
technology-data semantics in five source-aligned Delta tables. It writes a deterministic build
manifest under `data/silver/manifests/`; overlapping statistics remain source-aligned and are
reconciled explicitly by `atlas-complete-silver` rather than silently merged.
The report builder indexes every ONEE, ANRE, and MEF PDF page, extracts configured table cells,
and publishes only visually reviewed facts to `data/silver/report_metrics`. See
`docs/data/PDF_EXTRACTION.md` for the provenance and OCR policy.

The Zenodo builder converts the Morocco-specific techno-economic workbook into six source-aligned
Delta tables, a cross-source comparison with PyPSA technology-data, and a machine-readable data
dictionary. It compares numbers only when their parameter and unit bases match; USD/EUR and fixed
cost/percentage bases are retained as explicit mismatches. See
`docs/data/ZENODO_MOROCCO_TECHNOECONOMIC.md` for the table contract and known source issues.

Build versioned solar and wind availability features from Silver:

```powershell
$env:JAVA_HOME = "C:\Program Files\Eclipse Adoptium\jdk-21.0.7.6-hotspot"
$env:PATH = "$env:JAVA_HOME\bin;$env:PATH"
uv run atlas-build-renewable-features --year 2024
uv run atlas-complete-silver
```

The free `pvlib` and `windpowerlib` libraries derive PVWatts/SAPM solar and governed turbine-curve
wind availability inside the PySpark feature job. The engineering assumptions are versioned in
`configs/features/renewable_profiles.yml`. Outputs are written under
`data/features/renewable_capacity_factors/version=<hash>/`, include a manifest, and are labeled
`SYNTHETIC`; they are not represented as measured plant generation.

`atlas-complete-silver` closes the batch/model-input Silver milestone. It builds source-aligned
Open-Meteo weather, NASA/Open-Meteo comparisons, a 2024 national demand profile calibrated to
reviewed ONEE annual/peak/day anchors, canonical PyPSA technology assumptions, source-statistics
reconciliation, and the 8,784-hour leap-year optimizer input bundle. See
`docs/data/SILVER_COMPLETION.md` for the contracts and current row counts.

Publish a validated package that the energy-model network builder can consume directly:

```powershell
uv run atlas-build-model-inputs
```

The package contains the complete 8,784-hour 2024 calibration series, a documented 8,760-hour
non-leap planning representation, reconciled 2024 on-grid installed capacity, canonical 2030
technology assumptions, Parquet/CSV outputs, checksums, provenance, and validation results. The
current version is `0e5327a2df73e087`. See
`docs/data/MODEL_INPUT_PACKAGE.md` for the calendar and IRENA/ONEE reconciliation rules.

Run the optimization progression after installing the separate optimization environment:

```powershell
uv sync --extra dev --extra optimization
uv run atlas-optimize --mode toy
uv run atlas-optimize --mode week --calendar planning
uv run atlas-optimize --mode full --calendar planning
```

The first command is an exact 24-hour check. The second annualizes a 168-hour integration case.
The third solves the 8,760-hour 2030 planning representation using 2024 weather/profile shapes.
Use `--calendar baseline` to retain all 8,784 hours of the leap-year calibration. Run-scoped,
checksummed capacity, dispatch, storage, balance, annual-summary, KPI, and validation outputs are
written below `data/optimization/`. See `docs/optimization/BASELINE.md` for the model contract,
verified baseline, and limitations.

Publish a validated run to PostgreSQL without modifying its immutable artifacts:

```powershell
Get-Content -Raw .\infra\postgres\migrations\003_optimization_results.sql |
  docker compose exec -T postgres psql -v ON_ERROR_STOP=1 -U atlas -d atlas
Get-Content -Raw .\infra\postgres\migrations\004_result_publication_lifecycle.sql |
  docker compose exec -T postgres psql -v ON_ERROR_STOP=1 -U atlas -d atlas
Get-Content -Raw .\infra\postgres\migrations\005_result_annualization.sql |
  docker compose exec -T postgres psql -v ON_ERROR_STOP=1 -U atlas -d atlas
Get-Content -Raw .\infra\postgres\migrations\006_scenario_worker_execution.sql |
  docker compose exec -T postgres psql -v ON_ERROR_STOP=1 -U atlas -d atlas
Get-Content -Raw .\infra\postgres\migrations\007_job_lifecycle.sql |
  docker compose exec -T postgres psql -v ON_ERROR_STOP=1 -U atlas -d atlas
uv run atlas-publish-results `
  --run-path data/optimization/scenario_id=64c2ce05800db046/run_id=7ce9e36bc50405b2
```

The publisher verifies checksums, schemas, validation gates, hourly balance, costs, and emissions
before atomically inserting the result. Repeating the command is idempotent. See
`docs/optimization/RESULT_PUBLICATION.md`.

Build the PostgreSQL analytical models after publishing at least one run:

```powershell
uv sync --extra dev --extra analytics
Copy-Item dbt_atlas/profiles.example.yml dbt_atlas/profiles.yml
uv run dbt debug --project-dir dbt_atlas --profiles-dir dbt_atlas
uv run dbt build --project-dir dbt_atlas --profiles-dir dbt_atlas
```

The current project builds staging and intermediate views plus scenario, KPI, capacity, dispatch,
storage, cost, provenance, and comparison marts. Result routes are documented in
`docs/api/RESULT_ENDPOINTS.md` and in the live OpenAPI explorer at <http://localhost:8000/docs>.
Scenario submission and job polling are documented in `docs/api/JOB_LIFECYCLE.md`.

Start the frontend in a second terminal:

```powershell
cd frontend
npm.cmd install
npm.cmd run dev
```

Open <http://localhost:3000>.

For the repeatable desktop/mobile browser walkthrough, including the optional reused-preset
submission check, see `docs/operations/MVP_ACCEPTANCE.md`.

The browser reads scenario presets from `GET /v1/scenario-presets`, submits jobs through
`POST /v1/scenarios`, polls `GET /v1/jobs/{job_id}`, and displays only results published through
the PostgreSQL/dbt serving contract. Keep the API, outbox publisher, and scenario worker running
in their own terminals to execute a newly submitted scenario end to end. See
`docs/product/SCENARIO_DASHBOARD.md`. For a repeatable end-to-end readiness check, follow
`docs/operations/MVP_ACCEPTANCE.md` after starting all application services.

The Hourly Operations view requests one UTC day at a time from the capped dispatch and storage
endpoints. It never downloads an annual result into the browser and labels every chart as modeled,
synthetic-backed output rather than observed system operation.

Enable service-level API authentication and start the optional local monitoring stack with:

```powershell
# Put distinct generated API keys and a Grafana password in .env first.
docker compose --profile observability up -d prometheus grafana
uv run uvicorn atlas.api.main:app --host 0.0.0.0 --port 8000
```

Prometheus is available at <http://localhost:9090> and Grafana at <http://localhost:3001>. The
frontend forwards API calls through a server-only proxy, so protected credentials are not shipped
to the browser. See `docs/operations/SECURITY_AND_OBSERVABILITY.md` for configuration, endpoint,
network-exposure, and production limitations.

## Repository map

```text
airflow/dags/           Cross-system orchestration
configs/scenarios/      Versioned scenario assumptions
data/                   Local Bronze/Silver/features/results (ignored)
dbt_atlas/              Business-facing SQL models and tests
docs/                   Architecture, data, assumptions, limitations
frontend/               Next.js product shell
infra/opentofu/          Free/open-source infrastructure-as-code entrypoint
kafka/schemas/          Versioned event contracts
src/atlas/              Packaged application and domain logic
tests/                  Unit and API contract tests
```

## Build order

1. Foundation and contracts (complete for the current MVP scope).
2. Public-source ingestion and replayable Bronze data (complete for the current source registry).
3. Batch Spark/Delta Silver tables and quality gates (complete).
4. Synthetic Kafka telemetry path and quarantine handling (complete for the demo contract).
5. Versioned 8,784/8,760-hour optimizer input package with capacity and costs (complete).
6. PyPSA/HiGHS optimization and validation (complete for CLI baseline).
7. Persist optimization results to PostgreSQL (complete).
8. Build dbt staging/intermediate/business result marts (complete).
9. Connect the optimizer and publisher to the asynchronous worker (complete).
10. Add result/provenance APIs (complete).
11. Add additional policy/cost/demand scenarios and the executive UI (complete).
12. Add hourly dispatch and storage exploration (complete).
13. Add API service authentication and operational observability (complete for the local MVP).
14. Add complete Airflow orchestration and justified MLflow work (next).

The telemetry streaming path can be exercised with labeled synthetic demo events. It is separate
from the historical optimizer inputs; see `docs/data/TELEMETRY_STREAMING.md` for the contract and
local commands.

The optional Airflow profile coordinates source refreshes, Bronze audits, Silver builds, model
input packages, and one-shot telemetry catch-up. Start it with
`docker compose --profile airflow up -d --build airflow-db airflow`; see
`docs/operations/AIRFLOW_ORCHESTRATION.md` for DAG schedules, first login, and task checks.

See [CONNECTING_TECHNOLOGIES.md](CONNECTING_TECHNOLOGIES.md) before installing or connecting the remaining services.

## Credibility rule

Never label generated hourly data as observed. Every dataset and result must carry one of: `OBSERVED`, `REANALYSIS`, `DERIVED`, `ASSUMPTION`, `SYNTHETIC_CALIBRATED`, or `SYNTHETIC`.

Atlas Energy is an independent portfolio project and is not affiliated with ONEE, ANRE, BCG, or the Moroccan government.
