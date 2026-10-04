# Connecting the Atlas Energy stack

This guide is the source of truth for connecting the local platform. The default path is self-hosted and has no software license fee. Hosting, electricity, and optional cloud services can still cost money.

## 1. Chosen stack and substitutions

| Blueprint capability | Free default | Why it is here |
|---|---|---|
| Databricks compute | Apache Spark + Delta Lake locally | Batch/stream processing and medallion tables without a paid workspace |
| Snowflake warehouse | PostgreSQL | Governed serving tables and dbt marts without usage billing |
| Confluent Cloud | Apache Kafka in KRaft mode | Kafka-compatible event backbone without a hosted plan |
| Managed Airflow | Apache Airflow | Cross-system orchestration |
| dbt Cloud | dbt Core + dbt-postgres | Tested, documented SQL transformations |
| Managed MLflow | Self-hosted MLflow | Experiment tracking and artifact metadata |
| Commercial solver | PyPSA + HiGHS | Open energy-system modeling and linear optimization |
| Hosted API | FastAPI + Uvicorn | Typed REST/OpenAPI boundary |
| Hosted frontend | Next.js | Product UI; runs locally or on any compatible host |
| Terraform | OpenTofu | Open-source infrastructure as code |

Apache Kafka, Spark, and Airflow are Apache Software Foundation projects distributed under Apache License 2.0. PostgreSQL uses the permissive PostgreSQL License. The remaining components are community/open-source editions; check their linked license before a production redistribution.

Official references: [Apache licensing](https://www.apache.org/licenses/), [PostgreSQL licence](https://www.postgresql.org/about/licence/), [dbt Core](https://github.com/dbt-labs/dbt-core), [MLflow](https://github.com/mlflow/mlflow), [PyPSA](https://github.com/PyPSA/PyPSA), [HiGHS](https://github.com/ERGO-Code/HiGHS), [FastAPI](https://github.com/fastapi/fastapi), [Next.js](https://github.com/vercel/next.js), and [OpenTofu](https://opentofu.org/).

## 2. Connection map

```text
Public APIs/files
      |
      v
Spark ingestion -> data/bronze -> data/silver -> data/features
      ^                                      |
      |                                      v
Kafka <-> streaming consumer          PyPSA + HiGHS
      ^                                      |
      |                                      v
FastAPI -> scenario request worker -> PostgreSQL <- dbt Core
   |                                           |
   +---------------- Next.js UI <--------------+

Airflow triggers and checks the cross-system steps.
MLflow records forecasting experiments and model versions.

Python owns source acquisition, validation, checksums, and Bronze manifests. Airflow invokes those
packaged Python commands on a schedule and handles retries and dependencies; ingestion logic must
not be duplicated inside DAG files.
```

Data movement uses files/tables and stable contracts. Airflow coordinates work; it does not contain business logic. FastAPI never contains optimization mathematics, and the frontend never connects directly to PostgreSQL.

## 3. Environment variables

Copy `.env.example` to `.env`. Do not commit `.env`.

| Variable | Used by | Local value |
|---|---|---|
| `ATLAS_DATABASE_URL` | API, workers, publishing | `postgresql+psycopg://atlas:atlas@localhost:55432/atlas` |
| `ATLAS_KAFKA_BOOTSTRAP_SERVERS` | producers/consumers | `localhost:9092` |
| `ATLAS_MLFLOW_TRACKING_URI` | training code | `http://localhost:5000` |
| `ATLAS_DATA_ROOT` | ingestion/Spark/optimizer | `./data` |
| `ATLAS_API_URL` | Next.js server proxy | `http://localhost:8000` |
| `ATLAS_API_KEY` | Next.js server proxy | Set only when API service authentication is enabled |

The credentials in `.env.example` are local-development defaults only. Replace them before exposing any service outside your machine.

## 4. Start the shared services

Docker Compose starts PostgreSQL and a single-node Kafka broker:

```powershell
docker compose up -d postgres kafka
docker compose ps
```

Use `docker compose down` to stop them. Do not add `-v` unless you intentionally want to delete local database/Kafka volumes.

Connections from the host use `localhost:55432` for PostgreSQL. Connections between Compose containers use service names (`postgres:5432`, `kafka:29092`). The nonstandard host port avoids collisions with native PostgreSQL installations while the container continues using its standard internal port.

## 5. Python API and domain packages

Install the lightweight API/test foundation:

```powershell
uv sync --extra dev
uv run uvicorn atlas.api.main:app --reload
```

The API validates a scenario, canonicalizes its fields, hashes the request plus model/data versions, persists the request and run atomically with an outbox event, and returns `202 Accepted`.

Optional heavy capabilities are separate so a contributor does not need Spark and PyPSA merely to run API tests:

```powershell
uv sync --extra dev --extra data
uv pip install -r requirements/optimization.txt
uv pip install -r requirements/ml.txt
```

Spark needs a compatible Java runtime and `JAVA_HOME`. If Spark fails on a very new JDK, install an LTS JDK supported by the selected Spark release and point `JAVA_HOME` at it.

## 6. Kafka contracts

Topic names are versioned:

- `atlas.telemetry.v1`
- `atlas.scenario.requested.v1`
- `atlas.scenario.completed.v1`
- `atlas.alerts.v1`

The JSON Schema in `kafka/schemas/scenario-requested-v1.schema.json` is the source contract for submitted jobs. Producers must provide `event_id`, `event_time`, `schema_version`, `producer`, `key`, `trace_id`, and `payload`. Consumers must deduplicate by `event_id`; Kafka delivery alone does not give application-level exactly-once behavior.

The separate `atlas.telemetry.v1` stream currently accepts labeled synthetic demo events only.
`atlas-stream-telemetry` uses Spark's matching Kafka connector to preserve original Kafka bytes
and offsets in Bronze, then writes accepted and quarantined Delta tables. See
`docs/data/TELEMETRY_STREAMING.md` for setup, units, replay, and source limitations.

Scenario requests use a transactional outbox. Run `uv run atlas-outbox-publisher --once` for a single batch or `uv run atlas-outbox-publisher` as a continuous worker. The worker claims pending rows with `FOR UPDATE SKIP LOCKED`, waits for Kafka delivery acknowledgement, and only then sets `published_at`. A crash after Kafka accepts an event but before PostgreSQL commits can still produce a duplicate, so consumers must remain idempotent by `event_id`.

Run the scenario consumer in a separate terminal:

```powershell
uv sync --extra dev --extra data --extra optimization --extra analytics
uv run atlas-scenario-worker
```

It uses the stable consumer group `atlas-scenario-worker-v1`, disables automatic commits, and
atomically claims queued or retryable jobs in PostgreSQL. The processor verifies the event's model
and data versions, runs or reuses the full-year PyPSA/HiGHS result, publishes checksummed artifacts
to PostgreSQL, and refreshes dbt. Completion links `scenario_runs.optimizer_run_id` to the published
run and creates a `scenario.completed` outbox event in the same transaction. A failed attempt is
rewound in Kafka and retried up to three times; terminal and completed jobs commit their offsets.
`GET /v1/jobs/{job_id}` exposes the persisted optimization, publication, and analytics stages plus
stage-specific failures and timestamps. See `docs/api/JOB_LIFECYCLE.md`.

## 7. PostgreSQL and dbt Core

PostgreSQL is the local serving warehouse. It should contain conformed scenario dimensions, capacity/dispatch facts, KPI facts, and provenance metadata. Raw weather payloads belong in Bronze storage, not in reporting schemas.

Install dbt separately from the API environment if dependency resolution becomes heavy:

```powershell
uv tool install dbt-postgres
Copy-Item dbt_atlas/profiles.example.yml $HOME/.dbt/profiles.yml
cd dbt_atlas
dbt debug
dbt build
```

The profile reads `ATLAS_DBT_*` variables and defaults to the local Compose database. The implemented
project builds `analytics_staging`, `analytics_intermediate`, and `analytics_marts` schemas. In
production, grant the dbt role write access only to its transformation schemas; give the API
read-only access to marts plus narrowly scoped write access to job tables. See
`docs/analytics/DBT_MARTS.md` for model grains and reconciliation tests.

FastAPI exposes the marts through `/v1/results/{run_id}/kpis`, `/capacity`, `/dispatch`, `/storage`,
`/costs`, and `/provenance`, plus `/v1/comparisons`. Hourly responses are paginated and capped at
5,000 rows. See `docs/api/RESULT_ENDPOINTS.md` for filters and response semantics.

## 8. Airflow

Airflow runs in an optional Docker Compose profile with its own metadata database and an Atlas
virtual environment isolated from Airflow's Python packages. The DAG suite under `airflow/dags/`
coordinates audits, source refreshes, Silver/model-input builds, and one-shot telemetry catch-up.
See `docs/operations/AIRFLOW_ORCHESTRATION.md` for setup and schedules.

Connect Airflow through named connections/secrets, not hard-coded passwords:

- `atlas_postgres`: PostgreSQL connection.
- `atlas_kafka`: bootstrap servers and security settings.
- `atlas_mlflow`: tracking URI if training is orchestrated.

Tasks should call packaged code or APIs and pass identifiers/URIs through task metadata, not large dataframes.

## 9. Spark and Delta Lake

Spark owns heavy normalization, time alignment, deduplication, and feature construction. Keep the layers physically separate:

```text
data/bronze/<source>/ingest_date=YYYY-MM-DD/
data/silver/<dataset>/year=YYYY/month=MM/
data/features/<feature_set>/version=<hash>/
data/optimization/scenario_id=<id>/
```

Land the first immutable NASA POWER hourly source object with:

```powershell
uv run atlas-ingest-nasa-power --location ouarzazate --latitude 30.9335 --longitude -6.9370 --year 2024
```

This command validates UTC, the requested parameter set, leap-year-aware hourly counts, missing
values, and the raw-file checksum before the data is eligible for Silver transformation.

After validating one point, download the representative Morocco zone set sequentially:

```powershell
uv run atlas-ingest-nasa-power --config configs/locations/morocco.yml --year 2024
```

The location configuration is versioned in Git. Successful partial batches are safe to rerun:
existing files in the same ingestion-date partition are checksum-verified and reused.

The acquired weather window is 2020-2024 for all 11 locations and both weather providers. Rebuild
that Bronze coverage with:

```powershell
2020..2024 | ForEach-Object {
  uv run atlas-ingest-nasa-power --config configs/locations/morocco.yml --year $_
  uv run atlas-ingest-open-meteo --config configs/locations/morocco.yml --year $_
}
```

Acquire the approved non-NASA foundation sources with:

```powershell
uv run atlas-ingest-open-meteo --config configs/locations/morocco.yml --year 2024
uv run atlas-ingest-world-bank --config configs/indicators/world_bank.yml
uv run atlas-ingest-files --config configs/downloads/owid.yml
uv run atlas-ingest-files --config configs/downloads/ember.yml
uv run atlas-ingest-files --config configs/downloads/technology_data.yml
uv run atlas-audit-bronze
```

The source registry is under `configs/sources/`; direct-download asset lists are under
`configs/downloads/`. Publisher-gated IRENA files and government reports go through
`atlas-register-bronze`, preserving the original bytes and recording provenance rather than being
silently copied into the lake.

Pin Spark and Delta versions that declare compatibility with each other. Write a tiny round-trip test before ingesting large datasets. Windows-native Spark can be fragile; WSL2 or the project container is the preferred execution environment if Hadoop filesystem helpers become an issue.

Atlas keeps these heavy dependencies separate from the API environment. On this Windows machine,
select the installed JDK 21 rather than JDK 25 before starting Spark:

```powershell
uv pip install -r requirements/data.txt
$env:JAVA_HOME = "C:\Program Files\Eclipse Adoptium\jdk-21.0.7.6-hotspot"
$env:PATH = "$env:JAVA_HOME\bin;$env:PATH"
$env:PYSPARK_SUBMIT_ARGS = "--driver-memory 3g pyspark-shell"
uv run atlas-build-weather-silver --year 2024
uv run atlas-build-structured-silver
uv run atlas-build-report-silver
uv run atlas-build-zenodo-silver
```

For the complete historical weather table, run the weather builder once per year:

```powershell
2020..2024 | ForEach-Object { uv run atlas-build-weather-silver --year $_ }
```

The Silver builder verifies raw SHA-256 checksums, chooses one complete ingestion-date partition,
normalizes the four NASA parameters, rejects duplicate location-hour keys, and writes a Delta table
partitioned by `weather_year` under `data/silver/weather`.

The structured builder verifies the latest standardized Bronze partition for each source and
writes these source-aligned Delta tables:

- `data/silver/world_bank_indicators`
- `data/silver/owid_energy`
- `data/silver/ember_electricity`
- `data/silver/irena_energy`
- `data/silver/technology_costs`

Every row retains the Bronze filename, checksum, ingestion date, source identifier, and Silver
version. The build manifest under `data/silver/manifests/` records all input checksums and row
counts. Do not merge OWID, Ember, and IRENA into a single authoritative series until explicit
reconciliation rules and quality tests are approved.

Official PDF reports are represented by four additional Delta tables:

- `data/silver/report_documents` for document-level extraction coverage;
- `data/silver/report_pages` for page text, page labels, relevance tags, and OCR status;
- `data/silver/report_table_cells` for raw cells from configured, visually checked pages;
- `data/silver/report_metrics` for curated facts with PDF page, printed page, evidence, and review
  status.

The version-controlled extraction rules are in `configs/silver/reports.yml`. Image-only pages stay
marked `ocr_required` until a local OCR workflow is installed and validated.

The Morocco-specific Zenodo workbook is transformed separately with:

```powershell
uv run atlas-build-zenodo-silver
```

That command checksum-verifies the registered workbook and PyPSA technology-data inputs, then
writes six source-aligned Morocco tables plus:

- `data/silver/morocco_technology_assumption_comparison` for reviewed technology mappings and
  unit-safe comparisons;
- `data/silver/morocco_technoeconomic_data_dictionary` for field-level definitions and lineage;
- versioned JSON and Markdown manifests under `data/silver/manifests/`.

Only exact parameter/unit matches receive a numeric difference. Currency-basis and fixed-cost
basis mismatches remain visible but non-comparable. Broken workbook formulas are null with
`value_origin = formula_error`; the pipeline never guesses replacement values. The complete
contract is in `docs/data/ZENODO_MOROCCO_TECHNOECONOMIC.md`.

Create optimizer-ready renewable availability profiles with:

```powershell
uv run atlas-build-renewable-features --year 2024
uv run atlas-complete-silver
uv run atlas-build-model-inputs
```

Solar availability uses irradiance, a transparent system-loss assumption, and a simple cell
temperature correction. Wind availability extrapolates the 50 m wind speed to the configured hub
height and applies a generic cubic turbine curve. All parameters live in
`configs/features/renewable_profiles.yml`; changing an assumption changes the deterministic feature
version. These outputs are explicitly classified `SYNTHETIC` until calibrated against real Moroccan
plant production or a validated technology-specific model.

The final command writes the optimizer-facing Silver contract:

- `weather_open_meteo` and `weather_source_comparison` for the second weather provider;
- `demand_hourly`, classified `SYNTHETIC_CALIBRATED` and constrained to reviewed ONEE anchors;
- `canonical_technology_assumptions`, using PyPSA technology-data as the internally consistent
  generic assumption basis while keeping Zenodo as comparison evidence;
- `energy_statistics_reconciliation`, retaining both publisher values and their difference rather
  than silently merging them;
- `optimizer_hourly_inputs`, joining national demand with equal-weight representative-location
  solar and wind availability.

The table contract, calibration checks, and limitations are documented in
`docs/data/SILVER_COMPLETION.md`. Kafka telemetry Silver remains part of the separate streaming
phase; it is not an input dependency for the first PyPSA baseline.

## 10. MLflow

For a single developer, start with a local file-backed server:

```powershell
uv pip install -r requirements/ml.txt
uv run mlflow server --host 127.0.0.1 --port 5000
```

Log the training date range, feature version, dataset checksums, parameters, metrics, code commit, and model signature. Do not register a model merely because training completed; require a time-based validation gate against persistence and weekly-seasonal baselines.

## 11. Optimization

PyPSA builds the network model; HiGHS solves the linear program. Install the isolated dependency
group and run the validation sequence with:

```powershell
uv sync --extra dev --extra optimization
uv run atlas-optimize --mode toy
uv run atlas-optimize --mode week --calendar planning
uv run atlas-optimize --mode full --calendar planning
```

The planning calendar removes 29 February from the 2024 calibration series and maps the remaining
8,760 values to the planning year. `--calendar baseline` instead solves all 8,784 source hours.
The optimizer reads the latest optimizer-ready package by default; pin one explicitly with
`--input-package data/model_inputs/version=<version>` when reproducing a published run.

Each run persists under `data/optimization/scenario_id=<id>/run_id=<id>/`:

- canonical scenario request and deterministic hash;
- input dataset/version IDs;
- code/model/solver versions;
- solver status, termination condition, objective, and validation gates;
- capacity, hourly generator dispatch, hourly storage/SOC, hourly energy balance, annual asset
  summaries, KPIs, file row counts, and SHA-256 checksums in Parquet/CSV/JSON.

The implemented gate order is a hand-checkable 24-hour case, a 168-hour integration case, and then
the annual solve. The Kafka scenario worker now invokes this same tested Python boundary, then uses
the existing atomic publisher and dbt contracts; it does not duplicate optimization mathematics.

Publish a completed run manually only after applying migrations 003 through 007:

```powershell
uv run atlas-publish-results `
  --run-path data/optimization/scenario_id=64c2ce05800db046/run_id=7ce9e36bc50405b2
```

PostgreSQL stores the queryable serving projection; the checksummed Parquet/CSV/JSON files remain
immutable model outputs. Publication is atomic and idempotent. A future Snowflake adapter should
consume this same validated contract rather than replacing PostgreSQL in local development or
changing optimizer logic. See `docs/optimization/RESULT_PUBLICATION.md`.

## 12. Frontend

The browser speaks only to FastAPI:

```powershell
cd frontend
npm.cmd install
npm.cmd run dev
```

`NEXT_PUBLIC_ATLAS_API_URL` is compiled into client-side code, so it is not a secret. Never put database credentials or private tokens in a `NEXT_PUBLIC_*` variable.

## 13. Free does not mean unlimited

- All default software can run without a license fee.
- Docker Desktop is free only under Docker's current personal/education/small-business terms; use Docker Engine on Linux or Podman if those terms do not fit your organization.
- GitHub Actions and commercial deployment hosts may have quotas; local CI is always available.
- Public data APIs can impose rate limits and terms of use. Cache raw responses and record retrieval metadata.
- A future Databricks, Snowflake, AWS, Azure, GCP, or managed Kafka adapter is optional and may incur charges.

## 14. Recommended connection order

1. Run the API and tests without infrastructure.
2. Start PostgreSQL and persist scenario/job metadata.
3. Add the Kafka producer/worker path with idempotency tests.
4. Land one public weather source in Bronze and transform it to Silver.
5. Build the toy and one-week optimizer before the full-year model.
6. Publish validated results to PostgreSQL and build dbt marts.
7. Expose the published results through FastAPI and connect the UI to those endpoints.
8. Add justified MLflow work after the underlying forecasting commands work independently.

This order keeps every intermediate stage demonstrable and avoids installing tools only for appearance.
