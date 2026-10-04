# Local Airflow orchestration

Atlas uses Airflow to order and retry existing packaged commands. Transformation and source
parsing stay in `src/atlas`; DAG tasks pass only identifiers and config paths, never dataframes or
PDF bytes. The optional Compose profile uses the official Airflow 3.3.1 image, a separate
PostgreSQL metadata database, LocalExecutor, and a locked Atlas data environment with Java 17 for
Spark. It is intended for local academic demonstrations.

## Start

Airflow is optional and consumes more memory than the API and broker. From the repository root:

```powershell
# Set ATLAS_AIRFLOW_DB_PASSWORD in .env to a distinct local secret first.
docker compose --profile airflow up -d --build airflow-db airflow
docker compose --profile airflow ps
docker compose exec airflow airflow dags list-import-errors
```

The first image build downloads the pinned PySpark source archive (about 450 MB) and may take a
while on a slower connection. Use an alphanumeric local metadata-database password in `.env`
because Compose inserts it into a database URL.

Open <http://localhost:8080>. The `standalone` command creates a local admin password on first
startup. Retrieve it locally with:

```powershell
docker compose exec airflow cat /opt/airflow/simple_auth_manager_passwords.json.generated
```

The DAGs are paused when first created. Enable only the schedules you intend to run; manual DAGs
remain triggerable from the UI while paused. To stop the services without removing their volumes:

```powershell
docker compose --profile airflow stop airflow airflow-db
```

The image builds Atlas from `uv.lock` into a separate virtual environment, so the Airflow Python
environment is not changed by PySpark, Delta, or the source connectors. The repository `data/`
directory is mounted at `/opt/atlas/data`. DAG files are mounted read-only for local iteration,
while application code is included in the image. Rebuild the image after changing `src/atlas`,
dependencies, or configuration copied into the image.

## DAGs

- `atlas_bronze_audit`: daily at 05:00 UTC, paused initially. Runs the checksum and source audit.
- `atlas_structured_refresh`: monthly on day 1 at 04:00 UTC, paused initially. Downloads World
  Bank, OWID, Ember, and pinned technology data; audits Bronze; builds structured Silver. IRENA
  remains manually acquired and must already be registered when the Silver builder runs.
- `atlas_weather_backfill`: manual. Select `weather_year` from 2020-2024. Downloads the eleven
  configured NASA POWER and Open-Meteo locations, audits Bronze, and rebuilds only that NASA
  weather Silver partition. It does not imply full grid coverage.
- `atlas_report_refresh`: manual after untouched ONEE/ANRE/MEF and Zenodo files are registered in
  Bronze. Audits sources and builds the report and Zenodo Silver tables.
- `atlas_model_input_refresh`: manual. Audits existing Bronze, rebuilds the 2024 weather,
  structured, report, and Zenodo Silver inputs, renewable features, completed Silver, and a
  checksummed model-input package. The historical 2020-2023 weather partitions must already exist.
- `atlas_telemetry_catchup`: manual. Runs one Kafka-to-Delta catch-up for the currently synthetic
  telemetry contract. Start the Kafka service separately before triggering it.

All DAGs cap active runs at one and use bounded retries. The local Airflow executor runs one task
at a time globally to avoid concurrent writes to shared Delta tables. A task failure stops its
downstream work; upstream Bronze objects remain immutable. Failed tasks can be retried from
Airflow after the source or environment problem is corrected.

The scheduled DAGs are optional. The model-input rebuild is not on a timer because it consumes
substantial Spark resources and changes a governed package only after source review.

## Checks and limitations

`airflow dags list-import-errors` must return no errors. Test a low-cost task before enabling
downloads or Spark work:

```powershell
docker compose exec airflow airflow dags test atlas_bronze_audit 2026-10-03
```

This local smoke test passed with 135 valid Bronze objects, zero errors, and 12 existing legacy
NASA-manifest warnings. A `graphviz` warning from the Airflow CLI affects graph rendering only.

The Airflow metadata database is separate from Atlas's PostgreSQL serving database. Kafka events
and the scenario worker remain independent services; an indefinitely running worker is not an
Airflow task. This profile has no distributed workers, secret manager, alert routing, or cloud
deployment. Replace local demo credentials and add proper role boundaries before shared use.
