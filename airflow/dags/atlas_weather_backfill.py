"""Explicit historical weather refresh for one of the approved 2020-2024 years."""

from datetime import UTC, datetime, timedelta

from airflow.sdk import Param, dag
from atlas_tasks import run_atlas

DEFAULT_ARGS = {"retries": 2, "retry_delay": timedelta(minutes=5)}


@dag(
    dag_id="atlas_weather_backfill",
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    params={"weather_year": Param(2024, type="integer", minimum=2020, maximum=2024)},
    tags=["atlas", "weather", "bronze", "silver", "manual"],
)
def atlas_weather_backfill():
    nasa = run_atlas.override(task_id="ingest_nasa_power")(
        "atlas-ingest-nasa-power", ["--config", "configs/locations/morocco.yml"], True
    )
    open_meteo = run_atlas.override(task_id="ingest_open_meteo")(
        "atlas-ingest-open-meteo", ["--config", "configs/locations/morocco.yml"], True
    )
    audit = run_atlas.override(task_id="audit_bronze")("atlas-audit-bronze", [])
    silver = run_atlas.override(task_id="build_weather_silver")(
        "atlas-build-weather-silver", [], True
    )
    [nasa, open_meteo] >> audit >> silver


atlas_weather_backfill()
