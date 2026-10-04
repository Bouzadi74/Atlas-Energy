"""Monthly public structured-source refresh and source-aligned Silver build."""

from datetime import UTC, datetime, timedelta

from airflow.sdk import dag
from atlas_tasks import run_atlas


@dag(
    dag_id="atlas_structured_refresh",
    schedule="0 4 1 * *",
    start_date=datetime(2026, 10, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=15)},
    tags=["atlas", "public-sources", "bronze", "silver"],
)
def atlas_structured_refresh():
    world_bank = run_atlas.override(task_id="ingest_world_bank")(
        "atlas-ingest-world-bank", ["--config", "configs/indicators/world_bank.yml"]
    )
    owid = run_atlas.override(task_id="ingest_owid")(
        "atlas-ingest-files", ["--config", "configs/downloads/owid.yml"]
    )
    ember = run_atlas.override(task_id="ingest_ember")(
        "atlas-ingest-files", ["--config", "configs/downloads/ember.yml"]
    )
    technology = run_atlas.override(task_id="ingest_technology_data")(
        "atlas-ingest-files", ["--config", "configs/downloads/technology_data.yml"]
    )
    audit = run_atlas.override(task_id="audit_bronze")("atlas-audit-bronze", [])
    silver = run_atlas.override(task_id="build_structured_silver")(
        "atlas-build-structured-silver", []
    )
    [world_bank, owid, ember, technology] >> audit >> silver


atlas_structured_refresh()
