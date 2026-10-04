"""Daily integrity check for all registered Bronze objects."""

from datetime import UTC, datetime, timedelta

from airflow.sdk import dag
from atlas_tasks import run_atlas


@dag(
    dag_id="atlas_bronze_audit",
    schedule="0 5 * * *",
    start_date=datetime(2026, 10, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["atlas", "bronze", "quality"],
)
def atlas_bronze_audit():
    run_atlas.override(task_id="audit_bronze")("atlas-audit-bronze", [])


atlas_bronze_audit()
