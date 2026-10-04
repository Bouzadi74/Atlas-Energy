"""Rebuild reviewed report and Zenodo Silver after manual Bronze registration."""

from datetime import UTC, datetime, timedelta

from airflow.sdk import dag
from atlas_tasks import run_atlas


@dag(
    dag_id="atlas_report_refresh",
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=10)},
    tags=["atlas", "reports", "zenodo", "silver", "manual"],
)
def atlas_report_refresh():
    audit = run_atlas.override(task_id="audit_bronze")("atlas-audit-bronze", [])
    reports = run_atlas.override(task_id="build_report_silver")(
        "atlas-build-report-silver", []
    )
    zenodo = run_atlas.override(task_id="build_zenodo_silver")(
        "atlas-build-zenodo-silver", []
    )
    audit >> [reports, zenodo]


atlas_report_refresh()
