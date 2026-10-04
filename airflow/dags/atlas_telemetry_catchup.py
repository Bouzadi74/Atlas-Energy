"""Manual catch-up for the currently synthetic Kafka telemetry contract."""

from datetime import UTC, datetime

from airflow.sdk import dag
from atlas_tasks import run_atlas


@dag(
    dag_id="atlas_telemetry_catchup",
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    tags=["atlas", "kafka", "telemetry", "manual"],
)
def atlas_telemetry_catchup():
    run_atlas.override(task_id="stream_available_telemetry")(
        "atlas-stream-telemetry", ["--once"]
    )


atlas_telemetry_catchup()
