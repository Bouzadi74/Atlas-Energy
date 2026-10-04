"""Manual full Bronze-to-model-input rebuild from already acquired source objects."""

from datetime import UTC, datetime, timedelta

from airflow.sdk import dag
from atlas_tasks import run_atlas


@dag(
    dag_id="atlas_model_input_refresh",
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=10)},
    tags=["atlas", "silver", "features", "model-inputs", "manual"],
)
def atlas_model_input_refresh():
    audit = run_atlas.override(task_id="audit_bronze")("atlas-audit-bronze", [])
    weather = run_atlas.override(task_id="build_weather_silver_2024")(
        "atlas-build-weather-silver", ["--year", "2024"]
    )
    structured = run_atlas.override(task_id="build_structured_silver")(
        "atlas-build-structured-silver", []
    )
    reports = run_atlas.override(task_id="build_report_silver")(
        "atlas-build-report-silver", []
    )
    zenodo = run_atlas.override(task_id="build_zenodo_silver")(
        "atlas-build-zenodo-silver", []
    )
    features = run_atlas.override(task_id="build_renewable_features")(
        "atlas-build-renewable-features", ["--year", "2024"]
    )
    completed = run_atlas.override(task_id="complete_silver")("atlas-complete-silver", [])
    package = run_atlas.override(task_id="build_model_inputs")("atlas-build-model-inputs", [])

    audit >> [weather, structured, reports, zenodo]
    weather >> features
    [features, structured, reports, zenodo] >> completed >> package


atlas_model_input_refresh()
