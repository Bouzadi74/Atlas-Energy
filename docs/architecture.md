# Architecture

Atlas uses stable boundaries so local open-source services can later be replaced by managed services without rewriting domain logic.

1. Public API/file ingestors preserve raw responses and manifests in Bronze storage.
2. Spark validates, normalizes, deduplicates, and writes Silver/feature data.
3. Kafka carries telemetry and scenario lifecycle events, not historical batch files.
4. PyPSA/HiGHS reads versioned inputs and writes run-scoped technical outputs.
5. PostgreSQL serves conformed facts and dimensions; dbt owns business-facing marts.
6. FastAPI exposes controlled scenario and result contracts to Next.js.
7. Airflow coordinates cross-system tasks; MLflow tracks forecasting experiments.

Local directories emulate the lakehouse first. Production object storage is an adapter decision, not a prerequisite.

