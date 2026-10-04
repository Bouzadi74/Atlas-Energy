# Asynchronous scenario job lifecycle

`POST /v1/scenarios` is the non-blocking write boundary. It validates the scenario, combines the
canonical request with the configured model and data versions, computes a deterministic SHA-256
hash, and atomically stores the request, queued job, and `scenario.requested` outbox event. The
response is `202 Accepted`; FastAPI never runs PyPSA inside the request process.

Poll `GET /v1/jobs/{job_id}` for execution state. `GET /v1/runs/{scenario_id}` remains available
for compatibility and exposes the same stage fields from the scenario perspective.

## Status and stage

`status` is the stable high-level state used by existing clients:

- `queued`: accepted but not claimed, or waiting for a permitted retry;
- `running`: a worker owns or is recovering the job;
- `completed`: results and analytics are queryable;
- `failed`: all permitted attempts are exhausted.

`stage` provides operational detail:

```text
queued
  -> optimizing
  -> optimized
  -> publishing
  -> published
  -> building_analytics
  -> analytics_ready
  -> completed
```

The worker persists a transition before crossing each external boundary. A failure records
`failure_stage`, `error_type`, `error_message`, `failed_at`, the attempt number, and the latest
successful stage timestamps. A retry re-enters `optimizing`; deterministic optimizer artifacts,
idempotent result publication, and dbt rebuilds make replay safe.

## Completion guarantee

A job cannot transition to `completed` unless it has reached `analytics_ready`. Completion links
the job to `optimizer_run_id` and creates `scenario.completed` in the PostgreSQL outbox in the same
transaction. The completion event is published by the continuously running outbox publisher.

Run these long-lived processes separately:

```powershell
uv run uvicorn atlas.api.main:app --reload
uv run atlas-outbox-publisher
uv run atlas-scenario-worker
```

Example submission and polling:

```powershell
$scenario = @{
  name = "API Baseline 2030"
  planning_year = 2030
  weather_year = 2024
  renewable_generation_min = 0.50
} | ConvertTo-Json
$accepted = Invoke-RestMethod `
  -Uri "http://localhost:8000/v1/scenarios" `
  -Method Post `
  -ContentType "application/json" `
  -Body $scenario

Invoke-RestMethod "http://localhost:8000/v1/jobs/$($accepted.job_id)"
```

Identical completed requests return the original deterministic scenario/job with `reused=true`
instead of launching duplicate compute.
