# Scenario Builder and executive dashboard

Atlas exposes its version-controlled scenario catalog at `GET /v1/scenario-presets`. The endpoint
validates every YAML document under `configs/scenarios/` and returns the typed request together
with its `ASSUMPTION` classification, version, and academic-use note. The browser therefore does
not keep a second copy of scenario values.

## Product flow

The Scenario Builder starts from a governed preset or an editable custom request. Submitting the
form calls `POST /v1/scenarios` and immediately receives the deterministic scenario and job IDs.
The interface polls `GET /v1/jobs/{job_id}` through queued, optimization, publication, analytics,
and terminal stages. Re-submitting an identical request safely reuses the existing job.

The executive overview reads only published dbt-backed contracts:

- KPIs for annual cost, renewable share, emissions, imports, and reliability;
- existing and new optimized capacity by technology;
- annual cost components reconciled to the optimization objective;
- pairwise scenario deltas from the comparison mart;
- run, input-package, and model provenance.

The Hourly Operations view uses the same governed serving boundary. It requests a single UTC day
from the capped dispatch and storage endpoints, aggregates dispatch assets by carrier, and shows a
modeled supply stack, storage state of charge, daily summaries, and selected hourly checkpoints.
The browser rejects an unexpectedly paginated daily response instead of silently showing a partial
chart. Dates are constrained to the selected scenario's planning year.

## Interface design

The browser uses a dark grid-operations visual language: a compact navigation rail, high-contrast
lime status/action cues, restrained coral alerts, and monospace technical labels. The five views
and their API contracts are unchanged. Navigation exposes the active page; preset choices expose
their pressed state. On a narrow screen the rail becomes a labeled bottom navigation bar. When
the API is unavailable, the overview offers a retry action and the Builder disables submission
instead of implying an offline request can run. The icons use the free ISC-licensed Lucide React
package. The interface does not represent modeled hourly dispatch as measured operations.

The user-facing labels now describe tasks rather than implementation stages: Results, Try a
scenario, Hour by hour, Compare plans, and Data & limits. Results begins with a short explanation
of what a scenario is. The form explains cost multipliers and unusual units near the input,
while source classifications and version IDs remain available in Data & limits. Comparison
differences are labeled higher/lower, not automatically judged favorable or unfavorable.
Technical pipeline terms stay in the developer documentation rather than the main journey.
Browser and proxy requests have time limits, so a stalled local API/database produces an error
and retry control instead of leaving new users at a loading spinner indefinitely.
Changing the selected scenario clears the previous figures while the new result loads, so an
old result is never shown under a new scenario name.

Configured presets and completed optimizer results are deliberately distinct. A preset appears in
the builder immediately, but it does not appear as a governed dashboard result until the worker,
publisher, and dbt refresh complete successfully.

## Run locally

Start PostgreSQL, Kafka, FastAPI, the outbox publisher, and the scenario worker as described in the
root README. Then start the browser application:

```powershell
cd frontend
npm.cmd install
npm.cmd run dev
```

Open <http://localhost:3000>. `NEXT_PUBLIC_ATLAS_API_URL` defaults to
`http://localhost:8000`. FastAPI permits the configured `ATLAS_FRONTEND_ORIGIN`, which defaults to
`http://localhost:3000`; change both values together when using different local ports.

## Validation

The scenario catalog has contract tests for uniqueness, version/provenance fields, and the intended
direction of each sensitivity. The frontend is checked with strict TypeScript and a production
Next.js build. The API route is covered by the FastAPI test suite. Model outputs remain academic
decision-support results rather than forecasts or investment advice.

## Current published catalog

As verified on 2 October 2026, all five presets have completed the full asynchronous path and are
available to the dashboard:

- Baseline 2030: run `7ce9e36bc50405b2`;
- High Renewables 2035: run `bfb4ccba74c75a2e`;
- Accelerated Demand 2035: run `32868257a5e5a7f8`;
- High Fuel and Carbon 2030: run `e19115dd0cd73414`;
- Low Clean-Tech Cost 2035: run `35be18ce6883f08a`.

As verified on 3 October 2026, a custom 2027 scenario also completed as run
`e90ef9487ac87bb4`. The comparison mart now contains 15 unique pairs across six current
scenarios. The reported reliability and cost values are model results, not evidence about
historical operations.

During the final two full-year solves, Kafka consumer membership changed before an offset commit.
The already-persisted result remained safe, the event was redelivered, and the worker skipped the
completed job. The worker now treats this commit loss as an expected at-least-once redelivery path
instead of exiting after successful durable publication.
