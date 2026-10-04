# API security and observability

Atlas Energy has an opt-in service-key boundary and a free local monitoring stack. Authentication
is disabled by default for the local academic workflow. A deployed instance must enable it and
restrict network access to operational endpoints.

## Service authentication

Set two distinct secrets in the API environment:

```powershell
$env:ATLAS_AUTH_ENABLED = "true"
$env:ATLAS_API_READ_KEY = "<generated-read-key>"
$env:ATLAS_API_WRITE_KEY = "<generated-write-key>"
uv run uvicorn atlas.api.main:app --host 0.0.0.0 --port 8000
```

The read key can call `GET` result, scenario, job, and comparison routes. The write key can call
both read routes and mutating routes such as `POST /v1/scenarios`. Clients may send the credential
as `X-Atlas-API-Key` or `Authorization: Bearer <key>`. Missing credentials return `401`; invalid
credentials and read credentials used for writes return `403`.

Generate long random values, store them outside Git, and rotate them if exposed. This is
service-level protection for the current MVP, not end-user identity or role-based authorization.
TLS termination, a secret manager, user authentication, and fine-grained permissions remain
deployment work.

The Next.js application uses its server-side `/api/atlas/*` proxy, so an API key is never embedded
in browser JavaScript. Copy `frontend/.env.example` to `frontend/.env.local`, set
`ATLAS_API_URL`, and set `ATLAS_API_KEY` to the write key when API authentication is enabled.
Never use a `NEXT_PUBLIC_` variable for this credential.

## Health and telemetry endpoints

- `GET /health` confirms that the API process is alive.
- `GET /ready` verifies that the API can connect to PostgreSQL.
- `GET /metrics` exposes Prometheus-format request counts, latency histograms, in-progress requests,
  and build information.

These three routes are intentionally not protected by the service key so container health checks
and Prometheus can call them. In any shared or deployed environment, expose them only on a trusted
internal network or protect them at the reverse proxy/firewall. Do not publish `/metrics` directly
to the internet.

Every response includes `X-Request-ID`. A valid incoming request ID is preserved; otherwise the API
creates one. Each request produces a structured JSON log with request ID, method, normalized route,
status, and duration. Credentials and request bodies are not logged. Prometheus labels use route
templates rather than scenario/run identifiers to avoid unbounded cardinality.

## Local Prometheus and Grafana

Set a local Grafana password in `.env`, then start the optional profile:

```powershell
$env:ATLAS_GRAFANA_ADMIN_PASSWORD = "<local-password>"
docker compose --profile observability up -d prometheus grafana
uv run uvicorn atlas.api.main:app --host 0.0.0.0 --port 8000
```

Open:

- Prometheus: <http://localhost:9090>
- Grafana: <http://localhost:3001> (user `atlas`)

Prometheus scrapes `host.docker.internal:8000`, so the API must listen on `0.0.0.0` while the
monitoring containers are running. Grafana provisions the `Atlas Prometheus` data source and the
`Atlas Energy / Atlas API Operations` dashboard automatically. The dashboard reports request rate,
p95 latency, error rate, and requests in progress.

The Compose fallback Grafana password is for local bootstrap only. Set
`ATLAS_GRAFANA_ADMIN_PASSWORD` before using the profile on any shared machine. Stop the monitoring
services without deleting their volumes using:

```powershell
docker compose --profile observability stop prometheus grafana
```

Alert routing, distributed tracing, centralized log retention, and production SLOs are not yet
implemented.
