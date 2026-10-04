# Atlas Energy on GitHub

Atlas Energy is an independent academic project for exploring Morocco's electricity-transition choices. It combines public-source ingestion, documented model assumptions, hourly optimization, and a scenario dashboard. Results are model estimates—not live grid data or investment advice.

## What is in this repository

- `src/atlas/`: ingestion, transformation, scenario execution, optimization, API, and workers.
- `configs/`: versioned data-source, location, model, and scenario settings.
- `dbt_atlas/`: PostgreSQL analytical models and quality tests.
- `frontend/`: the Next.js dashboard and scenario builder.
- `infra/`, `airflow/`, `kafka/`: local infrastructure, orchestration, and event contracts.
- `tests/` and `.github/workflows/`: automated checks.

Raw downloads, generated Bronze/Silver tables, optimizer inputs and outputs, browser profiles, and local credentials are intentionally not included. Follow [README.md](README.md) and [CONNECTING_TECHNOLOGIES.md](CONNECTING_TECHNOLOGIES.md) to build those locally from documented sources. Source rights and attribution are documented in [docs/data/LICENSING_AND_ATTRIBUTION.md](docs/data/LICENSING_AND_ATTRIBUTION.md).

## Local start

On Windows PowerShell, install Python 3.12 or 3.13, `uv`, Node.js 20+, and Docker Desktop. Then:

```powershell
Copy-Item .env.example .env
uv sync --extra dev
docker compose up -d postgres kafka
uv run uvicorn atlas.api.main:app --reload
```

In another terminal:

```powershell
cd frontend
npm.cmd ci
npm.cmd run dev
```

Open `http://localhost:3000`. To execute a new scenario, start the outbox publisher and scenario worker in separate terminals using the commands in [README.md](README.md). Historical data and full-year model inputs must be acquired and built first; this repository does not ship the source files.

Before using dbt locally, copy `dbt_atlas/profiles.example.yml` to `dbt_atlas/profiles.yml` and set any non-default credentials through environment variables. The local profile is not tracked.

## Project status and limitations

The local MVP includes a validated national-node PyPSA/HiGHS workflow, five scenario presets, PostgreSQL/dbt result serving, and a browser interface. It is not a production deployment: end-user identity, managed secrets, TLS, operational alerting, and approved real-time utility telemetry remain outside the current scope. Hourly demand is synthetic-calibrated; renewable availability is modeled, not measured plant output. See [docs/limitations.md](docs/limitations.md) for the full qualification.

Atlas Energy is not affiliated with ONEE, ANRE, the Moroccan government, or the publishers of the referenced public datasets.
