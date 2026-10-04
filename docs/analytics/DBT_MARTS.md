# dbt analytical marts

## Purpose

dbt Core with the PostgreSQL adapter converts atomically published optimizer results into stable,
tested business-facing models. It does not repeat source cleansing or optimization mathematics.
PostgreSQL is the implemented local serving warehouse; the model boundaries are intentionally
portable for a future Snowflake adapter.

## Layers

Staging views standardize the eight `optimization` publication tables:

- `stg_runs`, `stg_scenario_inputs`, `stg_artifacts`, and `stg_provenance`;
- `stg_capacity`, `stg_dispatch`, `stg_storage`, and `stg_costs`.

Intermediate views own reusable calculations:

- `int_generation_by_run_asset`: annualized generation by generator/import asset;
- `int_storage_by_run_asset`: annualized charging/discharging and maximum state of charge;
- `int_cost_by_run_component`: capital and operating totals;
- `int_run_energy`: generator supply, storage net flow, imports, unserved energy, reservoir hydro,
  and renewable generation.

Business marts are materialized tables:

- `dim_scenario`: assumptions, versions, solver, and publication lifecycle by run;
- `fct_scenario_kpi`: cost, demand, renewable share, imports, emissions, curtailment, storage output,
  and reliability KPIs;
- `fct_capacity`: existing/new/optimized power and storage energy capacity;
- `fct_dispatch_hourly` and `fct_storage_hourly`: current hourly operating results;
- `fct_cost_breakdown`: asset-level capital and combined operating costs;
- `dim_run_provenance`: input metadata, validation, software, and output contracts;
- `fct_scenario_comparison`: pairwise KPI deltas ordered by planning year and run ID.

Battery and pumped-hydro discharge are reported separately but are not counted as new renewable
generation. Renewable generation includes solar, wind, bioenergy, and reservoir-hydro discharge.

## Build

```powershell
uv sync --extra dev --extra analytics
uv run dbt debug --project-dir dbt_atlas --profiles-dir dbt_atlas
uv run dbt build --project-dir dbt_atlas --profiles-dir dbt_atlas
```

The local profile reads `ATLAS_DBT_*` environment variables and defaults to the Docker Compose
PostgreSQL instance on port 55432.

## Quality gates

The project tests source relationships, nullability, accepted lifecycle/status values, run and
hourly natural-key uniqueness, current-run uniqueness, annual cost reconciliation, hourly-to-annual
energy reconciliation, renewable-generation reconciliation, and run/provenance-key uniqueness.
The current build contains 20 models and 50 data tests.

## Current comparison

The marts contain two corrected engine-v4 annual scenarios:

- Baseline 2030: EUR 3.416 billion annual cost, EUR 62.59/MWh average cost, 72.52% renewable share,
  13.458 TWh imports, 6.981 MtCO2, and zero unserved energy;
- High Renewables 2035: EUR 4.242 billion annual cost, EUR 60.28/MWh average cost, 82.81% renewable
  share, 12.090 TWh imports, 5.620 MtCO2, and zero unserved energy.

Relative to Baseline 2030, the 2035 scenario has EUR 0.826 billion higher annual cost because it
serves substantially more demand, but EUR 2.31/MWh lower average cost, 10.29 percentage points more
renewable generation, 1.361 MtCO2 lower emissions, and 1.368 TWh fewer imports. These are model
outputs under different years and assumptions, not a causal forecast or investment recommendation.
