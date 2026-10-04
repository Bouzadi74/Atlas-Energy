# National-node optimization baseline

## Status

Atlas Energy now has a reproducible PyPSA/HiGHS capacity-expansion and dispatch model. The model
reads the versioned optimizer package, not Bronze or ad-hoc user-interface values. The verified
progression is:

1. a hand-checkable 24-hour system;
2. an annualized 168-hour integration case;
3. the 8,760-hour Baseline 2030 planning case.

The optimizer remains callable through its CLI and is also connected to the Kafka scenario worker.
The worker uses the same deterministic engine, publishes verified outputs to PostgreSQL, refreshes
dbt marts, and records the resulting optimizer run ID on the application job.

## Model scope

The single Morocco bus includes fixed existing solar, wind, hydro, pumped hydro, coal, gas, oil,
bioenergy, and import capacity. Solar, onshore wind, CCGT, OCGT, and four-hour batteries can expand
within configured limits. A deliberately expensive load-shedding generator makes reliability
failures visible rather than making the linear problem infeasible.

The objective minimizes annualized new-build CAPEX and fixed OPEX plus variable OPEX, fuel, carbon,
imports, and reliability penalties. Thermal marginal cost divides thermal fuel and carbon inputs
by efficiency. Storage enforces charge/discharge efficiency, power and energy limits, and cyclic
state of charge. The scenario's minimum renewable-generation share is an explicit linear
constraint.

Configuration lives in `configs/optimization/national_baseline.yml`; the scenario lives in
`configs/scenarios/baseline-2030.yml`. Both are included in the deterministic run hash together
with the input-package version and optimizer-engine version.

## Commands

```powershell
uv sync --extra dev --extra optimization
uv run atlas-optimize --mode toy
uv run atlas-optimize --mode week --calendar planning
uv run atlas-optimize --mode full --calendar planning
```

The `planning` calendar removes leap day and maps 2024's remaining 8,760 hours to 2030. The demand
series is compounded from 2024 through the planning year using the scenario growth rate. Use
`--calendar baseline` for an 8,784-hour calibration-year run without that calendar conversion.

## Verified results

The current engine version is `optimization-engine-v4`, using PyPSA 1.2.4 and HiGHS 1.15.1.

- Toy run `5f5c03cb553d6321`: 2,400 MWh demand, 1,000 MWh solar generation, 200 MWh
  curtailment, EUR 70,000 objective, and zero unmet demand.
- Week run `ddbd8f4ddf520980`: optimal, zero unmet demand, and all validation gates passed. Its
  annualization is useful for integration testing, not as the reference investment answer.
- Full planning run `7ce9e36bc50405b2`: optimal over 8,760 hours; EUR 3.4162 billion total annual
  cost, 54.5838 TWh demand, 39.5834 TWh renewable generation (72.52%), 6.9815 MtCO2, 4.6249 TWh
  curtailment, and zero unmet demand.
- The full run selects 17.0436 GW new solar, 4.7859 GW new onshore wind, and 5.0702 GW / 20.2807 GWh
  new battery capacity. It selects no new CCGT or OCGT under this scenario's assumptions.

The full run's maximum hourly balance residual is `3.64e-12 MW`; objective reconciliation differs
by `4.77e-07 EUR`. Renewable output limits, storage bounds, the reservoir non-charging rule, cyclic
storage configuration, renewable policy, reliability, finite-value checks, and cost reconciliation
all pass.

Engine v4 corrected a defect found during the economic plausibility review: the reservoir-hydro
`StorageUnit` had inherited PyPSA's default ability to charge from the bus. It is now discharge-only
and its annual output reconciles to the configured 321 GWh budget. The earlier v3 database result
is retained for audit but marked superseded by the v4 run.

These figures are optimization outputs from assumptions and synthetic/calibrated profiles. They
are not observed system operations, forecasts, investment advice, or government plans.

## Run output contract

Each run writes Parquet and CSV capacity, generator dispatch, storage/SOC, annual asset summary,
and hourly balance tables, plus JSON KPIs and a manifest. The manifest records scenario/input/model
versions, solver result, every validation gate, output row counts, and SHA-256 checksums.

## Known limitations

- The model is a national copper plate without transmission congestion or regional siting.
- It is linear and uses perfect foresight; there is no unit commitment, reserve requirement,
  ramping, startup cost, outage schedule, or forecast error.
- Existing CSP uses the PV availability series in this MVP.
- Reservoir hydro uses a flat inflow calibrated to a 321 GWh annual generation budget.
- Pumped hydro and batteries use fixed durations; the battery is a PyPSA `StorageUnit` rather than
  independently optimized power and energy components.
- Import capacity, marginal cost, and emissions are explicit assumptions rather than a modeled
  neighboring market.
- Existing capacity has no sunk fixed-OPEX contribution in the objective; the current investment
  comparison focuses on new capacity and dispatch costs.
