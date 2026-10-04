# Versioned energy-model input package

## Current package

The validated Morocco baseline package is `0e5327a2df73e087` under:

```text
data/model_inputs/version=0e5327a2df73e087/
```

Rebuild or safely reuse it with:

```powershell
uv sync --extra dev --extra data
uv run atlas-build-model-inputs
```

The package version is a deterministic hash of the configuration, transformation version, active
Delta versions, and checksums of every active upstream Parquet file. A changed source, rule, or
transformation creates a new directory instead of overwriting an existing version.

## Files

- `hourly_baseline_8784`: complete 2024 UTC demand, solar availability, and wind availability.
- `hourly_planning_8760`: non-leap planning series plus source and planning timestamps.
- `existing_capacity`: IRENA on-grid technology detail reconciled to the reviewed ONEE total.
- `technology_assumptions`: canonical 2030 CAPEX, FOM, VOM, efficiency, lifetime, and emissions
  parameters where applicable.
- `assumptions.json`: the full machine-readable package policy.
- `manifest.json`: checksums, source snapshots, validation metrics, provenance, and limitations.

Tabular files are supplied as Parquet for typed model loading and CSV for inspection.

## Hourly inputs and provenance

NASA POWER is the canonical weather input for the initial renewable features. Open-Meteo is an
independent comparison source in Silver. The PySpark feature job uses `pvlib` PVWatts with SAPM
cell-temperature modeling for solar and `windpowerlib` with a governed generic turbine power curve
for wind. Assumptions are versioned in `configs/features/renewable_profiles.yml`; feature version
`d2d6908e2a97ee72` produced 96,624 location-hours. These outputs are `SYNTHETIC`, not measured
plant production. The national aggregation is an equal-weight mean across the eleven
representative locations.

The demand shape is `SYNTHETIC_CALIBRATED`. It is constrained to reviewed ONEE facts for 2024:

- 45,713,100 MWh annual called energy;
- 7,580 MW annual peak;
- 158,829 MWh on the maximum-energy day, 25 July 2024.

It is not an estimate of the real utility dispatch or SCADA load curve.

## Leap-year reconciliation

The baseline retains all 8,784 hours of leap year 2024. The 8,760-hour planning representation:

1. removes the 24 UTC hours of 29 February;
2. retains solar and wind capacity factors for every remaining source hour;
3. applies one affine transformation to remaining demand values;
4. preserves exactly 45,713,100 MWh annual demand and the 7,580 MW peak;
5. maps source month/day/hour to the configured non-leap planning year.

The conversion does not claim to preserve the maximum-day energy anchor. Both variants remain in
the package so validation can always return to the untouched leap-year calibration.

## Installed-capacity reconciliation

The national-grid model uses IRENA 2024 `On-grid electricity` technology rows and excludes
off-grid solar and wind. Raw IRENA detail totals 12,023.933 MW. ONEE's visually reviewed national
total is 12,017 MW, a difference of -6.933 MW (-0.0577%). The configured rule allocates only this
small residual to `fossil_other`; raw capacity and adjustment remain separate columns.

IRENA on-grid wind is 2,390 MW and exactly matches the reviewed ANRE value. MEF reports 5,304 MW
renewable capacity for August 2024, while the IRENA year-end on-grid sum excluding pumped storage
is 4,630.933 MW. Their period and technology bases differ, so the package records the difference
but does not average or allocate it.

## Validation result

- baseline hours: 8,784 unique UTC keys;
- planning hours: 8,760 sequential snapshots;
- annual demand preserved: 45,713,100 MWh in both variants;
- peak preserved: 7,580 MW in both variants;
- capacity-factor bounds: all values within `[0, 1]`;
- reconciled installed capacity: 12,017 MW;
- technology assumptions: 51 rows for model year 2030;
- every output file: SHA-256 recorded in the manifest.

The package can be loaded directly by the PyPSA network builder. Network equations, annualized
capital-cost calculation, fuel/carbon marginal costs, storage duration, and existing-only
technology operating rules remain responsibilities of the optimization phase rather than this
data package.
