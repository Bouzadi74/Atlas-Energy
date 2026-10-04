# Silver completion contract

## Status

The batch and optimizer-input Silver milestone is complete as version `114b32d17b684b03`. The
build is reproducible with:

```powershell
uv sync --extra dev --extra data
$env:JAVA_HOME = "C:\Program Files\Eclipse Adoptium\jdk-21.0.7.6-hotspot"
$env:PATH = "$env:JAVA_HOME\bin;$env:PATH"
$env:PYSPARK_SUBMIT_ARGS = "--driver-memory 3g pyspark-shell"
uv run atlas-complete-silver
```

Configuration and reviewed policy choices live in
`configs/silver/foundation_completion.yml`. The versioned manifest is written under
`data/silver/manifests/`.

## Materialized tables

- `weather_open_meteo`: 482,328 rows, representing 11 locations and every hour in 2020-2024.
- `weather_source_comparison`: 482,328 NASA/Open-Meteo aligned location-hours.
- `demand_hourly`: 8,784 national UTC hours for leap year 2024.
- `canonical_technology_assumptions`: 204 selected parameter rows for 12 technologies and model
  years 2020, 2030, 2040, and 2050.
- `energy_statistics_reconciliation`: 195 explicit OWID/Ember and IRENA/Ember comparisons.
- `optimizer_hourly_inputs`: 8,784 hourly demand, solar, and wind input rows.

## Weather policy

NASA POWER remains the canonical weather source for the first baseline because the renewable
feature method is based on its variables. The current feature version `d2d6908e2a97ee72` uses
`pvlib` PVWatts/SAPM for solar and `windpowerlib` for the governed wind power curve. Open-Meteo
ERA5 is retained as an independent source-aligned Silver table and is compared at identical
location-hour keys.

The comparison includes temperature, shortwave irradiation, and 10 m wind. NASA 50 m wind and
Open-Meteo 100 m wind are not directly subtracted because the measurement/model heights differ.

## Demand calibration

The hourly demand series is `SYNTHETIC_CALIBRATED`, not observed demand. Its deterministic shape
uses hour, weekday/weekend, season, and mean temperature across the 11 representative locations.
An affine/scale calibration enforces three visually reviewed ONEE facts:

- annual called energy: 45,713.1 GWh;
- maximum power: 7,580 MW;
- maximum-day energy on 25 July 2024: 158,829 MWh.

Validation independently confirmed 45,713,100 MWh, 7,580 MW, 158,829 MWh on the peak day, and
8,784 unique UTC timestamps. Matching these aggregates does not claim reconstruction of the real
ONEE hourly load curve.

## Technology assumption policy

The canonical table uses PyPSA technology-data v0.15.0 as the internally consistent generic
source for investment, fixed/variable O&M, efficiency, lifetime, and emissions parameters. It
normalizes unit labels without altering numeric currency values. Currency and currency year remain
explicit columns.

Morocco-specific Zenodo values remain comparison evidence. Atlas does not convert its 2011/2020
USD values into EUR without governed exchange-rate and inflation indices. This avoids false
precision and undocumented conversions.

## Statistical reconciliation

The reconciliation table never averages publishers. Ember is primary for electricity demand,
generation, mix, imports, emissions, and intensity; OWID is retained as a cross-check and may
incorporate Ember. IRENA is primary for renewable capacity, compared with Ember where the
technology basis aligns.

Using a one-percent relative tolerance, the build produced 10 exact matches, 178 matches within
tolerance, and 7 material differences. Material differences remain visible for review and do not
block optimizer inputs because these historical comparisons are not silently substituted into the
canonical technology or hourly demand tables.

## Optimizer input policy

The 2024 input bundle joins calibrated national demand with equal-weight mean solar and wind
availability across the 11 representative locations. Equal weighting is a transparent national-node
MVP assumption, not an estimate of the existing fleet's geographic weighting. A later regional or
capacity-weighted model must create a new version rather than overwrite this bundle.

## Scope boundary

This completion applies to batch sources and inputs required for the first PyPSA baseline. Kafka
telemetry-to-Delta processing belongs to the blueprint's separate streaming phase. It must retain
partition/offset lineage, deduplicate `event_id`, use event-time watermarks, and quarantine invalid
events, but it is not a dependency of the first capacity-expansion solve.

Twelve verified legacy NASA metadata sidecars still generate audit warnings. Their raw checksums
are valid and they do not affect Silver correctness. Migrate them by creating a new standardized
Bronze ingestion partition; never rewrite the historical Bronze partition in place.
