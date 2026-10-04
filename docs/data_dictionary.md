# Data dictionary foundation

All persisted datasets require these metadata fields where applicable:

- `source_id`: stable identifier for the upstream source.
- `retrieved_at`: UTC retrieval timestamp.
- `source_url`: exact retrieval or publication location.
- `content_checksum`: checksum of the raw payload.
- `provenance_class`: one of `OBSERVED`, `REANALYSIS`, `DERIVED`, `ASSUMPTION`, `SYNTHETIC_CALIBRATED`, or `SYNTHETIC`.
- `schema_version`: contract version.
- `data_batch_id`: idempotent ingestion batch identifier.

Canonical timestamps are UTC. Preserve source timezone metadata. Power uses MW, energy uses MWh, prices state numerator/denominator and currency year, and emissions use explicit mass and gas units.

## Optimizer-facing Silver tables

The field-level source dictionaries remain with their focused transformation documentation. The
completed optimizer-facing layer adds:

- `weather_open_meteo`: Open-Meteo ERA5 hourly weather keyed by
  `(source, location_id, timestamp_utc)`;
- `weather_source_comparison`: aligned NASA/Open-Meteo temperature, irradiation, and 10 m wind
  values with signed differences;
- `demand_hourly`: national hourly MW/MWh, national temperature, raw shape, calibration flags,
  target values, source IDs/checksum, method/profile versions, and provenance;
- `canonical_technology_assumptions`: model year, canonical/PyPSA technology, PyPSA component,
  carrier, parameter, value, source/canonical units, currency basis, source lineage, and selection
  policy;
- `energy_statistics_reconciliation`: metric/year/source pair, both values, unit, absolute and
  percentage difference, comparison status, tolerance, basis note, and checksums;
- `optimizer_hourly_inputs`: timestamp, demand MW, national solar/wind availability, quality flag,
  contributing location count, demand/renewable versions, aggregation method, and provenance.

## Versioned model-input package

- `hourly_baseline_8784`: snapshot index, 2024 UTC timestamp, demand MW, solar/wind capacity
  factors, quality flags, one-hour snapshot weight, feature/profile/Silver versions, and provenance;
- `hourly_planning_8760`: source timestamp, non-leap planning timestamp, sequential snapshot index,
  demand MW, unchanged renewable availability, affine scale/offset, calendar method, and lineage;
- `existing_capacity`: canonical technology/component/carrier, raw IRENA on-grid MW, explicit ONEE
  reconciliation adjustment, model MW, source selectors/checksum, provenance, and grid scope;
- `technology_assumptions`: planning/model year, canonical/PyPSA technology, component/carrier,
  parameter/value, source and canonical units, currency/year, source checksum, selection policy,
  and Silver/package lineage.

See `docs/data/MODEL_INPUT_PACKAGE.md` for package versioning and validation rules.

See `docs/data/SILVER_COMPLETION.md` for calibration and quality rules.

## Synthetic telemetry stream

- `bronze/telemetry/kafka`: `raw_key` and `raw_value` preserve the Kafka bytes. The natural key is
  `(topic, partition, offset)`; `kafka_timestamp` and `ingested_at` distinguish broker and Atlas
  arrival times.
- `silver/telemetry_events`: `event_id` is the deduplicated key. `event_time_utc` is the parsed
  event time; `producer`, `trace_id`, `source_id`, `asset_id`, `metric`, `value`, `unit`, and
  `provenance_classification` retain the source contract. `topic`, `partition`, `offset`,
  `kafka_timestamp`, and `ingested_at` provide Bronze lineage. Power metrics use MW, stored energy
  uses MWh, and the current source classification is `SYNTHETIC`.
- `silver/telemetry_quarantine`: original Kafka bytes and coordinates plus `rejection_reason`.
  Invalid values are preserved, not replaced.

See `docs/data/TELEMETRY_STREAMING.md` for the source boundary, watermark, and checkpoint policy.

## Optimization run outputs

Every directory under `data/optimization/scenario_id=<id>/run_id=<id>/` contains:

- `capacity`: component, asset, carrier, role, existing/optimized/new power capacity, optional
  reliability-slack limit, and storage energy capacity;
- `dispatch`: UTC timestamp, generator asset/carrier, and dispatch MW;
- `storage`: UTC timestamp, storage asset/carrier, signed net dispatch MW (positive discharge), and
  state of charge MWh;
- `hourly_balance`: demand, generator supply, storage net dispatch, and balance residual MW;
- `annual_summary`: asset-level annualized generation/charging, availability, curtailment, capital
  and operating cost, and emissions;
- `kpis.json`: total/capital/operating cost, demand, renewable generation/share, emissions,
  curtailment, unmet demand, snapshot count, and annualization factor;
- `manifest.json`: deterministic run/model/config identifiers, scenario and input metadata, solver
  outcome, validation gates, output row counts, and checksums.

Power is MW, energy is MWh, cost is EUR in the configured currency basis, and emissions are tCO2.

## PostgreSQL result-serving tables

- `scenario_requests`: immutable canonical request, hash, model version, and data version;
- `scenario_runs`: asynchronous job status plus granular optimizer/publication/analytics stage,
  retry counters, stage timestamps, published optimizer-run link, and stage-specific failure data;
- `outbox_events`: durable scenario request/completion events and Kafka publication attempts;

- `optimization_runs`: one row per immutable optimizer run with scenario identity, lifecycle,
  headline KPIs, solver/model/input versions, artifact root, and manifest checksum;
- `scenario_inputs`: one-to-one scenario assumptions plus the complete JSON request;
- `run_artifacts`: one row per physical file format with checksum, size, and optional row count;
- `capacity_results`: one row per run/component/asset;
- `dispatch_hourly`: one row per run/timestamp/generator asset;
- `storage_hourly`: one row per run/timestamp/storage asset;
- `cost_components`: capital and combined operating cost per asset;
- `run_provenance`: grouped JSON input, software, validation, and output contracts.

Only one result is current for a `(scenario_id, run_mode, calendar_mode)` combination. Superseded
rows remain queryable for audit. See `docs/optimization/RESULT_PUBLICATION.md`.

## dbt analytical models

The `analytics_staging` schema exposes typed source-aligned views. The `analytics_intermediate`
schema contains reusable annual generation, storage, cost, and energy reconciliations. The
`analytics_marts` schema contains `dim_scenario`, `fct_scenario_kpi`, `fct_capacity`,
`fct_dispatch_hourly`, `fct_storage_hourly`, `fct_cost_breakdown`, and
`fct_scenario_comparison`, plus `dim_run_provenance`. Model grain and KPI definitions are documented in
`docs/analytics/DBT_MARTS.md`.
