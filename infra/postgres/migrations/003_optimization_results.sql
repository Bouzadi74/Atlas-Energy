begin;

create schema if not exists optimization;

create table if not exists optimization.optimization_runs (
    run_id text primary key,
    scenario_id text not null,
    scenario_name text,
    status text not null,
    termination_condition text not null,
    planning_year integer,
    weather_year integer,
    snapshot_count integer not null,
    objective_total_eur double precision not null,
    annualized_capital_cost_eur double precision not null,
    annualized_operating_cost_eur double precision not null,
    demand_mwh double precision not null,
    renewable_generation_mwh double precision not null,
    renewable_generation_share double precision not null,
    emissions_tco2 double precision not null,
    curtailment_mwh double precision not null,
    unmet_demand_mwh double precision not null,
    unmet_demand_share double precision not null,
    solver_name text not null,
    solver_version text not null,
    model_version text not null,
    engine_version text not null,
    config_version text not null,
    input_package_version text,
    solve_duration_seconds double precision not null,
    artifact_root text not null,
    manifest_sha256 char(64) not null,
    generated_at timestamptz not null,
    published_at timestamptz not null default now(),

    constraint optimization_run_status check (status = 'ok'),
    constraint optimization_run_termination check (termination_condition = 'optimal'),
    constraint optimization_run_manifest_hash check (manifest_sha256 ~ '^[a-f0-9]{64}$'),
    constraint optimization_run_snapshot_count check (snapshot_count > 0),
    constraint optimization_run_renewable_share check (
        renewable_generation_share between 0 and 1
    ),
    constraint optimization_run_unmet_share check (unmet_demand_share between 0 and 1)
);

create index if not exists idx_optimization_runs_scenario
    on optimization.optimization_runs (scenario_id, published_at desc);

create table if not exists optimization.scenario_inputs (
    run_id text primary key references optimization.optimization_runs (run_id) on delete cascade,
    demand_growth double precision,
    gas_price_eur_mwh_th double precision,
    carbon_price_eur_t double precision,
    renewable_generation_min double precision,
    battery_capex_multiplier double precision,
    solar_capex_multiplier double precision,
    wind_capex_multiplier double precision,
    input_payload jsonb not null
);

create table if not exists optimization.run_artifacts (
    run_id text not null references optimization.optimization_runs (run_id) on delete cascade,
    artifact_name text not null,
    artifact_format text not null,
    relative_path text not null,
    sha256 char(64) not null,
    row_count bigint,
    byte_count bigint not null,
    primary key (run_id, artifact_name, artifact_format),

    constraint run_artifact_hash check (sha256 ~ '^[a-f0-9]{64}$'),
    constraint run_artifact_format check (artifact_format in ('parquet', 'csv', 'json')),
    constraint run_artifact_byte_count check (byte_count >= 0),
    constraint run_artifact_row_count check (row_count is null or row_count >= 0)
);

create table if not exists optimization.capacity_results (
    run_id text not null references optimization.optimization_runs (run_id) on delete cascade,
    component text not null,
    asset text not null,
    carrier text not null,
    asset_role text not null,
    existing_capacity_mw double precision not null,
    optimized_capacity_mw double precision not null,
    new_capacity_mw double precision not null,
    capacity_limit_mw double precision,
    energy_capacity_mwh double precision,
    primary key (run_id, component, asset)
);

create table if not exists optimization.dispatch_hourly (
    run_id text not null references optimization.optimization_runs (run_id) on delete cascade,
    timestamp_utc timestamp without time zone not null,
    asset text not null,
    carrier text not null,
    dispatch_mw double precision not null,
    primary key (run_id, timestamp_utc, asset)
);

create index if not exists idx_dispatch_hourly_run_carrier_time
    on optimization.dispatch_hourly (run_id, carrier, timestamp_utc);

create table if not exists optimization.storage_hourly (
    run_id text not null references optimization.optimization_runs (run_id) on delete cascade,
    timestamp_utc timestamp without time zone not null,
    asset text not null,
    carrier text not null,
    net_dispatch_mw double precision not null,
    state_of_charge_mwh double precision not null,
    primary key (run_id, timestamp_utc, asset)
);

create index if not exists idx_storage_hourly_run_carrier_time
    on optimization.storage_hourly (run_id, carrier, timestamp_utc);

create table if not exists optimization.cost_components (
    run_id text not null references optimization.optimization_runs (run_id) on delete cascade,
    asset text not null,
    carrier text not null,
    component_type text not null,
    amount_eur double precision not null,
    primary key (run_id, asset, component_type),

    constraint cost_component_type check (component_type in ('capital', 'operating'))
);

create table if not exists optimization.run_provenance (
    run_id text not null references optimization.optimization_runs (run_id) on delete cascade,
    provenance_key text not null,
    provenance_value jsonb not null,
    primary key (run_id, provenance_key)
);

commit;
