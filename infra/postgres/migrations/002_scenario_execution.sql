begin;

create schema if not exists optimization;

create table if not exists optimization.scenario_requests (
    scenario_id text primary key,
    request_hash char(64) not null unique,
    name text not null,
    request_payload jsonb not null,
    model_version text not null,
    data_version text not null,
    created_at timestamptz not null default now(),

    constraint scenario_request_hash_format
        check (request_hash ~ '^[a-f0-9]{64}$')
);

create table if not exists optimization.scenario_runs (
    job_id text primary key,
    scenario_id text not null references optimization.scenario_requests (
        scenario_id
    ),
    status text not null default 'queued',
    created_at timestamptz not null default now(),
    started_at timestamptz,
    completed_at timestamptz,
    error_message text,

    constraint scenario_run_status
        check (status in ('queued', 'running', 'completed', 'failed'))
);

create index if not exists idx_scenario_runs_scenario
    on optimization.scenario_runs (scenario_id);

create index if not exists idx_scenario_runs_status
    on optimization.scenario_runs (status);

create table if not exists optimization.outbox_events (
    event_id uuid primary key default gen_random_uuid(),
    aggregate_id text not null,
    event_type text not null,
    topic text not null,
    event_key text not null,
    schema_version integer not null default 1,
    payload jsonb not null,
    created_at timestamptz not null default now(),
    published_at timestamptz,
    publish_attempts integer not null default 0,
    last_error text,

    constraint outbox_schema_version_positive
        check (schema_version > 0),

    constraint outbox_publish_attempts_positive
        check (publish_attempts >= 0)
);

create index if not exists idx_outbox_unpublished
    on optimization.outbox_events (created_at)
    where published_at is null;

commit;