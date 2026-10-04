begin;

alter table optimization.scenario_runs
    add column if not exists optimizer_run_id text,
    add column if not exists processing_attempts integer not null default 0,
    add column if not exists max_attempts integer not null default 3,
    add column if not exists last_attempt_at timestamptz;

do $$
begin
    if not exists (
        select 1 from pg_constraint
        where conname = 'scenario_run_optimizer_run_fk'
          and conrelid = 'optimization.scenario_runs'::regclass
    ) then
        alter table optimization.scenario_runs
            add constraint scenario_run_optimizer_run_fk
            foreign key (optimizer_run_id)
            references optimization.optimization_runs (run_id);
    end if;
    if not exists (
        select 1 from pg_constraint
        where conname = 'scenario_run_attempts_valid'
          and conrelid = 'optimization.scenario_runs'::regclass
    ) then
        alter table optimization.scenario_runs
            add constraint scenario_run_attempts_valid
            check (
                processing_attempts >= 0
                and max_attempts > 0
                and processing_attempts <= max_attempts
            );
    end if;
end
$$;

create index if not exists idx_scenario_runs_optimizer_run
    on optimization.scenario_runs (optimizer_run_id)
    where optimizer_run_id is not null;

commit;
