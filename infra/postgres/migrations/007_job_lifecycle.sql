begin;

alter table optimization.scenario_runs
    add column if not exists stage text not null default 'queued',
    add column if not exists optimized_at timestamptz,
    add column if not exists result_published_at timestamptz,
    add column if not exists analytics_ready_at timestamptz,
    add column if not exists failed_at timestamptz,
    add column if not exists failure_stage text,
    add column if not exists error_type text;

update optimization.scenario_runs
set stage = case status
    when 'completed' then 'completed'
    when 'failed' then 'failed'
    when 'running' then 'optimizing'
    else 'queued'
end
where stage = 'queued';

do $$
begin
    if not exists (
        select 1 from pg_constraint
        where conname = 'scenario_run_stage_valid'
          and conrelid = 'optimization.scenario_runs'::regclass
    ) then
        alter table optimization.scenario_runs
            add constraint scenario_run_stage_valid
            check (stage in (
                'queued',
                'optimizing',
                'optimized',
                'publishing',
                'published',
                'building_analytics',
                'analytics_ready',
                'completed',
                'failed'
            ));
    end if;
    if not exists (
        select 1 from pg_constraint
        where conname = 'scenario_run_status_stage_consistent'
          and conrelid = 'optimization.scenario_runs'::regclass
    ) then
        alter table optimization.scenario_runs
            add constraint scenario_run_status_stage_consistent
            check (
                (status = 'queued' and stage = 'queued')
                or (
                    status = 'running'
                    and stage in (
                        'optimizing',
                        'optimized',
                        'publishing',
                        'published',
                        'building_analytics',
                        'analytics_ready'
                    )
                )
                or (status = 'completed' and stage = 'completed')
                or (status = 'failed' and stage = 'failed')
            );
    end if;
    if not exists (
        select 1 from pg_constraint
        where conname = 'scenario_run_failure_stage_valid'
          and conrelid = 'optimization.scenario_runs'::regclass
    ) then
        alter table optimization.scenario_runs
            add constraint scenario_run_failure_stage_valid
            check (
                failure_stage is null
                or failure_stage in (
                    'optimizing',
                    'optimized',
                    'publishing',
                    'published',
                    'building_analytics',
                    'analytics_ready'
                )
            );
    end if;
end
$$;

create index if not exists idx_scenario_runs_stage
    on optimization.scenario_runs (stage, last_attempt_at);

commit;
