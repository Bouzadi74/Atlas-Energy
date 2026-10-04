begin;

alter table optimization.optimization_runs
    add column if not exists run_mode text not null default 'unknown',
    add column if not exists calendar_mode text not null default 'unknown',
    add column if not exists publication_state text not null default 'published',
    add column if not exists is_current boolean not null default true,
    add column if not exists superseded_by_run_id text;

do $$
begin
    if not exists (
        select 1 from pg_constraint
        where conname = 'optimization_run_publication_state'
    ) then
        alter table optimization.optimization_runs
            add constraint optimization_run_publication_state
            check (publication_state in ('published', 'superseded', 'withdrawn'));
    end if;
    if not exists (
        select 1 from pg_constraint
        where conname = 'optimization_run_superseded_by_fk'
    ) then
        alter table optimization.optimization_runs
            add constraint optimization_run_superseded_by_fk
            foreign key (superseded_by_run_id)
            references optimization.optimization_runs (run_id);
    end if;
end
$$;

create unique index if not exists uq_current_optimization_run
    on optimization.optimization_runs (scenario_id, run_mode, calendar_mode)
    where is_current;

commit;
