begin;

alter table optimization.optimization_runs
    add column if not exists annualization_factor double precision not null default 1.0;

alter table optimization.optimization_runs
    drop constraint if exists optimization_run_annualization_factor;

alter table optimization.optimization_runs
    add constraint optimization_run_annualization_factor
    check (annualization_factor > 0);

commit;
