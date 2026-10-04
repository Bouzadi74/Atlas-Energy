select
    j.job_id,
    j.scenario_id as application_scenario_id,
    r.scenario_id as served_scenario_id,
    r.optimizer_scenario_id
from {{ source('optimization', 'scenario_runs') }} as j
inner join {{ ref('stg_runs') }} as r
    on r.run_id = j.optimizer_run_id
where j.optimizer_run_id is not null
    and (
        r.scenario_id <> j.scenario_id
        or r.optimizer_scenario_id is null
        or (j.status = 'failed' and r.is_current)
    )
