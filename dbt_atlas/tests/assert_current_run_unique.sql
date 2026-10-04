select
    scenario_id,
    run_mode,
    calendar_mode,
    count(*) as row_count
from {{ ref('stg_runs') }}
where is_current
group by scenario_id, run_mode, calendar_mode
having count(*) > 1
