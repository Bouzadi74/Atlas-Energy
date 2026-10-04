select
    d.run_id,
    r.scenario_id,
    d.timestamp_utc,
    d.asset,
    d.carrier,
    d.dispatch_mw
from {{ ref('stg_dispatch') }} as d
inner join {{ ref('stg_runs') }} as r using (run_id)
where r.is_current and r.publication_state = 'published'
