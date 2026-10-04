select
    s.run_id,
    r.scenario_id,
    s.timestamp_utc,
    s.asset,
    s.carrier,
    s.net_dispatch_mw,
    s.state_of_charge_mwh
from {{ ref('stg_storage') }} as s
inner join {{ ref('stg_runs') }} as r using (run_id)
where r.is_current and r.publication_state = 'published'
