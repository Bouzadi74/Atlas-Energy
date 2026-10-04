select
    c.run_id,
    r.scenario_id,
    r.scenario_name,
    c.component,
    c.asset,
    c.carrier,
    c.asset_role,
    c.existing_capacity_mw,
    c.new_capacity_mw,
    c.optimized_capacity_mw,
    c.energy_capacity_mwh
from {{ ref('stg_capacity') }} as c
inner join {{ ref('stg_runs') }} as r using (run_id)
where r.is_current and r.publication_state = 'published'
