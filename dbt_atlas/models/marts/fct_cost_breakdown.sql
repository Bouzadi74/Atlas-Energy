select
    c.run_id,
    r.scenario_id,
    r.scenario_name,
    c.asset,
    c.carrier,
    c.component_type,
    c.amount_eur
from {{ ref('stg_costs') }} as c
inner join {{ ref('stg_runs') }} as r using (run_id)
where r.is_current and r.publication_state = 'published'
