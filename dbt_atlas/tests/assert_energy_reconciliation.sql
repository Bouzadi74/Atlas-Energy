select
    r.run_id,
    r.demand_mwh,
    e.balanced_demand_mwh,
    abs(r.demand_mwh - e.balanced_demand_mwh) as difference_mwh
from {{ ref('stg_runs') }} as r
inner join {{ ref('int_run_energy') }} as e using (run_id)
where abs(r.demand_mwh - e.balanced_demand_mwh)
    > greatest(1e-4, abs(r.demand_mwh) * 1e-8)
