select
    d.run_id,
    d.asset,
    d.carrier,
    sum(d.dispatch_mw * r.annualization_factor) as annual_generation_mwh
from {{ ref('stg_dispatch') }} as d
inner join {{ ref('stg_runs') }} as r using (run_id)
group by d.run_id, d.asset, d.carrier
