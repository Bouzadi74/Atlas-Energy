select
    s.run_id,
    s.asset,
    s.carrier,
    sum(greatest(s.net_dispatch_mw, 0) * r.annualization_factor) as annual_discharge_mwh,
    sum(greatest(-s.net_dispatch_mw, 0) * r.annualization_factor) as annual_charge_mwh,
    max(s.state_of_charge_mwh) as maximum_state_of_charge_mwh
from {{ ref('stg_storage') }} as s
inner join {{ ref('stg_runs') }} as r using (run_id)
group by s.run_id, s.asset, s.carrier
