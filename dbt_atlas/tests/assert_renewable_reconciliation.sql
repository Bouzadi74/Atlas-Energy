select
    r.run_id,
    r.renewable_generation_mwh as reported_mwh,
    e.renewable_generation_mwh as calculated_mwh,
    abs(r.renewable_generation_mwh - e.renewable_generation_mwh) as difference_mwh
from {{ ref('stg_runs') }} as r
inner join {{ ref('int_run_energy') }} as e using (run_id)
where abs(r.renewable_generation_mwh - e.renewable_generation_mwh)
    > greatest(1e-4, abs(r.renewable_generation_mwh) * 1e-8)
