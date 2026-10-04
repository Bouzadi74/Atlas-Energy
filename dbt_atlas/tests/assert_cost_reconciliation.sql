with costs as (
    select
        run_id,
        sum(amount_eur) as calculated_cost_eur
    from {{ ref('stg_costs') }}
    group by run_id
)
select
    r.run_id,
    r.objective_total_eur,
    c.calculated_cost_eur,
    abs(r.objective_total_eur - c.calculated_cost_eur) as difference_eur
from {{ ref('stg_runs') }} as r
inner join costs as c using (run_id)
where abs(r.objective_total_eur - c.calculated_cost_eur)
    > greatest(0.01, abs(r.objective_total_eur) * 1e-8)
