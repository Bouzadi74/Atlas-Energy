select
    run_id,
    component_type,
    sum(amount_eur) as amount_eur
from {{ ref('stg_costs') }}
group by run_id, component_type
