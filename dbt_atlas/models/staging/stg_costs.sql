select
    run_id,
    asset,
    carrier,
    component_type,
    amount_eur
from {{ source('optimization', 'cost_components') }}
