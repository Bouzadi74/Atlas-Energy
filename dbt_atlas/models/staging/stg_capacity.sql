select
    run_id,
    component,
    asset,
    carrier,
    asset_role,
    existing_capacity_mw,
    optimized_capacity_mw,
    new_capacity_mw,
    capacity_limit_mw,
    energy_capacity_mwh
from {{ source('optimization', 'capacity_results') }}
