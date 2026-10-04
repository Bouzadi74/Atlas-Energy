select
    run_id,
    timestamp_utc,
    asset,
    carrier,
    net_dispatch_mw,
    state_of_charge_mwh
from {{ source('optimization', 'storage_hourly') }}
