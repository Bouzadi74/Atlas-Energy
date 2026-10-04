select
    run_id,
    timestamp_utc,
    asset,
    carrier,
    dispatch_mw
from {{ source('optimization', 'dispatch_hourly') }}
