select
    run_id,
    timestamp_utc,
    asset,
    count(*) as row_count
from {{ ref('stg_storage') }}
group by run_id, timestamp_utc, asset
having count(*) > 1
