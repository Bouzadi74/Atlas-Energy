select
    run_id,
    provenance_key,
    count(*) as row_count
from {{ ref('dim_run_provenance') }}
group by run_id, provenance_key
having count(*) > 1
