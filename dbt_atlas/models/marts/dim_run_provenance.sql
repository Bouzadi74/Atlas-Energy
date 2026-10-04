select
    p.run_id,
    r.scenario_id,
    r.scenario_name,
    p.provenance_key,
    p.provenance_value
from {{ ref('stg_provenance') }} as p
inner join {{ ref('stg_runs') }} as r using (run_id)
where r.is_current and r.publication_state = 'published'
