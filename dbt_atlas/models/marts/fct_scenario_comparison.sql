select
    a.run_id as base_run_id,
    b.run_id as comparison_run_id,
    a.scenario_name as base_scenario_name,
    b.scenario_name as comparison_scenario_name,
    b.annual_system_cost_eur - a.annual_system_cost_eur as annual_cost_delta_eur,
    b.average_cost_eur_mwh - a.average_cost_eur_mwh as average_cost_delta_eur_mwh,
    b.renewable_generation_share - a.renewable_generation_share as renewable_share_delta,
    b.emissions_tco2 - a.emissions_tco2 as emissions_delta_tco2,
    b.curtailment_mwh - a.curtailment_mwh as curtailment_delta_mwh,
    b.unserved_mwh - a.unserved_mwh as unserved_delta_mwh,
    b.imports_mwh - a.imports_mwh as imports_delta_mwh
from {{ ref('fct_scenario_kpi') }} as a
inner join {{ ref('fct_scenario_kpi') }} as b
    on (a.planning_year, a.run_id) < (b.planning_year, b.run_id)
