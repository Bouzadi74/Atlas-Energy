select
    r.run_id,
    r.scenario_id,
    r.scenario_name,
    r.planning_year,
    r.weather_year,
    r.objective_total_eur as annual_system_cost_eur,
    r.objective_total_eur / nullif(r.demand_mwh, 0) as average_cost_eur_mwh,
    r.annualized_capital_cost_eur,
    r.annualized_operating_cost_eur,
    r.demand_mwh,
    e.renewable_generation_mwh,
    e.renewable_generation_mwh / nullif(r.demand_mwh, 0) as renewable_generation_share,
    e.imports_mwh,
    e.imports_mwh / nullif(r.demand_mwh, 0) as import_share,
    e.reservoir_hydro_mwh,
    e.battery_discharge_mwh,
    e.pumped_hydro_discharge_mwh,
    r.emissions_tco2,
    r.curtailment_mwh,
    case when abs(e.unserved_mwh) < 1e-9 then 0 else e.unserved_mwh end as unserved_mwh,
    case
        when abs(e.unserved_mwh) < 1e-9 then 0
        else e.unserved_mwh / nullif(r.demand_mwh, 0)
    end as unserved_energy_share,
    e.balanced_demand_mwh,
    r.solve_duration_seconds,
    r.model_version,
    r.engine_version,
    r.input_package_version
from {{ ref('stg_runs') }} as r
inner join {{ ref('int_run_energy') }} as e using (run_id)
where r.is_current and r.publication_state = 'published'
