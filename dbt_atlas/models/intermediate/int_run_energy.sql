with generation as (
    select
        run_id,
        sum(annual_generation_mwh) as generator_supply_mwh,
        sum(annual_generation_mwh) filter (
            where carrier in ('solar', 'wind', 'bioenergy')
        ) as renewable_generator_mwh,
        sum(annual_generation_mwh) filter (where carrier = 'imports') as imports_mwh,
        sum(annual_generation_mwh) filter (where carrier = 'reliability') as unserved_mwh
    from {{ ref('int_generation_by_run_asset') }}
    group by run_id
),
storage as (
    select
        run_id,
        sum(annual_discharge_mwh - annual_charge_mwh) as storage_net_dispatch_mwh,
        sum(annual_discharge_mwh) filter (where carrier = 'hydro') as reservoir_hydro_mwh,
        sum(annual_discharge_mwh) filter (where carrier = 'battery') as battery_discharge_mwh,
        sum(annual_discharge_mwh) filter (
            where carrier = 'pumped_hydro'
        ) as pumped_hydro_discharge_mwh
    from {{ ref('int_storage_by_run_asset') }}
    group by run_id
)
select
    g.run_id,
    g.generator_supply_mwh,
    coalesce(s.storage_net_dispatch_mwh, 0) as storage_net_dispatch_mwh,
    g.generator_supply_mwh + coalesce(s.storage_net_dispatch_mwh, 0) as balanced_demand_mwh,
    g.renewable_generator_mwh + coalesce(s.reservoir_hydro_mwh, 0) as renewable_generation_mwh,
    g.imports_mwh,
    g.unserved_mwh,
    coalesce(s.reservoir_hydro_mwh, 0) as reservoir_hydro_mwh,
    coalesce(s.battery_discharge_mwh, 0) as battery_discharge_mwh,
    coalesce(s.pumped_hydro_discharge_mwh, 0) as pumped_hydro_discharge_mwh
from generation as g
left join storage as s using (run_id)
