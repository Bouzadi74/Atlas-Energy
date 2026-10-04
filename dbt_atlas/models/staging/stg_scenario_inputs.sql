select
    run_id,
    demand_growth,
    gas_price_eur_mwh_th,
    carbon_price_eur_t,
    renewable_generation_min,
    battery_capex_multiplier,
    solar_capex_multiplier,
    wind_capex_multiplier,
    input_payload
from {{ source('optimization', 'scenario_inputs') }}
