from pathlib import Path

from atlas.scenarios.catalog import load_scenario_presets

SCENARIO_DIR = Path("configs/scenarios")


def test_catalog_contains_five_versioned_reproducible_scenarios() -> None:
    presets = load_scenario_presets(SCENARIO_DIR)

    assert len(presets) >= 5
    assert len({preset.preset_id for preset in presets}) == len(presets)
    assert len({preset.request.name for preset in presets}) == len(presets)
    assert all(preset.provenance.classification == "ASSUMPTION" for preset in presets)
    assert all(preset.provenance.version == "1.0" for preset in presets)
    assert all(len(preset.provenance.note) >= 40 for preset in presets)


def test_sensitivity_presets_change_the_intended_assumption_family() -> None:
    by_id = {preset.preset_id: preset.request for preset in load_scenario_presets(SCENARIO_DIR)}
    baseline = by_id["baseline-2030"]

    fuel = by_id["high-fuel-carbon-2030"]
    assert fuel.gas_price_eur_mwh_th > baseline.gas_price_eur_mwh_th
    assert fuel.carbon_price_eur_t > baseline.carbon_price_eur_t
    assert fuel.demand_growth == baseline.demand_growth

    demand = by_id["accelerated-demand-2035"]
    assert demand.demand_growth > baseline.demand_growth
    assert demand.gas_price_eur_mwh_th == baseline.gas_price_eur_mwh_th

    technology = by_id["low-clean-tech-cost-2035"]
    assert technology.battery_capex_multiplier < baseline.battery_capex_multiplier
    assert technology.solar_capex_multiplier < baseline.solar_capex_multiplier
    assert technology.wind_capex_multiplier < baseline.wind_capex_multiplier
