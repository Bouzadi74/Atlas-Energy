from pathlib import Path

import pytest

from atlas.optimization.engine import (
    annualized_capital_cost,
    capital_recovery_factor,
    load_optimization_config,
    run_optimization,
    thermal_marginal_cost,
)


def test_capital_recovery_factor() -> None:
    assert capital_recovery_factor(0.0, 20) == pytest.approx(0.05)
    assert capital_recovery_factor(0.07, 20) == pytest.approx(0.0943929)
    assert annualized_capital_cost(1000, 2, 20, 0.07) == pytest.approx(114392.9, rel=1e-5)


def test_thermal_marginal_cost_uses_efficiency_and_carbon() -> None:
    value = thermal_marginal_cost(
        fuel_eur_mwh_th=60,
        efficiency=0.5,
        vom_eur_mwh=5,
        carbon_price_eur_t=50,
        emissions_tco2_mwh_th=0.2,
    )
    assert value == pytest.approx(145.0)

    higher_fuel_price = thermal_marginal_cost(
        fuel_eur_mwh_th=80,
        efficiency=0.5,
        vom_eur_mwh=5,
        carbon_price_eur_t=50,
        emissions_tco2_mwh_th=0.2,
    )
    assert higher_fuel_price > value


def test_toy_network_solves_and_passes_validation(tmp_path: Path) -> None:
    pytest.importorskip("pypsa")
    config = load_optimization_config(Path("configs/optimization/national_baseline.yml"))

    result = run_optimization(config=config, mode="toy", output_root=tmp_path)

    assert result.status == "ok"
    assert result.termination_condition == "optimal"
    assert result.kpis["snapshot_count"] == 24
    assert result.kpis["objective_total_eur"] == pytest.approx(70_000.0, abs=1e-4)
    assert result.kpis["demand_mwh"] == pytest.approx(2_400.0)
    assert result.kpis["renewable_generation_mwh"] == pytest.approx(1_000.0)
    assert result.kpis["curtailment_mwh"] == pytest.approx(200.0)
    assert result.kpis["unmet_demand_mwh"] == pytest.approx(0.0, abs=1e-6)
    assert (result.output_path / "manifest.json").exists()
    assert (result.output_path / "capacity.parquet").exists()
    assert (result.output_path / "annual_summary.parquet").exists()
    assert (result.output_path / "hourly_balance.parquet").exists()
