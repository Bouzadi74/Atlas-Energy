import argparse
import hashlib
import importlib.metadata
import json
import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from time import perf_counter
from typing import Any, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field

from atlas.config import get_settings
from atlas.schemas.scenario import ScenarioRequest

LOGGER = logging.getLogger(__name__)
ENGINE_VERSION = "optimization-engine-v4"
RunMode = Literal["toy", "week", "full"]
CalendarMode = Literal["planning", "baseline"]


class ReliabilityConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    load_shedding_penalty_eur_mwh: float = Field(gt=0)
    maximum_unmet_demand_share: float = Field(ge=0, le=1)


class ImportConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    capacity_mw: float = Field(ge=0)
    marginal_cost_eur_mwh: float = Field(ge=0)
    emissions_tco2_mwh: float = Field(ge=0)


class ThermalConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    coal_fuel_eur_mwh_th: float = Field(ge=0)
    oil_fuel_eur_mwh_th: float = Field(ge=0)
    bioenergy_marginal_cost_eur_mwh: float = Field(ge=0)
    bioenergy_emissions_tco2_mwh: float = Field(ge=0)
    gas_emissions_tco2_mwh_th: float = Field(ge=0)


class StorageConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    battery_duration_hours: float = Field(gt=0)
    battery_max_power_mw: float = Field(gt=0)
    pumped_hydro_duration_hours: float = Field(gt=0)
    pumped_hydro_charge_efficiency: float = Field(gt=0, le=1)
    pumped_hydro_discharge_efficiency: float = Field(gt=0, le=1)


class HydroConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    reservoir_duration_hours: float = Field(gt=0)
    annual_generation_budget_mwh: float = Field(ge=0)
    dispatch_efficiency: float = Field(gt=0, le=1)


class ExpansionConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    solar_max_new_mw: float = Field(ge=0)
    wind_max_new_mw: float = Field(ge=0)
    gas_ccgt_max_new_mw: float = Field(ge=0)
    gas_ocgt_max_new_mw: float = Field(ge=0)


class OptimizationConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    version: str
    model_version: str
    currency: str
    discount_rate: float = Field(ge=0, lt=1)
    solver_name: str
    reliability: ReliabilityConfig
    imports: ImportConfig
    thermal: ThermalConfig
    storage: StorageConfig
    hydro: HydroConfig
    expansion: ExpansionConfig
    modeling_policy: dict[str, str]


@dataclass(frozen=True)
class OptimizationResult:
    run_id: str
    output_path: Path
    status: str
    termination_condition: str
    kpis: dict[str, Any]


def load_optimization_config(path: Path) -> OptimizationConfig:
    try:
        payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        raise FileNotFoundError(f"Optimization configuration not found: {path}") from None
    except yaml.YAMLError as error:
        raise ValueError(f"Invalid optimization YAML {path}: {error}") from error
    return OptimizationConfig.model_validate(payload)


def load_scenario(path: Path) -> ScenarioRequest:
    try:
        payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        raise FileNotFoundError(f"Scenario configuration not found: {path}") from None
    except yaml.YAMLError as error:
        raise ValueError(f"Invalid scenario YAML {path}: {error}") from error
    payload.pop("provenance", None)
    return ScenarioRequest.model_validate(payload)


def capital_recovery_factor(discount_rate: float, lifetime_years: float) -> float:
    if lifetime_years <= 0:
        raise ValueError("Lifetime must be positive")
    if discount_rate == 0:
        return 1.0 / lifetime_years
    return discount_rate / (1.0 - (1.0 + discount_rate) ** (-lifetime_years))


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def annualized_capital_cost(
    investment_eur_per_kw: float,
    fom_percent: float,
    lifetime_years: float,
    discount_rate: float,
    multiplier: float = 1.0,
) -> float:
    overnight_eur_per_mw = investment_eur_per_kw * 1000.0 * multiplier
    return overnight_eur_per_mw * (
        capital_recovery_factor(discount_rate, lifetime_years) + fom_percent / 100.0
    )


def thermal_marginal_cost(
    *,
    fuel_eur_mwh_th: float,
    efficiency: float,
    vom_eur_mwh: float,
    carbon_price_eur_t: float,
    emissions_tco2_mwh_th: float,
) -> float:
    if efficiency <= 0 or efficiency > 1:
        raise ValueError("Thermal efficiency must be within (0, 1]")
    return (
        vom_eur_mwh
        + fuel_eur_mwh_th / efficiency
        + carbon_price_eur_t * emissions_tco2_mwh_th / efficiency
    )


def find_latest_package(data_root: Path) -> Path:
    candidates: list[tuple[float, Path]] = []
    for manifest in (data_root / "model_inputs").glob("version=*/manifest.json"):
        payload = json.loads(manifest.read_text(encoding="utf-8"))
        if payload.get("optimizer_ready") is True:
            candidates.append((manifest.stat().st_mtime, manifest.parent))
    if not candidates:
        raise FileNotFoundError("No optimizer-ready model-input package found")
    return max(candidates, key=lambda item: item[0])[1]


def _assumption_map(rows: Any) -> dict[str, dict[str, float]]:
    output: dict[str, dict[str, float]] = {}
    for row in rows.itertuples(index=False):
        output.setdefault(str(row.canonical_technology), {})[str(row.parameter)] = float(
            row.value
        )
    return output


def _capacity_map(rows: Any) -> dict[str, float]:
    return {
        str(row.canonical_technology): float(row.installed_capacity_mw)
        for row in rows.itertuples(index=False)
    }


def _technology_cost(
    assumptions: dict[str, dict[str, float]],
    technology: str,
    config: OptimizationConfig,
    multiplier: float = 1.0,
) -> float:
    values = assumptions[technology]
    return annualized_capital_cost(
        values["investment"],
        values.get("FOM", 0.0),
        values["lifetime"],
        config.discount_rate,
        multiplier,
    )


def _add_carriers(network: Any) -> None:
    for carrier in (
        "electricity",
        "solar",
        "wind",
        "hydro",
        "coal",
        "gas",
        "oil",
        "bioenergy",
        "imports",
        "battery",
        "pumped_hydro",
        "reliability",
    ):
        network.add("Carrier", carrier)


def build_toy_network(config: OptimizationConfig) -> tuple[Any, dict[str, Any]]:
    """Build a hand-checkable 24-hour dispatch and storage case."""

    import pandas as pd
    import pypsa

    snapshots = pd.date_range("2030-01-01", periods=24, freq="h")
    solar_availability = pd.Series(0.0, index=snapshots)
    solar_availability.iloc[8:16] = 1.0
    network = pypsa.Network()
    network.set_snapshots(snapshots)
    _add_carriers(network)
    network.add("Bus", "morocco", carrier="electricity")
    network.add("Load", "demand", bus="morocco", p_set=pd.Series(100.0, index=snapshots))
    network.add(
        "Generator",
        "solar_existing",
        bus="morocco",
        carrier="solar",
        p_nom=150.0,
        p_max_pu=solar_availability,
        marginal_cost=0.0,
    )
    network.add(
        "Generator",
        "gas_existing",
        bus="morocco",
        carrier="gas",
        p_nom=100.0,
        marginal_cost=50.0,
    )
    network.add(
        "Generator",
        "imports",
        bus="morocco",
        carrier="imports",
        p_nom=100.0,
        marginal_cost=80.0,
    )
    network.add(
        "Generator",
        "unmet_demand",
        bus="morocco",
        carrier="reliability",
        p_nom=100.0,
        marginal_cost=config.reliability.load_shedding_penalty_eur_mwh,
    )
    network.add(
        "StorageUnit",
        "battery",
        bus="morocco",
        carrier="battery",
        p_nom=50.0,
        max_hours=4.0,
        efficiency_store=1.0,
        efficiency_dispatch=1.0,
        cyclic_state_of_charge=True,
    )
    metadata = {
        "mode": "toy",
        "expected_demand_mwh": 2400.0,
        "expected_solar_available_mwh": 1200.0,
        "expected_zero_imports": True,
        "expected_zero_unmet_demand": True,
    }
    return network, metadata


def build_package_network(
    *,
    package_path: Path,
    scenario: ScenarioRequest,
    config: OptimizationConfig,
    mode: Literal["week", "full"],
    calendar_mode: CalendarMode,
) -> tuple[Any, dict[str, Any]]:
    import pandas as pd
    import pypsa

    manifest = json.loads((package_path / "manifest.json").read_text(encoding="utf-8"))
    hourly_name = (
        "hourly_baseline_8784.parquet"
        if calendar_mode == "baseline"
        else "hourly_planning_8760.parquet"
    )
    hourly = pd.read_parquet(package_path / hourly_name).sort_values("snapshot_index")
    if mode == "week":
        hourly = hourly.iloc[:168].copy()
    if hourly.empty:
        raise ValueError("Selected model-input horizon is empty")
    capacity = pd.read_parquet(package_path / "existing_capacity.parquet")
    technology = pd.read_parquet(package_path / "technology_assumptions.parquet")
    assumptions = _assumption_map(technology)
    capacities = _capacity_map(capacity)

    timestamp_column = (
        "source_timestamp_utc" if calendar_mode == "baseline" else "planning_timestamp_utc"
    )
    if timestamp_column not in hourly:
        timestamp_column = "timestamp_utc"
    snapshots = pd.DatetimeIndex(
        pd.to_datetime(hourly[timestamp_column], utc=True)
    ).tz_localize(None)
    hourly.index = snapshots
    years = max(0, scenario.planning_year - scenario.weather_year)
    demand_multiplier = (1.0 + scenario.demand_growth) ** years
    demand = hourly.demand_mw.astype(float) * demand_multiplier
    solar = hourly.solar_capacity_factor.astype(float)
    wind = hourly.wind_capacity_factor.astype(float)

    network = pypsa.Network()
    network.set_snapshots(snapshots)
    annualization_factor = 8760.0 / len(snapshots) if mode == "week" else 1.0
    network.snapshot_weightings.loc[:, "objective"] = annualization_factor
    network.snapshot_weightings.loc[:, "generators"] = annualization_factor
    network.snapshot_weightings.loc[:, "stores"] = 1.0
    _add_carriers(network)
    network.add("Bus", "morocco", carrier="electricity")
    network.add("Load", "demand", bus="morocco", p_set=demand)

    solar_existing = capacities["solar_utility"] + capacities["solar_csp"]
    hydro_existing = capacities["hydro"]
    pumped_existing = capacities["pumped_hydro"]
    oil_existing = capacities["fossil_other"]
    network.add(
        "Generator",
        "solar_existing",
        bus="morocco",
        carrier="solar",
        p_nom=solar_existing,
        p_max_pu=solar,
        marginal_cost=assumptions["solar_utility"].get("VOM", 0.0),
    )
    network.add(
        "Generator",
        "wind_existing",
        bus="morocco",
        carrier="wind",
        p_nom=capacities["onshore_wind"],
        p_max_pu=wind,
        marginal_cost=assumptions["onshore_wind"].get("VOM", 0.0),
    )

    coal_efficiency = assumptions["coal"]["efficiency"]
    oil_efficiency = assumptions["oil"]["efficiency"]
    gas_efficiency = assumptions["gas_ccgt"]["efficiency"]
    gas_emissions = config.thermal.gas_emissions_tco2_mwh_th / gas_efficiency
    coal_emissions = assumptions["coal"]["CO2 intensity"] / coal_efficiency
    oil_emissions = assumptions["oil"]["CO2 intensity"] / oil_efficiency
    thermal_specs = (
        (
            "coal_existing",
            "coal",
            capacities["coal"],
            thermal_marginal_cost(
                fuel_eur_mwh_th=config.thermal.coal_fuel_eur_mwh_th,
                efficiency=coal_efficiency,
                vom_eur_mwh=assumptions["coal"].get("VOM", 0.0),
                carbon_price_eur_t=scenario.carbon_price_eur_t,
                emissions_tco2_mwh_th=assumptions["coal"]["CO2 intensity"],
            ),
            coal_emissions,
        ),
        (
            "gas_existing",
            "gas",
            capacities["gas_ccgt"],
            thermal_marginal_cost(
                fuel_eur_mwh_th=scenario.gas_price_eur_mwh_th,
                efficiency=gas_efficiency,
                vom_eur_mwh=assumptions["gas_ccgt"].get("VOM", 0.0),
                carbon_price_eur_t=scenario.carbon_price_eur_t,
                emissions_tco2_mwh_th=config.thermal.gas_emissions_tco2_mwh_th,
            ),
            gas_emissions,
        ),
        (
            "oil_existing",
            "oil",
            oil_existing,
            thermal_marginal_cost(
                fuel_eur_mwh_th=config.thermal.oil_fuel_eur_mwh_th,
                efficiency=oil_efficiency,
                vom_eur_mwh=assumptions["oil"].get("VOM", 0.0),
                carbon_price_eur_t=scenario.carbon_price_eur_t,
                emissions_tco2_mwh_th=assumptions["oil"]["CO2 intensity"],
            ),
            oil_emissions,
        ),
    )
    for name, carrier, capacity_mw, marginal_cost, emissions in thermal_specs:
        network.add(
            "Generator",
            name,
            bus="morocco",
            carrier=carrier,
            p_nom=capacity_mw,
            marginal_cost=marginal_cost,
            emissions_tco2_mwh=emissions,
        )
    network.add(
        "Generator",
        "bioenergy_existing",
        bus="morocco",
        carrier="bioenergy",
        p_nom=capacities["bioenergy"],
        marginal_cost=config.thermal.bioenergy_marginal_cost_eur_mwh,
        emissions_tco2_mwh=config.thermal.bioenergy_emissions_tco2_mwh,
    )
    network.add(
        "Generator",
        "imports",
        bus="morocco",
        carrier="imports",
        p_nom=config.imports.capacity_mw,
        marginal_cost=config.imports.marginal_cost_eur_mwh,
        emissions_tco2_mwh=config.imports.emissions_tco2_mwh,
    )
    network.add(
        "Generator",
        "unmet_demand",
        bus="morocco",
        carrier="reliability",
        p_nom=float(demand.max()),
        marginal_cost=config.reliability.load_shedding_penalty_eur_mwh,
        emissions_tco2_mwh=0.0,
    )

    hydro_inflow = (
        config.hydro.annual_generation_budget_mwh
        / config.hydro.dispatch_efficiency
        / 8760.0
    )
    network.add(
        "StorageUnit",
        "reservoir_hydro",
        bus="morocco",
        carrier="hydro",
        p_nom=hydro_existing,
        max_hours=config.hydro.reservoir_duration_hours,
        p_min_pu=0.0,
        inflow=pd.Series(hydro_inflow, index=snapshots),
        efficiency_store=1.0,
        efficiency_dispatch=config.hydro.dispatch_efficiency,
        cyclic_state_of_charge=True,
        marginal_cost=0.0,
    )
    network.add(
        "StorageUnit",
        "pumped_hydro",
        bus="morocco",
        carrier="pumped_hydro",
        p_nom=pumped_existing,
        max_hours=config.storage.pumped_hydro_duration_hours,
        efficiency_store=config.storage.pumped_hydro_charge_efficiency,
        efficiency_dispatch=config.storage.pumped_hydro_discharge_efficiency,
        cyclic_state_of_charge=True,
        marginal_cost=0.0,
    )

    solar_capital = _technology_cost(
        assumptions, "solar_utility", config, scenario.solar_capex_multiplier
    )
    wind_capital = _technology_cost(
        assumptions, "onshore_wind", config, scenario.wind_capex_multiplier
    )
    ccgt_capital = _technology_cost(assumptions, "gas_ccgt", config)
    ocgt_capital = _technology_cost(assumptions, "gas_ocgt", config)
    network.add(
        "Generator",
        "solar_new",
        bus="morocco",
        carrier="solar",
        p_nom_extendable=True,
        p_nom_max=config.expansion.solar_max_new_mw,
        p_max_pu=solar,
        capital_cost=solar_capital,
        marginal_cost=assumptions["solar_utility"].get("VOM", 0.0),
    )
    network.add(
        "Generator",
        "wind_new",
        bus="morocco",
        carrier="wind",
        p_nom_extendable=True,
        p_nom_max=config.expansion.wind_max_new_mw,
        p_max_pu=wind,
        capital_cost=wind_capital,
        marginal_cost=assumptions["onshore_wind"].get("VOM", 0.0),
    )
    network.add(
        "Generator",
        "gas_ccgt_new",
        bus="morocco",
        carrier="gas",
        p_nom_extendable=True,
        p_nom_max=config.expansion.gas_ccgt_max_new_mw,
        capital_cost=ccgt_capital,
        marginal_cost=thermal_specs[1][3],
        emissions_tco2_mwh=gas_emissions,
    )
    ocgt_efficiency = assumptions["gas_ocgt"]["efficiency"]
    network.add(
        "Generator",
        "gas_ocgt_new",
        bus="morocco",
        carrier="gas",
        p_nom_extendable=True,
        p_nom_max=config.expansion.gas_ocgt_max_new_mw,
        capital_cost=ocgt_capital,
        marginal_cost=thermal_marginal_cost(
            fuel_eur_mwh_th=scenario.gas_price_eur_mwh_th,
            efficiency=ocgt_efficiency,
            vom_eur_mwh=assumptions["gas_ocgt"].get("VOM", 0.0),
            carbon_price_eur_t=scenario.carbon_price_eur_t,
            emissions_tco2_mwh_th=config.thermal.gas_emissions_tco2_mwh_th,
        ),
        emissions_tco2_mwh=config.thermal.gas_emissions_tco2_mwh_th
        / ocgt_efficiency,
    )

    inverter = assumptions["battery_inverter"]
    storage = assumptions["battery_storage"]
    inverter_cost = _technology_cost(
        assumptions, "battery_inverter", config, scenario.battery_capex_multiplier
    )
    storage_cost_per_mwh = annualized_capital_cost(
        storage["investment"],
        storage.get("FOM", 0.0),
        storage["lifetime"],
        config.discount_rate,
        scenario.battery_capex_multiplier,
    )
    battery_capital = (
        inverter_cost + storage_cost_per_mwh * config.storage.battery_duration_hours
    )
    network.add(
        "StorageUnit",
        "battery_new",
        bus="morocco",
        carrier="battery",
        p_nom_extendable=True,
        p_nom_max=config.storage.battery_max_power_mw,
        max_hours=config.storage.battery_duration_hours,
        efficiency_store=inverter["efficiency"],
        efficiency_dispatch=inverter["efficiency"],
        cyclic_state_of_charge=True,
        capital_cost=battery_capital,
        marginal_cost=0.0,
    )
    metadata = {
        "mode": mode,
        "calendar_mode": calendar_mode,
        "package_version": manifest["package_version"],
        "annualization_factor": annualization_factor,
        "demand_multiplier": demand_multiplier,
        "renewable_generation_min": scenario.renewable_generation_min,
        "costs_eur_per_mw_year": {
            "solar_new": solar_capital,
            "wind_new": wind_capital,
            "gas_ccgt_new": ccgt_capital,
            "gas_ocgt_new": ocgt_capital,
            "battery_new": battery_capital,
        },
    }
    return network, metadata


def _renewable_constraint(network: Any, snapshots: Any, share: float) -> None:
    renewable_names = network.generators.index[
        network.generators.carrier.isin(["solar", "wind", "bioenergy"])
    ]
    dispatch = network.model.variables["Generator-p"].sel(name=renewable_names)
    weights = network.snapshot_weightings.loc[snapshots, "generators"]
    lhs = (dispatch * weights).sum()
    hydro_names = network.storage_units.index[
        network.storage_units.carrier.eq("hydro")
    ]
    if len(hydro_names):
        hydro_dispatch = network.model.variables["StorageUnit-p_dispatch"].sel(
            name=hydro_names
        )
        lhs = lhs + (hydro_dispatch * weights).sum()
    demand = float(
        network.loads_t.p_set.loc[snapshots]
        .mul(weights, axis=0)
        .to_numpy()
        .sum()
    )
    network.model.add_constraints(lhs >= share * demand, name="renewable_generation_min")


def solve_network(
    network: Any,
    *,
    solver_name: str,
    renewable_generation_min: float | None = None,
) -> tuple[str, str, float]:
    extra = None
    if renewable_generation_min is not None:
        extra = lambda n, snapshots: _renewable_constraint(  # noqa: E731
            n, snapshots, renewable_generation_min
        )
    started_at = perf_counter()
    status, termination = network.optimize(
        solver_name=solver_name,
        extra_functionality=extra,
        log_to_console=False,
        include_objective_constant=False,
    )
    solve_duration_seconds = perf_counter() - started_at
    if status != "ok" or termination != "optimal":
        raise RuntimeError(f"Optimization failed: status={status}, termination={termination}")
    return str(status), str(termination), solve_duration_seconds


def validate_and_summarize(
    network: Any,
    *,
    config: OptimizationConfig,
    metadata: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, float]]:
    import numpy as np

    snapshots = network.snapshots
    generator_dispatch = network.generators_t.p.loc[snapshots]
    load = network.loads_t.p_set.loc[snapshots].sum(axis=1)
    storage_dispatch = network.storage_units_t.p.loc[snapshots]
    objective_weights = network.snapshot_weightings.loc[snapshots, "objective"]
    balance = generator_dispatch.sum(axis=1) + storage_dispatch.sum(axis=1) - load
    max_balance_error = float(balance.abs().max())

    availability_violations = 0
    curtailment_mwh = 0.0
    for name in network.generators.index:
        p_nom = float(
            network.generators.at[name, "p_nom_opt"]
            if network.generators.at[name, "p_nom_extendable"]
            else network.generators.at[name, "p_nom"]
        )
        if name in network.generators_t.p_max_pu.columns:
            availability = network.generators_t.p_max_pu.loc[snapshots, name]
        else:
            availability = float(network.generators.at[name, "p_max_pu"])
        available = availability * p_nom
        dispatch = generator_dispatch[name]
        availability_violations += int((dispatch > available + 1e-5).sum())
        if network.generators.at[name, "carrier"] in {"solar", "wind"}:
            curtailment_mwh += float(
                ((available - dispatch).clip(lower=0) * objective_weights).sum()
            )

    soc = network.storage_units_t.state_of_charge.loc[snapshots]
    storage_violations = 0
    storage_power_violations = 0
    reservoir_charging_violations = 0
    for name in network.storage_units.index:
        p_nom = float(
            network.storage_units.at[name, "p_nom_opt"]
            if network.storage_units.at[name, "p_nom_extendable"]
            else network.storage_units.at[name, "p_nom"]
        )
        maximum = p_nom * float(network.storage_units.at[name, "max_hours"])
        storage_violations += int(((soc[name] < -1e-5) | (soc[name] > maximum + 1e-5)).sum())
        storage_power_violations += int((storage_dispatch[name].abs() > p_nom + 1e-5).sum())
        if network.storage_units.at[name, "carrier"] == "hydro":
            reservoir_charging_violations += int((storage_dispatch[name] < -1e-5).sum())

    operational_cost = 0.0
    emissions = 0.0
    for name in network.generators.index:
        weighted_energy = float((generator_dispatch[name] * objective_weights).sum())
        operational_cost += weighted_energy * float(network.generators.at[name, "marginal_cost"])
        emission_factor = float(
            network.generators.at[name, "emissions_tco2_mwh"]
            if "emissions_tco2_mwh" in network.generators.columns
            else 0.0
        )
        if not np.isfinite(emission_factor):
            emission_factor = 0.0
        emissions += weighted_energy * emission_factor
    capital_cost = 0.0
    for name in network.generators.index[network.generators.p_nom_extendable]:
        capital_cost += float(network.generators.at[name, "p_nom_opt"]) * float(
            network.generators.at[name, "capital_cost"]
        )
    for name in network.storage_units.index[network.storage_units.p_nom_extendable]:
        capital_cost += float(network.storage_units.at[name, "p_nom_opt"]) * float(
            network.storage_units.at[name, "capital_cost"]
        )
    reconstructed_cost = capital_cost + operational_cost
    objective = float(network.objective)
    objective_error = abs(reconstructed_cost - objective)
    unmet_mwh = float((generator_dispatch.get("unmet_demand", 0.0) * objective_weights).sum())
    demand_mwh = float((load * objective_weights).sum())
    unmet_share = unmet_mwh / demand_mwh if demand_mwh else 0.0
    renewable_names = network.generators.index[
        network.generators.carrier.isin(["solar", "wind", "bioenergy"])
    ]
    renewable_mwh = float(
        generator_dispatch[renewable_names].mul(objective_weights, axis=0).to_numpy().sum()
    )
    hydro_mwh = 0.0
    if "reservoir_hydro" in storage_dispatch:
        hydro_mwh = float(
            storage_dispatch["reservoir_hydro"]
            .clip(lower=0)
            .mul(objective_weights)
            .sum()
        )
    renewable_share = (renewable_mwh + hydro_mwh) / demand_mwh if demand_mwh else 0.0
    checks = {
        "hourly_balance_max_abs_mw": max_balance_error,
        "hourly_balance_passed": max_balance_error <= 1e-4,
        "renewable_limits_passed": availability_violations == 0,
        "storage_bounds_passed": storage_violations == 0 and storage_power_violations == 0,
        "reservoir_not_charging_passed": reservoir_charging_violations == 0,
        "cyclic_storage_configured_passed": bool(
            network.storage_units.cyclic_state_of_charge.all()
        ),
        "cost_reconciliation_abs_eur": objective_error,
        "cost_reconciliation_passed": objective_error <= max(0.01, abs(objective) * 1e-8),
        "reliability_passed": unmet_share <= config.reliability.maximum_unmet_demand_share,
        "all_finite_passed": bool(
            np.isfinite([objective, capital_cost, operational_cost, emissions]).all()
        ),
    }
    required_share = metadata.get("renewable_generation_min")
    checks["renewable_policy_passed"] = (
        required_share is None or renewable_share + 1e-8 >= float(required_share)
    )
    kpis = {
        "objective_total_eur": objective,
        "reconstructed_total_cost_eur": reconstructed_cost,
        "annualized_capital_cost_eur": capital_cost,
        "annualized_operating_cost_eur": operational_cost,
        "demand_mwh": demand_mwh,
        "renewable_generation_mwh": renewable_mwh + hydro_mwh,
        "renewable_generation_share": renewable_share,
        "emissions_tco2": emissions,
        "curtailment_mwh": curtailment_mwh,
        "unmet_demand_mwh": unmet_mwh,
        "unmet_demand_share": unmet_share,
        "snapshot_count": len(snapshots),
        "annualization_factor": float(metadata.get("annualization_factor", 1.0)),
    }
    if not all(value for key, value in checks.items() if key.endswith("_passed")):
        failures = [key for key, value in checks.items() if key.endswith("_passed") and not value]
        raise ValueError(f"Optimization validation failed: {', '.join(failures)}")
    return checks, kpis


def _write_outputs(
    network: Any,
    *,
    output_path: Path,
    run_id: str,
    status: str,
    termination: str,
    solve_duration_seconds: float,
    config: OptimizationConfig,
    scenario: ScenarioRequest | None,
    metadata: dict[str, Any],
    checks: dict[str, Any],
    kpis: dict[str, Any],
) -> None:
    import pandas as pd

    output_path.mkdir(parents=True, exist_ok=False)
    capacity_rows: list[dict[str, Any]] = []
    for name, row in network.generators.iterrows():
        is_reliability_slack = row.carrier == "reliability"
        capacity_rows.append(
            {
                "component": "Generator",
                "asset": name,
                "carrier": row.carrier,
                "asset_role": "reliability_slack" if is_reliability_slack else "generation",
                "existing_capacity_mw": float(row.p_nom)
                if not row.p_nom_extendable and not is_reliability_slack
                else 0.0,
                "optimized_capacity_mw": float(row.p_nom_opt),
                "new_capacity_mw": float(row.p_nom_opt) if row.p_nom_extendable else 0.0,
                "capacity_limit_mw": float(row.p_nom) if is_reliability_slack else None,
            }
        )
    for name, row in network.storage_units.iterrows():
        capacity_rows.append(
            {
                "component": "StorageUnit",
                "asset": name,
                "carrier": row.carrier,
                "asset_role": "storage",
                "existing_capacity_mw": float(row.p_nom) if not row.p_nom_extendable else 0.0,
                "optimized_capacity_mw": float(row.p_nom_opt),
                "new_capacity_mw": float(row.p_nom_opt) if row.p_nom_extendable else 0.0,
                "energy_capacity_mwh": float(row.p_nom_opt) * float(row.max_hours),
            }
        )
    capacity = pd.DataFrame(capacity_rows)
    dispatch = network.generators_t.p.copy()
    dispatch.index.name = "timestamp_utc"
    dispatch = dispatch.reset_index().melt(
        id_vars="timestamp_utc", var_name="asset", value_name="dispatch_mw"
    )
    dispatch["carrier"] = dispatch.asset.map(network.generators.carrier)
    storage = network.storage_units_t.p.copy()
    storage.index.name = "timestamp_utc"
    storage = storage.reset_index().melt(
        id_vars="timestamp_utc", var_name="asset", value_name="net_dispatch_mw"
    )
    state = network.storage_units_t.state_of_charge.copy()
    state.index.name = "timestamp_utc"
    state = state.reset_index().melt(
        id_vars="timestamp_utc", var_name="asset", value_name="state_of_charge_mwh"
    )
    storage = storage.merge(state, on=["timestamp_utc", "asset"], how="outer")
    storage["carrier"] = storage.asset.map(network.storage_units.carrier)

    weights = network.snapshot_weightings.loc[network.snapshots, "objective"]
    generator_dispatch = network.generators_t.p.loc[network.snapshots]
    annual_rows: list[dict[str, Any]] = []
    for name, row in network.generators.iterrows():
        energy = float((generator_dispatch[name] * weights).sum())
        emission_factor = float(row.get("emissions_tco2_mwh", 0.0))
        if pd.isna(emission_factor):
            emission_factor = 0.0
        p_nom = float(row.p_nom_opt if row.p_nom_extendable else row.p_nom)
        if name in network.generators_t.p_max_pu.columns:
            availability = network.generators_t.p_max_pu.loc[network.snapshots, name]
        else:
            availability = float(row.p_max_pu)
        available_energy = float((availability * p_nom * weights).sum())
        annual_rows.append(
            {
                "component": "Generator",
                "asset": name,
                "carrier": row.carrier,
                "generation_mwh": energy,
                "available_energy_mwh": available_energy,
                "curtailment_mwh": max(0.0, available_energy - energy)
                if row.carrier in {"solar", "wind"}
                else 0.0,
                "capital_cost_eur": float(row.p_nom_opt) * float(row.capital_cost)
                if row.p_nom_extendable
                else 0.0,
                "operating_cost_eur": energy * float(row.marginal_cost),
                "emissions_tco2": energy * emission_factor,
            }
        )
    storage_dispatch = network.storage_units_t.p.loc[network.snapshots]
    for name, row in network.storage_units.iterrows():
        annual_rows.append(
            {
                "component": "StorageUnit",
                "asset": name,
                "carrier": row.carrier,
                "generation_mwh": float(
                    storage_dispatch[name].clip(lower=0).mul(weights).sum()
                ),
                "charging_mwh": float(
                    (-storage_dispatch[name].clip(upper=0)).mul(weights).sum()
                ),
                "available_energy_mwh": None,
                "curtailment_mwh": 0.0,
                "capital_cost_eur": float(row.p_nom_opt) * float(row.capital_cost)
                if row.p_nom_extendable
                else 0.0,
                "operating_cost_eur": 0.0,
                "emissions_tco2": 0.0,
            }
        )
    annual_summary = pd.DataFrame(annual_rows)
    demand = network.loads_t.p_set.loc[network.snapshots].sum(axis=1)
    hourly_balance = pd.DataFrame(
        {
            "timestamp_utc": network.snapshots,
            "demand_mw": demand.to_numpy(),
            "generator_supply_mw": generator_dispatch.sum(axis=1).to_numpy(),
            "storage_net_dispatch_mw": storage_dispatch.sum(axis=1).to_numpy(),
        }
    )
    hourly_balance["balance_error_mw"] = (
        hourly_balance.generator_supply_mw
        + hourly_balance.storage_net_dispatch_mw
        - hourly_balance.demand_mw
    )
    frames = (
        ("capacity", capacity),
        ("dispatch", dispatch),
        ("storage", storage),
        ("annual_summary", annual_summary),
        ("hourly_balance", hourly_balance),
    )
    output_files: dict[str, dict[str, Any]] = {}
    for name, frame in frames:
        frame.to_parquet(output_path / f"{name}.parquet", index=False)
        frame.to_csv(output_path / f"{name}.csv", index=False, encoding="utf-8")
        output_files[name] = {
            "rows": len(frame),
            "parquet_file": f"{name}.parquet",
            "parquet_sha256": _sha256_file(output_path / f"{name}.parquet"),
            "csv_file": f"{name}.csv",
            "csv_sha256": _sha256_file(output_path / f"{name}.csv"),
        }
    kpis_path = output_path / "kpis.json"
    kpis_path.write_text(
        json.dumps(kpis, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    output_files["kpis"] = {
        "json_file": "kpis.json",
        "json_sha256": _sha256_file(kpis_path),
    }
    manifest = {
        "run_id": run_id,
        "engine_version": ENGINE_VERSION,
        "model_version": config.model_version,
        "config_version": config.version,
        "generated_at_utc": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "solver": config.solver_name,
        "software_versions": {
            "pypsa": importlib.metadata.version("pypsa"),
            "highspy": importlib.metadata.version("highspy"),
        },
        "solve_duration_seconds": solve_duration_seconds,
        "solver_status": status,
        "termination_condition": termination,
        "scenario": scenario.model_dump(mode="json") if scenario else None,
        "input_metadata": metadata,
        "validation": checks,
        "kpis": kpis,
        "outputs": output_files,
    }
    (output_path / "manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )


def run_optimization(
    *,
    config: OptimizationConfig,
    mode: RunMode,
    output_root: Path,
    package_path: Path | None = None,
    scenario: ScenarioRequest | None = None,
    calendar_mode: CalendarMode = "planning",
) -> OptimizationResult:
    if mode == "toy":
        network, metadata = build_toy_network(config)
        scenario = None
        renewable_minimum = None
    else:
        if package_path is None or scenario is None:
            raise ValueError("Package path and scenario are required for week/full modes")
        network, metadata = build_package_network(
            package_path=package_path,
            scenario=scenario,
            config=config,
            mode=mode,
            calendar_mode=calendar_mode,
        )
        renewable_minimum = scenario.renewable_generation_min
    run_payload = {
        "engine_version": ENGINE_VERSION,
        "model_version": config.model_version,
        "config": config.model_dump(mode="json"),
        "mode": mode,
        "calendar_mode": calendar_mode,
        "scenario": scenario.model_dump(mode="json") if scenario else None,
        "metadata": metadata,
    }
    run_id = hashlib.sha256(
        json.dumps(run_payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()[:16]
    scenario_id = "toy" if scenario is None else hashlib.sha256(
        json.dumps(scenario.model_dump(mode="json"), sort_keys=True).encode()
    ).hexdigest()[:16]
    output_path = output_root / f"scenario_id={scenario_id}" / f"run_id={run_id}"
    if (output_path / "manifest.json").exists():
        manifest = json.loads((output_path / "manifest.json").read_text(encoding="utf-8"))
        return OptimizationResult(
            run_id=run_id,
            output_path=output_path,
            status=manifest["solver_status"],
            termination_condition=manifest["termination_condition"],
            kpis=manifest["kpis"],
        )
    status, termination, solve_duration_seconds = solve_network(
        network,
        solver_name=config.solver_name,
        renewable_generation_min=renewable_minimum,
    )
    checks, kpis = validate_and_summarize(
        network, config=config, metadata=metadata
    )
    _write_outputs(
        network,
        output_path=output_path,
        run_id=run_id,
        status=status,
        termination=termination,
        solve_duration_seconds=solve_duration_seconds,
        config=config,
        scenario=scenario,
        metadata=metadata,
        checks=checks,
        kpis=kpis,
    )
    return OptimizationResult(run_id, output_path, status, termination, kpis)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run Atlas PyPSA/HiGHS optimization")
    parser.add_argument("--mode", choices=("toy", "week", "full"), default="toy")
    parser.add_argument("--calendar", choices=("planning", "baseline"), default="planning")
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("configs/optimization/national_baseline.yml"),
    )
    parser.add_argument(
        "--scenario", type=Path, default=Path("configs/scenarios/baseline-2030.yml")
    )
    parser.add_argument("--input-package", type=Path)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    settings = get_settings()
    logging.basicConfig(
        level=settings.log_level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    config = load_optimization_config(args.config)
    package = args.input_package or find_latest_package(settings.data_root)
    scenario = None if args.mode == "toy" else load_scenario(args.scenario)
    result = run_optimization(
        config=config,
        mode=args.mode,
        output_root=settings.data_root / "optimization",
        package_path=package,
        scenario=scenario,
        calendar_mode=args.calendar,
    )
    LOGGER.info(
        "Optimization complete: run_id=%s status=%s termination=%s path=%s kpis=%s",
        result.run_id,
        result.status,
        result.termination_condition,
        result.output_path,
        result.kpis,
    )


if __name__ == "__main__":
    main()
