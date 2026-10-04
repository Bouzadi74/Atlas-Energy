from datetime import UTC, datetime

from pydantic import BaseModel, ConfigDict, Field, JsonValue, field_validator


class ResultModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class HourlyResult(ResultModel):
    timestamp_utc: datetime

    @field_validator("timestamp_utc")
    @classmethod
    def normalize_utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)


class ScenarioKpi(ResultModel):
    run_id: str
    scenario_id: str
    scenario_name: str | None
    planning_year: int | None
    weather_year: int | None
    annual_system_cost_eur: float
    average_cost_eur_mwh: float
    annualized_capital_cost_eur: float
    annualized_operating_cost_eur: float
    demand_mwh: float
    renewable_generation_mwh: float
    renewable_generation_share: float
    imports_mwh: float
    import_share: float
    reservoir_hydro_mwh: float
    battery_discharge_mwh: float
    pumped_hydro_discharge_mwh: float
    emissions_tco2: float
    curtailment_mwh: float
    unserved_mwh: float
    unserved_energy_share: float
    balanced_demand_mwh: float
    solve_duration_seconds: float
    model_version: str
    engine_version: str
    input_package_version: str | None


class CapacityResult(ResultModel):
    run_id: str
    scenario_id: str
    scenario_name: str | None
    component: str
    asset: str
    carrier: str
    asset_role: str
    existing_capacity_mw: float
    new_capacity_mw: float
    optimized_capacity_mw: float
    energy_capacity_mwh: float | None


class DispatchPoint(HourlyResult):
    run_id: str
    scenario_id: str
    asset: str
    carrier: str
    dispatch_mw: float


class StoragePoint(HourlyResult):
    run_id: str
    scenario_id: str
    asset: str
    carrier: str
    net_dispatch_mw: float
    state_of_charge_mwh: float


class CostResult(ResultModel):
    run_id: str
    scenario_id: str
    scenario_name: str | None
    asset: str
    carrier: str
    component_type: str
    amount_eur: float


class ProvenanceRecord(ResultModel):
    run_id: str
    scenario_id: str
    scenario_name: str | None
    provenance_key: str
    provenance_value: JsonValue


class ScenarioComparison(ResultModel):
    base_run_id: str
    comparison_run_id: str
    base_scenario_name: str | None
    comparison_scenario_name: str | None
    annual_cost_delta_eur: float
    average_cost_delta_eur_mwh: float
    renewable_share_delta: float
    emissions_delta_tco2: float
    curtailment_delta_mwh: float
    unserved_delta_mwh: float
    imports_delta_mwh: float


class DispatchPage(ResultModel):
    items: list[DispatchPoint]
    limit: int = Field(ge=1)
    offset: int = Field(ge=0)
    returned: int = Field(ge=0)
    has_more: bool


class StoragePage(ResultModel):
    items: list[StoragePoint]
    limit: int = Field(ge=1)
    offset: int = Field(ge=0)
    returned: int = Field(ge=0)
    has_more: bool
