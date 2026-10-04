from datetime import datetime
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field

Share = Annotated[float, Field(ge=0, le=1)]
PositiveMultiplier = Annotated[float, Field(gt=0, le=5)]
JobStatus = Literal["queued", "running", "completed", "failed"]
JobStage = Literal[
    "queued",
    "optimizing",
    "optimized",
    "publishing",
    "published",
    "building_analytics",
    "analytics_ready",
    "completed",
    "failed",
]


class ScenarioRequest(BaseModel):
    """Validated decision assumptions supplied by a user."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    name: Annotated[str, Field(min_length=3, max_length=120)]
    planning_year: Annotated[int, Field(ge=2024, le=2100)] = 2030
    weather_year: Annotated[int, Field(ge=1980, le=2100)] = 2024
    demand_growth: Annotated[float, Field(ge=-0.2, le=0.3)] = 0.0
    gas_price_eur_mwh_th: Annotated[float, Field(ge=0, le=1000)] = 65.0
    carbon_price_eur_t: Annotated[float, Field(ge=0, le=1000)] = 50.0
    renewable_generation_min: Share = 0.5
    battery_capex_multiplier: PositiveMultiplier = 1.0
    solar_capex_multiplier: PositiveMultiplier = 1.0
    wind_capex_multiplier: PositiveMultiplier = 1.0


class ScenarioProvenance(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    classification: Literal["ASSUMPTION"]
    version: Annotated[str, Field(min_length=1, max_length=40)]
    note: Annotated[str, Field(min_length=10, max_length=600)]


class ScenarioPreset(BaseModel):
    preset_id: Annotated[str, Field(pattern=r"^[a-z0-9]+(?:-[a-z0-9]+)*$")]
    request: ScenarioRequest
    provenance: ScenarioProvenance


class ScenarioAccepted(BaseModel):
    scenario_id: str
    job_id: str
    status: JobStatus
    stage: JobStage
    reused: bool


class ScenarioRecord(BaseModel):
    scenario_id: str
    job_id: str
    optimizer_run_id: str | None = None
    request: ScenarioRequest
    request_hash: str
    model_version: str
    data_version: str
    status: JobStatus
    stage: JobStage
    processing_attempts: int
    max_attempts: int
    started_at: datetime | None = None
    optimized_at: datetime | None = None
    result_published_at: datetime | None = None
    analytics_ready_at: datetime | None = None
    completed_at: datetime | None = None
    failed_at: datetime | None = None
    failure_stage: JobStage | None = None
    error_type: str | None = None
    error_message: str | None = None
    created_at: datetime


class JobRecord(BaseModel):
    job_id: str
    scenario_id: str
    optimizer_run_id: str | None = None
    status: JobStatus
    stage: JobStage
    request: ScenarioRequest
    request_hash: str
    model_version: str
    data_version: str
    processing_attempts: int
    max_attempts: int
    created_at: datetime
    started_at: datetime | None = None
    last_attempt_at: datetime | None = None
    optimized_at: datetime | None = None
    result_published_at: datetime | None = None
    analytics_ready_at: datetime | None = None
    completed_at: datetime | None = None
    failed_at: datetime | None = None
    failure_stage: JobStage | None = None
    error_type: str | None = None
    error_message: str | None = None
