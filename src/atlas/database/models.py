import uuid
from datetime import datetime

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class ScenarioRequestRow(Base):
    __tablename__ = "scenario_requests"
    __table_args__ = (
        CheckConstraint("request_hash ~ '^[a-f0-9]{64}$'", name="scenario_request_hash_format"),
        {"schema": "optimization"},
    )

    scenario_id: Mapped[str] = mapped_column(Text, primary_key=True)
    request_hash: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    request_payload: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False)
    model_version: Mapped[str] = mapped_column(Text, nullable=False)
    data_version: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=text("now()"),
    )


class ScenarioRunRow(Base):
    __tablename__ = "scenario_runs"
    __table_args__ = (
        CheckConstraint(
            "status in ('queued', 'running', 'completed', 'failed')",
            name="scenario_run_status",
        ),
        Index("idx_scenario_runs_scenario", "scenario_id"),
        Index("idx_scenario_runs_status", "status"),
        {"schema": "optimization"},
    )

    job_id: Mapped[str] = mapped_column(Text, primary_key=True)
    scenario_id: Mapped[str] = mapped_column(
        ForeignKey("optimization.scenario_requests.scenario_id"),
        nullable=False,
    )
    status: Mapped[str] = mapped_column(Text, nullable=False, server_default="queued")
    stage: Mapped[str] = mapped_column(Text, nullable=False, server_default="queued")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=text("now()"),
    )
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    optimized_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    result_published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    analytics_ready_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    failed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    failure_stage: Mapped[str | None] = mapped_column(Text)
    error_type: Mapped[str | None] = mapped_column(Text)
    error_message: Mapped[str | None] = mapped_column(Text)
    optimizer_run_id: Mapped[str | None] = mapped_column(
        ForeignKey("optimization.optimization_runs.run_id")
    )
    processing_attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    max_attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=3)
    last_attempt_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class OutboxEventRow(Base):
    __tablename__ = "outbox_events"
    __table_args__ = (
        CheckConstraint("schema_version > 0", name="outbox_schema_version_positive"),
        CheckConstraint("publish_attempts >= 0", name="outbox_publish_attempts_positive"),
        {"schema": "optimization"},
    )

    event_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    aggregate_id: Mapped[str] = mapped_column(Text, nullable=False)
    event_type: Mapped[str] = mapped_column(Text, nullable=False)
    topic: Mapped[str] = mapped_column(Text, nullable=False)
    event_key: Mapped[str] = mapped_column(Text, nullable=False)
    schema_version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    payload: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=text("now()"),
    )
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    publish_attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_error: Mapped[str | None] = mapped_column(Text)


class OptimizationRunRow(Base):
    __tablename__ = "optimization_runs"
    __table_args__ = ({"schema": "optimization"},)

    run_id: Mapped[str] = mapped_column(Text, primary_key=True)
    scenario_id: Mapped[str] = mapped_column(Text, nullable=False, index=True)
    scenario_name: Mapped[str | None] = mapped_column(Text)
    status: Mapped[str] = mapped_column(Text, nullable=False)
    termination_condition: Mapped[str] = mapped_column(Text, nullable=False)
    planning_year: Mapped[int | None] = mapped_column(Integer)
    weather_year: Mapped[int | None] = mapped_column(Integer)
    snapshot_count: Mapped[int] = mapped_column(Integer, nullable=False)
    annualization_factor: Mapped[float] = mapped_column(Float, nullable=False, default=1.0)
    objective_total_eur: Mapped[float] = mapped_column(Float, nullable=False)
    annualized_capital_cost_eur: Mapped[float] = mapped_column(Float, nullable=False)
    annualized_operating_cost_eur: Mapped[float] = mapped_column(Float, nullable=False)
    demand_mwh: Mapped[float] = mapped_column(Float, nullable=False)
    renewable_generation_mwh: Mapped[float] = mapped_column(Float, nullable=False)
    renewable_generation_share: Mapped[float] = mapped_column(Float, nullable=False)
    emissions_tco2: Mapped[float] = mapped_column(Float, nullable=False)
    curtailment_mwh: Mapped[float] = mapped_column(Float, nullable=False)
    unmet_demand_mwh: Mapped[float] = mapped_column(Float, nullable=False)
    unmet_demand_share: Mapped[float] = mapped_column(Float, nullable=False)
    solver_name: Mapped[str] = mapped_column(Text, nullable=False)
    solver_version: Mapped[str] = mapped_column(Text, nullable=False)
    model_version: Mapped[str] = mapped_column(Text, nullable=False)
    engine_version: Mapped[str] = mapped_column(Text, nullable=False)
    config_version: Mapped[str] = mapped_column(Text, nullable=False)
    input_package_version: Mapped[str | None] = mapped_column(Text)
    run_mode: Mapped[str] = mapped_column(Text, nullable=False, default="unknown")
    calendar_mode: Mapped[str] = mapped_column(Text, nullable=False, default="unknown")
    publication_state: Mapped[str] = mapped_column(Text, nullable=False, default="published")
    is_current: Mapped[bool] = mapped_column(nullable=False, default=True)
    superseded_by_run_id: Mapped[str | None] = mapped_column(
        ForeignKey("optimization.optimization_runs.run_id")
    )
    solve_duration_seconds: Mapped[float] = mapped_column(Float, nullable=False)
    artifact_root: Mapped[str] = mapped_column(Text, nullable=False)
    manifest_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    generated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    published_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )


class ScenarioInputRow(Base):
    __tablename__ = "scenario_inputs"
    __table_args__ = ({"schema": "optimization"},)

    run_id: Mapped[str] = mapped_column(
        ForeignKey("optimization.optimization_runs.run_id", ondelete="CASCADE"),
        primary_key=True,
    )
    demand_growth: Mapped[float | None] = mapped_column(Float)
    gas_price_eur_mwh_th: Mapped[float | None] = mapped_column(Float)
    carbon_price_eur_t: Mapped[float | None] = mapped_column(Float)
    renewable_generation_min: Mapped[float | None] = mapped_column(Float)
    battery_capex_multiplier: Mapped[float | None] = mapped_column(Float)
    solar_capex_multiplier: Mapped[float | None] = mapped_column(Float)
    wind_capex_multiplier: Mapped[float | None] = mapped_column(Float)
    input_payload: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False)


class RunArtifactRow(Base):
    __tablename__ = "run_artifacts"
    __table_args__ = ({"schema": "optimization"},)

    run_id: Mapped[str] = mapped_column(
        ForeignKey("optimization.optimization_runs.run_id", ondelete="CASCADE"),
        primary_key=True,
    )
    artifact_name: Mapped[str] = mapped_column(Text, primary_key=True)
    artifact_format: Mapped[str] = mapped_column(Text, primary_key=True)
    relative_path: Mapped[str] = mapped_column(Text, nullable=False)
    sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    row_count: Mapped[int | None] = mapped_column(BigInteger)
    byte_count: Mapped[int] = mapped_column(BigInteger, nullable=False)


class CapacityResultRow(Base):
    __tablename__ = "capacity_results"
    __table_args__ = ({"schema": "optimization"},)

    run_id: Mapped[str] = mapped_column(
        ForeignKey("optimization.optimization_runs.run_id", ondelete="CASCADE"),
        primary_key=True,
    )
    component: Mapped[str] = mapped_column(Text, primary_key=True)
    asset: Mapped[str] = mapped_column(Text, primary_key=True)
    carrier: Mapped[str] = mapped_column(Text, nullable=False)
    asset_role: Mapped[str] = mapped_column(Text, nullable=False)
    existing_capacity_mw: Mapped[float] = mapped_column(Float, nullable=False)
    optimized_capacity_mw: Mapped[float] = mapped_column(Float, nullable=False)
    new_capacity_mw: Mapped[float] = mapped_column(Float, nullable=False)
    capacity_limit_mw: Mapped[float | None] = mapped_column(Float)
    energy_capacity_mwh: Mapped[float | None] = mapped_column(Float)


class DispatchHourlyRow(Base):
    __tablename__ = "dispatch_hourly"
    __table_args__ = ({"schema": "optimization"},)

    run_id: Mapped[str] = mapped_column(
        ForeignKey("optimization.optimization_runs.run_id", ondelete="CASCADE"),
        primary_key=True,
    )
    timestamp_utc: Mapped[datetime] = mapped_column(DateTime(timezone=False), primary_key=True)
    asset: Mapped[str] = mapped_column(Text, primary_key=True)
    carrier: Mapped[str] = mapped_column(Text, nullable=False)
    dispatch_mw: Mapped[float] = mapped_column(Float, nullable=False)


class StorageHourlyRow(Base):
    __tablename__ = "storage_hourly"
    __table_args__ = ({"schema": "optimization"},)

    run_id: Mapped[str] = mapped_column(
        ForeignKey("optimization.optimization_runs.run_id", ondelete="CASCADE"),
        primary_key=True,
    )
    timestamp_utc: Mapped[datetime] = mapped_column(DateTime(timezone=False), primary_key=True)
    asset: Mapped[str] = mapped_column(Text, primary_key=True)
    carrier: Mapped[str] = mapped_column(Text, nullable=False)
    net_dispatch_mw: Mapped[float] = mapped_column(Float, nullable=False)
    state_of_charge_mwh: Mapped[float] = mapped_column(Float, nullable=False)


class CostComponentRow(Base):
    __tablename__ = "cost_components"
    __table_args__ = ({"schema": "optimization"},)

    run_id: Mapped[str] = mapped_column(
        ForeignKey("optimization.optimization_runs.run_id", ondelete="CASCADE"),
        primary_key=True,
    )
    asset: Mapped[str] = mapped_column(Text, primary_key=True)
    carrier: Mapped[str] = mapped_column(Text, nullable=False)
    component_type: Mapped[str] = mapped_column(Text, primary_key=True)
    amount_eur: Mapped[float] = mapped_column(Float, nullable=False)


class RunProvenanceRow(Base):
    __tablename__ = "run_provenance"
    __table_args__ = ({"schema": "optimization"},)

    run_id: Mapped[str] = mapped_column(
        ForeignKey("optimization.optimization_runs.run_id", ondelete="CASCADE"),
        primary_key=True,
    )
    provenance_key: Mapped[str] = mapped_column(Text, primary_key=True)
    provenance_value: Mapped[object] = mapped_column(JSONB, nullable=False)
