import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Literal

from sqlalchemy import case, select, update
from sqlalchemy.orm import Session, sessionmaker

from atlas.database.models import OutboxEventRow, ScenarioRunRow

RunStatus = Literal["queued", "running", "completed", "failed"]
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


@dataclass(frozen=True)
class RunClaim:
    job_id: str
    status: RunStatus
    claimed: bool
    attempt: int
    max_attempts: int
    stage: JobStage


@dataclass(frozen=True)
class RunFailure:
    retryable: bool
    attempt: int
    max_attempts: int
    failure_stage: JobStage


class PostgresScenarioRunRepository:
    """Own atomic state transitions for asynchronously executed scenario runs."""

    def __init__(self, session_factory: sessionmaker[Session]) -> None:
        self._session_factory = session_factory

    def claim(self, scenario_id: str) -> RunClaim:
        now = datetime.now(UTC)
        with self._session_factory.begin() as session:
            claimed = session.execute(
                update(ScenarioRunRow)
                .where(
                    ScenarioRunRow.scenario_id == scenario_id,
                    # Reclaiming `running` is required after a worker process dies before
                    # committing its Kafka offset. Processing remains idempotent downstream.
                    ScenarioRunRow.status.in_(["queued", "running", "failed"]),
                    ScenarioRunRow.processing_attempts < ScenarioRunRow.max_attempts,
                )
                .values(
                    status="running",
                    stage="optimizing",
                    started_at=now,
                    completed_at=None,
                    last_attempt_at=now,
                    processing_attempts=ScenarioRunRow.processing_attempts + 1,
                )
                .returning(
                    ScenarioRunRow.job_id,
                    ScenarioRunRow.processing_attempts,
                    ScenarioRunRow.max_attempts,
                    ScenarioRunRow.stage,
                )
            ).one_or_none()
            if claimed is not None:
                job_id, attempt, max_attempts, stage = claimed
                return RunClaim(
                    job_id=job_id,
                    status="running",
                    claimed=True,
                    attempt=attempt,
                    max_attempts=max_attempts,
                    stage=stage,
                )

            run = self._latest_run(session, scenario_id)
            return RunClaim(
                job_id=run.job_id,
                status=run.status,
                claimed=False,
                attempt=run.processing_attempts,
                max_attempts=run.max_attempts,
                stage=run.stage,
            )

    def mark_optimized(self, job_id: str) -> None:
        self._transition(job_id, expected="optimizing", target="optimized", optimized_at=True)

    def mark_publishing(self, job_id: str) -> None:
        self._transition(job_id, expected="optimized", target="publishing")

    def mark_published(self, job_id: str, optimizer_run_id: str) -> None:
        self._transition(
            job_id,
            expected="publishing",
            target="published",
            optimizer_run_id=optimizer_run_id,
            result_published_at=True,
        )

    def mark_building_analytics(self, job_id: str) -> None:
        self._transition(job_id, expected="published", target="building_analytics")

    def mark_analytics_ready(self, job_id: str) -> None:
        self._transition(
            job_id,
            expected="building_analytics",
            target="analytics_ready",
            analytics_ready_at=True,
        )

    def complete(self, job_id: str, optimizer_run_id: str) -> None:
        with self._session_factory.begin() as session:
            completed = session.execute(
                update(ScenarioRunRow)
                .where(
                    ScenarioRunRow.job_id == job_id,
                    ScenarioRunRow.status == "running",
                    ScenarioRunRow.stage == "analytics_ready",
                )
                .values(
                    status="completed",
                    stage="completed",
                    completed_at=datetime.now(UTC),
                    failed_at=None,
                    failure_stage=None,
                    error_type=None,
                    error_message=None,
                    optimizer_run_id=optimizer_run_id,
                )
                .returning(ScenarioRunRow.scenario_id)
            ).scalar_one_or_none()
            if completed is None:
                raise RuntimeError(f"Running scenario job {job_id} was not found")
            event_id = uuid.uuid4()
            now = datetime.now(UTC)
            event = {
                "event_id": str(event_id),
                "event_type": "scenario.completed",
                "schema_version": 1,
                "event_time": now.isoformat().replace("+00:00", "Z"),
                "producer": "atlas-scenario-worker",
                "key": completed,
                "trace_id": str(uuid.uuid4()),
                "payload": {
                    "scenario_id": completed,
                    "job_id": job_id,
                    "optimizer_run_id": optimizer_run_id,
                },
            }
            session.add(
                OutboxEventRow(
                    event_id=event_id,
                    aggregate_id=completed,
                    event_type="scenario.completed",
                    topic="atlas.scenario.completed.v1",
                    event_key=completed,
                    schema_version=1,
                    payload=event,
                    created_at=now,
                    publish_attempts=0,
                )
            )

    def fail(
        self,
        job_id: str,
        error_message: str,
        error_type: str = "RuntimeError",
    ) -> RunFailure:
        now = datetime.now(UTC)
        with self._session_factory.begin() as session:
            retryable = ScenarioRunRow.processing_attempts < ScenarioRunRow.max_attempts
            result = session.execute(
                update(ScenarioRunRow)
                .where(ScenarioRunRow.job_id == job_id, ScenarioRunRow.status == "running")
                .values(
                    status=case((retryable, "queued"), else_="failed"),
                    stage=case((retryable, "queued"), else_="failed"),
                    completed_at=case((retryable, None), else_=now),
                    failed_at=now,
                    failure_stage=ScenarioRunRow.stage,
                    error_type=error_type[:200],
                    error_message=error_message[:2_000],
                )
                .returning(
                    ScenarioRunRow.processing_attempts,
                    ScenarioRunRow.max_attempts,
                    ScenarioRunRow.failure_stage,
                )
            ).one_or_none()
            if result is None:
                raise RuntimeError(f"Running scenario job {job_id} was not found")
            attempt, max_attempts, failure_stage = result
            return RunFailure(
                retryable=attempt < max_attempts,
                attempt=attempt,
                max_attempts=max_attempts,
                failure_stage=failure_stage,
            )

    def _transition(
        self,
        job_id: str,
        *,
        expected: JobStage,
        target: JobStage,
        optimizer_run_id: str | None = None,
        optimized_at: bool = False,
        result_published_at: bool = False,
        analytics_ready_at: bool = False,
    ) -> None:
        now = datetime.now(UTC)
        values: dict[str, object] = {"stage": target}
        if optimizer_run_id is not None:
            values["optimizer_run_id"] = optimizer_run_id
        if optimized_at:
            values["optimized_at"] = now
        if result_published_at:
            values["result_published_at"] = now
        if analytics_ready_at:
            values["analytics_ready_at"] = now
        with self._session_factory.begin() as session:
            transitioned = session.scalar(
                update(ScenarioRunRow)
                .where(
                    ScenarioRunRow.job_id == job_id,
                    ScenarioRunRow.status == "running",
                    ScenarioRunRow.stage == expected,
                )
                .values(**values)
                .returning(ScenarioRunRow.job_id)
            )
            if transitioned is None:
                raise RuntimeError(
                    f"Scenario job {job_id} cannot transition from {expected} to {target}"
                )

    @staticmethod
    def _latest_run(session: Session, scenario_id: str) -> ScenarioRunRow:
        run = session.scalar(
            select(ScenarioRunRow)
            .where(ScenarioRunRow.scenario_id == scenario_id)
            .order_by(ScenarioRunRow.created_at.desc())
            .limit(1)
        )
        if run is None:
            raise LookupError(f"Scenario {scenario_id} has no run record")
        return run
