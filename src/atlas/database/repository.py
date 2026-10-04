import uuid
from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session, sessionmaker

from atlas.database.models import OutboxEventRow, ScenarioRequestRow, ScenarioRunRow
from atlas.schemas.scenario import JobRecord, ScenarioAccepted, ScenarioRecord, ScenarioRequest

SCENARIO_REQUESTED_TOPIC = "atlas.scenario.requested.v1"


class PostgresScenarioRepository:
    """Persist scenario requests, runs, and outbox events in one transaction."""

    def __init__(self, session_factory: sessionmaker[Session]) -> None:
        self._session_factory = session_factory

    def submit(
        self,
        request: ScenarioRequest,
        *,
        request_hash: str,
        model_version: str,
        data_version: str,
    ) -> ScenarioAccepted:
        scenario_id = f"scn_{request_hash[:16]}"
        job_id = f"job_{request_hash[:16]}"

        with self._session_factory.begin() as session:
            now = datetime.now(UTC)
            request_payload = request.model_dump(mode="json")
            inserted_scenario_id = session.scalar(
                insert(ScenarioRequestRow)
                .values(
                    scenario_id=scenario_id,
                    request_hash=request_hash,
                    name=request.name,
                    request_payload=request_payload,
                    model_version=model_version,
                    data_version=data_version,
                    created_at=now,
                )
                .on_conflict_do_nothing(index_elements=[ScenarioRequestRow.request_hash])
                .returning(ScenarioRequestRow.scenario_id)
            )
            if inserted_scenario_id is None:
                existing = session.scalar(
                    select(ScenarioRequestRow).where(
                        ScenarioRequestRow.request_hash == request_hash
                    )
                )
                if existing is None:
                    raise RuntimeError("Scenario conflict occurred but no existing row was found")
                run = self._latest_run(session, existing.scenario_id)
                return ScenarioAccepted(
                    scenario_id=existing.scenario_id,
                    job_id=run.job_id,
                    status=run.status,
                    stage=run.stage,
                    reused=True,
                )

            event_id = uuid.uuid4()
            event = {
                "event_id": str(event_id),
                "event_type": "scenario.requested",
                "schema_version": 1,
                "event_time": now.isoformat().replace("+00:00", "Z"),
                "producer": "atlas-api",
                "key": scenario_id,
                "trace_id": str(uuid.uuid4()),
                "payload": {
                    "scenario_id": scenario_id,
                    "request_hash": request_hash,
                    "model_version": model_version,
                    "data_version": data_version,
                    "request": request_payload,
                },
            }

            session.add_all(
                [
                    ScenarioRunRow(
                        job_id=job_id,
                        scenario_id=scenario_id,
                        status="queued",
                        stage="queued",
                        created_at=now,
                    ),
                    OutboxEventRow(
                        event_id=event_id,
                        aggregate_id=scenario_id,
                        event_type="scenario.requested",
                        topic=SCENARIO_REQUESTED_TOPIC,
                        event_key=scenario_id,
                        schema_version=1,
                        payload=event,
                        created_at=now,
                        publish_attempts=0,
                    ),
                ]
            )

        return ScenarioAccepted(
            scenario_id=scenario_id,
            job_id=job_id,
            status="queued",
            stage="queued",
            reused=False,
        )

    def list(self) -> list[ScenarioRecord]:
        with self._session_factory() as session:
            requests = session.scalars(
                select(ScenarioRequestRow).order_by(ScenarioRequestRow.created_at.desc())
            ).all()
            return [self._to_record(session, row) for row in requests]

    def get(self, scenario_id: str) -> ScenarioRecord | None:
        with self._session_factory() as session:
            request = session.get(ScenarioRequestRow, scenario_id)
            if request is None:
                return None
            return self._to_record(session, request)

    def get_job(self, job_id: str) -> JobRecord | None:
        with self._session_factory() as session:
            run = session.get(ScenarioRunRow, job_id)
            if run is None:
                return None
            request = session.get(ScenarioRequestRow, run.scenario_id)
            if request is None:
                raise RuntimeError(f"Scenario request {run.scenario_id} was not found")
            return self._to_job_record(request, run)

    @staticmethod
    def _latest_run(session: Session, scenario_id: str) -> ScenarioRunRow:
        run = session.scalar(
            select(ScenarioRunRow)
            .where(ScenarioRunRow.scenario_id == scenario_id)
            .order_by(ScenarioRunRow.created_at.desc())
            .limit(1)
        )
        if run is None:
            raise RuntimeError(f"Scenario {scenario_id} has no run record")
        return run

    def _to_record(self, session: Session, row: ScenarioRequestRow) -> ScenarioRecord:
        run = self._latest_run(session, row.scenario_id)
        return ScenarioRecord(
            scenario_id=row.scenario_id,
            job_id=run.job_id,
            optimizer_run_id=run.optimizer_run_id,
            request=ScenarioRequest.model_validate(row.request_payload),
            request_hash=row.request_hash,
            model_version=row.model_version,
            data_version=row.data_version,
            status=run.status,
            stage=run.stage,
            processing_attempts=run.processing_attempts,
            max_attempts=run.max_attempts,
            started_at=run.started_at,
            optimized_at=run.optimized_at,
            result_published_at=run.result_published_at,
            analytics_ready_at=run.analytics_ready_at,
            completed_at=run.completed_at,
            failed_at=run.failed_at,
            failure_stage=run.failure_stage,
            error_type=run.error_type,
            error_message=run.error_message,
            created_at=row.created_at,
        )

    @staticmethod
    def _to_job_record(request: ScenarioRequestRow, run: ScenarioRunRow) -> JobRecord:
        return JobRecord(
            job_id=run.job_id,
            scenario_id=run.scenario_id,
            optimizer_run_id=run.optimizer_run_id,
            status=run.status,
            stage=run.stage,
            request=ScenarioRequest.model_validate(request.request_payload),
            request_hash=request.request_hash,
            model_version=request.model_version,
            data_version=request.data_version,
            processing_attempts=run.processing_attempts,
            max_attempts=run.max_attempts,
            created_at=run.created_at,
            started_at=run.started_at,
            last_attempt_at=run.last_attempt_at,
            optimized_at=run.optimized_at,
            result_published_at=run.result_published_at,
            analytics_ready_at=run.analytics_ready_at,
            completed_at=run.completed_at,
            failed_at=run.failed_at,
            failure_stage=run.failure_stage,
            error_type=run.error_type,
            error_message=run.error_message,
        )
