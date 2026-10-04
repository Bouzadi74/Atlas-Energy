import hashlib
import json
from datetime import UTC, datetime
from typing import Protocol

from atlas.database.repository import PostgresScenarioRepository
from atlas.schemas.scenario import JobRecord, ScenarioAccepted, ScenarioRecord, ScenarioRequest


def scenario_hash(request: ScenarioRequest, *, model_version: str, data_version: str) -> str:
    canonical = {
        "data_version": data_version,
        "model_version": model_version,
        "request": request.model_dump(mode="json"),
    }
    payload = json.dumps(canonical, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(payload).hexdigest()


class ScenarioService(Protocol):
    def submit(self, request: ScenarioRequest) -> ScenarioAccepted: ...

    def list(self) -> list[ScenarioRecord]: ...

    def get(self, scenario_id: str) -> ScenarioRecord | None: ...

    def get_job(self, job_id: str) -> JobRecord | None: ...


class PostgresScenarioService:
    """Scenario application service backed by the transactional PostgreSQL repository."""

    def __init__(
        self,
        repository: PostgresScenarioRepository,
        *,
        model_version: str,
        data_version: str,
    ) -> None:
        self.repository = repository
        self.model_version = model_version
        self.data_version = data_version

    def submit(self, request: ScenarioRequest) -> ScenarioAccepted:
        request_hash = scenario_hash(
            request,
            model_version=self.model_version,
            data_version=self.data_version,
        )
        return self.repository.submit(
            request,
            request_hash=request_hash,
            model_version=self.model_version,
            data_version=self.data_version,
        )

    def list(self) -> list[ScenarioRecord]:
        return self.repository.list()

    def get(self, scenario_id: str) -> ScenarioRecord | None:
        return self.repository.get(scenario_id)

    def get_job(self, job_id: str) -> JobRecord | None:
        return self.repository.get_job(job_id)


class InMemoryScenarioService:
    """Fast isolated implementation used by unit tests."""

    def __init__(self, *, model_version: str, data_version: str) -> None:
        self.model_version = model_version
        self.data_version = data_version
        self._records: dict[str, ScenarioRecord] = {}

    def submit(self, request: ScenarioRequest) -> ScenarioAccepted:
        request_hash = scenario_hash(
            request,
            model_version=self.model_version,
            data_version=self.data_version,
        )
        scenario_id = f"scn_{request_hash[:16]}"
        reused = scenario_id in self._records
        if not reused:
            self._records[scenario_id] = ScenarioRecord(
                scenario_id=scenario_id,
                job_id=f"job_{request_hash[:16]}",
                request=request,
                request_hash=request_hash,
                model_version=self.model_version,
                data_version=self.data_version,
                status="queued",
                stage="queued",
                processing_attempts=0,
                max_attempts=3,
                created_at=datetime.now(UTC),
            )
        record = self._records[scenario_id]
        return ScenarioAccepted(
            scenario_id=scenario_id,
            job_id=record.job_id,
            status=record.status,
            stage=record.stage,
            reused=reused,
        )

    def list(self) -> list[ScenarioRecord]:
        return sorted(self._records.values(), key=lambda item: item.created_at, reverse=True)

    def get(self, scenario_id: str) -> ScenarioRecord | None:
        return self._records.get(scenario_id)

    def get_job(self, job_id: str) -> JobRecord | None:
        record = next(
            (item for item in self._records.values() if item.job_id == job_id),
            None,
        )
        if record is None:
            return None
        return JobRecord(
            job_id=record.job_id,
            scenario_id=record.scenario_id,
            optimizer_run_id=record.optimizer_run_id,
            status=record.status,
            stage=record.stage,
            request=record.request,
            request_hash=record.request_hash,
            model_version=record.model_version,
            data_version=record.data_version,
            processing_attempts=record.processing_attempts,
            max_attempts=record.max_attempts,
            created_at=record.created_at,
            started_at=record.started_at,
            optimized_at=record.optimized_at,
            result_published_at=record.result_published_at,
            analytics_ready_at=record.analytics_ready_at,
            completed_at=record.completed_at,
            failed_at=record.failed_at,
            failure_stage=record.failure_stage,
            error_type=record.error_type,
            error_message=record.error_message,
        )
