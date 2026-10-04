import os

import pytest
from sqlalchemy import delete

from atlas.config import get_settings
from atlas.database import (
    PostgresScenarioRepository,
    PostgresScenarioRunRepository,
    build_engine,
    build_session_factory,
)
from atlas.database.models import OutboxEventRow, ScenarioRequestRow, ScenarioRunRow
from atlas.events.outbox_publisher import OutboxPublisher
from atlas.scenarios.service import PostgresScenarioService
from atlas.schemas.scenario import ScenarioRequest

pytestmark = pytest.mark.skipif(
    os.getenv("ATLAS_RUN_INTEGRATION_TESTS") != "1",
    reason="Set ATLAS_RUN_INTEGRATION_TESTS=1 to test against local PostgreSQL",
)


class RecordingProducer:
    def __init__(self) -> None:
        self.events: list[dict[str, object]] = []

    def publish(self, *, topic: str, key: str, value: dict[str, object]) -> None:
        self.events.append({"topic": topic, "key": key, "value": value})

    def close(self) -> None:
        pass


def test_submit_persists_request_run_and_outbox_event() -> None:
    settings = get_settings()
    engine = build_engine(settings.database_url)
    session_factory = build_session_factory(engine)
    repository = PostgresScenarioRepository(session_factory)
    service = PostgresScenarioService(
        repository,
        model_version="integration-model-v1",
        data_version="integration-data-v1",
    )
    request = ScenarioRequest(name="Integration persistence test", planning_year=2042)
    first = None

    try:
        first = service.submit(request)
        second = service.submit(request)

        assert first.reused is False
        assert second.reused is True
        assert second.scenario_id == first.scenario_id

        with session_factory() as session:
            assert session.get(ScenarioRequestRow, first.scenario_id) is not None
            assert session.get(ScenarioRunRow, first.job_id) is not None
            event = session.query(OutboxEventRow).filter_by(aggregate_id=first.scenario_id).one()
            assert event.topic == "atlas.scenario.requested.v1"
            assert event.published_at is None

        producer = RecordingProducer()
        result = OutboxPublisher(session_factory, producer).publish_batch(
            aggregate_id=first.scenario_id
        )

        assert result.published == 1
        assert result.failed == 0
        assert producer.events[0]["key"] == first.scenario_id
        with session_factory() as session:
            event = session.query(OutboxEventRow).filter_by(aggregate_id=first.scenario_id).one()
            assert event.published_at is not None
            assert event.publish_attempts == 1
            assert event.last_error is None

        runs = PostgresScenarioRunRepository(session_factory)
        claim = runs.claim(first.scenario_id)
        assert claim.claimed is True
        assert claim.status == "running"
        assert claim.stage == "optimizing"
        recovered = runs.claim(first.scenario_id)
        assert recovered.claimed is True
        assert recovered.attempt == 2
        recovery_failure = runs.fail(recovered.job_id, "worker recovery failure")
        assert recovery_failure.retryable is True
        assert recovery_failure.failure_stage == "optimizing"
        with session_factory() as session:
            retrying = session.get(ScenarioRunRow, recovered.job_id)
            assert retrying is not None
            assert retrying.status == "queued"
            assert retrying.stage == "queued"
            assert retrying.failure_stage == "optimizing"
        final = runs.claim(first.scenario_id)
        assert final.attempt == 3
        assert runs.fail(final.job_id, "final failure").retryable is False
        duplicate = runs.claim(first.scenario_id)
        assert duplicate.claimed is False
        assert duplicate.status == "failed"
        assert duplicate.stage == "failed"
        assert duplicate.attempt == duplicate.max_attempts == 3
    finally:
        if first is not None:
            with session_factory.begin() as session:
                session.execute(
                    delete(OutboxEventRow).where(
                        OutboxEventRow.aggregate_id == first.scenario_id
                    )
                )
                session.execute(delete(ScenarioRunRow).where(ScenarioRunRow.job_id == first.job_id))
                session.execute(
                    delete(ScenarioRequestRow).where(
                        ScenarioRequestRow.scenario_id == first.scenario_id
                    )
                )
        engine.dispose()
