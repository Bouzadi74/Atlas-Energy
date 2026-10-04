import argparse
import logging
from typing import Protocol

from pydantic import BaseModel, ConfigDict, Field

from atlas.config import get_settings
from atlas.database import build_engine, build_session_factory
from atlas.database.run_repository import (
    PostgresScenarioRunRepository,
    RunClaim,
    RunFailure,
)
from atlas.events.kafka import KafkaCommitError, KafkaEvent, KafkaEventConsumer
from atlas.optimization.publication import PostgresResultPublisher
from atlas.schemas.scenario import ScenarioRequest
from atlas.workers.optimization_processor import (
    DbtAnalyticsRefresher,
    OptimizationScenarioProcessor,
)

LOGGER = logging.getLogger(__name__)
SCENARIO_REQUESTED_TOPIC = "atlas.scenario.requested.v1"


class ScenarioRequestedPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    scenario_id: str
    request_hash: str = Field(pattern=r"^[a-f0-9]{64}$")
    model_version: str
    data_version: str
    request: ScenarioRequest


class ScenarioRequestedEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: str
    event_type: str = Field(pattern=r"^scenario\.requested$")
    schema_version: int = Field(ge=1, le=1)
    event_time: str
    producer: str
    key: str
    trace_id: str
    payload: ScenarioRequestedPayload


class EventConsumer(Protocol):
    def poll(self, timeout_seconds: float = 1.0) -> KafkaEvent | None: ...

    def commit(self, event: KafkaEvent) -> None: ...

    def retry(self, event: KafkaEvent) -> None: ...

    def close(self) -> None: ...


class RunRepository(Protocol):
    def claim(self, scenario_id: str) -> RunClaim: ...

    def complete(self, job_id: str, optimizer_run_id: str) -> None: ...

    def fail(
        self, job_id: str, error_message: str, error_type: str = "RuntimeError"
    ) -> RunFailure: ...


class ScenarioProcessor(Protocol):
    def process(self, payload: ScenarioRequestedPayload, job_id: str) -> str: ...


class ScenarioWorker:
    def __init__(
        self,
        consumer: EventConsumer,
        runs: RunRepository,
        processor: ScenarioProcessor,
    ) -> None:
        self._consumer = consumer
        self._runs = runs
        self._processor = processor

    def process_one(self, *, timeout_seconds: float = 1.0) -> bool:
        message = self._consumer.poll(timeout_seconds)
        if message is None:
            return False

        event = ScenarioRequestedEvent.model_validate(message.value)
        if message.key is not None and message.key != event.payload.scenario_id:
            raise ValueError("Kafka message key does not match payload scenario_id")
        if event.key != event.payload.scenario_id:
            raise ValueError("Event key does not match payload scenario_id")

        claim = self._runs.claim(event.payload.scenario_id)
        if claim.claimed:
            try:
                optimizer_run_id = self._processor.process(event.payload, claim.job_id)
                self._runs.complete(claim.job_id, optimizer_run_id)
            except Exception as error:
                failure = self._runs.fail(
                    claim.job_id,
                    str(error),
                    type(error).__name__,
                )
                LOGGER.exception("Scenario job %s failed", claim.job_id)
                if failure.retryable:
                    LOGGER.warning(
                        "Retrying scenario job %s after attempt %d of %d",
                        claim.job_id,
                        failure.attempt,
                        failure.max_attempts,
                    )
                    self._consumer.retry(message)
                    return True
            else:
                LOGGER.info(
                    "Scenario job %s completed with optimizer run %s",
                    claim.job_id,
                    optimizer_run_id,
                )
        else:
            if claim.status == "running" and claim.attempt >= claim.max_attempts:
                self._runs.fail(
                    claim.job_id,
                    "Worker stopped before completing its final permitted attempt",
                    "WorkerInterrupted",
                )
                LOGGER.error(
                    "Scenario job %s exhausted %d attempts after worker interruption",
                    claim.job_id,
                    claim.max_attempts,
                )
            else:
                LOGGER.info(
                    "Skipping scenario %s because job %s is already %s",
                    event.payload.scenario_id,
                    claim.job_id,
                    claim.status,
                )

        try:
            self._consumer.commit(message)
        except KafkaCommitError:
            # A long optimization can outlive a transient consumer-group membership. The
            # persisted terminal state makes redelivery safe; polling again lets librdkafka
            # rejoin, replay the event, skip the completed job, and commit the new assignment.
            LOGGER.warning(
                "Kafka offset commit failed after durable scenario processing; "
                "waiting for safe event redelivery",
                exc_info=True,
            )
        return True

    def run_forever(self, *, poll_timeout_seconds: float = 1.0) -> None:
        LOGGER.info("Atlas scenario worker started")
        while True:
            self.process_one(timeout_seconds=poll_timeout_seconds)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Consume and execute Atlas scenario jobs")
    parser.add_argument("--once", action="store_true", help="Poll once and exit")
    parser.add_argument("--poll-timeout-seconds", type=float, default=5.0)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    if args.poll_timeout_seconds <= 0:
        raise SystemExit("--poll-timeout-seconds must be positive")

    settings = get_settings()
    logging.basicConfig(
        level=settings.log_level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    engine = build_engine(settings.database_url)
    session_factory = build_session_factory(engine)
    consumer = KafkaEventConsumer(
        settings.kafka_bootstrap_servers,
        topic=SCENARIO_REQUESTED_TOPIC,
        group_id="atlas-scenario-worker-v1",
    )
    runs = PostgresScenarioRunRepository(session_factory)
    worker = ScenarioWorker(
        consumer,
        runs,
        OptimizationScenarioProcessor(
            data_root=settings.data_root,
            config_path=settings.optimization_config_path,
            calendar_mode=settings.optimization_calendar,
            publisher=PostgresResultPublisher(session_factory),
            lifecycle=runs,
            analytics=DbtAnalyticsRefresher(
                project_dir=settings.dbt_project_dir,
                profiles_dir=settings.dbt_profiles_dir,
                timeout_seconds=settings.dbt_timeout_seconds,
                enabled=settings.dbt_refresh_enabled,
            ),
        ),
    )

    try:
        if args.once:
            processed = worker.process_one(timeout_seconds=args.poll_timeout_seconds)
            if not processed:
                LOGGER.info("No scenario event received")
        else:
            worker.run_forever(poll_timeout_seconds=args.poll_timeout_seconds)
    except KeyboardInterrupt:
        LOGGER.info("Atlas scenario worker stopped")
    finally:
        consumer.close()
        engine.dispose()


if __name__ == "__main__":
    main()
