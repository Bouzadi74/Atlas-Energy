import argparse
import logging
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol

from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from atlas.config import get_settings
from atlas.database import build_engine, build_session_factory
from atlas.database.models import OutboxEventRow
from atlas.events.kafka import KafkaEventProducer

LOGGER = logging.getLogger(__name__)
MAX_ERROR_LENGTH = 2_000


class EventProducer(Protocol):
    def publish(self, *, topic: str, key: str, value: dict[str, object]) -> None: ...

    def close(self) -> None: ...


@dataclass(frozen=True)
class PublishBatchResult:
    claimed: int
    published: int
    failed: int


class OutboxPublisher:
    """Publish pending outbox rows with database-level worker coordination."""

    def __init__(
        self,
        session_factory: sessionmaker[Session],
        producer: EventProducer,
    ) -> None:
        self._session_factory = session_factory
        self._producer = producer

    def publish_batch(
        self,
        *,
        batch_size: int = 100,
        aggregate_id: str | None = None,
    ) -> PublishBatchResult:
        if batch_size < 1:
            raise ValueError("batch_size must be at least 1")

        published = 0
        failed = 0
        with self._session_factory.begin() as session:
            statement = (
                select(OutboxEventRow)
                .where(OutboxEventRow.published_at.is_(None))
                .order_by(OutboxEventRow.created_at, OutboxEventRow.event_id)
                .with_for_update(skip_locked=True)
                .limit(batch_size)
            )
            if aggregate_id is not None:
                statement = statement.where(OutboxEventRow.aggregate_id == aggregate_id)
            events = session.scalars(statement).all()

            for event in events:
                event.publish_attempts += 1
                try:
                    self._producer.publish(
                        topic=event.topic,
                        key=event.event_key,
                        value=event.payload,
                    )
                except Exception as error:  # noqa: BLE001 - persist delivery failures for retry
                    failed += 1
                    event.last_error = str(error)[:MAX_ERROR_LENGTH]
                    LOGGER.exception("Failed to publish outbox event %s", event.event_id)
                else:
                    published += 1
                    event.published_at = datetime.now(UTC)
                    event.last_error = None

        return PublishBatchResult(
            claimed=published + failed,
            published=published,
            failed=failed,
        )

    def run_forever(self, *, batch_size: int = 100, interval_seconds: float = 2.0) -> None:
        if interval_seconds <= 0:
            raise ValueError("interval_seconds must be positive")

        LOGGER.info("Atlas outbox publisher started")
        while True:
            result = self.publish_batch(batch_size=batch_size)
            if result.claimed:
                LOGGER.info(
                    "Outbox batch complete: claimed=%d published=%d failed=%d",
                    result.claimed,
                    result.published,
                    result.failed,
                )
            time.sleep(interval_seconds)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Publish Atlas outbox events to Kafka")
    parser.add_argument("--once", action="store_true", help="Publish one batch and exit")
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--interval-seconds", type=float, default=2.0)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    settings = get_settings()
    logging.basicConfig(
        level=settings.log_level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    engine = build_engine(settings.database_url)
    producer = KafkaEventProducer(settings.kafka_bootstrap_servers)
    publisher = OutboxPublisher(build_session_factory(engine), producer)

    try:
        if args.once:
            result = publisher.publish_batch(batch_size=args.batch_size)
            LOGGER.info(
                "Outbox batch complete: claimed=%d published=%d failed=%d",
                result.claimed,
                result.published,
                result.failed,
            )
        else:
            publisher.run_forever(
                batch_size=args.batch_size,
                interval_seconds=args.interval_seconds,
            )
    except KeyboardInterrupt:
        LOGGER.info("Atlas outbox publisher stopped")
    finally:
        producer.close()
        engine.dispose()


if __name__ == "__main__":
    main()
