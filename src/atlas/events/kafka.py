import json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer, TopicPartition


class KafkaDeliveryError(RuntimeError):
    """Raised when Kafka does not acknowledge an event within the delivery timeout."""


class KafkaConsumeError(RuntimeError):
    """Raised when Kafka returns an error or a message cannot be decoded."""


class KafkaCommitError(KafkaConsumeError):
    """Raised when a processed event cannot be committed to its consumer group."""


@dataclass(frozen=True)
class KafkaEvent:
    topic: str
    partition: int
    offset: int
    key: str | None
    value: dict[str, object]
    raw_message: Any


class KafkaEventProducer:
    """Synchronous acknowledgement boundary around the asynchronous Kafka client."""

    def __init__(
        self,
        bootstrap_servers: str,
        *,
        client_id: str = "atlas-outbox-publisher",
        delivery_timeout_seconds: float = 15.0,
        client: Any | None = None,
    ) -> None:
        self._delivery_timeout_seconds = delivery_timeout_seconds
        self._client = client or Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "client.id": client_id,
                "enable.idempotence": True,
                "acks": "all",
            }
        )

    def publish(self, *, topic: str, key: str, value: Mapping[str, object]) -> None:
        delivery_error: list[str] = []
        delivered: list[bool] = []

        def on_delivery(error: object | None, _message: object) -> None:
            if error is None:
                delivered.append(True)
            else:
                delivery_error.append(str(error))

        payload = json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode("utf-8")
        self._client.produce(
            topic=topic,
            key=key.encode("utf-8"),
            value=payload,
            on_delivery=on_delivery,
        )
        remaining = self._client.flush(self._delivery_timeout_seconds)

        if delivery_error:
            raise KafkaDeliveryError(delivery_error[0])
        if remaining or not delivered:
            raise KafkaDeliveryError(
                f"Kafka did not acknowledge the event within "
                f"{self._delivery_timeout_seconds:g} seconds"
            )

    def close(self) -> None:
        remaining = self._client.flush(self._delivery_timeout_seconds)
        if remaining:
            raise KafkaDeliveryError(f"Kafka producer closed with {remaining} event(s) pending")


class KafkaEventConsumer:
    """Small manual-commit consumer used by Atlas background workers."""

    def __init__(
        self,
        bootstrap_servers: str,
        *,
        topic: str,
        group_id: str,
        client: Any | None = None,
    ) -> None:
        self._client = client or Consumer(
            {
                "bootstrap.servers": bootstrap_servers,
                "group.id": group_id,
                "client.id": f"{group_id}-consumer",
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
                "enable.auto.offset.store": False,
                # A full-year PyPSA solve can take longer than Kafka's five-minute default.
                "max.poll.interval.ms": 3_600_000,
            }
        )
        self._client.subscribe([topic])

    def poll(self, timeout_seconds: float = 1.0) -> KafkaEvent | None:
        message = self._client.poll(timeout_seconds)
        if message is None:
            return None
        if message.error():
            if message.error().code() == KafkaError._PARTITION_EOF:
                return None
            raise KafkaConsumeError(str(message.error()))

        try:
            raw_key = message.key()
            raw_value = message.value()
            key = raw_key.decode("utf-8") if raw_key is not None else None
            value = json.loads(raw_value.decode("utf-8"))
            if not isinstance(value, dict):
                raise TypeError("event payload must be a JSON object")
        except (AttributeError, UnicodeDecodeError, json.JSONDecodeError, TypeError) as error:
            raise KafkaConsumeError(f"Invalid Kafka event: {error}") from error

        return KafkaEvent(
            topic=message.topic(),
            partition=message.partition(),
            offset=message.offset(),
            key=key,
            value=value,
            raw_message=message,
        )

    def commit(self, event: KafkaEvent) -> None:
        try:
            self._client.commit(message=event.raw_message, asynchronous=False)
        except KafkaException as error:
            raise KafkaCommitError(f"Could not commit Kafka event offset: {error}") from error

    def retry(self, event: KafkaEvent) -> None:
        """Rewind this assigned partition so the failed event is polled again."""
        self._client.seek(TopicPartition(event.topic, event.partition, event.offset))

    def close(self) -> None:
        self._client.close()
