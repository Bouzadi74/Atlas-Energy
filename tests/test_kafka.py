import json

import pytest
from confluent_kafka import KafkaError, KafkaException

from atlas.events.kafka import (
    KafkaCommitError,
    KafkaDeliveryError,
    KafkaEventConsumer,
    KafkaEventProducer,
)


class FakeKafkaClient:
    def __init__(self, *, delivery_error: object | None = None, remaining: int = 0) -> None:
        self.delivery_error = delivery_error
        self.remaining = remaining
        self.messages: list[dict[str, object]] = []

    def produce(self, **message: object) -> None:
        self.messages.append(message)
        callback = message["on_delivery"]
        callback(self.delivery_error, object())  # type: ignore[operator]

    def flush(self, _timeout: float) -> int:
        return self.remaining


class FakeConsumedMessage:
    def error(self) -> None:
        return None

    def key(self) -> bytes:
        return b"scn_123"

    def value(self) -> bytes:
        return b'{"event_id":"evt_123"}'

    def topic(self) -> str:
        return "atlas.test.v1"

    def partition(self) -> int:
        return 1

    def offset(self) -> int:
        return 42


class FakeConsumerClient:
    def __init__(self, *, commit_error: bool = False) -> None:
        self.message = FakeConsumedMessage()
        self.commit_error = commit_error
        self.subscriptions: list[list[str]] = []
        self.commits: list[object] = []
        self.seeks: list[object] = []

    def subscribe(self, topics: list[str]) -> None:
        self.subscriptions.append(topics)

    def poll(self, _timeout: float) -> FakeConsumedMessage:
        return self.message

    def commit(self, *, message: object, asynchronous: bool) -> None:
        assert asynchronous is False
        if self.commit_error:
            raise KafkaException(KafkaError(KafkaError.UNKNOWN_MEMBER_ID))
        self.commits.append(message)

    def seek(self, partition: object) -> None:
        self.seeks.append(partition)

    def close(self) -> None:
        pass


def test_publish_serializes_event_and_waits_for_acknowledgement() -> None:
    client = FakeKafkaClient()
    producer = KafkaEventProducer("unused:9092", client=client)

    producer.publish(topic="atlas.test.v1", key="scn_123", value={"z": 2, "a": 1})

    assert client.messages[0]["topic"] == "atlas.test.v1"
    assert client.messages[0]["key"] == b"scn_123"
    assert json.loads(client.messages[0]["value"]) == {"a": 1, "z": 2}


def test_publish_raises_when_kafka_rejects_event() -> None:
    producer = KafkaEventProducer(
        "unused:9092",
        client=FakeKafkaClient(delivery_error="broker unavailable"),
    )

    with pytest.raises(KafkaDeliveryError, match="broker unavailable"):
        producer.publish(topic="atlas.test.v1", key="scn_123", value={"event_id": "evt"})


def test_consumer_decodes_and_manually_commits_event() -> None:
    client = FakeConsumerClient()
    consumer = KafkaEventConsumer(
        "unused:9092",
        topic="atlas.test.v1",
        group_id="atlas-test",
        client=client,
    )

    event = consumer.poll()

    assert client.subscriptions == [["atlas.test.v1"]]
    assert event is not None
    assert event.key == "scn_123"
    assert event.value == {"event_id": "evt_123"}
    assert event.offset == 42
    consumer.commit(event)
    assert client.commits == [client.message]
    consumer.retry(event)
    assert len(client.seeks) == 1
    assert client.seeks[0].topic == "atlas.test.v1"  # type: ignore[attr-defined]
    assert client.seeks[0].partition == 1  # type: ignore[attr-defined]
    assert client.seeks[0].offset == 42  # type: ignore[attr-defined]


def test_consumer_wraps_lost_group_membership_on_commit() -> None:
    consumer = KafkaEventConsumer(
        "unused:9092",
        topic="atlas.test.v1",
        group_id="atlas-test",
        client=FakeConsumerClient(commit_error=True),
    )
    event = consumer.poll()

    assert event is not None
    with pytest.raises(KafkaCommitError, match="Could not commit Kafka event offset"):
        consumer.commit(event)
