"""Deterministic synthetic messages for exercising the telemetry streaming contract."""

from __future__ import annotations

import argparse
import logging
import uuid
from datetime import UTC, datetime, timedelta

from atlas.config import get_settings
from atlas.events.kafka import KafkaEventProducer

TOPIC = "atlas.telemetry.v1"
LOGGER = logging.getLogger(__name__)


def make_event(*, batch_id: str, index: int, event_time: datetime, invalid: bool = False) -> dict:
    if event_time.tzinfo is None:
        raise ValueError("event_time must include a timezone")
    event_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"atlas-demo:{batch_id}:{index}:{invalid}"))
    return {
        "event_id": event_id,
        "event_type": "telemetry.sample",
        "schema_version": 1,
        "event_time": event_time.astimezone(UTC).isoformat().replace("+00:00", "Z"),
        "producer": "atlas-demo",
        "key": "demo-solar",
        "trace_id": batch_id,
        "payload": {
            "source_id": "atlas-synthetic-demo",
            "asset_id": "demo-solar",
            "metric": "generation_mw",
            "value": -1.0 if invalid else float(index % 6) * 10.0,
            "unit": "MW",
            "provenance_classification": "SYNTHETIC",
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Publish labeled synthetic telemetry samples")
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--start", required=True, help="ISO 8601 timestamp including timezone")
    parser.add_argument("--count", type=int, default=3)
    parser.add_argument("--include-invalid", action="store_true")
    args = parser.parse_args()
    if not args.batch_id.strip() or args.count < 1 or args.count > 100:
        parser.error("--batch-id must be nonempty and --count must be between 1 and 100")
    try:
        start = datetime.fromisoformat(args.start.replace("Z", "+00:00"))
    except ValueError:
        parser.error("--start must be an ISO 8601 timestamp")
    if start.tzinfo is None:
        parser.error("--start must include a timezone")

    logging.basicConfig(level=get_settings().log_level)
    producer = KafkaEventProducer(
        get_settings().kafka_bootstrap_servers, client_id="atlas-demo-telemetry"
    )
    try:
        for index in range(args.count):
            producer.publish(
                topic=TOPIC,
                key="demo-solar",
                value=make_event(
                    batch_id=args.batch_id,
                    index=index,
                    event_time=start + timedelta(minutes=index),
                ),
            )
        if args.include_invalid:
            producer.publish(
                topic=TOPIC,
                key="demo-solar",
                value=make_event(
                    batch_id=args.batch_id,
                    index=args.count,
                    event_time=start + timedelta(minutes=args.count),
                    invalid=True,
                ),
            )
    finally:
        producer.close()
    LOGGER.info("Published %d synthetic telemetry events", args.count + args.include_invalid)


if __name__ == "__main__":
    main()
