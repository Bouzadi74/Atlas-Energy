import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from atlas.events.demo_telemetry import make_event
from atlas.transforms.telemetry_stream import TelemetryConfig, kafka_connector_package, load_config


def test_demo_event_is_deterministic_and_explicitly_synthetic() -> None:
    time = datetime(2026, 10, 3, tzinfo=UTC)
    first = make_event(batch_id="sample-1", index=2, event_time=time)
    second = make_event(batch_id="sample-1", index=2, event_time=time)

    assert first == second
    assert first["payload"]["provenance_classification"] == "SYNTHETIC"
    assert first["event_type"] == "telemetry.sample"
    assert json.dumps(first)
    assert make_event(batch_id="sample-1", index=2, event_time=time, invalid=True)["payload"][
        "value"
    ] < 0


def test_demo_rejects_naive_time() -> None:
    with pytest.raises(ValueError, match="timezone"):
        make_event(batch_id="sample-1", index=1, event_time=datetime(2026, 10, 3))


def test_stream_config_is_versioned_and_rejects_duplicate_approvals() -> None:
    config = load_config(Path("configs/streaming/telemetry.yml"))

    assert config.topic == "atlas.telemetry.v1"
    assert config.allowed_provenance == ["SYNTHETIC"]
    assert kafka_connector_package().startswith("org.apache.spark:spark-sql-kafka-0-10_2.13:")

    with pytest.raises(ValueError, match="duplicates"):
        TelemetryConfig.model_validate(
            {**config.model_dump(), "allowed_producers": ["atlas-demo", "atlas-demo"]}
        )
