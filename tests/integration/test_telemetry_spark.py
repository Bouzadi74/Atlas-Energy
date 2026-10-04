import json
import os
from datetime import UTC, datetime
from pathlib import Path

import pytest

from atlas.events.demo_telemetry import make_event
from atlas.transforms.telemetry_stream import _merge_silver, classify_telemetry, load_config
from atlas.transforms.weather_silver import build_spark_session

pytestmark = pytest.mark.skipif(
    os.getenv("ATLAS_RUN_SPARK_INTEGRATION_TESTS") != "1",
    reason="Set ATLAS_RUN_SPARK_INTEGRATION_TESTS=1 to run local Spark/Delta checks",
)


def test_telemetry_classification_quarantine_and_merge_are_replay_safe(tmp_path) -> None:
    from pyspark.sql import functions as F
    from pyspark.sql.types import (
        BinaryType,
        IntegerType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    spark = build_spark_session("atlas-telemetry-contract-test")
    config = load_config(Path("configs/streaming/telemetry.yml"))
    event_time = datetime(2026, 10, 3, tzinfo=UTC)
    valid = make_event(batch_id="contract", index=1, event_time=event_time)
    invalid = make_event(batch_id="contract", index=2, event_time=event_time, invalid=True)
    unsupported = {**valid, "event_id": "9cb638b3-1c70-4bb7-b200-91351cab5d51", "schema_version": 2}
    payloads = [valid, invalid, unsupported]
    schema = StructType(
        [
            StructField("raw_key", BinaryType()),
            StructField("raw_value", BinaryType()),
            StructField("topic", StringType()),
            StructField("partition", IntegerType()),
            StructField("offset", LongType()),
            StructField("kafka_timestamp", TimestampType()),
            StructField("ingested_at", TimestampType()),
        ]
    )
    rows = [
        (
            b"demo-solar",
            json.dumps(payload).encode(),
            config.topic,
            0,
            index,
            event_time,
            event_time,
        )
        for index, payload in enumerate(payloads)
    ]
    rows.append((b"demo-solar", b"not json", config.topic, 0, 3, event_time, event_time))
    try:
        classified = classify_telemetry(spark.createDataFrame(rows, schema), config)
        results = {row.offset: row.rejection_reason for row in classified.collect()}
        assert results == {
            0: None,
            1: "invalid_value",
            2: "unsupported_schema_version",
            3: "malformed_json",
        }

        silver = classified.filter(F.col("rejection_reason").isNull()).select(
            F.col("event.event_id").alias("event_id"),
            "event_time_utc", "topic", "partition", "offset",
        )
        output = tmp_path / "silver"
        _merge_silver(silver, 0, output)
        _merge_silver(silver, 0, output)
        assert spark.read.format("delta").load(str(output)).count() == 1
    finally:
        spark.stop()
