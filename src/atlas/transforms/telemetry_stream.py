"""Replayable Kafka Bronze and source-aligned telemetry Silver/quarantine streams."""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator

from atlas.config import get_settings
from atlas.transforms.weather_silver import build_spark_session

LOGGER = logging.getLogger(__name__)


class TelemetryConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    version: str
    topic: str
    schema_version: int = Field(ge=1)
    allowed_producers: list[str] = Field(min_length=1)
    allowed_source_ids: list[str] = Field(min_length=1)
    allowed_provenance: list[str] = Field(min_length=1)
    watermark: str
    max_offsets_per_trigger: int = Field(ge=1)

    @model_validator(mode="after")
    def check_unique_values(self) -> TelemetryConfig:
        for name in ("allowed_producers", "allowed_source_ids", "allowed_provenance"):
            values = getattr(self, name)
            if len(values) != len(set(values)):
                raise ValueError(f"{name} contains duplicates")
        return self


def load_config(path: Path) -> TelemetryConfig:
    return TelemetryConfig.model_validate(yaml.safe_load(path.read_text(encoding="utf-8")))


def kafka_connector_package() -> str:
    """Spark's Kafka source must use the same version and Scala ABI as installed PySpark."""
    import pyspark

    return f"org.apache.spark:spark-sql-kafka-0-10_2.13:{pyspark.__version__}"


def telemetry_schema() -> Any:
    from pyspark.sql.types import (
        DoubleType,
        IntegerType,
        StringType,
        StructField,
        StructType,
    )

    payload = StructType(
        [
            StructField("source_id", StringType()),
            StructField("asset_id", StringType()),
            StructField("metric", StringType()),
            StructField("value", DoubleType()),
            StructField("unit", StringType()),
            StructField("provenance_classification", StringType()),
        ]
    )
    return StructType(
        [
            StructField("event_id", StringType()),
            StructField("event_type", StringType()),
            StructField("schema_version", IntegerType()),
            StructField("event_time", StringType()),
            StructField("producer", StringType()),
            StructField("key", StringType()),
            StructField("trace_id", StringType()),
            StructField("payload", payload),
            StructField("_corrupt_record", StringType()),
        ]
    )


def classify_telemetry(bronze: Any, config: TelemetryConfig) -> Any:
    """Classify each immutable Kafka record without repairing or dropping invalid input."""
    from pyspark.sql import functions as F

    event = F.from_json(
        F.decode(F.col("raw_value"), "UTF-8"),
        telemetry_schema(),
        {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": "_corrupt_record"},
    )
    frame = (
        bronze.withColumn("event", event)
        .withColumn("event_time_utc", F.to_timestamp("event.event_time"))
        .withColumn("kafka_key_text", F.decode(F.col("raw_key"), "UTF-8"))
    )
    expected_unit = (
        F.when(F.col("event.payload.metric") == "state_of_charge_mwh", "MWh")
        .otherwise("MW")
    )
    reasons = [
        (
            F.col("event").isNull() | F.col("event._corrupt_record").isNotNull(),
            "malformed_json",
        ),
        (F.col("event.schema_version") != config.schema_version, "unsupported_schema_version"),
        (F.col("event.event_type") != "telemetry.sample", "invalid_event_type"),
        (
            F.col("event.event_id").isNull()
            | ~F.col("event.event_id").rlike(
                r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-"
                r"[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
            ),
            "invalid_event_id",
        ),
        (
            F.col("event_time_utc").isNull()
            | ~F.col("event.event_time").rlike(
                r"T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$"
            ),
            "invalid_event_time",
        ),
        (~F.col("event.producer").isin(config.allowed_producers), "unapproved_producer"),
        (~F.col("event.payload.source_id").isin(config.allowed_source_ids), "unapproved_source"),
        (
            ~F.col("event.payload.provenance_classification").isin(config.allowed_provenance),
            "unapproved_provenance",
        ),
        (
            F.col("event.key").isNull()
            | (F.col("event.key") != F.col("kafka_key_text"))
            | (F.col("event.key") != F.col("event.payload.asset_id")),
            "key_mismatch",
        ),
        (
            ~F.col("event.payload.metric").isin(
                "generation_mw", "demand_mw", "state_of_charge_mwh"
            ),
            "invalid_metric",
        ),
        (
            F.col("event.payload.unit") != expected_unit,
            "unit_mismatch",
        ),
        (
            F.col("event.payload.value").isNull()
            | F.isnan(F.col("event.payload.value"))
            | (F.col("event.payload.value") < 0),
            "invalid_value",
        ),
        (
            F.col("event.trace_id").isNull() | (F.length("event.trace_id") == 0),
            "missing_trace_id",
        ),
    ]
    reason = F.lit(None).cast("string")
    for condition, label in reversed(reasons):
        reason = F.when(condition, F.lit(label)).otherwise(reason)
    # Missing nested fields cause SQL three-valued comparisons; reject them explicitly.
    required = (
        F.col("event.schema_version").isNull()
        | F.col("event.event_type").isNull()
        | F.col("event.producer").isNull()
        | F.col("event.payload.source_id").isNull()
        | F.col("event.payload.asset_id").isNull()
        | F.col("event.payload.metric").isNull()
        | F.col("event.payload.unit").isNull()
        | F.col("event.payload.provenance_classification").isNull()
    )
    return frame.withColumn(
        "rejection_reason", F.when(required & reason.isNull(), "missing_required_field")
        .otherwise(reason),
    )


def build_silver_stream(classified: Any, config: TelemetryConfig) -> Any:
    from pyspark.sql import functions as F

    return (
        classified.filter(F.col("rejection_reason").isNull())
        .select(
            F.col("event.event_id").alias("event_id"),
            "event_time_utc",
            F.col("event.producer").alias("producer"),
            F.col("event.trace_id").alias("trace_id"),
            F.col("event.payload.source_id").alias("source_id"),
            F.col("event.payload.asset_id").alias("asset_id"),
            F.col("event.payload.metric").alias("metric"),
            F.col("event.payload.value").alias("value"),
            F.col("event.payload.unit").alias("unit"),
            F.col("event.payload.provenance_classification").alias(
                "provenance_classification"
            ),
            "topic", "partition", "offset", "kafka_timestamp", "ingested_at",
        )
        .withWatermark("event_time_utc", config.watermark)
        .dropDuplicatesWithinWatermark(["event_id"])
    )


def _merge_silver(batch: Any, _batch_id: int, path: Path) -> None:
    from delta.tables import DeltaTable

    if batch.isEmpty():
        return
    batch = batch.dropDuplicates(["event_id"])
    resolved = str(path.resolve())
    if not DeltaTable.isDeltaTable(batch.sparkSession, resolved):
        batch.write.format("delta").mode("append").save(resolved)
        return
    (
        DeltaTable.forPath(batch.sparkSession, resolved)
        .alias("target")
        .merge(batch.alias("source"), "target.event_id = source.event_id")
        .whenNotMatchedInsertAll()
        .execute()
    )


def start_streams(
    spark: Any,
    *,
    config: TelemetryConfig,
    data_root: Path,
    bootstrap_servers: str,
    once: bool,
) -> list[Any]:
    """Start Bronze capture, then independent valid/quarantine Silver consumers."""
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F

    bronze_path = data_root / "bronze" / "telemetry" / "kafka"
    silver_path = data_root / "silver" / "telemetry_events"
    quarantine_path = data_root / "silver" / "telemetry_quarantine"
    checkpoints = data_root / "checkpoints" / "telemetry" / config.version
    source = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", config.topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "true")
        .option("maxOffsetsPerTrigger", config.max_offsets_per_trigger)
        .load()
        .select(
            F.col("key").alias("raw_key"),
            F.col("value").alias("raw_value"),
            "topic", "partition", "offset",
            F.col("timestamp").alias("kafka_timestamp"),
            F.current_timestamp().alias("ingested_at"),
        )
    )
    resolved_bronze = str(bronze_path.resolve())
    if not DeltaTable.isDeltaTable(spark, resolved_bronze):
        spark.createDataFrame([], source.schema).write.format("delta").save(resolved_bronze)
    bronze_writer = (
        source.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", str((checkpoints / "bronze").resolve()))
    )
    if once:
        bronze_writer = bronze_writer.trigger(availableNow=True)
    bronze_query = bronze_writer.start(resolved_bronze)
    if once:
        bronze_query.awaitTermination()

    classified = classify_telemetry(
        spark.readStream.format("delta").load(resolved_bronze), config
    )
    silver = build_silver_stream(classified, config)
    def publish_batch(batch: Any, batch_id: int) -> None:
        _merge_silver(batch, batch_id, silver_path)

    silver_writer = (
        silver.writeStream.foreachBatch(publish_batch)
        .outputMode("append")
        .option("checkpointLocation", str((checkpoints / "silver").resolve()))
    )
    quarantine = classified.filter(F.col("rejection_reason").isNotNull()).select(
        "topic", "partition", "offset", "raw_key", "raw_value", "kafka_timestamp",
        "ingested_at", "rejection_reason",
    )
    quarantine_writer = (
        quarantine.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", str((checkpoints / "quarantine").resolve()))
    )
    if once:
        silver_writer = silver_writer.trigger(availableNow=True)
        quarantine_writer = quarantine_writer.trigger(availableNow=True)
    silver_query = silver_writer.start()
    quarantine_query = quarantine_writer.start(str(quarantine_path.resolve()))
    return [bronze_query, silver_query, quarantine_query]


def write_catchup_manifest(
    spark: Any, *, config: TelemetryConfig, config_path: Path, data_root: Path
) -> Path:
    """Record completed table versions and counts after all three queries stop cleanly."""
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F

    locations = {
        "bronze": data_root / "bronze" / "telemetry" / "kafka",
        "silver": data_root / "silver" / "telemetry_events",
        "quarantine": data_root / "silver" / "telemetry_quarantine",
    }
    tables: dict[str, dict[str, object]] = {}
    for name, path in locations.items():
        resolved = str(path.resolve())
        if not DeltaTable.isDeltaTable(spark, resolved):
            tables[name] = {"path": str(path), "version": None, "rows": 0}
            continue
        frame = spark.read.format("delta").load(resolved)
        version = int(DeltaTable.forPath(spark, resolved).history(1).first()["version"])
        tables[name] = {"path": str(path), "version": version, "rows": frame.count()}
        if name == "quarantine":
            tables[name]["reasons"] = {
                row["rejection_reason"]: row["count"]
                for row in frame.groupBy("rejection_reason").count().collect()
            }
        if name == "bronze":
            tables[name]["offsets"] = [
                {
                    "topic": row["topic"],
                    "partition": row["partition"],
                    "min_offset": row["min_offset"],
                    "max_offset": row["max_offset"],
                }
                for row in frame.groupBy("topic", "partition")
                .agg(F.min("offset").alias("min_offset"), F.max("offset").alias("max_offset"))
                .orderBy("topic", "partition")
                .collect()
            ]
    generated = datetime.now(UTC)
    manifest = {
        "build_version": config.version,
        "config_sha256": hashlib.sha256(config_path.read_bytes()).hexdigest(),
        "schema_sha256": hashlib.sha256(
            Path("kafka/schemas/telemetry-v1.schema.json").read_bytes()
        ).hexdigest(),
        "generated_at_utc": generated.isoformat(),
        "topic": config.topic,
        "watermark": config.watermark,
        "checkpoint_root": str(data_root / "checkpoints" / "telemetry" / config.version),
        "tables": tables,
    }
    output_dir = data_root / "silver" / "manifests" / "telemetry"
    output_dir.mkdir(parents=True, exist_ok=True)
    output = output_dir / f"catchup-{generated:%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:8]}.json"
    output.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    LOGGER.info("Telemetry catch-up summary: %s", json.dumps(tables, sort_keys=True))
    return output


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Capture Kafka telemetry in Bronze and Delta Silver"
    )
    parser.add_argument("--config", type=Path, default=Path("configs/streaming/telemetry.yml"))
    parser.add_argument(
        "--once", action="store_true", help="Process currently available Kafka data"
    )
    args = parser.parse_args()
    settings = get_settings()
    logging.basicConfig(level=settings.log_level)
    config = load_config(args.config)
    spark = build_spark_session(
        "atlas-telemetry-stream", extra_packages=[kafka_connector_package()]
    )
    try:
        queries = start_streams(
            spark,
            config=config,
            data_root=settings.data_root,
            bootstrap_servers=settings.kafka_bootstrap_servers,
            once=args.once,
        )
        if args.once:
            for query in queries:
                query.awaitTermination()
            manifest = write_catchup_manifest(
                spark, config=config, config_path=args.config, data_root=settings.data_root
            )
            LOGGER.info("Telemetry catch-up manifest: %s", manifest)
            LOGGER.info("Telemetry Bronze, Silver, and quarantine catch-up complete")
        else:
            LOGGER.info("Telemetry Bronze, Silver, and quarantine streams started")
            spark.streams.awaitAnyTermination()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
