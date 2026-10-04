import argparse
import hashlib
import json
import logging
import os
import sys
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

from atlas.config import get_settings
from atlas.ingestion.nasa_power import (
    LocationConfigEntry,
    load_location_config,
    validate_nasa_power_payload,
)

LOGGER = logging.getLogger(__name__)
MISSING_VALUE = -999.0


@dataclass(frozen=True)
class BronzeWeatherInput:
    location: LocationConfigEntry
    raw_path: Path
    metadata_path: Path
    source_checksum: str
    provenance_classification: str
    ingest_date: date


@dataclass(frozen=True)
class SilverBuildResult:
    output_path: Path
    row_count: int
    location_count: int
    missing_row_count: int
    bronze_ingest_date: date


def _partition_date(path: Path) -> date:
    prefix = "ingest_date="
    if not path.name.startswith(prefix):
        raise ValueError(f"Invalid Bronze partition name: {path.name}")
    try:
        return date.fromisoformat(path.name.removeprefix(prefix))
    except ValueError as error:
        raise ValueError(f"Invalid Bronze partition date: {path.name}") from error


def _expected_paths(partition: Path, location_id: str, year: int) -> tuple[Path, Path]:
    stem = f"{location_id}_{year}_hourly"
    return partition / f"{stem}.json", partition / f"{stem}.metadata.json"


def _complete_partition(
    partition: Path,
    *,
    locations: list[LocationConfigEntry],
    year: int,
) -> bool:
    return all(
        raw_path.exists() and metadata_path.exists()
        for location in locations
        for raw_path, metadata_path in [_expected_paths(partition, location.id, year)]
    )


def discover_bronze_inputs(
    *,
    data_root: Path,
    config_path: Path,
    year: int,
    ingest_date: date | None = None,
) -> list[BronzeWeatherInput]:
    """Find one complete partition and verify every raw object against its metadata."""

    config = load_location_config(config_path)
    bronze_root = data_root / "bronze" / "nasa_power"
    if ingest_date is not None:
        partition = bronze_root / f"ingest_date={ingest_date.isoformat()}"
        if not _complete_partition(partition, locations=config.locations, year=year):
            raise FileNotFoundError(
                f"Bronze partition is incomplete for {year}: {partition}"
            )
    else:
        candidates = sorted(
            (path for path in bronze_root.glob("ingest_date=*") if path.is_dir()),
            key=_partition_date,
            reverse=True,
        )
        partition = next(
            (
                path
                for path in candidates
                if _complete_partition(path, locations=config.locations, year=year)
            ),
            None,
        )
        if partition is None:
            raise FileNotFoundError(
                f"No complete NASA POWER Bronze partition found for {year} under {bronze_root}"
            )

    partition_date = _partition_date(partition)
    inputs: list[BronzeWeatherInput] = []
    for location in config.locations:
        raw_path, metadata_path = _expected_paths(partition, location.id, year)
        raw_bytes = raw_path.read_bytes()
        actual_checksum = hashlib.sha256(raw_bytes).hexdigest()
        metadata = json.loads(metadata_path.read_text(encoding="utf-8-sig"))
        expected_checksum = str(metadata.get("sha256", "")).lower()
        if actual_checksum != expected_checksum:
            raise ValueError(f"Checksum mismatch for {raw_path}")
        if metadata.get("raw_file") != raw_path.name:
            raise ValueError(f"Metadata raw_file does not match {raw_path.name}")
        if str(metadata.get("location_name", "")).casefold() != location.id.casefold():
            raise ValueError(f"Metadata location does not match {location.id}")
        if float(metadata.get("latitude")) != location.latitude or float(
            metadata.get("longitude")
        ) != location.longitude:
            raise ValueError(f"Metadata coordinates do not match {location.id}")

        payload = json.loads(raw_bytes)
        if not isinstance(payload, dict):
            raise ValueError(f"NASA POWER payload must be an object: {raw_path}")
        validate_nasa_power_payload(payload, year=year)
        inputs.append(
            BronzeWeatherInput(
                location=location,
                raw_path=raw_path,
                metadata_path=metadata_path,
                source_checksum=actual_checksum,
                provenance_classification=str(
                    metadata.get("provenance_classification", "DERIVED")
                ),
                ingest_date=partition_date,
            )
        )

    return inputs


def build_spark_session(
    app_name: str = "atlas-weather-silver", *, extra_packages: list[str] | None = None
) -> Any:
    """Create a local Spark session configured with Delta Lake extensions."""

    python_executable = str(Path(sys.executable).resolve())
    # Use the PySpark distribution installed in this project instead of an unrelated
    # machine-wide SPARK_HOME, and prevent Windows from selecting its Store python alias.
    os.environ.pop("SPARK_HOME", None)
    os.environ["PYSPARK_PYTHON"] = python_executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = python_executable
    ivy_directory = (Path.cwd() / ".spark" / "ivy").resolve()
    ivy_directory.mkdir(parents=True, exist_ok=True)
    local_directory = (Path.cwd() / ".spark" / "local").resolve()
    local_directory.mkdir(parents=True, exist_ok=True)

    try:
        from delta import configure_spark_with_delta_pip
        from pyspark.sql import SparkSession
    except ImportError as error:
        raise RuntimeError(
            "Spark dependencies are missing. Run: uv sync --extra data --extra dev"
        ) from error

    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[4]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.jars.ivy", str(ivy_directory))
        .config("spark.local.dir", str(local_directory))
        .config("spark.ui.enabled", "false")
        .config("spark.pyspark.python", python_executable)
        .config("spark.pyspark.driver.python", python_executable)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    return configure_spark_with_delta_pip(builder, extra_packages=extra_packages).getOrCreate()


def build_weather_silver(
    *,
    spark: Any,
    inputs: list[BronzeWeatherInput],
    year: int,
    output_path: Path,
) -> SilverBuildResult:
    """Normalize verified NASA POWER files and write one partitioned Delta table."""

    if not inputs:
        raise ValueError("At least one Bronze input is required")

    from pyspark.sql import functions as F
    from pyspark.sql.types import (
        DoubleType,
        MapType,
        StringType,
        StructField,
        StructType,
    )

    parameter_maps = StructType(
        [
            StructField(
                name,
                MapType(StringType(), DoubleType(), valueContainsNull=True),
                nullable=False,
            )
            for name in ("ALLSKY_SFC_SW_DWN", "T2M", "WS10M", "WS50M")
        ]
    )
    raw_schema = StructType(
        [
            StructField(
                "properties",
                StructType([StructField("parameter", parameter_maps, nullable=False)]),
                nullable=False,
            )
        ]
    )
    raw_paths = [str(item.raw_path.resolve()) for item in inputs]
    raw = (
        spark.read.schema(raw_schema)
        .option("multiLine", True)
        .json(raw_paths)
        .withColumn("_source_uri", F.input_file_name())
        .withColumn(
            "source_file",
            F.regexp_extract(F.col("_source_uri"), r"([^/]+_hourly\.json)$", 1),
        )
    )

    maps = F.col("properties.parameter")
    expanded = raw.select(
        "source_file",
        F.explode(F.map_entries(maps["ALLSKY_SFC_SW_DWN"])).alias("solar"),
        maps["T2M"].alias("temperature"),
        maps["WS10M"].alias("wind_10m"),
        maps["WS50M"].alias("wind_50m"),
    ).select(
        "source_file",
        F.col("solar.key").alias("timestamp_key"),
        F.col("solar.value").alias("solar_raw"),
        F.element_at("temperature", F.col("solar.key")).alias("temperature_raw"),
        F.element_at("wind_10m", F.col("solar.key")).alias("wind_10m_raw"),
        F.element_at("wind_50m", F.col("solar.key")).alias("wind_50m_raw"),
    )

    lineage_rows = [
        (
            item.raw_path.name,
            item.location.id,
            item.location.zone,
            item.location.latitude,
            item.location.longitude,
            item.source_checksum,
            item.provenance_classification,
            item.ingest_date.isoformat(),
        )
        for item in inputs
    ]
    lineage_schema = (
        "source_file string, location_id string, zone string, latitude double, "
        "longitude double, source_checksum_sha256 string, provenance_classification string, "
        "bronze_ingest_date string"
    )
    lineage = spark.createDataFrame(lineage_rows, schema=lineage_schema)
    joined = expanded.join(lineage, on="source_file", how="inner")

    raw_value_columns = ["solar_raw", "temperature_raw", "wind_10m_raw", "wind_50m_raw"]
    missing_expression = F.lit(False)
    for column in raw_value_columns:
        missing_expression = missing_expression | F.col(column).isNull() | (
            F.col(column) == F.lit(MISSING_VALUE)
        )

    def clean(column: str) -> Any:
        return F.when(
            F.col(column).isNull() | (F.col(column) == F.lit(MISSING_VALUE)),
            F.lit(None).cast("double"),
        ).otherwise(F.col(column))

    silver = joined.select(
        F.to_timestamp("timestamp_key", "yyyyMMddHH").alias("timestamp_utc"),
        "location_id",
        "zone",
        "latitude",
        "longitude",
        clean("solar_raw").alias("solar_irradiance_wh_m2"),
        clean("temperature_raw").alias("temperature_c"),
        clean("wind_10m_raw").alias("wind_speed_10m_m_s"),
        clean("wind_50m_raw").alias("wind_speed_50m_m_s"),
        missing_expression.alias("quality_has_missing"),
        F.lit("NASA POWER").alias("source"),
        "source_file",
        "source_checksum_sha256",
        "provenance_classification",
        F.to_date("bronze_ingest_date").alias("bronze_ingest_date"),
        F.lit(year).cast("integer").alias("weather_year"),
        F.current_timestamp().alias("transformed_at_utc"),
    )

    expected_rows = len(inputs) * ((366 if _is_leap(year) else 365) * 24)
    row_count = silver.count()
    if row_count != expected_rows:
        raise ValueError(f"Silver row count is {row_count}; expected {expected_rows}")
    if silver.where(F.col("timestamp_utc").isNull()).limit(1).count():
        raise ValueError("Silver data contains an invalid UTC timestamp")
    duplicate_count = (
        silver.groupBy("timestamp_utc", "location_id")
        .count()
        .where(F.col("count") > 1)
        .limit(1)
        .count()
    )
    if duplicate_count:
        raise ValueError("Silver data contains duplicate location-hour keys")
    location_count = silver.select("location_id").distinct().count()
    if location_count != len(inputs):
        raise ValueError(
            f"Silver location count is {location_count}; expected {len(inputs)}"
        )
    missing_row_count = silver.where(F.col("quality_has_missing")).count()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    (
        silver.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"weather_year = {year}")
        .partitionBy("weather_year")
        .save(str(output_path.resolve()))
    )

    return SilverBuildResult(
        output_path=output_path,
        row_count=row_count,
        location_count=location_count,
        missing_row_count=missing_row_count,
        bronze_ingest_date=inputs[0].ingest_date,
    )


def _is_leap(year: int) -> bool:
    return year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build the conformed NASA POWER Silver table")
    parser.add_argument("--year", required=True, type=int)
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("configs/locations/morocco.yml"),
    )
    parser.add_argument("--ingest-date", type=date.fromisoformat)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    settings = get_settings()
    logging.basicConfig(
        level=settings.log_level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    inputs = discover_bronze_inputs(
        data_root=settings.data_root,
        config_path=args.config,
        year=args.year,
        ingest_date=args.ingest_date,
    )
    LOGGER.info(
        "Verified %d Bronze inputs from ingest_date=%s",
        len(inputs),
        inputs[0].ingest_date,
    )
    spark = build_spark_session()
    try:
        result = build_weather_silver(
            spark=spark,
            inputs=inputs,
            year=args.year,
            output_path=settings.data_root / "silver" / "weather",
        )
        LOGGER.info(
            "Silver build complete: rows=%d locations=%d missing_rows=%d path=%s",
            result.row_count,
            result.location_count,
            result.missing_row_count,
            result.output_path,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
