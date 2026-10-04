"""Normalize verified statistical Bronze sources into source-aligned Silver tables."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import logging
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict, Field

from atlas.config import get_settings
from atlas.ingestion.contracts import BronzeManifest
from atlas.ingestion.world_bank import load_world_bank_config
from atlas.transforms.weather_silver import build_spark_session

LOGGER = logging.getLogger(__name__)
TRANSFORM_VERSION = "structured-silver-v1"

LINEAGE_SCHEMA = """
source_id STRING, source_file STRING, source_checksum_sha256 STRING,
bronze_ingest_date STRING, silver_version STRING
"""
TABLE_SCHEMAS = {
    "world_bank_indicators": f"""
        country_code STRING, country_name STRING, year INT,
        indicator_code STRING, indicator_name STRING, value DOUBLE, unit STRING,
        {LINEAGE_SCHEMA}
    """,
    "owid_energy": f"""
        country_code STRING, country_name STRING, year INT,
        metric_code STRING, metric_name STRING, value DOUBLE, unit STRING,
        upstream_source STRING, {LINEAGE_SCHEMA}
    """,
    "ember_electricity": f"""
        country_code STRING, country_name STRING, year INT, category STRING,
        subcategory STRING, variable STRING, unit STRING, value DOUBLE,
        yoy_absolute_change DOUBLE, yoy_percent_change DOUBLE, {LINEAGE_SCHEMA}
    """,
    "irena_energy": f"""
        region STRING, sub_region STRING, country_name STRING, country_code STRING,
        m49_code INT, renewability STRING, technology_group STRING, technology STRING,
        sub_technology STRING, producer_type STRING, year INT,
        electricity_generation_gwh DOUBLE, installed_capacity_mw DOUBLE,
        heat_generation_tj DOUBLE, public_flows_2022_usd_m DOUBLE,
        sdg_7a1_public_flows_2022_usd_m DOUBLE,
        sdg_7b1_capacity_per_capita_w DOUBLE, {LINEAGE_SCHEMA}
    """,
    "technology_costs": f"""
        model_year INT, technology STRING, parameter STRING, value DOUBLE, unit STRING,
        upstream_source STRING, further_description STRING, currency_year DOUBLE,
        release STRING, {LINEAGE_SCHEMA}
    """,
}


class StructuredSilverConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    version: int = Field(ge=1, le=1)
    country_code: str = Field(pattern=r"^[A-Z]{3}$")
    start_year: int = Field(ge=1960)
    end_year: int = Field(ge=1960)
    owid_metrics: list[str] = Field(min_length=1)
    technology_model_years: list[int] = Field(min_length=1)


@dataclass(frozen=True)
class VerifiedBronzeObject:
    raw_path: Path
    metadata_path: Path
    manifest: BronzeManifest
    ingest_date: date


@dataclass(frozen=True)
class StructuredBuildResult:
    silver_version: str
    table_rows: dict[str, int]
    output_root: Path


def load_config(path: Path) -> StructuredSilverConfig:
    try:
        content = yaml.safe_load(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        raise FileNotFoundError(f"Structured Silver configuration not found: {path}") from None
    except yaml.YAMLError as error:
        message = f"Invalid YAML in structured Silver configuration {path}: {error}"
        raise ValueError(message) from error
    config = StructuredSilverConfig.model_validate(content)
    if config.end_year < config.start_year:
        raise ValueError("end_year cannot precede start_year")
    if len(config.owid_metrics) != len(set(config.owid_metrics)):
        raise ValueError("owid_metrics contains duplicates")
    return config


def _ingest_date(path: Path) -> date:
    try:
        return date.fromisoformat(path.name.removeprefix("ingest_date="))
    except ValueError as error:
        raise ValueError(f"Invalid Bronze partition: {path}") from error


def discover_latest_source(data_root: Path, bronze_prefix: str) -> list[VerifiedBronzeObject]:
    root = data_root / "bronze" / bronze_prefix
    partitions = sorted(
        (path for path in root.glob("ingest_date=*") if path.is_dir()),
        key=_ingest_date,
        reverse=True,
    )
    if not partitions:
        raise FileNotFoundError(f"No Bronze partition found under {root}")
    partition = partitions[0]
    objects: list[VerifiedBronzeObject] = []
    for metadata_path in sorted(partition.glob("*.metadata.json")):
        manifest = BronzeManifest.model_validate_json(metadata_path.read_text(encoding="utf-8-sig"))
        raw_path = metadata_path.with_name(manifest.raw_file)
        if not raw_path.is_file():
            raise FileNotFoundError(f"Bronze manifest references a missing file: {raw_path}")
        raw_bytes = raw_path.read_bytes()
        if len(raw_bytes) != manifest.byte_count:
            raise ValueError(f"Bronze byte count mismatch: {raw_path}")
        if hashlib.sha256(raw_bytes).hexdigest() != manifest.sha256:
            raise ValueError(f"Bronze checksum mismatch: {raw_path}")
        objects.append(
            VerifiedBronzeObject(raw_path, metadata_path, manifest, _ingest_date(partition))
        )
    if not objects:
        raise FileNotFoundError(f"No standardized Bronze manifests found in {partition}")
    return objects


def _lineage(item: VerifiedBronzeObject, silver_version: str) -> dict[str, Any]:
    return {
        "source_id": item.manifest.source_id,
        "source_file": item.raw_path.name,
        "source_checksum_sha256": item.manifest.sha256,
        "bronze_ingest_date": item.ingest_date.isoformat(),
        "silver_version": silver_version,
    }


def parse_world_bank(
    objects: list[VerifiedBronzeObject],
    config: StructuredSilverConfig,
    silver_version: str,
    indicator_units: Mapping[str, str] | None = None,
) -> list[dict[str, Any]]:
    units = indicator_units or {}
    rows: list[dict[str, Any]] = []
    for item in objects:
        payload = json.loads(item.raw_path.read_text(encoding="utf-8-sig"))
        if not isinstance(payload, list) or len(payload) != 2 or not isinstance(payload[1], list):
            raise ValueError(f"Invalid World Bank payload: {item.raw_path}")
        extra = item.manifest.model_extra or {}
        for observation in payload[1]:
            year = int(observation["date"])
            if year < config.start_year or year > config.end_year:
                continue
            indicator_code = observation["indicator"]["id"]
            rows.append(
                {
                    "country_code": observation.get("countryiso3code") or config.country_code,
                    "country_name": observation.get("country", {}).get("value", "Morocco"),
                    "year": year,
                    "indicator_code": indicator_code,
                    "indicator_name": observation["indicator"]["value"],
                    "value": _optional_float(observation.get("value")),
                    "unit": str(extra.get("unit") or units.get(indicator_code) or "unknown"),
                    **_lineage(item, silver_version),
                }
            )
    _require_unique(rows, ("country_code", "year", "indicator_code"), "World Bank")
    return rows


def parse_owid(
    objects: list[VerifiedBronzeObject], config: StructuredSilverConfig, silver_version: str
) -> list[dict[str, Any]]:
    data_item = next(
        (item for item in objects if item.raw_path.name == "owid-energy-data.csv"), None
    )
    codebook_item = next(
        (item for item in objects if item.raw_path.name == "owid-energy-codebook.csv"), None
    )
    if data_item is None or codebook_item is None:
        raise FileNotFoundError("OWID Silver requires both the energy data and codebook")
    with codebook_item.raw_path.open(encoding="utf-8-sig", newline="") as handle:
        codebook = {row["column"]: row for row in csv.DictReader(handle)}
    missing_metrics = sorted(set(config.owid_metrics) - set(codebook))
    if missing_metrics:
        raise ValueError(f"OWID codebook is missing metrics: {', '.join(missing_metrics)}")

    rows: list[dict[str, Any]] = []
    with data_item.raw_path.open(encoding="utf-8-sig", newline="") as handle:
        for source_row in csv.DictReader(handle):
            if source_row["iso_code"] != config.country_code:
                continue
            year = int(source_row["year"])
            if year < config.start_year or year > config.end_year:
                continue
            for metric in config.owid_metrics:
                raw_value = source_row.get(metric, "")
                info = codebook[metric]
                rows.append(
                    {
                        "country_code": config.country_code,
                        "country_name": source_row["country"],
                        "year": year,
                        "metric_code": metric,
                        "metric_name": info.get("title") or metric,
                        "value": float(raw_value) if raw_value else None,
                        "unit": info.get("unit") or "unknown",
                        "upstream_source": info.get("source") or "unknown",
                        **_lineage(data_item, silver_version),
                    }
                )
    _require_unique(rows, ("country_code", "year", "metric_code"), "OWID")
    return rows


def parse_ember(
    item: VerifiedBronzeObject, config: StructuredSilverConfig, silver_version: str
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with item.raw_path.open(encoding="utf-8-sig", newline="") as handle:
        for source_row in csv.DictReader(handle):
            if source_row["ISO 3 code"] != config.country_code:
                continue
            year = int(source_row["Year"])
            if year < config.start_year or year > config.end_year:
                continue
            rows.append(
                {
                    "country_code": config.country_code,
                    "country_name": source_row["Area"],
                    "year": year,
                    "category": source_row["Category"],
                    "subcategory": source_row["Subcategory"],
                    "variable": source_row["Variable"],
                    "unit": source_row["Unit"],
                    "value": float(source_row["Value"]) if source_row["Value"] else None,
                    "yoy_absolute_change": _optional_float(source_row["YoY absolute change"]),
                    "yoy_percent_change": _optional_float(source_row["YoY % change"]),
                    **_lineage(item, silver_version),
                }
            )
    _require_unique(
        rows,
        ("country_code", "year", "category", "subcategory", "variable", "unit"),
        "Ember",
    )
    return rows


def parse_technology_data(
    objects: list[VerifiedBronzeObject], config: StructuredSilverConfig, silver_version: str
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    requested_years = set(config.technology_model_years)
    for item in objects:
        model_year = int(item.manifest.source_period)
        if model_year not in requested_years:
            continue
        extra = item.manifest.model_extra or {}
        with item.raw_path.open(encoding="utf-8-sig", newline="") as handle:
            for source_row in csv.DictReader(handle):
                rows.append(
                    {
                        "model_year": model_year,
                        "technology": source_row["technology"],
                        "parameter": source_row["parameter"],
                        "value": float(source_row["value"]),
                        "unit": source_row["unit"],
                        "upstream_source": source_row["source"],
                        "further_description": source_row["further description"],
                        "currency_year": _optional_float(source_row["currency_year"]),
                        "release": extra.get("release"),
                        **_lineage(item, silver_version),
                    }
                )
    _require_unique(rows, ("model_year", "technology", "parameter"), "technology-data")
    return rows


IRENA_COLUMNS = (
    "region",
    "sub_region",
    "country_name",
    "country_code",
    "m49_code",
    "renewability",
    "technology_group",
    "technology",
    "sub_technology",
    "producer_type",
    "year",
    "electricity_generation_gwh",
    "installed_capacity_mw",
    "heat_generation_tj",
    "public_flows_2022_usd_m",
    "sdg_7a1_public_flows_2022_usd_m",
    "sdg_7b1_capacity_per_capita_w",
)


def normalize_irena_values(
    values: list[Any], config: StructuredSilverConfig
) -> dict[str, Any] | None:
    if len(values) < len(IRENA_COLUMNS) or values[3] != config.country_code or values[10] is None:
        return None
    year = int(values[10])
    if year < config.start_year or year > config.end_year:
        return None
    row = dict(zip(IRENA_COLUMNS, values[: len(IRENA_COLUMNS)], strict=True))
    row["year"] = year
    row["m49_code"] = int(row["m49_code"]) if row["m49_code"] is not None else None
    for column in IRENA_COLUMNS[11:]:
        row[column] = _optional_float(row[column])
    if not any(row[column] is not None for column in IRENA_COLUMNS[11:]):
        return None
    return row


def parse_irena(
    item: VerifiedBronzeObject, config: StructuredSilverConfig, silver_version: str
) -> list[dict[str, Any]]:
    try:
        from pyxlsb import open_workbook
    except ImportError as error:
        message = "IRENA XLSB support requires: uv sync --extra dev --extra data"
        raise RuntimeError(message) from error
    rows: list[dict[str, Any]] = []
    with open_workbook(item.raw_path) as workbook, workbook.get_sheet("Data") as sheet:
        for source_row in sheet.rows():
            normalized = normalize_irena_values([cell.v for cell in source_row], config)
            if normalized is not None:
                rows.append({**normalized, **_lineage(item, silver_version)})
    _require_unique(
        rows,
        (
            "country_code",
            "year",
            "renewability",
            "technology_group",
            "technology",
            "sub_technology",
            "producer_type",
        ),
        "IRENA",
    )
    return rows


def _optional_float(value: Any) -> float | None:
    return float(value) if value not in (None, "") else None


def _require_unique(rows: list[dict[str, Any]], keys: tuple[str, ...], label: str) -> None:
    identities = [tuple(row.get(key) for key in keys) for row in rows]
    if len(identities) != len(set(identities)):
        raise ValueError(f"{label} Silver rows contain duplicate keys: {keys}")


def _silver_version(objects: Iterable[VerifiedBronzeObject], config: StructuredSilverConfig) -> str:
    payload = {
        "transform_version": TRANSFORM_VERSION,
        "config": config.model_dump(mode="json"),
        "source_checksums": sorted(item.manifest.sha256 for item in objects),
    }
    digest = hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()
    return digest[:16]


def _write_delta(
    spark: Any, rows: list[dict[str, Any]], output_path: Path, schema: str
) -> int:
    if not rows:
        raise ValueError(f"Refusing to write an empty Silver table: {output_path.name}")
    from pyspark.sql import functions as F

    frame = spark.createDataFrame(rows, schema=schema).withColumn(
        "transformed_at_utc", F.current_timestamp()
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    frame.write.format("delta").mode("overwrite").save(str(output_path.resolve()))
    return frame.count()


def build_structured_silver(
    *, spark: Any, data_root: Path, config: StructuredSilverConfig
) -> StructuredBuildResult:
    sources = {
        "world_bank": discover_latest_source(data_root, "statistics/world_bank"),
        "owid": discover_latest_source(data_root, "statistics/owid"),
        "ember": discover_latest_source(data_root, "statistics/ember"),
        "irena": discover_latest_source(data_root, "statistics/irena"),
        "technology_data": discover_latest_source(data_root, "technology/pypsa_technology_data"),
    }
    all_objects = [item for objects in sources.values() for item in objects]
    silver_version = _silver_version(all_objects, config)
    world_bank_config = load_world_bank_config(Path("configs/indicators/world_bank.yml"))
    world_bank_units = {
        indicator.code: indicator.unit for indicator in world_bank_config.indicators
    }
    table_data = {
        "world_bank_indicators": parse_world_bank(
            sources["world_bank"], config, silver_version, world_bank_units
        ),
        "owid_energy": parse_owid(sources["owid"], config, silver_version),
        "ember_electricity": parse_ember(sources["ember"][0], config, silver_version),
        "irena_energy": parse_irena(sources["irena"][0], config, silver_version),
        "technology_costs": parse_technology_data(
            sources["technology_data"], config, silver_version
        ),
    }
    output_root = data_root / "silver"
    table_rows = {
        table: _write_delta(spark, rows, output_root / table, TABLE_SCHEMAS[table])
        for table, rows in table_data.items()
    }
    manifest = {
        "silver_version": silver_version,
        "transform_version": TRANSFORM_VERSION,
        "table_rows": table_rows,
        "source_checksums": {
            source: [item.manifest.sha256 for item in objects]
            for source, objects in sources.items()
        },
        "config": config.model_dump(mode="json"),
    }
    manifest_path = output_root / "manifests" / f"structured_sources_{silver_version}.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    return StructuredBuildResult(silver_version, table_rows, output_root)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build source-aligned structured Silver tables")
    parser.add_argument(
        "--config", type=Path, default=Path("configs/silver/structured_sources.yml")
    )
    args = parser.parse_args()
    settings = get_settings()
    logging.basicConfig(
        level=settings.log_level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    config = load_config(args.config)
    spark = build_spark_session("atlas-structured-silver")
    try:
        result = build_structured_silver(spark=spark, data_root=settings.data_root, config=config)
        LOGGER.info(
            "Structured Silver complete: version=%s tables=%s path=%s",
            result.silver_version,
            result.table_rows,
            result.output_root,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
