"""Extract registered official reports into provenance-rich Silver tables."""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import re
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator

from atlas.config import get_settings
from atlas.transforms.structured_silver import VerifiedBronzeObject, discover_latest_source
from atlas.transforms.weather_silver import build_spark_session

LOGGER = logging.getLogger(__name__)
TRANSFORM_VERSION = "report-silver-v1"


class TablePage(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source_id: Literal["onee", "anre", "mef"]
    source_file: str = Field(min_length=1)
    page_number_pdf: int = Field(ge=1)
    printed_page: str = Field(min_length=1)


class CuratedMetric(BaseModel):
    model_config = ConfigDict(extra="forbid")

    metric_id: str = Field(pattern=r"^[a-z0-9_]+$")
    source_id: Literal["onee", "anre", "mef"]
    source_file: str = Field(min_length=1)
    page_number_pdf: int = Field(ge=1)
    printed_page: str = Field(min_length=1)
    metric_name: str = Field(min_length=1)
    category: str = Field(min_length=1)
    value: float
    unit: str = Field(min_length=1)
    period: str = Field(min_length=1)
    geography: str = Field(min_length=1)
    qualifier: Literal["exact", "approximately", "minimum", "maximum"]
    evidence: str = Field(min_length=1, max_length=240)


class ReportSilverConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    version: Literal[1]
    minimum_embedded_text_characters: int = Field(ge=1)
    energy_keywords: list[str] = Field(min_length=1)
    table_pages: list[TablePage] = Field(default_factory=list)
    metrics: list[CuratedMetric] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_unique_entries(self) -> ReportSilverConfig:
        keywords = [normalize_for_search(keyword) for keyword in self.energy_keywords]
        if len(keywords) != len(set(keywords)):
            raise ValueError("energy_keywords contains duplicates after normalization")
        metric_ids = [metric.metric_id for metric in self.metrics]
        if len(metric_ids) != len(set(metric_ids)):
            raise ValueError("metrics contains duplicate metric_id values")
        table_keys = [
            (entry.source_id, entry.source_file, entry.page_number_pdf)
            for entry in self.table_pages
        ]
        if len(table_keys) != len(set(table_keys)):
            raise ValueError("table_pages contains duplicate source/page entries")
        return self


@dataclass(frozen=True)
class ReportBuildResult:
    silver_version: str
    table_rows: dict[str, int]
    output_root: Path


LINEAGE_SCHEMA = """
source_id STRING, source_file STRING, source_checksum_sha256 STRING,
bronze_ingest_date STRING, silver_version STRING
"""
TABLE_SCHEMAS = {
    "report_documents": f"""
        report_title STRING, source_period STRING, page_count INT,
        embedded_text_pages INT, sparse_text_pages INT, ocr_required_pages INT,
        extraction_engine STRING, {LINEAGE_SCHEMA}
    """,
    "report_pages": f"""
        page_number_pdf INT, page_label STRING, text STRING, character_count INT,
        word_count INT, extraction_status STRING, energy_relevant BOOLEAN,
        matched_keywords ARRAY<STRING>, {LINEAGE_SCHEMA}
    """,
    "report_table_cells": f"""
        page_number_pdf INT, printed_page STRING, table_index INT, row_index INT,
        column_index INT, cell_text STRING, extraction_method STRING, {LINEAGE_SCHEMA}
    """,
    "report_metrics": f"""
        metric_id STRING, metric_name STRING, category STRING, value DOUBLE, unit STRING,
        period STRING, geography STRING, qualifier STRING, evidence STRING,
        page_number_pdf INT, printed_page STRING, review_status STRING,
        extraction_method STRING, {LINEAGE_SCHEMA}
    """,
}


def load_config(path: Path) -> ReportSilverConfig:
    try:
        payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        raise FileNotFoundError(f"Report Silver configuration not found: {path}") from None
    except yaml.YAMLError as error:
        raise ValueError(f"Invalid report Silver YAML {path}: {error}") from error
    return ReportSilverConfig.model_validate(payload)


def normalize_for_search(value: str) -> str:
    decomposed = unicodedata.normalize("NFKD", value.casefold())
    return "".join(character for character in decomposed if not unicodedata.combining(character))


def normalize_page_text(value: str) -> str:
    value = value.replace("\x00", " ").replace("\u00ad", "")
    lines = [re.sub(r"[ \t]+", " ", line).strip() for line in value.splitlines()]
    return "\n".join(line for line in lines if line)


def classify_page(text: str, minimum_characters: int) -> str:
    if not text:
        return "ocr_required"
    if len(text) < minimum_characters:
        return "sparse_text"
    return "embedded_text"


def _lineage(item: VerifiedBronzeObject, silver_version: str) -> dict[str, Any]:
    return {
        "source_id": item.manifest.source_id,
        "source_file": item.raw_path.name,
        "source_checksum_sha256": item.manifest.sha256,
        "bronze_ingest_date": item.ingest_date.isoformat(),
        "silver_version": silver_version,
    }


def _report_version(
    objects: list[VerifiedBronzeObject], config: ReportSilverConfig
) -> str:
    payload = {
        "transform_version": TRANSFORM_VERSION,
        "config": config.model_dump(mode="json"),
        "source_checksums": sorted(item.manifest.sha256 for item in objects),
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()[:16]


def extract_pages(
    objects: list[VerifiedBronzeObject],
    config: ReportSilverConfig,
    silver_version: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, int]]:
    try:
        from pypdf import PdfReader
    except ImportError as error:
        message = "PDF extraction requires: uv sync --extra dev --extra data"
        raise RuntimeError(message) from error

    page_rows: list[dict[str, Any]] = []
    document_rows: list[dict[str, Any]] = []
    page_counts: dict[str, int] = {}
    keywords = [(keyword, normalize_for_search(keyword)) for keyword in config.energy_keywords]

    logging.getLogger("pypdf").setLevel(logging.ERROR)
    for item in objects:
        reader = PdfReader(item.raw_path)
        if reader.is_encrypted:
            raise ValueError(f"Encrypted PDF is not supported: {item.raw_path}")
        page_labels = reader.page_labels
        statuses: list[str] = []
        for page_index, page in enumerate(reader.pages):
            text = normalize_page_text(page.extract_text() or "")
            normalized = normalize_for_search(text)
            matched = sorted(keyword for keyword, token in keywords if token in normalized)
            status = classify_page(text, config.minimum_embedded_text_characters)
            statuses.append(status)
            page_rows.append(
                {
                    "page_number_pdf": page_index + 1,
                    "page_label": page_labels[page_index],
                    "text": text,
                    "character_count": len(text),
                    "word_count": len(text.split()),
                    "extraction_status": status,
                    "energy_relevant": bool(matched),
                    "matched_keywords": matched,
                    **_lineage(item, silver_version),
                }
            )
        page_counts[item.raw_path.name] = len(reader.pages)
        document_rows.append(
            {
                "report_title": item.raw_path.stem,
                "source_period": item.manifest.source_period,
                "page_count": len(reader.pages),
                "embedded_text_pages": statuses.count("embedded_text"),
                "sparse_text_pages": statuses.count("sparse_text"),
                "ocr_required_pages": statuses.count("ocr_required"),
                "extraction_engine": "pypdf",
                **_lineage(item, silver_version),
            }
        )
    return document_rows, page_rows, page_counts


def extract_table_cells(
    objects_by_name: dict[str, VerifiedBronzeObject],
    entries: list[TablePage],
    page_counts: dict[str, int],
    silver_version: str,
) -> list[dict[str, Any]]:
    try:
        import pdfplumber
    except ImportError as error:
        message = "PDF table extraction requires: uv sync --extra dev --extra data"
        raise RuntimeError(message) from error

    rows: list[dict[str, Any]] = []
    open_documents: dict[str, Any] = {}
    try:
        for entry in entries:
            item = _resolve_entry(entry, objects_by_name, page_counts)
            if entry.source_file not in open_documents:
                open_documents[entry.source_file] = pdfplumber.open(item.raw_path)
            document = open_documents[entry.source_file]
            tables = document.pages[entry.page_number_pdf - 1].extract_tables()
            if not tables:
                raise ValueError(
                    f"No table detected on configured page {entry.page_number_pdf} "
                    f"of {entry.source_file}"
                )
            for table_index, table in enumerate(tables):
                for row_index, table_row in enumerate(table):
                    for column_index, cell in enumerate(table_row):
                        rows.append(
                            {
                                "page_number_pdf": entry.page_number_pdf,
                                "printed_page": entry.printed_page,
                                "table_index": table_index,
                                "row_index": row_index,
                                "column_index": column_index,
                                "cell_text": normalize_page_text(cell or ""),
                                "extraction_method": "pdfplumber",
                                **_lineage(item, silver_version),
                            }
                        )
    finally:
        for document in open_documents.values():
            document.close()
    return rows


def build_metric_rows(
    objects_by_name: dict[str, VerifiedBronzeObject],
    metrics: list[CuratedMetric],
    page_counts: dict[str, int],
    silver_version: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for metric in metrics:
        item = _resolve_entry(metric, objects_by_name, page_counts)
        rows.append(
            {
                "metric_id": metric.metric_id,
                "metric_name": metric.metric_name,
                "category": metric.category,
                "value": metric.value,
                "unit": metric.unit,
                "period": metric.period,
                "geography": metric.geography,
                "qualifier": metric.qualifier,
                "evidence": metric.evidence,
                "page_number_pdf": metric.page_number_pdf,
                "printed_page": metric.printed_page,
                "review_status": "human_verified_visual",
                "extraction_method": "curated_config",
                **_lineage(item, silver_version),
            }
        )
    return rows


def _resolve_entry(
    entry: TablePage | CuratedMetric,
    objects_by_name: dict[str, VerifiedBronzeObject],
    page_counts: dict[str, int],
) -> VerifiedBronzeObject:
    item = objects_by_name.get(entry.source_file)
    if item is None:
        raise FileNotFoundError(f"Configured report is missing from Bronze: {entry.source_file}")
    if item.manifest.source_id != entry.source_id:
        raise ValueError(f"Source mismatch for configured report: {entry.source_file}")
    if entry.page_number_pdf > page_counts[entry.source_file]:
        raise ValueError(
            f"Configured page {entry.page_number_pdf} exceeds {entry.source_file} page count"
        )
    return item


def _write_delta(spark: Any, rows: list[dict[str, Any]], path: Path, schema: str) -> int:
    if not rows:
        raise ValueError(f"Refusing to write an empty Silver table: {path.name}")
    from pyspark.sql import functions as F

    frame = spark.createDataFrame(rows, schema=schema).withColumn(
        "transformed_at_utc", F.current_timestamp()
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.write.format("delta").mode("overwrite").save(str(path.resolve()))
    return frame.count()


def reuse_verified_build(
    *, spark: Any, output_root: Path, silver_version: str
) -> ReportBuildResult | None:
    manifest_path = output_root / "manifests" / f"reports_{silver_version}.json"
    if not manifest_path.is_file():
        return None
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        table_rows = {str(key): int(value) for key, value in manifest["table_rows"].items()}
    except (KeyError, TypeError, ValueError, json.JSONDecodeError):
        return None
    if manifest.get("silver_version") != silver_version:
        return None
    if set(table_rows) != set(TABLE_SCHEMAS):
        return None
    for table, expected_rows in table_rows.items():
        table_path = output_root / table
        if not (table_path / "_delta_log").is_dir():
            return None
        actual_rows = spark.read.format("delta").load(str(table_path.resolve())).count()
        if actual_rows != expected_rows:
            return None
    return ReportBuildResult(silver_version, table_rows, output_root)


def build_report_silver(
    *, spark: Any, data_root: Path, config: ReportSilverConfig
) -> ReportBuildResult:
    objects = [
        item
        for source in ("onee", "anre", "mef")
        for item in discover_latest_source(data_root, f"reports/{source}")
    ]
    objects_by_name = {item.raw_path.name: item for item in objects}
    if len(objects_by_name) != len(objects):
        raise ValueError("Report filenames must be unique across ONEE, ANRE, and MEF")
    silver_version = _report_version(objects, config)
    output_root = data_root / "silver"
    reused = reuse_verified_build(
        spark=spark, output_root=output_root, silver_version=silver_version
    )
    if reused is not None:
        LOGGER.info("Reused verified Report Silver version=%s", silver_version)
        return reused
    document_rows, page_rows, page_counts = extract_pages(objects, config, silver_version)
    table_data = {
        "report_documents": document_rows,
        "report_pages": page_rows,
        "report_table_cells": extract_table_cells(
            objects_by_name, config.table_pages, page_counts, silver_version
        ),
        "report_metrics": build_metric_rows(
            objects_by_name, config.metrics, page_counts, silver_version
        ),
    }
    table_rows = {
        table: _write_delta(spark, rows, output_root / table, TABLE_SCHEMAS[table])
        for table, rows in table_data.items()
    }
    manifest = {
        "silver_version": silver_version,
        "transform_version": TRANSFORM_VERSION,
        "table_rows": table_rows,
        "source_checksums": {
            item.raw_path.name: item.manifest.sha256 for item in objects
        },
        "page_counts": page_counts,
        "config": config.model_dump(mode="json"),
    }
    manifest_path = output_root / "manifests" / f"reports_{silver_version}.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    return ReportBuildResult(silver_version, table_rows, output_root)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build official-report Silver tables")
    parser.add_argument("--config", type=Path, default=Path("configs/silver/reports.yml"))
    args = parser.parse_args()
    settings = get_settings()
    logging.basicConfig(
        level=settings.log_level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    config = load_config(args.config)
    spark = build_spark_session("atlas-report-silver")
    try:
        result = build_report_silver(spark=spark, data_root=settings.data_root, config=config)
        LOGGER.info(
            "Report Silver complete: version=%s tables=%s path=%s",
            result.silver_version,
            result.table_rows,
            result.output_root,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
