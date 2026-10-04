"""Configuration-driven ingestion for stable direct-download datasets."""

from __future__ import annotations

import argparse
import csv
import io
import logging
from collections.abc import Callable
from datetime import UTC, date, datetime
from pathlib import Path

import yaml
from pydantic import BaseModel, ConfigDict, Field

from atlas.config import get_settings
from atlas.ingestion.bronze import (
    BronzeObject,
    HttpPayload,
    fetch_http,
    load_or_fetch_http,
    store_bronze_object,
)
from atlas.ingestion.contracts import load_source_definition

LOGGER = logging.getLogger(__name__)


class DirectAsset(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(pattern=r"^[a-z0-9_]+$")
    url: str = Field(pattern=r"^https://")
    filename: str = Field(pattern=r"^[A-Za-z0-9_.-]+$")
    source_period: str = Field(min_length=1)
    content: str = Field(pattern=r"^csv$")
    required_columns: list[str] = Field(min_length=1)


class DirectDownloadConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    version: int = Field(ge=1, le=1)
    source_id: str = Field(pattern=r"^[a-z0-9_]+$")
    release: str | None = None
    assets: list[DirectAsset] = Field(min_length=1)


def load_download_config(path: Path) -> DirectDownloadConfig:
    try:
        config = DirectDownloadConfig.model_validate(
            yaml.safe_load(path.read_text(encoding="utf-8"))
        )
    except FileNotFoundError:
        raise FileNotFoundError(f"Download configuration not found: {path}") from None
    except yaml.YAMLError as error:
        raise ValueError(f"Invalid YAML in download configuration {path}: {error}") from error
    names = [asset.name for asset in config.assets]
    filenames = [asset.filename for asset in config.assets]
    if len(names) != len(set(names)) or len(filenames) != len(set(filenames)):
        raise ValueError("Download configuration contains duplicate assets")
    return config


def validate_csv(raw_bytes: bytes, *, required_columns: list[str]) -> int:
    try:
        text = raw_bytes.decode("utf-8-sig")
    except UnicodeDecodeError as error:
        raise ValueError("CSV source is not valid UTF-8") from error
    reader = csv.DictReader(io.StringIO(text))
    columns = reader.fieldnames or []
    missing = sorted(set(required_columns) - set(columns))
    if missing:
        raise ValueError(f"CSV source is missing required columns: {', '.join(missing)}")
    row_count = sum(1 for _row in reader)
    if row_count == 0:
        raise ValueError("CSV source contains no data rows")
    return row_count


def ingest_direct_downloads(
    *,
    config_path: Path,
    registry_dir: Path,
    data_root: Path,
    ingest_date: date | None = None,
    fetcher: Callable[[str], HttpPayload] = fetch_http,
) -> list[BronzeObject]:
    config = load_download_config(config_path)
    source = load_source_definition(registry_dir / f"{config.source_id}.yml")
    partition_date = ingest_date or datetime.now(UTC).date()
    results: list[BronzeObject] = []
    for asset in config.assets:
        raw_path = (
            data_root
            / "bronze"
            / source.bronze_prefix
            / f"ingest_date={partition_date.isoformat()}"
            / asset.filename
        )
        payload = load_or_fetch_http(
            raw_path=raw_path,
            source_url=asset.url,
            fetcher=fetcher,
        )
        row_count = validate_csv(payload.body, required_columns=asset.required_columns)
        result = store_bronze_object(
            source=source,
            source_url=asset.url,
            source_period=asset.source_period,
            raw_path=raw_path,
            payload=payload,
            attribution=source.publisher,
            extra_metadata={
                "asset_name": asset.name,
                "row_count": row_count,
                "release": config.release,
                "required_columns": asset.required_columns,
            },
            validator=lambda raw_bytes, columns=asset.required_columns: validate_csv(
                raw_bytes, required_columns=columns
            ),
        )
        results.append(result)
        LOGGER.info(
            "%s %s (%d rows)",
            "Reused" if result.reused else "Downloaded",
            result.raw_path,
            row_count,
        )
    return results


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest a configured direct-download source")
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--registry", type=Path, default=Path("configs/sources"))
    args = parser.parse_args()
    settings = get_settings()
    logging.basicConfig(
        level=settings.log_level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    results = ingest_direct_downloads(
        config_path=args.config,
        registry_dir=args.registry,
        data_root=settings.data_root,
    )
    LOGGER.info(
        "Direct-download batch complete: total=%d downloaded=%d reused=%d",
        len(results),
        sum(not result.reused for result in results),
        sum(result.reused for result in results),
    )


if __name__ == "__main__":
    main()
