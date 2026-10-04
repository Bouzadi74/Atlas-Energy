"""World Bank annual indicator ingestion for Morocco."""

from __future__ import annotations

import argparse
import json
import logging
import re
import time
from collections.abc import Callable
from datetime import UTC, date, datetime
from pathlib import Path
from urllib.parse import urlencode

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
from atlas.ingestion.contracts import SourceDefinition, load_source_definition

LOGGER = logging.getLogger(__name__)
WORLD_BANK_ENDPOINT = "https://api.worldbank.org/v2"


class Indicator(BaseModel):
    model_config = ConfigDict(extra="forbid")

    code: str = Field(pattern=r"^[A-Z0-9.]+$")
    name: str = Field(min_length=1)
    unit: str = Field(default="unknown", min_length=1)


class WorldBankConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    version: int = Field(ge=1, le=1)
    country_code: str = Field(pattern=r"^[A-Z]{3}$")
    start_year: int = Field(ge=1960)
    end_year: int = Field(ge=1960)
    indicators: list[Indicator] = Field(min_length=1)


def load_world_bank_config(path: Path) -> WorldBankConfig:
    try:
        config = WorldBankConfig.model_validate(yaml.safe_load(path.read_text(encoding="utf-8")))
    except FileNotFoundError:
        raise FileNotFoundError(f"World Bank configuration not found: {path}") from None
    except yaml.YAMLError as error:
        raise ValueError(f"Invalid YAML in World Bank configuration {path}: {error}") from error
    if config.end_year < config.start_year:
        raise ValueError("World Bank end_year cannot precede start_year")
    codes = [indicator.code for indicator in config.indicators]
    if len(codes) != len(set(codes)):
        raise ValueError("World Bank configuration contains duplicate indicator codes")
    return config


def build_world_bank_url(
    *, country_code: str, indicator_code: str, start_year: int, end_year: int
) -> str:
    if not re.fullmatch(r"[A-Z]{3}", country_code):
        raise ValueError("country_code must be an ISO-style three-letter uppercase code")
    if not re.fullmatch(r"[A-Z0-9.]+", indicator_code):
        raise ValueError("indicator_code contains unsupported characters")
    query = urlencode(
        {"format": "json", "date": f"{start_year}:{end_year}", "per_page": "20000"}
    )
    return f"{WORLD_BANK_ENDPOINT}/country/{country_code}/indicator/{indicator_code}?{query}"


def validate_world_bank_payload(raw_bytes: bytes, *, indicator_code: str) -> int:
    try:
        payload = json.loads(raw_bytes)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("World Bank response is not valid UTF-8 JSON") from error
    if not isinstance(payload, list) or len(payload) != 2:
        raise ValueError("World Bank response must contain metadata and observations")
    metadata, observations = payload
    if not isinstance(metadata, dict) or not isinstance(observations, list):
        raise ValueError("World Bank response has an invalid structure")
    if metadata.get("pages") not in (None, 1):
        raise ValueError("World Bank response was unexpectedly paginated")
    for observation in observations:
        if observation.get("indicator", {}).get("id") != indicator_code:
            raise ValueError("World Bank response contains the wrong indicator")
    return len(observations)


def ingest_world_bank_indicator(
    *,
    country_code: str,
    indicator: Indicator,
    start_year: int,
    end_year: int,
    data_root: Path,
    source: SourceDefinition,
    ingest_date: date | None = None,
    fetcher: Callable[[str], HttpPayload] = fetch_http,
) -> BronzeObject:
    url = build_world_bank_url(
        country_code=country_code,
        indicator_code=indicator.code,
        start_year=start_year,
        end_year=end_year,
    )
    partition_date = ingest_date or datetime.now(UTC).date()
    filename_code = indicator.code.lower().replace(".", "_")
    raw_path = (
        data_root
        / "bronze"
        / source.bronze_prefix
        / f"ingest_date={partition_date.isoformat()}"
        / f"{country_code.lower()}_{filename_code}_{start_year}_{end_year}.json"
    )
    payload = load_or_fetch_http(raw_path=raw_path, source_url=url, fetcher=fetcher)
    observation_count = validate_world_bank_payload(payload.body, indicator_code=indicator.code)
    return store_bronze_object(
        source=source,
        source_url=url,
        source_period=f"{start_year}-{end_year}",
        raw_path=raw_path,
        payload=payload,
        attribution="World Bank Open Data",
        extra_metadata={
            "country_code": country_code,
            "indicator_code": indicator.code,
            "indicator_name": indicator.name,
            "unit": indicator.unit,
            "observation_count": observation_count,
        },
        validator=lambda raw_bytes: validate_world_bank_payload(
            raw_bytes, indicator_code=indicator.code
        ),
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest World Bank indicators to Bronze")
    parser.add_argument("--config", type=Path, default=Path("configs/indicators/world_bank.yml"))
    parser.add_argument("--delay-seconds", type=float, default=0.25)
    args = parser.parse_args()
    if args.delay_seconds < 0:
        parser.error("--delay-seconds cannot be negative")

    settings = get_settings()
    logging.basicConfig(
        level=settings.log_level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    config = load_world_bank_config(args.config)
    source = load_source_definition(Path("configs/sources/world_bank.yml"))
    results: list[BronzeObject] = []
    for index, indicator in enumerate(config.indicators):
        LOGGER.info("Processing %s (%d/%d)", indicator.code, index + 1, len(config.indicators))
        result = ingest_world_bank_indicator(
            country_code=config.country_code,
            indicator=indicator,
            start_year=config.start_year,
            end_year=config.end_year,
            data_root=settings.data_root,
            source=source,
        )
        results.append(result)
        if not result.reused and args.delay_seconds and index < len(config.indicators) - 1:
            time.sleep(args.delay_seconds)
    LOGGER.info(
        "World Bank batch complete: total=%d downloaded=%d reused=%d",
        len(results),
        sum(not result.reused for result in results),
        sum(result.reused for result in results),
    )


if __name__ == "__main__":
    main()
