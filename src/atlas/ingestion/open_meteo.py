"""Open-Meteo historical weather ingestion for Moroccan energy zones."""

from __future__ import annotations

import argparse
import calendar
import json
import logging
import time
from collections.abc import Callable
from datetime import UTC, date, datetime
from pathlib import Path
from urllib.parse import urlencode

from atlas.config import get_settings
from atlas.ingestion.bronze import (
    BronzeObject,
    HttpPayload,
    fetch_http,
    load_or_fetch_http,
    store_bronze_object,
)
from atlas.ingestion.contracts import SourceDefinition, load_source_definition
from atlas.ingestion.nasa_power import load_location_config

LOGGER = logging.getLogger(__name__)
OPEN_METEO_ENDPOINT = "https://archive-api.open-meteo.com/v1/archive"
OPEN_METEO_VARIABLES = (
    "temperature_2m",
    "shortwave_radiation",
    "wind_speed_10m",
    "wind_speed_100m",
)


def build_open_meteo_url(*, latitude: float, longitude: float, year: int) -> str:
    if not -90 <= latitude <= 90:
        raise ValueError("latitude must be between -90 and 90")
    if not -180 <= longitude <= 180:
        raise ValueError("longitude must be between -180 and 180")
    if not 1940 <= year <= datetime.now(UTC).year:
        raise ValueError("year must be between 1940 and the current year")
    query = urlencode(
        {
            "latitude": f"{latitude:.4f}",
            "longitude": f"{longitude:.4f}",
            "start_date": f"{year}-01-01",
            "end_date": f"{year}-12-31",
            "hourly": ",".join(OPEN_METEO_VARIABLES),
            "timezone": "GMT",
            "models": "era5",
        }
    )
    return f"{OPEN_METEO_ENDPOINT}?{query}"


def validate_open_meteo_payload(raw_bytes: bytes, *, year: int) -> dict[str, int]:
    try:
        payload = json.loads(raw_bytes)
        hourly = payload["hourly"]
    except (UnicodeDecodeError, json.JSONDecodeError, KeyError, TypeError) as error:
        raise ValueError("Open-Meteo response is missing valid hourly JSON data") from error
    if payload.get("utc_offset_seconds") != 0:
        raise ValueError("Open-Meteo response is not in UTC/GMT")
    expected_hours = (366 if calendar.isleap(year) else 365) * 24
    times = hourly.get("time")
    if not isinstance(times, list) or len(times) != expected_hours:
        raise ValueError(f"Open-Meteo contains {len(times or [])} hours; expected {expected_hours}")
    missing_counts: dict[str, int] = {}
    for variable in OPEN_METEO_VARIABLES:
        values = hourly.get(variable)
        if not isinstance(values, list) or len(values) != expected_hours:
            raise ValueError(f"Open-Meteo variable {variable} has an invalid hourly series")
        missing_counts[variable] = sum(value is None for value in values)
    return missing_counts


def ingest_open_meteo(
    *,
    location: str,
    latitude: float,
    longitude: float,
    year: int,
    data_root: Path,
    source: SourceDefinition,
    ingest_date: date | None = None,
    fetcher: Callable[[str], HttpPayload] = fetch_http,
) -> BronzeObject:
    url = build_open_meteo_url(latitude=latitude, longitude=longitude, year=year)
    partition_date = ingest_date or datetime.now(UTC).date()
    raw_path = (
        data_root
        / "bronze"
        / source.bronze_prefix
        / f"ingest_date={partition_date.isoformat()}"
        / f"{location}_{year}_hourly.json"
    )
    payload = load_or_fetch_http(raw_path=raw_path, source_url=url, fetcher=fetcher)
    missing_counts = validate_open_meteo_payload(payload.body, year=year)
    return store_bronze_object(
        source=source,
        source_url=url,
        source_period=str(year),
        raw_path=raw_path,
        payload=payload,
        attribution="Weather data by Open-Meteo.com",
        extra_metadata={
            "location_id": location,
            "latitude": latitude,
            "longitude": longitude,
            "time_standard": "UTC",
            "model": "era5",
            "variables": list(OPEN_METEO_VARIABLES),
            "missing_counts": missing_counts,
        },
        validator=lambda raw_bytes: validate_open_meteo_payload(raw_bytes, year=year),
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest Open-Meteo historical weather to Bronze")
    parser.add_argument("--config", type=Path, default=Path("configs/locations/morocco.yml"))
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--delay-seconds", type=float, default=1.0)
    args = parser.parse_args()
    if args.delay_seconds < 0:
        parser.error("--delay-seconds cannot be negative")

    settings = get_settings()
    logging.basicConfig(
        level=settings.log_level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    source = load_source_definition(Path("configs/sources/open_meteo.yml"))
    locations = load_location_config(args.config).locations
    results: list[BronzeObject] = []
    for index, location in enumerate(locations):
        LOGGER.info("Processing %s (%d/%d)", location.name, index + 1, len(locations))
        result = ingest_open_meteo(
            location=location.id,
            latitude=location.latitude,
            longitude=location.longitude,
            year=args.year,
            data_root=settings.data_root,
            source=source,
        )
        results.append(result)
        if not result.reused and args.delay_seconds and index < len(locations) - 1:
            time.sleep(args.delay_seconds)
    LOGGER.info(
        "Open-Meteo batch complete: total=%d downloaded=%d reused=%d",
        len(results),
        sum(not result.reused for result in results),
        sum(result.reused for result in results),
    )


if __name__ == "__main__":
    main()
