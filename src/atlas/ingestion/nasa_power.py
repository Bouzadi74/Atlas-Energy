import argparse
import calendar
import hashlib
import json
import logging
import re
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, BinaryIO
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import yaml
from pydantic import BaseModel, ConfigDict, Field

from atlas import __version__
from atlas.config import get_settings
from atlas.ingestion.contracts import SourceDefinition, load_source_definition

LOGGER = logging.getLogger(__name__)
NASA_POWER_ENDPOINT = "https://power.larc.nasa.gov/api/temporal/hourly/point"
NASA_POWER_PARAMETERS = ("ALLSKY_SFC_SW_DWN", "T2M", "WS10M", "WS50M")
MISSING_VALUE = -999


@dataclass(frozen=True)
class DownloadResult:
    raw_path: Path
    metadata_path: Path
    sha256: str
    hour_count: int
    missing_counts: dict[str, int]
    reused: bool


class LocationConfigEntry(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str = Field(pattern=r"^[a-z0-9_]+$")
    name: str = Field(min_length=1)
    zone: str = Field(min_length=1)
    latitude: float = Field(ge=-90, le=90)
    longitude: float = Field(ge=-180, le=180)


class LocationConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    version: int = Field(ge=1, le=1)
    scope: str = Field(min_length=1)
    description: str = Field(min_length=1)
    locations: list[LocationConfigEntry] = Field(min_length=1)


def _slug(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "_", value.strip().lower()).strip("_")
    if not slug:
        raise ValueError("location must contain a letter or number")
    return slug


def build_nasa_power_url(*, latitude: float, longitude: float, year: int) -> str:
    if not -90 <= latitude <= 90:
        raise ValueError("latitude must be between -90 and 90")
    if not -180 <= longitude <= 180:
        raise ValueError("longitude must be between -180 and 180")
    if not 2001 <= year <= datetime.now(UTC).year:
        raise ValueError("year must be between 2001 and the current year")

    query = urlencode(
        {
            "parameters": ",".join(NASA_POWER_PARAMETERS),
            "community": "RE",
            "longitude": f"{longitude:.4f}",
            "latitude": f"{latitude:.4f}",
            "start": f"{year}0101",
            "end": f"{year}1231",
            "format": "JSON",
            "time-standard": "UTC",
        }
    )
    return f"{NASA_POWER_ENDPOINT}?{query}"


def validate_nasa_power_payload(
    payload: dict[str, Any],
    *,
    year: int,
) -> tuple[int, dict[str, int]]:
    try:
        header = payload["header"]
        parameters = payload["properties"]["parameter"]
    except (KeyError, TypeError) as error:
        raise ValueError("NASA POWER response is missing its header or parameters") from error

    if header.get("time_standard") != "UTC":
        raise ValueError("NASA POWER response is not in UTC")

    expected_hours = (366 if calendar.isleap(year) else 365) * 24
    missing_counts: dict[str, int] = {}
    for parameter in NASA_POWER_PARAMETERS:
        values = parameters.get(parameter)
        if not isinstance(values, dict):
            raise ValueError(f"NASA POWER response is missing parameter {parameter}")
        if len(values) != expected_hours:
            raise ValueError(
                f"{parameter} contains {len(values)} hours; expected {expected_hours} for {year}"
            )
        missing_counts[parameter] = sum(value == MISSING_VALUE for value in values.values())

    return expected_hours, missing_counts


def _open_url(request: Request, timeout: float) -> BinaryIO:
    return urlopen(request, timeout=timeout)  # noqa: S310 - fixed HTTPS endpoint


def load_location_config(path: Path) -> LocationConfig:
    try:
        content = yaml.safe_load(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        raise FileNotFoundError(f"Location configuration not found: {path}") from None
    except yaml.YAMLError as error:
        raise ValueError(f"Invalid YAML in location configuration {path}: {error}") from error

    config = LocationConfig.model_validate(content)
    identifiers = [location.id for location in config.locations]
    if len(identifiers) != len(set(identifiers)):
        raise ValueError(f"Location configuration contains duplicate IDs: {path}")
    return config


def download_nasa_power(
    *,
    location: str,
    latitude: float,
    longitude: float,
    year: int,
    data_root: Path,
    ingest_date: date | None = None,
    timeout_seconds: float = 180,
    opener: Callable[[Request, float], BinaryIO] = _open_url,
    source: SourceDefinition | None = None,
) -> DownloadResult:
    """Download and validate one immutable NASA POWER hourly Bronze object."""

    location_slug = _slug(location)
    retrieved_at = datetime.now(UTC)
    partition_date = ingest_date or retrieved_at.date()
    source = source or load_source_definition(Path("configs/sources/nasa_power.yml"))
    output_dir = (
        data_root
        / "bronze"
        / source.bronze_prefix
        / f"ingest_date={partition_date.isoformat()}"
    )
    raw_path = output_dir / f"{location_slug}_{year}_hourly.json"
    metadata_path = output_dir / f"{location_slug}_{year}_hourly.metadata.json"
    url = build_nasa_power_url(latitude=latitude, longitude=longitude, year=year)

    if raw_path.exists():
        raw_bytes = raw_path.read_bytes()
        reused = True
    else:
        request = Request(url, headers={"User-Agent": "atlas-energy/0.1"})
        with opener(request, timeout_seconds) as response:
            raw_bytes = response.read()
        reused = False

    try:
        payload = json.loads(raw_bytes)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("NASA POWER response is not valid UTF-8 JSON") from error
    if not isinstance(payload, dict):
        raise ValueError("NASA POWER response must be a JSON object")

    hour_count, missing_counts = validate_nasa_power_payload(payload, year=year)
    sha256 = hashlib.sha256(raw_bytes).hexdigest()

    if reused:
        if not metadata_path.exists():
            raise FileExistsError(f"Raw file exists without metadata: {raw_path}")
        metadata = json.loads(metadata_path.read_text(encoding="utf-8-sig"))
        if metadata.get("sha256", "").lower() != sha256:
            raise ValueError(f"Checksum mismatch for existing Bronze file: {raw_path}")
        if (
            str(metadata.get("location_name", "")).casefold() != location.casefold()
            or float(metadata.get("latitude")) != latitude
            or float(metadata.get("longitude")) != longitude
        ):
            raise ValueError("Existing Bronze metadata does not match the requested location")
    else:
        output_dir.mkdir(parents=True, exist_ok=True)
        metadata = {
            "manifest_version": 1,
            "source_id": source.source_id,
            "source_name": source.name,
            "source_url": url,
            "landing_page": source.landing_page,
            "source_period": str(year),
            "content_type": "application/json",
            "byte_count": len(raw_bytes),
            "license_name": source.license.name,
            "license_url": source.license.url,
            "attribution": "NASA POWER Project, NASA Langley Research Center",
            "ingestion_version": __version__,
            "source": "NASA POWER",
            "endpoint": url,
            "retrieved_at_utc": retrieved_at.isoformat().replace("+00:00", "Z"),
            "location_name": location,
            "latitude": latitude,
            "longitude": longitude,
            "start_date": f"{year}-01-01",
            "end_date": f"{year}-12-31",
            "time_standard": "UTC",
            "parameters": list(NASA_POWER_PARAMETERS),
            "provenance_classification": "DERIVED",
            "raw_file": raw_path.name,
            "hour_count": hour_count,
            "missing_counts": missing_counts,
            "sha256": sha256,
        }
        try:
            with raw_path.open("xb") as raw_file:
                raw_file.write(raw_bytes)
            with metadata_path.open("x", encoding="utf-8", newline="\n") as metadata_file:
                json.dump(metadata, metadata_file, indent=2, ensure_ascii=False)
                metadata_file.write("\n")
        except Exception:
            if raw_path.exists() and not metadata_path.exists():
                raw_path.unlink()
            raise

    return DownloadResult(
        raw_path=raw_path,
        metadata_path=metadata_path,
        sha256=sha256,
        hour_count=hour_count,
        missing_counts=missing_counts,
        reused=reused,
    )


def download_nasa_power_config(
    *,
    config_path: Path,
    year: int,
    data_root: Path,
    ingest_date: date | None = None,
    delay_seconds: float = 1.0,
    opener: Callable[[Request, float], BinaryIO] = _open_url,
    sleeper: Callable[[float], None] = time.sleep,
) -> list[DownloadResult]:
    """Download a versioned location set sequentially and return each validated result."""

    if delay_seconds < 0:
        raise ValueError("delay_seconds cannot be negative")
    config = load_location_config(config_path)
    source = load_source_definition(Path("configs/sources/nasa_power.yml"))
    partition_date = ingest_date or datetime.now(UTC).date()
    results: list[DownloadResult] = []

    for index, location in enumerate(config.locations):
        LOGGER.info(
            "Processing %s (%d/%d)", location.name, index + 1, len(config.locations)
        )
        result = download_nasa_power(
            location=location.id,
            latitude=location.latitude,
            longitude=location.longitude,
            year=year,
            data_root=data_root,
            ingest_date=partition_date,
            opener=opener,
            source=source,
        )
        results.append(result)
        if not result.reused and delay_seconds and index < len(config.locations) - 1:
            sleeper(delay_seconds)

    return results


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Download NASA POWER hourly data to Bronze")
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--config", type=Path, help="YAML file containing multiple locations")
    source.add_argument("--location", help="Single location name or identifier")
    parser.add_argument("--latitude", type=float, help="Required with --location")
    parser.add_argument("--longitude", type=float, help="Required with --location")
    parser.add_argument("--year", required=True, type=int)
    parser.add_argument(
        "--delay-seconds",
        type=float,
        default=1.0,
        help="Delay between new downloads in config mode (default: 1)",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    settings = get_settings()
    logging.basicConfig(
        level=settings.log_level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    if args.config is not None:
        results = download_nasa_power_config(
            config_path=args.config,
            year=args.year,
            data_root=settings.data_root,
            delay_seconds=args.delay_seconds,
        )
        LOGGER.info(
            "Location batch complete: total=%d downloaded=%d reused=%d",
            len(results),
            sum(not result.reused for result in results),
            sum(result.reused for result in results),
        )
        return

    if args.latitude is None or args.longitude is None:
        build_parser().error("--latitude and --longitude are required with --location")
    result = download_nasa_power(
        location=args.location,
        latitude=args.latitude,
        longitude=args.longitude,
        year=args.year,
        data_root=settings.data_root,
    )
    action = "Reused and verified" if result.reused else "Downloaded and verified"
    LOGGER.info("%s %d hourly rows at %s", action, result.hour_count, result.raw_path)
    LOGGER.info("Missing values: %s", result.missing_counts)
    LOGGER.info("SHA-256: %s", result.sha256)


if __name__ == "__main__":
    main()
