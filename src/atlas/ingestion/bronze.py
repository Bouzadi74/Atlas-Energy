"""Safe, immutable storage primitives for the Atlas Bronze layer."""

from __future__ import annotations

import hashlib
import json
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, BinaryIO
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from atlas import __version__
from atlas.ingestion.contracts import BronzeManifest, SourceDefinition


@dataclass(frozen=True)
class BronzeObject:
    raw_path: Path
    metadata_path: Path
    sha256: str
    reused: bool


@dataclass(frozen=True)
class HttpPayload:
    body: bytes
    content_type: str
    etag: str | None
    last_modified: str | None


def _open_url(request: Request, timeout: float) -> BinaryIO:
    return urlopen(request, timeout=timeout)  # noqa: S310 - caller supplies approved HTTPS URL


def fetch_http(
    url: str,
    *,
    timeout_seconds: float = 180,
    attempts: int = 3,
    opener: Callable[[Request, float], BinaryIO] = _open_url,
    sleeper: Callable[[float], None] = time.sleep,
) -> HttpPayload:
    """Fetch one HTTPS resource with bounded retries and useful response metadata."""

    if not url.startswith("https://"):
        raise ValueError("Bronze downloads must use HTTPS")
    if attempts < 1:
        raise ValueError("attempts must be at least 1")

    request = Request(url, headers={"User-Agent": f"atlas-energy/{__version__}"})
    for attempt in range(1, attempts + 1):
        try:
            with opener(request, timeout_seconds) as response:
                body = response.read()
                headers = getattr(response, "headers", {})
                content_type = headers.get("Content-Type", "application/octet-stream")
                return HttpPayload(
                    body=body,
                    content_type=content_type.split(";", 1)[0].strip().lower(),
                    etag=headers.get("ETag"),
                    last_modified=headers.get("Last-Modified"),
                )
        except (HTTPError, URLError, TimeoutError) as error:
            if attempt == attempts:
                message = f"Download failed after {attempts} attempts: {url}"
                raise ConnectionError(message) from error
            sleeper(float(2 ** (attempt - 1)))
    raise AssertionError("unreachable")


def load_or_fetch_http(
    *,
    raw_path: Path,
    source_url: str,
    fetcher: Callable[[str], HttpPayload] = fetch_http,
) -> HttpPayload:
    """Reuse local immutable bytes when present, otherwise fetch the source URL."""

    if not raw_path.exists():
        return fetcher(source_url)
    metadata_path = raw_path.with_name(f"{raw_path.name}.metadata.json")
    if not metadata_path.exists():
        raise FileExistsError(f"Raw file exists without metadata: {raw_path}")
    metadata = json.loads(metadata_path.read_text(encoding="utf-8-sig"))
    return HttpPayload(
        body=raw_path.read_bytes(),
        content_type=metadata.get("content_type", "application/octet-stream"),
        etag=metadata.get("http_etag"),
        last_modified=metadata.get("http_last_modified"),
    )


def store_bronze_object(
    *,
    source: SourceDefinition,
    source_url: str,
    source_period: str,
    raw_path: Path,
    payload: HttpPayload,
    attribution: str,
    extra_metadata: dict[str, Any] | None = None,
    validator: Callable[[bytes], None] | None = None,
    retrieved_at: datetime | None = None,
) -> BronzeObject:
    """Validate and store source bytes plus a standardized provenance sidecar."""

    metadata_path = raw_path.with_name(f"{raw_path.name}.metadata.json")
    if not payload.body:
        raise ValueError(f"Refusing to store an empty Bronze object: {raw_path}")
    if validator is not None:
        validator(payload.body)
    sha256 = hashlib.sha256(payload.body).hexdigest()

    if raw_path.exists():
        if not metadata_path.exists():
            raise FileExistsError(f"Raw file exists without metadata: {raw_path}")
        existing_sha256 = hashlib.sha256(raw_path.read_bytes()).hexdigest()
        if existing_sha256 != sha256:
            raise FileExistsError(
                f"Immutable Bronze path already contains different bytes: {raw_path}"
            )
        metadata = BronzeManifest.model_validate_json(metadata_path.read_text(encoding="utf-8-sig"))
        if metadata.sha256 != existing_sha256 or metadata.source_id != source.source_id:
            raise ValueError(f"Existing Bronze metadata is inconsistent: {metadata_path}")
        return BronzeObject(raw_path, metadata_path, sha256, True)

    timestamp = retrieved_at or datetime.now(UTC)
    manifest_data: dict[str, Any] = {
        "manifest_version": 1,
        "source_id": source.source_id,
        "source_name": source.name,
        "source_url": source_url,
        "landing_page": source.landing_page,
        "retrieved_at_utc": timestamp,
        "source_period": source_period,
        "content_type": payload.content_type,
        "raw_file": raw_path.name,
        "byte_count": len(payload.body),
        "sha256": sha256,
        "license_name": source.license.name,
        "license_url": source.license.url,
        "attribution": attribution,
        "ingestion_version": __version__,
        "http_etag": payload.etag,
        "http_last_modified": payload.last_modified,
    }
    if extra_metadata:
        manifest_data.update(extra_metadata)
    manifest = BronzeManifest.model_validate(manifest_data)

    raw_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_raw = raw_path.with_name(f".{raw_path.name}.tmp")
    temporary_metadata = metadata_path.with_name(f".{metadata_path.name}.tmp")
    try:
        temporary_raw.write_bytes(payload.body)
        temporary_metadata.write_text(
            json.dumps(manifest.model_dump(mode="json"), indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        temporary_raw.replace(raw_path)
        temporary_metadata.replace(metadata_path)
    except Exception:
        temporary_raw.unlink(missing_ok=True)
        temporary_metadata.unlink(missing_ok=True)
        if raw_path.exists() and not metadata_path.exists():
            raw_path.unlink()
        raise

    return BronzeObject(raw_path, metadata_path, sha256, False)
