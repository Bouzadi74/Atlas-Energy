"""Register an untouched manually acquired file as an immutable Bronze object."""

from __future__ import annotations

import argparse
import mimetypes
from datetime import UTC, date, datetime
from pathlib import Path

from atlas.config import get_settings
from atlas.ingestion.bronze import HttpPayload, store_bronze_object
from atlas.ingestion.contracts import load_source_definition


def _validate_signature(raw_bytes: bytes, suffix: str) -> None:
    if suffix == ".pdf" and not raw_bytes.startswith(b"%PDF-"):
        raise ValueError("File extension is PDF but the PDF signature is missing")
    if suffix in {".xlsx", ".xlsb"} and not raw_bytes.startswith(b"PK"):
        raise ValueError(f"File extension is {suffix.upper()} but the ZIP signature is missing")
    if suffix in {".csv", ".json"}:
        try:
            raw_bytes.decode("utf-8-sig")
        except UnicodeDecodeError as error:
            raise ValueError(f"{suffix} file is not valid UTF-8 text") from error


def register_file(
    *,
    source_id: str,
    input_path: Path,
    source_url: str,
    source_period: str,
    attribution: str,
    data_root: Path,
    registry_dir: Path,
    ingest_date: date | None = None,
) -> Path:
    source = load_source_definition(registry_dir / f"{source_id}.yml")
    if not input_path.is_file():
        raise FileNotFoundError(f"Inbox file not found: {input_path}")
    if not source_url.startswith("https://"):
        raise ValueError("source_url must be the publisher's HTTPS URL")
    suffix = input_path.suffix.lower()
    allowed_suffixes = {f".{value.lower()}" for value in source.formats}
    if suffix not in allowed_suffixes:
        raise ValueError(f"{suffix or 'extensionless file'} is not allowed for {source_id}")
    raw_bytes = input_path.read_bytes()
    _validate_signature(raw_bytes, suffix)
    partition_date = ingest_date or datetime.now(UTC).date()
    raw_path = (
        data_root
        / "bronze"
        / source.bronze_prefix
        / f"ingest_date={partition_date.isoformat()}"
        / input_path.name
    )
    content_type = mimetypes.guess_type(input_path.name)[0] or "application/octet-stream"
    result = store_bronze_object(
        source=source,
        source_url=source_url,
        source_period=source_period,
        raw_path=raw_path,
        payload=HttpPayload(raw_bytes, content_type, None, None),
        attribution=attribution,
        extra_metadata={"acquisition_mode": "manual_assisted", "inbox_file": input_path.name},
        validator=lambda body: _validate_signature(body, suffix),
    )
    return result.raw_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Register an untouched file in Atlas Bronze")
    parser.add_argument("--source", required=True)
    parser.add_argument("--file", required=True, type=Path)
    parser.add_argument("--source-url", required=True)
    parser.add_argument("--source-period", required=True)
    parser.add_argument("--attribution", required=True)
    parser.add_argument("--registry", type=Path, default=Path("configs/sources"))
    args = parser.parse_args()
    path = register_file(
        source_id=args.source,
        input_path=args.file,
        source_url=args.source_url,
        source_period=args.source_period,
        attribution=args.attribution,
        data_root=get_settings().data_root,
        registry_dir=args.registry,
    )
    print(f"Registered Bronze object: {path}")


if __name__ == "__main__":
    main()
