"""Audit immutable Bronze objects and their provenance sidecars."""

from __future__ import annotations

import argparse
import hashlib
import json
from dataclasses import dataclass
from pathlib import Path

from atlas.config import get_settings
from atlas.ingestion.contracts import BronzeManifest, load_source_registry


@dataclass(frozen=True)
class AuditIssue:
    severity: str
    path: Path
    message: str


@dataclass(frozen=True)
class AuditReport:
    source_count: int
    object_count: int
    valid_count: int
    issues: tuple[AuditIssue, ...]

    @property
    def passed(self) -> bool:
        return not any(issue.severity == "error" for issue in self.issues)


def audit_bronze(*, data_root: Path, registry_dir: Path) -> AuditReport:
    registry = load_source_registry(registry_dir)
    bronze_root = data_root / "bronze"
    issues: list[AuditIssue] = []
    valid_count = 0
    manifests = list(bronze_root.rglob("*.metadata.json")) if bronze_root.exists() else []
    registered_with_data: set[str] = set()

    for metadata_path in manifests:
        try:
            raw_metadata = json.loads(metadata_path.read_text(encoding="utf-8-sig"))
            if "manifest_version" not in raw_metadata:
                issues.append(
                    AuditIssue(
                        "warning", metadata_path, "legacy manifest; re-ingest to standardize"
                    )
                )
                raw_name = raw_metadata.get("raw_file")
                raw_path = metadata_path.with_name(raw_name) if raw_name else None
                if raw_path is None or not raw_path.is_file():
                    issues.append(
                        AuditIssue("error", metadata_path, "referenced raw file is missing")
                    )
                elif (
                    hashlib.sha256(raw_path.read_bytes()).hexdigest()
                    != raw_metadata.get("sha256")
                ):
                    issues.append(AuditIssue("error", raw_path, "SHA-256 mismatch"))
                else:
                    valid_count += 1
                    if raw_metadata.get("source") == "NASA POWER":
                        registered_with_data.add("nasa_power")
                continue

            manifest = BronzeManifest.model_validate(raw_metadata)
            raw_path = metadata_path.with_name(manifest.raw_file)
            if manifest.source_id not in registry:
                issues.append(AuditIssue("error", metadata_path, "source_id is not registered"))
                continue
            if not raw_path.is_file():
                issues.append(AuditIssue("error", metadata_path, "referenced raw file is missing"))
                continue
            if raw_path.stat().st_size != manifest.byte_count:
                issues.append(AuditIssue("error", raw_path, "byte count mismatch"))
                continue
            if hashlib.sha256(raw_path.read_bytes()).hexdigest() != manifest.sha256:
                issues.append(AuditIssue("error", raw_path, "SHA-256 mismatch"))
                continue
            valid_count += 1
        except (OSError, ValueError, json.JSONDecodeError) as error:
            issues.append(AuditIssue("error", metadata_path, str(error)))

    for metadata_path in manifests:
        try:
            metadata = json.loads(metadata_path.read_text(encoding="utf-8-sig"))
            source_id = metadata.get("source_id")
            if source_id in registry:
                registered_with_data.add(source_id)
        except (OSError, json.JSONDecodeError):
            pass
    for source_id, source in registry.items():
        if source.enabled and source_id not in registered_with_data:
            issues.append(
                AuditIssue(
                    "warning", bronze_root / source.bronze_prefix, "registered source has no data"
                )
            )

    return AuditReport(len(registry), len(manifests), valid_count, tuple(issues))


def main() -> None:
    parser = argparse.ArgumentParser(description="Audit Atlas Bronze data and provenance")
    parser.add_argument("--registry", type=Path, default=Path("configs/sources"))
    args = parser.parse_args()
    report = audit_bronze(data_root=get_settings().data_root, registry_dir=args.registry)
    for issue in report.issues:
        print(f"{issue.severity.upper():7} {issue.path}: {issue.message}")
    print(
        f"Bronze audit: sources={report.source_count} objects={report.object_count} "
        f"valid={report.valid_count} errors="
        f"{sum(issue.severity == 'error' for issue in report.issues)} warnings="
        f"{sum(issue.severity == 'warning' for issue in report.issues)}"
    )
    if not report.passed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
