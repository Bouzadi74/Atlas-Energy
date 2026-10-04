"""Contracts shared by every Atlas Bronze data source."""

from __future__ import annotations

import re
from datetime import datetime
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator


class SourceLicense(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1)
    url: str = Field(min_length=1)
    attribution_required: bool
    usage: str = Field(min_length=1)


class SourceAccess(BaseModel):
    model_config = ConfigDict(extra="forbid")

    mode: Literal["api", "direct_download", "curated_urls", "manual_assisted", "git_release"]
    automated: bool
    authentication: Literal["none", "optional", "required", "manual_terms"]
    notes: str = Field(min_length=1)


class SourceCoverage(BaseModel):
    model_config = ConfigDict(extra="forbid")

    geography: str = Field(min_length=1)
    period: str = Field(min_length=1)
    cadence: str = Field(min_length=1)


class SourceDefinition(BaseModel):
    """Machine-readable entry in the Atlas source registry."""

    model_config = ConfigDict(extra="forbid")

    version: Literal[1]
    source_id: str = Field(pattern=r"^[a-z0-9_]+$")
    name: str = Field(min_length=1)
    publisher: str = Field(min_length=1)
    category: Literal["weather", "statistics", "report", "technology"]
    landing_page: str = Field(min_length=1)
    formats: list[str] = Field(min_length=1)
    bronze_prefix: str = Field(pattern=r"^[a-z0-9_/]+$")
    access: SourceAccess
    coverage: SourceCoverage
    license: SourceLicense
    enabled: bool = True

    @field_validator("landing_page")
    @classmethod
    def validate_landing_page(cls, value: str) -> str:
        if not re.match(r"^https://", value):
            raise ValueError("landing_page must use HTTPS")
        return value


class BronzeManifest(BaseModel):
    """Sidecar provenance record written beside an immutable Bronze object."""

    model_config = ConfigDict(extra="allow")

    manifest_version: Literal[1] = 1
    source_id: str = Field(pattern=r"^[a-z0-9_]+$")
    source_name: str = Field(min_length=1)
    source_url: str = Field(min_length=1)
    landing_page: str = Field(min_length=1)
    retrieved_at_utc: datetime
    source_period: str = Field(min_length=1)
    content_type: str = Field(min_length=1)
    raw_file: str = Field(min_length=1)
    byte_count: int = Field(gt=0)
    sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    license_name: str = Field(min_length=1)
    license_url: str = Field(min_length=1)
    attribution: str = Field(min_length=1)
    ingestion_version: str = Field(min_length=1)


def load_source_definition(path: Path) -> SourceDefinition:
    try:
        content = yaml.safe_load(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        raise FileNotFoundError(f"Source definition not found: {path}") from None
    except yaml.YAMLError as error:
        raise ValueError(f"Invalid YAML in source definition {path}: {error}") from error
    return SourceDefinition.model_validate(content)


def load_source_registry(directory: Path) -> dict[str, SourceDefinition]:
    if not directory.is_dir():
        raise FileNotFoundError(f"Source registry directory not found: {directory}")

    registry: dict[str, SourceDefinition] = {}
    for path in sorted(directory.glob("*.yml")):
        definition = load_source_definition(path)
        if definition.source_id in registry:
            raise ValueError(f"Duplicate source_id {definition.source_id!r} in {directory}")
        registry[definition.source_id] = definition
    if not registry:
        raise ValueError(f"Source registry is empty: {directory}")
    return registry
