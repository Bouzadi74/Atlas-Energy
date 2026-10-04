import hashlib
import json
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from atlas.ingestion.contracts import BronzeManifest
from atlas.transforms.structured_silver import (
    StructuredSilverConfig,
    VerifiedBronzeObject,
    load_config,
    normalize_irena_values,
    parse_world_bank,
)


def silver_config() -> StructuredSilverConfig:
    return StructuredSilverConfig(
        version=1,
        country_code="MAR",
        start_year=2010,
        end_year=2024,
        owid_metrics=["population"],
        technology_model_years=[2020],
    )


def test_irena_normalization_filters_country_and_preserves_empty_measure() -> None:
    values = [
        "Africa",
        "Northern Africa",
        "Morocco",
        "MAR",
        504,
        "Renewable",
        "Solar",
        "Solar photovoltaic",
        "Solar photovoltaic",
        "All types",
        2024,
        100.0,
        50.0,
        None,
        None,
        None,
        12.5,
    ]

    row = normalize_irena_values(values, silver_config())

    assert row is not None
    assert row["installed_capacity_mw"] == 50.0
    assert row["heat_generation_tj"] is None
    values[3] = "DZA"
    assert normalize_irena_values(values, silver_config()) is None


def test_world_bank_legacy_manifest_uses_registry_unit(tmp_path: Path) -> None:
    payload = [
        {"pages": 1},
        [
            {
                "indicator": {"id": "SP.POP.TOTL", "value": "Population"},
                "country": {"value": "Morocco"},
                "countryiso3code": "MAR",
                "date": "2024",
                "value": 38_000_000,
            }
        ],
    ]
    raw_path = tmp_path / "population.json"
    raw_path.write_text(json.dumps(payload), encoding="utf-8")
    raw_bytes = raw_path.read_bytes()
    manifest = BronzeManifest(
        source_id="world_bank",
        source_name="World Bank Indicators API",
        source_url="https://api.worldbank.org/example",
        landing_page="https://data.worldbank.org",
        retrieved_at_utc=datetime(2026, 9, 24, tzinfo=UTC),
        source_period="2010-2024",
        content_type="application/json",
        raw_file=raw_path.name,
        byte_count=len(raw_bytes),
        sha256=hashlib.sha256(raw_bytes).hexdigest(),
        license_name="World Bank Dataset Terms",
        license_url="https://www.worldbank.org/terms",
        attribution="World Bank Open Data",
        ingestion_version="0.1.0",
    )
    item = VerifiedBronzeObject(
        raw_path, tmp_path / "metadata.json", manifest, date(2026, 9, 24)
    )

    rows = parse_world_bank(
        [item], silver_config(), "test-version", {"SP.POP.TOTL": "people"}
    )

    assert rows[0]["unit"] == "people"
    assert rows[0]["value"] == 38_000_000.0


def test_structured_config_rejects_duplicate_owid_metrics(tmp_path: Path) -> None:
    config_path = tmp_path / "structured.yml"
    config_path.write_text(
        """version: 1
country_code: MAR
start_year: 2010
end_year: 2024
owid_metrics: [population, population]
technology_model_years: [2020]
""",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="duplicates"):
        load_config(config_path)
