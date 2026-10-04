import hashlib
import io
import json
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from atlas.ingestion.audit import audit_bronze
from atlas.ingestion.bronze import HttpPayload, fetch_http, store_bronze_object
from atlas.ingestion.contracts import load_source_definition, load_source_registry
from atlas.ingestion.file_sources import ingest_direct_downloads, load_download_config
from atlas.ingestion.open_meteo import (
    OPEN_METEO_VARIABLES,
    build_open_meteo_url,
    ingest_open_meteo,
)
from atlas.ingestion.register_file import register_file
from atlas.ingestion.world_bank import (
    Indicator,
    build_world_bank_url,
    ingest_world_bank_indicator,
    load_world_bank_config,
)


class FakeResponse(io.BytesIO):
    headers = {
        "Content-Type": "application/json; charset=utf-8",
        "ETag": '"example"',
    }

    def __enter__(self) -> "FakeResponse":
        return self

    def __exit__(self, *args: object) -> None:
        self.close()


def test_source_registry_contains_approved_sources() -> None:
    registry = load_source_registry(Path("configs/sources"))

    assert set(registry) == {
        "anre",
        "ember",
        "irena",
        "mef",
        "nasa_power",
        "onee",
        "open_meteo",
        "owid",
        "technology_data",
        "world_bank",
        "zenodo_morocco_technoeconomic",
    }
    assert registry["open_meteo"].license.usage.startswith("Non-commercial")


def test_fetch_http_captures_response_metadata() -> None:
    payload = fetch_http(
        "https://example.test/data",
        opener=lambda _request, _timeout: FakeResponse(b'{"ok":true}'),
    )

    assert payload.content_type == "application/json"
    assert payload.etag == '"example"'
    assert payload.body == b'{"ok":true}'


def test_store_and_audit_standard_bronze_object(tmp_path: Path) -> None:
    source = load_source_definition(Path("configs/sources/world_bank.yml"))
    raw_path = tmp_path / "data/bronze/statistics/world_bank/ingest_date=2026-09-24/a.json"
    payload = HttpPayload(b'{"value":1}', "application/json", None, None)

    result = store_bronze_object(
        source=source,
        source_url="https://example.test/a.json",
        source_period="2024",
        raw_path=raw_path,
        payload=payload,
        attribution="World Bank Open Data",
        retrieved_at=datetime(2026, 9, 24, tzinfo=UTC),
    )
    report = audit_bronze(data_root=tmp_path / "data", registry_dir=Path("configs/sources"))

    assert result.sha256 == hashlib.sha256(payload.body).hexdigest()
    assert report.object_count == 1
    assert report.valid_count == 1
    assert report.passed


def test_open_meteo_ingestion_is_validated_and_idempotent(tmp_path: Path) -> None:
    hours = 8784
    hourly = {"time": [f"t{i}" for i in range(hours)]}
    hourly.update({variable: [1.0] * hours for variable in OPEN_METEO_VARIABLES})
    raw_bytes = json.dumps({"utc_offset_seconds": 0, "hourly": hourly}).encode()
    calls: list[str] = []

    def fetcher(url: str) -> HttpPayload:
        calls.append(url)
        return HttpPayload(raw_bytes, "application/json", None, None)

    source = load_source_definition(Path("configs/sources/open_meteo.yml"))
    kwargs = {
        "location": "ouarzazate",
        "latitude": 30.9335,
        "longitude": -6.937,
        "year": 2024,
        "data_root": tmp_path,
        "source": source,
        "ingest_date": date(2026, 9, 24),
        "fetcher": fetcher,
    }
    first = ingest_open_meteo(**kwargs)
    second = ingest_open_meteo(**kwargs)

    assert first.reused is False
    assert second.reused is True
    assert len(calls) == 1
    assert "models=era5" in build_open_meteo_url(latitude=30, longitude=-7, year=2024)


def test_world_bank_config_and_ingestion(tmp_path: Path) -> None:
    config = load_world_bank_config(Path("configs/indicators/world_bank.yml"))
    indicator = Indicator(code="SP.POP.TOTL", name="Population")
    observations = [
        {
            "indicator": {"id": indicator.code},
            "countryiso3code": "MAR",
            "date": "2024",
            "value": 1,
        }
    ]
    raw_bytes = json.dumps([{"pages": 1}, observations]).encode()
    source = load_source_definition(Path("configs/sources/world_bank.yml"))

    result = ingest_world_bank_indicator(
        country_code="MAR",
        indicator=indicator,
        start_year=2010,
        end_year=2024,
        data_root=tmp_path,
        source=source,
        ingest_date=date(2026, 9, 24),
        fetcher=lambda _url: HttpPayload(raw_bytes, "application/json", None, None),
    )
    metadata = json.loads(result.metadata_path.read_text(encoding="utf-8"))

    assert config.country_code == "MAR"
    assert metadata["observation_count"] == 1
    assert "date=2010%3A2024" in build_world_bank_url(
        country_code="MAR",
        indicator_code=indicator.code,
        start_year=2010,
        end_year=2024,
    )


def test_world_bank_config_rejects_reversed_period(tmp_path: Path) -> None:
    path = tmp_path / "world_bank.yml"
    path.write_text(
        """version: 1
country_code: MAR
start_year: 2024
end_year: 2010
indicators:
  - {code: SP.POP.TOTL, name: Population}
""",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="cannot precede"):
        load_world_bank_config(path)


def test_direct_download_config_and_ingestion(tmp_path: Path) -> None:
    config = load_download_config(Path("configs/downloads/technology_data.yml"))
    csv_bytes = b"technology,parameter,value,unit,source\nsolar,investment,1,EUR/kW,test\n"

    results = ingest_direct_downloads(
        config_path=Path("configs/downloads/technology_data.yml"),
        registry_dir=Path("configs/sources"),
        data_root=tmp_path,
        ingest_date=date(2026, 9, 24),
        fetcher=lambda _url: HttpPayload(csv_bytes, "text/csv", None, None),
    )

    assert config.release == "v0.15.0"
    assert len(results) == 4
    assert all(result.raw_path.is_file() for result in results)


def test_manual_file_registration_preserves_input(tmp_path: Path) -> None:
    inbox_file = tmp_path / "report.pdf"
    inbox_file.write_bytes(b"%PDF-1.7\nacademic report")

    raw_path = register_file(
        source_id="anre",
        input_path=inbox_file,
        source_url="https://anre.ma/example.pdf",
        source_period="2024",
        attribution="ANRE, Rapport annuel 2024",
        data_root=tmp_path / "data",
        registry_dir=Path("configs/sources"),
        ingest_date=date(2026, 9, 24),
    )

    assert inbox_file.is_file()
    assert raw_path.read_bytes() == inbox_file.read_bytes()
    assert raw_path.with_name(f"{raw_path.name}.metadata.json").is_file()


def test_irena_xlsb_registration_is_supported(tmp_path: Path) -> None:
    inbox_file = tmp_path / "IRENA_Stats_Tool_v2.xlsb"
    inbox_file.write_bytes(b"PK\x03\x04binary-workbook")

    raw_path = register_file(
        source_id="irena",
        input_path=inbox_file,
        source_url="https://www.irena.org/Data/Downloads/Tools",
        source_period="2000-2024;release=2025-07-31",
        attribution="IRENASTAT renewable capacity and generation statistics",
        data_root=tmp_path / "data",
        registry_dir=Path("configs/sources"),
        ingest_date=date(2026, 9, 24),
    )

    assert raw_path.suffix == ".xlsb"
    assert raw_path.read_bytes() == inbox_file.read_bytes()
