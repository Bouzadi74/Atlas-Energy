import hashlib
import io
import json
from datetime import date
from pathlib import Path
from urllib.request import Request

import pytest

from atlas.ingestion.nasa_power import (
    NASA_POWER_PARAMETERS,
    build_nasa_power_url,
    download_nasa_power,
    download_nasa_power_config,
    load_location_config,
)


class FakeResponse(io.BytesIO):
    def __enter__(self) -> "FakeResponse":
        return self

    def __exit__(self, *args: object) -> None:
        self.close()


def make_payload(year: int) -> bytes:
    hour_count = 8784 if year == 2024 else 8760
    timestamps = [f"{year}{index:06d}" for index in range(hour_count)]
    return json.dumps(
        {
            "header": {"time_standard": "UTC"},
            "properties": {
                "parameter": {
                    parameter: dict.fromkeys(timestamps, 1.0)
                    for parameter in NASA_POWER_PARAMETERS
                }
            },
        },
        separators=(",", ":"),
    ).encode()


def test_build_url_contains_hourly_request_contract() -> None:
    url = build_nasa_power_url(latitude=30.9335, longitude=-6.937, year=2024)

    assert url.startswith("https://power.larc.nasa.gov/api/temporal/hourly/point?")
    assert "time-standard=UTC" in url
    assert "start=20240101" in url
    assert "end=20241231" in url


def test_download_writes_validated_raw_data_and_provenance(tmp_path: Path) -> None:
    raw_bytes = make_payload(2024)
    requests: list[tuple[Request, float]] = []

    def opener(request: Request, timeout: float) -> FakeResponse:
        requests.append((request, timeout))
        return FakeResponse(raw_bytes)

    result = download_nasa_power(
        location="Ouarzazate",
        latitude=30.9335,
        longitude=-6.937,
        year=2024,
        data_root=tmp_path,
        ingest_date=date(2026, 9, 23),
        opener=opener,
    )

    assert result.reused is False
    assert result.hour_count == 8784
    assert result.raw_path.read_bytes() == raw_bytes
    assert requests[0][1] == 180
    metadata = json.loads(result.metadata_path.read_text())
    assert metadata["manifest_version"] == 1
    assert metadata["source_id"] == "nasa_power"
    assert metadata["source_period"] == "2024"
    assert metadata["byte_count"] == len(raw_bytes)
    assert metadata["sha256"] == hashlib.sha256(raw_bytes).hexdigest()
    assert metadata["missing_counts"] == dict.fromkeys(NASA_POWER_PARAMETERS, 0)

    reused = download_nasa_power(
        location="Ouarzazate",
        latitude=30.9335,
        longitude=-6.937,
        year=2024,
        data_root=tmp_path,
        ingest_date=date(2026, 9, 23),
        opener=lambda *_args: pytest.fail("existing Bronze data must not be downloaded again"),
    )
    assert reused.reused is True


def test_download_rejects_wrong_hour_count(tmp_path: Path) -> None:
    payload = {
        "header": {"time_standard": "UTC"},
        "properties": {
            "parameter": {parameter: {"2024010100": 1.0} for parameter in NASA_POWER_PARAMETERS}
        },
    }

    with pytest.raises(ValueError, match="expected 8784"):
        download_nasa_power(
            location="Ouarzazate",
            latitude=30.9335,
            longitude=-6.937,
            year=2024,
            data_root=tmp_path,
            ingest_date=date(2026, 9, 23),
            opener=lambda *_args: FakeResponse(json.dumps(payload).encode()),
        )


def test_load_morocco_location_config() -> None:
    config = load_location_config(Path("configs/locations/morocco.yml"))

    assert config.scope == "morocco_representative_energy_zones"
    assert len(config.locations) == 11
    assert {location.id for location in config.locations} >= {"tangier", "ouarzazate", "dakhla"}


def test_config_download_is_sequential_and_uses_one_partition(tmp_path: Path) -> None:
    config_path = tmp_path / "locations.yml"
    config_path.write_text(
        """
version: 1
scope: test
description: Test locations
locations:
  - {id: first, name: First, zone: one, latitude: 30, longitude: -7}
  - {id: second, name: Second, zone: two, latitude: 31, longitude: -8}
""".strip(),
        encoding="utf-8",
    )
    raw_bytes = make_payload(2024)
    requests: list[str] = []
    delays: list[float] = []

    def opener(request: Request, _timeout: float) -> FakeResponse:
        requests.append(request.full_url)
        return FakeResponse(raw_bytes)

    results = download_nasa_power_config(
        config_path=config_path,
        year=2024,
        data_root=tmp_path / "data",
        ingest_date=date(2026, 9, 24),
        delay_seconds=0.25,
        opener=opener,
        sleeper=delays.append,
    )

    assert len(results) == 2
    assert len(requests) == 2
    assert delays == [0.25]
    assert all("ingest_date=2026-09-24" in str(result.raw_path) for result in results)
