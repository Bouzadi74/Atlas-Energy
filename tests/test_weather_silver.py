import json
from datetime import date
from pathlib import Path

import pytest

from atlas.transforms.weather_silver import discover_bronze_inputs


def test_discovers_latest_complete_verified_partition(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    config = tmp_path / "locations.yml"
    config.write_text(
        """
version: 1
scope: test
description: Test scope
locations:
  - {id: test_site, name: Test Site, zone: test_zone, latitude: 30, longitude: -7}
""".strip(),
        encoding="utf-8",
    )
    partition = data_root / "bronze" / "nasa_power" / "ingest_date=2026-09-24"
    partition.mkdir(parents=True)
    raw_path = partition / "test_site_2024_hourly.json"
    timestamps = {f"2024{index:06d}": 1.0 for index in range(8784)}
    payload = {
        "header": {"time_standard": "UTC"},
        "properties": {
            "parameter": {
                parameter: timestamps
                for parameter in ("ALLSKY_SFC_SW_DWN", "T2M", "WS10M", "WS50M")
            }
        },
    }
    raw_bytes = json.dumps(payload, separators=(",", ":")).encode()
    raw_path.write_bytes(raw_bytes)
    import hashlib

    metadata = {
        "raw_file": raw_path.name,
        "location_name": "test_site",
        "latitude": 30,
        "longitude": -7,
        "provenance_classification": "DERIVED",
        "sha256": hashlib.sha256(raw_bytes).hexdigest(),
    }
    (partition / "test_site_2024_hourly.metadata.json").write_text(
        json.dumps(metadata), encoding="utf-8"
    )

    inputs = discover_bronze_inputs(
        data_root=data_root,
        config_path=config,
        year=2024,
    )

    assert len(inputs) == 1
    assert inputs[0].location.zone == "test_zone"
    assert inputs[0].ingest_date == date(2026, 9, 24)


def test_rejects_incomplete_requested_partition(tmp_path: Path) -> None:
    config = tmp_path / "locations.yml"
    config.write_text(
        """
version: 1
scope: test
description: Test scope
locations:
  - {id: missing, name: Missing, zone: test, latitude: 30, longitude: -7}
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(FileNotFoundError, match="incomplete"):
        discover_bronze_inputs(
            data_root=tmp_path / "data",
            config_path=config,
            year=2024,
            ingest_date=date(2026, 9, 24),
        )
