from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from atlas.model_inputs.package import load_config, reconcile_leap_year_rows


def test_model_input_config_is_valid() -> None:
    config = load_config(Path("configs/model_inputs/morocco_baseline.yml"))

    assert config.baseline_year == 2024
    assert config.planning_year == 2030
    assert config.calendar.baseline_hours == 8784
    assert config.calendar.planning_hours == 8760
    assert config.capacity.residual_technology == "fossil_other"


def test_leap_conversion_preserves_energy_and_peak() -> None:
    start = datetime(2024, 1, 1, tzinfo=UTC)
    rows = [
        {
            "timestamp_utc": start + timedelta(hours=index),
            "demand_mw": 4000.0 + index % 100,
            "solar_capacity_factor": (index % 24) / 24.0,
            "wind_capacity_factor": 0.4,
        }
        for index in range(8784)
    ]

    converted = reconcile_leap_year_rows(rows, planning_year=2030)

    assert len(converted) == 8760
    assert not any(
        row["source_timestamp_utc"].month == 2
        and row["source_timestamp_utc"].day == 29
        for row in converted
    )
    assert sum(row["demand_mw"] for row in converted) == pytest.approx(
        sum(row["demand_mw"] for row in rows), abs=0.01
    )
    assert max(row["demand_mw"] for row in converted) == pytest.approx(
        max(row["demand_mw"] for row in rows)
    )
    assert [row["snapshot_index"] for row in converted] == list(range(8760))
    assert all(row["snapshot_weight_hours"] == 1.0 for row in converted)


def test_leap_conversion_rejects_wrong_coverage() -> None:
    with pytest.raises(ValueError, match="expected 8784"):
        reconcile_leap_year_rows([], planning_year=2030)
