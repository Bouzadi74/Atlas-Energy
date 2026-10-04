from datetime import UTC, datetime, timedelta

import pytest

from atlas.transforms.silver_completion import (
    calibrate_demand_profile,
    normalize_technology_unit,
    reconciliation_status,
)


def test_demand_profile_matches_all_three_calibration_targets() -> None:
    start = datetime(2024, 1, 1, tzinfo=UTC)
    timestamps = [start + timedelta(hours=index) for index in range(8784)]
    temperatures = [
        18.0 + 9.0 * ((timestamp.timetuple().tm_yday - 1) / 365.0)
        for timestamp in timestamps
    ]

    rows = calibrate_demand_profile(
        timestamps,
        temperatures,
        annual_energy_mwh=45_713_100.0,
        peak_power_mw=7_580.0,
        peak_day_energy_mwh=158_829.0,
        peak_date=datetime(2024, 7, 25, tzinfo=UTC).date(),
    )

    assert len(rows) == 8784
    assert sum(row["hourly_energy_mwh"] for row in rows) == pytest.approx(45_713_100.0)
    assert max(row["demand_mw"] for row in rows) == pytest.approx(7_580.0)
    assert sum(
        row["hourly_energy_mwh"] for row in rows if row["is_calibration_peak_day"]
    ) == pytest.approx(158_829.0)
    assert all(row["demand_mw"] > 0 for row in rows)


@pytest.mark.parametrize(
    ("source", "parameter", "expected"),
    [
        ("p.u.", "efficiency", "per_unit"),
        ("per unit", "efficiency", "per_unit"),
        ("EUR/kW_e", "investment", "EUR/kW"),
        ("EUR/kW_e, 2020", "investment", "EUR_2020/kW"),
        ("EUR/MWhel", "VOM", "EUR/MWh_electric"),
        ("years", "lifetime", "year"),
    ],
)
def test_normalize_technology_unit(source: str, parameter: str, expected: str) -> None:
    assert normalize_technology_unit(source, parameter) == expected


def test_reconciliation_status_is_explicit() -> None:
    assert reconciliation_status(10.0, 10.0, 1.0) == ("exact_match", 0.0, 0.0)
    assert reconciliation_status(10.05, 10.0, 1.0)[0] == "within_tolerance"
    assert reconciliation_status(11.0, 10.0, 1.0)[0] == "material_difference"
    assert reconciliation_status(None, 10.0, 1.0)[0] == "missing_source_value"
