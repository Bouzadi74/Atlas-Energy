from pathlib import Path

import pytest

from atlas.features.renewable_profiles import (
    feature_version,
    hub_height_wind_speed,
    load_renewable_profile_config,
    solar_capacity_factor,
    wind_capacity_factor,
)


@pytest.fixture
def config():  # type: ignore[no-untyped-def]
    return load_renewable_profile_config(Path("configs/features/renewable_profiles.yml"))


def test_solar_capacity_factor_is_bounded_and_zero_at_night(config) -> None:  # type: ignore[no-untyped-def]
    assert solar_capacity_factor(0, 20, config.solar) == 0
    assert 0 < solar_capacity_factor(1000, 25, config.solar) < 1
    assert solar_capacity_factor(5000, -10, config.solar) == 1


def test_wind_curve_has_expected_operating_regions(config) -> None:  # type: ignore[no-untyped-def]
    assert wind_capacity_factor(2.9, config.wind) == 0
    assert 0 < wind_capacity_factor(8, config.wind) < 1
    assert wind_capacity_factor(12, config.wind) == 1
    assert wind_capacity_factor(25, config.wind) == 0
    assert hub_height_wind_speed(7, config.wind) > 7


def test_feature_version_tracks_inputs(config) -> None:  # type: ignore[no-untyped-def]
    first = feature_version(config, weather_year=2024, silver_delta_version=0)
    repeated = feature_version(config, weather_year=2024, silver_delta_version=0)
    changed = feature_version(config, weather_year=2024, silver_delta_version=1)

    assert len(first) == 16
    assert first == repeated
    assert changed != first
