from pathlib import Path
from types import SimpleNamespace

import pytest

from atlas.transforms.structured_silver import discover_latest_source
from atlas.transforms.zenodo_technoeconomic import (
    ZENODO_PREFIX,
    _numeric_cell,
    load_config,
    parse_workbook,
)


def test_config_has_unique_explicit_technology_aliases() -> None:
    config = load_config(Path("configs/silver/zenodo_technoeconomic.yml"))

    aliases = [
        name.strip().casefold() for item in config.technology_mappings for name in item.zenodo_names
    ]
    assert len(aliases) == len(set(aliases))
    assert {item.canonical_name for item in config.technology_mappings} >= {
        "solar_utility",
        "onshore_wind",
        "electrolysis",
        "battery_storage",
    }


def test_numeric_cell_preserves_missing_markers_and_formula_errors() -> None:
    missing = _numeric_cell(
        SimpleNamespace(value="..", data_type="s", coordinate="D4"),
        SimpleNamespace(value=".."),
    )
    broken = _numeric_cell(
        SimpleNamespace(value="=AVERAGE(#REF!)", data_type="f", coordinate="L4"),
        SimpleNamespace(value="#REF!"),
    )
    cached = _numeric_cell(
        SimpleNamespace(value="=D5", data_type="f", coordinate="F5"),
        SimpleNamespace(value=1.25),
    )

    assert missing.value is None and missing.origin == "missing_marker"
    assert broken.value is None and broken.origin == "formula_error"
    assert cached.value == 1.25 and cached.origin == "formula_cached"


@pytest.mark.skipif(
    not Path("data/bronze/technology/zenodo_morocco_technoeconomic").exists(),
    reason="registered Zenodo Bronze workbook is not available",
)
def test_registered_workbook_contract_and_quality_flags() -> None:
    item = discover_latest_source(Path("data"), ZENODO_PREFIX)[0]

    tables = parse_workbook(item, "test")

    assert {name: len(rows) for name, rows in tables.items()} == {
        "morocco_electricity_capacity": 296,
        "morocco_power_plant_assumptions": 20,
        "morocco_renewable_capex": 432,
        "morocco_fossil_reserves": 3,
        "morocco_renewable_potential": 27,
        "morocco_hydrogen_export_demand": 72,
    }
    hydrogen = tables["morocco_hydrogen_export_demand"]
    assert sum(row["value_origin"] == "formula_error" for row in hydrogen) == 26
    assert all(
        row["demand_twh_per_year"] is None
        for row in hydrogen
        if row["value_origin"] == "formula_error"
    )
