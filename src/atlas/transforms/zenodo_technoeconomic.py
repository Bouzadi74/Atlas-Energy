"""Normalize the Morocco-specific Zenodo workbook and compare it with PyPSA data."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import logging
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator

from atlas.config import get_settings
from atlas.transforms.structured_silver import VerifiedBronzeObject, discover_latest_source
from atlas.transforms.weather_silver import build_spark_session

LOGGER = logging.getLogger(__name__)
TRANSFORM_VERSION = "zenodo-technoeconomic-silver-v1"
ZENODO_PREFIX = "technology/zenodo_morocco_technoeconomic"
PYPSA_PREFIX = "technology/pypsa_technology_data"

LINEAGE_SCHEMA = """
source_id STRING, source_file STRING, source_checksum_sha256 STRING,
bronze_ingest_date STRING, silver_version STRING
"""

TABLE_SCHEMAS = {
    "morocco_electricity_capacity": f"""
        technology STRING, grid_connection STRING, year INT,
        installed_capacity_gw DOUBLE, source_reference STRING,
        value_origin STRING, source_formula STRING,
        source_sheet STRING, source_row INT, source_cell STRING,
        {LINEAGE_SCHEMA}
    """,
    "morocco_power_plant_assumptions": f"""
        technology STRING, capital_cost_usd_per_kw_2020 DOUBLE,
        fixed_cost_usd_per_kw_year_2020 DOUBLE, operational_life_years DOUBLE,
        average_capacity_factor DOUBLE, source_reference STRING,
        source_sheet STRING, source_row INT, {LINEAGE_SCHEMA}
    """,
    "morocco_renewable_capex": f"""
        technology STRING, year INT, capital_cost_usd_2011_per_kw DOUBLE,
        source_reference STRING, value_origin STRING, source_formula STRING,
        source_sheet STRING, source_row INT, source_cell STRING,
        {LINEAGE_SCHEMA}
    """,
    "morocco_fossil_reserves": f"""
        fuel STRING, unit STRING, proven_reserves DOUBLE, source_reference STRING,
        source_sheet STRING, source_row INT, {LINEAGE_SCHEMA}
    """,
    "morocco_renewable_potential": f"""
        resource STRING, year INT, potential_value DOUBLE, unit STRING,
        source_reference STRING, value_origin STRING, source_formula STRING,
        source_sheet STRING, source_row INT, source_cell STRING,
        {LINEAGE_SCHEMA}
    """,
    "morocco_hydrogen_export_demand": f"""
        scenario_id STRING, year INT, demand_twh_per_year DOUBLE,
        source_reference STRING, value_origin STRING, source_formula STRING,
        source_sheet STRING, source_row INT, source_cell STRING,
        {LINEAGE_SCHEMA}
    """,
    "morocco_technoeconomic_data_dictionary": f"""
        table_name STRING, column_name STRING, data_type STRING, unit STRING,
        nullable BOOLEAN, description STRING, source_sheet STRING,
        {LINEAGE_SCHEMA}
    """,
    "morocco_technology_assumption_comparison": """
        source_dataset STRING, model_year INT, canonical_technology STRING,
        zenodo_technology STRING, pypsa_technology STRING,
        zenodo_parameter STRING, pypsa_parameter STRING,
        zenodo_value DOUBLE, zenodo_unit STRING,
        pypsa_value DOUBLE, pypsa_unit STRING,
        comparable BOOLEAN, comparison_status STRING,
        absolute_difference DOUBLE, percent_difference DOUBLE,
        mapping_note STRING, zenodo_source_file STRING, pypsa_source_file STRING,
        zenodo_checksum_sha256 STRING, pypsa_checksum_sha256 STRING,
        silver_version STRING
    """,
}


class TechnologyMapping(BaseModel):
    model_config = ConfigDict(extra="forbid")

    canonical_name: str = Field(min_length=1)
    pypsa_technology: str = Field(min_length=1)
    zenodo_names: list[str] = Field(min_length=1)

    @field_validator("zenodo_names")
    @classmethod
    def unique_names(cls, value: list[str]) -> list[str]:
        normalized = [_normalize_name(name) for name in value]
        if len(normalized) != len(set(normalized)):
            raise ValueError("zenodo_names contains duplicates")
        return value


class ZenodoSilverConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    version: Literal[1]
    comparison_model_years: list[int] = Field(min_length=1)
    technology_mappings: list[TechnologyMapping] = Field(min_length=1)

    @field_validator("comparison_model_years")
    @classmethod
    def unique_years(cls, value: list[int]) -> list[int]:
        if len(value) != len(set(value)):
            raise ValueError("comparison_model_years contains duplicates")
        return value


@dataclass(frozen=True)
class ZenodoBuildResult:
    silver_version: str
    table_rows: dict[str, int]
    formula_error_count: int
    output_root: Path
    dictionary_path: Path


@dataclass(frozen=True)
class NumericCell:
    value: float | None
    origin: str
    formula: str | None


def load_config(path: Path) -> ZenodoSilverConfig:
    try:
        content = yaml.safe_load(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        raise FileNotFoundError(f"Zenodo Silver configuration not found: {path}") from None
    except yaml.YAMLError as error:
        raise ValueError(f"Invalid YAML in Zenodo Silver configuration {path}: {error}") from error
    return ZenodoSilverConfig.model_validate(content)


def _normalize_name(value: str) -> str:
    return re.sub(r"\s+", " ", value.strip()).casefold()


def _lineage(item: VerifiedBronzeObject, silver_version: str) -> dict[str, Any]:
    return {
        "source_id": item.manifest.source_id,
        "source_file": item.raw_path.name,
        "source_checksum_sha256": item.manifest.sha256,
        "bronze_ingest_date": item.ingest_date.isoformat(),
        "silver_version": silver_version,
    }


def _numeric_cell(formula_cell: Any, cached_cell: Any) -> NumericCell:
    raw = formula_cell.value
    if formula_cell.data_type == "f":
        formula = str(raw)
        cached = cached_cell.value
        if "#REF!" in formula or (isinstance(cached, str) and cached.startswith("#")):
            return NumericCell(None, "formula_error", formula)
        if isinstance(cached, (int, float)) and not isinstance(cached, bool):
            return NumericCell(float(cached), "formula_cached", formula)
        return NumericCell(None, "formula_missing_cache", formula)
    if raw is None or raw == "":
        return NumericCell(None, "missing", None)
    if isinstance(raw, str) and raw.strip() in {".", "..", "�"}:
        return NumericCell(None, "missing_marker", None)
    if isinstance(raw, (int, float)) and not isinstance(raw, bool):
        return NumericCell(float(raw), "raw", None)
    raise ValueError(f"Expected a numeric workbook cell at {formula_cell.coordinate}: {raw!r}")


def _workbooks(path: Path) -> tuple[Any, Any]:
    try:
        from openpyxl import load_workbook
    except ImportError as error:
        raise RuntimeError(
            "Zenodo XLSX support requires: uv sync --extra dev --extra data"
        ) from error
    return (
        load_workbook(path, data_only=False, read_only=False),
        load_workbook(path, data_only=True, read_only=False),
    )


def parse_workbook(
    item: VerifiedBronzeObject, silver_version: str
) -> dict[str, list[dict[str, Any]]]:
    formulas, cached = _workbooks(item.raw_path)
    expected_sheets = {
        "Elec. cap. by sub-tech",
        "Power plants",
        "Cost of RE power plants",
        "FF reserves",
        "RE supply potential",
        "Green Hydrogen export demand",
    }
    if set(formulas.sheetnames) != expected_sheets:
        missing = sorted(expected_sheets - set(formulas.sheetnames))
        unexpected = sorted(set(formulas.sheetnames) - expected_sheets)
        raise ValueError(f"Workbook sheet mismatch; missing={missing}, unexpected={unexpected}")

    tables = {
        "morocco_electricity_capacity": _parse_capacity(formulas, cached, item, silver_version),
        "morocco_power_plant_assumptions": _parse_power_plants(formulas, item, silver_version),
        "morocco_renewable_capex": _parse_renewable_capex(formulas, cached, item, silver_version),
        "morocco_fossil_reserves": _parse_fossil_reserves(formulas, item, silver_version),
        "morocco_renewable_potential": _parse_renewable_potential(
            formulas, cached, item, silver_version
        ),
        "morocco_hydrogen_export_demand": _parse_hydrogen_demand(
            formulas, cached, item, silver_version
        ),
    }
    _require_unique(
        tables["morocco_electricity_capacity"],
        ("technology", "grid_connection", "year"),
        "electricity capacity",
    )
    _require_unique(tables["morocco_power_plant_assumptions"], ("technology",), "power plants")
    _require_unique(tables["morocco_renewable_capex"], ("technology", "year"), "renewable capex")
    _require_unique(tables["morocco_fossil_reserves"], ("fuel",), "fossil reserves")
    _require_unique(
        tables["morocco_renewable_potential"], ("resource", "year"), "renewable potential"
    )
    _require_unique(
        tables["morocco_hydrogen_export_demand"],
        ("scenario_id", "year"),
        "hydrogen demand",
    )
    return tables


def _parse_capacity(
    formulas: Any, cached: Any, item: VerifiedBronzeObject, version: str
) -> list[dict[str, Any]]:
    sheet = formulas["Elec. cap. by sub-tech"]
    cached_sheet = cached[sheet.title]
    years = [int(sheet.cell(3, column).value) for column in range(4, 12)]
    rows: list[dict[str, Any]] = []
    current_source: str | None = None
    for row_index in range(4, 41):
        current_source = str(sheet.cell(row_index, 1).value or current_source or "unknown")
        technology = str(sheet.cell(row_index, 2).value).strip()
        grid_connection = str(sheet.cell(row_index, 3).value).strip()
        for column, year in zip(range(4, 12), years, strict=True):
            cell = sheet.cell(row_index, column)
            numeric = _numeric_cell(cell, cached_sheet.cell(row_index, column))
            rows.append(
                {
                    "technology": technology,
                    "grid_connection": grid_connection,
                    "year": year,
                    "installed_capacity_gw": numeric.value,
                    "source_reference": current_source,
                    "value_origin": numeric.origin,
                    "source_formula": numeric.formula,
                    "source_sheet": sheet.title,
                    "source_row": row_index,
                    "source_cell": cell.coordinate,
                    **_lineage(item, version),
                }
            )
    return rows


def _parse_power_plants(
    formulas: Any, item: VerifiedBronzeObject, version: str
) -> list[dict[str, Any]]:
    sheet = formulas["Power plants"]
    rows: list[dict[str, Any]] = []
    for row_index in range(4, 24):
        rows.append(
            {
                "technology": str(sheet.cell(row_index, 2).value).strip(),
                "capital_cost_usd_per_kw_2020": float(sheet.cell(row_index, 3).value),
                "fixed_cost_usd_per_kw_year_2020": float(sheet.cell(row_index, 4).value),
                "operational_life_years": float(sheet.cell(row_index, 5).value),
                "average_capacity_factor": float(sheet.cell(row_index, 6).value),
                "source_reference": str(sheet.cell(row_index, 1).value),
                "source_sheet": sheet.title,
                "source_row": row_index,
                **_lineage(item, version),
            }
        )
    return rows


def _parse_renewable_capex(
    formulas: Any, cached: Any, item: VerifiedBronzeObject, version: str
) -> list[dict[str, Any]]:
    sheet = formulas["Cost of RE power plants"]
    cached_sheet = cached[sheet.title]
    years = [int(sheet.cell(4, column).value) for column in range(3, 39)]
    rows: list[dict[str, Any]] = []
    for row_index in range(5, 17):
        technology = str(sheet.cell(row_index, 2).value).strip()
        source = str(sheet.cell(row_index, 1).value)
        for column, year in zip(range(3, 39), years, strict=True):
            cell = sheet.cell(row_index, column)
            numeric = _numeric_cell(cell, cached_sheet.cell(row_index, column))
            rows.append(
                {
                    "technology": technology,
                    "year": year,
                    "capital_cost_usd_2011_per_kw": numeric.value,
                    "source_reference": source,
                    "value_origin": numeric.origin,
                    "source_formula": numeric.formula,
                    "source_sheet": sheet.title,
                    "source_row": row_index,
                    "source_cell": cell.coordinate,
                    **_lineage(item, version),
                }
            )
    return rows


def _parse_fossil_reserves(
    formulas: Any, item: VerifiedBronzeObject, version: str
) -> list[dict[str, Any]]:
    sheet = formulas["FF reserves"]
    rows: list[dict[str, Any]] = []
    current_source: str | None = None
    for row_index in range(4, 7):
        current_source = str(sheet.cell(row_index, 1).value or current_source or "unknown")
        rows.append(
            {
                "fuel": str(sheet.cell(row_index, 2).value).strip(),
                "unit": str(sheet.cell(row_index, 3).value).strip(),
                "proven_reserves": float(sheet.cell(row_index, 4).value),
                "source_reference": current_source,
                "source_sheet": sheet.title,
                "source_row": row_index,
                **_lineage(item, version),
            }
        )
    return rows


def _parse_renewable_potential(
    formulas: Any, cached: Any, item: VerifiedBronzeObject, version: str
) -> list[dict[str, Any]]:
    sheet = formulas["RE supply potential"]
    cached_sheet = cached[sheet.title]
    years = [int(sheet.cell(3, column).value) for column in range(4, 7)]
    rows: list[dict[str, Any]] = []
    for row_index in range(4, 13):
        resource = str(sheet.cell(row_index, 2).value).strip()
        unit = str(sheet.cell(row_index, 3).value).strip()
        source = str(sheet.cell(row_index, 1).value)
        for column, year in zip(range(4, 7), years, strict=True):
            cell = sheet.cell(row_index, column)
            numeric = _numeric_cell(cell, cached_sheet.cell(row_index, column))
            rows.append(
                {
                    "resource": resource,
                    "year": year,
                    "potential_value": numeric.value,
                    "unit": unit,
                    "source_reference": source,
                    "value_origin": numeric.origin,
                    "source_formula": numeric.formula,
                    "source_sheet": sheet.title,
                    "source_row": row_index,
                    "source_cell": cell.coordinate,
                    **_lineage(item, version),
                }
            )
    return rows


def _parse_hydrogen_demand(
    formulas: Any, cached: Any, item: VerifiedBronzeObject, version: str
) -> list[dict[str, Any]]:
    sheet = formulas["Green Hydrogen export demand"]
    cached_sheet = cached[sheet.title]
    years = [int(sheet.cell(3, column).value) for column in range(2, 38)]
    rows: list[dict[str, Any]] = []
    scenario_ids = {4: "vietnam_outlook_proxy", 5: "morocco_green_hydrogen_roadmap"}
    for row_index, scenario_id in scenario_ids.items():
        source = str(sheet.cell(row_index, 1).value)
        for column, year in zip(range(2, 38), years, strict=True):
            cell = sheet.cell(row_index, column)
            numeric = _numeric_cell(cell, cached_sheet.cell(row_index, column))
            rows.append(
                {
                    "scenario_id": scenario_id,
                    "year": year,
                    "demand_twh_per_year": numeric.value,
                    "source_reference": source,
                    "value_origin": numeric.origin,
                    "source_formula": numeric.formula,
                    "source_sheet": sheet.title,
                    "source_row": row_index,
                    "source_cell": cell.coordinate,
                    **_lineage(item, version),
                }
            )
    return rows


def _mapping_lookup(config: ZenodoSilverConfig) -> dict[str, TechnologyMapping]:
    lookup: dict[str, TechnologyMapping] = {}
    for mapping in config.technology_mappings:
        for name in mapping.zenodo_names:
            key = _normalize_name(name)
            if key in lookup:
                raise ValueError(f"Duplicate Zenodo technology mapping: {name}")
            lookup[key] = mapping
    return lookup


def _load_pypsa_values(
    objects: list[VerifiedBronzeObject], model_years: set[int]
) -> dict[tuple[int, str, str], tuple[float, str, VerifiedBronzeObject]]:
    values: dict[tuple[int, str, str], tuple[float, str, VerifiedBronzeObject]] = {}
    for item in objects:
        year = int(item.manifest.source_period)
        if year not in model_years:
            continue
        with item.raw_path.open(encoding="utf-8-sig", newline="") as handle:
            for row in csv.DictReader(handle):
                key = (year, row["technology"], row["parameter"])
                values[key] = (float(row["value"]), row["unit"], item)
    return values


def build_comparison(
    *,
    tables: dict[str, list[dict[str, Any]]],
    pypsa_objects: list[VerifiedBronzeObject],
    config: ZenodoSilverConfig,
    silver_version: str,
) -> list[dict[str, Any]]:
    lookup = _mapping_lookup(config)
    pypsa = _load_pypsa_values(pypsa_objects, set(config.comparison_model_years))
    candidates: list[dict[str, Any]] = []

    for row in tables["morocco_renewable_capex"]:
        if row["year"] not in config.comparison_model_years:
            continue
        candidates.append(
            {
                "source_dataset": "morocco_renewable_capex",
                "model_year": row["year"],
                "technology": row["technology"],
                "parameter": "capital_cost",
                "pypsa_parameter": "investment",
                "value": row["capital_cost_usd_2011_per_kw"],
                "unit": "USD_2011/kW",
                "source_file": row["source_file"],
                "checksum": row["source_checksum_sha256"],
            }
        )

    power_parameters = (
        ("capital_cost", "investment", "capital_cost_usd_per_kw_2020", "USD_2020/kW"),
        ("fixed_cost", "FOM", "fixed_cost_usd_per_kw_year_2020", "USD_2020/kW/year"),
        ("operational_life", "lifetime", "operational_life_years", "years"),
        ("capacity_factor", "capacity factor", "average_capacity_factor", "per unit"),
    )
    for row in tables["morocco_power_plant_assumptions"]:
        for parameter, pypsa_parameter, column, unit in power_parameters:
            candidates.append(
                {
                    "source_dataset": "morocco_power_plant_assumptions",
                    "model_year": 2020,
                    "technology": row["technology"],
                    "parameter": parameter,
                    "pypsa_parameter": pypsa_parameter,
                    "value": row[column],
                    "unit": unit,
                    "source_file": row["source_file"],
                    "checksum": row["source_checksum_sha256"],
                }
            )

    rows: list[dict[str, Any]] = []
    for candidate in candidates:
        mapping = lookup.get(_normalize_name(candidate["technology"]))
        if mapping is None:
            rows.append(
                _comparison_row(candidate, None, None, None, "mapping_unavailable", silver_version)
            )
            continue
        pypsa_value = pypsa.get(
            (candidate["model_year"], mapping.pypsa_technology, candidate["pypsa_parameter"])
        )
        if pypsa_value is None:
            rows.append(
                _comparison_row(
                    candidate,
                    mapping,
                    None,
                    None,
                    "pypsa_parameter_missing",
                    silver_version,
                )
            )
            continue
        value, unit, pypsa_item = pypsa_value
        if candidate["value"] is None:
            status = "source_value_missing"
        elif candidate["unit"] != unit:
            status = "matched_basis_mismatch"
        else:
            status = "matched_comparable"
        rows.append(
            _comparison_row(candidate, mapping, value, unit, status, silver_version, pypsa_item)
        )
    _require_unique(
        rows,
        (
            "source_dataset",
            "model_year",
            "zenodo_technology",
            "zenodo_parameter",
        ),
        "technology comparison",
    )
    return rows


def _comparison_row(
    candidate: dict[str, Any],
    mapping: TechnologyMapping | None,
    pypsa_value: float | None,
    pypsa_unit: str | None,
    status: str,
    version: str,
    pypsa_item: VerifiedBronzeObject | None = None,
) -> dict[str, Any]:
    comparable = status == "matched_comparable"
    source_value = candidate["value"]
    difference = source_value - pypsa_value if comparable else None
    percent = (
        difference / pypsa_value * 100 if comparable and pypsa_value not in (None, 0) else None
    )
    notes = {
        "matched_comparable": "Same unit; raw numeric difference is calculated.",
        "matched_basis_mismatch": "Technology matched, but currency or parameter basis differs.",
        "pypsa_parameter_missing": "Technology matched; PyPSA has no matching parameter.",
        "mapping_unavailable": "No explicit technology mapping was approved.",
        "source_value_missing": "Zenodo source value is missing.",
    }
    return {
        "source_dataset": candidate["source_dataset"],
        "model_year": candidate["model_year"],
        "canonical_technology": mapping.canonical_name if mapping else None,
        "zenodo_technology": candidate["technology"],
        "pypsa_technology": mapping.pypsa_technology if mapping else None,
        "zenodo_parameter": candidate["parameter"],
        "pypsa_parameter": candidate["pypsa_parameter"],
        "zenodo_value": source_value,
        "zenodo_unit": candidate["unit"],
        "pypsa_value": pypsa_value,
        "pypsa_unit": pypsa_unit,
        "comparable": comparable,
        "comparison_status": status,
        "absolute_difference": difference,
        "percent_difference": percent,
        "mapping_note": notes[status],
        "zenodo_source_file": candidate["source_file"],
        "pypsa_source_file": pypsa_item.raw_path.name if pypsa_item else None,
        "zenodo_checksum_sha256": candidate["checksum"],
        "pypsa_checksum_sha256": pypsa_item.manifest.sha256 if pypsa_item else None,
        "silver_version": version,
    }


DATA_DICTIONARY: dict[str, list[tuple[str, str, str, bool, str, str]]] = {
    "morocco_electricity_capacity": [
        ("technology", "string", "", False, "Source technology label.", "Elec. cap. by sub-tech"),
        (
            "grid_connection",
            "string",
            "",
            False,
            "On-grid or off-grid classification.",
            "Elec. cap. by sub-tech",
        ),
        ("year", "integer", "year", False, "Observation year.", "Elec. cap. by sub-tech"),
        (
            "installed_capacity_gw",
            "double",
            "GW",
            True,
            "Installed electricity capacity; source missing markers become null.",
            "Elec. cap. by sub-tech",
        ),
    ],
    "morocco_power_plant_assumptions": [
        ("technology", "string", "", False, "Generation or conversion technology.", "Power plants"),
        (
            "capital_cost_usd_per_kw_2020",
            "double",
            "USD_2020/kW",
            False,
            "Capital cost stated in the workbook.",
            "Power plants",
        ),
        (
            "fixed_cost_usd_per_kw_year_2020",
            "double",
            "USD_2020/kW/year",
            False,
            "Annual fixed cost stated in the workbook.",
            "Power plants",
        ),
        (
            "operational_life_years",
            "double",
            "years",
            False,
            "Technical operating life.",
            "Power plants",
        ),
        (
            "average_capacity_factor",
            "double",
            "per unit",
            False,
            "Average capacity factor.",
            "Power plants",
        ),
    ],
    "morocco_renewable_capex": [
        (
            "technology",
            "string",
            "",
            False,
            "Renewable or storage technology.",
            "Cost of RE power plants",
        ),
        ("year", "integer", "year", False, "Assumption year.", "Cost of RE power plants"),
        (
            "capital_cost_usd_2011_per_kw",
            "double",
            "USD_2011/kW",
            True,
            "Annual capital-cost trajectory; blank source cells remain null.",
            "Cost of RE power plants",
        ),
    ],
    "morocco_fossil_reserves": [
        ("fuel", "string", "", False, "Fossil fuel type.", "FF reserves"),
        ("unit", "string", "source-defined", False, "Reserve measurement unit.", "FF reserves"),
        (
            "proven_reserves",
            "double",
            "source-defined",
            False,
            "Published proven reserve quantity.",
            "FF reserves",
        ),
    ],
    "morocco_renewable_potential": [
        (
            "resource",
            "string",
            "",
            False,
            "Renewable resource or technology.",
            "RE supply potential",
        ),
        ("year", "integer", "year", False, "Potential-assumption year.", "RE supply potential"),
        (
            "potential_value",
            "double",
            "source-defined",
            True,
            "Available potential; cached formula values are retained and labelled.",
            "RE supply potential",
        ),
        (
            "unit",
            "string",
            "source-defined",
            False,
            "Workbook unit (GW in the supplied version).",
            "RE supply potential",
        ),
    ],
    "morocco_hydrogen_export_demand": [
        (
            "scenario_id",
            "string",
            "",
            False,
            "Stable identifier for the workbook scenario row.",
            "Green Hydrogen export demand",
        ),
        ("year", "integer", "year", False, "Demand year.", "Green Hydrogen export demand"),
        (
            "demand_twh_per_year",
            "double",
            "TWh/year",
            True,
            "Annual export-demand assumption; broken formula results remain null.",
            "Green Hydrogen export demand",
        ),
    ],
    "morocco_technology_assumption_comparison": [
        ("source_dataset", "string", "", False, "Zenodo Silver table being compared.", "Derived"),
        ("model_year", "integer", "year", False, "Comparison model year.", "Derived"),
        (
            "canonical_technology",
            "string",
            "",
            True,
            "Reviewed cross-source technology mapping.",
            "Derived",
        ),
        ("zenodo_technology", "string", "", False, "Original Zenodo technology label.", "Derived"),
        ("pypsa_technology", "string", "", True, "Mapped PyPSA technology label.", "Derived"),
        ("zenodo_parameter", "string", "", False, "Zenodo assumption being compared.", "Derived"),
        ("pypsa_parameter", "string", "", False, "Candidate PyPSA parameter.", "Derived"),
        (
            "zenodo_value",
            "double",
            "zenodo_unit",
            True,
            "Value from the Zenodo workbook.",
            "Derived",
        ),
        ("zenodo_unit", "string", "", False, "Zenodo value unit and currency basis.", "Derived"),
        (
            "pypsa_value",
            "double",
            "pypsa_unit",
            True,
            "Value from PyPSA technology-data.",
            "Derived",
        ),
        ("pypsa_unit", "string", "", True, "PyPSA value unit and currency basis.", "Derived"),
        (
            "comparable",
            "boolean",
            "",
            False,
            "True only when parameter and unit bases match.",
            "Derived",
        ),
        (
            "comparison_status",
            "string",
            "",
            False,
            "Whether values are comparable, mismatched, missing, or unmapped.",
            "Derived",
        ),
        (
            "absolute_difference",
            "double",
            "matched unit",
            True,
            "Zenodo minus PyPSA, only when units match exactly.",
            "Derived",
        ),
        (
            "percent_difference",
            "double",
            "%",
            True,
            "Difference divided by PyPSA value, only when units match exactly.",
            "Derived",
        ),
        ("mapping_note", "string", "", False, "Explanation of the comparison status.", "Derived"),
        ("zenodo_source_file", "string", "", False, "Zenodo source filename.", "Derived"),
        ("pypsa_source_file", "string", "", True, "PyPSA source filename when matched.", "Derived"),
        ("zenodo_checksum_sha256", "string", "", False, "Zenodo workbook checksum.", "Derived"),
        (
            "pypsa_checksum_sha256",
            "string",
            "",
            True,
            "PyPSA file checksum when matched.",
            "Derived",
        ),
        (
            "silver_version",
            "string",
            "",
            False,
            "Deterministic Silver build identifier.",
            "Derived",
        ),
        (
            "transformed_at_utc",
            "timestamp",
            "UTC",
            False,
            "Silver transformation timestamp.",
            "Derived",
        ),
    ],
}


def build_data_dictionary(item: VerifiedBronzeObject, silver_version: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    source_common = [
        ("source_reference", "string", "", False, "Citation copied from the workbook."),
        ("source_sheet", "string", "", False, "Original worksheet name."),
        ("source_row", "integer", "row", False, "Original worksheet row number."),
        ("source_id", "string", "", False, "Bronze source identifier."),
        ("source_file", "string", "", False, "Original Bronze filename."),
        ("source_checksum_sha256", "string", "", False, "SHA-256 of the Bronze workbook."),
        ("bronze_ingest_date", "string", "date", False, "Bronze ingestion date."),
        ("silver_version", "string", "", False, "Deterministic Silver build identifier."),
        ("transformed_at_utc", "timestamp", "UTC", False, "Silver transformation timestamp."),
    ]
    cell_common = [
        (
            "value_origin",
            "string",
            "",
            False,
            "raw, formula_cached, formula_error, missing, or missing_marker.",
        ),
        (
            "source_formula",
            "string",
            "",
            True,
            "Original Excel formula when the source cell is calculated.",
        ),
        ("source_cell", "string", "", False, "Original worksheet cell address."),
    ]
    cell_tables = {
        "morocco_electricity_capacity",
        "morocco_renewable_capex",
        "morocco_renewable_potential",
        "morocco_hydrogen_export_demand",
    }
    for table_name, fields in DATA_DICTIONARY.items():
        source_sheet = fields[0][5]
        for column, data_type, unit, nullable, description, field_sheet in fields:
            rows.append(
                {
                    "table_name": table_name,
                    "column_name": column,
                    "data_type": data_type,
                    "unit": unit,
                    "nullable": nullable,
                    "description": description,
                    "source_sheet": field_sheet,
                    **_lineage(item, silver_version),
                }
            )
        if table_name != "morocco_technology_assumption_comparison":
            common = source_common + (cell_common if table_name in cell_tables else [])
            for column, data_type, unit, nullable, description in common:
                rows.append(
                    {
                        "table_name": table_name,
                        "column_name": column,
                        "data_type": data_type,
                        "unit": unit,
                        "nullable": nullable,
                        "description": description,
                        "source_sheet": source_sheet,
                        **_lineage(item, silver_version),
                    }
                )
    return rows


def _require_unique(rows: list[dict[str, Any]], keys: tuple[str, ...], label: str) -> None:
    identities = [tuple(row.get(key) for key in keys) for row in rows]
    if len(identities) != len(set(identities)):
        raise ValueError(f"{label} rows contain duplicate keys: {keys}")


def _silver_version(
    zenodo_item: VerifiedBronzeObject,
    pypsa_objects: list[VerifiedBronzeObject],
    config: ZenodoSilverConfig,
) -> str:
    payload = {
        "transform_version": TRANSFORM_VERSION,
        "config": config.model_dump(mode="json"),
        "source_checksums": sorted(
            [zenodo_item.manifest.sha256] + [item.manifest.sha256 for item in pypsa_objects]
        ),
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()[:16]


def _write_delta(spark: Any, rows: list[dict[str, Any]], path: Path, schema: str) -> int:
    if not rows:
        raise ValueError(f"Refusing to write an empty Silver table: {path.name}")
    from pyspark.sql import functions as F

    frame = spark.createDataFrame(rows, schema=schema).withColumn(
        "transformed_at_utc", F.current_timestamp()
    )
    path.mkdir(parents=True, exist_ok=True)
    frame.write.format("delta").mode("overwrite").save(str(path.resolve()))
    return frame.count()


def _write_dictionary_markdown(rows: list[dict[str, Any]], path: Path, silver_version: str) -> None:
    lines = [
        "# Morocco techno-economic Silver data dictionary",
        "",
        f"Silver version: `{silver_version}`",
        "",
        "The original Zenodo workbook is preserved unchanged. Nulls distinguish missing source "
        "values from zeros. Formula-derived values retain the source formula and an origin flag.",
        "",
    ]
    current: str | None = None
    for row in rows:
        if row["table_name"] != current:
            current = row["table_name"]
            lines.extend(
                [
                    f"## `{current}`",
                    "",
                    "| Column | Type | Unit | Nullable | Description | Source sheet |",
                    "|---|---|---|---|---|---|",
                ]
            )
        description = str(row["description"]).replace("|", "\\|")
        lines.append(
            f"| `{row['column_name']}` | {row['data_type']} | {row['unit'] or '-'} | "
            f"{'yes' if row['nullable'] else 'no'} | {description} | {row['source_sheet']} |"
        )
    lines.extend(
        [
            "",
            "## Known source-quality issue",
            "",
            "The `vietnam_outlook_proxy` hydrogen-demand row contains broken `#REF!` formulas "
            "from 2025 onward in the published workbook. Silver stores those values as null with "
            "`value_origin = formula_error`. No values are inferred.",
            "",
        ]
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")


def build_zenodo_silver(
    *, spark: Any, data_root: Path, config: ZenodoSilverConfig
) -> ZenodoBuildResult:
    zenodo_objects = discover_latest_source(data_root, ZENODO_PREFIX)
    if len(zenodo_objects) != 1:
        raise ValueError("Expected exactly one Zenodo workbook in the latest Bronze partition")
    zenodo_item = zenodo_objects[0]
    pypsa_objects = discover_latest_source(data_root, PYPSA_PREFIX)
    silver_version = _silver_version(zenodo_item, pypsa_objects, config)
    tables = parse_workbook(zenodo_item, silver_version)
    tables["morocco_technology_assumption_comparison"] = build_comparison(
        tables=tables,
        pypsa_objects=pypsa_objects,
        config=config,
        silver_version=silver_version,
    )
    dictionary_rows = build_data_dictionary(zenodo_item, silver_version)
    tables["morocco_technoeconomic_data_dictionary"] = dictionary_rows

    formula_error_count = sum(
        row.get("value_origin") == "formula_error" for rows in tables.values() for row in rows
    )
    output_root = data_root / "silver"
    table_rows = {
        name: _write_delta(spark, rows, output_root / name, TABLE_SCHEMAS[name])
        for name, rows in tables.items()
    }
    dictionary_path = (
        output_root / "manifests" / f"zenodo_technoeconomic_data_dictionary_{silver_version}.md"
    )
    _write_dictionary_markdown(dictionary_rows, dictionary_path, silver_version)
    manifest = {
        "silver_version": silver_version,
        "transform_version": TRANSFORM_VERSION,
        "table_rows": table_rows,
        "formula_error_count": formula_error_count,
        "source_checksums": {
            "zenodo_morocco_technoeconomic": zenodo_item.manifest.sha256,
            "technology_data": [item.manifest.sha256 for item in pypsa_objects],
        },
        "config": config.model_dump(mode="json"),
        "data_dictionary": dictionary_path.name,
    }
    manifest_path = output_root / "manifests" / f"zenodo_technoeconomic_{silver_version}.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    return ZenodoBuildResult(
        silver_version, table_rows, formula_error_count, output_root, dictionary_path
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build Morocco-specific Zenodo techno-economic Silver tables"
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("configs/silver/zenodo_technoeconomic.yml"),
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    settings = get_settings()
    logging.basicConfig(
        level=settings.log_level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    config = load_config(args.config)
    spark = build_spark_session("atlas-zenodo-technoeconomic-silver")
    try:
        result = build_zenodo_silver(spark=spark, data_root=settings.data_root, config=config)
        LOGGER.info(
            "Zenodo Silver complete: version=%s tables=%s formula_errors=%d dictionary=%s",
            result.silver_version,
            result.table_rows,
            result.formula_error_count,
            result.dictionary_path,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
