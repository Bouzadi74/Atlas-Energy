"""Complete the optimizer-facing Silver foundation with explicit quality contracts."""

from __future__ import annotations

import argparse
import calendar
import hashlib
import json
import math
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator

from atlas.config import get_settings
from atlas.ingestion.nasa_power import load_location_config
from atlas.transforms.weather_silver import build_spark_session

TRANSFORM_VERSION = "silver-foundation-completion-v3"


class WeatherConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    years: list[int]
    canonical_source: str
    comparison_source: str


class DemandConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    year: int
    annual_energy_metric_id: str
    peak_power_metric_id: str
    peak_day_energy_metric_id: str
    calibration_tolerance_mwh: float = Field(gt=0)
    provenance_classification: Literal["SYNTHETIC_CALIBRATED"]
    method_version: str
    method_description: str


class StatisticsConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    relative_tolerance_percent: float = Field(ge=0)


class TechnologyChoice(BaseModel):
    model_config = ConfigDict(extra="forbid")

    canonical_name: str
    pypsa_technology: str
    component_type: str
    carrier: str


class TechnologyConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source_policy: str
    model_years: list[int]
    parameters: list[str]
    technologies: list[TechnologyChoice]

    @model_validator(mode="after")
    def unique_choices(self) -> TechnologyConfig:
        canonical = [item.canonical_name for item in self.technologies]
        upstream = [item.pypsa_technology for item in self.technologies]
        if len(canonical) != len(set(canonical)):
            raise ValueError("canonical technology names must be unique")
        if len(upstream) != len(set(upstream)):
            raise ValueError("PyPSA technology mappings must be unique")
        return self


class OptimizerInputConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    weather_year: int
    renewable_aggregation: Literal["equal_weight_across_representative_locations"]


class SilverCompletionConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    version: str
    weather: WeatherConfig
    demand: DemandConfig
    statistics: StatisticsConfig
    technology: TechnologyConfig
    optimizer_inputs: OptimizerInputConfig


@dataclass(frozen=True)
class OpenMeteoInput:
    raw_path: Path
    metadata_path: Path
    location_id: str
    zone: str
    latitude: float
    longitude: float
    year: int
    ingest_date: str
    checksum: str


@dataclass(frozen=True)
class CompletionResult:
    silver_version: str
    table_rows: dict[str, int]
    output_root: Path


def load_config(path: Path) -> SilverCompletionConfig:
    try:
        content = yaml.safe_load(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        raise FileNotFoundError(f"Silver completion config not found: {path}") from None
    except yaml.YAMLError as error:
        raise ValueError(f"Invalid Silver completion YAML {path}: {error}") from error
    return SilverCompletionConfig.model_validate(content)


def normalize_technology_unit(unit: str, parameter: str) -> str:
    normalized = unit.strip()
    aliases = {
        "p.u.": "per_unit",
        "per unit": "per_unit",
        "years": "year",
        "EUR/kW_e": "EUR/kW",
        "EUR/kWel": "EUR/kW",
        "EUR/kW_e, 2020": "EUR_2020/kW",
        "EUR/MWh_e": "EUR/MWh_electric",
        "EUR/MWhel": "EUR/MWh_electric",
    }
    result = aliases.get(normalized, normalized)
    if parameter == "investment" and result == "EUR/kW":
        return "EUR/kW"
    return result


def reconciliation_status(
    primary: float | None,
    secondary: float | None,
    tolerance_percent: float,
) -> tuple[str, float | None, float | None]:
    if primary is None or secondary is None:
        return "missing_source_value", None, None
    difference = primary - secondary
    percent = None if secondary == 0 else difference / secondary * 100.0
    if difference == 0:
        return "exact_match", 0.0, 0.0
    if percent is not None and abs(percent) <= tolerance_percent + 1e-9:
        return "within_tolerance", difference, percent
    return "material_difference", difference, percent


def raw_demand_shape(timestamp: datetime, temperature_c: float) -> float:
    hour = timestamp.hour
    weekday = timestamp.weekday()
    day_of_year = timestamp.timetuple().tm_yday
    morning = math.exp(-((hour - 9.0) / 3.2) ** 2)
    evening = math.exp(-((hour - 20.0) / 3.4) ** 2)
    overnight = math.exp(-((hour - 3.0) / 3.8) ** 2)
    weekend = 0.94 if weekday >= 5 else 1.0
    summer = 1.0 + 0.07 * math.cos(2.0 * math.pi * (day_of_year - 205) / 366.0)
    cooling = max(temperature_c - 22.0, 0.0) * 0.012
    heating = max(12.0 - temperature_c, 0.0) * 0.006
    return (0.72 + 0.14 * morning + 0.25 * evening - 0.05 * overnight) * weekend * (
        summer + cooling + heating
    )


def _affine_calibration(
    shapes: list[float], *, target_sum: float, target_max: float
) -> list[float]:
    low = min(shapes)
    high = max(shapes)
    if high <= low:
        raise ValueError("Demand shape has no variation")
    normalized = [(value - low) / (high - low) for value in shapes]
    normalized_sum = sum(normalized)
    denominator = len(normalized) - normalized_sum
    if denominator <= 0:
        raise ValueError("Demand calibration is singular")
    floor = (target_sum - target_max * normalized_sum) / denominator
    if floor <= 0 or floor >= target_max:
        raise ValueError("Demand calibration would create an invalid load floor")
    return [floor + (target_max - floor) * value for value in normalized]


def calibrate_demand_profile(
    timestamps: list[datetime],
    temperatures_c: list[float],
    *,
    annual_energy_mwh: float,
    peak_power_mw: float,
    peak_day_energy_mwh: float,
    peak_date: date,
) -> list[dict[str, Any]]:
    if len(timestamps) != len(temperatures_c) or not timestamps:
        raise ValueError("Demand timestamps and temperatures must be non-empty and aligned")
    if timestamps != sorted(timestamps):
        raise ValueError("Demand timestamps must be sorted")
    if len(timestamps) != len(set(timestamps)):
        raise ValueError(
            f"Demand timestamps must be unique: rows={len(timestamps)} "
            f"unique={len(set(timestamps))}"
        )
    expected_hours = (366 if calendar.isleap(peak_date.year) else 365) * 24
    if len(timestamps) != expected_hours:
        raise ValueError(f"Demand profile has {len(timestamps)} hours; expected {expected_hours}")

    shapes = [
        raw_demand_shape(timestamp, temperature)
        for timestamp, temperature in zip(timestamps, temperatures_c, strict=True)
    ]
    peak_indices = [i for i, timestamp in enumerate(timestamps) if timestamp.date() == peak_date]
    if len(peak_indices) != 24:
        raise ValueError("Peak calibration date does not contain exactly 24 hours")
    other_indices = [i for i in range(len(timestamps)) if i not in set(peak_indices)]

    peak_loads = _affine_calibration(
        [shapes[i] for i in peak_indices],
        target_sum=peak_day_energy_mwh,
        target_max=peak_power_mw,
    )
    remaining_energy = annual_energy_mwh - peak_day_energy_mwh
    other_shape_sum = sum(shapes[i] for i in other_indices)
    other_scale = remaining_energy / other_shape_sum
    other_loads = [shapes[i] * other_scale for i in other_indices]
    if max(other_loads) >= peak_power_mw:
        raise ValueError("Non-peak demand exceeds the calibrated annual peak")

    loads = [0.0] * len(timestamps)
    for index, load in zip(peak_indices, peak_loads, strict=True):
        loads[index] = load
    for index, load in zip(other_indices, other_loads, strict=True):
        loads[index] = load
    residual = annual_energy_mwh - sum(loads)
    loads[other_indices[-1]] += residual

    return [
        {
            "timestamp_utc": timestamp,
            "demand_mw": load,
            "hourly_energy_mwh": load,
            "national_temperature_c": temperature,
            "raw_shape": shape,
            "is_calibration_peak_day": timestamp.date() == peak_date,
        }
        for timestamp, temperature, shape, load in zip(
            timestamps, temperatures_c, shapes, loads, strict=True
        )
    ]


def _partition_date(path: Path) -> str:
    prefix = "ingest_date="
    if not path.name.startswith(prefix):
        raise ValueError(f"Invalid Bronze partition: {path}")
    return date.fromisoformat(path.name.removeprefix(prefix)).isoformat()


def discover_open_meteo_inputs(
    *, data_root: Path, locations_path: Path, years: list[int]
) -> list[OpenMeteoInput]:
    locations = load_location_config(locations_path).locations
    root = data_root / "bronze" / "weather" / "open_meteo"
    results: list[OpenMeteoInput] = []
    for year in years:
        candidates = sorted(root.glob("ingest_date=*"), reverse=True)
        partition = next(
            (
                candidate
                for candidate in candidates
                if all(
                    (candidate / f"{location.id}_{year}_hourly.json").is_file()
                    and (candidate / f"{location.id}_{year}_hourly.json.metadata.json").is_file()
                    for location in locations
                )
            ),
            None,
        )
        if partition is None:
            raise FileNotFoundError(f"No complete Open-Meteo Bronze partition for {year}")
        ingest_date = _partition_date(partition)
        for location in locations:
            raw_path = partition / f"{location.id}_{year}_hourly.json"
            metadata_path = raw_path.with_name(f"{raw_path.name}.metadata.json")
            metadata = json.loads(metadata_path.read_text(encoding="utf-8-sig"))
            checksum = hashlib.sha256(raw_path.read_bytes()).hexdigest()
            if checksum != metadata.get("sha256"):
                raise ValueError(f"Open-Meteo checksum mismatch: {raw_path}")
            if metadata.get("source_id") != "open_meteo":
                raise ValueError(f"Unexpected Open-Meteo source ID: {metadata_path}")
            results.append(
                OpenMeteoInput(
                    raw_path=raw_path,
                    metadata_path=metadata_path,
                    location_id=location.id,
                    zone=location.zone,
                    latitude=location.latitude,
                    longitude=location.longitude,
                    year=year,
                    ingest_date=ingest_date,
                    checksum=checksum,
                )
            )
    return results


def build_open_meteo_silver(spark: Any, inputs: list[OpenMeteoInput], output: Path) -> int:
    from pyspark.sql import functions as F
    from pyspark.sql.types import (
        ArrayType,
        DoubleType,
        IntegerType,
        StringType,
        StructField,
        StructType,
    )

    hourly = StructType(
        [
            StructField("time", ArrayType(StringType(), False), False),
            StructField("temperature_2m", ArrayType(DoubleType(), True), False),
            StructField("shortwave_radiation", ArrayType(DoubleType(), True), False),
            StructField("wind_speed_10m", ArrayType(DoubleType(), True), False),
            StructField("wind_speed_100m", ArrayType(DoubleType(), True), False),
        ]
    )
    schema = StructType(
        [
            StructField("utc_offset_seconds", IntegerType(), False),
            StructField("hourly", hourly, False),
        ]
    )
    raw = (
        spark.read.schema(schema)
        .option("multiLine", True)
        .json([str(item.raw_path.resolve()) for item in inputs])
        .withColumn("source_file", F.regexp_extract(F.input_file_name(), r"([^/]+\.json)$", 1))
    )
    lineage = spark.createDataFrame(
        [
            (
                item.raw_path.name,
                item.location_id,
                item.zone,
                item.latitude,
                item.longitude,
                item.year,
                item.ingest_date,
                item.checksum,
            )
            for item in inputs
        ],
        "source_file string, location_id string, zone string, latitude double, longitude double, "
        "weather_year int, bronze_ingest_date string, source_checksum_sha256 string",
    )
    joined = raw.join(F.broadcast(lineage), "source_file")
    exploded = joined.select(
        "source_file",
        "utc_offset_seconds",
        "hourly",
        "location_id",
        "zone",
        "latitude",
        "longitude",
        "weather_year",
        "bronze_ingest_date",
        "source_checksum_sha256",
        F.posexplode("hourly.time").alias("position", "timestamp_text"),
    )
    frame = exploded.select(
        F.to_timestamp("timestamp_text", "yyyy-MM-dd'T'HH:mm").alias("timestamp_utc"),
        "location_id",
        "zone",
        "latitude",
        "longitude",
        F.element_at("hourly.shortwave_radiation", F.col("position") + 1).alias(
            "solar_irradiance_wh_m2"
        ),
        F.element_at("hourly.temperature_2m", F.col("position") + 1).alias("temperature_c"),
        (
            F.element_at("hourly.wind_speed_10m", F.col("position") + 1) / F.lit(3.6)
        ).alias("wind_speed_10m_m_s"),
        (
            F.element_at("hourly.wind_speed_100m", F.col("position") + 1) / F.lit(3.6)
        ).alias("wind_speed_100m_m_s"),
        (
            F.element_at("hourly.temperature_2m", F.col("position") + 1).isNull()
            | F.element_at("hourly.shortwave_radiation", F.col("position") + 1).isNull()
            | F.element_at("hourly.wind_speed_10m", F.col("position") + 1).isNull()
            | F.element_at("hourly.wind_speed_100m", F.col("position") + 1).isNull()
        ).alias("quality_has_missing"),
        F.lit("Open-Meteo ERA5").alias("source"),
        "source_file",
        "source_checksum_sha256",
        F.lit("REANALYSIS").alias("provenance_classification"),
        F.to_date("bronze_ingest_date").alias("bronze_ingest_date"),
        "weather_year",
        F.current_timestamp().alias("transformed_at_utc"),
    )
    expected = sum((366 if calendar.isleap(item.year) else 365) * 24 for item in inputs)
    count = frame.count()
    if count != expected:
        raise ValueError(f"Open-Meteo Silver has {count} rows; expected {expected}")
    if frame.where(F.col("timestamp_utc").isNull()).limit(1).count():
        raise ValueError("Open-Meteo Silver contains invalid timestamps")
    if (
        frame.groupBy("source", "location_id", "timestamp_utc")
        .count()
        .where(F.col("count") > 1)
        .limit(1)
        .count()
    ):
        raise ValueError("Open-Meteo Silver contains duplicate source-location-hour keys")
    output.parent.mkdir(parents=True, exist_ok=True)
    frame.write.format("delta").mode("overwrite").partitionBy("weather_year").save(
        str(output.resolve())
    )
    return count


def build_weather_comparison(spark: Any, data_root: Path, output: Path) -> int:
    from pyspark.sql import functions as F

    nasa = spark.read.format("delta").load(str((data_root / "silver" / "weather").resolve()))
    open_meteo = spark.read.format("delta").load(
        str((data_root / "silver" / "weather_open_meteo").resolve())
    )
    joined = nasa.alias("n").join(
        open_meteo.alias("o"), ["timestamp_utc", "location_id", "weather_year"], "inner"
    )
    frame = joined.select(
        "timestamp_utc",
        "location_id",
        "weather_year",
        F.col("n.temperature_c").alias("nasa_temperature_c"),
        F.col("o.temperature_c").alias("open_meteo_temperature_c"),
        (F.col("n.temperature_c") - F.col("o.temperature_c")).alias(
            "temperature_difference_c"
        ),
        F.col("n.solar_irradiance_wh_m2").alias("nasa_solar_irradiance_wh_m2"),
        F.col("o.solar_irradiance_wh_m2").alias("open_meteo_solar_irradiance_wh_m2"),
        (
            F.col("n.solar_irradiance_wh_m2") - F.col("o.solar_irradiance_wh_m2")
        ).alias("solar_difference_wh_m2"),
        F.col("n.wind_speed_10m_m_s").alias("nasa_wind_speed_10m_m_s"),
        F.col("o.wind_speed_10m_m_s").alias("open_meteo_wind_speed_10m_m_s"),
        (F.col("n.wind_speed_10m_m_s") - F.col("o.wind_speed_10m_m_s")).alias(
            "wind_10m_difference_m_s"
        ),
        F.lit("NASA POWER").alias("canonical_source"),
        F.lit("Open-Meteo ERA5").alias("comparison_source"),
        F.col("n.source_checksum_sha256").alias("nasa_checksum_sha256"),
        F.col("o.source_checksum_sha256").alias("open_meteo_checksum_sha256"),
        F.current_timestamp().alias("transformed_at_utc"),
    )
    expected = nasa.count()
    count = frame.count()
    if count != expected:
        raise ValueError(f"Weather comparison has {count} rows; expected {expected}")
    output.parent.mkdir(parents=True, exist_ok=True)
    frame.write.format("delta").mode("overwrite").partitionBy("weather_year").save(
        str(output.resolve())
    )
    return count


def _metric(rows: list[Any], metric_id: str) -> Any:
    matches = [row for row in rows if row["metric_id"] == metric_id]
    if len(matches) != 1:
        raise ValueError(f"Expected exactly one reviewed metric: {metric_id}")
    return matches[0]


def build_demand_silver(
    spark: Any,
    data_root: Path,
    config: DemandConfig,
    silver_version: str,
    output: Path,
) -> tuple[int, str]:
    from pyspark.sql import functions as F

    weather = (
        spark.read.format("delta")
        .load(str((data_root / "silver" / "weather").resolve()))
        .where(F.col("weather_year") == config.year)
    )
    national = (
        weather.groupBy("timestamp_utc")
        .agg(F.avg("temperature_c").alias("temperature_c"))
        .select(
            F.date_format("timestamp_utc", "yyyy-MM-dd'T'HH:mm:ss").alias(
                "timestamp_utc_text"
            ),
            "temperature_c",
        )
        .orderBy("timestamp_utc_text")
        .collect()
    )
    report_rows = (
        spark.read.format("delta")
        .load(str((data_root / "silver" / "report_metrics").resolve()))
        .where(
            F.col("metric_id").isin(
                config.annual_energy_metric_id,
                config.peak_power_metric_id,
                config.peak_day_energy_metric_id,
            )
        )
        .collect()
    )
    annual = _metric(report_rows, config.annual_energy_metric_id)
    peak = _metric(report_rows, config.peak_power_metric_id)
    peak_day = _metric(report_rows, config.peak_day_energy_metric_id)
    if annual["unit"] != "GWh" or peak["unit"] != "MW" or peak_day["unit"] != "MWh":
        raise ValueError("Unexpected demand calibration units")
    peak_date = date.fromisoformat(peak_day["period"])
    national_values = sorted(
        (
            (
                datetime.fromisoformat(row["timestamp_utc_text"]).replace(tzinfo=UTC),
                float(row["temperature_c"]),
            )
            for row in national
        ),
        key=lambda item: item[0],
    )
    rows = calibrate_demand_profile(
        [item[0] for item in national_values],
        [item[1] for item in national_values],
        annual_energy_mwh=float(annual["value"]) * 1000.0,
        peak_power_mw=float(peak["value"]),
        peak_day_energy_mwh=float(peak_day["value"]),
        peak_date=peak_date,
    )
    profile_payload = {
        "method": config.method_version,
        "silver_version": silver_version,
        "annual_metric_checksum": annual["source_checksum_sha256"],
        "weather_year": config.year,
    }
    profile_version = hashlib.sha256(
        json.dumps(profile_payload, sort_keys=True).encode()
    ).hexdigest()[:16]
    enriched = [
        {
            **row,
            "weather_year": config.year,
            "profile_version": profile_version,
            "method_version": config.method_version,
            "provenance_classification": config.provenance_classification,
            "annual_energy_target_mwh": float(annual["value"]) * 1000.0,
            "peak_power_target_mw": float(peak["value"]),
            "peak_day_energy_target_mwh": float(peak_day["value"]),
            "calibration_source_ids": json.dumps(
                [annual["metric_id"], peak["metric_id"], peak_day["metric_id"]]
            ),
            "calibration_source_checksum_sha256": annual["source_checksum_sha256"],
            "silver_version": silver_version,
        }
        for row in rows
    ]
    schema = (
        "timestamp_utc timestamp, demand_mw double, hourly_energy_mwh double, "
        "national_temperature_c double, raw_shape double, is_calibration_peak_day boolean, "
        "weather_year int, profile_version string, method_version string, "
        "provenance_classification string, annual_energy_target_mwh double, "
        "peak_power_target_mw double, peak_day_energy_target_mwh double, "
        "calibration_source_ids string, calibration_source_checksum_sha256 string, "
        "silver_version string"
    )
    frame = spark.createDataFrame(enriched, schema=schema).withColumn(
        "transformed_at_utc", F.current_timestamp()
    )
    summary = frame.agg(
        F.sum("hourly_energy_mwh").alias("annual"), F.max("demand_mw").alias("peak")
    ).first()
    daily = (
        frame.where(F.to_date("timestamp_utc") == F.lit(peak_date.isoformat()))
        .agg(F.sum("hourly_energy_mwh").alias("energy"))
        .first()["energy"]
    )
    tolerance = config.calibration_tolerance_mwh
    if abs(summary["annual"] - float(annual["value"]) * 1000.0) > tolerance:
        raise ValueError("Demand annual-energy calibration failed")
    if abs(summary["peak"] - float(peak["value"])) > tolerance:
        raise ValueError("Demand peak calibration failed")
    if abs(daily - float(peak_day["value"])) > tolerance:
        raise ValueError("Demand peak-day calibration failed")
    output.parent.mkdir(parents=True, exist_ok=True)
    frame.write.format("delta").mode("overwrite").partitionBy("weather_year").save(
        str(output.resolve())
    )
    return frame.count(), profile_version


def build_canonical_technology_assumptions(
    spark: Any,
    data_root: Path,
    config: TechnologyConfig,
    silver_version: str,
    output: Path,
) -> int:
    from pyspark.sql import functions as F

    source = spark.read.format("delta").load(
        str((data_root / "silver" / "technology_costs").resolve())
    )
    choices = {item.pypsa_technology: item for item in config.technologies}
    collected = (
        source.where(F.col("model_year").isin(config.model_years))
        .where(F.col("technology").isin(list(choices)))
        .where(F.col("parameter").isin(config.parameters))
        .collect()
    )
    rows = []
    for row in collected:
        choice = choices[row["technology"]]
        rows.append(
            {
                "model_year": row["model_year"],
                "canonical_technology": choice.canonical_name,
                "pypsa_technology": row["technology"],
                "component_type": choice.component_type,
                "carrier": choice.carrier,
                "parameter": row["parameter"],
                "value": row["value"],
                "source_unit": row["unit"],
                "canonical_unit": normalize_technology_unit(row["unit"], row["parameter"]),
                "currency": "EUR" if "EUR" in row["unit"] else None,
                "currency_year": row["currency_year"],
                "provenance_classification": "ASSUMPTION",
                "selection_policy": config.source_policy,
                "source_id": row["source_id"],
                "source_file": row["source_file"],
                "source_checksum_sha256": row["source_checksum_sha256"],
                "source_silver_version": row["silver_version"],
                "silver_version": silver_version,
            }
        )
    identities = [(r["model_year"], r["canonical_technology"], r["parameter"]) for r in rows]
    if len(identities) != len(set(identities)):
        raise ValueError("Canonical technology assumptions contain duplicate natural keys")
    expected = {
        (year, tech.canonical_name)
        for year in config.model_years
        for tech in config.technologies
    }
    present = {(r["model_year"], r["canonical_technology"]) for r in rows}
    missing = sorted(expected - present)
    if missing:
        raise ValueError(f"Canonical technology mappings have no parameters: {missing}")
    schema = (
        "model_year int, canonical_technology string, pypsa_technology string, "
        "component_type string, carrier string, parameter string, value double, "
        "source_unit string, canonical_unit string, currency string, currency_year double, "
        "provenance_classification string, selection_policy string, source_id string, "
        "source_file string, source_checksum_sha256 string, source_silver_version string, "
        "silver_version string"
    )
    frame = spark.createDataFrame(rows, schema=schema).withColumn(
        "transformed_at_utc", F.current_timestamp()
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    frame.write.format("delta").mode("overwrite").partitionBy("model_year").save(
        str(output.resolve())
    )
    return frame.count()


OWID_EMBER_MAPPINGS = {
    "electricity_demand": ("Electricity demand", "Demand", "Demand", "TWh"),
    "electricity_generation": ("Electricity generation", "Total", "Total Generation", "TWh"),
    "fossil_electricity": ("Electricity generation", "Aggregate fuel", "Fossil", "TWh"),
    "renewables_electricity": ("Electricity generation", "Aggregate fuel", "Renewables", "TWh"),
    "solar_electricity": ("Electricity generation", "Fuel", "Solar", "TWh"),
    "wind_electricity": ("Electricity generation", "Fuel", "Wind", "TWh"),
    "hydro_electricity": ("Electricity generation", "Fuel", "Hydro", "TWh"),
    "net_elec_imports": ("Electricity imports", "Electricity imports", "Net Imports", "TWh"),
    "greenhouse_gas_emissions": ("Power sector emissions", "Total", "Total emissions", "mtCO2"),
    "carbon_intensity_elec": (
        "Power sector emissions",
        "CO2 intensity",
        "CO2 intensity",
        "gCO2/kWh",
    ),
    "renewables_share_elec": ("Electricity generation", "Aggregate fuel", "Renewables", "%"),
}


def build_statistics_reconciliation(
    spark: Any,
    data_root: Path,
    config: StatisticsConfig,
    silver_version: str,
    output: Path,
) -> int:
    from pyspark.sql import functions as F

    owid = spark.read.format("delta").load(str((data_root / "silver" / "owid_energy").resolve()))
    ember = spark.read.format("delta").load(
        str((data_root / "silver" / "ember_electricity").resolve())
    )
    irena = spark.read.format("delta").load(str((data_root / "silver" / "irena_energy").resolve()))
    owid_rows = {(r["year"], r["metric_code"]): r for r in owid.collect()}
    ember_rows = {
        (r["year"], r["category"], r["subcategory"], r["variable"], r["unit"]): r
        for r in ember.collect()
    }
    rows: list[dict[str, Any]] = []
    for (year, metric), primary in sorted(owid_rows.items()):
        mapping = OWID_EMBER_MAPPINGS.get(metric)
        if mapping is None:
            continue
        secondary = ember_rows.get((year, *mapping))
        primary_value = float(primary["value"]) if primary["value"] is not None else None
        secondary_value = None
        if secondary is not None and secondary["value"] is not None:
            secondary_value = float(secondary["value"])
        status, difference, percent = reconciliation_status(
            secondary_value, primary_value, config.relative_tolerance_percent
        )
        rows.append(
            {
                "comparison_id": f"ember_vs_owid:{metric}:{year}",
                "year": year,
                "metric": metric,
                "primary_source": "ember",
                "primary_value": secondary_value,
                "secondary_source": "owid",
                "secondary_value": primary_value,
                "unit": mapping[3],
                "absolute_difference": difference,
                "percent_difference": percent,
                "comparison_status": status,
                "tolerance_percent": config.relative_tolerance_percent,
                "basis_note": (
                    "OWID may incorporate Ember; matching values are not independent evidence."
                ),
                "primary_checksum_sha256": (
                    secondary["source_checksum_sha256"] if secondary else None
                ),
                "secondary_checksum_sha256": primary["source_checksum_sha256"],
                "silver_version": silver_version,
            }
        )

    irena_rows = irena.where(F.col("installed_capacity_mw").isNotNull()).collect()
    capacity_mapping = {
        "Solar": "Solar energy",
        "Wind": "Wind energy",
    }
    for ember_variable, irena_group in capacity_mapping.items():
        for year in sorted({int(r["year"]) for r in irena_rows}):
            selected = [
                r
                for r in irena_rows
                if r["year"] == year and r["technology_group"] == irena_group
            ]
            if not selected:
                continue
            irena_value = sum(float(r["installed_capacity_mw"]) for r in selected) / 1000.0
            ember_row = ember_rows.get((year, "Capacity", "Fuel", ember_variable, "GW"))
            ember_value = float(ember_row["value"]) if ember_row else None
            status, difference, percent = reconciliation_status(
                irena_value, ember_value, config.relative_tolerance_percent
            )
            rows.append(
                {
                    "comparison_id": f"irena_vs_ember:{ember_variable.casefold()}_capacity:{year}",
                    "year": year,
                    "metric": f"{ember_variable.casefold()}_installed_capacity",
                    "primary_source": "irena",
                    "primary_value": irena_value,
                    "secondary_source": "ember",
                    "secondary_value": ember_value,
                    "unit": "GW",
                    "absolute_difference": difference,
                    "percent_difference": percent,
                    "comparison_status": status,
                    "tolerance_percent": config.relative_tolerance_percent,
                    "basis_note": (
                        "IRENA leaf technologies aggregated to the source technology group."
                    ),
                    "primary_checksum_sha256": selected[0]["source_checksum_sha256"],
                    "secondary_checksum_sha256": (
                        ember_row["source_checksum_sha256"] if ember_row else None
                    ),
                    "silver_version": silver_version,
                }
            )
    schema = (
        "comparison_id string, year int, metric string, primary_source string, "
        "primary_value double, secondary_source string, secondary_value double, unit string, "
        "absolute_difference double, percent_difference double, comparison_status string, "
        "tolerance_percent double, basis_note string, primary_checksum_sha256 string, "
        "secondary_checksum_sha256 string, silver_version string"
    )
    frame = spark.createDataFrame(rows, schema=schema).withColumn(
        "transformed_at_utc", F.current_timestamp()
    )
    if frame.select("comparison_id").distinct().count() != frame.count():
        raise ValueError("Statistics reconciliation contains duplicate comparison IDs")
    output.parent.mkdir(parents=True, exist_ok=True)
    frame.write.format("delta").mode("overwrite").partitionBy("year").save(str(output.resolve()))
    return frame.count()


def _latest_feature_path(root: Path, weather_year: int) -> tuple[Path, dict[str, Any]]:
    candidates = []
    for manifest_path in root.glob("version=*/manifest.json"):
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if manifest.get("weather_year") == weather_year:
            candidates.append((manifest_path.stat().st_mtime, manifest_path.parent, manifest))
    if not candidates:
        raise FileNotFoundError(f"No renewable feature set for weather year {weather_year}")
    _, path, manifest = max(candidates, key=lambda item: item[0])
    return path, manifest


def build_optimizer_inputs(
    spark: Any,
    data_root: Path,
    config: OptimizerInputConfig,
    silver_version: str,
    demand_profile_version: str,
    output: Path,
) -> int:
    from pyspark.sql import functions as F

    demand = (
        spark.read.format("delta")
        .load(str((data_root / "silver" / "demand_hourly").resolve()))
        .where(F.col("weather_year") == config.weather_year)
    )
    feature_path, manifest = _latest_feature_path(
        data_root / "features" / "renewable_capacity_factors", config.weather_year
    )
    renewable = spark.read.format("delta").load(str(feature_path.resolve()))
    national = renewable.groupBy("timestamp_utc").agg(
        F.avg("solar_capacity_factor").alias("solar_capacity_factor"),
        F.avg("wind_capacity_factor").alias("wind_capacity_factor"),
        F.max(F.col("input_quality_has_missing").cast("int")).cast("boolean").alias(
            "input_quality_has_missing"
        ),
        F.countDistinct("location_id").alias("location_count"),
    )
    frame = demand.join(national, "timestamp_utc", "inner").select(
        "timestamp_utc",
        "demand_mw",
        "solar_capacity_factor",
        "wind_capacity_factor",
        "input_quality_has_missing",
        "location_count",
        F.lit(config.weather_year).cast("int").alias("weather_year"),
        F.lit(demand_profile_version).alias("demand_profile_version"),
        F.lit(manifest["feature_version"]).alias("renewable_feature_version"),
        F.lit(config.renewable_aggregation).alias("renewable_aggregation"),
        F.lit("SYNTHETIC_CALIBRATED").alias("demand_provenance"),
        F.lit("SYNTHETIC").alias("renewable_provenance"),
        F.lit(silver_version).alias("silver_version"),
        F.current_timestamp().alias("transformed_at_utc"),
    )
    expected = (366 if calendar.isleap(config.weather_year) else 365) * 24
    count = frame.count()
    if count != expected:
        raise ValueError(f"Optimizer hourly inputs have {count} rows; expected {expected}")
    if frame.where(
        F.col("demand_mw").isNull()
        | F.col("solar_capacity_factor").isNull()
        | F.col("wind_capacity_factor").isNull()
        | (F.col("solar_capacity_factor") < 0)
        | (F.col("solar_capacity_factor") > 1)
        | (F.col("wind_capacity_factor") < 0)
        | (F.col("wind_capacity_factor") > 1)
    ).limit(1).count():
        raise ValueError("Optimizer hourly inputs contain null or out-of-range values")
    output.parent.mkdir(parents=True, exist_ok=True)
    frame.write.format("delta").mode("overwrite").partitionBy("weather_year").save(
        str(output.resolve())
    )
    return count


def _delta_version(spark: Any, path: Path) -> int:
    from delta.tables import DeltaTable

    return int(DeltaTable.forPath(spark, str(path.resolve())).history(1).first()["version"])


def build_silver_completion(
    *, spark: Any, data_root: Path, config: SilverCompletionConfig
) -> CompletionResult:
    output_root = data_root / "silver"
    inputs = discover_open_meteo_inputs(
        data_root=data_root,
        locations_path=Path("configs/locations/morocco.yml"),
        years=config.weather.years,
    )
    feature_path, feature_manifest = _latest_feature_path(
        data_root / "features" / "renewable_capacity_factors",
        config.optimizer_inputs.weather_year,
    )
    source_payload = {
        "transform_version": TRANSFORM_VERSION,
        "config": config.model_dump(mode="json"),
        "open_meteo_checksums": sorted(item.checksum for item in inputs),
        "renewable_features": {
            "path": str(feature_path),
            "feature_version": feature_manifest["feature_version"],
            "assumption_set": feature_manifest["assumption_set"],
            "silver_delta_version": feature_manifest["silver_delta_version"],
        },
        "delta_versions": {
            name: _delta_version(spark, output_root / name)
            for name in [
                "weather",
                "report_metrics",
                "technology_costs",
                "owid_energy",
                "ember_electricity",
                "irena_energy",
            ]
        },
    }
    silver_version = hashlib.sha256(
        json.dumps(source_payload, sort_keys=True).encode()
    ).hexdigest()[:16]
    table_rows: dict[str, int] = {}
    table_rows["weather_open_meteo"] = build_open_meteo_silver(
        spark, inputs, output_root / "weather_open_meteo"
    )
    table_rows["weather_source_comparison"] = build_weather_comparison(
        spark, data_root, output_root / "weather_source_comparison"
    )
    demand_count, profile_version = build_demand_silver(
        spark,
        data_root,
        config.demand,
        silver_version,
        output_root / "demand_hourly",
    )
    table_rows["demand_hourly"] = demand_count
    table_rows["canonical_technology_assumptions"] = build_canonical_technology_assumptions(
        spark,
        data_root,
        config.technology,
        silver_version,
        output_root / "canonical_technology_assumptions",
    )
    table_rows["energy_statistics_reconciliation"] = build_statistics_reconciliation(
        spark,
        data_root,
        config.statistics,
        silver_version,
        output_root / "energy_statistics_reconciliation",
    )
    table_rows["optimizer_hourly_inputs"] = build_optimizer_inputs(
        spark,
        data_root,
        config.optimizer_inputs,
        silver_version,
        profile_version,
        output_root / "optimizer_hourly_inputs",
    )
    manifest = {
        "silver_version": silver_version,
        "transform_version": TRANSFORM_VERSION,
        "generated_at_utc": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "table_rows": table_rows,
        "demand_profile_version": profile_version,
        "config": config.model_dump(mode="json"),
        "inputs": source_payload,
    }
    manifest_path = output_root / "manifests" / f"silver_completion_{silver_version}.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    return CompletionResult(silver_version, table_rows, output_root)


def main() -> None:
    parser = argparse.ArgumentParser(description="Complete Atlas optimizer-facing Silver tables")
    parser.add_argument(
        "--config", type=Path, default=Path("configs/silver/foundation_completion.yml")
    )
    args = parser.parse_args()
    config = load_config(args.config)
    spark = build_spark_session("atlas-silver-completion")
    try:
        result = build_silver_completion(
            spark=spark, data_root=get_settings().data_root, config=config
        )
        print(
            f"Silver completion succeeded: version={result.silver_version} "
            f"tables={result.table_rows}"
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
