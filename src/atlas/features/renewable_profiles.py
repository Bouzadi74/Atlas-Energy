import argparse
import hashlib
import json
import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator

from atlas.config import get_settings
from atlas.transforms.weather_silver import build_spark_session

LOGGER = logging.getLogger(__name__)


class SolarAssumptions(BaseModel):
    model_config = ConfigDict(extra="forbid")

    reference_irradiance_wh_m2: float = Field(gt=0)
    system_loss_fraction: float = Field(ge=0, lt=1)
    temperature_coefficient_per_c: float = Field(lt=0)
    reference_cell_temperature_c: float
    sapm_temperature_a: float
    sapm_temperature_b: float
    sapm_delta_temperature_c: float = Field(ge=0)


class WindAssumptions(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source_height_m: float = Field(gt=0)
    hub_height_m: float = Field(gt=0)
    shear_exponent: float = Field(gt=0, le=1)
    cut_in_speed_m_s: float = Field(gt=0)
    rated_speed_m_s: float = Field(gt=0)
    cut_out_speed_m_s: float = Field(gt=0)

    @model_validator(mode="after")
    def validate_power_curve_order(self) -> "WindAssumptions":
        if not self.cut_in_speed_m_s < self.rated_speed_m_s < self.cut_out_speed_m_s:
            raise ValueError("wind speeds must satisfy cut-in < rated < cut-out")
        return self


class RenewableProfileConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    version: str = Field(pattern=r"^[a-z0-9][a-z0-9._-]+$")
    provenance_classification: Literal["SYNTHETIC"]
    description: str = Field(min_length=1)
    solar: SolarAssumptions
    wind: WindAssumptions


@dataclass(frozen=True)
class RenewableFeatureBuildResult:
    output_path: Path
    feature_version: str
    row_count: int
    location_count: int
    solar_mean: float
    wind_mean: float
    silver_delta_version: int


def load_renewable_profile_config(path: Path) -> RenewableProfileConfig:
    try:
        content = yaml.safe_load(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        raise FileNotFoundError(f"Renewable profile configuration not found: {path}") from None
    except yaml.YAMLError as error:
        raise ValueError(
            f"Invalid YAML in renewable profile configuration {path}: {error}"
        ) from error
    return RenewableProfileConfig.model_validate(content)


def feature_version(
    config: RenewableProfileConfig,
    *,
    weather_year: int,
    silver_delta_version: int,
) -> str:
    canonical = json.dumps(
        {
            "config": config.model_dump(mode="json"),
            "weather_year": weather_year,
            "silver_delta_version": silver_delta_version,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode()).hexdigest()[:16]


def solar_capacity_factor(
    irradiance_wh_m2: float,
    ambient_temperature_c: float,
    assumptions: SolarAssumptions,
    wind_speed_m_s: float = 1.0,
) -> float:
    """Calculate PVWatts availability using pvlib with versioned temperature assumptions."""

    from pvlib.pvsystem import pvwatts_dc
    from pvlib.temperature import sapm_cell

    irradiance = max(irradiance_wh_m2, 0.0)
    cell_temperature = sapm_cell(
        irradiance,
        ambient_temperature_c,
        max(wind_speed_m_s, 0.0),
        assumptions.sapm_temperature_a,
        assumptions.sapm_temperature_b,
        assumptions.sapm_delta_temperature_c,
    )
    dc_per_unit = pvwatts_dc(
        irradiance,
        cell_temperature,
        pdc0=1.0,
        gamma_pdc=assumptions.temperature_coefficient_per_c,
        temp_ref=assumptions.reference_cell_temperature_c,
    )
    capacity_factor = float(dc_per_unit) * (
        1000.0 / assumptions.reference_irradiance_wh_m2
    ) * (1.0 - assumptions.system_loss_fraction)
    return min(1.0, max(0.0, capacity_factor))


def _wind_power_curve(assumptions: WindAssumptions) -> tuple[list[float], list[float]]:
    steps = 181
    operating_speeds = [
        assumptions.cut_in_speed_m_s
        + (assumptions.rated_speed_m_s - assumptions.cut_in_speed_m_s) * index / (steps - 1)
        for index in range(steps)
    ]
    denominator = assumptions.rated_speed_m_s**3 - assumptions.cut_in_speed_m_s**3
    operating_values = [
        (speed**3 - assumptions.cut_in_speed_m_s**3) / denominator
        for speed in operating_speeds
    ]
    return (
        [
            0.0,
            *operating_speeds,
            assumptions.cut_out_speed_m_s - 1e-6,
            assumptions.cut_out_speed_m_s,
        ],
        [0.0, *operating_values, 1.0, 0.0],
    )


def hub_height_wind_speed(
    source_speed_m_s: float,
    assumptions: WindAssumptions,
) -> float:
    return max(source_speed_m_s, 0.0) * (
        assumptions.hub_height_m / assumptions.source_height_m
    ) ** assumptions.shear_exponent


def wind_capacity_factor(hub_speed_m_s: float, assumptions: WindAssumptions) -> float:
    """Calculate availability with windpowerlib and the governed generic power curve."""

    import numpy as np
    from windpowerlib.power_output import power_curve

    curve_speeds, curve_values = _wind_power_curve(assumptions)
    value = power_curve(
        np.asarray([max(hub_speed_m_s, 0.0)]),
        np.asarray(curve_speeds),
        np.asarray(curve_values),
    )[0]
    return min(1.0, max(0.0, float(value)))


def build_renewable_features(
    *,
    spark: Any,
    silver_path: Path,
    output_root: Path,
    config: RenewableProfileConfig,
    year: int,
) -> RenewableFeatureBuildResult:
    """Build validated solar/wind availability features from the Silver Delta table."""

    import pandas as pd
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F

    silver_absolute = str(silver_path.resolve())
    delta_table = DeltaTable.forPath(spark, silver_absolute)
    silver_delta_version = int(delta_table.history(1).select("version").first()["version"])
    version = feature_version(
        config,
        weather_year=year,
        silver_delta_version=silver_delta_version,
    )
    output_path = output_root / f"version={version}"

    weather = (
        spark.read.format("delta")
        .load(silver_absolute)
        .where(F.col("weather_year") == year)
    )
    if weather.limit(1).count() == 0:
        raise ValueError(f"Silver weather table contains no rows for {year}")

    solar = config.solar
    wind = config.wind

    @F.pandas_udf("double")
    def solar_cf_udf(
        irradiance: pd.Series, temperature: pd.Series, wind_speed: pd.Series
    ) -> pd.Series:
        from pvlib.pvsystem import pvwatts_dc
        from pvlib.temperature import sapm_cell

        poa = irradiance.clip(lower=0.0)
        cell_temperature = sapm_cell(
            poa,
            temperature,
            wind_speed.clip(lower=0.0),
            solar.sapm_temperature_a,
            solar.sapm_temperature_b,
            solar.sapm_delta_temperature_c,
        )
        result = pvwatts_dc(
            poa,
            cell_temperature,
            pdc0=1.0,
            gamma_pdc=solar.temperature_coefficient_per_c,
            temp_ref=solar.reference_cell_temperature_c,
        )
        result *= 1000.0 / solar.reference_irradiance_wh_m2
        result *= 1.0 - solar.system_loss_fraction
        return result.clip(lower=0.0, upper=1.0)

    curve_speeds, curve_values = _wind_power_curve(wind)

    @F.pandas_udf("double")
    def hub_speed_udf(source_speed: pd.Series) -> pd.Series:
        return source_speed.clip(lower=0.0) * (
            wind.hub_height_m / wind.source_height_m
        ) ** wind.shear_exponent

    @F.pandas_udf("double")
    def wind_cf_udf(hub_speed_series: pd.Series) -> pd.Series:
        import numpy as np
        from windpowerlib.power_output import power_curve

        values = power_curve(
            hub_speed_series.clip(lower=0.0).to_numpy(),
            np.asarray(curve_speeds),
            np.asarray(curve_values),
        )
        return pd.Series(values, index=hub_speed_series.index).clip(lower=0.0, upper=1.0)

    hub_speed = hub_speed_udf(F.col("wind_speed_50m_m_s"))
    solar_cf = solar_cf_udf(
        F.col("solar_irradiance_wh_m2"),
        F.col("temperature_c"),
        F.col("wind_speed_10m_m_s"),
    )
    wind_cf = wind_cf_udf(hub_speed)

    features = weather.select(
        "timestamp_utc",
        "location_id",
        "zone",
        "latitude",
        "longitude",
        solar_cf.alias("solar_capacity_factor"),
        hub_speed.alias("wind_speed_hub_m_s"),
        wind_cf.alias("wind_capacity_factor"),
        F.col("quality_has_missing").alias("input_quality_has_missing"),
        F.lit(config.version).alias("assumption_set"),
        F.lit(version).alias("feature_version"),
        F.lit(silver_delta_version).cast("long").alias("silver_delta_version"),
        F.lit(config.provenance_classification).alias("provenance_classification"),
        "source_checksum_sha256",
        F.lit(year).cast("integer").alias("weather_year"),
        F.current_timestamp().alias("generated_at_utc"),
    )

    row_count = features.count()
    expected_row_count = weather.count()
    if row_count != expected_row_count:
        raise ValueError(f"Feature row count is {row_count}; expected {expected_row_count}")
    if features.where(
        F.col("solar_capacity_factor").isNull()
        | F.col("wind_capacity_factor").isNull()
        | (F.col("solar_capacity_factor") < 0)
        | (F.col("solar_capacity_factor") > 1)
        | (F.col("wind_capacity_factor") < 0)
        | (F.col("wind_capacity_factor") > 1)
    ).limit(1).count():
        raise ValueError("Feature data contains null or out-of-range capacity factors")
    if (
        features.groupBy("timestamp_utc", "location_id")
        .count()
        .where(F.col("count") > 1)
        .limit(1)
        .count()
    ):
        raise ValueError("Feature data contains duplicate location-hour keys")

    location_count = features.select("location_id").distinct().count()
    summary = features.agg(
        F.avg("solar_capacity_factor").alias("solar_mean"),
        F.avg("wind_capacity_factor").alias("wind_mean"),
    ).first()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    (
        features.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"weather_year = {year}")
        .partitionBy("weather_year")
        .save(str(output_path.resolve()))
    )
    manifest = {
        "feature_version": version,
        "assumption_set": config.version,
        "provenance_classification": config.provenance_classification,
        "weather_year": year,
        "silver_path": str(silver_path),
        "silver_delta_version": silver_delta_version,
        "row_count": row_count,
        "location_count": location_count,
        "solar_mean_capacity_factor": float(summary["solar_mean"]),
        "wind_mean_capacity_factor": float(summary["wind_mean"]),
        "generated_at_utc": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "assumptions": config.model_dump(mode="json"),
    }
    (output_path / "manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    return RenewableFeatureBuildResult(
        output_path=output_path,
        feature_version=version,
        row_count=row_count,
        location_count=location_count,
        solar_mean=float(summary["solar_mean"]),
        wind_mean=float(summary["wind_mean"]),
        silver_delta_version=silver_delta_version,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build versioned renewable capacity-factor features"
    )
    parser.add_argument("--year", required=True, type=int)
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("configs/features/renewable_profiles.yml"),
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    settings = get_settings()
    logging.basicConfig(
        level=settings.log_level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    config = load_renewable_profile_config(args.config)
    spark = build_spark_session("atlas-renewable-features")
    try:
        result = build_renewable_features(
            spark=spark,
            silver_path=settings.data_root / "silver" / "weather",
            output_root=settings.data_root / "features" / "renewable_capacity_factors",
            config=config,
            year=args.year,
        )
        LOGGER.info(
            "Renewable features complete: version=%s rows=%d locations=%d "
            "solar_mean=%.4f wind_mean=%.4f path=%s",
            result.feature_version,
            result.row_count,
            result.location_count,
            result.solar_mean,
            result.wind_mean,
            result.output_path,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
