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

LOGGER = logging.getLogger(__name__)
TRANSFORM_VERSION = "model-input-package-v1"


class CalendarPolicy(BaseModel):
    model_config = ConfigDict(extra="forbid")

    baseline_hours: int = 8784
    planning_hours: int = 8760
    method: Literal["drop_feb29_affine_preserve_annual_energy_and_peak"]
    description: str = Field(min_length=1)


class CapacityMapping(BaseModel):
    model_config = ConfigDict(extra="forbid")

    canonical_technology: str
    component_type: str
    carrier: str
    source_technology_group: str | None = None
    source_technology: str | None = None
    source_sub_technology: str | None = None

    @model_validator(mode="after")
    def require_source_selector(self) -> "CapacityMapping":
        if not any(
            (self.source_technology_group, self.source_technology, self.source_sub_technology)
        ):
            raise ValueError("capacity mapping requires at least one source selector")
        return self


class CapacityPolicy(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source_year: int
    country_code: str = Field(min_length=3, max_length=3)
    producer_type: str
    system_total_metric_id: str
    wind_check_metric_id: str
    renewable_check_metric_id: str
    maximum_total_difference_percent: float = Field(gt=0)
    residual_technology: str
    residual_policy: str = Field(min_length=1)
    mappings: list[CapacityMapping] = Field(min_length=1)


class TechnologyPolicy(BaseModel):
    model_config = ConfigDict(extra="forbid")

    required_parameters_by_technology: dict[str, list[str]] = Field(min_length=1)
    selection_policy: str = Field(min_length=1)


class ModelInputConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    version: str = Field(pattern=r"^[a-z0-9][a-z0-9._-]+$")
    baseline_year: int
    planning_year: int
    cost_model_year: int
    calendar: CalendarPolicy
    capacity: CapacityPolicy
    technology: TechnologyPolicy

    @model_validator(mode="after")
    def validate_contract(self) -> "ModelInputConfig":
        names = [item.canonical_technology for item in self.capacity.mappings]
        if len(names) != len(set(names)):
            raise ValueError("capacity mapping canonical technologies must be unique")
        if self.capacity.residual_technology not in names:
            raise ValueError("residual technology must exist in capacity mappings")
        return self


@dataclass(frozen=True)
class DeltaSnapshot:
    table: Any
    delta_version: int
    active_files: tuple[Path, ...]
    checksum_sha256: str


@dataclass(frozen=True)
class ModelInputBuildResult:
    output_path: Path
    package_version: str
    baseline_hours: int
    planning_hours: int
    capacity_rows: int
    technology_rows: int


def load_config(path: Path) -> ModelInputConfig:
    try:
        content = yaml.safe_load(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        raise FileNotFoundError(f"Model-input configuration not found: {path}") from None
    except yaml.YAMLError as error:
        raise ValueError(f"Invalid model-input YAML {path}: {error}") from error
    return ModelInputConfig.model_validate(content)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def read_delta_snapshot(path: Path) -> DeltaSnapshot:
    """Read the active files from a local Delta table without starting another Spark JVM."""

    import pyarrow as pa
    import pyarrow.parquet as pq

    log_dir = path / "_delta_log"
    logs = sorted(log_dir.glob("[0-9]*.json"))
    if not logs:
        raise FileNotFoundError(f"Delta log not found: {log_dir}")
    active: dict[str, None] = {}
    for log_path in logs:
        for line in log_path.read_text(encoding="utf-8").splitlines():
            action = json.loads(line)
            if "add" in action:
                active[action["add"]["path"]] = None
            if "remove" in action:
                active.pop(action["remove"]["path"], None)
    files = tuple(
        sorted((path / relative for relative in active), key=lambda item: item.as_posix())
    )
    if not files:
        raise ValueError(f"Delta table has no active data files: {path}")
    tables = [pq.ParquetFile(file_path).read() for file_path in files]
    table = pa.concat_tables(tables, promote_options="default").to_pandas()
    digest = hashlib.sha256()
    for file_path in files:
        digest.update(file_path.relative_to(path).as_posix().encode())
        digest.update(_sha256_file(file_path).encode())
    return DeltaSnapshot(
        table=table,
        delta_version=int(logs[-1].stem),
        active_files=files,
        checksum_sha256=digest.hexdigest(),
    )


def reconcile_leap_year_rows(
    rows: list[dict[str, Any]],
    *,
    planning_year: int,
) -> list[dict[str, Any]]:
    """Remove 29 February while preserving annual demand energy and peak exactly."""

    if len(rows) != 8784:
        raise ValueError(f"Leap-year input has {len(rows)} rows; expected 8784")
    ordered = sorted(rows, key=lambda row: row["timestamp_utc"])
    timestamps = [row["timestamp_utc"] for row in ordered]
    if len(set(timestamps)) != 8784:
        raise ValueError("Leap-year timestamps must be unique")
    kept = [
        row.copy()
        for row in ordered
        if not (row["timestamp_utc"].month == 2 and row["timestamp_utc"].day == 29)
    ]
    if len(kept) != 8760:
        raise ValueError(f"Calendar conversion retained {len(kept)} rows; expected 8760")

    target_energy = sum(float(row["demand_mw"]) for row in ordered)
    target_peak = max(float(row["demand_mw"]) for row in ordered)
    retained_energy = sum(float(row["demand_mw"]) for row in kept)
    retained_peak = max(float(row["demand_mw"]) for row in kept)
    denominator = retained_energy - len(kept) * retained_peak
    if denominator == 0:
        raise ValueError("Cannot calibrate a constant planning demand profile")
    scale = (target_energy - len(kept) * target_peak) / denominator
    offset = target_peak - scale * retained_peak
    if scale <= 0:
        raise ValueError("Calendar reconciliation produced a non-positive demand scale")

    converted: list[dict[str, Any]] = []
    for index, row in enumerate(kept):
        source_timestamp = row["timestamp_utc"]
        demand = scale * float(row["demand_mw"]) + offset
        if demand <= 0:
            raise ValueError("Calendar reconciliation produced non-positive demand")
        row.update(
            {
                "snapshot_index": index,
                "source_timestamp_utc": source_timestamp,
                "planning_timestamp_utc": source_timestamp.replace(year=planning_year),
                "timestamp_utc": source_timestamp,
                "demand_mw": demand,
                "snapshot_weight_hours": 1.0,
                "calendar_policy": "drop_feb29_affine_preserve_annual_energy_and_peak",
                "demand_affine_scale": scale,
                "demand_affine_offset_mw": offset,
            }
        )
        converted.append(row)

    if abs(sum(row["demand_mw"] for row in converted) - target_energy) > 0.01:
        raise ValueError("Planning conversion did not preserve annual demand energy")
    if abs(max(row["demand_mw"] for row in converted) - target_peak) > 1e-9:
        raise ValueError("Planning conversion did not preserve annual demand peak")
    return converted


def _metric(report_rows: Any, metric_id: str) -> Any:
    selected = report_rows.loc[report_rows["metric_id"] == metric_id]
    if len(selected) != 1:
        raise ValueError(
            f"Expected exactly one reviewed report metric {metric_id}; got {len(selected)}"
        )
    row = selected.iloc[0]
    if row["review_status"] != "human_verified_visual":
        raise ValueError(f"Report metric {metric_id} is not visually reviewed")
    return row


def build_existing_capacity(
    irena_rows: Any,
    report_rows: Any,
    policy: CapacityPolicy,
) -> tuple[Any, dict[str, Any]]:
    import pandas as pd

    base = irena_rows.loc[
        (irena_rows["country_code"] == policy.country_code)
        & (irena_rows["year"] == policy.source_year)
        & (irena_rows["producer_type"] == policy.producer_type)
        & irena_rows["installed_capacity_mw"].notna()
    ].copy()
    if base.empty:
        raise ValueError("No IRENA capacity rows match the configured grid and year")

    output: list[dict[str, Any]] = []
    for mapping in policy.mappings:
        selected = base
        selectors = {
            "technology_group": mapping.source_technology_group,
            "technology": mapping.source_technology,
            "sub_technology": mapping.source_sub_technology,
        }
        for column, value in selectors.items():
            if value is not None:
                selected = selected.loc[selected[column] == value]
        if selected.empty:
            raise ValueError(
                f"No IRENA row matches capacity mapping {mapping.canonical_technology}"
            )
        raw_capacity = float(selected["installed_capacity_mw"].sum())
        output.append(
            {
                "canonical_technology": mapping.canonical_technology,
                "component_type": mapping.component_type,
                "carrier": mapping.carrier,
                "capacity_year": policy.source_year,
                "raw_installed_capacity_mw": raw_capacity,
                "reconciliation_adjustment_mw": 0.0,
                "installed_capacity_mw": raw_capacity,
                "source_row_count": len(selected),
                "source_technology_group": " | ".join(sorted(set(selected.technology_group))),
                "source_technology": " | ".join(sorted(set(selected.technology))),
                "source_sub_technology": " | ".join(sorted(set(selected.sub_technology))),
                "source_id": "irena",
                "source_file": " | ".join(sorted(set(selected.source_file))),
                "source_checksum_sha256": " | ".join(
                    sorted(set(selected.source_checksum_sha256))
                ),
                "upstream_provenance": "OBSERVED",
                "provenance_classification": "DERIVED",
                "grid_scope": "national_on_grid",
                "inclusion_policy": "included_existing_capacity",
            }
        )

    capacity = pd.DataFrame(output)
    onee_total = _metric(report_rows, policy.system_total_metric_id)
    if onee_total["unit"] != "MW":
        raise ValueError("ONEE installed-capacity total must use MW")
    target_total = float(onee_total["value"])
    raw_total = float(capacity["raw_installed_capacity_mw"].sum())
    difference_mw = target_total - raw_total
    difference_percent = abs(difference_mw) / target_total * 100.0
    if difference_percent > policy.maximum_total_difference_percent:
        raise ValueError(
            f"IRENA/ONEE capacity difference is {difference_percent:.3f}%; "
            f"limit is {policy.maximum_total_difference_percent:.3f}%"
        )
    residual_mask = capacity.canonical_technology == policy.residual_technology
    if residual_mask.sum() != 1:
        raise ValueError("Configured residual technology must identify exactly one capacity row")
    capacity.loc[residual_mask, "reconciliation_adjustment_mw"] = difference_mw
    capacity.loc[residual_mask, "installed_capacity_mw"] += difference_mw
    capacity["reconciliation_policy"] = policy.residual_policy
    capacity["system_total_source_id"] = onee_total["source_id"]
    capacity["system_total_source_checksum_sha256"] = onee_total[
        "source_checksum_sha256"
    ]
    if (capacity.installed_capacity_mw <= 0).any():
        raise ValueError("Reconciled installed capacities must be positive")
    if abs(float(capacity.installed_capacity_mw.sum()) - target_total) > 1e-6:
        raise ValueError("Reconciled capacities do not equal the ONEE national total")

    wind = _metric(report_rows, policy.wind_check_metric_id)
    wind_capacity = float(
        capacity.loc[
            capacity.canonical_technology == "onshore_wind", "raw_installed_capacity_mw"
        ].sum()
    )
    renewable = _metric(report_rows, policy.renewable_check_metric_id)
    renewable_technologies = {"bioenergy", "hydro", "solar_utility", "solar_csp", "onshore_wind"}
    irena_renewable = float(
        capacity.loc[
            capacity.canonical_technology.isin(renewable_technologies),
            "raw_installed_capacity_mw",
        ].sum()
    )
    reconciliation = {
        "onee_total_mw": target_total,
        "irena_on_grid_total_mw": raw_total,
        "residual_adjustment_mw": difference_mw,
        "absolute_difference_percent": difference_percent,
        "anre_wind_mw": float(wind["value"]),
        "irena_on_grid_wind_mw": wind_capacity,
        "wind_difference_mw": wind_capacity - float(wind["value"]),
        "mef_renewable_mw": float(renewable["value"]),
        "mef_renewable_period": str(renewable["period"]),
        "irena_on_grid_renewable_excluding_pumped_mw": irena_renewable,
        "renewable_difference_mw": irena_renewable - float(renewable["value"]),
        "renewable_basis_note": (
            "Comparison only: MEF is an August 2024 reported renewable total; IRENA is a "
            "year-end on-grid sum excluding pumped storage. The difference is not allocated."
        ),
    }
    return capacity.sort_values("canonical_technology").reset_index(drop=True), reconciliation


def validate_technology_assumptions(rows: Any, config: ModelInputConfig) -> Any:
    selected = rows.loc[rows["model_year"] == config.cost_model_year].copy()
    if selected.empty:
        raise ValueError(f"No canonical technology assumptions for {config.cost_model_year}")
    if selected[["canonical_technology", "parameter"]].duplicated().any():
        raise ValueError("Technology assumptions contain duplicate technology/parameter keys")
    available = set(zip(selected.canonical_technology, selected.parameter, strict=False))
    missing = [
        f"{technology}:{parameter}"
        for technology, parameters in config.technology.required_parameters_by_technology.items()
        for parameter in parameters
        if (technology, parameter) not in available
    ]
    if missing:
        raise ValueError(f"Required technology assumptions are missing: {', '.join(missing)}")
    if selected["value"].isna().any():
        raise ValueError("Technology assumptions contain null values")
    return selected.sort_values(["canonical_technology", "parameter"]).reset_index(drop=True)


def _write_frame(frame: Any, output_path: Path, name: str) -> dict[str, Any]:
    parquet_path = output_path / f"{name}.parquet"
    csv_path = output_path / f"{name}.csv"
    frame.to_parquet(parquet_path, index=False)
    frame.to_csv(csv_path, index=False, encoding="utf-8")
    return {
        "rows": len(frame),
        "parquet": parquet_path.name,
        "parquet_sha256": _sha256_file(parquet_path),
        "csv": csv_path.name,
        "csv_sha256": _sha256_file(csv_path),
    }


def build_model_input_package(
    *,
    data_root: Path,
    output_root: Path,
    config: ModelInputConfig,
) -> ModelInputBuildResult:
    import pandas as pd

    silver_root = data_root / "silver"
    source_names = (
        "optimizer_hourly_inputs",
        "irena_energy",
        "report_metrics",
        "canonical_technology_assumptions",
    )
    snapshots = {name: read_delta_snapshot(silver_root / name) for name in source_names}
    version_payload = {
        "transform_version": TRANSFORM_VERSION,
        "config": config.model_dump(mode="json"),
        "sources": {
            name: {
                "delta_version": snapshot.delta_version,
                "checksum_sha256": snapshot.checksum_sha256,
            }
            for name, snapshot in snapshots.items()
        },
    }
    package_version = hashlib.sha256(
        json.dumps(version_payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()[:16]
    output_path = output_root / f"version={package_version}"
    manifest_path = output_path / "manifest.json"
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        return ModelInputBuildResult(
            output_path=output_path,
            package_version=package_version,
            baseline_hours=manifest["validation"]["baseline_hours"],
            planning_hours=manifest["validation"]["planning_hours"],
            capacity_rows=manifest["validation"]["capacity_rows"],
            technology_rows=manifest["validation"]["technology_rows"],
        )

    hourly = snapshots["optimizer_hourly_inputs"].table.copy()
    hourly = hourly.loc[hourly["weather_year"] == config.baseline_year].copy()
    hourly = hourly.sort_values("timestamp_utc").reset_index(drop=True)
    if len(hourly) != config.calendar.baseline_hours:
        raise ValueError(
            f"Baseline hourly table has {len(hourly)} rows; "
            f"expected {config.calendar.baseline_hours}"
        )
    if hourly.timestamp_utc.duplicated().any() or hourly.input_quality_has_missing.any():
        raise ValueError("Baseline hourly inputs have duplicate timestamps or missing-input flags")
    renewable_bounds_valid = hourly.solar_capacity_factor.between(0, 1).all() and (
        hourly.wind_capacity_factor.between(0, 1).all()
    )
    if not renewable_bounds_valid:
        raise ValueError("Renewable capacity factors must be within [0, 1]")
    baseline_energy = float(hourly.demand_mw.sum())
    baseline_peak = float(hourly.demand_mw.max())
    baseline = hourly.copy()
    baseline.insert(0, "snapshot_index", range(len(baseline)))
    baseline["snapshot_weight_hours"] = 1.0
    baseline["calendar_policy"] = "complete_leap_year"

    planning_rows = reconcile_leap_year_rows(
        hourly.to_dict(orient="records"), planning_year=config.planning_year
    )
    planning = pd.DataFrame(planning_rows)
    if len(planning) != config.calendar.planning_hours:
        raise ValueError("Planning-hour validation failed")

    capacity, capacity_reconciliation = build_existing_capacity(
        snapshots["irena_energy"].table,
        snapshots["report_metrics"].table,
        config.capacity,
    )
    technology = validate_technology_assumptions(
        snapshots["canonical_technology_assumptions"].table,
        config,
    )

    output_path.mkdir(parents=True, exist_ok=False)
    files = {
        "hourly_baseline_8784": _write_frame(baseline, output_path, "hourly_baseline_8784"),
        "hourly_planning_8760": _write_frame(planning, output_path, "hourly_planning_8760"),
        "existing_capacity": _write_frame(capacity, output_path, "existing_capacity"),
        "technology_assumptions": _write_frame(
            technology, output_path, "technology_assumptions"
        ),
    }
    assumptions_path = output_path / "assumptions.json"
    assumptions_path.write_text(
        json.dumps(config.model_dump(mode="json"), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    files["assumptions"] = {
        "rows": 1,
        "json": assumptions_path.name,
        "json_sha256": _sha256_file(assumptions_path),
    }
    manifest = {
        "package_version": package_version,
        "transform_version": TRANSFORM_VERSION,
        "generated_at_utc": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "optimizer_ready": True,
        "files": files,
        "validation": {
            "baseline_hours": len(baseline),
            "planning_hours": len(planning),
            "baseline_annual_energy_mwh": baseline_energy,
            "planning_annual_energy_mwh": float(planning.demand_mw.sum()),
            "baseline_peak_mw": baseline_peak,
            "planning_peak_mw": float(planning.demand_mw.max()),
            "capacity_rows": len(capacity),
            "installed_capacity_mw": float(capacity.installed_capacity_mw.sum()),
            "technology_rows": len(technology),
            "capacity_factor_bounds_passed": True,
            "unique_hour_keys_passed": True,
        },
        "capacity_reconciliation": capacity_reconciliation,
        "source_snapshots": version_payload["sources"],
        "provenance": {
            "hourly_demand": "SYNTHETIC_CALIBRATED",
            "renewable_availability": "SYNTHETIC",
            "installed_capacity": "DERIVED from OBSERVED IRENA detail and reviewed ONEE total",
            "technology_assumptions": "ASSUMPTION",
        },
        "limitations": [
            "The hourly demand shape is synthetic-calibrated, not measured operations.",
            "Equal-weight renewable aggregation is a national-node MVP assumption.",
            "The 8760-hour planning conversion does not preserve the peak-day energy anchor.",
            (
                "MEF and IRENA renewable totals have different period/basis definitions and are "
                "not averaged."
            ),
            (
                "Existing-only technologies require separate operating assumptions in the "
                "PyPSA network builder."
            ),
        ],
    }
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return ModelInputBuildResult(
        output_path=output_path,
        package_version=package_version,
        baseline_hours=len(baseline),
        planning_hours=len(planning),
        capacity_rows=len(capacity),
        technology_rows=len(technology),
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build a versioned optimizer input package")
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("configs/model_inputs/morocco_baseline.yml"),
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    settings = get_settings()
    logging.basicConfig(
        level=settings.log_level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    result = build_model_input_package(
        data_root=settings.data_root,
        output_root=settings.data_root / "model_inputs",
        config=load_config(args.config),
    )
    LOGGER.info(
        "Model-input package complete: version=%s baseline_hours=%d planning_hours=%d "
        "capacity_rows=%d technology_rows=%d path=%s",
        result.package_version,
        result.baseline_hours,
        result.planning_hours,
        result.capacity_rows,
        result.technology_rows,
        result.output_path,
    )


if __name__ == "__main__":
    main()
