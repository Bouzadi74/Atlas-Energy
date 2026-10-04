import argparse
import hashlib
import json
import logging
import math
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from sqlalchemy import func, insert, select, update
from sqlalchemy.orm import Session, sessionmaker

from atlas.config import get_settings
from atlas.database import build_engine, build_session_factory
from atlas.database.models import (
    CapacityResultRow,
    CostComponentRow,
    DispatchHourlyRow,
    OptimizationRunRow,
    RunArtifactRow,
    RunProvenanceRow,
    ScenarioInputRow,
    StorageHourlyRow,
)

LOGGER = logging.getLogger(__name__)
REQUIRED_OUTPUTS = {
    "annual_summary",
    "capacity",
    "dispatch",
    "hourly_balance",
    "kpis",
    "storage",
}


@dataclass(frozen=True)
class PublicationResult:
    run_id: str
    reused: bool
    capacity_rows: int
    dispatch_rows: int
    storage_rows: int
    cost_rows: int


@dataclass(frozen=True)
class ValidatedRun:
    root: Path
    manifest: dict[str, Any]
    manifest_sha256: str
    scenario_id: str
    capacity: Any
    dispatch: Any
    storage: Any
    annual_summary: Any
    hourly_balance: Any
    artifacts: list[dict[str, Any]]


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _require_columns(frame: Any, name: str, columns: set[str]) -> None:
    missing = columns.difference(frame.columns)
    if missing:
        raise ValueError(f"{name} is missing columns: {', '.join(sorted(missing))}")


def _assert_close(actual: float, expected: float, label: str) -> None:
    tolerance = max(1e-5, abs(expected) * 1e-8)
    if not math.isclose(actual, expected, rel_tol=1e-8, abs_tol=tolerance):
        raise ValueError(f"{label} does not reconcile: actual={actual}, expected={expected}")


def _read_manifest(run_path: Path) -> tuple[Path, dict[str, Any], str]:
    root = run_path.resolve()
    manifest_path = root / "manifest.json"
    if not manifest_path.is_file():
        raise FileNotFoundError(f"Optimization manifest not found: {manifest_path}")
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as error:
        raise ValueError(f"Invalid optimization manifest: {error}") from error
    return root, manifest, _sha256(manifest_path)


def _verify_artifacts(
    root: Path, manifest: dict[str, Any]
) -> tuple[dict[str, Path], list[dict[str, Any]]]:
    outputs = manifest.get("outputs")
    if not isinstance(outputs, dict) or not REQUIRED_OUTPUTS.issubset(outputs):
        raise ValueError("Manifest does not declare the complete optimizer output contract")
    parquet_paths: dict[str, Path] = {}
    artifacts: list[dict[str, Any]] = []
    for artifact_name, contract in outputs.items():
        if not isinstance(contract, dict):
            raise ValueError(f"Invalid artifact contract for {artifact_name}")
        for artifact_format in ("parquet", "csv", "json"):
            file_key = f"{artifact_format}_file"
            checksum_key = f"{artifact_format}_sha256"
            if file_key not in contract:
                continue
            relative_path = str(contract[file_key])
            if Path(relative_path).name != relative_path:
                raise ValueError(f"Artifact path must be a filename: {relative_path}")
            path = root / relative_path
            if not path.is_file():
                raise FileNotFoundError(f"Optimizer artifact not found: {path}")
            expected_checksum = str(contract.get(checksum_key, ""))
            actual_checksum = _sha256(path)
            if actual_checksum != expected_checksum:
                raise ValueError(f"Checksum mismatch for {path}")
            if artifact_format == "parquet":
                parquet_paths[artifact_name] = path
            artifacts.append(
                {
                    "artifact_name": artifact_name,
                    "artifact_format": artifact_format,
                    "relative_path": relative_path,
                    "sha256": actual_checksum,
                    "row_count": contract.get("rows"),
                    "byte_count": path.stat().st_size,
                }
            )
    return parquet_paths, artifacts


def validate_run_artifacts(run_path: Path) -> ValidatedRun:
    import pandas as pd

    root, manifest, manifest_sha256 = _read_manifest(run_path)
    required_manifest = {
        "config_version",
        "engine_version",
        "generated_at_utc",
        "input_metadata",
        "kpis",
        "model_version",
        "run_id",
        "software_versions",
        "solver",
        "solver_status",
        "termination_condition",
        "validation",
    }
    missing_manifest = required_manifest.difference(manifest)
    if missing_manifest:
        raise ValueError(f"Manifest is missing fields: {', '.join(sorted(missing_manifest))}")
    if manifest["solver_status"] != "ok" or manifest["termination_condition"] != "optimal":
        raise ValueError("Only successful, optimal runs can be published")
    failed_checks = [
        key
        for key, value in manifest["validation"].items()
        if key.endswith("_passed") and value is not True
    ]
    if failed_checks:
        raise ValueError(f"Optimizer validation failed: {', '.join(failed_checks)}")
    if root.name != f"run_id={manifest['run_id']}":
        raise ValueError("Run directory and manifest run_id do not match")

    parquet_paths, artifacts = _verify_artifacts(root, manifest)
    capacity = pd.read_parquet(parquet_paths["capacity"])
    dispatch = pd.read_parquet(parquet_paths["dispatch"])
    storage = pd.read_parquet(parquet_paths["storage"])
    annual_summary = pd.read_parquet(parquet_paths["annual_summary"])
    hourly_balance = pd.read_parquet(parquet_paths["hourly_balance"])
    frames = {
        "capacity": capacity,
        "dispatch": dispatch,
        "storage": storage,
        "annual_summary": annual_summary,
        "hourly_balance": hourly_balance,
    }
    for name, frame in frames.items():
        expected_rows = int(manifest["outputs"][name]["rows"])
        if len(frame) != expected_rows:
            raise ValueError(f"{name} row count mismatch: {len(frame)} != {expected_rows}")

    _require_columns(
        capacity,
        "capacity",
        {
            "component",
            "asset",
            "carrier",
            "asset_role",
            "existing_capacity_mw",
            "optimized_capacity_mw",
            "new_capacity_mw",
            "capacity_limit_mw",
            "energy_capacity_mwh",
        },
    )
    _require_columns(dispatch, "dispatch", {"timestamp_utc", "asset", "carrier", "dispatch_mw"})
    _require_columns(
        storage,
        "storage",
        {"timestamp_utc", "asset", "carrier", "net_dispatch_mw", "state_of_charge_mwh"},
    )
    _require_columns(
        annual_summary,
        "annual_summary",
        {"asset", "carrier", "capital_cost_eur", "operating_cost_eur", "emissions_tco2"},
    )
    _require_columns(
        hourly_balance,
        "hourly_balance",
        {"timestamp_utc", "demand_mw", "balance_error_mw"},
    )
    if capacity.duplicated(["component", "asset"]).any():
        raise ValueError("capacity contains duplicate component/asset keys")
    if dispatch.duplicated(["timestamp_utc", "asset"]).any():
        raise ValueError("dispatch contains duplicate timestamp/asset keys")
    if storage.duplicated(["timestamp_utc", "asset"]).any():
        raise ValueError("storage contains duplicate timestamp/asset keys")
    snapshot_count = int(manifest["kpis"]["snapshot_count"])
    if hourly_balance["timestamp_utc"].nunique() != snapshot_count:
        raise ValueError("hourly_balance does not contain the declared snapshot count")
    max_balance_error = float(hourly_balance["balance_error_mw"].abs().max())
    if max_balance_error > 1e-4:
        raise ValueError(f"Hourly balance validation failed: {max_balance_error} MW")

    kpis = manifest["kpis"]
    capital_cost = float(annual_summary["capital_cost_eur"].sum())
    operating_cost = float(annual_summary["operating_cost_eur"].sum())
    _assert_close(capital_cost, float(kpis["annualized_capital_cost_eur"]), "capital cost")
    _assert_close(operating_cost, float(kpis["annualized_operating_cost_eur"]), "operating cost")
    _assert_close(capital_cost + operating_cost, float(kpis["objective_total_eur"]), "objective")
    _assert_close(
        float(annual_summary["emissions_tco2"].sum()),
        float(kpis["emissions_tco2"]),
        "emissions",
    )

    scenario_parent = root.parent.name
    if scenario_parent.startswith("scenario_id="):
        scenario_id = scenario_parent.removeprefix("scenario_id=")
    else:
        encoded = json.dumps(manifest.get("scenario"), sort_keys=True).encode()
        scenario_id = hashlib.sha256(encoded).hexdigest()[:16]
    return ValidatedRun(
        root=root,
        manifest=manifest,
        manifest_sha256=manifest_sha256,
        scenario_id=scenario_id,
        capacity=capacity,
        dispatch=dispatch,
        storage=storage,
        annual_summary=annual_summary,
        hourly_balance=hourly_balance,
        artifacts=artifacts,
    )


def _records(frame: Any, *, run_id: str) -> list[dict[str, Any]]:
    import pandas as pd

    normalized = frame.astype(object).where(pd.notna(frame), None)
    rows = normalized.to_dict(orient="records")
    for row in rows:
        row["run_id"] = run_id
        for key, value in tuple(row.items()):
            if isinstance(value, pd.Timestamp):
                row[key] = value.to_pydatetime().replace(tzinfo=None)
    return rows


def _insert_rows(session: Session, model: type[Any], rows: list[dict[str, Any]]) -> None:
    for offset in range(0, len(rows), 5000):
        session.execute(insert(model), rows[offset : offset + 5000])


class PostgresResultPublisher:
    def __init__(self, session_factory: sessionmaker[Session]) -> None:
        self._session_factory = session_factory

    def publish(self, run_path: Path) -> PublicationResult:
        validated = validate_run_artifacts(run_path)
        manifest = validated.manifest
        run_id = str(manifest["run_id"])
        kpis = manifest["kpis"]
        scenario = manifest.get("scenario") or {}
        software = manifest["software_versions"]
        input_metadata = manifest["input_metadata"]

        capacity_rows = _records(validated.capacity, run_id=run_id)
        dispatch_rows = _records(validated.dispatch, run_id=run_id)
        storage_rows = _records(validated.storage, run_id=run_id)
        cost_rows: list[dict[str, Any]] = []
        for row in validated.annual_summary.itertuples(index=False):
            for component_type, amount in (
                ("capital", row.capital_cost_eur),
                ("operating", row.operating_cost_eur),
            ):
                cost_rows.append(
                    {
                        "run_id": run_id,
                        "asset": str(row.asset),
                        "carrier": str(row.carrier),
                        "component_type": component_type,
                        "amount_eur": float(amount),
                    }
                )
        artifact_rows = [dict(row, run_id=run_id) for row in validated.artifacts]
        provenance_rows = [
            {
                "run_id": run_id,
                "provenance_key": key,
                "provenance_value": value,
            }
            for key, value in {
                "input_metadata": input_metadata,
                "software_versions": software,
                "validation": manifest["validation"],
                "output_contract": manifest["outputs"],
            }.items()
        ]

        with self._session_factory.begin() as session:
            existing = session.get(OptimizationRunRow, run_id)
            if existing is not None:
                if existing.manifest_sha256 != validated.manifest_sha256:
                    raise ValueError(f"Run {run_id} is already published with a different manifest")
                return PublicationResult(
                    run_id=run_id,
                    reused=True,
                    capacity_rows=self._count(session, CapacityResultRow, run_id),
                    dispatch_rows=self._count(session, DispatchHourlyRow, run_id),
                    storage_rows=self._count(session, StorageHourlyRow, run_id),
                    cost_rows=self._count(session, CostComponentRow, run_id),
                )

            generated_at = datetime.fromisoformat(
                str(manifest["generated_at_utc"]).replace("Z", "+00:00")
            )
            session.add(
                OptimizationRunRow(
                    run_id=run_id,
                    scenario_id=validated.scenario_id,
                    scenario_name=scenario.get("name"),
                    status=manifest["solver_status"],
                    termination_condition=manifest["termination_condition"],
                    planning_year=scenario.get("planning_year"),
                    weather_year=scenario.get("weather_year"),
                    snapshot_count=int(kpis["snapshot_count"]),
                    annualization_factor=float(kpis["annualization_factor"]),
                    objective_total_eur=float(kpis["objective_total_eur"]),
                    annualized_capital_cost_eur=float(kpis["annualized_capital_cost_eur"]),
                    annualized_operating_cost_eur=float(kpis["annualized_operating_cost_eur"]),
                    demand_mwh=float(kpis["demand_mwh"]),
                    renewable_generation_mwh=float(kpis["renewable_generation_mwh"]),
                    renewable_generation_share=float(kpis["renewable_generation_share"]),
                    emissions_tco2=float(kpis["emissions_tco2"]),
                    curtailment_mwh=float(kpis["curtailment_mwh"]),
                    unmet_demand_mwh=float(kpis["unmet_demand_mwh"]),
                    unmet_demand_share=float(kpis["unmet_demand_share"]),
                    solver_name=manifest["solver"],
                    solver_version=str(software["highspy"]),
                    model_version=manifest["model_version"],
                    engine_version=manifest["engine_version"],
                    config_version=manifest["config_version"],
                    input_package_version=input_metadata.get("package_version"),
                    run_mode=str(input_metadata.get("mode", "unknown")),
                    calendar_mode=str(input_metadata.get("calendar_mode", "unknown")),
                    publication_state="published",
                    is_current=True,
                    solve_duration_seconds=float(manifest["solve_duration_seconds"]),
                    artifact_root=str(validated.root),
                    manifest_sha256=validated.manifest_sha256,
                    generated_at=generated_at,
                )
            )
            session.add(
                ScenarioInputRow(
                    run_id=run_id,
                    demand_growth=scenario.get("demand_growth"),
                    gas_price_eur_mwh_th=scenario.get("gas_price_eur_mwh_th"),
                    carbon_price_eur_t=scenario.get("carbon_price_eur_t"),
                    renewable_generation_min=scenario.get("renewable_generation_min"),
                    battery_capex_multiplier=scenario.get("battery_capex_multiplier"),
                    solar_capex_multiplier=scenario.get("solar_capex_multiplier"),
                    wind_capex_multiplier=scenario.get("wind_capex_multiplier"),
                    input_payload=scenario,
                )
            )
            session.flush()
            session.execute(
                update(OptimizationRunRow)
                .where(
                    OptimizationRunRow.scenario_id == validated.scenario_id,
                    OptimizationRunRow.run_mode
                    == str(input_metadata.get("mode", "unknown")),
                    OptimizationRunRow.calendar_mode
                    == str(input_metadata.get("calendar_mode", "unknown")),
                    OptimizationRunRow.run_id != run_id,
                    OptimizationRunRow.is_current.is_(True),
                )
                .values(
                    publication_state="superseded",
                    is_current=False,
                    superseded_by_run_id=run_id,
                )
            )
            _insert_rows(session, RunArtifactRow, artifact_rows)
            _insert_rows(session, CapacityResultRow, capacity_rows)
            _insert_rows(session, DispatchHourlyRow, dispatch_rows)
            _insert_rows(session, StorageHourlyRow, storage_rows)
            _insert_rows(session, CostComponentRow, cost_rows)
            _insert_rows(session, RunProvenanceRow, provenance_rows)

        return PublicationResult(
            run_id=run_id,
            reused=False,
            capacity_rows=len(capacity_rows),
            dispatch_rows=len(dispatch_rows),
            storage_rows=len(storage_rows),
            cost_rows=len(cost_rows),
        )

    @staticmethod
    def _count(session: Session, model: type[Any], run_id: str) -> int:
        return int(
            session.scalar(select(func.count()).select_from(model).where(model.run_id == run_id))
            or 0
        )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Validate and atomically publish one optimizer run to PostgreSQL"
    )
    parser.add_argument("--run-path", type=Path, required=True)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    settings = get_settings()
    logging.basicConfig(
        level=settings.log_level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    engine = build_engine(settings.database_url)
    try:
        result = PostgresResultPublisher(build_session_factory(engine)).publish(args.run_path)
    finally:
        engine.dispose()
    LOGGER.info(
        "Result publication complete: run_id=%s reused=%s capacity=%d "
        "dispatch=%d storage=%d costs=%d",
        result.run_id,
        result.reused,
        result.capacity_rows,
        result.dispatch_rows,
        result.storage_rows,
        result.cost_rows,
    )


if __name__ == "__main__":
    main()
