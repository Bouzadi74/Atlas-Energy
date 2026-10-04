import logging
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Protocol

from atlas.optimization.engine import (
    CalendarMode,
    find_latest_package,
    load_optimization_config,
    run_optimization,
)

if TYPE_CHECKING:
    from atlas.workers.scenario_worker import ScenarioRequestedPayload

LOGGER = logging.getLogger(__name__)


class ResultPublisher(Protocol):
    def publish(self, run_path: Path) -> object: ...


class AnalyticsRefresher(Protocol):
    def refresh(self) -> None: ...


class JobLifecycle(Protocol):
    def mark_optimized(self, job_id: str) -> None: ...

    def mark_publishing(self, job_id: str) -> None: ...

    def mark_published(self, job_id: str, optimizer_run_id: str) -> None: ...

    def mark_building_analytics(self, job_id: str) -> None: ...

    def mark_analytics_ready(self, job_id: str) -> None: ...


class DbtAnalyticsRefresher:
    """Refresh PostgreSQL analytical marts after a result is safely published."""

    def __init__(
        self,
        *,
        project_dir: Path,
        profiles_dir: Path,
        timeout_seconds: int = 600,
        enabled: bool = True,
    ) -> None:
        self._project_dir = project_dir
        self._profiles_dir = profiles_dir
        self._timeout_seconds = timeout_seconds
        self._enabled = enabled

    def refresh(self) -> None:
        if not self._enabled:
            LOGGER.info("dbt refresh disabled for scenario worker")
            return
        sibling = Path(sys.executable).with_name("dbt.exe" if os.name == "nt" else "dbt")
        executable = str(sibling) if sibling.is_file() else shutil.which("dbt")
        if executable is None:
            raise RuntimeError(
                "dbt executable not found; install the analytics extra or set "
                "ATLAS_DBT_REFRESH_ENABLED=false"
            )
        completed = subprocess.run(
            [
                executable,
                "build",
                "--project-dir",
                str(self._project_dir),
                "--profiles-dir",
                str(self._profiles_dir),
            ],
            capture_output=True,
            check=False,
            text=True,
            timeout=self._timeout_seconds,
        )
        if completed.returncode != 0:
            detail = (completed.stderr or completed.stdout).strip()
            raise RuntimeError(f"dbt build failed: {detail[-2_000:]}")
        LOGGER.info("dbt analytical marts refreshed")


class OptimizationScenarioProcessor:
    """Run, validate, publish, and expose one requested optimization scenario."""

    def __init__(
        self,
        *,
        data_root: Path,
        config_path: Path,
        calendar_mode: CalendarMode,
        publisher: ResultPublisher,
        analytics: AnalyticsRefresher,
        lifecycle: JobLifecycle,
    ) -> None:
        self._data_root = data_root
        self._config = load_optimization_config(config_path)
        self._calendar_mode = calendar_mode
        self._publisher = publisher
        self._analytics = analytics
        self._lifecycle = lifecycle

    def process(self, payload: "ScenarioRequestedPayload", job_id: str) -> str:
        package_path = find_latest_package(self._data_root)
        package_version = self._package_version(package_path)
        if payload.model_version != self._config.model_version:
            raise ValueError(
                f"Requested model version {payload.model_version!r} does not match "
                f"worker version {self._config.model_version!r}"
            )
        if payload.data_version != package_version:
            raise ValueError(
                f"Requested data version {payload.data_version!r} does not match "
                f"optimizer package {package_version!r}"
            )

        LOGGER.info(
            "Executing scenario job %s with model=%s data=%s calendar=%s",
            job_id,
            payload.model_version,
            payload.data_version,
            self._calendar_mode,
        )
        result = run_optimization(
            config=self._config,
            mode="full",
            output_root=self._data_root / "optimization",
            package_path=package_path,
            scenario=payload.request,
            calendar_mode=self._calendar_mode,
        )
        self._lifecycle.mark_optimized(job_id)
        self._lifecycle.mark_publishing(job_id)
        publication = self._publisher.publish(result.output_path)
        published_run_id = getattr(publication, "run_id", None)
        if published_run_id != result.run_id:
            raise RuntimeError("Published result identifier does not match optimizer output")
        self._lifecycle.mark_published(job_id, result.run_id)
        self._lifecycle.mark_building_analytics(job_id)
        self._analytics.refresh()
        self._lifecycle.mark_analytics_ready(job_id)
        return result.run_id

    @staticmethod
    def _package_version(package_path: Path) -> str:
        import json

        manifest_path = package_path / "manifest.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if manifest.get("optimizer_ready") is not True:
            raise ValueError(f"Model-input package is not optimizer-ready: {package_path}")
        version = manifest.get("package_version")
        if not isinstance(version, str) or not version:
            raise ValueError(f"Model-input package has no version: {package_path}")
        return version
