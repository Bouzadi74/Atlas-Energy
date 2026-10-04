import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from atlas.workers.optimization_processor import (
    DbtAnalyticsRefresher,
    OptimizationScenarioProcessor,
)
from atlas.workers.scenario_worker import ScenarioRequestedPayload


class RecordingPublisher:
    def __init__(self) -> None:
        self.paths: list[Path] = []

    def publish(self, run_path: Path) -> object:
        self.paths.append(run_path)
        return SimpleNamespace(run_id="optimizer-run-123")


class RecordingAnalytics:
    def __init__(self) -> None:
        self.refreshes = 0

    def refresh(self) -> None:
        self.refreshes += 1


class RecordingLifecycle:
    def __init__(self) -> None:
        self.events: list[tuple[str, ...]] = []

    def mark_optimized(self, job_id: str) -> None:
        self.events.append(("optimized", job_id))

    def mark_publishing(self, job_id: str) -> None:
        self.events.append(("publishing", job_id))

    def mark_published(self, job_id: str, optimizer_run_id: str) -> None:
        self.events.append(("published", job_id, optimizer_run_id))

    def mark_building_analytics(self, job_id: str) -> None:
        self.events.append(("building_analytics", job_id))

    def mark_analytics_ready(self, job_id: str) -> None:
        self.events.append(("analytics_ready", job_id))


class FailingPublisher:
    def publish(self, _run_path: Path) -> object:
        raise RuntimeError("publication unavailable")


class FailingAnalytics:
    def refresh(self) -> None:
        raise RuntimeError("dbt unavailable")


def payload(*, model_version: str, data_version: str) -> ScenarioRequestedPayload:
    return ScenarioRequestedPayload.model_validate(
        {
            "scenario_id": "scn_123",
            "request_hash": "a" * 64,
            "model_version": model_version,
            "data_version": data_version,
            "request": {"name": "Processor test"},
        }
    )


def make_package(data_root: Path, version: str = "data-v1") -> Path:
    package = data_root / "model_inputs" / f"version={version}"
    package.mkdir(parents=True)
    (package / "manifest.json").write_text(
        json.dumps({"optimizer_ready": True, "package_version": version}),
        encoding="utf-8",
    )
    return package


def test_processor_runs_publishes_and_refreshes_analytics(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    package = make_package(tmp_path)
    output_path = tmp_path / "optimization" / "run_id=optimizer-run-123"
    calls: list[dict[str, object]] = []

    def fake_run_optimization(**kwargs: object) -> object:
        calls.append(kwargs)
        return SimpleNamespace(run_id="optimizer-run-123", output_path=output_path)

    monkeypatch.setattr(
        "atlas.workers.optimization_processor.run_optimization",
        fake_run_optimization,
    )
    publisher = RecordingPublisher()
    analytics = RecordingAnalytics()
    lifecycle = RecordingLifecycle()
    processor = OptimizationScenarioProcessor(
        data_root=tmp_path,
        config_path=Path("configs/optimization/national_baseline.yml"),
        calendar_mode="planning",
        publisher=publisher,
        analytics=analytics,
        lifecycle=lifecycle,
    )

    run_id = processor.process(
        payload(model_version="pypsa-national-node-v1", data_version="data-v1"),
        "job_123",
    )

    assert run_id == "optimizer-run-123"
    assert calls[0]["package_path"] == package
    assert calls[0]["mode"] == "full"
    assert publisher.paths == [output_path]
    assert analytics.refreshes == 1
    assert lifecycle.events == [
        ("optimized", "job_123"),
        ("publishing", "job_123"),
        ("published", "job_123", "optimizer-run-123"),
        ("building_analytics", "job_123"),
        ("analytics_ready", "job_123"),
    ]


def test_processor_rejects_stale_data_contract_before_solving(tmp_path: Path) -> None:
    make_package(tmp_path)
    processor = OptimizationScenarioProcessor(
        data_root=tmp_path,
        config_path=Path("configs/optimization/national_baseline.yml"),
        calendar_mode="planning",
        publisher=RecordingPublisher(),
        analytics=RecordingAnalytics(),
        lifecycle=RecordingLifecycle(),
    )

    with pytest.raises(ValueError, match="does not match optimizer package"):
        processor.process(
            payload(model_version="pypsa-national-node-v1", data_version="stale-data"),
            "job_123",
        )


def test_dbt_refresher_uses_its_python_environment_without_shell_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import atlas.workers.optimization_processor as module

    virtual_environment = tmp_path / "venv"
    virtual_environment.mkdir()
    python_path = virtual_environment / "python"
    dbt_path = virtual_environment / ("dbt.exe" if module.os.name == "nt" else "dbt")
    dbt_path.write_text("stub", encoding="utf-8")
    monkeypatch.setattr(module.sys, "executable", str(python_path))
    monkeypatch.setattr(module.shutil, "which", lambda _: None)
    calls: list[list[str]] = []

    def run(command: list[str], **_kwargs: object) -> object:
        calls.append(command)
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(module.subprocess, "run", run)
    DbtAnalyticsRefresher(project_dir=tmp_path, profiles_dir=tmp_path).refresh()
    assert calls[0][0] == str(dbt_path)


@pytest.mark.parametrize(
    ("publisher", "analytics", "expected_events", "error"),
    [
        (
            FailingPublisher(),
            RecordingAnalytics(),
            [("optimized", "job_123"), ("publishing", "job_123")],
            "publication unavailable",
        ),
        (
            RecordingPublisher(),
            FailingAnalytics(),
            [
                ("optimized", "job_123"),
                ("publishing", "job_123"),
                ("published", "job_123", "optimizer-run-123"),
                ("building_analytics", "job_123"),
            ],
            "dbt unavailable",
        ),
    ],
)
def test_processor_preserves_exact_failure_stage(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    publisher: object,
    analytics: object,
    expected_events: list[tuple[str, ...]],
    error: str,
) -> None:
    make_package(tmp_path)
    output_path = tmp_path / "optimization" / "run_id=optimizer-run-123"
    monkeypatch.setattr(
        "atlas.workers.optimization_processor.run_optimization",
        lambda **_kwargs: SimpleNamespace(
            run_id="optimizer-run-123",
            output_path=output_path,
        ),
    )
    lifecycle = RecordingLifecycle()
    processor = OptimizationScenarioProcessor(
        data_root=tmp_path,
        config_path=Path("configs/optimization/national_baseline.yml"),
        calendar_mode="planning",
        publisher=publisher,  # type: ignore[arg-type]
        analytics=analytics,  # type: ignore[arg-type]
        lifecycle=lifecycle,
    )

    with pytest.raises(RuntimeError, match=error):
        processor.process(
            payload(model_version="pypsa-national-node-v1", data_version="data-v1"),
            "job_123",
        )

    assert lifecycle.events == expected_events
