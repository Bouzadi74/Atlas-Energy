import os
from pathlib import Path

import pytest
from sqlalchemy import delete, func, select

import atlas.optimization.publication as publication_module
from atlas.config import get_settings
from atlas.database import (
    PostgresScenarioRepository,
    PostgresScenarioRunRepository,
    build_engine,
    build_session_factory,
)
from atlas.database.models import (
    CapacityResultRow,
    CostComponentRow,
    DispatchHourlyRow,
    OptimizationRunRow,
    OutboxEventRow,
    ScenarioRequestRow,
    ScenarioRunRow,
    StorageHourlyRow,
)
from atlas.optimization.engine import load_optimization_config, run_optimization
from atlas.optimization.publication import PostgresResultPublisher
from atlas.scenarios.service import PostgresScenarioService
from atlas.schemas.scenario import ScenarioRequest

pytestmark = pytest.mark.skipif(
    os.getenv("ATLAS_RUN_INTEGRATION_TESTS") != "1",
    reason="Set ATLAS_RUN_INTEGRATION_TESTS=1 to test against local PostgreSQL",
)


def test_result_publication_is_atomic_idempotent_and_reconciled(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_optimization_config(Path("configs/optimization/national_baseline.yml"))
    optimization = run_optimization(config=config, mode="toy", output_root=tmp_path)
    settings = get_settings()
    engine = build_engine(settings.database_url)
    session_factory = build_session_factory(engine)
    publisher = PostgresResultPublisher(session_factory)
    original_insert = publication_module._insert_rows
    scenario = None

    def fail_during_dispatch(session, model, rows):  # type: ignore[no-untyped-def]
        if model is DispatchHourlyRow:
            raise RuntimeError("forced publication failure")
        original_insert(session, model, rows)

    try:
        monkeypatch.setattr(publication_module, "_insert_rows", fail_during_dispatch)
        with pytest.raises(RuntimeError, match="forced publication failure"):
            publisher.publish(optimization.output_path)
        with session_factory() as session:
            assert session.get(OptimizationRunRow, optimization.run_id) is None

        monkeypatch.setattr(publication_module, "_insert_rows", original_insert)
        first = publisher.publish(optimization.output_path)
        second = publisher.publish(optimization.output_path)

        assert first.reused is False
        assert second.reused is True
        assert first.capacity_rows == second.capacity_rows == 5
        assert first.dispatch_rows == second.dispatch_rows == 96
        assert first.storage_rows == second.storage_rows == 24
        assert first.cost_rows == second.cost_rows == 10

        with session_factory() as session:
            run = session.get(OptimizationRunRow, optimization.run_id)
            assert run is not None
            assert run.objective_total_eur == pytest.approx(
                optimization.kpis["objective_total_eur"]
            )
            assert run.unmet_demand_mwh == pytest.approx(0.0)
            assert session.scalar(
                select(func.count())
                .select_from(DispatchHourlyRow)
                .where(DispatchHourlyRow.run_id == optimization.run_id)
            ) == 96
            capital = session.scalar(
                select(func.sum(CostComponentRow.amount_eur)).where(
                    CostComponentRow.run_id == optimization.run_id,
                    CostComponentRow.component_type == "capital",
                )
            )
            operating = session.scalar(
                select(func.sum(CostComponentRow.amount_eur)).where(
                    CostComponentRow.run_id == optimization.run_id,
                    CostComponentRow.component_type == "operating",
                )
            )
            assert float(capital or 0) + float(operating or 0) == pytest.approx(
                run.objective_total_eur
            )
            assert session.scalar(
                select(func.count())
                .select_from(CapacityResultRow)
                .where(CapacityResultRow.run_id == optimization.run_id)
            ) == 5
            assert session.scalar(
                select(func.count())
                .select_from(StorageHourlyRow)
                .where(StorageHourlyRow.run_id == optimization.run_id)
            ) == 24

        scenario = PostgresScenarioService(
            PostgresScenarioRepository(session_factory),
            model_version="integration-model-v1",
            data_version="integration-data-v1",
        ).submit(ScenarioRequest(name="Worker result-link integration test"))
        runs = PostgresScenarioRunRepository(session_factory)
        claim = runs.claim(scenario.scenario_id)
        with pytest.raises(RuntimeError, match="Running scenario job"):
            runs.complete(claim.job_id, optimization.run_id)
        runs.mark_optimized(claim.job_id)
        runs.mark_publishing(claim.job_id)
        runs.mark_published(claim.job_id, optimization.run_id)
        runs.mark_building_analytics(claim.job_id)
        runs.mark_analytics_ready(claim.job_id)
        runs.complete(claim.job_id, optimization.run_id)

        with session_factory() as session:
            linked = session.get(ScenarioRunRow, scenario.job_id)
            assert linked is not None
            assert linked.status == "completed"
            assert linked.stage == "completed"
            assert linked.optimizer_run_id == optimization.run_id
            assert linked.optimized_at is not None
            assert linked.result_published_at is not None
            assert linked.analytics_ready_at is not None
            job = PostgresScenarioRepository(session_factory).get_job(scenario.job_id)
            assert job is not None
            assert job.stage == "completed"
            assert job.analytics_ready_at is not None
            completion = session.query(OutboxEventRow).filter_by(
                aggregate_id=scenario.scenario_id,
                event_type="scenario.completed",
            ).one()
            assert completion.topic == "atlas.scenario.completed.v1"
            assert completion.payload["payload"]["optimizer_run_id"] == optimization.run_id
    finally:
        monkeypatch.setattr(publication_module, "_insert_rows", original_insert)
        with session_factory.begin() as session:
            if scenario is not None:
                session.execute(
                    delete(OutboxEventRow).where(
                        OutboxEventRow.aggregate_id == scenario.scenario_id
                    )
                )
                session.execute(
                    delete(ScenarioRunRow).where(ScenarioRunRow.job_id == scenario.job_id)
                )
                session.execute(
                    delete(ScenarioRequestRow).where(
                        ScenarioRequestRow.scenario_id == scenario.scenario_id
                    )
                )
            session.execute(
                delete(OptimizationRunRow).where(
                    OptimizationRunRow.run_id == optimization.run_id
                )
            )
        engine.dispose()
