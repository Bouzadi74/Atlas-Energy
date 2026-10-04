from datetime import datetime

import pytest
from fastapi.testclient import TestClient

from atlas.api.main import app
from atlas.schemas.results import (
    CapacityResult,
    CostResult,
    DispatchPoint,
    ProvenanceRecord,
    ScenarioComparison,
    ScenarioKpi,
    StoragePoint,
)


def kpis(run_id: str = "run_123") -> ScenarioKpi:
    return ScenarioKpi(
        run_id=run_id,
        scenario_id="scenario_123",
        scenario_name="Baseline 2030",
        planning_year=2030,
        weather_year=2024,
        annual_system_cost_eur=100.0,
        average_cost_eur_mwh=10.0,
        annualized_capital_cost_eur=40.0,
        annualized_operating_cost_eur=60.0,
        demand_mwh=10.0,
        renewable_generation_mwh=7.0,
        renewable_generation_share=0.7,
        imports_mwh=1.0,
        import_share=0.1,
        reservoir_hydro_mwh=1.0,
        battery_discharge_mwh=0.5,
        pumped_hydro_discharge_mwh=0.2,
        emissions_tco2=2.0,
        curtailment_mwh=0.1,
        unserved_mwh=0.0,
        unserved_energy_share=0.0,
        balanced_demand_mwh=10.0,
        solve_duration_seconds=1.5,
        model_version="model-v1",
        engine_version="engine-v1",
        input_package_version="data-v1",
    )


class FakeResultRepository:
    def __init__(self) -> None:
        self.dispatch_arguments: dict[str, object] = {}

    def get_kpis(self, run_id: str) -> ScenarioKpi | None:
        return None if run_id == "missing" else kpis(run_id)

    def list_capacity(self, run_id: str) -> list[CapacityResult] | None:
        if run_id == "missing":
            return None
        return [
            CapacityResult(
                run_id=run_id,
                scenario_id="scenario_123",
                scenario_name="Baseline 2030",
                component="Generator",
                asset="solar_new",
                carrier="solar",
                asset_role="generation",
                existing_capacity_mw=0,
                new_capacity_mw=100,
                optimized_capacity_mw=100,
                energy_capacity_mwh=None,
            )
        ]

    def list_dispatch(self, run_id: str, **kwargs: object):  # type: ignore[no-untyped-def]
        if run_id == "missing":
            return None
        self.dispatch_arguments = kwargs
        return (
            [
                DispatchPoint(
                    run_id=run_id,
                    scenario_id="scenario_123",
                    timestamp_utc=datetime(2030, 1, 1),
                    asset="solar_new",
                    carrier="solar",
                    dispatch_mw=80,
                )
            ],
            True,
        )

    def list_storage(self, run_id: str, **_kwargs: object):  # type: ignore[no-untyped-def]
        if run_id == "missing":
            return None
        return (
            [
                StoragePoint(
                    run_id=run_id,
                    scenario_id="scenario_123",
                    timestamp_utc=datetime(2030, 1, 1),
                    asset="battery_new",
                    carrier="battery",
                    net_dispatch_mw=20,
                    state_of_charge_mwh=40,
                )
            ],
            False,
        )

    def list_costs(self, run_id: str) -> list[CostResult] | None:
        if run_id == "missing":
            return None
        return [
            CostResult(
                run_id=run_id,
                scenario_id="scenario_123",
                scenario_name="Baseline 2030",
                asset="solar_new",
                carrier="solar",
                component_type="capital",
                amount_eur=40,
            )
        ]

    def list_provenance(self, run_id: str) -> list[ProvenanceRecord] | None:
        if run_id == "missing":
            return None
        return [
            ProvenanceRecord(
                run_id=run_id,
                scenario_id="scenario_123",
                scenario_name="Baseline 2030",
                provenance_key="validation",
                provenance_value={"energy_balance_passed": True},
            )
        ]

    def list_comparisons(self) -> list[ScenarioComparison]:
        return [self._comparison()]

    def get_comparison(
        self, base_run_id: str, comparison_run_id: str
    ) -> ScenarioComparison | None:
        if base_run_id == "missing":
            return None
        return self._comparison(base_run_id, comparison_run_id)

    @staticmethod
    def _comparison(
        base_run_id: str = "run_123",
        comparison_run_id: str = "run_456",
    ) -> ScenarioComparison:
        return ScenarioComparison(
            base_run_id=base_run_id,
            comparison_run_id=comparison_run_id,
            base_scenario_name="Baseline 2030",
            comparison_scenario_name="High Renewables 2035",
            annual_cost_delta_eur=20,
            average_cost_delta_eur_mwh=-2,
            renewable_share_delta=0.1,
            emissions_delta_tco2=-1,
            curtailment_delta_mwh=3,
            unserved_delta_mwh=0,
            imports_delta_mwh=-4,
        )


@pytest.fixture
def client_and_repository() -> tuple[TestClient, FakeResultRepository]:
    repository = FakeResultRepository()
    with TestClient(app) as client:
        client.app.state.results = repository
        yield client, repository


def test_kpi_capacity_cost_and_provenance_endpoints(
    client_and_repository: tuple[TestClient, FakeResultRepository],
) -> None:
    client, _repository = client_and_repository

    assert client.get("/v1/results/run_123/kpis").json()["demand_mwh"] == 10.0
    assert client.get("/v1/results/run_123/capacity").json()[0]["asset"] == "solar_new"
    assert client.get("/v1/results/run_123/costs").json()[0]["amount_eur"] == 40.0
    provenance = client.get("/v1/results/run_123/provenance").json()
    assert provenance[0]["provenance_value"]["energy_balance_passed"] is True


def test_dispatch_is_filtered_bounded_and_paginated(
    client_and_repository: tuple[TestClient, FakeResultRepository],
) -> None:
    client, repository = client_and_repository
    response = client.get(
        "/v1/results/run_123/dispatch",
        params={
            "start": "2030-01-01T00:00:00Z",
            "end": "2030-01-02T00:00:00Z",
            "carrier": "solar",
            "limit": 25,
            "offset": 10,
        },
    )

    assert response.status_code == 200
    assert response.json()["returned"] == 1
    assert response.json()["has_more"] is True
    assert repository.dispatch_arguments["carrier"] == "solar"
    assert repository.dispatch_arguments["limit"] == 25


def test_storage_and_comparison_endpoints(
    client_and_repository: tuple[TestClient, FakeResultRepository],
) -> None:
    client, _repository = client_and_repository

    storage = client.get("/v1/results/run_123/storage").json()
    assert storage["items"][0]["state_of_charge_mwh"] == 40.0
    comparisons = client.get("/v1/comparisons").json()
    assert comparisons[0]["renewable_share_delta"] == pytest.approx(0.1)
    comparison = client.get("/v1/comparisons/run_123/run_456").json()
    assert comparison["comparison_run_id"] == "run_456"


def test_result_endpoints_report_not_found_and_validate_ranges(
    client_and_repository: tuple[TestClient, FakeResultRepository],
) -> None:
    client, _repository = client_and_repository

    assert client.get("/v1/results/missing/kpis").status_code == 404
    assert client.get("/v1/results/missing/capacity").status_code == 404
    invalid = client.get(
        "/v1/results/run_123/dispatch",
        params={"start": "2030-01-02T00:00:00Z", "end": "2030-01-01T00:00:00Z"},
    )
    assert invalid.status_code == 422
    assert client.get("/v1/results/run_123/dispatch?limit=5001").status_code == 422
