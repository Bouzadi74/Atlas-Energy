import pytest
from fastapi.testclient import TestClient

from atlas.api.main import app
from atlas.config import get_settings
from atlas.scenarios.service import InMemoryScenarioService


@pytest.fixture
def client() -> TestClient:
    settings = get_settings()
    with TestClient(app) as test_client:
        test_client.app.state.scenarios = InMemoryScenarioService(
            model_version=settings.model_version,
            data_version=settings.data_version,
        )
        yield test_client


def test_health(client: TestClient) -> None:
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"
    assert response.headers["x-request-id"]


def test_metrics_expose_request_rate_and_latency(client: TestClient) -> None:
    client.get("/health")
    response = client.get("/metrics")

    assert response.status_code == 200
    assert "atlas_api_http_requests_total" in response.text
    assert "atlas_api_http_request_duration_seconds" in response.text


def test_scenario_presets_are_exposed(client: TestClient) -> None:
    response = client.get("/v1/scenario-presets")

    assert response.status_code == 200
    presets = response.json()
    assert len(presets) >= 5
    assert {preset["preset_id"] for preset in presets} >= {
        "baseline-2030",
        "high-renewables-2035",
        "high-fuel-carbon-2030",
        "accelerated-demand-2035",
        "low-clean-tech-cost-2035",
    }


def test_local_frontend_origin_is_allowed(client: TestClient) -> None:
    response = client.options(
        "/v1/scenario-presets",
        headers={
            "Origin": "http://localhost:3000",
            "Access-Control-Request-Method": "GET",
        },
    )

    assert response.status_code == 200
    assert response.headers["access-control-allow-origin"] == "http://localhost:3000"


def test_scenario_submission_is_idempotent(client: TestClient) -> None:
    request = {
        "name": "High renewables 2035",
        "planning_year": 2035,
        "weather_year": 2024,
        "demand_growth": 0.04,
        "gas_price_eur_mwh_th": 65,
        "carbon_price_eur_t": 50,
        "renewable_generation_min": 0.75,
        "battery_capex_multiplier": 0.7,
        "solar_capex_multiplier": 0.85,
        "wind_capex_multiplier": 0.9,
    }
    first = client.post("/v1/scenarios", json=request)
    second = client.post("/v1/scenarios", json=request)

    assert first.status_code == 202
    assert first.json()["stage"] == "queued"
    assert first.json()["reused"] is False
    assert second.json()["scenario_id"] == first.json()["scenario_id"]
    assert second.json()["reused"] is True
    assert second.headers["X-Atlas-Idempotent-Replay"] == "true"
    run = client.get(f"/v1/runs/{first.json()['scenario_id']}")
    assert run.status_code == 200
    assert run.json()["job_id"] == first.json()["job_id"]
    assert run.json()["optimizer_run_id"] is None
    assert run.json()["processing_attempts"] == 0
    job = client.get(f"/v1/jobs/{first.json()['job_id']}")
    assert job.status_code == 200
    assert job.json()["scenario_id"] == first.json()["scenario_id"]
    assert job.json()["stage"] == "queued"
    assert client.get("/v1/jobs/missing").status_code == 404


def test_invalid_share_is_rejected(client: TestClient) -> None:
    response = client.post(
        "/v1/scenarios",
        json={"name": "Impossible target", "renewable_generation_min": 1.1},
    )
    assert response.status_code == 422
