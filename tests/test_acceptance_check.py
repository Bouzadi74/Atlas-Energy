from __future__ import annotations

import pytest

from scripts.acceptance_check import verify_results, wait_for_job


def test_wait_for_completed_job() -> None:
    job = {"status": "completed", "stage": "completed"}
    assert wait_for_job(lambda _: job, "job_abc", 1, 1) is job


def test_wait_for_failed_job_reports_stage() -> None:
    job = {
        "status": "failed",
        "stage": "failed",
        "failure_stage": "building_analytics",
        "error_type": "DbrtError",
        "error_message": "model failed",
    }
    with pytest.raises(RuntimeError, match="building_analytics"):
        wait_for_job(lambda _: job, "job_abc", 1, 1)


def test_verify_results_reconciles_costs_and_comparison() -> None:
    job = {
        "job_id": "job_abc",
        "scenario_id": "scn_abc",
        "optimizer_run_id": "run_abc",
    }
    responses = {
        "v1/runs/scn_abc": {
            "status": "completed",
            "optimizer_run_id": "run_abc",
        },
        "v1/results/run_abc/kpis": {
            "run_id": "run_abc",
            "scenario_id": "scn_abc",
            "annual_system_cost_eur": 100.0,
            "demand_mwh": 10.0,
            "renewable_generation_share": 0.5,
            "unserved_mwh": 0.0,
        },
        "v1/results/run_abc/capacity": [{"run_id": "run_abc"}],
        "v1/results/run_abc/dispatch?limit=24": {"returned": 1, "items": [{}]},
        "v1/results/run_abc/storage?limit=24": {"returned": 1, "items": [{}]},
        "v1/results/run_abc/costs": [
            {"run_id": "run_abc", "amount_eur": 60.0},
            {"run_id": "run_abc", "amount_eur": 40.0},
        ],
        "v1/results/run_abc/provenance": [{"run_id": "run_abc"}],
        "v1/comparisons": [{"base_run_id": "run_abc", "comparison_run_id": "run_other"}],
        "v1/scenarios": [{"status": "completed"}, {"status": "completed"}],
    }
    result = verify_results(responses.__getitem__, job)
    assert result["status"] == "ready"
    assert result["cost_rows"] == 2

    responses["v1/results/run_abc/costs"][0]["amount_eur"] = 59.0
    with pytest.raises(ValueError, match="do not reconcile"):
        verify_results(responses.__getitem__, job)
