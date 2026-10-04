"""Verify a completed scenario through the same server-side proxy used by the UI.

This command is read-only. Submit a distinct scenario through the UI or API first,
then pass its job ID. It waits for the worker and checks the published serving contract.
"""

from __future__ import annotations

import argparse
import json
import math
import re
import sys
import time
from collections.abc import Callable
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


def fetch_json(base_url: str, path: str) -> Any:
    request = Request(
        f"{base_url.rstrip('/')}/{path.lstrip('/')}", headers={"Accept": "application/json"}
    )
    try:
        with urlopen(request, timeout=30) as response:  # noqa: S310 - operator-supplied URL
            return json.load(response)
    except (HTTPError, URLError) as error:
        raise RuntimeError(f"GET {path} failed: {error}") from error


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def wait_for_job(
    fetch: Callable[[str], Any], job_id: str, timeout_seconds: int, poll_seconds: int
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_seconds
    last_stage = None
    while True:
        job = fetch(f"v1/jobs/{job_id}")
        require(isinstance(job, dict), "Job response must be an object")
        stage = job.get("stage")
        if stage != last_stage:
            print(f"job={job_id} status={job.get('status')} stage={stage}", flush=True)
            last_stage = stage
        if job.get("status") == "completed":
            require(stage == "completed", "Completed job has inconsistent stage")
            return job
        if job.get("status") == "failed":
            raise RuntimeError(
                f"Job failed at {job.get('failure_stage')}: "
                f"{job.get('error_type')}: {job.get('error_message')}"
            )
        if time.monotonic() >= deadline:
            raise TimeoutError(f"Job {job_id} did not complete within {timeout_seconds}s")
        time.sleep(poll_seconds)


def verify_results(fetch: Callable[[str], Any], job: dict[str, Any]) -> dict[str, Any]:
    run_id = job.get("optimizer_run_id")
    scenario_id = job.get("scenario_id")
    require(bool(run_id) and bool(scenario_id), "Completed job has no result link")

    scenario = fetch(f"v1/runs/{scenario_id}")
    require(scenario.get("status") == "completed", "Scenario record is not completed")
    require(scenario.get("optimizer_run_id") == run_id, "Scenario result link disagrees with job")

    kpi = fetch(f"v1/results/{run_id}/kpis")
    capacity = fetch(f"v1/results/{run_id}/capacity")
    dispatch = fetch(f"v1/results/{run_id}/dispatch?limit=24")
    storage = fetch(f"v1/results/{run_id}/storage?limit=24")
    costs = fetch(f"v1/results/{run_id}/costs")
    provenance = fetch(f"v1/results/{run_id}/provenance")
    comparisons = fetch("v1/comparisons")
    scenarios = fetch("v1/scenarios")

    require(kpi.get("run_id") == run_id, "KPI run ID mismatch")
    require(kpi.get("scenario_id") == scenario_id, "KPI scenario ID mismatch")
    annual_cost = kpi.get("annual_system_cost_eur")
    require(
        isinstance(annual_cost, (int, float)) and math.isfinite(annual_cost),
        "Invalid annual cost",
    )
    require(annual_cost > 0, "Annual cost must be positive")
    require(kpi.get("demand_mwh", 0) > 0, "Demand must be positive")
    require(0 <= kpi.get("renewable_generation_share", -1) <= 1, "Invalid renewable share")
    require(kpi.get("unserved_mwh", -1) >= 0, "Invalid unserved energy")
    require(isinstance(capacity, list) and capacity, "Capacity results are empty")
    require(isinstance(costs, list) and costs, "Cost results are empty")
    require(isinstance(provenance, list) and provenance, "Provenance is empty")
    require(dispatch.get("returned", 0) > 0, "Dispatch page is empty")
    require(dispatch.get("returned") <= 24, "Dispatch page exceeded its limit")
    require(storage.get("returned", 0) > 0, "Storage page is empty")
    require(storage.get("returned") <= 24, "Storage page exceeded its limit")
    require(dispatch.get("returned") == len(dispatch.get("items", [])), "Dispatch count mismatch")
    require(storage.get("returned") == len(storage.get("items", [])), "Storage count mismatch")
    for label, rows in (("capacity", capacity), ("cost", costs), ("provenance", provenance)):
        require(all(row.get("run_id") == run_id for row in rows), f"{label} run ID mismatch")
    cost_sum = sum(row["amount_eur"] for row in costs)
    require(
        math.isclose(cost_sum, annual_cost, rel_tol=1e-12, abs_tol=0.01),
        f"Cost components ({cost_sum}) do not reconcile with KPI ({annual_cost})",
    )
    completed_count = sum(row.get("status") == "completed" for row in scenarios)
    if completed_count >= 2:
        require(
            any(
                run_id in (row.get("base_run_id"), row.get("comparison_run_id"))
                for row in comparisons
            ),
            "No comparison includes the new run",
        )

    return {
        "job_id": job["job_id"],
        "scenario_id": scenario_id,
        "run_id": run_id,
        "status": "ready",
        "annual_system_cost_eur": annual_cost,
        "capacity_rows": len(capacity),
        "dispatch_sample_rows": dispatch["returned"],
        "storage_sample_rows": storage["returned"],
        "cost_rows": len(costs),
        "provenance_rows": len(provenance),
        "comparison_rows": len(comparisons),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--job-id", required=True, help="Job ID returned by POST /v1/scenarios")
    parser.add_argument("--base-url", default="http://localhost:3000/api/atlas")
    parser.add_argument("--timeout-seconds", type=int, default=1800)
    parser.add_argument("--poll-seconds", type=int, default=10)
    args = parser.parse_args()
    if not re.fullmatch(r"job_[a-z0-9]+", args.job_id):
        parser.error("--job-id must be a job_ identifier")
    if args.timeout_seconds <= 0 or args.poll_seconds <= 0:
        parser.error("Timeout and polling interval must be positive")

    try:
        health = fetch_json(args.base_url, "health")
        readiness = fetch_json(args.base_url, "ready")
        require(health.get("status") == "ok", "API health check failed")
        require(readiness.get("status") == "ready", "Database readiness check failed")
        def fetch(path: str) -> Any:
            return fetch_json(args.base_url, path)

        job = wait_for_job(fetch, args.job_id, args.timeout_seconds, args.poll_seconds)
        print(json.dumps(verify_results(fetch, job), indent=2))
        return 0
    except (RuntimeError, ValueError, TimeoutError, KeyError, TypeError) as error:
        print(f"Acceptance check failed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
