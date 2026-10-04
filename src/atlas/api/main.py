import json
import logging
import re
import time
import uuid
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Annotated

from fastapi import Depends, FastAPI, HTTPException, Query, Request, Response, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from sqlalchemy import text

from atlas import __version__
from atlas.api.observability import HTTP_DURATION, HTTP_IN_PROGRESS, HTTP_REQUESTS
from atlas.api.security import authorize_request
from atlas.config import Settings, get_settings
from atlas.database import (
    PostgresResultRepository,
    PostgresScenarioRepository,
    ResultRepository,
    build_engine,
    build_session_factory,
)
from atlas.scenarios.catalog import load_scenario_presets
from atlas.scenarios.service import PostgresScenarioService, ScenarioService
from atlas.schemas.results import (
    CapacityResult,
    CostResult,
    DispatchPage,
    ProvenanceRecord,
    ScenarioComparison,
    ScenarioKpi,
    StoragePage,
)
from atlas.schemas.scenario import (
    JobRecord,
    ScenarioAccepted,
    ScenarioPreset,
    ScenarioRecord,
    ScenarioRequest,
)

LOGGER = logging.getLogger(__name__)
REQUEST_ID_PATTERN = re.compile(r"^[A-Za-z0-9._:-]{8,128}$")


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    engine = build_engine(settings.database_url)
    session_factory = build_session_factory(engine)
    repository = PostgresScenarioRepository(session_factory)
    app.state.engine = engine
    app.state.scenarios = PostgresScenarioService(
        repository,
        model_version=settings.model_version,
        data_version=settings.data_version,
    )
    app.state.results = PostgresResultRepository(session_factory)
    try:
        yield
    finally:
        engine.dispose()


app = FastAPI(
    title="Atlas Energy API",
    version=__version__,
    description="Scenario and result boundary for the Atlas Energy platform.",
    lifespan=lifespan,
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[get_settings().frontend_origin],
    allow_credentials=False,
    allow_methods=["GET", "POST"],
    allow_headers=["Content-Type"],
)


@app.middleware("http")
async def secure_and_observe(request: Request, call_next):
    started = time.perf_counter()
    method = request.method.upper()
    request_id_header = request.headers.get("x-request-id", "")
    request_id = (
        request_id_header
        if REQUEST_ID_PATTERN.fullmatch(request_id_header)
        else str(uuid.uuid4())
    )
    HTTP_IN_PROGRESS.labels(method=method).inc()
    status_code = 500
    # Raw paths can contain run identifiers or arbitrary input, so only resolved route
    # templates are allowed into Prometheus labels.
    route_label = "unmatched"
    try:
        failure = authorize_request(
            get_settings(),
            method=method,
            path=request.url.path,
            headers=request.headers,
        )
        if failure is not None:
            response = JSONResponse(
                status_code=failure.status_code,
                content={"detail": failure.detail},
            )
            if failure.status_code == 401:
                response.headers["WWW-Authenticate"] = "Bearer"
        else:
            response = await call_next(request)
            route = request.scope.get("route")
            route_label = getattr(route, "path", request.url.path)
        status_code = response.status_code
        response.headers["X-Request-ID"] = request_id
        return response
    finally:
        duration = time.perf_counter() - started
        HTTP_IN_PROGRESS.labels(method=method).dec()
        HTTP_REQUESTS.labels(
            method=method,
            route=route_label,
            status=str(status_code),
        ).inc()
        HTTP_DURATION.labels(method=method, route=route_label).observe(duration)
        LOGGER.info(
            json.dumps(
                {
                    "event": "http_request",
                    "request_id": request_id,
                    "method": method,
                    "route": route_label,
                    "status_code": status_code,
                    "duration_ms": round(duration * 1000, 3),
                },
                separators=(",", ":"),
            )
        )


def get_scenario_service() -> ScenarioService:
    return app.state.scenarios


def get_result_repository() -> ResultRepository:
    return app.state.results


@app.get("/health", tags=["system"])
def health(settings: Annotated[Settings, Depends(get_settings)]) -> dict[str, str]:
    return {"status": "ok", "service": "atlas-api", "version": __version__, "env": settings.env}


@app.get("/ready", tags=["system"])
def readiness() -> dict[str, str]:
    try:
        with app.state.engine.connect() as connection:
            connection.execute(text("select 1"))
    except Exception as error:
        LOGGER.error("Database readiness check failed: %s", type(error).__name__)
        raise HTTPException(status_code=503, detail="Database unavailable") from error
    return {"status": "ready", "database": "ok"}


@app.get("/metrics", include_in_schema=False)
def metrics() -> Response:
    return Response(content=generate_latest(), headers={"Content-Type": CONTENT_TYPE_LATEST})


@app.get("/v1/scenario-presets", response_model=list[ScenarioPreset], tags=["scenarios"])
def list_scenario_presets(
    settings: Annotated[Settings, Depends(get_settings)],
) -> list[ScenarioPreset]:
    return load_scenario_presets(settings.scenario_config_dir)


@app.get("/v1/scenarios", response_model=list[ScenarioRecord], tags=["scenarios"])
def list_scenarios(
    service: Annotated[ScenarioService, Depends(get_scenario_service)],
) -> list[ScenarioRecord]:
    return service.list()


@app.post(
    "/v1/scenarios",
    response_model=ScenarioAccepted,
    status_code=status.HTTP_202_ACCEPTED,
    tags=["scenarios"],
)
def submit_scenario(
    request: ScenarioRequest,
    response: Response,
    service: Annotated[ScenarioService, Depends(get_scenario_service)],
) -> ScenarioAccepted:
    accepted = service.submit(request)
    if accepted.reused:
        response.headers["X-Atlas-Idempotent-Replay"] = "true"
    return accepted


@app.get("/v1/runs/{scenario_id}", response_model=ScenarioRecord, tags=["runs"])
def get_run(
    scenario_id: str,
    service: Annotated[ScenarioService, Depends(get_scenario_service)],
) -> ScenarioRecord:
    record = service.get(scenario_id)
    if record is None:
        raise HTTPException(status_code=404, detail="Scenario run not found")
    return record


@app.get("/v1/jobs/{job_id}", response_model=JobRecord, tags=["jobs"])
def get_job(
    job_id: str,
    service: Annotated[ScenarioService, Depends(get_scenario_service)],
) -> JobRecord:
    record = service.get_job(job_id)
    if record is None:
        raise HTTPException(status_code=404, detail="Scenario job not found")
    return record


@app.get("/v1/results/{run_id}/kpis", response_model=ScenarioKpi, tags=["results"])
def get_result_kpis(
    run_id: str,
    repository: Annotated[ResultRepository, Depends(get_result_repository)],
) -> ScenarioKpi:
    result = repository.get_kpis(run_id)
    if result is None:
        raise HTTPException(status_code=404, detail="Published result not found")
    return result


@app.get(
    "/v1/results/{run_id}/capacity",
    response_model=list[CapacityResult],
    tags=["results"],
)
def list_result_capacity(
    run_id: str,
    repository: Annotated[ResultRepository, Depends(get_result_repository)],
) -> list[CapacityResult]:
    result = repository.list_capacity(run_id)
    if result is None:
        raise HTTPException(status_code=404, detail="Published result not found")
    return result


@app.get("/v1/results/{run_id}/dispatch", response_model=DispatchPage, tags=["results"])
def list_result_dispatch(
    run_id: str,
    repository: Annotated[ResultRepository, Depends(get_result_repository)],
    start: datetime | None = None,
    end: datetime | None = None,
    asset: Annotated[str | None, Query(max_length=120)] = None,
    carrier: Annotated[str | None, Query(max_length=120)] = None,
    limit: Annotated[int, Query(ge=1, le=5_000)] = 1_000,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> DispatchPage:
    _validate_time_range(start, end)
    result = repository.list_dispatch(
        run_id,
        start=start,
        end=end,
        asset=asset,
        carrier=carrier,
        limit=limit,
        offset=offset,
    )
    if result is None:
        raise HTTPException(status_code=404, detail="Published result not found")
    items, has_more = result
    return DispatchPage(
        items=items,
        limit=limit,
        offset=offset,
        returned=len(items),
        has_more=has_more,
    )


@app.get("/v1/results/{run_id}/storage", response_model=StoragePage, tags=["results"])
def list_result_storage(
    run_id: str,
    repository: Annotated[ResultRepository, Depends(get_result_repository)],
    start: datetime | None = None,
    end: datetime | None = None,
    asset: Annotated[str | None, Query(max_length=120)] = None,
    carrier: Annotated[str | None, Query(max_length=120)] = None,
    limit: Annotated[int, Query(ge=1, le=5_000)] = 1_000,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> StoragePage:
    _validate_time_range(start, end)
    result = repository.list_storage(
        run_id,
        start=start,
        end=end,
        asset=asset,
        carrier=carrier,
        limit=limit,
        offset=offset,
    )
    if result is None:
        raise HTTPException(status_code=404, detail="Published result not found")
    items, has_more = result
    return StoragePage(
        items=items,
        limit=limit,
        offset=offset,
        returned=len(items),
        has_more=has_more,
    )


@app.get(
    "/v1/results/{run_id}/costs",
    response_model=list[CostResult],
    tags=["results"],
)
def list_result_costs(
    run_id: str,
    repository: Annotated[ResultRepository, Depends(get_result_repository)],
) -> list[CostResult]:
    result = repository.list_costs(run_id)
    if result is None:
        raise HTTPException(status_code=404, detail="Published result not found")
    return result


@app.get(
    "/v1/results/{run_id}/provenance",
    response_model=list[ProvenanceRecord],
    tags=["results"],
)
def list_result_provenance(
    run_id: str,
    repository: Annotated[ResultRepository, Depends(get_result_repository)],
) -> list[ProvenanceRecord]:
    result = repository.list_provenance(run_id)
    if result is None:
        raise HTTPException(status_code=404, detail="Published result not found")
    return result


@app.get(
    "/v1/comparisons",
    response_model=list[ScenarioComparison],
    tags=["comparisons"],
)
def list_comparisons(
    repository: Annotated[ResultRepository, Depends(get_result_repository)],
) -> list[ScenarioComparison]:
    return repository.list_comparisons()


@app.get(
    "/v1/comparisons/{base_run_id}/{comparison_run_id}",
    response_model=ScenarioComparison,
    tags=["comparisons"],
)
def get_comparison(
    base_run_id: str,
    comparison_run_id: str,
    repository: Annotated[ResultRepository, Depends(get_result_repository)],
) -> ScenarioComparison:
    result = repository.get_comparison(base_run_id, comparison_run_id)
    if result is None:
        raise HTTPException(status_code=404, detail="Scenario comparison not found")
    return result


def _validate_time_range(start: datetime | None, end: datetime | None) -> None:
    if start is None or end is None:
        return
    comparable_start = start.replace(tzinfo=UTC) if start.tzinfo is None else start.astimezone(UTC)
    comparable_end = end.replace(tzinfo=UTC) if end.tzinfo is None else end.astimezone(UTC)
    if comparable_start >= comparable_end:
        raise HTTPException(status_code=422, detail="start must be earlier than end")
