from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any, Protocol, TypeVar

from sqlalchemy import text
from sqlalchemy.orm import Session, sessionmaker

from atlas.schemas.results import (
    CapacityResult,
    CostResult,
    DispatchPoint,
    ProvenanceRecord,
    ScenarioComparison,
    ScenarioKpi,
    StoragePoint,
)

ResultType = TypeVar(
    "ResultType",
    CapacityResult,
    CostResult,
    DispatchPoint,
    ProvenanceRecord,
    ScenarioComparison,
    ScenarioKpi,
    StoragePoint,
)


class ResultRepository(Protocol):
    def get_kpis(self, run_id: str) -> ScenarioKpi | None: ...

    def list_capacity(self, run_id: str) -> list[CapacityResult] | None: ...

    def list_dispatch(
        self,
        run_id: str,
        *,
        start: datetime | None,
        end: datetime | None,
        asset: str | None,
        carrier: str | None,
        limit: int,
        offset: int,
    ) -> tuple[list[DispatchPoint], bool] | None: ...

    def list_storage(
        self,
        run_id: str,
        *,
        start: datetime | None,
        end: datetime | None,
        asset: str | None,
        carrier: str | None,
        limit: int,
        offset: int,
    ) -> tuple[list[StoragePoint], bool] | None: ...

    def list_costs(self, run_id: str) -> list[CostResult] | None: ...

    def list_provenance(self, run_id: str) -> list[ProvenanceRecord] | None: ...

    def list_comparisons(self) -> list[ScenarioComparison]: ...

    def get_comparison(
        self, base_run_id: str, comparison_run_id: str
    ) -> ScenarioComparison | None: ...


class PostgresResultRepository:
    """Read-only adapter over tested dbt business marts."""

    def __init__(self, session_factory: sessionmaker[Session]) -> None:
        self._session_factory = session_factory

    def get_kpis(self, run_id: str) -> ScenarioKpi | None:
        with self._session_factory() as session:
            row = session.execute(
                text(
                    "select * from analytics_marts.fct_scenario_kpi "
                    "where run_id = :run_id"
                ),
                {"run_id": run_id},
            ).mappings().one_or_none()
            return self._model(ScenarioKpi, row)

    def list_capacity(self, run_id: str) -> list[CapacityResult] | None:
        return self._list_for_run(
            run_id,
            model=CapacityResult,
            query=(
                "select * from analytics_marts.fct_capacity "
                "where run_id = :run_id order by component, carrier, asset"
            ),
        )

    def list_dispatch(
        self,
        run_id: str,
        *,
        start: datetime | None,
        end: datetime | None,
        asset: str | None,
        carrier: str | None,
        limit: int,
        offset: int,
    ) -> tuple[list[DispatchPoint], bool] | None:
        clauses, parameters = self._hourly_filters(
            run_id,
            start=start,
            end=end,
            asset=asset,
            carrier=carrier,
        )
        parameters.update({"limit": limit + 1, "offset": offset})
        query = (
            "select * from analytics_marts.fct_dispatch_hourly where "
            + " and ".join(clauses)
            + " order by timestamp_utc, asset limit :limit offset :offset"
        )
        return self._paged_for_run(run_id, DispatchPoint, query, parameters, limit)

    def list_storage(
        self,
        run_id: str,
        *,
        start: datetime | None,
        end: datetime | None,
        asset: str | None,
        carrier: str | None,
        limit: int,
        offset: int,
    ) -> tuple[list[StoragePoint], bool] | None:
        clauses, parameters = self._hourly_filters(
            run_id,
            start=start,
            end=end,
            asset=asset,
            carrier=carrier,
        )
        parameters.update({"limit": limit + 1, "offset": offset})
        query = (
            "select * from analytics_marts.fct_storage_hourly where "
            + " and ".join(clauses)
            + " order by timestamp_utc, asset limit :limit offset :offset"
        )
        return self._paged_for_run(run_id, StoragePoint, query, parameters, limit)

    def list_costs(self, run_id: str) -> list[CostResult] | None:
        return self._list_for_run(
            run_id,
            model=CostResult,
            query=(
                "select * from analytics_marts.fct_cost_breakdown "
                "where run_id = :run_id order by component_type, carrier, asset"
            ),
        )

    def list_provenance(self, run_id: str) -> list[ProvenanceRecord] | None:
        return self._list_for_run(
            run_id,
            model=ProvenanceRecord,
            query=(
                "select * from analytics_marts.dim_run_provenance "
                "where run_id = :run_id order by provenance_key"
            ),
        )

    def list_comparisons(self) -> list[ScenarioComparison]:
        with self._session_factory() as session:
            rows = session.execute(
                text(
                    "select * from analytics_marts.fct_scenario_comparison "
                    "order by base_run_id, comparison_run_id"
                )
            ).mappings().all()
            return [ScenarioComparison.model_validate(row) for row in rows]

    def get_comparison(
        self, base_run_id: str, comparison_run_id: str
    ) -> ScenarioComparison | None:
        if base_run_id == comparison_run_id:
            return None
        with self._session_factory() as session:
            row = session.execute(
                text(
                    "select * from analytics_marts.fct_scenario_comparison where "
                    "(base_run_id = :base and comparison_run_id = :comparison) or "
                    "(base_run_id = :comparison and comparison_run_id = :base)"
                ),
                {"base": base_run_id, "comparison": comparison_run_id},
            ).mappings().one_or_none()
        if row is None:
            return None
        result = ScenarioComparison.model_validate(row)
        if result.base_run_id == base_run_id:
            return result
        values = result.model_dump()
        values.update(
            {
                "base_run_id": base_run_id,
                "comparison_run_id": comparison_run_id,
                "base_scenario_name": result.comparison_scenario_name,
                "comparison_scenario_name": result.base_scenario_name,
            }
        )
        for field in (
            "annual_cost_delta_eur",
            "average_cost_delta_eur_mwh",
            "renewable_share_delta",
            "emissions_delta_tco2",
            "curtailment_delta_mwh",
            "unserved_delta_mwh",
            "imports_delta_mwh",
        ):
            values[field] = -values[field]
        return ScenarioComparison.model_validate(values)

    def _list_for_run(
        self,
        run_id: str,
        *,
        model: type[ResultType],
        query: str,
    ) -> list[ResultType] | None:
        with self._session_factory() as session:
            if not self._run_exists(session, run_id):
                return None
            rows = session.execute(text(query), {"run_id": run_id}).mappings().all()
            return [model.model_validate(row) for row in rows]

    def _paged_for_run(
        self,
        run_id: str,
        model: type[ResultType],
        query: str,
        parameters: dict[str, object],
        limit: int,
    ) -> tuple[list[ResultType], bool] | None:
        with self._session_factory() as session:
            if not self._run_exists(session, run_id):
                return None
            rows = session.execute(text(query), parameters).mappings().all()
            has_more = len(rows) > limit
            return [model.model_validate(row) for row in rows[:limit]], has_more

    @staticmethod
    def _run_exists(session: Session, run_id: str) -> bool:
        return (
            session.execute(
                text(
                    "select 1 from analytics_marts.dim_scenario "
                    "where run_id = :run_id"
                ),
                {"run_id": run_id},
            ).scalar_one_or_none()
            is not None
        )

    @staticmethod
    def _hourly_filters(
        run_id: str,
        *,
        start: datetime | None,
        end: datetime | None,
        asset: str | None,
        carrier: str | None,
    ) -> tuple[list[str], dict[str, object]]:
        clauses = ["run_id = :run_id"]
        parameters: dict[str, object] = {"run_id": run_id}
        for name, value in (("start", start), ("end", end)):
            if value is not None:
                if value.tzinfo is not None:
                    value = value.astimezone(UTC).replace(tzinfo=None)
                operator = ">=" if name == "start" else "<"
                clauses.append(f"timestamp_utc {operator} :{name}")
                parameters[name] = value
        if asset is not None:
            clauses.append("asset = :asset")
            parameters["asset"] = asset
        if carrier is not None:
            clauses.append("carrier = :carrier")
            parameters["carrier"] = carrier
        return clauses, parameters

    @staticmethod
    def _model(
        model: type[ResultType], row: Mapping[str, Any] | None
    ) -> ResultType | None:
        return None if row is None else model.model_validate(row)
