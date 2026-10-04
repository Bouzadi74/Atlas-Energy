from atlas.scenarios.service import scenario_hash
from atlas.schemas.scenario import ScenarioRequest


def test_hash_is_independent_of_input_key_order() -> None:
    left = ScenarioRequest(name="Baseline", planning_year=2030)
    right = ScenarioRequest.model_validate({"planning_year": 2030, "name": "Baseline"})
    assert scenario_hash(left, model_version="v1", data_version="d1") == scenario_hash(
        right,
        model_version="v1",
        data_version="d1",
    )

