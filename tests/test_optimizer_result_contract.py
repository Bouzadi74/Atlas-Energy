from pathlib import Path

import pytest

from atlas.optimization.engine import load_optimization_config, run_optimization
from atlas.optimization.publication import validate_run_artifacts


def test_optimizer_run_contract_validates(tmp_path: Path) -> None:
    pytest.importorskip("pypsa")
    config = load_optimization_config(Path("configs/optimization/national_baseline.yml"))
    result = run_optimization(config=config, mode="toy", output_root=tmp_path)

    validated = validate_run_artifacts(result.output_path)

    assert validated.manifest["run_id"] == result.run_id
    assert len(validated.dispatch) == 96
    assert len(validated.storage) == 24
    assert len(validated.artifacts) == 11


def test_optimizer_run_contract_rejects_checksum_mismatch(tmp_path: Path) -> None:
    pytest.importorskip("pypsa")
    config = load_optimization_config(Path("configs/optimization/national_baseline.yml"))
    result = run_optimization(config=config, mode="toy", output_root=tmp_path)
    capacity_csv = result.output_path / "capacity.csv"
    capacity_csv.write_bytes(capacity_csv.read_bytes() + b"\ncorrupt")

    with pytest.raises(ValueError, match="Checksum mismatch"):
        validate_run_artifacts(result.output_path)
