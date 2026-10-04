from pathlib import Path

import yaml
from pydantic import BaseModel, ConfigDict

from atlas.schemas.scenario import ScenarioPreset


class _ScenarioDocument(BaseModel):
    """Strict on-disk contract for a versioned scenario preset."""

    model_config = ConfigDict(extra="forbid")

    name: str
    planning_year: int
    weather_year: int
    demand_growth: float
    gas_price_eur_mwh_th: float
    carbon_price_eur_t: float
    renewable_generation_min: float
    battery_capex_multiplier: float
    solar_capex_multiplier: float
    wind_capex_multiplier: float
    provenance: dict[str, object]


def load_scenario_presets(config_dir: Path) -> list[ScenarioPreset]:
    """Load and validate every versioned scenario YAML in deterministic order."""

    if not config_dir.is_dir():
        raise FileNotFoundError(f"Scenario configuration directory not found: {config_dir}")

    presets: list[ScenarioPreset] = []
    names: set[str] = set()
    for path in sorted(config_dir.glob("*.yml")):
        try:
            payload = yaml.safe_load(path.read_text(encoding="utf-8"))
        except yaml.YAMLError as error:
            raise ValueError(f"Invalid scenario YAML {path}: {error}") from error
        document = _ScenarioDocument.model_validate(payload)
        request_payload = document.model_dump(exclude={"provenance"})
        preset = ScenarioPreset.model_validate(
            {
                "preset_id": path.stem,
                "request": request_payload,
                "provenance": document.provenance,
            }
        )
        if preset.request.name in names:
            raise ValueError(f"Duplicate scenario name: {preset.request.name}")
        names.add(preset.request.name)
        presets.append(preset)

    if not presets:
        raise ValueError(f"No scenario presets found in {config_dir}")
    return presets
