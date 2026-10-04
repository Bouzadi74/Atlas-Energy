"""Small process boundary for Atlas commands; domain work stays in src/atlas."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

from airflow.sdk import get_current_context, task

PROJECT_ROOT = Path(os.environ.get("ATLAS_PROJECT_ROOT", "/opt/atlas"))
EXECUTABLE_ROOT = PROJECT_ROOT / ".venv" / "bin"


@task
def run_atlas(command: str, arguments: list[str], use_weather_year: bool = False) -> None:
    argv = [str(EXECUTABLE_ROOT / command), *arguments]
    if use_weather_year:
        year = get_current_context()["params"]["weather_year"]
        if type(year) is not int or not 2020 <= year <= 2024:
            raise ValueError("weather_year must be an integer from 2020 to 2024")
        argv.extend(("--year", str(year)))
    subprocess.run(argv, cwd=PROJECT_ROOT, check=True, timeout=7200)
