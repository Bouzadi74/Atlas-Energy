from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic import Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime configuration loaded from ATLAS_* environment variables."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_prefix="ATLAS_",
        extra="ignore",
    )

    env: str = "local"
    log_level: str = "INFO"
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    frontend_origin: str = "http://localhost:3000"
    auth_enabled: bool = False
    api_read_key: SecretStr | None = None
    api_write_key: SecretStr | None = None
    database_url: str = "postgresql+psycopg://atlas:atlas@localhost:55432/atlas"
    kafka_bootstrap_servers: str = "localhost:9092"
    mlflow_tracking_uri: str = "http://localhost:5000"
    data_root: Path = Field(default=Path("data"))
    scenario_config_dir: Path = Field(default=Path("configs/scenarios"))
    model_version: str = "pypsa-national-node-v1"
    data_version: str = "0e5327a2df73e087"
    optimization_config_path: Path = Field(
        default=Path("configs/optimization/national_baseline.yml")
    )
    optimization_calendar: Literal["planning", "baseline"] = "planning"
    dbt_refresh_enabled: bool = True
    dbt_project_dir: Path = Field(default=Path("dbt_atlas"))
    dbt_profiles_dir: Path = Field(default=Path("dbt_atlas"))
    dbt_timeout_seconds: int = 600

    @model_validator(mode="after")
    def validate_authentication(self) -> "Settings":
        if not self.auth_enabled:
            return self
        if self.api_read_key is None or self.api_write_key is None:
            raise ValueError(
                "ATLAS_API_READ_KEY and ATLAS_API_WRITE_KEY are required when auth is enabled"
            )
        if self.api_read_key.get_secret_value() == self.api_write_key.get_secret_value():
            raise ValueError("Read and write API keys must be different")
        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()
