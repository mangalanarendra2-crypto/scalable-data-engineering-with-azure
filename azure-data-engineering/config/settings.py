"""
config/settings.py
------------------
Centralized configuration management using Pydantic Settings.
Loads from environment variables, .env files, and config.yaml.
"""

from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from typing import Optional

import yaml
from pydantic import Field, SecretStr, validator
from pydantic_settings import BaseSettings


class ADLSSettings(BaseSettings):
    account_name: str = Field(..., env="ADLS_ACCOUNT_NAME")
    account_key: Optional[SecretStr] = Field(None, env="ADLS_ACCOUNT_KEY")
    container_bronze: str = "bronze"
    container_silver: str = "silver"
    container_gold: str = "gold"
    container_archive: str = "archive"

    class Config:
        env_prefix = "ADLS_"


class EventHubSettings(BaseSettings):
    namespace: str = Field(..., env="EVENT_HUB_NAMESPACE")
    connection_string: SecretStr = Field(..., env="EVENT_HUB_CONNECTION_STRING")
    checkpoint_store_connection: SecretStr = Field(..., env="CHECKPOINT_STORE_CONN")
    checkpoint_container: str = "eh-checkpoints"
    consumer_group: str = "$Default"

    class Config:
        env_prefix = "EVENT_HUB_"


class DatabricksSettings(BaseSettings):
    host: str = Field(..., env="DATABRICKS_HOST")
    token: SecretStr = Field(..., env="DATABRICKS_TOKEN")
    cluster_id: str = Field(..., env="DATABRICKS_CLUSTER_ID")

    class Config:
        env_prefix = "DATABRICKS_"


class SynapseSettings(BaseSettings):
    workspace_name: str = Field(..., env="SYNAPSE_WORKSPACE")
    sql_endpoint: str = Field(..., env="SYNAPSE_SQL_ENDPOINT")
    database: str = "DataWarehouse"

    class Config:
        env_prefix = "SYNAPSE_"


class AzureSettings(BaseSettings):
    tenant_id: str = Field(..., env="AZURE_TENANT_ID")
    subscription_id: str = Field(..., env="AZURE_SUBSCRIPTION_ID")
    resource_group: str = Field("rg-data-engineering-prod", env="AZURE_RESOURCE_GROUP")
    location: str = Field("eastus2", env="AZURE_LOCATION")
    key_vault_url: str = Field(..., env="KEY_VAULT_URL")

    class Config:
        env_prefix = "AZURE_"


class ProcessingSettings(BaseSettings):
    spark_executor_memory: str = "8g"
    spark_executor_cores: int = 4
    spark_num_executors: int = 10
    spark_shuffle_partitions: int = 200
    batch_size: int = 10_000
    max_retries: int = 3
    retry_delay_seconds: int = 30
    parallelism: int = 8


class DataQualitySettings(BaseSettings):
    enabled: bool = True
    fail_on_error: bool = False
    null_threshold: float = 0.05
    duplicate_threshold: float = 0.01
    freshness_hours: int = 24


class AppSettings(BaseSettings):
    """Root application settings."""

    environment: str = Field("development", env="APP_ENV")
    log_level: str = Field("INFO", env="LOG_LEVEL")
    log_format: str = Field("json", env="LOG_FORMAT")

    azure: AzureSettings = AzureSettings()
    adls: ADLSSettings = ADLSSettings()
    event_hub: EventHubSettings = EventHubSettings()
    databricks: DatabricksSettings = DatabricksSettings()
    synapse: SynapseSettings = SynapseSettings()
    processing: ProcessingSettings = ProcessingSettings()
    data_quality: DataQualitySettings = DataQualitySettings()

    @validator("environment")
    def validate_environment(cls, v: str) -> str:
        allowed = {"development", "staging", "production"}
        if v not in allowed:
            raise ValueError(f"environment must be one of {allowed}")
        return v

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False


@lru_cache(maxsize=1)
def get_settings() -> AppSettings:
    """Cached singleton settings loader."""
    return AppSettings()


def load_yaml_config(path: str | Path = "config/config.yaml") -> dict:
    """Load and resolve environment variables in YAML config."""
    config_path = Path(path)
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    raw = config_path.read_text()

    # Substitute ${ENV_VAR} patterns
    import re
    pattern = re.compile(r"\$\{(\w+)\}")
    resolved = pattern.sub(lambda m: os.environ.get(m.group(1), m.group(0)), raw)

    return yaml.safe_load(resolved)
