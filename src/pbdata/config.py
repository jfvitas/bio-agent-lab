"""YAML configuration loader for pbdata.

Loads and validates application and source configuration from YAML files.
"""

from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field


class SourceConfig(BaseModel):
    """Configuration for a single data source adapter."""

    enabled: bool = False
    extra: dict[str, Any] = Field(default_factory=dict)


class SourcesConfig(BaseModel):
    """Configuration block for all source adapters."""

    rcsb: SourceConfig = SourceConfig()
    bindingdb: SourceConfig = SourceConfig()
    pdbbind: SourceConfig = SourceConfig()
    biolip: SourceConfig = SourceConfig()
    skempi: SourceConfig = SourceConfig()


class AppConfig(BaseModel):
    """Top-level application configuration."""

    sources: SourcesConfig = SourcesConfig()


def load_config(path: str | Path) -> AppConfig:
    """Load and validate application configuration from a YAML file.

    Args:
        path: Path to the YAML config file.

    Returns:
        Validated AppConfig instance.

    Raises:
        FileNotFoundError: If the config file does not exist.
        ValueError: If the YAML content fails schema validation.
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")
    with path.open() as f:
        raw: dict[str, Any] = yaml.safe_load(f) or {}
    return AppConfig.model_validate(raw)
