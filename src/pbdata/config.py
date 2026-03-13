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

    # Default assumption:
    # - RCSB, BindingDB, and ChEMBL are the useful out-of-the-box live sources.
    # - PDBbind, BioLiP, and SKEMPI still require explicit user intent because
    #   they depend on local files or a specific dataset workflow.
    rcsb: SourceConfig = SourceConfig(enabled=True)
    bindingdb: SourceConfig = SourceConfig(enabled=True)
    chembl: SourceConfig = SourceConfig(enabled=True)
    pdbbind: SourceConfig = SourceConfig()
    biolip: SourceConfig = SourceConfig()
    skempi: SourceConfig = SourceConfig()
    alphafold_db: SourceConfig = SourceConfig()
    uniprot: SourceConfig = SourceConfig()
    reactome: SourceConfig = SourceConfig()
    interpro: SourceConfig = SourceConfig()
    pfam: SourceConfig = SourceConfig()
    cath: SourceConfig = SourceConfig()
    scop: SourceConfig = SourceConfig()


class AppConfig(BaseModel):
    """Top-level application configuration."""

    storage_root: str | None = None
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
