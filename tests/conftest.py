"""Shared pytest fixtures for pbdata tests."""

from pathlib import Path

import pytest
import yaml

_CONFIGS_DIR = Path(__file__).parent.parent / "configs"
_LOCAL_TMP = Path(__file__).parent / "_tmp"
_LOCAL_TMP.mkdir(exist_ok=True)

# Default config content for tests (decoupled from live GUI edits)
_DEFAULT_SOURCES = {
    "sources": {
        "rcsb": {"enabled": True},
        "bindingdb": {"enabled": False, "extra": {}},
        "chembl": {"enabled": False},
        "pdbbind": {"enabled": False, "extra": {}},
        "biolip": {"enabled": False, "extra": {}},
        "skempi": {"enabled": False, "extra": {}},
    }
}


@pytest.fixture()
def sources_config_path() -> Path:
    """Write a known-good sources config to a temp file.

    This isolates tests from GUI-driven changes to configs/sources.yaml.
    """
    from uuid import uuid4

    cfg_path = _LOCAL_TMP / f"{uuid4().hex}_sources.yaml"
    cfg_path.write_text(yaml.dump(_DEFAULT_SOURCES), encoding="utf-8")
    return cfg_path


@pytest.fixture()
def logging_config_path() -> Path:
    """Path to the canonical logging YAML config."""
    return _CONFIGS_DIR / "logging.yaml"
