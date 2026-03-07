"""Shared pytest fixtures for pbdata tests."""

from pathlib import Path

import pytest

_CONFIGS_DIR = Path(__file__).parent.parent / "configs"


@pytest.fixture()
def sources_config_path() -> Path:
    """Path to the canonical sources YAML config."""
    return _CONFIGS_DIR / "sources.yaml"


@pytest.fixture()
def logging_config_path() -> Path:
    """Path to the canonical logging YAML config."""
    return _CONFIGS_DIR / "logging.yaml"
