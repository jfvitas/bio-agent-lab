"""Tests for the config loader and logging setup."""

import logging
from pathlib import Path
from uuid import uuid4

import json
from pathlib import Path
from uuid import uuid4

import pytest
from typer.testing import CliRunner

from pbdata.cli import app
from pbdata.config import AppConfig, load_config
from pbdata.logging_config import setup_logging

_LOCAL_TMP = Path(__file__).parent / "_tmp"
_LOCAL_TMP.mkdir(exist_ok=True)


def _tmp_file(name: str) -> Path:
    return _LOCAL_TMP / f"{uuid4().hex}_{name}"


# ---------------------------------------------------------------------------
# Config loader
# ---------------------------------------------------------------------------

def test_load_config_returns_app_config(sources_config_path: Path) -> None:
    cfg = load_config(sources_config_path)
    assert isinstance(cfg, AppConfig)


def test_load_config_rcsb_enabled(sources_config_path: Path) -> None:
    cfg = load_config(sources_config_path)
    assert cfg.sources.rcsb.enabled is True


def test_load_config_other_sources_disabled(sources_config_path: Path) -> None:
    cfg = load_config(sources_config_path)
    assert cfg.sources.bindingdb.enabled is False
    assert cfg.sources.chembl.enabled is False
    assert cfg.sources.pdbbind.enabled is False
    assert cfg.sources.biolip.enabled is False
    assert cfg.sources.skempi.enabled is False


def test_load_config_missing_file_raises() -> None:
    with pytest.raises(FileNotFoundError):
        load_config("/nonexistent/config.yaml")


def test_load_config_empty_yaml() -> None:
    """An empty YAML file should produce a default AppConfig."""
    empty = _tmp_file("empty.yaml")
    empty.write_text("")
    cfg = load_config(empty)
    assert isinstance(cfg, AppConfig)


# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------

def test_setup_logging_default_no_error() -> None:
    """Calling setup_logging without a path should not raise."""
    setup_logging()
    logger = logging.getLogger("pbdata.test")
    assert logger is not None


def test_setup_logging_from_yaml(logging_config_path: Path) -> None:
    setup_logging(logging_config_path)
    root = logging.getLogger()
    assert root.level == logging.INFO


def test_setup_logging_missing_file_raises() -> None:
    with pytest.raises(FileNotFoundError):
        setup_logging("/nonexistent/logging.yaml")


def test_setup_logging_invalid_yaml_raises() -> None:
    """A YAML file missing 'version: 1' must be rejected before dictConfig sees it."""
    from pydantic import ValidationError
    bad = _tmp_file("bad_logging.yaml")
    bad.write_text("version: 2\n")  # dictConfig only accepts version 1
    with pytest.raises(ValidationError):
        setup_logging(bad)


# ---------------------------------------------------------------------------
# Base adapter contract
# ---------------------------------------------------------------------------

_LOCAL_TMP = Path(__file__).parent / "_tmp"
_LOCAL_TMP.mkdir(exist_ok=True)


def test_base_adapter_cannot_be_instantiated() -> None:
    from pbdata.sources.base import BaseAdapter
    with pytest.raises(TypeError):
        BaseAdapter()  # type: ignore[abstract]


def test_rcsb_adapter_has_correct_source_name() -> None:
    from pbdata.sources.rcsb import RCSBAdapter
    adapter = RCSBAdapter()
    assert adapter.source_name == "RCSB"


def test_rcsb_adapter_falls_back_to_cached_json_when_live_fetch_fails() -> None:
    from pbdata.sources.rcsb import RCSBAdapter
    from unittest.mock import patch

    pdb_id = "1ABC"
    cache_dir = _LOCAL_TMP / f"{uuid4().hex}_rcsb_raw"
    cache_dir.mkdir(parents=True)
    payload = {"rcsb_id": pdb_id, "polymer_entities": [], "nonpolymer_entities": []}
    (cache_dir / f"{pdb_id}.json").write_text(json.dumps(payload), encoding="utf-8")

    adapter = RCSBAdapter()
    with patch("pbdata.sources.rcsb._DEFAULT_RAW_DIR", cache_dir), patch(
        "pbdata.sources.rcsb_search.fetch_entries_batch",
        side_effect=RuntimeError("blocked"),
    ), patch("pbdata.sources.rcsb.fetch_mmcif_supplement", return_value=None):
        raw = adapter.fetch_metadata(pdb_id)

    assert raw["rcsb_id"] == pdb_id


def test_rcsb_adapter_raises_clear_error_when_live_and_cache_fail() -> None:
    from pbdata.sources.rcsb import RCSBAdapter
    from unittest.mock import patch

    adapter = RCSBAdapter()
    missing_cache_dir = _LOCAL_TMP / f"{uuid4().hex}_missing_rcsb_raw"
    with patch("pbdata.sources.rcsb._DEFAULT_RAW_DIR", missing_cache_dir), patch(
        "pbdata.sources.rcsb_search.fetch_entries_batch",
        side_effect=RuntimeError("blocked"),
    ):
        with pytest.raises(RuntimeError, match="no cached JSON was found"):
            adapter.fetch_metadata("1ABC")


def test_cli_ingest_dry_run_reports_count() -> None:
    """--dry-run queries RCSB and prints a count without downloading."""
    from unittest.mock import patch
    runner = CliRunner()
    with patch("pbdata.sources.rcsb_search.count_entries", return_value=1234):
        result = runner.invoke(app, ["ingest", "--dry-run"])
    assert result.exit_code == 0, result.output
    assert "1,234" in result.output


def test_rcsb_search_count_uses_v2_endpoint() -> None:
    from unittest.mock import Mock, patch

    from pbdata.criteria import SearchCriteria
    from pbdata.sources import rcsb_search

    response = Mock()
    response.json.return_value = {"total_count": 7}
    response.raise_for_status.return_value = None

    with patch("pbdata.sources.rcsb_search.requests.post", return_value=response) as post:
        count = rcsb_search.count_entries(SearchCriteria())

    assert count == 7
    assert post.call_args.args[0] == "https://search.rcsb.org/rcsbsearch/v2/query"


def test_cli_missing_config_fails_clearly() -> None:
    runner = CliRunner()
    result = runner.invoke(app, ["--config", "missing.yaml", "ingest"])
    assert result.exit_code != 0
    assert isinstance(result.exception, FileNotFoundError)
    assert "Config file not found" in str(result.exception)
