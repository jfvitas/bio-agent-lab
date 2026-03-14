from __future__ import annotations

from pathlib import Path
from uuid import uuid4

import yaml
from typer.testing import CliRunner

from pbdata.cli import app


_LOCAL_TMP = Path(__file__).parent / "_tmp"
_LOCAL_TMP.mkdir(exist_ok=True)


def _tmp_dir(name: str) -> Path:
    path = _LOCAL_TMP / f"{uuid4().hex}_{name}"
    path.mkdir(exist_ok=True)
    return path


def _write_sources_config(path: Path) -> None:
    path.write_text(
        yaml.dump(
            {
                "sources": {
                    "rcsb": {"enabled": True, "extra": {}},
                    "bindingdb": {"enabled": False, "extra": {}},
                    "chembl": {"enabled": False, "extra": {}},
                    "pdbbind": {"enabled": False, "extra": {}},
                    "biolip": {"enabled": False, "extra": {}},
                    "skempi": {"enabled": False, "extra": {}},
                }
            }
        ),
        encoding="utf-8",
    )


def test_report_storage_summarizes_managed_areas() -> None:
    tmp_path = _tmp_dir("storage_audit")
    storage_root = tmp_path / "storage"
    (storage_root / "data" / "raw" / "rcsb").mkdir(parents=True)
    (storage_root / "data" / "raw" / "rcsb" / "1ABC.json").write_text("{}", encoding="utf-8")
    (storage_root / "data" / "structures" / "rcsb").mkdir(parents=True)
    (storage_root / "data" / "structures" / "rcsb" / "1ABC.cif").write_text("data", encoding="utf-8")
    (storage_root / "runs" / "precompute").mkdir(parents=True)
    (storage_root / "runs" / "precompute" / "marker.txt").write_text("marker", encoding="utf-8")

    config_path = tmp_path / "sources.yaml"
    _write_sources_config(config_path)
    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "--storage-root",
            str(storage_root),
            "--config",
            str(config_path),
            "report-storage",
        ],
        catch_exceptions=False,
    )

    assert result.exit_code == 0
    assert "Storage root:" in result.output
    assert "Raw RCSB" in result.output
    assert "Structures" in result.output
    assert "Precompute runs" in result.output
    assert "Largest tracked area:" in result.output
