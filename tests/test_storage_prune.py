from __future__ import annotations

import json
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


def test_prune_storage_dry_run_only_targets_completed_merged_runs() -> None:
    tmp_path = _tmp_dir("storage_prune_dry_run")
    storage_root = tmp_path / "storage"
    run_dir = storage_root / "runs" / "precompute" / "extract_keep"
    (run_dir / "status").mkdir(parents=True)
    (run_dir / "shards" / "extract" / "chunk_00000").mkdir(parents=True)
    (run_dir / "merged" / "extract").mkdir(parents=True)
    (run_dir / "chunks").mkdir(parents=True)
    (run_dir / "run_manifest.json").write_text(
        json.dumps(
            {
                "run_id": "extract_keep",
                "stage": "extract",
                "total_inputs": 1,
                "chunks": [{"chunk_index": 0}],
            }
        ),
        encoding="utf-8",
    )
    (run_dir / "status" / "chunk_00000.status.json").write_text(
        json.dumps({"status": "completed", "processed": 1, "ok": 1, "cached": 0, "failed": 0}),
        encoding="utf-8",
    )
    (run_dir / "shards" / "extract" / "chunk_00000" / "artifact.txt").write_text("x", encoding="utf-8")
    (run_dir / "merged" / "extract" / "merge_manifest.json").write_text("{}", encoding="utf-8")
    (run_dir / "merged" / "extract" / "artifact.txt").write_text("y", encoding="utf-8")

    incomplete_run_dir = storage_root / "runs" / "precompute" / "extract_in_progress"
    (incomplete_run_dir / "status").mkdir(parents=True)
    (incomplete_run_dir / "shards" / "extract" / "chunk_00000").mkdir(parents=True)
    (incomplete_run_dir / "chunks").mkdir(parents=True)
    (incomplete_run_dir / "run_manifest.json").write_text(
        json.dumps(
            {
                "run_id": "extract_in_progress",
                "stage": "extract",
                "total_inputs": 1,
                "chunks": [{"chunk_index": 0}],
            }
        ),
        encoding="utf-8",
    )
    (incomplete_run_dir / "status" / "chunk_00000.status.json").write_text(
        json.dumps({"status": "running", "processed": 0, "ok": 0, "cached": 0, "failed": 0}),
        encoding="utf-8",
    )
    (incomplete_run_dir / "shards" / "extract" / "chunk_00000" / "artifact.txt").write_text("z", encoding="utf-8")

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
            "prune-storage",
        ],
        catch_exceptions=False,
    )

    assert result.exit_code == 0
    assert "Prunable total:" in result.output
    assert "extract_keep" in result.output
    assert "extract_in_progress" not in result.output
    assert (run_dir / "shards" / "extract").exists()
    assert (run_dir / "merged" / "extract").exists()


def test_prune_storage_apply_removes_only_safe_targets() -> None:
    tmp_path = _tmp_dir("storage_prune_apply")
    storage_root = tmp_path / "storage"
    run_dir = storage_root / "runs" / "precompute" / "graph_done"
    (run_dir / "status").mkdir(parents=True)
    (run_dir / "shards" / "build-graph" / "chunk_00000" / "graph").mkdir(parents=True)
    (run_dir / "merged" / "build-graph").mkdir(parents=True)
    (run_dir / "chunks").mkdir(parents=True)
    (run_dir / "run_manifest.json").write_text(
        json.dumps(
            {
                "run_id": "graph_done",
                "stage": "build-graph",
                "total_inputs": 1,
                "chunks": [{"chunk_index": 0}],
            }
        ),
        encoding="utf-8",
    )
    (run_dir / "status" / "chunk_00000.status.json").write_text(
        json.dumps({"status": "completed", "processed": 1, "ok": 1, "cached": 0, "failed": 0}),
        encoding="utf-8",
    )
    (run_dir / "shards" / "build-graph" / "chunk_00000" / "graph" / "graph_nodes.json").write_text("[]", encoding="utf-8")
    (run_dir / "merged" / "build-graph" / "merge_manifest.json").write_text("{}", encoding="utf-8")
    (run_dir / "merged" / "build-graph" / "graph_nodes.json").write_text("[]", encoding="utf-8")

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
            "prune-storage",
            "--apply",
        ],
        catch_exceptions=False,
    )

    assert result.exit_code == 0
    assert "Deleted:" in result.output
    assert not (run_dir / "shards" / "build-graph").exists()
    assert not (run_dir / "merged" / "build-graph").exists()
    assert (run_dir / "run_manifest.json").exists()
