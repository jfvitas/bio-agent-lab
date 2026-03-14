from __future__ import annotations

import json
from pathlib import Path
from uuid import uuid4
from unittest.mock import patch

import yaml
from typer.testing import CliRunner

from pbdata.cli import app
from pbdata.schemas.records import EntryRecord

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


def _write_raw_record(path: Path, pdb_id: str) -> None:
    path.write_text(
        json.dumps(
            {
                "rcsb_id": pdb_id,
                "rcsb_entry_info": {},
                "polymer_entities": [],
                "nonpolymer_entities": [],
            }
        ),
        encoding="utf-8",
    )


def test_plan_precompute_writes_chunk_manifests() -> None:
    tmp_path = _tmp_dir("precompute_plan")
    storage_root = tmp_path / "storage"
    raw_dir = storage_root / "data" / "raw" / "rcsb"
    raw_dir.mkdir(parents=True)
    for pdb_id in ["1ABC", "2DEF", "3GHI"]:
        _write_raw_record(raw_dir / f"{pdb_id}.json", pdb_id)

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
            "plan-precompute",
            "--stage",
            "extract",
            "--chunk-size",
            "2",
            "--run-id",
            "extract_test_run",
        ],
        catch_exceptions=False,
    )

    assert result.exit_code == 0
    manifest = json.loads((storage_root / "runs" / "precompute" / "extract_test_run" / "run_manifest.json").read_text(encoding="utf-8"))
    assert manifest["chunk_count"] == 2
    assert manifest["total_inputs"] == 3
    chunk0 = json.loads((storage_root / "runs" / "precompute" / "extract_test_run" / "chunks" / "chunk_00000.json").read_text(encoding="utf-8"))
    assert chunk0["input_count"] == 2


def test_cli_bootstrap_tolerates_stale_default_log_config() -> None:
    tmp_path = _tmp_dir("stale_log_config")
    storage_root = tmp_path / "storage"
    config_path = tmp_path / "sources.yaml"
    _write_sources_config(config_path)
    runner = CliRunner()
    with patch("pbdata.cli._path_exists_safe", side_effect=lambda path: False if str(path).endswith("logging.yaml") else True):
        result = runner.invoke(
            app,
            [
                "--storage-root",
                str(storage_root),
                "--config",
                str(config_path),
                "status",
            ],
            catch_exceptions=False,
        )
    assert result.exit_code == 0


def test_run_precompute_shard_executes_extract_into_shard_dir() -> None:
    tmp_path = _tmp_dir("precompute_shard")
    storage_root = tmp_path / "storage"
    raw_dir = storage_root / "data" / "raw" / "rcsb"
    raw_dir.mkdir(parents=True)
    _write_raw_record(raw_dir / "1ABC.json", "1ABC")

    config_path = tmp_path / "sources.yaml"
    _write_sources_config(config_path)
    runner = CliRunner()
    runner.invoke(
        app,
        [
            "--storage-root",
            str(storage_root),
            "--config",
            str(config_path),
            "plan-precompute",
            "--stage",
            "extract",
            "--chunk-size",
            "1",
            "--run-id",
            "extract_shard_run",
        ],
        catch_exceptions=False,
    )

    def _fake_extract_rcsb_entry(raw, **_kwargs):
        pdb_id = raw["rcsb_id"]
        return {
            "entry": EntryRecord(source_database="RCSB", source_record_id=pdb_id, pdb_id=pdb_id),
            "chains": [],
            "bound_objects": [],
            "interfaces": [],
            "assays": [],
            "provenance": [],
        }

    with patch("pbdata.precompute.extract_rcsb_entry", side_effect=_fake_extract_rcsb_entry):
        result = runner.invoke(
            app,
            [
                "--storage-root",
                str(storage_root),
                "--config",
                str(config_path),
                "run-precompute-shard",
                "--run-id",
                "extract_shard_run",
                "--chunk-index",
                "0",
                "--no-download-structures",
            ],
            catch_exceptions=False,
        )

    assert result.exit_code == 0
    shard_entry = (
        storage_root
        / "runs"
        / "precompute"
        / "extract_shard_run"
        / "shards"
        / "extract"
        / "chunk_00000"
        / "extracted"
        / "entry"
        / "1ABC.json"
    )
    assert shard_entry.exists()


def test_merge_precompute_shards_copies_into_workspace_outputs() -> None:
    tmp_path = _tmp_dir("precompute_merge")
    storage_root = tmp_path / "storage"
    raw_dir = storage_root / "data" / "raw" / "rcsb"
    raw_dir.mkdir(parents=True)
    _write_raw_record(raw_dir / "1ABC.json", "1ABC")

    config_path = tmp_path / "sources.yaml"
    _write_sources_config(config_path)
    runner = CliRunner()
    runner.invoke(
        app,
        [
            "--storage-root",
            str(storage_root),
            "--config",
            str(config_path),
            "plan-precompute",
            "--stage",
            "extract",
            "--chunk-size",
            "1",
            "--run-id",
            "extract_merge_run",
        ],
        catch_exceptions=False,
    )

    shard_output = (
        storage_root
        / "runs"
        / "precompute"
        / "extract_merge_run"
        / "shards"
        / "extract"
        / "chunk_00000"
        / "extracted"
        / "entry"
    )
    shard_output.mkdir(parents=True, exist_ok=True)
    (shard_output / "1ABC.json").write_text(json.dumps({"pdb_id": "1ABC"}), encoding="utf-8")
    for table in ["chains", "bound_objects", "interfaces", "assays", "provenance"]:
        table_dir = shard_output.parent / table
        table_dir.mkdir(parents=True, exist_ok=True)
        (table_dir / "1ABC.json").write_text("[]", encoding="utf-8")

    result = runner.invoke(
        app,
        [
            "--storage-root",
            str(storage_root),
            "--config",
            str(config_path),
            "merge-precompute-shards",
            "--run-id",
            "extract_merge_run",
        ],
        catch_exceptions=False,
    )

    assert result.exit_code == 0
    assert (storage_root / "data" / "extracted" / "entry" / "1ABC.json").exists()
    assert (
        storage_root
        / "runs"
        / "precompute"
        / "extract_merge_run"
        / "merged"
        / "extract"
        / "merge_manifest.json"
    ).exists()


def test_run_precompute_build_graph_shard_and_merge() -> None:
    tmp_path = _tmp_dir("precompute_graph")
    storage_root = tmp_path / "storage"
    entry_dir = storage_root / "data" / "extracted" / "entry"
    entry_dir.mkdir(parents=True)
    (entry_dir / "1ABC.json").write_text(
        json.dumps({"pdb_id": "1ABC", "source_database": "RCSB", "source_record_id": "1ABC"}),
        encoding="utf-8",
    )
    for table in ["chains", "bound_objects", "interfaces", "assays", "provenance"]:
        table_dir = storage_root / "data" / "extracted" / table
        table_dir.mkdir(parents=True, exist_ok=True)
        (table_dir / "1ABC.json").write_text("[]", encoding="utf-8")

    config_path = tmp_path / "sources.yaml"
    _write_sources_config(config_path)
    runner = CliRunner()
    runner.invoke(
        app,
        [
            "--storage-root",
            str(storage_root),
            "--config",
            str(config_path),
            "plan-precompute",
            "--stage",
            "build-graph",
            "--chunk-size",
            "1",
            "--run-id",
            "graph_run",
        ],
        catch_exceptions=False,
    )

    def _fake_build_graph_from_extracted(_extracted_dir, output_dir):
        output_dir.mkdir(parents=True, exist_ok=True)
        nodes = output_dir / "graph_nodes.json"
        edges = output_dir / "graph_edges.json"
        manifest = output_dir / "graph_manifest.json"
        nodes.write_text(json.dumps([{"node_id": "protein:1ABC", "metadata": {"pdb_id": "1ABC"}}], indent=2), encoding="utf-8")
        edges.write_text(json.dumps([{"edge_id": "edge:1", "source_node_id": "protein:1ABC", "target_node_id": "protein:1ABC"}], indent=2), encoding="utf-8")
        manifest.write_text(json.dumps({"status": "ok"}, indent=2), encoding="utf-8")
        return nodes, edges, manifest

    with patch("pbdata.graph.builder.build_graph_from_extracted", side_effect=_fake_build_graph_from_extracted):
        result = runner.invoke(
            app,
            [
                "--storage-root",
                str(storage_root),
                "--config",
                str(config_path),
                "run-precompute-shard",
                "--run-id",
                "graph_run",
                "--chunk-index",
                "0",
            ],
            catch_exceptions=False,
        )
        assert result.exit_code == 0

        merge_result = runner.invoke(
            app,
            [
                "--storage-root",
                str(storage_root),
                "--config",
                str(config_path),
                "merge-precompute-shards",
                "--run-id",
                "graph_run",
            ],
            catch_exceptions=False,
        )

    assert merge_result.exit_code == 0
    assert (storage_root / "data" / "graph" / "graph_nodes.json").exists()
    assert (storage_root / "data" / "graph" / "graph_edges.json").exists()
