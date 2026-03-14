from __future__ import annotations

import gzip
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


def test_package_raw_rcsb_writes_gzipped_jsonl_shards() -> None:
    tmp_path = _tmp_dir("package_raw_rcsb")
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
            "package-raw-rcsb",
            "--shard-size",
            "2",
            "--package-id",
            "raw_pkg_test",
        ],
        catch_exceptions=False,
    )

    assert result.exit_code == 0
    manifest_path = storage_root / "data" / "packaged" / "raw_rcsb" / "raw_pkg_test" / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["shard_count"] == 2
    shard_path = storage_root / "data" / "packaged" / "raw_rcsb" / "raw_pkg_test" / "shards" / "raw_rcsb_shard_00000.jsonl.gz"
    with gzip.open(shard_path, "rt", encoding="utf-8") as handle:
        rows = [json.loads(line) for line in handle if line.strip()]
    assert rows[0]["pdb_id"] == "1ABC"
    assert rows[0]["raw"]["rcsb_id"] == "1ABC"


def test_unpack_raw_rcsb_package_restores_json_files() -> None:
    tmp_path = _tmp_dir("unpack_raw_rcsb")
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
            "package-raw-rcsb",
            "--package-id",
            "raw_pkg_roundtrip",
        ],
        catch_exceptions=False,
    )

    for path in raw_dir.glob("*.json"):
        path.unlink()

    result = runner.invoke(
        app,
        [
            "--storage-root",
            str(storage_root),
            "--config",
            str(config_path),
            "unpack-raw-rcsb-package",
            "--package",
            str(storage_root / "data" / "packaged" / "raw_rcsb" / "raw_pkg_roundtrip"),
        ],
        catch_exceptions=False,
    )

    assert result.exit_code == 0
    restored = json.loads((raw_dir / "1ABC.json").read_text(encoding="utf-8"))
    assert restored["rcsb_id"] == "1ABC"


def test_unpack_raw_rcsb_package_falls_back_from_foreign_absolute_paths() -> None:
    tmp_path = _tmp_dir("unpack_raw_rcsb_foreign_paths")
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
            "package-raw-rcsb",
            "--package-id",
            "raw_pkg_foreign_paths",
        ],
        catch_exceptions=False,
    )

    manifest_path = storage_root / "data" / "packaged" / "raw_rcsb" / "raw_pkg_foreign_paths" / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["shards"][0]["path"] = r"C:\Users\jfvit\Documents\bio-agent-lab\data\packaged\raw_rcsb\raw_pkg_foreign_paths\shards\raw_rcsb_shard_00000.jsonl.gz"
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    for path in raw_dir.glob("*.json"):
        path.unlink()

    result = runner.invoke(
        app,
        [
            "--storage-root",
            str(storage_root),
            "--config",
            str(config_path),
            "unpack-raw-rcsb-package",
            "--package",
            str(storage_root / "data" / "packaged" / "raw_rcsb" / "raw_pkg_foreign_paths"),
        ],
        catch_exceptions=False,
    )

    assert result.exit_code == 0
    restored = json.loads((raw_dir / "1ABC.json").read_text(encoding="utf-8"))
    assert restored["rcsb_id"] == "1ABC"


def test_consolidate_extracted_writes_table_shards() -> None:
    tmp_path = _tmp_dir("consolidate_extracted")
    storage_root = tmp_path / "storage"
    extracted_dir = storage_root / "data" / "extracted"
    for table_name in ["entry", "chains", "bound_objects", "interfaces", "assays", "provenance"]:
        table_dir = extracted_dir / table_name
        table_dir.mkdir(parents=True, exist_ok=True)
        (table_dir / "1ABC.json").write_text(
            json.dumps({"pdb_id": "1ABC", "table": table_name}),
            encoding="utf-8",
        )

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
            "consolidate-extracted",
            "--run-id",
            "extracted_store_test",
            "--shard-size",
            "1",
        ],
        catch_exceptions=False,
    )

    assert result.exit_code == 0
    manifest_path = storage_root / "data" / "consolidated" / "extracted" / "extracted_store_test" / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["table_count"] == 6
    assert manifest["bundle_shards"][0]["tables"]["entry"].endswith("entry_shard_00000.jsonl.gz")
    entry_shard = storage_root / "data" / "consolidated" / "extracted" / "extracted_store_test" / "entry" / "entry_shard_00000.jsonl.gz"
    with gzip.open(entry_shard, "rt", encoding="utf-8") as handle:
        rows = [json.loads(line) for line in handle if line.strip()]
    assert rows == [{"pdb_id": "1ABC", "table": "entry", "payload": {"pdb_id": "1ABC", "table": "entry"}}]


def test_consolidated_extracted_manifest_falls_back_from_foreign_absolute_paths() -> None:
    tmp_path = _tmp_dir("consolidated_foreign_paths")
    storage_root = tmp_path / "storage"
    extracted_dir = storage_root / "data" / "extracted"
    for table_name in ["entry", "chains", "bound_objects", "interfaces", "assays", "provenance"]:
        table_dir = extracted_dir / table_name
        table_dir.mkdir(parents=True, exist_ok=True)
        (table_dir / "1ABC.json").write_text(json.dumps({"pdb_id": "1ABC", "table": table_name}), encoding="utf-8")

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
            "consolidate-extracted",
            "--run-id",
            "extracted_store_foreign_paths",
            "--shard-size",
            "1",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    manifest_path = storage_root / "data" / "consolidated" / "extracted" / "extracted_store_foreign_paths" / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["bundle_shards"][0]["tables"]["entry"] = (
        r"C:\Users\jfvit\Documents\bio-agent-lab\data\consolidated\extracted\extracted_store_foreign_paths\entry\entry_shard_00000.jsonl.gz"
    )
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    from pbdata.storage_packaging import iter_extracted_bundle_shards

    bundles = list(iter_extracted_bundle_shards(manifest_path.parent))
    assert bundles[0][1]["entry"][0]["pdb_id"] == "1ABC"
