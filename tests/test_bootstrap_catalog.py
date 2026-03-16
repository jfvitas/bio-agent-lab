import gzip
import json
from pathlib import Path

from typer.testing import CliRunner

from pbdata.bootstrap_catalog import build_bootstrap_catalog
from pbdata.cli import app
from pbdata.storage import build_storage_layout
from pbdata.workspace_state import build_status_report
from tests.test_feature_execution import _tmp_dir, _write_extracted_fixture


def _write_master_exports(layout) -> None:
    (layout.root / "master_pdb_repository.csv").write_text(
        "\n".join(
            [
                "pdb_id,experimental_method,structure_resolution,organism_names",
                "1ABC,X-RAY DIFFRACTION,2.1,Homo sapiens",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    (layout.root / "master_pdb_pairs.csv").write_text(
        "\n".join(
            [
                "pdb_id,source_database,receptor_uniprot_ids,receptor_organisms,ligand_types,matching_interface_types,binding_affinity_type,mutation_strings",
                "1ABC,BindingDB,P12345,Homo sapiens,small_molecule,protein_ligand,Kd,",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

def _seed_local_files(layout) -> None:
    layout.raw_rcsb_dir.mkdir(parents=True, exist_ok=True)
    (layout.raw_rcsb_dir / "1ABC.json").write_text(
        json.dumps(
            {
                "rcsb_id": "1ABC",
                "rcsb_entry_info": {},
                "polymer_entities": [],
                "nonpolymer_entities": [],
            }
        ),
        encoding="utf-8",
    )
    layout.processed_rcsb_dir.mkdir(parents=True, exist_ok=True)
    (layout.processed_rcsb_dir / "1ABC.json").write_text(
        json.dumps({"pdb_id": "1ABC", "rcsb_id": "1ABC"}),
        encoding="utf-8",
    )
    layout.structures_rcsb_dir.mkdir(parents=True, exist_ok=True)
    (layout.structures_rcsb_dir / "1ABC.cif").write_text("data_1ABC\n#", encoding="utf-8")


def test_build_bootstrap_catalog_writes_manifest_and_shards() -> None:
    layout = build_storage_layout(_tmp_dir("bootstrap_catalog_build"))
    _write_extracted_fixture(layout)
    _write_master_exports(layout)
    _seed_local_files(layout)

    result = build_bootstrap_catalog(layout, shard_size=1, package_id="seed_startup")

    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    shard_path = Path(manifest["shards"][0]["path"])
    with gzip.open(shard_path, "rt", encoding="utf-8") as handle:
        rows = [json.loads(line) for line in handle if line.strip()]

    assert result.package_id == "seed_startup"
    assert manifest["record_type"] == "bootstrap_catalog"
    assert manifest["summary"]["record_count"] >= 1
    assert manifest["update_policy"]["mode"] == "staged_bootstrap_then_targeted_refresh"
    assert rows[0]["pdb_id"] == "1ABC"
    assert rows[0]["source_databases"] == ["BindingDB"]
    assert rows[0]["has_structure_file"] is True
    assert rows[0]["has_processed_record"] is True


def test_build_bootstrap_catalog_cli_and_status_report() -> None:
    runner = CliRunner()
    root = _tmp_dir("bootstrap_catalog_cli")
    layout = build_storage_layout(root)
    _write_extracted_fixture(layout)
    _write_master_exports(layout)
    _seed_local_files(layout)

    result = runner.invoke(
        app,
        ["--storage-root", str(root), "build-bootstrap-catalog", "--shard-size", "2"],
        catch_exceptions=False,
    )

    assert result.exit_code == 0
    assert "Bootstrap package" in result.output

    status = build_status_report(layout)
    assert status.bootstrap_catalog_present is True
    assert status.bootstrap_catalog_package_count == 1
