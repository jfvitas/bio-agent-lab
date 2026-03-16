import json
import sqlite3
import gzip
import csv

from pbdata.config import AppConfig
from typer.testing import CliRunner

from pbdata.bootstrap_store import (
    execute_selected_pdb_refresh,
    export_bootstrap_summary,
    materialize_bootstrap_store,
    plan_selected_pdb_refresh,
)
from pbdata.cli import app
from pbdata.storage import build_storage_layout
from pbdata.workspace_state import build_status_report
from tests.test_bootstrap_catalog import _seed_local_files, _write_master_exports
from tests.test_feature_execution import _tmp_dir, _write_extracted_fixture


def _write_selected_training_set(layout) -> None:
    (layout.root / "custom_training_set.csv").write_text(
        "\n".join(
            [
                "selection_rank,selection_mode,pdb_id,pair_identity_key,source_database,receptor_uniprot_ids,release_split",
                "1,generalist,1ABC,protein_ligand|1ABC|A|LIG|wt,BindingDB,P12345,train",
            ]
        )
        + "\n",
        encoding="utf-8",
    )


def test_materialize_bootstrap_store_writes_sqlite_index() -> None:
    layout = build_storage_layout(_tmp_dir("bootstrap_store_build"))
    _write_extracted_fixture(layout)
    _write_master_exports(layout)
    _seed_local_files(layout)

    result = materialize_bootstrap_store(layout)

    with sqlite3.connect(result.database_path) as connection:
        row = connection.execute(
            "SELECT pdb_id, has_processed_record, has_structure_file FROM bootstrap_catalog WHERE pdb_id = ?",
            ("1ABC",),
        ).fetchone()

    assert row == ("1ABC", 1, 1)
    assert result.manifest_path.exists()


def test_plan_selected_pdb_refresh_writes_manifest() -> None:
    layout = build_storage_layout(_tmp_dir("bootstrap_refresh_plan"))
    _write_extracted_fixture(layout)
    _write_master_exports(layout)
    _seed_local_files(layout)
    _write_selected_training_set(layout)

    result = plan_selected_pdb_refresh(layout)
    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))

    assert manifest["record_count"] == 1
    assert manifest["records"][0]["pdb_id"] == "1ABC"
    assert manifest["records"][0]["missing_local_assets"] == []


def test_export_bootstrap_summary_writes_lightweight_package() -> None:
    layout = build_storage_layout(_tmp_dir("bootstrap_summary_export"))
    _write_extracted_fixture(layout)
    _write_master_exports(layout)
    _seed_local_files(layout)
    materialize_bootstrap_store(layout)

    result = export_bootstrap_summary(layout)

    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    with gzip.open(result.csv_path, "rt", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))

    assert result.record_count == 1
    assert manifest["github_friendly"] is True
    assert rows[0]["pdb_id"] == "1ABC"
    assert rows[0]["source_databases"] == "BindingDB"
    assert rows[0]["has_bindingdb"] == "True"
    assert rows[0]["bootstrap_ready"] == "True"


def test_bootstrap_store_cli_and_status_flags() -> None:
    runner = CliRunner()
    root = _tmp_dir("bootstrap_store_cli")
    layout = build_storage_layout(root)
    _write_extracted_fixture(layout)
    _write_master_exports(layout)
    _seed_local_files(layout)
    _write_selected_training_set(layout)

    store_result = runner.invoke(
        app,
        ["--storage-root", str(root), "materialize-bootstrap-store"],
        catch_exceptions=False,
    )
    refresh_result = runner.invoke(
        app,
        ["--storage-root", str(root), "plan-selected-pdb-refresh"],
        catch_exceptions=False,
    )

    assert store_result.exit_code == 0
    assert refresh_result.exit_code == 0
    assert "Bootstrap store" in store_result.output
    assert "Refresh manifest" in refresh_result.output

    status = build_status_report(layout)
    assert status.bootstrap_store_present is True
    assert status.bootstrap_refresh_manifest_present is True


def test_execute_selected_pdb_refresh_updates_workspace_artifacts(monkeypatch) -> None:
    layout = build_storage_layout(_tmp_dir("bootstrap_refresh_execute"))
    _write_extracted_fixture(layout)
    _write_master_exports(layout)
    _seed_local_files(layout)
    _write_selected_training_set(layout)
    plan_selected_pdb_refresh(layout)

    class _FakeNormalized:
        def model_dump_json(self, indent: int = 2) -> str:
            return json.dumps({"pdb_id": "1ABC", "source": "normalized"}, indent=indent)

    raw_payload = {
        "rcsb_id": "1ABC",
        "polymer_entities": [],
        "nonpolymer_entities": [],
        "rcsb_entry_info": {},
        "rcsb_accession_info": {},
        "struct": {},
        "struct_keywords": {},
        "exptl": [],
    }

    monkeypatch.setattr(
        "pbdata.bootstrap_store.fetch_entries_batch",
        lambda pdb_ids: [dict(raw_payload, rcsb_id=pdb_ids[0])],
    )
    monkeypatch.setattr(
        "pbdata.bootstrap_store.fetch_mmcif_supplement",
        lambda pdb_id, structures_dir=None: {"water_count": 0},
    )
    monkeypatch.setattr(
        "pbdata.bootstrap_store.download_structure_files",
        lambda pdb_id, structures_dir=None, download_pdb=False: {
            "structure_file_cif_path": str(layout.structures_rcsb_dir / f"{pdb_id}.cif")
        },
    )
    monkeypatch.setattr("pbdata.bootstrap_store.fetch_chemcomp_descriptors", lambda comp_ids: {})
    monkeypatch.setattr("pbdata.bootstrap_store.load_external_assay_samples", lambda config, layout: {})
    monkeypatch.setattr("pbdata.bootstrap_store.fetch_bindingdb_samples_for_pdb", lambda *args, **kwargs: [])
    monkeypatch.setattr("pbdata.bootstrap_store.fetch_chembl_samples_for_raw", lambda *args, **kwargs: [])
    monkeypatch.setattr(
        "pbdata.bootstrap_store.extract_rcsb_entry",
        lambda raw, **kwargs: {
            "entry": type("EntryStub", (), {"pdb_id": raw["rcsb_id"], "model_dump": lambda self, mode="json": {"pdb_id": raw["rcsb_id"]}})(),
            "chains": [],
            "bound_objects": [],
            "interfaces": [],
            "assays": [],
            "provenance": [],
        },
    )
    monkeypatch.setattr(
        "pbdata.bootstrap_store.write_records_json",
        lambda records, output_dir: (output_dir / "entry").mkdir(parents=True, exist_ok=True) or (output_dir / "entry" / f"{records['entry'].pdb_id}.json").write_text("{}", encoding="utf-8"),
    )
    monkeypatch.setattr(
        "pbdata.bootstrap_store.refresh_master_exports",
        lambda layout: {"master_csv": str(layout.root / "master_pdb_repository.csv")},
    )
    monkeypatch.setattr(
        "pbdata.bootstrap_store.RCSBAdapter",
        lambda: type("AdapterStub", (), {"normalize_record": lambda self, raw, chem_descriptors=None: _FakeNormalized()})(),
    )

    result = execute_selected_pdb_refresh(layout, AppConfig(), only_missing_assets=False)

    assert result.refreshed_count == 1
    assert result.failed_count == 0
    assert (layout.raw_rcsb_dir / "1ABC.json").exists()
    assert (layout.processed_rcsb_dir / "1ABC.json").exists()
    assert result.execution_report_path.exists()


def test_refresh_selected_pdbs_cli_uses_execution_result(monkeypatch) -> None:
    runner = CliRunner()
    root = _tmp_dir("bootstrap_refresh_cli")
    layout = build_storage_layout(root)
    layout.bootstrap_store_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = layout.bootstrap_store_dir / "selected_pdb_refresh_manifest.json"
    manifest_path.write_text(json.dumps({"records": [{"pdb_id": "1ABC"}]}), encoding="utf-8")

    monkeypatch.setattr(
        "pbdata.bootstrap_store.execute_selected_pdb_refresh",
        lambda layout, config, manifest_path=None, limit=None, only_missing_assets=True, with_live_enrichment=False: type(
            "RefreshResult",
            (),
            {
                "manifest_path": manifest_path or (layout.bootstrap_store_dir / "selected_pdb_refresh_manifest.json"),
                "execution_report_path": layout.bootstrap_store_dir / "selected_pdb_refresh_execution.json",
                "refreshed_count": 1,
                "skipped_count": 0,
                "failed_count": 0,
                "failed_pdb_ids": (),
                "export_status": {},
            },
        )(),
    )

    result = runner.invoke(
        app,
        ["--storage-root", str(root), "refresh-selected-pdbs"],
        catch_exceptions=False,
    )

    assert result.exit_code == 0
    assert "Refreshed PDBs" in result.output
