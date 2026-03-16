import json
from pathlib import Path
from uuid import uuid4

from typer.testing import CliRunner

from pbdata.cli import app
from pbdata.config import AppConfig, SourceConfig, SourcesConfig
from pbdata.screening_field_audit import build_screening_field_audit
from pbdata.source_lifecycle import build_source_lifecycle_report
from pbdata.storage import build_storage_layout

_LOCAL_TMP = Path(__file__).parent / "_tmp"
_LOCAL_TMP.mkdir(exist_ok=True)


def _tmp_dir(name: str) -> Path:
    path = _LOCAL_TMP / f"{uuid4().hex}_{name}"
    path.mkdir(parents=True, exist_ok=True)
    return path


def test_build_source_lifecycle_report_detects_ready_and_missing_assets() -> None:
    root = _tmp_dir("source_lifecycle")
    layout = build_storage_layout(root)
    (root / "data_sources" / "bindingdb").mkdir(parents=True, exist_ok=True)
    (root / "data_sources" / "bindingdb" / "bindingdb.zip").write_text("zip", encoding="utf-8")
    (root / "data_sources" / "chembl").mkdir(parents=True, exist_ok=True)
    (root / "data_sources" / "chembl" / "chembl.db").write_text("db", encoding="utf-8")

    config = AppConfig(
        sources=SourcesConfig(
            bindingdb=SourceConfig(enabled=True, extra={"bulk_zip": str(root / "data_sources" / "bindingdb" / "bindingdb.zip")}),
            chembl=SourceConfig(enabled=True),
        )
    )

    report = build_source_lifecycle_report(layout, config)
    bindingdb = next(item for item in report["sources"] if item["name"] == "bindingdb")
    chembl = next(item for item in report["sources"] if item["name"] == "chembl")
    uniprot = next(item for item in report["sources"] if item["name"] == "uniprot")

    assert bindingdb["status"] == "ready"
    assert chembl["status"] == "ready"
    assert uniprot["status"] == "missing"


def test_build_source_lifecycle_report_accepts_alphafold_archive() -> None:
    root = _tmp_dir("alphafold_lifecycle")
    layout = build_storage_layout(root)
    (root / "data_sources" / "alphafold").mkdir(parents=True, exist_ok=True)
    archive_path = root / "data_sources" / "alphafold" / "swissprot_pdb_v6.tar"
    archive_path.write_text("tar", encoding="utf-8")

    config = AppConfig(
        sources=SourcesConfig(
            alphafold_db=SourceConfig(enabled=True, extra={"local_archive": str(archive_path)}),
        )
    )

    report = build_source_lifecycle_report(layout, config)
    alphafold = next(item for item in report["sources"] if item["name"] == "alphafold_db")

    assert alphafold["status"] == "ready"
    assert alphafold["assets"][0]["exists"] is True


def test_build_screening_field_audit_flags_sparse_and_empty_fields() -> None:
    root = _tmp_dir("screening_audit")
    layout = build_storage_layout(root)
    (root / "master_pdb_repository.csv").write_text(
        "\n".join(
            [
                "pdb_id,experimental_method,structure_resolution,organism_names,quality_score,source_databases",
                "1ABC,X-ray,1.8,Homo sapiens,0.9,BindingDB",
                "2DEF,,,,,",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    (root / "master_pdb_pairs.csv").write_text(
        "\n".join(
            [
                "pdb_id,pair_identity_key,source_database,receptor_uniprot_ids,ligand_types,matching_interface_types,binding_affinity_type,mutation_strings,source_conflict_summary,source_agreement_band,release_split",
                "1ABC,key1,BindingDB,P12345,small_molecule,protein_ligand,Kd,,conflict,medium,train",
                "2DEF,key2,,,,,,,,",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    report = build_screening_field_audit(layout)
    pair_table = next(item for item in report["tables"] if item["table_name"] == "master_pdb_pairs.csv")
    conflict_field = next(field for field in pair_table["fields"] if field["field"] == "source_conflict_summary")
    mutation_field = next(field for field in pair_table["fields"] if field["field"] == "mutation_strings")

    assert conflict_field["status"] == "partial"
    assert mutation_field["status"] == "empty"
    assert report["issue_count"] >= 1


def test_cli_reports_write_output_files() -> None:
    runner = CliRunner()
    root = _tmp_dir("cli_source_reports")
    layout = build_storage_layout(root)
    (root / "data_sources" / "bindingdb").mkdir(parents=True, exist_ok=True)
    (root / "data_sources" / "bindingdb" / "BDB-mySQL_All_202603_dmp.zip").write_text("zip", encoding="utf-8")
    (root / "master_pdb_repository.csv").write_text("pdb_id\n1ABC\n", encoding="utf-8")
    (root / "master_pdb_pairs.csv").write_text("pdb_id,pair_identity_key,source_database\n1ABC,key1,BindingDB\n", encoding="utf-8")
    (root / "configs").mkdir(parents=True, exist_ok=True)
    (root / "configs" / "sources.yaml").write_text(
        "\n".join(
            [
                "storage_root: " + str(root),
                "sources:",
                "  bindingdb:",
                "    enabled: true",
                "    extra:",
                "      bulk_zip: " + str(root / "data_sources" / "bindingdb" / "BDB-mySQL_All_202603_dmp.zip"),
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    config_path = root / "configs" / "sources.yaml"

    source_result = runner.invoke(
        app,
        ["--config", str(config_path), "--storage-root", str(root), "report-source-lifecycle"],
        catch_exceptions=False,
    )
    audit_result = runner.invoke(
        app,
        ["--config", str(config_path), "--storage-root", str(root), "audit-screening-fields"],
        catch_exceptions=False,
    )

    assert source_result.exit_code == 0
    assert audit_result.exit_code == 0
    assert (layout.reports_dir / "source_lifecycle_report.json").exists()
    assert (layout.reports_dir / "screening_field_audit.json").exists()
    payload = json.loads((layout.reports_dir / "source_lifecycle_report.json").read_text(encoding="utf-8"))
    assert payload["summary"]["tracked_sources"] >= 1
