import json

from typer.testing import CliRunner

from pbdata.cli import app
from pbdata.identity_crosswalk import build_identity_crosswalk_report, export_identity_crosswalk
from pbdata.storage import build_storage_layout
from tests.test_feature_execution import _tmp_dir


def test_build_identity_crosswalk_report_uses_conservative_fallbacks() -> None:
    layout = build_storage_layout(_tmp_dir("identity_crosswalk"))
    (layout.extracted_dir / "chains").mkdir(parents=True, exist_ok=True)
    (layout.extracted_dir / "bound_objects").mkdir(parents=True, exist_ok=True)
    (layout.extracted_dir / "assays").mkdir(parents=True, exist_ok=True)

    (layout.extracted_dir / "chains" / "1ABC.json").write_text(
        json.dumps(
            [
                {
                    "pdb_id": "1ABC",
                    "chain_id": "A",
                    "is_protein": True,
                    "uniprot_id": "P12345",
                    "entity_source_organism": "Homo sapiens",
                    "chain_description": "Kinase",
                },
                {
                    "pdb_id": "1ABC",
                    "chain_id": "B",
                    "is_protein": True,
                    "uniprot_id": "",
                    "entity_source_organism": "Homo sapiens",
                    "chain_description": "Partner",
                },
            ]
        ),
        encoding="utf-8",
    )
    (layout.extracted_dir / "bound_objects" / "1ABC.json").write_text(
        json.dumps(
            [
                {
                    "pdb_id": "1ABC",
                    "component_id": "ATP",
                    "component_inchikey": "ATP-KEY",
                    "component_name": "ATP",
                    "component_smiles": "ATP-SMILES",
                    "component_type": "small_molecule",
                }
            ]
        ),
        encoding="utf-8",
    )
    (layout.extracted_dir / "assays" / "1ABC.json").write_text(
        json.dumps(
            [
                {
                    "pdb_id": "1ABC",
                    "pair_identity_key": "protein_ligand|1ABC|A|ATP-KEY|wt",
                    "binding_affinity_type": "Kd",
                    "source_database": "ChEMBL",
                },
                {
                    "pdb_id": "1ABC",
                    "pair_identity_key": "protein_ligand|1ABC|B|ATP-KEY|wt",
                    "binding_affinity_type": "Ki",
                    "source_database": "BindingDB",
                },
            ]
        ),
        encoding="utf-8",
    )

    report = build_identity_crosswalk_report(layout)

    assert report["status"] == "ready"
    assert report["counts"]["protein_identity_count"] == 2
    assert report["counts"]["ligand_identity_count"] == 1
    assert report["counts"]["pair_identity_count"] == 2
    assert report["counts"]["protein_fallback_count"] == 1
    assert report["counts"]["pair_exact_count"] == 1
    assert report["counts"]["pair_protein_partial_count"] == 1
    assert any(row["mapping_status"] == "exact" for row in report["pairs"])
    assert any(row["mapping_status"] == "partial" for row in report["pairs"])


def test_export_identity_crosswalk_cli_writes_outputs() -> None:
    layout = build_storage_layout(_tmp_dir("identity_crosswalk_cli"))
    (layout.extracted_dir / "chains").mkdir(parents=True, exist_ok=True)
    (layout.extracted_dir / "bound_objects").mkdir(parents=True, exist_ok=True)
    (layout.extracted_dir / "assays").mkdir(parents=True, exist_ok=True)
    (layout.extracted_dir / "chains" / "1ABC.json").write_text(
        json.dumps([{"pdb_id": "1ABC", "chain_id": "A", "is_protein": True, "uniprot_id": "P12345"}]),
        encoding="utf-8",
    )

    proteins_csv, ligands_csv, pairs_csv, summary_json, _ = export_identity_crosswalk(layout)
    runner = CliRunner()
    result = runner.invoke(
        app,
        ["--storage-root", str(layout.root), "export-identity-crosswalk"],
        catch_exceptions=False,
    )

    assert proteins_csv.exists()
    assert ligands_csv.exists()
    assert pairs_csv.exists()
    assert summary_json.exists()
    assert result.exit_code == 0
    assert "Protein crosswalk CSV" in result.output
