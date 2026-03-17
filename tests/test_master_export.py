import csv
import json
from pathlib import Path
from uuid import uuid4

from typer.testing import CliRunner

from pbdata.cli import app
from pbdata.master_export import (
    export_conflict_repository_csv,
    export_issue_repository_csv,
    export_master_pair_repository_csv,
    export_master_repository_csv,
    export_source_state_csv,
)
from pbdata.schemas.records import EntryRecord
from pbdata.storage import build_storage_layout

_LOCAL_TMP = Path(__file__).parent / "_tmp"
_LOCAL_TMP.mkdir(exist_ok=True)


def _tmp_dir(name: str) -> Path:
    path = _LOCAL_TMP / f"{uuid4().hex}_{name}"
    path.mkdir(exist_ok=True)
    return path


def test_export_master_repository_csv_flattens_extracted_tables() -> None:
    tmp_root = _tmp_dir("master_export")
    layout = build_storage_layout(tmp_root / "storage")
    (layout.extracted_dir / "entry").mkdir(parents=True)
    (layout.extracted_dir / "chains").mkdir(parents=True)
    (layout.extracted_dir / "bound_objects").mkdir(parents=True)
    (layout.extracted_dir / "interfaces").mkdir(parents=True)
    (layout.extracted_dir / "assays").mkdir(parents=True)

    (layout.extracted_dir / "entry" / "1ABC.json").write_text(json.dumps({
        "pdb_id": "1ABC",
        "title": "Example entry",
        "experimental_method": "X-RAY DIFFRACTION",
        "structure_resolution": 2.1,
        "quality_flags": ["cofactor_present"],
        "field_provenance": {"title": {"source": "RCSB"}},
        "field_confidence": {"title": "high"},
    }), encoding="utf-8")
    (layout.extracted_dir / "chains" / "1ABC.json").write_text(json.dumps([{
        "pdb_id": "1ABC",
        "chain_id": "A",
        "is_protein": True,
        "uniprot_id": "P12345",
        "entity_source_organism": "Homo sapiens",
    }]), encoding="utf-8")
    (layout.extracted_dir / "bound_objects" / "1ABC.json").write_text(json.dumps([{
        "pdb_id": "1ABC",
        "component_id": "ATP",
        "component_name": "ATP",
        "component_inchikey": "AAAA-BBBB",
        "component_type": "cofactor",
    }]), encoding="utf-8")
    (layout.extracted_dir / "interfaces" / "1ABC.json").write_text(json.dumps([{
        "pdb_id": "1ABC",
        "interface_type": "protein_ligand",
    }]), encoding="utf-8")
    (layout.extracted_dir / "assays" / "1ABC.json").write_text(json.dumps([{
        "pdb_id": "1ABC",
        "source_database": "PDBbind",
        "binding_affinity_type": "Kd",
        "pair_identity_key": "protein_ligand|1ABC|A|AAAA-BBBB|wildtype",
        "reported_measurements_text": "PDBbind:Kd=5 nM",
    }]), encoding="utf-8")

    out_path = export_master_repository_csv(layout, repo_root=tmp_root)

    with out_path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))

    assert out_path == tmp_root / "master_pdb_repository.csv"
    assert len(rows) == 1
    row = rows[0]
    assert row["pdb_id"] == "1ABC"
    assert row["protein_chain_ids"] == "A"
    assert row["protein_chain_uniprot_ids"] == "P12345"
    assert row["organism_names"] == "Homo sapiens"
    assert float(row["quality_score"]) > 0.7
    assert row["ligand_component_ids"] == "ATP"
    assert row["source_databases"] == "PDBbind"
    assert row["has_ligand_signal"] == "true"
    assert row["has_protein_signal"] == "true"
    assert row["assay_sources"] == "PDBbind"
    assert row["assay_types"] == "Kd"
    assert "RCSB" in row["field_provenance_json"]


def test_export_master_repository_csv_skips_unreadable_rows() -> None:
    tmp_root = _tmp_dir("master_export_unreadable")
    layout = build_storage_layout(tmp_root / "storage")
    (layout.extracted_dir / "entry").mkdir(parents=True)
    (layout.extracted_dir / "chains").mkdir(parents=True)
    (layout.extracted_dir / "bound_objects").mkdir(parents=True)
    (layout.extracted_dir / "interfaces").mkdir(parents=True)
    (layout.extracted_dir / "assays").mkdir(parents=True)
    layout.features_dir.mkdir(parents=True)
    layout.training_dir.mkdir(parents=True)

    (layout.extracted_dir / "entry" / "1ABC.json").write_text(json.dumps({
        "pdb_id": "1ABC",
        "title": "Example entry",
    }), encoding="utf-8")
    (layout.extracted_dir / "entry" / "broken.json").write_text("{invalid", encoding="utf-8")
    (layout.features_dir / "feature_records.json").write_text("{invalid", encoding="utf-8")

    out_path = export_master_repository_csv(layout, repo_root=tmp_root)

    with out_path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))

    assert len(rows) == 1
    assert rows[0]["pdb_id"] == "1ABC"


def test_export_master_pair_repository_csv_flattens_pair_rows() -> None:
    tmp_root = _tmp_dir("pair_export")
    layout = build_storage_layout(tmp_root / "storage")
    (layout.extracted_dir / "entry").mkdir(parents=True)
    (layout.extracted_dir / "chains").mkdir(parents=True)
    (layout.extracted_dir / "bound_objects").mkdir(parents=True)
    (layout.extracted_dir / "interfaces").mkdir(parents=True)
    (layout.extracted_dir / "assays").mkdir(parents=True)
    layout.features_dir.mkdir(parents=True)
    layout.training_dir.mkdir(parents=True)

    (layout.extracted_dir / "entry" / "1ABC.json").write_text(json.dumps({
        "pdb_id": "1ABC",
        "title": "Example entry",
        "experimental_method": "X-RAY DIFFRACTION",
        "structure_resolution": 2.1,
        "membrane_vs_soluble": "soluble",
        "quality_flags": ["cofactor_present"],
    }), encoding="utf-8")
    (layout.extracted_dir / "chains" / "1ABC.json").write_text(json.dumps([{
        "pdb_id": "1ABC",
        "chain_id": "A",
        "is_protein": True,
        "uniprot_id": "P12345",
        "entity_source_organism": "Homo sapiens",
    }]), encoding="utf-8")
    (layout.extracted_dir / "bound_objects" / "1ABC.json").write_text(json.dumps([{
        "pdb_id": "1ABC",
        "component_id": "ATP",
        "component_name": "ATP",
        "component_inchikey": "AAAA-BBBB",
        "component_type": "cofactor",
    }]), encoding="utf-8")
    (layout.extracted_dir / "interfaces" / "1ABC.json").write_text(json.dumps([{
        "pdb_id": "1ABC",
        "interface_type": "protein_ligand",
        "binding_site_chain_ids": ["A"],
        "entity_name_b": "AAAA-BBBB",
    }]), encoding="utf-8")
    pair_key = "protein_ligand|1ABC|A|AAAA-BBBB|wildtype"
    (layout.extracted_dir / "assays" / "1ABC.json").write_text(json.dumps([{
        "pdb_id": "1ABC",
        "source_database": "PDBbind",
        "binding_affinity_type": "Kd",
        "binding_affinity_value": 5.0,
        "binding_affinity_unit": "nM",
        "binding_affinity_log10_standardized": 0.69897,
        "pair_identity_key": pair_key,
        "reported_measurements_text": "PDBbind:Kd=5 nM",
        "reported_measurement_count": 1,
        "source_conflict_flag": False,
        "source_conflict_summary": "agreement_within_0.3_log10",
        "source_agreement_band": "high",
        "selected_preferred_source": "PDBbind",
        "field_provenance": {"binding_affinity_value": {"source": "PDBbind"}},
        "field_confidence": {"binding_affinity_value": "high"},
    }]), encoding="utf-8")
    (layout.features_dir / "feature_records.json").write_text(json.dumps([{
        "pdb_id": "1ABC",
        "pair_identity_key": pair_key,
        "values": {},
    }]), encoding="utf-8")
    (layout.training_dir / "training_examples.json").write_text(json.dumps([{
        "structure": {"pdb_id": "1ABC"},
        "provenance": {"pair_identity_key": pair_key},
    }]), encoding="utf-8")

    out_path = export_master_pair_repository_csv(layout, repo_root=tmp_root)

    with out_path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))

    assert out_path == tmp_root / "master_pdb_pairs.csv"
    assert len(rows) == 1
    row = rows[0]
    assert row["pdb_id"] == "1ABC"
    assert row["pair_identity_key"] == pair_key
    assert row["receptor_chain_ids"] == "A"
    assert row["receptor_uniprot_ids"] == "P12345"
    assert row["ligand_inchikeys"] == "AAAA-BBBB"
    assert row["matching_interface_count"] == "1"
    assert row["feature_record_count"] == "1"
    assert row["training_example_count"] == "1"
    assert "PDBbind" in row["assay_field_provenance_json"]
    assert row["selected_preferred_source"] == "PDBbind"


def test_export_master_pair_repository_csv_populates_policy_fields_from_live_inputs() -> None:
    tmp_root = _tmp_dir("pair_export_policy_fields")
    layout = build_storage_layout(tmp_root / "storage")
    (layout.extracted_dir / "entry").mkdir(parents=True)
    (layout.extracted_dir / "chains").mkdir(parents=True)
    (layout.extracted_dir / "bound_objects").mkdir(parents=True)
    (layout.extracted_dir / "interfaces").mkdir(parents=True)
    (layout.extracted_dir / "assays").mkdir(parents=True)
    layout.features_dir.mkdir(parents=True)
    layout.training_dir.mkdir(parents=True)
    layout.splits_dir.mkdir(parents=True)

    pair_key = "protein_ligand|1ABC|A|ATP|wt"
    (layout.extracted_dir / "entry" / "1ABC.json").write_text(json.dumps({
        "pdb_id": "1ABC",
        "title": "Example entry",
    }), encoding="utf-8")
    (layout.extracted_dir / "chains" / "1ABC.json").write_text(json.dumps([{
        "pdb_id": "1ABC",
        "chain_id": "A",
        "is_protein": True,
        "uniprot_id": "P12345",
    }]), encoding="utf-8")
    (layout.extracted_dir / "bound_objects" / "1ABC.json").write_text(json.dumps([{
        "pdb_id": "1ABC",
        "component_id": "ATP",
        "component_name": "ATP",
        "component_inchikey": "ATP-KEY",
        "component_type": "small_molecule",
    }]), encoding="utf-8")
    (layout.extracted_dir / "interfaces" / "1ABC.json").write_text(json.dumps([{
        "pdb_id": "1ABC",
        "interface_type": "protein_ligand",
        "binding_site_chain_ids": ["A"],
        "entity_name_b": "ATP",
    }]), encoding="utf-8")
    (layout.extracted_dir / "assays" / "1ABC.json").write_text(json.dumps([{
        "pdb_id": "1ABC",
        "pair_identity_key": pair_key,
        "source_database": "BindingDB",
        "binding_affinity_type": "Kd",
        "binding_affinity_value": 5.0,
        "binding_affinity_unit": "nM",
        "reported_measurement_count": 1,
        "selected_preferred_source": "BindingDB",
    }]), encoding="utf-8")
    (layout.splits_dir / "train.txt").write_text(f"{pair_key}|Kd\n", encoding="utf-8")

    out_path = export_master_pair_repository_csv(layout, repo_root=tmp_root)

    with out_path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))

    assert len(rows) == 1
    row = rows[0]
    assert row["mutation_strings"] == "wt"
    assert row["source_conflict_summary"] == "single_measurement_no_cross_source_conflict_assessment"
    assert row["source_agreement_band"] == "not_assessed_single_source"
    assert row["release_split"] == "train"


def test_export_issue_repository_csv_reports_review_gaps() -> None:
    tmp_root = _tmp_dir("issue_export")
    layout = build_storage_layout(tmp_root / "storage")
    (layout.extracted_dir / "entry").mkdir(parents=True)
    (layout.extracted_dir / "bound_objects").mkdir(parents=True)
    (layout.extracted_dir / "assays").mkdir(parents=True)

    (layout.extracted_dir / "entry" / "1ABC.json").write_text(json.dumps({
        "pdb_id": "1ABC",
        "field_confidence": {"membrane_vs_soluble": "medium"},
    }), encoding="utf-8")
    (layout.extracted_dir / "bound_objects" / "1ABC.json").write_text(json.dumps([{
        "pdb_id": "1ABC",
        "component_id": "ATP",
        "component_type": "small_molecule",
        "component_smiles": None,
        "component_inchikey": None,
    }]), encoding="utf-8")

    # Pair CSV is also part of issue generation.
    (tmp_root / "master_pdb_pairs.csv").write_text(
        "pdb_id,pair_identity_key,matching_interface_count,source_conflict_flag,source_conflict_summary,assay_field_confidence_json\n"
        "1ABC,protein_ligand|1ABC|A|ATP|mutation_unknown:1,0,true,high_conflict_spread=1.200,\"{\"\"binding_affinity_log10_standardized\"\":\"\"medium\"\"}\"\n",
        encoding="utf-8",
    )

    out_path = export_issue_repository_csv(layout, repo_root=tmp_root)
    with out_path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))

    issue_types = {row["issue_type"] for row in rows}
    assert "missing_structure_file" in issue_types
    assert "no_assay_data" in issue_types
    assert "non_high_confidence_fields" in issue_types
    assert "missing_ligand_descriptors" in issue_types
    assert "no_matched_interface" in issue_types
    assert "ambiguous_mutation_context" in issue_types
    assert "non_high_confidence_assay_fields" in issue_types
    assert "source_value_conflict" in issue_types


def test_export_issue_repository_csv_keeps_non_mutant_override_cases_advisory() -> None:
    tmp_root = _tmp_dir("issue_export_non_mutant_override")
    layout = build_storage_layout(tmp_root / "storage")
    (layout.extracted_dir / "entry").mkdir(parents=True)
    (layout.extracted_dir / "bound_objects").mkdir(parents=True)
    (layout.extracted_dir / "assays").mkdir(parents=True)

    (layout.extracted_dir / "entry" / "1ABC.json").write_text(json.dumps({
        "pdb_id": "1ABC",
        "structure_file_cif_path": "/tmp/1ABC.cif",
        "field_confidence": {},
    }), encoding="utf-8")

    (tmp_root / "master_pdb_pairs.csv").write_text(
        "pdb_id,pair_identity_key,matching_interface_count,source_conflict_flag,source_conflict_summary,binding_affinity_is_mutant_measurement,assay_field_provenance_json,assay_field_confidence_json\n"
        "1ABC,protein_ligand|1ABC|A|ATP|mutation_unknown:CHEMBL1,1,false,,false,\"{\"\"pair_identity_key\"\":{\"\"override_used\"\":true}}\",\"{\"\"pair_identity_key\"\":\"\"medium\"\",\"\"binding_affinity_value\"\":\"\"medium\"\"}\"\n",
        encoding="utf-8",
    )

    out_path = export_issue_repository_csv(layout, repo_root=tmp_root)
    with out_path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))

    issue_types = {row["issue_type"] for row in rows}
    assert "ambiguous_mutation_context" not in issue_types
    assert "non_high_confidence_assay_fields" not in issue_types
    assert "advisory_non_high_confidence_assay_fields" in issue_types


def test_export_conflict_repository_csv_filters_to_conflicted_pairs() -> None:
    tmp_root = _tmp_dir("conflict_export")
    layout = build_storage_layout(tmp_root / "storage")
    (tmp_root / "master_pdb_pairs.csv").write_text(
        "pdb_id,pair_identity_key,binding_affinity_type,source_database,reported_measurements_text,reported_measurement_count,reported_measurement_mean_log10_standardized,source_conflict_flag,source_conflict_summary,source_agreement_band,selected_preferred_source,measurement_source_reference,measurement_source_publication,measurement_source_doi,measurement_source_pubmed_id,assay_field_provenance_json,assay_field_confidence_json\n"
        "1ABC,protein_ligand|1ABC|A|ATP|wildtype,Kd,PDBbind,\"PDBbind:Kd=5 nM; ChEMBL:Kd=500 nM\",2,1.2,true,high_conflict_spread=2.000,low,PDBbind,ref,pub,doi,pmid,\"{\"\"binding_affinity_value\"\":{\"\"source\"\":\"\"PDBbind\"\"}}\",\"{\"\"binding_affinity_value\"\":\"\"medium\"\"}\"\n"
        "2DEF,protein_ligand|2DEF|A|HEM|wildtype,Kd,PDBbind,\"PDBbind:Kd=5 nM\",1,0.7,false,agreement_within_0.3_log10,high,PDBbind,ref,pub,doi,pmid,\"{}\",\"{}\"\n",
        encoding="utf-8",
    )

    out_path = export_conflict_repository_csv(layout, repo_root=tmp_root)
    with out_path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))

    assert out_path == tmp_root / "master_pdb_conflicts.csv"
    assert len(rows) == 1
    row = rows[0]
    assert row["pdb_id"] == "1ABC"
    assert row["selected_preferred_source"] == "PDBbind"
    assert row["source_conflict_summary"] == "high_conflict_spread=2.000"


def test_export_source_state_csv_flattens_source_manifests() -> None:
    tmp_root = _tmp_dir("source_state_export")
    layout = build_storage_layout(tmp_root / "storage")
    layout.source_state_dir.mkdir(parents=True, exist_ok=True)
    (layout.source_state_dir / "bindingdb.json").write_text(json.dumps({
        "source_name": "BindingDB",
        "status": "ready",
        "mode": "local_cache",
        "record_id": "1ABC",
        "record_count": 2,
        "cache_path": "C:/cache/1ABC.json",
        "generated_at": "2026-03-08T00:00:00+00:00",
        "notes": "BindingDB enrichment loaded and normalized.",
        "extra": {
            "configured_local_dir": "C:/cache",
            "attempt_count": 3,
            "total_records_observed": 5,
            "status_counts": {"ready": 2, "error": 1},
        },
    }), encoding="utf-8")

    out_path = export_source_state_csv(layout, repo_root=tmp_root)
    with out_path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))

    assert out_path == tmp_root / "master_source_state.csv"
    assert len(rows) == 1
    assert rows[0]["source_name"] == "BindingDB"
    assert rows[0]["mode"] == "local_cache"
    assert rows[0]["configured_local_dir"] == "C:/cache"
    assert rows[0]["attempt_count"] == "3"
    assert rows[0]["total_records_observed"] == "5"
    assert "ready" in rows[0]["status_counts"]


def test_extract_cli_refreshes_root_master_csv() -> None:
    tmp_root = _tmp_dir("master_export_cli")
    storage_root = tmp_root / "storage"
    raw_dir = storage_root / "data" / "raw" / "rcsb"
    raw_dir.mkdir(parents=True)
    (raw_dir / "1ABC.json").write_text('{"rcsb_id":"1ABC","nonpolymer_entities":[]}', encoding="utf-8")

    def _fake_extract_rcsb_entry(*_args, **_kwargs):
        return {
            "entry": EntryRecord(
                source_database="RCSB",
                source_record_id="1ABC",
                pdb_id="1ABC",
                title="Example entry",
            ),
            "chains": [],
            "bound_objects": [],
            "interfaces": [],
            "assays": [],
            "provenance": [],
        }

    runner = CliRunner()
    original_cwd = Path.cwd()
    try:
        import os
        from unittest.mock import patch

        os.chdir(tmp_root)
        with patch("pbdata.cli._load_external_assay_samples", return_value={}), patch(
            "pbdata.pipeline.extract.extract_rcsb_entry",
            side_effect=_fake_extract_rcsb_entry,
        ):
            result = runner.invoke(
                app,
                ["--storage-root", str(storage_root), "extract", "--no-download-structures"],
                catch_exceptions=False,
            )
    finally:
        os.chdir(original_cwd)

    assert result.exit_code == 0
    master_csv = tmp_root / "master_pdb_repository.csv"
    pair_csv = tmp_root / "master_pdb_pairs.csv"
    issue_csv = tmp_root / "master_pdb_issues.csv"
    conflict_csv = tmp_root / "master_pdb_conflicts.csv"
    source_state_csv = tmp_root / "master_source_state.csv"
    model_ready_csv = tmp_root / "model_ready_pairs.csv"
    release_manifest = tmp_root / "dataset_release_manifest.json"
    assert master_csv.exists()
    assert pair_csv.exists()
    assert issue_csv.exists()
    assert conflict_csv.exists()
    assert source_state_csv.exists()
    assert model_ready_csv.exists()
    assert release_manifest.exists()
    assert "Master CSV:" in result.output
    assert "Pair CSV:" in result.output
    assert "Issue CSV:" in result.output
    assert "Conflict CSV:" in result.output
    assert "Source State CSV:" in result.output
    assert "Model-ready CSV:" in result.output
    assert "Release Manifest:" in result.output
