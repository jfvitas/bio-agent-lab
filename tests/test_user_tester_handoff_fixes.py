import json

from typer.testing import CliRunner

from pbdata.cli import app
from pbdata.prediction.engine import run_peptide_binding_workflow
from pbdata.storage import build_storage_layout
from tests.test_feature_execution import _tmp_dir


def test_status_reports_processed_integrity_counts() -> None:
    layout = build_storage_layout(_tmp_dir("user_handoff_status"))
    layout.processed_rcsb_dir.mkdir(parents=True, exist_ok=True)
    (layout.processed_rcsb_dir / "empty.json").write_text("", encoding="utf-8")

    runner = CliRunner()
    result = runner.invoke(
        app,
        ["--storage-root", str(layout.root), "status"],
        catch_exceptions=False,
    )

    assert result.exit_code == 0
    assert "Processed valid" in result.output
    assert "Processed issues" in result.output
    assert "Processed health" in result.output


def test_clean_command_removes_empty_processed_files() -> None:
    layout = build_storage_layout(_tmp_dir("user_handoff_clean"))
    layout.processed_rcsb_dir.mkdir(parents=True, exist_ok=True)
    bad_path = layout.processed_rcsb_dir / "broken.json"
    bad_path.write_text("", encoding="utf-8")

    runner = CliRunner()
    result = runner.invoke(
        app,
        ["--storage-root", str(layout.root), "clean", "--processed", "--delete"],
        catch_exceptions=False,
    )

    assert result.exit_code == 0
    assert "Removed files: 1" in result.output
    assert not bad_path.exists()


def test_report_cli_aggregates_invalid_processed_summary() -> None:
    layout = build_storage_layout(_tmp_dir("user_handoff_report"))
    layout.processed_rcsb_dir.mkdir(parents=True, exist_ok=True)
    (layout.processed_rcsb_dir / "broken.json").write_text("", encoding="utf-8")

    runner = CliRunner()
    result = runner.invoke(
        app,
        ["--storage-root", str(layout.root), "report"],
        catch_exceptions=False,
    )

    assert result.exit_code == 0
    assert "Skipped invalid : 1" in result.output
    assert "broken.json" not in result.output


def test_feature_pipeline_lists_and_reports_valid_stage_names() -> None:
    runner = CliRunner()
    tmp_root = _tmp_dir("user_handoff_feature_stages")

    list_result = runner.invoke(
        app,
        ["--storage-root", str(tmp_root), "run-feature-pipeline", "--list-stages"],
        catch_exceptions=False,
    )
    bad_stage_result = runner.invoke(
        app,
        ["--storage-root", str(tmp_root), "run-feature-pipeline", "--run-mode", "stage_only", "--stage-name", "base_features"],
        catch_exceptions=False,
    )

    assert list_result.exit_code == 0
    assert "canonical_input_resolution" in list_result.output
    assert bad_stage_result.exit_code == 1
    assert "Valid stage names" in bad_stage_result.output


def test_peptide_binding_supports_fasta_sequence_only_mode() -> None:
    layout = build_storage_layout(_tmp_dir("user_handoff_peptide_fasta"))
    (layout.root / "master_pdb_pairs.csv").write_text(
        "pdb_id,pair_identity_key,receptor_uniprot_ids,source_database,selected_preferred_source,matching_interface_count\n"
        "EXAMPLE,protein_peptide|EXAMPLE|A|B|wt,P12345,BioLiP,BioLiP,2\n",
        encoding="utf-8",
    )

    out_path, manifest = run_peptide_binding_workflow(layout, fasta="ACDEFGHIK")

    assert out_path.exists()
    assert manifest["normalized_input_type"] == "FASTA"
    assert manifest["status"] == "baseline_sequence_only_predictions_generated"
    assert manifest["prediction_method"] == "baseline_sequence_support_lookup"
    assert manifest["predicted_targets"][0]["target_id"] == "P12345"


def test_pathway_risk_cli_shows_example_when_targets_missing() -> None:
    runner = CliRunner()
    tmp_root = _tmp_dir("user_handoff_risk_error")

    result = runner.invoke(
        app,
        ["--storage-root", str(tmp_root), "score-pathway-risk"],
        catch_exceptions=False,
    )

    assert result.exit_code == 1
    assert "Example: --targets P12345,Q99999" in result.output


def test_ligand_screening_manifest_includes_binding_call() -> None:
    layout = build_storage_layout(_tmp_dir("user_handoff_ligand_call"))
    (layout.root / "master_pdb_pairs.csv").write_text(
        "pdb_id,pair_identity_key,receptor_uniprot_ids,reported_measurement_mean_log10_standardized,source_conflict_flag,source_agreement_band,source_database,selected_preferred_source,ligand_key\n"
        "1ABC,protein_ligand|1ABC|A|ATP|wt,P12345,0.7,false,high,PDBbind,PDBbind,ATP\n",
        encoding="utf-8",
    )
    (layout.extracted_dir / "bound_objects").mkdir(parents=True, exist_ok=True)
    (layout.extracted_dir / "bound_objects" / "1ABC.json").write_text(
        json.dumps([{"component_id": "ATP", "component_smiles": "CCO"}]),
        encoding="utf-8",
    )

    runner = CliRunner()
    result = runner.invoke(
        app,
        ["--storage-root", str(layout.root), "predict-ligand-screening", "--smiles", "CCO"],
        catch_exceptions=False,
    )
    manifest = json.loads((layout.prediction_dir / "ligand_screening" / "prediction_manifest.json").read_text(encoding="utf-8"))

    assert result.exit_code == 0
    assert manifest["binding_call"] in {"strong_binder_predicted", "binder_predicted", "weak_binder_predicted", "unlikely_binder", "unknown"}
    assert "likely_binder" in manifest
