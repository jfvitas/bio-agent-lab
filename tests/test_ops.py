from typer.testing import CliRunner
from unittest.mock import patch

from pbdata.cli import app
from pbdata.config import AppConfig
from pbdata.demo import export_demo_snapshot
from pbdata.gui_overview import build_gui_overview_snapshot
from pbdata.models.baseline_memory import evaluate_ligand_memory_model
from pbdata.models.tabular_affinity import evaluate_tabular_affinity_model
from pbdata.ops import build_demo_readiness_report, build_doctor_report, build_status_report
from pbdata.storage import build_storage_layout
from pbdata.workspace_state import (
    build_demo_readiness_report as build_demo_readiness_state_report,
    build_doctor_report as build_doctor_state_report,
    build_status_report as build_status_state_report,
)
from tests.test_baseline_memory import _write_training_fixture
from tests.test_feature_execution import _tmp_dir


def test_status_and_doctor_reports_basic_fields() -> None:
    layout = build_storage_layout(_tmp_dir("ops_status"))
    status = build_status_report(layout)
    doctor = build_doctor_report(layout, AppConfig())
    demo = build_demo_readiness_report(layout, AppConfig())

    assert status["storage_root"] == str(layout.root)
    assert "raw_rcsb_count" in status
    assert "core_pipeline_ready" in status
    assert doctor["overall_status"] in {"ready", "missing_required_dependencies"}
    assert "dependency_checks" in doctor
    assert demo["readiness"] == "not_demo_ready"
    assert "no_extracted_entries" in demo["blockers"]
    assert build_status_state_report(layout).to_dict()["storage_root"] == str(layout.root)
    assert build_doctor_state_report(layout, AppConfig()).to_dict()["overall_status"] in {"ready", "missing_required_dependencies"}
    assert build_demo_readiness_state_report(layout, AppConfig()).to_dict()["readiness"] == "not_demo_ready"
    snapshot = build_gui_overview_snapshot(layout, AppConfig(), repo_root=layout.root)
    assert snapshot.demo_readiness.readiness == "not_demo_ready"
    assert "raw_rcsb" in snapshot.counts
    assert snapshot.workflow_guidance["phase"] == "Build core dataset"
    assert "Ingest Sources" in snapshot.workflow_guidance["step_1"]
    json_path, md_path, exported = export_demo_snapshot(layout, AppConfig())
    assert json_path.exists()
    assert md_path.exists()
    assert exported["readiness"] == demo["readiness"]


def test_gui_overview_snapshot_workflow_guidance_for_model_comparison() -> None:
    layout = build_storage_layout(_tmp_dir("ops_workflow_guidance"))
    _write_training_fixture(layout)
    (layout.extracted_dir / "entry").mkdir(parents=True, exist_ok=True)
    (layout.extracted_dir / "entry" / "1ABC.json").write_text("{}", encoding="utf-8")
    (layout.root / "model_ready_pairs.csv").write_text("pdb_id,pair_identity_key\n1ABC,protein_ligand|1ABC|A|ATP|wt\n", encoding="utf-8")
    evaluate_ligand_memory_model(layout)
    evaluate_tabular_affinity_model(layout)

    snapshot = build_gui_overview_snapshot(layout, AppConfig(), repo_root=layout.root)

    assert snapshot.workflow_guidance["phase"] == "Improve training quality"
    assert "Training Example Quality" in snapshot.workflow_guidance["step_1"]
    assert "rebuild training examples and splits" in snapshot.workflow_guidance["step_2"]


def test_gui_overview_snapshot_prediction_status_reads_manifest() -> None:
    layout = build_storage_layout(_tmp_dir("ops_prediction_status"))
    (layout.prediction_dir / "ligand_screening").mkdir(parents=True, exist_ok=True)
    (layout.prediction_dir / "ligand_screening" / "prediction_manifest.json").write_text(
        '{"status":"trained_supervised_predictions_generated","prediction_method":"trained_tabular_affinity_model","selected_model_preference":"tabular_affinity","candidate_target_count":2,"query_numeric_feature_count":4,"ranked_target_list":[{"target_id":"P12345","confidence_score":0.8123}],"notes":"Predictions use the supervised tabular affinity model."}',
        encoding="utf-8",
    )

    snapshot = build_gui_overview_snapshot(layout, AppConfig(), repo_root=layout.root)

    assert snapshot.prediction_status_summary["method"] == "trained_tabular_affinity_model"
    assert snapshot.prediction_status_summary["preference"] == "tabular_affinity"
    assert snapshot.prediction_status_kpis["top_target"] == "P12345"
    assert snapshot.prediction_status_kpis["query_features"] == "4"


def test_status_and_doctor_cli_commands() -> None:
    runner = CliRunner()
    tmp_path = _tmp_dir("ops_cli")

    status_result = runner.invoke(
        app,
        ["--storage-root", str(tmp_path), "status"],
        catch_exceptions=False,
    )
    doctor_result = runner.invoke(
        app,
        ["--storage-root", str(tmp_path), "doctor"],
        catch_exceptions=False,
    )
    demo_result = runner.invoke(
        app,
        ["--storage-root", str(tmp_path), "demo-readiness"],
        catch_exceptions=False,
    )
    export_result = runner.invoke(
        app,
        ["--storage-root", str(tmp_path), "export-demo-snapshot"],
        catch_exceptions=False,
    )

    assert status_result.exit_code == 0
    assert "Storage root" in status_result.output
    assert doctor_result.exit_code == 0
    assert "Overall status" in doctor_result.output
    assert demo_result.exit_code == 0
    assert "Demo readiness" in demo_result.output
    assert export_result.exit_code == 0
    assert "JSON snapshot" in export_result.output


def test_gui_cli_command_dispatches() -> None:
    runner = CliRunner()

    with patch("pbdata.gui.main") as mock_main:
        result = runner.invoke(app, ["gui"], catch_exceptions=False)

    assert result.exit_code == 0
    mock_main.assert_called_once()
