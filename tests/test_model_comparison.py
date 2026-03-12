import json

from typer.testing import CliRunner

from pbdata.cli import app
from pbdata.config import AppConfig
from pbdata.gui_overview import build_gui_overview_snapshot
from pbdata.model_comparison import build_model_comparison_report, export_model_comparison_report
from pbdata.models.baseline_memory import evaluate_ligand_memory_model
from pbdata.models.tabular_affinity import evaluate_tabular_affinity_model
from pbdata.storage import build_storage_layout
from tests.test_baseline_memory import _write_training_fixture
from tests.test_feature_execution import _tmp_dir


def test_build_model_comparison_report_reads_existing_evaluations() -> None:
    layout = build_storage_layout(_tmp_dir("model_comparison"))
    _write_training_fixture(layout)
    evaluate_ligand_memory_model(layout)
    evaluate_tabular_affinity_model(layout)

    report = build_model_comparison_report(layout)

    assert report["status"] == "comparison_ready"
    assert report["available_models"]["baseline_ready"] is True
    assert report["available_models"]["tabular_ready"] is True
    assert "val" in report["splits"]


def test_export_model_comparison_report_and_gui_snapshot() -> None:
    layout = build_storage_layout(_tmp_dir("model_comparison_export"))
    _write_training_fixture(layout)
    evaluate_ligand_memory_model(layout)
    evaluate_tabular_affinity_model(layout)

    json_path, md_path, report = export_model_comparison_report(layout)
    snapshot = build_gui_overview_snapshot(layout, AppConfig(), repo_root=layout.root)

    assert json_path.exists()
    assert md_path.exists()
    saved = json.loads(json_path.read_text(encoding="utf-8"))
    assert saved["status"] == report["status"]
    assert snapshot.model_comparison_summary["status"] == report["status"]
    assert snapshot.model_comparison_kpis["baseline"] == "ready"


def test_report_model_comparison_cli() -> None:
    layout = build_storage_layout(_tmp_dir("model_comparison_cli"))
    _write_training_fixture(layout)
    evaluate_ligand_memory_model(layout)
    evaluate_tabular_affinity_model(layout)
    runner = CliRunner()

    result = runner.invoke(
        app,
        ["--storage-root", str(layout.root), "report-model-comparison"],
        catch_exceptions=False,
    )

    assert result.exit_code == 0
    assert "Model comparison JSON" in result.output
    assert (layout.reports_dir / "model_comparison.json").exists()
