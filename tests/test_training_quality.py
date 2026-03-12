import json

from typer.testing import CliRunner

from pbdata.cli import app
from pbdata.config import AppConfig
from pbdata.gui_overview import build_gui_overview_snapshot
from pbdata.storage import build_storage_layout
from pbdata.training_quality import build_training_set_quality_report, export_training_set_quality_report
from tests.test_baseline_memory import _write_training_fixture
from tests.test_feature_execution import _tmp_dir


def test_build_training_set_quality_report_counts_and_overlap() -> None:
    layout = build_storage_layout(_tmp_dir("training_quality"))
    _write_training_fixture(layout)

    report = build_training_set_quality_report(layout)

    assert report["counts"]["example_count"] == 3
    assert report["counts"]["supervised_count"] == 3
    assert report["split_counts"]["train"] == 2
    assert report["split_counts"]["val"] == 1
    assert report["overlap_with_train"]["val"]["same_target_in_train"] == 1
    assert report["overlap_with_train"]["val"]["same_ligand_in_train"] == 1
    assert report["overlap_with_train"]["val"]["exact_pair_seen_in_train"] == 0


def test_export_training_set_quality_report_and_gui_snapshot() -> None:
    layout = build_storage_layout(_tmp_dir("training_quality_export"))
    _write_training_fixture(layout)

    json_path, md_path, report = export_training_set_quality_report(layout)
    snapshot = build_gui_overview_snapshot(layout, AppConfig(), repo_root=layout.root)

    assert json_path.exists()
    assert md_path.exists()
    saved = json.loads(json_path.read_text(encoding="utf-8"))
    assert saved["status"] == report["status"]
    assert snapshot.training_quality_summary["status"] == report["status"]
    assert snapshot.training_quality_kpis["examples"] == "3"


def test_report_training_set_quality_cli() -> None:
    layout = build_storage_layout(_tmp_dir("training_quality_cli"))
    _write_training_fixture(layout)
    runner = CliRunner()

    result = runner.invoke(
        app,
        ["--storage-root", str(layout.root), "report-training-set-quality"],
        catch_exceptions=False,
    )

    assert result.exit_code == 0
    assert "Training quality JSON" in result.output
    assert (layout.reports_dir / "training_set_quality.json").exists()
