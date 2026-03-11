from typer.testing import CliRunner
from unittest.mock import patch

from pbdata.cli import app
from pbdata.config import AppConfig
from pbdata.ops import build_doctor_report, build_status_report
from pbdata.storage import build_storage_layout
from tests.test_feature_execution import _tmp_dir


def test_status_and_doctor_reports_basic_fields() -> None:
    layout = build_storage_layout(_tmp_dir("ops_status"))
    status = build_status_report(layout)
    doctor = build_doctor_report(layout, AppConfig())

    assert status["storage_root"] == str(layout.root)
    assert "raw_rcsb_count" in status
    assert doctor["overall_status"] in {"ready", "missing_required_dependencies"}
    assert "dependency_checks" in doctor


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

    assert status_result.exit_code == 0
    assert "Storage root" in status_result.output
    assert doctor_result.exit_code == 0
    assert "Overall status" in doctor_result.output


def test_gui_cli_command_dispatches() -> None:
    runner = CliRunner()

    with patch("pbdata.gui.main") as mock_main:
        result = runner.invoke(app, ["gui"], catch_exceptions=False)

    assert result.exit_code == 0
    mock_main.assert_called_once()
