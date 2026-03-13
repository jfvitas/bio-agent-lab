import json

from typer.testing import CliRunner

from pbdata.cli import app
from pbdata.config import AppConfig
from pbdata.sources.registry import build_source_capability_report, export_source_capability_report
from pbdata.storage import build_storage_layout
from tests.test_feature_execution import _tmp_dir


def test_build_source_capability_report_marks_missing_local_paths() -> None:
    layout = build_storage_layout(_tmp_dir("source_capability_report"))
    config = AppConfig.model_validate(
        {
            "sources": {
                "rcsb": {"enabled": True},
                "pdbbind": {"enabled": True, "extra": {}},
                "biolip": {"enabled": True, "extra": {"local_dir": "C:/data/biolip"}},
            }
        }
    )

    report = build_source_capability_report(layout, config)

    assert report["status"] == "needs_configuration"
    assert report["counts"]["enabled_sources"] == 5
    assert report["counts"]["implemented_sources"] >= 6
    assert report["counts"]["planned_sources"] >= 1
    assert report["counts"]["misconfigured_sources"] == 1
    pdbbind_row = next(row for row in report["sources"] if row["name"] == "pdbbind")
    assert pdbbind_row["status"] == "misconfigured_missing_local_path"
    uniprot_row = next(row for row in report["sources"] if row["name"] == "uniprot")
    assert uniprot_row["implementation_state"] == "implemented"
    interpro_row = next(row for row in report["sources"] if row["name"] == "interpro")
    cath_row = next(row for row in report["sources"] if row["name"] == "cath")
    assert interpro_row["implementation_state"] == "implemented"
    assert cath_row["implementation_state"] == "implemented"


def test_report_source_capabilities_cli_writes_artifacts() -> None:
    layout = build_storage_layout(_tmp_dir("source_capability_cli"))
    config = AppConfig()
    json_path, md_path, report = export_source_capability_report(layout, config)
    runner = CliRunner()

    result = runner.invoke(
        app,
        ["--storage-root", str(layout.root), "report-source-capabilities"],
        catch_exceptions=False,
    )

    assert json_path.exists()
    assert md_path.exists()
    saved = json.loads(json_path.read_text(encoding="utf-8"))
    assert saved["status"] == report["status"]
    assert result.exit_code == 0
    assert "Source capability JSON" in result.output
