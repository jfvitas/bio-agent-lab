from typer.testing import CliRunner

from pbdata.cli import app
from pbdata.release_export import build_release_readiness_report
from pbdata.storage import build_storage_layout
from tests.test_feature_execution import _tmp_dir


def test_release_readiness_reports_blockers_when_release_surface_is_empty() -> None:
    tmp_path = _tmp_dir("release_readiness_empty")
    layout = build_storage_layout(tmp_path)
    out_path, report = build_release_readiness_report(layout, repo_root=tmp_path)

    assert out_path.exists()
    assert report["release_status"] == "blocked"
    assert "no_canonical_entries" in report["blockers"]
    assert "no_model_ready_pairs" in report["blockers"]


def test_release_check_and_strict_build_release_cli() -> None:
    runner = CliRunner()
    tmp_path = _tmp_dir("release_readiness_cli")

    check_result = runner.invoke(
        app,
        ["--storage-root", str(tmp_path), "release-check"],
        catch_exceptions=False,
    )
    build_result = runner.invoke(
        app,
        ["--storage-root", str(tmp_path), "build-release", "--tag", "empty-release", "--strict"],
        catch_exceptions=False,
    )

    assert check_result.exit_code == 0
    assert "Release status" in check_result.output
    assert build_result.exit_code == 1
    assert "Release blocked" in build_result.output
