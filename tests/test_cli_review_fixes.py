from pathlib import Path

from typer.testing import CliRunner

from pbdata.cli import app
from pbdata.pipeline.canonical_workflows import SKEMPIIngestResult
from tests.test_feature_execution import _tmp_dir


def test_cli_ingest_skempi_directs_users_to_extract(monkeypatch) -> None:
    runner = CliRunner()
    tmp_root = _tmp_dir("cli_ingest_skempi_message")
    calls: list[bool] = []

    def _fake_run_skempi_ingest(*, layout, dry_run, output_dir=None):
        calls.append(dry_run)
        if dry_run:
            return SKEMPIIngestResult(
                status="dry_run",
                csv_path=Path("skempi_v2.csv"),
                catalog_path=None,
                row_count=None,
                dry_run=True,
            )
        return SKEMPIIngestResult(
            status="downloaded",
            csv_path=Path("skempi_v2.csv"),
            catalog_path=Path("catalog.json"),
            row_count=1,
            dry_run=False,
        )

    monkeypatch.setattr("pbdata.cli.run_skempi_ingest", _fake_run_skempi_ingest)

    result = runner.invoke(
        app,
        ["--storage-root", str(tmp_root), "ingest", "--source", "skempi", "--yes"],
        catch_exceptions=False,
    )

    assert result.exit_code == 0
    assert calls == [True, False]
    assert "Run 'extract' with SKEMPI enabled" in result.output
