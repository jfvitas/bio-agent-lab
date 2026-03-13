import json
import os

from typer.testing import CliRunner

from pbdata.cli import app
from pbdata.stage_state import acquire_stage_lock, stage_lock_path
from pbdata.storage import build_storage_layout
from tests.test_feature_execution import _tmp_dir


def test_acquire_stage_lock_recovers_stale_lock() -> None:
    layout = build_storage_layout(_tmp_dir("stage_lock_stale"))
    lock_path = stage_lock_path(layout, "extract")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    lock_path.write_text(
        json.dumps({
            "stage": "extract",
            "pid": 999999,
            "storage_root": str(layout.root),
        }),
        encoding="utf-8",
    )

    acquired = acquire_stage_lock(layout, stage="extract")

    payload = json.loads(acquired.read_text(encoding="utf-8"))
    assert acquired == lock_path
    assert int(payload["pid"]) == os.getpid()


def test_extract_cli_rejects_active_stage_lock() -> None:
    tmp_root = _tmp_dir("extract_lock_cli")
    layout = build_storage_layout(tmp_root)
    layout.raw_rcsb_dir.mkdir(parents=True, exist_ok=True)
    (layout.raw_rcsb_dir / "1ABC.json").write_text("{}", encoding="utf-8")
    lock_path = stage_lock_path(layout, "extract")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    lock_path.write_text(
        json.dumps({
            "stage": "extract",
            "pid": os.getpid(),
            "storage_root": str(layout.root),
        }),
        encoding="utf-8",
    )

    runner = CliRunner()
    result = runner.invoke(
        app,
        ["--storage-root", str(layout.root), "extract"],
        catch_exceptions=False,
    )

    assert result.exit_code == 1
    assert "already running" in result.output
