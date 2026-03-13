"""Stage-state manifest helpers for resumable long-running pipeline steps.

These manifests are operational metadata only. They are not scientific
provenance; they describe which inputs and outputs a stage used and whether the
run completed cleanly.
"""

from __future__ import annotations

import json
import os
import socket
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

from pbdata.storage import StorageLayout


def write_stage_state(
    layout: StorageLayout,
    *,
    stage: str,
    status: str,
    input_dir: Path | None = None,
    output_dir: Path | None = None,
    workers: int | None = None,
    counts: dict[str, int] | None = None,
    notes: str | None = None,
) -> Path:
    """Write a small JSON manifest describing one stage run."""
    out_dir = layout.stage_state_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    payload: dict[str, Any] = {
        "stage": stage,
        "status": status,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "storage_root": str(layout.root),
        "input_dir": str(input_dir) if input_dir is not None else None,
        "output_dir": str(output_dir) if output_dir is not None else None,
        "workers": workers,
        "counts": counts or {},
        "notes": notes,
    }
    out_path = out_dir / f"{stage}.json"
    out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return out_path


def read_stage_state(layout: StorageLayout, stage: str) -> dict[str, Any]:
    """Read one stage-state manifest if it exists."""
    path = layout.stage_state_dir / f"{stage}.json"
    if not path.exists():
        return {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return raw if isinstance(raw, dict) else {}


def _stage_lock_dir(layout: StorageLayout) -> Path:
    return layout.stage_state_dir / "locks"


def stage_lock_path(layout: StorageLayout, stage: str) -> Path:
    """Return the canonical lock-file path for one stage."""
    return _stage_lock_dir(layout) / f"{stage}.lock.json"


def _pid_is_running(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    return True


def _read_stage_lock(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return raw if isinstance(raw, dict) else {}


def read_stage_lock(layout: StorageLayout, *, stage: str) -> dict[str, Any]:
    """Read one stage-lock payload if it exists."""
    return _read_stage_lock(stage_lock_path(layout, stage))


def list_stage_activity(layout: StorageLayout) -> list[dict[str, Any]]:
    """Return current stage manifests annotated with lock/runtime status."""
    states: list[dict[str, Any]] = []
    if not layout.stage_state_dir.exists():
        return states

    for path in sorted(layout.stage_state_dir.glob("*.json")):
        if path.name.endswith(".lock.json"):
            continue
        stage = path.stem
        payload = read_stage_state(layout, stage)
        if not payload:
            continue
        lock_payload = read_stage_lock(layout, stage=stage)
        lock_pid = int(lock_payload.get("pid") or 0)
        lock_active = lock_pid > 0 and _pid_is_running(lock_pid)
        payload = dict(payload)
        payload["lock_present"] = bool(lock_payload)
        payload["lock_active"] = lock_active
        payload["lock_pid"] = lock_pid if lock_pid > 0 else None
        payload["lock_created_at"] = lock_payload.get("created_at")
        states.append(payload)
    return states


def acquire_stage_lock(layout: StorageLayout, *, stage: str) -> Path:
    """Acquire a per-stage workspace lock or raise if one is already active."""
    lock_path = stage_lock_path(layout, stage)
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    if lock_path.exists():
        payload = _read_stage_lock(lock_path)
        pid = int(payload.get("pid") or 0)
        owner_root = str(payload.get("storage_root") or "")
        if pid > 0 and _pid_is_running(pid):
            raise RuntimeError(
                f"Stage '{stage}' is already running for {owner_root or layout.root} "
                f"(pid={pid}). Wait for that run to finish before starting another."
            )
        lock_path.unlink(missing_ok=True)

    payload = {
        "stage": stage,
        "pid": os.getpid(),
        "hostname": socket.gethostname(),
        "created_at": datetime.now(timezone.utc).isoformat(),
        "storage_root": str(layout.root),
    }
    lock_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return lock_path


def release_stage_lock(layout: StorageLayout, *, stage: str) -> None:
    """Release a per-stage workspace lock if it exists."""
    stage_lock_path(layout, stage).unlink(missing_ok=True)


@contextmanager
def stage_lock(layout: StorageLayout, *, stage: str) -> Iterator[Path]:
    """Context-manager wrapper for one stage lock."""
    lock_path = acquire_stage_lock(layout, stage=stage)
    try:
        yield lock_path
    finally:
        release_stage_lock(layout, stage=stage)
