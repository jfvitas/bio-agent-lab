"""Stage-state manifest helpers for resumable long-running pipeline steps.

These manifests are operational metadata only. They are not scientific
provenance; they describe which inputs and outputs a stage used and whether the
run completed cleanly.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

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
