"""Per-source operational state manifests.

These records are not scientific provenance. They document source freshness,
cache location, and the mode used to populate local data so users can review
whether enrichment inputs were reused, fetched live, or loaded from a user-
provided local cache.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pbdata.storage import StorageLayout


def _load_existing_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return raw if isinstance(raw, dict) else {}


def write_source_state(
    layout: StorageLayout,
    *,
    source_name: str,
    status: str,
    mode: str,
    cache_path: Path | None = None,
    record_id: str | None = None,
    record_count: int | None = None,
    notes: str | None = None,
    extra: dict[str, Any] | None = None,
) -> Path:
    """Write one source-state JSON manifest."""
    out_dir = layout.source_state_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{source_name.lower()}.json"
    existing = _load_existing_state(out_path)
    existing_extra = existing.get("extra") if isinstance(existing.get("extra"), dict) else {}
    status_counts = existing_extra.get("status_counts") if isinstance(existing_extra.get("status_counts"), dict) else {}
    mode_counts = existing_extra.get("mode_counts") if isinstance(existing_extra.get("mode_counts"), dict) else {}
    updated_extra: dict[str, Any] = dict(existing_extra)
    updated_extra["attempt_count"] = int(existing_extra.get("attempt_count") or 0) + 1
    updated_extra["status_counts"] = {
        **status_counts,
        status: int(status_counts.get(status) or 0) + 1,
    }
    updated_extra["mode_counts"] = {
        **mode_counts,
        mode: int(mode_counts.get(mode) or 0) + 1,
    }
    updated_extra["last_status"] = status
    updated_extra["last_mode"] = mode
    if record_count is not None:
        updated_extra["total_records_observed"] = int(existing_extra.get("total_records_observed") or 0) + int(record_count)
    if extra:
        updated_extra.update(extra)
    payload: dict[str, Any] = {
        "source_name": source_name,
        "status": status,
        "mode": mode,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "storage_root": str(layout.root),
        "record_id": record_id,
        "record_count": record_count,
        "cache_path": str(cache_path) if cache_path is not None else None,
        "notes": notes,
        "extra": updated_extra,
    }
    if cache_path is not None and cache_path.exists():
        payload["cache_mtime"] = datetime.fromtimestamp(
            cache_path.stat().st_mtime,
            tz=timezone.utc,
        ).isoformat()
        payload["cache_size_bytes"] = cache_path.stat().st_size
    out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return out_path
