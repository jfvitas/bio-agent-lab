"""Per-source operational state manifests.

These records are not scientific provenance. They document source freshness,
cache location, and the mode used to populate local data so users can review
whether enrichment inputs were reused, fetched live, or loaded from a user-
provided local cache.
"""

from __future__ import annotations

import json
from collections import Counter
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


def snapshot_source_state_counters(layout: StorageLayout) -> dict[str, dict[str, Any]]:
    """Capture the current cumulative counters for each source-state manifest."""
    snapshot: dict[str, dict[str, Any]] = {}
    if not layout.source_state_dir.exists():
        return snapshot
    for path in sorted(layout.source_state_dir.glob("*.json")):
        state = _load_existing_state(path)
        extra = state.get("extra") if isinstance(state.get("extra"), dict) else {}
        snapshot[path.stem.lower()] = {
            "attempt_count": int(extra.get("attempt_count") or 0),
            "total_records_observed": int(extra.get("total_records_observed") or 0),
            "status_counts": {
                str(key): int(value or 0)
                for key, value in (extra.get("status_counts") or {}).items()
            },
            "mode_counts": {
                str(key): int(value or 0)
                for key, value in (extra.get("mode_counts") or {}).items()
            },
        }
    return snapshot


def export_source_state_run_summary(
    layout: StorageLayout,
    *,
    baseline: dict[str, dict[str, Any]] | None = None,
    stage_name: str = "extract",
) -> tuple[Path, Path, dict[str, Any]]:
    """Write a run-scoped source/procurement summary by diffing cumulative counters.

    Biological assumption:
    - This is an operational ETL report, not scientific provenance.
    - Counts describe what happened during the current command invocation
      relative to the provided baseline snapshot.
    """
    baseline = baseline or {}
    per_source: list[dict[str, Any]] = []
    aggregate_status = Counter()
    aggregate_mode = Counter()
    total_attempts = 0
    total_records_observed = 0

    if layout.source_state_dir.exists():
        for path in sorted(layout.source_state_dir.glob("*.json")):
            state = _load_existing_state(path)
            extra = state.get("extra") if isinstance(state.get("extra"), dict) else {}
            source_key = path.stem.lower()
            before = baseline.get(source_key, {})
            before_status = before.get("status_counts") if isinstance(before.get("status_counts"), dict) else {}
            before_mode = before.get("mode_counts") if isinstance(before.get("mode_counts"), dict) else {}
            current_status = extra.get("status_counts") if isinstance(extra.get("status_counts"), dict) else {}
            current_mode = extra.get("mode_counts") if isinstance(extra.get("mode_counts"), dict) else {}

            status_delta = {
                str(key): int(current_status.get(key) or 0) - int(before_status.get(key) or 0)
                for key in set(current_status) | set(before_status)
            }
            status_delta = {key: value for key, value in status_delta.items() if value > 0}
            mode_delta = {
                str(key): int(current_mode.get(key) or 0) - int(before_mode.get(key) or 0)
                for key in set(current_mode) | set(before_mode)
            }
            mode_delta = {key: value for key, value in mode_delta.items() if value > 0}

            attempt_delta = int(extra.get("attempt_count") or 0) - int(before.get("attempt_count") or 0)
            record_delta = int(extra.get("total_records_observed") or 0) - int(before.get("total_records_observed") or 0)
            if attempt_delta <= 0 and record_delta <= 0 and not status_delta and not mode_delta:
                continue

            total_attempts += max(attempt_delta, 0)
            total_records_observed += max(record_delta, 0)
            aggregate_status.update(status_delta)
            aggregate_mode.update(mode_delta)
            per_source.append({
                "source_name": state.get("source_name") or source_key,
                "latest_status": state.get("status"),
                "latest_mode": state.get("mode"),
                "latest_notes": state.get("notes"),
                "attempt_count": max(attempt_delta, 0),
                "records_observed": max(record_delta, 0),
                "status_counts": status_delta,
                "mode_counts": mode_delta,
            })

    status = "no_source_activity" if not per_source else "ready"
    summary = (
        "No enrichment source activity was recorded during this run."
        if not per_source
        else (
            f"Observed {total_attempts} source attempt(s) across {len(per_source)} source(s); "
            f"{total_records_observed} record(s) were loaded or normalized."
        )
    )
    report = {
        "stage_name": stage_name,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "status": status,
        "summary": summary,
        "source_count": len(per_source),
        "total_attempt_count": total_attempts,
        "total_records_observed": total_records_observed,
        "aggregate_status_counts": dict(aggregate_status),
        "aggregate_mode_counts": dict(aggregate_mode),
        "sources": per_source,
    }

    layout.reports_dir.mkdir(parents=True, exist_ok=True)
    json_path = layout.reports_dir / f"{stage_name}_source_run_summary.json"
    md_path = layout.reports_dir / f"{stage_name}_source_run_summary.md"
    json_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

    lines = [
        f"# {stage_name.title()} Source Run Summary",
        "",
        f"Status: `{status}`",
        "",
        summary,
        "",
        f"- Sources touched: `{len(per_source)}`",
        f"- Source attempts: `{total_attempts}`",
        f"- Records observed: `{total_records_observed}`",
        f"- Status counts: `{dict(aggregate_status)}`",
        f"- Mode counts: `{dict(aggregate_mode)}`",
    ]
    if per_source:
        lines.extend(["", "## Per-source activity", ""])
        for source in per_source:
            lines.append(
                f"- `{source['source_name']}`: attempts=`{source['attempt_count']}`, "
                f"records=`{source['records_observed']}`, latest_status=`{source['latest_status']}`, "
                f"latest_mode=`{source['latest_mode']}`"
            )
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return json_path, md_path, report
