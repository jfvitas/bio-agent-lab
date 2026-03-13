"""Pre-ingest RCSB search preview exports.

Assumptions:
- This is an acquisition-planning artifact, not a canonical dataset object.
- When a result limit is active, the preview reflects the current best-effort
  representative selection logic used by RCSB ingest.
- Coverage summaries are computed from a bounded preview sample, so they are
  intended for user guidance rather than exact global statistics.
"""

from __future__ import annotations

import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pbdata.criteria import SearchCriteria
from pbdata.sources import rcsb_search
from pbdata.storage import StorageLayout

_PREVIEW_SAMPLE_LIMIT = 200


def _resolution_bucket(entry: dict[str, Any]) -> str:
    resolution = (entry.get("rcsb_entry_info") or {}).get("resolution_combined")
    if isinstance(resolution, list):
        resolution = resolution[0] if resolution else None
    try:
        value = float(resolution)
    except (TypeError, ValueError):
        return "unknown_resolution"
    if value <= 2.0:
        return "high_resolution"
    if value <= 3.0:
        return "medium_resolution"
    return "low_resolution"


def _tax_bucket(entry: dict[str, Any]) -> str:
    for entity in entry.get("polymer_entities") or []:
        for organism in entity.get("rcsb_entity_source_organism") or []:
            tax_id = organism.get("ncbi_taxonomy_id")
            if tax_id not in (None, ""):
                return str(tax_id)
    return "unknown"


def _summarize_entries(entries: list[dict[str, Any]]) -> dict[str, dict[str, int]]:
    methods = Counter()
    tasks = Counter()
    resolutions = Counter()
    taxa = Counter()
    for entry in entries:
        methods[str(((entry.get("exptl") or [{}])[0]).get("method") or "unknown")] += 1
        tasks[rcsb_search._infer_rcsb_task_hint(entry)] += 1
        resolutions[_resolution_bucket(entry)] += 1
        taxa[_tax_bucket(entry)] += 1
    return {
        "experimental_methods": dict(methods),
        "task_types": dict(tasks),
        "resolution_buckets": dict(resolutions),
        "top_taxonomy_ids": dict(taxa.most_common(10)),
    }


def build_rcsb_search_preview(
    layout: StorageLayout,
    criteria: SearchCriteria,
) -> dict[str, Any]:
    raw_ids = rcsb_search.search_entries(criteria.model_copy(update={"max_results": None}))
    total_count = len(raw_ids)
    selected_ids = rcsb_search.search_entries(criteria)
    preview_ids = selected_ids[:_PREVIEW_SAMPLE_LIMIT]
    preview_entries = rcsb_search.fetch_entries_batch(preview_ids) if preview_ids else []

    if criteria.max_results is None:
        selection_mode = "full_result_set"
        summary = f"{total_count:,} matching entries with no result cap."
        next_action = "Run Ingest Sources to download the full matching set."
    elif criteria.representative_sampling:
        selection_mode = "representative_limited"
        summary = (
            f"{total_count:,} total matches; {len(selected_ids):,} selected under the current "
            f"representative limit."
        )
        next_action = (
            "Inspect the preview distribution before ingest. Adjust filters or the limit if the "
            "selected pool is still too narrow."
        )
    else:
        selection_mode = "hard_limited"
        summary = (
            f"{total_count:,} total matches; {len(selected_ids):,} retained by the current hard result limit."
        )
        next_action = "Use representative sampling if you want a broader capped acquisition instead of first-N ordering."

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "status": "ready",
        "selection_mode": selection_mode,
        "summary": summary,
        "next_action": next_action,
        "criteria": criteria.model_dump(),
        "counts": {
            "total_match_count": total_count,
            "selected_match_count": len(selected_ids),
            "preview_sample_count": len(preview_entries),
        },
        "selected_pdb_ids_preview": preview_ids[:25],
        "preview_distribution": _summarize_entries(preview_entries),
        "notes": (
            "Preview distributions are computed from the selected preview sample only. "
            "They are intended for acquisition planning, not for exact corpus statistics."
        ),
    }


def export_rcsb_search_preview(
    layout: StorageLayout,
    criteria: SearchCriteria,
) -> tuple[Path, Path, dict[str, Any]]:
    report = build_rcsb_search_preview(layout, criteria)
    layout.reports_dir.mkdir(parents=True, exist_ok=True)
    json_path = layout.reports_dir / "rcsb_search_preview.json"
    md_path = layout.reports_dir / "rcsb_search_preview.md"
    json_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    lines = [
        "# RCSB Search Preview",
        "",
        f"- Status: {report['status']}",
        f"- Selection mode: {report['selection_mode']}",
        f"- Summary: {report['summary']}",
        f"- Next action: {report['next_action']}",
        "",
        "## Counts",
    ]
    for key, value in (report.get("counts") or {}).items():
        lines.append(f"- {key}: {value}")
    lines.extend(["", "## Selected PDB IDs preview"])
    for pdb_id in report.get("selected_pdb_ids_preview") or []:
        lines.append(f"- {pdb_id}")
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return json_path, md_path, report
