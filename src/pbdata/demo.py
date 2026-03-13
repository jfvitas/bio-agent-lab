"""Demo snapshot exports for walkthrough-ready workspace summaries."""

from __future__ import annotations

import json
from pathlib import Path

from pbdata.config import AppConfig
from pbdata.storage import StorageLayout
from pbdata.workspace_state import build_demo_readiness_report


def _demo_markdown(report: dict[str, object]) -> str:
    blockers = report.get("blockers") or []
    warnings = report.get("warnings") or []
    steps = report.get("recommended_demo_flow") or []
    assumptions = report.get("assumptions") or []
    readiness = str(report.get("readiness", "unknown") or "unknown")
    customer_message = {
        "ready_for_internal_demo": "Suitable for a customer-facing baseline demo if the presenter stays close to the visible artifacts.",
        "technically_reviewable_not_polished": "Suitable for a technical preview, but unfinished areas should be framed explicitly as work in progress.",
        "not_demo_ready": "Not yet suitable for a clean customer demo without additional setup or caveat-heavy narration.",
    }.get(readiness, "Current demo state should be reviewed before sharing externally.")

    lines = [
        "# Demo Snapshot",
        "",
        f"- Workspace readiness: `{readiness}`",
        f"- Demo summary: {report.get('summary', '')}",
        f"- Presenter note: {customer_message}",
        f"- Core pipeline ready: `{'yes' if report.get('core_pipeline_ready', False) else 'no'}`",
        f"- Advanced outputs ready: `{'yes' if report.get('advanced_outputs_ready', False) else 'no'}`",
        "",
        "## What Needs Attention Before The Demo",
    ]
    if blockers:
        lines.extend(f"- `{item}`" for item in blockers if item)
    else:
        lines.append("- No hard blockers detected.")

    lines.extend(["", "## Watchouts To Explain Clearly"])
    if warnings:
        lines.extend(f"- `{item}`" for item in warnings if item)
    else:
        lines.append("- No active warnings.")

    lines.extend(["", "## Recommended Walkthrough"])
    if steps:
        lines.extend(f"{index}. {step}" for index, step in enumerate(steps, start=1))
    else:
        lines.append("1. Open the workspace overview and confirm the current dataset state.")

    lines.extend(["", "## Ground Rules"])
    if assumptions:
        lines.extend(f"- {item}" for item in assumptions if item)
    else:
        lines.append("- No additional assumptions were recorded.")
    lines.append("")
    return "\n".join(lines)


def export_demo_snapshot(
    layout: StorageLayout,
    config: AppConfig,
) -> tuple[Path, Path, dict[str, object]]:
    report = build_demo_readiness_report(layout, config).to_dict()
    out_dir = layout.feature_reports_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    json_path = out_dir / "demo_readiness.json"
    md_path = out_dir / "demo_walkthrough.md"
    json_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    md_path.write_text(_demo_markdown(report), encoding="utf-8")
    return json_path, md_path, report
