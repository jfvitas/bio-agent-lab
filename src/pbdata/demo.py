"""Internal demo snapshot exports for operator-facing walkthroughs."""

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

    lines = [
        "# Internal Demo Snapshot",
        "",
        f"- Readiness: `{report.get('readiness', 'unknown')}`",
        f"- Summary: {report.get('summary', '')}",
        f"- Core pipeline ready: `{report.get('core_pipeline_ready', False)}`",
        f"- Advanced outputs ready: `{report.get('advanced_outputs_ready', False)}`",
        "",
        "## Blockers",
    ]
    if blockers:
        lines.extend(f"- `{item}`" for item in blockers if item)
    else:
        lines.append("- none")

    lines.extend(["", "## Warnings"])
    if warnings:
        lines.extend(f"- `{item}`" for item in warnings if item)
    else:
        lines.append("- none")

    lines.extend(["", "## Recommended Demo Flow"])
    lines.extend(f"{index}. {step}" for index, step in enumerate(steps, start=1))

    lines.extend(["", "## Assumptions"])
    lines.extend(f"- {item}" for item in assumptions if item)
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
