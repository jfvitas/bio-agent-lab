"""Shared CLI renderers for typed workspace-state reports."""

from __future__ import annotations

from pathlib import Path

import typer

from pbdata.workspace_state import DemoReadinessReport, DoctorReport, WorkspaceStatusReport


def emit_labeled_values(items: list[tuple[str, object]]) -> None:
    for label, value in items:
        typer.echo(f"{label:<18}: {value}")


def render_status_report(report: WorkspaceStatusReport) -> None:
    emit_labeled_values([
        ("Storage root", report.storage_root),
        ("Raw RCSB records", report.raw_rcsb_count),
        ("Processed records", report.processed_rcsb_count),
        ("Extracted entries", report.extracted_entry_count),
        ("Structure files", report.structure_file_count),
        ("Graph exports", report.graph_node_export_present and report.graph_edge_export_present),
        ("Feature manifest", report.feature_manifest_present),
        ("Training examples", report.training_example_count),
        ("Baseline model", report.baseline_model_present),
        ("Site feature runs", report.site_feature_runs),
        ("Surrogate checkpoint", report.surrogate_checkpoint_present),
        ("Latest release", report.release_snapshot_present),
        ("Core pipeline", report.core_pipeline_ready),
        ("Advanced outputs", report.advanced_outputs_ready),
    ])


def render_doctor_report(layout_root: Path, report: DoctorReport) -> None:
    emit_labeled_values([
        ("Storage root", layout_root),
        ("Overall status", report.overall_status),
        ("Python version", report.python_version),
        ("Data dir present", report.required_directories["data"]),
        ("Artifacts present", report.required_directories["artifacts"]),
    ])
    typer.echo("Dependencies:")
    for name, payload in report.dependency_checks.items():
        typer.echo(f"  - {name}: {payload.status}{' (required)' if payload.required else ''}")


def render_demo_readiness_report(layout_root: Path, report: DemoReadinessReport) -> None:
    emit_labeled_values([
        ("Storage root", layout_root),
        ("Demo readiness", report.readiness),
        ("Summary", report.summary),
        ("Core pipeline", report.core_pipeline_ready),
        ("Advanced outputs", report.advanced_outputs_ready),
        ("Blockers", ", ".join(report.blockers) if report.blockers else "none"),
        ("Warnings", ", ".join(report.warnings) if report.warnings else "none"),
    ])
    typer.echo("Recommended flow:")
    for step in report.recommended_demo_flow:
        typer.echo(f"  - {step}")
