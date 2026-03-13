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
        ("Processed valid", report.processed_rcsb_valid_count),
        ("Processed issues", report.processed_rcsb_problem_count),
        ("Extracted entries", report.extracted_entry_count),
        ("Structure files", report.structure_file_count),
        ("Graph exports", report.graph_node_export_present and report.graph_edge_export_present),
        ("Feature manifest", report.feature_manifest_present),
        ("Training examples", report.training_example_count),
        ("Baseline model", report.baseline_model_present),
        ("Site feature runs", report.site_feature_runs),
        ("Surrogate checkpoint", report.surrogate_checkpoint_present),
        ("Active locks", report.active_stage_lock_count),
        ("Running stages", report.running_stage_state_count),
        ("Failed stages", report.failed_stage_state_count),
        ("Latest stage", report.latest_stage_name or "none"),
        ("Latest status", report.latest_stage_status or "none"),
        ("Latest release", report.release_snapshot_present),
        ("Core pipeline", report.core_pipeline_ready),
        ("Advanced outputs", report.advanced_outputs_ready),
    ])
    if report.processed_rcsb_problem_count:
        typer.echo(
            "Processed health    : "
            f"{report.processed_rcsb_empty_count} empty, "
            f"{report.processed_rcsb_corrupt_count} corrupt, "
            f"{report.processed_rcsb_invalid_count} schema-invalid"
        )
        if report.processed_health_sample_problem_files:
            typer.echo(
                "Problem examples    : "
                + ", ".join(report.processed_health_sample_problem_files[:5])
            )


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
