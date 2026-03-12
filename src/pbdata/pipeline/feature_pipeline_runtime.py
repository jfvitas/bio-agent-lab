from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter
from typing import Any

import pandas as pd

from pbdata.storage import StorageLayout
from pbdata.table_io import write_dataframe


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def default_run_id() -> str:
    return datetime.now(timezone.utc).strftime("feature_run_%Y%m%dT%H%M%SZ")


def json_dump(path: Path, payload: Any) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return path


def write_df(path: Path, rows: list[dict[str, Any]]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    return write_dataframe(pd.DataFrame(rows), path)


def stage_manifest_path(layout: StorageLayout, run_id: str, stage_name: str) -> Path:
    return layout.artifact_manifests_dir / f"{run_id}_{stage_name}_status.json"


def structured_error_path(layout: StorageLayout, run_id: str) -> Path:
    return layout.artifact_logs_dir / f"{run_id}_structured_errors.jsonl"


def append_structured_error(layout: StorageLayout, run_id: str, payload: dict[str, Any]) -> None:
    path = structured_error_path(layout, run_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload) + "\n")


def write_stage_status(
    layout: StorageLayout,
    *,
    run_id: str,
    stage_name: str,
    start_time: str,
    end_time: str,
    status: str,
    records_attempted: int,
    records_succeeded: int,
    records_failed: int,
    upstream_dependencies: list[str],
    output_artifacts: list[str],
    warnings: list[str],
) -> Path:
    return json_dump(
        stage_manifest_path(layout, run_id, stage_name),
        {
            "stage_name": stage_name,
            "run_id": run_id,
            "start_time": start_time,
            "end_time": end_time,
            "status": status,
            "records_attempted": records_attempted,
            "records_succeeded": records_succeeded,
            "records_failed": records_failed,
            "upstream_dependencies": upstream_dependencies,
            "output_artifacts": output_artifacts,
            "warnings": warnings,
        },
    )


def load_stage_status(layout: StorageLayout, run_id: str, stage_name: str) -> dict[str, Any] | None:
    path = stage_manifest_path(layout, run_id, stage_name)
    if not path.exists():
        return None
    raw = json.loads(path.read_text(encoding="utf-8"))
    return raw if isinstance(raw, dict) else None


def write_input_manifest(
    layout: StorageLayout,
    *,
    config: Any,
    row_counts: dict[str, int],
    schema_version: str,
    pipeline_version: str,
    site_physics_spec_version: str,
    graph_representation_version: str,
    training_example_version: str,
) -> Path:
    return json_dump(
        layout.artifact_manifests_dir / f"{config.run_id}_input_manifest.json",
        {
            "schema_version": schema_version,
            "pipeline_version": pipeline_version,
            "site_physics_spec_version": site_physics_spec_version,
            "graph_representation_version": graph_representation_version,
            "training_example_version": training_example_version,
            "source_dataset_versions": {"canonical_tables": "derived_from_extracted_tables"},
            "git_commit_hash": None,
            "run_timestamp": utc_now(),
            "task_id": config.run_id,
            "row_counts_by_entity_table": row_counts,
            "run_mode": config.run_mode,
            "gpu_unavailable": not config.gpu_enabled,
        },
    )


def run_stage(layout: StorageLayout, config: Any, *, stage_name: str, dependencies: list[str], runner) -> dict[str, Any]:
    if config.run_mode == "resume":
        existing = load_stage_status(layout, config.run_id, stage_name)
        if existing and str(existing.get("status") or "") == "passed":
            return {"status": "skipped", "warnings": ["resume_reused_existing_stage"]}
    start = utc_now()
    t0 = perf_counter()
    attempted = succeeded = failed = 0
    warnings: list[str] = []
    outputs: list[str] = []
    status = "passed"
    try:
        attempted, succeeded, failed, outputs, warnings = runner()
        status = "partial" if failed and succeeded else "failed" if failed and not succeeded else "passed"
    except Exception as exc:
        status = "failed"
        failed = max(failed, 1)
        warnings.append(str(exc))
        append_structured_error(layout, config.run_id, {"stage_name": stage_name, "run_id": config.run_id, "error": str(exc), "generated_at": utc_now()})
        if config.fail_hard:
            raise
    write_stage_status(
        layout,
        run_id=config.run_id,
        stage_name=stage_name,
        start_time=start,
        end_time=utc_now(),
        status=status,
        records_attempted=attempted,
        records_succeeded=succeeded,
        records_failed=failed,
        upstream_dependencies=dependencies,
        output_artifacts=outputs,
        warnings=warnings + [f"elapsed_seconds={round(perf_counter() - t0, 3)}"],
    )
    return {"status": status, "warnings": warnings}
