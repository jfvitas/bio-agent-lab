"""Typed workspace-state reports shared across CLI, GUI, and demo exports."""

from __future__ import annotations

import fnmatch
import json
import os
import sys
from dataclasses import asdict, dataclass
from pathlib import Path

from pbdata.config import AppConfig
from pbdata.demo_workspace import demo_manifest_path
from pbdata.file_health import JsonFileHealthSummary, load_or_scan_json_directory, write_health_summary_report
from pbdata.schemas.canonical_sample import CanonicalBindingSample
from pbdata.stage_state import list_stage_activity
from pbdata.storage import StorageLayout

_DIRECTORY_COUNT_CACHE: dict[tuple[str, str], tuple[tuple[int, int, int], int]] = {}


def _directory_signature(path: Path) -> tuple[int, int, int] | None:
    try:
        stat = path.stat()
    except FileNotFoundError:
        return None
    return (stat.st_mtime_ns, getattr(stat, "st_ctime_ns", 0), stat.st_size)


def _count_glob(path: Path, pattern: str) -> int:
    signature = _directory_signature(path)
    if signature is None:
        return 0
    cache_key = (str(path), pattern)
    cached = _DIRECTORY_COUNT_CACHE.get(cache_key)
    if cached and cached[0] == signature:
        return cached[1]

    count = 0
    try:
        with os.scandir(path) as entries:
            for entry in entries:
                if entry.is_file() and fnmatch.fnmatch(entry.name, pattern):
                    count += 1
    except OSError:
        count = 0
    _DIRECTORY_COUNT_CACHE[cache_key] = (signature, count)
    return count


@dataclass(frozen=True)
class WorkspaceStatusReport:
    storage_root: str
    raw_rcsb_count: int
    processed_rcsb_count: int
    processed_rcsb_valid_count: int
    processed_rcsb_problem_count: int
    processed_rcsb_empty_count: int
    processed_rcsb_corrupt_count: int
    processed_rcsb_invalid_count: int
    processed_health_cache_used: bool
    processed_health_cache_stale: bool
    processed_health_generated_at: str | None
    processed_health_report_json: str | None
    processed_health_report_md: str | None
    processed_health_sample_problem_files: list[str]
    extracted_entry_count: int
    structure_file_count: int
    graph_node_export_present: bool
    graph_edge_export_present: bool
    feature_manifest_present: bool
    training_examples_present: bool
    training_example_count: int
    release_snapshot_present: bool
    baseline_model_present: bool
    site_feature_runs: int
    surrogate_checkpoint_present: bool
    active_stage_lock_count: int
    running_stage_state_count: int
    failed_stage_state_count: int
    latest_stage_name: str | None
    latest_stage_status: str | None
    core_pipeline_ready: bool
    advanced_outputs_ready: bool
    processed_health: JsonFileHealthSummary

    def to_dict(self) -> dict[str, object]:
        payload = asdict(self)
        payload["processed_health"] = self.processed_health.to_dict()
        return payload


def _validate_processed_record(raw: dict[str, object]) -> None:
    CanonicalBindingSample.model_validate(raw)


@dataclass(frozen=True)
class DependencyCheck:
    status: str
    required: bool
    note: str
    error: str | None = None

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class DoctorReport:
    python_version: str
    storage_root_exists: bool
    config_storage_root: str
    required_directories: dict[str, bool]
    dependency_checks: dict[str, DependencyCheck]
    source_status: dict[str, dict[str, object]]
    status_snapshot: WorkspaceStatusReport
    overall_status: str

    def to_dict(self) -> dict[str, object]:
        payload = asdict(self)
        payload["dependency_checks"] = {
            key: value.to_dict() for key, value in self.dependency_checks.items()
        }
        payload["status_snapshot"] = self.status_snapshot.to_dict()
        return payload


@dataclass(frozen=True)
class DemoReadinessReport:
    storage_root: str
    readiness: str
    blockers: list[str]
    warnings: list[str]
    core_pipeline_ready: bool
    advanced_outputs_ready: bool
    status_snapshot: WorkspaceStatusReport
    doctor_snapshot: dict[str, str]
    recommended_demo_flow: list[str]
    summary: str
    assumptions: list[str]

    def to_dict(self) -> dict[str, object]:
        payload = asdict(self)
        payload["status_snapshot"] = self.status_snapshot.to_dict()
        return payload


def build_status_report(
    layout: StorageLayout,
    *,
    prefer_cached_health: bool = False,
) -> WorkspaceStatusReport:
    training_examples_path = layout.training_dir / "training_examples.json"
    model_path = layout.models_dir / "ligand_memory_model.json"
    latest_release = layout.releases_dir / "latest_release.json"
    feature_manifest = layout.features_dir / "feature_manifest.json"
    training_example_count = 0

    if training_examples_path.exists():
        raw = json.loads(training_examples_path.read_text(encoding="utf-8"))
        if isinstance(raw, list):
            training_example_count = len(raw)

    graph_nodes = (layout.graph_dir / "graph_nodes.json").exists()
    graph_edges = (layout.graph_dir / "graph_edges.json").exists()
    feature_manifest_present = feature_manifest.exists()
    training_examples_present = training_examples_path.exists()
    release_snapshot_present = latest_release.exists()
    baseline_model_present = model_path.exists()
    site_feature_runs = _count_glob(layout.artifact_manifests_dir, "*_input_manifest.json")
    layout.reports_dir.mkdir(parents=True, exist_ok=True)
    processed_health_report_json = layout.reports_dir / "processed_json_health.json"
    processed_health_report_md = layout.reports_dir / "processed_json_health.md"
    processed_health = load_or_scan_json_directory(
        layout.processed_rcsb_dir,
        validator=_validate_processed_record,
        cache_path=processed_health_report_json,
        prefer_cached=prefer_cached_health,
    )
    write_health_summary_report(
        processed_health,
        json_path=processed_health_report_json,
        md_path=processed_health_report_md,
        title="Processed JSON Health",
        scan_target=layout.processed_rcsb_dir,
    )
    stage_activity = list_stage_activity(layout)
    latest_stage = max(
        stage_activity,
        key=lambda payload: str(payload.get("generated_at") or ""),
        default={},
    )
    active_stage_lock_count = sum(1 for payload in stage_activity if payload.get("lock_active"))
    running_stage_state_count = sum(
        1
        for payload in stage_activity
        if str(payload.get("status") or "") == "running" and bool(payload.get("lock_active"))
    )
    failed_stage_state_count = sum(
        1 for payload in stage_activity if str(payload.get("status") or "") == "failed"
    )

    return WorkspaceStatusReport(
        storage_root=str(layout.root),
        raw_rcsb_count=_count_glob(layout.raw_rcsb_dir, "*.json"),
        processed_rcsb_count=processed_health.total_count,
        processed_rcsb_valid_count=processed_health.valid_count,
        processed_rcsb_problem_count=processed_health.problem_count,
        processed_rcsb_empty_count=processed_health.empty_count,
        processed_rcsb_corrupt_count=processed_health.corrupt_count,
        processed_rcsb_invalid_count=processed_health.invalid_count,
        processed_health_cache_used=processed_health.cache_used,
        processed_health_cache_stale=processed_health.cache_stale,
        processed_health_generated_at=processed_health.generated_at,
        processed_health_report_json=str(processed_health_report_json) if processed_health_report_json.exists() else None,
        processed_health_report_md=str(processed_health_report_md) if processed_health_report_md.exists() else None,
        processed_health_sample_problem_files=list(processed_health.sample_problem_files),
        extracted_entry_count=_count_glob(layout.extracted_dir / "entry", "*.json"),
        structure_file_count=_count_glob(layout.structures_rcsb_dir, "*.cif") + _count_glob(layout.structures_rcsb_dir, "*.pdb"),
        graph_node_export_present=graph_nodes,
        graph_edge_export_present=graph_edges,
        feature_manifest_present=feature_manifest_present,
        training_examples_present=training_examples_present,
        training_example_count=training_example_count,
        release_snapshot_present=release_snapshot_present,
        baseline_model_present=baseline_model_present,
        site_feature_runs=site_feature_runs,
        surrogate_checkpoint_present=(layout.surrogate_training_artifacts_dir / "latest_surrogate_checkpoint.json").exists(),
        active_stage_lock_count=active_stage_lock_count,
        running_stage_state_count=running_stage_state_count,
        failed_stage_state_count=failed_stage_state_count,
        latest_stage_name=str(latest_stage.get("stage") or "") or None,
        latest_stage_status=str(latest_stage.get("status") or "") or None,
        core_pipeline_ready=bool(
            _count_glob(layout.raw_rcsb_dir, "*.json")
            or _count_glob(layout.processed_rcsb_dir, "*.json")
            or _count_glob(layout.extracted_dir / "entry", "*.json")
        ),
        advanced_outputs_ready=bool(
            graph_nodes
            or feature_manifest_present
            or training_examples_present
            or baseline_model_present
            or release_snapshot_present
            or site_feature_runs
        ),
        processed_health=processed_health,
    )


def build_doctor_report(
    layout: StorageLayout,
    config: AppConfig,
    *,
    status_snapshot: WorkspaceStatusReport | None = None,
) -> DoctorReport:
    dependency_checks: dict[str, DependencyCheck] = {}
    for package_name, required, note in [
        ("gemmi", True, "Required for structure parsing."),
        ("yaml", True, "Required for config loading."),
        ("torch", False, "Optional; enables tensor exports and trained surrogate backend."),
        ("pyarrow", False, "Optional; enables native parquet I/O."),
        ("fastparquet", False, "Optional parquet backend."),
        ("esm", False, "Optional; enables learned protein embeddings."),
    ]:
        try:
            __import__(package_name)
            dependency_checks[package_name] = DependencyCheck(
                status="available",
                required=required,
                note=note,
            )
        except Exception as exc:
            dependency_checks[package_name] = DependencyCheck(
                status="missing",
                required=required,
                note=note,
                error=str(exc),
            )

    source_status: dict[str, dict[str, object]] = {}
    for source_name in ("rcsb", "bindingdb", "chembl", "pdbbind", "biolip", "skempi"):
        source_cfg = getattr(config.sources, source_name)
        source_status[source_name] = {
            "enabled": bool(source_cfg.enabled),
            "extra_keys": sorted((source_cfg.extra or {}).keys()),
        }

    overall_status = "ready" if all(
        value.status == "available" or not value.required
        for value in dependency_checks.values()
    ) else "missing_required_dependencies"

    return DoctorReport(
        python_version=sys.version.split()[0],
        storage_root_exists=layout.root.exists(),
        config_storage_root=config.storage_root,
        required_directories={
            "data": layout.data_dir.exists(),
            "artifacts": layout.artifacts_dir.exists(),
        },
        dependency_checks=dependency_checks,
        source_status=source_status,
        status_snapshot=status_snapshot or build_status_report(layout),
        overall_status=overall_status,
    )


def build_demo_readiness_report(
    layout: StorageLayout,
    config: AppConfig,
    *,
    status_snapshot: WorkspaceStatusReport | None = None,
    doctor_report: DoctorReport | None = None,
) -> DemoReadinessReport:
    status = status_snapshot or build_status_report(layout)
    doctor = doctor_report or build_doctor_report(layout, config, status_snapshot=status)
    demo_manifest = {}
    manifest_path = demo_manifest_path(layout)
    if manifest_path.exists():
        try:
            loaded_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            if isinstance(loaded_manifest, dict):
                demo_manifest = loaded_manifest
        except Exception:
            demo_manifest = {}

    blockers: list[str] = []
    if doctor.overall_status != "ready":
        blockers.append("missing_required_dependencies")
    if status.extracted_entry_count == 0:
        blockers.append("no_extracted_entries")

    warnings: list[str] = []
    if status.processed_rcsb_count == 0:
        warnings.append("no_normalized_records")
    if not status.graph_node_export_present or not status.graph_edge_export_present:
        warnings.append("graph_exports_missing")
    if not status.feature_manifest_present:
        warnings.append("feature_manifest_missing")
    if status.training_example_count == 0:
        warnings.append("no_training_examples")
    if not status.baseline_model_present:
        warnings.append("baseline_model_missing")
    if not status.release_snapshot_present:
        warnings.append("release_snapshot_missing")
    if demo_manifest.get("simulated"):
        warnings.append("demo_mode_simulated_outputs")

    if blockers:
        readiness = "not_demo_ready"
    elif len(warnings) <= 2:
        readiness = "ready_for_internal_demo"
    else:
        readiness = "technically_reviewable_not_polished"

    recommended_flow = [
        "Run `pbdata status` to show dataset/build state.",
        "Run `pbdata doctor` to show dependency readiness.",
        "Show `pbdata build-graph`, `pbdata build-features`, and `pbdata build-training-examples` outputs if artifacts are present.",
        "Use prediction/risk outputs as baseline or scaffold demonstrations, not scientific claims.",
    ]

    summary = (
        "Suitable for an internal med-school AI team walkthrough."
        if readiness == "ready_for_internal_demo"
        else "Suitable for technical review, but still needs demo polish."
        if readiness == "technically_reviewable_not_polished"
        else "Not yet suitable for a clean team walkthrough."
    )
    if demo_manifest.get("simulated"):
        summary = (
            "Demo workspace is seeded with simulated artifacts that show the intended user experience. "
            "Outputs are presentable but not scientific results."
        )

    return DemoReadinessReport(
        storage_root=str(layout.root),
        readiness=readiness,
        blockers=blockers,
        warnings=warnings,
        core_pipeline_ready=status.core_pipeline_ready,
        advanced_outputs_ready=status.advanced_outputs_ready,
        status_snapshot=status,
        doctor_snapshot={
            "overall_status": doctor.overall_status,
            "python_version": doctor.python_version,
        },
        recommended_demo_flow=recommended_flow,
        summary=summary,
        assumptions=[
            "If `demo_mode_simulated_outputs` is present, visible search, training, evaluation, and prediction outputs are seeded demo artifacts.",
            "Prediction and risk outputs remain baseline or placeholder unless a trained artifact is explicitly present.",
            "This report measures demo readiness of the local workspace, not scientific validity of downstream conclusions.",
        ],
    )
