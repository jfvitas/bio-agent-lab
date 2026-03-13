"""Shared GUI overview summaries and workspace snapshot assembly."""

from __future__ import annotations

import csv
import json
import os
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pbdata.config import AppConfig
from pbdata.sources.registry import build_source_capability_report
from pbdata.model_comparison import build_model_comparison_report
from pbdata.stage_state import list_stage_activity
from pbdata.storage import StorageLayout
from pbdata.training_quality import build_training_set_quality_report
from pbdata.workspace_state import DemoReadinessReport, build_demo_readiness_report, build_doctor_report, build_status_report

_JSON_DICT_CACHE: dict[str, tuple[tuple[int, int], dict[str, Any]]] = {}
_CSV_ROWS_CACHE: dict[str, tuple[tuple[int, int], list[dict[str, str]]]] = {}


def _file_signature(path: Path) -> tuple[int, int] | None:
    try:
        stat = path.stat()
    except FileNotFoundError:
        return None
    return (stat.st_mtime_ns, stat.st_size)


def count_files(directory: Path, pattern: str = "*.json") -> int:
    if not directory.exists():
        return 0
    count = 0
    try:
        with os.scandir(directory) as entries:
            for entry in entries:
                if entry.is_file() and Path(entry.name).match(pattern):
                    count += 1
    except OSError:
        return 0
    return count


def load_csv_dict_rows(path: Path) -> list[dict[str, str]]:
    signature = _file_signature(path)
    if signature is None:
        return []
    cache_key = str(path)
    cached = _CSV_ROWS_CACHE.get(cache_key)
    if cached and cached[0] == signature:
        return list(cached[1])
    with path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    _CSV_ROWS_CACHE[cache_key] = (signature, rows)
    return list(rows)


def load_json_dict(path: Path) -> dict[str, Any]:
    signature = _file_signature(path)
    if signature is None:
        return {}
    cache_key = str(path)
    cached = _JSON_DICT_CACHE.get(cache_key)
    if cached and cached[0] == signature:
        return dict(cached[1])
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    value = raw if isinstance(raw, dict) else {}
    _JSON_DICT_CACHE[cache_key] = (signature, value)
    return dict(value)


def _parse_iso_datetime(value: object) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        normalized = text.replace("Z", "+00:00")
        dt = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


def _coerce_int(value: object, default: int = 0) -> int:
    if value in (None, "", "--"):
        return default
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    try:
        return int(str(value).replace(",", "").strip())
    except (TypeError, ValueError):
        return default


def _describe_stage_timestamp(value: object) -> str:
    dt = _parse_iso_datetime(value)
    if dt is None:
        return "unknown time"
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def build_review_health_summary(coverage: dict[str, Any]) -> dict[str, str]:
    counts = coverage.get("counts") or {}
    release = coverage.get("release") or {}
    issue_types = (coverage.get("coverage") or {}).get("issue_types") or {}

    entry_count = int(counts.get("entry_count") or 0)
    pair_count = int(counts.get("pair_count") or 0)
    model_ready = int(counts.get("model_ready_pair_count") or 0)
    conflicts = int(counts.get("pairs_with_source_conflicts") or 0)
    exclusions = int(release.get("model_ready_exclusion_count") or 0)
    structures = int(counts.get("entries_with_structure_file") or 0)
    missing_structures = int(issue_types.get("missing_structure_file") or 0)
    low_conf = int(issue_types.get("non_high_confidence_fields") or 0) + int(issue_types.get("non_high_confidence_assay_fields") or 0)

    readiness = "Not ready"
    if pair_count > 0 and exclusions == 0 and conflicts == 0:
        readiness = "Release-ready"
    elif model_ready > 0:
        readiness = "Partially ready"
    elif entry_count > 0:
        readiness = "Needs review"

    coverage_text = (
        f"{entry_count:,} entries, {pair_count:,} pairs, {model_ready:,} model-ready, "
        f"{structures:,} with structures"
    )
    quality_text = (
        f"{conflicts:,} conflicted pairs, {low_conf:,} non-high-confidence issues, "
        f"{missing_structures:,} missing structures"
    )

    if conflicts > 0:
        next_action = "Review master_pdb_conflicts.csv and master_pdb_issues.csv before release."
    elif missing_structures > 0:
        next_action = "Repair missing structure files before trusting model-ready outputs."
    elif exclusions > 0:
        next_action = "Review model_ready_exclusions.csv to resolve or accept blocked pairs."
    elif model_ready > 0:
        next_action = "Build or inspect the latest release snapshot."
    else:
        next_action = "Run ingest and extract to populate review artifacts."

    return {
        "readiness": readiness,
        "coverage": coverage_text,
        "quality": quality_text,
        "next_action": next_action,
    }


def build_training_set_builder_summary(
    scorecard: dict[str, Any],
    benchmark_rows: list[dict[str, str]],
) -> dict[str, str]:
    selected_count = int(scorecard.get("selected_count") or 0)
    candidate_pool_count = int(scorecard.get("candidate_pool_count") or 0)
    diversity = scorecard.get("diversity") or {}
    quality = scorecard.get("quality") or {}
    exclusions = scorecard.get("exclusions") or {}

    receptor_clusters = int(diversity.get("selected_receptor_clusters") or 0)
    pair_families = int(diversity.get("selected_pair_families") or 0)
    mean_quality = float(quality.get("mean_quality_score") or 0.0)
    exclusion_count = int(exclusions.get("count") or 0)

    dominant_benchmark = max(
        benchmark_rows,
        key=lambda row: float(str(row.get("largest_group_fraction") or "0") or 0.0),
        default={},
    )
    benchmark_mode = str(dominant_benchmark.get("benchmark_mode") or "n/a")
    benchmark_fraction = float(str(dominant_benchmark.get("largest_group_fraction") or "0") or 0.0)

    readiness = "Not built"
    if selected_count > 0 and benchmark_fraction <= 0.35:
        readiness = "Strong diversity"
    elif selected_count > 0:
        readiness = "Needs tuning"

    next_action = "Build custom training set from model-ready pairs."
    if selected_count > 0 and benchmark_fraction > 0.35:
        next_action = (
            "Reduce dominance by changing selection mode or lowering the per-receptor "
            "cluster cap before releasing the set."
        )
    elif selected_count > 0:
        next_action = "Inspect exclusions and split benchmark, then freeze a release snapshot."

    return {
        "status": readiness,
        "coverage": (
            f"{selected_count:,} selected from {candidate_pool_count:,}; "
            f"{receptor_clusters:,} receptor clusters, {pair_families:,} pair families"
        ),
        "quality": (
            f"mean quality {mean_quality:.3f}; {exclusion_count:,} excluded; "
            f"largest benchmark group {benchmark_mode}={benchmark_fraction:.2%}"
        ),
        "next_action": next_action,
    }


def build_training_set_kpis(
    scorecard: dict[str, Any],
    benchmark_rows: list[dict[str, str]],
) -> dict[str, str]:
    selected_count = int(scorecard.get("selected_count") or 0)
    diversity = scorecard.get("diversity") or {}
    quality = scorecard.get("quality") or {}
    exclusions = scorecard.get("exclusions") or {}
    largest_fraction = max(
        (float(str(row.get("largest_group_fraction") or "0") or 0.0) for row in benchmark_rows),
        default=0.0,
    )
    return {
        "selected": f"{selected_count:,}",
        "clusters": f"{int(diversity.get('selected_receptor_clusters') or 0):,}",
        "quality": f"{float(quality.get('mean_quality_score') or 0.0):.3f}",
        "dominance": f"{largest_fraction:.1%}",
        "excluded": f"{int(exclusions.get('count') or 0):,}",
    }


def build_training_set_workflow_status(review_paths: dict[str, str]) -> list[tuple[str, str]]:
    def _exists(key: str) -> bool:
        value = str(review_paths.get(key) or "").strip()
        return bool(value) and Path(value).exists()

    return [
        ("Model-ready pool", "ready" if _exists("model_ready_pairs_csv") else "missing"),
        ("Custom set", "ready" if _exists("custom_training_set_csv") else "pending"),
        ("Scorecard", "ready" if _exists("custom_training_scorecard_json") else "pending"),
        ("Benchmark", "ready" if _exists("custom_training_split_benchmark_csv") else "pending"),
        ("Release", "ready" if _exists("release_manifest_json") else "pending"),
    ]


def build_training_quality_summary(report: dict[str, Any]) -> dict[str, str]:
    return {
        "status": str(report.get("status") or "empty"),
        "coverage": str(report.get("summary") or "No training examples available."),
        "quality": str(report.get("quality") or "No quality signals available."),
        "next_action": str(report.get("next_action") or "Build training examples."),
    }


def build_training_quality_kpis(report: dict[str, Any]) -> dict[str, str]:
    counts = report.get("counts") or {}
    fractions = report.get("fractions") or {}
    return {
        "examples": f"{int(counts.get('example_count') or 0):,}",
        "supervised": f"{int(counts.get('supervised_count') or 0):,}",
        "targets": f"{int(counts.get('unique_target_count') or 0):,}",
        "ligands": f"{int(counts.get('unique_ligand_count') or 0):,}",
        "conflicts": f"{float(fractions.get('source_conflict_fraction') or 0.0):.1%}",
    }


def build_model_comparison_summary(report: dict[str, Any]) -> dict[str, str]:
    return {
        "status": str(report.get("status") or "missing_models"),
        "summary": str(report.get("summary") or "No model evaluations available."),
        "next_action": str(report.get("next_action") or "Evaluate baseline and tabular models."),
    }


def build_model_comparison_kpis(report: dict[str, Any]) -> dict[str, str]:
    splits = report.get("splits") or {}
    val = splits.get("val") or {}
    test = splits.get("test") or {}
    available = report.get("available_models") or {}
    return {
        "baseline": "ready" if available.get("baseline_ready") else "missing",
        "tabular": "ready" if available.get("tabular_ready") else "missing",
        "val_winner": str(val.get("winner") or "n/a"),
        "test_winner": str(test.get("winner") or "n/a"),
        "val_gap": _format_model_gap(val),
    }


def build_source_run_summary(layout: StorageLayout) -> tuple[dict[str, str], dict[str, str]]:
    report = load_json_dict(layout.reports_dir / "extract_source_run_summary.json")
    if not report:
        return (
            {
                "status": "no_extract_source_summary",
                "summary": "No extract-time source activity summary is available yet.",
                "next_action": "Run Extract to generate a per-run procurement summary.",
            },
            {
                "sources": "--",
                "attempts": "--",
                "records": "--",
                "mode": "--",
            },
        )

    mode_counts = report.get("aggregate_mode_counts") or {}
    dominant_mode = max(
        mode_counts.items(),
        key=lambda item: int(item[1] or 0),
        default=("n/a", 0),
    )[0]
    summary = {
        "status": str(report.get("status") or "no_extract_source_summary"),
        "summary": str(report.get("summary") or "No extract-time source activity summary is available."),
        "next_action": (
            "Inspect the source run summary report if lookup failures or invalid payloads are non-zero."
            if str(report.get("status") or "") == "ready"
            else "Run Extract to record source activity for the current workspace state."
        ),
    }
    kpis = {
        "sources": f"{int(report.get('source_count') or 0):,}",
        "attempts": f"{int(report.get('total_attempt_count') or 0):,}",
        "records": f"{int(report.get('total_records_observed') or 0):,}",
        "mode": str(dominant_mode or "n/a"),
    }
    return summary, kpis


def build_active_operations_summary(layout: StorageLayout) -> tuple[dict[str, str], dict[str, str]]:
    states = list_stage_activity(layout)
    active = [state for state in states if state.get("lock_active")]
    stale_locks = [state for state in states if state.get("lock_present") and not state.get("lock_active")]
    failed = [state for state in states if str(state.get("status") or "") == "failed"]
    running = [
        state
        for state in states
        if str(state.get("status") or "") in {"running", "completed_with_failures"}
    ]
    latest = max(
        states,
        key=lambda state: str(state.get("generated_at") or ""),
        default={},
    )
    latest_stage = str(latest.get("stage") or "n/a")
    latest_counts = latest.get("counts") or {}
    latest_attempts = sum(int(value or 0) for value in latest_counts.values()) if isinstance(latest_counts, dict) else 0
    latest_status = str(latest.get("status") or "unknown")

    if active:
        active_state = active[0]
        active_detail = (
            f"{str(active_state.get('stage') or 'unknown')} "
            f"(pid={active_state.get('lock_pid') or 'n/a'}, "
            f"started {_describe_stage_timestamp(active_state.get('lock_created_at'))})"
        )
    elif stale_locks:
        stale_state = stale_locks[0]
        active_detail = (
            f"stale lock detected for {str(stale_state.get('stage') or 'unknown')} "
            f"from {_describe_stage_timestamp(stale_state.get('lock_created_at'))}"
        )
    else:
        active_detail = "no active or stale lock files detected"

    if failed:
        failed_state = max(
            failed,
            key=lambda state: str(state.get("generated_at") or ""),
        )
        failed_detail = (
            f"{str(failed_state.get('stage') or 'unknown')} failed at "
            f"{_describe_stage_timestamp(failed_state.get('generated_at'))}"
        )
    else:
        failed_detail = "no failed stage-state manifests recorded"

    latest_detail = (
        f"{latest_stage} ({latest_status}) at {_describe_stage_timestamp(latest.get('generated_at'))}"
        if latest
        else "no stage-state manifest recorded yet"
    )

    if active:
        active_names = ", ".join(str(state.get("stage") or "unknown") for state in active[:3])
        summary = {
            "status": "running",
            "summary": (
                f"{len(active)} active stage lock(s): {active_names}. "
                f"Latest stage: {latest_stage}."
            ),
            "active_detail": active_detail,
            "failed_detail": failed_detail,
            "latest_detail": latest_detail,
            "next_action": "Wait for the active stage to finish or inspect the latest stage-state JSON if the run looks stuck.",
        }
    elif failed:
        failed_names = ", ".join(str(state.get("stage") or "unknown") for state in failed[:3])
        summary = {
            "status": "attention_needed",
            "summary": f"{len(failed)} stage(s) last reported failure: {failed_names}.",
            "active_detail": active_detail,
            "failed_detail": failed_detail,
            "latest_detail": latest_detail,
            "next_action": "Inspect the latest stage-state manifest and logs before rerunning the failed stage.",
        }
    elif stale_locks:
        summary = {
            "status": "stale_lock_detected",
            "summary": "No live stage locks are active, but stale lock files are present.",
            "active_detail": active_detail,
            "failed_detail": failed_detail,
            "latest_detail": latest_detail,
            "next_action": "Inspect the stage-state folder; the next run will usually recover stale locks automatically.",
        }
    elif states:
        summary = {
            "status": "idle",
            "summary": "No active stage locks are present. The latest completed stage state is available for review.",
            "active_detail": active_detail,
            "failed_detail": failed_detail,
            "latest_detail": latest_detail,
            "next_action": "Use the latest stage-state manifest when verifying that long-running jobs finished cleanly.",
        }
    else:
        summary = {
            "status": "no_stage_history",
            "summary": "No stage-state manifests are available yet.",
            "active_detail": active_detail,
            "failed_detail": failed_detail,
            "latest_detail": latest_detail,
            "next_action": "Run a mutating stage such as Extract to record operational stage-state history.",
        }

    kpis = {
        "active": f"{len(active):,}",
        "running": f"{len(running):,}",
        "failed": f"{len(failed):,}",
        "stale": f"{len(stale_locks):,}",
        "latest": latest_stage,
    }
    return summary, kpis


def build_data_integrity_summary(demo_readiness: DemoReadinessReport) -> tuple[dict[str, str], dict[str, str]]:
    status = demo_readiness.status_snapshot
    problem_count = int(status.processed_rcsb_problem_count or 0)
    empty_count = int(status.processed_rcsb_empty_count or 0)
    corrupt_count = int(status.processed_rcsb_corrupt_count or 0)
    invalid_count = int(status.processed_rcsb_invalid_count or 0)
    valid_count = int(status.processed_rcsb_valid_count or 0)
    total_count = int(status.processed_rcsb_count or 0)
    sample_files = list(status.processed_health_sample_problem_files or [])
    sample_text = ", ".join(sample_files[:5]) if sample_files else "none recorded"
    scan_mode = "cached" if status.processed_health_cache_used else "fresh"
    if status.processed_health_cache_stale:
        scan_mode = "cached-stale"
    generated_at = _describe_stage_timestamp(status.processed_health_generated_at)

    if total_count == 0:
        if status.processed_health_cache_used:
            return (
                {
                    "status": "no_processed_records",
                    "summary": "No processed JSON records are present in the cached integrity snapshot.",
                    "next_action": "Run Normalize after ingest to populate the processed cache.",
                    "detail": (
                        "The GUI is using a cached integrity snapshot for fast startup. "
                        "A full refresh will update this section if processed files changed."
                    ),
                },
                {
                    "valid": "0",
                    "issues": "0",
                    "empty": "0",
                    "corrupt": "0",
                    "invalid": "0",
                    "scan": f"{scan_mode} ({generated_at})",
                },
            )
        return (
            {
                "status": "no_processed_records",
                "summary": "No processed JSON records are present yet.",
                "next_action": "Run Normalize after ingest to populate the processed cache.",
                "detail": "The integrity report will appear after the first processed-record build.",
            },
            {
                "valid": "--",
                "issues": "--",
                "empty": "--",
                "corrupt": "--",
                "invalid": "--",
                "scan": "--",
            },
        )
    if problem_count > 0:
        top_issue = (
            "schema-invalid"
            if invalid_count >= max(empty_count, corrupt_count)
            else "empty"
            if empty_count >= corrupt_count
            else "corrupt"
        )
        return (
            {
                "status": "attention_needed",
                "summary": (
                    f"{problem_count:,} processed JSON file(s) need attention out of {total_count:,}. "
                    f"Most visible issue type: {top_issue}."
                ),
                "next_action": (
                    "Review the health report, run `pbdata clean --processed --delete`, "
                    "then rerun Normalize or Audit if needed."
                ),
                "detail": (
                    f"Empty={empty_count:,}, corrupt={corrupt_count:,}, schema-invalid={invalid_count:,}; "
                    f"examples: {sample_text}."
                ),
            },
            {
                "valid": f"{valid_count:,}",
                "issues": f"{problem_count:,}",
                "empty": f"{empty_count:,}",
                "corrupt": f"{corrupt_count:,}",
                "invalid": f"{invalid_count:,}",
                "scan": f"{scan_mode} ({generated_at})",
            },
        )
    return (
        {
            "status": "healthy",
            "summary": f"Processed cache health looks clean: {valid_count:,} valid JSON record(s), no detected corrupt files.",
            "next_action": "Continue with extract, graph/features, or training workflows.",
            "detail": f"Last integrity scan: {scan_mode} ({generated_at}).",
        },
        {
            "valid": f"{valid_count:,}",
            "issues": "0",
            "empty": "0",
            "corrupt": "0",
            "invalid": "0",
            "scan": f"{scan_mode} ({generated_at})",
        },
    )


def build_source_configuration_summary(
    layout: StorageLayout,
    config: AppConfig,
) -> tuple[dict[str, str], dict[str, str]]:
    report = build_source_capability_report(layout, config)
    counts = report.get("counts") or {}
    summary = {
        "status": str(report.get("status") or "no_sources_enabled"),
        "summary": (
            f"{str(report.get('summary') or 'No source capability summary is available.')}; "
            f"{int(counts.get('implemented_sources') or 0)} implemented, "
            f"{int(counts.get('planned_sources') or 0)} planned."
        ),
        "next_action": str(report.get("next_action") or "Enable and configure sources before ingest."),
    }
    kpis = {
        "enabled": f"{int(counts.get('enabled_sources') or 0):,}",
        "implemented": f"{int(counts.get('implemented_sources') or 0):,}",
        "planned": f"{int(counts.get('planned_sources') or 0):,}",
        "misconfigured": f"{int(counts.get('misconfigured_sources') or 0):,}",
    }
    return summary, kpis


def build_identity_crosswalk_summary(layout: StorageLayout) -> tuple[dict[str, str], dict[str, str]]:
    report = load_json_dict(layout.identity_dir / "identity_crosswalk_summary.json")
    if not report:
        return (
            {
                "status": "no_identity_crosswalk",
                "summary": "No identity crosswalk export is available yet.",
                "next_action": "Run Export Identity Crosswalk after Extract to inspect protein, ligand, and pair mappings.",
            },
            {
                "proteins": "--",
                "ligands": "--",
                "pairs": "--",
                "fallbacks": "--",
            },
        )

    counts = report.get("counts") or {}
    summary = {
        "status": str(report.get("status") or "empty"),
        "summary": str(report.get("summary") or "No identity crosswalk summary is available."),
        "next_action": str(report.get("next_action") or "Inspect fallback mappings before using the crosswalk downstream."),
    }
    kpis = {
        "proteins": f"{int(counts.get('protein_identity_count') or 0):,}",
        "ligands": f"{int(counts.get('ligand_identity_count') or 0):,}",
        "pairs": f"{int(counts.get('pair_identity_count') or 0):,}",
        "fallbacks": f"{int(counts.get('protein_fallback_count') or 0) + int(counts.get('ligand_fallback_count') or 0):,}",
    }
    return summary, kpis


def build_search_preview_summary(layout: StorageLayout) -> tuple[dict[str, str], dict[str, str]]:
    report = load_json_dict(layout.reports_dir / "rcsb_search_preview.json")
    if not report:
        return (
            {
                "status": "no_search_preview",
                "summary": "No RCSB search preview is available yet.",
                "next_action": "Run Preview RCSB Search before ingest when using broad or capped criteria.",
            },
            {
                "total": "--",
                "selected": "--",
                "sample": "--",
                "mode": "--",
            },
        )
    counts = report.get("counts") or {}
    return (
        {
            "status": str(report.get("status") or "ready"),
            "summary": str(report.get("summary") or "No preview summary available."),
            "next_action": str(report.get("next_action") or "Inspect the preview before ingest."),
        },
        {
            "total": f"{int(counts.get('total_match_count') or 0):,}",
            "selected": f"{int(counts.get('selected_match_count') or 0):,}",
            "sample": f"{int(counts.get('preview_sample_count') or 0):,}",
            "mode": str(report.get("selection_mode") or "n/a"),
        },
    )


def build_split_diagnostics_summary(layout: StorageLayout) -> tuple[dict[str, str], dict[str, str]]:
    report = load_json_dict(layout.splits_dir / "split_diagnostics.json")
    metadata = load_json_dict(layout.splits_dir / "metadata.json")
    if not report:
        return (
            {
                "status": "no_split_diagnostics",
                "summary": (
                    "No split diagnostics are available yet."
                    if not metadata
                    else f"Split metadata exists with strategy={metadata.get('strategy') or 'unknown'}, but diagnostics have not been exported yet."
                ),
                "next_action": "Run Build Splits to generate leakage and dominance diagnostics.",
            },
            {
                "strategy": "--",
                "held_out": "--",
                "hard_overlap": "--",
                "family_overlap": "--",
                "source_overlap": "--",
                "fold_overlap": "--",
                "dominance": "--",
            },
        )

    counts = report.get("counts") or {}
    sizes = metadata.get("sizes") or {}
    held_out = int(sizes.get("val") or 0) + int(sizes.get("test") or 0)
    overlap = report.get("overlap") or {}
    dominance = report.get("dominance") or {}
    largest_family_fraction = max(
        (
            float(
                ((dominance.get(split_name) or {}).get("family_key") or {}).get(
                    "largest_group_fraction",
                    0.0,
                )
            )
            for split_name in ("train", "val", "test")
        ),
        default=0.0,
    )
    return (
        {
            "status": str(report.get("status") or "ready"),
            "summary": (
                f"{str(report.get('summary') or 'No split diagnostics summary is available.')} "
                f"Strategy={metadata.get('strategy') or 'unknown'}; held-out items={held_out:,}."
            ),
            "next_action": str(report.get("next_action") or "Inspect the split diagnostics artifact."),
        },
        {
            "strategy": str(report.get("strategy") or metadata.get("strategy") or "n/a"),
            "held_out": f"{held_out:,}",
            "hard_overlap": f"{int(counts.get('hard_group_overlap_count') or 0):,}",
            "family_overlap": f"{int(counts.get('family_overlap_count') or 0):,}",
            "source_overlap": f"{int((overlap.get('source_group_key') or {}).get('overlap_count') or 0):,}",
            "fold_overlap": f"{int(counts.get('fold_overlap_count') or 0):,}",
            "dominance": f"{largest_family_fraction:.1%}",
        },
    )


def build_release_readiness_summary(layout: StorageLayout, repo_root: Path | None = None) -> tuple[dict[str, str], dict[str, str]]:
    report = load_json_dict((repo_root or Path.cwd()) / "release_readiness_report.json")
    if not report:
        return (
            {
                "status": "no_release_readiness_report",
                "summary": "No release readiness report is available yet.",
                "next_action": "Run Release Check before building or shipping a snapshot.",
            },
            {
                "entries": "--",
                "pairs": "--",
                "model_ready": "--",
                "held_out": "--",
                "blockers": "--",
            },
        )
    counts = report.get("counts") or {}
    blockers = report.get("blockers") or []
    split_readiness = report.get("split_readiness") or {}
    strategy = str(split_readiness.get("strategy") or "unknown")
    return (
        {
            "status": str(report.get("release_status") or "blocked"),
            "summary": (
                f"{len(blockers)} blocker(s) and {len(report.get('warnings') or [])} warning(s) remain for the current release surface. "
                f"Split strategy={strategy}."
            ),
            "next_action": (
                "Resolve blockers before using Build Release in strict mode or presenting this as a release-grade dataset."
                if blockers
                else "Review the warnings, then freeze a snapshot when the release surface matches the story you plan to tell."
            ),
        },
            {
                "entries": f"{int(counts.get('canonical_entry_count') or 0):,}",
                "pairs": f"{int(counts.get('canonical_pair_count') or 0):,}",
                "model_ready": f"{int(counts.get('model_ready_pair_count') or 0):,}",
                "held_out": f"{int(counts.get('held_out_count') or 0):,}",
                "blockers": f"{len(blockers):,}",
            },
        )


def build_risk_summary(layout: StorageLayout) -> tuple[dict[str, str], dict[str, str]]:
    report = load_json_dict(layout.risk_dir / "pathway_risk_summary.json")
    if not report:
        return (
            {
                "status": "no_risk_summary",
                "summary": "No pathway/risk context summary is available yet.",
                "next_action": "Run Score Pathway Risk after prediction and graph/pathway artifacts are available.",
            },
            {
                "severity": "--",
                "score": "--",
                "matches": "--",
                "pathways": "--",
            },
        )

    summary = {
        "status": str(report.get("status") or "graph_context_summary_not_clinical_risk_model"),
        "summary": str(report.get("notes") or "No pathway/risk notes available."),
        "next_action": (
            "Use this as biological context for the demo, not as a calibrated clinical risk claim."
            if report.get("risk_score_is_placeholder") is False
            else "Keep this panel framed as placeholder context until a meaningful risk model exists."
        ),
    }
    kpis = {
        "severity": str(report.get("severity_level") or "n/a"),
        "score": f"{float(report.get('risk_score') or 0.0):.3f}",
        "matches": f"{int(report.get('matching_pair_count') or 0):,}",
        "pathways": f"{int(report.get('pathway_overlap_count') or 0):,}",
    }
    return summary, kpis


def build_prediction_status_summary(layout: StorageLayout) -> tuple[dict[str, str], dict[str, str]]:
    manifest = load_json_dict(layout.prediction_dir / "ligand_screening" / "prediction_manifest.json")
    ranked = manifest.get("ranked_target_list") if isinstance(manifest.get("ranked_target_list"), list) else []
    top_target = ranked[0] if ranked and isinstance(ranked[0], dict) else {}
    notes = str(manifest.get("notes") or "").strip()
    if not notes:
        if manifest:
            notes = "Prediction artifacts are present. Review the selected model and top-ranked targets before presenting them."
        else:
            notes = "No ligand-screening prediction manifest available."
    summary = {
        "status": str(manifest.get("status") or "no_prediction_manifest"),
        "method": str(manifest.get("prediction_method") or "n/a"),
        "preference": str(manifest.get("selected_model_preference") or "n/a"),
        "summary": notes,
    }
    kpis = {
        "targets": f"{int(manifest.get('candidate_target_count') or 0):,}",
        "top_target": str(top_target.get("target_id") or "n/a"),
        "confidence": (
            f"{float(top_target.get('confidence_score') or 0.0):.3f}"
            if top_target
            else "--"
        ),
        "query_features": f"{int(manifest.get('query_numeric_feature_count') or 0):,}",
    }
    return summary, kpis


def build_recommended_workflow_summary(
    review_paths: dict[str, str],
    training_quality_report: dict[str, Any],
    model_comparison_report: dict[str, Any],
    demo_readiness: DemoReadinessReport,
) -> dict[str, str]:
    def _path_exists(key: str) -> bool:
        value = str(review_paths.get(key) or "").strip()
        return bool(value) and Path(value).exists()

    training_status = str(training_quality_report.get("status") or "empty")
    training_counts = training_quality_report.get("counts") or {}
    split_counts = training_quality_report.get("split_counts") or {}
    comparison_status = str(model_comparison_report.get("status") or "missing_models")
    processed_problem_count = int(demo_readiness.status_snapshot.processed_rcsb_problem_count or 0)

    if processed_problem_count > 0:
        return {
            "phase": "Repair data integrity",
            "summary": "Clean up broken normalized records first so counts, previews, and downstream artifacts stay trustworthy during the demo.",
            "step_1": "1. Run `pbdata clean --processed --delete` to remove empty or corrupt normalized files.",
            "step_2": "2. Re-run Normalize and Audit so the processed cache and review artifacts are coherent again.",
            "step_3": "3. Refresh downstream reports or splits only after the processed issue count returns to zero.",
        }

    if "no_extracted_entries" in demo_readiness.blockers:
        return {
            "phase": "Build core dataset",
            "summary": "Build the first end-to-end dataset slice so the demo has real records, review outputs, and a visible story to walk through.",
            "step_1": "1. Run Ingest Sources, then Extract Multi-Table.",
            "step_2": "2. Run Normalize Records and Audit Quality to create review artifacts.",
            "step_3": "3. Use Review Health to resolve blockers before moving downstream.",
        }

    if not _path_exists("model_ready_pairs_csv"):
        return {
            "phase": "Review and release core data",
            "summary": "The workspace has extracted content, but it still needs a reviewed, presentation-safe model-ready pair pool.",
            "step_1": "1. Refresh review exports and inspect issues, conflicts, and exclusions.",
            "step_2": "2. Resolve or accept blockers until model_ready_pairs.csv is present.",
            "step_3": "3. Build a release snapshot only after the review surface looks consistent.",
        }

    if int(training_counts.get("example_count") or 0) == 0:
        return {
            "phase": "Build training corpus",
            "summary": "Core data is ready, but the demo still needs training examples so modeling and benchmarking screens show meaningful outputs.",
            "step_1": "1. Run Build Graph, Build Features, and Build Training Examples.",
            "step_2": "2. Run Build Splits so train, validation, and test sets exist.",
            "step_3": "3. Export the Training Set Quality report and inspect supervision and conflict coverage.",
        }

    if int(split_counts.get("train") or 0) == 0 or (
        int(split_counts.get("val") or 0) == 0 and int(split_counts.get("test") or 0) == 0
    ):
        return {
            "phase": "Benchmark the corpus",
            "summary": "Training examples exist, but the held-out evaluation story is not yet strong enough for a confident comparison.",
            "step_1": "1. Run Build Splits with the intended leakage-control settings.",
            "step_2": "2. Re-export the Training Set Quality report and inspect overlap-with-train diagnostics.",
            "step_3": "3. Only train or compare models after at least one held-out split exists.",
        }

    if comparison_status != "comparison_ready":
        return {
            "phase": "Train and compare models",
            "summary": "The corpus is usable, but the side-by-side model story is incomplete until both baseline and learned evaluations are available.",
            "step_1": "1. Run Evaluate Baseline Model to establish the conservative reference.",
            "step_2": "2. Run Evaluate Tabular Affinity Model to produce the learned-model comparison.",
            "step_3": "3. Export Model Comparison and inspect the validation winner before presenting results.",
        }

    if training_status in {"weak_supervision", "usable_with_gaps", "undersized"}:
        return {
            "phase": "Improve training quality",
            "summary": "Models can be compared, but the current corpus still has supervision or conflict gaps that need to be called out carefully in a demo.",
            "step_1": "1. Review the Training Example Quality panel for conflict rate and label coverage gaps.",
            "step_2": "2. Improve extraction or curation, then rebuild training examples and splits.",
            "step_3": "3. Re-run both model evaluations before treating a winner as stable.",
        }

    return {
        "phase": "Package and present results",
        "summary": "Core data, training quality, and model comparison artifacts are all present. The remaining work is demo polish and message discipline.",
        "step_1": "1. Inspect Training Example Quality and Model Comparison together before choosing the default model story.",
        "step_2": "2. Export Demo Snapshot so the current workspace state is captured for walkthroughs and follow-up notes.",
        "step_3": "3. Use Demo Readiness warnings to separate what is production-like today from what is still baseline or in progress.",
    }


def _completion_row(area: str, current: str, target: str, gap: str, status: str) -> dict[str, str]:
    return {
        "area": area,
        "current": current,
        "target": target,
        "gap": gap,
        "status": status,
    }


def build_completion_status_snapshot(
    layout: StorageLayout,
    config: AppConfig,
    *,
    repo_root: Path | None = None,
    review_paths: dict[str, str],
    training_quality_report: dict[str, Any],
    model_comparison_report: dict[str, Any],
    demo_readiness: DemoReadinessReport,
    release_readiness_summary: dict[str, str],
    release_readiness_kpis: dict[str, str],
    source_configuration_summary: dict[str, str],
    source_configuration_kpis: dict[str, str],
    data_integrity_summary: dict[str, str],
    data_integrity_kpis: dict[str, str],
) -> tuple[dict[str, str], list[dict[str, str]]]:
    _ = (layout, config)
    root = repo_root or Path.cwd()
    release_report = load_json_dict(root / "release_readiness_report.json")
    blockers = release_report.get("blockers") or []
    warnings = release_report.get("warnings") or []
    training_counts = training_quality_report.get("counts") or {}
    split_counts = training_quality_report.get("split_counts") or {}
    comparison_status = str(model_comparison_report.get("status") or "missing_models")
    enabled_sources = _coerce_int(source_configuration_kpis.get("enabled"))
    misconfigured_sources = _coerce_int(source_configuration_kpis.get("misconfigured"))
    processed_issues = _coerce_int(data_integrity_kpis.get("issues"))
    example_count = _coerce_int(training_counts.get("example_count"))
    model_ready_count = _coerce_int(release_readiness_kpis.get("model_ready"))
    train_count = _coerce_int(split_counts.get("train"))
    held_out_count = _coerce_int(split_counts.get("val")) + _coerce_int(split_counts.get("test"))
    demo_warning_count = len(demo_readiness.warnings)

    rows = [
        _completion_row(
            "Source setup",
            f"{enabled_sources} enabled; {misconfigured_sources} need attention",
            "Demo sources enabled and correctly configured",
            (
                "Fix missing source paths or enable the sources you plan to show."
                if source_configuration_summary.get("status") != "ready" or misconfigured_sources > 0
                else "Aligned with the planned demo source surface."
            ),
            "done" if enabled_sources > 0 and misconfigured_sources == 0 else "partial" if enabled_sources > 0 else "not_started",
        ),
        _completion_row(
            "Data integrity",
            f"{processed_issues} processed issue(s)",
            "Zero corrupt or empty processed records",
            (
                "Run the processed clean-up flow and rebuild normalized records."
                if processed_issues > 0 or data_integrity_summary.get("status") == "attention_needed"
                else "No cleanup gap remains."
            ),
            "done" if processed_issues == 0 and data_integrity_summary.get("status") == "healthy" else "blocked" if processed_issues > 0 else "partial",
        ),
        _completion_row(
            "Core dataset",
            f"{demo_readiness.status_snapshot.extracted_entry_count:,} extracted entries",
            "Extracted entries, assays, and review exports available",
            (
                "Run ingest, extract, normalize, and audit to populate the core workspace."
                if "no_extracted_entries" in demo_readiness.blockers
                else "Core dataset slice exists."
            ),
            "done" if "no_extracted_entries" not in demo_readiness.blockers else "not_started",
        ),
        _completion_row(
            "Release review",
            f"{model_ready_count:,} model-ready pairs; {len(blockers)} blocker(s)",
            "Model-ready pool present with blockers resolved",
            (
                "Review issues, conflicts, and exclusions until the release surface is clear."
                if model_ready_count == 0 or blockers
                else "Release review surface is in place."
            ),
            "done" if model_ready_count > 0 and not blockers else "partial" if model_ready_count > 0 else "not_started",
        ),
        _completion_row(
            "Training corpus",
            f"{example_count:,} training example(s)",
            "Training examples built from the reviewed dataset",
            (
                "Build graph, features, and training examples."
                if example_count == 0
                else "Corpus exists; keep improving coverage and supervision quality."
            ),
            "done" if example_count > 0 else "not_started",
        ),
        _completion_row(
            "Split benchmarking",
            f"{train_count:,} train / {held_out_count:,} held-out examples",
            "Train plus held-out validation or test splits",
            (
                "Generate splits with held-out coverage before comparing models."
                if train_count == 0 or held_out_count == 0
                else "Held-out evaluation coverage is present."
            ),
            "done" if train_count > 0 and held_out_count > 0 else "partial" if train_count > 0 else "not_started",
        ),
        _completion_row(
            "Model comparison",
            comparison_status.replace("_", " "),
            "Baseline and learned model comparison ready",
            (
                "Run both evaluation stages and inspect the comparison report."
                if comparison_status != "comparison_ready"
                else "Comparison artifacts are ready for presentation."
            ),
            "done" if comparison_status == "comparison_ready" else "partial" if example_count > 0 else "not_started",
        ),
        _completion_row(
            "Demo packaging",
            f"{demo_readiness.readiness}; {demo_warning_count} warning(s)",
            "Walkthrough snapshot, talking points, and honest caveats ready",
            (
                "Export the demo snapshot and tighten the remaining warnings."
                if demo_readiness.readiness != "ready_for_internal_demo"
                else "Demo package is ready for a disciplined walkthrough."
            ),
            "done" if demo_readiness.readiness == "ready_for_internal_demo" else "partial" if not demo_readiness.blockers else "blocked",
        ),
    ]
    done_count = sum(1 for row in rows if row["status"] == "done")
    blocked_count = sum(1 for row in rows if row["status"] == "blocked")
    not_started_count = sum(1 for row in rows if row["status"] == "not_started")
    summary = {
        "status": (
            "complete"
            if done_count == len(rows)
            else "blocked"
            if blocked_count > 0
            else "in_progress"
        ),
        "headline": f"{done_count} of {len(rows)} completion areas are demo-ready.",
        "detail": (
            f"{blocked_count} blocked, {not_started_count} not started, {len(rows) - done_count - blocked_count - not_started_count} in progress."
        ),
        "next_action": (
            "Focus on the blocked rows first, then refresh the walkthrough snapshot."
            if blocked_count > 0
            else "Use the partial rows as the remaining checklist before the demo."
        ),
    }
    return summary, rows


def _describe_file_freshness(path: Path) -> str:
    if not path.exists():
        return "not generated yet"
    updated_at = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
    age = datetime.now(timezone.utc) - updated_at
    total_minutes = max(int(age.total_seconds() // 60), 0)
    if total_minutes < 1:
        age_text = "just now"
    elif total_minutes < 60:
        age_text = f"{total_minutes} min ago"
    elif total_minutes < 1440:
        age_text = f"{total_minutes // 60} hr ago"
    else:
        age_text = f"{total_minutes // 1440} day(s) ago"
    return f"{age_text} ({updated_at.strftime('%Y-%m-%d %H:%M UTC')})"


def build_presenter_banner(
    completion_summary: dict[str, str],
    workflow_guidance: dict[str, str],
    demo_readiness: DemoReadinessReport,
    release_readiness_summary: dict[str, str],
    prediction_status_summary: dict[str, str],
) -> dict[str, str]:
    readiness = str(demo_readiness.readiness or "unknown")
    headline = "Customer demo rehearsal workspace"
    if readiness == "ready_for_internal_demo":
        headline = "Customer-ready baseline walkthrough"
    elif readiness == "technically_reviewable_not_polished":
        headline = "Technical preview with explicit caveats"

    strongest_surface = "core dataset build"
    if str(prediction_status_summary.get("status") or "") != "no_prediction_manifest":
        strongest_surface = "prediction and ranking outputs"
    elif str(release_readiness_summary.get("status") or "") == "ready":
        strongest_surface = "release-grade dataset outputs"
    elif str(completion_summary.get("status") or "") in {"in_progress", "complete"}:
        strongest_surface = "reviewed dataset and workflow status"

    return {
        "headline": headline,
        "subhead": (
            f"Current phase: {workflow_guidance.get('phase', 'Unknown')}. "
            f"Strongest surface today: {strongest_surface}."
        ),
        "state": str(completion_summary.get("headline") or ""),
        "next_step": str(workflow_guidance.get("step_1") or completion_summary.get("next_action") or ""),
    }


def build_artifact_freshness_summary(
    layout: StorageLayout,
    review_paths: dict[str, str],
    *,
    repo_root: Path | None = None,
) -> dict[str, str]:
    root = repo_root or Path.cwd()
    return {
        "release_check": _describe_file_freshness(root / "release_readiness_report.json"),
        "demo_snapshot": _describe_file_freshness(layout.feature_reports_dir / "demo_walkthrough.md"),
        "prediction_manifest": _describe_file_freshness(layout.prediction_dir / "ligand_screening" / "prediction_manifest.json"),
        "risk_summary": _describe_file_freshness(layout.risk_dir / "pathway_risk_summary.json"),
        "model_comparison": _describe_file_freshness(layout.models_dir / "model_comparison_report.json"),
        "training_quality": _describe_file_freshness(layout.training_dir / "training_quality_report.json"),
        "release_manifest": _describe_file_freshness(Path(review_paths.get("release_manifest_json") or root / "dataset_release_manifest.json")),
    }


def build_last_run_summary(layout: StorageLayout) -> dict[str, str]:
    states = list_stage_activity(layout)
    if not states:
        return {
            "status": "no_history",
            "summary": "No workflow stages have recorded state yet.",
            "last_stage": "n/a",
            "last_result": "n/a",
            "next_action": "Run a stage from the GUI to populate workflow history.",
        }
    latest = max(states, key=lambda state: str(state.get("generated_at") or ""))
    last_stage = str(latest.get("stage") or "unknown")
    last_result = str(latest.get("status") or "unknown")
    failed_count = sum(1 for state in states if str(state.get("status") or "") == "failed")
    running_count = sum(1 for state in states if str(state.get("status") or "") in {"running", "completed_with_failures"})
    return {
        "status": "attention_needed" if failed_count > 0 else "running" if running_count > 0 else "stable",
        "summary": (
            f"Latest recorded stage: {last_stage} ({last_result}) at {_describe_stage_timestamp(latest.get('generated_at'))}."
        ),
        "last_stage": last_stage,
        "last_result": last_result,
        "next_action": (
            "Inspect the failed stage manifest or rerun the affected stage before the demo."
            if failed_count > 0
            else "Let the active workflow finish before refreshing the story."
            if running_count > 0
            else "Use this as the anchor for what the audience is seeing right now."
        ),
    }


def _format_model_gap(split_payload: dict[str, Any]) -> str:
    baseline = split_payload.get("baseline_affinity_mae_log10")
    tabular = split_payload.get("tabular_affinity_mae_log10")
    try:
        baseline_value = float(baseline)
        tabular_value = float(tabular)
    except (TypeError, ValueError):
        return "--"
    return f"{(baseline_value - tabular_value):+.3f}"


def build_curation_review_summary(
    exclusion_rows: list[dict[str, str]],
    conflict_rows: list[dict[str, str]],
    issue_rows: list[dict[str, str]],
) -> dict[str, str]:
    exclusion_counts = Counter(str(row.get("reason") or "unknown") for row in exclusion_rows)
    conflict_bands = Counter(str(row.get("source_agreement_band") or "unknown") for row in conflict_rows)
    issue_counts = Counter(str(row.get("issue_type") or "unknown") for row in issue_rows)

    top_exclusions = ", ".join(
        f"{reason}={count}"
        for reason, count in exclusion_counts.most_common(3)
    ) or "none"
    top_issues = ", ".join(
        f"{issue}={count}"
        for issue, count in issue_counts.most_common(3)
    ) or "none"
    conflict_summary = ", ".join(
        f"{band}={count}"
        for band, count in conflict_bands.most_common(3)
    ) or "none"

    next_action = "Build custom set and review exclusions."
    if conflict_rows:
        next_action = "Review conflicted pairs before freezing a release."
    elif exclusion_rows:
        next_action = "Inspect top exclusion reasons and retune selection mode or cluster cap."

    return {
        "exclusions": f"{len(exclusion_rows):,} rows; top reasons: {top_exclusions}",
        "conflicts": f"{len(conflict_rows):,} rows; agreement bands: {conflict_summary}",
        "issues": f"{len(issue_rows):,} rows; top issue types: {top_issues}",
        "next_action": next_action,
    }


def review_export_paths(layout: StorageLayout, repo_root: Path | None = None) -> dict[str, str]:
    repo_root = repo_root or Path.cwd()
    latest_release_path = layout.releases_dir / "latest_release.json"
    names = {
        "master_csv": "master_pdb_repository.csv",
        "pair_csv": "master_pdb_pairs.csv",
        "issue_csv": "master_pdb_issues.csv",
        "conflict_csv": "master_pdb_conflicts.csv",
        "source_state_csv": "master_source_state.csv",
        "model_ready_pairs_csv": "model_ready_pairs.csv",
        "custom_training_set_csv": "custom_training_set.csv",
        "custom_training_exclusions_csv": "custom_training_exclusions.csv",
        "custom_training_summary_json": "custom_training_summary.json",
        "custom_training_scorecard_json": "custom_training_scorecard.json",
        "custom_training_split_benchmark_csv": "custom_training_split_benchmark.csv",
        "release_manifest_json": "dataset_release_manifest.json",
        "split_summary_csv": "split_summary.csv",
        "scientific_coverage_json": "scientific_coverage_summary.json",
    }
    paths: dict[str, str] = {}
    for key, filename in names.items():
        path = repo_root / filename if not Path(filename).is_absolute() else Path(filename)
        paths[key] = str(path) if path.exists() else ""
    paths["latest_release_json"] = str(latest_release_path) if latest_release_path.exists() else ""
    return paths


@dataclass(frozen=True)
class GUIOverviewSnapshot:
    counts: dict[str, str]
    presenter_banner: dict[str, str]
    review_paths: dict[str, str]
    review_health: dict[str, str]
    completion_summary: dict[str, str]
    completion_rows: list[dict[str, str]]
    artifact_freshness: dict[str, str]
    last_run_summary: dict[str, str]
    training_summary: dict[str, str]
    training_kpis: dict[str, str]
    training_workflow: dict[str, str]
    training_quality_summary: dict[str, str]
    training_quality_kpis: dict[str, str]
    model_comparison_summary: dict[str, str]
    model_comparison_kpis: dict[str, str]
    split_diagnostics_summary: dict[str, str]
    split_diagnostics_kpis: dict[str, str]
    search_preview_summary: dict[str, str]
    search_preview_kpis: dict[str, str]
    source_configuration_summary: dict[str, str]
    source_configuration_kpis: dict[str, str]
    source_run_summary: dict[str, str]
    source_run_kpis: dict[str, str]
    data_integrity_summary: dict[str, str]
    data_integrity_kpis: dict[str, str]
    active_operations_summary: dict[str, str]
    active_operations_kpis: dict[str, str]
    identity_crosswalk_summary: dict[str, str]
    identity_crosswalk_kpis: dict[str, str]
    release_readiness_summary: dict[str, str]
    release_readiness_kpis: dict[str, str]
    risk_summary: dict[str, str]
    risk_kpis: dict[str, str]
    prediction_status_summary: dict[str, str]
    prediction_status_kpis: dict[str, str]
    workflow_guidance: dict[str, str]
    curation_summary: dict[str, str]
    demo_readiness: DemoReadinessReport


def build_gui_overview_snapshot(
    layout: StorageLayout,
    config: AppConfig,
    *,
    repo_root: Path | None = None,
    prefer_cached_status: bool = False,
) -> GUIOverviewSnapshot:
    paths = review_export_paths(layout, repo_root=repo_root)
    coverage_path = Path(paths["scientific_coverage_json"]) if paths.get("scientific_coverage_json") else None
    scorecard_path = Path(paths["custom_training_scorecard_json"]) if paths.get("custom_training_scorecard_json") else None
    benchmark_path = Path(paths["custom_training_split_benchmark_csv"]) if paths.get("custom_training_split_benchmark_csv") else None
    exclusion_path = Path(paths["custom_training_exclusions_csv"]) if paths.get("custom_training_exclusions_csv") else None
    conflict_path = Path(paths["conflict_csv"]) if paths.get("conflict_csv") else None
    issue_path = Path(paths["issue_csv"]) if paths.get("issue_csv") else None
    training_quality_report = build_training_set_quality_report(layout)
    model_comparison_report = build_model_comparison_report(layout)
    status_snapshot = build_status_report(layout, prefer_cached_health=prefer_cached_status)
    doctor_report = build_doctor_report(layout, config, status_snapshot=status_snapshot)
    demo_readiness = build_demo_readiness_report(
        layout,
        config,
        status_snapshot=status_snapshot,
        doctor_report=doctor_report,
    )
    split_diagnostics_summary, split_diagnostics_kpis = build_split_diagnostics_summary(layout)
    search_preview_summary, search_preview_kpis = build_search_preview_summary(layout)
    source_configuration_summary, source_configuration_kpis = build_source_configuration_summary(layout, config)
    source_run_summary, source_run_kpis = build_source_run_summary(layout)
    data_integrity_summary, data_integrity_kpis = build_data_integrity_summary(demo_readiness)
    active_operations_summary, active_operations_kpis = build_active_operations_summary(layout)
    identity_crosswalk_summary, identity_crosswalk_kpis = build_identity_crosswalk_summary(layout)
    release_readiness_summary, release_readiness_kpis = build_release_readiness_summary(layout, repo_root=repo_root)
    risk_summary, risk_kpis = build_risk_summary(layout)
    prediction_status_summary, prediction_status_kpis = build_prediction_status_summary(layout)
    workflow_guidance = build_recommended_workflow_summary(
        paths,
        training_quality_report,
        model_comparison_report,
        demo_readiness,
    )
    completion_summary, completion_rows = build_completion_status_snapshot(
        layout,
        config,
        repo_root=repo_root,
        review_paths=paths,
        training_quality_report=training_quality_report,
        model_comparison_report=model_comparison_report,
        demo_readiness=demo_readiness,
        release_readiness_summary=release_readiness_summary,
        release_readiness_kpis=release_readiness_kpis,
        source_configuration_summary=source_configuration_summary,
        source_configuration_kpis=source_configuration_kpis,
        data_integrity_summary=data_integrity_summary,
        data_integrity_kpis=data_integrity_kpis,
    )
    presenter_banner = build_presenter_banner(
        completion_summary,
        workflow_guidance,
        demo_readiness,
        release_readiness_summary,
        prediction_status_summary,
    )
    artifact_freshness = build_artifact_freshness_summary(layout, paths, repo_root=repo_root)
    last_run_summary = build_last_run_summary(layout)

    workflow_status = build_training_set_workflow_status(paths)
    workflow_key_map = {
        "Model-ready pool": "model_ready",
        "Custom set": "custom_set",
        "Scorecard": "scorecard",
        "Benchmark": "benchmark",
        "Release": "release",
    }

    return GUIOverviewSnapshot(
        counts={
            "raw_rcsb": f"{int(status_snapshot.raw_rcsb_count or 0):,}",
            "raw_skempi": "Yes" if (layout.raw_skempi_dir / "skempi_v2.csv").exists() else "No",
            "processed": f"{int(status_snapshot.processed_rcsb_count or 0):,}",
            "processed_valid": f"{int(status_snapshot.processed_rcsb_valid_count or 0):,}",
            "processed_issues": f"{int(status_snapshot.processed_rcsb_problem_count or 0):,}",
            "extracted": f"{int(status_snapshot.extracted_entry_count or 0):,}",
            "chains": f"{count_files(layout.extracted_dir / 'chains'):,}",
            "bound_objects": f"{count_files(layout.extracted_dir / 'bound_objects'):,}",
            "assays": f"{count_files(layout.extracted_dir / 'assays'):,}",
            "graph_nodes": f"{count_files(layout.graph_dir, 'graph_nodes*'):,}",
            "graph_edges": f"{count_files(layout.graph_dir, 'graph_edges*'):,}",
            "splits": f"{count_files(layout.splits_dir, '*.txt'):,}",
        },
        presenter_banner=presenter_banner,
        review_paths=paths,
        review_health=build_review_health_summary(load_json_dict(coverage_path) if coverage_path else {}),
        completion_summary=completion_summary,
        completion_rows=completion_rows,
        artifact_freshness=artifact_freshness,
        last_run_summary=last_run_summary,
        training_summary=build_training_set_builder_summary(
            load_json_dict(scorecard_path) if scorecard_path else {},
            load_csv_dict_rows(benchmark_path) if benchmark_path else [],
        ),
        training_kpis=build_training_set_kpis(
            load_json_dict(scorecard_path) if scorecard_path else {},
            load_csv_dict_rows(benchmark_path) if benchmark_path else [],
        ),
        training_workflow={
            workflow_key_map[label]: value
            for label, value in workflow_status
            if label in workflow_key_map
        },
        training_quality_summary=build_training_quality_summary(training_quality_report),
        training_quality_kpis=build_training_quality_kpis(training_quality_report),
        model_comparison_summary=build_model_comparison_summary(model_comparison_report),
        model_comparison_kpis=build_model_comparison_kpis(model_comparison_report),
        split_diagnostics_summary=split_diagnostics_summary,
        split_diagnostics_kpis=split_diagnostics_kpis,
        search_preview_summary=search_preview_summary,
        search_preview_kpis=search_preview_kpis,
        source_configuration_summary=source_configuration_summary,
        source_configuration_kpis=source_configuration_kpis,
        source_run_summary=source_run_summary,
        source_run_kpis=source_run_kpis,
        data_integrity_summary=data_integrity_summary,
        data_integrity_kpis=data_integrity_kpis,
        active_operations_summary=active_operations_summary,
        active_operations_kpis=active_operations_kpis,
        identity_crosswalk_summary=identity_crosswalk_summary,
        identity_crosswalk_kpis=identity_crosswalk_kpis,
        release_readiness_summary=release_readiness_summary,
        release_readiness_kpis=release_readiness_kpis,
        risk_summary=risk_summary,
        risk_kpis=risk_kpis,
        prediction_status_summary=prediction_status_summary,
        prediction_status_kpis=prediction_status_kpis,
        workflow_guidance=workflow_guidance,
        curation_summary=build_curation_review_summary(
            load_csv_dict_rows(exclusion_path) if exclusion_path else [],
            load_csv_dict_rows(conflict_path) if conflict_path else [],
            load_csv_dict_rows(issue_path) if issue_path else [],
        ),
        demo_readiness=demo_readiness,
    )
