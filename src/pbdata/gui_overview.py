"""Shared GUI overview summaries and workspace snapshot assembly."""

from __future__ import annotations

import csv
import json
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pbdata.config import AppConfig
from pbdata.model_comparison import build_model_comparison_report
from pbdata.storage import StorageLayout
from pbdata.training_quality import build_training_set_quality_report
from pbdata.workspace_state import DemoReadinessReport, build_demo_readiness_report


def count_files(directory: Path, pattern: str = "*.json") -> int:
    if not directory.exists():
        return 0
    return sum(1 for _ in directory.glob(pattern))


def load_csv_dict_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def load_json_dict(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return raw if isinstance(raw, dict) else {}


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


def build_prediction_status_summary(layout: StorageLayout) -> tuple[dict[str, str], dict[str, str]]:
    manifest = load_json_dict(layout.prediction_dir / "ligand_screening" / "prediction_manifest.json")
    ranked = manifest.get("ranked_target_list") if isinstance(manifest.get("ranked_target_list"), list) else []
    top_target = ranked[0] if ranked and isinstance(ranked[0], dict) else {}
    summary = {
        "status": str(manifest.get("status") or "no_prediction_manifest"),
        "method": str(manifest.get("prediction_method") or "n/a"),
        "preference": str(manifest.get("selected_model_preference") or "n/a"),
        "summary": str(manifest.get("notes") or "No ligand-screening prediction manifest available."),
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

    if "no_extracted_entries" in demo_readiness.blockers:
        return {
            "phase": "Build core dataset",
            "summary": "Populate the workspace from raw sources through extracted records before using the ML workflow.",
            "step_1": "1. Run Ingest Sources, then Extract Multi-Table.",
            "step_2": "2. Run Normalize Records and Audit Quality to create review artifacts.",
            "step_3": "3. Use Review Health to resolve blockers before moving downstream.",
        }

    if not _path_exists("model_ready_pairs_csv"):
        return {
            "phase": "Review and release core data",
            "summary": "The workspace has extracted content, but it does not yet have a reviewed model-ready pair pool.",
            "step_1": "1. Refresh review exports and inspect issues, conflicts, and exclusions.",
            "step_2": "2. Resolve or accept blockers until model_ready_pairs.csv is present.",
            "step_3": "3. Build a release snapshot only after the review surface looks consistent.",
        }

    if int(training_counts.get("example_count") or 0) == 0:
        return {
            "phase": "Build training corpus",
            "summary": "Core data is ready, but there are no training examples yet for benchmarking or model training.",
            "step_1": "1. Run Build Graph, Build Features, and Build Training Examples.",
            "step_2": "2. Run Build Splits so train, validation, and test sets exist.",
            "step_3": "3. Export the Training Set Quality report and inspect supervision and conflict coverage.",
        }

    if int(split_counts.get("train") or 0) == 0 or (
        int(split_counts.get("val") or 0) == 0 and int(split_counts.get("test") or 0) == 0
    ):
        return {
            "phase": "Benchmark the corpus",
            "summary": "Training examples exist, but the split structure is not yet strong enough for a trustworthy model comparison.",
            "step_1": "1. Run Build Splits with the intended leakage-control settings.",
            "step_2": "2. Re-export the Training Set Quality report and inspect overlap-with-train diagnostics.",
            "step_3": "3. Only train or compare models after at least one held-out split exists.",
        }

    if comparison_status != "comparison_ready":
        return {
            "phase": "Train and compare models",
            "summary": "The corpus is usable, but baseline and tabular evaluation artifacts are not both ready yet.",
            "step_1": "1. Run Evaluate Baseline Model to establish the conservative reference.",
            "step_2": "2. Run Evaluate Tabular Affinity Model to produce the learned-model comparison.",
            "step_3": "3. Export Model Comparison and inspect the validation winner before presenting results.",
        }

    if training_status in {"weak_supervision", "usable_with_gaps"}:
        return {
            "phase": "Improve training quality",
            "summary": "Models can be compared, but the current corpus still has supervision or conflict gaps that limit trust.",
            "step_1": "1. Review the Training Example Quality panel for conflict rate and label coverage gaps.",
            "step_2": "2. Improve extraction or curation, then rebuild training examples and splits.",
            "step_3": "3. Re-run both model evaluations before treating a winner as stable.",
        }

    return {
        "phase": "Package and present results",
        "summary": "Core data, training quality, and model comparison artifacts are all present. Focus on review and presentation.",
        "step_1": "1. Inspect Training Example Quality and Model Comparison together before choosing the default model.",
        "step_2": "2. Export Demo Snapshot so the current workspace state is captured for walkthroughs.",
        "step_3": "3. Use Demo Readiness warnings to frame baseline versus production-ready claims honestly.",
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
    review_paths: dict[str, str]
    review_health: dict[str, str]
    training_summary: dict[str, str]
    training_kpis: dict[str, str]
    training_workflow: dict[str, str]
    training_quality_summary: dict[str, str]
    training_quality_kpis: dict[str, str]
    model_comparison_summary: dict[str, str]
    model_comparison_kpis: dict[str, str]
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
    demo_readiness = build_demo_readiness_report(layout, config)
    prediction_status_summary, prediction_status_kpis = build_prediction_status_summary(layout)

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
            "raw_rcsb": f"{count_files(layout.raw_rcsb_dir):,}",
            "raw_skempi": "Yes" if (layout.raw_skempi_dir / "skempi_v2.csv").exists() else "No",
            "processed": f"{count_files(layout.processed_rcsb_dir):,}",
            "extracted": f"{count_files(layout.extracted_dir / 'entry'):,}",
            "chains": f"{count_files(layout.extracted_dir / 'chains'):,}",
            "bound_objects": f"{count_files(layout.extracted_dir / 'bound_objects'):,}",
            "assays": f"{count_files(layout.extracted_dir / 'assays'):,}",
            "graph_nodes": f"{count_files(layout.graph_dir, 'graph_nodes*'):,}",
            "graph_edges": f"{count_files(layout.graph_dir, 'graph_edges*'):,}",
            "splits": f"{count_files(layout.splits_dir, '*.txt'):,}",
        },
        review_paths=paths,
        review_health=build_review_health_summary(load_json_dict(coverage_path) if coverage_path else {}),
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
        prediction_status_summary=prediction_status_summary,
        prediction_status_kpis=prediction_status_kpis,
        workflow_guidance=build_recommended_workflow_summary(
            paths,
            training_quality_report,
            model_comparison_report,
            demo_readiness,
        ),
        curation_summary=build_curation_review_summary(
            load_csv_dict_rows(exclusion_path) if exclusion_path else [],
            load_csv_dict_rows(conflict_path) if conflict_path else [],
            load_csv_dict_rows(issue_path) if issue_path else [],
        ),
        demo_readiness=demo_readiness,
    )
