"""Stable release-oriented exports built from the current repo contents."""

from __future__ import annotations

import csv
import json
import shutil
from datetime import datetime, timezone
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from pbdata.config import AppConfig, load_config
from pbdata.model_comparison import build_model_comparison_report
from pbdata.sources.registry import build_source_capability_report
from pbdata.storage import StorageLayout
from pbdata.training_quality import build_training_set_quality_report

_CANONICAL_ENTRY_CSV = "canonical_entries.csv"
_CANONICAL_PAIR_CSV = "canonical_pairs.csv"
_MODEL_READY_PAIR_CSV = "model_ready_pairs.csv"
_MODEL_READY_EXCLUSIONS_CSV = "model_ready_exclusions.csv"
_SPLIT_SUMMARY_CSV = "split_summary.csv"
_RELEASE_MANIFEST_JSON = "dataset_release_manifest.json"
_SCIENTIFIC_COVERAGE_JSON = "scientific_coverage_summary.json"
_RELEASE_READINESS_JSON = "release_readiness_report.json"
_SNAPSHOT_MANIFEST_JSON = "release_snapshot_manifest.json"
_LATEST_RELEASE_JSON = "latest_release.json"

_MODEL_READY_BLOCKERS = {
    "missing_structure_file",
    "no_matched_interface",
    "ambiguous_mutation_context",
    "source_value_conflict",
    "non_high_confidence_assay_fields",
}

_RELEASE_GRADE_SPLIT_STRATEGIES = {
    "pair_aware_grouped",
    "scaffold_grouped",
    "family_grouped",
    "mutation_grouped",
    "source_grouped",
    "time_ordered",
}

_EXPLORATORY_SPLIT_STRATEGIES = {
    "hash",
    "cluster_aware",
}


def _repo_path(name: str, repo_root: Path | None = None) -> Path:
    return (repo_root or Path.cwd()) / name


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _write_csv(path: Path, columns: list[str], rows: list[dict[str, str]]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)
    tmp.replace(path)
    return path


def _stringify(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, list):
        return "; ".join(str(item) for item in value if item not in (None, ""))
    if isinstance(value, dict):
        return json.dumps(value, sort_keys=True)
    return str(value)


def _issue_map(issue_rows: list[dict[str, str]]) -> tuple[dict[tuple[str, str], set[str]], dict[str, set[str]]]:
    pair_issues: dict[tuple[str, str], set[str]] = defaultdict(set)
    entry_issues: dict[str, set[str]] = defaultdict(set)
    for row in issue_rows:
        issue_type = str(row.get("issue_type") or "")
        if not issue_type:
            continue
        pdb_id = str(row.get("pdb_id") or "")
        pair_key = str(row.get("pair_identity_key") or "")
        if pdb_id and not pair_key:
            entry_issues[pdb_id].add(issue_type)
        if pdb_id and pair_key:
            pair_issues[(pdb_id, pair_key)].add(issue_type)
    return pair_issues, entry_issues


def _counter_dict(values: list[str]) -> dict[str, int]:
    return dict(Counter(value for value in values if value))


def _semicolon_values(raw: str) -> list[str]:
    return [value.strip() for value in str(raw or "").split(";") if value.strip()]


def _split_readiness_summary(
    split_metadata: dict[str, Any],
    split_diagnostics: dict[str, Any],
) -> dict[str, Any]:
    strategy = str(split_metadata.get("strategy") or "").strip() or "unknown"
    sizes = split_metadata.get("sizes") or {}
    val_count = int(sizes.get("val") or 0)
    test_count = int(sizes.get("test") or 0)
    held_out_count = val_count + test_count
    diagnostics_counts = split_diagnostics.get("counts") or {}
    overlap = split_diagnostics.get("overlap") or {}
    split_status = str(split_diagnostics.get("status") or "missing")
    strategy_family = (
        "release_grade"
        if strategy in _RELEASE_GRADE_SPLIT_STRATEGIES
        else "exploratory"
        if strategy in _EXPLORATORY_SPLIT_STRATEGIES
        else "unknown"
    )
    recommended_usage = (
        "Suitable for leakage-aware held-out benchmarking."
        if strategy_family == "release_grade" and held_out_count > 0 and split_status == "ready"
        else "Usable for exploratory iteration, but not yet strong enough for release-grade benchmark claims."
        if strategy_family in {"release_grade", "exploratory"}
        else "Split strategy is not yet classified for benchmark readiness."
    )
    return {
        "strategy": strategy,
        "strategy_family": strategy_family,
        "held_out_count": held_out_count,
        "held_out_ready": held_out_count > 0,
        "diagnostic_status": split_status,
        "hard_group_overlap_count": int(diagnostics_counts.get("hard_group_overlap_count") or 0),
        "family_overlap_count": int(diagnostics_counts.get("family_overlap_count") or 0),
        "source_overlap_count": int((overlap.get("source_group_key") or {}).get("overlap_count") or 0),
        "domain_overlap_count": int(diagnostics_counts.get("domain_overlap_count") or 0),
        "pathway_overlap_count": int(diagnostics_counts.get("pathway_overlap_count") or 0),
        "fold_overlap_count": int(diagnostics_counts.get("fold_overlap_count") or 0),
        "recommended_usage": recommended_usage,
    }


def export_scientific_coverage_summary(
    layout: StorageLayout,
    *,
    repo_root: Path | None = None,
) -> Path:
    """Summarize scientific breadth from the current canonical/review exports.

    Assumption:
    - This is a review/reporting artifact only. It does not infer missing biology;
      it summarizes what the current extracted dataset already records.
    """
    from pbdata.master_export import master_csv_path, pair_master_csv_path, issue_csv_path

    root = repo_root or Path.cwd()
    master_rows = _read_csv(master_csv_path(root))
    pair_rows = _read_csv(pair_master_csv_path(root))
    issue_rows = _read_csv(issue_csv_path(root))
    release_manifest_path = _repo_path(_RELEASE_MANIFEST_JSON, root)
    release_manifest = json.loads(release_manifest_path.read_text(encoding="utf-8")) if release_manifest_path.exists() else {}

    entries_with_structure = sum(1 for row in master_rows if str(row.get("structure_file_cif_path") or "").strip())
    pairs_with_conflicts = sum(1 for row in pair_rows if str(row.get("source_conflict_flag") or "").lower() == "true")
    mutant_pairs = sum(1 for row in pair_rows if str(row.get("binding_affinity_is_mutant_measurement") or "").lower() == "true")

    issue_counts = Counter(str(row.get("issue_type") or "") for row in issue_rows if row.get("issue_type"))
    model_ready_count = int(release_manifest.get("model_ready_pair_count") or 0)

    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "storage_root": str(layout.root),
        "counts": {
            "entry_count": len(master_rows),
            "pair_count": len(pair_rows),
            "issue_count": len(issue_rows),
            "entries_with_structure_file": entries_with_structure,
            "pairs_with_source_conflicts": pairs_with_conflicts,
            "mutant_measurement_pairs": mutant_pairs,
            "model_ready_pair_count": model_ready_count,
        },
        "coverage": {
            "experimental_methods": _counter_dict([str(row.get("experimental_method") or "") for row in master_rows]),
            "membrane_vs_soluble": _counter_dict([str(row.get("membrane_vs_soluble") or "") for row in master_rows]),
            "homomer_or_heteromer": _counter_dict([str(row.get("homomer_or_heteromer") or "") for row in master_rows]),
            "assay_types": _counter_dict([str(row.get("binding_affinity_type") or "") for row in pair_rows]),
            "assay_sources": _counter_dict([str(row.get("source_database") or "") for row in pair_rows]),
            "preferred_sources": _counter_dict([str(row.get("selected_preferred_source") or "") for row in pair_rows]),
            "source_agreement_bands": _counter_dict([str(row.get("source_agreement_band") or "") for row in pair_rows]),
            "ligand_types": _counter_dict([
                ligand_type
                for row in pair_rows
                for ligand_type in _semicolon_values(str(row.get("ligand_types") or ""))
            ]),
            "interface_types": _counter_dict([
                interface_type
                for row in pair_rows
                for interface_type in _semicolon_values(str(row.get("matching_interface_types") or ""))
            ]),
            "issue_types": dict(issue_counts),
        },
        "flags": {
            "metal_present_entries": sum(1 for row in master_rows if str(row.get("metal_present") or "").lower() == "true"),
            "cofactor_present_entries": sum(1 for row in master_rows if str(row.get("cofactor_present") or "").lower() == "true"),
            "glycan_present_entries": sum(1 for row in master_rows if str(row.get("glycan_present") or "").lower() == "true"),
            "covalent_binder_entries": sum(1 for row in master_rows if str(row.get("covalent_binder_present") or "").lower() == "true"),
            "peptide_partner_entries": sum(1 for row in master_rows if str(row.get("peptide_partner_present") or "").lower() == "true"),
        },
        "release": {
            "policy_version": str(release_manifest.get("release_policy_version") or ""),
            "model_ready_exclusion_count": int(release_manifest.get("model_ready_exclusion_count") or 0),
            "model_ready_exclusion_reasons": release_manifest.get("model_ready_exclusion_reasons") or {},
            "split_metadata": release_manifest.get("split_metadata") or {},
        },
    }

    out_path = _repo_path(_SCIENTIFIC_COVERAGE_JSON, root)
    out_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    return out_path


def export_release_artifacts(
    layout: StorageLayout,
    *,
    repo_root: Path | None = None,
) -> dict[str, str]:
    """Write stable canonical/release exports from current root review files."""
    from pbdata.master_export import (
        issue_csv_path,
        master_csv_path,
        pair_master_csv_path,
    )

    root = repo_root or Path.cwd()
    master_rows = _read_csv(master_csv_path(root))
    pair_rows = _read_csv(pair_master_csv_path(root))
    issue_rows = _read_csv(issue_csv_path(root))
    pair_issues, entry_issues = _issue_map(issue_rows)

    canonical_entry_columns = [
        "pdb_id",
        "title",
        "experimental_method",
        "structure_resolution",
        "release_date",
        "membrane_vs_soluble",
        "oligomeric_state",
        "homomer_or_heteromer",
        "taxonomy_ids",
        "organism_names",
        "protein_entity_count",
        "nonpolymer_entity_count",
        "branched_entity_count",
        "metal_present",
        "cofactor_present",
        "glycan_present",
        "covalent_binder_present",
        "peptide_partner_present",
        "quality_flags",
        "quality_score",
        "structure_file_cif_path",
        "raw_file_path",
        "field_confidence_json",
    ]
    canonical_entries = [
        {column: str(row.get(column) or "") for column in canonical_entry_columns}
        for row in master_rows
    ]
    canonical_entries.sort(key=lambda row: row["pdb_id"])
    canonical_entries_path = _write_csv(
        _repo_path(_CANONICAL_ENTRY_CSV, root),
        canonical_entry_columns,
        canonical_entries,
    )

    canonical_pair_columns = [
        "pdb_id",
        "pair_identity_key",
        "source_database",
        "binding_affinity_type",
        "binding_affinity_value",
        "binding_affinity_unit",
        "binding_affinity_log10_standardized",
        "reported_measurements_text",
        "reported_measurement_mean_log10_standardized",
        "reported_measurement_count",
        "source_conflict_flag",
        "source_conflict_summary",
        "source_agreement_band",
        "selected_preferred_source",
        "selected_preferred_source_rationale",
        "receptor_chain_ids",
        "receptor_uniprot_ids",
        "ligand_key",
        "ligand_component_ids",
        "ligand_inchikeys",
        "ligand_types",
        "matching_interface_count",
        "matching_interface_types",
        "assay_field_confidence_json",
    ]
    canonical_pairs = [
        {column: str(row.get(column) or "") for column in canonical_pair_columns}
        for row in pair_rows
    ]
    canonical_pairs.sort(key=lambda row: (row["pdb_id"], row["pair_identity_key"], row["binding_affinity_type"]))
    canonical_pairs_path = _write_csv(
        _repo_path(_CANONICAL_PAIR_CSV, root),
        canonical_pair_columns,
        canonical_pairs,
    )

    model_ready_columns = canonical_pair_columns + ["release_split", "model_ready_policy_version"]
    model_ready_rows: list[dict[str, str]] = []
    exclusion_rows: list[dict[str, str]] = []

    training_examples_path = layout.training_dir / "training_examples.json"
    split_assignment: dict[tuple[str, str, str], str] = {}
    if training_examples_path.exists():
        raw = json.loads(training_examples_path.read_text(encoding="utf-8"))
        examples = raw if isinstance(raw, list) else []
        example_lookup: dict[str, tuple[str, str, str]] = {}
        for row in examples:
            if not isinstance(row, dict):
                continue
            example_id = str(row.get("example_id") or "")
            provenance = row.get("provenance") or {}
            labels = row.get("labels") or {}
            pair_key = str(provenance.get("pair_identity_key") or "")
            affinity_type = str(labels.get("affinity_type") or "")
            pdb_id = str((row.get("structure") or {}).get("pdb_id") or "")
            if example_id and pair_key:
                example_lookup[example_id] = (pdb_id, pair_key, affinity_type)
        for split_name in ("train", "val", "test"):
            split_path = layout.splits_dir / f"{split_name}.txt"
            if not split_path.exists():
                continue
            for line in split_path.read_text(encoding="utf-8").splitlines():
                item_id = line.strip()
                if item_id in example_lookup:
                    split_assignment[example_lookup[item_id]] = split_name

    exclusion_counts: Counter[str] = Counter()
    for row in pair_rows:
        pdb_id = str(row.get("pdb_id") or "")
        pair_key = str(row.get("pair_identity_key") or "")
        affinity_type = str(row.get("binding_affinity_type") or "")
        issue_types = sorted(pair_issues.get((pdb_id, pair_key), set()) | entry_issues.get(pdb_id, set()))

        reasons: list[str] = []
        blockers = [issue for issue in issue_types if issue in _MODEL_READY_BLOCKERS]
        if blockers:
            reasons.extend(blockers)
        if str(row.get("selected_preferred_source") or "") == "":
            reasons.append("missing_preferred_source")
        if str(row.get("binding_affinity_log10_standardized") or "") == "":
            reasons.append("missing_standardized_affinity")

        if reasons:
            unique_reasons = sorted(set(reasons))
            exclusion_rows.append({
                "pdb_id": pdb_id,
                "pair_identity_key": pair_key,
                "binding_affinity_type": affinity_type,
                "exclusion_reasons": "; ".join(unique_reasons),
                "issue_types": "; ".join(issue_types),
            })
            exclusion_counts.update(unique_reasons)
            continue

        model_ready_row = {
            column: str(row.get(column) or "")
            for column in canonical_pair_columns
        }
        model_ready_row["release_split"] = split_assignment.get((pdb_id, pair_key, affinity_type), "")
        model_ready_row["model_ready_policy_version"] = "v1"
        model_ready_rows.append(model_ready_row)

    model_ready_rows.sort(key=lambda row: (row["pdb_id"], row["pair_identity_key"], row["binding_affinity_type"]))
    model_ready_path = _write_csv(
        _repo_path(_MODEL_READY_PAIR_CSV, root),
        model_ready_columns,
        model_ready_rows,
    )
    exclusion_path = _write_csv(
        _repo_path(_MODEL_READY_EXCLUSIONS_CSV, root),
        ["pdb_id", "pair_identity_key", "binding_affinity_type", "exclusion_reasons", "issue_types"],
        exclusion_rows,
    )

    split_metadata_path = layout.splits_dir / "metadata.json"
    split_metadata = {}
    if split_metadata_path.exists():
        split_metadata = json.loads(split_metadata_path.read_text(encoding="utf-8"))
    split_diagnostics_path = layout.splits_dir / "split_diagnostics.json"
    split_diagnostics = (
        json.loads(split_diagnostics_path.read_text(encoding="utf-8"))
        if split_diagnostics_path.exists()
        else {}
    )
    split_diagnostics_counts = split_diagnostics.get("counts") or {}
    split_readiness = _split_readiness_summary(split_metadata, split_diagnostics)
    split_rows: list[dict[str, str]] = []
    for split_name in ("train", "val", "test"):
        split_path = layout.splits_dir / f"{split_name}.txt"
        item_ids = [
            line.strip() for line in split_path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ] if split_path.exists() else []
        ready_for_split = [row for row in model_ready_rows if row.get("release_split") == split_name]
        split_rows.append({
            "split_name": split_name,
            "split_item_count": str(len(item_ids)),
            "model_ready_pair_count": str(len(ready_for_split)),
            "affinity_types": "; ".join(sorted({row.get("binding_affinity_type", "") for row in ready_for_split if row.get("binding_affinity_type")})),
            "source_databases": "; ".join(sorted({row.get("selected_preferred_source", "") for row in ready_for_split if row.get("selected_preferred_source")})),
            "strategy": _stringify(split_metadata.get("strategy") if split_name == "train" else ""),
            "strategy_family": _stringify(split_readiness.get("strategy_family") if split_name == "train" else ""),
            "held_out_ready": _stringify(split_readiness.get("held_out_ready") if split_name == "train" else ""),
            "diagnostic_status": _stringify(split_diagnostics.get("status") if split_name == "train" else ""),
            "hard_group_overlap_count": _stringify(split_diagnostics_counts.get("hard_group_overlap_count") if split_name == "train" else ""),
            "family_overlap_count": _stringify(split_diagnostics_counts.get("family_overlap_count") if split_name == "train" else ""),
            "source_overlap_count": _stringify(split_readiness.get("source_overlap_count") if split_name == "train" else ""),
            "domain_overlap_count": _stringify(split_readiness.get("domain_overlap_count") if split_name == "train" else ""),
            "pathway_overlap_count": _stringify(split_readiness.get("pathway_overlap_count") if split_name == "train" else ""),
            "fold_overlap_count": _stringify(split_readiness.get("fold_overlap_count") if split_name == "train" else ""),
            "recommended_usage": _stringify(split_readiness.get("recommended_usage") if split_name == "train" else ""),
        })
    split_summary_path = _write_csv(
        _repo_path(_SPLIT_SUMMARY_CSV, root),
        [
            "split_name",
            "split_item_count",
            "model_ready_pair_count",
            "affinity_types",
            "source_databases",
            "strategy",
            "strategy_family",
            "held_out_ready",
            "diagnostic_status",
            "hard_group_overlap_count",
            "family_overlap_count",
            "source_overlap_count",
            "domain_overlap_count",
            "pathway_overlap_count",
            "fold_overlap_count",
            "recommended_usage",
        ],
        split_rows,
    )

    release_manifest = {
        "release_policy_version": "v1",
        "canonical_entry_count": len(canonical_entries),
        "canonical_pair_count": len(canonical_pairs),
        "model_ready_pair_count": len(model_ready_rows),
        "model_ready_exclusion_count": len(exclusion_rows),
        "model_ready_exclusion_reasons": dict(exclusion_counts),
        "split_metadata": split_metadata,
        "split_diagnostics": split_diagnostics,
        "split_readiness": split_readiness,
        "artifacts": {
            "canonical_entries_csv": str(canonical_entries_path),
            "canonical_pairs_csv": str(canonical_pairs_path),
            "model_ready_pairs_csv": str(model_ready_path),
            "model_ready_exclusions_csv": str(exclusion_path),
            "split_summary_csv": str(split_summary_path),
        },
    }
    release_manifest_path = _repo_path(_RELEASE_MANIFEST_JSON, root)
    release_manifest_path.write_text(json.dumps(release_manifest, indent=2), encoding="utf-8")

    scientific_coverage_path = export_scientific_coverage_summary(layout, repo_root=root)

    return {
        "canonical_entries_csv": str(canonical_entries_path),
        "canonical_pairs_csv": str(canonical_pairs_path),
        "model_ready_pairs_csv": str(model_ready_path),
        "model_ready_exclusions_csv": str(exclusion_path),
        "split_summary_csv": str(split_summary_path),
        "release_manifest_json": str(release_manifest_path),
        "scientific_coverage_json": str(scientific_coverage_path),
    }


def build_release_readiness_report(
    layout: StorageLayout,
    *,
    repo_root: Path | None = None,
) -> tuple[Path, dict[str, Any]]:
    """Build a release-readiness report with explicit blockers and warnings."""
    root = repo_root or Path.cwd()
    artifacts = export_release_artifacts(layout, repo_root=root)
    manifest_path = Path(artifacts["release_manifest_json"])
    manifest = json.loads(manifest_path.read_text(encoding="utf-8")) if manifest_path.exists() else {}

    blockers: list[str] = []
    warnings: list[str] = []
    canonical_entry_count = int(manifest.get("canonical_entry_count") or 0)
    canonical_pair_count = int(manifest.get("canonical_pair_count") or 0)
    model_ready_pair_count = int(manifest.get("model_ready_pair_count") or 0)
    split_metadata = manifest.get("split_metadata") or {}
    split_diagnostics = manifest.get("split_diagnostics") or {}
    split_readiness = manifest.get("split_readiness") or _split_readiness_summary(split_metadata, split_diagnostics)
    exclusion_reasons = manifest.get("model_ready_exclusion_reasons") or {}

    if canonical_entry_count <= 0:
        blockers.append("no_canonical_entries")
    if canonical_pair_count <= 0:
        blockers.append("no_canonical_pairs")
    if model_ready_pair_count <= 0:
        blockers.append("no_model_ready_pairs")
    if not split_metadata:
        blockers.append("no_split_metadata")
    elif not bool(split_readiness.get("held_out_ready")):
        blockers.append("no_held_out_split")

    cfg_path = layout.root / "configs" / "sources.yaml"
    try:
        config = load_config(cfg_path) if cfg_path.exists() else AppConfig()
    except Exception:
        config = AppConfig()
    source_capabilities = build_source_capability_report(layout, config)
    training_quality = build_training_set_quality_report(layout)
    model_comparison = build_model_comparison_report(layout)
    identity_summary_path = layout.identity_dir / "identity_crosswalk_summary.json"
    identity_summary = (
        json.loads(identity_summary_path.read_text(encoding="utf-8"))
        if identity_summary_path.exists()
        else {}
    )

    source_status = str(source_capabilities.get("status") or "")
    if source_status == "needs_configuration":
        blockers.append("source_configuration_incomplete")

    training_status = str(training_quality.get("status") or "")
    if training_status in {"empty", "weak_supervision"}:
        blockers.append("training_corpus_not_ready")

    if exclusion_reasons:
        warnings.append("model_ready_exclusions_present")
    if str(split_diagnostics.get("status") or "") == "leakage_risk":
        blockers.append("split_leakage_risk_detected")
    elif str(split_diagnostics.get("status") or "") in {"dominance_risk", "attention_needed"}:
        warnings.append("split_diagnostics_attention_needed")
    if str(split_readiness.get("strategy_family") or "") == "exploratory":
        warnings.append("exploratory_split_strategy")
    split_counts = split_diagnostics.get("counts") or {}
    if any(
        int(split_counts.get(key) or 0) > 0
        for key in ("domain_overlap_count", "pathway_overlap_count", "fold_overlap_count")
    ):
        warnings.append("metadata_group_overlap_detected")
    if int(split_readiness.get("source_overlap_count") or 0) > 0:
        warnings.append("source_group_overlap_detected")
    if training_status == "undersized":
        warnings.append("training_corpus_not_release_grade")
    training_counts = training_quality.get("counts") or {}
    metadata_annotation_scope = max(
        canonical_pair_count,
        model_ready_pair_count,
        int(training_counts.get("unique_pair_count") or 0),
        int(training_counts.get("example_count") or 0),
    )
    if metadata_annotation_scope > 0 and int(training_counts.get("unique_metadata_family_count") or 0) == 0:
        warnings.append("metadata_family_annotations_missing")
    if model_comparison.get("status") != "comparison_ready":
        warnings.append("model_comparison_not_ready")
    if not identity_summary:
        warnings.append("identity_crosswalk_missing")
    else:
        counts = identity_summary.get("counts") or {}
        pair_count = int(counts.get("pair_identity_count") or 0)
        pair_partial_or_unresolved_count = int(counts.get("pair_partial_or_unresolved_count") or 0)
        pair_partial_fraction = (
            float(pair_partial_or_unresolved_count) / pair_count
            if pair_count
            else 0.0
        )
        if pair_count and pair_partial_fraction > 0.10:
            warnings.append("identity_crosswalk_contains_many_fallbacks")
    if not (layout.models_dir / "ligand_memory_model.json").exists():
        warnings.append("no_baseline_model_artifact")

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "storage_root": str(layout.root),
        "repo_root": str(root),
        "release_status": "ready" if not blockers else "blocked",
        "blockers": blockers,
        "warnings": warnings,
        "counts": {
            "canonical_entry_count": canonical_entry_count,
            "canonical_pair_count": canonical_pair_count,
            "model_ready_pair_count": model_ready_pair_count,
            "model_ready_exclusion_count": int(manifest.get("model_ready_exclusion_count") or 0),
            "held_out_count": int(split_readiness.get("held_out_count") or 0),
        },
        "split_readiness": split_readiness,
        "quality_gates": {
            "source_capabilities": {
                "status": source_status,
                "counts": source_capabilities.get("counts") or {},
            },
            "training_quality": {
                "status": training_status,
                "counts": training_quality.get("counts") or {},
            },
            "model_comparison": {
                "status": model_comparison.get("status"),
                "available_models": model_comparison.get("available_models") or {},
            },
            "identity_crosswalk": {
                "status": identity_summary.get("status") or "missing",
                "counts": identity_summary.get("counts") or {},
            },
        },
        "supported_release_surface": {
            "gui_framework": "tkinter",
            "authoritative_release_artifacts": [
                _CANONICAL_ENTRY_CSV,
                _CANONICAL_PAIR_CSV,
                _MODEL_READY_PAIR_CSV,
                _MODEL_READY_EXCLUSIONS_CSV,
                _SPLIT_SUMMARY_CSV,
                _RELEASE_MANIFEST_JSON,
                _SCIENTIFIC_COVERAGE_JSON,
            ],
            "experimental_or_baseline_surfaces": [
                "risk_summary",
                "prediction_manifests",
                "site_physics_surrogate_without_offline_labels",
            ],
        },
        "artifacts": artifacts,
    }
    out_path = _repo_path(_RELEASE_READINESS_JSON, root)
    out_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    return out_path, report


def build_release_snapshot(
    layout: StorageLayout,
    *,
    release_tag: str | None = None,
    repo_root: Path | None = None,
    strict: bool = False,
) -> dict[str, str]:
    """Freeze the current release artifacts into a versioned snapshot directory."""
    root = repo_root or Path.cwd()
    artifacts = export_release_artifacts(layout, repo_root=root)
    readiness_path, readiness = build_release_readiness_report(layout, repo_root=root)
    artifacts["release_readiness_json"] = str(readiness_path)
    if strict and readiness.get("blockers"):
        raise ValueError(f"Release blocked: {', '.join(str(item) for item in readiness['blockers'])}")
    review_artifacts = {
        "master_csv": str(_repo_path("master_pdb_repository.csv", root)),
        "pair_csv": str(_repo_path("master_pdb_pairs.csv", root)),
        "issue_csv": str(_repo_path("master_pdb_issues.csv", root)),
        "conflict_csv": str(_repo_path("master_pdb_conflicts.csv", root)),
        "source_state_csv": str(_repo_path("master_source_state.csv", root)),
        "custom_training_set_csv": str(_repo_path("custom_training_set.csv", root)),
        "custom_training_exclusions_csv": str(_repo_path("custom_training_exclusions.csv", root)),
        "custom_training_summary_json": str(_repo_path("custom_training_summary.json", root)),
        "custom_training_scorecard_json": str(_repo_path("custom_training_scorecard.json", root)),
        "custom_training_split_benchmark_csv": str(_repo_path("custom_training_split_benchmark.csv", root)),
        "custom_training_manifest_json": str(_repo_path("custom_training_manifest.json", root)),
        "release_readiness_json": str(readiness_path),
    }
    for key, path in review_artifacts.items():
        if Path(path).exists():
            artifacts[key] = path

    tag = release_tag or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    snapshot_dir = layout.releases_dir / tag
    snapshot_dir.mkdir(parents=True, exist_ok=True)

    copied: dict[str, str] = {}
    for key, source in artifacts.items():
        src = Path(source)
        if not src.exists():
            continue
        dst = snapshot_dir / src.name
        shutil.copy2(src, dst)
        copied[key] = str(dst)

    snapshot_manifest = {
        "release_tag": tag,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "storage_root": str(layout.root),
        "repo_root": str(root),
        "artifacts": copied,
    }
    snapshot_manifest_path = snapshot_dir / _SNAPSHOT_MANIFEST_JSON
    snapshot_manifest_path.write_text(json.dumps(snapshot_manifest, indent=2), encoding="utf-8")

    latest_payload = {
        "release_tag": tag,
        "snapshot_dir": str(snapshot_dir),
        "snapshot_manifest": str(snapshot_manifest_path),
        "created_at": snapshot_manifest["created_at"],
    }
    latest_path = layout.releases_dir / _LATEST_RELEASE_JSON
    latest_path.parent.mkdir(parents=True, exist_ok=True)
    latest_path.write_text(json.dumps(latest_payload, indent=2), encoding="utf-8")

    copied["release_snapshot_manifest_json"] = str(snapshot_manifest_path)
    copied["release_snapshot_dir"] = str(snapshot_dir)
    copied["latest_release_json"] = str(latest_path)
    return copied
