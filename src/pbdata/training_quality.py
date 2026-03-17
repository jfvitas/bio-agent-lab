from __future__ import annotations

import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pbdata.pairing import parse_pair_identity_key
from pbdata.storage import StorageLayout

_MIN_EXAMPLES_FOR_USABLE_CORPUS = 25
_MIN_UNIQUE_TARGETS_FOR_USABLE_CORPUS = 5
_MIN_UNIQUE_LIGANDS_FOR_USABLE_CORPUS = 5
_MIN_EXAMPLES_FOR_STRONG_CORPUS = 100
_MIN_UNIQUE_TARGETS_FOR_STRONG_CORPUS = 10
_MIN_UNIQUE_LIGANDS_FOR_STRONG_CORPUS = 10


def _read_json(path: Path) -> Any:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _read_split_ids(path: Path) -> set[str]:
    if not path.exists():
        return set()
    return {line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip()}


def _safe_dict(value: object) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _split_assignment_by_example(
    examples: list[dict[str, Any]],
    layout: StorageLayout,
) -> dict[str, str]:
    assignment: dict[str, str] = {}
    key_to_example_ids: dict[tuple[str, str], list[str]] = {}
    example_ids: set[str] = set()
    for row in examples:
        if not isinstance(row, dict):
            continue
        example_id = str(row.get("example_id") or "").strip()
        provenance = _safe_dict(row.get("provenance"))
        labels = _safe_dict(row.get("labels"))
        pair_key = str(provenance.get("pair_identity_key") or "").strip()
        affinity_type = str(labels.get("affinity_type") or "").strip()
        if example_id and pair_key:
            example_ids.add(example_id)
            key_to_example_ids.setdefault((pair_key, affinity_type), []).append(example_id)

    for split_name in ("train", "val", "test"):
        for item_id in _read_split_ids(layout.splits_dir / f"{split_name}.txt"):
            if item_id in example_ids:
                assignment[item_id] = split_name
                continue
            matched = False
            pair_key, _, affinity_type = item_id.rpartition("|")
            if pair_key:
                normalized_affinity = "" if affinity_type == "assay_unknown" else affinity_type
                for example_id in key_to_example_ids.get((pair_key, normalized_affinity), []):
                    assignment[example_id] = split_name
                    matched = True
            if matched:
                continue
            parsed = parse_pair_identity_key(item_id)
            if parsed is not None:
                for (pair_key, _), example_id_rows in key_to_example_ids.items():
                    if pair_key == item_id:
                        for example_id in example_id_rows:
                            assignment[example_id] = split_name
    return assignment


def build_training_set_quality_report(layout: StorageLayout) -> dict[str, Any]:
    raw = _read_json(layout.training_dir / "training_examples.json")
    examples = [row for row in raw if isinstance(row, dict)] if isinstance(raw, list) else []
    split_diagnostics = _read_json(layout.splits_dir / "split_diagnostics.json")
    split_diagnostics = split_diagnostics if isinstance(split_diagnostics, dict) else {}
    metadata_rows = _read_csv_rows(layout.workspace_metadata_dir / "protein_metadata.csv")
    split_assignment = _split_assignment_by_example(examples, layout)

    example_count = len(examples)
    supervised_count = 0
    source_conflict_count = 0
    has_graph_count = 0
    has_smiles_count = 0
    has_target_count = 0
    mutation_count = 0
    degraded_count = 0
    unique_targets: set[str] = set()
    unique_ligands: set[str] = set()
    unique_pairs: set[str] = set()
    unique_interpro_families: set[str] = set()
    unique_pathways: set[str] = set()
    affinity_type_counts: dict[str, int] = {}
    source_database_counts: dict[str, int] = {}
    source_agreement_counts: dict[str, int] = {}
    missing_field_counts = {
        "labels": 0,
        "binding_affinity_log10": 0,
        "affinity_type": 0,
        "target_id": 0,
        "ligand_smiles": 0,
        "graph_data": 0,
    }
    supervision_blockers = {
        "missing_labels": 0,
        "missing_affinity_log10": 0,
        "missing_affinity_type": 0,
    }
    split_counts = {"train": 0, "val": 0, "test": 0, "unsplit": 0}
    train_targets: set[str] = set()
    train_ligands: set[str] = set()
    train_pairs: set[str] = set()
    metadata_by_pair: dict[str, dict[str, str]] = {}
    for row in metadata_rows:
        pair_key = str(row.get("pair_identity_key") or "").strip()
        if pair_key and pair_key not in metadata_by_pair:
            metadata_by_pair[pair_key] = row

    normalized_rows: list[dict[str, str | bool]] = []
    for row in examples:
        labels = _safe_dict(row.get("labels"))
        protein = _safe_dict(row.get("protein"))
        ligand = _safe_dict(row.get("ligand"))
        provenance = _safe_dict(row.get("provenance"))
        example_id = str(row.get("example_id") or "")
        target_id = str(protein.get("uniprot_id") or "").strip()
        ligand_smiles = str(ligand.get("smiles") or "").strip()
        pair_key = str(provenance.get("pair_identity_key") or "").strip()
        affinity_type = str(labels.get("affinity_type") or "").strip() or "unknown"
        source_database = str(
            provenance.get("preferred_source_database")
            or provenance.get("source_database")
            or ""
        ).strip() or "unknown"
        source_agreement = str(provenance.get("source_agreement_band") or "").strip() or "unknown"
        is_supervised = labels.get("binding_affinity_log10") not in (None, "")
        has_graph = bool(provenance.get("has_graph_data"))
        has_smiles = bool(ligand_smiles)
        has_target = bool(target_id)
        is_mutation = bool(labels.get("is_mutant"))
        degraded_mode = bool(provenance.get("degraded_mode"))

        if not labels:
            missing_field_counts["labels"] += 1
            supervision_blockers["missing_labels"] += 1
        if labels.get("binding_affinity_log10") in (None, ""):
            missing_field_counts["binding_affinity_log10"] += 1
            supervision_blockers["missing_affinity_log10"] += 1
        if not affinity_type or affinity_type == "unknown":
            missing_field_counts["affinity_type"] += 1
            supervision_blockers["missing_affinity_type"] += 1
        if not has_target:
            missing_field_counts["target_id"] += 1
        if not has_smiles:
            missing_field_counts["ligand_smiles"] += 1
        if not has_graph:
            missing_field_counts["graph_data"] += 1

        if is_supervised:
            supervised_count += 1
        if bool(labels.get("source_conflict_flag") or provenance.get("source_conflict_flag")):
            source_conflict_count += 1
        if has_graph:
            has_graph_count += 1
        if has_smiles:
            has_smiles_count += 1
        if has_target:
            has_target_count += 1
        if target_id:
            unique_targets.add(target_id)
        if ligand_smiles:
            unique_ligands.add(ligand_smiles)
        if pair_key:
            unique_pairs.add(pair_key)
            metadata_row = metadata_by_pair.get(pair_key) or {}
            for family_id in str(metadata_row.get("interpro_ids") or metadata_row.get("pfam_ids") or "").replace(";", ",").split(","):
                family_id = family_id.strip()
                if family_id:
                    unique_interpro_families.add(family_id)
            for pathway_id in str(metadata_row.get("reactome_pathway_ids") or "").replace(";", ",").split(","):
                pathway_id = pathway_id.strip()
                if pathway_id:
                    unique_pathways.add(pathway_id)
        if is_mutation:
            mutation_count += 1
        if degraded_mode:
            degraded_count += 1
        affinity_type_counts[affinity_type] = affinity_type_counts.get(affinity_type, 0) + 1
        source_database_counts[source_database] = source_database_counts.get(source_database, 0) + 1
        source_agreement_counts[source_agreement] = source_agreement_counts.get(source_agreement, 0) + 1

        split_name = split_assignment.get(example_id, "unsplit")
        split_counts[split_name] += 1
        normalized_rows.append(
            {
                "example_id": example_id,
                "target_id": target_id,
                "ligand_smiles": ligand_smiles,
                "pair_identity_key": pair_key,
                "split_name": split_name,
                "is_supervised": is_supervised,
            }
        )
        if split_name == "train":
            if target_id:
                train_targets.add(target_id)
            if ligand_smiles:
                train_ligands.add(ligand_smiles)
            if pair_key:
                train_pairs.add(pair_key)

    overlap = {
        "val": {"same_target_in_train": 0, "same_ligand_in_train": 0, "exact_pair_seen_in_train": 0, "novel_examples": 0},
        "test": {"same_target_in_train": 0, "same_ligand_in_train": 0, "exact_pair_seen_in_train": 0, "novel_examples": 0},
    }
    for row in normalized_rows:
        split_name = str(row["split_name"])
        if split_name not in overlap:
            continue
        target_id = str(row["target_id"] or "")
        ligand_smiles = str(row["ligand_smiles"] or "")
        pair_key = str(row["pair_identity_key"] or "")
        same_target = bool(target_id and target_id in train_targets)
        same_ligand = bool(ligand_smiles and ligand_smiles in train_ligands)
        exact_pair = bool(pair_key and pair_key in train_pairs)
        if same_target:
            overlap[split_name]["same_target_in_train"] += 1
        if same_ligand:
            overlap[split_name]["same_ligand_in_train"] += 1
        if exact_pair:
            overlap[split_name]["exact_pair_seen_in_train"] += 1
        if not same_target and not same_ligand:
            overlap[split_name]["novel_examples"] += 1

    supervised_fraction = (supervised_count / example_count) if example_count else 0.0
    graph_fraction = (has_graph_count / example_count) if example_count else 0.0
    conflict_fraction = (source_conflict_count / example_count) if example_count else 0.0
    mutation_fraction = (mutation_count / example_count) if example_count else 0.0
    degraded_fraction = (degraded_count / example_count) if example_count else 0.0
    target_coverage_fraction = (has_target_count / example_count) if example_count else 0.0
    smiles_coverage_fraction = (has_smiles_count / example_count) if example_count else 0.0
    split_status = str(split_diagnostics.get("status") or "")
    split_counts_meta = split_diagnostics.get("counts") or {}
    readiness = "empty"
    if example_count == 0:
        readiness = "empty"
    elif supervised_count == 0:
        readiness = "weak_supervision"
    elif (
        example_count < _MIN_EXAMPLES_FOR_USABLE_CORPUS
        or len(unique_targets) < _MIN_UNIQUE_TARGETS_FOR_USABLE_CORPUS
        or len(unique_ligands) < _MIN_UNIQUE_LIGANDS_FOR_USABLE_CORPUS
    ):
        readiness = "undersized"
    elif split_status == "leakage_risk":
        readiness = "split_leakage_risk"
    elif split_status in {"dominance_risk", "attention_needed"}:
        readiness = "usable_with_gaps"
    elif (
        example_count
        >= _MIN_EXAMPLES_FOR_STRONG_CORPUS
        and len(unique_targets) >= _MIN_UNIQUE_TARGETS_FOR_STRONG_CORPUS
        and len(unique_ligands) >= _MIN_UNIQUE_LIGANDS_FOR_STRONG_CORPUS
        and supervised_fraction >= 0.8
        and conflict_fraction <= 0.1
        and degraded_fraction <= 0.1
        and target_coverage_fraction >= 0.95
        and smiles_coverage_fraction >= 0.95
    ):
        readiness = "strong"
    elif example_count and supervised_count:
        readiness = "usable_with_gaps"

    summary = (
        f"{example_count:,} examples, {supervised_count:,} supervised, "
        f"{len(unique_targets):,} targets, {len(unique_ligands):,} ligands, "
        f"{len(unique_pairs):,} unique pairs"
    )
    quality = (
        f"{supervised_fraction:.1%} supervised; {graph_fraction:.1%} with graph linkage; "
        f"{conflict_fraction:.1%} conflicted; {degraded_fraction:.1%} degraded; "
        f"split={split_status or 'unknown'}"
    )
    if example_count == 0:
        next_action = "Build training examples, then splits, before training models."
    elif supervised_count == 0:
        next_action = "Inspect assay extraction and improve standardized affinity coverage before training any supervised model."
    elif example_count < _MIN_EXAMPLES_FOR_USABLE_CORPUS:
        next_action = (
            "Expand assay enrichment before training models; the current corpus is too small "
            "to benchmark credibly."
        )
    elif len(unique_targets) < _MIN_UNIQUE_TARGETS_FOR_USABLE_CORPUS or len(unique_ligands) < _MIN_UNIQUE_LIGANDS_FOR_USABLE_CORPUS:
        next_action = (
            "Broaden target and ligand coverage before trusting model comparisons; the current "
            "corpus is too narrow."
        )
    elif split_status == "leakage_risk":
        next_action = "Regenerate splits with stronger grouping before trusting held-out metrics or model comparisons."
    elif split_status in {"dominance_risk", "attention_needed"}:
        next_action = "Inspect split diagnostics and reduce family/domain/pathway dominance before claiming robust generalization."
    elif supervision_blockers["missing_affinity_log10"] > 0:
        next_action = "Improve standardized affinity coverage so more examples become usable supervised rows."
    elif split_counts["train"] == 0:
        next_action = "Build splits so the training set can be benchmarked and models can be trained."
    elif split_counts["val"] == 0 and split_counts["test"] == 0:
        next_action = "Create held-out validation/test splits before trusting training metrics."
    elif source_conflict_count > 0:
        next_action = "Review conflicted examples and source-agreement bands before relying on model outputs."
    elif degraded_count > 0:
        next_action = "Reduce degraded-mode examples or track them separately before treating the corpus as stable."
    else:
        next_action = "Train and compare baseline versus tabular models on the current splits."

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "status": readiness,
        "summary": summary,
        "quality": quality,
        "next_action": next_action,
        "counts": {
            "example_count": example_count,
            "supervised_count": supervised_count,
            "unique_target_count": len(unique_targets),
            "unique_ligand_count": len(unique_ligands),
            "unique_pair_count": len(unique_pairs),
            "unique_metadata_family_count": len(unique_interpro_families),
            "unique_pathway_count": len(unique_pathways),
            "source_conflict_count": source_conflict_count,
            "graph_linked_count": has_graph_count,
            "smiles_present_count": has_smiles_count,
            "target_present_count": has_target_count,
            "mutation_count": mutation_count,
            "degraded_count": degraded_count,
        },
        "fractions": {
            "supervised_fraction": round(supervised_fraction, 6),
            "graph_linked_fraction": round(graph_fraction, 6),
            "source_conflict_fraction": round(conflict_fraction, 6),
            "mutation_fraction": round(mutation_fraction, 6),
            "degraded_fraction": round(degraded_fraction, 6),
            "target_coverage_fraction": round(target_coverage_fraction, 6),
            "smiles_coverage_fraction": round(smiles_coverage_fraction, 6),
        },
        "split_counts": split_counts,
        "split_assignment_coverage": {
            "assigned_example_count": len(split_assignment),
            "assigned_fraction": round((len(split_assignment) / example_count), 6) if example_count else 0.0,
        },
        "overlap_with_train": overlap,
        "missing_field_counts": missing_field_counts,
        "supervision_blockers": supervision_blockers,
        "affinity_type_counts": affinity_type_counts,
        "source_database_counts": source_database_counts,
        "source_agreement_band_counts": source_agreement_counts,
        "split_diagnostics": {
            "status": split_status or "missing",
            "hard_group_overlap_count": int(split_counts_meta.get("hard_group_overlap_count") or 0),
            "family_overlap_count": int(split_counts_meta.get("family_overlap_count") or 0),
            "domain_overlap_count": int(split_counts_meta.get("domain_overlap_count") or 0),
            "pathway_overlap_count": int(split_counts_meta.get("pathway_overlap_count") or 0),
            "fold_overlap_count": int(split_counts_meta.get("fold_overlap_count") or 0),
        },
    }


def export_training_set_quality_report(layout: StorageLayout) -> tuple[Path, Path, dict[str, Any]]:
    report = build_training_set_quality_report(layout)
    layout.reports_dir.mkdir(parents=True, exist_ok=True)
    json_path = layout.reports_dir / "training_set_quality.json"
    md_path = layout.reports_dir / "training_set_quality.md"
    json_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    lines = [
        "# Training Set Quality",
        "",
        f"- Status: {report['status']}",
        f"- Summary: {report['summary']}",
        f"- Quality: {report['quality']}",
        f"- Next action: {report['next_action']}",
        f"- Split assignment coverage: {report['split_assignment_coverage']['assigned_example_count']}/{report['counts']['example_count']}",
        "",
        "## Split counts",
    ]
    for split_name, count in (report.get("split_counts") or {}).items():
        lines.append(f"- {split_name}: {count}")
    lines.extend([
        "",
        "## Missingness and supervision blockers",
    ])
    for key, count in (report.get("missing_field_counts") or {}).items():
        lines.append(f"- missing {key}: {count}")
    for key, count in (report.get("supervision_blockers") or {}).items():
        lines.append(f"- blocker {key}: {count}")
    lines.extend([
        "",
        "## Overlap with train",
    ])
    overlap = report.get("overlap_with_train") or {}
    for split_name in ("val", "test"):
        payload = overlap.get(split_name) or {}
        lines.append(
            f"- {split_name}: target overlap={payload.get('same_target_in_train', 0)}, "
            f"ligand overlap={payload.get('same_ligand_in_train', 0)}, "
            f"exact pair overlap={payload.get('exact_pair_seen_in_train', 0)}, "
            f"novel={payload.get('novel_examples', 0)}"
        )
    lines.extend([
        "",
        "## Split diagnostics",
    ])
    split_info = report.get("split_diagnostics") or {}
    for key, value in split_info.items():
        lines.append(f"- {key}: {value}")
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return json_path, md_path, report
