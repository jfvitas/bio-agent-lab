from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pbdata.storage import StorageLayout


def _read_json(path: Path) -> Any:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def _read_split_ids(path: Path) -> set[str]:
    if not path.exists():
        return set()
    return {line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip()}


def _safe_dict(value: object) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def build_training_set_quality_report(layout: StorageLayout) -> dict[str, Any]:
    raw = _read_json(layout.training_dir / "training_examples.json")
    examples = [row for row in raw if isinstance(row, dict)] if isinstance(raw, list) else []
    train_ids = _read_split_ids(layout.splits_dir / "train.txt")
    val_ids = _read_split_ids(layout.splits_dir / "val.txt")
    test_ids = _read_split_ids(layout.splits_dir / "test.txt")

    example_count = len(examples)
    supervised_count = 0
    source_conflict_count = 0
    has_graph_count = 0
    has_smiles_count = 0
    has_target_count = 0
    unique_targets: set[str] = set()
    unique_ligands: set[str] = set()
    affinity_type_counts: dict[str, int] = {}
    source_database_counts: dict[str, int] = {}
    source_agreement_counts: dict[str, int] = {}
    split_counts = {"train": 0, "val": 0, "test": 0, "unsplit": 0}
    train_targets: set[str] = set()
    train_ligands: set[str] = set()
    train_pairs: set[str] = set()

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
        affinity_type_counts[affinity_type] = affinity_type_counts.get(affinity_type, 0) + 1
        source_database_counts[source_database] = source_database_counts.get(source_database, 0) + 1
        source_agreement_counts[source_agreement] = source_agreement_counts.get(source_agreement, 0) + 1

        split_name = "unsplit"
        if example_id in train_ids:
            split_name = "train"
        elif example_id in val_ids:
            split_name = "val"
        elif example_id in test_ids:
            split_name = "test"
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
    readiness = "empty"
    if example_count and supervised_fraction >= 0.8 and conflict_fraction <= 0.1:
        readiness = "strong"
    elif example_count and supervised_count:
        readiness = "usable_with_gaps"
    elif example_count:
        readiness = "weak_supervision"

    summary = (
        f"{example_count:,} examples, {supervised_count:,} supervised, "
        f"{len(unique_targets):,} targets, {len(unique_ligands):,} ligands"
    )
    quality = (
        f"{supervised_fraction:.1%} supervised; {graph_fraction:.1%} with graph linkage; "
        f"{conflict_fraction:.1%} conflicted"
    )
    if example_count == 0:
        next_action = "Build training examples, then splits, before training models."
    elif supervised_count == 0:
        next_action = "Inspect assay extraction and label coverage before training any supervised model."
    elif split_counts["train"] == 0:
        next_action = "Build splits so the training set can be benchmarked and models can be trained."
    elif split_counts["val"] == 0 and split_counts["test"] == 0:
        next_action = "Create held-out validation/test splits before trusting training metrics."
    elif source_conflict_count > 0:
        next_action = "Review conflicted examples and source-agreement bands before relying on model outputs."
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
            "source_conflict_count": source_conflict_count,
            "graph_linked_count": has_graph_count,
            "smiles_present_count": has_smiles_count,
            "target_present_count": has_target_count,
        },
        "fractions": {
            "supervised_fraction": round(supervised_fraction, 6),
            "graph_linked_fraction": round(graph_fraction, 6),
            "source_conflict_fraction": round(conflict_fraction, 6),
        },
        "split_counts": split_counts,
        "overlap_with_train": overlap,
        "affinity_type_counts": affinity_type_counts,
        "source_database_counts": source_database_counts,
        "source_agreement_band_counts": source_agreement_counts,
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
        "",
        "## Split counts",
    ]
    for split_name, count in (report.get("split_counts") or {}).items():
        lines.append(f"- {split_name}: {count}")
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
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return json_path, md_path, report
