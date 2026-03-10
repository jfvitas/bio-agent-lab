"""Pathway/risk workflow manifests with explicit uncertainty."""

from __future__ import annotations

import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pbdata.storage import StorageLayout


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _read_json(path: Path) -> Any:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def build_pathway_risk_summary(
    layout: StorageLayout,
    *,
    targets: list[str] | None = None,
) -> tuple[Path, dict[str, Any]]:
    pair_rows = _read_csv(layout.root / "model_ready_pairs.csv")
    ligand_prediction = _read_json(layout.prediction_dir / "ligand_screening" / "prediction_manifest.json")
    coverage_path = layout.root / "scientific_coverage_summary.json"
    coverage = json.loads(coverage_path.read_text(encoding="utf-8")) if coverage_path.exists() else {}
    target_set = {target.strip() for target in (targets or []) if target.strip()}

    matching_rows = []
    for row in pair_rows:
        receptors = {
            value.strip()
            for value in str(row.get("receptor_uniprot_ids") or "").split(";")
            if value.strip()
        }
        if not target_set or receptors.intersection(target_set):
            matching_rows.append(row)

    source_conflicts = sum(1 for row in matching_rows if str(row.get("source_conflict_flag") or "").lower() == "true")
    assay_types = sorted({
        str(row.get("binding_affinity_type") or "")
        for row in matching_rows
        if row.get("binding_affinity_type")
    })
    predicted_affinity = 0.0
    if matching_rows:
        values = []
        for row in matching_rows:
            try:
                values.append(float(str(row.get("reported_measurement_mean_log10_standardized") or row.get("binding_affinity_log10_standardized") or "").strip()))
            except ValueError:
                continue
        if values:
            predicted_affinity = sum(values) / len(values)
    pathway_similarity = 1.0 if coverage else 0.0
    # Placeholder scaffold weights until real pathway-overlap and calibrated risk
    # models are implemented.
    binding_weight = 0.7
    pathway_overlap_weight = 0.3
    risk_score = (binding_weight * predicted_affinity) + (pathway_overlap_weight * pathway_similarity)
    predicted_targets = []
    if isinstance(ligand_prediction, dict):
        for row in ligand_prediction.get("ranked_target_list") or []:
            if not isinstance(row, dict):
                continue
            target_id = str(row.get("target_id") or "")
            if target_id and target_id in target_set:
                predicted_targets.append({
                    "target_id": target_id,
                    "rank": row.get("rank"),
                    "confidence_score": row.get("confidence_score"),
                    "predicted_kd_nM": row.get("predicted_kd_nM"),
                })
    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "status": "dataset_context_summary_not_model_score",
        "targets_requested": sorted(target_set),
        "matching_pair_count": len(matching_rows),
        "assay_types_present": assay_types,
        "source_conflict_pair_count": source_conflicts,
        "pathway_context_available": bool(coverage),
        "binding_weight": binding_weight,
        "pathway_overlap_weight": pathway_overlap_weight,
        "predicted_affinity_proxy": predicted_affinity,
        "prediction_context_available": bool(predicted_targets),
        "prediction_method": (
            str(ligand_prediction.get("prediction_method") or "")
            if isinstance(ligand_prediction, dict)
            else None
        ),
        "predicted_target_matches": predicted_targets,
        "pathway_similarity_proxy": pathway_similarity,
        "pathway_similarity_method": "binary_coverage_proxy",
        "risk_score_is_placeholder": True,
        "risk_score": risk_score,
        "pathway_activation_probability": None,
        "pathway_conflict_score": None,
        "severity_level": "high" if risk_score >= 2.0 else "medium" if risk_score >= 0.75 else "low",
        "notes": (
            "This is a pathway/risk readiness summary from current dataset and graph coverage. "
            "The score uses placeholder weights and a binary pathway-coverage proxy, not a "
            "real pathway-overlap or trained risk model."
        ),
    }
    out_dir = layout.risk_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "pathway_risk_summary.json"
    out_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    return out_path, summary
