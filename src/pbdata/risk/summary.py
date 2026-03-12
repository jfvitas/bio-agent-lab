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


def _protein_pathway_map(layout: StorageLayout) -> tuple[dict[str, set[str]], dict[str, set[str]], bool]:
    nodes = _read_json(layout.graph_dir / "graph_nodes.json")
    edges = _read_json(layout.graph_dir / "graph_edges.json")
    if not isinstance(nodes, list) or not isinstance(edges, list):
        return {}, {}, False

    pathway_node_ids = {
        str(row.get("node_id") or "")
        for row in nodes
        if isinstance(row, dict) and str(row.get("node_type") or "") == "Pathway"
    }
    placeholder_pathways = {
        str(row.get("node_id") or "")
        for row in nodes
        if isinstance(row, dict)
        and str(row.get("node_type") or "") == "Pathway"
        and bool((row.get("metadata") or {}).get("placeholder"))
    }
    protein_to_pathways: dict[str, set[str]] = {}
    pathway_sources: dict[str, set[str]] = {}
    for edge in edges:
        if not isinstance(edge, dict) or str(edge.get("edge_type") or "") != "ProteinPathway":
            continue
        src = str(edge.get("source_node_id") or "")
        tgt = str(edge.get("target_node_id") or "")
        protein_node = ""
        pathway_node = ""
        if tgt in pathway_node_ids and src.startswith("protein:"):
            protein_node = src
            pathway_node = tgt
        elif src in pathway_node_ids and tgt.startswith("protein:"):
            protein_node = tgt
            pathway_node = src
        if not protein_node or not pathway_node or pathway_node in placeholder_pathways:
            continue
        protein_id = protein_node.split("protein:", 1)[-1]
        protein_to_pathways.setdefault(protein_id, set()).add(pathway_node)
        pathway_sources.setdefault(protein_id, set()).add(str(edge.get("source_database") or ""))
    return protein_to_pathways, pathway_sources, bool(protein_to_pathways)


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
    protein_to_pathways, pathway_sources, graph_pathway_ready = _protein_pathway_map(layout)

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
    pair_target_set: set[str] = set()
    for row in matching_rows:
        pair_target_set.update(
            value.strip()
            for value in str(row.get("receptor_uniprot_ids") or "").split(";")
            if value.strip()
        )
    requested_pathways = set().union(*(protein_to_pathways.get(target_id, set()) for target_id in target_set)) if target_set else set()
    matched_pair_pathways = set().union(*(protein_to_pathways.get(target_id, set()) for target_id in pair_target_set)) if pair_target_set else set()
    overlap_count = len(requested_pathways.intersection(matched_pair_pathways))
    union_count = len(requested_pathways.union(matched_pair_pathways))
    pathway_similarity = (overlap_count / union_count) if union_count else (1.0 if coverage and matching_rows else 0.0)
    conflict_fraction = (source_conflicts / len(matching_rows)) if matching_rows else 0.0
    prediction_rows = ligand_prediction.get("ranked_target_list") if isinstance(ligand_prediction, dict) else []
    prediction_confidences = [
        float(row.get("confidence_score") or 0.0)
        for row in (prediction_rows or [])
        if isinstance(row, dict) and (not target_set or str(row.get("target_id") or "") in target_set)
    ]
    prediction_support = max(prediction_confidences) if prediction_confidences else 0.0
    binding_strength = max(0.0, min(1.0, 1.0 - (predicted_affinity / 6.0))) if matching_rows else 0.0
    binding_weight = 0.45
    pathway_overlap_weight = 0.35
    prediction_support_weight = 0.20
    conflict_penalty_weight = 0.25
    risk_score = (
        (binding_weight * binding_strength)
        + (pathway_overlap_weight * pathway_similarity)
        + (prediction_support_weight * prediction_support)
        - (conflict_penalty_weight * conflict_fraction)
    )
    risk_score = max(risk_score, 0.0)
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
        "status": "graph_context_summary_not_clinical_risk_model",
        "targets_requested": sorted(target_set),
        "matching_pair_count": len(matching_rows),
        "assay_types_present": assay_types,
        "source_conflict_pair_count": source_conflicts,
        "pathway_context_available": bool(coverage) or graph_pathway_ready,
        "binding_weight": binding_weight,
        "pathway_overlap_weight": pathway_overlap_weight,
        "prediction_support_weight": prediction_support_weight,
        "conflict_penalty_weight": conflict_penalty_weight,
        "predicted_affinity_proxy": predicted_affinity,
        "binding_strength_proxy": round(binding_strength, 4),
        "prediction_context_available": bool(predicted_targets),
        "prediction_method": (
            str(ligand_prediction.get("prediction_method") or "")
            if isinstance(ligand_prediction, dict)
            else None
        ),
        "predicted_target_matches": predicted_targets,
        "pathway_overlap_count": overlap_count,
        "pathway_union_count": union_count,
        "pathway_source_databases": sorted(set().union(*(pathway_sources.get(target_id, set()) for target_id in target_set))) if target_set else [],
        "pathway_similarity_proxy": round(pathway_similarity, 4),
        "pathway_similarity_method": (
            "target_pair_pathway_jaccard"
            if union_count
            else ("graph_pathways_unavailable_fallback" if not graph_pathway_ready else "no_pathway_overlap")
        ),
        "risk_score_is_placeholder": False,
        "risk_score": round(risk_score, 4),
        "pathway_activation_probability": round(max(0.0, min(1.0, (0.55 * pathway_similarity) + (0.45 * prediction_support))), 4),
        "pathway_conflict_score": round(conflict_fraction, 4),
        "severity_level": "high" if risk_score >= 0.7 else "medium" if risk_score >= 0.35 else "low",
        "notes": (
            "This is a graph-and-prediction context summary built from matched assay rows, "
            "ProteinPathway edges, prediction confidence, and source-conflict penalties. "
            "It is more informative than the old binary coverage proxy, but it is still not "
            "a trained or clinically calibrated risk model."
        ),
    }
    out_dir = layout.risk_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "pathway_risk_summary.json"
    out_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    return out_path, summary
