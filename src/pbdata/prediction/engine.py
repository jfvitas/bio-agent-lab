"""Prediction-workflow manifests with explicit uncertainty."""

from __future__ import annotations

import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pbdata.features.microstate import summarize_structure_microstates
from pbdata.features.physics_features import summarize_microstates_to_physics_features
from pbdata.models.affinity_models import (
    affinity_strength_from_log10,
    confidence_bonus,
    ligand_similarity_score,
    logistic_confidence,
)
from pbdata.models.baseline_memory import (
    load_ligand_memory_model,
    predict_with_ligand_memory_model,
    score_query_context_against_target_profile,
)
from pbdata.schemas.prediction_input import PredictionInputRecord
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


def _safe_float(value: object) -> float | None:
    try:
        text = str(value or "").strip()
        if not text:
            return None
        return float(text)
    except ValueError:
        return None


def _semicolon_values(raw: str | None) -> list[str]:
    return [item.strip() for item in str(raw or "").replace(",", ";").split(";") if item.strip()]


def _load_bound_object_rows(layout: StorageLayout, pdb_id: str) -> list[dict[str, Any]]:
    path = layout.extracted_dir / "bound_objects" / f"{pdb_id}.json"
    rows = _read_json(path)
    return rows if isinstance(rows, list) else []


def _match_bound_object_for_pair(layout: StorageLayout, row: dict[str, str]) -> dict[str, Any]:
    pdb_id = str(row.get("pdb_id") or "")
    ligand_key = str(row.get("ligand_key") or "")
    for bound in _load_bound_object_rows(layout, pdb_id):
        ids = {
            str(bound.get("component_id") or ""),
            str(bound.get("component_name") or ""),
            str(bound.get("component_inchikey") or ""),
        }
        if ligand_key and ligand_key in ids:
            return bound
    return {}


def _rank_target_rows(layout: StorageLayout, query_smiles: str, pair_rows: list[dict[str, str]]) -> list[dict[str, Any]]:
    ranked: list[dict[str, Any]] = []
    for row in pair_rows:
        bound = _match_bound_object_for_pair(layout, row)
        ref_smiles = str(bound.get("component_smiles") or "")
        similarity = ligand_similarity_score(query_smiles, ref_smiles)
        affinity_log10 = _safe_float(
            row.get("reported_measurement_mean_log10_standardized") or row.get("binding_affinity_log10_standardized")
        )
        affinity_strength = affinity_strength_from_log10(affinity_log10)
        agreement_band = str(row.get("source_agreement_band") or "")
        conflict_flag = str(row.get("source_conflict_flag") or "").strip().lower() == "true"
        quality_score = _safe_float(row.get("quality_score")) or 0.0
        raw_score = (
            (0.55 * similarity)
            + (0.25 * affinity_strength)
            + (0.10 * min(max(quality_score, 0.0), 1.0))
            + confidence_bonus(agreement_band, conflict_flag)
        )
        confidence = logistic_confidence(raw_score)
        target_ids = _semicolon_values(row.get("receptor_uniprot_ids"))
        if not target_ids:
            continue
        ranked.append({
            "pair_identity_key": str(row.get("pair_identity_key") or ""),
            "pdb_id": str(row.get("pdb_id") or ""),
            "target_ids": target_ids,
            "ligand_similarity": round(similarity, 4),
            "affinity_strength": round(affinity_strength, 4),
            "predicted_kd_nM": None if affinity_log10 is None else round(10 ** affinity_log10, 3),
            "predicted_delta_g_proxy": None if affinity_log10 is None else round(-1.364 * affinity_log10, 3),
            "confidence_score": round(confidence, 4),
            "source_database": str(row.get("selected_preferred_source") or row.get("source_database") or ""),
            "source_agreement_band": agreement_band or None,
            "source_conflict_flag": conflict_flag,
            "raw_score": raw_score,
            "ranking_basis": "ligand_similarity_plus_affinity_proxy",
        })
    ranked.sort(key=lambda item: (item["raw_score"], item["confidence_score"]), reverse=True)
    return ranked


def _aggregate_targets(ranked_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_target: dict[str, dict[str, Any]] = {}
    for row in ranked_rows:
        for target_id in row["target_ids"]:
            current = by_target.get(target_id)
            if current is None or row["raw_score"] > current["raw_score"]:
                by_target[target_id] = {
                    "target_id": target_id,
                    "supporting_pair_identity_key": row["pair_identity_key"],
                    "supporting_pdb_id": row["pdb_id"],
                    "predicted_kd_nM": row["predicted_kd_nM"],
                    "predicted_delta_g_proxy": row["predicted_delta_g_proxy"],
                    "confidence_score": row["confidence_score"],
                    "ligand_similarity": row["ligand_similarity"],
                    "source_database": row["source_database"],
                    "source_agreement_band": row["source_agreement_band"],
                    "source_conflict_flag": row["source_conflict_flag"],
                    "ranking_basis": row["ranking_basis"],
                    "raw_score": row["raw_score"],
                }
    ranked_targets = sorted(by_target.values(), key=lambda item: item["raw_score"], reverse=True)
    for index, item in enumerate(ranked_targets, start=1):
        item["rank"] = index
        item.pop("raw_score", None)
    return ranked_targets


def _pair_rows_for_prediction(layout: StorageLayout) -> list[dict[str, str]]:
    return _read_csv(layout.root / "master_pdb_pairs.csv") or _read_csv(layout.root / "model_ready_pairs.csv")


def _interface_summary_for_structure(layout: StorageLayout, structure_file: str) -> dict[str, Any]:
    pdb_id = Path(structure_file).stem.upper()
    interface_path = layout.extracted_dir / "interfaces" / f"{pdb_id}.json"
    interfaces = _read_json(interface_path)
    if not isinstance(interfaces, list) or not interfaces:
        return {
            "status": "no_interface_annotations_available",
            "feature_context_available": (layout.features_dir / "feature_records.json").exists(),
            "graph_context_available": (layout.graph_dir / "graph_nodes.json").exists() and (layout.graph_dir / "graph_edges.json").exists(),
            "observed_interface_count": 0,
            "observed_interface_types": [],
            "predicted_interface_residues": [],
        }

    residue_ids: list[str] = []
    interface_types: set[str] = set()
    for interface in interfaces:
        if not isinstance(interface, dict):
            continue
        interface_type = str(interface.get("interface_type") or "").strip()
        if interface_type:
            interface_types.add(interface_type)
        for residue_id in interface.get("binding_site_residue_ids") or []:
            rid = str(residue_id).strip()
            if rid and rid not in residue_ids:
                residue_ids.append(rid)

    return {
        "status": "observed_from_extracted_interfaces" if residue_ids or interface_types else "interface_annotations_present_but_sparse",
        "feature_context_available": (layout.features_dir / "feature_records.json").exists(),
        "graph_context_available": (layout.graph_dir / "graph_nodes.json").exists() and (layout.graph_dir / "graph_edges.json").exists(),
        "observed_interface_count": len(interfaces),
        "observed_interface_types": sorted(interface_types),
        "predicted_interface_residues": residue_ids[:50],
    }


def _query_numeric_features_from_structure(
    structure_file: str,
    *,
    interface_summary: dict[str, Any] | None = None,
) -> dict[str, float]:
    """Derive inference-time query features using the same microstate/physics semantics."""
    summary = summarize_structure_microstates(Path(structure_file))
    microstates = (summary or {}).get("microstates") or []
    physics = summarize_microstates_to_physics_features(microstates) or {}
    features: dict[str, float] = {}
    for key, value in physics.items():
        numeric = _safe_float(value)
        if numeric is not None:
            features[f"interaction.{key}"] = numeric
    if interface_summary:
        residue_count = len(interface_summary.get("predicted_interface_residues") or [])
        observed_count = _safe_float(interface_summary.get("observed_interface_count"))
        if residue_count:
            features["interaction.interface_residue_count"] = float(residue_count)
        elif observed_count is not None:
            features["interaction.interface_residue_count"] = observed_count
    return features


def _detect_input_type(
    *,
    smiles: str | None = None,
    sdf: str | None = None,
    structure_file: str | None = None,
    fasta: str | None = None,
) -> tuple[str, dict[str, Any]]:
    if smiles:
        PredictionInputRecord(input_type="smiles", input_value=smiles)
        return "SMILES", {"smiles": smiles}
    if sdf:
        PredictionInputRecord(input_type="sdf", input_value=sdf)
        return "SDF", {"sdf_path": sdf}
    if structure_file:
        suffix = Path(structure_file).suffix.lower()
        input_type = "mmcif" if suffix in {".cif", ".mmcif"} else "pdb"
        PredictionInputRecord(input_type=input_type, input_value=structure_file)
        return ("mmCIF" if input_type == "mmcif" else "PDB"), {"structure_file": structure_file}
    if fasta:
        PredictionInputRecord(input_type="fasta", input_value=fasta)
        return "FASTA", {"fasta": fasta, "structure_generation_strategy": "AlphaFold_planned"}
    raise ValueError("One input is required: smiles, sdf, structure_file, or fasta")


def run_ligand_screening_workflow(
    layout: StorageLayout,
    *,
    smiles: str | None = None,
    sdf: str | None = None,
    structure_file: str | None = None,
    fasta: str | None = None,
) -> tuple[Path, dict[str, Any]]:
    """Write a structured manifest for baseline ligand/off-target screening."""
    input_type, normalized = _detect_input_type(
        smiles=smiles,
        sdf=sdf,
        structure_file=structure_file,
        fasta=fasta,
    )
    pair_rows = _pair_rows_for_prediction(layout)
    query_smiles = str(smiles or "")
    model_info = None
    ranked_targets: list[dict[str, Any]] = []
    if query_smiles:
        model_info, ranked_targets = predict_with_ligand_memory_model(layout, query_smiles=query_smiles)
    if not ranked_targets:
        ranked_rows = _rank_target_rows(layout, query_smiles, pair_rows) if query_smiles else []
        ranked_targets = _aggregate_targets(ranked_rows)
    graph_ready = (layout.graph_dir / "graph_nodes.json").exists() and (layout.graph_dir / "graph_edges.json").exists()
    coverage_ready = (layout.root / "scientific_coverage_summary.json").exists()
    top_target = ranked_targets[0] if ranked_targets else None
    using_trained_model = model_info is not None and bool(ranked_targets)

    manifest = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "status": (
            "trained_baseline_predictions_generated"
            if using_trained_model
            else "baseline_heuristic_predictions_generated"
        ) if ranked_targets else "no_candidate_predictions_available",
        "workflow": "ligand_screening",
        "normalized_input_type": input_type,
        "normalized_input": normalized,
        "candidate_target_count": len(ranked_targets),
        "ranked_target_list": ranked_targets[:25],
        "candidate_targets_preview": [row["target_id"] for row in ranked_targets[:25]],
        "graph_context_available": graph_ready,
        "pathway_context_available": graph_ready and coverage_ready,
        "predicted_kd": top_target["predicted_kd_nM"] if top_target else None,
        "predicted_delta_g": top_target["predicted_delta_g_proxy"] if top_target else None,
        "binding_probability": top_target["ligand_similarity"] if top_target else None,
        "confidence_score": top_target["confidence_score"] if top_target else None,
        "prediction_method": (
            "trained_ligand_memory_model"
            if using_trained_model
            else "baseline_heuristic_similarity_affinity_proxy"
        ),
        "model_artifact_path": (
            str(layout.models_dir / "ligand_memory_model.json")
            if using_trained_model
            else None
        ),
        "notes": (
            "Predictions use a split-aware ligand-memory baseline when a trained model artifact "
            "is available; otherwise they fall back to a heuristic over ligand-string similarity, "
            "observed assay strength, and source-quality penalties."
        ),
    }

    out_dir = layout.prediction_dir / "ligand_screening"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "prediction_manifest.json"
    out_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return out_path, manifest


def run_peptide_binding_workflow(
    layout: StorageLayout,
    *,
    structure_file: str,
) -> tuple[Path, dict[str, Any]]:
    """Write a structured manifest for baseline peptide-binding prediction."""
    if not structure_file:
        raise ValueError("structure_file is required")
    suffix = Path(structure_file).suffix.lower()
    PredictionInputRecord(
        input_type="mmcif" if suffix in {".cif", ".mmcif"} else "pdb",
        input_value=structure_file,
    )
    input_type = "mmCIF" if suffix in {".cif", ".mmcif"} else "PDB"
    interface_summary = _interface_summary_for_structure(layout, structure_file)
    query_numeric_features = _query_numeric_features_from_structure(
        structure_file,
        interface_summary=interface_summary,
    )
    pdb_id = Path(structure_file).stem.upper()
    pair_rows = [
        row for row in _pair_rows_for_prediction(layout)
        if str(row.get("pdb_id") or "").upper() == pdb_id
    ]
    predicted_targets: list[dict[str, Any]] = []
    seen_targets: set[str] = set()
    model = load_ligand_memory_model(layout)
    target_profiles = model.get("target_profiles") if isinstance(model, dict) and isinstance(model.get("target_profiles"), dict) else {}
    for row in pair_rows:
        for target_id in _semicolon_values(row.get("receptor_uniprot_ids")):
            if target_id in seen_targets:
                continue
            seen_targets.add(target_id)
            context_alignment = score_query_context_against_target_profile(
                query_numeric_features,
                target_profiles.get(target_id) if isinstance(target_profiles, dict) else None,
            )
            predicted_targets.append({
                "target_id": target_id,
                "supporting_pair_identity_key": str(row.get("pair_identity_key") or ""),
                "source_database": str(row.get("selected_preferred_source") or row.get("source_database") or ""),
                "matching_interface_count": int(float(str(row.get("matching_interface_count") or "0"))),
                "query_context_alignment": context_alignment,
            })
    predicted_targets.sort(
        key=lambda item: (
            float(item.get("query_context_alignment") or 0.0),
            int(item.get("matching_interface_count") or 0),
        ),
        reverse=True,
    )
    for index, item in enumerate(predicted_targets, start=1):
        item["rank"] = index
    observed_count = int(interface_summary.get("observed_interface_count") or 0)
    top_alignment = float(predicted_targets[0].get("query_context_alignment") or 0.0) if predicted_targets else 0.0
    binding_probability = (
        min(1.0, 0.22 + (0.12 * observed_count) + (0.30 * top_alignment))
        if predicted_targets else 0.0
    )

    manifest = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "status": "baseline_heuristic_predictions_generated" if predicted_targets else "no_candidate_predictions_available",
        "workflow": "peptide_binding",
        "normalized_input_type": input_type,
        "normalized_input": {"structure_file": structure_file},
        "predicted_targets": predicted_targets[:25],
        "binding_probability": round(binding_probability, 4) if predicted_targets else None,
        "interface_summary": interface_summary,
        "query_numeric_feature_count": len(query_numeric_features),
        "query_structure_context": {
            "microstate_feature_available": bool(query_numeric_features),
            "query_numeric_features": query_numeric_features,
        },
        "prediction_method": (
            "baseline_interface_context_lookup_with_microstate_alignment"
            if query_numeric_features
            else "baseline_interface_context_lookup"
        ),
        "notes": (
            "Predictions use extracted interface annotations and existing pair records "
            "for the same structure, plus query-side microstate/physics-style features "
            "when those can be derived from the provided structure."
        ),
    }
    out_dir = layout.prediction_dir / "peptide_binding"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "prediction_manifest.json"
    out_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return out_path, manifest
