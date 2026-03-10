"""Dependency-free ligand-memory baseline model.

Assumptions:
- This is a memory-based baseline over existing training examples, not a
  mechanistic or quantum-chemical model.
- It is appropriate as a conservative learned substrate because it respects the
  current train/val/test split files and stays auditable.
- Generalization is limited by the chemical and biological neighborhoods already
  represented in the training examples.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pbdata.models.affinity_models import (
    affinity_strength_from_log10,
    confidence_bonus,
    ligand_similarity_score,
    logistic_confidence,
)
from pbdata.storage import StorageLayout

_MODEL_FILENAME = "ligand_memory_model.json"
_EVAL_FILENAME = "ligand_memory_evaluation.json"
_NUMERIC_FEATURE_KEYS: tuple[tuple[str, str], ...] = (
    ("graph_features", "network_degree"),
    ("graph_features", "ppi_degree"),
    ("graph_features", "pli_degree"),
    ("graph_features", "pathway_count"),
    ("interaction", "interface_residue_count"),
    ("interaction", "microstate_record_count"),
    ("interaction", "estimated_net_charge"),
    ("interaction", "mean_abs_residue_charge"),
    ("interaction", "positive_residue_count"),
    ("interaction", "negative_residue_count"),
    ("interaction", "same_charge_contact_count"),
    ("interaction", "opposite_charge_contact_count"),
    ("interaction", "metal_contact_count"),
    ("interaction", "acidic_cluster_penalty"),
    ("interaction", "local_electrostatic_balance"),
    ("structure", "resolution"),
    ("protein", "sequence_length"),
    ("protein", "mean_hydropathy"),
    ("protein", "charged_fraction"),
    ("ligand", "molecular_weight"),
)


def _read_json(path: Path) -> Any:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def _read_split_ids(path: Path) -> set[str]:
    if not path.exists():
        return set()
    return {line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip()}


def _safe_float(value: object) -> float | None:
    try:
        text = str(value or "").strip()
        if not text:
            return None
        return float(text)
    except ValueError:
        return None


def _memory_weight(example: dict[str, Any]) -> float:
    experiment = example.get("experiment") or {}
    provenance = example.get("provenance") or {}
    labels = example.get("labels") or {}
    weight = 1.0
    reported_count = _safe_float(experiment.get("reported_measurement_count")) or 0.0
    weight += min(reported_count, 5.0) * 0.05
    weight += confidence_bonus(
        provenance.get("source_agreement_band"),
        bool(labels.get("source_conflict_flag") or provenance.get("source_conflict_flag")),
    )
    if bool(labels.get("is_mutant")):
        weight -= 0.05
    return max(weight, 0.1)


def _feature_key_name(section: str, field: str) -> str:
    return f"{section}.{field}"


def _extract_numeric_features(example: dict[str, Any]) -> dict[str, float]:
    features: dict[str, float] = {}
    for section_name, field_name in _NUMERIC_FEATURE_KEYS:
        section = example.get(section_name) or {}
        if not isinstance(section, dict):
            continue
        value = _safe_float(section.get(field_name))
        if value is not None:
            features[_feature_key_name(section_name, field_name)] = value
    return features


def _target_profiles(exemplars: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    profiles: dict[str, dict[str, Any]] = {}
    for exemplar in exemplars:
        target_id = str(exemplar.get("target_id") or "")
        if not target_id:
            continue
        profile = profiles.setdefault(target_id, {
            "feature_sums": {},
            "weight_total": 0.0,
            "exemplar_count": 0,
        })
        weight = _safe_float(exemplar.get("memory_weight")) or 1.0
        numeric_features = exemplar.get("numeric_features") or {}
        if not isinstance(numeric_features, dict):
            numeric_features = {}
        for key, raw_value in numeric_features.items():
            value = _safe_float(raw_value)
            if value is None:
                continue
            profile["feature_sums"][key] = float(profile["feature_sums"].get(key, 0.0)) + (weight * value)
        profile["weight_total"] = float(profile["weight_total"]) + weight
        profile["exemplar_count"] = int(profile["exemplar_count"]) + 1
    for target_id, profile in profiles.items():
        weight_total = float(profile["weight_total"]) or 1.0
        mean_features = {
            key: round(float(value) / weight_total, 6)
            for key, value in (profile.get("feature_sums") or {}).items()
        }
        profile["mean_features"] = mean_features
        # Conservative prior: denser graph/interface/electrostatic support raises score modestly.
        support_terms = [
            min(max(_safe_float(mean_features.get("graph_features.network_degree")) or 0.0, 0.0), 20.0) / 20.0,
            min(max(_safe_float(mean_features.get("graph_features.pathway_count")) or 0.0, 0.0), 10.0) / 10.0,
            min(max(_safe_float(mean_features.get("interaction.microstate_record_count")) or 0.0, 0.0), 20.0) / 20.0,
            min(max(_safe_float(mean_features.get("interaction.opposite_charge_contact_count")) or 0.0, 0.0), 20.0) / 20.0,
            1.0 - min(max(_safe_float(mean_features.get("interaction.acidic_cluster_penalty")) or 0.0, 0.0), 5.0) / 5.0,
        ]
        profile["target_prior_score"] = round(sum(support_terms) / len(support_terms), 4)
        profile["mean_features"] = mean_features
        profile.pop("feature_sums", None)
    return profiles


def score_query_context_against_target_profile(
    query_numeric_features: dict[str, float] | None,
    target_profile: dict[str, Any] | None,
) -> float:
    """Return a conservative 0..1 context-alignment score."""
    if not query_numeric_features or not isinstance(target_profile, dict):
        return 0.0
    mean_features = target_profile.get("mean_features") or {}
    if not isinstance(mean_features, dict):
        return 0.0
    shared: list[float] = []
    for key, query_value in query_numeric_features.items():
        profile_value = _safe_float(mean_features.get(key))
        if profile_value is None:
            continue
        q = _safe_float(query_value)
        if q is None:
            continue
        scale = max(abs(q), abs(profile_value), 1.0)
        shared.append(max(0.0, 1.0 - (abs(q - profile_value) / scale)))
    if not shared:
        return 0.0
    return round(sum(shared) / len(shared), 4)


def _example_to_exemplar(example: dict[str, Any]) -> dict[str, Any] | None:
    protein = example.get("protein") or {}
    ligand = example.get("ligand") or {}
    labels = example.get("labels") or {}
    provenance = example.get("provenance") or {}
    experiment = example.get("experiment") or {}
    structure = example.get("structure") or {}

    target_id = str(protein.get("uniprot_id") or "").strip()
    smiles = str(ligand.get("smiles") or "").strip()
    example_id = str(example.get("example_id") or "").strip()
    if not target_id or not smiles or not example_id:
        return None

    return {
        "example_id": example_id,
        "target_id": target_id,
        "pair_identity_key": str(provenance.get("pair_identity_key") or ""),
        "pdb_id": str(structure.get("pdb_id") or ""),
        "ligand_smiles": smiles,
        "ligand_id": str(ligand.get("ligand_id") or ""),
        "affinity_type": str((labels.get("affinity_type") or experiment.get("affinity_type") or "")).strip() or None,
        "affinity_log10": _safe_float(labels.get("binding_affinity_log10")),
        "source_database": str(
            provenance.get("preferred_source_database")
            or provenance.get("source_database")
            or experiment.get("preferred_source_database")
            or experiment.get("source_database")
            or ""
        ),
        "source_agreement_band": str(provenance.get("source_agreement_band") or ""),
        "source_conflict_flag": bool(labels.get("source_conflict_flag") or provenance.get("source_conflict_flag")),
        "memory_weight": round(_memory_weight(example), 4),
        "numeric_features": _extract_numeric_features(example),
    }


def _load_training_examples(layout: StorageLayout) -> list[dict[str, Any]]:
    raw = _read_json(layout.training_dir / "training_examples.json")
    return raw if isinstance(raw, list) else []


def _training_examples_by_id(layout: StorageLayout) -> dict[str, dict[str, Any]]:
    examples = _load_training_examples(layout)
    by_id: dict[str, dict[str, Any]] = {}
    for row in examples:
        if not isinstance(row, dict):
            continue
        example_id = str(row.get("example_id") or "")
        if example_id:
            by_id[example_id] = row
    return by_id


def _predict_from_exemplars(
    query_smiles: str,
    exemplars: list[dict[str, Any]],
    *,
    target_profiles: dict[str, dict[str, Any]] | None = None,
    query_numeric_features: dict[str, float] | None = None,
) -> list[dict[str, Any]]:
    target_rows: dict[str, dict[str, Any]] = {}
    for exemplar in exemplars:
        similarity = ligand_similarity_score(query_smiles, exemplar.get("ligand_smiles"))
        if similarity <= 0.0:
            continue
        affinity_log10 = _safe_float(exemplar.get("affinity_log10"))
        strength = affinity_strength_from_log10(affinity_log10)
        memory_weight = _safe_float(exemplar.get("memory_weight")) or 1.0
        support = similarity * memory_weight
        target_id = str(exemplar.get("target_id") or "")
        if not target_id:
            continue
        target_profile = (target_profiles or {}).get(target_id) or {}
        target_prior = _safe_float((target_profile or {}).get("target_prior_score")) or 0.0
        query_context_alignment = score_query_context_against_target_profile(
            query_numeric_features,
            target_profile if isinstance(target_profile, dict) else None,
        )
        raw_score = support * (
            0.52
            + (0.23 * strength)
            + (0.15 * target_prior)
            + (0.10 * query_context_alignment)
        )
        row = target_rows.setdefault(target_id, {
            "target_id": target_id,
            "raw_score": 0.0,
            "weighted_affinity": 0.0,
            "weight_total": 0.0,
            "best_similarity": 0.0,
            "supporting_examples": [],
            "source_databases": set(),
            "target_prior_score": target_prior,
            "query_context_alignment": 0.0,
        })
        row["raw_score"] += raw_score
        if affinity_log10 is not None:
            row["weighted_affinity"] += support * affinity_log10
            row["weight_total"] += support
        row["best_similarity"] = max(float(row["best_similarity"]), similarity)
        row["supporting_examples"].append({
            "example_id": exemplar.get("example_id"),
            "pdb_id": exemplar.get("pdb_id"),
            "pair_identity_key": exemplar.get("pair_identity_key"),
            "similarity": round(similarity, 4),
            "memory_weight": exemplar.get("memory_weight"),
            "source_database": exemplar.get("source_database"),
        })
        source_database = str(exemplar.get("source_database") or "")
        if source_database:
            row["source_databases"].add(source_database)
        row["query_context_alignment"] = max(
            float(row["query_context_alignment"]),
            float(query_context_alignment),
        )

    ranked: list[dict[str, Any]] = []
    for row in target_rows.values():
        avg_affinity_log10 = (
            row["weighted_affinity"] / row["weight_total"]
            if row["weight_total"] > 0
            else None
        )
        predicted_kd = None if avg_affinity_log10 is None else round(10 ** avg_affinity_log10, 3)
        predicted_delta_g = None if avg_affinity_log10 is None else round(-1.364 * avg_affinity_log10, 3)
        confidence = logistic_confidence(float(row["raw_score"]))
        ranked.append({
            "target_id": row["target_id"],
            "predicted_kd_nM": predicted_kd,
            "predicted_delta_g_proxy": predicted_delta_g,
            "confidence_score": round(confidence, 4),
            "ligand_similarity": round(float(row["best_similarity"]), 4),
            "source_databases": sorted(row["source_databases"]),
            "target_prior_score": round(float(row["target_prior_score"]), 4),
            "query_context_alignment": round(float(row["query_context_alignment"]), 4),
            "supporting_examples": sorted(
                row["supporting_examples"],
                key=lambda item: float(item["similarity"]),
                reverse=True,
            )[:5],
            "ranking_basis": "trained_ligand_memory_model_with_pair_features",
            "raw_score": row["raw_score"],
        })
    ranked.sort(key=lambda item: (float(item["raw_score"]), float(item["confidence_score"])), reverse=True)
    for index, row in enumerate(ranked, start=1):
        row["rank"] = index
        row.pop("raw_score", None)
    return ranked


def train_ligand_memory_model(layout: StorageLayout) -> tuple[Path, dict[str, Any]]:
    examples_by_id = _training_examples_by_id(layout)
    train_ids = _read_split_ids(layout.splits_dir / "train.txt")
    selected_ids = sorted(example_id for example_id in train_ids if example_id in examples_by_id)
    exemplars = [
        exemplar
        for example_id in selected_ids
        if (exemplar := _example_to_exemplar(examples_by_id[example_id])) is not None
    ]
    target_profiles = _target_profiles(exemplars)
    model = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "status": "trained" if exemplars else "no_training_exemplars_available",
        "model_type": "ligand_memory_baseline",
        "training_split": "train",
        "training_example_count": len(selected_ids),
        "training_exemplar_count": len(exemplars),
        "target_count": len({str(item.get('target_id') or '') for item in exemplars if item.get('target_id')}),
        "numeric_feature_keys": [_feature_key_name(section, field) for section, field in _NUMERIC_FEATURE_KEYS],
        "biological_assumptions": [
            "Predictions are memory-based over known ligand-target exemplars.",
            "Performance depends on chemical neighborhood coverage in the current training split.",
            "Graph and electrostatic-like pair features contribute only as modest target priors, not as a mechanistic simulation.",
            "This baseline does not infer new binding mechanisms beyond observed training examples.",
        ],
        "exemplars": exemplars,
        "target_profiles": target_profiles,
    }
    layout.models_dir.mkdir(parents=True, exist_ok=True)
    out_path = layout.models_dir / _MODEL_FILENAME
    out_path.write_text(json.dumps(model, indent=2), encoding="utf-8")
    return out_path, model


def load_ligand_memory_model(layout: StorageLayout) -> dict[str, Any] | None:
    raw = _read_json(layout.models_dir / _MODEL_FILENAME)
    if not isinstance(raw, dict):
        return None
    if str(raw.get("status") or "") != "trained":
        return None
    exemplars = raw.get("exemplars")
    if not isinstance(exemplars, list) or not exemplars:
        return None
    return raw


def predict_with_ligand_memory_model(
    layout: StorageLayout,
    *,
    query_smiles: str,
    query_numeric_features: dict[str, float] | None = None,
) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
    model = load_ligand_memory_model(layout)
    if model is None:
        return None, []
    target_profiles = model.get("target_profiles") if isinstance(model.get("target_profiles"), dict) else {}
    ranked = _predict_from_exemplars(
        query_smiles,
        model.get("exemplars") or [],
        target_profiles=target_profiles,
        query_numeric_features=query_numeric_features,
    )
    return model, ranked


def evaluate_ligand_memory_model(layout: StorageLayout) -> tuple[Path, dict[str, Any]]:
    examples_by_id = _training_examples_by_id(layout)
    train_ids = _read_split_ids(layout.splits_dir / "train.txt")
    val_ids = _read_split_ids(layout.splits_dir / "val.txt")
    test_ids = _read_split_ids(layout.splits_dir / "test.txt")
    train_exemplars = [
        exemplar
        for example_id in sorted(train_ids)
        if example_id in examples_by_id
        if (exemplar := _example_to_exemplar(examples_by_id[example_id])) is not None
    ]
    train_target_profiles = _target_profiles(train_exemplars)

    def _evaluate_split(split_name: str, split_ids: set[str]) -> dict[str, Any]:
        evaluated = 0
        top1_hits = 0
        affinity_errors: list[float] = []
        for example_id in sorted(split_ids):
            example = examples_by_id.get(example_id)
            if not isinstance(example, dict):
                continue
            exemplar = _example_to_exemplar(example)
            if exemplar is None:
                continue
            ranked = _predict_from_exemplars(
                str(exemplar.get("ligand_smiles") or ""),
                train_exemplars,
                target_profiles=train_target_profiles,
            )
            if not ranked:
                continue
            evaluated += 1
            top_target = ranked[0]
            if str(top_target.get("target_id") or "") == str(exemplar.get("target_id") or ""):
                top1_hits += 1
            predicted_kd = _safe_float(top_target.get("predicted_kd_nM"))
            actual_log10 = _safe_float(exemplar.get("affinity_log10"))
            actual_kd = None if actual_log10 is None else 10 ** actual_log10
            if predicted_kd is not None and actual_kd is not None:
                affinity_errors.append(abs(predicted_kd - actual_kd))
        return {
            "evaluated_count": evaluated,
            "top1_target_accuracy": round(top1_hits / evaluated, 4) if evaluated else None,
            "affinity_mae_nM": round(sum(affinity_errors) / len(affinity_errors), 4) if affinity_errors else None,
        }

    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "status": "evaluated" if train_exemplars else "no_training_exemplars_available",
        "model_type": "ligand_memory_baseline",
        "train_exemplar_count": len(train_exemplars),
        "splits": {
            "val": _evaluate_split("val", val_ids),
            "test": _evaluate_split("test", test_ids),
        },
        "notes": (
            "Evaluation uses the current train split as the memory bank and predicts held-out "
            "ligands by ligand-string similarity against known training exemplars."
        ),
    }
    layout.models_dir.mkdir(parents=True, exist_ok=True)
    out_path = layout.models_dir / _EVAL_FILENAME
    out_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    return out_path, summary
