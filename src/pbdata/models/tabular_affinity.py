"""Lightweight supervised tabular affinity model.

Assumptions:
- Uses existing assembled training examples and split files only.
- Learns a conservative ridge-style linear regressor over numeric fields.
- Intended as the first non-memory supervised comparator, not a final model.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pbdata.models.affinity_models import affinity_strength_from_log10, logistic_confidence
from pbdata.models.baseline_memory import evaluate_ligand_memory_model
from pbdata.storage import StorageLayout

_MODEL_FILENAME = "tabular_affinity_model.json"
_EVAL_FILENAME = "tabular_affinity_evaluation.json"
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
    ("protein", "aromatic_fraction"),
    ("protein", "charged_fraction"),
    ("protein", "polar_fraction"),
    ("ligand", "molecular_weight"),
    ("experiment", "reported_measurement_count"),
)
_SMILES_HASH_BINS = 32
_TARGET_HASH_BINS = 16
_LIGAND_HASH_BINS = 8
_SOURCE_HASH_BINS = 8


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


def _feature_name(section: str, field: str) -> str:
    return f"{section}.{field}"


def _training_examples_by_id(layout: StorageLayout) -> dict[str, dict[str, Any]]:
    raw = _read_json(layout.training_dir / "training_examples.json")
    if not isinstance(raw, list):
        return {}
    by_id: dict[str, dict[str, Any]] = {}
    for row in raw:
        if not isinstance(row, dict):
            continue
        example_id = str(row.get("example_id") or "")
        if example_id:
            by_id[example_id] = row
    return by_id


def _extract_numeric_features(example: dict[str, Any]) -> dict[str, float]:
    features: dict[str, float] = {}
    for section_name, field_name in _NUMERIC_FEATURE_KEYS:
        section = example.get(section_name) or {}
        if not isinstance(section, dict):
            continue
        value = _safe_float(section.get(field_name))
        if value is not None:
            features[_feature_name(section_name, field_name)] = value
    return features


def _hash_bucket(prefix: str, token: str, *, bins: int) -> str:
    digest = hashlib.md5(f"{prefix}:{token}".encode("utf-8")).hexdigest()
    return f"{prefix}.hash_{int(digest[:8], 16) % bins:02d}"


def _hashed_text_features(prefix: str, text: str, *, bins: int, ngrams: tuple[int, ...] = (1,)) -> dict[str, float]:
    cleaned = str(text or "").strip()
    if not cleaned:
        return {}
    tokens: list[str] = []
    if ngrams == (1,):
        tokens = [cleaned]
    else:
        for ngram_size in ngrams:
            if len(cleaned) < ngram_size:
                continue
            tokens.extend(cleaned[idx:idx + ngram_size] for idx in range(len(cleaned) - ngram_size + 1))
    counts: dict[str, float] = {}
    for token in tokens:
        bucket = _hash_bucket(prefix, token, bins=bins)
        counts[bucket] = float(counts.get(bucket, 0.0)) + 1.0
    scale = float(len(tokens)) or 1.0
    return {bucket: round(value / scale, 6) for bucket, value in counts.items()}


def _example_row(example: dict[str, Any]) -> dict[str, Any] | None:
    example_id = str(example.get("example_id") or "")
    protein = example.get("protein") or {}
    ligand = example.get("ligand") or {}
    provenance = example.get("provenance") or {}
    labels = example.get("labels") or {}
    experiment = example.get("experiment") or {}
    affinity_log10 = _safe_float(labels.get("binding_affinity_log10"))
    target_id = str(protein.get("uniprot_id") or "")
    ligand_id = str(ligand.get("ligand_id") or "")
    ligand_smiles = str(ligand.get("smiles") or "")
    affinity_type = str(labels.get("affinity_type") or experiment.get("affinity_type") or "")
    source_database = str(provenance.get("preferred_source_database") or provenance.get("source_database") or experiment.get("source_database") or "")
    if not example_id or affinity_log10 is None:
        return None
    features = _extract_numeric_features(example)
    features.update(_hashed_text_features("ligand_smiles", ligand_smiles, bins=_SMILES_HASH_BINS, ngrams=(2, 3)))
    features.update(_hashed_text_features("target_id", target_id, bins=_TARGET_HASH_BINS))
    features.update(_hashed_text_features("ligand_id", ligand_id, bins=_LIGAND_HASH_BINS))
    features.update(_hashed_text_features("source_database", source_database, bins=_SOURCE_HASH_BINS))
    features.update(_hashed_text_features("affinity_type", affinity_type, bins=4))
    return {
        "example_id": example_id,
        "target_id": target_id,
        "ligand_smiles": ligand_smiles,
        "pair_identity_key": str(provenance.get("pair_identity_key") or ""),
        "affinity_log10": affinity_log10,
        "features": features,
    }


def _transpose(matrix: list[list[float]]) -> list[list[float]]:
    return [list(column) for column in zip(*matrix)] if matrix else []


def _matmul(left: list[list[float]], right: list[list[float]]) -> list[list[float]]:
    if not left or not right:
        return []
    right_t = _transpose(right)
    return [[sum(lval * rval for lval, rval in zip(row, column)) for column in right_t] for row in left]


def _invert_square_matrix(matrix: list[list[float]]) -> list[list[float]]:
    size = len(matrix)
    augmented = [
        [float(value) for value in row] + [1.0 if idx == row_idx else 0.0 for idx in range(size)]
        for row_idx, row in enumerate(matrix)
    ]
    for col in range(size):
        pivot = max(range(col, size), key=lambda row_idx: abs(augmented[row_idx][col]))
        if abs(augmented[pivot][col]) < 1e-12:
            augmented[col][col] += 1e-6
            pivot = col
        augmented[col], augmented[pivot] = augmented[pivot], augmented[col]
        pivot_value = augmented[col][col]
        augmented[col] = [value / pivot_value for value in augmented[col]]
        for row_idx in range(size):
            if row_idx == col:
                continue
            factor = augmented[row_idx][col]
            augmented[row_idx] = [
                current - factor * pivoted
                for current, pivoted in zip(augmented[row_idx], augmented[col])
            ]
    return [row[size:] for row in augmented]


def _least_squares_vector(x_rows: list[list[float]], y_values: list[float]) -> list[float]:
    x_t = _transpose(x_rows)
    xtx = _matmul(x_t, x_rows)
    for idx in range(len(xtx)):
        xtx[idx][idx] += 1e-4
    y_matrix = [[value] for value in y_values]
    xty = _matmul(x_t, y_matrix)
    solved = _matmul(_invert_square_matrix(xtx), xty)
    return [float(row[0]) for row in solved]


def _feature_layout(rows: list[dict[str, Any]]) -> tuple[list[str], list[float], list[float]]:
    feature_names = sorted({name for row in rows for name in row["features"].keys()})
    means: list[float] = []
    stds: list[float] = []
    for name in feature_names:
        values = [float(row["features"].get(name, 0.0)) for row in rows]
        mean = sum(values) / len(values)
        variance = sum((value - mean) ** 2 for value in values) / max(len(values), 1)
        means.append(mean)
        stds.append(max(variance ** 0.5, 1e-6))
    return feature_names, means, stds


def _vectorize(row: dict[str, Any], feature_names: list[str], means: list[float], stds: list[float]) -> list[float]:
    standardized = [
        (float(row["features"].get(name, 0.0)) - means[idx]) / stds[idx]
        for idx, name in enumerate(feature_names)
    ]
    return standardized + [1.0]


def _predict_row(model: dict[str, Any], row: dict[str, Any]) -> float:
    feature_names = list(model["feature_names"])
    means = [float(value) for value in model["feature_means"]]
    stds = [float(value) for value in model["feature_stds"]]
    weights = [float(value) for value in model["weights"]]
    vector = _vectorize(row, feature_names, means, stds)
    return sum(value * weight for value, weight in zip(vector, weights))


def _query_conditioned_row(
    example: dict[str, Any],
    *,
    query_smiles: str,
    query_numeric_features: dict[str, float] | None = None,
) -> dict[str, Any] | None:
    row = _example_row(example)
    if row is None:
        return None
    conditioned = {
        "example_id": row["example_id"],
        "target_id": row["target_id"],
        "ligand_smiles": query_smiles,
        "pair_identity_key": row["pair_identity_key"],
        "affinity_log10": row["affinity_log10"],
        "features": dict(row["features"]),
    }
    conditioned["features"].update(_hashed_text_features("ligand_smiles", query_smiles, bins=_SMILES_HASH_BINS, ngrams=(2, 3)))
    conditioned["features"].update(_hashed_text_features("ligand_id", "", bins=_LIGAND_HASH_BINS))
    if query_numeric_features:
        for key, value in query_numeric_features.items():
            numeric = _safe_float(value)
            if numeric is not None:
                conditioned["features"][key] = numeric
    return conditioned


def _overlap_sets(rows: list[dict[str, Any]]) -> dict[str, set[str]]:
    return {
        "target_ids": {str(row.get("target_id") or "") for row in rows if str(row.get("target_id") or "")},
        "ligand_smiles": {str(row.get("ligand_smiles") or "") for row in rows if str(row.get("ligand_smiles") or "")},
        "pair_keys": {str(row.get("pair_identity_key") or "") for row in rows if str(row.get("pair_identity_key") or "")},
    }


def train_tabular_affinity_model(layout: StorageLayout) -> tuple[Path, dict[str, Any]]:
    examples_by_id = _training_examples_by_id(layout)
    train_ids = _read_split_ids(layout.splits_dir / "train.txt")
    rows = [
        row
        for example_id in sorted(train_ids)
        if example_id in examples_by_id
        if (row := _example_row(examples_by_id[example_id])) is not None
    ]
    status = "trained" if rows else "no_training_examples_available"
    feature_names, means, stds = _feature_layout(rows) if rows else ([], [], [])
    x_rows = [_vectorize(row, feature_names, means, stds) for row in rows] if rows else []
    y_values = [float(row["affinity_log10"]) for row in rows]
    weights = _least_squares_vector(x_rows, y_values) if rows else []
    model = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "status": status,
        "model_type": "tabular_affinity_linear_regression",
        "training_split": "train",
        "training_example_count": len(rows),
        "feature_names": feature_names,
        "feature_means": means,
        "feature_stds": stds,
        "weights": weights,
        "biological_assumptions": [
            "This model is a numeric-feature regressor over assembled training examples.",
            "Hashed ligand/target/source features provide lightweight categorical signal without external chemistry toolkits.",
            "It learns from current split-defined examples only and does not encode explicit structure geometry.",
            "Outputs are suitable as an early supervised comparator, not as a final scientific model.",
        ],
    }
    layout.models_dir.mkdir(parents=True, exist_ok=True)
    out_path = layout.models_dir / _MODEL_FILENAME
    out_path.write_text(json.dumps(model, indent=2), encoding="utf-8")
    return out_path, model


def load_tabular_affinity_model(layout: StorageLayout) -> dict[str, Any] | None:
    raw = _read_json(layout.models_dir / _MODEL_FILENAME)
    if not isinstance(raw, dict):
        return None
    if str(raw.get("status") or "") != "trained":
        return None
    if not isinstance(raw.get("weights"), list) or not raw.get("weights"):
        return None
    return raw


def predict_with_tabular_affinity_model(
    layout: StorageLayout,
    *,
    query_smiles: str,
    query_numeric_features: dict[str, float] | None = None,
) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
    model = load_tabular_affinity_model(layout)
    if model is None or not str(query_smiles or "").strip():
        return None, []
    examples_by_id = _training_examples_by_id(layout)
    candidate_rows: list[dict[str, Any]] = []
    for example in examples_by_id.values():
        if not isinstance(example, dict):
            continue
        conditioned = _query_conditioned_row(
            example,
            query_smiles=query_smiles,
            query_numeric_features=query_numeric_features,
        )
        if conditioned is not None:
            candidate_rows.append(conditioned)

    by_target: dict[str, dict[str, Any]] = {}
    for row in candidate_rows:
        target_id = str(row.get("target_id") or "")
        if not target_id:
            continue
        predicted_log10 = _predict_row(model, row)
        strength = affinity_strength_from_log10(predicted_log10)
        predicted_kd = round(10 ** predicted_log10, 3)
        predicted_delta_g = round(-1.364 * predicted_log10, 3)
        support = by_target.setdefault(target_id, {
            "target_id": target_id,
            "best_predicted_log10": predicted_log10,
            "best_supporting_example_id": str(row.get("example_id") or ""),
            "best_pair_identity_key": str(row.get("pair_identity_key") or ""),
            "support_count": 0,
            "supporting_examples": [],
        })
        support["support_count"] = int(support["support_count"]) + 1
        support["supporting_examples"].append({
            "example_id": str(row.get("example_id") or ""),
            "pair_identity_key": str(row.get("pair_identity_key") or ""),
            "predicted_affinity_log10": round(predicted_log10, 4),
        })
        if predicted_log10 < float(support["best_predicted_log10"]):
            support["best_predicted_log10"] = predicted_log10
            support["best_supporting_example_id"] = str(row.get("example_id") or "")
            support["best_pair_identity_key"] = str(row.get("pair_identity_key") or "")

    ranked: list[dict[str, Any]] = []
    for target_id, payload in by_target.items():
        predicted_log10 = float(payload["best_predicted_log10"])
        support_count = int(payload["support_count"])
        strength = affinity_strength_from_log10(predicted_log10)
        confidence = logistic_confidence((1.75 * strength) + (0.08 * min(support_count, 5)))
        ranked.append({
            "target_id": target_id,
            "predicted_kd_nM": round(10 ** predicted_log10, 3),
            "predicted_delta_g_proxy": round(-1.364 * predicted_log10, 3),
            "predicted_affinity_log10": round(predicted_log10, 4),
            "confidence_score": round(confidence, 4),
            "support_count": support_count,
            "best_supporting_example_id": str(payload["best_supporting_example_id"]),
            "supporting_pair_identity_key": str(payload["best_pair_identity_key"]),
            "ranking_basis": "query_conditioned_tabular_affinity_model",
            "supporting_examples": sorted(
                payload["supporting_examples"],
                key=lambda item: float(item["predicted_affinity_log10"]),
            )[:5],
        })
    ranked.sort(
        key=lambda item: (
            float(item["predicted_affinity_log10"]),
            -float(item["confidence_score"]),
        ),
    )
    for index, row in enumerate(ranked, start=1):
        row["rank"] = index
    return model, ranked


def evaluate_tabular_affinity_model(layout: StorageLayout) -> tuple[Path, dict[str, Any]]:
    model = load_tabular_affinity_model(layout)
    if model is None:
        _, trained = train_tabular_affinity_model(layout)
        model = trained
    examples_by_id = _training_examples_by_id(layout)
    train_rows = [
        row
        for example_id in sorted(_read_split_ids(layout.splits_dir / "train.txt"))
        if example_id in examples_by_id
        if (row := _example_row(examples_by_id[example_id])) is not None
    ]
    overlap_sets = _overlap_sets(train_rows)

    def _evaluate_split(split_ids: set[str]) -> dict[str, Any]:
        evaluated = 0
        no_prediction_count = 0
        abs_errors: list[float] = []
        squared_errors: list[float] = []
        same_ligand_in_train_count = 0
        same_target_in_train_count = 0
        exact_pair_seen_in_train_count = 0
        novel_case_count = 0
        for example_id in sorted(split_ids):
            example = examples_by_id.get(example_id)
            if not isinstance(example, dict):
                continue
            row = _example_row(example)
            if row is None:
                continue
            ligand_smiles = str(row.get("ligand_smiles") or "")
            target_id = str(row.get("target_id") or "")
            pair_key = str(row.get("pair_identity_key") or "")
            if ligand_smiles in overlap_sets["ligand_smiles"]:
                same_ligand_in_train_count += 1
            if target_id in overlap_sets["target_ids"]:
                same_target_in_train_count += 1
            if pair_key in overlap_sets["pair_keys"]:
                exact_pair_seen_in_train_count += 1
            if ligand_smiles not in overlap_sets["ligand_smiles"] and target_id not in overlap_sets["target_ids"]:
                novel_case_count += 1
            if str(model.get("status") or "") != "trained":
                no_prediction_count += 1
                continue
            prediction = _predict_row(model, row)
            evaluated += 1
            actual = float(row["affinity_log10"])
            abs_errors.append(abs(prediction - actual))
            squared_errors.append((prediction - actual) ** 2)
        return {
            "evaluated_count": evaluated,
            "no_prediction_count": no_prediction_count,
            "affinity_mae_log10": round(sum(abs_errors) / len(abs_errors), 4) if abs_errors else None,
            "affinity_rmse_log10": round((sum(squared_errors) / len(squared_errors)) ** 0.5, 4) if squared_errors else None,
            "same_ligand_in_train_count": same_ligand_in_train_count,
            "same_target_in_train_count": same_target_in_train_count,
            "exact_pair_seen_in_train_count": exact_pair_seen_in_train_count,
            "novel_case_count": novel_case_count,
        }

    _, baseline_eval = evaluate_ligand_memory_model(layout)
    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "status": str(model.get("status") or "no_training_examples_available"),
        "model_type": "tabular_affinity_linear_regression",
        "training_example_count": int(model.get("training_example_count") or 0),
        "feature_count": len(model.get("feature_names") or []),
        "splits": {
            "val": _evaluate_split(_read_split_ids(layout.splits_dir / "val.txt")),
            "test": _evaluate_split(_read_split_ids(layout.splits_dir / "test.txt")),
        },
        "baseline_comparison": {
            split_name: {
                "tabular_affinity_mae_log10": split_summary.get("affinity_mae_log10"),
                "baseline_top1_target_accuracy": ((baseline_eval.get("splits") or {}).get(split_name) or {}).get("top1_target_accuracy"),
                "baseline_affinity_mae_log10": ((baseline_eval.get("splits") or {}).get(split_name) or {}).get("affinity_mae_log10"),
            }
            for split_name, split_summary in {
                "val": _evaluate_split(_read_split_ids(layout.splits_dir / "val.txt")),
                "test": _evaluate_split(_read_split_ids(layout.splits_dir / "test.txt")),
            }.items()
        },
        "notes": (
            "This model is a simple supervised numeric regressor intended as a first non-memory comparator "
            "against the ligand-memory baseline."
        ),
    }
    layout.models_dir.mkdir(parents=True, exist_ok=True)
    out_path = layout.models_dir / _EVAL_FILENAME
    out_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    return out_path, summary
