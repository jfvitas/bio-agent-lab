"""Executable Model Studio training and import helpers."""

from __future__ import annotations

import hashlib
import json
import math
import pickle
import shutil
import csv
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pbdata.storage import StorageLayout
from pbdata.modeling.graph_contract import build_graph_learning_contract
from pbdata.modeling.graph_dataset import materialize_graph_dataset_records
from pbdata.modeling.graph_pyg_adapter import build_pyg_ready_graph_samples
from pbdata.modeling.graph_samples import build_graph_sample_manifest
from pbdata.modeling.graph_training_payload import materialize_graph_training_payload
from pbdata.modeling.hybrid_training_payload import materialize_hybrid_training_payload
from pbdata.modeling.pyg_training import train_pyg_gnn, train_pyg_hybrid_fusion
from pbdata.modeling.trainer_registry import resolve_trainer_backend

_NUMERIC_SECTIONS: tuple[str, ...] = (
    "structure",
    "protein",
    "ligand",
    "interaction",
    "experiment",
    "graph_features",
)
_CATEGORICAL_FIELDS: tuple[tuple[str, str, int], ...] = (
    ("ligand", "ligand_id", 16),
    ("ligand", "ligand_type", 8),
    ("protein", "uniprot_id", 16),
    ("protein", "gene", 16),
    ("experiment", "affinity_type", 8),
    ("experiment", "source_database", 8),
    ("experiment", "preferred_source_database", 8),
)
_INFERENCE_FEATURE_VALUE_MAP: tuple[tuple[str, str, str], ...] = (
    ("structure", "resolution", "structure_resolution"),
    ("structure", "atom_count_total", "atom_count_total"),
    ("structure", "residue_count_observed", "residue_count_observed"),
    ("structure", "radius_of_gyration_residue_centroids", "radius_of_gyration_residue_centroids"),
    ("protein", "sequence_length", "sequence_length"),
    ("protein", "mean_hydropathy", "protein_mean_hydropathy"),
    ("protein", "aromatic_fraction", "protein_aromatic_fraction"),
    ("protein", "charged_fraction", "protein_charged_fraction"),
    ("protein", "polar_fraction", "protein_polar_fraction"),
    ("ligand", "molecular_weight", "ligand_molecular_weight"),
    ("interaction", "interface_residue_count", "interface_residue_count"),
    ("interaction", "microstate_record_count", "microstate_record_count"),
    ("graph_features", "network_degree", "network_degree"),
    ("graph_features", "ppi_degree", "ppi_degree"),
    ("graph_features", "pli_degree", "pli_degree"),
    ("graph_features", "pathway_count", "pathway_count"),
    ("experiment", "reported_measurement_count", "reported_measurement_count"),
)


@dataclass(frozen=True)
class TrainingRunResult:
    run_name: str
    run_dir: Path
    family: str
    task: str
    metrics: dict[str, Any]
    warnings: tuple[str, ...]
    summary: str


@dataclass(frozen=True)
class RunComparison:
    run_name: str
    location: Path
    family: str
    requested_family: str
    executed_family: str
    task: str
    source: str
    functional_status: str
    backend_id: str
    primary_metric_name: str
    primary_metric_value: float | None
    summary: str


@dataclass(frozen=True)
class RunInspection:
    run_name: str
    location: Path
    family: str
    requested_family: str
    executed_family: str
    task: str
    source: str
    runtime_target: str
    functional_status: str
    backend_id: str
    primary_metric_name: str
    primary_metric_value: float | None
    epoch_count: int
    chart_ready: bool
    test_plot_ready: bool
    split_counts: dict[str, int]
    metrics: dict[str, Any]
    history_summary: dict[str, Any]
    warnings: tuple[str, ...]
    artifacts: dict[str, str]


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _read_json(path: Path) -> Any:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def _find_import_payload_root(source_path: Path, import_manifest: dict[str, Any] | None) -> Path:
    configured_root = ""
    if isinstance(import_manifest, dict):
        configured_root = str(
            import_manifest.get("model_output_dir")
            or import_manifest.get("payload_root")
            or ""
        ).strip()
    if configured_root:
        configured_path = (source_path / configured_root).resolve()
        if configured_path.exists() and configured_path.is_dir():
            return configured_path

    candidate_roots = [
        source_path / "model_outputs",
        source_path / "outputs",
        source_path / "output",
        source_path / "artifacts",
        source_path / "run",
        source_path,
    ]
    model_markers = (
        "model.pkl",
        "model.pt",
        "checkpoint.pt",
        "model.json",
        "metrics.json",
        "run_metrics.json",
        "run_manifest.json",
    )
    for candidate in candidate_roots:
        if candidate.exists() and candidate.is_dir() and any((candidate / marker).exists() for marker in model_markers):
            return candidate
    for candidate in source_path.rglob("*"):
        if candidate.is_dir() and any((candidate / marker).exists() for marker in model_markers):
            return candidate
    return source_path


def _normalize_import_metrics(dest_dir: Path) -> dict[str, Any]:
    metric_candidates = [
        dest_dir / "metrics.json",
        dest_dir / "run_metrics.json",
        dest_dir / "evaluation.json",
        dest_dir / "results.json",
    ]
    for candidate in metric_candidates:
        payload = _read_json(candidate)
        if isinstance(payload, dict):
            normalized = payload.get("metrics") if isinstance(payload.get("metrics"), dict) else payload
            if not (dest_dir / "metrics.json").exists():
                (dest_dir / "metrics.json").write_text(json.dumps(normalized, indent=2), encoding="utf-8")
            return normalized
    normalized = {"status": "imported_without_metrics"}
    if not (dest_dir / "metrics.json").exists():
        (dest_dir / "metrics.json").write_text(json.dumps(normalized, indent=2), encoding="utf-8")
    return normalized


def _extract_primary_metric(metrics: Any) -> tuple[str, float | None]:
    if not isinstance(metrics, dict):
        return "metric", None
    for section_name in ("test", "test_metrics", "evaluation", "validation_metrics", "val", "train"):
        section = metrics.get(section_name)
        if isinstance(section, dict) and section:
            if "rmse" in section:
                return "rmse", _safe_float(section.get("rmse"))
            if "f1" in section:
                return "f1", _safe_float(section.get("f1"))
            if "accuracy" in section:
                return "accuracy", _safe_float(section.get("accuracy"))
            if "mae" in section:
                return "mae", _safe_float(section.get("mae"))
    if isinstance(metrics.get("unsupervised"), dict):
        unsupervised_metrics = metrics.get("unsupervised") or {}
        if "silhouette" in unsupervised_metrics:
            return "silhouette", _safe_float(unsupervised_metrics.get("silhouette"))
        if "reconstruction_rmse" in unsupervised_metrics:
            return "reconstruction_rmse", _safe_float(unsupervised_metrics.get("reconstruction_rmse"))
    for key in ("rmse", "f1", "accuracy", "mae", "score"):
        if key in metrics:
            metric_name = "metric" if key == "score" else key
            return metric_name, _safe_float(metrics.get(key))
    return "metric", None


def _functional_status_from_manifest(manifest: dict[str, Any]) -> str:
    status = str(manifest.get("functional_status") or "").strip()
    if status:
        return status
    backend_plan = manifest.get("backend_plan") or {}
    implementation = str(backend_plan.get("implementation") or "").strip()
    if implementation == "native":
        return "first_class"
    if implementation == "fallback":
        return "fallback"
    if implementation == "surrogate":
        return "surrogate"
    if implementation == "unsupported":
        return "planned"
    return "unknown"


def _executed_family_from_backend(backend_id: str, fallback_family: str) -> str:
    backend_to_family = {
        "xgboost": "xgboost",
        "sklearn_hist_gradient_boosting": "hist_gradient_boosting",
        "sklearn_random_forest": "random_forest",
        "torch_tabular_mlp": "dense_nn",
        "sklearn_mlp": "dense_nn",
        "pyg_gnn": "gnn",
        "pyg_hybrid_fusion": "hybrid_fusion",
        "graph_surrogate_dense": "dense_nn",
        "hybrid_surrogate_dense": "dense_nn",
        "sklearn_kmeans": "clustering",
        "pca_autoencoder": "pca_embedding",
    }
    return backend_to_family.get(str(backend_id or "").strip(), fallback_family)


def _requested_family_from_manifest(manifest: dict[str, Any], metrics: dict[str, Any]) -> str:
    return str(
        manifest.get("requested_family")
        or manifest.get("family")
        or metrics.get("requested_family")
        or metrics.get("family")
        or "unknown"
    )


def _executed_family_from_manifest(manifest: dict[str, Any], metrics: dict[str, Any]) -> str:
    backend_plan = manifest.get("backend_plan") or {}
    backend_id = str(backend_plan.get("backend_id") or manifest.get("backend") or metrics.get("backend") or "").strip()
    fallback_family = str(
        manifest.get("executed_family")
        or backend_plan.get("execution_family")
        or manifest.get("family")
        or metrics.get("family")
        or "unknown"
    )
    return _executed_family_from_backend(backend_id, fallback_family)


def _extract_split_counts(split_summary: Any) -> dict[str, int]:
    split_counts: dict[str, int] = {}
    if not isinstance(split_summary, dict):
        return split_counts
    nested_counts = split_summary.get("counts")
    if isinstance(nested_counts, dict):
        for split_name in ("train", "val", "test", "all"):
            if split_name in nested_counts:
                split_counts[split_name] = int(nested_counts.get(split_name) or 0)
    for split_name in ("train", "val", "test", "all"):
        details = split_summary.get(split_name)
        if isinstance(details, dict):
            split_counts[split_name] = int(details.get("count") or split_counts.get(split_name) or 0)
    return split_counts


def _copy_alias_artifact(dest_dir: Path, canonical_name: str, aliases: tuple[str, ...]) -> str | None:
    canonical_path = dest_dir / canonical_name
    if canonical_path.exists():
        return canonical_name
    for alias in aliases:
        alias_path = dest_dir / alias
        if alias_path.exists() and alias_path.is_file():
            shutil.copy2(alias_path, canonical_path)
            return canonical_name
    return None


def _normalize_import_artifacts(dest_dir: Path) -> dict[str, str | None]:
    artifacts: dict[str, str | None] = {}
    artifacts["split_summary"] = _copy_alias_artifact(
        dest_dir,
        "split_summary.json",
        ("splits.json", "split_stats.json", "dataset_splits.json"),
    )
    artifacts["history"] = _copy_alias_artifact(
        dest_dir,
        "history.json",
        ("training_history.json", "history_export.json", "fit_history.json"),
    )
    artifacts["training_curve"] = _copy_alias_artifact(
        dest_dir,
        "training_curve.svg",
        ("learning_curve.svg", "loss_curve.svg", "train_curve.svg"),
    )
    artifacts["test_performance"] = _copy_alias_artifact(
        dest_dir,
        "test_performance.svg",
        ("evaluation.svg", "test_metrics.svg", "performance.svg"),
    )
    for split_name, aliases in (
        ("train_predictions", ("train_results.json", "predictions_train.json")),
        ("val_predictions", ("val_results.json", "predictions_val.json", "validation_predictions.json")),
        ("test_predictions", ("test_results.json", "predictions_test.json")),
    ):
        artifacts[split_name] = _copy_alias_artifact(dest_dir, f"{split_name}.json", aliases)
    return artifacts


def _normalize_import_manifest(
    dest_dir: Path,
    *,
    import_manifest: dict[str, Any] | None,
    normalized_metrics: dict[str, Any],
    normalized_artifacts: dict[str, str | None],
) -> dict[str, Any]:
    existing_manifest = _read_json(dest_dir / "run_manifest.json")
    if isinstance(existing_manifest, dict):
        return existing_manifest
    config = _read_json(dest_dir / "config.json") or {}
    config_backend_plan = config.get("backend_plan") if isinstance(config, dict) else {}
    config_model = config.get("model") if isinstance(config, dict) else {}
    runtime_targets = _read_json(dest_dir / "runtime_targets.json") or {}
    runtime_target = (
        str(runtime_targets.get("selected_target") or "")
        if isinstance(runtime_targets, dict)
        else ""
    ) or (
        str((import_manifest or {}).get("target_runtime") or "")
        if isinstance(import_manifest, dict)
        else ""
    ) or "unknown"
    family = str(
        normalized_metrics.get("family")
        or (config.get("family") if isinstance(config, dict) else "")
        or (config_model.get("type") if isinstance(config_model, dict) else "")
        or "unknown"
    )
    task = str(
        normalized_metrics.get("task")
        or (config.get("task") if isinstance(config, dict) else "")
        or "unknown"
    )
    backend = str(
        normalized_metrics.get("backend")
        or (config.get("trainer_backend") if isinstance(config, dict) else "")
        or (config_backend_plan.get("backend_id") if isinstance(config_backend_plan, dict) else "")
        or "unknown"
    )
    backend_plan = {
        "requested_family": str(
            (config_backend_plan.get("requested_family") if isinstance(config_backend_plan, dict) else "")
            or family
        ),
        "execution_family": str(
            (config_backend_plan.get("execution_family") if isinstance(config_backend_plan, dict) else "")
            or family
        ),
        "backend_id": str(
            (config_backend_plan.get("backend_id") if isinstance(config_backend_plan, dict) else "")
            or backend
        ),
        "implementation": str(
            (config_backend_plan.get("implementation") if isinstance(config_backend_plan, dict) else "")
            or ("native" if backend.startswith("pyg_") or backend.startswith("torch_") else "imported")
        ),
        "native_graph": bool(
            (config_backend_plan.get("native_graph") if isinstance(config_backend_plan, dict) else False)
            or backend in {"pyg_gnn", "pyg_hybrid_fusion"}
        ),
    }
    requested_family = family
    executed_family = _executed_family_from_backend(
        str(backend_plan.get("backend_id") or backend),
        str(backend_plan.get("execution_family") or family),
    )
    normalized_manifest = {
        "generated_at": _utc_now(),
        "family": family,
        "requested_family": requested_family,
        "executed_family": executed_family,
        "functional_status": _functional_status_from_manifest({"backend_plan": backend_plan}),
        "task": task,
        "backend": backend,
        "backend_plan": backend_plan,
        "runtime_target": runtime_target,
        "graph_contract": (
            config.get("graph_contract")
            if isinstance(config.get("graph_contract"), dict)
            else {}
        ),
        "warnings": list(normalized_metrics.get("warnings") or []) if isinstance(normalized_metrics, dict) else [],
        "artifacts": {
            "metrics": "metrics.json" if (dest_dir / "metrics.json").exists() else None,
            "split_summary": normalized_artifacts.get("split_summary"),
            "history": normalized_artifacts.get("history"),
            "training_curve": normalized_artifacts.get("training_curve"),
            "test_performance": normalized_artifacts.get("test_performance"),
            "train_predictions": normalized_artifacts.get("train_predictions"),
            "val_predictions": normalized_artifacts.get("val_predictions"),
            "test_predictions": normalized_artifacts.get("test_predictions"),
            "model": (
                "model.pkl" if (dest_dir / "model.pkl").exists()
                else "checkpoint.pt" if (dest_dir / "checkpoint.pt").exists()
                else "model.pt" if (dest_dir / "model.pt").exists()
                else "model.json" if (dest_dir / "model.json").exists()
                else None
            ),
        },
    }
    (dest_dir / "run_manifest.json").write_text(json.dumps(normalized_manifest, indent=2), encoding="utf-8")
    return normalized_manifest


def _safe_float(value: object) -> float | None:
    try:
        if value is None:
            return None
        text = str(value).strip()
        if not text:
            return None
        return float(text)
    except (TypeError, ValueError):
        return None


def _hash_bucket(prefix: str, token: str, *, bins: int) -> str:
    digest = hashlib.md5(f"{prefix}:{token}".encode("utf-8")).hexdigest()
    return f"{prefix}.hash_{int(digest[:8], 16) % bins:02d}"


def _hashed_feature(prefix: str, token: str, *, bins: int) -> dict[str, float]:
    cleaned = str(token or "").strip()
    if not cleaned:
        return {}
    return {_hash_bucket(prefix, cleaned, bins=bins): 1.0}


def _flatten_numeric(prefix: str, value: Any, out: dict[str, float]) -> None:
    if isinstance(value, dict):
        for key, inner in value.items():
            _flatten_numeric(f"{prefix}.{key}" if prefix else str(key), inner, out)
        return
    if isinstance(value, bool):
        out[prefix] = 1.0 if value else 0.0
        return
    numeric = _safe_float(value)
    if numeric is not None:
        out[prefix] = numeric


def _example_features(example: dict[str, Any]) -> dict[str, float]:
    features: dict[str, float] = {}
    for section_name in _NUMERIC_SECTIONS:
        section = example.get(section_name)
        if isinstance(section, dict):
            _flatten_numeric(section_name, section, features)
    for section_name, field_name, bins in _CATEGORICAL_FIELDS:
        section = example.get(section_name) or {}
        if isinstance(section, dict):
            features.update(_hashed_feature(f"{section_name}.{field_name}", section.get(field_name) or "", bins=bins))
    return features


def _graph_context_by_pdb(layout: StorageLayout) -> dict[str, dict[str, float]]:
    nodes = _read_json(layout.graph_dir / "graph_nodes.json")
    edges = _read_json(layout.graph_dir / "graph_edges.json")
    context: dict[str, dict[str, float]] = {}
    if isinstance(nodes, list):
        for node in nodes:
            if not isinstance(node, dict):
                continue
            metadata = node.get("metadata") or {}
            pdb_id = str(metadata.get("pdb_id") or node.get("primary_id") or "").strip().upper()
            if not pdb_id:
                continue
            row = context.setdefault(pdb_id, {})
            row["graph_context.node_count"] = float(row.get("graph_context.node_count", 0.0)) + 1.0
            node_type = str(node.get("node_type") or "").strip()
            if node_type:
                key = f"graph_context.node_type.{node_type.lower()}"
                row[key] = float(row.get(key, 0.0)) + 1.0
    if isinstance(edges, list):
        for edge in edges:
            if not isinstance(edge, dict):
                continue
            metadata = edge.get("metadata") or {}
            pdb_id = str(metadata.get("pdb_id") or "").strip().upper()
            if not pdb_id:
                continue
            row = context.setdefault(pdb_id, {})
            row["graph_context.edge_count"] = float(row.get("graph_context.edge_count", 0.0)) + 1.0
            edge_type = str(edge.get("edge_type") or "").strip()
            if edge_type:
                key = f"graph_context.edge_type.{edge_type.lower()}"
                row[key] = float(row.get(key, 0.0)) + 1.0
    return context


def _augment_examples_with_graph_context(
    examples: list[dict[str, Any]],
    graph_context_by_pdb: dict[str, dict[str, float]],
) -> None:
    for example in examples:
        structure = example.get("structure") or {}
        provenance = example.get("provenance") or {}
        pdb_id = str(structure.get("pdb_id") or provenance.get("pdb_id") or "").strip().upper()
        context = graph_context_by_pdb.get(pdb_id)
        if not context:
            continue
        features = example.setdefault("_features", {})
        if isinstance(features, dict):
            features.update(context)


def _resolve_target(examples: list[dict[str, Any]], task: str) -> tuple[str, dict[str, Any], list[dict[str, Any]]]:
    labeled: list[dict[str, Any]] = []
    metadata: dict[str, Any] = {}
    if task == "classification":
        binary_rows: list[dict[str, Any]] = []
        for example in examples:
            labels = example.get("labels") or {}
            if "is_mutant" in labels and labels.get("is_mutant") is not None:
                row = dict(example)
                row["_target"] = 1 if bool(labels.get("is_mutant")) else 0
                binary_rows.append(row)
        if binary_rows:
            return "labels.is_mutant", metadata, binary_rows
        affinity_values = [
            _safe_float((example.get("labels") or {}).get("binding_affinity_log10"))
            for example in examples
        ]
        observed = sorted(value for value in affinity_values if value is not None)
        if not observed:
            raise ValueError("No supervised label field is available for classification.")
        threshold = observed[len(observed) // 2]
        metadata["derived_threshold"] = threshold
        for example in examples:
            value = _safe_float((example.get("labels") or {}).get("binding_affinity_log10"))
            if value is None:
                continue
            row = dict(example)
            row["_target"] = 1 if value >= threshold else 0
            labeled.append(row)
        return "labels.binding_affinity_log10_median_bin", metadata, labeled

    for example in examples:
        value = _safe_float((example.get("labels") or {}).get("binding_affinity_log10"))
        if value is None:
            continue
        row = dict(example)
        row["_target"] = value
        labeled.append(row)
    if not labeled:
        raise ValueError("No regression label field is available in training_examples.json.")
    return "labels.binding_affinity_log10", metadata, labeled


def _read_split_ids(path: Path) -> set[str]:
    if not path.exists():
        return set()
    return {line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip()}


def _canonical_identifiers(example: dict[str, Any]) -> set[str]:
    identifiers: set[str] = set()
    example_id = str(example.get("example_id") or "").strip()
    if example_id:
        identifiers.add(example_id)
    structure = example.get("structure") or {}
    provenance = example.get("provenance") or {}
    pdb_id = str(structure.get("pdb_id") or provenance.get("pdb_id") or "").strip().upper()
    if pdb_id:
        identifiers.add(pdb_id)
        identifiers.add(f"RCSB_{pdb_id}")
    pair_key = str(provenance.get("pair_identity_key") or "").strip()
    if pair_key:
        identifiers.add(pair_key)
    return identifiers


def _fallback_split(example_id: str) -> str:
    digest = hashlib.md5(example_id.encode("utf-8")).hexdigest()
    bucket = int(digest[:8], 16) % 100
    if bucket < 70:
        return "train"
    if bucket < 85:
        return "val"
    return "test"


def _assign_splits(layout: StorageLayout, examples: list[dict[str, Any]]) -> tuple[dict[str, list[dict[str, Any]]], dict[str, Any]]:
    explicit = {
        "train": _read_split_ids(layout.splits_dir / "train.txt"),
        "val": _read_split_ids(layout.splits_dir / "val.txt"),
        "test": _read_split_ids(layout.splits_dir / "test.txt"),
    }
    assigned = {"train": [], "val": [], "test": []}
    matched = 0
    for example in examples:
        identifiers = _canonical_identifiers(example)
        split_name: str | None = None
        for candidate_name in ("train", "val", "test"):
            if identifiers & explicit[candidate_name]:
                split_name = candidate_name
                matched += 1
                break
        if split_name is None:
            split_name = _fallback_split(str(example.get("example_id") or "unknown"))
        assigned[split_name].append(example)

    strategy = "explicit+fallback" if matched and matched < len(examples) else ("explicit" if matched else "hash_fallback")
    if not assigned["val"] and len(assigned["train"]) > 3:
        assigned["val"].append(assigned["train"].pop())
    if not assigned["test"] and len(assigned["train"]) > 4:
        assigned["test"].append(assigned["train"].pop())
    return assigned, {
        "strategy": strategy,
        "explicit_matches": matched,
        "counts": {name: len(rows) for name, rows in assigned.items()},
    }


def _feature_schema(rows: list[dict[str, Any]]) -> tuple[list[str], list[float], list[float]]:
    feature_names = sorted({name for row in rows for name in row["_features"].keys()})
    means: list[float] = []
    stds: list[float] = []
    for feature_name in feature_names:
        values = [float(row["_features"].get(feature_name, 0.0)) for row in rows]
        mean = sum(values) / max(len(values), 1)
        variance = sum((value - mean) ** 2 for value in values) / max(len(values), 1)
        means.append(mean)
        stds.append(max(variance ** 0.5, 1e-6))
    return feature_names, means, stds


def _vectorize(rows: list[dict[str, Any]], feature_names: list[str]) -> list[list[float]]:
    return [
        [float(row["_features"].get(feature_name, 0.0)) for feature_name in feature_names]
        for row in rows
    ]


def _targets(rows: list[dict[str, Any]]) -> list[float] | list[int]:
    return [row["_target"] for row in rows]


def _regression_metrics(y_true: list[float], y_pred: list[float]) -> dict[str, float]:
    if not y_true:
        return {}
    errors = [pred - true for true, pred in zip(y_true, y_pred)]
    mse = sum(error * error for error in errors) / len(errors)
    mae = sum(abs(error) for error in errors) / len(errors)
    mean_true = sum(y_true) / len(y_true)
    total_ss = sum((value - mean_true) ** 2 for value in y_true)
    residual_ss = sum((pred - true) ** 2 for true, pred in zip(y_true, y_pred))
    r2 = 1.0 - (residual_ss / total_ss) if total_ss > 1e-9 else 0.0
    return {
        "rmse": round(math.sqrt(mse), 6),
        "mae": round(mae, 6),
        "r2": round(r2, 6),
    }


def _classification_metrics(y_true: list[int], y_pred: list[int]) -> dict[str, float]:
    if not y_true:
        return {}
    total = len(y_true)
    correct = sum(1 for truth, pred in zip(y_true, y_pred) if truth == pred)
    tp = sum(1 for truth, pred in zip(y_true, y_pred) if truth == 1 and pred == 1)
    fp = sum(1 for truth, pred in zip(y_true, y_pred) if truth == 0 and pred == 1)
    fn = sum(1 for truth, pred in zip(y_true, y_pred) if truth == 1 and pred == 0)
    precision = tp / (tp + fp) if (tp + fp) else 0.0
    recall = tp / (tp + fn) if (tp + fn) else 0.0
    f1 = (2 * precision * recall / (precision + recall)) if (precision + recall) else 0.0
    return {
        "accuracy": round(correct / total, 6),
        "precision": round(precision, 6),
        "recall": round(recall, 6),
        "f1": round(f1, 6),
    }


def _line_chart_svg(points_a: list[float], points_b: list[float], *, title: str, label_a: str, label_b: str) -> str:
    width = 720
    height = 320
    margin = 40
    points = [value for value in points_a + points_b if value is not None]
    if not points:
        points = [0.0, 1.0]
    lo = min(points)
    hi = max(points)
    if abs(hi - lo) < 1e-9:
        hi = lo + 1.0

    def _polyline(values: list[float]) -> str:
        if len(values) == 1:
            x = margin
            y = height - margin - ((values[0] - lo) / (hi - lo)) * (height - 2 * margin)
            return f"{x:.1f},{y:.1f} {x + 1:.1f},{y:.1f}"
        coords: list[str] = []
        span = max(len(values) - 1, 1)
        for idx, value in enumerate(values):
            x = margin + (idx / span) * (width - 2 * margin)
            y = height - margin - ((value - lo) / (hi - lo)) * (height - 2 * margin)
            coords.append(f"{x:.1f},{y:.1f}")
        return " ".join(coords)

    return "\n".join([
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        '<rect width="100%" height="100%" fill="#ffffff"/>',
        f'<text x="{margin}" y="24" font-size="16" font-family="Segoe UI" fill="#222222">{title}</text>',
        f'<line x1="{margin}" y1="{height - margin}" x2="{width - margin}" y2="{height - margin}" stroke="#888888" stroke-width="1"/>',
        f'<line x1="{margin}" y1="{margin}" x2="{margin}" y2="{height - margin}" stroke="#888888" stroke-width="1"/>',
        f'<polyline fill="none" stroke="#1f77b4" stroke-width="2" points="{_polyline(points_a)}"/>',
        f'<polyline fill="none" stroke="#d62728" stroke-width="2" points="{_polyline(points_b)}"/>',
        f'<text x="{width - 180}" y="28" font-size="12" font-family="Segoe UI" fill="#1f77b4">{label_a}</text>',
        f'<text x="{width - 90}" y="28" font-size="12" font-family="Segoe UI" fill="#d62728">{label_b}</text>',
        "</svg>",
    ])


def _scatter_svg(actual: list[float], predicted: list[float], *, title: str) -> str:
    width = 720
    height = 320
    margin = 40
    values = actual + predicted
    if not values:
        values = [0.0, 1.0]
    lo = min(values)
    hi = max(values)
    if abs(hi - lo) < 1e-9:
        hi = lo + 1.0

    def _scale(value: float) -> float:
        return margin + ((value - lo) / (hi - lo)) * (width - 2 * margin)

    points = []
    for truth, pred in zip(actual, predicted):
        x = _scale(truth)
        y = height - _scale(pred) + margin
        points.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="3" fill="#1f77b4" opacity="0.8"/>')
    diag_start = _scale(lo)
    diag_end = _scale(hi)
    return "\n".join([
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        '<rect width="100%" height="100%" fill="#ffffff"/>',
        f'<text x="{margin}" y="24" font-size="16" font-family="Segoe UI" fill="#222222">{title}</text>',
        f'<line x1="{margin}" y1="{height - margin}" x2="{width - margin}" y2="{height - margin}" stroke="#888888" stroke-width="1"/>',
        f'<line x1="{margin}" y1="{margin}" x2="{margin}" y2="{height - margin}" stroke="#888888" stroke-width="1"/>',
        f'<line x1="{diag_start:.1f}" y1="{height - diag_start + margin:.1f}" x2="{diag_end:.1f}" y2="{height - diag_end + margin:.1f}" stroke="#aaaaaa" stroke-dasharray="4 4"/>',
        *points,
        "</svg>",
    ])


def _classification_bar_svg(metrics: dict[str, float], *, title: str) -> str:
    width = 720
    height = 320
    margin = 40
    items = list(metrics.items())
    if not items:
        items = [("accuracy", 0.0)]
    bar_width = (width - 2 * margin) / max(len(items), 1)
    bars = []
    labels = []
    for idx, (label, value) in enumerate(items):
        x = margin + idx * bar_width + 10
        bar_h = max(0.0, min(1.0, value)) * (height - 2 * margin)
        y = height - margin - bar_h
        bars.append(f'<rect x="{x:.1f}" y="{y:.1f}" width="{bar_width - 20:.1f}" height="{bar_h:.1f}" fill="#1f77b4"/>')
        labels.append(f'<text x="{x + (bar_width - 20)/2:.1f}" y="{height - 16}" text-anchor="middle" font-size="11" font-family="Segoe UI">{label}</text>')
        labels.append(f'<text x="{x + (bar_width - 20)/2:.1f}" y="{y - 6:.1f}" text-anchor="middle" font-size="11" font-family="Segoe UI">{value:.2f}</text>')
    return "\n".join([
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        '<rect width="100%" height="100%" fill="#ffffff"/>',
        f'<text x="{margin}" y="24" font-size="16" font-family="Segoe UI" fill="#222222">{title}</text>',
        f'<line x1="{margin}" y1="{height - margin}" x2="{width - margin}" y2="{height - margin}" stroke="#888888" stroke-width="1"/>',
        *bars,
        *labels,
        "</svg>",
    ])


def _summary_progress_svg(metrics: dict[str, Any], *, task: str) -> str:
    train_metrics = metrics.get("train") if isinstance(metrics, dict) else {}
    val_metrics = metrics.get("val") if isinstance(metrics, dict) else {}
    test_metrics = metrics.get("test") if isinstance(metrics, dict) else {}
    if task == "classification":
        metric_name = "f1"
        fallback = "accuracy"
    else:
        metric_name = "rmse"
        fallback = "mae"

    def _metric_value(section: Any) -> float:
        if not isinstance(section, dict):
            return 0.0
        value = _safe_float(section.get(metric_name))
        if value is None:
            value = _safe_float(section.get(fallback))
        return float(value or 0.0)

    values = [
        _metric_value(train_metrics),
        _metric_value(val_metrics),
        _metric_value(test_metrics),
    ]
    return _line_chart_svg(
        values,
        values,
        title="Split performance summary",
        label_a=metric_name,
        label_b=metric_name,
    )


def _embedding_scatter_svg(points: list[tuple[float, float]], colors: list[int], *, title: str) -> str:
    width = 720
    height = 320
    margin = 40
    if not points:
        return _scatter_svg([0.0], [0.0], title=title)
    xs = [point[0] for point in points]
    ys = [point[1] for point in points]
    lo_x = min(xs)
    hi_x = max(xs)
    lo_y = min(ys)
    hi_y = max(ys)
    if abs(hi_x - lo_x) < 1e-9:
        hi_x = lo_x + 1.0
    if abs(hi_y - lo_y) < 1e-9:
        hi_y = lo_y + 1.0
    palette = ("#1f77b4", "#d62728", "#2ca02c", "#ff7f0e", "#9467bd", "#8c564b")
    circles: list[str] = []
    for idx, (x_value, y_value) in enumerate(points):
        x = margin + ((x_value - lo_x) / (hi_x - lo_x)) * (width - 2 * margin)
        y = height - margin - ((y_value - lo_y) / (hi_y - lo_y)) * (height - 2 * margin)
        color = palette[colors[idx] % len(palette)] if idx < len(colors) else palette[0]
        circles.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="4" fill="{color}" fill-opacity="0.75"/>')
    return "\n".join([
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        '<rect width="100%" height="100%" fill="#ffffff"/>',
        f'<text x="{margin}" y="24" font-size="16" font-family="Segoe UI" fill="#222222">{title}</text>',
        f'<line x1="{margin}" y1="{height - margin}" x2="{width - margin}" y2="{height - margin}" stroke="#888888" stroke-width="1"/>',
        f'<line x1="{margin}" y1="{margin}" x2="{margin}" y2="{height - margin}" stroke="#888888" stroke-width="1"/>',
        *circles,
        "</svg>",
    ])


def _write_predictions(path: Path, rows: list[dict[str, Any]], predictions: list[float] | list[int], split_name: str) -> None:
    export_rows: list[dict[str, Any]] = []
    for row, prediction in zip(rows, predictions):
        export_rows.append({
            "split": split_name,
            "example_id": str(row.get("example_id") or ""),
            "pdb_id": str(((row.get("structure") or {}).get("pdb_id")) or ((row.get("provenance") or {}).get("pdb_id")) or ""),
            "target": row.get("_target"),
            "prediction": prediction,
        })
    path.write_text(json.dumps(export_rows, indent=2), encoding="utf-8")


def _predict_values(model: Any, x_rows: list[list[float]], *, task: str) -> list[float]:
    if not x_rows:
        return []
    if hasattr(model, "predict"):
        return [float(value) for value in list(getattr(model, "predict")(x_rows))]
    try:
        import torch  # type: ignore
    except Exception as exc:  # pragma: no cover - only relevant if torch model is loaded without torch installed
        raise RuntimeError("A torch model was loaded but torch is not available for prediction.") from exc
    with torch.no_grad():
        input_tensor = torch.tensor(x_rows, dtype=torch.float32)
        output = model(input_tensor).detach().cpu().numpy().reshape(-1)
    if task == "classification":
        return [float(1 if value >= 0.0 else 0) for value in output]
    return [float(value) for value in output]


def _train_unsupervised_family(
    *,
    family: str,
    x_rows: list[list[float]],
    training_cfg: dict[str, Any],
) -> tuple[Any, dict[str, Any], list[dict[str, float]], list[str]]:
    try:
        from sklearn.cluster import KMeans
        from sklearn.decomposition import PCA
        from sklearn.metrics import silhouette_score
        from sklearn.preprocessing import StandardScaler
    except Exception as exc:  # pragma: no cover
        raise RuntimeError("Unsupervised local training currently requires scikit-learn.") from exc

    warnings: list[str] = []
    history: list[dict[str, float]] = []
    scaler = StandardScaler()
    x_scaled = scaler.fit_transform(x_rows)
    latent_dim = max(2, min(int(training_cfg.get("latent_dim", 8)), len(x_rows), len(x_rows[0]) if x_rows else 2))
    embedder = PCA(n_components=latent_dim, random_state=int(training_cfg.get("seed", 42)))
    embeddings = embedder.fit_transform(x_scaled)
    projection = embeddings[:, :2] if getattr(embeddings, "shape", [0, 0])[1] >= 2 else [[float(row[0]), 0.0] for row in embeddings]
    if family == "clustering":
        cluster_count = max(2, min(int(training_cfg.get("cluster_count", 4)), len(x_rows)))
        clusterer = KMeans(n_clusters=cluster_count, random_state=int(training_cfg.get("seed", 42)), n_init="auto")
        assignments = clusterer.fit_predict(embeddings)
        silhouette = None
        try:
            if len(set(int(value) for value in assignments)) > 1 and len(assignments) > cluster_count:
                silhouette = float(silhouette_score(embeddings, assignments))
        except Exception:
            silhouette = None
        metrics = {
            "silhouette": None if silhouette is None else round(silhouette, 6),
            "cluster_count": cluster_count,
            "embedding_dim": int(getattr(embeddings, "shape", [0, 0])[1] or 0),
            "explained_variance_ratio": round(float(sum(embedder.explained_variance_ratio_[: min(2, len(embedder.explained_variance_ratio_))])), 6),
        }
        return (
            {"scaler": scaler, "embedder": embedder, "clusterer": clusterer},
            {
                "backend": "sklearn_kmeans",
                "embeddings": [[float(value) for value in row[:2]] for row in projection],
                "assignments": [int(value) for value in assignments],
                "metrics": metrics,
            },
            history,
            warnings,
        )

    reconstructed = embedder.inverse_transform(embeddings)
    errors = [
        math.sqrt(sum((float(source) - float(recon)) ** 2 for source, recon in zip(source_row, recon_row)) / max(len(source_row), 1))
        for source_row, recon_row in zip(x_scaled, reconstructed)
    ]
    metrics = {
        "reconstruction_rmse": round(sum(errors) / max(len(errors), 1), 6),
        "embedding_dim": int(getattr(embeddings, "shape", [0, 0])[1] or 0),
        "explained_variance_ratio": round(float(sum(embedder.explained_variance_ratio_)), 6),
    }
    warnings.append("Autoencoder execution currently uses a PCA reconstruction baseline for unsupervised exploration.")
    return (
        {"scaler": scaler, "embedder": embedder},
        {
            "backend": "pca_autoencoder",
            "embeddings": [[float(value) for value in row[:2]] for row in projection],
            "assignments": [0 for _ in range(len(x_rows))],
            "reconstruction_error": [round(float(value), 6) for value in errors],
            "metrics": metrics,
        },
        history,
        warnings,
    )


def _materialize_run_graph_artifacts(
    layout: StorageLayout,
    *,
    run_dir: Path,
    task: str,
) -> tuple[str | None, str | None, str | None, str | None, str | None]:
    graph_sample_manifest_path: str | None = None
    graph_dataset_manifest_path: str | None = None
    pyg_ready_manifest_path: str | None = None
    graph_training_payload_manifest_path: str | None = None
    hybrid_training_payload_manifest_path: str | None = None

    graph_manifest_path = build_graph_sample_manifest(
        layout,
        output_path=run_dir / "graph_sample_manifest.json",
    )
    graph_sample_manifest_path = graph_manifest_path.name
    _records_path, dataset_manifest_path = materialize_graph_dataset_records(
        layout,
        output_dir=run_dir / "graph_dataset",
    )
    graph_dataset_manifest_path = str(dataset_manifest_path.relative_to(run_dir))
    _samples_path, pyg_manifest_path = build_pyg_ready_graph_samples(
        layout,
        output_dir=run_dir / "pyg_ready_graphs",
    )
    pyg_ready_manifest_path = str(pyg_manifest_path.relative_to(run_dir))
    _payload_path, payload_manifest_path = materialize_graph_training_payload(
        layout,
        task=task,
        output_dir=run_dir / "graph_training_payload",
    )
    graph_training_payload_manifest_path = str(payload_manifest_path.relative_to(run_dir))
    _hybrid_path, hybrid_manifest_path = materialize_hybrid_training_payload(
        layout,
        task=task,
        output_dir=run_dir / "hybrid_training_payload",
    )
    hybrid_training_payload_manifest_path = str(hybrid_manifest_path.relative_to(run_dir))
    return (
        graph_sample_manifest_path,
        graph_dataset_manifest_path,
        pyg_ready_manifest_path,
        graph_training_payload_manifest_path,
        hybrid_training_payload_manifest_path,
    )


def _build_inference_example_from_feature_record(layout: StorageLayout, pdb_id: str) -> dict[str, Any] | None:
    feature_records = _read_json(layout.features_dir / "feature_records.json")
    if not isinstance(feature_records, list):
        return None
    normalized = pdb_id.strip().upper()
    feature_row: dict[str, Any] | None = None
    for row in feature_records:
        if not isinstance(row, dict):
            continue
        if str(row.get("pdb_id") or "").strip().upper() == normalized:
            feature_row = row
            break
    if feature_row is None:
        return None
    values = feature_row.get("values") or {}
    if not isinstance(values, dict):
        values = {}
    example: dict[str, Any] = {
        "example_id": f"inference:{normalized}",
        "structure": {"pdb_id": normalized},
        "protein": {},
        "ligand": {},
        "interaction": {},
        "experiment": {},
        "graph_features": {},
        "provenance": {"pdb_id": normalized, "pair_identity_key": str(feature_row.get("pair_identity_key") or "")},
        "labels": {},
    }
    for section_name, field_name, source_key in _INFERENCE_FEATURE_VALUE_MAP:
        section = example.setdefault(section_name, {})
        if isinstance(section, dict) and source_key in values:
            section[field_name] = values.get(source_key)
    ligand = example.get("ligand") or {}
    if isinstance(ligand, dict):
        ligand["ligand_id"] = values.get("ligand_inchikey") or values.get("ligand_component_type") or ""
        ligand["ligand_type"] = values.get("ligand_component_type") or ""
    protein = example.get("protein") or {}
    if isinstance(protein, dict):
        protein["uniprot_id"] = str(feature_row.get("pair_identity_key") or "").split("|")[0] if feature_row.get("pair_identity_key") else ""
    experiment = example.get("experiment") or {}
    if isinstance(experiment, dict):
        experiment["affinity_type"] = values.get("binding_affinity_type") or ""
        experiment["source_database"] = values.get("assay_source_database") or values.get("preferred_source_database") or ""
        experiment["preferred_source_database"] = values.get("preferred_source_database") or values.get("assay_source_database") or ""
    return example


def _build_inference_example_from_raw_rcsb(layout: StorageLayout, pdb_id: str) -> dict[str, Any] | None:
    raw_path = layout.raw_rcsb_dir / f"{pdb_id.strip().upper()}.json"
    raw = _read_json(raw_path)
    if not isinstance(raw, dict):
        return None
    normalized = pdb_id.strip().upper()
    entry_info = raw.get("rcsb_entry_info") or {}
    struct = raw.get("struct") or {}
    exptl = raw.get("exptl") or []
    method = ""
    if isinstance(exptl, list) and exptl and isinstance(exptl[0], dict):
        method = str(exptl[0].get("method") or "")
    resolution = None
    if isinstance(entry_info, dict):
        combined = entry_info.get("resolution_combined")
        if isinstance(combined, list) and combined:
            resolution = combined[0]
    ligand_id = ""
    if isinstance(raw.get("nonpolymer_entities"), list):
        for entity in raw.get("nonpolymer_entities") or []:
            if not isinstance(entity, dict):
                continue
            comp = entity.get("chem_comp") or {}
            ligand_id = str(comp.get("id") or comp.get("chem_comp_id") or "").strip()
            if ligand_id:
                break
    return {
        "example_id": f"inference:{normalized}",
        "structure": {
            "pdb_id": normalized,
            "resolution": resolution,
            "atom_count_total": entry_info.get("deposited_atom_count"),
            "residue_count_observed": entry_info.get("deposited_polymer_monomer_count"),
            "title": struct.get("title"),
        },
        "protein": {
            "sequence_length": entry_info.get("deposited_polymer_monomer_count"),
        },
        "ligand": {
            "ligand_id": ligand_id,
            "ligand_type": "raw_rcsb_nonpolymer" if ligand_id else "",
        },
        "interaction": {},
        "experiment": {
            "affinity_type": "",
            "source_database": "RCSB",
            "preferred_source_database": "RCSB",
            "experimental_method": method,
        },
        "graph_features": {},
        "provenance": {"pdb_id": normalized, "pair_identity_key": f"raw:{normalized}"},
        "labels": {},
    }


def _train_sklearn_family(
    *,
    family: str,
    task: str,
    x_train: list[list[float]],
    y_train: list[float] | list[int],
    x_val: list[list[float]],
    y_val: list[float] | list[int],
    model_cfg: dict[str, Any],
    training_cfg: dict[str, Any],
) -> tuple[Any, dict[str, Any], list[dict[str, float]], list[str]]:
    from sklearn.ensemble import HistGradientBoostingClassifier, HistGradientBoostingRegressor, RandomForestClassifier, RandomForestRegressor
    from sklearn.neural_network import MLPClassifier, MLPRegressor
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import StandardScaler

    warnings: list[str] = []
    history: list[dict[str, float]] = []
    seed = int(training_cfg.get("seed", 42))

    if family == "random_forest":
        if task == "classification":
            model = RandomForestClassifier(
                n_estimators=int(model_cfg.get("n_estimators", 400)),
                max_depth=model_cfg.get("max_depth"),
                min_samples_leaf=int(model_cfg.get("min_samples_leaf", 1)),
                random_state=seed,
            )
        else:
            model = RandomForestRegressor(
                n_estimators=int(model_cfg.get("n_estimators", 400)),
                max_depth=model_cfg.get("max_depth"),
                min_samples_leaf=int(model_cfg.get("min_samples_leaf", 1)),
                random_state=seed,
            )
        model.fit(x_train, y_train)
        return model, {"backend": "sklearn_random_forest"}, history, warnings

    if family == "xgboost":
        try:
            import xgboost as xgb  # type: ignore

            if task == "classification":
                model = xgb.XGBClassifier(
                    n_estimators=int(model_cfg.get("n_estimators", 800)),
                    max_depth=int(model_cfg.get("max_depth", 8)),
                    learning_rate=float(model_cfg.get("learning_rate", 0.05)),
                    subsample=float(model_cfg.get("subsample", 0.8)),
                    colsample_bytree=float(model_cfg.get("colsample_bytree", 0.8)),
                    random_state=seed,
                    eval_metric="logloss",
                )
            else:
                model = xgb.XGBRegressor(
                    n_estimators=int(model_cfg.get("n_estimators", 800)),
                    max_depth=int(model_cfg.get("max_depth", 8)),
                    learning_rate=float(model_cfg.get("learning_rate", 0.05)),
                    subsample=float(model_cfg.get("subsample", 0.8)),
                    colsample_bytree=float(model_cfg.get("colsample_bytree", 0.8)),
                    random_state=seed,
                )
            model.fit(x_train, y_train)
            return model, {"backend": "xgboost"}, history, warnings
        except Exception:
            warnings.append("xgboost is not installed; used sklearn HistGradientBoosting fallback instead.")
            if task == "classification":
                model = HistGradientBoostingClassifier(
                    max_depth=int(model_cfg.get("max_depth", 8)),
                    learning_rate=float(model_cfg.get("learning_rate", 0.05)),
                    random_state=seed,
                )
            else:
                model = HistGradientBoostingRegressor(
                    max_depth=int(model_cfg.get("max_depth", 8)),
                    learning_rate=float(model_cfg.get("learning_rate", 0.05)),
                    random_state=seed,
                )
            model.fit(x_train, y_train)
            return model, {"backend": "sklearn_hist_gradient_boosting"}, history, warnings

    hidden_dims = tuple(int(value) for value in (model_cfg.get("hidden_dims") or [256, 128]))
    epochs = int(training_cfg.get("epochs", 60))
    learning_rate = float(training_cfg.get("learning_rate", 1e-3))
    if task == "classification":
        estimator = MLPClassifier(
            hidden_layer_sizes=hidden_dims,
            activation="relu",
            random_state=seed,
            max_iter=1,
            warm_start=True,
            learning_rate_init=learning_rate,
        )
        classes = sorted({int(value) for value in y_train})
        pipeline = Pipeline([("scaler", StandardScaler()), ("mlp", estimator)])
        best_snapshot: bytes | None = None
        best_score = -1.0
        for epoch in range(1, epochs + 1):
            pipeline.named_steps["scaler"].fit(x_train)
            x_train_scaled = pipeline.named_steps["scaler"].transform(x_train)
            x_val_scaled = pipeline.named_steps["scaler"].transform(x_val) if x_val else x_train_scaled
            estimator.partial_fit(x_train_scaled, y_train, classes=classes)
            train_pred = estimator.predict(x_train_scaled)
            val_pred = estimator.predict(x_val_scaled)
            train_metrics = _classification_metrics(list(y_train), list(train_pred))
            val_metrics = _classification_metrics(list(y_val), list(val_pred)) if y_val else train_metrics
            current = float(val_metrics.get("f1", 0.0))
            history.append({"epoch": float(epoch), "train_metric": float(train_metrics.get("f1", 0.0)), "val_metric": current})
            if current >= best_score:
                best_score = current
                best_snapshot = pickle.dumps(pipeline)
        pipeline = pickle.loads(best_snapshot) if best_snapshot is not None else pipeline
        return pipeline, {"backend": "sklearn_mlp"}, history, warnings

    estimator = MLPRegressor(
        hidden_layer_sizes=hidden_dims,
        activation="relu",
        random_state=seed,
        max_iter=1,
        warm_start=True,
        learning_rate_init=learning_rate,
    )
    pipeline = Pipeline([("scaler", StandardScaler()), ("mlp", estimator)])
    best_snapshot = None
    best_score = float("inf")
    for epoch in range(1, epochs + 1):
        pipeline.named_steps["scaler"].fit(x_train)
        x_train_scaled = pipeline.named_steps["scaler"].transform(x_train)
        x_val_scaled = pipeline.named_steps["scaler"].transform(x_val) if x_val else x_train_scaled
        estimator.partial_fit(x_train_scaled, y_train)
        train_pred = estimator.predict(x_train_scaled)
        val_pred = estimator.predict(x_val_scaled)
        train_metrics = _regression_metrics(list(y_train), list(train_pred))
        val_metrics = _regression_metrics(list(y_val), list(val_pred)) if y_val else train_metrics
        current = float(val_metrics.get("rmse", 0.0))
        history.append({"epoch": float(epoch), "train_metric": float(train_metrics.get("rmse", 0.0)), "val_metric": current})
        if current <= best_score:
            best_score = current
            best_snapshot = pickle.dumps(pipeline)
    pipeline = pickle.loads(best_snapshot) if best_snapshot is not None else pipeline
    return pipeline, {"backend": "sklearn_mlp"}, history, warnings


def _train_torch_dense_family(
    *,
    task: str,
    x_train: list[list[float]],
    y_train: list[float] | list[int],
    x_val: list[list[float]],
    y_val: list[float] | list[int],
    model_cfg: dict[str, Any],
    training_cfg: dict[str, Any],
    runtime_target: str,
) -> tuple[Any, dict[str, Any], list[dict[str, float]], list[str]]:
    try:
        import torch  # type: ignore
    except Exception as exc:  # pragma: no cover - exercised only when torch missing at runtime
        raise RuntimeError("PyTorch backend was selected but torch is not installed.") from exc

    seed = int(training_cfg.get("seed", 42))
    torch.manual_seed(seed)
    device = "cpu"
    if runtime_target == "local_gpu" and bool(getattr(torch, "cuda", None)) and bool(torch.cuda.is_available()):
        device = "cuda"
    hidden_dims = [int(value) for value in (model_cfg.get("hidden_dims") or [256, 128])]
    dropout = float(model_cfg.get("dropout", 0.2))
    epochs = int(training_cfg.get("epochs", 60))
    learning_rate = float(training_cfg.get("learning_rate", 1e-3))

    input_dim = len(x_train[0]) if x_train else 0
    layers: list[Any] = []
    prev_dim = input_dim
    for hidden_dim in hidden_dims:
        layers.append(torch.nn.Linear(prev_dim, hidden_dim))
        layers.append(torch.nn.ReLU())
        if dropout > 0:
            layers.append(torch.nn.Dropout(dropout))
        prev_dim = hidden_dim
    output_dim = 1
    layers.append(torch.nn.Linear(prev_dim, output_dim))
    model = torch.nn.Sequential(*layers).to(device)
    optimizer = torch.optim.AdamW(model.parameters(), lr=learning_rate)
    loss_fn = torch.nn.BCEWithLogitsLoss() if task == "classification" else torch.nn.MSELoss()

    x_train_t = torch.tensor(x_train, dtype=torch.float32, device=device)
    y_train_t = torch.tensor([[float(value)] for value in y_train], dtype=torch.float32, device=device)
    x_val_t = torch.tensor(x_val, dtype=torch.float32, device=device) if x_val else None
    y_val_t = torch.tensor([[float(value)] for value in y_val], dtype=torch.float32, device=device) if y_val else None
    history: list[dict[str, float]] = []
    best_score = -1.0 if task == "classification" else float("inf")
    best_state: dict[str, Any] | None = None

    for epoch in range(1, epochs + 1):
        model.train()
        optimizer.zero_grad()
        train_logits = model(x_train_t)
        loss = loss_fn(train_logits, y_train_t)
        loss.backward()
        optimizer.step()

        model.eval()
        with torch.no_grad():
            train_output = model(x_train_t).detach().cpu().numpy().reshape(-1)
            if task == "classification":
                train_pred = [1 if value >= 0.0 else 0 for value in train_output]
                train_metrics = _classification_metrics([int(value) for value in y_train], train_pred)
                if x_val_t is not None and y_val_t is not None:
                    val_output = model(x_val_t).detach().cpu().numpy().reshape(-1)
                    val_pred = [1 if value >= 0.0 else 0 for value in val_output]
                    val_metrics = _classification_metrics([int(value) for value in y_val], val_pred)
                else:
                    val_metrics = train_metrics
                current = float(val_metrics.get("f1", 0.0))
                history.append({"epoch": float(epoch), "train_metric": float(train_metrics.get("f1", 0.0)), "val_metric": current})
                if current >= best_score:
                    best_score = current
                    best_state = {key: value.detach().cpu().clone() for key, value in model.state_dict().items()}
            else:
                train_metrics = _regression_metrics([float(value) for value in y_train], [float(value) for value in train_output])
                if x_val_t is not None and y_val_t is not None:
                    val_output = model(x_val_t).detach().cpu().numpy().reshape(-1)
                    val_metrics = _regression_metrics([float(value) for value in y_val], [float(value) for value in val_output])
                else:
                    val_metrics = train_metrics
                current = float(val_metrics.get("rmse", 0.0))
                history.append({"epoch": float(epoch), "train_metric": float(train_metrics.get("rmse", 0.0)), "val_metric": current})
                if current <= best_score:
                    best_score = current
                    best_state = {key: value.detach().cpu().clone() for key, value in model.state_dict().items()}

    if best_state is not None:
        model.load_state_dict(best_state)
    return model, {"backend": "torch_tabular_mlp", "device": device}, history, []


def execute_training_run(
    layout: StorageLayout,
    *,
    starter_config: dict[str, Any],
    runtime_target: str = "local_cpu",
    run_name: str | None = None,
) -> TrainingRunResult:
    task = str(starter_config.get("task") or "regression")
    if task == "ranking":
        task = "regression"
    family = str(starter_config.get("family") or ((starter_config.get("model") or {}).get("type")) or "unknown")
    graph_contract = build_graph_learning_contract(layout)
    backend_plan = resolve_trainer_backend(
        family,
        runtime_target=runtime_target,
        native_graph_contract_available=graph_contract.available,
    )
    if backend_plan.implementation == "unsupported":
        raise ValueError(
            f"Local execution currently supports random_forest, xgboost, dense_nn, gnn, hybrid_fusion, clustering, and autoencoder. Received: {family}"
        )
    resolved_run_name = run_name or f"{family}_{_timestamp()}"
    run_dir = layout.models_dir / "model_studio" / "runs" / resolved_run_name
    run_dir.mkdir(parents=True, exist_ok=True)

    raw_examples = _read_json(layout.training_dir / "training_examples.json")
    if not isinstance(raw_examples, list) or not raw_examples:
        raise ValueError("No training examples are available under data/training_examples/training_examples.json.")

    if task == "unsupervised" or family in {"clustering", "autoencoder"}:
        examples = [dict(example) for example in raw_examples if isinstance(example, dict)]
        if not examples:
            raise ValueError("No usable training examples are available for unsupervised execution.")
        for example in examples:
            example["_features"] = _example_features(example)
        if graph_contract.available:
            _augment_examples_with_graph_context(examples, _graph_context_by_pdb(layout))
        feature_names, _means, _stds = _feature_schema(examples)
        x_rows = _vectorize(examples, feature_names)
        if not x_rows:
            raise ValueError("No vectorizable features are available for unsupervised execution.")
        model, backend_info, history, trainer_warnings = _train_unsupervised_family(
            family=family,
            x_rows=x_rows,
            training_cfg=dict(starter_config.get("training") or {}),
        )
        warnings = list(backend_plan.warnings) + list(trainer_warnings)
        functional_status = _functional_status_from_manifest({
            "backend_plan": {"implementation": backend_plan.implementation},
        })
        executed_family = _executed_family_from_backend(
            str(backend_plan.backend_id),
            str(backend_plan.execution_family or family),
        )
        metrics = {"unsupervised": dict(backend_info.get("metrics") or {})}
        resolved_config = dict(starter_config)
        resolved_config.update({
            "runtime_target": runtime_target,
            "trainer_backend": backend_plan.backend_id,
            "backend_plan": {
                "requested_family": backend_plan.requested_family,
                "execution_family": backend_plan.execution_family,
                "backend_id": backend_plan.backend_id,
                "implementation": backend_plan.implementation,
                "native_graph": backend_plan.native_graph,
            },
        })
        run_dir.mkdir(parents=True, exist_ok=True)
        (run_dir / "config.json").write_text(json.dumps(resolved_config, indent=2), encoding="utf-8")
        (run_dir / "feature_schema.json").write_text(json.dumps({
            "generated_at": _utc_now(),
            "target_name": "",
            "target_metadata": {},
            "feature_names": feature_names,
        }, indent=2), encoding="utf-8")
        (run_dir / "split_summary.json").write_text(json.dumps({
            "all": {"count": len(examples)},
        }, indent=2), encoding="utf-8")
        (run_dir / "metrics.json").write_text(json.dumps(metrics, indent=2), encoding="utf-8")
        (run_dir / "history.json").write_text(json.dumps(history, indent=2), encoding="utf-8")
        embeddings = backend_info.get("embeddings") or []
        assignments = backend_info.get("assignments") or []
        (run_dir / "embedding_records.json").write_text(json.dumps([
            {
                "example_id": str(example.get("example_id") or ""),
                "pdb_id": str(((example.get("structure") or {}).get("pdb_id")) or ((example.get("provenance") or {}).get("pdb_id")) or ""),
                "embedding": embeddings[idx] if idx < len(embeddings) else [],
                "cluster": assignments[idx] if idx < len(assignments) else 0,
            }
            for idx, example in enumerate(examples)
        ], indent=2), encoding="utf-8")
        projection_points = [
            (float(point[0]), float(point[1]))
            for point in embeddings
            if isinstance(point, list) and len(point) >= 2
        ]
        (run_dir / "test_performance.svg").write_text(
            _embedding_scatter_svg(
                projection_points,
                [int(value) for value in assignments[: len(projection_points)]],
                title="Embedding projection",
            ),
            encoding="utf-8",
        )
        with (run_dir / "model.pkl").open("wb") as handle:
            pickle.dump({
                "model": model,
                "feature_names": feature_names,
                "task": "unsupervised",
                "family": family,
                "backend_id": backend_plan.backend_id,
            }, handle)
        run_manifest = {
            "generated_at": _utc_now(),
            "run_name": resolved_run_name,
            "family": family,
            "requested_family": family,
            "executed_family": executed_family,
            "functional_status": functional_status,
            "task": "unsupervised",
            "runtime_target": runtime_target,
            "backend": backend_info.get("backend"),
            "backend_plan": {
                "requested_family": backend_plan.requested_family,
                "execution_family": backend_plan.execution_family,
                "backend_id": backend_plan.backend_id,
                "implementation": backend_plan.implementation,
                "native_graph": backend_plan.native_graph,
            },
            "storage_root": str(layout.root),
            "warnings": warnings,
            "data_readiness": {
                "raw_example_count": len(raw_examples),
                "usable_example_count": len(examples),
                "graph_contract_available": graph_contract.available,
                "graph_contract_fraction": (
                    graph_contract.matched_example_count / graph_contract.total_example_count
                    if graph_contract.total_example_count
                    else 0.0
                ),
            },
            "artifacts": {
                "config": "config.json",
                "model": "model.pkl",
                "metrics": "metrics.json",
                "feature_schema": "feature_schema.json",
                "history": "history.json",
                "embedding_records": "embedding_records.json",
                "test_performance": "test_performance.svg",
            },
        }
        (run_dir / "run_manifest.json").write_text(json.dumps(run_manifest, indent=2), encoding="utf-8")
        summary_metric = backend_info.get("metrics") or {}
        if family == "clustering":
            score_text = f"silhouette={summary_metric.get('silhouette')}"
        else:
            score_text = f"reconstruction_rmse={summary_metric.get('reconstruction_rmse')}"
        return TrainingRunResult(
            run_name=resolved_run_name,
            run_dir=run_dir,
            family=family,
            task="unsupervised",
            metrics=metrics,
            warnings=tuple(warnings),
            summary=f"{family} training run '{resolved_run_name}' finished with {score_text}.",
        )

    target_name, target_metadata, labeled_examples = _resolve_target(raw_examples, task)
    for example in labeled_examples:
        example["_features"] = _example_features(example)
    graph_context = _graph_context_by_pdb(layout)
    if family in {"gnn", "hybrid_fusion"}:
        _augment_examples_with_graph_context(labeled_examples, graph_context)
    split_rows, split_info = _assign_splits(layout, labeled_examples)
    if not split_rows["train"]:
        raise ValueError("No training rows are available after split assignment.")

    feature_names, _means, _stds = _feature_schema(split_rows["train"])
    x_train = _vectorize(split_rows["train"], feature_names)
    y_train = _targets(split_rows["train"])
    x_val = _vectorize(split_rows["val"], feature_names)
    y_val = _targets(split_rows["val"])
    x_test = _vectorize(split_rows["test"], feature_names)
    y_test = _targets(split_rows["test"])

    effective_family = backend_plan.execution_family
    warnings: list[str] = list(backend_plan.warnings)
    if str(split_info.get("strategy") or "") != "explicit":
        warnings.append("Training split assignment was not fully explicit; at least part of this run used hash fallback splits.")
    if family in {"gnn", "hybrid_fusion"} and not graph_contract.available:
        warnings.append("Requested graph-native family without a native graph learning contract; graph execution could not be first-class.")
    graph_sample_manifest_path: str | None = None
    graph_dataset_manifest_path: str | None = None
    pyg_ready_manifest_path: str | None = None
    graph_training_payload_manifest_path: str | None = None
    hybrid_training_payload_manifest_path: str | None = None

    if family in {"gnn", "hybrid_fusion"} or graph_contract.available:
        (
            graph_sample_manifest_path,
            graph_dataset_manifest_path,
            pyg_ready_manifest_path,
            graph_training_payload_manifest_path,
            hybrid_training_payload_manifest_path,
        ) = _materialize_run_graph_artifacts(
            layout,
            run_dir=run_dir,
            task=task,
        )

    if backend_plan.backend_id == "pyg_gnn":
        payload_path = run_dir / "graph_training_payload" / "graph_training_payload.json"
        model, backend_info, history, trainer_warnings = train_pyg_gnn(
            payload_path,
            runtime_target=runtime_target,
            training_cfg=dict(starter_config.get("training") or {}),
        )
    elif backend_plan.backend_id == "pyg_hybrid_fusion":
        payload_path = run_dir / "hybrid_training_payload" / "hybrid_training_payload.json"
        model, backend_info, history, trainer_warnings = train_pyg_hybrid_fusion(
            payload_path,
            runtime_target=runtime_target,
            training_cfg=dict(starter_config.get("training") or {}),
        )
    elif backend_plan.backend_id == "torch_tabular_mlp":
        model, backend_info, history, trainer_warnings = _train_torch_dense_family(
            task=task,
            x_train=x_train,
            y_train=y_train,
            x_val=x_val,
            y_val=y_val,
            model_cfg=dict(starter_config.get("model") or {}),
            training_cfg=dict(starter_config.get("training") or {}),
            runtime_target=runtime_target,
        )
    else:
        model, backend_info, history, trainer_warnings = _train_sklearn_family(
            family=effective_family,
            task=task,
            x_train=x_train,
            y_train=y_train,
            x_val=x_val,
            y_val=y_val,
            model_cfg=dict(starter_config.get("model") or {}),
            training_cfg=dict(starter_config.get("training") or {}),
        )
    warnings.extend(trainer_warnings)
    functional_status = _functional_status_from_manifest({
        "backend_plan": {"implementation": backend_plan.implementation},
    })
    executed_family = _executed_family_from_backend(
        str(backend_plan.backend_id),
        str(backend_plan.execution_family or family),
    )

    if runtime_target == "local_gpu":
        warnings.append("The initial local trainer currently uses CPU-backed sklearn/xgboost paths even when local_gpu is selected.")

    native_split_predictions = backend_info.get("split_predictions") if isinstance(backend_info, dict) else None
    if isinstance(native_split_predictions, dict):
        train_pred = [float(value) for value in ((native_split_predictions.get("train") or {}).get("pred") or [])]
        val_pred = [float(value) for value in ((native_split_predictions.get("val") or {}).get("pred") or [])]
        test_pred = [float(value) for value in ((native_split_predictions.get("test") or {}).get("pred") or [])]
        if task == "classification":
            metrics = {
                "train": _classification_metrics(
                    [int(value) for value in ((native_split_predictions.get("train") or {}).get("truth") or [])],
                    [int(round(value)) for value in train_pred],
                ),
                "val": _classification_metrics(
                    [int(value) for value in ((native_split_predictions.get("val") or {}).get("truth") or [])],
                    [int(round(value)) for value in val_pred],
                ) if val_pred else {},
                "test": _classification_metrics(
                    [int(value) for value in ((native_split_predictions.get("test") or {}).get("truth") or [])],
                    [int(round(value)) for value in test_pred],
                ) if test_pred else {},
            }
        else:
            metrics = {
                "train": _regression_metrics(
                    [float(value) for value in ((native_split_predictions.get("train") or {}).get("truth") or [])],
                    [float(value) for value in train_pred],
                ),
                "val": _regression_metrics(
                    [float(value) for value in ((native_split_predictions.get("val") or {}).get("truth") or [])],
                    [float(value) for value in val_pred],
                ) if val_pred else {},
                "test": _regression_metrics(
                    [float(value) for value in ((native_split_predictions.get("test") or {}).get("truth") or [])],
                    [float(value) for value in test_pred],
                ) if test_pred else {},
            }
    else:
        train_pred = _predict_values(model, x_train, task=task)
        val_pred = _predict_values(model, x_val, task=task) if x_val else []
        test_pred = _predict_values(model, x_test, task=task) if x_test else []

        if task == "classification":
            metrics = {
                "train": _classification_metrics(list(y_train), [int(round(value)) for value in train_pred]),
                "val": _classification_metrics(list(y_val), [int(round(value)) for value in val_pred]) if y_val else {},
                "test": _classification_metrics(list(y_test), [int(round(value)) for value in test_pred]) if y_test else {},
            }
        else:
            metrics = {
                "train": _regression_metrics([float(value) for value in y_train], [float(value) for value in train_pred]),
                "val": _regression_metrics([float(value) for value in y_val], [float(value) for value in val_pred]) if y_val else {},
                "test": _regression_metrics([float(value) for value in y_test], [float(value) for value in test_pred]) if y_test else {},
            }

    resolved_config = dict(starter_config)
    resolved_config.update({
        "runtime_target": runtime_target,
        "trainer_backend": backend_plan.backend_id,
        "backend_plan": {
            "requested_family": backend_plan.requested_family,
            "execution_family": backend_plan.execution_family,
            "backend_id": backend_plan.backend_id,
            "implementation": backend_plan.implementation,
            "native_graph": backend_plan.native_graph,
        },
    })
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "config.json").write_text(json.dumps(resolved_config, indent=2), encoding="utf-8")
    (run_dir / "feature_schema.json").write_text(json.dumps({
        "generated_at": _utc_now(),
        "target_name": target_name,
        "target_metadata": target_metadata,
        "feature_names": feature_names,
    }, indent=2), encoding="utf-8")
    (run_dir / "split_summary.json").write_text(json.dumps(split_info, indent=2), encoding="utf-8")
    (run_dir / "metrics.json").write_text(json.dumps(metrics, indent=2), encoding="utf-8")
    (run_dir / "history.json").write_text(json.dumps(history, indent=2), encoding="utf-8")
    with (run_dir / "model.pkl").open("wb") as handle:
        pickle.dump({
            "model": model,
            "feature_names": feature_names,
            "task": task,
            "family": family,
            "target_name": target_name,
            "target_metadata": target_metadata,
            "backend_id": backend_plan.backend_id,
        }, handle)

    _write_predictions(run_dir / "train_predictions.json", split_rows["train"], train_pred, "train")
    _write_predictions(run_dir / "val_predictions.json", split_rows["val"], val_pred, "val")
    _write_predictions(run_dir / "test_predictions.json", split_rows["test"], test_pred, "test")

    training_curve_svg = (
        _line_chart_svg(
            [float(row["train_metric"]) for row in history],
            [float(row["val_metric"]) for row in history],
            title="Training vs validation progress",
            label_a="train",
            label_b="val",
        )
        if history
        else _summary_progress_svg(metrics, task=task)
    )
    (run_dir / "training_curve.svg").write_text(training_curve_svg, encoding="utf-8")
    if task == "classification":
        (run_dir / "test_performance.svg").write_text(
            _classification_bar_svg(metrics.get("test") or {}, title="Test-set classification metrics"),
            encoding="utf-8",
        )
    else:
        (run_dir / "test_performance.svg").write_text(
            _scatter_svg([float(value) for value in y_test], [float(value) for value in test_pred], title="Test-set actual vs predicted"),
            encoding="utf-8",
        )

    run_manifest = {
        "generated_at": _utc_now(),
        "run_name": resolved_run_name,
        "family": family,
        "requested_family": family,
        "executed_family": executed_family,
        "functional_status": functional_status,
        "task": task,
        "runtime_target": runtime_target,
        "backend": backend_info.get("backend"),
        "backend_plan": {
            "requested_family": backend_plan.requested_family,
            "execution_family": backend_plan.execution_family,
            "backend_id": backend_plan.backend_id,
            "implementation": backend_plan.implementation,
            "native_graph": backend_plan.native_graph,
        },
        "graph_contract": {
            "available": graph_contract.available,
            "matched_example_count": graph_contract.matched_example_count,
            "total_example_count": graph_contract.total_example_count,
            "summary": graph_contract.summary,
            "sample_pdb_ids": list(graph_contract.sample_pdb_ids),
        },
        "storage_root": str(layout.root),
        "target_name": target_name,
        "warnings": warnings,
        "data_readiness": {
            "raw_example_count": len(raw_examples),
            "labeled_example_count": len(labeled_examples),
            "split_strategy": split_info.get("strategy"),
            "explicit_split_matches": int(split_info.get("explicit_matches") or 0),
            "split_counts": split_info.get("counts") or {},
            "graph_contract_available": graph_contract.available,
            "graph_contract_fraction": (
                graph_contract.matched_example_count / graph_contract.total_example_count
                if graph_contract.total_example_count
                else 0.0
            ),
        },
        "evaluation_summary": {
            "primary_metric_name": "f1" if task == "classification" else "rmse",
            "primary_metric_value": (
                float((metrics.get("test") or metrics.get("val") or metrics.get("train") or {}).get("f1", 0.0))
                if task == "classification"
                else float((metrics.get("test") or metrics.get("val") or metrics.get("train") or {}).get("rmse", 0.0))
            ),
            "train_prediction_count": len(train_pred),
            "val_prediction_count": len(val_pred),
            "test_prediction_count": len(test_pred),
        },
        "artifacts": {
            "config": "config.json",
            "model": "model.pkl",
            "metrics": "metrics.json",
            "feature_schema": "feature_schema.json",
            "split_summary": "split_summary.json",
            "history": "history.json",
            "graph_sample_manifest": graph_sample_manifest_path,
            "graph_dataset_manifest": graph_dataset_manifest_path,
            "pyg_ready_manifest": pyg_ready_manifest_path,
            "graph_training_payload_manifest": graph_training_payload_manifest_path,
            "hybrid_training_payload_manifest": hybrid_training_payload_manifest_path,
            "training_curve": "training_curve.svg",
            "test_performance": "test_performance.svg",
            "train_predictions": "train_predictions.json",
            "val_predictions": "val_predictions.json",
            "test_predictions": "test_predictions.json",
        },
    }
    (run_dir / "run_manifest.json").write_text(json.dumps(run_manifest, indent=2), encoding="utf-8")

    test_summary = metrics.get("test") or metrics.get("val") or metrics.get("train") or {}
    if task == "classification":
        score_text = f"test_f1={test_summary.get('f1', 0.0):.3f}"
    else:
        score_text = f"test_rmse={test_summary.get('rmse', 0.0):.3f}"
    summary = f"{family} training run '{resolved_run_name}' finished with {score_text}."
    return TrainingRunResult(
        run_name=resolved_run_name,
        run_dir=run_dir,
        family=family,
        task=task,
        metrics=metrics,
        warnings=tuple(warnings),
        summary=summary,
    )


def import_training_run(
    layout: StorageLayout,
    *,
    source_dir: str | Path,
    run_name: str | None = None,
) -> Path:
    source_path = Path(source_dir).resolve()
    if not source_path.exists():
        raise FileNotFoundError(f"Training run source directory does not exist: {source_path}")

    import_manifest = _read_json(source_path / "import_run.json")
    payload_root = _find_import_payload_root(source_path, import_manifest if isinstance(import_manifest, dict) else None)

    model_candidates = [
        payload_root / "model.pkl",
        payload_root / "model.pt",
        payload_root / "model.json",
        payload_root / "checkpoint.pt",
        payload_root / "metrics.json",
        payload_root / "run_metrics.json",
    ]
    if not any(path.exists() for path in model_candidates):
        raise ValueError(f"No recognizable trained-model artifacts were found in {payload_root}")

    destination_name = run_name or source_path.name or f"imported_run_{_timestamp()}"
    dest_dir = layout.models_dir / "imported_runs" / destination_name
    if dest_dir.exists():
        dest_dir = layout.models_dir / "imported_runs" / f"{destination_name}_{_timestamp()}"
    shutil.copytree(payload_root, dest_dir)
    copied_context_files: list[str] = []
    if source_path != payload_root:
        for extra_name in (
            "import_run.json",
            "config.json",
            "requirements.txt",
            "README.md",
            "environment.json",
            "runtime_targets.json",
        ):
            extra_src = source_path / extra_name
            if extra_src.exists():
                shutil.copy2(extra_src, dest_dir / extra_name)
                copied_context_files.append(extra_name)
    normalized_metrics = _normalize_import_metrics(dest_dir)
    normalized_artifacts = _normalize_import_artifacts(dest_dir)
    normalized_manifest = _normalize_import_manifest(
        dest_dir,
        import_manifest=import_manifest if isinstance(import_manifest, dict) else None,
        normalized_metrics=normalized_metrics,
        normalized_artifacts=normalized_artifacts,
    )
    (dest_dir / "import_manifest.json").write_text(json.dumps({
        "imported_at": _utc_now(),
        "source_dir": str(source_path),
        "payload_root": str(payload_root),
        "copied_context_files": copied_context_files,
        "detected_runtime_target": str(normalized_manifest.get("runtime_target") or "unknown"),
        "files": sorted(str(path.relative_to(dest_dir)) for path in dest_dir.rglob("*") if path.is_file()),
    }, indent=2), encoding="utf-8")
    return dest_dir


def compare_training_runs(layout: StorageLayout) -> list[RunComparison]:
    comparisons: list[RunComparison] = []
    locations = [
        ("local", layout.models_dir / "model_studio" / "runs"),
        ("imported", layout.models_dir / "imported_runs"),
    ]
    for source_name, root in locations:
        if not root.exists():
            continue
        for run_dir in sorted((path for path in root.iterdir() if path.is_dir()), key=lambda item: item.name):
            manifest = _read_json(run_dir / "run_manifest.json") or {}
            metrics = _read_json(run_dir / "metrics.json") or _read_json(run_dir / "run_metrics.json") or {}
            requested_family = _requested_family_from_manifest(manifest, metrics if isinstance(metrics, dict) else {})
            executed_family = _executed_family_from_manifest(manifest, metrics if isinstance(metrics, dict) else {})
            family = executed_family
            task = str(manifest.get("task") or metrics.get("task") or "unknown")
            backend_plan = manifest.get("backend_plan") or {}
            backend_id = str(backend_plan.get("backend_id") or manifest.get("backend") or metrics.get("backend") or "unknown")
            functional_status = _functional_status_from_manifest(manifest)
            primary_name, primary_value = _extract_primary_metric(metrics)
            if primary_value is None:
                summary = f"{run_dir.name}: metrics not yet normalized for comparison."
            else:
                summary = (
                    f"{run_dir.name}: requested={requested_family}, executed={executed_family}, "
                    f"status={functional_status}, {task} {primary_name}={primary_value:.3f}"
                )
            comparisons.append(RunComparison(
                run_name=run_dir.name,
                location=run_dir,
                family=family,
                requested_family=requested_family,
                executed_family=executed_family,
                task=task,
                source=source_name,
                functional_status=functional_status,
                backend_id=backend_id,
                primary_metric_name=primary_name,
                primary_metric_value=primary_value,
                summary=summary,
            ))

    def _sort_key(item: RunComparison) -> tuple[int, float, str]:
        if item.primary_metric_value is None:
            return (2, float("inf"), item.run_name)
        if item.primary_metric_name in {"rmse", "reconstruction_rmse"}:
            return (0, item.primary_metric_value, item.run_name)
        return (1, -item.primary_metric_value, item.run_name)

    return sorted(comparisons, key=_sort_key)


def inspect_training_run(run_dir: str | Path, *, source: str = "unknown") -> RunInspection:
    resolved_run_dir = Path(run_dir).resolve()
    manifest = _read_json(resolved_run_dir / "run_manifest.json") or {}
    metrics = _read_json(resolved_run_dir / "metrics.json") or _read_json(resolved_run_dir / "run_metrics.json") or {}
    history = _read_json(resolved_run_dir / "history.json") or []
    split_summary = _read_json(resolved_run_dir / "split_summary.json") or {}
    requested_family = _requested_family_from_manifest(manifest, metrics if isinstance(metrics, dict) else {})
    executed_family = _executed_family_from_manifest(manifest, metrics if isinstance(metrics, dict) else {})
    family = executed_family
    task = str(manifest.get("task") or metrics.get("task") or "unknown")
    runtime_target = str(manifest.get("runtime_target") or "unknown")
    backend_plan = manifest.get("backend_plan") or {}
    backend_id = str(backend_plan.get("backend_id") or manifest.get("backend") or "unknown")
    functional_status = _functional_status_from_manifest(manifest)
    primary_metric_name, primary_metric_value = _extract_primary_metric(metrics)
    epoch_count = len(history) if isinstance(history, list) else 0
    history_summary: dict[str, Any] = {"epoch_count": epoch_count}
    if isinstance(history, list) and history:
        last_row = history[-1] if isinstance(history[-1], dict) else {}
        history_summary.update({
            "final_epoch": int(last_row.get("epoch") or epoch_count),
            "final_train_metric": _safe_float(last_row.get("train_metric")),
            "final_val_metric": _safe_float(last_row.get("val_metric")),
        })
        val_candidates = [
            _safe_float(row.get("val_metric"))
            for row in history
            if isinstance(row, dict) and _safe_float(row.get("val_metric")) is not None
        ]
        if val_candidates:
            if task == "regression":
                history_summary["best_val_metric"] = min(val_candidates)
            else:
                history_summary["best_val_metric"] = max(val_candidates)
    split_counts = _extract_split_counts(split_summary)
    training_curve_path = resolved_run_dir / "training_curve.svg"
    test_performance_path = resolved_run_dir / "test_performance.svg"
    train_predictions_path = resolved_run_dir / "train_predictions.json"
    val_predictions_path = resolved_run_dir / "val_predictions.json"
    test_predictions_path = resolved_run_dir / "test_predictions.json"
    import_manifest_path = resolved_run_dir / "import_manifest.json"
    artifacts = {
        "training_curve": str(training_curve_path) if training_curve_path.exists() else "",
        "test_performance": str(test_performance_path) if test_performance_path.exists() else "",
        "metrics": str((resolved_run_dir / "metrics.json").resolve()) if (resolved_run_dir / "metrics.json").exists() else "",
        "split_summary": str((resolved_run_dir / "split_summary.json").resolve()) if (resolved_run_dir / "split_summary.json").exists() else "",
        "history": str((resolved_run_dir / "history.json").resolve()) if (resolved_run_dir / "history.json").exists() else "",
        "train_predictions": str(train_predictions_path.resolve()) if train_predictions_path.exists() else "",
        "val_predictions": str(val_predictions_path.resolve()) if val_predictions_path.exists() else "",
        "test_predictions": str(test_predictions_path.resolve()) if test_predictions_path.exists() else "",
        "run_manifest": str((resolved_run_dir / "run_manifest.json").resolve()) if (resolved_run_dir / "run_manifest.json").exists() else "",
        "import_manifest": str(import_manifest_path.resolve()) if import_manifest_path.exists() else "",
    }
    train_predictions = _read_json(train_predictions_path)
    val_predictions = _read_json(val_predictions_path)
    test_predictions = _read_json(test_predictions_path)
    artifacts["artifact_count"] = str(sum(1 for value in artifacts.values() if value))
    if isinstance(train_predictions, list):
        artifacts["train_prediction_count"] = str(len(train_predictions))
    if isinstance(val_predictions, list):
        artifacts["val_prediction_count"] = str(len(val_predictions))
    if isinstance(test_predictions, list):
        artifacts["test_prediction_count"] = str(len(test_predictions))
    warnings = tuple(str(item) for item in (manifest.get("warnings") or []) if str(item).strip())
    return RunInspection(
        run_name=resolved_run_dir.name,
        location=resolved_run_dir,
        family=family,
        requested_family=requested_family,
        executed_family=executed_family,
        task=task,
        source=source,
        runtime_target=runtime_target,
        functional_status=functional_status,
        backend_id=backend_id,
        primary_metric_name=primary_metric_name,
        primary_metric_value=primary_metric_value,
        epoch_count=epoch_count,
        chart_ready=training_curve_path.exists(),
        test_plot_ready=test_performance_path.exists(),
        split_counts=split_counts,
        metrics=metrics if isinstance(metrics, dict) else {},
        history_summary=history_summary,
        warnings=warnings,
        artifacts=artifacts,
    )


def build_training_run_report(layout: StorageLayout) -> dict[str, Any]:
    comparisons = compare_training_runs(layout)
    inspections: list[RunInspection] = [
        inspect_training_run(item.location, source=item.source)
        for item in comparisons
    ]
    family_best: dict[str, RunInspection] = {}
    for inspection in inspections:
        current = family_best.get(inspection.family)
        if current is None:
            family_best[inspection.family] = inspection
            continue
        if inspection.primary_metric_value is None:
            continue
        if current.primary_metric_value is None:
            family_best[inspection.family] = inspection
            continue
        better = (
            inspection.primary_metric_value < current.primary_metric_value
            if inspection.primary_metric_name in {"rmse", "reconstruction_rmse"}
            else inspection.primary_metric_value > current.primary_metric_value
        )
        if better:
            family_best[inspection.family] = inspection
    best_overall = inspections[0] if inspections else None
    return {
        "generated_at": _utc_now(),
        "run_count": len(inspections),
        "chart_ready_count": sum(1 for item in inspections if item.chart_ready),
        "test_plot_ready_count": sum(1 for item in inspections if item.test_plot_ready),
        "native_graph_run_count": sum(1 for item in inspections if item.backend_id in {"pyg_gnn", "pyg_hybrid_fusion"}),
        "non_first_class_run_count": sum(1 for item in inspections if item.functional_status != "first_class"),
        "best_overall": None if best_overall is None else {
            "run_name": best_overall.run_name,
            "family": best_overall.family,
            "requested_family": best_overall.requested_family,
            "executed_family": best_overall.executed_family,
            "task": best_overall.task,
            "backend_id": best_overall.backend_id,
            "functional_status": best_overall.functional_status,
            "primary_metric_name": best_overall.primary_metric_name,
            "primary_metric_value": best_overall.primary_metric_value,
            "location": str(best_overall.location),
        },
        "best_by_family": [
            {
                "family": family,
                "run_name": inspection.run_name,
                "requested_family": inspection.requested_family,
                "executed_family": inspection.executed_family,
                "backend_id": inspection.backend_id,
                "functional_status": inspection.functional_status,
                "primary_metric_name": inspection.primary_metric_name,
                "primary_metric_value": inspection.primary_metric_value,
                "location": str(inspection.location),
            }
            for family, inspection in sorted(family_best.items())
        ],
        "recent_runs": [
            {
                "run_name": inspection.run_name,
                "family": inspection.family,
                "requested_family": inspection.requested_family,
                "executed_family": inspection.executed_family,
                "task": inspection.task,
                "source": inspection.source,
                "runtime_target": inspection.runtime_target,
                "backend_id": inspection.backend_id,
                "functional_status": inspection.functional_status,
                "primary_metric_name": inspection.primary_metric_name,
                "primary_metric_value": inspection.primary_metric_value,
                "epoch_count": inspection.epoch_count,
                "chart_ready": inspection.chart_ready,
                "test_plot_ready": inspection.test_plot_ready,
                "split_counts": inspection.split_counts,
                "history_summary": inspection.history_summary,
                "artifacts": inspection.artifacts,
                "warnings": list(inspection.warnings),
                "location": str(inspection.location),
            }
            for inspection in inspections[:5]
        ],
    }


def _predict_native_graph_inference(
    layout: StorageLayout,
    *,
    run_dir: Path,
    model: Any,
    backend_id: str,
    pdb_id: str,
    feature_names: list[str],
    selected_example: dict[str, Any],
) -> float:
    try:
        import torch  # type: ignore
        from torch_geometric.data import Data  # type: ignore
    except Exception as exc:  # pragma: no cover
        raise RuntimeError("Native graph inference requires torch and torch_geometric.") from exc

    reference_samples = _read_json(run_dir / "pyg_ready_graphs" / "pyg_ready_graph_samples.json")
    if not isinstance(reference_samples, list) or not reference_samples:
        raise ValueError(f"No PyG-ready graph reference samples are available under {run_dir}.")
    node_feature_keys = tuple(
        str(key).strip()
        for key in (reference_samples[0].get("node_feature_keys") or [])
        if str(key).strip()
    )
    edge_feature_keys = tuple(
        str(key).strip()
        for key in (reference_samples[0].get("edge_feature_keys") or [])
        if str(key).strip()
    )
    nodes = _read_json(layout.graph_dir / "graph_nodes.json")
    edges = _read_json(layout.graph_dir / "graph_edges.json")
    if not isinstance(nodes, list):
        nodes = []
    if not isinstance(edges, list):
        edges = []
    normalized = pdb_id.strip().upper()
    matched_nodes = [
        row for row in nodes
        if isinstance(row, dict) and str((row.get("metadata") or {}).get("pdb_id") or row.get("primary_id") or "").strip().upper() == normalized
    ]
    if not matched_nodes:
        raise ValueError(f"No graph nodes are available in the workspace for PDB ID {normalized}.")
    node_ids = [str(row.get("node_id") or "") for row in matched_nodes if str(row.get("node_id") or "").strip()]
    node_index = {node_id: idx for idx, node_id in enumerate(node_ids)}
    x_rows = []
    for row in matched_nodes:
        features = row.get("features") or {}
        if not isinstance(features, dict):
            features = {}
        x_rows.append([float(features.get(name, 0.0) or 0.0) for name in node_feature_keys])
    edge_pairs: list[tuple[int, int]] = []
    edge_rows = []
    for row in edges:
        if not isinstance(row, dict):
            continue
        candidate_pdb = str((row.get("metadata") or {}).get("pdb_id") or "").strip().upper()
        if candidate_pdb != normalized:
            continue
        source_id = str(row.get("source_node_id") or row.get("source") or "").strip()
        target_id = str(row.get("target_node_id") or row.get("target") or "").strip()
        if source_id not in node_index or target_id not in node_index:
            continue
        edge_pairs.append((node_index[source_id], node_index[target_id]))
        edge_features = row.get("features") or {}
        if not isinstance(edge_features, dict):
            edge_features = {}
        edge_rows.append([float(edge_features.get(name, 0.0) or 0.0) for name in edge_feature_keys])
    x_tensor = torch.tensor(x_rows, dtype=torch.float32)
    edge_index = torch.tensor(edge_pairs, dtype=torch.long).t().contiguous() if edge_pairs else torch.empty((2, 0), dtype=torch.long)
    edge_attr = torch.tensor(edge_rows, dtype=torch.float32) if edge_rows else torch.empty((0, len(edge_feature_keys)), dtype=torch.float32)
    graph_item = Data(x=x_tensor, edge_index=edge_index, edge_attr=edge_attr)
    graph_item.batch = torch.zeros(graph_item.x.shape[0], dtype=torch.long)
    if backend_id == "pyg_hybrid_fusion":
        selected_example["_features"] = _example_features(selected_example)
        _augment_examples_with_graph_context([selected_example], _graph_context_by_pdb(layout))
        graph_item.attr_x = torch.tensor(
            [[float(selected_example["_features"].get(feature_name, 0.0)) for feature_name in feature_names]],
            dtype=torch.float32,
        )
    model.eval()
    with torch.no_grad():
        output = model(graph_item).detach().cpu().numpy().reshape(-1)
    return float(output[0])


def run_saved_model_inference(
    layout: StorageLayout,
    *,
    run_dir: str | Path,
    pdb_id: str,
) -> dict[str, Any]:
    resolved_run_dir = Path(run_dir).resolve()
    model_payload_path = resolved_run_dir / "model.pkl"
    if not model_payload_path.exists():
        raise FileNotFoundError(f"No model.pkl found in {resolved_run_dir}")
    with model_payload_path.open("rb") as handle:
        payload = pickle.load(handle)
    model = payload.get("model")
    feature_names = list(payload.get("feature_names") or [])
    task = str(payload.get("task") or "regression")
    backend_id = str(payload.get("backend_id") or "")
    if task == "unsupervised":
        raise ValueError("Saved-model inference is not yet defined for unsupervised runs; inspect the embedding artifacts instead.")
    examples = _read_json(layout.training_dir / "training_examples.json")
    if not isinstance(examples, list):
        examples = []
    normalized_pdb_id = str(pdb_id or "").strip().upper()
    selected: dict[str, Any] | None = None
    for example in examples:
        structure = example.get("structure") or {}
        provenance = example.get("provenance") or {}
        candidate_pdb = str(structure.get("pdb_id") or provenance.get("pdb_id") or "").strip().upper()
        if candidate_pdb == normalized_pdb_id:
            selected = example
            break
    if selected is None:
        selected = _build_inference_example_from_feature_record(layout, normalized_pdb_id)
    if selected is None:
        selected = _build_inference_example_from_raw_rcsb(layout, normalized_pdb_id)
    if selected is None:
        raise ValueError(f"No training example, feature record, or cached raw RCSB entry in the current workspace matches PDB ID {normalized_pdb_id}.")
    if backend_id in {"pyg_gnn", "pyg_hybrid_fusion"}:
        prediction = _predict_native_graph_inference(
            layout,
            run_dir=resolved_run_dir,
            model=model,
            backend_id=backend_id,
            pdb_id=normalized_pdb_id,
            feature_names=feature_names,
            selected_example=selected,
        )
    else:
        selected["_features"] = _example_features(selected)
        _augment_examples_with_graph_context([selected], _graph_context_by_pdb(layout))
        vector = [[float(selected["_features"].get(feature_name, 0.0)) for feature_name in feature_names]]
        prediction = _predict_values(model, vector, task=task)[0]
    labels = selected.get("labels") or {}
    truth = labels.get("binding_affinity_log10")
    if task == "classification":
        prediction = int(round(float(prediction)))
    else:
        prediction = round(float(prediction), 6)
    return {
        "run_dir": str(resolved_run_dir),
        "pdb_id": normalized_pdb_id,
        "task": task,
        "prediction": prediction,
        "ground_truth": truth,
        "example_id": str(selected.get("example_id") or ""),
    }


def run_saved_model_batch_inference(
    layout: StorageLayout,
    *,
    run_dir: str | Path,
    pdb_ids: list[str],
) -> dict[str, Any]:
    resolved_run_dir = Path(run_dir).resolve()
    normalized_ids = [str(pdb_id or "").strip().upper() for pdb_id in pdb_ids if str(pdb_id or "").strip()]
    if not normalized_ids:
        raise ValueError("At least one non-empty PDB ID is required for batch inference.")
    results: list[dict[str, Any]] = []
    failures: list[dict[str, str]] = []
    for pdb_id in normalized_ids:
        try:
            results.append(run_saved_model_inference(layout, run_dir=resolved_run_dir, pdb_id=pdb_id))
        except Exception as exc:
            failures.append({"pdb_id": pdb_id, "error": str(exc)})
    batch_dir = resolved_run_dir / "batch_inference"
    batch_dir.mkdir(parents=True, exist_ok=True)
    batch_name = f"batch_inference_{_timestamp()}.json"
    batch_payload = {
        "generated_at": _utc_now(),
        "run_dir": str(resolved_run_dir),
        "requested_count": len(normalized_ids),
        "success_count": len(results),
        "failure_count": len(failures),
        "requested_pdb_ids": normalized_ids,
        "results": results,
        "failures": failures,
    }
    batch_path = batch_dir / batch_name
    batch_path.write_text(json.dumps(batch_payload, indent=2), encoding="utf-8")
    csv_path = batch_dir / batch_name.replace(".json", ".csv")
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=("pdb_id", "status", "prediction", "ground_truth", "example_id", "error"),
        )
        writer.writeheader()
        for item in results:
            writer.writerow({
                "pdb_id": item.get("pdb_id", ""),
                "status": "ok",
                "prediction": item.get("prediction", ""),
                "ground_truth": item.get("ground_truth", ""),
                "example_id": item.get("example_id", ""),
                "error": "",
            })
        for item in failures:
            writer.writerow({
                "pdb_id": item.get("pdb_id", ""),
                "status": "error",
                "prediction": "",
                "ground_truth": "",
                "example_id": "",
                "error": item.get("error", ""),
            })
    batch_payload["artifact_path"] = str(batch_path)
    batch_payload["artifact_csv_path"] = str(csv_path)
    return batch_payload
