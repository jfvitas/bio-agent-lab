"""Dataset engineering for diverse, leakage-aware ML splits.

Assumptions:
- If ESM is unavailable, this module falls back to deterministic sequence and
  metadata embeddings so the workflow remains dependency-light and reproducible.
- Hard leakage groups take precedence over diversity optimization.
- Diversity is approximated from available metadata columns rather than assumed
  to be complete for every record.
"""

from __future__ import annotations

import csv
import hashlib
import json
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pbdata.data_pipeline.workflow_engine import write_dataset_export_configs
from pbdata.storage import StorageLayout

_ESM_CACHE_DIR: Path | None = None


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(frozen=True)
class DatasetEngineeringConfig:
    dataset_name: str = "engineered_dataset"
    test_frac: float = 0.20
    cv_folds: int = 0
    strict_family_isolation: bool = False
    embedding_backend: str = "auto"
    cluster_count: int = 8
    seed: int = 42


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _safe_text(value: object) -> str:
    return str(value or "").strip()


def _sequence_embedding(sequence: str, *, dims: int = 32) -> list[float]:
    if not sequence:
        return [0.0] * dims
    sequence = sequence.upper()
    alphabet = "ACDEFGHIKLMNPQRSTVWY"
    counts = [sequence.count(letter) / max(len(sequence), 1) for letter in alphabet]
    kmer_bins = [0.0] * (dims - len(counts))
    for i in range(max(len(sequence) - 2, 0)):
        digest = hashlib.md5(sequence[i : i + 3].encode("utf-8")).digest()
        kmer_bins[digest[0] % len(kmer_bins)] += 1.0
    total = sum(kmer_bins) or 1.0
    kmer_bins = [value / total for value in kmer_bins]
    return counts + kmer_bins


def _metadata_embedding(row: dict[str, str], *, dims: int = 8) -> list[float]:
    fields = [
        _safe_text(row.get("organism")),
        _safe_text(row.get("protein_family")),
        _safe_text(row.get("binding_interface_type")),
        _safe_text(row.get("ligand_class")),
    ]
    values = [0.0] * dims
    for field in fields:
        if not field:
            continue
        digest = hashlib.md5(field.lower().encode("utf-8")).digest()
        values[digest[0] % dims] += 1.0
    scale = sum(values) or 1.0
    return [value / scale for value in values]


def _embedding_backend_name(requested: str) -> str:
    if requested not in {"auto", "esm", "fallback"}:
        raise ValueError("embedding_backend must be auto, esm, or fallback")
    if requested == "fallback":
        return "fallback"
    try:
        __import__("esm")
        return "esm"
    except Exception:
        return "fallback" if requested == "auto" else "esm_unavailable"


def _row_embedding(row: dict[str, str], *, backend: str) -> list[float]:
    sequence = _safe_text(row.get("sequence"))
    if backend == "esm":
        embedded = _esm_embedding(sequence)
        if embedded is not None:
            return embedded + _metadata_embedding(row)
    return _sequence_embedding(sequence) + _metadata_embedding(row)


def _esm_cache_path(sequence: str) -> Path:
    digest = hashlib.md5(sequence.encode("utf-8")).hexdigest()
    base_dir = _ESM_CACHE_DIR or (Path(".") / ".esm_cache")
    return base_dir / f"{digest}.json"


def _esm_embedding(sequence: str, *, dims: int = 32) -> list[float] | None:
    if not sequence:
        return [0.0] * dims
    cache_path = _esm_cache_path(sequence)
    if cache_path.exists():
        try:
            raw = json.loads(cache_path.read_text(encoding="utf-8"))
            values = raw.get("embedding")
            if isinstance(values, list) and len(values) == dims:
                return [float(v) for v in values]
        except Exception:
            pass
    try:
        import esm  # type: ignore
        import torch as _torch
    except Exception:
        return None
    try:
        model, alphabet = esm.pretrained.esm2_t6_8M_UR50D()
        model.eval()
        batch_converter = alphabet.get_batch_converter()
        _, _, tokens = batch_converter([("sequence", sequence)])
        with _torch.no_grad():
            outputs = model(tokens, repr_layers=[6], return_contacts=False)
        representations = outputs["representations"][6][0, 1 : len(sequence) + 1]
        vector = representations.mean(dim=0).tolist()[:dims]
        if len(vector) < dims:
            vector.extend([0.0] * (dims - len(vector)))
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_text(json.dumps({"embedding": vector}), encoding="utf-8")
        return [float(v) for v in vector]
    except Exception:
        return None


def _squared_distance(left: list[float], right: list[float]) -> float:
    return sum((lval - rval) ** 2 for lval, rval in zip(left, right))


def _mean_vector(rows: list[list[float]]) -> list[float]:
    if not rows:
        return []
    dims = len(rows[0])
    return [sum(row[idx] for row in rows) / len(rows) for idx in range(dims)]


def _kmeans(vectors: list[list[float]], *, k: int, seed: int) -> list[int]:
    if not vectors:
        return []
    k = max(1, min(k, len(vectors)))
    start_digest = hashlib.md5(f"kmeans:{seed}:{len(vectors)}".encode("utf-8")).hexdigest()
    start = int(start_digest[:8], 16) % len(vectors)
    centroids = [list(vectors[(start + idx) % len(vectors)]) for idx in range(k)]
    assignments = [0] * len(vectors)
    for _ in range(12):
        for idx, vector in enumerate(vectors):
            distances = [_squared_distance(vector, centroid) for centroid in centroids]
            assignments[idx] = min(range(len(distances)), key=lambda item: distances[item])
        for idx in range(k):
            cluster_rows = [vector for vector, assignment in zip(vectors, assignments) if assignment == idx]
            if cluster_rows:
                centroids[idx] = _mean_vector(cluster_rows)
    return assignments


def _hard_group_key(row: dict[str, str], *, strict_family_isolation: bool) -> str:
    pair_key = _safe_text(row.get("pair_identity_key"))
    family = _safe_text(row.get("protein_family"))
    mutation = _safe_text(row.get("mutation_strings")) or "wt"
    if strict_family_isolation and family:
        return f"family:{family}|mutation:{mutation}"
    if pair_key:
        pair_family = pair_key.split("|", 4)
        prefix = "|".join(pair_family[:4]) if len(pair_family) >= 4 else pair_key
        return f"pair:{prefix}|mutation:{mutation}"
    return f"row:{_safe_text(row.get('pdb_id'))}|mutation:{mutation}"


def _representation_bucket(row: dict[str, str]) -> str:
    organism = _safe_text(row.get("organism")) or "unknown_organism"
    family = _safe_text(row.get("protein_family")) or "unknown_family"
    ligand = _safe_text(row.get("ligand_class")) or "unknown_ligand"
    interface = _safe_text(row.get("binding_interface_type")) or "unknown_interface"
    return "|".join([organism, family, ligand, interface])


def _allocate_groups(
    groups: list[dict[str, Any]],
    *,
    test_frac: float,
    seed: int,
) -> dict[str, str]:
    total = sum(int(group["size"]) for group in groups)
    test_target = max(1, int(total * test_frac)) if groups else 0
    train_target = total - test_target
    split_sizes = {"train": 0, "test": 0}
    split_repr: dict[str, Counter[str]] = {"train": Counter(), "test": Counter()}
    total_repr = Counter(group["bucket"] for group in groups)
    groups = sorted(
        groups,
        key=lambda row: (
            -int(row["size"]),
            hashlib.md5(f"{seed}:{row['group_key']}".encode("utf-8")).hexdigest(),
        ),
    )
    assignment: dict[str, str] = {}
    for group in groups:
        scores: list[tuple[float, str]] = []
        for split_name in ("train", "test"):
            target = train_target if split_name == "train" else test_target
            after_size = split_sizes[split_name] + int(group["size"])
            size_penalty = abs(after_size - target)
            bucket = str(group["bucket"])
            current_repr = split_repr[split_name][bucket]
            ideal = (total_repr[bucket] * (target / max(total, 1))) if total else 0.0
            repr_penalty = abs((current_repr + int(group["size"])) - ideal)
            scores.append((size_penalty + repr_penalty, split_name))
        chosen = min(scores, key=lambda item: (item[0], item[1]))[1]
        assignment[str(group["group_key"])] = chosen
        split_sizes[chosen] += int(group["size"])
        split_repr[chosen][str(group["bucket"])] += int(group["size"])
    return assignment


def engineer_dataset(
    layout: StorageLayout,
    *,
    config: DatasetEngineeringConfig,
) -> dict[str, str]:
    global _ESM_CACHE_DIR
    _ESM_CACHE_DIR = layout.workspace_metadata_dir / "esm_cache"
    metadata_path = layout.workspace_metadata_dir / "protein_metadata.csv"
    rows = _read_csv_rows(metadata_path)
    if not rows:
        raise ValueError(f"No metadata rows found at {metadata_path}. Run metadata harvest first.")

    backend = _embedding_backend_name(config.embedding_backend)
    vectors = [_row_embedding(row, backend=backend if backend != "esm_unavailable" else "fallback") for row in rows]
    cluster_ids = _kmeans(vectors, k=config.cluster_count, seed=config.seed)

    grouped: dict[str, dict[str, Any]] = {}
    for row, cluster_id in zip(rows, cluster_ids):
        group_key = _hard_group_key(row, strict_family_isolation=config.strict_family_isolation)
        bucket = _representation_bucket(row)
        group = grouped.setdefault(
            group_key,
            {
                "group_key": group_key,
                "bucket": bucket,
                "cluster_id": cluster_id,
                "rows": [],
                "size": 0,
            },
        )
        group["rows"].append(row)
        group["size"] += 1

    assignments = _allocate_groups(list(grouped.values()), test_frac=config.test_frac, seed=config.seed)
    out_dir = layout.workspace_datasets_dir / config.dataset_name
    out_dir.mkdir(parents=True, exist_ok=True)

    split_rows: dict[str, list[dict[str, str]]] = {"train": [], "test": []}
    for group_key, group in grouped.items():
        split_name = assignments[group_key]
        for row in group["rows"]:
            split_rows[split_name].append({**row, "cluster_id": str(group["cluster_id"]), "dataset_split": split_name})

    artifacts: dict[str, str] = {}
    for split_name, rows_for_split in split_rows.items():
        path = out_dir / f"{split_name}.csv"
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(rows_for_split[0].keys()) if rows_for_split else ["dataset_split"])
            writer.writeheader()
            writer.writerows(rows_for_split)
        artifacts[f"{split_name}_csv"] = str(path)

    cv_dir = out_dir / "cv_folds"
    if config.cv_folds > 1:
        cv_dir.mkdir(parents=True, exist_ok=True)
        train_rows = split_rows["train"]
        for fold in range(config.cv_folds):
            fold_rows = [
                {**row, "cv_fold": str(fold)}
                for idx, row in enumerate(train_rows)
                if idx % config.cv_folds == fold
            ]
            fold_path = cv_dir / f"fold_{fold}.csv"
            with fold_path.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(handle, fieldnames=list(fold_rows[0].keys()) if fold_rows else ["cv_fold"])
                writer.writeheader()
                writer.writerows(fold_rows)
        artifacts["cv_folds_dir"] = str(cv_dir)

    diversity_report = {
        "generated_at": _utc_now(),
        "embedding_backend": backend,
        "row_count": len(rows),
        "train_count": len(split_rows["train"]),
        "test_count": len(split_rows["test"]),
        "protein_family_diversity": len(set(_safe_text(row.get("protein_family")) for row in rows if _safe_text(row.get("protein_family")))),
        "organism_diversity": len(set(_safe_text(row.get("organism")) for row in rows if _safe_text(row.get("organism")))),
        "structure_diversity": len(set(_safe_text(row.get("structural_fold")) for row in rows if _safe_text(row.get("structural_fold")))),
        "cluster_count": len(set(cluster_ids)),
    }
    diversity_path = out_dir / "diversity_report.json"
    diversity_path.write_text(json.dumps(diversity_report, indent=2), encoding="utf-8")
    artifacts["diversity_report"] = str(diversity_path)

    config_artifacts = write_dataset_export_configs(
        layout,
        dataset_name=config.dataset_name,
        dataset_config={
            "dataset_name": config.dataset_name,
            "test_frac": config.test_frac,
            "cv_folds": config.cv_folds,
            "strict_family_isolation": config.strict_family_isolation,
            "embedding_backend": backend,
            "cluster_count": config.cluster_count,
            "seed": config.seed,
        },
        feature_schema={
            "source": str(metadata_path),
            "feature_types": ["sequence_embedding", "metadata_embedding"],
        },
        graph_config={
            "graph_inputs": ["structural_graphs", "canonical_graph_optional"],
            "cluster_assignment_strategy": "hard_group_then_kmeans",
        },
    )
    artifacts.update(config_artifacts)
    return artifacts
