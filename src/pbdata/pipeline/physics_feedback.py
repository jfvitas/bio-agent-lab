"""Offline physics results ingest and surrogate training helpers.

Assumptions:
- External ORCA/APBS/OpenMM jobs are run outside the normal project runtime.
- Project code only consumes parsed, machine-readable result tables from the
  expected external-analysis directories.
- The initial surrogate is a deterministic linear model over site environment
  descriptors plus motif identity, not a full equivariant GNN.
"""

from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

try:
    import torch
except ModuleNotFoundError:
    torch = None  # type: ignore[assignment]

from pbdata.storage import StorageLayout
from pbdata.table_io import read_dataframe, write_dataframe

TARGET_COLUMNS = [
    "refined_partial_charge",
    "electrostatic_potential",
    "electric_field_magnitude",
    "donor_strength",
    "acceptor_strength",
    "polarizability_proxy",
    "effective_steric_radius",
    "desolvation_penalty_proxy",
    "protonation_preference_score",
    "metal_binding_propensity",
    "aromatic_interaction_propensity",
    "local_environment_strain_score",
]
_SURROGATE_FILE = "site_physics_surrogate.pt"
_LATEST_FILE = "latest_surrogate_checkpoint.json"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _read_table_rows(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    if path.suffix.lower() == ".parquet":
        return read_dataframe(path).to_dict(orient="records")
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        rows.append(json.loads(line))
    return rows


def _load_parsed_rows(base_dir: Path, batch_id: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    result_sets = []
    for tool_name in ("orca", "apbs", "openmm"):
        parsed_dir = base_dir / tool_name / batch_id / "parsed"
        rows: list[dict[str, Any]] = []
        for candidate in (parsed_dir / "parsed_results.parquet", parsed_dir / "parsed_results.jsonl"):
            if candidate.exists():
                rows = _read_table_rows(candidate)
                break
        result_sets.append(rows)
    return tuple(result_sets)  # type: ignore[return-value]


def _as_float(value: object) -> float | None:
    try:
        text = str(value or "").strip()
        if not text:
            return None
        return float(text)
    except ValueError:
        return None


def _normalized_target_row(
    key: tuple[str, str, str],
    source_rows: dict[str, dict[str, Any]],
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    fragment_id, archetype_id, motif_class = key
    merged: dict[str, Any] = {
        "fragment_id": fragment_id,
        "archetype_id": archetype_id,
        "motif_class": motif_class,
    }
    methods: list[str] = []
    quality_flags: list[str] = []
    provenance = {"sources": {}}

    orca = source_rows.get("orca") or {}
    apbs = source_rows.get("apbs") or {}
    openmm = source_rows.get("openmm") or {}

    def _status_failed(row: dict[str, Any]) -> bool:
        return str(row.get("status") or "").lower() not in {"", "ok", "success", "completed", "passed"}

    if orca:
        methods.append("ORCA")
        provenance["sources"]["orca"] = orca
        if _status_failed(orca):
            quality_flags.append("orca_failed")
        central_charge = None
        atomic_charges = orca.get("atomic_charges")
        if isinstance(atomic_charges, list) and atomic_charges:
            central_charge = _as_float(atomic_charges[0])
        merged["refined_partial_charge"] = _as_float(orca.get("refined_partial_charge")) or central_charge
        merged["donor_strength"] = _as_float(orca.get("donor_strength")) or _as_float(orca.get("donor_probe_preference"))
        merged["acceptor_strength"] = _as_float(orca.get("acceptor_strength")) or _as_float(orca.get("acceptor_probe_preference"))
        merged["polarizability_proxy"] = _as_float(orca.get("polarizability_proxy")) or _as_float(orca.get("polarizability_summary"))
        merged["protonation_preference_score"] = _as_float(orca.get("protonation_preference_score")) or _as_float(orca.get("protonation_preference"))
        merged["metal_binding_propensity"] = _as_float(orca.get("metal_binding_propensity"))
        merged["aromatic_interaction_propensity"] = _as_float(orca.get("aromatic_interaction_propensity"))

    if apbs:
        methods.append("APBS")
        provenance["sources"]["apbs"] = apbs
        if _status_failed(apbs):
            quality_flags.append("apbs_failed")
        merged["electrostatic_potential"] = _as_float(apbs.get("electrostatic_potential")) or _as_float(apbs.get("site_potential"))
        merged["electric_field_magnitude"] = _as_float(apbs.get("electric_field_magnitude")) or _as_float(apbs.get("field_magnitude_proxy"))
        merged["desolvation_penalty_proxy"] = _as_float(apbs.get("desolvation_penalty_proxy")) or _as_float(apbs.get("electrostatic_desolvation"))

    if openmm:
        methods.append("OpenMM")
        provenance["sources"]["openmm"] = openmm
        if _status_failed(openmm):
            quality_flags.append("openmm_failed")
        merged["effective_steric_radius"] = _as_float(openmm.get("effective_steric_radius")) or _as_float(openmm.get("steric_radius")) or _as_float(openmm.get("vdw_proxy"))
        merged["local_environment_strain_score"] = _as_float(openmm.get("local_environment_strain_score")) or _as_float(openmm.get("strain_proxy"))

    missing_targets = [column for column in TARGET_COLUMNS if merged.get(column) is None]
    if missing_targets:
        quality_flags.append("missing_targets:" + ",".join(missing_targets))
    merged["source_analysis_methods"] = "; ".join(methods)
    merged["target_quality_flag"] = "ok" if not quality_flags else "; ".join(quality_flags)
    merged["provenance_json"] = json.dumps(provenance, sort_keys=True)
    if len(missing_targets) == len(TARGET_COLUMNS):
        return None, {
            "fragment_id": fragment_id,
            "archetype_id": archetype_id,
            "motif_class": motif_class,
            "reason": "no_usable_target_values",
            "source_analysis_methods": "; ".join(methods),
        }
    return merged, None


def ingest_external_analysis_results(layout: StorageLayout, *, batch_id: str) -> dict[str, str]:
    base_dir = layout.external_analysis_artifacts_dir
    orca_rows, apbs_rows, openmm_rows = _load_parsed_rows(base_dir, batch_id)
    merged_rows: dict[tuple[str, str, str], dict[str, dict[str, Any]]] = defaultdict(dict)

    def _ingest_rows(tool_name: str, rows: list[dict[str, Any]]) -> None:
        for row in rows:
            fragment_id = str(row.get("fragment_id") or "")
            archetype_id = str(row.get("archetype_id") or "")
            motif_class = str(row.get("motif_class") or "")
            if not fragment_id or not archetype_id or not motif_class:
                continue
            merged_rows[(fragment_id, archetype_id, motif_class)][tool_name] = row

    _ingest_rows("orca", orca_rows)
    _ingest_rows("apbs", apbs_rows)
    _ingest_rows("openmm", openmm_rows)

    physics_rows: list[dict[str, Any]] = []
    failed_rows: list[dict[str, Any]] = []
    for key, source_rows in merged_rows.items():
        normalized, failed = _normalized_target_row(key, source_rows)
        if normalized is not None:
            physics_rows.append(normalized)
        if failed is not None:
            failed_rows.append(failed)

    out_dir = layout.physics_targets_artifacts_dir / batch_id
    out_dir.mkdir(parents=True, exist_ok=True)
    physics_path = out_dir / "physics_targets.parquet"
    failed_path = out_dir / "failed_fragments.parquet"
    manifest_path = out_dir / "physics_target_manifest.json"
    write_dataframe(pd.DataFrame(physics_rows), physics_path)
    write_dataframe(pd.DataFrame(failed_rows), failed_path)
    manifest = {
        "generated_at": _utc_now(),
        "batch_id": batch_id,
        "status": "ingested",
        "row_count": len(physics_rows),
        "failed_fragment_count": len(failed_rows),
        "tools_seen": {
            "orca_rows": len(orca_rows),
            "apbs_rows": len(apbs_rows),
            "openmm_rows": len(openmm_rows),
        },
        "target_columns": TARGET_COLUMNS,
    }
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return {
        "physics_targets": str(physics_path),
        "failed_fragments": str(failed_path),
        "manifest": str(manifest_path),
    }


def _site_feature_rows(layout: StorageLayout, source_run_id: str) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    env_dir = layout.base_features_artifacts_dir / source_run_id
    for env_path in sorted(env_dir.glob("*.env_vectors.parquet")):
        env_df = read_dataframe(env_path)
        if env_df.empty:
            continue
        for site_id, site_df in env_df.groupby("site_id", sort=True):
            motif_class = str(site_df["motif_class"].iloc[0])
            feature_row: dict[str, Any] = {"site_id": site_id, "motif_class": motif_class}
            for row in site_df.to_dict(orient="records"):
                prefix = str(row["shell_name"])
                for key in (
                    "neighbor_atom_count",
                    "heavy_atom_count",
                    "polar_atom_count",
                    "charged_atom_count",
                    "aromatic_centroid_count",
                    "metal_count",
                    "sum_partial_charge",
                    "electric_field_magnitude",
                    "donor_count",
                    "acceptor_count",
                ):
                    feature_row[f"{prefix}.{key}"] = float(row.get(key) or 0.0)
            rows.append(feature_row)
    return pd.DataFrame(rows)


def _build_training_matrix(
    physics_targets: pd.DataFrame,
    archetypes: pd.DataFrame,
    site_features: pd.DataFrame,
) -> tuple[list[list[float]], list[list[float]], list[str], list[str], list[str]]:
    merged = physics_targets.merge(archetypes[["archetype_id", "site_id", "motif_class"]], on=["archetype_id", "motif_class"], how="inner")
    merged = merged.merge(site_features, on=["site_id", "motif_class"], how="inner")
    feature_columns = sorted(
        column for column in merged.columns
        if any(column.startswith(f"{shell}.") for shell in ("shell_1", "shell_2", "shell_3"))
    )
    motif_classes = sorted(set(str(value) for value in merged["motif_class"]))
    motif_index = {motif: idx for idx, motif in enumerate(motif_classes)}

    x_rows: list[list[float]] = []
    y_rows: list[list[float]] = []
    for row in merged.to_dict(orient="records"):
        features = [float(row.get(column) or 0.0) for column in feature_columns]
        one_hot = [0.0] * len(motif_classes)
        one_hot[motif_index[str(row["motif_class"])]] = 1.0
        x_rows.append(features + one_hot + [1.0])
        y_rows.append([float(row.get(column) or 0.0) for column in TARGET_COLUMNS])
    return x_rows, y_rows, feature_columns, motif_classes, TARGET_COLUMNS


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


def _least_squares_weights(x_rows: list[list[float]], y_rows: list[list[float]]) -> list[list[float]]:
    x_t = _transpose(x_rows)
    xtx = _matmul(x_t, x_rows)
    for idx in range(len(xtx)):
        xtx[idx][idx] += 1e-6
    xty = _matmul(x_t, y_rows)
    return _matmul(_invert_square_matrix(xtx), xty)


def train_site_physics_surrogate(
    layout: StorageLayout,
    *,
    batch_id: str,
    source_run_id: str,
    surrogate_run_id: str | None = None,
) -> dict[str, str]:
    surrogate_run_id = surrogate_run_id or f"surrogate_{batch_id}"
    physics_targets = read_dataframe(layout.physics_targets_artifacts_dir / batch_id / "physics_targets.parquet")
    archetypes = read_dataframe(layout.archetypes_artifacts_dir / source_run_id / "archetypes.parquet")
    site_features = _site_feature_rows(layout, source_run_id)

    x_rows, y_rows, feature_columns, motif_classes, target_columns = _build_training_matrix(physics_targets, archetypes, site_features)
    if not x_rows or not y_rows:
        raise ValueError("No training rows available after joining physics targets with archetype site features.")
    if torch is not None:
        x_tensor = torch.tensor(x_rows, dtype=torch.float32)
        y_tensor = torch.tensor(y_rows, dtype=torch.float32)
        solution = torch.linalg.lstsq(x_tensor, y_tensor).solution
        predictions = x_tensor @ solution
        mse_values = torch.mean((predictions - y_tensor) ** 2, dim=0).tolist()
        weights_payload: Any = solution
        checkpoint_format = "torch_tensor"
    else:
        solution = _least_squares_weights(x_rows, y_rows)
        predictions = _matmul(x_rows, solution)
        mse_values = []
        for idx in range(len(target_columns)):
            deltas = [(prediction[idx] - actual[idx]) ** 2 for prediction, actual in zip(predictions, y_rows)]
            mse_values.append(sum(deltas) / max(len(deltas), 1))
        weights_payload = solution
        checkpoint_format = "json_matrix"

    out_dir = layout.surrogate_training_artifacts_dir / surrogate_run_id
    out_dir.mkdir(parents=True, exist_ok=True)
    checkpoint_path = out_dir / _SURROGATE_FILE
    manifest_path = out_dir / "surrogate_manifest.json"
    checkpoint_payload = {
        "version": "site_physics_surrogate_v1",
        "source_batch_id": batch_id,
        "source_run_id": source_run_id,
        "feature_columns": feature_columns,
        "motif_classes": motif_classes,
        "target_columns": target_columns,
        "weights": weights_payload,
        "checkpoint_format": checkpoint_format,
    }
    if torch is not None:
        torch.save(checkpoint_payload, checkpoint_path)
    else:
        checkpoint_path.write_text(json.dumps(checkpoint_payload, indent=2), encoding="utf-8")
    manifest = {
        "generated_at": _utc_now(),
        "surrogate_run_id": surrogate_run_id,
        "source_batch_id": batch_id,
        "source_run_id": source_run_id,
        "status": "trained",
        "training_row_count": len(x_rows),
        "feature_count": len(feature_columns),
        "motif_class_count": len(motif_classes),
        "target_columns": target_columns,
        "per_target_mse": {
            column: round(float(mse_values[idx]), 8)
            for idx, column in enumerate(target_columns)
        },
        "checkpoint_path": str(checkpoint_path),
    }
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    latest_path = layout.surrogate_training_artifacts_dir / _LATEST_FILE
    latest_path.write_text(json.dumps({
        "surrogate_run_id": surrogate_run_id,
        "checkpoint_path": str(checkpoint_path),
        "manifest_path": str(manifest_path),
        "generated_at": _utc_now(),
    }, indent=2), encoding="utf-8")
    return {
        "checkpoint": str(checkpoint_path),
        "manifest": str(manifest_path),
        "latest": str(latest_path),
    }


def load_latest_site_physics_surrogate(layout: StorageLayout) -> dict[str, Any] | None:
    latest_path = layout.surrogate_training_artifacts_dir / _LATEST_FILE
    if not latest_path.exists():
        return None
    latest = json.loads(latest_path.read_text(encoding="utf-8"))
    checkpoint_path = Path(str(latest.get("checkpoint_path") or ""))
    if not checkpoint_path.exists():
        return None
    if torch is not None:
        try:
            return torch.load(checkpoint_path, map_location="cpu")
        except Exception:
            pass
    return json.loads(checkpoint_path.read_text(encoding="utf-8"))


def predict_site_physics_from_surrogate(
    model: dict[str, Any],
    *,
    motif_class: str,
    feature_values: dict[str, float],
) -> dict[str, float]:
    feature_columns = list(model["feature_columns"])
    motif_classes = list(model["motif_classes"])
    target_columns = list(model["target_columns"])
    weights = model["weights"]
    row = [float(feature_values.get(column, 0.0)) for column in feature_columns]
    one_hot = [0.0] * len(motif_classes)
    if motif_class in motif_classes:
        one_hot[motif_classes.index(motif_class)] = 1.0
    vector = row + one_hot + [1.0]
    if torch is not None and hasattr(weights, "shape"):
        prediction = torch.tensor([vector], dtype=torch.float32) @ weights
        return {
            column: float(prediction[0, idx].item())
            for idx, column in enumerate(target_columns)
        }
    prediction_values = [
        sum(vector[feature_idx] * float(weights[feature_idx][target_idx]) for feature_idx in range(len(vector)))
        for target_idx in range(len(target_columns))
    ]
    return {
        column: float(prediction_values[idx])
        for idx, column in enumerate(target_columns)
    }
