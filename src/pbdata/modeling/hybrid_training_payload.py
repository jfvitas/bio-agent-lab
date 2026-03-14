"""Hybrid graph+attribute training payload materialization."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pbdata.storage import StorageLayout
from pbdata.modeling.graph_training_payload import materialize_graph_training_payload


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _read_json(path: Path) -> Any:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def _safe_float(value: object) -> float | None:
    try:
        text = str(value).strip()
        if not text:
            return None
        return float(text)
    except Exception:
        return None


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


def _attribute_vector(example: dict[str, Any]) -> dict[str, float]:
    features: dict[str, float] = {}
    for section_name in ("structure", "protein", "ligand", "interaction", "experiment", "graph_features"):
        section = example.get(section_name)
        if isinstance(section, dict):
            _flatten_numeric(section_name, section, features)
    return features


def materialize_hybrid_training_payload(
    layout: StorageLayout,
    *,
    task: str = "regression",
    output_dir: Path | None = None,
) -> tuple[Path, Path]:
    graph_payload_path, _graph_manifest_path = materialize_graph_training_payload(layout, task=task)
    graph_payload = _read_json(graph_payload_path)
    examples = _read_json(layout.training_dir / "training_examples.json")
    if not isinstance(graph_payload, list):
        graph_payload = []
    if not isinstance(examples, list):
        examples = []

    example_by_id: dict[str, dict[str, Any]] = {}
    for row in examples:
        if not isinstance(row, dict):
            continue
        example_id = str(row.get("example_id") or "").strip()
        if example_id:
            example_by_id[example_id] = row

    rows: list[dict[str, Any]] = []
    attribute_keys: set[str] = set()
    for row in graph_payload:
        if not isinstance(row, dict):
            continue
        example_id = str(row.get("example_id") or "").strip()
        example = example_by_id.get(example_id, {})
        attribute_features = _attribute_vector(example)
        attribute_keys.update(attribute_features.keys())
        rows.append({
            "example_id": example_id,
            "pdb_id": str(row.get("pdb_id") or ""),
            "pair_identity_key": str(row.get("pair_identity_key") or ""),
            "split": str(row.get("split") or "unspecified"),
            "task": str(row.get("task") or task),
            "target_name": str(row.get("target_name") or ""),
            "target_value": row.get("target_value"),
            "attribute_features": attribute_features,
            "graph_sample": row.get("graph_sample") or {},
        })

    destination_dir = output_dir or (layout.models_dir / "model_studio" / "hybrid_training_payload")
    destination_dir.mkdir(parents=True, exist_ok=True)
    payload_path = destination_dir / "hybrid_training_payload.json"
    payload_path.write_text(json.dumps(rows, indent=2), encoding="utf-8")
    manifest = {
        "generated_at": _utc_now(),
        "task": task,
        "record_count": len(rows),
        "attribute_feature_keys": sorted(attribute_keys),
        "graph_payload_path": str(graph_payload_path),
        "payload_path": str(payload_path),
    }
    manifest_path = destination_dir / "hybrid_training_payload_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return payload_path, manifest_path
