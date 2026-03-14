"""Supervised graph training payload materialization for Model Studio."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pbdata.storage import StorageLayout
from pbdata.modeling.graph_pyg_adapter import build_pyg_ready_graph_samples


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


def _read_split_ids(path: Path) -> set[str]:
    if not path.exists():
        return set()
    return {line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip()}


def _split_for_pdb(layout: StorageLayout, pdb_id: str) -> str:
    explicit = {
        "train": _read_split_ids(layout.splits_dir / "train.txt"),
        "val": _read_split_ids(layout.splits_dir / "val.txt"),
        "test": _read_split_ids(layout.splits_dir / "test.txt"),
    }
    candidates = {pdb_id.strip().upper(), f"RCSB_{pdb_id.strip().upper()}"}
    for split_name in ("train", "val", "test"):
        if explicit[split_name] & candidates:
            return split_name
    return "unspecified"


def materialize_graph_training_payload(
    layout: StorageLayout,
    *,
    task: str = "regression",
    output_dir: Path | None = None,
) -> tuple[Path, Path]:
    samples_path, _samples_manifest_path = build_pyg_ready_graph_samples(layout)
    samples = _read_json(samples_path)
    examples = _read_json(layout.training_dir / "training_examples.json")
    if not isinstance(samples, list):
        samples = []
    if not isinstance(examples, list):
        examples = []

    labels_by_example_id: dict[str, dict[str, Any]] = {}
    fallback_values: list[float] = []
    for row in examples:
        if not isinstance(row, dict):
            continue
        example_id = str(row.get("example_id") or "").strip()
        labels = row.get("labels") or {}
        if example_id and isinstance(labels, dict):
            labels_by_example_id[example_id] = labels
            value = _safe_float(labels.get("binding_affinity_log10"))
            if value is not None:
                fallback_values.append(value)

    threshold = None
    if task == "classification" and fallback_values:
        ordered = sorted(fallback_values)
        threshold = ordered[len(ordered) // 2]

    payload_rows: list[dict[str, Any]] = []
    for row in samples:
        if not isinstance(row, dict):
            continue
        example_id = str(row.get("example_id") or "").strip()
        labels = labels_by_example_id.get(example_id, {})
        target = None
        target_name = ""
        if task == "classification":
            if "is_mutant" in labels and labels.get("is_mutant") is not None:
                target = 1 if bool(labels.get("is_mutant")) else 0
                target_name = "labels.is_mutant"
            else:
                affinity = _safe_float(labels.get("binding_affinity_log10"))
                if affinity is not None and threshold is not None:
                    target = 1 if affinity >= threshold else 0
                    target_name = "labels.binding_affinity_log10_median_bin"
        else:
            affinity = _safe_float(labels.get("binding_affinity_log10"))
            if affinity is not None:
                target = affinity
                target_name = "labels.binding_affinity_log10"

        payload_rows.append({
            "example_id": example_id,
            "pdb_id": str(row.get("pdb_id") or ""),
            "pair_identity_key": str(row.get("pair_identity_key") or ""),
            "split": _split_for_pdb(layout, str(row.get("pdb_id") or "")),
            "task": task,
            "target_name": target_name,
            "target_value": target,
            "graph_sample": row,
        })

    destination_dir = output_dir or (layout.models_dir / "model_studio" / "graph_training_payload")
    destination_dir.mkdir(parents=True, exist_ok=True)
    payload_path = destination_dir / "graph_training_payload.json"
    payload_path.write_text(json.dumps(payload_rows, indent=2), encoding="utf-8")
    manifest = {
        "generated_at": _utc_now(),
        "task": task,
        "record_count": len(payload_rows),
        "labeled_record_count": sum(1 for row in payload_rows if row.get("target_value") is not None),
        "samples_path": str(samples_path),
        "payload_path": str(payload_path),
    }
    manifest_path = destination_dir / "graph_training_payload_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return payload_path, manifest_path
