"""PyG-ready graph sample adapter helpers.

This module does not require torch/torch_geometric. It prepares a canonical
sample representation so a future native PyG trainer can focus on tensorization
instead of re-deriving graph semantics from raw exports.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pbdata.storage import StorageLayout
from pbdata.modeling.graph_dataset import materialize_graph_dataset_records

_NODE_TYPE_KEYS = (
    "proteincomplex",
    "protein",
    "ligand",
    "peptide",
    "domain",
    "interface",
)
_EDGE_TYPE_KEYS = (
    "proteinligandinteraction",
    "proteinproteininteraction",
    "association",
    "annotation",
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _read_json(path: Path) -> Any:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def _node_feature_vector(node: dict[str, Any]) -> dict[str, float]:
    node_type = str(node.get("node_type") or "").strip().lower()
    metadata = node.get("metadata") or {}
    source_dbs = node.get("source_databases") or []
    features: dict[str, float] = {
        "bias": 1.0,
        "has_uniprot_id": 1.0 if node.get("uniprot_id") else 0.0,
        "has_entrez_id": 1.0 if node.get("entrez_id") else 0.0,
        "has_ensembl_id": 1.0 if node.get("ensembl_id") else 0.0,
        "has_display_name": 1.0 if node.get("display_name") else 0.0,
        "has_pdb_metadata": 1.0 if metadata.get("pdb_id") else 0.0,
        "source_db_count": float(len(source_dbs)) if isinstance(source_dbs, list) else 0.0,
    }
    for key in _NODE_TYPE_KEYS:
        features[f"node_type.{key}"] = 1.0 if node_type == key else 0.0
    return features


def _edge_feature_vector(edge: dict[str, Any]) -> dict[str, float]:
    edge_type = str(edge.get("edge_type") or "").strip().lower()
    metadata = edge.get("metadata") or {}
    relation = str(edge.get("relation") or "").strip()
    evidence_score = edge.get("evidence_score")
    numeric_evidence = 0.0
    try:
        if evidence_score is not None:
            numeric_evidence = float(evidence_score)
    except (TypeError, ValueError):
        numeric_evidence = 0.0
    features: dict[str, float] = {
        "bias": 1.0,
        "has_relation": 1.0 if relation else 0.0,
        "has_affinity_value": 1.0 if metadata.get("binding_affinity_value") is not None else 0.0,
        "evidence_score": numeric_evidence,
    }
    for key in _EDGE_TYPE_KEYS:
        features[f"edge_type.{key}"] = 1.0 if edge_type == key else 0.0
    return features


def build_pyg_ready_graph_samples(
    layout: StorageLayout,
    *,
    output_dir: Path | None = None,
) -> tuple[Path, Path]:
    records_path, _dataset_manifest_path = materialize_graph_dataset_records(layout)
    records = _read_json(records_path)
    if not isinstance(records, list):
        records = []

    graph_samples: list[dict[str, Any]] = []
    for record in records:
        if not isinstance(record, dict):
            continue
        nodes = record.get("nodes") or []
        edges = record.get("edges") or []
        if not isinstance(nodes, list) or not isinstance(edges, list):
            continue

        node_index_by_id: dict[str, int] = {}
        node_feature_rows: list[dict[str, float]] = []
        ordered_node_ids: list[str] = []
        for idx, node in enumerate(nodes):
            if not isinstance(node, dict):
                continue
            node_id = str(node.get("node_id") or "").strip()
            if not node_id:
                continue
            node_index_by_id[node_id] = len(ordered_node_ids)
            ordered_node_ids.append(node_id)
            node_feature_rows.append(_node_feature_vector(node))

        edge_index: list[list[int]] = []
        edge_feature_rows: list[dict[str, float]] = []
        unresolved_edges = 0
        for edge in edges:
            if not isinstance(edge, dict):
                continue
            src = str(edge.get("source_node_id") or "").strip()
            dst = str(edge.get("target_node_id") or "").strip()
            if src in node_index_by_id and dst in node_index_by_id:
                edge_index.append([node_index_by_id[src], node_index_by_id[dst]])
                edge_feature_rows.append(_edge_feature_vector(edge))
            else:
                unresolved_edges += 1

        graph_samples.append({
            "example_id": str(record.get("example_id") or ""),
            "pdb_id": str(record.get("pdb_id") or ""),
            "pair_identity_key": str(record.get("pair_identity_key") or ""),
            "node_ids": ordered_node_ids,
            "edge_index": edge_index,
            "node_features": node_feature_rows,
            "edge_features": edge_feature_rows,
            "node_feature_keys": sorted({key for row in node_feature_rows for key in row.keys()}),
            "edge_feature_keys": sorted({key for row in edge_feature_rows for key in row.keys()}),
            "node_count": len(ordered_node_ids),
            "edge_count": len(edge_index),
            "unresolved_edge_count": unresolved_edges,
        })

    destination_dir = output_dir or (layout.models_dir / "model_studio" / "pyg_ready_graphs")
    destination_dir.mkdir(parents=True, exist_ok=True)
    samples_path = destination_dir / "pyg_ready_graph_samples.json"
    samples_path.write_text(json.dumps(graph_samples, indent=2), encoding="utf-8")
    manifest = {
        "generated_at": _utc_now(),
        "sample_count": len(graph_samples),
        "records_path": str(records_path),
        "samples_path": str(samples_path),
        "notes": "PyG-ready canonical samples prepared without requiring torch_geometric at materialization time.",
    }
    manifest_path = destination_dir / "pyg_ready_graph_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return samples_path, manifest_path
