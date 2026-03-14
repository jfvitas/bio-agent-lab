"""Materialize per-example graph dataset records for Model Studio."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pbdata.storage import StorageLayout
from pbdata.modeling.graph_samples import build_graph_sample_manifest


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _read_json(path: Path) -> Any:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def materialize_graph_dataset_records(
    layout: StorageLayout,
    *,
    output_dir: Path | None = None,
) -> tuple[Path, Path]:
    manifest_path = build_graph_sample_manifest(layout)
    manifest = _read_json(manifest_path) or {}
    graph_rows = manifest.get("rows") if isinstance(manifest, dict) else []
    nodes = _read_json(layout.graph_dir / "graph_nodes.json")
    edges = _read_json(layout.graph_dir / "graph_edges.json")

    node_by_id: dict[str, dict[str, Any]] = {}
    edge_by_id: dict[str, dict[str, Any]] = {}
    if isinstance(nodes, list):
        for node in nodes:
            if not isinstance(node, dict):
                continue
            node_id = str(node.get("node_id") or "").strip()
            if node_id:
                node_by_id[node_id] = node
    if isinstance(edges, list):
        for edge in edges:
            if not isinstance(edge, dict):
                continue
            edge_id = str(edge.get("edge_id") or "").strip()
            if edge_id:
                edge_by_id[edge_id] = edge

    records: list[dict[str, Any]] = []
    if isinstance(graph_rows, list):
        for row in graph_rows:
            if not isinstance(row, dict) or not row.get("graph_ready"):
                continue
            node_ids = [str(value) for value in row.get("node_ids") or [] if str(value)]
            edge_ids = [str(value) for value in row.get("edge_ids") or [] if str(value)]
            sliced_nodes = [node_by_id[node_id] for node_id in node_ids if node_id in node_by_id]
            sliced_edges = [edge_by_id[edge_id] for edge_id in edge_ids if edge_id in edge_by_id]
            records.append({
                "example_id": str(row.get("example_id") or ""),
                "pdb_id": str(row.get("pdb_id") or ""),
                "pair_identity_key": str(row.get("pair_identity_key") or ""),
                "node_count": len(sliced_nodes),
                "edge_count": len(sliced_edges),
                "nodes": sliced_nodes,
                "edges": sliced_edges,
            })

    destination_dir = output_dir or (layout.models_dir / "model_studio" / "graph_dataset")
    destination_dir.mkdir(parents=True, exist_ok=True)
    records_path = destination_dir / "graph_dataset_records.json"
    records_path.write_text(json.dumps(records, indent=2), encoding="utf-8")
    dataset_manifest = {
        "generated_at": _utc_now(),
        "record_count": len(records),
        "source_graph_sample_manifest": str(manifest_path),
        "records_path": str(records_path),
    }
    manifest_out_path = destination_dir / "graph_dataset_manifest.json"
    manifest_out_path.write_text(json.dumps(dataset_manifest, indent=2), encoding="utf-8")
    return records_path, manifest_out_path
