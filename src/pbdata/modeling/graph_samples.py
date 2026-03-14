"""Per-example graph sample manifests for Model Studio."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pbdata.storage import StorageLayout


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _read_json(path: Path) -> Any:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def build_graph_sample_manifest(
    layout: StorageLayout,
    *,
    output_path: Path | None = None,
) -> Path:
    examples = _read_json(layout.training_dir / "training_examples.json")
    nodes = _read_json(layout.graph_dir / "graph_nodes.json")
    edges = _read_json(layout.graph_dir / "graph_edges.json")

    node_ids_by_pdb: dict[str, list[str]] = {}
    edge_ids_by_pdb: dict[str, list[str]] = {}
    if isinstance(nodes, list):
        for node in nodes:
            if not isinstance(node, dict):
                continue
            metadata = node.get("metadata") or {}
            pdb_id = str(metadata.get("pdb_id") or node.get("primary_id") or "").strip().upper()
            node_id = str(node.get("node_id") or "").strip()
            if pdb_id and node_id:
                node_ids_by_pdb.setdefault(pdb_id, []).append(node_id)
    if isinstance(edges, list):
        for edge in edges:
            if not isinstance(edge, dict):
                continue
            metadata = edge.get("metadata") or {}
            pdb_id = str(metadata.get("pdb_id") or "").strip().upper()
            edge_id = str(edge.get("edge_id") or "").strip()
            if pdb_id and edge_id:
                edge_ids_by_pdb.setdefault(pdb_id, []).append(edge_id)

    rows: list[dict[str, Any]] = []
    if isinstance(examples, list):
        for row in examples:
            if not isinstance(row, dict):
                continue
            structure = row.get("structure") or {}
            provenance = row.get("provenance") or {}
            pdb_id = str(structure.get("pdb_id") or provenance.get("pdb_id") or "").strip().upper()
            if not pdb_id:
                continue
            node_ids = node_ids_by_pdb.get(pdb_id, [])
            edge_ids = edge_ids_by_pdb.get(pdb_id, [])
            rows.append({
                "example_id": str(row.get("example_id") or ""),
                "pdb_id": pdb_id,
                "pair_identity_key": str(provenance.get("pair_identity_key") or ""),
                "node_ids": node_ids,
                "edge_ids": edge_ids,
                "node_count": len(node_ids),
                "edge_count": len(edge_ids),
                "graph_ready": bool(node_ids and edge_ids),
            })

    manifest = {
        "generated_at": _utc_now(),
        "sample_count": len(rows),
        "graph_ready_count": sum(1 for row in rows if row.get("graph_ready")),
        "rows": rows,
    }
    destination = output_path or (layout.models_dir / "model_studio" / "graph_samples" / "graph_sample_manifest.json")
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return destination
