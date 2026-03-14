"""Graph-sample contract helpers for native graph training."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pbdata.storage import StorageLayout


@dataclass(frozen=True)
class GraphLearningContract:
    available: bool
    matched_example_count: int
    total_example_count: int
    summary: str
    sample_pdb_ids: tuple[str, ...]


def _read_json(path: Path) -> Any:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def build_graph_learning_contract(layout: StorageLayout) -> GraphLearningContract:
    examples = _read_json(layout.training_dir / "training_examples.json")
    nodes = _read_json(layout.graph_dir / "graph_nodes.json")
    edges = _read_json(layout.graph_dir / "graph_edges.json")
    if not isinstance(examples, list) or not examples:
        return GraphLearningContract(
            available=False,
            matched_example_count=0,
            total_example_count=0,
            summary="No training examples available for graph learning.",
            sample_pdb_ids=(),
        )

    node_pdbs: set[str] = set()
    edge_pdbs: set[str] = set()
    if isinstance(nodes, list):
        for node in nodes:
            if not isinstance(node, dict):
                continue
            metadata = node.get("metadata") or {}
            pdb_id = str(metadata.get("pdb_id") or node.get("primary_id") or "").strip().upper()
            if pdb_id:
                node_pdbs.add(pdb_id)
    if isinstance(edges, list):
        for edge in edges:
            if not isinstance(edge, dict):
                continue
            metadata = edge.get("metadata") or {}
            pdb_id = str(metadata.get("pdb_id") or "").strip().upper()
            if pdb_id:
                edge_pdbs.add(pdb_id)

    matched: list[str] = []
    for row in examples:
        if not isinstance(row, dict):
            continue
        structure = row.get("structure") or {}
        provenance = row.get("provenance") or {}
        pdb_id = str(structure.get("pdb_id") or provenance.get("pdb_id") or "").strip().upper()
        if pdb_id and pdb_id in node_pdbs and pdb_id in edge_pdbs:
            matched.append(pdb_id)

    unique_matched = tuple(sorted(dict.fromkeys(matched)))
    available = bool(unique_matched)
    summary = (
        f"Matched {len(unique_matched):,} graph-ready PDB IDs across {len(examples):,} training examples."
        if available
        else "No training examples currently map to both graph nodes and graph edges."
    )
    return GraphLearningContract(
        available=available,
        matched_example_count=len(unique_matched),
        total_example_count=len(examples),
        summary=summary,
        sample_pdb_ids=unique_matched[:10],
    )
