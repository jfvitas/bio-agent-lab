"""Pathway feature helpers backed by the canonical graph layer."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class PathwayFeaturePlan:
    node_id: str
    pathway_count: int | None = None
    status: str = "stub"
    notes: str = (
        "Populate after graph pathway nodes and ProteinPathway edges are "
        "materialized from external databases."
    )


def plan_pathway_features(node_id: str) -> PathwayFeaturePlan:
    return PathwayFeaturePlan(node_id=node_id)


def summarize_pathway_features(node_id: str, graph_dir: Path) -> PathwayFeaturePlan:
    nodes_path = graph_dir / "graph_nodes.json"
    edges_path = graph_dir / "graph_edges.json"
    if not nodes_path.exists() or not edges_path.exists():
        return PathwayFeaturePlan(
            node_id=node_id,
            pathway_count=0,
            status="graph_missing",
            notes="Graph files are missing, so pathway membership cannot be summarized.",
        )

    nodes = json.loads(nodes_path.read_text(encoding="utf-8"))
    edges = json.loads(edges_path.read_text(encoding="utf-8"))
    pathway_node_ids = {
        str(node.get("node_id") or "")
        for node in nodes
        if isinstance(node, dict) and str(node.get("node_type") or "") == "Pathway"
    }
    count = 0
    for edge in edges:
        if not isinstance(edge, dict):
            continue
        if str(edge.get("edge_type") or "") != "ProteinPathway":
            continue
        src = str(edge.get("source_node_id") or "")
        tgt = str(edge.get("target_node_id") or "")
        if src == node_id and tgt in pathway_node_ids:
            count += 1
        elif tgt == node_id and src in pathway_node_ids:
            count += 1

    status = "from_graph_pathways" if count > 0 else "no_external_sources"
    notes = (
        "Pathway count derived from canonical ProteinPathway graph edges."
        if count > 0
        else "No graph pathway memberships were found for this node."
    )
    return PathwayFeaturePlan(
        node_id=node_id,
        pathway_count=count,
        status=status,
        notes=notes,
    )
