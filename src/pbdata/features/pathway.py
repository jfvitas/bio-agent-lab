"""Pathway feature stubs.

The current feature layer sets pathway-derived fields to unknown/zero because
external pathway sources are not yet ingested into the graph.
"""

from __future__ import annotations

from dataclasses import dataclass


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
