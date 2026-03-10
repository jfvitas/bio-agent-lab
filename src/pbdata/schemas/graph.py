"""Graph-layer record schemas for the biological interaction network."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict


class GraphNodeRecord(BaseModel):
    """One graph node in the feature/graph layer."""

    model_config = ConfigDict(frozen=True)

    node_id: str
    node_type: Literal["Protein", "Ligand", "Gene", "Pathway", "ProteinComplex"]
    primary_id: str
    display_name: str | None = None
    source_databases: list[str] | None = None
    uniprot_id: str | None = None
    entrez_id: str | None = None
    ensembl_id: str | None = None
    metadata: dict[str, Any] | None = None
    provenance: dict[str, str | None] | None = None


class GraphEdgeRecord(BaseModel):
    """One graph edge connecting two node records."""

    model_config = ConfigDict(frozen=True)

    edge_id: str
    edge_type: Literal[
        "ProteinProteinInteraction",
        "ProteinLigandInteraction",
        "GeneProtein",
        "ProteinPathway",
        "LigandSimilarity",
    ]
    source_node_id: str
    target_node_id: str
    source_database: str
    evidence_score: float | None = None
    relation: str | None = None
    metadata: dict[str, Any] | None = None
    provenance: dict[str, str | None] | None = None
