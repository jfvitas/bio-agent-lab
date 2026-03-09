"""External graph source connectors.

Implements adapters for key external biological interaction and pathway
databases.  Each connector:
- downloads/fetches data via public API or flat-file
- normalizes to GraphNodeRecord / GraphEdgeRecord
- maps identifiers to UniProt where possible
- applies conservative evidence filtering

Currently implemented:
  STRING   — protein-protein interaction scores (combined_score >= threshold)
  Reactome — pathway membership (protein -> pathway edges)
  BioGRID  — literature-curated binary protein interactions

All connectors are designed to be called offline (batch) or on-demand
(per-protein) and return canonical graph records.
"""

from __future__ import annotations

import csv
import io
import logging
import re
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import requests

from pbdata.schemas.graph import GraphEdgeRecord, GraphNodeRecord

logger = logging.getLogger(__name__)

_TIMEOUT = 60
_DELAY = 0.25


# ---------------------------------------------------------------------------
# Base / stub
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class GraphConnectorStub:
    source_name: str
    raw_dir: Path
    status: str = "stub"
    instructions: str = (
        "Implement raw download, schema normalization, identifier mapping, "
        "and evidence-score handling before enabling this connector."
    )


def connector_stub(source_name: str, raw_dir: Path) -> GraphConnectorStub:
    return GraphConnectorStub(source_name=source_name, raw_dir=raw_dir)


# ---------------------------------------------------------------------------
# STRING connector — protein-protein interactions
# ---------------------------------------------------------------------------

_STRING_API_BASE = "https://string-db.org/api"
_STRING_VERSION = "12.0"


@dataclass
class STRINGConnector:
    """Fetch protein-protein interactions from STRING.

    Uses the STRING REST API with UniProt identifiers.
    Only returns edges above the evidence score threshold.
    """

    species_id: int = 9606  # default: Homo sapiens
    score_threshold: int = 400  # STRING combined score (0-1000)
    network_type: str = "physical"  # "physical" or "functional"

    def fetch_interactions(
        self,
        uniprot_ids: list[str],
    ) -> tuple[list[GraphNodeRecord], list[GraphEdgeRecord]]:
        """Fetch PPI edges for a set of UniProt IDs from STRING."""
        if not uniprot_ids:
            return [], []

        nodes: dict[str, GraphNodeRecord] = {}
        edges: dict[str, GraphEdgeRecord] = {}

        # STRING API accepts batches of identifiers
        batch_size = 200
        for start in range(0, len(uniprot_ids), batch_size):
            batch = uniprot_ids[start:start + batch_size]
            batch_nodes, batch_edges = self._fetch_batch(batch)
            for n in batch_nodes:
                nodes.setdefault(n.node_id, n)
            for e in batch_edges:
                edges.setdefault(e.edge_id, e)

        return list(nodes.values()), list(edges.values())

    def _fetch_batch(
        self,
        uniprot_ids: list[str],
    ) -> tuple[list[GraphNodeRecord], list[GraphEdgeRecord]]:
        nodes: list[GraphNodeRecord] = []
        edges: list[GraphEdgeRecord] = []

        try:
            resp = requests.post(
                f"{_STRING_API_BASE}/tsv/network",
                data={
                    "identifiers": "\r".join(uniprot_ids),
                    "species": self.species_id,
                    "caller_identity": "pbdata",
                    "network_type": self.network_type,
                    "required_score": self.score_threshold,
                },
                timeout=_TIMEOUT,
            )
            resp.raise_for_status()
        except Exception as exc:
            logger.warning("STRING API request failed: %s", exc)
            return nodes, edges

        time.sleep(_DELAY)

        seen_proteins: set[str] = set()
        reader = csv.DictReader(io.StringIO(resp.text), delimiter="\t")

        for row in reader:
            try:
                raw_score = row.get("score")
                if raw_score is None:
                    raw_score = row.get("combined_score")
                score = int(raw_score) if raw_score is not None else 0
            except (TypeError, ValueError):
                score = 0

            if score < self.score_threshold:
                continue

            pref_a = row.get("preferredName_A")
            pref_b = row.get("preferredName_B")
            string_a = str(row.get("stringId_A") or "").strip()
            string_b = str(row.get("stringId_B") or "").strip()
            prot_a = str(pref_a).strip() if pref_a else string_a
            prot_b = str(pref_b).strip() if pref_b else string_b

            if not prot_a or not prot_b:
                continue

            # Create protein nodes for proteins we haven't seen
            for prot, string_id in [(prot_a, string_a), (prot_b, string_b)]:
                if prot not in seen_proteins:
                    seen_proteins.add(prot)
                    node_id = f"protein:{prot}"
                    nodes.append(GraphNodeRecord(
                        node_id=node_id,
                        node_type="Protein",
                        primary_id=prot,
                        display_name=prot,
                        source_databases=["STRING"],
                        metadata={"string_id": string_id, "species": self.species_id},
                    ))

            # Canonical edge (sorted node IDs for deduplication)
            src, dst = sorted([prot_a, prot_b])
            edge_id = f"string_ppi:{src}:{dst}"
            evidence_score = round(score / 1000.0, 4)

            edges.append(GraphEdgeRecord(
                edge_id=edge_id,
                edge_type="ProteinProteinInteraction",
                source_node_id=f"protein:{src}",
                target_node_id=f"protein:{dst}",
                source_database="STRING",
                evidence_score=evidence_score,
                relation=self.network_type,
                metadata={
                    "combined_score": score,
                    "string_id_a": string_a,
                    "string_id_b": string_b,
                },
            ))

        return nodes, edges


# ---------------------------------------------------------------------------
# Reactome connector — pathway membership
# ---------------------------------------------------------------------------

_REACTOME_CONTENT_SERVICE = "https://reactome.org/ContentService"


@dataclass
class ReactomeConnector:
    """Fetch pathway membership from Reactome.

    Maps UniProt IDs to Reactome pathway nodes, creating:
    - Pathway nodes (one per Reactome pathway)
    - ProteinPathway edges (protein -> pathway membership)
    """

    species: str = "Homo sapiens"

    def fetch_pathways(
        self,
        uniprot_ids: list[str],
    ) -> tuple[list[GraphNodeRecord], list[GraphEdgeRecord]]:
        """Fetch pathway nodes and membership edges for UniProt IDs."""
        if not uniprot_ids:
            return [], []

        pathway_nodes: dict[str, GraphNodeRecord] = {}
        edges: dict[str, GraphEdgeRecord] = {}

        for uniprot_id in uniprot_ids:
            try:
                pathways = self._fetch_pathways_for_protein(uniprot_id)
            except Exception as exc:
                logger.warning(
                    "Reactome lookup failed for %s: %s", uniprot_id, exc,
                )
                continue

            for pathway_id, pathway_name in pathways:
                # Pathway node
                node_id = f"pathway:{pathway_id}"
                pathway_nodes.setdefault(node_id, GraphNodeRecord(
                    node_id=node_id,
                    node_type="Pathway",
                    primary_id=pathway_id,
                    display_name=pathway_name,
                    source_databases=["Reactome"],
                    metadata={"species": self.species},
                ))

                # ProteinPathway edge
                edge_id = f"reactome_membership:{uniprot_id}:{pathway_id}"
                edges.setdefault(edge_id, GraphEdgeRecord(
                    edge_id=edge_id,
                    edge_type="ProteinPathway",
                    source_node_id=f"protein:{uniprot_id}",
                    target_node_id=node_id,
                    source_database="Reactome",
                    relation="pathway_membership",
                    metadata={
                        "uniprot_id": uniprot_id,
                        "pathway_id": pathway_id,
                        "pathway_name": pathway_name,
                    },
                ))

        return list(pathway_nodes.values()), list(edges.values())

    def _fetch_pathways_for_protein(
        self,
        uniprot_id: str,
    ) -> list[tuple[str, str]]:
        """Return list of (pathway_id, pathway_name) for one UniProt ID."""
        resp = requests.get(
            f"{_REACTOME_CONTENT_SERVICE}/data/pathways/low/entity/{uniprot_id}",
            params={"species": self.species},
            headers={"Accept": "application/json"},
            timeout=_TIMEOUT,
        )
        time.sleep(_DELAY)

        if resp.status_code == 404:
            # No pathways found for this protein
            return []
        resp.raise_for_status()

        results: list[tuple[str, str]] = []
        data = resp.json()
        if not isinstance(data, list):
            return []
        for entry in data:
            stable_id = str(entry.get("stId") or "")
            name = str(entry.get("displayName") or "")
            if stable_id:
                results.append((stable_id, name))
        return results


# ---------------------------------------------------------------------------
# BioGRID connector — literature-curated PPIs
# ---------------------------------------------------------------------------

_BIOGRID_REST_URL = "https://webservice.thebiogrid.org/interactions/"
_BIOGRID_EVIDENCE_TYPES = {
    "physical",
    "genetic",
}


@dataclass
class BioGRIDConnector:
    """Fetch protein-protein interactions from BioGRID.

    Requires an API access key (free registration at thebiogrid.org).
    Returns literature-curated binary interactions.
    """

    access_key: str = ""
    evidence_type: str = "physical"  # "physical" or "genetic"
    tax_id: int = 9606

    def fetch_interactions(
        self,
        gene_names: list[str],
    ) -> tuple[list[GraphNodeRecord], list[GraphEdgeRecord]]:
        """Fetch BioGRID interactions for a list of gene/protein names."""
        if not gene_names or not self.access_key:
            if not self.access_key:
                logger.info("BioGRID access key not configured; skipping.")
            return [], []

        nodes: dict[str, GraphNodeRecord] = {}
        edges: dict[str, GraphEdgeRecord] = {}

        # BioGRID API accepts batches
        batch_size = 100
        for start in range(0, len(gene_names), batch_size):
            batch = gene_names[start:start + batch_size]
            try:
                batch_nodes, batch_edges = self._fetch_batch(batch)
                for n in batch_nodes:
                    nodes.setdefault(n.node_id, n)
                for e in batch_edges:
                    edges.setdefault(e.edge_id, e)
            except Exception as exc:
                logger.warning("BioGRID batch fetch failed: %s", exc)

        return list(nodes.values()), list(edges.values())

    def _fetch_batch(
        self,
        gene_names: list[str],
    ) -> tuple[list[GraphNodeRecord], list[GraphEdgeRecord]]:
        nodes: list[GraphNodeRecord] = []
        edges: list[GraphEdgeRecord] = []

        params: dict[str, Any] = {
            "accesskey": self.access_key,
            "format": "json",
            "searchNames": "true",
            "geneList": "|".join(gene_names),
            "taxId": self.tax_id,
            "includeInteractors": "true",
            "max": 10000,
        }
        if self.evidence_type in _BIOGRID_EVIDENCE_TYPES:
            params["interSpeciesExcluded"] = "true"
            params["selfInteractionsExcluded"] = "true"
            params["evidenceList"] = self.evidence_type

        try:
            resp = requests.get(
                _BIOGRID_REST_URL,
                params=params,
                timeout=_TIMEOUT,
            )
            resp.raise_for_status()
        except Exception as exc:
            logger.warning("BioGRID API request failed: %s", exc)
            return nodes, edges

        time.sleep(_DELAY)

        data = resp.json()
        if not isinstance(data, dict):
            return nodes, edges

        seen_proteins: set[str] = set()

        for interaction in data.values():
            if not isinstance(interaction, dict):
                continue

            gene_a = str(interaction.get("OFFICIAL_SYMBOL_A") or "").strip()
            gene_b = str(interaction.get("OFFICIAL_SYMBOL_B") or "").strip()
            sys_a = str(interaction.get("SYSTEMATIC_NAME_A") or "").strip()
            sys_b = str(interaction.get("SYSTEMATIC_NAME_B") or "").strip()
            biogrid_id = str(interaction.get("BIOGRID_INTERACTION_ID") or "")
            exp_system = str(interaction.get("EXPERIMENTAL_SYSTEM") or "")
            pubmed = str(interaction.get("PUBMED_ID") or "")

            if not gene_a or not gene_b:
                continue

            # Create protein nodes
            for gene, systematic in [(gene_a, sys_a), (gene_b, sys_b)]:
                if gene not in seen_proteins:
                    seen_proteins.add(gene)
                    node_id = f"protein:{gene}"
                    nodes.append(GraphNodeRecord(
                        node_id=node_id,
                        node_type="Protein",
                        primary_id=gene,
                        display_name=gene,
                        source_databases=["BioGRID"],
                        metadata={
                            "systematic_name": systematic,
                            "tax_id": self.tax_id,
                        },
                    ))

            # Create edge (canonical ordering for dedup)
            src, dst = sorted([gene_a, gene_b])
            edge_id = f"biogrid_ppi:{src}:{dst}:{biogrid_id}"

            edges.append(GraphEdgeRecord(
                edge_id=edge_id,
                edge_type="ProteinProteinInteraction",
                source_node_id=f"protein:{src}",
                target_node_id=f"protein:{dst}",
                source_database="BioGRID",
                relation=exp_system or "physical_interaction",
                metadata={
                    "biogrid_id": biogrid_id,
                    "experimental_system": exp_system,
                    "pubmed_id": pubmed,
                },
            ))

        return nodes, edges


# ---------------------------------------------------------------------------
# Connector registry — used by the graph builder to discover connectors
# ---------------------------------------------------------------------------

@dataclass
class ConnectorRegistry:
    """Registry of available graph connectors and their configuration."""

    string: STRINGConnector = field(default_factory=STRINGConnector)
    reactome: ReactomeConnector = field(default_factory=ReactomeConnector)
    biogrid: BioGRIDConnector = field(default_factory=BioGRIDConnector)

    def enabled_connectors(self) -> list[str]:
        """Return names of connectors that are ready to use."""
        names: list[str] = ["string", "reactome"]
        if self.biogrid.access_key:
            names.append("biogrid")
        return names
