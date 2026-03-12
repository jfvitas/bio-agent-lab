"""Graph builder for the canonical graph layer.

This module supports three states:
- if extracted records are present, materialize canonical graph nodes/edges
- optionally merge external source data (STRING, Reactome, BioGRID)
- otherwise, emit an architecture manifest describing the planned subsystem
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pbdata.pairing import parse_pair_identity_key
from pbdata.schemas.graph import GraphEdgeRecord, GraphNodeRecord

logger = logging.getLogger(__name__)

_GRAPH_SOURCES = [
    "STRING",
    "BioGRID",
    "IntAct",
    "DIP",
    "MINT",
    "Reactome",
    "PathwayCommons",
    "KEGG",
    "SIGNOR",
    "OmniPath",
    "IID",
    "HPRD",
    "HIPPIE",
    "MatrixDB",
    "CORUM",
    "ComplexPortal",
    "PINA",
    "iRefIndex",
    "APID",
    "InnateDB",
    "TRRUST",
    "ChEA",
    "PhosphoSitePlus",
    "DrugBank",
    "BindingDB",
    "ChEMBL",
    "STITCH",
    "GuideToPharmacology",
    "TCRD",
    "OpenTargets",
    "HumanNet",
    "BioPlex",
    "HuRI",
    "PrePPI",
    "PROPER",
]


def _edge_provenance(
    *,
    source: str,
    confidence: str,
    source_record_key: str,
    extraction_method: str,
) -> dict[str, str]:
    return {
        "source": source,
        "confidence": confidence,
        "retrieved_at": datetime.now(timezone.utc).isoformat(),
        "source_record_key": source_record_key,
        "extraction_method": extraction_method,
    }


def _load_table_json(table_dir: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not table_dir.exists():
        return rows
    for path in sorted(table_dir.glob("*.json")):
        raw = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(raw, list):
            rows.extend(item for item in raw if isinstance(item, dict))
        elif isinstance(raw, dict):
            rows.append(raw)
    return rows


def _protein_node_id(chain: dict[str, Any]) -> str:
    uniprot_id = chain.get("uniprot_id")
    if uniprot_id:
        return f"protein:{uniprot_id}"
    pdb_id = chain.get("pdb_id") or "unknown"
    chain_id = chain.get("chain_id") or "?"
    return f"protein:{pdb_id}:{chain_id}"


def _ligand_node_id(bound_object: dict[str, Any]) -> str:
    # Prefer InChIKey (globally unique), then component_id, then name
    primary = bound_object.get("component_inchikey")
    if not primary:
        primary = bound_object.get("component_id")
    if not primary:
        primary = bound_object.get("component_name")
    if not primary:
        primary = "unknown"
    return f"ligand:{primary}"


def _complex_node_id(entry: dict[str, Any]) -> str:
    pdb_id = entry.get("pdb_id") or entry.get("source_record_id") or "unknown"
    assembly_id = entry.get("assembly_id") or "default"
    return f"complex:{pdb_id}:{assembly_id}"


def _make_graph_manifest(
    output_dir: Path,
    *,
    status: str,
    node_count: int | None = None,
    edge_count: int | None = None,
    external_sources_merged: list[str] | None = None,
    notes: str,
) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    manifest = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "status": status,
        "layer": "feature_graph",
        "node_types": ["Protein", "Ligand", "Gene", "Pathway", "ProteinComplex"],
        "edge_types": [
            "ProteinProteinInteraction",
            "ProteinLigandInteraction",
            "GeneProtein",
            "ProteinPathway",
            "LigandSimilarity",
        ],
        "identifier_mapping": ["UniProt", "Entrez", "Ensembl"],
        "upstream_sources": _GRAPH_SOURCES,
        "external_sources_merged": external_sources_merged or [],
        "node_count": node_count,
        "edge_count": edge_count,
        "notes": notes,
    }
    out_path = output_dir / "graph_manifest.json"
    out_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return out_path


def build_graph_manifest(output_dir: Path) -> Path:
    """Write a graph-build manifest describing the planned subsystem."""
    return _make_graph_manifest(
        output_dir,
        status="planned",
        notes=(
            "Architecture scaffold only. Data ingestion, identifier harmonization, "
            "evidence merging, and persistence are not implemented yet."
        ),
    )


def _collect_uniprot_ids(
    protein_nodes: dict[str, GraphNodeRecord],
) -> list[str]:
    """Extract unique UniProt IDs from protein nodes."""
    ids: list[str] = []
    seen: set[str] = set()
    for node in protein_nodes.values():
        uid = node.uniprot_id
        if uid and uid not in seen:
            seen.add(uid)
            ids.append(uid)
    return ids


def merge_external_sources(
    protein_nodes: dict[str, GraphNodeRecord],
    ligand_nodes: dict[str, GraphNodeRecord],
    pathway_nodes: dict[str, GraphNodeRecord],
    edges: dict[str, GraphEdgeRecord],
    *,
    enable_string: bool = True,
    enable_reactome: bool = True,
    enable_biogrid: bool = False,
    biogrid_access_key: str = "",
    string_score_threshold: int = 400,
    log_fn: Any = None,
) -> list[str]:
    """Merge external connector data into the graph.

    Modifies the node/edge dicts in place.  Returns list of source names merged.
    """
    from pbdata.graph.connectors import (
        BioGRIDConnector,
        ReactomeConnector,
        STRINGConnector,
    )

    merged_sources: list[str] = []
    uniprot_ids = _collect_uniprot_ids(protein_nodes)

    if not uniprot_ids:
        if log_fn:
            log_fn("No UniProt IDs found in protein nodes; skipping external sources.")
        return merged_sources

    if log_fn:
        log_fn(f"Found {len(uniprot_ids)} UniProt IDs for external lookups.")

    # --- STRING ---
    if enable_string:
        if log_fn:
            log_fn(f"Querying STRING (threshold={string_score_threshold})...")
        try:
            connector = STRINGConnector(score_threshold=string_score_threshold)
            new_nodes, new_edges = connector.fetch_interactions(uniprot_ids)
            _merge_nodes_and_edges(protein_nodes, edges, new_nodes, new_edges)
            merged_sources.append("STRING")
            if log_fn:
                log_fn(f"  STRING: +{len(new_nodes)} nodes, +{len(new_edges)} edges")
        except Exception as exc:
            logger.warning("STRING integration failed: %s", exc)
            if log_fn:
                log_fn(f"  STRING failed: {exc}")

    # --- Reactome ---
    if enable_reactome:
        if log_fn:
            log_fn("Querying Reactome for pathway membership...")
        try:
            connector_r = ReactomeConnector()
            pathway_nodes_list, pathway_edges_list = connector_r.fetch_pathways(uniprot_ids)
            for node in pathway_nodes_list:
                pathway_nodes.setdefault(node.node_id, node)
            for edge in pathway_edges_list:
                edges.setdefault(edge.edge_id, edge)
            merged_sources.append("Reactome")
            if log_fn:
                log_fn(f"  Reactome: +{len(pathway_nodes_list)} pathway nodes, +{len(pathway_edges_list)} edges")
        except Exception as exc:
            logger.warning("Reactome integration failed: %s", exc)
            if log_fn:
                log_fn(f"  Reactome failed: {exc}")

    # --- BioGRID ---
    if enable_biogrid and biogrid_access_key:
        if log_fn:
            log_fn("Querying BioGRID...")
        try:
            connector_bg = BioGRIDConnector(access_key=biogrid_access_key)
            # BioGRID uses gene names, so extract display names
            gene_names = [
                n.display_name for n in protein_nodes.values()
                if n.display_name and n.node_type == "Protein"
            ]
            if gene_names:
                new_nodes, new_edges = connector_bg.fetch_interactions(gene_names)
                _merge_nodes_and_edges(protein_nodes, edges, new_nodes, new_edges)
                merged_sources.append("BioGRID")
                if log_fn:
                    log_fn(f"  BioGRID: +{len(new_nodes)} nodes, +{len(new_edges)} edges")
        except Exception as exc:
            logger.warning("BioGRID integration failed: %s", exc)
            if log_fn:
                log_fn(f"  BioGRID failed: {exc}")

    return merged_sources


def _merge_nodes_and_edges(
    protein_nodes: dict[str, GraphNodeRecord],
    edges: dict[str, GraphEdgeRecord],
    new_nodes: list[GraphNodeRecord],
    new_edges: list[GraphEdgeRecord],
) -> None:
    """Merge new nodes/edges into existing dicts, deduplicating by ID.

    For nodes with the same ID from different sources, merge source_databases.
    """
    for node in new_nodes:
        if node.node_id in protein_nodes:
            existing = protein_nodes[node.node_id]
            # Merge source databases
            existing_dbs = set(existing.source_databases or [])
            new_dbs = set(node.source_databases or [])
            merged_dbs = sorted(existing_dbs | new_dbs)
            if merged_dbs != sorted(existing_dbs):
                protein_nodes[node.node_id] = existing.model_copy(update={
                    "source_databases": merged_dbs,
                })
        else:
            protein_nodes[node.node_id] = node

    for edge in new_edges:
        edges.setdefault(edge.edge_id, edge)


def build_graph_from_extracted(
    extracted_dir: Path,
    output_dir: Path,
    *,
    enable_external: bool = False,
    enable_string: bool = True,
    enable_reactome: bool = True,
    enable_biogrid: bool = False,
    biogrid_access_key: str = "",
    string_score_threshold: int = 400,
    log_fn: Any = None,
) -> tuple[Path, Path, Path]:
    """Materialize graph nodes and edges from extracted structure tables.

    When enable_external=True, also queries STRING, Reactome, and BioGRID
    to enrich the graph with external interactions and pathway membership.
    """
    entries = _load_table_json(extracted_dir / "entry")
    chains = _load_table_json(extracted_dir / "chains")
    bound_objects = _load_table_json(extracted_dir / "bound_objects")
    interfaces = _load_table_json(extracted_dir / "interfaces")
    assays = _load_table_json(extracted_dir / "assays")

    output_dir.mkdir(parents=True, exist_ok=True)

    protein_nodes: dict[str, GraphNodeRecord] = {}
    ligand_nodes: dict[str, GraphNodeRecord] = {}
    complex_nodes: dict[str, GraphNodeRecord] = {}
    pathway_nodes: dict[str, GraphNodeRecord] = {}

    for entry in entries:
        node_id = _complex_node_id(entry)
        complex_nodes.setdefault(node_id, GraphNodeRecord(
            node_id=node_id,
            node_type="ProteinComplex",
            primary_id=str(entry.get("pdb_id") or entry.get("source_record_id") or node_id),
            display_name=entry.get("title"),
            source_databases=[str(entry.get("source_database") or "RCSB")],
            metadata={
                "pdb_id": entry.get("pdb_id"),
                "assembly_id": entry.get("assembly_id"),
                "organism_names": entry.get("organism_names"),
            },
            provenance={
                "source": str(entry.get("source_database") or "RCSB"),
                "confidence": "high",
                "method": "extracted_entry_record",
            },
        ))

    protein_chain_index: dict[tuple[str, str], str] = {}
    for chain in chains:
        if not chain.get("is_protein"):
            continue
        node_id = _protein_node_id(chain)
        protein_chain_index[(str(chain.get("pdb_id") or ""), str(chain.get("chain_id") or ""))] = node_id
        protein_nodes.setdefault(node_id, GraphNodeRecord(
            node_id=node_id,
            node_type="Protein",
            primary_id=str(chain.get("uniprot_id") or f"{chain.get('pdb_id')}:{chain.get('chain_id')}"),
            display_name=chain.get("chain_description") or chain.get("entity_description"),
            source_databases=["RCSB"],
            uniprot_id=chain.get("uniprot_id"),
            metadata={
                "pdb_id": chain.get("pdb_id"),
                "chain_id": chain.get("chain_id"),
                "taxonomy_id": chain.get("entity_source_taxonomy_id"),
                "organism": chain.get("entity_source_organism"),
            },
            provenance={
                "source": "RCSB",
                "confidence": "high",
                "method": "extracted_chain_record",
            },
        ))

    ligand_component_index: dict[tuple[str, str], str] = {}
    for bound_object in bound_objects:
        node_id = _ligand_node_id(bound_object)
        ligand_nodes.setdefault(node_id, GraphNodeRecord(
            node_id=node_id,
            node_type="Ligand",
            primary_id=str(
                bound_object.get("component_inchikey")
                or bound_object.get("component_id")
                or node_id
            ),
            display_name=bound_object.get("component_name") or bound_object.get("component_id"),
            source_databases=["RCSB"],
            metadata={
                "component_id": bound_object.get("component_id"),
                "component_type": bound_object.get("component_type"),
                "smiles": bound_object.get("component_smiles"),
            },
            provenance={
                "source": "RCSB",
                "confidence": "high",
                "method": "extracted_bound_object_record",
            },
        ))
        pdb_id = str(bound_object.get("pdb_id") or "")
        comp_id = str(bound_object.get("component_id") or "")
        if pdb_id and comp_id:
            ligand_component_index[(pdb_id, comp_id)] = node_id

    edges: dict[str, GraphEdgeRecord] = {}

    for interface in interfaces:
        iface_type = interface.get("interface_type")
        pdb_id = str(interface.get("pdb_id") or "")
        if iface_type == "protein_protein":
            for src_chain in interface.get("partner_a_chain_ids") or []:
                for dst_chain in interface.get("partner_b_chain_ids") or []:
                    src_node = protein_chain_index.get((pdb_id, str(src_chain)))
                    dst_node = protein_chain_index.get((pdb_id, str(dst_chain)))
                    if not src_node or not dst_node:
                        continue
                    edge_id = f"ppi:{pdb_id}:{src_node}:{dst_node}"
                    edges.setdefault(edge_id, GraphEdgeRecord(
                        edge_id=edge_id,
                        edge_type="ProteinProteinInteraction",
                        source_node_id=src_node,
                        target_node_id=dst_node,
                        source_database="RCSB",
                        relation="structural_interface",
                        metadata={"pdb_id": pdb_id},
                        provenance=_edge_provenance(
                            source="RCSB",
                            confidence="medium",
                            source_record_key=(
                                f"{pdb_id}:"
                                f"{','.join(str(chain) for chain in interface.get('partner_a_chain_ids') or [])}:"
                                f"{','.join(str(chain) for chain in interface.get('partner_b_chain_ids') or [])}"
                            ),
                            extraction_method="structural_interface_graph_projection",
                        ),
                    ))
        elif iface_type == "protein_ligand":
            ligand_name = str(interface.get("entity_name_b") or "")
            ligand_node = ligand_component_index.get((pdb_id, ligand_name))
            if not ligand_node and ligand_name:
                ligand_node = next(
                    (
                        node_id for node_id, node in ligand_nodes.items()
                        if ligand_name in {
                            str(node.primary_id or ""),
                            str(node.display_name or ""),
                        }
                    ),
                    None,
                )
            if not ligand_node:
                continue
            for chain_id in interface.get("binding_site_chain_ids") or []:
                protein_node = protein_chain_index.get((pdb_id, str(chain_id)))
                if not protein_node:
                    continue
                edge_id = f"pli:{pdb_id}:{protein_node}:{ligand_node}"
                edges.setdefault(edge_id, GraphEdgeRecord(
                    edge_id=edge_id,
                    edge_type="ProteinLigandInteraction",
                    source_node_id=protein_node,
                    target_node_id=ligand_node,
                    source_database="BioLiP",
                    relation="binding_site_annotation",
                    metadata={
                        "pdb_id": pdb_id,
                        "binding_site_residue_ids": interface.get("binding_site_residue_ids"),
                    },
                    provenance=_edge_provenance(
                        source="BioLiP",
                        confidence="medium",
                        source_record_key=(
                            f"{pdb_id}:{ligand_name or ligand_node}:"
                            f"{','.join(str(chain) for chain in interface.get('binding_site_chain_ids') or [])}"
                        ),
                        extraction_method="binding_site_annotation_graph_projection",
                    ),
                ))

    for assay in assays:
        pair_key = str(assay.get("pair_identity_key") or "")
        parsed_pair = parse_pair_identity_key(pair_key)
        if parsed_pair is None or parsed_pair.task_type != "protein_ligand":
            continue
        pdb_id = parsed_pair.pdb_id or ""
        ligand_key = parsed_pair.ligand_key or ""
        ligand_node = next(
            (node_id for node_id, node in ligand_nodes.items() if node.primary_id == ligand_key or node_id == f"ligand:{ligand_key}"),
            None,
        )
        if not ligand_node:
            continue
        for chain_id in parsed_pair.receptor_chain_ids:
            protein_node = protein_chain_index.get((pdb_id, chain_id))
            if not protein_node:
                continue
            edge_id = f"assay_pli:{pdb_id}:{protein_node}:{ligand_node}:{assay.get('binding_affinity_type')}"
            edges.setdefault(edge_id, GraphEdgeRecord(
                edge_id=edge_id,
                edge_type="ProteinLigandInteraction",
                source_node_id=protein_node,
                target_node_id=ligand_node,
                source_database=str(assay.get("source_database") or "unknown"),
                relation=str(assay.get("binding_affinity_type") or "assay_measurement"),
                metadata={
                    "pdb_id": pdb_id,
                    "pair_identity_key": pair_key,
                    "binding_affinity_value": assay.get("binding_affinity_value"),
                    "binding_affinity_unit": assay.get("binding_affinity_unit"),
                },
                provenance=_edge_provenance(
                    source=str(assay.get("source_database") or "unknown"),
                    confidence="medium",
                    source_record_key=pair_key or edge_id,
                    extraction_method="assay_pair_graph_projection",
                ),
            ))

    # --- Merge external sources ---
    merged_sources: list[str] = []
    if enable_external:
        merged_sources = merge_external_sources(
            protein_nodes, ligand_nodes, pathway_nodes, edges,
            enable_string=enable_string,
            enable_reactome=enable_reactome,
            enable_biogrid=enable_biogrid,
            biogrid_access_key=biogrid_access_key,
            string_score_threshold=string_score_threshold,
            log_fn=log_fn,
        )

    if not pathway_nodes:
        pathway_nodes["pathway:placeholder_unannotated_context"] = GraphNodeRecord(
            node_id="pathway:placeholder_unannotated_context",
            node_type="Pathway",
            primary_id="placeholder_unannotated_context",
            display_name="Pathway context not yet annotated",
            source_databases=["internal_placeholder"],
            metadata={
                "placeholder": True,
                "reason": "external pathway ingestion disabled or no pathway membership was available",
            },
            provenance={
                "source": "internal_placeholder",
                "confidence": "low",
                "method": "placeholder_pathway_backfill",
            },
        )

    all_nodes = [
        *sorted(complex_nodes.values(), key=lambda item: item.node_id),
        *sorted(pathway_nodes.values(), key=lambda item: item.node_id),
        *sorted(protein_nodes.values(), key=lambda item: item.node_id),
        *sorted(ligand_nodes.values(), key=lambda item: item.node_id),
    ]
    all_edges = sorted(edges.values(), key=lambda item: item.edge_id)

    nodes_path = output_dir / "graph_nodes.json"
    edges_path = output_dir / "graph_edges.json"
    nodes_path.write_text(
        json.dumps([node.model_dump(mode="json") for node in all_nodes], indent=2),
        encoding="utf-8",
    )
    edges_path.write_text(
        json.dumps([edge.model_dump(mode="json") for edge in all_edges], indent=2),
        encoding="utf-8",
    )

    status = "materialized_from_extracted"
    if merged_sources:
        status = "materialized_with_external"

    manifest_path = _make_graph_manifest(
        output_dir,
        status=status,
        node_count=len(all_nodes),
        edge_count=len(all_edges),
        external_sources_merged=merged_sources,
        notes=(
            f"Graph materialized from extracted structure and assay tables. "
            f"External sources merged: {', '.join(merged_sources) if merged_sources else 'none'}."
        ),
    )
    return nodes_path, edges_path, manifest_path
