"""Training-example assembly.

Joins extracted structure records, assay records, graph features,
and feature-layer outputs into spec-aligned TrainingExampleRecord objects.

Assembly rules:
- One training example per unique (pair_identity_key, binding_affinity_type)
- Missing fields are set to None (not imputed)
- Labels use binding_affinity_log10_standardized when available
- Provenance tracks which tables contributed to each example
"""

from __future__ import annotations

import json
import logging
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pbdata.pairing import bound_object_matches_ligand_key, parse_pair_identity_key
from pbdata.schemas.training_example import (
    ExperimentFields,
    GraphFeatureFields,
    InteractionFields,
    LigandFields,
    ProteinFields,
    StructureFields,
    TrainingExampleRecord,
)

logger = logging.getLogger(__name__)


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


def _load_json_file(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    raw = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(raw, list):
        return [item for item in raw if isinstance(item, dict)]
    return [raw] if isinstance(raw, dict) else []


def assemble_training_examples(
    extracted_dir: Path,
    features_dir: Path,
    graph_dir: Path,
    output_dir: Path,
) -> tuple[Path, Path]:
    """Assemble TrainingExampleRecords from all upstream layers.

    Returns (examples_path, manifest_path).
    """
    # Load all upstream data
    entries = _load_table_json(extracted_dir / "entry")
    chains = _load_table_json(extracted_dir / "chains")
    bound_objects = _load_table_json(extracted_dir / "bound_objects")
    interfaces = _load_table_json(extracted_dir / "interfaces")
    assays = _load_table_json(extracted_dir / "assays")
    features = _load_json_file(features_dir / "feature_records.json")
    graph_nodes = _load_json_file(graph_dir / "graph_nodes.json")
    graph_edges = _load_json_file(graph_dir / "graph_edges.json")

    output_dir.mkdir(parents=True, exist_ok=True)

    # Build lookup indices
    entry_by_pdb: dict[str, dict[str, Any]] = {}
    for entry in entries:
        pdb_id = str(entry.get("pdb_id") or "")
        if pdb_id:
            entry_by_pdb[pdb_id] = entry

    # Chains by PDB ID — sorted by chain_id for deterministic primary chain selection
    protein_chains_by_pdb: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for chain in chains:
        if chain.get("is_protein"):
            pdb_id = str(chain.get("pdb_id") or "")
            if pdb_id:
                protein_chains_by_pdb[pdb_id].append(chain)
    for pdb_id in protein_chains_by_pdb:
        protein_chains_by_pdb[pdb_id].sort(
            key=lambda c: str(c.get("chain_id") or ""),
        )

    # Bound objects by PDB ID
    bound_by_pdb: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for obj in bound_objects:
        pdb_id = str(obj.get("pdb_id") or "")
        if pdb_id:
            bound_by_pdb[pdb_id].append(obj)

    # Interfaces by PDB ID
    interfaces_by_pdb: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for iface in interfaces:
        pdb_id = str(iface.get("pdb_id") or "")
        if pdb_id:
            interfaces_by_pdb[pdb_id].append(iface)

    # Feature records by pair_identity_key
    feature_by_pair: dict[str, dict[str, Any]] = {}
    for feat in features:
        pair_key = str(feat.get("pair_identity_key") or "")
        if pair_key and pair_key not in feature_by_pair:
            feature_by_pair[pair_key] = feat

    # Graph degree by node ID
    graph_degree: dict[str, int] = defaultdict(int)
    for edge in graph_edges:
        graph_degree[str(edge.get("source_node_id") or "")] += 1
        graph_degree[str(edge.get("target_node_id") or "")] += 1

    # Pathway count per protein node
    pathway_counts: dict[str, int] = defaultdict(int)
    pathway_node_ids = {
        str(n.get("node_id") or "") for n in graph_nodes
        if n.get("node_type") == "Pathway"
    }
    for edge in graph_edges:
        if edge.get("edge_type") != "ProteinPathway":
            continue
        src = str(edge.get("source_node_id") or "")
        tgt = str(edge.get("target_node_id") or "")
        if tgt in pathway_node_ids:
            pathway_counts[src] += 1
        elif src in pathway_node_ids:
            logger.warning(
                "ProteinPathway edge has reversed direction: source=%s (Pathway), "
                "target=%s. Counting anyway but upstream data should be fixed.",
                src, tgt,
            )
            pathway_counts[tgt] += 1

    # Protein node by PDB ID
    protein_node_by_pdb: dict[str, str] = {}
    protein_chain_to_node: dict[tuple[str, str], str] = {}
    for node in graph_nodes:
        if node.get("node_type") != "Protein":
            continue
        metadata = node.get("metadata") or {}
        pdb_id = str(metadata.get("pdb_id") or "")
        chain_id = str(metadata.get("chain_id") or "")
        if pdb_id and pdb_id not in protein_node_by_pdb:
            protein_node_by_pdb[pdb_id] = str(node.get("node_id") or "")
        if pdb_id and chain_id:
            protein_chain_to_node[(pdb_id, chain_id)] = str(node.get("node_id") or "")

    # Assemble examples — one per unique (pair_identity_key, affinity_type)
    examples: list[TrainingExampleRecord] = []
    seen_keys: set[tuple[str, str]] = set()
    generated_at = datetime.now(timezone.utc).isoformat()
    sources_used: set[str] = set()

    for assay in assays:
        pdb_id = str(assay.get("pdb_id") or "")
        pair_key = str(assay.get("pair_identity_key") or "")
        affinity_type = str(assay.get("binding_affinity_type") or "")
        if not pdb_id or not pair_key:
            continue

        dedupe_key = (pair_key, affinity_type)
        if dedupe_key in seen_keys:
            continue
        seen_keys.add(dedupe_key)

        entry = entry_by_pdb.get(pdb_id, {})
        protein_chains = protein_chains_by_pdb.get(pdb_id, [])
        ligands = bound_by_pdb.get(pdb_id, [])
        pdb_interfaces = interfaces_by_pdb.get(pdb_id, [])
        feat = feature_by_pair.get(pair_key, {})
        feat_values = feat.get("values", {}) if feat else {}
        parsed_pair = parse_pair_identity_key(pair_key)
        receptor_chain_ids = list(parsed_pair.receptor_chain_ids) if parsed_pair is not None else []
        src_db = str(assay.get("source_database") or "")

        # --- Structure ---
        chain_ids = receptor_chain_ids or [
            str(c.get("chain_id") or "") for c in protein_chains if c.get("chain_id")
        ]
        structure = StructureFields(
            pdb_id=pdb_id or None,
            chain_ids=chain_ids or None,
            assembly=str(entry.get("assembly_id") or "") or None,
            resolution=_safe_float(entry.get("structure_resolution")),
            atom_count_total=_safe_int(feat_values.get("atom_count_total")),
            heavy_atom_fraction=_safe_float(feat_values.get("heavy_atom_fraction")),
            mean_atomic_weight=_safe_float(feat_values.get("mean_atomic_weight")),
            mean_covalent_radius=_safe_float(feat_values.get("mean_covalent_radius")),
            mean_b_factor=_safe_float(feat_values.get("mean_b_factor")),
            mean_occupancy=_safe_float(feat_values.get("mean_occupancy")),
            residue_count_observed=_safe_int(feat_values.get("residue_count_observed")),
            radius_of_gyration_residue_centroids=_safe_float(
                feat_values.get("radius_of_gyration_residue_centroids")
            ),
        )

        # --- Protein (from first receptor chain when pair-specific chains are known) ---
        chain_by_id = {
            str(chain.get("chain_id") or ""): chain for chain in protein_chains if chain.get("chain_id")
        }
        primary_chain = {}
        for chain_id in receptor_chain_ids:
            if chain_id in chain_by_id:
                primary_chain = chain_by_id[chain_id]
                break
        if not primary_chain and protein_chains:
            primary_chain = protein_chains[0]
        protein = ProteinFields(
            uniprot_id=primary_chain.get("uniprot_id"),
            organism=primary_chain.get("entity_source_organism"),
            gene=primary_chain.get("chain_description"),
            domains=None,  # Not yet available
            sequence_length=_safe_int(feat_values.get("sequence_length")),
            mean_hydropathy=_safe_float(feat_values.get("protein_mean_hydropathy")),
            aromatic_fraction=_safe_float(feat_values.get("protein_aromatic_fraction")),
            charged_fraction=_safe_float(feat_values.get("protein_charged_fraction")),
            polar_fraction=_safe_float(feat_values.get("protein_polar_fraction")),
        )

        # --- Ligand (pair-matched ligand if available) ---
        primary_ligand = {}
        ligand_key = parsed_pair.ligand_key if parsed_pair is not None else None
        if ligand_key:
            for ligand_row in ligands:
                if bound_object_matches_ligand_key(ligand_row, ligand_key):
                    primary_ligand = ligand_row
                    break
        if not primary_ligand and ligands:
            primary_ligand = ligands[0]
        ligand = LigandFields(
            ligand_id=primary_ligand.get("component_id"),
            ligand_type=primary_ligand.get("component_type"),
            inchikey=primary_ligand.get("component_inchikey"),
            smiles=primary_ligand.get("component_smiles"),
            molecular_weight=_safe_float(primary_ligand.get("component_molecular_weight")),
        )

        # --- Interaction ---
        all_residues: list[str] = []
        for iface in pdb_interfaces:
            interface_chain_ids = set(iface.get("binding_site_chain_ids") or [])
            if receptor_chain_ids and interface_chain_ids and not interface_chain_ids.intersection(receptor_chain_ids):
                continue
            if ligand_key and str(iface.get("entity_name_b") or "") and str(iface.get("entity_name_b") or "") != ligand_key:
                continue
            for res in iface.get("binding_site_residue_ids") or []:
                if res not in all_residues:
                    all_residues.append(str(res))
        interaction = InteractionFields(
            interface_residues=all_residues or None,
            hydrogen_bonds=None,  # Would require coordinate analysis
            salt_bridges=None,
            interface_residue_count=_safe_int(feat_values.get("interface_residue_count")),
            microstate_record_count=_safe_int(feat_values.get("microstate_record_count")),
            estimated_net_charge=_safe_float(feat_values.get("estimated_net_charge")),
            mean_abs_residue_charge=_safe_float(feat_values.get("mean_abs_residue_charge")),
            positive_residue_count=_safe_int(feat_values.get("positive_residue_count")),
            negative_residue_count=_safe_int(feat_values.get("negative_residue_count")),
            same_charge_contact_count=_safe_int(feat_values.get("same_charge_contact_count")),
            opposite_charge_contact_count=_safe_int(feat_values.get("opposite_charge_contact_count")),
            metal_contact_count=_safe_int(feat_values.get("metal_contact_count")),
            acidic_cluster_penalty=_safe_float(feat_values.get("acidic_cluster_penalty")),
            local_electrostatic_balance=_safe_float(feat_values.get("local_electrostatic_balance")),
        )

        # --- Experiment ---
        experiment = ExperimentFields(
            affinity_type=affinity_type or None,
            affinity_value=_safe_float(assay.get("binding_affinity_value")),
            temperature=_safe_float(assay.get("assay_temperature_c")),
            ph=_safe_float(assay.get("assay_ph")),
            source_database=src_db or None,
            preferred_source_database=str(assay.get("selected_preferred_source") or "") or None,
            reported_measurement_count=_safe_int(assay.get("reported_measurement_count")),
            source_conflict_flag=_safe_bool(assay.get("source_conflict_flag")),
            source_agreement_band=str(assay.get("source_agreement_band") or "") or None,
        )

        # --- Graph features ---
        node_ids: list[str] = []
        for chain_id in receptor_chain_ids:
            node_id = protein_chain_to_node.get((pdb_id, chain_id))
            if node_id and node_id not in node_ids:
                node_ids.append(node_id)
        if not node_ids:
            fallback_node_id = protein_node_by_pdb.get(pdb_id, "")
            if fallback_node_id:
                node_ids.append(fallback_node_id)

        if node_ids:
            nd = sum(graph_degree[node_id] for node_id in node_ids)
            ppi = feat_values.get("ppi_degree")
            pli = feat_values.get("pli_degree")
            pc = sum(pathway_counts[node_id] for node_id in node_ids)
        else:
            # No graph node — fall back to feature layer values
            nd = feat_values.get("network_degree")
            ppi = feat_values.get("ppi_degree")
            pli = feat_values.get("pli_degree")
            pc = feat_values.get("pathway_count")
        graph_feat = GraphFeatureFields(
            network_degree=nd,
            ppi_degree=ppi,
            pli_degree=pli,
            pathway_count=pc,
        )

        # --- Labels ---
        labels: dict[str, Any] = {}
        log10_val = assay.get("binding_affinity_log10_standardized")
        if log10_val is not None:
            labels["binding_affinity_log10"] = log10_val
        raw_val = assay.get("binding_affinity_value")
        if raw_val is not None:
            labels["binding_affinity_raw"] = raw_val
        if affinity_type:
            labels["affinity_type"] = affinity_type
        is_mutant = assay.get("binding_affinity_is_mutant_measurement")
        if is_mutant is not None:
            labels["is_mutant"] = is_mutant
        if assay.get("source_conflict_flag") is not None:
            labels["source_conflict_flag"] = _safe_bool(assay.get("source_conflict_flag"))
        if assay.get("selected_preferred_source"):
            labels["preferred_source_database"] = assay.get("selected_preferred_source")
        ddg = assay.get("delta_delta_g")
        if ddg is not None:
            labels["delta_delta_g"] = ddg

        # Track sources
        if src_db:
            sources_used.add(src_db)

        examples.append(TrainingExampleRecord(
            example_id=f"train:{pdb_id}:{len(examples)}",
            structure=structure,
            protein=protein,
            ligand=ligand,
            interaction=interaction,
            experiment=experiment,
            graph_features=graph_feat,
            labels=labels if labels else None,
            provenance={
                "generated_at": generated_at,
                "pdb_id": pdb_id,
                "pair_identity_key": pair_key,
                "source_database": src_db,
                "preferred_source_database": str(assay.get("selected_preferred_source") or "") or None,
                "source_conflict_flag": _safe_bool(assay.get("source_conflict_flag")),
                "source_agreement_band": str(assay.get("source_agreement_band") or "") or None,
                "measurement_source_reference": assay.get("measurement_source_reference"),
                "measurement_source_doi": assay.get("measurement_source_doi"),
                "measurement_source_pubmed_id": assay.get("measurement_source_pubmed_id"),
                "has_features": bool(feat),
                "has_graph_data": bool(node_ids),
            },
        ))

    # Write output
    examples_path = output_dir / "training_examples.json"
    examples_path.write_text(
        json.dumps(
            [ex.model_dump(mode="json") for ex in examples],
            indent=2,
        ),
        encoding="utf-8",
    )

    manifest = {
        "generated_at": generated_at,
        "status": "assembled",
        "layer": "training_example",
        "example_count": len(examples),
        "sources_used": sorted(sources_used),
        "required_sections": [
            "structure", "protein", "ligand",
            "interaction", "experiment", "graph_features",
        ],
        "label_fields": ["binding_affinity_log10", "binding_affinity_raw", "affinity_type", "is_mutant", "delta_delta_g"],
        "notes": (
            f"Assembled {len(examples)} training examples from extracted, "
            f"graph, and feature layers. Sources: {', '.join(sorted(sources_used)) or 'none'}."
        ),
    }
    manifest_path = output_dir / "training_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    return examples_path, manifest_path


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _safe_bool(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"true", "1", "yes"}:
        return True
    if text in {"false", "0", "no"}:
        return False
    return None


# ---------------------------------------------------------------------------
# Legacy compatibility — kept for existing test imports
# ---------------------------------------------------------------------------

from dataclasses import dataclass as _dataclass


@_dataclass(frozen=True)
class TrainingAssemblyPlan:
    """Legacy stub kept for backward compatibility."""

    extracted_dir: Path
    features_dir: Path
    graph_dir: Path
    output_dir: Path
    status: str = "stub"
    notes: str = (
        "Use assemble_training_examples() for actual assembly. "
        "This plan stub is kept for backward compatibility."
    )


def plan_training_assembly(
    extracted_dir: Path,
    features_dir: Path,
    graph_dir: Path,
    output_dir: Path,
) -> TrainingAssemblyPlan:
    return TrainingAssemblyPlan(
        extracted_dir=extracted_dir,
        features_dir=features_dir,
        graph_dir=graph_dir,
        output_dir=output_dir,
    )
