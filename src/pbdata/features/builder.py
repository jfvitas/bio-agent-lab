"""Feature builder for the materialized feature layer.

Produces FeatureRecord objects from:
- extracted multi-table records (entry, chains, bound_objects, interfaces, assays)
- canonical graph (nodes + edges, including external pathway edges)
"""

from __future__ import annotations

import json
import logging
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import gemmi

logger = logging.getLogger(__name__)

_MAX_EAGER_STRUCTURE_FEATURE_ENTRIES = 200

from pbdata.pairing import bound_object_matches_ligand_key, parse_pair_identity_key
from pbdata.schemas.features import FeatureRecord


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


def _load_optional_pair_records(path: Path) -> dict[tuple[str, str], dict[str, Any]]:
    lookup: dict[tuple[str, str], dict[str, Any]] = {}
    for row in _load_json_file(path):
        pair_key = str(row.get("pair_identity_key") or "")
        affinity_type = str(row.get("binding_affinity_type") or "")
        if pair_key:
            lookup[(pair_key, affinity_type)] = row
    return lookup


def _make_feature_manifest(
    output_dir: Path,
    *,
    status: str,
    record_count: int | None = None,
    notes: str,
) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    manifest = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "status": status,
        "layer": "feature",
        "feature_groups": [
            "structure_features",
            "biological_features",
            "graph_features",
            "experimental_features",
            "chemical_descriptors",
            "optional_mm_features",
        ],
        "record_count": record_count,
        "notes": notes,
    }
    out_path = output_dir / "feature_manifest.json"
    out_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return out_path


def build_feature_manifest(output_dir: Path) -> Path:
    """Write a feature-build manifest for the missing feature subsystem."""
    return _make_feature_manifest(
        output_dir,
        status="planned",
        notes=(
            "Architecture scaffold only. Feature computation and materialization "
            "are not implemented yet."
        ),
    )


def _compute_pathway_counts(
    graph_edges: list[dict[str, Any]],
    graph_nodes: list[dict[str, Any]],
) -> dict[str, int]:
    """Count pathways per protein node from ProteinPathway edges.

    Returns {protein_node_id: count_of_distinct_pathway_targets}.
    """
    counts: dict[str, set[str]] = defaultdict(set)
    pathway_node_ids = {
        n.get("node_id") for n in graph_nodes
        if n.get("node_type") == "Pathway"
    }

    for edge in graph_edges:
        if edge.get("edge_type") != "ProteinPathway":
            continue
        src = str(edge.get("source_node_id") or "")
        tgt = str(edge.get("target_node_id") or "")
        # ProteinPathway: source=protein, target=pathway (canonical direction)
        if tgt in pathway_node_ids:
            counts[src].add(tgt)
        elif src in pathway_node_ids:
            logger.warning(
                "ProteinPathway edge has reversed direction: source=%s (Pathway), "
                "target=%s. Counting anyway but upstream data should be fixed.",
                src, tgt,
            )
            counts[tgt].add(src)

    return {node_id: len(pathways) for node_id, pathways in counts.items()}


def _compute_graph_features(
    graph_edges: list[dict[str, Any]],
    graph_nodes: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    """Compute per-node graph features: degree, PPI degree, pathway count, etc.

    Returns {node_id: {feature_name: value}}.
    """
    total_degree: Counter[str] = Counter()
    ppi_degree: Counter[str] = Counter()
    pli_degree: Counter[str] = Counter()

    for edge in graph_edges:
        src = str(edge.get("source_node_id") or "")
        tgt = str(edge.get("target_node_id") or "")
        edge_type = edge.get("edge_type")

        total_degree[src] += 1
        total_degree[tgt] += 1

        if edge_type == "ProteinProteinInteraction":
            ppi_degree[src] += 1
            ppi_degree[tgt] += 1
        elif edge_type == "ProteinLigandInteraction":
            pli_degree[src] += 1
            pli_degree[tgt] += 1

    pathway_counts = _compute_pathway_counts(graph_edges, graph_nodes)

    # Collect all protein node IDs
    protein_node_ids = {
        str(n.get("node_id") or "")
        for n in graph_nodes
        if n.get("node_type") == "Protein"
    }

    # Build features per protein node
    features: dict[str, dict[str, Any]] = {}
    for node_id in protein_node_ids:
        features[node_id] = {
            "network_degree": total_degree.get(node_id, 0),
            "ppi_degree": ppi_degree.get(node_id, 0),
            "pli_degree": pli_degree.get(node_id, 0),
            "pathway_count": pathway_counts.get(node_id, 0),
        }

    return features


def _compute_sequence_features(
    chains: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    """Compute per-chain sequence features.

    Returns {(pdb_id, chain_id): {feature_name: value}}.
    """
    features: dict[str, dict[str, Any]] = {}
    hydropathy_scale = {
        "A": 1.8, "R": -4.5, "N": -3.5, "D": -3.5, "C": 2.5,
        "Q": -3.5, "E": -3.5, "G": -0.4, "H": -3.2, "I": 4.5,
        "L": 3.8, "K": -3.9, "M": 1.9, "F": 2.8, "P": -1.6,
        "S": -0.8, "T": -0.7, "W": -0.9, "Y": -1.3, "V": 4.2,
    }
    aromatic = {"F", "W", "Y", "H"}
    charged = {"D", "E", "K", "R", "H"}
    polar = {"N", "Q", "S", "T", "Y", "C", "H", "D", "E", "K", "R"}
    for chain in chains:
        if not chain.get("is_protein"):
            continue
        pdb_id = str(chain.get("pdb_id") or "")
        chain_id = str(chain.get("chain_id") or "")
        sequence = chain.get("polymer_sequence") or chain.get("entity_sequence") or ""
        key = f"{pdb_id}:{chain_id}"

        seq_len = len(sequence) if sequence else None
        seq_chars = [aa for aa in sequence if aa in hydropathy_scale]
        denom = len(seq_chars) or 1
        features[key] = {
            "sequence_length": seq_len,
            "mean_hydropathy": (
                sum(hydropathy_scale[aa] for aa in seq_chars) / denom
                if seq_chars else None
            ),
            "aromatic_fraction": (
                sum(1 for aa in seq_chars if aa in aromatic) / denom
                if seq_chars else None
            ),
            "charged_fraction": (
                sum(1 for aa in seq_chars if aa in charged) / denom
                if seq_chars else None
            ),
            "polar_fraction": (
                sum(1 for aa in seq_chars if aa in polar) / denom
                if seq_chars else None
            ),
        }
    return features


def _compute_structure_file_features(
    entries: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    """Compute dense continuous structure descriptors from mmCIF coordinates.

    Assumption:
    - We only use local mmCIF coordinates already downloaded for the entry.
    - Electron density maps are not assumed to exist, so B-factor / occupancy
      are used as the reliable continuous crystallographic proxies available
      directly in the structure file.
    """
    features: dict[str, dict[str, Any]] = {}
    for entry in entries:
        pdb_id = str(entry.get("pdb_id") or "")
        cif_path = str(entry.get("structure_file_cif_path") or "")
        if not pdb_id or not cif_path:
            continue
        path = Path(cif_path)
        if not path.exists():
            continue
        try:
            structure = gemmi.read_structure(str(path))
        except Exception as exc:
            logger.warning("Failed to parse structure file %s: %s", path, exc)
            continue

        atom_count = 0
        heavy_atom_count = 0
        occupancies: list[float] = []
        b_factors: list[float] = []
        covalent_radii: list[float] = []
        atomic_weights: list[float] = []
        residue_centroids: dict[tuple[str, str, int, str], tuple[float, float, float]] = {}
        residue_atom_coords: dict[tuple[str, str, int, str], list[tuple[float, float, float]]] = defaultdict(list)

        for model in structure:
            for chain in model:
                for residue in chain:
                    res_key = (str(model.num), str(chain.name), int(residue.seqid.num), str(residue.name))
                    for atom in residue:
                        atom_count += 1
                        occupancies.append(float(atom.occ))
                        b_factors.append(float(atom.b_iso))
                        covalent_radii.append(float(atom.element.covalent_r))
                        atomic_weights.append(float(atom.element.weight))
                        coord = (float(atom.pos.x), float(atom.pos.y), float(atom.pos.z))
                        residue_atom_coords[res_key].append(coord)
                        if not atom.element.is_hydrogen:
                            heavy_atom_count += 1

        for key, coords in residue_atom_coords.items():
            n = len(coords)
            if n:
                residue_centroids[key] = (
                    sum(coord[0] for coord in coords) / n,
                    sum(coord[1] for coord in coords) / n,
                    sum(coord[2] for coord in coords) / n,
                )

        radius_of_gyration = None
        if residue_centroids:
            centroid_values = list(residue_centroids.values())
            cx = sum(x for x, _, _ in centroid_values) / len(centroid_values)
            cy = sum(y for _, y, _ in centroid_values) / len(centroid_values)
            cz = sum(z for _, _, z in centroid_values) / len(centroid_values)
            radius_of_gyration = (
                sum((x - cx) ** 2 + (y - cy) ** 2 + (z - cz) ** 2 for x, y, z in centroid_values) / len(centroid_values)
            ) ** 0.5

        features[pdb_id] = {
            "atom_count_total": atom_count,
            "heavy_atom_fraction": (heavy_atom_count / atom_count) if atom_count else None,
            "mean_atomic_weight": (sum(atomic_weights) / len(atomic_weights)) if atomic_weights else None,
            "mean_covalent_radius": (sum(covalent_radii) / len(covalent_radii)) if covalent_radii else None,
            "mean_b_factor": (sum(b_factors) / len(b_factors)) if b_factors else None,
            "mean_occupancy": (sum(occupancies) / len(occupancies)) if occupancies else None,
            "residue_count_observed": len(residue_centroids),
            "radius_of_gyration_residue_centroids": radius_of_gyration,
        }
    return features


def _aggregate_graph_features_for_pair(
    *,
    parsed_pair,
    protein_chain_to_node: dict[tuple[str, str], str],
    graph_features: dict[str, dict[str, Any]],
    fallback_node_id: str,
) -> dict[str, int]:
    node_ids: list[str] = []
    if parsed_pair is not None and parsed_pair.pdb_id:
        for chain_id in parsed_pair.receptor_chain_ids:
            node_id = protein_chain_to_node.get((parsed_pair.pdb_id, chain_id))
            if node_id and node_id not in node_ids:
                node_ids.append(node_id)
    if not node_ids and fallback_node_id:
        node_ids.append(fallback_node_id)

    return {
        "network_degree": sum(int(graph_features.get(node_id, {}).get("network_degree", 0)) for node_id in node_ids),
        "ppi_degree": sum(int(graph_features.get(node_id, {}).get("ppi_degree", 0)) for node_id in node_ids),
        "pli_degree": sum(int(graph_features.get(node_id, {}).get("pli_degree", 0)) for node_id in node_ids),
        "pathway_count": sum(int(graph_features.get(node_id, {}).get("pathway_count", 0)) for node_id in node_ids),
    }


def build_features_from_extracted_and_graph(
    extracted_dir: Path,
    graph_dir: Path,
    output_dir: Path,
    *,
    microstate_dir: Path | None = None,
    physics_dir: Path | None = None,
) -> tuple[Path, Path]:
    """Materialize features from extracted tables and graph outputs."""
    entries = _load_table_json(extracted_dir / "entry")
    chains = _load_table_json(extracted_dir / "chains")
    bound_objects = _load_table_json(extracted_dir / "bound_objects")
    interfaces = _load_table_json(extracted_dir / "interfaces")
    assays = _load_table_json(extracted_dir / "assays")
    graph_nodes = _load_json_file(graph_dir / "graph_nodes.json")
    graph_edges = _load_json_file(graph_dir / "graph_edges.json")
    microstate_by_pair = _load_optional_pair_records((microstate_dir or output_dir / "microstates") / "microstate_records.json")
    physics_by_pair = _load_optional_pair_records((physics_dir or output_dir / "physics") / "physics_feature_records.json")

    output_dir.mkdir(parents=True, exist_ok=True)

    entry_by_pdb = {str(entry.get("pdb_id") or ""): entry for entry in entries}
    protein_chain_counts: Counter[str] = Counter()
    for chain in chains:
        if chain.get("is_protein"):
            protein_chain_counts[str(chain.get("pdb_id") or "")] += 1

    ligand_counts: Counter[str] = Counter()
    ligand_mw_by_pdb: dict[str, float] = {}
    for bound_object in bound_objects:
        pdb_id = str(bound_object.get("pdb_id") or "")
        ligand_counts[pdb_id] += 1
        if pdb_id not in ligand_mw_by_pdb and bound_object.get("component_molecular_weight") is not None:
            try:
                ligand_mw_by_pdb[pdb_id] = float(bound_object["component_molecular_weight"])
            except (TypeError, ValueError):
                pass

    interface_residue_counts: dict[str, int] = defaultdict(int)
    for interface in interfaces:
        pdb_id = str(interface.get("pdb_id") or "")
        residues = interface.get("binding_site_residue_ids") or []
        interface_residue_counts[pdb_id] += len(residues)

    # Graph-derived features (degree, PPI degree, pathway count, etc.)
    graph_features = _compute_graph_features(graph_edges, graph_nodes)

    # Sequence features
    seq_features = _compute_sequence_features(chains)
    assay_pdb_ids = {str(assay.get("pdb_id") or "") for assay in assays if assay.get("pdb_id")}
    if 0 < len(assay_pdb_ids) <= _MAX_EAGER_STRUCTURE_FEATURE_ENTRIES:
        structure_features = _compute_structure_file_features(
            [entry for entry in entries if str(entry.get("pdb_id") or "") in assay_pdb_ids]
        )
        structure_feature_status = "computed_from_local_cif"
    else:
        structure_features = {}
        structure_feature_status = (
            "skipped_large_dataset"
            if len(assay_pdb_ids) > _MAX_EAGER_STRUCTURE_FEATURE_ENTRIES
            else "no_assay_entries"
        )

    # Map PDB ID → primary protein node ID
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

    # Check if we have any external pathway data
    has_pathway_data = any(
        edge.get("edge_type") == "ProteinPathway" for edge in graph_edges
    )

    feature_rows: list[FeatureRecord] = []
    seen_keys: set[tuple[str, str]] = set()
    generated_at = datetime.now(timezone.utc).isoformat()

    for assay in assays:
        pdb_id = str(assay.get("pdb_id") or "")
        pair_key = str(assay.get("pair_identity_key") or "")
        if not pdb_id or not pair_key:
            continue
        dedupe_key = (pair_key, str(assay.get("binding_affinity_type") or ""))
        if dedupe_key in seen_keys:
            continue
        seen_keys.add(dedupe_key)

        entry = entry_by_pdb.get(pdb_id, {})
        microstate_row = microstate_by_pair.get(dedupe_key, {})
        physics_row = physics_by_pair.get(dedupe_key, {})
        protein_node_id = protein_node_by_pdb.get(pdb_id, "")
        parsed_pair = parse_pair_identity_key(pair_key)
        gf = _aggregate_graph_features_for_pair(
            parsed_pair=parsed_pair,
            protein_chain_to_node=protein_chain_to_node,
            graph_features=graph_features,
            fallback_node_id=protein_node_id,
        )

        # Use the first receptor chain from the pair key when available.
        primary_seq_len = None
        primary_chain_id = None
        receptor_chain_ids = list(parsed_pair.receptor_chain_ids) if parsed_pair is not None else []
        candidate_chain_ids = receptor_chain_ids or [
            str(chain.get("chain_id") or "")
            for chain in chains
            if str(chain.get("pdb_id") or "") == pdb_id and chain.get("is_protein")
        ]
        for chain_id in candidate_chain_ids:
            sf = seq_features.get(f"{pdb_id}:{chain_id}", {})
            if sf.get("sequence_length") is not None:
                primary_seq_len = sf["sequence_length"]
                primary_chain_id = chain_id
                break

        pair_interface_residue_count = 0
        pair_interface_types: set[str] = set()
        for interface in interfaces:
            if str(interface.get("pdb_id") or "") != pdb_id:
                continue
            interface_chain_ids = set(interface.get("binding_site_chain_ids") or [])
            if receptor_chain_ids and not interface_chain_ids.intersection(receptor_chain_ids):
                continue
            if (
                parsed_pair is not None
                and parsed_pair.task_type == "protein_ligand"
                and parsed_pair.ligand_key
                and str(interface.get("entity_name_b") or "")
                and str(interface.get("entity_name_b") or "") != parsed_pair.ligand_key
            ):
                continue
            pair_interface_residue_count += len(interface.get("binding_site_residue_ids") or [])
            if interface.get("interface_type"):
                pair_interface_types.add(str(interface.get("interface_type")))

        ligand_mw = None
        ligand_component_type = None
        ligand_is_covalent = None
        ligand_inchikey = None
        if parsed_pair is not None and parsed_pair.ligand_key:
            for bound_object in bound_objects:
                if str(bound_object.get("pdb_id") or "") != pdb_id:
                    continue
                if not bound_object_matches_ligand_key(bound_object, parsed_pair.ligand_key):
                    continue
                try:
                    ligand_mw = float(bound_object["component_molecular_weight"])
                except (KeyError, TypeError, ValueError):
                    ligand_mw = None
                ligand_component_type = bound_object.get("component_type")
                ligand_is_covalent = bound_object.get("is_covalent")
                ligand_inchikey = bound_object.get("component_inchikey")
                break
        if ligand_mw is None:
            ligand_mw = ligand_mw_by_pdb.get(pdb_id)

        pathway_count = gf.get("pathway_count", 0)
        pathway_status = (
            "from_reactome" if has_pathway_data
            else "unknown_external_pathway_sources_not_ingested"
        )

        feature_rows.append(FeatureRecord(
            feature_id=f"features:{pdb_id}:{len(feature_rows)}",
            pdb_id=pdb_id,
            pair_identity_key=pair_key,
            feature_group="training_ready_core",
            values={
                # Structure features
                "structure_resolution": entry.get("structure_resolution"),
                "atom_count_total": structure_features.get(pdb_id, {}).get("atom_count_total"),
                "heavy_atom_fraction": structure_features.get(pdb_id, {}).get("heavy_atom_fraction"),
                "mean_atomic_weight": structure_features.get(pdb_id, {}).get("mean_atomic_weight"),
                "mean_covalent_radius": structure_features.get(pdb_id, {}).get("mean_covalent_radius"),
                "mean_b_factor": structure_features.get(pdb_id, {}).get("mean_b_factor"),
                "mean_occupancy": structure_features.get(pdb_id, {}).get("mean_occupancy"),
                "residue_count_observed": structure_features.get(pdb_id, {}).get("residue_count_observed"),
                "radius_of_gyration_residue_centroids": structure_features.get(pdb_id, {}).get("radius_of_gyration_residue_centroids"),
                "protein_chain_count": protein_chain_counts.get(pdb_id, 0),
                "ligand_count": ligand_counts.get(pdb_id, 0),
                "multiligand_entry": bool(entry.get("multiligand_entry")),
                "interface_residue_count": (
                    pair_interface_residue_count
                    if parsed_pair is not None
                    else interface_residue_counts.get(pdb_id, 0)
                ),
                "interface_types": sorted(pair_interface_types),
                "receptor_chain_count": len(receptor_chain_ids),
                # Sequence features
                "sequence_length": primary_seq_len,
                "protein_mean_hydropathy": (
                    seq_features.get(f"{pdb_id}:{primary_chain_id}", {}).get("mean_hydropathy")
                    if primary_chain_id else None
                ),
                "protein_aromatic_fraction": (
                    seq_features.get(f"{pdb_id}:{primary_chain_id}", {}).get("aromatic_fraction")
                    if primary_chain_id else None
                ),
                "protein_charged_fraction": (
                    seq_features.get(f"{pdb_id}:{primary_chain_id}", {}).get("charged_fraction")
                    if primary_chain_id else None
                ),
                "protein_polar_fraction": (
                    seq_features.get(f"{pdb_id}:{primary_chain_id}", {}).get("polar_fraction")
                    if primary_chain_id else None
                ),
                # Experimental features
                "assay_source_database": assay.get("source_database"),
                "preferred_source_database": assay.get("selected_preferred_source"),
                "binding_affinity_type": assay.get("binding_affinity_type"),
                "binding_affinity_value": assay.get("binding_affinity_value"),
                "binding_affinity_log10_standardized": assay.get("binding_affinity_log10_standardized"),
                "binding_affinity_is_mutant_measurement": assay.get("binding_affinity_is_mutant_measurement"),
                "reported_measurement_count": assay.get("reported_measurement_count"),
                "source_conflict_flag": assay.get("source_conflict_flag"),
                "source_agreement_band": assay.get("source_agreement_band"),
                "assay_temperature_c": assay.get("assay_temperature_c"),
                "assay_ph": assay.get("assay_ph"),
                # Graph features
                "network_degree": gf.get("network_degree", 0),
                "ppi_degree": gf.get("ppi_degree", 0),
                "pli_degree": gf.get("pli_degree", 0),
                "pathway_count": pathway_count,
                # Chemical features
                "ligand_molecular_weight": ligand_mw,
                "ligand_component_type": ligand_component_type,
                "ligand_inchikey": ligand_inchikey,
                "ligand_is_covalent": ligand_is_covalent,
                # Entry-level biological flags
                "metal_present": entry.get("metal_present"),
                "cofactor_present": entry.get("cofactor_present"),
                "glycan_present": entry.get("glycan_present"),
                "covalent_binder_present": entry.get("covalent_binder_present"),
                "peptide_partner_present": entry.get("peptide_partner_present"),
                "membrane_vs_soluble": entry.get("membrane_vs_soluble"),
                "quality_score": entry.get("quality_score"),
                # Microstate / local physics features
                "microstate_record_count": microstate_row.get("record_count"),
                "estimated_net_charge": physics_row.get("estimated_net_charge"),
                "mean_abs_residue_charge": physics_row.get("mean_abs_residue_charge"),
                "positive_residue_count": physics_row.get("positive_residue_count"),
                "negative_residue_count": physics_row.get("negative_residue_count"),
                "same_charge_contact_count": physics_row.get("same_charge_contact_count"),
                "opposite_charge_contact_count": physics_row.get("opposite_charge_contact_count"),
                "metal_contact_count": physics_row.get("metal_contact_count"),
                "acidic_cluster_penalty": physics_row.get("acidic_cluster_penalty"),
                "local_electrostatic_balance": physics_row.get("local_electrostatic_balance"),
            },
            provenance={
                "generated_at": generated_at,
                "source_tables": ["entry", "chains", "bound_objects", "interfaces", "assays", "graph_edges"],
                "pathway_count_status": pathway_status,
                "structure_descriptor_status": structure_feature_status,
                "microstate_status": "present" if microstate_row else "not_ingested",
                "physics_feature_status": "present" if physics_row else "not_ingested",
            },
        ))

    features_path = output_dir / "feature_records.json"
    features_path.write_text(
        json.dumps([row.model_dump(mode="json") for row in feature_rows], indent=2),
        encoding="utf-8",
    )
    manifest_path = _make_feature_manifest(
        output_dir,
        status="materialized_from_extracted_and_graph",
        record_count=len(feature_rows),
        notes=(
            "Feature layer materialized from extracted tables and canonical graph. "
            f"Pathway data {'present' if has_pathway_data else 'not yet ingested'}. "
            f"Dense structure descriptors: {structure_feature_status}."
        ),
    )
    return features_path, manifest_path
