"""Site-centric feature pipeline execution under the new artifact contract.

Assumptions:
- This pipeline is additive to the legacy `data/` pipeline and does not replace
  it yet.
- Version 1 uses deterministic site extraction and explicit degraded-mode
  physics proxies until offline labels and a surrogate checkpoint are available.
- Canonical inputs are resolved from existing extracted tables so the pipeline
  can run immediately after Extract.
"""

from __future__ import annotations

import json
import hashlib
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import gemmi
import pandas as pd

try:
    import torch
except ModuleNotFoundError:  # torch is optional; degrade gracefully
    torch = None  # type: ignore[assignment]

from pbdata.pipeline.physics_feedback import (
    load_latest_site_physics_surrogate,
    predict_site_physics_from_surrogate,
)
from pbdata.pipeline.feature_post_pipeline import (
    build_analysis_queue_batches,
    build_archetype_rows,
    build_cluster_summary_rows,
    build_representative_archetype_rows,
    export_representative_fragment_inputs,
    write_analysis_queue_yaml,
)
from pbdata.pipeline.feature_pipeline_stages import (
    FEATURE_PIPELINE_STAGE_NAMES,
    build_feature_pipeline_stages,
)
from pbdata.pipeline.feature_pipeline_runtime import (
    append_structured_error as _append_structured_error,
    default_run_id as _default_run_id,
    json_dump as _json_dump,
    load_stage_status as _load_stage_status,
    run_stage as _run_stage,
    stage_manifest_path as _stage_manifest_path,
    structured_error_path as _structured_error_path,
    utc_now as _utc_now,
    write_df as _write_df,
    write_input_manifest as _write_input_manifest,
    write_stage_status as _write_stage_status,
)
from pbdata.storage import StorageLayout
from pbdata.table_io import read_dataframe

PIPELINE_VERSION = "site_feature_pipeline_v1"
SITE_PHYSICS_SPEC_VERSION = "1.0"
GRAPH_REPRESENTATION_VERSION = "site_graph_v1"
TRAINING_EXAMPLE_VERSION = "site_training_example_v1"
SCHEMA_VERSION = "canonical_schema_v1"

_SHELLS: tuple[tuple[str, float, float], ...] = (
    ("shell_1", 0.0, 3.5),
    ("shell_2", 3.5, 6.0),
    ("shell_3", 6.0, 8.0),
)
_AA_CODES = {
    "ALA", "ARG", "ASN", "ASP", "CYS", "GLU", "GLN", "GLY", "HIS", "ILE",
    "LEU", "LYS", "MET", "PHE", "PRO", "SER", "THR", "TRP", "TYR", "VAL",
}
_METAL_ELEMENTS = {"ZN", "FE", "MG", "MN", "CA", "CU", "CO", "NI"}
_POSITIVE_RESIDUES = {"LYS", "ARG", "HIS"}
_NEGATIVE_RESIDUES = {"ASP", "GLU"}
_POLAR_ELEMENTS = {"N", "O", "S", "P"}
_CHARGED_ELEMENTS = {"N", "O"}


@dataclass(frozen=True)
class FeaturePipelineConfig:
    run_id: str
    run_mode: str = "full_build"
    stage_only: str | None = None
    degraded_mode: bool = True
    fail_hard: bool = False
    gpu_enabled: bool = False
    cpu_workers: int = 1

def _read_json_rows(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    raw = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(raw, list):
        return [item for item in raw if isinstance(item, dict)]
    return [raw] if isinstance(raw, dict) else []


def _load_table_rows(table_dir: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not table_dir.exists():
        return rows
    for path in sorted(table_dir.glob("*.json")):
        rows.extend(_read_json_rows(path))
    return rows

def _canonical_input_rows(layout: StorageLayout) -> dict[str, list[dict[str, Any]]]:
    return {
        "entry": _load_table_rows(layout.extracted_dir / "entry"),
        "chain": _load_table_rows(layout.extracted_dir / "chains"),
        "bound_object": _load_table_rows(layout.extracted_dir / "bound_objects"),
        "interface": _load_table_rows(layout.extracted_dir / "interfaces"),
        "assay": _load_table_rows(layout.extracted_dir / "assays"),
        "provenance": _load_table_rows(layout.extracted_dir / "provenance"),
    }

def _element_charge_proxy(element: str, residue_name: str) -> float:
    if residue_name in _NEGATIVE_RESIDUES and element == "O":
        return -0.5
    if residue_name in _POSITIVE_RESIDUES and element == "N":
        return 0.5
    if element in _METAL_ELEMENTS:
        return 1.0
    return 0.0


def _distance(a: tuple[float, float, float], b: tuple[float, float, float]) -> float:
    return ((a[0] - b[0]) ** 2 + (a[1] - b[1]) ** 2 + (a[2] - b[2]) ** 2) ** 0.5


def _site_id(pdb_id: str, chain_id: str, residue_name: str, residue_number: int, atom_name: str, motif_class: str) -> str:
    return "|".join([pdb_id, chain_id or "-", residue_name, str(residue_number), atom_name, motif_class])


def _protein_atom_motif(residue_name: str, atom_name: str) -> str | None:
    residue_name = residue_name.upper()
    atom_name = atom_name.upper()
    if atom_name == "O":
        return "backbone_carbonyl_oxygen"
    if atom_name == "N":
        return "backbone_amide_nitrogen"
    mapping = {
        ("ASP", "OD1"): "asp_carboxylate_oxygen",
        ("ASP", "OD2"): "asp_carboxylate_oxygen",
        ("GLU", "OE1"): "glu_carboxylate_oxygen",
        ("GLU", "OE2"): "glu_carboxylate_oxygen",
        ("ASN", "OD1"): "asn_carbonyl_oxygen",
        ("ASN", "ND2"): "asn_amide_nitrogen",
        ("GLN", "OE1"): "gln_carbonyl_oxygen",
        ("GLN", "NE2"): "gln_amide_nitrogen",
        ("SER", "OG"): "ser_hydroxyl_oxygen",
        ("THR", "OG1"): "thr_hydroxyl_oxygen",
        ("TYR", "OH"): "tyr_hydroxyl_oxygen",
        ("LYS", "NZ"): "lys_terminal_amine_nitrogen",
        ("ARG", "NH1"): "arg_terminal_nitrogen",
        ("ARG", "NH2"): "arg_terminal_nitrogen",
        ("ARG", "NE"): "arg_central_nitrogen",
        ("HIS", "ND1"): "his_delta_nitrogen",
        ("HIS", "NE2"): "his_epsilon_nitrogen",
        ("CYS", "SG"): "cys_sulfur",
        ("MET", "SD"): "met_sulfur",
        ("TRP", "NE1"): "trp_indole_nitrogen",
    }
    return mapping.get((residue_name, atom_name))


def _ligand_atom_motif(element: str) -> str | None:
    return {
        "O": "carbonyl_oxygen",
        "N": "amine_nitrogen",
        "S": "thioether_sulfur",
        "F": "halogen_atom",
        "CL": "halogen_atom",
        "BR": "halogen_atom",
        "I": "halogen_atom",
    }.get(element.upper())


def _site_candidates_from_structure(pdb_id: str, structure_path: Path) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    structure = gemmi.read_structure(str(structure_path))
    atom_rows: list[dict[str, Any]] = []
    site_rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for model in structure:
        for chain in model:
            for residue in chain:
                residue_name = residue.name.strip().upper()
                chain_id = str(chain.name)
                residue_number = int(residue.seqid.num)
                is_protein = residue_name in _AA_CODES
                is_metal = residue_name in _METAL_ELEMENTS
                is_ligand = not is_protein and not is_metal and residue_name not in {"HOH", "WAT"}
                ring_coords: list[tuple[float, float, float]] = []
                if residue_name in {"PHE", "TYR", "TRP", "HIS"}:
                    ring_coords = [(float(a.pos.x), float(a.pos.y), float(a.pos.z)) for a in residue]
                for atom in residue:
                    atom_name = atom.name.strip().upper()
                    element = atom.element.name.upper()
                    coord = (float(atom.pos.x), float(atom.pos.y), float(atom.pos.z))
                    atom_rows.append({
                        "pdb_id": pdb_id, "chain_id": chain_id, "residue_name": residue_name, "residue_number": residue_number,
                        "atom_name": atom_name, "element": element, "x": coord[0], "y": coord[1], "z": coord[2],
                        "is_heavy": not atom.element.is_hydrogen, "is_polar": element in _POLAR_ELEMENTS,
                        "is_charged": element in _CHARGED_ELEMENTS or residue_name in _POSITIVE_RESIDUES or residue_name in _NEGATIVE_RESIDUES,
                        "partial_charge_proxy": _element_charge_proxy(element, residue_name),
                        "is_aromatic_like": residue_name in {"PHE", "TYR", "TRP", "HIS"}, "is_metal": is_metal or element in _METAL_ELEMENTS,
                    })
                    motif_class = None
                    source_kind = None
                    if is_protein:
                        motif_class = _protein_atom_motif(residue_name, atom_name)
                        source_kind = "protein"
                    elif is_metal or element in _METAL_ELEMENTS:
                        motif_class = "metal_ion"
                        source_kind = "metal"
                    elif is_ligand:
                        motif_class = _ligand_atom_motif(element)
                        source_kind = "ligand"
                    if motif_class:
                        site_id = _site_id(pdb_id, chain_id, residue_name, residue_number, atom_name, motif_class)
                        if site_id not in seen:
                            seen.add(site_id)
                            site_rows.append({
                                "record_id": pdb_id, "pdb_id": pdb_id, "site_id": site_id, "chain_id": chain_id,
                                "residue_name": residue_name, "residue_number": residue_number, "atom_name": atom_name,
                                "atomic_number": atom.element.atomic_number, "formal_charge": _element_charge_proxy(element, residue_name),
                                "initial_partial_charge": _element_charge_proxy(element, residue_name), "motif_class": motif_class,
                                "source_kind": source_kind, "x": coord[0], "y": coord[1], "z": coord[2],
                                "aromatic_flag": residue_name in {"PHE", "TYR", "TRP", "HIS"},
                                "ring_membership": residue_name in {"PHE", "TYR", "TRP", "HIS"},
                                "backbone_vs_sidechain": "backbone" if motif_class.startswith("backbone") else "sidechain",
                                "interface_flag": None,
                            })
                if ring_coords and residue_name in {"PHE", "TYR", "TRP", "HIS"}:
                    motif_class = {"PHE": "phe_aromatic_centroid", "TYR": "tyr_aromatic_centroid", "TRP": "trp_aromatic_centroid", "HIS": "his_aromatic_centroid"}[residue_name]
                    centroid = tuple(sum(coord[i] for coord in ring_coords) / len(ring_coords) for i in range(3))
                    site_id = _site_id(pdb_id, chain_id, residue_name, residue_number, "CENTROID", motif_class)
                    if site_id not in seen:
                        seen.add(site_id)
                        site_rows.append({
                            "record_id": pdb_id, "pdb_id": pdb_id, "site_id": site_id, "chain_id": chain_id, "residue_name": residue_name,
                            "residue_number": residue_number, "atom_name": "CENTROID", "atomic_number": None, "formal_charge": 0.0,
                            "initial_partial_charge": 0.0, "motif_class": motif_class, "source_kind": "protein",
                            "x": centroid[0], "y": centroid[1], "z": centroid[2], "aromatic_flag": True, "ring_membership": True,
                            "backbone_vs_sidechain": "sidechain", "interface_flag": None,
                        })
    return atom_rows, site_rows


def _shell_descriptor_rows(record_id: str, sites: list[dict[str, Any]], atoms: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    env_rows: list[dict[str, Any]] = []
    node_rows: list[dict[str, Any]] = []
    for site in sites:
        center = (float(site["x"]), float(site["y"]), float(site["z"]))
        node_rows.append({
            "record_id": record_id,
            "site_id": site["site_id"],
            "motif_class": site["motif_class"],
            "atomic_number": site["atomic_number"],
            "formal_charge": site["formal_charge"],
            "initial_partial_charge": site["initial_partial_charge"],
            "sasa": None,
            "burial_score": None,
            "b_factor": None,
            "occupancy": None,
            "interface_flag": site["interface_flag"],
        })
        for shell_name, start, end in _SHELLS:
            shell_neighbors = []
            for atom in atoms:
                atom_center = (float(atom["x"]), float(atom["y"]), float(atom["z"]))
                d = _distance(center, atom_center)
                if d == 0.0 or d < start or d > end:
                    continue
                shell_neighbors.append((atom, d))
            charges = [float(atom["partial_charge_proxy"]) for atom, _ in shell_neighbors]
            positive = [value for value in charges if value > 0]
            negative = [value for value in charges if value < 0]
            env_rows.append({
                "record_id": record_id,
                "site_id": site["site_id"],
                "motif_class": site["motif_class"],
                "shell_name": shell_name,
                "shell_start": start,
                "shell_end": end,
                "neighbor_atom_count": len(shell_neighbors),
                "heavy_atom_count": sum(1 for atom, _ in shell_neighbors if bool(atom["is_heavy"])),
                "polar_atom_count": sum(1 for atom, _ in shell_neighbors if bool(atom["is_polar"])),
                "charged_atom_count": sum(1 for atom, _ in shell_neighbors if bool(atom["is_charged"])),
                "aromatic_centroid_count": sum(1 for atom, _ in shell_neighbors if bool(atom["is_aromatic_like"])),
                "metal_count": sum(1 for atom, _ in shell_neighbors if bool(atom["is_metal"])),
                "nearest_neighbor_distance": min((d for _, d in shell_neighbors), default=None),
                "sum_partial_charge": round(sum(charges), 6),
                "sum_positive_charge": round(sum(positive), 6),
                "sum_negative_charge": round(sum(negative), 6),
                "inverse_distance_charge_sum": round(sum(charge / max(d, 0.1) for charge, (_, d) in zip(charges, shell_neighbors)), 6) if shell_neighbors else 0.0,
                "inverse_square_charge_sum": round(sum(charge / max(d * d, 0.1) for charge, (_, d) in zip(charges, shell_neighbors)), 6) if shell_neighbors else 0.0,
                "electric_field_magnitude": round(abs(sum(charges)), 6),
                "electrostatic_potential_proxy": round(sum(charges), 6),
                "donor_count": sum(1 for atom, _ in shell_neighbors if str(atom["element"]) == "N"),
                "acceptor_count": sum(1 for atom, _ in shell_neighbors if str(atom["element"]) == "O"),
                "hbond_candidate_count": sum(1 for atom, _ in shell_neighbors if str(atom["element"]) in {"N", "O"}),
                "intramolecular_hbond_satisfied_flag": None,
                "sasa_site": None,
                "sasa_residue": None,
                "burial_score": None,
                "pocket_score": None,
                "solvent_distance": None,
                "normalized_b_factor": None,
                "occupancy": None,
                "sidechain_rotamer_class": None,
                "backbone_phi": None,
                "backbone_psi": None,
                "secondary_structure_class": None,
                "residue_depth": None,
                "interface_flag": None,
                "degraded_mode": True,
            })
    return env_rows, node_rows


def _edge_rows_from_sites(record_id: str, sites: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for index, source in enumerate(sites):
        a = (float(source["x"]), float(source["y"]), float(source["z"]))
        for target in sites[index + 1:]:
            b = (float(target["x"]), float(target["y"]), float(target["z"]))
            distance = _distance(a, b)
            if distance > 8.0:
                continue
            rows.append({
                "record_id": record_id,
                "source_site_id": source["site_id"],
                "target_site_id": target["site_id"],
                "distance": round(distance, 6),
                "orientation_angle": None,
            })
    return rows


def _site_refined_rows(env_rows: list[dict[str, Any]], *, degraded_mode: bool) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in env_rows:
        grouped[str(row["site_id"])].append(row)
    refined_rows: list[dict[str, Any]] = []
    provenance_rows: list[dict[str, Any]] = []
    seen_hashes: set[str] = set()
    cache_hits = 0
    cache_misses = 0
    for site_id, rows in grouped.items():
        motif_class = str(rows[0]["motif_class"])
        env_hash = hashlib.sha256(json.dumps({
            "motif_class": motif_class,
            "shells": [{k: row[k] for k in ("shell_name", "neighbor_atom_count", "sum_partial_charge", "electric_field_magnitude", "donor_count", "acceptor_count")} for row in rows],
        }, sort_keys=True).encode("utf-8")).hexdigest()
        if env_hash in seen_hashes:
            cache_hits += 1
        else:
            cache_misses += 1
            seen_hashes.add(env_hash)
        total_charge = sum(float(row["sum_partial_charge"]) for row in rows)
        donor = sum(int(row["donor_count"]) for row in rows)
        acceptor = sum(int(row["acceptor_count"]) for row in rows)
        metals = sum(int(row["metal_count"]) for row in rows)
        aromatics = sum(int(row["aromatic_centroid_count"]) for row in rows)
        field = max(float(row["electric_field_magnitude"]) for row in rows)
        refined_rows.append({
            "record_id": str(rows[0]["record_id"]),
            "site_id": site_id,
            "motif_class": motif_class,
            "refined_partial_charge": round(total_charge / max(len(rows), 1), 6),
            "electrostatic_potential": round(total_charge, 6),
            "electric_field_magnitude": round(field, 6),
            "donor_strength": round(donor / max(len(rows), 1), 6),
            "acceptor_strength": round(acceptor / max(len(rows), 1), 6),
            "polarizability_proxy": round((donor + acceptor + aromatics) / max(len(rows), 1), 6),
            "steric_radius": round(max(float(row["neighbor_atom_count"]) for row in rows) / 10.0, 6),
            "desolvation_penalty": round(max(0.0, -total_charge), 6),
            "protonation_preference": round(total_charge / max(len(rows), 1), 6),
            "metal_binding_propensity": round(min(1.0, metals / max(len(rows), 1)), 6),
            "aromatic_interaction_propensity": round(min(1.0, aromatics / max(len(rows), 1)), 6),
            "local_environment_strain_score": round(max(0.0, len(rows) - donor - acceptor) / max(len(rows), 1), 6),
            "environment_hash": env_hash,
            "degraded_mode": degraded_mode,
        })
        provenance_rows.append({
            "record_id": str(rows[0]["record_id"]),
            "site_id": site_id,
            "motif_class": motif_class,
            "environment_hash": env_hash,
            "source_analysis_methods": "proxy_from_environment_vectors" if degraded_mode else "surrogate_inference",
            "target_quality_flag": "degraded_proxy" if degraded_mode else "surrogate_predicted",
            "provenance_json": json.dumps({"pipeline_version": PIPELINE_VERSION, "site_physics_spec_version": SITE_PHYSICS_SPEC_VERSION, "degraded_mode": degraded_mode}, sort_keys=True),
        })
    return refined_rows, provenance_rows, {"cache_hits": cache_hits, "cache_misses": cache_misses, "degraded_mode": degraded_mode}


def _site_feature_values_for_surrogate(rows: list[dict[str, Any]]) -> dict[str, float]:
    values: dict[str, float] = {}
    for row in rows:
        prefix = str(row["shell_name"])
        for key in (
            "neighbor_atom_count",
            "heavy_atom_count",
            "polar_atom_count",
            "charged_atom_count",
            "aromatic_centroid_count",
            "metal_count",
            "sum_partial_charge",
            "electric_field_magnitude",
            "donor_count",
            "acceptor_count",
        ):
            values[f"{prefix}.{key}"] = float(row.get(key) or 0.0)
    return values


def _site_refined_rows_from_surrogate(env_rows: list[dict[str, Any]], model: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in env_rows:
        grouped[str(row["site_id"])].append(row)
    refined_rows: list[dict[str, Any]] = []
    provenance_rows: list[dict[str, Any]] = []
    for site_id, rows in grouped.items():
        motif_class = str(rows[0]["motif_class"])
        predictions = predict_site_physics_from_surrogate(
            model,
            motif_class=motif_class,
            feature_values=_site_feature_values_for_surrogate(rows),
        )
        refined_rows.append({
            "record_id": str(rows[0]["record_id"]),
            "site_id": site_id,
            "motif_class": motif_class,
            **predictions,
            "environment_hash": hashlib.sha256(json.dumps(_site_feature_values_for_surrogate(rows), sort_keys=True).encode("utf-8")).hexdigest(),
            "degraded_mode": False,
        })
        provenance_rows.append({
            "record_id": str(rows[0]["record_id"]),
            "site_id": site_id,
            "motif_class": motif_class,
            "environment_hash": hashlib.sha256(json.dumps(_site_feature_values_for_surrogate(rows), sort_keys=True).encode("utf-8")).hexdigest(),
            "source_analysis_methods": "surrogate_inference",
            "target_quality_flag": "surrogate_predicted",
            "provenance_json": json.dumps({
                "pipeline_version": PIPELINE_VERSION,
                "site_physics_spec_version": SITE_PHYSICS_SPEC_VERSION,
                "degraded_mode": False,
                "surrogate_version": str(model.get("version") or ""),
            }, sort_keys=True),
        })
    return refined_rows, provenance_rows, {"cache_hits": 0, "cache_misses": len(grouped), "degraded_mode": False, "surrogate_version": str(model.get("version") or "")}


def _graph_rows(node_rows: list[dict[str, Any]], edge_rows: list[dict[str, Any]], refined_rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    refined_by_site = {str(row["site_id"]): row for row in refined_rows}
    graph_nodes = [{**row, **refined_by_site.get(str(row["site_id"]), {})} for row in node_rows]
    graph_edges: list[dict[str, Any]] = []
    for row in edge_rows:
        source = refined_by_site.get(str(row["source_site_id"]), {})
        target = refined_by_site.get(str(row["target_site_id"]), {})
        distance = float(row["distance"])
        source_charge = float(source.get("refined_partial_charge") or 0.0)
        target_charge = float(target.get("refined_partial_charge") or 0.0)
        graph_edges.append({
            **row,
            "coulombic_proxy": round((source_charge * target_charge) / max(distance, 0.1), 6),
            "hbond_geometry_score": round(max(0.0, 1.0 - abs(distance - 3.0) / 3.0), 6),
            "salt_bridge_score": round(1.0 if source_charge * target_charge < 0 and distance < 4.5 else 0.0, 6),
            "vdW_overlap_score": round(max(0.0, 1.0 - distance / 8.0), 6),
            "steric_clash_score": round(1.0 if distance < 2.0 else 0.0, 6),
            "aromatic_stack_score": round(1.0 if "aromatic_centroid" in str(source.get("motif_class") or "") and "aromatic_centroid" in str(target.get("motif_class") or "") and distance < 5.0 else 0.0, 6),
            "metal_bridge_flag": bool(str(source.get("motif_class") or "") == "metal_ion" or str(target.get("motif_class") or "") == "metal_ion"),
        })
    return graph_nodes, graph_edges, {"node_count": len(graph_nodes), "edge_count": len(graph_edges), "graph_representation_version": GRAPH_REPRESENTATION_VERSION}


def _training_example_rows(record_id: str, pair_rows: list[dict[str, Any]], graph_meta: dict[str, Any], degraded_mode: bool) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    example_rows: list[dict[str, Any]] = []
    label_rows: list[dict[str, Any]] = []
    meta_rows: list[dict[str, Any]] = []
    if not pair_rows:
        return [], [], [{"record_id": record_id, "task_type": "inference_ready_only", "supervised_label_available": False, "degraded_mode": degraded_mode}]
    for index, row in enumerate(pair_rows):
        pair_key = str(row.get("pair_identity_key") or "")
        affinity_type = str(row.get("binding_affinity_type") or "")
        example_id = f"{record_id}:{index}"
        label_value = row.get("binding_affinity_log10_standardized") or row.get("reported_measurement_mean_log10_standardized")
        supervised = label_value not in (None, "")
        label_rows.append({"record_id": record_id, "example_id": example_id, "pair_identity_key": pair_key, "KD": label_value if affinity_type == "Kd" else None, "delta_G": row.get("delta_delta_g") or None, "class_label": affinity_type or None, "off_target_label": None})
        meta_rows.append({"record_id": record_id, "example_id": example_id, "pair_identity_key": pair_key, "task_type": str(pair_key.split("|", 1)[0] if pair_key else "unknown"), "split_group_id": pair_key, "supervised_label_available": supervised, "degraded_mode": degraded_mode, "graph_node_count": graph_meta.get("node_count"), "graph_edge_count": graph_meta.get("edge_count")})
        if supervised:
            example_rows.append({"record_id": record_id, "example_id": example_id, "task_type": str(pair_key.split("|", 1)[0] if pair_key else "unknown"), "label_type": affinity_type or None, "degraded_mode": degraded_mode})
    return example_rows, label_rows, meta_rows


def run_feature_pipeline(
    layout: StorageLayout,
    *,
    run_mode: str = "full_build",
    stage_only: str | None = None,
    run_id: str | None = None,
    degraded_mode: bool = True,
    fail_hard: bool = False,
    gpu_enabled: bool = False,
    cpu_workers: int = 1,
) -> dict[str, Any]:
    config = FeaturePipelineConfig(
        run_id=run_id or _default_run_id(),
        run_mode=run_mode,
        stage_only=stage_only,
        degraded_mode=degraded_mode,
        fail_hard=fail_hard,
        gpu_enabled=gpu_enabled,
        cpu_workers=cpu_workers,
    )
    rows = _canonical_input_rows(layout)
    input_manifest = _write_input_manifest(
        layout,
        config=config,
        row_counts={key: len(value) for key, value in rows.items()},
        schema_version=SCHEMA_VERSION,
        pipeline_version=PIPELINE_VERSION,
        site_physics_spec_version=SITE_PHYSICS_SPEC_VERSION,
        graph_representation_version=GRAPH_REPRESENTATION_VERSION,
        training_example_version=TRAINING_EXAMPLE_VERSION,
    )
    stages = build_feature_pipeline_stages(
        layout,
        config=config,
        rows=rows,
        helpers={
            "utc_now": _utc_now,
            "json_dump": _json_dump,
            "write_df": _write_df,
            "load_stage_status": _load_stage_status,
            "site_candidates_from_structure": _site_candidates_from_structure,
            "shell_descriptor_rows": _shell_descriptor_rows,
            "edge_rows_from_sites": _edge_rows_from_sites,
            "site_refined_rows": _site_refined_rows,
            "site_refined_rows_from_surrogate": _site_refined_rows_from_surrogate,
            "load_latest_site_physics_surrogate": load_latest_site_physics_surrogate,
            "graph_rows": _graph_rows,
            "training_example_rows": _training_example_rows,
            "torch": torch,
            "schema_version": SCHEMA_VERSION,
            "pipeline_version": PIPELINE_VERSION,
            "site_physics_spec_version": SITE_PHYSICS_SPEC_VERSION,
            "graph_representation_version": GRAPH_REPRESENTATION_VERSION,
            "training_example_version": TRAINING_EXAMPLE_VERSION,
        },
    )
    if config.run_mode == "stage_only":
        if not config.stage_only:
            raise ValueError("stage_only run mode requires --stage-name")
        stages = [stage for stage in stages if stage[0] == config.stage_only]
        if not stages:
            raise ValueError(
                f"Unknown stage_only target: {config.stage_only}. "
                f"Valid stage names: {', '.join(FEATURE_PIPELINE_STAGE_NAMES)}"
            )
        for dependency in stages[0][1]:
            dep_status = _load_stage_status(layout, config.run_id, dependency)
            if dep_status is None or str(dep_status.get("status") or "") != "passed":
                raise ValueError(f"stage_only requires completed dependency: {dependency}")
    statuses: dict[str, str] = {}
    for stage_name, dependencies, runner in stages:
        result = _run_stage(layout, config, stage_name=stage_name, dependencies=dependencies, runner=runner)
        statuses[stage_name] = str(result["status"])
        if result["status"] == "failed" and config.fail_hard:
            break
    return {"run_id": config.run_id, "input_manifest": str(input_manifest), "stage_statuses": statuses, "artifacts_root": str(layout.artifacts_dir)}


def export_analysis_queue(layout: StorageLayout, *, run_id: str) -> dict[str, str]:
    archetype_rows = build_archetype_rows(layout, run_id=run_id)
    archetype_df = pd.DataFrame(archetype_rows)
    archetype_path = _write_df(layout.archetypes_artifacts_dir / run_id / "archetypes.parquet", archetype_df.drop_duplicates(subset=["archetype_id"]).to_dict(orient="records") if not archetype_df.empty else [])
    representative_rows = build_representative_archetype_rows(archetype_df)
    representative_path = _write_df(layout.archetypes_artifacts_dir / run_id / "representative_archetypes.parquet", representative_rows)
    cluster_summary_rows = build_cluster_summary_rows(archetype_df)
    cluster_summary_path = _write_df(layout.archetypes_artifacts_dir / run_id / "cluster_summary.parquet", cluster_summary_rows)
    fragment_rows = export_representative_fragment_inputs(layout, run_id=run_id, representative_rows=representative_rows)
    fragment_manifest_path = _write_df(layout.archetypes_artifacts_dir / run_id / "representative_fragments.parquet", fragment_rows)
    queue_rows = build_analysis_queue_batches(archetype_df)
    fragment_by_archetype = {str(row["archetype_id"]): row for row in fragment_rows}
    for queue_row in queue_rows:
        queue_row["fragments"] = [
            {
                "archetype_id": archetype_id,
                "fragment_id": fragment_by_archetype[archetype_id]["fragment_id"],
                "fragment_file": fragment_by_archetype[archetype_id]["fragment_file"],
                "metadata_file": fragment_by_archetype[archetype_id]["metadata_file"],
            }
            for archetype_id in queue_row["archetype_ids"]
            if archetype_id in fragment_by_archetype
        ]
    queue_path = layout.external_analysis_artifacts_dir / f"{run_id}_analysis_queue.yaml"
    write_analysis_queue_yaml(queue_path, run_id=run_id, queue_rows=queue_rows)
    batch_manifest = _json_dump(
        layout.external_analysis_artifacts_dir / f"{run_id}_analysis_batch_manifest.json",
        {
            "generated_at": _utc_now(),
            "run_id": run_id,
            "motif_class_count": len(queue_rows),
            "archetype_count": int(len(archetype_df.index)),
            "representative_archetype_count": len(representative_rows),
            "cluster_count": len(cluster_summary_rows),
            "cluster_summary_path": str(cluster_summary_path),
            "representative_archetypes_path": str(representative_path),
            "representative_fragments_path": str(fragment_manifest_path),
            "fragment_count": len(fragment_rows),
        },
    )
    return {
        "archetypes": str(archetype_path),
        "representatives": str(representative_path),
        "cluster_summary": str(cluster_summary_path),
        "fragments": str(fragment_manifest_path),
        "queue": str(queue_path),
        "batch_manifest": str(batch_manifest),
    }
