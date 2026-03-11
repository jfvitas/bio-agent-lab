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

import hashlib
import json
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter
from typing import Any

import gemmi
import pandas as pd
import yaml

try:
    import torch
except ModuleNotFoundError:  # torch is optional; degrade gracefully
    torch = None  # type: ignore[assignment]

from pbdata.pipeline.physics_feedback import (
    load_latest_site_physics_surrogate,
    predict_site_physics_from_surrogate,
)
from pbdata.storage import StorageLayout
from pbdata.table_io import read_dataframe, write_dataframe

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


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _default_run_id() -> str:
    return datetime.now(timezone.utc).strftime("feature_run_%Y%m%dT%H%M%SZ")


def _json_dump(path: Path, payload: Any) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return path


def _write_df(path: Path, rows: list[dict[str, Any]]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    return write_dataframe(pd.DataFrame(rows), path)


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


def _stage_manifest_path(layout: StorageLayout, run_id: str, stage_name: str) -> Path:
    return layout.artifact_manifests_dir / f"{run_id}_{stage_name}_status.json"


def _structured_error_path(layout: StorageLayout, run_id: str) -> Path:
    return layout.artifact_logs_dir / f"{run_id}_structured_errors.jsonl"


def _append_structured_error(layout: StorageLayout, run_id: str, payload: dict[str, Any]) -> None:
    path = _structured_error_path(layout, run_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload) + "\n")


def _write_stage_status(
    layout: StorageLayout,
    *,
    run_id: str,
    stage_name: str,
    start_time: str,
    end_time: str,
    status: str,
    records_attempted: int,
    records_succeeded: int,
    records_failed: int,
    upstream_dependencies: list[str],
    output_artifacts: list[str],
    warnings: list[str],
) -> Path:
    return _json_dump(
        _stage_manifest_path(layout, run_id, stage_name),
        {
            "stage_name": stage_name,
            "run_id": run_id,
            "start_time": start_time,
            "end_time": end_time,
            "status": status,
            "records_attempted": records_attempted,
            "records_succeeded": records_succeeded,
            "records_failed": records_failed,
            "upstream_dependencies": upstream_dependencies,
            "output_artifacts": output_artifacts,
            "warnings": warnings,
        },
    )


def _load_stage_status(layout: StorageLayout, run_id: str, stage_name: str) -> dict[str, Any] | None:
    path = _stage_manifest_path(layout, run_id, stage_name)
    if not path.exists():
        return None
    raw = json.loads(path.read_text(encoding="utf-8"))
    return raw if isinstance(raw, dict) else None


def _canonical_input_rows(layout: StorageLayout) -> dict[str, list[dict[str, Any]]]:
    return {
        "entry": _load_table_rows(layout.extracted_dir / "entry"),
        "chain": _load_table_rows(layout.extracted_dir / "chains"),
        "bound_object": _load_table_rows(layout.extracted_dir / "bound_objects"),
        "interface": _load_table_rows(layout.extracted_dir / "interfaces"),
        "assay": _load_table_rows(layout.extracted_dir / "assays"),
        "provenance": _load_table_rows(layout.extracted_dir / "provenance"),
    }


def _write_input_manifest(layout: StorageLayout, config: FeaturePipelineConfig, row_counts: dict[str, int]) -> Path:
    return _json_dump(
        layout.artifact_manifests_dir / f"{config.run_id}_input_manifest.json",
        {
            "schema_version": SCHEMA_VERSION,
            "pipeline_version": PIPELINE_VERSION,
            "site_physics_spec_version": SITE_PHYSICS_SPEC_VERSION,
            "graph_representation_version": GRAPH_REPRESENTATION_VERSION,
            "training_example_version": TRAINING_EXAMPLE_VERSION,
            "source_dataset_versions": {"canonical_tables": "derived_from_extracted_tables"},
            "git_commit_hash": None,
            "run_timestamp": _utc_now(),
            "task_id": config.run_id,
            "row_counts_by_entity_table": row_counts,
            "run_mode": config.run_mode,
            "gpu_unavailable": not config.gpu_enabled,
        },
    )


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


def _run_stage(layout: StorageLayout, config: FeaturePipelineConfig, *, stage_name: str, dependencies: list[str], runner) -> dict[str, Any]:
    if config.run_mode == "resume":
        existing = _load_stage_status(layout, config.run_id, stage_name)
        if existing and str(existing.get("status") or "") == "passed":
            return {"status": "skipped", "warnings": ["resume_reused_existing_stage"]}
    start = _utc_now()
    t0 = perf_counter()
    attempted = succeeded = failed = 0
    warnings: list[str] = []
    outputs: list[str] = []
    status = "passed"
    try:
        attempted, succeeded, failed, outputs, warnings = runner()
        status = "partial" if failed and succeeded else "failed" if failed and not succeeded else "passed"
    except Exception as exc:
        status = "failed"
        failed = max(failed, 1)
        warnings.append(str(exc))
        _append_structured_error(layout, config.run_id, {"stage_name": stage_name, "run_id": config.run_id, "error": str(exc), "generated_at": _utc_now()})
        if config.fail_hard:
            raise
    _write_stage_status(
        layout,
        run_id=config.run_id,
        stage_name=stage_name,
        start_time=start,
        end_time=_utc_now(),
        status=status,
        records_attempted=attempted,
        records_succeeded=succeeded,
        records_failed=failed,
        upstream_dependencies=dependencies,
        output_artifacts=outputs,
        warnings=warnings + [f"elapsed_seconds={round(perf_counter() - t0, 3)}"],
    )
    return {"status": status, "warnings": warnings}


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
    input_manifest = _write_input_manifest(layout, config, {key: len(value) for key, value in rows.items()})

    def stage1():
        entries = rows["entry"]
        chain_counts: dict[str, int] = defaultdict(int)
        for chain in rows["chain"]:
            pdb_id = str(chain.get("pdb_id") or "")
            if pdb_id:
                chain_counts[pdb_id] += 1
        resolved: list[dict[str, Any]] = []
        rejected: list[dict[str, Any]] = []
        for entry in entries:
            pdb_id = str(entry.get("pdb_id") or "")
            cif_path = str(entry.get("structure_file_cif_path") or "")
            if not pdb_id or not cif_path:
                rejected.append({"pdb_id": pdb_id, "reason": "missing_structure_reference"})
                continue
            if not Path(cif_path).exists():
                rejected.append({"pdb_id": pdb_id, "reason": "structure_file_missing_on_disk"})
                continue
            resolved.append({"record_id": pdb_id, "pdb_id": pdb_id, "structure_file_cif_path": cif_path, "chain_count": chain_counts.get(pdb_id, 0), "experimental_method": entry.get("experimental_method"), "structure_resolution": entry.get("structure_resolution")})
        resolved_path = _write_df(layout.artifact_manifests_dir / f"{config.run_id}_stage1_resolved_records.parquet", resolved)
        rejected_path = _write_df(layout.artifact_manifests_dir / f"{config.run_id}_stage1_rejections.parquet", rejected)
        manifest_path = _json_dump(layout.artifact_manifests_dir / f"{config.run_id}_stage1_manifest.json", {"generated_at": _utc_now(), "status": "resolved", "resolved_count": len(resolved), "rejected_count": len(rejected)})
        return len(entries), len(resolved), len(rejected), [str(resolved_path), str(rejected_path), str(manifest_path)], []

    def stage2():
        df = read_dataframe(layout.artifact_manifests_dir / f"{config.run_id}_stage1_resolved_records.parquet")
        outputs: list[str] = []
        warnings: list[str] = []
        attempted = len(df.index)
        succeeded = failed = 0
        for row in df.to_dict(orient="records"):
            pdb_id = str(row["pdb_id"])
            try:
                atom_rows, site_rows = _site_candidates_from_structure(pdb_id, Path(str(row["structure_file_cif_path"])))
                outputs.append(str(_write_df(layout.prepared_structures_artifacts_dir / config.run_id / f"{pdb_id}.prepared.parquet", [{"record_id": pdb_id, "pdb_id": pdb_id, "structure_file_cif_path": row["structure_file_cif_path"], "chain_count": row.get("chain_count"), "atom_count": len(atom_rows), "site_count": len(site_rows)}])))
                outputs.append(str(_write_df(layout.prepared_structures_artifacts_dir / config.run_id / f"{pdb_id}.sites.parquet", site_rows)))
                outputs.append(str(_write_df(layout.site_envs_artifacts_dir / config.run_id / f"{pdb_id}.atoms.parquet", atom_rows)))
                succeeded += 1
            except Exception as exc:
                failed += 1
                warnings.append(f"{pdb_id}: {exc}")
        outputs.append(str(_json_dump(layout.artifact_manifests_dir / f"{config.run_id}_stage2_manifest.json", {"generated_at": _utc_now(), "status": "prepared", "records": succeeded})))
        return attempted, succeeded, failed, outputs, warnings

    def stage3():
        prepared_dir = layout.prepared_structures_artifacts_dir / config.run_id
        attempted = succeeded = failed = 0
        outputs: list[str] = []
        warnings: list[str] = []
        for prepared_path in sorted(prepared_dir.glob("*.prepared.parquet")):
            pdb_id = prepared_path.stem.replace(".prepared", "")
            attempted += 1
            try:
                sites = read_dataframe(prepared_dir / f"{pdb_id}.sites.parquet").to_dict(orient="records")
                atoms = read_dataframe(layout.site_envs_artifacts_dir / config.run_id / f"{pdb_id}.atoms.parquet").to_dict(orient="records")
                env_rows, node_rows = _shell_descriptor_rows(pdb_id, sites, atoms)
                edge_rows = _edge_rows_from_sites(pdb_id, sites)
                outputs.extend([
                    str(_write_df(layout.base_features_artifacts_dir / config.run_id / f"{pdb_id}.env_vectors.parquet", env_rows)),
                    str(_write_df(layout.base_features_artifacts_dir / config.run_id / f"{pdb_id}.node_base.parquet", node_rows)),
                    str(_write_df(layout.base_features_artifacts_dir / config.run_id / f"{pdb_id}.edge_base.parquet", edge_rows)),
                    str(_json_dump(layout.base_features_artifacts_dir / config.run_id / f"{pdb_id}.global_base.json", {"generated_at": _utc_now(), "record_id": pdb_id, "site_count": len(sites), "env_row_count": len(env_rows), "edge_candidate_count": len(edge_rows)})),
                ])
                succeeded += 1
            except Exception as exc:
                failed += 1
                warnings.append(f"{pdb_id}: {exc}")
        return attempted, succeeded, failed, outputs, warnings

    def stage4():
        surrogate_model = None if config.degraded_mode else load_latest_site_physics_surrogate(layout)
        if not config.degraded_mode and surrogate_model is None:
            raise RuntimeError("site_physics_enrichment requires a trained surrogate checkpoint when degraded_mode=false")
        attempted = succeeded = failed = 0
        outputs: list[str] = []
        warnings: list[str] = []
        for env_path in sorted((layout.base_features_artifacts_dir / config.run_id).glob("*.env_vectors.parquet")):
            pdb_id = env_path.stem.replace(".env_vectors", "")
            attempted += 1
            try:
                env_rows = read_dataframe(env_path).to_dict(orient="records")
                if surrogate_model is not None:
                    refined_rows, provenance_rows, cache_stats = _site_refined_rows_from_surrogate(env_rows, surrogate_model)
                else:
                    refined_rows, provenance_rows, cache_stats = _site_refined_rows(env_rows, degraded_mode=config.degraded_mode)
                outputs.extend([
                    str(_write_df(layout.site_physics_artifacts_dir / config.run_id / f"{pdb_id}.site_refined.parquet", refined_rows)),
                    str(_write_df(layout.site_physics_artifacts_dir / config.run_id / f"{pdb_id}.physics_provenance.parquet", provenance_rows)),
                    str(_json_dump(layout.site_physics_artifacts_dir / config.run_id / f"{pdb_id}.cache_stats.json", cache_stats)),
                ])
                succeeded += 1
            except Exception as exc:
                failed += 1
                warnings.append(f"{pdb_id}: {exc}")
        outputs.append(str(_json_dump(layout.artifact_caches_dir / f"{config.run_id}_cache_manifest.json", {"generated_at": _utc_now(), "schema_version": SCHEMA_VERSION, "feature_pipeline_version": PIPELINE_VERSION, "motif_taxonomy_version": SITE_PHYSICS_SPEC_VERSION, "surrogate_checkpoint_id": None if surrogate_model is None else str(surrogate_model.get("version") or "site_physics_surrogate"), "graph_representation_version": GRAPH_REPRESENTATION_VERSION, "training_example_version": TRAINING_EXAMPLE_VERSION, "degraded_mode": config.degraded_mode})))
        return attempted, succeeded, failed, outputs, warnings

    def stage5():
        attempted = succeeded = failed = 0
        outputs: list[str] = []
        warnings: list[str] = []
        base_dir = layout.base_features_artifacts_dir / config.run_id
        site_dir = layout.site_physics_artifacts_dir / config.run_id
        for node_path in sorted(base_dir.glob("*.node_base.parquet")):
            pdb_id = node_path.stem.replace(".node_base", "")
            attempted += 1
            try:
                graph_nodes, graph_edges, meta = _graph_rows(
                    read_dataframe(node_path).to_dict(orient="records"),
                    read_dataframe(base_dir / f"{pdb_id}.edge_base.parquet").to_dict(orient="records"),
                    read_dataframe(site_dir / f"{pdb_id}.site_refined.parquet").to_dict(orient="records"),
                )
                graph_pt = layout.graphs_artifacts_dir / config.run_id / f"{pdb_id}.graph.pt"
                graph_pt.parent.mkdir(parents=True, exist_ok=True)
                graph_payload = {"nodes": graph_nodes, "edges": graph_edges, "meta": meta}
                if torch is not None:
                    torch.save(graph_payload, graph_pt)
                else:
                    graph_pt.write_text(json.dumps(graph_payload, indent=2), encoding="utf-8")
                outputs.extend([
                    str(_write_df(layout.graphs_artifacts_dir / config.run_id / f"{pdb_id}.nodes.parquet", graph_nodes)),
                    str(_write_df(layout.graphs_artifacts_dir / config.run_id / f"{pdb_id}.edges.parquet", graph_edges)),
                    str(_json_dump(layout.graphs_artifacts_dir / config.run_id / f"{pdb_id}.graph_meta.json", meta)),
                    str(graph_pt),
                ])
                succeeded += 1
            except Exception as exc:
                failed += 1
                warnings.append(f"{pdb_id}: {exc}")
        return attempted, succeeded, failed, outputs, warnings

    def stage6():
        pairs_by_pdb: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in rows["assay"]:
            pdb_id = str(row.get("pdb_id") or "")
            if pdb_id:
                pairs_by_pdb[pdb_id].append(row)
        attempted = succeeded = failed = 0
        outputs: list[str] = []
        warnings: list[str] = []
        manifest_rows: list[dict[str, Any]] = []
        for graph_meta_path in sorted((layout.graphs_artifacts_dir / config.run_id).glob("*.graph_meta.json")):
            pdb_id = graph_meta_path.stem.replace(".graph_meta", "")
            attempted += 1
            try:
                graph_meta = json.loads(graph_meta_path.read_text(encoding="utf-8"))
                examples, labels, metas = _training_example_rows(pdb_id, pairs_by_pdb.get(pdb_id, []), graph_meta, config.degraded_mode)
                for index, meta in enumerate(metas):
                    record_id = str(meta["record_id"])
                    if index < len(examples):
                        example_path = layout.training_examples_artifacts_dir / config.run_id / f"{record_id}_{index}.example.pt"
                        example_path.parent.mkdir(parents=True, exist_ok=True)
                        if torch is not None:
                            torch.save(examples[index], example_path)
                        else:
                            example_path.write_text(json.dumps(examples[index], indent=2), encoding="utf-8")
                        outputs.append(str(example_path))
                    if index < len(labels):
                        outputs.append(str(_json_dump(layout.training_examples_artifacts_dir / config.run_id / f"{record_id}_{index}.label.json", labels[index])))
                    outputs.append(str(_json_dump(layout.training_examples_artifacts_dir / config.run_id / f"{record_id}_{index}.meta.json", meta)))
                    manifest_rows.append({"record_id": record_id, "example_id": meta.get("example_id"), "supervised_label_available": bool(meta.get("supervised_label_available")), "task_type": meta.get("task_type"), "degraded_mode": bool(meta.get("degraded_mode"))})
                succeeded += 1
            except Exception as exc:
                failed += 1
                warnings.append(f"{pdb_id}: {exc}")
        outputs.append(str(_write_df(layout.training_examples_artifacts_dir / config.run_id / "manifest.parquet", manifest_rows)))
        return attempted, succeeded, failed, outputs, warnings

    def stage7():
        payloads = [payload for name in ("canonical_input_resolution", "structure_preparation", "base_feature_extraction", "site_physics_enrichment", "graph_construction", "training_example_assembly") if (payload := _load_stage_status(layout, config.run_id, name))]
        summary = [f"# Feature Pipeline Summary: {config.run_id}", "", f"- Pipeline version: {PIPELINE_VERSION}", f"- Run mode: {config.run_mode}", f"- Degraded mode: {config.degraded_mode}", "", "## Stage outcomes"]
        for payload in payloads:
            attempted = int(payload["records_attempted"])
            succeeded = int(payload["records_succeeded"])
            if attempted == 0 and str(payload["status"]) == "passed":
                line = f"- {payload['stage_name']}: passed (0 records; upstream input was empty)"
            else:
                line = f"- {payload['stage_name']}: {payload['status']} ({succeeded}/{attempted} succeeded)"
            summary.append(line)
        summary_path = layout.feature_reports_dir / f"{config.run_id}_summary.md"
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path.write_text("\n".join(summary), encoding="utf-8")
        coverage = _json_dump(layout.feature_reports_dir / f"{config.run_id}_coverage.json", {"run_id": config.run_id, "stages": {payload['stage_name']: payload['records_succeeded'] for payload in payloads}, "degraded_mode": config.degraded_mode})
        failures = _json_dump(layout.feature_reports_dir / f"{config.run_id}_failures.json", {"run_id": config.run_id, "failed_stages": [payload for payload in payloads if payload.get('status') in {'failed', 'partial'}]})
        performance = _json_dump(layout.feature_reports_dir / f"{config.run_id}_performance.json", {"run_id": config.run_id, "gpu_unavailable": not config.gpu_enabled, "stage_statuses": {payload['stage_name']: payload['status'] for payload in payloads}})
        return 1, 1, 0, [str(summary_path), str(coverage), str(failures), str(performance)], []

    stages = [
        ("canonical_input_resolution", [], stage1),
        ("structure_preparation", ["canonical_input_resolution"], stage2),
        ("base_feature_extraction", ["structure_preparation"], stage3),
        ("site_physics_enrichment", ["base_feature_extraction"], stage4),
        ("graph_construction", ["site_physics_enrichment"], stage5),
        ("training_example_assembly", ["graph_construction"], stage6),
        ("validation_reporting_export", ["training_example_assembly"], stage7),
    ]
    if config.run_mode == "stage_only":
        if not config.stage_only:
            raise ValueError("stage_only run mode requires --stage-name")
        stages = [stage for stage in stages if stage[0] == config.stage_only]
        if not stages:
            raise ValueError(f"Unknown stage_only target: {config.stage_only}")
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
    env_dir = layout.base_features_artifacts_dir / run_id
    archetype_rows: list[dict[str, Any]] = []
    for env_path in sorted(env_dir.glob("*.env_vectors.parquet")):
        env_df = read_dataframe(env_path)
        if env_df.empty:
            continue
        for site_id, site_df in env_df.groupby("site_id", sort=True):
            motif_class = str(site_df["motif_class"].iloc[0])
            descriptor_hash = hashlib.sha256(site_df[["shell_name", "neighbor_atom_count", "sum_partial_charge", "electric_field_magnitude", "donor_count", "acceptor_count"]].to_json(orient="records").encode("utf-8")).hexdigest()
            archetype_rows.append({"run_id": run_id, "site_id": site_id, "motif_class": motif_class, "archetype_id": f"{motif_class}:{descriptor_hash[:12]}", "descriptor_hash": descriptor_hash})
    archetype_df = pd.DataFrame(archetype_rows)
    archetype_path = _write_df(layout.archetypes_artifacts_dir / run_id / "archetypes.parquet", archetype_df.drop_duplicates(subset=["archetype_id"]).to_dict(orient="records") if not archetype_df.empty else [])
    queue_rows = []
    if not archetype_df.empty:
        for motif_class, motif_df in archetype_df.drop_duplicates(subset=["archetype_id"]).groupby("motif_class", sort=True):
            queue_rows.append({"motif_class": motif_class, "archetype_ids": motif_df.head(20)["archetype_id"].tolist(), "analysis_types": ["orca", "apbs", "openmm"]})
    queue_path = layout.external_analysis_artifacts_dir / f"{run_id}_analysis_queue.yaml"
    queue_path.parent.mkdir(parents=True, exist_ok=True)
    with queue_path.open("w", encoding="utf-8") as handle:
        yaml.safe_dump({"run_id": run_id, "batches": queue_rows}, handle, sort_keys=False)
    batch_manifest = _json_dump(layout.external_analysis_artifacts_dir / f"{run_id}_analysis_batch_manifest.json", {"generated_at": _utc_now(), "run_id": run_id, "motif_class_count": len(queue_rows), "archetype_count": int(len(archetype_df.index))})
    return {"archetypes": str(archetype_path), "queue": str(queue_path), "batch_manifest": str(batch_manifest)}
