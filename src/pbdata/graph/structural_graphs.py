"""Structural graph generation from cleaned experimental structures.

Assumptions:
- This module is additive to the canonical biological graph. It produces local
  structure graphs for ML, not pathway/knowledge graphs.
- Export formats are dependency-light: PyG/DGL outputs are written as plain
  tensor dictionaries so they remain usable without requiring those packages.
- Chemistry is heuristic where the structure file lacks explicit bond orders.
"""

from __future__ import annotations

import csv
import json
import math
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import gemmi
import pandas as pd

from pbdata.storage import StorageLayout
from pbdata.table_io import write_dataframe

_AA_HYDROPHOBICITY = {
    "ALA": 1.8, "ARG": -4.5, "ASN": -3.5, "ASP": -3.5, "CYS": 2.5, "GLN": -3.5,
    "GLU": -3.5, "GLY": -0.4, "HIS": -3.2, "ILE": 4.5, "LEU": 3.8, "LYS": -3.9,
    "MET": 1.9, "PHE": 2.8, "PRO": -1.6, "SER": -0.8, "THR": -0.7, "TRP": -0.9,
    "TYR": -1.3, "VAL": 4.2,
}
_AA_CHARGE = {
    "ASP": -1.0, "GLU": -1.0, "LYS": 1.0, "ARG": 1.0, "HIS": 0.5,
}
_ELECTRONEGATIVITY = {
    "H": 2.20, "C": 2.55, "N": 3.04, "O": 3.44, "S": 2.58, "P": 2.19,
    "F": 3.98, "CL": 3.16, "BR": 2.96, "I": 2.66, "ZN": 1.65, "FE": 1.83,
}
_VDW_RADIUS = {
    "H": 1.20, "C": 1.70, "N": 1.55, "O": 1.52, "S": 1.80, "P": 1.80,
    "F": 1.47, "CL": 1.75, "BR": 1.85, "I": 1.98, "ZN": 1.39, "FE": 1.56,
}
_AROMATIC_RESIDUES = {"PHE", "TYR", "TRP", "HIS"}
_HYDROPHOBIC_RESIDUES = {"ALA", "VAL", "ILE", "LEU", "MET", "PHE", "TRP", "TYR", "PRO"}
_POSITIVE_RESIDUES = {"LYS", "ARG", "HIS"}
_NEGATIVE_RESIDUES = {"ASP", "GLU"}
_SP2_ATOMS = {
    ("ASP", "OD1"), ("ASP", "OD2"), ("GLU", "OE1"), ("GLU", "OE2"),
    ("ASN", "OD1"), ("GLN", "OE1"), ("ARG", "NE"), ("ARG", "NH1"), ("ARG", "NH2"),
}
_METAL_ELEMENTS = {"ZN", "FE", "MG", "MN", "CA", "CU", "CO", "NI"}


@dataclass(frozen=True)
class StructuralGraphConfig:
    graph_level: str = "residue"
    scope: str = "whole_protein"
    shell_radius: float = 8.0
    export_formats: tuple[str, ...] = ("pyg", "networkx")


def _normalize_pdb_ids(values: Iterable[object]) -> list[str]:
    seen: dict[str, None] = {}
    for value in values:
        text = str(value or "").strip().upper()
        if text:
            seen[text] = None
    return list(seen)


def _load_pdb_ids_from_manifest(path: Path) -> list[str]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    records = raw.get("records") if isinstance(raw, dict) else None
    if isinstance(records, list):
        return _normalize_pdb_ids(
            record.get("pdb_id")
            for record in records
            if isinstance(record, dict)
        )
    return []


def _load_pdb_ids_from_csv(path: Path) -> list[str]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        candidate_columns = ("pdb_id", "structure_id", "source_record_id", "pdb")
        values: list[str] = []
        for row in reader:
            for column in candidate_columns:
                if row.get(column):
                    values.append(str(row[column]))
                    break
        return _normalize_pdb_ids(values)


def _resolve_selected_pdb_ids(
    layout: StorageLayout,
    *,
    selection: str,
    pdb_ids: Iterable[str] | None,
    manifest_path: Path | None,
    source_csv: Path | None,
    limit: int | None,
) -> tuple[list[str] | None, str]:
    if pdb_ids:
        ordered = _normalize_pdb_ids(pdb_ids)
        return (ordered[:limit] if limit else ordered), "explicit_pdb_ids"

    resolved_manifest = manifest_path
    resolved_csv = source_csv

    if selection == "refresh_plan" and resolved_manifest is None:
        candidate = layout.bootstrap_store_dir / "selected_pdb_refresh_manifest.json"
        if candidate.exists():
            resolved_manifest = candidate
    elif selection == "training_set" and resolved_csv is None:
        for candidate in (layout.root / "custom_training_set.csv", layout.root / "model_ready_pairs.csv"):
            if candidate.exists():
                resolved_csv = candidate
                break
    elif selection == "preview":
        if resolved_manifest is None:
            candidate_manifest = layout.bootstrap_store_dir / "selected_pdb_refresh_manifest.json"
            if candidate_manifest.exists():
                resolved_manifest = candidate_manifest
        if resolved_manifest is None and resolved_csv is None:
            for candidate in (layout.root / "custom_training_set.csv", layout.root / "model_ready_pairs.csv"):
                if candidate.exists():
                    resolved_csv = candidate
                    break
        if limit is None:
            limit = 24

    selected: list[str] | None = None
    selection_label = selection
    if resolved_manifest is not None:
        selected = _load_pdb_ids_from_manifest(resolved_manifest)
        selection_label = f"manifest:{resolved_manifest}"
    elif resolved_csv is not None:
        selected = _load_pdb_ids_from_csv(resolved_csv)
        selection_label = f"csv:{resolved_csv}"

    if selected is not None and limit is not None:
        selected = selected[:limit]

    return selected, selection_label


def _expected_export_paths(out_dir: Path, pdb_id: str, export_formats: tuple[str, ...]) -> list[Path]:
    expected = [out_dir / f"{pdb_id}.nodes.parquet", out_dir / f"{pdb_id}.edges.parquet", out_dir / f"{pdb_id}.summary.json"]
    for export_format in export_formats:
        if export_format == "networkx":
            expected.append(out_dir / f"{pdb_id}.networkx.json")
        elif _try_import_torch() is not None:
            expected.append(out_dir / f"{pdb_id}.{export_format}.pt")
        else:
            expected.append(out_dir / f"{pdb_id}.{export_format}.json")
    return expected


def _load_existing_manifest_rows(manifest_path: Path) -> dict[str, dict[str, Any]]:
    if not manifest_path.exists():
        return {}
    try:
        raw = json.loads(manifest_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    rows = raw.get("graphs") if isinstance(raw, dict) else None
    if not isinstance(rows, list):
        return {}
    manifest_rows: dict[str, dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        pdb_id = str(row.get("pdb_id") or "").upper()
        if pdb_id:
            manifest_rows[pdb_id] = row
    return manifest_rows


def _is_cached_graph_current(
    manifest_row: dict[str, Any] | None,
    *,
    out_dir: Path,
    pdb_id: str,
    structure_path: Path,
    graph_level: str,
    scope: str,
    shell_radius: float,
    export_formats: tuple[str, ...],
) -> bool:
    if not manifest_row:
        return False
    if str(manifest_row.get("graph_level") or "") != graph_level:
        return False
    if str(manifest_row.get("scope") or "") != scope:
        return False
    try:
        recorded_radius = float(manifest_row.get("shell_radius") or 0.0)
    except (TypeError, ValueError):
        return False
    if not math.isclose(recorded_radius, shell_radius, rel_tol=0.0, abs_tol=1e-9):
        return False

    stat = structure_path.stat()
    if int(manifest_row.get("structure_mtime_ns") or -1) != stat.st_mtime_ns:
        return False
    if int(manifest_row.get("structure_size_bytes") or -1) != stat.st_size:
        return False

    recorded_formats = {
        str(value).strip().lower()
        for value in manifest_row.get("export_formats") or []
        if str(value).strip()
    }
    if recorded_formats != {value.lower() for value in export_formats}:
        return False

    return all(path.exists() for path in _expected_export_paths(out_dir, pdb_id, export_formats))


def _try_import_torch() -> Any | None:
    try:
        import torch  # type: ignore
    except ModuleNotFoundError:
        return None
    return torch


def _load_entry_rows(layout: StorageLayout) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    table_dir = layout.extracted_dir / "entry"
    if not table_dir.exists():
        return rows
    for path in sorted(table_dir.glob("*.json")):
        raw = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(raw, dict):
            rows.append(raw)
    return rows


def _load_interface_rows(layout: StorageLayout) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    table_dir = layout.extracted_dir / "interfaces"
    if not table_dir.exists():
        return rows
    for path in sorted(table_dir.glob("*.json")):
        raw = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(raw, list):
            rows.extend(item for item in raw if isinstance(item, dict))
    return rows


def _index_interface_rows_by_pdb(rows: Iterable[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    indexed: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        pdb_id = str(row.get("pdb_id") or "").upper()
        if pdb_id:
            indexed[pdb_id].append(row)
    return indexed


def _scope_chain_ids(pdb_id: str, interfaces: list[dict[str, Any]], scope: str) -> set[str]:
    if scope == "whole_protein":
        return set()
    scoped: set[str] = set()
    for row in interfaces:
        for field in ("chain_id_1", "chain_id_2", "chain_ids", "receptor_chain_ids"):
            raw = str(row.get(field) or "")
            for token in raw.replace(";", ",").split(","):
                token = token.strip()
                if token:
                    scoped.add(token)
    return scoped


def _parse_residue_identifier(raw: object) -> tuple[str, int] | None:
    text = str(raw or "").strip()
    if not text:
        return None
    if ":" in text:
        chain_id, tail = text.split(":", 1)
        digits = "".join(ch for ch in tail if ch.isdigit() or ch == "-")
        if chain_id.strip() and digits:
            try:
                return chain_id.strip(), int(digits)
            except ValueError:
                return None
    return None


def _focus_residue_keys(pdb_id: str, interfaces: list[dict[str, Any]]) -> set[tuple[str, int]]:
    keys: set[tuple[str, int]] = set()
    for row in interfaces:
        for residue_id in row.get("binding_site_residue_ids") or []:
            parsed = _parse_residue_identifier(residue_id)
            if parsed is not None:
                keys.add(parsed)
    return keys


def _residue_secondary_structure(residue: gemmi.Residue) -> str:
    return "coil"


def _secondary_structure_index(structure_path: Path) -> dict[tuple[str, int], str]:
    """Read helix/sheet annotations directly from mmCIF/PDB metadata when present."""
    index: dict[tuple[str, int], str] = {}
    suffix = structure_path.suffix.lower()
    try:
        if suffix in {".cif", ".mmcif"}:
            block = gemmi.cif.read_file(str(structure_path)).sole_block()
            if block.find_loop("_struct_conf.beg_auth_asym_id"):
                asym = block.find_values("_struct_conf.beg_auth_asym_id")
                beg_seq = block.find_values("_struct_conf.beg_auth_seq_id")
                end_seq = block.find_values("_struct_conf.end_auth_seq_id")
                conf_type = block.find_values("_struct_conf.conf_type_id")
                for chain_id, start, end, raw_type in zip(asym, beg_seq, end_seq, conf_type):
                    if not str(chain_id).strip():
                        continue
                    ss = "helix" if "HELX" in str(raw_type).upper() else "coil"
                    try:
                        start_i = int(float(start))
                        end_i = int(float(end))
                    except ValueError:
                        continue
                    for seq in range(start_i, end_i + 1):
                        index[(str(chain_id), seq)] = ss
            if block.find_loop("_struct_sheet_range.beg_auth_asym_id"):
                asym = block.find_values("_struct_sheet_range.beg_auth_asym_id")
                beg_seq = block.find_values("_struct_sheet_range.beg_auth_seq_id")
                end_seq = block.find_values("_struct_sheet_range.end_auth_seq_id")
                for chain_id, start, end in zip(asym, beg_seq, end_seq):
                    if not str(chain_id).strip():
                        continue
                    try:
                        start_i = int(float(start))
                        end_i = int(float(end))
                    except ValueError:
                        continue
                    for seq in range(start_i, end_i + 1):
                        index[(str(chain_id), seq)] = "sheet"
        else:
            for line in structure_path.read_text(encoding="utf-8", errors="ignore").splitlines():
                if line.startswith("HELIX "):
                    chain_id = line[19].strip()
                    try:
                        start_i = int(line[21:25].strip())
                        end_i = int(line[33:37].strip())
                    except ValueError:
                        continue
                    for seq in range(start_i, end_i + 1):
                        index[(chain_id, seq)] = "helix"
                elif line.startswith("SHEET "):
                    chain_id = line[21].strip()
                    try:
                        start_i = int(line[22:26].strip())
                        end_i = int(line[33:37].strip())
                    except ValueError:
                        continue
                    for seq in range(start_i, end_i + 1):
                        index[(chain_id, seq)] = "sheet"
    except Exception:
        return {}
    return index


def _residue_node(chain_name: str, residue: gemmi.Residue, *, secondary_structure_index: dict[tuple[str, int], str]) -> tuple[str, dict[str, Any]]:
    residue_name = residue.name.strip().upper()
    residue_number = int(residue.seqid.num)
    node_id = f"{chain_name}:{residue_number}:{residue_name}"
    coords = [(float(atom.pos.x), float(atom.pos.y), float(atom.pos.z)) for atom in residue]
    centroid = (
        sum(x for x, _, _ in coords) / len(coords),
        sum(y for _, y, _ in coords) / len(coords),
        sum(z for _, _, z in coords) / len(coords),
    ) if coords else (0.0, 0.0, 0.0)
    secondary_structure = secondary_structure_index.get((chain_name, residue_number), _residue_secondary_structure(residue))
    return node_id, {
        "node_id": node_id,
        "chain_id": chain_name,
        "residue_name": residue_name,
        "residue_number": residue_number,
        "amino_acid_type": residue_name,
        "hydrophobicity": _AA_HYDROPHOBICITY.get(residue_name, 0.0),
        "charge": _AA_CHARGE.get(residue_name, 0.0),
        "secondary_structure": secondary_structure,
        "x": centroid[0],
        "y": centroid[1],
        "z": centroid[2],
        "aromatic_flag": residue_name in _AROMATIC_RESIDUES,
        "hydrophobic_flag": residue_name in _HYDROPHOBIC_RESIDUES,
        "residue_atom_count": len(coords),
    }


def _atom_node(chain_name: str, residue: gemmi.Residue, atom: gemmi.Atom) -> tuple[str, dict[str, Any]]:
    residue_name = residue.name.strip().upper()
    residue_number = int(residue.seqid.num)
    element = atom.element.name.upper()
    atom_name = atom.name.strip().upper()
    node_id = f"{chain_name}:{residue_number}:{residue_name}:{atom.name.strip().upper()}"
    donor_acceptor = "none"
    if element == "N":
        donor_acceptor = "donor"
    elif element == "O":
        donor_acceptor = "acceptor"
    hybridization = "sp2" if (residue_name, atom_name) in _SP2_ATOMS or residue_name in _AROMATIC_RESIDUES else "sp3"
    return node_id, {
        "node_id": node_id,
        "chain_id": chain_name,
        "residue_name": residue_name,
        "residue_number": residue_number,
        "atom_name": atom_name,
        "atomic_number": atom.element.atomic_number,
        "electronegativity": _ELECTRONEGATIVITY.get(element, 0.0),
        "formal_charge": _AA_CHARGE.get(residue_name, 0.0) if element in {"N", "O"} else 0.0,
        "vdw_radius": _VDW_RADIUS.get(element, 1.7),
        "hybridization": hybridization,
        "aromatic_flag": residue_name in _AROMATIC_RESIDUES,
        "donor_acceptor": donor_acceptor,
        "metal_flag": element in _METAL_ELEMENTS,
        "x": float(atom.pos.x),
        "y": float(atom.pos.y),
        "z": float(atom.pos.z),
    }


def _distance(a: dict[str, Any], b: dict[str, Any]) -> float:
    return (
        (float(a["x"]) - float(b["x"])) ** 2
        + (float(a["y"]) - float(b["y"])) ** 2
        + (float(a["z"]) - float(b["z"])) ** 2
    ) ** 0.5


def _spatial_cell_id(node: dict[str, Any], cell_size: float) -> tuple[int, int, int]:
    if cell_size <= 0:
        raise ValueError("cell_size must be positive")
    return (
        math.floor(float(node["x"]) / cell_size),
        math.floor(float(node["y"]) / cell_size),
        math.floor(float(node["z"]) / cell_size),
    )


def _candidate_node_pairs(nodes: list[dict[str, Any]], radius: float) -> list[tuple[int, int]]:
    if radius <= 0 or len(nodes) < 2:
        return []

    buckets: dict[tuple[int, int, int], list[int]] = defaultdict(list)
    for index, node in enumerate(nodes):
        buckets[_spatial_cell_id(node, radius)].append(index)

    offsets = (-1, 0, 1)
    pairs: list[tuple[int, int]] = []
    for left_index, left in enumerate(nodes):
        left_cell = _spatial_cell_id(left, radius)
        for dx in offsets:
            for dy in offsets:
                for dz in offsets:
                    neighbor_cell = (left_cell[0] + dx, left_cell[1] + dy, left_cell[2] + dz)
                    for right_index in buckets.get(neighbor_cell, []):
                        if right_index <= left_index:
                            continue
                        pairs.append((left_index, right_index))
    return pairs


def _residue_edges(nodes: list[dict[str, Any]], chain_groups: dict[str, list[dict[str, Any]]], shell_radius: float) -> list[dict[str, Any]]:
    edges: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()
    for chain_nodes in chain_groups.values():
        ordered = sorted(chain_nodes, key=lambda row: int(row["residue_number"]))
        for left, right in zip(ordered, ordered[1:]):
            key = (left["node_id"], right["node_id"], "covalent_bond")
            seen.add(key)
            edges.append({"source": left["node_id"], "target": right["node_id"], "edge_type": "covalent_bond", "distance": _distance(left, right)})
    for left_index, right_index in _candidate_node_pairs(nodes, shell_radius):
        left = nodes[left_index]
        right = nodes[right_index]
        dist = _distance(left, right)
        if dist > shell_radius:
            continue
        edge_type = "distance_neighbor"
        if left["charge"] > 0 and right["charge"] < 0 or left["charge"] < 0 and right["charge"] > 0:
            edge_type = "salt_bridge"
        elif left["hydrophobic_flag"] and right["hydrophobic_flag"]:
            edge_type = "hydrophobic_contact"
        elif left["aromatic_flag"] and right["aromatic_flag"] and dist <= 6.0:
            edge_type = "pi_stacking"
        key = (left["node_id"], right["node_id"], edge_type)
        if key in seen:
            continue
        seen.add(key)
        edges.append({"source": left["node_id"], "target": right["node_id"], "edge_type": edge_type, "distance": dist})
    return edges


def _atom_edges(nodes: list[dict[str, Any]], shell_radius: float) -> list[dict[str, Any]]:
    edges: list[dict[str, Any]] = []
    for left_index, right_index in _candidate_node_pairs(nodes, shell_radius):
        left = nodes[left_index]
        right = nodes[right_index]
        dist = _distance(left, right)
        if dist > shell_radius:
            continue
        sum_vdw = float(left["vdw_radius"]) + float(right["vdw_radius"])
        edge_type = "distance_neighbor"
        if dist <= 1.9:
            edge_type = "covalent_bond"
        elif bool(left.get("metal_flag")) or bool(right.get("metal_flag")):
            edge_type = "metal_coordination"
        elif left["donor_acceptor"] == "donor" and right["donor_acceptor"] == "acceptor" and dist <= 3.5:
            edge_type = "hydrogen_bond"
        elif right["donor_acceptor"] == "donor" and left["donor_acceptor"] == "acceptor" and dist <= 3.5:
            edge_type = "hydrogen_bond"
        elif float(left["formal_charge"]) * float(right["formal_charge"]) < 0 and dist <= 4.5:
            edge_type = "salt_bridge"
        elif left["aromatic_flag"] and right["aromatic_flag"] and dist <= 5.5:
            edge_type = "pi_stacking"
        elif dist <= min(sum_vdw, 5.0):
            edge_type = "hydrophobic_contact"
        edges.append({"source": left["node_id"], "target": right["node_id"], "edge_type": edge_type, "distance": dist})
    return edges


def _edge_feature_rows(edges: list[dict[str, Any]]) -> list[list[float]]:
    rows = []
    for edge in edges:
        edge_type = str(edge.get("edge_type") or "")
        rows.append([
            float(edge.get("distance") or 0.0),
            1.0 if edge_type == "covalent_bond" else 0.0,
            1.0 if edge_type == "hydrogen_bond" else 0.0,
            1.0 if edge_type == "salt_bridge" else 0.0,
            1.0 if edge_type == "hydrophobic_contact" else 0.0,
            1.0 if edge_type == "pi_stacking" else 0.0,
            1.0 if edge_type == "metal_coordination" else 0.0,
        ])
    return rows


def _graph_summary(pdb_id: str, nodes: list[dict[str, Any]], edges: list[dict[str, Any]], *, graph_level: str, scope: str) -> dict[str, Any]:
    edge_counts = defaultdict(int)
    for edge in edges:
        edge_counts[str(edge.get("edge_type") or "unknown")] += 1
    if graph_level == "residue":
        helix_count = sum(1 for node in nodes if str(node.get("secondary_structure") or "") == "helix")
        sheet_count = sum(1 for node in nodes if str(node.get("secondary_structure") or "") == "sheet")
        coil_count = max(len(nodes) - helix_count - sheet_count, 0)
        return {
            "pdb_id": pdb_id,
            "graph_level": graph_level,
            "scope": scope,
            "node_count": len(nodes),
            "edge_count": len(edges),
            "helix_residue_count": helix_count,
            "sheet_residue_count": sheet_count,
            "coil_residue_count": coil_count,
            "salt_bridge_count": edge_counts["salt_bridge"],
            "hydrophobic_contact_count": edge_counts["hydrophobic_contact"],
            "pi_stacking_count": edge_counts["pi_stacking"],
            "distance_neighbor_count": edge_counts["distance_neighbor"],
        }
    return {
        "pdb_id": pdb_id,
        "graph_level": graph_level,
        "scope": scope,
        "node_count": len(nodes),
        "edge_count": len(edges),
        "hydrogen_bond_count": edge_counts["hydrogen_bond"],
        "salt_bridge_count": edge_counts["salt_bridge"],
        "hydrophobic_contact_count": edge_counts["hydrophobic_contact"],
        "pi_stacking_count": edge_counts["pi_stacking"],
        "metal_coordination_count": edge_counts["metal_coordination"],
        "distance_neighbor_count": edge_counts["distance_neighbor"],
    }


def summarize_structure_graph_from_file(
    structure_path: Path,
    *,
    graph_level: str = "residue",
    shell_radius: float = 8.0,
) -> dict[str, Any]:
    """Build an in-memory structural graph summary for inference-time use."""
    structure = gemmi.read_structure(str(structure_path))
    pdb_id = structure_path.stem.upper()
    secondary_structure_index = _secondary_structure_index(structure_path)
    nodes: list[dict[str, Any]] = []
    chain_groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for model in structure:
        for chain in model:
            chain_name = str(chain.name)
            for residue in chain:
                if graph_level == "residue":
                    _, node = _residue_node(chain_name, residue, secondary_structure_index=secondary_structure_index)
                    nodes.append(node)
                    chain_groups[chain_name].append(node)
                else:
                    for atom in residue:
                        _, node = _atom_node(chain_name, residue, atom)
                        nodes.append(node)
    edges = _residue_edges(nodes, chain_groups, shell_radius) if graph_level == "residue" else _atom_edges(nodes, shell_radius)
    return _graph_summary(pdb_id, nodes, edges, graph_level=graph_level, scope="whole_protein")


def _node_feature_rows(nodes: list[dict[str, Any]], graph_level: str) -> list[list[float]]:
    if graph_level == "residue":
        rows = [
            [
                float(node.get("hydrophobicity") or 0.0),
                float(node.get("charge") or 0.0),
                1.0 if str(node.get("secondary_structure") or "") == "helix" else 0.0,
                1.0 if str(node.get("secondary_structure") or "") == "sheet" else 0.0,
                1.0 if bool(node.get("aromatic_flag")) else 0.0,
            ]
            for node in nodes
        ]
    else:
        rows = [
            [
                float(node.get("atomic_number") or 0.0),
                float(node.get("electronegativity") or 0.0),
                float(node.get("formal_charge") or 0.0),
                float(node.get("vdw_radius") or 0.0),
                1.0 if bool(node.get("aromatic_flag")) else 0.0,
                1.0 if str(node.get("donor_acceptor") or "") == "donor" else 0.0,
                1.0 if str(node.get("donor_acceptor") or "") == "acceptor" else 0.0,
            ]
            for node in nodes
        ]
    return rows


def _edge_index_rows(nodes: list[dict[str, Any]], edges: list[dict[str, Any]]) -> list[list[int]]:
    node_index = {row["node_id"]: idx for idx, row in enumerate(nodes)}
    pairs: list[list[int]] = []
    for edge in edges:
        if edge["source"] not in node_index or edge["target"] not in node_index:
            continue
        pairs.append([node_index[edge["source"]], node_index[edge["target"]]])
        pairs.append([node_index[edge["target"]], node_index[edge["source"]]])
    return pairs


def _rows_to_torch_tensor(rows: list[list[float]] | list[list[int]], *, dtype: str) -> Any:
    torch = _try_import_torch()
    if torch is None:
        return rows
    if not rows:
        shape = (2, 0) if dtype == "long" else (0, 0)
        return torch.zeros(shape, dtype=getattr(torch, dtype))
    if dtype == "long":
        return torch.tensor(rows, dtype=torch.long).t().contiguous()
    return torch.tensor(rows, dtype=torch.float32)


def _write_export_bundle(
    out_dir: Path,
    *,
    pdb_id: str,
    export_name: str,
    payload: dict[str, Any],
) -> Path:
    torch = _try_import_torch()
    if torch is not None:
        path = out_dir / f"{pdb_id}.{export_name}.pt"
        torch.save(payload, path)
        return path
    path = out_dir / f"{pdb_id}.{export_name}.json"
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return path


def build_structural_graphs(
    layout: StorageLayout,
    *,
    graph_level: str = "residue",
    scope: str = "whole_protein",
    shell_radius: float = 8.0,
    export_formats: tuple[str, ...] = ("pyg", "networkx"),
    selection: str = "all",
    pdb_ids: Iterable[str] | None = None,
    manifest_path: Path | None = None,
    source_csv: Path | None = None,
    limit: int | None = None,
    only_missing: bool = False,
) -> dict[str, str]:
    """Build structure-level ML graphs from extracted mmCIF/CIF paths."""
    if graph_level not in {"residue", "atom"}:
        raise ValueError("graph_level must be 'residue' or 'atom'")
    if scope not in {"whole_protein", "interface_only", "shell"}:
        raise ValueError("scope must be 'whole_protein', 'interface_only', or 'shell'")
    if selection not in {"all", "refresh_plan", "training_set", "preview"}:
        raise ValueError("selection must be 'all', 'refresh_plan', 'training_set', or 'preview'")

    entries = _load_entry_rows(layout)
    interfaces_by_pdb = _index_interface_rows_by_pdb(_load_interface_rows(layout))
    entries_by_pdb = {
        str(entry.get("pdb_id") or "").upper(): entry
        for entry in entries
        if str(entry.get("pdb_id") or "").strip()
    }
    selected_pdb_ids, selection_label = _resolve_selected_pdb_ids(
        layout,
        selection=selection,
        pdb_ids=pdb_ids,
        manifest_path=manifest_path,
        source_csv=source_csv,
        limit=limit,
    )
    if selected_pdb_ids is None:
        ordered_entries = [
            entries_by_pdb[pdb_id]
            for pdb_id in sorted(entries_by_pdb)
        ]
        if limit is not None:
            ordered_entries = ordered_entries[:limit]
    else:
        ordered_entries = [
            entries_by_pdb[pdb_id]
            for pdb_id in selected_pdb_ids
            if pdb_id in entries_by_pdb
        ]

    out_dir = layout.workspace_graphs_dir / f"{graph_level}_{scope}"
    out_dir.mkdir(parents=True, exist_ok=True)
    manifest_path_out = out_dir / "graph_manifest.json"
    existing_manifest_rows = _load_existing_manifest_rows(manifest_path_out)
    torch_available = _try_import_torch() is not None

    outputs: dict[str, str] = {}
    manifest_rows_by_pdb = dict(existing_manifest_rows)
    processed_count = 0
    skipped_count = 0

    for entry in ordered_entries:
        pdb_id = str(entry.get("pdb_id") or "").upper()
        structure_path = Path(str(entry.get("structure_file_cif_path") or entry.get("structure_file_pdb_path") or ""))
        if not pdb_id or not structure_path.exists():
            continue
        existing_row = existing_manifest_rows.get(pdb_id)
        if only_missing and _is_cached_graph_current(
            existing_row,
            out_dir=out_dir,
            pdb_id=pdb_id,
            structure_path=structure_path,
            graph_level=graph_level,
            scope=scope,
            shell_radius=shell_radius,
            export_formats=export_formats,
        ):
            skipped_count += 1
            outputs[f"{pdb_id}_summary"] = str(out_dir / f"{pdb_id}.summary.json")
            for export_path in _expected_export_paths(out_dir, pdb_id, export_formats):
                if export_path.name.endswith(".networkx.json"):
                    outputs[f"{pdb_id}_networkx"] = str(export_path)
            if "pyg" in export_formats:
                outputs[f"{pdb_id}_pyg"] = str((out_dir / f"{pdb_id}.pyg.pt") if (out_dir / f"{pdb_id}.pyg.pt").exists() else (out_dir / f"{pdb_id}.pyg.json"))
            if "dgl" in export_formats:
                outputs[f"{pdb_id}_dgl"] = str((out_dir / f"{pdb_id}.dgl.pt") if (out_dir / f"{pdb_id}.dgl.pt").exists() else (out_dir / f"{pdb_id}.dgl.json"))
            manifest_rows_by_pdb[pdb_id] = {
                **existing_row,
                "cached": True,
            }
            continue

        structure = gemmi.read_structure(str(structure_path))
        secondary_structure_index = _secondary_structure_index(structure_path)
        interface_rows = interfaces_by_pdb.get(pdb_id, [])
        scoped_chain_ids = _scope_chain_ids(pdb_id, interface_rows, scope)
        focus_residue_keys = _focus_residue_keys(pdb_id, interface_rows)
        all_nodes: list[dict[str, Any]] = []
        focus_node_ids: set[str] = set()
        for model in structure:
            for chain in model:
                chain_name = str(chain.name)
                for residue in chain:
                    if graph_level == "residue":
                        node_id, node = _residue_node(chain_name, residue, secondary_structure_index=secondary_structure_index)
                        all_nodes.append(node)
                        if (chain_name, int(residue.seqid.num)) in focus_residue_keys:
                            focus_node_ids.add(node_id)
                    else:
                        for atom in residue:
                            node_id, node = _atom_node(chain_name, residue, atom)
                            all_nodes.append(node)
                            if (chain_name, int(residue.seqid.num)) in focus_residue_keys:
                                focus_node_ids.add(node_id)
        if scope == "whole_protein":
            nodes = list(all_nodes)
        elif scope == "interface_only":
            if focus_node_ids:
                nodes = [node for node in all_nodes if str(node.get("node_id") or "") in focus_node_ids]
            else:
                nodes = [
                    node for node in all_nodes
                    if not scoped_chain_ids or str(node.get("chain_id") or "") in scoped_chain_ids
                ]
        else:
            focus_points = [
                (float(node.get("x") or 0.0), float(node.get("y") or 0.0), float(node.get("z") or 0.0))
                for node in all_nodes
                if str(node.get("node_id") or "") in focus_node_ids
            ]
            if not focus_points:
                focus_points = [
                    (float(node.get("x") or 0.0), float(node.get("y") or 0.0), float(node.get("z") or 0.0))
                    for node in all_nodes
                    if not scoped_chain_ids or str(node.get("chain_id") or "") in scoped_chain_ids
                ]
            nodes = []
            for node in all_nodes:
                point = (float(node.get("x") or 0.0), float(node.get("y") or 0.0), float(node.get("z") or 0.0))
                if any(((point[0] - fx) ** 2 + (point[1] - fy) ** 2 + (point[2] - fz) ** 2) ** 0.5 <= shell_radius for fx, fy, fz in focus_points):
                    nodes.append(node)
        chain_groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for node in nodes:
            chain_groups[str(node.get("chain_id") or "")].append(node)
        edges = _residue_edges(nodes, chain_groups, shell_radius) if graph_level == "residue" else _atom_edges(nodes, shell_radius)
        nodes_path = out_dir / f"{pdb_id}.nodes.parquet"
        edges_path = out_dir / f"{pdb_id}.edges.parquet"
        write_dataframe(pd.DataFrame(nodes), nodes_path)
        write_dataframe(pd.DataFrame(edges), edges_path)

        edge_index_rows = _edge_index_rows(nodes, edges)
        node_feature_rows = _node_feature_rows(nodes, graph_level)
        edge_feature_rows = _edge_feature_rows(edges)
        edge_index = _rows_to_torch_tensor(edge_index_rows, dtype="long")
        node_features = _rows_to_torch_tensor(node_feature_rows, dtype="float32")
        edge_features = _rows_to_torch_tensor(edge_feature_rows, dtype="float32")
        summary = _graph_summary(pdb_id, nodes, edges, graph_level=graph_level, scope=scope)
        if "pyg" in export_formats:
            pyg_path = _write_export_bundle(
                out_dir,
                pdb_id=pdb_id,
                export_name="pyg",
                payload={
                    "format_note": "torch_tensor_bundle" if torch_available else "json_feature_bundle_no_torch",
                    "x": node_features,
                    "edge_index": edge_index,
                    "edge_attr": edge_features,
                    "node_rows": nodes,
                    "edge_rows": edges,
                    "summary": summary,
                },
            )
            outputs[f"{pdb_id}_pyg"] = str(pyg_path)
        if "dgl" in export_formats:
            dgl_path = _write_export_bundle(
                out_dir,
                pdb_id=pdb_id,
                export_name="dgl",
                payload={
                    "format_note": "dgl_compatible_bundle_not_native_dgl_graph",
                    "num_nodes": len(nodes),
                    "edges": edge_index,
                    "edge_attr": edge_features,
                    "node_rows": nodes,
                    "edge_rows": edges,
                    "summary": summary,
                },
            )
            outputs[f"{pdb_id}_dgl"] = str(dgl_path)
        if "networkx" in export_formats:
            nx_path = out_dir / f"{pdb_id}.networkx.json"
            nx_payload = {"directed": False, "multigraph": False, "graph": {"pdb_id": pdb_id}, "nodes": nodes, "links": edges}
            nx_path.write_text(json.dumps(nx_payload, indent=2), encoding="utf-8")
            outputs[f"{pdb_id}_networkx"] = str(nx_path)
        summary_path = out_dir / f"{pdb_id}.summary.json"
        summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
        outputs[f"{pdb_id}_summary"] = str(summary_path)

        structure_stat = structure_path.stat()
        manifest_rows_by_pdb[pdb_id] = {
            "pdb_id": pdb_id,
            "graph_level": graph_level,
            "scope": scope,
            "shell_radius": shell_radius,
            "node_count": len(nodes),
            "edge_count": len(edges),
            "structure_path": str(structure_path),
            "structure_mtime_ns": structure_stat.st_mtime_ns,
            "structure_size_bytes": structure_stat.st_size,
            "summary_path": str(summary_path),
            "export_formats": list(export_formats),
            "cached": False,
        }
        processed_count += 1

    manifest_rows = [
        manifest_rows_by_pdb[pdb_id]
        for pdb_id in sorted(manifest_rows_by_pdb)
    ]
    manifest_path_out.write_text(
        json.dumps(
            {
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "graph_level": graph_level,
                "scope": scope,
                "export_formats": list(export_formats),
                "selection": selection_label,
                "selected_count": len(ordered_entries),
                "processed_count": processed_count,
                "skipped_count": skipped_count,
                "graphs": manifest_rows,
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    outputs["manifest"] = str(manifest_path_out)
    outputs["processed_count"] = str(processed_count)
    outputs["skipped_count"] = str(skipped_count)
    outputs["selected_count"] = str(len(ordered_entries))
    return outputs
