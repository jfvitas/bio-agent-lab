"""Heuristic microstate assignment from local structure context.

Assumptions:
- This is not a replacement for AmberTools/CHARMM pKa or protonation workflows.
- Outputs are explicit approximations with confidence labels.
- Unknown contexts remain approximate rather than silently promoted to certainty.
"""

from __future__ import annotations

import json
import logging
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import gemmi

from pbdata.pairing import parse_pair_identity_key

logger = logging.getLogger(__name__)

_IONIZABLE_RESIDUES = {"ASP", "GLU", "HIS", "LYS", "ARG", "CYS", "TYR"}
_POSITIVE_DEFAULT = {"LYS": 1.0, "ARG": 1.0}
_NEGATIVE_DEFAULT = {"ASP": -1.0, "GLU": -1.0}
_METAL_ELEMENTS = {"ZN", "FE", "MG", "MN", "CA", "CU", "CO", "NI"}


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


def _distance(a: tuple[float, float, float], b: tuple[float, float, float]) -> float:
    return ((a[0] - b[0]) ** 2 + (a[1] - b[1]) ** 2 + (a[2] - b[2]) ** 2) ** 0.5


def _residue_charge_default(name: str) -> float:
    if name in _POSITIVE_DEFAULT:
        return _POSITIVE_DEFAULT[name]
    if name in _NEGATIVE_DEFAULT:
        return _NEGATIVE_DEFAULT[name]
    return 0.0


def _load_residue_contexts(cif_path: Path, receptor_chain_ids: set[str]) -> list[dict[str, Any]]:
    structure = gemmi.read_structure(str(cif_path))
    records: list[dict[str, Any]] = []
    metal_coords: list[tuple[float, float, float]] = []

    for model in structure:
        for chain in model:
            for residue in chain:
                coords = [(float(atom.pos.x), float(atom.pos.y), float(atom.pos.z)) for atom in residue]
                if not coords:
                    continue
                centroid = (
                    sum(coord[0] for coord in coords) / len(coords),
                    sum(coord[1] for coord in coords) / len(coords),
                    sum(coord[2] for coord in coords) / len(coords),
                )
                if residue.name.strip().upper() in _METAL_ELEMENTS:
                    metal_coords.append(centroid)
                if str(chain.name) not in receptor_chain_ids:
                    continue
                res_name = residue.name.strip().upper()
                if res_name not in _IONIZABLE_RESIDUES:
                    continue
                records.append({
                    "chain_id": str(chain.name),
                    "residue_name": res_name,
                    "residue_number": int(residue.seqid.num),
                    "centroid": centroid,
                })

    for record in records:
        centroid = record["centroid"]
        same_charge_distances: list[float] = []
        opposite_charge_distances: list[float] = []
        for other in records:
            if other is record:
                continue
            d = _distance(centroid, other["centroid"])
            if d > 6.0:
                continue
            sign_a = _residue_charge_default(record["residue_name"])
            sign_b = _residue_charge_default(other["residue_name"])
            if sign_a == 0.0 or sign_b == 0.0:
                continue
            if sign_a * sign_b > 0:
                same_charge_distances.append(d)
            else:
                opposite_charge_distances.append(d)
        metal_distances = [
            _distance(centroid, metal_coord)
            for metal_coord in metal_coords
            if _distance(centroid, metal_coord) <= 5.0
        ]
        record["nearest_same_charge_distance"] = min(same_charge_distances) if same_charge_distances else None
        record["nearest_opposite_charge_distance"] = min(opposite_charge_distances) if opposite_charge_distances else None
        record["nearest_metal_distance"] = min(metal_distances) if metal_distances else None
    return records


def _assign_microstate(record: dict[str, Any]) -> dict[str, Any]:
    residue_name = str(record["residue_name"])
    base_charge = _residue_charge_default(residue_name)
    adjusted = base_charge
    confidence = "low"

    same_d = record.get("nearest_same_charge_distance")
    opp_d = record.get("nearest_opposite_charge_distance")
    metal_d = record.get("nearest_metal_distance")

    if residue_name in {"ASP", "GLU"}:
        if same_d is not None and same_d < 4.0:
            adjusted += 0.35
        if opp_d is not None and opp_d < 4.5:
            adjusted -= 0.20
        if metal_d is not None and metal_d < 3.2:
            adjusted -= 0.25
        adjusted = max(-1.0, min(0.0, adjusted))
        state = "deprotonated" if adjusted <= -0.65 else "possibly_protonated"
        confidence = "medium" if (same_d is not None or opp_d is not None or metal_d is not None) else "low"
    elif residue_name in {"LYS", "ARG"}:
        if same_d is not None and same_d < 4.0:
            adjusted -= 0.25
        if opp_d is not None and opp_d < 4.5:
            adjusted += 0.10
        adjusted = max(0.0, min(1.0, adjusted))
        state = "protonated" if adjusted >= 0.6 else "possibly_neutralized"
        confidence = "medium" if (same_d is not None or opp_d is not None) else "low"
    elif residue_name == "HIS":
        if metal_d is not None and metal_d < 3.2:
            adjusted = 0.7
        elif opp_d is not None and opp_d < 4.0:
            adjusted = 0.5
        elif same_d is not None and same_d < 4.0:
            adjusted = 0.1
        state = "protonated" if adjusted >= 0.6 else ("mixed" if adjusted >= 0.25 else "neutral")
        confidence = "medium" if (metal_d is not None or opp_d is not None or same_d is not None) else "low"
    elif residue_name in {"CYS", "TYR"}:
        if metal_d is not None and metal_d < 3.0:
            adjusted = -0.4
        elif opp_d is not None and opp_d < 4.0:
            adjusted = -0.2
        else:
            adjusted = 0.0
        state = "deprotonated_like" if adjusted <= -0.3 else "neutral"
        confidence = "low"
    else:
        state = "unknown"

    return {
        "residue_name": residue_name,
        "chain_id": record["chain_id"],
        "residue_number": record["residue_number"],
        "canonical_charge": base_charge,
        "adjusted_charge_estimate": round(float(adjusted), 4),
        "state_label": state,
        "confidence": confidence,
        "nearest_same_charge_distance": record.get("nearest_same_charge_distance"),
        "nearest_opposite_charge_distance": record.get("nearest_opposite_charge_distance"),
        "nearest_metal_distance": record.get("nearest_metal_distance"),
    }


def summarize_structure_microstates(
    structure_file: Path,
    *,
    receptor_chain_ids: set[str] | None = None,
) -> dict[str, Any] | None:
    """Compute heuristic microstates directly from a structure file for inference-time use."""
    try:
        structure = gemmi.read_structure(str(structure_file))
    except Exception:
        return None

    selected_chain_ids = receptor_chain_ids
    if not selected_chain_ids:
        selected_chain_ids = {
            str(chain.name)
            for model in structure
            for chain in model
            if any(residue.name.strip().upper() in _IONIZABLE_RESIDUES for residue in chain)
        }
    if not selected_chain_ids:
        return None

    try:
        contexts = _load_residue_contexts(structure_file, set(selected_chain_ids))
    except Exception:
        return None
    assignments = [_assign_microstate(record) for record in contexts]
    if not assignments:
        return None
    return {
        "microstates": assignments,
        "record_count": len(assignments),
        "chain_ids": sorted(selected_chain_ids),
        "method": "heuristic_local_context_v1",
    }


def build_microstate_records(
    extracted_dir: Path,
    output_dir: Path,
) -> tuple[Path, Path]:
    """Build pair-level microstate records from extracted data and local structures."""
    entries = _load_table_json(extracted_dir / "entry")
    assays = _load_table_json(extracted_dir / "assays")
    entry_by_pdb = {str(entry.get("pdb_id") or ""): entry for entry in entries if entry.get("pdb_id")}
    output_dir.mkdir(parents=True, exist_ok=True)

    rows: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for assay in assays:
        pdb_id = str(assay.get("pdb_id") or "")
        pair_key = str(assay.get("pair_identity_key") or "")
        affinity_type = str(assay.get("binding_affinity_type") or "")
        if not pdb_id or not pair_key:
            continue
        dedupe = (pair_key, affinity_type)
        if dedupe in seen:
            continue
        seen.add(dedupe)
        parsed = parse_pair_identity_key(pair_key)
        if parsed is None or not parsed.receptor_chain_ids:
            continue
        entry = entry_by_pdb.get(pdb_id, {})
        cif_path = Path(str(entry.get("structure_file_cif_path") or ""))
        if not cif_path.exists():
            continue
        try:
            contexts = _load_residue_contexts(cif_path, set(parsed.receptor_chain_ids))
        except Exception as exc:
            logger.warning("Microstate parsing failed for %s: %s", pdb_id, exc)
            continue
        assignments = [_assign_microstate(record) for record in contexts]
        rows.append({
            "pdb_id": pdb_id,
            "pair_identity_key": pair_key,
            "binding_affinity_type": affinity_type,
            "microstates": assignments,
            "record_count": len(assignments),
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "method": "heuristic_local_context_v1",
        })

    out_path = output_dir / "microstate_records.json"
    out_path.write_text(json.dumps(rows, indent=2), encoding="utf-8")
    manifest = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "status": "materialized_from_local_structures",
        "record_count": len(rows),
        "notes": (
            "Heuristic local-context microstate approximations from mmCIF geometry. "
            "Not a substitute for AmberTools/CHARMM/QM protonation workflows."
        ),
    }
    manifest_path = output_dir / "microstate_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return out_path, manifest_path
