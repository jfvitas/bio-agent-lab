"""Lightweight mmCIF supplement parsing for structural edge cases.

This module augments the GraphQL metadata with coordinate-derived signals:
- per-instance nonpolymer residues (for repeated metals/cofactors/ligands)
- branched carbohydrate entities
- polymer entity chain IDs / residue counts when GraphQL is incomplete

The goal is conservative enrichment, not a full structural parser.

It also handles permanent storage of downloaded mmCIF files with SHA-256
hashing and file provenance metadata per the structure extraction spec.
"""

from __future__ import annotations

import gzip
import hashlib
import logging
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import gemmi
import requests

from pbdata.storage import reuse_existing_file, validate_mmcif_file, validate_pdb_file

logger = logging.getLogger(__name__)

_MMCIF_URL = "https://files.rcsb.org/download/{pdb_id}.cif"
_PDB_URL = "https://files.rcsb.org/download/{pdb_id}.pdb"
_TIMEOUT = 60
_MISSING = {"", ".", "?"}
_DEFAULT_RAW_DIR = Path("data/raw/rcsb")
_DEFAULT_STRUCTURES_DIR = Path("data/structures/rcsb")


def _clean(value: Any) -> str:
    return gemmi.cif.as_string(value).strip()


def _norm(value: str) -> str:
    return value.strip().lower()


def _read_column(block: gemmi.cif.Block, tag: str) -> list[str]:
    return [_clean(value) for value in block.find_values(tag)]


def _read_pairs(block: gemmi.cif.Block, left: str, right: str) -> list[tuple[str, str]]:
    a = _read_column(block, left)
    b = _read_column(block, right)
    if len(a) != len(b):
        raise ValueError(f"Mismatched column lengths for {left} and {right}")
    return list(zip(a, b))


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _structure_urls(pdb_id: str, mirror: str) -> tuple[str, str]:
    mirror_key = str(mirror or "rcsb").strip().lower()
    if mirror_key == "pdbj":
        shard = pdb_id[1:3].lower()
        return (
            f"https://files.pdbj.org/pub/pdb/data/structures/divided/mmCIF/{shard}/{pdb_id.lower()}.cif.gz",
            f"https://files.pdbj.org/pub/pdb/data/structures/divided/pdb/{shard}/pdb{pdb_id.lower()}.ent.gz",
        )
    return (
        _MMCIF_URL.format(pdb_id=pdb_id),
        _PDB_URL.format(pdb_id=pdb_id),
    )


def _download_bytes(url: str) -> bytes:
    response = requests.get(url, timeout=_TIMEOUT)
    response.raise_for_status()
    payload = response.content
    if url.endswith(".gz"):
        return gzip.decompress(payload)
    return payload


def download_structure_files(
    pdb_id: str,
    structures_dir: Path | None = None,
    download_pdb: bool = False,
    mirror: str = "rcsb",
) -> dict[str, Any]:
    """Download mmCIF (and optionally PDB) files, save permanently, return provenance.

    Returns a dict with file provenance fields per the spec:
    - structure_file_cif_path, structure_file_cif_size_bytes
    - structure_file_pdb_path, structure_file_pdb_size_bytes (if downloaded)
    - parsed_structure_format, structure_download_url
    - structure_downloaded_at, structure_file_hash_sha256
    """
    pdb_id = pdb_id.upper()
    out_dir = structures_dir or _DEFAULT_STRUCTURES_DIR
    out_dir.mkdir(parents=True, exist_ok=True)

    provenance: dict[str, Any] = {
        "parsed_structure_format": "mmCIF",
    }

    # ── mmCIF (required primary format) ──────────────────────────────
    cif_path = out_dir / f"{pdb_id}.cif"
    cif_url, pdb_url = _structure_urls(pdb_id, mirror)

    if reuse_existing_file(cif_path, validator=validate_mmcif_file):
        cif_bytes = cif_path.read_bytes()
        provenance["structure_file_cif_path"] = str(cif_path)
        provenance["structure_file_cif_size_bytes"] = len(cif_bytes)
        provenance["structure_file_hash_sha256"] = _sha256(cif_bytes)
        provenance["structure_download_url"] = cif_url
        provenance["structure_downloaded_at"] = "cached"
        provenance["structure_download_mirror"] = mirror
    else:
        try:
            cif_bytes = _download_bytes(cif_url)
            cif_path.write_bytes(cif_bytes)
            provenance["structure_file_cif_path"] = str(cif_path)
            provenance["structure_file_cif_size_bytes"] = len(cif_bytes)
            provenance["structure_file_hash_sha256"] = _sha256(cif_bytes)
            provenance["structure_download_url"] = cif_url
            provenance["structure_downloaded_at"] = datetime.now(timezone.utc).isoformat()
            provenance["structure_download_mirror"] = mirror
        except Exception as exc:
            logger.warning("mmCIF download failed for %s: %s", pdb_id, exc)
            provenance["structure_file_cif_path"] = None
            provenance["structure_download_url"] = cif_url
            provenance["structure_download_mirror"] = mirror

    # ── PDB (optional compatibility fallback) ────────────────────────
    if download_pdb:
        pdb_path = out_dir / f"{pdb_id}.pdb"
        if reuse_existing_file(pdb_path, validator=validate_pdb_file):
            pdb_bytes = pdb_path.read_bytes()
            provenance["structure_file_pdb_path"] = str(pdb_path)
            provenance["structure_file_pdb_size_bytes"] = len(pdb_bytes)
        else:
            try:
                pdb_bytes = _download_bytes(pdb_url)
                pdb_path.write_bytes(pdb_bytes)
                provenance["structure_file_pdb_path"] = str(pdb_path)
                provenance["structure_file_pdb_size_bytes"] = len(pdb_bytes)
            except Exception as exc:
                logger.warning("PDB download failed for %s: %s", pdb_id, exc)
                provenance["structure_file_pdb_path"] = None

    return provenance


def fetch_mmcif_supplement(
    pdb_id: str,
    structures_dir: Path | None = None,
    mirror: str = "rcsb",
) -> dict[str, Any] | None:
    """Fetch and parse a compact mmCIF supplement for one PDB entry.

    If a saved mmCIF file exists in structures_dir (or the default
    location), it is read from disk.  Otherwise it is downloaded and
    saved permanently.
    """
    pdb_id = pdb_id.upper()
    struct_dir = structures_dir or _DEFAULT_STRUCTURES_DIR
    saved_path = struct_dir / f"{pdb_id}.cif"

    # Try saved structure file first
    if reuse_existing_file(saved_path, validator=validate_mmcif_file):
        text = saved_path.read_text(encoding="utf-8")
        return parse_mmcif_supplement(text)

    # Fall back to legacy cache location
    legacy_path = _DEFAULT_RAW_DIR / f"{pdb_id}.cif"
    if reuse_existing_file(legacy_path, validator=validate_mmcif_file):
        text = legacy_path.read_text(encoding="utf-8")
        return parse_mmcif_supplement(text)

    # Download fresh
    try:
        cif_url, _ = _structure_urls(pdb_id, mirror)
        text = _download_bytes(cif_url).decode("utf-8")
    except Exception:
        raise
    # Save to structures dir for future use
    struct_dir.mkdir(parents=True, exist_ok=True)
    saved_path.write_text(text, encoding="utf-8")
    return parse_mmcif_supplement(text)


def parse_mmcif_supplement(text: str) -> dict[str, Any]:
    """Parse coordinate-derived structural signals from mmCIF text."""
    block = gemmi.cif.read_string(text).sole_block()

    entity_types = {eid: etype for eid, etype in _read_pairs(block, "_entity.id", "_entity.type")}

    poly_types: dict[str, str] = {}
    poly_sequences: dict[str, str] = {}
    for eid, ptype in _read_pairs(block, "_entity_poly.entity_id", "_entity_poly.type"):
        poly_types[eid] = ptype
    seq_ids = _read_column(block, "_entity_poly.entity_id")
    seq_vals = _read_column(block, "_entity_poly.pdbx_seq_one_letter_code_can")
    if seq_ids and len(seq_ids) == len(seq_vals):
        for eid, seq in zip(seq_ids, seq_vals):
            if seq not in _MISSING:
                poly_sequences[eid] = seq.replace("\n", "").strip()

    residue_counts = Counter(
        eid for eid in _read_column(block, "_entity_poly_seq.entity_id") if eid not in _MISSING
    )

    chains_by_entity: dict[str, set[str]] = defaultdict(set)
    for asym_id, entity_id in _read_pairs(block, "_struct_asym.id", "_struct_asym.entity_id"):
        if asym_id not in _MISSING and entity_id not in _MISSING:
            chains_by_entity[entity_id].add(asym_id)

    polymer_entities: list[dict[str, Any]] = []
    for entity_id, poly_type in poly_types.items():
        polymer_entities.append({
            "entity_id": entity_id,
            "entity_type": entity_types.get(entity_id, "polymer"),
            "poly_type": poly_type,
            "sequence": poly_sequences.get(entity_id),
            "residue_count": residue_counts.get(entity_id),
            "chain_ids": sorted(chains_by_entity.get(entity_id, set())),
        })

    # Track branched entities separately; these are often glycans omitted from the GraphQL view.
    branched_entities: list[dict[str, Any]] = []
    for entity_id, entity_type in entity_types.items():
        if _norm(entity_type) != "branched":
            continue
        branched_entities.append({
            "entity_id": entity_id,
            "entity_type": entity_type,
            "chain_ids": sorted(chains_by_entity.get(entity_id, set())),
        })

    atom_group = _read_column(block, "_atom_site.group_PDB")
    atom_entity = _read_column(block, "_atom_site.label_entity_id")
    atom_comp = _read_column(block, "_atom_site.label_comp_id")
    atom_auth_asym = _read_column(block, "_atom_site.auth_asym_id")
    atom_label_asym = _read_column(block, "_atom_site.label_asym_id")
    atom_auth_seq = _read_column(block, "_atom_site.auth_seq_id")
    atom_label_seq = _read_column(block, "_atom_site.label_seq_id")

    row_count = len(atom_group)
    atom_columns = [
        atom_entity,
        atom_comp,
        atom_auth_asym,
        atom_label_asym,
        atom_auth_seq,
        atom_label_seq,
    ]
    if any(len(col) != row_count for col in atom_columns):
        raise ValueError("Mismatched _atom_site column lengths in mmCIF supplement")

    instance_keys: set[tuple[str, str, str, str]] = set()
    nonpolymer_instances: list[dict[str, str]] = []
    for group, entity_id, comp_id, auth_asym, label_asym, auth_seq, label_seq in zip(
        atom_group,
        atom_entity,
        atom_comp,
        atom_auth_asym,
        atom_label_asym,
        atom_auth_seq,
        atom_label_seq,
    ):
        if group != "HETATM" or entity_id in _MISSING or comp_id in _MISSING:
            continue
        entity_type = _norm(entity_types.get(entity_id, ""))
        if entity_type != "non-polymer":
            continue
        chain_id = auth_asym if auth_asym not in _MISSING else label_asym
        residue_id = auth_seq if auth_seq not in _MISSING else label_seq
        key = (entity_id, comp_id, chain_id, residue_id)
        if key in instance_keys:
            continue
        instance_keys.add(key)
        nonpolymer_instances.append({
            "entity_id": entity_id,
            "comp_id": comp_id,
            "chain_id": chain_id,
            "residue_id": residue_id,
        })

    # ── Water entity count ──────────────────────────────────────────
    water_count = 0
    for entity_id, entity_type in entity_types.items():
        if _norm(entity_type) == "water":
            # Count unique water residues via struct_asym chain count
            water_count += len(chains_by_entity.get(entity_id, set()))

    # ── Branched entity count ────────────────────────────────────────
    branched_entity_count = len(branched_entities)

    return {
        "polymer_entities": polymer_entities,
        "branched_entities": branched_entities,
        "branched_entity_count": branched_entity_count,
        "nonpolymer_instances": nonpolymer_instances,
        "water_count": water_count,
        "entity_types": entity_types,
        "chains_by_entity": {k: sorted(v) for k, v in chains_by_entity.items()},
    }


def parse_structure_quality(text: str) -> dict[str, Any]:
    """Extract structure quality fields from mmCIF text (spec group 13).

    Reads _refine, _pdbx_struct_oper_list, _pdbx_unobs_or_zero_occ_residues,
    and _atom_site alternate conformer / occupancy columns.
    """
    block = gemmi.cif.read_string(text).sole_block()
    result: dict[str, Any] = {}

    # R-work / R-free from _refine
    for tag, key in [
        ("_refine.ls_R_factor_R_work", "r_work"),
        ("_refine.ls_R_factor_R_free", "r_free"),
        ("_refine.ls_d_res_high", "refinement_resolution_high"),
    ]:
        vals = _read_column(block, tag)
        if vals and vals[0] not in _MISSING:
            try:
                result[key] = round(float(vals[0]), 4)
            except (TypeError, ValueError):
                pass

    # Model count from _pdbx_struct_oper_list or _atom_site.pdbx_PDB_model_num
    model_nums = _read_column(block, "_atom_site.pdbx_PDB_model_num")
    if model_nums:
        result["model_count"] = len(set(model_nums) - _MISSING)

    # Alternate conformers
    alt_ids = _read_column(block, "_atom_site.label_alt_id")
    non_trivial_alt = {a for a in alt_ids if a not in _MISSING and a != "."}
    result["contains_alternate_locations"] = len(non_trivial_alt) > 0

    # Partial occupancy
    occupancies = _read_column(block, "_atom_site.occupancy")
    has_partial = False
    for occ in occupancies:
        if occ not in _MISSING:
            try:
                if float(occ) < 1.0:
                    has_partial = True
                    break
            except (TypeError, ValueError):
                pass
    result["contains_partial_occupancy"] = has_partial

    # Missing residues (from _pdbx_unobs_or_zero_occ_residues)
    missing_ids = _read_column(block, "_pdbx_unobs_or_zero_occ_residues.auth_seq_id")
    result["missing_residue_count"] = len([m for m in missing_ids if m not in _MISSING])
    result["contains_missing_residues"] = result["missing_residue_count"] > 0

    return result
