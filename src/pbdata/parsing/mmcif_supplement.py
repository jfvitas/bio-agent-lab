"""Lightweight mmCIF supplement parsing for structural edge cases.

This module augments the GraphQL metadata with coordinate-derived signals:
- per-instance nonpolymer residues (for repeated metals/cofactors/ligands)
- branched carbohydrate entities
- polymer entity chain IDs / residue counts when GraphQL is incomplete

The goal is conservative enrichment, not a full structural parser.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

import gemmi
import requests

_MMCIF_URL = "https://files.rcsb.org/download/{pdb_id}.cif"
_TIMEOUT = 60
_MISSING = {"", ".", "?"}
_DEFAULT_RAW_DIR = Path("data/raw/rcsb")


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


def fetch_mmcif_supplement(pdb_id: str) -> dict[str, Any] | None:
    """Fetch and parse a compact mmCIF supplement for one PDB entry."""
    pdb_id = pdb_id.upper()
    try:
        response = requests.get(_MMCIF_URL.format(pdb_id=pdb_id), timeout=_TIMEOUT)
        response.raise_for_status()
        text = response.text
    except Exception:
        cache_path = _DEFAULT_RAW_DIR / f"{pdb_id}.cif"
        if not cache_path.exists():
            raise
        text = cache_path.read_text(encoding="utf-8")
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

    return {
        "polymer_entities": polymer_entities,
        "branched_entities": branched_entities,
        "nonpolymer_instances": nonpolymer_instances,
    }
