"""Helpers for parsing and matching pair-identity keys."""

from __future__ import annotations

from dataclasses import dataclass


def chain_group_key(chains: list[str] | None) -> str:
    if not chains:
        return "-"
    return ",".join(sorted({c.strip() for c in chains if c and c.strip()}))


def split_chain_group(group: str | None) -> list[str]:
    if not group or group == "-":
        return []
    return [part for part in group.split(",") if part]


@dataclass(frozen=True)
class ParsedPairKey:
    raw_key: str
    task_type: str
    pdb_id: str | None = None
    subject_key: str | None = None
    receptor_chain_ids: tuple[str, ...] = ()
    partner_chain_ids: tuple[str, ...] = ()
    ligand_key: str | None = None
    mutation_key: str | None = None


def parse_pair_identity_key(key: str) -> ParsedPairKey | None:
    parts = key.split("|")
    if len(parts) == 4:
        task_type, pdb_id, subject_key, mutation_key = parts
        if task_type in {"protein_ligand", "protein_protein"}:
            return None
        return ParsedPairKey(
            raw_key=key,
            task_type=task_type,
            pdb_id=pdb_id or None,
            subject_key=subject_key or None,
            mutation_key=mutation_key or None,
        )

    if len(parts) != 5:
        return None

    task_type, pdb_id, left, right, mutation_key = parts
    if task_type == "protein_ligand":
        return ParsedPairKey(
            raw_key=key,
            task_type=task_type,
            pdb_id=pdb_id or None,
            subject_key=right or None,
            receptor_chain_ids=tuple(split_chain_group(left)),
            ligand_key=right or None,
            mutation_key=mutation_key or None,
        )
    if task_type == "protein_protein":
        return ParsedPairKey(
            raw_key=key,
            task_type=task_type,
            pdb_id=pdb_id or None,
            subject_key=right or None,
            receptor_chain_ids=tuple(split_chain_group(left)),
            partner_chain_ids=tuple(split_chain_group(right)),
            mutation_key=mutation_key or None,
        )
    return ParsedPairKey(
        raw_key=key,
        task_type=task_type,
        pdb_id=pdb_id or None,
        subject_key=right or None,
        mutation_key=mutation_key or None,
    )


def bound_object_matches_ligand_key(bound_object: dict, ligand_key: str | None) -> bool:
    if not ligand_key:
        return False
    candidates = {
        str(bound_object.get("component_inchikey") or ""),
        str(bound_object.get("component_id") or ""),
        str(bound_object.get("component_name") or ""),
        str(bound_object.get("primary_id") or ""),
    }
    return ligand_key in {candidate for candidate in candidates if candidate}
