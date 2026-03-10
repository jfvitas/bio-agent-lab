"""Ligand-screening workflow scaffold."""

from dataclasses import dataclass


@dataclass(frozen=True)
class LigandScreeningPlan:
    status: str = "stub"
    accepted_inputs: tuple[str, ...] = ("SMILES", "SDF", "PDB", "mmCIF", "FASTA")


def plan_ligand_screening() -> LigandScreeningPlan:
    return LigandScreeningPlan()
