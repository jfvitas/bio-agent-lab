"""Optional molecular-mechanics feature planning hooks.

This module intentionally does not run CHARMM/AMBER/QM yet. It defines the
staging boundary for denser physical features that go beyond categorical
encodings or raw structure metadata.

Recommended staged approach:
1. Fast local proxies from the existing structure file.
2. Per-residue protonation / microstate assignment in local structural context.
3. Classical MM electrostatics / minimization around the binding environment.
4. Targeted semiempirical or QM/MM refinement for the highest-value local region.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


TierName = Literal[
    "structure_proxy",
    "microstate_assignment",
    "classical_mm_refinement",
    "local_qm_refinement",
]


@dataclass(frozen=True)
class MolecularMechanicsFeaturePlan:
    structure_id: str
    status: str = "stub"
    tier: TierName = "structure_proxy"
    recommended_engine: str = "gemmi"
    notes: str = ""


def plan_mm_features(structure_id: str) -> MolecularMechanicsFeaturePlan:
    return MolecularMechanicsFeaturePlan(
        structure_id=structure_id,
        notes=(
            "Start with structure-derived continuous descriptors already available "
            "from the local mmCIF. Add external MM/QM only after caching, "
            "protonation-state handling, and local-region extraction are finalized."
        ),
    )


def plan_microstate_assignment(structure_id: str) -> MolecularMechanicsFeaturePlan:
    return MolecularMechanicsFeaturePlan(
        structure_id=structure_id,
        tier="microstate_assignment",
        recommended_engine="AmberTools_or_CHARMM_with_explicit_protonation_workflow",
        notes=(
            "Assign context-sensitive residue and ligand states before MM scoring. "
            "This is the stage where Asp/Glu/Lys/His/terminal states should become "
            "environment-aware rather than residue-name defaults."
        ),
    )


def plan_classical_mm_refinement(structure_id: str) -> MolecularMechanicsFeaturePlan:
    return MolecularMechanicsFeaturePlan(
        structure_id=structure_id,
        tier="classical_mm_refinement",
        recommended_engine="OpenMM_plus_AMBER_or_CHARMM_force_fields",
        notes=(
            "Refine only the local binding environment, not the full structure. "
            "Use this stage for Coulombic potentials, GB/SA-like terms, local "
            "strain, and environment-dependent atomic descriptors."
        ),
    )


def plan_local_qm_refinement(structure_id: str) -> MolecularMechanicsFeaturePlan:
    return MolecularMechanicsFeaturePlan(
        structure_id=structure_id,
        tier="local_qm_refinement",
        recommended_engine="semiempirical_or_QM_MM_local_cluster",
        notes=(
            "Reserve for a small local region: binding-site residues, ligand, metals, "
            "and catalytic waters. Full-structure ab initio is not operationally "
            "realistic for dataset-scale feature generation."
        ),
    )
