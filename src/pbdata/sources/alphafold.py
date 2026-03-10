"""AlphaFold source scaffolding.

Assumptions:
- AlphaFold-derived structures are predicted states, not experimental archive entries.
- They must remain provenance-distinct from RCSB/mmCIF experimental structures.
- This module currently plans and catalogs AlphaFold usage; it does not run structure prediction.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class AlphaFoldStatePlan:
    target_id: str
    status: str = "planned"
    structure_source: str = "AlphaFold"
    notes: str = (
        "Use AlphaFold when only sequence is available or when conformational-state coverage "
        "is incomplete in the experimental archive."
    )


def plan_alphafold_state(target_id: str) -> AlphaFoldStatePlan:
    return AlphaFoldStatePlan(target_id=target_id)
