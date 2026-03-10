"""Conformational-state schema aligned to the instruction pack."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict


class ConformationalStateRecord(BaseModel):
    model_config = ConfigDict(frozen=True)

    target_id: str
    state_id: str
    pdb_id: str | None = None
    structure_source: str
    apo_or_holo: str | None = None
    active_inactive_unknown: str | None = None
    open_closed_unknown: str | None = None
    ligand_class_in_state: str | None = None
    conformation_cluster: str | None = None
    provenance: dict[str, str | None]
