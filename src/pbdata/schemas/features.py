"""Feature-layer record schema."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict


class FeatureRecord(BaseModel):
    """One materialized feature row for a protein-ligand or protein-protein context."""

    model_config = ConfigDict(frozen=True)

    feature_id: str
    pdb_id: str | None = None
    pair_identity_key: str | None = None
    feature_group: str
    values: dict[str, Any]
    provenance: dict[str, Any]
