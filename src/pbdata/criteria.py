"""Search criteria for RCSB data collection.

Criteria are persisted to configs/criteria.yaml and loaded on startup.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field

# ---------------------------------------------------------------------------
# Constants exposed to the GUI
# ---------------------------------------------------------------------------

EXPERIMENTAL_METHODS: dict[str, str] = {
    "xray":    "X-RAY DIFFRACTION",
    "em":      "ELECTRON MICROSCOPY",
    "nmr":     "SOLUTION NMR",
    "neutron": "NEUTRON DIFFRACTION",
}

RESOLUTION_OPTIONS: list[str] = [
    "No limit", "1.5 Å", "2.0 Å", "2.5 Å", "3.0 Å", "4.0 Å", "5.0 Å",
]


def resolution_label_to_value(label: str) -> float | None:
    """Convert a display label like '2.5 Å' to a float, or None for 'No limit'."""
    if label == "No limit":
        return None
    return float(label.replace(" Å", ""))


def resolution_value_to_label(value: float | None) -> str:
    """Convert a float (or None) back to its display label."""
    if value is None:
        return "No limit"
    return f"{value:g} Å"


# ---------------------------------------------------------------------------
# Model
# ---------------------------------------------------------------------------

class SearchCriteria(BaseModel):
    """Criteria for filtering RCSB entries during ingestion."""

    experimental_methods: list[str] = Field(
        default_factory=lambda: ["xray", "em"],
        description="Active method keys: xray | em | nmr | neutron",
    )
    max_resolution_angstrom: float | None = 3.0
    task_types: list[str] = Field(
        default_factory=lambda: ["protein_ligand", "protein_protein"],
        description="protein_ligand | protein_protein | mutation_ddg",
    )
    require_protein: bool = True
    min_release_year: int | None = None

    def rcsb_method_labels(self) -> list[str]:
        """Return RCSB API method strings for the active method keys."""
        return [
            EXPERIMENTAL_METHODS[m]
            for m in self.experimental_methods
            if m in EXPERIMENTAL_METHODS
        ]


# ---------------------------------------------------------------------------
# Load / save
# ---------------------------------------------------------------------------

def load_criteria(path: str | Path) -> SearchCriteria:
    """Load SearchCriteria from YAML, returning defaults if the file is absent."""
    p = Path(path)
    if not p.exists():
        return SearchCriteria()
    with p.open() as f:
        raw: dict[str, Any] = yaml.safe_load(f) or {}
    return SearchCriteria.model_validate(raw)


def save_criteria(criteria: SearchCriteria, path: str | Path) -> None:
    """Persist SearchCriteria to YAML."""
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w") as f:
        yaml.safe_dump(criteria.model_dump(), f, default_flow_style=False, sort_keys=False)
