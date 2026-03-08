"""PDBbind source adapter.

PDBbind (https://www.pdbbind-plus.org.cn/) is a curated database of
experimentally measured binding affinities for protein-ligand, protein-
protein, and protein-nucleic acid complexes taken from the PDB.

This adapter supports the common local-file workflow:
1. Register and download the PDBbind dataset manually.
2. Extract it to a local directory.
3. Point the adapter at that directory and parse the index file.

The implementation here is intentionally conservative. It extracts only
the fields that are stable across the published INDEX_general_PL_data
files: PDB ID, resolution, release year, and affinity label/value/unit.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pbdata.schemas.canonical_sample import CanonicalBindingSample
from pbdata.sources.base import BaseAdapter

_ADAPTER_VERSION = "0.2.0"
_AFFINITY_RE = re.compile(
    r"(?P<kind>Kd|Ki|IC50)\s*=\s*(?P<value>[<>~]?\d+(?:\.\d+)?)\s*(?P<unit>fM|pM|nM|uM|mM|M)?",
    re.IGNORECASE,
)
_TO_NM = {
    "fm": 0.000001,
    "pm": 0.001,
    "nm": 1.0,
    "um": 1000.0,
    "mm": 1000000.0,
    "m": 1000000000.0,
}


def _parse_affinity_token(token: str) -> tuple[str | None, float | None, str | None, float | None]:
    match = _AFFINITY_RE.search(token.strip())
    if not match:
        return None, None, None, None

    kind = match.group("kind")
    unit = match.group("unit")
    value_str = match.group("value").lstrip("<>~")
    try:
        value = float(value_str)
    except ValueError:
        return kind, None, unit, None

    standardized = None
    if unit:
        factor = _TO_NM.get(unit.lower())
        if factor is not None:
            standardized = round(value * factor, 6)
    return kind, value, unit, standardized


def _parse_index_line(line: str) -> dict[str, Any] | None:
    stripped = line.strip()
    if not stripped or stripped.startswith("#"):
        return None

    head, sep, tail = stripped.partition("//")
    tokens = head.split()
    if len(tokens) < 5:
        return None

    pdb_id = tokens[0].upper()
    try:
        resolution = float(tokens[1])
    except ValueError:
        resolution = None
    try:
        release_year = int(tokens[2])
    except ValueError:
        release_year = None

    affinity_type, affinity_value, affinity_unit, standardized = _parse_affinity_token(tokens[4])
    return {
        "pdb_id": pdb_id,
        "resolution": resolution,
        "release_year": release_year,
        "raw_affinity": tokens[4],
        "affinity_type": affinity_type,
        "affinity_value": affinity_value,
        "affinity_unit": affinity_unit,
        "affinity_value_standardized": standardized,
        "reference_text": tail.strip() if sep else None,
        "raw_line": stripped,
    }


def load_pdbbind_index(local_dir: Path) -> list[dict[str, Any]]:
    index_dir = local_dir / "index"
    index_files = sorted(index_dir.glob("INDEX_general_PL_data*"))
    if not index_files:
        raise FileNotFoundError(f"No PDBbind index file found under {index_dir}")

    rows: list[dict[str, Any]] = []
    text = index_files[0].read_text(encoding="utf-8", errors="ignore")
    for line in text.splitlines():
        row = _parse_index_line(line)
        if row is not None:
            rows.append(row)
    return rows


class PDBbindAdapter(BaseAdapter):
    """Adapter for local PDBbind binding-affinity data."""

    def __init__(self, local_dir: Path | None = None) -> None:
        self._local_dir = local_dir

    @property
    def source_name(self) -> str:
        return "PDBbind"

    def fetch_metadata(self, record_id: str) -> dict[str, Any]:
        if self._local_dir is None:
            raise FileNotFoundError("PDBbind local_dir is required.")
        pdb_id = record_id.upper()
        for row in load_pdbbind_index(self._local_dir):
            if row["pdb_id"] == pdb_id:
                return row
        raise KeyError(f"PDBbind entry not found for {pdb_id}")

    def normalize_record(self, raw: dict[str, Any]) -> CanonicalBindingSample:
        pdb_id = (raw.get("pdb_id") or "").upper()
        affinity_type = raw.get("affinity_type")
        affinity_value = raw.get("affinity_value")
        standardized = raw.get("affinity_value_standardized")

        log10_value = None
        if standardized is not None and standardized > 0:
            import math
            log10_value = round(math.log10(standardized), 6)

        return CanonicalBindingSample(
            sample_id=f"PDBBIND_{pdb_id}_{affinity_type or 'AFF'}",
            task_type="protein_ligand",
            source_database="PDBbind",
            source_record_id=pdb_id,
            pdb_id=pdb_id,
            structure_resolution=raw.get("resolution"),
            assay_type=affinity_type,
            assay_value=affinity_value,
            assay_unit=raw.get("affinity_unit"),
            assay_value_standardized=standardized,
            assay_value_log10=log10_value,
            provenance={
                "source_database": "PDBbind",
                "ingested_at": datetime.now(timezone.utc).isoformat(),
                "adapter_version": _ADAPTER_VERSION,
                "reference_text": raw.get("reference_text"),
                "raw_affinity": raw.get("raw_affinity"),
            },
            quality_flags=[],
            quality_score=0.0,
        )

    def fetch_all(self) -> list[CanonicalBindingSample]:
        if self._local_dir is None:
            raise FileNotFoundError("PDBbind local_dir is required.")
        return [self.normalize_record(row) for row in load_pdbbind_index(self._local_dir)]
