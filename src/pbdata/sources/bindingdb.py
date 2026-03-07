"""BindingDB source adapter.

BindingDB (https://www.bindingdb.org) publishes protein-ligand binding
affinity measurements.  This adapter fetches records via the BindingDB
REST API using PDB entry IDs as the join key to RCSB data.

API used:
  GET https://bindingdb.org/axis2/services/BDBService/getLigandsByPdb
  Returns XML (converted to JSON here) with binding affinities, ligand
  SMILES, and assay metadata for a given PDB ID.

Assay values are normalised to nM (nanomolar) where possible.

Rate-limiting: BindingDB asks users to limit to ~3 requests/second.
This adapter enforces a 0.35s inter-request delay.
"""

from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any

import requests

from pbdata.schemas.canonical_sample import CanonicalBindingSample
from pbdata.sources.base import BaseAdapter

_ADAPTER_VERSION = "0.1.0"
_BASE_URL = "https://bindingdb.org/axis2/services/BDBService"
_TIMEOUT  = 30  # seconds
_DELAY    = 0.35  # inter-request delay (seconds)

# Multipliers to convert common assay units to nM
_TO_NM: dict[str, float] = {
    "nm":  1.0,
    "um":  1_000.0,
    "mm":  1_000_000.0,
    "m":   1_000_000_000.0,
    "pm":  0.001,
}

_ASSAY_TYPES = {"ki", "kd", "ic50", "ec50"}


# ---------------------------------------------------------------------------
# Unit helpers
# ---------------------------------------------------------------------------

def _parse_affinity(value_str: str, unit_str: str) -> tuple[float | None, str | None, float | None]:
    """Parse raw affinity string and unit.

    Returns (assay_value, assay_unit, assay_value_standardized_nM).
    'assay_value_standardized' is the value converted to nM, or None if
    conversion is not possible.
    """
    # Strip inequality prefixes: ">", "<", ">=", "<="
    cleaned = value_str.strip().lstrip("<>=").strip()
    try:
        val = float(cleaned)
    except (ValueError, TypeError):
        return None, None, None

    unit = unit_str.strip().lower() if unit_str else ""
    multiplier = _TO_NM.get(unit)
    std = round(val * multiplier, 6) if multiplier is not None else None
    return val, unit_str.strip() if unit_str else None, std


# ---------------------------------------------------------------------------
# XML / JSON parsing helpers
# ---------------------------------------------------------------------------

def _first(d: dict, *keys: str) -> Any:
    """Return first non-None value for any key in d."""
    for k in keys:
        v = d.get(k)
        if v is not None:
            return v
    return None


def _parse_monomer(mono: dict[str, Any], pdb_id: str) -> list[CanonicalBindingSample]:
    """Convert a single BindingDB 'affinities' block to CanonicalBindingSample list.

    One monomer entry maps to one sample (the best/primary affinity value).
    """
    ligand_smiles: str | None = _first(mono, "smiles", "Smiles")
    ligand_id:     str | None = _first(mono, "cid", "monomerID")
    ligand_name:   str | None = _first(mono, "name", "monomerName")

    results: list[CanonicalBindingSample] = []

    affinities = mono.get("affinities") or []
    if not affinities:
        return results

    for aff in affinities:
        assay_raw   = _first(aff, "affinity", "Affinity") or ""
        unit_raw    = _first(aff, "affinityUnit", "AffinityUnit") or ""
        assay_type  = (_first(aff, "affinityType", "AffinityType") or "").lower()
        target_name = _first(aff, "target", "Target", "targetName")
        uniprot     = _first(aff, "uniprotID", "uniprot_primary_id")

        if assay_type not in _ASSAY_TYPES:
            assay_type = None  # type: ignore[assignment]

        val, unit, std = _parse_affinity(assay_raw, unit_raw)

        import math
        log10: float | None = None
        if std is not None and std > 0:
            log10 = round(math.log10(std), 6)

        record_id = f"BDB_{pdb_id}_{ligand_id or 'UNK'}_{assay_type or 'aff'}"

        provenance: dict[str, Any] = {
            "source_database": "BindingDB",
            "ingested_at": datetime.now(timezone.utc).isoformat(),
            "adapter_version": _ADAPTER_VERSION,
            "target_name": target_name,
            "ligand_name": ligand_name,
        }

        results.append(CanonicalBindingSample(
            sample_id=record_id,
            task_type="protein_ligand",
            source_database="BindingDB",
            source_record_id=record_id,
            pdb_id=pdb_id.upper(),
            uniprot_ids=[uniprot] if uniprot else None,
            ligand_id=ligand_id,
            ligand_smiles=ligand_smiles,
            assay_type=assay_type,
            assay_value=val,
            assay_unit=unit,
            assay_value_standardized=std,
            assay_value_log10=log10,
            provenance=provenance,
            quality_flags=[],
            quality_score=0.0,
        ))

    return results


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------

class BindingDBAdapter(BaseAdapter):
    """Adapter for BindingDB binding-affinity data.

    Fetches ligand binding data by PDB ID using the BindingDB REST API.
    Each (PDB ID, ligand, assay type) triple becomes one CanonicalBindingSample.
    """

    @property
    def source_name(self) -> str:
        return "BindingDB"

    def fetch_metadata(self, record_id: str) -> dict[str, Any]:
        """Fetch raw BindingDB JSON for a single PDB ID.

        Args:
            record_id: A 4-character PDB ID (case-insensitive).

        Returns:
            Parsed JSON response dict from the BindingDB API.
        """
        url = f"{_BASE_URL}/getLigandsByPdb"
        params = {"pdb": record_id.upper(), "repr": "json"}
        resp = requests.get(url, params=params, timeout=_TIMEOUT)
        resp.raise_for_status()
        time.sleep(_DELAY)
        return resp.json()

    def normalize_record(self, raw: dict[str, Any]) -> CanonicalBindingSample:
        """Return the first CanonicalBindingSample parsed from a raw BindingDB dict.

        For multi-affinity entries, call normalize_all() instead.
        """
        records = self.normalize_all(raw)
        if not records:
            raise ValueError("No usable affinities found in BindingDB record.")
        return records[0]

    def normalize_all(self, raw: dict[str, Any]) -> list[CanonicalBindingSample]:
        """Return all CanonicalBindingSample records from a raw BindingDB response.

        A single PDB entry may have affinities for multiple ligands and assay
        types, each yielding an independent sample.
        """
        pdb_id: str = (raw.get("pdb_id") or raw.get("pdbID") or "").upper()
        monomers: list[dict] = raw.get("affinities") or raw.get("monomers") or []
        results: list[CanonicalBindingSample] = []
        for mono in monomers:
            results.extend(_parse_monomer(mono, pdb_id))
        return results

    def fetch_by_pdb(self, pdb_id: str) -> list[CanonicalBindingSample]:
        """Convenience wrapper: fetch + normalize in one call.

        Returns a list of records (empty if the PDB ID has no BindingDB data).
        """
        try:
            raw = self.fetch_metadata(pdb_id)
        except requests.HTTPError:
            return []
        return self.normalize_all(raw)
