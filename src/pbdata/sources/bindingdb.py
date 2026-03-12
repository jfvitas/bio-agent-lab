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

import math
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
_RETRY_ATTEMPTS = 3
_RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}

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

def _parse_affinity(
    value_str: str,
    unit_str: str,
) -> tuple[float | None, str | None, float | None, str | None]:
    """Parse raw affinity string and unit.

    Returns (assay_value, assay_unit, assay_value_standardized_nM, relation).
    'assay_value_standardized' is the value converted to nM, or None if
    conversion is not possible.
    """
    raw_text = str(value_str or "").strip()
    relation = None
    if raw_text.startswith(">="):
        relation = ">="
    elif raw_text.startswith("<="):
        relation = "<="
    elif raw_text.startswith(">"):
        relation = ">"
    elif raw_text.startswith("<"):
        relation = "<"
    elif raw_text.startswith("~"):
        relation = "approx"

    cleaned = raw_text.lstrip("<>~= ").strip()
    try:
        val = float(cleaned)
    except (ValueError, TypeError):
        return None, None, None, relation

    unit = unit_str.strip().lower() if unit_str else ""
    multiplier = _TO_NM.get(unit)
    std = round(val * multiplier, 6) if multiplier is not None else None
    return val, unit_str.strip() if unit_str else None, std, relation


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
        assay_raw   = _first(aff, "affinity", "Affinity", "value", "Value") or ""
        unit_raw    = _first(aff, "affinityUnit", "AffinityUnit", "unit", "Unit") or ""
        assay_type  = (_first(aff, "affinityType", "AffinityType") or "").lower()
        target_name = _first(aff, "target", "Target", "targetName")
        uniprot     = _first(aff, "uniprotID", "uniprot_primary_id")
        reference_text = _first(aff, "articleTitle", "ArticleTitle", "referenceText", "ReferenceText")
        pubmed_id = _first(aff, "pubmedId", "PubMedID", "pmid", "PMID")
        doi = _first(aff, "doi", "DOI")
        mutation_string = _first(aff, "mutation", "Mutation", "mutationString", "MutationString")
        assay_method = _first(aff, "assayMethod", "AssayMethod")
        temperature_raw = _first(aff, "temperature", "Temperature")
        ph_raw = _first(aff, "pH", "ph", "PH")
        chain_ids_raw = _first(aff, "chainIDs", "chainIds", "chainId", "ChainIDs", "ChainId")

        if assay_type not in _ASSAY_TYPES:
            assay_type = None  # type: ignore[assignment]

        val, unit, std, relation = _parse_affinity(assay_raw, unit_raw)
        log10: float | None = None
        if std is not None and std > 0:
            log10 = round(math.log10(std), 6)

        chain_ids_receptor = None
        if chain_ids_raw:
            chain_ids_receptor = [
                token.strip()
                for token in str(chain_ids_raw).replace(";", ",").split(",")
                if token.strip()
            ] or None

        temperature_c = None
        if temperature_raw not in (None, ""):
            try:
                temperature_c = float(temperature_raw)
            except (TypeError, ValueError):
                temperature_c = None

        ph = None
        if ph_raw not in (None, ""):
            try:
                ph = float(ph_raw)
            except (TypeError, ValueError):
                ph = None

        record_id = f"BDB_{pdb_id}_{ligand_id or 'UNK'}_{assay_type or 'aff'}"

        provenance: dict[str, Any] = {
            "source_database": "BindingDB",
            "ingested_at": datetime.now(timezone.utc).isoformat(),
            "adapter_version": _ADAPTER_VERSION,
            "target_name": target_name,
            "ligand_name": ligand_name,
            "reference_text": reference_text,
            "pubmed_id": str(pubmed_id) if pubmed_id not in (None, "") else None,
            "doi": str(doi) if doi not in (None, "") else None,
            "raw_affinity_text": str(assay_raw) if assay_raw else None,
            "standard_relation": relation,
            "assay_method": assay_method,
        }

        results.append(CanonicalBindingSample(
            sample_id=record_id,
            task_type="protein_ligand",
            source_database="BindingDB",
            source_record_id=record_id,
            pdb_id=pdb_id.upper(),
            chain_ids_receptor=chain_ids_receptor,
            uniprot_ids=[uniprot] if uniprot else None,
            ligand_id=ligand_id,
            ligand_smiles=ligand_smiles,
            assay_type=assay_type,
            assay_value=val,
            assay_unit=unit,
            assay_value_standardized=std,
            assay_value_log10=log10,
            temperature_c=temperature_c,
            ph=ph,
            mutation_string=str(mutation_string) if mutation_string not in (None, "") else None,
            wildtype_or_mutant="mutant" if mutation_string not in (None, "") else "wildtype",
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
        last_error: Exception | None = None
        for attempt in range(1, _RETRY_ATTEMPTS + 1):
            try:
                resp = requests.get(url, params=params, timeout=_TIMEOUT)
                resp.raise_for_status()
                time.sleep(_DELAY)
                return resp.json()
            except requests.RequestException as exc:
                last_error = exc
                status_code = getattr(getattr(exc, "response", None), "status_code", None)
                retryable = status_code in _RETRYABLE_STATUS_CODES or isinstance(exc, (requests.Timeout, requests.ConnectionError))
                if not retryable or attempt >= _RETRY_ATTEMPTS:
                    raise
                time.sleep(_DELAY * attempt)
        if last_error is not None:
            raise last_error
        raise RuntimeError("Unreachable BindingDB request state")

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
