"""AlphaFold source scaffolding.

Assumptions:
- AlphaFold-derived structures are predicted states, not experimental archive entries.
- They must remain provenance-distinct from RCSB/mmCIF experimental structures.
- This module currently plans and catalogs AlphaFold usage; it does not run structure prediction.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import requests

from pbdata.storage import reuse_existing_file

_ALPHAFOLD_PREDICTION_URL = "https://alphafold.ebi.ac.uk/api/prediction/{accession}"
_TIMEOUT = 60


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


@dataclass(frozen=True)
class AlphaFoldStructureRecord:
    accession: str
    entry_id: str
    model_created_date: str
    model_version: str
    uniprot_start: int | None
    uniprot_end: int | None
    cif_url: str
    pae_url: str
    plddt_url: str
    status: str = "ready"


def _validate_alphafold_json(path: Path, *, expected_accession: str | None = None) -> bool:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return False
    if not isinstance(raw, list) or not raw:
        return False
    first = raw[0]
    if not isinstance(first, dict):
        return False
    accession = str(first.get("uniprotAccession") or "").strip().upper()
    if expected_accession and accession != expected_accession.strip().upper():
        return False
    return bool(accession)


class AlphaFoldAdapter:
    """Fetch AlphaFold DB prediction metadata by UniProt accession."""

    def __init__(
        self,
        *,
        cache_dir: Path | None = None,
        timeout: int = _TIMEOUT,
        storage_root: Path | None = None,
    ) -> None:
        self.cache_dir = cache_dir
        self.timeout = timeout
        self.storage_root = storage_root

    def fetch_metadata(self, accession: str) -> list[dict[str, Any]]:
        accession = accession.strip().upper()
        if not accession:
            raise ValueError("AlphaFold accession must be non-empty.")

        cache_path = (self.cache_dir / f"{accession}.json") if self.cache_dir is not None else None
        if cache_path is not None and reuse_existing_file(
            cache_path,
            validator=lambda path, expected=accession: _validate_alphafold_json(path, expected_accession=expected),
        ):
            raw = json.loads(cache_path.read_text(encoding="utf-8"))
            return raw if isinstance(raw, list) else []

        response = requests.get(
            _ALPHAFOLD_PREDICTION_URL.format(accession=accession),
            headers={"Accept": "application/json"},
            timeout=self.timeout,
        )
        if response.status_code == 404:
            raise requests.HTTPError(f"AlphaFold prediction not found: {accession}", response=response)
        response.raise_for_status()
        raw = response.json()
        if cache_path is not None:
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            cache_path.write_text(json.dumps(raw, indent=2), encoding="utf-8")
        return raw if isinstance(raw, list) else []

    def normalize_record(self, raw: dict[str, Any]) -> AlphaFoldStructureRecord:
        accession = str(raw.get("uniprotAccession") or "").strip().upper()
        entry_id = str(raw.get("entryId") or "").strip()
        created_date = str(raw.get("latestVersion") or raw.get("modelCreatedDate") or "").strip()
        return AlphaFoldStructureRecord(
            accession=accession,
            entry_id=entry_id,
            model_created_date=created_date,
            model_version=str(raw.get("latestVersion") or "").strip(),
            uniprot_start=_safe_int(raw.get("uniprotStart")),
            uniprot_end=_safe_int(raw.get("uniprotEnd")),
            cif_url=str(raw.get("cifUrl") or "").strip(),
            pae_url=str(raw.get("paeDocUrl") or "").strip(),
            plddt_url=str(raw.get("bcifUrl") or raw.get("pdbUrl") or "").strip(),
        )

    def fetch_prediction(self, accession: str) -> AlphaFoldStructureRecord:
        local_record = self._fetch_local_index_record(accession)
        if local_record is not None:
            return local_record
        raw = self.fetch_metadata(accession)
        if not raw:
            raise ValueError(f"No AlphaFold prediction records returned for {accession}.")
        first = raw[0]
        if not isinstance(first, dict):
            raise ValueError(f"Unexpected AlphaFold payload for {accession}.")
        return self.normalize_record(first)

    def _fetch_local_index_record(self, accession: str) -> AlphaFoldStructureRecord | None:
        if self.storage_root is None:
            return None
        try:
            from pbdata.source_indexes import query_alphafold_archive_index
            from pbdata.storage import build_storage_layout

            payload = query_alphafold_archive_index(
                build_storage_layout(self.storage_root),
                accession,
            )
        except Exception:
            return None
        if not payload:
            return None
        return AlphaFoldStructureRecord(
            accession=str(payload.get("accession") or "").strip().upper(),
            entry_id=str(payload.get("entry_id") or "").strip(),
            model_created_date="",
            model_version=str(payload.get("model_version") or "").strip(),
            uniprot_start=None,
            uniprot_end=None,
            cif_url="",
            pae_url="",
            plddt_url="",
        )


def _safe_int(value: Any) -> int | None:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None
