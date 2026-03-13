"""ChEMBL activity adapter using the official ChEMBL web services API.

This adapter is intentionally conservative:
- it uses exact identifier lookups where possible
- it only emits supported activity types
- it only groups mutant measurements when mutation annotations are explicit
- otherwise it prevents unsafe averaging by forcing per-activity grouping
"""

from __future__ import annotations

import math
import re
import time
from datetime import datetime, timezone
from typing import Any

import requests

from pbdata.schemas.canonical_sample import CanonicalBindingSample
from pbdata.sources.base import BaseAdapter

_ADAPTER_VERSION = "0.1.0"
_BASE_URL = "https://www.ebi.ac.uk/chembl/api/data"
_TIMEOUT = 30
_DELAY = 0.2
_RETRY_ATTEMPTS = 3
_RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}
_SUPPORTED_TYPES = {"kd", "ki", "ic50", "ec50", "dg", "Δg"}
_TO_NM = {
    "fm": 0.000001,
    "pm": 0.001,
    "nm": 1.0,
    "um": 1000.0,
    "mm": 1000000.0,
    "m": 1000000000.0,
}
_MUTATION_RE = re.compile(r"\b[A-Z]\d+[A-Z]\b")


def _normalized_params_key(params: dict[str, Any]) -> tuple[tuple[str, str], ...]:
    return tuple(sorted((str(key), str(value)) for key, value in params.items()))


def _get_json_uncached(resource: str, params: dict[str, Any]) -> dict[str, Any]:
    last_error: Exception | None = None
    for attempt in range(1, _RETRY_ATTEMPTS + 1):
        try:
            resp = requests.get(
                f"{_BASE_URL}/{resource}.json",
                params=params,
                timeout=_TIMEOUT,
            )
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
    raise RuntimeError("Unreachable ChEMBL request state")


def _normalize_type(value: str | None) -> str | None:
    if not value:
        return None
    normalized = value.strip()
    lowered = normalized.lower()
    if lowered not in _SUPPORTED_TYPES:
        return None
    if lowered == "Δg":
        return "dG"
    if lowered == "dg":
        return "dG"
    return normalized


def _standardize_value(value: Any, units: str | None) -> tuple[float | None, float | None]:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None, None

    if not units:
        return numeric, None
    factor = _TO_NM.get(units.strip().lower())
    if factor is None:
        return numeric, None
    standardized = round(numeric * factor, 6)
    return numeric, standardized


def _mutation_annotation(texts: list[str]) -> tuple[str | None, str | None]:
    haystack = " ".join(texts)
    found = sorted(set(_MUTATION_RE.findall(haystack)))
    if found:
        return ",".join(found), "mutant"
    lowered = haystack.lower()
    if "wild type" in lowered or "wild-type" in lowered or re.search(r"\bwt\b", lowered):
        return None, "wildtype"
    return None, None


def _activity_to_sample(
    activity: dict[str, Any],
    *,
    accession: str,
    inchikey: str,
    target_chembl_id: str,
    molecule_chembl_id: str,
) -> CanonicalBindingSample | None:
    assay_type = _normalize_type(str(activity.get("standard_type") or ""))
    if assay_type is None:
        return None

    assay_value, standardized = _standardize_value(
        activity.get("standard_value"),
        activity.get("standard_units"),
    )
    log10_value = None
    if assay_type != "dG":
        if standardized is not None and standardized > 0:
            log10_value = round(math.log10(standardized), 6)
        elif activity.get("pchembl_value") not in (None, ""):
            try:
                log10_value = round(9.0 - float(activity["pchembl_value"]), 6)
            except (TypeError, ValueError):
                pass

    assay_chembl_id = str(activity.get("activity_chembl_id") or "")
    texts = [
        str(activity.get("assay_description") or ""),
        str(activity.get("activity_comment") or ""),
        str(activity.get("target_pref_name") or ""),
    ]
    mutation_string, wt_or_mutant = _mutation_annotation(texts)
    provenance: dict[str, Any] = {
        "source_database": "ChEMBL",
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "adapter_version": _ADAPTER_VERSION,
        "target_chembl_id": target_chembl_id,
        "molecule_chembl_id": molecule_chembl_id,
        "assay_chembl_id": activity.get("assay_chembl_id"),
        "activity_chembl_id": assay_chembl_id or None,
        "document_chembl_id": activity.get("document_chembl_id"),
        "standard_relation": activity.get("standard_relation"),
        "assay_description": activity.get("assay_description"),
        "activity_comment": activity.get("activity_comment"),
        "target_name": activity.get("target_pref_name"),
        "inchikey": inchikey,
        "accession": accession,
    }
    if mutation_string is None and wt_or_mutant is None and assay_chembl_id:
        provenance["pair_grouping_override"] = (
            f"protein_ligand|-|-|{inchikey}|mutation_unknown:{assay_chembl_id}"
        )

    return CanonicalBindingSample(
        sample_id=f"CHEMBL_{assay_chembl_id or molecule_chembl_id}",
        task_type="protein_ligand",
        source_database="ChEMBL",
        source_record_id=assay_chembl_id or molecule_chembl_id,
        uniprot_ids=[accession],
        ligand_inchi_key=inchikey,
        ligand_id=molecule_chembl_id,
        assay_type=assay_type,
        assay_value=assay_value,
        assay_unit=activity.get("standard_units"),
        assay_value_standardized=standardized,
        assay_value_log10=log10_value,
        mutation_string=mutation_string,
        wildtype_or_mutant=wt_or_mutant,
        provenance=provenance,
        quality_flags=[],
        quality_score=0.0,
    )


class ChEMBLAdapter(BaseAdapter):
    """Adapter for exact-identifier ChEMBL activity lookup."""

    def __init__(self) -> None:
        self._response_cache: dict[tuple[str, tuple[tuple[str, str], ...]], dict[str, Any]] = {}

    @property
    def source_name(self) -> str:
        return "ChEMBL"

    def fetch_metadata(self, record_id: str) -> dict[str, Any]:
        raise NotImplementedError(
            "Use fetch_by_uniprot_and_inchikey() for exact ChEMBL lookups."
        )

    def normalize_record(self, raw: dict[str, Any]) -> CanonicalBindingSample:
        sample = _activity_to_sample(
            raw["activity"],
            accession=raw["accession"],
            inchikey=raw["inchikey"],
            target_chembl_id=raw["target_chembl_id"],
            molecule_chembl_id=raw["molecule_chembl_id"],
        )
        if sample is None:
            raise ValueError("Unsupported or unparseable ChEMBL activity record.")
        return sample

    def resolve_target_chembl_ids(self, accession: str) -> list[str]:
        body = self._get_json("target", {
            "target_components__accession__iexact": accession,
            "limit": 100,
        })
        return [
            str(entry.get("target_chembl_id"))
            for entry in body.get("targets") or []
            if entry.get("target_chembl_id")
        ]

    def resolve_molecule_chembl_ids(self, inchikey: str) -> list[str]:
        body = self._get_json("molecule", {
            "molecule_structures__standard_inchi_key__iexact": inchikey,
            "limit": 100,
        })
        return [
            str(entry.get("molecule_chembl_id"))
            for entry in body.get("molecules") or []
            if entry.get("molecule_chembl_id")
        ]

    def fetch_activities(
        self,
        *,
        target_chembl_id: str,
        molecule_chembl_id: str,
    ) -> list[dict[str, Any]]:
        body = self._get_json("activity", {
            "target_chembl_id": target_chembl_id,
            "molecule_chembl_id": molecule_chembl_id,
            "standard_type__in": "Kd,Ki,IC50,EC50,dG",
            "limit": 1000,
        })
        return list(body.get("activities") or [])

    def _get_json(self, resource: str, params: dict[str, Any]) -> dict[str, Any]:
        key = (resource, _normalized_params_key(params))
        if key not in self._response_cache:
            self._response_cache[key] = _get_json_uncached(resource, params)
        return self._response_cache[key]

    def fetch_by_uniprot_and_inchikey(
        self,
        accession: str,
        inchikey: str,
    ) -> list[CanonicalBindingSample]:
        results: list[CanonicalBindingSample] = []
        seen: set[str] = set()
        for target_chembl_id in self.resolve_target_chembl_ids(accession):
            for molecule_chembl_id in self.resolve_molecule_chembl_ids(inchikey):
                for activity in self.fetch_activities(
                    target_chembl_id=target_chembl_id,
                    molecule_chembl_id=molecule_chembl_id,
                ):
                    sample = _activity_to_sample(
                        activity,
                        accession=accession,
                        inchikey=inchikey,
                        target_chembl_id=target_chembl_id,
                        molecule_chembl_id=molecule_chembl_id,
                    )
                    if sample is None or sample.sample_id in seen:
                        continue
                    seen.add(sample.sample_id)
                    results.append(sample)
        return results
