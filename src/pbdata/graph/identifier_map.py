"""Identifier harmonization for the graph subsystem.

This implementation is intentionally conservative and UniProt-centered:
- exact mapping only
- supported identifier families: UniProt accession/ID, Ensembl, Entrez Gene
- no silent guessing when mappings are absent or ambiguous
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Iterable

import requests

_UNIPROT_IDMAP_URL = "https://idmapping.uniprot.org/cgi-bin/idmapping_http_client3"
_TIMEOUT = 30

_UNIPROT_ACC_RE = re.compile(r"^(?:[OPQ][0-9][A-Z0-9]{3}[0-9]|[A-NR-Z][0-9][A-Z0-9]{3}[0-9])(?:-\d+)?$")
_UNIPROT_ID_RE = re.compile(r"^[A-Z0-9]+_[A-Z0-9]+$")
_ENSEMBL_RE = re.compile(r"^ENS[A-Z]*[GTP]\d+(?:\.\d+)?$")
_ENTREZ_RE = re.compile(r"^\d+$")


@dataclass(frozen=True)
class IdentifierMappingPlan:
    """Cross-reference payload for one biological entity identifier."""

    primary_id: str
    detected_id_type: str
    uniprot_id: str | None = None
    entrez_id: str | None = None
    ensembl_id: str | None = None
    status: str = "unmapped"
    notes: str = ""


def detect_identifier_type(identifier: str) -> str:
    value = identifier.strip()
    if not value:
        return "UNKNOWN"
    if _UNIPROT_ACC_RE.fullmatch(value):
        return "ACC"
    if _UNIPROT_ID_RE.fullmatch(value):
        return "ID"
    if _ENSEMBL_RE.fullmatch(value):
        return "ENSEMBL_ID"
    if _ENTREZ_RE.fullmatch(value):
        return "P_ENTREZGENEID"
    return "UNKNOWN"


def _post_id_mapping(ids: Iterable[str], from_type: str, to_type: str) -> dict[str, list[str]]:
    response = requests.post(
        _UNIPROT_IDMAP_URL,
        data={
            "ids": ",".join(ids),
            "from": from_type,
            "to": to_type,
            "async": "NO",
        },
        timeout=_TIMEOUT,
    )
    response.raise_for_status()
    return _parse_idmapping_response(response.text)


def _parse_idmapping_response(text: str) -> dict[str, list[str]]:
    mapping: dict[str, list[str]] = {}
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped or "\t" not in stripped:
            continue
        source_id, target_id = stripped.split("\t", 1)
        source_id = source_id.strip()
        target_id = target_id.strip()
        if not source_id or not target_id:
            continue
        mapping.setdefault(source_id, [])
        if target_id not in mapping[source_id]:
            mapping[source_id].append(target_id)
    return mapping


def _single_or_none(values: list[str]) -> tuple[str | None, str]:
    if not values:
        return None, "unmapped"
    if len(values) > 1:
        return values[0], "ambiguous"
    return values[0], "exact_mapped"


def map_protein_identifier(
    identifier: str,
    *,
    resolve_remote: bool = True,
) -> IdentifierMappingPlan:
    """Map an identifier across UniProt / Entrez / Ensembl using exact rules."""
    value = identifier.strip()
    detected = detect_identifier_type(value)
    if detected == "UNKNOWN":
        return IdentifierMappingPlan(
            primary_id=value,
            detected_id_type=detected,
            status="unsupported",
            notes="Identifier type is not recognized by the current harmonizer.",
        )

    if not resolve_remote:
        return IdentifierMappingPlan(
            primary_id=value,
            detected_id_type=detected,
            uniprot_id=value if detected in {"ACC", "ID"} else None,
            status="stub",
            notes=(
                "Offline identifier plan only. Set resolve_remote=True to fetch exact "
                "UniProt-centered cross-references."
            ),
        )

    uniprot_id: str | None = None
    entrez_id: str | None = None
    ensembl_id: str | None = None
    statuses: list[str] = []

    if detected in {"ACC", "ID"}:
        uniprot_id = value
    else:
        uniprot_map = _post_id_mapping([value], detected, "ACC")
        uniprot_id, status = _single_or_none(uniprot_map.get(value, []))
        statuses.append(status)

    if uniprot_id:
        entrez_map = _post_id_mapping([uniprot_id], "ACC", "P_ENTREZGENEID")
        ensembl_map = _post_id_mapping([uniprot_id], "ACC", "ENSEMBL_ID")
        entrez_id, entrez_status = _single_or_none(entrez_map.get(uniprot_id, []))
        ensembl_id, ensembl_status = _single_or_none(ensembl_map.get(uniprot_id, []))
        statuses.extend([entrez_status, ensembl_status])

    final_status = "exact_mapped"
    if "ambiguous" in statuses:
        final_status = "ambiguous"
    elif not uniprot_id and all(status in {"unmapped"} for status in statuses):
        final_status = "unmapped"
    elif "unmapped" in statuses and final_status != "ambiguous":
        final_status = "partial"

    notes = (
        "UniProt-centered exact identifier mapping. "
        "Ambiguous means multiple target IDs were returned; partial means only some cross-references were found."
    )
    return IdentifierMappingPlan(
        primary_id=value,
        detected_id_type=detected,
        uniprot_id=uniprot_id,
        entrez_id=entrez_id,
        ensembl_id=ensembl_id,
        status=final_status,
        notes=notes,
    )


def batch_map_protein_identifiers(identifiers: Iterable[str]) -> list[IdentifierMappingPlan]:
    """Convenience wrapper for mapping multiple identifiers serially."""
    return [map_protein_identifier(identifier) for identifier in identifiers]
