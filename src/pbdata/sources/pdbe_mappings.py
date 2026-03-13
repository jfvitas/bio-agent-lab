"""PDBe/SIFTS-backed structure-domain mapping helpers.

These adapters are metadata-oriented. They enrich harvested workspace metadata
with chain-aware domain and fold classifications keyed by PDB entry rather than
acting as direct assay or structure-ingest sources.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import requests

from pbdata.storage import reuse_existing_file

_TIMEOUT = 60
_PDBe_MAPPING_URL = "https://www.ebi.ac.uk/pdbe/api/mappings/{mapping_type}/{pdb_id}"


@dataclass(frozen=True)
class StructureDomainAnnotationRecord:
    source_name: str
    pdb_id: str
    domain_ids: list[str]
    domain_names: list[str]
    chain_ids: list[str]
    chain_to_domain_ids: dict[str, list[str]]
    mapping_count: int
    status: str = "ready"


def _validate_mapping_json(path: Path, *, expected_pdb_id: str | None = None) -> bool:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return False
    if not isinstance(raw, dict):
        return False
    if expected_pdb_id is None:
        return bool(raw)
    expected = expected_pdb_id.strip().lower()
    return expected in {str(key).strip().lower() for key in raw}


def _chain_id_from_mapping(mapping: dict[str, Any]) -> str:
    for key in ("chain_id", "struct_asym_id", "asym_id", "chain_label", "auth_asym_id"):
        value = str(mapping.get(key) or "").strip()
        if value:
            return value
    return ""


def _extract_mapping_block(raw: dict[str, Any], pdb_id: str, source_name: str) -> dict[str, Any]:
    root = raw.get(pdb_id.lower()) or raw.get(pdb_id.upper()) or raw.get(pdb_id) or {}
    if not isinstance(root, dict):
        return {}
    for key, value in root.items():
        if str(key).strip().lower() == source_name.lower() and isinstance(value, dict):
            return value
    return root


class PDBeMappingAdapter:
    """Fetch and normalize PDBe mapping blocks for a specific mapping type."""

    def __init__(
        self,
        *,
        mapping_type: str,
        source_name: str,
        cache_dir: Path | None = None,
        timeout: int = _TIMEOUT,
    ) -> None:
        self.mapping_type = mapping_type
        self.source_name = source_name
        self.cache_dir = cache_dir
        self.timeout = timeout

    def fetch_metadata(self, pdb_id: str) -> dict[str, Any]:
        normalized_pdb_id = pdb_id.strip().lower()
        if not normalized_pdb_id:
            raise ValueError(f"{self.source_name} mapping lookup requires a non-empty PDB ID.")

        cache_path = (
            self.cache_dir / f"{normalized_pdb_id}.json"
            if self.cache_dir is not None
            else None
        )
        if cache_path is not None and reuse_existing_file(
            cache_path,
            validator=lambda path, expected=normalized_pdb_id: _validate_mapping_json(path, expected_pdb_id=expected),
        ):
            return json.loads(cache_path.read_text(encoding="utf-8"))

        response = requests.get(
            _PDBe_MAPPING_URL.format(mapping_type=self.mapping_type, pdb_id=normalized_pdb_id),
            headers={"Accept": "application/json"},
            timeout=self.timeout,
        )
        if response.status_code == 404:
            raise requests.HTTPError(f"{self.source_name} mapping not found for PDB ID: {normalized_pdb_id}", response=response)
        response.raise_for_status()
        raw = response.json()
        if cache_path is not None:
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            cache_path.write_text(json.dumps(raw, indent=2), encoding="utf-8")
        return raw if isinstance(raw, dict) else {}

    def normalize_record(self, raw: dict[str, Any], *, pdb_id: str) -> StructureDomainAnnotationRecord:
        block = _extract_mapping_block(raw, pdb_id, self.source_name)
        domain_ids: list[str] = []
        domain_names: list[str] = []
        chain_ids: list[str] = []
        chain_to_domain_ids: dict[str, list[str]] = {}
        mapping_count = 0

        for domain_id, payload in block.items():
            if not isinstance(payload, dict):
                continue
            domain_id_text = str(domain_id or "").strip()
            if not domain_id_text:
                continue
            domain_ids.append(domain_id_text)
            name = str(payload.get("name") or payload.get("description") or "").strip()
            if name:
                domain_names.append(name)
            mappings = payload.get("mappings") or []
            if not isinstance(mappings, list):
                mappings = []
            for mapping in mappings:
                if not isinstance(mapping, dict):
                    continue
                mapping_count += 1
                chain_id = _chain_id_from_mapping(mapping)
                if chain_id:
                    chain_ids.append(chain_id)
                    chain_to_domain_ids.setdefault(chain_id, [])
                    if domain_id_text not in chain_to_domain_ids[chain_id]:
                        chain_to_domain_ids[chain_id].append(domain_id_text)

        return StructureDomainAnnotationRecord(
            source_name=self.source_name,
            pdb_id=pdb_id.strip().upper(),
            domain_ids=sorted(set(domain_ids)),
            domain_names=sorted(set(name for name in domain_names if name)),
            chain_ids=sorted(set(chain_ids)),
            chain_to_domain_ids={
                chain_id: sorted(set(ids))
                for chain_id, ids in chain_to_domain_ids.items()
            },
            mapping_count=mapping_count,
        )

    def fetch_annotation(self, pdb_id: str) -> StructureDomainAnnotationRecord:
        normalized_pdb_id = pdb_id.strip().upper()
        return self.normalize_record(self.fetch_metadata(normalized_pdb_id), pdb_id=normalized_pdb_id)
