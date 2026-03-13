"""Reactome pathway annotation adapter."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import requests

from pbdata.storage import reuse_existing_file

_REACTOME_CONTENT_SERVICE = "https://reactome.org/ContentService"
_TIMEOUT = 60


@dataclass(frozen=True)
class ReactomePathwayRecord:
    uniprot_id: str
    pathway_ids: list[str]
    pathway_names: list[str]
    pathway_count: int
    status: str = "ready"


def _validate_reactome_json(path: Path) -> bool:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return False
    return isinstance(raw, list)


class ReactomeAdapter:
    """Fetch per-protein pathway membership from Reactome ContentService."""

    def __init__(
        self,
        *,
        species: str = "Homo sapiens",
        cache_dir: Path | None = None,
        timeout: int = _TIMEOUT,
    ) -> None:
        self.species = species
        self.cache_dir = cache_dir
        self.timeout = timeout

    def fetch_metadata(self, uniprot_id: str) -> list[dict[str, Any]]:
        uniprot_id = uniprot_id.strip().upper()
        if not uniprot_id:
            raise ValueError("Reactome UniProt ID must be non-empty.")

        cache_path = (self.cache_dir / f"{uniprot_id}.json") if self.cache_dir is not None else None
        if cache_path is not None and reuse_existing_file(cache_path, validator=_validate_reactome_json):
            raw = json.loads(cache_path.read_text(encoding="utf-8"))
            return raw if isinstance(raw, list) else []

        response = requests.get(
            f"{_REACTOME_CONTENT_SERVICE}/data/pathways/low/entity/{uniprot_id}",
            params={"species": self.species},
            headers={"Accept": "application/json"},
            timeout=self.timeout,
        )
        if response.status_code == 404:
            return []
        response.raise_for_status()
        raw = response.json()
        if cache_path is not None:
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            cache_path.write_text(json.dumps(raw, indent=2), encoding="utf-8")
        return raw if isinstance(raw, list) else []

    def normalize_record(self, uniprot_id: str, raw: list[dict[str, Any]]) -> ReactomePathwayRecord:
        pathway_pairs = []
        for item in raw:
            if not isinstance(item, dict):
                continue
            pathway_id = str(item.get("stId") or "").strip()
            pathway_name = str(item.get("displayName") or "").strip()
            if pathway_id:
                pathway_pairs.append((pathway_id, pathway_name))
        pathway_pairs = sorted(set(pathway_pairs))
        return ReactomePathwayRecord(
            uniprot_id=uniprot_id.strip().upper(),
            pathway_ids=[pathway_id for pathway_id, _ in pathway_pairs],
            pathway_names=[pathway_name for _, pathway_name in pathway_pairs if pathway_name],
            pathway_count=len(pathway_pairs),
        )

    def fetch_annotation(self, uniprot_id: str) -> ReactomePathwayRecord:
        raw = self.fetch_metadata(uniprot_id)
        return self.normalize_record(uniprot_id, raw)
