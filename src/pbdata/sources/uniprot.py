"""UniProt annotation adapter.

This adapter is intentionally metadata-oriented rather than assay-oriented.
It provides conservative per-accession annotations that can enrich the
workflow-engine metadata table without claiming that UniProt is a direct
binding-measurement source.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import requests

from pbdata.storage import reuse_existing_file

_UNIPROT_ENTRY_URL = "https://rest.uniprot.org/uniprotkb/{accession}.json"
_TIMEOUT = 60


@dataclass(frozen=True)
class UniProtAnnotationRecord:
    accession: str
    reviewed: bool
    protein_name: str
    gene_names: list[str]
    organism_name: str
    taxonomy_id: int | None
    sequence: str
    sequence_length: int | None
    pdb_ids: list[str]
    interpro_ids: list[str]
    pfam_ids: list[str]
    go_terms: list[str]
    keywords: list[str]
    status: str = "ready"


def _validate_uniprot_json(path: Path, *, expected_accession: str | None = None) -> bool:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return False
    if not isinstance(raw, dict):
        return False
    accession = str(raw.get("primaryAccession") or "").strip().upper()
    if expected_accession and accession != expected_accession.strip().upper():
        return False
    return bool(accession)


class UniProtAdapter:
    """Fetch and normalize UniProtKB entry metadata."""

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

    def fetch_metadata(self, accession: str) -> dict[str, Any]:
        accession = accession.strip().upper()
        if not accession:
            raise ValueError("UniProt accession must be non-empty.")

        cache_path = (self.cache_dir / f"{accession}.json") if self.cache_dir is not None else None
        if cache_path is not None and reuse_existing_file(
            cache_path,
            validator=lambda path, expected=accession: _validate_uniprot_json(path, expected_accession=expected),
        ):
            return json.loads(cache_path.read_text(encoding="utf-8"))

        response = requests.get(
            _UNIPROT_ENTRY_URL.format(accession=accession),
            headers={"Accept": "application/json"},
            timeout=self.timeout,
        )
        if response.status_code == 404:
            raise requests.HTTPError(f"UniProt accession not found: {accession}", response=response)
        response.raise_for_status()
        raw = response.json()
        if cache_path is not None:
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            cache_path.write_text(json.dumps(raw, indent=2), encoding="utf-8")
        return raw if isinstance(raw, dict) else {}

    def normalize_record(self, raw: dict[str, Any]) -> UniProtAnnotationRecord:
        accession = str(raw.get("primaryAccession") or "").strip().upper()
        protein_desc = raw.get("proteinDescription") or {}
        rec_name = ((protein_desc.get("recommendedName") or {}).get("fullName") or {}).get("value")
        alt_names = protein_desc.get("submissionNames") or []
        protein_name = str(rec_name or "").strip()
        if not protein_name and alt_names:
            protein_name = str((((alt_names[0] or {}).get("fullName") or {}).get("value")) or "").strip()

        genes = raw.get("genes") or []
        gene_names: list[str] = []
        for gene in genes:
            if not isinstance(gene, dict):
                continue
            primary = ((gene.get("geneName") or {}).get("value")) if isinstance(gene.get("geneName"), dict) else None
            if primary:
                gene_names.append(str(primary))
            for synonym in gene.get("synonyms") or []:
                if isinstance(synonym, dict) and synonym.get("value"):
                    gene_names.append(str(synonym["value"]))

        organism = raw.get("organism") or {}
        organism_name = str(organism.get("scientificName") or "").strip()
        taxonomy_id = organism.get("taxonId")
        try:
            taxonomy_id = int(taxonomy_id) if taxonomy_id is not None else None
        except (TypeError, ValueError):
            taxonomy_id = None

        sequence_payload = raw.get("sequence") or {}
        sequence = str(sequence_payload.get("value") or "").strip()
        sequence_length = sequence_payload.get("length")
        try:
            sequence_length = int(sequence_length) if sequence_length is not None else (len(sequence) if sequence else None)
        except (TypeError, ValueError):
            sequence_length = len(sequence) if sequence else None

        pdb_ids: list[str] = []
        interpro_ids: list[str] = []
        pfam_ids: list[str] = []
        go_terms: list[str] = []
        for ref in raw.get("uniProtKBCrossReferences") or []:
            if not isinstance(ref, dict):
                continue
            database = str(ref.get("database") or "").strip()
            ref_id = str(ref.get("id") or "").strip()
            if not ref_id:
                continue
            if database == "PDB":
                pdb_ids.append(ref_id)
            elif database == "InterPro":
                interpro_ids.append(ref_id)
            elif database == "Pfam":
                pfam_ids.append(ref_id)
            elif database == "GO":
                go_terms.append(ref_id)

        keywords = [
            str(keyword.get("name") or "").strip()
            for keyword in (raw.get("keywords") or [])
            if isinstance(keyword, dict) and keyword.get("name")
        ]

        return UniProtAnnotationRecord(
            accession=accession,
            reviewed=str(raw.get("entryType") or "").startswith("UniProtKB reviewed"),
            protein_name=protein_name,
            gene_names=sorted({name for name in gene_names if name}),
            organism_name=organism_name,
            taxonomy_id=taxonomy_id,
            sequence=sequence,
            sequence_length=sequence_length,
            pdb_ids=sorted(set(pdb_ids)),
            interpro_ids=sorted(set(interpro_ids)),
            pfam_ids=sorted(set(pfam_ids)),
            go_terms=sorted(set(go_terms)),
            keywords=sorted(set(keyword for keyword in keywords if keyword)),
        )

    def fetch_annotation(self, accession: str) -> UniProtAnnotationRecord:
        local_record = self._fetch_local_index_record(accession)
        if local_record is not None:
            return local_record
        return self.normalize_record(self.fetch_metadata(accession))

    def _fetch_local_index_record(self, accession: str) -> UniProtAnnotationRecord | None:
        if self.storage_root is None:
            return None
        try:
            from pbdata.source_indexes import query_uniprot_swissprot_index
            from pbdata.storage import build_storage_layout

            payload = query_uniprot_swissprot_index(
                build_storage_layout(self.storage_root),
                accession,
            )
        except Exception:
            return None
        if not payload:
            return None
        return UniProtAnnotationRecord(
            accession=str(payload.get("accession") or "").strip().upper(),
            reviewed=bool(payload.get("reviewed")),
            protein_name=str(payload.get("protein_name") or "").strip(),
            gene_names=[str(value) for value in payload.get("gene_names") or [] if str(value).strip()],
            organism_name=str(payload.get("organism_name") or "").strip(),
            taxonomy_id=_safe_int(payload.get("taxonomy_id")),
            sequence="",
            sequence_length=_safe_int(payload.get("sequence_length")),
            pdb_ids=[str(value) for value in payload.get("pdb_ids") or [] if str(value).strip()],
            interpro_ids=[str(value) for value in payload.get("interpro_ids") or [] if str(value).strip()],
            pfam_ids=[str(value) for value in payload.get("pfam_ids") or [] if str(value).strip()],
            go_terms=[str(value) for value in payload.get("go_terms") or [] if str(value).strip()],
            keywords=[str(value) for value in payload.get("keywords") or [] if str(value).strip()],
        )


def _safe_int(value: Any) -> int | None:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None
