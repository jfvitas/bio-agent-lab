"""SCOP fold annotation adapter via PDBe/SIFTS mappings."""

from __future__ import annotations

from pathlib import Path

from pbdata.source_indexes import query_scop_domain_index
from pbdata.storage import build_storage_layout
from pbdata.sources.pdbe_mappings import PDBeMappingAdapter
from pbdata.sources.pdbe_mappings import StructureDomainAnnotationRecord


class SCOPAdapter(PDBeMappingAdapter):
    def __init__(
        self,
        *,
        cache_dir: Path | None = None,
        storage_root: Path | None = None,
        timeout: int = 60,
    ) -> None:
        super().__init__(
            mapping_type="scop",
            source_name="SCOP",
            cache_dir=cache_dir,
            timeout=timeout,
        )
        self.storage_root = Path(storage_root) if storage_root is not None else None

    def fetch_annotation(self, pdb_id: str) -> StructureDomainAnnotationRecord:
        local_record = self._fetch_local_index_record(pdb_id)
        if local_record is not None:
            return local_record
        return super().fetch_annotation(pdb_id)

    def _fetch_local_index_record(self, pdb_id: str) -> StructureDomainAnnotationRecord | None:
        if self.storage_root is None:
            return None
        payload = query_scop_domain_index(build_storage_layout(self.storage_root), pdb_id)
        if payload is None:
            return None
        return StructureDomainAnnotationRecord(
            source_name="SCOP",
            pdb_id=str(payload.get("pdb_id") or pdb_id).strip().upper(),
            domain_ids=[str(value) for value in payload.get("domain_ids") or [] if str(value).strip()],
            domain_names=[str(value) for value in payload.get("domain_names") or [] if str(value).strip()],
            chain_ids=[str(value) for value in payload.get("chain_ids") or [] if str(value).strip()],
            chain_to_domain_ids={
                str(chain_id): [str(value) for value in values or [] if str(value).strip()]
                for chain_id, values in dict(payload.get("chain_to_domain_ids") or {}).items()
                if str(chain_id).strip()
            },
            mapping_count=int(payload.get("mapping_count") or 0),
        )
