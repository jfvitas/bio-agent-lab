"""CATH fold annotation adapter via PDBe/SIFTS mappings."""

from __future__ import annotations

from pathlib import Path

from pbdata.sources.pdbe_mappings import PDBeMappingAdapter


class CATHAdapter(PDBeMappingAdapter):
    def __init__(self, *, cache_dir: Path | None = None, timeout: int = 60) -> None:
        super().__init__(
            mapping_type="cath",
            source_name="CATH",
            cache_dir=cache_dir,
            timeout=timeout,
        )
