"""RCSB source adapter — full normalization from GraphQL entity data.

Responsibilities:
- fetch raw entry metadata (delegates to rcsb_search)
- normalize raw records into CanonicalBindingSample
- preserve provenance on every record
- never write directly to processed datasets

Structural classification is delegated to rcsb_classify, which keeps
entity-level logic separated from the adapter plumbing.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from pbdata.schemas.canonical_sample import CanonicalBindingSample
from pbdata.sources.base import BaseAdapter
from pbdata.sources.rcsb_classify import (
    _polymer_chain_ids,
    _sequence,
    classify_entry,
)

_ADAPTER_VERSION = "0.2.0"

# Re-export for backward compat (callers may import _EXCLUDED_COMPS from here)
from pbdata.sources.rcsb_classify import _EXCLUDED_COMPS  # noqa: F401


# ---------------------------------------------------------------------------
# Legacy private helpers (kept for backward compat / tests)
# ---------------------------------------------------------------------------

def _is_protein(entity: dict[str, Any]) -> bool:
    poly = entity.get("entity_poly") or {}
    return poly.get("type", "").lower() in {"polypeptide(l)", "polypeptide(d)"}


def _is_ligand(entity: dict[str, Any]) -> bool:
    comp = (entity.get("nonpolymer_comp") or {}).get("chem_comp") or {}
    cid = comp.get("id", "")
    return bool(cid) and cid not in _EXCLUDED_COMPS


def _chain_ids(entity: dict[str, Any] | None) -> list[str] | None:
    if not entity:
        return None
    return _polymer_chain_ids(entity)


def _uniprot_ids(protein_entities: list[dict[str, Any]]) -> list[str] | None:
    seen: dict[str, None] = {}
    for e in protein_entities:
        uids = (
            (e.get("rcsb_polymer_entity_container_identifiers") or {})
            .get("uniprot_ids") or []
        )
        seen.update(dict.fromkeys(uids))
    return list(seen) or None


def _taxonomy_ids(protein_entities: list[dict[str, Any]]) -> list[int] | None:
    seen: dict[int, None] = {}
    for e in protein_entities:
        for org in (e.get("rcsb_entity_source_organism") or []):
            tid = org.get("ncbi_taxonomy_id")
            if tid is not None:
                try:
                    seen[int(tid)] = None
                except (TypeError, ValueError):
                    pass
    return list(seen) or None


def _resolution(entry_info: dict[str, Any]) -> float | None:
    rc = entry_info.get("resolution_combined")
    if not rc:
        return None
    try:
        val = float(rc[0]) if isinstance(rc, list) else float(rc)
        return val if val > 0 else None
    except (TypeError, ValueError, IndexError):
        return None


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------

class RCSBAdapter(BaseAdapter):
    """Adapter for RCSB PDB metadata ingestion."""

    @property
    def source_name(self) -> str:
        return "RCSB"

    def fetch_metadata(self, record_id: str) -> dict[str, Any]:
        """Fetch entry metadata for a single PDB ID."""
        from pbdata.sources.rcsb_search import fetch_entries_batch
        results = fetch_entries_batch([record_id.upper()])
        if not results:
            raise ValueError(f"No RCSB entry found for {record_id!r}")
        return results[0]

    def normalize_record(
        self,
        raw: dict[str, Any],
        chem_descriptors: dict[str, dict[str, str]] | None = None,
    ) -> CanonicalBindingSample:
        """Map a raw RCSB GraphQL entry dict to a CanonicalBindingSample.

        Args:
            raw:              Entry dict from data/raw/rcsb/{PDB_ID}.json.
            chem_descriptors: Optional {comp_id → {descriptor_type: value}}
                              from fetch_chemcomp_descriptors().  Populates
                              ligand_smiles and ligand_inchi_key when present.

        All entity classification is delegated to rcsb_classify.classify_entry().
        Backward-compatible single-ligand / receptor fields are still populated
        so that downstream consumers do not need to switch to bound_objects.
        """
        pdb_id: str = (raw.get("rcsb_id") or "").strip().upper()
        entry_info: dict[str, Any] = raw.get("rcsb_entry_info") or {}

        # ------------------------------------------------------------------
        # Run full classification pipeline
        # ------------------------------------------------------------------
        classified = classify_entry(raw, chem_descriptors=chem_descriptors)

        protein_entities  = classified["protein_entities"]
        peptide_entities  = classified["peptide_entities"]
        bound_objects     = classified["bound_objects"]
        interfaces        = classified["interfaces"]
        assembly_info     = classified["assembly_info"]
        is_homo           = classified["is_homo_oligomeric"]
        oligo_state       = classified["oligomeric_state"]
        task_type: str    = classified["task_type"]
        membrane_context: bool = classified["membrane_context"]

        # ------------------------------------------------------------------
        # Backward-compatible single-receptor / single-ligand fields
        # ------------------------------------------------------------------

        # Receptor = first protein entity (by GraphQL ordering)
        receptor = protein_entities[0] if protein_entities else None

        # Partner = second protein entity (protein_protein task only)
        partner = (
            protein_entities[1]
            if task_type == "protein_protein" and len(protein_entities) >= 2
            else None
        )

        # Primary ligand: first small_molecule BoundObject (if any)
        ligand_obj = next(
            (b for b in bound_objects if b.binder_type == "small_molecule"),
            None,
        )
        ligand_id    = ligand_obj.comp_id    if ligand_obj else None
        ligand_smiles = ligand_obj.smiles    if ligand_obj else None
        ligand_inchi  = ligand_obj.inchi_key if ligand_obj else None

        # ------------------------------------------------------------------
        # Experimental metadata
        # ------------------------------------------------------------------
        exptl: list[dict] = raw.get("exptl") or []
        method: str | None = exptl[0].get("method") if exptl else None

        # ------------------------------------------------------------------
        # Provenance
        # ------------------------------------------------------------------
        provenance: dict[str, Any] = {
            "source_database":        "RCSB",
            "ingested_at":            datetime.now(timezone.utc).isoformat(),
            "adapter_version":        _ADAPTER_VERSION,
            "oligomeric_state":       oligo_state,
            "bound_object_count":     len(bound_objects),
            "membrane_protein_context": membrane_context,
        }

        # ------------------------------------------------------------------
        # Struct title (for provenance / manifest)
        # ------------------------------------------------------------------
        struct_title = (raw.get("struct") or {}).get("title")
        if struct_title:
            provenance["struct_title"] = struct_title

        # ------------------------------------------------------------------
        # Build the canonical record
        # ------------------------------------------------------------------
        return CanonicalBindingSample(
            sample_id=f"RCSB_{pdb_id}",
            task_type=task_type,
            source_database="RCSB",
            source_record_id=pdb_id,
            pdb_id=pdb_id,
            experimental_method=method,
            structure_resolution=_resolution(entry_info),
            chain_ids_receptor=_chain_ids(receptor),
            chain_ids_partner=_chain_ids(partner),
            sequence_receptor=_sequence(receptor) if receptor else None,
            sequence_partner=_sequence(partner)   if partner  else None,
            uniprot_ids=_uniprot_ids(protein_entities),
            taxonomy_ids=_taxonomy_ids(protein_entities),
            # Primary ligand (backward compat)
            ligand_id=ligand_id,
            ligand_smiles=ligand_smiles,
            ligand_inchi_key=ligand_inchi,
            # Extended structural fields
            bound_objects=[b.model_dump() for b in bound_objects] or None,
            interfaces=[i.model_dump() for i in interfaces]       or None,
            assembly_info=assembly_info.model_dump()               if assembly_info else None,
            oligomeric_state=oligo_state,
            is_homo_oligomeric=is_homo,
            polymer_entity_count=len(protein_entities) + len(peptide_entities),
            provenance=provenance,
            quality_flags=[],
            quality_score=0.0,
        )
