"""RCSB source adapter — full normalization from GraphQL entity data.

Responsibilities:
- fetch raw entry metadata (delegates to rcsb_search)
- normalize raw records into CanonicalBindingSample
- preserve provenance on every record
- never write directly to processed datasets
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from pbdata.schemas.canonical_sample import CanonicalBindingSample
from pbdata.sources.base import BaseAdapter

_ADAPTER_VERSION = "0.1.0"

# Polymer subtypes that indicate a protein chain
_PROTEIN_POLY_TYPES = {"polypeptide(l)", "polypeptide(d)"}

# Non-polymer comp_ids to exclude from ligand classification
# (common solvents, buffer components, ions, crystallisation agents)
_EXCLUDED_COMPS: frozenset[str] = frozenset({
    "HOH", "DOD",                                   # water
    "SO4", "PO4", "CL", "NA", "MG", "ZN", "CA",    # common ions/salts
    "K", "MN", "FE", "NI", "CU", "CD", "CO", "BR",
    "IOD", "F", "CS", "RB", "SR", "BA", "AU", "HG", "PT", "PB",
    "GOL", "EDO", "PEG", "MPD", "PG4", "PGE",      # cryo-protectants
    "FMT", "ACT", "ACE", "ACY", "ETH", "DMS",      # solvents/acetate
    "MES", "TRS", "HEP", "BME", "EPE", "MLI",      # buffer components
    "SUC", "TAR", "AZI", "IMD", "NH2",              # other common additives
})


# ---------------------------------------------------------------------------
# Entity-level helpers
# ---------------------------------------------------------------------------

def _is_protein(entity: dict[str, Any]) -> bool:
    poly = entity.get("entity_poly") or {}
    return poly.get("type", "").lower() in _PROTEIN_POLY_TYPES


def _is_ligand(entity: dict[str, Any]) -> bool:
    comp = (entity.get("nonpolymer_comp") or {}).get("chem_comp") or {}
    cid = comp.get("id", "")
    return bool(cid) and cid not in _EXCLUDED_COMPS


def _chain_ids(entity: dict[str, Any] | None) -> list[str] | None:
    if not entity:
        return None
    ids = (
        (entity.get("rcsb_polymer_entity_container_identifiers") or {})
        .get("auth_asym_ids")
    )
    return ids if ids else None


def _sequence(entity: dict[str, Any] | None) -> str | None:
    if not entity:
        return None
    poly = entity.get("entity_poly") or {}
    seq: str | None = poly.get("pdbx_seq_one_letter_code_can")
    return seq.replace("\n", "").strip() if seq else None


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
        """Fetch entry metadata for a single PDB ID.

        Prefer search_and_download() for bulk ingestion.
        """
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
            raw:              Entry dict as stored in data/raw/rcsb/{PDB_ID}.json.
            chem_descriptors: Optional mapping from comp_id → {descriptor_type: value}
                              as returned by fetch_chemcomp_descriptors().  When
                              provided, ligand_smiles and ligand_inchi_key are
                              populated from 'SMILES_CANONICAL' / 'InChIKey'.

        Returns:
            Validated canonical record.

        TODO: assembly_id — use rcsb_assembly_info once assembly endpoint added
        """
        pdb_id: str = (raw.get("rcsb_id") or "").strip().upper()

        entry_info: dict[str, Any] = raw.get("rcsb_entry_info") or {}

        # Classify entities
        poly_entities:    list[dict] = raw.get("polymer_entities") or []
        nonpoly_entities: list[dict] = raw.get("nonpolymer_entities") or []

        protein_entities = [e for e in poly_entities if _is_protein(e)]
        ligand_entities  = [e for e in nonpoly_entities if _is_ligand(e)]

        # Heuristic task type
        if ligand_entities and protein_entities:
            task_type = "protein_ligand"
        elif len(protein_entities) >= 2:
            task_type = "protein_protein"
        else:
            task_type = "protein_ligand"

        # Receptor = first protein entity
        receptor = protein_entities[0] if protein_entities else None
        # Partner = second protein entity (protein_protein only)
        partner = (
            protein_entities[1]
            if task_type == "protein_protein" and len(protein_entities) >= 2
            else None
        )

        # Ligand comp_id + optional SMILES / InChIKey enrichment
        ligand_id: str | None = None
        ligand_smiles: str | None = None
        ligand_inchi_key: str | None = None
        if ligand_entities:
            chem = (ligand_entities[0].get("nonpolymer_comp") or {}).get("chem_comp") or {}
            ligand_id = chem.get("id") or None
            if ligand_id and chem_descriptors:
                descs = chem_descriptors.get(ligand_id, {})
                ligand_smiles    = descs.get("SMILES_CANONICAL") or descs.get("SMILES")
                ligand_inchi_key = descs.get("InChIKey")

        # Experimental method
        exptl: list[dict] = raw.get("exptl") or []
        method: str | None = exptl[0].get("method") if exptl else None

        provenance: dict[str, Any] = {
            "source_database": "RCSB",
            "ingested_at": datetime.now(timezone.utc).isoformat(),
            "adapter_version": _ADAPTER_VERSION,
        }

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
            sequence_receptor=_sequence(receptor),
            sequence_partner=_sequence(partner),
            uniprot_ids=_uniprot_ids(protein_entities),
            taxonomy_ids=_taxonomy_ids(protein_entities),
            ligand_id=ligand_id,
            ligand_smiles=ligand_smiles,
            ligand_inchi_key=ligand_inchi_key,
            provenance=provenance,
            quality_flags=[],
            quality_score=0.0,
        )
