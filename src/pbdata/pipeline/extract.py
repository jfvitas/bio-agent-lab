"""Multi-table extraction pipeline per STRUCTURE_EXTRACTION_AGENT_SPEC.md.

Converts raw RCSB GraphQL metadata + mmCIF parsing into the six-table
record structure:
  1. EntryRecord
  2. ChainRecord
  3. BoundObjectRecord
  4. InterfaceRecord
  5. AssayRecord
  6. ProvenanceRecord

This module is source-agnostic in schema but currently implements the
RCSB extraction path.  Other sources (BindingDB, SKEMPI, PDBbind) feed
into AssayRecord via merge logic.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pbdata.parsing.mmcif_supplement import (
    download_structure_files,
    fetch_mmcif_supplement,
    parse_structure_quality,
)
from pbdata.pipeline.assay_merge import merge_assay_samples
from pbdata.schemas.canonical_sample import CanonicalBindingSample
from pbdata.schemas.records import (
    AssayRecord,
    BoundObjectRecord,
    ChainRecord,
    EntryRecord,
    InterfaceRecord,
    ProvenanceRecord,
)
from pbdata.sources.rcsb_classify import (
    _EXCLUDED_COMPS,
    _polymer_chain_ids,
    _sequence,
    classify_entry,
)

logger = logging.getLogger(__name__)

_RCSB_ENTRY_URL = "https://www.rcsb.org/structure/{pdb_id}"
_ADAPTER_VERSION = "0.3.0"


def _augment_interfaces_from_biolip(
    pdb_id: str,
    assay_samples: list[CanonicalBindingSample],
) -> list[InterfaceRecord]:
    records: list[InterfaceRecord] = []
    for sample in assay_samples:
        if sample.source_database != "BioLiP" or sample.task_type != "protein_ligand":
            continue
        provenance = sample.provenance or {}
        residue_ids = provenance.get("binding_site_residue_ids") or []
        residue_names = provenance.get("binding_site_residue_names") or []
        if not residue_ids:
            continue
        records.append(InterfaceRecord(
            pdb_id=pdb_id,
            interface_type="protein_ligand",
            binding_site_chain_ids=sample.chain_ids_receptor,
            binding_site_residue_ids=residue_ids,
            binding_site_residue_names=residue_names or None,
            entity_name_b=sample.ligand_id,
        ))
    return records


# ── Resolution binning ───────────────────────────────────────────────

def _resolution_bin(res: float | None) -> str | None:
    if res is None:
        return None
    if res <= 1.5:
        return "high_res_<=1.5"
    if res <= 2.5:
        return "medium_res_1.5-2.5"
    if res <= 3.5:
        return "low_res_2.5-3.5"
    return "very_low_res_>3.5"


# ── Helpers ──────────────────────────────────────────────────────────

def _safe_float(val: Any) -> float | None:
    if val is None:
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _resolution(entry_info: dict[str, Any]) -> float | None:
    rc = entry_info.get("resolution_combined")
    if not rc:
        return None
    try:
        val = float(rc[0]) if isinstance(rc, list) else float(rc)
        return val if val > 0 else None
    except (TypeError, ValueError, IndexError):
        return None


def _uniprot_ids(protein_entities: list[dict[str, Any]]) -> list[str]:
    seen: dict[str, None] = {}
    for e in protein_entities:
        uids = (
            (e.get("rcsb_polymer_entity_container_identifiers") or {})
            .get("uniprot_ids") or []
        )
        seen.update(dict.fromkeys(uids))
    return list(seen)


def _taxonomy_ids(protein_entities: list[dict[str, Any]]) -> list[int]:
    seen: dict[int, None] = {}
    for e in protein_entities:
        for org in (e.get("rcsb_entity_source_organism") or []):
            tid = org.get("ncbi_taxonomy_id")
            if tid is not None:
                try:
                    seen[int(tid)] = None
                except (TypeError, ValueError):
                    pass
    return list(seen)


def _organism_names(protein_entities: list[dict[str, Any]]) -> list[str]:
    seen: dict[str, None] = {}
    for e in protein_entities:
        for org in (e.get("rcsb_entity_source_organism") or []):
            name = org.get("ncbi_scientific_name")
            if name:
                seen[str(name)] = None
    return list(seen)


# ── Main extraction function ─────────────────────────────────────────


def extract_rcsb_entry(
    raw: dict[str, Any],
    chem_descriptors: dict[str, dict[str, str]] | None = None,
    assay_samples: list[CanonicalBindingSample] | None = None,
    structures_dir: Path | None = None,
    download_structures: bool = True,
    download_pdb: bool = False,
    structure_mirror: str = "rcsb",
) -> dict[str, Any]:
    """Extract multi-table records from a single raw RCSB entry.

    Returns a dict with keys:
      entry: EntryRecord
      chains: list[ChainRecord]
      bound_objects: list[BoundObjectRecord]
      interfaces: list[InterfaceRecord]
      assays: list[AssayRecord]  (empty from RCSB; filled by merge)
      provenance: list[ProvenanceRecord]
    """
    pdb_id: str = (raw.get("rcsb_id") or "").strip().upper()
    entry_info: dict[str, Any] = raw.get("rcsb_entry_info") or {}
    accession: dict[str, Any] = raw.get("rcsb_accession_info") or {}
    struct: dict[str, Any] = raw.get("struct") or {}
    struct_kw: dict[str, Any] = raw.get("struct_keywords") or {}

    # ── Run classification pipeline ──────────────────────────────────
    classified = classify_entry(raw, chem_descriptors=chem_descriptors)

    protein_entities = classified["protein_entities"]
    peptide_entities = classified["peptide_entities"]
    other_poly = classified.get("other_poly", [])
    bound_objects_typed = classified["bound_objects"]
    interfaces_typed = classified["interfaces"]
    assembly_info = classified["assembly_info"]
    is_homo = classified["is_homo_oligomeric"]
    oligo_state = classified["oligomeric_state"]
    task_type: str = classified["task_type"]
    membrane_context: bool = classified["membrane_context"]

    # ── Download structure files ─────────────────────────────────────
    file_prov: dict[str, Any] = {}
    if pdb_id and download_structures:
        try:
            file_prov = download_structure_files(
                pdb_id,
                structures_dir=structures_dir,
                download_pdb=download_pdb,
                mirror=structure_mirror,
            )
        except Exception as exc:
            logger.warning("Structure file download failed for %s: %s", pdb_id, exc)

    # ── Parse structure quality from mmCIF ────────────────────────────
    quality: dict[str, Any] = {}
    cif_path = file_prov.get("structure_file_cif_path")
    if cif_path and Path(cif_path).exists():
        try:
            cif_text = Path(cif_path).read_text(encoding="utf-8")
            quality = parse_structure_quality(cif_text)
        except Exception as exc:
            logger.warning("Structure quality parse failed for %s: %s", pdb_id, exc)

    # ── mmCIF supplement data ────────────────────────────────────────
    supplement = raw.get("mmcif_supplement") or {}
    branched_count = supplement.get("branched_entity_count", 0)
    water_count = supplement.get("water_count", 0)

    # ── Experimental method ──────────────────────────────────────────
    exptl: list[dict] = raw.get("exptl") or []
    method: str | None = exptl[0].get("method") if exptl else None
    resolution = _resolution(entry_info)

    now_iso = datetime.now(timezone.utc).isoformat()
    tax_ids = _taxonomy_ids(protein_entities)
    org_names = _organism_names(protein_entities)

    # ── Bias flags ───────────────────────────────────────────────────
    has_metal = any(b.binder_type == "metal_ion" for b in bound_objects_typed)
    has_cofactor = any(b.binder_type == "cofactor" for b in bound_objects_typed)
    has_glycan = any(b.binder_type == "glycan" for b in bound_objects_typed)
    has_covalent = any(b.covalent_warhead_flag for b in bound_objects_typed)
    has_peptide = bool(peptide_entities) or any(
        b.binder_type == "peptide" for b in bound_objects_typed
    )
    sm_count = sum(1 for b in bound_objects_typed if b.binder_type == "small_molecule")

    quality_flags: list[str] = []
    if has_metal:
        quality_flags.append("metal_present")
    if has_cofactor:
        quality_flags.append("cofactor_present")
    if has_glycan:
        quality_flags.append("glycan_present")
    if has_covalent:
        quality_flags.append("covalent_binder")
    if has_peptide:
        quality_flags.append("peptide_partner")
    if sm_count > 1:
        quality_flags.append("multiple_bound_objects")
    if quality.get("contains_alternate_locations"):
        quality_flags.append("alternate_conformer_present")
    if quality.get("contains_partial_occupancy"):
        quality_flags.append("partial_occupancy_present")
    if quality.get("contains_missing_residues"):
        quality_flags.append("interface_incomplete_or_missing_residues")

    # ── 1. EntryRecord ───────────────────────────────────────────────
    entry = EntryRecord(
        source_database="RCSB",
        source_record_id=pdb_id,
        source_url=_RCSB_ENTRY_URL.format(pdb_id=pdb_id) if pdb_id else None,
        pdb_id=pdb_id,
        task_hint=task_type,
        downloaded_at=now_iso,
        title=struct.get("title"),
        experimental_method=method,
        structure_resolution=resolution,
        release_date=accession.get("initial_release_date"),
        deposit_date=accession.get("deposit_date"),
        deposited_atom_count=entry_info.get("deposited_atom_count"),
        protein_entity_count=entry_info.get("polymer_entity_count_protein"),
        nonpolymer_entity_count=entry_info.get("nonpolymer_entity_count"),
        polymer_entity_count=len(protein_entities) + len(peptide_entities) + len(other_poly),
        branched_entity_count=branched_count,
        water_count=water_count,
        assembly_count=entry_info.get("assembly_count"),
        # File provenance
        structure_file_cif_path=file_prov.get("structure_file_cif_path"),
        structure_file_cif_size_bytes=file_prov.get("structure_file_cif_size_bytes"),
        structure_file_pdb_path=file_prov.get("structure_file_pdb_path"),
        structure_file_pdb_size_bytes=file_prov.get("structure_file_pdb_size_bytes"),
        parsed_structure_format=file_prov.get("parsed_structure_format"),
        structure_download_url=file_prov.get("structure_download_url"),
        structure_downloaded_at=file_prov.get("structure_downloaded_at"),
        structure_file_hash_sha256=file_prov.get("structure_file_hash_sha256"),
        raw_file_path=str(Path("data/raw/rcsb") / f"{pdb_id}.json"),
        raw_format="json",
        # Quality from mmCIF
        refinement_resolution_high=quality.get("refinement_resolution_high"),
        r_work=quality.get("r_work"),
        r_free=quality.get("r_free"),
        model_count=quality.get("model_count"),
        contains_alternate_locations=quality.get("contains_alternate_locations"),
        contains_partial_occupancy=quality.get("contains_partial_occupancy"),
        contains_missing_residues=quality.get("contains_missing_residues"),
        # Assembly
        assembly_id=assembly_info.preferred_id if assembly_info else None,
        assembly_stoichiometry=(
            assembly_info.oligomeric_details if assembly_info else None
        ),
        oligomeric_state=oligo_state,
        homomer_or_heteromer=(
            "homomer" if is_homo else ("heteromer" if is_homo is False else None)
        ),
        # Biology
        taxonomy_ids=tax_ids or None,
        organism_names=org_names or None,
        membrane_vs_soluble="membrane" if membrane_context else "soluble",
        # Bias
        resolution_bin=_resolution_bin(resolution),
        metal_present=has_metal,
        cofactor_present=has_cofactor,
        glycan_present=has_glycan,
        covalent_binder_present=has_covalent,
        peptide_partner_present=has_peptide,
        multiligand_entry=sm_count > 1,
        mutation_present=None,  # requires UniProt enrichment
        quality_flags=quality_flags,
        field_provenance={
            "title": {"source": "RCSB", "method": "GraphQL_batch_API"},
            "experimental_method": {"source": "RCSB", "method": "GraphQL_batch_API"},
            "structure_resolution": {"source": "RCSB", "method": "GraphQL_batch_API"},
            "taxonomy_ids": {"source": "RCSB", "method": "GraphQL_batch_API_entity_organism"},
            "organism_names": {"source": "RCSB", "method": "GraphQL_batch_API_entity_organism"},
            "oligomeric_state": {
                "source": "RCSB",
                "method": "assembly_classifier",
                "assembly_id": assembly_info.preferred_id if assembly_info else None,
            },
            "homomer_or_heteromer": {
                "source": "RCSB",
                "method": "assembly_classifier",
            },
            "metal_present": {"source": "pbdata_rcsb_classify", "method": "bound_object_classification"},
            "cofactor_present": {"source": "pbdata_rcsb_classify", "method": "bound_object_classification"},
            "glycan_present": {"source": "pbdata_rcsb_classify", "method": "bound_object_classification"},
            "covalent_binder_present": {"source": "pbdata_rcsb_classify", "method": "bound_object_classification"},
            "peptide_partner_present": {"source": "pbdata_rcsb_classify", "method": "bound_object_classification"},
            "quality_flags": {"source": "pbdata_extract", "method": "quality_flag_aggregation"},
            "membrane_vs_soluble": {"source": "RCSB", "method": "keyword_heuristic"},
            "structure_file_cif_path": {
                "source": "RCSB",
                "method": "HTTP_download" if file_prov.get("structure_file_cif_path") else None,
                "mirror": file_prov.get("structure_download_mirror"),
            },
        },
        field_confidence={
            "title": "high",
            "experimental_method": "high",
            "structure_resolution": "high",
            "taxonomy_ids": "high" if tax_ids else "unknown",
            "organism_names": "high" if org_names else "unknown",
            "oligomeric_state": "medium" if oligo_state else "unknown",
            "homomer_or_heteromer": "medium" if is_homo is not None else "unknown",
            "metal_present": "medium",
            "cofactor_present": "medium",
            "glycan_present": "medium",
            "covalent_binder_present": "medium",
            "peptide_partner_present": "medium",
            "quality_flags": "medium" if quality_flags else "unknown",
            "membrane_vs_soluble": "medium",
            "structure_file_cif_path": "high" if file_prov.get("structure_file_cif_path") else None,
        },
    )

    # ── 2. ChainRecords ──────────────────────────────────────────────
    chains: list[ChainRecord] = []
    for ent in protein_entities + peptide_entities + other_poly:
        chain_ids = _polymer_chain_ids(ent) or []
        entity_id = ent.get("rcsb_id")
        poly = ent.get("entity_poly") or {}
        poly_type = poly.get("type")
        seq = _sequence(ent)
        desc = (ent.get("rcsb_polymer_entity") or {}).get("pdbx_description")

        # Determine subtype
        subtype = "other"
        if poly_type and poly_type.lower() in {"polypeptide(l)", "polypeptide(d)"}:
            subtype = "peptide" if (seq and len(seq) <= 30) else "protein"
        elif poly_type and "ribonucleotide" in poly_type.lower():
            subtype = "RNA" if "ribo" in poly_type.lower() and "deoxy" not in poly_type.lower() else "DNA"

        # UniProt / taxonomy per entity
        uid_list = (
            (ent.get("rcsb_polymer_entity_container_identifiers") or {})
            .get("uniprot_ids") or []
        )
        tax_list = []
        org_name = None
        for org in (ent.get("rcsb_entity_source_organism") or []):
            tid = org.get("ncbi_taxonomy_id")
            if tid is not None:
                try:
                    tax_list.append(int(tid))
                except (TypeError, ValueError):
                    pass
            if org.get("ncbi_scientific_name"):
                org_name = org["ncbi_scientific_name"]

        for cid in chain_ids:
            chains.append(ChainRecord(
                pdb_id=pdb_id,
                assembly_id=assembly_info.preferred_id if assembly_info else None,
                chain_id=cid,
                entity_id=entity_id,
                entity_type="polymer",
                polymer_type=poly_type,
                polymer_subtype=subtype,
                polymer_length=len(seq) if seq else None,
                polymer_sequence=seq,
                polymer_sequence_length=len(seq) if seq else None,
                chain_description=desc,
                entity_description=desc,
                entity_source_organism=org_name,
                entity_source_taxonomy_id=tax_list[0] if tax_list else None,
                is_protein=subtype == "protein",
                is_peptide=subtype == "peptide",
                is_nucleic_acid=subtype in ("DNA", "RNA"),
                uniprot_id=uid_list[0] if uid_list else None,
                copy_number_in_assembly=len(chain_ids),
            ))

    # ── 3. BoundObjectRecords ────────────────────────────────────────
    bo_records: list[BoundObjectRecord] = []
    for bo in bound_objects_typed:
        descriptor_info = chem_descriptors.get(bo.comp_id or "", {}) if chem_descriptors else {}
        molecular_weight = _safe_float(
            descriptor_info.get("formula_weight")
            or descriptor_info.get("FORMULA_WEIGHT")
            or descriptor_info.get("formulaWeight")
        )
        # Map binder_type to spec component_type
        type_map = {
            "small_molecule": "small_molecule",
            "cofactor": "cofactor",
            "metal_ion": "metal",
            "glycan": "glycan",
            "peptide": "peptide",
            "protein_chain": "protein_partner",
            "additive": "crystallization_additive",
            "nucleic_acid": "nucleic_acid",
            "unknown": "unknown",
        }
        # Map role to spec component_role
        role_map = {
            "primary_ligand": "primary_binder",
            "co_ligand": "co_binder",
            "cofactor": "catalytic_cofactor",
            "structural_ion": "metal_mediator",
            "metal_mediated_contact": "metal_mediator",
            "artifact": "likely_additive",
            "unknown": "unknown",
        }
        bo_records.append(BoundObjectRecord(
            pdb_id=pdb_id,
            component_id=bo.comp_id,
            component_name=bo.name,
            component_smiles=bo.smiles,
            component_inchikey=bo.inchi_key,
            component_molecular_weight=molecular_weight,
            component_type=type_map.get(bo.binder_type, "unknown"),
            component_role=role_map.get(bo.role, "unknown"),
            component_count=1,
            entity_id=bo.entity_id,
            chain_ids=bo.chain_ids,
            is_covalent=bo.is_covalent,
            covalent_warhead_flag=bo.covalent_warhead_flag,
            metal_elements=(
                [bo.comp_id] if bo.binder_type == "metal_ion" and bo.comp_id else None
            ),
            possible_metal_mediated_binding=(
                bo.role == "metal_mediated_contact" if bo.binder_type == "metal_ion" else None
            ),
            glycan_present=bo.binder_type == "glycan" or None,
            classification_rationale=bo.classification_rationale or None,
        ))

    # ── 4. InterfaceRecords ──────────────────────────────────────────
    iface_records: list[InterfaceRecord] = []
    for iface in interfaces_typed:
        iface_records.append(InterfaceRecord(
            pdb_id=pdb_id,
            interface_type=iface.interface_type,
            partner_a_chain_ids=iface.chain_ids_a,
            partner_b_chain_ids=iface.chain_ids_b,
            entity_id_a=iface.entity_id_a,
            entity_id_b=iface.entity_id_b,
            interface_is_symmetric=iface.is_symmetric,
            is_hetero=iface.is_hetero,
            entity_name_a=iface.entity_name_a,
            entity_name_b=iface.entity_name_b,
        ))
    iface_records.extend(_augment_interfaces_from_biolip(pdb_id, assay_samples or []))

    # ── 5. AssayRecords (empty from RCSB; filled by merge) ───────────
    assays: list[AssayRecord] = merge_assay_samples(assay_samples or [])

    # ── 6. ProvenanceRecords ─────────────────────────────────────────
    prov_records: list[ProvenanceRecord] = []
    prov_records.append(ProvenanceRecord(
        pdb_id=pdb_id,
        field_name="entry_metadata",
        source_name="RCSB",
        extraction_method="GraphQL_batch_API",
        confidence="high",
        timestamp=now_iso,
    ))
    if file_prov.get("structure_file_cif_path"):
        prov_records.append(ProvenanceRecord(
            pdb_id=pdb_id,
            field_name="structure_file_cif",
            source_name="RCSB",
            extraction_method="HTTP_download",
            raw_value=file_prov.get("structure_download_url"),
            normalized_value=file_prov.get("structure_file_cif_path"),
            confidence="high",
            timestamp=file_prov.get("structure_downloaded_at"),
        ))
    prov_records.append(ProvenanceRecord(
        pdb_id=pdb_id,
        field_name="entity_classification",
        source_name="pbdata_rcsb_classify",
        extraction_method="heuristic_classification",
        confidence="medium",
        timestamp=now_iso,
    ))

    return {
        "entry": entry,
        "chains": chains,
        "bound_objects": bo_records,
        "interfaces": iface_records,
        "assays": assays,
        "provenance": prov_records,
    }


def write_records_json(
    records: dict[str, Any],
    output_dir: Path,
) -> None:
    """Write multi-table records to individual JSON files per table.

    Creates output_dir/{table_name}/{pdb_id}.json for each table.
    """
    pdb_id = records["entry"].pdb_id or "UNKNOWN"

    for table_name, data in records.items():
        table_dir = output_dir / table_name
        table_dir.mkdir(parents=True, exist_ok=True)
        out_path = table_dir / f"{pdb_id}.json"

        if isinstance(data, list):
            json_data = [r.model_dump(mode="json") for r in data]
        else:
            json_data = data.model_dump(mode="json")

        out_path.write_text(json.dumps(json_data, indent=2), encoding="utf-8")
