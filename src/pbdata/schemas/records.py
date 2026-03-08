"""Multi-table record schemas per STRUCTURE_EXTRACTION_AGENT_SPEC.md.

These Pydantic v2 models map 1:1 to the six output tables:
  1. EntryRecord        -> entry_records.parquet
  2. ChainRecord        -> chain_records.parquet
  3. BoundObjectRecord  -> bound_object_records.parquet
  4. InterfaceRecord    -> interface_records.parquet
  5. AssayRecord        -> assay_records.parquet
  6. ProvenanceRecord   -> provenance_records.parquet

All models are frozen (immutable) to protect provenance integrity.
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict


# ── 1. Entry-level record ────────────────────────────────────────────


class EntryRecord(BaseModel):
    """One row per source entry / PDB entry."""

    model_config = ConfigDict(frozen=True)

    # Source & provenance (spec group 1)
    source_database: str
    source_record_id: str
    source_priority_rank: int = 1
    source_url: str | None = None
    secondary_source_ids: list[str] | None = None
    pdb_id: str | None = None
    task_hint: str | None = None
    record_version: str = "1"
    downloaded_at: str | None = None
    status: str = "ok"
    notes: str | None = None

    # Entry-level structural metadata (spec group 2)
    title: str | None = None
    experimental_method: str | None = None
    structure_resolution: float | None = None
    release_date: str | None = None
    deposit_date: str | None = None
    deposited_atom_count: int | None = None
    protein_entity_count: int | None = None
    nonpolymer_entity_count: int | None = None
    polymer_entity_count: int | None = None
    branched_entity_count: int | None = None
    water_count: int | None = None
    assembly_count: int | None = None

    # File provenance (spec file download policy)
    structure_file_cif_path: str | None = None
    structure_file_cif_size_bytes: int | None = None
    structure_file_pdb_path: str | None = None
    structure_file_pdb_size_bytes: int | None = None
    parsed_structure_format: str | None = None
    structure_download_url: str | None = None
    structure_downloaded_at: str | None = None
    structure_file_hash_sha256: str | None = None
    raw_file_path: str | None = None
    raw_format: str | None = None
    file_size_bytes: int | None = None

    # Recommended additional fields (spec group 2)
    refinement_resolution_high: float | None = None
    r_work: float | None = None
    r_free: float | None = None
    model_count: int | None = None
    contains_alternate_locations: bool | None = None
    contains_partial_occupancy: bool | None = None
    contains_missing_residues: bool | None = None

    # Assembly & oligomerization (spec group 4)
    assembly_id: str | None = None
    assembly_stoichiometry: str | None = None
    assembly_symmetry: str | None = None
    biological_assembly_description: str | None = None
    oligomeric_state: str | None = None
    homomer_or_heteromer: str | None = None
    is_symmetric_assembly: bool | None = None
    assembly_confidence: str | None = None

    # Organism / biological context (spec group 5) — entry-level convenience
    taxonomy_ids: list[int] | None = None
    organism_names: list[str] | None = None
    membrane_vs_soluble: str | None = None

    # Bias / audit fields (spec group 14)
    protein_family: str | None = None
    protein_class: str | None = None
    protein_fold: str | None = None
    domain_architecture: str | None = None
    organism_group: str | None = None
    pathway_category: str | None = None
    ligand_class: str | None = None
    ligand_scaffold: str | None = None
    ligand_molecular_weight_bin: str | None = None
    resolution_bin: str | None = None
    mutation_present: bool | None = None
    metal_present: bool | None = None
    cofactor_present: bool | None = None
    glycan_present: bool | None = None
    covalent_binder_present: bool | None = None
    peptide_partner_present: bool | None = None
    multiligand_entry: bool | None = None

    # Quality & ambiguity flags (spec group 15)
    quality_flags: list[str] | None = None
    quality_score: float | None = None

    # Per-field provenance support (spec group 1)
    field_provenance: dict[str, Any] | None = None
    field_confidence: dict[str, Any] | None = None


# ── 2. Chain-level record ────────────────────────────────────────────


class ChainRecord(BaseModel):
    """One row per chain/entity assignment."""

    model_config = ConfigDict(frozen=True)

    pdb_id: str | None = None
    assembly_id: str | None = None
    chain_id: str
    entity_id: str | None = None
    entity_type: str | None = None  # polymer | non-polymer | branched | water

    # Polymer identity (spec group 3)
    polymer_type: str | None = None  # polypeptide(L), polyribonucleotide, ...
    polymer_subtype: str | None = None  # protein | peptide | DNA | RNA | hybrid | other
    polymer_length: int | None = None
    polymer_sequence: str | None = None
    polymer_sequence_length: int | None = None
    chain_description: str | None = None
    entity_description: str | None = None
    entity_source_organism: str | None = None
    entity_source_taxonomy_id: int | None = None

    # Protein-specific (spec group 3)
    is_protein: bool = False
    is_peptide: bool = False
    is_nucleic_acid: bool = False
    protein_name: str | None = None
    gene_name: str | None = None
    uniprot_id: str | None = None
    engineered_mutations_present: bool | None = None
    mutation_strings: list[str] | None = None
    wildtype_or_mutant: str | None = None

    # Copy number / stoichiometry (spec groups 3 & 4)
    copy_number_in_assembly: int | None = None
    chain_stoichiometry: str | None = None
    entity_stoichiometry: str | None = None


# ── 3. Bound object record ───────────────────────────────────────────


class BoundObjectRecord(BaseModel):
    """One row per ligand/cofactor/metal/glycan/additive/etc."""

    model_config = ConfigDict(frozen=True)

    pdb_id: str | None = None

    # Component identity (spec group 6)
    component_id: str | None = None
    component_name: str | None = None
    component_synonyms: list[str] | None = None
    component_iupac_name: str | None = None
    component_iupac_name_truncated: str | None = None
    component_preferred_display_name: str | None = None
    component_iupac_display_suppressed: bool = False
    component_formula: str | None = None
    component_smiles: str | None = None
    component_inchi: str | None = None
    component_inchikey: str | None = None
    component_molecular_weight: float | None = None
    component_formal_charge: int | None = None
    component_stereochemistry_available: bool | None = None
    component_count: int | None = None

    # Type & role (spec group 6)
    component_type: str | None = None
    component_role: str | None = None
    bound_object_chain_contacts: list[str] | None = None
    bound_object_relevance_confidence: str | None = None

    # Entity cross-reference
    entity_id: str | None = None
    chain_ids: list[str] | None = None

    # Covalent flags
    is_covalent: bool | None = None
    covalent_warhead_flag: bool = False

    # Metal-specific (spec group 6)
    metal_elements: list[str] | None = None
    metal_counts: dict[str, int] | None = None
    metal_binding_residues: list[str] | None = None
    metal_roles: list[str] | None = None
    possible_metal_mediated_binding: bool | None = None

    # Glycan-specific (spec group 6)
    glycan_present: bool | None = None
    glycan_component_ids: list[str] | None = None
    glycan_chain_attachment_sites: list[str] | None = None
    glycan_binding_relevance: str | None = None

    # Ligand chemistry descriptors for bias analysis (spec group 9)
    ligand_is_fragment_like: bool | None = None
    ligand_is_druglike: bool | None = None
    ligand_is_peptidic: bool | None = None
    ligand_heavy_atom_count: int | None = None
    ligand_rotatable_bond_count: int | None = None
    ligand_hbond_donor_count: int | None = None
    ligand_hbond_acceptor_count: int | None = None
    ligand_tpsa: float | None = None
    ligand_logp: float | None = None
    ligand_formal_charge: int | None = None
    ligand_aromatic_ring_count: int | None = None

    # Provenance
    classification_rationale: str | None = None


# ── 4. Interface record ──────────────────────────────────────────────


class InterfaceRecord(BaseModel):
    """One row per interface/binding site."""

    model_config = ConfigDict(frozen=True)

    pdb_id: str | None = None

    # Interface type (spec group 7)
    interface_type: str | None = None
    # protein_protein | protein_peptide | protein_ligand |
    # protein_glycan | protein_nucleic_acid | mixed

    # Protein-protein / protein-peptide (spec group 7)
    partner_a_chain_ids: list[str] | None = None
    partner_b_chain_ids: list[str] | None = None
    entity_id_a: str | None = None
    entity_id_b: str | None = None
    interface_residues_a: list[str] | None = None
    interface_residues_b: list[str] | None = None
    interface_area: float | None = None
    interface_hydrogen_bonds: int | None = None
    interface_salt_bridges: int | None = None
    interface_hydrophobic_contacts: int | None = None
    interface_is_symmetric: bool = False
    is_hetero: bool = False

    # Protein-small-molecule binding site (spec group 7)
    binding_site_chain_ids: list[str] | None = None
    binding_site_residue_ids: list[str] | None = None
    binding_site_residue_names: list[str] | None = None
    ligand_contact_residues: list[str] | None = None
    contact_atom_count: int | None = None
    contact_residue_count: int | None = None
    hydrogen_bond_count: int | None = None
    salt_bridge_count: int | None = None
    hydrophobic_contact_count: int | None = None
    metal_contact_count: int | None = None
    binding_site_surface_area: float | None = None

    # Entity names
    entity_name_a: str | None = None
    entity_name_b: str | None = None


# ── 5. Assay record ──────────────────────────────────────────────────


class AssayRecord(BaseModel):
    """One row per affinity / measurement record."""

    model_config = ConfigDict(frozen=True)

    pdb_id: str | None = None
    source_database: str | None = None
    pair_identity_key: str | None = None

    # Binding energetics (spec group 10)
    binding_affinity_type: str | None = None
    binding_affinity_value: float | None = None
    binding_affinity_unit: str | None = None
    binding_affinity_log10_standardized: float | None = None
    binding_affinity_relation: str | None = None  # = | < | > | approx
    binding_affinity_is_mutant_measurement: bool | None = None
    delta_g: float | None = None
    delta_delta_g: float | None = None
    kon: float | None = None
    koff: float | None = None
    enthalpy_delta_h: float | None = None
    entropy_delta_s: float | None = None

    # Assay conditions (spec group 11)
    assay_method: str | None = None
    assay_temperature_c: float | None = None
    assay_temperature_k: float | None = None
    assay_ph: float | None = None
    assay_buffer: str | None = None
    assay_ionic_strength: float | None = None
    assay_salt: str | None = None
    assay_additives: list[str] | None = None
    assay_notes: str | None = None
    reported_measurements_text: str | None = None
    reported_measurement_mean_log10_standardized: float | None = None
    reported_measurement_count: int | None = None
    measurement_source_reference: str | None = None
    measurement_source_publication: str | None = None
    measurement_source_doi: str | None = None
    measurement_source_pubmed_id: str | None = None

    # Mutation annotations (spec group 12)
    mutation_strings: list[str] | None = None
    mutation_chain_ids: list[str] | None = None
    mutation_positions: list[int] | None = None
    mutation_wt_residues: list[str] | None = None
    mutation_mut_residues: list[str] | None = None
    mutation_count: int | None = None


# ── 6. Provenance record ─────────────────────────────────────────────


class ProvenanceRecord(BaseModel):
    """Per-field provenance trail for critical transformed fields."""

    model_config = ConfigDict(frozen=True)

    pdb_id: str | None = None
    field_name: str
    source_name: str | None = None
    source_record_key: str | None = None
    extraction_method: str | None = None
    raw_value: str | None = None
    normalized_value: str | None = None
    confidence: str | None = None
    timestamp: str | None = None


# ── Convenience: structure quality (spec group 13) ────────────────────


class StructureQuality(BaseModel):
    """Structure quality and model completeness fields (spec group 13).

    Stored as part of EntryRecord.field_provenance or as a separate
    nested dict, depending on pipeline configuration.
    """

    model_config = ConfigDict(frozen=True)

    structure_quality_score: float | None = None
    missing_residue_count: int | None = None
    missing_residues_near_interface: bool | None = None
    alternate_conformer_present: bool | None = None
    partial_occupancy_present: bool | None = None
    clash_or_geometry_warnings: list[str] | None = None
    model_completeness: float | None = None
    contains_unresolved_binding_site_regions: bool | None = None
