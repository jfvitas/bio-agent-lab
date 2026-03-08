# Protein Binding Dataset Pipeline — Comprehensive Structure Extraction & Enrichment Spec

## Purpose
This document is the authoritative implementation spec for coding agents working on structural extraction, metadata enrichment, and dataset-ready normalization for the protein binding dataset pipeline.

The goal is to produce records rich enough to:
- support training and evaluation of ML models for protein–ligand, protein–protein, and mutation-based binding tasks
- support dataset filtering by structure, chemistry, biology, assay type, and quality
- detect sampling bias, redundancy, and train/test leakage
- preserve provenance and uncertainty instead of silently guessing

---

## Core implementation principles
1. **Prefer explicit ambiguity to silent assumptions.**
   - If the code cannot confidently determine a single biologically relevant binder, store all candidates plus role/confidence/flags.
   - Use values such as `unknown`, `ambiguous`, `not_available`, or `low_confidence` instead of guessing.

2. **Separate raw extraction from normalized fields.**
   - Keep the original source values and the normalized values side by side.
   - Track provenance for every nontrivial field.

3. **Do not flatten all non-polymer components into “ligand.”**
   - Distinguish small molecules, cofactors, metals/ions, glycans, peptides, nucleic acids, additives, detergents, and artifacts.

4. **Do not merge incompatible assay labels without explicit transformation logic.**
   - `Kd`, `Ki`, `IC50`, `EC50`, `ΔG`, `ΔΔG`, `kon`, and `koff` must remain distinguishable.

5. **The default parser format is mmCIF.**
   - PDB format is a compatibility fallback, not the canonical parse target.

---

## Source landscape and what each source can reliably provide

### 1) RCSB PDB
Use for:
- structure identifiers and metadata
- experimental method and structure resolution
- deposition/release dates
- polymer entities, chain IDs, sequences, assemblies, taxonomy, cross-references
- nonpolymer identifiers and chemical component metadata
- downloadable coordinate files

Do **not** expect RCSB alone to reliably provide:
- curated absolute binding affinities for all entries
- assay temperature / pH / buffer conditions in a standardized, complete way

Primary interfaces:
- RCSB Data API / GraphQL / REST metadata
- RCSB Search API
- file download services

### 2) BindingDB
Use for:
- experimentally measured protein–small-molecule binding affinities
- `Kd`, `Ki`, `IC50`, and related assay-linked values
- assay conditions when available, such as pH, temperature, and buffer details
- ligand structures and target associations

Do **not** use BindingDB as the structural source of truth.

### 3) PDBbind / PDBbind+
Use for:
- structure-linked protein–ligand affinity annotations
- affinity labels associated with PDB complexes, commonly `Kd`, `Ki`, or `IC50`
- standardized benchmark-style protein–ligand datasets

Treat as:
- affinity annotation source for PDB-linked complexes
- not the sole source of structural or biochemical truth

### 4) BioLiP / BioLiP2
Use for:
- biologically relevant ligand annotations for PDB-derived structures
- curated binding-site information
- filtering out obvious non-biological ligands/additives where possible
- broad ligand classes including small molecules, peptides, nucleic acids, and others

This is especially useful when determining whether a HET group is biologically meaningful.

### 5) SKEMPI 2.0
Use for:
- mutation-centric protein–protein interaction data
- `ΔΔG`, affinity changes, kinetic terms like `kon` and `koff` when available
- thermodynamic and kinetic changes tied to a structurally resolved complex

This is primarily for mutant PPI tasks, not generic absolute-affinity protein–ligand data.

### 6) UniProt and linked biological resources
Use for enrichment of:
- canonical protein names
- gene names
- organism details
- function and subcellular information
- pathways and GO annotations
- domain/family/class information via cross-links when available

### 7) Additional enrichment resources
Use only as enrichment layers when available and justified:
- InterPro / Pfam / CATH / SCOP(e) for domains, families, and structural classes
- KEGG / Reactome / UniProt pathways for pathway/category labeling
- PubChem / ChEMBL for ligand names, synonyms, IUPAC names, structures, and descriptors

---

## File download policy

### Canonical structural file priority
1. **mmCIF (`.cif`) — required primary format**
2. **BinaryCIF (`.bcif`) — optional performance/storage optimization**
3. **PDB (`.pdb`) — compatibility fallback only**

### Rules
- Always download mmCIF first when available.
- Parse mmCIF as the canonical source for structure extraction.
- Optionally download PDB only if:
  - legacy tooling requires it, or
  - a downstream compatibility workflow depends on it.
- Store file provenance fields indicating which format(s) were downloaded and which format was actually parsed.

### Why mmCIF is the default
- It is the wwPDB’s official working/archive format.
- It handles structures of any size and carries richer structured metadata.
- It avoids many legacy PDB-format limitations and truncation issues.

### Required file fields
- `structure_file_cif_path`
- `structure_file_cif_size_bytes`
- `structure_file_pdb_path` (nullable)
- `structure_file_pdb_size_bytes` (nullable)
- `parsed_structure_format`
- `structure_download_url`
- `structure_downloaded_at`
- `structure_file_hash_sha256`

---

## Record granularity
The pipeline must support at least three linked record levels:

1. **Entry-level record** — one per source entry / PDB entry
2. **Chain/entity-level record** — one per polymer entity / chain mapping
3. **Binder/interface-level record** — one per meaningful interaction object or interface

This is necessary because a single structure can contain:
- multiple polymer partners
- multiple ligands/cofactors/metals
- multiple distinct interfaces
- multiple biological interpretations

Do not force all information into a single flat row if it destroys meaning.

---

## Required top-level schema groups
The implementation should support the following field groups.

# 1. Source & provenance
Required fields:
- `source_database`
- `source_record_id`
- `source_priority_rank`
- `source_url`
- `secondary_source_ids`
- `pdb_id`
- `task_hint`
- `record_version`
- `downloaded_at`
- `status`
- `notes`

Per-field provenance support:
- `field_provenance` (map from field name to source metadata)
- `field_confidence` (map)
- `field_transformations` (map)

Per-field provenance payload should support:
- `source_name`
- `source_record_key`
- `extraction_method`
- `raw_value`
- `normalized_value`
- `confidence`
- `timestamp`

# 2. Entry-level structural metadata
Required fields:
- `title`
- `experimental_method`
- `structure_resolution`
- `release_date`
- `deposit_date`
- `deposited_atom_count`
- `protein_entity_count`
- `nonpolymer_entity_count`
- `polymer_entity_count`
- `branched_entity_count`
- `water_count`
- `assembly_count`
- `raw_file_path`
- `raw_format`
- `file_size_bytes`

Recommended additional fields:
- `refinement_resolution_high`
- `r_work`
- `r_free`
- `phasing_or_em_details`
- `model_count`
- `contains_alternate_locations`
- `contains_partial_occupancy`
- `contains_missing_residues`

# 3. Polymer entities / chain identity
The code must extract **the identities of each protein chain contained in the file** and not just total counts.

Required fields:
- `polymer_entities`
- `chain_ids`
- `chain_entity_map`
- `entity_chain_map`
- `chain_stoichiometry`
- `entity_stoichiometry`
- `polymer_type`
- `polymer_subtype` (protein / peptide / DNA / RNA / hybrid / other)
- `polymer_length`
- `polymer_sequence`
- `polymer_sequence_length`
- `polymer_modifications`
- `chain_description`
- `entity_description`
- `entity_source_organism`
- `entity_source_taxonomy_id`

For proteins specifically:
- `protein_chain_ids`
- `protein_entity_ids`
- `protein_names`
- `gene_names`
- `uniprot_ids`
- `engineered_mutations_present`
- `mutation_strings`
- `wildtype_or_mutant`

For repeats / counts:
- include counts for each repeated entity in the biological assembly, not only in the asymmetric unit.

Suggested normalized chain table fields:
- `pdb_id`
- `assembly_id`
- `chain_id`
- `entity_id`
- `entity_type`
- `polymer_type`
- `sequence`
- `sequence_length`
- `protein_name`
- `gene_name`
- `uniprot_id`
- `organism_name`
- `taxonomy_id`
- `copy_number_in_assembly`
- `is_protein`
- `is_peptide`
- `is_nucleic_acid`

# 4. Biological assembly & oligomerization
Required fields:
- `assembly_id`
- `assembly_stoichiometry`
- `assembly_symmetry`
- `biological_assembly_description`
- `asymmetric_unit_chains`
- `assembly_chains`
- `oligomeric_state`
- `homomer_or_heteromer`
- `is_symmetric_assembly`
- `assembly_confidence`

Rules:
- distinguish asymmetric unit from biological assembly
- avoid double-counting symmetric homomer interfaces
- retain both chain-level and assembly-level stoichiometry

# 5. Organism and biological context
Required fields:
- `taxonomy_ids`
- `organism_names`
- `genus`
- `species`
- `strain`
- `kingdom`
- `host_organism` (when relevant)
- `subcellular_location`
- `biological_process_terms`
- `molecular_function_terms`
- `cellular_component_terms`
- `pathways`
- `protein_family`
- `protein_class`
- `enzyme_ec_number`

Rules:
- if multiple polymer partners come from different taxa, preserve all partner-specific organism assignments
- store entry-level combined taxonomy only as a convenience field, not as a replacement for chain-level assignments

# 6. Ligands, cofactors, metals, glycans, additives, and other bound objects
The existing `ligand_ids` and `ligand_names` fields are insufficient by themselves.

Required bound-object fields:
- `bound_object_ids`
- `bound_object_names`
- `bound_object_counts`
- `bound_object_roles`
- `bound_object_types`
- `bound_object_chain_contacts`
- `bound_object_relevance_confidence`

For each bound object, extract:
- `component_id` (CCD / source identifier)
- `component_name`
- `component_synonyms`
- `component_iupac_name`
- `component_formula`
- `component_smiles`
- `component_inchi`
- `component_inchikey`
- `component_molecular_weight`
- `component_formal_charge`
- `component_stereochemistry_available`
- `component_count`
- `component_type`
- `component_role`

Supported `component_type` values:
- `small_molecule`
- `metal`
- `cofactor`
- `peptide`
- `protein_partner`
- `glycan`
- `nucleic_acid`
- `lipid`
- `detergent`
- `buffer_component`
- `crystallization_additive`
- `solvent`
- `artifact`
- `unknown`

Supported `component_role` values:
- `primary_binder`
- `co_binder`
- `catalytic_cofactor`
- `structural_cofactor`
- `metal_mediator`
- `substrate`
- `product`
- `substrate_analog`
- `allosteric_modulator`
- `orthosteric_ligand`
- `detergent_or_stabilizer`
- `likely_additive`
- `unknown`

### IUPAC naming rule
- Store `component_iupac_name` when available from a trusted chemical source.
- If the IUPAC name is excessively long, unwieldy, peptide-like, polymeric, or clearly not useful for filtering/UI display, also populate:
  - `component_iupac_name_truncated`
  - `component_preferred_display_name`
  - `component_iupac_display_suppressed` = true

### Metal-specific fields
Required:
- `metal_elements`
- `metal_counts`
- `metal_binding_residues`
- `metal_contact_objects`
- `metal_roles`
- `possible_metal_mediated_binding`

Rules:
- metals must not be silently merged with small molecules
- support repeated metals and per-element counts, e.g. `Mg: 2`, `Zn: 1`

### Glycan-specific fields
Required:
- `glycan_present`
- `glycan_component_ids`
- `glycan_chain_attachment_sites`
- `glycan_binding_relevance`

### Additive/artifact detection
The pipeline should attempt to flag likely non-biological components such as:
- glycerol
- sulfate
- tris
- PEG fragments
- detergents
- cryoprotectants

Use BioLiP / BioLiP2 or curated rules where possible to determine likely biological relevance.

# 7. Binding site and interface extraction
The pipeline must extract structural motifs and residue-level interface information, especially around active sites and binding sites.

Required fields for protein–small-molecule interactions:
- `binding_site_chain_ids`
- `binding_site_residue_ids`
- `binding_site_residue_names`
- `ligand_contact_residues`
- `contact_atom_count`
- `contact_residue_count`
- `hydrogen_bond_count`
- `salt_bridge_count`
- `hydrophobic_contact_count`
- `metal_contact_count`
- `binding_site_surface_area` (if available)

Required fields for protein–protein / protein–peptide interfaces:
- `partner_a_chain_ids`
- `partner_b_chain_ids`
- `interface_residues_a`
- `interface_residues_b`
- `interface_area`
- `interface_hydrogen_bonds`
- `interface_salt_bridges`
- `interface_hydrophobic_contacts`
- `interface_is_symmetric`
- `interface_type`

Supported `interface_type` values:
- `protein_protein`
- `protein_peptide`
- `protein_ligand`
- `protein_glycan`
- `protein_nucleic_acid`
- `mixed`

Rules:
- support multiple interfaces per entry
- do not assume one structure means one interface

# 8. Structural motifs, domains, and active-site annotations
Required fields:
- `binding_motifs`
- `active_site_annotations`
- `catalytic_residues`
- `motif_source`
- `domain_architecture`
- `domain_ids`
- `domain_names`
- `structural_fold_classes`

Potential enrichment sources:
- UniProt feature annotations
- InterPro
- Pfam
- CATH / SCOP(e)
- BioLiP binding site annotations

Rules:
- active-site or catalytic motifs should be stored separately from generic domains
- preserve the source of each motif annotation

# 9. Ligand chemistry descriptors for bias analysis
To support dataset filtering and bias detection, enrich ligand records with:
- `ligand_scaffold`
- `ligand_class`
- `ligand_is_fragment_like`
- `ligand_is_druglike`
- `ligand_is_peptidic`
- `ligand_heavy_atom_count`
- `ligand_rotatable_bond_count`
- `ligand_hbond_donor_count`
- `ligand_hbond_acceptor_count`
- `ligand_tpsa`
- `ligand_logp`
- `ligand_formal_charge`
- `ligand_aromatic_ring_count`

If a chemistry toolkit is used for descriptors, record the method in provenance.

# 10. Binding energetics and assay data
Required fields:
- `binding_affinity_type`
- `binding_affinity_value`
- `binding_affinity_unit`
- `binding_affinity_log10_standardized`
- `delta_g`
- `delta_delta_g`
- `kon`
- `koff`
- `enthalpy_delta_h`
- `entropy_delta_s`
- `binding_affinity_relation` (e.g. `=`, `<`, `>`, `approx`)
- `binding_affinity_is_mutant_measurement`

Supported affinity labels:
- `Kd`
- `Ki`
- `IC50`
- `EC50`
- `ΔG`
- `ΔΔG`
- `kon`
- `koff`
- `Ka`

Rules:
- keep the original raw reported value and unit
- store standardized value separately
- do not convert `IC50` to `Kd` unless an explicit, documented transformation model is used
- mutation-derived values belong in mutant/PPI task tracks unless explicitly modeled otherwise

# 11. Assay conditions and experimental context
Required fields:
- `assay_method`
- `assay_temperature_c`
- `assay_temperature_k`
- `assay_ph`
- `assay_buffer`
- `assay_ionic_strength`
- `assay_salt`
- `assay_additives`
- `assay_notes`
- `measurement_source_reference`
- `measurement_source_publication`
- `measurement_source_doi`
- `measurement_source_pubmed_id`

Rules:
- these are often absent from RCSB and must usually come from BindingDB, SKEMPI, literature, or source-specific files
- preserve `not_available` rather than guessing

# 12. Mutation annotations
Required fields when applicable:
- `mutation_strings`
- `mutation_chain_ids`
- `mutation_positions`
- `mutation_wt_residues`
- `mutation_mut_residues`
- `mutation_count`
- `engineered_mutation_notes`

Use especially for:
- SKEMPI
- mutated crystal structures
- engineered constructs from RCSB / source metadata

# 13. Structure quality and model completeness
Required fields:
- `structure_quality_score`
- `missing_residue_count`
- `missing_residues_near_interface`
- `alternate_conformer_present`
- `partial_occupancy_present`
- `clash_or_geometry_warnings`
- `model_completeness`
- `contains_unresolved_binding_site_regions`

Rules:
- these are critical for deciding whether a structure should be trainable, testable, or only retained with flags

# 14. Bias/audit-oriented feature fields
These fields must exist to make it possible to inspect and filter dataset composition.

Required bias/audit fields:
- `protein_family`
- `protein_class`
- `protein_fold`
- `domain_architecture`
- `organism_group`
- `pathway_category`
- `oligomeric_state`
- `homomer_or_heteromer`
- `membrane_vs_soluble`
- `ligand_class`
- `ligand_scaffold`
- `ligand_molecular_weight_bin`
- `ligand_charge_bin`
- `ligand_logp_bin`
- `binding_affinity_type`
- `experimental_method`
- `resolution_bin`
- `mutation_present`
- `metal_present`
- `cofactor_present`
- `glycan_present`
- `covalent_binder_present`
- `peptide_partner_present`
- `multiligand_entry`

These fields should be populated even if they are partially derived from lower-level tables.

# 15. Quality and ambiguity flags
Every entry should support a machine-readable list of flags.

Required flags include:
- `homomeric_symmetric_interface`
- `heteromeric_interface`
- `multiple_bound_objects`
- `metal_present`
- `possible_metal_mediated_binding`
- `cofactor_present`
- `glycan_present`
- `covalent_binder`
- `peptide_partner`
- `assembly_ambiguity`
- `alternate_conformer_present`
- `partial_occupancy_present`
- `possible_crystallization_additive`
- `interface_incomplete_or_missing_residues`
- `assay_data_missing`
- `assay_conditions_missing`
- `structure_quality_warning`
- `large_multicomponent_assembly`

---

## Source-specific population rules and fallback methods

### RCSB PDB
Populate directly from RCSB when available:
- `title`
- `experimental_method`
- `structure_resolution`
- `release_date`
- `deposit_date`
- `polymer entities`
- `chain/entity mappings`
- `polymer sequences`
- `taxonomy`
- `assemblies`
- `nonpolymer component ids`
- `chemical component names`
- `coordinate files`

Fallbacks / enrichment for missing biology:
- `uniprot_ids`, `gene_names`, `protein_names`, `enzyme_ec_number`, `GO`, `pathways` via UniProt and related resources

Fallbacks / enrichment for missing chemistry:
- `SMILES`, `InChI`, `IUPAC`, descriptors via chemical component dictionary, PubChem, or ChEMBL

Fallbacks for binding energetics:
- BindingDB
- PDBbind
- BioLiP relevance annotations
- SKEMPI for mutation-driven PPI values
- literature extraction only if still missing and if provenance is explicit

### BindingDB
Populate directly when available:
- affinity types/values/units
- target names and identifiers
- ligand structures and names
- assay conditions such as temperature, pH, and buffer details when present
- publication references

Fallback alignment rule:
- if BindingDB target must be mapped to an RCSB structure, do not assume 1:1 mapping without sequence / identifier validation

### PDBbind / PDBbind+
Populate directly when available:
- PDB-linked affinity label
- affinity type/value/unit
- benchmark subset membership (if relevant)

Fallback rule:
- structural detail still comes from RCSB/mmCIF parsing, not from PDBbind text fields alone

### BioLiP / BioLiP2
Populate directly when available:
- biologically relevant ligand/site annotations
- curated binding-site relevance
- ligand role hints

Use as a strong relevance layer when distinguishing meaningful ligands from additives/artifacts.

### SKEMPI 2.0
Populate directly when available:
- mutation strings
- `ΔΔG`
- kinetic changes (`kon`, `koff`) when present
- temperature if present
- interaction partner names

Fallback rule:
- structural parsing still comes from the linked PDB/mmCIF structure

---

## Output tables that must be produced
At minimum, the pipeline should write:

1. `entry_records.parquet`
   - one row per entry/source record

2. `chain_records.parquet`
   - one row per chain/entity assignment

3. `bound_object_records.parquet`
   - one row per ligand/cofactor/metal/glycan/additive/etc.

4. `interface_records.parquet`
   - one row per interface/binding site

5. `assay_records.parquet`
   - one row per affinity/measurement record

6. `provenance_records.parquet` (or JSON sidecar)
   - normalized provenance trail for all critical transformed fields

This layered output is strongly preferred over a single giant denormalized table.

---

## Filtering and bias-analysis requirements
The resulting schema must support filtering by at least the following:
- source database
- task type
- protein family/class/fold
- organism/genus/species
- membrane vs soluble
- homo- vs heteromer
- oligomeric state
- ligand type/class/scaffold
- metal presence
- cofactor presence
- glycan presence
- peptide partner presence
- covalent vs noncovalent
- assay type
- assay condition ranges (temperature, pH)
- experimental method
- resolution range
- mutation present vs absent
- structure-quality flags
- large multicomponent assemblies vs simple systems

The audit layer must be able to summarize overrepresentation in any of these categories.

---

## Implementation priorities
### Phase 1 — immediately required
Implement first:
- mmCIF download + parsing
- chain/entity mapping
- assembly-level stoichiometry
- bound object extraction with role/type classification
- metal/glycan/cofactor/additive detection
- UniProt-based protein identity enrichment
- basic assay merge logic for RCSB + BindingDB + PDBbind + SKEMPI
- provenance scaffolding

### Phase 2 — strongly recommended
Implement next:
- interface residue extraction
- active-site / binding-site enrichment
- ligand chemistry descriptors and scaffolding
- BioLiP relevance integration
- structure quality and missing-residue flags
- bias audit features

### Phase 3 — advanced refinement
Implement later if needed:
- domain/fold annotations from InterPro/Pfam/CATH/SCOP(e)
- pathway/category enrichment from KEGG/Reactome
- literature mining for missing assay conditions
- advanced motif detection at active sites and interfaces

---

## What not to do
- Do not make a single `ligand_name` field carry every nonpolymer concept.
- Do not assume the first nonpolymer object is the real ligand.
- Do not assume a single affinity value exists per structure.
- Do not treat all bound peptides as small molecules.
- Do not collapse covalent and noncovalent binders together.
- Do not silently discard glycans, metals, cofactors, or additives.
- Do not overwrite raw source values with normalized values.

---

## Acceptance criteria for coding agents
The implementation is acceptable only if it can:
- download and parse mmCIF as the default structure format
- populate chain-level protein identity and stoichiometry
- preserve repeated copies of protein entities in assemblies
- distinguish proteins, peptides, small molecules, cofactors, metals, glycans, additives, and artifacts
- record metal identities and counts
- store ligand names and IUPAC names where appropriate
- enrich protein identity with UniProt-linked biological data
- attach binding affinity values and assay conditions from non-RCSB sources when available
- write normalized multi-table outputs suitable for filtering and bias analysis
- preserve provenance and confidence for critical fields
- prefer ambiguity flags over unsupported assumptions

---

## Recommended developer note
When a field is not available from the source currently being processed, do not leave the schema design underspecified. Instead:
1. keep the field in the schema
2. populate it from an approved fallback source if available
3. otherwise fill with `null` / `not_available` plus provenance

This ensures the dataset stays structurally consistent across sources.
