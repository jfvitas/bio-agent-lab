# Structural Edge-Case Refactor — Change Report

## What changed

### New modules

| File | Purpose |
|------|---------|
| `src/pbdata/schemas/bound_objects.py` | Pydantic sub-schemas: `BoundObject`, `InterfaceInfo`, `AssemblyInfo` |
| `src/pbdata/sources/rcsb_classify.py` | All entity-classification logic, separated from the adapter |
| `docs/structural_edge_cases_report.md` | This file |
| `tests/test_structural_edge_cases.py` | 64 unit tests + parametrised integration tests |

### Modified modules

#### `src/pbdata/schemas/canonical_sample.py`
Added six optional fields (backward-compatible — all default to `None`):
- `bound_objects` — typed list of dicts from `BoundObject.model_dump()`.  Covers every nonpolymer entity and short peptide polymer entity.
- `interfaces` — list of dicts from `InterfaceInfo.model_dump()`.  One per unique polymer–polymer pair.
- `assembly_info` — dict from `AssemblyInfo.model_dump()`.  Preferred biological assembly metadata.
- `oligomeric_state` — string such as `"homodimer"`, `"hetero_complex_3_entities"`.
- `is_homo_oligomeric` — `True/False/None`.
- `polymer_entity_count` — total distinct polymer entities (protein + peptide).

#### `src/pbdata/sources/rcsb_classify.py` (new, extracts from rcsb.py)
Core additions:
- `classify_polymer_entity()` — protein / peptide (≤ 30 aa) / nucleic_acid / glycan / other
- `classify_nonpolymer_entity()` — metal_ion / cofactor / glycan / additive / small_molecule, with rationale string
- `has_covalent_warhead(smiles)` — regex-based pattern matching for 8 reactive groups
- `detect_membrane_context(raw_entry)` — keyword scan of `struct_keywords`
- `build_bound_objects()` — typed `BoundObject` list for all non-receptor entities
- `disambiguate_roles()` — promotes co-ligands; flags metal-mediated contacts
- `build_interfaces()` — homomeric, heteromeric, and protein–peptide interfaces
- `build_assembly_info()` — extracts `pdbx_struct_assembly` and `rcsb_assembly_info`
- `classify_entry()` — single top-level function returning fully classified dict

New classification constants:
- `_METAL_COMP_IDS` — 55+ free metal / halide ion comp_ids
- `_COFACTOR_COMP_IDS` — 45+ biochemical cofactors (nucleotides, hemes, flavins, CoA, etc.)
- `_GLYCAN_SUGAR_COMP_IDS` — 18 common monosaccharide comp_ids
- `_EXCLUDED_COMPS` — solvent / buffer / crystallisation artefacts (carried over from rcsb.py)
- `_METALLO_COFACTOR_IDS` — cofactors that coordinate a metal atom (hemes, Fe-S clusters, etc.)
- `_MEMBRANE_KEYWORDS` — 13 membrane-context keyword substrings

#### `src/pbdata/sources/rcsb.py`
- `normalize_record()` delegates all classification to `classify_entry()`.
- Backward-compatible single-ligand fields (`ligand_id`, `ligand_smiles`, etc.) still populated from the primary `small_molecule` BoundObject.
- `membrane_protein_context` stored in provenance dict so `audit.py` can read it.

#### `src/pbdata/sources/rcsb_search.py`
Extended `_ENTRY_GQL` to fetch:
- `rcsb_entry_info.assembly_count`
- `struct_keywords { pdbx_keywords text }` — for membrane detection
- `assemblies { rcsb_id pdbx_struct_assembly { oligomeric_details oligomeric_count } rcsb_assembly_info { polymer_entity_count polymer_entity_count_protein } }`
- `nonpolymer_entities.rcsb_nonpolymer_entity_container_identifiers { auth_asym_ids }` — chain IDs per ligand entity

#### `src/pbdata/quality/audit.py`
`compute_flags()` now derives the full set of structural / ambiguity flags from the
canonical sample fields.  New flags added:

| Flag | Trigger |
|------|---------|
| `homomeric_symmetric_interface` | InterfaceInfo with `is_symmetric=True` |
| `heteromeric_interface` | InterfaceInfo with `is_hetero=True` |
| `multimeric_complex` | `polymer_entity_count ≥ 3` or `len(interfaces) ≥ 2` |
| `multiple_bound_objects` | ≥ 2 non-artifact bound objects |
| `metal_present` | Free metal_ion entity OR metallocofactor (HEM, FES, CLA, …) |
| `metal_mediated_binding_possible` | Free metal + another non-artifact bound object |
| `cofactor_present` | Any BoundObject with `binder_type=="cofactor"` |
| `glycan_present` | Any BoundObject with `binder_type=="glycan"` |
| `covalent_binder` | `is_covalent=True` or `covalent_warhead_flag=True` |
| `peptide_partner` | Any BoundObject with `binder_type=="peptide"` |
| `possible_crystallization_additive` | Any BoundObject with `role=="artifact"` |
| `assembly_ambiguity` | `AssemblyInfo.assembly_count > 1` |
| `membrane_protein_context` | `provenance["membrane_protein_context"] == True` |

Flags documented but **not yet automatically set** (require coordinate parsing):
- `alternate_conformer_present`
- `partial_occupancy_present`
- `interface_incomplete_or_missing_residues`

`quality_score` is unchanged — only data-quality checks (resolution, method, sequence
coverage) affect it, so biologically complex structures are not penalised.

---

## What remains ambiguous

### Covalent binding (1PW8 acyl-enzyme case)
SMILES-based warhead detection catches Michael acceptors and epoxides at
normalisation time.  Serine-protease acyl-enzyme intermediates (cephalosporin-type)
form a covalent ester bond with the active-site Ser, which is NOT detectable from
SMILES alone — the warhead is not in the ligand but in the enzyme.  Detection of
this class requires parsing `struct_conn` records from the mmCIF file.

**Current behaviour:** `1PW8` will NOT set `covalent_binder` automatically unless
the inhibitor's SMILES happens to match a warhead pattern.

### Metal-mediated binding (4P57 Be²⁺ case)
Beryllium (Be) is not in `_METAL_COMP_IDS` — it is an extremely rare crystallography
ion.  The `metal_mediated_binding_possible` flag will not fire unless Be is added to
the set.  This is an intentional omission to avoid false positives; add `"BE"` to
`_METAL_COMP_IDS` if beryllium structures become relevant.

### Glycan detection (6EAQ, 4P57)
Polysaccharide **polymer entities** (entity_poly.type = polysaccharide) are classified
correctly via `classify_polymer_entity()`.  Glycan **nonpolymer entities** (small
sugars like NAG attached to protein) are detected via `_GLYCAN_SUGAR_COMP_IDS`.
However, glycan chains linked to protein residues as PTMs may appear as polymer
entities with type `polysaccharide`, in which case they are correctly classified but
are not reflected in `bound_objects` (only nonpolymer entities and short peptides
appear there).  The `glycan_present` flag covers the nonpolymer sugar case; a
separate pass over `other_poly` entities (stored in provenance) would be needed for
full coverage.

### Membrane context keyword heuristic
`detect_membrane_context()` relies on `struct_keywords.pdbx_keywords` and
`struct_keywords.text`.  These fields are not uniformly filled in RCSB — some
membrane proteins lack these annotations.  For rigorous membrane classification,
an external membrane-protein database (OPM, PDBTM) lookup would be required.

### Alternate conformers and partial occupancy
Both require coordinate-level inspection of the mmCIF file (`ANISOU` records,
occupancy columns in `ATOM`/`HETATM` lines).  These flags are documented but not
yet set.  Add a mmCIF parsing step (using the bundled `gemmi` dependency) to
populate them.

### Stoichiometry string
`AssemblyInfo` captures `oligomeric_count` (chain count) but does not derive a
Hill-notation stoichiometry string (e.g. `"A2B2"`).  This would require mapping
assembly chains back to their entity IDs, which is possible with the current
GraphQL data but not yet implemented.

### Double-counting symmetric interfaces
For a homo-oligomeric ring (e.g. GroEL 7-mer), `build_interfaces()` currently
creates one `InterfaceInfo` with `chain_ids_a=[chains[0]]` and
`chain_ids_b=chains[1:]`.  This correctly marks the interface as symmetric but does
not enumerate each unique pairwise contact.  For structures like GroEL where each
subunit–subunit interface is chemically distinct from the cross-ring interface,
additional logic would be needed.

---

## Cases that still need manual review

| PDB | Issue |
|-----|-------|
| `1PW8` | `covalent_binder` flag will NOT be set automatically — requires mmCIF struct_conn parsing |
| `4P57` | `metal_present` flag requires adding Be to `_METAL_COMP_IDS`; multi-component immune complex stoichiometry unverified |
| `6EAQ` | Glycan chains as polymer entities — `glycan_present` via `other_poly` requires additional code path |
| `4XT1` | Membrane context depends on keyword quality in RCSB for this specific entry — verify at integration test time |
| `6DO1` | Same as 4XT1; nanobody classification (short Fv fragment) may or may not be ≤ 30 aa — check actual sequence length |
| Any large assembly | GroEL-type 14+7 ring assemblies: oligomeric state string will be correct but interface enumeration is simplified |
