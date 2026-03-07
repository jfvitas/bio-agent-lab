# Claude Code Task: Refine Structural Extraction, Normalization, and Stress Testing

You are **not** building the repo skeleton from scratch. Assume the project structure already exists. Your job is to refine the **details of what is extracted**, **how it is normalized**, and **how edge cases are tested** for difficult PDB/mmCIF structures.

## Goal
Make the structure-ingestion and normalization pipeline robust for **difficult, biologically realistic structures**, not just clean monomer + single-ligand cases.

The pipeline must handle:
- homo- vs heteromeric complexes
- symmetric assemblies
- protein–protein, protein–peptide, and protein–small-molecule interactions
- structures with multiple bound objects
- cofactors, metals, glycans, and likely crystallization additives
- covalent vs noncovalent binders
- membrane proteins
- ambiguity and incompleteness

## Core principle
Prefer **explicit ambiguity flags** over silent assumptions.

If the structure does not justify a single confident interpretation, store:
- candidate interpretations
- a confidence or rationale field
- machine-readable flags
- provenance linking back to raw source fields

---

## Required extraction behavior

### 1) Assembly and partner identity
Implement or refine extraction so the pipeline can:
- distinguish **asymmetric unit** from **biological assembly**
- identify all **polymer entities** and their **chain IDs**
- preserve entity IDs, chain IDs, and assembly IDs
- determine whether an interaction is:
  - homomeric
  - heteromeric
  - multimeric with multiple interfaces
- detect and avoid **double-counting symmetric interfaces**
- preserve stoichiometry where possible

### 2) Binder typing
Refine bound-object extraction so the code separates:
- protein partners
- peptide partners
- small-molecule ligands
- cofactors
- metals / ions
- glycans / PTM-linked carbohydrate objects
- likely crystallization additives / buffer components / artifacts

Do **not** collapse all HET-like objects into a single “ligand” bucket.

For every extracted non-polymer or binder-like object, create or refine normalized fields such as:
- object identifier / comp ID
- object class
- object role candidate(s)
- supporting evidence / rationale
- provenance

### 3) Multiple bound objects
Support structures containing more than one relevant bound object.

For each bound object, classify as one or more of:
- primary ligand
- co-ligand
- peptide binder
- catalytic cofactor
- prosthetic group
- structural ion
- metal-mediated interaction component
- glycan
- likely additive / artifact

Do **not** force a single “true ligand” if the structure clearly contains multiple possibilities.

### 4) Special chemistry and structural complications
Detect and flag when present:
- covalent protein–ligand attachment
- metal presence
- possible metal-mediated binding
- glycan presence
- peptide-vs-small-molecule ambiguity
- alternate conformers / altlocs
- partial occupancy
- unresolved or missing interface residues where detectable
- possible detergent/additive artifacts in membrane systems

### 5) Interface extraction
For each biologically relevant interface, extract or refine:
- partner chain sets
- partner entity sets
- interface type:
  - protein_protein
  - protein_peptide
  - protein_small_molecule
  - protein_glycan
  - mixed / ambiguous
- interface residue sets on each side
- contact counts
- symmetry relevance
- whether multiple interfaces exist in one assembly

### 6) Quality and ambiguity flags
Implement machine-readable flags including, at minimum:
- `homomeric_symmetric_interface`
- `heteromeric_interface`
- `multimeric_complex`
- `multiple_bound_objects`
- `metal_present`
- `metal_mediated_binding_possible`
- `cofactor_present`
- `glycan_present`
- `covalent_binder`
- `peptide_partner`
- `membrane_protein_context`
- `assembly_ambiguity`
- `alternate_conformer_present`
- `partial_occupancy_present`
- `possible_crystallization_additive`
- `interface_incomplete_or_missing_residues`

### 7) Normalization rules
Refine normalization so that:
- raw source values are preserved separately from normalized values
- ambiguous role assignments are stored as candidates plus rationale
- peptide binders remain distinguishable from small molecules
- metals remain distinguishable from cofactors and ligands
- covalent and noncovalent cases remain distinguishable
- glycan-containing cases remain distinguishable
- membrane-protein context remains distinguishable

---

## Required test work
Create a **stress-test suite** based on the file `tests/stress_test_panel.yaml`.

### You must:
1. Use the listed PDB IDs as adversarial parser/normalizer tests.
2. Build automated tests that assert expected extraction behavior.
3. Add tests at the **parser/normalizer layer**, not just end-to-end smoke tests.
4. Keep tests deterministic.
5. Fail loudly when expected structure features are missed.

### Test expectations
For each case, your tests should check as applicable:
- oligomer type
- multimer / symmetry handling
- presence of multiple bound objects
- presence of peptide binder
- presence of metals
- metal-mediated ambiguity flag
- presence of glycans
- covalent binder flag
- membrane-protein context
- whether additive/artifact ambiguity should be possible

---

## Deliverables
1. Refined extraction code
2. Refined normalization logic
3. Tests implementing the stress-test expectations
4. Any required manifest/helpers to keep these tests maintainable
5. A short markdown note describing:
   - what changed
   - what remains ambiguous
   - which cases still need manual review

---

## Constraints
- Do not remove important biological weirdness for convenience.
- Do not silently flatten multiple bound objects into one ligand.
- Do not silently treat peptide binders as ordinary small molecules.
- Do not silently merge covalent and noncovalent binder cases.
- Preserve provenance for all major transformed fields.
- Keep code typed and modular.
