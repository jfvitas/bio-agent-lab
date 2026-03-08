
# Panel C – Expected Outcomes

This file defines validation expectations for the extended stress-test panel.

The extraction pipeline should verify the following capabilities.

## Structural Extraction

The parser must correctly extract:

- chain IDs
- polymer entities
- biological assembly
- oligomeric state
- ligand identities
- metal atoms
- glycans
- cofactors
- peptide vs small molecule ligands

## Multi-Object Handling

Structures containing multiple bound objects must preserve all entities rather than collapsing to a single ligand.

## Source-Specific Extraction Expectations

### RCSB
Must provide:

- chain IDs
- polymer sequences
- ligand IDs
- ligand names
- metal atoms
- glycan entities
- assembly information
- experimental method
- resolution

### PDBbind
If available:

- affinity values
- ΔG estimates

### BindingDB
If available:

- Kd
- Ki
- IC50
- assay conditions

### BioLiP
If available:

- biologically relevant ligand annotations
- curated binding site residues

### SKEMPI
If available:

- mutation annotations
- ΔΔG values
- kon/koff

## Acceptance Criteria

For each PDB entry:

1. Structure parsed successfully.
2. All chains mapped to entities.
3. Ligands classified correctly.
4. Metals and cofactors identified.
5. Biological assembly determined.
6. No silent collapsing of multi-ligand systems.
7. Provenance recorded for each field.

Ambiguities must be flagged rather than silently resolved.
