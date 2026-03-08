# Panel B Expected Outcomes

This file is for **evaluation only**. It should be used to test parser and normalizer behavior, not as training supervision for model fitting or heuristic overfitting.

## Required philosophy

- Prefer **explicit ambiguity flags** over silent guesses.
- Preserve **multiple bound objects** when present.
- Keep **proteins, peptides, small molecules, cofactors, metals, and glycans** distinct.
- Keep **covalent** and **noncovalent** cases separable.
- Do not flatten giant multicomponent assemblies into a fake one-ligand representation.

## Per-entry expectations

| PDB ID | Core challenge | Minimum expected outcome |
|---|---|---|
| 6PFY | Huge membrane multimer with many cofactors | Many polymer chains detected; membrane-complex classification; multiple bound objects retained; cofactors and metals flagged; no high-confidence single-primary-ligand claim |
| 3WU2 | Large membrane assembly with cofactors/metals | Multimeric membrane assembly retained; cofactors and metals preserved; no flattening to a simple ligand case |
| 7O7Q | Tetrameric protein trap / activated multimer | Tetrameric or otherwise multimeric assembly recognized; protein-partner complexity preserved; no default pairwise fragmentation without explicit policy |
| 9IU1 | Glycosylated viral-receptor complex | Protein-protein interface recognized; glycans detected and preserved as biologically relevant objects |
| 5XJE | Glycan-mediated Fc/FcγR interaction | Glycans detected and preserved distinctly; heteromeric protein-protein complex retained |
| 4X4M | Glycan recognition in immune complex | Glycans present and biologically relevant; not discarded as incidental HET groups |
| 6BML | Membrane covalent intermediate case | Covalent-binder logic triggered; membrane context retained; record can be excluded from noncovalent-only subsets |
| 1DY9 | Cofactor + inhibitor + reactive chemistry | Multiple bound objects retained; cofactor recognized; reactive/covalent-like chemistry handled explicitly or flagged as ambiguity |
| 2VKE | Metal-chelated small-molecule binding | Metal present; metal-mediated or chelate logic triggered; not reduced to plain ligand + unrelated ion |
| 4H26 | Metal-dependent immune recognition | Metal present; peptide partner recognized; metal-mediated binding ambiguity/logic explicitly flagged |

## Suggested machine-checkable assertions

### 6PFY
- `polymer_chain_count >= 20`
- `multiple_bound_objects == true`
- `cofactor_present == true`
- `metal_present == true`
- `membrane_complex == true`
- `single_primary_ligand_confidence != high`

### 3WU2
- `polymer_chain_count >= 10`
- `multiple_bound_objects == true`
- `cofactor_present == true`
- `metal_present == true`
- `membrane_complex == true`
- `single_primary_ligand_confidence != high`

### 7O7Q
- `polymer_chain_count >= 4`
- `multiple_protein_partners == true`
- `single_primary_ligand_confidence != high`
- `default_pairwise_fragmentation == false`

### 9IU1
- `polymer_chain_count >= 2`
- `protein_partner_present == true`
- `glycan_present == true`
- `glycan_biological_relevance == true`

### 5XJE
- `polymer_chain_count >= 2`
- `protein_partner_present == true`
- `glycan_present == true`
- `glycan_biological_relevance == true`

### 4X4M
- `polymer_chain_count >= 2`
- `protein_partner_present == true`
- `glycan_present == true`
- `glycan_biological_relevance == true`

### 6BML
- `membrane_complex == true`
- `covalent_binder == true`
- `multiple_bound_objects == true`

### 1DY9
- `multiple_bound_objects == true`
- `cofactor_present == true`
- `covalent_or_reactive_binder_logic == true`

### 2VKE
- `multiple_bound_objects == true`
- `metal_present == true`
- `metal_mediated_binding_possible == true`

### 4H26
- `polymer_chain_count >= 3`
- `multiple_protein_partners == true`
- `peptide_partner == true`
- `metal_present == true`
- `metal_mediated_binding_possible == true`

## Pass/fail guidance

A case should be considered a failure if the system:
- collapses biologically distinct objects into one generic ligand,
- silently discards glycans, cofactors, or metals that are structurally relevant,
- treats clearly covalent/reactive chemistry as ordinary noncovalent binding,
- silently guesses a single interaction interpretation where ambiguity should be flagged,
- or fragments giant assemblies into simplistic pairwise records without a declared interface policy.
