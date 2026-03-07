# Stress-Test Panel: Quick Expected Outcomes Table

| PDB | Main stressor(s) | Must detect / preserve |
|---|---|---|
| 2HHB | heterotetramer, repeated cofactors, symmetry | multimeric heteromer context, heme cofactors, metal present, symmetry-aware handling |
| 1ATP | protein + inhibitor peptide + ATP + 2 Mn | peptide binder, nucleotide/cofactor, metal ions, multiple bound objects |
| 8E1I | ATP + divalent cation(s) + inositol polyphosphate | multiple chemically distinct bound objects, metal presence, co-ligand handling |
| 4XT1 | membrane GPCR + protein ligand | membrane context, protein partner, no collapse into small-molecule logic |
| 2XPG | MHC with bound nonamer peptide | peptide classification, immune-complex partner context |
| 4P57 | TCR–MHC–peptide with Be2+ and Na+ | multicomponent immune complex, metal presence, metal-mediated binding ambiguity |
| 6EAQ | glycosylated receptor–Fc complex | glycans preserved, multimeric protein–protein context |
| 7DTZ | FGFR4 covalent inhibitor | covalent-binder flag, noncovalent/covalent separation |
| 1PW8 | acyl-enzyme complex | covalent-binder flag, acyl-enzyme handling |
| 6DO1 | membrane GPCR + peptide agonist + nanobody | membrane context, peptide binder, auxiliary protein partner |

## Minimum pass criteria
For the parser/normalizer to count as acceptable on this panel, it should:
- correctly separate polymer partners from non-polymer objects
- correctly distinguish peptide binders from ordinary small molecules
- preserve multiple bound objects when present
- flag covalent binders on 7DTZ and 1PW8
- flag membrane-protein context on 4XT1 and 6DO1
- preserve glycan presence on 6EAQ
- flag metal presence on 2HHB, 1ATP, 8E1I, and 4P57
- avoid flattening complex assemblies into over-simplified binary records without flags
