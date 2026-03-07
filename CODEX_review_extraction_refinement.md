# Codex Review Task: Structural Extraction and Edge-Case Correctness

Review the implementation specifically for **scientific and data-integrity correctness** on difficult PDB/mmCIF cases.

## Review objective
Judge whether the code correctly handles adversarial structural cases, not merely easy sanity checks.

## Review focus
Check whether the implementation correctly handles:
- biological assembly vs asymmetric unit
- homo- vs heteromer distinction
- symmetric interface deduplication
- multimeric complexes with multiple interfaces
- protein vs peptide vs small-molecule classification
- multiple ligands / cofactors / ions in one structure
- glycan detection and preservation
- covalent binder detection
- metal detection and metal-mediated interaction ambiguity
- membrane protein context
- alternate conformers / occupancy ambiguity
- incomplete interfaces / missing residues
- possible crystallization additive / artifact handling
- provenance preservation
- situations where ambiguity should be flagged instead of over-resolved

## Required output
Return:
1. critical scientific or data-integrity bugs
2. extraction/normalization mismatches against the manifest expectations
3. missing or weak stress tests
4. places where the code is too confident and should flag ambiguity instead
5. exact patch suggestions where possible

## Specific expectation
Use `tests/stress_test_panel.yaml` as the adversarial reference set.
These are intended to expose failure modes in extraction logic. Your review should evaluate whether the implementation would generalize to similarly difficult structures.
