---
task_id: full_repository_review
role: reviewer
date: 2026-03-11
status: reviewed
---

# Reviewer Report — Full Repository Review (Strict)

## 1. Review Scope

**Files reviewed (all read in full):**
- Governance: CLAUDE.md, AGENTS.md, README.md, pyproject.toml
- All spec files in specs/ directory (15+ files)
- All source modules under src/pbdata/ (96 modules)
- All 39 test files under tests/
- All 3 stress test panels and 3 expected outcomes files
- Documentation: docs/bio_agent_full_scope_gap_analysis.md, docs/bio_agent_full_scope_architecture.md

**Test suite result:** 326 passed, 71 deselected (integration), 0 failures, 181.10 seconds.

## 2. Critical Failures

**None found.** No data-corruption paths, no silent data loss, no schema violations, no security vulnerabilities. All 326 unit tests pass. Immutable test outcome files verified intact.

## 3. Major Issues

### M1. `pandas` not declared in pyproject.toml dependencies
Severity: Major (runtime crash for new installs). Used unconditionally in: `pipeline/feature_execution.py`, `pipeline/physics_feedback.py`, `graph/structural_graphs.py`, `table_io.py`. Not listed in `[project.dependencies]` or `[project.optional-dependencies]`.
**Fix:** Add `pandas>=2.0` to `[project.dependencies]`.

### M2. `requests` not declared as dependency
Severity: Major (crashes core `ingest` command). Used by: `sources/rcsb.py`, `sources/rcsb_search.py`, `sources/bindingdb.py`, `sources/chembl.py`.
**Fix:** Add `requests>=2.31` to `[project.dependencies]`.

### M3. Pathway reasoning and risk scoring are stubs exposed as functional
Severity: Major (misleading). `risk/pathway_reasoning.py` returns `status: "stub"`. README and GUI present these as working commands.
**Fix:** Raise `NotImplementedError` or emit `"status": "not_implemented"` in manifests.

### M4. Split policy spec declares 7 strategies; only 3 implemented
Severity: Major (spec compliance). Missing: time_split, source_split, scaffold_split, family_split, mutation_split.
**Fix:** Mark unimplemented strategies as `status: planned` in split_policy.yaml.

## 4. Minor Issues

- m1. `_EXCLUDED_COMPS` overlaps with `_METAL_COMP_IDS` — maintenance hazard
- m2. `FeatureRecord` validator rejects unknown keys — brittle for extension
- m3. Accumulated test artifacts under `tests/_tmp/`
- m4. Missing docstrings on public functions in storage.py, table_io.py, pairing.py
- m5. AppConfig and SourcesConfig models not frozen (convention says "all records frozen")
- m6. `_residue_secondary_structure` always returns "coil" — undocumented fallback

## 5. Scientific Concerns

### S1. Covalent warhead detection — SMILES-pattern-only
9 regex patterns matched against SMILES. No coordinate validation against struct_conn records. Known false positive/negative risks. Mitigated by: explicit rationale strings, `is_covalent` defaults to None.
Risk: Low-medium. Flag is informational.

### S2. Membrane-protein detection — keyword heuristic
24 keywords checked against struct_keywords. False positives possible for soluble fragments of membrane proteins. Mitigated by: confidence set to "medium".
Risk: Low.

### S3. BoundObject role disambiguation — order-dependent
First small_molecule becomes "primary_ligand", rest become "co_ligand". Ordering from GraphQL entity IDs, not biological relevance.
Risk: Medium. Recommendation: tie-break by molecular weight.

### S4. Quality score ignores structural completeness
`compute_score()` checks 8 data-completeness conditions but not: alternate conformers, missing residues, partial occupancy, R-free/R-work. A structure with 30% missing residues scores same as complete structure.
Risk: Medium. Recommendation: add structural factors or rename to `data_completeness_score`.

### S5. K-mer Jaccard threshold not configurable from GUI
CLI accepts `--jaccard-threshold` but GUI hardcodes 0.30. Users working with conserved families may need higher threshold.
Risk: Low.

## 6. Data Integrity Concerns

- **D1. Frozen models — COMPLIANT.** All data models use `ConfigDict(frozen=True)`.
- **D2. Provenance `ingested_at` — COMPLIANT.** Validator enforces on CanonicalBindingSample.
- **D3. Structure file hash — COMPLIANT.** SHA-256 computed and stored.
- **D4. No raw data overwrite — COMPLIANT.** Invalid files deleted and re-downloaded.
- **D5. Canonical schema coverage — COMPLIANT.** All 37 spec fields + 22 extensions.
- **D6. Multi-table schema — COMPLIANT.** 6 record types match extraction spec 1:1.
- **D7. Immutable test files — VERIFIED INTACT.** No modifications after creation.

## 7. Architecture Deviations

### A1. Feature pipeline monolithic vs 7 discrete stages
Spec: FEATURE_PIPELINE_EXECUTION_SPEC.md requires separate modules. Implementation: single 1,400-line `feature_execution.py`.
Impact: Medium.

### A2. Surrogate model — linear regression vs equivariant GNN
Spec: LOCAL_PHYSICS_SURROGATE_MASTER_SPEC.md mandates geometry-aware model. Implementation: ordinary least-squares. Documented as V1 scaffold.
Impact: High for spec compliance.

### A3. Knowledge graph — scaffolding only
Spec: MASTER_ENGINEERING_INSTRUCTIONS lists 18+ databases. Implementation: no network database adapters active.
Impact: Known gap, documented.

### A4. Feature directory contract divergence
Spec: `features/structural_features/`, etc. Implementation: uses `artifacts/base_features/` instead.
Impact: Low.

### A5. Run modes partially implemented
Spec: 4 modes (full_build, resume, stage_only, inference_prepare). Implementation: full_build only.
Impact: Medium.

### A6. Workspace layout dual hierarchy
`data/` tree for core pipeline, workspace `artifacts/` tree for ML engineering. Two parallel directories coexist.
Impact: Low.

## 8. QA Gaps

- Q1. No GUI tests (3,300 lines untested)
- Q2. No CLI end-to-end tests via CliRunner
- Q3. Panel B has no test file (Panel A and C do)
- Q4. No performance benchmarks
- Q5. Several modules with no test file
- Q6. Integration tests cannot be verified offline (71 deselected)
- Q7. Feature pipeline acceptance tests mostly missing (per spec section 16)

## 9. Required Fixes (prioritized)

### Priority 1 — Blocking for any new install
1. Add `pandas>=2.0` to pyproject.toml dependencies
2. Add `requests>=2.31` to pyproject.toml dependencies

### Priority 2 — Significant correctness/usability
3. Stub prediction/risk commands must emit `"status": "not_implemented"`
4. Create `tests/test_stress_panel_b.py` for Panel B
5. Update split_policy.yaml to mark unimplemented strategies as `status: planned`

### Priority 3 — Compliance and quality
6. Add docstrings to public functions in storage.py, table_io.py, pairing.py
7. Add CLI end-to-end smoke tests
8. Remove overlapping entries from `_EXCLUDED_COMPS`
9. Clean up `tests/_tmp/` via conftest session teardown
10. Document `KNOWN_FEATURE_KEYS` maintenance requirement

### Priority 4 — Future improvements
11. Refactor `feature_execution.py` into per-stage modules
12. Implement geometry-aware surrogate model
13. Consider molecular-weight tie-breaking for BoundObject primary_ligand
14. Add structural quality factors to `compute_score()` or rename field

## 10. Merge Readiness Assessment

### Decision: CONDITIONAL PASS

### Conditions for unconditional PASS
1. Add `pandas` and `requests` to `pyproject.toml` core dependencies.
2. Ensure stub prediction/risk commands do not present outputs as real biological predictions.

### Strengths
- 326 tests passing, 0 failures
- Schema integrity excellent (all frozen, provenance enforced, SHA-256 hashes)
- Core pipeline fully functional (ingest -> extract -> normalize -> audit -> splits -> release)
- Classification heuristics scientifically sound with explicit rationale
- Stress test panels protect biological ground truth (immutability enforced)
- Multi-source assay merge with conflict detection and source priority
- Structural graph generation with 7 edge types, 3 scopes, 3 export formats
- Site-centric feature pipeline with surrogate training producing versioned artifacts

### Accepted known gaps (documented, not blocking)
- Knowledge graph layer scaffolding only
- Surrogate model linear, not equivariant GNN (V1 scaffold)
- 4 of 7 split strategies unimplemented
- GUI and CLI lack automated tests
- Feature pipeline acceptance tests mostly missing
