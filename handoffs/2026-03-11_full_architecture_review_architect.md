---
task_id: full_architecture_review
role: architect
date: 2026-03-11
allowed_files: []
forbidden_files:
  - stress_test_panel.yaml
  - stress_test_panel_B.yaml
  - stress_test_panel_C.yaml
  - expected_outcomes_table.md
  - expected_outcomes_panel_B.md
  - expected_outcomes_panel_C.md
required_tests:
  - .venv/Scripts/python.exe -m pytest tests/ -q
status: reviewed
---

# Architect Handoff -- Full Repository Architecture Review

## 1. System Overview

Bio-Agent-Lab is a protein-binding ML dataset platform that ingests data from biological databases, normalizes to a canonical schema, audits quality, engineers features, and prepares training-ready datasets. The codebase spans ~23,500 lines of Python across 96 source modules, 135 test files, and 50+ specification/documentation files.

### Architecture layers

| Layer | Spec Section | Key Modules | Status |
|-------|-------------|-------------|--------|
| Data Ingestion | source_requirements.md | `sources/` (rcsb, rcsb_search, bindingdb, skempi, biolip, pdbbind, chembl) | **Mostly implemented** |
| Canonical Schema | canonical_schema.yaml | `schemas/canonical_sample.py`, `schemas/records.py`, `schemas/bound_objects.py` | **Implemented** |
| Multi-table Extraction | STRUCTURE_EXTRACTION_AGENT_SPEC.md | `pipeline/extract.py` | **Implemented** |
| Assay Merge | STRUCTURE_EXTRACTION_AGENT_SPEC.md | `pipeline/assay_merge.py` | **Implemented** |
| Quality & Splits | quality_rules.yaml, split_policy.yaml | `quality/audit.py`, `dataset/splits.py` | **Implemented** |
| Knowledge Graph | bio_agent_full_scope_architecture.md | `graph/builder.py` | **Implemented (no external connectors)** |
| Structural Graphs | SITE_CENTRIC_PHYSICS_SPEC.md | `graph/structural_graphs.py` | **Implemented** |
| Feature Pipeline | FEATURE_PIPELINE_EXECUTION_SPEC.md | `pipeline/feature_execution.py` | **Implemented** |
| Physics Feedback | LOCAL_PHYSICS_SURROGATE_MASTER_SPEC.md | `pipeline/physics_feedback.py` | **Partial (linear, not GNN)** |
| Training Assembly | bio_agent_full_scope_architecture.md | `training/assembler.py` | **Implemented** |
| Dataset Engineering | bio_agent_full_scope_architecture.md | `dataset/engineering.py` | **Implemented** |
| Baseline Model | bio_agent_full_scope_architecture.md | `models/baseline_memory.py` | **Implemented** |
| Prediction Engine | bio_agent_full_scope_architecture.md | `prediction/engine.py` | **Implemented** |
| CLI | coding_standards.md | `cli.py` (30+ commands) | **Implemented** |
| GUI | README.md | `gui.py` (3,300+ lines) | **Implemented** |
| Storage | repo_contract.md | `storage.py` (StorageLayout, 40+ paths) | **Implemented** |

### Data flow

```
External APIs (RCSB, BindingDB, SKEMPI, BioLiP, PDBbind)
    |
    v
sources/*.py  -->  data/raw/{source}/
    |
    v
pipeline/extract.py  -->  data/extracted/{entry,chains,bound_objects,interfaces,assays,provenance}/
    |
    v
pipeline/assay_merge.py  -->  merged assay records
    |
    v
quality/audit.py  -->  data/audit/
    |
    v
dataset/splits.py  -->  data/splits/{train,val,test}.txt
    |
    v
pipeline/feature_execution.py  -->  artifacts/features/
    |
    v
training/assembler.py  -->  TrainingExampleRecords
    |
    v
models/baseline_memory.py  -->  trained model artifacts
    |
    v
prediction/engine.py  -->  screening manifests
```

---

## 2. Architecture Strengths

**S1. Rigorous schema design.** All data models use frozen Pydantic v2 models with field validators. `CanonicalBindingSample` enforces physical bounds (quality_score in [0,1], pH in [0,14], resolution > 0, temperature > -273.15). The six multi-table schemas in `records.py` capture 120+ fields with per-field provenance and confidence tracking.

**S2. Conservative assay merge logic.** `assay_merge.py` implements a principled approach: deterministic pair_identity_key construction by task type, log10-spread conflict detection, source priority ranking, and agreement band computation. This prevents silent data corruption from incompatible measurements.

**S3. Comprehensive bound object classification.** The classification system in `bound_objects.py` distinguishes small molecules, cofactors, metals, glycans, peptides, nucleic acids, additives, and artifacts with controlled vocabulary literals (`BinderType`, `BinderRole`). The stress test panels validate this classification against biological ground truth.

**S4. Provenance preservation throughout.** Every record carries provenance with mandatory `ingested_at` timestamp. The extraction pipeline tracks per-field provenance dictionaries and confidence scores. This satisfies the repo_contract.md requirement for full auditability.

**S5. Cluster-aware dataset splitting.** `splits.py` implements k-mer Jaccard clustering to prevent data leakage between train/val/test sets. The `dataset/engineering.py` module adds ESM-based embeddings, k-means clustering, hard leakage group isolation, and representation-balanced allocation.

**S6. Structure quality integration.** The mmCIF parser in `parsing/mmcif_supplement.py` extracts R-work, R-free, missing residues, and alternate conformers. These feed into `quality/audit.py` for hard exclusions (missing_structure_file, ambiguous_chain_assignment) and soft penalties (low_resolution, missing_interface_residues).

**S7. Dual graph architecture.** The system correctly separates the canonical knowledge graph (`graph/builder.py` -- protein/ligand/complex/pathway nodes with PPI/PLI edges) from the structural ML graph (`graph/structural_graphs.py` -- residue-level and atom-level graphs with covalent, hydrogen bond, salt bridge, hydrophobic, pi-stacking, metal coordination edges). Both export to PyG/DGL/NetworkX.

**S8. Immutable test ground truth.** The stress test panels and expected outcome files are declared read-only in CLAUDE.md and AGENTS.md. This prevents regression by ensuring classification logic is fixed rather than test expectations being weakened.

**S9. Feature pipeline completeness.** `feature_execution.py` implements all 7 stages from FEATURE_PIPELINE_EXECUTION_SPEC.md: site identification, environment extraction, motif classification, archetype clustering, surrogate inference, graph construction, and training example assembly. At ~1,400 lines it is the largest single module but covers the full pipeline.

**S10. Multi-source adapter pattern.** The adapter pattern in `sources/` cleanly separates data acquisition from normalization. Each adapter (RCSB GraphQL, RCSB search, BindingDB REST, SKEMPI CSV, BioLiP flat file, PDBbind index file) produces raw data that flows through a common normalization path.

---

## 3. Architecture Weaknesses

**W1. Monolithic feature pipeline module.** `pipeline/feature_execution.py` at ~1,400 lines combines all 7 pipeline stages in a single file. The FEATURE_PIPELINE_EXECUTION_SPEC.md defines independent stages with separate cache classes and failure isolation, but the implementation couples them. This makes individual stage testing, caching, and resumption harder than necessary.

**W2. Dual directory contract.** The system uses two parallel directory trees: legacy `data/` (raw, processed, extracted, splits, audit) and site-centric `artifacts/` (features, manifests, logs, caches). `StorageLayout` in `storage.py` has 40+ path properties spanning both trees. There is no migration path documented, and some modules write to both directories within a single workflow.

**W3. GUI complexity.** `gui.py` at 3,300+ lines is a single-file Tkinter application. It mixes presentation logic, data fetching, state management, and pipeline orchestration. This makes it fragile to refactor and difficult to test.

**W4. Optional dependency fragility.** Several modules use conditional imports for `gemmi`, `torch`, `pandas`, `sklearn`, and `Bio.PDB` with fallback paths. The fallback paths are inconsistent -- some raise ImportError, some silently degrade, some use pure-Python alternatives. This creates hard-to-reproduce behavior differences across environments.

**W5. Linear surrogate instead of equivariant GNN.** The `physics_feedback.py` module explicitly acknowledges (line 8) that it uses a "deterministic linear model over site environment descriptors plus motif identity, not a full equivariant GNN." The LOCAL_PHYSICS_SURROGATE_MASTER_SPEC.md explicitly forbids lookup-table or geometry-ignoring models and mandates an equivariant GNN.

**W6. Knowledge graph lacks external connectors.** `graph/builder.py` has a `merge_external_sources()` method for STRING, Reactome, and BioGRID integration, but the actual connector modules (`graph/connectors.py`, `graph/identifier_map.py`) are documented stubs per `full_scope_stub_checklist.md`.

**W7. Split policy partial implementation.** `split_policy.yaml` defines 7 split strategies (random, sequence_cluster, time_split, source_split, scaffold_split, family_split, mutation_split). Only sequence_cluster (k-mer Jaccard) and hash-based random are implemented.

---

## 4. Violations of Spec Files

**V1. Physics surrogate architecture (CRITICAL).** LOCAL_PHYSICS_SURROGATE_MASTER_SPEC.md Section 2 mandates: "The surrogate must be an equivariant graph neural network... Lookup-table, kNN-only, or other geometry-ignoring surrogates are explicitly forbidden." The implementation in `physics_feedback.py` uses `torch.linalg.lstsq` / pure-Python least-squares -- a linear model. This is a direct violation of the master spec.
- File: `src/pbdata/pipeline/physics_feedback.py`, lines 305-338

**V2. Missing run modes.** FEATURE_PIPELINE_EXECUTION_SPEC.md defines 4 run modes: full_build, resume, stage_only, inference_prepare. The implementation in `feature_execution.py` does not expose these as discrete modes with the specified behavior (e.g., resume should skip completed stages by checking cache state).

**V3. Missing cache classes.** FEATURE_PIPELINE_EXECUTION_SPEC.md defines 4 cache classes (structure_cache, environment_cache, archetype_cache, surrogate_cache) with specific eviction and versioning rules. The implementation does not implement these as distinct managed caches with the specified policies.

**V4. Missing performance requirements.** FEATURE_PIPELINE_EXECUTION_SPEC.md Section 9 specifies performance targets (e.g., "1000 sites/hour on 8-core CPU for stages 1-4"). No benchmarking or performance validation exists.

**V5. External analysis directory contract.** EXTERNAL_ANALYSIS_EXECUTION_SPEC.md specifies `external_analysis/{orca,apbs,openmm}/{batch_id}/{inputs,outputs,parsed}/` directory structure. `physics_feedback.py` reads from this structure but the StorageLayout does not define these paths, and no validation ensures the directory contract is followed.

**V6. Target vector completeness.** RESULTS_INGEST_AND_FEEDBACK_SPEC.md requires a 5-stage ingest pipeline (raw parse, quality validation, normalization, merge, target table export) with specific outputs (physics_targets.parquet, physics_target_manifest.json, failed_fragments.parquet). The implementation covers the merge and export stages but skips the quality validation stage with its specified statistical tests.

**V7. Split strategies.** split_policy.yaml specifies 7 strategies. Only 2 are implemented (sequence_cluster, random/hash). The remaining 5 (time_split, source_split, scaffold_split, family_split, mutation_split) are missing.

**V8. Acceptance test matrix.** FEATURE_PIPELINE_EXECUTION_SPEC.md Section 10 defines a specific acceptance test matrix. No corresponding test file implements these acceptance tests.

**V9. Repo contract: typing.** coding_standards.md and repo_contract.md require type hints on all public functions. Several modules (particularly gui.py and portions of cli.py) have incomplete type annotations.

---

## 5. Stubbed or Incomplete Systems

| System | File(s) | Status | Notes |
|--------|---------|--------|-------|
| ChEMBL adapter | `sources/chembl.py` | **Raises NotImplementedError** | Only module with explicit NotImplementedError |
| Graph connectors | `graph/connectors.py` | **Documented stub** | STRING, Reactome, BioGRID connectors |
| Identifier map | `graph/identifier_map.py` | **Documented stub** | Cross-database identifier resolution |
| Pathway features | `features/pathway.py` | **Documented stub** | Per full_scope_stub_checklist.md |
| MM features | `features/mm_features.py` | **Documented stub** | Molecular mechanics feature extraction |
| Equivariant GNN surrogate | `pipeline/physics_feedback.py` | **Linear placeholder** | Mandated GNN not implemented |
| Time-based splits | `dataset/splits.py` | **Missing** | Specified in split_policy.yaml |
| Source-based splits | `dataset/splits.py` | **Missing** | Specified in split_policy.yaml |
| Scaffold splits | `dataset/splits.py` | **Missing** | Specified in split_policy.yaml |
| Family splits | `dataset/splits.py` | **Missing** | Specified in split_policy.yaml |
| Mutation splits | `dataset/splits.py` | **Missing** | Specified in split_policy.yaml |
| Feature pipeline run modes | `pipeline/feature_execution.py` | **Missing** | resume/stage_only/inference_prepare |

---

## 6. Structural Risks

**R1. Single-file GUI brittleness.** At 3,300+ lines, `gui.py` is the single largest module. Any change to pipeline logic, storage paths, or schema fields risks breaking the GUI. There are no GUI-specific tests. A Tkinter crash in production would block the entire user workflow.

**R2. StorageLayout path explosion.** `storage.py` has 40+ path properties and growing. New features add new paths. There is no validation that the directory tree is consistent, no cleanup for orphaned directories, and no migration mechanism when paths change.

**R3. Feature pipeline single-point-of-failure.** `feature_execution.py` runs all 7 stages sequentially in-process. A failure at stage 5 (surrogate inference) loses all work from stages 1-4 because the cache isolation specified in the spec is not implemented.

**R4. Optional dependency matrix.** The codebase depends on `gemmi`, `torch`, `pandas`, `sklearn`, `Bio.PDB`, `networkx`, and optionally `torch_geometric` / `dgl`. Not all combinations of present/absent optional dependencies are tested. A user installing without `gemmi` would hit failures deep in the pipeline rather than at startup.

**R5. Concurrent access unsafety.** Multiple CLI commands or GUI actions can write to the same `data/` and `artifacts/` directories simultaneously. There is no file locking, atomic write, or transaction mechanism.

**R6. Spec drift.** The specifications (50+ files) describe a more complete system than exists. Without a machine-readable spec-to-implementation mapping, new developers cannot easily determine which spec sections are implemented. The `full_scope_stub_checklist.md` helps but is manually maintained and may drift.

---

## 7. Required Refactors

**RF1. Split feature_execution.py into per-stage modules.**
- Create `pipeline/stages/{site_id.py, environment.py, motif.py, archetype.py, surrogate.py, graph_build.py, assembly.py}`
- Each stage gets its own cache class, entry point, and test file
- Add a `pipeline/stages/orchestrator.py` that implements the 4 run modes
- Priority: HIGH -- blocks cache isolation, resume, and stage-only execution

**RF2. Implement equivariant GNN surrogate.**
- Replace the linear least-squares model in `physics_feedback.py` with an equivariant GNN per LOCAL_PHYSICS_SURROGATE_MASTER_SPEC.md
- The linear model can remain as a fast fallback but must not be the default
- Requires: torch_geometric or e3nn dependency, training data from external analysis results
- Priority: HIGH -- spec violation

**RF3. Unify directory contracts.**
- Decide on a single directory tree (recommend `artifacts/` for all generated outputs, `data/raw/` for downloads only)
- Migrate StorageLayout properties to use the unified tree
- Add a one-time migration script for existing installations
- Priority: MEDIUM -- prevents confusion, reduces path count

**RF4. Extract GUI into MVC layers.**
- Separate `gui.py` into: `gui/views.py` (Tkinter widgets), `gui/controllers.py` (event handlers), `gui/state.py` (application state)
- This enables testing controllers without Tkinter and swapping to a web UI later
- Priority: MEDIUM -- improves maintainability

**RF5. Implement managed cache classes.**
- Create `pipeline/caches.py` with StructureCache, EnvironmentCache, ArchetypeCache, SurrogateCache
- Each cache implements the eviction policy and version key from FEATURE_PIPELINE_EXECUTION_SPEC.md
- Priority: MEDIUM -- required for resume mode and stage isolation

**RF6. Add dependency presence validation at startup.**
- Create `src/pbdata/deps.py` that checks for all optional dependencies at import time
- Produce a clear report of available vs missing capabilities
- Gate pipeline stages on their required dependencies rather than failing mid-execution
- Priority: LOW -- quality of life improvement

---

## 8. Recommended Architecture Improvements

**AI1. Add a spec compliance matrix.** Create a machine-readable YAML file mapping each spec section to its implementing module, test file, and status (implemented/partial/missing). This eliminates the manual tracking problem in R6 and provides a dashboard for development progress.

**AI2. Implement pipeline checkpointing.** Beyond cache classes (RF5), add explicit checkpoint files after each pipeline stage. On resume, the orchestrator reads the last successful checkpoint and restarts from the next stage. This is specified in FEATURE_PIPELINE_EXECUTION_SPEC.md but not implemented.

**AI3. Add integration test harness for physics feedback loop.** The external analysis workflow (ORCA/APBS/OpenMM) is designed for offline execution. Create a test harness with synthetic physics results that exercises the full ingest-merge-train-evaluate cycle without requiring actual QM calculations.

**AI4. Implement remaining split strategies.** The 5 missing split strategies (time, source, scaffold, family, mutation) are all specified with clear semantics in split_policy.yaml. Each is straightforward to implement given the existing cluster_aware_split infrastructure. Scaffold and family splits would significantly improve model evaluation rigor.

**AI5. Add CLI command validation.** The CLI has 30+ commands. Add a test that imports all CLI commands and verifies their type signatures match expectations. This catches import errors and missing dependencies early.

**AI6. Create a minimal end-to-end integration test.** A single test that runs: ingest (1 PDB entry) -> extract -> audit -> split -> feature pipeline -> assemble training example -> predict. This validates the full data flow without requiring large datasets.

---

## 9. Missing Systems Needed for Final Vision

| System | Spec Reference | Priority | Effort |
|--------|---------------|----------|--------|
| Equivariant GNN surrogate | LOCAL_PHYSICS_SURROGATE_MASTER_SPEC.md | **Critical** | Large |
| Graph external connectors (STRING, Reactome, BioGRID) | bio_agent_full_scope_architecture.md | High | Medium |
| Identifier cross-mapping service | full_scope_stub_checklist.md | High | Medium |
| Pathway feature extraction | FEATURE_PIPELINE_EXECUTION_SPEC.md | Medium | Medium |
| Molecular mechanics features | SITE_CENTRIC_PHYSICS_SPEC.md | Medium | Medium |
| ChEMBL adapter | source_requirements.md | Medium | Small |
| Pipeline run modes (resume, stage_only, inference_prepare) | FEATURE_PIPELINE_EXECUTION_SPEC.md | High | Medium |
| Managed cache classes | FEATURE_PIPELINE_EXECUTION_SPEC.md | High | Medium |
| Remaining 5 split strategies | split_policy.yaml | Medium | Small each |
| Acceptance test matrix | FEATURE_PIPELINE_EXECUTION_SPEC.md | Medium | Medium |
| Performance benchmarking | FEATURE_PIPELINE_EXECUTION_SPEC.md | Low | Small |

---

## 10. Implementation Priorities

### Phase 1: Spec Compliance (1-2 weeks)
1. Split `feature_execution.py` into per-stage modules (RF1)
2. Implement managed cache classes (RF5)
3. Add pipeline run modes (resume, stage_only)
4. Implement feature pipeline checkpointing (AI2)

### Phase 2: Physics Subsystem (2-4 weeks)
1. Design and implement equivariant GNN surrogate (RF2)
2. Create integration test harness for physics feedback loop (AI3)
3. Implement quality validation stage in physics ingest
4. Add external analysis directory validation to StorageLayout

### Phase 3: Completeness (2-3 weeks)
1. Implement remaining 5 split strategies (AI4)
2. Implement graph external connectors (STRING, Reactome, BioGRID)
3. Implement identifier cross-mapping service
4. Complete ChEMBL adapter
5. Add pathway and MM feature extraction

### Phase 4: Polish (1-2 weeks)
1. Unify directory contracts (RF3)
2. Extract GUI into MVC layers (RF4)
3. Add dependency validation (RF6)
4. Create spec compliance matrix (AI1)
5. Add performance benchmarking
6. Implement acceptance test matrix

---

## 11. Final Architecture Health Assessment

**Grade: B- (Functional but specification-divergent)**

### What works well
The core data pipeline -- from ingestion through extraction, quality audit, splitting, and training assembly -- is solid. The schema design is rigorous, provenance is tracked throughout, and the stress test framework with immutable ground truth is an excellent quality gate. The multi-source adapter pattern is clean and extensible. The assay merge logic is conservatively correct. The structural graph builder produces production-quality molecular graphs.

### What needs attention
The physics enrichment subsystem is the primary gap. The spec mandates an equivariant GNN but the implementation uses a linear model. This is acknowledged in the code comments but remains a blocking spec violation. The feature pipeline's monolithic structure prevents the cache isolation, run modes, and failure recovery that the spec requires.

### What is at risk
The dual directory contract (`data/` vs `artifacts/`) creates confusion about where outputs live. The 3,300-line GUI is a maintenance liability. The growing StorageLayout (40+ paths) needs rationalization. The 5 missing split strategies limit evaluation rigor for different model architectures.

### Summary
The repository demonstrates strong fundamentals in data engineering and biological domain modeling. The primary architectural debt is concentrated in the physics subsystem (linear vs GNN) and pipeline orchestration (monolithic vs staged). Phase 1 and Phase 2 of the priority plan would bring the system to spec compliance. The remaining phases address completeness and polish.
