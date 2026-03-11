---
task_id: full_repo_architecture_review
role: architect
date: 2026-03-10
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

# Architect Handoff — Full Repository Architecture Review

## 1. System Overview

Bio-Agent-Lab is a multi-modal biological interaction prediction platform. The codebase spans ~18,000+ lines of Python across 49 source modules, 35 test files, 50+ specification documents, and 3 handoff artifacts.

### Architecture layers implemented

| Layer | Spec Section | Modules | Status |
|-------|-------------|---------|--------|
| Data Ingestion | §3 | `sources/` (7 adapters), `pipeline/enrichment.py` | **Implemented** |
| Canonical Dataset | §5 | `schemas/`, `pipeline/extract.py`, `pipeline/assay_merge.py` | **Implemented** |
| Quality & Splits | §14 | `quality/audit.py`, `dataset/splits.py` | **Implemented** |
| Feature Engineering | §7 | `features/` (5 modules), `pipeline/feature_execution.py` | **Partially implemented** |
| Interaction Graph | §7 (graph) | `graph/` (4 modules) | **Partially implemented** |
| Conformational States | §8 | `dataset/conformations.py`, `schemas/conformational_state.py` | **Scaffold** |
| Prediction Engine | §9 | `prediction/engine.py` + 3 stubs | **Scaffold with baseline** |
| Off-Target Analysis | §10 | `models/off_target_models.py` | **Stub** |
| Pathway Reasoning | §11 | `risk/pathway_reasoning.py` | **Stub** |
| Risk Scoring | §12 | `risk/summary.py`, `risk/severity_scoring.py` | **Scaffold with placeholders** |
| QA System | §14 | `qa/scenario_runner.py`, stress panels, 35 test files | **Implemented** |
| Bias Audit | §16 | `reports/bias.py` | **Implemented** |
| Physics Subsystem | SITE_CENTRIC_PHYSICS_SPEC | `pipeline/physics_feedback.py`, `pipeline/feature_execution.py` | **Implemented (awaiting labels)** |
| GUI | CLAUDE.md | `gui.py` (2,837 lines) | **Implemented (Tkinter)** |
| CLI | — | `cli.py` (2,022 lines, 23+ commands) | **Implemented** |

### Codebase metrics

- **Source modules:** 49 files, ~18,000 lines
- **Test files:** 35 files, 297+ passing unit tests, 71 deselected integration tests
- **Schemas:** 8 frozen Pydantic v2 models
- **CLI commands:** 23+
- **Data source adapters:** 7 (RCSB, BindingDB, ChEMBL, SKEMPI, BioLiP, PDBbind, AlphaFold stub)
- **Spec documents:** 50+ files across 7 spec packs

---

## 2. Architecture Strengths

### S1 — Canonical schema is well-defined and enforced
`CanonicalBindingSample` (148 lines, frozen, 5 validators) and the 6 multi-table records in `records.py` (381 lines) accurately implement `specs/canonical_schema.yaml` and `STRUCTURE_EXTRACTION_AGENT_SPEC.md`. Provenance is mandatory (`ingested_at` required by validator). All records are frozen Pydantic v2 models.

### S2 — Multi-table extraction pipeline is production-grade
`pipeline/extract.py` (557 lines) produces 6 linked output tables (entry, chain, bound_object, interface, assay, provenance) with 73+ fields on EntryRecord alone. Entity classification in `rcsb_classify.py` (870+ lines) correctly distinguishes proteins, peptides, small molecules, cofactors, metals, glycans, additives, and nucleic acids. Covalent warhead detection uses SMILES reactive-group patterns.

### S3 — Assay merge with conflict detection is correct
`pipeline/assay_merge.py` (302 lines) implements source priority ranking (SKEMPI > PDBbind > BioLiP > BindingDB > ChEMBL), conflict detection via log₁₀ spread analysis, and per-field provenance/confidence tracking. It never silently collapses distinct assay types.

### S4 — Feature pipeline follows the FEATURE_PIPELINE_EXECUTION_SPEC
`pipeline/feature_execution.py` (861 lines) implements all 7 spec-required stages: canonical input resolution → structure preparation → base feature extraction → site physics enrichment → graph construction → training example assembly → validation/reporting. It produces stage manifests, structured error logs, coverage reports, and supports degraded mode.

### S5 — Physics subsystem architecture is spec-compliant
`pipeline/physics_feedback.py` (371 lines) implements all 12 target columns from LOCAL_PHYSICS_SURROGATE_MASTER_SPEC, supports ORCA/APBS/OpenMM result parsing, quality validation, normalized target table export, and linear surrogate training. The system correctly separates offline analysis from in-project inference.

### S6 — Storage layout is comprehensive and immutable
`storage.py` (343 lines) provides a frozen dataclass with 60+ path properties covering all data layers, artifact directories, and workspace subdirectories. File validators exist for mmCIF, PDB, RCSB JSON, SKEMPI CSV, and BindingDB JSON.

### S7 — Test infrastructure is strong
297+ passing unit tests across 35 files. Integration tests are properly isolated behind `pytest -m integration`. Stress test panels (A, B, C) define immutable biological ground truth. `conftest.py` isolates tests from live config modifications.

### S8 — Training example schema is spec-aligned
`training_example.py` (106 lines) decomposes into 6 field-group sub-models (Structure, Protein, Ligand, Interaction, Experiment, GraphFeature) matching `bio_agent_full_spec/specs/TRAINING_EXAMPLE_SCHEMA.yaml`.

### S9 — Explicit uncertainty throughout
Risk scores are flagged `risk_score_is_placeholder: True`. Prediction status says `scaffold_only_no_predictions`. MM jobs are marked `planned_not_executed`. Physics surrogates degrade explicitly. This follows the master spec's final principle: "explicit uncertainty over incorrect certainty."

---

## 3. Architecture Weaknesses

### W1 — CLI embeds too much pipeline orchestration logic
`cli.py` (2,022 lines) directly constructs source adapters, calls enrichment functions, and manages pipeline state. The `ingest` command alone contains ~220 lines of extraction/enrichment logic that should live in the pipeline layer. This violates the master spec's rule: "Agents must never mix responsibilities across layers."

### W2 — GUI uses Tkinter but spec requires PySide6
`CLAUDE.md` declares "Tkinter GUI" and the current implementation (2,837 lines) is Tkinter. However, `protein_ml_data_lab_agent_instruction_pack/specs/GUI_ARCHITECTURE.md` explicitly requires "PySide6 (Qt)" with a sidebar workflow layout, eventFilter-based scrolling, and Qt signal/slot threading. This is a fundamental framework mismatch.

### W3 — Two competing workflow models
The current codebase has a 12-stage pipeline model (Data Acquisition → Processing → Quality & Analysis → ML Pipeline) in the GUI, while the protein_ml_data_lab spec defines a 7-step sidebar workflow (Workspace → Protein Search → Metadata → Download → Features → Graph → Dataset). These are architecturally incompatible.

### W4 — Feature outputs go to `data/features/` not `features/` or `artifacts/`
FEATURE_PIPELINE_EXECUTION_SPEC §3 requires `artifacts/` and `features/` as top-level directories. The codebase stores everything under `data/features/`, `data/graph/`, etc. Neither `artifacts/` nor `features/` (top-level) exist. This violates the directory contract.

### W5 — No orchestrator module
FEATURE_PIPELINE_EXECUTION_SPEC §18 recommends `pipeline/orchestrator.py` for stage dependency management, resumable runs, and shard-aware execution. This module does not exist. The orchestration logic is embedded in `feature_execution.py` and `cli.py`.

### W6 — Graph schema lacks per-node provenance
Master spec §6 requires "Every extracted field must include provenance." `GraphNodeRecord` has a `provenance` dict, but `GraphEdgeRecord` has `provenance` as a generic dict without structured `retrieved_at`, `confidence`, or `source_record_key` fields. This was flagged in the previous QA review (M3) and remains unfixed.

---

## 4. Violations of Spec Files

### V1 — FEATURE_PIPELINE_EXECUTION_SPEC directory contract (Critical)
**Spec:** §3 requires `artifacts/canonical/`, `artifacts/prepared_structures/`, `artifacts/base_features/`, `artifacts/site_physics/`, `artifacts/graphs/`, `artifacts/training_examples/`, `artifacts/caches/`, `artifacts/logs/`, `artifacts/manifests/`, and `features/structural_features/`, `features/ligand_features/`, `features/interface_features/`, `features/graph_features/`.
**Actual:** Neither `artifacts/` nor top-level `features/` directories exist. All outputs go to `data/`.
**Impact:** Pipeline artifacts are mixed with raw/processed data. No version-isolated artifact tree exists.

### V2 — FEATURE_PIPELINE_EXECUTION_SPEC run modes (Major)
**Spec:** §5 requires exactly 4 run modes: `full_build`, `resume`, `stage_only`, `inference_prepare`.
**Actual:** `FeaturePipelineConfig` has `run_mode` and `stage_only` fields and supports full_build/resume/stage_only. The `inference_prepare` mode is not explicitly implemented as a distinct mode.

### V3 — FEATURE_PIPELINE_EXECUTION_SPEC caching (Major)
**Spec:** §9 requires mandatory caching with keys including `schema_version`, `feature_pipeline_version`, `motif_taxonomy_version`, `surrogate_checkpoint_ID`, `input_structure_hash`, and `state_label`. Each run must emit `artifacts/caches/<run_id>_cache_manifest.json`.
**Actual:** No formal cache manifest is generated. Caching exists only for structure file reuse and API responses, not for feature computation.

### V4 — FEATURE_PIPELINE_EXECUTION_SPEC versioning (Major)
**Spec:** §10 requires recording `schema_version`, `feature_pipeline_version`, `site_physics_spec_version`, `surrogate_model_version`, `graph_representation_version`, `training_example_version` in every run.
**Actual:** `PIPELINE_VERSION = "site_feature_pipeline_v1"` exists but the full version matrix is not recorded in manifests.

### V5 — MASTER_ENGINEERING_INSTRUCTIONS §2 layer boundaries (Major)
**Spec:** Requires strict directory-based layer separation: `data_pipeline/`, `dataset/`, `features/`, `graph/`, `models/`, `prediction/`, `risk/`, `qa/`.
**Actual:** Most modules exist under the correct packages, but `pipeline/` contains both extraction (`extract.py`) and feature execution (`feature_execution.py`) in the same directory. The spec requires `data_pipeline/extraction/` separate from `features/`. The `data_pipeline/` package exists but is only a compatibility wrapper.

### V6 — MASTER_ENGINEERING_INSTRUCTIONS §3 interaction network sources (Major)
**Spec:** Lists ~17 interaction network databases (STRING, BioGRID, IntAct, Reactome, KEGG, Pathway Commons, WikiPathways, DIP, MINT, HPRD, SIGNOR, OmniPath, CORUM, PhosphoSitePlus, Human Protein Atlas, GTEx, OpenTargets, DisGeNET).
**Actual:** Only 3 connectors implemented (STRING, Reactome, BioGRID). 14 sources are missing.

### V7 — SITE_CENTRIC_PHYSICS_SPEC archetype generation (Major)
**Spec:** §7 requires motif class clustering producing 20–60 archetypes per motif. §8 requires physics calculations on those archetypes.
**Actual:** Archetype clustering is not implemented. The feature pipeline has `export_analysis_queue()` but no `cluster_archetypes()` or similar function. The queue export generates fragment lists but does not cluster them into representative archetypes.

### V8 — protein_ml_data_lab GUI_ARCHITECTURE (Architectural conflict)
**Spec:** Requires PySide6 (Qt), sidebar workflow with 7 steps, eventFilter scroll behavior, Qt signal/slot threading.
**Actual:** Tkinter with tabbed notebook layout, subprocess-based threading, and a different 12-stage pipeline model. This is an unresolved architectural conflict between two spec packs.

### V9 — STRUCTURE_EXTRACTION_AGENT_SPEC output format (Minor)
**Spec:** Requires output tables as `.parquet` files (`entry_records.parquet`, `chain_records.parquet`, etc.).
**Actual:** All extraction outputs are JSON files under `data/extracted/entry/*.json`, etc. No Parquet support for extraction outputs.

### V10 — Repo contract: "All major functions must have tests" (Minor)
Several newer modules lack dedicated test coverage: `enrichment.py`, `workflow_engine.py` (has test), `engineering.py`, `structural_graphs.py`, `scenario_runner.py`. Total test count (297) is strong but some modules are untested.

---

## 5. Stubbed or Incomplete Systems

### Complete stubs (14-line dataclass + plan function)
| Module | Purpose | Status |
|--------|---------|--------|
| `prediction/ligand_screening.py` | Ligand screening logic | Stub; real work in `engine.py` |
| `prediction/peptide_binding.py` | Peptide binding logic | Stub; real work in `engine.py` |
| `prediction/variant_effects.py` | Mutation variant prediction | Stub; not implemented |
| `risk/pathway_reasoning.py` | Pathway overlap reasoning | Stub; not implemented |
| `risk/severity_scoring.py` | Risk severity assignment | Stub; not implemented |
| `models/off_target_models.py` | Off-target ranking | Stub; not implemented |
| `sources/alphafold.py` | AlphaFold structure prediction | Stub; planning only |

### Functional scaffolds (produce manifests but no real computation)
| Module | Lines | What it does | What it lacks |
|--------|-------|-------------|---------------|
| `risk/summary.py` | 119 | Writes risk manifest with placeholder weights | Real pathway overlap, tissue expression, toxicity history |
| `features/pathway.py` | 68 | Counts pathway nodes from graph | Real pathway feature computation |
| `dataset/conformations.py` | 95 | Catalogs experimental states | AlphaFold/Rosetta predicted states |

### Implemented but awaiting external data
| Module | Lines | Blocker |
|--------|-------|---------|
| `pipeline/physics_feedback.py` | 371 | No offline ORCA/APBS/OpenMM results yet |
| `pipeline/feature_execution.py` (stage 4) | — | No surrogate checkpoint; runs in degraded mode |
| `graph/connectors.py` (14 of 17 sources) | — | Only STRING/Reactome/BioGRID; 14 more needed |

---

## 6. Structural Risks

### R1 — CLI as orchestrator creates testing brittleness (High)
With 2,022 lines and 23 commands, `cli.py` is the largest file. It mixes configuration loading, adapter instantiation, pipeline orchestration, and output formatting. Bugs in one command can cascade. Refactoring CLI commands into thin wrappers over pipeline functions would improve testability and maintainability.

### R2 — Two GUI specifications create permanent ambiguity (High)
`CLAUDE.md` mandates Tkinter. `protein_ml_data_lab_agent_instruction_pack` mandates PySide6. Both cannot be authoritative. This must be resolved before any further GUI work, or agents will produce conflicting implementations.

### R3 — No artifact isolation creates version contamination risk (Medium)
Without the `artifacts/` directory tree, feature pipeline outputs are mixed with raw/processed data. A re-run overwrites previous outputs. The FEATURE_PIPELINE_EXECUTION_SPEC's run-ID-based artifact isolation is not implemented.

### R4 — Surrogate model has no training data (Medium)
The linear surrogate in `physics_feedback.py` is fully implemented but has never been trained because no ORCA/APBS/OpenMM results exist. The entire site-centric physics enrichment layer operates in permanent degraded mode. This is architecturally correct but scientifically incomplete.

### R5 — Feature key validation is tightly coupled (Low)
`schemas/features.py` validates against a hardcoded `KNOWN_FEATURE_KEYS` set of 68 keys. Adding any new feature requires editing this set. A feature registry pattern would be more extensible.

### R6 — Optional dependency fragility (Low)
`torch`, `pyarrow`, and `gemmi` are used across multiple modules. When missing, tests fail at collection time (6 failures seen). The lazy-import pattern is applied inconsistently.

---

## 7. Required Refactors

### RF1 — Extract pipeline logic from CLI (Priority: High)
Move the ~220 lines of ingest/normalize orchestration from `cli.py` into dedicated pipeline modules. CLI commands should be thin wrappers: parse args → call pipeline function → format output.

**Scope:**
- Create `pipeline/ingest.py` with `run_ingest(config, criteria, layout)` → returns IngestResult
- Create `pipeline/normalize.py` with `run_normalize(config, layout)` → returns NormalizeResult
- Slim `cli.py` ingest/normalize commands to <30 lines each

### RF2 — Implement `artifacts/` directory tree (Priority: High)
Add the FEATURE_PIPELINE_EXECUTION_SPEC §3 directory contract. `StorageLayout` already has `artifacts_*` properties — wire them into the feature pipeline.

**Scope:**
- Create `artifacts/` top-level directory
- Update `feature_execution.py` to write stage outputs to `artifacts/<stage>/<run_id>/`
- Generate `artifacts/manifests/<run_id>_*` per spec

### RF3 — Add `inference_prepare` run mode (Priority: Medium)
Implement the 4th required run mode that skips archetype clustering and offline physics, using only cached surrogates for fast user-facing prediction.

### RF4 — Implement feature computation caching (Priority: Medium)
Add cache key generation (schema_version + pipeline_version + input_hash) and manifest export per FEATURE_PIPELINE_EXECUTION_SPEC §9.

---

## 8. Recommended Architecture Improvements

### AI1 — Resolve GUI framework conflict
**Decision required:** Tkinter (current, working, 2,837 lines) vs PySide6 (spec-required, not implemented).
**Recommendation:** Keep Tkinter as the production GUI. Reclassify `protein_ml_data_lab_agent_instruction_pack` as a future/alternative spec, not the active implementation target. Update `CLAUDE.md` to make this explicit.

### AI2 — Add pipeline orchestrator module
Create `pipeline/orchestrator.py` implementing:
- Stage dependency DAG
- Run state persistence (JSON manifests)
- Resume from last completed stage
- Parallel per-record execution with worker pool
This replaces the embedded orchestration in `feature_execution.py` and `cli.py`.

### AI3 — Implement archetype clustering
Add `pipeline/cluster_archetypes.py` implementing SITE_CENTRIC_PHYSICS_SPEC §7:
- Collect environment vectors by motif class
- K-means or DBSCAN clustering
- Select 20–60 representatives per motif
- Export fragment files for offline analysis

### AI4 — Add Parquet output support
The FEATURE_PIPELINE_EXECUTION_SPEC and STRUCTURE_EXTRACTION_AGENT_SPEC both require Parquet output. Add `pandas` serialization for extraction and feature tables alongside JSON.

### AI5 — Expand graph connectors incrementally
Prioritize connectors by data value: IntAct (high-quality PPIs) → KEGG (pathway) → Pathway Commons (integrated pathways) → OmniPath (aggregated) before others.

### AI6 — Add structured provenance to graph edges
Expand `GraphEdgeRecord.provenance` to include `retrieved_at`, `confidence`, `source_record_key`, `extraction_method` matching the pattern used in all other record types.

---

## 9. Missing Systems Needed for Final Vision

| System | Spec Source | Effort | Dependencies |
|--------|-----------|--------|-------------|
| **Real prediction models** | MASTER §9 | Large | Training data, features, graph |
| **Off-target screening** | MASTER §10 | Large | Trained affinity model |
| **Pathway reasoning** (real) | MASTER §11 | Medium | 4+ pathway database connectors |
| **Risk scoring** (real) | MASTER §12 | Medium | Binding model + pathway reasoning |
| **AlphaFold structure prediction** | MASTER §8 | Medium | AlphaFold API or local install |
| **Archetype clustering** | SITE_CENTRIC §7 | Medium | Environment vector extraction |
| **Offline QM/MM analysis** | EXTERNAL_ANALYSIS_SPEC | Large | ORCA/APBS/OpenMM installations |
| **Surrogate model training** | LOCAL_PHYSICS_SURROGATE §6 | Large | Archetype physics labels |
| **ESM protein embeddings** | MASTER §7 | Small | ESM model weights |
| **RDKit ligand descriptors** | EXTRACTION_SPEC §9 | Small | RDKit installation |
| **14 additional graph connectors** | MASTER §3 | Large | Per-source API integration |
| **Parquet export pipeline** | FEATURE_PIPELINE §3 | Small | pyarrow |
| **Interface residue extraction from coordinates** | EXTRACTION_SPEC §7 | Medium | gemmi coordinate analysis |
| **BinaryCIF fallback** | MASTER §4 | Small | gemmi BinaryCIF reader |

---

## 10. Implementation Priorities

### Tier 1 — Immediate (blocks correctness)
1. **RF1:** Extract pipeline logic from CLI into dedicated modules
2. **AI1:** Resolve GUI framework conflict (decision, not code)
3. **RF2:** Implement `artifacts/` directory tree for feature pipeline

### Tier 2 — Near-term (blocks feature pipeline completeness)
4. **AI3:** Implement archetype clustering for physics subsystem
5. **RF3:** Add `inference_prepare` run mode
6. **RF4:** Implement feature computation caching with version-keyed manifests
7. **AI6:** Add structured provenance to graph edges

### Tier 3 — Medium-term (blocks prediction capability)
8. **AI5:** Expand graph connectors (IntAct, KEGG, Pathway Commons, OmniPath)
9. Add RDKit ligand descriptor computation
10. Add ESM protein embedding integration
11. Implement interface residue extraction from mmCIF coordinates

### Tier 4 — Long-term (full vision)
12. Run offline ORCA/APBS/OpenMM analysis (human execution playbook)
13. Train real surrogate model from physics labels
14. Train real binding affinity prediction model
15. Implement real pathway reasoning and risk scoring
16. Implement off-target screening with trained model

---

## 11. Final Architecture Health Assessment

### Overall grade: **B+**

**Layers 1–3 (Ingestion → Canonical → Quality):** **A** — Production-grade. Multi-table extraction, 6 source adapters, assay merge with conflict detection, quality audit with 22+ flags, sequence-clustering splits. This is the strongest part of the codebase.

**Layer 4 (Features):** **B** — Feature builder, microstate assignment, physics features, and MM job manifests are real implementations. The feature pipeline executor follows the 7-stage spec. Missing: caching, archetype clustering, Parquet output.

**Layer 5 (Graph):** **B-** — Graph builder works with 3 external connectors. Structural graph generation supports residue/atom level with PyG/DGL/NetworkX export. Missing: 14 connectors, structured edge provenance, pathway nodes by default.

**Layer 6 (Prediction):** **C** — Baseline memory model exists and works. Prediction engine produces manifests. But actual ML models are not trained; ligand screening, peptide binding, and variant effects are stubs.

**Layer 7 (Risk/Pathway):** **D** — Only placeholder risk scoring exists. Pathway reasoning and severity scoring are 14-line stubs. These layers cannot produce real predictions.

**Infrastructure (CLI, GUI, Storage, Schemas, Tests):** **A-** — 23 CLI commands, 2,837-line GUI, comprehensive storage layout, 8 frozen schemas, 297+ tests. The GUI framework conflict with the PySide6 spec is the main concern.

**Physics Subsystem:** **B** — Architecture is complete and spec-compliant. Training code, inference code, and ingest pipeline are all implemented. Missing: actual offline analysis results to train the surrogate.

### Summary

The platform has a strong foundation in data ingestion, schema design, and extraction. The feature pipeline architecture is well-designed but needs caching and archetype clustering. The prediction and risk layers are scaffolds awaiting trained models. The largest architectural risk is the unresolved GUI framework conflict and the CLI-as-orchestrator pattern. The physics subsystem is architecturally complete but operationally blocked on external analysis results.

The system correctly follows the master spec's final principle: it prefers explicit uncertainty over incorrect certainty at every layer.
