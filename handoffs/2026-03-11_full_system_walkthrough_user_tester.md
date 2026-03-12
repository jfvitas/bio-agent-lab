---
task_id: full_system_walkthrough
role: user_tester
date: 2026-03-11
status: reviewed
---

# User Workflow Report — Full System Walkthrough

## 1. User Goals Tested

Eight primary workflows were traced through the codebase by reading all relevant source files:

1. **Fresh install and dataset building** — new user installs, configures, and ingests data from RCSB PDB
2. **Extraction** — CLI `extract` command to produce multi-table records from raw data
3. **Report generation** — CLI `report` command for summary statistics over processed records
4. **Split building** — CLI `build-splits` command for train/val/test partitions
5. **Full end-to-end pipeline** — GUI "Run Full Pipeline" button executing all stages sequentially
6. **Output inspection** — what files are produced, their formats, and where they land
7. **Prediction workflows** — `predict-ligand-screening`, `predict-peptide-binding`, `score-pathway-risk`
8. **GUI usage** — Tkinter desktop application for all pipeline stages including source configuration, search criteria editing, curation review, and one-click pipeline execution

## 2. Steps Performed

All analysis was performed by reading source code only. No external API calls were made. Key files traced:

- `src/pbdata/cli.py` — 1800+ lines, approximately 30 Typer commands
- `src/pbdata/gui.py` — 3144 lines, full Tkinter GUI with tabbed panels, subprocess pipeline execution, and curation review dashboards
- `src/pbdata/pipeline/extract.py` — multi-table extraction from raw RCSB GraphQL metadata plus mmCIF parsing
- `src/pbdata/dataset/splits.py` — three split strategies: hash, cluster-aware (k-mer Jaccard), pair-aware grouped
- `src/pbdata/dataset/engineering.py` — diversity-optimized dataset engineering with optional ESM embeddings
- `src/pbdata/training/assembler.py` — joins extracted records, assays, graph features, and feature-layer outputs into TrainingExampleRecord objects
- `src/pbdata/features/builder.py` — materializes features from extracted tables and canonical graph
- `src/pbdata/graph/builder.py` — builds canonical biological graph from extracted structure tables with optional STRING/Reactome/BioGRID enrichment
- `src/pbdata/graph/structural_graphs.py` — residue-level and atom-level structural graphs for ML, with PyG/DGL/NetworkX export
- `src/pbdata/data_pipeline/workflow_engine.py` — workspace initialization and unified metadata harvesting
- `src/pbdata/storage.py` — StorageLayout dataclass defining 50+ directory paths under a single root
- `configs/criteria.yaml` — default RCSB search criteria
- `README.md` — 14-section GUI-focused user guide

## 3. Observed Outputs

### Workflow 1: Fresh Install
- `pip install -e ".[dev]"` installs the package. `pbdata-gui` or `pbdata gui` launches the desktop GUI.
- Default criteria in `configs/criteria.yaml` targets protein-protein complexes from X-ray and EM at resolution 3.0 angstroms or better.
- If `configs/sources.yaml` does not exist, the CLI logs a warning and proceeds with `AppConfig()` defaults. Acceptable behavior.

### Workflow 2: Extraction
- `pbdata extract` reads raw JSON files from `data/raw/rcsb/`, fetches chem-comp descriptors, downloads mmCIF structure files, runs classification, parses structure quality, and writes six JSON table directories to `data/extracted/`.
- Supports `--workers` for thread-pool parallelism, `--no-download-structures`, `--download-pdb`, `--structures`.
- Each PDB entry produces files at: `data/extracted/{entry,chains,bound_objects,interfaces,assays,provenance}/<PDB_ID>.json`.
- Master CSV exports auto-refreshed after extraction.
- Validation function checks all six table files exist before skipping.

### Workflow 3: Report Generation
- `pbdata report` delegates to `run_processed_report()` and prints stats.
- **Critical code issue**: bare `return` at line 580 makes ~70 lines unreachable (lines 582-653). Dead block contains undefined variables that would crash with `NameError` if reached.

### Workflow 4: Build Splits
- `pbdata build-splits` supports four modes: `auto`, `pair-aware`, `legacy-sequence`, `hash`.
- In `auto` mode, checks for pair-level items from extracted assay data.
- Outputs: `data/splits/{train.txt,val.txt,test.txt,metadata.json}`.
- Default fractions: 70/15/15. Default Jaccard threshold: 0.30.

### Workflow 5: Full Pipeline (GUI)
- "Run Full Pipeline" in `hybrid` mode runs 24 stages sequentially via subprocess.
- Stops on first non-zero exit code.
- Semaphore prevents concurrent executions.

### Workflow 6: Output Inspection
Complete file tree produced by a full pipeline run:
- `data/raw/rcsb/<PDB_ID>.json` — raw GraphQL metadata
- `data/extracted/{entry,chains,bound_objects,interfaces,assays,provenance}/<PDB_ID>.json` — six-table records
- `data/structures/rcsb/<PDB_ID>.cif.gz` — mmCIF files with SHA-256 provenance
- `data/graph/{graph_nodes.json,graph_edges.json,graph_manifest.json}` — canonical graph
- `data/features/feature_records.json` — materialized features
- `data/training_examples/training_examples.json` — assembled training records
- `data/splits/{train.txt,val.txt,test.txt,metadata.json}` — split assignments
- `structures/residue_whole_protein/<PDB_ID>.{nodes.parquet,edges.parquet,pyg.pt,...}` — structural ML graphs
- `datasets/<name>/{train.csv,test.csv,diversity_report.json,...}` — engineered datasets
- `master_pdb_repository.csv`, `master_pdb_pairs.csv`, etc. — root-level exports

### Workflow 7: Prediction Workflows
- Three prediction commands exist with graceful dependency error handling.
- All write output to `data/prediction/`.

### Workflow 8: GUI
- Left panel: Sources / Search Criteria / Pipeline Options tabs.
- Right panel: 28+ stage buttons grouped into 5 sections.
- Bottom: scrollable live log.
- Curation review dashboards with filtering by issue type, confidence, conflicts, mutations.

## 4. Expected Outputs

For a complete pipeline run: raw downloads, 6 extracted tables, normalized records, audit summary, reports, graph data, features, training examples, splits, structural graphs, engineered datasets, release snapshot. All JSON/CSV/Parquet with provenance metadata.

## 5. Confusing Behaviors

### 5.1 Dead Code in `report` Command
File: `src/pbdata/cli.py`, lines 580-653. Bare `return` makes ~70 lines unreachable. Dead block references undefined variables. Indicates incomplete refactoring.

### 5.2 `normalize` Tells Users to Run Non-Existent Command
After SKEMPI ingest, CLI prints: "Run 'normalize --source skempi'" but `normalize` accepts no `--source` option. Dead-end workflow.

### 5.3 Duplicate Dead Ingest Functions
`_ingest_rcsb()` and `_ingest_skempi()` (lines 369-467) are complete alternative implementations never called. ~100 lines of maintenance debt.

### 5.4 Implicit Stage Dependencies
No dependency graph or DAG validation. Stages depend on upstream outputs implicitly. Running out of order produces "planned" manifests that look like success.

### 5.5 GUI Stage List Overwhelm
22 stages in ML Pipeline group, many labeled "(Experimental)" or "(Preview)". No clear indication which are required for a basic dataset.

### 5.6 `engineer-dataset` Prerequisite Not Obvious
Error says "Run metadata harvest first" but CLI command is `harvest-metadata`. Mapping not explicit.

### 5.7 "Architecture Manifest" Silent Success Pattern
Several commands produce "planned" manifests with exit code 0 when upstream data is missing. Downstream stages read these and produce further empty outputs. User sees "PIPELINE COMPLETE" with vacuous artifacts.

## 6. Missing Functionality

1. **SKEMPI normalization** — `normalize --source skempi` not implemented
2. **BioLiP and PDBbind adapters** — documented stubs raising `NotImplementedError`
3. **BindingDB standalone ingest** — no CLI command
4. **ChEMBL standalone ingest** — enrichment-only, no ingest path
5. **CV folds in `build-splits`** — only available in `engineer-dataset`
6. **Pipeline resume/retry** — no mechanism to resume from failure point
7. **No `--source` flag on `normalize`** — hardcoded RCSB only

## 7. Undesirable States

1. **Placeholder manifests masquerading as success** — empty "planned" artifacts exit code 0
2. **Dead code crash risk** — `NameError` if return removed without cleanup
3. **Experimental stages block core stages** — hybrid mode runs all, stops on first error
4. **Eager gemmi/pandas imports** — crash unrelated commands if missing
5. **Storage root defaults to cwd** — data scattered across directories
6. **Empty assay tables from RCSB alone** — no warning surfaced

## 8. User Experience Assessment

### Strengths
- Comprehensive README with 10-step GUI walkthrough
- Confirmation dialogs before downloads
- Rich provenance tracking throughout
- Multiple split strategies with reproducibility metadata
- Graceful dependency handling for optional packages
- Diagnostic commands: `status`, `doctor`, `demo-readiness`
- GUI curation review dashboards with 8 filter categories

### Weaknesses
- Implicit stage dependency management (no DAG)
- No progress indicators for long operations
- Experimental stages mixed with core stages
- Stop-on-first-failure with no resume
- Dead code and stale references (~170 lines)
- Empty assay data not surfaced to users
- Storage root footgun with cwd default

## 9. Recommended Improvements

### Priority 1: Code Cleanup
1. Remove dead code in `report` command (lines 582-653)
2. Remove dead ingest functions `_ingest_rcsb()` and `_ingest_skempi()`
3. Fix misleading SKEMPI normalization message

### Priority 2: Robustness
4. Add `--skip-experimental` / `--core-only` flag
5. Warn when assay data is empty after extraction
6. Make gemmi/pandas imports lazy
7. Add explicit prerequisite validation to all dependent commands

### Priority 3: Usability
8. Separate experimental stages visually in GUI
9. Add progress counters to long-running stages
10. Document pipeline DAG in README
11. Pin storage root to repo directory when config exists
12. Add pipeline resume capability
