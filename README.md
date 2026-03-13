# pbdata — Protein Binding Dataset Platform

A desktop application for building, curating, and releasing protein-binding
datasets for machine learning. It pulls structural and affinity data from
public databases (RCSB PDB, ChEMBL, BindingDB, SKEMPI v2, PDBbind, BioLiP),
normalizes every record into a canonical schema, scores quality, and produces
reproducible train/val/test splits that guard against sequence-identity leakage.

**The primary interface is a Tkinter desktop GUI.** Every pipeline stage can
also be run from the Typer CLI.

---

## Table of contents

1.  [Quick start](#quick-start)
2.  [Installation](#installation)
3.  [Launching the GUI](#launching-the-gui)
4.  [GUI walkthrough](#gui-walkthrough)
    - [Window layout](#window-layout)
    - [Step 1 — Configure sources](#step-1--configure-sources)
    - [Step 2 — Set search criteria](#step-2--set-search-criteria)
    - [Step 3 — Set pipeline options](#step-3--set-pipeline-options)
    - [Step 4 — Run ingest](#step-4--run-ingest)
    - [Step 5 — Run extract](#step-5--run-extract)
    - [Step 6 — Normalize, audit, and report](#step-6--normalize-audit-and-report)
    - [Step 7 — Review data quality](#step-7--review-data-quality)
    - [Step 8 — Build splits and training sets](#step-8--build-splits-and-training-sets)
    - [Step 9 — Build a release snapshot](#step-9--build-a-release-snapshot)
    - [Step 10 — Advanced / experimental stages](#step-10--advanced--experimental-stages)
    - [Run Full Pipeline (one click)](#run-full-pipeline-one-click)
5.  [Data overview panel](#data-overview-panel)
6.  [Review and curation dashboards](#review-and-curation-dashboards)
7.  [Log panel](#log-panel)
8.  [Supported data sources](#supported-data-sources)
9.  [CLI reference](#cli-reference)
10. [Where files are stored](#where-files-are-stored)
11. [Configuration files](#configuration-files)
12. [Testing](#testing)
13. [Project layout](#project-layout)
14. [Troubleshooting](#troubleshooting)

---

## Quick start

```bash
# 1. Clone and install
git clone <repo-url> bio-agent-lab
cd bio-agent-lab
python -m venv .venv
.venv\Scripts\activate        # Windows
# source .venv/bin/activate   # macOS / Linux
pip install -e ".[dev]"

# 2. Launch the GUI
pbdata-gui
```

That's it. The GUI opens and you can start building datasets immediately.
By default, out-of-the-box runs use live RCSB, BindingDB, and ChEMBL enrichment.
Local-file sources such as PDBbind, BioLiP, and SKEMPI remain opt-in.

---

## Installation

**Requirements:** Python 3.11 or newer, pip.

```bash
# Create a virtual environment (recommended)
python -m venv .venv

# Activate it
.venv\Scripts\activate          # Windows (cmd / PowerShell)
source .venv/bin/activate       # macOS / Linux

# Install the package in editable mode with dev extras
pip install -e ".[dev]"
```

This installs all core dependencies: `pydantic>=2.7`, `pyyaml`, `typer`,
`rich`, `gemmi`, `pandas>=2.0`, and `requests>=2.31`.

### Optional heavy dependencies

These are **not required** for the core pipeline. Install only if you plan to
use the advanced ML stages (structural graphs, dataset engineering, physics
features):

| Package          | Used by                                  |
|------------------|------------------------------------------|
| `torch`          | Structural graphs, dataset engineering   |
| `torch-geometric`| PyG graph export format                  |
| `pyarrow`        | Parquet export in feature pipeline       |
| `scikit-learn`   | Tabular affinity model training          |
| `dgl`            | DGL graph export format                  |

All optional imports are lazy-guarded — the GUI and CLI start without them.
If a stage needs a missing package, it prints a clear installation instruction
and exits without crashing.

---

## Launching the GUI

Any of these commands open the same window:

```bash
pbdata-gui                           # entry-point script
python -m pbdata.cli gui             # via the CLI
python -c "from pbdata.gui import main; main()"   # direct import
```

The window opens at roughly 1280 x 860 (scales to your screen) and is fully
resizable. Minimum size is 760 x 520.

---

## GUI walkthrough

### Window layout

```
+-------------------------------------------------------------------+
|  HEADER BAR                                                       |
|  pbdata -- Protein Binding Dataset Platform                       |
+-------------------------------------------------------------------+
|                       |                                           |
|  LEFT COLUMN          |  RIGHT COLUMN                             |
|  (tabbed notebook)    |  (scrollable)                             |
|                       |                                           |
|  +-- Sources ------+  |  +-- Data Overview --+                    |
|  |  enable/disable |  |  |  file counts      |                    |
|  |  each database  |  |  |  review exports   |                    |
|  +-----------------+  |  |  health summary   |                    |
|                       |  |  training builder  |                    |
|  +-- Search -------+  |  |  curation review  |                    |
|  |  criteria for   |  |  +-------------------+                    |
|  |  RCSB queries   |  |                                           |
|  |  review filters |  |  +-- Pipeline --------+                   |
|  +-----------------+  |  |  Workflow Engine    |                   |
|                       |  |  Data Acquisition   |                   |
|  +-- Options ------+  |  |  Processing         |                   |
|  |  storage root   |  |  |  Quality & Analysis |                   |
|  |  split params   |  |  |  ML Pipeline        |                   |
|  |  graph options  |  |  |  [Run Full Pipeline] |                   |
|  |  release tag    |  |  +--------------------+                   |
|  +-----------------+  |                                           |
|                       |                                           |
+-------------------------------------------------------------------+
|  LOG PANEL                                                        |
|  Live output from every pipeline stage                            |
+-------------------------------------------------------------------+
```

- **Left column** has three tabs: Sources, Search Criteria, Options.
- **Right column** scrolls vertically and contains the Data Overview
  dashboards at top, then every pipeline stage button grouped by phase.
- **Log panel** at the bottom streams real-time output from running stages.

All panels support mouse-wheel scrolling anywhere inside them.

---

### Step 1 — Configure sources

Click the **Sources** tab (left column).

1. **Enable databases.** Tick the checkboxes next to the sources you want:

   | Source    | What it provides                                    |
   |-----------|-----------------------------------------------------|
   | RCSB      | Structural metadata via RCSB Search + GraphQL API   |
   | ChEMBL    | Bioactivity data (Kd, Ki, IC50) -- queried at Extract time |
   | BindingDB | Binding affinity by PDB ID -- queried at Extract time |
   | SKEMPI    | Protein-protein mutation ddG dataset                |
   | PDBbind   | Curated protein-ligand affinities (requires local files) |
   | BioLiP    | Biologically relevant ligand-protein (requires local files) |

2. **Set local paths** (only for sources that need them):
   - BindingDB: optional local cache directory
   - PDBbind: path to your local PDBbind dataset directory
   - BioLiP: path to your local BioLiP data directory
   - SKEMPI: optional path to a pre-downloaded CSV file

   Use the `...` button to browse for directories or files.

3. **Choose a structure mirror:** RCSB (default) or PDBj. This controls where
   mmCIF/PDB files are downloaded from during extraction.

4. Click **Save Source Config**. This writes `configs/sources.yaml`.

---

### Step 2 — Set search criteria

Click the **Search Criteria** tab.

This controls which PDB entries are returned when you run Ingest with RCSB
enabled.

1. **Direct PDB IDs** (top of the tab): Enter a comma-separated list of PDB
   IDs (e.g. `1ATP, 3HTB, 6LU7`). When set, this bypasses the RCSB Search
   API and fetches those entries directly.

2. **Optional result limit:** Set a maximum number of structures to keep from
   the RCSB search.

3. **Representative sampling:** When a result limit is active, leave this on
   to make a best-effort representative selection across task-type, method,
   taxonomy, and resolution buckets instead of taking a naive top-N slice.

4. **Text filters:**
   - Keywords / full-text -- free-text search across PDB titles and descriptions
   - Organism name -- e.g. "Homo sapiens"
   - NCBI taxonomy ID -- e.g. `9606`

5. **Checkboxes:**
   - Membrane-related structures only
   - Require multimeric protein entries

6. **Experimental methods:** tick X-Ray, Cryo-EM, NMR, Neutron Diffraction.

7. **Max resolution:** dropdown from 1.0 A to 5.0 A (default 3.0 A).

8. **Interaction types:**
   - Protein-Ligand binding
   - Protein-Protein interaction
   - Mutation ddG

9. **Structure filters:**
   - Require protein entity / ligand / branched entities
   - Min/max counts for protein entities, nonpolymer entities, branched
     entities, biological assemblies
   - Max deposited atom count

10. **Release year range:** From / To year fields.

11. Click **Save Search Criteria**. This writes `configs/criteria.yaml`.
12. Click **Preview RCSB Search** to write a preview report showing total
    matches, selected matches under the current cap, and a small distribution
    summary before download.

#### Local review filters (bottom of the Search Criteria tab)

After you have run Extract and refreshed root exports, you can filter the
review CSVs right inside the GUI:

- Filter by PDB ID, pair key, issue type, confidence level
- Toggle: conflicted pairs only, mutation-ambiguous only, metal/cofactor/glycan
- Click **Apply Review Filter** to write `master_pdb_review_filtered.csv`
- Click **Reset Review Filter** to clear all filters
- Click **Refresh Root Exports** to regenerate the master CSVs from extracted
  data

---

### Step 3 — Set pipeline options

Click the **Options** tab.

#### Storage root
Where all generated files go. Defaults to the current working directory.
Click **Browse...** to change it. All output lands under
`<storage root>/data/`.

#### Pipeline mode
- **legacy** -- current pipeline stages only
- **site-centric** -- new artifacts/ pipeline only (experimental)
- **hybrid** (default) -- runs both, sharing extract/canonical inputs

Additional fields for site-centric mode:
- Site-centric run ID
- Physics batch ID
- "Allow degraded site physics proxies" checkbox

#### Extract options
- Download mmCIF structure files (on by default)
- Also download PDB format files (off by default)

#### Structural graph options
- Graph level: residue (default) or atom
- Scope: whole_protein, interface_only, or shell
- Export formats: comma-separated list (pyg, dgl, networkx)

#### Split options
- Workers count (parallel threads for extract/normalize/audit)
- Split mode: auto, pair-aware, legacy-sequence, hash
- Train fraction (default 0.70)
- Validation fraction (default 0.15)
- Random seed (default 42)
- Jaccard threshold (default 0.30)
- Hash-only split checkbox (skips sequence clustering)

#### Dataset engineering
- Dataset name, test fraction, CV folds, cluster count
- Embedding backend: auto, esm, fallback
- Strict protein-family isolation checkbox

#### Release options
- Release tag (optional -- defaults to UTC timestamp if blank)

#### Custom training set
- Selection mode: generalist, protein_ligand, protein_protein,
  mutation_effect, high_trust
- Target size, seed, per-receptor cluster cap

---

### Step 4 — Run ingest

In the **right column**, under **Data Acquisition**, click **Ingest Sources**.

What happens:
1. The GUI saves your current source config.
2. For each enabled source:
   - **RCSB:** queries the RCSB Search API (or fetches direct PDB IDs),
     shows a confirmation dialog with the entry count, then downloads raw
     JSON metadata to `data/raw/rcsb/`. Already-cached valid entries are
     reused automatically.
   - **SKEMPI:** downloads the SKEMPI v2 CSV (~3 MB) after confirmation,
     or reuses an existing valid copy.
   - **ChEMBL / BindingDB:** logged as "enabled" -- these are queried later
     during the Extract stage.
   - **PDBbind / BioLiP:** validates the local path you set.
3. A summary appears in the log panel.

---

### Step 5 — Run extract

Under **Processing**, click **Extract Multi-Table**.

This reads raw RCSB JSON files, downloads mmCIF structure files (if enabled),
and produces six output tables:

| Table          | Contents                                          |
|----------------|---------------------------------------------------|
| `entry/`       | One JSON per PDB entry -- metadata, resolution, quality |
| `chains/`      | Protein chain records with sequences              |
| `bound_objects/` | Ligands, ions, cofactors, glycans               |
| `interfaces/`  | Protein-protein and protein-ligand interfaces     |
| `assays/`      | Binding affinity data from ChEMBL, BindingDB, etc. |
| `provenance/`  | SHA-256 hashes, timestamps, source versions       |

Output goes to `data/extracted/`. If ChEMBL or BindingDB are enabled, their
APIs are queried during this stage to enrich each entry with assay data.

---

### Step 6 — Normalize, audit, and report

Still under **Processing** and **Quality & Analysis**:

1. **Normalize Records** -- converts raw records into the canonical Pydantic
   schema and writes to `data/processed/rcsb/`.
2. **Audit Quality** -- scores each record on a 0.0-1.0 scale and flags issues.
3. **Generate Report** -- writes a summary statistics report.
4. **Report Bias** -- generates automatic dataset-bias summaries from extracted
   records.

Each button runs independently. Click them in order, or use
**Run Full Pipeline** to run everything sequentially.

---

### Step 7 — Review data quality

After Extract completes, click **Refresh Root Exports** (in the Data Overview
panel or the Search Criteria tab). This regenerates:

- `master_pdb_repository.csv` -- one row per PDB entry
- `master_pdb_pairs.csv` -- one row per binding pair
- `master_pdb_issues.csv` -- flagged quality issues
- `master_pdb_conflicts.csv` -- multi-source value conflicts
- `master_source_state.csv` -- source coverage summary

The **Data Overview** panel updates automatically to show file counts.

The **Review Health** section shows:
- Release readiness (Not ready / Needs review / Partially ready / Release-ready)
- Coverage snapshot (entries, pairs, model-ready count)
- Quality snapshot (conflicts, missing structures, low-confidence issues)
- Recommended next action

The **Curation Review** section shows top exclusion reasons, conflict bands,
and issue types at a glance.

Use the **Local Review Filters** (Search Criteria tab, bottom) to drill into
specific PDB IDs, issue types, or confidence levels.

---

### Step 8 — Build splits and training sets

Under **ML Pipeline**:

1. **Build Splits** -- creates `data/splits/train.txt`, `val.txt`, `test.txt`
   using k-mer Jaccard sequence clustering to prevent data leakage. Falls
   back to hash-based splitting if sequences are unavailable.

2. **Build Custom Training Set** -- selects a diversity-optimized subset from
   model-ready pairs. Controls: selection mode, target size, seed, and per-
   receptor cluster cap (set in the Options tab).

Or use the **Training Set Builder** dashboard (in the Data Overview panel):
- Click **Run Training Set Workflow** to run Build Splits, Build Custom
  Training Set, and Build Release in sequence.
- The dashboard shows KPI tiles: selected count, clusters, mean quality,
  max dominance, excluded count.
- Workflow progress pills track each step: Model-ready, Custom set,
  Scorecard, Benchmark, Release.

---

### Step 9 — Build a release snapshot

Under **ML Pipeline**, click **Build Release Snapshot**.

This freezes all current artifacts into a versioned directory under
`data/releases/<tag>/` with a manifest JSON. If no release tag is set in
Options, it uses the current UTC timestamp.

You can also click **Open Latest Release** in the Release Artifacts section
of the Data Overview to jump to the most recent snapshot folder.

---

### Step 10 — Advanced / experimental stages

These stages appear under **ML Pipeline** and are optional:

#### Core ML stages

| Stage                           | What it does                              |
|---------------------------------|-------------------------------------------|
| Build Structural Graphs         | Residue- or atom-level graphs for GNNs    |
| Build Graph                     | Graph-layer architecture manifest          |
| Build Features                  | First-pass features from extract + graph   |
| Build Training Examples         | Assemble examples from all upstream layers |
| Train Baseline Model            | Split-aware ligand-memory baseline         |
| Evaluate Baseline Model         | Evaluate baseline against split files      |
| Train Tabular Affinity Model    | Lightweight supervised affinity model      |
| Evaluate Tabular Affinity Model | Evaluate supervised model                  |
| Engineer Dataset                | Full diverse ML dataset export              |
| Run Scenario Tests              | QA scenario templates                      |

#### Physics and site-centric stages (experimental)

| Stage                             | What it does                              |
|-----------------------------------|-------------------------------------------|
| Build Microstates                 | Pair-level microstate assignments          |
| Build Physics Features            | Electrostatic proxy features               |
| Build Conformational States       | Catalogs experimental conformations        |
| Build Microstate Refinement       | Protonation-policy planning                |
| Build MM Job Manifests            | OpenMM job manifests                       |
| Run MM Jobs                       | Dispatch OpenMM jobs                       |
| Run Site-Centric Feature Pipeline | New artifacts/ feature pipeline            |
| Export Analysis Queue             | Motif queues for external ORCA/APBS        |
| Ingest Physics Results            | Import ORCA/APBS/OpenMM outputs            |
| Train Site-Physics Surrogate      | Train a deterministic surrogate model      |

Experimental stages require additional dependencies (torch, pyarrow) and may
produce scaffold/placeholder outputs if upstream physics data is not yet
available. The GUI labels them as "(Experimental)" or "(Preview)".

---

### Run Full Pipeline (one click)

Click **Run Full Pipeline** at the bottom of the Pipeline panel to run
every stage in sequence:

1. Ingest (with confirmation dialogs)
2. Extract, Normalize, Audit, Report
3. All ML Pipeline stages (based on your pipeline mode selection)

The pipeline stops on the first error. Each stage's status indicator updates
in real time (idle / running / done / error).

---

## Data overview panel

The Data Overview panel (top of the right column) shows at a glance:

| Metric           | Source                           |
|------------------|----------------------------------|
| Raw RCSB entries | `data/raw/rcsb/*.json`           |
| SKEMPI CSV       | `data/raw/skempi/skempi_v2.csv`  |
| Processed records| `data/processed/rcsb/*.json`     |
| Extracted entries| `data/extracted/entry/*.json`    |
| Chains           | `data/extracted/chains/*.json`   |
| Bound objects    | `data/extracted/bound_objects/*.json` |
| Assay records    | `data/extracted/assays/*.json`   |
| Graph nodes/edges| `data/graph/graph_*`             |
| Split files      | `data/splits/*.txt`              |

Below the counts:
- **Root Review Exports** -- paths to the master CSVs + refresh button
- **Release Artifacts** -- links to model-ready CSV, custom training set,
  scorecard, manifest, coverage summary
- **Training Set Builder** -- KPI tiles + workflow progress + quick actions
- **Curation Review** -- exclusion/conflict/issue summaries
- **Review Health** -- release readiness assessment
- **Interpretation Guide** -- explains confidence levels and conflict flags
- **Quick Actions** -- buttons to open any artifact file directly

---

## Review and curation dashboards

The GUI provides three integrated review dashboards:

### Review Health
Automatically computed from `scientific_coverage_summary.json`:
- **Release readiness:** Not ready, Needs review, Partially ready, or
  Release-ready
- **Recommended next step:** actionable guidance based on current blockers

### Training Set Builder
Updated from `custom_training_scorecard.json` and split benchmark:
- KPI tiles: Selected, Clusters, Mean quality, Max dominance, Excluded
- Workflow pills: Model-ready -> Custom set -> Scorecard -> Benchmark -> Release
- One-click **Run Training Set Workflow** button

### Curation Review
Summarizes exclusions, conflicts, and issues from root CSVs:
- Top exclusion reasons, conflict agreement bands, issue types
- Quick-open buttons for issues and conflicts CSVs

---

## Log panel

The bottom panel shows live output from every running stage:

- Dark-themed, monospace font (Cascadia Code)
- Auto-scrolls to the latest output
- Mouse-wheel scrollable
- **Clear Log** button at bottom right

Each stage logs a separator banner when it starts and a `[stage] done/error`
line when it finishes.

---

## Supported data sources

| Source    | Type         | Status             | Data provided                    |
|-----------|-------------|--------------------|---------------------------------|
| RCSB PDB  | Remote API  | Fully implemented  | Structural metadata, mmCIF files |
| ChEMBL    | Remote API  | Fully implemented  | Kd, Ki, IC50 bioactivity         |
| BindingDB | Remote API  | Fully implemented  | Binding affinity by PDB ID       |
| SKEMPI v2 | Remote CSV  | Fully implemented  | Protein-protein mutation ddG     |
| PDBbind   | Local files | Index parser ready | Curated protein-ligand affinities |
| BioLiP    | Local files | Row parser ready   | Biologically relevant ligands     |

---

## CLI reference

Every GUI action has a CLI equivalent. Run any command with `--help` for
detailed options.

### Core pipeline

```bash
pbdata ingest --source rcsb          # download raw metadata
pbdata ingest --source skempi        # download SKEMPI v2
pbdata preview-rcsb-search           # preview capped/representative RCSB search
pbdata extract                       # multi-table extraction + structure download
pbdata normalize                     # canonicalize records
pbdata audit                         # quality score + flag
pbdata report                        # summary statistics
pbdata report-bias                   # dataset bias summaries
```

### Splits, training, and release

```bash
pbdata build-splits                  # k-mer Jaccard clustering splits
pbdata build-custom-training-set     # diversity-optimized subset
pbdata build-release --tag v1.0      # freeze a release snapshot
pbdata release-check                 # release readiness report
```

### ML pipeline

```bash
pbdata build-structural-graphs       # residue/atom-level graphs
pbdata build-graph                   # graph-layer manifest
pbdata build-features                # first-pass features
pbdata build-training-examples       # assemble from all layers
pbdata engineer-dataset              # full dataset export with CV folds
pbdata train-baseline-model          # ligand-memory baseline
pbdata evaluate-baseline-model       # evaluate against splits
pbdata train-tabular-affinity-model  # supervised affinity model
pbdata evaluate-tabular-affinity-model
pbdata report-training-set-quality   # training set quality report
pbdata report-model-comparison       # model comparison summary
pbdata export-identity-crosswalk     # protein / ligand / pair crosswalk tables
pbdata report-source-capabilities    # source capability and config summary
```

### Physics and site-centric pipeline

```bash
pbdata build-microstates             # microstate assignments
pbdata build-physics-features        # electrostatic features
pbdata build-microstate-refinement   # protonation planning
pbdata build-mm-job-manifests        # OpenMM job generation
pbdata run-mm-jobs                   # dispatch OpenMM
pbdata run-feature-pipeline          # site-centric feature pipeline
pbdata export-analysis-queue         # ORCA/APBS motif queues
pbdata ingest-physics-results        # import external physics
pbdata train-site-physics-surrogate  # surrogate model training
```

### Prediction and risk scoring

```bash
pbdata predict-ligand-screening --smiles "CC(=O)Oc1ccccc1C(=O)O"
pbdata predict-peptide-binding --structure-file structure.cif
pbdata score-pathway-risk --targets "P00533,P04637"
```

### Workspace utilities

```bash
pbdata status                        # data snapshot
pbdata doctor                        # dependency + config check
pbdata demo-readiness                # demo readiness assessment
pbdata setup-workspace               # create workspace directories
pbdata harvest-metadata              # build unified metadata table
pbdata build-conformational-states   # experimental conformations
pbdata run-scenario-tests            # QA scenarios
pbdata gui                           # launch GUI from CLI
```

### Global options

```bash
pbdata --storage-root /path/to/data <command>
pbdata --config configs/sources.yaml <command>
```

---

## Where files are stored

All output lives under `<storage root>/data/`:

```
data/
  raw/
    rcsb/          *.json    Raw RCSB GraphQL responses
    skempi/        *.csv     SKEMPI v2 download
  processed/
    rcsb/          *.json    Canonical schema records
  extracted/
    entry/         *.json    Multi-table: entry metadata
    chains/        *.json    Multi-table: protein chains
    bound_objects/ *.json    Multi-table: ligands, ions, cofactors
    interfaces/    *.json    Multi-table: binding interfaces
    assays/        *.json    Multi-table: binding affinities
    provenance/    *.json    Multi-table: source hashes + timestamps
  structures/
    rcsb/          *.cif.gz  Downloaded mmCIF structure files
  graph/                     Graph nodes and edges
  features/                  Computed features
  splits/          *.txt     train.txt, val.txt, test.txt
  audit/                     Quality audit outputs
  reports/                   Summary and bias reports
  identity/                  Protein / ligand / pair crosswalk exports
  models/                    Trained model artifacts
  prediction/                Prediction outputs
  releases/                  Versioned release snapshots
  custom_training_sets/      Curated training subsets
  catalog/                   Download manifest, stage/source state

artifacts/                   Site-centric feature pipeline outputs
  manifests/                 Feature run manifests
  base_features/             Base feature extractions
  site_physics/              Physics enrichment artifacts
  graphs/                    Graph construction artifacts
  training_examples/         Assembled training records
  archetypes/                Motif archetype clusters
  physics_targets/           Physics target records
  surrogate_training/        Surrogate model outputs
```

Root-level review CSVs are written to the repo root:
- `master_pdb_repository.csv`
- `master_pdb_pairs.csv`
- `master_pdb_issues.csv`
- `master_pdb_conflicts.csv`
- `master_source_state.csv`
- `model_ready_pairs.csv`
- `custom_training_set.csv`

---

## Configuration files

| File                     | Purpose                                   |
|--------------------------|-------------------------------------------|
| `configs/criteria.yaml`  | RCSB search criteria (methods, resolution, task types, filters) |
| `configs/sources.yaml`   | Enabled sources, local paths, structure mirror |

Both are auto-saved by the GUI when you click their respective Save buttons.
You can also edit them by hand -- they are plain YAML.

**Example `configs/criteria.yaml`:**
```yaml
direct_pdb_ids: []
max_results: 250
representative_sampling: true
experimental_methods: [xray, em]
max_resolution_angstrom: 3.0
task_types: [protein_ligand, protein_protein]
membrane_only: false
require_protein: true
require_ligand: false
```

**Example `configs/sources.yaml`:**
```yaml
sources:
  rcsb:
    enabled: true
    extra:
      structure_mirror: rcsb
  chembl:
    enabled: true
  skempi:
    enabled: false
storage_root: /path/to/workspace
```

---

## Testing

```bash
# Run the full test suite
.venv/Scripts/python.exe -m pytest               # Windows
python -m pytest                                  # macOS / Linux

# Run the suite in shorter sequential shards
.venv/Scripts/python.exe scripts/run_test_shards.py
make test-sharded

# Run only the integration-marked subset
python -m pytest -m integration

# Run a specific test file
python -m pytest tests/test_extract_pipeline.py -v
```

If your environment is unstable with a single long `pytest` invocation, prefer
the sharded runner above. It executes the suite in smaller file batches and
stops on the first failing shard.

The test suite includes three immutable stress test panels (32 adversarial
PDB entries) that validate classification logic against biological ground
truth. These panels and their expected outcomes are read-only and must never
be modified to make tests pass.

---

## Project layout

```
bio-agent-lab/
  src/pbdata/
    cli.py                   Typer CLI (42 commands)
    gui.py                   Tkinter GUI (3,300+ lines)
    gui_overview.py          GUI data overview helpers
    cli_reporting.py         CLI reporting helpers
    config.py                Pydantic config models
    criteria.py              Search criteria model + YAML I/O
    storage.py               Storage layout (50+ paths)
    master_export.py         Root CSV export logic
    pairing.py               Pair identity key + matching
    stage_state.py           Pipeline stage state tracking
    source_state.py          Source state tracking
    table_io.py              Parquet I/O wrappers
    workspace_state.py       Workspace state management
    schemas/
      canonical_sample.py    CanonicalBindingSample (frozen Pydantic v2)
      records.py             Multi-table schemas (Entry, Chain, BoundObject, etc.)
      bound_objects.py       BoundObject, InterfaceInfo, AssemblyInfo
      features.py            Feature record schema
      graph.py               Graph node/edge schemas
      training_example.py    Training example schema
      prediction_input.py    Prediction input schema
      conformational_state.py Conformational state schema
    sources/
      rcsb.py                RCSB GraphQL adapter
      rcsb_search.py         RCSB Search API adapter
      rcsb_classify.py       Entity classification pipeline (946 lines)
      chembl.py              ChEMBL REST API adapter
      bindingdb.py           BindingDB adapter
      skempi.py              SKEMPI v2 adapter
      pdbbind.py             PDBbind index parser
      biolip.py              BioLiP row parser
    parsing/
      mmcif_supplement.py    mmCIF download + quality extraction
    pipeline/
      extract.py             Multi-table extraction pipeline
      assay_merge.py         Multi-source assay merge with conflict detection
      enrichment.py          External assay sample loading
      canonical_workflows.py Reusable pipeline orchestration
      feature_execution.py   Site-centric feature pipeline (7 stages)
      feature_pipeline_stages.py  Feature stage definitions
      feature_pipeline_runtime.py Feature runtime helpers
      feature_post_pipeline.py    Post-pipeline analysis
      physics_feedback.py    Physics results ingest + surrogate training
    quality/
      audit.py               Quality flags + scoring
      stress_panel.py        Stress test panel comparison
    dataset/
      splits.py              k-mer Jaccard clustering splits
      engineering.py         Dataset engineering with ESM embeddings
      conformations.py       Conformational state builder
    graph/
      builder.py             Canonical knowledge graph builder
      structural_graphs.py   Residue/atom graph builder (PyG/DGL/NetworkX)
      connectors.py          STRING/Reactome/BioGRID connectors
      identifier_map.py      Cross-database identifier mapping
    features/
      builder.py             Feature materializer
      microstate.py          Electrostatic microstate assignment
      physics_features.py    Local physics feature builder
      mm_features.py         Molecular mechanics features
      pathway.py             Pathway feature extraction
    training/
      assembler.py           Training example assembly
      generator.py           Training data generator
    models/
      baseline_memory.py     Ligand-memory baseline model
      tabular_affinity.py    Tabular supervised affinity model
      affinity_models.py     Affinity model definitions
      off_target_models.py   Off-target model definitions
    prediction/
      engine.py              Prediction workflow engine
      ligand_screening.py    Ligand screening workflow
      peptide_binding.py     Peptide binding workflow
      variant_effects.py     Variant effect prediction
    risk/
      summary.py             Pathway risk summary
      pathway_reasoning.py   Pathway reasoning logic
      severity_scoring.py    Severity scoring
    qa/
      scenario_runner.py     QA scenario framework
    custom_training_set.py   Custom training set builder
    release_export.py        Release artifacts + readiness
    model_comparison.py      Model comparison reporting
    training_quality.py      Training set quality metrics
    demo.py                  Demo snapshot export
  configs/
    criteria.yaml            Search criteria
    sources.yaml             Source configuration
  tests/                     326+ unit tests, 48 test files
  specs/                     Specification documents
  handoffs/                  Agent analysis reports
  docs/                      Architecture and gap analysis docs
  data/                      All pipeline output (gitignored)
```

---

## Troubleshooting

**GUI won't start / Tkinter not found:**
Tkinter ships with standard Python on Windows. On Linux, install
`python3-tk` (e.g. `sudo apt install python3-tk`).

**`ModuleNotFoundError: No module named 'pbdata'`:**
Run `pip install -e ".[dev]"` from the repo root.

**Stages crash with `ModuleNotFoundError: torch`:**
Torch is optional. The core pipeline (ingest, extract, normalize, audit,
report, splits, custom training set, release) works without it. Install
torch only if you need structural graphs or dataset engineering.

**Stages crash with `ModuleNotFoundError: pyarrow`:**
Install pyarrow (`pip install pyarrow`) only if you need the Parquet export
in the feature pipeline.

**"No sources enabled" error on ingest:**
Open the Sources tab, tick at least one source checkbox, and click
Save Source Config.

**Ingest shows 0 matching entries:**
Your search criteria may be too restrictive. Try widening the resolution
limit, enabling more experimental methods, or adding direct PDB IDs.

**Data appears in the wrong directory:**
The storage root defaults to the current working directory. If you run
`pbdata` from different directories, output may land in unexpected places.
Set an explicit storage root in the Options tab or use
`pbdata --storage-root /path/to/workspace <command>`.

**"No metadata rows found" from engineer-dataset:**
Run `pbdata harvest-metadata` first. This requires extracted data from a
prior `pbdata extract` run.

**Empty assay tables after extraction:**
Assay data comes from external sources (ChEMBL, BindingDB, SKEMPI), not
from RCSB alone. Enable at least one affinity source in the Sources tab to
populate assay records.
