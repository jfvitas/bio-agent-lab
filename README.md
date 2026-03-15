# pbdata — Protein Binding Dataset Platform

Build, curate, and release protein-binding datasets for machine learning —
entirely from a desktop GUI.

pbdata pulls structural and affinity data from public databases (RCSB PDB,
ChEMBL, BindingDB, SKEMPI v2, PDBbind, BioLiP), normalizes every record into a
canonical schema, scores quality, detects cross-source conflicts, and produces
reproducible train/val/test splits that guard against sequence-identity leakage.
The result is a versioned, release-ready dataset you can hand directly to a
model training pipeline.

---

## Table of contents

1. [Quick start](#quick-start)
2. [Installation](#installation)
3. [Launching the GUI](#launching-the-gui)
4. [What the GUI does](#what-the-gui-does)
5. [GUI layout](#gui-layout)
6. [Walkthrough: building a dataset from scratch](#walkthrough-building-a-dataset-from-scratch)
   - [Step 1 — Pick your data sources](#step-1--pick-your-data-sources)
   - [Step 2 — Define search criteria](#step-2--define-search-criteria)
   - [Step 3 — Set pipeline options](#step-3--set-pipeline-options)
   - [Step 4 — Ingest raw data](#step-4--ingest-raw-data)
   - [Step 5 — Extract multi-table records](#step-5--extract-multi-table-records)
   - [Step 6 — Normalize, audit, and report](#step-6--normalize-audit-and-report)
   - [Step 7 — Review data quality](#step-7--review-data-quality)
   - [Step 8 — Build splits and training sets](#step-8--build-splits-and-training-sets)
   - [Step 9 — Build a release snapshot](#step-9--build-a-release-snapshot)
   - [Step 10 — Advanced and experimental stages](#step-10--advanced-and-experimental-stages)
   - [Run Full Pipeline (one click)](#run-full-pipeline-one-click)
7. [Demo Mode](#demo-mode)
8. [Data overview panel](#data-overview-panel)
9. [Review and curation dashboards](#review-and-curation-dashboards)
10. [Log panel](#log-panel)
11. [Supported data sources](#supported-data-sources)
12. [CLI reference](#cli-reference)
13. [Where files are stored](#where-files-are-stored)
14. [Configuration files](#configuration-files)
15. [Testing](#testing)
16. [Troubleshooting](#troubleshooting)

---

## Quick start

```bash
# 1. Clone and install
git clone https://github.com/jfvitas/bio-agent-lab.git
cd bio-agent-lab
python -m venv .venv
.venv\Scripts\activate        # Windows
# source .venv/bin/activate   # macOS / Linux
pip install -e ".[dev]"

# 2. Launch the GUI
pbdata-gui
```

The GUI opens immediately. Enable at least RCSB in the Sources tab, click
**Ingest Sources**, then work your way down the pipeline buttons on the right
side of the window. Every stage streams live output to the log panel at the
bottom.

On Windows you can also double-click **Launch PBData GUI.bat** in the repo root
— it finds the virtual environment automatically.

There is also a separate WinUI 3 preview shell at **Launch PBData WinUI.bat**.
That launcher opens the guided application under `apps/PbdataWinUI`.

For a fresh Windows clone, the launcher now tries to do the right thing:

- if `.NET 8` is already installed, it uses it
- if `.NET 8` is missing, it installs a private local SDK into `.tools/dotnet`
  for this repo
- if **Windows App Runtime 1.8** is missing, it installs it automatically with
  `winget`
- it builds the WinUI app in `Release` mode and launches the built executable

The older **Launch PBData WinUI Demo.bat** file still works and now forwards to
the same launcher for compatibility.

---

## Installation

**Requirements:** Python 3.11 or newer.

```bash
python -m venv .venv

# Activate the environment
.venv\Scripts\activate          # Windows (cmd / PowerShell)
source .venv/bin/activate       # macOS / Linux

# Install with dev extras (includes test dependencies)
pip install -e ".[dev]"
```

This installs all core dependencies: `pydantic>=2.7`, `pyyaml`, `typer`,
`rich`, `gemmi`, `pandas>=2.0`, and `requests>=2.31`.

### Optional heavy dependencies

These are **not required** for the core pipeline. Install only if you plan to
use the advanced ML stages:

| Package           | Used by                                |
|-------------------|----------------------------------------|
| `torch`           | Structural graphs, dataset engineering |
| `torch-geometric` | PyG graph export format                |
| `pyarrow`         | Parquet export in the feature pipeline |
| `scikit-learn`    | Tabular affinity model training        |
| `dgl`             | DGL graph export format                |

All optional imports are lazy-guarded. The GUI and CLI start without them.
If a stage needs a missing package it prints a clear installation instruction
and exits without crashing.

---

## Launching the GUI

Any of these open the same window:

```bash
pbdata-gui                                         # entry-point script
pbdata gui                                         # via the Typer CLI
python -m pbdata.gui                               # direct module
python -c "from pbdata.gui import main; main()"    # import
```

On Windows you can also double-click **Launch PBData GUI.bat**.

The window opens at roughly 1280 x 860 pixels and is fully resizable (minimum
760 x 520).

---

## What the GUI does

The GUI is a single window that lets you:

1. **Choose data sources** — toggle RCSB, ChEMBL, BindingDB, SKEMPI, PDBbind,
   and BioLiP on or off. Point local-file sources at their directories.
2. **Define search criteria** — control which PDB entries come back: resolution
   cutoffs, experimental methods, organism filters, interaction types, structure
   composition, and direct PDB ID lists.
3. **Run every pipeline stage** — from raw download through extraction,
   normalization, quality audit, graph building, feature computation, training
   set assembly, model training, and final release snapshot. Each stage has its
   own button; there is also a one-click "Run Full Pipeline" that executes
   everything in order.
4. **Monitor progress** — a live log panel streams real-time stdout from every
   running stage. Status indicators next to each button show idle / running /
   done / error.
5. **Review and curate** — built-in dashboards show file counts, quality
   summaries, conflict reports, training set KPIs, release readiness, and
   demo readiness without leaving the application.
6. **Export a release** — freeze the current dataset into a versioned snapshot
   with a manifest, ready to hand off to a training pipeline.

---

## GUI layout

```
+---------------------------------------------------------------------+
|  HEADER BAR                                          [ Demo Mode ]  |
|  pbdata — Protein Binding Dataset Platform                          |
+---------------------------------------------------------------------+
|                         |                                           |
|  LEFT COLUMN            |  RIGHT COLUMN (scrollable)                |
|  (tabbed notebook)      |                                           |
|                         |  +-- Data Overview -----------------+     |
|  +-- Sources ---------+ |  |  file counts, review exports,    |     |
|  |  enable/disable    | |  |  health summary, training KPIs,  |     |
|  |  each database     | |  |  curation review, demo readiness |     |
|  +--------------------+ |  +----------------------------------+     |
|                         |                                           |
|  +-- Search ----------+ |  +-- Pipeline Stages ---------------+     |
|  |  RCSB criteria     | |  |  Workflow Engine                 |     |
|  |  review filters    | |  |  Data Acquisition (Ingest)       |     |
|  +--------------------+ |  |  Processing (Extract, Normalize) |     |
|                         |  |  Quality & Analysis              |     |
|  +-- Options ---------+ |  |  ML Pipeline (Graphs → Release)  |     |
|  |  storage root      | |  |  Experimental & Preview          |     |
|  |  split params      | |  |  [Run Full Pipeline]             |     |
|  |  graph options     | |  +----------------------------------+     |
|  |  release tag       | |                                           |
|  +--------------------+ |                                           |
|                         |                                           |
+---------------------------------------------------------------------+
|  LOG PANEL                                          [Clear Log]     |
|  Live output from every pipeline stage                              |
+---------------------------------------------------------------------+
```

- **Left column** — three tabs: Sources, Search Criteria, Options.
- **Right column** — scrolls vertically. Data Overview dashboards at top, then
  pipeline stage buttons grouped by phase.
- **Log panel** — dark themed, monospace, auto-scrolls to newest output.
- **Header bar** — title, subtitle, and the Demo Mode checkbox (top-right).

---

## Walkthrough: building a dataset from scratch

This section walks through the typical end-to-end workflow. You can run stages
individually or use Run Full Pipeline to do everything at once.

### Step 1 — Pick your data sources

Click the **Sources** tab in the left column.

1. **Tick the checkboxes** next to the databases you want to use:

   | Source    | What it provides                                         | When it runs              |
   |-----------|----------------------------------------------------------|---------------------------|
   | RCSB      | Structural metadata via RCSB Search + GraphQL API        | Ingest stage              |
   | ChEMBL    | Bioactivity data (Kd, Ki, IC50) via REST API             | Extract stage (enrichment)|
   | BindingDB | Binding affinity by PDB ID via REST API                  | Extract stage (enrichment)|
   | SKEMPI    | Protein-protein mutation ddG dataset (CSV download)      | Ingest stage              |
   | PDBbind   | Curated protein-ligand affinities (local files required) | Extract stage             |
   | BioLiP    | Biologically relevant ligand-protein (local files required) | Extract stage          |

2. **Set local paths** where needed. BindingDB, PDBbind, BioLiP, and SKEMPI
   have optional or required path fields. Click the `...` button to browse.

3. **Choose a structure mirror:** RCSB (default) or PDBj. This controls where
   mmCIF structure files are downloaded from.

4. Click **Save Source Config**. This writes `configs/sources.yaml`.

### Step 2 — Define search criteria

Click the **Search Criteria** tab.

This controls which PDB entries are returned when you ingest from RCSB.

- **Direct PDB IDs** — enter a comma-separated list (e.g. `1ATP, 3HTB, 6LU7`)
  to bypass the search API and fetch specific entries.
- **Result limit** — cap the number of structures returned.
- **Representative sampling** — when a limit is active, this selects a balanced
  sample across task type, method, taxonomy, and resolution instead of a naive
  top-N.
- **Text filters** — keywords, organism name, NCBI taxonomy ID.
- **Checkboxes** — membrane-only, require multimeric entries.
- **Experimental methods** — X-Ray, Cryo-EM, NMR, Neutron Diffraction.
- **Max resolution** — dropdown from 1.0 A to 5.0 A (default 3.0 A).
- **Interaction types** — protein-ligand, protein-protein, mutation ddG.
- **Structure composition** — require protein/ligand/branched entities, set
  min/max counts for entity types, max atom count.
- **Release year range** — filter by deposition year.

Click **Save Search Criteria** to write `configs/criteria.yaml`.

Click **Preview RCSB Search** to see a distribution summary of what the current
criteria would return, before downloading anything.

#### Local review filters (bottom of the tab)

After running Extract, you can filter the review CSVs directly:

- Filter by PDB ID, pair key, issue type, or confidence level
- Toggle: conflicted pairs only, mutation-ambiguous only, metal/cofactor/glycan
- **Apply Review Filter** writes `master_pdb_review_filtered.csv`
- **Reset Review Filter** clears all filters
- **Refresh Root Exports** regenerates the master CSVs from extracted data

### Step 3 — Set pipeline options

Click the **Options** tab.

| Setting                | What it controls                                    |
|------------------------|-----------------------------------------------------|
| **Storage root**       | Where all output goes. Default: current directory.  |
| **Pipeline mode**      | `legacy`, `site-centric`, or `hybrid` (default).    |
| **Download mmCIF**     | Fetch structure files during Extract (on by default).|
| **Also download PDB**  | Fetch legacy PDB format too (off by default).       |
| **Graph level**        | `residue` (default) or `atom`.                      |
| **Graph scope**        | `whole_protein`, `interface_only`, or `shell`.       |
| **Export formats**     | Comma-separated: `pyg`, `dgl`, `networkx`.          |
| **Workers**            | Thread count for parallel stages.                   |
| **Split mode**         | `auto`, `pair-aware`, `legacy-sequence`, `hash`.    |
| **Train / val / test** | Fraction splits (defaults: 0.70 / 0.15 / 0.15).    |
| **Random seed**        | Reproducibility seed (default 42).                  |
| **Jaccard threshold**  | Sequence similarity cutoff (default 0.30).          |
| **Hash-only splits**   | Skip clustering, use deterministic hashing.         |
| **Dataset name**       | Name for the engineered dataset export.             |
| **Release tag**        | Version label for the release snapshot.              |
| **Custom training set**| Selection mode, target size, seed, cluster cap.     |

### Step 4 — Ingest raw data

In the right column, under **Data Acquisition**, click **Ingest Sources**.

What happens:
1. The GUI saves your current source config.
2. For each enabled source:
   - **RCSB** — queries the Search API (or fetches direct PDB IDs), shows a
     confirmation dialog with the entry count, then downloads raw JSON to
     `data/raw/rcsb/`. Already-cached valid entries are reused.
   - **SKEMPI** — downloads the SKEMPI v2 CSV (~3 MB) after confirmation, or
     reuses an existing copy.
   - **ChEMBL / BindingDB** — logged as "enabled." These are queried later
     during Extract.
   - **PDBbind / BioLiP** — validates the local path.
3. A summary appears in the log panel.

### Step 5 — Extract multi-table records

Under **Processing**, click **Extract Multi-Table**.

This reads raw RCSB JSON, downloads mmCIF structure files (if enabled), and
produces six output tables:

| Table            | Contents                                               |
|------------------|--------------------------------------------------------|
| `entry/`         | One JSON per PDB entry — metadata, resolution, quality |
| `chains/`        | Protein chain records with sequences                   |
| `bound_objects/` | Ligands, ions, cofactors, glycans                      |
| `interfaces/`    | Protein-protein and protein-ligand interfaces          |
| `assays/`        | Binding affinity data from ChEMBL, BindingDB, etc.     |
| `provenance/`    | SHA-256 hashes, timestamps, source versions            |

If ChEMBL or BindingDB are enabled, their APIs are queried here to enrich each
entry with assay data. Output goes to `data/extracted/`.

### Step 6 — Normalize, audit, and report

Under **Processing** and **Quality & Analysis**:

1. **Normalize Records** — converts raw records into the canonical Pydantic
   schema (`CanonicalBindingSample`) and writes to `data/processed/rcsb/`.
2. **Audit Quality** — scores each record on a 0.0–1.0 scale and flags issues.
3. **Generate Report** — writes summary statistics.
4. **Report Bias** — generates dataset bias summaries from the extracted
   records.

Each button runs independently. Click them in order, or use Run Full Pipeline.

### Step 7 — Review data quality

After Extract completes, click **Refresh Root Exports** (in Data Overview or
the Search Criteria tab). This regenerates:

- `master_pdb_repository.csv` — one row per PDB entry
- `master_pdb_pairs.csv` — one row per binding pair
- `master_pdb_issues.csv` — flagged quality issues
- `master_pdb_conflicts.csv` — multi-source value conflicts
- `master_source_state.csv` — source coverage summary

The **Data Overview** panel updates to show file counts. The **Review Health**
section shows release readiness, coverage and quality snapshots, and a
recommended next action. The **Curation Review** section shows top exclusion
reasons, conflict bands, and issue types.

Use the local review filters (Search Criteria tab, bottom) to drill into
specific PDB IDs, issue types, or confidence levels.

### Step 8 — Build splits and training sets

Under **ML Pipeline**:

1. **Build Splits** — creates `data/splits/train.txt`, `val.txt`, `test.txt`
   using k-mer Jaccard sequence clustering to prevent data leakage. Falls back
   to hash-based splitting if sequences are unavailable.

2. **Build Custom Training Set** — selects a diversity-optimized subset from
   model-ready pairs. Controls: selection mode, target size, seed, and
   per-receptor cluster cap (set in the Options tab).

The **Training Set Builder** dashboard (in Data Overview) shows KPI tiles:
selected count, clusters, mean quality, max dominance, excluded count.
Click **Run Training Set Workflow** to run Build Splits, Build Custom Training
Set, and Build Release in one step.

### Step 9 — Build a release snapshot

Under **ML Pipeline**, click **Build Release Snapshot**.

This freezes all current artifacts into a versioned directory under
`data/releases/<tag>/` with a manifest JSON. If no release tag is set in
Options, it defaults to a UTC timestamp.

Click **Open Latest Release** in the Data Overview panel to jump to the most
recent snapshot folder.

### Step 10 — Advanced and experimental stages

These stages are optional. The GUI labels experimental stages with
"(Experimental)" or "(Preview)".

**Core ML stages:**

| Stage                    | What it does                                    |
|--------------------------|-------------------------------------------------|
| Build Structural Graphs  | Residue- or atom-level graphs for GNNs          |
| Build Graph              | Graph-layer architecture manifest                |
| Build Features           | First-pass features from extract + graph         |
| Build Training Examples  | Assemble examples from all upstream layers       |
| Train Baseline Model     | Split-aware ligand-memory baseline               |
| Evaluate Baseline Model  | Evaluate baseline against split files            |
| Engineer Dataset         | Full diverse ML dataset export                   |
| Run Scenario Tests       | QA scenario tests                                |

**Physics and site-centric stages (experimental):**

| Stage                            | What it does                             |
|----------------------------------|------------------------------------------|
| Build Microstates                | Pair-level microstate assignments         |
| Build Physics Features           | Electrostatic proxy features              |
| Build Conformational States      | Catalogs experimental conformations       |
| Build Microstate Refinement      | Protonation-policy planning               |
| Build MM Job Manifests           | OpenMM job manifests                      |
| Run MM Jobs                      | Dispatch OpenMM jobs                      |
| Run Site-Centric Feature Pipeline| New artifacts/ feature pipeline           |
| Export Analysis Queue            | Motif queues for external ORCA/APBS       |
| Ingest Physics Results           | Import ORCA/APBS/OpenMM outputs           |
| Train Site-Physics Surrogate     | Train a deterministic surrogate model     |

### Run Full Pipeline (one click)

Click **Run Full Pipeline** at the bottom of the pipeline panel. This runs
every stage in sequence:

1. Ingest (with confirmation dialogs for downloads)
2. Extract, Normalize, Audit, Report
3. All ML Pipeline stages based on your pipeline mode selection

The pipeline stops on the first error. Status indicators next to each button
update in real time.

---

## Demo Mode

The **Demo Mode** checkbox is in the top-right corner of the header bar. It
controls which sections of the Data Overview panel are visible.

### What Demo Mode does

When Demo Mode is **off** (the default), all Data Overview sections are shown —
file counts, review exports, health summaries, training builder, curation
review, completion status, artifact freshness, and everything else.

When Demo Mode is **on**, the overview is trimmed to only the sections that
matter for a live walkthrough or presentation:

| Visible in Demo Mode          | Description                                    |
|-------------------------------|------------------------------------------------|
| Presenter Banner              | High-level headline and suggested talking point|
| Completion Status             | Progress grid showing each area vs. target     |
| Training Quality              | Training set quality metrics                   |
| Model Comparison              | Model performance summary                      |
| Search Preview                | RCSB search result preview                     |
| Source Configuration          | Which sources are enabled and their status     |
| Data Integrity                | File health and integrity checks               |
| Release Readiness             | Whether the dataset is release-ready           |
| Risk Context                  | Pathway risk context summary                   |
| Prediction Status             | Prediction output status                       |
| Workflow Guidance              | Recommended next steps                         |
| Demo Readiness                | Readiness assessment with walkthrough steps    |
| Last Run                      | Summary of the most recent pipeline execution  |
| Artifact Freshness            | Staleness check for key outputs                |
| Quick Actions                 | Buttons to open key artifact files             |

Sections **not** in that list (raw file counts, detailed review CSVs, curation
drill-downs) are hidden so the screen stays clean during a presentation.

### Demo Readiness panel

Whether or not Demo Mode is active, the **Demo Readiness** section in the Data
Overview panel shows an automated assessment of your workspace:

- **Readiness level** — one of three states:
  - `ready_for_internal_demo` — the workspace has extracted data, no hard
    blockers, and at most two minor warnings. Suitable for a customer-facing
    baseline walkthrough.
  - `technically_reviewable_not_polished` — the workspace can be shown for a
    technical review, but unfinished areas should be called out explicitly as
    work in progress.
  - `not_demo_ready` — hard blockers exist (e.g. no extracted entries, missing
    required dependencies). Finish the blockers before presenting.

- **Summary** — a one-line description of the current state.
- **Customer message** — guidance on how to frame this workspace externally.
- **Blockers** — hard blockers that must be resolved (e.g.
  `missing_required_dependencies`, `no_extracted_entries`).
- **Warnings** — non-blocking issues (e.g. `graph_exports_missing`,
  `baseline_model_missing`, `release_snapshot_missing`).
- **Recommended walkthrough** — a numbered list of steps for the presenter:
  1. Run `pbdata status` to show the dataset/build state.
  2. Run `pbdata doctor` to show dependency readiness.
  3. Show graph, feature, and training example outputs if artifacts are present.
  4. Use prediction/risk outputs as baseline demonstrations, not scientific
     claims.

- **Ground rules** — assumptions the report is built on:
  - Prediction and risk outputs are baselines unless a trained artifact is
    explicitly present.
  - The report measures workspace demo readiness, not scientific validity of
    downstream conclusions.

### Export Demo Snapshot

Click **Export Demo Snapshot** in the Demo Readiness panel (or run
`pbdata export-demo-snapshot` from the CLI). This writes two files to the
feature reports directory:

- `demo_readiness.json` — the full readiness report as structured JSON.
- `demo_walkthrough.md` — a Markdown document with the readiness level,
  blockers, warnings, recommended walkthrough steps, and ground rules. Ready
  to share with colleagues or attach to a meeting invite.

Click **Open Demo Walkthrough** to open the generated Markdown file directly.

---

## Data overview panel

The Data Overview panel (top of the right column) shows at a glance:

| Metric            | Source                                |
|-------------------|---------------------------------------|
| Raw RCSB entries  | `data/raw/rcsb/*.json`                |
| SKEMPI CSV        | `data/raw/skempi/skempi_v2.csv`       |
| Processed records | `data/processed/rcsb/*.json`          |
| Extracted entries | `data/extracted/entry/*.json`         |
| Chains            | `data/extracted/chains/*.json`        |
| Bound objects     | `data/extracted/bound_objects/*.json`  |
| Assay records     | `data/extracted/assays/*.json`        |
| Graph nodes/edges | `data/graph/graph_*`                  |
| Split files       | `data/splits/*.txt`                   |

Below the counts are several dashboard sections:

- **Root Review Exports** — paths to the master CSVs with a refresh button.
- **Release Artifacts** — links to model-ready CSV, custom training set,
  scorecard, manifest, and coverage summary.
- **Training Set Builder** — KPI tiles, workflow progress pills, and a
  one-click Run Training Set Workflow button.
- **Curation Review** — top exclusion reasons, conflict bands, issue types.
- **Review Health** — release readiness assessment with recommended next action.
- **Demo Readiness** — workspace demo readiness (see [Demo Mode](#demo-mode)).
- **Quick Actions** — buttons to open any artifact file directly.

---

## Review and curation dashboards

### Review Health

Computed from `scientific_coverage_summary.json`:
- **Release readiness:** Not ready / Needs review / Partially ready /
  Release-ready.
- **Recommended next step:** actionable guidance based on current blockers.

### Training Set Builder

Updated from `custom_training_scorecard.json` and split benchmark:
- KPI tiles: Selected count, Clusters, Mean quality, Max dominance, Excluded.
- Workflow progress pills: Model-ready → Custom set → Scorecard → Benchmark →
  Release.
- One-click **Run Training Set Workflow** button.

### Curation Review

Summarizes exclusions, conflicts, and issues from root CSVs:
- Top exclusion reasons, conflict agreement bands, issue types.
- Quick-open buttons for issues and conflicts CSVs.

---

## Log panel

The bottom panel shows live output from every running stage:

- Dark-themed, monospace font
- Auto-scrolls to the latest output
- Mouse-wheel scrollable
- **Clear Log** button at bottom right

Each stage logs a separator banner when it starts and a `[stage] done/error`
line when it finishes.

---

## Supported data sources

| Source    | Type        | Status            | Data provided                      |
|-----------|-------------|-------------------|------------------------------------|
| RCSB PDB  | Remote API  | Fully implemented | Structural metadata, mmCIF files   |
| ChEMBL    | Remote API  | Fully implemented | Kd, Ki, IC50 bioactivity           |
| BindingDB | Remote API  | Fully implemented | Binding affinity by PDB ID         |
| SKEMPI v2 | Remote CSV  | Fully implemented | Protein-protein mutation ddG       |
| PDBbind   | Local files | Index parser ready| Curated protein-ligand affinities  |
| BioLiP    | Local files | Row parser ready  | Biologically relevant ligands      |

---

## CLI reference

Every GUI action has a CLI equivalent. Run any command with `--help` for
detailed options.

### Core pipeline

```bash
pbdata ingest --source rcsb          # download raw metadata
pbdata ingest --source skempi        # download SKEMPI v2
pbdata preview-rcsb-search           # preview search results before download
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
```

### Physics and site-centric pipeline

```bash
pbdata build-microstates             # microstate assignments
pbdata build-physics-features        # electrostatic features
pbdata run-feature-pipeline          # site-centric feature pipeline
pbdata export-analysis-queue         # ORCA/APBS motif queues
pbdata ingest-physics-results        # import external physics
pbdata train-site-physics-surrogate  # surrogate model training
```

### Workspace utilities

```bash
pbdata status                        # data snapshot
pbdata doctor                        # dependency + config check
pbdata demo-readiness                # demo readiness assessment
pbdata export-demo-snapshot          # export demo walkthrough files
pbdata setup-workspace               # create workspace directories
pbdata harvest-metadata              # build unified metadata table
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
    rcsb/              Raw RCSB GraphQL JSON responses
    skempi/            SKEMPI v2 CSV download
  processed/
    rcsb/              Canonical schema records (JSON)
  extracted/
    entry/             Entry metadata
    chains/            Protein chains with sequences
    bound_objects/     Ligands, ions, cofactors, glycans
    interfaces/        Binding interfaces
    assays/            Binding affinities
    provenance/        Source hashes and timestamps
  structures/
    rcsb/              Downloaded mmCIF structure files
  graph/               Graph nodes and edges
  features/            Computed features
  splits/              train.txt, val.txt, test.txt
  audit/               Quality audit outputs
  reports/             Summary and bias reports
  identity/            Protein / ligand / pair crosswalk exports
  models/              Trained model artifacts
  releases/            Versioned release snapshots
  custom_training_sets/ Curated training subsets
  catalog/             Download manifest, stage/source state
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

| File                    | Purpose                                                          |
|-------------------------|------------------------------------------------------------------|
| `configs/criteria.yaml` | RCSB search criteria (methods, resolution, task types, filters)  |
| `configs/sources.yaml`  | Enabled sources, local paths, structure mirror, storage root     |

Both are auto-saved by the GUI when you click their Save buttons. You can also
edit them by hand — they are plain YAML.

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
storage_root: .
```

---

## Testing

```bash
# Run the full unit test suite
.venv\Scripts\python.exe -m pytest               # Windows
python -m pytest                                  # macOS / Linux

# Run in shorter sequential shards (more stable on some environments)
python scripts/run_test_shards.py

# Integration tests (require network access)
python -m pytest -m integration

# Single test file
python -m pytest tests/test_extract_pipeline.py -v
```

The test suite includes three immutable stress test panels (32 adversarial PDB
entries) that validate classification logic against biological ground truth.
These panels and their expected outcomes are read-only — they must never be
modified to make tests pass.

---

## Troubleshooting

**GUI won't start / Tkinter not found:**
Tkinter ships with standard Python on Windows and macOS. On Linux, install
`python3-tk` (e.g. `sudo apt install python3-tk`).

**`ModuleNotFoundError: No module named 'pbdata'`:**
Run `pip install -e ".[dev]"` from the repo root.

**Stages crash with `ModuleNotFoundError: torch`:**
Torch is optional. The core pipeline (ingest, extract, normalize, audit,
report, splits, custom training set, release) works without it. Install torch
only if you need structural graphs or dataset engineering.

**"No sources enabled" error on ingest:**
Open the Sources tab, tick at least one source checkbox, and click Save Source
Config.

**Ingest shows 0 matching entries:**
Your search criteria may be too restrictive. Try widening the resolution limit,
enabling more experimental methods, or adding direct PDB IDs.

**Data appears in the wrong directory:**
The storage root defaults to the current working directory. Set an explicit
storage root in the Options tab or use
`pbdata --storage-root /path/to/workspace <command>`.

**Empty assay tables after extraction:**
Assay data comes from ChEMBL, BindingDB, and SKEMPI — not from RCSB alone.
Enable at least one affinity source in the Sources tab to populate assay
records.

**"No metadata rows found" from engineer-dataset:**
Run `pbdata harvest-metadata` first. This requires extracted data from a prior
`pbdata extract` run.
