# pbdata — Protein Binding Dataset Platform

A Python toolkit for constructing, auditing, and versioning protein-binding
datasets for machine learning.  It ingests raw structural and affinity data
from multiple public databases, normalizes every record into a canonical
schema, extracts multi-table records with full provenance, scores data
quality, and produces reproducible train/val/test splits that guard against
sequence-identity leakage.

The entire pipeline is accessible through a **Tkinter desktop GUI** or
a **Typer CLI**. Most users will work exclusively through the GUI.

---

## Table of contents

1. [What it does](#what-it-does)
2. [Installation](#installation)
3. [GUI — primary interface](#gui--primary-interface)
   - [Launching the GUI](#launching-the-gui)
   - [Layout overview](#layout-overview)
   - [Sources tab](#sources-tab)
   - [Search Criteria tab](#search-criteria-tab)
   - [Pipeline Options tab](#pipeline-options-tab)
   - [Pipeline panel (right column)](#pipeline-panel-right-column)
   - [Data overview and review health](#data-overview-and-review-health)
   - [Review exports and quick actions](#review-exports-and-quick-actions)
   - [Log panel](#log-panel)
   - [Typical GUI workflow](#typical-gui-workflow)
4. [Supported data sources](#supported-data-sources)
5. [CLI reference](#cli-reference)
6. [Multi-table extraction pipeline](#multi-table-extraction-pipeline)
7. [Canonical schema](#canonical-schema)
8. [Quality scoring](#quality-scoring)
9. [Dataset splitting](#dataset-splitting)
10. [Configuration](#configuration)
11. [Prediction, risk, and QA layers](#prediction-risk-and-qa-layers)
12. [Agent handoff system](#agent-handoff-system)
13. [Specification documents](#specification-documents)
14. [Project layout](#project-layout)
15. [Testing](#testing)
16. [Development](#development)
17. [Roadmap](#roadmap)

---

## What it does

```
RCSB PDB ───┐
BindingDB ───┤
ChEMBL ──────┤  ingest → normalize → extract → audit → build-splits
SKEMPI v2 ───┤
PDBbind ─────┤       │            │          │         │
BioLiP ──────┘  raw JSON     canonical   6-table   train.txt
                             JSON       records   val.txt
                                        (JSON)    test.txt
```

| Stage | What happens |
|-------|--------------|
| **Ingest** | Queries source APIs or downloads bulk files → `data/raw/<source>/` |
| **Normalize** | Maps raw records to `CanonicalBindingSample` → `data/processed/<source>/` |
| **Extract** | Produces 6 linked output tables (entry, chain, bound_object, interface, assay, provenance). Downloads mmCIF structure files with SHA-256 hashing |
| **Audit** | Computes per-record quality flags and a `quality_score` in [0, 1] |
| **Report** | Generates summary statistics JSON |
| **Build-splits** | Assigns records to train/val/test using k-mer Jaccard clustering |

Additional ML pipeline stages (graph building, feature engineering, training
example assembly, custom training sets, release snapshots) are accessible from
the GUI and CLI.

### Extended pipeline (layers 5–7)

| Stage | What happens |
|-------|--------------|
| **Predict (ligand screening)** | Accepts SMILES/SDF/structure input, validates, writes prediction manifest |
| **Predict (peptide binding)** | Accepts structure file, validates existence, writes prediction manifest |
| **Score pathway risk** | Matches targets against dataset pairs, computes severity scores |
| **Run scenario tests** | Self-test against QA scenario templates, verifies output completeness |
| **Report bias** | Analyzes dataset composition: resolution bins, method balance, scaffold diversity |
| **Build conformational states** | Materializes experimental + predicted conformational state records |
| **Build features** | Physics-based, molecular-mechanics, microstate, and pathway features |
| **Build graph** | Knowledge graph with protein/ligand/pathway nodes and interaction edges |
| **Build training examples** | Assembles feature + graph + assay data into ML-ready records |
| **Site-centric feature pipeline** | Runs site extraction, physics surrogates, and graph materialization |

---

## Installation

Requires **Python 3.11+**.

```bash
# 1. Clone the repo
git clone https://github.com/jfvitas/bio-agent-lab.git
cd bio-agent-lab

# 2. Create a virtual environment
python -m venv .venv

# Windows
.venv\Scripts\activate
# macOS / Linux
source .venv/bin/activate

# 3. Install the package and dev dependencies
pip install -e ".[dev]"

# 4. (Optional) Scaffold data directories
python bootstrap_repo.py
```

---

## GUI — primary interface

The GUI is the recommended way to use pbdata. It provides full control over
every pipeline stage, configuration option, and review artifact without
touching the command line.

### Launching the GUI

```bash
pbdata-gui
```

Or directly:

```bash
python -m pbdata.gui
```

### Layout overview

```
┌─────────────────────────────────────────────────────────────┐
│  pbdata — Protein Binding Dataset Platform                  │  ← Header
├───────────────────────┬─────────────────────────────────────┤
│  ┌─────────────────┐  │  Data Overview          [counts]    │
│  │ Sources          │  │  ──────────────────────────────── │
│  │ Search Criteria  │  │  Root Review Exports    [paths]    │
│  │ Pipeline Options │  │  Release Artifacts      [paths]    │
│  │                  │  │  Review Health          [status]   │
│  │  (tabbed config  │  │  ──────────────────────────────── │
│  │   notebook)      │  │  Pipeline Stages                  │
│  │                  │  │   ▸ Data Acquisition               │
│  │                  │  │   ▸ Processing                     │
│  │                  │  │   ▸ Quality & Analysis             │
│  │                  │  │   ▸ ML Pipeline                    │
│  └─────────────────┘  │              [Run All Pipeline]     │
├───────────────────────┴─────────────────────────────────────┤
│  $ Live log output...                                       │  ← Log panel
│  $ Streaming subprocess stdout...                           │
└─────────────────────────────────────────────────────────────┘
```

The window auto-scales to your screen (recommended 980–1280 px width).

### Sources tab

Configure which biological databases are enabled and where local files live.

| Control | Description |
|---------|-------------|
| **RCSB PDB** checkbox | Enable RCSB structural data (Search API + GraphQL) |
| **BindingDB** checkbox + path | Enable binding affinity enrichment; optional local cache directory |
| **ChEMBL** checkbox | Enable bioactivity enrichment (Ki/Kd/IC50 by UniProt + InChIKey) |
| **SKEMPI v2** checkbox + path | Enable protein-protein mutation ddG data; optional local CSV path |
| **PDBbind** checkbox + path | Enable curated protein-ligand affinities (requires manual download) |
| **BioLiP** checkbox + path | Enable biologically relevant ligand-protein data (local flat files) |
| **Storage root** | Base directory for all data (default: current working directory) |
| **Save** button | Persists settings to `configs/sources.yaml` |

Each source shows a one-line description and ingest note explaining when
data is fetched (at ingest time vs. during extract enrichment).

### Search Criteria tab

Controls which RCSB PDB entries match your query. All fields are optional
and combine as AND filters.

**Identity filters:**

| Control | Description |
|---------|-------------|
| PDB IDs | Comma-separated list of specific PDB IDs to fetch (bypasses search) |
| Keyword query | Free-text search against RCSB metadata |
| Organism name | Filter by organism (e.g. "Homo sapiens") |
| Taxonomy ID | NCBI taxonomy ID |

**Structural filters:**

| Control | Description |
|---------|-------------|
| Task types | Checkboxes: protein_ligand, protein_protein, mutation_ddg |
| Experimental methods | Checkboxes: X-ray, EM, NMR, Neutron |
| Max resolution | Dropdown: 1.5 Å to 5.0 Å (or unlimited) |
| Membrane only | Restrict to membrane proteins |
| Require multimer | Require multi-chain assemblies |
| Require protein | At least one protein entity (default: on) |
| Require ligand | At least one non-polymer ligand |
| Require branched entities | At least one carbohydrate/glycan |

**Entity count filters:**

| Control | Description |
|---------|-------------|
| Min protein entities | Minimum polymer chain count |
| Min/Max nonpolymer entities | Ligand count range |
| Min/Max branched entities | Glycan count range |
| Min/Max assembly count | Biological assembly range |
| Max atom count | Upper limit on deposited atoms |

**Date filters:**

| Control | Description |
|---------|-------------|
| Min release year | Earliest PDB release year |
| Max release year | Latest PDB release year |

**Review filters** (for filtering review exports in-GUI):

| Control | Description |
|---------|-------------|
| PDB query | Filter review rows by PDB ID substring |
| Pair query | Filter by pair identity key substring |
| Issue type | Dropdown: missing_structure_file, no_assay_data, non_high_confidence_fields, etc. |
| Confidence level | Dropdown: All, Non-high, Medium, Low |
| Toggles | Conflict only, Mutation ambiguous only, Metal only, Cofactor only, Glycan only |

Clicking **Save** persists to `configs/criteria.yaml`. Clicking **Apply Filters**
writes a filtered review CSV for quick inspection.

### Pipeline Options tab

Controls how pipeline stages execute.

**General options:**

| Control | Default | Description |
|---------|---------|-------------|
| Storage root | cwd | Base directory (with folder picker button) |
| Download structures | on | Download mmCIF files during extract |
| Download PDB format | off | Also download legacy PDB format |
| Workers | 1 | Parallelization level |

**Split options:**

| Control | Default | Description |
|---------|---------|-------------|
| Split mode | auto | auto, pair-aware, legacy-sequence, or hash |
| Train fraction | 0.70 | Target training set fraction |
| Validation fraction | 0.15 | Target validation fraction |
| Random seed | 42 | Reproducibility seed |
| Jaccard threshold | 0.30 | Sequence similarity clustering threshold |
| Hash-only | off | Skip clustering, use deterministic hash split |

**Release options:**

| Control | Default | Description |
|---------|---------|-------------|
| Release tag | (empty) | Tag for the release snapshot (e.g. "v1.0") |

**Custom training set options:**

| Control | Default | Description |
|---------|---------|-------------|
| Mode | generalist | generalist, protein_ligand, protein_protein, mutation_effect, high_trust |
| Target size | 500 | Desired number of training examples |
| Seed | 42 | Sampling seed |
| Per-receptor cluster cap | 1 | Max examples per receptor cluster |

### Pipeline panel (right column)

Pipeline stages are grouped into four phases. Each stage has a colored status
indicator and a **Run** button.

**Data Acquisition:**
- **Ingest Sources** — Downloads raw data from enabled sources. For RCSB, queries
  the entry count first and shows a confirmation dialog before downloading.
  Warns if >5,000 entries match. Supports both search-based and direct PDB ID input.

**Processing:**
- **Extract Multi-Table** — Produces 6 linked output tables with optional mmCIF downloads
- **Normalize Records** — Converts raw records to canonical CanonicalBindingSample JSON

**Quality & Analysis:**
- **Audit Quality** — Scores quality flags and computes quality_score [0, 1]
- **Generate Report** — Writes summary statistics

**ML Pipeline:**
- **Build Graph** — Constructs protein interaction graph
- **Build Microstates** — Computes conformational microstates
- **Build Physics Features** — Extracts physics-based descriptors
- **Build Features** — Aggregates all feature types
- **Build Training Examples** — Assembles complete training records
- **Build Splits** — Creates train/val/test splits with sequence clustering
- **Build Custom Training Set** — Produces mode-specific training subsets
- **Build Release Snapshot** — Creates versioned release artifacts

The **Run All Pipeline** button executes every stage sequentially. A lock
prevents concurrent pipeline runs.

### Data overview and review health

The top of the right column shows live counts that refresh after each stage:

- Raw RCSB records, SKEMPI CSV presence, processed records
- Extracted entries, chains, bound objects, assays
- Graph nodes/edges, split files

**Review Health** displays:
- Release readiness status (Not ready / Needs review / Partially ready / Release-ready)
- Coverage snapshot (entries, pairs, model-ready counts, structures)
- Quality snapshot (conflicts, non-high-confidence issues, missing structures)
- Recommended next action

### Review exports and quick actions

**Root Review Exports** shows paths to the master CSV artifacts:
- `master_pdb_repository.csv` — full entry-level index
- `master_pdb_pairs.csv` — pair-level records
- `master_pdb_issues.csv` — issue log
- `master_pdb_conflicts.csv` — conflict tracker
- `master_source_state.csv` — source processing state

**Refresh Root Exports** regenerates these from extracted data.
**Open Repo Root** opens the project folder in your file manager.

**Release Artifacts** shows paths to:
- Model-ready pairs CSV
- Custom training set CSV
- Release manifest JSON
- Split summary CSV
- Scientific coverage JSON
- Latest release snapshot directory

**Quick Actions** provides one-click buttons to open:
- Filtered review CSV
- Model-ready pairs
- Custom training set
- Coverage summary
- Storage root directory

### Log panel

A dark-themed (`#1e1e1e` background) scrollable text area at the bottom that
streams real-time output from all pipeline stages. Subprocess stdout is
captured line-by-line. The log is read-only — output is appended automatically.

### Typical GUI workflow

1. **Configure sources** — Open the Sources tab, enable RCSB (and optionally
   other sources), set the storage root, click **Save**.

2. **Set search criteria** — Open the Search Criteria tab, choose task types
   (protein_ligand, protein_protein), experimental methods (X-ray, EM),
   resolution limit, and any organism/keyword filters. Click **Save**.

3. **Ingest** — Click **Ingest Sources** in the pipeline panel. The GUI
   queries the entry count and shows a confirmation dialog. Approve to
   begin downloading.

4. **Extract** — Click **Extract Multi-Table** to produce the 6-table output.
   mmCIF structure files are downloaded and hashed by default.

5. **Normalize + Audit** — Click each button or use **Run All Pipeline**
   to execute everything sequentially.

6. **Review** — Check the Data Overview counts and Review Health status.
   Use the review filters in the Search Criteria tab to investigate issues.
   Click **Refresh Root Exports** to regenerate review CSVs.

7. **Build splits** — Click **Build Splits** to create train/val/test
   assignments with sequence-identity-aware clustering.

8. **Release** — Set a release tag in Pipeline Options, then click
   **Build Release Snapshot** to produce a versioned artifact directory.

---

## Supported data sources

| Source | Type | Adapter | Notes |
|--------|------|---------|-------|
| [RCSB PDB](https://www.rcsb.org) | Structural | `rcsb.py` + `rcsb_search.py` + `rcsb_classify.py` | Search API, GraphQL batch, mmCIF download, chem-comp SMILES/InChIKey enrichment. 870+ lines of entity classification logic |
| [BindingDB](https://www.bindingdb.org) | Affinity | `bindingdb.py` | REST API by PDB ID; Ki/Kd/IC50/EC50 → nM conversion; 0.35s rate limiting |
| [ChEMBL](https://www.ebi.ac.uk/chembl/) | Affinity | `chembl.py` | REST API by UniProt accession + InChIKey; enrichment-only (not primary ingest) |
| [SKEMPI v2](https://life.bsc.es/pid/skempi2/) | Mutation ddG | `skempi.py` | Downloads bulk CSV; computes ddG = RT·ln(Kd_mut/Kd_wt); temperature-aware |
| [BioLiP](https://zhanggroup.org/BioLiP/) | Structural | `biolip.py` | Local flat-file parser; binding site residues, EC numbers, GO terms |
| [PDBbind](https://www.pdbbind-plus.org.cn/) | Affinity | `pdbbind.py` | Local INDEX file parser; requires manual download and registration |

All adapters inherit from `BaseAdapter` and produce `CanonicalBindingSample` records.

---

## CLI reference

The CLI is available for scripting and automation. All commands are also
accessible from the GUI.

All commands share optional global flags:

| Flag | Description |
|------|-------------|
| `--config PATH` | Path to sources YAML config (default: `configs/sources.yaml`) |
| `--log-config PATH` | Path to logging YAML config (default: `configs/logging.yaml`) |

### `pbdata ingest`

Downloads raw data from a source database.

| Flag | Default | Description |
|------|---------|-------------|
| `--source` | `rcsb` | Data source: `rcsb` or `skempi` |
| `--dry-run` | off | Count entries only; do not download |
| `--yes / -y` | off | Skip confirmation prompt |
| `--criteria PATH` | `configs/criteria.yaml` | Search criteria YAML (RCSB only) |
| `--output PATH` | `data/raw/<source>/` | Override output directory |

### `pbdata normalize`

Normalizes raw RCSB records into canonical JSON. Batch-fetches SMILES and
InChIKey for all unique ligand comp_ids.

### `pbdata extract`

Produces six linked output tables with optional structure file downloads.

| Flag | Default | Description |
|------|---------|-------------|
| `--output PATH` | `data/extracted/` | Override output directory |
| `--structures PATH` | `data/structures/rcsb/` | Override structures directory |
| `--download-pdb` | off | Also download PDB format files |
| `--download-structures / --no-download-structures` | on | Download mmCIF files |

### `pbdata audit`

Computes `quality_flags` and `quality_score` for every normalized record.

### `pbdata report`

Generates `data/reports/summary.json` with task-type counts, method
distribution, resolution statistics, quality scores, and field coverage.

### `pbdata build-splits`

Writes `data/splits/train.txt`, `val.txt`, `test.txt`, and `metadata.json`.

| Flag | Default | Description |
|------|---------|-------------|
| `--train-frac` | `0.70` | Target training fraction |
| `--val-frac` | `0.15` | Target validation fraction |
| `--seed` | `42` | Reproducibility seed |
| `--hash-only` | off | Use fast hash split (skips clustering) |
| `--threshold` | `0.30` | Jaccard similarity threshold for clustering |

### `pbdata predict-ligand-screening`

Runs ligand off-target screening prediction workflow.

| Flag | Description |
|------|-------------|
| `--smiles` | SMILES string for the candidate ligand |
| `--sdf` | Path to SDF file |
| `--structure-file` | Path to structure file (CIF/PDB) |
| `--fasta` | FASTA sequence |

### `pbdata predict-peptide-binding`

Runs peptide binding prediction workflow.

| Flag | Description |
|------|-------------|
| `--structure-file` | Path to structure file (CIF/PDB) |

### `pbdata score-pathway-risk`

Scores pathway activation risk for specified targets.

| Flag | Description |
|------|-------------|
| `--targets` | Comma-separated UniProt accessions |

### `pbdata run-scenario-tests`

Runs QA scenario templates and writes a scenario test report.

### `pbdata report-bias`

Analyzes dataset composition bias (resolution, method, scaffold diversity).

### `pbdata build-conformational-states`

Materializes conformational state records from extracted data.

### `pbdata build-graph`

Constructs the protein interaction knowledge graph (nodes + edges).

### `pbdata run-feature-pipeline`

Runs the site-centric feature pipeline (site extraction, physics surrogates, graph materialization).

### `pbdata ingest-physics-results`

Ingests external physics analysis results (ORCA/APBS/OpenMM).

### `pbdata train-site-physics-surrogate`

Trains a linear surrogate model over site environment descriptors.

### `pbdata train-baseline-model`

Trains a baseline affinity prediction model.

### `pbdata evaluate-baseline-model`

Evaluates a trained baseline model on the test split.

### Quick start (CLI)

```bash
# Count RCSB entries matching criteria (no download)
pbdata ingest --dry-run

# Download matching RCSB entries
pbdata ingest --yes

# Download SKEMPI v2 mutation dataset
pbdata ingest --source skempi

# Normalize, extract, audit, report, split
pbdata normalize
pbdata extract
pbdata audit
pbdata report
pbdata build-splits
```

### Python API

```python
from pathlib import Path
from pbdata.sources.rcsb import RCSBAdapter
from pbdata.pipeline.extract import extract_rcsb_entry
from pbdata.quality.audit import audit_record
from pbdata.dataset.splits import cluster_aware_split

# Fetch and normalize a single PDB entry
adapter = RCSBAdapter()
raw = adapter.fetch_metadata("1ATP")
record = adapter.normalize_record(raw)

# Score it
audited = audit_record(record)
print(audited.quality_score)   # e.g. 0.875
print(audited.quality_flags)   # e.g. ['no_uniprot_id']

# Multi-table extraction
records = extract_rcsb_entry(raw)
entry = records["entry"]           # EntryRecord
chains = records["chains"]         # list[ChainRecord]
bound_objs = records["bound_objects"]  # list[BoundObjectRecord]
interfaces = records["interfaces"]     # list[InterfaceRecord]
provenance = records["provenance"]     # list[ProvenanceRecord]

# Build cluster-aware splits
sample_ids = ["RCSB_1ATP", "RCSB_2SRC", ...]
sequences  = ["MGSS...", "MASL...", ...]
result = cluster_aware_split(sample_ids, sequences)
print(result.sizes())  # {'train': 700, 'val': 150, 'test': 150}
```

---

## Multi-table extraction pipeline

The `extract` command implements the
[Structure Extraction Agent Spec](STRUCTURE_EXTRACTION_AGENT_SPEC.md),
producing six linked tables:

### EntryRecord (73 fields)

Source/provenance, structural metadata, file provenance (mmCIF path, size,
SHA-256 hash, download URL, timestamp), assembly/oligomerization, organism,
bias/audit fields (resolution bin, metal/cofactor/glycan/covalent/peptide
presence), quality flags.

### ChainRecord (26 fields)

Per-chain polymer identity with subtype classification (protein/peptide/
DNA/RNA), UniProt cross-reference, taxonomy, organism, copy number in
assembly.

### BoundObjectRecord (45 fields)

Component identity (CCD ID, name, SMILES, InChIKey, formula, molecular
weight), type classification (small_molecule/metal/cofactor/peptide/glycan/
nucleic_acid/crystallization_additive), role assignment (primary_binder/
co_binder/catalytic_cofactor/metal_mediator/likely_additive), covalent
warhead detection, metal-specific and glycan-specific fields, ligand
chemistry descriptors for bias analysis.

### InterfaceRecord (27 fields)

Protein-protein and protein-ligand interface fields. Binding site residues
from BioLiP when available.

### AssayRecord (33 fields)

Binding affinity type/value/unit/log10, delta_g/delta_delta_g, kon/koff,
assay conditions (temperature, pH, buffer, ionic strength), mutation
annotations, measurement source references. Cross-source merge with
pair-aware grouping and conflict detection.

### ProvenanceRecord (9 fields)

Per-field provenance trail: source name, extraction method, raw/normalized
values, confidence, timestamp.

### File download policy

- mmCIF is the required primary format (`.cif`)
- PDB (`.pdb`) is an optional compatibility fallback
- Files are saved to `data/structures/rcsb/` with SHA-256 hashing
- File provenance fields track path, size, hash, download URL, and timestamp

### Entity classification

The pipeline (via `rcsb_classify.py`, 870+ lines) distinguishes:

| Entity type | Detection method |
|-------------|-----------------|
| **Proteins** | Polypeptides > 30 residues |
| **Peptides** | Polypeptides ≤ 30 residues |
| **Small molecules** | Organic non-cofactor ligands |
| **Cofactors** | ~50 curated biochemical cofactors (ATP, NAD, FAD, etc.) |
| **Metal ions** | 70+ curated metal/halide comp_ids |
| **Glycans** | Monosaccharides, polysaccharides, branched entities |
| **Additives** | ~60 crystallization artifacts excluded from analysis |
| **Nucleic acids** | DNA, RNA polymers |

Covalent binders are detected via SMILES reactive-group patterns and
entry title keywords. Membrane context is detected via struct_keywords.

---

## Canonical schema

Every normalized record is a `CanonicalBindingSample` (Pydantic v2, frozen).

### Identity fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `sample_id` | `str` | Yes | Globally unique internal ID |
| `task_type` | `str` | Yes | `protein_ligand` \| `protein_protein` \| `mutation_ddg` |
| `source_database` | `str` | Yes | Source name |
| `source_record_id` | `str` | Yes | Native ID in source database |
| `pdb_id` | `str` | — | 4-character PDB accession |

### Structural fields

| Field | Type | Description |
|-------|------|-------------|
| `title` | `str` | Structure title |
| `chain_ids_receptor` | `list[str]` | Auth chain IDs for receptor |
| `chain_ids_partner` | `list[str]` | Auth chain IDs for partner |
| `sequence_receptor` | `str` | Canonical amino-acid sequence |
| `sequence_partner` | `str` | Partner sequence |
| `uniprot_ids` | `list[str]` | UniProt accessions |
| `taxonomy_ids` | `list[int]` | NCBI taxonomy IDs |
| `experimental_method` | `str` | e.g. `X-RAY DIFFRACTION` |
| `structure_resolution` | `float` | Resolution in Angstroms |
| `release_date` | `str` | PDB release date |
| `deposit_date` | `str` | PDB deposit date |
| `deposited_atom_count` | `int` | Total atom count |

### Extended structural fields

| Field | Type | Description |
|-------|------|-------------|
| `bound_objects` | `list[dict]` | All bound entities (ligands, metals, cofactors, glycans, peptides) |
| `interfaces` | `list[dict]` | Pairwise polymer-polymer interfaces |
| `assembly_info` | `dict` | Biological assembly metadata |
| `oligomeric_state` | `str` | e.g. `monomer`, `homodimer`, `heterodimer` |
| `is_homo_oligomeric` | `bool` | All chains same entity? |
| `polymer_entity_count` | `int` | Total polymer chain instances |

### File provenance fields

| Field | Type | Description |
|-------|------|-------------|
| `structure_file_cif_path` | `str` | Path to downloaded mmCIF |
| `structure_file_cif_size_bytes` | `int` | mmCIF file size |
| `structure_file_pdb_path` | `str` | Path to downloaded PDB (if any) |
| `structure_file_pdb_size_bytes` | `int` | PDB file size |
| `parsed_structure_format` | `str` | Format used for parsing |
| `structure_download_url` | `str` | Download URL |
| `structure_downloaded_at` | `str` | Download timestamp |
| `structure_file_hash_sha256` | `str` | SHA-256 hash of mmCIF file |

### Quality metadata

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `provenance` | `dict` | Yes | Must contain `ingested_at` |
| `quality_flags` | `list[str]` | Yes | Quality warning codes |
| `quality_score` | `float` | Yes | Score in [0.0, 1.0] |

---

## Quality scoring

The audit step assigns each record a `quality_score` (0–1) and a list
of quality/ambiguity flags:

| Flag | Meaning |
|------|---------|
| `no_resolution` | Resolution field absent |
| `low_resolution` | Resolution > 3.5 Å |
| `very_low_resolution` | Resolution > 4.5 Å |
| `no_experimental_method` | Method field absent |
| `missing_sequence_receptor` | Receptor sequence absent |
| `missing_sequence_partner` | Partner sequence absent |
| `no_uniprot_id` | No UniProt accession |
| `no_chain_ids` | No receptor chain IDs |
| `missing_ligand_id` | No ligand comp_id |
| `metal_present` | Metal ions in structure |
| `metal_mediated_binding_possible` | Metal may mediate binding |
| `cofactor_present` | Biochemical cofactor present |
| `glycan_present` | Glycan entities present |
| `covalent_binder` | Covalent warhead detected |
| `peptide_partner` | Peptide binding partner |
| `multiple_bound_objects` | Multiple non-artifact bound objects |
| `homomeric_symmetric_interface` | Symmetric homo-oligomeric interface |
| `heteromeric_interface` | Heteromeric polymer interface |
| `assembly_ambiguity` | Multiple assemblies annotated |
| `membrane_protein_context` | Membrane protein detected |
| `possible_crystallization_additive` | Likely buffer/crystallization artifact |

---

## Dataset splitting

`build-splits` uses **k-mer Jaccard sequence-identity clustering** by default
to prevent data leakage between train and test sets.

### Algorithm

1. Compute 5-gram k-mer sets for every receptor sequence.
2. Build an inverted index for fast candidate lookup.
3. Greedy single-linkage clustering with Jaccard threshold (default 0.30).
4. Sort clusters largest-first, greedily fill train → val → test.

Whole clusters are assigned atomically. Records without sequences fall
back to deterministic MD5 hash-based assignment.

### Output files

```
data/splits/
  train.txt        # one sample_id per line
  val.txt
  test.txt
  metadata.json    # seed, strategy, sizes, fractions, created_at
```

---

## Configuration

### Search criteria — `configs/criteria.yaml`

Controls which RCSB entries are fetched during ingest. Editable from the
GUI's Search Criteria tab.

```yaml
experimental_methods:
  - xray    # X-RAY DIFFRACTION
  - em      # ELECTRON MICROSCOPY
max_resolution_angstrom: 3.0
task_types:
  - protein_ligand
  - protein_protein
require_protein: true
min_release_year: null
```

### Sources — `configs/sources.yaml`

Editable from the GUI's Sources tab.

```yaml
sources:
  rcsb:
    enabled: true
  bindingdb:
    enabled: false
  chembl:
    enabled: false
  pdbbind:
    enabled: false
  biolip:
    enabled: false
  skempi:
    enabled: false
```

---

## Prediction, risk, and QA layers

The platform includes scaffold implementations for layers 5–7 of the
bio-agent architecture. These modules produce structured manifest files
documenting what a trained model would output, while clearly marking
predictions as unavailable until model training is complete.

### Prediction engine (`src/pbdata/prediction/`)

- **`engine.py`** — Input type detection, SMILES validation, structure file
  existence checks. Orchestrates ligand screening and peptide binding workflows.
- **`ligand_screening.py`** — Ligand off-target screening logic.
- **`peptide_binding.py`** — Peptide binding partner prediction logic.
- **`variant_effects.py`** — Mutation variant effect prediction.

### Risk scoring (`src/pbdata/risk/`)

- **`summary.py`** — Pathway risk summary: matches targets against dataset pairs,
  computes composite risk scores with configurable binding/pathway weights.
- **`pathway_reasoning.py`** — Pathway overlap and activation reasoning.
- **`severity_scoring.py`** — Risk severity level assignment (low/medium/high).

### QA system (`src/pbdata/qa/`)

- **`scenario_runner.py`** — Runs scenario test templates from
  `specs/bio_agent_full_instruction_pack/qa/scenario_test_templates.yaml`.
  Verifies that expected outputs exist and contain non-null values.

### Bias reporting (`src/pbdata/reports/`)

- **`bias.py`** — Analyzes dataset composition: resolution bin distribution,
  experimental method balance, scaffold diversity, organism coverage.

### Conformational states (`src/pbdata/dataset/conformations.py`)

Materializes conformational state records from extracted structural data,
combining experimental states with planned predicted states.

### Baseline models (`src/pbdata/models/`)

- **`affinity_models.py`** — Baseline affinity prediction model (train + evaluate).
- **`off_target_models.py`** — Off-target binding risk model.
- **`baseline_memory.py`** — Model checkpoint and metric persistence.

### Site-centric feature pipeline (`src/pbdata/pipeline/`)

- **`feature_execution.py`** — Full site-centric feature pipeline: site extraction,
  physics proxy computation, graph materialization, training example export.
  Requires `torch` and `gemmi` (optional dependencies).
- **`physics_feedback.py`** — Offline physics results ingest (ORCA/APBS/OpenMM),
  linear surrogate training over site environment descriptors.
- **`enrichment.py`** — Cross-source enrichment helpers.

---

## Agent handoff system

The `handoffs/` directory contains structured review artifacts produced by
specialized agent roles. Each handoff is a markdown file with YAML frontmatter
tracking task ID, role, date, file permissions, required tests, and pass/fail
status.

| Handoff | Role | Summary |
|---------|------|---------|
| `2026-03-09_spec_compliance_review_architect.md` | Architect | 8 architectural issues against master spec |
| `2026-03-09_scenario_test_execution_user_tester.md` | User Tester | 7 usability/correctness problems from scenario execution |
| `2026-03-09_full_codebase_qa_reviewer.md` | QA Reviewer | 4 critical, 4 major, 5 minor issues with severity assessment |

Handoff format follows `specs/AGENT_OUTPUT_REQUIREMENTS.md`.

---

## Specification documents

The `specs/` directory contains authoritative engineering specifications:

| File | Description |
|------|-------------|
| `bio_agent_master_instruction_file.md` | Master engineering spec for the 7-layer architecture |
| `bio_agent_full_instruction_pack/` | Full spec pack: layer definitions, QA rubrics, scenario templates |
| `AGENT_OUTPUT_REQUIREMENTS.md` | Format requirements for agent handoff artifacts |
| `FEATURE_PIPELINE_EXECUTION_SPEC.md` | Site-centric feature pipeline contract |
| `SITE_CENTRIC_PHYSICS_SPEC.md` | Physics-based feature extraction specification |
| `local_physics_agent_pack/` | Local physics computation agent instructions |
| `canonical_schema.yaml` | Canonical schema definition |
| `quality_rules.yaml` | Quality scoring rules |
| `split_policy.yaml` | Dataset splitting policy |
| `source_requirements.md` | Data source adapter requirements |
| `coding_standards.md` | Coding conventions |
| `repo_contract.md` | Repository structure contract |

---

## Project layout

```
bio-agent-lab/
├── configs/
│   ├── criteria.yaml          # RCSB search criteria (GUI-editable)
│   ├── sources.yaml           # enabled data sources (GUI-editable)
│   └── logging.yaml           # logging configuration
├── src/
│   └── pbdata/
│       ├── cli.py                     # Typer CLI (ingest, normalize, extract, audit, report, build-splits)
│       ├── gui.py                     # Tkinter GUI (2,400+ lines)
│       ├── config.py                  # AppConfig loader
│       ├── criteria.py                # SearchCriteria Pydantic model
│       ├── logging_config.py          # Logging setup
│       ├── storage.py                 # Storage layout & file validation
│       ├── master_export.py           # Master CSV export pipeline
│       ├── release_export.py          # Release artifact generation
│       ├── custom_training_set.py     # Mode-specific training set builder
│       ├── pairing.py                 # Pair identity key generation
│       ├── stage_state.py             # Pipeline stage tracking
│       ├── source_state.py            # Source ingestion state
│       ├── catalog.py                 # Entry catalog utilities
│       ├── schemas/
│       │   ├── canonical_sample.py    # CanonicalBindingSample (main schema)
│       │   ├── bound_objects.py       # BoundObject, InterfaceInfo, AssemblyInfo
│       │   ├── records.py            # Multi-table: Entry, Chain, BoundObject, Interface, Assay, Provenance
│       │   ├── features.py           # Feature vector schemas
│       │   ├── graph.py              # Graph node/edge schemas
│       │   └── training_example.py   # Training example schema
│       ├── pipeline/
│       │   ├── extract.py            # Multi-table extraction pipeline
│       │   └── assay_merge.py        # Cross-source assay merge + conflict detection
│       ├── parsing/
│       │   └── mmcif_supplement.py   # mmCIF download, parsing, structure quality
│       ├── sources/
│       │   ├── base.py               # BaseAdapter ABC
│       │   ├── rcsb.py               # RCSB adapter (GraphQL + REST)
│       │   ├── rcsb_search.py        # RCSB Search API + batch fetch
│       │   ├── rcsb_classify.py      # Entity classification (870+ lines)
│       │   ├── bindingdb.py          # BindingDB REST adapter
│       │   ├── chembl.py             # ChEMBL REST adapter
│       │   ├── skempi.py             # SKEMPI v2 CSV adapter
│       │   ├── biolip.py             # BioLiP flat-file adapter
│       │   └── pdbbind.py            # PDBbind INDEX file adapter
│       ├── quality/
│       │   ├── audit.py              # Quality flags + score
│       │   └── stress_panel.py       # Stress panel evaluation helpers
│       ├── dataset/
│       │   ├── splits.py             # k-mer Jaccard clustering splits
│       │   └── conformations.py      # Conformational state materialization
│       ├── features/
│       │   ├── builder.py            # Feature aggregation
│       │   ├── microstate.py         # Conformational microstate features
│       │   ├── mm_features.py        # Molecular mechanics features
│       │   ├── physics_features.py   # Physics-based descriptors
│       │   └── pathway.py            # Pathway connectivity features
│       ├── graph/
│       │   ├── builder.py            # Knowledge graph construction
│       │   ├── connectors.py         # STRING, Reactome, BioGRID connectors
│       │   └── identifier_map.py     # UniProt/Ensembl/Entrez ID mapping
│       ├── prediction/
│       │   ├── engine.py             # Prediction orchestration + input validation
│       │   ├── ligand_screening.py   # Ligand off-target screening
│       │   ├── peptide_binding.py    # Peptide binding prediction
│       │   └── variant_effects.py    # Mutation variant effects
│       ├── risk/
│       │   ├── summary.py            # Pathway risk summary
│       │   ├── pathway_reasoning.py  # Pathway overlap reasoning
│       │   └── severity_scoring.py   # Risk severity levels
│       ├── qa/
│       │   └── scenario_runner.py    # QA scenario test execution
│       ├── reports/
│       │   └── bias.py               # Dataset composition bias analysis
│       ├── models/
│       │   ├── affinity_models.py    # Baseline affinity model
│       │   ├── off_target_models.py  # Off-target binding model
│       │   └── baseline_memory.py    # Model checkpoint persistence
│       ├── data_pipeline/
│       │   ├── extraction.py         # Extraction pipeline helpers
│       │   ├── ingestion.py          # Ingestion pipeline helpers
│       │   └── normalization.py      # Normalization pipeline helpers
│       └── training/
│           └── assembler.py          # Training example assembly
├── specs/
│   ├── bio_agent_master_instruction_file.md
│   ├── bio_agent_full_instruction_pack/   # Full 7-layer spec pack + QA rubrics
│   ├── AGENT_OUTPUT_REQUIREMENTS.md
│   ├── FEATURE_PIPELINE_EXECUTION_SPEC.md
│   ├── SITE_CENTRIC_PHYSICS_SPEC.md
│   └── local_physics_agent_pack/          # Physics computation agent specs
├── handoffs/                              # Agent review artifacts (YAML frontmatter)
│   ├── 2026-03-09_spec_compliance_review_architect.md
│   ├── 2026-03-09_scenario_test_execution_user_tester.md
│   └── 2026-03-09_full_codebase_qa_reviewer.md
├── tests/                             # 297+ unit tests, 71+ integration tests
│   ├── conftest.py                    # Fixtures
│   ├── test_smoke.py                  # Import smoke tests
│   ├── test_schema.py                 # Schema validation
│   ├── test_config.py                 # Config loader
│   ├── test_search.py                 # RCSB search
│   ├── test_extract_pipeline.py       # Multi-table extraction + GUI integration
│   ├── test_assay_merge.py            # Assay merge + conflict detection
│   ├── test_chembl.py                 # ChEMBL adapter
│   ├── test_biolip.py                 # BioLiP adapter
│   ├── test_bindingdb.py              # BindingDB adapter
│   ├── test_pdbbind.py                # PDBbind adapter
│   ├── test_mmcif_supplement.py       # mmCIF parsing
│   ├── test_review_fixes.py           # Splitting & review logic
│   ├── test_feature_builder.py        # Feature engineering
│   ├── test_graph_connectors.py       # Graph connectivity
│   ├── test_identifier_map.py         # ID mapping
│   ├── test_training_assembler.py     # Training pipeline
│   ├── test_master_export.py          # Master CSV export
│   ├── test_release_export.py         # Release artifacts
│   ├── test_custom_training_set.py    # Custom training sets
│   ├── test_structural_edge_cases.py  # Panel A/B: 100+ unit + integration tests
│   ├── test_stress_panel.py           # Panel A stress tests
│   ├── test_stress_panel_c.py         # Panel C: 48 integration tests
│   ├── test_prediction_engine.py      # Prediction engine tests
│   ├── test_risk_scoring.py           # Risk scoring tests
│   ├── test_conformational_state.py   # Conformational state tests
│   ├── test_baseline_memory.py        # Model persistence tests
│   ├── test_feature_execution.py      # Site-centric feature pipeline tests
│   ├── test_physics_feedback.py       # Physics surrogate tests
│   ├── test_mm_features.py            # Molecular mechanics feature tests
│   └── test_full_scope_architecture.py  # Architecture compliance tests
├── data/
│   ├── raw/rcsb/                      # Raw RCSB GraphQL JSON
│   ├── raw/skempi/                    # Raw SKEMPI CSV
│   ├── structures/rcsb/               # Downloaded mmCIF/PDB files
│   ├── processed/rcsb/                # Normalized canonical JSON
│   ├── extracted/                     # Multi-table output (6 subdirs)
│   │   ├── entry/
│   │   ├── chains/
│   │   ├── bound_objects/
│   │   ├── interfaces/
│   │   ├── assays/
│   │   └── provenance/
│   ├── features/                      # Feature vectors
│   ├── graph/                         # Knowledge graph data
│   ├── training_examples/             # Assembled training records
│   ├── splits/                        # train/val/test splits
│   ├── audit/                         # Audit summary
│   ├── reports/                       # Statistics reports
│   ├── conformations/                 # Conformational state records
│   ├── prediction/                    # Prediction manifests (ligand/peptide)
│   ├── qa/                            # Scenario test reports
│   └── risk/                          # Pathway risk summaries
├── docs/
│   ├── bio_agent_full_scope_architecture.md
│   ├── bio_agent_full_scope_gap_analysis.md
│   ├── full_scope_stub_checklist.md
│   ├── structural_edge_cases_report.md
│   ├── mcp_stack_recommendations.md
│   └── release_policy_and_artifacts.md
├── stress_test_panel.yaml             # Panel A: 10 structural edge cases
├── stress_test_panel_B.yaml           # Panel B: 10 adversarial cases
├── stress_test_panel_C.yaml           # Panel C: 12 extended extraction cases
├── expected_outcomes_table.md         # Panel A acceptance criteria
├── expected_outcomes_panel_B.md       # Panel B acceptance criteria
├── expected_outcomes_panel_C.md       # Panel C acceptance criteria
├── STRUCTURE_EXTRACTION_AGENT_SPEC.md # Authoritative extraction spec
├── pyproject.toml                     # Package config
└── README.md
```

---

## Testing

```bash
# Run unit tests only (default, excludes integration)
.venv/Scripts/python.exe -m pytest -q        # Windows
pytest -q                                     # macOS / Linux

# Run integration tests (requires network, fetches live RCSB data)
pytest -m integration -v

# Run all tests
pytest -m "" -v

# Run a specific panel
pytest tests/test_stress_panel_c.py -m integration -v
```

### Test coverage

- **297+ unit tests** — entity classification, bound object detection,
  oligomeric state inference, covalent warhead detection, membrane context,
  quality flags, schema validation, config loading, assay merge, feature
  engineering, graph construction, training assembly, master export,
  release artifacts, custom training sets, GUI integration, prediction engine,
  risk scoring, conformational states, bias reporting, baseline models,
  site-centric feature pipeline, physics surrogates, architecture compliance
- **71+ integration tests** — Panel A (10 entries), Panel B (10 entries),
  Panel C (48 tests across 12 entries covering classification flags,
  source expectations, multi-table extraction, and field coverage)

### Stress test panels

Three panels of real PDB entries validate correctness against biological
ground truth:

| Panel | Entries | Focus |
|-------|---------|-------|
| A | 10 | Core structural edge cases (hemoglobin, kinases, GPCRs) |
| B | 10 | Adversarial complexity (photosystems, metalloenzymes, covalent inhibitors) |
| C | 12 | Extended extraction (glycosylated complexes, large assemblies, metal chelates) |

**Panel files are immutable.** If tests fail, fix the code or assertions,
never the panel files.

---

## Development

```bash
# Install with dev dependencies
pip install -e ".[dev]"

# Run tests
pytest -q

# Lint
ruff check src/ tests/

# Run the GUI
pbdata-gui
```

### Adding a new source adapter

1. Create `src/pbdata/sources/<name>.py` implementing `BaseAdapter`:
   ```python
   from pbdata.sources.base import BaseAdapter
   from pbdata.schemas.canonical_sample import CanonicalBindingSample

   class MyAdapter(BaseAdapter):
       @property
       def source_name(self) -> str:
           return "MySource"

       def fetch_metadata(self, record_id: str) -> dict:
           ...

       def normalize_record(self, raw: dict) -> CanonicalBindingSample:
           ...
   ```
2. Enable it in `configs/sources.yaml` (or via the GUI Sources tab).
3. Add tests to `tests/`.

---

## Roadmap

- [x] Prediction engine scaffold (ligand screening, peptide binding)
- [x] Pathway risk scoring with severity levels
- [x] QA scenario testing framework
- [x] Dataset bias reporting
- [x] Conformational state materialization
- [x] Site-centric feature pipeline with physics surrogates
- [x] Baseline affinity model training/evaluation
- [x] Agent handoff system with structured review artifacts
- [ ] Train actual prediction models (replace scaffold manifests with real predictions)
- [ ] RDKit SMILES validation for ligand screening input
- [ ] UniProt enrichment (GO terms, pathways, protein families, gene names)
- [ ] InterPro/Pfam/CATH domain annotations
- [ ] Interface residue extraction from mmCIF coordinates
- [ ] Ligand chemistry descriptors via RDKit (MW, logP, TPSA, H-bond counts)
- [ ] Parquet export for large-scale ML training
- [ ] MMseqs2 fast path for sequence clustering (>500k records)
- [ ] HuggingFace Datasets integration
- [ ] Docker image for reproducible pipelines
