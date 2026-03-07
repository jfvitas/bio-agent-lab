# pbdata — Protein Binding Dataset Platform

A Python toolkit for constructing, auditing, and versioning protein-binding
datasets for machine learning.  It ingests raw structural and affinity data
from multiple public databases, normalises every record into a single
canonical schema, scores data quality, and produces reproducible
train / val / test splits that guard against sequence-identity leakage.

---

## Table of contents

1. [What it does](#what-it-does)
2. [Supported data sources](#supported-data-sources)
3. [Installation](#installation)
4. [Quick start](#quick-start)
5. [CLI reference](#cli-reference)
6. [GUI](#gui)
7. [Canonical schema](#canonical-schema)
8. [Quality scoring](#quality-scoring)
9. [Dataset splitting](#dataset-splitting)
10. [Configuration](#configuration)
11. [Project layout](#project-layout)
12. [Development](#development)
13. [Roadmap](#roadmap)

---

## What it does

```
RCSB PDB ──┐
BindingDB ──┤  ingest  →  normalize  →  audit  →  build-splits
SKEMPI v2 ──┘
                             │               │            │
                      canonical JSON    quality flags  train.txt
                      (one file/record) + score        val.txt
                                                       test.txt
```

1. **Ingest** — queries the source database API or downloads a bulk file and
   saves raw records to `data/raw/<source>/`.
2. **Normalize** — maps each raw record to a `CanonicalBindingSample` (Pydantic
   model) and writes it as JSON to `data/processed/<source>/`.
3. **Audit** — computes per-record quality flags and a `quality_score ∈ [0, 1]`
   based on field coverage and resolution.
4. **Report** — generates a summary statistics JSON (task-type counts, method
   distribution, resolution stats, field coverage %).
5. **Build-splits** — assigns every record to train / val / test using
   k-mer Jaccard sequence-identity clustering so that highly similar
   proteins never straddle the train/test boundary.

---

## Supported data sources

| Source | Type | Status | Notes |
|--------|------|--------|-------|
| [RCSB PDB](https://www.rcsb.org) | Structural | ✅ Implemented | Searches via Search API v1, fetches metadata via GraphQL, enriches ligands with SMILES/InChIKey from chem-comp endpoint |
| [BindingDB](https://www.bindingdb.org) | Affinity | ✅ Implemented | Fetches Ki/Kd/IC50/EC50 by PDB ID; converts to nM |
| [SKEMPI v2](https://life.bsc.es/pid/skempi2/) | Mutation ddG | ✅ Implemented | Downloads bulk CSV; computes ΔΔG from Kd ratios when direct values absent |
| [BioLiP](https://zhanggroup.org/BioLiP/) | Structural | 🔧 Stub | See `src/pbdata/sources/biolip.py` for implementation notes |
| [PDBbind](https://www.pdbbind-plus.org.cn/) | Affinity | 🔧 Stub | Requires manual registration/download; see `src/pbdata/sources/pdbbind.py` |

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

## Quick start

### Command line

```bash
# Count RCSB entries matching the default criteria (no download)
pbdata ingest --dry-run

# Download matching RCSB entries (prompts for confirmation)
pbdata ingest

# Download without confirmation
pbdata ingest --yes

# Download SKEMPI v2 mutation dataset
pbdata ingest --source skempi

# Normalize raw RCSB records to canonical JSON
# (also fetches SMILES/InChIKey for every ligand)
pbdata normalize

# Score quality flags on all normalized records
pbdata audit

# Generate summary statistics report
pbdata report

# Build sequence-identity-aware train/val/test splits
pbdata build-splits

# Hash-based split (faster, no clustering)
pbdata build-splits --hash-only
```

### Python API

```python
from pathlib import Path
from pbdata.sources.rcsb import RCSBAdapter
from pbdata.quality.audit import audit_record
from pbdata.dataset.splits import cluster_aware_split

# Normalize a single raw RCSB record
adapter = RCSBAdapter()
raw = adapter.fetch_metadata("1ATP")
record = adapter.normalize_record(raw)

# Score it
audited = audit_record(record)
print(audited.quality_score)   # e.g. 0.875
print(audited.quality_flags)   # e.g. ['no_uniprot_id']

# Build cluster-aware splits
sample_ids = ["RCSB_1ATP", "RCSB_2SRC", ...]
sequences  = ["MGSS...", "MASL...", ...]
result = cluster_aware_split(sample_ids, sequences)
print(result.sizes())  # {'train': 700, 'val': 150, 'test': 150}
```

---

## CLI reference

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

Normalizes raw records from `data/raw/rcsb/` into canonical JSON in
`data/processed/rcsb/`.  Automatically batch-fetches SMILES and InChIKey
for all unique ligand comp_ids found in the raw records.

### `pbdata audit`

Computes `quality_flags` and `quality_score` for every record in
`data/processed/rcsb/` and writes an audit summary to
`data/audit/audit_summary.json`.

### `pbdata report`

Generates `data/reports/summary.json` with:
- Total records, task-type breakdown
- Experimental method distribution
- Resolution statistics (mean, median, quartiles)
- Quality score statistics
- Field coverage percentages

### `pbdata build-splits`

Writes `data/splits/train.txt`, `val.txt`, `test.txt`, and `metadata.json`.

| Flag | Default | Description |
|------|---------|-------------|
| `--train-frac` | `0.70` | Target training fraction |
| `--val-frac` | `0.15` | Target validation fraction |
| `--seed` | `42` | Reproducibility seed |
| `--hash-only` | off | Use fast hash split (skips clustering) |
| `--threshold` | `0.30` | Jaccard similarity threshold for clustering |

---

## GUI

A Tkinter desktop interface is included for users who prefer point-and-click
operation.

```bash
pbdata-gui
```

The GUI provides:

- **Sources panel** — enable/disable databases and save to `configs/sources.yaml`
- **Criteria panel** — select experimental methods, resolution limit, task
  types, minimum release year, and save to `configs/criteria.yaml`
- **Pipeline panel** — individual stage buttons plus a "Run Full Pipeline"
  button that executes all stages sequentially
- **Search & Download checkpoint** — queries the entry count first and shows
  a confirmation dialog ("N entries found. Proceed?") before downloading
- **Log panel** — dark-theme scrollable log with real-time output from all
  pipeline stages

---

## Canonical schema

Every normalized record is stored as a `CanonicalBindingSample` (Pydantic v2,
frozen).  All records from all sources share this schema.

### Identity fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `sample_id` | `str` | ✅ | Globally unique internal ID (e.g. `RCSB_1ATP`) |
| `task_type` | `str` | ✅ | `protein_ligand` \| `protein_protein` \| `mutation_ddg` |
| `source_database` | `str` | ✅ | Source name (e.g. `RCSB`, `BindingDB`, `SKEMPI`) |
| `source_record_id` | `str` | ✅ | Native ID in the source database |
| `pdb_id` | `str` | — | 4-character PDB accession |

### Structural fields

| Field | Type | Description |
|-------|------|-------------|
| `chain_ids_receptor` | `list[str]` | Auth chain IDs for the receptor protein |
| `chain_ids_partner` | `list[str]` | Auth chain IDs for the binding partner |
| `sequence_receptor` | `str` | Canonical amino-acid sequence (one-letter) |
| `sequence_partner` | `str` | Partner sequence (protein-protein tasks) |
| `uniprot_ids` | `list[str]` | UniProt accessions for all protein entities |
| `taxonomy_ids` | `list[int]` | NCBI taxonomy IDs |
| `experimental_method` | `str` | e.g. `X-RAY DIFFRACTION`, `ELECTRON MICROSCOPY` |
| `structure_resolution` | `float` | Resolution in Angstroms (X-ray / cryo-EM) |

### Ligand fields

| Field | Type | Description |
|-------|------|-------------|
| `ligand_id` | `str` | CCD comp_id (e.g. `ATP`) |
| `ligand_smiles` | `str` | Canonical SMILES string |
| `ligand_inchi_key` | `str` | InChIKey |

### Assay / affinity fields

| Field | Type | Description |
|-------|------|-------------|
| `assay_type` | `str` | `Ki`, `Kd`, `IC50`, `EC50`, `ddG` |
| `assay_value` | `float` | Raw measured value |
| `assay_unit` | `str` | Unit as reported (e.g. `nM`, `uM`) |
| `assay_value_standardized` | `float` | Value converted to nM (or kcal/mol for ddG) |
| `assay_value_log10` | `float` | log₁₀(standardized value) |
| `temperature_c` | `float` | Assay temperature in °C |
| `ph` | `float` | Assay pH (validated: 0–14) |
| `ionic_strength` | `float` | Ionic strength in mM (≥ 0) |

### Mutation fields

| Field | Type | Description |
|-------|------|-------------|
| `mutation_string` | `str` | Mutation notation (e.g. `A:R45K,A:L48V`) |
| `wildtype_or_mutant` | `str` | `wildtype` or `mutant` |

### Metadata fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `provenance` | `dict` | ✅ | Must contain `ingested_at` (ISO-8601 timestamp) |
| `quality_flags` | `list[str]` | ✅ | List of quality warning codes |
| `quality_score` | `float` | ✅ | Overall quality score in [0.0, 1.0] |

---

## Quality scoring

The audit step assigns each record a `quality_score` (0–1) based on 8 equally
weighted boolean checks:

| Check | Applies to |
|-------|-----------|
| `experimental_method` present | All |
| `structure_resolution` ≤ 3.5 Å | All |
| `sequence_receptor` present | All |
| `chain_ids_receptor` non-empty | All |
| `uniprot_ids` non-empty | All |
| `taxonomy_ids` non-empty | All |
| `ligand_id` present | `protein_ligand` |
| `sequence_partner` present | `protein_protein` |

Quality flags (informational, do not filter records):

| Flag | Meaning |
|------|---------|
| `no_resolution` | Resolution field absent |
| `low_resolution` | Resolution > 3.5 Å |
| `very_low_resolution` | Resolution > 4.5 Å |
| `no_experimental_method` | Method field absent |
| `missing_sequence_receptor` | Receptor sequence absent |
| `missing_sequence_partner` | Partner sequence absent (protein-protein only) |
| `no_uniprot_id` | No UniProt accession |
| `no_chain_ids` | No receptor chain IDs |
| `missing_ligand_id` | No ligand comp_id (protein-ligand only) |

---

## Dataset splitting

`build-splits` uses **k-mer Jaccard sequence-identity clustering** by default
to prevent data leakage between train and test sets.

### Algorithm

1. Compute 5-gram k-mer sets for every receptor sequence.
2. Build an inverted index (k-mer → record indices) for fast candidate lookup.
3. Greedy single-linkage clustering: for each unassigned record, find all
   other records sharing enough k-mers, compute exact Jaccard similarity, and
   merge those above `threshold` (default 0.30 ≈ 30–40% sequence identity)
   into the same cluster.
4. Sort clusters largest-first, then greedily fill train → val → test until
   each partition reaches its target fraction.

Whole clusters are assigned atomically, so proteins with high sequence
similarity always land in the same partition.

Records without a sequence fall back to deterministic MD5 hash-based
assignment.

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

Controls which RCSB entries are fetched during `ingest`.

```yaml
experimental_methods:
  - xray    # X-RAY DIFFRACTION
  - em      # ELECTRON MICROSCOPY
max_resolution_angstrom: 3.0   # null = no limit
task_types:
  - protein_ligand
  - protein_protein
require_protein: true
min_release_year: null         # e.g. 2010 to limit to recent structures
```

Available method keys: `xray`, `em`, `nmr`, `neutron`

### Sources — `configs/sources.yaml`

```yaml
sources:
  rcsb:
    enabled: true
  bindingdb:
    enabled: false
  skempi:
    enabled: false
  biolip:
    enabled: false
  pdbbind:
    enabled: false
```

### Logging — `configs/logging.yaml`

Standard Python `logging.config.dictConfig` format (version: 1).

---

## Project layout

```
bio-agent-lab/
├── configs/
│   ├── criteria.yaml       # RCSB search criteria
│   ├── sources.yaml        # enabled data sources
│   └── logging.yaml        # logging configuration
├── specs/
│   ├── canonical_schema.yaml
│   ├── coding_standards.md
│   ├── quality_rules.yaml
│   ├── repo_contract.md
│   ├── source_requirements.md
│   └── split_policy.yaml
├── src/
│   └── pbdata/
│       ├── cli.py                  # Typer CLI entry point
│       ├── gui.py                  # Tkinter GUI
│       ├── config.py               # AppConfig / SourceConfig loader
│       ├── criteria.py             # SearchCriteria model + YAML helpers
│       ├── logging_config.py       # Validated logging setup
│       ├── schemas/
│       │   └── canonical_sample.py # CanonicalBindingSample Pydantic model
│       ├── sources/
│       │   ├── base.py             # BaseAdapter ABC
│       │   ├── rcsb.py             # RCSB normalizer
│       │   ├── rcsb_search.py      # RCSB Search + GraphQL + chem-comp client
│       │   ├── bindingdb.py        # BindingDB REST adapter
│       │   ├── skempi.py           # SKEMPI v2 CSV adapter
│       │   ├── biolip.py           # BioLiP stub
│       │   └── pdbbind.py          # PDBbind stub
│       ├── quality/
│       │   └── audit.py            # quality_flags + quality_score
│       └── dataset/
│           └── splits.py           # k-mer Jaccard clustering splits
├── tests/
│   ├── conftest.py
│   ├── test_schema.py
│   ├── test_config.py
│   └── test_smoke.py
├── scripts/
│   └── validate_schema.py
├── bootstrap_repo.py               # Creates data/ directory scaffold
├── pyproject.toml
└── README.md
```

---

## Development

```bash
# Run tests
pytest -q

# Lint
ruff check src/ tests/

# Validate the canonical schema spec
python scripts/validate_schema.py
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
           ...  # call API or read local file

       def normalize_record(self, raw: dict) -> CanonicalBindingSample:
           ...  # map raw fields → CanonicalBindingSample
   ```
2. Enable it in `configs/sources.yaml`.
3. Add a dispatch branch to `cli.py`'s `ingest` command.
4. Add tests to `tests/`.

---

## Roadmap

- [ ] `normalize --source skempi` CLI variant
- [ ] BindingDB bulk ingest CLI command
- [ ] BioLiP adapter (weekly flat-file download + RCSB sequence join)
- [ ] PDBbind adapter (local directory reader for registered users)
- [ ] MMseqs2 fast path for sequence clustering (>500k records)
- [ ] Parquet export for large-scale ML training
- [ ] HuggingFace Datasets integration
- [ ] Docker image for reproducible pipelines
