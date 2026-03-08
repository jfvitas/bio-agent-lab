# pbdata — Protein Binding Dataset Platform

A Python toolkit for constructing, auditing, and versioning protein-binding
datasets for machine learning.  It ingests raw structural and affinity data
from multiple public databases, normalizes every record into a canonical
schema, extracts multi-table records with full provenance, scores data
quality, and produces reproducible train/val/test splits that guard against
sequence-identity leakage.

---

## Table of contents

1. [What it does](#what-it-does)
2. [Supported data sources](#supported-data-sources)
3. [Installation](#installation)
4. [Quick start](#quick-start)
5. [CLI reference](#cli-reference)
6. [GUI](#gui)
7. [Multi-table extraction pipeline](#multi-table-extraction-pipeline)
8. [Canonical schema](#canonical-schema)
9. [Quality scoring](#quality-scoring)
10. [Dataset splitting](#dataset-splitting)
11. [Configuration](#configuration)
12. [Project layout](#project-layout)
13. [Testing](#testing)
14. [Development](#development)
15. [Roadmap](#roadmap)

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

1. **Ingest** — queries source database APIs or downloads bulk files and
   saves raw records to `data/raw/<source>/`.
2. **Normalize** — maps each raw record to a `CanonicalBindingSample` (Pydantic
   model) and writes it as JSON to `data/processed/<source>/`.
3. **Extract** — produces six linked output tables per the structure
   extraction spec: entry, chain, bound_object, interface, assay, and
   provenance records. Downloads and stores mmCIF structure files with
   SHA-256 hashing and full file provenance.
4. **Audit** — computes per-record quality flags and a `quality_score` in
   [0, 1] based on field coverage, resolution, and structural features.
5. **Report** — generates summary statistics JSON.
6. **Build-splits** — assigns every record to train/val/test using
   k-mer Jaccard sequence-identity clustering so that highly similar
   proteins never straddle the train/test boundary.

---

## Supported data sources

| Source | Type | Status | Notes |
|--------|------|--------|-------|
| [RCSB PDB](https://www.rcsb.org) | Structural | Implemented | Search API, GraphQL metadata, mmCIF download + parsing, chem-comp SMILES/InChIKey enrichment |
| [BindingDB](https://www.bindingdb.org) | Affinity | Implemented | Fetches Ki/Kd/IC50/EC50 by PDB ID; converts to nM |
| [ChEMBL](https://www.ebi.ac.uk/chembl/) | Affinity | Implemented | REST API lookup by UniProt accession + InChIKey |
| [SKEMPI v2](https://life.bsc.es/pid/skempi2/) | Mutation ddG | Implemented | Downloads bulk CSV; computes ddG from Kd ratios |
| [BioLiP](https://zhanggroup.org/BioLiP/) | Structural | Implemented | Flat-file parser with binding site residue extraction |
| [PDBbind](https://www.pdbbind-plus.org.cn/) | Affinity | Implemented | Local INDEX file parser (requires manual download) |

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
# Count RCSB entries matching criteria (no download)
pbdata ingest --dry-run

# Download matching RCSB entries
pbdata ingest --yes

# Download SKEMPI v2 mutation dataset
pbdata ingest --source skempi

# Normalize raw RCSB records to canonical JSON
# (also fetches SMILES/InChIKey for every ligand)
pbdata normalize

# Extract multi-table records with mmCIF structure files
pbdata extract

# Extract without downloading structure files
pbdata extract --no-download-structures

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
for all unique ligand comp_ids.

### `pbdata extract`

Produces six linked output tables per the structure extraction spec.
Downloads mmCIF structure files with SHA-256 provenance by default.

| Flag | Default | Description |
|------|---------|-------------|
| `--output PATH` | `data/extracted/` | Override output directory |
| `--structures PATH` | `data/structures/rcsb/` | Override structures directory |
| `--download-pdb` | off | Also download PDB format files |
| `--download-structures / --no-download-structures` | on | Download mmCIF files |

Output tables:
- `entry/` — one record per PDB entry (73 fields)
- `chains/` — one record per chain/entity assignment (26 fields)
- `bound_objects/` — one record per ligand/cofactor/metal/glycan (45 fields)
- `interfaces/` — one record per interface/binding site (27 fields)
- `assays/` — one record per affinity measurement (33 fields)
- `provenance/` — per-field provenance trail (9 fields)

### `pbdata audit`

Computes `quality_flags` and `quality_score` for every record in
`data/processed/rcsb/` and writes an audit summary to
`data/audit/audit_summary.json`.

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
  types, minimum release year
- **Pipeline panel** — individual stage buttons plus a "Run Full Pipeline"
  button that executes all stages sequentially (including extract)
- **Search & Download checkpoint** — queries the entry count first and shows
  a confirmation dialog before downloading
- **Log panel** — dark-theme scrollable log with real-time output

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
pair-aware grouping.

### ProvenanceRecord (9 fields)

Per-field provenance trail: source name, extraction method, raw/normalized
values, confidence, timestamp.

### File download policy

- mmCIF is the required primary format (`.cif`)
- PDB (`.pdb`) is an optional compatibility fallback
- Files are saved to `data/structures/rcsb/` with SHA-256 hashing
- File provenance fields track path, size, hash, download URL, and timestamp

### Entity classification

The pipeline distinguishes:
- **Proteins** (polypeptides > 30 residues)
- **Peptides** (polypeptides <= 30 residues)
- **Small molecules** (organic non-cofactor ligands)
- **Cofactors** (~50 curated biochemical cofactors)
- **Metal ions** (70+ curated metal/halide comp_ids)
- **Glycans** (monosaccharides, polysaccharides, branched entities)
- **Additives** (~60 crystallization artifacts excluded from analysis)
- **Nucleic acids** (DNA, RNA polymers)

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

### Extended structural fields

| Field | Type | Description |
|-------|------|-------------|
| `bound_objects` | `list[dict]` | All bound entities (ligands, metals, cofactors, glycans, peptides) |
| `interfaces` | `list[dict]` | Pairwise polymer-polymer interfaces |
| `assembly_info` | `dict` | Biological assembly metadata |
| `oligomeric_state` | `str` | e.g. `monomer`, `homodimer`, `heterodimer` |
| `is_homo_oligomeric` | `bool` | All chains same entity? |
| `polymer_entity_count` | `int` | Total polymer chain instances |

### Quality metadata

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `provenance` | `dict` | Yes | Must contain `ingested_at` |
| `quality_flags` | `list[str]` | Yes | Quality warning codes |
| `quality_score` | `float` | Yes | Score in [0.0, 1.0] |

---

## Quality scoring

The audit step assigns each record a `quality_score` (0-1) and a list
of quality/ambiguity flags:

| Flag | Meaning |
|------|---------|
| `no_resolution` | Resolution field absent |
| `low_resolution` | Resolution > 3.5 A |
| `very_low_resolution` | Resolution > 4.5 A |
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
4. Sort clusters largest-first, greedily fill train -> val -> test.

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

Controls which RCSB entries are fetched during `ingest`.

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

## Project layout

```
bio-agent-lab/
├── configs/
│   ├── criteria.yaml          # RCSB search criteria
│   ├── sources.yaml           # enabled data sources
│   └── logging.yaml           # logging configuration
├── src/
│   └── pbdata/
│       ├── cli.py                     # Typer CLI (ingest, normalize, extract, audit, report, build-splits)
│       ├── gui.py                     # Tkinter GUI
│       ├── config.py                  # AppConfig loader
│       ├── criteria.py                # SearchCriteria model
│       ├── logging_config.py          # Logging setup
│       ├── schemas/
│       │   ├── canonical_sample.py    # CanonicalBindingSample
│       │   ├── bound_objects.py       # BoundObject, InterfaceInfo, AssemblyInfo
│       │   └── records.py            # Multi-table schemas (Entry, Chain, BoundObject, Interface, Assay, Provenance)
│       ├── pipeline/
│       │   ├── extract.py            # Multi-table extraction pipeline
│       │   └── assay_merge.py        # Cross-source assay merge
│       ├── parsing/
│       │   └── mmcif_supplement.py   # mmCIF download, parsing, structure quality
│       ├── sources/
│       │   ├── base.py               # BaseAdapter ABC
│       │   ├── rcsb.py               # RCSB adapter
│       │   ├── rcsb_search.py        # RCSB Search + GraphQL client
│       │   ├── rcsb_classify.py      # Entity classification (870+ lines)
│       │   ├── bindingdb.py          # BindingDB REST adapter
│       │   ├── chembl.py             # ChEMBL REST adapter
│       │   ├── skempi.py             # SKEMPI v2 CSV adapter
│       │   ├── biolip.py             # BioLiP flat-file adapter
│       │   └── pdbbind.py            # PDBbind INDEX file adapter
│       ├── quality/
│       │   ├── audit.py              # Quality flags + score
│       │   └── stress_panel.py       # Stress panel evaluation helpers
│       └── dataset/
│           └── splits.py             # k-mer Jaccard clustering splits
├── tests/
│   ├── test_structural_edge_cases.py # 100+ unit tests + Panel A/B integration
│   ├── test_stress_panel.py          # Stress panel unit tests
│   ├── test_stress_panel_c.py        # Panel C integration tests (48 tests)
│   ├── test_extract_pipeline.py      # Multi-table extraction tests
│   ├── test_assay_merge.py           # Assay merge tests
│   ├── test_biolip.py               # BioLiP adapter tests
│   ├── test_chembl.py               # ChEMBL adapter tests
│   ├── test_pdbbind.py              # PDBbind adapter tests
│   ├── test_schema.py               # Schema validation tests
│   ├── test_config.py               # Config loader tests
│   ├── test_search.py               # RCSB search tests
│   └── test_smoke.py                # Basic smoke tests
├── data/
│   ├── raw/rcsb/                    # Raw RCSB GraphQL JSON
│   ├── raw/skempi/                  # Raw SKEMPI CSV
│   ├── structures/rcsb/             # Downloaded mmCIF/PDB files
│   ├── processed/rcsb/              # Normalized canonical JSON
│   ├── extracted/                   # Multi-table output (6 subdirs)
│   ├── splits/                      # train/val/test splits
│   ├── audit/                       # Audit summary
│   └── reports/                     # Statistics reports
├── stress_test_panel.yaml           # Panel A: 10 structural edge cases
├── stress_test_panel_B.yaml         # Panel B: 10 adversarial cases
├── stress_test_panel_C.yaml         # Panel C: 12 extended extraction cases
├── expected_outcomes_panel_C.md     # Panel C acceptance criteria
├── STRUCTURE_EXTRACTION_AGENT_SPEC.md  # Authoritative extraction spec
├── AGENTS.md                        # Agent instructions
├── CLAUDE.md                        # Claude Code instructions
├── pyproject.toml                   # Package config
└── README.md
```

---

## Testing

```bash
# Run unit tests only (default, excludes integration)
pytest -q

# Run integration tests (requires network, fetches live RCSB data)
pytest -m integration -v

# Run all tests
pytest -m "" -v

# Run a specific panel
pytest tests/test_stress_panel_c.py -m integration -v
```

### Test coverage

- **161 unit tests** — entity classification, bound object detection,
  oligomeric state inference, covalent warhead detection, membrane
  context, quality flags, schema validation, config loading, assay merge
- **71 integration tests** — Panel A (10 entries), Panel B (10 entries),
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
2. Enable it in `configs/sources.yaml`.
3. Add tests to `tests/`.

---

## Roadmap

- [ ] UniProt enrichment (GO terms, pathways, protein families, gene names)
- [ ] InterPro/Pfam/CATH domain annotations
- [ ] Interface residue extraction from mmCIF coordinates
- [ ] Ligand chemistry descriptors via RDKit (MW, logP, TPSA, H-bond counts)
- [ ] Parquet export for large-scale ML training
- [ ] MMseqs2 fast path for sequence clustering (>500k records)
- [ ] HuggingFace Datasets integration
- [ ] Docker image for reproducible pipelines
