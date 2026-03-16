# Roadmap To A Fully Usable PBData Platform

Date: 2026-03-16

This document turns the current repository direction into a concrete execution
roadmap. It is intended to guide both autonomous development work and operator
setup decisions.

## End state

The target is a local-first, GUI-driven protein data platform that can:

- pre-populate and manage broad biological source data under a user-selected
  root
- build representative, leakage-resistant training sets from real workspace
  artifacts
- support multiple structural graph designs and robust model-development paths
- present cleanly in Demo Mode
- run truthfully in Functional Mode without simulated workflow state being
  mistaken for real outputs

## Current state summary

The repository already has substantial working pieces:

- source adapters and a source-capability registry
- a bootstrap store and selected-PDB refresh planning
- structural graph packaging
- engineered dataset export
- backend-backed model recommendation and recommendation-driven training
- a launchable WinUI shell
- smoke checks and targeted regression tests

The main remaining gap is mixed maturity. Some paths are real and useful, but
functional mode still coexists with simulated/demo artifacts and partially
populated decision fields.

## Primary blockers

1. Functional mode still mixes live workflow artifacts with simulated stage
   state.
2. Source prepopulation, freshness checks, targeted refresh, and deletion
   policies are not yet fully explicit per source.
3. Some intended screening and quality parameters are present in exports but
   not populated enough to govern real decisions.
4. Training-set creation, split generation, and engineered dataset assembly are
   not yet consistently proven on a fully real local corpus.
5. Model Studio is much stronger than before, but not every desirable family,
   tuning surface, and evaluation dimension is equally mature.
6. The WinUI shell is presentable, but it still needs final convergence between
   Demo Mode and Functional Mode.

## Phase 0: Separate Demo And Functional State

Goals:

- treat Demo Mode and Functional Mode as separate artifact domains
- ensure simulated stage-state can never be mistaken for live operational state
- define which artifacts are presentation-only, fixture-only, or live

Deliverables:

- explicit demo workspace fixture
- explicit functional workspace fixture
- stage-state labeling rules for `live`, `fixture`, and `simulated`
- WinUI mode switch that isolates the two modes cleanly

Acceptance criteria:

- functional dashboards never report simulated outputs as completed live work
- smoke validation fails if functional mode points at seeded demo state

## Phase 1: Source Prepopulation And Local Packaging

Goals:

- make the bootstrap store the canonical fast planning layer
- define exactly what gets preloaded, when it is refreshed, and when stale
  files are removed

Required policy for every material source:

- snapshot mechanism
- schema/version tracking
- freshness check rule
- targeted refresh rule
- file retention rule
- deletion / garbage-collection rule

Priority sources:

- RCSB PDB
- BindingDB
- ChEMBL
- PDBbind
- BioLiP
- SKEMPI v2
- UniProt
- AlphaFold DB
- InterPro
- Pfam
- CATH
- SCOPe
- Reactome

Deliverables:

- per-source manifest schema
- source snapshot inventory under the managed root
- bootstrap store expansion for source coverage, freshness, and file-state facts
- PDB lifecycle policy covering mmCIF/PDB pull, retention, update checks, and
  deletion

Acceptance criteria:

- the platform can explain why a source snapshot is current or stale
- selected-PDB refresh plans read from the bootstrap store rather than ad hoc
  scans
- stale local files are deleted only by explicit policy, never implicitly

## Phase 2: Field And Screening-Parameter Audit

Goals:

- inventory every field used for candidate screening, data quality, train/test
  governance, and release readiness
- remove the gap between "field exists" and "field is scientifically usable"

For each field, record:

- source of truth
- derivation logic
- expected null rate
- whether null is meaningful
- whether the field is required, advisory, planned, or deprecated

Immediate focus examples:

- `quality_score`
- `organism_names`
- `source_conflict_summary`
- `source_agreement_band`
- `release_split`
- affinity-log and unit conventions

Deliverables:

- machine-readable field audit report
- policy gate that blocks hard decisions on empty/planned fields
- cleanup or backfill plan for scientifically weak parameters

Acceptance criteria:

- no screening or split policy can silently rely on a field that is effectively
  unpopulated
- docs explain why each decision field exists and how it is populated

## Phase 3: Training-Set Creation And Split Hardening

Goals:

- make custom training-set creation and split generation fully real
- prove that leakage-resistant policies are actually enforced on live data

Required dimensions:

- sequence identity grouping / holdout
- fold/class/family-aware grouping
- mutation-cluster-aware grouping
- source-held-out evaluation
- duplicate and near-duplicate handling
- representative rather than over-clustered subset selection

Deliverables:

- live non-empty custom training-set generation
- real scorecards and real split manifests
- split diagnostics for leakage, imbalance, and over-concentration
- MMseqs2-assisted fast path for larger corpora

Acceptance criteria:

- functional mode produces non-simulated train/val/test splits
- engineered datasets match the live split manifests
- split reports explicitly quantify leakage risk across the supported axes

## Phase 4: Graph And Feature Hardening

Goals:

- preserve the full graph design space while making graph builds operational and
  efficient
- improve graph-backed feature coverage enough to support better model choices

Supported design space should continue to include:

- whole-protein graphs
- interface-only graphs
- shell / neighborhood graphs
- residue-level graphs
- atom-level graphs
- protein-ligand graphs
- protein-protein interface graphs
- heterogeneous / multimodal graphs with metadata context

Deliverables:

- chunked graph builds with reuse keyed by structure signatures
- graph-package manifests tied directly to dataset rows
- graph coverage accounting in training/evaluation artifacts
- graph-derived feature export that aligns with model-family expectations

Acceptance criteria:

- preview graph builds are fast
- refresh-plan and training-set graph builds are trustworthy
- graph coverage is real, row-linked, and visible in both CLI and GUI

## Phase 5: Model Studio And Training Robustness

Goals:

- make Model Studio fully honest about what is runnable now versus planned later
- expose enough configuration to build robust models rather than just demo
  models

Required work:

- define the supported model-family matrix
- expose real hyperparameter and preset controls
- strengthen evaluation outputs
- improve import/export parity for saved runs
- keep recommendation, runtime readiness, and actual training behavior aligned

Evaluation should include:

- calibration
- subgroup performance
- novelty / holdout behavior
- graph-coverage context
- feature availability context
- provenance of execution strategy and runtime fallback

Acceptance criteria:

- every runnable family can be trained, evaluated, and reloaded cleanly
- every planned family is clearly marked as planned
- saved runs explain what was recommended, what actually ran, and why

## Phase 6: GUI Convergence And Production UX

Goals:

- keep Demo Mode strong for presentation
- make Functional Mode equally complete and trustworthy
- finish the WinUI shell as the primary polished surface

Required work:

- first-run setup wizard
- workspace/profile management
- artifact previews and charts
- robust empty/error states
- keyboard/accessibility pass
- responsive layout pass
- clear guidance for large prepopulation and refresh workflows

Acceptance criteria:

- a fresh-clone user can launch, understand readiness, and complete the guided
  workflow
- demo presentations remain smooth and visually polished
- functional mode actions produce live artifacts, not placeholders

## Phase 7: Validation, Packaging, And Release Discipline

Goals:

- move from ad hoc validation to a release-quality verification ladder

Required layers:

- fast smoke suite for fresh clones
- targeted regression suites
- golden functional workspace fixture
- golden presentation workspace fixture
- longer-running data/model verification suite
- release checklist that rejects simulated functional artifacts

Acceptance criteria:

- release candidates have a repeatable pass/fail gate
- launcher, CLI, and GUI checks agree on readiness
- the repo can be handed to a new user with a supported bootstrap path

## Operator Acquisition Checklist

The following external assets are the most helpful things to obtain now.

### Highest priority

1. BindingDB bulk dump and assay companion files
   - Why: needed for real local-first affinity enrichment instead of mostly
     live-on-demand lookups
   - Where: official download page at
     https://www.bindingdb.org/rwd/bind/chemsearch/marvin/Download.jsp
2. ChEMBL full release dump
   - Why: needed for durable local bioactivity enrichment and chemical context
   - Where: official ChEMBL site at https://www.ebi.ac.uk/chembl/
   - Bulk release area: https://ftp.ebi.ac.uk/pub/databases/chembl/ChEMBLdb/latest/
3. PDBbind current licensed release
   - Why: curated affinity corpus and benchmark-quality local package
   - Where: official site at https://www.pdbbind-plus.org.cn/download
   - Note: licensing/registration terms apply; do not redistribute in-repo
4. BioLiP download set
   - Why: biologically relevant binding-site annotations and ligand context
   - Where: official download page at https://zhanggroup.org/BioLiP/download.html
5. MMseqs2 binary
   - Why: fast sequence clustering / leakage-aware split support at larger scale
   - Where: official releases at https://github.com/soedinglab/MMseqs2/releases

### Very helpful next

1. UniProt local knowledgebase slice or full bulk files
   - Where: official bulk files at
     https://ftp.uniprot.org/pub/databases/uniprot/current_release/knowledgebase/complete/
   - Also useful: REST/API docs at https://www.uniprot.org/help/api_queries
2. AlphaFold DB bulk metadata / species packs
   - Where: official download page at https://alphafold.ebi.ac.uk/download
3. InterPro data snapshot
   - Where: InterPro data access guidance at
     https://www.ebi.ac.uk/training/online/courses/interpro-quick-tour/getting-data-from-interpro/
4. Pfam current release flat files
   - Where: official FTP guidance at
     https://pfam-docs.readthedocs.io/en/dev/ftp-site.html
5. CATH latest classification data
   - Where: official download page at https://www.cathdb.info/download
   - Direct file browser: https://download.cathdb.info/cath/releases/latest-release/
6. SCOPe parseable files
   - Where: official downloads at https://scop.berkeley.edu/downloads/ver=2.08
7. Reactome mappings and pathway files
   - Where: official download page at https://reactome.org/download-data
8. SKEMPI v2 dataset files
   - Where: official download page at https://life.bsc.es/pid/skempi2

### Helpful but not required immediately

1. BioGRID bulk interaction downloads
2. STRING bulk interaction downloads
3. IntAct bulk interaction datasets

These are valuable for the future broader biological graph layer, but they are
not the first blocker to solve before the core local-first dataset pipeline is
real and stable.

## Recommended Local Placement For Acquired Assets

Until the full source-snapshot manager is finished, use the managed workspace
root and place external downloads under predictable folders.

Recommended layout:

- `<storage_root>/data_sources/pdbbind/`
  - extract the licensed PDBbind release here
  - the current adapter expects `index/INDEX_general_PL_data*` under this
    directory
- `<storage_root>/data_sources/biolip/`
  - place `BioLiP.txt` here
- `<storage_root>/data/raw/skempi/skempi_v2.csv`
  - this already matches the current default ingest/extract path
- `<storage_root>/data/raw/bindingdb/`
  - use for cached or bulk BindingDB payloads
- `<storage_root>/data_sources/chembl/`
  - park ChEMBL release files here until the source-snapshot loader is wired
- `<storage_root>/data_sources/uniprot/`
- `<storage_root>/data_sources/alphafold_db/`
- `<storage_root>/data_sources/interpro/`
- `<storage_root>/data_sources/pfam/`
- `<storage_root>/data_sources/cath/`
- `<storage_root>/data_sources/scop/`
- `<storage_root>/data_sources/reactome/`
  - use these as raw/source parking locations for the next prepopulation pass

Operator note:

- if you keep the current repo root as the storage root, those paths map inside
  the repository
- if you prefer a larger external disk location, we should move `storage_root`
  there and keep the repo as code only

## Tooling And MCP Wishlist

The project can keep moving without additional tooling, but these would help:

- Figma MCP for a more intentional production design pass on the WinUI shell
- Windows desktop UI automation / testing support for end-to-end GUI smoke
  coverage
- stronger SQLite / data-audit tooling for field-population analysis across the
  bootstrap store and exported tables

## What To Ask The Operator For First

If only a few things can be acquired right now, ask for these first:

1. BindingDB bulk dump access/download
2. ChEMBL bulk release
3. PDBbind licensed local release
4. BioLiP download package
5. MMseqs2 binary

That set would unblock the biggest near-term improvements in prepopulation,
training-set realism, split governance, and model-development quality.
