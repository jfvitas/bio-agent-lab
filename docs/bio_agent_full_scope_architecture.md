# Full-Scope Architecture

This architecture is the target implementation plan for satisfying
`bio_agent_full_spec`.

## Data Layers

### Raw Layer

Persist source-native files under:
- `data/raw/rcsb/`
- `data/raw/bindingdb/`
- `data/raw/chembl/`
- `data/raw/pdbbind/`
- `data/raw/biolip/`
- `data/raw/skempi/`
- `data/raw/graph_sources/<source>/`

### Canonical Layer

Persist normalized records under:
- `data/processed/<source>/`
- canonical structure tables
- canonical assay records
- canonical graph node/edge records

### Feature Layer

Persist derived features under:
- `data/features/structure/`
- `data/features/chemistry/`
- `data/features/biology/`
- `data/features/graph/`

### Training Example Layer

Persist final examples under:
- `data/training_examples/`

## Subsystems

### Structure Extraction

Current home:
- `src/pbdata/parsing/`
- `src/pbdata/sources/rcsb*.py`
- `src/pbdata/pipeline/extract.py`

Target expansion:
- BinaryCIF fallback
- interface/contact extraction from coordinates
- explicit confidence annotations for weak calls

### Experimental Ingestion

Current home:
- `src/pbdata/sources/`
- `src/pbdata/pipeline/assay_merge.py`

Target expansion:
- bulk ingest for structured sources
- literature-review ingestion queue
- stricter assay-condition normalization

### Graph Builder

Current scaffold:
- `src/pbdata/graph/`
- `src/pbdata/schemas/graph.py`
- `src/pbdata/graph/connectors.py`
- `src/pbdata/graph/identifier_map.py`

Target implementation:
- source connectors for interaction/pathway databases
- identifier harmonization service
- node/edge canonicalization
- graph persistence and feature extraction

Current identifier support:
- UniProt accession / ID detection
- Ensembl exact mapping via UniProt ID mapping
- Entrez Gene exact mapping via UniProt ID mapping

Still missing:
- cached/bulk mapping workflow
- conflict-resolution policy beyond first-returned exact mapping
- graph-builder integration for external-source harmonization

### Feature Builder

Current scaffold:
- `src/pbdata/features/`
- `src/pbdata/features/pathway.py`
- `src/pbdata/features/mm_features.py`

Target implementation:
- structural features
- chemical descriptors
- biological context features
- graph embeddings / network statistics
- optional molecular mechanics features

### Training Generator

Current scaffold:
- `src/pbdata/training/`
- `src/pbdata/schemas/training_example.py`
- `src/pbdata/training/assembler.py`

Target implementation:
- join extracted records + assay records + graph features
- emit spec-aligned training examples
- support split-aware generation

## CLI Stages

Current implemented stages:
- `ingest`
- `normalize`
- `extract`
- `audit`
- `report`
- `build-splits`

Architecture stages added:
- `build-graph`
- `build-features`
- `build-training-examples`

These currently emit architecture manifests and define the expected
filesystem contracts for the missing subsystems.

## Explicit Stub Modules

The repo now includes explicit stub modules for the remaining full-scope work:
- graph source connectors
- identifier harmonization
- pathway feature enrichment
- optional molecular-mechanics features
- training-example assembly

Each stub exposes a narrow placeholder API plus implementation notes so the
remaining scope can be filled in incrementally without rediscovering the
boundary each time.
