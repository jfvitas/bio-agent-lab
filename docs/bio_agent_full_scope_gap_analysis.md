# Full-Scope Gap Analysis

This document compares the current `pbdata` repo against the requirements in
[`bio_agent_full_spec`](/C:/Users/jfvit/documents/bio-agent-lab/bio_agent_full_spec).

## Status Summary

### 1. Structure Extraction Pipeline

Status: partially implemented

Implemented today:
- RCSB search, download, and raw persistence
- mmCIF-first structure download
- structural classification for proteins, ligands, metals, glycans, cofactors
- multi-table extraction output
- quality auditing and split generation

Still missing or partial:
- BinaryCIF fallback
- richer assembly/interface extraction from coordinates
- full per-field `unknown` / `ambiguous` / `low_confidence` handling
- complete training-ready structural feature layer

### 2. Experimental Data Ingestion

Status: partially implemented

Implemented today:
- BindingDB live adapter
- ChEMBL exact-match activity adapter
- PDBbind local INDEX parser
- BioLiP local flat-file parser
- SKEMPI bulk CSV loader
- pair-aware assay merge rules that avoid collapsing assay types or mutations

Still missing or partial:
- BindingDB bulk/local ingestion path
- broader mutation disambiguation for external records
- literature mining / review queue for non-structured affinity data
- stronger assay-condition normalization across all sources

### 3. Biological Interaction Graph Builder

Status: architecture only

Implemented today:
- graph schema scaffolding
- graph architecture manifest writer

Missing:
- source ingestion for network databases
- identifier harmonization across UniProt / Entrez / Ensembl
- node and edge materialization
- evidence merging and graph persistence
- graph-derived features and embeddings

### 4. ML Training Data Generator

Status: architecture only

Implemented today:
- training example schema scaffolding
- training architecture manifest writer

Missing:
- example assembler from extracted + graph + feature layers
- label policy
- feature materialization
- export format and split-aware builders

## Highest-Priority Next Work

1. Build canonical graph raw/normalized layers and identifier mapping.
2. Add feature-layer materialization for structural, chemical, and graph features.
3. Implement training-example assembly from extracted records plus graph/features.
4. Replace remaining heuristic-only cases with explicit `unknown` / `ambiguous` markers where the source is insufficient.
