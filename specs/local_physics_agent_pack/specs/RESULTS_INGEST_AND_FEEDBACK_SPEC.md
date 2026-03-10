# RESULTS INGEST AND FEEDBACK SPEC
Version: 1.0
Status: Authoritative
Applies to: Implementation, reviewer, QA, and human operator

## 1. Purpose

This file defines how offline analysis results are fed back into the project and how the coding agents
should use those results to build the surrogate system.

## 2. Principle

External analysis outputs are not manually copied into model code.
They must be ingested through a reproducible results-ingest pipeline.

## 3. Required ingest stages

### Stage 1: raw parse
Read raw ORCA/APBS/OpenMM outputs and extract machine-readable property fields.

### Stage 2: quality validation
Check for:
- missing central site mapping
- malformed fragment IDs
- missing charge/potential values
- failed jobs
- impossible values

### Stage 3: normalization
Map parsed outputs into the normalized site-physics target schema.

### Stage 4: merge
Merge ORCA/APBS/OpenMM-derived properties by fragment_id and archetype_id.

### Stage 5: target table export
Write:
- physics_targets.parquet
- physics_target_manifest.json
- failed_fragments.parquet

## 4. Feedback into coding agents

The coding agents do not need to understand chemistry manually.
They need structured artifacts.

The handoff back into the coding agents should be:

1. batch manifest
2. normalized target table
3. quality report
4. failure report
5. example rows
6. exact paths to files

The implementation agents then build:
- surrogate training dataset loader
- surrogate training code
- inference code
- graph enrichment code
against those artifacts.

## 5. Human decision points

These are the specific steps you perform separately outside normal project code:

### Human step H1
Choose the source structure corpus to use for environment extraction.

### Human step H2
Choose the first motif classes to prioritize if not all are built at once.

### Human step H3
Launch ORCA/APBS/OpenMM jobs on exported fragment batches.

### Human step H4
Inspect failed jobs only when the automated parser reports systematic failures.

### Human step H5
Approve the resulting normalized target table for surrogate training.

Everything else should be automated by the project.

## 6. How you know what to analyze

You do not manually pick arbitrary residues one by one.
The project should pick what to analyze using this exact procedure:

1. extract all site environments from the structure corpus
2. group by motif_class
3. cluster each motif_class
4. choose 20–60 representative archetypes per motif_class
5. export those archetypes as the analysis queue

That is how you know what to analyze.

If you need a limited version 1 rollout, analyze these motif classes first:
- backbone_carbonyl_oxygen
- backbone_amide_nitrogen
- asp_carboxylate_oxygen
- glu_carboxylate_oxygen
- lys_terminal_amine_nitrogen
- his_delta_nitrogen
- his_epsilon_nitrogen
- ser_hydroxyl_oxygen
- tyr_hydroxyl_oxygen
- carbonyl_oxygen
- carboxylate_oxygen
- amine_nitrogen
- aromatic_nitrogen
- aromatic_centroid
- metal_ion

## 7. What gets fed back into the codebase

The coding models need these project-local artifacts only:

- `artifacts/physics_targets/<batch_id>/physics_targets.parquet`
- `artifacts/physics_targets/<batch_id>/physics_target_manifest.json`
- `artifacts/physics_targets/<batch_id>/failed_fragments.parquet`

The agents should then build code against these stable files, not raw ORCA text outputs.

## 8. Acceptance test for ingest

The ingest system is correct if:
- raw analysis files are parsed reproducibly
- target rows map correctly to archetype_id and motif_class
- failed jobs remain represented explicitly
- the surrogate training pipeline can consume the exported target table without manual editing
