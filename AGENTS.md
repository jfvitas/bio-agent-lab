# AGENTS.md

## Project mission

This repository is being developed into a large-scale, GUI-driven protein data platform for building machine learning datasets and analysis workflows related to protein binding, molecular interactions, structural biology, and broader biological context.

This is not just a simple parser or single-database pipeline. The long-term goal is a broad, modular, scalable platform that can ingest, normalize, and connect many biological data sources.

## Product priorities

1. GUI-driven workflow
2. Broad source coverage
3. Strong provenance and reproducibility
4. Canonical entity resolution
5. Robust dataset curation
6. Leakage-resistant train/test splitting
7. Scalability to much larger datasets and compute environments

## Important functional expectations

- A root file location should be configurable by the user and all managed data should live under that root.
- The GUI should clearly explain the process flow.
- The interface should be intuitive and usable on screen without awkward overflow.
- Scrollable panels should support mouse-wheel scrolling from anywhere over the panel.
- Search and ingestion should support multiple data sources.
- If a result limit is used, the platform should try to return broad, representative data rather than a narrow cluster of near-duplicates.

## Data-source philosophy

The platform should evolve toward integrating categories such as:
- experimental structures
- predicted structures
- binding/affinity/bioactivity data
- sequence/evolution/domain annotations
- structural classifications
- interaction networks
- pathways
- kinetics/assay context
- motifs/sites/domains

Use modular adapters so sources can be added without rewiring the whole application.

## Architecture expectations

Prefer modular subsystems for:
- source adapters / ingestion
- canonical identity mapping
- storage and caching
- structural parsing
- feature extraction
- dataset generation
- split generation
- graph/relationship representation
- GUI orchestration

## Dataset quality expectations

Do not rely on random row splits alone.

Support or prepare for:
- sequence identity-based grouping/splitting
- fold/class/family-aware grouping
- source-held-out evaluation
- mutation-cluster-aware grouping
- deduplication and near-duplicate handling

The purpose is to support real generalization, not inflated benchmark scores.

## Coding expectations

- Inspect before refactoring
- Preserve good existing work
- Avoid breaking working flows unnecessarily
- Keep modules clean and well named
- Add docstrings/comments where useful
- Wire new code into the actual application
- Avoid dead scaffolding unless it clearly enables later integration

## Validation expectations

- Run existing tests where possible
- Add focused tests for critical logic
- Verify GUI launch if feasible
- Verify imports and dependency integrity
- Use smoke tests if full external ingestion is too large

## Decision rules

Prefer:
- extensibility
- provenance
- reproducibility
- explicit mappings
- modular design
- representative datasets
- future scalability

Avoid:
- hardcoded one-off logic
- silent data ambiguity
- hidden identifier assumptions
- giant unstructured downloads without normalization
- designs that only work for toy datasets

## Current execution plan

The current mission is to move the repository from a mixed demo-plus-functional
state into a fully usable local-first platform.

The highest-priority execution rules are:

- Treat demo/presentation artifacts and functional/live artifacts as separate
  products. Do not allow simulated stage-state or seeded walkthrough data to be
  mistaken for real functional outputs.
- Make the bootstrap store the canonical fast local planning layer for source
  coverage, training-set design, refresh planning, and graph targeting.
- Build a real source prepopulation/update/delete policy for every source we
  materially depend on. Every source should have explicit rules for:
  - initial local packaging
  - freshness checks
  - targeted refresh
  - stale-file retention
  - deletion / garbage collection
- Audit every screening and dataset-selection field before using it in policy.
  Any field that is empty, poorly populated, scientifically ambiguous, or only
  nominally present must either be populated properly, marked as advisory only,
  or removed from decision-making.
- Make training-set creation, split generation, engineered-dataset export,
  graph coverage accounting, model recommendation, and training fully real in
  functional mode. The platform should not rely on seeded scorecards, seeded
  split files, or simulated stage-state for those paths.
- Preserve and expand the full graph design space. Do not collapse the system
  into one graph representation when the platform is intended to support
  multiple graph scopes, granularities, and export targets.
- Make Model Studio robust rather than aspirational. Expose only model-family
  options that are truly supported in functional mode as first-class runnable
  paths, and clearly mark roadmap architectures as planned when they are not yet
  executable end to end.
- Continue improving the WinUI shell as the polished presentation surface, but
  ensure that functional mode is equally real and trustworthy.
- Prefer validation that proves real usability:
  - focused regression tests
  - smoke checks
  - GUI build and launch checks
  - artifact verification
  - explicit detection of simulated versus live workflow state

For the fuller phased roadmap, acceptance criteria, and external acquisition
checklist, see:

- `docs/roadmap_to_fully_usable_platform.md`
