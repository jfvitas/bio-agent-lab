# Full-Scope Stub Checklist

This file tracks the explicit stub modules that still need real implementations.

## Graph

- `src/pbdata/graph/connectors.py`
  - add one connector per external graph source
  - define raw download and canonical edge normalization
- `src/pbdata/graph/identifier_map.py`
  - implemented for exact UniProt-centered mapping
  - still needs bulk caching and stricter ambiguity handling

## Features

- `src/pbdata/features/pathway.py`
  - populate pathway counts and pathway-derived feature vectors
- `src/pbdata/features/mm_features.py`
  - add optional Rosetta / CHARMM / AMBER feature computation

## Training

- `src/pbdata/training/assembler.py`
  - define example identity and label policy
  - join extracted, assay, feature, and graph layers
  - define split-aware export behavior

## Rules

- Keep `unknown`, `ambiguous`, and `low_confidence` explicit.
- Do not silently merge mutation-specific records.
- Do not add external graph/pathway data without identifier harmonization.
