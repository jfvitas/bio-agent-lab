# Repo Contract

## Purpose
This repository builds auditable, versioned protein-binding ML datasets from multiple sources.

## Non-negotiable requirements
- All normalized records must conform to `specs/canonical_schema.yaml`
- Every transformed field must preserve provenance
- No source adapter may write directly to final datasets without normalization
- No incompatible assay types may be merged without explicit transform logic
- All outputs must be reproducible from config files
- All modules must be typed
- All major functions must have tests

## Phase 1 deliverables
- package structure
- canonical schema models
- CLI skeleton
- config loader
- logging
- tests
