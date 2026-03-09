
# MASTER BUILD SPEC

This document is the authoritative specification for the entire system.

If any other instruction conflicts with this file, this file takes precedence.

The project consists of four primary subsystems:

1. STRUCTURE EXTRACTION PIPELINE
2. EXPERIMENTAL DATA INGESTION
3. BIOLOGICAL INTERACTION GRAPH BUILDER
4. ML TRAINING DATA GENERATOR

## Core Design Rule

No data should be silently inferred.

If data is uncertain, store:

unknown
ambiguous
low_confidence

## Data Layers

1 Raw layer  
2 Normalized canonical layer  
3 Feature layer  
4 Training example layer

