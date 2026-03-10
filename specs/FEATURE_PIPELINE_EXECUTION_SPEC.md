
# FEATURE PIPELINE EXECUTION SPECIFICATION
Version: 1.0
Status: Authoritative
Applies to: All coding agents implementing feature generation, caching, orchestration, and model-input preparation

This document defines the exact execution plan for the feature pipeline in Bio-Agent-Lab.
It specifies pipeline stages, job boundaries, caching rules, CPU/GPU assignment, artifact formats,
failure handling, and merge requirements.

This specification is the operational companion to:

- specs/MASTER_ENGINEERING_INSTRUCTIONS.md
- specs/SITE_CENTRIC_PHYSICS_SPEC.md
- specs/AGENT_OUTPUT_REQUIREMENTS.md
- specs/canonical_schema.yaml

If any implementation conflicts with this file, this file controls pipeline execution behavior.

------------------------------------------------------------
# 1. PURPOSE

The feature pipeline transforms normalized canonical dataset records into reusable,
cached, versioned feature artifacts for model training and inference.

The feature pipeline must:

1. consume canonical dataset artifacts only
2. never re-ingest raw data during feature generation
3. produce deterministic, versioned feature outputs
4. support resumable execution
5. support CPU/GPU stage separation
6. support offline expensive physics stages
7. support precomputation for training and fast inference for prediction

------------------------------------------------------------
# 2. PIPELINE LAYERS

The pipeline is divided into seven execution layers:

1. canonical input resolution
2. structure preparation
3. base feature extraction
4. site-centric physics enrichment
5. graph construction
6. training example assembly
7. validation, reporting, and export

Each layer must be implemented as a distinct stage with explicit inputs and outputs.

------------------------------------------------------------
# 3. DIRECTORY CONTRACT

The pipeline must use the following artifact directories:

data/
  raw/
  interim/
  processed/
  reports/

artifacts/
  canonical/
  prepared_structures/
  base_features/
  site_physics/
  graphs/
  training_examples/
  caches/
  logs/
  manifests/

features/
  structural_features/
  ligand_features/
  interface_features/
  graph_features/

handoffs/

No stage may write outputs into another stage's directory except through the declared artifact contract.

------------------------------------------------------------
# 4. INPUT CONTRACT

The feature pipeline may only start from artifacts produced by the canonical dataset layer.

Required inputs:
- canonical entry table
- canonical chain table
- canonical bound_object table
- canonical interface table
- canonical assay table
- canonical provenance table

Input sources must be version-pinned and recorded in a manifest.

Each run must create:
artifacts/manifests/<run_id>_input_manifest.json

The manifest must record:
- schema_version
- pipeline_version
- source dataset versions
- git commit hash if available
- run timestamp
- task_id
- row counts by entity table

------------------------------------------------------------
# 5. RUN MODES

The pipeline must support exactly four run modes:

1. full_build
   - build all stages from canonical inputs to training examples

2. resume
   - continue from the last successful completed stage using manifest state

3. stage_only
   - run one named stage without implicitly running upstream stages
   - only allowed if required inputs already exist

4. inference_prepare
   - run minimal feature generation for user prediction workflows
   - must use cached surrogates and must not launch offline archetype physics jobs

------------------------------------------------------------
# 6. STAGE DEFINITIONS

## Stage 1: canonical input resolution

Purpose:
Resolve the exact canonical records to process for a given dataset shard or prediction request.

Inputs:
- canonical entity tables
- run configuration

Outputs:
- resolved record list
- stage manifest

Artifacts:
artifacts/manifests/<run_id>_stage1_resolved_records.parquet
artifacts/manifests/<run_id>_stage1_manifest.json

Required operations:
- validate schema version
- validate required columns
- validate existence of structure references
- shard record set if needed

Failure behavior:
- fail fast on missing required canonical fields
- do not silently drop rows
- emit explicit rejection report for invalid records

------------------------------------------------------------
## Stage 2: structure preparation

Purpose:
Prepare structure files and map structural entities needed for feature extraction.

Inputs:
- resolved record list
- mmCIF structure files
- optional PDB fallback files
- optional apo/holo paired structures

Outputs:
- prepared structure objects
- chain/entity maps
- site candidate records
- apo/holo mapping table if available

Artifacts:
artifacts/prepared_structures/<run_id>/<record_id>.prepared.parquet
artifacts/prepared_structures/<run_id>/<record_id>.sites.parquet
artifacts/prepared_structures/<run_id>/<record_id>.state_map.parquet

Required operations:
- load mmCIF preferentially
- map polymer entities to chains
- identify ligand objects, metals, glycans, and pseudo-sites
- identify site motif class candidates per specs/SITE_CENTRIC_PHYSICS_SPEC.md
- generate deterministic atom/site IDs
- pair apo/holo states if available

CPU/GPU:
- CPU only

Failure behavior:
- record structure-level errors in per-record logs
- preserve failure manifest
- continue processing other records unless run config is fail_hard=true

------------------------------------------------------------
## Stage 3: base feature extraction

Purpose:
Compute fast deterministic structural, ligand, interface, and environment features.

Inputs:
- prepared structures
- site candidate records

Outputs:
- base node features
- base edge candidates
- environment descriptors
- base global graph features

Artifacts:
artifacts/base_features/<run_id>/<record_id>.node_base.parquet
artifacts/base_features/<run_id>/<record_id>.edge_base.parquet
artifacts/base_features/<run_id>/<record_id>.env_vectors.parquet
artifacts/base_features/<run_id>/<record_id>.global_base.json

Required operations:
- compute geometry features
- compute SASA / burial / residue depth
- compute donor/acceptor counts and shell summaries
- compute electrostatic proxy summaries
- compute B-factor / occupancy summaries
- compute local environment vectors exactly as defined in specs/SITE_CENTRIC_PHYSICS_SPEC.md
- compute apo/holo delta-ready mappings where possible

CPU/GPU:
- CPU preferred
- vectorized numeric kernels allowed

Failure behavior:
- if one feature family fails, mark feature family failed, do not fabricate zeros unless explicitly configured
- all missing features must be flagged

------------------------------------------------------------
## Stage 4: site-centric physics enrichment

Purpose:
Convert base site environments into refined physics-aware descriptors.

Inputs:
- environment vectors
- local neighborhood graphs
- surrogate model checkpoint
- cache store
- optional offline archetype library

Outputs:
- refined node descriptors
- refinement provenance
- cache hit/miss summary

Artifacts:
artifacts/site_physics/<run_id>/<record_id>.site_refined.parquet
artifacts/site_physics/<run_id>/<record_id>.physics_provenance.parquet
artifacts/site_physics/<run_id>/<record_id>.cache_stats.json

Required operations:
- compute environment hash = hash(motif_class + canonicalized environment vector)
- lookup cache before inference
- batch surrogate inference by motif family where possible
- attach refined descriptors:
  - refined_partial_charge
  - electrostatic_potential
  - electric_field_magnitude
  - donor_strength
  - acceptor_strength
  - polarizability_proxy
  - steric_radius
  - desolvation_penalty
  - protonation_preference
  - metal_binding_propensity
  - aromatic_interaction_propensity

CPU/GPU:
- GPU preferred if surrogate uses GNN
- CPU fallback required

Critical rule:
- no QM/archetype calculations may run in this stage
- this stage only uses precomputed surrogate and cache

Failure behavior:
- if surrogate unavailable, fail stage
- do not silently fall back to generic atom constants unless explicitly configured and flagged as degraded_mode=true

------------------------------------------------------------
## Stage 5: graph construction

Purpose:
Build enriched graphs for proteins, ligands, complexes, and interfaces.

Inputs:
- base node features
- base edge candidates
- refined site descriptors
- interface tables
- optional pathway/graph context inputs

Outputs:
- enriched graph object
- node table
- edge table
- graph-level metadata

Artifacts:
artifacts/graphs/<run_id>/<record_id>.graph.pt
artifacts/graphs/<run_id>/<record_id>.nodes.parquet
artifacts/graphs/<run_id>/<record_id>.edges.parquet
artifacts/graphs/<run_id>/<record_id>.graph_meta.json

Required operations:
- attach refined descriptors to node features
- compute interaction priors for candidate edges:
  - coulombic_proxy
  - hbond_geometry_score
  - salt_bridge_score
  - vdW_overlap_score
  - steric_clash_score
  - aromatic_stack_score
  - metal_bridge_flag
- preserve deterministic node ordering
- support atom-level, site-level, or mixed graph representations only if declared in config

CPU/GPU:
- CPU preferred for assembly
- GPU optional for heavy neighbor graph construction

Failure behavior:
- graph serialization errors must fail record
- malformed graphs must never be emitted

------------------------------------------------------------
## Stage 6: training example assembly

Purpose:
Package graphs and labels into model-ready training examples.

Inputs:
- enriched graphs
- assay labels
- metadata
- split annotations if available

Outputs:
- training examples
- label tables
- training manifest

Artifacts:
artifacts/training_examples/<run_id>/<record_id>.example.pt
artifacts/training_examples/<run_id>/<record_id>.label.json
artifacts/training_examples/<run_id>/<record_id>.meta.json
artifacts/training_examples/<run_id>/manifest.parquet

Required operations:
- attach label targets:
  - KD
  - ΔG
  - class labels
  - off-target labels where applicable
- attach provenance references
- attach task type
- attach split group IDs if precomputed
- attach degradation flags

CPU/GPU:
- CPU only

Failure behavior:
- records with missing mandatory labels may be exported only to inference-ready examples, not supervised training examples
- this must be explicit in metadata

------------------------------------------------------------
## Stage 7: validation, reporting, export

Purpose:
Validate artifact completeness and generate run summary.

Inputs:
- training examples
- stage manifests
- cache statistics
- logs

Outputs:
- run report
- coverage report
- failure report
- performance report

Artifacts:
artifacts/reports/<run_id>_summary.md
artifacts/reports/<run_id>_coverage.json
artifacts/reports/<run_id>_failures.json
artifacts/reports/<run_id>_performance.json

Required operations:
- count records processed per stage
- count records failed per stage
- summarize cache hit rate
- summarize missing descriptor rates
- summarize degraded-mode usage
- verify deterministic row counts across stage transitions

------------------------------------------------------------
# 7. PIPELINE ORCHESTRATION RULES

The orchestrator must support:
- stage dependency graph
- resumable runs
- shard-aware execution
- parallel per-record execution where safe
- centralized manifest writing

Each stage must write:
artifacts/manifests/<run_id>_<stage_name>_status.json

Required fields:
- stage_name
- run_id
- start_time
- end_time
- status
- records_attempted
- records_succeeded
- records_failed
- upstream_dependencies
- output_artifacts
- warnings

Allowed status:
- pending
- running
- passed
- failed
- partial

------------------------------------------------------------
# 8. CPU / GPU ASSIGNMENT

The following execution split is mandatory by default.

CPU stages:
- canonical input resolution
- structure preparation
- base feature extraction
- training example assembly
- reporting/export

GPU-preferred stages:
- site-centric surrogate inference
- optional heavy graph batching
- downstream model training

If no GPU is present:
- all stages must still run
- pipeline must emit gpu_unavailable=true in run manifest

------------------------------------------------------------
# 9. CACHING RULES

Caching is mandatory.

Cache classes:
1. structure parse cache
2. environment vector cache
3. surrogate descriptor cache
4. graph assembly cache where safe

Cache keys must include:
- schema_version
- feature_pipeline_version
- motif taxonomy version
- surrogate checkpoint ID
- input structure hash
- state label if applicable

No cache entry may be reused across incompatible version boundaries.

Each run must emit:
artifacts/caches/<run_id>_cache_manifest.json

------------------------------------------------------------
# 10. VERSIONING RULES

The following version identifiers must be recorded in every run:
- schema_version
- feature_pipeline_version
- site_physics_spec_version
- surrogate_model_version
- graph_representation_version
- training_example_version

Every artifact must be traceable back to these versions.

------------------------------------------------------------
# 11. FAILURE HANDLING RULES

The pipeline must prefer explicit failure over silent degradation.

Required behaviors:
- every failed record must produce a structured error entry
- every degraded record must be explicitly labeled degraded_mode=true
- no stage may silently fill unknown physics descriptors with generic defaults unless configured and flagged
- missing apo/holo state is not a failure; it is a missing optional feature and must be flagged accordingly
- malformed mmCIF parsing is a record-level failure

Each run must produce:
artifacts/logs/<run_id>_structured_errors.jsonl

------------------------------------------------------------
# 12. CONFIGURATION RULES

A pipeline run must be driven by a config object or YAML file containing:
- run_mode
- dataset shard or input selection
- graph representation type
- batch sizes
- CPU worker count
- GPU enable flag
- fail_hard flag
- degraded_mode policy
- cache policy
- output compression policy

No hidden global defaults may change scientific behavior without being recorded in the manifest.

------------------------------------------------------------
# 13. PERFORMANCE REQUIREMENTS

Performance improvements are required, but must not sacrifice correctness.

Mandatory speed strategies:
1. vectorize base feature computation
2. batch surrogate inference
3. shard record processing
4. precompute descriptors before training
5. use Parquet for tabular artifacts
6. use memory-efficient graph serialization
7. reuse cached environment descriptor results aggressively across homologous or repeated structures only when version-compatible

Forbidden speed shortcuts:
- dropping provenance
- skipping validation
- replacing failed calculations with zero vectors without flags
- skipping shell-based descriptor computation
- silently truncating large neighborhoods

Each performance optimization must include:
- expected speedup
- correctness risk
- validation plan

------------------------------------------------------------
# 14. USER-FACING INFERENCE EXECUTION

Inference_prepare mode must:
- start from user input structure or ligand representation
- normalize to canonical-like temporary record
- prepare structures
- compute base features
- run cached surrogate enrichment
- build prediction graph

Inference_prepare mode must not:
- launch archetype clustering
- launch offline QM physics jobs
- rebuild the training corpus

The temporary run manifest must still be written.

------------------------------------------------------------
# 15. AGENT ROLE REQUIREMENTS

## Architect agent
Must define:
- stage boundaries
- allowed files
- acceptance criteria
- version impacts

Output artifact required by:
specs/AGENT_OUTPUT_REQUIREMENTS.md

## Implementation agent
Must:
- modify only scoped pipeline files
- return unified diff
- run targeted stage tests

## Reviewer agent
Must check:
- stage boundary violations
- cache invalidation correctness
- silent degradation
- reproducibility issues

## User simulation tester
Must execute end-to-end scenarios such as:
- ligand off-target prediction
- peptide structure prediction fallback
- inference_prepare workflow
- resume after interrupted feature run

The user tester must report usability and undesirable states, not just crashes.

------------------------------------------------------------
# 16. ACCEPTANCE TEST MATRIX

The feature pipeline is acceptable only if the following tests exist and pass.

## Deterministic tests
1. environment vectors are deterministic for the same input
2. site motif assignment is deterministic
3. cache keys change when versions change
4. surrogate inference reproduces fixed outputs on a snapshot fixture
5. graph node ordering is deterministic
6. training example manifests contain complete provenance references

## Resume tests
7. interrupted run resumes from the correct next stage
8. stage_only mode fails if dependencies are missing
9. full_build followed by resume does not duplicate outputs

## Degraded mode tests
10. missing surrogate checkpoint fails explicitly unless degraded_mode policy allows fallback
11. degraded outputs are flagged in metadata

## Performance tests
12. batch surrogate inference outperforms single-site inference on fixture benchmark
13. cache hit path is faster than cold path and yields identical descriptors

## User-simulation scenario tests
14. ligand off-target workflow produces ranked outputs and confidence values
15. peptide binding workflow handles structure input and reports missing optional fields clearly
16. malformed structure input yields clear error report, not silent failure

------------------------------------------------------------
# 17. REQUIRED OUTPUT FILES FOR EACH FULL RUN

A successful full_build run must produce at minimum:

- input manifest
- per-stage status manifests
- prepared structure artifacts
- base feature artifacts
- site physics artifacts
- graph artifacts
- training example artifacts
- structured error log
- coverage report
- summary report
- performance report

If any required output class is missing, the run is incomplete.

------------------------------------------------------------
# 18. RECOMMENDED FILES AND MODULES

The codebase should expose modules similar to:

src/.../pipeline/resolve_inputs.py
src/.../pipeline/prepare_structures.py
src/.../pipeline/extract_base_features.py
src/.../pipeline/run_site_physics.py
src/.../pipeline/build_graphs.py
src/.../pipeline/assemble_examples.py
src/.../pipeline/report_run.py
src/.../pipeline/orchestrator.py

This is a recommendation, but stage separation is mandatory even if filenames differ.

------------------------------------------------------------
# 19. FINAL RULE

The feature pipeline must be:
- deterministic
- resumable
- versioned
- cache-aware
- scientifically conservative
- explicit about uncertainty and degradation

The pipeline must prefer:
explicit artifacts and explicit failure states

over:
implicit behavior and silent approximation
