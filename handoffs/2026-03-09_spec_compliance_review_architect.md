---
task_id: spec_compliance_review
role: architect
date: 2026-03-09
allowed_files:
  - src/pbdata/cli.py
  - src/pbdata/prediction/__init__.py
  - src/pbdata/prediction/engine.py
  - src/pbdata/risk/__init__.py
  - src/pbdata/risk/summary.py
  - src/pbdata/schemas/conformational_state.py
  - src/pbdata/schemas/prediction_input.py
  - src/pbdata/schemas/features.py
  - src/pbdata/features/pathway.py
  - src/pbdata/graph/builder.py
  - src/pbdata/custom_training_set.py
  - tests/test_prediction_engine.py
  - tests/test_risk_scoring.py
  - tests/test_conformational_state.py
forbidden_files:
  - specs/canonical_schema.yaml
  - stress_test_panel.yaml
  - stress_test_panel_B.yaml
  - stress_test_panel_C.yaml
  - expected_outcomes_table.md
  - expected_outcomes_panel_B.md
  - expected_outcomes_panel_C.md
  - src/pbdata/schemas/records.py
  - src/pbdata/schemas/canonical_sample.py
  - src/pbdata/pipeline/extract.py
  - src/pbdata/pipeline/assay_merge.py
required_tests:
  - .venv/Scripts/python.exe -m pytest tests/ -q
status: proposed
---

# Architect Handoff

## Objective

Resolve 8 architectural violations found during review of the codebase against the master engineering spec (`bio_prediction_engine_instruction_pack/specs/MASTER_BUILD_SPEC.md` and companion specs).

## Why this change is needed

The master spec defines a 7-layer architecture. Layers 1-4 (Ingestion, Canonical Dataset, Feature, Interaction Graph) are implemented. Layers 5-7 (Prediction Engine, Pathway Reasoning, Risk Scoring) are referenced by CLI commands but have no backing modules. The CLI will crash with `ModuleNotFoundError` if those commands are invoked. Additional issues include untyped feature schemas, missing conformational state schemas, and CLI-level business logic that belongs in the pipeline layer.

## Scope

### Files allowed to change

- `src/pbdata/cli.py` — move adapter-instantiation helpers into pipeline layer
- `src/pbdata/prediction/__init__.py` — new package (Layer 5)
- `src/pbdata/prediction/engine.py` — new module: ligand screening + peptide binding stubs
- `src/pbdata/risk/__init__.py` — new package (Layers 6+7)
- `src/pbdata/risk/summary.py` — new module: pathway risk scoring stub
- `src/pbdata/schemas/conformational_state.py` — new Pydantic schema per CONFORMATIONAL_STATE_MODELING spec
- `src/pbdata/schemas/prediction_input.py` — new schema for SMILES/SDF/PDB/mmCIF/FASTA inputs
- `src/pbdata/schemas/features.py` — replace untyped `dict[str, Any]` with documented typed fields
- `src/pbdata/features/pathway.py` — wire into graph layer pathway nodes
- `src/pbdata/graph/builder.py` — emit pathway nodes by default (not only when external=True)
- `src/pbdata/custom_training_set.py` — read sequences from training layer instead of extracted/chains
- `tests/test_prediction_engine.py` — new tests for Layer 5
- `tests/test_risk_scoring.py` — new tests for Layers 6+7
- `tests/test_conformational_state.py` — new schema tests

### Files that must not change

- All spec files in `specs/`, `bio_prediction_engine_instruction_pack/`, `bio_agent_full_spec/`
- Stress test panels and expected outcome files
- Canonical schema (`schemas/canonical_sample.py`, `schemas/records.py`)
- Extraction pipeline (`pipeline/extract.py`, `pipeline/assay_merge.py`)
- Existing source adapters in `sources/`

## Required changes

### Critical (Layers 5-7 missing)

1. **Create `src/pbdata/prediction/` package** with `engine.py` containing:
   - `run_ligand_screening_workflow(layout, input_path, output_dir)` — validates input, writes workflow manifest, returns scaffold results
   - `run_peptide_binding_workflow(layout, input_path, output_dir)` — same pattern
   - Both must accept the 5 input formats defined in PREDICTION_PIPELINE spec Step 1
   - Implementation may be stub/scaffold but must not raise `NotImplementedError` silently — must write a manifest with `status: "planned"` and document missing capabilities

2. **Create `src/pbdata/risk/` package** with `summary.py` containing:
   - `build_pathway_risk_summary(layout, output_dir)` — reads graph layer pathway nodes + predicted affinities, writes risk summary JSON
   - Must implement the formula skeleton from RISK_SCORING_MODEL spec: `risk_score = (binding_weight * predicted_affinity) + (pathway_overlap_weight * pathway_similarity)`
   - May use placeholder weights but must write auditable output

3. **Create `src/pbdata/schemas/prediction_input.py`** — Pydantic v2 frozen model:
   - `input_type`: Literal["smiles", "sdf", "pdb", "mmcif", "fasta"]
   - `input_value`: str (raw content or file path)
   - `target_id`: str | None
   - Validators for each input type

### High (CLI layer violation)

4. **Move CLI extraction helpers into `pipeline/`**:
   - Extract `_fetch_bindingdb_samples_for_pdb()`, `_fetch_chembl_samples_for_raw()`, `_load_external_assay_samples()` from `cli.py` into a new function or existing module in `pipeline/extract.py` (or a new `pipeline/enrichment.py`)
   - CLI `extract` command should call the pipeline function, not directly instantiate adapters

### Medium

5. **Create `src/pbdata/schemas/conformational_state.py`** — Pydantic v2 frozen model with the 9 fields from CONFORMATIONAL_STATE_MODELING spec:
   - `target_id`, `state_id`, `pdb_id`, `structure_source`, `apo_or_holo`, `active_inactive_unknown`, `open_closed_unknown`, `ligand_class_in_state`, `conformation_cluster`

6. **Type the feature schema** — Replace `FeatureRecord.values: dict[str, Any]` with explicit optional fields or a documented TypedDict. At minimum, add a `KNOWN_FEATURE_KEYS` constant and validate at write time.

7. **Default pathway nodes in graph** — `graph/builder.py` should emit at least a placeholder pathway node set from extracted data (e.g., from UniProt cross-references) even when `enable_external=False`, so the graph always contains the three spec-required node types.

### Low

8. **Fix `custom_training_set.py` layer reach** — Read receptor sequences from `training_examples.json` (protein.sequence_length is already there) or `master_pdb_repository.csv` instead of directly globbing `extracted/chains/*.json`.

## Acceptance criteria

- `pbdata predict-ligand-screening` and `pbdata predict-peptide-binding` execute without `ModuleNotFoundError` and write manifest JSON
- `pbdata score-pathway-risk` executes without crash and writes risk summary JSON
- `pbdata build-conformational-states` writes records that validate against the new `ConformationalStateRecord` schema
- `FeatureRecord` values are validated against known keys at write time
- Default graph output contains at least one pathway-type node (even if placeholder)
- CLI `extract` command no longer directly instantiates `BindingDBAdapter` or `ChEMBLAdapter`
- All existing tests (263 unit, 71 integration) continue to pass
- New tests cover prediction engine stubs, risk scoring stubs, and conformational state schema

## Constraints

- New modules for Layers 5-7 may be scaffolds/stubs but must write auditable output files (JSON manifests with `status` field), not raise `NotImplementedError`
- Do not change the canonical schema or multi-table record schemas
- Do not change existing source adapter interfaces
- Do not modify stress test panels or expected outcomes
- Preserve all existing CLI command signatures (no breaking changes)
- All new schemas must be frozen Pydantic v2 models with provenance

## Risks to avoid

- Breaking existing `extract` command behavior when moving helpers out of CLI
- Introducing circular imports between prediction/risk modules and existing layers
- Over-engineering stubs — keep Layer 5-7 scaffolds minimal and honest about their planned status
- Changing feature key names in `builder.py` without updating `assembler.py` consumers

## Tests to run

```bash
# Full unit test suite (must remain green)
.venv/Scripts/python.exe -m pytest tests/ -q

# New tests for added modules
.venv/Scripts/python.exe -m pytest tests/test_prediction_engine.py tests/test_risk_scoring.py tests/test_conformational_state.py -v
```

## Expected implementation output

Implementation report (`handoffs/2026-03-09_spec_compliance_review_implementation.md`) with:
- Unified diff for all changed files
- New module listings for prediction/, risk/, and new schemas
- Test commands and results
- Confirmation that all 8 issues are addressed or explicitly deferred with rationale
