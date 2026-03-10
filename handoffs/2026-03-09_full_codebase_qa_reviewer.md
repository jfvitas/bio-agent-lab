---
task_id: full_codebase_qa
role: reviewer
date: 2026-03-09
allowed_files: []
forbidden_files:
  - stress_test_panel.yaml
  - stress_test_panel_B.yaml
  - stress_test_panel_C.yaml
  - expected_outcomes_table.md
  - expected_outcomes_panel_B.md
  - expected_outcomes_panel_C.md
required_tests:
  - .venv/Scripts/python.exe -m pytest tests/ -q
status: failed
---

# Review Report

## Scope reviewed

Full codebase evaluation against:
- `specs/bio_agent_full_instruction_pack/qa/undesirable_state_rubric.md` (primary rubric)
- `specs/bio_agent_full_instruction_pack/specs/MASTER_ENGINEERING_INSTRUCTIONS.md` (spec authority)
- `handoffs/2026-03-09_spec_compliance_review_architect.md` (architect findings)
- `handoffs/2026-03-09_scenario_test_execution_user_tester.md` (user simulation findings)

Modules audited: `prediction/engine.py`, `risk/summary.py`, `qa/scenario_runner.py`, `reports/bias.py`, `dataset/conformations.py`, `features/pathway.py`, `schemas/features.py`, `schemas/graph.py`, `schemas/training_example.py`, `cli.py` (21 commands).

## Pass / Fail decision

**FAIL**

4 critical issues, 4 major issues, 5 minor issues. The critical issues involve silent acceptance of invalid/missing inputs in the prediction layer and a misleading self-assessment in the QA system. These violate the master spec's final principle: "The system must prefer explicit uncertainty over incorrect certainty."

---

## Critical issues

### C1 — Invalid SMILES accepted without validation
- **Rubric category:** Scientific Failure — "misleading certainty when evidence weak"
- **Severity:** Critical
- **Location:** `src/pbdata/prediction/engine.py`, `_detect_input_type()` line 30
- **Description:** `PredictionInputRecord` is instantiated for SMILES validation but the result is not checked. Any string is accepted as SMILES. The manifest reports `status: "workflow_ready_no_trained_model"` and `normalized_input_type: "SMILES"` for garbage input like `NOT_A_SMILES`. A medicinal chemist would trust this.
- **Evidence:** User tester ran `predict-ligand-screening --smiles "NOT_A_SMILES"` — produced success manifest with no warning.
- **Suggested fix:** Capture `PredictionInputRecord` validation result. If it raises `ValidationError`, propagate a clear error. At minimum, validate basic SMILES character set and bracket balance.

### C2 — Nonexistent structure file accepted silently
- **Rubric category:** Functional Failure — "required fields missing"
- **Severity:** Critical
- **Location:** `src/pbdata/prediction/engine.py`, `_detect_input_type()` lines 33-34, `run_peptide_binding_workflow()` lines 106-110
- **Description:** Structure file path is only examined for its suffix (`.cif` vs `.pdb`). No `Path.exists()` check. A nonexistent file produces a manifest with `status: "workflow_ready_no_trained_model"` referencing a path that does not exist on disk.
- **Evidence:** User tester ran `predict-peptide-binding --structure-file "does_not_exist.cif"` — produced success manifest.
- **Suggested fix:** Add `if not Path(structure_file).is_file(): raise FileNotFoundError(...)` before type detection.

### C3 — Scenario self-test produces false passes
- **Rubric category:** Scientific Failure — "misleading certainty when evidence weak"
- **Severity:** Critical
- **Location:** `src/pbdata/qa/scenario_runner.py`, lines 29-46
- **Description:** `run_scenario_templates()` determines expected output availability by checking **file existence only** (e.g., does `model_ready_pairs.csv` exist?). It does not open files or verify content. If the CSV is empty or all prediction fields are null, the scenario still reports `missing_expected_outputs: []` and `severity: "low"`. This is a false pass that would deceive a QA engineer.
- **Evidence:** Both scenarios report no missing outputs despite every prediction value being null.
- **Suggested fix:** After file-existence check, open the file, verify it has >0 data rows (CSV) or non-null prediction fields (JSON). Flag fields that are structurally present but semantically empty.

### C4 — Zero CLI error handling across all 4 prediction/risk/QA commands
- **Rubric category:** UX Failure — "confusing CLI outputs", "unclear error messages"
- **Severity:** Critical
- **Location:** `src/pbdata/cli.py` — `predict_ligand_screening_cmd`, `predict_peptide_binding_cmd`, `score_pathway_risk_cmd`, `run_scenario_tests_cmd`
- **Description:** None of the four commands have any try/except wrapping. All four expose raw Python tracebacks to the user on any error. Confirmed: running `predict-ligand-screening` with no arguments produces a `ValueError` traceback with internal file paths and line numbers.
- **Evidence:** User tester confirmed raw traceback for no-input case.
- **Suggested fix:** Wrap each command body in `try: ... except (ValueError, FileNotFoundError, OSError) as exc: typer.echo(f"Error: {exc}"); raise typer.Exit(code=1)`.

---

## Major issues

### M1 — Status string "workflow_ready" is misleading
- **Rubric category:** Scientific Failure — "misleading certainty when evidence weak"
- **Severity:** Major
- **Location:** `src/pbdata/prediction/engine.py`, lines 73, 117
- **Description:** Both prediction commands write `status: "workflow_ready_no_trained_model"`. The word "ready" implies the workflow can produce results. In reality, every output field is null. The `notes` field explains the limitation, but status is the first thing a user or automated system checks.
- **Suggested fix:** Change to `scaffold_only_no_predictions` or `input_accepted_no_model_available`. Reserve "ready" for states that can produce partial results.

### M2 — Pathway risk accepts empty target list
- **Rubric category:** Scientific Failure — "pathway reasoning based on incomplete data"
- **Severity:** Major
- **Location:** `src/pbdata/risk/summary.py` line 29; `src/pbdata/cli.py` `score_pathway_risk_cmd`
- **Description:** `--targets` is optional. When omitted, the summary runs with `targets_requested: []`, produces zero matches, and writes a manifest that looks structurally complete. A user may not notice their targets were never processed.
- **Suggested fix:** If targets list is empty after parsing, emit a warning via `typer.echo("Warning: no targets provided, results will be empty.")` and set status to `no_targets_provided`.

### M3 — Graph schemas missing provenance fields
- **Rubric category:** Engineering Failure — "hidden assumptions"
- **Severity:** Major
- **Location:** `src/pbdata/schemas/graph.py`
- **Description:** Neither `GraphNodeRecord` nor `GraphEdgeRecord` has a `provenance` field. Master spec section 6 requires "Every extracted field must include provenance." Graph edges have `source_database` but no `retrieved_at`, `confidence`, or structured provenance dict. Nodes have `source_databases` (a list) but no per-node provenance.
- **Suggested fix:** Add `provenance: dict[str, str | None] = {}` to both schemas, consistent with all other record types in the codebase.

### M4 — Pathway feature layer is a non-functional stub
- **Rubric category:** Engineering Failure — "dead code"
- **Severity:** Major
- **Location:** `src/pbdata/features/pathway.py`
- **Description:** Contains only a `PathwayFeaturePlan` dataclass with `status: str = "stub"` and a planning function that returns a stub. No disk I/O, no feature computation, no integration with graph pathway nodes. The master spec section 7 requires graph features including "pathway membership" and "interaction clusters". This module does not deliver.
- **Suggested fix:** Wire into `graph/builder.py` output. At minimum, count pathway edges per protein node and emit a FeatureRecord. If external connectors are unavailable, emit a record with `pathway_count: 0` and `provenance.confidence: "no_external_sources"`.

---

## Minor issues

### m1 — Field name mismatch: `candidate_targets_preview` vs `ranked_target_list`
- **Rubric category:** UX Failure — "results not discoverable"
- **Severity:** Minor
- **Location:** `src/pbdata/prediction/engine.py` line 75 vs `scenario_test_templates.yaml` line 11
- **Description:** Scenario expects `ranked_target_list`, manifest uses `candidate_targets_preview`. The scenario runner internally maps between them, but users reading the manifest would not find the expected field.
- **Suggested fix:** Rename to `ranked_target_list` in engine.py output.

### m2 — Silent empty-list return when CSVs are missing
- **Rubric category:** Engineering Failure — "hidden assumptions"
- **Severity:** Minor
- **Location:** `src/pbdata/prediction/engine.py` line 16-17; `src/pbdata/risk/summary.py` line 16-17
- **Description:** `_read_csv()` returns `[]` when the file does not exist. No log message, no warning. Downstream code silently processes empty data. The user sees `candidate_target_count: 0` with no explanation.
- **Suggested fix:** Add `logger.warning("CSV not found: %s", path)` before returning empty list.

### m3 — Bias report uses "unknown" masking for missing fields
- **Rubric category:** Scientific Failure — "misleading certainty when evidence weak"
- **Severity:** Minor
- **Location:** `src/pbdata/reports/bias.py` lines 35, 38, 46-50
- **Description:** Missing `task_hint`, `experimental_method`, `resolution_bin`, and `component_inchikey` all fall back to the string `"unknown"`. The bias report counts these as a category. A user might interpret "unknown: 1128" in scaffold diversity as a real category rather than missing data.
- **Suggested fix:** Separate "unknown" into a dedicated `missing_data_count` field rather than mixing it into the distribution.

### m4 — Training example provenance is top-level only
- **Rubric category:** Engineering Failure — "hidden assumptions"
- **Severity:** Minor
- **Location:** `src/pbdata/schemas/training_example.py`
- **Description:** Provenance exists only on `TrainingExampleRecord` (line 106), not on individual field sections (`StructureFields`, `ProteinFields`, etc.). Master spec section 6 implies per-field provenance. Current design makes it impossible to audit which source contributed a specific feature value.
- **Suggested fix:** Accept as-is for now (refactoring all 6 field sections is high-effort). Document the limitation. Consider adding field-level provenance in a future schema version.

### m5 — Hardcoded risk scoring weights with no documentation
- **Rubric category:** Engineering Failure — "hidden assumptions"
- **Severity:** Minor
- **Location:** `src/pbdata/risk/summary.py` lines 57-60
- **Description:** `binding_weight=0.7`, `pathway_overlap_weight=0.3` are hardcoded. `pathway_similarity` is set to `1.0` if a coverage file exists, `0.0` otherwise — a binary flag masquerading as a similarity score. No documentation explains the weight choices.
- **Suggested fix:** Add a comment documenting the weights are placeholders. Consider moving them to a config constant.

---

## Scientific/data-integrity concerns

1. **False confidence in prediction outputs.** Every prediction manifest reports a status containing "ready" while delivering zero predictions. A downstream automated system parsing status fields would classify these as usable. This violates the master spec's final principle.

2. **QA system cannot catch its own gaps.** The scenario runner's file-existence-only check means the QA layer will report green even when the prediction layer delivers nothing. This creates a false safety net.

3. **Pathway reasoning operates on incomplete data.** The risk scoring module uses a binary `pathway_similarity` (1.0 or 0.0) based on whether a coverage summary file exists, not on actual pathway overlap computation. The severity levels (low/medium/high) derived from this are not scientifically meaningful.

## Architecture concerns

1. **Spec layers 5-7 are scaffolds, not implementations.** The prediction, pathway reasoning, and risk scoring modules write manifest files but perform no computation. This is acceptable for a scaffold phase but should be explicitly marked as `status: "scaffold"` rather than `"ready"`.

2. **CLI commands directly instantiate source adapters.** The architect handoff (issue #4) correctly identified that `cli.py` contains ~220 lines of extraction logic that belongs in the pipeline layer. This was confirmed during review. The `extract` command directly calls `BindingDBAdapter()`, `ChEMBLAdapter()`, and `BioLiPAdapter()`.

3. **Graph layer omits pathway nodes by default.** The master spec requires proteins, ligands, and pathways as node types. Without `enable_external=True`, zero pathway nodes are emitted.

---

## Required fixes before merge

### Must fix (Critical — blocks merge)

1. **C1:** Validate SMILES input in `prediction/engine.py`. Reject invalid input with a clear error.
2. **C2:** Validate structure file existence in `prediction/engine.py`. Reject missing files with a clear error.
3. **C3:** Fix scenario runner to verify content, not just file existence. Flag null/empty prediction values.
4. **C4:** Add try/except error handling to all 4 prediction/risk/QA CLI commands. No raw tracebacks.

### Must fix (Major — blocks next QA cycle)

5. **M1:** Change prediction status strings from "ready" to "scaffold" or "no_predictions".
6. **M2:** Warn or error when pathway risk receives empty target list.
7. **M3:** Add provenance fields to `GraphNodeRecord` and `GraphEdgeRecord`.
8. **M4:** Wire pathway.py into graph output or document it as a planned stub.

## Suggested follow-up improvements

- Add `logger.warning()` calls in all `_read_csv()` / `_load_table_json()` silent-return paths (m2)
- Separate "unknown" from real categories in bias report (m3)
- Document hardcoded risk weights and binary pathway_similarity (m5)
- Consider field-level provenance for training examples in a future schema version (m4)
- Move CLI extraction helpers into `pipeline/enrichment.py` per architect handoff issue #4
