---
task_id: scenario_test_execution
role: user_tester
date: 2026-03-09
allowed_files: []
forbidden_files: []
required_tests:
  - .venv/Scripts/python.exe -m pytest tests/ -q
status: failed
---

# User Simulation Report

## Scenario ID

Two scenarios executed from `specs/bio_agent_full_instruction_pack/qa/scenario_test_templates.yaml`:

1. `ligand_offtarget_basic` (medicinal chemist)
2. `peptide_binding_prediction` (protein engineer)

Additional ad-hoc edge-case testing performed on all prediction/risk CLI commands.

## User goal

1. **ligand_offtarget_basic** — A medicinal chemist wants to identify off-target binding risks for a candidate ligand (aspirin, SMILES: `CC(=O)Oc1ccccc1C(=O)O`).
2. **peptide_binding_prediction** — A protein engineer wants to predict binding partners for a peptide structure (1ATP.cif kinase complex).

## Inputs used

| Scenario | Input | Value |
|----------|-------|-------|
| ligand_offtarget_basic | `--smiles` | `CC(=O)Oc1ccccc1C(=O)O` (aspirin) |
| ligand_offtarget_basic | (no args) | (edge case: no input provided) |
| ligand_offtarget_basic | `--smiles` | `NOT_A_SMILES` (edge case: invalid input) |
| peptide_binding_prediction | `--structure-file` | `data/structures/rcsb/1ATP.cif` |
| peptide_binding_prediction | `--structure-file` | `does_not_exist.cif` (edge case: missing file) |
| pathway risk | `--targets` | `P00533,P04637` (EGFR and p53) |
| pathway risk | (no args) | (edge case: no targets) |

## Steps taken

### Scenario 1: ligand_offtarget_basic

1. Ran `pbdata predict-ligand-screening --help` — confirmed command exists with options: `--smiles`, `--sdf`, `--structure-file`, `--fasta`
2. Ran `pbdata predict-ligand-screening --smiles "CC(=O)Oc1ccccc1C(=O)O"` — command completed, wrote manifest JSON
3. Inspected output manifest at `data/prediction/ligand_screening/prediction_manifest.json`
4. Ran `pbdata score-pathway-risk --targets "P00533,P04637"` — completed, wrote risk summary JSON
5. Inspected output at `data/risk/pathway_risk_summary.json`
6. Ran `pbdata predict-ligand-screening` with no arguments — **crashed with raw traceback**
7. Ran `pbdata predict-ligand-screening --smiles "NOT_A_SMILES"` — **accepted silently, no validation**

### Scenario 2: peptide_binding_prediction

1. Ran `pbdata predict-peptide-binding --structure-file "data/structures/rcsb/1ATP.cif"` — completed, wrote manifest JSON
2. Inspected output at `data/prediction/peptide_binding/prediction_manifest.json`
3. Ran `pbdata predict-peptide-binding --structure-file "does_not_exist.cif"` — **accepted silently, wrote manifest referencing nonexistent file**

### Additional testing

4. Ran `pbdata run-scenario-tests` — completed, wrote scenario report JSON
5. Inspected `data/qa/scenario_test_report.json` — claims `missing_expected_outputs: []` for both scenarios
6. Ran `pbdata report-bias` — completed, wrote bias report JSON
7. Ran `pbdata build-conformational-states` — completed, wrote 4900 conformational states
8. Ran `pbdata build-splits --hash-only` — correctly told user to run `normalize` first

## Observed outputs

### ligand_offtarget_basic — manifest content

```json
{
  "status": "workflow_ready_no_trained_model",
  "normalized_input_type": "SMILES",
  "candidate_target_count": 0,
  "candidate_targets_preview": [],
  "predicted_kd": null,
  "predicted_delta_g": null,
  "binding_probability": null,
  "confidence_score": null
}
```

### peptide_binding_prediction — manifest content

```json
{
  "status": "workflow_ready_no_trained_model",
  "predicted_targets": [],
  "binding_probability": null,
  "interface_summary": {
    "feature_context_available": true,
    "graph_context_available": true
  }
}
```

### pathway risk — summary content

```json
{
  "status": "dataset_context_summary_not_model_score",
  "matching_pair_count": 0,
  "pathway_activation_probability": null,
  "pathway_conflict_score": null,
  "severity_level": null
}
```

## Expected outputs

### Per scenario template

| Scenario | Expected Output | Present in Manifest? | Contains Real Value? |
|----------|----------------|---------------------|---------------------|
| ligand_offtarget_basic | ranked_target_list | No (field is `candidate_targets_preview: []`) | No — empty list |
| ligand_offtarget_basic | predicted_affinity | Yes (`predicted_kd`, `predicted_delta_g`) | No — both null |
| ligand_offtarget_basic | confidence_score | Yes | No — null |
| ligand_offtarget_basic | pathway_risk_summary | Separate command output | All scores null |
| peptide_binding_prediction | predicted_targets | Yes | No — empty list |
| peptide_binding_prediction | binding_probability | Yes | No — null |
| peptide_binding_prediction | interface_summary | Yes | Partial — only availability flags, no actual interface data |

## Undesirable states detected

### PROBLEM 1 — Silent acceptance of invalid SMILES input
- **Severity:** Critical (per rubric: "misleading certainty when evidence weak")
- **Location:** `src/pbdata/prediction/engine.py` → `_detect_input_type()`
- **Description:** `NOT_A_SMILES` is accepted as valid SMILES with no validation. The manifest reports `status: "workflow_ready_no_trained_model"` and `normalized_input_type: "SMILES"`. A medicinal chemist would assume their input was validated. No warning, no error, no flag.
- **Forbidden behavior triggered:** `silent_failure` — bad input produces a success-looking manifest

### PROBLEM 2 — Silent acceptance of nonexistent structure file
- **Severity:** Critical (per rubric: "required fields missing", "silent_failure")
- **Location:** `src/pbdata/prediction/engine.py` → `_detect_input_type()`
- **Description:** `does_not_exist.cif` is accepted without checking if the file exists. The manifest stores the path as-is. A protein engineer would trust the output and only discover the problem downstream.
- **Forbidden behavior triggered:** `silent_failure`

### PROBLEM 3 — Raw Python traceback on missing input
- **Severity:** High (per rubric: "confusing CLI outputs", "unclear error messages")
- **Location:** `src/pbdata/cli.py:988` → `predict_ligand_screening_cmd()`
- **Description:** Running `predict-ligand-screening` with no arguments produces a full `ValueError` traceback instead of a user-friendly error message. A medicinal chemist would see `C:\Users\jfvit\Documents\bio-agent-lab\src\pbdata\prediction\engine.py:37 in _detect_input_type` and have no idea what went wrong.
- **Suggested fix:** Catch `ValueError` in the CLI command and print a clean error message via `typer.echo()` + `raise typer.Exit(code=1)`

### PROBLEM 4 — All prediction outputs are null (every field)
- **Severity:** High (per rubric: "empty_output_without_warning")
- **Location:** `src/pbdata/prediction/engine.py`
- **Description:** Both scenarios produce manifests where every prediction field is `null` and every list is empty. The `notes` field explains why, but the `status` field says `workflow_ready_no_trained_model` which implies readiness. A user seeing "ready" expects partial results. The status should be `no_predictions_available` or similar.
- **Forbidden behavior triggered:** `missing_confidence_scores` (all null), `empty_output_without_warning` (empty lists with "ready" status)

### PROBLEM 5 — Scenario test self-assessment is misleading
- **Severity:** Medium (per rubric: "misleading certainty when evidence weak")
- **Location:** `data/qa/scenario_test_report.json`
- **Description:** The `run-scenario-tests` command reports `missing_expected_outputs: []` for both scenarios, implying all expected outputs are present. But it only checks that field *names* exist in the manifest vocabulary — not that they contain actual values. `predicted_affinity` is listed as "available" but the actual value is `null`. This is a false pass.

### PROBLEM 6 — Pathway risk accepts empty target list without warning
- **Severity:** Medium (per rubric: "pathway reasoning based on incomplete data")
- **Location:** `src/pbdata/cli.py` → `score_pathway_risk_cmd()`
- **Description:** Running `score-pathway-risk` with no `--targets` flag produces a report with `targets_requested: []`. No warning that the input was empty. A user might not realize they forgot the required argument.

### PROBLEM 7 — No ranked target list field in ligand screening output
- **Severity:** Low (field naming mismatch)
- **Location:** `src/pbdata/prediction/engine.py`
- **Description:** The scenario expects `ranked_target_list` but the manifest uses `candidate_targets_preview`. The scenario self-test maps between them internally, but a user reading the manifest would not find the expected field name.

## Severity assessment

| # | Problem | Severity | Rubric Category |
|---|---------|----------|----------------|
| 1 | Invalid SMILES accepted silently | Critical | Scientific Failure |
| 2 | Nonexistent file accepted silently | Critical | Functional Failure |
| 3 | Raw traceback on missing input | High | UX Failure |
| 4 | All predictions null with "ready" status | High | UX Failure |
| 5 | Scenario self-test false pass | Medium | Scientific Failure |
| 6 | Empty targets accepted without warning | Medium | UX Failure |
| 7 | Field name mismatch vs scenario template | Low | UX Failure |

## Pass / Fail decision

**FAIL**

Both scenarios fail against the rubric. The forbidden behaviors `silent_failure` and `missing_confidence_scores` are triggered in scenario 1. The forbidden behavior `missing_interface_data` is triggered in scenario 2 (interface_summary contains only boolean flags, no actual residue or interaction data).

## Recommended fixes

### Must fix before merge (Critical + High)

1. **Validate SMILES input** — Add basic SMILES validation (at minimum: contains only valid SMILES characters, has balanced parentheses/brackets). If invalid, raise a clear error before writing any manifest. No RDKit required for basic checks.

2. **Validate structure file existence** — Check `Path(structure_file).exists()` before accepting it. If missing, raise `typer.BadParameter("Structure file not found: ...")`.

3. **Catch ValueError in CLI commands** — Wrap prediction command bodies in try/except for `ValueError` and emit a clean `typer.echo("Error: ...")` + `raise typer.Exit(code=1)`.

4. **Change status field from `workflow_ready_no_trained_model` to `scaffold_only_no_predictions`** — "Ready" implies usable output. The current output is a placeholder manifest with no predictions. The status should reflect that honestly.

### Should fix (Medium)

5. **Fix scenario self-test to check for non-null values** — `missing_expected_outputs` should flag fields that are present but null/empty, not just missing keys.

6. **Require `--targets` for pathway risk** — Either make it a required option or print a warning when the target list is empty.

### Nice to fix (Low)

7. **Align field names with scenario template** — Rename `candidate_targets_preview` to `ranked_target_list` or document the mapping.
