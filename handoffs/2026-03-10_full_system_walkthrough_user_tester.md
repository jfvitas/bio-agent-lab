---
task_id: full_system_walkthrough
role: user_tester
date: 2026-03-10
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

# User Simulation Report

## 1. User Goals Tested

Seven user personas were simulated:

| # | Persona | Goal | Scenario |
|---|---------|------|----------|
| 1 | Data scientist (new) | Discover available commands | Run `pbdata --help`, read command descriptions |
| 2 | Data scientist (dataset builder) | Build a protein binding dataset from scratch | ingest → extract → normalize → audit → report → build-splits |
| 3 | Medicinal chemist | Screen a candidate ligand for off-targets | predict-ligand-screening --smiles |
| 4 | Protein engineer | Predict binding partners for a structure | predict-peptide-binding --structure-file |
| 5 | Computational biologist | Assess pathway risk for targets | score-pathway-risk --targets |
| 6 | ML engineer | Build training data and train model | build-features → build-training-examples → train-baseline-model |
| 7 | Pipeline operator | Run full feature pipeline end-to-end | run-feature-pipeline, export-analysis-queue |

---

## 2. Steps Performed

### Scenario 1: Command discovery

| Step | Command | Result |
|------|---------|--------|
| 1a | `python -m pbdata` | **CRASH** — `No module named pbdata.__main__` |
| 1b | `pbdata --help` | **OK** — Lists 32 commands with descriptions |
| 1c | `pbdata ingest --help` | **OK** — Shows flags: --source, --dry-run, --yes, --criteria, --output |
| 1d | `pbdata predict-ligand-screening --help` | **OK** — Shows flags: --smiles, --sdf, --structure-file, --fasta |

### Scenario 2: Dataset building pipeline

| Step | Command | Result |
|------|---------|--------|
| 2a | `pbdata ingest --dry-run` | **OK** — "Found 39,240 RCSB entries matching criteria" |
| 2b | (ingest already done) | 39,248 raw JSON files exist in data/raw/rcsb/ |
| 2c | `pbdata extract` | (already done) — 2,500 entries extracted, 2,518 structures downloaded |
| 2d | `pbdata normalize` | **PARTIAL** — Creates processed JSON files but many are 0 bytes. Silent failure — no error or count summary at end |
| 2e | `pbdata audit` | **SLOW + NOISY** — Floods console with per-file "Failed to audit" warnings for empty processed files. No final summary shown before timeout |
| 2f | `pbdata report` | **SLOW + NOISY** — Same flood of "Skipping" warnings. No final summary before timeout |
| 2g | `pbdata build-splits --hash-only` | **OK** — Clear message: "No processed records found... Run 'normalize' first." |
| 2h | `pbdata build-features` | **OK** — Writes feature_records.json + feature_manifest.json. record_count: 0 (no assay data) |
| 2i | `pbdata build-training-examples` | **OK** — Writes training_examples.json. example_count: 0 |
| 2j | `pbdata build-custom-training-set` | **OK** — Writes CSVs and snapshot directory |
| 2k | `pbdata build-release --tag "test_v1"` | **OK** — Creates data/releases/test_v1/ with manifest |

### Scenario 3: Ligand off-target screening

| Step | Command | Result |
|------|---------|--------|
| 3a | `pbdata predict-ligand-screening --smiles "CC(=O)Oc1ccccc1C(=O)O"` | **CRASH** — Raw traceback: `ModuleNotFoundError: No module named 'torch'` |
| 3b | `pbdata predict-ligand-screening --smiles "NOT_A_SMILES"` | **CRASH** — Same torch import error; never reaches SMILES validation |
| 3c | `pbdata predict-ligand-screening` (no args) | **CRASH** — Same torch import error; never reaches missing-arg check |

### Scenario 4: Peptide binding prediction

| Step | Command | Result |
|------|---------|--------|
| 4a | `pbdata predict-peptide-binding --structure-file "data/structures/rcsb/1ATP.cif"` | **CRASH** — Same torch import traceback |
| 4b | `pbdata predict-peptide-binding --structure-file "does_not_exist.cif"` | **CRASH** — Same; never reaches file validation |

### Scenario 5: Pathway risk scoring

| Step | Command | Result |
|------|---------|--------|
| 5a | `pbdata score-pathway-risk --targets "P00533,P04637"` | **OK** — Writes pathway_risk_summary.json. Status: `dataset_context_summary_not_model_score` |
| 5b | `pbdata score-pathway-risk` (no targets) | **OK** — Clean error: "Error: --targets is required and must contain at least one UniProt ID." Exit code 1 |
| 5c | Inspect output JSON | Risk score: 0.3, severity: "low", all real predictions null. Notes explain it's a placeholder. `risk_score_is_placeholder: true` |

### Scenario 6: ML training pipeline

| Step | Command | Result |
|------|---------|--------|
| 6a | `pbdata train-baseline-model` | **OK** — Writes model JSON. Status: `no_training_exemplars_available` |
| 6b | `pbdata evaluate-baseline-model` | **OK** — Writes evaluation JSON. Status: `no_training_exemplars_available` |
| 6c | `pbdata build-microstates` | **OK** — Writes microstate_records.json + manifest |
| 6d | `pbdata build-physics-features` | **OK** — Writes physics_feature_records.json + manifest |

### Scenario 7: Feature pipeline operation

| Step | Command | Result |
|------|---------|--------|
| 7a | `pbdata run-feature-pipeline` | **PARTIAL** — Runs 7 stages. 3 passed, 3 failed, 1 passed. Summary written to artifacts/ |
| 7b | Inspect input manifest | **OK** — Records schema_version, pipeline_version, row counts, gpu_unavailable=true |
| 7c | Inspect summary report | **CONFUSING** — Shows "0/0 succeeded" for every stage including "passed" ones |
| 7d | `pbdata export-analysis-queue` | **CRASH** — Raw traceback: `ImportError: ... pyarrow ... fastparquet` |
| 7e | `pbdata build-graph` | **CRASH** — Same torch import traceback |
| 7f | `pbdata build-structural-graphs` | **CRASH** — Same torch import traceback |
| 7g | `pbdata engineer-dataset` | **CRASH** — Same torch import traceback |

### Additional testing

| Step | Command | Result |
|------|---------|--------|
| 8a | `pbdata report-bias` | **OK** — Writes bias_report.json |
| 8b | `pbdata run-scenario-tests` | **OK** — Writes scenario_test_report.json. Correctly identifies missing outputs |
| 8c | `pbdata build-conformational-states` | **OK** — Writes 4,900 conformational state records |
| 8d | `pbdata setup-workspace` | **OK** — Creates metadata/workflow_manifest.json |
| 8e | `pbdata harvest-metadata` | **OK** — Creates metadata/protein_metadata.csv |
| 8f | `pbdata ingest-physics-results` (no batch-id) | **OK** — Clean error: "Missing option '--batch-id'" |
| 8g | `pbdata build-release --release-tag test` | **CONFUSING** — "No such option: --release-tag". Correct flag is --tag, but README says --release-tag |

---

## 3. Observed Outputs

### Outputs that were correct and useful

| Output | Content | Assessment |
|--------|---------|------------|
| `data/risk/pathway_risk_summary.json` | Honest placeholder scoring with `risk_score_is_placeholder: true` | Good — clearly labeled as non-production |
| `data/qa/scenario_test_report.json` | Lists missing expected outputs with severity | Good — actionable self-test |
| `data/conformations/conformation_state_manifest.json` | Record count: 4,900, status properly labeled | Good |
| `data/features/microstates/microstate_manifest.json` | Stage state tracking works | Good |
| `data/models/ligand_memory_model.json` | status: `no_training_exemplars_available`, 20+ feature keys listed | Good — informative even with no data |
| `artifacts/manifests/*_input_manifest.json` | Full version matrix, row counts by table, gpu_unavailable flag | Good — spec-compliant manifest |
| `data/releases/test_v1/` | Release snapshot with manifest, coverage, model-ready pairs | Good |

### Outputs that were confusing or misleading

| Output | Problem |
|--------|---------|
| `artifacts/reports/*_summary.md` | Shows "0/0 succeeded" for stages marked "passed" — misleading. A "passed" stage with 0 records processed looks like a silent skip, not a success |
| `data/features/feature_manifest.json` | record_count: 0. Notes say "no_assay_entries" but don't explain that features require assay data. User expected features from structures alone |
| `data/training_examples/training_manifest.json` | example_count: 0, sources_used: []. No guidance on what needs to happen to get >0 examples |

---

## 4. Expected Outputs

| Scenario | Expected | Actual |
|----------|----------|--------|
| Ligand screening (aspirin SMILES) | Prediction manifest or clear "no model trained" message | Raw Python traceback from missing `torch` |
| Peptide binding (1ATP.cif) | Prediction manifest or clear scaffold message | Raw Python traceback |
| Graph building | Graph nodes/edges or "requires extracted data" message | Raw Python traceback |
| Audit of 39k records | Summary: "Audited X records, Y passed, Z failed" | Thousands of warning lines, no summary |
| Normalize | Summary: "Normalized X of Y records, Z failed" | Silent completion with many 0-byte output files |
| Feature pipeline summary | Per-stage record counts matching manifests | "0/0 succeeded" for every stage |
| `build-release --release-tag` | Works | "No such option" (flag is --tag) |
| `python -m pbdata` | CLI help or module execution | "No module named pbdata.__main__" |

---

## 5. Confusing Behaviors

### CB1 — 8 commands crash with same torch import error (Critical)
**Commands affected:** `predict-ligand-screening`, `predict-peptide-binding`, `build-graph`, `build-structural-graphs`, `engineer-dataset`, and any command importing from `pbdata.graph` or `pbdata.prediction.engine`.

**Root cause:** `graph/structural_graphs.py` line 22 has `import torch` at module level. `graph/__init__.py` line 9 eagerly imports `structural_graphs`. Any module that imports from `pbdata.graph` (including `prediction/engine.py`) triggers the cascade.

**User impact:** A standard install without torch (which is a 2GB+ optional dependency) cannot use prediction, graph building, or dataset engineering commands. No friendly error message — just a raw Python traceback with internal file paths.

### CB2 — Normalize writes 0-byte files silently (High)
When normalization fails for a record (e.g., no canonical mapping available), it creates an empty file at `data/processed/rcsb/<pdb_id>.json`. No warning is logged, no count is given. Downstream commands (audit, report, build-splits) encounter these empty files and emit individual per-file warnings, flooding the console.

### CB3 — Audit/report flood console with thousands of warnings (High)
With 39,248 files and many being empty, the audit and report commands produce thousands of "Failed to audit" / "Skipping" lines. No progress bar, no final summary, and the useful information (how many succeeded) is buried.

### CB4 — Feature pipeline reports "0/0 succeeded" for passed stages (Medium)
The summary report at `artifacts/reports/*_summary.md` shows "0/0 succeeded" for stages marked as "passed". This is mathematically correct (0 records attempted, 0 succeeded, which is technically a pass) but confusing. A user reads "passed (0/0)" and doesn't know if it ran or was skipped.

### CB5 — `python -m pbdata` doesn't work (Low)
There is no `__main__.py` in the `pbdata` package. Users must use the `pbdata` entry point or `python -m pbdata.cli`. This is a minor discoverability issue.

### CB6 — README documents `--release-tag` but flag is `--tag` (Low)
The README Pipeline Options table says "Release tag" but the actual CLI flag is `--tag`. A user copying from the README gets "No such option."

---

## 6. Missing Functionality

### MF1 — No `--verbose` / `--quiet` flag for any command
All commands emit the same level of output regardless of user preference. The audit command's thousands of warnings cannot be suppressed.

### MF2 — No progress indicators for long-running commands
`audit`, `report`, `normalize`, and `extract` process tens of thousands of files with no progress bar or percentage. Users cannot tell if the command is working or stuck.

### MF3 — No `--output-format` option for any reporting command
All reports are JSON-only. A user who wants CSV or markdown output must post-process.

### MF4 — No way to validate installation
There is no `pbdata check` or `pbdata doctor` command that verifies: required dependencies present, optional dependencies available, config files valid, data directories exist.

### MF5 — No `pbdata status` command
No single command shows: raw record count, extracted count, processed count, structure count, split status, model status, last pipeline run. Users must inspect individual directories.

### MF6 — Cannot run prediction commands without torch installed
The entire prediction/graph subsystem is inaccessible without torch (2GB+ dependency). These commands should degrade gracefully with a one-line message.

---

## 7. Undesirable States

### US1 — Silent data loss in normalize (Critical)
**Severity:** Critical
**Category:** Silent failure (forbidden by undesirable_state_rubric.md)
**Location:** `normalize` command → `data/processed/rcsb/`
**Description:** Normalization creates 0-byte files for records that fail to normalize. No error count, no summary, no list of failed records. A user running `normalize` → `audit` gets thousands of "Failed to audit" warnings with no understanding of what went wrong.
**Impact:** User cannot determine which records successfully normalized without checking file sizes manually.

### US2 — 8 CLI commands produce raw Python tracebacks (Critical)
**Severity:** Critical
**Category:** UX failure — "confusing CLI outputs", "unclear error messages"
**Location:** All commands that import from `pbdata.graph` or `pbdata.prediction.engine`
**Description:** `ModuleNotFoundError: No module named 'torch'` with full internal traceback including line numbers and file paths. Affected commands: predict-ligand-screening, predict-peptide-binding, build-graph, build-structural-graphs, engineer-dataset, export-analysis-queue (pyarrow variant), and any command touching these imports.
**Impact:** A medicinal chemist, protein engineer, or ML researcher sees cryptic Python internals instead of "torch is required for this command. Install with: pip install torch".

### US3 — export-analysis-queue crashes with pyarrow error (High)
**Severity:** High
**Category:** Functional failure — required pipeline step inaccessible
**Location:** `export-analysis-queue` → `_write_df()` → `pd.DataFrame.to_parquet()`
**Description:** Parquet writing requires pyarrow or fastparquet, neither of which is installed. Raw ImportError traceback.
**Impact:** The physics subsystem's analysis queue export is completely blocked, preventing the offline ORCA/APBS/OpenMM workflow.

### US4 — Audit console flood with no summary (High)
**Severity:** High
**Category:** UX failure — "confusing CLI outputs"
**Location:** `audit` command output
**Description:** Thousands of per-file "Failed to audit" warnings scroll past with no aggregation. The useful information (total audited, total failed, quality score distribution) never appears.
**Impact:** User loses trust in the pipeline and cannot assess dataset health.

### US5 — Feature pipeline "0/0 succeeded" is misleading (Medium)
**Severity:** Medium
**Category:** UX failure — "misleading certainty"
**Location:** `artifacts/reports/*_summary.md`
**Description:** Stages that "pass" with zero records show "passed (0/0 succeeded)". This looks like nothing ran, not that the stage correctly handled an empty input.
**Impact:** Pipeline operator cannot distinguish "ran successfully on nothing" from "skipped" from "failed silently".

### US6 — No training data → model says "no_training_exemplars_available" without guidance (Low)
**Severity:** Low
**Category:** UX failure — missing guidance
**Location:** `train-baseline-model` output
**Description:** Status says `no_training_exemplars_available` but doesn't explain what prerequisites are missing (assay data, normalized records, features).

---

## 8. User Experience Assessment

### What works well

1. **Command discovery is good.** `pbdata --help` lists 32 commands with concise descriptions. Per-command `--help` shows all flags with types and defaults.

2. **Dataset building pipeline (when data exists) works.** Extract → build-features → build-training-examples → build-custom-training-set → build-release is a smooth sequence with clear output paths.

3. **Error handling on some commands is excellent.** `score-pathway-risk` with no targets gives a clean error. `ingest-physics-results` without `--batch-id` gives a clean error. `build-splits --hash-only` without processed records gives a helpful "Run 'normalize' first" message.

4. **Output manifests are informative.** Status fields like `risk_score_is_placeholder: true` and `no_training_exemplars_available` are honest and machine-parseable.

5. **Feature pipeline artifacts directory is well-organized.** `artifacts/manifests/` with run-ID-based naming is discoverable and reproducible.

### What needs improvement

1. **Dependency failures are catastrophic.** 8 commands crash with raw tracebacks because of a missing optional dependency. This is the single biggest usability issue.

2. **No progress feedback.** Long operations (audit, normalize, extract) give no indication of progress. Users wait minutes with no output.

3. **Silent failures in normalize.** 0-byte files are created without logging, creating downstream confusion.

4. **No pipeline summary command.** Users must manually inspect multiple directories to understand system state.

### Overall usability score: **C+**

The core data pipeline works, but the prediction/graph subsystem is completely inaccessible without torch. Silent failures in normalize and console flooding in audit make the inner loop painful. The honest manifest content and clean error handling in newer commands suggest the codebase is improving, but the older commands need the same treatment.

---

## 9. Recommended Improvements

### Must fix (Critical — blocks basic usability)

1. **Guard torch import in `graph/structural_graphs.py` and `graph/__init__.py`.** Make the import lazy or catch `ModuleNotFoundError` and provide a one-line message: `"torch is required for this command. Install with: pip install torch"`. This unblocks 8 commands.

2. **Guard pyarrow import in `pipeline/feature_execution.py._write_df()`.** Fall back to JSON or CSV if pyarrow is unavailable, or emit a clean error.

3. **Fix normalize to log failures and emit a summary.** At minimum: `"Normalized X of Y records (Z failed). Failed records: data/processed/rcsb/<failed>.json"`. Do not create 0-byte files — either skip the file or write a JSON error record.

### Should fix (High — significantly improves experience)

4. **Add a final summary line to audit and report commands.** Example: `"Audited 39,248 records: 36,750 passed, 2,498 failed. Mean quality score: 0.82."`

5. **Suppress per-file warnings by default; show with `--verbose`.** Add `--verbose / --quiet` flags to all commands that process many files.

6. **Add `pbdata status` command.** Show: raw records, extracted entries, processed records, structures, splits, model status, last pipeline run. One command for system health.

7. **Fix feature pipeline summary to distinguish "ran on 0 records" from "failed".** Show "passed (0 records, input was empty)" vs "failed (error: ...)".

### Nice to fix (Medium — polish)

8. **Add `__main__.py`** so `python -m pbdata` works.

9. **Fix README `--release-tag` → `--tag`** documentation mismatch.

10. **Add `pbdata doctor` command** that checks: Python version, required deps, optional deps (torch, pyarrow, rdkit), config validity, data directory structure.

11. **Add progress bars** (tqdm or simple counter) for commands processing >100 files.
