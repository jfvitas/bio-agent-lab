# AGENT_OUTPUT_REQUIREMENTS.md

## Purpose
This file defines the **required output artifact** for every agent run.  
All agents must produce one structured handoff file for every task so work is auditable, reviewable, and easy to pass between Claude and GPT/Codex.

If any instruction elsewhere conflicts with this file, this file controls **how the agent must package its output**.

---

## Global Rule

**Every agent run must end by writing exactly one primary output file** in:

```text
handoffs/
```

The file must be Markdown unless this file explicitly requires an additional machine-readable companion file.

The agent must not return only chat text or only code changes.  
It must always create the required handoff artifact.

---

## Required File Naming Convention

Use this format:

```text
handoffs/YYYY-MM-DD_<task_id>_<role>.md
```

Examples:

```text
handoffs/2026-03-09_bindingdb_merge_fix_architect.md
handoffs/2026-03-09_bindingdb_merge_fix_implementation.md
handoffs/2026-03-09_bindingdb_merge_fix_reviewer.md
handoffs/2026-03-09_ligand_offtarget_scenario_user_tester.md
```

### Rules
- `YYYY-MM-DD` = current date
- `task_id` = short snake_case identifier
- `role` must be one of:
  - `architect`
  - `implementation`
  - `reviewer`
  - `debug`
  - `user_tester`
  - `qa`
  - `performance`
- Use lowercase only
- Do not use spaces
- Do not overwrite an earlier file unless explicitly asked to update it

---

## Mandatory Output by Role

### 1. Architect role
The architect must create:

```text
handoffs/YYYY-MM-DD_<task_id>_architect.md
```

Required sections:

```markdown
# Architect Handoff

## Objective
## Why this change is needed
## Scope
### Files allowed to change
### Files that must not change
## Required changes
## Acceptance criteria
## Constraints
## Risks to avoid
## Tests to run
## Expected implementation output
```

Architect output must:
- define the exact scope
- define allowed and forbidden files
- define acceptance criteria
- not contain implementation code unless explicitly requested

---

### 2. Implementation role
The implementation agent must create:

```text
handoffs/YYYY-MM-DD_<task_id>_implementation.md
```

Required sections:

```markdown
# Implementation Report

## Objective implemented
## Files changed
## Summary of changes
## Unified diff
## Tests run
## Test results
## Known limitations
## Follow-up recommendations
```

Implementation output must:
- summarize exactly what changed
- include the unified diff or patch
- include test commands and results
- explicitly note anything left unresolved

The implementation agent may also create a machine-readable companion file:

```text
handoffs/YYYY-MM-DD_<task_id>_implementation.json
```

with:
- changed_files
- tests_run
- tests_passed
- warnings
- unresolved_items

---

### 3. Reviewer role
The reviewer must create:

```text
handoffs/YYYY-MM-DD_<task_id>_reviewer.md
```

Required sections:

```markdown
# Review Report

## Scope reviewed
## Pass / Fail decision
## Critical issues
## Major issues
## Minor issues
## Scientific/data-integrity concerns
## Architecture concerns
## Required fixes before merge
## Suggested follow-up improvements
```

Reviewer output must:
- clearly say pass or fail
- separate critical vs noncritical issues
- identify exact required fixes before merge

---

### 4. Debug role
The debug agent must create:

```text
handoffs/YYYY-MM-DD_<task_id>_debug.md
```

Required sections:

```markdown
# Debug Handoff

## Bug summary
## Observed behavior
## Expected behavior
## Likely root cause
## Evidence
## Files likely involved
## Minimal fix strategy
## Tests to verify the fix
## Risks of the fix
```

Debug output must:
- explain the bug clearly
- propose a minimal fix strategy
- not expand scope unnecessarily

---

### 5. User simulation tester role
The user simulation tester must create:

```text
handoffs/YYYY-MM-DD_<task_id>_user_tester.md
```

Required sections:

```markdown
# User Simulation Report

## Scenario ID
## User goal
## Inputs used
## Steps taken
## Observed outputs
## Expected outputs
## Undesirable states detected
## Severity assessment
## Pass / Fail decision
## Recommended fixes
```

User tester output must:
- behave like a real user
- focus on usability and workflow quality
- flag confusing or misleading behavior even if tests technically pass

---

### 6. QA role
The QA agent must create:

```text
handoffs/YYYY-MM-DD_<task_id>_qa.md
```

Required sections:

```markdown
# QA Report

## Scope tested
## Deterministic tests run
## Scenario tests run
## Stress tests run
## Results summary
## Failures
## Regression risks
## Merge recommendation
```

QA output must:
- include all tests run
- identify regressions
- recommend merge or block

---

### 7. Performance role
The performance agent must create:

```text
handoffs/YYYY-MM-DD_<task_id>_performance.md
```

Required sections:

```markdown
# Performance Report

## Scope analyzed
## Baseline behavior
## Bottlenecks identified
## Proposed optimizations
## Expected speed impact
## Correctness risks
## Validation plan
## Recommendation
```

Performance output must:
- never recommend removing validation or provenance tracking
- estimate impact and risks
- preserve scientific correctness

---

## Required Machine-Readable Scope Block

Every handoff file must begin with a YAML block like this:

```yaml
task_id: example_task
role: architect
date: 2026-03-09
allowed_files:
  - src/example.py
forbidden_files:
  - specs/canonical_schema.yaml
required_tests:
  - pytest tests/test_example.py -q
status: proposed
```

### Required YAML fields
- `task_id`
- `role`
- `date`
- `allowed_files`
- `forbidden_files`
- `required_tests`
- `status`

Allowed values for `status`:
- `proposed`
- `implemented`
- `reviewed`
- `blocked`
- `passed`
- `failed`

If a field does not apply, include it with an empty list.

---

## Output Quality Rules

All handoff files must be:
- concise
- specific
- scoped
- auditable
- actionable

They must not be:
- vague
- conversational
- long essays
- missing test instructions
- missing pass/fail decisions where applicable

---

## Merge-Gating Rule

No task is complete until the required handoff file exists.

Minimum required chain for a nontrivial code change:

1. architect handoff
2. implementation report
3. reviewer or QA report

For user-facing workflow changes, also require:
4. user simulation report

---

## Automatic Agent Instruction

All agents should be prompted with this rule:

> In addition to your normal response, create the required handoff file in `handoffs/` using the naming convention and section requirements in `AGENT_OUTPUT_REQUIREMENTS.md`. If you do not create the required handoff file, the task is incomplete.

---

## Example Minimal Architect Output

```markdown
---
task_id: bindingdb_merge_fix
role: architect
date: 2026-03-09
allowed_files:
  - src/pbdata/dataset/merge.py
  - tests/test_dataset/test_merge.py
forbidden_files:
  - specs/canonical_schema.yaml
  - tests/stress_test_panel_A.yaml
required_tests:
  - pytest tests/test_dataset/test_merge.py -q
status: proposed
---

# Architect Handoff

## Objective
Fix BindingDB assay merge logic so duplicate assay rows are normalized without dropping provenance.

## Why this change is needed
Current merge logic may collapse distinct assay sources into one row and lose source-level traceability.

## Scope
### Files allowed to change
- src/pbdata/dataset/merge.py
- tests/test_dataset/test_merge.py

### Files that must not change
- specs/canonical_schema.yaml
- tests/stress_test_panel_A.yaml

## Required changes
1. Preserve provenance for each merged assay record.
2. Prevent duplicate collapse when assay metadata differs.
3. Add regression tests.

## Acceptance criteria
- provenance retained for merged rows
- duplicate collapse only when records are truly identical
- test suite passes

## Constraints
- do not change schema
- do not refactor unrelated modules

## Risks to avoid
- breaking current CLI behavior
- dropping assay metadata

## Tests to run
- pytest tests/test_dataset/test_merge.py -q

## Expected implementation output
Implementation report with unified diff and test results.
```

---

## Final Rule

The system must prefer:
- explicit scope
- explicit artifacts
- explicit pass/fail outcomes

over:
- informal chat
- implied completion
- undocumented changes
