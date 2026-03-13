# CODEX_RUNBOOK.md

## Files to add to the repo root

Place these files in the repository root before launching Codex:
- `MASTER_PROMPT.txt`
- `AGENTS.md`

Optional:
- a `docs/` folder for architecture notes and implementation summaries
- a `scratch/` or `notes/` folder if the agent needs a place for working plans

## Recommended launch flow

From the repository root:

```bash
codex exec --ask-for-approval never "$(cat MASTER_PROMPT.txt)"
```

If your shell has trouble with large inline content, use your terminal paste workflow instead.

## Recommended autonomous workflow

For a large repository like this, two passes are often more stable than one giant pass.

### Pass 1
Focus on:
- repo inspection
- architecture mapping
- source-ingestion framework
- identity resolution
- dataset-builder scaffolding
- validation and docs

### Pass 2
Focus on:
- GUI refinement
- wiring new modules into the application
- tests and smoke tests
- final cleanup
- implementation summary

## What Codex should do first

The prompt instructs Codex to create a short implementation plan before major changes.
That plan should include:
- current repo state
- proposed phases
- assumptions
- risks
- immediate next steps

This reduces drift and makes the run easier to review.

## What to watch for during autonomous runs

Review the repo periodically for:
- dependency drift
- large destructive refactors
- broken imports
- fake scaffolding that is not actually wired in
- source adapters that look complete but are only placeholders
- GUI changes that break usability or layout

## Preferred design outcomes

The platform should evolve toward:
- broad multi-source biological integration
- explicit provenance
- canonical cross-database identity mapping
- leakage-resistant data splits
- GUI-driven workflows
- future scaling to heavier compute environments

## Suggested review checklist after a run

1. Does the app still launch?
2. Does the GUI still work?
3. Is the root-path configuration coherent?
4. Are source adapters actually wired into the platform?
5. Is there a real dataset builder, not just a stub?
6. Are split strategies present beyond random splits?
7. Is there documentation of what changed?
8. Are new tests or smoke tests included?
9. Are blocked items clearly documented?

## Optional follow-up prompt

After the first run, you can do a follow-up Codex pass with a focused prompt like:

```text
Review the repository changes you previously made.
Now focus only on:
1. tightening GUI usability
2. improving validation and smoke tests
3. documenting exact setup and run steps
4. eliminating dead code and placeholder modules
5. ensuring new ingestion and dataset-building paths are truly wired into the application
```
