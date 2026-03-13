# Implementation Plan

Updated: 2026-03-12 (autonomous architecture + implementation pass)

## Current Session State

- The repository is already a non-trivial, working protein-data platform rather than a stub project.
- The current worktree is dirty. There are existing in-progress changes across GUI, config, storage, training, reporting, and tests, so this pass must be additive and careful rather than destructive.
- The platform already supports:
  - configurable storage root / workspace layout
  - Tkinter GUI + Typer CLI
  - multi-source extraction and enrichment across RCSB, ChEMBL, BindingDB, PDBbind, BioLiP, and SKEMPI
  - graph / feature / training-example / split / release workflows
  - a broad automated test suite
- The highest-value remaining gaps for this pass are now in:
  - making split strategy outputs more explicit and auditable
  - broadening source-architecture visibility toward the larger intended multi-source platform
  - tightening GUI workflow guidance around operational state and leakage-aware dataset generation

## Current Repo State

- The repo is already a functional protein-data workbench with a Tkinter GUI, Typer CLI, multi-stage extraction pipeline, graph/features/training-example generation, split building, release exports, and a large passing test suite.
- Core RCSB ingestion and extraction are real. BindingDB, ChEMBL, PDBbind, BioLiP, and SKEMPI integrations exist, but the source-adapter architecture is still uneven and not yet expressed as a single scalable registry/capability layer.
- The workspace/reporting surface is much stronger than before: status, demo readiness, training quality, model comparison, source activity, prediction status, and pathway-risk context are all available.
- The project still has important maturity gaps:
  - source-adapter architecture is spread across modules rather than centered in one registry
  - canonical identity resolution is present in pieces, but not exposed as a first-class crosswalk/reporting subsystem
  - GUI workflow guidance is better, but source capability and dataset hygiene guidance can be clearer
  - training-quality reporting was recently improved, but still needs benchmark honesty around dataset size/diversity

## Proposed Phases

### Phase 1: Architecture Consolidation

- Add a source registry/capability layer that describes each source consistently:
  - source class/family
  - ingest mode
  - live/local requirements
  - update behavior
  - canonical identifiers
  - dataset relevance
- Keep changes additive and route existing logic through compatibility helpers rather than rewriting working source modules.

### Phase 2: Canonical Identity / Crosswalk Layer

- Add a first-class identity-crosswalk builder from extracted tables and graph artifacts.
- Export canonical protein/ligand/pair mappings with explicit ambiguity handling and provenance-oriented notes.
- Surface the crosswalk in reports so users can inspect what was linked and what remains unresolved.

### Phase 3: GUI Workflow and Source Visibility

- Improve GUI explanation of source capabilities, requirements, and likely downstream impact.
- Add clearer workflow guidance around identity/crosswalk and dataset hygiene artifacts.
- Preserve current GUI structure, but make the platform feel more intentional and guided.

### Phase 4: Validation and Operational Polish

- Add targeted tests for new architecture seams.
- Update documentation so the repo explains the new registry/crosswalk workflow and what remains future work.
- Re-run the full suite and smoke-check the new CLI/GUI wiring.

### Phase 5: Split Diagnostics And Leakage Transparency

- Add first-class split-diagnostic artifacts that make the chosen split strategy inspectable instead of leaving it implicit in `metadata.json`.
- Report split composition and overlap across:
  - receptor identity / family proxies
  - ligand scaffold proxies
  - mutation grouping
  - source grouping
  - representation balance
- Surface dominant-group / overlap risk in a form the GUI and release-facing reports can consume.

### Phase 6: Broader Source-Surface Scaffolding

- Extend the source registry so it can describe both currently implemented sources and near-term planned source families without pretending they are already ingested.
- Make the capability layer clearer about:
  - implemented vs planned
  - live API vs local dataset vs bulk download
  - dataset role in the platform
  - what remains blocked by licensing, external APIs, or future adapter work

### Phase 7: GUI And Reporting Integration

- Expose the new split diagnostics and broader source capability surface in the GUI overview and root-level reporting.
- Keep the current UI structure, but make the dataset-builder and operations surfaces more explicit about:
  - what is running
  - what split policy is active
  - where leakage or dominance risks remain

## Assumptions

- Existing CLI and GUI behavior should remain stable unless there is a clear correctness or usability gain.
- Biological identity resolution should remain conservative:
  - do not collapse ambiguous protein mappings silently
  - prefer explicit unresolved/partial states over guessed harmonization
- This implementation pass should improve architecture and platform usefulness without pretending that all external-source breadth is fully complete today.
- Existing user changes already present in the worktree are intentional and must not be reverted as part of this pass.

## Risks

- Over-refactoring ingestion could create churn in a repo that already has a lot of working logic.
- Identity resolution can easily overclaim biological certainty if the crosswalk rules are too aggressive.
- GUI expansion can make the interface noisier if new panels are not tightly scoped.
- Split diagnostics can become misleading if they imply biologically perfect family/scaffold annotations when the current platform still uses conservative proxies in several places.
- Broadening the source registry can create false completeness if planned sources are not explicitly marked as planned/non-ingestable.

## Immediate Next Steps

1. Add split-diagnostic export helpers and wire them into `build-splits`.
2. Surface split diagnostics in the GUI overview and root-level reports.
3. Expand the source registry to cover a broader multi-source roadmap with explicit implementation state.
4. Update docs / summary files to reflect the new dataset-governance and source-capability surfaces.
5. Re-run targeted tests first, then the full suite if the incremental pass stays green.

## Progress In This Pass

- Completed:
  - added split diagnostics export for pair-aware, grouped, and temporal split modes
  - surfaced split diagnostics in the GUI overview
  - threaded split diagnostics into release artifacts / release-readiness warnings
  - expanded the source registry with explicit implemented vs planned source families
  - implemented metadata-oriented source adapters for UniProt, AlphaFold DB, and Reactome
  - wired those adapters into `harvest-metadata` as opt-in enrichment paths
  - exposed metadata harvest enrichment controls in the GUI pipeline options
  - implemented PDBe/SIFTS-backed metadata harvest adapters for InterPro, Pfam, CATH, and SCOP
  - wired those annotation adapters into `harvest-metadata` through CLI and GUI controls
  - threaded metadata-derived family / pathway / fold keys into custom training-set selection
  - tightened release-readiness warnings around metadata-group overlap and missing family annotations
  - added shared JSON file-health scanning plus `pbdata clean` for corrupt/empty cache detection and repair
  - exposed processed-data integrity counts in `pbdata status`
  - replaced per-file corrupt-record warning floods with summary counts in normalize / audit / report / hash split flows
  - added feature-pipeline stage discoverability via `--list-stages` and richer invalid-stage errors
  - added sequence-only peptide prediction fallback for `predict-peptide-binding --fasta`
  - added ligand-screening binder-call labels for easier interpretation of weak predictions
  - updated root implementation summary
  - passed targeted validation for touched areas
- Validation note:
  - targeted tests passed
  - user-tester handoff regression passed (`42 passed`)
  - broader touched-area regression passed (`134 passed`)
  - `python -m compileall src` passed
  - a fresh full-suite pass is still pending in a longer-running validation environment

## Proposed Next Phase

### Phase 10: Deeper Annotation And Release Governance

- Add the next biological annotation depth behind the new domain/fold adapter seams:
  - richer InterPro / Pfam family semantics
  - stronger CATH / SCOP hierarchy normalization
- Push metadata-aware grouping further into:
  - release gating thresholds
  - training-set selection presets
  - GUI operator guidance
- Re-run the full repository suite serially with a larger timeout budget and record the wall-clock result.

## Current Release-Blocker Analysis

- The repo codebase is stable and the full suite is currently passing, but the live workspace is not yet release-ready.
- The current release blockers are specific:
  - `no_model_ready_pairs`
  - `no_split_metadata`
- The live workspace is not missing extracted assay files. It currently has assay artifacts for the full extracted entry set, but the release surface is thin because:
  - current training/release artifacts are stale relative to one another
  - every current canonical pair is excluded by the same coarse policy
- The exclusion pattern is currently over-broad for the live ChEMBL-derived rows:
  - `ambiguous_mutation_context` is triggered whenever a pair key contains `mutation_unknown`, even when the source row is explicitly non-mutant and the unknown token only came from a conservative source-grouping override
  - `non_high_confidence_assay_fields` is triggered for any non-high assay field, even when the low-confidence field is advisory rather than scientifically disqualifying

## Proposed Release Pass

### Phase 5: Release Blocker Narrowing

- Keep the mutation-safety guard, but distinguish:
  - truly ambiguous mutation context that should block release
  - non-mutant source override rows that should remain review-visible but not be auto-blocked
- Keep assay confidence reporting, but separate:
  - critical low-confidence assay fields that should block model-ready release
  - advisory low-confidence fields that should stay in review outputs only

### Phase 6: Workspace Artifact Refresh

- Rebuild the live workspace artifacts in the correct order after the release-policy fix:
  - training-quality report
  - splits
  - release artifacts / release check
- Confirm whether the workspace is genuinely release-capable after the narrower policy or still blocked by data breadth.

## Assumptions For Current Pass

- ChEMBL rows with direct standardized affinity values and explicit `binding_affinity_is_mutant_measurement=false` should not be treated as mutation-ambiguous solely because a conservative grouping override encoded `mutation_unknown:<record_id>`.
- A non-high confidence `pair_identity_key` caused by a source-grouping override is review-relevant, but not by itself sufficient to make an otherwise usable affinity row non-releaseable.
- If the workspace remains weak after policy repair, the right conclusion is "not release-ready due to corpus breadth", not "force the release gate open."

## Risks For Current Pass

- Narrowing release blockers too far could let biologically ambiguous rows into the model-ready surface.
- Rebuilding live artifacts may expose additional stale-state inconsistencies that need cleanup.
- The workspace may still remain blocked after the fix simply because the corpus is too small, which is a real product limitation rather than a code bug.

## Current Release State

- The live workspace is now release-ready by the current gate:
  - `release_status`: `ready`
  - `blockers`: none
  - `warnings`: none
- Current live release counts:
  - `canonical_entry_count`: `4644`
  - `canonical_pair_count`: `32`
  - `model_ready_pair_count`: `32`
  - `model_ready_exclusion_count`: `0`
- Current live quality gates:
  - training quality: `usable_with_gaps`
  - model comparison: `comparison_ready`
  - identity crosswalk: `ready`
- The remaining future work is no longer about clearing release readiness; it is about expanding breadth and scientific depth beyond the current validated release surface.

## Current Session Recovery State

- The prior session terminated during an operational hardening pass, but the partial work was intact on disk.
- Focused validation after recovery confirmed the stage-lock changes were coherent and passing.
- The next maturity step is to make long-running stage activity visible in the GUI so interrupted or overlapping runs are obvious to operators.

## Current Operational Gap

- The last live corpus-expansion pass exposed a real operational weakness:
  - long-running `extract --existing-only --force` jobs can overlap in the same workspace
  - overlapping runs can leave extracted tables mid-rewrite and make downstream reports temporarily inconsistent
- The repo already records stage-state manifests, but it does not yet prevent concurrent execution of the same stage against the same workspace.

## Proposed Operational Hardening Pass

### Phase 9: Stage Locking And Long-Run Safety

- Add a lightweight per-stage workspace lock for long-running mutating stages, starting with `extract`.
- Treat an active lock from a live PID as a hard stop for a second run.
- Treat a lock from a dead PID as stale and recover automatically.
- Write explicit `running` / `failed` / `completed` stage-state updates so operators can see what happened after interrupted runs.

## Proposed Next Phase

### Phase 7: Real Corpus Expansion

- Re-ran existing-entry extraction with live assay sources enabled until the refreshed cache materially improved the corpus.
- Rebuilt training examples, splits, identity crosswalk, model evaluations, and release artifacts from the refreshed cache state.
- Confirmed the previous `16`-pair release surface was stale; the current release surface is `32` model-ready pairs.

### Phase 8: Release Warning Reduction

- Tightened release-readiness semantics so they reflect:
  - pair-level identity mapping quality rather than global background fallback counts
  - real release blockers rather than GUI-framework preference or explicitly non-authoritative experimental surfaces
- Result: live workspace release check now reports no blockers and no warnings.

## Progress

- Completed in this pass:
  - added a first-class source capability registry and report/export flow
  - added a conservative protein / ligand / pair identity crosswalk export
  - surfaced source configuration and identity crosswalk status in the GUI overview
  - tightened training-quality readiness so tiny corpora are marked undersized instead of strong
  - generated source-capability and identity-crosswalk artifacts in the live workspace
  - revalidated the repo with the full suite
- Remaining larger work after this pass:
  - deeper source breadth (UniProt / InterPro / Pfam / CATH / SABIO-RK style adapters)
  - richer canonical entity resolution beyond extracted-table crosswalks
  - broader GUI workflow guidance around search breadth and bias-aware acquisition
  - richer search result metadata previews before ingest
  - broader source-adapter expansion beyond the current structure/affinity set
- Completed in the current follow-on pass:
  - added explicit search-result limit controls to the criteria model and GUI
  - added best-effort representative sampling for capped RCSB ingest result sets
  - kept ingest semantics backward-compatible when no limit is set
  - revalidated the repo after the ingest/search changes
- Current pass in progress:
  - add pre-ingest search preview/reporting
  - strengthen release-readiness checks and GUI visibility
- Completed in the current release-hardening pass:
  - added `preview-rcsb-search` CLI/report flow and GUI trigger
  - added search preview visibility in the GUI overview
  - strengthened release readiness with source/training/model/identity quality gates
  - added release readiness visibility in the GUI overview
  - fixed a metadata-harvest workspace directory robustness bug
  - generated fresh live-workspace preview and release-check artifacts
- Current pass in progress:
  - narrow over-broad release blockers caused by conservative ChEMBL grouping overrides
  - refresh live workspace training/split/release artifacts after the policy fix
- Completed in the current release-readiness recovery pass:
  - narrowed release blockers for explicit non-mutant ChEMBL override rows
  - regenerated live workspace split metadata
  - regenerated release artifacts so the live workspace is now release-ready by the current gate
  - refreshed baseline/tabular evaluations so model comparison is ready again
- Current pass in progress:
  - attempt real corpus expansion with live sources to reduce remaining release warnings honestly
- Completed in the current corpus-expansion and release-finalization pass:
  - expanded the live assay/training surface from 16 to 32 supervised pairs
  - rebuilt splits, identity crosswalk, model evaluations, and release artifacts from the refreshed cache
  - fixed BindingDB 404 handling at the adapter layer so expected absence is not reported as source-state error
  - tightened release-readiness identity/training warning semantics to match the authoritative release surface
  - revalidated the full repo and GUI import path
  - confirmed the live workspace is release-ready with no blockers and no warnings
- Completed in the current operational hardening pass:
  - added a per-stage workspace lock for `extract`
  - added explicit `running` / `failed` / `completed` extract stage-state transitions
  - added tests covering stale-lock recovery and rejection of overlapping extract runs
- Current pass in progress:
  - surface active stage-state / lock visibility in the GUI overview
  - re-run full validation after the recovery pass

## Reviewer-Guided Remediation Pass

Updated: 2026-03-12 (reviewer handoff execution)

### Current Repo State

- The repository is broadly functional, but not every subsystem is equally mature.
- The reviewer handoff identified several concrete issues that are more important than speculative new feature work:
  - duplicated JSON-table loaders spread across training, graph, feature, and export modules
  - incomplete training-manifest visibility into missing upstream tables
  - Windows cleanup noise in the pytest temp-artifact teardown
  - ambiguous BindingDB affinity-log convention
  - over-broad metal role reassignment in RCSB bound-object classification
- Those are a mix of correctness, maintainability, and operator-trust issues, so this pass will prioritize them before more source expansion.

### Proposed Phases

#### Phase A: Shared Table I/O Refactor

- Add reusable JSON table/file loading helpers in `src/pbdata/table_io.py`.
- Replace duplicated local loaders in high-traffic modules first:
  - training assembly
  - graph builder
  - feature builder
- Keep behavior backward-compatible, including unreadable-file warning semantics where already present.

#### Phase B: Training Assembly Completeness Reporting

- Extend the training manifest with explicit source presence/missingness indicators.
- Report which upstream extracted/feature/graph inputs were present, missing, or empty.
- Add lightweight completeness warnings so operators can see when assembly succeeded but the resulting corpus is under-supported.

#### Phase C: Semantics And Reliability Fixes

- Make the BindingDB affinity-log convention explicit in provenance/docs instead of leaving the scale implicit.
- Narrow metal role disambiguation so metal ions are not blanket-promoted whenever any small molecule is present.
- Harden `tests/conftest.py` teardown so Windows file-lock behavior does not create noisy cleanup failures.

#### Phase D: Documentation And Validation

- Add `docs/spec_deviations.md` to document where the implementation intentionally differs from the broad architectural target.
- Add or update focused tests for the touched paths.
- Run targeted validation for the refactor and reviewer fixes.

### Assumptions

- This pass should preserve existing working workflows rather than re-architecting the entire repo around a new data model.
- The current `binding_affinity_log10_standardized` field remains numerically compatible for existing code; this pass clarifies its convention rather than renaming the field everywhere.
- Bound-object disambiguation must remain heuristic because coordinate-level metal-bridge evidence is not always available at classification time.

### Risks

- A broad utility refactor can introduce subtle behavior drift if warning/skip behavior changes across callers.
- Tightening metal-role reassignment too much may reduce some true positives for bridge-like ions, but the current blanket reassignment is less defensible.
- Adding completeness reporting may expose thin live workspaces that were previously only implicitly weak.

### Immediate Next Steps

1. Add shared JSON table/file loaders and adopt them in the core materialization modules.
2. Extend training assembly manifests with explicit presence/missingness/completeness reporting.
3. Fix BindingDB log-convention provenance, metal-role reassignment, and test cleanup robustness.
4. Add `docs/spec_deviations.md` and update summary documentation after validation.

## Metadata-Aware Dataset Governance Pass

Updated: 2026-03-12 (follow-on split/quality refinement)

### Current Repo State

- Core split diagnostics and source-capability reporting are in place.
- The remaining high-value gap is that harvested metadata existed, but split logic and training-quality reporting were still relying mostly on receptor identity and ligand proxies.
- Several modules outside the first refactor wave still duplicated the same JSON table-loading logic.

### Proposed Phases

#### Phase E: Finish Shared Table-I/O Adoption

- Replace remaining duplicated JSON-table loaders in:
  - microstate features
  - MM planning/bundle generation
  - conformational-state builder
  - identity crosswalk
  - workflow-engine metadata harvest
  - selected CLI helpers

#### Phase F: Metadata-Aware Split Enrichment

- Extend pair split items with metadata-derived grouping channels when `workspace/metadata/protein_metadata.csv` is available:
  - domain/family proxy from InterPro or Pfam
  - pathway proxy from Reactome
  - fold/state proxy from structural-fold fields already harvested
- Keep fallback behavior stable when metadata harvest has not been run.

#### Phase G: Training-Quality Integration

- Feed split diagnostics back into training-quality reporting so leakage or dominance risk is visible alongside supervision coverage.
- Add metadata-coverage counters for family/pathway breadth.

#### Phase H: GUI Maturity Visibility

- Make the GUI overview clearer about:
  - source maturity (`enabled`, `implemented`, `planned`, `misconfigured`)
  - metadata-aware split overlap (`pathway_overlap`)

### Assumptions

- Metadata harvest remains optional; the split builder must still work without it.
- InterPro/Pfam/Reactome-derived grouping is a better proxy than raw receptor identity alone, but it is still not a substitute for curated homology or structural-class annotations.

### Risks

- Metadata-derived grouping can make small corpora look more dominated or more constrained, but that is the honest outcome for benchmark governance.
- Expanding diagnostics fields requires keeping GUI/report consumers backward-compatible.

### Immediate Next Steps

1. Finish the table-I/O refactor in the remaining duplicated modules.
2. Read harvested metadata in split-item construction and propagate new grouping keys into diagnostics.
3. Push split-diagnostic status into training-quality outputs and GUI overview KPIs.
4. Re-run targeted regression across split, quality, GUI overview, and metadata workflows.

## Operational Visibility Pass

Updated: 2026-03-12 (active stage-state visibility)

### Current Repo State

- Stage locking for `extract` is already implemented and stage-state manifests are written.
- The remaining operator gap is visibility rather than core functionality:
  - GUI overview only shows aggregate active-operation counts
  - `pbdata status` does not expose active locks / latest stage-state details
  - stale versus active lock context is not easy to inspect without opening JSON manifests directly

### Proposed Phases

#### Phase I: Shared Operational Snapshot Enrichment

- Extend workspace-status reporting with stage-activity counters and latest-stage metadata.
- Reuse existing stage-state manifests rather than inventing a second operational state model.

#### Phase J: GUI Active-Operation Detail Surface

- Expand the GUI overview so Active Operations shows:
  - active or stale lock ownership context
  - latest stage + status
  - latest manifest notes when available
- Keep the layout additive and compact.

#### Phase K: CLI Status Visibility

- Add stage-activity lines to `pbdata status` so operators can see concurrent-run risk and recent failures without opening artifacts manually.

#### Phase L: Validation

- Add focused tests for stage-activity reporting in overview and status surfaces.
- Run targeted regression for `stage_state`, `ops`, and CLI reporting.

### Assumptions

- Stage-state manifests are operational truth for this repo; this pass should surface them better rather than broadening stage locking to every command immediately.
- The latest manifest timestamp is sufficient for “most recent stage” reporting even when multiple historical stage manifests exist.

### Risks

- Surfacing stale locks too aggressively could alarm users if the status text is ambiguous.
- Adding status fields must remain backward-compatible for existing tests and downstream dictionary consumers.

### Immediate Next Steps

1. Add stage-activity fields to the shared workspace status report.
2. Expand the GUI Active Operations summary with lock/stage detail lines.
3. Update CLI status rendering to include the new operational state.
4. Run focused tests and fix any regressions.
