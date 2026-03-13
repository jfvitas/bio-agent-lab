# Specification Deviations

Updated: 2026-03-12

This document records deliberate implementation deviations from the broad platform target so operators and future maintainers can distinguish between:

- intentional current-state design
- incomplete future work
- bugs that still need fixing

## GUI Toolkit

- Current implementation: `Tkinter`
- Broader target discussed in planning: richer GUI platform abstractions
- Rationale:
  - Tkinter is already wired through the repo and has broad local availability.
  - Replacing it midstream with a different desktop stack would create avoidable churn without improving data integrity.
- Implication:
  - GUI improvements should continue inside the current Tkinter architecture unless there is a specific migration plan.

## Affinity Log Convention

- Current implementation field: `binding_affinity_log10_standardized`
- Current numeric convention:
  - standardized raw affinity values are normalized to `nM`
  - the log field represents `log10(nM)`
- This is not the same as medicinal-chemistry-style `pKd/pKi = -log10(M)`.
- Rationale:
  - the pipeline already uses a monotonic, standardized affinity transform across merged assay sources
  - renaming the field everywhere would create broad schema churn
- Mitigation:
  - source provenance now records the convention explicitly for BindingDB-derived rows
  - downstream consumers should treat the field as a standardized regression label, not assume p-scale semantics

## Structural Feature Depth

- Current implementation:
  - graph, feature, and training-example layers are real and materialized
  - some advanced structural/physics stages remain heuristic or planned
- Examples:
  - microstate assignment is heuristic local-context logic
  - MM job generation creates explicit planning/runtime bundles rather than running a full production MM workflow
- Rationale:
  - explicit heuristic/planned artifacts are preferable to silently overstating scientific fidelity

## Source Coverage Uniformity

- Current implementation:
  - sources are not all at the same maturity level
- Practical source classes today:
  - real ingest/query sources: `RCSB`, `SKEMPI`
  - enrichment/query-time sources: `BindingDB`, `ChEMBL`
  - local-dataset parsers: `PDBbind`, `BioLiP`
  - metadata harvest sources: `UniProt`, `AlphaFold DB`, `Reactome`
- Rationale:
  - the architecture is intentionally broader than the currently completed adapters
  - the registry distinguishes implemented versus planned capabilities so the platform does not overclaim completeness

## Metal Role Disambiguation

- Current implementation:
  - metal ions are no longer blanket-promoted to `metal_mediated_contact` whenever any ligand exists
  - reassignment now requires shared chain-local context with a small-molecule ligand
- Remaining limitation:
  - this is still heuristic and does not replace coordinate-level bridge validation
- Rationale:
  - the prior blanket behavior overclaimed mediation for unrelated structural ions
