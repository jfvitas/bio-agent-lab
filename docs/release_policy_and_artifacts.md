# Release Policy And Artifact Contract

This document defines the supported release surface for the current implementation.

## Current implementation stance

- The shipped GUI is the Tkinter application in `src/pbdata/gui.py`.
- The release contract is defined by the CLI, root review exports, and frozen snapshot artifacts.
- Some subsystems remain intentionally labeled `Experimental` or `Baseline`:
  - site-physics offline feedback loop
  - prediction manifests
  - pathway/risk scoring

These surfaces are available for expert use, but they are not the core release-grade dataset contract.

This repository now maintains two artifact layers:

## 1. Live review artifacts in the repo root

These files are refreshed from the current local dataset state:

- `master_pdb_repository.csv`
- `master_pdb_pairs.csv`
- `master_pdb_issues.csv`
- `master_pdb_conflicts.csv`
- `master_source_state.csv`
- `canonical_entries.csv`
- `canonical_pairs.csv`
- `model_ready_pairs.csv`
- `model_ready_exclusions.csv`
- `split_summary.csv`
- `dataset_release_manifest.json`
- `scientific_coverage_summary.json`

These additional live artifacts are created when you build a custom training set:

- `custom_training_set.csv`
- `custom_training_exclusions.csv`
- `custom_training_summary.json`
- `custom_training_manifest.json`

These are intended for inspection, iteration, and current-state QA.

## 2. Frozen release snapshots under the storage root

Use:

```bash
pbdata --storage-root <root> release-check
pbdata --storage-root <root> build-release --tag <release_tag>
```

For strict release gating, use:

```bash
pbdata --storage-root <root> build-release --tag <release_tag> --strict
```

This writes a snapshot under:

`<storage root>/data/releases/<release_tag>/`

and updates:

`<storage root>/data/releases/latest_release.json`

The snapshot includes:

- canonical entry export
- canonical pair export
- model-ready pair export
- model-ready exclusions
- split summary
- release manifest
- scientific coverage summary
- custom training-set artifacts if they exist at snapshot time
- root review exports available at snapshot time
- `release_snapshot_manifest.json`
- `release_readiness_report.json`

## Scientific coverage summary

`scientific_coverage_summary.json` is a root-level review artifact that summarizes
the current dataset breadth without introducing any new biological inference.

It captures:

- entry, pair, issue, and model-ready counts
- assay source/type coverage
- experimental method coverage
- membrane / soluble and oligomeric coverage
- ligand and interface type coverage
- major biological flags such as metals, cofactors, glycans, peptides, and covalent binders
- current exclusion reasons from the release policy

## Model-ready policy

`model_ready_pairs.csv` is intentionally conservative.

Pairs are excluded if they have any of these blockers:

- `missing_structure_file`
- `no_matched_interface`
- `ambiguous_mutation_context`
- `source_value_conflict`
- `non_high_confidence_assay_fields`
- `missing_preferred_source`
- `missing_standardized_affinity`

These exclusions are written explicitly to `model_ready_exclusions.csv`.

## Important assumptions

- The release layer does not silently repair biological ambiguity.
- If source reconciliation is unresolved, the pair stays excluded rather than being forced into the model-ready set.
- Root review artifacts reflect the current repo state; release snapshots are the durable, versioned artifacts for downstream use.

## Intended workflow

1. Run ingest / extract / graph / features / training / splits.
2. Inspect root review artifacts.
3. Optionally run `build-custom-training-set` to create a diversity-optimized subset for model development.
4. Run `release-check` and resolve blockers.
5. Resolve or accept exclusions and warnings.
6. Run `build-release`.
6. Treat the snapshot directory as the frozen dataset handoff.
