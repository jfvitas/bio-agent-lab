# Implementation Summary

Updated: 2026-03-12 (annotation-adapter + UX hardening pass)

## What Changed

- Added first-class split diagnostics:
  - `data/splits/split_diagnostics.json`
  - `data/splits/split_diagnostics.md`
- Wired split diagnostics into `build-splits` for:
  - pair-aware grouped splits
  - scaffold / family / mutation / source grouped splits
  - time-ordered splits
- Added GUI overview visibility for split diagnostics so operators can see:
  - current split strategy
  - hard-group overlap
  - family overlap
  - dominant family share
- Extended the source registry so it now describes a broader multi-source roadmap with explicit implementation state:
  - currently implemented sources
  - planned source families such as AlphaFold DB, UniProt, InterPro, Pfam, CATH, SCOP, STRING, BioGRID, IntAct, Reactome, SABIO-RK, PROSITE, and ELM
- Implemented real metadata-source adapters for:
  - UniProt
  - AlphaFold DB
  - Reactome
- Implemented PDBe/SIFTS-backed annotation adapters for:
  - InterPro
  - Pfam
  - CATH
  - SCOP
- Wired those adapters into the workflow-engine metadata path:
  - `harvest-metadata --with-uniprot`
  - `harvest-metadata --with-alphafold`
  - `harvest-metadata --with-reactome`
  - `harvest-metadata --with-interpro`
  - `harvest-metadata --with-pfam`
  - `harvest-metadata --with-cath`
  - `harvest-metadata --with-scop`
  - optional `--max-proteins` cap for bounded enrichment runs
- Exposed the metadata harvest enrichment controls in the GUI pipeline options so the new sources are usable from the primary interface.
- Tightened release/readiness reporting so split diagnostics flow into release artifacts and can raise warnings or blockers when leakage-risk conditions are detected.
- Added shared JSON row/table loading helpers in `src/pbdata/table_io.py` and adopted them in core materialization/export modules:
  - training assembly
  - graph builder
  - feature builder
  - master export
- Extended training assembly manifests with explicit upstream completeness reporting:
  - `sources_present`
  - `sources_missing`
  - `sources_empty`
  - `source_status`
  - `data_completeness.warnings`
- Narrowed RCSB metal-role reassignment so metal ions are only promoted to `metal_mediated_contact` when they share chain-local context with a small-molecule ligand.
- Made the BindingDB affinity-log convention explicit in source provenance:
  - `assay_value_log10_convention = log10_nM`
  - `standardized_affinity_unit = nM`
- Hardened pytest temporary-artifact cleanup for Windows-style file-lock behavior.
- Added `docs/spec_deviations.md` to document intentional current-state deviations from the broader platform target.
- Finished the next shared table-I/O adoption wave in:
  - [src/pbdata/features/microstate.py](/C:/Users/jfvit/Documents/bio-agent-lab/src/pbdata/features/microstate.py)
  - [src/pbdata/features/mm_features.py](/C:/Users/jfvit/Documents/bio-agent-lab/src/pbdata/features/mm_features.py)
  - [src/pbdata/dataset/conformations.py](/C:/Users/jfvit/Documents/bio-agent-lab/src/pbdata/dataset/conformations.py)
  - [src/pbdata/identity_crosswalk.py](/C:/Users/jfvit/Documents/bio-agent-lab/src/pbdata/identity_crosswalk.py)
  - [src/pbdata/data_pipeline/workflow_engine.py](/C:/Users/jfvit/Documents/bio-agent-lab/src/pbdata/data_pipeline/workflow_engine.py)
  - [src/pbdata/cli.py](/C:/Users/jfvit/Documents/bio-agent-lab/src/pbdata/cli.py)
- Extended split items and diagnostics with metadata-aware grouping channels:
  - `domain_group_key`
  - `pathway_group_key`
  - `fold_group_key`
- Taught `build-splits` to prefer harvested metadata from `workspace/metadata/protein_metadata.csv` when building family/domain/pathway/fold proxies.
- Integrated split-diagnostic status and metadata breadth into training-quality reporting.
- Extended custom training-set selection to understand metadata-aware benchmark modes:
  - `metadata_family`
  - `pathway_group`
  - `fold_group`
- Extended release-readiness reporting to warn on metadata-group overlap and missing metadata-family coverage when the corpus is otherwise usable.
- Updated the GUI overview to surface:
  - source maturity as `enabled / implemented / planned / misconfigured`
  - split `pathway_overlap` alongside existing overlap/dominance KPIs
- Added shared file-health scanning in [src/pbdata/file_health.py](/C:/Users/jfvit/Documents/bio-agent-lab/src/pbdata/file_health.py) and wired it into:
  - `pbdata status`
  - new `pbdata clean`
  - normalize / audit / report corrupt-file summaries
- Added explicit processed-data integrity counts to workspace status so empty/corrupt normalized files are visible instead of being hidden inside inflated file counts.
- Added `pbdata clean --processed/--raw --delete` to remove empty/corrupt JSON caches without manual filesystem cleanup.
- Aggregated corrupt-file warnings in normalize / audit / report / hash split flows so users see summary counts instead of hundreds of per-file lines.
- Added feature-pipeline stage discoverability:
  - `run-feature-pipeline --list-stages`
  - richer invalid-stage error messages
  - clearer `--stage-name` help text
- Improved pathway-risk CLI guidance by including a concrete `--targets` usage example in the missing-input error.
- Added conservative sequence-only peptide prediction support:
  - `predict-peptide-binding --fasta`
  - manifest status `baseline_sequence_only_predictions_generated`
  - explicit note that structure input remains the stronger path
- Added ligand-screening `binding_call` / `likely_binder` outputs so very weak predictions are easier to distinguish from plausible binders.

## What Now Works

- Split generation is no longer a black box. Pair-aware split modes now emit auditable leakage/dominance diagnostics that downstream reports and the GUI can consume.
- The GUI overview now has an explicit split-governance surface instead of relying on users to inspect raw split metadata files manually.
- The source capability layer is closer to the product’s intended breadth and is clearer about what is implemented versus planned.
- The workflow-engine metadata table can now carry real annotation columns from UniProt / AlphaFold / Reactome instead of only extracted-table fields.
- The workflow-engine metadata table can now also carry:
  - `interpro_ids`
  - `pfam_ids`
  - `cath_ids`
  - `scop_ids`
  - `cath_names`
  - `scop_names`
- Release exports now carry split-diagnostic context rather than only split counts and strategy text.
- Table-loading behavior is less fragmented across the repo, reducing duplicated JSON parsing code in major pipeline modules.
- Training manifests now tell operators whether a run was assembled from complete upstream inputs or merely succeeded with missing/empty layers.
- BindingDB-derived log-affinity values are no longer semantically implicit at the provenance level.
- RCSB bound-object classification no longer overstates metal-mediated contacts for unrelated structural ions in mixed entries.
- Split diagnostics can now flag pathway/domain/fold overlap when harvested metadata is available, rather than relying only on receptor identity and ligand proxies.
- Training-quality reports now carry split-diagnostic state and metadata breadth counts, so weak held-out design is visible next to label coverage and conflict rates.
- The GUI source-configuration surface is clearer about maturity: implemented versus planned sources are now distinguished in the overview layer.
- The CLI now surfaces processed-data integrity instead of hiding corrupt caches behind a single file-count number.
- Users can repair empty/corrupt JSON caches directly from the application with `pbdata clean`.
- Normalize, audit, report, and hash split runs no longer flood the console with one warning per corrupt file.
- Feature-pipeline stage-only mode is now discoverable from the CLI instead of requiring source inspection.
- Peptide prediction now has a lower-confidence FASTA path instead of failing outright on sequence input.
- Ligand-screening manifests now label likely versus unlikely binders instead of leaving users to infer that from extreme Kd values.
- Validation completed for the touched surfaces:
  - targeted metadata/release/training-set slice: `43 passed`
  - user-tester handoff regression slice: `42 passed`
  - broader touched-area regression: `134 passed`
  - `python -m compileall src`: passed

## External Data / Access Still Relevant

- Planned sources added to the registry are architecture/reporting scaffolds only. They are not full ingest adapters yet.
- Full biological breadth still depends on:
  - additional live APIs
  - optional licensed/local datasets
  - future adapter work
- The full repo suite did not finish within the current execution timeout window, so this pass is validated by targeted tests plus compile checks rather than a confirmed end-to-end full-suite completion.

## What Should Be Done Next

1. Deepen the new annotation adapters so CATH / SCOP / InterPro / Pfam hierarchy levels are normalized beyond flat IDs and can drive stronger family/fold holdouts.
2. Push metadata-aware signals further into release thresholds and dataset-builder presets, especially where pathway/fold overlap should become a harder warning or block.
3. Normalize the remaining source-mode differences in the GUI so operators can tell ingest sources from enrichment and metadata-only sources at a glance.
4. Push the new file-health signals into GUI overview panels and long-running progress surfaces.
5. Re-run the full suite serially in an environment with a longer timeout budget and record the final wall-clock outcome.
