# Internal Demo Runbook

This runbook is for an internal med-school AI team walkthrough. The goal is to
show a credible system boundary, not to overclaim model maturity.

## Narrative

1. Show environment and workspace readiness.
2. Show that the data pipeline is reproducible.
3. Show downstream graph, feature, and training artifacts.
4. Show prediction and risk surfaces as baseline/scaffold outputs only.

## Commands

```bash
pbdata status
pbdata doctor
pbdata demo-readiness
pbdata export-demo-snapshot
```

If the workspace is not demo-ready, stop and fix blockers before presenting.

For a happy-path walkthrough:

```bash
pbdata ingest --dry-run
pbdata extract
pbdata normalize
pbdata audit
pbdata report
pbdata build-graph
pbdata build-features
pbdata build-training-examples
pbdata train-baseline-model
```

Optional baseline demonstrations:

```bash
pbdata predict-ligand-screening --smiles "CC(=O)Oc1ccccc1C(=O)O"
pbdata predict-peptide-binding --structure-file data/structures/rcsb/1ATP.cif
pbdata score-pathway-risk --targets "P00533,P04637"
```

## Screens To Show

1. GUI Data Overview
2. GUI Demo Readiness panel
3. Generated `artifacts/reports/demo_walkthrough.md`
4. Root export artifacts and release artifacts

## Guardrails

- Do not present prediction outputs as validated biological decision support.
- Do not imply that placeholder or heuristic risk scores are real clinical risk estimates.
- Emphasize provenance, QA, and reproducibility as the current strengths.
