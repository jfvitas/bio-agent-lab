# HPC Precompute Workflow

This project now includes a generic shard-aware precompute layer that can be
used locally, on a workstation, or behind a scheduler such as Slurm.

The current implementation supports shard-aware precompute for:

- `extract`
- `build-structural-graphs`
- `build-graph`
- `build-features`
- `build-training-examples`

## Goal

Use the cluster for heavy reusable preprocessing, then merge the shard outputs
back into a normal workspace so later graph, feature, and training-set work can
reuse them.

## Generic command flow

Plan a run:

```bash
python -m pbdata.cli \
  --storage-root /path/to/workspace \
  --config /path/to/sources.yaml \
  plan-precompute \
  --stage extract \
  --chunk-size 500 \
  --run-id extract_20260314
```

Run one shard:

```bash
python -m pbdata.cli \
  --storage-root /path/to/workspace \
  --config /path/to/sources.yaml \
  run-precompute-shard \
  --run-id extract_20260314 \
  --chunk-index 0 \
  --workers 8
```

Merge shard outputs:

```bash
python -m pbdata.cli \
  --storage-root /path/to/workspace \
  --config /path/to/sources.yaml \
  merge-precompute-shards \
  --run-id extract_20260314
```

Report status:

```bash
python -m pbdata.cli \
  --storage-root /path/to/workspace \
  --config /path/to/sources.yaml \
  report-precompute-run-status \
  --run-id extract_20260314
```

## Run layout

Precompute runs live under:

```text
<storage-root>/runs/precompute/<run-id>/
```

Important subdirectories:

```text
run_manifest.json
chunks/
status/
shards/extract/
merged/extract/
```

## Suggested scheduler pattern

1. Run `plan-precompute` once.
2. Submit an array job where each task runs one `run-precompute-shard`.
3. After all shard jobs complete, run `merge-precompute-shards`.
4. Optionally run `report-precompute-run-status` before and after merge.

## Stage notes

- `extract`
  Best first cluster target. Produces reusable extracted tables and structure downloads.
- `build-structural-graphs`
  Produces per-PDB structural graph exports and is naturally shard-friendly.
- `build-graph`
  Produces canonical graph nodes/edges and merges by deduplicating node/edge IDs.
- `build-features`
  Produces feature records and merges by pair identity.
- `build-training-examples`
  Produces training-example records and merges by example ID.

## What this does not do yet

The current shard-aware framework is still a first usable slice:

- `normalize`, `audit`, `report`, `build-splits`, and `engineer-dataset` are not shard-aware yet
- scheduler wrappers are intentionally thin and generic
- cluster-specific environment setup should still live outside the core Python logic

## Cluster-specific wrappers

Scheduler wrappers should remain thin and optional.

Generic Slurm templates are provided under:

```text
scripts/slurm/
```

You should customize those scripts per environment without changing the core
Python logic.
