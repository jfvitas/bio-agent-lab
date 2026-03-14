#!/bin/bash
# Generic Slurm wrapper for merging shard-aware precompute runs.
# Customize the variables below for your environment before submission.

#SBATCH --job-name=pbdata-merge-precompute
#SBATCH --output=logs/pbdata_merge_precompute_%j.out
#SBATCH --error=logs/pbdata_merge_precompute_%j.err
#SBATCH --cpus-per-task=1
#SBATCH --mem=4G

set -euo pipefail

: "${REPO_ROOT:?Set REPO_ROOT to the repository root}"
: "${STORAGE_ROOT:?Set STORAGE_ROOT to the workspace/storage root}"
: "${CONFIG_PATH:?Set CONFIG_PATH to the sources config path}"
: "${RUN_ID:?Set RUN_ID to the planned precompute run id}"

cd "${REPO_ROOT}"

python -m pbdata.cli \
  --storage-root "${STORAGE_ROOT}" \
  --config "${CONFIG_PATH}" \
  merge-precompute-shards \
  --run-id "${RUN_ID}"
