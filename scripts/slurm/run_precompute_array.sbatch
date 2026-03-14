#!/bin/bash
# Generic Slurm array wrapper for shard-aware precompute stages.
# Customize the variables below for your environment before submission.

#SBATCH --job-name=pbdata-precompute
#SBATCH --output=logs/pbdata_precompute_%A_%a.out
#SBATCH --error=logs/pbdata_precompute_%A_%a.err
#SBATCH --cpus-per-task=8
#SBATCH --mem=16G
#SBATCH --array=0-9

set -euo pipefail

: "${REPO_ROOT:?Set REPO_ROOT to the repository root}"
: "${STORAGE_ROOT:?Set STORAGE_ROOT to the workspace/storage root}"
: "${CONFIG_PATH:?Set CONFIG_PATH to the sources config path}"
: "${RUN_ID:?Set RUN_ID to the planned precompute run id}"

cd "${REPO_ROOT}"

python -m pbdata.cli \
  --storage-root "${STORAGE_ROOT}" \
  --config "${CONFIG_PATH}" \
  run-precompute-shard \
  --run-id "${RUN_ID}" \
  --chunk-index "${SLURM_ARRAY_TASK_ID}" \
  --workers "${SLURM_CPUS_PER_TASK:-1}" \
  ${PRECOMPUTE_STAGE_ARGS:-}
