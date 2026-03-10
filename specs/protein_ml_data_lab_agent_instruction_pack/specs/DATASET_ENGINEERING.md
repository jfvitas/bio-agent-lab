# Dataset Engineering Specification

Purpose:
Build **diverse, unbiased ML datasets**.

## Input

Protein metadata table.

## Diversity Goals

Ensure diversity across:

- protein family
- organism
- structural fold
- binding interface type
- ligand class

## Algorithm

1. Generate sequence embeddings (ESM)
2. Cluster embeddings
3. Perform stratified sampling

## Data Leakage Prevention

Never allow:

- homologous proteins in train/test
- mutation variants across splits
- same protein family across splits (optional strict mode)

## Outputs

train.csv  
test.csv  
(optional) cv_folds/