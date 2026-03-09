
# Prediction Pipeline

## Step 1: Input Processing

Supported inputs:

- SMILES
- SDF
- PDB
- mmCIF
- FASTA

If sequence only:

- predict structure using AlphaFold.

## Step 2: Ligand Feature Extraction

Tools:

- RDKit
- OpenBabel

Features:

- atom graph
- molecular fingerprints
- partial charges

## Step 3: Protein Feature Extraction

Features:

- residue embeddings
- structure graph
- surface pockets

## Step 4: Candidate Target Search

Use:

- sequence similarity
- pocket similarity
- graph proximity

## Step 5: Binding Prediction

Predict:

- KD
- ΔG
- binding probability
