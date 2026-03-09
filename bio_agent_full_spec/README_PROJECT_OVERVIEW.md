
# Protein Binding Dataset & Graph-ML Platform

This repository builds a **multi-modal dataset** and **graph knowledge layer**
for predicting molecular binding interactions between:

- proteins
- ligands
- peptides
- cofactors
- metals
- glycans
- protein complexes

The system combines:

1. Structural data (PDB/mmCIF)
2. Experimental binding measurements
3. Biological pathway information
4. Protein–protein interaction networks
5. Ligand chemical features
6. Molecular mechanics features (Rosetta/CHARMM/AMBER optional)
7. Structural similarity signals

Outputs are **training example files** ready for ML training.

Training examples combine:

structure features  
biological features  
graph features  
experimental measurements  
chemical descriptors

