# Graph Generation Specification

Graphs represent protein structures.

Supported graph types:

• residue graphs  
• atom graphs  

## Node Types

Residue nodes:

- amino acid type
- hydrophobicity
- charge
- secondary structure

Atom nodes:

- atomic_number
- electronegativity
- formal_charge
- vdw_radius
- hybridization
- aromatic_flag
- donor_acceptor

## Edge Types

Possible edges:

- covalent bonds
- distance neighbors
- hydrogen bonds
- salt bridges
- hydrophobic contacts
- pi stacking

## Graph Scope

Options:

- whole protein
- interface only
- N angstrom shell

## Export Formats

PyTorch Geometric  
DGL  
NetworkX