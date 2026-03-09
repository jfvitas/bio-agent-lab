
# Conformational State Modeling

The system must not assume a single static structure represents a protein target.

Multiple conformational states must be supported.

## State Sources

- Experimental PDB structures
- AlphaFold predictions
- Rosetta models

## State Attributes

Each state record must include:

- target_id
- state_id
- pdb_id
- structure_source
- apo_or_holo
- active_inactive_unknown
- open_closed_unknown
- ligand_class_in_state
- conformation_cluster
