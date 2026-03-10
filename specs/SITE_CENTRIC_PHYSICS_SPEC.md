
# SITE-CENTRIC PHYSICS ENRICHMENT SPECIFICATION
Version: 1.0
Status: Authoritative
Applies to: All coding agents building the binding prediction pipeline

This document defines the exact implementation specification for the
Site-Centric Physics Enrichment subsystem used by the Bio-Agent-Lab platform.

The subsystem generates environment-conditioned physical descriptors
for chemically relevant atomic sites in proteins and ligands.

These descriptors are attached to the protein/ligand graph and consumed by
the main interaction prediction model.

------------------------------------------------------------

# 1. SYSTEM OVERVIEW

The system has three layers:

1. Motif Physics Dataset (offline computation)
2. Local Physics Surrogate Model
3. Graph Enrichment Pipeline

Workflow:

structure → site identification → environment extraction → surrogate inference → enriched graph

------------------------------------------------------------

# 2. SITE IDENTIFICATION

Sites are atoms or pseudo-atoms that participate in intermolecular interactions.

Each site must be assigned a motif class.

------------------------------------------------------------

# 3. PROTEIN SITE MOTIF CLASSES

Backbone
- backbone_carbonyl_oxygen
- backbone_amide_nitrogen

Acidic residues
- asp_carboxylate_oxygen
- glu_carboxylate_oxygen

Amide residues
- asn_carbonyl_oxygen
- asn_amide_nitrogen
- gln_carbonyl_oxygen
- gln_amide_nitrogen

Hydroxyl residues
- ser_hydroxyl_oxygen
- thr_hydroxyl_oxygen
- tyr_hydroxyl_oxygen

Basic residues
- lys_terminal_amine_nitrogen
- arg_terminal_nitrogen
- arg_central_nitrogen
- his_delta_nitrogen
- his_epsilon_nitrogen

Sulfur
- cys_sulfur
- met_sulfur

Aromatic pseudo-sites
- phe_aromatic_centroid
- tyr_aromatic_centroid
- trp_aromatic_centroid
- his_aromatic_centroid

Special
- trp_indole_nitrogen
- n_terminus_amine
- c_terminus_carboxylate
- metal_ion

------------------------------------------------------------

# 4. LIGAND SITE MOTIF CLASSES

- carbonyl_oxygen
- carboxylate_oxygen
- hydroxyl_oxygen
- ether_oxygen
- amide_nitrogen
- amine_nitrogen
- aromatic_nitrogen
- nitrile_nitrogen
- thiol_sulfur
- thioether_sulfur
- aromatic_centroid
- halogen_atom
- cation_center
- anion_center
- metal_chelation_atom

------------------------------------------------------------

# 5. LOCAL ENVIRONMENT EXTRACTION

For each site extract neighbors within:

8.0 Å

Shell definitions:

Shell 1: 0 – 3.5 Å  
Shell 2: 3.5 – 6.0 Å  
Shell 3: 6.0 – 8.0 Å  

------------------------------------------------------------

# 6. ENVIRONMENT DESCRIPTOR VECTOR

Compute descriptors per shell.

Geometry
- neighbor_atom_count
- heavy_atom_count
- polar_atom_count
- charged_atom_count
- aromatic_centroid_count
- metal_count
- nearest_neighbor_distance

Charge summaries
- sum_partial_charge
- sum_positive_charge
- sum_negative_charge
- inverse_distance_charge_sum
- inverse_square_charge_sum

Electrostatics
- electric_field_vector_proxy
- electric_field_magnitude
- electrostatic_potential_proxy

Hydrogen bonding
- donor_count
- acceptor_count
- hbond_candidate_count
- intramolecular_hbond_satisfied_flag

Solvent/burial
- sasa_site
- sasa_residue
- burial_score
- pocket_score
- solvent_distance

Flexibility
- normalized_b_factor
- occupancy
- sidechain_rotamer_class
- backbone_phi
- backbone_psi

Context
- secondary_structure_class
- residue_depth
- interface_flag

------------------------------------------------------------

# 7. ARCHETYPE GENERATION

From the structure corpus:

1. collect environments for each motif class
2. cluster environments using descriptor vectors
3. choose representative archetypes

Target archetypes per motif class:

20–60

------------------------------------------------------------

# 8. PHYSICS CALCULATIONS

For each archetype construct a local fragment including:

- central site
- atoms within 5 Å
- capped bonds

Compute target descriptors:

- refined_partial_charge
- electrostatic_potential
- electric_field
- donor_strength
- acceptor_strength
- polarizability_proxy
- steric_radius
- desolvation_penalty
- protonation_preference
- metal_binding_propensity
- aromatic_interaction_propensity

------------------------------------------------------------

# 9. PROBE INTERACTIONS

Probe types:

- donor_probe
- acceptor_probe
- cation_probe
- anion_probe
- aromatic_probe
- hydrophobe_probe
- metal_probe

Distances:

2.2 Å
2.5 Å
3.0 Å
3.5 Å
4.0 Å
5.0 Å

Orientations:

- inline
- angled
- perpendicular

------------------------------------------------------------

# 10. SURROGATE MODEL

Input:

- motif_class
- environment_descriptor_vector
- local_neighborhood_graph

Output:

refined_descriptor_vector (16–32 values)

Architecture recommendation:

local equivariant GNN

Cutoff: 8 Å

------------------------------------------------------------

# 11. GRAPH ENRICHMENT PIPELINE

Algorithm:

1. parse structure (mmCIF)
2. build base graph
3. identify site nodes
4. extract environment vectors
5. run surrogate model
6. attach refined descriptors to node features
7. compute pairwise interaction features
8. export enriched graph

------------------------------------------------------------

# 12. NODE FEATURES

Base features
- atomic_number
- coordinates
- formal_charge
- initial_partial_charge
- sasa
- burial_score
- b_factor
- occupancy

Refined features
- refined_partial_charge
- electrostatic_potential
- electric_field_magnitude
- donor_strength
- acceptor_strength
- polarizability_proxy
- steric_radius
- desolvation_penalty
- protonation_preference
- metal_binding_propensity
- aromatic_interaction_propensity

------------------------------------------------------------

# 13. EDGE FEATURES

- distance
- orientation_angle
- coulombic_proxy
- hbond_geometry_score
- salt_bridge_score
- vdW_overlap_score
- steric_clash_score
- aromatic_stack_score
- metal_bridge_flag

------------------------------------------------------------

# 14. APO / HOLO STATE FEATURES

If both structures exist compute descriptors for each state.

Add delta features:

- delta_sasa
- delta_potential
- delta_field
- delta_burial
- delta_position

------------------------------------------------------------

# 15. CACHING

Cache descriptors using:

hash(motif_class + environment_vector)

Reuse descriptors whenever possible.

------------------------------------------------------------

# 16. PERFORMANCE RULES

- Surrogate inference must be batchable
- Site descriptors must be precomputed
- Archetype QM calculations must remain offline

------------------------------------------------------------

# 17. OUTPUT FORMAT

Each enriched graph must include:

node_features  
edge_features  
global_features  
label

Store as:

Parquet + tensor bundle  
or serialized graph object.

------------------------------------------------------------

# 18. ACCEPTANCE TESTS

Implementation passes if:

1. motif classes assigned correctly
2. environment vectors deterministic
3. archetype clustering produces 20–60 clusters per motif class
4. surrogate outputs descriptor vectors
5. node features include refined descriptors
6. edge features include interaction priors

END OF SPECIFICATION
