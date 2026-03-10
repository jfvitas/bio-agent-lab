# EXTERNAL ANALYSIS EXECUTION SPEC
Version: 1.0
Status: Authoritative
Applies to: Human operator and coding agents preparing offline physics analysis jobs

## 1. Purpose

This file defines exactly which analyses are run outside the project code, what software to use,
what outputs are required, and how those outputs return to the project.

## 2. External analysis overview

There are three mandatory offline analysis types in version 1:

1. Local quantum chemistry on representative fragment archetypes
2. Continuum electrostatics analysis
3. Local molecular mechanics analysis

These analyses produce the supervised labels for the surrogate model.

## 3. Analysis software

### 3.1 Quantum chemistry
Use:
- ORCA

Purpose:
- refined partial charge proxies
- electrostatic potential descriptors
- polarizability proxy inputs
- donor/acceptor behavior proxies
- frontier-orbital-related reactivity summaries if desired later

### 3.2 Continuum electrostatics
Use:
- APBS

Purpose:
- electrostatic potential grid
- electric field proxy sampled at site coordinates
- desolvation-related electrostatic context

### 3.3 Molecular mechanics
Use:
- OpenMM

Purpose:
- local steric strain
- local vdW interaction proxies
- local electrostatic interaction decomposition proxies
- restrained local minimization if required

## 4. Exact unit of analysis

The unit of offline analysis is not the whole protein.
The unit is a **local fragment archetype** centered on one site.

Each fragment must contain:
- one central site
- all atoms within 5 Å
- capped broken covalent boundaries
- original coordinates preserved unless a restrained minimization step is explicitly requested

Each fragment must have:
- fragment_id
- source structure_id
- site_id
- motif_class
- archetype_id
- total_charge
- spin_multiplicity
- fragment atom map

## 5. Human/operator workflow outside the project

The operator performs the following steps separately from ordinary project code:

### Step A
Run project code to produce:
- site environments
- motif clusters
- representative archetype fragments
- external analysis queue files

### Step B
Take the generated fragment files and run:
- ORCA jobs
- APBS jobs
- OpenMM jobs

### Step C
Place the completed outputs into the expected external-analysis results directory.

### Step D
Run the project's results-ingest pipeline to convert raw outputs into target tables.

## 6. Exact analyses to run

### 6.1 ORCA analysis per archetype fragment

Input:
- fragment coordinates
- total charge
- multiplicity
- fragment metadata JSON

Minimum ORCA job contents:
- single-point electronic structure calculation
- printed atomic charge population
- printed molecular dipole
- printed polarizability if feasible
- enough output to derive site-centered potential-related features

Recommended analysis batch:
- one fragment per job
- separate probe interaction jobs where applicable

Required outputs to retain:
- stdout text
- parsed property JSON
- run metadata JSON
- success/failure status

Required parsed fields:
- fragment_id
- central_site_atom_index
- atomic_charges
- molecular_dipole_magnitude
- polarizability_summary if available
- calculation_method
- basis_set
- status

### 6.2 Probe interaction ORCA jobs

For each archetype, create probe jobs with:

Probe types:
- donor_probe
- acceptor_probe
- cation_probe
- anion_probe
- aromatic_probe
- hydrophobe_probe
- metal_probe where chemically valid

Distances:
- 2.2
- 2.5
- 3.0
- 3.5
- 4.0
- 5.0 Å

Orientations:
- inline
- angled
- perpendicular

Required output:
- interaction_energy curve
- preferred geometry summary

If all probe jobs are too expensive initially, version 1 minimum requirement is:
- donor_probe
- acceptor_probe
- cation_probe
- anion_probe

### 6.3 APBS analysis

Input:
- prepared structure/fragment in APBS-compatible form
- charge/radius parameterization consistent within batch

Required outputs:
- electrostatic potential map file
- sampled potential at central site coordinate
- sampled field magnitude proxy at central site coordinate
- run metadata

Required parsed fields:
- fragment_id
- site_potential
- field_magnitude_proxy
- parameterization_id
- status

### 6.4 OpenMM local analysis

Input:
- local fragment or restrained local neighborhood
- assigned force field parameters

Required outputs:
- local steric strain proxy
- local vdW interaction proxy
- local electrostatic interaction proxy
- optional minimized energy delta if restrained minimization is used

Required parsed fields:
- fragment_id
- strain_proxy
- vdw_proxy
- electrostatic_proxy
- minimization_applied flag
- status

## 7. Exactly what the project code must generate for these jobs

Before any external job is run, the project code must generate:

1. Fragment coordinate files
2. Fragment metadata JSON
3. Analysis queue YAML
4. Batch manifest JSON
5. Probe placement definitions where relevant

The human operator should not manually assemble fragments by hand except for rare debugging cases.

## 8. Directory contract for external analysis

Expected structure:

external_analysis/
  orca/
    <batch_id>/
      inputs/
      outputs/
      parsed/
  apbs/
    <batch_id>/
      inputs/
      outputs/
      parsed/
  openmm/
    <batch_id>/
      inputs/
      outputs/
      parsed/

Each batch must include:
- batch_manifest.json
- job_status.jsonl
- parsed_results.parquet or parsed_results.jsonl

## 9. What to do when a fragment fails

If a fragment fails in ORCA/APBS/OpenMM:
- do not silently discard it
- record the failure
- keep its metadata
- mark it as unavailable_for_label_generation=true

The ingest pipeline must support partial batches.

## 10. What exact outputs are needed by the project

The project ultimately needs one normalized target table per analyzed site/archetype with these columns:

- fragment_id
- archetype_id
- motif_class
- refined_partial_charge
- electrostatic_potential
- electric_field_magnitude
- donor_strength
- acceptor_strength
- polarizability_proxy
- effective_steric_radius
- desolvation_penalty_proxy
- protonation_preference_score
- metal_binding_propensity
- aromatic_interaction_propensity
- local_environment_strain_score
- source_analysis_methods
- target_quality_flag
- provenance_json

All offline analysis results must eventually map into this schema.

## 11. Minimal initial execution order

Execute in this exact order:

1. generate environments from structures
2. cluster motif archetypes
3. export fragment jobs
4. run ORCA fragment jobs
5. run APBS jobs
6. run OpenMM jobs
7. parse raw outputs
8. normalize into target table
9. train surrogate model
10. apply surrogate to full corpus
