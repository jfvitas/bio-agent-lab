# HUMAN EXECUTION PLAYBOOK
Version: 1.0

This file tells the human operator exactly what to do separately from normal project code.

## A. What you do outside the project code

You personally do these things:

1. Decide the structure corpus to extract environments from
2. Run the exported offline ORCA/APBS/OpenMM jobs
3. Inspect only serious failures
4. Place result folders back into the expected directory
5. Trigger the ingest pipeline

That is all.

You should not manually:
- hand-edit target values
- hand-map atom IDs
- manually build training tables
- manually rewrite model inputs

Those tasks belong in project code.

## B. Your concrete execution order

### Step 1
Run project code to build the environment corpus and archetype queue.

Expected project outputs:
- site environment tables
- motif cluster assignments
- representative archetype fragment exports
- analysis queue YAML files

### Step 2
Take the exported analysis batches and run them externally.

#### 2A ORCA
Run ORCA on all fragment jobs.

#### 2B APBS
Run APBS electrostatics jobs on the same fragment batch or compatible prepared structures.

#### 2C OpenMM
Run local MM/strain/proxy jobs.

### Step 3
Collect results into the expected batch folders.

### Step 4
Run project ingest command or ingest module.

Expected outputs:
- normalized physics target table
- failed fragment report
- training-ready surrogate labels

### Step 5
Hand those stable artifact paths back to the coding agents.

## C. How you know what to analyze

Do not manually invent combinations.

Use the project-generated archetype queue.
That queue is the authoritative list of environments to analyze.

If the queue is too large:
- start with one batch of motif classes
- complete that batch fully
- ingest and train surrogate v1
- expand later

## D. What to tell the coding agents after your external runs

Give them:

1. the path to `physics_targets.parquet`
2. the path to `physics_target_manifest.json`
3. the path to `failed_fragments.parquet`
4. the batch summary
5. whether this is a partial or full motif-class batch

Example handoff:

- batch_id: physics_batch_001
- physics_targets: artifacts/physics_targets/physics_batch_001/physics_targets.parquet
- manifest: artifacts/physics_targets/physics_batch_001/physics_target_manifest.json
- failed: artifacts/physics_targets/physics_batch_001/failed_fragments.parquet
- status: ready_for_surrogate_training

Then tell the agents:
"Build surrogate training and inference against these artifacts only. Do not parse raw external outputs directly."

## E. Version 1 practical rollout

Do this first:

### Batch 1 motifs
- backbone_carbonyl_oxygen
- backbone_amide_nitrogen
- asp_carboxylate_oxygen
- glu_carboxylate_oxygen
- lys_terminal_amine_nitrogen
- his_delta_nitrogen
- his_epsilon_nitrogen
- ser_hydroxyl_oxygen
- tyr_hydroxyl_oxygen
- carbonyl_oxygen
- carboxylate_oxygen
- amine_nitrogen
- aromatic_nitrogen
- aromatic_centroid
- metal_ion

Target archetypes:
- 20 archetypes per motif class minimum
- 40 preferred

Do not try to analyze the entire chemistry universe first.
Get batch 1 working end-to-end.
