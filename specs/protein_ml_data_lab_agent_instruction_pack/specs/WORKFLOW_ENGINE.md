# Workflow Engine Specification

The workflow engine coordinates execution of all modules.

## Steps

1. Workspace Setup
2. Protein Search
3. Metadata Harvest
4. Structure Download
5. Feature Extraction
6. Graph Generation
7. Dataset Engineering

Each step produces artifacts stored in workspace directories.

## Workspace Layout

workspace_root/

data_sources/
structures/
clean_structures/
features/
graphs/
datasets/
metadata/
logs/
rosetta_outputs/

## Reproducibility

Each dataset export must contain:

dataset_config.yaml  
feature_schema.json  
graph_config.json