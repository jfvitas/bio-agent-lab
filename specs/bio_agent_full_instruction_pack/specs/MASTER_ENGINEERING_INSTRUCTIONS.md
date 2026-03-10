
# BIO-AGENT-LAB MASTER ENGINEERING SPECIFICATION
Version: 1.0
Status: Authoritative
Applies to: All coding agents, QA agents, and reviewers

---

# 1. PROJECT OBJECTIVE

Bio-Agent-Lab is a multi‑modal biological interaction prediction platform.

The system predicts:

• ligand → protein binding
• peptide/protein → binding partners
• off-target interactions
• pathway activation impacts
• adverse-effect risk

The platform integrates:

• structural biology
• ligand chemistry
• biological networks
• pathway databases
• experimental binding data

The system must support both dataset construction and prediction workflows.

---

# 2. CORE ARCHITECTURE

Strict layered architecture is required.

data_pipeline/
  ingestion/
  normalization/
  extraction/

dataset/
  canonical_records/
  quality_scoring/
  dataset_audit/
  split_builder/

features/
  structural_features/
  ligand_features/
  interface_features/
  graph_features/

graph/
  interaction_graph/
  pathway_graph/

models/
  affinity_models/
  off_target_models/

prediction/
  ligand_screening/
  peptide_binding/
  variant_effects/

risk/
  pathway_reasoning/
  severity_scoring/

qa/
  deterministic_tests/
  scenario_tests/
  stress_panels/

Agents must never mix responsibilities across layers.

---

# 3. DATA INGESTION

Supported data sources:

Structural sources
- RCSB PDB
- UniProt

Binding datasets
- BindingDB
- PDBbind
- ChEMBL
- BioLiP
- SKEMPI

Interaction network sources (~35)
- STRING
- BioGRID
- IntAct
- Reactome
- KEGG
- Pathway Commons
- WikiPathways
- DIP
- MINT
- HPRD
- SIGNOR
- OmniPath
- CORUM
- PhosphoSitePlus
- Human Protein Atlas
- GTEx
- OpenTargets
- DisGeNET

Raw files must always be stored in:

data/raw/

Raw data must never be overwritten.

---

# 4. STRUCTURE FILE PRIORITY

Preferred order:

1. mmCIF
2. BinaryCIF
3. PDB (fallback)

mmCIF must always be preferred because it preserves complete metadata.

---

# 5. CANONICAL DATASET LAYER

All ingested data must normalize into canonical schema.

Canonical entities:

entry
chain
bound_object
interface
assay
provenance

Schema definition:

specs/canonical_schema.yaml

Schema changes must increment schema_version.

---

# 6. PROVENANCE TRACKING

Every extracted field must include provenance.

Example:

provenance:
  source: BindingDB
  source_record_id: BDB123456
  retrieved_at: 2026‑03‑01
  confidence: high

Agents must never remove provenance metadata.

---

# 7. FEATURE ENGINEERING

Feature generation must be separate from ingestion.

Protein features
- residue embeddings (ESM)
- pocket geometry
- surface accessibility
- electrostatics
- flexibility

Ligand features
- atom graph
- Morgan fingerprints
- partial charges
- chemical descriptors

Interface features
- hydrogen bonds
- salt bridges
- contact density
- buried surface area

Graph features
- centrality metrics
- pathway membership
- interaction clusters

Outputs stored in:

features/

---

# 8. CONFORMATIONAL STATE MODELING

Proteins must support multiple structural states.

State sources:
- experimental PDB structures
- AlphaFold predictions
- Rosetta models

State attributes:

target_id
state_id
structure_source
apo_or_holo
active_inactive
open_closed
conformation_cluster

Predictions must consider all available states.

---

# 9. PREDICTION PIPELINE

Accepted inputs:

SMILES
SDF
PDB
mmCIF
FASTA

If only sequence exists, generate structure using AlphaFold.

Pipeline:

1. normalize input
2. generate ligand features
3. generate protein features
4. search candidate targets
5. predict affinity
6. rank targets

Outputs:

predicted KD
predicted ΔG
binding probability
confidence score

---

# 10. OFF‑TARGET ANALYSIS

Ligands must be screened against all proteins in the dataset.

Results must include:

- ranked off‑target list
- predicted affinity
- prediction confidence

---

# 11. PATHWAY REASONING

Databases used:

Reactome
KEGG
Pathway Commons
WikiPathways

Steps:

1 identify pathways containing targets
2 detect pathway overlap
3 evaluate pathway activation risk

Outputs:

pathway activation probability
pathway conflict score

---

# 12. RISK SCORING

Risk score components:

binding strength
pathway overlap
target essentiality
tissue expression
toxicity history

Example formula:

risk_score =
(binding_weight * predicted_affinity)
+ (pathway_overlap_weight * pathway_similarity)
+ (expression_weight)
+ (toxicity_weight)

Severity levels:

low
moderate
high
critical

---

# 13. PERFORMANCE OPTIMIZATION

Allowed performance improvements:

parallel processing
batch processing
caching expensive calculations
lazy loading
GPU acceleration
memory mapped datasets
Parquet serialization

Agents must never remove validation or provenance tracking for speed.

---

# 14. QA SYSTEM

QA requires three layers:

Deterministic tests
Scenario tests
Stress tests

Deterministic tests verify:

schema validity
parser correctness
feature generation
data merges

Scenario tests simulate user workflows.

Stress tests include:

large complexes
metals
glycans
multi‑ligand systems
missing metadata

Stress panels must never be modified by coding agents.

---

# 15. TESTING AGENT ROLE

Testing agents simulate real users.

Responsibilities:

execute scenario tests
interact with CLI or GUI
inspect outputs
identify usability issues

Testing agents must flag:

missing outputs
ambiguous results
incorrect ranking
confusing workflows

Reports must include:

scenario_id
goal
observed_behavior
expected_behavior
severity

---

# 16. DATASET BIAS AUDIT

Dataset bias must be automatically reported.

Metrics:

protein family distribution
ligand scaffold diversity
organism distribution
resolution distribution
experimental method distribution

Command example:

pbdata report-bias

---

# 17. CODE QUALITY RULES

Coding agents must follow:

no duplicated logic
no dead code
no silent failures
no schema violations

All modules require:

docstrings
type hints
unit tests

---

# 18. MERGE REQUIREMENTS

Code may be merged only if it passes:

deterministic tests
scenario tests
stress tests
QA review

---

# 19. FINAL PRINCIPLE

The system must prefer:

explicit uncertainty

over

incorrect certainty.

Scientific correctness has highest priority.
