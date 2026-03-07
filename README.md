# bio-agent-lab

A Python library for classifying macromolecular structures from mmCIF files.

## What it does

`classify_structure` reads an mmCIF file and classifies it into one of four categories:

| Label | Meaning |
|---|---|
| `polymer_only` | One or more polymer entities, no small-molecule ligands |
| `protein_ligand` | A single protein entity with at least one non-polymer ligand |
| `multi_polymer_complex` | Two or more distinct polymer entities (e.g. protein + RNA) |
| `unknown` | No polymer entities detected |

It also returns intermediate flags (`has_protein`, `has_nucleic_acid`, `ligand_entities`, etc.) so callers can inspect the decision.

## Setup

```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

## Usage

```python
from bio_agent_lab.main import classify_structure

result = classify_structure("path/to/structure.cif")
print(result["classification"])   # e.g. "protein_ligand"
print(result["has_protein"])      # True
print(result["ligand_entities"])  # [{"id": "2", "entity_type": "non-polymer", "poly_subtype": None}]
```

Or from the command line:

```bash
python -m bio_agent_lab.main --cif path/to/structure.cif
```

## Output fields

| Field | Type | Description |
|---|---|---|
| `file` | str | Absolute path to the input file |
| `entities` | list | All entities: `{id, entity_type, poly_subtype}` |
| `polymer_entities` | list | Subset of `entities` where `entity_type == "polymer"` |
| `ligand_entities` | list | Subset of `entities` where `entity_type == "non-polymer"` |
| `n_polymer_entities` | int | Count of distinct polymer entity IDs |
| `distinct_poly_subtypes` | list | Sorted list of unique polymer subtypes present |
| `has_protein` | bool | Any `polypeptide(L)` or `polypeptide(D)` entity present |
| `has_nucleic_acid` | bool | Any RNA, DNA, or hybrid entity present |
| `classification` | str | Final label (see table above) |

## Running tests

```bash
pytest -q
```

## Biological limitations

- Homo-oligomers (one entity used in multiple chains) are classified as `polymer_only`, not `multi_polymer_complex`.
- Glycans (`_entity.type = "branched"`) are not counted as ligands.
- `water` entities are excluded from `ligand_entities` by the mmCIF format (they carry their own type).
- Polymer subtype detection covers standard PDB subtypes; non-standard or hybrid subtypes may fall through to `unknown`.
