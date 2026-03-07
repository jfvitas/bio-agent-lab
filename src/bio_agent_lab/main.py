from pathlib import Path
import gemmi

def classify_structure(cif_path: str) -> dict:
    """
    Classify an mmCIF structure into one of:
      multi_polymer_complex, protein_ligand, polymer_only, unknown.
    Returns a dict with all intermediate flags for caller inspection.
    """
    path = Path(cif_path)
    if not path.exists():
        raise FileNotFoundError(f"Missing file: {cif_path}")

    doc = gemmi.cif.read_file(str(path))
    block = doc.sole_block()

    poly_subtypes = {}
    if block.find_mmcif_category("_entity_poly."):
        poly_ids = block.find_values("_entity_poly.entity_id")
        poly_type_vals = block.find_values("_entity_poly.type")
        for eid, ptype in zip(poly_ids, poly_type_vals):
            poly_subtypes[gemmi.cif.as_string(eid)] = gemmi.cif.as_string(ptype)

    entities = []
    if block.find_mmcif_category("_entity."):
        ids = block.find_values("_entity.id")
        types = block.find_values("_entity.type")
        for eid, etype in zip(ids, types):
            eid = gemmi.cif.as_string(eid)
            entities.append({
                "id": eid,
                "entity_type": gemmi.cif.as_string(etype),
                "poly_subtype": poly_subtypes.get(eid),
            })

    polymer_entities = [e for e in entities if e["entity_type"] == "polymer"]
    ligand_entities  = [e for e in entities if e["entity_type"] == "non-polymer"]
    n_polymer_entities = len(polymer_entities)
    distinct_poly_subtypes = {e["poly_subtype"] for e in polymer_entities if e["poly_subtype"]}
    has_protein = any(
        e["poly_subtype"] in {"polypeptide(L)", "polypeptide(D)"}
        for e in polymer_entities
    )
    has_nucleic_acid = any(
        e["poly_subtype"] in {
            "polyribonucleotide",
            "polydeoxyribonucleotide",
            "polydeoxyribonucleotide/polyribonucleotide hybrid",
        }
        for e in polymer_entities
    )
    if n_polymer_entities > 1:
        kind = "multi_polymer_complex"
    elif has_protein and ligand_entities:
        kind = "protein_ligand"
    elif n_polymer_entities >= 1 and not ligand_entities:
        kind = "polymer_only"
    else:
        kind = "unknown"

    return {
        "file": str(path),
        "entities": entities,
        "polymer_entities": polymer_entities,
        "n_polymer_entities": n_polymer_entities,
        "distinct_poly_subtypes": sorted(distinct_poly_subtypes),
        "has_protein": has_protein,
        "has_nucleic_acid": has_nucleic_acid,
        "ligand_entities": ligand_entities,
        "classification": kind,
    }

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--cif", required=True, help="Path to mmCIF file")
    args = parser.parse_args()

    result = classify_structure(args.cif)
    print(result)

if __name__ == "__main__":
    main()
