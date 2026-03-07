from pathlib import Path
import gemmi

def classify_structure(cif_path: str) -> dict:
    """
    Very simple starter classifier for mmCIF files.
    Returns a rough classification based on polymer / non-polymer presence.
    """
    path = Path(cif_path)
    if not path.exists():
        raise FileNotFoundError(f"Missing file: {cif_path}")

    doc = gemmi.cif.read_file(str(path))
    block = doc.sole_block()

    entity_types = []
    if block.find_mmcif_category("_entity."):
        ids = block.find_values("_entity.id")
        types = block.find_values("_entity.type")
        entity_types = list(zip(ids, types))

    has_polymer = any(t == "polymer" for _, t in entity_types)
    has_nonpolymer = any(t == "non-polymer" for _, t in entity_types)

    if has_polymer and has_nonpolymer:
        kind = "protein_or_nucleic_acid_plus_ligand_or_other_nonpolymer"
    elif has_polymer:
        kind = "polymer_only"
    elif has_nonpolymer:
        kind = "nonpolymer_only"
    else:
        kind = "unknown"

    return {
        "file": str(path),
        "entity_types": entity_types,
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
