from pathlib import Path
from typing import Any

import gemmi


def _clean(value) -> str:
    return gemmi.cif.as_string(value).strip()


def _norm(value: str) -> str:
    return value.lower().strip()


_EXCLUDED_NON_POLYMER_COMP_IDS = {
    "HOH",
    "DOD",
    "NA",
    "K",
    "CL",
    "CA",
    "MG",
    "MN",
    "ZN",
    "FE",
    "CU",
    "CO",
    "NI",
    "CD",
    "IOD",
    "BR",
    "SO4",
    "PO4",
    "NO3",
    "GOL",
    "EDO",
    "PEG",
    "PG4",
    "MPD",
    "MES",
    "HEP",
    "TRS",
    "BME",
    "DTT",
    "ACE",
    "ACT",
}


def _read_nonpoly_comp_ids(block: gemmi.cif.Block) -> dict[str, str]:
    """Map non-polymer entity IDs to component IDs when present."""
    comp_ids: dict[str, str] = {}
    if not block.find_mmcif_category("_pdbx_entity_nonpoly."):
        return comp_ids

    entity_ids = block.find_values("_pdbx_entity_nonpoly.entity_id")
    raw_comp_ids = block.find_values("_pdbx_entity_nonpoly.comp_id")
    if len(entity_ids) != len(raw_comp_ids):
        raise ValueError(
            "Mismatched _pdbx_entity_nonpoly.entity_id and _pdbx_entity_nonpoly.comp_id lengths"
        )

    for entity_id, comp_id in zip(entity_ids, raw_comp_ids):
        comp_ids[_clean(entity_id)] = _clean(comp_id)
    return comp_ids


def _is_biologically_relevant_ligand(entity: dict[str, str | None], comp_ids: dict[str, str]) -> bool:
    """Conservatively identify likely ligand entities.

    Non-polymers without a component ID are treated as unknown rather than
    assumed ligands, which avoids obvious false positives for solvents/ions.
    """
    if _norm(entity["entity_type"] or "") != "non-polymer":
        return False
    comp_id = comp_ids.get(entity["id"] or "")
    if comp_id is None:
        return False
    return comp_id.upper() not in _EXCLUDED_NON_POLYMER_COMP_IDS


def classify_structure(cif_path: str) -> dict[str, Any]:
    """
    Classify an mmCIF structure into one of:
      multi_polymer_complex, protein_ligand, polymer_only, unknown.
    Returns a dict with all intermediate flags for caller inspection.
    """
    path = Path(cif_path)
    if not path.exists():
        raise FileNotFoundError(f"Missing file: {cif_path}")
    if not path.is_file():
        raise IsADirectoryError(f"Expected file path, got non-file path: {cif_path}")

    doc = gemmi.cif.read_file(str(path))
    block = doc.sole_block()

    poly_subtypes = {}
    if block.find_mmcif_category("_entity_poly."):
        poly_ids = block.find_values("_entity_poly.entity_id")
        poly_type_vals = block.find_values("_entity_poly.type")
        if len(poly_ids) != len(poly_type_vals):
            raise ValueError("Mismatched _entity_poly.entity_id and _entity_poly.type lengths")
        for eid, ptype in zip(poly_ids, poly_type_vals):
            poly_subtypes[_clean(eid)] = _clean(ptype)

    entities = []
    nonpoly_comp_ids = _read_nonpoly_comp_ids(block)
    if block.find_mmcif_category("_entity."):
        ids = block.find_values("_entity.id")
        types = block.find_values("_entity.type")
        if len(ids) != len(types):
            raise ValueError("Mismatched _entity.id and _entity.type lengths")
        for eid, etype in zip(ids, types):
            eid = _clean(eid)
            entities.append({
                "id": eid,
                "entity_type": _clean(etype),
                "poly_subtype": poly_subtypes.get(eid),
            })

    polymer_entities = [e for e in entities if _norm(e["entity_type"]) == "polymer"]
    ligand_entities = [
        e for e in entities if _is_biologically_relevant_ligand(e, nonpoly_comp_ids)
    ]
    excluded_nonpoly_entities = [
        e
        for e in entities
        if _norm(e["entity_type"]) == "non-polymer" and not _is_biologically_relevant_ligand(e, nonpoly_comp_ids)
    ]
    n_polymer_entities = len(polymer_entities)
    distinct_poly_subtypes = {e["poly_subtype"] for e in polymer_entities if e["poly_subtype"]}
    has_protein = any(
        _norm(e["poly_subtype"]) in {"polypeptide(l)", "polypeptide(d)"}
        for e in polymer_entities
        if e["poly_subtype"]
    )
    has_nucleic_acid = any(
        _norm(e["poly_subtype"]) in {
            "polyribonucleotide",
            "polydeoxyribonucleotide",
            "polydeoxyribonucleotide/polyribonucleotide hybrid",
        }
        for e in polymer_entities
        if e["poly_subtype"]
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
        "excluded_nonpoly_entities": excluded_nonpoly_entities,
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
