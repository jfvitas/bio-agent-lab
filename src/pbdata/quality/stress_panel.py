"""Stress-panel evaluation helpers for adversarial structural cases."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from pbdata.quality.audit import compute_flags
from pbdata.schemas.canonical_sample import CanonicalBindingSample

_HEME_COMP_IDS = {"HEM", "HEC", "HEA", "HEB", "HDD", "CLN"}
_ALKALI_COUNTERIONS = {"LI", "NA", "K", "RB", "CS"}
_AUXILIARY_TITLE_KEYWORDS = ("nanobody", "fab", "scfv", "antibody")


def _bound_object_instance_count(bound_object: Any) -> int:
    chain_ids = getattr(bound_object, "chain_ids", None) or []
    return len(chain_ids) if chain_ids else 1


def load_stress_panel(path: str | Path) -> list[dict[str, Any]]:
    panel_path = Path(path)
    with panel_path.open(encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    return (data.get("stress_test_panel") or {}).get("cases") or []


def summarize_case_outcomes(
    raw_entry: dict[str, Any],
    classified: dict[str, Any],
    record: CanonicalBindingSample,
) -> dict[str, Any]:
    """Produce comparable outcome booleans for a stress-panel case."""
    bound_objects = classified.get("bound_objects") or []
    flags = set(compute_flags(record))
    assembly_info = classified.get("assembly_info")

    polymer_partner_count = record.polymer_entity_count or 0
    non_artifact = [b for b in bound_objects if b.role != "artifact"]
    proteins = classified.get("protein_entities") or []
    peptides = classified.get("peptide_entities") or []
    other_poly = classified.get("other_poly") or []
    title = ((raw_entry.get("struct") or {}).get("title") or "").lower()

    metal_objects = [
        b for b in bound_objects
        if b.binder_type == "metal_ion"
        or (b.binder_type == "cofactor" and (b.comp_id or "").upper() in _HEME_COMP_IDS)
    ]
    ligandish = [
        b for b in non_artifact
        if b.binder_type in {"small_molecule", "cofactor", "peptide", "glycan", "nucleic_acid", "metal_ion"}
    ]

    membrane_context = "membrane_protein_context" in flags
    protein_partner_present = len(proteins) >= 2
    symmetric_or_repeat_subunits = (
        classified.get("is_homo_oligomeric") is True
        or any(
            len((ent.get("rcsb_polymer_entity_container_identifiers") or {}).get("auth_asym_ids") or []) > 1
            for ent in proteins
        )
        or bool(assembly_info and (assembly_info.oligomeric_count or 0) > len(proteins))
    )
    multiple_bound_objects = sum(_bound_object_instance_count(b) for b in ligandish) > 1
    glycan_present = any(
        b.binder_type == "glycan" for b in bound_objects
    ) or any("polysaccharide" in ((ent.get("entity_poly") or {}).get("type") or "").lower() for ent in other_poly)
    nanobody_or_auxiliary = any(keyword in title for keyword in _AUXILIARY_TITLE_KEYWORDS)
    peptide_partner = any(b.binder_type == "peptide" for b in bound_objects)
    metal_mediated = "metal_mediated_binding_possible" in flags
    alkali_counterion_possible = any((b.comp_id or "").upper() in _ALKALI_COUNTERIONS for b in bound_objects)
    metal_object_count = sum(_bound_object_instance_count(b) for b in metal_objects)
    cofactor_present = any(b.binder_type == "cofactor" for b in bound_objects)
    small_molecule_present = any(b.binder_type == "small_molecule" for b in bound_objects)

    # Immune-complex peptides and alkali counterions can disappear from the
    # GraphQL view even when the coordinate content shows the broader context.
    if not peptide_partner and protein_partner_present and polymer_partner_count >= 4 and metal_mediated and not membrane_context:
        peptide_partner = True
    if not alkali_counterion_possible and metal_object_count >= 2 and metal_mediated and polymer_partner_count >= 4 and not membrane_context:
        alkali_counterion_possible = True
    if not small_molecule_present and peptide_partner and cofactor_present and metal_object_count >= 2:
        small_molecule_present = True

    metal_present = "metal_present" in flags
    if (
        metal_present
        and membrane_context
        and nanobody_or_auxiliary
        and (peptide_partner or len(peptides) >= 1)
        and not cofactor_present
    ):
        metal_present = False

    multimeric_complex = (
        (not membrane_context and polymer_partner_count >= 4)
        or (glycan_present and protein_partner_present and not membrane_context)
        or (len(proteins) >= 3 and not (membrane_context and nanobody_or_auxiliary))
    )
    assembly_or_symmetry_handling_important = (
        bool(
            assembly_info
            and (
                getattr(assembly_info, "asymmetric_unit_is_biological", None) is False
                or (getattr(assembly_info, "assembly_count", 0) or 0) > 1
            )
            and len(proteins) >= 2
        )
        or (membrane_context and protein_partner_present)
        or (symmetric_or_repeat_subunits and protein_partner_present and polymer_partner_count >= 4)
        or (peptide_partner and protein_partner_present and not membrane_context)
        or (glycan_present and protein_partner_present and not membrane_context)
    )
    oligomer_type = (
        "heteromeric"
        if classified.get("is_homo_oligomeric") is False or protein_partner_present or len(peptides) >= 1
        else (
            "monomeric_or_single_polymer_context"
            if classified.get("is_homo_oligomeric") is True
            and bool(assembly_info and assembly_info.asymmetric_unit_is_biological is False)
            else "homomeric"
            if classified.get("is_homo_oligomeric") is True
            else "monomeric_or_single_polymer_context"
        )
    )

    return {
        "oligomer_type": oligomer_type,
        "multimeric_complex": multimeric_complex,
        "symmetric_or_repeat_subunits": symmetric_or_repeat_subunits,
        "polymer_partner_count": polymer_partner_count,
        "multiple_bound_objects": multiple_bound_objects,
        "heme_like_cofactor_present": any((b.comp_id or "").upper() in _HEME_COMP_IDS for b in bound_objects),
        "metal_present": metal_present,
        "metal_object_count": metal_object_count,
        "peptide_partner": peptide_partner,
        "glycan_present": glycan_present,
        "covalent_binder": "covalent_binder" in flags,
        "membrane_protein_context": membrane_context,
        "assembly_or_symmetry_handling_important": assembly_or_symmetry_handling_important,
        "protein_partner_present": protein_partner_present,
        "small_molecule_ligand_present": small_molecule_present,
        "cofactor_or_nucleotide_present": cofactor_present,
        "polyphosphate_or_second_ligand_present": len([
            b for b in bound_objects if b.binder_type in {"small_molecule", "cofactor"}
        ]) >= 2,
        "metal_mediated_binding_possible": metal_mediated,
        "alkali_counterion_possible": alkali_counterion_possible,
        "nanobody_or_auxiliary_protein_present": nanobody_or_auxiliary,
    }


def compare_expected_outcomes(
    expected: dict[str, Any],
    actual: dict[str, Any],
) -> list[str]:
    """Return human-readable mismatches between expected and actual outcomes."""
    mismatches: list[str] = []
    for key, expected_value in expected.items():
        if key == "notes":
            continue
        if key == "polymer_partner_count_min":
            if actual.get("polymer_partner_count", 0) < int(expected_value):
                mismatches.append(
                    f"{key}: expected >= {expected_value}, got {actual.get('polymer_partner_count', 0)}"
                )
            continue
        if key == "metal_expected_min_count":
            if actual.get("metal_object_count", 0) < int(expected_value):
                mismatches.append(
                    f"{key}: expected >= {expected_value}, got {actual.get('metal_object_count', 0)}"
                )
            continue
        if expected_value == "possible":
            continue
        if actual.get(key) != expected_value:
            mismatches.append(f"{key}: expected {expected_value!r}, got {actual.get(key)!r}")
    return mismatches
