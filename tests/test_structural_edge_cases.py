"""Structural edge-case tests for the RCSB parser and normalizer.

Two test groups:

  Unit tests  (no network, always run)
  ─────────────────────────────────────
  Use hand-crafted minimal GraphQL dicts that represent key edge cases.
  Tests are deterministic: they call classify_entry() or normalize_record()
  directly and assert on the returned classification / flag fields.

  Integration tests  (@pytest.mark.integration, require network)
  ──────────────────────────────────────────────────────────────
  Load the stress-test panel from stress_test_panel.yaml, fetch live
  RCSB data, normalize, and assert minimum expected properties.
  Run with:  pytest -m integration tests/test_structural_edge_cases.py
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest
import yaml

from pbdata.quality.audit import compute_flags
from pbdata.schemas.canonical_sample import CanonicalBindingSample
from pbdata.sources.rcsb import RCSBAdapter
from pbdata.sources.rcsb_classify import (
    PEPTIDE_MAX_RESIDUES,
    build_bound_objects,
    build_interfaces,
    classify_entry,
    classify_nonpolymer_entity,
    classify_polymer_entity,
    detect_membrane_context,
    has_covalent_warhead,
    infer_oligomeric_state,
)

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

_REPO_ROOT = Path(__file__).parent.parent
_PANEL_PATH = _REPO_ROOT / "stress_test_panel.yaml"

# ---------------------------------------------------------------------------
# Shared provenance for unit-test records
# ---------------------------------------------------------------------------

_PROV = {"ingested_at": datetime.now(timezone.utc).isoformat(), "source_database": "RCSB"}


# ===========================================================================
# UNIT TESTS — classify_polymer_entity
# ===========================================================================

class TestClassifyPolymerEntity:

    def _make_poly(self, poly_type: str, seq: str) -> dict[str, Any]:
        return {"entity_poly": {"type": poly_type, "pdbx_seq_one_letter_code_can": seq}}

    def test_long_polypeptide_is_protein(self):
        e = self._make_poly("polypeptide(L)", "A" * 100)
        assert classify_polymer_entity(e) == "protein"

    def test_short_polypeptide_is_peptide(self):
        e = self._make_poly("polypeptide(L)", "AKLVFGAST")  # 9 residues
        assert classify_polymer_entity(e) == "peptide"

    def test_exactly_at_threshold_is_peptide(self):
        e = self._make_poly("polypeptide(L)", "A" * PEPTIDE_MAX_RESIDUES)
        assert classify_polymer_entity(e) == "peptide"

    def test_one_over_threshold_is_protein(self):
        e = self._make_poly("polypeptide(L)", "A" * (PEPTIDE_MAX_RESIDUES + 1))
        assert classify_polymer_entity(e) == "protein"

    def test_d_polypeptide_classified(self):
        e = self._make_poly("polypeptide(D)", "A" * 50)
        assert classify_polymer_entity(e) == "protein"

    def test_rna_is_nucleic_acid(self):
        e = self._make_poly("polyribonucleotide", "AUGC")
        assert classify_polymer_entity(e) == "nucleic_acid"

    def test_polysaccharide_is_glycan(self):
        e = self._make_poly("polysaccharide(D)", "NAG")
        assert classify_polymer_entity(e) == "glycan"

    def test_missing_sequence_defaults_to_protein(self):
        e = {"entity_poly": {"type": "polypeptide(L)"}}
        # No sequence → None → not <= threshold → protein
        assert classify_polymer_entity(e) == "protein"


# ===========================================================================
# UNIT TESTS — classify_nonpolymer_entity
# ===========================================================================

class TestClassifyNonpolymerEntity:

    def _make_np(self, comp_id: str) -> dict[str, Any]:
        return {"nonpolymer_comp": {"chem_comp": {"id": comp_id, "name": "test"}}}

    def test_zinc_is_metal(self):
        btype, role, _ = classify_nonpolymer_entity(self._make_np("ZN"))
        assert btype == "metal_ion"
        assert role == "structural_ion"

    def test_iron_variant_is_metal(self):
        btype, _, _ = classify_nonpolymer_entity(self._make_np("FE2"))
        assert btype == "metal_ion"

    def test_atp_is_cofactor(self):
        btype, role, _ = classify_nonpolymer_entity(self._make_np("ATP"))
        assert btype == "cofactor"
        assert role == "cofactor"

    def test_heme_is_cofactor(self):
        btype, role, _ = classify_nonpolymer_entity(self._make_np("HEM"))
        assert btype == "cofactor"

    def test_nag_is_glycan(self):
        btype, _, _ = classify_nonpolymer_entity(self._make_np("NAG"))
        assert btype == "glycan"

    def test_water_is_additive(self):
        btype, role, _ = classify_nonpolymer_entity(self._make_np("HOH"))
        assert btype == "additive"
        assert role == "artifact"

    def test_sulfate_is_additive(self):
        btype, _, _ = classify_nonpolymer_entity(self._make_np("SO4"))
        assert btype == "additive"

    def test_unknown_organic_is_small_molecule(self):
        btype, role, _ = classify_nonpolymer_entity(self._make_np("XYZ"))
        assert btype == "small_molecule"
        assert role == "primary_ligand"


# ===========================================================================
# UNIT TESTS — infer_oligomeric_state
# ===========================================================================

def _prot_entity(chains: list[str], entity_id: str = "E1") -> dict[str, Any]:
    return {
        "rcsb_id": entity_id,
        "entity_poly": {"type": "polypeptide(L)", "pdbx_seq_one_letter_code_can": "A" * 100},
        "rcsb_polymer_entity_container_identifiers": {"auth_asym_ids": chains},
    }


class TestInferOligomericState:

    def test_no_entities(self):
        is_homo, state = infer_oligomeric_state([])
        assert is_homo is None
        assert state == "no_protein"

    def test_single_chain_is_monomer(self):
        is_homo, state = infer_oligomeric_state([_prot_entity(["A"])])
        assert is_homo is None
        assert state == "monomer"

    def test_two_same_entity_chains_is_homodimer(self):
        is_homo, state = infer_oligomeric_state([_prot_entity(["A", "B"])])
        assert is_homo is True
        assert state == "homodimer"

    def test_four_same_entity_chains_is_homotetramer(self):
        is_homo, state = infer_oligomeric_state([_prot_entity(["A", "B", "C", "D"])])
        assert is_homo is True
        assert state == "homotetramer"

    def test_two_distinct_entities_is_heterodimer(self):
        entities = [_prot_entity(["A"], "E1"), _prot_entity(["B"], "E2")]
        is_homo, state = infer_oligomeric_state(entities)
        assert is_homo is False
        assert state == "heterodimer"

    def test_three_entities_is_hetero_complex(self):
        entities = [
            _prot_entity(["A"], "E1"),
            _prot_entity(["B"], "E2"),
            _prot_entity(["C"], "E3"),
        ]
        is_homo, state = infer_oligomeric_state(entities)
        assert is_homo is False
        assert "3" in state


# ===========================================================================
# UNIT TESTS — has_covalent_warhead
# ===========================================================================

class TestCovalentWarhead:

    def test_acrylamide_detected(self):
        # ibrutinib-like: acrylamide Michael acceptor
        assert has_covalent_warhead("C=CC(=O)Nc1ccccc1")

    def test_acrylate_detected(self):
        assert has_covalent_warhead("C=CC(=O)Oc1ccccc1")

    def test_epoxide_detected(self):
        assert has_covalent_warhead("CC1CO1")

    def test_non_covalent_not_detected(self):
        # Ibuprofen — no reactive group
        assert not has_covalent_warhead("CC(C)Cc1ccc(cc1)C(C)C(=O)O")

    def test_atp_smiles_not_detected(self):
        # ATP SMILES — should not be flagged
        atp = "Nc1ncnc2c1ncn2[C@@H]1O[C@H](COP(=O)(O)OP(=O)(O)OP(=O)(O)O)[C@@H](O)[C@H]1O"
        assert not has_covalent_warhead(atp)


# ===========================================================================
# UNIT TESTS — detect_membrane_context
# ===========================================================================

class TestMembraneContext:

    def _entry(self, pdbx_kw: str = "", text: str = "") -> dict[str, Any]:
        return {"struct_keywords": {"pdbx_keywords": pdbx_kw, "text": text}}

    def test_gpcr_keyword_detected(self):
        assert detect_membrane_context(self._entry(pdbx_kw="GPCR")) is True

    def test_membrane_protein_detected(self):
        assert detect_membrane_context(self._entry(text="membrane protein complex")) is True

    def test_transmembrane_detected(self):
        assert detect_membrane_context(self._entry(pdbx_kw="TRANSMEMBRANE")) is True

    def test_soluble_not_detected(self):
        assert detect_membrane_context(self._entry(pdbx_kw="HYDROLASE")) is False

    def test_missing_keywords_field(self):
        assert detect_membrane_context({}) is False


# ===========================================================================
# UNIT TESTS — build_bound_objects
# ===========================================================================

def _np_entity(comp_id: str, name: str = "", chains: list[str] | None = None) -> dict[str, Any]:
    e: dict[str, Any] = {
        "rcsb_id": f"X_{comp_id}",
        "nonpolymer_comp": {"chem_comp": {"id": comp_id, "name": name}},
    }
    if chains is not None:
        e["rcsb_nonpolymer_entity_container_identifiers"] = {"auth_asym_ids": chains}
    return e


class TestBuildBoundObjects:

    def test_metal_classified(self):
        objs = build_bound_objects([_np_entity("ZN")], [])
        assert len(objs) == 1
        assert objs[0].binder_type == "metal_ion"

    def test_cofactor_classified(self):
        objs = build_bound_objects([_np_entity("ATP")], [])
        assert objs[0].binder_type == "cofactor"

    def test_small_molecule_classified(self):
        objs = build_bound_objects([_np_entity("ABC")], [])
        assert objs[0].binder_type == "small_molecule"

    def test_additive_classified(self):
        objs = build_bound_objects([_np_entity("GOL")], [])
        assert objs[0].binder_type == "additive"

    def test_peptide_from_polymer(self):
        pep = _prot_entity(["P"], "PEP_E")
        pep["entity_poly"]["pdbx_seq_one_letter_code_can"] = "AKLVFGAST"  # 9 aa
        objs = build_bound_objects([], [pep])
        assert len(objs) == 1
        assert objs[0].binder_type == "peptide"
        assert objs[0].residue_count == 9

    def test_long_protein_not_included(self):
        long_prot = _prot_entity(["A"], "PROT_E")
        objs = build_bound_objects([], [long_prot])
        # Full protein should NOT appear in bound_objects
        assert len(objs) == 0

    def test_multiple_objects_all_retained(self):
        entities = [_np_entity("ZN"), _np_entity("HEM"), _np_entity("XYZ"), _np_entity("GOL")]
        objs = build_bound_objects(entities, [])
        assert len(objs) == 4
        types = {o.binder_type for o in objs}
        assert "metal_ion" in types
        assert "cofactor" in types
        assert "small_molecule" in types
        assert "additive" in types

    def test_covalent_warhead_smiles_flagged(self):
        objs = build_bound_objects(
            [_np_entity("XXX")],
            [],
            chem_descriptors={"XXX": {"SMILES_CANONICAL": "C=CC(=O)Nc1ccccc1"}},
        )
        assert objs[0].covalent_warhead_flag is True
        assert objs[0].is_covalent is True


# ===========================================================================
# UNIT TESTS — build_interfaces
# ===========================================================================

class TestBuildInterfaces:

    def test_homomeric_produces_symmetric_interface(self):
        ent = _prot_entity(["A", "B"])
        ifaces = build_interfaces([ent], [])
        assert len(ifaces) == 1
        assert ifaces[0].is_symmetric is True
        assert ifaces[0].interface_type == "protein_protein"

    def test_heterodimer_produces_hetero_interface(self):
        ifaces = build_interfaces(
            [_prot_entity(["A"], "E1"), _prot_entity(["B"], "E2")], []
        )
        assert len(ifaces) == 1
        assert ifaces[0].is_hetero is True
        assert ifaces[0].is_symmetric is False

    def test_trimer_produces_three_interfaces(self):
        entities = [
            _prot_entity(["A"], "E1"),
            _prot_entity(["B"], "E2"),
            _prot_entity(["C"], "E3"),
        ]
        ifaces = build_interfaces(entities, [])
        assert len(ifaces) == 3

    def test_protein_peptide_interface(self):
        prot = _prot_entity(["A"], "E1")
        pep_ent = _prot_entity(["P"], "PEP")
        pep_ent["entity_poly"]["pdbx_seq_one_letter_code_can"] = "AKLVFGAST"
        ifaces = build_interfaces([prot], [pep_ent])
        pp_ifaces = [i for i in ifaces if i.interface_type == "protein_peptide"]
        assert len(pp_ifaces) == 1
        assert pp_ifaces[0].is_hetero is True

    def test_monomer_no_interface(self):
        ifaces = build_interfaces([_prot_entity(["A"])], [])
        assert len(ifaces) == 0


# ===========================================================================
# UNIT TESTS — classify_entry (full pipeline on mock dicts)
# ===========================================================================

def _make_entry(
    poly_entities: list[dict],
    nonpoly_entities: list[dict] | None = None,
    struct_keywords: dict | None = None,
    assemblies: list[dict] | None = None,
    assembly_count: int | None = None,
) -> dict[str, Any]:
    entry: dict[str, Any] = {
        "rcsb_id": "MOCK",
        "polymer_entities": poly_entities,
        "nonpolymer_entities": nonpoly_entities or [],
    }
    if struct_keywords:
        entry["struct_keywords"] = struct_keywords
    if assemblies is not None or assembly_count is not None:
        entry["rcsb_entry_info"] = {"assembly_count": assembly_count}
        if assemblies is not None:
            entry["assemblies"] = assemblies
    return entry


class TestClassifyEntry:

    def test_homodimer_with_ligand(self):
        ent = _prot_entity(["A", "B"])
        result = classify_entry(_make_entry(
            [ent],
            [_np_entity("XYZ")],
        ))
        assert result["is_homo_oligomeric"] is True
        assert result["oligomeric_state"] == "homodimer"
        assert len(result["interfaces"]) == 1
        assert result["interfaces"][0].is_symmetric is True
        sm = [b for b in result["bound_objects"] if b.binder_type == "small_molecule"]
        assert len(sm) == 1

    def test_heterodimer_with_peptide(self):
        prot = _prot_entity(["A"], "E1")
        pep  = _prot_entity(["P"], "E2")
        pep["entity_poly"]["pdbx_seq_one_letter_code_can"] = "AKLVFGAST"
        result = classify_entry(_make_entry([prot, pep]))
        # Peptide goes to bound_objects, not to protein_entities —
        # so infer_oligomeric_state sees only 1 protein entity (monomer).
        # The key assertion is that the peptide partner is detected.
        pep_objs = [b for b in result["bound_objects"] if b.binder_type == "peptide"]
        assert len(pep_objs) == 1
        assert result["peptide_entities"]  # peptide entity was routed correctly

    def test_metal_and_cofactor_separate(self):
        result = classify_entry(_make_entry(
            [_prot_entity(["A"])],
            [_np_entity("ZN"), _np_entity("HEM"), _np_entity("LIG")],
        ))
        btypes = {b.binder_type for b in result["bound_objects"]}
        assert "metal_ion" in btypes
        assert "cofactor" in btypes
        assert "small_molecule" in btypes

    def test_membrane_context_flagged(self):
        result = classify_entry(_make_entry(
            [_prot_entity(["A"])],
            struct_keywords={"pdbx_keywords": "GPCR MEMBRANE PROTEIN", "text": ""},
        ))
        assert result["membrane_context"] is True

    def test_assembly_info_extracted(self):
        result = classify_entry(_make_entry(
            [_prot_entity(["A"])],
            assemblies=[{
                "rcsb_id": "MOCK-1",
                "pdbx_struct_assembly": {
                    "oligomeric_details": "HOMO 2-MER",
                    "oligomeric_count": 2,
                },
                "rcsb_assembly_info": {"polymer_entity_count": 1, "polymer_entity_count_protein": 1},
            }],
            assembly_count=1,
        ))
        ai = result["assembly_info"]
        assert ai.oligomeric_details == "HOMO 2-MER"
        assert ai.is_homo_oligomeric is True
        assert ai.assembly_count == 1


# ===========================================================================
# UNIT TESTS — audit flags (compute_flags on synthetic records)
# ===========================================================================

def _make_record(**kwargs: Any) -> CanonicalBindingSample:
    defaults: dict[str, Any] = {
        "sample_id": "TEST_RECORD",
        "task_type": "protein_ligand",
        "source_database": "RCSB",
        "source_record_id": "TEST",
        "provenance": _PROV,
        "quality_flags": [],
        "quality_score": 0.0,
    }
    defaults.update(kwargs)
    return CanonicalBindingSample(**defaults)


class TestComputeFlags:

    def test_no_resolution_flagged(self):
        r = _make_record()
        assert "no_resolution" in compute_flags(r)

    def test_low_resolution_flagged(self):
        r = _make_record(structure_resolution=4.0)
        assert "low_resolution" in compute_flags(r)

    def test_very_low_resolution_flagged(self):
        r = _make_record(structure_resolution=5.0)
        assert "very_low_resolution" in compute_flags(r)

    def test_metal_present_from_bound_objects(self):
        r = _make_record(
            bound_objects=[{"binder_type": "metal_ion", "role": "structural_ion", "comp_id": "ZN"}],
        )
        assert "metal_present" in compute_flags(r)

    def test_metal_present_from_metallo_cofactor(self):
        r = _make_record(
            bound_objects=[{"binder_type": "cofactor", "role": "cofactor", "comp_id": "HEM"}],
        )
        assert "metal_present" in compute_flags(r)

    def test_cofactor_flag(self):
        r = _make_record(
            bound_objects=[{"binder_type": "cofactor", "role": "cofactor", "comp_id": "ATP"}],
        )
        assert "cofactor_present" in compute_flags(r)

    def test_glycan_flag(self):
        r = _make_record(
            bound_objects=[{"binder_type": "glycan", "role": "unknown", "comp_id": "NAG"}],
        )
        assert "glycan_present" in compute_flags(r)

    def test_covalent_binder_flag(self):
        r = _make_record(
            bound_objects=[{
                "binder_type": "small_molecule", "role": "primary_ligand",
                "comp_id": "IBR", "covalent_warhead_flag": True, "is_covalent": True,
            }],
        )
        assert "covalent_binder" in compute_flags(r)

    def test_peptide_partner_flag(self):
        r = _make_record(
            bound_objects=[{"binder_type": "peptide", "role": "primary_ligand",
                            "comp_id": None, "residue_count": 9}],
        )
        assert "peptide_partner" in compute_flags(r)

    def test_multiple_bound_objects_flag(self):
        r = _make_record(
            bound_objects=[
                {"binder_type": "small_molecule", "role": "primary_ligand", "comp_id": "X1"},
                {"binder_type": "small_molecule", "role": "co_ligand",      "comp_id": "X2"},
            ],
        )
        assert "multiple_bound_objects" in compute_flags(r)

    def test_homomeric_symmetric_interface_flag(self):
        r = _make_record(
            interfaces=[{
                "entity_id_a": "E1", "entity_id_b": "E1",
                "chain_ids_a": ["A"], "chain_ids_b": ["B"],
                "interface_type": "protein_protein",
                "is_symmetric": True, "is_hetero": False,
            }],
        )
        assert "homomeric_symmetric_interface" in compute_flags(r)

    def test_assembly_ambiguity_flag(self):
        r = _make_record(
            assembly_info={"assembly_count": 3, "oligomeric_details": "HETERO 3-MER"},
        )
        assert "assembly_ambiguity" in compute_flags(r)

    def test_membrane_context_flag_from_provenance(self):
        r = _make_record(
            provenance={**_PROV, "membrane_protein_context": True},
        )
        assert "membrane_protein_context" in compute_flags(r)

    def test_possible_additive_flag(self):
        r = _make_record(
            bound_objects=[{"binder_type": "additive", "role": "artifact", "comp_id": "GOL"}],
        )
        assert "possible_crystallization_additive" in compute_flags(r)


# ===========================================================================
# INTEGRATION TESTS — stress-test panel (require network + real RCSB data)
# ===========================================================================

def _load_panel() -> list[dict]:
    if not _PANEL_PATH.exists():
        return []
    with _PANEL_PATH.open() as f:
        data = yaml.safe_load(f)
    return (data.get("stress_test_panel") or {}).get("cases") or []


@pytest.mark.integration
@pytest.mark.parametrize(
    "case",
    _load_panel(),
    ids=[c.get("pdb_id", "?") for c in _load_panel()],
)
def test_stress_panel_case(case: dict) -> None:
    """Fetch a real RCSB entry and assert the expected structural properties."""
    pdb_id: str = case["pdb_id"]
    expected: dict = case.get("expected_outcomes", {})
    label: str = case.get("label", pdb_id)

    adapter = RCSBAdapter()
    raw = adapter.fetch_metadata(pdb_id)
    classified = classify_entry(raw)
    record = adapter.normalize_record(raw)
    flags = compute_flags(record)

    # --- Multiple bound objects ---
    if expected.get("multiple_bound_objects") is True:
        non_artifact = [
            b for b in classified["bound_objects"]
            if b.role != "artifact"
        ]
        assert len(non_artifact) > 1, (
            f"{pdb_id} ({label}): expected multiple_bound_objects but got "
            f"{len(non_artifact)} non-artifact bound objects"
        )

    # --- Metal present ---
    if expected.get("metal_present") is True:
        assert "metal_present" in flags, (
            f"{pdb_id} ({label}): expected metal_present flag; "
            f"flags={flags}, bound_objects={[b.comp_id for b in classified['bound_objects']]}"
        )

    if expected.get("metal_expected_min_count") is not None:
        metal_count = sum(
            1 for b in classified["bound_objects"] if b.binder_type == "metal_ion"
        )
        # Also count metallocofactors
        metal_cofactor_count = sum(
            1 for b in classified["bound_objects"]
            if b.binder_type == "cofactor" and (b.comp_id or "") in {
                "HEM", "HEC", "HEA", "HEB", "FES", "SF4", "F3S", "MGD", "CLA", "BCL"
            }
        )
        total_metal_objects = metal_count + metal_cofactor_count
        min_count = expected["metal_expected_min_count"]
        assert total_metal_objects >= min_count, (
            f"{pdb_id} ({label}): expected ≥{min_count} metal objects, "
            f"got {total_metal_objects}"
        )

    # --- Cofactor / nucleotide ---
    if expected.get("cofactor_or_nucleotide_present") is True:
        cofactors = [b for b in classified["bound_objects"] if b.binder_type == "cofactor"]
        assert len(cofactors) >= 1, (
            f"{pdb_id} ({label}): expected cofactor_present; "
            f"bound_objects={[b.comp_id for b in classified['bound_objects']]}"
        )

    # --- Peptide partner ---
    if expected.get("peptide_partner") is True:
        peptides = [b for b in classified["bound_objects"] if b.binder_type == "peptide"]
        assert len(peptides) >= 1, (
            f"{pdb_id} ({label}): expected peptide_partner but found none; "
            f"polymer entities: {[e.get('rcsb_id') for e in classified['protein_entities'] + classified['peptide_entities']]}"
        )
    elif expected.get("peptide_partner") is False:
        peptides = [b for b in classified["bound_objects"] if b.binder_type == "peptide"]
        assert len(peptides) == 0, (
            f"{pdb_id} ({label}): expected NO peptide_partner but found {len(peptides)}"
        )

    # --- Glycan present ---
    if expected.get("glycan_present") is True:
        glycans = [b for b in classified["bound_objects"] if b.binder_type == "glycan"]
        assert len(glycans) >= 1, (
            f"{pdb_id} ({label}): expected glycan_present but found none"
        )

    # --- Covalent binder ---
    if expected.get("covalent_binder") is True:
        assert "covalent_binder" in flags, (
            f"{pdb_id} ({label}): expected covalent_binder flag"
        )

    # --- Membrane protein context ---
    if expected.get("membrane_protein_context") is True:
        assert "membrane_protein_context" in flags, (
            f"{pdb_id} ({label}): expected membrane_protein_context flag"
        )

    # --- Heteromeric ---
    if expected.get("oligomer_type") == "heteromeric":
        assert classified["is_homo_oligomeric"] is False or len(
            classified["protein_entities"]
        ) >= 2 or len(classified["peptide_entities"]) >= 1, (
            f"{pdb_id} ({label}): expected heteromeric but "
            f"is_homo={classified['is_homo_oligomeric']}, "
            f"n_protein_entities={len(classified['protein_entities'])}"
        )

    # --- Homomeric / symmetric ---
    if expected.get("symmetric_or_repeat_subunits") is True:
        symmetric_ifaces = [i for i in classified["interfaces"] if i.is_symmetric]
        homo_oligo = classified["is_homo_oligomeric"] is True
        assert symmetric_ifaces or homo_oligo, (
            f"{pdb_id} ({label}): expected symmetric interface or homo-oligomer"
        )

    # --- Small molecule ligand present ---
    if expected.get("small_molecule_ligand_present") is True:
        sm = [b for b in classified["bound_objects"] if b.binder_type == "small_molecule"]
        assert len(sm) >= 1, (
            f"{pdb_id} ({label}): expected small_molecule_ligand_present"
        )

    # --- Multimeric polymer count ---
    if expected.get("polymer_partner_count_min") is not None:
        total_poly = (
            len(classified["protein_entities"])
            + len(classified["peptide_entities"])
        )
        min_count = expected["polymer_partner_count_min"]
        assert total_poly >= min_count, (
            f"{pdb_id} ({label}): expected ≥{min_count} polymer entities, got {total_poly}"
        )

    # --- Heme-like cofactor (specifically for hemoglobin-type cases) ---
    if expected.get("heme_like_cofactor_present") is True:
        heme_comps = {"HEM", "HEC", "HEA", "HEB", "HDD"}
        heme_objs = [
            b for b in classified["bound_objects"]
            if (b.comp_id or "").upper() in heme_comps
        ]
        assert len(heme_objs) >= 1, (
            f"{pdb_id} ({label}): expected heme-like cofactor but found none; "
            f"bound_object comp_ids={[b.comp_id for b in classified['bound_objects']]}"
        )
