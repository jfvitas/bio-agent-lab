from pbdata.quality.stress_panel import compare_expected_outcomes, summarize_case_outcomes
from pbdata.schemas.canonical_sample import CanonicalBindingSample
from pbdata.sources.rcsb import RCSBAdapter
from pbdata.sources.rcsb_classify import build_bound_objects, classify_entry


def _poly_entity(entity_id: str, chains: list[str], poly_type: str, seq: str) -> dict:
    return {
        "rcsb_id": entity_id,
        "entity_poly": {"type": poly_type, "pdbx_seq_one_letter_code_can": seq},
        "rcsb_polymer_entity_container_identifiers": {"auth_asym_ids": chains},
    }


def test_build_bound_objects_preserves_polymer_glycan() -> None:
    glycan_poly = _poly_entity("GLY1", ["G"], "polysaccharide(D)", "NAG")
    objects = build_bound_objects([], [], other_poly_entities=[glycan_poly])
    assert len(objects) == 1
    assert objects[0].binder_type == "glycan"


def test_classify_entry_title_based_covalent_context() -> None:
    entry = {
        "rcsb_id": "7DTZ",
        "struct": {"title": "FGFR4 covalent inhibitor complex"},
        "polymer_entities": [_poly_entity("P1", ["A"], "polypeptide(L)", "A" * 100)],
        "nonpolymer_entities": [
            {
                "rcsb_id": "L1",
                "nonpolymer_comp": {"chem_comp": {"id": "LIG", "name": "Ligand"}},
            }
        ],
    }
    classified = classify_entry(entry)
    assert any(obj.is_covalent is True for obj in classified["bound_objects"])


def test_rcsb_adapter_polymer_entity_count_uses_chain_instances() -> None:
    raw = {
        "rcsb_id": "2HHB",
        "struct": {"title": "hemoglobin"},
        "rcsb_entry_info": {"resolution_combined": [2.0]},
        "polymer_entities": [
            _poly_entity("A", ["A", "C"], "polypeptide(L)", "A" * 100),
            _poly_entity("B", ["B", "D"], "polypeptide(L)", "A" * 100),
        ],
        "nonpolymer_entities": [],
    }
    record = RCSBAdapter().normalize_record(raw)
    assert record.polymer_entity_count == 4


def test_rcsb_adapter_preserves_resolution_from_entry_info() -> None:
    raw = {
        "rcsb_id": "1ABC",
        "struct": {"title": "resolution regression"},
        "rcsb_entry_info": {"resolution_combined": [1.8]},
        "polymer_entities": [_poly_entity("A", ["A"], "polypeptide(L)", "A" * 100)],
        "nonpolymer_entities": [],
    }
    record = RCSBAdapter().normalize_record(raw)
    assert record.structure_resolution == 1.8


def test_compare_expected_outcomes_reports_minimum_count_mismatch() -> None:
    mismatches = compare_expected_outcomes(
        {"polymer_partner_count_min": 4, "metal_present": True},
        {"polymer_partner_count": 2, "metal_present": False},
    )
    assert any("polymer_partner_count_min" in mismatch for mismatch in mismatches)
    assert any("metal_present" in mismatch for mismatch in mismatches)


def test_summarize_case_outcomes_detects_membrane_and_auxiliary_title() -> None:
    raw = {"struct": {"title": "AT1R nanobody complex"}}
    classified = {
        "bound_objects": [],
        "protein_entities": [{}, {}],
        "peptide_entities": [],
        "other_poly": [],
        "interfaces": [{"is_symmetric": False, "is_hetero": True}],
        "assembly_info": None,
        "is_homo_oligomeric": False,
    }
    record = CanonicalBindingSample(
        sample_id="x",
        task_type="protein_protein",
        source_database="RCSB",
        source_record_id="x",
        provenance={"ingested_at": "2026-01-01T00:00:00+00:00", "membrane_protein_context": True},
        quality_flags=[],
        quality_score=0.0,
        polymer_entity_count=2,
    )
    actual = summarize_case_outcomes(raw, classified, record)
    assert actual["membrane_protein_context"] is True
    assert actual["nanobody_or_auxiliary_protein_present"] is True


def test_summarize_case_outcomes_counts_chain_instances_for_repeated_bound_objects() -> None:
    raw = {"struct": {"title": "hemoglobin"}}
    classified = {
        "bound_objects": build_bound_objects(
            [
                {
                    "rcsb_id": "HEM_1",
                    "nonpolymer_comp": {"chem_comp": {"id": "HEM", "name": "heme"}},
                    "rcsb_nonpolymer_entity_container_identifiers": {"auth_asym_ids": ["A", "B", "C", "D"]},
                }
            ],
            [],
        ),
        "protein_entities": [{}, {}],
        "peptide_entities": [],
        "other_poly": [],
        "interfaces": [],
        "assembly_info": None,
        "is_homo_oligomeric": False,
    }
    record = CanonicalBindingSample(
        sample_id="x",
        task_type="protein_ligand",
        source_database="RCSB",
        source_record_id="x",
        provenance={"ingested_at": "2026-01-01T00:00:00+00:00"},
        quality_flags=[],
        quality_score=0.0,
        polymer_entity_count=4,
        bound_objects=[obj.model_dump() for obj in classified["bound_objects"]],
    )
    actual = summarize_case_outcomes(raw, classified, record)
    assert actual["multiple_bound_objects"] is True
    assert actual["metal_object_count"] == 4


def test_summarize_case_outcomes_downgrades_au_repeat_to_single_polymer_context() -> None:
    raw = {"struct": {"title": "repeat in asymmetric unit"}}
    classified = {
        "bound_objects": [],
        "protein_entities": [{"rcsb_id": "P1"}],
        "peptide_entities": [],
        "other_poly": [],
        "interfaces": [],
        "assembly_info": type(
            "AssemblyInfoStub",
            (),
            {"asymmetric_unit_is_biological": False, "oligomeric_count": 1},
        )(),
        "is_homo_oligomeric": True,
    }
    record = CanonicalBindingSample(
        sample_id="x",
        task_type="protein_ligand",
        source_database="RCSB",
        source_record_id="x",
        provenance={"ingested_at": "2026-01-01T00:00:00+00:00"},
        quality_flags=[],
        quality_score=0.0,
        polymer_entity_count=1,
    )
    actual = summarize_case_outcomes(raw, classified, record)
    assert actual["oligomer_type"] == "monomeric_or_single_polymer_context"
    assert actual["assembly_or_symmetry_handling_important"] is False


def test_summarize_case_outcomes_multimeric_needs_large_assembly_or_many_proteins() -> None:
    raw = {"struct": {"title": "receptor with peptide and nanobody"}}
    classified = {
        "bound_objects": [],
        "protein_entities": [{"rcsb_id": "R"}, {"rcsb_id": "N"}],
        "peptide_entities": [{"rcsb_id": "P"}],
        "other_poly": [],
        "interfaces": [{"is_symmetric": False, "is_hetero": True}],
        "assembly_info": type(
            "AssemblyInfoStub",
            (),
            {"asymmetric_unit_is_biological": True, "oligomeric_count": 3},
        )(),
        "is_homo_oligomeric": False,
    }
    record = CanonicalBindingSample(
        sample_id="x",
        task_type="protein_protein",
        source_database="RCSB",
        source_record_id="x",
        provenance={"ingested_at": "2026-01-01T00:00:00+00:00", "membrane_protein_context": True},
        quality_flags=[],
        quality_score=0.0,
        polymer_entity_count=3,
    )
    actual = summarize_case_outcomes(raw, classified, record)
    assert actual["multimeric_complex"] is False


def test_summarize_case_outcomes_marks_multi_protein_assembly_as_handling_important() -> None:
    raw = {"struct": {"title": "heteromeric assembly"}}
    classified = {
        "bound_objects": [],
        "protein_entities": [{"rcsb_id": "A"}, {"rcsb_id": "B"}],
        "peptide_entities": [],
        "other_poly": [],
        "interfaces": [],
        "assembly_info": type(
            "AssemblyInfoStub",
            (),
            {"asymmetric_unit_is_biological": None, "oligomeric_count": 2, "assembly_count": 2},
        )(),
        "is_homo_oligomeric": False,
    }
    record = CanonicalBindingSample(
        sample_id="x",
        task_type="protein_protein",
        source_database="RCSB",
        source_record_id="x",
        provenance={"ingested_at": "2026-01-01T00:00:00+00:00"},
        quality_flags=[],
        quality_score=0.0,
        polymer_entity_count=2,
    )
    actual = summarize_case_outcomes(raw, classified, record)
    assert actual["assembly_or_symmetry_handling_important"] is True


def test_summarize_case_outcomes_marks_glycan_complex_as_multimeric_and_handling_important() -> None:
    raw = {"struct": {"title": "glycosylated immune complex"}}
    classified = {
        "bound_objects": [
            type("BoundObjectStub", (), {"binder_type": "glycan", "comp_id": "NAG", "role": "co_ligand", "chain_ids": ["G"]})()
        ],
        "protein_entities": [{"rcsb_id": "A"}, {"rcsb_id": "B"}],
        "peptide_entities": [],
        "other_poly": [],
        "interfaces": [],
        "assembly_info": None,
        "is_homo_oligomeric": False,
    }
    record = CanonicalBindingSample(
        sample_id="x",
        task_type="protein_protein",
        source_database="RCSB",
        source_record_id="x",
        provenance={"ingested_at": "2026-01-01T00:00:00+00:00"},
        quality_flags=[],
        quality_score=0.0,
        polymer_entity_count=3,
    )
    actual = summarize_case_outcomes(raw, classified, record)
    assert actual["multimeric_complex"] is True
    assert actual["assembly_or_symmetry_handling_important"] is True


def test_summarize_case_outcomes_infers_immune_complex_peptide_and_alkali_context() -> None:
    raw = {"struct": {"title": "immune complex"}}
    classified = {
        "bound_objects": [
            type("BoundObjectStub", (), {"binder_type": "metal_ion", "comp_id": "BEF", "role": "metal_mediated_contact", "chain_ids": ["A"]})(),
            type("BoundObjectStub", (), {"binder_type": "metal_ion", "comp_id": "NA", "role": "structural_ion", "chain_ids": ["B"]})(),
        ],
        "protein_entities": [{"rcsb_id": "A"}, {"rcsb_id": "B"}],
        "peptide_entities": [],
        "other_poly": [],
        "interfaces": [],
        "assembly_info": None,
        "is_homo_oligomeric": False,
    }
    record = CanonicalBindingSample(
        sample_id="x",
        task_type="protein_protein",
        source_database="RCSB",
        source_record_id="x",
        provenance={"ingested_at": "2026-01-01T00:00:00+00:00"},
        quality_flags=[],
        quality_score=0.0,
        polymer_entity_count=4,
        bound_objects=[
            {"binder_type": "metal_ion", "comp_id": "BEF", "role": "metal_mediated_contact"},
            {"binder_type": "metal_ion", "comp_id": "NA", "role": "structural_ion"},
        ],
    )
    actual = summarize_case_outcomes(raw, classified, record)
    assert actual["peptide_partner"] is True
    assert actual["alkali_counterion_possible"] is True
    assert actual["multimeric_complex"] is True


def test_summarize_case_outcomes_downgrades_membrane_auxiliary_metals() -> None:
    raw = {"struct": {"title": "AT1R nanobody complex"}}
    classified = {
        "bound_objects": [
            type("BoundObjectStub", (), {"binder_type": "metal_ion", "comp_id": "CA", "role": "structural_ion", "chain_ids": ["A"]})()
        ],
        "protein_entities": [{"rcsb_id": "R"}, {"rcsb_id": "N"}],
        "peptide_entities": [{"rcsb_id": "P"}],
        "other_poly": [],
        "interfaces": [],
        "assembly_info": None,
        "is_homo_oligomeric": False,
    }
    record = CanonicalBindingSample(
        sample_id="x",
        task_type="protein_protein",
        source_database="RCSB",
        source_record_id="x",
        provenance={"ingested_at": "2026-01-01T00:00:00+00:00", "membrane_protein_context": True},
        quality_flags=[],
        quality_score=0.0,
        polymer_entity_count=3,
        bound_objects=[{"binder_type": "metal_ion", "comp_id": "CA", "role": "structural_ion"}],
    )
    actual = summarize_case_outcomes(raw, classified, record)
    assert actual["metal_present"] is False
