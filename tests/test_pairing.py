from pbdata.pairing import parse_pair_identity_key
from pbdata.pipeline.assay_merge import pair_identity_key
from pbdata.schemas.canonical_sample import CanonicalBindingSample


def test_parse_pair_identity_key_supports_five_part_protein_ligand_keys() -> None:
    parsed = parse_pair_identity_key("protein_ligand|1ABC|A,B|ATP|wt")

    assert parsed is not None
    assert parsed.task_type == "protein_ligand"
    assert parsed.pdb_id == "1ABC"
    assert parsed.receptor_chain_ids == ("A", "B")
    assert parsed.ligand_key == "ATP"
    assert parsed.subject_key == "ATP"
    assert parsed.mutation_key == "wt"


def test_parse_pair_identity_key_supports_legacy_four_part_non_pair_keys() -> None:
    sample = CanonicalBindingSample(
        sample_id="skempi-1",
        task_type="mutation_ddg",
        source_database="SKEMPI",
        source_record_id="1ABC:A42V",
        pdb_id="1ABC",
        mutation_string="A42V",
        wildtype_or_mutant="mutant",
        assay_type="ddG",
        assay_value=1.2,
        assay_unit="kcal/mol",
        assay_value_standardized=1.2,
        assay_value_log10=None,
        provenance={"ingested_at": "2026-03-11T00:00:00+00:00"},
        quality_flags=[],
        quality_score=0.0,
    )

    parsed = parse_pair_identity_key(pair_identity_key(sample))

    assert parsed is not None
    assert parsed.task_type == "mutation_ddg"
    assert parsed.pdb_id == "1ABC"
    assert parsed.subject_key == "1ABC:A42V"
    assert parsed.mutation_key == "A42V"


def test_parse_pair_identity_key_rejects_legacy_four_part_pair_keys() -> None:
    assert parse_pair_identity_key("protein_ligand|1ABC|A|ATP") is None
