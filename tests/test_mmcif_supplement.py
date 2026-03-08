from pbdata.parsing.mmcif_supplement import parse_mmcif_supplement
from pbdata.sources.rcsb_classify import classify_entry


_MMCIF_TEXT = """data_TEST
loop_
_entity.id
_entity.type
1 polymer
2 non-polymer
3 branched
loop_
_entity_poly.entity_id
_entity_poly.type
_entity_poly.pdbx_seq_one_letter_code_can
1 'polypeptide(L)' AAAAAAAAA
loop_
_entity_poly_seq.entity_id
_entity_poly_seq.num
_entity_poly_seq.mon_id
1 1 ALA
1 2 GLY
1 3 SER
1 4 THR
1 5 TYR
1 6 LEU
1 7 VAL
1 8 GLY
1 9 ASN
loop_
_struct_asym.id
_struct_asym.entity_id
A 1
B 3
loop_
_atom_site.group_PDB
_atom_site.label_entity_id
_atom_site.label_comp_id
_atom_site.auth_asym_id
_atom_site.label_asym_id
_atom_site.auth_seq_id
_atom_site.label_seq_id
HETATM 2 MN A A 301 1
HETATM 2 MN A A 302 2
HETATM 3 NAG B B 401 1
"""


def test_parse_mmcif_supplement_extracts_instances_and_branched_entities() -> None:
    supplement = parse_mmcif_supplement(_MMCIF_TEXT)
    assert len(supplement["nonpolymer_instances"]) == 2
    assert supplement["nonpolymer_instances"][0]["comp_id"] == "MN"
    assert supplement["branched_entities"][0]["entity_id"] == "3"
    assert supplement["branched_entities"][0]["chain_ids"] == ["B"]


def test_classify_entry_uses_mmcif_supplement_for_missing_nonpoly_and_glycan_data() -> None:
    raw = {
        "rcsb_id": "TEST",
        "polymer_entities": [
            {
                "rcsb_id": "TEST_1",
                "entity_poly": {"type": "polypeptide(L)", "pdbx_seq_one_letter_code_can": "A" * 100},
                "rcsb_polymer_entity_container_identifiers": {"auth_asym_ids": ["A"]},
            }
        ],
        "nonpolymer_entities": [],
        "mmcif_supplement": {
            "polymer_entities": [],
            "branched_entities": [{"entity_id": "3", "chain_ids": ["B"]}],
            "nonpolymer_instances": [
                {"entity_id": "2", "comp_id": "MN", "chain_id": "A", "residue_id": "301"},
                {"entity_id": "2", "comp_id": "MN", "chain_id": "A", "residue_id": "302"},
            ],
        },
    }
    classified = classify_entry(raw)
    metals = [obj for obj in classified["bound_objects"] if obj.binder_type == "metal_ion"]
    glycans = [obj for obj in classified["bound_objects"] if obj.binder_type == "glycan"]
    assert len(metals) == 2
    assert len(glycans) == 1
