import pytest
from pathlib import Path
from uuid import uuid4

from bio_agent_lab.main import classify_structure

_LOCAL_TMP = Path(__file__).parent / "_cif_tmp"
_LOCAL_TMP.mkdir(exist_ok=True)


def _write_cif(cif_content: str) -> str:
    cif_file = _LOCAL_TMP / f"{uuid4().hex}.cif"
    cif_file.write_text(cif_content)
    return str(cif_file)


def test_imports():
    assert callable(classify_structure)


def test_missing_file_raises():
    """FileNotFoundError raised for a non-existent path."""
    with pytest.raises(FileNotFoundError):
        classify_structure("/nonexistent/path/file.cif")

def test_directory_path_raises():
    """Directory path should fail fast with IsADirectoryError."""
    dir_path = _LOCAL_TMP / f"dir_{uuid4().hex}"
    dir_path.mkdir(exist_ok=True)
    with pytest.raises(IsADirectoryError):
        classify_structure(str(dir_path))

def test_entities_inventory_polymer_only():
    """entities list must have one record with correct poly_subtype."""
    cif_content = """\
data_TEST
_entry.id TEST
loop_
_entity.id
_entity.type
1 polymer
#
loop_
_entity_poly.entity_id
_entity_poly.type
1 'polypeptide(L)'
#
"""
    result = classify_structure(_write_cif(cif_content))
    assert "entities" in result
    assert result["entities"] == [{"id": "1", "entity_type": "polymer", "poly_subtype": "polypeptide(L)"}]
    assert result["classification"] == "polymer_only"


def test_entities_inventory_polymer_and_nonpolymer():
    """Non-polymer entity must appear in entities with poly_subtype: None."""
    cif_content = """\
data_TEST
_entry.id TEST
loop_
_entity.id
_entity.type
1 polymer
2 non-polymer
#
loop_
_entity_poly.entity_id
_entity_poly.type
1 'polypeptide(L)'
#
"""
    result = classify_structure(_write_cif(cif_content))
    assert result["entities"][0] == {"id": "1", "entity_type": "polymer", "poly_subtype": "polypeptide(L)"}
    assert result["entities"][1] == {"id": "2", "entity_type": "non-polymer", "poly_subtype": None}


def test_flags_protein_only():
    """has_protein True, has_nucleic_acid False, no ligands for a single polypeptide."""
    cif_content = """\
data_TEST
_entry.id TEST
loop_
_entity.id
_entity.type
1 polymer
#
loop_
_entity_poly.entity_id
_entity_poly.type
1 'polypeptide(L)'
#
"""
    result = classify_structure(_write_cif(cif_content))
    assert result["has_protein"] is True
    assert result["has_nucleic_acid"] is False
    assert result["n_polymer_entities"] == 1
    assert result["ligand_entities"] == []
    assert result["distinct_poly_subtypes"] == ["polypeptide(L)"]


def test_flags_dna_with_ligand():
    """has_nucleic_acid True, has_protein False, one ligand entity."""
    cif_content = """\
data_TEST
_entry.id TEST
loop_
_entity.id
_entity.type
1 polymer
2 non-polymer
#
loop_
_pdbx_entity_nonpoly.entity_id
_pdbx_entity_nonpoly.comp_id
2 ATP
#
loop_
_entity_poly.entity_id
_entity_poly.type
1 polydeoxyribonucleotide
#
"""
    result = classify_structure(_write_cif(cif_content))
    assert result["has_protein"] is False
    assert result["has_nucleic_acid"] is True
    assert result["n_polymer_entities"] == 1
    assert len(result["ligand_entities"]) == 1


def test_classification_multi_polymer_complex():
    """Two distinct polymer entities → multi_polymer_complex."""
    cif_content = """\
data_TEST
_entry.id TEST
loop_
_entity.id
_entity.type
1 polymer
2 polymer
#
loop_
_entity_poly.entity_id
_entity_poly.type
1 'polypeptide(L)'
2 polyribonucleotide
#
"""
    result = classify_structure(_write_cif(cif_content))
    assert result["classification"] == "multi_polymer_complex"


def test_classification_protein_ligand():
    """One polypeptide + one non-polymer → protein_ligand."""
    cif_content = """\
data_TEST
_entry.id TEST
loop_
_entity.id
_entity.type
1 polymer
2 non-polymer
#
loop_
_pdbx_entity_nonpoly.entity_id
_pdbx_entity_nonpoly.comp_id
2 ATP
#
loop_
_entity_poly.entity_id
_entity_poly.type
1 'polypeptide(L)'
#
"""
    result = classify_structure(_write_cif(cif_content))
    assert result["classification"] == "protein_ligand"


def test_common_solvent_is_not_treated_as_ligand():
    cif_content = """\
data_TEST
_entry.id TEST
loop_
_entity.id
_entity.type
1 polymer
2 non-polymer
#
loop_
_pdbx_entity_nonpoly.entity_id
_pdbx_entity_nonpoly.comp_id
2 HOH
#
loop_
_entity_poly.entity_id
_entity_poly.type
1 'polypeptide(L)'
#
"""
    result = classify_structure(_write_cif(cif_content))
    assert result["classification"] == "polymer_only"
    assert result["ligand_entities"] == []
    assert len(result["excluded_nonpoly_entities"]) == 1


def test_nonpolymer_without_comp_id_is_not_assumed_to_be_ligand():
    cif_content = """\
data_TEST
_entry.id TEST
loop_
_entity.id
_entity.type
1 polymer
2 non-polymer
#
loop_
_entity_poly.entity_id
_entity_poly.type
1 'polypeptide(L)'
#
"""
    result = classify_structure(_write_cif(cif_content))
    assert result["classification"] == "polymer_only"
    assert result["ligand_entities"] == []
    assert len(result["excluded_nonpoly_entities"]) == 1


def test_classification_polymer_only_nucleic():
    """RNA only, no ligand → polymer_only."""
    cif_content = """\
data_TEST
_entry.id TEST
loop_
_entity.id
_entity.type
1 polymer
#
loop_
_entity_poly.entity_id
_entity_poly.type
1 polyribonucleotide
#
"""
    result = classify_structure(_write_cif(cif_content))
    assert result["classification"] == "polymer_only"


def test_classification_unknown():
    """Non-polymer only (no polymer entity) → unknown."""
    cif_content = """\
data_TEST
_entry.id TEST
loop_
_entity.id
_entity.type
1 non-polymer
#
"""
    result = classify_structure(_write_cif(cif_content))
    assert result["classification"] == "unknown"


def test_classification_is_case_and_whitespace_tolerant():
    cif_content = """\
data_TEST
_entry.id TEST
loop_
_entity.id
_entity.type
1 ' Polymer '
2 ' NON-POLYMER '
#
loop_
_pdbx_entity_nonpoly.entity_id
_pdbx_entity_nonpoly.comp_id
2 ' ATP '
#
loop_
_entity_poly.entity_id
_entity_poly.type
1 ' Polypeptide(L) '
#
"""
    result = classify_structure(_write_cif(cif_content))
    assert result["classification"] == "protein_ligand"
    assert result["has_protein"] is True


def test_entity_poly_id_whitespace_still_maps_to_entity():
    cif_content = """\
data_TEST
_entry.id TEST
loop_
_entity.id
_entity.type
1 polymer
#
loop_
_entity_poly.entity_id
_entity_poly.type
' 1 ' 'polypeptide(L)'
#
"""
    result = classify_structure(_write_cif(cif_content))
    assert result["entities"] == [{"id": "1", "entity_type": "polymer", "poly_subtype": "polypeptide(L)"}]
