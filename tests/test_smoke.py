import pytest
from src.bio_agent_lab.main import classify_structure

def test_imports():
    assert callable(classify_structure)


def test_missing_file_raises():
    """FileNotFoundError raised for a non-existent path."""
    with pytest.raises(FileNotFoundError):
        classify_structure("/nonexistent/path/file.cif")

def test_entities_inventory_polymer_only(tmp_path):
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
    cif_file = tmp_path / "test.cif"
    cif_file.write_text(cif_content)

    result = classify_structure(str(cif_file))
    assert "entities" in result
    assert result["entities"] == [{"id": "1", "entity_type": "polymer", "poly_subtype": "polypeptide(L)"}]
    assert result["classification"] == "polymer_only"


def test_entities_inventory_polymer_and_nonpolymer(tmp_path):
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
    cif_file = tmp_path / "test.cif"
    cif_file.write_text(cif_content)

    result = classify_structure(str(cif_file))
    assert result["entities"][0] == {"id": "1", "entity_type": "polymer", "poly_subtype": "polypeptide(L)"}
    assert result["entities"][1] == {"id": "2", "entity_type": "non-polymer", "poly_subtype": None}


def test_flags_protein_only(tmp_path):
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
    cif_file = tmp_path / "test.cif"
    cif_file.write_text(cif_content)

    result = classify_structure(str(cif_file))
    assert result["has_protein"] is True
    assert result["has_nucleic_acid"] is False
    assert result["n_polymer_entities"] == 1
    assert result["ligand_entities"] == []
    assert result["distinct_poly_subtypes"] == ["polypeptide(L)"]


def test_flags_dna_with_ligand(tmp_path):
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
_entity_poly.entity_id
_entity_poly.type
1 polydeoxyribonucleotide
#
"""
    cif_file = tmp_path / "test.cif"
    cif_file.write_text(cif_content)

    result = classify_structure(str(cif_file))
    assert result["has_protein"] is False
    assert result["has_nucleic_acid"] is True
    assert result["n_polymer_entities"] == 1
    assert len(result["ligand_entities"]) == 1


def test_classification_multi_polymer_complex(tmp_path):
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
    cif_file = tmp_path / "test.cif"
    cif_file.write_text(cif_content)

    result = classify_structure(str(cif_file))
    assert result["classification"] == "multi_polymer_complex"


def test_classification_protein_ligand(tmp_path):
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
_entity_poly.entity_id
_entity_poly.type
1 'polypeptide(L)'
#
"""
    cif_file = tmp_path / "test.cif"
    cif_file.write_text(cif_content)

    result = classify_structure(str(cif_file))
    assert result["classification"] == "protein_ligand"


def test_classification_polymer_only_nucleic(tmp_path):
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
    cif_file = tmp_path / "test.cif"
    cif_file.write_text(cif_content)

    result = classify_structure(str(cif_file))
    assert result["classification"] == "polymer_only"


def test_classification_unknown(tmp_path):
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
    cif_file = tmp_path / "test.cif"
    cif_file.write_text(cif_content)

    result = classify_structure(str(cif_file))
    assert result["classification"] == "unknown"
