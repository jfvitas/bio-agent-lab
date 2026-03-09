from unittest.mock import Mock, patch

from pbdata.graph.identifier_map import (
    IdentifierMappingPlan,
    batch_map_protein_identifiers,
    detect_identifier_type,
    map_protein_identifier,
)


def _resp(text: str) -> Mock:
    resp = Mock()
    resp.text = text
    resp.raise_for_status.return_value = None
    return resp


def test_detect_identifier_type_supports_expected_families() -> None:
    assert detect_identifier_type("P12345") == "ACC"
    assert detect_identifier_type("RL11_HUMAN") == "ID"
    assert detect_identifier_type("ENSG00000123456") == "ENSEMBL_ID"
    assert detect_identifier_type("672") == "P_ENTREZGENEID"
    assert detect_identifier_type("not-an-id") == "UNKNOWN"


def test_map_protein_identifier_from_uniprot_accession() -> None:
    with patch("pbdata.graph.identifier_map.requests.post", side_effect=[
        _resp("P12345\t672\n"),
        _resp("P12345\tENSG00000123456\n"),
    ]):
        result = map_protein_identifier("P12345")

    assert isinstance(result, IdentifierMappingPlan)
    assert result.uniprot_id == "P12345"
    assert result.entrez_id == "672"
    assert result.ensembl_id == "ENSG00000123456"
    assert result.status == "exact_mapped"


def test_map_protein_identifier_from_ensembl_id() -> None:
    with patch("pbdata.graph.identifier_map.requests.post", side_effect=[
        _resp("ENSG00000123456\tP12345\n"),
        _resp("P12345\t672\n"),
        _resp("P12345\tENSG00000123456\n"),
    ]):
        result = map_protein_identifier("ENSG00000123456")

    assert result.detected_id_type == "ENSEMBL_ID"
    assert result.uniprot_id == "P12345"
    assert result.entrez_id == "672"
    assert result.status == "exact_mapped"


def test_map_protein_identifier_marks_ambiguous_results() -> None:
    with patch("pbdata.graph.identifier_map.requests.post", side_effect=[
        _resp("P12345\t672\nP12345\t673\n"),
        _resp("P12345\tENSG00000123456\n"),
    ]):
        result = map_protein_identifier("P12345")

    assert result.status == "ambiguous"
    assert result.entrez_id == "672"


def test_batch_map_protein_identifiers_returns_multiple_results() -> None:
    with patch("pbdata.graph.identifier_map.requests.post", side_effect=[
        _resp("P12345\t672\n"),
        _resp("P12345\tENSG00000123456\n"),
        _resp("Q99999\t7157\n"),
        _resp("Q99999\tENSG00000141510\n"),
    ]):
        results = batch_map_protein_identifiers(["P12345", "Q99999"])

    assert len(results) == 2
    assert results[0].uniprot_id == "P12345"
    assert results[1].uniprot_id == "Q99999"
