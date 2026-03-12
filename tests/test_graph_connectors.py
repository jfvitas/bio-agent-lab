"""Tests for graph source connectors (STRING, Reactome, BioGRID).

All external API calls are mocked — these tests validate normalization,
edge cases, deduplication, and error handling.
"""

import json
from pathlib import Path
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest

from pbdata.graph.connectors import (
    BioGRIDConnector,
    ConnectorRegistry,
    GraphConnectorStub,
    ReactomeConnector,
    STRINGConnector,
    connector_stub,
)
from pbdata.schemas.graph import GraphEdgeRecord, GraphNodeRecord

_LOCAL_TMP = Path(__file__).parent / "_tmp"
_LOCAL_TMP.mkdir(exist_ok=True)


# ---------------------------------------------------------------------------
# GraphConnectorStub (backward compat)
# ---------------------------------------------------------------------------


def test_connector_stub_still_works() -> None:
    stub = connector_stub("STRING", Path("data/raw/graph_sources/STRING"))
    assert stub.status == "stub"
    assert stub.source_name == "STRING"
    assert isinstance(stub, GraphConnectorStub)


# ---------------------------------------------------------------------------
# STRING connector
# ---------------------------------------------------------------------------


_STRING_TSV_RESPONSE = (
    "stringId_A\tstringId_B\tpreferredName_A\tpreferredName_B\tncbiTaxonId\tscore\tnscore\tfscore\tpscore\tascore\tescore\tdscore\ttscore\n"
    "9606.ENSP00000269305\t9606.ENSP00000344818\tTP53\tMDM2\t9606\t999\t0\t0\t0\t0.99\t0.99\t0\t0.99\n"
    "9606.ENSP00000269305\t9606.ENSP00000284811\tTP53\tBRCA1\t9606\t800\t0\t0\t0\t0.80\t0.80\t0\t0.80\n"
    "9606.ENSP00000269305\t9606.ENSP00000000001\tTP53\tLOW_SCORE\t9606\t100\t0\t0\t0\t0.10\t0.10\t0\t0.10\n"
)


@patch("pbdata.graph.connectors.requests.post")
def test_string_fetch_interactions(mock_post: MagicMock) -> None:
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.text = _STRING_TSV_RESPONSE
    mock_resp.raise_for_status = MagicMock()
    mock_post.return_value = mock_resp

    connector = STRINGConnector(score_threshold=400)
    nodes, edges = connector.fetch_interactions(["P04637"])

    # Should get TP53, MDM2, BRCA1 (LOW_SCORE filtered out by threshold)
    assert len(nodes) == 3
    assert len(edges) == 2

    # Check edge evidence scores
    for edge in edges:
        assert edge.edge_type == "ProteinProteinInteraction"
        assert edge.source_database == "STRING"
        assert edge.evidence_score is not None
        assert edge.evidence_score >= 0.4
        assert edge.provenance is not None
        assert edge.provenance["source"] == "STRING"
        assert edge.provenance["source_record_key"].startswith("string_ppi:")
        assert edge.provenance["extraction_method"] == "string_network_api_normalization"
        assert "retrieved_at" in edge.provenance

    # Check canonical ordering (sorted node IDs)
    for edge in edges:
        src_name = edge.source_node_id.split(":")[1]
        tgt_name = edge.target_node_id.split(":")[1]
        assert src_name <= tgt_name, "Edge nodes should be canonically ordered"


@patch("pbdata.graph.connectors.requests.post")
def test_string_threshold_filters_low_scores(mock_post: MagicMock) -> None:
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.text = _STRING_TSV_RESPONSE
    mock_resp.raise_for_status = MagicMock()
    mock_post.return_value = mock_resp

    # High threshold should filter out 800-score edge too
    connector = STRINGConnector(score_threshold=900)
    nodes, edges = connector.fetch_interactions(["P04637"])

    assert len(edges) == 1
    assert edges[0].evidence_score == pytest.approx(0.999)


@patch("pbdata.graph.connectors.requests.post")
def test_string_empty_input(mock_post: MagicMock) -> None:
    connector = STRINGConnector()
    nodes, edges = connector.fetch_interactions([])
    assert nodes == []
    assert edges == []
    mock_post.assert_not_called()


@patch("pbdata.graph.connectors.requests.post")
def test_string_api_failure_returns_empty(mock_post: MagicMock) -> None:
    mock_post.side_effect = ConnectionError("API down")

    connector = STRINGConnector()
    nodes, edges = connector.fetch_interactions(["P04637"])
    assert nodes == []
    assert edges == []


@patch("pbdata.graph.connectors.requests.post")
def test_string_batching(mock_post: MagicMock) -> None:
    """Verify large ID lists are batched correctly."""
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.text = "stringId_A\tstringId_B\tpreferredName_A\tpreferredName_B\tncbiTaxonId\tscore\n"
    mock_resp.raise_for_status = MagicMock()
    mock_post.return_value = mock_resp

    ids = [f"P{i:05d}" for i in range(450)]  # > 200, should need 3 batches
    connector = STRINGConnector()
    connector.fetch_interactions(ids)

    assert mock_post.call_count == 3


@patch("pbdata.graph.connectors.requests.post")
def test_string_deduplicates_nodes(mock_post: MagicMock) -> None:
    """Same protein appearing in multiple interactions should produce one node."""
    tsv = (
        "stringId_A\tstringId_B\tpreferredName_A\tpreferredName_B\tncbiTaxonId\tscore\n"
        "9606.ENSP1\t9606.ENSP2\tTP53\tMDM2\t9606\t900\n"
        "9606.ENSP1\t9606.ENSP3\tTP53\tBRCA1\t9606\t800\n"
    )
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.text = tsv
    mock_resp.raise_for_status = MagicMock()
    mock_post.return_value = mock_resp

    connector = STRINGConnector(score_threshold=400)
    nodes, edges = connector.fetch_interactions(["P04637"])

    node_ids = [n.node_id for n in nodes]
    assert len(node_ids) == len(set(node_ids)), "No duplicate nodes"
    assert len(edges) == 2


# ---------------------------------------------------------------------------
# Reactome connector
# ---------------------------------------------------------------------------

_REACTOME_PATHWAYS_RESPONSE = [
    {"stId": "R-HSA-109582", "displayName": "Hemostasis"},
    {"stId": "R-HSA-168256", "displayName": "Immune System"},
    {"stId": "R-HSA-1640170", "displayName": "Cell Cycle"},
]


@patch("pbdata.graph.connectors.requests.get")
def test_reactome_fetch_pathways(mock_get: MagicMock) -> None:
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = _REACTOME_PATHWAYS_RESPONSE
    mock_resp.raise_for_status = MagicMock()
    mock_get.return_value = mock_resp

    connector = ReactomeConnector()
    nodes, edges = connector.fetch_pathways(["P04637"])

    assert len(nodes) == 3
    assert len(edges) == 3

    # All nodes should be Pathway type
    for node in nodes:
        assert node.node_type == "Pathway"
        assert node.source_databases == ["Reactome"]
        assert node.node_id.startswith("pathway:")

    # All edges should be ProteinPathway
    for edge in edges:
        assert edge.edge_type == "ProteinPathway"
        assert edge.source_database == "Reactome"
        assert edge.source_node_id == "protein:P04637"
        assert edge.target_node_id.startswith("pathway:")
        assert edge.provenance is not None
        assert edge.provenance["source"] == "Reactome"
        assert edge.provenance["source_record_key"].startswith("P04637:R-HSA-")
        assert edge.provenance["extraction_method"] == "reactome_content_service_membership_lookup"


@patch("pbdata.graph.connectors.requests.get")
def test_reactome_protein_not_found(mock_get: MagicMock) -> None:
    mock_resp = MagicMock()
    mock_resp.status_code = 404
    mock_get.return_value = mock_resp

    connector = ReactomeConnector()
    nodes, edges = connector.fetch_pathways(["FAKE12345"])
    assert nodes == []
    assert edges == []


@patch("pbdata.graph.connectors.requests.get")
def test_reactome_empty_input(mock_get: MagicMock) -> None:
    connector = ReactomeConnector()
    nodes, edges = connector.fetch_pathways([])
    assert nodes == []
    assert edges == []
    mock_get.assert_not_called()


@patch("pbdata.graph.connectors.requests.get")
def test_reactome_deduplicates_shared_pathways(mock_get: MagicMock) -> None:
    """Two proteins in the same pathway should produce one pathway node."""
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = [
        {"stId": "R-HSA-109582", "displayName": "Hemostasis"},
    ]
    mock_resp.raise_for_status = MagicMock()
    mock_get.return_value = mock_resp

    connector = ReactomeConnector()
    nodes, edges = connector.fetch_pathways(["P04637", "P38398"])

    # Same pathway from two proteins — one node, two edges
    assert len(nodes) == 1
    assert len(edges) == 2
    assert nodes[0].node_id == "pathway:R-HSA-109582"


@patch("pbdata.graph.connectors.requests.get")
def test_reactome_api_failure_skips_protein(mock_get: MagicMock) -> None:
    mock_get.side_effect = ConnectionError("API down")

    connector = ReactomeConnector()
    nodes, edges = connector.fetch_pathways(["P04637"])
    assert nodes == []
    assert edges == []


@patch("pbdata.graph.connectors.requests.get")
def test_reactome_empty_pathway_list(mock_get: MagicMock) -> None:
    """Protein exists but has no annotated pathways."""
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = []
    mock_resp.raise_for_status = MagicMock()
    mock_get.return_value = mock_resp

    connector = ReactomeConnector()
    nodes, edges = connector.fetch_pathways(["P04637"])
    assert nodes == []
    assert edges == []


# ---------------------------------------------------------------------------
# BioGRID connector
# ---------------------------------------------------------------------------

_BIOGRID_RESPONSE = {
    "1": {
        "BIOGRID_INTERACTION_ID": "12345",
        "OFFICIAL_SYMBOL_A": "TP53",
        "OFFICIAL_SYMBOL_B": "MDM2",
        "SYSTEMATIC_NAME_A": "TP53",
        "SYSTEMATIC_NAME_B": "MDM2",
        "EXPERIMENTAL_SYSTEM": "Two-hybrid",
        "PUBMED_ID": "9876543",
    },
    "2": {
        "BIOGRID_INTERACTION_ID": "12346",
        "OFFICIAL_SYMBOL_A": "TP53",
        "OFFICIAL_SYMBOL_B": "BRCA1",
        "SYSTEMATIC_NAME_A": "TP53",
        "SYSTEMATIC_NAME_B": "BRCA1",
        "EXPERIMENTAL_SYSTEM": "Affinity Capture-MS",
        "PUBMED_ID": "1234567",
    },
}


@patch("pbdata.graph.connectors.requests.get")
def test_biogrid_fetch_interactions(mock_get: MagicMock) -> None:
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = _BIOGRID_RESPONSE
    mock_resp.raise_for_status = MagicMock()
    mock_get.return_value = mock_resp

    connector = BioGRIDConnector(access_key="test_key")
    nodes, edges = connector.fetch_interactions(["TP53"])

    assert len(nodes) == 3  # TP53, MDM2, BRCA1
    assert len(edges) == 2

    for edge in edges:
        assert edge.edge_type == "ProteinProteinInteraction"
        assert edge.source_database == "BioGRID"


def test_biogrid_no_access_key_returns_empty() -> None:
    connector = BioGRIDConnector(access_key="")
    nodes, edges = connector.fetch_interactions(["TP53"])
    assert nodes == []
    assert edges == []


def test_biogrid_empty_input_returns_empty() -> None:
    connector = BioGRIDConnector(access_key="key")
    nodes, edges = connector.fetch_interactions([])
    assert nodes == []
    assert edges == []


@patch("pbdata.graph.connectors.requests.get")
def test_biogrid_api_failure(mock_get: MagicMock) -> None:
    mock_get.side_effect = ConnectionError("API down")

    connector = BioGRIDConnector(access_key="key")
    nodes, edges = connector.fetch_interactions(["TP53"])
    assert nodes == []
    assert edges == []


@patch("pbdata.graph.connectors.requests.get")
def test_biogrid_malformed_response(mock_get: MagicMock) -> None:
    """Non-dict interactions should be skipped gracefully."""
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {
        "1": "not a dict",
        "2": {"OFFICIAL_SYMBOL_A": "", "OFFICIAL_SYMBOL_B": ""},  # empty names
        "3": {
            "BIOGRID_INTERACTION_ID": "99",
            "OFFICIAL_SYMBOL_A": "VALID_A",
            "OFFICIAL_SYMBOL_B": "VALID_B",
            "SYSTEMATIC_NAME_A": "",
            "SYSTEMATIC_NAME_B": "",
            "EXPERIMENTAL_SYSTEM": "Co-crystal",
            "PUBMED_ID": "",
        },
    }
    mock_resp.raise_for_status = MagicMock()
    mock_get.return_value = mock_resp

    connector = BioGRIDConnector(access_key="key")
    nodes, edges = connector.fetch_interactions(["test"])

    assert len(nodes) == 2  # VALID_A and VALID_B
    assert len(edges) == 1


# ---------------------------------------------------------------------------
# ConnectorRegistry
# ---------------------------------------------------------------------------


def test_connector_registry_defaults() -> None:
    reg = ConnectorRegistry()
    enabled = reg.enabled_connectors()
    assert "string" in enabled
    assert "reactome" in enabled
    assert "biogrid" not in enabled  # no access key


def test_connector_registry_with_biogrid_key() -> None:
    reg = ConnectorRegistry(biogrid=BioGRIDConnector(access_key="test"))
    assert "biogrid" in reg.enabled_connectors()


# ---------------------------------------------------------------------------
# Graph builder integration with external sources
# ---------------------------------------------------------------------------


@patch("pbdata.graph.connectors.requests.get")
@patch("pbdata.graph.connectors.requests.post")
def test_build_graph_with_external_sources(mock_post: MagicMock, mock_get: MagicMock) -> None:
    """Full integration: extracted + STRING + Reactome merge."""
    from pbdata.graph.builder import build_graph_from_extracted

    # Mock STRING
    string_resp = MagicMock()
    string_resp.status_code = 200
    string_resp.text = (
        "stringId_A\tstringId_B\tpreferredName_A\tpreferredName_B\tncbiTaxonId\tscore\n"
        "9606.ENSP1\t9606.ENSP2\tP12345\tQ99999\t9606\t900\n"
    )
    string_resp.raise_for_status = MagicMock()
    mock_post.return_value = string_resp

    # Mock Reactome
    reactome_resp = MagicMock()
    reactome_resp.status_code = 200
    reactome_resp.json.return_value = [
        {"stId": "R-HSA-12345", "displayName": "Test Pathway"},
    ]
    reactome_resp.raise_for_status = MagicMock()
    mock_get.return_value = reactome_resp

    # Set up extracted data
    tmp_root = _LOCAL_TMP / f"{uuid4().hex}_graph_ext"
    extracted = tmp_root / "data" / "extracted"
    for name in ["entry", "chains", "bound_objects", "interfaces", "assays"]:
        (extracted / name).mkdir(parents=True, exist_ok=True)

    (extracted / "entry" / "1ABC.json").write_text(json.dumps({
        "pdb_id": "1ABC",
        "source_record_id": "1ABC",
        "source_database": "RCSB",
        "assembly_id": "1",
    }), encoding="utf-8")
    (extracted / "chains" / "1ABC.json").write_text(json.dumps([
        {
            "pdb_id": "1ABC", "chain_id": "A", "is_protein": True,
            "uniprot_id": "P12345", "chain_description": "Kinase",
        },
        {
            "pdb_id": "1ABC", "chain_id": "B", "is_protein": True,
            "uniprot_id": "Q99999", "chain_description": "Adaptor",
        },
    ]), encoding="utf-8")
    (extracted / "bound_objects" / "1ABC.json").write_text("[]", encoding="utf-8")
    (extracted / "interfaces" / "1ABC.json").write_text("[]", encoding="utf-8")
    (extracted / "assays" / "1ABC.json").write_text("[]", encoding="utf-8")

    output_dir = tmp_root / "data" / "graph"
    nodes_path, edges_path, manifest_path = build_graph_from_extracted(
        extracted, output_dir,
        enable_external=True,
        enable_string=True,
        enable_reactome=True,
    )

    nodes = json.loads(nodes_path.read_text(encoding="utf-8"))
    edges = json.loads(edges_path.read_text(encoding="utf-8"))
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    # Should have protein nodes from extraction + any new from STRING
    protein_nodes = [n for n in nodes if n["node_type"] == "Protein"]
    assert len(protein_nodes) >= 2  # P12345 and Q99999

    # Should have pathway nodes from Reactome
    pathway_nodes = [n for n in nodes if n["node_type"] == "Pathway"]
    assert len(pathway_nodes) >= 1

    # Should have ProteinPathway edges
    pathway_edges = [e for e in edges if e["edge_type"] == "ProteinPathway"]
    assert len(pathway_edges) >= 1

    # Manifest should reflect external sources
    assert manifest["status"] == "materialized_with_external"
    assert "STRING" in manifest["external_sources_merged"]
    assert "Reactome" in manifest["external_sources_merged"]


def test_build_graph_without_external_sources() -> None:
    """Without enable_external, graph should only have extracted data."""
    from pbdata.graph.builder import build_graph_from_extracted

    tmp_root = _LOCAL_TMP / f"{uuid4().hex}_graph_noext"
    extracted = tmp_root / "data" / "extracted"
    for name in ["entry", "chains", "bound_objects", "interfaces", "assays"]:
        (extracted / name).mkdir(parents=True, exist_ok=True)

    (extracted / "entry" / "1ABC.json").write_text(json.dumps({
        "pdb_id": "1ABC", "source_record_id": "1ABC",
    }), encoding="utf-8")
    (extracted / "chains" / "1ABC.json").write_text(json.dumps([
        {"pdb_id": "1ABC", "chain_id": "A", "is_protein": True, "uniprot_id": "P12345"},
    ]), encoding="utf-8")
    (extracted / "bound_objects" / "1ABC.json").write_text("[]", encoding="utf-8")
    (extracted / "interfaces" / "1ABC.json").write_text("[]", encoding="utf-8")
    (extracted / "assays" / "1ABC.json").write_text("[]", encoding="utf-8")

    output_dir = tmp_root / "data" / "graph"
    _, _, manifest_path = build_graph_from_extracted(extracted, output_dir)

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["status"] == "materialized_from_extracted"
    assert manifest["external_sources_merged"] == []


def test_build_graph_skips_malformed_pair_keys() -> None:
    """Pair keys with wrong number of parts should be skipped."""
    from pbdata.graph.builder import build_graph_from_extracted

    tmp_root = _LOCAL_TMP / f"{uuid4().hex}_graph_badkeys"
    extracted = tmp_root / "data" / "extracted"
    for name in ["entry", "chains", "bound_objects", "interfaces", "assays"]:
        (extracted / name).mkdir(parents=True, exist_ok=True)

    (extracted / "entry" / "1ABC.json").write_text(json.dumps({
        "pdb_id": "1ABC", "source_record_id": "1ABC",
    }), encoding="utf-8")
    (extracted / "chains" / "1ABC.json").write_text(json.dumps([
        {"pdb_id": "1ABC", "chain_id": "A", "is_protein": True, "uniprot_id": "P12345"},
    ]), encoding="utf-8")
    (extracted / "bound_objects" / "1ABC.json").write_text(json.dumps([
        {"pdb_id": "1ABC", "component_id": "ATP"},
    ]), encoding="utf-8")
    (extracted / "interfaces" / "1ABC.json").write_text("[]", encoding="utf-8")
    # Mix of valid and malformed pair keys
    (extracted / "assays" / "1ABC.json").write_text(json.dumps([
        {
            "pdb_id": "1ABC",
            "pair_identity_key": "protein_ligand|1ABC|A|ATP|wt",
            "binding_affinity_type": "Kd",
            "binding_affinity_value": 5.0,
        },
        {
            "pdb_id": "1ABC",
            "pair_identity_key": "protein_ligand|1ABC|A",  # only 3 parts
            "binding_affinity_type": "Kd",
            "binding_affinity_value": 3.0,
        },
        {
            "pdb_id": "1ABC",
            "pair_identity_key": "protein_ligand|1ABC|A|ATP|wt|extra",  # 6 parts
            "binding_affinity_type": "Ki",
            "binding_affinity_value": 2.0,
        },
    ]), encoding="utf-8")

    output_dir = tmp_root / "data" / "graph"
    _, edges_path, _ = build_graph_from_extracted(extracted, output_dir)

    edges = json.loads(edges_path.read_text(encoding="utf-8"))
    # Only the valid 5-part pair key should produce an assay edge
    assay_edges = [e for e in edges if e["edge_id"].startswith("assay_pli:")]
    assert len(assay_edges) == 1
