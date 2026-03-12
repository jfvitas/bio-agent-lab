import json
from pathlib import Path
from uuid import uuid4

from pbdata.graph.builder import build_graph_from_extracted

_LOCAL_TMP = Path(__file__).parent / "_tmp"
_LOCAL_TMP.mkdir(exist_ok=True)


def _tmp_dir(name: str) -> Path:
    path = _LOCAL_TMP / f"{uuid4().hex}_{name}"
    path.mkdir(exist_ok=True)
    return path


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_build_graph_from_extracted_merges_source_databases_for_duplicate_proteins() -> None:
    tmp_path = _tmp_dir("graph_builder_merge")
    extracted = tmp_path / "data" / "extracted"
    _write_json(extracted / "entry" / "1ABC.json", {"pdb_id": "1ABC", "source_record_id": "1ABC"})
    _write_json(extracted / "chains" / "1ABC.json", [
        {"pdb_id": "1ABC", "chain_id": "A", "is_protein": True, "uniprot_id": "P12345"},
        {"pdb_id": "1ABC", "chain_id": "B", "is_protein": True, "uniprot_id": "P12345"},
    ])
    _write_json(extracted / "bound_objects" / "1ABC.json", [])
    _write_json(extracted / "interfaces" / "1ABC.json", [
        {
            "pdb_id": "1ABC",
            "interface_type": "protein_protein",
            "partner_a_chain_ids": ["A"],
            "partner_b_chain_ids": ["B"],
        }
    ])
    _write_json(extracted / "assays" / "1ABC.json", [])

    nodes_path, edges_path, _ = build_graph_from_extracted(extracted, tmp_path / "data" / "graph")
    nodes = json.loads(nodes_path.read_text(encoding="utf-8"))
    edges = json.loads(edges_path.read_text(encoding="utf-8"))

    protein_nodes = [node for node in nodes if node["node_type"] == "Protein"]
    assert len(protein_nodes) == 1
    assert len([edge for edge in edges if edge["edge_type"] == "ProteinProteinInteraction"]) == 1


def test_build_graph_from_extracted_backfills_placeholder_pathway_when_none_exist() -> None:
    tmp_path = _tmp_dir("graph_builder_placeholder")
    extracted = tmp_path / "data" / "extracted"
    _write_json(extracted / "entry" / "1ABC.json", {"pdb_id": "1ABC", "source_record_id": "1ABC"})
    _write_json(extracted / "chains" / "1ABC.json", [])
    _write_json(extracted / "bound_objects" / "1ABC.json", [])
    _write_json(extracted / "interfaces" / "1ABC.json", [])
    _write_json(extracted / "assays" / "1ABC.json", [])

    nodes_path, _, _ = build_graph_from_extracted(extracted, tmp_path / "data" / "graph")
    nodes = json.loads(nodes_path.read_text(encoding="utf-8"))

    pathway_nodes = [node for node in nodes if node["node_type"] == "Pathway"]
    assert len(pathway_nodes) == 1
    assert pathway_nodes[0]["metadata"]["placeholder"] is True
