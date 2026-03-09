import json
import os
from pathlib import Path
from uuid import uuid4

from typer.testing import CliRunner

from pbdata.cli import app
from pbdata.features.builder import build_features_from_extracted_and_graph
from pbdata.features.mm_features import plan_mm_features
from pbdata.features.pathway import plan_pathway_features
from pbdata.graph.builder import build_graph_from_extracted
from pbdata.graph.connectors import connector_stub
from pbdata.graph.identifier_map import map_protein_identifier
from pbdata.schemas.features import FeatureRecord
from pbdata.schemas.graph import GraphEdgeRecord, GraphNodeRecord
from pbdata.schemas.training_example import (
    ExperimentFields,
    GraphFeatureFields,
    InteractionFields,
    LigandFields,
    ProteinFields,
    StructureFields,
    TrainingExampleRecord,
)
from pbdata.training.assembler import plan_training_assembly

_LOCAL_TMP = Path(__file__).parent / "_tmp"
_LOCAL_TMP.mkdir(exist_ok=True)


def _tmp_dir(name: str) -> Path:
    path = _LOCAL_TMP / f"{uuid4().hex}_{name}"
    path.mkdir(exist_ok=True)
    return path


def test_graph_schema_records_validate() -> None:
    node = GraphNodeRecord(node_id="P12345", node_type="Protein", primary_id="P12345")
    edge = GraphEdgeRecord(
        edge_id="e1",
        edge_type="ProteinProteinInteraction",
        source_node_id="P12345",
        target_node_id="Q99999",
        source_database="STRING",
    )
    assert node.node_type == "Protein"
    assert edge.edge_type == "ProteinProteinInteraction"


def test_training_example_schema_validates() -> None:
    record = TrainingExampleRecord(
        example_id="ex1",
        structure=StructureFields(pdb_id="1ABC"),
        protein=ProteinFields(uniprot_id="P12345"),
        ligand=LigandFields(ligand_id="ATP"),
        interaction=InteractionFields(interface_residues=["A:TYR15"]),
        experiment=ExperimentFields(affinity_type="Kd", affinity_value=5.0),
        graph_features=GraphFeatureFields(network_degree=12),
        provenance={"generated_at": "2026-03-08T00:00:00+00:00"},
    )
    assert record.example_id == "ex1"
    assert record.protein.uniprot_id == "P12345"


def test_feature_schema_validates() -> None:
    record = FeatureRecord(
        feature_id="f1",
        pdb_id="1ABC",
        pair_identity_key="protein_ligand|1ABC|A|ATP|wt_or_unspecified",
        feature_group="training_ready_core",
        values={"network_degree": 2},
        provenance={"generated_at": "2026-03-08T00:00:00+00:00"},
    )
    assert record.feature_group == "training_ready_core"


def test_remaining_scope_stubs_are_importable_and_explicit() -> None:
    connector = connector_stub("STRING", Path("data/raw/graph_sources/STRING"))
    mapping = map_protein_identifier("P12345", resolve_remote=False)
    pathway = plan_pathway_features("protein:P12345")
    mm_plan = plan_mm_features("1ABC")
    training_plan = plan_training_assembly(
        Path("data/extracted"),
        Path("data/features"),
        Path("data/graph"),
        Path("data/training_examples"),
    )

    assert connector.status == "stub"
    assert mapping.status == "stub"
    assert pathway.status == "stub"
    assert mm_plan.status == "stub"
    assert training_plan.status == "stub"


def test_architecture_cli_commands_write_manifests() -> None:
    runner = CliRunner()
    result = runner.invoke(
        app,
        ["build-graph"],
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    result = runner.invoke(
        app,
        ["build-features"],
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    result = runner.invoke(
        app,
        ["build-training-examples"],
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    graph_manifest = Path("data/graph/graph_manifest.json")
    feature_manifest = Path("data/features/feature_manifest.json")
    training_manifest = Path("data/training_examples/training_manifest.json")
    assert graph_manifest.exists()
    assert feature_manifest.exists()
    assert training_manifest.exists()

    graph_body = json.loads(graph_manifest.read_text(encoding="utf-8"))
    assert graph_body["status"] in {"planned", "materialized_from_extracted", "materialized_with_external"}


def test_build_graph_from_extracted_materializes_nodes_and_edges() -> None:
    tmp_root = _tmp_dir("graph_from_extracted")
    extracted = tmp_root / "data" / "extracted"
    for name in ["entry", "chains", "bound_objects", "interfaces", "assays"]:
        (extracted / name).mkdir(parents=True, exist_ok=True)

    (extracted / "entry" / "1ABC.json").write_text(json.dumps({
        "pdb_id": "1ABC",
        "source_record_id": "1ABC",
        "source_database": "RCSB",
        "assembly_id": "1",
        "title": "Example complex",
        "organism_names": ["Homo sapiens"],
    }), encoding="utf-8")
    (extracted / "chains" / "1ABC.json").write_text(json.dumps([
        {
            "pdb_id": "1ABC",
            "chain_id": "A",
            "is_protein": True,
            "uniprot_id": "P12345",
            "chain_description": "Kinase alpha",
            "entity_source_taxonomy_id": 9606,
            "entity_source_organism": "Homo sapiens",
        },
        {
            "pdb_id": "1ABC",
            "chain_id": "B",
            "is_protein": True,
            "uniprot_id": "Q99999",
            "chain_description": "Adaptor beta",
        },
    ]), encoding="utf-8")
    (extracted / "bound_objects" / "1ABC.json").write_text(json.dumps([
        {
            "pdb_id": "1ABC",
            "component_id": "ATP",
            "component_name": "ATP",
            "component_type": "small_molecule",
            "component_inchikey": "ATP-INCHIKEY",
            "component_smiles": "C1=NC",
        }
    ]), encoding="utf-8")
    (extracted / "interfaces" / "1ABC.json").write_text(json.dumps([
        {
            "pdb_id": "1ABC",
            "interface_type": "protein_protein",
            "partner_a_chain_ids": ["A"],
            "partner_b_chain_ids": ["B"],
        },
        {
            "pdb_id": "1ABC",
            "interface_type": "protein_ligand",
            "binding_site_chain_ids": ["A"],
            "binding_site_residue_ids": ["TYR15", "ASP34"],
            "entity_name_b": "ATP",
        },
    ]), encoding="utf-8")
    (extracted / "assays" / "1ABC.json").write_text(json.dumps([
        {
            "pdb_id": "1ABC",
            "source_database": "PDBbind",
            "pair_identity_key": "protein_ligand|1ABC|A|ATP-INCHIKEY|wt_or_unspecified",
            "binding_affinity_type": "Kd",
            "binding_affinity_value": 5.0,
            "binding_affinity_unit": "nM",
        }
    ]), encoding="utf-8")

    output_dir = tmp_root / "data" / "graph"
    nodes_path, edges_path, manifest_path = build_graph_from_extracted(extracted, output_dir)

    nodes = json.loads(nodes_path.read_text(encoding="utf-8"))
    edges = json.loads(edges_path.read_text(encoding="utf-8"))
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    assert any(node["node_type"] == "ProteinComplex" for node in nodes)
    assert any(node["node_type"] == "Protein" and node["uniprot_id"] == "P12345" for node in nodes)
    assert any(node["node_type"] == "Ligand" and node["primary_id"] == "ATP-INCHIKEY" for node in nodes)
    assert any(edge["edge_type"] == "ProteinProteinInteraction" for edge in edges)
    assert any(edge["edge_type"] == "ProteinLigandInteraction" for edge in edges)
    assert manifest["status"] == "materialized_from_extracted"


def test_build_graph_cli_materializes_from_extracted_when_present() -> None:
    tmp_root = _tmp_dir("graph_cli")
    extracted = tmp_root / "data" / "extracted" / "entry"
    extracted.mkdir(parents=True, exist_ok=True)
    (extracted / "1ABC.json").write_text(json.dumps({
        "pdb_id": "1ABC",
        "source_record_id": "1ABC",
    }), encoding="utf-8")

    runner = CliRunner()
    original_cwd = Path.cwd()
    os.chdir(tmp_root)
    try:
        result = runner.invoke(app, ["build-graph"], catch_exceptions=False)
    finally:
        os.chdir(original_cwd)

    assert result.exit_code == 0
    assert (tmp_root / "data" / "graph" / "graph_manifest.json").exists()


def test_build_features_from_extracted_and_graph_materializes_feature_rows() -> None:
    tmp_root = _tmp_dir("features_from_graph")
    extracted = tmp_root / "data" / "extracted"
    for name in ["entry", "chains", "bound_objects", "interfaces", "assays"]:
        (extracted / name).mkdir(parents=True, exist_ok=True)

    (extracted / "entry" / "1ABC.json").write_text(json.dumps({
        "pdb_id": "1ABC",
        "structure_resolution": 2.0,
        "multiligand_entry": False,
    }), encoding="utf-8")
    (extracted / "chains" / "1ABC.json").write_text(json.dumps([
        {"pdb_id": "1ABC", "chain_id": "A", "is_protein": True, "uniprot_id": "P12345"},
    ]), encoding="utf-8")
    (extracted / "bound_objects" / "1ABC.json").write_text(json.dumps([
        {"pdb_id": "1ABC", "component_id": "ATP", "component_molecular_weight": 507.0}
    ]), encoding="utf-8")
    (extracted / "interfaces" / "1ABC.json").write_text(json.dumps([
        {"pdb_id": "1ABC", "interface_type": "protein_ligand", "binding_site_residue_ids": ["TYR15", "ASP34"]}
    ]), encoding="utf-8")
    (extracted / "assays" / "1ABC.json").write_text(json.dumps([
        {
            "pdb_id": "1ABC",
            "pair_identity_key": "protein_ligand|1ABC|A|ATP|wt_or_unspecified",
            "binding_affinity_type": "Kd",
            "binding_affinity_value": 5.0,
            "binding_affinity_log10_standardized": 0.69897,
            "assay_temperature_c": 25.0,
            "assay_ph": 7.4,
        }
    ]), encoding="utf-8")

    graph_dir = tmp_root / "data" / "graph"
    graph_dir.mkdir(parents=True, exist_ok=True)
    (graph_dir / "graph_nodes.json").write_text(json.dumps([
        {"node_id": "protein:P12345", "node_type": "Protein", "primary_id": "P12345", "metadata": {"pdb_id": "1ABC"}},
        {"node_id": "ligand:ATP", "node_type": "Ligand", "primary_id": "ATP"},
    ]), encoding="utf-8")
    (graph_dir / "graph_edges.json").write_text(json.dumps([
        {
            "edge_id": "e1",
            "edge_type": "ProteinLigandInteraction",
            "source_node_id": "protein:P12345",
            "target_node_id": "ligand:ATP",
            "source_database": "RCSB",
        }
    ]), encoding="utf-8")

    output_dir = tmp_root / "data" / "features"
    features_path, manifest_path = build_features_from_extracted_and_graph(extracted, graph_dir, output_dir)
    rows = json.loads(features_path.read_text(encoding="utf-8"))
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    assert len(rows) == 1
    assert rows[0]["values"]["structure_resolution"] == 2.0
    assert rows[0]["values"]["network_degree"] == 1
    assert rows[0]["values"]["pathway_count"] == 0
    assert manifest["status"] == "materialized_from_extracted_and_graph"


def test_build_features_cli_materializes_when_inputs_exist() -> None:
    tmp_root = _tmp_dir("features_cli")
    extracted = tmp_root / "data" / "extracted" / "assays"
    extracted.mkdir(parents=True, exist_ok=True)
    (extracted / "1ABC.json").write_text("[]", encoding="utf-8")
    graph_dir = tmp_root / "data" / "graph"
    graph_dir.mkdir(parents=True, exist_ok=True)
    (graph_dir / "graph_edges.json").write_text("[]", encoding="utf-8")

    runner = CliRunner()
    original_cwd = Path.cwd()
    os.chdir(tmp_root)
    try:
        result = runner.invoke(app, ["build-features"], catch_exceptions=False)
    finally:
        os.chdir(original_cwd)

    assert result.exit_code == 0
    assert (tmp_root / "data" / "features" / "feature_manifest.json").exists()
