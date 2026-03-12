import json
import os
from pathlib import Path
from uuid import uuid4

import pytest
from typer.testing import CliRunner

from pbdata.cli import app
from pbdata.models import plan_affinity_models, plan_off_target_models
from pbdata.prediction import plan_ligand_screening, plan_peptide_binding, plan_variant_effects
from pbdata.risk import plan_pathway_reasoning, plan_severity_scoring
from pbdata.sources.alphafold import plan_alphafold_state
from pbdata.features.builder import build_features_from_extracted_and_graph
from pbdata.features.mm_features import plan_mm_features
from pbdata.features.pathway import plan_pathway_features, summarize_pathway_features
from pbdata.reports.bias import build_bias_report
from pbdata.graph.builder import build_graph_from_extracted
from pbdata.graph.connectors import connector_stub
from pbdata.graph.identifier_map import map_protein_identifier
from pbdata.qa.scenario_runner import run_scenario_templates
from pbdata.schemas.features import FeatureRecord
from pbdata.schemas.graph import GraphEdgeRecord, GraphNodeRecord
from pbdata.schemas.conformation import ConformationStateRecord
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
    node = GraphNodeRecord(
        node_id="P12345",
        node_type="Protein",
        primary_id="P12345",
        provenance={"source": "RCSB"},
    )
    edge = GraphEdgeRecord(
        edge_id="e1",
        edge_type="ProteinProteinInteraction",
        source_node_id="P12345",
        target_node_id="Q99999",
        source_database="STRING",
        provenance={"source": "STRING"},
    )
    assert node.node_type == "Protein"
    assert edge.edge_type == "ProteinProteinInteraction"
    assert node.provenance["source"] == "RCSB"
    assert edge.provenance["source"] == "STRING"


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


def test_conformation_state_schema_validates() -> None:
    record = ConformationStateRecord(
        target_id="P12345",
        state_id="P12345:alphafold_planned",
        structure_source="AlphaFold",
        provenance={"source": "AlphaFold", "retrieved_at": "2026-03-09T00:00:00+00:00", "confidence": "planned"},
    )
    assert record.structure_source == "AlphaFold"


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


def test_feature_schema_rejects_unknown_key() -> None:
    with pytest.raises(Exception):
        FeatureRecord(
            feature_id="f1",
            pdb_id="1ABC",
            pair_identity_key="protein_ligand|1ABC|A|ATP|wt_or_unspecified",
            feature_group="training_ready_core",
            values={"unknown_feature_key": 2},
            provenance={"generated_at": "2026-03-08T00:00:00+00:00"},
        )


def test_graph_builder_emits_placeholder_pathway_node_without_external_sources() -> None:
    tmp_root = _tmp_dir("graph_placeholder")
    extracted_dir = tmp_root / "extracted"
    (extracted_dir / "entry").mkdir(parents=True, exist_ok=True)
    (extracted_dir / "chains").mkdir(parents=True, exist_ok=True)
    (extracted_dir / "bound_objects").mkdir(parents=True, exist_ok=True)
    (extracted_dir / "interfaces").mkdir(parents=True, exist_ok=True)
    (extracted_dir / "assays").mkdir(parents=True, exist_ok=True)
    (extracted_dir / "entry" / "1ABC.json").write_text(json.dumps({"pdb_id": "1ABC"}), encoding="utf-8")
    (extracted_dir / "chains" / "1ABC.json").write_text(
        json.dumps([{"pdb_id": "1ABC", "chain_id": "A", "is_protein": True, "uniprot_id": "P12345"}]),
        encoding="utf-8",
    )
    (extracted_dir / "bound_objects" / "1ABC.json").write_text("[]", encoding="utf-8")
    (extracted_dir / "interfaces" / "1ABC.json").write_text("[]", encoding="utf-8")
    (extracted_dir / "assays" / "1ABC.json").write_text("[]", encoding="utf-8")

    graph_dir = tmp_root / "graph"
    nodes_path, _, _ = build_graph_from_extracted(extracted_dir, graph_dir, enable_external=False)

    nodes = json.loads(nodes_path.read_text(encoding="utf-8"))
    assert any(node["node_type"] == "Pathway" for node in nodes)


def test_graph_builder_emits_structured_edge_provenance() -> None:
    tmp_root = _tmp_dir("graph_edge_provenance")
    extracted_dir = tmp_root / "extracted"
    output_dir = tmp_root / "graph"
    for name in ["entry", "chains", "bound_objects", "interfaces", "assays"]:
        (extracted_dir / name).mkdir(parents=True, exist_ok=True)

    (extracted_dir / "entry" / "1ABC.json").write_text(json.dumps({
        "pdb_id": "1ABC",
        "assembly_id": "1",
        "source_database": "RCSB",
    }), encoding="utf-8")
    (extracted_dir / "chains" / "1ABC.json").write_text(json.dumps([
        {
            "pdb_id": "1ABC",
            "chain_id": "A",
            "is_protein": True,
            "uniprot_id": "P12345",
        }
    ]), encoding="utf-8")
    (extracted_dir / "bound_objects" / "1ABC.json").write_text(json.dumps([
        {
            "pdb_id": "1ABC",
            "component_id": "ATP",
            "component_name": "ATP",
        }
    ]), encoding="utf-8")
    (extracted_dir / "interfaces" / "1ABC.json").write_text(json.dumps([
        {
            "pdb_id": "1ABC",
            "interface_type": "protein_ligand",
            "entity_name_b": "ATP",
            "binding_site_chain_ids": ["A"],
            "binding_site_residue_ids": ["A:TYR15"],
        }
    ]), encoding="utf-8")
    (extracted_dir / "assays" / "1ABC.json").write_text(json.dumps([
        {
            "pdb_id": "1ABC",
            "pair_identity_key": "protein_ligand|1ABC|A|ATP|wt",
            "source_database": "PDBbind",
            "binding_affinity_type": "Kd",
            "binding_affinity_value": 5.0,
            "binding_affinity_unit": "nM",
        }
    ]), encoding="utf-8")

    _, edges_path, _ = build_graph_from_extracted(extracted_dir, output_dir)
    edges = json.loads(edges_path.read_text(encoding="utf-8"))

    assert edges
    for edge in edges:
        provenance = edge["provenance"]
        assert provenance["source"]
        assert provenance["confidence"]
        assert provenance["source_record_key"]
        assert provenance["extraction_method"]
        assert provenance["retrieved_at"]


def test_remaining_scope_stubs_are_importable_and_explicit() -> None:
    connector = connector_stub("STRING", Path("data/raw/graph_sources/STRING"))
    mapping = map_protein_identifier("P12345", resolve_remote=False)
    pathway = plan_pathway_features("protein:P12345")
    mm_plan = plan_mm_features("1ABC")
    alphafold_plan = plan_alphafold_state("P12345")
    affinity_plan = plan_affinity_models()
    off_target_plan = plan_off_target_models()
    ligand_screening_plan = plan_ligand_screening()
    peptide_binding_plan = plan_peptide_binding()
    variant_effects_plan = plan_variant_effects()
    pathway_reasoning_plan = plan_pathway_reasoning()
    severity_plan = plan_severity_scoring()
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
    assert alphafold_plan.status == "planned"
    assert affinity_plan.status == "baseline_heuristic_available"
    assert off_target_plan.status == "baseline_heuristic_available"
    assert ligand_screening_plan.status == "stub"
    assert peptide_binding_plan.status == "stub"
    assert variant_effects_plan.status == "stub"
    assert pathway_reasoning_plan.status == "stub"
    assert severity_plan.status == "stub"
    assert training_plan.status == "stub"


def test_pathway_summary_reads_graph_edges() -> None:
    tmp_root = _tmp_dir("pathway_summary")
    graph_dir = tmp_root / "graph"
    graph_dir.mkdir(parents=True, exist_ok=True)
    (graph_dir / "graph_nodes.json").write_text(json.dumps([
        {"node_id": "protein:P12345", "node_type": "Protein", "primary_id": "P12345"},
        {"node_id": "pathway:R-HSA-1", "node_type": "Pathway", "primary_id": "R-HSA-1"},
    ]), encoding="utf-8")
    (graph_dir / "graph_edges.json").write_text(json.dumps([
        {"edge_id": "e1", "edge_type": "ProteinPathway", "source_node_id": "protein:P12345", "target_node_id": "pathway:R-HSA-1", "source_database": "Reactome"},
    ]), encoding="utf-8")

    summary = summarize_pathway_features("protein:P12345", graph_dir)

    assert summary.pathway_count == 1
    assert summary.status == "from_graph_pathways"


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


def test_scenario_runner_writes_reports() -> None:
    tmp = _tmp_dir("scenario_runner")
    scenario_yaml = tmp / "scenario_test_templates.yaml"
    rubric = tmp / "undesirable_state_rubric.md"
    scenario_yaml.write_text(
        "scenarios:\n  example:\n    goal: demo\n    expected_outputs: [ranked_target_list]\n    forbidden_behaviors: [silent_failure]\n",
        encoding="utf-8",
    )
    rubric.write_text("severity\nlocation\ndescription\nsuggested_fix\n", encoding="utf-8")

    prediction_dir = tmp / "data" / "prediction" / "ligand_screening"
    prediction_dir.mkdir(parents=True, exist_ok=True)
    (prediction_dir / "prediction_manifest.json").write_text(
        json.dumps({
            "status": "scaffold_only_no_predictions",
            "ranked_target_list": [],
            "predicted_kd": None,
            "predicted_delta_g": None,
            "confidence_score": None,
        }),
        encoding="utf-8",
    )

    report_path, manifest_path = run_scenario_templates(scenario_yaml, rubric, tmp / "data" / "qa_out")
    reports = json.loads(report_path.read_text(encoding="utf-8"))
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    assert reports[0]["scenario_id"] == "example"
    assert reports[0]["missing_expected_outputs"] == ["ranked_target_list"]
    assert manifest["status"] == "scenario_templates_loaded"


def test_scenario_runner_executes_workflows_when_inputs_are_available() -> None:
    tmp = _tmp_dir("scenario_runner_exec")
    scenario_yaml = tmp / "scenario_test_templates.yaml"
    rubric = tmp / "undesirable_state_rubric.md"
    scenario_yaml.write_text(
        "scenarios:\n  ligand:\n    goal: demo\n    expected_outputs: [ranked_target_list, confidence_score, pathway_risk_summary]\n",
        encoding="utf-8",
    )
    rubric.write_text("severity\nlocation\ndescription\nsuggested_fix\n", encoding="utf-8")
    (tmp / "data" / "extracted" / "bound_objects").mkdir(parents=True, exist_ok=True)
    (tmp / "data" / "extracted" / "bound_objects" / "1ABC.json").write_text(
        json.dumps([{"component_smiles": "CCO"}]),
        encoding="utf-8",
    )
    (tmp / "master_pdb_pairs.csv").write_text(
        "pdb_id,pair_identity_key,receptor_uniprot_ids,reported_measurement_mean_log10_standardized,source_conflict_flag,source_agreement_band,source_database,selected_preferred_source,ligand_key\n"
        "1ABC,protein_ligand|1ABC|A|ATP|wt,P12345,0.7,false,high,PDBbind,PDBbind,ATP\n",
        encoding="utf-8",
    )
    (tmp / "data" / "extracted" / "bound_objects" / "1ABC.json").write_text(
        json.dumps([{"component_id": "ATP", "component_smiles": "CCO"}]),
        encoding="utf-8",
    )

    report_path, manifest_path = run_scenario_templates(
        scenario_yaml,
        rubric,
        tmp / "data" / "qa_out",
        execute_workflows=True,
    )

    reports = json.loads(report_path.read_text(encoding="utf-8"))
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert any("ligand_screening_executed" in step for step in reports[0]["steps_taken"])
    assert manifest["status"] == "scenario_templates_executed"


def test_bias_and_conformation_cli_commands_write_outputs() -> None:
    tmp_root = _tmp_dir("bias_and_conformations")
    extracted = tmp_root / "data" / "extracted"
    for name in ["entry", "chains", "bound_objects"]:
        (extracted / name).mkdir(parents=True, exist_ok=True)
    (extracted / "entry" / "1ABC.json").write_text(json.dumps({
        "pdb_id": "1ABC",
        "task_hint": "protein_ligand",
        "organism_names": ["Homo sapiens"],
        "experimental_method": "X-RAY DIFFRACTION",
        "resolution_bin": "medium_res_1.5-2.5",
        "structure_file_cif_path": "C:/tmp/1ABC.cif",
        "downloaded_at": "2026-03-09T00:00:00+00:00",
    }), encoding="utf-8")
    (extracted / "chains" / "1ABC.json").write_text(json.dumps([
        {"pdb_id": "1ABC", "chain_id": "A", "is_protein": True, "uniprot_id": "P12345"},
    ]), encoding="utf-8")
    (extracted / "bound_objects" / "1ABC.json").write_text(json.dumps([
        {"pdb_id": "1ABC", "component_id": "ATP", "component_inchikey": "ATP-KEY"},
    ]), encoding="utf-8")

    runner = CliRunner()
    result = runner.invoke(app, ["--storage-root", str(tmp_root), "report-bias"], catch_exceptions=False)
    assert result.exit_code == 0
    assert (tmp_root / "data" / "reports" / "bias_report.json").exists()

    result = runner.invoke(app, ["--storage-root", str(tmp_root), "build-conformational-states"], catch_exceptions=False)
    assert result.exit_code == 0
    assert (tmp_root / "data" / "conformations" / "conformation_states.json").exists()

    result = runner.invoke(app, ["--storage-root", str(tmp_root), "run-scenario-tests"], catch_exceptions=False)
    assert result.exit_code == 0
    assert (tmp_root / "data" / "qa" / "scenario_test_report.json").exists()


def test_bias_report_separates_missing_data_from_real_categories() -> None:
    tmp_root = _tmp_dir("bias_report")
    extracted = tmp_root / "extracted"
    (extracted / "entry").mkdir(parents=True, exist_ok=True)
    (extracted / "bound_objects").mkdir(parents=True, exist_ok=True)
    (extracted / "entry" / "1ABC.json").write_text(
        json.dumps({"pdb_id": "1ABC", "organism_names": []}),
        encoding="utf-8",
    )
    (extracted / "bound_objects" / "1ABC.json").write_text(
        json.dumps([{}]),
        encoding="utf-8",
    )

    _, report = build_bias_report(extracted, tmp_root / "reports")

    assert report["missing_data_count"]["task_hint"] == 1
    assert report["missing_data_count"]["organism_names"] == 1
    assert report["missing_data_count"]["ligand_scaffold_identifier"] == 1
    assert "unknown" not in report["protein_family_distribution"]


def test_prediction_and_risk_cli_commands_write_outputs() -> None:
    tmp_root = _tmp_dir("prediction_and_risk")
    (tmp_root / "model_ready_pairs.csv").write_text(
        "pdb_id,pair_identity_key,receptor_uniprot_ids,binding_affinity_type,source_conflict_flag\n"
        "1ABC,protein_ligand|1ABC|A|ATP|wt,P12345; Q99999,Kd,false\n",
        encoding="utf-8",
    )
    (tmp_root / "scientific_coverage_summary.json").write_text(
        json.dumps({"counts": {"pair_count": 1}}),
        encoding="utf-8",
    )
    graph_dir = tmp_root / "data" / "graph"
    graph_dir.mkdir(parents=True, exist_ok=True)
    (graph_dir / "graph_nodes.json").write_text("[]", encoding="utf-8")
    (graph_dir / "graph_edges.json").write_text("[]", encoding="utf-8")

    structure_path = tmp_root / "demo.cif"
    structure_path.write_text("data_demo\n#", encoding="utf-8")

    runner = CliRunner()
    result = runner.invoke(
        app,
        ["--storage-root", str(tmp_root), "predict-ligand-screening", "--smiles", "CCO"],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    assert (tmp_root / "data" / "prediction" / "ligand_screening" / "prediction_manifest.json").exists()

    result = runner.invoke(
        app,
        ["--storage-root", str(tmp_root), "predict-peptide-binding", "--structure-file", str(structure_path)],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    assert (tmp_root / "data" / "prediction" / "peptide_binding" / "prediction_manifest.json").exists()

    result = runner.invoke(
        app,
        ["--storage-root", str(tmp_root), "score-pathway-risk", "--targets", "P12345"],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    risk_path = tmp_root / "data" / "risk" / "pathway_risk_summary.json"
    assert risk_path.exists()
    risk_body = json.loads(risk_path.read_text(encoding="utf-8"))
    assert risk_body["matching_pair_count"] == 1


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
