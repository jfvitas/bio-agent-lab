import json
from pathlib import Path
import csv

from typer.testing import CliRunner

from pbdata.cli import app
from pbdata.data_pipeline.workflow_engine import harvest_unified_metadata, initialize_workspace
from pbdata.dataset.engineering import DatasetEngineeringConfig, engineer_dataset
from pbdata.graph.structural_graphs import build_structural_graphs
from pbdata.storage import build_storage_layout
from tests.test_feature_execution import _tmp_dir, _write_extracted_fixture


def test_initialize_workspace_and_harvest_metadata() -> None:
    tmp_root = _tmp_dir("workflow_engine")
    layout = build_storage_layout(tmp_root)
    _write_extracted_fixture(layout)

    workspace_artifacts = initialize_workspace(layout)
    harvest_artifacts = harvest_unified_metadata(layout)

    assert Path(workspace_artifacts["workflow_manifest"]).exists()
    assert Path(harvest_artifacts["metadata_csv"]).exists()
    assert Path(harvest_artifacts["manifest"]).exists()
    assert Path(harvest_artifacts["source_annotation_summary_json"]).exists()
    assert Path(harvest_artifacts["source_annotation_summary_md"]).exists()
    manifest = json.loads(Path(harvest_artifacts["manifest"]).read_text(encoding="utf-8"))
    assert "annotation_caches" in manifest
    assert "uniprot" in manifest["annotation_caches"]


def test_build_structural_graphs_and_engineer_dataset() -> None:
    tmp_root = _tmp_dir("workflow_graph_dataset")
    layout = build_storage_layout(tmp_root)
    _write_extracted_fixture(layout)
    initialize_workspace(layout)
    harvest_unified_metadata(layout)

    graph_artifacts = build_structural_graphs(
        layout,
        graph_level="residue",
        scope="whole_protein",
        export_formats=("pyg", "networkx"),
    )
    dataset_artifacts = engineer_dataset(
        layout,
        config=DatasetEngineeringConfig(
            dataset_name="bench",
            test_frac=0.5,
            cv_folds=2,
            cluster_count=2,
            seed=7,
        ),
    )

    assert Path(graph_artifacts["manifest"]).exists()
    assert any(key.endswith("_summary") for key in graph_artifacts)
    assert Path(dataset_artifacts["train_csv"]).exists()
    assert Path(dataset_artifacts["test_csv"]).exists()
    assert Path(dataset_artifacts["dataset_config"]).exists()
    assert Path(dataset_artifacts["feature_schema"]).exists()
    assert Path(dataset_artifacts["graph_config"]).exists()
    assert Path(dataset_artifacts["cv_folds_dir"]).exists()

    graph_config = json.loads(Path(dataset_artifacts["graph_config"]).read_text(encoding="utf-8"))
    diversity_report = json.loads(Path(dataset_artifacts["diversity_report"]).read_text(encoding="utf-8"))
    train_rows = Path(dataset_artifacts["train_csv"]).read_text(encoding="utf-8")

    assert graph_config["latest_graph_manifest"].endswith("graph_manifest.json")
    assert graph_config["graph_level"] == "residue"
    assert graph_config["graph_scope"] == "whole_protein"
    assert diversity_report["graph_covered_row_count"] >= 1
    assert "graph_available" in train_rows


def test_build_atom_structural_graphs_includes_graph_summaries() -> None:
    tmp_root = _tmp_dir("workflow_atom_graphs")
    layout = build_storage_layout(tmp_root)
    _write_extracted_fixture(layout)
    initialize_workspace(layout)

    graph_artifacts = build_structural_graphs(
        layout,
        graph_level="atom",
        scope="whole_protein",
        export_formats=("pyg",),
    )

    summary_paths = [Path(value) for key, value in graph_artifacts.items() if key.endswith("_summary")]
    assert summary_paths
    summary = json.loads(summary_paths[0].read_text(encoding="utf-8"))
    assert summary["graph_level"] == "atom"
    assert "metal_coordination_count" in summary


def test_cli_workflow_graph_and_dataset_commands() -> None:
    runner = CliRunner()
    storage_root = _tmp_dir("workflow_cli")
    layout = build_storage_layout(storage_root)
    _write_extracted_fixture(layout)

    result_workspace = runner.invoke(
        app,
        ["--storage-root", str(storage_root), "setup-workspace"],
        catch_exceptions=False,
    )
    result_harvest = runner.invoke(
        app,
        ["--storage-root", str(storage_root), "harvest-metadata"],
        catch_exceptions=False,
    )
    result_graphs = runner.invoke(
        app,
        [
            "--storage-root",
            str(storage_root),
            "build-structural-graphs",
            "--graph-level",
            "residue",
            "--scope",
            "whole_protein",
            "--export-format",
            "pyg",
        ],
        catch_exceptions=False,
    )
    result_dataset = runner.invoke(
        app,
        [
            "--storage-root",
            str(storage_root),
            "engineer-dataset",
            "--dataset-name",
            "cli_bench",
            "--test-frac",
            "0.5",
            "--cluster-count",
            "2",
        ],
        catch_exceptions=False,
    )

    assert result_workspace.exit_code == 0
    assert result_harvest.exit_code == 0
    assert result_graphs.exit_code == 0
    assert result_dataset.exit_code == 0


def test_engineer_dataset_aligns_to_custom_training_selection() -> None:
    tmp_root = _tmp_dir("workflow_custom_training_alignment")
    layout = build_storage_layout(tmp_root)
    initialize_workspace(layout)
    for name in ["entry", "chains", "assays"]:
        (layout.extracted_dir / name).mkdir(parents=True, exist_ok=True)
    (layout.extracted_dir / "entry" / "1ABC.json").write_text(json.dumps({
        "pdb_id": "1ABC",
        "experimental_method": "X-RAY DIFFRACTION",
        "structure_resolution": 2.0,
        "organism_names": ["Homo sapiens"],
    }), encoding="utf-8")
    (layout.extracted_dir / "entry" / "2DEF.json").write_text(json.dumps({
        "pdb_id": "2DEF",
        "experimental_method": "X-RAY DIFFRACTION",
        "structure_resolution": 2.5,
        "organism_names": ["Mus musculus"],
    }), encoding="utf-8")
    (layout.extracted_dir / "chains" / "1ABC.json").write_text(json.dumps([
        {"pdb_id": "1ABC", "chain_id": "A", "polymer_sequence": "M" * 180, "uniprot_id": "P12345"},
    ]), encoding="utf-8")
    (layout.extracted_dir / "chains" / "2DEF.json").write_text(json.dumps([
        {"pdb_id": "2DEF", "chain_id": "A", "polymer_sequence": "A" * 200, "uniprot_id": "Q99999"},
    ]), encoding="utf-8")
    (layout.extracted_dir / "assays" / "1ABC.json").write_text(json.dumps([
        {
            "pdb_id": "1ABC",
            "pair_identity_key": "protein_ligand|1ABC|A|ATP|wt",
            "source_database": "BindingDB",
            "binding_affinity_type": "Kd",
            "binding_affinity_value": 5.0,
            "receptor_chain_ids": "A",
            "receptor_uniprot_ids": "P12345",
            "ligand_types": "small_molecule",
            "matching_interface_types": "protein_ligand",
            "mutation_strings": "wt",
            "release_split": "train",
        },
    ]), encoding="utf-8")
    (layout.extracted_dir / "assays" / "2DEF.json").write_text(json.dumps([
        {
            "pdb_id": "2DEF",
            "pair_identity_key": "protein_ligand|2DEF|A|GTP|wt",
            "source_database": "BindingDB",
            "binding_affinity_type": "Kd",
            "binding_affinity_value": 8.0,
            "receptor_chain_ids": "A",
            "receptor_uniprot_ids": "Q99999",
            "ligand_types": "small_molecule",
            "matching_interface_types": "protein_ligand",
            "mutation_strings": "wt",
            "release_split": "test",
        },
    ]), encoding="utf-8")
    (tmp_root / "master_pdb_repository.csv").write_text(
        "pdb_id,experimental_method,structure_resolution,organism_names,oligomeric_state\n"
        "1ABC,X-RAY DIFFRACTION,2.0,Homo sapiens,1.10.510.10\n"
        "2DEF,X-RAY DIFFRACTION,2.5,Mus musculus,2.40.50.140\n",
        encoding="utf-8",
    )
    (tmp_root / "master_pdb_pairs.csv").write_text(
        "pdb_id,pair_identity_key,source_database,binding_affinity_type,binding_affinity_value,receptor_chain_ids,receptor_uniprot_ids,receptor_organisms,ligand_types,matching_interface_types,mutation_strings,release_split\n"
        "1ABC,protein_ligand|1ABC|A|ATP|wt,BindingDB,Kd,5,A,P12345,Homo sapiens,small_molecule,protein_ligand,wt,train\n"
        "2DEF,protein_ligand|2DEF|A|GTP|wt,BindingDB,Kd,8,A,Q99999,Mus musculus,small_molecule,protein_ligand,wt,test\n",
        encoding="utf-8",
    )
    (tmp_root / "custom_training_set.csv").write_text(
        "selection_rank,selection_mode,pdb_id,pair_identity_key,binding_affinity_type,source_database,receptor_uniprot_ids,receptor_chain_ids,ligand_types,matching_interface_types,mutation_strings,release_split\n"
        "1,generalist,1ABC,protein_ligand|1ABC|A|ATP|wt,Kd,BindingDB,P12345,A,small_molecule,protein_ligand,wt,train\n",
        encoding="utf-8",
    )

    harvest_unified_metadata(layout)
    dataset_artifacts = engineer_dataset(
        layout,
        config=DatasetEngineeringConfig(
            dataset_name="selected_only",
            test_frac=0.5,
            cluster_count=1,
            seed=5,
        ),
    )

    rows = []
    for split_name in ("train_csv", "test_csv"):
        with Path(dataset_artifacts[split_name]).open(newline="", encoding="utf-8") as handle:
            rows.extend(list(csv.DictReader(handle)))
    diversity_report = json.loads(Path(dataset_artifacts["diversity_report"]).read_text(encoding="utf-8"))

    assert len(rows) == 1
    assert rows[0]["pair_identity_key"] == "protein_ligand|1ABC|A|ATP|wt"
    assert rows[0]["dataset_source"] == "custom_training_set"
    assert diversity_report["dataset_source"] == "custom_training_set"
