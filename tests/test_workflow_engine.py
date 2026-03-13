import json
from pathlib import Path

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
