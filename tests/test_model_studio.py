import json
import subprocess
import sys
from pathlib import Path
from uuid import uuid4

import pytest

from pbdata.modeling.studio import (
    ModelStudioSelection,
    build_dataset_profile,
    build_starter_model_config,
    export_starter_model_config,
    recommend_model_architectures,
    validate_model_studio_selection,
)
from pbdata.modeling.runtime import detect_runtime_capabilities, export_training_package
from pbdata.modeling.graph_contract import build_graph_learning_contract
from pbdata.modeling.graph_dataset import materialize_graph_dataset_records
from pbdata.modeling.graph_native_backend import load_native_graph_samples
from pbdata.modeling.graph_pyg_adapter import build_pyg_ready_graph_samples
from pbdata.modeling.graph_samples import build_graph_sample_manifest
from pbdata.modeling.graph_training_payload import materialize_graph_training_payload
from pbdata.modeling.hybrid_training_payload import materialize_hybrid_training_payload
from pbdata.modeling.pyg_training import load_graph_training_records
from pbdata.modeling.pyg_training import load_hybrid_training_records
from pbdata.modeling.trainer_registry import resolve_trainer_backend
from pbdata.modeling.training_runs import (
    build_training_run_report,
    compare_training_runs,
    execute_training_run,
    import_training_run,
    inspect_training_run,
    run_saved_model_batch_inference,
    run_saved_model_inference,
)
from pbdata.storage import build_storage_layout

_LOCAL_TMP = Path(__file__).parent / "_tmp"
_LOCAL_TMP.mkdir(exist_ok=True)


def _tmp_dir(name: str) -> Path:
    path = _LOCAL_TMP / f"{uuid4().hex}_{name}"
    path.mkdir(exist_ok=True)
    return path


def test_build_dataset_profile_detects_graph_and_attribute_modalities() -> None:
    layout = build_storage_layout(_tmp_dir("model_studio_profile"))
    layout.training_dir.mkdir(parents=True, exist_ok=True)
    layout.graph_dir.mkdir(parents=True, exist_ok=True)
    layout.splits_dir.mkdir(parents=True, exist_ok=True)
    (layout.training_dir / "training_examples.json").write_text(json.dumps([{
        "example_id": "train:1ABC:0",
        "labels": {"binding_affinity_log10": 1.2, "affinity_type": "Kd"},
    }]), encoding="utf-8")
    (layout.graph_dir / "graph_nodes.json").write_text(json.dumps([{"node_id": "n1"}]), encoding="utf-8")
    (layout.graph_dir / "graph_edges.json").write_text(json.dumps([{"source_node_id": "n1", "target_node_id": "n1"}]), encoding="utf-8")
    (layout.splits_dir / "train.txt").write_text("train:1ABC:0\n", encoding="utf-8")
    (layout.splits_dir / "val.txt").write_text("val:1ABC:0\n", encoding="utf-8")
    (layout.splits_dir / "test.txt").write_text("test:1ABC:0\n", encoding="utf-8")

    profile = build_dataset_profile(layout)

    assert profile.example_count == 1
    assert profile.graph_ready is True
    assert profile.attribute_ready is True
    assert "graphs+attributes" in profile.modalities_available
    assert "regression" in profile.tasks_available
    assert "classification" in profile.tasks_available


def test_validate_model_studio_selection_flags_missing_graph_inputs() -> None:
    layout = build_storage_layout(_tmp_dir("model_studio_validate"))
    layout.training_dir.mkdir(parents=True, exist_ok=True)
    (layout.training_dir / "training_examples.json").write_text(json.dumps([{
        "example_id": "train:1ABC:0",
        "labels": {"binding_affinity_log10": 1.2},
    }]), encoding="utf-8")

    profile = build_dataset_profile(layout)
    messages = validate_model_studio_selection(
        profile,
        ModelStudioSelection(modality="graphs", task="regression", preferred_family="gnn"),
    )

    assert any(message.priority == "error" for message in messages)
    assert any("Graph artifacts" in message.title for message in messages)


def test_recommend_model_architectures_prefers_hybrid_when_both_modalities_exist() -> None:
    layout = build_storage_layout(_tmp_dir("model_studio_recommend"))
    layout.training_dir.mkdir(parents=True, exist_ok=True)
    layout.graph_dir.mkdir(parents=True, exist_ok=True)
    layout.splits_dir.mkdir(parents=True, exist_ok=True)
    (layout.training_dir / "training_examples.json").write_text(json.dumps([
        {"example_id": f"train:1ABC:{idx}", "labels": {"binding_affinity_log10": 1.0 + idx}}
        for idx in range(1200)
    ]), encoding="utf-8")
    (layout.graph_dir / "graph_nodes.json").write_text(json.dumps([{"node_id": "n1"}]), encoding="utf-8")
    (layout.graph_dir / "graph_edges.json").write_text(json.dumps([{"source_node_id": "n1", "target_node_id": "n1"}]), encoding="utf-8")
    (layout.splits_dir / "train.txt").write_text("\n".join(f"train:1ABC:{idx}" for idx in range(900)), encoding="utf-8")
    (layout.splits_dir / "val.txt").write_text("\n".join(f"val:1ABC:{idx}" for idx in range(150)), encoding="utf-8")
    (layout.splits_dir / "test.txt").write_text("\n".join(f"test:1ABC:{idx}" for idx in range(150)), encoding="utf-8")

    profile = build_dataset_profile(layout)
    recommendations = recommend_model_architectures(
        profile,
        ModelStudioSelection(modality="graphs+attributes", task="regression", compute_budget="high"),
    )

    assert len(recommendations) == 3
    assert recommendations[0].family in {"hybrid_fusion", "gnn", "xgboost"}
    assert any(rec.family == "hybrid_fusion" for rec in recommendations)


def test_recommend_model_architectures_returns_unsupervised_options() -> None:
    layout = build_storage_layout(_tmp_dir("model_studio_unsupervised"))
    profile = build_dataset_profile(layout)

    recommendations = recommend_model_architectures(
        profile,
        ModelStudioSelection(modality="unsupervised", task="unsupervised"),
    )

    assert len(recommendations) >= 1
    assert all(rec.supervision == "unsupervised" for rec in recommendations)


def test_build_and_export_starter_model_config() -> None:
    layout = build_storage_layout(_tmp_dir("model_studio_export"))
    layout.training_dir.mkdir(parents=True, exist_ok=True)
    layout.graph_dir.mkdir(parents=True, exist_ok=True)
    layout.splits_dir.mkdir(parents=True, exist_ok=True)
    (layout.training_dir / "training_examples.json").write_text(json.dumps([
        {"example_id": "train:1ABC:0", "labels": {"binding_affinity_log10": 1.2}},
    ]), encoding="utf-8")
    (layout.graph_dir / "graph_nodes.json").write_text(json.dumps([{"node_id": "n1"}]), encoding="utf-8")
    (layout.graph_dir / "graph_edges.json").write_text(json.dumps([{"source_node_id": "n1", "target_node_id": "n1"}]), encoding="utf-8")
    (layout.splits_dir / "train.txt").write_text("train:1ABC:0\n", encoding="utf-8")

    profile = build_dataset_profile(layout)
    recommendation = recommend_model_architectures(
        profile,
        ModelStudioSelection(modality="graphs+attributes", task="regression", compute_budget="high"),
    )[0]
    starter = build_starter_model_config(profile, recommendation, ModelStudioSelection())
    out_path = export_starter_model_config(layout, starter)

    assert starter.config["model_id"] == recommendation.model_id
    assert out_path.exists()
    exported = json.loads(out_path.read_text(encoding="utf-8"))
    assert exported["family"] == recommendation.family


def test_detect_runtime_capabilities_reports_local_cpu() -> None:
    runtime = detect_runtime_capabilities()

    assert runtime.local_cpu_available is True
    assert "local_cpu" in runtime.supported_targets
    assert runtime.python_version


def test_resolve_trainer_backend_prefers_expected_fallbacks() -> None:
    dense_plan = resolve_trainer_backend(
        "dense_nn",
        runtime_target="local_gpu",
        installed_backends=("torch",),
    )
    assert dense_plan.implementation == "native"
    assert dense_plan.backend_id == "torch_tabular_mlp"

    gnn_plan = resolve_trainer_backend(
        "gnn",
        runtime_target="local_cpu",
        installed_backends=("sklearn",),
    )
    assert gnn_plan.implementation == "surrogate"
    assert gnn_plan.execution_family == "dense_nn"

    xgb_plan = resolve_trainer_backend(
        "xgboost",
        runtime_target="local_cpu",
        installed_backends=("sklearn",),
    )
    assert xgb_plan.implementation == "fallback"
    assert xgb_plan.backend_id == "sklearn_hist_gradient_boosting"

    native_gnn_plan = resolve_trainer_backend(
        "gnn",
        runtime_target="local_gpu",
        installed_backends=("torch", "torch_geometric"),
        native_graph_contract_available=True,
    )
    assert native_gnn_plan.implementation == "native"
    assert native_gnn_plan.backend_id == "pyg_gnn"

    native_hybrid_plan = resolve_trainer_backend(
        "hybrid_fusion",
        runtime_target="local_gpu",
        installed_backends=("torch", "torch_geometric"),
        native_graph_contract_available=True,
    )
    assert native_hybrid_plan.implementation == "native"
    assert native_hybrid_plan.backend_id == "pyg_hybrid_fusion"


def test_export_training_package_writes_portable_files() -> None:
    layout = build_storage_layout(_tmp_dir("model_studio_package"))
    layout.training_dir.mkdir(parents=True, exist_ok=True)
    layout.splits_dir.mkdir(parents=True, exist_ok=True)
    (layout.training_dir / "training_examples.json").write_text(json.dumps([{
        "example_id": "train:1ABC:0",
        "structure": {"pdb_id": "1ABC"},
        "labels": {"binding_affinity_log10": 1.2},
    }]), encoding="utf-8")
    (layout.splits_dir / "train.txt").write_text("RCSB_1ABC\n", encoding="utf-8")
    starter_config = {
        "model_id": "xgboost_hybrid_baseline",
        "family": "xgboost",
        "model": {"type": "xgboost"},
    }

    out_dir = export_training_package(
        layout,
        starter_config=starter_config,
        target="kaggle",
        package_name="demo_package",
    )

    assert (out_dir / "config.json").exists()
    assert (out_dir / "requirements.txt").exists()
    assert (out_dir / "train.py").exists()
    assert (out_dir / "trainer_backend.py").exists()
    assert (out_dir / "import_run.json").exists()
    assert (out_dir / "runtime_targets.json").exists()
    assert (out_dir / "kaggle_notebook.ipynb").exists()
    assert (out_dir / "package_data" / "training_examples.json").exists()
    assert (out_dir / "package_data" / "splits" / "train.txt").exists()
    notebook = json.loads((out_dir / "kaggle_notebook.ipynb").read_text(encoding="utf-8"))
    assert notebook["cells"]
    exported_config = json.loads((out_dir / "config.json").read_text(encoding="utf-8"))
    assert "trainer_backend" in exported_config


def test_exported_portable_package_runs_unsupervised_training() -> None:
    pytest.importorskip("sklearn")
    layout = build_storage_layout(_tmp_dir("model_studio_portable_unsupervised"))
    layout.training_dir.mkdir(parents=True, exist_ok=True)
    rows = []
    for idx in range(6):
        pdb_id = f"PU{idx:03d}"
        rows.append({
            "example_id": f"unsup:{pdb_id}:0",
            "structure": {"pdb_id": pdb_id, "resolution": 1.5 + idx * 0.1},
            "protein": {"sequence_length": 100 + idx},
            "ligand": {"ligand_id": f"LPU{idx:03d}", "molecular_weight": 170.0 + idx},
            "interaction": {"interface_residue_count": 5 + idx},
            "experiment": {"source_database": "RCSB"},
            "graph_features": {"network_degree": idx % 3},
            "provenance": {"pdb_id": pdb_id, "pair_identity_key": f"pair:{pdb_id}"},
            "labels": {},
        })
    (layout.training_dir / "training_examples.json").write_text(json.dumps(rows), encoding="utf-8")

    out_dir = export_training_package(
        layout,
        starter_config={"family": "clustering", "task": "unsupervised", "training": {"cluster_count": 2, "seed": 3}},
        target="colab",
        package_name="portable_unsupervised",
    )
    transport_dir = out_dir / "model_outputs"
    result = subprocess.run(
        [str(Path(sys.executable)), str(out_dir / "train.py")],
        cwd=out_dir,
        check=True,
        capture_output=True,
        text=True,
    )

    assert "Portable training completed" in result.stdout
    assert (transport_dir / "metrics.json").exists()
    assert (transport_dir / "embedding_records.json").exists()
    manifest = json.loads((transport_dir / "run_manifest.json").read_text(encoding="utf-8"))
    assert manifest["task"] == "unsupervised"


def test_execute_training_run_writes_local_artifacts() -> None:
    pytest.importorskip("sklearn")
    layout = build_storage_layout(_tmp_dir("model_studio_training"))
    layout.training_dir.mkdir(parents=True, exist_ok=True)
    layout.splits_dir.mkdir(parents=True, exist_ok=True)
    examples = []
    for idx in range(12):
        pdb_id = f"A{idx:03d}"
        examples.append({
            "example_id": f"train:{pdb_id}:0",
            "structure": {"pdb_id": pdb_id, "resolution": 1.5 + idx * 0.1},
            "protein": {"uniprot_id": f"P{idx:05d}", "sequence_length": 100 + idx},
            "ligand": {"ligand_id": f"L{idx:03d}", "molecular_weight": 200.0 + idx},
            "interaction": {"interface_residue_count": idx + 1},
            "experiment": {"affinity_type": "Ki", "source_database": "ChEMBL", "reported_measurement_count": 1},
            "graph_features": {"network_degree": idx % 3, "pathway_count": idx % 2},
            "labels": {"binding_affinity_log10": 0.5 + idx * 0.2, "is_mutant": bool(idx % 2)},
            "provenance": {"pdb_id": pdb_id, "pair_identity_key": f"pair:{pdb_id}"},
        })
    (layout.training_dir / "training_examples.json").write_text(json.dumps(examples), encoding="utf-8")
    (layout.splits_dir / "train.txt").write_text("\n".join(f"RCSB_A{idx:03d}" for idx in range(8)), encoding="utf-8")
    (layout.splits_dir / "val.txt").write_text("\n".join(f"RCSB_A{idx:03d}" for idx in range(8, 10)), encoding="utf-8")
    (layout.splits_dir / "test.txt").write_text("\n".join(f"RCSB_A{idx:03d}" for idx in range(10, 12)), encoding="utf-8")

    result = execute_training_run(
        layout,
        starter_config={
            "family": "random_forest",
            "task": "regression",
            "training": {"seed": 42},
            "model": {"type": "random_forest", "n_estimators": 20, "min_samples_leaf": 1},
        },
    )

    assert result.run_dir.exists()
    assert (result.run_dir / "metrics.json").exists()
    assert (result.run_dir / "run_manifest.json").exists()
    assert (result.run_dir / "model.pkl").exists()
    metrics = json.loads((result.run_dir / "metrics.json").read_text(encoding="utf-8"))
    assert "train" in metrics
    assert "test" in metrics


def test_import_training_run_copies_remote_artifacts() -> None:
    layout = build_storage_layout(_tmp_dir("model_studio_import"))
    source_dir = _tmp_dir("model_studio_import_source")
    (source_dir / "model_outputs").mkdir(parents=True, exist_ok=True)
    (source_dir / "import_run.json").write_text(json.dumps({"target_runtime": "kaggle"}), encoding="utf-8")
    (source_dir / "model_outputs" / "model.pkl").write_bytes(b"demo")
    (source_dir / "model_outputs" / "run_metrics.json").write_text(json.dumps({"status": "ok"}), encoding="utf-8")

    imported_dir = import_training_run(layout, source_dir=source_dir)

    assert imported_dir.exists()
    assert (imported_dir / "model.pkl").exists()
    assert (imported_dir / "run_metrics.json").exists()
    assert (imported_dir / "import_manifest.json").exists()
    assert (imported_dir / "metrics.json").exists()
    assert (imported_dir / "run_manifest.json").exists()


def test_import_training_run_detects_nested_output_layout_and_metrics_json() -> None:
    layout = build_storage_layout(_tmp_dir("model_studio_import_nested"))
    source_dir = _tmp_dir("model_studio_import_nested_source")
    nested_dir = source_dir / "outputs" / "final_run"
    nested_dir.mkdir(parents=True, exist_ok=True)
    (source_dir / "import_run.json").write_text(
        json.dumps({"target_runtime": "colab", "model_output_dir": "outputs/final_run"}),
        encoding="utf-8",
    )
    (source_dir / "runtime_targets.json").write_text(
        json.dumps({"selected_target": "colab"}),
        encoding="utf-8",
    )
    (nested_dir / "checkpoint.pt").write_bytes(b"demo-checkpoint")
    (nested_dir / "metrics.json").write_text(
        json.dumps({
            "family": "dense_nn",
            "task": "regression",
            "backend": "torch_tabular_mlp",
            "test": {"rmse": 0.42},
        }),
        encoding="utf-8",
    )

    imported_dir = import_training_run(layout, source_dir=source_dir)

    assert imported_dir.exists()
    assert (imported_dir / "checkpoint.pt").exists()
    manifest = json.loads((imported_dir / "run_manifest.json").read_text(encoding="utf-8"))
    import_manifest = json.loads((imported_dir / "import_manifest.json").read_text(encoding="utf-8"))
    assert manifest["runtime_target"] == "colab"
    assert manifest["backend"] == "torch_tabular_mlp"
    assert import_manifest["detected_runtime_target"] == "colab"
    assert "runtime_targets.json" in import_manifest["copied_context_files"]


def test_import_training_run_normalizes_common_remote_artifact_aliases() -> None:
    layout = build_storage_layout(_tmp_dir("model_studio_import_aliases"))
    source_dir = _tmp_dir("model_studio_import_aliases_source")
    payload_dir = source_dir / "artifacts"
    payload_dir.mkdir(parents=True, exist_ok=True)
    (source_dir / "import_run.json").write_text(json.dumps({"target_runtime": "cluster"}), encoding="utf-8")
    (payload_dir / "model.pkl").write_bytes(b"demo-model")
    (payload_dir / "results.json").write_text(
        json.dumps({"family": "xgboost", "task": "classification", "backend": "xgboost", "test": {"f1": 0.81}}),
        encoding="utf-8",
    )
    (payload_dir / "training_history.json").write_text(
        json.dumps([{"epoch": 1, "train_metric": 0.7, "val_metric": 0.68}]),
        encoding="utf-8",
    )
    (payload_dir / "dataset_splits.json").write_text(
        json.dumps({"train": {"count": 8}, "val": {"count": 2}, "test": {"count": 2}}),
        encoding="utf-8",
    )
    (payload_dir / "predictions_test.json").write_text(
        json.dumps([{"pdb_id": "ALI1", "prediction": 0.81}]),
        encoding="utf-8",
    )
    (payload_dir / "learning_curve.svg").write_text("<svg/>", encoding="utf-8")
    (payload_dir / "evaluation.svg").write_text("<svg/>", encoding="utf-8")

    imported_dir = import_training_run(layout, source_dir=source_dir)

    assert (imported_dir / "history.json").exists()
    assert (imported_dir / "split_summary.json").exists()
    assert (imported_dir / "training_curve.svg").exists()
    assert (imported_dir / "test_performance.svg").exists()
    assert (imported_dir / "test_predictions.json").exists()
    manifest = json.loads((imported_dir / "run_manifest.json").read_text(encoding="utf-8"))
    inspection = inspect_training_run(imported_dir, source="imported")
    assert inspection.split_counts["test"] == 2
    assert inspection.artifacts["run_manifest"].endswith("run_manifest.json")
    assert inspection.artifacts["split_summary"].endswith("split_summary.json")
    assert inspection.artifacts["test_predictions"].endswith("test_predictions.json")
    assert inspection.artifacts["test_prediction_count"] == "1"
    assert manifest["artifacts"]["history"] == "history.json"
    assert manifest["artifacts"]["split_summary"] == "split_summary.json"
    assert manifest["artifacts"]["training_curve"] == "training_curve.svg"
    assert manifest["artifacts"]["test_performance"] == "test_performance.svg"
    assert manifest["artifacts"]["test_predictions"] == "test_predictions.json"


def test_imported_run_with_test_metrics_aliases_compares_cleanly() -> None:
    layout = build_storage_layout(_tmp_dir("model_studio_import_metric_aliases"))
    source_dir = _tmp_dir("model_studio_import_metric_aliases_source")
    payload_dir = source_dir / "outputs"
    payload_dir.mkdir(parents=True, exist_ok=True)
    (source_dir / "import_run.json").write_text(json.dumps({"target_runtime": "kaggle"}), encoding="utf-8")
    (payload_dir / "model.pkl").write_bytes(b"demo-model")
    (payload_dir / "results.json").write_text(
        json.dumps({
            "family": "random_forest",
            "task": "classification",
            "backend": "sklearn_random_forest",
            "test_metrics": {"accuracy": 0.73},
            "validation_metrics": {"accuracy": 0.7},
        }),
        encoding="utf-8",
    )

    imported_dir = import_training_run(layout, source_dir=source_dir)
    comparisons = compare_training_runs(layout)
    inspection = inspect_training_run(imported_dir, source="imported")

    assert comparisons
    assert comparisons[0].primary_metric_name == "accuracy"
    assert comparisons[0].primary_metric_value == pytest.approx(0.73)
    assert inspection.primary_metric_name == "accuracy"
    assert inspection.primary_metric_value == pytest.approx(0.73)


def test_import_training_run_uses_config_metadata_when_metrics_are_sparse() -> None:
    layout = build_storage_layout(_tmp_dir("model_studio_import_config_metadata"))
    source_dir = _tmp_dir("model_studio_import_config_metadata_source")
    payload_dir = source_dir / "outputs"
    payload_dir.mkdir(parents=True, exist_ok=True)
    (source_dir / "import_run.json").write_text(json.dumps({"target_runtime": "colab"}), encoding="utf-8")
    (source_dir / "config.json").write_text(
        json.dumps({
            "family": "hybrid_fusion",
            "task": "regression",
            "trainer_backend": "pyg_hybrid_fusion",
            "backend_plan": {
                "requested_family": "hybrid_fusion",
                "execution_family": "hybrid_fusion",
                "backend_id": "pyg_hybrid_fusion",
                "implementation": "native",
                "native_graph": True,
            },
        }),
        encoding="utf-8",
    )
    (payload_dir / "model.pkl").write_bytes(b"demo-model")
    (payload_dir / "run_metrics.json").write_text(
        json.dumps({"status": "ok", "test_metrics": {"rmse": 0.38}}),
        encoding="utf-8",
    )

    imported_dir = import_training_run(layout, source_dir=source_dir)
    inspection = inspect_training_run(imported_dir, source="imported")
    manifest = json.loads((imported_dir / "run_manifest.json").read_text(encoding="utf-8"))

    assert manifest["family"] == "hybrid_fusion"
    assert manifest["task"] == "regression"
    assert manifest["backend"] == "pyg_hybrid_fusion"
    assert manifest["backend_plan"]["native_graph"] is True
    assert inspection.family == "hybrid_fusion"
    assert inspection.backend_id == "pyg_hybrid_fusion"
    assert inspection.primary_metric_name == "rmse"
    assert inspection.primary_metric_value == pytest.approx(0.38)
    assert inspection.artifacts["import_manifest"].endswith("import_manifest.json")


def test_run_saved_model_batch_inference_collects_successes_and_failures() -> None:
    pytest.importorskip("sklearn")
    layout = build_storage_layout(_tmp_dir("model_studio_batch_inference"))
    layout.training_dir.mkdir(parents=True, exist_ok=True)
    layout.splits_dir.mkdir(parents=True, exist_ok=True)
    layout.raw_rcsb_dir.mkdir(parents=True, exist_ok=True)
    examples = []
    for idx in range(10):
        pdb_id = f"BI{idx:03d}"
        examples.append({
            "example_id": f"train:{pdb_id}:0",
            "structure": {"pdb_id": pdb_id, "resolution": 1.5 + idx * 0.1},
            "protein": {"sequence_length": 100 + idx},
            "ligand": {"ligand_id": f"LBI{idx:03d}", "molecular_weight": 150.0 + idx},
            "interaction": {"interface_residue_count": 6 + idx},
            "experiment": {"affinity_type": "Kd", "source_database": "ChEMBL"},
            "labels": {"binding_affinity_log10": 0.5 + idx * 0.2},
            "provenance": {"pdb_id": pdb_id, "pair_identity_key": f"pair:{pdb_id}"},
        })
    (layout.training_dir / "training_examples.json").write_text(json.dumps(examples), encoding="utf-8")
    (layout.splits_dir / "train.txt").write_text("\n".join(f"RCSB_BI{idx:03d}" for idx in range(7)), encoding="utf-8")
    (layout.splits_dir / "val.txt").write_text("RCSB_BI007\nRCSB_BI008\n", encoding="utf-8")
    (layout.splits_dir / "test.txt").write_text("RCSB_BI009\n", encoding="utf-8")
    (layout.raw_rcsb_dir / "BIX01.json").write_text(json.dumps({
        "rcsb_id": "BIX01",
        "struct": {"title": "batch raw fallback"},
        "exptl": [{"method": "X-RAY DIFFRACTION"}],
        "rcsb_entry_info": {"resolution_combined": [2.2], "deposited_atom_count": 1200, "deposited_polymer_monomer_count": 210},
    }), encoding="utf-8")

    result = execute_training_run(
        layout,
        starter_config={"family": "random_forest", "task": "regression", "training": {"seed": 21}},
    )
    batch = run_saved_model_batch_inference(
        layout,
        run_dir=result.run_dir,
        pdb_ids=["BI009", "BIX01", "DOESNOTEXIST"],
    )

    assert batch["success_count"] == 2
    assert batch["failure_count"] == 1
    assert batch["results"][0]["pdb_id"] == "BI009"
    assert batch["results"][1]["pdb_id"] == "BIX01"
    assert Path(batch["artifact_path"]).exists()
    assert Path(batch["artifact_csv_path"]).exists()
    persisted = json.loads(Path(batch["artifact_path"]).read_text(encoding="utf-8"))
    assert persisted["success_count"] == 2
    assert persisted["requested_pdb_ids"] == ["BI009", "BIX01", "DOESNOTEXIST"]
    csv_lines = Path(batch["artifact_csv_path"]).read_text(encoding="utf-8").strip().splitlines()
    assert csv_lines[0].startswith("pdb_id,status,prediction")
    assert any("BI009,ok," in line for line in csv_lines[1:])
    assert any("DOESNOTEXIST,error," in line for line in csv_lines[1:])


def test_compare_runs_and_saved_model_inference() -> None:
    pytest.importorskip("sklearn")
    layout = build_storage_layout(_tmp_dir("model_studio_compare"))
    layout.training_dir.mkdir(parents=True, exist_ok=True)
    layout.splits_dir.mkdir(parents=True, exist_ok=True)
    examples = []
    for idx in range(10):
        pdb_id = f"B{idx:03d}"
        examples.append({
            "example_id": f"train:{pdb_id}:0",
            "structure": {"pdb_id": pdb_id, "resolution": 2.0 + idx * 0.1},
            "protein": {"uniprot_id": f"P{idx:05d}", "sequence_length": 80 + idx},
            "ligand": {"ligand_id": f"L{idx:03d}", "molecular_weight": 150.0 + idx},
            "interaction": {"interface_residue_count": idx + 2},
            "experiment": {"affinity_type": "Ki", "source_database": "ChEMBL"},
            "graph_features": {"network_degree": idx % 4},
            "labels": {"binding_affinity_log10": 0.3 + idx * 0.15, "is_mutant": False},
            "provenance": {"pdb_id": pdb_id, "pair_identity_key": f"pair:{pdb_id}"},
        })
    (layout.training_dir / "training_examples.json").write_text(json.dumps(examples), encoding="utf-8")
    (layout.splits_dir / "train.txt").write_text("\n".join(f"RCSB_B{idx:03d}" for idx in range(6)), encoding="utf-8")
    (layout.splits_dir / "val.txt").write_text("\n".join(f"RCSB_B{idx:03d}" for idx in range(6, 8)), encoding="utf-8")
    (layout.splits_dir / "test.txt").write_text("\n".join(f"RCSB_B{idx:03d}" for idx in range(8, 10)), encoding="utf-8")

    result = execute_training_run(
        layout,
        starter_config={
            "family": "random_forest",
            "task": "regression",
            "training": {"seed": 7},
            "model": {"type": "random_forest", "n_estimators": 16},
        },
    )

    comparisons = compare_training_runs(layout)
    assert comparisons
    assert comparisons[0].run_name == result.run_name

    inference = run_saved_model_inference(layout, run_dir=result.run_dir, pdb_id="B008")
    assert inference["pdb_id"] == "B008"
    assert "prediction" in inference


def test_build_graph_learning_contract_detects_graph_ready_examples() -> None:
    layout = build_storage_layout(_tmp_dir("model_studio_graph_contract"))
    layout.training_dir.mkdir(parents=True, exist_ok=True)
    layout.graph_dir.mkdir(parents=True, exist_ok=True)
    (layout.training_dir / "training_examples.json").write_text(json.dumps([
        {"example_id": "train:C001:0", "structure": {"pdb_id": "C001"}, "labels": {"binding_affinity_log10": 1.0}},
        {"example_id": "train:C002:0", "structure": {"pdb_id": "C002"}, "labels": {"binding_affinity_log10": 2.0}},
    ]), encoding="utf-8")
    (layout.graph_dir / "graph_nodes.json").write_text(json.dumps([
        {"node_id": "n1", "primary_id": "C001", "metadata": {"pdb_id": "C001"}},
    ]), encoding="utf-8")
    (layout.graph_dir / "graph_edges.json").write_text(json.dumps([
        {"edge_id": "e1", "metadata": {"pdb_id": "C001"}},
    ]), encoding="utf-8")

    contract = build_graph_learning_contract(layout)

    assert contract.available is True
    assert contract.matched_example_count == 1


def test_build_graph_sample_manifest_and_export_package_include_graph_samples() -> None:
    layout = build_storage_layout(_tmp_dir("model_studio_graph_manifest"))
    layout.training_dir.mkdir(parents=True, exist_ok=True)
    layout.graph_dir.mkdir(parents=True, exist_ok=True)
    layout.splits_dir.mkdir(parents=True, exist_ok=True)
    (layout.training_dir / "training_examples.json").write_text(json.dumps([
        {
            "example_id": "train:H001:0",
            "structure": {"pdb_id": "H001"},
            "provenance": {"pdb_id": "H001", "pair_identity_key": "pair:H001"},
            "labels": {"binding_affinity_log10": 1.0},
        }
    ]), encoding="utf-8")
    (layout.graph_dir / "graph_nodes.json").write_text(json.dumps([
        {"node_id": "node:H001", "primary_id": "H001", "metadata": {"pdb_id": "H001"}},
    ]), encoding="utf-8")
    (layout.graph_dir / "graph_edges.json").write_text(json.dumps([
        {"edge_id": "edge:H001", "metadata": {"pdb_id": "H001"}},
    ]), encoding="utf-8")
    (layout.splits_dir / "train.txt").write_text("RCSB_H001\n", encoding="utf-8")

    manifest_path = build_graph_sample_manifest(layout)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["graph_ready_count"] == 1
    assert manifest["rows"][0]["pdb_id"] == "H001"

    out_dir = export_training_package(
        layout,
        starter_config={"family": "gnn", "model": {"type": "graphsage"}},
        target="kaggle",
        package_name="graph_package",
    )
    assert (out_dir / "package_data" / "graph" / "graph_sample_manifest.json").exists()
    assert (out_dir / "package_data" / "graph" / "dataset" / "graph_dataset_records.json").exists()


def test_materialize_graph_dataset_records_slices_nodes_and_edges() -> None:
    layout = build_storage_layout(_tmp_dir("model_studio_graph_dataset"))
    layout.training_dir.mkdir(parents=True, exist_ok=True)
    layout.graph_dir.mkdir(parents=True, exist_ok=True)
    (layout.training_dir / "training_examples.json").write_text(json.dumps([
        {
            "example_id": "train:J001:0",
            "structure": {"pdb_id": "J001"},
            "provenance": {"pdb_id": "J001", "pair_identity_key": "pair:J001"},
            "labels": {"binding_affinity_log10": 1.2},
        }
    ]), encoding="utf-8")
    (layout.graph_dir / "graph_nodes.json").write_text(json.dumps([
        {"node_id": "node:J001:1", "primary_id": "J001", "metadata": {"pdb_id": "J001"}},
        {"node_id": "node:J001:2", "primary_id": "J001", "metadata": {"pdb_id": "J001"}},
    ]), encoding="utf-8")
    (layout.graph_dir / "graph_edges.json").write_text(json.dumps([
        {"edge_id": "edge:J001:1", "metadata": {"pdb_id": "J001"}},
    ]), encoding="utf-8")

    records_path, manifest_path = materialize_graph_dataset_records(layout)
    records = json.loads(records_path.read_text(encoding="utf-8"))
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    assert len(records) == 1
    assert records[0]["example_id"] == "train:J001:0"
    assert records[0]["node_count"] == 2
    assert records[0]["edge_count"] == 1
    assert manifest["record_count"] == 1


def test_build_pyg_ready_graph_samples_creates_canonical_graph_inputs() -> None:
    layout = build_storage_layout(_tmp_dir("model_studio_pyg_ready"))
    layout.training_dir.mkdir(parents=True, exist_ok=True)
    layout.graph_dir.mkdir(parents=True, exist_ok=True)
    (layout.training_dir / "training_examples.json").write_text(json.dumps([
        {
            "example_id": "train:K001:0",
            "structure": {"pdb_id": "K001"},
            "provenance": {"pdb_id": "K001", "pair_identity_key": "pair:K001"},
            "labels": {"binding_affinity_log10": 1.1},
        }
    ]), encoding="utf-8")
    (layout.graph_dir / "graph_nodes.json").write_text(json.dumps([
        {
            "node_id": "protein:K001",
            "node_type": "Protein",
            "primary_id": "K001",
            "metadata": {"pdb_id": "K001"},
            "source_databases": ["RCSB"],
        },
        {
            "node_id": "ligand:K001",
            "node_type": "Ligand",
            "primary_id": "K001",
            "metadata": {"pdb_id": "K001"},
            "source_databases": ["ChEMBL"],
        },
    ]), encoding="utf-8")
    (layout.graph_dir / "graph_edges.json").write_text(json.dumps([
        {
            "edge_id": "edge:K001",
            "edge_type": "ProteinLigandInteraction",
            "source_node_id": "protein:K001",
            "target_node_id": "ligand:K001",
            "metadata": {"pdb_id": "K001", "binding_affinity_value": 12.0},
            "relation": "Ki",
        }
    ]), encoding="utf-8")

    samples_path, manifest_path = build_pyg_ready_graph_samples(layout)
    samples = json.loads(samples_path.read_text(encoding="utf-8"))
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    assert manifest["sample_count"] == 1
    assert samples[0]["example_id"] == "train:K001:0"
    assert samples[0]["node_count"] == 2
    assert samples[0]["edge_count"] == 1
    assert samples[0]["edge_index"] == [[0, 1]]
    assert "node_type.protein" in samples[0]["node_feature_keys"]
    assert "edge_type.proteinligandinteraction" in samples[0]["edge_feature_keys"]

    native_samples = load_native_graph_samples(samples_path)
    assert len(native_samples) == 1
    assert native_samples[0].example_id == "train:K001:0"
    assert native_samples[0].node_count == 2
    assert native_samples[0].edge_count == 1
    assert len(native_samples[0].node_features[0]) == len(native_samples[0].node_feature_keys)
    assert native_samples[0].edge_index == ((0, 1),)


def test_materialize_graph_training_payload_attaches_targets_and_splits() -> None:
    layout = build_storage_layout(_tmp_dir("model_studio_graph_payload"))
    layout.training_dir.mkdir(parents=True, exist_ok=True)
    layout.graph_dir.mkdir(parents=True, exist_ok=True)
    layout.splits_dir.mkdir(parents=True, exist_ok=True)
    (layout.training_dir / "training_examples.json").write_text(json.dumps([
        {
            "example_id": "train:L001:0",
            "structure": {"pdb_id": "L001"},
            "provenance": {"pdb_id": "L001", "pair_identity_key": "pair:L001"},
            "labels": {"binding_affinity_log10": 1.7, "is_mutant": True},
        }
    ]), encoding="utf-8")
    (layout.graph_dir / "graph_nodes.json").write_text(json.dumps([
        {"node_id": "protein:L001", "node_type": "Protein", "primary_id": "L001", "metadata": {"pdb_id": "L001"}},
        {"node_id": "ligand:L001", "node_type": "Ligand", "primary_id": "L001", "metadata": {"pdb_id": "L001"}},
    ]), encoding="utf-8")
    (layout.graph_dir / "graph_edges.json").write_text(json.dumps([
        {"edge_id": "edge:L001", "edge_type": "ProteinLigandInteraction", "source_node_id": "protein:L001", "target_node_id": "ligand:L001", "metadata": {"pdb_id": "L001"}},
    ]), encoding="utf-8")
    (layout.splits_dir / "train.txt").write_text("RCSB_L001\n", encoding="utf-8")

    payload_path, manifest_path = materialize_graph_training_payload(layout, task="classification")
    payload = json.loads(payload_path.read_text(encoding="utf-8"))
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    assert manifest["record_count"] == 1
    assert manifest["labeled_record_count"] == 1
    assert payload[0]["split"] == "train"
    assert payload[0]["target_name"] == "labels.is_mutant"
    assert payload[0]["target_value"] == 1
    training_records = load_graph_training_records(payload_path)
    assert len(training_records) == 1
    assert training_records[0].example_id == "train:L001:0"
    assert training_records[0].target_value == 1


def test_materialize_hybrid_training_payload_attaches_attribute_features() -> None:
    layout = build_storage_layout(_tmp_dir("model_studio_hybrid_payload"))
    layout.training_dir.mkdir(parents=True, exist_ok=True)
    layout.graph_dir.mkdir(parents=True, exist_ok=True)
    layout.splits_dir.mkdir(parents=True, exist_ok=True)
    (layout.training_dir / "training_examples.json").write_text(json.dumps([
        {
            "example_id": "train:M001:0",
            "structure": {"pdb_id": "M001", "resolution": 2.0},
            "protein": {"sequence_length": 140},
            "ligand": {"molecular_weight": 250.0},
            "interaction": {"interface_residue_count": 9},
            "experiment": {"reported_measurement_count": 1},
            "graph_features": {"network_degree": 3},
            "provenance": {"pdb_id": "M001", "pair_identity_key": "pair:M001"},
            "labels": {"binding_affinity_log10": 2.3},
        }
    ]), encoding="utf-8")
    (layout.graph_dir / "graph_nodes.json").write_text(json.dumps([
        {"node_id": "protein:M001", "node_type": "Protein", "primary_id": "M001", "metadata": {"pdb_id": "M001"}},
        {"node_id": "ligand:M001", "node_type": "Ligand", "primary_id": "M001", "metadata": {"pdb_id": "M001"}},
    ]), encoding="utf-8")
    (layout.graph_dir / "graph_edges.json").write_text(json.dumps([
        {"edge_id": "edge:M001", "edge_type": "ProteinLigandInteraction", "source_node_id": "protein:M001", "target_node_id": "ligand:M001", "metadata": {"pdb_id": "M001"}},
    ]), encoding="utf-8")
    (layout.splits_dir / "train.txt").write_text("RCSB_M001\n", encoding="utf-8")

    payload_path, manifest_path = materialize_hybrid_training_payload(layout, task="regression")
    payload = json.loads(payload_path.read_text(encoding="utf-8"))
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    records = load_hybrid_training_records(payload_path)

    assert manifest["record_count"] == 1
    assert payload[0]["split"] == "train"
    assert payload[0]["target_value"] == 2.3
    assert "structure.resolution" in payload[0]["attribute_features"]
    assert records[0].attribute_features["protein.sequence_length"] == 140.0


def test_execute_graph_family_as_surrogate_and_infer_from_feature_record() -> None:
    pytest.importorskip("sklearn")
    layout = build_storage_layout(_tmp_dir("model_studio_graph_surrogate"))
    layout.training_dir.mkdir(parents=True, exist_ok=True)
    layout.splits_dir.mkdir(parents=True, exist_ok=True)
    layout.graph_dir.mkdir(parents=True, exist_ok=True)
    layout.features_dir.mkdir(parents=True, exist_ok=True)
    examples = []
    for idx in range(8):
        pdb_id = f"G{idx:03d}"
        examples.append({
            "example_id": f"train:{pdb_id}:0",
            "structure": {"pdb_id": pdb_id, "resolution": 1.8 + idx * 0.1},
            "protein": {"uniprot_id": f"PG{idx:05d}", "sequence_length": 90 + idx},
            "ligand": {"ligand_id": f"LG{idx:03d}", "molecular_weight": 180.0 + idx},
            "interaction": {"interface_residue_count": idx + 3},
            "experiment": {"affinity_type": "Kd", "source_database": "ChEMBL"},
            "graph_features": {"network_degree": idx, "ppi_degree": idx % 2, "pli_degree": idx % 3, "pathway_count": idx % 4},
            "labels": {"binding_affinity_log10": 0.8 + idx * 0.1},
            "provenance": {"pdb_id": pdb_id, "pair_identity_key": f"pair:{pdb_id}"},
        })
    (layout.training_dir / "training_examples.json").write_text(json.dumps(examples), encoding="utf-8")
    (layout.splits_dir / "train.txt").write_text("\n".join(f"RCSB_G{idx:03d}" for idx in range(5)), encoding="utf-8")
    (layout.splits_dir / "val.txt").write_text("RCSB_G005\nRCSB_G006\n", encoding="utf-8")
    (layout.splits_dir / "test.txt").write_text("RCSB_G007\n", encoding="utf-8")
    (layout.graph_dir / "graph_nodes.json").write_text(json.dumps([
        {"node_id": "n1", "node_type": "ProteinComplex", "primary_id": "G000", "metadata": {"pdb_id": "G000"}},
        {"node_id": "n2", "node_type": "ProteinComplex", "primary_id": "G999", "metadata": {"pdb_id": "G999"}},
    ]), encoding="utf-8")
    (layout.graph_dir / "graph_edges.json").write_text(json.dumps([
        {"edge_id": "e1", "edge_type": "ProteinLigandInteraction", "metadata": {"pdb_id": "G000"}},
        {"edge_id": "e2", "edge_type": "ProteinLigandInteraction", "metadata": {"pdb_id": "G999"}},
    ]), encoding="utf-8")
    (layout.features_dir / "feature_records.json").write_text(json.dumps([
        {
            "feature_id": "features:G999:0",
            "pdb_id": "G999",
            "pair_identity_key": "pair:G999",
            "values": {
                "structure_resolution": 2.4,
                "atom_count_total": 1234,
                "sequence_length": 145,
                "protein_mean_hydropathy": -0.2,
                "protein_aromatic_fraction": 0.1,
                "protein_charged_fraction": 0.3,
                "protein_polar_fraction": 0.4,
                "ligand_molecular_weight": 222.2,
                "interface_residue_count": 12,
                "network_degree": 4,
                "ppi_degree": 1,
                "pli_degree": 2,
                "pathway_count": 0,
                "reported_measurement_count": 1,
                "binding_affinity_type": "Kd",
                "assay_source_database": "ChEMBL",
                "preferred_source_database": "ChEMBL",
                "ligand_inchikey": "DEMO-INCHIKEY",
                "ligand_component_type": "small_molecule",
            },
        }
    ]), encoding="utf-8")

    result = execute_training_run(
        layout,
        starter_config={
            "family": "gnn",
            "task": "regression",
            "training": {"seed": 11, "epochs": 8},
            "model": {"type": "graphsage"},
        },
    )

    assert result.run_dir.exists()
    assert result.warnings
    assert "graph-aware surrogate" in result.warnings[0]
    assert (result.run_dir / "graph_sample_manifest.json").exists()
    assert (result.run_dir / "graph_dataset" / "graph_dataset_records.json").exists()
    assert (result.run_dir / "pyg_ready_graphs" / "pyg_ready_graph_samples.json").exists()
    assert (result.run_dir / "graph_training_payload" / "graph_training_payload.json").exists()
    assert (result.run_dir / "hybrid_training_payload" / "hybrid_training_payload.json").exists()

    inference = run_saved_model_inference(layout, run_dir=result.run_dir, pdb_id="G999")
    assert inference["pdb_id"] == "G999"
    assert inference["example_id"] == "inference:G999"


def test_execute_native_pyg_gnn_run_records_backend_metadata() -> None:
    pytest.importorskip("torch")
    pytest.importorskip("torch_geometric")
    layout = build_storage_layout(_tmp_dir("model_studio_native_pyg_gnn"))
    layout.training_dir.mkdir(parents=True, exist_ok=True)
    layout.splits_dir.mkdir(parents=True, exist_ok=True)
    layout.graph_dir.mkdir(parents=True, exist_ok=True)
    (layout.training_dir / "training_examples.json").write_text(json.dumps([
        {
            "example_id": "train:N001:0",
            "structure": {"pdb_id": "N001"},
            "protein": {"sequence_length": 101},
            "graph_features": {"network_degree": 2},
            "provenance": {"pdb_id": "N001", "pair_identity_key": "pair:N001"},
            "labels": {"binding_affinity_log10": 1.0},
        },
        {
            "example_id": "val:N002:0",
            "structure": {"pdb_id": "N002"},
            "protein": {"sequence_length": 102},
            "graph_features": {"network_degree": 2},
            "provenance": {"pdb_id": "N002", "pair_identity_key": "pair:N002"},
            "labels": {"binding_affinity_log10": 2.0},
        },
        {
            "example_id": "test:N003:0",
            "structure": {"pdb_id": "N003"},
            "protein": {"sequence_length": 103},
            "graph_features": {"network_degree": 2},
            "provenance": {"pdb_id": "N003", "pair_identity_key": "pair:N003"},
            "labels": {"binding_affinity_log10": 3.0},
        },
    ]), encoding="utf-8")
    (layout.splits_dir / "train.txt").write_text("RCSB_N001\n", encoding="utf-8")
    (layout.splits_dir / "val.txt").write_text("RCSB_N002\n", encoding="utf-8")
    (layout.splits_dir / "test.txt").write_text("RCSB_N003\n", encoding="utf-8")
    (layout.graph_dir / "graph_nodes.json").write_text(json.dumps([
        {"node_id": "protein:N001", "node_type": "Protein", "primary_id": "N001", "metadata": {"pdb_id": "N001"}, "features": {"degree": 1.0, "weight": 0.2}},
        {"node_id": "ligand:N001", "node_type": "Ligand", "primary_id": "N001", "metadata": {"pdb_id": "N001"}, "features": {"degree": 2.0, "weight": 0.4}},
        {"node_id": "protein:N002", "node_type": "Protein", "primary_id": "N002", "metadata": {"pdb_id": "N002"}, "features": {"degree": 1.0, "weight": 0.3}},
        {"node_id": "ligand:N002", "node_type": "Ligand", "primary_id": "N002", "metadata": {"pdb_id": "N002"}, "features": {"degree": 2.0, "weight": 0.5}},
        {"node_id": "protein:N003", "node_type": "Protein", "primary_id": "N003", "metadata": {"pdb_id": "N003"}, "features": {"degree": 1.0, "weight": 0.4}},
        {"node_id": "ligand:N003", "node_type": "Ligand", "primary_id": "N003", "metadata": {"pdb_id": "N003"}, "features": {"degree": 2.0, "weight": 0.6}},
    ]), encoding="utf-8")
    (layout.graph_dir / "graph_edges.json").write_text(json.dumps([
        {"edge_id": "edge:N001", "edge_type": "ProteinLigandInteraction", "source_node_id": "protein:N001", "target_node_id": "ligand:N001", "metadata": {"pdb_id": "N001"}, "features": {"distance": 1.0}},
        {"edge_id": "edge:N002", "edge_type": "ProteinLigandInteraction", "source_node_id": "protein:N002", "target_node_id": "ligand:N002", "metadata": {"pdb_id": "N002"}, "features": {"distance": 1.1}},
        {"edge_id": "edge:N003", "edge_type": "ProteinLigandInteraction", "source_node_id": "protein:N003", "target_node_id": "ligand:N003", "metadata": {"pdb_id": "N003"}, "features": {"distance": 1.2}},
    ]), encoding="utf-8")

    result = execute_training_run(
        layout,
        starter_config={
            "family": "gnn",
            "task": "regression",
            "training": {"seed": 7, "epochs": 3, "learning_rate": 0.01},
        },
    )

    config = json.loads((result.run_dir / "config.json").read_text(encoding="utf-8"))
    manifest = json.loads((result.run_dir / "run_manifest.json").read_text(encoding="utf-8"))

    assert config["trainer_backend"] == "pyg_gnn"
    assert config["backend_plan"]["native_graph"] is True
    assert manifest["backend_plan"]["backend_id"] == "pyg_gnn"
    assert (result.run_dir / "model.pkl").exists()
    assert (result.run_dir / "graph_training_payload" / "graph_training_payload.json").exists()
    (layout.graph_dir / "graph_nodes.json").write_text(json.dumps([
        {"node_id": "protein:N001", "node_type": "Protein", "primary_id": "N001", "metadata": {"pdb_id": "N001"}, "features": {"degree": 1.0, "weight": 0.2}},
        {"node_id": "ligand:N001", "node_type": "Ligand", "primary_id": "N001", "metadata": {"pdb_id": "N001"}, "features": {"degree": 2.0, "weight": 0.4}},
        {"node_id": "protein:N002", "node_type": "Protein", "primary_id": "N002", "metadata": {"pdb_id": "N002"}, "features": {"degree": 1.0, "weight": 0.3}},
        {"node_id": "ligand:N002", "node_type": "Ligand", "primary_id": "N002", "metadata": {"pdb_id": "N002"}, "features": {"degree": 2.0, "weight": 0.5}},
        {"node_id": "protein:N003", "node_type": "Protein", "primary_id": "N003", "metadata": {"pdb_id": "N003"}, "features": {"degree": 1.0, "weight": 0.4}},
        {"node_id": "ligand:N003", "node_type": "Ligand", "primary_id": "N003", "metadata": {"pdb_id": "N003"}, "features": {"degree": 2.0, "weight": 0.6}},
        {"node_id": "protein:N004", "node_type": "Protein", "primary_id": "N004", "metadata": {"pdb_id": "N004"}, "features": {"degree": 1.0, "weight": 0.45}},
        {"node_id": "ligand:N004", "node_type": "Ligand", "primary_id": "N004", "metadata": {"pdb_id": "N004"}, "features": {"degree": 2.0, "weight": 0.65}},
    ]), encoding="utf-8")
    (layout.graph_dir / "graph_edges.json").write_text(json.dumps([
        {"edge_id": "edge:N001", "edge_type": "ProteinLigandInteraction", "source_node_id": "protein:N001", "target_node_id": "ligand:N001", "metadata": {"pdb_id": "N001"}, "features": {"distance": 1.0}},
        {"edge_id": "edge:N002", "edge_type": "ProteinLigandInteraction", "source_node_id": "protein:N002", "target_node_id": "ligand:N002", "metadata": {"pdb_id": "N002"}, "features": {"distance": 1.1}},
        {"edge_id": "edge:N003", "edge_type": "ProteinLigandInteraction", "source_node_id": "protein:N003", "target_node_id": "ligand:N003", "metadata": {"pdb_id": "N003"}, "features": {"distance": 1.2}},
        {"edge_id": "edge:N004", "edge_type": "ProteinLigandInteraction", "source_node_id": "protein:N004", "target_node_id": "ligand:N004", "metadata": {"pdb_id": "N004"}, "features": {"distance": 1.3}},
    ]), encoding="utf-8")
    layout.raw_rcsb_dir.mkdir(parents=True, exist_ok=True)
    (layout.raw_rcsb_dir / "N004.json").write_text(json.dumps({
        "rcsb_id": "N004",
        "struct": {"title": "demo raw entry"},
        "exptl": [{"method": "X-RAY DIFFRACTION"}],
        "rcsb_entry_info": {"resolution_combined": [2.1], "deposited_atom_count": 1234, "deposited_polymer_monomer_count": 250},
        "nonpolymer_entities": [{"chem_comp": {"id": "LIG"}}],
    }), encoding="utf-8")
    inference = run_saved_model_inference(layout, run_dir=result.run_dir, pdb_id="N004")
    assert inference["pdb_id"] == "N004"
    assert inference["example_id"] == "inference:N004"


def test_exported_graph_package_runs_native_pyg_training_when_available() -> None:
    pytest.importorskip("torch")
    pytest.importorskip("torch_geometric")
    layout = build_storage_layout(_tmp_dir("model_studio_portable_native_graph"))
    layout.training_dir.mkdir(parents=True, exist_ok=True)
    layout.splits_dir.mkdir(parents=True, exist_ok=True)
    layout.graph_dir.mkdir(parents=True, exist_ok=True)
    (layout.training_dir / "training_examples.json").write_text(json.dumps([
        {
            "example_id": "train:PG1:0",
            "structure": {"pdb_id": "PG1"},
            "protein": {"sequence_length": 101},
            "graph_features": {"network_degree": 2},
            "provenance": {"pdb_id": "PG1", "pair_identity_key": "pair:PG1"},
            "labels": {"binding_affinity_log10": 1.0},
        },
        {
            "example_id": "val:PG2:0",
            "structure": {"pdb_id": "PG2"},
            "protein": {"sequence_length": 102},
            "graph_features": {"network_degree": 3},
            "provenance": {"pdb_id": "PG2", "pair_identity_key": "pair:PG2"},
            "labels": {"binding_affinity_log10": 2.0},
        },
        {
            "example_id": "test:PG3:0",
            "structure": {"pdb_id": "PG3"},
            "protein": {"sequence_length": 103},
            "graph_features": {"network_degree": 4},
            "provenance": {"pdb_id": "PG3", "pair_identity_key": "pair:PG3"},
            "labels": {"binding_affinity_log10": 3.0},
        },
    ]), encoding="utf-8")
    (layout.splits_dir / "train.txt").write_text("RCSB_PG1\n", encoding="utf-8")
    (layout.splits_dir / "val.txt").write_text("RCSB_PG2\n", encoding="utf-8")
    (layout.splits_dir / "test.txt").write_text("RCSB_PG3\n", encoding="utf-8")
    (layout.graph_dir / "graph_nodes.json").write_text(json.dumps([
        {"node_id": "protein:PG1", "node_type": "Protein", "primary_id": "PG1", "metadata": {"pdb_id": "PG1"}, "features": {"degree": 1.0, "weight": 0.2}},
        {"node_id": "ligand:PG1", "node_type": "Ligand", "primary_id": "PG1", "metadata": {"pdb_id": "PG1"}, "features": {"degree": 2.0, "weight": 0.4}},
        {"node_id": "protein:PG2", "node_type": "Protein", "primary_id": "PG2", "metadata": {"pdb_id": "PG2"}, "features": {"degree": 1.0, "weight": 0.3}},
        {"node_id": "ligand:PG2", "node_type": "Ligand", "primary_id": "PG2", "metadata": {"pdb_id": "PG2"}, "features": {"degree": 2.0, "weight": 0.5}},
        {"node_id": "protein:PG3", "node_type": "Protein", "primary_id": "PG3", "metadata": {"pdb_id": "PG3"}, "features": {"degree": 1.0, "weight": 0.4}},
        {"node_id": "ligand:PG3", "node_type": "Ligand", "primary_id": "PG3", "metadata": {"pdb_id": "PG3"}, "features": {"degree": 2.0, "weight": 0.6}},
    ]), encoding="utf-8")
    (layout.graph_dir / "graph_edges.json").write_text(json.dumps([
        {"edge_id": "edge:PG1", "edge_type": "ProteinLigandInteraction", "source_node_id": "protein:PG1", "target_node_id": "ligand:PG1", "metadata": {"pdb_id": "PG1"}, "features": {"distance": 1.0}},
        {"edge_id": "edge:PG2", "edge_type": "ProteinLigandInteraction", "source_node_id": "protein:PG2", "target_node_id": "ligand:PG2", "metadata": {"pdb_id": "PG2"}, "features": {"distance": 1.1}},
        {"edge_id": "edge:PG3", "edge_type": "ProteinLigandInteraction", "source_node_id": "protein:PG3", "target_node_id": "ligand:PG3", "metadata": {"pdb_id": "PG3"}, "features": {"distance": 1.2}},
    ]), encoding="utf-8")

    out_dir = export_training_package(
        layout,
        starter_config={"family": "gnn", "task": "regression", "training": {"epochs": 3, "learning_rate": 0.01}},
        target="colab",
        package_name="portable_native_graph",
    )
    result = subprocess.run(
        [str(Path(sys.executable)), str(out_dir / "train.py")],
        cwd=out_dir,
        check=True,
        capture_output=True,
        text=True,
    )

    assert "Portable training completed" in result.stdout
    manifest = json.loads((out_dir / "model_outputs" / "run_manifest.json").read_text(encoding="utf-8"))
    assert manifest["backend"] == "pyg_gnn"


def test_exported_hybrid_package_runs_native_pyg_training_when_available() -> None:
    pytest.importorskip("torch")
    pytest.importorskip("torch_geometric")
    layout = build_storage_layout(_tmp_dir("model_studio_portable_native_hybrid"))
    layout.training_dir.mkdir(parents=True, exist_ok=True)
    layout.splits_dir.mkdir(parents=True, exist_ok=True)
    layout.graph_dir.mkdir(parents=True, exist_ok=True)
    (layout.training_dir / "training_examples.json").write_text(json.dumps([
        {
            "example_id": "train:PH1:0",
            "structure": {"pdb_id": "PH1", "resolution": 1.7},
            "protein": {"sequence_length": 111},
            "ligand": {"ligand_id": "L1", "molecular_weight": 180.0},
            "interaction": {"interface_residue_count": 8},
            "graph_features": {"network_degree": 2},
            "provenance": {"pdb_id": "PH1", "pair_identity_key": "pair:PH1"},
            "labels": {"binding_affinity_log10": 1.0},
        },
        {
            "example_id": "val:PH2:0",
            "structure": {"pdb_id": "PH2", "resolution": 1.9},
            "protein": {"sequence_length": 112},
            "ligand": {"ligand_id": "L2", "molecular_weight": 181.0},
            "interaction": {"interface_residue_count": 9},
            "graph_features": {"network_degree": 3},
            "provenance": {"pdb_id": "PH2", "pair_identity_key": "pair:PH2"},
            "labels": {"binding_affinity_log10": 2.0},
        },
        {
            "example_id": "test:PH3:0",
            "structure": {"pdb_id": "PH3", "resolution": 2.1},
            "protein": {"sequence_length": 113},
            "ligand": {"ligand_id": "L3", "molecular_weight": 182.0},
            "interaction": {"interface_residue_count": 10},
            "graph_features": {"network_degree": 4},
            "provenance": {"pdb_id": "PH3", "pair_identity_key": "pair:PH3"},
            "labels": {"binding_affinity_log10": 3.0},
        },
    ]), encoding="utf-8")
    (layout.splits_dir / "train.txt").write_text("RCSB_PH1\n", encoding="utf-8")
    (layout.splits_dir / "val.txt").write_text("RCSB_PH2\n", encoding="utf-8")
    (layout.splits_dir / "test.txt").write_text("RCSB_PH3\n", encoding="utf-8")
    (layout.graph_dir / "graph_nodes.json").write_text(json.dumps([
        {"node_id": "protein:PH1", "node_type": "Protein", "primary_id": "PH1", "metadata": {"pdb_id": "PH1"}, "features": {"degree": 1.0, "weight": 0.2}},
        {"node_id": "ligand:PH1", "node_type": "Ligand", "primary_id": "PH1", "metadata": {"pdb_id": "PH1"}, "features": {"degree": 2.0, "weight": 0.4}},
        {"node_id": "protein:PH2", "node_type": "Protein", "primary_id": "PH2", "metadata": {"pdb_id": "PH2"}, "features": {"degree": 1.0, "weight": 0.3}},
        {"node_id": "ligand:PH2", "node_type": "Ligand", "primary_id": "PH2", "metadata": {"pdb_id": "PH2"}, "features": {"degree": 2.0, "weight": 0.5}},
        {"node_id": "protein:PH3", "node_type": "Protein", "primary_id": "PH3", "metadata": {"pdb_id": "PH3"}, "features": {"degree": 1.0, "weight": 0.4}},
        {"node_id": "ligand:PH3", "node_type": "Ligand", "primary_id": "PH3", "metadata": {"pdb_id": "PH3"}, "features": {"degree": 2.0, "weight": 0.6}},
    ]), encoding="utf-8")
    (layout.graph_dir / "graph_edges.json").write_text(json.dumps([
        {"edge_id": "edge:PH1", "edge_type": "ProteinLigandInteraction", "source_node_id": "protein:PH1", "target_node_id": "ligand:PH1", "metadata": {"pdb_id": "PH1"}, "features": {"distance": 1.0}},
        {"edge_id": "edge:PH2", "edge_type": "ProteinLigandInteraction", "source_node_id": "protein:PH2", "target_node_id": "ligand:PH2", "metadata": {"pdb_id": "PH2"}, "features": {"distance": 1.1}},
        {"edge_id": "edge:PH3", "edge_type": "ProteinLigandInteraction", "source_node_id": "protein:PH3", "target_node_id": "ligand:PH3", "metadata": {"pdb_id": "PH3"}, "features": {"distance": 1.2}},
    ]), encoding="utf-8")

    out_dir = export_training_package(
        layout,
        starter_config={"family": "hybrid_fusion", "task": "regression", "training": {"epochs": 3, "learning_rate": 0.01}},
        target="kaggle",
        package_name="portable_native_hybrid",
    )
    result = subprocess.run(
        [str(Path(sys.executable)), str(out_dir / "train.py")],
        cwd=out_dir,
        check=True,
        capture_output=True,
        text=True,
    )

    assert "Portable training completed" in result.stdout
    manifest = json.loads((out_dir / "model_outputs" / "run_manifest.json").read_text(encoding="utf-8"))
    assert manifest["backend"] == "pyg_hybrid_fusion"


def test_build_training_run_report_summarizes_chart_and_backend_state() -> None:
    pytest.importorskip("sklearn")
    layout = build_storage_layout(_tmp_dir("model_studio_run_report"))
    layout.training_dir.mkdir(parents=True, exist_ok=True)
    layout.splits_dir.mkdir(parents=True, exist_ok=True)
    examples = []
    for idx in range(12):
        pdb_id = f"R{idx:03d}"
        examples.append({
            "example_id": f"train:{pdb_id}:0",
            "structure": {"pdb_id": pdb_id, "resolution": 1.6 + idx * 0.05},
            "protein": {"uniprot_id": f"PR{idx:05d}", "sequence_length": 80 + idx},
            "ligand": {"ligand_id": f"LR{idx:03d}", "molecular_weight": 150.0 + idx},
            "interaction": {"interface_residue_count": idx + 2},
            "experiment": {"affinity_type": "Kd", "source_database": "ChEMBL"},
            "graph_features": {"network_degree": idx % 4},
            "labels": {"binding_affinity_log10": 0.4 + idx * 0.2},
            "provenance": {"pdb_id": pdb_id, "pair_identity_key": f"pair:{pdb_id}"},
        })
    (layout.training_dir / "training_examples.json").write_text(json.dumps(examples), encoding="utf-8")
    (layout.splits_dir / "train.txt").write_text("\n".join(f"RCSB_R{idx:03d}" for idx in range(8)), encoding="utf-8")
    (layout.splits_dir / "val.txt").write_text("\n".join(f"RCSB_R{idx:03d}" for idx in range(8, 10)), encoding="utf-8")
    (layout.splits_dir / "test.txt").write_text("\n".join(f"RCSB_R{idx:03d}" for idx in range(10, 12)), encoding="utf-8")

    result = execute_training_run(
        layout,
        starter_config={
            "family": "random_forest",
            "task": "regression",
            "training": {"seed": 13},
        },
    )

    report = build_training_run_report(layout)

    assert report["run_count"] >= 1
    assert report["chart_ready_count"] >= 1
    assert report["test_plot_ready_count"] >= 1
    assert report["best_overall"]["run_name"] == result.run_name
    assert report["recent_runs"][0]["backend_id"] == "sklearn_random_forest"
    assert report["recent_runs"][0]["artifacts"]["training_curve"].endswith("training_curve.svg")


def test_saved_model_inference_falls_back_to_raw_rcsb_record() -> None:
    pytest.importorskip("sklearn")
    layout = build_storage_layout(_tmp_dir("model_studio_raw_inference"))
    layout.training_dir.mkdir(parents=True, exist_ok=True)
    layout.splits_dir.mkdir(parents=True, exist_ok=True)
    layout.raw_rcsb_dir.mkdir(parents=True, exist_ok=True)
    examples = []
    for idx in range(10):
        pdb_id = f"Q{idx:03d}"
        examples.append({
            "example_id": f"train:{pdb_id}:0",
            "structure": {"pdb_id": pdb_id, "resolution": 1.7 + idx * 0.1},
            "protein": {"uniprot_id": f"PQ{idx:05d}", "sequence_length": 100 + idx},
            "ligand": {"ligand_id": f"LQ{idx:03d}", "molecular_weight": 180.0 + idx},
            "interaction": {"interface_residue_count": 6 + idx},
            "experiment": {"affinity_type": "Kd", "source_database": "ChEMBL"},
            "labels": {"binding_affinity_log10": 0.6 + idx * 0.15},
            "provenance": {"pdb_id": pdb_id, "pair_identity_key": f"pair:{pdb_id}"},
        })
    (layout.training_dir / "training_examples.json").write_text(json.dumps(examples), encoding="utf-8")
    (layout.splits_dir / "train.txt").write_text("\n".join(f"RCSB_Q{idx:03d}" for idx in range(7)), encoding="utf-8")
    (layout.splits_dir / "val.txt").write_text("RCSB_Q007\nRCSB_Q008\n", encoding="utf-8")
    (layout.splits_dir / "test.txt").write_text("RCSB_Q009\n", encoding="utf-8")
    (layout.raw_rcsb_dir / "Q999.json").write_text(json.dumps({
        "rcsb_id": "Q999",
        "struct": {"title": "raw fallback"},
        "exptl": [{"method": "X-RAY DIFFRACTION"}],
        "rcsb_entry_info": {"resolution_combined": [2.3], "deposited_atom_count": 1400, "deposited_polymer_monomer_count": 220},
        "nonpolymer_entities": [{"chem_comp": {"id": "FAL"}}],
    }), encoding="utf-8")

    result = execute_training_run(
        layout,
        starter_config={"family": "random_forest", "task": "regression", "training": {"seed": 5}},
    )
    inference = run_saved_model_inference(layout, run_dir=result.run_dir, pdb_id="Q999")
    assert inference["pdb_id"] == "Q999"
    assert inference["example_id"] == "inference:Q999"


def test_execute_unsupervised_clustering_run_writes_embedding_artifacts() -> None:
    pytest.importorskip("sklearn")
    layout = build_storage_layout(_tmp_dir("model_studio_unsupervised"))
    layout.training_dir.mkdir(parents=True, exist_ok=True)
    examples = []
    for idx in range(8):
        pdb_id = f"U{idx:03d}"
        examples.append({
            "example_id": f"unsup:{pdb_id}:0",
            "structure": {"pdb_id": pdb_id, "resolution": 1.4 + idx * 0.1},
            "protein": {"sequence_length": 90 + idx},
            "ligand": {"ligand_id": f"LU{idx:03d}", "molecular_weight": 160.0 + idx},
            "interaction": {"interface_residue_count": 4 + idx},
            "experiment": {"source_database": "RCSB"},
            "graph_features": {"network_degree": idx % 3},
            "provenance": {"pdb_id": pdb_id, "pair_identity_key": f"pair:{pdb_id}"},
            "labels": {},
        })
    (layout.training_dir / "training_examples.json").write_text(json.dumps(examples), encoding="utf-8")

    result = execute_training_run(
        layout,
        starter_config={"family": "clustering", "task": "unsupervised", "training": {"cluster_count": 3, "seed": 9}},
    )
    metrics = json.loads((result.run_dir / "metrics.json").read_text(encoding="utf-8"))
    assert result.task == "unsupervised"
    assert "unsupervised" in metrics
    assert (result.run_dir / "embedding_records.json").exists()
    assert (result.run_dir / "test_performance.svg").exists()
