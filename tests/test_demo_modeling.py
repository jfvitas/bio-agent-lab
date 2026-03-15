from pbdata.config import AppConfig
from pbdata.demo_modeling import simulate_model_training_run, simulate_saved_model_inference
from pbdata.storage import build_storage_layout
from tests.test_model_studio import _tmp_dir


def test_simulate_model_training_run_creates_saved_run() -> None:
    layout = build_storage_layout(_tmp_dir("demo_model_training"))

    result = simulate_model_training_run(
        layout,
        AppConfig(storage_root=str(layout.root)),
        starter_config={"family": "hybrid_fusion", "task": "regression", "model_id": "demo_hybrid"},
        runtime_target="local_gpu",
        repo_root=layout.root,
    )

    assert result.family == "hybrid_fusion"
    assert result.run_dir.exists()
    assert (result.run_dir / "run_manifest.json").exists()
    assert (result.run_dir / "training_curve.svg").exists()
    assert "demo_mode_simulated_outputs" in result.warnings


def test_simulate_saved_model_inference_returns_demo_payload() -> None:
    layout = build_storage_layout(_tmp_dir("demo_model_inference"))
    run = simulate_model_training_run(
        layout,
        AppConfig(storage_root=str(layout.root)),
        starter_config={"family": "xgboost", "task": "regression", "model_id": "demo_tree"},
        runtime_target="local_cpu",
        repo_root=layout.root,
    )

    result = simulate_saved_model_inference(
        layout,
        AppConfig(storage_root=str(layout.root)),
        run_dir=run.run_dir,
        pdb_id="D010",
        repo_root=layout.root,
    )

    assert result["pdb_id"] == "D010"
    assert result["simulated"] is True
    assert "disclaimer" in result


def test_simulate_model_training_run_changes_metrics_by_family() -> None:
    layout = build_storage_layout(_tmp_dir("demo_model_family_branch"))
    cfg = AppConfig(storage_root=str(layout.root))

    tabular = simulate_model_training_run(
        layout,
        cfg,
        starter_config={"family": "random_forest", "task": "regression", "model_id": "demo_rf"},
        runtime_target="local_cpu",
        repo_root=layout.root,
    )
    graph = simulate_model_training_run(
        layout,
        cfg,
        starter_config={
            "family": "hybrid_fusion",
            "task": "regression",
            "model_id": "demo_hybrid",
            "compute_budget": "high",
            "modality": "graphs+attributes",
        },
        runtime_target="local_gpu",
        repo_root=layout.root,
    )

    assert float(graph.metrics["test"]["rmse"]) < float(tabular.metrics["test"]["rmse"])
    assert "demo_narrative" in graph.metrics
