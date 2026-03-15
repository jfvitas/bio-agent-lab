import json

from pbdata.config import AppConfig
from pbdata.demo import export_demo_snapshot
from pbdata.demo_workspace import demo_manifest_path, seed_demo_workspace
from pbdata.gui_overview import build_gui_overview_snapshot
from pbdata.modeling.studio import build_dataset_profile
from pbdata.modeling.training_runs import compare_training_runs
from pbdata.storage import build_storage_layout
from tests.test_model_studio import _tmp_dir


def test_seed_demo_workspace_populates_demo_ready_artifacts() -> None:
    layout = build_storage_layout(_tmp_dir("demo_workspace"))
    result = seed_demo_workspace(layout, AppConfig(storage_root=str(layout.root)), repo_root=layout.root)

    assert result.seeded is True
    assert demo_manifest_path(layout).exists()
    assert (layout.training_dir / "training_examples.json").exists()
    assert (layout.graph_dir / "graph_nodes.json").exists()
    assert (layout.features_dir / "feature_manifest.json").exists()
    assert (layout.models_dir / "model_studio" / "runs" / "demo_xgboost_affinity" / "training_curve.svg").exists()

    profile = build_dataset_profile(layout)
    assert profile.example_count == 32
    assert profile.dataset_source == "custom_training_set"
    assert profile.graph_ready is True
    assert profile.attribute_ready is True
    assert "graphs+attributes" in profile.modalities_available

    comparisons = compare_training_runs(layout)
    assert len(comparisons) >= 2
    assert any(item.family == "hybrid_fusion" for item in comparisons)

    snapshot = build_gui_overview_snapshot(layout, AppConfig(storage_root=str(layout.root)), repo_root=layout.root)
    assert snapshot.demo_readiness.readiness == "ready_for_internal_demo"
    assert "demo_mode_simulated_outputs" in snapshot.demo_readiness.warnings
    assert snapshot.demo_readiness.summary.startswith("Demo workspace is seeded")
    assert snapshot.model_comparison_summary["status"] == "comparison_ready"

    _, markdown_path, report = export_demo_snapshot(layout, AppConfig(storage_root=str(layout.root)))
    assert markdown_path.exists()
    markdown = markdown_path.read_text(encoding="utf-8")
    assert "## Demo Disclaimer" in markdown
    assert report["readiness"] == snapshot.demo_readiness.readiness


def test_seed_demo_workspace_is_idempotent_without_force() -> None:
    layout = build_storage_layout(_tmp_dir("demo_workspace_idempotent"))
    first = seed_demo_workspace(layout, AppConfig(storage_root=str(layout.root)), repo_root=layout.root)
    second = seed_demo_workspace(layout, AppConfig(storage_root=str(layout.root)), repo_root=layout.root)

    assert first.seeded is True
    assert second.seeded is False
    manifest = json.loads(demo_manifest_path(layout).read_text(encoding="utf-8"))
    assert manifest["simulated"] is True
