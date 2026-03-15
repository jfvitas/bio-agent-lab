from pbdata.config import AppConfig
from pbdata.demo_pipeline import simulate_demo_stage
from pbdata.storage import build_storage_layout
from tests.test_model_studio import _tmp_dir


def test_simulate_demo_stage_seeds_workspace_and_updates_stage_state() -> None:
    layout = build_storage_layout(_tmp_dir("demo_pipeline_stage"))

    simulation = simulate_demo_stage(
        layout,
        AppConfig(storage_root=str(layout.root)),
        stage="build-graph",
        repo_root=layout.root,
    )

    assert simulation.status == "done"
    assert any("graph" in line.lower() for line in simulation.lines)
    assert (layout.graph_dir / "graph_nodes.json").exists()
    assert (layout.stage_state_dir / "build-graph.json").exists()


def test_simulate_demo_stage_refreshes_demo_snapshot_stage() -> None:
    layout = build_storage_layout(_tmp_dir("demo_pipeline_snapshot"))

    simulation = simulate_demo_stage(
        layout,
        AppConfig(storage_root=str(layout.root)),
        stage="export-demo-snapshot",
        repo_root=layout.root,
    )

    assert simulation.status == "done"
    assert (layout.feature_reports_dir / "demo_walkthrough.md").exists()
    assert any("walkthrough" in line.lower() or "snapshot" in line.lower() for line in simulation.lines)


def test_simulate_demo_report_and_export_stages_write_expected_artifacts() -> None:
    layout = build_storage_layout(_tmp_dir("demo_pipeline_reports"))
    config = AppConfig(storage_root=str(layout.root))

    simulate_demo_stage(layout, config, stage="report-source-capabilities", repo_root=layout.root)
    simulate_demo_stage(layout, config, stage="export-identity-crosswalk", repo_root=layout.root)
    simulate_demo_stage(layout, config, stage="run-scenario-tests", repo_root=layout.root)

    assert (layout.reports_dir / "source_capabilities.json").exists()
    assert (layout.identity_dir / "protein_identity_crosswalk.csv").exists()
    assert (layout.qa_dir / "scenario_test_report.json").exists()


def test_simulate_demo_custom_training_set_reflects_selection_context() -> None:
    layout = build_storage_layout(_tmp_dir("demo_pipeline_custom_set"))
    config = AppConfig(storage_root=str(layout.root))

    simulation = simulate_demo_stage(
        layout,
        config,
        stage="build-custom-training-set",
        repo_root=layout.root,
        context={"custom_set_mode": "protein_ligand", "custom_set_target_size": "180"},
    )

    assert any("protein-ligand" in line.lower() for line in simulation.lines)
    scorecard = (layout.root / "custom_training_scorecard.json").read_text(encoding="utf-8")
    assert "protein_ligand" in scorecard
