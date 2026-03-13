from typer.testing import CliRunner
from unittest.mock import patch

from pbdata.cli import app
from pbdata.config import AppConfig
from pbdata.demo import export_demo_snapshot
from pbdata.gui_overview import build_gui_overview_snapshot, count_files, load_json_dict
from pbdata.models.baseline_memory import evaluate_ligand_memory_model
from pbdata.models.tabular_affinity import evaluate_tabular_affinity_model
from pbdata.ops import build_demo_readiness_report, build_doctor_report, build_status_report
from pbdata.storage import build_storage_layout
from pbdata.workspace_state import (
    build_demo_readiness_report as build_demo_readiness_state_report,
    build_doctor_report as build_doctor_state_report,
    build_status_report as build_status_state_report,
)
from tests.test_baseline_memory import _write_training_fixture
from tests.test_feature_execution import _tmp_dir


def test_status_and_doctor_reports_basic_fields() -> None:
    layout = build_storage_layout(_tmp_dir("ops_status"))
    status = build_status_report(layout)
    doctor = build_doctor_report(layout, AppConfig())
    demo = build_demo_readiness_report(layout, AppConfig())

    assert status["storage_root"] == str(layout.root)
    assert "raw_rcsb_count" in status
    assert "core_pipeline_ready" in status
    assert "active_stage_lock_count" in status
    assert "latest_stage_name" in status
    assert doctor["overall_status"] in {"ready", "missing_required_dependencies"}
    assert "dependency_checks" in doctor
    assert demo["readiness"] == "not_demo_ready"
    assert "no_extracted_entries" in demo["blockers"]
    assert build_status_state_report(layout).to_dict()["storage_root"] == str(layout.root)
    assert build_doctor_state_report(layout, AppConfig()).to_dict()["overall_status"] in {"ready", "missing_required_dependencies"}
    assert build_demo_readiness_state_report(layout, AppConfig()).to_dict()["readiness"] == "not_demo_ready"
    snapshot = build_gui_overview_snapshot(layout, AppConfig(), repo_root=layout.root)
    assert snapshot.demo_readiness.readiness == "not_demo_ready"
    assert "headline" in snapshot.presenter_banner
    assert "Current phase:" in snapshot.presenter_banner["subhead"]
    assert "raw_rcsb" in snapshot.counts
    assert "processed_valid" in snapshot.counts
    assert "processed_issues" in snapshot.counts
    assert snapshot.completion_summary["status"] in {"blocked", "in_progress"}
    assert len(snapshot.completion_rows) == 8
    assert snapshot.completion_rows[0]["area"] == "Source setup"
    assert "release_check" in snapshot.artifact_freshness
    assert snapshot.last_run_summary["status"] == "no_history"
    assert snapshot.workflow_guidance["phase"] == "Build core dataset"
    assert "Ingest Sources" in snapshot.workflow_guidance["step_1"]
    json_path, md_path, exported = export_demo_snapshot(layout, AppConfig())
    assert json_path.exists()
    assert md_path.exists()
    assert exported["readiness"] == demo["readiness"]
    markdown = md_path.read_text(encoding="utf-8")
    assert "# Demo Snapshot" in markdown
    assert "## Recommended Walkthrough" in markdown
    assert "## Ground Rules" in markdown
    assert "Presenter note:" in markdown


def test_gui_overview_snapshot_workflow_guidance_for_model_comparison() -> None:
    layout = build_storage_layout(_tmp_dir("ops_workflow_guidance"))
    _write_training_fixture(layout)
    (layout.extracted_dir / "entry").mkdir(parents=True, exist_ok=True)
    (layout.extracted_dir / "entry" / "1ABC.json").write_text("{}", encoding="utf-8")
    (layout.root / "model_ready_pairs.csv").write_text("pdb_id,pair_identity_key\n1ABC,protein_ligand|1ABC|A|ATP|wt\n", encoding="utf-8")
    evaluate_ligand_memory_model(layout)
    evaluate_tabular_affinity_model(layout)

    snapshot = build_gui_overview_snapshot(layout, AppConfig(), repo_root=layout.root)

    assert snapshot.workflow_guidance["phase"] == "Improve training quality"
    assert "Training Example Quality" in snapshot.workflow_guidance["step_1"]
    assert "rebuild training examples and splits" in snapshot.workflow_guidance["step_2"]
    assert "need to be called out carefully in a demo" in snapshot.workflow_guidance["summary"]
    assert any(row["area"] == "Model comparison" and row["status"] == "done" for row in snapshot.completion_rows)


def test_gui_overview_snapshot_prediction_status_reads_manifest() -> None:
    layout = build_storage_layout(_tmp_dir("ops_prediction_status"))
    (layout.prediction_dir / "ligand_screening").mkdir(parents=True, exist_ok=True)
    (layout.prediction_dir / "ligand_screening" / "prediction_manifest.json").write_text(
        '{"status":"trained_supervised_predictions_generated","prediction_method":"trained_tabular_affinity_model","selected_model_preference":"tabular_affinity","candidate_target_count":2,"query_numeric_feature_count":4,"ranked_target_list":[{"target_id":"P12345","confidence_score":0.8123}],"notes":"Predictions use the supervised tabular affinity model."}',
        encoding="utf-8",
    )

    snapshot = build_gui_overview_snapshot(layout, AppConfig(), repo_root=layout.root)

    assert snapshot.prediction_status_summary["method"] == "trained_tabular_affinity_model"
    assert snapshot.prediction_status_summary["preference"] == "tabular_affinity"
    assert snapshot.prediction_status_kpis["top_target"] == "P12345"
    assert snapshot.prediction_status_kpis["query_features"] == "4"


def test_gui_overview_snapshot_source_run_summary_reads_report() -> None:
    layout = build_storage_layout(_tmp_dir("ops_source_run_summary"))
    layout.reports_dir.mkdir(parents=True, exist_ok=True)
    (layout.reports_dir / "extract_source_run_summary.json").write_text(
        '{"status":"ready","summary":"Observed 3 source attempt(s) across 2 source(s); 5 record(s) were loaded or normalized.","source_count":2,"total_attempt_count":3,"total_records_observed":5,"aggregate_mode_counts":{"live_api":2,"managed_cache":1}}',
        encoding="utf-8",
    )

    snapshot = build_gui_overview_snapshot(layout, AppConfig(), repo_root=layout.root)

    assert snapshot.source_run_summary["status"] == "ready"
    assert snapshot.source_run_kpis["sources"] == "2"
    assert snapshot.source_run_kpis["attempts"] == "3"
    assert snapshot.source_run_kpis["records"] == "5"
    assert snapshot.source_run_kpis["mode"] == "live_api"


def test_gui_overview_snapshot_active_operations_reads_stage_state() -> None:
    layout = build_storage_layout(_tmp_dir("ops_active_operations"))
    layout.stage_state_dir.mkdir(parents=True, exist_ok=True)
    (layout.stage_state_dir / "extract.json").write_text(
        '{"stage":"extract","status":"running","generated_at":"2026-03-12T12:00:00+00:00","notes":"Extraction in progress."}',
        encoding="utf-8",
    )
    locks_dir = layout.stage_state_dir / "locks"
    locks_dir.mkdir(parents=True, exist_ok=True)
    (locks_dir / "extract.lock.json").write_text(
        '{"stage":"extract","pid":999999,"storage_root":"ignored"}',
        encoding="utf-8",
    )
    (layout.stage_state_dir / "normalize.json").write_text(
        '{"stage":"normalize","status":"failed","generated_at":"2026-03-11T12:00:00+00:00","notes":"Normalization failed."}',
        encoding="utf-8",
    )

    snapshot = build_gui_overview_snapshot(layout, AppConfig(), repo_root=layout.root)

    assert snapshot.active_operations_summary["status"] == "attention_needed"
    assert "normalize" in snapshot.active_operations_summary["summary"]
    assert "stale lock detected" in snapshot.active_operations_summary["active_detail"]
    assert "extract (running)" in snapshot.active_operations_summary["latest_detail"]
    assert snapshot.last_run_summary["status"] == "attention_needed"
    assert snapshot.last_run_summary["last_stage"] == "extract"
    assert snapshot.last_run_summary["last_result"] == "running"
    assert snapshot.active_operations_kpis["active"] == "0"
    assert snapshot.active_operations_kpis["running"] == "1"
    assert snapshot.active_operations_kpis["failed"] == "1"
    assert snapshot.active_operations_kpis["stale"] == "1"
    assert snapshot.active_operations_kpis["latest"] == "extract"


def test_gui_overview_snapshot_data_integrity_surfaces_processed_issues() -> None:
    layout = build_storage_layout(_tmp_dir("ops_data_integrity"))
    layout.processed_rcsb_dir.mkdir(parents=True, exist_ok=True)
    (layout.processed_rcsb_dir / "broken.json").write_text("", encoding="utf-8")

    snapshot = build_gui_overview_snapshot(layout, AppConfig(), repo_root=layout.root)

    assert snapshot.data_integrity_summary["status"] == "attention_needed"
    assert "need attention" in snapshot.data_integrity_summary["summary"]
    assert "examples: broken.json" in snapshot.data_integrity_summary["detail"]
    assert snapshot.data_integrity_kpis["issues"] == "1"
    assert snapshot.data_integrity_kpis["invalid"] == "0"
    assert "fresh" in snapshot.data_integrity_kpis["scan"]
    assert snapshot.workflow_guidance["phase"] == "Repair data integrity"


def test_gui_overview_snapshot_handles_comma_formatted_integrity_counts() -> None:
    layout = build_storage_layout(_tmp_dir("ops_large_integrity_counts"))
    layout.processed_rcsb_dir.mkdir(parents=True, exist_ok=True)
    for idx in range(3):
        (layout.processed_rcsb_dir / f"broken_{idx}.json").write_text("", encoding="utf-8")

    snapshot = build_gui_overview_snapshot(layout, AppConfig(), repo_root=layout.root)

    assert snapshot.data_integrity_kpis["issues"] == "3"
    assert any(row["area"] == "Data integrity" for row in snapshot.completion_rows)


def test_status_report_writes_cached_processed_health_report() -> None:
    layout = build_storage_layout(_tmp_dir("ops_processed_health_cache"))
    layout.processed_rcsb_dir.mkdir(parents=True, exist_ok=True)
    (layout.processed_rcsb_dir / "broken.json").write_text("", encoding="utf-8")

    first = build_status_state_report(layout)
    second = build_status_state_report(layout)

    assert first.processed_health_report_json is not None
    assert first.processed_health_report_md is not None
    assert first.processed_health_cache_used is False
    assert second.processed_health_cache_used is True


def test_status_report_does_not_count_completed_with_failures_as_running() -> None:
    layout = build_storage_layout(_tmp_dir("ops_stage_counts"))
    layout.stage_state_dir.mkdir(parents=True, exist_ok=True)
    (layout.stage_state_dir / "audit.json").write_text(
        '{"stage":"audit","status":"completed_with_failures","generated_at":"2026-03-12T12:00:00+00:00"}',
        encoding="utf-8",
    )

    status = build_status_state_report(layout)

    assert status.running_stage_state_count == 0
    assert status.latest_stage_status == "completed_with_failures"


def test_status_report_does_not_count_stale_running_stage_as_live() -> None:
    layout = build_storage_layout(_tmp_dir("ops_stale_running_stage"))
    layout.stage_state_dir.mkdir(parents=True, exist_ok=True)
    (layout.stage_state_dir / "extract.json").write_text(
        '{"stage":"extract","status":"running","generated_at":"2026-03-12T12:00:00+00:00"}',
        encoding="utf-8",
    )
    locks_dir = layout.stage_state_dir / "locks"
    locks_dir.mkdir(parents=True, exist_ok=True)
    (locks_dir / "extract.lock.json").write_text(
        '{"stage":"extract","pid":999999,"storage_root":"ignored"}',
        encoding="utf-8",
    )

    status = build_status_state_report(layout)

    assert status.active_stage_lock_count == 0
    assert status.running_stage_state_count == 0


def test_gui_overview_snapshot_surfaces_schema_invalid_processed_files() -> None:
    layout = build_storage_layout(_tmp_dir("ops_invalid_processed"))
    layout.processed_rcsb_dir.mkdir(parents=True, exist_ok=True)
    (layout.processed_rcsb_dir / "invalid.json").write_text("{}", encoding="utf-8")

    snapshot = build_gui_overview_snapshot(layout, AppConfig(), repo_root=layout.root)

    assert snapshot.data_integrity_summary["status"] == "attention_needed"
    assert snapshot.data_integrity_kpis["invalid"] == "1"
    assert "schema-invalid=1" in snapshot.data_integrity_summary["detail"]


def test_gui_overview_snapshot_can_paint_from_stale_cached_integrity_report() -> None:
    layout = build_storage_layout(_tmp_dir("ops_stale_integrity_cache"))
    layout.processed_rcsb_dir.mkdir(parents=True, exist_ok=True)

    fresh = build_status_state_report(layout)
    assert fresh.processed_rcsb_problem_count == 0

    (layout.processed_rcsb_dir / "broken.json").write_text("", encoding="utf-8")

    stale_snapshot = build_gui_overview_snapshot(
        layout,
        AppConfig(),
        repo_root=layout.root,
        prefer_cached_status=True,
    )
    refreshed_snapshot = build_gui_overview_snapshot(layout, AppConfig(), repo_root=layout.root)

    assert stale_snapshot.data_integrity_kpis["issues"] == "0"
    assert "cached-stale" in stale_snapshot.data_integrity_kpis["scan"]
    assert refreshed_snapshot.data_integrity_kpis["issues"] == "1"


def test_count_files_cache_invalidates_when_directory_changes() -> None:
    layout = build_storage_layout(_tmp_dir("ops_count_cache"))
    target = layout.extracted_dir / "entry"
    target.mkdir(parents=True, exist_ok=True)

    assert count_files(target) == 0
    (target / "1ABC.json").write_text("{}", encoding="utf-8")

    assert count_files(target) == 1


def test_load_json_dict_cache_invalidates_when_file_changes() -> None:
    layout = build_storage_layout(_tmp_dir("ops_json_cache"))
    report_path = layout.reports_dir / "status.json"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text('{"status":"ready"}', encoding="utf-8")

    assert load_json_dict(report_path)["status"] == "ready"
    report_path.write_text('{"status":"blocked"}', encoding="utf-8")

    assert load_json_dict(report_path)["status"] == "blocked"


def test_gui_overview_snapshot_source_configuration_and_crosswalk() -> None:
    layout = build_storage_layout(_tmp_dir("ops_source_config_crosswalk"))
    layout.identity_dir.mkdir(parents=True, exist_ok=True)
    (layout.identity_dir / "identity_crosswalk_summary.json").write_text(
        '{"status":"ready","summary":"2 protein identities, 1 ligand identities, 1 pair identities","next_action":"Inspect fallback mappings before using the crosswalk as a benchmark split or deduplication input.","counts":{"protein_identity_count":2,"ligand_identity_count":1,"pair_identity_count":1,"protein_fallback_count":1,"ligand_fallback_count":0}}',
        encoding="utf-8",
    )
    config = AppConfig.model_validate(
        {
            "sources": {
                "rcsb": {"enabled": True},
                "pdbbind": {"enabled": True, "extra": {}},
            }
        }
    )

    snapshot = build_gui_overview_snapshot(layout, config, repo_root=layout.root)

    assert snapshot.source_configuration_summary["status"] == "needs_configuration"
    assert snapshot.source_configuration_kpis["enabled"] == "4"
    assert snapshot.source_configuration_kpis["misconfigured"] == "1"
    assert snapshot.identity_crosswalk_summary["status"] == "ready"
    assert snapshot.identity_crosswalk_kpis["proteins"] == "2"
    assert snapshot.identity_crosswalk_kpis["fallbacks"] == "1"


def test_gui_overview_snapshot_search_preview_and_release_readiness() -> None:
    layout = build_storage_layout(_tmp_dir("ops_search_preview_release"))
    layout.reports_dir.mkdir(parents=True, exist_ok=True)
    (layout.reports_dir / "rcsb_search_preview.json").write_text(
        '{"status":"ready","selection_mode":"representative_limited","summary":"120 total matches; 40 selected under the current representative limit.","next_action":"Inspect the preview distribution before ingest.","counts":{"total_match_count":120,"selected_match_count":40,"preview_sample_count":40}}',
        encoding="utf-8",
    )
    (layout.root / "release_readiness_report.json").write_text(
        '{"release_status":"blocked","blockers":["training_corpus_not_ready"],"warnings":["model_comparison_not_ready"],"counts":{"canonical_entry_count":10,"canonical_pair_count":20,"model_ready_pair_count":5,"held_out_count":2},"split_readiness":{"strategy":"source_grouped"}}',
        encoding="utf-8",
    )

    snapshot = build_gui_overview_snapshot(layout, AppConfig(), repo_root=layout.root)

    assert snapshot.search_preview_summary["status"] == "ready"
    assert snapshot.search_preview_kpis["total"] == "120"
    assert snapshot.search_preview_kpis["mode"] == "representative_limited"
    assert snapshot.release_readiness_summary["status"] == "blocked"
    assert snapshot.release_readiness_kpis["model_ready"] == "5"
    assert snapshot.release_readiness_kpis["held_out"] == "2"
    assert snapshot.release_readiness_kpis["blockers"] == "1"
    assert "release-grade dataset" in snapshot.release_readiness_summary["next_action"]
    assert "not generated yet" in snapshot.artifact_freshness["demo_snapshot"] or "(" in snapshot.artifact_freshness["release_check"]


def test_gui_overview_snapshot_split_diagnostics_surfaces_strategy_and_source_overlap() -> None:
    layout = build_storage_layout(_tmp_dir("ops_split_diagnostics"))
    layout.splits_dir.mkdir(parents=True, exist_ok=True)
    (layout.splits_dir / "metadata.json").write_text(
        '{"strategy":"source_grouped","sizes":{"train":8,"val":2,"test":2}}',
        encoding="utf-8",
    )
    (layout.splits_dir / "split_diagnostics.json").write_text(
        '{"status":"attention_needed","strategy":"source_grouped","summary":"Source overlap still needs review.","next_action":"Inspect source overlap before benchmark claims.","counts":{"hard_group_overlap_count":0,"family_overlap_count":1,"fold_overlap_count":2},"overlap":{"source_group_key":{"overlap_count":3}},"dominance":{"train":{"family_key":{"largest_group_fraction":0.4}},"val":{"family_key":{"largest_group_fraction":0.5}},"test":{"family_key":{"largest_group_fraction":0.25}}}}',
        encoding="utf-8",
    )

    snapshot = build_gui_overview_snapshot(layout, AppConfig(), repo_root=layout.root)

    assert snapshot.split_diagnostics_summary["status"] == "attention_needed"
    assert "Strategy=source_grouped" in snapshot.split_diagnostics_summary["summary"]
    assert snapshot.split_diagnostics_kpis["strategy"] == "source_grouped"
    assert snapshot.split_diagnostics_kpis["held_out"] == "4"
    assert snapshot.split_diagnostics_kpis["source_overlap"] == "3"
    assert snapshot.split_diagnostics_kpis["fold_overlap"] == "2"


def test_gui_overview_snapshot_risk_summary_reads_report() -> None:
    layout = build_storage_layout(_tmp_dir("ops_risk_summary"))
    layout.risk_dir.mkdir(parents=True, exist_ok=True)
    (layout.risk_dir / "pathway_risk_summary.json").write_text(
        '{"status":"graph_context_summary_not_clinical_risk_model","notes":"This is a graph-and-prediction context summary.","risk_score_is_placeholder":false,"severity_level":"medium","risk_score":0.482,"matching_pair_count":3,"pathway_overlap_count":2}',
        encoding="utf-8",
    )

    snapshot = build_gui_overview_snapshot(layout, AppConfig(), repo_root=layout.root)

    assert snapshot.risk_summary["status"] == "graph_context_summary_not_clinical_risk_model"
    assert "biological context" in snapshot.risk_summary["next_action"]
    assert snapshot.risk_kpis["severity"] == "medium"
    assert snapshot.risk_kpis["score"] == "0.482"
    assert snapshot.risk_kpis["matches"] == "3"
    assert snapshot.risk_kpis["pathways"] == "2"


def test_status_and_doctor_cli_commands() -> None:
    runner = CliRunner()
    tmp_path = _tmp_dir("ops_cli")
    stage_state_dir = build_storage_layout(tmp_path).stage_state_dir
    stage_state_dir.mkdir(parents=True, exist_ok=True)
    (stage_state_dir / "extract.json").write_text(
        '{"stage":"extract","status":"completed","generated_at":"2026-03-12T12:00:00+00:00"}',
        encoding="utf-8",
    )

    status_result = runner.invoke(
        app,
        ["--storage-root", str(tmp_path), "status"],
        catch_exceptions=False,
    )
    doctor_result = runner.invoke(
        app,
        ["--storage-root", str(tmp_path), "doctor"],
        catch_exceptions=False,
    )
    demo_result = runner.invoke(
        app,
        ["--storage-root", str(tmp_path), "demo-readiness"],
        catch_exceptions=False,
    )
    export_result = runner.invoke(
        app,
        ["--storage-root", str(tmp_path), "export-demo-snapshot"],
        catch_exceptions=False,
    )

    assert status_result.exit_code == 0
    assert "Storage root" in status_result.output
    assert "Active locks" in status_result.output
    assert "Latest stage" in status_result.output
    assert doctor_result.exit_code == 0
    assert "Overall status" in doctor_result.output
    assert demo_result.exit_code == 0
    assert "Demo readiness" in demo_result.output
    assert export_result.exit_code == 0
    assert "JSON snapshot" in export_result.output


def test_gui_cli_command_dispatches() -> None:
    runner = CliRunner()

    with patch("pbdata.gui.main") as mock_main:
        result = runner.invoke(app, ["gui"], catch_exceptions=False)

    assert result.exit_code == 0
    mock_main.assert_called_once()
