import json
from pathlib import Path

from typer.testing import CliRunner

from pbdata.cli import app
from pbdata.gui import PbdataGUI
from tests.test_feature_execution import _tmp_dir


def test_engineer_dataset_missing_metadata_mentions_harvest_command() -> None:
    runner = CliRunner()
    tmp_root = _tmp_dir("handoff_dataset_msg")

    result = runner.invoke(
        app,
        ["--storage-root", str(tmp_root), "engineer-dataset"],
        catch_exceptions=False,
    )

    assert result.exit_code == 1
    assert "Run 'harvest-metadata' first" in result.output


def test_build_features_strict_prereqs_exits_when_inputs_missing() -> None:
    runner = CliRunner()
    tmp_root = _tmp_dir("handoff_build_features")

    result = runner.invoke(
        app,
        ["--storage-root", str(tmp_root), "build-features", "--strict-prereqs"],
        catch_exceptions=False,
    )

    assert result.exit_code == 1
    assert "Run 'extract' first" in result.output


def test_build_graph_strict_prereqs_exits_when_inputs_missing() -> None:
    runner = CliRunner()
    tmp_root = _tmp_dir("handoff_build_graph")

    result = runner.invoke(
        app,
        ["--storage-root", str(tmp_root), "build-graph", "--strict-prereqs"],
        catch_exceptions=False,
    )

    assert result.exit_code == 1
    assert "Run 'extract' first" in result.output


def test_build_training_examples_without_strict_writes_planned_manifest_notice() -> None:
    runner = CliRunner()
    tmp_root = _tmp_dir("handoff_training_manifest")

    result = runner.invoke(
        app,
        ["--storage-root", str(tmp_root), "build-training-examples"],
        catch_exceptions=False,
    )

    assert result.exit_code == 0
    assert "planned manifest only" in result.output
    manifest = json.loads((tmp_root / "data" / "training_examples" / "training_manifest.json").read_text(encoding="utf-8"))
    assert manifest["status"] == "planned"


def test_gui_build_stage_cmd_uses_strict_prereqs_for_manifest_only_stages() -> None:
    class _Var:
        def __init__(self, value):
            self._value = value

        def get(self):
            return self._value

    gui = PbdataGUI.__new__(PbdataGUI)
    gui._storage_root_var = _Var(r"C:\tmp\pbdata-root")
    gui._workers_var = _Var("1")
    gui._pipeline_execution_mode_var = _Var("hybrid")
    gui._skip_experimental_stages_var = _Var(True)
    gui._site_pipeline_degraded_mode_var = _Var(True)
    gui._site_pipeline_run_id_var = _Var("")
    gui._site_physics_batch_id_var = _Var("")
    gui._structural_graph_level_var = _Var("residue")
    gui._structural_graph_scope_var = _Var("whole_protein")
    gui._structural_graph_exports_var = _Var("pyg")
    gui._split_mode_var = _Var("auto")
    gui._download_structures_var = _Var(True)
    gui._download_pdb_var = _Var(False)
    gui._train_frac_var = _Var("0.70")
    gui._val_frac_var = _Var("0.15")
    gui._split_seed_var = _Var("42")
    gui._hash_only_var = _Var(False)
    gui._jaccard_threshold_var = _Var("0.30")
    gui._release_tag_var = _Var("")
    gui._custom_set_mode_var = _Var("generalist")
    gui._custom_set_target_size_var = _Var("500")
    gui._custom_set_seed_var = _Var("42")
    gui._custom_set_cluster_cap_var = _Var("1")
    gui._engineered_dataset_name_var = _Var("engineered_dataset")
    gui._engineered_dataset_test_frac_var = _Var("0.20")
    gui._engineered_dataset_cv_folds_var = _Var("0")
    gui._engineered_dataset_cluster_count_var = _Var("8")
    gui._engineered_dataset_embedding_backend_var = _Var("auto")
    gui._engineered_dataset_strict_family_var = _Var(False)

    assert "--strict-prereqs" in gui._build_stage_cmd("build-graph")
    assert "--strict-prereqs" in gui._build_stage_cmd("build-features")
    assert "--strict-prereqs" in gui._build_stage_cmd("build-training-examples")


def test_gui_pipeline_stages_for_mode_can_skip_experimental() -> None:
    class _Var:
        def __init__(self, value):
            self._value = value

        def get(self):
            return self._value

    gui = PbdataGUI.__new__(PbdataGUI)
    gui._skip_experimental_stages_var = _Var(True)

    stages = gui._pipeline_stages_for_mode("hybrid")

    assert "build-mm-job-manifests" not in stages
    assert "run-mm-jobs" not in stages
    assert "build-features" in stages


def test_gui_stage_prerequisite_message_covers_engineer_dataset() -> None:
    class _Var:
        def __init__(self, value):
            self._value = value

        def get(self):
            return self._value

    from pbdata.storage import build_storage_layout

    gui = PbdataGUI.__new__(PbdataGUI)
    gui._storage_layout = lambda: build_storage_layout(_tmp_dir("handoff_gui_prereqs"))

    message = gui._stage_prerequisite_message("engineer-dataset")

    assert message is not None
    assert "harvest-metadata" in message
