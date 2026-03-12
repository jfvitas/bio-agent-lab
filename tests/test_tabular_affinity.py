import json
from pathlib import Path
from uuid import uuid4

from typer.testing import CliRunner

from pbdata.cli import app
from pbdata.models.tabular_affinity import (
    evaluate_tabular_affinity_model,
    train_tabular_affinity_model,
)
from pbdata.storage import build_storage_layout
from tests.test_baseline_memory import _write_training_fixture

_LOCAL_TMP = Path(__file__).parent / "_tmp"
_LOCAL_TMP.mkdir(exist_ok=True)


def _tmp_dir(name: str) -> Path:
    path = _LOCAL_TMP / f"{uuid4().hex}_{name}"
    path.mkdir(exist_ok=True)
    return path


def test_train_tabular_affinity_model_writes_artifact() -> None:
    tmp_root = _tmp_dir("tabular_affinity_train")
    layout = build_storage_layout(tmp_root)
    _write_training_fixture(layout)

    out_path, manifest = train_tabular_affinity_model(layout)

    assert out_path.exists()
    assert manifest["status"] == "trained"
    assert manifest["training_example_count"] == 2
    saved = json.loads(out_path.read_text(encoding="utf-8"))
    assert saved["model_type"] == "tabular_affinity_linear_regression"
    assert len(saved["feature_names"]) >= 1
    assert len(saved["weights"]) == len(saved["feature_names"]) + 1
    assert any(str(name).startswith("ligand_smiles.hash_") for name in saved["feature_names"])
    assert any(str(name).startswith("target_id.hash_") for name in saved["feature_names"])


def test_evaluate_tabular_affinity_model_reports_split_metrics_and_baseline_comparison() -> None:
    tmp_root = _tmp_dir("tabular_affinity_eval")
    layout = build_storage_layout(tmp_root)
    _write_training_fixture(layout)

    out_path, summary = evaluate_tabular_affinity_model(layout)

    assert out_path.exists()
    assert summary["status"] == "trained"
    assert summary["splits"]["val"]["evaluated_count"] == 1
    assert summary["splits"]["val"]["affinity_mae_log10"] is not None
    assert summary["splits"]["val"]["same_ligand_in_train_count"] == 1
    assert summary["baseline_comparison"]["val"]["baseline_top1_target_accuracy"] == 1.0


def test_train_and_evaluate_tabular_affinity_model_cli() -> None:
    tmp_root = _tmp_dir("tabular_affinity_cli")
    layout = build_storage_layout(tmp_root)
    _write_training_fixture(layout)
    runner = CliRunner()

    train_result = runner.invoke(
        app,
        ["--storage-root", str(tmp_root), "train-tabular-affinity-model"],
        catch_exceptions=False,
    )
    eval_result = runner.invoke(
        app,
        ["--storage-root", str(tmp_root), "evaluate-tabular-affinity-model"],
        catch_exceptions=False,
    )

    assert train_result.exit_code == 0
    assert eval_result.exit_code == 0
    assert (layout.models_dir / "tabular_affinity_model.json").exists()
    assert (layout.models_dir / "tabular_affinity_evaluation.json").exists()
