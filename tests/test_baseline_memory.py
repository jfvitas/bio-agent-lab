import json
from pathlib import Path
from uuid import uuid4

from typer.testing import CliRunner

from pbdata.cli import app
from pbdata.models.baseline_memory import (
    evaluate_ligand_memory_model,
    train_ligand_memory_model,
)
from pbdata.data_pipeline.workflow_engine import harvest_unified_metadata, initialize_workspace
from pbdata.graph.structural_graphs import build_structural_graphs
from pbdata.storage import build_storage_layout
from tests.test_feature_execution import _write_extracted_fixture

_LOCAL_TMP = Path(__file__).parent / "_tmp"
_LOCAL_TMP.mkdir(exist_ok=True)


def _tmp_dir(name: str) -> Path:
    path = _LOCAL_TMP / f"{uuid4().hex}_{name}"
    path.mkdir(exist_ok=True)
    return path


def _write_training_fixture(layout) -> None:
    layout.training_dir.mkdir(parents=True, exist_ok=True)
    layout.splits_dir.mkdir(parents=True, exist_ok=True)
    examples = [
        {
            "example_id": "train:1ABC:0",
            "structure": {"pdb_id": "1ABC", "resolution": 2.1},
            "protein": {"uniprot_id": "P12345", "sequence_length": 320, "charged_fraction": 0.18},
            "ligand": {"ligand_id": "ATP", "smiles": "CCO", "molecular_weight": 507.0},
            "interaction": {"microstate_record_count": 4, "opposite_charge_contact_count": 3, "acidic_cluster_penalty": 0.2},
            "experiment": {"affinity_type": "Kd", "reported_measurement_count": 2},
            "graph_features": {"network_degree": 8, "pathway_count": 3},
            "labels": {"binding_affinity_log10": 1.0, "affinity_type": "Kd"},
            "provenance": {
                "pair_identity_key": "protein_ligand|1ABC|A|ATP|wt",
                "source_database": "PDBbind",
                "source_agreement_band": "high",
            },
        },
        {
            "example_id": "train:2DEF:1",
            "structure": {"pdb_id": "2DEF", "resolution": 2.8},
            "protein": {"uniprot_id": "Q99999", "sequence_length": 250, "charged_fraction": 0.12},
            "ligand": {"ligand_id": "GTP", "smiles": "NNNN", "molecular_weight": 523.0},
            "interaction": {"microstate_record_count": 1, "opposite_charge_contact_count": 0, "acidic_cluster_penalty": 1.7},
            "experiment": {"affinity_type": "Kd", "reported_measurement_count": 1},
            "graph_features": {"network_degree": 2, "pathway_count": 0},
            "labels": {"binding_affinity_log10": 2.0, "affinity_type": "Kd", "source_conflict_flag": True},
            "provenance": {
                "pair_identity_key": "protein_ligand|2DEF|A|GTP|wt",
                "source_database": "BindingDB",
                "source_agreement_band": "low",
            },
        },
        {
            "example_id": "train:3GHI:2",
            "structure": {"pdb_id": "3GHI", "resolution": 2.0},
            "protein": {"uniprot_id": "P12345", "sequence_length": 321, "charged_fraction": 0.17},
            "ligand": {"ligand_id": "ATP", "smiles": "CCO", "molecular_weight": 507.0},
            "interaction": {"microstate_record_count": 5, "opposite_charge_contact_count": 4, "acidic_cluster_penalty": 0.1},
            "experiment": {"affinity_type": "Kd", "reported_measurement_count": 2},
            "graph_features": {"network_degree": 9, "pathway_count": 4},
            "labels": {"binding_affinity_log10": 1.1, "affinity_type": "Kd"},
            "provenance": {
                "pair_identity_key": "protein_ligand|3GHI|A|ATP|wt",
                "source_database": "PDBbind",
                "source_agreement_band": "high",
            },
        },
    ]
    (layout.training_dir / "training_examples.json").write_text(json.dumps(examples), encoding="utf-8")
    (layout.splits_dir / "train.txt").write_text("train:1ABC:0\ntrain:2DEF:1\n", encoding="utf-8")
    (layout.splits_dir / "val.txt").write_text("train:3GHI:2\n", encoding="utf-8")
    (layout.splits_dir / "test.txt").write_text("", encoding="utf-8")


def test_train_ligand_memory_model_writes_artifact() -> None:
    tmp_root = _tmp_dir("train_memory_model")
    layout = build_storage_layout(tmp_root)
    _write_training_fixture(layout)

    out_path, manifest = train_ligand_memory_model(layout)

    assert out_path.exists()
    assert manifest["status"] == "trained"
    assert manifest["training_example_count"] == 2
    assert manifest["training_exemplar_count"] == 2
    assert "scoring_weights" in manifest
    assert set(manifest["scoring_weights"]) == {
        "intercept",
        "affinity_strength",
        "target_prior_score",
        "query_context_alignment",
    }
    saved = json.loads(out_path.read_text(encoding="utf-8"))
    assert saved["target_count"] == 2
    assert "graph_features.network_degree" in saved["numeric_feature_keys"]
    assert isinstance(saved["target_profiles"], dict)


def test_train_ligand_memory_model_uses_structural_graph_summaries_when_available() -> None:
    tmp_root = _tmp_dir("train_memory_model_graphs")
    layout = build_storage_layout(tmp_root)
    _write_training_fixture(layout)
    _write_extracted_fixture(layout)
    initialize_workspace(layout)
    harvest_unified_metadata(layout)
    build_structural_graphs(layout, graph_level="residue", scope="whole_protein", export_formats=("pyg",))

    out_path, manifest = train_ligand_memory_model(layout)

    assert out_path.exists()
    assert "structural_graph.node_count" in manifest["numeric_feature_keys"]
    assert any(
        "structural_graph.node_count" in exemplar.get("numeric_features", {})
        for exemplar in manifest["exemplars"]
    )


def test_evaluate_ligand_memory_model_scores_validation_split() -> None:
    tmp_root = _tmp_dir("eval_memory_model")
    layout = build_storage_layout(tmp_root)
    _write_training_fixture(layout)

    out_path, summary = evaluate_ligand_memory_model(layout)

    assert out_path.exists()
    assert summary["status"] == "evaluated"
    assert summary["splits"]["val"]["evaluated_count"] == 1
    assert summary["splits"]["val"]["no_prediction_count"] == 0
    assert summary["splits"]["val"]["top1_target_accuracy"] == 1.0
    assert summary["splits"]["val"]["top3_target_accuracy"] == 1.0
    assert summary["splits"]["val"]["same_ligand_in_train_count"] == 1
    assert summary["splits"]["val"]["same_target_in_train_count"] == 1
    assert summary["splits"]["val"]["exact_pair_seen_in_train_count"] == 0
    assert summary["splits"]["val"]["novel_case_count"] == 0
    assert summary["splits"]["val"]["affinity_mae_log10"] is not None
    assert set(summary["scoring_weights"]) == {
        "intercept",
        "affinity_strength",
        "target_prior_score",
        "query_context_alignment",
    }


def test_train_and_evaluate_baseline_model_cli() -> None:
    tmp_root = _tmp_dir("baseline_model_cli")
    layout = build_storage_layout(tmp_root)
    _write_training_fixture(layout)
    runner = CliRunner()

    train_result = runner.invoke(
        app,
        ["--storage-root", str(tmp_root), "train-baseline-model"],
        catch_exceptions=False,
    )
    eval_result = runner.invoke(
        app,
        ["--storage-root", str(tmp_root), "evaluate-baseline-model"],
        catch_exceptions=False,
    )

    assert train_result.exit_code == 0
    assert eval_result.exit_code == 0
    assert (layout.models_dir / "ligand_memory_model.json").exists()
    assert (layout.models_dir / "ligand_memory_evaluation.json").exists()
