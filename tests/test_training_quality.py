import json

from typer.testing import CliRunner

from pbdata.cli import app
from pbdata.config import AppConfig
from pbdata.gui_overview import build_gui_overview_snapshot
from pbdata.storage import build_storage_layout
from pbdata.training_quality import build_training_set_quality_report, export_training_set_quality_report
from tests.test_baseline_memory import _write_training_fixture
from tests.test_feature_execution import _tmp_dir


def test_build_training_set_quality_report_counts_and_overlap() -> None:
    layout = build_storage_layout(_tmp_dir("training_quality"))
    _write_training_fixture(layout)

    report = build_training_set_quality_report(layout)

    assert report["counts"]["example_count"] == 3
    assert report["counts"]["supervised_count"] == 3
    assert report["counts"]["unique_pair_count"] == 3
    assert report["split_counts"]["train"] == 2
    assert report["split_counts"]["val"] == 1
    assert report["overlap_with_train"]["val"]["same_target_in_train"] == 1
    assert report["overlap_with_train"]["val"]["same_ligand_in_train"] == 1
    assert report["overlap_with_train"]["val"]["exact_pair_seen_in_train"] == 0
    assert report["missing_field_counts"]["binding_affinity_log10"] == 0
    assert report["fractions"]["degraded_fraction"] == 0.0


def test_export_training_set_quality_report_and_gui_snapshot() -> None:
    layout = build_storage_layout(_tmp_dir("training_quality_export"))
    _write_training_fixture(layout)
    (layout.splits_dir / "split_diagnostics.json").write_text(json.dumps({
        "status": "ready",
        "strategy": "pair_aware_grouped",
        "summary": "Pair-aware split looks acceptable.",
        "next_action": "Inspect diagnostics.",
        "counts": {"hard_group_overlap_count": 0, "family_overlap_count": 1},
        "dominance": {
            "train": {"family_key": {"largest_group_fraction": 0.5}},
            "val": {"family_key": {"largest_group_fraction": 1.0}},
            "test": {"family_key": {"largest_group_fraction": 1.0}},
        },
    }), encoding="utf-8")

    json_path, md_path, report = export_training_set_quality_report(layout)
    snapshot = build_gui_overview_snapshot(layout, AppConfig(), repo_root=layout.root)

    assert json_path.exists()
    assert md_path.exists()
    saved = json.loads(json_path.read_text(encoding="utf-8"))
    assert saved["status"] == report["status"]
    assert snapshot.training_quality_summary["status"] == report["status"]
    assert snapshot.training_quality_kpis["examples"] == "3"
    assert snapshot.split_diagnostics_summary["status"] == "ready"
    assert snapshot.split_diagnostics_kpis["strategy"] == "pair_aware_grouped"


def test_report_training_set_quality_cli() -> None:
    layout = build_storage_layout(_tmp_dir("training_quality_cli"))
    _write_training_fixture(layout)
    runner = CliRunner()

    result = runner.invoke(
        app,
        ["--storage-root", str(layout.root), "report-training-set-quality"],
        catch_exceptions=False,
    )

    assert result.exit_code == 0
    assert "Training quality JSON" in result.output
    assert (layout.reports_dir / "training_set_quality.json").exists()


def test_build_training_set_quality_report_surfaces_missingness_blockers() -> None:
    layout = build_storage_layout(_tmp_dir("training_quality_missingness"))
    layout.training_dir.mkdir(parents=True, exist_ok=True)
    layout.splits_dir.mkdir(parents=True, exist_ok=True)
    (layout.training_dir / "training_examples.json").write_text(json.dumps([
        {
            "example_id": "train:1ABC:0",
            "structure": {"pdb_id": "1ABC"},
            "protein": {"uniprot_id": ""},
            "ligand": {"ligand_id": "ATP", "smiles": ""},
            "interaction": {},
            "experiment": {"affinity_type": ""},
            "graph_features": {},
            "labels": {},
            "provenance": {
                "pair_identity_key": "protein_ligand|1ABC|A|ATP|wt",
                "degraded_mode": True,
            },
        }
    ]), encoding="utf-8")

    report = build_training_set_quality_report(layout)

    assert report["status"] == "weak_supervision"
    assert report["counts"]["degraded_count"] == 1
    assert report["missing_field_counts"]["binding_affinity_log10"] == 1
    assert report["missing_field_counts"]["target_id"] == 1
    assert report["missing_field_counts"]["ligand_smiles"] == 1
    assert report["supervision_blockers"]["missing_affinity_log10"] == 1
    assert "standardized affinity coverage" in report["next_action"]


def test_build_training_set_quality_report_marks_tiny_corpus_undersized() -> None:
    layout = build_storage_layout(_tmp_dir("training_quality_undersized"))
    layout.training_dir.mkdir(parents=True, exist_ok=True)
    layout.splits_dir.mkdir(parents=True, exist_ok=True)
    (layout.training_dir / "training_examples.json").write_text(json.dumps([
        {
            "example_id": "train:1ABC:0",
            "structure": {"pdb_id": "1ABC"},
            "protein": {"uniprot_id": "P12345"},
            "ligand": {"ligand_id": "ATP", "smiles": "ATP-SMILES"},
            "interaction": {},
            "experiment": {"affinity_type": "Kd"},
            "graph_features": {},
            "labels": {"binding_affinity_log10": 0.7, "affinity_type": "Kd"},
            "provenance": {
                "pair_identity_key": "protein_ligand|1ABC|A|ATP|wt",
                "has_graph_data": False,
                "source_database": "ChEMBL",
            },
        }
    ]), encoding="utf-8")

    report = build_training_set_quality_report(layout)

    assert report["status"] == "undersized"
    assert "too small" in report["next_action"]


def test_training_quality_uses_split_diagnostics_and_metadata_context() -> None:
    layout = build_storage_layout(_tmp_dir("training_quality_split_metadata"))
    layout.training_dir.mkdir(parents=True, exist_ok=True)
    layout.splits_dir.mkdir(parents=True, exist_ok=True)
    layout.workspace_metadata_dir.mkdir(parents=True, exist_ok=True)
    (layout.training_dir / "training_examples.json").write_text(json.dumps([
        {
            "example_id": "train:1ABC:0",
            "structure": {"pdb_id": "1ABC"},
            "protein": {"uniprot_id": "P12345"},
            "ligand": {"ligand_id": "ATP", "smiles": "ATP-SMILES"},
            "interaction": {},
            "experiment": {"affinity_type": "Kd"},
            "graph_features": {},
            "labels": {"binding_affinity_log10": 0.7, "affinity_type": "Kd"},
            "provenance": {
                "pair_identity_key": "protein_ligand|1ABC|A|ATP|wt",
                "source_database": "BindingDB",
            },
        }
    ]), encoding="utf-8")
    (layout.splits_dir / "split_diagnostics.json").write_text(json.dumps({
        "status": "leakage_risk",
        "counts": {
            "hard_group_overlap_count": 1,
            "family_overlap_count": 1,
            "domain_overlap_count": 1,
            "pathway_overlap_count": 1,
            "fold_overlap_count": 0,
        },
    }), encoding="utf-8")
    (layout.workspace_metadata_dir / "protein_metadata.csv").write_text(
        "pdb_id,pair_identity_key,interpro_ids,reactome_pathway_ids\n"
        "1ABC,protein_ligand|1ABC|A|ATP|wt,IPR0001,R-HSA-1\n",
        encoding="utf-8",
    )

    report = build_training_set_quality_report(layout)

    assert report["status"] == "undersized" or report["status"] == "split_leakage_risk"
    assert report["split_diagnostics"]["status"] == "leakage_risk"
    assert report["counts"]["unique_metadata_family_count"] == 1
    assert report["counts"]["unique_pathway_count"] == 1
