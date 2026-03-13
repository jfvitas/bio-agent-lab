import json

from typer.testing import CliRunner

from pbdata.cli import app
from pbdata.criteria import SearchCriteria, save_criteria
from pbdata.release_export import build_release_readiness_report
from pbdata.config import AppConfig
from pbdata.sources.registry import export_source_capability_report
from pbdata.storage import build_storage_layout
from pbdata.training_quality import export_training_set_quality_report
from tests.test_feature_execution import _tmp_dir


def test_release_readiness_reports_blockers_when_release_surface_is_empty() -> None:
    tmp_path = _tmp_dir("release_readiness_empty")
    layout = build_storage_layout(tmp_path)
    out_path, report = build_release_readiness_report(layout, repo_root=tmp_path)

    assert out_path.exists()
    assert report["release_status"] == "blocked"
    assert "no_canonical_entries" in report["blockers"]
    assert "no_model_ready_pairs" in report["blockers"]


def test_release_check_and_strict_build_release_cli() -> None:
    runner = CliRunner()
    tmp_path = _tmp_dir("release_readiness_cli")

    check_result = runner.invoke(
        app,
        ["--storage-root", str(tmp_path), "release-check"],
        catch_exceptions=False,
    )
    build_result = runner.invoke(
        app,
        ["--storage-root", str(tmp_path), "build-release", "--tag", "empty-release", "--strict"],
        catch_exceptions=False,
    )

    assert check_result.exit_code == 0
    assert "Release status" in check_result.output
    assert build_result.exit_code == 1
    assert "Release blocked" in build_result.output


def test_release_readiness_uses_quality_gates() -> None:
    tmp_path = _tmp_dir("release_readiness_quality_gates")
    layout = build_storage_layout(tmp_path)
    (tmp_path / "canonical_entries.csv").write_text("pdb_id\n1ABC\n", encoding="utf-8")
    (tmp_path / "canonical_pairs.csv").write_text("pdb_id,pair_identity_key\n1ABC,protein_ligand|1ABC|A|ATP|wt\n", encoding="utf-8")
    (tmp_path / "model_ready_pairs.csv").write_text("pdb_id,pair_identity_key\n1ABC,protein_ligand|1ABC|A|ATP|wt\n", encoding="utf-8")
    (tmp_path / "dataset_release_manifest.json").write_text('{"model_ready_pair_count":1}', encoding="utf-8")
    (layout.splits_dir).mkdir(parents=True, exist_ok=True)
    (layout.splits_dir / "metadata.json").write_text('{"strategy":"pair_aware_grouped"}', encoding="utf-8")
    layout.training_dir.mkdir(parents=True, exist_ok=True)
    (layout.training_dir / "training_examples.json").write_text("[]", encoding="utf-8")
    export_training_set_quality_report(layout)
    export_source_capability_report(layout, AppConfig.model_validate({"sources": {"pdbbind": {"enabled": True, "extra": {}}}}))
    save_criteria(SearchCriteria(), layout.root / "configs" / "criteria.yaml")
    (layout.root / "configs").mkdir(parents=True, exist_ok=True)
    (layout.root / "configs" / "sources.yaml").write_text(
        "sources:\n  pdbbind:\n    enabled: true\n    extra: {}\n",
        encoding="utf-8",
    )

    _, report = build_release_readiness_report(layout, repo_root=tmp_path)

    assert report["release_status"] == "blocked"
    assert "source_configuration_incomplete" in report["blockers"]
    assert "training_corpus_not_ready" in report["blockers"]
    assert "no_held_out_split" in report["blockers"]
    assert "quality_gates" in report


def test_release_readiness_ignores_global_identity_fallback_when_pair_mappings_are_exact() -> None:
    tmp_path = _tmp_dir("release_readiness_identity_pairs")
    layout = build_storage_layout(tmp_path)
    (tmp_path / "canonical_entries.csv").write_text("pdb_id\n1ABC\n", encoding="utf-8")
    (tmp_path / "canonical_pairs.csv").write_text("pdb_id,pair_identity_key\n1ABC,protein_ligand|1ABC|A|ATP|wt\n", encoding="utf-8")
    (tmp_path / "model_ready_pairs.csv").write_text("pdb_id,pair_identity_key\n1ABC,protein_ligand|1ABC|A|ATP|wt\n", encoding="utf-8")
    (tmp_path / "dataset_release_manifest.json").write_text('{"model_ready_pair_count":1}', encoding="utf-8")
    layout.splits_dir.mkdir(parents=True, exist_ok=True)
    (layout.splits_dir / "metadata.json").write_text('{"strategy":"pair_aware_grouped"}', encoding="utf-8")
    layout.training_dir.mkdir(parents=True, exist_ok=True)
    (layout.training_dir / "training_examples.json").write_text("[]", encoding="utf-8")
    layout.identity_dir.mkdir(parents=True, exist_ok=True)
    (layout.identity_dir / "identity_crosswalk_summary.json").write_text(
        json.dumps({
            "status": "ready",
            "counts": {
                "protein_identity_count": 100,
                "ligand_identity_count": 100,
                "pair_identity_count": 5,
                "protein_fallback_count": 90,
                "ligand_fallback_count": 95,
                "pair_exact_count": 5,
                "pair_ligand_fallback_count": 0,
                "pair_protein_partial_count": 0,
                "pair_partial_or_unresolved_count": 0,
            },
        }),
        encoding="utf-8",
    )
    export_training_set_quality_report(layout)
    export_source_capability_report(layout, AppConfig.model_validate({"sources": {"pdbbind": {"enabled": True, "extra": {}}}}))
    save_criteria(SearchCriteria(), layout.root / "configs" / "criteria.yaml")
    (layout.root / "configs").mkdir(parents=True, exist_ok=True)
    (layout.root / "configs" / "sources.yaml").write_text(
        "sources:\n  pdbbind:\n    enabled: true\n    extra: {}\n",
        encoding="utf-8",
    )

    _, report = build_release_readiness_report(layout, repo_root=tmp_path)

    assert "identity_crosswalk_contains_many_fallbacks" not in report["warnings"]


def test_release_readiness_warns_for_material_pair_identity_fallbacks() -> None:
    tmp_path = _tmp_dir("release_readiness_identity_threshold")
    layout = build_storage_layout(tmp_path)
    (tmp_path / "canonical_entries.csv").write_text("pdb_id\n1ABC\n", encoding="utf-8")
    (tmp_path / "canonical_pairs.csv").write_text("pdb_id,pair_identity_key\n1ABC,protein_ligand|1ABC|A|ATP|wt\n", encoding="utf-8")
    (tmp_path / "model_ready_pairs.csv").write_text("pdb_id,pair_identity_key\n1ABC,protein_ligand|1ABC|A|ATP|wt\n", encoding="utf-8")
    (tmp_path / "dataset_release_manifest.json").write_text('{"model_ready_pair_count":1}', encoding="utf-8")
    layout.splits_dir.mkdir(parents=True, exist_ok=True)
    (layout.splits_dir / "metadata.json").write_text('{"strategy":"pair_aware_grouped"}', encoding="utf-8")
    layout.training_dir.mkdir(parents=True, exist_ok=True)
    (layout.training_dir / "training_examples.json").write_text(
        json.dumps([{
            "example_id": "train:1ABC:0",
            "protein": {"uniprot_id": "P12345"},
            "ligand": {"smiles": "CCO"},
            "provenance": {"pair_identity_key": "protein_ligand|1ABC|A|ATP|wt", "has_graph_data": True},
            "labels": {"binding_affinity_log10": 1.0, "affinity_type": "Kd"},
        }]),
        encoding="utf-8",
    )
    layout.identity_dir.mkdir(parents=True, exist_ok=True)
    (layout.identity_dir / "identity_crosswalk_summary.json").write_text(
        json.dumps({
            "status": "ready",
            "counts": {
                "protein_identity_count": 100,
                "ligand_identity_count": 100,
                "pair_identity_count": 10,
                "protein_fallback_count": 90,
                "ligand_fallback_count": 95,
                "pair_exact_count": 8,
                "pair_ligand_fallback_count": 0,
                "pair_protein_partial_count": 2,
                "pair_partial_or_unresolved_count": 2,
            },
        }),
        encoding="utf-8",
    )
    export_training_set_quality_report(layout)
    export_source_capability_report(layout, AppConfig.model_validate({"sources": {"pdbbind": {"enabled": True, "extra": {}}}}))
    save_criteria(SearchCriteria(), layout.root / "configs" / "criteria.yaml")
    (layout.root / "configs").mkdir(parents=True, exist_ok=True)
    (layout.root / "configs" / "sources.yaml").write_text(
        "sources:\n  pdbbind:\n    enabled: true\n    extra: {}\n",
        encoding="utf-8",
    )

    _, report = build_release_readiness_report(layout, repo_root=tmp_path)

    assert "identity_crosswalk_contains_many_fallbacks" in report["warnings"]


def test_release_readiness_does_not_warn_for_usable_with_gaps_training_status() -> None:
    tmp_path = _tmp_dir("release_readiness_usable_with_gaps")
    layout = build_storage_layout(tmp_path)
    (tmp_path / "canonical_entries.csv").write_text("pdb_id\n1ABC\n", encoding="utf-8")
    (tmp_path / "canonical_pairs.csv").write_text("pdb_id,pair_identity_key\n1ABC,protein_ligand|1ABC|A|ATP|wt\n", encoding="utf-8")
    (tmp_path / "model_ready_pairs.csv").write_text("pdb_id,pair_identity_key\n1ABC,protein_ligand|1ABC|A|ATP|wt\n", encoding="utf-8")
    (tmp_path / "dataset_release_manifest.json").write_text('{"model_ready_pair_count":1}', encoding="utf-8")
    layout.splits_dir.mkdir(parents=True, exist_ok=True)
    (layout.splits_dir / "metadata.json").write_text('{"strategy":"pair_aware_grouped"}', encoding="utf-8")
    (layout.splits_dir / "train.txt").write_text("", encoding="utf-8")
    (layout.splits_dir / "val.txt").write_text("", encoding="utf-8")
    (layout.splits_dir / "test.txt").write_text("", encoding="utf-8")
    layout.training_dir.mkdir(parents=True, exist_ok=True)
    examples = []
    for idx in range(30):
        examples.append({
            "example_id": f"train:1ABC:{idx}",
            "protein": {"uniprot_id": f"P{idx:05d}"},
            "ligand": {"smiles": f"CCO{idx}"},
            "provenance": {"pair_identity_key": f"protein_ligand|1ABC|A|ATP|wt:{idx}", "has_graph_data": idx % 2 == 0},
            "labels": {"binding_affinity_log10": 1.0 + idx / 100.0, "affinity_type": "Kd"},
        })
    (layout.training_dir / "training_examples.json").write_text(json.dumps(examples), encoding="utf-8")
    layout.identity_dir.mkdir(parents=True, exist_ok=True)
    (layout.identity_dir / "identity_crosswalk_summary.json").write_text(
        json.dumps({
            "status": "ready",
            "counts": {
                "protein_identity_count": 30,
                "ligand_identity_count": 30,
                "pair_identity_count": 30,
                "protein_fallback_count": 0,
                "ligand_fallback_count": 0,
                "pair_exact_count": 30,
                "pair_ligand_fallback_count": 0,
                "pair_protein_partial_count": 0,
                "pair_partial_or_unresolved_count": 0,
            },
        }),
        encoding="utf-8",
    )
    export_training_set_quality_report(layout)
    export_source_capability_report(layout, AppConfig.model_validate({"sources": {"pdbbind": {"enabled": True, "extra": {}}}}))
    save_criteria(SearchCriteria(), layout.root / "configs" / "criteria.yaml")
    (layout.root / "configs").mkdir(parents=True, exist_ok=True)
    (layout.root / "configs" / "sources.yaml").write_text(
        "sources:\n  pdbbind:\n    enabled: true\n    extra: {}\n",
        encoding="utf-8",
    )

    _, report = build_release_readiness_report(layout, repo_root=tmp_path)

    assert "training_corpus_not_release_grade" not in report["warnings"]


def test_release_readiness_warns_for_metadata_overlap_and_missing_family_annotations() -> None:
    tmp_path = _tmp_dir("release_readiness_metadata_overlap")
    layout = build_storage_layout(tmp_path)
    (tmp_path / "canonical_entries.csv").write_text("pdb_id\n1ABC\n", encoding="utf-8")
    (tmp_path / "canonical_pairs.csv").write_text("pdb_id,pair_identity_key\n1ABC,protein_ligand|1ABC|A|ATP|wt\n", encoding="utf-8")
    (tmp_path / "model_ready_pairs.csv").write_text("pdb_id,pair_identity_key\n1ABC,protein_ligand|1ABC|A|ATP|wt\n", encoding="utf-8")
    (tmp_path / "dataset_release_manifest.json").write_text('{"model_ready_pair_count":1}', encoding="utf-8")
    layout.splits_dir.mkdir(parents=True, exist_ok=True)
    (layout.splits_dir / "metadata.json").write_text('{"strategy":"pair_aware_grouped"}', encoding="utf-8")
    (layout.splits_dir / "split_diagnostics.json").write_text(
        json.dumps({
            "status": "attention_needed",
            "counts": {
                "domain_overlap_count": 1,
                "pathway_overlap_count": 1,
                "fold_overlap_count": 0,
            },
        }),
        encoding="utf-8",
    )
    layout.training_dir.mkdir(parents=True, exist_ok=True)
    (layout.training_dir / "training_examples.json").write_text(
        json.dumps([{
            "example_id": "train:1ABC:0",
            "protein": {"uniprot_id": "P12345"},
            "ligand": {"smiles": "CCO"},
            "provenance": {"pair_identity_key": "protein_ligand|1ABC|A|ATP|wt", "has_graph_data": True},
            "labels": {"binding_affinity_log10": 1.0, "affinity_type": "Kd"},
        }]),
        encoding="utf-8",
    )
    export_training_set_quality_report(layout)
    export_source_capability_report(layout, AppConfig())
    save_criteria(SearchCriteria(), layout.root / "configs" / "criteria.yaml")

    _, report = build_release_readiness_report(layout, repo_root=tmp_path)

    assert "metadata_group_overlap_detected" in report["warnings"]
    assert "metadata_family_annotations_missing" in report["warnings"]


def test_release_readiness_surfaces_split_readiness_and_exploratory_strategy_warning() -> None:
    tmp_path = _tmp_dir("release_readiness_split_readiness")
    layout = build_storage_layout(tmp_path)
    (tmp_path / "canonical_entries.csv").write_text("pdb_id\n1ABC\n", encoding="utf-8")
    (tmp_path / "canonical_pairs.csv").write_text("pdb_id,pair_identity_key\n1ABC,protein_ligand|1ABC|A|ATP|wt\n", encoding="utf-8")
    (tmp_path / "model_ready_pairs.csv").write_text("pdb_id,pair_identity_key\n1ABC,protein_ligand|1ABC|A|ATP|wt\n", encoding="utf-8")
    layout.splits_dir.mkdir(parents=True, exist_ok=True)
    (layout.splits_dir / "metadata.json").write_text(
        '{"strategy":"hash","sizes":{"train":8,"val":1,"test":1}}',
        encoding="utf-8",
    )
    (layout.splits_dir / "split_diagnostics.json").write_text(
        json.dumps({
            "status": "ready",
            "counts": {
                "hard_group_overlap_count": 0,
                "family_overlap_count": 0,
                "domain_overlap_count": 0,
                "pathway_overlap_count": 0,
                "fold_overlap_count": 0,
            },
            "overlap": {
                "source_group_key": {"overlap_count": 2},
            },
        }),
        encoding="utf-8",
    )
    layout.training_dir.mkdir(parents=True, exist_ok=True)
    (layout.training_dir / "training_examples.json").write_text(
        json.dumps([{
            "example_id": "train:1ABC:0",
            "protein": {"uniprot_id": "P12345"},
            "ligand": {"smiles": "CCO"},
            "provenance": {"pair_identity_key": "protein_ligand|1ABC|A|ATP|wt", "has_graph_data": True},
            "labels": {"binding_affinity_log10": 1.0, "affinity_type": "Kd"},
        }]),
        encoding="utf-8",
    )
    layout.identity_dir.mkdir(parents=True, exist_ok=True)
    (layout.identity_dir / "identity_crosswalk_summary.json").write_text(
        json.dumps({
            "status": "ready",
            "counts": {
                "protein_identity_count": 1,
                "ligand_identity_count": 1,
                "pair_identity_count": 1,
                "pair_partial_or_unresolved_count": 0,
            },
        }),
        encoding="utf-8",
    )
    export_training_set_quality_report(layout)
    export_source_capability_report(layout, AppConfig())
    save_criteria(SearchCriteria(), layout.root / "configs" / "criteria.yaml")

    _, report = build_release_readiness_report(layout, repo_root=tmp_path)

    assert report["counts"]["held_out_count"] == 2
    assert report["split_readiness"]["strategy"] == "hash"
    assert report["split_readiness"]["strategy_family"] == "exploratory"
    assert "exploratory_split_strategy" in report["warnings"]
    assert "source_group_overlap_detected" in report["warnings"]
