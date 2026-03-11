import json
from pathlib import Path
from uuid import uuid4

from typer.testing import CliRunner

from pbdata.cli import app

_LOCAL_TMP = Path(__file__).parent / "_tmp"
_LOCAL_TMP.mkdir(exist_ok=True)


def _tmp_dir(name: str) -> Path:
    path = _LOCAL_TMP / f"{uuid4().hex}_{name}"
    path.mkdir(exist_ok=True)
    return path


def test_build_release_creates_versioned_snapshot() -> None:
    tmp_root = _tmp_dir("build_release")
    storage_root = tmp_root / "storage"
    (tmp_root / "master_pdb_repository.csv").write_text("pdb_id,title\n1ABC,Example\n", encoding="utf-8")
    (tmp_root / "master_pdb_pairs.csv").write_text(
        "pdb_id,pair_identity_key,source_database,binding_affinity_type,binding_affinity_value,binding_affinity_unit,binding_affinity_log10_standardized,reported_measurements_text,reported_measurement_mean_log10_standardized,reported_measurement_count,source_conflict_flag,source_conflict_summary,source_agreement_band,selected_preferred_source,selected_preferred_source_rationale,receptor_chain_ids,receptor_uniprot_ids,ligand_key,ligand_component_ids,ligand_inchikeys,ligand_types,matching_interface_count,matching_interface_types,assay_field_confidence_json\n"
        "1ABC,protein_ligand|1ABC|A|ATP|wt,PDBbind,Kd,5,nM,0.699,\"PDBbind:Kd=5 nM\",0.699,1,false,,high,PDBbind,\"single_source:PDBbind\",A,P12345,ATP,ATP,ATP-KEY,small_molecule,1,protein_ligand,\"{}\"\n",
        encoding="utf-8",
    )
    (tmp_root / "master_pdb_issues.csv").write_text("scope,pdb_id,pair_identity_key,issue_type,details\n", encoding="utf-8")
    (tmp_root / "custom_training_set.csv").write_text("pdb_id,pair_identity_key\n1ABC,protein_ligand|1ABC|A|ATP|wt\n", encoding="utf-8")
    (tmp_root / "custom_training_summary.json").write_text('{"selected_count": 1}', encoding="utf-8")
    (tmp_root / "custom_training_scorecard.json").write_text('{"selected_count": 1, "benchmark_modes": []}', encoding="utf-8")
    (tmp_root / "custom_training_split_benchmark.csv").write_text("benchmark_mode,group_count\nreceptor_cluster,1\n", encoding="utf-8")
    (tmp_root / "custom_training_manifest.json").write_text('{"selection_mode": "generalist"}', encoding="utf-8")
    (tmp_root / "custom_training_exclusions.csv").write_text("pdb_id,reason\n", encoding="utf-8")
    (storage_root / "data" / "training_examples").mkdir(parents=True, exist_ok=True)
    (storage_root / "data" / "training_examples" / "training_examples.json").write_text(json.dumps([]), encoding="utf-8")
    (storage_root / "data" / "splits").mkdir(parents=True, exist_ok=True)

    runner = CliRunner()
    original_cwd = Path.cwd()
    try:
        import os

        os.chdir(tmp_root)
        result = runner.invoke(
            app,
            ["--storage-root", str(storage_root), "build-release", "--tag", "v1-test"],
            catch_exceptions=False,
        )
    finally:
        os.chdir(original_cwd)

    assert result.exit_code == 0
    snapshot_dir = storage_root / "data" / "releases" / "v1-test"
    assert snapshot_dir.exists()
    assert (snapshot_dir / "model_ready_pairs.csv").exists()
    assert (snapshot_dir / "dataset_release_manifest.json").exists()
    assert (snapshot_dir / "scientific_coverage_summary.json").exists()
    assert (snapshot_dir / "custom_training_set.csv").exists()
    assert (snapshot_dir / "custom_training_scorecard.json").exists()
    assert (snapshot_dir / "custom_training_split_benchmark.csv").exists()
    assert (snapshot_dir / "release_snapshot_manifest.json").exists()
    assert (snapshot_dir / "master_pdb_repository.csv").exists()
    assert (storage_root / "data" / "releases" / "latest_release.json").exists()
    assert "Release snapshot:" in result.output
    assert "Coverage JSON" in result.output
    assert "Latest Release" in result.output
