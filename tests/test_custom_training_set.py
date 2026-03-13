import csv
import json
from pathlib import Path
from uuid import uuid4

from typer.testing import CliRunner

from pbdata.cli import app
from pbdata.custom_training_set import build_custom_training_set
from pbdata.storage import build_storage_layout

_LOCAL_TMP = Path(__file__).parent / "_tmp"
_LOCAL_TMP.mkdir(exist_ok=True)


def _tmp_dir(name: str) -> Path:
    path = _LOCAL_TMP / f"{uuid4().hex}_{name}"
    path.mkdir(exist_ok=True)
    return path


def _write_selection_fixtures(tmp_root: Path) -> Path:
    storage_root = tmp_root / "storage"
    layout = build_storage_layout(storage_root)
    (layout.extracted_dir / "chains").mkdir(parents=True, exist_ok=True)
    (layout.extracted_dir / "chains" / "all.json").write_text(json.dumps([
        {"pdb_id": "1ABC", "chain_id": "A", "polymer_sequence": "M" * 180},
        {"pdb_id": "2DEF", "chain_id": "A", "polymer_sequence": "M" * 180},
        {"pdb_id": "3GHI", "chain_id": "A", "polymer_sequence": "A" * 220},
        {"pdb_id": "4JKL", "chain_id": "A", "polymer_sequence": "Q" * 260},
    ]), encoding="utf-8")
    (tmp_root / "master_pdb_repository.csv").write_text(
        "pdb_id,title,experimental_method,membrane_vs_soluble,oligomeric_state,homomer_or_heteromer,taxonomy_ids,quality_score,field_confidence_json\n"
        "1ABC,Kinase ATP,X-RAY DIFFRACTION,soluble,monomeric,heteromer,9606,0.91,\"{}\"\n"
        "2DEF,Kinase GTP,X-RAY DIFFRACTION,soluble,monomeric,heteromer,9606,0.90,\"{}\"\n"
        "3GHI,Complex PPI,ELECTRON MICROSCOPY,membrane,dimeric,heteromer,10090,0.88,\"{}\"\n"
        "4JKL,Mutant Complex,ELECTRON MICROSCOPY,soluble,dimeric,heteromer,9606,0.86,\"{}\"\n",
        encoding="utf-8",
    )
    (tmp_root / "master_pdb_pairs.csv").write_text(
        "pdb_id,pair_identity_key,source_database,binding_affinity_type,binding_affinity_value,binding_affinity_unit,binding_affinity_log10_standardized,reported_measurements_text,reported_measurement_mean_log10_standardized,reported_measurement_count,source_conflict_flag,source_conflict_summary,source_agreement_band,selected_preferred_source,selected_preferred_source_rationale,receptor_chain_ids,receptor_uniprot_ids,ligand_key,ligand_component_ids,ligand_inchikeys,ligand_types,matching_interface_count,matching_interface_types,assay_field_confidence_json,release_split\n"
        "1ABC,protein_ligand|1ABC|A|ATP|wt,PDBbind,Kd,5,nM,0.699,\"PDBbind:Kd=5 nM\",0.699,2,false,,high,PDBbind,preferred,A,P11111,ATP,ATP,ATPKEY,small_molecule,1,protein_ligand,\"{}\",train\n"
        "2DEF,protein_ligand|2DEF|A|GTP|wt,PDBbind,Kd,8,nM,0.903,\"PDBbind:Kd=8 nM\",0.903,1,false,,high,PDBbind,preferred,A,P11111,GTP,GTP,GTPKEY,small_molecule,1,protein_ligand,\"{}\",train\n"
        "3GHI,protein_protein|3GHI|A|B|wt,SKEMPI,Kd,20,nM,1.301,\"SKEMPI:Kd=20 nM\",1.301,1,false,,medium,SKEMPI,preferred,A,Q99999,, , ,protein_protein,1,protein_protein,\"{}\",val\n"
        "4JKL,protein_protein|4JKL|A|B|A23V,SKEMPI,ddG,1,kcal/mol,1.000,\"SKEMPI:ddG=1\",1.000,3,false,,high,SKEMPI,preferred,A,Q88888,, , ,protein_protein,1,protein_protein,\"{}\",test\n",
        encoding="utf-8",
    )
    (tmp_root / "model_ready_pairs.csv").write_text(
        "pdb_id,pair_identity_key,source_database,binding_affinity_type,binding_affinity_value,binding_affinity_unit,binding_affinity_log10_standardized,reported_measurements_text,reported_measurement_mean_log10_standardized,reported_measurement_count,source_conflict_flag,source_conflict_summary,source_agreement_band,selected_preferred_source,selected_preferred_source_rationale,receptor_chain_ids,receptor_uniprot_ids,ligand_key,ligand_component_ids,ligand_inchikeys,ligand_types,matching_interface_count,matching_interface_types,assay_field_confidence_json,release_split,model_ready_policy_version\n"
        "1ABC,protein_ligand|1ABC|A|ATP|wt,PDBbind,Kd,5,nM,0.699,\"PDBbind:Kd=5 nM\",0.699,2,false,,high,PDBbind,preferred,A,P11111,ATP,ATP,ATPKEY,small_molecule,1,protein_ligand,\"{}\",train,v1\n"
        "2DEF,protein_ligand|2DEF|A|GTP|wt,PDBbind,Kd,8,nM,0.903,\"PDBbind:Kd=8 nM\",0.903,1,false,,high,PDBbind,preferred,A,P11111,GTP,GTP,GTPKEY,small_molecule,1,protein_ligand,\"{}\",train,v1\n"
        "3GHI,protein_protein|3GHI|A|B|wt,SKEMPI,Kd,20,nM,1.301,\"SKEMPI:Kd=20 nM\",1.301,1,false,,medium,SKEMPI,preferred,A,Q99999,,,protein_protein,1,protein_protein,\"{}\",val,v1\n"
        "4JKL,protein_protein|4JKL|A|B|A23V,SKEMPI,ddG,1,kcal/mol,1.000,\"SKEMPI:ddG=1\",1.000,3,false,,high,SKEMPI,preferred,A,Q88888,,,protein_protein,1,protein_protein,\"{}\",test,v1\n",
        encoding="utf-8",
    )
    layout.workspace_metadata_dir.mkdir(parents=True, exist_ok=True)
    (layout.workspace_metadata_dir / "protein_metadata.csv").write_text(
        "pdb_id,pair_identity_key,interpro_ids,reactome_pathway_ids,structural_fold\n"
        "1ABC,protein_ligand|1ABC|A|ATP|wt,IPR0001,R-HSA-1,1.10.510.10\n"
        "2DEF,protein_ligand|2DEF|A|GTP|wt,IPR0002,R-HSA-2,1.10.510.10\n"
        "3GHI,protein_protein|3GHI|A|B|wt,IPR0003,R-HSA-3,b.1.1.1\n"
        "4JKL,protein_protein|4JKL|A|B|A23V,IPR0004,R-HSA-4,b.1.1.2\n",
        encoding="utf-8",
    )
    return storage_root


def test_build_custom_training_set_enforces_cluster_diversity() -> None:
    tmp_root = _tmp_dir("custom_training")
    storage_root = _write_selection_fixtures(tmp_root)
    layout = build_storage_layout(storage_root)

    artifacts = build_custom_training_set(
        layout,
        repo_root=tmp_root,
        target_size=2,
        per_receptor_cluster_cap=1,
        seed=7,
    )

    with Path(artifacts["custom_training_set_csv"]).open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    with Path(artifacts["custom_training_summary_json"]).open(encoding="utf-8") as handle:
        summary = json.load(handle)
    with Path(artifacts["custom_training_scorecard_json"]).open(encoding="utf-8") as handle:
        scorecard = json.load(handle)
    with Path(artifacts["custom_training_split_benchmark_csv"]).open(newline="", encoding="utf-8") as handle:
        benchmark_rows = list(csv.DictReader(handle))

    assert len(rows) == 2
    assert len({row["receptor_cluster_key"] for row in rows}) == 2
    assert Path(artifacts["custom_training_manifest_json"]).exists()
    assert summary["selected_count"] == 2
    assert summary["selected_receptor_clusters"] == 2
    assert summary["selected_metadata_families"] == 2
    assert scorecard["diversity"]["selected_receptor_clusters"] == 2
    assert any(row["benchmark_mode"] == "receptor_cluster" for row in benchmark_rows)
    assert any(row["benchmark_mode"] == "metadata_family" for row in benchmark_rows)
    assert any(row["benchmark_mode"] == "pathway_group" for row in benchmark_rows)
    assert any(row["benchmark_mode"] == "fold_group" for row in benchmark_rows)


def test_build_custom_training_set_mutation_mode_prefers_mutants() -> None:
    tmp_root = _tmp_dir("custom_training_mut")
    storage_root = _write_selection_fixtures(tmp_root)
    layout = build_storage_layout(storage_root)

    artifacts = build_custom_training_set(
        layout,
        repo_root=tmp_root,
        mode="mutation_effect",
        target_size=2,
        per_receptor_cluster_cap=1,
    )

    with Path(artifacts["custom_training_set_csv"]).open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))

    assert rows
    assert all(row["mutation_family"] != "wildtype_family" for row in rows)


def test_build_custom_training_set_cli_writes_outputs() -> None:
    tmp_root = _tmp_dir("custom_training_cli")
    storage_root = _write_selection_fixtures(tmp_root)
    runner = CliRunner()
    original_cwd = Path.cwd()
    try:
        import os

        os.chdir(tmp_root)
        result = runner.invoke(
            app,
            [
                "--storage-root", str(storage_root),
                "build-custom-training-set",
                "--mode", "generalist",
                "--target-size", "2",
                "--per-receptor-cluster-cap", "1",
                "--tag", "custom-v1",
            ],
            catch_exceptions=False,
        )
    finally:
        os.chdir(original_cwd)

    assert result.exit_code == 0
    assert (tmp_root / "custom_training_set.csv").exists()
    assert (tmp_root / "custom_training_summary.json").exists()
    assert (tmp_root / "custom_training_scorecard.json").exists()
    assert (tmp_root / "custom_training_split_benchmark.csv").exists()
    assert (storage_root / "data" / "custom_training_sets" / "custom-v1").exists()
    assert "Custom training set" in result.output
    assert "Scorecard" in result.output
