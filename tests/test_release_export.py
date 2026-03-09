import csv
import json
from pathlib import Path
from uuid import uuid4

from pbdata.release_export import export_release_artifacts
from pbdata.storage import build_storage_layout

_LOCAL_TMP = Path(__file__).parent / "_tmp"
_LOCAL_TMP.mkdir(exist_ok=True)


def _tmp_dir(name: str) -> Path:
    path = _LOCAL_TMP / f"{uuid4().hex}_{name}"
    path.mkdir(exist_ok=True)
    return path


def test_export_release_artifacts_builds_model_ready_and_manifest() -> None:
    tmp_root = _tmp_dir("release_export")
    layout = build_storage_layout(tmp_root / "storage")
    layout.training_dir.mkdir(parents=True, exist_ok=True)
    layout.splits_dir.mkdir(parents=True, exist_ok=True)

    (tmp_root / "master_pdb_repository.csv").write_text(
        "pdb_id,title,structure_file_cif_path,metal_present,cofactor_present,glycan_present,quality_flags,field_confidence_json\n"
        "1ABC,Example,/tmp/1ABC.cif,true,false,false,,\"{}\"\n",
        encoding="utf-8",
    )
    (tmp_root / "master_pdb_pairs.csv").write_text(
        "pdb_id,pair_identity_key,source_database,binding_affinity_type,binding_affinity_value,binding_affinity_unit,binding_affinity_log10_standardized,reported_measurements_text,reported_measurement_mean_log10_standardized,reported_measurement_count,source_conflict_flag,source_conflict_summary,source_agreement_band,selected_preferred_source,selected_preferred_source_rationale,receptor_chain_ids,receptor_uniprot_ids,ligand_key,ligand_component_ids,ligand_inchikeys,ligand_types,matching_interface_count,matching_interface_types,assay_field_confidence_json\n"
        "1ABC,protein_ligand|1ABC|A|ATP|wt,PDBbind,Kd,5,nM,0.699,\"PDBbind:Kd=5 nM\",0.699,1,false,,high,PDBbind,\"single_source:PDBbind\",A,P12345,ATP,ATP,ATP-KEY,small_molecule,1,protein_ligand,\"{}\"\n"
        "1ABC,protein_ligand|1ABC|A|GTP|mutation_unknown:1,BindingDB,Kd,500,nM,2.699,\"BindingDB:Kd=500 nM\",2.699,1,false,,high,BindingDB,\"single_source:BindingDB\",A,P12345,GTP,GTP,GTP-KEY,small_molecule,0,,\"{}\"\n",
        encoding="utf-8",
    )
    (tmp_root / "master_pdb_issues.csv").write_text(
        "scope,pdb_id,pair_identity_key,issue_type,details\n"
        "pair,1ABC,protein_ligand|1ABC|A|GTP|mutation_unknown:1,ambiguous_mutation_context,mutation_unknown\n",
        encoding="utf-8",
    )
    (layout.training_dir / "training_examples.json").write_text(json.dumps([{
        "example_id": "train:1ABC:0",
        "structure": {"pdb_id": "1ABC"},
        "provenance": {"pair_identity_key": "protein_ligand|1ABC|A|ATP|wt"},
        "labels": {"affinity_type": "Kd"},
    }]), encoding="utf-8")
    (layout.splits_dir / "train.txt").write_text("train:1ABC:0\n", encoding="utf-8")
    (layout.splits_dir / "val.txt").write_text("", encoding="utf-8")
    (layout.splits_dir / "test.txt").write_text("", encoding="utf-8")
    (layout.splits_dir / "metadata.json").write_text(json.dumps({
        "strategy": "pair_aware_grouped",
        "sizes": {"train": 1, "val": 0, "test": 0},
    }), encoding="utf-8")

    paths = export_release_artifacts(layout, repo_root=tmp_root)

    assert Path(paths["canonical_entries_csv"]).exists()
    assert Path(paths["canonical_pairs_csv"]).exists()
    assert Path(paths["model_ready_pairs_csv"]).exists()
    assert Path(paths["model_ready_exclusions_csv"]).exists()
    assert Path(paths["split_summary_csv"]).exists()
    assert Path(paths["release_manifest_json"]).exists()
    assert Path(paths["scientific_coverage_json"]).exists()

    with Path(paths["model_ready_pairs_csv"]).open(newline="", encoding="utf-8") as handle:
        model_ready_rows = list(csv.DictReader(handle))
    with Path(paths["model_ready_exclusions_csv"]).open(newline="", encoding="utf-8") as handle:
        exclusion_rows = list(csv.DictReader(handle))
    manifest = json.loads(Path(paths["release_manifest_json"]).read_text(encoding="utf-8"))
    coverage = json.loads(Path(paths["scientific_coverage_json"]).read_text(encoding="utf-8"))

    assert len(model_ready_rows) == 1
    assert model_ready_rows[0]["pair_identity_key"] == "protein_ligand|1ABC|A|ATP|wt"
    assert model_ready_rows[0]["release_split"] == "train"
    assert len(exclusion_rows) == 1
    assert "ambiguous_mutation_context" in exclusion_rows[0]["exclusion_reasons"]
    assert manifest["model_ready_pair_count"] == 1
    assert manifest["model_ready_exclusion_reasons"]["ambiguous_mutation_context"] == 1
    assert coverage["counts"]["entry_count"] == 1
    assert coverage["coverage"]["assay_sources"]["PDBbind"] == 1
    assert coverage["flags"]["metal_present_entries"] == 1
