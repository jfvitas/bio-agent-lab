import json
from pathlib import Path
from uuid import uuid4

import gemmi
import pytest
from typer.testing import CliRunner

from pbdata.cli import app
from pbdata.models.baseline_memory import train_ligand_memory_model
from pbdata.models.tabular_affinity import evaluate_tabular_affinity_model, train_tabular_affinity_model
from pbdata.prediction.engine import run_ligand_screening_workflow, run_peptide_binding_workflow
from pbdata.schemas.prediction_input import PredictionInputRecord
from pbdata.storage import build_storage_layout

_LOCAL_TMP = Path(__file__).parent / "_tmp"
_LOCAL_TMP.mkdir(exist_ok=True)


def _tmp_dir(name: str) -> Path:
    path = _LOCAL_TMP / f"{uuid4().hex}_{name}"
    path.mkdir(exist_ok=True)
    return path


def _write_ionizable_context_cif(path: Path) -> None:
    structure = gemmi.Structure()
    structure.name = "EXAMPLE"
    model = gemmi.Model("1")
    chain = gemmi.Chain("A")

    def _residue(name: str, seq_num: int, atom_name: str, element: str, pos: tuple[float, float, float]) -> gemmi.Residue:
        residue = gemmi.Residue()
        residue.name = name
        residue.seqid = gemmi.SeqId(str(seq_num))
        atom = gemmi.Atom()
        atom.name = atom_name
        atom.element = gemmi.Element(element)
        atom.pos = gemmi.Position(*pos)
        residue.add_atom(atom)
        return residue

    chain.add_residue(_residue("ASP", 10, "CG", "C", (0.0, 0.0, 0.0)))
    chain.add_residue(_residue("LYS", 11, "NZ", "N", (4.0, 0.0, 0.0)))
    model.add_chain(chain)

    metal_chain = gemmi.Chain("Z")
    metal_chain.add_residue(_residue("ZN", 1, "ZN", "Zn", (0.8, 0.0, 0.0)))
    model.add_chain(metal_chain)

    structure.add_model(model)
    structure.make_mmcif_document().write_file(str(path))


def test_prediction_input_validates_supported_types() -> None:
    PredictionInputRecord(input_type="smiles", input_value="CCO")
    PredictionInputRecord(input_type="fasta", input_value=">seq\nMSTNPKPQR")
    PredictionInputRecord(input_type="pdb", input_value="HEADER TEST\nATOM      1  N   GLY A   1")


def test_prediction_input_rejects_invalid_mmcif_text() -> None:
    with pytest.raises(ValueError):
        PredictionInputRecord(input_type="mmcif", input_value="not a cif")


def test_prediction_input_rejects_invalid_smiles() -> None:
    with pytest.raises(ValueError):
        PredictionInputRecord(input_type="smiles", input_value="NOT_A_SMILES")


def test_prediction_input_rejects_missing_structure_path() -> None:
    with pytest.raises(ValueError):
        PredictionInputRecord(input_type="mmcif", input_value="does_not_exist.cif")


def test_ligand_screening_writes_manifest() -> None:
    tmp_root = _tmp_dir("prediction_ligand")
    layout = build_storage_layout(tmp_root)
    (layout.root / "master_pdb_pairs.csv").write_text(
        "pdb_id,pair_identity_key,receptor_uniprot_ids,reported_measurement_mean_log10_standardized,source_conflict_flag,source_agreement_band,source_database,selected_preferred_source,ligand_key\n"
        "1ABC,protein_ligand|1ABC|A|ATP|wt,P12345,0.7,false,high,PDBbind,PDBbind,ATP\n"
        "2DEF,protein_ligand|2DEF|A|GTP|wt,Q99999,2.1,true,low,PDBbind,PDBbind,GTP\n",
        encoding="utf-8",
    )
    (layout.extracted_dir / "bound_objects").mkdir(parents=True, exist_ok=True)
    (layout.extracted_dir / "bound_objects" / "1ABC.json").write_text(
        json.dumps([{"component_id": "ATP", "component_smiles": "CCO"}]),
        encoding="utf-8",
    )
    (layout.extracted_dir / "bound_objects" / "2DEF.json").write_text(
        json.dumps([{"component_id": "GTP", "component_smiles": "NNNN"}]),
        encoding="utf-8",
    )

    out_path, manifest = run_ligand_screening_workflow(layout, smiles="CCO")

    assert out_path.exists()
    assert manifest["workflow"] == "ligand_screening"
    assert manifest["normalized_input_type"] == "SMILES"
    assert manifest["status"] == "baseline_heuristic_predictions_generated"
    assert manifest["ranked_target_list"][0]["target_id"] == "P12345"
    assert manifest["predicted_kd"] is not None
    assert manifest["confidence_score"] is not None
    saved = json.loads(out_path.read_text(encoding="utf-8"))
    assert saved["candidate_target_count"] == 2
    assert saved["ranked_target_list"][0]["rank"] == 1


def test_ligand_screening_prefers_trained_memory_model() -> None:
    tmp_root = _tmp_dir("prediction_ligand_trained")
    layout = build_storage_layout(tmp_root)
    layout.training_dir.mkdir(parents=True, exist_ok=True)
    layout.splits_dir.mkdir(parents=True, exist_ok=True)
    (layout.training_dir / "training_examples.json").write_text(
        json.dumps([
            {
                "example_id": "train:1ABC:0",
                "structure": {"pdb_id": "1ABC"},
                "protein": {"uniprot_id": "P12345"},
                "ligand": {"ligand_id": "ATP", "smiles": "CCO"},
                "interaction": {},
                "experiment": {"affinity_type": "Kd", "reported_measurement_count": 2},
                "graph_features": {},
                "labels": {"binding_affinity_log10": 1.0, "affinity_type": "Kd"},
                "provenance": {
                    "pair_identity_key": "protein_ligand|1ABC|A|ATP|wt",
                    "source_database": "PDBbind",
                    "source_agreement_band": "high",
                },
            }
        ]),
        encoding="utf-8",
    )
    (layout.splits_dir / "train.txt").write_text("train:1ABC:0\n", encoding="utf-8")
    (layout.splits_dir / "val.txt").write_text("", encoding="utf-8")
    (layout.splits_dir / "test.txt").write_text("", encoding="utf-8")
    train_ligand_memory_model(layout)

    out_path, manifest = run_ligand_screening_workflow(layout, smiles="CCO")

    assert out_path.exists()
    assert manifest["status"] == "trained_supervised_predictions_generated"
    assert manifest["prediction_method"] == "trained_ligand_memory_model"
    assert manifest["ranked_target_list"][0]["target_id"] == "P12345"
    assert manifest["ranked_target_list"][0]["target_prior_score"] is not None
    assert manifest["model_artifact_path"] is not None


def test_ligand_screening_prefers_tabular_model_when_validation_winner() -> None:
    tmp_root = _tmp_dir("prediction_ligand_tabular")
    layout = build_storage_layout(tmp_root)
    layout.training_dir.mkdir(parents=True, exist_ok=True)
    layout.splits_dir.mkdir(parents=True, exist_ok=True)
    (layout.training_dir / "training_examples.json").write_text(
        json.dumps([
            {
                "example_id": "train:1ABC:0",
                "structure": {"pdb_id": "1ABC", "resolution": 2.0},
                "protein": {"uniprot_id": "P12345", "sequence_length": 220, "charged_fraction": 0.12},
                "ligand": {"ligand_id": "ATP", "smiles": "CCO", "molecular_weight": 507.0},
                "interaction": {"microstate_record_count": 2, "opposite_charge_contact_count": 3, "acidic_cluster_penalty": 0.2},
                "experiment": {"affinity_type": "Kd", "reported_measurement_count": 2},
                "graph_features": {"network_degree": 6, "pathway_count": 2},
                "labels": {"binding_affinity_log10": 0.8, "affinity_type": "Kd"},
                "provenance": {
                    "pair_identity_key": "protein_ligand|1ABC|A|ATP|wt",
                    "source_database": "PDBbind",
                    "source_agreement_band": "high",
                },
            },
            {
                "example_id": "train:2DEF:1",
                "structure": {"pdb_id": "2DEF", "resolution": 2.7},
                "protein": {"uniprot_id": "Q99999", "sequence_length": 240, "charged_fraction": 0.20},
                "ligand": {"ligand_id": "GTP", "smiles": "NNNN", "molecular_weight": 523.0},
                "interaction": {"microstate_record_count": 1, "opposite_charge_contact_count": 0, "acidic_cluster_penalty": 1.6},
                "experiment": {"affinity_type": "Kd", "reported_measurement_count": 1},
                "graph_features": {"network_degree": 2, "pathway_count": 0},
                "labels": {"binding_affinity_log10": 2.5, "affinity_type": "Kd"},
                "provenance": {
                    "pair_identity_key": "protein_ligand|2DEF|A|GTP|wt",
                    "source_database": "BindingDB",
                    "source_agreement_band": "low",
                },
            },
            {
                "example_id": "val:3GHI:2",
                "structure": {"pdb_id": "3GHI", "resolution": 2.1},
                "protein": {"uniprot_id": "P12345", "sequence_length": 221, "charged_fraction": 0.11},
                "ligand": {"ligand_id": "ATP", "smiles": "CCO", "molecular_weight": 507.0},
                "interaction": {"microstate_record_count": 3, "opposite_charge_contact_count": 4, "acidic_cluster_penalty": 0.1},
                "experiment": {"affinity_type": "Kd", "reported_measurement_count": 2},
                "graph_features": {"network_degree": 7, "pathway_count": 3},
                "labels": {"binding_affinity_log10": 0.9, "affinity_type": "Kd"},
                "provenance": {
                    "pair_identity_key": "protein_ligand|3GHI|A|ATP|wt",
                    "source_database": "PDBbind",
                    "source_agreement_band": "high",
                },
            },
        ]),
        encoding="utf-8",
    )
    (layout.splits_dir / "train.txt").write_text("train:1ABC:0\ntrain:2DEF:1\n", encoding="utf-8")
    (layout.splits_dir / "val.txt").write_text("val:3GHI:2\n", encoding="utf-8")
    (layout.splits_dir / "test.txt").write_text("", encoding="utf-8")
    train_ligand_memory_model(layout)
    train_tabular_affinity_model(layout)
    evaluate_tabular_affinity_model(layout)
    (layout.models_dir / "ligand_memory_evaluation.json").write_text(
        json.dumps({
            "status": "evaluated",
            "splits": {
                "val": {"affinity_mae_log10": 0.9, "top1_target_accuracy": 1.0},
                "test": {"affinity_mae_log10": None, "top1_target_accuracy": None},
            },
        }),
        encoding="utf-8",
    )
    (layout.models_dir / "tabular_affinity_evaluation.json").write_text(
        json.dumps({
            "status": "trained",
            "splits": {
                "val": {"affinity_mae_log10": 0.2, "affinity_rmse_log10": 0.25},
                "test": {"affinity_mae_log10": None, "affinity_rmse_log10": None},
            },
        }),
        encoding="utf-8",
    )

    out_path, manifest = run_ligand_screening_workflow(layout, smiles="CCO")

    assert out_path.exists()
    assert manifest["status"] == "trained_supervised_predictions_generated"
    assert manifest["prediction_method"] == "trained_tabular_affinity_model"
    assert manifest["selected_model_preference"] == "tabular_affinity"
    assert manifest["ranked_target_list"][0]["target_id"] == "P12345"
    assert manifest["ranked_target_list"][0]["predicted_affinity_log10"] is not None
    assert manifest["model_artifact_path"] is not None


def test_peptide_binding_writes_manifest() -> None:
    tmp_root = _tmp_dir("prediction_peptide")
    layout = build_storage_layout(tmp_root)
    structure_path = tmp_root / "example.cif"
    _write_ionizable_context_cif(structure_path)
    (layout.root / "master_pdb_pairs.csv").write_text(
        "pdb_id,pair_identity_key,receptor_uniprot_ids,source_database,selected_preferred_source,matching_interface_count\n"
        "EXAMPLE,protein_peptide|EXAMPLE|A|B|wt,P12345,BioLiP,BioLiP,2\n",
        encoding="utf-8",
    )
    (layout.extracted_dir / "interfaces").mkdir(parents=True, exist_ok=True)
    (layout.extracted_dir / "interfaces" / "EXAMPLE.json").write_text(
        json.dumps([{"interface_type": "protein_peptide", "binding_site_residue_ids": ["A:TYR15"]}]),
        encoding="utf-8",
    )

    out_path, manifest = run_peptide_binding_workflow(layout, structure_file=str(structure_path))

    assert out_path.exists()
    assert manifest["workflow"] == "peptide_binding"
    assert manifest["normalized_input_type"] == "mmCIF"
    assert manifest["status"] == "baseline_heuristic_predictions_generated"
    assert manifest["interface_summary"]["status"] == "observed_from_extracted_interfaces"
    assert manifest["predicted_targets"][0]["target_id"] == "P12345"
    assert manifest["binding_probability"] is not None
    assert manifest["query_numeric_feature_count"] > 0
    assert manifest["query_structure_context"]["microstate_feature_available"] is True
    assert manifest["predicted_targets"][0]["query_context_alignment"] is not None


def test_peptide_binding_uses_trained_target_profiles_for_context_alignment() -> None:
    tmp_root = _tmp_dir("prediction_peptide_trained")
    layout = build_storage_layout(tmp_root)
    structure_path = tmp_root / "example.cif"
    _write_ionizable_context_cif(structure_path)
    layout.training_dir.mkdir(parents=True, exist_ok=True)
    layout.splits_dir.mkdir(parents=True, exist_ok=True)
    (layout.training_dir / "training_examples.json").write_text(
        json.dumps([
            {
                "example_id": "train:EXAMPLE:0",
                "structure": {"pdb_id": "EXAMPLE"},
                "protein": {"uniprot_id": "P12345"},
                "ligand": {"ligand_id": "PEP", "smiles": "CCO"},
                "interaction": {
                    "microstate_record_count": 2,
                    "estimated_net_charge": -0.3,
                    "mean_abs_residue_charge": 0.6,
                    "positive_residue_count": 1,
                    "negative_residue_count": 1,
                    "same_charge_contact_count": 0,
                    "opposite_charge_contact_count": 2,
                    "metal_contact_count": 1,
                    "acidic_cluster_penalty": 0.2,
                    "local_electrostatic_balance": 1.8,
                },
                "experiment": {"affinity_type": "Kd", "reported_measurement_count": 2},
                "graph_features": {"network_degree": 6, "pathway_count": 2},
                "labels": {"binding_affinity_log10": 1.0, "affinity_type": "Kd"},
                "provenance": {
                    "pair_identity_key": "protein_peptide|EXAMPLE|A|B|wt",
                    "source_database": "BioLiP",
                    "source_agreement_band": "high",
                },
            }
        ]),
        encoding="utf-8",
    )
    (layout.splits_dir / "train.txt").write_text("train:EXAMPLE:0\n", encoding="utf-8")
    (layout.splits_dir / "val.txt").write_text("", encoding="utf-8")
    (layout.splits_dir / "test.txt").write_text("", encoding="utf-8")
    train_ligand_memory_model(layout)
    (layout.root / "master_pdb_pairs.csv").write_text(
        "pdb_id,pair_identity_key,receptor_uniprot_ids,source_database,selected_preferred_source,matching_interface_count\n"
        "EXAMPLE,protein_peptide|EXAMPLE|A|B|wt,P12345,BioLiP,BioLiP,2\n",
        encoding="utf-8",
    )
    (layout.extracted_dir / "interfaces").mkdir(parents=True, exist_ok=True)
    (layout.extracted_dir / "interfaces" / "EXAMPLE.json").write_text(
        json.dumps([{"interface_type": "protein_peptide", "binding_site_residue_ids": ["A:ASP10", "A:LYS11"]}]),
        encoding="utf-8",
    )

    _, manifest = run_peptide_binding_workflow(layout, structure_file=str(structure_path))

    assert manifest["prediction_method"] == "baseline_interface_context_lookup_with_microstate_alignment"
    assert manifest["predicted_targets"][0]["target_id"] == "P12345"
    assert manifest["predicted_targets"][0]["query_context_alignment"] >= 0.0


def test_ligand_screening_cli_shows_clean_error_for_missing_input() -> None:
    tmp_root = _tmp_dir("prediction_cli_missing")
    runner = CliRunner()

    result = runner.invoke(
        app,
        ["--storage-root", str(tmp_root), "predict-ligand-screening"],
        catch_exceptions=False,
    )

    assert result.exit_code == 1
    assert "Error:" in result.output
    assert "One input is required" in result.output


def test_ligand_screening_cli_shows_clean_error_for_invalid_smiles() -> None:
    tmp_root = _tmp_dir("prediction_cli_bad_smiles")
    runner = CliRunner()

    result = runner.invoke(
        app,
        ["--storage-root", str(tmp_root), "predict-ligand-screening", "--smiles", "NOT_A_SMILES"],
        catch_exceptions=False,
    )

    assert result.exit_code == 1
    assert "SMILES input" in result.output


def test_peptide_binding_cli_rejects_missing_structure_path() -> None:
    tmp_root = _tmp_dir("prediction_cli_missing_structure")
    runner = CliRunner()

    result = runner.invoke(
        app,
        ["--storage-root", str(tmp_root), "predict-peptide-binding", "--structure-file", "does_not_exist.cif"],
        catch_exceptions=False,
    )

    assert result.exit_code == 1
    assert "structure file not found" in result.output.lower()
