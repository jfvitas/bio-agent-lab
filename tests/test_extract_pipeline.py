"""Tests for the multi-table extraction pipeline."""

import json
import os
from pathlib import Path
from uuid import uuid4
from unittest.mock import patch
from unittest.mock import Mock

import gemmi
from typer.testing import CliRunner

from pbdata.cli import app
from pbdata.gui import _SUBPROCESS_STAGES
from pbdata.gui import PbdataGUI
from pbdata.gui import build_filtered_review_rows
from pbdata.gui import build_review_health_summary
from pbdata.gui import build_training_set_kpis
from pbdata.gui import build_training_set_builder_summary
from pbdata.gui import build_training_set_workflow_status
from pbdata.gui import build_curation_review_summary
from pbdata.pipeline.assay_merge import pair_identity_key
from pbdata.pipeline.extract import extract_rcsb_entry
from pbdata.schemas.canonical_sample import CanonicalBindingSample
from pbdata.schemas.records import (
    AssayRecord,
    BoundObjectRecord,
    ChainRecord,
    EntryRecord,
    InterfaceRecord,
    ProvenanceRecord,
)

_LOCAL_TMP = Path(__file__).parent / "_tmp"
_LOCAL_TMP.mkdir(exist_ok=True)


def _tmp_dir(name: str) -> Path:
    path = _LOCAL_TMP / f"{uuid4().hex}_{name}"
    path.mkdir(exist_ok=True)
    return path


def _write_minimal_microstate_cif(path: Path) -> None:
    structure = gemmi.Structure()
    structure.name = "1ABC"
    model = gemmi.Model("1")

    chain = gemmi.Chain("A")
    for seq_num, residue_name, atom_name, element, pos in [
        (10, "ASP", "CG", "C", (0.0, 0.0, 0.0)),
        (11, "GLU", "CD", "C", (3.1, 0.0, 0.0)),
        (12, "LYS", "NZ", "N", (5.0, 0.0, 0.0)),
    ]:
        residue = gemmi.Residue()
        residue.name = residue_name
        residue.seqid = gemmi.SeqId(str(seq_num))
        atom = gemmi.Atom()
        atom.name = atom_name
        atom.element = gemmi.Element(element)
        atom.pos = gemmi.Position(*pos)
        residue.add_atom(atom)
        chain.add_residue(residue)
    model.add_chain(chain)

    metal_chain = gemmi.Chain("Z")
    residue = gemmi.Residue()
    residue.name = "ZN"
    residue.seqid = gemmi.SeqId("1")
    atom = gemmi.Atom()
    atom.name = "ZN"
    atom.element = gemmi.Element("Zn")
    atom.pos = gemmi.Position(0.8, 0.0, 0.0)
    residue.add_atom(atom)
    metal_chain.add_residue(residue)
    model.add_chain(metal_chain)

    structure.add_model(model)
    structure.make_mmcif_document().write_file(str(path))


def _mock_entry(pdb_id="1ABC"):
    """Minimal RCSB GraphQL entry dict for testing."""
    return {
        "rcsb_id": pdb_id,
        "struct": {"title": "Test structure"},
        "struct_keywords": {"pdbx_keywords": "HYDROLASE", "text": "hydrolase"},
        "exptl": [{"method": "X-RAY DIFFRACTION"}],
        "rcsb_entry_info": {
            "resolution_combined": [2.0],
            "polymer_entity_count_protein": 1,
            "nonpolymer_entity_count": 1,
            "deposited_atom_count": 5000,
            "assembly_count": 1,
        },
        "rcsb_accession_info": {
            "initial_release_date": "2020-01-15",
            "deposit_date": "2019-06-01",
        },
        "polymer_entities": [
            {
                "rcsb_id": f"{pdb_id}_1",
                "entity_poly": {
                    "pdbx_seq_one_letter_code_can": "MKTVRQERLKSIVRILERSKEPVSGAQ" * 4,
                    "type": "polypeptide(L)",
                },
                "rcsb_polymer_entity": {"pdbx_description": "Test protein"},
                "rcsb_polymer_entity_container_identifiers": {
                    "auth_asym_ids": ["A"],
                    "uniprot_ids": ["P12345"],
                },
                "rcsb_entity_source_organism": [
                    {"ncbi_taxonomy_id": 9606, "ncbi_scientific_name": "Homo sapiens"},
                ],
            },
        ],
        "nonpolymer_entities": [
            {
                "rcsb_id": f"{pdb_id}_2",
                "nonpolymer_comp": {
                    "chem_comp": {"id": "ATP", "name": "ADENOSINE-5'-TRIPHOSPHATE"},
                },
                "rcsb_nonpolymer_entity_container_identifiers": {
                    "auth_asym_ids": ["B"],
                },
            },
        ],
        "assemblies": [
            {
                "rcsb_id": f"{pdb_id}-1",
                "pdbx_struct_assembly": {
                    "oligomeric_details": "MONOMERIC",
                    "oligomeric_count": "1",
                },
                "rcsb_assembly_info": {
                    "polymer_entity_count": 1,
                    "polymer_entity_count_protein": 1,
                },
            },
        ],
    }


@patch("pbdata.pipeline.extract.download_structure_files")
def test_extract_produces_all_tables(mock_download):
    """extract_rcsb_entry returns all six table types."""
    mock_download.return_value = {
        "structure_file_cif_path": "data/structures/rcsb/1ABC.cif",
        "structure_file_cif_size_bytes": 123456,
        "structure_file_hash_sha256": "abc123",
        "structure_download_url": "https://files.rcsb.org/download/1ABC.cif",
        "structure_downloaded_at": "2024-01-01T00:00:00+00:00",
        "parsed_structure_format": "mmCIF",
    }

    result = extract_rcsb_entry(_mock_entry("1ABC"))

    assert isinstance(result["entry"], EntryRecord)
    assert isinstance(result["chains"], list)
    assert isinstance(result["bound_objects"], list)
    assert isinstance(result["interfaces"], list)
    assert isinstance(result["assays"], list)
    assert isinstance(result["provenance"], list)


@patch("pbdata.pipeline.extract.download_structure_files")
def test_entry_record_fields(mock_download):
    """EntryRecord has correct entry-level fields populated."""
    mock_download.return_value = {
        "parsed_structure_format": "mmCIF",
    }

    result = extract_rcsb_entry(_mock_entry("4HHB"))
    entry = result["entry"]

    assert entry.pdb_id == "4HHB"
    assert entry.source_database == "RCSB"
    assert entry.title == "Test structure"
    assert entry.experimental_method == "X-RAY DIFFRACTION"
    assert entry.structure_resolution == 2.0
    assert entry.release_date == "2020-01-15"
    assert entry.deposit_date == "2019-06-01"
    assert entry.deposited_atom_count == 5000
    assert entry.parsed_structure_format == "mmCIF"
    assert entry.resolution_bin == "medium_res_1.5-2.5"
    assert entry.membrane_vs_soluble == "soluble"
    assert entry.field_provenance is not None
    assert entry.field_confidence is not None
    assert entry.field_provenance["taxonomy_ids"]["method"] == "GraphQL_batch_API_entity_organism"
    assert entry.field_confidence["metal_present"] == "medium"


@patch("pbdata.pipeline.extract.download_structure_files")
def test_chain_records(mock_download):
    """ChainRecords correctly map polymer entities to chains."""
    mock_download.return_value = {}

    result = extract_rcsb_entry(_mock_entry("1ABC"))
    chains = result["chains"]

    assert len(chains) >= 1
    chain = chains[0]
    assert isinstance(chain, ChainRecord)
    assert chain.pdb_id == "1ABC"
    assert chain.chain_id == "A"
    assert chain.is_protein is True
    assert chain.polymer_subtype == "protein"
    assert chain.uniprot_id == "P12345"
    assert chain.entity_source_organism == "Homo sapiens"
    assert chain.entity_source_taxonomy_id == 9606


@patch("pbdata.pipeline.extract.download_structure_files")
def test_bound_object_records(mock_download):
    """BoundObjectRecords have correct component type/role mapping."""
    mock_download.return_value = {}

    result = extract_rcsb_entry(_mock_entry("1ABC"))
    bos = result["bound_objects"]

    assert len(bos) >= 1
    atp = next((b for b in bos if b.component_id == "ATP"), None)
    assert atp is not None
    assert isinstance(atp, BoundObjectRecord)
    assert atp.component_type == "cofactor"


@patch("pbdata.pipeline.extract.download_structure_files")
def test_provenance_records(mock_download):
    """Provenance records are created for key extraction steps."""
    mock_download.return_value = {
        "structure_file_cif_path": "test.cif",
        "structure_download_url": "https://example.com/test.cif",
        "structure_downloaded_at": "2024-01-01",
    }

    result = extract_rcsb_entry(_mock_entry("1ABC"))
    provs = result["provenance"]

    assert len(provs) >= 2
    assert all(isinstance(p, ProvenanceRecord) for p in provs)
    sources = [p.source_name for p in provs]
    assert "RCSB" in sources
    assert "pbdata_rcsb_classify" in sources


@patch("pbdata.pipeline.extract.download_structure_files")
def test_file_provenance_on_entry(mock_download):
    """Entry record carries structure file provenance fields."""
    mock_download.return_value = {
        "structure_file_cif_path": "data/structures/rcsb/2XYZ.cif",
        "structure_file_cif_size_bytes": 999,
        "structure_file_hash_sha256": "deadbeef",
        "structure_download_url": "https://files.rcsb.org/download/2XYZ.cif",
        "structure_downloaded_at": "2024-06-01T12:00:00+00:00",
        "parsed_structure_format": "mmCIF",
    }

    result = extract_rcsb_entry(_mock_entry("2XYZ"))
    entry = result["entry"]

    assert entry.structure_file_cif_path == "data/structures/rcsb/2XYZ.cif"
    assert entry.structure_file_cif_size_bytes == 999
    assert entry.structure_file_hash_sha256 == "deadbeef"
    assert entry.parsed_structure_format == "mmCIF"


@patch("pbdata.pipeline.extract.download_structure_files")
def test_quality_flags_populated(mock_download):
    """Quality flags are populated based on bound object types."""
    mock_download.return_value = {}

    result = extract_rcsb_entry(_mock_entry("1ABC"))
    entry = result["entry"]

    # ATP is a cofactor
    assert entry.cofactor_present is True
    assert "cofactor_present" in (entry.quality_flags or [])


@patch("pbdata.pipeline.extract.download_structure_files")
def test_extract_can_skip_structure_downloads(mock_download):
    """extract_rcsb_entry should not fetch structure files when disabled."""
    result = extract_rcsb_entry(_mock_entry("1ABC"), download_structures=False)

    mock_download.assert_not_called()
    entry = result["entry"]
    assert entry.structure_file_cif_path is None
    assert entry.parsed_structure_format is None


@patch("pbdata.pipeline.extract.download_structure_files")
def test_extract_passes_structure_mirror_to_download(mock_download):
    mock_download.return_value = {"parsed_structure_format": "mmCIF"}

    extract_rcsb_entry(_mock_entry("1ABC"), structure_mirror="pdbj")

    assert mock_download.call_args.kwargs["mirror"] == "pdbj"


def test_gui_pipeline_includes_extract_stage() -> None:
    assert _SUBPROCESS_STAGES[0] == "extract"
    assert "extract" in _SUBPROCESS_STAGES
    assert "build-graph" in _SUBPROCESS_STAGES
    assert "build-microstates" in _SUBPROCESS_STAGES
    assert "build-physics-features" in _SUBPROCESS_STAGES
    assert "build-microstate-refinement" in _SUBPROCESS_STAGES
    assert "build-mm-job-manifests" in _SUBPROCESS_STAGES
    assert "run-mm-jobs" in _SUBPROCESS_STAGES
    assert "run-feature-pipeline" in _SUBPROCESS_STAGES
    assert "export-analysis-queue" in _SUBPROCESS_STAGES
    assert "build-features" in _SUBPROCESS_STAGES
    assert "build-training-examples" in _SUBPROCESS_STAGES
    assert "build-splits" in _SUBPROCESS_STAGES
    assert "build-custom-training-set" in _SUBPROCESS_STAGES
    assert "build-release" in _SUBPROCESS_STAGES


def test_gui_stage_command_includes_storage_root_and_workers() -> None:
    class _Var:
        def __init__(self, value):
            self._value = value

        def get(self):
            return self._value

        def set(self, value):
            self._value = value

    gui = PbdataGUI.__new__(PbdataGUI)
    gui._storage_root_var = _Var(r"C:\tmp\pbdata-root")
    gui._workers_var = _Var("4")
    gui._pipeline_execution_mode_var = _Var("hybrid")
    gui._site_pipeline_degraded_mode_var = _Var(True)
    gui._site_pipeline_run_id_var = _Var("site-run-001")
    gui._site_physics_batch_id_var = _Var("batch-001")
    gui._structural_graph_level_var = _Var("residue")
    gui._structural_graph_scope_var = _Var("whole_protein")
    gui._structural_graph_exports_var = _Var("pyg,networkx")
    gui._split_mode_var = _Var("pair-aware")
    gui._download_structures_var = _Var(True)
    gui._download_pdb_var = _Var(True)
    gui._train_frac_var = _Var("0.70")
    gui._val_frac_var = _Var("0.15")
    gui._split_seed_var = _Var("42")
    gui._hash_only_var = _Var(False)
    gui._jaccard_threshold_var = _Var("0.30")
    gui._release_tag_var = _Var("release-20260308")
    gui._custom_set_mode_var = _Var("generalist")
    gui._custom_set_target_size_var = _Var("250")
    gui._custom_set_seed_var = _Var("9")
    gui._custom_set_cluster_cap_var = _Var("2")
    gui._engineered_dataset_name_var = _Var("bench")
    gui._engineered_dataset_test_frac_var = _Var("0.25")
    gui._engineered_dataset_cv_folds_var = _Var("3")
    gui._engineered_dataset_cluster_count_var = _Var("7")
    gui._engineered_dataset_embedding_backend_var = _Var("auto")
    gui._engineered_dataset_strict_family_var = _Var(True)
    cmd = gui._build_stage_cmd("extract")

    assert "--storage-root" in cmd
    assert r"C:\tmp\pbdata-root" in cmd
    assert "--workers" in cmd
    assert "4" in cmd
    assert "--download-pdb" in cmd

    microstate_cmd = gui._build_stage_cmd("build-microstates")
    assert "--storage-root" in microstate_cmd
    assert r"C:\tmp\pbdata-root" in microstate_cmd
    physics_cmd = gui._build_stage_cmd("build-physics-features")
    assert "--storage-root" in physics_cmd
    assert r"C:\tmp\pbdata-root" in physics_cmd
    refinement_cmd = gui._build_stage_cmd("build-microstate-refinement")
    assert "--storage-root" in refinement_cmd
    assert r"C:\tmp\pbdata-root" in refinement_cmd
    mm_cmd = gui._build_stage_cmd("build-mm-job-manifests")
    assert "--storage-root" in mm_cmd
    assert r"C:\tmp\pbdata-root" in mm_cmd
    run_mm_cmd = gui._build_stage_cmd("run-mm-jobs")
    assert "--storage-root" in run_mm_cmd
    assert r"C:\tmp\pbdata-root" in run_mm_cmd
    feature_cmd = gui._build_stage_cmd("run-feature-pipeline")
    assert "--run-mode" in feature_cmd
    assert "full_build" in feature_cmd
    assert "--degraded-mode" in feature_cmd
    assert "--run-id" in feature_cmd
    assert "site-run-001" in feature_cmd
    queue_cmd = gui._build_stage_cmd("export-analysis-queue")
    assert "--run-id" in queue_cmd
    assert "site-run-001" in queue_cmd
    structural_graph_cmd = gui._build_stage_cmd("build-structural-graphs")
    assert "--graph-level" in structural_graph_cmd
    assert "residue" in structural_graph_cmd
    assert "--scope" in structural_graph_cmd
    assert "whole_protein" in structural_graph_cmd
    assert structural_graph_cmd.count("--export-format") == 2
    split_cmd = gui._build_stage_cmd("build-splits")
    assert "--split-mode" in split_cmd
    assert "pair-aware" in split_cmd
    release_cmd = gui._build_stage_cmd("build-release")
    assert "--tag" in release_cmd
    assert "release-20260308" in release_cmd
    custom_cmd = gui._build_stage_cmd("build-custom-training-set")
    assert "--mode" in custom_cmd
    assert "generalist" in custom_cmd
    assert "--target-size" in custom_cmd
    assert "250" in custom_cmd
    assert "--per-receptor-cluster-cap" in custom_cmd
    assert "2" in custom_cmd
    dataset_cmd = gui._build_stage_cmd("engineer-dataset")
    assert "--dataset-name" in dataset_cmd
    assert "bench" in dataset_cmd
    assert "--strict-family-isolation" in dataset_cmd


def test_cli_build_microstates_and_physics_features() -> None:
    runner = CliRunner()
    storage_root = _tmp_dir("cli_microstates")
    extracted = storage_root / "data" / "extracted"
    for name in ["entry", "assays"]:
        (extracted / name).mkdir(parents=True, exist_ok=True)

    cif_path = storage_root / "data" / "structures" / "rcsb" / "1ABC.cif"
    cif_path.parent.mkdir(parents=True, exist_ok=True)
    _write_minimal_microstate_cif(cif_path)

    (extracted / "entry" / "1ABC.json").write_text(json.dumps({
        "pdb_id": "1ABC",
        "structure_file_cif_path": str(cif_path),
    }), encoding="utf-8")
    (extracted / "assays" / "1ABC.json").write_text(json.dumps([{
        "pdb_id": "1ABC",
        "pair_identity_key": "protein_ligand|1ABC|A|ATP|wt",
        "binding_affinity_type": "Kd",
    }]), encoding="utf-8")

    result = runner.invoke(app, ["--storage-root", str(storage_root), "build-microstates"])
    assert result.exit_code == 0
    assert (storage_root / "data" / "features" / "microstates" / "microstate_records.json").exists()

    result = runner.invoke(app, ["--storage-root", str(storage_root), "build-physics-features"])
    assert result.exit_code == 0
    assert (storage_root / "data" / "features" / "physics" / "physics_feature_records.json").exists()

    result = runner.invoke(app, ["--storage-root", str(storage_root), "build-microstate-refinement"])
    assert result.exit_code == 0
    assert (storage_root / "data" / "features" / "microstate_refinement" / "microstate_refinement_records.json").exists()

    result = runner.invoke(app, ["--storage-root", str(storage_root), "build-mm-job-manifests"])
    assert result.exit_code == 0
    assert (storage_root / "data" / "features" / "mm_jobs" / "mm_job_records.json").exists()


def test_gui_criteria_from_ui_includes_branched_and_assembly_filters() -> None:
    class _Var:
        def __init__(self, value):
            self._value = value

        def get(self):
            return self._value

        def set(self, value):
            self._value = value

    gui = PbdataGUI.__new__(PbdataGUI)
    gui._method_vars = {"xray": _Var(True), "em": _Var(False), "nmr": _Var(False), "neutron": _Var(False)}
    gui._task_vars = {
        "protein_ligand": _Var(True),
        "protein_protein": _Var(False),
        "mutation_ddg": _Var(False),
    }
    gui._pdb_ids_var = _Var("1abc, 2def")
    gui._keyword_query_var = _Var("")
    gui._organism_name_var = _Var("")
    gui._taxonomy_id_var = _Var("")
    gui._resolution_var = _Var("3.0 Å")
    gui._membrane_only_var = _Var(False)
    gui._require_multimer_var = _Var(False)
    gui._require_protein_var = _Var(True)
    gui._require_ligand_var = _Var(True)
    gui._require_branched_entities_var = _Var(True)
    gui._min_protein_entities_var = _Var("1")
    gui._min_nonpolymer_entities_var = _Var("")
    gui._max_nonpolymer_entities_var = _Var("")
    gui._min_branched_entities_var = _Var("1")
    gui._max_branched_entities_var = _Var("3")
    gui._min_assembly_count_var = _Var("1")
    gui._max_assembly_count_var = _Var("4")
    gui._max_atom_count_var = _Var("")
    gui._min_year_var = _Var("")
    gui._max_year_var = _Var("")

    criteria = gui._criteria_from_ui()

    assert criteria.direct_pdb_ids == ["1ABC", "2DEF"]
    assert criteria.require_branched_entities is True
    assert criteria.min_branched_entities == 1
    assert criteria.max_branched_entities == 3
    assert criteria.min_assembly_count == 1
    assert criteria.max_assembly_count == 4


def test_gui_refresh_review_exports_logs_conflict_csv() -> None:
    class _Var:
        def __init__(self, value):
            self._value = value

        def get(self):
            return self._value

        def set(self, value):
            self._value = value

    gui = PbdataGUI.__new__(PbdataGUI)
    gui._storage_root_var = _Var(r"C:\tmp\pbdata-root")
    gui._log_line = Mock()
    gui._refresh_overview = Mock()
    gui._storage_layout = Mock(return_value=Mock())

    with patch("pbdata.master_export.refresh_master_exports", return_value={
        "master_csv": "C:/repo/master_pdb_repository.csv",
        "pair_csv": "C:/repo/master_pdb_pairs.csv",
        "issue_csv": "C:/repo/master_pdb_issues.csv",
        "conflict_csv": "C:/repo/master_pdb_conflicts.csv",
        "source_state_csv": "C:/repo/master_source_state.csv",
        "model_ready_pairs_csv": "C:/repo/model_ready_pairs.csv",
        "custom_training_scorecard_json": "C:/repo/custom_training_scorecard.json",
        "custom_training_split_benchmark_csv": "C:/repo/custom_training_split_benchmark.csv",
        "release_manifest_json": "C:/repo/dataset_release_manifest.json",
        "split_summary_csv": "C:/repo/split_summary.csv",
        "scientific_coverage_json": "C:/repo/scientific_coverage_summary.json",
    }):
        gui._refresh_review_exports()

    logged = " ".join(call.args[0] for call in gui._log_line.call_args_list)
    assert "master_pdb_conflicts.csv" in logged
    assert "master_source_state.csv" in logged
    assert "model_ready_pairs.csv" in logged
    assert "custom_training_scorecard.json" in logged
    assert "custom_training_split_benchmark.csv" in logged
    assert "dataset_release_manifest.json" in logged
    assert "scientific_coverage_summary.json" in logged
    gui._refresh_overview.assert_called_once()


def test_build_training_set_builder_summary_reports_benchmark_pressure() -> None:
    summary = build_training_set_builder_summary(
        {
            "selected_count": 120,
            "candidate_pool_count": 400,
            "diversity": {
                "selected_receptor_clusters": 45,
                "selected_pair_families": 110,
            },
            "quality": {
                "mean_quality_score": 0.84,
            },
            "exclusions": {
                "count": 280,
            },
        },
        [
            {"benchmark_mode": "receptor_cluster", "largest_group_fraction": "0.20"},
            {"benchmark_mode": "pair_family", "largest_group_fraction": "0.42"},
        ],
    )

    assert summary["status"] == "Needs tuning"
    assert "120 selected from 400" in summary["coverage"]
    assert "pair_family=42.00%" in summary["quality"]


def test_build_training_set_kpis_and_workflow_status() -> None:
    kpis = build_training_set_kpis(
        {
            "selected_count": 120,
            "diversity": {"selected_receptor_clusters": 45},
            "quality": {"mean_quality_score": 0.84},
            "exclusions": {"count": 280},
        },
        [
            {"benchmark_mode": "pair_family", "largest_group_fraction": "0.42"},
        ],
    )
    assert kpis["selected"] == "120"
    assert kpis["clusters"] == "45"
    assert kpis["quality"] == "0.840"
    assert kpis["dominance"] == "42.0%"
    assert kpis["excluded"] == "280"

    status = build_training_set_workflow_status({
        "model_ready_pairs_csv": "C:/repo/model_ready_pairs.csv",
        "custom_training_set_csv": "",
        "custom_training_scorecard_json": "",
        "custom_training_split_benchmark_csv": "",
        "release_manifest_json": "",
    })
    assert status[0] == ("Model-ready pool", "missing")
    assert status[1] == ("Custom set", "pending")


def test_build_curation_review_summary_highlights_top_reasons() -> None:
    summary = build_curation_review_summary(
        [
            {"reason": "receptor_cluster_cap_reached"},
            {"reason": "receptor_cluster_cap_reached"},
            {"reason": "redundant_pair_family"},
        ],
        [
            {"source_agreement_band": "low"},
            {"source_agreement_band": "medium"},
        ],
        [
            {"issue_type": "missing_structure_file"},
            {"issue_type": "missing_structure_file"},
            {"issue_type": "ambiguous_mutation_context"},
        ],
    )
    assert "receptor_cluster_cap_reached=2" in summary["exclusions"]
    assert "low=1" in summary["conflicts"]
    assert "missing_structure_file=2" in summary["issues"]


def test_training_set_workflow_runs_expected_stages() -> None:
    gui = PbdataGUI.__new__(PbdataGUI)
    root = Mock()
    root.after = Mock(side_effect=lambda _delay, fn, *args: fn(*args))
    gui._root = root
    gui._running = Mock()
    gui._run_stage = Mock(side_effect=["done", "done", "done"])
    gui._refresh_overview = Mock()
    gui._log_line = Mock()

    gui._run_training_set_workflow_thread()

    assert gui._run_stage.call_args_list[0].args[0] == "build-splits"
    assert gui._run_stage.call_args_list[1].args[0] == "build-custom-training-set"
    assert gui._run_stage.call_args_list[2].args[0] == "build-release"
    gui._running.release.assert_called_once()


def test_build_filtered_review_rows_applies_conflict_and_flag_filters() -> None:
    master_rows = [{
        "pdb_id": "1ABC",
        "title": "Example",
        "metal_present": "true",
        "cofactor_present": "false",
        "glycan_present": "true",
        "membrane_vs_soluble": "membrane",
        "quality_flags": "glycan_present",
        "field_confidence_json": "{\"membrane_vs_soluble\": \"medium\"}",
    }]
    pair_rows = [{
        "pdb_id": "1ABC",
        "pair_identity_key": "protein_ligand|1ABC|A|ATP|mutation_unknown:1",
        "source_conflict_flag": "true",
        "source_conflict_summary": "high_conflict_spread=1.200",
        "source_agreement_band": "low",
        "selected_preferred_source": "PDBbind",
        "binding_affinity_type": "Kd",
        "assay_field_confidence_json": "{\"binding_affinity_value\": \"low\"}",
    }]
    issue_rows = [{
        "pdb_id": "1ABC",
        "pair_identity_key": "protein_ligand|1ABC|A|ATP|mutation_unknown:1",
        "issue_type": "ambiguous_mutation_context",
        "details": "mutation_unknown",
    }]

    rows = build_filtered_review_rows(
        master_rows,
        pair_rows,
        issue_rows,
        conflict_only=True,
        mutation_ambiguous_only=True,
        metal_only=True,
        glycan_only=True,
        confidence_filter="Low",
    )

    assert len(rows) == 1
    row = rows[0]
    assert row["pdb_id"] == "1ABC"
    assert row["source_conflict_flag"] == "true"
    assert "ambiguous_mutation_context" in row["issue_types"]


def test_build_filtered_review_rows_supports_entry_only_non_high_filter() -> None:
    master_rows = [{
        "pdb_id": "2DEF",
        "title": "Entry only",
        "metal_present": "false",
        "cofactor_present": "true",
        "glycan_present": "false",
        "membrane_vs_soluble": "soluble",
        "quality_flags": "",
        "field_confidence_json": "{\"oligomeric_state\": \"medium\"}",
    }]

    rows = build_filtered_review_rows(
        master_rows,
        [],
        [{
            "pdb_id": "2DEF",
            "pair_identity_key": "",
            "issue_type": "non_high_confidence_fields",
            "details": "oligomeric_state",
        }],
        confidence_filter="Non-high",
        cofactor_only=True,
    )

    assert len(rows) == 1
    assert rows[0]["scope"] == "entry"
    assert rows[0]["pdb_id"] == "2DEF"


def test_build_review_health_summary_prioritizes_conflicts() -> None:
    summary = build_review_health_summary({
        "counts": {
            "entry_count": 12,
            "pair_count": 20,
            "model_ready_pair_count": 8,
            "pairs_with_source_conflicts": 3,
            "entries_with_structure_file": 11,
        },
        "coverage": {
            "issue_types": {
                "missing_structure_file": 1,
                "non_high_confidence_fields": 2,
                "non_high_confidence_assay_fields": 4,
            },
        },
        "release": {
            "model_ready_exclusion_count": 5,
        },
    })

    assert summary["readiness"] == "Partially ready"
    assert "3 conflicted pairs" in summary["quality"]
    assert "master_pdb_conflicts.csv" in summary["next_action"]


def test_cli_extract_no_download_structures_passes_flag() -> None:
    tmp_root = _tmp_dir("extract_cli")
    raw_dir = tmp_root / "data" / "raw" / "rcsb"
    raw_dir.mkdir(parents=True)
    (raw_dir / "1ABC.json").write_text('{"rcsb_id":"1ABC","nonpolymer_entities":[]}', encoding="utf-8")

    captured: list[bool] = []

    def _fake_extract_rcsb_entry(*_args, **kwargs):
        captured.append(kwargs["download_structures"])
        return {
            "entry": EntryRecord(source_database="RCSB", source_record_id="1ABC", pdb_id="1ABC"),
            "chains": [],
            "bound_objects": [],
            "interfaces": [],
            "assays": [],
            "provenance": [],
        }

    runner = CliRunner()
    original_cwd = Path.cwd()
    os.chdir(tmp_root)
    try:
        with patch(
            "pbdata.cli._load_external_assay_samples", return_value={}
        ), patch(
            "pbdata.pipeline.extract.extract_rcsb_entry", side_effect=_fake_extract_rcsb_entry
        ), patch("pbdata.pipeline.extract.write_records_json", return_value=None):
            result = runner.invoke(app, ["extract", "--no-download-structures"], catch_exceptions=False)
    finally:
        os.chdir(original_cwd)

    assert result.exit_code == 0
    assert captured == [False]


def test_cli_extract_respects_storage_root() -> None:
    tmp_root = _tmp_dir("extract_storage_root")
    storage_root = tmp_root / "custom-root"
    raw_dir = storage_root / "data" / "raw" / "rcsb"
    raw_dir.mkdir(parents=True)
    (raw_dir / "1ABC.json").write_text('{"rcsb_id":"1ABC","nonpolymer_entities":[]}', encoding="utf-8")

    written_outputs: list[Path] = []

    def _fake_extract_rcsb_entry(*_args, **_kwargs):
        return {
            "entry": EntryRecord(source_database="RCSB", source_record_id="1ABC", pdb_id="1ABC"),
            "chains": [],
            "bound_objects": [],
            "interfaces": [],
            "assays": [],
            "provenance": [],
        }

    def _fake_write_records_json(_records, output_dir):
        written_outputs.append(output_dir)

    runner = CliRunner()
    original_cwd = Path.cwd()
    os.chdir(tmp_root)
    try:
        with patch(
            "pbdata.cli._load_external_assay_samples", return_value={}
        ), patch(
            "pbdata.pipeline.extract.extract_rcsb_entry", side_effect=_fake_extract_rcsb_entry
        ), patch(
            "pbdata.pipeline.extract.write_records_json", side_effect=_fake_write_records_json
        ):
            result = runner.invoke(
                app,
                ["--storage-root", str(storage_root), "extract", "--no-download-structures"],
                catch_exceptions=False,
            )
    finally:
        os.chdir(original_cwd)

    assert result.exit_code == 0
    assert written_outputs == [storage_root / "data" / "extracted"]


def test_gui_close_terminates_active_processes() -> None:
    proc = Mock()
    proc.poll.side_effect = [None, 0, 0]
    root = Mock()

    gui = PbdataGUI.__new__(PbdataGUI)
    gui._root = root
    gui._active_processes = {proc}
    gui._closing = False

    gui._on_close()

    proc.terminate.assert_called_once()
    root.destroy.assert_called_once()


@patch("pbdata.pipeline.extract.download_structure_files")
def test_extract_merges_external_assay_samples(mock_download) -> None:
    mock_download.return_value = {}
    sample = CanonicalBindingSample(
        sample_id="PDBBIND_1ABC_Kd",
        task_type="protein_ligand",
        source_database="PDBbind",
        source_record_id="1ABC",
        pdb_id="1ABC",
        chain_ids_receptor=["A"],
        ligand_id="ATP",
        assay_type="Kd",
        assay_value=5.0,
        assay_unit="nM",
        assay_value_standardized=5.0,
        assay_value_log10=0.69897,
        provenance={"ingested_at": "2026-03-08T00:00:00+00:00"},
        quality_flags=[],
        quality_score=0.0,
    )

    result = extract_rcsb_entry(_mock_entry("1ABC"), assay_samples=[sample])
    assays = result["assays"]

    assert len(assays) == 1
    assay = assays[0]
    assert isinstance(assay, AssayRecord)
    assert assay.pair_identity_key == pair_identity_key(sample)
    assert assay.binding_affinity_type == "Kd"
    assert assay.reported_measurement_count == 1


@patch("pbdata.pipeline.extract.download_structure_files")
def test_extract_adds_biolip_binding_site_interfaces(mock_download) -> None:
    mock_download.return_value = {}
    sample = CanonicalBindingSample(
        sample_id="BIOLIP_1ABC_A_401",
        task_type="protein_ligand",
        source_database="BioLiP",
        source_record_id="1ABC:A:B:ATP",
        pdb_id="1ABC",
        chain_ids_receptor=["A"],
        ligand_id="ATP",
        provenance={
            "ingested_at": "2026-03-08T00:00:00+00:00",
            "binding_site_residue_ids": ["TYR15", "ASP34"],
            "binding_site_residue_names": ["TYR", "ASP"],
        },
        quality_flags=[],
        quality_score=0.0,
    )

    result = extract_rcsb_entry(_mock_entry("1ABC"), assay_samples=[sample])
    interfaces = result["interfaces"]
    biolip_iface = next(
        iface for iface in interfaces
        if iface.binding_site_residue_ids == ["TYR15", "ASP34"]
    )
    assert biolip_iface.interface_type == "protein_ligand"
    assert biolip_iface.binding_site_chain_ids == ["A"]
