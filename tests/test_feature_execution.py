import json
from pathlib import Path
from uuid import uuid4

import gemmi
from typer.testing import CliRunner

from pbdata.cli import app
from pbdata.pipeline.feature_execution import run_feature_pipeline
from pbdata.storage import build_storage_layout

_LOCAL_TMP = Path(__file__).parent / "_tmp"
_LOCAL_TMP.mkdir(exist_ok=True)


def _tmp_dir(name: str) -> Path:
    path = _LOCAL_TMP / f"{uuid4().hex}_{name}"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _write_minimal_feature_cif(path: Path) -> None:
    structure = gemmi.Structure()
    structure.name = "1ABC"
    model = gemmi.Model("1")
    chain = gemmi.Chain("A")

    def _add_residue(name: str, seq_num: int, atom_name: str, element: str, pos: tuple[float, float, float]) -> None:
        residue = gemmi.Residue()
        residue.name = name
        residue.seqid = gemmi.SeqId(str(seq_num))
        atom = gemmi.Atom()
        atom.name = atom_name
        atom.element = gemmi.Element(element)
        atom.pos = gemmi.Position(*pos)
        residue.add_atom(atom)
        chain.add_residue(residue)

    _add_residue("ASP", 10, "OD1", "O", (0.0, 0.0, 0.0))
    _add_residue("LYS", 11, "NZ", "N", (3.0, 0.0, 0.0))
    model.add_chain(chain)

    ligand_chain = gemmi.Chain("B")
    ligand = gemmi.Residue()
    ligand.name = "LIG"
    ligand.seqid = gemmi.SeqId("1")
    atom = gemmi.Atom()
    atom.name = "O1"
    atom.element = gemmi.Element("O")
    atom.pos = gemmi.Position(2.0, 1.0, 0.0)
    ligand.add_atom(atom)
    ligand_chain.add_residue(ligand)
    model.add_chain(ligand_chain)

    metal_chain = gemmi.Chain("Z")
    metal = gemmi.Residue()
    metal.name = "ZN"
    metal.seqid = gemmi.SeqId("1")
    metal_atom = gemmi.Atom()
    metal_atom.name = "ZN"
    metal_atom.element = gemmi.Element("Zn")
    metal_atom.pos = gemmi.Position(0.5, 0.0, 0.0)
    metal.add_atom(metal_atom)
    metal_chain.add_residue(metal)
    model.add_chain(metal_chain)

    structure.add_model(model)
    structure.make_mmcif_document().write_file(str(path))


def _write_extracted_fixture(layout) -> Path:
    cif_path = layout.root / "1ABC.cif"
    _write_minimal_feature_cif(cif_path)
    for name in ["entry", "chains", "assays", "bound_objects", "interfaces", "provenance"]:
        (layout.extracted_dir / name).mkdir(parents=True, exist_ok=True)
    (layout.extracted_dir / "entry" / "1ABC.json").write_text(json.dumps({
        "pdb_id": "1ABC",
        "experimental_method": "X-RAY DIFFRACTION",
        "structure_resolution": 2.0,
        "structure_file_cif_path": str(cif_path),
    }), encoding="utf-8")
    (layout.extracted_dir / "chains" / "1ABC.json").write_text(json.dumps([
        {"pdb_id": "1ABC", "chain_id": "A", "is_protein": True, "uniprot_id": "P12345"},
    ]), encoding="utf-8")
    (layout.extracted_dir / "assays" / "1ABC.json").write_text(json.dumps([
        {
            "pdb_id": "1ABC",
            "pair_identity_key": "protein_ligand|1ABC|A|LIG|wt",
            "binding_affinity_type": "Kd",
            "binding_affinity_log10_standardized": 1.0,
            "reported_measurement_mean_log10_standardized": 1.0,
        }
    ]), encoding="utf-8")
    return cif_path


def test_run_feature_pipeline_full_build_creates_artifacts() -> None:
    tmp_root = _tmp_dir("site_feature_pipeline")
    layout = build_storage_layout(tmp_root)
    _write_extracted_fixture(layout)

    result = run_feature_pipeline(layout, run_id="testrun", degraded_mode=True)

    assert result["stage_statuses"]["canonical_input_resolution"] == "passed"
    assert (layout.artifact_manifests_dir / "testrun_input_manifest.json").exists()
    assert (layout.prepared_structures_artifacts_dir / "testrun" / "1ABC.sites.parquet").exists()
    assert (layout.base_features_artifacts_dir / "testrun" / "1ABC.env_vectors.parquet").exists()
    assert (layout.site_physics_artifacts_dir / "testrun" / "1ABC.site_refined.parquet").exists()
    assert (layout.graphs_artifacts_dir / "testrun" / "1ABC.graph.pt").exists()
    assert (layout.training_examples_artifacts_dir / "testrun" / "manifest.parquet").exists()
    assert (layout.feature_reports_dir / "testrun_summary.md").exists()


def test_feature_pipeline_cli_and_analysis_queue_export() -> None:
    tmp_root = _tmp_dir("site_feature_pipeline_cli")
    layout = build_storage_layout(tmp_root)
    _write_extracted_fixture(layout)
    runner = CliRunner()

    result = runner.invoke(
        app,
        ["--storage-root", str(tmp_root), "run-feature-pipeline", "--run-id", "queue_run", "--degraded-mode"],
        catch_exceptions=False,
    )
    queue_result = runner.invoke(
        app,
        ["--storage-root", str(tmp_root), "export-analysis-queue", "--run-id", "queue_run"],
        catch_exceptions=False,
    )

    assert result.exit_code == 0
    assert queue_result.exit_code == 0
    assert (layout.archetypes_artifacts_dir / "queue_run" / "archetypes.parquet").exists()
    assert (layout.external_analysis_artifacts_dir / "queue_run_analysis_queue.yaml").exists()
    batch_manifest = json.loads((layout.external_analysis_artifacts_dir / "queue_run_analysis_batch_manifest.json").read_text(encoding="utf-8"))
    assert batch_manifest["motif_class_count"] >= 1


def test_run_feature_pipeline_uses_surrogate_when_not_degraded() -> None:
    from pbdata.pipeline.physics_feedback import ingest_external_analysis_results, train_site_physics_surrogate

    tmp_root = _tmp_dir("site_feature_pipeline_surrogate")
    layout = build_storage_layout(tmp_root)
    _write_extracted_fixture(layout)
    run_feature_pipeline(layout, run_id="source_run", degraded_mode=True)
    archetypes_path = layout.archetypes_artifacts_dir / "source_run" / "archetypes.parquet"
    archetypes_path.parent.mkdir(parents=True, exist_ok=True)
    import pandas as pd
    pd.DataFrame([{
        "run_id": "source_run",
        "site_id": "1ABC|A|ASP|10|OD1|asp_carboxylate_oxygen",
        "motif_class": "asp_carboxylate_oxygen",
        "archetype_id": "asp_carboxylate_oxygen:abc123",
        "descriptor_hash": "abc123",
    }]).to_parquet(archetypes_path, index=False)
    for tool_name, payload in {
        "orca": {"atomic_charges": [-0.4], "donor_strength": 0.1, "acceptor_strength": 0.7, "polarizability_proxy": 0.3, "protonation_preference_score": -0.5, "metal_binding_propensity": 0.4, "aromatic_interaction_propensity": 0.2},
        "apbs": {"site_potential": -1.1, "field_magnitude_proxy": 2.2, "desolvation_penalty_proxy": 0.5},
        "openmm": {"effective_steric_radius": 1.0, "strain_proxy": 0.2},
    }.items():
        parsed_dir = layout.external_analysis_artifacts_dir / tool_name / "batch1" / "parsed"
        parsed_dir.mkdir(parents=True, exist_ok=True)
        parsed_dir.joinpath("parsed_results.jsonl").write_text(json.dumps({
            "fragment_id": "frag1",
            "archetype_id": "asp_carboxylate_oxygen:abc123",
            "motif_class": "asp_carboxylate_oxygen",
            "status": "success",
            **payload,
        }) + "\n", encoding="utf-8")
    ingest_external_analysis_results(layout, batch_id="batch1")
    train_site_physics_surrogate(layout, batch_id="batch1", source_run_id="source_run", surrogate_run_id="sur1")

    result = run_feature_pipeline(layout, run_id="surrogate_run", degraded_mode=False)

    assert result["stage_statuses"]["site_physics_enrichment"] == "passed"
    site_refined = pd.read_parquet(layout.site_physics_artifacts_dir / "surrogate_run" / "1ABC.site_refined.parquet")
    assert not site_refined.empty
    assert bool(site_refined.iloc[0]["degraded_mode"]) is False
