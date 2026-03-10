"""Tests for microstate refinement and MM job planning artifacts."""

import json
from pathlib import Path
from uuid import uuid4

import gemmi

from pbdata.features.mm_features import (
    build_microstate_refinement_plan,
    build_mm_job_manifests,
    run_mm_job_bundles,
)

_LOCAL_TMP = Path(__file__).parent / "_tmp"
_LOCAL_TMP.mkdir(exist_ok=True)


def _tmp_dir(name: str) -> Path:
    path = _LOCAL_TMP / f"{uuid4().hex}_{name}"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _write_shell_cif(path: Path) -> None:
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

    _add_residue("ASP", 10, "CG", "C", (0.0, 0.0, 0.0))
    _add_residue("HIS", 11, "CE1", "C", (2.0, 0.0, 0.0))
    _add_residue("LYS", 12, "NZ", "N", (6.5, 0.0, 0.0))
    _add_residue("GLU", 40, "CD", "C", (20.0, 0.0, 0.0))
    model.add_chain(chain)
    structure.add_model(model)
    structure.make_mmcif_document().write_file(str(path))


def test_build_microstate_refinement_plan_from_microstates_and_interfaces() -> None:
    tmp = _tmp_dir("microstate_refinement")
    extracted = tmp / "extracted"
    for name in ["entry", "interfaces"]:
        (extracted / name).mkdir(parents=True, exist_ok=True)
    microstates_dir = tmp / "microstates"
    microstates_dir.mkdir(parents=True, exist_ok=True)

    cif_path = tmp / "1ABC.cif"
    _write_shell_cif(cif_path)
    (extracted / "entry" / "1ABC.json").write_text(json.dumps({
        "pdb_id": "1ABC",
        "structure_file_cif_path": str(cif_path),
    }), encoding="utf-8")
    (extracted / "interfaces" / "1ABC.json").write_text(json.dumps([{
        "pdb_id": "1ABC",
        "binding_site_chain_ids": ["A"],
        "binding_site_residue_ids": ["A:ASP10", "A:HIS11"],
        "entity_name_b": "ATP",
    }]), encoding="utf-8")
    (microstates_dir / "microstate_records.json").write_text(json.dumps([{
        "pdb_id": "1ABC",
        "pair_identity_key": "protein_ligand|1ABC|A|ATP|mut_A10V",
        "binding_affinity_type": "Kd",
        "microstates": [
            {"residue_name": "ASP", "confidence": "low", "nearest_same_charge_distance": 3.2},
            {"residue_name": "HIS", "confidence": "medium", "nearest_metal_distance": 2.8},
        ],
    }]), encoding="utf-8")

    out_dir = tmp / "refinement"
    records_path, manifest_path = build_microstate_refinement_plan(extracted, microstates_dir, out_dir)

    rows = json.loads(records_path.read_text(encoding="utf-8"))
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert len(rows) == 1
    row = rows[0]
    assert row["focus_residue_ids"] == ["A:ASP10", "A:HIS11"]
    assert row["shell_residue_ids"] == ["A:10", "A:11", "A:12"]
    assert row["shell_residue_count"] == 3
    assert any(plan["candidate_states"] == ["neutral_delta", "neutral_epsilon", "diprotonated"] for plan in row["protonation_site_plans"])
    assert "review_low_confidence_ionizable_residues" in row["recommended_policies"]
    assert "enumerate_histidine_tautomers" in row["recommended_policies"]
    assert "preserve_metal_coordination_state" in row["recommended_policies"]
    assert "keep_mutation_specific_microstates_separate" in row["recommended_policies"]
    assert manifest["status"] == "planned_from_microstate_and_interface_context"


def test_build_mm_job_manifests_from_refinement_records() -> None:
    tmp = _tmp_dir("mm_jobs")
    refinement_dir = tmp / "refinement"
    refinement_dir.mkdir(parents=True, exist_ok=True)
    cif_path = tmp / "1ABC.cif"
    _write_shell_cif(cif_path)
    (refinement_dir / "microstate_refinement_records.json").write_text(json.dumps([{
        "pdb_id": "1ABC",
        "pair_identity_key": "protein_ligand|1ABC|A|ATP|mut_A10V",
        "binding_affinity_type": "Kd",
        "structure_file_cif_path": str(cif_path),
        "mutation_key": "mut_A10V",
        "focus_residue_ids": ["A:ASP10", "A:HIS11"],
        "shell_residue_ids": ["A:10", "A:11", "A:12"],
        "protonation_site_plans": [{
            "chain_id": "A",
            "residue_number": 11,
            "residue_name": "HIS",
            "preferred_state": "mixed",
            "candidate_states": ["neutral_delta", "neutral_epsilon", "diprotonated"],
        }],
        "recommended_policies": [
            "preserve_metal_coordination_state",
            "review_acidic_cluster_protonation",
        ],
    }]), encoding="utf-8")

    out_dir = tmp / "jobs"
    records_path, manifest_path = build_mm_job_manifests(refinement_dir, out_dir)

    rows = json.loads(records_path.read_text(encoding="utf-8"))
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert len(rows) == 1
    row = rows[0]
    assert row["recommended_backend"] == "openmm_amber_local"
    assert row["shell_residue_count"] == 3
    assert "semiempirical_local_cluster" in row["backend_candidates"]
    assert row["requires_metal_parameterization"] is True
    assert row["requires_mutation_specific_setup"] is True
    bundles = json.loads((out_dir / "mm_backend_input_bundles.json").read_text(encoding="utf-8"))
    assert len(bundles) == 1
    assert bundles[0]["region_residue_ids"] == ["A:10", "A:11", "A:12"]
    assert bundles[0]["backend_inputs"]["semiempirical_local_cluster"]["enabled"] is True
    assert manifest["status"] == "planned_from_refinement_records"
    assert manifest["bundle_count"] == 1
    assert manifest["materialized_openmm_job_count"] == 1
    job_dir = Path(manifest["materialized_openmm_job_dirs"][0])
    assert job_dir.exists()
    assert (job_dir / "run_openmm_local.py").exists()
    assert (job_dir / "openmm_job_config.json").exists()
    assert (job_dir / cif_path.name).exists()
    config = json.loads((job_dir / "openmm_job_config.json").read_text(encoding="utf-8"))
    assert config["backend"] == "openmm_local"
    assert config["requires_metal_parameterization"] is True
    assert config["protonation_assignments"][0]["preferred_state"] == "mixed"


def test_run_mm_job_bundles_reports_backend_unavailable() -> None:
    tmp = _tmp_dir("run_mm_jobs")
    refinement_dir = tmp / "refinement"
    refinement_dir.mkdir(parents=True, exist_ok=True)
    cif_path = tmp / "1ABC.cif"
    _write_shell_cif(cif_path)
    (refinement_dir / "microstate_refinement_records.json").write_text(json.dumps([{
        "pdb_id": "1ABC",
        "pair_identity_key": "protein_ligand|1ABC|A|ATP|wt",
        "binding_affinity_type": "Kd",
        "structure_file_cif_path": str(cif_path),
        "mutation_key": "wt",
        "focus_residue_ids": ["A:ASP10"],
        "shell_residue_ids": ["A:10", "A:11"],
        "protonation_site_plans": [{
            "chain_id": "A",
            "residue_number": 10,
            "residue_name": "ASP",
            "preferred_state": "deprotonated",
            "candidate_states": ["deprotonated", "protonated"],
        }],
        "recommended_policies": [],
    }]), encoding="utf-8")

    jobs_dir = tmp / "jobs"
    build_mm_job_manifests(refinement_dir, jobs_dir)
    results_path, manifest_path = run_mm_job_bundles(jobs_dir, execute=False)

    results = json.loads(results_path.read_text(encoding="utf-8"))
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert len(results) == 1
    assert results[0]["bundle_valid"] is True
    assert results[0]["status"] in {"backend_unavailable", "backend_ready_not_executed"}
    execution_result = json.loads((Path(results[0]["job_dir"]) / "execution_result.json").read_text(encoding="utf-8"))
    assert execution_result["job_id"] == results[0]["job_id"]
    assert manifest["job_count"] == 1
