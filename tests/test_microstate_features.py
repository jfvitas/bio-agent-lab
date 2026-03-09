"""Tests for heuristic microstate and local physics feature stages."""

import json
from pathlib import Path
from uuid import uuid4

import gemmi

from pbdata.features.microstate import build_microstate_records
from pbdata.features.physics_features import build_local_physics_features

_LOCAL_TMP = Path(__file__).parent / "_tmp"
_LOCAL_TMP.mkdir(exist_ok=True)


def _tmp_dir(name: str) -> Path:
    path = _LOCAL_TMP / f"{uuid4().hex}_{name}"
    path.mkdir(exist_ok=True)
    return path


def _write_ionizable_context_cif(path: Path) -> None:
    structure = gemmi.Structure()
    structure.name = "1ABC"
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
    chain.add_residue(_residue("GLU", 11, "CD", "C", (3.2, 0.0, 0.0)))
    chain.add_residue(_residue("LYS", 12, "NZ", "N", (5.1, 0.0, 0.0)))
    model.add_chain(chain)

    metal_chain = gemmi.Chain("Z")
    metal_chain.add_residue(_residue("ZN", 1, "ZN", "Zn", (0.8, 0.0, 0.0)))
    model.add_chain(metal_chain)

    structure.add_model(model)
    structure.make_mmcif_document().write_file(str(path))


def test_build_microstate_records_from_local_cif() -> None:
    tmp = _tmp_dir("microstates")
    extracted = tmp / "extracted"
    for name in ["entry", "assays"]:
        (extracted / name).mkdir(parents=True, exist_ok=True)

    cif_path = tmp / "1ABC.cif"
    _write_ionizable_context_cif(cif_path)

    (extracted / "entry" / "1ABC.json").write_text(json.dumps({
        "pdb_id": "1ABC",
        "structure_file_cif_path": str(cif_path),
    }), encoding="utf-8")
    (extracted / "assays" / "1ABC.json").write_text(json.dumps([{
        "pdb_id": "1ABC",
        "pair_identity_key": "protein_ligand|1ABC|A|ATP|wt",
        "binding_affinity_type": "Kd",
    }]), encoding="utf-8")

    out_dir = tmp / "microstates_out"
    records_path, manifest_path = build_microstate_records(extracted, out_dir)

    rows = json.loads(records_path.read_text(encoding="utf-8"))
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    assert len(rows) == 1
    assert rows[0]["record_count"] == 3
    assert rows[0]["method"] == "heuristic_local_context_v1"
    residue_names = {item["residue_name"] for item in rows[0]["microstates"]}
    assert residue_names == {"ASP", "GLU", "LYS"}
    assert any(item["confidence"] == "medium" for item in rows[0]["microstates"])
    assert manifest["status"] == "materialized_from_local_structures"


def test_build_local_physics_features_from_microstates() -> None:
    tmp = _tmp_dir("physics")
    microstate_path = tmp / "microstate_records.json"
    microstate_path.write_text(json.dumps([{
        "pdb_id": "1ABC",
        "pair_identity_key": "protein_ligand|1ABC|A|ATP|wt",
        "binding_affinity_type": "Kd",
        "microstates": [
            {
                "residue_name": "ASP",
                "adjusted_charge_estimate": -0.7,
                "nearest_same_charge_distance": 3.3,
                "nearest_opposite_charge_distance": 4.1,
                "nearest_metal_distance": 2.8,
            },
            {
                "residue_name": "GLU",
                "adjusted_charge_estimate": -0.8,
                "nearest_same_charge_distance": 3.3,
                "nearest_opposite_charge_distance": 4.2,
                "nearest_metal_distance": None,
            },
            {
                "residue_name": "LYS",
                "adjusted_charge_estimate": 0.9,
                "nearest_same_charge_distance": None,
                "nearest_opposite_charge_distance": 3.9,
                "nearest_metal_distance": None,
            },
        ],
    }]), encoding="utf-8")

    out_dir = tmp / "physics_out"
    records_path, manifest_path = build_local_physics_features(microstate_path, out_dir)

    rows = json.loads(records_path.read_text(encoding="utf-8"))
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    assert len(rows) == 1
    row = rows[0]
    assert row["microstate_record_count"] == 3
    assert row["positive_residue_count"] == 1
    assert row["negative_residue_count"] == 2
    assert row["same_charge_contact_count"] == 2
    assert row["opposite_charge_contact_count"] == 3
    assert row["metal_contact_count"] == 1
    assert row["acidic_cluster_penalty"] > 0
    assert manifest["status"] == "materialized_from_microstates"
