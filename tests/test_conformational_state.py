import json
from pathlib import Path
from uuid import uuid4

import pytest

from pbdata.dataset.conformations import build_conformation_states
from pbdata.schemas.conformational_state import ConformationalStateRecord

_LOCAL_TMP = Path(__file__).parent / "_tmp"
_LOCAL_TMP.mkdir(exist_ok=True)


def _tmp_dir(name: str) -> Path:
    path = _LOCAL_TMP / f"{uuid4().hex}_{name}"
    path.mkdir(exist_ok=True)
    return path


def test_conformational_state_schema_validates_required_fields() -> None:
    record = ConformationalStateRecord(
        target_id="P12345",
        state_id="P12345:experimental",
        pdb_id="1ABC",
        structure_source="RCSB",
        apo_or_holo="holo",
        active_inactive_unknown="unknown",
        open_closed_unknown="unknown",
        ligand_class_in_state="protein_ligand",
        conformation_cluster="experimental_observed",
        provenance={"source": "RCSB"},
    )
    assert record.pdb_id == "1ABC"


def test_conformational_state_schema_requires_state_id() -> None:
    with pytest.raises(Exception):
        ConformationalStateRecord(target_id="P12345", pdb_id="1ABC", structure_source="RCSB", provenance={"source": "RCSB"})  # type: ignore[call-arg]


def test_build_conformation_states_writes_new_shape() -> None:
    tmp_root = _tmp_dir("conformation_build")
    extracted_dir = tmp_root / "extracted"
    (extracted_dir / "entry").mkdir(parents=True, exist_ok=True)
    (extracted_dir / "chains").mkdir(parents=True, exist_ok=True)
    (extracted_dir / "entry" / "1ABC.json").write_text(
        json.dumps({
            "pdb_id": "1ABC",
            "structure_file_cif_path": "data/structures/rcsb/1ABC.cif",
            "task_hint": "protein_ligand",
        }),
        encoding="utf-8",
    )
    (extracted_dir / "chains" / "1ABC.json").write_text(
        json.dumps([{"pdb_id": "1ABC", "uniprot_id": "P12345"}]),
        encoding="utf-8",
    )

    out_dir = tmp_root / "conformations"
    states_path, manifest_path = build_conformation_states(extracted_dir, out_dir)

    assert states_path.exists()
    assert manifest_path.exists()
    rows = json.loads(states_path.read_text(encoding="utf-8"))
    assert rows[0]["pdb_id"] == "1ABC"
    assert "ligand_class_in_state" in rows[0]
