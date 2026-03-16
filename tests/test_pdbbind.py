from pathlib import Path
from uuid import uuid4

from pbdata.sources.pdbbind import PDBbindAdapter, load_pdbbind_index

_LOCAL_TMP = Path(__file__).parent / "_tmp"
_LOCAL_TMP.mkdir(exist_ok=True)


def _tmp_dir(name: str) -> Path:
    path = _LOCAL_TMP / f"{uuid4().hex}_{name}"
    path.mkdir(exist_ok=True)
    return path


def test_load_pdbbind_index_parses_affinity_rows() -> None:
    root = _tmp_dir("pdbbind")
    index_dir = root / "index"
    index_dir.mkdir()
    (index_dir / "INDEX_general_PL_data.2020").write_text(
        "# comment\n"
        "1abc 2.00 2020 7.30 Kd=5.0nM // reference text\n",
        encoding="utf-8",
    )

    rows = load_pdbbind_index(root)

    assert len(rows) == 1
    row = rows[0]
    assert row["pdb_id"] == "1ABC"
    assert row["affinity_type"] == "Kd"
    assert row["affinity_value"] == 5.0
    assert row["affinity_value_standardized"] == 5.0


def test_load_pdbbind_index_accepts_current_release_name() -> None:
    root = _tmp_dir("pdbbind_current_release")
    index_dir = root / "index"
    index_dir.mkdir()
    (index_dir / "INDEX_general_PL.2020R1.lst").write_text(
        "# comment\n"
        "2xyz 1.80 2020 8.10 Ki=2.5uM // current release naming\n",
        encoding="utf-8",
    )

    rows = load_pdbbind_index(root)

    assert len(rows) == 1
    row = rows[0]
    assert row["pdb_id"] == "2XYZ"
    assert row["affinity_type"] == "Ki"
    assert row["affinity_value_standardized"] == 2500.0


def test_pdbbind_adapter_normalizes_local_index_row() -> None:
    root = _tmp_dir("pdbbind_adapter")
    index_dir = root / "index"
    index_dir.mkdir()
    (index_dir / "INDEX_general_PL_data.2020").write_text(
        "1abc 2.00 2020 7.30 Ki=2.0uM // reference text\n",
        encoding="utf-8",
    )

    adapter = PDBbindAdapter(local_dir=root)
    sample = adapter.fetch_all()[0]

    assert sample.source_database == "PDBbind"
    assert sample.pdb_id == "1ABC"
    assert sample.assay_type == "Ki"
    assert sample.assay_value == 2.0
    assert sample.assay_value_standardized == 2000.0
