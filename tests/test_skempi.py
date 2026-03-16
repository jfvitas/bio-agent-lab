from pathlib import Path
from unittest.mock import Mock, patch
from uuid import uuid4

from pbdata.storage import validate_skempi_csv
from pbdata.sources.skempi import SKEMPIAdapter, _compute_ddg, _parse_row, load_skempi_csv

_LOCAL_TMP = Path(__file__).parent / "_tmp"
_LOCAL_TMP.mkdir(exist_ok=True)


def _tmp_dir(name: str) -> Path:
    path = _LOCAL_TMP / f"{uuid4().hex}_{name}"
    path.mkdir(exist_ok=True)
    return path


def test_compute_ddg_prefers_explicit_column() -> None:
    ddg, temp_c = _compute_ddg({
        "ddG (kcal/mol)": "1.75",
        "Temperature": "300.15",
    })

    assert ddg == 1.75
    assert temp_c == 27.0


def test_compute_ddg_from_kd_ratio() -> None:
    ddg, temp_c = _compute_ddg({
        "affinity_mut (M)": "2e-8",
        "affinity_wt (M)": "1e-8",
        "Temperature": "298.15",
    })

    assert ddg is not None
    assert round(ddg, 6) == 0.410637
    assert temp_c == 25.0


def test_parse_row_returns_mutation_ddg_sample() -> None:
    sample = _parse_row({
        "#Pdb": "1abc",
        "#Mutation(s)_cleaned": "A42V",
        "Protein 1": "ProtA",
        "Protein 2": "ProtB",
        "ddG (kcal/mol)": "0.5",
    }, 7)

    assert sample is not None
    assert sample.task_type == "mutation_ddg"
    assert sample.pdb_id == "1ABC"
    assert sample.mutation_string == "A42V"
    assert sample.source_record_id == "1ABC:A42V"
    assert sample.provenance["skempi_row"] == 7


def test_load_skempi_csv_reads_local_file() -> None:
    path = _tmp_dir("skempi_local") / "skempi.csv"
    path.write_text(
        "#Pdb;#Mutation(s)_cleaned;ddG (kcal/mol)\n"
        "1ABC;A42V;1.2\n",
        encoding="utf-8",
    )

    rows = load_skempi_csv(path, download=False)

    assert len(rows) == 1
    assert rows[0].pdb_id == "1ABC"


def test_validate_skempi_csv_accepts_current_header_variant() -> None:
    path = _tmp_dir("skempi_validate") / "skempi.csv"
    path.write_text(
        "#Pdb;Mutation(s)_PDB;Mutation(s)_cleaned;ddG (kcal/mol)\n"
        "1ABC;A42V;A42V;1.2\n",
        encoding="utf-8",
    )

    assert validate_skempi_csv(path) is True


def test_skempi_adapter_fetch_metadata_writes_local_cache() -> None:
    local_path = _tmp_dir("skempi_cache") / "cache" / "skempi.csv"
    response = Mock()
    response.raise_for_status.return_value = None
    response.text = "#Pdb;#Mutation(s)_cleaned;ddG (kcal/mol)\n1ABC;A42V;1.2\n"

    with patch("pbdata.sources.skempi.requests.get", return_value=response):
        raw = SKEMPIAdapter(local_path=local_path).fetch_metadata("ignored")

    assert "csv_text" in raw
    assert local_path.exists()
