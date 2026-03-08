from pathlib import Path
from uuid import uuid4

from pbdata.sources.biolip import BioLiPAdapter, load_biolip_rows

_LOCAL_TMP = Path(__file__).parent / "_tmp"
_LOCAL_TMP.mkdir(exist_ok=True)


def _tmp_dir(name: str) -> Path:
    path = _LOCAL_TMP / f"{uuid4().hex}_{name}"
    path.mkdir(exist_ok=True)
    return path


def test_load_biolip_rows_parses_header_file() -> None:
    root = _tmp_dir("biolip")
    (root / "BioLiP.txt").write_text(
        "PDB_ID\treceptor_chain\tbinding_site_residues\tligand_chain\tligand_ID\tligand_serial\tbinding_affinity\tcatalytic_site_residues\tPubMed_ID\n"
        "1ABC\tA\tTYR15 ASP34\tB\tATP\t401\tKd=5.0 nM\tHIS57\t123456\n",
        encoding="utf-8",
    )

    rows = load_biolip_rows(root)

    assert len(rows) == 1
    assert rows[0]["pdb_id"] == "1ABC"
    assert rows[0]["ligand_id"] == "ATP"


def test_biolip_adapter_normalizes_binding_site_and_affinity() -> None:
    root = _tmp_dir("biolip_adapter")
    (root / "BioLiP.txt").write_text(
        "PDB_ID\treceptor_chain\tbinding_site_residues\tligand_chain\tligand_ID\tligand_serial\tbinding_affinity\tcatalytic_site_residues\tPubMed_ID\n"
        "1ABC\tA\tTYR15 ASP34\tB\tATP\t401\tKd=5.0 nM\tHIS57\t123456\n",
        encoding="utf-8",
    )

    sample = BioLiPAdapter(local_dir=root).fetch_all()[0]

    assert sample.source_database == "BioLiP"
    assert sample.pdb_id == "1ABC"
    assert sample.chain_ids_receptor == ["A"]
    assert sample.ligand_id == "ATP"
    assert sample.assay_type == "Kd"
    assert sample.assay_value == 5.0
    assert sample.assay_value_standardized == 5.0
    assert sample.provenance["binding_site_residue_ids"] == ["TYR15", "ASP34"]
    assert sample.provenance["pubmed_id"] == "123456"
