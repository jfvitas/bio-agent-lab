import gzip
import json
import tarfile
from pathlib import Path
from uuid import uuid4

from typer.testing import CliRunner

from pbdata.cli import app
from pbdata.source_indexes import index_alphafold_archive, index_uniprot_swissprot
from pbdata.storage import build_storage_layout

_LOCAL_TMP = Path(__file__).parent / "_tmp"
_LOCAL_TMP.mkdir(exist_ok=True)


def _tmp_dir(name: str) -> Path:
    path = _LOCAL_TMP / f"{uuid4().hex}_{name}"
    path.mkdir(parents=True, exist_ok=True)
    return path


def test_index_uniprot_swissprot_builds_lightweight_index() -> None:
    root = _tmp_dir("uniprot_index")
    layout = build_storage_layout(root)
    source_path = root / "uniprot_sprot.dat.gz"
    with gzip.open(source_path, "wt", encoding="utf-8") as handle:
        handle.write(
            "ID   TEST_HUMAN Reviewed; 123 AA.\n"
            "AC   P12345;\n"
            "DE   RecName: Full=Test protein;\n"
            "GN   Name=TP1;\n"
            "OS   Homo sapiens.\n"
            "OX   NCBI_TaxID=9606;\n"
            "DR   PDB; 1ABC; X-ray; 2.00 A.\n"
            "DR   InterPro; IPR000001; Example.\n"
            "DR   Pfam; PF00001; Example.\n"
            "DR   GO; GO:0000001; Example.\n"
            "KW   Keyword1; Keyword2.\n"
            "SQ   SEQUENCE   4 AA;\n"
            "     MAAA\n"
            "//\n"
        )

    result = index_uniprot_swissprot(layout, source_path=source_path)

    with gzip.open(result.index_path, "rt", encoding="utf-8") as handle:
        rows = [json.loads(line) for line in handle if line.strip()]

    assert result.record_count == 1
    assert rows[0]["accession"] == "P12345"
    assert rows[0]["sequence_length"] == 4
    assert rows[0]["pdb_ids"] == ["1ABC"]


def test_index_alphafold_archive_builds_accession_lookup() -> None:
    root = _tmp_dir("alphafold_index")
    layout = build_storage_layout(root)
    archive_path = root / "swissprot_pdb_v6.tar"
    member_path = root / "AF-P12345-F1-model_v6.pdb.gz"
    with gzip.open(member_path, "wt", encoding="utf-8") as handle:
        handle.write("MODEL")
    with tarfile.open(archive_path, "w") as archive:
        archive.add(member_path, arcname=member_path.name)

    result = index_alphafold_archive(layout, archive_path=archive_path)

    with gzip.open(result.index_path, "rt", encoding="utf-8") as handle:
        rows = [json.loads(line) for line in handle if line.strip()]

    assert result.record_count == 1
    assert rows[0]["accession"] == "P12345"
    assert rows[0]["entry_id"] == "AF-P12345-F1-model_v6"
    assert rows[0]["model_version"] == "v6"


def test_cli_source_index_commands_write_indexes() -> None:
    runner = CliRunner()
    root = _tmp_dir("source_index_cli")
    layout = build_storage_layout(root)
    (root / "data_sources" / "alphafold").mkdir(parents=True, exist_ok=True)
    (root / "data_sources" / "uniprot").mkdir(parents=True, exist_ok=True)

    uniprot_path = root / "data_sources" / "uniprot" / "uniprot_sprot.dat.gz"
    with gzip.open(uniprot_path, "wt", encoding="utf-8") as handle:
        handle.write(
            "ID   TEST_HUMAN Reviewed; 123 AA.\n"
            "AC   P12345;\n"
            "DE   RecName: Full=Test protein;\n"
            "OS   Homo sapiens.\n"
            "SQ   SEQUENCE   4 AA;\n"
            "     MAAA\n"
            "//\n"
        )

    archive_path = root / "data_sources" / "alphafold" / "swissprot_pdb_v6.tar"
    member_path = root / "AF-P12345-F1-model_v6.pdb.gz"
    with gzip.open(member_path, "wt", encoding="utf-8") as handle:
        handle.write("MODEL")
    with tarfile.open(archive_path, "w") as archive:
        archive.add(member_path, arcname=member_path.name)

    (root / "configs").mkdir(parents=True, exist_ok=True)
    config_path = root / "configs" / "sources.yaml"
    config_path.write_text(
        "\n".join(
            [
                "storage_root: " + str(root),
                "sources:",
                "  alphafold_db:",
                "    enabled: true",
                "    extra:",
                "      local_archive: " + str(archive_path),
                "  uniprot:",
                "    enabled: true",
                "    extra:",
                "      local_swissprot: " + str(uniprot_path),
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    alphafold_result = runner.invoke(
        app,
        ["--config", str(config_path), "--storage-root", str(root), "index-alphafold-archive"],
        catch_exceptions=False,
    )
    uniprot_result = runner.invoke(
        app,
        ["--config", str(config_path), "--storage-root", str(root), "index-uniprot-swissprot"],
        catch_exceptions=False,
    )

    assert alphafold_result.exit_code == 0
    assert uniprot_result.exit_code == 0
    assert (layout.source_indexes_dir / "alphafold_archive_index.jsonl.gz").exists()
    assert (layout.source_indexes_dir / "uniprot_swissprot_index.jsonl.gz").exists()
