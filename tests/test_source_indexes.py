import gzip
import json
import zipfile
import tarfile
from pathlib import Path
from uuid import uuid4

from typer.testing import CliRunner

from pbdata.cli import app
from pbdata.source_indexes import (
    index_alphafold_archive,
    index_cath_domains,
    index_reactome_pathways,
    index_scop_domains,
    index_uniprot_swissprot,
)
from pbdata.sources.bindingdb_bulk import build_bindingdb_bulk_index, fetch_bindingdb_bulk_samples
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
    assert result.lookup_db_path is not None
    assert result.lookup_db_path.exists()


def test_index_reactome_pathways_builds_accession_lookup() -> None:
    root = _tmp_dir("reactome_index")
    layout = build_storage_layout(root)
    mapping_path = root / "UniProt2Reactome_All_Levels.txt"
    pathways_path = root / "ReactomePathways.txt"
    mapping_path.write_text(
        "\n".join(
            [
                "P12345\tR-HSA-12345\thttps://reactome.org/PathwayBrowser/#/R-HSA-12345\tSignal transduction\tTAS\tHomo sapiens",
                "P12345\tR-HSA-67890\thttps://reactome.org/PathwayBrowser/#/R-HSA-67890\tImmune system\tIEA\tHomo sapiens",
                "Q99999\tR-HSA-12345\thttps://reactome.org/PathwayBrowser/#/R-HSA-12345\tSignal transduction\tIEA\tHomo sapiens",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    pathways_path.write_text(
        "\n".join(
            [
                "R-HSA-12345\tSignal transduction\tHomo sapiens",
                "R-HSA-67890\tImmune system\tHomo sapiens",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    result = index_reactome_pathways(layout, mapping_path=mapping_path, pathways_path=pathways_path)

    with gzip.open(result.index_path, "rt", encoding="utf-8") as handle:
        rows = [json.loads(line) for line in handle if line.strip()]

    assert result.record_count == 2
    assert rows[0]["uniprot_id"] == "P12345"
    assert rows[0]["pathway_ids"] == ["R-HSA-12345", "R-HSA-67890"]
    assert rows[0]["pathway_count"] == 2
    assert result.lookup_db_path is not None
    assert result.lookup_db_path.exists()


def test_index_cath_domains_builds_pdb_lookup() -> None:
    root = _tmp_dir("cath_index")
    layout = build_storage_layout(root)
    cath_dir = root / "data_sources" / "cath"
    cath_dir.mkdir(parents=True, exist_ok=True)
    domain_list_path = cath_dir / "cath-domain-list.txt"
    boundaries_path = cath_dir / "cath-domain-boundaries.txt"
    names_path = cath_dir / "cath-names.txt"
    domain_list_path.write_text(
        "1abcA00     1    10     8    10     1     1     1     1     1    59 1.000\n",
        encoding="utf-8",
    )
    boundaries_path.write_text(
        "1abcA D01 F00  1  A    1 - A  100 -\n",
        encoding="utf-8",
    )
    names_path.write_text(
        "1.10.8.10    1abcA00    :Example cath fold\n",
        encoding="utf-8",
    )

    result = index_cath_domains(
        layout,
        domain_list_path=domain_list_path,
        boundaries_path=boundaries_path,
        names_path=names_path,
    )

    with gzip.open(result.index_path, "rt", encoding="utf-8") as handle:
        rows = [json.loads(line) for line in handle if line.strip()]

    assert result.record_count == 1
    assert rows[0]["pdb_id"] == "1ABC"
    assert rows[0]["domain_ids"] == ["1.10.8.10"]
    assert rows[0]["chain_to_domain_ids"]["A"] == ["1.10.8.10"]


def test_index_scop_domains_builds_pdb_lookup() -> None:
    root = _tmp_dir("scop_index")
    layout = build_storage_layout(root)
    scop_dir = root / "data_sources" / "scope"
    scop_dir.mkdir(parents=True, exist_ok=True)
    classification_path = scop_dir / "dir.cla.scope.2.08-stable.txt"
    descriptions_path = scop_dir / "dir.des.scope.txt"
    classification_path.write_text(
        "d1abca_\t1abc\tA:\ta.1.1.1\t113449\tcl=46456,cf=46457,sf=46458,fa=46459,dm=46460,sp=116748,px=113449\n",
        encoding="utf-8",
    )
    descriptions_path.write_text(
        "46459\tfa\ta.1.1.1\t-\tExample scop fold\n",
        encoding="utf-8",
    )

    result = index_scop_domains(
        layout,
        classification_path=classification_path,
        descriptions_path=descriptions_path,
    )

    with gzip.open(result.index_path, "rt", encoding="utf-8") as handle:
        rows = [json.loads(line) for line in handle if line.strip()]

    assert result.record_count == 1
    assert rows[0]["pdb_id"] == "1ABC"
    assert rows[0]["domain_ids"] == ["a.1.1.1"]
    assert rows[0]["chain_to_domain_ids"]["A"] == ["a.1.1.1"]


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
    assert result.lookup_db_path is not None
    assert result.lookup_db_path.exists()


def test_index_alphafold_archive_supports_chunked_resume() -> None:
    root = _tmp_dir("alphafold_resume")
    layout = build_storage_layout(root)
    archive_path = root / "swissprot_pdb_v6.tar"
    member_paths: list[Path] = []
    for accession in ("P12345", "Q99999", "R11111"):
        member_path = root / f"AF-{accession}-F1-model_v6.pdb.gz"
        with gzip.open(member_path, "wt", encoding="utf-8") as handle:
            handle.write("MODEL")
        member_paths.append(member_path)
    with tarfile.open(archive_path, "w") as archive:
        for member_path in member_paths:
            archive.add(member_path, arcname=member_path.name)

    first = index_alphafold_archive(layout, archive_path=archive_path, chunk_size=1)
    first_manifest = json.loads(first.manifest_path.read_text(encoding="utf-8"))
    second = index_alphafold_archive(layout, archive_path=archive_path, chunk_size=1, resume=True)
    third = index_alphafold_archive(layout, archive_path=archive_path, chunk_size=1, resume=True)

    final_manifest = json.loads(third.manifest_path.read_text(encoding="utf-8"))

    assert first.record_count == 1
    assert second.record_count == 2
    assert third.record_count == 3
    assert first_manifest["status"] == "partial"
    assert first_manifest["progress"]["next_byte_offset"] > 0
    assert final_manifest["status"] == "completed"
    assert final_manifest["progress"]["resume_supported"] is True


def test_build_bindingdb_bulk_index_materializes_lookup_rows() -> None:
    root = _tmp_dir("bindingdb_bulk_index")
    layout = build_storage_layout(root)
    dump_zip_path = root / "BDB-mySQL_All_202603_dmp.zip"
    dump_member = "BDB-mySQL_All_202603.dmp"
    dump_text = "\n".join(
        [
            "INSERT INTO `pdb_bdb` VALUES ('1ABC','1001',NULL,NULL,NULL,NULL,NULL,NULL,NULL);",
            "INSERT INTO `cobweb_bdb` VALUES ('Kinase A','ATP',42,'Kd',5.0000,' 5.0',0,1001,'Homo sapiens');",
            "INSERT INTO `monomer` VALUES (0,'','null','C10H16N5O13P3','ATP','ATP','AAAA-BBBB',NULL,NULL,42,'InChI=1S/example','507.00','Small organic molecule',1,'CCO',NULL);",
            "INSERT INTO `enzyme_reactant_set` VALUES ('Kinase A',NULL,1,1001,NULL,NULL,7,NULL,NULL,NULL,'ATP',NULL,42,NULL,NULL,NULL,8,'protein_ligand',NULL,NULL);",
            "INSERT INTO `polymer` VALUES (NULL,NULL,'Linear',NULL,'Homo sapiens',NULL,'Homo sapiens','Protein','Kinase A',320,NULL,1,'9606','P12345',8,'1ABC',NULL,NULL,NULL);",
            "INSERT INTO `entry` VALUES (NULL,'Example citation','2026-03-16 00:00:00','Example kinase binding',NULL,NULL,7,'Enzyme Inhibition',NULL,'EZ123');",
        ]
    )
    with zipfile.ZipFile(dump_zip_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(dump_member, dump_text)

    result = build_bindingdb_bulk_index(layout, dump_zip_path=dump_zip_path)
    rows = fetch_bindingdb_bulk_samples(layout, "1ABC", index_path=result.index_path)

    assert result.record_count == 1
    assert result.pdb_count == 1
    assert rows
    assert rows[0].pdb_id == "1ABC"
    assert rows[0].uniprot_ids == ["P12345"]
    assert rows[0].ligand_id == "ATP"
    assert rows[0].assay_type == "Kd"
    assert rows[0].assay_value_standardized == 5.0
    assert rows[0].provenance["source_mode"] == "bulk_index"


def test_cli_source_index_commands_write_indexes() -> None:
    runner = CliRunner()
    root = _tmp_dir("source_index_cli")
    layout = build_storage_layout(root)
    (root / "data_sources" / "alphafold").mkdir(parents=True, exist_ok=True)
    (root / "data_sources" / "bindingdb").mkdir(parents=True, exist_ok=True)
    (root / "data_sources" / "cath").mkdir(parents=True, exist_ok=True)
    (root / "data_sources" / "reactome").mkdir(parents=True, exist_ok=True)
    (root / "data_sources" / "scope").mkdir(parents=True, exist_ok=True)
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

    bindingdb_path = root / "data_sources" / "bindingdb" / "BDB-mySQL_All_202603_dmp.zip"
    with zipfile.ZipFile(bindingdb_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(
            "BDB-mySQL_All_202603.dmp",
            "\n".join(
                [
                    "INSERT INTO `pdb_bdb` VALUES ('1ABC','1001',NULL,NULL,NULL,NULL,NULL,NULL,NULL);",
                    "INSERT INTO `cobweb_bdb` VALUES ('Kinase A','ATP',42,'Kd',5.0000,' 5.0',0,1001,'Homo sapiens');",
                    "INSERT INTO `monomer` VALUES (0,'','null','C10H16N5O13P3','ATP','ATP','AAAA-BBBB',NULL,NULL,42,'InChI=1S/example','507.00','Small organic molecule',1,'CCO',NULL);",
                    "INSERT INTO `enzyme_reactant_set` VALUES ('Kinase A',NULL,1,1001,NULL,NULL,7,NULL,NULL,NULL,'ATP',NULL,42,NULL,NULL,NULL,8,'protein_ligand',NULL,NULL);",
                    "INSERT INTO `polymer` VALUES (NULL,NULL,'Linear',NULL,'Homo sapiens',NULL,'Homo sapiens','Protein','Kinase A',320,NULL,1,'9606','P12345',8,'1ABC',NULL,NULL,NULL);",
                    "INSERT INTO `entry` VALUES (NULL,'Example citation','2026-03-16 00:00:00','Example kinase binding',NULL,NULL,7,'Enzyme Inhibition',NULL,'EZ123');",
                ]
            ),
        )

    reactome_mapping_path = root / "data_sources" / "reactome" / "UniProt2Reactome_All_Levels.txt"
    reactome_pathways_path = root / "data_sources" / "reactome" / "ReactomePathways.txt"
    reactome_mapping_path.write_text(
        "P12345\tR-HSA-12345\thttps://reactome.org/PathwayBrowser/#/R-HSA-12345\tSignal transduction\tTAS\tHomo sapiens\n",
        encoding="utf-8",
    )
    reactome_pathways_path.write_text(
        "R-HSA-12345\tSignal transduction\tHomo sapiens\n",
        encoding="utf-8",
    )
    cath_domain_list_path = root / "data_sources" / "cath" / "cath-domain-list.txt"
    cath_boundaries_path = root / "data_sources" / "cath" / "cath-domain-boundaries.txt"
    cath_names_path = root / "data_sources" / "cath" / "cath-names.txt"
    cath_domain_list_path.write_text(
        "1abcA00     1    10     8    10     1     1     1     1     1    59 1.000\n",
        encoding="utf-8",
    )
    cath_boundaries_path.write_text(
        "1abcA D01 F00  1  A    1 - A  100 -\n",
        encoding="utf-8",
    )
    cath_names_path.write_text(
        "1.10.8.10    1abcA00    :Example cath fold\n",
        encoding="utf-8",
    )
    scop_classification_path = root / "data_sources" / "scope" / "dir.cla.scope.2.08-stable.txt"
    scop_descriptions_path = root / "data_sources" / "scope" / "dir.des.scope.txt"
    scop_classification_path.write_text(
        "d1abca_\t1abc\tA:\ta.1.1.1\t113449\tcl=46456,cf=46457,sf=46458,fa=46459,dm=46460,sp=116748,px=113449\n",
        encoding="utf-8",
    )
    scop_descriptions_path.write_text(
        "46459\tfa\ta.1.1.1\t-\tExample scop fold\n",
        encoding="utf-8",
    )

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
                "  bindingdb:",
                "    enabled: true",
                "    extra:",
                "      bulk_zip: " + str(bindingdb_path),
                "  reactome:",
                "    enabled: true",
                "    extra:",
                "      local_dir: " + str(reactome_mapping_path.parent),
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
    bindingdb_result = runner.invoke(
        app,
        ["--config", str(config_path), "--storage-root", str(root), "index-bindingdb-bulk"],
        catch_exceptions=False,
    )
    reactome_result = runner.invoke(
        app,
        ["--config", str(config_path), "--storage-root", str(root), "index-reactome-pathways"],
        catch_exceptions=False,
    )
    cath_result = runner.invoke(
        app,
        ["--config", str(config_path), "--storage-root", str(root), "index-cath-domains"],
        catch_exceptions=False,
    )
    scop_result = runner.invoke(
        app,
        ["--config", str(config_path), "--storage-root", str(root), "index-scop-domains"],
        catch_exceptions=False,
    )

    assert alphafold_result.exit_code == 0
    assert uniprot_result.exit_code == 0
    assert bindingdb_result.exit_code == 0
    assert reactome_result.exit_code == 0
    assert cath_result.exit_code == 0
    assert scop_result.exit_code == 0
    assert (layout.source_indexes_dir / "alphafold_archive_index.jsonl.gz").exists()
    assert (layout.source_indexes_dir / "uniprot_swissprot_index.jsonl.gz").exists()
    assert (layout.source_indexes_dir / "bindingdb_bulk_index.sqlite").exists()
    assert (layout.source_indexes_dir / "reactome_pathway_index.sqlite").exists()
    assert (layout.source_indexes_dir / "cath_domain_index.sqlite").exists()
    assert (layout.source_indexes_dir / "scop_domain_index.sqlite").exists()
