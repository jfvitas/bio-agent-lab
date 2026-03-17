import csv
import json
import gzip
import tarfile
from pathlib import Path
from unittest.mock import MagicMock, patch

from typer.testing import CliRunner

from pbdata.cli import app
from pbdata.data_pipeline.workflow_engine import harvest_unified_metadata
from pbdata.sources.alphafold import AlphaFoldAdapter
from pbdata.sources.cath import CATHAdapter
from pbdata.sources.interpro import InterProAdapter
from pbdata.sources.pfam import PfamAdapter
from pbdata.sources.reactome import ReactomeAdapter
from pbdata.sources.scop import SCOPAdapter
from pbdata.sources.uniprot import UniProtAdapter
from pbdata.source_indexes import (
    index_alphafold_archive,
    index_cath_domains,
    index_reactome_pathways,
    index_scop_domains,
    index_uniprot_swissprot,
)
from pbdata.storage import build_storage_layout
from tests.test_feature_execution import _tmp_dir, _write_extracted_fixture


@patch("pbdata.sources.uniprot.requests.get")
def test_uniprot_adapter_fetches_and_normalizes(mock_get: MagicMock) -> None:
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "primaryAccession": "P12345",
        "entryType": "UniProtKB reviewed (Swiss-Prot)",
        "proteinDescription": {
            "recommendedName": {
                "fullName": {"value": "Example kinase"}
            }
        },
        "genes": [{"geneName": {"value": "KIN1"}}],
        "organism": {"scientificName": "Homo sapiens", "taxonId": 9606},
        "sequence": {"value": "MPEPTIDE", "length": 8},
        "uniProtKBCrossReferences": [
            {"database": "PDB", "id": "1ABC"},
            {"database": "InterPro", "id": "IPR000001"},
            {"database": "Pfam", "id": "PF00001"},
            {"database": "GO", "id": "GO:0004672"},
        ],
        "keywords": [{"name": "Kinase"}],
    }
    mock_response.raise_for_status = MagicMock()
    mock_get.return_value = mock_response

    record = UniProtAdapter().fetch_annotation("P12345")

    assert record.accession == "P12345"
    assert record.reviewed is True
    assert record.protein_name == "Example kinase"
    assert record.gene_names == ["KIN1"]
    assert record.organism_name == "Homo sapiens"
    assert record.sequence_length == 8
    assert record.interpro_ids == ["IPR000001"]
    assert record.pfam_ids == ["PF00001"]


@patch("pbdata.sources.alphafold.requests.get")
def test_alphafold_adapter_fetches_prediction_metadata(mock_get: MagicMock) -> None:
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = [
        {
            "uniprotAccession": "P12345",
            "entryId": "AF-P12345-F1",
            "latestVersion": "4",
            "uniprotStart": 1,
            "uniprotEnd": 250,
            "cifUrl": "https://example.org/model.cif",
            "paeDocUrl": "https://example.org/model-pae.json",
            "bcifUrl": "https://example.org/model.bcif",
        }
    ]
    mock_response.raise_for_status = MagicMock()
    mock_get.return_value = mock_response

    record = AlphaFoldAdapter().fetch_prediction("P12345")

    assert record.accession == "P12345"
    assert record.entry_id == "AF-P12345-F1"
    assert record.model_version == "4"
    assert record.cif_url.endswith("model.cif")


@patch("pbdata.sources.reactome.requests.get")
def test_reactome_adapter_fetches_pathway_membership(mock_get: MagicMock) -> None:
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = [
        {"stId": "R-HSA-12345", "displayName": "Signal transduction"},
        {"stId": "R-HSA-67890", "displayName": "Immune system"},
    ]
    mock_response.raise_for_status = MagicMock()
    mock_get.return_value = mock_response

    record = ReactomeAdapter().fetch_annotation("P12345")

    assert record.uniprot_id == "P12345"
    assert record.pathway_count == 2
    assert "R-HSA-12345" in record.pathway_ids


@patch("pbdata.sources.pdbe_mappings.requests.get")
def test_pdbe_domain_mapping_adapters_fetch_and_normalize(mock_get: MagicMock) -> None:
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "1abc": {
            "InterPro": {
                "IPR000001": {
                    "name": "Kinase domain",
                    "mappings": [{"chain_id": "A"}],
                }
            }
        }
    }
    mock_response.raise_for_status = MagicMock()
    mock_get.return_value = mock_response

    record = InterProAdapter().fetch_annotation("1ABC")

    assert record.pdb_id == "1ABC"
    assert record.domain_ids == ["IPR000001"]
    assert record.chain_to_domain_ids["A"] == ["IPR000001"]


@patch("pbdata.sources.pdbe_mappings.requests.get")
def test_pdbe_fold_mapping_adapters_fetch_and_normalize(mock_get: MagicMock) -> None:
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "1abc": {
            "CATH": {
                "1.10.510.10": {
                    "name": "Mainly Alpha",
                    "mappings": [{"struct_asym_id": "A"}],
                }
            }
        }
    }
    mock_response.raise_for_status = MagicMock()
    mock_get.return_value = mock_response

    cath_record = CATHAdapter().fetch_annotation("1ABC")
    scop_record = SCOPAdapter().normalize_record(
        {
            "1abc": {
                "SCOP": {
                    "b.1.1.1": {
                        "name": "Immunoglobulin-like beta-sandwich",
                        "mappings": [{"chain_id": "A"}],
                    }
                }
            }
        },
        pdb_id="1ABC",
    )
    pfam_record = PfamAdapter().normalize_record(
        {
            "1abc": {
                "Pfam": {
                    "PF00069": {
                        "name": "Pkinase",
                        "mappings": [{"chain_id": "A"}],
                    }
                }
            }
        },
        pdb_id="1ABC",
    )

    assert cath_record.domain_ids == ["1.10.510.10"]
    assert scop_record.domain_ids == ["b.1.1.1"]
    assert pfam_record.domain_ids == ["PF00069"]


def test_harvest_unified_metadata_with_external_annotations() -> None:
    layout = build_storage_layout(_tmp_dir("workflow_metadata_enriched"))
    _write_extracted_fixture(layout)

    with (
        patch.object(
            UniProtAdapter,
            "fetch_annotation",
            return_value=UniProtAdapter().normalize_record(
                {
                    "primaryAccession": "P12345",
                    "entryType": "UniProtKB reviewed (Swiss-Prot)",
                    "proteinDescription": {"recommendedName": {"fullName": {"value": "Example kinase"}}},
                    "genes": [{"geneName": {"value": "KIN1"}}],
                    "organism": {"scientificName": "Homo sapiens", "taxonId": 9606},
                    "sequence": {"value": "MPEPTIDE", "length": 8},
                    "uniProtKBCrossReferences": [{"database": "InterPro", "id": "IPR000001"}],
                    "keywords": [{"name": "Kinase"}],
                }
            ),
        ),
        patch.object(
            AlphaFoldAdapter,
            "fetch_prediction",
            return_value=AlphaFoldAdapter().normalize_record(
                {
                    "uniprotAccession": "P12345",
                    "entryId": "AF-P12345-F1",
                    "latestVersion": "4",
                    "uniprotStart": 1,
                    "uniprotEnd": 250,
                    "cifUrl": "https://example.org/model.cif",
                    "paeDocUrl": "https://example.org/model-pae.json",
                    "bcifUrl": "https://example.org/model.bcif",
                }
            ),
        ),
        patch.object(
            ReactomeAdapter,
            "fetch_annotation",
            return_value=ReactomeAdapter().normalize_record(
                "P12345",
                [{"stId": "R-HSA-12345", "displayName": "Signal transduction"}],
            ),
        ),
        patch.object(
            InterProAdapter,
            "fetch_annotation",
            return_value=InterProAdapter().normalize_record(
                {"1abc": {"InterPro": {"IPR000001": {"name": "Kinase domain", "mappings": [{"chain_id": "A"}]}}}},
                pdb_id="1ABC",
            ),
        ),
        patch.object(
            PfamAdapter,
            "fetch_annotation",
            return_value=PfamAdapter().normalize_record(
                {"1abc": {"Pfam": {"PF00001": {"name": "Pkinase", "mappings": [{"chain_id": "A"}]}}}},
                pdb_id="1ABC",
            ),
        ),
        patch.object(
            CATHAdapter,
            "fetch_annotation",
            return_value=CATHAdapter().normalize_record(
                {"1abc": {"CATH": {"1.10.510.10": {"name": "Mainly Alpha", "mappings": [{"chain_id": "A"}]}}}},
                pdb_id="1ABC",
            ),
        ),
        patch.object(
            SCOPAdapter,
            "fetch_annotation",
            return_value=SCOPAdapter().normalize_record(
                {"1abc": {"SCOP": {"b.1.1.1": {"name": "Ig-like", "mappings": [{"chain_id": "A"}]}}}},
                pdb_id="1ABC",
            ),
        ),
    ):
        artifacts = harvest_unified_metadata(
            layout,
            enrich_uniprot=True,
            enrich_alphafold=True,
            enrich_reactome=True,
            enrich_interpro=True,
            enrich_pfam=True,
            enrich_cath=True,
            enrich_scop=True,
        )

    with Path(artifacts["metadata_csv"]).open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    manifest = json.loads(Path(artifacts["manifest"]).read_text(encoding="utf-8"))

    assert rows
    assert rows[0]["uniprot_protein_name"] == "Example kinase"
    assert rows[0]["alphafold_entry_id"] == "AF-P12345-F1"
    assert rows[0]["reactome_pathway_ids"] == "R-HSA-12345"
    assert rows[0]["cath_ids"] == "1.10.510.10"
    assert rows[0]["scop_ids"] == "b.1.1.1"
    assert manifest["annotation_summary"]["uniprot_ready"] == 1
    assert manifest["annotation_summary"]["alphafold_ready"] == 1
    assert manifest["annotation_summary"]["reactome_ready"] == 1
    assert manifest["annotation_summary"]["interpro_ready"] == 1
    assert manifest["annotation_summary"]["cath_ready"] == 1


@patch("pbdata.sources.uniprot.requests.get")
def test_uniprot_adapter_prefers_local_index_when_available(mock_get: MagicMock) -> None:
    layout = build_storage_layout(_tmp_dir("uniprot_local_index"))
    source_path = layout.root / "data_sources" / "uniprot" / "uniprot_sprot.dat.gz"
    source_path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(source_path, "wt", encoding="utf-8") as handle:
        handle.write(
            "ID   TEST_HUMAN Reviewed; 123 AA.\n"
            "AC   P12345;\n"
            "DE   RecName: Full=Indexed protein;\n"
            "GN   Name=KIN1;\n"
            "OS   Homo sapiens.\n"
            "OX   NCBI_TaxID=9606;\n"
            "DR   InterPro; IPR000001; Example.\n"
            "DR   Pfam; PF00001; Example.\n"
            "DR   GO; GO:0004672; Example.\n"
            "KW   Kinase.\n"
            "SQ   SEQUENCE   4 AA;\n"
            "     MAAA\n"
            "//\n"
        )
    index_uniprot_swissprot(layout, source_path=source_path)

    record = UniProtAdapter(storage_root=layout.root).fetch_annotation("P12345")

    assert record.protein_name == "Indexed protein"
    assert record.sequence_length == 4
    assert record.interpro_ids == ["IPR000001"]
    mock_get.assert_not_called()


@patch("pbdata.sources.alphafold.requests.get")
def test_alphafold_adapter_prefers_local_index_when_available(mock_get: MagicMock) -> None:
    layout = build_storage_layout(_tmp_dir("alphafold_local_index"))
    archive_path = layout.root / "data_sources" / "alphafold" / "swissprot_pdb_v6.tar"
    archive_path.parent.mkdir(parents=True, exist_ok=True)
    member_path = layout.root / "AF-P12345-F1-model_v6.pdb.gz"
    with gzip.open(member_path, "wt", encoding="utf-8") as handle:
        handle.write("MODEL")
    with tarfile.open(archive_path, "w") as archive:
        archive.add(member_path, arcname=member_path.name)
    index_alphafold_archive(layout, archive_path=archive_path)

    record = AlphaFoldAdapter(storage_root=layout.root).fetch_prediction("P12345")

    assert record.entry_id == "AF-P12345-F1-model_v6"
    assert record.model_version == "v6"
    mock_get.assert_not_called()


@patch("pbdata.sources.reactome.requests.get")
def test_reactome_adapter_prefers_local_index_when_available(mock_get: MagicMock) -> None:
    layout = build_storage_layout(_tmp_dir("reactome_local_index"))
    reactome_dir = layout.root / "data_sources" / "reactome"
    reactome_dir.mkdir(parents=True, exist_ok=True)
    mapping_path = reactome_dir / "UniProt2Reactome_All_Levels.txt"
    pathways_path = reactome_dir / "ReactomePathways.txt"
    mapping_path.write_text(
        "\n".join(
            [
                "P12345\tR-HSA-12345\thttps://reactome.org/PathwayBrowser/#/R-HSA-12345\tSignal transduction\tTAS\tHomo sapiens",
                "P12345\tR-HSA-67890\thttps://reactome.org/PathwayBrowser/#/R-HSA-67890\tImmune system\tIEA\tHomo sapiens",
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
    index_reactome_pathways(layout, mapping_path=mapping_path, pathways_path=pathways_path)

    record = ReactomeAdapter(storage_root=layout.root).fetch_annotation("P12345")

    assert record.pathway_count == 2
    assert record.pathway_ids == ["R-HSA-12345", "R-HSA-67890"]
    mock_get.assert_not_called()


def test_harvest_unified_metadata_uses_local_reactome_index() -> None:
    layout = build_storage_layout(_tmp_dir("workflow_metadata_reactome_local"))
    _write_extracted_fixture(layout)
    reactome_dir = layout.root / "data_sources" / "reactome"
    reactome_dir.mkdir(parents=True, exist_ok=True)
    mapping_path = reactome_dir / "UniProt2Reactome_All_Levels.txt"
    pathways_path = reactome_dir / "ReactomePathways.txt"
    mapping_path.write_text(
        "P12345\tR-HSA-12345\thttps://reactome.org/PathwayBrowser/#/R-HSA-12345\tSignal transduction\tTAS\tHomo sapiens\n",
        encoding="utf-8",
    )
    pathways_path.write_text(
        "R-HSA-12345\tSignal transduction\tHomo sapiens\n",
        encoding="utf-8",
    )
    index_reactome_pathways(layout, mapping_path=mapping_path, pathways_path=pathways_path)

    artifacts = harvest_unified_metadata(layout, enrich_reactome=True)
    with Path(artifacts["metadata_csv"]).open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))

    assert rows[0]["reactome_pathway_count"] == "1"
    assert rows[0]["reactome_pathway_ids"] == "R-HSA-12345"
    assert rows[0]["reactome_pathway_names"] == "Signal transduction"


@patch("pbdata.sources.pdbe_mappings.requests.get")
def test_cath_adapter_prefers_local_index_when_available(mock_get: MagicMock) -> None:
    layout = build_storage_layout(_tmp_dir("cath_local_index"))
    cath_dir = layout.root / "data_sources" / "cath"
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
    index_cath_domains(
        layout,
        domain_list_path=domain_list_path,
        boundaries_path=boundaries_path,
        names_path=names_path,
    )

    record = CATHAdapter(storage_root=layout.root).fetch_annotation("1ABC")

    assert record.domain_ids == ["1.10.8.10"]
    assert record.chain_to_domain_ids["A"] == ["1.10.8.10"]
    mock_get.assert_not_called()


@patch("pbdata.sources.pdbe_mappings.requests.get")
def test_scop_adapter_prefers_local_index_when_available(mock_get: MagicMock) -> None:
    layout = build_storage_layout(_tmp_dir("scop_local_index"))
    scop_dir = layout.root / "data_sources" / "scope"
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
    index_scop_domains(
        layout,
        classification_path=classification_path,
        descriptions_path=descriptions_path,
    )

    record = SCOPAdapter(storage_root=layout.root).fetch_annotation("1ABC")

    assert record.domain_ids == ["a.1.1.1"]
    assert record.chain_to_domain_ids["A"] == ["a.1.1.1"]
    mock_get.assert_not_called()


def test_harvest_unified_metadata_uses_local_cath_and_scop_indexes() -> None:
    layout = build_storage_layout(_tmp_dir("workflow_metadata_fold_local"))
    _write_extracted_fixture(layout)
    cath_dir = layout.root / "data_sources" / "cath"
    cath_dir.mkdir(parents=True, exist_ok=True)
    scop_dir = layout.root / "data_sources" / "scope"
    scop_dir.mkdir(parents=True, exist_ok=True)
    (cath_dir / "cath-domain-list.txt").write_text(
        "1abcA00     1    10     8    10     1     1     1     1     1    59 1.000\n",
        encoding="utf-8",
    )
    (cath_dir / "cath-domain-boundaries.txt").write_text(
        "1abcA D01 F00  1  A    1 - A  100 -\n",
        encoding="utf-8",
    )
    (cath_dir / "cath-names.txt").write_text(
        "1.10.8.10    1abcA00    :Example cath fold\n",
        encoding="utf-8",
    )
    (scop_dir / "dir.cla.scope.2.08-stable.txt").write_text(
        "d1abca_\t1abc\tA:\ta.1.1.1\t113449\tcl=46456,cf=46457,sf=46458,fa=46459,dm=46460,sp=116748,px=113449\n",
        encoding="utf-8",
    )
    (scop_dir / "dir.des.scope.txt").write_text(
        "46459\tfa\ta.1.1.1\t-\tExample scop fold\n",
        encoding="utf-8",
    )
    index_cath_domains(
        layout,
        domain_list_path=(cath_dir / "cath-domain-list.txt"),
        boundaries_path=(cath_dir / "cath-domain-boundaries.txt"),
        names_path=(cath_dir / "cath-names.txt"),
        force=True,
    )
    index_scop_domains(
        layout,
        classification_path=(scop_dir / "dir.cla.scope.2.08-stable.txt"),
        descriptions_path=(scop_dir / "dir.des.scope.txt"),
        force=True,
    )

    artifacts = harvest_unified_metadata(layout, enrich_cath=True, enrich_scop=True)
    with Path(artifacts["metadata_csv"]).open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))

    assert rows[0]["cath_ids"] == "1.10.8.10"
    assert rows[0]["scop_ids"] == "a.1.1.1"
    assert rows[0]["cath_names"] == "Example cath fold"
    assert rows[0]["scop_names"] == "Example scop fold"


def test_harvest_metadata_cli_with_external_annotations() -> None:
    storage_root = _tmp_dir("workflow_metadata_cli_enriched")
    layout = build_storage_layout(storage_root)
    _write_extracted_fixture(layout)
    runner = CliRunner()

    with (
        patch.object(
            UniProtAdapter,
            "fetch_annotation",
            return_value=UniProtAdapter().normalize_record(
                {
                    "primaryAccession": "P12345",
                    "entryType": "UniProtKB reviewed (Swiss-Prot)",
                    "proteinDescription": {"recommendedName": {"fullName": {"value": "Example kinase"}}},
                    "genes": [{"geneName": {"value": "KIN1"}}],
                    "organism": {"scientificName": "Homo sapiens", "taxonId": 9606},
                    "sequence": {"value": "MPEPTIDE", "length": 8},
                }
            ),
        ),
        patch.object(
            AlphaFoldAdapter,
            "fetch_prediction",
            return_value=AlphaFoldAdapter().normalize_record(
                {
                    "uniprotAccession": "P12345",
                    "entryId": "AF-P12345-F1",
                    "latestVersion": "4",
                    "uniprotStart": 1,
                    "uniprotEnd": 250,
                    "cifUrl": "https://example.org/model.cif",
                    "paeDocUrl": "https://example.org/model-pae.json",
                    "bcifUrl": "https://example.org/model.bcif",
                }
            ),
        ),
        patch.object(
            ReactomeAdapter,
            "fetch_annotation",
            return_value=ReactomeAdapter().normalize_record(
                "P12345",
                [{"stId": "R-HSA-12345", "displayName": "Signal transduction"}],
            ),
        ),
        patch.object(
            InterProAdapter,
            "fetch_annotation",
            return_value=InterProAdapter().normalize_record(
                {"1abc": {"InterPro": {"IPR000001": {"name": "Kinase domain", "mappings": [{"chain_id": "A"}]}}}},
                pdb_id="1ABC",
            ),
        ),
        patch.object(
            PfamAdapter,
            "fetch_annotation",
            return_value=PfamAdapter().normalize_record(
                {"1abc": {"Pfam": {"PF00001": {"name": "Pkinase", "mappings": [{"chain_id": "A"}]}}}},
                pdb_id="1ABC",
            ),
        ),
        patch.object(
            CATHAdapter,
            "fetch_annotation",
            return_value=CATHAdapter().normalize_record(
                {"1abc": {"CATH": {"1.10.510.10": {"name": "Mainly Alpha", "mappings": [{"chain_id": "A"}]}}}},
                pdb_id="1ABC",
            ),
        ),
        patch.object(
            SCOPAdapter,
            "fetch_annotation",
            return_value=SCOPAdapter().normalize_record(
                {"1abc": {"SCOP": {"b.1.1.1": {"name": "Ig-like", "mappings": [{"chain_id": "A"}]}}}},
                pdb_id="1ABC",
            ),
        ),
    ):
        result = runner.invoke(
            app,
            [
                "--storage-root",
                str(storage_root),
                "harvest-metadata",
                "--with-uniprot",
                "--with-alphafold",
                "--with-reactome",
                "--with-interpro",
                "--with-pfam",
                "--with-cath",
                "--with-scop",
            ],
            catch_exceptions=False,
        )

    assert result.exit_code == 0
    assert (layout.workspace_metadata_dir / "protein_metadata.csv").exists()
