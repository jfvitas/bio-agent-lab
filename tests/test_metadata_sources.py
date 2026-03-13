import csv
import json
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
