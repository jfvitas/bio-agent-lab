from unittest.mock import Mock, patch

from pbdata.cli import _fetch_chembl_samples_for_raw
from pbdata.config import AppConfig, SourceConfig, SourcesConfig
from pbdata.pipeline.assay_merge import pair_identity_key
from pbdata.sources.chembl import ChEMBLAdapter


def _response(payload):
    resp = Mock()
    resp.json.return_value = payload
    resp.raise_for_status.return_value = None
    return resp


def test_chembl_adapter_fetches_exact_identifier_match() -> None:
    with patch("pbdata.sources.chembl.requests.get", side_effect=[
        _response({"targets": [{"target_chembl_id": "CHEMBL_TGT"}]}),
        _response({"molecules": [{"molecule_chembl_id": "CHEMBL_MOL"}]}),
        _response({"activities": [{
            "activity_chembl_id": "CHEMBL_ACT_1",
            "assay_chembl_id": "CHEMBL_ASSAY_1",
            "document_chembl_id": "CHEMBL_DOC_1",
            "standard_type": "Kd",
            "standard_value": "5",
            "standard_units": "nM",
            "standard_relation": "=",
            "target_pref_name": "Example kinase",
            "assay_description": "wild type enzyme",
        }]}),
    ]), patch("pbdata.sources.chembl.time.sleep", return_value=None):
        samples = ChEMBLAdapter().fetch_by_uniprot_and_inchikey("P12345", "AAAA-BBBB")

    assert len(samples) == 1
    sample = samples[0]
    assert sample.source_database == "ChEMBL"
    assert sample.uniprot_ids == ["P12345"]
    assert sample.ligand_inchi_key == "AAAA-BBBB"
    assert sample.assay_type == "Kd"
    assert sample.assay_value_standardized == 5.0
    assert sample.wildtype_or_mutant == "wildtype"


def test_chembl_unknown_mutation_uses_safe_grouping_override() -> None:
    with patch("pbdata.sources.chembl.requests.get", side_effect=[
        _response({"targets": [{"target_chembl_id": "CHEMBL_TGT"}]}),
        _response({"molecules": [{"molecule_chembl_id": "CHEMBL_MOL"}]}),
        _response({"activities": [{
            "activity_chembl_id": "CHEMBL_ACT_2",
            "assay_chembl_id": "CHEMBL_ASSAY_2",
            "standard_type": "Ki",
            "standard_value": "15",
            "standard_units": "nM",
            "target_pref_name": "Example kinase construct",
            "assay_description": "biochemical inhibition assay",
        }]}),
    ]), patch("pbdata.sources.chembl.time.sleep", return_value=None):
        sample = ChEMBLAdapter().fetch_by_uniprot_and_inchikey("P12345", "AAAA-BBBB")[0]

    key = pair_identity_key(sample)
    assert "mutation_unknown" in key
    assert "AAAA-BBBB" in key


def test_cli_chembl_enrichment_attaches_pdb_and_receptor_chains() -> None:
    raw = {
        "rcsb_id": "1ABC",
        "polymer_entities": [{
            "rcsb_polymer_entity_container_identifiers": {
                "uniprot_ids": ["P12345"],
                "auth_asym_ids": ["A"],
            }
        }],
        "nonpolymer_entities": [{
            "nonpolymer_comp": {"chem_comp": {"id": "ATP"}},
        }],
    }
    chem_descriptors = {"ATP": {"InChIKey": "AAAA-BBBB"}}
    config = AppConfig(
        sources=SourcesConfig(
            chembl=SourceConfig(enabled=True),
        )
    )

    sample = ChEMBLAdapter().normalize_record({
        "activity": {
            "activity_chembl_id": "CHEMBL_ACT_3",
            "assay_chembl_id": "CHEMBL_ASSAY_3",
            "standard_type": "Kd",
            "standard_value": "5",
            "standard_units": "nM",
            "target_pref_name": "Example kinase",
            "assay_description": "wild type enzyme",
        },
        "accession": "P12345",
        "inchikey": "AAAA-BBBB",
        "target_chembl_id": "CHEMBL_TGT",
        "molecule_chembl_id": "CHEMBL_MOL",
    })

    with patch.object(
        ChEMBLAdapter,
        "fetch_by_uniprot_and_inchikey",
        return_value=[sample],
    ):
        enriched = _fetch_chembl_samples_for_raw(raw, chem_descriptors, config)

    assert len(enriched) == 1
    assert enriched[0].pdb_id == "1ABC"
    assert enriched[0].chain_ids_receptor == ["A"]
    key = pair_identity_key(enriched[0])
    assert key == "protein_ligand|1ABC|A|AAAA-BBBB|wildtype"
