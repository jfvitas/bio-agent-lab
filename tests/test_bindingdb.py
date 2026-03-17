import json
import zipfile
from pathlib import Path
from uuid import uuid4
from unittest.mock import Mock, patch

import requests

from pbdata.cli import _fetch_bindingdb_samples_for_pdb
from pbdata.config import AppConfig, SourceConfig, SourcesConfig
from pbdata.sources.bindingdb import BindingDBAdapter, _parse_affinity, _parse_monomer
from pbdata.storage import build_storage_layout

_LOCAL_TMP = Path(__file__).parent / "_tmp"
_LOCAL_TMP.mkdir(exist_ok=True)


def _tmp_dir(name: str) -> Path:
    path = _LOCAL_TMP / f"{uuid4().hex}_{name}"
    path.mkdir(exist_ok=True)
    return path


def test_parse_affinity_preserves_relation() -> None:
    value, unit, standardized, relation = _parse_affinity("<= 5", "uM")
    assert value == 5.0
    assert unit == "uM"
    assert standardized == 5000.0
    assert relation == "<="


def test_parse_monomer_captures_context_fields() -> None:
    rows = _parse_monomer({
        "smiles": "CCO",
        "monomerID": "ATP",
        "affinities": [{
            "affinity": "> 10",
            "affinityUnit": "nM",
            "affinityType": "Kd",
            "target": "Kinase",
            "uniprotID": "P12345",
            "pubmedId": "123456",
            "doi": "10.1000/example",
            "mutationString": "A123V",
            "chainIDs": "A,B",
            "temperature": "25",
            "pH": "7.2",
        }],
    }, "1ABC")

    assert len(rows) == 1
    row = rows[0]
    assert row.chain_ids_receptor == ["A", "B"]
    assert row.mutation_string == "A123V"
    assert row.wildtype_or_mutant == "mutant"
    assert row.temperature_c == 25.0
    assert row.ph == 7.2
    assert row.provenance["standard_relation"] == ">"
    assert row.provenance["pubmed_id"] == "123456"
    assert row.provenance["doi"] == "10.1000/example"
    assert row.provenance["assay_value_log10_convention"] == "log10_nM"
    assert row.provenance["standardized_affinity_unit"] == "nM"


def test_fetch_bindingdb_samples_prefers_local_cache_dir() -> None:
    tmp_root = _tmp_dir("bindingdb_local_cache")
    layout = build_storage_layout(tmp_root / "storage")
    local_dir = tmp_root / "bindingdb_local"
    local_dir.mkdir(parents=True)
    cache_path = local_dir / "1ABC.json"
    cache_path.write_text(json.dumps({
        "pdb_id": "1ABC",
        "monomers": [{
            "monomerID": "ATP",
            "affinities": [{
                "affinity": "5",
                "affinityUnit": "nM",
                "affinityType": "Kd",
            }],
        }],
    }), encoding="utf-8")

    config = AppConfig(
        sources=SourcesConfig(
            bindingdb=SourceConfig(enabled=True, extra={"local_dir": str(local_dir)}),
        ),
    )

    rows = _fetch_bindingdb_samples_for_pdb("1ABC", config, layout=layout)

    assert len(rows) == 1
    assert rows[0].provenance["cache_mode"] == "local_cache"
    assert rows[0].provenance["cache_path"] == str(cache_path)
    state_path = layout.source_state_dir / "bindingdb.json"
    assert state_path.exists()
    state = json.loads(state_path.read_text(encoding="utf-8"))
    assert state["mode"] == "local_cache"
    assert state["record_count"] == 1


def test_fetch_bindingdb_samples_prefers_bulk_index_when_staged_dump_is_present() -> None:
    tmp_root = _tmp_dir("bindingdb_bulk_preferred")
    layout = build_storage_layout(tmp_root / "storage")
    bulk_dir = tmp_root / "data_sources" / "bindingdb"
    bulk_dir.mkdir(parents=True, exist_ok=True)
    dump_zip_path = bulk_dir / "BDB-mySQL_All_202603_dmp.zip"
    with zipfile.ZipFile(dump_zip_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
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

    local_dir = tmp_root / "bindingdb_local"
    local_dir.mkdir(parents=True, exist_ok=True)
    (local_dir / "1ABC.json").write_text(
        json.dumps(
            {
                "pdb_id": "1ABC",
                "monomers": [
                    {
                        "monomerID": "WRONG",
                        "affinities": [
                            {
                                "affinity": "99",
                                "affinityUnit": "nM",
                                "affinityType": "Kd",
                            }
                        ],
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    config = AppConfig(
        sources=SourcesConfig(
            bindingdb=SourceConfig(
                enabled=True,
                extra={
                    "bulk_zip": str(dump_zip_path),
                    "local_dir": str(local_dir),
                },
            ),
        ),
    )
    raw = {
        "rcsb_id": "1ABC",
        "nonpolymer_entities": [
            {"nonpolymer_comp": {"chem_comp": {"id": "ATP"}}},
        ],
        "polymer_entities": [
            {
                "rcsb_polymer_entity_container_identifiers": {
                    "uniprot_ids": ["P12345"],
                    "auth_asym_ids": ["A"],
                }
            }
        ],
    }

    rows = _fetch_bindingdb_samples_for_pdb("1ABC", config, layout=layout, raw=raw)

    assert len(rows) == 1
    assert rows[0].ligand_id == "ATP"
    assert rows[0].chain_ids_receptor == ["A"]
    assert rows[0].provenance["source_mode"] == "bulk_index"
    state = json.loads((layout.source_state_dir / "bindingdb.json").read_text(encoding="utf-8"))
    assert state["mode"] == "bulk_index"


def test_bindingdb_adapter_retries_transient_failure() -> None:
    transient = requests.HTTPError("busy")
    transient.response = Mock(status_code=503)
    response = Mock()
    response.raise_for_status.return_value = None
    response.json.return_value = {"pdb_id": "1ABC", "monomers": []}

    with patch("pbdata.sources.bindingdb.requests.get", side_effect=[transient, response]), patch(
        "pbdata.sources.bindingdb.time.sleep",
        return_value=None,
    ):
        payload = BindingDBAdapter().fetch_metadata("1ABC")

    assert payload["pdb_id"] == "1ABC"


def test_bindingdb_adapter_returns_empty_payload_for_404() -> None:
    response = Mock(status_code=404)

    with patch("pbdata.sources.bindingdb.requests.get", return_value=response), patch(
        "pbdata.sources.bindingdb.time.sleep",
        return_value=None,
    ):
        payload = BindingDBAdapter().fetch_metadata("1ABC")

    assert payload == {"pdb_id": "1ABC", "monomers": []}
