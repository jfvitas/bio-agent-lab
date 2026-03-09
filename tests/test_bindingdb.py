import json
from pathlib import Path
from uuid import uuid4

from pbdata.cli import _fetch_bindingdb_samples_for_pdb
from pbdata.config import AppConfig, SourceConfig, SourcesConfig
from pbdata.sources.bindingdb import _parse_affinity, _parse_monomer
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
