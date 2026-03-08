from pathlib import Path
from uuid import uuid4

from pbdata.criteria import SearchCriteria
from pbdata.sources import rcsb_search

_LOCAL_TMP = Path(__file__).parent / "_tmp"
_LOCAL_TMP.mkdir(exist_ok=True)


def _tmp_dir(name: str) -> Path:
    path = _LOCAL_TMP / f"{uuid4().hex}_{name}"
    path.mkdir(exist_ok=True)
    return path


def test_build_query_includes_high_level_filters() -> None:
    criteria = SearchCriteria(
        keyword_query="kinase inhibitor",
        organism_name_query="Homo sapiens",
        taxonomy_id=9606,
        require_ligand=True,
        min_protein_entities=2,
        max_deposited_atom_count=50000,
        min_release_year=2015,
        max_release_year=2020,
    )

    payload = rcsb_search._build_query(criteria)
    query = payload["query"]
    assert query["type"] == "group"

    nodes = query["nodes"]
    assert any(node["service"] == "full_text" for node in nodes)
    assert any(
        node.get("parameters", {}).get("attribute") == "rcsb_entity_source_organism.ncbi_scientific_name"
        for node in nodes
    )
    assert any(
        node.get("parameters", {}).get("attribute") == "rcsb_entity_source_organism.ncbi_taxonomy_id"
        for node in nodes
    )
    assert any(
        node.get("parameters", {}).get("attribute") == "rcsb_entry_info.nonpolymer_entity_count"
        for node in nodes
    )
    assert any(
        node.get("parameters", {}).get("attribute") == "rcsb_entry_info.deposited_atom_count"
        for node in nodes
    )
    assert any(
        node.get("parameters", {}).get("attribute") == "rcsb_accession_info.initial_release_date"
        and node.get("parameters", {}).get("operator") == "less_or_equal"
        for node in nodes
    )


def test_search_and_download_updates_manifest() -> None:
    output_dir = _tmp_dir("rcsb_raw")
    manifest_path = _LOCAL_TMP / f"{uuid4().hex}_manifest.csv"

    entry = {
        "rcsb_id": "1ABC",
        "exptl": [{"method": "X-RAY DIFFRACTION"}],
        "rcsb_entry_info": {
            "resolution_combined": [2.1],
            "polymer_entity_count_protein": 1,
            "nonpolymer_entity_count": 1,
            "deposited_atom_count": 4321,
        },
        "rcsb_accession_info": {
            "initial_release_date": "2020-01-01T00:00:00Z",
            "deposit_date": "2019-06-01T00:00:00Z",
        },
        "struct": {"title": "Example complex"},
        "polymer_entities": [
            {"rcsb_entity_source_organism": [{"ncbi_taxonomy_id": 9606}]}
        ],
        "nonpolymer_entities": [
            {"nonpolymer_comp": {"chem_comp": {"id": "ATP", "name": "ADENOSINE-5'-TRIPHOSPHATE"}}}
        ],
    }

    original_search_entries = rcsb_search.search_entries
    original_fetch_entries_batch = rcsb_search.fetch_entries_batch
    try:
        rcsb_search.search_entries = lambda _criteria: ["1ABC"]
        rcsb_search.fetch_entries_batch = lambda _ids: [entry]
        downloaded = rcsb_search.search_and_download(
            SearchCriteria(),
            output_dir,
            log_fn=lambda _msg: None,
            manifest_path=manifest_path,
        )
    finally:
        rcsb_search.search_entries = original_search_entries
        rcsb_search.fetch_entries_batch = original_fetch_entries_batch

    assert downloaded == ["1ABC"]
    raw_path = output_dir / "1ABC.json"
    assert raw_path.exists()
    manifest_text = manifest_path.read_text(encoding="utf-8")
    assert "RCSB" in manifest_text
    assert "1ABC" in manifest_text
    assert "Example complex" in manifest_text
