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
        direct_pdb_ids=["1ABC", "2DEF"],
        keyword_query="kinase inhibitor",
        organism_name_query="Homo sapiens",
        taxonomy_id=9606,
        membrane_only=True,
        require_multimer=True,
        require_ligand=True,
        require_branched_entities=True,
        min_protein_entities=2,
        min_nonpolymer_entities=1,
        max_nonpolymer_entities=5,
        min_branched_entities=1,
        max_branched_entities=3,
        min_assembly_count=1,
        max_assembly_count=4,
        max_deposited_atom_count=50000,
        min_release_year=2015,
        max_release_year=2020,
    )

    payload = rcsb_search._build_query(criteria)
    query = payload["query"]
    assert query["type"] == "group"

    nodes = query["nodes"]
    assert any(node.get("service") == "full_text" for node in nodes)
    assert any(
        node.get("logical_operator") == "or"
        and any(
            subnode.get("parameters", {}).get("attribute") == "rcsb_entry_container_identifiers.entry_id"
            for subnode in node.get("nodes", [])
        )
        for node in nodes
    )
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
        and node.get("parameters", {}).get("operator") == "greater_or_equal"
        for node in nodes
    )
    assert any(
        node.get("parameters", {}).get("attribute") == "rcsb_entry_info.nonpolymer_entity_count"
        and node.get("parameters", {}).get("operator") == "less_or_equal"
        for node in nodes
    )
    assert any(
        node.get("parameters", {}).get("attribute") == "rcsb_entry_info.branched_entity_count"
        and node.get("parameters", {}).get("operator") == "greater"
        for node in nodes
    )
    assert any(
        node.get("parameters", {}).get("attribute") == "rcsb_entry_info.branched_entity_count"
        and node.get("parameters", {}).get("operator") == "greater_or_equal"
        for node in nodes
    )
    assert any(
        node.get("parameters", {}).get("attribute") == "rcsb_entry_info.branched_entity_count"
        and node.get("parameters", {}).get("operator") == "less_or_equal"
        for node in nodes
    )
    assert any(
        node.get("parameters", {}).get("attribute") == "rcsb_entry_info.assembly_count"
        and node.get("parameters", {}).get("operator") == "greater_or_equal"
        for node in nodes
    )
    assert any(
        node.get("parameters", {}).get("attribute") == "rcsb_entry_info.assembly_count"
        and node.get("parameters", {}).get("operator") == "less_or_equal"
        for node in nodes
    )
    assert any(
        node.get("parameters", {}).get("attribute") == "rcsb_entry_info.deposited_atom_count"
        for node in nodes
    )
    assert any(
        node.get("logical_operator") == "or"
        and any(
            subnode.get("parameters", {}).get("attribute") == "struct_keywords.text"
            for subnode in node.get("nodes", [])
        )
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


def test_search_and_download_replaces_invalid_cached_json() -> None:
    output_dir = _tmp_dir("rcsb_raw_replace")
    (output_dir / "1ABC.json").write_text('{"rcsb_id":"WRONG"}', encoding="utf-8")

    entry = {
        "rcsb_id": "1ABC",
        "rcsb_entry_info": {},
        "polymer_entities": [],
        "nonpolymer_entities": [],
    }

    original_search_entries = rcsb_search.search_entries
    original_fetch_entries_batch = rcsb_search.fetch_entries_batch
    try:
        rcsb_search.search_entries = lambda _criteria: ["1ABC"]
        rcsb_search.fetch_entries_batch = lambda _ids: [entry]
        rcsb_search.search_and_download(
            SearchCriteria(),
            output_dir,
            log_fn=lambda _msg: None,
            manifest_path=_LOCAL_TMP / f"{uuid4().hex}_manifest.csv",
        )
    finally:
        rcsb_search.search_entries = original_search_entries
        rcsb_search.fetch_entries_batch = original_fetch_entries_batch

    body = (output_dir / "1ABC.json").read_text(encoding="utf-8")
    assert '"rcsb_id": "1ABC"' in body


def test_fetch_chemcomp_descriptors_parses_current_schema() -> None:
    class _Response:
        def raise_for_status(self):
            return None

        def json(self):
            return {
                "data": {
                    "chem_comps": [{
                        "rcsb_id": "ATP",
                        "chem_comp": {"formula": "C10 H16 N5 O13 P3", "formula_weight": 507.181},
                        "rcsb_chem_comp_descriptor": {
                            "InChI": "InChI=1S/...",
                            "InChIKey": "AAAA-BBBB",
                        },
                        "pdbx_chem_comp_descriptor": [{
                            "type": "SMILES_CANONICAL",
                            "program": "OpenEye OEToolkits",
                            "descriptor": "C1=NC",
                        }],
                    }]
                }
            }

    original_post = rcsb_search.requests.post
    try:
        rcsb_search.requests.post = lambda *args, **kwargs: _Response()
        descriptors = rcsb_search.fetch_chemcomp_descriptors(["ATP"])
    finally:
        rcsb_search.requests.post = original_post

    assert descriptors["ATP"]["InChIKey"] == "AAAA-BBBB"
    assert descriptors["ATP"]["SMILES_CANONICAL"] == "C1=NC"
    assert descriptors["ATP"]["SMILES"] == "C1=NC"
    assert descriptors["ATP"]["formula_weight"] == "507.181"


def test_search_entries_applies_representative_result_limit() -> None:
    criteria = SearchCriteria(max_results=3, representative_sampling=True)

    class _Response:
        def __init__(self, payload):
            self._payload = payload

        def raise_for_status(self):
            return None

        def json(self):
            return self._payload

    search_payload = {
        "total_count": 6,
        "result_set": [
            {"identifier": "1AAA"},
            {"identifier": "1AAB"},
            {"identifier": "1AAC"},
            {"identifier": "2BBB"},
            {"identifier": "3CCC"},
            {"identifier": "4DDD"},
        ],
    }
    representative_entries = [
        {
            "rcsb_id": "1AAA",
            "exptl": [{"method": "X-RAY DIFFRACTION"}],
            "rcsb_entry_info": {"resolution_combined": [1.8], "polymer_entity_count_protein": 1, "nonpolymer_entity_count": 1},
            "polymer_entities": [{"rcsb_entity_source_organism": [{"ncbi_taxonomy_id": 9606}]}],
            "nonpolymer_entities": [{"nonpolymer_comp": {"chem_comp": {"id": "ATP"}}}],
        },
        {
            "rcsb_id": "1AAB",
            "exptl": [{"method": "X-RAY DIFFRACTION"}],
            "rcsb_entry_info": {"resolution_combined": [1.9], "polymer_entity_count_protein": 1, "nonpolymer_entity_count": 1},
            "polymer_entities": [{"rcsb_entity_source_organism": [{"ncbi_taxonomy_id": 9606}]}],
            "nonpolymer_entities": [{"nonpolymer_comp": {"chem_comp": {"id": "ATP"}}}],
        },
        {
            "rcsb_id": "1AAC",
            "exptl": [{"method": "X-RAY DIFFRACTION"}],
            "rcsb_entry_info": {"resolution_combined": [2.0], "polymer_entity_count_protein": 1, "nonpolymer_entity_count": 1},
            "polymer_entities": [{"rcsb_entity_source_organism": [{"ncbi_taxonomy_id": 9606}]}],
            "nonpolymer_entities": [{"nonpolymer_comp": {"chem_comp": {"id": "ATP"}}}],
        },
        {
            "rcsb_id": "2BBB",
            "exptl": [{"method": "ELECTRON MICROSCOPY"}],
            "rcsb_entry_info": {"resolution_combined": [3.5], "polymer_entity_count_protein": 2, "nonpolymer_entity_count": 0},
            "polymer_entities": [{"rcsb_entity_source_organism": [{"ncbi_taxonomy_id": 10090}]}],
            "nonpolymer_entities": [],
        },
        {
            "rcsb_id": "3CCC",
            "exptl": [{"method": "SOLUTION NMR"}],
            "rcsb_entry_info": {"resolution_combined": [], "polymer_entity_count_protein": 2, "nonpolymer_entity_count": 0},
            "polymer_entities": [{"rcsb_entity_source_organism": [{"ncbi_taxonomy_id": 7227}]}],
            "nonpolymer_entities": [],
        },
        {
            "rcsb_id": "4DDD",
            "exptl": [{"method": "X-RAY DIFFRACTION"}],
            "rcsb_entry_info": {"resolution_combined": [2.6], "polymer_entity_count_protein": 1, "nonpolymer_entity_count": 1},
            "polymer_entities": [{"rcsb_entity_source_organism": [{"ncbi_taxonomy_id": 4932}]}],
            "nonpolymer_entities": [{"nonpolymer_comp": {"chem_comp": {"id": "HEM"}}}],
        },
    ]

    original_post = rcsb_search.requests.post
    original_fetch_entries_batch = rcsb_search.fetch_entries_batch
    try:
        rcsb_search.requests.post = lambda *args, **kwargs: _Response(search_payload)
        rcsb_search.fetch_entries_batch = lambda ids: [entry for entry in representative_entries if entry["rcsb_id"] in ids]
        ids = rcsb_search.search_entries(criteria)
    finally:
        rcsb_search.requests.post = original_post
        rcsb_search.fetch_entries_batch = original_fetch_entries_batch

    assert ids == ["1AAA", "2BBB", "3CCC"]


def test_search_entries_hard_limit_preserves_order() -> None:
    criteria = SearchCriteria(max_results=2, representative_sampling=False)

    class _Response:
        def raise_for_status(self):
            return None

        def json(self):
            return {
                "total_count": 4,
                "result_set": [
                    {"identifier": "1AAA"},
                    {"identifier": "1AAB"},
                    {"identifier": "2BBB"},
                    {"identifier": "3CCC"},
                ],
            }

    original_post = rcsb_search.requests.post
    try:
        rcsb_search.requests.post = lambda *args, **kwargs: _Response()
        ids = rcsb_search.search_entries(criteria)
    finally:
        rcsb_search.requests.post = original_post

    assert ids == ["1AAA", "1AAB"]
