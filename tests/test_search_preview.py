import json

from typer.testing import CliRunner

from pbdata.cli import app
from pbdata.criteria import SearchCriteria, save_criteria
from pbdata.search_preview import build_rcsb_search_preview
from pbdata.storage import build_storage_layout
from pbdata.sources import rcsb_search
from tests.test_feature_execution import _tmp_dir


def test_build_rcsb_search_preview_reports_selection_counts() -> None:
    layout = build_storage_layout(_tmp_dir("search_preview"))
    criteria = SearchCriteria(max_results=2, representative_sampling=False)

    original_search_entries = rcsb_search.search_entries
    original_fetch_entries_batch = rcsb_search.fetch_entries_batch
    try:
        def _fake_search_entries(local_criteria):
            if local_criteria.max_results is None:
                return ["1AAA", "2BBB", "3CCC"]
            return ["1AAA", "2BBB"]

        rcsb_search.search_entries = _fake_search_entries
        rcsb_search.fetch_entries_batch = lambda ids: [
            {
                "rcsb_id": pdb_id,
                "exptl": [{"method": "X-RAY DIFFRACTION"}],
                "rcsb_entry_info": {"resolution_combined": [2.0], "polymer_entity_count_protein": 1, "nonpolymer_entity_count": 1},
                "polymer_entities": [{"rcsb_entity_source_organism": [{"ncbi_taxonomy_id": 9606}]}],
                "nonpolymer_entities": [{"nonpolymer_comp": {"chem_comp": {"id": "ATP"}}}],
            }
            for pdb_id in ids
        ]
        report = build_rcsb_search_preview(layout, criteria)
    finally:
        rcsb_search.search_entries = original_search_entries
        rcsb_search.fetch_entries_batch = original_fetch_entries_batch

    assert report["counts"]["total_match_count"] == 3
    assert report["counts"]["selected_match_count"] == 2
    assert report["selection_mode"] == "hard_limited"


def test_preview_rcsb_search_cli_writes_reports() -> None:
    layout = build_storage_layout(_tmp_dir("search_preview_cli"))
    criteria_path = layout.root / "criteria.yaml"
    save_criteria(SearchCriteria(max_results=2), criteria_path)
    runner = CliRunner()

    original_search_entries = rcsb_search.search_entries
    original_fetch_entries_batch = rcsb_search.fetch_entries_batch
    try:
        rcsb_search.search_entries = lambda _criteria: ["1AAA", "2BBB"]
        rcsb_search.fetch_entries_batch = lambda ids: [
            {
                "rcsb_id": pdb_id,
                "exptl": [{"method": "X-RAY DIFFRACTION"}],
                "rcsb_entry_info": {"resolution_combined": [2.0], "polymer_entity_count_protein": 1, "nonpolymer_entity_count": 1},
                "polymer_entities": [{"rcsb_entity_source_organism": [{"ncbi_taxonomy_id": 9606}]}],
                "nonpolymer_entities": [{"nonpolymer_comp": {"chem_comp": {"id": "ATP"}}}],
            }
            for pdb_id in ids
        ]
        result = runner.invoke(
            app,
            ["--storage-root", str(layout.root), "preview-rcsb-search", "--criteria", str(criteria_path)],
            catch_exceptions=False,
        )
    finally:
        rcsb_search.search_entries = original_search_entries
        rcsb_search.fetch_entries_batch = original_fetch_entries_batch

    assert result.exit_code == 0
    assert "RCSB search preview JSON" in result.output
    assert (layout.reports_dir / "rcsb_search_preview.json").exists()
    saved = json.loads((layout.reports_dir / "rcsb_search_preview.json").read_text(encoding="utf-8"))
    assert saved["counts"]["selected_match_count"] == 2
