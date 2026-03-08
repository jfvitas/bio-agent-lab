from pathlib import Path
from uuid import uuid4

import pytest

from pbdata.dataset.splits import build_splits, cluster_aware_split
from pbdata.gui import _call_on_tk_thread
from pbdata.sources import rcsb_search

_LOCAL_TMP = Path(__file__).parent / "_tmp"
_LOCAL_TMP.mkdir(exist_ok=True)


def _tmp_dir(name: str) -> Path:
    path = _LOCAL_TMP / f"{uuid4().hex}_{name}"
    path.mkdir(exist_ok=True)
    return path


def test_cluster_aware_split_seed_changes_assignment_for_same_size_clusters() -> None:
    sample_ids = ["S1", "S2", "S3", "S4", "S5", "S6"]
    sequences = [
        "AAAAABBBBB",
        "CCCCCDDDDD",
        "EEEEEFFFFF",
        "GGGGGHHHHH",
        "IIIIIJJJJJ",
        "KKKKKLLLLL",
    ]

    result_a = cluster_aware_split(
        sample_ids,
        sequences,
        train_frac=0.5,
        val_frac=0.25,
        seed=1,
        log_fn=lambda _msg: None,
    )
    result_b = cluster_aware_split(
        sample_ids,
        sequences,
        train_frac=0.5,
        val_frac=0.25,
        seed=2,
        log_fn=lambda _msg: None,
    )

    assert result_a.sizes() == result_b.sizes()
    assert (
        result_a.train != result_b.train
        or result_a.val != result_b.val
        or result_a.test != result_b.test
    )


@pytest.mark.parametrize("train_frac,val_frac", [(-0.1, 0.2), (0.8, 0.3), (0.2, 1.1)])
def test_split_fraction_validation_rejects_invalid_values(
    train_frac: float,
    val_frac: float,
) -> None:
    with pytest.raises(ValueError):
        build_splits(["S1"], train_frac=train_frac, val_frac=val_frac)

    with pytest.raises(ValueError):
        cluster_aware_split(
            ["S1"],
            ["AAAAABBBBB"],
            train_frac=train_frac,
            val_frac=val_frac,
            log_fn=lambda _msg: None,
        )


def test_search_and_download_reports_missing_ids_from_partial_batch() -> None:
    output_dir = _tmp_dir("partial_batch_raw")
    manifest_path = _LOCAL_TMP / f"{uuid4().hex}_partial_manifest.csv"
    logs: list[str] = []

    entry = {
        "rcsb_id": "1ABC",
        "exptl": [{"method": "X-RAY DIFFRACTION"}],
        "rcsb_entry_info": {
            "resolution_combined": [2.1],
            "polymer_entity_count_protein": 1,
            "nonpolymer_entity_count": 0,
            "deposited_atom_count": 4321,
        },
        "rcsb_accession_info": {
            "initial_release_date": "2020-01-01T00:00:00Z",
            "deposit_date": "2019-06-01T00:00:00Z",
        },
        "struct": {"title": "Example protein"},
        "polymer_entities": [],
        "nonpolymer_entities": [],
    }

    original_search_entries = rcsb_search.search_entries
    original_fetch_entries_batch = rcsb_search.fetch_entries_batch
    try:
        rcsb_search.search_entries = lambda _criteria: ["1ABC", "2DEF"]
        rcsb_search.fetch_entries_batch = lambda _ids: [entry]
        downloaded = rcsb_search.search_and_download(
            rcsb_search.SearchCriteria(),
            output_dir,
            log_fn=logs.append,
            manifest_path=manifest_path,
        )
    finally:
        rcsb_search.search_entries = original_search_entries
        rcsb_search.fetch_entries_batch = original_fetch_entries_batch

    assert downloaded == ["1ABC"]
    assert not (output_dir / "2DEF.json").exists()
    assert any("missing IDs" in message and "2DEF" in message for message in logs)


def test_fetch_chemcomp_descriptors_logs_failures(caplog: pytest.LogCaptureFixture) -> None:
    from unittest.mock import patch

    caplog.set_level("WARNING")
    with patch("pbdata.sources.rcsb_search.requests.post", side_effect=RuntimeError("boom")):
        result = rcsb_search.fetch_chemcomp_descriptors(["ATP"])

    assert result == {}
    assert any("Chem-comp descriptor fetch failed" in rec.message for rec in caplog.records)


def test_call_on_tk_thread_returns_result() -> None:
    class RootStub:
        def after(self, _delay: int, fn) -> None:
            fn()

    assert _call_on_tk_thread(RootStub(), lambda: 7) == 7


def test_call_on_tk_thread_reraises_exceptions() -> None:
    class RootStub:
        def after(self, _delay: int, fn) -> None:
            fn()

    with pytest.raises(RuntimeError, match="ui failure"):
        _call_on_tk_thread(RootStub(), lambda: (_ for _ in ()).throw(RuntimeError("ui failure")))
