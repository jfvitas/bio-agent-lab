"""Panel B structural-complexity checks.

These tests validate that the parser/classifier preserves the core biological
signals encoded in `stress_test_panel_B.yaml`. The panel file is immutable.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from pbdata.quality.audit import compute_flags
from pbdata.sources.rcsb import RCSBAdapter
from pbdata.sources.rcsb_classify import classify_entry

_REPO_ROOT = Path(__file__).parent.parent
_PANEL_B_PATH = _REPO_ROOT / "stress_test_panel_B.yaml"


def _load_panel_b() -> list[dict]:
    if not _PANEL_B_PATH.exists():
        return []
    with _PANEL_B_PATH.open(encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}
    return data.get("entries") or []


@pytest.mark.integration
@pytest.mark.parametrize(
    "case",
    _load_panel_b(),
    ids=[case.get("pdb_id", "?") for case in _load_panel_b()],
)
def test_panel_b_expected_flags(case: dict) -> None:
    pdb_id = case["pdb_id"]
    expected = case.get("expected_outcomes", {})

    adapter = RCSBAdapter()
    try:
        raw = adapter.fetch_metadata(pdb_id)
    except Exception as exc:
        pytest.skip(f"live RCSB fetch unavailable for {pdb_id}: {exc}")

    classified = classify_entry(raw)
    record = adapter.normalize_record(raw)
    flags = set(compute_flags(record))
    bound_objects = classified.get("bound_objects", [])

    for flag in expected.get("expected_flags", []):
        assert flag in flags, f"{pdb_id}: expected flag '{flag}' missing from {sorted(flags)}"

    if expected.get("min_polymer_chain_count") is not None:
        assert (record.polymer_entity_count or 0) >= int(expected["min_polymer_chain_count"])

    if expected.get("expected_multiple_bound_objects") is True:
        non_artifact = [obj for obj in bound_objects if getattr(obj, "role", None) != "artifact"]
        assert len(non_artifact) > 1, f"{pdb_id}: expected multiple bound objects"
