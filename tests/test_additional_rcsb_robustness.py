"""Additional live-RCSB robustness checks using coarse, high-confidence assertions."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from pbdata.pipeline.extract import extract_rcsb_entry
from pbdata.sources.rcsb import RCSBAdapter


_ADDITIONAL_CASES = [
    {
        "pdb_id": "5XJE",
        "expected_flags": {"glycan_present": True, "heteromeric": True},
    },
    {
        "pdb_id": "6BML",
        "expected_flags": {"membrane_context": True, "covalent_like": True},
    },
    {
        "pdb_id": "4H26",
        "expected_flags": {"peptide_partner": True, "metal_present": True, "heteromeric": True},
    },
]


@pytest.mark.integration
@pytest.mark.parametrize("case", _ADDITIONAL_CASES, ids=[case["pdb_id"] for case in _ADDITIONAL_CASES])
def test_additional_live_entries_have_expected_coarse_signals(case: dict) -> None:
    adapter = RCSBAdapter()
    pdb_id = case["pdb_id"]

    try:
        raw = adapter.fetch_metadata(pdb_id)
    except Exception as exc:
        pytest.skip(f"live RCSB fetch unavailable for {pdb_id}: {exc}")

    with patch("pbdata.pipeline.extract.download_structure_files") as mock_dl:
        mock_dl.return_value = {"parsed_structure_format": "mmCIF"}
        records = extract_rcsb_entry(raw, download_structures=False)

    entry = records["entry"]
    bound_objects = records["bound_objects"]
    interfaces = records["interfaces"]

    if case["expected_flags"].get("glycan_present"):
        assert any(bo.component_type == "glycan" for bo in bound_objects), (
            f"{pdb_id}: expected glycan bound objects"
        )

    if case["expected_flags"].get("membrane_context"):
        assert entry.membrane_vs_soluble == "membrane", (
            f"{pdb_id}: expected membrane classification"
        )

    if case["expected_flags"].get("metal_present"):
        assert any(bo.component_type == "metal" for bo in bound_objects), (
            f"{pdb_id}: expected metal bound object"
        )

    if case["expected_flags"].get("peptide_partner"):
        assert any(bo.component_type == "peptide" for bo in bound_objects), (
            f"{pdb_id}: expected peptide bound object"
        )

    if case["expected_flags"].get("heteromeric"):
        assert entry.homomer_or_heteromer == "heteromer" or any(
            iface.is_hetero for iface in interfaces
        ), f"{pdb_id}: expected heteromeric signal"

    if case["expected_flags"].get("covalent_like"):
        assert entry.covalent_binder_present is True or any(
            bo.is_covalent is True or bo.covalent_warhead_flag for bo in bound_objects
        ), f"{pdb_id}: expected covalent or reactive-binder signal"
