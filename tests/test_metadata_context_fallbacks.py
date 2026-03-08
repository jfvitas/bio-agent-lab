"""Fallback inference tests for entries with incomplete GraphQL metadata."""

from __future__ import annotations

import json
from pathlib import Path

from pbdata.quality.audit import compute_flags
from pbdata.sources.rcsb import RCSBAdapter
from pbdata.sources.rcsb_classify import classify_entry

_RAW_DIR = Path(__file__).parent.parent / "data" / "raw" / "rcsb"


def _load_raw(pdb_id: str) -> dict:
    return json.loads((_RAW_DIR / f"{pdb_id}.json").read_text(encoding="utf-8"))


def test_glycan_context_fallback_marks_5xje() -> None:
    raw = _load_raw("5XJE")
    classified = classify_entry(raw)
    record = RCSBAdapter().normalize_record(raw)

    assert any(obj.binder_type == "glycan" for obj in classified["bound_objects"])
    assert "glycan_present" in compute_flags(record)


def test_metal_context_fallback_marks_4h26() -> None:
    raw = _load_raw("4H26")
    classified = classify_entry(raw)
    record = RCSBAdapter().normalize_record(raw)

    assert any(obj.binder_type == "metal_ion" and obj.comp_id == "NI" for obj in classified["bound_objects"])
    flags = compute_flags(record)
    assert "metal_present" in flags
    assert "metal_mediated_binding_possible" in flags
