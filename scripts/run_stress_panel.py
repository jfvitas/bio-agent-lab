"""Run the adversarial structural stress panel against live RCSB data."""

from __future__ import annotations

import json
from pathlib import Path
from urllib.error import URLError

from pbdata.quality.stress_panel import (
    compare_expected_outcomes,
    load_stress_panel,
    summarize_case_outcomes,
)
from pbdata.sources.rcsb import RCSBAdapter
from pbdata.sources.rcsb_classify import classify_entry

_PANEL_PATH = Path("stress_test_panel.yaml")
_OUT_PATH = Path("data/reports/stress_panel_results.json")


def _is_connection_error(exc: Exception) -> bool:
    message = str(exc).lower()
    return isinstance(exc, URLError) or any(
        token in message
        for token in (
            "connectionerror",
            "winerror 10013",
            "failed to establish",
            "max retries exceeded",
        )
    )


def main() -> None:
    cases = load_stress_panel(_PANEL_PATH)
    adapter = RCSBAdapter()
    results: list[dict] = []

    for case in cases:
        pdb_id = case["pdb_id"]
        expected = case.get("expected_outcomes", {})
        try:
            raw = adapter.fetch_metadata(pdb_id)
            classified = classify_entry(raw)
            record = adapter.normalize_record(raw)
            actual = summarize_case_outcomes(raw, classified, record)
            mismatches = compare_expected_outcomes(expected, actual)
            results.append({
                "pdb_id": pdb_id,
                "label": case.get("label", pdb_id),
                "status": "pass" if not mismatches else "mismatch",
                "actual_outcomes": actual,
                "expected_outcomes": expected,
                "mismatches": mismatches,
            })
        except Exception as exc:
            results.append({
                "pdb_id": pdb_id,
                "label": case.get("label", pdb_id),
                "status": "blocked" if _is_connection_error(exc) else "error",
                "error": f"{type(exc).__name__}: {exc}",
            })

    _OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    _OUT_PATH.write_text(json.dumps({"results": results}, indent=2), encoding="utf-8")
    print(f"Wrote stress-panel results to {_OUT_PATH}")


if __name__ == "__main__":
    main()
