import json
from pathlib import Path
from unittest.mock import patch

from pbdata.pipeline.canonical_workflows import (
    run_normalize_rcsb,
    run_processed_report,
    run_rcsb_ingest,
)
from pbdata.schemas.canonical_sample import CanonicalBindingSample
from pbdata.storage import build_storage_layout
from tests.test_feature_execution import _tmp_dir


def _sample_record(**updates: object) -> CanonicalBindingSample:
    payload = {
        "sample_id": "x1",
        "task_type": "protein_ligand",
        "source_database": "RCSB",
        "source_record_id": "1ABC",
        "pdb_id": "1ABC",
        "experimental_method": "X-RAY DIFFRACTION",
        "structure_resolution": 2.0,
        "provenance": {"ingested_at": "2026-01-01T00:00:00+00:00"},
        "quality_flags": [],
        "quality_score": 0.8,
    }
    payload.update(updates)
    return CanonicalBindingSample(**payload)


def test_run_rcsb_ingest_dry_run_uses_search_count() -> None:
    layout = build_storage_layout(_tmp_dir("canonical_workflow_rcsb_ingest"))
    with patch("pbdata.sources.rcsb_search.count_entries", return_value=42):
        result = run_rcsb_ingest(
            layout=layout,
            criteria_path=Path("configs/criteria.yaml"),
            dry_run=True,
        )

    assert result.match_count == 42
    assert result.dry_run is True


def test_run_normalize_rcsb_returns_none_when_no_raw_files() -> None:
    layout = build_storage_layout(_tmp_dir("canonical_workflow_empty_normalize"))
    assert run_normalize_rcsb(layout=layout, workers=1) is None


def test_run_processed_report_writes_summary_and_returns_counts() -> None:
    layout = build_storage_layout(_tmp_dir("canonical_workflow_report"))
    layout.processed_rcsb_dir.mkdir(parents=True, exist_ok=True)
    record = _sample_record()
    (layout.processed_rcsb_dir / "1ABC.json").write_text(
        record.model_dump_json(indent=2),
        encoding="utf-8",
    )

    with patch("pbdata.pipeline.canonical_workflows.refresh_master_exports", return_value={"master_csv": "master.csv"}):
        result = run_processed_report(layout=layout)

    assert result is not None
    assert result.total_records == 1
    assert result.report_path.exists()
    written = json.loads(result.report_path.read_text(encoding="utf-8"))
    assert written["total_records"] == 1
    assert result.export_status["master_csv"] == "master.csv"
