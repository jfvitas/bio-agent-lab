"""Reusable orchestration for canonical ingestion, normalization, audit, and reporting."""

from __future__ import annotations

import json
import logging
import statistics
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from pbdata.catalog import summarize_bulk_file, update_download_manifest
from pbdata.criteria import load_criteria
from pbdata.master_export import refresh_master_exports
from pbdata.quality.audit import audit_record
from pbdata.schemas.canonical_sample import CanonicalBindingSample
from pbdata.source_state import write_source_state
from pbdata.stage_state import write_stage_state
from pbdata.sources.rcsb import RCSBAdapter
from pbdata.sources.skempi import _SKEMPI_URL
from pbdata.sources import rcsb_search
from pbdata.storage import (
    StorageLayout,
    reuse_existing_file,
    validate_skempi_csv,
)

logger = logging.getLogger(__name__)

LogFn = Callable[[str], None]


def _count_delimited_rows(path: Path, delimiter: str = ",") -> int | None:
    try:
        with path.open(encoding="utf-8", newline="") as handle:
            lines = [line for line in handle.read().splitlines() if line.strip()]
        return max(len(lines) - 1, 0)
    except OSError:
        return None


def _coerce_workers(workers: int) -> int:
    if workers <= 0:
        import os

        return max(os.cpu_count() or 1, 1)
    return workers


def _is_up_to_date(source_path: Path, output_path: Path) -> bool:
    try:
        return output_path.stat().st_mtime >= source_path.stat().st_mtime
    except OSError:
        return False


def _validate_processed_record(path: Path) -> bool:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
        CanonicalBindingSample.model_validate(raw)
        return True
    except Exception:
        return False


@dataclass(frozen=True)
class RCSBIngestResult:
    criteria_path: Path
    match_count: int
    dry_run: bool
    output_dir: Path | None = None
    catalog_path: Path | None = None


@dataclass(frozen=True)
class SKEMPIIngestResult:
    status: str
    csv_path: Path
    catalog_path: Path | None
    row_count: int | None
    dry_run: bool


@dataclass(frozen=True)
class NormalizeResult:
    raw_dir: Path
    output_dir: Path
    state_path: Path
    inputs: int
    normalized: int
    cached: int
    failed: int
    chem_descriptor_count: int


@dataclass(frozen=True)
class AuditResult:
    processed_dir: Path
    summary_path: Path
    state_path: Path
    audited: int
    failed: int
    mean_quality_score: float
    top_flags: dict[str, int]


@dataclass(frozen=True)
class ProcessedReportResult:
    processed_dir: Path
    report_path: Path
    total_records: int
    parse_failures: int
    task_type_counts: dict[str, int]
    top_methods: dict[str, int]
    mean_quality_score: float | None
    export_status: dict[str, Any]


def run_rcsb_ingest(
    *,
    layout: StorageLayout,
    criteria_path: Path,
    dry_run: bool,
    output_dir: Path | None = None,
    log_fn: LogFn | None = None,
) -> RCSBIngestResult:
    sc = load_criteria(criteria_path)
    logger.info("Querying RCSB Search API...")
    count = rcsb_search.count_entries(sc)
    if dry_run:
        return RCSBIngestResult(criteria_path=criteria_path, match_count=count, dry_run=True)

    resolved_output = output_dir if output_dir is not None else layout.raw_rcsb_dir
    rcsb_search.search_and_download(sc, resolved_output, log_fn=log_fn, manifest_path=layout.catalog_path)
    return RCSBIngestResult(
        criteria_path=criteria_path,
        match_count=count,
        dry_run=False,
        output_dir=resolved_output,
        catalog_path=layout.catalog_path,
    )


def run_skempi_ingest(
    *,
    layout: StorageLayout,
    dry_run: bool,
    output_dir: Path | None = None,
) -> SKEMPIIngestResult:
    import requests

    out_dir = output_dir if output_dir is not None else layout.raw_skempi_dir
    csv_path = out_dir / "skempi_v2.csv"
    downloaded_at = datetime.now(timezone.utc).isoformat()

    if reuse_existing_file(csv_path, validator=validate_skempi_csv):
        row_count = _count_delimited_rows(csv_path, delimiter=";")
        update_download_manifest([
            summarize_bulk_file(
                source_database="SKEMPI",
                source_record_id="SKEMPI_V2",
                raw_file_path=csv_path,
                raw_format="csv",
                downloaded_at=downloaded_at,
                title="SKEMPI v2 mutation ddG dataset",
                task_hint="mutation_ddg",
                notes=f"rows={row_count}" if row_count is not None else "",
                status="cached",
            )
        ], layout.catalog_path)
        return SKEMPIIngestResult(
            status="cached",
            csv_path=csv_path,
            catalog_path=layout.catalog_path,
            row_count=row_count,
            dry_run=False,
        )

    if dry_run:
        return SKEMPIIngestResult(
            status="dry_run",
            csv_path=csv_path,
            catalog_path=None,
            row_count=None,
            dry_run=True,
        )

    out_dir.mkdir(parents=True, exist_ok=True)
    resp = requests.get(_SKEMPI_URL, timeout=60)
    resp.raise_for_status()
    csv_path.write_text(resp.text, encoding="utf-8")
    if not validate_skempi_csv(csv_path):
        csv_path.unlink(missing_ok=True)
        raise RuntimeError("Downloaded SKEMPI CSV failed validation and was removed.")
    row_count = _count_delimited_rows(csv_path, delimiter=";")
    update_download_manifest([
        summarize_bulk_file(
            source_database="SKEMPI",
            source_record_id="SKEMPI_V2",
            raw_file_path=csv_path,
            raw_format="csv",
            downloaded_at=downloaded_at,
            title="SKEMPI v2 mutation ddG dataset",
            task_hint="mutation_ddg",
            notes=f"rows={row_count}" if row_count is not None else "",
        )
    ], layout.catalog_path)
    return SKEMPIIngestResult(
        status="downloaded",
        csv_path=csv_path,
        catalog_path=layout.catalog_path,
        row_count=row_count,
        dry_run=False,
    )


def run_normalize_rcsb(
    *,
    layout: StorageLayout,
    workers: int,
    progress_fn: LogFn | None = None,
) -> NormalizeResult | None:
    raw_dir = layout.raw_rcsb_dir
    out_dir = layout.processed_rcsb_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    files = sorted(raw_dir.glob("*.json"))
    if not files:
        return None

    if progress_fn:
        progress_fn(f"Scanning {len(files):,} RCSB records for ligand IDs...")

    comp_ids: list[str] = []
    raw_data: list[tuple[Path, dict[str, Any]]] = []
    for file_path in files:
        try:
            raw = json.loads(file_path.read_text())
            raw_data.append((file_path, raw))
            for ent in raw.get("nonpolymer_entities") or []:
                cid = (((ent.get("nonpolymer_comp") or {}).get("chem_comp") or {}).get("id", ""))
                if cid:
                    comp_ids.append(cid)
        except Exception as exc:
            logger.warning("Failed to read %s: %s", file_path.name, exc)

    chem_descriptors: dict[str, dict[str, str]] = {}
    if comp_ids:
        unique = list(dict.fromkeys(comp_ids))
        if progress_fn:
            progress_fn(f"Fetching chem-comp descriptors for {len(unique):,} unique ligands...")
        try:
            chem_descriptors = rcsb_search.fetch_chemcomp_descriptors(unique)
            if progress_fn:
                progress_fn(f"  Got descriptors for {len(chem_descriptors):,} ligands.")
        except Exception as exc:
            logger.warning("Chem-comp fetch failed (SMILES will be absent): %s", exc)

    if progress_fn:
        progress_fn(f"Normalizing {len(raw_data):,} RCSB records...")

    adapter = RCSBAdapter()
    ok = cached = failed = 0
    worker_count = _coerce_workers(workers)

    def _normalize_one(item: tuple[Path, dict[str, Any]]) -> tuple[str, str]:
        path, raw = item
        out_path = out_dir / path.name
        if reuse_existing_file(out_path, validator=_validate_processed_record) and _is_up_to_date(path, out_path):
            return path.name, "cached"
        out_path.unlink(missing_ok=True)
        record = adapter.normalize_record(raw, chem_descriptors=chem_descriptors)
        out_path.write_text(record.model_dump_json(indent=2))
        return path.name, "ok"

    if worker_count == 1:
        for item in raw_data:
            try:
                _, status = _normalize_one(item)
                if status == "cached":
                    cached += 1
                else:
                    ok += 1
            except Exception as exc:
                logger.warning("Failed to normalize %s: %s", item[0].name, exc)
                failed += 1
    else:
        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            futures = {executor.submit(_normalize_one, item): item[0].name for item in raw_data}
            for future in as_completed(futures):
                name = futures[future]
                try:
                    _, status = future.result()
                    if status == "cached":
                        cached += 1
                    else:
                        ok += 1
                except Exception as exc:
                    logger.warning("Failed to normalize %s: %s", name, exc)
                    failed += 1

    state_path = write_stage_state(
        layout,
        stage="normalize",
        status="completed" if failed == 0 else "completed_with_failures",
        input_dir=raw_dir,
        output_dir=out_dir,
        workers=worker_count,
        counts={
            "inputs": len(raw_data),
            "normalized": ok,
            "cached": cached,
            "failed": failed,
        },
        notes="Valid cached canonical records were reused when newer than their raw JSON source.",
    )

    return NormalizeResult(
        raw_dir=raw_dir,
        output_dir=out_dir,
        state_path=state_path,
        inputs=len(raw_data),
        normalized=ok,
        cached=cached,
        failed=failed,
        chem_descriptor_count=len(chem_descriptors),
    )


def run_audit_processed_records(
    *,
    layout: StorageLayout,
    workers: int,
    progress_fn: LogFn | None = None,
) -> AuditResult | None:
    processed_dir = layout.processed_rcsb_dir
    files = sorted(processed_dir.glob("*.json")) if processed_dir.exists() else []
    if not files:
        return None

    if progress_fn:
        progress_fn(f"Auditing {len(files):,} records...")
    layout.audit_dir.mkdir(parents=True, exist_ok=True)

    flag_counter: Counter[str] = Counter()
    scores: list[float] = []
    ok = failed = 0
    worker_count = _coerce_workers(workers)

    def _audit_one(path: Path) -> CanonicalBindingSample:
        raw = json.loads(path.read_text())
        record = CanonicalBindingSample.model_validate(raw)
        audited = audit_record(record)
        processed_dir.mkdir(parents=True, exist_ok=True)
        (processed_dir / path.name).write_text(audited.model_dump_json(indent=2))
        return audited

    if worker_count == 1:
        for file_path in files:
            try:
                audited = _audit_one(file_path)
                flag_counter.update(audited.quality_flags)
                scores.append(audited.quality_score)
                ok += 1
            except Exception as exc:
                logger.warning("Failed to audit %s: %s", file_path.name, exc)
                failed += 1
    else:
        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            futures = {executor.submit(_audit_one, file_path): file_path.name for file_path in files}
            for future in as_completed(futures):
                name = futures[future]
                try:
                    audited = future.result()
                    flag_counter.update(audited.quality_flags)
                    scores.append(audited.quality_score)
                    ok += 1
                except Exception as exc:
                    logger.warning("Failed to audit %s: %s", name, exc)
                    failed += 1

    summary = {
        "total": ok + failed,
        "audited": ok,
        "failed": failed,
        "quality_score": {
            "mean": round(statistics.mean(scores), 4) if scores else 0,
            "median": round(statistics.median(scores), 4) if scores else 0,
            "min": round(min(scores), 4) if scores else 0,
            "max": round(max(scores), 4) if scores else 0,
        },
        "flag_counts": dict(flag_counter.most_common()),
    }
    summary_path = layout.audit_dir / "audit_summary.json"
    summary_path.write_text(json.dumps(summary, indent=2))
    state_path = write_stage_state(
        layout,
        stage="audit",
        status="completed" if failed == 0 else "completed_with_failures",
        input_dir=processed_dir,
        output_dir=layout.audit_dir,
        workers=worker_count,
        counts={
            "inputs": len(files),
            "audited": ok,
            "failed": failed,
        },
        notes="Audit rewrites processed records in place and stores aggregate summaries separately.",
    )

    return AuditResult(
        processed_dir=processed_dir,
        summary_path=summary_path,
        state_path=state_path,
        audited=ok,
        failed=failed,
        mean_quality_score=float(summary["quality_score"]["mean"]),
        top_flags=dict(flag_counter.most_common(5)),
    )


def run_processed_report(
    *,
    layout: StorageLayout,
    progress_fn: LogFn | None = None,
) -> ProcessedReportResult | None:
    processed_dir = layout.processed_rcsb_dir
    files = sorted(processed_dir.glob("*.json")) if processed_dir.exists() else []
    if not files:
        return None

    if progress_fn:
        progress_fn(f"Generating report for {len(files):,} records...")

    task_counts: Counter[str] = Counter()
    method_counts: Counter[str] = Counter()
    resolutions: list[float] = []
    scores: list[float] = []
    field_present: Counter[str] = Counter()
    optional_fields = [
        "sequence_receptor", "sequence_partner", "chain_ids_receptor",
        "uniprot_ids", "taxonomy_ids", "ligand_id", "ligand_smiles",
        "experimental_method", "structure_resolution",
    ]
    failed = 0

    for file_path in files:
        try:
            raw = json.loads(file_path.read_text())
            rec = CanonicalBindingSample.model_validate(raw)
            task_counts[rec.task_type] += 1
            if rec.experimental_method:
                method_counts[rec.experimental_method] += 1
            if rec.structure_resolution is not None:
                resolutions.append(rec.structure_resolution)
            scores.append(rec.quality_score)
            for field in optional_fields:
                val = getattr(rec, field, None)
                if val is not None and val != [] and val != "":
                    field_present[field] += 1
        except Exception as exc:
            logger.warning("Skipping %s: %s", file_path.name, exc)
            failed += 1

    total = len(files) - failed

    def _pct(n: int) -> float:
        return round(100 * n / total, 1) if total else 0.0

    def _res_stats(vals: list[float]) -> dict[str, float | int]:
        if not vals:
            return {}
        qs = statistics.quantiles(vals, n=4)
        return {
            "count": len(vals),
            "mean": round(statistics.mean(vals), 2),
            "median": round(statistics.median(vals), 2),
            "q1": round(qs[0], 2),
            "q3": round(qs[2], 2),
            "min": round(min(vals), 2),
            "max": round(max(vals), 2),
        }

    rep = {
        "total_records": total,
        "parse_failures": failed,
        "task_type_counts": dict(task_counts),
        "experimental_method_counts": dict(method_counts.most_common(10)),
        "resolution_angstrom": _res_stats(resolutions),
        "quality_score": _res_stats(scores),
        "field_coverage_pct": {field: _pct(field_present[field]) for field in optional_fields},
    }

    layout.reports_dir.mkdir(parents=True, exist_ok=True)
    report_path = layout.reports_dir / "summary.json"
    report_path.write_text(json.dumps(rep, indent=2))
    export_status = refresh_master_exports(layout)

    return ProcessedReportResult(
        processed_dir=processed_dir,
        report_path=report_path,
        total_records=total,
        parse_failures=failed,
        task_type_counts=dict(task_counts),
        top_methods=dict(method_counts.most_common(3)),
        mean_quality_score=statistics.mean(scores) if scores else None,
        export_status=export_status,
    )
