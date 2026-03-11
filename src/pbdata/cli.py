import json
import logging
import os
import statistics
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from collections import Counter
from collections import defaultdict
from pathlib import Path
from typing import Annotated, Optional

import typer

from pbdata.config import AppConfig, load_config
from pbdata.logging_config import setup_logging
from pbdata.pairing import chain_group_key, parse_pair_identity_key
from pbdata.source_state import write_source_state
from pbdata.stage_state import write_stage_state
from pbdata.storage import (
    StorageLayout,
    build_storage_layout,
    reuse_existing_file,
    validate_bindingdb_raw_json,
    validate_rcsb_raw_json,
    validate_skempi_csv,
)

app = typer.Typer(help="Protein binding dataset platform CLI.")

_DEFAULT_CONFIG      = Path("configs/sources.yaml")
_DEFAULT_LOG_CONFIG  = Path("configs/logging.yaml")
_DEFAULT_CRITERIA    = Path("configs/criteria.yaml")
logger = logging.getLogger(__name__)


@app.command("gui")
def gui_cmd() -> None:
    """Launch the desktop GUI."""
    from pbdata.gui import main as gui_main

    gui_main()


def _storage_layout(ctx: typer.Context) -> StorageLayout:
    return ctx.obj["storage_layout"]


def _coerce_workers(workers: int) -> int:
    if workers <= 0:
        return max(os.cpu_count() or 1, 1)
    return workers


def _is_up_to_date(source_path: Path, output_path: Path) -> bool:
    try:
        return output_path.stat().st_mtime >= source_path.stat().st_mtime
    except OSError:
        return False


def _validate_processed_record(path: Path) -> bool:
    from pbdata.schemas.canonical_sample import CanonicalBindingSample

    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
        CanonicalBindingSample.model_validate(raw)
        return True
    except Exception:
        return False


def _validate_extracted_bundle(output_dir: Path, pdb_id: str) -> bool:
    required = [
        output_dir / "entry" / f"{pdb_id}.json",
        output_dir / "chains" / f"{pdb_id}.json",
        output_dir / "bound_objects" / f"{pdb_id}.json",
        output_dir / "interfaces" / f"{pdb_id}.json",
        output_dir / "assays" / f"{pdb_id}.json",
        output_dir / "provenance" / f"{pdb_id}.json",
    ]
    try:
        if not all(path.exists() and path.stat().st_size >= 0 for path in required):
            return False
        json.loads(required[0].read_text(encoding="utf-8"))
        return True
    except Exception:
        return False


def _exit_with_dependency_error(exc: Exception) -> None:
    message = str(exc)
    lowered = message.lower()
    if "torch" in lowered:
        typer.echo("Error: this command requires the optional 'torch' dependency. Install it and retry.")
    elif "pyarrow" in lowered or "fastparquet" in lowered or "parquet" in lowered:
        typer.echo("Error: this command requires parquet support. Install 'pyarrow' or 'fastparquet' and retry.")
    else:
        typer.echo(f"Error: {exc}")
    raise typer.Exit(code=1)


def _fetch_bindingdb_samples_for_pdb(
    pdb_id: str,
    config: AppConfig,
    *,
    layout: StorageLayout,
) -> list:
    from pbdata.pipeline.enrichment import fetch_bindingdb_samples_for_pdb

    return fetch_bindingdb_samples_for_pdb(pdb_id, config, layout=layout)


def _delete_extracted_bundle(output_dir: Path, pdb_id: str) -> None:
    for table_name in ["entry", "chains", "bound_objects", "interfaces", "assays", "provenance"]:
        (output_dir / table_name / f"{pdb_id}.json").unlink(missing_ok=True)


def _load_json_rows(path: Path) -> list[dict]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(raw, list):
        return [row for row in raw if isinstance(row, dict)]
    return [raw] if isinstance(raw, dict) else []


def _load_table_rows(table_dir: Path) -> list[dict]:
    rows: list[dict] = []
    if not table_dir.exists():
        return rows
    for path in sorted(table_dir.glob("*.json")):
        try:
            rows.extend(_load_json_rows(path))
        except Exception:
            continue
    return rows


def _load_external_assay_samples(
    config: AppConfig,
    *,
    layout: StorageLayout,
) -> dict[str, list]:
    from pbdata.pipeline.enrichment import load_external_assay_samples

    return load_external_assay_samples(config, layout=layout)


def _raw_uniprot_ids(raw: dict) -> list[str]:
    seen: dict[str, None] = {}
    for ent in raw.get("polymer_entities") or []:
        ids = (
            (ent.get("rcsb_polymer_entity_container_identifiers") or {})
            .get("uniprot_ids") or []
        )
        for uniprot_id in ids:
            if uniprot_id:
                seen[str(uniprot_id)] = None
    return list(seen)


def _raw_chain_ids_by_uniprot(raw: dict) -> dict[str, list[str]]:
    mapping: dict[str, list[str]] = {}
    for ent in raw.get("polymer_entities") or []:
        ids = (
            (ent.get("rcsb_polymer_entity_container_identifiers") or {})
            .get("uniprot_ids") or []
        )
        chains = (
            (ent.get("rcsb_polymer_entity_container_identifiers") or {})
            .get("auth_asym_ids") or []
        )
        if not chains:
            continue
        for uniprot_id in ids:
            if not uniprot_id:
                continue
            mapping.setdefault(str(uniprot_id), [])
            for chain_id in chains:
                if chain_id and chain_id not in mapping[str(uniprot_id)]:
                    mapping[str(uniprot_id)].append(str(chain_id))
    return mapping


def _raw_ligand_inchikeys(
    raw: dict,
    chem_descriptors: dict[str, dict[str, str]],
) -> list[str]:
    seen: dict[str, None] = {}
    for ent in raw.get("nonpolymer_entities") or []:
        comp_id = (
            ((ent.get("nonpolymer_comp") or {}).get("chem_comp") or {})
            .get("id", "")
        )
        if not comp_id:
            continue
        desc = chem_descriptors.get(str(comp_id), {})
        inchikey = desc.get("InChIKey")
        if inchikey:
            seen[str(inchikey)] = None
    return list(seen)


def _fetch_chembl_samples_for_raw(
    raw: dict,
    chem_descriptors: dict[str, dict[str, str]],
    config: AppConfig,
) -> list:
    from pbdata.pipeline.enrichment import fetch_chembl_samples_for_raw

    return fetch_chembl_samples_for_raw(raw, chem_descriptors, config)


def _count_delimited_rows(path: Path, delimiter: str = ",") -> int | None:
    try:
        with path.open(encoding="utf-8", newline="") as f:
            lines = [line for line in f.read().splitlines() if line.strip()]
        return max(len(lines) - 1, 0)
    except OSError:
        return None


@app.callback()
def _setup(
    ctx: typer.Context,
    config: Annotated[
        Optional[Path],
        typer.Option("--config", "-c", help="Path to sources YAML config."),
    ] = None,
    log_config: Annotated[
        Optional[Path],
        typer.Option("--log-config", help="Path to logging YAML config."),
    ] = None,
    storage_root: Annotated[
        Optional[Path],
        typer.Option(
            "--storage-root",
            help="Parent folder under which all data/ outputs are created.",
        ),
    ] = None,
) -> None:
    """Global setup: logging and config loading."""
    log_path: Optional[Path] = log_config
    if log_path is None:
        log_path = _DEFAULT_LOG_CONFIG if _DEFAULT_LOG_CONFIG.exists() else None
    setup_logging(log_path)

    cfg_path: Optional[Path] = config
    if cfg_path is None:
        if _DEFAULT_CONFIG.exists():
            cfg_path = _DEFAULT_CONFIG
        else:
            logger.warning(
                "No sources config found at %s; using defaults. "
                "Pass --config to specify a config file.",
                _DEFAULT_CONFIG,
            )

    ctx.ensure_object(dict)
    cfg = load_config(cfg_path) if cfg_path is not None else AppConfig()
    layout = build_storage_layout(storage_root or cfg.storage_root)
    ctx.obj["config"] = cfg
    ctx.obj["storage_layout"] = layout


# ---------------------------------------------------------------------------
# ingest
# ---------------------------------------------------------------------------

@app.command()
def ingest(
    ctx: typer.Context,
    source: Annotated[
        str,
        typer.Option(
            "--source", "-s",
            help="Data source to ingest: rcsb | skempi  (default: rcsb).",
        ),
    ] = "rcsb",
    dry_run: Annotated[
        bool,
        typer.Option("--dry-run", help="Count matching entries only; do not download."),
    ] = False,
    yes: Annotated[
        bool,
        typer.Option("--yes", "-y", help="Skip confirmation prompt."),
    ] = False,
    criteria: Annotated[
        Optional[Path],
        typer.Option("--criteria", help="Path to criteria YAML (RCSB only)."),
    ] = None,
    output: Annotated[
        Optional[Path],
        typer.Option("--output", "-o", help="Override default output directory."),
    ] = None,
) -> None:
    """Download raw data from a supported source database.

    Sources:
      rcsb    — RCSB PDB (default): search by criteria and download metadata
      skempi  — SKEMPI v2: download the full mutation-ddG CSV
    """
    source_lower = source.lower()

    if source_lower == "rcsb":
        _ingest_rcsb(ctx, dry_run=dry_run, yes=yes, criteria=criteria, output=output)
    elif source_lower == "skempi":
        _ingest_skempi(ctx, dry_run=dry_run, yes=yes, output=output)
    else:
        typer.echo(
            f"Unknown source: '{source}'.  Supported: rcsb, skempi.",
            err=True,
        )
        raise typer.Exit(code=1)


def _ingest_rcsb(
    ctx: typer.Context,
    *,
    dry_run: bool,
    yes: bool,
    criteria: Optional[Path],
    output: Optional[Path],
) -> None:
    from pbdata.criteria import load_criteria
    from pbdata.sources.rcsb_search import count_entries, search_and_download

    criteria_path = criteria if criteria is not None else _DEFAULT_CRITERIA
    sc = load_criteria(criteria_path)

    logger.info("Querying RCSB Search API...")
    count = count_entries(sc)
    typer.echo(f"Found {count:,} RCSB entries matching criteria.")

    if dry_run:
        return

    if not yes:
        typer.confirm(f"Proceed with downloading {count:,} entries?", abort=True)

    layout = _storage_layout(ctx)
    out_dir = output if output is not None else layout.raw_rcsb_dir
    search_and_download(sc, out_dir, log_fn=typer.echo, manifest_path=layout.catalog_path)
    typer.echo("RCSB ingest complete.")


def _ingest_skempi(
    ctx: typer.Context,
    *,
    dry_run: bool,
    yes: bool,
    output: Optional[Path],
) -> None:
    import requests

    from pbdata.catalog import summarize_bulk_file, update_download_manifest
    from pbdata.sources.skempi import _SKEMPI_URL

    layout = _storage_layout(ctx)
    out_dir = output if output is not None else layout.raw_skempi_dir
    csv_path = out_dir / "skempi_v2.csv"
    downloaded_at = datetime.now(timezone.utc).isoformat()

    if reuse_existing_file(csv_path, validator=validate_skempi_csv):
        typer.echo(f"SKEMPI CSV already present at {csv_path}.  Skipping download.")
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
        return

    typer.echo(f"SKEMPI v2 will be downloaded from {_SKEMPI_URL}")
    typer.echo("File size is approximately 3 MB.")

    if dry_run:
        typer.echo("[dry-run] Would download SKEMPI CSV — skipping.")
        return

    if not yes:
        typer.confirm("Proceed with downloading SKEMPI v2 CSV?", abort=True)

    out_dir.mkdir(parents=True, exist_ok=True)
    typer.echo("Downloading SKEMPI v2 CSV...")
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
    typer.echo(f"SKEMPI CSV saved to {csv_path}")
    typer.echo(f"Download manifest updated at {layout.catalog_path}")
    typer.echo("Run 'normalize --source skempi' to convert to canonical records.")


# ---------------------------------------------------------------------------
# normalize
# ---------------------------------------------------------------------------

@app.command("normalize")
def normalize_cmd(
    ctx: typer.Context,
    workers: Annotated[
        int,
        typer.Option("--workers", min=0, help="Worker count (0 = CPU count)."),
    ] = 1,
) -> None:
    """Normalize raw RCSB records from data/raw/rcsb/ into the canonical schema.

    Also fetches ligand SMILES / InChIKey from the RCSB chem-comp API for
    every unique ligand comp_id present in the raw records.
    """
    from pbdata.sources.rcsb import RCSBAdapter
    from pbdata.sources.rcsb_search import fetch_chemcomp_descriptors

    layout = _storage_layout(ctx)
    raw_dir = layout.raw_rcsb_dir
    out_dir = layout.processed_rcsb_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    files = sorted(raw_dir.glob("*.json"))
    if not files:
        typer.echo(f"No raw files found in {raw_dir}. Run 'ingest' first.")
        return

    # Collect all unique ligand comp_ids in one pass before normalizing
    typer.echo(f"Scanning {len(files):,} RCSB records for ligand IDs...")
    comp_ids: list[str] = []
    raw_data: list[tuple[Path, dict]] = []
    for f in files:
        try:
            raw = json.loads(f.read_text())
            raw_data.append((f, raw))
            for ent in (raw.get("nonpolymer_entities") or []):
                cid = (
                    ((ent.get("nonpolymer_comp") or {}).get("chem_comp") or {})
                    .get("id", "")
                )
                if cid:
                    comp_ids.append(cid)
        except Exception as exc:
            logger.warning("Failed to read %s: %s", f.name, exc)

    # Batch-fetch SMILES / InChIKey (best-effort)
    chem_descriptors: dict[str, dict[str, str]] = {}
    if comp_ids:
        unique = list(dict.fromkeys(comp_ids))
        typer.echo(f"Fetching chem-comp descriptors for {len(unique):,} unique ligands...")
        try:
            chem_descriptors = fetch_chemcomp_descriptors(unique)
            typer.echo(f"  Got descriptors for {len(chem_descriptors):,} ligands.")
        except Exception as exc:
            logger.warning("Chem-comp fetch failed (SMILES will be absent): %s", exc)

    typer.echo(f"Normalizing {len(raw_data):,} RCSB records...")
    adapter = RCSBAdapter()
    ok = cached = failed = 0
    worker_count = _coerce_workers(workers)

    def _normalize_one(item: tuple[Path, dict]) -> tuple[str, str]:
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

    typer.echo(f"Done. Normalized: {ok:,}, Cached: {cached:,}, Failed: {failed:,}")
    typer.echo(f"Storage root: {layout.root}")
    typer.echo(f"Output: {out_dir}")
    typer.echo(f"Stage state: {state_path}")


# ---------------------------------------------------------------------------
# audit
# ---------------------------------------------------------------------------

@app.command()
def audit(
    ctx: typer.Context,
    workers: Annotated[
        int,
        typer.Option("--workers", min=0, help="Worker count (0 = CPU count)."),
    ] = 1,
) -> None:
    """Score and flag all normalized records; write audit summary."""
    from pbdata.quality.audit import audit_record
    from pbdata.schemas.canonical_sample import CanonicalBindingSample

    layout = _storage_layout(ctx)
    processed_dir = layout.processed_rcsb_dir
    files = sorted(processed_dir.glob("*.json")) if processed_dir.exists() else []
    if not files:
        typer.echo(f"No processed records found in {processed_dir}. Run 'normalize' first.")
        return

    typer.echo(f"Auditing {len(files):,} records...")
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
        for f in files:
            try:
                audited = _audit_one(f)
                flag_counter.update(audited.quality_flags)
                scores.append(audited.quality_score)
                ok += 1
            except Exception as exc:
                logger.warning("Failed to audit %s: %s", f.name, exc)
                failed += 1
    else:
        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            futures = {executor.submit(_audit_one, f): f.name for f in files}
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
            "mean":   round(statistics.mean(scores), 4) if scores else 0,
            "median": round(statistics.median(scores), 4) if scores else 0,
            "min":    round(min(scores), 4) if scores else 0,
            "max":    round(max(scores), 4) if scores else 0,
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

    typer.echo(f"Audit complete. Mean quality score: {summary['quality_score']['mean']:.3f}")
    typer.echo(f"Top flags: {dict(flag_counter.most_common(5))}")
    typer.echo(f"Storage root: {layout.root}")
    typer.echo(f"Summary written to {summary_path}")
    typer.echo(f"Stage state: {state_path}")


# ---------------------------------------------------------------------------
# report
# ---------------------------------------------------------------------------

@app.command()
def report(ctx: typer.Context) -> None:
    """Generate a summary statistics report over all processed records."""
    from pbdata.master_export import (
        refresh_master_exports,
    )
    from pbdata.schemas.canonical_sample import CanonicalBindingSample

    layout = _storage_layout(ctx)
    processed_dir = layout.processed_rcsb_dir
    files = sorted(processed_dir.glob("*.json")) if processed_dir.exists() else []
    if not files:
        typer.echo(f"No processed records found in {processed_dir}. Run 'normalize' first.")
        return

    typer.echo(f"Generating report for {len(files):,} records...")

    task_counts:   Counter[str]         = Counter()
    method_counts: Counter[str]         = Counter()
    resolutions:   list[float]          = []
    scores:        list[float]          = []
    field_present: Counter[str]         = Counter()
    OPTIONAL_FIELDS = [
        "sequence_receptor", "sequence_partner", "chain_ids_receptor",
        "uniprot_ids", "taxonomy_ids", "ligand_id", "ligand_smiles",
        "experimental_method", "structure_resolution",
    ]

    failed = 0
    for f in files:
        try:
            raw = json.loads(f.read_text())
            rec = CanonicalBindingSample.model_validate(raw)
            task_counts[rec.task_type] += 1
            if rec.experimental_method:
                method_counts[rec.experimental_method] += 1
            if rec.structure_resolution is not None:
                resolutions.append(rec.structure_resolution)
            scores.append(rec.quality_score)
            for field in OPTIONAL_FIELDS:
                val = getattr(rec, field, None)
                if val is not None and val != [] and val != "":
                    field_present[field] += 1
        except Exception as exc:
            logger.warning("Skipping %s: %s", f.name, exc)
            failed += 1

    total = len(files) - failed

    def _pct(n: int) -> float:
        return round(100 * n / total, 1) if total else 0.0

    def _res_stats(vals: list[float]) -> dict:
        if not vals:
            return {}
        qs = statistics.quantiles(vals, n=4)
        return {
            "count":  len(vals),
            "mean":   round(statistics.mean(vals), 2),
            "median": round(statistics.median(vals), 2),
            "q1":     round(qs[0], 2),
            "q3":     round(qs[2], 2),
            "min":    round(min(vals), 2),
            "max":    round(max(vals), 2),
        }

    rep = {
        "total_records":    total,
        "parse_failures":   failed,
        "task_type_counts": dict(task_counts),
        "experimental_method_counts": dict(method_counts.most_common(10)),
        "resolution_angstrom": _res_stats(resolutions),
        "quality_score": _res_stats(scores),
        "field_coverage_pct": {
            f: _pct(field_present[f]) for f in OPTIONAL_FIELDS
        },
    }

    layout.reports_dir.mkdir(parents=True, exist_ok=True)
    report_path = layout.reports_dir / "summary.json"
    report_path.write_text(json.dumps(rep, indent=2))
    export_status = refresh_master_exports(layout)

    typer.echo(f"\n{'─'*40}")
    typer.echo(f"Total records   : {total:,}")
    typer.echo(f"Task types      : {dict(task_counts)}")
    typer.echo(f"Methods         : {dict(method_counts.most_common(3))}")
    if resolutions:
        typer.echo(f"Resolution (Å)  : mean={statistics.mean(resolutions):.2f}  "
                   f"median={statistics.median(resolutions):.2f}")
    typer.echo(f"Mean quality    : {statistics.mean(scores):.3f}" if scores else "")
    typer.echo(f"Storage root    : {layout.root}")
    typer.echo(f"Report written to {report_path}")
    if "master_csv" in export_status:
        typer.echo(f"Master CSV      : {export_status['master_csv']}")
    if "pair_csv" in export_status:
        typer.echo(f"Pair CSV        : {export_status['pair_csv']}")
    if "issue_csv" in export_status:
        typer.echo(f"Issue CSV       : {export_status['issue_csv']}")
    if "conflict_csv" in export_status:
        typer.echo(f"Conflict CSV    : {export_status['conflict_csv']}")
    if "source_state_csv" in export_status:
        typer.echo(f"Source State CSV: {export_status['source_state_csv']}")
    if "model_ready_pairs_csv" in export_status:
        typer.echo(f"Model-ready CSV : {export_status['model_ready_pairs_csv']}")
    if "scientific_coverage_json" in export_status:
        typer.echo(f"Coverage JSON   : {export_status['scientific_coverage_json']}")
    if "release_manifest_json" in export_status:
        typer.echo(f"Release Manifest : {export_status['release_manifest_json']}")
    if "master_csv_error" in export_status:
        typer.echo(f"Master CSV refresh warning: {export_status['master_csv_error']}")
    if "pair_csv_error" in export_status:
        typer.echo(f"Pair CSV refresh warning: {export_status['pair_csv_error']}")
    if "issue_csv_error" in export_status:
        typer.echo(f"Issue CSV refresh warning: {export_status['issue_csv_error']}")
    if "conflict_csv_error" in export_status:
        typer.echo(f"Conflict CSV refresh warning: {export_status['conflict_csv_error']}")
    if "source_state_csv_error" in export_status:
        typer.echo(f"Source State CSV refresh warning: {export_status['source_state_csv_error']}")
    if "release_exports_error" in export_status:
        typer.echo(f"Release export refresh warning: {export_status['release_exports_error']}")


@app.command("report-bias")
def report_bias_cmd(ctx: typer.Context) -> None:
    """Generate automatic dataset-bias summaries from extracted records."""
    from pbdata.reports.bias import build_bias_report

    layout = _storage_layout(ctx)
    if not (layout.extracted_dir / "entry").exists():
        typer.echo(f"No extracted entries found in {layout.extracted_dir}. Run 'extract' first.")
        return
    report_path, _ = build_bias_report(layout.extracted_dir, layout.reports_dir)
    typer.echo(f"Storage root: {layout.root}")
    typer.echo(f"Bias report written to {report_path}")


@app.command("run-scenario-tests")
def run_scenario_tests_cmd(ctx: typer.Context) -> None:
    """Execute scenario templates when possible and emit a structured QA report."""
    from pbdata.qa.scenario_runner import run_scenario_templates

    layout = _storage_layout(ctx)
    scenario_yaml = Path("specs/bio_agent_full_instruction_pack/qa/scenario_test_templates.yaml")
    rubric_path = Path("specs/bio_agent_full_instruction_pack/qa/undesirable_state_rubric.md")
    report_path, manifest_path = run_scenario_templates(
        scenario_yaml,
        rubric_path,
        layout.qa_dir,
        execute_workflows=True,
    )
    typer.echo(f"Storage root: {layout.root}")
    typer.echo(f"Scenario test report written to {report_path}")
    typer.echo(f"Scenario test manifest written to {manifest_path}")


@app.command("status")
def status_cmd(ctx: typer.Context) -> None:
    """Show a concise snapshot of repository data and pipeline state."""
    from pbdata.ops import build_status_report

    layout = _storage_layout(ctx)
    status = build_status_report(layout)
    typer.echo(f"Storage root           : {status['storage_root']}")
    typer.echo(f"Raw RCSB records       : {status['raw_rcsb_count']}")
    typer.echo(f"Processed records      : {status['processed_rcsb_count']}")
    typer.echo(f"Extracted entries      : {status['extracted_entry_count']}")
    typer.echo(f"Structure files        : {status['structure_file_count']}")
    typer.echo(f"Graph exports present  : {status['graph_node_export_present'] and status['graph_edge_export_present']}")
    typer.echo(f"Feature manifest       : {status['feature_manifest_present']}")
    typer.echo(f"Training examples      : {status['training_example_count']}")
    typer.echo(f"Baseline model         : {status['baseline_model_present']}")
    typer.echo(f"Site feature runs      : {status['site_feature_runs']}")
    typer.echo(f"Surrogate checkpoint   : {status['surrogate_checkpoint_present']}")
    typer.echo(f"Latest release         : {status['release_snapshot_present']}")


@app.command("doctor")
def doctor_cmd(ctx: typer.Context) -> None:
    """Check dependency and configuration readiness for the current installation."""
    from pbdata.ops import build_doctor_report

    layout = _storage_layout(ctx)
    cfg: AppConfig = ctx.obj["config"]
    report = build_doctor_report(layout, cfg)
    typer.echo(f"Storage root      : {layout.root}")
    typer.echo(f"Overall status    : {report['overall_status']}")
    typer.echo(f"Python version    : {report['python_version']}")
    typer.echo(f"Data dir present  : {report['required_directories']['data']}")
    typer.echo(f"Artifacts present : {report['required_directories']['artifacts']}")
    typer.echo("Dependencies:")
    for name, payload in report["dependency_checks"].items():
        typer.echo(f"  - {name}: {payload['status']}{' (required)' if payload['required'] else ''}")


@app.command("predict-ligand-screening")
def predict_ligand_screening_cmd(
    ctx: typer.Context,
    smiles: Annotated[Optional[str], typer.Option(help="SMILES input.")] = None,
    sdf: Annotated[Optional[str], typer.Option(help="Path to SDF input.")] = None,
    structure_file: Annotated[Optional[str], typer.Option(help="Path to PDB/mmCIF input.")] = None,
    fasta: Annotated[Optional[str], typer.Option(help="FASTA sequence input.")] = None,
) -> None:
    """Normalize ligand-screening inputs and write a workflow manifest."""
    from pbdata.prediction.engine import run_ligand_screening_workflow

    layout = _storage_layout(ctx)
    try:
        out_path, manifest = run_ligand_screening_workflow(
            layout,
            smiles=smiles,
            sdf=sdf,
            structure_file=structure_file,
            fasta=fasta,
        )
    except (ValueError, ModuleNotFoundError, ImportError, RuntimeError) as exc:
        _exit_with_dependency_error(exc)
    typer.echo(f"Storage root: {layout.root}")
    typer.echo(f"Ligand screening manifest written to {out_path}")
    typer.echo(f"Workflow status: {manifest['status']}")


@app.command("train-baseline-model")
def train_baseline_model_cmd(ctx: typer.Context) -> None:
    """Train the dependency-free split-aware ligand-memory baseline model."""
    from pbdata.models.baseline_memory import train_ligand_memory_model

    layout = _storage_layout(ctx)
    out_path, manifest = train_ligand_memory_model(layout)
    typer.echo(f"Storage root: {layout.root}")
    typer.echo(f"Baseline model artifact written to {out_path}")
    typer.echo(f"Workflow status: {manifest['status']}")


@app.command("evaluate-baseline-model")
def evaluate_baseline_model_cmd(ctx: typer.Context) -> None:
    """Evaluate the ligand-memory baseline model against the current split files."""
    from pbdata.models.baseline_memory import evaluate_ligand_memory_model

    layout = _storage_layout(ctx)
    out_path, manifest = evaluate_ligand_memory_model(layout)
    typer.echo(f"Storage root: {layout.root}")
    typer.echo(f"Baseline model evaluation written to {out_path}")
    typer.echo(f"Workflow status: {manifest['status']}")


@app.command("predict-peptide-binding")
def predict_peptide_binding_cmd(
    ctx: typer.Context,
    structure_file: Annotated[str, typer.Option(help="Path to peptide PDB/mmCIF input.")],
) -> None:
    """Normalize peptide-binding inputs and write a workflow manifest."""
    from pbdata.prediction.engine import run_peptide_binding_workflow

    layout = _storage_layout(ctx)
    try:
        out_path, manifest = run_peptide_binding_workflow(layout, structure_file=structure_file)
    except (ValueError, ModuleNotFoundError, ImportError, RuntimeError) as exc:
        _exit_with_dependency_error(exc)
    typer.echo(f"Storage root: {layout.root}")
    typer.echo(f"Peptide binding manifest written to {out_path}")
    typer.echo(f"Workflow status: {manifest['status']}")


@app.command("score-pathway-risk")
def score_pathway_risk_cmd(
    ctx: typer.Context,
    targets: Annotated[Optional[str], typer.Option(help="Comma-separated UniProt IDs.")] = None,
) -> None:
    """Write a pathway/risk summary from current dataset and graph context."""
    from pbdata.risk.summary import build_pathway_risk_summary

    layout = _storage_layout(ctx)
    target_list = [item.strip() for item in str(targets or "").split(",") if item.strip()]
    if not target_list:
        typer.echo("Error: --targets is required and must contain at least one UniProt ID.")
        raise typer.Exit(code=1)
    out_path, summary = build_pathway_risk_summary(layout, targets=target_list)
    typer.echo(f"Storage root: {layout.root}")
    typer.echo(f"Pathway risk summary written to {out_path}")
    typer.echo(f"Workflow status: {summary['status']}")


def _pair_split_items_from_layout(layout: StorageLayout) -> list:
    chains = _load_table_rows(layout.extracted_dir / "chains")
    assays = _load_table_rows(layout.extracted_dir / "assays")
    training_examples = _load_json_rows(layout.training_dir / "training_examples.json") if (layout.training_dir / "training_examples.json").exists() else []
    if not assays:
        return []

    from pbdata.dataset.splits import PairSplitItem

    sequence_by_pdb_chain: dict[tuple[str, str], str] = {}
    uniprot_by_pdb_chain: dict[tuple[str, str], str] = {}
    for chain in chains:
        pdb_id = str(chain.get("pdb_id") or "")
        chain_id = str(chain.get("chain_id") or "")
        if not pdb_id or not chain_id:
            continue
        seq = str(chain.get("polymer_sequence") or "")
        if seq:
            sequence_by_pdb_chain[(pdb_id, chain_id)] = seq
        uniprot_id = str(chain.get("uniprot_id") or "")
        if uniprot_id:
            uniprot_by_pdb_chain[(pdb_id, chain_id)] = uniprot_id

    example_id_by_key: dict[tuple[str, str], list[str]] = defaultdict(list)
    for row in training_examples:
        provenance = row.get("provenance") or {}
        labels = row.get("labels") or {}
        pair_key = str(provenance.get("pair_identity_key") or "")
        affinity_type = str(labels.get("affinity_type") or "")
        example_id = str(row.get("example_id") or "")
        if pair_key and example_id:
            example_id_by_key[(pair_key, affinity_type)].append(example_id)

    items: list[PairSplitItem] = []
    for assay in assays:
        pair_key = str(assay.get("pair_identity_key") or "")
        affinity_type = str(assay.get("binding_affinity_type") or "")
        parsed_pair = parse_pair_identity_key(pair_key)
        if not pair_key or parsed_pair is None:
            continue
        pdb_id = parsed_pair.pdb_id or str(assay.get("pdb_id") or "")
        receptor_chain_ids = list(parsed_pair.receptor_chain_ids)
        sequences = [
            sequence_by_pdb_chain.get((pdb_id, chain_id), "")
            for chain_id in receptor_chain_ids
            if sequence_by_pdb_chain.get((pdb_id, chain_id), "")
        ]
        receptor_sequence = "|".join(sorted(sequences)) if sequences else None
        uniprot_ids = sorted(
            {
                uniprot_by_pdb_chain.get((pdb_id, chain_id), "")
                for chain_id in receptor_chain_ids
                if uniprot_by_pdb_chain.get((pdb_id, chain_id), "")
            }
        )
        receptor_identity = ",".join(uniprot_ids) if uniprot_ids else (
            receptor_sequence if receptor_sequence else f"{pdb_id}:{','.join(receptor_chain_ids) or '-'}"
        )
        mutation_key = (parsed_pair.mutation_key or "wt_or_unspecified").lower()
        mutation_family = (
            "wildtype"
            if mutation_key in {"wt", "wildtype", "wt_or_unspecified"}
            else ("unknown" if mutation_key.startswith("mutation_unknown") else "mutant")
        )
        representation_key = "|".join([
            parsed_pair.task_type,
            affinity_type or "assay_unknown",
            mutation_family,
            "has_sequence" if receptor_sequence else "sequence_unknown",
        ])
        hard_group_key = "|".join([
            parsed_pair.task_type,
            receptor_identity,
            parsed_pair.ligand_key or ",".join(parsed_pair.partner_chain_ids) or "-",
        ])
        target_ids = example_id_by_key.get((pair_key, affinity_type), []) or [f"{pair_key}|{affinity_type or 'assay_unknown'}"]
        for target_id in target_ids:
            items.append(PairSplitItem(
                item_id=target_id,
                pair_identity_key=pair_key,
                affinity_type=affinity_type or None,
                receptor_sequence=receptor_sequence,
                receptor_identity=receptor_identity,
                representation_key=representation_key,
                hard_group_key=hard_group_key,
            ))
    return items


# ---------------------------------------------------------------------------
# build-microstates
# ---------------------------------------------------------------------------


@app.command("build-microstates")
def build_microstates_cmd(ctx: typer.Context) -> None:
    """Build heuristic pair-level microstate assignments from local structures."""
    layout = _storage_layout(ctx)
    from pbdata.features.microstate import build_microstate_records

    extracted_dir = layout.extracted_dir
    if not (extracted_dir / "assays").exists():
        typer.echo(f"No extracted assays found in {extracted_dir}. Run 'extract' first.")
        return

    records_path, manifest_path = build_microstate_records(extracted_dir, layout.microstates_dir)
    record_count = 0
    try:
        record_count = len(json.loads(records_path.read_text(encoding="utf-8")))
    except Exception:
        record_count = 0
    state_path = write_stage_state(
        layout,
        stage="build-microstates",
        status="completed",
        input_dir=extracted_dir,
        output_dir=layout.microstates_dir,
        counts={
            "pairs_scored": record_count,
        },
        notes=(
            "Heuristic local-context microstate approximations from mmCIF geometry. "
            "Not a substitute for AmberTools/CHARMM/QM protonation workflows."
        ),
    )
    typer.echo(f"Microstate records written to {records_path}")
    typer.echo(f"Storage root: {layout.root}")
    typer.echo(f"Microstate manifest written to {manifest_path}")
    typer.echo(f"Stage state: {state_path}")


# ---------------------------------------------------------------------------
# build-physics-features
# ---------------------------------------------------------------------------


@app.command("build-physics-features")
def build_physics_features_cmd(ctx: typer.Context) -> None:
    """Build dense local electrostatic proxy features from microstate records."""
    layout = _storage_layout(ctx)
    from pbdata.features.physics_features import build_local_physics_features

    microstate_path = layout.microstates_dir / "microstate_records.json"
    if not microstate_path.exists():
        typer.echo(
            f"No microstate records found at {microstate_path}. "
            "Run 'build-microstates' first."
        )
        return

    records_path, manifest_path = build_local_physics_features(microstate_path, layout.physics_dir)
    record_count = 0
    try:
        record_count = len(json.loads(records_path.read_text(encoding="utf-8")))
    except Exception:
        record_count = 0
    state_path = write_stage_state(
        layout,
        stage="build-physics-features",
        status="completed",
        input_dir=layout.microstates_dir,
        output_dir=layout.physics_dir,
        counts={
            "pairs_scored": record_count,
        },
        notes=(
            "Pair-level local electrostatic proxy features derived from heuristic "
            "microstate assignments. These are not full MM energies."
        ),
    )
    typer.echo(f"Physics feature records written to {records_path}")
    typer.echo(f"Storage root: {layout.root}")
    typer.echo(f"Physics feature manifest written to {manifest_path}")
    typer.echo(f"Stage state: {state_path}")


# ---------------------------------------------------------------------------
# build-microstate-refinement
# ---------------------------------------------------------------------------


@app.command("build-microstate-refinement")
def build_microstate_refinement_cmd(ctx: typer.Context) -> None:
    """Build explicit protonation-policy planning records for local refinement."""
    layout = _storage_layout(ctx)
    from pbdata.features.mm_features import build_microstate_refinement_plan

    microstate_path = layout.microstates_dir / "microstate_records.json"
    if not microstate_path.exists():
        typer.echo(f"No microstate records found at {microstate_path}. Run 'build-microstates' first.")
        return

    records_path, manifest_path = build_microstate_refinement_plan(
        layout.extracted_dir,
        layout.microstates_dir,
        layout.microstate_refinement_dir,
    )
    record_count = 0
    try:
        record_count = len(json.loads(records_path.read_text(encoding="utf-8")))
    except Exception:
        record_count = 0
    state_path = write_stage_state(
        layout,
        stage="build-microstate-refinement",
        status="completed",
        input_dir=layout.microstates_dir,
        output_dir=layout.microstate_refinement_dir,
        counts={"pairs_planned": record_count},
        notes=(
            "Pair-level protonation-policy planning records. "
            "No external MM backend was executed."
        ),
    )
    typer.echo(f"Microstate refinement records written to {records_path}")
    typer.echo(f"Storage root: {layout.root}")
    typer.echo(f"Microstate refinement manifest written to {manifest_path}")
    typer.echo(f"Stage state: {state_path}")


# ---------------------------------------------------------------------------
# build-mm-job-manifests
# ---------------------------------------------------------------------------


@app.command("build-mm-job-manifests")
def build_mm_job_manifests_cmd(ctx: typer.Context) -> None:
    """Build backend-ready local MM job manifests from refinement plans."""
    layout = _storage_layout(ctx)
    from pbdata.features.mm_features import build_mm_job_manifests

    refinement_path = layout.microstate_refinement_dir / "microstate_refinement_records.json"
    if not refinement_path.exists():
        typer.echo(
            f"No microstate refinement records found at {refinement_path}. "
            "Run 'build-microstate-refinement' first."
        )
        return

    records_path, manifest_path = build_mm_job_manifests(
        layout.microstate_refinement_dir,
        layout.mm_jobs_dir,
    )
    record_count = 0
    try:
        record_count = len(json.loads(records_path.read_text(encoding="utf-8")))
    except Exception:
        record_count = 0
    state_path = write_stage_state(
        layout,
        stage="build-mm-job-manifests",
        status="completed",
        input_dir=layout.microstate_refinement_dir,
        output_dir=layout.mm_jobs_dir,
        counts={"jobs_planned": record_count},
        notes=(
            "Backend-ready local MM job manifests for later Amber/CHARMM/OpenMM execution. "
            "No MM engine was run in this stage."
        ),
    )
    typer.echo(f"MM job records written to {records_path}")
    typer.echo(f"Storage root: {layout.root}")
    typer.echo(f"MM job manifest written to {manifest_path}")
    typer.echo(f"Stage state: {state_path}")


# ---------------------------------------------------------------------------
# run-mm-jobs
# ---------------------------------------------------------------------------


@app.command("run-mm-jobs")
def run_mm_jobs_cmd(
    ctx: typer.Context,
    execute: Annotated[
        bool,
        typer.Option(
            "--execute",
            help="Attempt execution when OpenMM is available. Otherwise validate bundles only.",
        ),
    ] = False,
) -> None:
    """Validate or dispatch local OpenMM-ready MM job bundles."""
    layout = _storage_layout(ctx)
    from pbdata.features.mm_features import run_mm_job_bundles

    jobs_path = layout.mm_jobs_dir / "mm_job_records.json"
    if not jobs_path.exists():
        typer.echo(
            f"No MM job records found at {jobs_path}. "
            "Run 'build-mm-job-manifests' first."
        )
        return

    results_path, manifest_path = run_mm_job_bundles(layout.mm_jobs_dir, execute=execute)
    results = []
    try:
        results = json.loads(results_path.read_text(encoding="utf-8"))
    except Exception:
        results = []
    backend_unavailable = sum(1 for row in results if str(row.get("status") or "") == "backend_unavailable")
    ready = sum(
        1
        for row in results
        if str(row.get("status") or "") in {"backend_ready_not_executed", "backend_ready_execution_deferred"}
    )
    invalid = sum(1 for row in results if str(row.get("status") or "") == "invalid_bundle")
    state_status = "completed" if results else "skipped"
    state_path = write_stage_state(
        layout,
        stage="run-mm-jobs",
        status=state_status,
        input_dir=layout.mm_jobs_dir,
        output_dir=layout.mm_jobs_dir,
        counts={
            "jobs_seen": len(results),
            "backend_ready_jobs": ready,
            "backend_unavailable_jobs": backend_unavailable,
            "invalid_bundles": invalid,
        },
        notes=(
            "MM job execution stage validates OpenMM-ready bundles and backend availability. "
            "It does not fabricate refinement output when the backend is unavailable or execution is deferred."
        ),
    )
    typer.echo(f"MM job execution results written to {results_path}")
    typer.echo(f"Storage root: {layout.root}")
    typer.echo(f"MM job execution manifest written to {manifest_path}")
    typer.echo(f"Stage state: {state_path}")


# ---------------------------------------------------------------------------
# site-centric feature pipeline
# ---------------------------------------------------------------------------


@app.command("run-feature-pipeline")
def run_feature_pipeline_cmd(
    ctx: typer.Context,
    run_mode: Annotated[
        str,
        typer.Option(
            "--run-mode",
            help="Feature pipeline run mode: full_build | resume | stage_only | inference_prepare",
        ),
    ] = "full_build",
    stage_name: Annotated[
        Optional[str],
        typer.Option("--stage-name", help="Stage name for stage_only mode."),
    ] = None,
    run_id: Annotated[
        Optional[str],
        typer.Option("--run-id", help="Optional explicit run identifier."),
    ] = None,
    degraded_mode: Annotated[
        bool,
        typer.Option("--degraded-mode/--no-degraded-mode", help="Allow explicit degraded proxy site physics if no surrogate checkpoint is available."),
    ] = True,
    fail_hard: Annotated[
        bool,
        typer.Option("--fail-hard/--no-fail-hard", help="Stop on the first failed record/stage."),
    ] = False,
    gpu_enabled: Annotated[
        bool,
        typer.Option("--gpu/--no-gpu", help="Record whether GPU-backed site physics is expected for this run."),
    ] = False,
    workers: Annotated[
        int,
        typer.Option("--workers", help="CPU worker count recorded in the run config."),
    ] = 1,
) -> None:
    """Run the new site-centric feature pipeline under artifacts/."""
    from pbdata.pipeline.feature_execution import run_feature_pipeline

    layout = _storage_layout(ctx)
    try:
        result = run_feature_pipeline(
            layout,
            run_mode=run_mode,
            stage_only=stage_name,
            run_id=run_id,
            degraded_mode=degraded_mode,
            fail_hard=fail_hard,
            gpu_enabled=gpu_enabled,
            cpu_workers=_coerce_workers(workers),
        )
    except (ModuleNotFoundError, ImportError, RuntimeError, ValueError) as exc:
        _exit_with_dependency_error(exc)
    typer.echo(f"Storage root: {layout.root}")
    typer.echo(f"Feature pipeline run id: {result['run_id']}")
    typer.echo(f"Artifacts root: {result['artifacts_root']}")
    typer.echo(f"Input manifest: {result['input_manifest']}")
    for stage, status in result["stage_statuses"].items():
        typer.echo(f"{stage}: {status}")


@app.command("export-analysis-queue")
def export_analysis_queue_cmd(
    ctx: typer.Context,
    run_id: Annotated[
        Optional[str],
        typer.Option("--run-id", help="Existing feature-pipeline run id to export archetype queues from."),
    ] = None,
) -> None:
    """Export motif/archetype analysis queues for external ORCA/APBS/OpenMM runs."""
    from pbdata.pipeline.feature_execution import export_analysis_queue

    layout = _storage_layout(ctx)
    resolved_run_id = run_id
    if not resolved_run_id:
        manifests = sorted(
            layout.artifact_manifests_dir.glob("*_input_manifest.json"),
            key=lambda path: path.stat().st_mtime,
            reverse=True,
        )
        if not manifests:
            typer.echo("Error: --run-id is required until at least one site-centric feature run exists.")
            raise typer.Exit(code=1)
        resolved_run_id = manifests[0].name.replace("_input_manifest.json", "")
    try:
        result = export_analysis_queue(layout, run_id=resolved_run_id)
    except (ModuleNotFoundError, ImportError, RuntimeError, ValueError) as exc:
        _exit_with_dependency_error(exc)
    typer.echo(f"Storage root: {layout.root}")
    typer.echo(f"Feature pipeline run id: {resolved_run_id}")
    typer.echo(f"Archetypes: {result['archetypes']}")
    typer.echo(f"Analysis queue: {result['queue']}")
    typer.echo(f"Batch manifest: {result['batch_manifest']}")


@app.command("ingest-physics-results")
def ingest_physics_results_cmd(
    ctx: typer.Context,
    batch_id: Annotated[
        str,
        typer.Option("--batch-id", help="External analysis batch id to ingest."),
    ],
) -> None:
    """Ingest parsed ORCA/APBS/OpenMM outputs into normalized physics targets."""
    from pbdata.pipeline.physics_feedback import ingest_external_analysis_results

    layout = _storage_layout(ctx)
    result = ingest_external_analysis_results(layout, batch_id=batch_id)
    typer.echo(f"Storage root: {layout.root}")
    typer.echo(f"Physics targets: {result['physics_targets']}")
    typer.echo(f"Failed fragments: {result['failed_fragments']}")
    typer.echo(f"Manifest: {result['manifest']}")


@app.command("train-site-physics-surrogate")
def train_site_physics_surrogate_cmd(
    ctx: typer.Context,
    batch_id: Annotated[
        str,
        typer.Option("--batch-id", help="Physics target batch id."),
    ],
    source_run_id: Annotated[
        str,
        typer.Option("--source-run-id", help="Site-centric feature run id used to generate archetypes/env vectors."),
    ],
    surrogate_run_id: Annotated[
        Optional[str],
        typer.Option("--surrogate-run-id", help="Optional explicit surrogate training run id."),
    ] = None,
) -> None:
    """Train the deterministic site-physics surrogate from normalized physics targets."""
    from pbdata.pipeline.physics_feedback import train_site_physics_surrogate

    layout = _storage_layout(ctx)
    result = train_site_physics_surrogate(
        layout,
        batch_id=batch_id,
        source_run_id=source_run_id,
        surrogate_run_id=surrogate_run_id,
    )
    typer.echo(f"Storage root: {layout.root}")
    typer.echo(f"Checkpoint: {result['checkpoint']}")
    typer.echo(f"Manifest: {result['manifest']}")
    typer.echo(f"Latest pointer: {result['latest']}")


# ---------------------------------------------------------------------------
# build-graph
# ---------------------------------------------------------------------------


@app.command("build-conformational-states")
def build_conformational_states_cmd(ctx: typer.Context) -> None:
    """Catalog experimental and planned predicted conformational states."""
    from pbdata.dataset.conformations import build_conformation_states

    layout = _storage_layout(ctx)
    extracted_dir = layout.extracted_dir
    if not (extracted_dir / "entry").exists():
        typer.echo(f"No extracted entries found in {extracted_dir}. Run 'extract' first.")
        return

    states_path, manifest_path = build_conformation_states(extracted_dir, layout.conformations_dir)
    typer.echo(f"Storage root: {layout.root}")
    typer.echo(f"Conformational states written to {states_path}")
    typer.echo(f"Conformation manifest written to {manifest_path}")


@app.command("setup-workspace")
def setup_workspace_cmd(ctx: typer.Context) -> None:
    """Create the workflow-engine workspace directories and manifest."""
    from pbdata.data_pipeline.workflow_engine import initialize_workspace

    layout = _storage_layout(ctx)
    artifacts = initialize_workspace(layout)
    typer.echo(f"Storage root: {layout.root}")
    typer.echo(f"Workflow manifest written to {artifacts['workflow_manifest']}")


@app.command("harvest-metadata")
def harvest_metadata_cmd(ctx: typer.Context) -> None:
    """Build the unified metadata table for dataset engineering workflows."""
    from pbdata.data_pipeline.workflow_engine import harvest_unified_metadata

    layout = _storage_layout(ctx)
    artifacts = harvest_unified_metadata(layout)
    typer.echo(f"Storage root: {layout.root}")
    typer.echo(f"Metadata CSV written to {artifacts['metadata_csv']}")
    typer.echo(f"Metadata manifest written to {artifacts['manifest']}")


@app.command("build-structural-graphs")
def build_structural_graphs_cmd(
    ctx: typer.Context,
    graph_level: Annotated[str, typer.Option(help="residue | atom")] = "residue",
    scope: Annotated[str, typer.Option(help="whole_protein | interface_only | shell")] = "whole_protein",
    shell_radius: Annotated[float, typer.Option(help="Neighborhood shell radius in angstroms.")] = 8.0,
    export_formats: Annotated[list[str] | None, typer.Option("--export-format", help="Repeatable: pyg | dgl | networkx")] = None,
) -> None:
    """Build residue- or atom-level structural graphs for ML workflows."""
    from pbdata.graph.structural_graphs import build_structural_graphs

    layout = _storage_layout(ctx)
    formats = tuple(export_formats or ["pyg", "networkx"])
    try:
        artifacts = build_structural_graphs(
            layout,
            graph_level=graph_level,
            scope=scope,
            shell_radius=shell_radius,
            export_formats=formats,
        )
    except (ModuleNotFoundError, ImportError, RuntimeError, ValueError) as exc:
        _exit_with_dependency_error(exc)
    typer.echo(f"Storage root: {layout.root}")
    typer.echo(f"Structural graph manifest written to {artifacts['manifest']}")


@app.command("build-graph")
def build_graph_cmd(ctx: typer.Context) -> None:
    """Write the graph-layer architecture manifest.

    If extracted structure records are available, materialize a first
    canonical graph layer from those records. Otherwise write the
    architecture manifest for the planned graph subsystem.
    """
    layout = _storage_layout(ctx)
    from pbdata.master_export import refresh_master_exports
    from pbdata.graph.builder import build_graph_from_extracted, build_graph_manifest

    extracted_dir = layout.extracted_dir
    if (extracted_dir / "entry").exists():
        nodes_path, edges_path, manifest_path = build_graph_from_extracted(extracted_dir, layout.graph_dir)
        typer.echo(f"Graph nodes written to {nodes_path}")
        typer.echo(f"Graph edges written to {edges_path}")
        typer.echo(f"Storage root: {layout.root}")
        typer.echo(f"Graph manifest written to {manifest_path}")
        export_status = refresh_master_exports(layout)
        if "master_csv" in export_status:
            typer.echo(f"Master CSV refreshed at {export_status['master_csv']}")
        if "pair_csv" in export_status:
            typer.echo(f"Pair CSV refreshed at {export_status['pair_csv']}")
        if "issue_csv" in export_status:
            typer.echo(f"Issue CSV refreshed at {export_status['issue_csv']}")
        if "conflict_csv" in export_status:
            typer.echo(f"Conflict CSV refreshed at {export_status['conflict_csv']}")
        if "source_state_csv" in export_status:
            typer.echo(f"Source State CSV refreshed at {export_status['source_state_csv']}")
        if "model_ready_pairs_csv" in export_status:
            typer.echo(f"Model-ready CSV refreshed at {export_status['model_ready_pairs_csv']}")
        if "scientific_coverage_json" in export_status:
            typer.echo(f"Coverage JSON refreshed at {export_status['scientific_coverage_json']}")
        if "release_manifest_json" in export_status:
            typer.echo(f"Release manifest refreshed at {export_status['release_manifest_json']}")
        if "master_csv_error" in export_status:
            typer.echo(f"Master CSV refresh warning: {export_status['master_csv_error']}")
        if "pair_csv_error" in export_status:
            typer.echo(f"Pair CSV refresh warning: {export_status['pair_csv_error']}")
        if "issue_csv_error" in export_status:
            typer.echo(f"Issue CSV refresh warning: {export_status['issue_csv_error']}")
        if "conflict_csv_error" in export_status:
            typer.echo(f"Conflict CSV refresh warning: {export_status['conflict_csv_error']}")
        if "source_state_csv_error" in export_status:
            typer.echo(f"Source State CSV refresh warning: {export_status['source_state_csv_error']}")
        if "release_exports_error" in export_status:
            typer.echo(f"Release export refresh warning: {export_status['release_exports_error']}")
        return

    manifest_path = build_graph_manifest(layout.graph_dir)
    typer.echo(f"Storage root: {layout.root}")
    typer.echo(f"Graph architecture manifest written to {manifest_path}")


# ---------------------------------------------------------------------------
# build-features
# ---------------------------------------------------------------------------


@app.command("build-features")
def build_features_cmd(ctx: typer.Context) -> None:
    """Materialize first-pass features when extracted+graph data are present."""
    layout = _storage_layout(ctx)
    from pbdata.master_export import refresh_master_exports
    from pbdata.features.builder import (
        build_feature_manifest,
        build_features_from_extracted_and_graph,
    )

    extracted_dir = layout.extracted_dir
    graph_dir = layout.graph_dir
    if (extracted_dir / "assays").exists() and (graph_dir / "graph_edges.json").exists():
        features_path, manifest_path = build_features_from_extracted_and_graph(
            extracted_dir,
            graph_dir,
            layout.features_dir,
            microstate_dir=layout.microstates_dir,
            physics_dir=layout.physics_dir,
        )
        typer.echo(f"Feature records written to {features_path}")
        typer.echo(f"Storage root: {layout.root}")
        typer.echo(f"Feature manifest written to {manifest_path}")
        export_status = refresh_master_exports(layout)
        if "master_csv" in export_status:
            typer.echo(f"Master CSV refreshed at {export_status['master_csv']}")
        if "pair_csv" in export_status:
            typer.echo(f"Pair CSV refreshed at {export_status['pair_csv']}")
        if "issue_csv" in export_status:
            typer.echo(f"Issue CSV refreshed at {export_status['issue_csv']}")
        if "conflict_csv" in export_status:
            typer.echo(f"Conflict CSV refreshed at {export_status['conflict_csv']}")
        if "source_state_csv" in export_status:
            typer.echo(f"Source State CSV refreshed at {export_status['source_state_csv']}")
        if "model_ready_pairs_csv" in export_status:
            typer.echo(f"Model-ready CSV refreshed at {export_status['model_ready_pairs_csv']}")
        if "scientific_coverage_json" in export_status:
            typer.echo(f"Coverage JSON refreshed at {export_status['scientific_coverage_json']}")
        if "release_manifest_json" in export_status:
            typer.echo(f"Release manifest refreshed at {export_status['release_manifest_json']}")
        if "master_csv_error" in export_status:
            typer.echo(f"Master CSV refresh warning: {export_status['master_csv_error']}")
        if "pair_csv_error" in export_status:
            typer.echo(f"Pair CSV refresh warning: {export_status['pair_csv_error']}")
        if "issue_csv_error" in export_status:
            typer.echo(f"Issue CSV refresh warning: {export_status['issue_csv_error']}")
        if "conflict_csv_error" in export_status:
            typer.echo(f"Conflict CSV refresh warning: {export_status['conflict_csv_error']}")
        if "source_state_csv_error" in export_status:
            typer.echo(f"Source State CSV refresh warning: {export_status['source_state_csv_error']}")
        if "release_exports_error" in export_status:
            typer.echo(f"Release export refresh warning: {export_status['release_exports_error']}")
        return

    manifest_path = build_feature_manifest(layout.features_dir)
    typer.echo(f"Storage root: {layout.root}")
    typer.echo(f"Feature architecture manifest written to {manifest_path}")


# ---------------------------------------------------------------------------
# build-training-examples
# ---------------------------------------------------------------------------


@app.command("build-training-examples")
def build_training_examples_cmd(ctx: typer.Context) -> None:
    """Assemble training examples from extracted, graph, and feature layers.

    If all upstream layers are present, joins them into spec-aligned
    TrainingExampleRecord objects.  Otherwise writes a planned manifest.
    """
    layout = _storage_layout(ctx)
    from pbdata.master_export import refresh_master_exports
    from pbdata.training.generator import build_training_examples, build_training_manifest

    extracted_dir = layout.extracted_dir
    has_assays = (extracted_dir / "assays").exists()
    has_features = (layout.features_dir / "feature_records.json").exists()
    has_graph = (layout.graph_dir / "graph_nodes.json").exists()

    if has_assays and has_features and has_graph:
        examples_path, manifest_path = build_training_examples(
            extracted_dir, layout.features_dir, layout.graph_dir, layout.training_dir,
        )
        typer.echo(f"Training examples written to {examples_path}")
        typer.echo(f"Storage root: {layout.root}")
        typer.echo(f"Training manifest written to {manifest_path}")
        export_status = refresh_master_exports(layout)
        if "master_csv" in export_status:
            typer.echo(f"Master CSV refreshed at {export_status['master_csv']}")
        if "pair_csv" in export_status:
            typer.echo(f"Pair CSV refreshed at {export_status['pair_csv']}")
        if "issue_csv" in export_status:
            typer.echo(f"Issue CSV refreshed at {export_status['issue_csv']}")
        if "conflict_csv" in export_status:
            typer.echo(f"Conflict CSV refreshed at {export_status['conflict_csv']}")
        if "source_state_csv" in export_status:
            typer.echo(f"Source State CSV refreshed at {export_status['source_state_csv']}")
        if "model_ready_pairs_csv" in export_status:
            typer.echo(f"Model-ready CSV refreshed at {export_status['model_ready_pairs_csv']}")
        if "scientific_coverage_json" in export_status:
            typer.echo(f"Coverage JSON refreshed at {export_status['scientific_coverage_json']}")
        if "release_manifest_json" in export_status:
            typer.echo(f"Release manifest refreshed at {export_status['release_manifest_json']}")
        if "master_csv_error" in export_status:
            typer.echo(f"Master CSV refresh warning: {export_status['master_csv_error']}")
        if "pair_csv_error" in export_status:
            typer.echo(f"Pair CSV refresh warning: {export_status['pair_csv_error']}")
        if "issue_csv_error" in export_status:
            typer.echo(f"Issue CSV refresh warning: {export_status['issue_csv_error']}")
        if "conflict_csv_error" in export_status:
            typer.echo(f"Conflict CSV refresh warning: {export_status['conflict_csv_error']}")
        if "source_state_csv_error" in export_status:
            typer.echo(f"Source State CSV refresh warning: {export_status['source_state_csv_error']}")
        if "release_exports_error" in export_status:
            typer.echo(f"Release export refresh warning: {export_status['release_exports_error']}")
        return

    manifest_path = build_training_manifest(layout.training_dir)
    typer.echo(f"Storage root: {layout.root}")
    typer.echo(f"Training-example architecture manifest written to {manifest_path}")
    if not has_assays:
        typer.echo("  (missing: extracted assays — run 'extract' first)")
    if not has_graph:
        typer.echo("  (missing: graph data — run 'build-graph' first)")
    if not has_features:
        typer.echo("  (missing: feature records — run 'build-features' first)")


# ---------------------------------------------------------------------------
# build-custom-training-set
# ---------------------------------------------------------------------------


@app.command("build-custom-training-set")
def build_custom_training_set_cmd(
    ctx: typer.Context,
    mode: Annotated[
        str,
        typer.Option(help="generalist | protein_ligand | protein_protein | mutation_effect | high_trust"),
    ] = "generalist",
    target_size: Annotated[
        int,
        typer.Option(help="Target number of examples to select."),
    ] = 500,
    seed: Annotated[
        int,
        typer.Option(help="Deterministic tie-break seed."),
    ] = 42,
    per_receptor_cluster_cap: Annotated[
        int,
        typer.Option(help="Soft cap on selected examples per receptor sequence cluster."),
    ] = 1,
    tag: Annotated[
        Optional[str],
        typer.Option("--tag", help="Optional snapshot tag. Defaults to current UTC timestamp."),
    ] = None,
) -> None:
    """Build a diversity-optimized custom training set from model-ready pairs."""
    from pbdata.custom_training_set import build_custom_training_set

    layout = _storage_layout(ctx)
    artifacts = build_custom_training_set(
        layout,
        mode=mode,
        target_size=target_size,
        seed=seed,
        per_receptor_cluster_cap=per_receptor_cluster_cap,
        release_tag=tag,
    )
    typer.echo(f"Storage root: {layout.root}")
    typer.echo(f"Custom training set : {artifacts['custom_training_set_csv']}")
    typer.echo(f"Exclusions          : {artifacts['custom_training_exclusions_csv']}")
    typer.echo(f"Summary             : {artifacts['custom_training_summary_json']}")
    typer.echo(f"Scorecard           : {artifacts['custom_training_scorecard_json']}")
    typer.echo(f"Split benchmark     : {artifacts['custom_training_split_benchmark_csv']}")
    typer.echo(f"Manifest            : {artifacts['custom_training_manifest_json']}")
    typer.echo(f"Snapshot dir        : {artifacts['custom_training_snapshot_dir']}")


@app.command("engineer-dataset")
def engineer_dataset_cmd(
    ctx: typer.Context,
    dataset_name: Annotated[str, typer.Option(help="Name of the engineered dataset export.")] = "engineered_dataset",
    test_frac: Annotated[float, typer.Option(help="Test-set fraction.")] = 0.20,
    cv_folds: Annotated[int, typer.Option(help="Optional number of CV folds.")] = 0,
    strict_family_isolation: Annotated[bool, typer.Option(help="Keep the same protein family entirely in one split.")] = False,
    embedding_backend: Annotated[str, typer.Option(help="auto | esm | fallback")] = "auto",
    cluster_count: Annotated[int, typer.Option(help="Target cluster count for diversity grouping.")] = 8,
    seed: Annotated[int, typer.Option(help="Deterministic seed.")] = 42,
) -> None:
    """Build a diverse, leakage-aware ML dataset export."""
    from pbdata.dataset.engineering import DatasetEngineeringConfig, engineer_dataset

    layout = _storage_layout(ctx)
    try:
        artifacts = engineer_dataset(
            layout,
            config=DatasetEngineeringConfig(
                dataset_name=dataset_name,
                test_frac=test_frac,
                cv_folds=cv_folds,
                strict_family_isolation=strict_family_isolation,
                embedding_backend=embedding_backend,
                cluster_count=cluster_count,
                seed=seed,
            ),
        )
    except (ModuleNotFoundError, ImportError, RuntimeError, ValueError) as exc:
        _exit_with_dependency_error(exc)
    typer.echo(f"Storage root: {layout.root}")
    if "train_csv" in artifacts:
        typer.echo(f"Train CSV        : {artifacts['train_csv']}")
    if "test_csv" in artifacts:
        typer.echo(f"Test CSV         : {artifacts['test_csv']}")
    if "cv_folds_dir" in artifacts:
        typer.echo(f"CV folds         : {artifacts['cv_folds_dir']}")
    typer.echo(f"Diversity report : {artifacts['diversity_report']}")
    typer.echo(f"Dataset config   : {artifacts['dataset_config']}")
    typer.echo(f"Feature schema   : {artifacts['feature_schema']}")
    typer.echo(f"Graph config     : {artifacts['graph_config']}")


# ---------------------------------------------------------------------------
# build-release
# ---------------------------------------------------------------------------


@app.command("build-release")
def build_release_cmd(
    ctx: typer.Context,
    tag: Annotated[
        Optional[str],
        typer.Option("--tag", help="Optional release tag. Defaults to current UTC timestamp."),
    ] = None,
    strict: Annotated[
        bool,
        typer.Option("--strict/--no-strict", help="Block snapshot creation when release readiness has blockers."),
    ] = False,
) -> None:
    """Freeze the current release artifacts into a versioned snapshot directory."""
    from pbdata.release_export import build_release_snapshot

    layout = _storage_layout(ctx)
    try:
        artifacts = build_release_snapshot(layout, release_tag=tag, strict=strict)
    except ValueError as exc:
        typer.echo(f"Error: {exc}")
        raise typer.Exit(code=1)
    typer.echo(f"Storage root: {layout.root}")
    typer.echo(f"Release snapshot: {artifacts['release_snapshot_dir']}")
    if "model_ready_pairs_csv" in artifacts:
        typer.echo(f"Model-ready CSV : {artifacts['model_ready_pairs_csv']}")
    if "scientific_coverage_json" in artifacts:
        typer.echo(f"Coverage JSON   : {artifacts['scientific_coverage_json']}")
    if "release_manifest_json" in artifacts:
        typer.echo(f"Release Manifest : {artifacts['release_manifest_json']}")
    typer.echo(f"Snapshot Manifest: {artifacts['release_snapshot_manifest_json']}")
    if "latest_release_json" in artifacts:
        typer.echo(f"Latest Release   : {artifacts['latest_release_json']}")
    if "release_readiness_json" in artifacts:
        typer.echo(f"Readiness Report : {artifacts['release_readiness_json']}")


@app.command("release-check")
def release_check_cmd(ctx: typer.Context) -> None:
    """Build the release-readiness report and print blockers/warnings."""
    from pbdata.release_export import build_release_readiness_report

    layout = _storage_layout(ctx)
    out_path, report = build_release_readiness_report(layout)
    typer.echo(f"Storage root    : {layout.root}")
    typer.echo(f"Release status  : {report['release_status']}")
    typer.echo(f"Blockers        : {', '.join(report['blockers']) or 'none'}")
    typer.echo(f"Warnings        : {', '.join(report['warnings']) or 'none'}")
    typer.echo(f"Readiness report: {out_path}")


# ---------------------------------------------------------------------------
# build-splits
# ---------------------------------------------------------------------------

@app.command("build-splits")
def build_splits_cmd(
    ctx: typer.Context,
    train_frac:    Annotated[float, typer.Option(help="Train fraction.")] = 0.70,
    val_frac:      Annotated[float, typer.Option(help="Validation fraction.")] = 0.15,
    seed:          Annotated[int,   typer.Option(help="Hash seed for reproducibility.")] = 42,
    split_mode:    Annotated[str,   typer.Option(help="auto | pair-aware | legacy-sequence | hash")] = "auto",
    hash_only:     Annotated[bool,  typer.Option("--hash-only", help="Use fast hash split (no clustering).")] = False,
    threshold:     Annotated[float, typer.Option(help="Jaccard threshold for sequence clustering.")] = 0.30,
) -> None:
    """Build reproducible train/val/test splits from processed records.

    By default uses k-mer Jaccard clustering so that proteins with high
    sequence identity stay in the same partition (leakage prevention).
    Use --hash-only for a fast but leakage-prone hash-based split.

    Outputs train.txt, val.txt, test.txt, and metadata.json to data/splits/.
    """
    from pbdata.dataset.splits import (
        build_pair_aware_splits,
        build_splits,
        cluster_aware_split,
        save_splits,
    )

    layout = _storage_layout(ctx)
    if split_mode not in {"auto", "pair-aware", "legacy-sequence", "hash"}:
        raise typer.BadParameter("split-mode must be one of: auto, pair-aware, legacy-sequence, hash")
    processed_dir = layout.processed_rcsb_dir
    files = sorted(processed_dir.glob("*.json")) if processed_dir.exists() else []
    pair_items = _pair_split_items_from_layout(layout)
    prefer_pair_aware = split_mode == "pair-aware" or (
        split_mode == "auto" and bool(pair_items) and not hash_only
    )

    if prefer_pair_aware:
        typer.echo(f"Building pair-aware grouped splits from {len(pair_items):,} pair-level items...")
        result, extra_metadata = build_pair_aware_splits(
            pair_items,
            train_frac=train_frac,
            val_frac=val_frac,
            seed=seed,
            threshold=threshold,
            log_fn=typer.echo,
        )
        strategy = "pair_aware_grouped"
        save_splits(result, layout.splits_dir, seed=seed, strategy=strategy, extra_metadata=extra_metadata)
        sizes = result.sizes()
        typer.echo(f"Train: {sizes['train']:,}  Val: {sizes['val']:,}  Test: {sizes['test']:,}")
        typer.echo(f"Storage root: {layout.root}")
        typer.echo(f"Splits written to {layout.splits_dir}/")
        return

    if not files:
        typer.echo(f"No processed records found in {processed_dir}. Run 'normalize' first.")
        return

    typer.echo(f"Loading {len(files):,} processed records...")
    sample_ids: list[str] = []
    sequences:  list[str | None] = []
    for f in files:
        try:
            raw = json.loads(f.read_text())
            sample_ids.append(raw["sample_id"])
            sequences.append(raw.get("sequence_receptor"))
        except Exception as exc:
            logger.warning("Skipping %s: %s", f.name, exc)

    has_sequences = any(s is not None for s in sequences)

    use_hash = hash_only or split_mode == "hash" or not has_sequences
    if use_hash:
        strategy = "hash"
        typer.echo(
            f"Building hash-based splits "
            f"(train={train_frac:.0%}, val={val_frac:.0%}, "
            f"test={1-train_frac-val_frac:.0%}, seed={seed})..."
        )
        result = build_splits(sample_ids, train_frac=train_frac, val_frac=val_frac, seed=seed)
    else:
        strategy = "cluster_aware"
        typer.echo(
            f"Building cluster-aware splits "
            f"(train={train_frac:.0%}, val={val_frac:.0%}, "
            f"test={1-train_frac-val_frac:.0%}, threshold={threshold}, seed={seed})..."
        )
        result = cluster_aware_split(
            sample_ids, sequences,
            train_frac=train_frac, val_frac=val_frac, seed=seed,
            threshold=threshold, log_fn=typer.echo,
        )

    save_splits(result, layout.splits_dir, seed=seed, strategy=strategy)

    sizes = result.sizes()
    typer.echo(f"Train: {sizes['train']:,}  Val: {sizes['val']:,}  Test: {sizes['test']:,}")
    typer.echo(f"Storage root: {layout.root}")
    typer.echo(f"Splits written to {layout.splits_dir}/")


@app.command("extract")
def extract_cmd(
    ctx: typer.Context,
    output: Annotated[
        Optional[Path],
        typer.Option("--output", "-o", help="Override default output directory."),
    ] = None,
    structures: Annotated[
        Optional[Path],
        typer.Option("--structures", help="Override default structures directory."),
    ] = None,
    download_pdb: Annotated[
        bool,
        typer.Option("--download-pdb", help="Also download PDB format files."),
    ] = False,
    download_structures: Annotated[
        bool,
        typer.Option("--download-structures/--no-download-structures",
                     help="Download mmCIF structure files."),
    ] = True,
    workers: Annotated[
        int,
        typer.Option("--workers", min=0, help="Worker count (0 = CPU count)."),
    ] = 1,
) -> None:
    """Extract multi-table records from raw RCSB data.

    Produces six output tables per the structure extraction spec:
      entry/       - one record per PDB entry
      chains/      - one record per chain/entity assignment
      bound_objects/ - one record per bound object
      interfaces/  - one record per interface
      assays/      - one record per assay measurement
      provenance/  - per-field provenance trail

    Also downloads mmCIF files to data/structures/rcsb/ (unless --no-download-structures).
    """
    from pbdata.master_export import refresh_master_exports
    from pbdata.pipeline.extract import extract_rcsb_entry, write_records_json
    from pbdata.sources.rcsb_search import fetch_chemcomp_descriptors

    layout = _storage_layout(ctx)
    raw_dir = layout.raw_rcsb_dir
    out_dir = output if output is not None else layout.extracted_dir
    struct_dir = structures if structures is not None else layout.structures_rcsb_dir

    files = sorted(raw_dir.glob("*.json"))
    if not files:
        typer.echo(f"No raw files found in {raw_dir}. Run 'ingest' first.")
        return

    cfg: AppConfig = ctx.obj.get("config", AppConfig())
    structure_mirror = str(cfg.sources.rcsb.extra.get("structure_mirror") or "rcsb").strip().lower()
    assay_samples_by_pdb = _load_external_assay_samples(cfg, layout=layout)

    # Collect ligand comp_ids for batch descriptor fetch
    typer.echo(f"Scanning {len(files):,} RCSB records for ligand IDs...")
    comp_ids: list[str] = []
    raw_data: list[tuple[Path, dict]] = []
    for f in files:
        try:
            raw = json.loads(f.read_text())
            raw_data.append((f, raw))
            for ent in (raw.get("nonpolymer_entities") or []):
                cid = (
                    ((ent.get("nonpolymer_comp") or {}).get("chem_comp") or {})
                    .get("id", "")
                )
                if cid:
                    comp_ids.append(cid)
        except Exception as exc:
            logger.warning("Failed to read %s: %s", f.name, exc)

    chem_descriptors: dict[str, dict[str, str]] = {}
    if comp_ids:
        unique = list(dict.fromkeys(comp_ids))
        typer.echo(f"Fetching chem-comp descriptors for {len(unique):,} ligands...")
        try:
            chem_descriptors = fetch_chemcomp_descriptors(unique)
            typer.echo(f"  Got descriptors for {len(chem_descriptors):,} ligands.")
        except Exception as exc:
            logger.warning("Chem-comp fetch failed: %s", exc)

    typer.echo(f"Extracting {len(raw_data):,} entries to multi-table records...")
    ok = cached = failed = 0
    worker_count = _coerce_workers(workers)

    def _extract_one(item: tuple[Path, dict]) -> tuple[str, str]:
        path, raw = item
        pdb_id = str(raw.get("rcsb_id") or "").upper()
        if _validate_extracted_bundle(out_dir, pdb_id) and _is_up_to_date(path, out_dir / "entry" / f"{pdb_id}.json"):
            return path.name, "cached"
        _delete_extracted_bundle(out_dir, pdb_id)
        chembl_samples = _fetch_chembl_samples_for_raw(raw, chem_descriptors, cfg)
        bindingdb_samples = _fetch_bindingdb_samples_for_pdb(pdb_id, cfg, layout=layout)
        records = extract_rcsb_entry(
            raw,
            chem_descriptors=chem_descriptors,
            assay_samples=assay_samples_by_pdb.get(pdb_id, []) + bindingdb_samples + chembl_samples,
            structures_dir=struct_dir if download_structures else None,
            download_structures=download_structures,
            download_pdb=download_pdb,
            structure_mirror=structure_mirror,
        )
        write_records_json(records, out_dir)
        return path.name, "ok"

    processed_count = 0
    if worker_count == 1:
        for item in raw_data:
            try:
                _, status = _extract_one(item)
                if status == "cached":
                    cached += 1
                else:
                    ok += 1
            except Exception as exc:
                logger.warning("Failed to extract %s: %s", item[0].name, exc)
                failed += 1
            processed_count += 1
            if processed_count % 100 == 0:
                typer.echo(f"  {processed_count:,}/{len(raw_data):,} processed...")
    else:
        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            futures = {executor.submit(_extract_one, item): item[0].name for item in raw_data}
            for future in as_completed(futures):
                name = futures[future]
                try:
                    _, status = future.result()
                    if status == "cached":
                        cached += 1
                    else:
                        ok += 1
                except Exception as exc:
                    logger.warning("Failed to extract %s: %s", name, exc)
                    failed += 1
                processed_count += 1
                if processed_count % 100 == 0:
                    typer.echo(f"  {processed_count:,}/{len(raw_data):,} processed...")

    state_path = write_stage_state(
        layout,
        stage="extract",
        status="completed" if failed == 0 else "completed_with_failures",
        input_dir=raw_dir,
        output_dir=out_dir,
        workers=worker_count,
        counts={
            "inputs": len(raw_data),
            "extracted": ok,
            "cached": cached,
            "failed": failed,
        },
        notes=(
            "Valid extracted bundles were reused when newer than raw JSON. "
            "BindingDB, ChEMBL, PDBbind, BioLiP, and SKEMPI enrichment may contribute assays when configured."
        ),
    )

    typer.echo(f"Extraction complete. OK: {ok:,}, Cached: {cached:,}, Failed: {failed:,}")
    typer.echo(f"Storage root: {layout.root}")
    typer.echo(f"Output: {out_dir}/")
    if download_structures:
        typer.echo(f"Structures: {struct_dir}/")
    typer.echo(f"Stage state: {state_path}")
    export_status = refresh_master_exports(layout)
    if "master_csv" in export_status:
        typer.echo(f"Master CSV: {export_status['master_csv']}")
    if "pair_csv" in export_status:
        typer.echo(f"Pair CSV: {export_status['pair_csv']}")
    if "issue_csv" in export_status:
        typer.echo(f"Issue CSV: {export_status['issue_csv']}")
    if "conflict_csv" in export_status:
        typer.echo(f"Conflict CSV: {export_status['conflict_csv']}")
    if "source_state_csv" in export_status:
        typer.echo(f"Source State CSV: {export_status['source_state_csv']}")
    if "model_ready_pairs_csv" in export_status:
        typer.echo(f"Model-ready CSV: {export_status['model_ready_pairs_csv']}")
    if "scientific_coverage_json" in export_status:
        typer.echo(f"Coverage JSON: {export_status['scientific_coverage_json']}")
    if "release_manifest_json" in export_status:
        typer.echo(f"Release Manifest: {export_status['release_manifest_json']}")
    if "master_csv_error" in export_status:
        typer.echo(f"Master CSV refresh warning: {export_status['master_csv_error']}")
    if "pair_csv_error" in export_status:
        typer.echo(f"Pair CSV refresh warning: {export_status['pair_csv_error']}")
    if "issue_csv_error" in export_status:
        typer.echo(f"Issue CSV refresh warning: {export_status['issue_csv_error']}")
    if "conflict_csv_error" in export_status:
        typer.echo(f"Conflict CSV refresh warning: {export_status['conflict_csv_error']}")
    if "source_state_csv_error" in export_status:
        typer.echo(f"Source State CSV refresh warning: {export_status['source_state_csv_error']}")
    if "release_exports_error" in export_status:
        typer.echo(f"Release export refresh warning: {export_status['release_exports_error']}")


if __name__ == "__main__":
    app()
