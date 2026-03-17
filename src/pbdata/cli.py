import json
import logging
import os
import threading
import time
import csv
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from collections import defaultdict
from pathlib import Path
from typing import Annotated, Optional

import typer

from pbdata.cli_reporting import (
    emit_labeled_values,
    render_demo_readiness_report,
    render_doctor_report,
    render_status_report,
)
from pbdata.config import AppConfig, load_config
from pbdata.demo_workspace import seed_demo_workspace
from pbdata.file_health import remove_problem_json_files, scan_json_directory
from pbdata.logging_config import setup_logging
from pbdata.pairing import parse_pair_identity_key
from pbdata.pipeline.canonical_workflows import (
    run_audit_processed_records,
    run_normalize_rcsb,
    run_processed_report,
    run_rcsb_ingest,
    run_skempi_ingest,
)
from pbdata.schemas.canonical_sample import CanonicalBindingSample
from pbdata.source_state import export_source_state_run_summary, snapshot_source_state_counters, write_source_state
from pbdata.stage_state import stage_lock, write_stage_state
from pbdata.storage import (
    StorageLayout,
    build_storage_layout,
)
from pbdata.storage_packaging import (
    consolidate_extracted_tables,
    package_raw_rcsb_records,
    unpack_raw_rcsb_package,
)
from pbdata.storage_audit import build_storage_usage_report, render_storage_usage_report
from pbdata.storage_prune import (
    build_storage_prune_plan,
    prune_storage,
    render_storage_prune_plan,
    render_storage_prune_result,
)
from pbdata.table_io import load_json_rows, load_table_json
from pbdata.workspace_state import (
    build_demo_readiness_report as build_demo_readiness_state_report,
    build_doctor_report as build_doctor_state_report,
    build_status_report as build_status_state_report,
)
from pbdata.precompute import (
    build_precompute_run_status,
    merge_precompute_shards,
    plan_precompute_run,
    run_precompute_shard,
)

app = typer.Typer(help="Protein binding dataset platform CLI.")

_DEFAULT_CONFIG      = Path("configs/sources.yaml")
_DEFAULT_LOG_CONFIG  = Path("configs/logging.yaml")
_DEFAULT_CRITERIA    = Path("configs/criteria.yaml")
logger = logging.getLogger(__name__)

_EXTRACT_PROGRESS_EVERY = 100
_EXTRACT_HEARTBEAT_SECONDS = 15.0
_EXTRACT_STAGE_STATE_UPDATE_SECONDS = 30.0
_EXTRACT_ACTIVE_SAMPLE_LIMIT = 3


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


def _path_exists_safe(path: Path) -> bool:
    try:
        return path.exists()
    except OSError as exc:
        logger.warning("Path availability check failed for %s: %s", path, exc)
        return False


def _resolve_latest_feature_pipeline_run_id(layout: StorageLayout, explicit_run_id: Optional[str]) -> str:
    if explicit_run_id:
        return explicit_run_id
    manifests = sorted(
        layout.artifact_manifests_dir.glob("*_input_manifest.json"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    if not manifests:
        typer.echo("Error: --run-id is required until at least one site-centric feature run exists.")
        raise typer.Exit(code=1)
    return manifests[0].name.replace("_input_manifest.json", "")


def _validate_processed_record_json(raw: dict[str, object]) -> None:
    CanonicalBindingSample.model_validate(raw)


def _emit_feature_workflow_output(layout: StorageLayout, items: list[tuple[str, object]]) -> None:
    typer.echo(f"Storage root: {layout.root}")
    for label, value in items:
        typer.echo(f"{label}: {value}")


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


def _render_precompute_status(status: dict[str, object]) -> None:
    typer.echo(f"Run ID: {status.get('run_id')}")
    typer.echo(f"Stage: {status.get('stage')}")
    typer.echo(f"Status: {status.get('status')}")
    typer.echo(f"Storage root: {status.get('storage_root')}")
    typer.echo(f"Run dir: {status.get('run_dir')}")
    typer.echo(
        "Chunks: "
        f"{status.get('completed_chunks')}/{status.get('chunk_count')} completed, "
        f"{status.get('failed_chunks')} failed"
    )
    typer.echo(
        "Work: "
        f"inputs={status.get('total_inputs')} | processed={status.get('processed')} | "
        f"ok={status.get('ok')} | cached={status.get('cached')} | failed={status.get('failed')}"
    )


def _fetch_bindingdb_samples_for_pdb(
    pdb_id: str,
    config: AppConfig,
    *,
    layout: StorageLayout,
    raw: dict | None = None,
) -> list:
    from pbdata.pipeline.enrichment import fetch_bindingdb_samples_for_pdb

    return fetch_bindingdb_samples_for_pdb(pdb_id, config, layout=layout, raw=raw)


def _delete_extracted_bundle(output_dir: Path, pdb_id: str) -> None:
    for table_name in ["entry", "chains", "bound_objects", "interfaces", "assays", "provenance"]:
        (output_dir / table_name / f"{pdb_id}.json").unlink(missing_ok=True)


def _load_json_rows(path: Path) -> list[dict]:
    return load_json_rows(path, logger=logger, warning_prefix="Skipping unreadable CLI input")


def _load_table_rows(table_dir: Path) -> list[dict]:
    return load_table_json(table_dir, logger=logger, warning_prefix="Skipping unreadable CLI input")


def _read_metadata_rows(layout: StorageLayout) -> list[dict[str, str]]:
    path = layout.workspace_metadata_dir / "protein_metadata.csv"
    if not path.exists():
        return []
    try:
        with path.open(newline="", encoding="utf-8") as handle:
            import csv

            return list(csv.DictReader(handle))
    except OSError:
        return []


def _read_repo_csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    try:
        with path.open(newline="", encoding="utf-8") as handle:
            return list(csv.DictReader(handle))
    except OSError:
        return []


def _normalize_delimited_values(raw: object, *, fallback: str = "") -> list[str]:
    text = str(raw or "")
    values = [
        value.strip()
        for chunk in text.replace(";", ",").split(",")
        for value in [chunk.strip()]
        if value.strip()
    ]
    deduped = list(dict.fromkeys(values))
    return deduped or ([fallback] if fallback else [])


def _count_extracted_assay_rows(extracted_dir: Path) -> int:
    return len(_load_table_rows(extracted_dir / "assays"))


def _existing_extracted_pdb_ids(output_dir: Path) -> set[str]:
    entry_dir = output_dir / "entry"
    if not entry_dir.exists():
        return set()
    return {path.stem.upper() for path in entry_dir.glob("*.json")}


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
    *,
    layout: StorageLayout | None = None,
) -> list:
    from pbdata.pipeline.enrichment import fetch_chembl_samples_for_raw

    return fetch_chembl_samples_for_raw(raw, chem_descriptors, config, layout=layout)


def _count_delimited_rows(path: Path, delimiter: str = ",") -> int | None:
    try:
        with path.open(encoding="utf-8", newline="") as f:
            lines = [line for line in f.read().splitlines() if line.strip()]
        return max(len(lines) - 1, 0)
    except OSError:
        return None


def _emit_planned_manifest_notice(
    *,
    artifact_label: str,
    manifest_path: Path,
    missing_steps: list[str],
) -> None:
    typer.echo(f"Storage root: {manifest_path.parents[2] if len(manifest_path.parents) >= 3 else manifest_path.parent}")
    typer.echo(f"{artifact_label} manifest written to {manifest_path}")
    typer.echo("Prerequisites missing; wrote a planned manifest only.")
    for step in missing_steps:
        typer.echo(f"  - {step}")


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
        log_path = _DEFAULT_LOG_CONFIG if _path_exists_safe(_DEFAULT_LOG_CONFIG) else None
    setup_logging(log_path)

    cfg_path: Optional[Path] = config
    if cfg_path is None:
        if _path_exists_safe(_DEFAULT_CONFIG):
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
    layout = _storage_layout(ctx)

    if source_lower == "rcsb":
        criteria_path = criteria if criteria is not None else _DEFAULT_CRITERIA
        probe = run_rcsb_ingest(
            layout=layout,
            criteria_path=criteria_path,
            dry_run=True,
            output_dir=output,
            log_fn=typer.echo,
        )
        typer.echo(f"Found {probe.match_count:,} RCSB entries matching criteria.")
        if dry_run:
            return
        if not yes:
            typer.confirm(f"Proceed with downloading {probe.match_count:,} entries?", abort=True)
        run_rcsb_ingest(
            layout=layout,
            criteria_path=criteria_path,
            dry_run=False,
            output_dir=output,
            log_fn=typer.echo,
        )
        typer.echo("RCSB ingest complete.")
    elif source_lower == "skempi":
        cached_probe = run_skempi_ingest(layout=layout, dry_run=True, output_dir=output)
        if cached_probe.status == "cached":
            typer.echo(f"SKEMPI CSV already present at {cached_probe.csv_path}.  Skipping download.")
            return
        typer.echo("SKEMPI v2 will be downloaded from https://life.bsc.es/pid/skempi2/database/download/SKEMPI2_PDBs.tgz")
        typer.echo("File size is approximately 3 MB.")
        if dry_run:
            typer.echo("[dry-run] Would download SKEMPI CSV - skipping.")
            return
        if not yes:
            typer.confirm("Proceed with downloading SKEMPI v2 CSV?", abort=True)
        result = run_skempi_ingest(layout=layout, dry_run=False, output_dir=output)
        typer.echo(f"SKEMPI CSV saved to {result.csv_path}")
        if result.catalog_path is not None:
            typer.echo(f"Download manifest updated at {result.catalog_path}")
        typer.echo("Run 'extract' with SKEMPI enabled to merge mutation-ddG assays into extracted tables.")
    else:
        typer.echo(
            f"Unknown source: '{source}'.  Supported: rcsb, skempi.",
            err=True,
        )
        raise typer.Exit(code=1)

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
    layout = _storage_layout(ctx)
    result = run_normalize_rcsb(
        layout=layout,
        workers=workers,
        progress_fn=typer.echo,
    )
    if result is None:
        typer.echo(f"No raw files found in {layout.raw_rcsb_dir}. Run 'ingest' first.")
        return
    typer.echo(f"Done. Normalized: {result.normalized:,}, Cached: {result.cached:,}, Failed: {result.failed:,}")
    if result.unreadable_inputs:
        typer.echo(
            f"Unreadable raw inputs skipped: {result.unreadable_inputs:,} "
            f"(run `pbdata clean --raw --delete` to remove them)."
        )
    typer.echo(f"Storage root: {layout.root}")
    typer.echo(f"Output: {result.output_dir}")
    typer.echo(f"Stage state: {result.state_path}")


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
    layout = _storage_layout(ctx)
    result = run_audit_processed_records(
        layout=layout,
        workers=workers,
        progress_fn=typer.echo,
    )
    if result is None:
        typer.echo(f"No processed records found in {layout.processed_rcsb_dir}. Run 'normalize' first.")
        return

    typer.echo(f"Audit complete. Mean quality score: {result.mean_quality_score:.3f}")
    typer.echo(f"Top flags: {result.top_flags}")
    if result.unreadable_inputs:
        typer.echo(
            f"Unreadable/invalid processed records skipped: {result.unreadable_inputs:,} "
            f"(run `pbdata clean --processed --delete` to remove them)."
        )
    typer.echo(f"Storage root: {layout.root}")
    typer.echo(f"Summary written to {result.summary_path}")
    typer.echo(f"Stage state: {result.state_path}")


# ---------------------------------------------------------------------------
# report
# ---------------------------------------------------------------------------

@app.command()
def report(ctx: typer.Context) -> None:
    """Generate a summary statistics report over all processed records."""
    layout = _storage_layout(ctx)
    result = run_processed_report(layout=layout, progress_fn=typer.echo)
    if result is None:
        typer.echo(f"No processed records found in {layout.processed_rcsb_dir}. Run 'normalize' first.")
        return

    typer.echo(f"\n{'-'*40}")
    typer.echo(f"Total records   : {result.total_records:,}")
    typer.echo(f"Task types      : {result.task_type_counts}")
    typer.echo(f"Methods         : {result.top_methods}")
    typer.echo(f"Mean quality    : {result.mean_quality_score:.3f}" if result.mean_quality_score is not None else "")
    if result.parse_failures:
        typer.echo(
            f"Skipped invalid : {result.parse_failures:,} "
            f"(run `pbdata clean --processed --delete` to remove corrupt files)"
        )
    typer.echo(f"Storage root    : {layout.root}")
    typer.echo(f"Report written to {result.report_path}")
    if "master_csv" in result.export_status:
        typer.echo(f"Master CSV      : {result.export_status['master_csv']}")
    if "pair_csv" in result.export_status:
        typer.echo(f"Pair CSV        : {result.export_status['pair_csv']}")
    if "issue_csv" in result.export_status:
        typer.echo(f"Issue CSV       : {result.export_status['issue_csv']}")
    if "conflict_csv" in result.export_status:
        typer.echo(f"Conflict CSV    : {result.export_status['conflict_csv']}")
    if "source_state_csv" in result.export_status:
        typer.echo(f"Source State CSV: {result.export_status['source_state_csv']}")
    if "model_ready_pairs_csv" in result.export_status:
        typer.echo(f"Model-ready CSV : {result.export_status['model_ready_pairs_csv']}")
    if "scientific_coverage_json" in result.export_status:
        typer.echo(f"Coverage JSON   : {result.export_status['scientific_coverage_json']}")
    if "release_manifest_json" in result.export_status:
        typer.echo(f"Release Manifest : {result.export_status['release_manifest_json']}")
    if "master_csv_error" in result.export_status:
        typer.echo(f"Master CSV refresh warning: {result.export_status['master_csv_error']}")
    if "pair_csv_error" in result.export_status:
        typer.echo(f"Pair CSV refresh warning: {result.export_status['pair_csv_error']}")
    if "issue_csv_error" in result.export_status:
        typer.echo(f"Issue CSV refresh warning: {result.export_status['issue_csv_error']}")
    if "conflict_csv_error" in result.export_status:
        typer.echo(f"Conflict CSV refresh warning: {result.export_status['conflict_csv_error']}")
    if "source_state_csv_error" in result.export_status:
        typer.echo(f"Source State CSV refresh warning: {result.export_status['source_state_csv_error']}")
    if "release_exports_error" in result.export_status:
        typer.echo(f"Release export refresh warning: {result.export_status['release_exports_error']}")


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
    layout = _storage_layout(ctx)
    render_status_report(build_status_state_report(layout))


@app.command("clean")
def clean_cmd(
    ctx: typer.Context,
    processed: Annotated[
        bool,
        typer.Option("--processed/--no-processed", help="Scan processed JSON files."),
    ] = True,
    raw: Annotated[
        bool,
        typer.Option("--raw/--no-raw", help="Also scan raw JSON files."),
    ] = False,
    delete: Annotated[
        bool,
        typer.Option("--delete/--dry-run", help="Delete corrupt files instead of reporting only."),
    ] = False,
) -> None:
    """Scan for empty/corrupt JSON files and optionally remove them."""
    layout = _storage_layout(ctx)
    targets: list[tuple[str, Path, object]] = []
    if processed:
        targets.append(("processed", layout.processed_rcsb_dir, _validate_processed_record_json))
    if raw:
        targets.append(("raw", layout.raw_rcsb_dir, None))
    if not targets:
        typer.echo("Nothing to scan. Enable --processed and/or --raw.")
        raise typer.Exit(code=1)

    total_removed = 0
    total_problem = 0
    for label, directory, validator in targets:
        summary = scan_json_directory(directory, validator=validator)
        total_problem += summary.problem_count
        typer.echo(
            f"{label.title()} JSON health: total={summary.total_count:,}, valid={summary.valid_count:,}, "
            f"problems={summary.problem_count:,} (empty={summary.empty_count:,}, "
            f"corrupt_or_invalid={summary.corrupt_count + summary.invalid_count:,})"
        )
        if summary.sample_problem_files:
            typer.echo(f"Problem examples: {', '.join(summary.sample_problem_files[:5])}")
        if delete and summary.problem_count:
            removed = remove_problem_json_files(directory, validator=validator)
            total_removed += len(removed)
            typer.echo(f"Removed {len(removed):,} file(s) from {directory}")
    typer.echo(f"Storage root: {layout.root}")
    if delete:
        typer.echo(f"Removed files: {total_removed:,}")
    else:
        typer.echo(f"Detected problem files: {total_problem:,}")


@app.command("doctor")
def doctor_cmd(ctx: typer.Context) -> None:
    """Check dependency and configuration readiness for the current installation."""
    layout = _storage_layout(ctx)
    cfg: AppConfig = ctx.obj["config"]
    render_doctor_report(layout.root, build_doctor_state_report(layout, cfg))


@app.command("demo-readiness")
def demo_readiness_cmd(ctx: typer.Context) -> None:
    """Assess whether the current workspace is presentable for an internal demo."""    
    layout = _storage_layout(ctx)
    cfg: AppConfig = ctx.obj["config"]
    render_demo_readiness_report(layout.root, build_demo_readiness_state_report(layout, cfg))


@app.command("export-demo-snapshot")
def export_demo_snapshot_cmd(ctx: typer.Context) -> None:
    """Write JSON and markdown artifacts for a canned internal demo walkthrough."""
    from pbdata.demo import export_demo_snapshot

    layout = _storage_layout(ctx)
    cfg: AppConfig = ctx.obj["config"]
    json_path, md_path, report = export_demo_snapshot(layout, cfg)
    emit_labeled_values([
        ("Storage root", layout.root),
        ("Demo readiness", report["readiness"]),
        ("JSON snapshot", json_path),
        ("Markdown guide", md_path),
    ])


@app.command("seed-demo-workspace")
def seed_demo_workspace_cmd(
    ctx: typer.Context,
    force: Annotated[
        bool,
        typer.Option("--force", help="Overwrite and refresh an existing seeded demo workspace."),
    ] = False,
) -> None:
    """Seed a convincing simulated workspace so Demo Mode can showcase the intended workflow instantly."""
    layout = _storage_layout(ctx)
    cfg: AppConfig = ctx.obj["config"]
    result = seed_demo_workspace(layout, cfg, repo_root=Path.cwd(), force=force)
    emit_labeled_values([
        ("Storage root", layout.root),
        ("Seeded", "yes" if result.seeded else "already_present"),
        ("Demo manifest", result.manifest_path),
        ("Demo readiness", result.report_path),
        ("Demo walkthrough", result.walkthrough_path),
    ])


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


@app.command("plan-precompute")
def plan_precompute_cmd(
    ctx: typer.Context,
    stage: Annotated[
        str,
        typer.Option("--stage", help="Shard-aware preprocessing stage to plan."),
    ] = "extract",
    chunk_size: Annotated[
        int,
        typer.Option("--chunk-size", min=1, help="Approximate number of inputs per chunk."),
    ] = 500,
    chunk_count: Annotated[
        Optional[int],
        typer.Option("--chunk-count", min=1, help="Optional explicit chunk count."),
    ] = None,
    run_id: Annotated[
        Optional[str],
        typer.Option("--run-id", help="Optional explicit run identifier."),
    ] = None,
    input_package: Annotated[
        Optional[Path],
        typer.Option(
            "--input-package",
            help="Optional package dir or manifest for extract planning (e.g. packaged raw_rcsb shards).",
        ),
    ] = None,
) -> None:
    """Plan a generic shard-aware preprocessing run."""
    layout = _storage_layout(ctx)
    try:
        result = plan_precompute_run(
            layout,
            stage=stage,
            chunk_size=chunk_size,
            chunk_count=chunk_count,
            run_id=run_id,
            input_package=input_package,
        )
    except Exception as exc:
        typer.echo(f"Error: {exc}")
        raise typer.Exit(code=1) from exc

    typer.echo("Precompute plan created.")
    typer.echo(f"Run ID: {result.run_id}")
    typer.echo(f"Stage: {result.stage}")
    typer.echo(f"Run dir: {result.run_dir}")
    typer.echo(f"Run manifest: {result.manifest_path}")
    typer.echo(f"Chunk dir: {result.chunk_dir}")
    typer.echo(f"Chunks: {result.chunk_count}")
    typer.echo(f"Inputs: {result.total_inputs}")


@app.command("run-precompute-shard")
def run_precompute_shard_cmd(
    ctx: typer.Context,
    run_id: Annotated[
        str,
        typer.Option("--run-id", help="Planned precompute run identifier."),
    ],
    chunk_index: Annotated[
        int,
        typer.Option("--chunk-index", min=0, help="Chunk index to execute."),
    ],
    workers: Annotated[
        int,
        typer.Option("--workers", min=0, help="Worker count (0 = CPU count)."),
    ] = 1,
    force: Annotated[
        bool,
        typer.Option("--force", help="Rebuild shard outputs even when cached outputs exist."),
    ] = False,
    download_pdb: Annotated[
        bool,
        typer.Option("--download-pdb", help="Also download PDB format files."),
    ] = False,
    download_structures: Annotated[
        bool,
        typer.Option("--download-structures/--no-download-structures",
                     help="Download mmCIF structure files for extraction shards."),
    ] = True,
    graph_level: Annotated[
        str,
        typer.Option("--graph-level", help="Structural graph level for build-structural-graphs shards."),
    ] = "residue",
    scope: Annotated[
        str,
        typer.Option("--scope", help="Structural graph scope for build-structural-graphs shards."),
    ] = "whole_protein",
    shell_radius: Annotated[
        float,
        typer.Option("--shell-radius", help="Structural graph shell radius for build-structural-graphs shards."),
    ] = 8.0,
    export_formats: Annotated[
        list[str] | None,
        typer.Option("--export-format", help="Repeatable structural graph export format."),
    ] = None,
) -> None:
    """Execute one planned preprocessing shard."""
    layout = _storage_layout(ctx)
    cfg: AppConfig = ctx.obj.get("config", AppConfig())
    try:
        result = run_precompute_shard(
            layout,
            run_id=run_id,
            chunk_index=chunk_index,
            config=cfg,
            workers=workers,
            force=force,
            download_structures=download_structures,
            download_pdb=download_pdb,
            graph_level=graph_level,
            scope=scope,
            shell_radius=shell_radius,
            export_formats=tuple(export_formats or ["pyg", "networkx"]),
            log_fn=typer.echo,
        )
    except Exception as exc:
        typer.echo(f"Error: {exc}")
        raise typer.Exit(code=1) from exc

    typer.echo("Precompute shard complete.")
    typer.echo(f"Run ID: {result.run_id}")
    typer.echo(f"Chunk index: {result.chunk_index}")
    typer.echo(f"Shard dir: {result.shard_dir}")
    typer.echo(f"Status file: {result.status_path}")
    typer.echo(
        f"Processed={result.processed:,} | ok={result.ok:,} | cached={result.cached:,} | failed={result.failed:,}"
    )


@app.command("merge-precompute-shards")
def merge_precompute_shards_cmd(
    ctx: typer.Context,
    run_id: Annotated[
        str,
        typer.Option("--run-id", help="Planned precompute run identifier."),
    ],
) -> None:
    """Merge shard-local outputs into reusable workspace outputs."""
    layout = _storage_layout(ctx)
    try:
        result = merge_precompute_shards(layout, run_id=run_id)
    except Exception as exc:
        typer.echo(f"Error: {exc}")
        raise typer.Exit(code=1) from exc

    typer.echo("Precompute shard merge complete.")
    typer.echo(f"Run ID: {result.run_id}")
    typer.echo(f"Stage: {result.stage}")
    typer.echo(f"Merged dir: {result.merged_dir}")
    typer.echo(f"Merge manifest: {result.manifest_path}")
    typer.echo(f"Copied files: {result.copied:,}")


@app.command("report-precompute-run-status")
def report_precompute_run_status_cmd(
    ctx: typer.Context,
    run_id: Annotated[
        str,
        typer.Option("--run-id", help="Planned precompute run identifier."),
    ],
) -> None:
    """Report shard-level completion state for a planned precompute run."""
    layout = _storage_layout(ctx)
    try:
        status = build_precompute_run_status(layout, run_id=run_id)
    except Exception as exc:
        typer.echo(f"Error: {exc}")
        raise typer.Exit(code=1) from exc
    _render_precompute_status(status)


@app.command("package-raw-rcsb")
def package_raw_rcsb_cmd(
    ctx: typer.Context,
    shard_size: Annotated[
        int,
        typer.Option("--shard-size", min=1, help="Approximate raw records per gzipped JSONL shard."),
    ] = 5000,
    package_id: Annotated[
        Optional[str],
        typer.Option("--package-id", help="Optional explicit package identifier."),
    ] = None,
) -> None:
    """Package raw RCSB JSON files into gzipped JSONL shards for transfer/HPC use."""
    layout = _storage_layout(ctx)
    try:
        result = package_raw_rcsb_records(layout, shard_size=shard_size, package_id=package_id)
    except Exception as exc:
        typer.echo(f"Error: {exc}")
        raise typer.Exit(code=1) from exc

    typer.echo("Raw RCSB packaging complete.")
    typer.echo(f"Package ID: {result.package_id}")
    typer.echo(f"Storage root: {layout.root}")
    typer.echo(f"Package dir: {result.package_dir}")
    typer.echo(f"Manifest: {result.manifest_path}")
    typer.echo(f"Shards: {result.shard_count:,}")
    typer.echo(f"Readable records: {result.total_records:,}")
    typer.echo(f"Unreadable records skipped: {result.unreadable_records:,}")


@app.command("unpack-raw-rcsb-package")
def unpack_raw_rcsb_package_cmd(
    ctx: typer.Context,
    package: Annotated[
        Path,
        typer.Option("--package", exists=True, file_okay=True, dir_okay=True, help="Package dir or manifest.json path."),
    ],
    output_dir: Annotated[
        Optional[Path],
        typer.Option("--output-dir", help="Optional alternate output directory for restored raw JSON files."),
    ] = None,
    overwrite: Annotated[
        bool,
        typer.Option("--overwrite", help="Overwrite existing raw JSON files."),
    ] = False,
) -> None:
    """Restore raw RCSB JSON files from a packaged shard archive."""
    layout = _storage_layout(ctx)
    try:
        result = unpack_raw_rcsb_package(layout, package=package, output_dir=output_dir, overwrite=overwrite)
    except Exception as exc:
        typer.echo(f"Error: {exc}")
        raise typer.Exit(code=1) from exc

    typer.echo("Raw RCSB package unpack complete.")
    typer.echo(f"Package ID: {result.package_id}")
    typer.echo(f"Output dir: {result.output_dir}")
    typer.echo(f"Restored records: {result.restored_records:,}")


@app.command("consolidate-extracted")
def consolidate_extracted_cmd(
    ctx: typer.Context,
    shard_size: Annotated[
        int,
        typer.Option("--shard-size", min=1, help="Approximate extracted records per gzipped JSONL shard."),
    ] = 5000,
    run_id: Annotated[
        Optional[str],
        typer.Option("--run-id", help="Optional explicit consolidation run identifier."),
    ] = None,
) -> None:
    """Consolidate per-PDB extracted tables into gzipped JSONL shard stores."""
    layout = _storage_layout(ctx)
    try:
        result = consolidate_extracted_tables(layout, shard_size=shard_size, run_id=run_id)
    except Exception as exc:
        typer.echo(f"Error: {exc}")
        raise typer.Exit(code=1) from exc

    typer.echo("Extracted-table consolidation complete.")
    typer.echo(f"Run ID: {result.run_id}")
    typer.echo(f"Storage root: {layout.root}")
    typer.echo(f"Output dir: {result.output_dir}")
    typer.echo(f"Manifest: {result.manifest_path}")
    typer.echo(f"Tables: {result.table_count:,}")
    typer.echo(f"Readable records: {result.total_records:,}")
    typer.echo(f"Unreadable records skipped: {result.unreadable_records:,}")


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


@app.command("train-tabular-affinity-model")
def train_tabular_affinity_model_cmd(ctx: typer.Context) -> None:
    """Train a lightweight supervised tabular affinity model on training examples."""
    from pbdata.models.tabular_affinity import train_tabular_affinity_model

    layout = _storage_layout(ctx)
    out_path, manifest = train_tabular_affinity_model(layout)
    typer.echo(f"Storage root: {layout.root}")
    typer.echo(f"Tabular affinity model artifact written to {out_path}")
    typer.echo(f"Workflow status: {manifest['status']}")


@app.command("train-recommended-model")
def train_recommended_model_cmd(
    ctx: typer.Context,
    runtime_target: Annotated[
        str,
        typer.Option("--runtime-target", help="local_cpu | local_gpu"),
    ] = "local_cpu",
    execution_strategy: Annotated[
        str,
        typer.Option("--execution-strategy", help="auto | prefer_native | safe_baseline"),
    ] = "auto",
    run_name: Annotated[
        Optional[str],
        typer.Option("--run-name", help="Optional explicit run name."),
    ] = None,
) -> None:
    """Train the current workspace-backed recommended model using the exported starter config."""
    from pbdata.modeling.runtime import detect_runtime_capabilities
    from pbdata.modeling.studio import resolve_recommended_starter_config
    from pbdata.modeling.training_runs import execute_training_run

    layout = _storage_layout(ctx)
    write_stage_state(
        layout,
        stage="train-recommended-model",
        status="running",
        input_dir=layout.reports_dir,
        output_dir=layout.models_dir / "model_studio" / "runs",
        workers=1,
        counts={},
        notes="Training the current recommended model from the exported Model Studio starter configuration.",
    )
    fallback_note = ""
    try:
        with stage_lock(layout, stage="train-recommended-model"):
            starter_config_path, starter_config, recommendation_report = resolve_recommended_starter_config(layout)
            runtime_capabilities = detect_runtime_capabilities()
            original_family = str(starter_config.get("family") or "")
            if execution_strategy not in {"auto", "prefer_native", "safe_baseline"}:
                raise RuntimeError("execution-strategy must be one of: auto, prefer_native, safe_baseline")

            if execution_strategy == "safe_baseline":
                if "sklearn" in runtime_capabilities.installed_backends:
                    starter_config = dict(starter_config)
                    starter_config["recommended_family"] = original_family
                    starter_config["family"] = "random_forest"
                    starter_config["label"] = "Safe baseline (random forest)"
                    starter_config["model_id"] = "safe_baseline_random_forest"
                    starter_config["model"] = {
                        "type": "random_forest",
                        "n_estimators": 500,
                        "max_depth": None,
                        "min_samples_leaf": 1,
                        "use_graph_summaries": True,
                    }
                    fallback_note = "Execution strategy requested a safe executable baseline, so training was routed to a random-forest starter."
                elif "torch" in runtime_capabilities.installed_backends:
                    starter_config = dict(starter_config)
                    starter_config["recommended_family"] = original_family
                    starter_config["family"] = "dense_nn"
                    starter_config["label"] = "Safe baseline (dense neural net)"
                    starter_config["model_id"] = "safe_baseline_dense_nn"
                    starter_config["model"] = {
                        "type": "residual_mlp",
                        "hidden_dims": [256, 256, 128],
                        "dropout": 0.2,
                        "normalization": "layernorm",
                        "activation": "gelu",
                    }
                    starter_config["training"] = {
                        "seed": 42,
                        "early_stopping": True,
                        "monitor": "val_loss",
                        "batch_size": 64,
                        "epochs": 80,
                        "optimizer": "adamw",
                        "learning_rate": 1e-3,
                    }
                    fallback_note = "Execution strategy requested a safe executable baseline, so training was routed to a torch-backed dense neural net."
                else:
                    raise RuntimeError("safe_baseline requested, but neither sklearn nor torch is installed in the current runtime.")
            elif original_family in {"random_forest", "xgboost"} and "sklearn" not in runtime_capabilities.installed_backends:
                if execution_strategy == "prefer_native":
                    raise RuntimeError(
                        f"Recommended family '{original_family}' cannot run natively because scikit-learn is unavailable in this runtime."
                    )
                if "torch" in runtime_capabilities.installed_backends:
                    starter_config = dict(starter_config)
                    starter_config["recommended_family"] = original_family
                    starter_config["family"] = "dense_nn"
                    starter_config["label"] = f"{starter_config.get('label', 'Recommended model')} (runtime-adjusted)"
                    starter_config["model_id"] = f"{starter_config.get('model_id', original_family)}_runtime_adjusted_dense_nn"
                    starter_config["model"] = {
                        "type": "residual_mlp",
                        "hidden_dims": [256, 256, 128],
                        "dropout": 0.2,
                        "normalization": "layernorm",
                        "activation": "gelu",
                    }
                    starter_config["training"] = {
                        "seed": 42,
                        "early_stopping": True,
                        "monitor": "val_loss",
                        "batch_size": 64,
                        "epochs": 80,
                        "optimizer": "adamw",
                        "learning_rate": 1e-3,
                    }
                    fallback_note = (
                        f"Recommended family '{original_family}' requires scikit-learn, which is unavailable in this runtime. "
                        "Fell back to a torch-backed dense_nn starter so training could proceed."
                    )
                else:
                    raise RuntimeError(
                        f"Recommended family '{original_family}' requires scikit-learn, but sklearn is not installed and no torch fallback is available."
                    )
            result = execute_training_run(
                layout,
                starter_config=starter_config,
                runtime_target=runtime_target,
                run_name=run_name,
            )
            manifest_path = result.run_dir / "run_manifest.json"
            manifest_payload = json.loads(manifest_path.read_text(encoding="utf-8")) if manifest_path.exists() else None
            if isinstance(manifest_payload, dict):
                manifest_payload["execution_strategy"] = execution_strategy
                manifest_payload["recommended_family"] = original_family
                manifest_payload["selected_family"] = str(starter_config.get("family") or original_family)
                manifest_payload["executed_family"] = str(
                    manifest_payload.get("executed_family")
                    or (manifest_payload.get("backend_plan") or {}).get("execution_family")
                    or manifest_payload.get("family")
                    or starter_config.get("family")
                    or original_family
                )
                if fallback_note:
                    manifest_payload["runtime_adjustment"] = fallback_note
                manifest_path.write_text(json.dumps(manifest_payload, indent=2), encoding="utf-8")

            config_path = result.run_dir / "config.json"
            config_payload = json.loads(config_path.read_text(encoding="utf-8")) if config_path.exists() else None
            if isinstance(config_payload, dict):
                config_payload["execution_strategy"] = execution_strategy
                config_payload["recommended_family"] = original_family
                config_payload["selected_family"] = str(starter_config.get("family") or original_family)
                config_payload["executed_family"] = str(
                    (manifest_payload or {}).get("executed_family")
                    or (config_payload.get("backend_plan") or {}).get("execution_family")
                    or config_payload.get("family")
                    or starter_config.get("family")
                    or original_family
                )
                if fallback_note:
                    config_payload["runtime_adjustment"] = fallback_note
                config_path.write_text(json.dumps(config_payload, indent=2), encoding="utf-8")
    except Exception as exc:
        write_stage_state(
            layout,
            stage="train-recommended-model",
            status="failed",
            input_dir=layout.reports_dir,
            output_dir=layout.models_dir / "model_studio" / "runs",
            workers=1,
            counts={},
            notes=f"Recommended model training failed: {exc}",
        )
        typer.echo(f"Error: {exc}")
        raise typer.Exit(code=1) from exc

    test_metrics = result.metrics.get("test") or result.metrics.get("val") or result.metrics.get("train") or {}
    write_stage_state(
        layout,
        stage="train-recommended-model",
        status="completed",
        input_dir=layout.reports_dir,
        output_dir=result.run_dir,
        workers=1,
        counts={
            "warning_count": len(result.warnings),
            "metric_count": len(test_metrics),
        },
        notes=(
            f"Completed recommended {starter_config.get('family', 'unknown')} run '{result.run_name}' from {starter_config_path.name}."
            + (f" {fallback_note}" if fallback_note else "")
        ),
    )

    typer.echo(f"Storage root          : {layout.root}")
    typer.echo(f"Recommendation report : {layout.reports_dir / 'model_studio_recommendation.json'}")
    typer.echo(f"Starter config        : {starter_config_path}")
    typer.echo(f"Recommended label     : {(recommendation_report.get('top_recommendation') or {}).get('label', 'unknown')}")
    typer.echo(f"Execution strategy    : {execution_strategy}")
    typer.echo(f"Training run dir      : {result.run_dir}")
    typer.echo(f"Training summary      : {result.summary}")
    if fallback_note:
        typer.echo(f"Runtime adjustment    : {fallback_note}")
    if result.warnings:
        typer.echo(f"Warnings              : {', '.join(result.warnings)}")


@app.command("evaluate-tabular-affinity-model")
def evaluate_tabular_affinity_model_cmd(ctx: typer.Context) -> None:
    """Evaluate the supervised tabular affinity model against the current split files."""
    from pbdata.models.tabular_affinity import evaluate_tabular_affinity_model

    layout = _storage_layout(ctx)
    out_path, manifest = evaluate_tabular_affinity_model(layout)
    typer.echo(f"Storage root: {layout.root}")
    typer.echo(f"Tabular affinity model evaluation written to {out_path}")
    typer.echo(f"Workflow status: {manifest['status']}")


@app.command("report-training-set-quality")
def report_training_set_quality_cmd(ctx: typer.Context) -> None:
    """Write a training-set quality report and summary for current training examples and splits."""
    from pbdata.training_quality import export_training_set_quality_report

    layout = _storage_layout(ctx)
    json_path, md_path, report = export_training_set_quality_report(layout)
    typer.echo(f"Storage root: {layout.root}")
    typer.echo(f"Training quality JSON: {json_path}")
    typer.echo(f"Training quality Markdown: {md_path}")
    typer.echo(f"Workflow status: {report['status']}")


@app.command("report-model-comparison")
def report_model_comparison_cmd(ctx: typer.Context) -> None:
    """Write a model-comparison report from the current baseline and tabular evaluation artifacts."""
    from pbdata.model_comparison import export_model_comparison_report

    layout = _storage_layout(ctx)
    json_path, md_path, report = export_model_comparison_report(layout)
    typer.echo(f"Storage root: {layout.root}")
    typer.echo(f"Model comparison JSON: {json_path}")
    typer.echo(f"Model comparison Markdown: {md_path}")
    typer.echo(f"Workflow status: {report['status']}")


@app.command("report-model-recommendation")
def report_model_recommendation_cmd(ctx: typer.Context) -> None:
    """Write a model-studio recommendation report from the current workspace artifacts."""
    from pbdata.modeling.studio import export_model_recommendation_report

    layout = _storage_layout(ctx)
    json_path, md_path, report = export_model_recommendation_report(layout)
    typer.echo(f"Storage root: {layout.root}")
    typer.echo(f"Model recommendation JSON: {json_path}")
    typer.echo(f"Model recommendation Markdown: {md_path}")
    if report.get("starter_config_path"):
        typer.echo(f"Starter config: {report['starter_config_path']}")
    typer.echo(f"Workflow status: {report['status']}")


@app.command("preview-rcsb-search")
def preview_rcsb_search_cmd(
    ctx: typer.Context,
    criteria: Annotated[
        Optional[Path],
        typer.Option("--criteria", help="Path to criteria YAML (defaults to configs/criteria.yaml)."),
    ] = None,
) -> None:
    """Preview the current RCSB search result set before ingest."""
    from pbdata.criteria import load_criteria
    from pbdata.search_preview import export_rcsb_search_preview

    layout = _storage_layout(ctx)
    criteria_path = criteria if criteria is not None else _DEFAULT_CRITERIA
    report_criteria = load_criteria(criteria_path)
    json_path, md_path, report = export_rcsb_search_preview(layout, report_criteria)
    typer.echo(f"Storage root: {layout.root}")
    typer.echo(f"RCSB search preview JSON: {json_path}")
    typer.echo(f"RCSB search preview Markdown: {md_path}")
    typer.echo(f"Workflow status: {report['status']}")


@app.command("report-source-capabilities")
def report_source_capabilities_cmd(ctx: typer.Context) -> None:
    """Write a source-capability report from the current source configuration."""
    from pbdata.sources.registry import export_source_capability_report

    layout = _storage_layout(ctx)
    config = ctx.obj["config"]
    json_path, md_path, report = export_source_capability_report(layout, config)
    typer.echo(f"Storage root: {layout.root}")
    typer.echo(f"Source capability JSON: {json_path}")
    typer.echo(f"Source capability Markdown: {md_path}")
    typer.echo(f"Workflow status: {report['status']}")


@app.command("export-identity-crosswalk")
def export_identity_crosswalk_cmd(ctx: typer.Context) -> None:
    """Export conservative protein, ligand, and pair identity crosswalk tables."""
    from pbdata.identity_crosswalk import export_identity_crosswalk

    layout = _storage_layout(ctx)
    proteins_csv, ligands_csv, pairs_csv, summary_json, report = export_identity_crosswalk(layout)
    typer.echo(f"Storage root: {layout.root}")
    typer.echo(f"Protein crosswalk CSV: {proteins_csv}")
    typer.echo(f"Ligand crosswalk CSV: {ligands_csv}")
    typer.echo(f"Pair crosswalk CSV: {pairs_csv}")
    typer.echo(f"Identity summary JSON: {summary_json}")
    typer.echo(f"Workflow status: {report['status']}")


@app.command("predict-peptide-binding")
def predict_peptide_binding_cmd(
    ctx: typer.Context,
    structure_file: Annotated[
        Optional[str],
        typer.Option(help="Path to peptide PDB/mmCIF input."),
    ] = None,
    fasta: Annotated[
        Optional[str],
        typer.Option(help="FASTA text, raw peptide sequence, or FASTA file."),
    ] = None,
) -> None:
    """Normalize peptide-binding inputs and write a workflow manifest."""
    from pbdata.prediction.engine import run_peptide_binding_workflow

    layout = _storage_layout(ctx)
    try:
        out_path, manifest = run_peptide_binding_workflow(layout, structure_file=structure_file, fasta=fasta)
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
        typer.echo(
            "Error: --targets is required and must contain at least one UniProt ID. "
            "Example: --targets P12345,Q99999"
        )
        raise typer.Exit(code=1)
    out_path, summary = build_pathway_risk_summary(layout, targets=target_list)
    typer.echo(f"Storage root: {layout.root}")
    typer.echo(f"Pathway risk summary written to {out_path}")
    typer.echo(f"Workflow status: {summary['status']}")


def _pair_split_items_from_layout(layout: StorageLayout) -> list:
    artifact_items = _pair_split_items_from_artifacts(layout)
    if artifact_items:
        return artifact_items

    chains = _load_table_rows(layout.extracted_dir / "chains")
    assays = _load_table_rows(layout.extracted_dir / "assays")
    entries = _load_table_rows(layout.extracted_dir / "entry")
    training_examples = _load_json_rows(layout.training_dir / "training_examples.json") if (layout.training_dir / "training_examples.json").exists() else []
    metadata_rows = _read_metadata_rows(layout)
    if not assays:
        return []

    from pbdata.dataset.splits import PairSplitItem

    sequence_by_pdb_chain: dict[tuple[str, str], str] = {}
    uniprot_by_pdb_chain: dict[tuple[str, str], str] = {}
    release_date_by_pdb: dict[str, str] = {}
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
    for entry in entries:
        pdb_id = str(entry.get("pdb_id") or "")
        release_date = str(entry.get("release_date") or "")[:10]
        if pdb_id and release_date:
            release_date_by_pdb[pdb_id] = release_date

    example_id_by_key: dict[tuple[str, str], list[str]] = defaultdict(list)
    training_example_by_id: dict[str, dict] = {}
    metadata_by_pair_key: dict[str, dict[str, str]] = {}
    metadata_by_pdb: dict[str, dict[str, str]] = {}
    for row in training_examples:
        provenance = row.get("provenance") or {}
        labels = row.get("labels") or {}
        pair_key = str(provenance.get("pair_identity_key") or "")
        affinity_type = str(labels.get("affinity_type") or "")
        example_id = str(row.get("example_id") or "")
        if pair_key and example_id:
            example_id_by_key[(pair_key, affinity_type)].append(example_id)
            training_example_by_id[example_id] = row
    for row in metadata_rows:
        pair_key = str(row.get("pair_identity_key") or "").strip()
        pdb_id = str(row.get("pdb_id") or "").strip().upper()
        if pair_key and pair_key not in metadata_by_pair_key:
            metadata_by_pair_key[pair_key] = row
        if pdb_id and pdb_id not in metadata_by_pdb:
            metadata_by_pdb[pdb_id] = row

    items: list[PairSplitItem] = []
    for assay in assays:
        pair_key = str(assay.get("pair_identity_key") or "")
        affinity_type = str(assay.get("binding_affinity_type") or "")
        parsed_pair = parse_pair_identity_key(pair_key)
        if not pair_key or parsed_pair is None:
            continue
        pdb_id = parsed_pair.pdb_id or str(assay.get("pdb_id") or "")
        metadata_row = metadata_by_pair_key.get(pair_key) or metadata_by_pdb.get(str(pdb_id).upper()) or {}
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
        interpro_ids = _normalize_delimited_values(metadata_row.get("interpro_ids"))
        pfam_ids = _normalize_delimited_values(metadata_row.get("pfam_ids"))
        pathway_ids = _normalize_delimited_values(metadata_row.get("reactome_pathway_ids"))
        structural_fold = str(
            metadata_row.get("structural_fold")
            or metadata_row.get("oligomeric_state")
            or ""
        ).strip()
        metadata_family_tokens = interpro_ids or pfam_ids
        metadata_family_key = ",".join(metadata_family_tokens) if metadata_family_tokens else ""
        domain_group_key = (
            "|".join([parsed_pair.task_type, metadata_family_key])
            if metadata_family_key
            else ""
        )
        pathway_group_key = (
            "|".join([parsed_pair.task_type, pathway_ids[0]])
            if pathway_ids
            else ""
        )
        fold_group_key = (
            "|".join([parsed_pair.task_type, structural_fold])
            if structural_fold
            else ""
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
            training_example = training_example_by_id.get(target_id) or {}
            ligand = training_example.get("ligand") if isinstance(training_example.get("ligand"), dict) else {}
            ligand_proxy = (
                str(ligand.get("inchikey") or "").strip()
                or str(ligand.get("smiles") or "").strip()
                or str(ligand.get("ligand_id") or "").strip()
                or str(parsed_pair.ligand_key or "").strip()
                or ",".join(parsed_pair.partner_chain_ids)
                or "-"
            )
            preferred_source = (
                str((training_example.get("labels") or {}).get("preferred_source_database") or "").strip()
                or str((training_example.get("experiment") or {}).get("preferred_source_database") or "").strip()
                or str(assay.get("selected_preferred_source") or "").strip()
                or str(assay.get("source_database") or "").strip()
                or "source_unknown"
            )
            mutation_group = (parsed_pair.mutation_key or "wt_or_unspecified").strip().lower() or "wt_or_unspecified"
            items.append(PairSplitItem(
                item_id=target_id,
                pair_identity_key=pair_key,
                affinity_type=affinity_type or None,
                receptor_sequence=receptor_sequence,
                receptor_identity=receptor_identity,
                representation_key=representation_key,
                hard_group_key=hard_group_key,
                scaffold_key="|".join([parsed_pair.task_type, ligand_proxy]),
                family_key=(
                    "|".join([parsed_pair.task_type, metadata_family_key])
                    if metadata_family_key
                    else "|".join([parsed_pair.task_type, receptor_identity])
                ),
                domain_group_key=domain_group_key,
                pathway_group_key=pathway_group_key,
                fold_group_key=fold_group_key,
                mutation_group_key="|".join([
                    parsed_pair.task_type,
                    metadata_family_key or receptor_identity,
                    ligand_proxy,
                    mutation_group,
                ]),
                source_group_key="|".join([parsed_pair.task_type, preferred_source]),
                release_date=release_date_by_pdb.get(pdb_id),
            ))
    return items


def _pair_split_items_from_artifacts(layout: StorageLayout) -> list:
    from pbdata.dataset.splits import PairSplitItem
    from pbdata.master_export import master_csv_path

    root = layout.root
    model_ready_rows = _read_repo_csv_rows(root / "model_ready_pairs.csv")
    if not model_ready_rows:
        return []

    training_examples = (
        _load_json_rows(layout.training_dir / "training_examples.json")
        if (layout.training_dir / "training_examples.json").exists()
        else []
    )
    metadata_rows = _read_metadata_rows(layout)
    master_rows = _read_repo_csv_rows(master_csv_path(root))

    example_id_by_key: dict[tuple[str, str], list[str]] = defaultdict(list)
    training_example_by_id: dict[str, dict] = {}
    for row in training_examples:
        provenance = row.get("provenance") or {}
        labels = row.get("labels") or {}
        pair_key = str(provenance.get("pair_identity_key") or "")
        affinity_type = str(labels.get("affinity_type") or "")
        example_id = str(row.get("example_id") or "")
        if pair_key and example_id:
            example_id_by_key[(pair_key, affinity_type)].append(example_id)
            training_example_by_id[example_id] = row

    metadata_by_pair_key: dict[str, dict[str, str]] = {}
    metadata_by_pdb: dict[str, dict[str, str]] = {}
    for row in metadata_rows:
        pair_key = str(row.get("pair_identity_key") or "").strip()
        pdb_id = str(row.get("pdb_id") or "").strip().upper()
        if pair_key and pair_key not in metadata_by_pair_key:
            metadata_by_pair_key[pair_key] = row
        if pdb_id and pdb_id not in metadata_by_pdb:
            metadata_by_pdb[pdb_id] = row

    release_date_by_pdb = {
        str(row.get("pdb_id") or "").strip().upper(): str(row.get("release_date") or "").strip()[:10]
        for row in master_rows
        if str(row.get("pdb_id") or "").strip()
    }

    items: list[PairSplitItem] = []
    seen_keys: set[tuple[str, str, str, str]] = set()
    for row in model_ready_rows:
        pair_key = str(row.get("pair_identity_key") or "").strip()
        affinity_type = str(row.get("binding_affinity_type") or "").strip()
        source_name = str(row.get("source_database") or "").strip()
        dedupe_key = (
            str(row.get("pdb_id") or "").strip().upper(),
            pair_key,
            affinity_type,
            source_name,
        )
        if dedupe_key in seen_keys:
            continue
        seen_keys.add(dedupe_key)

        parsed_pair = parse_pair_identity_key(pair_key)
        if not pair_key or parsed_pair is None:
            continue
        pdb_id = (parsed_pair.pdb_id or str(row.get("pdb_id") or "")).strip().upper()
        metadata_row = metadata_by_pair_key.get(pair_key) or metadata_by_pdb.get(pdb_id) or {}
        receptor_uniprot_ids = _normalize_delimited_values(row.get("receptor_uniprot_ids"))
        receptor_chain_ids = list(parsed_pair.receptor_chain_ids) or _normalize_delimited_values(
            row.get("receptor_chain_ids")
        )
        receptor_identity = ",".join(receptor_uniprot_ids) if receptor_uniprot_ids else (
            f"{pdb_id}:{','.join(receptor_chain_ids) or '-'}"
        )
        interpro_ids = _normalize_delimited_values(metadata_row.get("interpro_ids"))
        pfam_ids = _normalize_delimited_values(metadata_row.get("pfam_ids"))
        pathway_ids = _normalize_delimited_values(metadata_row.get("reactome_pathway_ids"))
        structural_fold = str(
            metadata_row.get("structural_fold")
            or metadata_row.get("cath_ids")
            or metadata_row.get("scop_ids")
            or ""
        ).strip()
        metadata_family_tokens = interpro_ids or pfam_ids
        metadata_family_key = ",".join(metadata_family_tokens) if metadata_family_tokens else ""
        domain_group_key = (
            "|".join([parsed_pair.task_type, metadata_family_key]) if metadata_family_key else ""
        )
        pathway_group_key = (
            "|".join([parsed_pair.task_type, pathway_ids[0]]) if pathway_ids else ""
        )
        fold_group_key = (
            "|".join([parsed_pair.task_type, structural_fold]) if structural_fold else ""
        )
        mutation_key = (parsed_pair.mutation_key or "wt_or_unspecified").lower()
        mutation_family = (
            "wildtype"
            if mutation_key in {"wt", "wildtype", "wt_or_unspecified"}
            else ("unknown" if mutation_key.startswith("mutation_unknown") else "mutant")
        )
        representation_key = "|".join(
            [
                parsed_pair.task_type,
                affinity_type or "assay_unknown",
                mutation_family,
                "sequence_unknown",
            ]
        )
        hard_group_key = "|".join(
            [
                parsed_pair.task_type,
                receptor_identity,
                parsed_pair.ligand_key or ",".join(parsed_pair.partner_chain_ids) or "-",
            ]
        )
        target_ids = example_id_by_key.get((pair_key, affinity_type), []) or [f"{pair_key}|{affinity_type or 'assay_unknown'}"]
        for target_id in target_ids:
            training_example = training_example_by_id.get(target_id) or {}
            ligand = training_example.get("ligand") if isinstance(training_example.get("ligand"), dict) else {}
            ligand_proxy = (
                str(ligand.get("inchikey") or "").strip()
                or str(ligand.get("smiles") or "").strip()
                or str(ligand.get("ligand_id") or "").strip()
                or str(parsed_pair.ligand_key or "").strip()
                or ",".join(parsed_pair.partner_chain_ids)
                or "-"
            )
            preferred_source = (
                str((training_example.get("labels") or {}).get("preferred_source_database") or "").strip()
                or str((training_example.get("experiment") or {}).get("preferred_source_database") or "").strip()
                or str(row.get("selected_preferred_source") or "").strip()
                or source_name
                or "source_unknown"
            )
            mutation_group = (parsed_pair.mutation_key or "wt_or_unspecified").strip().lower() or "wt_or_unspecified"
            items.append(
                PairSplitItem(
                    item_id=target_id,
                    pair_identity_key=pair_key,
                    affinity_type=affinity_type or None,
                    receptor_sequence=None,
                    receptor_identity=receptor_identity,
                    representation_key=representation_key,
                    hard_group_key=hard_group_key,
                    scaffold_key="|".join([parsed_pair.task_type, ligand_proxy]),
                    family_key=(
                        "|".join([parsed_pair.task_type, metadata_family_key])
                        if metadata_family_key
                        else "|".join([parsed_pair.task_type, receptor_identity])
                    ),
                    domain_group_key=domain_group_key,
                    pathway_group_key=pathway_group_key,
                    fold_group_key=fold_group_key,
                    mutation_group_key="|".join(
                        [
                            parsed_pair.task_type,
                            metadata_family_key or receptor_identity,
                            ligand_proxy,
                            mutation_group,
                        ]
                    ),
                    source_group_key="|".join([parsed_pair.task_type, preferred_source]),
                    release_date=release_date_by_pdb.get(pdb_id),
                )
            )
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
        typer.Option("--stage-name", help="Stage name for stage_only mode. Use --list-stages to show valid names."),
    ] = None,
    list_stages: Annotated[
        bool,
        typer.Option("--list-stages", help="List valid stage names and exit."),
    ] = False,
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
    from pbdata.pipeline.feature_pipeline_stages import feature_pipeline_stage_help_text

    layout = _storage_layout(ctx)
    if list_stages:
        typer.echo(feature_pipeline_stage_help_text())
        return
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
    _emit_feature_workflow_output(
        layout,
        [
            ("Feature pipeline run id", result["run_id"]),
            ("Artifacts root", result["artifacts_root"]),
            ("Input manifest", result["input_manifest"]),
        ],
    )
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
    resolved_run_id = _resolve_latest_feature_pipeline_run_id(layout, run_id)
    try:
        result = export_analysis_queue(layout, run_id=resolved_run_id)
    except (ModuleNotFoundError, ImportError, RuntimeError, ValueError) as exc:
        _exit_with_dependency_error(exc)
    _emit_feature_workflow_output(
        layout,
        [
            ("Feature pipeline run id", resolved_run_id),
            ("Archetypes", result["archetypes"]),
            ("Representatives", result["representatives"]),
            ("Cluster summary", result["cluster_summary"]),
            ("Fragments", result["fragments"]),
            ("Analysis queue", result["queue"]),
            ("Batch manifest", result["batch_manifest"]),
        ],
    )


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
    _emit_feature_workflow_output(
        layout,
        [
            ("Physics targets", result["physics_targets"]),
            ("Failed fragments", result["failed_fragments"]),
            ("Manifest", result["manifest"]),
        ],
    )


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
    _emit_feature_workflow_output(
        layout,
        [
            ("Checkpoint", result["checkpoint"]),
            ("Manifest", result["manifest"]),
            ("Latest pointer", result["latest"]),
        ],
    )


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
def harvest_metadata_cmd(
    ctx: typer.Context,
    with_uniprot: Annotated[
        bool,
        typer.Option("--with-uniprot", help="Enrich harvested metadata with UniProt annotations."),
    ] = False,
    with_alphafold: Annotated[
        bool,
        typer.Option("--with-alphafold", help="Enrich harvested metadata with AlphaFold DB prediction metadata."),
    ] = False,
    with_reactome: Annotated[
        bool,
        typer.Option("--with-reactome", help="Enrich harvested metadata with Reactome pathway memberships."),
    ] = False,
    with_interpro: Annotated[
        bool,
        typer.Option("--with-interpro", help="Enrich harvested metadata with PDBe/SIFTS InterPro domain mappings."),
    ] = False,
    with_pfam: Annotated[
        bool,
        typer.Option("--with-pfam", help="Enrich harvested metadata with PDBe/SIFTS Pfam domain mappings."),
    ] = False,
    with_cath: Annotated[
        bool,
        typer.Option("--with-cath", help="Enrich harvested metadata with PDBe/SIFTS CATH fold mappings."),
    ] = False,
    with_scop: Annotated[
        bool,
        typer.Option("--with-scop", help="Enrich harvested metadata with PDBe/SIFTS SCOP fold mappings."),
    ] = False,
    max_proteins: Annotated[
        int | None,
        typer.Option("--max-proteins", min=0, help="Limit external annotation requests to the first N unique UniProt IDs."),
    ] = None,
) -> None:
    """Build the unified metadata table for dataset engineering workflows."""
    from pbdata.data_pipeline.workflow_engine import harvest_unified_metadata

    layout = _storage_layout(ctx)
    artifacts = harvest_unified_metadata(
        layout,
        enrich_uniprot=with_uniprot,
        enrich_alphafold=with_alphafold,
        enrich_reactome=with_reactome,
        enrich_interpro=with_interpro,
        enrich_pfam=with_pfam,
        enrich_cath=with_cath,
        enrich_scop=with_scop,
        max_proteins=max_proteins,
    )
    typer.echo(f"Storage root: {layout.root}")
    typer.echo(f"Metadata CSV written to {artifacts['metadata_csv']}")
    typer.echo(f"Metadata manifest written to {artifacts['manifest']}")


@app.command("build-bootstrap-catalog")
def build_bootstrap_catalog_cmd(
    ctx: typer.Context,
    shard_size: Annotated[
        int,
        typer.Option("--shard-size", min=1, help="Approximate number of PDB records per shard."),
    ] = 5000,
    package_id: Annotated[
        Optional[str],
        typer.Option("--package-id", help="Optional explicit package identifier."),
    ] = None,
) -> None:
    """Build a lightweight per-PDB startup catalog for fast local planning."""
    from pbdata.bootstrap_catalog import build_bootstrap_catalog

    layout = _storage_layout(ctx)
    try:
        result = build_bootstrap_catalog(
            layout,
            shard_size=shard_size,
            package_id=package_id,
        )
    except (FileNotFoundError, FileExistsError, ValueError) as exc:
        typer.echo(f"Error: {exc}")
        raise typer.Exit(code=1) from exc

    typer.echo(f"Storage root: {layout.root}")
    typer.echo(f"Bootstrap package : {result.package_dir}")
    typer.echo(f"Manifest          : {result.manifest_path}")
    typer.echo(f"Shards            : {result.shard_count}")
    typer.echo(f"PDB records       : {result.record_count}")


@app.command("materialize-bootstrap-store")
def materialize_bootstrap_store_cmd(ctx: typer.Context) -> None:
    """Build a persistent local bootstrap index for fast startup and dataset planning."""
    from pbdata.bootstrap_store import materialize_bootstrap_store

    layout = _storage_layout(ctx)
    try:
        result = materialize_bootstrap_store(layout)
    except FileNotFoundError as exc:
        typer.echo(f"Error: {exc}")
        raise typer.Exit(code=1) from exc

    typer.echo(f"Storage root: {layout.root}")
    typer.echo(f"Bootstrap store  : {result.database_path}")
    typer.echo(f"Store manifest   : {result.manifest_path}")
    typer.echo(f"PDB records      : {result.record_count}")


@app.command("export-bootstrap-summary")
def export_bootstrap_summary_cmd(ctx: typer.Context) -> None:
    """Export a lightweight bootstrap summary package for GitHub-friendly planning."""
    from pbdata.bootstrap_store import export_bootstrap_summary

    layout = _storage_layout(ctx)
    try:
        result = export_bootstrap_summary(layout)
    except FileNotFoundError as exc:
        typer.echo(f"Error: {exc}")
        raise typer.Exit(code=1) from exc

    typer.echo(f"Storage root: {layout.root}")
    typer.echo(f"PDB summary CSV : {result.csv_path}")
    if result.pair_csv_path:
        typer.echo(f"Pair summary CSV: {result.pair_csv_path}")
    typer.echo(f"Summary manifest: {result.manifest_path}")
    typer.echo(f"PDB records     : {result.record_count}")
    if result.pair_csv_path:
        typer.echo(f"Pair records    : {result.pair_record_count}")


@app.command("report-source-lifecycle")
def report_source_lifecycle_cmd(ctx: typer.Context) -> None:
    """Report staged local source assets and their lifecycle policies."""
    from pbdata.source_lifecycle import export_source_lifecycle_report

    layout = _storage_layout(ctx)
    config = ctx.obj["config"]
    json_path, md_path, report = export_source_lifecycle_report(layout, config)

    typer.echo(f"Storage root : {layout.root}")
    typer.echo(f"JSON report  : {json_path}")
    typer.echo(f"Markdown     : {md_path}")
    typer.echo(f"Ready sources: {report['summary']['ready_sources']}")
    typer.echo(f"Missing      : {report['summary']['missing_sources']}")
    typer.echo(f"Next action  : {report['next_action']}")


@app.command("audit-screening-fields")
def audit_screening_fields_cmd(ctx: typer.Context) -> None:
    """Audit screening and dataset-selection field population in the workspace exports."""
    from pbdata.screening_field_audit import export_screening_field_audit

    layout = _storage_layout(ctx)
    json_path, md_path, report = export_screening_field_audit(layout)

    typer.echo(f"Storage root: {layout.root}")
    typer.echo(f"JSON report : {json_path}")
    typer.echo(f"Markdown    : {md_path}")
    typer.echo(f"Issue count : {report['issue_count']}")
    typer.echo(f"Next action : {report['next_action']}")


@app.command("index-uniprot-swissprot")
def index_uniprot_swissprot_cmd(
    ctx: typer.Context,
    source_path: Annotated[
        Optional[Path],
        typer.Option("--source-path", help="Optional path to the staged UniProt Swiss-Prot .dat.gz file."),
    ] = None,
    limit: Annotated[
        Optional[int],
        typer.Option("--limit", min=1, help="Optional maximum number of Swiss-Prot records to index for smoke/preview runs."),
    ] = None,
    force: Annotated[
        bool,
        typer.Option("--force", help="Rebuild the local index even if the existing manifest matches the staged source."),
    ] = False,
) -> None:
    """Build a lightweight local index from the staged UniProt Swiss-Prot flat file."""
    from pbdata.source_indexes import index_uniprot_swissprot

    layout = _storage_layout(ctx)
    config = ctx.obj["config"]
    resolved_source = source_path
    if resolved_source is None:
        configured = str(config.sources.uniprot.extra.get("local_swissprot") or "").strip()
        resolved_source = Path(configured) if configured else (layout.root / "data_sources" / "uniprot" / "uniprot_sprot.dat.gz")

    result = index_uniprot_swissprot(layout, source_path=resolved_source, limit=limit, force=force)
    typer.echo(f"Storage root : {layout.root}")
    typer.echo(f"Source path  : {resolved_source}")
    typer.echo(f"Index path   : {result.index_path}")
    if result.lookup_db_path is not None:
        typer.echo(f"Lookup DB    : {result.lookup_db_path}")
    typer.echo(f"Manifest     : {result.manifest_path}")
    typer.echo(f"Record count : {result.record_count}")


@app.command("index-alphafold-archive")
def index_alphafold_archive_cmd(
    ctx: typer.Context,
    archive_path: Annotated[
        Optional[Path],
        typer.Option("--archive-path", help="Optional path to the staged AlphaFold bulk archive."),
    ] = None,
    limit: Annotated[
        Optional[int],
        typer.Option("--limit", min=1, help="Optional maximum number of AlphaFold archive members to index for smoke/preview runs."),
    ] = None,
    chunk_size: Annotated[
        Optional[int],
        typer.Option("--chunk-size", min=1, help="Optional number of archive members to index in this chunk before stopping."),
    ] = None,
    resume: Annotated[
        bool,
        typer.Option("--resume", help="Resume a partial AlphaFold index from the manifest's saved tar offset."),
    ] = False,
    force: Annotated[
        bool,
        typer.Option("--force", help="Discard any existing AlphaFold local index and rebuild from the beginning."),
    ] = False,
) -> None:
    """Build a lightweight accession index from the staged AlphaFold bulk archive."""
    from pbdata.source_indexes import index_alphafold_archive

    layout = _storage_layout(ctx)
    config = ctx.obj["config"]
    resolved_archive = archive_path
    if resolved_archive is None:
        configured = str(config.sources.alphafold_db.extra.get("local_archive") or "").strip()
        resolved_archive = Path(configured) if configured else (layout.root / "data_sources" / "alphafold" / "swissprot_pdb_v6.tar")

    result = index_alphafold_archive(
        layout,
        archive_path=resolved_archive,
        limit=limit,
        chunk_size=chunk_size,
        resume=resume,
        force=force,
    )
    typer.echo(f"Storage root : {layout.root}")
    typer.echo(f"Archive path : {resolved_archive}")
    typer.echo(f"Index path   : {result.index_path}")
    if result.lookup_db_path is not None:
        typer.echo(f"Lookup DB    : {result.lookup_db_path}")
    typer.echo(f"Manifest     : {result.manifest_path}")
    typer.echo(f"Record count : {result.record_count}")


@app.command("index-bindingdb-bulk")
def index_bindingdb_bulk_cmd(
    ctx: typer.Context,
    dump_zip_path: Annotated[
        Optional[Path],
        typer.Option("--dump-zip-path", help="Optional path to the staged BindingDB bulk dump zip."),
    ] = None,
    force: Annotated[
        bool,
        typer.Option("--force", help="Rebuild the local BindingDB bulk index even if the manifest matches the staged dump."),
    ] = False,
) -> None:
    """Build a reusable local BindingDB assay lookup from the staged MySQL dump."""
    from pbdata.sources.bindingdb_bulk import build_bindingdb_bulk_index

    layout = _storage_layout(ctx)
    config = ctx.obj["config"]
    resolved_dump = dump_zip_path
    if resolved_dump is None:
        configured = str(config.sources.bindingdb.extra.get("bulk_zip") or "").strip()
        resolved_dump = Path(configured) if configured else (layout.root / "data_sources" / "bindingdb" / "BDB-mySQL_All_202603_dmp.zip")

    result = build_bindingdb_bulk_index(layout, dump_zip_path=resolved_dump, force=force)
    typer.echo(f"Storage root : {layout.root}")
    typer.echo(f"Dump zip     : {resolved_dump}")
    typer.echo(f"Index path   : {result.index_path}")
    typer.echo(f"Manifest     : {result.manifest_path}")
    typer.echo(f"Record count : {result.record_count}")
    typer.echo(f"PDB count    : {result.pdb_count}")


@app.command("index-reactome-pathways")
def index_reactome_pathways_cmd(
    ctx: typer.Context,
    mapping_path: Annotated[
        Optional[Path],
        typer.Option("--mapping-path", help="Optional path to the staged UniProt-to-Reactome mapping file."),
    ] = None,
    pathways_path: Annotated[
        Optional[Path],
        typer.Option("--pathways-path", help="Optional path to the staged Reactome pathway names file."),
    ] = None,
    limit: Annotated[
        Optional[int],
        typer.Option("--limit", min=1, help="Optional maximum number of UniProt accessions to index for preview runs."),
    ] = None,
    force: Annotated[
        bool,
        typer.Option("--force", help="Discard any existing Reactome local index and rebuild it."),
    ] = False,
) -> None:
    """Build a reusable local Reactome pathway lookup from staged flat files."""
    from pbdata.source_indexes import index_reactome_pathways

    layout = _storage_layout(ctx)
    config = ctx.obj["config"]
    resolved_mapping = mapping_path
    if resolved_mapping is None:
        configured_dir = str(config.sources.reactome.extra.get("local_dir") or "").strip()
        base_dir = Path(configured_dir) if configured_dir else (layout.root / "data_sources" / "reactome")
        resolved_mapping = base_dir / "UniProt2Reactome_All_Levels.txt"
    resolved_pathways = pathways_path
    if resolved_pathways is None:
        configured_dir = str(config.sources.reactome.extra.get("local_dir") or "").strip()
        base_dir = Path(configured_dir) if configured_dir else (layout.root / "data_sources" / "reactome")
        resolved_pathways = base_dir / "ReactomePathways.txt"

    result = index_reactome_pathways(
        layout,
        mapping_path=resolved_mapping,
        pathways_path=resolved_pathways,
        limit=limit,
        force=force,
    )
    typer.echo(f"Storage root : {layout.root}")
    typer.echo(f"Mapping path : {resolved_mapping}")
    typer.echo(f"Pathways path: {resolved_pathways}")
    typer.echo(f"Index path   : {result.index_path}")
    if result.lookup_db_path is not None:
        typer.echo(f"Lookup DB    : {result.lookup_db_path}")
    typer.echo(f"Manifest     : {result.manifest_path}")
    typer.echo(f"Record count : {result.record_count}")


@app.command("index-cath-domains")
def index_cath_domains_cmd(
    ctx: typer.Context,
    domain_list_path: Annotated[
        Optional[Path],
        typer.Option("--domain-list-path", help="Optional path to the staged CATH domain list file."),
    ] = None,
    boundaries_path: Annotated[
        Optional[Path],
        typer.Option("--boundaries-path", help="Optional path to the staged CATH domain boundaries file."),
    ] = None,
    names_path: Annotated[
        Optional[Path],
        typer.Option("--names-path", help="Optional path to the staged CATH names file."),
    ] = None,
    limit: Annotated[
        Optional[int],
        typer.Option("--limit", min=1, help="Optional maximum number of PDB entries to index for preview runs."),
    ] = None,
    force: Annotated[
        bool,
        typer.Option("--force", help="Discard any existing local CATH index and rebuild it."),
    ] = False,
) -> None:
    """Build a reusable local CATH chain-aware lookup from staged flat files."""
    from pbdata.source_indexes import index_cath_domains

    layout = _storage_layout(ctx)
    base_dir = layout.root / "data_sources" / "cath"
    resolved_domain_list = domain_list_path or (base_dir / "cath-domain-list.txt")
    resolved_boundaries = boundaries_path or (base_dir / "cath-domain-boundaries.txt")
    resolved_names = names_path or (base_dir / "cath-names.txt")

    result = index_cath_domains(
        layout,
        domain_list_path=resolved_domain_list,
        boundaries_path=resolved_boundaries,
        names_path=resolved_names,
        limit=limit,
        force=force,
    )
    typer.echo(f"Storage root : {layout.root}")
    typer.echo(f"Domain list  : {resolved_domain_list}")
    typer.echo(f"Boundaries   : {resolved_boundaries}")
    typer.echo(f"Names path   : {resolved_names}")
    typer.echo(f"Index path   : {result.index_path}")
    if result.lookup_db_path is not None:
        typer.echo(f"Lookup DB    : {result.lookup_db_path}")
    typer.echo(f"Manifest     : {result.manifest_path}")
    typer.echo(f"Record count : {result.record_count}")


@app.command("index-scop-domains")
def index_scop_domains_cmd(
    ctx: typer.Context,
    classification_path: Annotated[
        Optional[Path],
        typer.Option("--classification-path", help="Optional path to the staged SCOPe classification file."),
    ] = None,
    descriptions_path: Annotated[
        Optional[Path],
        typer.Option("--descriptions-path", help="Optional path to the staged SCOPe description file."),
    ] = None,
    limit: Annotated[
        Optional[int],
        typer.Option("--limit", min=1, help="Optional maximum number of PDB entries to index for preview runs."),
    ] = None,
    force: Annotated[
        bool,
        typer.Option("--force", help="Discard any existing local SCOPe index and rebuild it."),
    ] = False,
) -> None:
    """Build a reusable local SCOPe chain-aware lookup from staged flat files."""
    from pbdata.source_indexes import index_scop_domains

    layout = _storage_layout(ctx)
    base_dir = layout.root / "data_sources" / "scope"
    resolved_classification = classification_path or (base_dir / "dir.cla.scope.2.08-stable.txt")
    resolved_descriptions = descriptions_path or (base_dir / "dir.des.scope.txt")

    result = index_scop_domains(
        layout,
        classification_path=resolved_classification,
        descriptions_path=resolved_descriptions,
        limit=limit,
        force=force,
    )
    typer.echo(f"Storage root : {layout.root}")
    typer.echo(f"Class path   : {resolved_classification}")
    typer.echo(f"Desc path    : {resolved_descriptions}")
    typer.echo(f"Index path   : {result.index_path}")
    if result.lookup_db_path is not None:
        typer.echo(f"Lookup DB    : {result.lookup_db_path}")
    typer.echo(f"Manifest     : {result.manifest_path}")
    typer.echo(f"Record count : {result.record_count}")


@app.command("plan-selected-pdb-refresh")
def plan_selected_pdb_refresh_cmd(
    ctx: typer.Context,
    source_csv: Annotated[
        Optional[Path],
        typer.Option("--source-csv", help="Optional selected-PDB CSV. Defaults to custom_training_set.csv, then model_ready_pairs.csv."),
    ] = None,
    plan_name: Annotated[
        str,
        typer.Option("--plan-name", help="Output filename for the refresh manifest."),
    ] = "selected_pdb_refresh_manifest.json",
) -> None:
    """Plan a targeted upstream refresh for the selected training-set or review PDB IDs."""
    from pbdata.bootstrap_store import plan_selected_pdb_refresh

    layout = _storage_layout(ctx)
    try:
        result = plan_selected_pdb_refresh(layout, source_csv=source_csv, plan_name=plan_name)
    except FileNotFoundError as exc:
        typer.echo(f"Error: {exc}")
        raise typer.Exit(code=1) from exc

    typer.echo(f"Storage root: {layout.root}")
    typer.echo(f"Selected source : {result.selected_source}")
    typer.echo(f"Refresh manifest: {result.manifest_path}")
    typer.echo(f"PDB records     : {result.record_count}")


@app.command("refresh-selected-pdbs")
def refresh_selected_pdbs_cmd(
    ctx: typer.Context,
    manifest_path: Annotated[
        Optional[Path],
        typer.Option("--manifest-path", help="Optional refresh manifest path. Defaults to metadata/bootstrap_catalog/selected_pdb_refresh_manifest.json."),
    ] = None,
    limit: Annotated[
        Optional[int],
        typer.Option("--limit", min=1, help="Optional maximum number of selected PDB IDs to refresh from the manifest."),
    ] = None,
    all_selected: Annotated[
        bool,
        typer.Option("--all-selected", help="Refresh all selected PDB IDs instead of only those with missing local assets."),
    ] = False,
    with_live_enrichment: Annotated[
        bool,
        typer.Option("--with-live-enrichment/--skip-live-enrichment", help="Whether to re-run live BindingDB/ChEMBL enrichment during the targeted refresh."),
    ] = False,
) -> None:
    """Refresh the selected PDB IDs described by the targeted refresh manifest."""
    from pbdata.bootstrap_store import execute_selected_pdb_refresh

    layout = _storage_layout(ctx)
    cfg: AppConfig = ctx.obj.get("config", AppConfig())
    manifest = manifest_path or (layout.bootstrap_store_dir / "selected_pdb_refresh_manifest.json")
    selected_count = 0
    try:
        if manifest.exists():
            payload = json.loads(manifest.read_text(encoding="utf-8"))
            selected_count = len(payload.get("records") or [])
    except Exception:
        selected_count = 0

    write_stage_state(
        layout,
        stage="refresh-selected-pdbs",
        status="running",
        input_dir=manifest.parent,
        output_dir=layout.extracted_dir,
        workers=1,
        counts={"inputs": selected_count},
        notes="Refreshing the selected PDB IDs from the targeted bootstrap refresh manifest.",
    )

    try:
        with stage_lock(layout, stage="refresh-selected-pdbs"):
            result = execute_selected_pdb_refresh(
                layout,
                cfg,
                manifest_path=manifest_path,
                limit=limit,
                only_missing_assets=not all_selected,
                with_live_enrichment=with_live_enrichment,
            )
    except FileNotFoundError as exc:
        write_stage_state(
            layout,
            stage="refresh-selected-pdbs",
            status="failed",
            input_dir=manifest.parent,
            output_dir=layout.extracted_dir,
            workers=1,
            counts={"inputs": selected_count},
            notes=str(exc),
        )
        typer.echo(f"Error: {exc}")
        raise typer.Exit(code=1) from exc
    except Exception as exc:
        write_stage_state(
            layout,
            stage="refresh-selected-pdbs",
            status="failed",
            input_dir=manifest.parent,
            output_dir=layout.extracted_dir,
            workers=1,
            counts={"inputs": selected_count},
            notes=f"Selected-PDB refresh failed: {exc}",
        )
        raise

    write_stage_state(
        layout,
        stage="refresh-selected-pdbs",
        status="completed" if result.failed_count == 0 else "completed_with_failures",
        input_dir=result.manifest_path.parent,
        output_dir=layout.extracted_dir,
        workers=1,
        counts={
            "inputs": selected_count,
            "refreshed": result.refreshed_count,
            "skipped": result.skipped_count,
            "failed": result.failed_count,
        },
        notes="Selected-PDB refresh updated raw, processed, extracted, and export artifacts for the targeted set.",
    )

    typer.echo(f"Storage root      : {layout.root}")
    typer.echo(f"Refresh manifest  : {result.manifest_path}")
    typer.echo(f"Execution report  : {result.execution_report_path}")
    typer.echo(f"Refreshed PDBs    : {result.refreshed_count}")
    typer.echo(f"Skipped PDBs      : {result.skipped_count}")
    typer.echo(f"Failed PDBs       : {result.failed_count}")
    if result.failed_pdb_ids:
        typer.echo(f"Failed IDs        : {', '.join(result.failed_pdb_ids[:20])}")
    if "master_csv" in result.export_status:
        typer.echo(f"Master CSV        : {result.export_status['master_csv']}")
    if "pair_csv" in result.export_status:
        typer.echo(f"Pair CSV          : {result.export_status['pair_csv']}")
    if "model_ready_pairs_csv" in result.export_status:
        typer.echo(f"Model-ready CSV   : {result.export_status['model_ready_pairs_csv']}")
    if "source_state_csv" in result.export_status:
        typer.echo(f"Source State CSV  : {result.export_status['source_state_csv']}")


@app.command("build-structural-graphs")
def build_structural_graphs_cmd(
    ctx: typer.Context,
    graph_level: Annotated[str, typer.Option(help="residue | atom")] = "residue",
    scope: Annotated[str, typer.Option(help="whole_protein | interface_only | shell")] = "whole_protein",
    shell_radius: Annotated[float, typer.Option(help="Neighborhood shell radius in angstroms.")] = 8.0,
    selection: Annotated[
        str,
        typer.Option(help="all | refresh_plan | training_set | preview"),
    ] = "all",
    export_formats: Annotated[list[str] | None, typer.Option("--export-format", help="Repeatable: pyg | dgl | networkx")] = None,
    pdb_ids: Annotated[list[str] | None, typer.Option("--pdb-id", help="Repeatable explicit PDB ID selection.")] = None,
    manifest_path: Annotated[Optional[Path], typer.Option("--manifest-path", help="Optional manifest with records[].pdb_id entries.")] = None,
    source_csv: Annotated[Optional[Path], typer.Option("--source-csv", help="Optional CSV with a pdb_id-style column.")] = None,
    limit: Annotated[Optional[int], typer.Option("--limit", min=1, help="Optional cap on selected PDB IDs to graph.")] = None,
    only_missing: Annotated[bool, typer.Option("--only-missing/--force-rebuild", help="Reuse current graph artifacts when the source structure has not changed.")] = True,
) -> None:
    """Build residue- or atom-level structural graphs for ML workflows."""
    from pbdata.graph.structural_graphs import build_structural_graphs

    layout = _storage_layout(ctx)
    formats = tuple(export_formats or ["pyg", "networkx"])
    selected_hint = len(pdb_ids or []) if pdb_ids else (limit or 0)
    write_stage_state(
        layout,
        stage="build-structural-graphs",
        status="running",
        input_dir=layout.extracted_dir,
        output_dir=layout.workspace_graphs_dir / f"{graph_level}_{scope}",
        workers=1,
        counts={"inputs_hint": selected_hint},
        notes=f"Building {graph_level}/{scope} structural graphs from selection '{selection}'.",
    )
    try:
        with stage_lock(layout, stage="build-structural-graphs"):
            artifacts = build_structural_graphs(
                layout,
                graph_level=graph_level,
                scope=scope,
                shell_radius=shell_radius,
                export_formats=formats,
                selection=selection,
                pdb_ids=pdb_ids,
                manifest_path=manifest_path,
                source_csv=source_csv,
                limit=limit,
                only_missing=only_missing,
            )
    except (ModuleNotFoundError, ImportError, RuntimeError, ValueError) as exc:
        write_stage_state(
            layout,
            stage="build-structural-graphs",
            status="failed",
            input_dir=layout.extracted_dir,
            output_dir=layout.workspace_graphs_dir / f"{graph_level}_{scope}",
            workers=1,
            counts={"inputs_hint": selected_hint},
            notes=f"Structural graph build failed: {exc}",
        )
        _exit_with_dependency_error(exc)
    write_stage_state(
        layout,
        stage="build-structural-graphs",
        status="completed",
        input_dir=layout.extracted_dir,
        output_dir=layout.workspace_graphs_dir / f"{graph_level}_{scope}",
        workers=1,
        counts={
            "selected": int(artifacts.get("selected_count", "0") or 0),
            "processed": int(artifacts.get("processed_count", "0") or 0),
            "reused": int(artifacts.get("skipped_count", "0") or 0),
        },
        notes=f"Structural graph build completed for {graph_level}/{scope} using selection '{selection}'.",
    )
    typer.echo(f"Storage root: {layout.root}")
    typer.echo(f"Structural graph manifest written to {artifacts['manifest']}")
    typer.echo(f"Selected PDBs: {artifacts.get('selected_count', '0')}")
    typer.echo(f"Built graphs : {artifacts.get('processed_count', '0')}")
    typer.echo(f"Reused graphs: {artifacts.get('skipped_count', '0')}")


@app.command("build-graph")
def build_graph_cmd(
    ctx: typer.Context,
    strict_prereqs: Annotated[
        bool,
        typer.Option("--strict-prereqs", help="Exit with an error instead of writing a planned manifest when upstream inputs are missing."),
    ] = False,
) -> None:
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

    if strict_prereqs:
        typer.echo("Run 'extract' first so graph materialization has extracted entry records.")
        raise typer.Exit(code=1)

    manifest_path = build_graph_manifest(layout.graph_dir)
    _emit_planned_manifest_notice(
        artifact_label="Graph architecture",
        manifest_path=manifest_path,
        missing_steps=["Run 'extract' first so graph materialization has extracted entry records."],
    )


# ---------------------------------------------------------------------------
# build-features
# ---------------------------------------------------------------------------


@app.command("build-features")
def build_features_cmd(
    ctx: typer.Context,
    strict_prereqs: Annotated[
        bool,
        typer.Option("--strict-prereqs", help="Exit with an error instead of writing a planned manifest when upstream inputs are missing."),
    ] = False,
) -> None:
    """Materialize first-pass features when extracted+graph data are present."""
    layout = _storage_layout(ctx)
    from pbdata.master_export import refresh_master_exports
    from pbdata.features.builder import (
        build_feature_manifest,
        build_features_from_extracted_and_graph,
    )

    extracted_dir = layout.extracted_dir
    graph_dir = layout.graph_dir
    has_assays = (extracted_dir / "assays").exists()
    has_graph_edges = (graph_dir / "graph_edges.json").exists()
    if has_assays and has_graph_edges:
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

    missing_steps: list[str] = []
    if not has_assays:
        missing_steps.append("Run 'extract' first so assay records exist.")
    if not has_graph_edges:
        missing_steps.append("Run 'build-graph' first so canonical graph edges exist.")
    if strict_prereqs:
        for step in missing_steps:
            typer.echo(step)
        raise typer.Exit(code=1)

    manifest_path = build_feature_manifest(layout.features_dir)
    _emit_planned_manifest_notice(
        artifact_label="Feature architecture",
        manifest_path=manifest_path,
        missing_steps=missing_steps,
    )


# ---------------------------------------------------------------------------
# build-training-examples
# ---------------------------------------------------------------------------


@app.command("build-training-examples")
def build_training_examples_cmd(
    ctx: typer.Context,
    strict_prereqs: Annotated[
        bool,
        typer.Option("--strict-prereqs", help="Exit with an error instead of writing a planned manifest when upstream inputs are missing."),
    ] = False,
) -> None:
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

    missing_steps: list[str] = []
    if not has_assays:
        missing_steps.append("Run 'extract' first so extracted assay rows exist.")
    if not has_graph:
        missing_steps.append("Run 'build-graph' first so canonical graph nodes exist.")
    if not has_features:
        missing_steps.append("Run 'build-features' first so feature records exist.")
    if strict_prereqs:
        for step in missing_steps:
            typer.echo(step)
        raise typer.Exit(code=1)

    manifest_path = build_training_manifest(layout.training_dir)
    _emit_planned_manifest_notice(
        artifact_label="Training-example architecture",
        manifest_path=manifest_path,
        missing_steps=missing_steps,
    )


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
    dataset_output_dir = layout.workspace_datasets_dir / dataset_name
    write_stage_state(
        layout,
        stage="engineer-dataset",
        status="running",
        input_dir=layout.root,
        output_dir=dataset_output_dir,
        workers=1,
        counts={},
        notes=f"Engineering dataset '{dataset_name}' from the current curated workspace artifacts.",
    )
    try:
        with stage_lock(layout, stage="engineer-dataset"):
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
        write_stage_state(
            layout,
            stage="engineer-dataset",
            status="failed",
            input_dir=layout.root,
            output_dir=dataset_output_dir,
            workers=1,
            counts={},
            notes=f"Engineered dataset export failed: {exc}",
        )
        _exit_with_dependency_error(exc)
    write_stage_state(
        layout,
        stage="engineer-dataset",
        status="completed",
        input_dir=layout.root,
        output_dir=dataset_output_dir,
        workers=1,
        counts={
            "train_rows": _count_delimited_rows(Path(artifacts["train_csv"])) or 0 if "train_csv" in artifacts else 0,
            "test_rows": _count_delimited_rows(Path(artifacts["test_csv"])) or 0 if "test_csv" in artifacts else 0,
            "cv_folds": cv_folds,
        },
        notes=f"Engineered dataset '{dataset_name}' exported with diversity, feature, and graph coverage metadata.",
    )
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
    split_mode:    Annotated[str,   typer.Option(help="auto | pair-aware | legacy-sequence | hash | scaffold | family | mutation | source | time")] = "auto",
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
        build_grouped_pair_splits,
        build_pair_aware_splits,
        build_splits,
        build_temporal_pair_splits,
        cluster_aware_split,
        export_split_diagnostics,
        save_splits,
    )

    layout = _storage_layout(ctx)
    if split_mode not in {"auto", "pair-aware", "legacy-sequence", "hash", "scaffold", "family", "mutation", "source", "time"}:
        raise typer.BadParameter("split-mode must be one of: auto, pair-aware, legacy-sequence, hash, scaffold, family, mutation, source, time")
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
        diagnostics_json, diagnostics_md, _ = export_split_diagnostics(
            pair_items,
            result,
            layout.splits_dir,
            strategy=strategy,
            extra_metadata=extra_metadata,
        )
        sizes = result.sizes()
        typer.echo(f"Train: {sizes['train']:,}  Val: {sizes['val']:,}  Test: {sizes['test']:,}")
        typer.echo(f"Storage root: {layout.root}")
        typer.echo(f"Splits written to {layout.splits_dir}/")
        typer.echo(f"Split diagnostics JSON: {diagnostics_json}")
        typer.echo(f"Split diagnostics Markdown: {diagnostics_md}")
        return

    if split_mode in {"scaffold", "family", "mutation", "source"}:
        if not pair_items:
            typer.echo("No pair-aware items available. Run 'extract' first, and build training examples for the strongest scaffold grouping.")
            return
        typer.echo(f"Building {split_mode}-grouped splits from {len(pair_items):,} pair-level items...")
        result, extra_metadata = build_grouped_pair_splits(
            pair_items,
            grouping=split_mode,
            train_frac=train_frac,
            val_frac=val_frac,
            seed=seed,
            log_fn=typer.echo,
        )
        strategy = f"{split_mode}_grouped"
        save_splits(result, layout.splits_dir, seed=seed, strategy=strategy, extra_metadata=extra_metadata)
        diagnostics_json, diagnostics_md, _ = export_split_diagnostics(
            pair_items,
            result,
            layout.splits_dir,
            strategy=strategy,
            extra_metadata=extra_metadata,
        )
        sizes = result.sizes()
        typer.echo(f"Train: {sizes['train']:,}  Val: {sizes['val']:,}  Test: {sizes['test']:,}")
        typer.echo(f"Storage root: {layout.root}")
        typer.echo(f"Splits written to {layout.splits_dir}/")
        typer.echo(f"Split diagnostics JSON: {diagnostics_json}")
        typer.echo(f"Split diagnostics Markdown: {diagnostics_md}")
        return

    if split_mode == "time":
        if not pair_items:
            typer.echo("No pair-aware items available. Run 'extract' first so entry release dates are available for temporal splitting.")
            return
        typer.echo(f"Building time-ordered splits from {len(pair_items):,} pair-level items...")
        result, extra_metadata = build_temporal_pair_splits(
            pair_items,
            train_frac=train_frac,
            val_frac=val_frac,
            seed=seed,
            log_fn=typer.echo,
        )
        strategy = "time_ordered"
        save_splits(result, layout.splits_dir, seed=seed, strategy=strategy, extra_metadata=extra_metadata)
        diagnostics_json, diagnostics_md, _ = export_split_diagnostics(
            pair_items,
            result,
            layout.splits_dir,
            strategy=strategy,
            extra_metadata=extra_metadata,
        )
        sizes = result.sizes()
        typer.echo(f"Train: {sizes['train']:,}  Val: {sizes['val']:,}  Test: {sizes['test']:,}")
        typer.echo(f"Storage root: {layout.root}")
        typer.echo(f"Splits written to {layout.splits_dir}/")
        typer.echo(f"Split diagnostics JSON: {diagnostics_json}")
        typer.echo(f"Split diagnostics Markdown: {diagnostics_md}")
        return

    if not files:
        typer.echo(f"No processed records found in {processed_dir}. Run 'normalize' first.")
        return

    typer.echo(f"Loading {len(files):,} processed records...")
    sample_ids: list[str] = []
    sequences:  list[str | None] = []
    skipped_files: list[str] = []
    for f in files:
        try:
            raw = json.loads(f.read_text(encoding="utf-8"))
            sample_ids.append(raw["sample_id"])
            sequences.append(raw.get("sequence_receptor"))
        except Exception:
            if len(skipped_files) < 5:
                skipped_files.append(f.name)
    skipped_count = len(files) - len(sample_ids)
    if skipped_count:
        preview = ", ".join(skipped_files)
        suffix = f" Examples: {preview}." if preview else ""
        typer.echo(
            f"Skipped {skipped_count:,} unreadable or invalid processed record(s) while building splits.{suffix}"
        )

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
    force: Annotated[
        bool,
        typer.Option("--force", help="Rebuild extracted bundles even when cached outputs already exist."),
    ] = False,
    existing_only: Annotated[
        bool,
        typer.Option("--existing-only", help="Only rebuild entries that already exist in the extracted output directory."),
    ] = False,
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
    worker_count = _coerce_workers(workers)

    files = sorted(raw_dir.glob("*.json"))
    if existing_only:
        existing_ids = _existing_extracted_pdb_ids(out_dir)
        if not existing_ids:
            typer.echo(
                f"No existing extracted entry bundles found in {out_dir / 'entry'}. "
                "Run extract without --existing-only first."
            )
            return
        files = [path for path in files if path.stem.upper() in existing_ids]
    if not files:
        typer.echo(f"No raw files found in {raw_dir}. Run 'ingest' first.")
        return

    try:
        with stage_lock(layout, stage="extract"):
            cfg: AppConfig = ctx.obj.get("config", AppConfig())
            structure_mirror = str(cfg.sources.rcsb.extra.get("structure_mirror") or "rcsb").strip().lower()
            assay_samples_by_pdb = _load_external_assay_samples(cfg, layout=layout)
            source_state_baseline = snapshot_source_state_counters(layout)
            external_assay_sample_count = sum(len(samples) for samples in assay_samples_by_pdb.values())
            enabled_enrichment_sources = [
                label
                for label, enabled in [
                    ("BindingDB", cfg.sources.bindingdb.enabled),
                    ("ChEMBL", cfg.sources.chembl.enabled),
                    ("PDBbind", cfg.sources.pdbbind.enabled),
                    ("BioLiP", cfg.sources.biolip.enabled),
                    ("SKEMPI", cfg.sources.skempi.enabled),
                ]
                if enabled
            ]

            typer.echo("Extraction plan:")
            typer.echo(f"  Storage root: {layout.root}")
            typer.echo(f"  Raw input dir: {raw_dir}")
            typer.echo(f"  Output dir: {out_dir}")
            typer.echo(f"  Workers: {worker_count}")
            typer.echo(
                "  Structure downloads: "
                f"{'enabled' if download_structures else 'disabled'}"
                + (f" ({structure_mirror})" if download_structures else "")
            )
            typer.echo(f"  Optional PDB downloads: {'enabled' if download_pdb else 'disabled'}")
            typer.echo(f"  Cache policy: {'force rebuild' if force else 'reuse valid extracted bundles'}")
            typer.echo(f"  Scope: {'existing extracted entries only' if existing_only else 'all raw entries'}")
            typer.echo(
                "  Enrichment sources: "
                + (", ".join(enabled_enrichment_sources) if enabled_enrichment_sources else "none enabled")
            )
            typer.echo(
                "  Preloaded external assay samples: "
                f"{external_assay_sample_count:,} across {len(assay_samples_by_pdb):,} PDB IDs"
            )

            # Collect ligand comp_ids for batch descriptor fetch
            typer.echo(f"Scanning {len(files):,} RCSB records for ligand IDs...")
            comp_ids: list[str] = []
            raw_data: list[tuple[Path, dict]] = []
            unreadable_inputs = 0
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
                    unreadable_inputs += 1

            if unreadable_inputs:
                typer.echo(
                    f"Skipped {unreadable_inputs:,} unreadable raw input record(s) before extraction started."
                )
            typer.echo(
                f"Loaded {len(raw_data):,} readable raw record(s) from {len(files):,} discovered file(s)."
            )

            chem_descriptors: dict[str, dict[str, str]] = {}
            if comp_ids:
                unique = list(dict.fromkeys(comp_ids))
                typer.echo(f"Fetching chem-comp descriptors for {len(unique):,} ligands...")
                try:
                    chem_descriptors = fetch_chemcomp_descriptors(unique)
                    typer.echo(f"  Got descriptors for {len(chem_descriptors):,} ligands.")
                except Exception as exc:
                    logger.warning("Chem-comp fetch failed: %s", exc)
            else:
                typer.echo("No nonpolymer ligand IDs were found in the raw records.")

            typer.echo(f"Extracting {len(raw_data):,} entries to multi-table records...")
            ok = cached = failed = 0
            write_stage_state(
                layout,
                stage="extract",
                status="running",
                input_dir=raw_dir,
                output_dir=out_dir,
                workers=worker_count,
                counts={"inputs": len(raw_data)},
                notes=(
                    "Extraction in progress. A workspace-local stage lock prevents "
                    "concurrent extract runs from rewriting the same tables."
                ),
            )

            stage_state_last_update = time.monotonic()
            progress_last_log = time.monotonic()
            run_started_at = time.monotonic()

            def _write_running_progress(*, active_count: int, active_preview: str = "") -> None:
                nonlocal stage_state_last_update
                now = time.monotonic()
                if now - stage_state_last_update < _EXTRACT_STAGE_STATE_UPDATE_SECONDS:
                    return
                write_stage_state(
                    layout,
                    stage="extract",
                    status="running",
                    input_dir=raw_dir,
                    output_dir=out_dir,
                    workers=worker_count,
                    counts={
                        "inputs": len(raw_data),
                        "processed": processed_count,
                        "extracted": ok,
                        "cached": cached,
                        "failed": failed,
                        "active": active_count,
                    },
                    notes=(
                        f"Extraction in progress for {processed_count:,}/{len(raw_data):,} entries."
                        + (f" Active sample: {active_preview}." if active_preview else "")
                    ),
                )
                stage_state_last_update = now

            def _emit_progress_update(prefix: str, *, active_count: int, active_preview: str = "") -> None:
                elapsed_seconds = max(time.monotonic() - run_started_at, 0.0)
                rate = processed_count / elapsed_seconds if elapsed_seconds > 0 else 0.0
                remaining = max(len(raw_data) - processed_count, 0)
                eta_seconds = int(round(remaining / rate)) if rate > 0 else None
                percent = (processed_count / len(raw_data) * 100.0) if raw_data else 100.0
                parts = [
                    f"{prefix} {processed_count:,}/{len(raw_data):,} processed",
                    f"({percent:.1f}%)",
                    f"ok={ok:,}",
                    f"cached={cached:,}",
                    f"failed={failed:,}",
                    f"active={active_count:,}",
                    f"rate={rate:.1f}/s",
                ]
                if eta_seconds is not None:
                    parts.append(f"eta~{eta_seconds}s")
                if active_preview:
                    parts.append(f"active sample: {active_preview}")
                typer.echo("  " + " | ".join(parts))
                _write_running_progress(active_count=active_count, active_preview=active_preview)

            def _extract_one(item: tuple[Path, dict]) -> tuple[str, str]:
                path, raw = item
                pdb_id = str(raw.get("rcsb_id") or "").upper()
                if (
                    not force
                    and _validate_extracted_bundle(out_dir, pdb_id)
                    and _is_up_to_date(path, out_dir / "entry" / f"{pdb_id}.json")
                ):
                    return path.name, "cached"
                _delete_extracted_bundle(out_dir, pdb_id)
                chembl_samples = _fetch_chembl_samples_for_raw(raw, chem_descriptors, cfg, layout=layout)
                bindingdb_samples = _fetch_bindingdb_samples_for_pdb(pdb_id, cfg, layout=layout, raw=raw)
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
                    path, raw = item
                    pdb_id = str(raw.get("rcsb_id") or path.stem).upper()
                    item_status: str | None = None
                    item_started_at = time.monotonic()
                    item_done = threading.Event()

                    def _single_worker_heartbeat() -> None:
                        while not item_done.wait(_EXTRACT_HEARTBEAT_SECONDS):
                            active_preview = f"{pdb_id} ({int(time.monotonic() - item_started_at)}s)"
                            _emit_progress_update(
                                "heartbeat:",
                                active_count=1,
                                active_preview=active_preview,
                            )

                    heartbeat_thread = threading.Thread(
                        target=_single_worker_heartbeat,
                        name=f"extract-heartbeat-{pdb_id}",
                        daemon=True,
                    )
                    heartbeat_thread.start()
                    try:
                        _, item_status = _extract_one(item)
                        if item_status == "cached":
                            cached += 1
                        else:
                            ok += 1
                    except Exception as exc:
                        logger.warning("Failed to extract %s: %s", item[0].name, exc)
                        typer.echo(f"  ERROR {pdb_id}: {exc}")
                        failed += 1
                    finally:
                        item_done.set()
                        heartbeat_thread.join(timeout=0.05)
                    processed_count += 1
                    active_preview = (
                        f"{pdb_id} ({'cached' if item_status == 'cached' else 'complete'})"
                        if item_status is not None
                        else pdb_id
                    )
                    if processed_count % _EXTRACT_PROGRESS_EVERY == 0:
                        _emit_progress_update("progress:", active_count=0, active_preview=active_preview)
                    else:
                        _write_running_progress(active_count=0, active_preview=active_preview)
            else:
                active_jobs: dict[object, tuple[str, float]] = {}
                active_lock = threading.Lock()

                def _extract_tracked(item: tuple[Path, dict]) -> tuple[str, str]:
                    path, raw = item
                    pdb_id = str(raw.get("rcsb_id") or path.stem).upper()
                    with active_lock:
                        active_jobs[threading.current_thread().ident or id(threading.current_thread())] = (
                            pdb_id,
                            time.monotonic(),
                        )
                    try:
                        return _extract_one(item)
                    finally:
                        with active_lock:
                            active_jobs.pop(threading.current_thread().ident or id(threading.current_thread()), None)

                with ThreadPoolExecutor(max_workers=worker_count) as executor:
                    futures = {
                        executor.submit(_extract_tracked, item): (
                            item[0].name,
                            str(item[1].get("rcsb_id") or item[0].stem).upper(),
                        )
                        for item in raw_data
                    }
                    pending = set(futures)
                    while pending:
                        done, pending = wait(
                            pending,
                            timeout=_EXTRACT_HEARTBEAT_SECONDS,
                            return_when=FIRST_COMPLETED,
                        )
                        if not done:
                            with active_lock:
                                active_snapshot = sorted(
                                    active_jobs.values(),
                                    key=lambda payload: payload[1],
                                )
                            preview_items = [
                                f"{pdb_id} ({int(time.monotonic() - started_at)}s)"
                                for pdb_id, started_at in active_snapshot[:_EXTRACT_ACTIVE_SAMPLE_LIMIT]
                            ]
                            _emit_progress_update(
                                "heartbeat:",
                                active_count=len(active_snapshot),
                                active_preview=", ".join(preview_items),
                            )
                            progress_last_log = time.monotonic()
                            continue

                        for future in done:
                            name, pdb_id = futures[future]
                            try:
                                _, status = future.result()
                                if status == "cached":
                                    cached += 1
                                else:
                                    ok += 1
                            except Exception as exc:
                                logger.warning("Failed to extract %s: %s", name, exc)
                                typer.echo(f"  ERROR {pdb_id}: {exc}")
                                failed += 1
                            processed_count += 1
                            with active_lock:
                                active_snapshot = sorted(
                                    active_jobs.values(),
                                    key=lambda payload: payload[1],
                                )
                            preview_items = [
                                f"{active_pdb} ({int(time.monotonic() - started_at)}s)"
                                for active_pdb, started_at in active_snapshot[:_EXTRACT_ACTIVE_SAMPLE_LIMIT]
                            ]
                            now = time.monotonic()
                            if (
                                processed_count % _EXTRACT_PROGRESS_EVERY == 0
                                or now - progress_last_log >= _EXTRACT_HEARTBEAT_SECONDS
                                or not pending
                            ):
                                _emit_progress_update(
                                    "progress:",
                                    active_count=len(active_snapshot),
                                    active_preview=", ".join(preview_items),
                                )
                                progress_last_log = now
                            else:
                                _write_running_progress(
                                    active_count=len(active_snapshot),
                                    active_preview=", ".join(preview_items),
                                )

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
    except RuntimeError as exc:
        typer.echo(str(exc))
        raise typer.Exit(code=1) from exc
    except Exception as exc:
        write_stage_state(
            layout,
            stage="extract",
            status="failed",
            input_dir=raw_dir,
            output_dir=out_dir,
            workers=_coerce_workers(workers),
            counts={"inputs": len(files)},
            notes=f"Extraction failed: {exc}",
        )
        raise

    typer.echo(f"Extraction complete. OK: {ok:,}, Cached: {cached:,}, Failed: {failed:,}")
    typer.echo(f"Storage root: {layout.root}")
    typer.echo(f"Output: {out_dir}/")
    if download_structures:
        typer.echo(f"Structures: {struct_dir}/")
    typer.echo(f"Stage state: {state_path}")
    source_summary_json, source_summary_md, _ = export_source_state_run_summary(
        layout,
        baseline=source_state_baseline,
        stage_name="extract",
    )
    typer.echo(f"Source Run Summary JSON: {source_summary_json}")
    typer.echo(f"Source Run Summary Markdown: {source_summary_md}")
    assay_row_count = _count_extracted_assay_rows(out_dir)
    if assay_row_count == 0:
        typer.echo(
            "Warning: extracted assay table is empty. Enable external assay sources or "
            "verify enrichment inputs if you expected binding measurements."
        )
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


@app.command("report-storage")
def report_storage_cmd(ctx: typer.Context) -> None:
    """Report disk usage across the main managed storage areas."""
    layout = _storage_layout(ctx)
    report = build_storage_usage_report(layout)
    for line in render_storage_usage_report(report):
        typer.echo(line)


@app.command("prune-storage")
def prune_storage_cmd(
    ctx: typer.Context,
    apply: Annotated[
        bool,
        typer.Option("--apply", help="Actually delete the safe prune targets instead of only previewing them."),
    ] = False,
    run_id: Annotated[
        Optional[str],
        typer.Option("--run-id", help="Limit pruning to one precompute run identifier."),
    ] = None,
    include_precompute_shards: Annotated[
        bool,
        typer.Option(
            "--include-precompute-shards/--skip-precompute-shards",
            help="Include redundant per-chunk shard outputs for merged precompute runs.",
        ),
    ] = True,
    include_precompute_merged: Annotated[
        bool,
        typer.Option(
            "--include-precompute-merged/--skip-precompute-merged",
            help="Include redundant merged copies for merged precompute runs.",
        ),
    ] = True,
) -> None:
    """Preview or delete safe regenerable storage artifacts."""
    layout = _storage_layout(ctx)
    if apply:
        result = prune_storage(
            layout,
            include_precompute_shards=include_precompute_shards,
            include_precompute_merged=include_precompute_merged,
            run_id=run_id,
        )
        for line in render_storage_prune_result(result):
            typer.echo(line)
        return

    plan = build_storage_prune_plan(
        layout,
        include_precompute_shards=include_precompute_shards,
        include_precompute_merged=include_precompute_merged,
        run_id=run_id,
    )
    for line in render_storage_prune_plan(plan):
        typer.echo(line)
    typer.echo("Dry run only. Re-run with --apply to delete these paths.")


if __name__ == "__main__":
    app()
