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


def _fetch_bindingdb_samples_for_pdb(
    pdb_id: str,
    config: AppConfig,
    *,
    layout: StorageLayout,
) -> list:
    if not config.sources.bindingdb.enabled or not pdb_id:
        return []

    from pbdata.sources.bindingdb import BindingDBAdapter
    from pbdata.catalog import summarize_bulk_file, update_download_manifest

    local_dir = str(config.sources.bindingdb.extra.get("local_dir") or "").strip()
    local_cache_path = Path(local_dir) / f"{pdb_id.upper()}.json" if local_dir else None
    managed_cache_path = layout.raw_bindingdb_dir / f"{pdb_id.upper()}.json"
    cache_path = managed_cache_path
    cache_mode = "managed_cache"
    raw: dict | None = None

    if local_cache_path and reuse_existing_file(
        local_cache_path,
        validator=lambda path, expected=pdb_id: validate_bindingdb_raw_json(path, expected_pdb_id=expected),
    ):
        cache_path = local_cache_path
        cache_mode = "local_cache"
        raw = json.loads(cache_path.read_text(encoding="utf-8"))
    elif reuse_existing_file(
        managed_cache_path,
        validator=lambda path, expected=pdb_id: validate_bindingdb_raw_json(path, expected_pdb_id=expected),
    ):
        cache_path = managed_cache_path
        raw = json.loads(cache_path.read_text(encoding="utf-8"))
    else:
        managed_cache_path.parent.mkdir(parents=True, exist_ok=True)
        adapter = BindingDBAdapter()
        try:
            raw = adapter.fetch_metadata(pdb_id)
        except Exception as exc:
            logger.warning("BindingDB lookup failed for %s: %s", pdb_id, exc)
            write_source_state(
                layout,
                source_name="BindingDB",
                status="error",
                mode="live_api",
                record_id=pdb_id.upper(),
                notes=str(exc),
                extra={"configured_local_dir": local_dir or None},
            )
            return []
        managed_cache_path.write_text(json.dumps(raw, indent=2), encoding="utf-8")
        cache_path = managed_cache_path
        cache_mode = "live_api"
        if not validate_bindingdb_raw_json(cache_path, expected_pdb_id=pdb_id):
            managed_cache_path.unlink(missing_ok=True)
            logger.warning("BindingDB payload for %s was invalid and was removed.", pdb_id)
            write_source_state(
                layout,
                source_name="BindingDB",
                status="invalid_payload",
                mode="live_api",
                cache_path=managed_cache_path,
                record_id=pdb_id.upper(),
                extra={"configured_local_dir": local_dir or None},
            )
            return []
        update_download_manifest([
            summarize_bulk_file(
                source_database="BindingDB",
                source_record_id=pdb_id.upper(),
                pdb_id=pdb_id.upper(),
                raw_file_path=managed_cache_path,
                raw_format="json",
                downloaded_at=datetime.now(timezone.utc).isoformat(),
                title="BindingDB cache payload",
                task_hint="protein_ligand",
                notes="BindingDB enrichment payload cached by PDB ID.",
                status="cached",
            )
        ], layout.catalog_path)

    try:
        samples = BindingDBAdapter().normalize_all(raw or {})
    except Exception as exc:
        logger.warning("BindingDB normalization failed for %s: %s", pdb_id, exc)
        write_source_state(
            layout,
            source_name="BindingDB",
            status="normalization_error",
            mode=cache_mode,
            cache_path=cache_path,
            record_id=pdb_id.upper(),
            notes=str(exc),
            extra={"configured_local_dir": local_dir or None},
        )
        return []

    samples = [
        sample.model_copy(update={
            "provenance": {
                **(sample.provenance or {}),
                "cache_mode": cache_mode,
                "cache_path": str(cache_path),
                "configured_local_dir": local_dir or None,
            },
        })
        for sample in samples
    ]
    write_source_state(
        layout,
        source_name="BindingDB",
        status="ready",
        mode=cache_mode,
        cache_path=cache_path,
        record_id=pdb_id.upper(),
        record_count=len(samples),
        notes="BindingDB enrichment loaded and normalized.",
        extra={"configured_local_dir": local_dir or None},
    )
    return samples


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
    """Load locally available affinity sources for extract-time attachment."""
    grouped: dict[str, list] = defaultdict(list)

    if config.sources.skempi.enabled:
        from pbdata.sources.skempi import load_skempi_csv

        raw_path = config.sources.skempi.extra.get("local_path") or str(layout.raw_skempi_dir / "skempi_v2.csv")
        path = Path(raw_path)
        if path.exists():
            for sample in load_skempi_csv(path, download=False):
                if sample.pdb_id:
                    grouped[sample.pdb_id].append(sample)
        else:
            logger.warning("SKEMPI enabled but file not found: %s", path)

    if config.sources.pdbbind.enabled:
        from pbdata.sources.pdbbind import PDBbindAdapter

        local_dir_raw = config.sources.pdbbind.extra.get("local_dir")
        if local_dir_raw:
            local_dir = Path(str(local_dir_raw))
            if local_dir.exists():
                for sample in PDBbindAdapter(local_dir=local_dir).fetch_all():
                    if sample.pdb_id:
                        grouped[sample.pdb_id].append(sample)
            else:
                logger.warning("PDBbind enabled but local_dir not found: %s", local_dir)

    if config.sources.biolip.enabled:
        from pbdata.sources.biolip import BioLiPAdapter

        local_dir_raw = config.sources.biolip.extra.get("local_dir")
        if local_dir_raw:
            local_dir = Path(str(local_dir_raw))
            if local_dir.exists():
                for sample in BioLiPAdapter(local_dir=local_dir).fetch_all():
                    if sample.pdb_id:
                        grouped[sample.pdb_id].append(sample)
            else:
                logger.warning("BioLiP enabled but local_dir not found: %s", local_dir)

    return dict(grouped)


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
    if not config.sources.chembl.enabled:
        return []

    from pbdata.sources.chembl import ChEMBLAdapter

    accession_ids = _raw_uniprot_ids(raw)
    inchikeys = _raw_ligand_inchikeys(raw, chem_descriptors)
    if not accession_ids or not inchikeys:
        return []

    adapter = ChEMBLAdapter()
    results: list = []
    seen: set[str] = set()
    raw_pdb_id = str(raw.get("rcsb_id") or "").upper()
    chain_ids_by_uniprot = _raw_chain_ids_by_uniprot(raw)
    for accession in accession_ids:
        for inchikey in inchikeys:
            try:
                samples = adapter.fetch_by_uniprot_and_inchikey(accession, inchikey)
            except Exception as exc:
                logger.warning(
                    "ChEMBL lookup failed for accession=%s inchikey=%s: %s",
                    accession,
                    inchikey,
                    exc,
                )
                continue
            for sample in samples:
                if sample.sample_id in seen:
                    continue
                seen.add(sample.sample_id)
                chain_ids = chain_ids_by_uniprot.get(accession, sample.chain_ids_receptor or [])
                provenance = dict(sample.provenance or {})
                ligand_key = sample.ligand_inchi_key or sample.ligand_id or sample.ligand_smiles or "unknown_ligand"
                mutation_key = (
                    sample.mutation_string
                    or sample.wildtype_or_mutant
                    or f"mutation_unknown:{sample.source_record_id}"
                )
                provenance["pair_grouping_override"] = "|".join([
                    "protein_ligand",
                    raw_pdb_id or "-",
                    chain_group_key(chain_ids),
                    ligand_key,
                    mutation_key,
                ])
                results.append(sample.model_copy(update={
                    "pdb_id": raw_pdb_id or sample.pdb_id,
                    "chain_ids_receptor": chain_ids or sample.chain_ids_receptor,
                    "provenance": provenance,
                }))
    return results


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
# build-graph
# ---------------------------------------------------------------------------


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
    typer.echo(f"Manifest            : {artifacts['custom_training_manifest_json']}")
    typer.echo(f"Snapshot dir        : {artifacts['custom_training_snapshot_dir']}")


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
) -> None:
    """Freeze the current release artifacts into a versioned snapshot directory."""
    from pbdata.release_export import build_release_snapshot

    layout = _storage_layout(ctx)
    artifacts = build_release_snapshot(layout, release_tag=tag)
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
