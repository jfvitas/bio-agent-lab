import json
import logging
import statistics
from datetime import datetime, timezone
from collections import Counter
from collections import defaultdict
from pathlib import Path
from typing import Annotated, Optional

import typer

from pbdata.config import AppConfig, load_config
from pbdata.logging_config import setup_logging

app = typer.Typer(help="Protein binding dataset platform CLI.")

_DEFAULT_CONFIG      = Path("configs/sources.yaml")
_DEFAULT_LOG_CONFIG  = Path("configs/logging.yaml")
_DEFAULT_CRITERIA    = Path("configs/criteria.yaml")
_PROCESSED_DIR       = Path("data/processed/rcsb")
_AUDIT_DIR           = Path("data/audit")
_REPORTS_DIR         = Path("data/reports")
_SPLITS_DIR          = Path("data/splits")
_CATALOG_PATH        = Path("data/catalog/download_manifest.csv")

logger = logging.getLogger(__name__)


def _load_external_assay_samples(config: AppConfig) -> dict[str, list]:
    """Load locally available affinity sources for extract-time attachment."""
    grouped: dict[str, list] = defaultdict(list)

    if config.sources.skempi.enabled:
        from pbdata.sources.skempi import load_skempi_csv

        raw_path = config.sources.skempi.extra.get("local_path") or str(Path("data/raw/skempi/skempi_v2.csv"))
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
                results.append(sample)
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
    ctx.obj["config"] = load_config(cfg_path) if cfg_path is not None else AppConfig()


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
        _ingest_rcsb(dry_run=dry_run, yes=yes, criteria=criteria, output=output)
    elif source_lower == "skempi":
        _ingest_skempi(dry_run=dry_run, yes=yes, output=output)
    else:
        typer.echo(
            f"Unknown source: '{source}'.  Supported: rcsb, skempi.",
            err=True,
        )
        raise typer.Exit(code=1)


def _ingest_rcsb(
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

    out_dir = output if output is not None else Path("data/raw/rcsb")
    search_and_download(sc, out_dir, log_fn=typer.echo)
    typer.echo("RCSB ingest complete.")


def _ingest_skempi(*, dry_run: bool, yes: bool, output: Optional[Path]) -> None:
    import requests

    from pbdata.catalog import summarize_bulk_file, update_download_manifest
    from pbdata.sources.skempi import _SKEMPI_URL

    out_dir = output if output is not None else Path("data/raw/skempi")
    csv_path = out_dir / "skempi_v2.csv"
    downloaded_at = datetime.now(timezone.utc).isoformat()

    if csv_path.exists():
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
        ], _CATALOG_PATH)
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
    ], _CATALOG_PATH)
    typer.echo(f"SKEMPI CSV saved to {csv_path}")
    typer.echo(f"Download manifest updated at {_CATALOG_PATH}")
    typer.echo("Run 'normalize --source skempi' to convert to canonical records.")


# ---------------------------------------------------------------------------
# normalize
# ---------------------------------------------------------------------------

@app.command("normalize")
def normalize_cmd(ctx: typer.Context) -> None:
    """Normalize raw RCSB records from data/raw/rcsb/ into the canonical schema.

    Also fetches ligand SMILES / InChIKey from the RCSB chem-comp API for
    every unique ligand comp_id present in the raw records.
    """
    from pbdata.sources.rcsb import RCSBAdapter
    from pbdata.sources.rcsb_search import fetch_chemcomp_descriptors

    raw_dir = Path("data/raw/rcsb")
    out_dir = _PROCESSED_DIR
    out_dir.mkdir(parents=True, exist_ok=True)

    files = sorted(raw_dir.glob("*.json"))
    if not files:
        typer.echo(f"No raw files found in {raw_dir}. Run 'ingest' first.")
        return

    cfg: AppConfig = ctx.obj.get("config", AppConfig())
    assay_samples_by_pdb = _load_external_assay_samples(cfg)

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
    ok = failed = 0

    for f, raw in raw_data:
        try:
            record = adapter.normalize_record(raw, chem_descriptors=chem_descriptors)
            (out_dir / f.name).write_text(record.model_dump_json(indent=2))
            ok += 1
        except Exception as exc:
            logger.warning("Failed to normalize %s: %s", f.name, exc)
            failed += 1

    typer.echo(f"Done. Normalized: {ok:,}, Failed: {failed:,}")
    typer.echo(f"Output: {out_dir}")


# ---------------------------------------------------------------------------
# audit
# ---------------------------------------------------------------------------

@app.command()
def audit(ctx: typer.Context) -> None:
    """Score and flag all normalized records; write audit summary."""
    from pbdata.quality.audit import audit_record
    from pbdata.schemas.canonical_sample import CanonicalBindingSample

    files = sorted(_PROCESSED_DIR.glob("*.json")) if _PROCESSED_DIR.exists() else []
    if not files:
        typer.echo(f"No processed records found in {_PROCESSED_DIR}. Run 'normalize' first.")
        return

    typer.echo(f"Auditing {len(files):,} records...")
    _AUDIT_DIR.mkdir(parents=True, exist_ok=True)

    flag_counter: Counter[str] = Counter()
    scores: list[float] = []
    ok = failed = 0

    for f in files:
        try:
            raw = json.loads(f.read_text())
            record = CanonicalBindingSample.model_validate(raw)
            audited = audit_record(record)
            # Overwrite processed file with updated quality fields
            (_PROCESSED_DIR / f.name).write_text(audited.model_dump_json(indent=2))
            flag_counter.update(audited.quality_flags)
            scores.append(audited.quality_score)
            ok += 1
        except Exception as exc:
            logger.warning("Failed to audit %s: %s", f.name, exc)
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

    summary_path = _AUDIT_DIR / "audit_summary.json"
    summary_path.write_text(json.dumps(summary, indent=2))

    typer.echo(f"Audit complete. Mean quality score: {summary['quality_score']['mean']:.3f}")
    typer.echo(f"Top flags: {dict(flag_counter.most_common(5))}")
    typer.echo(f"Summary written to {summary_path}")


# ---------------------------------------------------------------------------
# report
# ---------------------------------------------------------------------------

@app.command()
def report(ctx: typer.Context) -> None:
    """Generate a summary statistics report over all processed records."""
    from pbdata.schemas.canonical_sample import CanonicalBindingSample

    files = sorted(_PROCESSED_DIR.glob("*.json")) if _PROCESSED_DIR.exists() else []
    if not files:
        typer.echo(f"No processed records found in {_PROCESSED_DIR}. Run 'normalize' first.")
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

    _REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    report_path = _REPORTS_DIR / "summary.json"
    report_path.write_text(json.dumps(rep, indent=2))

    typer.echo(f"\n{'─'*40}")
    typer.echo(f"Total records   : {total:,}")
    typer.echo(f"Task types      : {dict(task_counts)}")
    typer.echo(f"Methods         : {dict(method_counts.most_common(3))}")
    if resolutions:
        typer.echo(f"Resolution (Å)  : mean={statistics.mean(resolutions):.2f}  "
                   f"median={statistics.median(resolutions):.2f}")
    typer.echo(f"Mean quality    : {statistics.mean(scores):.3f}" if scores else "")
    typer.echo(f"Report written to {report_path}")


# ---------------------------------------------------------------------------
# build-splits
# ---------------------------------------------------------------------------

@app.command("build-splits")
def build_splits_cmd(
    ctx: typer.Context,
    train_frac:    Annotated[float, typer.Option(help="Train fraction.")] = 0.70,
    val_frac:      Annotated[float, typer.Option(help="Validation fraction.")] = 0.15,
    seed:          Annotated[int,   typer.Option(help="Hash seed for reproducibility.")] = 42,
    hash_only:     Annotated[bool,  typer.Option("--hash-only", help="Use fast hash split (no clustering).")] = False,
    threshold:     Annotated[float, typer.Option(help="Jaccard threshold for sequence clustering.")] = 0.30,
) -> None:
    """Build reproducible train/val/test splits from processed records.

    By default uses k-mer Jaccard clustering so that proteins with high
    sequence identity stay in the same partition (leakage prevention).
    Use --hash-only for a fast but leakage-prone hash-based split.

    Outputs train.txt, val.txt, test.txt, and metadata.json to data/splits/.
    """
    from pbdata.dataset.splits import build_splits, cluster_aware_split, save_splits

    files = sorted(_PROCESSED_DIR.glob("*.json")) if _PROCESSED_DIR.exists() else []
    if not files:
        typer.echo(f"No processed records found in {_PROCESSED_DIR}. Run 'normalize' first.")
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

    if hash_only or not has_sequences:
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

    save_splits(result, _SPLITS_DIR, seed=seed, strategy=strategy)

    sizes = result.sizes()
    typer.echo(f"Train: {sizes['train']:,}  Val: {sizes['val']:,}  Test: {sizes['test']:,}")
    typer.echo(f"Splits written to {_SPLITS_DIR}/")


# ---------------------------------------------------------------------------
# extract (multi-table output per spec)
# ---------------------------------------------------------------------------

_EXTRACT_DIR = Path("data/extracted")
_STRUCTURES_DIR = Path("data/structures/rcsb")


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
    from pbdata.pipeline.extract import extract_rcsb_entry, write_records_json
    from pbdata.sources.rcsb_search import fetch_chemcomp_descriptors

    raw_dir = Path("data/raw/rcsb")
    out_dir = output if output is not None else _EXTRACT_DIR
    struct_dir = structures if structures is not None else _STRUCTURES_DIR

    files = sorted(raw_dir.glob("*.json"))
    if not files:
        typer.echo(f"No raw files found in {raw_dir}. Run 'ingest' first.")
        return

    cfg: AppConfig = ctx.obj.get("config", AppConfig())
    assay_samples_by_pdb = _load_external_assay_samples(cfg)

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
    ok = failed = 0

    for f, raw in raw_data:
        try:
            pdb_id = str(raw.get("rcsb_id") or "").upper()
            chembl_samples = _fetch_chembl_samples_for_raw(raw, chem_descriptors, cfg)
            records = extract_rcsb_entry(
                raw,
                chem_descriptors=chem_descriptors,
                assay_samples=assay_samples_by_pdb.get(pdb_id, []) + chembl_samples,
                structures_dir=struct_dir if download_structures else None,
                download_structures=download_structures,
                download_pdb=download_pdb,
            )
            write_records_json(records, out_dir)
            ok += 1
        except Exception as exc:
            logger.warning("Failed to extract %s: %s", f.name, exc)
            failed += 1

        if (ok + failed) % 100 == 0:
            typer.echo(f"  {ok + failed:,}/{len(raw_data):,} processed...")

    typer.echo(f"Extraction complete. OK: {ok:,}, Failed: {failed:,}")
    typer.echo(f"Output: {out_dir}/")
    if download_structures:
        typer.echo(f"Structures: {struct_dir}/")


if __name__ == "__main__":
    app()
