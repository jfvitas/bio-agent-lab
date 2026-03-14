"""Generic shard-aware precompute helpers.

The initial implementation focuses on extraction because that is the most
expensive reusable preprocessing layer today. The run/chunk/merge layout is
generic so later stages can plug into the same structure.
"""

from __future__ import annotations

import json
import math
import os
import shutil
import threading
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from pbdata.config import AppConfig
from pbdata.pipeline.enrichment import (
    fetch_bindingdb_samples_for_pdb,
    fetch_chembl_samples_for_raw,
    load_external_assay_samples,
)
from pbdata.pipeline.extract import extract_rcsb_entry, write_records_json
from pbdata.sources.rcsb_search import fetch_chemcomp_descriptors
from pbdata.storage import StorageLayout

_SUPPORTED_STAGES = {
    "extract",
    "build-structural-graphs",
    "build-graph",
    "build-features",
    "build-training-examples",
}
_HEARTBEAT_SECONDS = 15.0
_ACTIVE_SAMPLE_LIMIT = 3
_PROGRESS_EVERY = 25


@dataclass(frozen=True)
class PrecomputePlanResult:
    run_id: str
    stage: str
    run_dir: Path
    manifest_path: Path
    chunk_dir: Path
    chunk_count: int
    total_inputs: int


@dataclass(frozen=True)
class PrecomputeShardResult:
    run_id: str
    stage: str
    chunk_index: int
    shard_dir: Path
    status_path: Path
    processed: int
    ok: int
    cached: int
    failed: int


@dataclass(frozen=True)
class PrecomputeMergeResult:
    run_id: str
    stage: str
    merged_dir: Path
    manifest_path: Path
    copied: int


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _ensure_supported_stage(stage: str) -> str:
    normalized = stage.strip().lower()
    if normalized not in _SUPPORTED_STAGES:
        raise ValueError(
            f"Unsupported precompute stage '{stage}'. Supported stages: {', '.join(sorted(_SUPPORTED_STAGES))}."
        )
    return normalized


def _run_root(layout: StorageLayout, run_id: str) -> Path:
    return layout.precompute_runs_dir / run_id


def _run_manifest_path(layout: StorageLayout, run_id: str) -> Path:
    return _run_root(layout, run_id) / "run_manifest.json"


def _status_dir(layout: StorageLayout, run_id: str) -> Path:
    return _run_root(layout, run_id) / "status"


def _chunk_dir(layout: StorageLayout, run_id: str) -> Path:
    return _run_root(layout, run_id) / "chunks"


def _shards_dir(layout: StorageLayout, run_id: str, stage: str) -> Path:
    return _run_root(layout, run_id) / "shards" / stage


def _merged_dir(layout: StorageLayout, run_id: str, stage: str) -> Path:
    return _run_root(layout, run_id) / "merged" / stage


def _chunk_filename(chunk_index: int) -> str:
    return f"chunk_{chunk_index:05d}.json"


def _chunk_status_filename(chunk_index: int) -> str:
    return f"chunk_{chunk_index:05d}.status.json"


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _coerce_workers(workers: int) -> int:
    if workers <= 0:
        return max(os.cpu_count() or 1, 1)
    return workers


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


def _delete_extracted_bundle(output_dir: Path, pdb_id: str) -> None:
    for table_name in ["entry", "chains", "bound_objects", "interfaces", "assays", "provenance"]:
        (output_dir / table_name / f"{pdb_id}.json").unlink(missing_ok=True)


def _is_up_to_date(source_path: Path, output_path: Path) -> bool:
    try:
        return output_path.stat().st_mtime >= source_path.stat().st_mtime
    except OSError:
        return False


def _stage_input_files(layout: StorageLayout, stage: str) -> list[Path]:
    if stage == "extract":
        return sorted(layout.raw_rcsb_dir.glob("*.json"))
    if stage in {"build-structural-graphs", "build-graph", "build-features", "build-training-examples"}:
        return sorted((layout.extracted_dir / "entry").glob("*.json"))
    raise ValueError(f"Unsupported precompute stage '{stage}'.")


def plan_precompute_run(
    layout: StorageLayout,
    *,
    stage: str,
    chunk_size: int = 500,
    chunk_count: int | None = None,
    run_id: str | None = None,
) -> PrecomputePlanResult:
    normalized_stage = _ensure_supported_stage(stage)
    files = _stage_input_files(layout, normalized_stage)
    if not files:
        if normalized_stage == "extract":
            raise FileNotFoundError(f"No raw extract inputs found in {layout.raw_rcsb_dir}.")
        raise FileNotFoundError(f"No extracted entry inputs found in {layout.extracted_dir / 'entry'}.")

    if chunk_count is not None and chunk_count <= 0:
        raise ValueError("--chunk-count must be positive when provided.")
    if chunk_size <= 0:
        raise ValueError("--chunk-size must be positive.")

    if chunk_count is None:
        chunk_count = max(math.ceil(len(files) / chunk_size), 1)
    else:
        chunk_size = max(math.ceil(len(files) / chunk_count), 1)

    run_id = run_id or f"{normalized_stage}_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    run_dir = _run_root(layout, run_id)
    manifest_path = _run_manifest_path(layout, run_id)
    chunk_dir = _chunk_dir(layout, run_id)
    status_dir = _status_dir(layout, run_id)
    if manifest_path.exists():
        raise FileExistsError(f"Precompute run '{run_id}' already exists at {run_dir}.")

    chunk_payloads: list[dict[str, Any]] = []
    for chunk_index in range(chunk_count):
        start = chunk_index * chunk_size
        stop = min(start + chunk_size, len(files))
        if start >= len(files):
            break
        chunk_files = files[start:stop]
        items = [
            {
                "pdb_id": path.stem.upper(),
                "source_path": str(path),
            }
            for path in chunk_files
        ]
        chunk_manifest = {
            "run_id": run_id,
            "stage": normalized_stage,
            "chunk_index": chunk_index,
            "created_at": _utc_now(),
            "input_count": len(items),
            "items": items,
        }
        chunk_path = chunk_dir / _chunk_filename(chunk_index)
        _write_json(chunk_path, chunk_manifest)
        chunk_payloads.append(
            {
                "chunk_index": chunk_index,
                "manifest_path": str(chunk_path),
                "input_count": len(items),
                "first_pdb_id": items[0]["pdb_id"] if items else None,
                "last_pdb_id": items[-1]["pdb_id"] if items else None,
            }
        )

    run_manifest = {
        "run_id": run_id,
        "stage": normalized_stage,
        "created_at": _utc_now(),
        "storage_root": str(layout.root),
        "input_dir": str(layout.raw_rcsb_dir if normalized_stage == "extract" else layout.extracted_dir / "entry"),
        "total_inputs": len(files),
        "chunk_count": len(chunk_payloads),
        "chunk_size": chunk_size,
        "chunks": chunk_payloads,
        "status": "planned",
    }
    _write_json(manifest_path, run_manifest)
    _write_json(
        status_dir / "summary.json",
        {
            "run_id": run_id,
            "stage": normalized_stage,
            "generated_at": _utc_now(),
            "status": "planned",
            "chunk_count": len(chunk_payloads),
            "completed_chunks": 0,
            "failed_chunks": 0,
            "total_inputs": len(files),
        },
    )
    return PrecomputePlanResult(
        run_id=run_id,
        stage=normalized_stage,
        run_dir=run_dir,
        manifest_path=manifest_path,
        chunk_dir=chunk_dir,
        chunk_count=len(chunk_payloads),
        total_inputs=len(files),
    )


def _load_chunk_manifest(layout: StorageLayout, run_id: str, chunk_index: int) -> dict[str, Any]:
    chunk_path = _chunk_dir(layout, run_id) / _chunk_filename(chunk_index)
    if not chunk_path.exists():
        raise FileNotFoundError(f"Chunk manifest not found: {chunk_path}")
    return _read_json(chunk_path)


def _write_chunk_status(
    layout: StorageLayout,
    run_id: str,
    chunk_index: int,
    payload: dict[str, Any],
) -> Path:
    status_path = _status_dir(layout, run_id) / _chunk_status_filename(chunk_index)
    _write_json(status_path, payload)
    return status_path


def _load_raw_items(chunk_manifest: dict[str, Any]) -> list[tuple[Path, dict[str, Any]]]:
    items: list[tuple[Path, dict[str, Any]]] = []
    for item in chunk_manifest.get("items") or []:
        source_path = Path(str(item.get("source_path") or ""))
        raw = json.loads(source_path.read_text(encoding="utf-8"))
        items.append((source_path, raw))
    return items


def _chunk_pdb_ids(chunk_manifest: dict[str, Any]) -> list[str]:
    return [
        str(item.get("pdb_id") or "").upper()
        for item in (chunk_manifest.get("items") or [])
        if str(item.get("pdb_id") or "").strip()
    ]


def _copy_extracted_subset(layout: StorageLayout, pdb_ids: list[str], target_root: Path) -> Path:
    subset_dir = target_root / "data" / "extracted"
    for table_name in ["entry", "chains", "bound_objects", "interfaces", "assays", "provenance"]:
        src_dir = layout.extracted_dir / table_name
        dst_dir = subset_dir / table_name
        dst_dir.mkdir(parents=True, exist_ok=True)
        if not src_dir.exists():
            continue
        for pdb_id in pdb_ids:
            src_path = src_dir / f"{pdb_id}.json"
            if src_path.exists():
                shutil.copy2(src_path, dst_dir / src_path.name)
    return subset_dir


def _copy_optional_pair_records(src_path: Path, dst_path: Path, pdb_ids: set[str]) -> None:
    if not src_path.exists():
        return
    rows = json.loads(src_path.read_text(encoding="utf-8"))
    filtered = [
        row for row in rows
        if isinstance(row, dict) and str(row.get("pdb_id") or "").upper() in pdb_ids
    ]
    dst_path.parent.mkdir(parents=True, exist_ok=True)
    dst_path.write_text(json.dumps(filtered, indent=2), encoding="utf-8")


def _copy_feature_subset(layout: StorageLayout, pdb_ids: list[str], target_root: Path) -> Path:
    subset_dir = target_root / "data" / "features"
    subset_dir.mkdir(parents=True, exist_ok=True)
    _copy_optional_pair_records(layout.features_dir / "feature_records.json", subset_dir / "feature_records.json", set(pdb_ids))
    for name in ["feature_manifest.json"]:
        src = layout.features_dir / name
        if src.exists():
            shutil.copy2(src, subset_dir / name)
    return subset_dir


def _copy_microstate_and_physics_subset(layout: StorageLayout, pdb_ids: list[str], target_root: Path) -> tuple[Path, Path]:
    micro_dir = target_root / "data" / "features" / "microstates"
    physics_dir = target_root / "data" / "features" / "physics"
    _copy_optional_pair_records(layout.microstates_dir / "microstate_records.json", micro_dir / "microstate_records.json", set(pdb_ids))
    _copy_optional_pair_records(layout.physics_dir / "physics_feature_records.json", physics_dir / "physics_feature_records.json", set(pdb_ids))
    return micro_dir, physics_dir


def _graph_subset_rows(layout: StorageLayout, pdb_ids: set[str]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    graph_nodes_path = layout.graph_dir / "graph_nodes.json"
    graph_edges_path = layout.graph_dir / "graph_edges.json"
    if not graph_nodes_path.exists() or not graph_edges_path.exists():
        return [], []
    nodes = json.loads(graph_nodes_path.read_text(encoding="utf-8"))
    edges = json.loads(graph_edges_path.read_text(encoding="utf-8"))
    allowed_nodes = {
        str(node.get("node_id") or "")
        for node in nodes
        if isinstance(node, dict) and str((node.get("metadata") or {}).get("pdb_id") or "").upper() in pdb_ids
    }
    filtered_edges = [
        edge for edge in edges
        if isinstance(edge, dict)
        and str(edge.get("source_node_id") or "") in allowed_nodes
        and str(edge.get("target_node_id") or "") in allowed_nodes
    ]
    referenced_node_ids = {
        str(edge.get("source_node_id") or "") for edge in filtered_edges
    } | {
        str(edge.get("target_node_id") or "") for edge in filtered_edges
    }
    filtered_nodes = [
        node for node in nodes
        if isinstance(node, dict)
        and (
            str(node.get("node_id") or "") in referenced_node_ids
            or str((node.get("metadata") or {}).get("pdb_id") or "").upper() in pdb_ids
        )
    ]
    return filtered_nodes, filtered_edges


def _copy_graph_subset(layout: StorageLayout, pdb_ids: list[str], target_root: Path, *, use_full_graph: bool = False) -> Path:
    subset_dir = target_root / "data" / "graph"
    subset_dir.mkdir(parents=True, exist_ok=True)
    source_manifest = layout.graph_dir / "graph_manifest.json"
    if use_full_graph:
        for name in ["graph_nodes.json", "graph_edges.json", "graph_manifest.json"]:
            src = layout.graph_dir / name
            if src.exists():
                shutil.copy2(src, subset_dir / name)
        return subset_dir
    nodes, edges = _graph_subset_rows(layout, set(pdb_ids))
    (subset_dir / "graph_nodes.json").write_text(json.dumps(nodes, indent=2), encoding="utf-8")
    (subset_dir / "graph_edges.json").write_text(json.dumps(edges, indent=2), encoding="utf-8")
    if source_manifest.exists():
        shutil.copy2(source_manifest, subset_dir / "graph_manifest.json")
    return subset_dir


def _copy_structural_graph_outputs(src_dir: Path, dst_dir: Path) -> int:
    copied = 0
    if not src_dir.exists():
        return copied
    dst_dir.mkdir(parents=True, exist_ok=True)
    for path in src_dir.glob("*"):
        if path.is_file():
            shutil.copy2(path, dst_dir / path.name)
            copied += 1
    return copied


def _emit_progress(
    log_fn: Callable[[str], None],
    *,
    prefix: str,
    processed_count: int,
    total_count: int,
    ok: int,
    cached: int,
    failed: int,
    run_started_at: float,
    active_preview: str = "",
    active_count: int = 0,
) -> None:
    elapsed_seconds = max(time.monotonic() - run_started_at, 0.0)
    rate = processed_count / elapsed_seconds if elapsed_seconds > 0 else 0.0
    remaining = max(total_count - processed_count, 0)
    eta_seconds = int(round(remaining / rate)) if rate > 0 else None
    percent = (processed_count / total_count * 100.0) if total_count else 100.0
    parts = [
        f"{prefix} {processed_count:,}/{total_count:,} processed",
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
    log_fn("  " + " | ".join(parts))


def run_extract_shard(
    layout: StorageLayout,
    *,
    run_id: str,
    chunk_index: int,
    config: AppConfig,
    workers: int = 1,
    force: bool = False,
    download_structures: bool = True,
    download_pdb: bool = False,
    log_fn: Callable[[str], None] | None = None,
) -> PrecomputeShardResult:
    log = log_fn or (lambda _message: None)
    chunk_manifest = _load_chunk_manifest(layout, run_id, chunk_index)
    raw_items = _load_raw_items(chunk_manifest)
    shard_dir = _shards_dir(layout, run_id, "extract") / f"chunk_{chunk_index:05d}"
    output_dir = shard_dir / "extracted"
    output_dir.mkdir(parents=True, exist_ok=True)
    status_payload = {
        "run_id": run_id,
        "stage": "extract",
        "chunk_index": chunk_index,
        "generated_at": _utc_now(),
        "status": "running",
        "input_count": len(raw_items),
        "shard_dir": str(shard_dir),
        "output_dir": str(output_dir),
    }
    status_path = _write_chunk_status(layout, run_id, chunk_index, status_payload)

    assay_samples_by_pdb = load_external_assay_samples(config, layout=layout)
    structure_mirror = str(config.sources.rcsb.extra.get("structure_mirror") or "rcsb").strip().lower()
    comp_ids: list[str] = []
    for _path, raw in raw_items:
        for ent in (raw.get("nonpolymer_entities") or []):
            cid = (((ent.get("nonpolymer_comp") or {}).get("chem_comp") or {}).get("id", ""))
            if cid:
                comp_ids.append(cid)
    chem_descriptors: dict[str, dict[str, str]] = {}
    if comp_ids:
        try:
            chem_descriptors = fetch_chemcomp_descriptors(list(dict.fromkeys(comp_ids)))
        except Exception as exc:
            log(f"Warning: chem-comp descriptor fetch failed for shard {chunk_index}: {exc}")

    worker_count = _coerce_workers(workers)
    ok = cached = failed = 0
    processed_count = 0
    run_started_at = time.monotonic()

    def _extract_one(item: tuple[Path, dict[str, Any]]) -> tuple[str, str]:
        path, raw = item
        pdb_id = str(raw.get("rcsb_id") or path.stem).upper()
        if (
            not force
            and _validate_extracted_bundle(output_dir, pdb_id)
            and _is_up_to_date(path, output_dir / "entry" / f"{pdb_id}.json")
        ):
            return pdb_id, "cached"
        _delete_extracted_bundle(output_dir, pdb_id)
        chembl_samples = fetch_chembl_samples_for_raw(raw, chem_descriptors, config, layout=layout)
        bindingdb_samples = fetch_bindingdb_samples_for_pdb(pdb_id, config, layout=layout, raw=raw)
        records = extract_rcsb_entry(
            raw,
            chem_descriptors=chem_descriptors,
            assay_samples=assay_samples_by_pdb.get(pdb_id, []) + bindingdb_samples + chembl_samples,
            structures_dir=layout.structures_rcsb_dir if download_structures else None,
            download_structures=download_structures,
            download_pdb=download_pdb,
            structure_mirror=structure_mirror,
        )
        write_records_json(records, output_dir)
        return pdb_id, "ok"

    if worker_count == 1:
        for item in raw_items:
            path, raw = item
            pdb_id = str(raw.get("rcsb_id") or path.stem).upper()
            item_started_at = time.monotonic()
            item_done = threading.Event()

            def _heartbeat() -> None:
                while not item_done.wait(_HEARTBEAT_SECONDS):
                    _emit_progress(
                        log,
                        prefix="heartbeat:",
                        processed_count=processed_count,
                        total_count=len(raw_items),
                        ok=ok,
                        cached=cached,
                        failed=failed,
                        run_started_at=run_started_at,
                        active_preview=f"{pdb_id} ({int(time.monotonic() - item_started_at)}s)",
                        active_count=1,
                    )

            heartbeat_thread = threading.Thread(target=_heartbeat, daemon=True)
            heartbeat_thread.start()
            try:
                _, status = _extract_one(item)
                if status == "cached":
                    cached += 1
                else:
                    ok += 1
            except Exception as exc:
                log(f"  ERROR {pdb_id}: {exc}")
                failed += 1
            finally:
                item_done.set()
                heartbeat_thread.join(timeout=0.05)
            processed_count += 1
            if processed_count % _PROGRESS_EVERY == 0 or processed_count == len(raw_items):
                _emit_progress(
                    log,
                    prefix="progress:",
                    processed_count=processed_count,
                    total_count=len(raw_items),
                    ok=ok,
                    cached=cached,
                    failed=failed,
                    run_started_at=run_started_at,
                    active_preview=f"{pdb_id} (complete)",
                    active_count=0,
                )
    else:
        active_jobs: dict[object, tuple[str, float]] = {}
        active_lock = threading.Lock()

        def _tracked(item: tuple[Path, dict[str, Any]]) -> tuple[str, str]:
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
            futures = {executor.submit(_tracked, item): item for item in raw_items}
            pending = set(futures)
            while pending:
                done, pending = wait(pending, timeout=_HEARTBEAT_SECONDS, return_when=FIRST_COMPLETED)
                if not done:
                    with active_lock:
                        active_snapshot = sorted(active_jobs.values(), key=lambda payload: payload[1])
                    preview = ", ".join(
                        f"{pdb_id} ({int(time.monotonic() - started_at)}s)"
                        for pdb_id, started_at in active_snapshot[:_ACTIVE_SAMPLE_LIMIT]
                    )
                    _emit_progress(
                        log,
                        prefix="heartbeat:",
                        processed_count=processed_count,
                        total_count=len(raw_items),
                        ok=ok,
                        cached=cached,
                        failed=failed,
                        run_started_at=run_started_at,
                        active_preview=preview,
                        active_count=len(active_snapshot),
                    )
                    continue
                for future in done:
                    path, raw = futures[future]
                    pdb_id = str(raw.get("rcsb_id") or path.stem).upper()
                    try:
                        _, status = future.result()
                        if status == "cached":
                            cached += 1
                        else:
                            ok += 1
                    except Exception as exc:
                        log(f"  ERROR {pdb_id}: {exc}")
                        failed += 1
                    processed_count += 1
                    with active_lock:
                        active_snapshot = sorted(active_jobs.values(), key=lambda payload: payload[1])
                    preview = ", ".join(
                        f"{active_pdb} ({int(time.monotonic() - started_at)}s)"
                        for active_pdb, started_at in active_snapshot[:_ACTIVE_SAMPLE_LIMIT]
                    )
                    if processed_count % _PROGRESS_EVERY == 0 or not pending:
                        _emit_progress(
                            log,
                            prefix="progress:",
                            processed_count=processed_count,
                            total_count=len(raw_items),
                            ok=ok,
                            cached=cached,
                            failed=failed,
                            run_started_at=run_started_at,
                            active_preview=preview,
                            active_count=len(active_snapshot),
                        )

    completed_payload = {
        **status_payload,
        "generated_at": _utc_now(),
        "status": "completed_with_failures" if failed else "completed",
        "processed": processed_count,
        "ok": ok,
        "cached": cached,
        "failed": failed,
    }
    status_path = _write_chunk_status(layout, run_id, chunk_index, completed_payload)
    return PrecomputeShardResult(
        run_id=run_id,
        stage="extract",
        chunk_index=chunk_index,
        shard_dir=shard_dir,
        status_path=status_path,
        processed=processed_count,
        ok=ok,
        cached=cached,
        failed=failed,
    )


def _run_structural_graphs_shard(
    layout: StorageLayout,
    *,
    run_id: str,
    chunk_index: int,
    graph_level: str,
    scope: str,
    shell_radius: float,
    export_formats: tuple[str, ...],
) -> PrecomputeShardResult:
    from pbdata.graph.structural_graphs import build_structural_graphs
    from pbdata.storage import build_storage_layout

    chunk_manifest = _load_chunk_manifest(layout, run_id, chunk_index)
    pdb_ids = _chunk_pdb_ids(chunk_manifest)
    shard_dir = _shards_dir(layout, run_id, "build-structural-graphs") / f"chunk_{chunk_index:05d}"
    temp_root = shard_dir / "workspace"
    _copy_extracted_subset(layout, pdb_ids, temp_root)
    temp_layout = build_storage_layout(temp_root)
    outputs = build_structural_graphs(
        temp_layout,
        graph_level=graph_level,
        scope=scope,
        shell_radius=shell_radius,
        export_formats=export_formats,
    )
    shard_output_dir = shard_dir / "structural_graphs" / f"{graph_level}_{scope}"
    copied = _copy_structural_graph_outputs(temp_layout.workspace_graphs_dir / f"{graph_level}_{scope}", shard_output_dir)
    status_path = _write_chunk_status(
        layout,
        run_id,
        chunk_index,
        {
            "run_id": run_id,
            "stage": "build-structural-graphs",
            "chunk_index": chunk_index,
            "generated_at": _utc_now(),
            "status": "completed",
            "input_count": len(pdb_ids),
            "processed": len(pdb_ids),
            "ok": len(pdb_ids),
            "cached": 0,
            "failed": 0,
            "copied_files": copied,
            "shard_dir": str(shard_dir),
            "output_dir": str(shard_output_dir),
            "manifest": outputs.get("manifest"),
        },
    )
    return PrecomputeShardResult(run_id, "build-structural-graphs", chunk_index, shard_dir, status_path, len(pdb_ids), len(pdb_ids), 0, 0)


def _run_graph_shard(layout: StorageLayout, *, run_id: str, chunk_index: int) -> PrecomputeShardResult:
    from pbdata.graph.builder import build_graph_from_extracted

    chunk_manifest = _load_chunk_manifest(layout, run_id, chunk_index)
    pdb_ids = _chunk_pdb_ids(chunk_manifest)
    shard_dir = _shards_dir(layout, run_id, "build-graph") / f"chunk_{chunk_index:05d}"
    temp_root = shard_dir / "workspace"
    extracted_dir = _copy_extracted_subset(layout, pdb_ids, temp_root)
    shard_graph_dir = shard_dir / "graph"
    nodes_path, edges_path, manifest_path = build_graph_from_extracted(extracted_dir, shard_graph_dir)
    status_path = _write_chunk_status(
        layout,
        run_id,
        chunk_index,
        {
            "run_id": run_id,
            "stage": "build-graph",
            "chunk_index": chunk_index,
            "generated_at": _utc_now(),
            "status": "completed",
            "input_count": len(pdb_ids),
            "processed": len(pdb_ids),
            "ok": len(pdb_ids),
            "cached": 0,
            "failed": 0,
            "shard_dir": str(shard_dir),
            "output_dir": str(shard_graph_dir),
            "nodes_path": str(nodes_path),
            "edges_path": str(edges_path),
            "manifest_path": str(manifest_path),
        },
    )
    return PrecomputeShardResult(run_id, "build-graph", chunk_index, shard_dir, status_path, len(pdb_ids), len(pdb_ids), 0, 0)


def _run_feature_shard(layout: StorageLayout, *, run_id: str, chunk_index: int) -> PrecomputeShardResult:
    from pbdata.features.builder import build_features_from_extracted_and_graph

    chunk_manifest = _load_chunk_manifest(layout, run_id, chunk_index)
    pdb_ids = _chunk_pdb_ids(chunk_manifest)
    shard_dir = _shards_dir(layout, run_id, "build-features") / f"chunk_{chunk_index:05d}"
    temp_root = shard_dir / "workspace"
    extracted_dir = _copy_extracted_subset(layout, pdb_ids, temp_root)
    graph_dir = _copy_graph_subset(layout, pdb_ids, temp_root, use_full_graph=True)
    micro_dir, physics_dir = _copy_microstate_and_physics_subset(layout, pdb_ids, temp_root)
    output_dir = shard_dir / "features"
    features_path, manifest_path = build_features_from_extracted_and_graph(
        extracted_dir,
        graph_dir,
        output_dir,
        microstate_dir=micro_dir,
        physics_dir=physics_dir,
    )
    status_path = _write_chunk_status(
        layout,
        run_id,
        chunk_index,
        {
            "run_id": run_id,
            "stage": "build-features",
            "chunk_index": chunk_index,
            "generated_at": _utc_now(),
            "status": "completed",
            "input_count": len(pdb_ids),
            "processed": len(pdb_ids),
            "ok": len(pdb_ids),
            "cached": 0,
            "failed": 0,
            "shard_dir": str(shard_dir),
            "output_dir": str(output_dir),
            "features_path": str(features_path),
            "manifest_path": str(manifest_path),
        },
    )
    return PrecomputeShardResult(run_id, "build-features", chunk_index, shard_dir, status_path, len(pdb_ids), len(pdb_ids), 0, 0)


def _run_training_example_shard(layout: StorageLayout, *, run_id: str, chunk_index: int) -> PrecomputeShardResult:
    from pbdata.training.generator import build_training_examples

    chunk_manifest = _load_chunk_manifest(layout, run_id, chunk_index)
    pdb_ids = _chunk_pdb_ids(chunk_manifest)
    shard_dir = _shards_dir(layout, run_id, "build-training-examples") / f"chunk_{chunk_index:05d}"
    temp_root = shard_dir / "workspace"
    extracted_dir = _copy_extracted_subset(layout, pdb_ids, temp_root)
    graph_dir = _copy_graph_subset(layout, pdb_ids, temp_root, use_full_graph=True)
    features_dir = _copy_feature_subset(layout, pdb_ids, temp_root)
    output_dir = shard_dir / "training_examples"
    examples_path, manifest_path = build_training_examples(extracted_dir, features_dir, graph_dir, output_dir)
    status_path = _write_chunk_status(
        layout,
        run_id,
        chunk_index,
        {
            "run_id": run_id,
            "stage": "build-training-examples",
            "chunk_index": chunk_index,
            "generated_at": _utc_now(),
            "status": "completed",
            "input_count": len(pdb_ids),
            "processed": len(pdb_ids),
            "ok": len(pdb_ids),
            "cached": 0,
            "failed": 0,
            "shard_dir": str(shard_dir),
            "output_dir": str(output_dir),
            "examples_path": str(examples_path),
            "manifest_path": str(manifest_path),
        },
    )
    return PrecomputeShardResult(run_id, "build-training-examples", chunk_index, shard_dir, status_path, len(pdb_ids), len(pdb_ids), 0, 0)


def run_precompute_shard(
    layout: StorageLayout,
    *,
    run_id: str,
    chunk_index: int,
    config: AppConfig,
    workers: int = 1,
    force: bool = False,
    download_structures: bool = True,
    download_pdb: bool = False,
    graph_level: str = "residue",
    scope: str = "whole_protein",
    shell_radius: float = 8.0,
    export_formats: tuple[str, ...] = ("pyg", "networkx"),
    log_fn: Callable[[str], None] | None = None,
) -> PrecomputeShardResult:
    manifest = _read_json(_run_manifest_path(layout, run_id))
    stage = str(manifest.get("stage") or "")
    if stage == "extract":
        return run_extract_shard(
            layout,
            run_id=run_id,
            chunk_index=chunk_index,
            config=config,
            workers=workers,
            force=force,
            download_structures=download_structures,
            download_pdb=download_pdb,
            log_fn=log_fn,
        )
    if stage == "build-structural-graphs":
        return _run_structural_graphs_shard(
            layout,
            run_id=run_id,
            chunk_index=chunk_index,
            graph_level=graph_level,
            scope=scope,
            shell_radius=shell_radius,
            export_formats=export_formats,
        )
    if stage == "build-graph":
        return _run_graph_shard(layout, run_id=run_id, chunk_index=chunk_index)
    if stage == "build-features":
        return _run_feature_shard(layout, run_id=run_id, chunk_index=chunk_index)
    if stage == "build-training-examples":
        return _run_training_example_shard(layout, run_id=run_id, chunk_index=chunk_index)
    raise ValueError(f"Unsupported precompute run stage '{stage}'.")


def _merge_extract_shards(layout: StorageLayout, *, run_id: str) -> PrecomputeMergeResult:
    shard_root = _shards_dir(layout, run_id, "extract")
    if not shard_root.exists():
        raise FileNotFoundError(f"No extract shards found in {shard_root}.")
    merged_dir = _merged_dir(layout, run_id, "extract")
    merged_dir.mkdir(parents=True, exist_ok=True)
    copied = 0
    for shard_dir in sorted(shard_root.glob("chunk_*")):
        extracted_dir = shard_dir / "extracted"
        if not extracted_dir.exists():
            continue
        for table_dir in extracted_dir.iterdir():
            if not table_dir.is_dir():
                continue
            target_dir = layout.extracted_dir / table_dir.name
            target_dir.mkdir(parents=True, exist_ok=True)
            merged_table_dir = merged_dir / table_dir.name
            merged_table_dir.mkdir(parents=True, exist_ok=True)
            for path in table_dir.glob("*.json"):
                shutil.copy2(path, target_dir / path.name)
                shutil.copy2(path, merged_table_dir / path.name)
                copied += 1
    manifest_path = merged_dir / "merge_manifest.json"
    _write_json(
        manifest_path,
        {
            "run_id": run_id,
            "stage": "extract",
            "generated_at": _utc_now(),
            "copied_files": copied,
            "merged_output_dir": str(merged_dir),
            "workspace_output_dir": str(layout.extracted_dir),
        },
    )
    return PrecomputeMergeResult(run_id=run_id, stage="extract", merged_dir=merged_dir, manifest_path=manifest_path, copied=copied)


def _merge_json_list_records(
    shard_paths: list[Path],
    target_path: Path,
    merged_path: Path,
    *,
    key_field: str,
) -> int:
    seen: dict[str, dict[str, Any]] = {}
    for shard_path in shard_paths:
        if not shard_path.exists():
            continue
        rows = json.loads(shard_path.read_text(encoding="utf-8"))
        for row in rows:
            if not isinstance(row, dict):
                continue
            key = str(row.get(key_field) or "")
            if key and key not in seen:
                seen[key] = row
    payload = list(seen.values())
    target_path.parent.mkdir(parents=True, exist_ok=True)
    merged_path.parent.mkdir(parents=True, exist_ok=True)
    encoded = json.dumps(payload, indent=2)
    target_path.write_text(encoded, encoding="utf-8")
    merged_path.write_text(encoded, encoding="utf-8")
    return len(payload)


def _merge_structural_graph_shards(layout: StorageLayout, *, run_id: str) -> PrecomputeMergeResult:
    shard_root = _shards_dir(layout, run_id, "build-structural-graphs")
    if not shard_root.exists():
        raise FileNotFoundError(f"No structural-graph shards found in {shard_root}.")
    merged_dir = _merged_dir(layout, run_id, "build-structural-graphs")
    merged_dir.mkdir(parents=True, exist_ok=True)
    copied = 0
    for shard_dir in sorted(shard_root.glob("chunk_*")):
        for graph_config_dir in (shard_dir / "structural_graphs").glob("*"):
            if not graph_config_dir.is_dir():
                continue
            target_dir = layout.workspace_graphs_dir / graph_config_dir.name
            target_dir.mkdir(parents=True, exist_ok=True)
            merged_target_dir = merged_dir / graph_config_dir.name
            merged_target_dir.mkdir(parents=True, exist_ok=True)
            for path in graph_config_dir.glob("*"):
                if path.is_file():
                    shutil.copy2(path, target_dir / path.name)
                    shutil.copy2(path, merged_target_dir / path.name)
                    copied += 1
    manifest_path = merged_dir / "merge_manifest.json"
    _write_json(manifest_path, {"run_id": run_id, "stage": "build-structural-graphs", "generated_at": _utc_now(), "copied_files": copied, "merged_output_dir": str(merged_dir), "workspace_output_dir": str(layout.workspace_graphs_dir)})
    return PrecomputeMergeResult(run_id=run_id, stage="build-structural-graphs", merged_dir=merged_dir, manifest_path=manifest_path, copied=copied)


def _merge_graph_shards(layout: StorageLayout, *, run_id: str) -> PrecomputeMergeResult:
    shard_root = _shards_dir(layout, run_id, "build-graph")
    if not shard_root.exists():
        raise FileNotFoundError(f"No graph shards found in {shard_root}.")
    merged_dir = _merged_dir(layout, run_id, "build-graph")
    merged_dir.mkdir(parents=True, exist_ok=True)
    shard_node_paths = [path for path in shard_root.glob("chunk_*/graph/graph_nodes.json")]
    shard_edge_paths = [path for path in shard_root.glob("chunk_*/graph/graph_edges.json")]
    node_count = _merge_json_list_records(shard_node_paths, layout.graph_dir / "graph_nodes.json", merged_dir / "graph_nodes.json", key_field="node_id")
    edge_count = _merge_json_list_records(shard_edge_paths, layout.graph_dir / "graph_edges.json", merged_dir / "graph_edges.json", key_field="edge_id")
    manifest_path = merged_dir / "merge_manifest.json"
    _write_json(manifest_path, {"run_id": run_id, "stage": "build-graph", "generated_at": _utc_now(), "node_count": node_count, "edge_count": edge_count, "merged_output_dir": str(merged_dir), "workspace_output_dir": str(layout.graph_dir)})
    return PrecomputeMergeResult(run_id=run_id, stage="build-graph", merged_dir=merged_dir, manifest_path=manifest_path, copied=node_count + edge_count)


def _merge_feature_shards(layout: StorageLayout, *, run_id: str) -> PrecomputeMergeResult:
    shard_root = _shards_dir(layout, run_id, "build-features")
    if not shard_root.exists():
        raise FileNotFoundError(f"No feature shards found in {shard_root}.")
    merged_dir = _merged_dir(layout, run_id, "build-features")
    merged_dir.mkdir(parents=True, exist_ok=True)
    record_count = _merge_json_list_records(
        [path for path in shard_root.glob("chunk_*/features/feature_records.json")],
        layout.features_dir / "feature_records.json",
        merged_dir / "feature_records.json",
        key_field="pair_identity_key",
    )
    manifest_path = merged_dir / "merge_manifest.json"
    _write_json(manifest_path, {"run_id": run_id, "stage": "build-features", "generated_at": _utc_now(), "record_count": record_count, "merged_output_dir": str(merged_dir), "workspace_output_dir": str(layout.features_dir)})
    return PrecomputeMergeResult(run_id=run_id, stage="build-features", merged_dir=merged_dir, manifest_path=manifest_path, copied=record_count)


def _merge_training_example_shards(layout: StorageLayout, *, run_id: str) -> PrecomputeMergeResult:
    shard_root = _shards_dir(layout, run_id, "build-training-examples")
    if not shard_root.exists():
        raise FileNotFoundError(f"No training-example shards found in {shard_root}.")
    merged_dir = _merged_dir(layout, run_id, "build-training-examples")
    merged_dir.mkdir(parents=True, exist_ok=True)
    record_count = _merge_json_list_records(
        [path for path in shard_root.glob("chunk_*/training_examples/training_examples.json")],
        layout.training_dir / "training_examples.json",
        merged_dir / "training_examples.json",
        key_field="example_id",
    )
    manifest_path = merged_dir / "merge_manifest.json"
    _write_json(manifest_path, {"run_id": run_id, "stage": "build-training-examples", "generated_at": _utc_now(), "record_count": record_count, "merged_output_dir": str(merged_dir), "workspace_output_dir": str(layout.training_dir)})
    return PrecomputeMergeResult(run_id=run_id, stage="build-training-examples", merged_dir=merged_dir, manifest_path=manifest_path, copied=record_count)


def merge_precompute_shards(layout: StorageLayout, *, run_id: str) -> PrecomputeMergeResult:
    manifest = _read_json(_run_manifest_path(layout, run_id))
    stage = str(manifest.get("stage") or "")
    if stage == "extract":
        return _merge_extract_shards(layout, run_id=run_id)
    if stage == "build-structural-graphs":
        return _merge_structural_graph_shards(layout, run_id=run_id)
    if stage == "build-graph":
        return _merge_graph_shards(layout, run_id=run_id)
    if stage == "build-features":
        return _merge_feature_shards(layout, run_id=run_id)
    if stage == "build-training-examples":
        return _merge_training_example_shards(layout, run_id=run_id)
    raise ValueError(f"Unsupported precompute run stage '{stage}'.")


def build_precompute_run_status(layout: StorageLayout, *, run_id: str) -> dict[str, Any]:
    run_manifest = _read_json(_run_manifest_path(layout, run_id))
    status_dir = _status_dir(layout, run_id)
    chunk_statuses = sorted(status_dir.glob("chunk_*.status.json"))
    statuses = [_read_json(path) for path in chunk_statuses]
    completed = sum(1 for item in statuses if str(item.get("status", "")).startswith("completed"))
    failed_chunks = sum(1 for item in statuses if item.get("failed", 0))
    processed = sum(int(item.get("processed", 0)) for item in statuses)
    ok = sum(int(item.get("ok", 0)) for item in statuses)
    cached = sum(int(item.get("cached", 0)) for item in statuses)
    failed = sum(int(item.get("failed", 0)) for item in statuses)
    return {
        "run_id": run_id,
        "stage": run_manifest.get("stage"),
        "status": "completed" if completed == len(run_manifest.get("chunks") or []) and failed_chunks == 0 else ("completed_with_failures" if completed == len(run_manifest.get("chunks") or []) else "in_progress"),
        "storage_root": str(layout.root),
        "chunk_count": len(run_manifest.get("chunks") or []),
        "completed_chunks": completed,
        "failed_chunks": failed_chunks,
        "total_inputs": int(run_manifest.get("total_inputs", 0)),
        "processed": processed,
        "ok": ok,
        "cached": cached,
        "failed": failed,
        "run_dir": str(_run_root(layout, run_id)),
    }
