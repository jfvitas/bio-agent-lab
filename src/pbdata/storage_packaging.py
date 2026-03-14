"""Helpers for packaging large JSON corpora into transport- and HPC-friendly shards."""

from __future__ import annotations

import gzip
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from pathlib import PureWindowsPath
from typing import Any

from pbdata.storage import StorageLayout


_EXTRACTED_TABLES = (
    "entry",
    "chains",
    "bound_objects",
    "interfaces",
    "assays",
    "provenance",
)


@dataclass(frozen=True)
class RawPackagingResult:
    package_id: str
    package_dir: Path
    manifest_path: Path
    shard_count: int
    total_records: int
    unreadable_records: int


@dataclass(frozen=True)
class RawUnpackResult:
    package_id: str
    output_dir: Path
    restored_records: int


@dataclass(frozen=True)
class ExtractedConsolidationResult:
    run_id: str
    output_dir: Path
    manifest_path: Path
    table_count: int
    total_records: int
    unreadable_records: int


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _load_manifest(package: Path) -> tuple[dict[str, Any], Path]:
    if package.is_file():
        manifest_path = package
    else:
        manifest_path = package / "manifest.json"
    if not manifest_path.exists():
        raise FileNotFoundError(f"Package manifest not found: {manifest_path}")
    return json.loads(manifest_path.read_text(encoding="utf-8")), manifest_path.parent


def load_packaged_manifest(package: Path) -> tuple[dict[str, Any], Path]:
    """Load a package manifest from a manifest path or package directory."""
    return _load_manifest(package)


def _resolve_manifest_file_ref(package_dir: Path, ref: str, *relative_parts: str) -> Path:
    candidate = Path(ref)
    if candidate.exists():
        return candidate
    # Handle cross-platform manifests copied from Windows to POSIX or vice versa.
    try:
        windows_name = PureWindowsPath(ref).name
    except Exception:
        windows_name = Path(ref).name
    fallback = package_dir.joinpath(*relative_parts, windows_name or Path(ref).name)
    if fallback.exists():
        return fallback
    if not candidate.is_absolute():
        candidate = package_dir.joinpath(*relative_parts, candidate.name)
        if candidate.exists():
            return candidate
    return fallback


def resolve_packaged_file_ref(package: Path, ref: str, *relative_parts: str) -> Path:
    """Resolve a manifest-stored file reference against a package location."""
    _manifest, package_dir = _load_manifest(package)
    return _resolve_manifest_file_ref(package_dir, ref, *relative_parts)


def package_raw_rcsb_records(
    layout: StorageLayout,
    *,
    shard_size: int = 5000,
    package_id: str | None = None,
) -> RawPackagingResult:
    if shard_size <= 0:
        raise ValueError("shard_size must be positive.")

    raw_files = sorted(layout.raw_rcsb_dir.glob("*.json"))
    if not raw_files:
        raise FileNotFoundError(f"No raw RCSB records found in {layout.raw_rcsb_dir}.")

    package_id = package_id or f"raw_rcsb_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    package_dir = layout.raw_rcsb_packages_dir / package_id
    manifest_path = package_dir / "manifest.json"
    if manifest_path.exists():
        raise FileExistsError(f"Raw package '{package_id}' already exists at {package_dir}.")

    shards_dir = package_dir / "shards"
    shards_dir.mkdir(parents=True, exist_ok=True)

    shard_records: list[dict[str, Any]] = []
    unreadable_records = 0
    readable_items: list[tuple[str, dict[str, Any]]] = []
    for path in raw_files:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            unreadable_records += 1
            continue
        readable_items.append((path.stem.upper(), payload))

    for shard_index in range(0, len(readable_items), shard_size):
        shard_items = readable_items[shard_index: shard_index + shard_size]
        shard_id = shard_index // shard_size
        shard_path = shards_dir / f"raw_rcsb_shard_{shard_id:05d}.jsonl.gz"
        with gzip.open(shard_path, "wt", encoding="utf-8") as handle:
            for pdb_id, payload in shard_items:
                handle.write(json.dumps({"pdb_id": pdb_id, "raw": payload}, separators=(",", ":")))
                handle.write("\n")
        shard_records.append(
            {
                "shard_index": shard_id,
                "path": str(shard_path),
                "record_count": len(shard_items),
                "first_pdb_id": shard_items[0][0] if shard_items else None,
                "last_pdb_id": shard_items[-1][0] if shard_items else None,
            }
        )

    _write_json(
        manifest_path,
        {
            "package_id": package_id,
            "created_at": _utc_now(),
            "storage_root": str(layout.root),
            "source_dir": str(layout.raw_rcsb_dir),
            "format": "jsonl.gz",
            "record_type": "raw_rcsb",
            "total_records": len(readable_items),
            "unreadable_records": unreadable_records,
            "shard_size": shard_size,
            "shard_count": len(shard_records),
            "shards": shard_records,
        },
    )
    return RawPackagingResult(
        package_id=package_id,
        package_dir=package_dir,
        manifest_path=manifest_path,
        shard_count=len(shard_records),
        total_records=len(readable_items),
        unreadable_records=unreadable_records,
    )


def unpack_raw_rcsb_package(
    layout: StorageLayout,
    *,
    package: Path,
    output_dir: Path | None = None,
    overwrite: bool = False,
) -> RawUnpackResult:
    manifest, package_dir = _load_manifest(package)
    if str(manifest.get("record_type") or "") != "raw_rcsb":
        raise ValueError(f"Package at {package_dir} is not a raw_rcsb package.")

    target_dir = output_dir or layout.raw_rcsb_dir
    target_dir.mkdir(parents=True, exist_ok=True)
    restored = 0
    for shard in manifest.get("shards") or []:
        shard_path = _resolve_manifest_file_ref(package_dir, str(shard.get("path") or ""), "shards")
        if not shard_path.exists():
            raise FileNotFoundError(f"Shard not found: {shard_path}")
        with gzip.open(shard_path, "rt", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                row = json.loads(line)
                pdb_id = str(row.get("pdb_id") or "").upper()
                payload = row.get("raw")
                if not pdb_id:
                    continue
                output_path = target_dir / f"{pdb_id}.json"
                if output_path.exists() and not overwrite:
                    continue
                output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
                restored += 1
    return RawUnpackResult(
        package_id=str(manifest.get("package_id") or package_dir.name),
        output_dir=target_dir,
        restored_records=restored,
    )


def iter_raw_rcsb_package_rows(package: Path) -> tuple[str, list[dict[str, Any]]]:
    """Yield raw packaged shard paths and their decoded rows."""
    manifest, package_dir = _load_manifest(package)
    if str(manifest.get("record_type") or "") != "raw_rcsb":
        raise ValueError(f"Package at {package_dir} is not a raw_rcsb package.")
    for shard in manifest.get("shards") or []:
        shard_path = _resolve_manifest_file_ref(package_dir, str(shard.get("path") or ""), "shards")
        if not shard_path.exists():
            raise FileNotFoundError(f"Shard not found: {shard_path}")
        rows: list[dict[str, Any]] = []
        with gzip.open(shard_path, "rt", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                rows.append(json.loads(line))
        yield str(shard_path), rows


def iter_extracted_bundle_shards(package: Path) -> tuple[dict[str, Any], dict[str, list[dict[str, Any]]]]:
    """Yield consolidated extracted shard metadata and decoded per-table rows."""
    manifest, package_dir = _load_manifest(package)
    if str(manifest.get("record_type") or "") != "extracted_tables":
        raise ValueError(f"Package at {package_dir} is not an extracted_tables package.")
    for bundle in manifest.get("bundle_shards") or []:
        table_rows: dict[str, list[dict[str, Any]]] = {}
        for table_name, shard_ref in (bundle.get("tables") or {}).items():
            shard_path = _resolve_manifest_file_ref(package_dir, str(shard_ref or ""), str(table_name))
            if not shard_path.exists():
                raise FileNotFoundError(f"Consolidated extracted shard not found: {shard_path}")
            rows: list[dict[str, Any]] = []
            with gzip.open(shard_path, "rt", encoding="utf-8") as handle:
                for line in handle:
                    line = line.strip()
                    if not line:
                        continue
                    rows.append(json.loads(line))
            table_rows[str(table_name)] = rows
        yield bundle, table_rows


def consolidate_extracted_tables(
    layout: StorageLayout,
    *,
    shard_size: int = 5000,
    run_id: str | None = None,
) -> ExtractedConsolidationResult:
    if shard_size <= 0:
        raise ValueError("shard_size must be positive.")

    run_id = run_id or f"extracted_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    output_dir = layout.extracted_consolidated_dir / run_id
    manifest_path = output_dir / "manifest.json"
    if manifest_path.exists():
        raise FileExistsError(f"Extracted consolidation run '{run_id}' already exists at {output_dir}.")

    output_dir.mkdir(parents=True, exist_ok=True)
    unreadable_records = 0
    total_records = 0
    table_manifests: list[dict[str, Any]] = []
    table_shard_maps: dict[str, dict[int, dict[str, Any]]] = {}

    for table_name in _EXTRACTED_TABLES:
        table_dir = layout.extracted_dir / table_name
        table_files = sorted(table_dir.glob("*.json")) if table_dir.exists() else []
        readable_rows: list[tuple[str, Any]] = []
        for path in table_files:
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                unreadable_records += 1
                continue
            readable_rows.append((path.stem.upper(), payload))

        table_output_dir = output_dir / table_name
        table_output_dir.mkdir(parents=True, exist_ok=True)
        shard_records: list[dict[str, Any]] = []
        shard_map: dict[int, dict[str, Any]] = {}
        for shard_index in range(0, len(readable_rows), shard_size):
            shard_items = readable_rows[shard_index: shard_index + shard_size]
            shard_id = shard_index // shard_size
            shard_path = table_output_dir / f"{table_name}_shard_{shard_id:05d}.jsonl.gz"
            with gzip.open(shard_path, "wt", encoding="utf-8") as handle:
                for pdb_id, payload in shard_items:
                    handle.write(
                        json.dumps(
                            {"pdb_id": pdb_id, "table": table_name, "payload": payload},
                            separators=(",", ":"),
                        )
                    )
                    handle.write("\n")
            shard_records.append(
                {
                    "shard_index": shard_id,
                    "path": str(shard_path),
                    "record_count": len(shard_items),
                    "first_pdb_id": shard_items[0][0] if shard_items else None,
                    "last_pdb_id": shard_items[-1][0] if shard_items else None,
                }
            )
            shard_map[shard_id] = shard_records[-1]

        total_records += len(readable_rows)
        table_shard_maps[table_name] = shard_map
        table_manifests.append(
            {
                "table": table_name,
                "input_dir": str(table_dir),
                "record_count": len(readable_rows),
                "shard_count": len(shard_records),
                "shards": shard_records,
            }
        )

    bundle_shards: list[dict[str, Any]] = []
    entry_shards = table_shard_maps.get("entry", {})
    for shard_index, entry_info in sorted(entry_shards.items()):
        bundle_tables = {
            table_name: table_shard_maps[table_name][shard_index]["path"]
            for table_name in _EXTRACTED_TABLES
            if shard_index in table_shard_maps.get(table_name, {})
        }
        bundle_shards.append(
            {
                "shard_index": shard_index,
                "record_count": entry_info["record_count"],
                "first_pdb_id": entry_info["first_pdb_id"],
                "last_pdb_id": entry_info["last_pdb_id"],
                "tables": bundle_tables,
            }
        )

    _write_json(
        manifest_path,
        {
            "run_id": run_id,
            "created_at": _utc_now(),
            "storage_root": str(layout.root),
            "source_dir": str(layout.extracted_dir),
            "format": "jsonl.gz",
            "record_type": "extracted_tables",
            "shard_size": shard_size,
            "table_count": len(table_manifests),
            "total_records": total_records,
            "unreadable_records": unreadable_records,
            "bundle_shards": bundle_shards,
            "tables": table_manifests,
        },
    )
    return ExtractedConsolidationResult(
        run_id=run_id,
        output_dir=output_dir,
        manifest_path=manifest_path,
        table_count=len(table_manifests),
        total_records=total_records,
        unreadable_records=unreadable_records,
    )
