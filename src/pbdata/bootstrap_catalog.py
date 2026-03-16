"""Build lightweight, sharded startup catalogs for fast local workspace bootstraps.

Assumptions:
- The bootstrap catalog is intentionally smaller and more stable than the full
  raw/extracted corpus; it is meant to answer "what do we know about this PDB?"
  quickly and drive dataset planning.
- This package is built from already-normalized local artifacts and review
  exports. It does not replace the fuller raw/extracted stores.
- Update policy is staged: ship the bootstrap package, construct training sets
  from it, then optionally refresh the selected PDB IDs against upstream
  sources.
"""

from __future__ import annotations

import csv
import gzip
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pbdata.storage import StorageLayout
from pbdata.table_io import load_table_json


@dataclass(frozen=True)
class BootstrapCatalogBuildResult:
    package_id: str
    package_dir: Path
    manifest_path: Path
    shard_count: int
    record_count: int


def build_bootstrap_catalog(
    layout: StorageLayout,
    *,
    shard_size: int = 5000,
    package_id: str | None = None,
) -> BootstrapCatalogBuildResult:
    """Materialize a sharded per-PDB startup catalog for fast local planning."""
    if shard_size <= 0:
        raise ValueError("shard_size must be positive.")

    records = _build_bootstrap_records(layout)
    if not records:
        raise FileNotFoundError(
            "No bootstrap records available. Build extracted/master exports first."
        )

    package_id = package_id or f"bootstrap_catalog_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    package_dir = layout.bootstrap_catalog_packages_dir / package_id
    manifest_path = package_dir / "manifest.json"
    if manifest_path.exists():
        raise FileExistsError(f"Bootstrap catalog '{package_id}' already exists at {package_dir}.")

    shards_dir = package_dir / "shards"
    shards_dir.mkdir(parents=True, exist_ok=True)

    shard_manifest: list[dict[str, Any]] = []
    for shard_index in range(0, len(records), shard_size):
        shard_records = records[shard_index: shard_index + shard_size]
        shard_id = shard_index // shard_size
        shard_path = shards_dir / f"bootstrap_catalog_{shard_id:05d}.jsonl.gz"
        with gzip.open(shard_path, "wt", encoding="utf-8") as handle:
            for record in shard_records:
                handle.write(json.dumps(record, separators=(",", ":")))
                handle.write("\n")

        shard_manifest.append(
            {
                "shard_index": shard_id,
                "path": str(shard_path),
                "record_count": len(shard_records),
                "first_pdb_id": shard_records[0]["pdb_id"],
                "last_pdb_id": shard_records[-1]["pdb_id"],
            }
        )

    summary = _summarize_records(records)
    manifest = {
        "package_id": package_id,
        "created_at": _utc_now(),
        "storage_root": str(layout.root),
        "record_type": "bootstrap_catalog",
        "format": "jsonl.gz",
        "record_count": len(records),
        "shard_count": len(shard_manifest),
        "shard_size": shard_size,
        "summary": summary,
        "update_policy": {
            "mode": "staged_bootstrap_then_targeted_refresh",
            "bootstrap_use": "startup planning, source coverage, training-set design",
            "refresh_trigger": "optionally refresh only the selected training-set PDB IDs before final training or release",
        },
        "source_inputs": {
            "master_repository_csv": str(layout.root / "master_pdb_repository.csv"),
            "master_pairs_csv": str(layout.root / "master_pdb_pairs.csv"),
            "entry_table_dir": str(layout.extracted_dir / "entry"),
            "assays_table_dir": str(layout.extracted_dir / "assays"),
            "chains_table_dir": str(layout.extracted_dir / "chains"),
        },
        "shards": shard_manifest,
    }
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return BootstrapCatalogBuildResult(
        package_id=package_id,
        package_dir=package_dir,
        manifest_path=manifest_path,
        shard_count=len(shard_manifest),
        record_count=len(records),
    )


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _split_values(raw: Any) -> list[str]:
    text = str(raw or "")
    values = [part.strip() for part in text.replace(";", ",").split(",") if part.strip()]
    return list(dict.fromkeys(values))


def _bool_has_file(path: Path) -> bool:
    return path.exists() and path.stat().st_size > 0


def _directory_pdb_ids(path: Path) -> set[str]:
    if not path.exists():
        return set()
    return {
        candidate.stem.upper()
        for candidate in path.glob("*.json")
        if candidate.is_file()
    }


def _load_json(path: Path) -> Any:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _load_entry_fallback(layout: StorageLayout, pdb_id: str) -> dict[str, Any]:
    payload = _load_json(layout.extracted_dir / "entry" / f"{pdb_id}.json")
    if isinstance(payload, dict):
        return payload
    return {}


def _load_table_fallback(table_dir: Path, pdb_id: str) -> list[dict[str, Any]]:
    payload = _load_json(table_dir / f"{pdb_id}.json")
    if isinstance(payload, dict):
        return [payload]
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]
    return []


def _build_bootstrap_records(layout: StorageLayout) -> list[dict[str, Any]]:
    repository_rows = _read_csv_rows(layout.root / "master_pdb_repository.csv")
    pair_rows = _read_csv_rows(layout.root / "master_pdb_pairs.csv")
    use_repository_fallback = not repository_rows
    use_pair_fallback = not pair_rows

    entry_rows = load_table_json(layout.extracted_dir / "entry") if not repository_rows else []
    assay_rows = load_table_json(layout.extracted_dir / "assays") if not pair_rows else []
    chain_rows = load_table_json(layout.extracted_dir / "chains") if not pair_rows else []
    interface_rows = load_table_json(layout.extracted_dir / "interfaces") if not pair_rows else []
    bound_rows = load_table_json(layout.extracted_dir / "bound_objects") if not pair_rows else []

    repository_by_pdb = {
        str(row.get("pdb_id") or "").upper(): row
        for row in repository_rows
        if str(row.get("pdb_id") or "").strip()
    }
    if not repository_by_pdb:
        repository_by_pdb = {
            str(row.get("pdb_id") or "").upper(): row
            for row in entry_rows
            if str(row.get("pdb_id") or "").strip()
        }

    pairs_by_pdb: dict[str, list[dict[str, Any]]] = {}
    for row in pair_rows:
        pdb_id = str(row.get("pdb_id") or "").upper()
        if pdb_id:
            pairs_by_pdb.setdefault(pdb_id, []).append(row)
    if not pairs_by_pdb:
        for row in assay_rows:
            pdb_id = str(row.get("pdb_id") or "").upper()
            if pdb_id:
                pairs_by_pdb.setdefault(pdb_id, []).append(row)

    chains_by_pdb: dict[str, list[dict[str, Any]]] = {}
    for row in chain_rows:
        pdb_id = str(row.get("pdb_id") or "").upper()
        if pdb_id:
            chains_by_pdb.setdefault(pdb_id, []).append(row)

    interfaces_by_pdb: dict[str, list[dict[str, Any]]] = {}
    for row in interface_rows:
        pdb_id = str(row.get("pdb_id") or "").upper()
        if pdb_id:
            interfaces_by_pdb.setdefault(pdb_id, []).append(row)

    bound_objects_by_pdb: dict[str, list[dict[str, Any]]] = {}
    for row in bound_rows:
        pdb_id = str(row.get("pdb_id") or "").upper()
        if pdb_id:
            bound_objects_by_pdb.setdefault(pdb_id, []).append(row)

    pdb_ids = sorted(
        set(repository_by_pdb)
        | set(pairs_by_pdb)
        | set(chains_by_pdb)
        | set(interfaces_by_pdb)
        | set(bound_objects_by_pdb)
        | _directory_pdb_ids(layout.raw_rcsb_dir)
        | _directory_pdb_ids(layout.processed_rcsb_dir)
        | _directory_pdb_ids(layout.extracted_dir / "entry")
    )

    records: list[dict[str, Any]] = []
    for pdb_id in pdb_ids:
        repo_row = repository_by_pdb.get(pdb_id, {})
        if not repo_row and use_repository_fallback:
            repo_row = _load_entry_fallback(layout, pdb_id)

        pair_group = pairs_by_pdb.get(pdb_id, [])
        chain_group = chains_by_pdb.get(pdb_id, [])
        interface_group = interfaces_by_pdb.get(pdb_id, [])
        bound_group = bound_objects_by_pdb.get(pdb_id, [])
        if use_pair_fallback:
            if not pair_group:
                pair_group = _load_table_fallback(layout.extracted_dir / "assays", pdb_id)
            if not chain_group:
                chain_group = _load_table_fallback(layout.extracted_dir / "chains", pdb_id)
            if not interface_group:
                interface_group = _load_table_fallback(layout.extracted_dir / "interfaces", pdb_id)
            if not bound_group:
                bound_group = _load_table_fallback(layout.extracted_dir / "bound_objects", pdb_id)
        processed_path = layout.processed_rcsb_dir / f"{pdb_id}.json"
        raw_path = layout.raw_rcsb_dir / f"{pdb_id}.json"
        cif_path = layout.structures_rcsb_dir / f"{pdb_id}.cif"
        pdb_path = layout.structures_rcsb_dir / f"{pdb_id}.pdb"

        receptor_chain_ids = sorted({
            value
            for row in pair_group
            for value in _split_values(row.get("receptor_chain_ids"))
        })

        source_databases = sorted({
            value
            for row in pair_group
            for value in _split_values(row.get("source_database"))
        })
        receptor_uniprot_ids = sorted({
            value
            for row in pair_group
            for value in _split_values(row.get("receptor_uniprot_ids") or row.get("uniprot_id"))
        } | {
            str(row.get("uniprot_id") or "").strip()
            for row in chain_group
            if str(row.get("uniprot_id") or "").strip()
        })
        ligand_types = sorted({
            value
            for row in pair_group
            for value in _split_values(row.get("ligand_types"))
        } | {
            str(row.get("ligand_type") or "").strip()
            for row in bound_group
            if str(row.get("ligand_type") or "").strip()
        })
        interface_types = sorted({
            value
            for row in pair_group
            for value in _split_values(row.get("matching_interface_types"))
        } | {
            str(row.get("interface_type") or "").strip()
            for row in interface_group
            if str(row.get("interface_type") or "").strip()
        })
        affinity_types = sorted({
            value
            for row in pair_group
            for value in _split_values(row.get("binding_affinity_type"))
        })

        records.append(
            {
                "pdb_id": pdb_id,
                "experimental_method": str(
                    repo_row.get("experimental_method")
                    or repo_row.get("method")
                    or repo_row.get("title")
                    or ""
                ),
                "resolution": str(
                    repo_row.get("structure_resolution")
                    or repo_row.get("resolution")
                    or ""
                ),
                "organisms": sorted({
                    value
                    for row in pair_group
                    for value in _split_values(row.get("receptor_organisms"))
                } | {
                    value
                    for value in _split_values(repo_row.get("organism_names") or repo_row.get("organism"))
                }),
                "source_databases": source_databases,
                "receptor_uniprot_ids": receptor_uniprot_ids,
                "ligand_types": ligand_types,
                "interface_types": interface_types,
                "affinity_types": affinity_types,
                "pair_count": len(pair_group),
                "assay_count": len(pair_group),
                "chain_count": len(chain_group) if chain_group else len(receptor_chain_ids),
                "interface_count": len(interface_group),
                "bound_object_count": len(bound_group),
                "mutation_pair_count": sum(
                    1
                    for row in pair_group
                    if str(row.get("mutation_strings") or row.get("mutation_string") or "").strip()
                ),
                "has_raw_payload": _bool_has_file(raw_path),
                "has_processed_record": _bool_has_file(processed_path),
                "has_structure_file": _bool_has_file(cif_path) or _bool_has_file(pdb_path),
                "bootstrap_ready": bool(pair_group or chain_group or interface_group or bound_group or repo_row),
            }
        )

    return records


def _summarize_records(records: list[dict[str, Any]]) -> dict[str, Any]:
    unique_sources = sorted({
        source
        for record in records
        for source in record.get("source_databases", [])
    })
    return {
        "record_count": len(records),
        "source_database_count": len(unique_sources),
        "source_databases": unique_sources,
        "records_with_processed_payload": sum(1 for record in records if record["has_processed_record"]),
        "records_with_structure_file": sum(1 for record in records if record["has_structure_file"]),
        "records_with_assays": sum(1 for record in records if record["assay_count"] > 0),
    }
