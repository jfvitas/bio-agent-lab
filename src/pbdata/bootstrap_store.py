"""Persistent local bootstrap store and targeted refresh planning helpers."""

from __future__ import annotations

import csv
import gzip
import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pbdata.bootstrap_catalog import _build_bootstrap_records
from pbdata.config import AppConfig
from pbdata.master_export import refresh_master_exports
from pbdata.parsing.mmcif_supplement import download_structure_files, fetch_mmcif_supplement
from pbdata.pipeline.enrichment import (
    fetch_bindingdb_samples_for_pdb,
    fetch_chembl_samples_for_raw,
    load_external_assay_samples,
)
from pbdata.pipeline.extract import extract_rcsb_entry, write_records_json
from pbdata.storage import StorageLayout
from pbdata.sources.rcsb import RCSBAdapter
from pbdata.sources.rcsb_search import fetch_chemcomp_descriptors, fetch_entries_batch


@dataclass(frozen=True)
class BootstrapStoreResult:
    store_dir: Path
    database_path: Path
    manifest_path: Path
    record_count: int


@dataclass(frozen=True)
class BootstrapRefreshPlanResult:
    manifest_path: Path
    record_count: int
    selected_source: str


@dataclass(frozen=True)
class BootstrapRefreshExecutionResult:
    manifest_path: Path
    execution_report_path: Path
    refreshed_count: int
    skipped_count: int
    failed_count: int
    refreshed_pdb_ids: tuple[str, ...]
    failed_pdb_ids: tuple[str, ...]
    export_status: dict[str, Any]


@dataclass(frozen=True)
class BootstrapSummaryExportResult:
    csv_path: Path
    manifest_path: Path
    record_count: int
    pair_csv_path: Path | None = None
    pair_record_count: int = 0


def materialize_bootstrap_store(layout: StorageLayout) -> BootstrapStoreResult:
    """Write a persistent local bootstrap index for fast startup queries."""
    records = _build_bootstrap_records(layout)
    if not records:
        raise FileNotFoundError(
            "No bootstrap records available. Build extracted/master exports first."
        )

    store_dir = layout.workspace_metadata_dir / "bootstrap_catalog"
    store_dir.mkdir(parents=True, exist_ok=True)
    database_path = store_dir / "bootstrap_catalog.sqlite"
    manifest_path = store_dir / "bootstrap_store_manifest.json"

    with sqlite3.connect(database_path) as connection:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS bootstrap_catalog (
                pdb_id TEXT PRIMARY KEY,
                experimental_method TEXT,
                resolution TEXT,
                organisms_json TEXT NOT NULL,
                source_databases_json TEXT NOT NULL,
                receptor_uniprot_ids_json TEXT NOT NULL,
                ligand_types_json TEXT NOT NULL,
                interface_types_json TEXT NOT NULL,
                affinity_types_json TEXT NOT NULL,
                pair_count INTEGER NOT NULL,
                assay_count INTEGER NOT NULL,
                chain_count INTEGER NOT NULL,
                interface_count INTEGER NOT NULL,
                bound_object_count INTEGER NOT NULL,
                mutation_pair_count INTEGER NOT NULL,
                has_raw_payload INTEGER NOT NULL,
                has_processed_record INTEGER NOT NULL,
                has_structure_file INTEGER NOT NULL,
                bootstrap_ready INTEGER NOT NULL,
                record_json TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_bootstrap_ready ON bootstrap_catalog(bootstrap_ready)"
        )
        connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_structure_file ON bootstrap_catalog(has_structure_file)"
        )
        connection.executemany(
            """
            INSERT INTO bootstrap_catalog (
                pdb_id,
                experimental_method,
                resolution,
                organisms_json,
                source_databases_json,
                receptor_uniprot_ids_json,
                ligand_types_json,
                interface_types_json,
                affinity_types_json,
                pair_count,
                assay_count,
                chain_count,
                interface_count,
                bound_object_count,
                mutation_pair_count,
                has_raw_payload,
                has_processed_record,
                has_structure_file,
                bootstrap_ready,
                record_json,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(pdb_id) DO UPDATE SET
                experimental_method=excluded.experimental_method,
                resolution=excluded.resolution,
                organisms_json=excluded.organisms_json,
                source_databases_json=excluded.source_databases_json,
                receptor_uniprot_ids_json=excluded.receptor_uniprot_ids_json,
                ligand_types_json=excluded.ligand_types_json,
                interface_types_json=excluded.interface_types_json,
                affinity_types_json=excluded.affinity_types_json,
                pair_count=excluded.pair_count,
                assay_count=excluded.assay_count,
                chain_count=excluded.chain_count,
                interface_count=excluded.interface_count,
                bound_object_count=excluded.bound_object_count,
                mutation_pair_count=excluded.mutation_pair_count,
                has_raw_payload=excluded.has_raw_payload,
                has_processed_record=excluded.has_processed_record,
                has_structure_file=excluded.has_structure_file,
                bootstrap_ready=excluded.bootstrap_ready,
                record_json=excluded.record_json,
                updated_at=excluded.updated_at
            """,
            [_record_row(record) for record in records],
        )
        connection.commit()

    manifest = {
        "generated_at": _utc_now(),
        "storage_root": str(layout.root),
        "database_path": str(database_path),
        "record_count": len(records),
        "source_inputs": {
            "master_repository_csv": str(layout.root / "master_pdb_repository.csv"),
            "master_pairs_csv": str(layout.root / "master_pdb_pairs.csv"),
            "custom_training_set_csv": str(layout.root / "custom_training_set.csv"),
            "model_ready_pairs_csv": str(layout.root / "model_ready_pairs.csv"),
        },
        "refresh_strategy": {
            "initial_population": "populate broad local bootstrap store from all locally available PDB-linked data",
            "follow_up": "refresh only the PDB IDs selected into the active training set or review set",
        },
    }
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return BootstrapStoreResult(
        store_dir=store_dir,
        database_path=database_path,
        manifest_path=manifest_path,
        record_count=len(records),
    )


def export_bootstrap_summary(
    layout: StorageLayout,
    *,
    output_name: str = "bootstrap_summary.csv.gz",
    pair_output_name: str = "bootstrap_pair_summary.csv.gz",
    manifest_name: str = "bootstrap_summary_manifest.json",
) -> BootstrapSummaryExportResult:
    """Export a GitHub-friendly lightweight bootstrap summary table.

    The exported summary is intentionally compact enough to version and share
    without bundling the much larger raw source payloads. It is designed to
    provide the planning fields needed to build candidate pools and training
    sets from a lightweight local package.
    """
    store_dir = layout.workspace_metadata_dir / "bootstrap_catalog"
    store_dir.mkdir(parents=True, exist_ok=True)
    database_path = store_dir / "bootstrap_catalog.sqlite"

    if database_path.exists():
        records = _load_records_from_store(database_path)
        source_mode = "bootstrap_store"
    else:
        records = _build_bootstrap_records(layout)
        source_mode = "local_artifacts"

    if not records:
        raise FileNotFoundError(
            "No bootstrap records available. Materialize the bootstrap store or build extracted/master exports first."
        )

    columns = [
        "pdb_id",
        "experimental_method",
        "resolution",
        "organisms",
        "source_databases",
        "source_count",
        "receptor_uniprot_ids",
        "receptor_uniprot_count",
        "ligand_types",
        "interface_types",
        "affinity_types",
        "pair_count",
        "assay_count",
        "chain_count",
        "interface_count",
        "bound_object_count",
        "mutation_pair_count",
        "has_bindingdb",
        "has_chembl",
        "has_pdbbind",
        "has_biolip",
        "has_skempi",
        "has_raw_payload",
        "has_processed_record",
        "has_structure_file",
        "bootstrap_ready",
    ]

    csv_path = store_dir / output_name
    with gzip.open(csv_path, "wt", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for record in records:
            writer.writerow(_summary_row(record))

    pair_rows = _load_pair_summary_rows(layout)
    pair_csv_path = store_dir / pair_output_name
    pair_columns = [
        "pdb_id",
        "pair_identity_key",
        "source_database",
        "receptor_uniprot_ids",
        "ligand_types",
        "matching_interface_types",
        "binding_affinity_type",
        "mutation_strings",
        "release_split",
        "source_conflict_summary",
        "source_agreement_band",
    ]
    with gzip.open(pair_csv_path, "wt", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=pair_columns)
        writer.writeheader()
        for row in pair_rows:
            writer.writerow(row)

    manifest_path = store_dir / manifest_name
    manifest = {
        "generated_at": _utc_now(),
        "storage_root": str(layout.root),
        "source_mode": source_mode,
        "record_count": len(records),
        "csv_path": str(csv_path),
        "compressed_size_bytes": csv_path.stat().st_size,
        "pair_summary_path": str(pair_csv_path),
        "pair_record_count": len(pair_rows),
        "pair_compressed_size_bytes": pair_csv_path.stat().st_size,
        "github_friendly": True,
        "intended_use": [
            "lightweight planning package",
            "candidate-pool and training-set design",
            "refresh targeting before full training",
        ],
        "columns": columns,
        "notes": (
            "This summary is compact enough to share in a code repository while the larger "
            "raw source payloads remain local-only."
        ),
    }
    if database_path.exists():
        manifest["database_path"] = str(database_path)
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return BootstrapSummaryExportResult(
        csv_path=csv_path,
        manifest_path=manifest_path,
        record_count=len(records),
        pair_csv_path=pair_csv_path,
        pair_record_count=len(pair_rows),
    )


def plan_selected_pdb_refresh(
    layout: StorageLayout,
    *,
    source_csv: Path | None = None,
    plan_name: str = "selected_pdb_refresh_manifest.json",
) -> BootstrapRefreshPlanResult:
    """Build a targeted refresh manifest for selected training-set or model-ready PDB IDs."""
    selected_source = source_csv or _resolve_selected_source(layout)
    rows = _read_csv_rows(selected_source)
    if not rows:
        raise FileNotFoundError(f"No selected rows available in {selected_source}.")

    grouped: dict[str, dict[str, Any]] = {}
    for row in rows:
        pdb_id = str(row.get("pdb_id") or "").strip().upper()
        if not pdb_id:
            continue
        payload = grouped.setdefault(
            pdb_id,
            {
                "pdb_id": pdb_id,
                "pair_identity_keys": [],
                "source_databases": set(),
                "receptor_uniprot_ids": set(),
                "release_splits": set(),
            },
        )
        pair_key = str(row.get("pair_identity_key") or "").strip()
        if pair_key:
            payload["pair_identity_keys"].append(pair_key)
        for source in _split_values(row.get("source_database")):
            payload["source_databases"].add(source)
        for accession in _split_values(row.get("receptor_uniprot_ids")):
            payload["receptor_uniprot_ids"].add(accession)
        for split_name in _split_values(row.get("release_split")):
            payload["release_splits"].add(split_name)

    records = []
    for pdb_id, payload in sorted(grouped.items()):
        raw_path = layout.raw_rcsb_dir / f"{pdb_id}.json"
        processed_path = layout.processed_rcsb_dir / f"{pdb_id}.json"
        cif_path = layout.structures_rcsb_dir / f"{pdb_id}.cif"
        pdb_path = layout.structures_rcsb_dir / f"{pdb_id}.pdb"
        missing_assets = []
        if not raw_path.exists():
            missing_assets.append("raw_rcsb_json")
        if not processed_path.exists():
            missing_assets.append("processed_record")
        if not cif_path.exists() and not pdb_path.exists():
            missing_assets.append("structure_file")
        records.append(
            {
                "pdb_id": pdb_id,
                "pair_identity_keys": payload["pair_identity_keys"],
                "source_databases": sorted(payload["source_databases"]),
                "receptor_uniprot_ids": sorted(payload["receptor_uniprot_ids"]),
                "release_splits": sorted(payload["release_splits"]),
                "missing_local_assets": missing_assets,
                "refresh_priority": "high" if missing_assets else "standard",
            }
        )

    store_dir = layout.workspace_metadata_dir / "bootstrap_catalog"
    store_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = store_dir / plan_name
    manifest_path.write_text(
        json.dumps(
            {
                "generated_at": _utc_now(),
                "storage_root": str(layout.root),
                "selected_source": str(selected_source),
                "record_count": len(records),
                "records": records,
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    return BootstrapRefreshPlanResult(
        manifest_path=manifest_path,
        record_count=len(records),
        selected_source=str(selected_source),
    )


def execute_selected_pdb_refresh(
    layout: StorageLayout,
    config: AppConfig,
    *,
    manifest_path: Path | None = None,
    limit: int | None = None,
    only_missing_assets: bool = True,
    refresh_structures: bool = True,
    with_live_enrichment: bool = False,
    report_name: str = "selected_pdb_refresh_execution.json",
) -> BootstrapRefreshExecutionResult:
    """Refresh the selected PDB IDs described by a targeted refresh manifest."""
    manifest_path = manifest_path or (layout.bootstrap_store_dir / "selected_pdb_refresh_manifest.json")
    if not manifest_path.exists():
        raise FileNotFoundError(f"Refresh manifest not found: {manifest_path}")

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    raw_records = list(manifest.get("records") or [])
    if not raw_records:
        raise FileNotFoundError(f"No refresh records found in {manifest_path}.")
    if limit is not None:
        raw_records = raw_records[: max(limit, 0)]

    assay_samples_by_pdb = load_external_assay_samples(config, layout=layout)
    adapter = RCSBAdapter()

    refreshed_ids: list[str] = []
    failed_ids: list[str] = []
    failures: list[dict[str, str]] = []
    skipped_count = 0

    for record in raw_records:
        pdb_id = str(record.get("pdb_id") or "").strip().upper()
        if not pdb_id:
            continue
        missing_assets = set(record.get("missing_local_assets") or [])
        if only_missing_assets and not missing_assets:
            skipped_count += 1
            continue

        try:
            raw = _refresh_single_pdb(layout, pdb_id, refresh_structures=refresh_structures)
            chem_descriptors = _fetch_descriptors_for_raw(raw)
            normalized = adapter.normalize_record(raw, chem_descriptors=chem_descriptors)
            layout.processed_rcsb_dir.mkdir(parents=True, exist_ok=True)
            (layout.processed_rcsb_dir / f"{pdb_id}.json").write_text(
                normalized.model_dump_json(indent=2),
                encoding="utf-8",
            )

            assay_samples = list(assay_samples_by_pdb.get(pdb_id, []))
            if with_live_enrichment:
                assay_samples.extend(fetch_bindingdb_samples_for_pdb(pdb_id, config, layout=layout, raw=raw))
                assay_samples.extend(
                    fetch_chembl_samples_for_raw(
                        raw,
                        chem_descriptors,
                        config,
                        layout=layout,
                    )
                )

            extracted = extract_rcsb_entry(
                raw,
                chem_descriptors=chem_descriptors,
                assay_samples=assay_samples,
                structures_dir=layout.structures_rcsb_dir,
                download_structures=refresh_structures,
            )
            write_records_json(extracted, layout.extracted_dir)
            refreshed_ids.append(pdb_id)
        except Exception as exc:
            failed_ids.append(pdb_id)
            failures.append({"pdb_id": pdb_id, "error": str(exc)})

    export_status = refresh_master_exports(layout)
    report_dir = layout.bootstrap_store_dir
    report_dir.mkdir(parents=True, exist_ok=True)
    report_path = report_dir / report_name
    report_path.write_text(
        json.dumps(
            {
                "generated_at": _utc_now(),
                "storage_root": str(layout.root),
                "manifest_path": str(manifest_path),
                "with_live_enrichment": with_live_enrichment,
                "refreshed_count": len(refreshed_ids),
                "skipped_count": skipped_count,
                "failed_count": len(failed_ids),
                "refreshed_pdb_ids": refreshed_ids,
                "failed_pdb_ids": failed_ids,
                "failures": failures,
                "export_status": export_status,
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    return BootstrapRefreshExecutionResult(
        manifest_path=manifest_path,
        execution_report_path=report_path,
        refreshed_count=len(refreshed_ids),
        skipped_count=skipped_count,
        failed_count=len(failed_ids),
        refreshed_pdb_ids=tuple(refreshed_ids),
        failed_pdb_ids=tuple(failed_ids),
        export_status=export_status,
    )


def _record_row(record: dict[str, Any]) -> tuple[Any, ...]:
    updated_at = _utc_now()
    return (
        record["pdb_id"],
        str(record.get("experimental_method") or ""),
        str(record.get("resolution") or ""),
        json.dumps(record.get("organisms", []), separators=(",", ":")),
        json.dumps(record.get("source_databases", []), separators=(",", ":")),
        json.dumps(record.get("receptor_uniprot_ids", []), separators=(",", ":")),
        json.dumps(record.get("ligand_types", []), separators=(",", ":")),
        json.dumps(record.get("interface_types", []), separators=(",", ":")),
        json.dumps(record.get("affinity_types", []), separators=(",", ":")),
        int(record.get("pair_count") or 0),
        int(record.get("assay_count") or 0),
        int(record.get("chain_count") or 0),
        int(record.get("interface_count") or 0),
        int(record.get("bound_object_count") or 0),
        int(record.get("mutation_pair_count") or 0),
        1 if record.get("has_raw_payload") else 0,
        1 if record.get("has_processed_record") else 0,
        1 if record.get("has_structure_file") else 0,
        1 if record.get("bootstrap_ready") else 0,
        json.dumps(record, separators=(",", ":")),
        updated_at,
    )


def _load_records_from_store(database_path: Path) -> list[dict[str, Any]]:
    with sqlite3.connect(database_path) as connection:
        rows = connection.execute(
            "SELECT record_json FROM bootstrap_catalog ORDER BY pdb_id"
        ).fetchall()
    return [json.loads(payload) for (payload,) in rows if payload]


def _summary_row(record: dict[str, Any]) -> dict[str, Any]:
    source_databases = [str(value).strip() for value in record.get("source_databases", []) if str(value).strip()]
    receptor_uniprot_ids = [str(value).strip() for value in record.get("receptor_uniprot_ids", []) if str(value).strip()]
    ligand_types = [str(value).strip() for value in record.get("ligand_types", []) if str(value).strip()]
    interface_types = [str(value).strip() for value in record.get("interface_types", []) if str(value).strip()]
    affinity_types = [str(value).strip() for value in record.get("affinity_types", []) if str(value).strip()]
    organisms = [str(value).strip() for value in record.get("organisms", []) if str(value).strip()]
    source_set = {value.lower() for value in source_databases}

    return {
        "pdb_id": str(record.get("pdb_id") or ""),
        "experimental_method": str(record.get("experimental_method") or ""),
        "resolution": str(record.get("resolution") or ""),
        "organisms": ";".join(organisms),
        "source_databases": ";".join(source_databases),
        "source_count": len(source_databases),
        "receptor_uniprot_ids": ";".join(receptor_uniprot_ids),
        "receptor_uniprot_count": len(receptor_uniprot_ids),
        "ligand_types": ";".join(ligand_types),
        "interface_types": ";".join(interface_types),
        "affinity_types": ";".join(affinity_types),
        "pair_count": int(record.get("pair_count") or 0),
        "assay_count": int(record.get("assay_count") or 0),
        "chain_count": int(record.get("chain_count") or 0),
        "interface_count": int(record.get("interface_count") or 0),
        "bound_object_count": int(record.get("bound_object_count") or 0),
        "mutation_pair_count": int(record.get("mutation_pair_count") or 0),
        "has_bindingdb": "bindingdb" in source_set,
        "has_chembl": "chembl" in source_set,
        "has_pdbbind": "pdbbind" in source_set,
        "has_biolip": "biolip" in source_set,
        "has_skempi": "skempi" in source_set,
        "has_raw_payload": bool(record.get("has_raw_payload")),
        "has_processed_record": bool(record.get("has_processed_record")),
        "has_structure_file": bool(record.get("has_structure_file")),
        "bootstrap_ready": bool(record.get("bootstrap_ready")),
    }


def _load_pair_summary_rows(layout: StorageLayout) -> list[dict[str, str]]:
    source_path = _resolve_pair_summary_source(layout)
    if source_path is None:
        return []
    with source_path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    return [_pair_summary_row(row) for row in rows]


def _pair_summary_row(row: dict[str, Any]) -> dict[str, str]:
    return {
        "pdb_id": str(row.get("pdb_id") or "").strip().upper(),
        "pair_identity_key": str(row.get("pair_identity_key") or "").strip(),
        "source_database": str(row.get("source_database") or "").strip(),
        "receptor_uniprot_ids": str(row.get("receptor_uniprot_ids") or "").strip(),
        "ligand_types": str(row.get("ligand_types") or row.get("ligand_type") or "").strip(),
        "matching_interface_types": str(row.get("matching_interface_types") or row.get("interface_type") or "").strip(),
        "binding_affinity_type": str(row.get("binding_affinity_type") or "").strip(),
        "mutation_strings": str(row.get("mutation_strings") or row.get("mutation_string") or "").strip(),
        "release_split": str(row.get("release_split") or "").strip(),
        "source_conflict_summary": str(row.get("source_conflict_summary") or "").strip(),
        "source_agreement_band": str(row.get("source_agreement_band") or "").strip(),
    }


def _resolve_selected_source(layout: StorageLayout) -> Path:
    preferred = layout.root / "custom_training_set.csv"
    if preferred.exists() and preferred.stat().st_size > 0:
        try:
            if _read_csv_rows(preferred):
                return preferred
        except Exception:
            pass
    fallback = layout.root / "model_ready_pairs.csv"
    if fallback.exists() and fallback.stat().st_size > 0:
        try:
            if _read_csv_rows(fallback):
                return fallback
        except Exception:
            pass
    raise FileNotFoundError("Neither custom_training_set.csv nor model_ready_pairs.csv is available.")


def _resolve_pair_summary_source(layout: StorageLayout) -> Path | None:
    preferred = layout.root / "model_ready_pairs.csv"
    if preferred.exists() and preferred.stat().st_size > 0:
        return preferred
    fallback = layout.root / "master_pdb_pairs.csv"
    if fallback.exists() and fallback.stat().st_size > 0:
        return fallback
    return None


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _split_values(raw: Any) -> list[str]:
    text = str(raw or "")
    values = [part.strip() for part in text.replace(";", ",").split(",") if part.strip()]
    return list(dict.fromkeys(values))


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _refresh_single_pdb(
    layout: StorageLayout,
    pdb_id: str,
    *,
    refresh_structures: bool,
) -> dict[str, Any]:
    entries = fetch_entries_batch([pdb_id])
    if not entries:
        raise ValueError(f"No RCSB entry found for {pdb_id}.")

    raw = entries[0]
    try:
        supplement = fetch_mmcif_supplement(pdb_id, structures_dir=layout.structures_rcsb_dir)
    except Exception:
        supplement = None
    if supplement:
        raw["mmcif_supplement"] = supplement

    file_provenance = download_structure_files(
        pdb_id,
        structures_dir=layout.structures_rcsb_dir,
        download_pdb=False,
    ) if refresh_structures else {}
    if file_provenance:
        raw["structure_file_provenance"] = file_provenance

    layout.raw_rcsb_dir.mkdir(parents=True, exist_ok=True)
    (layout.raw_rcsb_dir / f"{pdb_id}.json").write_text(
        json.dumps(raw, indent=2),
        encoding="utf-8",
    )
    return raw


def _fetch_descriptors_for_raw(raw: dict[str, Any]) -> dict[str, dict[str, str]]:
    comp_ids = [
        str((((entity.get("nonpolymer_comp") or {}).get("chem_comp") or {}).get("id") or "")).strip().upper()
        for entity in raw.get("nonpolymer_entities") or []
    ]
    unique_ids = [comp_id for comp_id in dict.fromkeys(comp_ids) if comp_id]
    if not unique_ids:
        return {}
    return fetch_chemcomp_descriptors(unique_ids)
