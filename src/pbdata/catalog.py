"""Download-manifest helpers for raw source files."""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Any

DEFAULT_MANIFEST_PATH = Path("data/catalog/download_manifest.csv")

_MANIFEST_COLUMNS = [
    "source_database",
    "source_record_id",
    "pdb_id",
    "task_hint",
    "title",
    "experimental_method",
    "structure_resolution",
    "release_date",
    "deposit_date",
    "protein_entity_count",
    "nonpolymer_entity_count",
    "deposited_atom_count",
    "ligand_ids",
    "ligand_names",
    "taxonomy_ids",
    "raw_file_path",
    "raw_format",
    "file_size_bytes",
    "downloaded_at",
    "status",
    "notes",
]


def _stringify(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, list):
        return ";".join(str(v) for v in value if v not in (None, ""))
    return str(value)


def _row_key(row: dict[str, Any]) -> tuple[str, str, str]:
    return (
        _stringify(row.get("source_database")),
        _stringify(row.get("source_record_id")),
        _stringify(row.get("raw_file_path")),
    )


def _infer_rcsb_task_hint(entry: dict[str, Any]) -> str:
    entry_info = entry.get("rcsb_entry_info") or {}
    protein_count = int(entry_info.get("polymer_entity_count_protein") or 0)
    nonpoly_count = int(entry_info.get("nonpolymer_entity_count") or 0)
    if protein_count >= 1 and nonpoly_count > 0:
        return "protein_ligand"
    if protein_count >= 2:
        return "protein_protein"
    return "unspecified"


def summarize_rcsb_entry(
    entry: dict[str, Any],
    raw_file_path: Path,
    *,
    downloaded_at: str,
    status: str,
) -> dict[str, str]:
    """Flatten one RCSB raw entry into a manifest row."""
    entry_info = entry.get("rcsb_entry_info") or {}
    accession = entry.get("rcsb_accession_info") or {}
    exptl = entry.get("exptl") or []

    ligand_ids: list[str] = []
    ligand_names: list[str] = []
    for entity in entry.get("nonpolymer_entities") or []:
        chem = (entity.get("nonpolymer_comp") or {}).get("chem_comp") or {}
        chem_id = chem.get("id")
        chem_name = chem.get("name")
        if chem_id:
            ligand_ids.append(str(chem_id))
        if chem_name:
            ligand_names.append(str(chem_name))

    taxonomy_ids: list[str] = []
    for entity in entry.get("polymer_entities") or []:
        for organism in entity.get("rcsb_entity_source_organism") or []:
            tid = organism.get("ncbi_taxonomy_id")
            if tid not in (None, ""):
                taxonomy_ids.append(str(tid))

    resolution = entry_info.get("resolution_combined")
    if isinstance(resolution, list):
        resolution = resolution[0] if resolution else None

    return {
        "source_database": "RCSB",
        "source_record_id": str(entry.get("rcsb_id") or ""),
        "pdb_id": str(entry.get("rcsb_id") or ""),
        "task_hint": _infer_rcsb_task_hint(entry),
        "title": str((entry.get("struct") or {}).get("title") or ""),
        "experimental_method": str(exptl[0].get("method") or "") if exptl else "",
        "structure_resolution": _stringify(resolution),
        "release_date": str(accession.get("initial_release_date") or ""),
        "deposit_date": str(accession.get("deposit_date") or ""),
        "protein_entity_count": _stringify(entry_info.get("polymer_entity_count_protein")),
        "nonpolymer_entity_count": _stringify(entry_info.get("nonpolymer_entity_count")),
        "deposited_atom_count": _stringify(entry_info.get("deposited_atom_count")),
        "ligand_ids": _stringify(ligand_ids),
        "ligand_names": _stringify(ligand_names),
        "taxonomy_ids": _stringify(sorted(set(taxonomy_ids))),
        "raw_file_path": str(raw_file_path),
        "raw_format": "json",
        "file_size_bytes": _stringify(raw_file_path.stat().st_size if raw_file_path.exists() else None),
        "downloaded_at": downloaded_at,
        "status": status,
        "notes": "",
    }


def summarize_bulk_file(
    *,
    source_database: str,
    source_record_id: str,
    raw_file_path: Path,
    raw_format: str,
    downloaded_at: str,
    title: str = "",
    pdb_id: str = "",
    task_hint: str = "",
    notes: str = "",
    status: str = "downloaded",
) -> dict[str, str]:
    """Build a manifest row for one bulk raw file such as SKEMPI CSV."""
    return {
        "source_database": source_database,
        "source_record_id": source_record_id,
        "pdb_id": pdb_id,
        "task_hint": task_hint,
        "title": title,
        "experimental_method": "",
        "structure_resolution": "",
        "release_date": "",
        "deposit_date": "",
        "protein_entity_count": "",
        "nonpolymer_entity_count": "",
        "deposited_atom_count": "",
        "ligand_ids": "",
        "ligand_names": "",
        "taxonomy_ids": "",
        "raw_file_path": str(raw_file_path),
        "raw_format": raw_format,
        "file_size_bytes": _stringify(raw_file_path.stat().st_size if raw_file_path.exists() else None),
        "downloaded_at": downloaded_at,
        "status": status,
        "notes": notes,
    }


def update_download_manifest(
    rows: list[dict[str, Any]],
    manifest_path: Path = DEFAULT_MANIFEST_PATH,
) -> None:
    """Upsert manifest rows into the cross-database CSV catalog."""
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    existing: dict[tuple[str, str, str], dict[str, str]] = {}

    if manifest_path.exists():
        with manifest_path.open(newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                existing[_row_key(row)] = {
                    col: row.get(col, "") for col in _MANIFEST_COLUMNS
                }

    for row in rows:
        normalized = {col: _stringify(row.get(col)) for col in _MANIFEST_COLUMNS}
        existing[_row_key(normalized)] = normalized

    ordered_rows = sorted(existing.values(), key=lambda row: _row_key(row))
    with manifest_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=_MANIFEST_COLUMNS)
        writer.writeheader()
        writer.writerows(ordered_rows)
