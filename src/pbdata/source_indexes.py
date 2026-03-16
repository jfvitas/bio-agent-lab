"""Persistent local indexes for large staged source assets.

These indexes convert heavyweight bulk archives into lightweight lookup layers
that can be reused by dataset planning, enrichment, and GUI readiness flows.
"""

from __future__ import annotations

import gzip
import json
import tarfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pbdata.storage import StorageLayout


@dataclass(frozen=True)
class SourceIndexResult:
    source_name: str
    index_path: Path
    manifest_path: Path
    record_count: int


def index_alphafold_archive(
    layout: StorageLayout,
    *,
    archive_path: Path,
    limit: int | None = None,
) -> SourceIndexResult:
    """Index a local AlphaFold archive by accession and archive member name."""
    if not archive_path.exists():
        raise FileNotFoundError(f"AlphaFold archive not found: {archive_path}")

    output_dir = layout.source_indexes_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    index_path = output_dir / "alphafold_archive_index.jsonl.gz"
    manifest_path = output_dir / "alphafold_archive_index_manifest.json"

    record_count = 0
    with gzip.open(index_path, "wt", encoding="utf-8") as handle:
        with tarfile.open(archive_path, "r") as archive:
            for member in archive:
                if not member.isfile():
                    continue
                accession = _alphafold_accession_from_member(member.name)
                if not accession:
                    continue
                payload = {
                    "accession": accession,
                    "entry_id": _alphafold_entry_id_from_member(member.name),
                    "member_name": member.name,
                    "size_bytes": int(member.size),
                    "model_version": _alphafold_model_version_from_member(member.name),
                }
                handle.write(json.dumps(payload, separators=(",", ":")))
                handle.write("\n")
                record_count += 1
                if limit is not None and record_count >= limit:
                    break

    manifest = {
        "generated_at": _utc_now(),
        "source_name": "alphafold_db",
        "archive_path": str(archive_path),
        "index_path": str(index_path),
        "record_count": record_count,
        "limit": limit,
        "intended_use": [
            "local predicted-structure lookup by UniProt accession",
            "fast readiness and coverage checks",
            "targeted extraction planning before full unpack",
        ],
    }
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return SourceIndexResult(
        source_name="alphafold_db",
        index_path=index_path,
        manifest_path=manifest_path,
        record_count=record_count,
    )


def index_uniprot_swissprot(
    layout: StorageLayout,
    *,
    source_path: Path,
    limit: int | None = None,
) -> SourceIndexResult:
    """Stream-index a staged UniProt Swiss-Prot flat file."""
    if not source_path.exists():
        raise FileNotFoundError(f"UniProt source file not found: {source_path}")

    output_dir = layout.source_indexes_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    index_path = output_dir / "uniprot_swissprot_index.jsonl.gz"
    manifest_path = output_dir / "uniprot_swissprot_index_manifest.json"

    record_count = 0
    with gzip.open(index_path, "wt", encoding="utf-8") as out_handle:
        with gzip.open(source_path, "rt", encoding="utf-8", errors="replace") as in_handle:
            current: dict[str, Any] = _empty_uniprot_record()
            sequence_mode = False
            for raw_line in in_handle:
                line = raw_line.rstrip("\n")
                if line == "//":
                    if current["accession"]:
                        current["gene_names"] = sorted(current["gene_names"])
                        current["pdb_ids"] = sorted(current["pdb_ids"])
                        current["interpro_ids"] = sorted(current["interpro_ids"])
                        current["pfam_ids"] = sorted(current["pfam_ids"])
                        current["go_terms"] = sorted(current["go_terms"])
                        current["keywords"] = sorted(current["keywords"])
                        current["sequence_length"] = len(current["sequence"])
                        out_handle.write(json.dumps(_serialize_uniprot_record(current), separators=(",", ":")))
                        out_handle.write("\n")
                        record_count += 1
                        if limit is not None and record_count >= limit:
                            break
                    current = _empty_uniprot_record()
                    sequence_mode = False
                    continue

                if sequence_mode:
                    if line.startswith(" "):
                        current["sequence"] += "".join(line.split())
                        continue
                    sequence_mode = False

                if line.startswith("ID   "):
                    current["reviewed"] = "Reviewed;" in line
                elif line.startswith("AC   ") and not current["accession"]:
                    accession = line[5:].split(";")[0].strip()
                    current["accession"] = accession.upper()
                elif line.startswith("DE   RecName: Full=") and not current["protein_name"]:
                    current["protein_name"] = line.split("Full=", 1)[1].rstrip(";").strip()
                elif line.startswith("GN   Name="):
                    gene_name = line.split("Name=", 1)[1].split(";", 1)[0].strip()
                    if gene_name:
                        current["gene_names"].add(gene_name)
                elif line.startswith("OS   "):
                    text = line[5:].strip()
                    current["organism_name"] = (current["organism_name"] + " " + text).strip() if current["organism_name"] else text
                elif line.startswith("OX   NCBI_TaxID=") and current["taxonomy_id"] is None:
                    taxid_text = line.split("NCBI_TaxID=", 1)[1].split(";", 1)[0].strip()
                    try:
                        current["taxonomy_id"] = int(taxid_text)
                    except ValueError:
                        current["taxonomy_id"] = None
                elif line.startswith("DR   "):
                    parts = [part.strip() for part in line[5:].split(";")]
                    if len(parts) >= 2:
                        database = parts[0]
                        ref_id = parts[1]
                        if database == "PDB":
                            current["pdb_ids"].add(ref_id)
                        elif database == "InterPro":
                            current["interpro_ids"].add(ref_id)
                        elif database == "Pfam":
                            current["pfam_ids"].add(ref_id)
                        elif database == "GO":
                            current["go_terms"].add(ref_id)
                elif line.startswith("KW   "):
                    keywords = [keyword.strip().rstrip(".") for keyword in line[5:].split(";") if keyword.strip()]
                    current["keywords"].update(keywords)
                elif line.startswith("SQ   SEQUENCE"):
                    sequence_mode = True

    manifest = {
        "generated_at": _utc_now(),
        "source_name": "uniprot",
        "source_path": str(source_path),
        "index_path": str(index_path),
        "record_count": record_count,
        "limit": limit,
        "intended_use": [
            "local accession lookup",
            "bootstrap annotation coverage planning",
            "cross-source identity enrichment",
        ],
    }
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return SourceIndexResult(
        source_name="uniprot",
        index_path=index_path,
        manifest_path=manifest_path,
        record_count=record_count,
    )


def _empty_uniprot_record() -> dict[str, Any]:
    return {
        "accession": "",
        "reviewed": False,
        "protein_name": "",
        "gene_names": set(),
        "organism_name": "",
        "taxonomy_id": None,
        "sequence": "",
        "pdb_ids": set(),
        "interpro_ids": set(),
        "pfam_ids": set(),
        "go_terms": set(),
        "keywords": set(),
    }


def _serialize_uniprot_record(record: dict[str, Any]) -> dict[str, Any]:
    return {
        "accession": record["accession"],
        "reviewed": bool(record["reviewed"]),
        "protein_name": record["protein_name"],
        "gene_names": list(record["gene_names"]),
        "organism_name": record["organism_name"].rstrip("."),
        "taxonomy_id": record["taxonomy_id"],
        "sequence_length": record["sequence_length"],
        "pdb_ids": list(record["pdb_ids"]),
        "interpro_ids": list(record["interpro_ids"]),
        "pfam_ids": list(record["pfam_ids"]),
        "go_terms": list(record["go_terms"]),
        "keywords": list(record["keywords"]),
    }


def _alphafold_accession_from_member(name: str) -> str | None:
    normalized = Path(name).name
    if not normalized.startswith("AF-") or "-model_" not in normalized:
        return None
    parts = normalized.split("-")
    if len(parts) < 3:
        return None
    return parts[1].strip().upper() or None


def _alphafold_entry_id_from_member(name: str) -> str:
    normalized = Path(name).name
    stem = normalized.removesuffix(".pdb.gz").removesuffix(".cif.gz").removesuffix(".pdb").removesuffix(".cif")
    return stem


def _alphafold_model_version_from_member(name: str) -> str:
    normalized = Path(name).name
    if "model_" not in normalized:
        return ""
    return normalized.split("model_", 1)[1].split(".", 1)[0]


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()
