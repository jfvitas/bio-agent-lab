"""Persistent local indexes for large staged source assets.

These indexes convert heavyweight bulk archives into lightweight lookup layers
that can be reused by dataset planning, enrichment, and GUI readiness flows.
"""

from __future__ import annotations

import gzip
import json
import sqlite3
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
    lookup_db_path: Path | None = None


def index_alphafold_archive(
    layout: StorageLayout,
    *,
    archive_path: Path,
    limit: int | None = None,
    chunk_size: int | None = None,
    resume: bool = False,
    force: bool = False,
) -> SourceIndexResult:
    """Index a local AlphaFold archive by accession and archive member name.

    Assumptions:
    - The staged AlphaFold archive is an uncompressed tar where each member name
      contains the UniProt accession and model version.
    - Resuming from a saved tar byte offset is stable as long as the source tar
      file is unchanged.
    """
    if not archive_path.exists():
        raise FileNotFoundError(f"AlphaFold archive not found: {archive_path}")

    output_dir = layout.source_indexes_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    index_path = output_dir / "alphafold_archive_index.jsonl.gz"
    lookup_db_path = output_dir / "alphafold_archive_index.sqlite"
    manifest_path = output_dir / "alphafold_archive_index_manifest.json"
    source_mtime = archive_path.stat().st_mtime
    record_budget = chunk_size if chunk_size is not None else limit

    existing_manifest = _load_manifest(manifest_path)
    can_resume = (
        resume
        and index_path.exists()
        and lookup_db_path.exists()
        and existing_manifest.get("archive_path") == str(archive_path)
        and float(existing_manifest.get("archive_mtime_epoch") or 0.0) == float(source_mtime)
    )
    if (
        not force
        and not resume
        and record_budget is None
        and existing_manifest.get("status") == "completed"
        and index_path.exists()
        and lookup_db_path.exists()
        and existing_manifest.get("archive_path") == str(archive_path)
        and float(existing_manifest.get("archive_mtime_epoch") or 0.0) == float(source_mtime)
    ):
        return SourceIndexResult(
            source_name="alphafold_db",
            index_path=index_path,
            manifest_path=manifest_path,
            record_count=int(existing_manifest.get("record_count") or 0),
            lookup_db_path=lookup_db_path,
        )

    if force or not can_resume:
        index_path.unlink(missing_ok=True)
        lookup_db_path.unlink(missing_ok=True)

    start_offset = int(existing_manifest.get("progress", {}).get("next_byte_offset") or 0) if can_resume else 0
    existing_count = int(existing_manifest.get("record_count") or 0) if can_resume else 0
    completed_chunks = int(existing_manifest.get("progress", {}).get("completed_chunks") or 0) if can_resume else 0
    chunk_index = completed_chunks if can_resume else 0

    conn = sqlite3.connect(lookup_db_path)
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS alphafold_members (
                entry_id TEXT PRIMARY KEY,
                accession TEXT NOT NULL,
                member_name TEXT NOT NULL,
                size_bytes INTEGER NOT NULL,
                model_version TEXT NOT NULL
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_alphafold_accession ON alphafold_members(accession)"
        )

        indexed_this_run = 0
        next_offset = start_offset
        with gzip.open(index_path, "at" if can_resume else "wt", encoding="utf-8") as handle:
            with archive_path.open("rb") as raw_handle:
                raw_handle.seek(start_offset)
                with tarfile.open(fileobj=raw_handle, mode="r|") as archive:
                    for member in archive:
                        if not member.isfile():
                            next_offset = _next_tar_header_offset(start_offset, member)
                            continue
                        accession = _alphafold_accession_from_member(member.name)
                        if not accession:
                            next_offset = _next_tar_header_offset(start_offset, member)
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
                        conn.execute(
                            """
                            INSERT OR REPLACE INTO alphafold_members (
                                entry_id,
                                accession,
                                member_name,
                                size_bytes,
                                model_version
                            ) VALUES (?, ?, ?, ?, ?)
                            """,
                            (
                                payload["entry_id"],
                                payload["accession"],
                                payload["member_name"],
                                payload["size_bytes"],
                                payload["model_version"],
                            ),
                        )
                        indexed_this_run += 1
                        next_offset = _next_tar_header_offset(start_offset, member)
                        if record_budget is not None and indexed_this_run >= record_budget:
                            break
        conn.commit()
        total_count = existing_count + indexed_this_run
        archive_complete = _archive_iteration_completed(archive_path, next_offset)
        status = "completed" if archive_complete and (record_budget is None or indexed_this_run > 0) else "partial"
        manifest = {
            "generated_at": _utc_now(),
            "source_name": "alphafold_db",
            "archive_path": str(archive_path),
            "archive_mtime_epoch": source_mtime,
            "index_path": str(index_path),
            "lookup_db_path": str(lookup_db_path),
            "record_count": total_count,
            "limit": limit,
            "chunk_size": chunk_size,
            "status": status,
            "intended_use": [
                "local predicted-structure lookup by UniProt accession",
                "fast readiness and coverage checks",
                "targeted extraction planning before full unpack",
            ],
            "progress": {
                "resume_supported": True,
                "next_byte_offset": next_offset,
                "completed_chunks": completed_chunks + (1 if indexed_this_run or (record_budget is not None and archive_complete) else 0),
                "indexed_this_run": indexed_this_run,
            },
        }
        manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
        return SourceIndexResult(
            source_name="alphafold_db",
            index_path=index_path,
            manifest_path=manifest_path,
            record_count=total_count,
            lookup_db_path=lookup_db_path,
        )
    finally:
        conn.close()


def index_uniprot_swissprot(
    layout: StorageLayout,
    *,
    source_path: Path,
    limit: int | None = None,
    force: bool = False,
) -> SourceIndexResult:
    """Stream-index a staged UniProt Swiss-Prot flat file."""
    if not source_path.exists():
        raise FileNotFoundError(f"UniProt source file not found: {source_path}")

    output_dir = layout.source_indexes_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    index_path = output_dir / "uniprot_swissprot_index.jsonl.gz"
    lookup_db_path = output_dir / "uniprot_swissprot_index.sqlite"
    manifest_path = output_dir / "uniprot_swissprot_index_manifest.json"
    source_mtime = source_path.stat().st_mtime
    existing_manifest = _load_manifest(manifest_path)
    if (
        not force
        and limit is None
        and index_path.exists()
        and lookup_db_path.exists()
        and existing_manifest.get("status") == "completed"
        and existing_manifest.get("source_path") == str(source_path)
        and float(existing_manifest.get("source_mtime_epoch") or 0.0) == float(source_mtime)
    ):
        return SourceIndexResult(
            source_name="uniprot",
            index_path=index_path,
            manifest_path=manifest_path,
            record_count=int(existing_manifest.get("record_count") or 0),
            lookup_db_path=lookup_db_path,
        )

    if force:
        index_path.unlink(missing_ok=True)
        lookup_db_path.unlink(missing_ok=True)

    record_count = 0
    conn = sqlite3.connect(lookup_db_path)
    try:
        conn.execute("DROP TABLE IF EXISTS uniprot_records")
        conn.execute(
            """
            CREATE TABLE uniprot_records (
                accession TEXT PRIMARY KEY,
                payload_json TEXT NOT NULL
            )
            """
        )

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
                            serialized = _serialize_uniprot_record(current)
                            payload_json = json.dumps(serialized, separators=(",", ":"))
                            out_handle.write(payload_json)
                            out_handle.write("\n")
                            conn.execute(
                                "INSERT OR REPLACE INTO uniprot_records(accession, payload_json) VALUES (?, ?)",
                                (serialized["accession"], payload_json),
                            )
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
        conn.commit()
    finally:
        conn.close()

    manifest = {
        "generated_at": _utc_now(),
        "source_name": "uniprot",
        "source_path": str(source_path),
        "source_mtime_epoch": source_mtime,
        "index_path": str(index_path),
        "lookup_db_path": str(lookup_db_path),
        "record_count": record_count,
        "limit": limit,
        "status": "completed" if limit is None else "partial",
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
        lookup_db_path=lookup_db_path,
    )


def index_reactome_pathways(
    layout: StorageLayout,
    *,
    mapping_path: Path,
    pathways_path: Path | None = None,
    limit: int | None = None,
    force: bool = False,
) -> SourceIndexResult:
    """Build a local UniProt-to-Reactome lookup from staged flat files."""
    if not mapping_path.exists():
        raise FileNotFoundError(f"Reactome mapping file not found: {mapping_path}")

    output_dir = layout.source_indexes_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    index_path = output_dir / "reactome_pathway_index.jsonl.gz"
    lookup_db_path = output_dir / "reactome_pathway_index.sqlite"
    manifest_path = output_dir / "reactome_pathway_index_manifest.json"
    mapping_mtime = mapping_path.stat().st_mtime
    pathways_mtime = pathways_path.stat().st_mtime if pathways_path is not None and pathways_path.exists() else None
    existing_manifest = _load_manifest(manifest_path)
    if (
        not force
        and limit is None
        and index_path.exists()
        and lookup_db_path.exists()
        and existing_manifest.get("status") == "completed"
        and existing_manifest.get("mapping_path") == str(mapping_path)
        and float(existing_manifest.get("mapping_mtime_epoch") or 0.0) == float(mapping_mtime)
        and existing_manifest.get("pathways_path") == (str(pathways_path) if pathways_path is not None else None)
        and float(existing_manifest.get("pathways_mtime_epoch") or 0.0) == float(pathways_mtime or 0.0)
    ):
        return SourceIndexResult(
            source_name="reactome",
            index_path=index_path,
            manifest_path=manifest_path,
            record_count=int(existing_manifest.get("record_count") or 0),
            lookup_db_path=lookup_db_path,
        )

    if force:
        index_path.unlink(missing_ok=True)
        lookup_db_path.unlink(missing_ok=True)

    pathway_name_by_id = _load_reactome_pathway_names(pathways_path)
    conn = sqlite3.connect(lookup_db_path)
    raw_row_count = 0
    try:
        conn.execute("DROP TABLE IF EXISTS reactome_membership")
        conn.execute("DROP TABLE IF EXISTS reactome_records")
        conn.execute(
            """
            CREATE TABLE reactome_membership (
                accession TEXT NOT NULL,
                pathway_id TEXT NOT NULL,
                pathway_name TEXT NOT NULL,
                species TEXT NOT NULL,
                evidence_code TEXT,
                pathway_url TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE reactome_records (
                accession TEXT PRIMARY KEY,
                payload_json TEXT NOT NULL
            )
            """
        )

        batch: list[tuple[str, str, str, str, str | None, str | None]] = []
        with mapping_path.open("r", encoding="utf-8", errors="replace") as handle:
            for raw_line in handle:
                line = raw_line.rstrip("\n")
                if not line:
                    continue
                parts = line.split("\t")
                if len(parts) < 6:
                    continue
                accession = parts[0].strip().upper()
                pathway_id = parts[1].strip()
                pathway_url = parts[2].strip() or None
                pathway_name = pathway_name_by_id.get(pathway_id) or parts[3].strip()
                evidence_code = parts[4].strip() or None
                species = parts[5].strip()
                if not accession or not pathway_id:
                    continue
                batch.append((accession, pathway_id, pathway_name, species, evidence_code, pathway_url))
                raw_row_count += 1
                if len(batch) >= 5000:
                    conn.executemany(
                        """
                        INSERT INTO reactome_membership (
                            accession,
                            pathway_id,
                            pathway_name,
                            species,
                            evidence_code,
                            pathway_url
                        ) VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        batch,
                    )
                    batch.clear()
        if batch:
            conn.executemany(
                """
                INSERT INTO reactome_membership (
                    accession,
                    pathway_id,
                    pathway_name,
                    species,
                    evidence_code,
                    pathway_url
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                batch,
            )
        conn.execute(
            """
            CREATE INDEX idx_reactome_membership_accession
            ON reactome_membership(accession)
            """
        )

        record_count = 0
        with gzip.open(index_path, "wt", encoding="utf-8") as out_handle:
            current_accession = ""
            current_items: list[dict[str, Any]] = []
            current_species: list[str] = []
            for row in conn.execute(
                """
                SELECT accession, pathway_id, pathway_name, species, evidence_code, pathway_url
                FROM reactome_membership
                ORDER BY accession, pathway_id, species
                """
            ):
                accession = str(row[0])
                item = {
                    "pathway_id": str(row[1]),
                    "pathway_name": str(row[2]),
                    "species": str(row[3]),
                    "evidence_code": str(row[4]) if row[4] is not None else "",
                    "pathway_url": str(row[5]) if row[5] is not None else "",
                }
                if current_accession and accession != current_accession:
                    payload_json = _write_reactome_record(
                        out_handle,
                        current_accession,
                        current_items,
                        current_species,
                    )
                    conn.execute(
                        "INSERT OR REPLACE INTO reactome_records(accession, payload_json) VALUES (?, ?)",
                        (current_accession, payload_json),
                    )
                    record_count += 1
                    if limit is not None and record_count >= limit:
                        current_accession = ""
                        current_items = []
                        current_species = []
                        break
                    current_items = []
                    current_species = []
                current_accession = accession
                current_items.append(item)
                if item["species"] and item["species"] not in current_species:
                    current_species.append(item["species"])
            if current_accession and current_items and (limit is None or record_count < limit):
                payload_json = _write_reactome_record(
                    out_handle,
                    current_accession,
                    current_items,
                    current_species,
                )
                conn.execute(
                    "INSERT OR REPLACE INTO reactome_records(accession, payload_json) VALUES (?, ?)",
                    (current_accession, payload_json),
                )
                record_count += 1
        conn.commit()
    finally:
        conn.close()

    manifest = {
        "generated_at": _utc_now(),
        "source_name": "reactome",
        "mapping_path": str(mapping_path),
        "mapping_mtime_epoch": mapping_mtime,
        "pathways_path": str(pathways_path) if pathways_path is not None else None,
        "pathways_mtime_epoch": pathways_mtime,
        "index_path": str(index_path),
        "lookup_db_path": str(lookup_db_path),
        "record_count": record_count,
        "raw_membership_row_count": raw_row_count,
        "limit": limit,
        "status": "completed" if limit is None else "partial",
        "intended_use": [
            "local UniProt-to-Reactome pathway lookup",
            "offline metadata harvest enrichment",
            "reproducible pathway coverage auditing",
        ],
    }
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return SourceIndexResult(
        source_name="reactome",
        index_path=index_path,
        manifest_path=manifest_path,
        record_count=record_count,
        lookup_db_path=lookup_db_path,
    )


def index_cath_domains(
    layout: StorageLayout,
    *,
    domain_list_path: Path,
    boundaries_path: Path,
    names_path: Path | None = None,
    limit: int | None = None,
    force: bool = False,
) -> SourceIndexResult:
    """Build a local PDB-chain-to-CATH lookup from staged flat files."""
    if not domain_list_path.exists():
        raise FileNotFoundError(f"CATH domain list file not found: {domain_list_path}")
    if not boundaries_path.exists():
        raise FileNotFoundError(f"CATH domain boundaries file not found: {boundaries_path}")

    output_dir = layout.source_indexes_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    index_path = output_dir / "cath_domain_index.jsonl.gz"
    lookup_db_path = output_dir / "cath_domain_index.sqlite"
    manifest_path = output_dir / "cath_domain_index_manifest.json"
    domain_list_mtime = domain_list_path.stat().st_mtime
    boundaries_mtime = boundaries_path.stat().st_mtime
    names_mtime = names_path.stat().st_mtime if names_path is not None and names_path.exists() else None
    existing_manifest = _load_manifest(manifest_path)
    if (
        not force
        and limit is None
        and index_path.exists()
        and lookup_db_path.exists()
        and existing_manifest.get("status") == "completed"
        and existing_manifest.get("domain_list_path") == str(domain_list_path)
        and float(existing_manifest.get("domain_list_mtime_epoch") or 0.0) == float(domain_list_mtime)
        and existing_manifest.get("boundaries_path") == str(boundaries_path)
        and float(existing_manifest.get("boundaries_mtime_epoch") or 0.0) == float(boundaries_mtime)
        and existing_manifest.get("names_path") == (str(names_path) if names_path is not None else None)
        and float(existing_manifest.get("names_mtime_epoch") or 0.0) == float(names_mtime or 0.0)
    ):
        return SourceIndexResult(
            source_name="cath",
            index_path=index_path,
            manifest_path=manifest_path,
            record_count=int(existing_manifest.get("record_count") or 0),
            lookup_db_path=lookup_db_path,
        )

    if force:
        index_path.unlink(missing_ok=True)
        lookup_db_path.unlink(missing_ok=True)

    name_by_code = _load_cath_names(names_path)
    classification_by_domain = _load_cath_classification(domain_list_path)
    conn = sqlite3.connect(lookup_db_path)
    record_count = 0
    mapping_count = 0
    try:
        conn.execute("DROP TABLE IF EXISTS cath_membership")
        conn.execute("DROP TABLE IF EXISTS cath_records")
        conn.execute(
            """
            CREATE TABLE cath_membership (
                pdb_id TEXT NOT NULL,
                chain_id TEXT NOT NULL,
                domain_id TEXT NOT NULL,
                domain_name TEXT NOT NULL,
                domain_instance_id TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE cath_records (
                pdb_id TEXT PRIMARY KEY,
                payload_json TEXT NOT NULL
            )
            """
        )
        batch: list[tuple[str, str, str, str, str]] = []
        for raw_line in boundaries_path.open("r", encoding="utf-8", errors="replace"):
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            domain_instance_id, chain_id = _parse_cath_boundary_line(line)
            classification = classification_by_domain.get(domain_instance_id)
            if classification is None:
                continue
            pdb_id = domain_instance_id[:4].upper()
            domain_name = name_by_code.get(classification) or ""
            batch.append((pdb_id, chain_id, classification, domain_name, domain_instance_id))
            mapping_count += 1
            if len(batch) >= 5000:
                conn.executemany(
                    """
                    INSERT INTO cath_membership (
                        pdb_id,
                        chain_id,
                        domain_id,
                        domain_name,
                        domain_instance_id
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    batch,
                )
                batch.clear()
        if batch:
            conn.executemany(
                """
                INSERT INTO cath_membership (
                    pdb_id,
                    chain_id,
                    domain_id,
                    domain_name,
                    domain_instance_id
                ) VALUES (?, ?, ?, ?, ?)
                """,
                batch,
            )
        conn.execute("CREATE INDEX idx_cath_membership_pdb ON cath_membership(pdb_id)")

        with gzip.open(index_path, "wt", encoding="utf-8") as out_handle:
            current_pdb_id = ""
            rows_for_pdb: list[dict[str, Any]] = []
            for row in conn.execute(
                """
                SELECT pdb_id, chain_id, domain_id, domain_name, domain_instance_id
                FROM cath_membership
                ORDER BY pdb_id, chain_id, domain_id, domain_instance_id
                """
            ):
                pdb_id = str(row[0])
                payload_row = {
                    "chain_id": str(row[1]),
                    "domain_id": str(row[2]),
                    "domain_name": str(row[3]),
                    "domain_instance_id": str(row[4]),
                }
                if current_pdb_id and pdb_id != current_pdb_id:
                    payload_json = _write_structure_domain_record(
                        out_handle,
                        source_name="CATH",
                        pdb_id=current_pdb_id,
                        rows=rows_for_pdb,
                    )
                    conn.execute(
                        "INSERT OR REPLACE INTO cath_records(pdb_id, payload_json) VALUES (?, ?)",
                        (current_pdb_id, payload_json),
                    )
                    record_count += 1
                    if limit is not None and record_count >= limit:
                        current_pdb_id = ""
                        rows_for_pdb = []
                        break
                    rows_for_pdb = []
                current_pdb_id = pdb_id
                rows_for_pdb.append(payload_row)
            if current_pdb_id and rows_for_pdb and (limit is None or record_count < limit):
                payload_json = _write_structure_domain_record(
                    out_handle,
                    source_name="CATH",
                    pdb_id=current_pdb_id,
                    rows=rows_for_pdb,
                )
                conn.execute(
                    "INSERT OR REPLACE INTO cath_records(pdb_id, payload_json) VALUES (?, ?)",
                    (current_pdb_id, payload_json),
                )
                record_count += 1
        conn.commit()
    finally:
        conn.close()

    manifest = {
        "generated_at": _utc_now(),
        "source_name": "cath",
        "domain_list_path": str(domain_list_path),
        "domain_list_mtime_epoch": domain_list_mtime,
        "boundaries_path": str(boundaries_path),
        "boundaries_mtime_epoch": boundaries_mtime,
        "names_path": str(names_path) if names_path is not None else None,
        "names_mtime_epoch": names_mtime,
        "index_path": str(index_path),
        "lookup_db_path": str(lookup_db_path),
        "record_count": record_count,
        "raw_mapping_row_count": mapping_count,
        "limit": limit,
        "status": "completed" if limit is None else "partial",
        "intended_use": [
            "local PDB-chain-to-CATH lookup",
            "offline structural fold enrichment for metadata exports",
            "reproducible chain-aware fold coverage auditing",
        ],
    }
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return SourceIndexResult(
        source_name="cath",
        index_path=index_path,
        manifest_path=manifest_path,
        record_count=record_count,
        lookup_db_path=lookup_db_path,
    )


def index_scop_domains(
    layout: StorageLayout,
    *,
    classification_path: Path,
    descriptions_path: Path | None = None,
    limit: int | None = None,
    force: bool = False,
) -> SourceIndexResult:
    """Build a local PDB-chain-to-SCOPe lookup from staged flat files."""
    if not classification_path.exists():
        raise FileNotFoundError(f"SCOP classification file not found: {classification_path}")

    output_dir = layout.source_indexes_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    index_path = output_dir / "scop_domain_index.jsonl.gz"
    lookup_db_path = output_dir / "scop_domain_index.sqlite"
    manifest_path = output_dir / "scop_domain_index_manifest.json"
    classification_mtime = classification_path.stat().st_mtime
    descriptions_mtime = descriptions_path.stat().st_mtime if descriptions_path is not None and descriptions_path.exists() else None
    existing_manifest = _load_manifest(manifest_path)
    if (
        not force
        and limit is None
        and index_path.exists()
        and lookup_db_path.exists()
        and existing_manifest.get("status") == "completed"
        and existing_manifest.get("classification_path") == str(classification_path)
        and float(existing_manifest.get("classification_mtime_epoch") or 0.0) == float(classification_mtime)
        and existing_manifest.get("descriptions_path") == (str(descriptions_path) if descriptions_path is not None else None)
        and float(existing_manifest.get("descriptions_mtime_epoch") or 0.0) == float(descriptions_mtime or 0.0)
    ):
        return SourceIndexResult(
            source_name="scop",
            index_path=index_path,
            manifest_path=manifest_path,
            record_count=int(existing_manifest.get("record_count") or 0),
            lookup_db_path=lookup_db_path,
        )

    if force:
        index_path.unlink(missing_ok=True)
        lookup_db_path.unlink(missing_ok=True)

    name_by_code = _load_scop_names(descriptions_path)
    conn = sqlite3.connect(lookup_db_path)
    record_count = 0
    mapping_count = 0
    try:
        conn.execute("DROP TABLE IF EXISTS scop_membership")
        conn.execute("DROP TABLE IF EXISTS scop_records")
        conn.execute(
            """
            CREATE TABLE scop_membership (
                pdb_id TEXT NOT NULL,
                chain_id TEXT NOT NULL,
                domain_id TEXT NOT NULL,
                domain_name TEXT NOT NULL,
                domain_instance_id TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE scop_records (
                pdb_id TEXT PRIMARY KEY,
                payload_json TEXT NOT NULL
            )
            """
        )
        batch: list[tuple[str, str, str, str, str]] = []
        with classification_path.open("r", encoding="utf-8", errors="replace") as handle:
            for raw_line in handle:
                line = raw_line.strip()
                if not line or line.startswith("#"):
                    continue
                parsed = _parse_scop_classification_line(line)
                if parsed is None:
                    continue
                domain_instance_id, pdb_id, chain_ids, domain_id = parsed
                domain_name = name_by_code.get(domain_id) or ""
                for chain_id in chain_ids:
                    batch.append((pdb_id, chain_id, domain_id, domain_name, domain_instance_id))
                    mapping_count += 1
                if len(batch) >= 5000:
                    conn.executemany(
                        """
                        INSERT INTO scop_membership (
                            pdb_id,
                            chain_id,
                            domain_id,
                            domain_name,
                            domain_instance_id
                        ) VALUES (?, ?, ?, ?, ?)
                        """,
                        batch,
                    )
                    batch.clear()
        if batch:
            conn.executemany(
                """
                INSERT INTO scop_membership (
                    pdb_id,
                    chain_id,
                    domain_id,
                    domain_name,
                    domain_instance_id
                ) VALUES (?, ?, ?, ?, ?)
                """,
                batch,
            )
        conn.execute("CREATE INDEX idx_scop_membership_pdb ON scop_membership(pdb_id)")

        with gzip.open(index_path, "wt", encoding="utf-8") as out_handle:
            current_pdb_id = ""
            rows_for_pdb: list[dict[str, Any]] = []
            for row in conn.execute(
                """
                SELECT pdb_id, chain_id, domain_id, domain_name, domain_instance_id
                FROM scop_membership
                ORDER BY pdb_id, chain_id, domain_id, domain_instance_id
                """
            ):
                pdb_id = str(row[0])
                payload_row = {
                    "chain_id": str(row[1]),
                    "domain_id": str(row[2]),
                    "domain_name": str(row[3]),
                    "domain_instance_id": str(row[4]),
                }
                if current_pdb_id and pdb_id != current_pdb_id:
                    payload_json = _write_structure_domain_record(
                        out_handle,
                        source_name="SCOP",
                        pdb_id=current_pdb_id,
                        rows=rows_for_pdb,
                    )
                    conn.execute(
                        "INSERT OR REPLACE INTO scop_records(pdb_id, payload_json) VALUES (?, ?)",
                        (current_pdb_id, payload_json),
                    )
                    record_count += 1
                    if limit is not None and record_count >= limit:
                        current_pdb_id = ""
                        rows_for_pdb = []
                        break
                    rows_for_pdb = []
                current_pdb_id = pdb_id
                rows_for_pdb.append(payload_row)
            if current_pdb_id and rows_for_pdb and (limit is None or record_count < limit):
                payload_json = _write_structure_domain_record(
                    out_handle,
                    source_name="SCOP",
                    pdb_id=current_pdb_id,
                    rows=rows_for_pdb,
                )
                conn.execute(
                    "INSERT OR REPLACE INTO scop_records(pdb_id, payload_json) VALUES (?, ?)",
                    (current_pdb_id, payload_json),
                )
                record_count += 1
        conn.commit()
    finally:
        conn.close()

    manifest = {
        "generated_at": _utc_now(),
        "source_name": "scop",
        "classification_path": str(classification_path),
        "classification_mtime_epoch": classification_mtime,
        "descriptions_path": str(descriptions_path) if descriptions_path is not None else None,
        "descriptions_mtime_epoch": descriptions_mtime,
        "index_path": str(index_path),
        "lookup_db_path": str(lookup_db_path),
        "record_count": record_count,
        "raw_mapping_row_count": mapping_count,
        "limit": limit,
        "status": "completed" if limit is None else "partial",
        "intended_use": [
            "local PDB-chain-to-SCOPe lookup",
            "offline structural fold enrichment for metadata exports",
            "reproducible chain-aware fold coverage auditing",
        ],
    }
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return SourceIndexResult(
        source_name="scop",
        index_path=index_path,
        manifest_path=manifest_path,
        record_count=record_count,
        lookup_db_path=lookup_db_path,
    )


def query_uniprot_swissprot_index(
    layout: StorageLayout,
    accession: str,
    *,
    lookup_db_path: Path | None = None,
) -> dict[str, Any] | None:
    """Return one indexed UniProt record by accession."""
    normalized = accession.strip().upper()
    if not normalized:
        return None
    db_path = lookup_db_path or (layout.source_indexes_dir / "uniprot_swissprot_index.sqlite")
    if not db_path.exists():
        return None
    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute(
            "SELECT payload_json FROM uniprot_records WHERE accession = ?",
            (normalized,),
        ).fetchone()
    finally:
        conn.close()
    if row is None:
        return None
    payload = json.loads(str(row[0]))
    return payload if isinstance(payload, dict) else None


def query_reactome_pathway_index(
    layout: StorageLayout,
    accession: str,
    *,
    lookup_db_path: Path | None = None,
) -> dict[str, Any] | None:
    """Return one indexed Reactome pathway record by UniProt accession."""
    normalized = accession.strip().upper()
    if not normalized:
        return None
    db_path = lookup_db_path or (layout.source_indexes_dir / "reactome_pathway_index.sqlite")
    if not db_path.exists():
        return None
    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute(
            "SELECT payload_json FROM reactome_records WHERE accession = ?",
            (normalized,),
        ).fetchone()
    finally:
        conn.close()
    if row is None:
        return None
    payload = json.loads(str(row[0]))
    return payload if isinstance(payload, dict) else None


def query_cath_domain_index(
    layout: StorageLayout,
    pdb_id: str,
    *,
    lookup_db_path: Path | None = None,
) -> dict[str, Any] | None:
    """Return one indexed CATH record by PDB ID."""
    return _query_structure_domain_index(
        layout,
        pdb_id,
        lookup_db_path=lookup_db_path,
        default_name="cath_domain_index.sqlite",
        table_name="cath_records",
    )


def query_scop_domain_index(
    layout: StorageLayout,
    pdb_id: str,
    *,
    lookup_db_path: Path | None = None,
) -> dict[str, Any] | None:
    """Return one indexed SCOPe record by PDB ID."""
    return _query_structure_domain_index(
        layout,
        pdb_id,
        lookup_db_path=lookup_db_path,
        default_name="scop_domain_index.sqlite",
        table_name="scop_records",
    )


def query_alphafold_archive_index(
    layout: StorageLayout,
    accession: str,
    *,
    lookup_db_path: Path | None = None,
) -> dict[str, Any] | None:
    """Return the first indexed AlphaFold archive member for an accession."""
    normalized = accession.strip().upper()
    if not normalized:
        return None
    db_path = lookup_db_path or (layout.source_indexes_dir / "alphafold_archive_index.sqlite")
    if not db_path.exists():
        return None
    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute(
            """
            SELECT accession, entry_id, member_name, size_bytes, model_version
            FROM alphafold_members
            WHERE accession = ?
            ORDER BY entry_id
            LIMIT 1
            """,
            (normalized,),
        ).fetchone()
    finally:
        conn.close()
    if row is None:
        return None
    return {
        "accession": str(row[0]),
        "entry_id": str(row[1]),
        "member_name": str(row[2]),
        "size_bytes": int(row[3]),
        "model_version": str(row[4]),
    }


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


def _load_reactome_pathway_names(pathways_path: Path | None) -> dict[str, str]:
    if pathways_path is None or not pathways_path.exists():
        return {}
    names: dict[str, str] = {}
    with pathways_path.open("r", encoding="utf-8", errors="replace") as handle:
        for raw_line in handle:
            line = raw_line.rstrip("\n")
            if not line:
                continue
            parts = line.split("\t")
            if len(parts) < 2:
                continue
            pathway_id = parts[0].strip()
            pathway_name = parts[1].strip()
            if pathway_id and pathway_name and pathway_id not in names:
                names[pathway_id] = pathway_name
    return names


def _load_cath_names(names_path: Path | None) -> dict[str, str]:
    if names_path is None or not names_path.exists():
        return {}
    names: dict[str, str] = {}
    with names_path.open("r", encoding="utf-8", errors="replace") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split()
            if len(parts) < 3:
                continue
            code = parts[0].strip()
            description = line.split(":", 1)[1].strip() if ":" in line else ""
            if code and description and code not in names:
                names[code] = description
    return names


def _load_cath_classification(domain_list_path: Path) -> dict[str, str]:
    mapping: dict[str, str] = {}
    with domain_list_path.open("r", encoding="utf-8", errors="replace") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split()
            if len(parts) < 5:
                continue
            mapping[parts[0].strip()] = ".".join(parts[1:5])
    return mapping


def _parse_cath_boundary_line(line: str) -> tuple[str, str]:
    parts = line.split()
    if len(parts) < 5:
        raise ValueError(f"Malformed CATH boundary line: {line}")
    domain_prefix = parts[0].strip()
    domain_token = parts[1].strip()
    chain_id = parts[4].strip()
    domain_number = max(int(domain_token[1:]) - 1, 0)
    domain_instance_id = f"{domain_prefix}{domain_number:02d}"
    return domain_instance_id, chain_id


def _load_scop_names(descriptions_path: Path | None) -> dict[str, str]:
    if descriptions_path is None or not descriptions_path.exists():
        return {}
    scores = {"cl": 1, "cf": 2, "sf": 3, "fa": 4, "dm": 0, "sp": 0}
    names: dict[str, tuple[int, str]] = {}
    with descriptions_path.open("r", encoding="utf-8", errors="replace") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split("\t")
            if len(parts) < 5:
                continue
            node_type = parts[1].strip()
            code = parts[2].strip()
            description = parts[4].strip()
            score = scores.get(node_type)
            if not code or not description or score is None:
                continue
            existing = names.get(code)
            if existing is None or score > existing[0]:
                names[code] = (score, description)
    return {code: description for code, (_, description) in names.items()}


def _parse_scop_classification_line(line: str) -> tuple[str, str, list[str], str] | None:
    parts = line.split("\t")
    if len(parts) < 4:
        return None
    domain_instance_id = parts[0].strip()
    pdb_id = parts[1].strip().upper()
    regions = parts[2].strip()
    domain_id = parts[3].strip()
    if not domain_instance_id or not pdb_id or not domain_id:
        return None
    chain_ids = _scop_chain_ids_from_regions(regions)
    if not chain_ids:
        chain_ids = ["-"]
    return domain_instance_id, pdb_id, chain_ids, domain_id


def _scop_chain_ids_from_regions(regions: str) -> list[str]:
    chain_ids: list[str] = []
    for region in regions.split(","):
        token = region.strip()
        if not token:
            continue
        chain_id = token.split(":", 1)[0].strip()
        if chain_id and chain_id not in chain_ids:
            chain_ids.append(chain_id)
    return chain_ids


def _write_structure_domain_record(
    handle: Any,
    *,
    source_name: str,
    pdb_id: str,
    rows: list[dict[str, Any]],
) -> str:
    domain_ids: list[str] = []
    domain_names: list[str] = []
    chain_ids: list[str] = []
    chain_to_domain_ids: dict[str, list[str]] = {}
    for row in rows:
        domain_id = str(row.get("domain_id") or "")
        domain_name = str(row.get("domain_name") or "")
        chain_id = str(row.get("chain_id") or "")
        if domain_id and domain_id not in domain_ids:
            domain_ids.append(domain_id)
        if domain_name and domain_name not in domain_names:
            domain_names.append(domain_name)
        if chain_id and chain_id not in chain_ids:
            chain_ids.append(chain_id)
        if chain_id and domain_id:
            chain_to_domain_ids.setdefault(chain_id, [])
            if domain_id not in chain_to_domain_ids[chain_id]:
                chain_to_domain_ids[chain_id].append(domain_id)
    payload = {
        "source_name": source_name,
        "pdb_id": pdb_id,
        "domain_ids": domain_ids,
        "domain_names": domain_names,
        "chain_ids": chain_ids,
        "chain_to_domain_ids": chain_to_domain_ids,
        "mapping_count": len(rows),
    }
    payload_json = json.dumps(payload, separators=(",", ":"))
    handle.write(payload_json)
    handle.write("\n")
    return payload_json


def _query_structure_domain_index(
    layout: StorageLayout,
    pdb_id: str,
    *,
    lookup_db_path: Path | None,
    default_name: str,
    table_name: str,
) -> dict[str, Any] | None:
    normalized = pdb_id.strip().upper()
    if not normalized:
        return None
    db_path = lookup_db_path or (layout.source_indexes_dir / default_name)
    if not db_path.exists():
        return None
    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute(
            f"SELECT payload_json FROM {table_name} WHERE pdb_id = ?",
            (normalized,),
        ).fetchone()
    finally:
        conn.close()
    if row is None:
        return None
    payload = json.loads(str(row[0]))
    return payload if isinstance(payload, dict) else None


def _write_reactome_record(
    handle: gzip.GzipFile,
    accession: str,
    items: list[dict[str, Any]],
    species_values: list[str],
) -> str:
    seen_pairs: set[tuple[str, str]] = set()
    pathway_ids: list[str] = []
    pathway_names: list[str] = []
    for item in items:
        key = (str(item["pathway_id"]), str(item["pathway_name"]))
        if key in seen_pairs:
            continue
        seen_pairs.add(key)
        pathway_ids.append(key[0])
        if key[1]:
            pathway_names.append(key[1])
    payload = {
        "uniprot_id": accession,
        "pathway_ids": pathway_ids,
        "pathway_names": pathway_names,
        "pathway_count": len(pathway_ids),
        "species": species_values,
    }
    payload_json = json.dumps(payload, separators=(",", ":"))
    handle.write(payload_json)
    handle.write("\n")
    return payload_json


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


def _archive_iteration_completed(archive_path: Path, next_offset: int) -> bool:
    try:
        if next_offset < 0:
            return False
        with archive_path.open("rb") as handle:
            handle.seek(next_offset)
            tail = handle.read()
        return not tail or set(tail) <= {0}
    except OSError:
        return False


def _next_tar_header_offset(start_offset: int, member: tarfile.TarInfo) -> int:
    block_count = (int(member.size) + 511) // 512
    return start_offset + int(member.offset_data) + (block_count * 512)


def _load_manifest(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()
