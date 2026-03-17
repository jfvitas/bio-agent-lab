"""Local-first BindingDB bulk index and lookup helpers.

The staged BindingDB asset is a MySQL dump rather than a directly queryable
table. This module builds a compact SQLite lookup layer keyed by PDB ID so the
extract pipeline can reuse the staged dump instead of relying on per-PDB live
requests.
"""

from __future__ import annotations

import json
import math
import re
import sqlite3
import zipfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from pbdata.schemas.canonical_sample import CanonicalBindingSample
from pbdata.source_indexes import _load_manifest
from pbdata.storage import StorageLayout
from pbdata.sources.bindingdb import _parse_affinity

_BULK_INDEX_VERSION = "0.2.0"
_RELEVANT_TABLES = frozenset({
    "cobweb_bdb",
    "enzyme_reactant_set",
    "entry",
    "monomer",
    "pdb_bdb",
    "polymer",
})
_MUTATION_RE = re.compile(r"\b[A-Z]\d+[A-Z]\b")


@dataclass(frozen=True)
class BindingDBBulkIndexResult:
    index_path: Path
    manifest_path: Path
    record_count: int
    pdb_count: int


def build_bindingdb_bulk_index(
    layout: StorageLayout,
    *,
    dump_zip_path: Path,
    force: bool = False,
) -> BindingDBBulkIndexResult:
    """Build a reusable SQLite lookup keyed by PDB ID from the staged SQL dump."""
    if not dump_zip_path.exists():
        raise FileNotFoundError(f"BindingDB bulk dump not found: {dump_zip_path}")

    output_dir = layout.source_indexes_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    index_path = output_dir / "bindingdb_bulk_index.sqlite"
    manifest_path = output_dir / "bindingdb_bulk_index_manifest.json"
    source_mtime = dump_zip_path.stat().st_mtime
    existing_manifest = _load_manifest(manifest_path)
    if (
        not force
        and index_path.exists()
        and existing_manifest.get("status") == "completed"
        and existing_manifest.get("dump_zip_path") == str(dump_zip_path)
        and float(existing_manifest.get("dump_zip_mtime_epoch") or 0.0) == float(source_mtime)
    ):
        return BindingDBBulkIndexResult(
            index_path=index_path,
            manifest_path=manifest_path,
            record_count=int(existing_manifest.get("record_count") or 0),
            pdb_count=int(existing_manifest.get("pdb_count") or 0),
        )

    if force:
        index_path.unlink(missing_ok=True)

    conn = sqlite3.connect(index_path)
    try:
        _initialize_work_tables(conn)
        table_counts = {name: 0 for name in _RELEVANT_TABLES}
        batch_size = 1000
        batches: dict[str, list[tuple[Any, ...]]] = {name: [] for name in _RELEVANT_TABLES}

        with zipfile.ZipFile(dump_zip_path) as archive:
            dump_member = _resolve_dump_member_name(archive)
            with archive.open(dump_member) as handle:
                for raw_line in handle:
                    line = raw_line.decode("utf-8", errors="replace").rstrip("\n")
                    if not line.startswith("INSERT INTO `"):
                        continue
                    table_name = line.split("`", 2)[1]
                    if table_name not in _RELEVANT_TABLES:
                        continue
                    values_blob = line.split(" VALUES ", 1)[1].rstrip(";")
                    for row in _iter_mysql_values(values_blob):
                        normalized_rows = _normalize_dump_row(table_name, row)
                        for normalized in normalized_rows:
                            batches[table_name].append(normalized)
                            table_counts[table_name] += 1
                            if len(batches[table_name]) >= batch_size:
                                _flush_table_batch(conn, table_name, batches[table_name])
                                batches[table_name].clear()

        for table_name, rows in batches.items():
            if rows:
                _flush_table_batch(conn, table_name, rows)
        conn.commit()

        _create_join_indexes(conn)
        _materialize_final_binding_rows(conn)
        record_count = int(conn.execute("SELECT COUNT(*) FROM bindingdb_bulk_rows").fetchone()[0])
        pdb_count = int(conn.execute("SELECT COUNT(DISTINCT pdb_id) FROM bindingdb_bulk_rows").fetchone()[0])
        conn.commit()

        manifest = {
            "generated_at": _utc_now(),
            "status": "completed",
            "index_version": _BULK_INDEX_VERSION,
            "dump_zip_path": str(dump_zip_path),
            "dump_zip_mtime_epoch": source_mtime,
            "index_path": str(index_path),
            "record_count": record_count,
            "pdb_count": pdb_count,
            "loaded_table_counts": table_counts,
            "row_schema": [
                "pdb_id",
                "reactant_set_id",
                "assay_type",
                "affinity_value_nM",
                "affinity_display",
                "target_name",
                "source_organism",
                "ligand_id",
                "ligand_name",
                "ligand_smiles",
                "ligand_inchi_key",
                "target_uniprot_ids",
                "target_display_name",
                "entry_title",
                "entry_comments",
                "entry_date",
                "measurement_technique",
            ],
            "assumptions": [
                "BindingDB cobweb_bdb affinity values are treated as nM-scale measurements for local extract-time enrichment.",
                "Chain-level receptor mapping remains unresolved inside the bulk dump and is recovered from the current RCSB raw entry when UniProt IDs match.",
                "Mutation context is advisory unless the target name explicitly contains amino-acid substitution tokens.",
            ],
            "intended_use": [
                "local extract-time BindingDB assay enrichment",
                "offline content inspection and field-population auditing",
                "reproducible local-first assay reuse without per-PDB live requests",
            ],
        }
        manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
        return BindingDBBulkIndexResult(
            index_path=index_path,
            manifest_path=manifest_path,
            record_count=record_count,
            pdb_count=pdb_count,
        )
    finally:
        conn.close()


def fetch_bindingdb_bulk_samples(
    layout: StorageLayout,
    pdb_id: str,
    *,
    index_path: Path | None = None,
) -> list[CanonicalBindingSample]:
    """Return normalized BindingDB samples for one PDB ID from the local bulk index."""
    normalized_pdb_id = str(pdb_id or "").strip().upper()
    if not normalized_pdb_id:
        return []
    resolved_index_path = index_path or (layout.source_indexes_dir / "bindingdb_bulk_index.sqlite")
    if not resolved_index_path.exists():
        return []

    conn = sqlite3.connect(resolved_index_path)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT *
            FROM bindingdb_bulk_rows
            WHERE pdb_id = ?
            ORDER BY assay_type, ligand_id, reactant_set_id
            """,
            (normalized_pdb_id,),
        ).fetchall()
    finally:
        conn.close()
    return [_bindingdb_bulk_row_to_sample(dict(row)) for row in rows]


def _initialize_work_tables(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        DROP TABLE IF EXISTS pdb_bdb_map;
        DROP TABLE IF EXISTS cobweb_bdb_rows;
        DROP TABLE IF EXISTS monomer_rows;
        DROP TABLE IF EXISTS enzyme_reactant_set_rows;
        DROP TABLE IF EXISTS polymer_rows;
        DROP TABLE IF EXISTS entry_rows;
        DROP TABLE IF EXISTS bindingdb_bulk_rows;

        CREATE TABLE pdb_bdb_map (
            pdb_id TEXT NOT NULL,
            reactant_set_id INTEGER NOT NULL,
            PRIMARY KEY (pdb_id, reactant_set_id)
        );

        CREATE TABLE cobweb_bdb_rows (
            reactant_set_id INTEGER NOT NULL,
            monomer_id INTEGER NOT NULL,
            target_name TEXT,
            inhibitor_name TEXT,
            assay_type TEXT,
            affinity_value REAL,
            affinity_display TEXT,
            affinity_strength INTEGER,
            source_organism TEXT,
            PRIMARY KEY (reactant_set_id, monomer_id, assay_type, source_organism)
        );

        CREATE TABLE monomer_rows (
            monomer_id INTEGER PRIMARY KEY,
            ligand_id TEXT,
            ligand_name TEXT,
            ligand_smiles TEXT,
            ligand_inchi_key TEXT,
            ligand_type TEXT
        );

        CREATE TABLE enzyme_reactant_set_rows (
            reactant_set_id INTEGER PRIMARY KEY,
            entry_id INTEGER,
            enzyme_polymer_id INTEGER,
            inhibitor_monomer_id INTEGER,
            inhibitor_name TEXT,
            category TEXT
        );

        CREATE TABLE polymer_rows (
            polymer_id INTEGER PRIMARY KEY,
            display_name TEXT,
            source_organism TEXT,
            scientific_name TEXT,
            unpid1 TEXT,
            unpid2 TEXT
        );

        CREATE TABLE entry_rows (
            entry_id INTEGER PRIMARY KEY,
            entry_title TEXT,
            entry_comments TEXT,
            entry_date TEXT,
            measurement_technique TEXT,
            ezid TEXT
        );

        """
    )


def _flush_table_batch(
    conn: sqlite3.Connection,
    table_name: str,
    rows: list[tuple[Any, ...]],
) -> None:
    if table_name == "pdb_bdb":
        conn.executemany(
            "INSERT OR IGNORE INTO pdb_bdb_map(pdb_id, reactant_set_id) VALUES (?, ?)",
            rows,
        )
    elif table_name == "cobweb_bdb":
        conn.executemany(
            """
            INSERT OR REPLACE INTO cobweb_bdb_rows(
                target_name,
                inhibitor_name,
                monomer_id,
                assay_type,
                affinity_value,
                affinity_display,
                affinity_strength,
                reactant_set_id,
                source_organism
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
    elif table_name == "monomer":
        conn.executemany(
            """
            INSERT OR REPLACE INTO monomer_rows(
                monomer_id,
                ligand_id,
                ligand_name,
                ligand_smiles,
                ligand_inchi_key,
                ligand_type
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
    elif table_name == "enzyme_reactant_set":
        conn.executemany(
            """
            INSERT OR REPLACE INTO enzyme_reactant_set_rows(
                reactant_set_id,
                entry_id,
                enzyme_polymer_id,
                inhibitor_monomer_id,
                inhibitor_name,
                category
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
    elif table_name == "polymer":
        conn.executemany(
            """
            INSERT OR REPLACE INTO polymer_rows(
                polymer_id,
                display_name,
                source_organism,
                scientific_name,
                unpid1,
                unpid2
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
    elif table_name == "entry":
        conn.executemany(
            """
            INSERT OR REPLACE INTO entry_rows(
                entry_id,
                entry_title,
                entry_comments,
                entry_date,
                measurement_technique,
                ezid
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
    else:
        raise KeyError(f"Unsupported BindingDB bulk table flush: {table_name}")


def _create_join_indexes(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE INDEX IF NOT EXISTS idx_pdb_bdb_reactant_set ON pdb_bdb_map(reactant_set_id);
        CREATE INDEX IF NOT EXISTS idx_cobweb_reactant_set ON cobweb_bdb_rows(reactant_set_id);
        CREATE INDEX IF NOT EXISTS idx_monomer_rows ON monomer_rows(monomer_id);
        CREATE INDEX IF NOT EXISTS idx_enzyme_reactant_entry ON enzyme_reactant_set_rows(entry_id);
        CREATE INDEX IF NOT EXISTS idx_enzyme_reactant_polymer ON enzyme_reactant_set_rows(enzyme_polymer_id);
        """
    )


def _materialize_final_binding_rows(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE bindingdb_bulk_rows AS
        SELECT
            pdb.pdb_id AS pdb_id,
            cobweb.reactant_set_id AS reactant_set_id,
            cobweb.monomer_id AS monomer_id,
            cobweb.assay_type AS assay_type,
            cobweb.affinity_value AS affinity_value_nM,
            cobweb.affinity_display AS affinity_display,
            cobweb.affinity_strength AS affinity_strength,
            cobweb.target_name AS target_name,
            cobweb.source_organism AS source_organism,
            monomer.ligand_id AS ligand_id,
            monomer.ligand_name AS ligand_name,
            monomer.ligand_smiles AS ligand_smiles,
            monomer.ligand_inchi_key AS ligand_inchi_key,
            monomer.ligand_type AS ligand_type,
            reactant.entry_id AS entry_id,
            reactant.enzyme_polymer_id AS enzyme_polymer_id,
            reactant.inhibitor_monomer_id AS inhibitor_monomer_id,
            reactant.inhibitor_name AS inhibitor_name,
            reactant.category AS reactant_category,
            polymer.display_name AS target_display_name,
            polymer.source_organism AS target_polymer_source_organism,
            polymer.scientific_name AS target_scientific_name,
            polymer.unpid1 AS target_unpid1,
            polymer.unpid2 AS target_unpid2,
            entry.entry_title AS entry_title,
            entry.entry_comments AS entry_comments,
            entry.entry_date AS entry_date,
            entry.measurement_technique AS measurement_technique,
            entry.ezid AS entry_ezid
        FROM pdb_bdb_map AS pdb
        INNER JOIN cobweb_bdb_rows AS cobweb
            ON cobweb.reactant_set_id = pdb.reactant_set_id
        LEFT JOIN monomer_rows AS monomer
            ON monomer.monomer_id = cobweb.monomer_id
        LEFT JOIN enzyme_reactant_set_rows AS reactant
            ON reactant.reactant_set_id = cobweb.reactant_set_id
        LEFT JOIN polymer_rows AS polymer
            ON polymer.polymer_id = reactant.enzyme_polymer_id
        LEFT JOIN entry_rows AS entry
            ON entry.entry_id = reactant.entry_id
        ;

        CREATE INDEX idx_bindingdb_bulk_rows_pdb_id ON bindingdb_bulk_rows(pdb_id);
        CREATE INDEX idx_bindingdb_bulk_rows_reactant_set_id ON bindingdb_bulk_rows(reactant_set_id);
        """
    )


def _normalize_dump_row(table_name: str, row: list[Any]) -> list[tuple[Any, ...]]:
    if table_name == "pdb_bdb":
        pdb_id = str(row[0] or "").strip().upper()
        reactant_ids = set(_split_int_tokens(row[1]))
        reactant_ids.update(_split_int_tokens(row[2]))
        return [(pdb_id, reactant_id) for reactant_id in sorted(reactant_ids) if pdb_id]
    if table_name == "cobweb_bdb":
        return [(
            _clean_text(row[0]),
            _clean_text(row[1]),
            _safe_int(row[2]),
            _clean_text(row[3]),
            _safe_float(row[4]),
            _clean_text(row[5]),
            _safe_int(row[6]),
            _safe_int(row[7]),
            _clean_text(row[8]),
        )]
    if table_name == "monomer":
        monomer_id = _safe_int(row[9])
        return [(
            monomer_id,
            _clean_text(row[4]) or f"BDBM{monomer_id}",
            _clean_text(row[5]) or f"BDBM{monomer_id}",
            _clean_text(row[14]),
            _clean_text(row[6]),
            _clean_text(row[12]),
        )]
    if table_name == "enzyme_reactant_set":
        return [(
            _safe_int(row[3]),
            _safe_int(row[6]),
            _safe_int(row[16]),
            _safe_int(row[12]),
            _clean_text(row[10]),
            _clean_text(row[17]),
        )]
    if table_name == "polymer":
        return [(
            _safe_int(row[14]),
            _clean_text(row[8]),
            _clean_text(row[4]),
            _clean_text(row[6]),
            _clean_text(row[13]),
            _clean_text(row[5]),
        )]
    if table_name == "entry":
        return [(
            _safe_int(row[6]),
            _clean_text(row[3]),
            _clean_text(row[1]),
            _clean_text(row[2]),
            _clean_text(row[7]),
            _clean_text(row[9]),
        )]
    raise KeyError(f"Unsupported BindingDB dump table: {table_name}")


def _resolve_dump_member_name(archive: zipfile.ZipFile) -> str:
    candidates = [info.filename for info in archive.infolist() if info.filename.endswith(".dmp")]
    if not candidates:
        raise FileNotFoundError("BindingDB dump zip does not contain a .dmp payload.")
    return sorted(candidates)[0]


def _iter_mysql_values(values_blob: str) -> Iterable[list[Any]]:
    index = 0
    length = len(values_blob)
    while index < length:
        if values_blob[index] != "(":
            index += 1
            continue
        index += 1
        row: list[Any] = []
        field_buffer: list[str] = []
        in_string = False
        escaped = False
        field_is_string = False
        while index < length:
            ch = values_blob[index]
            if in_string:
                if escaped:
                    field_buffer.append(_decode_mysql_escape(ch))
                    escaped = False
                elif ch == "\\":
                    escaped = True
                elif ch == "'":
                    in_string = False
                else:
                    field_buffer.append(ch)
            else:
                if ch == "'":
                    in_string = True
                    field_is_string = True
                elif ch == ",":
                    row.append(_coerce_mysql_value("".join(field_buffer), field_is_string=field_is_string))
                    field_buffer = []
                    field_is_string = False
                elif ch == ")":
                    row.append(_coerce_mysql_value("".join(field_buffer), field_is_string=field_is_string))
                    yield row
                    break
                else:
                    field_buffer.append(ch)
            index += 1
        index += 1


def _coerce_mysql_value(raw: str, *, field_is_string: bool) -> Any:
    text = raw if field_is_string else raw.strip()
    if not field_is_string and text.upper() == "NULL":
        return None
    return text


def _decode_mysql_escape(ch: str) -> str:
    return {
        "0": "\0",
        "b": "\b",
        "n": "\n",
        "r": "\r",
        "t": "\t",
        "Z": "\x1a",
        "'": "'",
        '"': '"',
        "\\": "\\",
    }.get(ch, ch)


def _bindingdb_bulk_row_to_sample(row: dict[str, Any]) -> CanonicalBindingSample:
    affinity_display = str(row.get("affinity_display") or row.get("affinity_value_nM") or "").strip()
    assay_value, _, standardized_nM, relation = _parse_affinity(affinity_display, "nM")
    if assay_value is None:
        try:
            assay_value = float(row.get("affinity_value_nM"))
        except (TypeError, ValueError):
            assay_value = None
    if standardized_nM is None:
        standardized_nM = assay_value
    assay_value_log10 = round(math.log10(standardized_nM), 6) if standardized_nM not in (None, 0) and standardized_nM > 0 else None

    target_name = str(row.get("target_name") or row.get("target_display_name") or "").strip()
    mutation_string = _mutation_from_target_name(target_name)
    uniprot_ids = _joined_uniprot_ids(row)
    ligand_id = str(row.get("ligand_id") or f"BDBM{row.get('monomer_id')}").strip()
    source_record_id = f"reactant_set:{row.get('reactant_set_id')}"
    sample_id = f"BDB_BULK_{row.get('pdb_id')}_{row.get('reactant_set_id')}_{row.get('monomer_id')}_{row.get('assay_type')}"

    provenance = {
        "source_database": "BindingDB",
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "adapter_version": _BULK_INDEX_VERSION,
        "source_mode": "bulk_index",
        "target_name": target_name or None,
        "reference_text": str(row.get("entry_comments") or row.get("entry_title") or "").strip() or None,
        "entry_title": str(row.get("entry_title") or "").strip() or None,
        "entry_id": row.get("entry_id"),
        "reactant_set_id": row.get("reactant_set_id"),
        "measurement_technique": str(row.get("measurement_technique") or "").strip() or None,
        "bindingdb_ezid": str(row.get("entry_ezid") or "").strip() or None,
        "ligand_name": str(row.get("ligand_name") or "").strip() or None,
        "source_organism": str(row.get("source_organism") or row.get("target_scientific_name") or "").strip() or None,
        "raw_affinity_text": affinity_display or None,
        "standard_relation": relation,
        "standardized_affinity_unit": "nM" if standardized_nM is not None else None,
    }

    return CanonicalBindingSample(
        sample_id=sample_id,
        task_type="protein_ligand",
        source_database="BindingDB",
        source_record_id=source_record_id,
        pdb_id=str(row.get("pdb_id") or "").strip().upper() or None,
        uniprot_ids=uniprot_ids or None,
        ligand_id=ligand_id or None,
        ligand_smiles=str(row.get("ligand_smiles") or "").strip() or None,
        ligand_inchi_key=str(row.get("ligand_inchi_key") or "").strip() or None,
        title=str(row.get("entry_title") or "").strip() or None,
        experimental_method=str(row.get("measurement_technique") or "").strip() or None,
        release_date=str(row.get("entry_date") or "").strip() or None,
        assay_type=str(row.get("assay_type") or "").strip() or None,
        assay_value=assay_value,
        assay_unit="nM" if assay_value is not None else None,
        assay_value_standardized=standardized_nM,
        assay_value_log10=assay_value_log10,
        mutation_string=mutation_string,
        wildtype_or_mutant="mutant" if mutation_string else "wildtype",
        provenance=provenance,
        quality_flags=[],
        quality_score=0.0,
    )


def _mutation_from_target_name(target_name: str) -> str | None:
    if not target_name:
        return None
    matches = sorted(set(_MUTATION_RE.findall(target_name)))
    return ",".join(matches) if matches else None


def _joined_uniprot_ids(row: dict[str, Any]) -> list[str]:
    values: list[str] = []
    for key in ("target_unpid1", "target_unpid2"):
        token = str(row.get(key) or "").strip()
        if not token or token.lower() == "null":
            continue
        for value in token.replace(";", ",").split(","):
            normalized = value.strip()
            if normalized and normalized.lower() != "null" and normalized not in values:
                values.append(normalized)
    return values


def _split_int_tokens(raw: Any) -> list[int]:
    values: list[int] = []
    for token in str(raw or "").replace(";", ",").split(","):
        normalized = token.strip()
        if not normalized or normalized.lower() == "null":
            continue
        try:
            values.append(int(normalized))
        except ValueError:
            continue
    return values


def _clean_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.lower() == "null":
        return None
    return text


def _safe_int(value: Any) -> int | None:
    try:
        return int(str(value).strip()) if value not in (None, "") else None
    except (TypeError, ValueError):
        return None


def _safe_float(value: Any) -> float | None:
    try:
        return float(str(value).strip()) if value not in (None, "") else None
    except (TypeError, ValueError):
        return None


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()
