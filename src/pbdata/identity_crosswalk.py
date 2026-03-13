"""Conservative identity crosswalk exports for proteins, ligands, and pairs.

Assumptions:
- Protein identity prefers exact UniProt accessions when present in extracted
  chain rows. Otherwise it falls back to a PDB-chain scoped identifier.
- Ligand identity prefers InChIKey, then component ID, then a scoped fallback.
- Pair identity remains anchored to the existing pair_identity_key. The
  crosswalk explains how the protein and ligand sides map; it does not replace
  the canonical pair key.
- Ambiguity is preserved explicitly. Missing or fallback mappings are reported
  instead of silently collapsed.
"""

from __future__ import annotations

import csv
import json
import logging
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pbdata.pairing import bound_object_matches_ligand_key, parse_pair_identity_key
from pbdata.storage import StorageLayout
from pbdata.table_io import load_table_json

logger = logging.getLogger(__name__)

def _write_csv(path: Path, rows: list[dict[str, Any]], columns: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({column: _stringify(row.get(column)) for column in columns})


def _stringify(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, list):
        return "; ".join(str(item) for item in value if item not in (None, ""))
    return str(value)


def _protein_identity_row(chain: dict[str, Any]) -> dict[str, Any]:
    pdb_id = str(chain.get("pdb_id") or "").strip()
    chain_id = str(chain.get("chain_id") or "").strip()
    uniprot_id = str(chain.get("uniprot_id") or "").strip()
    if uniprot_id:
        canonical_id = f"protein:{uniprot_id}"
        status = "exact_uniprot"
        notes = "Exact UniProt-centered protein identity."
    else:
        canonical_id = f"protein:{pdb_id}:{chain_id}"
        status = "pdb_chain_fallback"
        notes = "Fallback protein identity because no UniProt accession was extracted."
    return {
        "canonical_protein_id": canonical_id,
        "pdb_id": pdb_id,
        "chain_id": chain_id,
        "uniprot_id": uniprot_id,
        "organism": str(chain.get("entity_source_organism") or "").strip(),
        "gene": str(chain.get("chain_description") or "").strip(),
        "mapping_status": status,
        "notes": notes,
    }


def _ligand_identity_row(bound_object: dict[str, Any]) -> dict[str, Any]:
    pdb_id = str(bound_object.get("pdb_id") or "").strip()
    component_id = str(bound_object.get("component_id") or "").strip()
    inchikey = str(bound_object.get("component_inchikey") or "").strip()
    component_name = str(bound_object.get("component_name") or "").strip()
    if inchikey:
        canonical_id = f"ligand:{inchikey}"
        status = "exact_inchikey"
        notes = "Exact ligand identity anchored to InChIKey."
    elif component_id:
        canonical_id = f"ligand:{component_id}"
        status = "component_fallback"
        notes = "Fallback ligand identity anchored to component ID because InChIKey is missing."
    else:
        label = component_name or "unknown"
        canonical_id = f"ligand:{pdb_id}:{label}"
        status = "scoped_name_fallback"
        notes = "Scoped ligand fallback because neither InChIKey nor component ID was available."
    return {
        "canonical_ligand_id": canonical_id,
        "pdb_id": pdb_id,
        "component_id": component_id,
        "component_name": component_name,
        "inchikey": inchikey,
        "smiles": str(bound_object.get("component_smiles") or "").strip(),
        "component_type": str(bound_object.get("component_type") or "").strip(),
        "mapping_status": status,
        "notes": notes,
    }


def build_identity_crosswalk_report(layout: StorageLayout) -> dict[str, Any]:
    chains = [row for row in load_table_json(layout.extracted_dir / "chains", logger=logger, warning_prefix="Skipping unreadable identity input") if row.get("is_protein")]
    bound_objects = load_table_json(layout.extracted_dir / "bound_objects", logger=logger, warning_prefix="Skipping unreadable identity input")
    assays = load_table_json(layout.extracted_dir / "assays", logger=logger, warning_prefix="Skipping unreadable identity input")

    protein_rows_raw = [_protein_identity_row(chain) for chain in chains]
    ligand_rows_raw = [_ligand_identity_row(bound) for bound in bound_objects]

    protein_by_chain: dict[tuple[str, str], dict[str, Any]] = {}
    for row in protein_rows_raw:
        protein_by_chain[(str(row["pdb_id"]), str(row["chain_id"]))] = row

    ligand_by_pdb: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in ligand_rows_raw:
        ligand_by_pdb[str(row["pdb_id"])].append(row)

    protein_groups: dict[str, dict[str, Any]] = {}
    for row in protein_rows_raw:
        canonical_id = str(row["canonical_protein_id"])
        group = protein_groups.setdefault(
            canonical_id,
            {
                "canonical_protein_id": canonical_id,
                "uniprot_id": str(row["uniprot_id"] or ""),
                "mapping_status": str(row["mapping_status"]),
                "pdb_ids": [],
                "chain_refs": [],
                "organisms": [],
                "genes": [],
                "notes": str(row["notes"]),
            },
        )
        if row["pdb_id"] and row["pdb_id"] not in group["pdb_ids"]:
            group["pdb_ids"].append(row["pdb_id"])
        chain_ref = f"{row['pdb_id']}:{row['chain_id']}"
        if row["pdb_id"] and row["chain_id"] and chain_ref not in group["chain_refs"]:
            group["chain_refs"].append(chain_ref)
        if row["organism"] and row["organism"] not in group["organisms"]:
            group["organisms"].append(row["organism"])
        if row["gene"] and row["gene"] not in group["genes"]:
            group["genes"].append(row["gene"])

    ligand_groups: dict[str, dict[str, Any]] = {}
    for row in ligand_rows_raw:
        canonical_id = str(row["canonical_ligand_id"])
        group = ligand_groups.setdefault(
            canonical_id,
            {
                "canonical_ligand_id": canonical_id,
                "inchikey": str(row["inchikey"] or ""),
                "mapping_status": str(row["mapping_status"]),
                "component_ids": [],
                "component_names": [],
                "smiles_values": [],
                "component_types": [],
                "pdb_ids": [],
                "notes": str(row["notes"]),
            },
        )
        for key, field in (
            ("component_id", "component_ids"),
            ("component_name", "component_names"),
            ("smiles", "smiles_values"),
            ("component_type", "component_types"),
            ("pdb_id", "pdb_ids"),
        ):
            value = str(row.get(key) or "")
            if value and value not in group[field]:
                group[field].append(value)

    pair_rows: list[dict[str, Any]] = []
    seen_pairs: set[str] = set()
    for assay in assays:
        pair_key = str(assay.get("pair_identity_key") or "").strip()
        pdb_id = str(assay.get("pdb_id") or "").strip()
        if not pair_key or pair_key in seen_pairs:
            continue
        seen_pairs.add(pair_key)

        parsed = parse_pair_identity_key(pair_key)
        protein_ids: list[str] = []
        protein_mapping_statuses: list[str] = []
        if parsed is not None:
            for chain_id in parsed.receptor_chain_ids:
                protein_row = protein_by_chain.get((pdb_id, chain_id))
                if protein_row:
                    protein_ids.append(str(protein_row["canonical_protein_id"]))
                    protein_mapping_statuses.append(str(protein_row["mapping_status"]))
                else:
                    protein_ids.append(f"protein:{pdb_id}:{chain_id}")
                    protein_mapping_statuses.append("missing_chain_mapping")

        ligand_id = ""
        ligand_status = "missing_ligand_mapping"
        if parsed is not None and parsed.ligand_key:
            for ligand_row in ligand_by_pdb.get(pdb_id, []):
                match_row = {
                    "pdb_id": pdb_id,
                    "component_id": next(iter(ligand_groups[str(ligand_row['canonical_ligand_id'])]["component_ids"]), ""),
                    "component_inchikey": ligand_row.get("inchikey"),
                    "component_name": next(iter(ligand_groups[str(ligand_row['canonical_ligand_id'])]["component_names"]), ""),
                }
                if bound_object_matches_ligand_key(match_row, parsed.ligand_key):
                    ligand_id = str(ligand_row["canonical_ligand_id"])
                    ligand_status = str(ligand_row["mapping_status"])
                    break
        if not ligand_id and parsed is not None and parsed.ligand_key:
            ligand_id = f"ligand:{parsed.ligand_key}"
            ligand_status = "pair_key_fallback"

        if protein_mapping_statuses and all(status == "exact_uniprot" for status in protein_mapping_statuses):
            protein_mapping = "exact"
        elif protein_mapping_statuses:
            protein_mapping = "partial"
        else:
            protein_mapping = "unresolved"
        pair_status = (
            "exact"
            if protein_mapping == "exact" and ligand_status == "exact_inchikey"
            else "partial"
            if protein_ids or ligand_id
            else "unresolved"
        )
        pair_rows.append(
            {
                "pair_identity_key": pair_key,
                "pdb_id": pdb_id,
                "canonical_protein_ids": protein_ids,
                "canonical_ligand_id": ligand_id,
                "protein_mapping_status": protein_mapping,
                "ligand_mapping_status": ligand_status,
                "binding_affinity_types": sorted(
                    {
                        str(assay.get("binding_affinity_type") or "").strip()
                        for assay in assays
                        if str(assay.get("pair_identity_key") or "").strip() == pair_key
                        and str(assay.get("binding_affinity_type") or "").strip()
                    }
                ),
                "source_databases": sorted(
                    {
                        str(assay.get("source_database") or "").strip()
                        for assay in assays
                        if str(assay.get("pair_identity_key") or "").strip() == pair_key
                        and str(assay.get("source_database") or "").strip()
                    }
                ),
                "mapping_status": pair_status,
                "notes": (
                    "Exact pair-side mapping requires exact UniProt protein identity and exact InChIKey ligand identity. "
                    "Otherwise the crosswalk stays partial or fallback."
                ),
            }
        )

    protein_rows = sorted(protein_groups.values(), key=lambda row: str(row["canonical_protein_id"]))
    ligand_rows = sorted(ligand_groups.values(), key=lambda row: str(row["canonical_ligand_id"]))
    pair_rows = sorted(pair_rows, key=lambda row: str(row["pair_identity_key"]))

    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "status": "ready" if protein_rows or ligand_rows or pair_rows else "empty",
        "summary": (
            f"{len(protein_rows):,} protein identities, {len(ligand_rows):,} ligand identities, "
            f"{len(pair_rows):,} pair identities"
        ),
        "next_action": (
            "Inspect fallback and unresolved mappings before using the crosswalk as a benchmark split or deduplication input."
            if pair_rows
            else "Run Extract first so identity-bearing tables exist."
        ),
        "counts": {
            "protein_identity_count": len(protein_rows),
            "ligand_identity_count": len(ligand_rows),
            "pair_identity_count": len(pair_rows),
            "protein_fallback_count": sum(1 for row in protein_rows if row["mapping_status"] != "exact_uniprot"),
            "ligand_fallback_count": sum(1 for row in ligand_rows if row["mapping_status"] != "exact_inchikey"),
            "pair_exact_count": sum(1 for row in pair_rows if row["mapping_status"] == "exact"),
            "pair_ligand_fallback_count": sum(1 for row in pair_rows if row["ligand_mapping_status"] != "exact_inchikey"),
            "pair_protein_partial_count": sum(1 for row in pair_rows if row["protein_mapping_status"] != "exact"),
            "pair_partial_or_unresolved_count": sum(1 for row in pair_rows if row["mapping_status"] != "exact"),
        },
        "proteins": protein_rows,
        "ligands": ligand_rows,
        "pairs": pair_rows,
    }
    return summary


def export_identity_crosswalk(layout: StorageLayout) -> tuple[Path, Path, Path, Path, dict[str, Any]]:
    report = build_identity_crosswalk_report(layout)
    layout.identity_dir.mkdir(parents=True, exist_ok=True)
    proteins_csv = layout.identity_dir / "protein_crosswalk.csv"
    ligands_csv = layout.identity_dir / "ligand_crosswalk.csv"
    pairs_csv = layout.identity_dir / "pair_crosswalk.csv"
    summary_json = layout.identity_dir / "identity_crosswalk_summary.json"

    _write_csv(
        proteins_csv,
        list(report["proteins"]),
        ["canonical_protein_id", "uniprot_id", "mapping_status", "pdb_ids", "chain_refs", "organisms", "genes", "notes"],
    )
    _write_csv(
        ligands_csv,
        list(report["ligands"]),
        ["canonical_ligand_id", "inchikey", "mapping_status", "component_ids", "component_names", "smiles_values", "component_types", "pdb_ids", "notes"],
    )
    _write_csv(
        pairs_csv,
        list(report["pairs"]),
        [
            "pair_identity_key",
            "pdb_id",
            "canonical_protein_ids",
            "canonical_ligand_id",
            "protein_mapping_status",
            "ligand_mapping_status",
            "binding_affinity_types",
            "source_databases",
            "mapping_status",
            "notes",
        ],
    )
    summary_json.write_text(json.dumps(report, indent=2), encoding="utf-8")
    return proteins_csv, ligands_csv, pairs_csv, summary_json, report
