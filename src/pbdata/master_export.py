"""Root-level master CSV export for human review.

This export is intentionally entry-centric: one row per PDB entry with
high-value extracted fields plus aggregated chain, ligand, interface, assay,
graph, and training-example summaries. It is designed for inspection in Excel
or other spreadsheet tools without introducing a new dependency.
"""

from __future__ import annotations

import csv
import json
import logging
from collections import defaultdict
from pathlib import Path
from typing import Any

from pbdata.pairing import parse_pair_identity_key
from pbdata.storage import StorageLayout
from pbdata.table_io import load_json_rows, load_table_json

_MASTER_CSV_NAME = "master_pdb_repository.csv"
_PAIR_MASTER_CSV_NAME = "master_pdb_pairs.csv"
_ISSUE_CSV_NAME = "master_pdb_issues.csv"
_CONFLICT_CSV_NAME = "master_pdb_conflicts.csv"
_SOURCE_STATE_CSV_NAME = "master_source_state.csv"
_CRITICAL_ASSAY_CONFIDENCE_FIELDS = frozenset({
    "binding_affinity_log10_standardized",
    "binding_affinity_type",
})

logger = logging.getLogger(__name__)


def master_csv_path(repo_root: Path | None = None) -> Path:
    root = repo_root or Path.cwd()
    return root / _MASTER_CSV_NAME


def pair_master_csv_path(repo_root: Path | None = None) -> Path:
    root = repo_root or Path.cwd()
    return root / _PAIR_MASTER_CSV_NAME


def issue_csv_path(repo_root: Path | None = None) -> Path:
    root = repo_root or Path.cwd()
    return root / _ISSUE_CSV_NAME


def conflict_csv_path(repo_root: Path | None = None) -> Path:
    root = repo_root or Path.cwd()
    return root / _CONFLICT_CSV_NAME


def source_state_csv_path(repo_root: Path | None = None) -> Path:
    root = repo_root or Path.cwd()
    return root / _SOURCE_STATE_CSV_NAME

def _parse_json_object(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    if not raw:
        return {}
    try:
        parsed = json.loads(str(raw))
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _load_csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _stringify(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, list):
        return "; ".join(str(v) for v in value if v not in (None, ""))
    if isinstance(value, dict):
        return json.dumps(value, sort_keys=True)
    return str(value)


def _safe_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _split_assignment_map(layout: StorageLayout) -> dict[tuple[str, str, str], str]:
    split_dir = layout.splits_dir
    if not split_dir.exists():
        return {}

    training_examples = load_json_rows(
        layout.training_dir / "training_examples.json",
        logger=logger,
        warning_prefix="Skipping unreadable split-assignment input",
    )
    example_lookup: dict[str, tuple[str, str, str]] = {}
    for row in training_examples:
        if not isinstance(row, dict):
            continue
        example_id = str(row.get("example_id") or "").strip()
        provenance = row.get("provenance") or {}
        labels = row.get("labels") or {}
        pair_key = str(provenance.get("pair_identity_key") or "").strip()
        affinity_type = str(labels.get("affinity_type") or "").strip()
        pdb_id = str((row.get("structure") or {}).get("pdb_id") or "").strip()
        if example_id and pair_key:
            example_lookup[example_id] = (pdb_id, pair_key, affinity_type)

    split_assignment: dict[tuple[str, str, str], str] = {}
    for split_name in ("train", "val", "test"):
        split_path = split_dir / f"{split_name}.txt"
        if not split_path.exists():
            continue
        for line in split_path.read_text(encoding="utf-8").splitlines():
            item_id = line.strip()
            if not item_id:
                continue
            if item_id in example_lookup:
                split_assignment[example_lookup[item_id]] = split_name
                continue
            pair_key, _, affinity_type = item_id.rpartition("|")
            if not pair_key:
                continue
            parsed = parse_pair_identity_key(pair_key)
            pdb_id = parsed.pdb_id if parsed is not None else ""
            normalized_affinity = "" if affinity_type == "assay_unknown" else affinity_type
            split_assignment[(pdb_id, pair_key, normalized_affinity)] = split_name
    return split_assignment


def _entry_organism_names(entry: dict[str, Any], pdb_chains: list[dict[str, Any]]) -> list[str]:
    organism_names = [
        str(value).strip()
        for value in (entry.get("organism_names") or [])
        if str(value).strip()
    ]
    if organism_names:
        return organism_names
    chain_organisms = sorted(
        {
            str(row.get("entity_source_organism") or "").strip()
            for row in pdb_chains
            if str(row.get("entity_source_organism") or "").strip()
        }
    )
    return chain_organisms


def _entry_quality_score(entry: dict[str, Any], pdb_chains: list[dict[str, Any]]) -> float | None:
    explicit = _safe_float(entry.get("quality_score"))
    if explicit is not None:
        return round(max(0.0, min(explicit, 1.0)), 4)

    protein_chains = [row for row in pdb_chains if row.get("is_protein")]
    uniprot_ids = {
        str(row.get("uniprot_id") or "").strip()
        for row in protein_chains
        if str(row.get("uniprot_id") or "").strip()
    }
    taxonomy_ids = list(entry.get("taxonomy_ids") or [])
    chain_taxonomy_ids: set[int] = set()
    for row in protein_chains:
        value = row.get("entity_source_taxonomy_id")
        if value in (None, ""):
            continue
        try:
            chain_taxonomy_ids.add(int(value))
        except (TypeError, ValueError):
            continue
    organism_names = _entry_organism_names(entry, pdb_chains)
    resolution = _safe_float(entry.get("structure_resolution"))
    checks = [
        bool(str(entry.get("experimental_method") or "").strip()),
        resolution is not None and resolution <= 3.5,
        bool(protein_chains),
        bool(uniprot_ids),
        bool(taxonomy_ids or chain_taxonomy_ids),
        bool(organism_names),
        bool(str(entry.get("structure_file_cif_path") or "").strip()),
    ]
    if not any(checks):
        return None
    return round(sum(checks) / len(checks), 4)


def _normalized_mutation_strings(assay: dict[str, Any]) -> list[str]:
    raw_values = assay.get("mutation_strings") or []
    if isinstance(raw_values, str):
        values = [part.strip() for part in raw_values.split(";") if part.strip()]
    else:
        values = [str(part).strip() for part in raw_values if str(part).strip()]
    if values:
        return values

    parsed = parse_pair_identity_key(str(assay.get("pair_identity_key") or ""))
    mutation_key = str(parsed.mutation_key or "").strip() if parsed is not None else ""
    if not mutation_key:
        return []
    lowered = mutation_key.lower()
    if lowered in {"wt", "wildtype", "wt_or_unspecified"}:
        return ["wt"]
    return [mutation_key]


def _normalized_conflict_fields(assay: dict[str, Any]) -> tuple[str, str]:
    summary = str(assay.get("source_conflict_summary") or "").strip()
    band = str(assay.get("source_agreement_band") or "").strip().lower()
    if summary and band:
        return summary, band

    measurement_count = _safe_int(assay.get("reported_measurement_count")) or 0
    conflict_flag = str(assay.get("source_conflict_flag") or "").strip().lower() == "true"
    standardized = _safe_float(assay.get("binding_affinity_log10_standardized"))
    reported_mean = _safe_float(assay.get("reported_measurement_mean_log10_standardized"))

    if conflict_flag:
        return summary or "conflict_flagged_without_spread_summary", band or "low"
    if measurement_count <= 1:
        return (
            summary or "single_measurement_no_cross_source_conflict_assessment",
            band or "not_assessed_single_source",
        )
    if standardized is None and reported_mean is None:
        return (
            summary or "multiple_measurements_without_standardized_affinity_conflict_assessment",
            band or "not_assessed_missing_standardized_values",
        )
    return (
        summary or "multiple_measurements_without_explicit_conflict_summary",
        band or "not_assessed_multi_measurement",
    )


def _write_csv_rows(
    out_path: Path,
    *,
    columns: list[str],
    rows: list[dict[str, str]],
) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = out_path.with_suffix(out_path.suffix + ".tmp")
    with tmp_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)
    tmp_path.replace(out_path)


def export_master_repository_csv(
    layout: StorageLayout,
    *,
    repo_root: Path | None = None,
) -> Path:
    """Export one row per PDB entry to a root-level CSV."""
    extracted_dir = layout.extracted_dir
    entries = load_table_json(extracted_dir / "entry", logger=logger, warning_prefix="Skipping unreadable export input")
    chains = load_table_json(extracted_dir / "chains", logger=logger, warning_prefix="Skipping unreadable export input")
    bound_objects = load_table_json(extracted_dir / "bound_objects", logger=logger, warning_prefix="Skipping unreadable export input")
    interfaces = load_table_json(extracted_dir / "interfaces", logger=logger, warning_prefix="Skipping unreadable export input")
    assays = load_table_json(extracted_dir / "assays", logger=logger, warning_prefix="Skipping unreadable export input")
    features = load_json_rows(layout.features_dir / "feature_records.json", logger=logger, warning_prefix="Skipping unreadable export input")
    training_examples = load_json_rows(layout.training_dir / "training_examples.json", logger=logger, warning_prefix="Skipping unreadable export input")

    chains_by_pdb: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in chains:
        chains_by_pdb[str(row.get("pdb_id") or "")].append(row)

    bound_by_pdb: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in bound_objects:
        bound_by_pdb[str(row.get("pdb_id") or "")].append(row)

    interfaces_by_pdb: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in interfaces:
        interfaces_by_pdb[str(row.get("pdb_id") or "")].append(row)

    assays_by_pdb: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in assays:
        assays_by_pdb[str(row.get("pdb_id") or "")].append(row)

    features_by_pdb: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in features:
        pdb_id = str(row.get("pdb_id") or "")
        if pdb_id:
            features_by_pdb[pdb_id].append(row)

    training_by_pdb: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in training_examples:
        structure = row.get("structure") or {}
        pdb_id = str(structure.get("pdb_id") or "")
        if pdb_id:
            training_by_pdb[pdb_id].append(row)

    columns = [
        "pdb_id",
        "title",
        "task_hint",
        "experimental_method",
        "structure_resolution",
        "release_date",
        "deposit_date",
        "membrane_vs_soluble",
        "oligomeric_state",
        "homomer_or_heteromer",
        "taxonomy_ids",
        "organism_names",
        "protein_entity_count",
        "nonpolymer_entity_count",
        "polymer_entity_count",
        "branched_entity_count",
        "water_count",
        "deposited_atom_count",
        "metal_present",
        "cofactor_present",
        "glycan_present",
        "covalent_binder_present",
        "peptide_partner_present",
        "has_ligand_signal",
        "has_protein_signal",
        "multiligand_entry",
        "source_databases",
        "quality_flags",
        "quality_score",
        "structure_file_cif_path",
        "structure_file_pdb_path",
        "raw_file_path",
        "chain_count",
        "protein_chain_ids",
        "protein_chain_uniprot_ids",
        "ligand_count",
        "ligand_component_ids",
        "ligand_component_names",
        "ligand_inchikeys",
        "ligand_types",
        "interface_count",
        "interface_types",
        "assay_record_count",
        "assay_sources",
        "assay_types",
        "assay_pairs",
        "reported_measurements",
        "feature_record_count",
        "training_example_count",
        "field_provenance_json",
        "field_confidence_json",
    ]

    rows: list[dict[str, str]] = []
    for entry in sorted(entries, key=lambda row: str(row.get("pdb_id") or "")):
        pdb_id = str(entry.get("pdb_id") or "")
        pdb_chains = chains_by_pdb.get(pdb_id, [])
        pdb_bound = bound_by_pdb.get(pdb_id, [])
        pdb_interfaces = interfaces_by_pdb.get(pdb_id, [])
        pdb_assays = assays_by_pdb.get(pdb_id, [])
        organism_names = _entry_organism_names(entry, pdb_chains)
        quality_score = _entry_quality_score(entry, pdb_chains)

        protein_chains = [row for row in pdb_chains if row.get("is_protein")]
        protein_chain_ids = sorted(
            {
                str(row.get("chain_id") or "")
                for row in protein_chains
                if row.get("chain_id")
            }
        )
        uniprot_ids = sorted(
            {
                str(row.get("uniprot_id") or "")
                for row in protein_chains
                if row.get("uniprot_id")
            }
        )
        ligand_component_ids = sorted(
            {
                str(row.get("component_id") or "")
                for row in pdb_bound
                if row.get("component_id")
            }
        )
        ligand_component_names = sorted(
            {
                str(row.get("component_name") or "")
                for row in pdb_bound
                if row.get("component_name")
            }
        )
        ligand_inchikeys = sorted(
            {
                str(row.get("component_inchikey") or "")
                for row in pdb_bound
                if row.get("component_inchikey")
            }
        )
        ligand_types = sorted(
            {
                str(row.get("component_type") or "")
                for row in pdb_bound
                if row.get("component_type")
            }
        )
        interface_types = sorted(
            {
                str(row.get("interface_type") or "")
                for row in pdb_interfaces
                if row.get("interface_type")
            }
        )
        assay_sources = sorted(
            {
                str(row.get("source_database") or "")
                for row in pdb_assays
                if row.get("source_database")
            }
        )
        assay_types = sorted(
            {
                str(row.get("binding_affinity_type") or "")
                for row in pdb_assays
                if row.get("binding_affinity_type")
            }
        )
        assay_pairs = sorted(
            {
                str(row.get("pair_identity_key") or "")
                for row in pdb_assays
                if row.get("pair_identity_key")
            }
        )
        reported_measurements = sorted(
            {
                str(row.get("reported_measurements_text") or "")
                for row in pdb_assays
                if row.get("reported_measurements_text")
            }
        )

        rows.append({
            "pdb_id": _stringify(entry.get("pdb_id")),
            "title": _stringify(entry.get("title")),
            "task_hint": _stringify(entry.get("task_hint")),
            "experimental_method": _stringify(entry.get("experimental_method")),
            "structure_resolution": _stringify(entry.get("structure_resolution")),
            "release_date": _stringify(entry.get("release_date")),
            "deposit_date": _stringify(entry.get("deposit_date")),
            "membrane_vs_soluble": _stringify(entry.get("membrane_vs_soluble")),
            "oligomeric_state": _stringify(entry.get("oligomeric_state")),
            "homomer_or_heteromer": _stringify(entry.get("homomer_or_heteromer")),
            "taxonomy_ids": _stringify(entry.get("taxonomy_ids")),
            "organism_names": _stringify(organism_names),
            "protein_entity_count": _stringify(entry.get("protein_entity_count")),
            "nonpolymer_entity_count": _stringify(entry.get("nonpolymer_entity_count")),
            "polymer_entity_count": _stringify(entry.get("polymer_entity_count")),
            "branched_entity_count": _stringify(entry.get("branched_entity_count")),
            "water_count": _stringify(entry.get("water_count")),
            "deposited_atom_count": _stringify(entry.get("deposited_atom_count")),
            "metal_present": _stringify(entry.get("metal_present")),
            "cofactor_present": _stringify(entry.get("cofactor_present")),
            "glycan_present": _stringify(entry.get("glycan_present")),
            "covalent_binder_present": _stringify(entry.get("covalent_binder_present")),
            "peptide_partner_present": _stringify(entry.get("peptide_partner_present")),
            "has_ligand_signal": _stringify(bool(pdb_bound or pdb_assays)),
            "has_protein_signal": _stringify(bool(protein_chains)),
            "multiligand_entry": _stringify(entry.get("multiligand_entry")),
            "source_databases": _stringify(assay_sources),
            "quality_flags": _stringify(entry.get("quality_flags")),
            "quality_score": _stringify(quality_score),
            "structure_file_cif_path": _stringify(entry.get("structure_file_cif_path")),
            "structure_file_pdb_path": _stringify(entry.get("structure_file_pdb_path")),
            "raw_file_path": _stringify(entry.get("raw_file_path")),
            "chain_count": _stringify(len(pdb_chains)),
            "protein_chain_ids": _stringify(protein_chain_ids),
            "protein_chain_uniprot_ids": _stringify(uniprot_ids),
            "ligand_count": _stringify(len(pdb_bound)),
            "ligand_component_ids": _stringify(ligand_component_ids),
            "ligand_component_names": _stringify(ligand_component_names),
            "ligand_inchikeys": _stringify(ligand_inchikeys),
            "ligand_types": _stringify(ligand_types),
            "interface_count": _stringify(len(pdb_interfaces)),
            "interface_types": _stringify(interface_types),
            "assay_record_count": _stringify(len(pdb_assays)),
            "assay_sources": _stringify(assay_sources),
            "assay_types": _stringify(assay_types),
            "assay_pairs": _stringify(assay_pairs),
            "reported_measurements": _stringify(reported_measurements),
            "feature_record_count": _stringify(len(features_by_pdb.get(pdb_id, []))),
            "training_example_count": _stringify(len(training_by_pdb.get(pdb_id, []))),
            "field_provenance_json": _stringify(entry.get("field_provenance")),
            "field_confidence_json": _stringify(entry.get("field_confidence")),
        })

    out_path = master_csv_path(repo_root)
    _write_csv_rows(out_path, columns=columns, rows=rows)
    return out_path


def export_master_pair_repository_csv(
    layout: StorageLayout,
    *,
    repo_root: Path | None = None,
) -> Path:
    """Export one row per pair/assay context to a root-level CSV."""
    extracted_dir = layout.extracted_dir
    entries = load_table_json(extracted_dir / "entry", logger=logger, warning_prefix="Skipping unreadable export input")
    chains = load_table_json(extracted_dir / "chains", logger=logger, warning_prefix="Skipping unreadable export input")
    bound_objects = load_table_json(extracted_dir / "bound_objects", logger=logger, warning_prefix="Skipping unreadable export input")
    interfaces = load_table_json(extracted_dir / "interfaces", logger=logger, warning_prefix="Skipping unreadable export input")
    assays = load_table_json(extracted_dir / "assays", logger=logger, warning_prefix="Skipping unreadable export input")
    features = load_json_rows(layout.features_dir / "feature_records.json", logger=logger, warning_prefix="Skipping unreadable export input")
    training_examples = load_json_rows(layout.training_dir / "training_examples.json", logger=logger, warning_prefix="Skipping unreadable export input")

    entry_by_pdb = {
        str(row.get("pdb_id") or ""): row for row in entries if row.get("pdb_id")
    }
    chains_by_pdb: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in chains:
        chains_by_pdb[str(row.get("pdb_id") or "")].append(row)

    bound_by_pdb: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in bound_objects:
        bound_by_pdb[str(row.get("pdb_id") or "")].append(row)

    interfaces_by_pdb: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in interfaces:
        interfaces_by_pdb[str(row.get("pdb_id") or "")].append(row)

    features_by_pair: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in features:
        pair_key = str(row.get("pair_identity_key") or "")
        if pair_key:
            features_by_pair[pair_key].append(row)

    training_by_pair: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in training_examples:
        provenance = row.get("provenance") or {}
        pair_key = str(provenance.get("pair_identity_key") or "")
        if pair_key:
            training_by_pair[pair_key].append(row)

    chain_uniprot_by_pdb_chain: dict[tuple[str, str], str] = {}
    chain_org_by_pdb_chain: dict[tuple[str, str], str] = {}
    for row in chains:
        pdb_id = str(row.get("pdb_id") or "")
        chain_id = str(row.get("chain_id") or "")
        if pdb_id and chain_id:
            if row.get("uniprot_id"):
                chain_uniprot_by_pdb_chain[(pdb_id, chain_id)] = str(row.get("uniprot_id"))
            if row.get("entity_source_organism"):
                chain_org_by_pdb_chain[(pdb_id, chain_id)] = str(row.get("entity_source_organism"))
    split_assignment = _split_assignment_map(layout)

    columns = [
        "pdb_id",
        "pair_identity_key",
        "source_database",
        "binding_affinity_type",
        "binding_affinity_value",
        "binding_affinity_unit",
        "binding_affinity_log10_standardized",
        "delta_g",
        "delta_delta_g",
        "binding_affinity_is_mutant_measurement",
        "mutation_strings",
        "mutation_chain_ids",
        "reported_measurements_text",
        "reported_measurement_mean_log10_standardized",
        "reported_measurement_count",
        "measurement_source_reference",
        "measurement_source_publication",
        "measurement_source_doi",
        "measurement_source_pubmed_id",
        "source_conflict_flag",
        "source_conflict_summary",
        "source_agreement_band",
        "selected_preferred_source",
        "selected_preferred_source_rationale",
        "assay_field_provenance_json",
        "assay_field_confidence_json",
        "title",
        "experimental_method",
        "structure_resolution",
        "membrane_vs_soluble",
        "oligomeric_state",
        "homomer_or_heteromer",
        "quality_flags",
        "receptor_chain_ids",
        "receptor_uniprot_ids",
        "receptor_organisms",
        "ligand_key",
        "ligand_component_ids",
        "ligand_component_names",
        "ligand_inchikeys",
        "ligand_types",
        "matching_interface_count",
        "matching_interface_types",
        "release_split",
        "feature_record_count",
        "training_example_count",
    ]

    rows: list[dict[str, str]] = []
    for assay in sorted(
        assays,
        key=lambda row: (
            str(row.get("pdb_id") or ""),
            str(row.get("pair_identity_key") or ""),
            str(row.get("binding_affinity_type") or ""),
            str(row.get("source_database") or ""),
        ),
    ):
        pdb_id = str(assay.get("pdb_id") or "")
        pair_key = str(assay.get("pair_identity_key") or "")
        if not pdb_id or not pair_key:
            continue
        entry = entry_by_pdb.get(pdb_id, {})
        pair_parts = pair_key.split("|")
        receptor_chain_ids = pair_parts[2].split(",") if len(pair_parts) > 2 and pair_parts[2] not in ("", "-") else []
        ligand_key = pair_parts[3] if len(pair_parts) > 3 and pair_parts[3] not in ("", "-") else ""
        receptor_uniprot_ids = sorted(
            {
                chain_uniprot_by_pdb_chain.get((pdb_id, chain_id), "")
                for chain_id in receptor_chain_ids
                if chain_uniprot_by_pdb_chain.get((pdb_id, chain_id), "")
            }
        )
        receptor_organisms = sorted(
            {
                chain_org_by_pdb_chain.get((pdb_id, chain_id), "")
                for chain_id in receptor_chain_ids
                if chain_org_by_pdb_chain.get((pdb_id, chain_id), "")
            }
        )

        matching_bound = []
        for row in bound_by_pdb.get(pdb_id, []):
            candidate_ids = {
                str(row.get("component_id") or ""),
                str(row.get("component_name") or ""),
                str(row.get("component_inchikey") or ""),
            }
            if ligand_key and ligand_key in candidate_ids:
                matching_bound.append(row)

        matching_interfaces = []
        for row in interfaces_by_pdb.get(pdb_id, []):
            row_chain_ids = set(row.get("binding_site_chain_ids") or [])
            row_ligand = str(row.get("entity_name_b") or "")
            if receptor_chain_ids and row_chain_ids and not row_chain_ids.intersection(receptor_chain_ids):
                continue
            if ligand_key and row_ligand and row_ligand != ligand_key:
                continue
            matching_interfaces.append(row)
        mutation_strings = _normalized_mutation_strings(assay)
        conflict_summary, agreement_band = _normalized_conflict_fields(assay)
        release_split = split_assignment.get((pdb_id, pair_key, str(assay.get("binding_affinity_type") or "")), "")

        rows.append({
            "pdb_id": _stringify(pdb_id),
            "pair_identity_key": _stringify(pair_key),
            "source_database": _stringify(assay.get("source_database")),
            "binding_affinity_type": _stringify(assay.get("binding_affinity_type")),
            "binding_affinity_value": _stringify(assay.get("binding_affinity_value")),
            "binding_affinity_unit": _stringify(assay.get("binding_affinity_unit")),
            "binding_affinity_log10_standardized": _stringify(assay.get("binding_affinity_log10_standardized")),
            "delta_g": _stringify(assay.get("delta_g")),
            "delta_delta_g": _stringify(assay.get("delta_delta_g")),
            "binding_affinity_is_mutant_measurement": _stringify(assay.get("binding_affinity_is_mutant_measurement")),
            "mutation_strings": _stringify(mutation_strings),
            "mutation_chain_ids": _stringify(assay.get("mutation_chain_ids")),
            "reported_measurements_text": _stringify(assay.get("reported_measurements_text")),
            "reported_measurement_mean_log10_standardized": _stringify(assay.get("reported_measurement_mean_log10_standardized")),
            "reported_measurement_count": _stringify(assay.get("reported_measurement_count")),
            "measurement_source_reference": _stringify(assay.get("measurement_source_reference")),
            "measurement_source_publication": _stringify(assay.get("measurement_source_publication")),
            "measurement_source_doi": _stringify(assay.get("measurement_source_doi")),
            "measurement_source_pubmed_id": _stringify(assay.get("measurement_source_pubmed_id")),
            "source_conflict_flag": _stringify(assay.get("source_conflict_flag")),
            "source_conflict_summary": _stringify(conflict_summary),
            "source_agreement_band": _stringify(agreement_band),
            "selected_preferred_source": _stringify(assay.get("selected_preferred_source")),
            "selected_preferred_source_rationale": _stringify(assay.get("selected_preferred_source_rationale")),
            "assay_field_provenance_json": _stringify(assay.get("field_provenance")),
            "assay_field_confidence_json": _stringify(assay.get("field_confidence")),
            "title": _stringify(entry.get("title")),
            "experimental_method": _stringify(entry.get("experimental_method")),
            "structure_resolution": _stringify(entry.get("structure_resolution")),
            "membrane_vs_soluble": _stringify(entry.get("membrane_vs_soluble")),
            "oligomeric_state": _stringify(entry.get("oligomeric_state")),
            "homomer_or_heteromer": _stringify(entry.get("homomer_or_heteromer")),
            "quality_flags": _stringify(entry.get("quality_flags")),
            "receptor_chain_ids": _stringify(receptor_chain_ids),
            "receptor_uniprot_ids": _stringify(receptor_uniprot_ids),
            "receptor_organisms": _stringify(receptor_organisms),
            "ligand_key": _stringify(ligand_key),
            "ligand_component_ids": _stringify(sorted({str(row.get("component_id") or "") for row in matching_bound if row.get("component_id")})),
            "ligand_component_names": _stringify(sorted({str(row.get("component_name") or "") for row in matching_bound if row.get("component_name")})),
            "ligand_inchikeys": _stringify(sorted({str(row.get("component_inchikey") or "") for row in matching_bound if row.get("component_inchikey")})),
            "ligand_types": _stringify(sorted({str(row.get("component_type") or "") for row in matching_bound if row.get("component_type")})),
            "matching_interface_count": _stringify(len(matching_interfaces)),
            "matching_interface_types": _stringify(sorted({str(row.get("interface_type") or "") for row in matching_interfaces if row.get("interface_type")})),
            "release_split": _stringify(release_split),
            "feature_record_count": _stringify(len(features_by_pair.get(pair_key, []))),
            "training_example_count": _stringify(len(training_by_pair.get(pair_key, []))),
        })

    out_path = pair_master_csv_path(repo_root)
    _write_csv_rows(out_path, columns=columns, rows=rows)
    return out_path


def refresh_master_exports(
    layout: StorageLayout,
    *,
    repo_root: Path | None = None,
) -> dict[str, str]:
    """Refresh root-level master CSVs without breaking the pipeline if locked.

    This is an operational safeguard: if a user has the CSV open in another
    application, the data pipeline should still complete and report that export
    refresh failed.
    """
    result: dict[str, str] = {}
    try:
        result["master_csv"] = str(export_master_repository_csv(layout, repo_root=repo_root))
    except OSError as exc:
        result["master_csv_error"] = str(exc)
    try:
        result["pair_csv"] = str(export_master_pair_repository_csv(layout, repo_root=repo_root))
    except OSError as exc:
        result["pair_csv_error"] = str(exc)
    try:
        result["issue_csv"] = str(export_issue_repository_csv(layout, repo_root=repo_root))
    except OSError as exc:
        result["issue_csv_error"] = str(exc)
    try:
        result["conflict_csv"] = str(export_conflict_repository_csv(layout, repo_root=repo_root))
    except OSError as exc:
        result["conflict_csv_error"] = str(exc)
    try:
        result["source_state_csv"] = str(export_source_state_csv(layout, repo_root=repo_root))
    except OSError as exc:
        result["source_state_csv_error"] = str(exc)
    try:
        from pbdata.release_export import export_release_artifacts

        result.update(export_release_artifacts(layout, repo_root=repo_root))
    except OSError as exc:
        result["release_exports_error"] = str(exc)
    return result


def export_issue_repository_csv(
    layout: StorageLayout,
    *,
    repo_root: Path | None = None,
) -> Path:
    """Export filtered issue rows for rapid manual review."""
    extracted_dir = layout.extracted_dir
    entries = load_table_json(extracted_dir / "entry", logger=logger, warning_prefix="Skipping unreadable export input")
    bound_objects = load_table_json(extracted_dir / "bound_objects", logger=logger, warning_prefix="Skipping unreadable export input")
    assays = load_table_json(extracted_dir / "assays", logger=logger, warning_prefix="Skipping unreadable export input")
    pair_rows = _load_csv_rows(pair_master_csv_path(repo_root))

    assay_count_by_pdb: dict[str, int] = defaultdict(int)
    for row in assays:
        assay_count_by_pdb[str(row.get("pdb_id") or "")] += 1

    bound_by_pdb: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in bound_objects:
        bound_by_pdb[str(row.get("pdb_id") or "")].append(row)

    issue_rows: list[dict[str, str]] = []
    for entry in entries:
        pdb_id = str(entry.get("pdb_id") or "")
        if not pdb_id:
            continue
        if not entry.get("structure_file_cif_path"):
            issue_rows.append({
                "scope": "entry",
                "pdb_id": pdb_id,
                "pair_identity_key": "",
                "issue_type": "missing_structure_file",
                "details": "No mmCIF/CIF path recorded on the entry record.",
            })
        if assay_count_by_pdb.get(pdb_id, 0) == 0:
            issue_rows.append({
                "scope": "entry",
                "pdb_id": pdb_id,
                "pair_identity_key": "",
                "issue_type": "no_assay_data",
                "details": "No assay rows are currently attached to this entry.",
            })
        field_conf = entry.get("field_confidence") or {}
        non_high_fields = sorted(
            field for field, value in field_conf.items()
            if str(value or "").lower() not in {"", "high"}
        )
        if non_high_fields:
            issue_rows.append({
                "scope": "entry",
                "pdb_id": pdb_id,
                "pair_identity_key": "",
                "issue_type": "non_high_confidence_fields",
                "details": "; ".join(non_high_fields),
            })
        missing_descriptor_ids = sorted(
            {
                str(row.get("component_id") or "")
                for row in bound_by_pdb.get(pdb_id, [])
                if str(row.get("component_type") or "") in {"small_molecule", "cofactor"}
                and not (row.get("component_smiles") or row.get("component_inchikey"))
                and row.get("component_id")
            }
        )
        if missing_descriptor_ids:
            issue_rows.append({
                "scope": "entry",
                "pdb_id": pdb_id,
                "pair_identity_key": "",
                "issue_type": "missing_ligand_descriptors",
                "details": "; ".join(missing_descriptor_ids),
            })

    for row in pair_rows:
        pair_key = str(row.get("pair_identity_key") or "")
        pdb_id = str(row.get("pdb_id") or "")
        if not pair_key:
            continue
        if str(row.get("matching_interface_count") or "0") == "0":
            issue_rows.append({
                "scope": "pair",
                "pdb_id": pdb_id,
                "pair_identity_key": pair_key,
                "issue_type": "no_matched_interface",
                "details": "No interface rows matched this pair context.",
            })
        field_conf = _parse_json_object(row.get("assay_field_confidence_json"))
        field_prov = _parse_json_object(row.get("assay_field_provenance_json"))
        pair_key_prov = field_prov.get("pair_identity_key") or {}
        override_used = bool(pair_key_prov.get("override_used"))
        is_explicit_non_mutant = str(row.get("binding_affinity_is_mutant_measurement") or "").strip().lower() == "false"

        # Assumption:
        # - `mutation_unknown` is a hard ambiguity when the source row could still
        #   plausibly describe a mutant context.
        # - When the source explicitly says the measurement is not mutant and the
        #   token only comes from a conservative pair-grouping override, keep the
        #   caution in field confidence but do not promote it to a biological
        #   blocker.
        if "mutation_unknown" in pair_key and not (override_used and is_explicit_non_mutant):
            issue_rows.append({
                "scope": "pair",
                "pdb_id": pdb_id,
                "pair_identity_key": pair_key,
                "issue_type": "ambiguous_mutation_context",
                "details": "Pair key contains mutation_unknown, so assay context is not fully resolved.",
            })
        if str(row.get("source_conflict_flag") or "").lower() == "true":
            issue_rows.append({
                "scope": "pair",
                "pdb_id": pdb_id,
                "pair_identity_key": pair_key,
                "issue_type": "source_value_conflict",
                "details": str(row.get("source_conflict_summary") or "Conflicting source values detected."),
            })
        non_high_fields = sorted(
            field for field, value in field_conf.items()
            if str(value or "").lower() not in {"", "high"}
        )
        critical_non_high_fields = [
            field for field in non_high_fields
            if field in _CRITICAL_ASSAY_CONFIDENCE_FIELDS
        ]
        advisory_non_high_fields = [
            field for field in non_high_fields
            if field not in _CRITICAL_ASSAY_CONFIDENCE_FIELDS
        ]
        if critical_non_high_fields:
            issue_rows.append({
                "scope": "pair",
                "pdb_id": pdb_id,
                "pair_identity_key": pair_key,
                "issue_type": "non_high_confidence_assay_fields",
                "details": "; ".join(critical_non_high_fields),
            })
        if advisory_non_high_fields:
            issue_rows.append({
                "scope": "pair",
                "pdb_id": pdb_id,
                "pair_identity_key": pair_key,
                "issue_type": "advisory_non_high_confidence_assay_fields",
                "details": "; ".join(advisory_non_high_fields),
            })

    columns = ["scope", "pdb_id", "pair_identity_key", "issue_type", "details"]
    out_path = issue_csv_path(repo_root)
    _write_csv_rows(out_path, columns=columns, rows=issue_rows)
    return out_path


def export_conflict_repository_csv(
    layout: StorageLayout,
    *,
    repo_root: Path | None = None,
) -> Path:
    """Export only assay rows with explicit cross-source value conflicts."""
    del layout  # Export is driven by the already-joined pair master CSV.
    pair_rows = _load_csv_rows(pair_master_csv_path(repo_root))

    columns = [
        "pdb_id",
        "pair_identity_key",
        "binding_affinity_type",
        "source_database",
        "reported_measurements_text",
        "reported_measurement_count",
        "reported_measurement_mean_log10_standardized",
        "source_conflict_summary",
        "source_agreement_band",
        "selected_preferred_source",
        "selected_preferred_source_rationale",
        "measurement_source_reference",
        "measurement_source_publication",
        "measurement_source_doi",
        "measurement_source_pubmed_id",
        "assay_field_provenance_json",
        "assay_field_confidence_json",
    ]

    rows = [
        {column: str(row.get(column) or "") for column in columns}
        for row in pair_rows
        if str(row.get("source_conflict_flag") or "").lower() == "true"
    ]
    rows.sort(
        key=lambda row: (
            row["pdb_id"],
            row["pair_identity_key"],
            row["binding_affinity_type"],
            row["selected_preferred_source"],
            row["source_database"],
        )
    )

    out_path = conflict_csv_path(repo_root)
    _write_csv_rows(out_path, columns=columns, rows=rows)
    return out_path


def export_source_state_csv(
    layout: StorageLayout,
    *,
    repo_root: Path | None = None,
) -> Path:
    """Export per-source operational state for root-level review."""
    rows: list[dict[str, str]] = []
    if layout.source_state_dir.exists():
        for path in sorted(layout.source_state_dir.glob("*.json")):
            raw = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(raw, dict):
                continue
            extra = raw.get("extra") or {}
            rows.append({
                "source_name": _stringify(raw.get("source_name")),
                "status": _stringify(raw.get("status")),
                "mode": _stringify(raw.get("mode")),
                "record_id": _stringify(raw.get("record_id")),
                "record_count": _stringify(raw.get("record_count")),
                "attempt_count": _stringify(extra.get("attempt_count")),
                "total_records_observed": _stringify(extra.get("total_records_observed")),
                "status_counts": _stringify(extra.get("status_counts")),
                "cache_path": _stringify(raw.get("cache_path")),
                "cache_mtime": _stringify(raw.get("cache_mtime")),
                "cache_size_bytes": _stringify(raw.get("cache_size_bytes")),
                "generated_at": _stringify(raw.get("generated_at")),
                "notes": _stringify(raw.get("notes")),
                "configured_local_dir": _stringify(extra.get("configured_local_dir")),
            })

    columns = [
        "source_name",
        "status",
        "mode",
        "record_id",
        "record_count",
        "attempt_count",
        "total_records_observed",
        "status_counts",
        "cache_path",
        "cache_mtime",
        "cache_size_bytes",
        "generated_at",
        "notes",
        "configured_local_dir",
    ]
    out_path = source_state_csv_path(repo_root)
    _write_csv_rows(out_path, columns=columns, rows=rows)
    return out_path
