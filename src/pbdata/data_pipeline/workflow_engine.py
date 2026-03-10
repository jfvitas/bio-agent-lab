"""Workspace-oriented workflow engine helpers.

Assumptions:
- This module adds the instruction-pack workspace layout alongside the existing
  `data/` and `artifacts/` layout rather than replacing it.
- Metadata harvest produces a unified review/build table from the repo's
  extracted and root-level exports, preferring explicit extracted values.
- Dataset export configs are intentionally simple and reproducible; they are not
  a substitute for the richer release manifests already present elsewhere.
"""

from __future__ import annotations

import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

from pbdata.storage import StorageLayout

_WORKFLOW_STEPS = [
    "workspace_setup",
    "protein_search",
    "metadata_harvest",
    "structure_download",
    "feature_extraction",
    "graph_generation",
    "dataset_engineering",
]


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def initialize_workspace(layout: StorageLayout) -> dict[str, str]:
    """Create the instruction-pack workspace directories and manifest."""
    dirs = [
        layout.data_sources_dir,
        layout.workspace_structures_dir,
        layout.clean_structures_dir,
        layout.workspace_features_dir,
        layout.workspace_graphs_dir,
        layout.workspace_datasets_dir,
        layout.workspace_metadata_dir,
        layout.workspace_logs_dir,
        layout.rosetta_outputs_dir,
    ]
    for path in dirs:
        path.mkdir(parents=True, exist_ok=True)

    manifest = {
        "generated_at": _utc_now(),
        "workflow_engine_version": "protein_ml_data_lab_v1",
        "workspace_root": str(layout.root),
        "steps": _WORKFLOW_STEPS,
        "directories": {path.name: str(path) for path in dirs},
    }
    manifest_path = layout.workspace_metadata_dir / "workflow_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return {"workflow_manifest": str(manifest_path)}


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _load_json_rows(table_dir: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not table_dir.exists():
        return rows
    for path in sorted(table_dir.glob("*.json")):
        raw = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(raw, list):
            rows.extend(item for item in raw if isinstance(item, dict))
        elif isinstance(raw, dict):
            rows.append(raw)
    return rows


def _chain_sequence_index(layout: StorageLayout) -> dict[tuple[str, str], dict[str, Any]]:
    rows = _load_json_rows(layout.extracted_dir / "chains")
    index: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        pdb_id = str(row.get("pdb_id") or "").upper()
        chain_id = str(row.get("chain_id") or "")
        if pdb_id and chain_id:
            index[(pdb_id, chain_id)] = row
    return index


def harvest_unified_metadata(layout: StorageLayout) -> dict[str, str]:
    """Build a unified metadata table for downstream graph/dataset workflows."""
    layout.workspace_metadata_dir.mkdir(parents=True, exist_ok=True)
    pair_rows = _read_csv_rows(layout.root / "master_pdb_pairs.csv")
    entry_rows = _read_csv_rows(layout.root / "master_pdb_repository.csv")
    if not pair_rows:
        pair_rows = []
        assays = _load_json_rows(layout.extracted_dir / "assays")
        entries = {
            str(row.get("pdb_id") or "").upper(): row
            for row in _load_json_rows(layout.extracted_dir / "entry")
            if row.get("pdb_id")
        }
        for assay in assays:
            pdb_id = str(assay.get("pdb_id") or "").upper()
            entry = entries.get(pdb_id, {})
            pair_rows.append(
                {
                    "pdb_id": pdb_id,
                    "pair_identity_key": assay.get("pair_identity_key") or "",
                    "source_database": assay.get("source_database") or "RCSB",
                    "binding_affinity_type": assay.get("binding_affinity_type") or "",
                    "binding_affinity_value": assay.get("binding_affinity_value") or assay.get("reported_measurement_mean_log10_standardized") or "",
                    "receptor_chain_ids": assay.get("receptor_chain_ids") or "A",
                    "receptor_uniprot_ids": assay.get("receptor_uniprot_ids") or "",
                    "receptor_organisms": assay.get("receptor_organisms") or entry.get("organism_names") or "",
                    "ligand_types": assay.get("ligand_types") or "",
                    "matching_interface_types": assay.get("matching_interface_types") or "",
                    "experimental_method": entry.get("experimental_method") or "",
                    "structure_resolution": entry.get("structure_resolution") or "",
                    "mutation_strings": assay.get("mutation_strings") or "",
                }
            )
    entry_by_pdb = {
        str(row.get("pdb_id") or "").upper(): row
        for row in entry_rows
        if row.get("pdb_id")
    }
    if not entry_by_pdb:
        entry_by_pdb = {
            str(row.get("pdb_id") or "").upper(): row
            for row in _load_json_rows(layout.extracted_dir / "entry")
            if row.get("pdb_id")
        }
    chain_index = _chain_sequence_index(layout)

    out_rows: list[dict[str, Any]] = []
    for pair in pair_rows:
        pdb_id = str(pair.get("pdb_id") or "").upper()
        if not pdb_id:
            continue
        entry = entry_by_pdb.get(pdb_id, {})
        chain_ids = [
            part.strip()
            for part in str(pair.get("receptor_chain_ids") or "").replace(";", ",").split(",")
            if part.strip()
        ]
        sequences = [
            str(chain_index.get((pdb_id, chain_id), {}).get("polymer_sequence") or "")
            for chain_id in chain_ids
        ]
        sequences = [seq for seq in sequences if seq]
        out_rows.append(
            {
                "pdb_id": pdb_id,
                "pair_identity_key": pair.get("pair_identity_key") or "",
                "uniprot_id": pair.get("receptor_uniprot_ids") or "",
                "organism": pair.get("receptor_organisms") or entry.get("organism_names") or "",
                "structure_resolution": pair.get("structure_resolution") or entry.get("structure_resolution") or "",
                "binding_affinity": pair.get("binding_affinity_value") or "",
                "binding_affinity_type": pair.get("binding_affinity_type") or "",
                "protein_family": pair.get("receptor_uniprot_ids") or "",
                "structural_fold": entry.get("oligomeric_state") or "",
                "binding_interface_type": pair.get("matching_interface_types") or "",
                "ligand_class": pair.get("ligand_types") or "",
                "experimental_method": pair.get("experimental_method") or entry.get("experimental_method") or "",
                "sequence": sequences[0] if sequences else "",
                "sequence_count": len(sequences),
                "mutation_strings": pair.get("mutation_strings") or "",
                "source_database": pair.get("source_database") or "",
                "release_split": pair.get("release_split") or "",
            }
        )

    metadata_path = layout.workspace_metadata_dir / "protein_metadata.csv"
    if out_rows:
        with metadata_path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(out_rows[0].keys()))
            writer.writeheader()
            writer.writerows(out_rows)

    manifest = {
        "generated_at": _utc_now(),
        "status": "harvested" if out_rows else "empty",
        "row_count": len(out_rows),
        "source_files": [
            str(layout.root / "master_pdb_pairs.csv"),
            str(layout.root / "master_pdb_repository.csv"),
            str(layout.extracted_dir / "chains"),
        ],
    }
    manifest_path = layout.workspace_metadata_dir / "metadata_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return {"metadata_csv": str(metadata_path), "manifest": str(manifest_path)}


def write_dataset_export_configs(
    layout: StorageLayout,
    *,
    dataset_name: str,
    dataset_config: dict[str, Any],
    feature_schema: dict[str, Any],
    graph_config: dict[str, Any],
) -> dict[str, str]:
    """Write the reproducibility config files required by the workflow spec."""
    out_dir = layout.workspace_datasets_dir / dataset_name
    out_dir.mkdir(parents=True, exist_ok=True)
    dataset_config_path = out_dir / "dataset_config.yaml"
    feature_schema_path = out_dir / "feature_schema.json"
    graph_config_path = out_dir / "graph_config.json"
    dataset_config_path.write_text(yaml.safe_dump(dataset_config, sort_keys=True), encoding="utf-8")
    feature_schema_path.write_text(json.dumps(feature_schema, indent=2), encoding="utf-8")
    graph_config_path.write_text(json.dumps(graph_config, indent=2), encoding="utf-8")
    return {
        "dataset_config": str(dataset_config_path),
        "feature_schema": str(feature_schema_path),
        "graph_config": str(graph_config_path),
    }
