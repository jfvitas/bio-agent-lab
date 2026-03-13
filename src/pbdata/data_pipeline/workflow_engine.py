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
from pbdata.table_io import load_table_json

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


def _path_freshness_payload(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {
            "path": str(path),
            "exists": False,
            "file_count": 0,
            "latest_mtime": None,
            "freshness": "missing",
        }
    files = [candidate for candidate in path.rglob("*") if candidate.is_file()]
    latest = max((candidate.stat().st_mtime for candidate in files), default=path.stat().st_mtime)
    latest_dt = datetime.fromtimestamp(latest, tz=timezone.utc)
    age_seconds = max((datetime.now(timezone.utc) - latest_dt).total_seconds(), 0.0)
    if age_seconds < 3600:
        freshness = "fresh"
    elif age_seconds < 86400:
        freshness = "recent"
    else:
        freshness = "stale"
    return {
        "path": str(path),
        "exists": True,
        "file_count": len(files),
        "latest_mtime": latest_dt.isoformat(),
        "freshness": freshness,
    }


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

def _chain_sequence_index(layout: StorageLayout) -> dict[tuple[str, str], dict[str, Any]]:
    rows = load_table_json(layout.extracted_dir / "chains")
    index: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        pdb_id = str(row.get("pdb_id") or "").upper()
        chain_id = str(row.get("chain_id") or "")
        if pdb_id and chain_id:
            index[(pdb_id, chain_id)] = row
    return index


def _annotation_cache_dir(layout: StorageLayout, source_name: str) -> Path:
    return layout.workspace_metadata_dir / "source_annotations" / source_name


def _annotation_cache_summary(layout: StorageLayout, source_name: str) -> dict[str, Any]:
    return {
        "source_name": source_name,
        **_path_freshness_payload(_annotation_cache_dir(layout, source_name)),
    }


def _split_identifier_values(raw: Any) -> list[str]:
    text = str(raw or "")
    values = [
        part.strip()
        for part in text.replace(";", ",").split(",")
        if part.strip()
    ]
    return list(dict.fromkeys(values))


def _row_primary_uniprot_id(row: dict[str, Any]) -> str:
    ids = _split_identifier_values(row.get("uniprot_id") or row.get("receptor_uniprot_ids") or "")
    return ids[0] if ids else ""


def _collect_annotation_maps(
    layout: StorageLayout,
    rows: list[dict[str, Any]],
    *,
    enrich_uniprot: bool,
    enrich_alphafold: bool,
    enrich_reactome: bool,
    enrich_interpro: bool,
    enrich_pfam: bool,
    enrich_cath: bool,
    enrich_scop: bool,
    max_proteins: int | None = None,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
    uniprot_map: dict[str, Any] = {}
    alphafold_map: dict[str, Any] = {}
    reactome_map: dict[str, Any] = {}
    interpro_map: dict[str, Any] = {}
    pfam_map: dict[str, Any] = {}
    cath_map: dict[str, Any] = {}
    scop_map: dict[str, Any] = {}
    summary = {
        "requested_uniprot_ids": 0,
        "requested_pdb_ids": 0,
        "uniprot_ready": 0,
        "alphafold_ready": 0,
        "reactome_ready": 0,
        "interpro_ready": 0,
        "pfam_ready": 0,
        "cath_ready": 0,
        "scop_ready": 0,
        "errors": [],
    }
    uniprot_ids = [
        _row_primary_uniprot_id(row)
        for row in rows
        if _row_primary_uniprot_id(row)
    ]
    unique_ids = list(dict.fromkeys(uniprot_ids))
    if max_proteins is not None and max_proteins >= 0:
        unique_ids = unique_ids[:max_proteins]
    summary["requested_uniprot_ids"] = len(unique_ids)
    pdb_ids = [
        str(row.get("pdb_id") or "").strip().upper()
        for row in rows
        if str(row.get("pdb_id") or "").strip()
    ]
    unique_pdb_ids = list(dict.fromkeys(pdb_ids))
    if max_proteins is not None and max_proteins >= 0:
        unique_pdb_ids = unique_pdb_ids[:max_proteins]
    summary["requested_pdb_ids"] = len(unique_pdb_ids)

    if enrich_uniprot and unique_ids:
        from pbdata.sources.uniprot import UniProtAdapter

        adapter = UniProtAdapter(cache_dir=_annotation_cache_dir(layout, "uniprot"))
        for accession in unique_ids:
            try:
                uniprot_map[accession] = adapter.fetch_annotation(accession)
                summary["uniprot_ready"] += 1
            except Exception as exc:
                summary["errors"].append(f"UniProt:{accession}:{exc}")

    if enrich_alphafold and unique_ids:
        from pbdata.sources.alphafold import AlphaFoldAdapter

        adapter = AlphaFoldAdapter(cache_dir=_annotation_cache_dir(layout, "alphafold_db"))
        for accession in unique_ids:
            try:
                alphafold_map[accession] = adapter.fetch_prediction(accession)
                summary["alphafold_ready"] += 1
            except Exception as exc:
                summary["errors"].append(f"AlphaFold:{accession}:{exc}")

    if enrich_reactome and unique_ids:
        from pbdata.sources.reactome import ReactomeAdapter

        adapter = ReactomeAdapter(cache_dir=_annotation_cache_dir(layout, "reactome"))
        for accession in unique_ids:
            try:
                reactome_map[accession] = adapter.fetch_annotation(accession)
                summary["reactome_ready"] += 1
            except Exception as exc:
                summary["errors"].append(f"Reactome:{accession}:{exc}")

    if enrich_interpro and unique_pdb_ids:
        from pbdata.sources.interpro import InterProAdapter

        adapter = InterProAdapter(cache_dir=_annotation_cache_dir(layout, "interpro"))
        for pdb_id in unique_pdb_ids:
            try:
                interpro_map[pdb_id] = adapter.fetch_annotation(pdb_id)
                summary["interpro_ready"] += 1
            except Exception as exc:
                summary["errors"].append(f"InterPro:{pdb_id}:{exc}")

    if enrich_pfam and unique_pdb_ids:
        from pbdata.sources.pfam import PfamAdapter

        adapter = PfamAdapter(cache_dir=_annotation_cache_dir(layout, "pfam"))
        for pdb_id in unique_pdb_ids:
            try:
                pfam_map[pdb_id] = adapter.fetch_annotation(pdb_id)
                summary["pfam_ready"] += 1
            except Exception as exc:
                summary["errors"].append(f"Pfam:{pdb_id}:{exc}")

    if enrich_cath and unique_pdb_ids:
        from pbdata.sources.cath import CATHAdapter

        adapter = CATHAdapter(cache_dir=_annotation_cache_dir(layout, "cath"))
        for pdb_id in unique_pdb_ids:
            try:
                cath_map[pdb_id] = adapter.fetch_annotation(pdb_id)
                summary["cath_ready"] += 1
            except Exception as exc:
                summary["errors"].append(f"CATH:{pdb_id}:{exc}")

    if enrich_scop and unique_pdb_ids:
        from pbdata.sources.scop import SCOPAdapter

        adapter = SCOPAdapter(cache_dir=_annotation_cache_dir(layout, "scop"))
        for pdb_id in unique_pdb_ids:
            try:
                scop_map[pdb_id] = adapter.fetch_annotation(pdb_id)
                summary["scop_ready"] += 1
            except Exception as exc:
                summary["errors"].append(f"SCOP:{pdb_id}:{exc}")

    return uniprot_map, alphafold_map, reactome_map, interpro_map, pfam_map, cath_map, scop_map, summary


def _preferred_chain_ids(row: dict[str, Any]) -> list[str]:
    return [
        chain_id.strip()
        for chain_id in str(row.get("receptor_chain_ids") or "").replace(";", ",").split(",")
        if chain_id.strip()
    ]


def _annotation_ids_for_chains(record: Any, chain_ids: list[str]) -> list[str]:
    if record is None:
        return []
    chain_map = getattr(record, "chain_to_domain_ids", {}) or {}
    if chain_ids and chain_map:
        values: list[str] = []
        for chain_id in chain_ids:
            values.extend(chain_map.get(chain_id, []) or [])
        deduped = [value for value in dict.fromkeys(values) if value]
        if deduped:
            return deduped
    return list(getattr(record, "domain_ids", []) or [])


def harvest_unified_metadata(
    layout: StorageLayout,
    *,
    enrich_uniprot: bool = False,
    enrich_alphafold: bool = False,
    enrich_reactome: bool = False,
    enrich_interpro: bool = False,
    enrich_pfam: bool = False,
    enrich_cath: bool = False,
    enrich_scop: bool = False,
    max_proteins: int | None = None,
) -> dict[str, str]:
    """Build a unified metadata table for downstream graph/dataset workflows."""
    layout.workspace_metadata_dir.mkdir(parents=True, exist_ok=True)
    pair_rows = _read_csv_rows(layout.root / "master_pdb_pairs.csv")
    entry_rows = _read_csv_rows(layout.root / "master_pdb_repository.csv")
    if not pair_rows:
        pair_rows = []
        assays = load_table_json(layout.extracted_dir / "assays")
        entries = {
            str(row.get("pdb_id") or "").upper(): row
            for row in load_table_json(layout.extracted_dir / "entry")
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
            for row in load_table_json(layout.extracted_dir / "entry")
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
        chain_uniprot_ids = [
            str(chain_index.get((pdb_id, chain_id), {}).get("uniprot_id") or "").strip()
            for chain_id in chain_ids
        ]
        chain_uniprot_ids = [uniprot_id for uniprot_id in chain_uniprot_ids if uniprot_id]
        sequences = [
            str(chain_index.get((pdb_id, chain_id), {}).get("polymer_sequence") or "")
            for chain_id in chain_ids
        ]
        sequences = [seq for seq in sequences if seq]
        out_rows.append(
            {
                "pdb_id": pdb_id,
                "pair_identity_key": pair.get("pair_identity_key") or "",
                "uniprot_id": pair.get("receptor_uniprot_ids") or "; ".join(dict.fromkeys(chain_uniprot_ids)),
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

    (
        uniprot_map,
        alphafold_map,
        reactome_map,
        interpro_map,
        pfam_map,
        cath_map,
        scop_map,
        annotation_summary,
    ) = _collect_annotation_maps(
        layout,
        out_rows,
        enrich_uniprot=enrich_uniprot,
        enrich_alphafold=enrich_alphafold,
        enrich_reactome=enrich_reactome,
        enrich_interpro=enrich_interpro,
        enrich_pfam=enrich_pfam,
        enrich_cath=enrich_cath,
        enrich_scop=enrich_scop,
        max_proteins=max_proteins,
    )
    for row in out_rows:
        accession = _row_primary_uniprot_id(row)
        pdb_id = str(row.get("pdb_id") or "").strip().upper()
        chain_ids = _preferred_chain_ids(row)
        uniprot_record = uniprot_map.get(accession)
        alphafold_record = alphafold_map.get(accession)
        reactome_record = reactome_map.get(accession)
        interpro_record = interpro_map.get(pdb_id)
        pfam_record = pfam_map.get(pdb_id)
        cath_record = cath_map.get(pdb_id)
        scop_record = scop_map.get(pdb_id)
        merged_interpro_ids = sorted(set((getattr(uniprot_record, "interpro_ids", []) or []) + _annotation_ids_for_chains(interpro_record, chain_ids)))
        merged_pfam_ids = sorted(set((getattr(uniprot_record, "pfam_ids", []) or []) + _annotation_ids_for_chains(pfam_record, chain_ids)))
        cath_ids = _annotation_ids_for_chains(cath_record, chain_ids)
        scop_ids = _annotation_ids_for_chains(scop_record, chain_ids)
        row["annotation_uniprot_id"] = accession
        row["uniprot_protein_name"] = getattr(uniprot_record, "protein_name", "")
        row["uniprot_gene_names"] = "; ".join(getattr(uniprot_record, "gene_names", []) or [])
        row["uniprot_reviewed"] = (
            "true" if getattr(uniprot_record, "reviewed", False) else "false"
            if uniprot_record is not None
            else ""
        )
        row["uniprot_sequence_length"] = getattr(uniprot_record, "sequence_length", "") or ""
        row["interpro_ids"] = "; ".join(merged_interpro_ids)
        row["pfam_ids"] = "; ".join(merged_pfam_ids)
        row["go_terms"] = "; ".join(getattr(uniprot_record, "go_terms", []) or [])
        row["alphafold_entry_id"] = getattr(alphafold_record, "entry_id", "")
        row["alphafold_cif_url"] = getattr(alphafold_record, "cif_url", "")
        row["alphafold_model_version"] = getattr(alphafold_record, "model_version", "")
        row["reactome_pathway_count"] = getattr(reactome_record, "pathway_count", "") or ""
        row["reactome_pathway_ids"] = "; ".join(getattr(reactome_record, "pathway_ids", []) or [])
        row["reactome_pathway_names"] = "; ".join(getattr(reactome_record, "pathway_names", []) or [])
        row["cath_ids"] = "; ".join(cath_ids)
        row["scop_ids"] = "; ".join(scop_ids)
        row["cath_names"] = "; ".join(getattr(cath_record, "domain_names", []) or [])
        row["scop_names"] = "; ".join(getattr(scop_record, "domain_names", []) or [])
        row["structural_fold"] = (
            "; ".join(cath_ids)
            or "; ".join(scop_ids)
            or str(row.get("structural_fold") or "")
        )

    metadata_path = layout.workspace_metadata_dir / "protein_metadata.csv"
    if out_rows:
        metadata_path.parent.mkdir(parents=True, exist_ok=True)
        with metadata_path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(out_rows[0].keys()))
            writer.writeheader()
            writer.writerows(out_rows)

    annotation_cache_summary = {
        name: _annotation_cache_summary(layout, name)
        for name in ("uniprot", "alphafold_db", "reactome", "interpro", "pfam", "cath", "scop")
    }
    source_annotation_summary = {
        "generated_at": _utc_now(),
        "status": "ready" if out_rows else "empty",
        "row_count": len(out_rows),
        "annotation_summary": annotation_summary,
        "annotation_caches": annotation_cache_summary,
    }
    source_annotation_summary_json = layout.workspace_metadata_dir / "source_annotation_summary.json"
    source_annotation_summary_md = layout.workspace_metadata_dir / "source_annotation_summary.md"
    source_annotation_summary_json.write_text(
        json.dumps(source_annotation_summary, indent=2),
        encoding="utf-8",
    )
    md_lines = [
        "# Source Annotation Summary",
        "",
        f"- Status: `{source_annotation_summary['status']}`",
        f"- Rows harvested: `{len(out_rows)}`",
        f"- Requested UniProt IDs: `{annotation_summary['requested_uniprot_ids']}`",
        f"- Requested PDB IDs: `{annotation_summary['requested_pdb_ids']}`",
        "",
        "## Cache Freshness",
    ]
    for source_name, payload in annotation_cache_summary.items():
        md_lines.append(
            f"- `{source_name}`: freshness=`{payload['freshness']}`, "
            f"files=`{payload['file_count']}`, latest=`{payload['latest_mtime'] or 'n/a'}`"
        )
    if annotation_summary.get("errors"):
        md_lines.extend(["", "## Errors"])
        md_lines.extend(f"- {error}" for error in annotation_summary["errors"])
    source_annotation_summary_md.write_text("\n".join(md_lines) + "\n", encoding="utf-8")

    manifest = {
        "generated_at": _utc_now(),
        "status": "harvested" if out_rows else "empty",
        "row_count": len(out_rows),
        "annotation_summary": annotation_summary,
        "annotation_caches": annotation_cache_summary,
        "source_files": [
            str(layout.root / "master_pdb_pairs.csv"),
            str(layout.root / "master_pdb_repository.csv"),
            str(layout.extracted_dir / "chains"),
        ],
    }
    manifest_path = layout.workspace_metadata_dir / "metadata_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return {
        "metadata_csv": str(metadata_path),
        "manifest": str(manifest_path),
        "source_annotation_summary_json": str(source_annotation_summary_json),
        "source_annotation_summary_md": str(source_annotation_summary_md),
    }


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
