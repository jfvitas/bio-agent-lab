from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path
from typing import Any

from pbdata.storage import StorageLayout
from pbdata.table_io import read_dataframe


def build_feature_pipeline_stages(
    layout: StorageLayout,
    *,
    config: Any,
    rows: dict[str, list[dict[str, Any]]],
    helpers: dict[str, Any],
) -> list[tuple[str, list[str], Any]]:
    def stage1():
        entries = rows["entry"]
        chain_counts: dict[str, int] = defaultdict(int)
        for chain in rows["chain"]:
            pdb_id = str(chain.get("pdb_id") or "")
            if pdb_id:
                chain_counts[pdb_id] += 1
        resolved: list[dict[str, Any]] = []
        rejected: list[dict[str, Any]] = []
        for entry in entries:
            pdb_id = str(entry.get("pdb_id") or "")
            cif_path = str(entry.get("structure_file_cif_path") or "")
            if not pdb_id or not cif_path:
                rejected.append({"pdb_id": pdb_id, "reason": "missing_structure_reference"})
                continue
            if not Path(cif_path).exists():
                rejected.append({"pdb_id": pdb_id, "reason": "structure_file_missing_on_disk"})
                continue
            resolved.append(
                {
                    "record_id": pdb_id,
                    "pdb_id": pdb_id,
                    "structure_file_cif_path": cif_path,
                    "chain_count": chain_counts.get(pdb_id, 0),
                    "experimental_method": entry.get("experimental_method"),
                    "structure_resolution": entry.get("structure_resolution"),
                }
            )
        resolved_path = helpers["write_df"](layout.artifact_manifests_dir / f"{config.run_id}_stage1_resolved_records.parquet", resolved)
        rejected_path = helpers["write_df"](layout.artifact_manifests_dir / f"{config.run_id}_stage1_rejections.parquet", rejected)
        manifest_path = helpers["json_dump"](
            layout.artifact_manifests_dir / f"{config.run_id}_stage1_manifest.json",
            {
                "generated_at": helpers["utc_now"](),
                "status": "resolved",
                "resolved_count": len(resolved),
                "rejected_count": len(rejected),
            },
        )
        return len(entries), len(resolved), len(rejected), [str(resolved_path), str(rejected_path), str(manifest_path)], []

    def stage2():
        df = read_dataframe(layout.artifact_manifests_dir / f"{config.run_id}_stage1_resolved_records.parquet")
        outputs: list[str] = []
        warnings: list[str] = []
        attempted = len(df.index)
        succeeded = failed = 0
        for row in df.to_dict(orient="records"):
            pdb_id = str(row["pdb_id"])
            try:
                atom_rows, site_rows = helpers["site_candidates_from_structure"](pdb_id, Path(str(row["structure_file_cif_path"])))
                outputs.append(
                    str(
                        helpers["write_df"](
                            layout.prepared_structures_artifacts_dir / config.run_id / f"{pdb_id}.prepared.parquet",
                            [
                                {
                                    "record_id": pdb_id,
                                    "pdb_id": pdb_id,
                                    "structure_file_cif_path": row["structure_file_cif_path"],
                                    "chain_count": row.get("chain_count"),
                                    "atom_count": len(atom_rows),
                                    "site_count": len(site_rows),
                                }
                            ],
                        )
                    )
                )
                outputs.append(str(helpers["write_df"](layout.prepared_structures_artifacts_dir / config.run_id / f"{pdb_id}.sites.parquet", site_rows)))
                outputs.append(str(helpers["write_df"](layout.site_envs_artifacts_dir / config.run_id / f"{pdb_id}.atoms.parquet", atom_rows)))
                succeeded += 1
            except Exception as exc:
                failed += 1
                warnings.append(f"{pdb_id}: {exc}")
        outputs.append(
            str(
                helpers["json_dump"](
                    layout.artifact_manifests_dir / f"{config.run_id}_stage2_manifest.json",
                    {"generated_at": helpers["utc_now"](), "status": "prepared", "records": succeeded},
                )
            )
        )
        return attempted, succeeded, failed, outputs, warnings

    def stage3():
        prepared_dir = layout.prepared_structures_artifacts_dir / config.run_id
        attempted = succeeded = failed = 0
        outputs: list[str] = []
        warnings: list[str] = []
        for prepared_path in sorted(prepared_dir.glob("*.prepared.parquet")):
            pdb_id = prepared_path.stem.replace(".prepared", "")
            attempted += 1
            try:
                sites = read_dataframe(prepared_dir / f"{pdb_id}.sites.parquet").to_dict(orient="records")
                atoms = read_dataframe(layout.site_envs_artifacts_dir / config.run_id / f"{pdb_id}.atoms.parquet").to_dict(orient="records")
                env_rows, node_rows = helpers["shell_descriptor_rows"](pdb_id, sites, atoms)
                edge_rows = helpers["edge_rows_from_sites"](pdb_id, sites)
                outputs.extend(
                    [
                        str(helpers["write_df"](layout.base_features_artifacts_dir / config.run_id / f"{pdb_id}.env_vectors.parquet", env_rows)),
                        str(helpers["write_df"](layout.base_features_artifacts_dir / config.run_id / f"{pdb_id}.node_base.parquet", node_rows)),
                        str(helpers["write_df"](layout.base_features_artifacts_dir / config.run_id / f"{pdb_id}.edge_base.parquet", edge_rows)),
                        str(
                            helpers["json_dump"](
                                layout.base_features_artifacts_dir / config.run_id / f"{pdb_id}.global_base.json",
                                {
                                    "generated_at": helpers["utc_now"](),
                                    "record_id": pdb_id,
                                    "site_count": len(sites),
                                    "env_row_count": len(env_rows),
                                    "edge_candidate_count": len(edge_rows),
                                },
                            )
                        ),
                    ]
                )
                succeeded += 1
            except Exception as exc:
                failed += 1
                warnings.append(f"{pdb_id}: {exc}")
        return attempted, succeeded, failed, outputs, warnings

    def stage4():
        surrogate_model = None if config.degraded_mode else helpers["load_latest_site_physics_surrogate"](layout)
        if not config.degraded_mode and surrogate_model is None:
            raise RuntimeError("site_physics_enrichment requires a trained surrogate checkpoint when degraded_mode=false")
        attempted = succeeded = failed = 0
        outputs: list[str] = []
        warnings: list[str] = []
        for env_path in sorted((layout.base_features_artifacts_dir / config.run_id).glob("*.env_vectors.parquet")):
            pdb_id = env_path.stem.replace(".env_vectors", "")
            attempted += 1
            try:
                env_rows = read_dataframe(env_path).to_dict(orient="records")
                if surrogate_model is not None:
                    refined_rows, provenance_rows, cache_stats = helpers["site_refined_rows_from_surrogate"](env_rows, surrogate_model)
                else:
                    refined_rows, provenance_rows, cache_stats = helpers["site_refined_rows"](env_rows, degraded_mode=config.degraded_mode)
                outputs.extend(
                    [
                        str(helpers["write_df"](layout.site_physics_artifacts_dir / config.run_id / f"{pdb_id}.site_refined.parquet", refined_rows)),
                        str(helpers["write_df"](layout.site_physics_artifacts_dir / config.run_id / f"{pdb_id}.physics_provenance.parquet", provenance_rows)),
                        str(helpers["json_dump"](layout.site_physics_artifacts_dir / config.run_id / f"{pdb_id}.cache_stats.json", cache_stats)),
                    ]
                )
                succeeded += 1
            except Exception as exc:
                failed += 1
                warnings.append(f"{pdb_id}: {exc}")
        outputs.append(
            str(
                helpers["json_dump"](
                    layout.artifact_caches_dir / f"{config.run_id}_cache_manifest.json",
                    {
                        "generated_at": helpers["utc_now"](),
                        "schema_version": helpers["schema_version"],
                        "feature_pipeline_version": helpers["pipeline_version"],
                        "motif_taxonomy_version": helpers["site_physics_spec_version"],
                        "surrogate_checkpoint_id": None if surrogate_model is None else str(surrogate_model.get("version") or "site_physics_surrogate"),
                        "graph_representation_version": helpers["graph_representation_version"],
                        "training_example_version": helpers["training_example_version"],
                        "degraded_mode": config.degraded_mode,
                    },
                )
            )
        )
        return attempted, succeeded, failed, outputs, warnings

    def stage5():
        attempted = succeeded = failed = 0
        outputs: list[str] = []
        warnings: list[str] = []
        base_dir = layout.base_features_artifacts_dir / config.run_id
        site_dir = layout.site_physics_artifacts_dir / config.run_id
        for node_path in sorted(base_dir.glob("*.node_base.parquet")):
            pdb_id = node_path.stem.replace(".node_base", "")
            attempted += 1
            try:
                graph_nodes, graph_edges, meta = helpers["graph_rows"](
                    read_dataframe(node_path).to_dict(orient="records"),
                    read_dataframe(base_dir / f"{pdb_id}.edge_base.parquet").to_dict(orient="records"),
                    read_dataframe(site_dir / f"{pdb_id}.site_refined.parquet").to_dict(orient="records"),
                )
                graph_pt = layout.graphs_artifacts_dir / config.run_id / f"{pdb_id}.graph.pt"
                graph_pt.parent.mkdir(parents=True, exist_ok=True)
                graph_payload = {"nodes": graph_nodes, "edges": graph_edges, "meta": meta}
                if helpers["torch"] is not None:
                    helpers["torch"].save(graph_payload, graph_pt)
                else:
                    graph_pt.write_text(json.dumps(graph_payload, indent=2), encoding="utf-8")
                outputs.extend(
                    [
                        str(helpers["write_df"](layout.graphs_artifacts_dir / config.run_id / f"{pdb_id}.nodes.parquet", graph_nodes)),
                        str(helpers["write_df"](layout.graphs_artifacts_dir / config.run_id / f"{pdb_id}.edges.parquet", graph_edges)),
                        str(helpers["json_dump"](layout.graphs_artifacts_dir / config.run_id / f"{pdb_id}.graph_meta.json", meta)),
                        str(graph_pt),
                    ]
                )
                succeeded += 1
            except Exception as exc:
                failed += 1
                warnings.append(f"{pdb_id}: {exc}")
        return attempted, succeeded, failed, outputs, warnings

    def stage6():
        pairs_by_pdb: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in rows["assay"]:
            pdb_id = str(row.get("pdb_id") or "")
            if pdb_id:
                pairs_by_pdb[pdb_id].append(row)
        attempted = succeeded = failed = 0
        outputs: list[str] = []
        warnings: list[str] = []
        manifest_rows: list[dict[str, Any]] = []
        for graph_meta_path in sorted((layout.graphs_artifacts_dir / config.run_id).glob("*.graph_meta.json")):
            pdb_id = graph_meta_path.stem.replace(".graph_meta", "")
            attempted += 1
            try:
                graph_meta = json.loads(graph_meta_path.read_text(encoding="utf-8"))
                examples, labels, metas = helpers["training_example_rows"](
                    pdb_id,
                    pairs_by_pdb.get(pdb_id, []),
                    graph_meta,
                    config.degraded_mode,
                )
                for index, meta in enumerate(metas):
                    record_id = str(meta["record_id"])
                    if index < len(examples):
                        example_path = layout.training_examples_artifacts_dir / config.run_id / f"{record_id}_{index}.example.pt"
                        example_path.parent.mkdir(parents=True, exist_ok=True)
                        if helpers["torch"] is not None:
                            helpers["torch"].save(examples[index], example_path)
                        else:
                            example_path.write_text(json.dumps(examples[index], indent=2), encoding="utf-8")
                        outputs.append(str(example_path))
                    if index < len(labels):
                        outputs.append(
                            str(
                                helpers["json_dump"](
                                    layout.training_examples_artifacts_dir / config.run_id / f"{record_id}_{index}.label.json",
                                    labels[index],
                                )
                            )
                        )
                    outputs.append(
                        str(
                            helpers["json_dump"](
                                layout.training_examples_artifacts_dir / config.run_id / f"{record_id}_{index}.meta.json",
                                meta,
                            )
                        )
                    )
                    manifest_rows.append(
                        {
                            "record_id": record_id,
                            "example_id": meta.get("example_id"),
                            "supervised_label_available": bool(meta.get("supervised_label_available")),
                            "task_type": meta.get("task_type"),
                            "degraded_mode": bool(meta.get("degraded_mode")),
                        }
                    )
                succeeded += 1
            except Exception as exc:
                failed += 1
                warnings.append(f"{pdb_id}: {exc}")
        outputs.append(str(helpers["write_df"](layout.training_examples_artifacts_dir / config.run_id / "manifest.parquet", manifest_rows)))
        return attempted, succeeded, failed, outputs, warnings

    def stage7():
        payloads = [
            payload
            for name in (
                "canonical_input_resolution",
                "structure_preparation",
                "base_feature_extraction",
                "site_physics_enrichment",
                "graph_construction",
                "training_example_assembly",
            )
            if (payload := helpers["load_stage_status"](layout, config.run_id, name))
        ]
        summary = [
            f"# Feature Pipeline Summary: {config.run_id}",
            "",
            f"- Pipeline version: {helpers['pipeline_version']}",
            f"- Run mode: {config.run_mode}",
            f"- Degraded mode: {config.degraded_mode}",
            "",
            "## Stage outcomes",
        ]
        for payload in payloads:
            attempted = int(payload["records_attempted"])
            succeeded = int(payload["records_succeeded"])
            if attempted == 0 and str(payload["status"]) == "passed":
                line = f"- {payload['stage_name']}: passed (0 records; upstream input was empty)"
            else:
                line = f"- {payload['stage_name']}: {payload['status']} ({succeeded}/{attempted} succeeded)"
            summary.append(line)
        summary_path = layout.feature_reports_dir / f"{config.run_id}_summary.md"
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path.write_text("\n".join(summary), encoding="utf-8")
        coverage = helpers["json_dump"](
            layout.feature_reports_dir / f"{config.run_id}_coverage.json",
            {
                "run_id": config.run_id,
                "stages": {payload["stage_name"]: payload["records_succeeded"] for payload in payloads},
                "degraded_mode": config.degraded_mode,
            },
        )
        failures = helpers["json_dump"](
            layout.feature_reports_dir / f"{config.run_id}_failures.json",
            {
                "run_id": config.run_id,
                "failed_stages": [payload for payload in payloads if payload.get("status") in {"failed", "partial"}],
            },
        )
        performance = helpers["json_dump"](
            layout.feature_reports_dir / f"{config.run_id}_performance.json",
            {
                "run_id": config.run_id,
                "gpu_unavailable": not config.gpu_enabled,
                "stage_statuses": {payload["stage_name"]: payload["status"] for payload in payloads},
            },
        )
        return 1, 1, 0, [str(summary_path), str(coverage), str(failures), str(performance)], []

    return [
        ("canonical_input_resolution", [], stage1),
        ("structure_preparation", ["canonical_input_resolution"], stage2),
        ("base_feature_extraction", ["structure_preparation"], stage3),
        ("site_physics_enrichment", ["base_feature_extraction"], stage4),
        ("graph_construction", ["site_physics_enrichment"], stage5),
        ("training_example_assembly", ["graph_construction"], stage6),
        ("validation_reporting_export", ["training_example_assembly"], stage7),
    ]
