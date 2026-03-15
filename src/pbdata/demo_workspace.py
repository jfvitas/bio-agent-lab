"""Seed a simulated workspace so Demo Mode can showcase intended platform behavior."""

from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pbdata.config import AppConfig
from pbdata.model_comparison import export_model_comparison_report
from pbdata.storage import StorageLayout


@dataclass(frozen=True)
class DemoWorkspaceSeedResult:
    manifest_path: Path
    report_path: Path
    walkthrough_path: Path
    seeded: bool


def demo_manifest_path(layout: StorageLayout) -> Path:
    return layout.data_dir / "demo" / "demo_workspace_manifest.json"


def is_demo_workspace_seeded(layout: StorageLayout) -> bool:
    return demo_manifest_path(layout).exists()


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames: list[str] = []
    for row in rows:
        for key in row:
            if key not in fieldnames:
                fieldnames.append(key)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _line_chart_svg(title: str, points: list[tuple[float, float]], *, stroke: str) -> str:
    width = 720
    height = 260
    pad = 34
    if not points:
        polyline = ""
    else:
        xs = [point[0] for point in points]
        ys = [point[1] for point in points]
        min_x, max_x = min(xs), max(xs)
        min_y, max_y = min(ys), max(ys)
        span_x = max(max_x - min_x, 1.0)
        span_y = max(max_y - min_y, 1.0)
        coords: list[str] = []
        for x_value, y_value in points:
            x = pad + ((x_value - min_x) / span_x) * (width - pad * 2)
            y = height - pad - ((y_value - min_y) / span_y) * (height - pad * 2)
            coords.append(f"{x:.1f},{y:.1f}")
        polyline = " ".join(coords)
    return (
        f"<svg xmlns='http://www.w3.org/2000/svg' width='{width}' height='{height}' viewBox='0 0 {width} {height}'>"
        "<rect width='100%' height='100%' fill='#f8fbff'/>"
        f"<text x='{pad}' y='22' font-size='16' font-family='Segoe UI' fill='#0f172a'>{title}</text>"
        f"<line x1='{pad}' y1='{height-pad}' x2='{width-pad}' y2='{height-pad}' stroke='#cbd5e1' stroke-width='1'/>"
        f"<line x1='{pad}' y1='{pad}' x2='{pad}' y2='{height-pad}' stroke='#cbd5e1' stroke-width='1'/>"
        f"<polyline fill='none' stroke='{stroke}' stroke-width='3' points='{polyline}'/>"
        "</svg>"
    )


def _bar_chart_svg(title: str, bars: list[tuple[str, float]], *, fill: str) -> str:
    width = 720
    height = 260
    pad = 34
    bar_width = max((width - pad * 2) / max(len(bars), 1) - 12, 20)
    max_value = max((value for _, value in bars), default=1.0)
    bar_parts: list[str] = []
    for index, (label, value) in enumerate(bars):
        x = pad + index * (bar_width + 12)
        bar_height = ((value / max_value) * (height - pad * 2)) if max_value else 0
        y = height - pad - bar_height
        bar_parts.append(
            f"<rect x='{x:.1f}' y='{y:.1f}' width='{bar_width:.1f}' height='{bar_height:.1f}' rx='4' fill='{fill}'/>"
        )
        bar_parts.append(
            f"<text x='{x + bar_width / 2:.1f}' y='{height - 12}' text-anchor='middle' font-size='10' font-family='Segoe UI' fill='#334155'>{label}</text>"
        )
    return (
        f"<svg xmlns='http://www.w3.org/2000/svg' width='{width}' height='{height}' viewBox='0 0 {width} {height}'>"
        "<rect width='100%' height='100%' fill='#f8fbff'/>"
        f"<text x='{pad}' y='22' font-size='16' font-family='Segoe UI' fill='#0f172a'>{title}</text>"
        f"<line x1='{pad}' y1='{height-pad}' x2='{width-pad}' y2='{height-pad}' stroke='#cbd5e1' stroke-width='1'/>"
        + "".join(bar_parts)
        + "</svg>"
    )


def _demo_raw_records() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for idx in range(1, 65):
        pdb_id = f"D{idx:03d}"
        rows.append(
            {
                "pdb_id": pdb_id,
                "title": f"Demo protein complex {idx}",
                "experimental_method": "xray" if idx % 3 else "em",
                "resolution": round(1.4 + (idx % 7) * 0.18, 2),
                "protein_entities": 1 + (idx % 3),
                "ligand_entities": idx % 4,
                "source": "rcsb",
            }
        )
    return rows


def _demo_training_examples() -> list[dict[str, Any]]:
    examples: list[dict[str, Any]] = []
    for idx in range(1, 49):
        pdb_id = f"D{idx:03d}"
        affinity = round(5.0 + (idx % 9) * 0.27, 3)
        examples.append(
            {
                "example_id": f"demo:{pdb_id}:pair_{idx:03d}",
                "structure": {"pdb_id": pdb_id, "resolution": round(1.5 + (idx % 6) * 0.15, 2)},
                "protein": {
                    "uniprot_id": f"P{10000 + idx}",
                    "gene": f"GENE{idx:03d}",
                    "sequence_length": 180 + idx,
                    "family": ["kinase", "gpcr", "transferase", "enzyme"][idx % 4],
                },
                "ligand": {
                    "ligand_id": f"LIG{idx:03d}",
                    "ligand_type": "drug_like" if idx % 2 else "fragment",
                    "molecular_weight": 240.0 + idx * 3.1,
                },
                "interaction": {
                    "interface_residue_count": 12 + (idx % 11),
                    "microstate_record_count": 2 + (idx % 4),
                    "pathway_count": 1 + (idx % 3),
                },
                "experiment": {
                    "affinity_type": "Kd" if idx % 2 else "Ki",
                    "source_database": "demo_merge",
                    "preferred_source_database": "rcsb",
                    "reported_measurement_count": 1 + (idx % 3),
                },
                "labels": {
                    "binding_affinity_log10": affinity,
                    "affinity_type": "Kd" if idx % 2 else "Ki",
                    "source_conflict_flag": idx % 7 == 0,
                    "is_mutant": idx % 5 == 0,
                },
                "quality_flags": [] if idx % 9 else ["needs_manual_review"],
                "provenance": {"pdb_id": pdb_id, "sources": ["rcsb", "bindingdb", "chembl"]},
            }
        )
    return examples


def _demo_feature_records(examples: list[dict[str, Any]]) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for idx, example in enumerate(examples, start=1):
        protein = example.get("protein") or {}
        ligand = example.get("ligand") or {}
        interaction = example.get("interaction") or {}
        records.append(
            {
                "example_id": example["example_id"],
                "pdb_id": example["structure"]["pdb_id"],
                "feature_vector": {
                    "structure_resolution": example["structure"]["resolution"],
                    "sequence_length": protein.get("sequence_length"),
                    "protein_mean_hydropathy": round(0.18 + idx * 0.003, 3),
                    "protein_aromatic_fraction": round(0.08 + (idx % 6) * 0.01, 3),
                    "ligand_molecular_weight": ligand.get("molecular_weight"),
                    "interface_residue_count": interaction.get("interface_residue_count"),
                    "network_degree": 1 + (idx % 6),
                    "ppi_degree": idx % 4,
                    "pli_degree": 1 + (idx % 5),
                },
            }
        )
    return records


def _demo_graph_records(examples: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    nodes: list[dict[str, Any]] = []
    edges: list[dict[str, Any]] = []
    for idx, example in enumerate(examples, start=1):
        pdb_id = example["structure"]["pdb_id"]
        protein_node = f"{pdb_id}:protein"
        ligand_node = f"{pdb_id}:ligand"
        nodes.extend(
            [
                {"node_id": protein_node, "pdb_id": pdb_id, "kind": "protein", "feature_dim": 16},
                {"node_id": ligand_node, "pdb_id": pdb_id, "kind": "ligand", "feature_dim": 8},
            ]
        )
        edges.append(
            {
                "source_node_id": protein_node,
                "target_node_id": ligand_node,
                "pdb_id": pdb_id,
                "kind": "binding_contact",
                "weight": round(0.6 + (idx % 5) * 0.05, 3),
            }
        )
    return nodes, edges


def _split_membership(examples: list[dict[str, Any]]) -> tuple[list[str], list[str], list[str]]:
    train: list[str] = []
    val: list[str] = []
    test: list[str] = []
    for idx, example in enumerate(examples):
        target = train if idx < 30 else val if idx < 39 else test
        target.append(str(example["example_id"]))
    return train, val, test


def _seed_stage_state(layout: StorageLayout) -> None:
    stage_rows = [
        ("ingest", "completed"),
        ("extract", "completed"),
        ("build-graph", "completed"),
        ("build-features", "completed"),
        ("build-training-examples", "completed"),
        ("build-splits", "completed"),
        ("evaluate-baseline-model", "completed"),
        ("build-release", "completed"),
    ]
    for stage_name, status in stage_rows:
        _write_json(
            layout.stage_state_dir / f"{stage_name}.json",
            {
                "stage": stage_name,
                "status": status,
                "generated_at": "2026-03-14T16:30:00+00:00",
                "notes": f"Demo mode simulated stage output for {stage_name}.",
            },
        )


def _seed_model_runs(layout: StorageLayout) -> list[dict[str, Any]]:
    runs_root = layout.models_dir / "model_studio" / "runs"
    runs_root.mkdir(parents=True, exist_ok=True)
    run_specs = [
        {
            "run_name": "demo_xgboost_affinity",
            "family": "xgboost",
            "task": "regression",
            "backend": "sklearn_hist_gradient_boosting",
            "runtime_target": "local_cpu",
            "metric_name": "rmse",
            "metric_value": 0.612,
            "history": [
                {"epoch": 1, "train_metric": 0.92, "val_metric": 0.88},
                {"epoch": 2, "train_metric": 0.78, "val_metric": 0.73},
                {"epoch": 3, "train_metric": 0.68, "val_metric": 0.64},
            ],
            "test_bar_values": [("RMSE", 0.61), ("MAE", 0.43), ("R2", 0.78)],
        },
        {
            "run_name": "demo_pyg_hybrid_affinity",
            "family": "hybrid_fusion",
            "task": "regression",
            "backend": "pyg_hybrid_fusion",
            "runtime_target": "local_gpu",
            "metric_name": "rmse",
            "metric_value": 0.471,
            "history": [
                {"epoch": 1, "train_metric": 0.83, "val_metric": 0.79},
                {"epoch": 2, "train_metric": 0.66, "val_metric": 0.58},
                {"epoch": 3, "train_metric": 0.53, "val_metric": 0.50},
                {"epoch": 4, "train_metric": 0.47, "val_metric": 0.46},
            ],
            "test_bar_values": [("RMSE", 0.47), ("MAE", 0.31), ("R2", 0.84)],
        },
    ]
    seeded: list[dict[str, Any]] = []
    for spec in run_specs:
        run_dir = runs_root / spec["run_name"]
        run_dir.mkdir(parents=True, exist_ok=True)
        history = spec["history"]
        metrics = {
            "family": spec["family"],
            "task": spec["task"],
            "train": {"rmse": history[-1]["train_metric"]},
            "val": {"rmse": history[-1]["val_metric"]},
            "test": {"rmse": spec["metric_value"], "mae": round(spec["metric_value"] * 0.68, 3)},
        }
        split_summary = {"train": {"count": 30}, "val": {"count": 9}, "test": {"count": 9}}
        predictions = [
            {
                "example_id": f"demo:D{40 + idx:03d}:pair_{40 + idx:03d}",
                "actual": round(5.4 + idx * 0.11, 3),
                "predicted": round(5.35 + idx * 0.11 + (0.04 if idx % 2 else -0.03), 3),
            }
            for idx in range(1, 10)
        ]
        _write_json(run_dir / "metrics.json", metrics)
        _write_json(run_dir / "history.json", history)
        _write_json(run_dir / "split_summary.json", split_summary)
        _write_json(run_dir / "test_predictions.json", predictions)
        _write_json(
            run_dir / "config.json",
            {
                "family": spec["family"],
                "task": spec["task"],
                "trainer_backend": spec["backend"],
                "model": {"type": spec["family"]},
                "backend_plan": {
                    "requested_family": spec["family"],
                    "execution_family": spec["family"],
                    "backend_id": spec["backend"],
                    "implementation": "native" if str(spec["backend"]).startswith("pyg_") else "fallback",
                    "native_graph": str(spec["backend"]).startswith("pyg_"),
                },
            },
        )
        _write_json(
            run_dir / "run_manifest.json",
            {
                "generated_at": "2026-03-14T16:30:00+00:00",
                "run_name": spec["run_name"],
                "family": spec["family"],
                "task": spec["task"],
                "backend": spec["backend"],
                "runtime_target": spec["runtime_target"],
                "backend_plan": {
                    "requested_family": spec["family"],
                    "execution_family": spec["family"],
                    "backend_id": spec["backend"],
                    "implementation": "native" if str(spec["backend"]).startswith("pyg_") else "fallback",
                    "native_graph": str(spec["backend"]).startswith("pyg_"),
                },
                "warnings": [
                    "demo_mode_simulated_run",
                ],
            },
        )
        _write_text(
            run_dir / "training_curve.svg",
            _line_chart_svg(
                f"{spec['run_name']} validation curve",
                [(float(row["epoch"]), float(row["val_metric"])) for row in history],
                stroke="#0f766e",
            ),
        )
        _write_text(
            run_dir / "test_performance.svg",
            _bar_chart_svg(
                f"{spec['run_name']} held-out metrics",
                [(label, float(value)) for label, value in spec["test_bar_values"]],
                fill="#2563eb",
            ),
        )
        seeded.append({"run_name": spec["run_name"], "metric": spec["metric_value"]})
    return seeded


def seed_demo_workspace(
    layout: StorageLayout,
    config: AppConfig,
    *,
    repo_root: Path,
    force: bool = False,
) -> DemoWorkspaceSeedResult:
    manifest_path = demo_manifest_path(layout)
    if manifest_path.exists() and not force:
        report_path = layout.feature_reports_dir / "demo_readiness.json"
        walkthrough_path = layout.feature_reports_dir / "demo_walkthrough.md"
        return DemoWorkspaceSeedResult(
            manifest_path=manifest_path,
            report_path=report_path,
            walkthrough_path=walkthrough_path,
            seeded=False,
        )

    raw_records = _demo_raw_records()
    training_examples = _demo_training_examples()
    feature_records = _demo_feature_records(training_examples)
    graph_nodes, graph_edges = _demo_graph_records(training_examples)
    train_ids, val_ids, test_ids = _split_membership(training_examples)

    for record in raw_records:
        pdb_id = str(record["pdb_id"])
        _write_json(layout.raw_rcsb_dir / f"{pdb_id}.json", record)
        _write_text(layout.structures_rcsb_dir / f"{pdb_id}.cif", f"data_{pdb_id}\n# demo structure placeholder\n")

    table_rows = {
        "entry": [
            {
                "pdb_id": example["structure"]["pdb_id"],
                "title": f"Demo entry for {example['structure']['pdb_id']}",
                "membrane_vs_soluble": "soluble",
                "metal_present": "no",
                "cofactor_present": "yes" if idx % 4 == 0 else "no",
                "glycan_present": "yes" if idx % 6 == 0 else "no",
                "quality_flags": ",".join(example.get("quality_flags") or []),
                "field_confidence_json": json.dumps({"structure_resolution": "high", "ligand_assignment": "high"}),
            }
            for idx, example in enumerate(training_examples, start=1)
        ],
        "chains": [
            {"pdb_id": example["structure"]["pdb_id"], "chain_id": "A", "entity_type": "protein"}
            for example in training_examples
        ],
        "bound_objects": [
            {
                "pdb_id": example["structure"]["pdb_id"],
                "object_id": example["ligand"]["ligand_id"],
                "object_type": "ligand",
            }
            for example in training_examples
        ],
        "interfaces": [
            {
                "pdb_id": example["structure"]["pdb_id"],
                "interface_id": f"{example['structure']['pdb_id']}_if",
                "interface_residue_count": example["interaction"]["interface_residue_count"],
            }
            for example in training_examples
        ],
        "assays": [
            {
                "pdb_id": example["structure"]["pdb_id"],
                "pair_identity_key": f"protein_ligand|{example['structure']['pdb_id']}|A|{example['ligand']['ligand_id']}|wt",
                "binding_affinity_log10": example["labels"]["binding_affinity_log10"],
                "assay_field_confidence_json": json.dumps({"binding_affinity_log10": "high"}),
            }
            for example in training_examples
        ],
        "provenance": [
            {"pdb_id": example["structure"]["pdb_id"], "sources": example["provenance"]["sources"]}
            for example in training_examples
        ],
    }
    for table_name, rows in table_rows.items():
        for row in rows:
            _write_json(layout.extracted_dir / table_name / f"{row['pdb_id']}.json", row)

    _write_json(layout.graph_dir / "graph_nodes.json", graph_nodes)
    _write_json(layout.graph_dir / "graph_edges.json", graph_edges)
    _write_json(layout.features_dir / "feature_records.json", feature_records)
    _write_json(
        layout.features_dir / "feature_manifest.json",
        {
            "status": "ready",
            "record_count": len(feature_records),
            "feature_groups": ["structure", "protein", "ligand", "interaction", "graph_features"],
            "simulated": True,
        },
    )
    _write_json(layout.training_dir / "training_examples.json", training_examples)
    _write_text(layout.splits_dir / "train.txt", "\n".join(train_ids) + "\n")
    _write_text(layout.splits_dir / "val.txt", "\n".join(val_ids) + "\n")
    _write_text(layout.splits_dir / "test.txt", "\n".join(test_ids) + "\n")
    _write_json(
        layout.splits_dir / "metadata.json",
        {
            "status": "ready",
            "split_strategy": "demo_family_balanced_holdout",
            "counts": {"train": len(train_ids), "val": len(val_ids), "test": len(test_ids)},
            "notes": "Simulated split summary for demonstration mode only.",
        },
    )
    _write_json(
        layout.splits_dir / "split_diagnostics.json",
        {
            "status": "ready",
            "largest_group_fraction": 0.23,
            "novel_receptor_fraction": 0.18,
            "leakage_risk": "low",
        },
    )
    _write_text(
        layout.splits_dir / "split_diagnostics.md",
        "# Split Diagnostics\n\n- Demo-only leakage diagnostics.\n- Largest held-out family fraction: 23%.\n- Intended to showcase the reporting surface, not scientific conclusions.\n",
    )

    _write_csv(
        layout.workspace_metadata_dir / "protein_metadata.csv",
        [
            {
                "pdb_id": example["structure"]["pdb_id"],
                "uniprot_id": example["protein"]["uniprot_id"],
                "gene": example["protein"]["gene"],
                "family": example["protein"]["family"],
            }
            for example in training_examples
        ],
    )

    _write_json(layout.models_dir / "ligand_memory_model.json", {"status": "ready", "simulated": True})
    _write_json(
        layout.models_dir / "ligand_memory_evaluation.json",
        {
            "splits": {
                "val": {"top1_target_accuracy": 0.64, "affinity_mae_log10": 0.81, "novel_top1_target_accuracy": 0.52},
                "test": {"top1_target_accuracy": 0.61, "affinity_mae_log10": 0.84, "novel_top1_target_accuracy": 0.49},
            }
        },
    )
    _write_json(
        layout.models_dir / "tabular_affinity_evaluation.json",
        {
            "splits": {
                "val": {"affinity_mae_log10": 0.55, "affinity_rmse_log10": 0.68, "novel_case_count": 12},
                "test": {"affinity_mae_log10": 0.57, "affinity_rmse_log10": 0.71, "novel_case_count": 11},
            }
        },
    )
    export_model_comparison_report(layout)

    _write_json(
        layout.identity_dir / "identity_crosswalk_summary.json",
        {
            "status": "ready",
            "entity_count": 164,
            "canonical_pair_count": len(training_examples),
            "conflict_count": 3,
        },
    )
    _write_csv(
        layout.identity_dir / "protein_identity_crosswalk.csv",
        [
            {
                "protein_key": example["protein"]["uniprot_id"],
                "canonical_protein_id": example["protein"]["uniprot_id"],
                "gene": example["protein"]["gene"],
            }
            for example in training_examples[:20]
        ],
    )
    _write_csv(
        layout.identity_dir / "ligand_identity_crosswalk.csv",
        [
            {
                "ligand_key": example["ligand"]["ligand_id"],
                "canonical_ligand_id": example["ligand"]["ligand_id"],
                "ligand_type": example["ligand"]["ligand_type"],
            }
            for example in training_examples[:20]
        ],
    )
    _write_csv(
        layout.identity_dir / "pair_identity_crosswalk.csv",
        [
            {
                "pair_identity_key": f"protein_ligand|{example['structure']['pdb_id']}|A|{example['ligand']['ligand_id']}|wt",
                "canonical_pair_id": f"pair_{idx:03d}",
                "pdb_id": example["structure"]["pdb_id"],
            }
            for idx, example in enumerate(training_examples[:20], start=1)
        ],
    )
    _write_json(
        layout.reports_dir / "rcsb_search_preview.json",
        {
            "status": "ready",
            "summary": "Demo workspace includes a representative cross-section of structural biology cases across protein-ligand and protein-protein motifs.",
            "result_count": len(raw_records),
            "example_ids": [row["pdb_id"] for row in raw_records[:6]],
            "representative_sampling": True,
        },
    )
    _write_json(
        layout.reports_dir / "extract_source_run_summary.json",
        {
            "status": "ready",
            "summary": "Observed 196 source attempt(s) across 5 source(s); 412 record(s) were normalized into the demo extract bundle.",
            "source_count": 5,
            "total_attempt_count": 196,
            "total_records_observed": 412,
            "aggregate_mode_counts": {"managed_cache": 128, "live_api": 68},
        },
    )
    _write_text(
        layout.reports_dir / "extract_source_run_summary.md",
        "# Extract Source Summary\n\nDemo-only simulated source procurement summary for the presenter workflow.\n",
    )
    _write_json(
        layout.reports_dir / "source_capabilities.json",
        {
            "status": "ready",
            "counts": {"enabled_sources": 5, "local_sources": 2, "live_sources": 3},
            "summary": "Demo source capability report spans structural, enrichment, and local reference sources.",
        },
    )
    _write_text(
        layout.reports_dir / "source_capabilities.md",
        "# Source Capabilities\n\n- RCSB: structural backbone\n- BindingDB/ChEMBL: enrichment\n- PDBbind/BioLiP: local reference-style inputs\n",
    )
    _write_json(
        layout.prediction_dir / "ligand_screening" / "prediction_manifest.json",
        {
            "status": "trained_supervised_predictions_generated",
            "prediction_method": "demo_hybrid_model",
            "selected_model_preference": "hybrid_fusion",
            "candidate_target_count": 128,
            "query_numeric_feature_count": 18,
            "ranked_target_list": [
                {"target_id": "P12345", "confidence_score": 0.88},
                {"target_id": "Q54321", "confidence_score": 0.81},
            ],
            "notes": "Demo mode simulated output. Values are illustrative only.",
        },
    )
    _write_json(
        layout.risk_dir / "pathway_risk_summary.json",
        {
            "status": "ready",
            "summary": "Top-risk pathways have been summarized for demonstration of downstream biological review panels.",
            "high_risk_pathway_count": 3,
            "medium_risk_pathway_count": 7,
        },
    )
    _write_json(
        layout.releases_dir / "latest_release.json",
        {
            "release_id": "demo_release_20260314",
            "status": "ready",
            "example_count": len(training_examples),
            "notes": "Demo-mode simulated release snapshot.",
        },
    )
    _write_json(
        layout.artifact_manifests_dir / "demo_site_pipeline_input_manifest.json",
        {
            "run_id": "demo_site_pipeline",
            "status": "completed",
            "notes": "Simulated site-centric feature pipeline manifest for Demo Mode.",
        },
    )
    _write_json(
        layout.qa_dir / "scenario_test_report.json",
        {
            "status": "scenario_templates_executed",
            "scenario_count": 3,
            "passed": 3,
            "warnings": [],
        },
    )
    _write_json(
        layout.qa_dir / "scenario_test_manifest.json",
        {
            "status": "ready",
            "report": str(layout.qa_dir / "scenario_test_report.json"),
        },
    )

    _seed_stage_state(layout)
    seeded_runs = _seed_model_runs(layout)

    _write_csv(
        repo_root / "master_pdb_repository.csv",
        [{"pdb_id": row["pdb_id"], "title": row["title"], "status": "ready"} for row in raw_records[:48]],
    )
    _write_csv(
        repo_root / "master_pdb_pairs.csv",
        [
            {
                "pdb_id": example["structure"]["pdb_id"],
                "pair_identity_key": f"protein_ligand|{example['structure']['pdb_id']}|A|{example['ligand']['ligand_id']}|wt",
                "binding_affinity_type": example["labels"]["affinity_type"],
                "source_conflict_flag": str(example["labels"]["source_conflict_flag"]).lower(),
            }
            for example in training_examples
        ],
    )
    _write_csv(
        repo_root / "master_pdb_issues.csv",
        [
            {"pdb_id": "D009", "pair_identity_key": "", "issue_type": "non_high_confidence_fields", "details": "Demo quality follow-up."},
            {"pdb_id": "D021", "pair_identity_key": "", "issue_type": "source_value_conflict", "details": "Demo-only source disagreement."},
        ],
    )
    _write_csv(
        repo_root / "master_pdb_conflicts.csv",
        [
            {"pdb_id": "D021", "pair_identity_key": "protein_ligand|D021|A|LIG021|wt", "conflict_summary": "Demo-only conflict summary."},
        ],
    )
    _write_csv(
        repo_root / "master_source_state.csv",
        [
            {"source": "rcsb", "enabled": "true", "mode": "managed_cache"},
            {"source": "bindingdb", "enabled": "true", "mode": "live_api"},
            {"source": "chembl", "enabled": "true", "mode": "live_api"},
        ],
    )
    _write_csv(
        repo_root / "model_ready_pairs.csv",
        [
            {
                "pdb_id": example["structure"]["pdb_id"],
                "pair_identity_key": f"protein_ligand|{example['structure']['pdb_id']}|A|{example['ligand']['ligand_id']}|wt",
            }
            for example in training_examples[:36]
        ],
    )
    _write_csv(
        repo_root / "custom_training_set.csv",
        [
            {
                "example_id": example["example_id"],
                "pdb_id": example["structure"]["pdb_id"],
                "binding_affinity_log10": example["labels"]["binding_affinity_log10"],
            }
            for example in training_examples[:32]
        ],
    )
    _write_csv(
        repo_root / "custom_training_exclusions.csv",
        [
            {"example_id": "demo:D009:pair_009", "reason": "held_out_for_demo_diversity"},
            {"example_id": "demo:D021:pair_021", "reason": "source_conflict_demo_case"},
        ],
    )
    _write_csv(
        repo_root / "custom_training_split_benchmark.csv",
        [
            {"benchmark_mode": "receptor_family", "largest_group_fraction": "0.23"},
            {"benchmark_mode": "source_holdout", "largest_group_fraction": "0.18"},
        ],
    )
    _write_csv(
        repo_root / "split_summary.csv",
        [
            {"split": "train", "count": len(train_ids)},
            {"split": "val", "count": len(val_ids)},
            {"split": "test", "count": len(test_ids)},
        ],
    )
    _write_json(
        repo_root / "custom_training_summary.json",
        {
            "status": "ready",
            "selected_count": 32,
            "candidate_pool_count": 48,
        },
    )
    _write_json(
        repo_root / "custom_training_scorecard.json",
        {
            "selected_count": 32,
            "candidate_pool_count": 48,
            "diversity": {"selected_receptor_clusters": 14, "selected_pair_families": 11},
            "quality": {"mean_quality_score": 0.92},
            "exclusions": {"count": 2},
        },
    )
    _write_json(
        repo_root / "dataset_release_manifest.json",
        {
            "release_id": "demo_release_20260314",
            "status": "ready",
            "selected_example_count": len(training_examples),
        },
    )
    _write_json(
        repo_root / "scientific_coverage_summary.json",
        {
            "counts": {
                "entry_count": len(raw_records),
                "pair_count": len(training_examples),
                "model_ready_pair_count": 36,
                "pairs_with_source_conflicts": 1,
                "entries_with_structure_file": len(raw_records),
            },
            "release": {"model_ready_exclusion_count": 2},
            "coverage": {"issue_types": {"missing_structure_file": 0, "non_high_confidence_fields": 1, "non_high_confidence_assay_fields": 1}},
        },
    )
    _write_json(
        repo_root / "release_readiness_report.json",
        {
            "status": "ready",
            "summary": "Demo workspace release checks passed for presenter walkthrough purposes.",
        },
    )

    from pbdata.demo import export_demo_snapshot

    report_path, walkthrough_path, _report = export_demo_snapshot(layout, config)
    manifest = {
        "seeded_at": "2026-03-14T16:30:00+00:00",
        "simulated": True,
        "disclaimer": "Demo Mode seeded this workspace with simulated artifacts intended to showcase the designed workflow. Outputs are illustrative and not scientific results.",
        "raw_record_count": len(raw_records),
        "training_example_count": len(training_examples),
        "graph_node_count": len(graph_nodes),
        "graph_edge_count": len(graph_edges),
        "model_studio_runs": seeded_runs,
        "repo_root": str(repo_root),
    }
    _write_json(manifest_path, manifest)

    return DemoWorkspaceSeedResult(
        manifest_path=manifest_path,
        report_path=report_path,
        walkthrough_path=walkthrough_path,
        seeded=True,
    )
