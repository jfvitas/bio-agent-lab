"""Fast stage-by-stage demo pipeline simulation for the GUI and CLI."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pbdata.config import AppConfig
from pbdata.demo import export_demo_snapshot
from pbdata.demo_workspace import seed_demo_workspace
from pbdata.storage import StorageLayout


@dataclass(frozen=True)
class DemoStageSimulation:
    stage: str
    status: str
    lines: tuple[str, ...]
    artifacts: tuple[str, ...]


def _read_demo_manifest(layout: StorageLayout) -> dict[str, object]:
    path = layout.data_dir / "demo" / "demo_workspace_manifest.json"
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _write_stage_state(layout: StorageLayout, stage: str, *, status: str, notes: str) -> None:
    payload = {
        "stage": stage,
        "status": status,
        "generated_at": "2026-03-14T17:00:00+00:00",
        "notes": notes,
        "simulated": True,
    }
    path = layout.stage_state_dir / f"{stage}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _counts(layout: StorageLayout) -> dict[str, int]:
    manifest = _read_demo_manifest(layout)
    return {
        "raw": int(manifest.get("raw_record_count") or 64),
        "examples": int(manifest.get("training_example_count") or 48),
        "graphs": int(manifest.get("graph_node_count") or 96),
        "edges": int(manifest.get("graph_edge_count") or 48),
    }


def _selection_notes(context: dict[str, Any] | None) -> tuple[str, str]:
    context = context or {}
    mode = str(context.get("custom_set_mode") or "generalist")
    target_size = str(context.get("custom_set_target_size") or "500")
    if mode == "protein_ligand":
        return (
            f"Applied a protein-ligand curation recipe targeting roughly {target_size} examples.",
            "This path emphasizes ligand chemistry coverage, scaffold variety, and assay balance.",
        )
    if mode == "protein_protein":
        return (
            f"Applied a protein-protein curation recipe targeting roughly {target_size} examples.",
            "This path emphasizes interface diversity, family holdouts, and mutation-aware grouping.",
        )
    if mode == "mutation_effect":
        return (
            f"Applied a mutation-effect recipe targeting roughly {target_size} examples.",
            "This path highlights variant clustering so held-out evaluation stays biologically meaningful.",
        )
    if mode == "high_trust":
        return (
            f"Applied a high-trust recipe targeting roughly {target_size} examples.",
            "This path favors conservative provenance, conflict filtering, and lower-risk benchmarking.",
        )
    return (
        f"Applied a generalist recipe targeting roughly {target_size} examples.",
        "This path emphasizes representative coverage rather than over-focusing on one biological niche.",
    )


def _stage_lines(
    stage: str,
    layout: StorageLayout,
    *,
    context: dict[str, Any] | None = None,
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    counts = _counts(layout)
    custom_detail, custom_detail_2 = _selection_notes(context)
    stage_map: dict[str, tuple[tuple[str, ...], tuple[str, ...]]] = {
        "ingest": (
            (
                f"Demo Mode: simulating ingest across {counts['raw']:,} representative RCSB records.",
                "Collected a mixed structural slice spanning protein-ligand, protein-protein, and single-protein cases.",
                "Cached demo raw payloads and staged structure references for downstream extract.",
            ),
            ("data/raw/rcsb", "data/structures/rcsb"),
        ),
        "preview-rcsb-search": (
            (
                f"Demo Mode: previewing a representative structural search across {counts['raw']:,} demo records.",
                "Prepared a realistic search preview emphasizing breadth and representative sampling.",
            ),
            ("data/reports/rcsb_search_preview.json",),
        ),
        "setup-workspace": (
            (
                "Demo Mode: preparing workspace manifests, folders, and dashboard artifacts.",
                "Initialized the canonical data, artifact, and report locations expected by the GUI.",
            ),
            ("artifacts/manifests", "data/reports"),
        ),
        "harvest-metadata": (
            (
                "Demo Mode: simulating metadata harvest across UniProt, pathway, and family context.",
                "Materialized representative protein metadata to support review, curation, and split diagnostics.",
            ),
            ("metadata/protein_metadata.csv",),
        ),
        "extract": (
            (
                f"Demo Mode: extracting {counts['raw']:,} raw records into normalized multi-table bundles.",
                "Built entry, chain, interface, assay, and provenance tables with mixed source coverage.",
                f"Prepared {counts['examples']:,} model-relevant interaction records for downstream graph and feature stages.",
                "This is where the workflow starts turning many-source structural evidence into canonical, reviewable biological rows.",
            ),
            ("data/extracted/entry", "data/extracted/assays"),
        ),
        "normalize": (
            (
                "Demo Mode: simulating canonical normalization and source reconciliation.",
                "Resolved representative assay and entity differences into canonical review-ready rows.",
            ),
            ("data/processed/rcsb", "data/identity"),
        ),
        "audit": (
            (
                "Demo Mode: generating quality audit summaries and processed health checks.",
                "Flagged a small number of illustrative source conflicts and review issues.",
            ),
            ("data/reports/processed_json_health.json", "master_pdb_issues.csv"),
        ),
        "report": (
            (
                "Demo Mode: assembling release-facing review exports and coverage summaries.",
                "Generated model-ready pair exports, conflict tables, and release-readiness scaffolding.",
            ),
            ("model_ready_pairs.csv", "scientific_coverage_summary.json"),
        ),
        "report-source-capabilities": (
            (
                "Demo Mode: exporting a source-capability report from the simulated source configuration.",
                "Summarized live, cached, and local-source roles for the workflow overview.",
            ),
            ("data/reports/source_capabilities.json", "data/reports/source_capabilities.md"),
        ),
        "export-identity-crosswalk": (
            (
                "Demo Mode: exporting canonical protein, ligand, and pair identity crosswalk tables.",
                "Prepared conservative identity bridge tables for review and release surfaces.",
            ),
            (
                "data/identity/protein_identity_crosswalk.csv",
                "data/identity/ligand_identity_crosswalk.csv",
                "data/identity/pair_identity_crosswalk.csv",
            ),
        ),
        "report-bias": (
            (
                "Demo Mode: summarizing distribution, diversity, and leakage-sensitive grouping signals.",
                "Prepared a lightweight fairness and dominance snapshot for the dashboard.",
            ),
            ("data/splits/split_diagnostics.json", "custom_training_split_benchmark.csv"),
        ),
        "build-structural-graphs": (
            (
                "Demo Mode: building residue-scale structural graph exports.",
                "Prepared PyG-style graph-ready structural neighborhoods for representative complexes.",
            ),
            ("artifacts/graphs", "data/graph"),
        ),
        "build-graph": (
            (
                f"Demo Mode: materializing unified graph nodes ({counts['graphs']:,}) and edges ({counts['edges']:,}).",
                "Connected proteins, ligands, and interfaces into demo graph artifacts for downstream modeling.",
            ),
            ("data/graph/graph_nodes.json", "data/graph/graph_edges.json"),
        ),
        "build-microstates": (
            (
                "Demo Mode: simulating microstate generation across representative binding environments.",
                "Prepared microstate placeholders for downstream physics-style stages.",
            ),
            ("data/features/microstates",),
        ),
        "build-physics-features": (
            (
                "Demo Mode: synthesizing representative physics-inspired descriptors.",
                "Populated illustrative energetic and geometric feature summaries.",
            ),
            ("data/features/physics",),
        ),
        "build-microstate-refinement": (
            (
                "Demo Mode: simulating microstate refinement and ranking outputs.",
                "Prepared refinement placeholders to demonstrate the extended workflow.",
            ),
            ("data/features/microstate_refinement",),
        ),
        "build-mm-job-manifests": (
            (
                "Demo Mode: generating MM job manifests for an external compute queue.",
                "Prepared a fake batch definition so the queue/export UI looks populated.",
            ),
            ("data/features/mm_jobs",),
        ),
        "run-mm-jobs": (
            (
                "Demo Mode: simulating completion of queued molecular mechanics jobs.",
                "Attached representative completed-job metadata for downstream ingestion surfaces.",
            ),
            ("data/features/mm_jobs",),
        ),
        "run-feature-pipeline": (
            (
                "Demo Mode: running the site-centric feature pipeline over representative proteins.",
                "Generated site-centric manifests and cached feature-run records for the dashboard.",
            ),
            ("artifacts/manifests/demo_site_pipeline_input_manifest.json",),
        ),
        "export-analysis-queue": (
            (
                "Demo Mode: exporting an external analysis queue for deferred computation.",
                "Prepared a queue payload so the workflow can demonstrate handoff into other tooling.",
            ),
            ("artifacts/manifests",),
        ),
        "ingest-physics-results": (
            (
                "Demo Mode: ingesting simulated external physics results.",
                "Merged illustrative score bundles back into the feature workspace.",
            ),
            ("data/features/physics",),
        ),
        "train-site-physics-surrogate": (
            (
                "Demo Mode: training a surrogate over simulated site-physics outputs.",
                "Recorded a placeholder checkpoint so downstream orchestration appears complete.",
            ),
            ("artifacts/surrogate_training",),
        ),
        "build-features": (
            (
                "Demo Mode: generating engineered and graph-summary feature records.",
                f"Built {counts['examples']:,} feature rows covering structural, protein, ligand, interaction, and graph groups.",
                "The feature layer is intentionally multi-view so the same curated corpus can support interpretable tabular baselines and graph-native experiments.",
            ),
            ("data/features/feature_manifest.json", "data/features/feature_records.json"),
        ),
        "build-training-examples": (
            (
                "Demo Mode: assembling supervised training examples from the extracted and feature layers.",
                f"Materialized {counts['examples']:,} representative labeled examples for Model Studio and downstream reporting.",
                "This demonstrates that the platform is meant to create explicit, leakage-aware learning problems rather than generic merged tables.",
            ),
            ("data/training_examples/training_examples.json",),
        ),
        "build-splits": (
            (
                "Demo Mode: creating leakage-aware train/val/test membership files.",
                "Prepared family-balanced split diagnostics and release-oriented split metadata.",
                "The split story is important because real biological generalization needs grouping, family awareness, and redundancy control instead of random row shuffles.",
            ),
            ("data/splits/train.txt", "data/splits/split_diagnostics.json"),
        ),
        "train-baseline-model": (
            (
                "Demo Mode: simulating baseline model training on the curated demo dataset.",
                "Generated a ready baseline artifact to support model-comparison visuals.",
            ),
            ("data/models/ligand_memory_model.json",),
        ),
        "evaluate-baseline-model": (
            (
                "Demo Mode: evaluating the baseline and tabular demo models on held-out splits.",
                "Updated model-comparison artifacts and stage-ready evaluation summaries.",
            ),
            ("data/models/ligand_memory_evaluation.json", "data/reports/model_comparison.json"),
        ),
        "build-custom-training-set": (
            (
                "Demo Mode: constructing a curated training subset with diversity-aware filters.",
                custom_detail,
                custom_detail_2,
                "Wrote selection, exclusion, and scorecard artifacts to showcase the dataset-builder flow.",
            ),
            ("custom_training_set.csv", "custom_training_scorecard.json"),
        ),
        "engineer-dataset": (
            (
                "Demo Mode: synthesizing an engineered dataset release candidate.",
                "Prepared representative tabular exports and workspace-level derived datasets.",
            ),
            ("datasets", "features"),
        ),
        "build-release": (
            (
                "Demo Mode: freezing a release-style manifest and presenter-ready outputs.",
                "Prepared the latest release pointer and release-readiness summaries.",
                "This frames the data product as a governed release surface with provenance and reproducible handoff artifacts.",
            ),
            ("data/releases/latest_release.json", "dataset_release_manifest.json"),
        ),
        "run-scenario-tests": (
            (
                "Demo Mode: running scenario-style checks over the seeded workflow outputs.",
                "All simulated showcase artifacts are present and internally consistent.",
            ),
            ("data/qa/scenario_test_report.json", "data/qa/scenario_test_manifest.json"),
        ),
        "export-demo-snapshot": (
            (
                "Demo Mode: exporting the presenter walkthrough and readiness summary.",
                "Refreshed the demo snapshot so the scripted walk-through matches the current seeded artifacts.",
            ),
            ("artifacts/reports/demo_walkthrough.md",),
        ),
    }
    return stage_map.get(
        stage,
        (
            (
                f"Demo Mode: simulating the {stage} stage.",
                "Representative artifacts and summaries have been refreshed for presentation purposes.",
            ),
            tuple(),
        ),
    )


def simulate_demo_stage(
    layout: StorageLayout,
    config: AppConfig,
    *,
    stage: str,
    repo_root: Path,
    context: dict[str, Any] | None = None,
) -> DemoStageSimulation:
    seed_demo_workspace(layout, config, repo_root=repo_root)
    if stage == "export-demo-snapshot":
        export_demo_snapshot(layout, config)
    if stage == "build-custom-training-set":
        custom_detail, custom_detail_2 = _selection_notes(context)
        scorecard = {
            "selection_mode": str((context or {}).get("custom_set_mode") or "generalist"),
            "target_size": int(str((context or {}).get("custom_set_target_size") or "500")),
            "selection_summary": custom_detail,
            "interpretation": custom_detail_2,
            "simulated": True,
        }
        (layout.root / "custom_training_scorecard.json").write_text(json.dumps(scorecard, indent=2), encoding="utf-8")
    lines, artifacts = _stage_lines(stage, layout, context=context)
    notes = f"Demo Mode simulated {stage}. Outputs are illustrative only."
    _write_stage_state(layout, stage, status="completed", notes=notes)
    return DemoStageSimulation(
        stage=stage,
        status="done",
        lines=lines,
        artifacts=artifacts,
    )
