"""Operator-facing status and environment checks."""

from __future__ import annotations

import csv
import json
import sys
from pathlib import Path
from typing import Any

from pbdata.config import AppConfig
from pbdata.storage import StorageLayout


def _count_glob(path: Path, pattern: str) -> int:
    if not path.exists():
        return 0
    return sum(1 for _ in path.glob(pattern))


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def build_status_report(layout: StorageLayout) -> dict[str, Any]:
    training_examples_path = layout.training_dir / "training_examples.json"
    model_path = layout.models_dir / "ligand_memory_model.json"
    latest_release = layout.releases_dir / "latest_release.json"
    feature_manifest = layout.features_dir / "feature_manifest.json"
    summary = {
        "storage_root": str(layout.root),
        "raw_rcsb_count": _count_glob(layout.raw_rcsb_dir, "*.json"),
        "processed_rcsb_count": _count_glob(layout.processed_rcsb_dir, "*.json"),
        "extracted_entry_count": _count_glob(layout.extracted_dir / "entry", "*.json"),
        "structure_file_count": _count_glob(layout.structures_rcsb_dir, "*.cif") + _count_glob(layout.structures_rcsb_dir, "*.pdb"),
        "graph_node_export_present": (layout.graph_dir / "graph_nodes.json").exists(),
        "graph_edge_export_present": (layout.graph_dir / "graph_edges.json").exists(),
        "feature_manifest_present": feature_manifest.exists(),
        "training_examples_present": training_examples_path.exists(),
        "training_example_count": 0,
        "release_snapshot_present": latest_release.exists(),
        "baseline_model_present": model_path.exists(),
        "site_feature_runs": _count_glob(layout.artifact_manifests_dir, "*_input_manifest.json"),
        "surrogate_checkpoint_present": (layout.surrogate_training_artifacts_dir / "latest_surrogate_checkpoint.json").exists(),
    }
    if training_examples_path.exists():
        raw = json.loads(training_examples_path.read_text(encoding="utf-8"))
        if isinstance(raw, list):
            summary["training_example_count"] = len(raw)
    return summary


def build_doctor_report(layout: StorageLayout, config: AppConfig) -> dict[str, Any]:
    dependency_checks: dict[str, dict[str, Any]] = {}
    for package_name, required, note in [
        ("gemmi", True, "Required for structure parsing."),
        ("yaml", True, "Required for config loading."),
        ("torch", False, "Optional; enables tensor exports and trained surrogate backend."),
        ("pyarrow", False, "Optional; enables native parquet I/O."),
        ("fastparquet", False, "Optional parquet backend."),
        ("esm", False, "Optional; enables learned protein embeddings."),
    ]:
        try:
            __import__(package_name)
            dependency_checks[package_name] = {"status": "available", "required": required, "note": note}
        except Exception as exc:
            dependency_checks[package_name] = {"status": "missing", "required": required, "note": note, "error": str(exc)}

    source_status: dict[str, dict[str, Any]] = {}
    for source_name in ("rcsb", "bindingdb", "chembl", "pdbbind", "biolip", "skempi"):
        source_cfg = getattr(config.sources, source_name)
        source_status[source_name] = {
            "enabled": bool(source_cfg.enabled),
            "extra_keys": sorted((source_cfg.extra or {}).keys()),
        }

    checks = {
        "python_version": sys.version.split()[0],
        "storage_root_exists": layout.root.exists(),
        "config_storage_root": config.storage_root,
        "required_directories": {
            "data": layout.data_dir.exists(),
            "artifacts": layout.artifacts_dir.exists(),
        },
        "dependency_checks": dependency_checks,
        "source_status": source_status,
        "status_snapshot": build_status_report(layout),
    }
    checks["overall_status"] = "ready" if all(
        value["status"] == "available" or not value["required"]
        for value in dependency_checks.values()
    ) else "missing_required_dependencies"
    return checks
