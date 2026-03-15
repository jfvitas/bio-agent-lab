"""Dataset bias report generation."""

from __future__ import annotations

from collections import Counter
from pathlib import Path
from typing import Any

import json

from pbdata.table_io import load_table_json

def build_bias_report(extracted_dir: Path, output_dir: Path) -> tuple[Path, dict[str, Any]]:
    entries = load_table_json(extracted_dir / "entry")
    bound_objects = load_table_json(extracted_dir / "bound_objects")

    family_counter: Counter[str] = Counter()
    scaffold_counter: Counter[str] = Counter()
    organism_counter: Counter[str] = Counter()
    resolution_counter: Counter[str] = Counter()
    method_counter: Counter[str] = Counter()
    missing_counts: Counter[str] = Counter()

    for entry in entries:
        task_hint = str(entry.get("task_hint") or "").strip()
        if task_hint:
            family_counter[task_hint] += 1
        else:
            missing_counts["task_hint"] += 1
        organisms = entry.get("organism_names") or []
        if not organisms:
            missing_counts["organism_names"] += 1
        for organism in organisms:
            if organism:
                organism_counter[str(organism)] += 1
        method = str(entry.get("experimental_method") or "").strip()
        if method:
            method_counter[method] += 1
        else:
            missing_counts["experimental_method"] += 1
        resolution_bin = str(entry.get("resolution_bin") or "").strip()
        if resolution_bin:
            resolution_counter[resolution_bin] += 1
        else:
            missing_counts["resolution_bin"] += 1

    for bound_object in bound_objects:
        scaffold = str(bound_object.get("component_inchikey") or "").strip()
        if not scaffold:
            scaffold = str(bound_object.get("component_id") or "").strip()
        if scaffold:
            scaffold_counter[scaffold] += 1
        else:
            missing_counts["ligand_scaffold_identifier"] += 1

    report = {
        "protein_family_distribution": dict(family_counter.most_common()),
        "ligand_scaffold_diversity": dict(scaffold_counter.most_common()),
        "organism_distribution": dict(organism_counter.most_common()),
        "resolution_distribution": dict(resolution_counter.most_common()),
        "experimental_method_distribution": dict(method_counter.most_common()),
        "missing_data_count": dict(missing_counts),
    }
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / "bias_report.json"
    out_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    return out_path, report
