"""Audit screening and dataset-selection field population.

The goal is to prevent fields that are only nominally present from quietly
driving policy. This report classifies how well important screening fields are
actually populated in the current workspace exports.
"""

from __future__ import annotations

import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pbdata.storage import StorageLayout

_TARGET_TABLES: dict[str, list[str]] = {
    "master_pdb_repository.csv": [
        "pdb_id",
        "experimental_method",
        "structure_resolution",
        "organism_names",
        "quality_score",
        "source_databases",
        "has_ligand_signal",
        "has_protein_signal",
    ],
    "master_pdb_pairs.csv": [
        "pdb_id",
        "pair_identity_key",
        "source_database",
        "receptor_uniprot_ids",
        "ligand_types",
        "matching_interface_types",
        "binding_affinity_type",
        "mutation_strings",
        "source_conflict_summary",
        "source_agreement_band",
        "release_split",
    ],
    "model_ready_pairs.csv": [
        "pdb_id",
        "pair_identity_key",
        "source_database",
        "receptor_uniprot_ids",
        "ligand_types",
        "matching_interface_types",
        "binding_affinity_type",
        "mutation_strings",
        "source_conflict_summary",
        "source_agreement_band",
        "release_split",
    ],
    "custom_training_set.csv": [
        "pdb_id",
        "pair_identity_key",
        "source_database",
        "receptor_uniprot_ids",
        "ligand_types",
        "matching_interface_types",
        "binding_affinity_type",
        "mutation_strings",
        "release_split",
    ],
}

_VALID_RELEASE_SPLITS = frozenset({"train", "val", "test"})
_COMPARABLE_AGREEMENT_BANDS = frozenset({"high", "medium", "low"})


def build_screening_field_audit(layout: StorageLayout, *, repo_root: Path | None = None) -> dict[str, Any]:
    root = repo_root or layout.root
    tables = []
    issues: list[str] = []
    restricted_policy_fields: list[dict[str, str]] = []
    unsafe_policy_fields: list[dict[str, str]] = []
    for filename, fields in _TARGET_TABLES.items():
        path = root / filename
        table_payload = _audit_table(path, fields)
        tables.append(table_payload)
        for field in table_payload["fields"]:
            if field["status"] in {"empty", "sparse"} and field["tracked"]:
                issues.append(f"{filename}:{field['field']}={field['status']}")
            if field["tracked"] and field["policy_role"] == "advisory_only":
                restricted_policy_fields.append({
                    "table": filename,
                    "field": field["field"],
                    "policy_role": field["policy_role"],
                    "policy_reason": field["policy_reason"],
                })
            elif field["tracked"] and field["policy_role"] != "decision_grade":
                unsafe_policy_fields.append({
                    "table": filename,
                    "field": field["field"],
                    "policy_role": field["policy_role"],
                    "policy_reason": field["policy_reason"],
                })
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "storage_root": str(layout.root),
        "audited_root": str(root),
        "tracked_tables": len(tables),
        "tables": tables,
        "issue_count": len(issues),
        "issues": issues,
        "restricted_policy_field_count": len(restricted_policy_fields),
        "restricted_policy_fields": restricted_policy_fields,
        "unsafe_policy_field_count": len(unsafe_policy_fields),
        "unsafe_policy_fields": unsafe_policy_fields,
        "next_action": _next_action(issues),
    }


def export_screening_field_audit(layout: StorageLayout, *, repo_root: Path | None = None) -> tuple[Path, Path, dict[str, Any]]:
    report = build_screening_field_audit(layout, repo_root=repo_root)
    layout.reports_dir.mkdir(parents=True, exist_ok=True)
    json_path = layout.reports_dir / "screening_field_audit.json"
    md_path = layout.reports_dir / "screening_field_audit.md"
    json_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

    lines = [
        "# Screening Field Audit",
        "",
        f"- Storage root: {layout.root}",
        f"- Audited root: {report['audited_root']}",
        f"- Tracked tables: {report['tracked_tables']}",
        f"- Issue count: {report['issue_count']}",
        f"- Next action: {report['next_action']}",
        "",
    ]
    for table in report["tables"]:
        lines.extend(
            [
                f"## {table['table_name']}",
                "",
                f"- Exists: {table['exists']}",
                f"- Row count: {table['row_count']}",
                "",
            ]
        )
        for field in table["fields"]:
            lines.append(
                f"- `{field['field']}`: {field['status']} "
                f"({field['non_empty_count']}/{table['row_count']} non-empty); "
                f"policy={field['policy_role']} ({field['policy_reason']})"
            )
        lines.append("")

    md_path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
    return json_path, md_path, report


def _audit_table(path: Path, tracked_fields: list[str]) -> dict[str, Any]:
    if not path.exists() or path.stat().st_size <= 0:
        return {
            "table_name": path.name,
            "path": str(path),
            "exists": False,
            "row_count": 0,
            "fields": [
                {
                    "field": field,
                    "tracked": True,
                    "status": "missing_table",
                    "non_empty_count": 0,
                    "non_empty_fraction": 0.0,
                    "distinct_non_empty_count": 0,
                    "policy_role": "quarantined",
                    "policy_reason": "table_missing",
                }
                for field in tracked_fields
            ],
        }

    with path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    header = list(rows[0].keys()) if rows else []
    row_count = len(rows)
    fields = []
    for field in tracked_fields:
        fields.append(_field_payload(path.name, field, rows, row_count, field in header))

    extra_fields = [field for field in header if field not in tracked_fields]
    for field in extra_fields:
        fields.append(_field_payload(path.name, field, rows, row_count, True, tracked=False))

    return {
        "table_name": path.name,
        "path": str(path),
        "exists": True,
        "row_count": row_count,
        "fields": fields,
    }


def _field_payload(
    table_name: str,
    field: str,
    rows: list[dict[str, str]],
    row_count: int,
    present: bool,
    *,
    tracked: bool = True,
) -> dict[str, Any]:
    if not present:
        policy_role, policy_reason = _policy_assessment(table_name, field, [], 0, 0.0, "missing_column", tracked)
        return {
            "field": field,
            "tracked": tracked,
            "status": "missing_column",
            "non_empty_count": 0,
            "non_empty_fraction": 0.0,
            "distinct_non_empty_count": 0,
            "policy_role": policy_role,
            "policy_reason": policy_reason,
        }
    non_empty_values = [
        str(row.get(field) or "").strip()
        for row in rows
        if str(row.get(field) or "").strip()
    ]
    non_empty_count = len(non_empty_values)
    fraction = round((non_empty_count / row_count), 4) if row_count else 0.0
    if row_count == 0:
        status = "empty_table"
    elif non_empty_count == 0:
        status = "empty"
    elif fraction < 0.1:
        status = "sparse"
    elif fraction < 0.6:
        status = "partial"
    else:
        status = "populated"
    policy_role, policy_reason = _policy_assessment(
        table_name,
        field,
        non_empty_values,
        non_empty_count,
        fraction,
        status,
        tracked,
    )
    return {
        "field": field,
        "tracked": tracked,
        "status": status,
        "non_empty_count": non_empty_count,
        "non_empty_fraction": fraction,
        "distinct_non_empty_count": len(set(non_empty_values)),
        "policy_role": policy_role,
        "policy_reason": policy_reason,
    }


def _policy_assessment(
    table_name: str,
    field: str,
    non_empty_values: list[str],
    non_empty_count: int,
    fraction: float,
    status: str,
    tracked: bool,
) -> tuple[str, str]:
    if not tracked:
        return "untracked", "extra_column_not_classified"
    if status in {"missing_table", "missing_column"}:
        return "quarantined", status
    if field == "source_conflict_summary":
        if non_empty_count == 0:
            return "advisory_only", "conflict_summary_missing"
        return "advisory_only", "text_summary_for_manual_review"
    if field == "organism_names":
        if non_empty_count == 0:
            return "advisory_only", "organism_coverage_missing"
        return "advisory_only", "representation_context_only"
    if field == "source_agreement_band":
        comparable_count = sum(1 for value in non_empty_values if value.lower() in _COMPARABLE_AGREEMENT_BANDS)
        if comparable_count == 0:
            return "advisory_only", "no_comparable_agreement_bands"
        if fraction < 0.6:
            return "advisory_only", "agreement_band_partial_coverage"
        return "decision_grade", "comparable_agreement_bands_populated"
    if field == "release_split":
        valid_count = sum(1 for value in non_empty_values if value.lower() in _VALID_RELEASE_SPLITS)
        if valid_count == 0:
            return "quarantined", "no_valid_release_split_assignments"
        if fraction < 0.6:
            return "quarantined", "release_split_partial_coverage"
        return "decision_grade", "release_split_assignments_present"
    if field == "quality_score":
        valid_count = 0
        for value in non_empty_values:
            try:
                parsed = float(value)
            except ValueError:
                continue
            if 0.0 <= parsed <= 1.0:
                valid_count += 1
        if valid_count == 0:
            return "quarantined", "quality_score_missing_or_invalid"
        if fraction < 0.6 or valid_count != non_empty_count:
            return "quarantined", "quality_score_partial_or_invalid"
        return "decision_grade", "quality_score_numeric_and_populated"
    if field == "mutation_strings":
        if non_empty_count == 0:
            return "quarantined", "mutation_annotations_missing"
        if fraction < 0.6:
            return "advisory_only", "mutation_annotations_partial"
        return "decision_grade", "mutation_annotations_populated"
    if status in {"empty", "sparse"}:
        return "quarantined", f"{field}_insufficient_population"
    return "decision_grade", "field_populated_enough_for_policy"


def _next_action(issues: list[str]) -> str:
    if not issues:
        return "Tracked screening fields look populated enough to keep tightening policy rules."
    return (
        "Audit the sparse or empty fields before using them in selection policy: "
        + ", ".join(issues[:8])
        + (" ..." if len(issues) > 8 else "")
    )
