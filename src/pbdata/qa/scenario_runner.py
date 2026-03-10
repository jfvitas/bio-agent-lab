"""Scenario-test runner driven by the authoritative instruction pack."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml


def _read_json(path: Path) -> Any:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def _has_value(payload: Any, field_name: str) -> bool:
    if not isinstance(payload, dict):
        return False
    value = payload.get(field_name)
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, list):
        return len(value) > 0
    if isinstance(value, dict):
        if field_name == "interface_summary":
            residues = value.get("predicted_interface_residues") or []
            observed_count = value.get("observed_interface_count")
            return bool(residues) or (isinstance(observed_count, int) and observed_count > 0)
        return any(_has_value({k: v}, k) for k, v in value.items())
    return True


def run_scenario_templates(
    scenario_yaml_path: Path,
    rubric_path: Path,
    output_dir: Path,
) -> tuple[Path, Path]:
    raw = yaml.safe_load(scenario_yaml_path.read_text(encoding="utf-8")) or {}
    scenarios = raw.get("scenarios") or {}
    rubric = rubric_path.read_text(encoding="utf-8")

    repo_root = output_dir.parent.parent if output_dir.parent.parent.exists() else Path.cwd()
    ligand_manifest = _read_json(repo_root / "data" / "prediction" / "ligand_screening" / "prediction_manifest.json")
    peptide_manifest = _read_json(repo_root / "data" / "prediction" / "peptide_binding" / "prediction_manifest.json")
    risk_summary = _read_json(repo_root / "data" / "risk" / "pathway_risk_summary.json")

    reports: list[dict[str, Any]] = []
    for scenario_id, scenario in scenarios.items():
        expected_outputs = list(scenario.get("expected_outputs") or [])
        forbidden = list(scenario.get("forbidden_behaviors") or [])
        available_outputs: list[str] = []
        non_null_outputs: list[str] = []
        if _has_value(ligand_manifest, "ranked_target_list"):
            available_outputs.append("ranked_target_list")
            non_null_outputs.append("ranked_target_list")
        elif isinstance(ligand_manifest, dict) and "ranked_target_list" in ligand_manifest:
            available_outputs.append("ranked_target_list")
        if _has_value(ligand_manifest, "predicted_kd") or _has_value(ligand_manifest, "predicted_delta_g"):
            available_outputs.append("predicted_affinity")
            non_null_outputs.append("predicted_affinity")
        elif isinstance(ligand_manifest, dict) and ("predicted_kd" in ligand_manifest or "predicted_delta_g" in ligand_manifest):
            available_outputs.append("predicted_affinity")
        if _has_value(ligand_manifest, "confidence_score"):
            available_outputs.append("confidence_score")
            non_null_outputs.append("confidence_score")
        elif isinstance(ligand_manifest, dict) and "confidence_score" in ligand_manifest:
            available_outputs.append("confidence_score")
        if _has_value(risk_summary, "risk_score"):
            available_outputs.append("pathway_risk_summary")
            non_null_outputs.append("pathway_risk_summary")
        elif isinstance(risk_summary, dict):
            available_outputs.append("pathway_risk_summary")
        if _has_value(peptide_manifest, "predicted_targets"):
            available_outputs.append("predicted_targets")
            non_null_outputs.append("predicted_targets")
        elif isinstance(peptide_manifest, dict) and "predicted_targets" in peptide_manifest:
            available_outputs.append("predicted_targets")
        if _has_value(peptide_manifest, "interface_summary"):
            available_outputs.append("interface_summary")
            non_null_outputs.append("interface_summary")
        elif isinstance(peptide_manifest, dict) and "interface_summary" in peptide_manifest:
            available_outputs.append("interface_summary")
        if _has_value(peptide_manifest, "binding_probability"):
            available_outputs.append("binding_probability")
            non_null_outputs.append("binding_probability")
        elif isinstance(peptide_manifest, dict) and "binding_probability" in peptide_manifest:
            available_outputs.append("binding_probability")

        missing_outputs = [item for item in expected_outputs if item not in non_null_outputs]
        reports.append({
            "scenario_id": scenario_id,
            "user_goal": scenario.get("goal"),
            "steps_taken": ["scenario_loaded", "artifact_values_checked"],
            "observed_behavior": (
                f"Available outputs: {available_outputs or ['none']}. Non-null outputs: {non_null_outputs or ['none']}."
            ),
            "expected_behavior": expected_outputs,
            "missing_expected_outputs": missing_outputs,
            "severity": "low" if not missing_outputs else "medium",
            "forbidden_behaviors": forbidden,
            "rubric_reference": "undesirable_state_rubric.md",
            "generated_at": datetime.now(timezone.utc).isoformat(),
        })

    output_dir.mkdir(parents=True, exist_ok=True)
    report_path = output_dir / "scenario_test_report.json"
    report_path.write_text(json.dumps(reports, indent=2), encoding="utf-8")
    manifest_path = output_dir / "scenario_test_manifest.json"
    manifest_path.write_text(json.dumps({
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "scenario_count": len(reports),
        "rubric_loaded": bool(rubric.strip()),
        "status": "scenario_templates_loaded",
    }, indent=2), encoding="utf-8")
    return report_path, manifest_path
