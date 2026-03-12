from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pbdata.storage import StorageLayout


def _read_json_dict(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return raw if isinstance(raw, dict) else {}


def _safe_float(value: object) -> float | None:
    try:
        text = str(value or "").strip()
        if not text:
            return None
        return float(text)
    except ValueError:
        return None


def _split_winner(baseline_split: dict[str, Any], tabular_split: dict[str, Any]) -> str:
    baseline_mae = _safe_float(baseline_split.get("affinity_mae_log10"))
    tabular_mae = _safe_float(tabular_split.get("affinity_mae_log10"))
    if baseline_mae is None and tabular_mae is None:
        return "no_comparison"
    if baseline_mae is None:
        return "tabular_affinity"
    if tabular_mae is None:
        return "ligand_memory_baseline"
    if tabular_mae < baseline_mae:
        return "tabular_affinity"
    if baseline_mae < tabular_mae:
        return "ligand_memory_baseline"
    return "tie"


def build_model_comparison_report(layout: StorageLayout) -> dict[str, Any]:
    baseline = _read_json_dict(layout.models_dir / "ligand_memory_evaluation.json")
    tabular = _read_json_dict(layout.models_dir / "tabular_affinity_evaluation.json")
    baseline_splits = baseline.get("splits") if isinstance(baseline.get("splits"), dict) else {}
    tabular_splits = tabular.get("splits") if isinstance(tabular.get("splits"), dict) else {}
    splits: dict[str, dict[str, Any]] = {}
    for split_name in ("val", "test"):
        baseline_split = baseline_splits.get(split_name) if isinstance(baseline_splits.get(split_name), dict) else {}
        tabular_split = tabular_splits.get(split_name) if isinstance(tabular_splits.get(split_name), dict) else {}
        splits[split_name] = {
            "winner": _split_winner(baseline_split, tabular_split),
            "baseline_top1_target_accuracy": baseline_split.get("top1_target_accuracy"),
            "baseline_affinity_mae_log10": baseline_split.get("affinity_mae_log10"),
            "tabular_affinity_mae_log10": tabular_split.get("affinity_mae_log10"),
            "tabular_affinity_rmse_log10": tabular_split.get("affinity_rmse_log10"),
            "tabular_novel_case_count": tabular_split.get("novel_case_count"),
            "baseline_novel_top1_target_accuracy": baseline_split.get("novel_top1_target_accuracy"),
        }

    available_models = {
        "baseline_ready": bool(baseline),
        "tabular_ready": bool(tabular),
    }
    status = "missing_models"
    if all(available_models.values()):
        status = "comparison_ready"
    elif any(available_models.values()):
        status = "partial_comparison"

    val_winner = str((splits.get("val") or {}).get("winner") or "no_comparison")
    test_winner = str((splits.get("test") or {}).get("winner") or "no_comparison")
    summary = (
        f"val={val_winner}, test={test_winner}; "
        f"baseline_ready={available_models['baseline_ready']}, tabular_ready={available_models['tabular_ready']}"
    )
    if not available_models["baseline_ready"] and not available_models["tabular_ready"]:
        next_action = "Run evaluate-baseline-model and evaluate-tabular-affinity-model after building training examples and splits."
    elif not available_models["baseline_ready"]:
        next_action = "Run evaluate-baseline-model so the supervised model has a baseline comparator."
    elif not available_models["tabular_ready"]:
        next_action = "Run evaluate-tabular-affinity-model to compare the supervised model against the baseline."
    elif val_winner == "tabular_affinity":
        next_action = "Inspect tabular validation metrics and, if acceptable, prefer it as the stronger current supervised model."
    elif val_winner == "ligand_memory_baseline":
        next_action = "Improve training-set quality or tabular features before preferring the supervised model over the baseline."
    else:
        next_action = "Inspect split-level metrics and novelty slices before choosing a default model."

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "status": status,
        "summary": summary,
        "next_action": next_action,
        "available_models": available_models,
        "splits": splits,
    }


def export_model_comparison_report(layout: StorageLayout) -> tuple[Path, Path, dict[str, Any]]:
    report = build_model_comparison_report(layout)
    layout.reports_dir.mkdir(parents=True, exist_ok=True)
    json_path = layout.reports_dir / "model_comparison.json"
    md_path = layout.reports_dir / "model_comparison.md"
    json_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    lines = [
        "# Model Comparison",
        "",
        f"- Status: {report['status']}",
        f"- Summary: {report['summary']}",
        f"- Next action: {report['next_action']}",
        "",
        "## Split comparison",
    ]
    for split_name in ("val", "test"):
        payload = (report.get("splits") or {}).get(split_name) or {}
        lines.append(
            f"- {split_name}: winner={payload.get('winner')}, "
            f"baseline_top1={payload.get('baseline_top1_target_accuracy')}, "
            f"baseline_mae_log10={payload.get('baseline_affinity_mae_log10')}, "
            f"tabular_mae_log10={payload.get('tabular_affinity_mae_log10')}"
        )
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return json_path, md_path, report
