"""Model Studio demo-mode simulations for local training and inference."""

from __future__ import annotations

import hashlib
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pbdata.config import AppConfig
from pbdata.demo_workspace import seed_demo_workspace
from pbdata.modeling.training_runs import TrainingRunResult
from pbdata.storage import StorageLayout


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _read_json(path: Path) -> Any:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def _template_run_dir(layout: StorageLayout, family: str) -> Path:
    root = layout.models_dir / "model_studio" / "runs"
    if family in {"gnn", "hybrid_fusion"}:
        return root / "demo_pyg_hybrid_affinity"
    return root / "demo_xgboost_affinity"


def _family_metric_profile(
    family: str,
    *,
    compute_budget: str,
    interpretability: str,
    modality: str,
) -> tuple[float, list[str], str]:
    normalized_family = family.strip().lower()
    budget = compute_budget.strip().lower()
    explainability = interpretability.strip().lower()
    normalized_modality = modality.strip().lower()
    warnings: list[str] = []
    narrative = "Balanced demo baseline."
    rmse = 0.56
    if normalized_family == "hybrid_fusion":
        rmse = 0.41 if budget == "high" else 0.44
        narrative = "Hybrid fusion combines graph topology with attribute context for stronger structure-aware generalization."
        warnings.append("graph_attribute_fusion_demo_profile")
    elif normalized_family == "gnn":
        rmse = 0.46 if budget != "low" else 0.5
        narrative = "Native graph modeling captures contact topology directly and usually improves on flat descriptors when graph coverage is strong."
    elif normalized_family == "xgboost":
        rmse = 0.49
        narrative = "Gradient-boosted trees offer a fast, strong tabular baseline with understandable feature importance."
    elif normalized_family == "random_forest":
        rmse = 0.54
        narrative = "Random forests are conservative but interpretable and resilient for mixed engineered features."
    elif normalized_family == "dense_nn":
        rmse = 0.52 if budget != "low" else 0.58
        narrative = "Dense neural nets benefit from larger feature surfaces but trade away some interpretability."
        if explainability == "high":
            warnings.append("interpretability_tradeoff_demo_profile")
    elif normalized_family == "clustering":
        rmse = 0.61
        narrative = "Clustering is presented as an exploratory pattern-finding path rather than a strict predictive winner."
        warnings.append("unsupervised_demo_profile")
    elif normalized_family == "autoencoder":
        rmse = 0.59
        narrative = "Autoencoders emphasize latent structure discovery and representation learning over headline predictive metrics."
        warnings.append("representation_learning_demo_profile")
    if normalized_modality in {"graphs", "graphs+attributes"} and normalized_family in {"xgboost", "random_forest"}:
        narrative += " The current selection still flattens graph-derived summaries into a tabular view for comparison."
    if explainability == "high" and normalized_family in {"hybrid_fusion", "gnn"}:
        warnings.append("native_graph_interpretability_tradeoff")
    return rmse, warnings, narrative


def simulate_model_training_run(
    layout: StorageLayout,
    config: AppConfig,
    *,
    starter_config: dict[str, Any],
    runtime_target: str,
    repo_root: Path,
) -> TrainingRunResult:
    seed_demo_workspace(layout, config, repo_root=repo_root)
    family = str(starter_config.get("family") or "xgboost")
    task = str(starter_config.get("task") or "regression")
    model_id = str(starter_config.get("model_id") or family)
    modality = str(starter_config.get("modality") or starter_config.get("input_modality") or "auto")
    compute_budget = str(starter_config.get("compute_budget") or "balanced")
    interpretability = str(starter_config.get("interpretability") or "balanced")
    template_dir = _template_run_dir(layout, family)
    run_name = f"demo_{family}_{_timestamp().lower()}"
    run_dir = layout.models_dir / "model_studio" / "runs" / run_name
    shutil.copytree(template_dir, run_dir)

    metrics = _read_json(run_dir / "metrics.json") or {}
    if isinstance(metrics, dict):
        metrics["family"] = family
        metrics["task"] = task
        base_rmse, metric_warnings, narrative = _family_metric_profile(
            family,
            compute_budget=compute_budget,
            interpretability=interpretability,
            modality=modality,
        )
        adjustment = (int(hashlib.md5(run_name.encode("utf-8")).hexdigest()[:4], 16) % 18) / 1000.0
        test_metrics = metrics.get("test") if isinstance(metrics.get("test"), dict) else {}
        if isinstance(test_metrics, dict) and "rmse" in test_metrics:
            test_metrics["rmse"] = round(max(0.35, base_rmse + adjustment), 3)
            test_metrics["mae"] = round(float(test_metrics["rmse"]) * 0.68, 3)
            test_metrics["r2"] = round(max(0.42, 1.0 - float(test_metrics["rmse"]) / 1.2), 3)
        metrics["test"] = test_metrics
        metrics["demo_narrative"] = narrative
        metrics["demo_selection"] = {
            "family": family,
            "modality": modality,
            "compute_budget": compute_budget,
            "interpretability": interpretability,
            "runtime_target": runtime_target,
        }
        (run_dir / "metrics.json").write_text(json.dumps(metrics, indent=2), encoding="utf-8")
    else:
        metric_warnings = []
        narrative = "Demo Mode simulated a model run."

    config_payload = _read_json(run_dir / "config.json") or {}
    if isinstance(config_payload, dict):
        config_payload["family"] = family
        config_payload["task"] = task
        config_payload["model_id"] = model_id
        config_payload["modality"] = modality
        config_payload["compute_budget"] = compute_budget
        config_payload["interpretability"] = interpretability
        config_payload["runtime_target"] = runtime_target
        config_payload["trainer_backend"] = (
            "pyg_hybrid_fusion" if family in {"gnn", "hybrid_fusion"} else "sklearn_hist_gradient_boosting"
        )
        config_payload["backend_plan"] = {
            "requested_family": family,
            "execution_family": family,
            "backend_id": config_payload["trainer_backend"],
            "implementation": "native" if family in {"gnn", "hybrid_fusion"} else "fallback",
            "native_graph": family in {"gnn", "hybrid_fusion"},
        }
        (run_dir / "config.json").write_text(json.dumps(config_payload, indent=2), encoding="utf-8")

    manifest = _read_json(run_dir / "run_manifest.json") or {}
    if isinstance(manifest, dict):
        manifest["generated_at"] = _utc_now()
        manifest["run_name"] = run_name
        manifest["family"] = family
        manifest["task"] = task
        manifest["runtime_target"] = runtime_target
        manifest["backend"] = config_payload.get("trainer_backend") if isinstance(config_payload, dict) else family
        manifest["backend_plan"] = config_payload.get("backend_plan") if isinstance(config_payload, dict) else {}
        warnings = list(manifest.get("warnings") or [])
        if "demo_mode_simulated_outputs" not in warnings:
            warnings.append("demo_mode_simulated_outputs")
        for warning in metric_warnings:
            if warning not in warnings:
                warnings.append(warning)
        manifest["warnings"] = warnings
        manifest["demo_story"] = narrative
        (run_dir / "run_manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    return TrainingRunResult(
        run_name=run_name,
        run_dir=run_dir,
        family=family,
        task=task,
        metrics=metrics if isinstance(metrics, dict) else {},
        warnings=tuple((manifest.get("warnings") or ["demo_mode_simulated_outputs"]) if isinstance(manifest, dict) else ["demo_mode_simulated_outputs"]),
        summary=(
            f"Demo Mode simulated a local {family} training run for {model_id}. {narrative} "
            "Artifacts are illustrative only."
        ),
    )


def simulate_saved_model_inference(
    layout: StorageLayout,
    config: AppConfig,
    *,
    run_dir: str | Path,
    pdb_id: str,
    repo_root: Path,
) -> dict[str, Any]:
    seed_demo_workspace(layout, config, repo_root=repo_root)
    resolved_run_dir = Path(run_dir).resolve()
    manifest = _read_json(resolved_run_dir / "run_manifest.json") or {}
    digest = hashlib.md5(str(pdb_id).upper().encode("utf-8")).hexdigest()
    prediction = round(4.8 + (int(digest[:6], 16) % 1700) / 1000.0, 3)
    ground_truth = None
    examples = _read_json(layout.training_dir / "training_examples.json")
    if isinstance(examples, list):
        for example in examples:
            if str(((example.get("structure") or {}).get("pdb_id")) or "").upper() == str(pdb_id).upper():
                labels = example.get("labels") or {}
                if isinstance(labels, dict) and "binding_affinity_log10" in labels:
                    ground_truth = labels.get("binding_affinity_log10")
                break
    return {
        "pdb_id": str(pdb_id).upper(),
        "prediction": prediction,
        "ground_truth": ground_truth,
        "family": str(manifest.get("family") or "demo_model"),
        "task": str(manifest.get("task") or "regression"),
        "run_dir": str(resolved_run_dir),
        "simulated": True,
        "disclaimer": "Demo Mode simulated saved-model inference. Values are illustrative only.",
    }
