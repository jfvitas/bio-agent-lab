"""Runtime capability detection and portable package export helpers for Model Studio."""

from __future__ import annotations

import json
import platform
import shutil
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pbdata.storage import StorageLayout
from pbdata.modeling.portable_package import (
    portable_train_entrypoint_source,
    portable_training_backend_source,
)
from pbdata.modeling.graph_contract import build_graph_learning_contract
from pbdata.modeling.graph_dataset import materialize_graph_dataset_records
from pbdata.modeling.graph_pyg_adapter import build_pyg_ready_graph_samples
from pbdata.modeling.graph_samples import build_graph_sample_manifest
from pbdata.modeling.graph_training_payload import materialize_graph_training_payload
from pbdata.modeling.hybrid_training_payload import materialize_hybrid_training_payload
from pbdata.modeling.trainer_registry import resolve_trainer_backend


@dataclass(frozen=True)
class RuntimeCapabilities:
    python_version: str
    platform: str
    local_cpu_available: bool
    local_gpu_available: bool
    gpu_backend: str | None
    installed_backends: tuple[str, ...]
    supported_targets: tuple[str, ...]
    summary: str


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _module_available(name: str) -> bool:
    try:
        __import__(name)
        return True
    except Exception:
        return False


def detect_runtime_capabilities() -> RuntimeCapabilities:
    installed: list[str] = []
    gpu_backend: str | None = None
    local_gpu_available = False

    if _module_available("torch"):
        installed.append("torch")
        try:
            import torch  # type: ignore

            if bool(torch.cuda.is_available()):
                local_gpu_available = True
                gpu_backend = "cuda"
            elif bool(getattr(torch.backends, "mps", None)) and bool(torch.backends.mps.is_available()):
                local_gpu_available = True
                gpu_backend = "mps"
        except Exception:
            pass
    if _module_available("tensorflow"):
        installed.append("tensorflow")
        if not local_gpu_available:
            try:
                import tensorflow as tf  # type: ignore

                if tf.config.list_physical_devices("GPU"):
                    local_gpu_available = True
                    gpu_backend = "tensorflow_gpu"
            except Exception:
                pass
    if _module_available("xgboost"):
        installed.append("xgboost")
    if _module_available("lightgbm"):
        installed.append("lightgbm")
    if _module_available("sklearn"):
        installed.append("sklearn")
    if _module_available("dgl"):
        installed.append("dgl")
    if _module_available("torch_geometric"):
        installed.append("torch_geometric")

    supported_targets = ["local_cpu"]
    if local_gpu_available:
        supported_targets.append("local_gpu")
    supported_targets.extend(["cluster", "kaggle", "colab"])
    summary = (
        f"python={platform.python_version()} | platform={platform.system()} | "
        f"gpu={'yes' if local_gpu_available else 'no'}"
        + (f" ({gpu_backend})" if gpu_backend else "")
        + f" | backends={', '.join(installed) if installed else 'none detected'}"
    )
    return RuntimeCapabilities(
        python_version=platform.python_version(),
        platform=f"{platform.system()} {platform.release()}",
        local_cpu_available=True,
        local_gpu_available=local_gpu_available,
        gpu_backend=gpu_backend,
        installed_backends=tuple(installed),
        supported_targets=tuple(supported_targets),
        summary=summary,
    )


def export_training_package(
    layout: StorageLayout,
    *,
    starter_config: dict[str, Any],
    target: str,
    package_name: str,
) -> Path:
    out_dir = layout.models_dir / "model_studio" / "exports" / package_name
    out_dir.mkdir(parents=True, exist_ok=True)
    package_data_dir = out_dir / "package_data"
    package_data_dir.mkdir(parents=True, exist_ok=True)
    split_data_dir = package_data_dir / "splits"
    split_data_dir.mkdir(parents=True, exist_ok=True)
    graph_data_dir = package_data_dir / "graph"
    graph_data_dir.mkdir(parents=True, exist_ok=True)
    features_data_dir = package_data_dir / "features"
    features_data_dir.mkdir(parents=True, exist_ok=True)

    config_path = out_dir / "config.json"
    graph_contract = build_graph_learning_contract(layout)
    backend_plan = resolve_trainer_backend(
        str(starter_config.get("family") or ((starter_config.get("model") or {}).get("type")) or ""),
        runtime_target=target,
        native_graph_contract_available=graph_contract.available,
    )
    export_config = dict(starter_config)
    export_config["trainer_backend"] = {
        "requested_family": backend_plan.requested_family,
        "execution_family": backend_plan.execution_family,
        "backend_id": backend_plan.backend_id,
        "implementation": backend_plan.implementation,
        "native_graph": backend_plan.native_graph,
        "warnings": list(backend_plan.warnings),
    }
    export_config["graph_contract"] = {
        "available": graph_contract.available,
        "matched_example_count": graph_contract.matched_example_count,
        "total_example_count": graph_contract.total_example_count,
        "summary": graph_contract.summary,
        "sample_pdb_ids": list(graph_contract.sample_pdb_ids),
    }
    config_path.write_text(json.dumps(export_config, indent=2), encoding="utf-8")

    requirements = [
        "pydantic>=2.7",
        "pyyaml>=6.0.1",
        "typer>=0.12",
        "rich>=13.7",
        "pandas>=2.0",
    ]
    family = str(((starter_config.get("model") or {}).get("type")) or starter_config.get("family") or "")
    if family in {"xgboost", "xgboost_hybrid_baseline"}:
        requirements.append("xgboost>=2.0")
    if family in {"random_forest", "residual_mlp", "autoencoder", "graphsage", "hybrid_graph_attribute_fusion", "clustering"}:
        requirements.append("scikit-learn>=1.5")
    if family in {"residual_mlp", "autoencoder", "graphsage", "hybrid_graph_attribute_fusion"}:
        requirements.append("torch>=2.2")
    if family in {"graphsage", "hybrid_graph_attribute_fusion"}:
        requirements.append("torch-geometric>=2.5")

    (out_dir / "requirements.txt").write_text("\n".join(requirements) + "\n", encoding="utf-8")

    readme_lines = [
        f"# {package_name}",
        "",
        f"Exported at: {_utc_now()}",
        f"Target runtime: {target}",
        "",
        "Contents:",
        "- `config.json`: model plan and dataset contract",
        "- `requirements.txt`: starter dependency list",
        "- `trainer_backend.py`: portable trainer implementation for supported local families",
        "- `train.py`: portable training entrypoint",
        "- `package_data/`: staged training examples, split files, and graph manifests when available",
        "- `import_run.json`: describes what Model Studio expects back after remote training",
        "",
        "Expected return artifacts:",
        "- trained checkpoint/model file",
        "- metrics JSON",
        "- training curves / evaluation plots",
        "- updated run manifest",
        "",
        "Use this package as a starting point for Kaggle, Colab, or cluster execution.",
        "Run `python train.py` after installing dependencies to produce importable model outputs.",
        f"Resolved trainer backend: {backend_plan.backend_id} ({backend_plan.implementation})",
        f"Graph contract: {graph_contract.summary}",
    ]
    if backend_plan.warnings:
        readme_lines.extend(["", "Backend notes:"])
        readme_lines.extend(f"- {warning}" for warning in backend_plan.warnings)
    if target == "kaggle":
        readme_lines.extend([
            "",
            "Kaggle notes:",
            "- Upload the package as a dataset or paste the files into a notebook.",
            "- Point training outputs to `/kaggle/working/model_outputs`.",
        ])
    elif target == "colab":
        readme_lines.extend([
            "",
            "Colab notes:",
            "- Mount Google Drive or upload the package directly.",
            "- Save outputs under a persistent Drive path if you want to re-import them later.",
        ])
    elif target == "cluster":
        readme_lines.extend([
            "",
            "Cluster notes:",
            "- Submit `train.py` with the scheduler wrapper appropriate for your environment.",
            "- Prefer staging datasets on shared storage before execution.",
        ])
    (out_dir / "README.md").write_text("\n".join(readme_lines) + "\n", encoding="utf-8")

    (out_dir / "trainer_backend.py").write_text(portable_training_backend_source(), encoding="utf-8")
    (out_dir / "train.py").write_text(portable_train_entrypoint_source(target), encoding="utf-8")

    training_examples_src = layout.training_dir / "training_examples.json"
    if training_examples_src.exists():
        shutil.copy2(training_examples_src, package_data_dir / "training_examples.json")
    feature_records_src = layout.features_dir / "feature_records.json"
    if feature_records_src.exists():
        shutil.copy2(feature_records_src, features_data_dir / "feature_records.json")
    for graph_name in ("graph_nodes.json", "graph_edges.json"):
        graph_src = layout.graph_dir / graph_name
        if graph_src.exists():
            shutil.copy2(graph_src, graph_data_dir / graph_name)
    if graph_contract.available:
        build_graph_sample_manifest(layout, output_path=graph_data_dir / "graph_sample_manifest.json")
        materialize_graph_dataset_records(layout, output_dir=graph_data_dir / "dataset")
        build_pyg_ready_graph_samples(layout, output_dir=graph_data_dir / "pyg_ready")
        materialize_graph_training_payload(
            layout,
            task=str(export_config.get("task") or "regression"),
            output_dir=graph_data_dir / "training_payload",
        )
        materialize_hybrid_training_payload(
            layout,
            task=str(export_config.get("task") or "regression"),
            output_dir=graph_data_dir / "hybrid_training_payload",
        )
    for split_name in ("train", "val", "test"):
        split_src = layout.splits_dir / f"{split_name}.txt"
        if split_src.exists():
            shutil.copy2(split_src, split_data_dir / f"{split_name}.txt")

    import_manifest = {
        "generated_at": _utc_now(),
        "package_name": package_name,
        "target_runtime": target,
        "expected_files": [
            "model_outputs/run_metrics.json",
            "model_outputs/model.pkl",
            "model_outputs/metrics.json",
            "model_outputs/training_curve.svg",
        ],
        "import_into_model_studio": {
            "destination_dir": str(layout.models_dir / "imported_runs" / package_name),
            "required_any_of": [
                "model_outputs/model.pkl",
                "model_outputs/model.json",
                "model_outputs/run_metrics.json",
            ],
        },
    }
    (out_dir / "import_run.json").write_text(json.dumps(import_manifest, indent=2), encoding="utf-8")
    (out_dir / "runtime_targets.json").write_text(json.dumps({
        "generated_at": _utc_now(),
        "selected_target": target,
        "supported_targets": ["local_cpu", "local_gpu", "cluster", "kaggle", "colab"],
        "recommended_backend": backend_plan.backend_id,
        "graph_contract": export_config["graph_contract"],
    }, indent=2), encoding="utf-8")

    if target == "cluster":
        cluster_script = """#!/bin/bash
set -euo pipefail
python train.py
"""
        (out_dir / "submit.sh").write_text(cluster_script, encoding="utf-8")
        slurm_script = """#!/bin/bash
#SBATCH --job-name=pbdata-model
#SBATCH --output=slurm-%j.out
#SBATCH --time=04:00:00
#SBATCH --cpus-per-task=8
#SBATCH --mem=16G

set -euo pipefail
python train.py
"""
        (out_dir / "slurm_submit.sh").write_text(slurm_script, encoding="utf-8")
    elif target == "kaggle":
        notebook_stub = {
            "cells": [
                {
                    "cell_type": "markdown",
                    "metadata": {},
                    "source": [f"# {package_name}\\n", "Install dependencies, then run the exported trainer.\\n"],
                },
                {
                    "cell_type": "code",
                    "execution_count": None,
                    "metadata": {},
                    "outputs": [],
                    "source": ["!pip install -r requirements.txt\\n"],
                },
                {
                    "cell_type": "code",
                    "execution_count": None,
                    "metadata": {},
                    "outputs": [],
                    "source": ["!python train.py\\n"],
                },
            ],
            "metadata": {"kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"}},
            "nbformat": 4,
            "nbformat_minor": 5,
        }
        (out_dir / "kaggle_notebook.ipynb").write_text(json.dumps(notebook_stub, indent=2), encoding="utf-8")
    elif target == "colab":
        notebook_stub = {
            "cells": [
                {
                    "cell_type": "markdown",
                    "metadata": {},
                    "source": [f"# {package_name}\\n", "Mount storage if needed, install deps, and run the exported trainer.\\n"],
                },
                {
                    "cell_type": "code",
                    "execution_count": None,
                    "metadata": {},
                    "outputs": [],
                    "source": ["!pip install -r requirements.txt\\n"],
                },
                {
                    "cell_type": "code",
                    "execution_count": None,
                    "metadata": {},
                    "outputs": [],
                    "source": ["!python train.py\\n"],
                },
            ],
            "metadata": {"colab": {"name": f"{package_name}.ipynb"}},
            "nbformat": 4,
            "nbformat_minor": 0,
        }
        (out_dir / "colab_notebook.ipynb").write_text(json.dumps(notebook_stub, indent=2), encoding="utf-8")

    # Include a small snapshot of local environment info for debugging portability issues.
    env_info = {
        "generated_at": _utc_now(),
        "python_executable": sys.executable,
        "python_version": platform.python_version(),
        "platform": f"{platform.system()} {platform.release()}",
        "git_available": shutil.which("git") is not None,
    }
    (out_dir / "environment.json").write_text(json.dumps(env_info, indent=2), encoding="utf-8")

    return out_dir
