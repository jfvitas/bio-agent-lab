"""Model Studio profiling, compatibility, and recommendation helpers."""

from __future__ import annotations

import csv
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

from pbdata.storage import StorageLayout

Modality = Literal["attributes", "graphs", "graphs+attributes", "unsupervised"]
TaskType = Literal["regression", "classification", "ranking", "unsupervised"]
Priority = Literal["error", "warning", "info"]


@dataclass(frozen=True)
class DatasetProfile:
    storage_root: str
    dataset_source: str
    example_count: int
    train_count: int
    val_count: int
    test_count: int
    graph_ready: bool
    attribute_ready: bool
    modalities_available: tuple[str, ...]
    tasks_available: tuple[str, ...]
    label_fields: tuple[str, ...]
    quality_flags: tuple[str, ...]
    available_artifacts: dict[str, bool]
    summary: str
    next_action: str


@dataclass(frozen=True)
class ModelStudioSelection:
    dataset_source: str = "auto"
    modality: str = "auto"
    task: str = "auto"
    preferred_family: str = "auto"
    compute_budget: str = "balanced"
    interpretability_priority: str = "balanced"


@dataclass(frozen=True)
class CompatibilityMessage:
    priority: Priority
    title: str
    body: str


@dataclass(frozen=True)
class ModelRecommendation:
    rank: int
    model_id: str
    label: str
    family: str
    modality: str
    supervision: str
    fit_score: float
    summary: str
    why_it_fits: str
    strengths: tuple[str, ...]
    drawbacks: tuple[str, ...]
    starter_recipe: tuple[str, ...]
    compute_cost: str
    interpretability: str
    warnings: tuple[str, ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class StarterModelConfig:
    model_id: str
    label: str
    family: str
    config: dict[str, Any]
    summary: str


@dataclass(frozen=True)
class _Candidate:
    model_id: str
    label: str
    family: str
    modality_support: tuple[str, ...]
    task_support: tuple[str, ...]
    supervision: str
    compute_cost: str
    interpretability: str
    strengths: tuple[str, ...]
    drawbacks: tuple[str, ...]
    recipe: tuple[str, ...]


_CANDIDATES: tuple[_Candidate, ...] = (
    _Candidate(
        model_id="xgboost_hybrid_baseline",
        label="XGBoost on engineered attributes",
        family="xgboost",
        modality_support=("attributes", "graphs+attributes"),
        task_support=("regression", "classification", "ranking"),
        supervision="supervised",
        compute_cost="low",
        interpretability="medium",
        strengths=(
            "Fast, strong baseline for structured numeric/categorical data.",
            "Handles moderate sample sizes and missingness better than most neural nets.",
            "Good first comparator before moving to more complex multimodal models.",
        ),
        drawbacks=(
            "Cannot directly consume full graph topology.",
            "Performance depends on how much signal is preserved in engineered features.",
        ),
        recipe=(
            "Use engineered attributes plus graph summary features when available.",
            "Start with regression for affinity targets and inspect feature importance.",
            "Use as the practical baseline before graph-native experiments.",
        ),
    ),
    _Candidate(
        model_id="random_forest_interpretable",
        label="Random Forest on curated attributes",
        family="random_forest",
        modality_support=("attributes", "graphs+attributes"),
        task_support=("regression", "classification"),
        supervision="supervised",
        compute_cost="low",
        interpretability="high",
        strengths=(
            "Very robust starting point for smaller datasets.",
            "Easy to inspect and explain to collaborators.",
            "Good fit when interpretability matters more than squeezing out the last bit of performance.",
        ),
        drawbacks=(
            "Usually weaker than boosted trees on harder numeric tasks.",
            "Still depends on engineered features rather than native graph structure.",
        ),
        recipe=(
            "Use curated attribute subsets and simple graph summary statistics.",
            "Enable permutation importance and subgroup evaluation.",
            "Compare directly against XGBoost and tabular neural baselines.",
        ),
    ),
    _Candidate(
        model_id="residual_mlp_tabular",
        label="Residual dense neural network",
        family="dense_nn",
        modality_support=("attributes", "graphs+attributes"),
        task_support=("regression", "classification", "ranking"),
        supervision="supervised",
        compute_cost="medium",
        interpretability="low",
        strengths=(
            "Learns nonlinear interactions across diverse feature groups.",
            "Natural stepping stone toward larger multimodal neural systems.",
        ),
        drawbacks=(
            "Needs more data and tuning than tree models.",
            "Harder to interpret and easier to overfit on modest datasets.",
        ),
        recipe=(
            "Use 3-5 dense layers with residual connections, dropout, and normalization.",
            "Pair with early stopping and strict split evaluation.",
            "Best after establishing a tree baseline.",
        ),
    ),
    _Candidate(
        model_id="graphsage_pooling",
        label="GraphSAGE with pooled graph head",
        family="gnn",
        modality_support=("graphs", "graphs+attributes"),
        task_support=("regression", "classification"),
        supervision="supervised",
        compute_cost="high",
        interpretability="low",
        strengths=(
            "Uses graph topology directly instead of compressing everything into tabular summaries.",
            "Good match for structural tasks where local neighborhoods matter.",
        ),
        drawbacks=(
            "Requires graph artifacts and more training infrastructure.",
            "Harder to debug than tabular baselines.",
        ),
        recipe=(
            "Start with 2-3 message-passing layers and global pooling.",
            "Use node and edge attributes from structural graph exports.",
            "Benchmark against engineered-feature XGBoost to justify added complexity.",
        ),
    ),
    _Candidate(
        model_id="hybrid_gnn_mlp_fusion",
        label="Hybrid graph + attribute fusion model",
        family="hybrid_fusion",
        modality_support=("graphs+attributes",),
        task_support=("regression", "classification", "ranking"),
        supervision="supervised",
        compute_cost="high",
        interpretability="low",
        strengths=(
            "Combines topology-sensitive graph encoders with global assay/context attributes.",
            "Best match when both structural graphs and rich feature tables are available.",
        ),
        drawbacks=(
            "Higher engineering and tuning cost.",
            "More ways to misalign modalities or leak information if preprocessing contracts are weak.",
        ),
        recipe=(
            "Encode graphs with a GNN branch and attributes with an MLP branch.",
            "Fuse branches late with concatenation and a small prediction head.",
            "Use only after validating both unimodal baselines.",
        ),
    ),
    _Candidate(
        model_id="cnn_contact_map",
        label="CNN over contact-map or grid representation",
        family="cnn",
        modality_support=("graphs",),
        task_support=("regression", "classification"),
        supervision="supervised",
        compute_cost="high",
        interpretability="low",
        strengths=(
            "Useful when graph exports can be transformed into dense local tensors.",
            "Can capture spatial motifs if you later add grid/contact-map views.",
        ),
        drawbacks=(
            "Current pipeline does not yet materialize a first-class image/grid tensor dataset.",
            "Less natural than a GNN for native graph artifacts.",
        ),
        recipe=(
            "Reserve for later tensorized structural views or contact-map exports.",
            "Treat as an advanced option, not the first benchmark.",
        ),
    ),
    _Candidate(
        model_id="unet_structural_field",
        label="U-Net style structural field model",
        family="unet",
        modality_support=("graphs",),
        task_support=("regression", "classification"),
        supervision="supervised",
        compute_cost="high",
        interpretability="low",
        strengths=(
            "Strong option for dense spatial prediction once volumetric or segmentation-like targets exist.",
            "Good fit for future structural field or site-map tasks.",
        ),
        drawbacks=(
            "Not a natural fit for the current artifact contracts.",
            "Requires grid/tensor inputs that the current pipeline does not yet expose as a primary dataset.",
        ),
        recipe=(
            "Use only after adding voxel/grid exports or segmentation-style supervision.",
            "Keep as a roadmap architecture rather than an MVP default.",
        ),
    ),
    _Candidate(
        model_id="autoencoder_cluster",
        label="Autoencoder + clustering explorer",
        family="autoencoder",
        modality_support=("attributes", "graphs+attributes", "unsupervised"),
        task_support=("unsupervised",),
        supervision="unsupervised",
        compute_cost="medium",
        interpretability="medium",
        strengths=(
            "Good for exploring structure/function organization before choosing a supervised target.",
            "Can reveal redundancy, outliers, and latent clusters in curated training examples.",
        ),
        drawbacks=(
            "Does not directly optimize a prediction target.",
            "Latent clusters still need scientific interpretation.",
        ),
        recipe=(
            "Train an autoencoder on standardized attributes or fused embeddings.",
            "Run clustering and outlier analysis on the latent space.",
            "Use the results to refine splits, labels, or supervised architectures.",
        ),
    ),
    _Candidate(
        model_id="pca_hdbscan_explorer",
        label="PCA/UMAP + HDBSCAN explorer",
        family="clustering",
        modality_support=("attributes", "graphs+attributes", "unsupervised"),
        task_support=("unsupervised",),
        supervision="unsupervised",
        compute_cost="low",
        interpretability="high",
        strengths=(
            "Very fast exploratory baseline for understanding dataset geometry.",
            "Useful for detecting clusters, anomalies, and redundancy before training a supervised model.",
        ),
        drawbacks=(
            "Exploratory only, not a predictive endpoint.",
            "Cluster stability can vary with preprocessing choices.",
        ),
        recipe=(
            "Use standardized engineered attributes or latent embeddings.",
            "Inspect clusters by family, ligand class, and source provenance.",
            "Pair with release review and leakage checks.",
        ),
    ),
)


def _read_json(path: Path) -> Any:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    try:
        with path.open(newline="", encoding="utf-8") as handle:
            return list(csv.DictReader(handle))
    except Exception:
        return []


def _read_text_lines(path: Path) -> list[str]:
    if not path.exists():
        return []
    try:
        return [line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    except Exception:
        return []


def _available_dataset_source(layout: StorageLayout) -> str:
    if (layout.workspace_datasets_dir / "engineered_dataset" / "train.csv").exists():
        return "engineered_dataset"
    if (layout.root / "custom_training_set.csv").exists():
        return "custom_training_set"
    if (layout.training_dir / "training_examples.json").exists():
        return "training_examples"
    return "workspace"


def _label_fields(training_examples: list[dict[str, Any]]) -> tuple[str, ...]:
    fields: dict[str, None] = {}
    for row in training_examples:
        labels = row.get("labels") or {}
        if not isinstance(labels, dict):
            continue
        for key in labels:
            if key:
                fields[str(key)] = None
    return tuple(fields)


def _tasks_from_labels(label_fields: tuple[str, ...]) -> tuple[str, ...]:
    tasks: list[str] = []
    if any(field in {"binding_affinity_log10", "binding_affinity_raw", "delta_delta_g"} for field in label_fields):
        tasks.append("regression")
    if any(field in {"affinity_type", "source_conflict_flag", "is_mutant"} for field in label_fields):
        tasks.append("classification")
    if label_fields:
        tasks.append("ranking")
    tasks.append("unsupervised")
    return tuple(dict.fromkeys(tasks))


def build_dataset_profile(layout: StorageLayout, *, dataset_source: str = "auto") -> DatasetProfile:
    training_examples = _read_json(layout.training_dir / "training_examples.json")
    training_rows = training_examples if isinstance(training_examples, list) else []
    train_ids = _read_text_lines(layout.splits_dir / "train.txt")
    val_ids = _read_text_lines(layout.splits_dir / "val.txt")
    test_ids = _read_text_lines(layout.splits_dir / "test.txt")
    graph_nodes = _read_json(layout.graph_dir / "graph_nodes.json")
    graph_edges = _read_json(layout.graph_dir / "graph_edges.json")
    feature_records = _read_json(layout.features_dir / "feature_records.json")
    engineered_train = _read_csv_rows(layout.workspace_datasets_dir / "engineered_dataset" / "train.csv")
    custom_rows = _read_csv_rows(layout.root / "custom_training_set.csv")

    graph_ready = isinstance(graph_nodes, list) and bool(graph_nodes) and isinstance(graph_edges, list) and bool(graph_edges)
    attribute_ready = bool(training_rows or engineered_train or custom_rows or isinstance(feature_records, list) and feature_records)
    modalities: list[str] = []
    if attribute_ready:
        modalities.append("attributes")
    if graph_ready:
        modalities.append("graphs")
    if attribute_ready and graph_ready:
        modalities.append("graphs+attributes")
    modalities.append("unsupervised")

    label_fields = _label_fields(training_rows)
    tasks_available = _tasks_from_labels(label_fields)
    source_name = _available_dataset_source(layout) if dataset_source == "auto" else dataset_source
    example_count = (
        len(engineered_train) if source_name == "engineered_dataset" and engineered_train
        else len(custom_rows) if source_name == "custom_training_set" and custom_rows
        else len(training_rows)
    )

    quality_flags: list[str] = []
    if not train_ids or not val_ids or not test_ids:
        quality_flags.append("missing_standard_splits")
    if not training_rows:
        quality_flags.append("training_examples_missing")
    if not graph_ready:
        quality_flags.append("graph_artifacts_missing")
    if not attribute_ready:
        quality_flags.append("attribute_artifacts_missing")

    summary = (
        f"{example_count:,} examples available; modalities="
        f"{', '.join(modalities)}; tasks={', '.join(tasks_available)}"
    )
    if training_rows:
        next_action = "Use the recommendations below to choose a baseline, then compare against a graph-native or hybrid model."
    else:
        next_action = "Run build-training-examples and build-splits first so Model Studio can recommend trainable architectures."

    return DatasetProfile(
        storage_root=str(layout.root),
        dataset_source=source_name,
        example_count=example_count,
        train_count=len(train_ids),
        val_count=len(val_ids),
        test_count=len(test_ids),
        graph_ready=graph_ready,
        attribute_ready=attribute_ready,
        modalities_available=tuple(dict.fromkeys(modalities)),
        tasks_available=tasks_available,
        label_fields=label_fields,
        quality_flags=tuple(quality_flags),
        available_artifacts={
            "training_examples": bool(training_rows),
            "splits": bool(train_ids or val_ids or test_ids),
            "graph_nodes": graph_ready,
            "feature_records": attribute_ready,
            "engineered_dataset": bool(engineered_train),
            "custom_training_set": bool(custom_rows),
        },
        summary=summary,
        next_action=next_action,
    )


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def validate_model_studio_selection(
    profile: DatasetProfile,
    selection: ModelStudioSelection,
) -> list[CompatibilityMessage]:
    messages: list[CompatibilityMessage] = []
    modality = selection.modality if selection.modality != "auto" else (
        "graphs+attributes" if "graphs+attributes" in profile.modalities_available
        else "attributes" if "attributes" in profile.modalities_available
        else "graphs" if "graphs" in profile.modalities_available
        else "unsupervised"
    )
    task = selection.task if selection.task != "auto" else (
        "regression" if "regression" in profile.tasks_available
        else "classification" if "classification" in profile.tasks_available
        else "unsupervised"
    )

    if profile.example_count == 0 and task != "unsupervised":
        messages.append(CompatibilityMessage(
            priority="error",
            title="No training examples",
            body="Supervised training needs assembled training examples. Run build-training-examples first.",
        ))
    if task != "unsupervised" and not profile.available_artifacts.get("splits", False):
        messages.append(CompatibilityMessage(
            priority="warning",
            title="Splits missing",
            body="Training without explicit leakage-aware splits is risky. Run build-splits before model comparison.",
        ))
    if task in {"regression", "classification", "ranking"} and not profile.label_fields:
        messages.append(CompatibilityMessage(
            priority="error",
            title="No supervised labels detected",
            body="The current training examples do not expose label fields for supervised learning.",
        ))
    if modality == "graphs" and not profile.graph_ready:
        messages.append(CompatibilityMessage(
            priority="error",
            title="Graph artifacts unavailable",
            body="Graph-native models need graph_nodes.json and graph_edges.json from the graph stages.",
        ))
    if modality == "attributes" and not profile.attribute_ready:
        messages.append(CompatibilityMessage(
            priority="error",
            title="Attribute artifacts unavailable",
            body="Attribute-based models need training examples, engineered datasets, or feature records.",
        ))
    if modality == "graphs+attributes" and (not profile.graph_ready or not profile.attribute_ready):
        messages.append(CompatibilityMessage(
            priority="error",
            title="Hybrid inputs incomplete",
            body="Hybrid graph+attribute models need both graph artifacts and attribute-style training data.",
        ))
    family = selection.preferred_family
    if family in {"cnn", "unet"} and modality not in {"graphs"}:
        messages.append(CompatibilityMessage(
            priority="warning",
            title="Tensor architecture mismatch",
            body="CNN/U-Net style models are best reserved for grid/contact-map style inputs. The current pipeline is stronger on graphs and tabular attributes.",
        ))
    if family == "random_forest" and modality == "graphs":
        messages.append(CompatibilityMessage(
            priority="warning",
            title="Forest cannot use raw graphs",
            body="Random Forest expects attributes. Use graph summary features or choose a GNN/hybrid model instead.",
        ))
    if family == "hybrid_fusion" and modality != "graphs+attributes":
        messages.append(CompatibilityMessage(
            priority="warning",
            title="Hybrid model underfed",
            body="Hybrid fusion models make the most sense when both graph and attribute branches are available.",
        ))
    if family in {"dense_nn", "gnn", "hybrid_fusion"} and profile.example_count and profile.example_count < 250:
        messages.append(CompatibilityMessage(
            priority="warning",
            title="Small dataset for deep model",
            body="Deep architectures may overfit with fewer than ~250 examples unless strong regularization or pretraining is used.",
        ))
    if task == "unsupervised":
        messages.append(CompatibilityMessage(
            priority="info",
            title="Unsupervised mode",
            body="Unsupervised workflows are useful for structure/function exploration, anomaly detection, and pretraining rather than direct endpoint prediction.",
        ))
    return messages


def _effective_modality(profile: DatasetProfile, selection: ModelStudioSelection) -> str:
    if selection.modality != "auto":
        return selection.modality
    if "graphs+attributes" in profile.modalities_available:
        return "graphs+attributes"
    if "attributes" in profile.modalities_available:
        return "attributes"
    if "graphs" in profile.modalities_available:
        return "graphs"
    return "unsupervised"


def _effective_task(profile: DatasetProfile, selection: ModelStudioSelection) -> str:
    if selection.task != "auto":
        return selection.task
    if "regression" in profile.tasks_available:
        return "regression"
    if "classification" in profile.tasks_available:
        return "classification"
    return "unsupervised"


def _score_candidate(candidate: _Candidate, profile: DatasetProfile, selection: ModelStudioSelection) -> tuple[float, list[str]]:
    modality = _effective_modality(profile, selection)
    task = _effective_task(profile, selection)
    reasons: list[str] = []
    score = 0.0

    if modality in candidate.modality_support:
        score += 4.0
        reasons.append(f"Matches the available {modality} modality.")
    elif modality == "graphs+attributes" and "attributes" in candidate.modality_support:
        score += 2.0
        reasons.append("Can use the attribute portion of a hybrid dataset as a solid baseline.")
    else:
        score -= 6.0
        reasons.append("Modality fit is weak for the currently selected data representation.")

    if task in candidate.task_support:
        score += 3.0
        reasons.append(f"Supports the current {task} objective.")
    else:
        score -= 5.0
        reasons.append("Task fit is weak for the chosen prediction objective.")

    if selection.preferred_family != "auto" and selection.preferred_family == candidate.family:
        score += 2.5
        reasons.append("Matches the currently preferred model family.")

    if selection.compute_budget == candidate.compute_cost:
        score += 1.5
    elif selection.compute_budget == "balanced" and candidate.compute_cost in {"medium", "low"}:
        score += 1.0
    elif selection.compute_budget == "low" and candidate.compute_cost == "high":
        score -= 2.0
        reasons.append("Higher compute cost than requested.")

    if selection.interpretability_priority == candidate.interpretability:
        score += 1.5
    elif selection.interpretability_priority == "high" and candidate.interpretability == "low":
        score -= 1.5
        reasons.append("Interpretability is weaker than requested.")

    if profile.example_count < 500 and candidate.family in {"dense_nn", "gnn", "hybrid_fusion"}:
        score -= 1.5
        reasons.append("This architecture may be data-hungry for the current corpus size.")
    if profile.example_count >= 1000 and candidate.family in {"xgboost", "dense_nn", "gnn", "hybrid_fusion"}:
        score += 0.8

    if candidate.supervision == "unsupervised" and task == "unsupervised":
        score += 3.0
    if candidate.supervision == "supervised" and task == "unsupervised":
        score -= 8.0

    return score, reasons


def recommend_model_architectures(
    profile: DatasetProfile,
    selection: ModelStudioSelection,
) -> list[ModelRecommendation]:
    ranked: list[ModelRecommendation] = []
    for candidate in _CANDIDATES:
        score, reasons = _score_candidate(candidate, profile, selection)
        if score <= -2.0:
            continue
        warnings: list[str] = []
        if candidate.family in {"cnn", "unet"}:
            warnings.append("Advanced roadmap option: current pipeline does not yet expose first-class grid/tensor datasets.")
        if candidate.family == "hybrid_fusion" and not (profile.graph_ready and profile.attribute_ready):
            warnings.append("Requires both graph and attribute artifacts.")
        ranked.append(ModelRecommendation(
            rank=0,
            model_id=candidate.model_id,
            label=candidate.label,
            family=candidate.family,
            modality=selection.modality if selection.modality != "auto" else _effective_modality(profile, selection),
            supervision=candidate.supervision,
            fit_score=round(score, 3),
            summary=f"{candidate.label} is a {candidate.compute_cost}-compute {candidate.family} option for {candidate.supervision} learning.",
            why_it_fits=" ".join(reasons[:3]),
            strengths=candidate.strengths,
            drawbacks=candidate.drawbacks,
            starter_recipe=candidate.recipe,
            compute_cost=candidate.compute_cost,
            interpretability=candidate.interpretability,
            warnings=tuple(warnings),
        ))
    ranked.sort(key=lambda item: (-item.fit_score, item.compute_cost, item.label))
    top = ranked[:3]
    return [
        ModelRecommendation(
            rank=index,
            model_id=item.model_id,
            label=item.label,
            family=item.family,
            modality=item.modality,
            supervision=item.supervision,
            fit_score=item.fit_score,
            summary=item.summary,
            why_it_fits=item.why_it_fits,
            strengths=item.strengths,
            drawbacks=item.drawbacks,
            starter_recipe=item.starter_recipe,
            compute_cost=item.compute_cost,
            interpretability=item.interpretability,
            warnings=item.warnings,
        )
        for index, item in enumerate(top, start=1)
    ]


def build_starter_model_config(
    profile: DatasetProfile,
    recommendation: ModelRecommendation,
    selection: ModelStudioSelection,
) -> StarterModelConfig:
    task = _effective_task(profile, selection)
    modality = recommendation.modality
    config: dict[str, Any] = {
        "generated_at": _utc_now(),
        "model_id": recommendation.model_id,
        "family": recommendation.family,
        "label": recommendation.label,
        "dataset_source": profile.dataset_source,
        "storage_root": profile.storage_root,
        "task": task,
        "modality": modality,
        "compute_budget": selection.compute_budget,
        "interpretability_priority": selection.interpretability_priority,
        "artifacts": {
            "training_examples": "data/training_examples/training_examples.json",
            "splits_dir": "data/splits",
            "graph_dir": "data/graph" if profile.graph_ready else None,
            "features_dir": "data/features" if profile.attribute_ready else None,
        },
        "training": {
            "seed": 42,
            "early_stopping": True,
            "monitor": "val_loss" if task == "regression" else "val_metric",
        },
        "evaluation": {
            "report_calibration": task in {"classification", "ranking"},
            "report_subgroups": True,
            "report_feature_importance": recommendation.family in {"random_forest", "xgboost"},
        },
    }
    if recommendation.family == "random_forest":
        config["model"] = {
            "type": "random_forest",
            "n_estimators": 500,
            "max_depth": None,
            "min_samples_leaf": 1,
            "use_graph_summaries": modality == "graphs+attributes",
        }
    elif recommendation.family == "xgboost":
        config["model"] = {
            "type": "xgboost",
            "n_estimators": 800,
            "max_depth": 8,
            "learning_rate": 0.05,
            "subsample": 0.8,
            "colsample_bytree": 0.8,
            "use_graph_summaries": modality == "graphs+attributes",
        }
    elif recommendation.family == "dense_nn":
        config["model"] = {
            "type": "residual_mlp",
            "hidden_dims": [256, 256, 128],
            "dropout": 0.2,
            "normalization": "layernorm",
            "activation": "gelu",
        }
        config["training"].update({"batch_size": 64, "epochs": 80, "optimizer": "adamw", "learning_rate": 1e-3})
    elif recommendation.family == "gnn":
        config["model"] = {
            "type": "graphsage",
            "hidden_dim": 128,
            "message_passing_layers": 3,
            "pooling": "mean",
            "use_edge_features": True,
        }
        config["training"].update({"batch_size": 16, "epochs": 120, "optimizer": "adamw", "learning_rate": 5e-4})
    elif recommendation.family == "hybrid_fusion":
        config["model"] = {
            "type": "hybrid_graph_attribute_fusion",
            "graph_encoder": {
                "family": "graphsage",
                "hidden_dim": 128,
                "message_passing_layers": 3,
                "pooling": "mean",
            },
            "attribute_encoder": {
                "family": "residual_mlp",
                "hidden_dims": [256, 128],
                "dropout": 0.2,
            },
            "fusion": {
                "method": "concatenate",
                "head_hidden_dims": [128, 64],
            },
        }
        config["training"].update({"batch_size": 16, "epochs": 120, "optimizer": "adamw", "learning_rate": 3e-4})
    elif recommendation.family == "autoencoder":
        config["model"] = {
            "type": "autoencoder",
            "encoder_dims": [256, 128, 32],
            "decoder_dims": [128, 256],
            "latent_dim": 32,
            "posthoc_clustering": "hdbscan",
        }
        config["training"].update({"batch_size": 64, "epochs": 100, "optimizer": "adamw", "learning_rate": 1e-3})
    else:
        config["model"] = {
            "type": recommendation.family,
            "notes": "Roadmap/advanced architecture; adapt once the corresponding data representation is fully materialized.",
        }
    return StarterModelConfig(
        model_id=recommendation.model_id,
        label=recommendation.label,
        family=recommendation.family,
        config=config,
        summary=f"Starter config for {recommendation.label} using {profile.dataset_source} and {modality} inputs.",
    )


def export_starter_model_config(
    layout: StorageLayout,
    starter: StarterModelConfig,
) -> Path:
    out_dir = layout.models_dir / "model_studio"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{starter.model_id}_starter.json"
    out_path.write_text(json.dumps(starter.config, indent=2), encoding="utf-8")
    return out_path
