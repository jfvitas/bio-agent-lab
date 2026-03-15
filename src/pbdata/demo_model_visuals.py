"""Simple architecture descriptions for demo-facing Model Studio visuals."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class DemoArchitectureSpec:
    family: str
    title: str
    subtitle: str
    left_label: str
    center_label: str
    right_label: str
    footer: str


def architecture_spec_for_selection(family: str, modality: str, task: str) -> DemoArchitectureSpec:
    normalized_family = (family or "auto").strip().lower()
    normalized_modality = (modality or "auto").strip().lower()
    normalized_task = (task or "auto").strip().lower()
    if normalized_family == "hybrid_fusion":
        return DemoArchitectureSpec(
            family="hybrid_fusion",
            title="Hybrid Fusion Architecture",
            subtitle="Graph stream + attribute stream merged into one prediction head",
            left_label="Residue / ligand graph",
            center_label="Fusion block",
            right_label=f"{normalized_task.title()} output",
            footer="Best when you want structure-aware learning without giving up curated biological features.",
        )
    if normalized_family == "gnn":
        return DemoArchitectureSpec(
            family="gnn",
            title="Graph Neural Network",
            subtitle="Message passing over structural neighborhoods",
            left_label="Graph nodes",
            center_label="Message passing",
            right_label=f"{normalized_task.title()} output",
            footer="Best when topology and contacts matter more than tabular interpretability.",
        )
    if normalized_family in {"xgboost", "random_forest"}:
        return DemoArchitectureSpec(
            family=normalized_family,
            title=f"{normalized_family.replace('_', ' ').title()} Baseline",
            subtitle="Structured engineered features into an ensemble predictor",
            left_label=f"{normalized_modality.title()} features",
            center_label="Tree ensemble",
            right_label=f"{normalized_task.title()} output",
            footer="Good for quick comparisons, stronger interpretability, and compact local demos.",
        )
    if normalized_family == "dense_nn":
        return DemoArchitectureSpec(
            family="dense_nn",
            title="Dense Neural Network",
            subtitle="Stacked hidden layers over engineered features",
            left_label=f"{normalized_modality.title()} features",
            center_label="Dense layers",
            right_label=f"{normalized_task.title()} output",
            footer="Useful when you want a neural baseline without committing to graph-native execution.",
        )
    if normalized_family in {"clustering", "autoencoder"}:
        return DemoArchitectureSpec(
            family=normalized_family,
            title=f"{normalized_family.title()} Explorer",
            subtitle="Unsupervised structure discovery over the curated corpus",
            left_label=f"{normalized_modality.title()} inputs",
            center_label="Latent structure",
            right_label="Clusters / embeddings",
            footer="Best for exploration, diversity mapping, and explaining the geometry of the dataset.",
        )
    return DemoArchitectureSpec(
        family="auto",
        title="Auto-Selected Model Path",
        subtitle="Model Studio will infer a suitable path from modality, task, and runtime choices",
        left_label=f"{normalized_modality.title()} inputs",
        center_label="Recommendation engine",
        right_label=f"{normalized_task.title()} output",
        footer="Use this when you want the app to explain the tradeoffs before committing to a family.",
    )
