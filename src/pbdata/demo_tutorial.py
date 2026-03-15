"""Guided tutorial logic for Demo Mode."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class DemoTutorialStep:
    key: str
    title: str
    detail: str
    innovation: str
    instruction: str
    scroll_hint: str
    target_ids: tuple[str, ...]


def _custom_set_detail(selection: dict[str, Any]) -> tuple[str, str]:
    mode = str(selection.get("custom_set_mode") or "generalist")
    target_size = str(selection.get("custom_set_target_size") or "500")
    if mode == "protein_ligand":
        return (
            f"Build a protein-ligand focused training slice targeting about {target_size} rows.",
            "This demonstrates that the platform can re-balance the corpus around assay context and ligand chemistry instead of treating every structure as interchangeable.",
        )
    if mode == "protein_protein":
        return (
            f"Build a protein-protein interaction slice targeting about {target_size} rows.",
            "This shows the workflow can pivot from small-molecule binding into interface-driven supervision without rebuilding the entire workspace from scratch.",
        )
    if mode == "mutation_effect":
        return (
            f"Build a mutation-effect slice targeting about {target_size} rows.",
            "This highlights mutation-aware grouping and the idea that leakage-resistant datasets need biological neighborhood logic, not just random rows.",
        )
    if mode == "high_trust":
        return (
            f"Build a high-trust curated slice targeting about {target_size} rows.",
            "This demonstrates that the dataset builder can prioritize provenance quality and conflict reduction when you want a conservative benchmark set.",
        )
    return (
        f"Build a generalist balanced slice targeting about {target_size} rows.",
        "This shows the platform's main curation value: broad coverage, capped redundancy, and representative training examples instead of a narrow cluster of lookalikes.",
    )


def _model_detail(selection: dict[str, Any]) -> tuple[str, str, str]:
    family = str(selection.get("model_family") or "auto")
    modality = str(selection.get("model_modality") or "auto")
    runtime = str(selection.get("model_runtime_target") or "local_cpu")
    if family == "hybrid_fusion":
        return (
            "Run a hybrid graph-plus-attribute model.",
            "This is one of the more distinctive parts of the platform: it combines graph topology with engineered biological context instead of forcing everything into one feature surface.",
            f"Use the current {modality} setting and launch a {runtime} run from the first recommendation card.",
        )
    if family == "gnn":
        return (
            "Run a native graph model.",
            "This shows how the app can move beyond flat tables and reason over residue, ligand, and interaction structure directly.",
            f"Keep the modality aligned with {modality} and run the top graph recommendation on {runtime}.",
        )
    if family in {"random_forest", "xgboost", "dense_nn"}:
        return (
            f"Run a {family} tabular model.",
            "This demonstrates that the same curated dataset can support simpler, more interpretable baselines alongside heavier graph-native approaches.",
            f"Use the current runtime target of {runtime} and compare the saved run against the seeded demo runs afterward.",
        )
    if family in {"autoencoder", "clustering"}:
        return (
            f"Run a {family} exploratory model.",
            "This shows Model Studio is not limited to prediction. It can also surface structure in the dataset itself for curation and hypothesis-building.",
            f"Launch the current unsupervised-style recommendation on {runtime} and inspect the saved charts.",
        )
    return (
        "Run the top recommended local model.",
        "This step ties the whole platform together: curated data, split logic, and backend selection turn into an executable experiment with saved metrics and plots.",
        f"Refresh recommendations if needed, then run the first local recommendation on {runtime}.",
    )


def build_demo_tutorial_steps(selection: dict[str, Any]) -> list[DemoTutorialStep]:
    custom_detail, custom_innovation = _custom_set_detail(selection)
    model_title, model_innovation, model_instruction = _model_detail(selection)
    return [
        DemoTutorialStep(
            key="search_preview",
            title="Preview A Broad Search",
            detail=(
                "Start by previewing the structural search slice. In demo mode this is simulated instantly, "
                "but it still illustrates the intended broad, representative search behavior."
            ),
            innovation=(
                "The useful idea here is representative intake: the platform is designed to avoid a demo that only shows a narrow cluster of near-duplicate structures."
            ),
            instruction="Open the Search Criteria tab and click Preview RCSB Search.",
            scroll_hint="If you do not see it, scroll to the bottom of the Search Criteria tab where the action buttons live.",
            target_ids=("search.preview_rcsb",),
        ),
        DemoTutorialStep(
            key="run_pipeline",
            title="Run The End-To-End Workflow",
            detail=(
                "Next, run the full pipeline so the user can see ingestion, extraction, graph building, feature generation, dataset assembly, and release-style outputs progress in one place."
            ),
            innovation=(
                "What is unusual here is that the workflow spans source ingestion, canonicalization, graph construction, curation, dataset assembly, and reporting inside one GUI rather than leaving them as disconnected scripts."
            ),
            instruction="Click Run Full Pipeline in the right-side pipeline panel.",
            scroll_hint="Look near the top of the right-side workflow area under Run Status and Pipeline.",
            target_ids=("pipeline.run_full",),
        ),
        DemoTutorialStep(
            key="custom_dataset",
            title="Shape A Training Set",
            detail=custom_detail,
            innovation=custom_innovation,
            instruction="In the training-set builder area, click Build Custom Set after reviewing the current selection mode and target size.",
            scroll_hint="Scroll through the right-side overview until you reach the Training Set Builder and workflow actions.",
            target_ids=("training.build_custom_set",),
        ),
        DemoTutorialStep(
            key="model_refresh",
            title="Profile The Workspace",
            detail=(
                "Open Model Studio and refresh recommendations so the tutorial can explain why certain model families fit the current dataset better than others."
            ),
            innovation=(
                "The intended value is not just training a model. It is teaching the user how dataset modality, supervision style, compute budget, and interpretability interact."
            ),
            instruction="Open the Model Studio tab and click Refresh Recommendations.",
            scroll_hint="The button appears at the top of Model Studio and again in the saved-run action area.",
            target_ids=("model.refresh",),
        ),
        DemoTutorialStep(
            key="model_train",
            title=model_title,
            detail=(
                "Now launch a local simulated run so the user can see plausible metrics, saved artifacts, charts, and experiment metadata change in response to the selected design."
            ),
            innovation=model_innovation,
            instruction=model_instruction,
            scroll_hint="Use the recommendation cards in Model Studio. The guided button will highlight the first Run Locally action.",
            target_ids=("model.run_local.primary",),
        ),
        DemoTutorialStep(
            key="compare_runs",
            title="Compare Saved Experiments",
            detail=(
                "After the run completes, compare it against the seeded demo runs so the user sees Model Studio behave like an experiment tracker rather than a single-shot trainer."
            ),
            innovation=(
                "This matters because scientific model work depends on comparing tradeoffs, not just reporting one metric from one lucky run."
            ),
            instruction="Click Compare Saved Runs in Model Studio.",
            scroll_hint="Stay in Model Studio and look in the action row below the selected-run and chart preview area.",
            target_ids=("model.compare_runs",),
        ),
        DemoTutorialStep(
            key="saved_inference",
            title="Run Saved-Model Inference",
            detail=(
                "Finish by running saved-model inference on a PDB ID so the walkthrough closes the loop from broad structural intake to a concrete downstream prediction surface."
            ),
            innovation=(
                "This demonstrates that the app is meant to become a reusable decision-support platform, not just an offline dataset preprocessor."
            ),
            instruction="Enter a demo PDB like D010 and click Run Saved-Model Inference.",
            scroll_hint="The inference controls are in the lower Model Studio preview section.",
            target_ids=("model.saved_inference",),
        ),
    ]


def next_demo_tutorial_step(selection: dict[str, Any], completed_actions: set[str]) -> DemoTutorialStep:
    steps = build_demo_tutorial_steps(selection)
    completion_map = {
        "search_preview": {"search.preview_rcsb"},
        "run_pipeline": {"pipeline.run_full"},
        "custom_dataset": {"training.build_custom_set", "training.run_workflow"},
        "model_refresh": {"model.refresh"},
        "model_train": {"model.run_local"},
        "compare_runs": {"model.compare_runs"},
        "saved_inference": {"model.inference"},
    }
    for step in steps:
        required = completion_map.get(step.key, set())
        if not required.issubset(completed_actions):
            return step
    return steps[-1]
