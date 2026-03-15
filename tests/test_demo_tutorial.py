from pbdata.demo_tutorial import build_demo_tutorial_steps, next_demo_tutorial_step


def test_demo_tutorial_branches_for_custom_set_and_model_family() -> None:
    selection = {
        "custom_set_mode": "protein_protein",
        "custom_set_target_size": "240",
        "model_family": "hybrid_fusion",
        "model_modality": "graphs+attributes",
        "model_runtime_target": "local_gpu",
    }

    steps = build_demo_tutorial_steps(selection)

    assert any("protein-protein" in step.detail.lower() for step in steps)
    assert any("hybrid" in step.title.lower() for step in steps)


def test_next_demo_tutorial_step_advances_after_completed_actions() -> None:
    selection = {
        "custom_set_mode": "generalist",
        "custom_set_target_size": "500",
        "model_family": "auto",
        "model_modality": "auto",
        "model_runtime_target": "local_cpu",
    }

    step = next_demo_tutorial_step(selection, {"search.preview_rcsb", "pipeline.run_full"})

    assert step.key == "custom_dataset"
