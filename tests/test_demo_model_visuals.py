from pbdata.demo_model_visuals import architecture_spec_for_selection


def test_architecture_spec_for_hybrid_fusion_is_graph_and_attribute_oriented() -> None:
    spec = architecture_spec_for_selection("hybrid_fusion", "graphs+attributes", "regression")

    assert "Hybrid" in spec.title
    assert "graph" in spec.subtitle.lower() or "graph" in spec.left_label.lower()
    assert "fusion" in spec.center_label.lower()


def test_architecture_spec_for_tabular_baseline_mentions_tree_ensemble() -> None:
    spec = architecture_spec_for_selection("xgboost", "attributes", "classification")

    assert "Baseline" in spec.title
    assert "Tree ensemble" == spec.center_label
