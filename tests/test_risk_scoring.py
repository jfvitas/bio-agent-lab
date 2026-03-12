import json
from pathlib import Path
from uuid import uuid4

from typer.testing import CliRunner

from pbdata.cli import app
from pbdata.risk.summary import build_pathway_risk_summary
from pbdata.storage import build_storage_layout

_LOCAL_TMP = Path(__file__).parent / "_tmp"
_LOCAL_TMP.mkdir(exist_ok=True)


def _tmp_dir(name: str) -> Path:
    path = _LOCAL_TMP / f"{uuid4().hex}_{name}"
    path.mkdir(exist_ok=True)
    return path


def test_pathway_risk_summary_writes_formula_outputs() -> None:
    tmp_root = _tmp_dir("risk")
    layout = build_storage_layout(tmp_root)
    (layout.root / "model_ready_pairs.csv").write_text(
        "pdb_id,pair_identity_key,receptor_uniprot_ids,binding_affinity_type,reported_measurement_mean_log10_standardized,source_conflict_flag\n"
        "1ABC,protein_ligand|1ABC|A|ATP|wt,P12345,Kd,1.2,false\n"
        "2DEF,protein_ligand|2DEF|A|GTP|wt,P12345,Kd,0.8,true\n",
        encoding="utf-8",
    )
    (layout.root / "scientific_coverage_summary.json").write_text(
        json.dumps({"pathway_sources": ["Reactome"]}),
        encoding="utf-8",
    )
    layout.graph_dir.mkdir(parents=True, exist_ok=True)
    (layout.graph_dir / "graph_nodes.json").write_text(
        json.dumps([
            {"node_id": "protein:P12345", "node_type": "Protein", "primary_id": "P12345"},
            {"node_id": "pathway:R-HSA-1", "node_type": "Pathway", "primary_id": "R-HSA-1"},
            {"node_id": "pathway:R-HSA-2", "node_type": "Pathway", "primary_id": "R-HSA-2"},
        ]),
        encoding="utf-8",
    )
    (layout.graph_dir / "graph_edges.json").write_text(
        json.dumps([
            {"edge_id": "e1", "edge_type": "ProteinPathway", "source_node_id": "protein:P12345", "target_node_id": "pathway:R-HSA-1", "source_database": "Reactome"},
            {"edge_id": "e2", "edge_type": "ProteinPathway", "source_node_id": "protein:P12345", "target_node_id": "pathway:R-HSA-2", "source_database": "Reactome"},
        ]),
        encoding="utf-8",
    )
    (layout.prediction_dir / "ligand_screening").mkdir(parents=True, exist_ok=True)
    (layout.prediction_dir / "ligand_screening" / "prediction_manifest.json").write_text(
        json.dumps({
            "prediction_method": "trained_ligand_memory_model",
            "ranked_target_list": [
                {"target_id": "P12345", "rank": 1, "confidence_score": 0.81, "predicted_kd_nM": 5.0}
            ]
        }),
        encoding="utf-8",
    )

    out_path, summary = build_pathway_risk_summary(layout, targets=["P12345"])

    assert out_path.exists()
    assert summary["matching_pair_count"] == 2
    assert summary["source_conflict_pair_count"] == 1
    assert summary["risk_score"] > 0.0
    assert summary["risk_score_is_placeholder"] is False
    assert summary["pathway_similarity_method"] == "target_pair_pathway_jaccard"
    assert summary["prediction_context_available"] is True
    assert summary["prediction_method"] == "trained_ligand_memory_model"
    assert summary["pathway_overlap_count"] == 2
    assert summary["pathway_source_databases"] == ["Reactome"]
    assert summary["predicted_target_matches"][0]["target_id"] == "P12345"
    saved = json.loads(out_path.read_text(encoding="utf-8"))
    assert saved["severity_level"] in {"low", "medium", "high"}
    assert "not a trained or clinically calibrated risk model" in saved["notes"]


def test_pathway_risk_summary_falls_back_cleanly_without_graph_pathways() -> None:
    tmp_root = _tmp_dir("risk_no_graph")
    layout = build_storage_layout(tmp_root)
    (layout.root / "model_ready_pairs.csv").write_text(
        "pdb_id,pair_identity_key,receptor_uniprot_ids,binding_affinity_type,reported_measurement_mean_log10_standardized,source_conflict_flag\n"
        "1ABC,protein_ligand|1ABC|A|ATP|wt,P12345,Kd,1.2,false\n",
        encoding="utf-8",
    )

    out_path, summary = build_pathway_risk_summary(layout, targets=["P12345"])

    assert out_path.exists()
    assert summary["pathway_context_available"] is False
    assert summary["pathway_similarity_method"] == "graph_pathways_unavailable_fallback"
    assert summary["pathway_overlap_count"] == 0
    assert summary["risk_score_is_placeholder"] is False


def test_score_pathway_risk_cli_requires_targets() -> None:
    tmp_root = _tmp_dir("risk_cli_no_targets")
    runner = CliRunner()

    result = runner.invoke(
        app,
        ["--storage-root", str(tmp_root), "score-pathway-risk"],
        catch_exceptions=False,
    )

    assert result.exit_code == 1
    assert "--targets is required" in result.output
