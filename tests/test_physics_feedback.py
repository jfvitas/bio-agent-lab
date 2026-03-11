import json
from pathlib import Path
from uuid import uuid4

import pandas as pd
from typer.testing import CliRunner

from pbdata.cli import app
from pbdata.pipeline.feature_execution import run_feature_pipeline, export_analysis_queue
from pbdata.pipeline.physics_feedback import (
    ingest_external_analysis_results,
    load_latest_site_physics_surrogate,
    train_site_physics_surrogate,
)
from pbdata.storage import build_storage_layout
from pbdata.table_io import read_dataframe, write_dataframe
from tests.test_feature_execution import _write_extracted_fixture

_LOCAL_TMP = Path(__file__).parent / "_tmp"
_LOCAL_TMP.mkdir(exist_ok=True)


def _tmp_dir(name: str) -> Path:
    path = _LOCAL_TMP / f"{uuid4().hex}_{name}"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _prepare_site_pipeline(layout, run_id: str = "physics_run") -> None:
    _write_extracted_fixture(layout)
    run_feature_pipeline(layout, run_id=run_id, degraded_mode=True)
    export_analysis_queue(layout, run_id=run_id)


def _write_parsed_results(layout, batch_id: str) -> None:
    for tool_name in ("orca", "apbs", "openmm"):
        parsed_dir = layout.external_analysis_artifacts_dir / tool_name / batch_id / "parsed"
        parsed_dir.mkdir(parents=True, exist_ok=True)
    (layout.external_analysis_artifacts_dir / "orca" / batch_id / "parsed" / "parsed_results.jsonl").write_text(
        "\n".join([
            json.dumps({
                "fragment_id": "frag1",
                "archetype_id": "asp_carboxylate_oxygen:abc123",
                "motif_class": "asp_carboxylate_oxygen",
                "atomic_charges": [-0.66, -0.12],
                "donor_strength": 0.1,
                "acceptor_strength": 0.8,
                "polarizability_proxy": 0.4,
                "protonation_preference_score": -0.7,
                "metal_binding_propensity": 0.5,
                "aromatic_interaction_propensity": 0.2,
                "status": "success",
            })
        ]),
        encoding="utf-8",
    )
    (layout.external_analysis_artifacts_dir / "apbs" / batch_id / "parsed" / "parsed_results.jsonl").write_text(
        json.dumps({
            "fragment_id": "frag1",
            "archetype_id": "asp_carboxylate_oxygen:abc123",
            "motif_class": "asp_carboxylate_oxygen",
            "site_potential": -1.3,
            "field_magnitude_proxy": 2.4,
            "desolvation_penalty_proxy": 0.6,
            "status": "success",
        }) + "\n",
        encoding="utf-8",
    )
    (layout.external_analysis_artifacts_dir / "openmm" / batch_id / "parsed" / "parsed_results.jsonl").write_text(
        json.dumps({
            "fragment_id": "frag1",
            "archetype_id": "asp_carboxylate_oxygen:abc123",
            "motif_class": "asp_carboxylate_oxygen",
            "effective_steric_radius": 1.1,
            "strain_proxy": 0.25,
            "status": "success",
        }) + "\n",
        encoding="utf-8",
    )


def test_ingest_external_analysis_results_writes_targets() -> None:
    tmp_root = _tmp_dir("physics_ingest")
    layout = build_storage_layout(tmp_root)
    _prepare_site_pipeline(layout)
    # replace queue archetype ids with deterministic test id
    archetypes = pd.DataFrame([{
        "run_id": "physics_run",
        "site_id": "1ABC|A|ASP|10|OD1|asp_carboxylate_oxygen",
        "motif_class": "asp_carboxylate_oxygen",
        "archetype_id": "asp_carboxylate_oxygen:abc123",
        "descriptor_hash": "abc123",
    }])
    write_dataframe(archetypes, layout.archetypes_artifacts_dir / "physics_run" / "archetypes.parquet")
    _write_parsed_results(layout, "batch1")

    result = ingest_external_analysis_results(layout, batch_id="batch1")

    assert Path(result["physics_targets"]).exists()
    df = read_dataframe(Path(result["physics_targets"]))
    assert len(df.index) == 1
    assert float(df.iloc[0]["electrostatic_potential"]) == -1.3
    assert "ORCA" in str(df.iloc[0]["source_analysis_methods"])


def test_train_site_physics_surrogate_and_load_latest() -> None:
    tmp_root = _tmp_dir("physics_surrogate")
    layout = build_storage_layout(tmp_root)
    _prepare_site_pipeline(layout)
    archetypes = pd.DataFrame([{
        "run_id": "physics_run",
        "site_id": "1ABC|A|ASP|10|OD1|asp_carboxylate_oxygen",
        "motif_class": "asp_carboxylate_oxygen",
        "archetype_id": "asp_carboxylate_oxygen:abc123",
        "descriptor_hash": "abc123",
    }])
    write_dataframe(archetypes, layout.archetypes_artifacts_dir / "physics_run" / "archetypes.parquet")
    _write_parsed_results(layout, "batch1")
    ingest_external_analysis_results(layout, batch_id="batch1")

    result = train_site_physics_surrogate(layout, batch_id="batch1", source_run_id="physics_run", surrogate_run_id="sur1")

    assert Path(result["checkpoint"]).exists()
    manifest = json.loads(Path(result["manifest"]).read_text(encoding="utf-8"))
    assert manifest["status"] == "trained"
    model = load_latest_site_physics_surrogate(layout)
    assert model is not None
    assert model["version"] == "site_physics_surrogate_v1"


def test_cli_ingest_and_train_site_physics_surrogate() -> None:
    tmp_root = _tmp_dir("physics_cli")
    layout = build_storage_layout(tmp_root)
    _prepare_site_pipeline(layout)
    archetypes = pd.DataFrame([{
        "run_id": "physics_run",
        "site_id": "1ABC|A|ASP|10|OD1|asp_carboxylate_oxygen",
        "motif_class": "asp_carboxylate_oxygen",
        "archetype_id": "asp_carboxylate_oxygen:abc123",
        "descriptor_hash": "abc123",
    }])
    write_dataframe(archetypes, layout.archetypes_artifacts_dir / "physics_run" / "archetypes.parquet")
    _write_parsed_results(layout, "batch1")
    runner = CliRunner()

    ingest_result = runner.invoke(
        app,
        ["--storage-root", str(tmp_root), "ingest-physics-results", "--batch-id", "batch1"],
        catch_exceptions=False,
    )
    train_result = runner.invoke(
        app,
        ["--storage-root", str(tmp_root), "train-site-physics-surrogate", "--batch-id", "batch1", "--source-run-id", "physics_run", "--surrogate-run-id", "sur1"],
        catch_exceptions=False,
    )

    assert ingest_result.exit_code == 0
    assert train_result.exit_code == 0
    assert (layout.surrogate_training_artifacts_dir / "sur1" / "surrogate_manifest.json").exists()
