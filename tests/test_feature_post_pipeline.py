import json
from pathlib import Path
from uuid import uuid4

import yaml
from tests.test_feature_execution import _write_extracted_fixture
from pbdata.table_io import read_dataframe

from pbdata.pipeline.feature_execution import export_analysis_queue, run_feature_pipeline
from pbdata.pipeline.feature_post_pipeline import (
    build_analysis_queue_batches,
    build_archetype_rows,
    build_cluster_summary_rows,
    build_representative_archetype_rows,
)
from pbdata.storage import build_storage_layout
from pbdata.table_io import write_dataframe

_LOCAL_TMP = Path(__file__).parent / "_tmp"
_LOCAL_TMP.mkdir(exist_ok=True)


def _tmp_dir(name: str) -> Path:
    path = _LOCAL_TMP / f"{uuid4().hex}_{name}"
    path.mkdir(parents=True, exist_ok=True)
    return path


def test_build_archetype_rows_deduplicates_at_queue_stage() -> None:
    import pandas as pd

    tmp_root = _tmp_dir("feature_post_pipeline")
    layout = build_storage_layout(tmp_root)
    env_dir = layout.base_features_artifacts_dir / "run1"
    env_dir.mkdir(parents=True, exist_ok=True)
    write_dataframe(
        pd.DataFrame(
            [
                {
                    "site_id": "site_a",
                    "motif_class": "asp_carboxylate_oxygen",
                    "shell_name": "shell_1",
                    "neighbor_atom_count": 2,
                    "sum_partial_charge": -0.5,
                    "electric_field_magnitude": 0.5,
                    "donor_count": 0,
                    "acceptor_count": 2,
                },
                {
                    "site_id": "site_b",
                    "motif_class": "asp_carboxylate_oxygen",
                    "shell_name": "shell_1",
                    "neighbor_atom_count": 2,
                    "sum_partial_charge": -0.5,
                    "electric_field_magnitude": 0.5,
                    "donor_count": 0,
                    "acceptor_count": 2,
                },
            ]
        ),
        env_dir / "1ABC.env_vectors.parquet",
    )

    archetype_rows = build_archetype_rows(layout, run_id="run1")
    queue_rows = build_analysis_queue_batches(pd.DataFrame(archetype_rows))

    assert len(archetype_rows) == 2
    assert len({row["cluster_id"] for row in archetype_rows}) == 1
    assert sum(bool(row["is_cluster_representative"]) for row in archetype_rows) == 1
    assert len(queue_rows) == 1
    assert len(queue_rows[0]["archetype_ids"]) == 1
    assert len(queue_rows[0]["cluster_ids"]) == 1
    cluster_summary = build_cluster_summary_rows(pd.DataFrame(archetype_rows))
    representatives = build_representative_archetype_rows(pd.DataFrame(archetype_rows))
    assert len(cluster_summary) == 1
    assert len(representatives) == 1


def test_build_archetype_rows_separates_distinct_clusters() -> None:
    import pandas as pd

    tmp_root = _tmp_dir("feature_post_pipeline_clusters")
    layout = build_storage_layout(tmp_root)
    env_dir = layout.base_features_artifacts_dir / "run2"
    env_dir.mkdir(parents=True, exist_ok=True)
    write_dataframe(
        pd.DataFrame(
            [
                {
                    "site_id": "site_a",
                    "motif_class": "asp_carboxylate_oxygen",
                    "shell_name": "shell_1",
                    "neighbor_atom_count": 2,
                    "sum_partial_charge": -0.5,
                    "electric_field_magnitude": 0.5,
                    "donor_count": 0,
                    "acceptor_count": 2,
                },
                {
                    "site_id": "site_b",
                    "motif_class": "asp_carboxylate_oxygen",
                    "shell_name": "shell_1",
                    "neighbor_atom_count": 11,
                    "sum_partial_charge": 3.0,
                    "electric_field_magnitude": 7.5,
                    "donor_count": 4,
                    "acceptor_count": 0,
                },
            ]
        ),
        env_dir / "1ABC.env_vectors.parquet",
    )

    archetype_rows = build_archetype_rows(layout, run_id="run2")
    queue_rows = build_analysis_queue_batches(pd.DataFrame(archetype_rows))

    assert len({row["cluster_id"] for row in archetype_rows}) == 2
    assert len(queue_rows) == 1
    assert len(queue_rows[0]["archetype_ids"]) == 2


def test_export_analysis_queue_writes_empty_batches_when_no_env_vectors_exist() -> None:
    tmp_root = _tmp_dir("feature_post_pipeline_empty")
    layout = build_storage_layout(tmp_root)

    result = export_analysis_queue(layout, run_id="empty_run")

    assert Path(result["archetypes"]).exists()
    assert Path(result["representatives"]).exists()
    assert Path(result["cluster_summary"]).exists()
    queue_payload = yaml.safe_load(Path(result["queue"]).read_text(encoding="utf-8"))
    assert queue_payload == {"run_id": "empty_run", "batches": []}
    manifest = json.loads(Path(result["batch_manifest"]).read_text(encoding="utf-8"))
    assert manifest["motif_class_count"] == 0
    assert manifest["archetype_count"] == 0
    assert manifest["representative_archetype_count"] == 0
    assert manifest["cluster_count"] == 0


def test_export_analysis_queue_writes_cluster_level_artifacts() -> None:
    tmp_root = _tmp_dir("feature_post_pipeline_export")
    layout = build_storage_layout(tmp_root)
    _write_extracted_fixture(layout)
    run_feature_pipeline(layout, run_id="run3", degraded_mode=True)

    result = export_analysis_queue(layout, run_id="run3")

    representatives = read_dataframe(Path(result["representatives"]))
    cluster_summary = read_dataframe(Path(result["cluster_summary"]))
    fragments = read_dataframe(Path(result["fragments"]))
    queue_payload = yaml.safe_load(Path(result["queue"]).read_text(encoding="utf-8"))
    manifest = json.loads(Path(result["batch_manifest"]).read_text(encoding="utf-8"))

    assert len(representatives.index) >= 1
    assert len(cluster_summary.index) >= 1
    assert len(fragments.index) >= 1
    assert Path(str(fragments.iloc[0]["fragment_file"])).exists()
    assert Path(str(fragments.iloc[0]["metadata_file"])).exists()
    first_metadata = json.loads(Path(str(fragments.iloc[0]["metadata_file"])).read_text(encoding="utf-8"))
    assert first_metadata["fragment_atom_count"] >= 1
    assert queue_payload["batches"][0]["fragments"]
    assert manifest["representative_archetype_count"] >= 1
    assert manifest["cluster_count"] >= 1
    assert manifest["fragment_count"] >= 1
