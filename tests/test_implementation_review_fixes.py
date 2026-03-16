import json
from pathlib import Path

from typer.testing import CliRunner

from pbdata.cli import app
from pbdata.graph.structural_graphs import build_structural_graphs
from pbdata.training.assembler import assemble_training_examples
from pbdata.storage import build_storage_layout
from tests.test_feature_execution import _tmp_dir, _write_extracted_fixture
from tests.test_training_assembler import _setup_test_data


def test_structural_graph_shell_scope_is_spatially_narrower_than_whole_protein() -> None:
    tmp_root = _tmp_dir("shell_scope_graphs")
    layout = build_storage_layout(tmp_root)
    _write_extracted_fixture(layout)
    (layout.extracted_dir / "interfaces" / "1ABC.json").write_text(
        json.dumps([
            {
                "pdb_id": "1ABC",
                "interface_type": "protein_ligand",
                "binding_site_residue_ids": ["A:ASP10"],
                "binding_site_chain_ids": ["A"],
            }
        ]),
        encoding="utf-8",
    )

    whole = build_structural_graphs(layout, graph_level="residue", scope="whole_protein", export_formats=("networkx",))
    shell = build_structural_graphs(layout, graph_level="residue", scope="shell", shell_radius=1.0, export_formats=("networkx",))
    interface_only = build_structural_graphs(layout, graph_level="residue", scope="interface_only", export_formats=("networkx",))

    whole_summary = json.loads(Path(next(value for key, value in whole.items() if key.endswith("_summary"))).read_text(encoding="utf-8"))
    shell_summary = json.loads(Path(next(value for key, value in shell.items() if key.endswith("_summary"))).read_text(encoding="utf-8"))
    interface_summary = json.loads(Path(next(value for key, value in interface_only.items() if key.endswith("_summary"))).read_text(encoding="utf-8"))

    assert shell_summary["scope"] == "shell"
    assert shell_summary["node_count"] < whole_summary["node_count"]
    assert interface_summary["node_count"] <= shell_summary["node_count"]


def test_training_examples_include_field_provenance() -> None:
    tmp = _tmp_dir("training_field_provenance")
    extracted, features, graph, output = _setup_test_data(
        tmp,
        entries=[{"pdb_id": "1ABC", "structure_resolution": 2.0, "assembly_id": "1"}],
        chains=[{
            "pdb_id": "1ABC",
            "chain_id": "A",
            "is_protein": True,
            "uniprot_id": "P12345",
            "entity_source_organism": "Homo sapiens",
            "chain_description": "Kinase",
        }],
        bound_objects=[{"pdb_id": "1ABC", "component_id": "ATP", "component_smiles": "CCO"}],
        interfaces=[{"pdb_id": "1ABC", "interface_type": "protein_ligand", "binding_site_residue_ids": ["A:TYR15"]}],
        assays=[{
            "pdb_id": "1ABC",
            "source_database": "PDBbind",
            "pair_identity_key": "protein_ligand|1ABC|A|ATP|wt",
            "binding_affinity_type": "Kd",
            "binding_affinity_value": 5.0,
            "field_provenance": {"binding_affinity_value": {"source": "PDBbind"}},
            "field_confidence": {"binding_affinity_value": "high"},
        }],
        features=[{
            "feature_id": "f1",
            "pdb_id": "1ABC",
            "pair_identity_key": "protein_ligand|1ABC|A|ATP|wt",
            "feature_group": "training_ready_core",
            "values": {"network_degree": 3},
            "provenance": {},
        }],
        graph_nodes=[],
        graph_edges=[],
    )

    examples_path, _ = assemble_training_examples(extracted, features, graph, output)
    rows = json.loads(examples_path.read_text(encoding="utf-8"))

    assert rows[0]["field_provenance"]["experiment"]["source_tables"] == ["assays"]
    assert rows[0]["field_provenance"]["experiment"]["field_confidence"]["binding_affinity_value"] == "high"


def test_cli_reports_optional_dependency_errors_cleanly(monkeypatch) -> None:
    runner = CliRunner()
    storage_root = _tmp_dir("cli_dependency_error")
    layout = build_storage_layout(storage_root)
    _write_extracted_fixture(layout)

    import pbdata.graph.structural_graphs as structural_graphs

    def _boom(*args, **kwargs):
        raise ModuleNotFoundError("No module named 'torch'")

    monkeypatch.setattr(structural_graphs, "build_structural_graphs", _boom)
    result = runner.invoke(
        app,
        ["--storage-root", str(storage_root), "build-structural-graphs", "--graph-level", "residue"],
        catch_exceptions=False,
    )

    assert result.exit_code == 1
    assert "requires the optional 'torch' dependency" in result.output


def test_structural_graph_preview_selection_uses_training_csv_limit() -> None:
    tmp_root = _tmp_dir("graph_preview_selection")
    layout = build_storage_layout(tmp_root)
    _write_extracted_fixture(layout)
    (layout.root / "custom_training_set.csv").write_text(
        "pdb_id\n1ABC\n2XYZ\n",
        encoding="utf-8",
    )

    artifacts = build_structural_graphs(
        layout,
        graph_level="residue",
        scope="whole_protein",
        selection="preview",
        limit=1,
        export_formats=("networkx",),
    )

    manifest = json.loads(Path(artifacts["manifest"]).read_text(encoding="utf-8"))
    assert manifest["selected_count"] == 1
    assert manifest["processed_count"] == 1
    assert manifest["selection"].startswith("csv:")
    assert len(manifest["graphs"]) == 1


def test_structural_graph_only_missing_reuses_cached_outputs() -> None:
    tmp_root = _tmp_dir("graph_cache_reuse")
    layout = build_storage_layout(tmp_root)
    _write_extracted_fixture(layout)

    first = build_structural_graphs(
        layout,
        graph_level="residue",
        scope="whole_protein",
        pdb_ids=["1ABC"],
        export_formats=("networkx",),
        only_missing=False,
    )
    second = build_structural_graphs(
        layout,
        graph_level="residue",
        scope="whole_protein",
        pdb_ids=["1ABC"],
        export_formats=("networkx",),
        only_missing=True,
    )

    assert first["processed_count"] == "1"
    assert second["processed_count"] == "0"
    assert second["skipped_count"] == "1"

    manifest = json.loads(Path(second["manifest"]).read_text(encoding="utf-8"))
    assert manifest["skipped_count"] == 1
    assert manifest["graphs"][0]["cached"] is True


def test_package_main_module_exists() -> None:
    from pbdata.__main__ import main

    assert callable(main)
