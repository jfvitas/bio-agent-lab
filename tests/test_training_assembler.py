"""Tests for the training example assembler.

Validates that examples are correctly assembled from extracted, graph,
and feature layers, with proper handling of missing data, deduplication,
label assignment, and edge cases.
"""

import json
from pathlib import Path
from uuid import uuid4

import pytest

from pbdata.training.assembler import assemble_training_examples, _safe_float
from pbdata.schemas.training_example import TrainingExampleRecord

_LOCAL_TMP = Path(__file__).parent / "_tmp"
_LOCAL_TMP.mkdir(exist_ok=True)


def _tmp_dir(name: str) -> Path:
    path = _LOCAL_TMP / f"{uuid4().hex}_{name}"
    path.mkdir(exist_ok=True)
    return path


def _setup_test_data(
    tmp_root: Path,
    *,
    entries: list | None = None,
    chains: list | None = None,
    bound_objects: list | None = None,
    interfaces: list | None = None,
    assays: list | None = None,
    features: list | None = None,
    graph_nodes: list | None = None,
    graph_edges: list | None = None,
) -> tuple[Path, Path, Path, Path]:
    """Create test data directories and return (extracted, features, graph, output) dirs."""
    extracted = tmp_root / "extracted"
    features_dir = tmp_root / "features"
    graph_dir = tmp_root / "graph"
    output_dir = tmp_root / "output"

    for name in ["entry", "chains", "bound_objects", "interfaces", "assays"]:
        (extracted / name).mkdir(parents=True, exist_ok=True)
    features_dir.mkdir(parents=True, exist_ok=True)
    graph_dir.mkdir(parents=True, exist_ok=True)

    if entries is not None:
        (extracted / "entry" / "data.json").write_text(
            json.dumps(entries), encoding="utf-8",
        )
    if chains is not None:
        (extracted / "chains" / "data.json").write_text(
            json.dumps(chains), encoding="utf-8",
        )
    if bound_objects is not None:
        (extracted / "bound_objects" / "data.json").write_text(
            json.dumps(bound_objects), encoding="utf-8",
        )
    if interfaces is not None:
        (extracted / "interfaces" / "data.json").write_text(
            json.dumps(interfaces), encoding="utf-8",
        )
    if assays is not None:
        (extracted / "assays" / "data.json").write_text(
            json.dumps(assays), encoding="utf-8",
        )
    if features is not None:
        (features_dir / "feature_records.json").write_text(
            json.dumps(features), encoding="utf-8",
        )
    if graph_nodes is not None:
        (graph_dir / "graph_nodes.json").write_text(
            json.dumps(graph_nodes), encoding="utf-8",
        )
    if graph_edges is not None:
        (graph_dir / "graph_edges.json").write_text(
            json.dumps(graph_edges), encoding="utf-8",
        )

    return extracted, features_dir, graph_dir, output_dir


# ---------------------------------------------------------------------------
# Basic assembly
# ---------------------------------------------------------------------------


def test_assemble_basic_example() -> None:
    """Assemble one training example from complete upstream data."""
    tmp = _tmp_dir("assemble_basic")
    extracted, features, graph, output = _setup_test_data(
        tmp,
        entries=[{
            "pdb_id": "1ABC",
            "structure_resolution": 2.0,
            "assembly_id": "1",
        }],
        chains=[{
            "pdb_id": "1ABC", "chain_id": "A", "is_protein": True,
            "uniprot_id": "P12345", "entity_source_organism": "Homo sapiens",
            "chain_description": "Kinase",
        }],
        bound_objects=[{
            "pdb_id": "1ABC", "component_id": "ATP",
            "component_smiles": "c1nc(ncc1)N", "component_molecular_weight": 507.0,
        }],
        interfaces=[{
            "pdb_id": "1ABC", "interface_type": "protein_ligand",
            "binding_site_residue_ids": ["TYR15", "ASP34"],
        }],
        assays=[{
            "pdb_id": "1ABC",
            "source_database": "PDBbind",
            "pair_identity_key": "protein_ligand|1ABC|A|ATP|wt",
            "binding_affinity_type": "Kd",
            "binding_affinity_value": 5.0,
            "binding_affinity_log10_standardized": 0.699,
            "assay_temperature_c": 25.0,
            "assay_ph": 7.4,
        }],
        features=[{
            "feature_id": "f1",
            "pdb_id": "1ABC",
            "pair_identity_key": "protein_ligand|1ABC|A|ATP|wt",
            "feature_group": "training_ready_core",
            "values": {"network_degree": 5, "pathway_count": 2},
            "provenance": {},
        }],
        graph_nodes=[
            {"node_id": "protein:P12345", "node_type": "Protein", "primary_id": "P12345",
             "metadata": {"pdb_id": "1ABC"}},
        ],
        graph_edges=[
            {"edge_id": "e1", "edge_type": "ProteinLigandInteraction",
             "source_node_id": "protein:P12345", "target_node_id": "ligand:ATP",
             "source_database": "RCSB"},
        ],
    )

    examples_path, manifest_path = assemble_training_examples(
        extracted, features, graph, output,
    )

    examples = json.loads(examples_path.read_text(encoding="utf-8"))
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    assert len(examples) == 1
    ex = examples[0]

    # Structure
    assert ex["structure"]["pdb_id"] == "1ABC"
    assert ex["structure"]["resolution"] == 2.0
    assert ex["structure"]["chain_ids"] == ["A"]

    # Protein
    assert ex["protein"]["uniprot_id"] == "P12345"
    assert ex["protein"]["organism"] == "Homo sapiens"

    # Ligand
    assert ex["ligand"]["ligand_id"] == "ATP"
    assert ex["ligand"]["smiles"] == "c1nc(ncc1)N"
    assert ex["ligand"]["molecular_weight"] == 507.0

    # Interaction
    assert ex["interaction"]["interface_residues"] == ["TYR15", "ASP34"]

    # Experiment
    assert ex["experiment"]["affinity_type"] == "Kd"
    assert ex["experiment"]["affinity_value"] == 5.0
    assert ex["experiment"]["temperature"] == 25.0
    assert ex["experiment"]["ph"] == 7.4
    assert ex["experiment"]["source_database"] == "PDBbind"

    # Labels
    assert ex["labels"]["binding_affinity_log10"] == 0.699
    assert ex["labels"]["binding_affinity_raw"] == 5.0
    assert ex["labels"]["affinity_type"] == "Kd"

    # Provenance
    assert ex["provenance"]["pdb_id"] == "1ABC"
    assert ex["provenance"]["source_database"] == "PDBbind"

    # Manifest
    assert manifest["status"] == "assembled"
    assert manifest["example_count"] == 1
    assert "PDBbind" in manifest["sources_used"]

    # Validate as Pydantic model
    record = TrainingExampleRecord.model_validate(ex)
    assert record.example_id.startswith("train:")


# ---------------------------------------------------------------------------
# Deduplication
# ---------------------------------------------------------------------------


def test_assemble_deduplicates_by_pair_and_type() -> None:
    """Duplicate (pair_key, affinity_type) should produce one example."""
    tmp = _tmp_dir("assemble_dedup")
    extracted, features, graph, output = _setup_test_data(
        tmp,
        entries=[{"pdb_id": "1ABC"}],
        chains=[],
        bound_objects=[],
        interfaces=[],
        assays=[
            {
                "pdb_id": "1ABC",
                "pair_identity_key": "protein_ligand|1ABC|A|ATP|wt",
                "binding_affinity_type": "Kd",
                "binding_affinity_value": 5.0,
                "source_database": "PDBbind",
            },
            {
                "pdb_id": "1ABC",
                "pair_identity_key": "protein_ligand|1ABC|A|ATP|wt",
                "binding_affinity_type": "Kd",
                "binding_affinity_value": 6.0,
                "source_database": "BindingDB",
            },
        ],
        features=[],
        graph_nodes=[],
        graph_edges=[],
    )

    examples_path, _ = assemble_training_examples(extracted, features, graph, output)
    examples = json.loads(examples_path.read_text(encoding="utf-8"))
    assert len(examples) == 1


def test_assemble_different_affinity_types_not_deduped() -> None:
    """Same pair with different affinity types should produce separate examples."""
    tmp = _tmp_dir("assemble_diff_type")
    extracted, features, graph, output = _setup_test_data(
        tmp,
        entries=[{"pdb_id": "1ABC"}],
        chains=[],
        bound_objects=[],
        interfaces=[],
        assays=[
            {
                "pdb_id": "1ABC",
                "pair_identity_key": "protein_ligand|1ABC|A|ATP|wt",
                "binding_affinity_type": "Kd",
                "binding_affinity_value": 5.0,
                "source_database": "PDBbind",
            },
            {
                "pdb_id": "1ABC",
                "pair_identity_key": "protein_ligand|1ABC|A|ATP|wt",
                "binding_affinity_type": "Ki",
                "binding_affinity_value": 3.0,
                "source_database": "ChEMBL",
            },
        ],
        features=[],
        graph_nodes=[],
        graph_edges=[],
    )

    examples_path, _ = assemble_training_examples(extracted, features, graph, output)
    examples = json.loads(examples_path.read_text(encoding="utf-8"))
    assert len(examples) == 2


# ---------------------------------------------------------------------------
# Missing / empty data handling
# ---------------------------------------------------------------------------


def test_assemble_with_no_assays_produces_no_examples() -> None:
    tmp = _tmp_dir("assemble_no_assays")
    extracted, features, graph, output = _setup_test_data(
        tmp,
        entries=[{"pdb_id": "1ABC"}],
        chains=[],
        bound_objects=[],
        interfaces=[],
        assays=[],
        features=[],
        graph_nodes=[],
        graph_edges=[],
    )

    examples_path, manifest_path = assemble_training_examples(extracted, features, graph, output)
    examples = json.loads(examples_path.read_text(encoding="utf-8"))
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    assert len(examples) == 0
    assert manifest["example_count"] == 0


def test_assemble_with_missing_upstream_dirs() -> None:
    """Assembler should handle missing directories gracefully."""
    tmp = _tmp_dir("assemble_missing_dirs")
    extracted = tmp / "nonexistent_extracted"
    features = tmp / "nonexistent_features"
    graph = tmp / "nonexistent_graph"
    output = tmp / "output"

    examples_path, manifest_path = assemble_training_examples(
        extracted, features, graph, output,
    )
    examples = json.loads(examples_path.read_text(encoding="utf-8"))
    assert len(examples) == 0


def test_assemble_with_partial_data() -> None:
    """Assay without matching entry/chain should still produce example with Nones."""
    tmp = _tmp_dir("assemble_partial")
    extracted, features, graph, output = _setup_test_data(
        tmp,
        entries=[],  # No entry for 1ABC
        chains=[],
        bound_objects=[],
        interfaces=[],
        assays=[{
            "pdb_id": "1ABC",
            "pair_identity_key": "protein_ligand|1ABC|A|ATP|wt",
            "binding_affinity_type": "Kd",
            "binding_affinity_value": 5.0,
            "source_database": "PDBbind",
        }],
        features=[],
        graph_nodes=[],
        graph_edges=[],
    )

    examples_path, _ = assemble_training_examples(extracted, features, graph, output)
    examples = json.loads(examples_path.read_text(encoding="utf-8"))

    assert len(examples) == 1
    ex = examples[0]
    assert ex["structure"]["resolution"] is None  # No entry data
    assert ex["protein"]["uniprot_id"] is None  # No chain data
    assert ex["ligand"]["ligand_id"] is None  # No bound objects
    assert ex["experiment"]["affinity_value"] == 5.0  # From assay


def test_assemble_skips_assays_without_pdb_id() -> None:
    tmp = _tmp_dir("assemble_no_pdb")
    extracted, features, graph, output = _setup_test_data(
        tmp,
        entries=[],
        chains=[],
        bound_objects=[],
        interfaces=[],
        assays=[
            {"pair_identity_key": "test|key", "binding_affinity_type": "Kd"},  # no pdb_id
            {"pdb_id": "", "pair_identity_key": "", "binding_affinity_type": "Kd"},  # empty
        ],
        features=[],
        graph_nodes=[],
        graph_edges=[],
    )

    examples_path, _ = assemble_training_examples(extracted, features, graph, output)
    examples = json.loads(examples_path.read_text(encoding="utf-8"))
    assert len(examples) == 0


# ---------------------------------------------------------------------------
# Graph feature integration
# ---------------------------------------------------------------------------


def test_assemble_includes_pathway_counts() -> None:
    """Examples should include pathway counts from graph edges."""
    tmp = _tmp_dir("assemble_pathways")
    extracted, features, graph, output = _setup_test_data(
        tmp,
        entries=[{"pdb_id": "1ABC"}],
        chains=[{
            "pdb_id": "1ABC", "chain_id": "A", "is_protein": True,
            "uniprot_id": "P12345",
        }],
        bound_objects=[],
        interfaces=[],
        assays=[{
            "pdb_id": "1ABC",
            "pair_identity_key": "protein_ligand|1ABC|A|ATP|wt",
            "binding_affinity_type": "Kd",
            "binding_affinity_value": 5.0,
            "source_database": "test",
        }],
        features=[{
            "feature_id": "f1",
            "pdb_id": "1ABC",
            "pair_identity_key": "protein_ligand|1ABC|A|ATP|wt",
            "feature_group": "training_ready_core",
            "values": {"network_degree": 3, "pathway_count": 5},
            "provenance": {},
        }],
        graph_nodes=[
            {"node_id": "protein:P12345", "node_type": "Protein",
             "primary_id": "P12345", "metadata": {"pdb_id": "1ABC"}},
            {"node_id": "pathway:R-HSA-1", "node_type": "Pathway",
             "primary_id": "R-HSA-1"},
            {"node_id": "pathway:R-HSA-2", "node_type": "Pathway",
             "primary_id": "R-HSA-2"},
        ],
        graph_edges=[
            {"edge_id": "e1", "edge_type": "ProteinPathway",
             "source_node_id": "protein:P12345", "target_node_id": "pathway:R-HSA-1",
             "source_database": "Reactome"},
            {"edge_id": "e2", "edge_type": "ProteinPathway",
             "source_node_id": "protein:P12345", "target_node_id": "pathway:R-HSA-2",
             "source_database": "Reactome"},
            {"edge_id": "e3", "edge_type": "ProteinLigandInteraction",
             "source_node_id": "protein:P12345", "target_node_id": "ligand:ATP",
             "source_database": "RCSB"},
        ],
    )

    examples_path, _ = assemble_training_examples(extracted, features, graph, output)
    examples = json.loads(examples_path.read_text(encoding="utf-8"))

    assert len(examples) == 1
    ex = examples[0]
    assert ex["graph_features"]["pathway_count"] == 2  # From graph edges
    assert ex["graph_features"]["network_degree"] == 3  # degree in graph


# ---------------------------------------------------------------------------
# Label fields
# ---------------------------------------------------------------------------


def test_assemble_labels_include_mutant_and_ddg() -> None:
    tmp = _tmp_dir("assemble_labels")
    extracted, features, graph, output = _setup_test_data(
        tmp,
        entries=[{"pdb_id": "1ABC"}],
        chains=[],
        bound_objects=[],
        interfaces=[],
        assays=[{
            "pdb_id": "1ABC",
            "pair_identity_key": "mutation_ddg|1ABC|A,B|mutant",
            "binding_affinity_type": "ddG",
            "binding_affinity_value": -1.5,
            "binding_affinity_log10_standardized": None,
            "binding_affinity_is_mutant_measurement": True,
            "delta_delta_g": -1.5,
            "source_database": "SKEMPI",
        }],
        features=[],
        graph_nodes=[],
        graph_edges=[],
    )

    examples_path, _ = assemble_training_examples(extracted, features, graph, output)
    examples = json.loads(examples_path.read_text(encoding="utf-8"))

    assert len(examples) == 1
    labels = examples[0]["labels"]
    assert labels["is_mutant"] is True
    assert labels["delta_delta_g"] == -1.5
    assert labels["binding_affinity_raw"] == -1.5


# ---------------------------------------------------------------------------
# Multi-PDB assembly
# ---------------------------------------------------------------------------


def test_assemble_multiple_pdbs() -> None:
    tmp = _tmp_dir("assemble_multi_pdb")
    extracted, features, graph, output = _setup_test_data(
        tmp,
        entries=[
            {"pdb_id": "1ABC", "structure_resolution": 2.0},
            {"pdb_id": "2DEF", "structure_resolution": 1.5},
        ],
        chains=[
            {"pdb_id": "1ABC", "chain_id": "A", "is_protein": True, "uniprot_id": "P12345"},
            {"pdb_id": "2DEF", "chain_id": "A", "is_protein": True, "uniprot_id": "Q99999"},
        ],
        bound_objects=[],
        interfaces=[],
        assays=[
            {
                "pdb_id": "1ABC",
                "pair_identity_key": "protein_ligand|1ABC|A|ATP|wt",
                "binding_affinity_type": "Kd",
                "binding_affinity_value": 5.0,
                "source_database": "PDBbind",
            },
            {
                "pdb_id": "2DEF",
                "pair_identity_key": "protein_ligand|2DEF|A|GTP|wt",
                "binding_affinity_type": "Ki",
                "binding_affinity_value": 10.0,
                "source_database": "ChEMBL",
            },
        ],
        features=[],
        graph_nodes=[],
        graph_edges=[],
    )

    examples_path, manifest_path = assemble_training_examples(
        extracted, features, graph, output,
    )
    examples = json.loads(examples_path.read_text(encoding="utf-8"))
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    assert len(examples) == 2
    pdb_ids = {ex["structure"]["pdb_id"] for ex in examples}
    assert pdb_ids == {"1ABC", "2DEF"}
    assert manifest["example_count"] == 2
    assert set(manifest["sources_used"]) == {"PDBbind", "ChEMBL"}


# ---------------------------------------------------------------------------
# _safe_float edge cases
# ---------------------------------------------------------------------------


def test_safe_float_valid() -> None:
    assert _safe_float(3.14) == 3.14
    assert _safe_float("2.5") == 2.5
    assert _safe_float(0) == 0.0
    assert _safe_float("0") == 0.0


def test_safe_float_none() -> None:
    assert _safe_float(None) is None


def test_safe_float_invalid() -> None:
    assert _safe_float("not a number") is None
    assert _safe_float("") is None
    assert _safe_float([]) is None


# ---------------------------------------------------------------------------
# CLI integration
# ---------------------------------------------------------------------------


def test_cli_build_training_examples_assembles_when_data_present() -> None:
    import os
    from typer.testing import CliRunner
    from pbdata.cli import app

    tmp_root = _tmp_dir("training_cli")
    extracted = tmp_root / "data" / "extracted"
    for name in ["entry", "chains", "bound_objects", "interfaces", "assays"]:
        (extracted / name).mkdir(parents=True, exist_ok=True)

    (extracted / "assays" / "1ABC.json").write_text(json.dumps([{
        "pdb_id": "1ABC",
        "pair_identity_key": "test|key",
        "binding_affinity_type": "Kd",
        "binding_affinity_value": 5.0,
        "source_database": "test",
    }]), encoding="utf-8")

    features_dir = tmp_root / "data" / "features"
    features_dir.mkdir(parents=True, exist_ok=True)
    (features_dir / "feature_records.json").write_text("[]", encoding="utf-8")

    graph_dir = tmp_root / "data" / "graph"
    graph_dir.mkdir(parents=True, exist_ok=True)
    (graph_dir / "graph_nodes.json").write_text("[]", encoding="utf-8")
    (graph_dir / "graph_edges.json").write_text("[]", encoding="utf-8")

    runner = CliRunner()
    original_cwd = Path.cwd()
    os.chdir(tmp_root)
    try:
        result = runner.invoke(app, ["build-training-examples"], catch_exceptions=False)
    finally:
        os.chdir(original_cwd)

    assert result.exit_code == 0
    assert "Training examples written" in result.output

    manifest = json.loads(
        (tmp_root / "data" / "training_examples" / "training_manifest.json")
        .read_text(encoding="utf-8")
    )
    assert manifest["status"] == "assembled"


# ---------------------------------------------------------------------------
# Zero-value edge cases (bug fix regression)
# ---------------------------------------------------------------------------


def test_assemble_zero_degree_not_treated_as_missing() -> None:
    """network_degree=0 should stay 0, not fall through to feature values."""
    tmp = _tmp_dir("assemble_zero_degree")
    extracted, features, graph, output = _setup_test_data(
        tmp,
        entries=[{"pdb_id": "1ABC"}],
        chains=[{"pdb_id": "1ABC", "chain_id": "A", "is_protein": True, "uniprot_id": "P12345"}],
        bound_objects=[],
        interfaces=[],
        assays=[{
            "pdb_id": "1ABC",
            "pair_identity_key": "protein_ligand|1ABC|A|ATP|wt",
            "binding_affinity_type": "Kd",
            "binding_affinity_value": 5.0,
        }],
        # Feature record with non-zero network_degree — should NOT be used
        features=[{
            "pair_identity_key": "protein_ligand|1ABC|A|ATP|wt",
            "values": {"network_degree": 99, "pathway_count": 77},
            "provenance": {},
        }],
        graph_nodes=[
            {"node_id": "protein:P12345", "node_type": "Protein",
             "primary_id": "P12345", "metadata": {"pdb_id": "1ABC"}},
        ],
        # No edges — so degree should be 0, not 99 from features
        graph_edges=[],
    )

    examples_path, _ = assemble_training_examples(extracted, features, graph, output)
    examples = json.loads(examples_path.read_text(encoding="utf-8"))

    assert len(examples) == 1
    gf = examples[0]["graph_features"]
    # Node exists in graph but has zero edges — degree must be 0, not 99
    assert gf["network_degree"] == 0
    assert gf["pathway_count"] == 0


def test_assemble_picks_chain_a_as_primary_deterministically() -> None:
    """With multiple protein chains, assembler should pick chain A (sorted first)."""
    tmp = _tmp_dir("assemble_chain_order")
    extracted, features, graph, output = _setup_test_data(
        tmp,
        entries=[{"pdb_id": "1ABC"}],
        # Chains listed in reverse order — B before A
        chains=[
            {"pdb_id": "1ABC", "chain_id": "B", "is_protein": True, "uniprot_id": "Q99999"},
            {"pdb_id": "1ABC", "chain_id": "A", "is_protein": True, "uniprot_id": "P12345"},
        ],
        bound_objects=[],
        interfaces=[],
        assays=[{
            "pdb_id": "1ABC",
            "pair_identity_key": "protein_ligand|1ABC|A|ATP|wt",
            "binding_affinity_type": "Kd",
            "binding_affinity_value": 5.0,
        }],
        features=[],
        graph_nodes=[],
        graph_edges=[],
    )

    examples_path, _ = assemble_training_examples(extracted, features, graph, output)
    examples = json.loads(examples_path.read_text(encoding="utf-8"))

    assert len(examples) == 1
    # Primary protein should be chain A (P12345), not chain B (Q99999)
    assert examples[0]["protein"]["uniprot_id"] == "P12345"
    # pair-specific structure chains should follow the pair key
    assert examples[0]["structure"]["chain_ids"] == ["A"]


def test_assemble_uses_pair_specific_chain_ligand_and_interface() -> None:
    tmp = _tmp_dir("assemble_pair_specific")
    extracted, features, graph, output = _setup_test_data(
        tmp,
        entries=[{"pdb_id": "1ABC"}],
        chains=[
            {"pdb_id": "1ABC", "chain_id": "A", "is_protein": True, "uniprot_id": "P11111"},
            {"pdb_id": "1ABC", "chain_id": "B", "is_protein": True, "uniprot_id": "P22222"},
        ],
        bound_objects=[
            {"pdb_id": "1ABC", "component_id": "ATP", "component_smiles": "ATP-SMILES"},
            {
                "pdb_id": "1ABC", "component_id": "GTP", "component_smiles": "GTP-SMILES",
                "component_type": "small_molecule", "component_inchikey": "GTP-KEY",
            },
        ],
        interfaces=[
            {
                "pdb_id": "1ABC",
                "interface_type": "protein_ligand",
                "binding_site_chain_ids": ["A"],
                "binding_site_residue_ids": ["A:TYR15"],
                "entity_name_b": "ATP",
            },
            {
                "pdb_id": "1ABC",
                "interface_type": "protein_ligand",
                "binding_site_chain_ids": ["B"],
                "binding_site_residue_ids": ["B:ASP40"],
                "entity_name_b": "GTP",
            },
        ],
        assays=[{
            "pdb_id": "1ABC",
            "source_database": "ChEMBL",
            "pair_identity_key": "protein_ligand|1ABC|B|GTP|wt",
            "binding_affinity_type": "Kd",
            "binding_affinity_value": 5.0,
            "selected_preferred_source": "PDBbind",
            "reported_measurement_count": 3,
            "source_conflict_flag": True,
            "source_agreement_band": "low",
            "measurement_source_doi": "10.1000/example",
        }],
        features=[{
            "feature_id": "f1",
            "pdb_id": "1ABC",
            "pair_identity_key": "protein_ligand|1ABC|B|GTP|wt",
            "feature_group": "training_ready_core",
            "values": {"ppi_degree": 0, "pli_degree": 1},
            "provenance": {},
        }],
        graph_nodes=[
            {"node_id": "protein:1ABC:A", "node_type": "Protein", "primary_id": "1ABC:A",
             "metadata": {"pdb_id": "1ABC", "chain_id": "A"}},
            {"node_id": "protein:1ABC:B", "node_type": "Protein", "primary_id": "1ABC:B",
             "metadata": {"pdb_id": "1ABC", "chain_id": "B"}},
        ],
        graph_edges=[{
            "edge_id": "e1",
            "edge_type": "ProteinLigandInteraction",
            "source_node_id": "protein:1ABC:B",
            "target_node_id": "ligand:GTP",
            "source_database": "RCSB",
        }],
    )

    examples = json.loads(
        assemble_training_examples(extracted, features, graph, output)[0]
        .read_text(encoding="utf-8")
    )

    assert len(examples) == 1
    ex = examples[0]
    assert ex["structure"]["chain_ids"] == ["B"]
    assert ex["protein"]["uniprot_id"] == "P22222"
    assert ex["ligand"]["ligand_id"] == "GTP"
    assert ex["ligand"]["ligand_type"] == "small_molecule"
    assert ex["ligand"]["inchikey"] == "GTP-KEY"
    assert ex["ligand"]["smiles"] == "GTP-SMILES"
    assert ex["interaction"]["interface_residues"] == ["B:ASP40"]
    assert ex["experiment"]["source_database"] == "ChEMBL"
    assert ex["experiment"]["preferred_source_database"] == "PDBbind"
    assert ex["experiment"]["reported_measurement_count"] == 3
    assert ex["experiment"]["source_conflict_flag"] is True
    assert ex["experiment"]["source_agreement_band"] == "low"
    assert ex["labels"]["source_conflict_flag"] is True
    assert ex["labels"]["preferred_source_database"] == "PDBbind"
    assert ex["provenance"]["measurement_source_doi"] == "10.1000/example"
    assert ex["graph_features"]["pli_degree"] == 1


def test_assemble_propagates_dense_continuous_descriptors() -> None:
    tmp = _tmp_dir("assemble_dense_descriptors")
    extracted, features, graph, output = _setup_test_data(
        tmp,
        entries=[{"pdb_id": "1ABC", "structure_resolution": 2.0}],
        chains=[{"pdb_id": "1ABC", "chain_id": "A", "is_protein": True, "uniprot_id": "P12345"}],
        bound_objects=[],
        interfaces=[],
        assays=[{
            "pdb_id": "1ABC",
            "pair_identity_key": "protein_ligand|1ABC|A|ATP|wt",
            "binding_affinity_type": "Kd",
            "binding_affinity_value": 5.0,
        }],
        features=[{
            "feature_id": "f1",
            "pdb_id": "1ABC",
            "pair_identity_key": "protein_ligand|1ABC|A|ATP|wt",
            "feature_group": "training_ready_core",
            "values": {
                "sequence_length": 250,
                "protein_mean_hydropathy": 0.4,
                "protein_aromatic_fraction": 0.12,
                "protein_charged_fraction": 0.18,
                "protein_polar_fraction": 0.36,
                "atom_count_total": 3200,
                "heavy_atom_fraction": 0.94,
                "mean_atomic_weight": 13.5,
                "mean_covalent_radius": 0.82,
                "mean_b_factor": 21.7,
                "mean_occupancy": 0.98,
                "residue_count_observed": 248,
                "radius_of_gyration_residue_centroids": 18.2,
                "interface_residue_count": 7,
                "microstate_record_count": 6,
                "estimated_net_charge": -0.9,
                "mean_abs_residue_charge": 0.55,
                "positive_residue_count": 2,
                "negative_residue_count": 3,
                "same_charge_contact_count": 2,
                "opposite_charge_contact_count": 4,
                "metal_contact_count": 1,
                "acidic_cluster_penalty": 0.35,
                "local_electrostatic_balance": 1.65,
            },
            "provenance": {},
        }],
        graph_nodes=[],
        graph_edges=[],
    )

    examples = json.loads(
        assemble_training_examples(extracted, features, graph, output)[0]
        .read_text(encoding="utf-8")
    )
    ex = examples[0]

    assert ex["structure"]["atom_count_total"] == 3200
    assert ex["structure"]["mean_covalent_radius"] == 0.82
    assert ex["protein"]["sequence_length"] == 250
    assert ex["protein"]["mean_hydropathy"] == 0.4
    assert ex["interaction"]["interface_residue_count"] == 7
    assert ex["interaction"]["microstate_record_count"] == 6
    assert ex["interaction"]["estimated_net_charge"] == -0.9
    assert ex["interaction"]["local_electrostatic_balance"] == 1.65
