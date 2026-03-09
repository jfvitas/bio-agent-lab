"""Tests for the enhanced feature builder.

Validates pathway count computation from graph edges, graph feature
aggregation, sequence features, and edge cases.
"""

import json
from pathlib import Path
from uuid import uuid4

import gemmi

from pbdata.features.builder import (
    _compute_graph_features,
    _compute_pathway_counts,
    build_features_from_extracted_and_graph,
)

_LOCAL_TMP = Path(__file__).parent / "_tmp"
_LOCAL_TMP.mkdir(exist_ok=True)


def _tmp_dir(name: str) -> Path:
    path = _LOCAL_TMP / f"{uuid4().hex}_{name}"
    path.mkdir(exist_ok=True)
    return path


def _write_minimal_cif(path: Path) -> None:
    structure = gemmi.Structure()
    structure.name = "1ABC"
    model = gemmi.Model("1")
    chain = gemmi.Chain("A")
    residue = gemmi.Residue()
    residue.name = "GLY"
    residue.seqid = gemmi.SeqId("1")

    atom1 = gemmi.Atom()
    atom1.name = "CA"
    atom1.element = gemmi.Element("C")
    atom1.pos = gemmi.Position(0.0, 0.0, 0.0)
    atom1.occ = 1.0
    atom1.b_iso = 10.0

    atom2 = gemmi.Atom()
    atom2.name = "N"
    atom2.element = gemmi.Element("N")
    atom2.pos = gemmi.Position(1.2, 0.0, 0.0)
    atom2.occ = 0.8
    atom2.b_iso = 14.0

    residue.add_atom(atom1)
    residue.add_atom(atom2)
    chain.add_residue(residue)
    model.add_chain(chain)
    structure.add_model(model)
    structure.make_mmcif_document().write_file(str(path))


# ---------------------------------------------------------------------------
# _compute_pathway_counts
# ---------------------------------------------------------------------------


def test_pathway_counts_from_edges() -> None:
    nodes = [
        {"node_id": "protein:P12345", "node_type": "Protein"},
        {"node_id": "pathway:R-HSA-1", "node_type": "Pathway"},
        {"node_id": "pathway:R-HSA-2", "node_type": "Pathway"},
        {"node_id": "pathway:R-HSA-3", "node_type": "Pathway"},
    ]
    edges = [
        {"edge_type": "ProteinPathway", "source_node_id": "protein:P12345",
         "target_node_id": "pathway:R-HSA-1"},
        {"edge_type": "ProteinPathway", "source_node_id": "protein:P12345",
         "target_node_id": "pathway:R-HSA-2"},
        {"edge_type": "ProteinPathway", "source_node_id": "protein:P12345",
         "target_node_id": "pathway:R-HSA-3"},
        # PPI edge — should not count as pathway
        {"edge_type": "ProteinProteinInteraction", "source_node_id": "protein:P12345",
         "target_node_id": "protein:Q99999"},
    ]

    counts = _compute_pathway_counts(edges, nodes)
    assert counts["protein:P12345"] == 3
    assert "protein:Q99999" not in counts


def test_pathway_counts_empty() -> None:
    counts = _compute_pathway_counts([], [])
    assert counts == {}


def test_pathway_counts_no_pathway_edges() -> None:
    nodes = [{"node_id": "protein:P12345", "node_type": "Protein"}]
    edges = [
        {"edge_type": "ProteinProteinInteraction",
         "source_node_id": "protein:P12345",
         "target_node_id": "protein:Q99999"},
    ]
    counts = _compute_pathway_counts(edges, nodes)
    assert counts == {}


def test_pathway_counts_deduplicates_same_pathway() -> None:
    """Same pathway linked twice should count as one."""
    nodes = [
        {"node_id": "protein:P12345", "node_type": "Protein"},
        {"node_id": "pathway:R-HSA-1", "node_type": "Pathway"},
    ]
    edges = [
        {"edge_type": "ProteinPathway", "source_node_id": "protein:P12345",
         "target_node_id": "pathway:R-HSA-1"},
        # Duplicate edge (e.g., from different sources)
        {"edge_type": "ProteinPathway", "source_node_id": "protein:P12345",
         "target_node_id": "pathway:R-HSA-1"},
    ]
    counts = _compute_pathway_counts(edges, nodes)
    assert counts["protein:P12345"] == 1


# ---------------------------------------------------------------------------
# _compute_graph_features
# ---------------------------------------------------------------------------


def test_graph_features_comprehensive() -> None:
    nodes = [
        {"node_id": "protein:P12345", "node_type": "Protein"},
        {"node_id": "protein:Q99999", "node_type": "Protein"},
        {"node_id": "ligand:ATP", "node_type": "Ligand"},
        {"node_id": "pathway:R-HSA-1", "node_type": "Pathway"},
    ]
    edges = [
        # PPI
        {"edge_type": "ProteinProteinInteraction",
         "source_node_id": "protein:P12345",
         "target_node_id": "protein:Q99999"},
        # PLI
        {"edge_type": "ProteinLigandInteraction",
         "source_node_id": "protein:P12345",
         "target_node_id": "ligand:ATP"},
        # Pathway
        {"edge_type": "ProteinPathway",
         "source_node_id": "protein:P12345",
         "target_node_id": "pathway:R-HSA-1"},
    ]

    features = _compute_graph_features(edges, nodes)

    # P12345: degree=3 (PPI+PLI+pathway), ppi=1, pli=1, pathway=1
    assert features["protein:P12345"]["network_degree"] == 3
    assert features["protein:P12345"]["ppi_degree"] == 1
    assert features["protein:P12345"]["pli_degree"] == 1
    assert features["protein:P12345"]["pathway_count"] == 1

    # Q99999: degree=1 (PPI only), ppi=1
    assert features["protein:Q99999"]["network_degree"] == 1
    assert features["protein:Q99999"]["ppi_degree"] == 1
    assert features["protein:Q99999"]["pli_degree"] == 0
    assert features["protein:Q99999"]["pathway_count"] == 0


def test_graph_features_empty() -> None:
    features = _compute_graph_features([], [])
    assert features == {}


# ---------------------------------------------------------------------------
# Full feature builder integration
# ---------------------------------------------------------------------------


def test_features_with_pathway_data() -> None:
    """Feature builder should report pathway status correctly when pathway edges exist."""
    tmp = _tmp_dir("features_pathway")
    extracted = tmp / "extracted"
    for name in ["entry", "chains", "bound_objects", "interfaces", "assays"]:
        (extracted / name).mkdir(parents=True, exist_ok=True)

    (extracted / "entry" / "1ABC.json").write_text(json.dumps({
        "pdb_id": "1ABC", "structure_resolution": 2.0, "multiligand_entry": False,
    }), encoding="utf-8")
    (extracted / "chains" / "1ABC.json").write_text(json.dumps([
        {"pdb_id": "1ABC", "chain_id": "A", "is_protein": True, "uniprot_id": "P12345"},
    ]), encoding="utf-8")
    (extracted / "bound_objects" / "1ABC.json").write_text("[]", encoding="utf-8")
    (extracted / "interfaces" / "1ABC.json").write_text("[]", encoding="utf-8")
    (extracted / "assays" / "1ABC.json").write_text(json.dumps([{
        "pdb_id": "1ABC",
        "pair_identity_key": "protein_ligand|1ABC|A|ATP|wt",
        "binding_affinity_type": "Kd",
        "binding_affinity_value": 5.0,
    }]), encoding="utf-8")

    graph_dir = tmp / "graph"
    graph_dir.mkdir(parents=True, exist_ok=True)
    (graph_dir / "graph_nodes.json").write_text(json.dumps([
        {"node_id": "protein:P12345", "node_type": "Protein",
         "primary_id": "P12345", "metadata": {"pdb_id": "1ABC"}},
        {"node_id": "pathway:R-HSA-1", "node_type": "Pathway",
         "primary_id": "R-HSA-1"},
        {"node_id": "pathway:R-HSA-2", "node_type": "Pathway",
         "primary_id": "R-HSA-2"},
    ]), encoding="utf-8")
    (graph_dir / "graph_edges.json").write_text(json.dumps([
        {"edge_id": "e1", "edge_type": "ProteinPathway",
         "source_node_id": "protein:P12345", "target_node_id": "pathway:R-HSA-1",
         "source_database": "Reactome"},
        {"edge_id": "e2", "edge_type": "ProteinPathway",
         "source_node_id": "protein:P12345", "target_node_id": "pathway:R-HSA-2",
         "source_database": "Reactome"},
    ]), encoding="utf-8")

    output_dir = tmp / "features"
    features_path, manifest_path = build_features_from_extracted_and_graph(
        extracted, graph_dir, output_dir,
    )

    rows = json.loads(features_path.read_text(encoding="utf-8"))
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    assert len(rows) == 1
    assert rows[0]["values"]["pathway_count"] == 2
    assert rows[0]["provenance"]["pathway_count_status"] == "from_reactome"
    assert "Pathway data present" in manifest["notes"]


def test_features_without_pathway_data() -> None:
    """Without pathway edges, pathway_count should be 0 and status 'unknown'."""
    tmp = _tmp_dir("features_no_pathway")
    extracted = tmp / "extracted"
    for name in ["entry", "chains", "bound_objects", "interfaces", "assays"]:
        (extracted / name).mkdir(parents=True, exist_ok=True)

    (extracted / "entry" / "1ABC.json").write_text(json.dumps({
        "pdb_id": "1ABC", "structure_resolution": 2.0,
    }), encoding="utf-8")
    (extracted / "chains" / "1ABC.json").write_text(json.dumps([
        {"pdb_id": "1ABC", "chain_id": "A", "is_protein": True, "uniprot_id": "P12345"},
    ]), encoding="utf-8")
    (extracted / "bound_objects" / "1ABC.json").write_text("[]", encoding="utf-8")
    (extracted / "interfaces" / "1ABC.json").write_text("[]", encoding="utf-8")
    (extracted / "assays" / "1ABC.json").write_text(json.dumps([{
        "pdb_id": "1ABC",
        "pair_identity_key": "test|key",
        "binding_affinity_type": "Kd",
        "binding_affinity_value": 5.0,
    }]), encoding="utf-8")

    graph_dir = tmp / "graph"
    graph_dir.mkdir(parents=True, exist_ok=True)
    (graph_dir / "graph_nodes.json").write_text(json.dumps([
        {"node_id": "protein:P12345", "node_type": "Protein",
         "primary_id": "P12345", "metadata": {"pdb_id": "1ABC"}},
    ]), encoding="utf-8")
    (graph_dir / "graph_edges.json").write_text(json.dumps([
        {"edge_id": "e1", "edge_type": "ProteinLigandInteraction",
         "source_node_id": "protein:P12345", "target_node_id": "ligand:ATP",
         "source_database": "RCSB"},
    ]), encoding="utf-8")

    output_dir = tmp / "features"
    features_path, _ = build_features_from_extracted_and_graph(
        extracted, graph_dir, output_dir,
    )

    rows = json.loads(features_path.read_text(encoding="utf-8"))
    assert len(rows) == 1
    assert rows[0]["values"]["pathway_count"] == 0
    assert rows[0]["provenance"]["pathway_count_status"] == "unknown_external_pathway_sources_not_ingested"


def test_features_include_ppi_and_pli_degree() -> None:
    """Feature builder should include new ppi_degree and pli_degree fields."""
    tmp = _tmp_dir("features_degrees")
    extracted = tmp / "extracted"
    for name in ["entry", "chains", "bound_objects", "interfaces", "assays"]:
        (extracted / name).mkdir(parents=True, exist_ok=True)

    (extracted / "entry" / "1ABC.json").write_text(json.dumps({
        "pdb_id": "1ABC",
    }), encoding="utf-8")
    (extracted / "chains" / "1ABC.json").write_text(json.dumps([
        {"pdb_id": "1ABC", "chain_id": "A", "is_protein": True, "uniprot_id": "P12345"},
    ]), encoding="utf-8")
    (extracted / "bound_objects" / "1ABC.json").write_text("[]", encoding="utf-8")
    (extracted / "interfaces" / "1ABC.json").write_text("[]", encoding="utf-8")
    (extracted / "assays" / "1ABC.json").write_text(json.dumps([{
        "pdb_id": "1ABC",
        "pair_identity_key": "test|key",
        "binding_affinity_type": "Kd",
        "binding_affinity_value": 5.0,
    }]), encoding="utf-8")

    graph_dir = tmp / "graph"
    graph_dir.mkdir(parents=True, exist_ok=True)
    (graph_dir / "graph_nodes.json").write_text(json.dumps([
        {"node_id": "protein:P12345", "node_type": "Protein",
         "primary_id": "P12345", "metadata": {"pdb_id": "1ABC"}},
    ]), encoding="utf-8")
    (graph_dir / "graph_edges.json").write_text(json.dumps([
        {"edge_id": "e1", "edge_type": "ProteinProteinInteraction",
         "source_node_id": "protein:P12345", "target_node_id": "protein:Q99999",
         "source_database": "STRING"},
        {"edge_id": "e2", "edge_type": "ProteinLigandInteraction",
         "source_node_id": "protein:P12345", "target_node_id": "ligand:ATP",
         "source_database": "RCSB"},
    ]), encoding="utf-8")

    output_dir = tmp / "features"
    features_path, _ = build_features_from_extracted_and_graph(
        extracted, graph_dir, output_dir,
    )

    rows = json.loads(features_path.read_text(encoding="utf-8"))
    assert len(rows) == 1
    vals = rows[0]["values"]
    assert vals["network_degree"] == 2  # PPI + PLI
    assert vals["ppi_degree"] == 1


def test_features_merge_optional_microstate_and_physics_records() -> None:
    tmp = _tmp_dir("features_microstate_physics")
    extracted = tmp / "extracted"
    for name in ["entry", "chains", "bound_objects", "interfaces", "assays"]:
        (extracted / name).mkdir(parents=True, exist_ok=True)

    (extracted / "entry" / "1ABC.json").write_text(json.dumps({
        "pdb_id": "1ABC",
        "structure_resolution": 2.0,
    }), encoding="utf-8")
    (extracted / "chains" / "1ABC.json").write_text(json.dumps([{
        "pdb_id": "1ABC", "chain_id": "A", "is_protein": True, "uniprot_id": "P12345",
    }]), encoding="utf-8")
    (extracted / "bound_objects" / "1ABC.json").write_text("[]", encoding="utf-8")
    (extracted / "interfaces" / "1ABC.json").write_text("[]", encoding="utf-8")
    (extracted / "assays" / "1ABC.json").write_text(json.dumps([{
        "pdb_id": "1ABC",
        "pair_identity_key": "protein_ligand|1ABC|A|ATP|wt",
        "binding_affinity_type": "Kd",
        "binding_affinity_value": 5.0,
    }]), encoding="utf-8")

    graph_dir = tmp / "graph"
    graph_dir.mkdir(parents=True, exist_ok=True)
    (graph_dir / "graph_nodes.json").write_text(json.dumps([
        {"node_id": "protein:P12345", "node_type": "Protein",
         "primary_id": "P12345", "metadata": {"pdb_id": "1ABC", "chain_id": "A"}},
    ]), encoding="utf-8")
    (graph_dir / "graph_edges.json").write_text(json.dumps([
        {"edge_id": "e1", "edge_type": "ProteinLigandInteraction",
         "source_node_id": "protein:P12345", "target_node_id": "ligand:ATP",
         "source_database": "RCSB"},
    ]), encoding="utf-8")

    microstate_dir = tmp / "microstates"
    microstate_dir.mkdir(parents=True, exist_ok=True)
    (microstate_dir / "microstate_records.json").write_text(json.dumps([{
        "pdb_id": "1ABC",
        "pair_identity_key": "protein_ligand|1ABC|A|ATP|wt",
        "binding_affinity_type": "Kd",
        "record_count": 4,
    }]), encoding="utf-8")

    physics_dir = tmp / "physics"
    physics_dir.mkdir(parents=True, exist_ok=True)
    (physics_dir / "physics_feature_records.json").write_text(json.dumps([{
        "pdb_id": "1ABC",
        "pair_identity_key": "protein_ligand|1ABC|A|ATP|wt",
        "binding_affinity_type": "Kd",
        "estimated_net_charge": -0.8,
        "mean_abs_residue_charge": 0.6,
        "positive_residue_count": 1,
        "negative_residue_count": 2,
        "same_charge_contact_count": 1,
        "opposite_charge_contact_count": 3,
        "metal_contact_count": 1,
        "acidic_cluster_penalty": 0.4,
        "local_electrostatic_balance": 1.6,
    }]), encoding="utf-8")

    output_dir = tmp / "features"
    features_path, _ = build_features_from_extracted_and_graph(
        extracted,
        graph_dir,
        output_dir,
        microstate_dir=microstate_dir,
        physics_dir=physics_dir,
    )

    rows = json.loads(features_path.read_text(encoding="utf-8"))
    assert len(rows) == 1
    vals = rows[0]["values"]
    assert vals["microstate_record_count"] == 4
    assert vals["estimated_net_charge"] == -0.8
    assert vals["metal_contact_count"] == 1
    assert vals["pli_degree"] == 1
    assert rows[0]["provenance"]["microstate_status"] == "present"
    assert rows[0]["provenance"]["physics_feature_status"] == "present"


# ---------------------------------------------------------------------------
# Reversed pathway edge warning
# ---------------------------------------------------------------------------


def test_pathway_counts_reversed_edge_still_counted() -> None:
    """A ProteinPathway edge with reversed direction should still be counted (with warning)."""
    nodes = [
        {"node_id": "protein:P12345", "node_type": "Protein"},
        {"node_id": "pathway:R-HSA-1", "node_type": "Pathway"},
    ]
    # Source is pathway, target is protein — wrong direction
    edges = [
        {"edge_type": "ProteinPathway", "source_node_id": "pathway:R-HSA-1",
         "target_node_id": "protein:P12345"},
    ]

    counts = _compute_pathway_counts(edges, nodes)
    assert counts["protein:P12345"] == 1


def test_pathway_counts_dedup_across_directions() -> None:
    """Same pathway linked in both directions should count as one."""
    nodes = [
        {"node_id": "protein:P12345", "node_type": "Protein"},
        {"node_id": "pathway:R-HSA-1", "node_type": "Pathway"},
    ]
    edges = [
        {"edge_type": "ProteinPathway", "source_node_id": "protein:P12345",
         "target_node_id": "pathway:R-HSA-1"},
        # Same pathway, reversed direction
        {"edge_type": "ProteinPathway", "source_node_id": "pathway:R-HSA-1",
         "target_node_id": "protein:P12345"},
    ]

    counts = _compute_pathway_counts(edges, nodes)
    assert counts["protein:P12345"] == 1


def test_features_use_polymer_sequence_and_pair_specific_ligand() -> None:
    tmp = _tmp_dir("features_pair_specific")
    extracted = tmp / "extracted"
    for name in ["entry", "chains", "bound_objects", "interfaces", "assays"]:
        (extracted / name).mkdir(parents=True, exist_ok=True)

    (extracted / "entry" / "1ABC.json").write_text(json.dumps({
        "pdb_id": "1ABC",
        "structure_resolution": 2.0,
        "metal_present": True,
        "cofactor_present": False,
        "glycan_present": True,
        "covalent_binder_present": False,
        "peptide_partner_present": False,
        "membrane_vs_soluble": "membrane",
        "quality_score": 0.75,
    }), encoding="utf-8")
    (extracted / "chains" / "1ABC.json").write_text(json.dumps([
        {"pdb_id": "1ABC", "chain_id": "A", "is_protein": True, "polymer_sequence": "AAAA"},
        {"pdb_id": "1ABC", "chain_id": "B", "is_protein": True, "polymer_sequence": "BBBBBB"},
    ]), encoding="utf-8")
    (extracted / "bound_objects" / "1ABC.json").write_text(json.dumps([
        {"pdb_id": "1ABC", "component_id": "ATP", "component_molecular_weight": 507.0},
        {
            "pdb_id": "1ABC", "component_id": "GTP", "component_molecular_weight": 600.0,
            "component_type": "small_molecule", "component_inchikey": "GTP-KEY",
            "is_covalent": False,
        },
    ]), encoding="utf-8")
    (extracted / "interfaces" / "1ABC.json").write_text(json.dumps([
        {
            "pdb_id": "1ABC",
            "interface_type": "protein_ligand",
            "binding_site_chain_ids": ["B"],
            "binding_site_residue_ids": ["TYR15", "ASP34", "LYS40"],
            "entity_name_b": "GTP",
        }
    ]), encoding="utf-8")
    (extracted / "assays" / "1ABC.json").write_text(json.dumps([{
        "pdb_id": "1ABC",
        "pair_identity_key": "protein_ligand|1ABC|B|GTP|wt",
        "source_database": "PDBbind",
        "selected_preferred_source": "PDBbind",
        "binding_affinity_type": "Kd",
        "binding_affinity_value": 5.0,
        "binding_affinity_is_mutant_measurement": False,
        "reported_measurement_count": 2,
        "source_conflict_flag": True,
        "source_agreement_band": "low",
    }]), encoding="utf-8")

    graph_dir = tmp / "graph"
    graph_dir.mkdir(parents=True, exist_ok=True)
    (graph_dir / "graph_nodes.json").write_text(json.dumps([
        {
            "node_id": "protein:1ABC:A",
            "node_type": "Protein",
            "primary_id": "1ABC:A",
            "metadata": {"pdb_id": "1ABC", "chain_id": "A"},
        },
        {
            "node_id": "protein:1ABC:B",
            "node_type": "Protein",
            "primary_id": "1ABC:B",
            "metadata": {"pdb_id": "1ABC", "chain_id": "B"},
        },
    ]), encoding="utf-8")
    (graph_dir / "graph_edges.json").write_text(json.dumps([
        {
            "edge_id": "e1",
            "edge_type": "ProteinLigandInteraction",
            "source_node_id": "protein:1ABC:B",
            "target_node_id": "ligand:GTP",
            "source_database": "RCSB",
        }
    ]), encoding="utf-8")

    rows = json.loads(
        build_features_from_extracted_and_graph(
            extracted, graph_dir, tmp / "features",
        )[0].read_text(encoding="utf-8")
    )
    vals = rows[0]["values"]
    assert vals["sequence_length"] == 6
    assert vals["ligand_molecular_weight"] == 600.0
    assert vals["interface_residue_count"] == 3
    assert vals["ligand_component_type"] == "small_molecule"
    assert vals["ligand_inchikey"] == "GTP-KEY"
    assert vals["source_conflict_flag"] is True
    assert vals["preferred_source_database"] == "PDBbind"
    assert vals["metal_present"] is True
    assert vals["glycan_present"] is True
    assert vals["membrane_vs_soluble"] == "membrane"
    assert vals["quality_score"] == 0.75


def test_features_include_dense_structure_descriptors_from_cif() -> None:
    tmp = _tmp_dir("features_dense_structure")
    extracted = tmp / "extracted"
    for name in ["entry", "chains", "bound_objects", "interfaces", "assays"]:
        (extracted / name).mkdir(parents=True, exist_ok=True)

    cif_path = tmp / "1ABC.cif"
    _write_minimal_cif(cif_path)

    (extracted / "entry" / "1ABC.json").write_text(json.dumps({
        "pdb_id": "1ABC",
        "structure_resolution": 2.0,
        "structure_file_cif_path": str(cif_path),
    }), encoding="utf-8")
    (extracted / "chains" / "1ABC.json").write_text(json.dumps([
        {"pdb_id": "1ABC", "chain_id": "A", "is_protein": True, "polymer_sequence": "AFDK"},
    ]), encoding="utf-8")
    (extracted / "bound_objects" / "1ABC.json").write_text("[]", encoding="utf-8")
    (extracted / "interfaces" / "1ABC.json").write_text("[]", encoding="utf-8")
    (extracted / "assays" / "1ABC.json").write_text(json.dumps([{
        "pdb_id": "1ABC",
        "pair_identity_key": "protein_ligand|1ABC|A|ATP|wt",
        "binding_affinity_type": "Kd",
        "binding_affinity_value": 5.0,
    }]), encoding="utf-8")

    graph_dir = tmp / "graph"
    graph_dir.mkdir(parents=True, exist_ok=True)
    (graph_dir / "graph_nodes.json").write_text(json.dumps([
        {"node_id": "protein:1ABC:A", "node_type": "Protein", "primary_id": "1ABC:A", "metadata": {"pdb_id": "1ABC", "chain_id": "A"}},
    ]), encoding="utf-8")
    (graph_dir / "graph_edges.json").write_text("[]", encoding="utf-8")

    values = json.loads(
        build_features_from_extracted_and_graph(extracted, graph_dir, tmp / "features")[0]
        .read_text(encoding="utf-8")
    )[0]["values"]

    assert values["atom_count_total"] == 2
    assert values["mean_b_factor"] == 12.0
    assert abs(values["mean_occupancy"] - 0.9) < 1e-6
    assert values["mean_covalent_radius"] is not None
    assert values["protein_mean_hydropathy"] is not None
    assert values["protein_aromatic_fraction"] == 0.25
