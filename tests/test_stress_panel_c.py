"""Panel C stress tests — extended structural extraction validation.

Tests the multi-table extraction pipeline against 12 PDB entries covering:
- multimeric complexes, cofactors, metals, symmetric assemblies
- glycosylated immune complexes
- covalent inhibitors
- very large membrane complexes
- metal-chelate ligands
- glycan binders
- peptide ligands in membrane receptors
- large oligomeric conformational complexes

Per AGENTS.md: stress_test_panel_C.yaml and expected_outcomes_panel_C.md
are IMMUTABLE.  If a test fails, fix the code or assertions, not the panels.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest
import yaml

from pbdata.pipeline.extract import extract_rcsb_entry
from pbdata.quality.audit import compute_flags
from pbdata.schemas.records import (
    BoundObjectRecord,
    ChainRecord,
    EntryRecord,
    InterfaceRecord,
    ProvenanceRecord,
)
from pbdata.sources.rcsb import RCSBAdapter
from pbdata.sources.rcsb_classify import classify_entry

_REPO_ROOT = Path(__file__).parent.parent
_PANEL_C_PATH = _REPO_ROOT / "stress_test_panel_C.yaml"


# ── Known xfail cases ───────────────────────────────────────────────
# Document known discrepancies between panel expectations and deposited
# RCSB data.  These record structural biology facts — they do NOT weaken
# the panel.
_PANEL_C_XFAIL: dict[str, dict[str, str]] = {}


# ── Panel loader ─────────────────────────────────────────────────────

def _load_panel_c() -> list[dict]:
    if not _PANEL_C_PATH.exists():
        return []
    with _PANEL_C_PATH.open(encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    return data.get("structures") or []


# ── Integration test: classification flags ───────────────────────────

@pytest.mark.integration
@pytest.mark.parametrize(
    "case",
    _load_panel_c(),
    ids=[c.get("pdb_id", "?") for c in _load_panel_c()],
)
def test_panel_c_classification_flags(case: dict) -> None:
    """Verify classification flags match panel expectations for each entry."""
    pdb_id: str = case["pdb_id"]
    expected_flags: dict = case.get("expected_flags", {})
    categories: list[str] = case.get("categories", [])
    xfails = _PANEL_C_XFAIL.get(pdb_id, {})

    adapter = RCSBAdapter()
    try:
        raw = adapter.fetch_metadata(pdb_id)
    except Exception as exc:
        pytest.skip(f"live RCSB fetch unavailable for {pdb_id}: {exc}")

    classified = classify_entry(raw)
    record = adapter.normalize_record(raw)
    flags = compute_flags(record)
    bound_objects = classified["bound_objects"]
    non_artifact = [b for b in bound_objects if b.role != "artifact"]

    # ── homomeric_or_heteromeric ─────────────────────────────────────
    if "homomeric_or_heteromeric" in expected_flags:
        expected_hh = expected_flags["homomeric_or_heteromeric"]
        if expected_hh == "heteromeric":
            n_protein = len(classified["protein_entities"])
            n_peptide = len(classified["peptide_entities"])
            assert n_protein >= 2 or n_peptide >= 1 or classified["is_homo_oligomeric"] is False, (
                f"{pdb_id}: expected heteromeric but "
                f"is_homo={classified['is_homo_oligomeric']}, "
                f"n_protein={n_protein}, n_peptide={n_peptide}"
            )
        elif expected_hh == "homomeric":
            assert classified["is_homo_oligomeric"] is True, (
                f"{pdb_id}: expected homomeric"
            )

    # ── metal_present ────────────────────────────────────────────────
    if expected_flags.get("metal_present") is True:
        xfail_key = "metal_present"
        if xfail_key in xfails and "metal_present" not in flags:
            pytest.xfail(f"{pdb_id}: {xfails[xfail_key]}")
        else:
            assert "metal_present" in flags, (
                f"{pdb_id}: expected metal_present flag; "
                f"flags={flags}, "
                f"bound_objects={[(b.comp_id, b.binder_type) for b in bound_objects]}"
            )

    # ── cofactor_present ─────────────────────────────────────────────
    if expected_flags.get("cofactor_present") is True:
        # Accept either cofactor or peptide_partner (NS4A-type peptide
        # activators are cofactors biologically)
        assert "cofactor_present" in flags or "peptide_partner" in flags, (
            f"{pdb_id}: expected cofactor_present flag; flags={flags}"
        )

    # ── peptide_partner ──────────────────────────────────────────────
    if expected_flags.get("peptide_partner") is True:
        peptides = [b for b in bound_objects if b.binder_type == "peptide"]
        assert len(peptides) >= 1 or "peptide_partner" in flags, (
            f"{pdb_id}: expected peptide_partner; "
            f"bound_objects={[(b.comp_id, b.binder_type) for b in bound_objects]}"
        )

    # ── multiple_bound_objects ───────────────────────────────────────
    if expected_flags.get("multiple_bound_objects") is True:
        assert len(non_artifact) > 1, (
            f"{pdb_id}: expected multiple_bound_objects, got {len(non_artifact)} non-artifact"
        )

    # ── glycan_present ───────────────────────────────────────────────
    if expected_flags.get("glycan_present") is True:
        glycans = [b for b in bound_objects if b.binder_type == "glycan"]
        other_poly = classified.get("other_poly", [])
        poly_glycans = [
            e for e in other_poly
            if "polysaccharide" in ((e.get("entity_poly") or {}).get("type") or "").lower()
        ]
        # Also check branched entities from mmCIF supplement
        supplement = raw.get("mmcif_supplement") or {}
        branched = supplement.get("branched_entities", [])
        assert glycans or poly_glycans or branched, (
            f"{pdb_id}: expected glycan_present but found none; "
            f"bound_objects={[(b.comp_id, b.binder_type) for b in bound_objects]}"
        )

    # ── heteromeric_interface ────────────────────────────────────────
    if expected_flags.get("heteromeric_interface") is True:
        hetero_ifaces = [i for i in classified["interfaces"] if i.is_hetero]
        n_protein = len(classified["protein_entities"])
        n_peptide = len(classified["peptide_entities"])
        assert hetero_ifaces or n_protein >= 2 or n_peptide >= 1, (
            f"{pdb_id}: expected heteromeric_interface; "
            f"interfaces={len(classified['interfaces'])}, "
            f"n_protein={n_protein}, n_peptide={n_peptide}"
        )

    # ── covalent_binder ──────────────────────────────────────────────
    if expected_flags.get("covalent_binder") is True:
        assert "covalent_binder" in flags, (
            f"{pdb_id}: expected covalent_binder flag; flags={flags}"
        )

    # ── large_assembly ───────────────────────────────────────────────
    if expected_flags.get("large_assembly") is True:
        chain_count = record.polymer_entity_count or 0
        n_bound = len(non_artifact)
        assert chain_count >= 4 or n_bound >= 5, (
            f"{pdb_id}: expected large_assembly; "
            f"chain_count={chain_count}, n_bound={n_bound}"
        )

    # ── membrane_complex ─────────────────────────────────────────────
    if expected_flags.get("membrane_complex") is True:
        assert "membrane_protein_context" in flags, (
            f"{pdb_id}: expected membrane_complex flag; flags={flags}"
        )

    # ── metal_mediated_binding_possible ──────────────────────────────
    if expected_flags.get("metal_mediated_binding_possible") is True:
        xfail_key = "metal_mediated_binding_possible"
        if xfail_key in xfails and "metal_mediated_binding_possible" not in flags:
            pytest.xfail(f"{pdb_id}: {xfails[xfail_key]}")
        else:
            assert "metal_mediated_binding_possible" in flags, (
                f"{pdb_id}: expected metal_mediated_binding_possible; flags={flags}"
            )

    # ── multimeric_complex ───────────────────────────────────────────
    if expected_flags.get("multimeric_complex") is True:
        chain_count = record.polymer_entity_count or 0
        n_protein = len(classified["protein_entities"])
        assert chain_count >= 4 or n_protein >= 3, (
            f"{pdb_id}: expected multimeric_complex; "
            f"chain_count={chain_count}, n_protein={n_protein}"
        )


# ── Integration test: RCSB source expectations ──────────────────────

@pytest.mark.integration
@pytest.mark.parametrize(
    "case",
    _load_panel_c(),
    ids=[c.get("pdb_id", "?") for c in _load_panel_c()],
)
def test_panel_c_rcsb_source_expectations(case: dict) -> None:
    """Verify RCSB-sourced fields are populated per panel source_expectations."""
    pdb_id: str = case["pdb_id"]
    source_exp = case.get("source_expectations", {}).get("rcsb", {})
    provide_fields: list[str] = source_exp.get("provide", [])

    if not provide_fields:
        pytest.skip(f"{pdb_id}: no RCSB source_expectations defined")

    adapter = RCSBAdapter()
    try:
        raw = adapter.fetch_metadata(pdb_id)
    except Exception as exc:
        pytest.skip(f"live RCSB fetch unavailable for {pdb_id}: {exc}")

    classified = classify_entry(raw)
    record = adapter.normalize_record(raw)
    bound_objects = classified["bound_objects"]

    for field in provide_fields:
        if field == "chain_ids":
            assert record.chain_ids_receptor, (
                f"{pdb_id}: RCSB should provide chain_ids but receptor chains empty"
            )

        elif field == "polymer_sequences":
            assert record.sequence_receptor, (
                f"{pdb_id}: RCSB should provide polymer_sequences"
            )

        elif field == "assembly_information":
            assert record.assembly_info or record.oligomeric_state, (
                f"{pdb_id}: RCSB should provide assembly_information"
            )

        elif field == "ligand_ids":
            ligands = [b for b in bound_objects if b.binder_type != "additive"]
            assert ligands, (
                f"{pdb_id}: RCSB should provide ligand_ids but no non-additive bound objects"
            )

        elif field == "ligand_names":
            named = [b for b in bound_objects if b.name]
            assert named, (
                f"{pdb_id}: RCSB should provide ligand_names"
            )

        elif field == "metal_atoms":
            metals = [b for b in bound_objects if b.binder_type == "metal_ion"]
            metal_cofactors = [
                b for b in bound_objects
                if b.binder_type == "cofactor"
                and (b.comp_id or "").upper() in {"HEM", "HEC", "HEA", "FES", "SF4"}
            ]
            assert metals or metal_cofactors, (
                f"{pdb_id}: RCSB should provide metal_atoms"
            )

        elif field == "glycan_entities":
            glycans = [b for b in bound_objects if b.binder_type == "glycan"]
            other_poly = classified.get("other_poly", [])
            poly_glycans = [
                e for e in other_poly
                if "polysaccharide" in ((e.get("entity_poly") or {}).get("type") or "").lower()
            ]
            supplement = raw.get("mmcif_supplement") or {}
            branched = supplement.get("branched_entities", [])
            assert glycans or poly_glycans or branched, (
                f"{pdb_id}: RCSB should provide glycan_entities"
            )

        elif field == "taxonomy":
            assert record.taxonomy_ids, (
                f"{pdb_id}: RCSB should provide taxonomy"
            )

        elif field == "cofactor_entities" or field == "cofactors":
            cofactors = [b for b in bound_objects if b.binder_type == "cofactor"]
            assert cofactors, (
                f"{pdb_id}: RCSB should provide cofactor entities"
            )

        elif field == "chemical_component_dictionary_entries":
            # Just verify nonpolymer entities are present
            assert bound_objects, (
                f"{pdb_id}: RCSB should provide chemical component entries"
            )


# ── Integration test: multi-table extraction pipeline ────────────────

@pytest.mark.integration
@pytest.mark.parametrize(
    "case",
    _load_panel_c(),
    ids=[c.get("pdb_id", "?") for c in _load_panel_c()],
)
def test_panel_c_multi_table_extraction(case: dict) -> None:
    """Verify multi-table extraction produces correct record types with required fields.

    Checks acceptance criteria from expected_outcomes_panel_C.md:
    1. Structure parsed successfully
    2. All chains mapped to entities
    3. Ligands classified correctly
    4. Metals and cofactors identified
    5. Biological assembly determined
    6. No silent collapsing of multi-ligand systems
    7. Provenance recorded for each field
    """
    pdb_id: str = case["pdb_id"]
    expected_flags: dict = case.get("expected_flags", {})

    adapter = RCSBAdapter()
    try:
        raw = adapter.fetch_metadata(pdb_id)
    except Exception as exc:
        pytest.skip(f"live RCSB fetch unavailable for {pdb_id}: {exc}")

    # Mock structure download to avoid downloading large CIF files during tests
    with patch("pbdata.pipeline.extract.download_structure_files") as mock_dl:
        mock_dl.return_value = {
            "parsed_structure_format": "mmCIF",
            "structure_download_url": f"https://files.rcsb.org/download/{pdb_id}.cif",
        }
        records = extract_rcsb_entry(raw)

    entry = records["entry"]
    chains = records["chains"]
    bound_objs = records["bound_objects"]
    interfaces = records["interfaces"]
    provenance = records["provenance"]

    # ── 1. Structure parsed successfully ─────────────────────────────
    assert isinstance(entry, EntryRecord), f"{pdb_id}: EntryRecord not produced"
    assert entry.pdb_id == pdb_id
    assert entry.source_database == "RCSB"
    assert entry.title is not None, f"{pdb_id}: title should be populated"
    assert entry.experimental_method is not None, f"{pdb_id}: experimental_method should be populated"

    # ── 2. All chains mapped to entities ─────────────────────────────
    assert len(chains) >= 1, f"{pdb_id}: should have at least one chain record"
    assert all(isinstance(c, ChainRecord) for c in chains)
    assert all(c.chain_id for c in chains), f"{pdb_id}: all chains must have chain_id"
    # Every chain should have an entity type
    for c in chains:
        assert c.entity_type is not None, f"{pdb_id}: chain {c.chain_id} missing entity_type"

    # ── 3. Ligands classified correctly ──────────────────────────────
    if bound_objs:
        assert all(isinstance(b, BoundObjectRecord) for b in bound_objs)
        for bo in bound_objs:
            assert bo.component_type is not None, (
                f"{pdb_id}: bound object {bo.component_id} missing component_type"
            )
            assert bo.component_role is not None, (
                f"{pdb_id}: bound object {bo.component_id} missing component_role"
            )

    # ── 4. Metals and cofactors identified ───────────────────────────
    if expected_flags.get("metal_present"):
        metal_bos = [b for b in bound_objs if b.component_type == "metal"]
        metal_cofactor_bos = [
            b for b in bound_objs
            if b.component_type == "cofactor"
            and (b.component_id or "").upper() in {"HEM", "HEC", "HEA", "FES", "SF4"}
        ]
        assert metal_bos or metal_cofactor_bos, (
            f"{pdb_id}: expected metal in bound_object_records but found none"
        )

    if expected_flags.get("cofactor_present"):
        cofactor_bos = [
            b for b in bound_objs
            if b.component_type in ("cofactor", "peptide")
        ]
        assert cofactor_bos, (
            f"{pdb_id}: expected cofactor in bound_object_records"
        )

    # ── 5. Biological assembly determined ────────────────────────────
    assert entry.oligomeric_state is not None or entry.assembly_id is not None or \
           entry.homomer_or_heteromer is not None, (
        f"{pdb_id}: assembly information should be populated"
    )

    # ── 6. No silent collapsing of multi-ligand systems ──────────────
    if expected_flags.get("multiple_bound_objects"):
        non_additive = [
            b for b in bound_objs
            if b.component_type not in ("crystallization_additive", "buffer_component", "solvent")
        ]
        assert len(non_additive) > 1, (
            f"{pdb_id}: expected multiple bound objects but got {len(non_additive)}"
        )

    # ── 7. Provenance recorded ───────────────────────────────────────
    assert len(provenance) >= 1, f"{pdb_id}: provenance records should be present"
    assert all(isinstance(p, ProvenanceRecord) for p in provenance)
    sources = {p.source_name for p in provenance}
    assert "RCSB" in sources, f"{pdb_id}: RCSB should be in provenance sources"

    # ── Entry-level bias fields populated ────────────────────────────
    assert entry.membrane_vs_soluble in ("membrane", "soluble"), (
        f"{pdb_id}: membrane_vs_soluble should be populated"
    )
    if entry.structure_resolution is not None:
        assert entry.resolution_bin is not None, (
            f"{pdb_id}: resolution_bin should be populated when resolution is available"
        )

    # ── Taxonomy populated ───────────────────────────────────────────
    protein_chains = [c for c in chains if c.is_protein]
    if protein_chains:
        chains_with_tax = [c for c in protein_chains if c.entity_source_taxonomy_id]
        assert chains_with_tax, (
            f"{pdb_id}: at least one protein chain should have taxonomy_id"
        )


# ── Integration test: entry-level field coverage ─────────────────────

@pytest.mark.integration
@pytest.mark.parametrize(
    "case",
    _load_panel_c(),
    ids=[c.get("pdb_id", "?") for c in _load_panel_c()],
)
def test_panel_c_entry_field_coverage(case: dict) -> None:
    """Verify that key entry-level fields are populated per the spec."""
    pdb_id: str = case["pdb_id"]

    adapter = RCSBAdapter()
    try:
        raw = adapter.fetch_metadata(pdb_id)
    except Exception as exc:
        pytest.skip(f"live RCSB fetch unavailable for {pdb_id}: {exc}")

    with patch("pbdata.pipeline.extract.download_structure_files") as mock_dl:
        mock_dl.return_value = {"parsed_structure_format": "mmCIF"}
        records = extract_rcsb_entry(raw)

    entry = records["entry"]

    # Fields that should always be populated from RCSB
    assert entry.title, f"{pdb_id}: title missing"
    assert entry.experimental_method, f"{pdb_id}: experimental_method missing"
    assert entry.source_url, f"{pdb_id}: source_url missing"
    assert entry.pdb_id == pdb_id
    assert entry.task_hint is not None, f"{pdb_id}: task_hint missing"
    assert entry.downloaded_at is not None, f"{pdb_id}: downloaded_at missing"

    # Counts should be populated
    assert entry.protein_entity_count is not None, f"{pdb_id}: protein_entity_count missing"
    assert entry.polymer_entity_count is not None, f"{pdb_id}: polymer_entity_count missing"

    # Quality flags should be a list (may be empty)
    assert isinstance(entry.quality_flags, list), f"{pdb_id}: quality_flags should be a list"
