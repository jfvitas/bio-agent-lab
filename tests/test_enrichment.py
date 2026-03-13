import json
from pathlib import Path
from unittest.mock import patch
from uuid import uuid4

import requests

from pbdata.config import AppConfig, SourceConfig, SourcesConfig
from pbdata.pipeline.enrichment import (
    fetch_bindingdb_samples_for_pdb,
    fetch_chembl_samples_for_raw,
    load_external_assay_samples,
)
from pbdata.sources.chembl import ChEMBLAdapter
from pbdata.storage import build_storage_layout
from pbdata.source_state import export_source_state_run_summary, snapshot_source_state_counters, write_source_state

_LOCAL_TMP = Path(__file__).parent / "_tmp"
_LOCAL_TMP.mkdir(exist_ok=True)


def _tmp_dir(name: str) -> Path:
    path = _LOCAL_TMP / f"{uuid4().hex}_{name}"
    path.mkdir(exist_ok=True)
    return path


def test_load_external_assay_samples_loads_skempi_file() -> None:
    layout = build_storage_layout(_tmp_dir("enrichment_skempi"))
    skempi_path = layout.raw_skempi_dir / "skempi_v2.csv"
    skempi_path.parent.mkdir(parents=True, exist_ok=True)
    skempi_path.write_text(
        "#Pdb;#Mutation(s)_cleaned;ddG (kcal/mol)\n"
        "1ABC;A42V;1.1\n",
        encoding="utf-8",
    )
    config = AppConfig(
        sources=SourcesConfig(
            skempi=SourceConfig(enabled=True, extra={"local_path": str(skempi_path)}),
        )
    )

    grouped = load_external_assay_samples(config, layout=layout)

    assert list(grouped) == ["1ABC"]
    assert grouped["1ABC"][0].task_type == "mutation_ddg"


def test_fetch_bindingdb_samples_for_pdb_uses_managed_cache_when_present() -> None:
    layout = build_storage_layout(_tmp_dir("enrichment_bindingdb"))
    cache_path = layout.raw_bindingdb_dir / "1ABC.json"
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(json.dumps({
        "pdb_id": "1ABC",
        "monomers": [{
            "monomerID": "ATP",
            "affinities": [{"affinity": "5", "affinityUnit": "nM", "affinityType": "Kd"}],
        }],
    }), encoding="utf-8")
    config = AppConfig(
        sources=SourcesConfig(
            bindingdb=SourceConfig(enabled=True),
        )
    )

    rows = fetch_bindingdb_samples_for_pdb("1ABC", config, layout=layout)

    assert len(rows) == 1
    assert rows[0].provenance["cache_mode"] == "managed_cache"
    assert rows[0].provenance["cache_path"] == str(cache_path)


def test_fetch_chembl_samples_for_raw_returns_empty_without_identifiers() -> None:
    raw = {"rcsb_id": "1ABC", "polymer_entities": [], "nonpolymer_entities": []}
    config = AppConfig(sources=SourcesConfig(chembl=SourceConfig(enabled=True)))

    assert fetch_chembl_samples_for_raw(raw, {}, config) == []


def test_fetch_chembl_samples_for_raw_applies_pair_override() -> None:
    raw = {
        "rcsb_id": "1ABC",
        "polymer_entities": [{
            "rcsb_polymer_entity_container_identifiers": {
                "uniprot_ids": ["P12345"],
                "auth_asym_ids": ["A"],
            }
        }],
        "nonpolymer_entities": [{"nonpolymer_comp": {"chem_comp": {"id": "ATP"}}}],
    }
    config = AppConfig(sources=SourcesConfig(chembl=SourceConfig(enabled=True)))
    chem_descriptors = {"ATP": {"InChIKey": "AAAA-BBBB"}}
    sample = ChEMBLAdapter().normalize_record({
        "activity": {
            "activity_chembl_id": "CHEMBL_ACT_3",
            "assay_chembl_id": "CHEMBL_ASSAY_3",
            "standard_type": "Kd",
            "standard_value": "5",
            "standard_units": "nM",
            "target_pref_name": "Example kinase",
            "assay_description": "wild type enzyme",
        },
        "accession": "P12345",
        "inchikey": "AAAA-BBBB",
        "target_chembl_id": "CHEMBL_TGT",
        "molecule_chembl_id": "CHEMBL_MOL",
    })

    layout = build_storage_layout(_tmp_dir("chembl_source_state"))

    with patch.object(ChEMBLAdapter, "fetch_by_uniprot_and_inchikey", return_value=[sample]):
        enriched = fetch_chembl_samples_for_raw(raw, chem_descriptors, config, layout=layout)

    assert len(enriched) == 1
    assert enriched[0].provenance["pair_grouping_override"] == "protein_ligand|1ABC|A|AAAA-BBBB|wildtype"
    state = json.loads((layout.source_state_dir / "chembl.json").read_text(encoding="utf-8"))
    assert state["status"] == "ready"
    assert state["record_count"] == 1
    assert state["extra"]["failed_lookup_count"] == 0


def test_fetch_chembl_samples_for_raw_records_lookup_failure() -> None:
    raw = {
        "rcsb_id": "1ABC",
        "polymer_entities": [{
            "rcsb_polymer_entity_container_identifiers": {
                "uniprot_ids": ["P12345"],
                "auth_asym_ids": ["A"],
            }
        }],
        "nonpolymer_entities": [{"nonpolymer_comp": {"chem_comp": {"id": "ATP"}}}],
    }
    config = AppConfig(sources=SourcesConfig(chembl=SourceConfig(enabled=True)))
    chem_descriptors = {"ATP": {"InChIKey": "AAAA-BBBB"}}
    layout = build_storage_layout(_tmp_dir("chembl_failure_state"))

    with patch.object(ChEMBLAdapter, "fetch_by_uniprot_and_inchikey", side_effect=RuntimeError("upstream busy")):
        enriched = fetch_chembl_samples_for_raw(raw, chem_descriptors, config, layout=layout)

    assert enriched == []
    state = json.loads((layout.source_state_dir / "chembl.json").read_text(encoding="utf-8"))
    assert state["status"] == "lookup_failed"
    assert state["extra"]["failed_lookup_count"] == 1


def test_fetch_bindingdb_samples_for_pdb_treats_404_as_no_match() -> None:
    layout = build_storage_layout(_tmp_dir("bindingdb_404"))
    config = AppConfig(
        sources=SourcesConfig(
            bindingdb=SourceConfig(enabled=True),
        )
    )
    err = requests.HTTPError("not found")
    err.response = type("_Resp", (), {"status_code": 404})()

    with patch("pbdata.sources.bindingdb.BindingDBAdapter.fetch_metadata", side_effect=err):
        rows = fetch_bindingdb_samples_for_pdb("1ABC", config, layout=layout)

    assert rows == []
    state = json.loads((layout.source_state_dir / "bindingdb.json").read_text(encoding="utf-8"))
    assert state["status"] == "no_matches"


def test_fetch_bindingdb_samples_for_pdb_skips_entries_without_enrichable_ligands() -> None:
    layout = build_storage_layout(_tmp_dir("bindingdb_skip_no_ligand"))
    config = AppConfig(
        sources=SourcesConfig(
            bindingdb=SourceConfig(enabled=True),
        )
    )
    raw = {
        "rcsb_id": "2D0N",
        "nonpolymer_entities": [],
    }

    with patch("pbdata.sources.bindingdb.BindingDBAdapter.fetch_metadata") as fetch_metadata:
        rows = fetch_bindingdb_samples_for_pdb("2D0N", config, layout=layout, raw=raw)

    assert rows == []
    fetch_metadata.assert_not_called()
    state = json.loads((layout.source_state_dir / "bindingdb.json").read_text(encoding="utf-8"))
    assert state["status"] == "missing_identifiers"


def test_fetch_chembl_samples_for_raw_ignores_excluded_small_molecule_artifacts() -> None:
    raw = {
        "rcsb_id": "1ABC",
        "polymer_entities": [{
            "rcsb_polymer_entity_container_identifiers": {
                "uniprot_ids": ["P12345"],
                "auth_asym_ids": ["A"],
            }
        }],
        "nonpolymer_entities": [
            {"nonpolymer_comp": {"chem_comp": {"id": "SO4"}}},
            {"nonpolymer_comp": {"chem_comp": {"id": "MG"}}},
        ],
    }
    config = AppConfig(sources=SourcesConfig(chembl=SourceConfig(enabled=True)))
    chem_descriptors = {
        "SO4": {"InChIKey": "SULFATE-KEY"},
        "MG": {"InChIKey": "MAGNESIUM-KEY"},
    }

    with patch.object(ChEMBLAdapter, "fetch_by_uniprot_and_inchikey") as fetch_lookup:
        enriched = fetch_chembl_samples_for_raw(raw, chem_descriptors, config)

    assert enriched == []
    fetch_lookup.assert_not_called()


def test_write_source_state_accumulates_attempt_counts() -> None:
    layout = build_storage_layout(_tmp_dir("source_state_accumulate"))

    write_source_state(layout, source_name="BindingDB", status="ready", mode="managed_cache", record_count=2)
    write_source_state(layout, source_name="BindingDB", status="error", mode="live_api", notes="boom")

    state = json.loads((layout.source_state_dir / "bindingdb.json").read_text(encoding="utf-8"))
    assert state["status"] == "error"
    assert state["extra"]["attempt_count"] == 2
    assert state["extra"]["status_counts"]["ready"] == 1
    assert state["extra"]["status_counts"]["error"] == 1
    assert state["extra"]["total_records_observed"] == 2


def test_export_source_state_run_summary_reports_only_delta() -> None:
    layout = build_storage_layout(_tmp_dir("source_state_delta_summary"))

    write_source_state(layout, source_name="BindingDB", status="ready", mode="managed_cache", record_count=2)
    baseline = snapshot_source_state_counters(layout)

    write_source_state(layout, source_name="BindingDB", status="ready", mode="live_api", record_count=3)
    write_source_state(layout, source_name="ChEMBL", status="lookup_failed", mode="live_api")

    json_path, md_path, report = export_source_state_run_summary(
        layout,
        baseline=baseline,
        stage_name="extract",
    )

    assert json_path.exists()
    assert md_path.exists()
    assert report["status"] == "ready"
    assert report["source_count"] == 2
    assert report["total_attempt_count"] == 2
    assert report["total_records_observed"] == 3
    assert report["aggregate_status_counts"]["ready"] == 1
    assert report["aggregate_status_counts"]["lookup_failed"] == 1
    assert report["aggregate_mode_counts"]["live_api"] == 2
