"""Tests for the multi-table extraction pipeline."""

import os
from pathlib import Path
from uuid import uuid4
from unittest.mock import patch

from typer.testing import CliRunner

from pbdata.cli import app
from pbdata.gui import _SUBPROCESS_STAGES
from pbdata.pipeline.assay_merge import pair_identity_key
from pbdata.pipeline.extract import extract_rcsb_entry
from pbdata.schemas.canonical_sample import CanonicalBindingSample
from pbdata.schemas.records import (
    AssayRecord,
    BoundObjectRecord,
    ChainRecord,
    EntryRecord,
    InterfaceRecord,
    ProvenanceRecord,
)

_LOCAL_TMP = Path(__file__).parent / "_tmp"
_LOCAL_TMP.mkdir(exist_ok=True)


def _tmp_dir(name: str) -> Path:
    path = _LOCAL_TMP / f"{uuid4().hex}_{name}"
    path.mkdir(exist_ok=True)
    return path


def _mock_entry(pdb_id="1ABC"):
    """Minimal RCSB GraphQL entry dict for testing."""
    return {
        "rcsb_id": pdb_id,
        "struct": {"title": "Test structure"},
        "struct_keywords": {"pdbx_keywords": "HYDROLASE", "text": "hydrolase"},
        "exptl": [{"method": "X-RAY DIFFRACTION"}],
        "rcsb_entry_info": {
            "resolution_combined": [2.0],
            "polymer_entity_count_protein": 1,
            "nonpolymer_entity_count": 1,
            "deposited_atom_count": 5000,
            "assembly_count": 1,
        },
        "rcsb_accession_info": {
            "initial_release_date": "2020-01-15",
            "deposit_date": "2019-06-01",
        },
        "polymer_entities": [
            {
                "rcsb_id": f"{pdb_id}_1",
                "entity_poly": {
                    "pdbx_seq_one_letter_code_can": "MKTVRQERLKSIVRILERSKEPVSGAQ" * 4,
                    "type": "polypeptide(L)",
                },
                "rcsb_polymer_entity": {"pdbx_description": "Test protein"},
                "rcsb_polymer_entity_container_identifiers": {
                    "auth_asym_ids": ["A"],
                    "uniprot_ids": ["P12345"],
                },
                "rcsb_entity_source_organism": [
                    {"ncbi_taxonomy_id": 9606, "ncbi_scientific_name": "Homo sapiens"},
                ],
            },
        ],
        "nonpolymer_entities": [
            {
                "rcsb_id": f"{pdb_id}_2",
                "nonpolymer_comp": {
                    "chem_comp": {"id": "ATP", "name": "ADENOSINE-5'-TRIPHOSPHATE"},
                },
                "rcsb_nonpolymer_entity_container_identifiers": {
                    "auth_asym_ids": ["B"],
                },
            },
        ],
        "assemblies": [
            {
                "rcsb_id": f"{pdb_id}-1",
                "pdbx_struct_assembly": {
                    "oligomeric_details": "MONOMERIC",
                    "oligomeric_count": "1",
                },
                "rcsb_assembly_info": {
                    "polymer_entity_count": 1,
                    "polymer_entity_count_protein": 1,
                },
            },
        ],
    }


@patch("pbdata.pipeline.extract.download_structure_files")
def test_extract_produces_all_tables(mock_download):
    """extract_rcsb_entry returns all six table types."""
    mock_download.return_value = {
        "structure_file_cif_path": "data/structures/rcsb/1ABC.cif",
        "structure_file_cif_size_bytes": 123456,
        "structure_file_hash_sha256": "abc123",
        "structure_download_url": "https://files.rcsb.org/download/1ABC.cif",
        "structure_downloaded_at": "2024-01-01T00:00:00+00:00",
        "parsed_structure_format": "mmCIF",
    }

    result = extract_rcsb_entry(_mock_entry("1ABC"))

    assert isinstance(result["entry"], EntryRecord)
    assert isinstance(result["chains"], list)
    assert isinstance(result["bound_objects"], list)
    assert isinstance(result["interfaces"], list)
    assert isinstance(result["assays"], list)
    assert isinstance(result["provenance"], list)


@patch("pbdata.pipeline.extract.download_structure_files")
def test_entry_record_fields(mock_download):
    """EntryRecord has correct entry-level fields populated."""
    mock_download.return_value = {
        "parsed_structure_format": "mmCIF",
    }

    result = extract_rcsb_entry(_mock_entry("4HHB"))
    entry = result["entry"]

    assert entry.pdb_id == "4HHB"
    assert entry.source_database == "RCSB"
    assert entry.title == "Test structure"
    assert entry.experimental_method == "X-RAY DIFFRACTION"
    assert entry.structure_resolution == 2.0
    assert entry.release_date == "2020-01-15"
    assert entry.deposit_date == "2019-06-01"
    assert entry.deposited_atom_count == 5000
    assert entry.parsed_structure_format == "mmCIF"
    assert entry.resolution_bin == "medium_res_1.5-2.5"
    assert entry.membrane_vs_soluble == "soluble"


@patch("pbdata.pipeline.extract.download_structure_files")
def test_chain_records(mock_download):
    """ChainRecords correctly map polymer entities to chains."""
    mock_download.return_value = {}

    result = extract_rcsb_entry(_mock_entry("1ABC"))
    chains = result["chains"]

    assert len(chains) >= 1
    chain = chains[0]
    assert isinstance(chain, ChainRecord)
    assert chain.pdb_id == "1ABC"
    assert chain.chain_id == "A"
    assert chain.is_protein is True
    assert chain.polymer_subtype == "protein"
    assert chain.uniprot_id == "P12345"
    assert chain.entity_source_organism == "Homo sapiens"
    assert chain.entity_source_taxonomy_id == 9606


@patch("pbdata.pipeline.extract.download_structure_files")
def test_bound_object_records(mock_download):
    """BoundObjectRecords have correct component type/role mapping."""
    mock_download.return_value = {}

    result = extract_rcsb_entry(_mock_entry("1ABC"))
    bos = result["bound_objects"]

    assert len(bos) >= 1
    atp = next((b for b in bos if b.component_id == "ATP"), None)
    assert atp is not None
    assert isinstance(atp, BoundObjectRecord)
    assert atp.component_type == "cofactor"


@patch("pbdata.pipeline.extract.download_structure_files")
def test_provenance_records(mock_download):
    """Provenance records are created for key extraction steps."""
    mock_download.return_value = {
        "structure_file_cif_path": "test.cif",
        "structure_download_url": "https://example.com/test.cif",
        "structure_downloaded_at": "2024-01-01",
    }

    result = extract_rcsb_entry(_mock_entry("1ABC"))
    provs = result["provenance"]

    assert len(provs) >= 2
    assert all(isinstance(p, ProvenanceRecord) for p in provs)
    sources = [p.source_name for p in provs]
    assert "RCSB" in sources
    assert "pbdata_rcsb_classify" in sources


@patch("pbdata.pipeline.extract.download_structure_files")
def test_file_provenance_on_entry(mock_download):
    """Entry record carries structure file provenance fields."""
    mock_download.return_value = {
        "structure_file_cif_path": "data/structures/rcsb/2XYZ.cif",
        "structure_file_cif_size_bytes": 999,
        "structure_file_hash_sha256": "deadbeef",
        "structure_download_url": "https://files.rcsb.org/download/2XYZ.cif",
        "structure_downloaded_at": "2024-06-01T12:00:00+00:00",
        "parsed_structure_format": "mmCIF",
    }

    result = extract_rcsb_entry(_mock_entry("2XYZ"))
    entry = result["entry"]

    assert entry.structure_file_cif_path == "data/structures/rcsb/2XYZ.cif"
    assert entry.structure_file_cif_size_bytes == 999
    assert entry.structure_file_hash_sha256 == "deadbeef"
    assert entry.parsed_structure_format == "mmCIF"


@patch("pbdata.pipeline.extract.download_structure_files")
def test_quality_flags_populated(mock_download):
    """Quality flags are populated based on bound object types."""
    mock_download.return_value = {}

    result = extract_rcsb_entry(_mock_entry("1ABC"))
    entry = result["entry"]

    # ATP is a cofactor
    assert entry.cofactor_present is True
    assert "cofactor_present" in (entry.quality_flags or [])


@patch("pbdata.pipeline.extract.download_structure_files")
def test_extract_can_skip_structure_downloads(mock_download):
    """extract_rcsb_entry should not fetch structure files when disabled."""
    result = extract_rcsb_entry(_mock_entry("1ABC"), download_structures=False)

    mock_download.assert_not_called()
    entry = result["entry"]
    assert entry.structure_file_cif_path is None
    assert entry.parsed_structure_format is None


def test_gui_pipeline_includes_extract_stage() -> None:
    assert _SUBPROCESS_STAGES[0] == "extract"
    assert "extract" in _SUBPROCESS_STAGES


def test_cli_extract_no_download_structures_passes_flag() -> None:
    tmp_root = _tmp_dir("extract_cli")
    raw_dir = tmp_root / "data" / "raw" / "rcsb"
    raw_dir.mkdir(parents=True)
    (raw_dir / "1ABC.json").write_text('{"rcsb_id":"1ABC","nonpolymer_entities":[]}', encoding="utf-8")

    captured: list[bool] = []

    def _fake_extract_rcsb_entry(*_args, **kwargs):
        captured.append(kwargs["download_structures"])
        return {
            "entry": EntryRecord(source_database="RCSB", source_record_id="1ABC", pdb_id="1ABC"),
            "chains": [],
            "bound_objects": [],
            "interfaces": [],
            "assays": [],
            "provenance": [],
        }

    runner = CliRunner()
    original_cwd = Path.cwd()
    os.chdir(tmp_root)
    try:
        with patch(
            "pbdata.cli._load_external_assay_samples", return_value={}
        ), patch(
            "pbdata.pipeline.extract.extract_rcsb_entry", side_effect=_fake_extract_rcsb_entry
        ), patch("pbdata.pipeline.extract.write_records_json", return_value=None):
            result = runner.invoke(app, ["extract", "--no-download-structures"], catch_exceptions=False)
    finally:
        os.chdir(original_cwd)

    assert result.exit_code == 0
    assert captured == [False]


@patch("pbdata.pipeline.extract.download_structure_files")
def test_extract_merges_external_assay_samples(mock_download) -> None:
    mock_download.return_value = {}
    sample = CanonicalBindingSample(
        sample_id="PDBBIND_1ABC_Kd",
        task_type="protein_ligand",
        source_database="PDBbind",
        source_record_id="1ABC",
        pdb_id="1ABC",
        chain_ids_receptor=["A"],
        ligand_id="ATP",
        assay_type="Kd",
        assay_value=5.0,
        assay_unit="nM",
        assay_value_standardized=5.0,
        assay_value_log10=0.69897,
        provenance={"ingested_at": "2026-03-08T00:00:00+00:00"},
        quality_flags=[],
        quality_score=0.0,
    )

    result = extract_rcsb_entry(_mock_entry("1ABC"), assay_samples=[sample])
    assays = result["assays"]

    assert len(assays) == 1
    assay = assays[0]
    assert isinstance(assay, AssayRecord)
    assert assay.pair_identity_key == pair_identity_key(sample)
    assert assay.binding_affinity_type == "Kd"
    assert assay.reported_measurement_count == 1


@patch("pbdata.pipeline.extract.download_structure_files")
def test_extract_adds_biolip_binding_site_interfaces(mock_download) -> None:
    mock_download.return_value = {}
    sample = CanonicalBindingSample(
        sample_id="BIOLIP_1ABC_A_401",
        task_type="protein_ligand",
        source_database="BioLiP",
        source_record_id="1ABC:A:B:ATP",
        pdb_id="1ABC",
        chain_ids_receptor=["A"],
        ligand_id="ATP",
        provenance={
            "ingested_at": "2026-03-08T00:00:00+00:00",
            "binding_site_residue_ids": ["TYR15", "ASP34"],
            "binding_site_residue_names": ["TYR", "ASP"],
        },
        quality_flags=[],
        quality_score=0.0,
    )

    result = extract_rcsb_entry(_mock_entry("1ABC"), assay_samples=[sample])
    interfaces = result["interfaces"]
    biolip_iface = next(
        iface for iface in interfaces
        if iface.binding_site_residue_ids == ["TYR15", "ASP34"]
    )
    assert biolip_iface.interface_type == "protein_ligand"
    assert biolip_iface.binding_site_chain_ids == ["A"]
