from pathlib import Path
from uuid import uuid4

import pytest

import json
from typer.testing import CliRunner

from pbdata.cli import app
from pbdata.cli import _pair_split_items_from_layout
from pbdata.dataset.splits import (
    PairSplitItem,
    build_grouped_pair_splits,
    build_pair_aware_splits,
    build_split_diagnostics,
    build_splits,
    build_temporal_pair_splits,
    cluster_aware_split,
)
from pbdata.gui import _call_on_tk_thread
from pbdata.sources import rcsb_search
from pbdata.storage import build_storage_layout

_LOCAL_TMP = Path(__file__).parent / "_tmp"
_LOCAL_TMP.mkdir(exist_ok=True)


def _tmp_dir(name: str) -> Path:
    path = _LOCAL_TMP / f"{uuid4().hex}_{name}"
    path.mkdir(exist_ok=True)
    return path


def test_cluster_aware_split_seed_changes_assignment_for_same_size_clusters() -> None:
    sample_ids = ["S1", "S2", "S3", "S4", "S5", "S6"]
    sequences = [
        "AAAAABBBBB",
        "CCCCCDDDDD",
        "EEEEEFFFFF",
        "GGGGGHHHHH",
        "IIIIIJJJJJ",
        "KKKKKLLLLL",
    ]

    result_a = cluster_aware_split(
        sample_ids,
        sequences,
        train_frac=0.5,
        val_frac=0.25,
        seed=1,
        log_fn=lambda _msg: None,
    )
    result_b = cluster_aware_split(
        sample_ids,
        sequences,
        train_frac=0.5,
        val_frac=0.25,
        seed=2,
        log_fn=lambda _msg: None,
    )

    assert result_a.sizes() == result_b.sizes()
    assert (
        result_a.train != result_b.train
        or result_a.val != result_b.val
        or result_a.test != result_b.test
    )


@pytest.mark.parametrize("train_frac,val_frac", [(-0.1, 0.2), (0.8, 0.3), (0.2, 1.1)])
def test_split_fraction_validation_rejects_invalid_values(
    train_frac: float,
    val_frac: float,
) -> None:
    with pytest.raises(ValueError):
        build_splits(["S1"], train_frac=train_frac, val_frac=val_frac)

    with pytest.raises(ValueError):
        cluster_aware_split(
            ["S1"],
            ["AAAAABBBBB"],
            train_frac=train_frac,
            val_frac=val_frac,
            log_fn=lambda _msg: None,
        )


def test_search_and_download_reports_missing_ids_from_partial_batch() -> None:
    output_dir = _tmp_dir("partial_batch_raw")
    manifest_path = _LOCAL_TMP / f"{uuid4().hex}_partial_manifest.csv"
    logs: list[str] = []

    entry = {
        "rcsb_id": "1ABC",
        "exptl": [{"method": "X-RAY DIFFRACTION"}],
        "rcsb_entry_info": {
            "resolution_combined": [2.1],
            "polymer_entity_count_protein": 1,
            "nonpolymer_entity_count": 0,
            "deposited_atom_count": 4321,
        },
        "rcsb_accession_info": {
            "initial_release_date": "2020-01-01T00:00:00Z",
            "deposit_date": "2019-06-01T00:00:00Z",
        },
        "struct": {"title": "Example protein"},
        "polymer_entities": [],
        "nonpolymer_entities": [],
    }

    original_search_entries = rcsb_search.search_entries
    original_fetch_entries_batch = rcsb_search.fetch_entries_batch
    try:
        rcsb_search.search_entries = lambda _criteria: ["1ABC", "2DEF"]
        rcsb_search.fetch_entries_batch = lambda _ids: [entry]
        downloaded = rcsb_search.search_and_download(
            rcsb_search.SearchCriteria(),
            output_dir,
            log_fn=logs.append,
            manifest_path=manifest_path,
        )
    finally:
        rcsb_search.search_entries = original_search_entries
        rcsb_search.fetch_entries_batch = original_fetch_entries_batch

    assert downloaded == ["1ABC"]
    assert not (output_dir / "2DEF.json").exists()
    assert any("missing IDs" in message and "2DEF" in message for message in logs)


def test_fetch_chemcomp_descriptors_logs_failures(caplog: pytest.LogCaptureFixture) -> None:
    from unittest.mock import patch

    caplog.set_level("WARNING")
    with patch("pbdata.sources.rcsb_search.requests.post", side_effect=RuntimeError("boom")):
        result = rcsb_search.fetch_chemcomp_descriptors(["ATP"])

    assert result == {}
    assert any("Chem-comp descriptor fetch failed" in rec.message for rec in caplog.records)


def test_call_on_tk_thread_returns_result() -> None:
    class RootStub:
        def after(self, _delay: int, fn) -> None:
            fn()

    assert _call_on_tk_thread(RootStub(), lambda: 7) == 7


def test_call_on_tk_thread_reraises_exceptions() -> None:
    class RootStub:
        def after(self, _delay: int, fn) -> None:
            fn()

    with pytest.raises(RuntimeError, match="ui failure"):
        _call_on_tk_thread(RootStub(), lambda: (_ for _ in ()).throw(RuntimeError("ui failure")))


def test_pair_aware_splits_keep_wildtype_and_mutant_together() -> None:
    items = [
        PairSplitItem(
            item_id="ex1",
            pair_identity_key="protein_ligand|1ABC|A|ATP|wt",
            affinity_type="Kd",
            receptor_sequence="M" * 250,
            receptor_identity="P12345",
            representation_key="protein_ligand|Kd|wildtype|has_sequence",
            hard_group_key="protein_ligand|P12345|ATP",
        ),
        PairSplitItem(
            item_id="ex2",
            pair_identity_key="protein_ligand|2DEF|A|ATP|A42V",
            affinity_type="Kd",
            receptor_sequence="M" * 249 + "A",
            receptor_identity="P12345",
            representation_key="protein_ligand|Kd|mutant|has_sequence",
            hard_group_key="protein_ligand|P12345|ATP",
        ),
        PairSplitItem(
            item_id="ex3",
            pair_identity_key="protein_ligand|3GHI|A|GTP|wt",
            affinity_type="Ki",
            receptor_sequence="Q" * 240,
            receptor_identity="Q99999",
            representation_key="protein_ligand|Ki|wildtype|has_sequence",
            hard_group_key="protein_ligand|Q99999|GTP",
        ),
    ]

    result, metadata = build_pair_aware_splits(
        items,
        train_frac=0.5,
        val_frac=0.25,
        seed=42,
        log_fn=lambda _msg: None,
    )

    placements = {
        "train": set(result.train),
        "val": set(result.val),
        "test": set(result.test),
    }
    assert any({"ex1", "ex2"}.issubset(ids) for ids in placements.values())
    assert metadata["hard_group_count"] == 2


def test_grouped_pair_splits_keep_shared_scaffold_together() -> None:
    items = [
        PairSplitItem(
            item_id="ex1",
            pair_identity_key="protein_ligand|1ABC|A|ATP|wt",
            affinity_type="Kd",
            receptor_sequence="M" * 220,
            receptor_identity="P12345",
            representation_key="protein_ligand|Kd|wildtype|has_sequence",
            hard_group_key="protein_ligand|P12345|ATP",
            scaffold_key="protein_ligand|ATP-KEY",
            family_key="protein_ligand|P12345",
            mutation_group_key="protein_ligand|P12345|ATP-KEY|wt",
        ),
        PairSplitItem(
            item_id="ex2",
            pair_identity_key="protein_ligand|2DEF|A|ATP|A42V",
            affinity_type="Kd",
            receptor_sequence="Q" * 210,
            receptor_identity="Q99999",
            representation_key="protein_ligand|Kd|mutant|has_sequence",
            hard_group_key="protein_ligand|Q99999|ATP",
            scaffold_key="protein_ligand|ATP-KEY",
            family_key="protein_ligand|Q99999",
            mutation_group_key="protein_ligand|Q99999|ATP-KEY|a42v",
        ),
        PairSplitItem(
            item_id="ex3",
            pair_identity_key="protein_ligand|3GHI|A|GTP|wt",
            affinity_type="Ki",
            receptor_sequence="R" * 200,
            receptor_identity="R11111",
            representation_key="protein_ligand|Ki|wildtype|has_sequence",
            hard_group_key="protein_ligand|R11111|GTP",
            scaffold_key="protein_ligand|GTP-KEY",
            family_key="protein_ligand|R11111",
            mutation_group_key="protein_ligand|R11111|GTP-KEY|wt",
        ),
    ]

    result, metadata = build_grouped_pair_splits(
        items,
        grouping="scaffold",
        train_frac=0.5,
        val_frac=0.25,
        seed=42,
        log_fn=lambda _msg: None,
    )

    placements = [set(result.train), set(result.val), set(result.test)]
    assert any({"ex1", "ex2"}.issubset(ids) for ids in placements)
    assert metadata["mode"] == "scaffold_grouped"
    assert metadata["group_count"] == 2


def test_grouped_pair_splits_keep_exact_mutation_series_together() -> None:
    items = [
        PairSplitItem(
            item_id="wt1",
            pair_identity_key="protein_ligand|1ABC|A|ATP|wt",
            affinity_type="Kd",
            receptor_sequence="M" * 220,
            receptor_identity="P12345",
            representation_key="protein_ligand|Kd|wildtype|has_sequence",
            hard_group_key="protein_ligand|P12345|ATP",
            scaffold_key="protein_ligand|ATP-KEY",
            family_key="protein_ligand|P12345",
            mutation_group_key="protein_ligand|P12345|ATP-KEY|wt",
        ),
        PairSplitItem(
            item_id="wt2",
            pair_identity_key="protein_ligand|2DEF|A|ATP|wt",
            affinity_type="Kd",
            receptor_sequence="M" * 219 + "A",
            receptor_identity="P12345",
            representation_key="protein_ligand|Kd|wildtype|has_sequence",
            hard_group_key="protein_ligand|P12345|ATP",
            scaffold_key="protein_ligand|ATP-KEY",
            family_key="protein_ligand|P12345",
            mutation_group_key="protein_ligand|P12345|ATP-KEY|wt",
        ),
        PairSplitItem(
            item_id="mut1",
            pair_identity_key="protein_ligand|3GHI|A|ATP|A42V",
            affinity_type="Kd",
            receptor_sequence="M" * 218 + "AA",
            receptor_identity="P12345",
            representation_key="protein_ligand|Kd|mutant|has_sequence",
            hard_group_key="protein_ligand|P12345|ATP",
            scaffold_key="protein_ligand|ATP-KEY",
            family_key="protein_ligand|P12345",
            mutation_group_key="protein_ligand|P12345|ATP-KEY|a42v",
        ),
    ]

    result, metadata = build_grouped_pair_splits(
        items,
        grouping="mutation",
        train_frac=0.5,
        val_frac=0.25,
        seed=42,
        log_fn=lambda _msg: None,
    )

    placements = [set(result.train), set(result.val), set(result.test)]
    assert any({"wt1", "wt2"}.issubset(ids) for ids in placements)
    assert metadata["mode"] == "mutation_grouped"


def test_grouped_pair_splits_keep_shared_source_together() -> None:
    items = [
        PairSplitItem(
            item_id="ex1",
            pair_identity_key="protein_ligand|1ABC|A|ATP|wt",
            affinity_type="Kd",
            receptor_sequence="M" * 220,
            receptor_identity="P12345",
            representation_key="protein_ligand|Kd|wildtype|has_sequence",
            hard_group_key="protein_ligand|P12345|ATP",
            source_group_key="protein_ligand|PDBbind",
        ),
        PairSplitItem(
            item_id="ex2",
            pair_identity_key="protein_ligand|2DEF|A|GTP|wt",
            affinity_type="Kd",
            receptor_sequence="Q" * 220,
            receptor_identity="Q99999",
            representation_key="protein_ligand|Kd|wildtype|has_sequence",
            hard_group_key="protein_ligand|Q99999|GTP",
            source_group_key="protein_ligand|PDBbind",
        ),
        PairSplitItem(
            item_id="ex3",
            pair_identity_key="protein_ligand|3GHI|A|LIG|wt",
            affinity_type="Kd",
            receptor_sequence="R" * 220,
            receptor_identity="R11111",
            representation_key="protein_ligand|Kd|wildtype|has_sequence",
            hard_group_key="protein_ligand|R11111|LIG",
            source_group_key="protein_ligand|BindingDB",
        ),
    ]

    result, metadata = build_grouped_pair_splits(
        items,
        grouping="source",
        train_frac=0.5,
        val_frac=0.25,
        seed=42,
        log_fn=lambda _msg: None,
    )

    placements = [set(result.train), set(result.val), set(result.test)]
    assert any({"ex1", "ex2"}.issubset(ids) for ids in placements)
    assert metadata["mode"] == "source_grouped"


def test_temporal_pair_splits_order_by_release_date() -> None:
    items = [
        PairSplitItem(item_id="late", pair_identity_key="k3", affinity_type="Kd", receptor_sequence=None, receptor_identity="P3", representation_key="r", hard_group_key="g3", release_date="2022-01-01"),
        PairSplitItem(item_id="early", pair_identity_key="k1", affinity_type="Kd", receptor_sequence=None, receptor_identity="P1", representation_key="r", hard_group_key="g1", release_date="2018-01-01"),
        PairSplitItem(item_id="mid", pair_identity_key="k2", affinity_type="Kd", receptor_sequence=None, receptor_identity="P2", representation_key="r", hard_group_key="g2", release_date="2020-01-01"),
    ]

    result, metadata = build_temporal_pair_splits(
        items,
        train_frac=0.34,
        val_frac=0.33,
        seed=42,
        log_fn=lambda _msg: None,
    )

    assert result.train == ["early"]
    assert result.val == []
    assert result.test == ["mid", "late"]
    assert metadata["mode"] == "time_ordered"


def test_split_diagnostics_include_metadata_overlap_channels() -> None:
    items = [
        PairSplitItem(
            item_id="ex1",
            pair_identity_key="protein_ligand|1ABC|A|ATP|wt",
            affinity_type="Kd",
            receptor_sequence="M" * 220,
            receptor_identity="P12345",
            representation_key="protein_ligand|Kd|wildtype|has_sequence",
            hard_group_key="protein_ligand|P12345|ATP",
            family_key="protein_ligand|IPR0001",
            domain_group_key="protein_ligand|IPR0001",
            pathway_group_key="protein_ligand|R-HSA-1",
            fold_group_key="protein_ligand|monomer",
        ),
        PairSplitItem(
            item_id="ex2",
            pair_identity_key="protein_ligand|2DEF|A|GTP|wt",
            affinity_type="Kd",
            receptor_sequence="Q" * 220,
            receptor_identity="Q99999",
            representation_key="protein_ligand|Kd|wildtype|has_sequence",
            hard_group_key="protein_ligand|Q99999|GTP",
            family_key="protein_ligand|IPR0002",
            domain_group_key="protein_ligand|IPR0002",
            pathway_group_key="protein_ligand|R-HSA-1",
            fold_group_key="protein_ligand|monomer",
        ),
    ]

    result = type("SplitResultStub", (), {"train": ["ex1"], "val": ["ex2"], "test": []})()
    diagnostics = build_split_diagnostics(items, result, strategy="pair_aware_grouped")

    assert diagnostics["counts"]["pathway_overlap_count"] == 1
    assert diagnostics["counts"]["domain_overlap_count"] == 0
    assert diagnostics["status"] == "dominance_risk"


def test_pair_split_items_prefer_metadata_family_keys() -> None:
    tmp_root = _tmp_dir("pair_split_metadata_keys")
    from pbdata.storage import build_storage_layout

    layout = build_storage_layout(tmp_root)
    extracted = tmp_root / "data" / "extracted"
    training = tmp_root / "data" / "training_examples"
    metadata = layout.workspace_metadata_dir
    for name in ["assays", "chains", "entry"]:
        (extracted / name).mkdir(parents=True, exist_ok=True)
    training.mkdir(parents=True, exist_ok=True)
    metadata.mkdir(parents=True, exist_ok=True)

    (extracted / "chains" / "1ABC.json").write_text(json.dumps([
        {"pdb_id": "1ABC", "chain_id": "A", "polymer_sequence": "M" * 220, "uniprot_id": "P12345"},
    ]), encoding="utf-8")
    (extracted / "entry" / "1ABC.json").write_text(json.dumps([
        {"pdb_id": "1ABC", "release_date": "2020-01-01", "oligomeric_state": "monomer"},
    ]), encoding="utf-8")
    (extracted / "assays" / "pairs.json").write_text(json.dumps([
        {"pdb_id": "1ABC", "pair_identity_key": "protein_ligand|1ABC|A|ATP|wt", "binding_affinity_type": "Kd"},
    ]), encoding="utf-8")
    (metadata / "protein_metadata.csv").write_text(
        "pdb_id,pair_identity_key,interpro_ids,pfam_ids,reactome_pathway_ids,structural_fold\n"
        "1ABC,protein_ligand|1ABC|A|ATP|wt,IPR0001,PF0001,R-HSA-1,monomer\n",
        encoding="utf-8",
    )

    items = _pair_split_items_from_layout(layout)

    assert len(items) == 1
    assert items[0].family_key == "protein_ligand|IPR0001"
    assert items[0].domain_group_key == "protein_ligand|IPR0001"
    assert items[0].pathway_group_key == "protein_ligand|R-HSA-1"
    assert items[0].fold_group_key == "protein_ligand|monomer"


def test_build_splits_auto_uses_pair_aware_training_examples() -> None:
    tmp_root = _tmp_dir("pair_aware_cli")
    extracted = tmp_root / "data" / "extracted"
    training = tmp_root / "data" / "training_examples"
    for name in ["assays", "chains"]:
        (extracted / name).mkdir(parents=True, exist_ok=True)
    training.mkdir(parents=True, exist_ok=True)

    (extracted / "chains" / "1ABC.json").write_text(json.dumps([
        {"pdb_id": "1ABC", "chain_id": "A", "polymer_sequence": "M" * 220, "uniprot_id": "P12345"},
        {"pdb_id": "2DEF", "chain_id": "A", "polymer_sequence": "M" * 219 + "A", "uniprot_id": "P12345"},
    ]), encoding="utf-8")
    (extracted / "assays" / "pairs.json").write_text(json.dumps([
        {"pdb_id": "1ABC", "pair_identity_key": "protein_ligand|1ABC|A|ATP|wt", "binding_affinity_type": "Kd"},
        {"pdb_id": "2DEF", "pair_identity_key": "protein_ligand|2DEF|A|ATP|A42V", "binding_affinity_type": "Kd"},
    ]), encoding="utf-8")
    (training / "training_examples.json").write_text(json.dumps([
        {"example_id": "ex1", "provenance": {"pair_identity_key": "protein_ligand|1ABC|A|ATP|wt"}, "labels": {"affinity_type": "Kd"}},
        {"example_id": "ex2", "provenance": {"pair_identity_key": "protein_ligand|2DEF|A|ATP|A42V"}, "labels": {"affinity_type": "Kd"}},
    ]), encoding="utf-8")

    runner = CliRunner()
    result = runner.invoke(
        app,
        ["--storage-root", str(tmp_root), "build-splits"],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    metadata = json.loads((tmp_root / "data" / "splits" / "metadata.json").read_text(encoding="utf-8"))
    assert metadata["strategy"] == "pair_aware_grouped"
    diagnostics = json.loads((tmp_root / "data" / "splits" / "split_diagnostics.json").read_text(encoding="utf-8"))
    assert diagnostics["strategy"] == "pair_aware_grouped"
    assert diagnostics["counts"]["hard_group_overlap_count"] == 0
    train_ids = set((tmp_root / "data" / "splits" / "train.txt").read_text(encoding="utf-8").split())
    val_ids = set((tmp_root / "data" / "splits" / "val.txt").read_text(encoding="utf-8").split())
    test_ids = set((tmp_root / "data" / "splits" / "test.txt").read_text(encoding="utf-8").split())
    assert any({"ex1", "ex2"}.issubset(ids) for ids in (train_ids, val_ids, test_ids))


def test_build_splits_scaffold_mode_uses_training_example_ligand_proxy() -> None:
    tmp_root = _tmp_dir("scaffold_split_cli")
    extracted = tmp_root / "data" / "extracted"
    training = tmp_root / "data" / "training_examples"
    for name in ["assays", "chains"]:
        (extracted / name).mkdir(parents=True, exist_ok=True)
    training.mkdir(parents=True, exist_ok=True)

    (extracted / "chains" / "1ABC.json").write_text(json.dumps([
        {"pdb_id": "1ABC", "chain_id": "A", "polymer_sequence": "M" * 220, "uniprot_id": "P12345"},
        {"pdb_id": "2DEF", "chain_id": "A", "polymer_sequence": "Q" * 220, "uniprot_id": "Q99999"},
        {"pdb_id": "3GHI", "chain_id": "A", "polymer_sequence": "R" * 220, "uniprot_id": "R11111"},
    ]), encoding="utf-8")
    (extracted / "assays" / "pairs.json").write_text(json.dumps([
        {"pdb_id": "1ABC", "pair_identity_key": "protein_ligand|1ABC|A|ATP|wt", "binding_affinity_type": "Kd"},
        {"pdb_id": "2DEF", "pair_identity_key": "protein_ligand|2DEF|A|ATP|A42V", "binding_affinity_type": "Kd"},
        {"pdb_id": "3GHI", "pair_identity_key": "protein_ligand|3GHI|A|GTP|wt", "binding_affinity_type": "Kd"},
    ]), encoding="utf-8")
    (training / "training_examples.json").write_text(json.dumps([
        {
            "example_id": "ex1",
            "provenance": {"pair_identity_key": "protein_ligand|1ABC|A|ATP|wt"},
            "labels": {"affinity_type": "Kd"},
            "ligand": {"inchikey": "ATP-KEY", "smiles": "ATP-SMILES", "ligand_id": "ATP"},
        },
        {
            "example_id": "ex2",
            "provenance": {"pair_identity_key": "protein_ligand|2DEF|A|ATP|A42V"},
            "labels": {"affinity_type": "Kd"},
            "ligand": {"inchikey": "ATP-KEY", "smiles": "ATP-SMILES", "ligand_id": "ATP"},
        },
        {
            "example_id": "ex3",
            "provenance": {"pair_identity_key": "protein_ligand|3GHI|A|GTP|wt"},
            "labels": {"affinity_type": "Kd"},
            "ligand": {"inchikey": "GTP-KEY", "smiles": "GTP-SMILES", "ligand_id": "GTP"},
        },
    ]), encoding="utf-8")

    runner = CliRunner()
    result = runner.invoke(
        app,
        ["--storage-root", str(tmp_root), "build-splits", "--split-mode", "scaffold"],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    metadata = json.loads((tmp_root / "data" / "splits" / "metadata.json").read_text(encoding="utf-8"))
    diagnostics = json.loads((tmp_root / "data" / "splits" / "split_diagnostics.json").read_text(encoding="utf-8"))
    assert metadata["strategy"] == "scaffold_grouped"
    assert metadata["mode"] == "scaffold_grouped"
    assert diagnostics["strategy"] == "scaffold_grouped"
    assert diagnostics["status"] in {"ready", "attention_needed", "dominance_risk"}
    placements = [
        set((tmp_root / "data" / "splits" / f"{name}.txt").read_text(encoding="utf-8").split())
        for name in ("train", "val", "test")
    ]
    assert any({"ex1", "ex2"}.issubset(ids) for ids in placements)


def test_build_splits_source_and_time_modes() -> None:
    tmp_root = _tmp_dir("source_time_split_cli")
    extracted = tmp_root / "data" / "extracted"
    training = tmp_root / "data" / "training_examples"
    for name in ["assays", "chains", "entry"]:
        (extracted / name).mkdir(parents=True, exist_ok=True)
    training.mkdir(parents=True, exist_ok=True)

    (extracted / "chains" / "1ABC.json").write_text(json.dumps([
        {"pdb_id": "1ABC", "chain_id": "A", "polymer_sequence": "M" * 220, "uniprot_id": "P12345"},
        {"pdb_id": "2DEF", "chain_id": "A", "polymer_sequence": "Q" * 220, "uniprot_id": "Q99999"},
    ]), encoding="utf-8")
    (extracted / "entry" / "entries.json").write_text(json.dumps([
        {"pdb_id": "1ABC", "release_date": "2018-01-01"},
        {"pdb_id": "2DEF", "release_date": "2022-01-01"},
    ]), encoding="utf-8")
    (extracted / "assays" / "pairs.json").write_text(json.dumps([
        {"pdb_id": "1ABC", "pair_identity_key": "protein_ligand|1ABC|A|ATP|wt", "binding_affinity_type": "Kd", "source_database": "PDBbind", "selected_preferred_source": "PDBbind"},
        {"pdb_id": "2DEF", "pair_identity_key": "protein_ligand|2DEF|A|ATP|A42V", "binding_affinity_type": "Kd", "source_database": "BindingDB", "selected_preferred_source": "BindingDB"},
    ]), encoding="utf-8")
    (training / "training_examples.json").write_text(json.dumps([
        {
            "example_id": "ex1",
            "provenance": {"pair_identity_key": "protein_ligand|1ABC|A|ATP|wt"},
            "labels": {"affinity_type": "Kd", "preferred_source_database": "PDBbind"},
            "ligand": {"inchikey": "ATP-KEY", "smiles": "ATP-SMILES", "ligand_id": "ATP"},
        },
        {
            "example_id": "ex2",
            "provenance": {"pair_identity_key": "protein_ligand|2DEF|A|ATP|A42V"},
            "labels": {"affinity_type": "Kd", "preferred_source_database": "BindingDB"},
            "ligand": {"inchikey": "ATP-KEY", "smiles": "ATP-SMILES", "ligand_id": "ATP"},
        },
    ]), encoding="utf-8")

    runner = CliRunner()
    source_result = runner.invoke(
        app,
        ["--storage-root", str(tmp_root), "build-splits", "--split-mode", "source"],
        catch_exceptions=False,
    )
    assert source_result.exit_code == 0
    source_metadata = json.loads((tmp_root / "data" / "splits" / "metadata.json").read_text(encoding="utf-8"))
    source_diagnostics = json.loads((tmp_root / "data" / "splits" / "split_diagnostics.json").read_text(encoding="utf-8"))
    assert source_metadata["strategy"] == "source_grouped"
    assert source_diagnostics["strategy"] == "source_grouped"

    time_result = runner.invoke(
        app,
        ["--storage-root", str(tmp_root), "build-splits", "--split-mode", "time"],
        catch_exceptions=False,
    )
    assert time_result.exit_code == 0
    time_metadata = json.loads((tmp_root / "data" / "splits" / "metadata.json").read_text(encoding="utf-8"))
    time_diagnostics = json.loads((tmp_root / "data" / "splits" / "split_diagnostics.json").read_text(encoding="utf-8"))
    assert time_metadata["strategy"] == "time_ordered"
    assert time_diagnostics["strategy"] == "time_ordered"


def test_build_splits_uses_existing_artifact_tables_when_extracted_tables_are_unavailable() -> None:
    tmp_root = _tmp_dir("artifact_split_cli")
    layout = build_storage_layout(tmp_root)
    layout.training_dir.mkdir(parents=True, exist_ok=True)
    layout.workspace_metadata_dir.mkdir(parents=True, exist_ok=True)

    (tmp_root / "master_pdb_repository.csv").write_text(
        "pdb_id,release_date\n"
        "1ABC,2018-01-01\n"
        "2DEF,2022-01-01\n",
        encoding="utf-8",
    )
    (tmp_root / "model_ready_pairs.csv").write_text(
        "pdb_id,pair_identity_key,source_database,binding_affinity_type,selected_preferred_source,receptor_chain_ids,receptor_uniprot_ids,ligand_key\n"
        "1ABC,protein_ligand|1ABC|A|ATP|wt,PDBbind,Kd,PDBbind,A,P12345,ATP\n"
        "2DEF,protein_ligand|2DEF|A|GTP|A42V,BindingDB,Kd,BindingDB,A,Q99999,GTP\n",
        encoding="utf-8",
    )
    (layout.training_dir / "training_examples.json").write_text(
        json.dumps(
            [
                {
                    "example_id": "ex1",
                    "provenance": {"pair_identity_key": "protein_ligand|1ABC|A|ATP|wt"},
                    "labels": {"affinity_type": "Kd", "preferred_source_database": "PDBbind"},
                    "ligand": {"inchikey": "ATP-KEY", "ligand_id": "ATP"},
                },
                {
                    "example_id": "ex2",
                    "provenance": {"pair_identity_key": "protein_ligand|2DEF|A|GTP|A42V"},
                    "labels": {"affinity_type": "Kd", "preferred_source_database": "BindingDB"},
                    "ligand": {"inchikey": "GTP-KEY", "ligand_id": "GTP"},
                },
            ]
        ),
        encoding="utf-8",
    )
    (layout.workspace_metadata_dir / "protein_metadata.csv").write_text(
        "pdb_id,pair_identity_key,interpro_ids,reactome_pathway_ids,structural_fold\n"
        "1ABC,protein_ligand|1ABC|A|ATP|wt,IPR0001,R-HSA-1,foldA\n"
        "2DEF,protein_ligand|2DEF|A|GTP|A42V,IPR0002,R-HSA-2,foldB\n",
        encoding="utf-8",
    )

    runner = CliRunner()
    result = runner.invoke(
        app,
        ["--storage-root", str(tmp_root), "build-splits"],
        catch_exceptions=False,
    )

    assert result.exit_code == 0
    metadata = json.loads((tmp_root / "data" / "splits" / "metadata.json").read_text(encoding="utf-8"))
    assert metadata["strategy"] == "pair_aware_grouped"
    written_ids = {
        split_name: set((tmp_root / "data" / "splits" / f"{split_name}.txt").read_text(encoding="utf-8").split())
        for split_name in ("train", "val", "test")
    }
    assert sum(len(ids) for ids in written_ids.values()) == 2
    assert {"ex1", "ex2"} == set().union(*written_ids.values())
