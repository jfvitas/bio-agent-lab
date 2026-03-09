from pbdata.pipeline.assay_merge import merge_assay_samples, pair_identity_key
from pbdata.schemas.canonical_sample import CanonicalBindingSample


def _sample(
    sample_id: str,
    *,
    assay_type: str = "Kd",
    assay_value: float = 5.0,
    assay_value_standardized: float = 5.0,
    assay_value_log10: float = 0.69897,
    mutation_string: str | None = None,
    source_database: str = "BindingDB",
) -> CanonicalBindingSample:
    return CanonicalBindingSample(
        sample_id=sample_id,
        task_type="protein_ligand",
        source_database=source_database,
        source_record_id=sample_id,
        pdb_id="1ABC",
        chain_ids_receptor=["A"],
        ligand_id="ATP",
        assay_type=assay_type,
        assay_value=assay_value,
        assay_unit="nM",
        assay_value_standardized=assay_value_standardized,
        assay_value_log10=assay_value_log10,
        mutation_string=mutation_string,
        provenance={"ingested_at": "2026-03-08T00:00:00+00:00"},
        quality_flags=[],
        quality_score=0.0,
    )


def test_merge_assay_samples_adds_pair_summary_without_collapsing_rows() -> None:
    rows = merge_assay_samples([
        _sample("a", assay_value=5.0, assay_value_standardized=5.0, assay_value_log10=0.69897),
        _sample("b", assay_value=7.0, assay_value_standardized=7.0, assay_value_log10=0.845098),
    ])

    assert len(rows) == 2
    assert all(row.reported_measurement_count == 2 for row in rows)
    assert all(row.reported_measurement_mean_log10_standardized == 0.772034 for row in rows)
    assert all("BindingDB:Kd=" in (row.reported_measurements_text or "") for row in rows)


def test_pair_identity_key_keeps_mutants_separate() -> None:
    wt = _sample("wt")
    mutant = _sample("mut", mutation_string="A42V")

    assert pair_identity_key(wt) != pair_identity_key(mutant)

    rows = merge_assay_samples([wt, mutant])
    assert all(row.reported_measurement_count == 1 for row in rows)


def test_merge_assay_samples_does_not_mix_assay_types() -> None:
    kd = _sample("kd", assay_type="Kd", assay_value=5.0, assay_value_log10=0.69897)
    ki = _sample("ki", assay_type="Ki", assay_value=9.0, assay_value_log10=0.954243, source_database="PDBbind")

    rows = merge_assay_samples([kd, ki])

    assert len(rows) == 2
    by_type = {row.binding_affinity_type: row for row in rows}
    assert by_type["Kd"].reported_measurement_count == 1
    assert by_type["Ki"].reported_measurement_count == 1


def test_merge_assay_samples_attaches_field_provenance_and_confidence() -> None:
    rows = merge_assay_samples([
        _sample("a", assay_value=5.0, assay_value_standardized=5.0, assay_value_log10=0.69897),
        _sample("b", assay_value=7.0, assay_value_standardized=7.0, assay_value_log10=0.845098),
    ])

    row = rows[0]
    assert row.field_provenance is not None
    assert row.field_confidence is not None
    assert row.field_provenance["binding_affinity_value"]["source"] == "BindingDB"
    assert row.field_provenance["reported_measurement_mean_log10_standardized"]["method"] == "pair_group_summary_mean_log10"
    assert row.field_confidence["reported_measurement_count"] == "high"


def test_merge_assay_samples_marks_conflicts_and_preferred_source() -> None:
    rows = merge_assay_samples([
        _sample("a", assay_value=5.0, assay_value_standardized=5.0, assay_value_log10=0.69897, source_database="PDBbind"),
        _sample("b", assay_value=500.0, assay_value_standardized=500.0, assay_value_log10=2.69897, source_database="BindingDB"),
    ])

    row = rows[0]
    assert row.source_conflict_flag is True
    assert row.source_conflict_summary is not None
    assert row.source_agreement_band == "low"
    assert row.selected_preferred_source == "PDBbind"
    assert row.field_provenance is not None
    assert row.field_provenance["selected_preferred_source"]["method"] == "explicit_source_priority_policy"
