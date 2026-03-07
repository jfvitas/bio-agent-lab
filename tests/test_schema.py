import pytest
from pydantic import ValidationError

from pbdata.schemas.canonical_sample import CanonicalBindingSample

_VALID_PROVENANCE = {"ingested_at": "2026-01-01T00:00:00+00:00"}


def _make(**kwargs) -> CanonicalBindingSample:
    """Build a minimal valid sample, with overrides."""
    defaults = dict(
        sample_id="x1",
        task_type="protein_ligand",
        source_database="RCSB",
        source_record_id="1abc",
        provenance=_VALID_PROVENANCE,
        quality_flags=[],
        quality_score=0.0,
    )
    defaults.update(kwargs)
    return CanonicalBindingSample(**defaults)


# ---------------------------------------------------------------------------
# Required fields
# ---------------------------------------------------------------------------

def test_canonical_binding_sample_has_required_fields() -> None:
    fields = CanonicalBindingSample.model_fields
    required = [
        "sample_id",
        "task_type",
        "source_database",
        "source_record_id",
        "provenance",
        "quality_flags",
        "quality_score",
    ]
    for name in required:
        assert name in fields


def test_required_fields_cannot_be_omitted() -> None:
    with pytest.raises(ValidationError):
        CanonicalBindingSample(
            sample_id="x1",
            task_type="protein_ligand",
            source_database="RCSB",
            source_record_id="1abc",
        )


def test_task_type_allowed_values() -> None:
    item = _make(task_type="protein_ligand")
    assert item.task_type == "protein_ligand"


# ---------------------------------------------------------------------------
# Immutability (frozen=True)
# ---------------------------------------------------------------------------

def test_record_is_immutable() -> None:
    item = _make()
    with pytest.raises(ValidationError):
        item.quality_score = 0.5  # type: ignore[misc]


# ---------------------------------------------------------------------------
# quality_score validator
# ---------------------------------------------------------------------------

def test_quality_score_valid_bounds() -> None:
    assert _make(quality_score=0.0).quality_score == 0.0
    assert _make(quality_score=1.0).quality_score == 1.0
    assert _make(quality_score=0.5).quality_score == 0.5


def test_quality_score_out_of_range() -> None:
    with pytest.raises(ValidationError):
        _make(quality_score=-0.1)
    with pytest.raises(ValidationError):
        _make(quality_score=1.1)


# ---------------------------------------------------------------------------
# ph validator
# ---------------------------------------------------------------------------

def test_ph_valid() -> None:
    assert _make(ph=7.0).ph == 7.0
    assert _make(ph=0.0).ph == 0.0
    assert _make(ph=14.0).ph == 14.0


def test_ph_out_of_range() -> None:
    with pytest.raises(ValidationError):
        _make(ph=-0.1)
    with pytest.raises(ValidationError):
        _make(ph=14.1)


def test_ph_none_is_allowed() -> None:
    assert _make(ph=None).ph is None


# ---------------------------------------------------------------------------
# structure_resolution validator
# ---------------------------------------------------------------------------

def test_resolution_positive() -> None:
    assert _make(structure_resolution=2.0).structure_resolution == 2.0


def test_resolution_non_positive_rejected() -> None:
    with pytest.raises(ValidationError):
        _make(structure_resolution=0.0)
    with pytest.raises(ValidationError):
        _make(structure_resolution=-1.5)


# ---------------------------------------------------------------------------
# temperature_c validator
# ---------------------------------------------------------------------------

def test_temperature_valid() -> None:
    assert _make(temperature_c=25.0).temperature_c == 25.0
    assert _make(temperature_c=-270.0).temperature_c == -270.0


def test_temperature_below_absolute_zero_rejected() -> None:
    with pytest.raises(ValidationError):
        _make(temperature_c=-273.15)
    with pytest.raises(ValidationError):
        _make(temperature_c=-300.0)


# ---------------------------------------------------------------------------
# ionic_strength validator
# ---------------------------------------------------------------------------

def test_ionic_strength_zero_allowed() -> None:
    assert _make(ionic_strength=0.0).ionic_strength == 0.0


def test_ionic_strength_negative_rejected() -> None:
    with pytest.raises(ValidationError):
        _make(ionic_strength=-0.1)


# ---------------------------------------------------------------------------
# provenance validator
# ---------------------------------------------------------------------------

def test_provenance_missing_ingested_at_rejected() -> None:
    with pytest.raises(ValidationError):
        _make(provenance={})
    with pytest.raises(ValidationError):
        _make(provenance={"source_database": "RCSB"})


def test_provenance_with_ingested_at_accepted() -> None:
    item = _make(provenance={"ingested_at": "2026-01-01T00:00:00+00:00"})
    assert "ingested_at" in item.provenance
