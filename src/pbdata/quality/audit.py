"""Quality audit logic for CanonicalBindingSample records.

Computes quality_flags and quality_score for each record.
Flags are informational — they do not remove records from the dataset.
"""

from __future__ import annotations

from pbdata.schemas.canonical_sample import CanonicalBindingSample

# Resolution thresholds (Angstroms)
_RES_LOW      = 3.5
_RES_VERY_LOW = 4.5


def compute_flags(record: CanonicalBindingSample) -> list[str]:
    """Return a list of quality flag strings for a record."""
    flags: list[str] = []

    # Resolution
    if record.structure_resolution is None:
        flags.append("no_resolution")
    elif record.structure_resolution > _RES_VERY_LOW:
        flags.append("very_low_resolution")
    elif record.structure_resolution > _RES_LOW:
        flags.append("low_resolution")

    # Experimental method
    if record.experimental_method is None:
        flags.append("no_experimental_method")

    # Sequence coverage
    if record.sequence_receptor is None:
        flags.append("missing_sequence_receptor")

    if record.task_type == "protein_protein" and record.sequence_partner is None:
        flags.append("missing_sequence_partner")

    # Identifier coverage
    if not record.uniprot_ids:
        flags.append("no_uniprot_id")

    if not record.chain_ids_receptor:
        flags.append("no_chain_ids")

    # Ligand coverage
    if record.task_type == "protein_ligand" and record.ligand_id is None:
        flags.append("missing_ligand_id")

    return flags


def compute_score(record: CanonicalBindingSample) -> float:
    """Return a quality score in [0.0, 1.0] based on field coverage.

    Each check contributes equally.  Checks are task-type-aware so that
    a protein_ligand record is not penalised for missing sequence_partner.
    """
    checks = [
        record.experimental_method is not None,
        record.structure_resolution is not None
            and record.structure_resolution <= _RES_LOW,
        record.sequence_receptor is not None,
        bool(record.chain_ids_receptor),
        bool(record.uniprot_ids),
        bool(record.taxonomy_ids),
        # Task-type-specific checks
        record.task_type != "protein_ligand" or record.ligand_id is not None,
        record.task_type != "protein_protein" or record.sequence_partner is not None,
    ]
    return round(sum(checks) / len(checks), 4)


def audit_record(record: CanonicalBindingSample) -> CanonicalBindingSample:
    """Return a new (frozen) record with updated quality_flags and quality_score."""
    return record.model_copy(update={
        "quality_flags": compute_flags(record),
        "quality_score": compute_score(record),
    })
