"""Helpers for converting external assay samples into extraction-layer records.

The merge logic is intentionally conservative:
- keep one output row per measurement
- do not merge distinct assay labels
- do not merge wildtype and mutant measurements
- do not merge different mutation strings into one pair summary
"""

from __future__ import annotations

import statistics
from collections import defaultdict

from pbdata.schemas.canonical_sample import CanonicalBindingSample
from pbdata.schemas.records import AssayRecord


def _chain_group_key(chains: list[str] | None) -> str:
    if not chains:
        return "-"
    return ",".join(sorted({c.strip() for c in chains if c and c.strip()}))


def pair_identity_key(sample: CanonicalBindingSample) -> str:
    """Return a deterministic key for one biological interaction pair."""
    provenance = sample.provenance or {}
    override = provenance.get("pair_grouping_override")
    if override:
        return str(override)

    mutation_key = sample.mutation_string or sample.wildtype_or_mutant or "wt_or_unspecified"

    if sample.task_type == "protein_ligand":
        ligand_key = sample.ligand_inchi_key or sample.ligand_id or sample.ligand_smiles or "unknown_ligand"
        return "|".join([
            sample.task_type,
            sample.pdb_id or "-",
            _chain_group_key(sample.chain_ids_receptor),
            ligand_key,
            mutation_key,
        ])

    if sample.task_type == "protein_protein":
        groups = sorted([
            _chain_group_key(sample.chain_ids_receptor),
            _chain_group_key(sample.chain_ids_partner),
        ])
        return "|".join([
            sample.task_type,
            sample.pdb_id or "-",
            groups[0],
            groups[1],
            mutation_key,
        ])

    return "|".join([
        sample.task_type,
        sample.pdb_id or "-",
        sample.source_record_id,
        mutation_key,
    ])


def canonical_sample_to_assay_record(sample: CanonicalBindingSample) -> AssayRecord:
    assay_type = sample.assay_type
    delta_g = sample.assay_value if assay_type == "dG" else None
    delta_delta_g = sample.assay_value if assay_type == "ddG" else None
    provenance = sample.provenance or {}
    return AssayRecord(
        pdb_id=sample.pdb_id,
        source_database=sample.source_database,
        pair_identity_key=pair_identity_key(sample),
        binding_affinity_type=sample.assay_type,
        binding_affinity_value=sample.assay_value,
        binding_affinity_unit=sample.assay_unit,
        binding_affinity_log10_standardized=sample.assay_value_log10,
        binding_affinity_relation=provenance.get("standard_relation"),
        binding_affinity_is_mutant_measurement=(sample.wildtype_or_mutant == "mutant"),
        delta_g=delta_g,
        delta_delta_g=delta_delta_g,
        assay_temperature_c=sample.temperature_c,
        assay_ph=sample.ph,
        assay_buffer=sample.buffer,
        assay_ionic_strength=sample.ionic_strength,
        measurement_source_reference=sample.source_record_id,
        measurement_source_publication=provenance.get("reference_text"),
        measurement_source_pubmed_id=provenance.get("pubmed_id"),
        assay_notes=provenance.get("target_name") or provenance.get("raw_affinity_text"),
        mutation_strings=[sample.mutation_string] if sample.mutation_string else None,
        mutation_count=1 if sample.mutation_string else 0,
    )


def merge_assay_samples(samples: list[CanonicalBindingSample]) -> list[AssayRecord]:
    """Convert samples to assay rows and attach pair-aware summary fields."""
    if not samples:
        return []

    assay_rows = [canonical_sample_to_assay_record(sample) for sample in samples]
    grouped: dict[tuple[str, str | None], list[AssayRecord]] = defaultdict(list)
    for row in assay_rows:
        grouped[(row.pair_identity_key or "", row.binding_affinity_type)].append(row)

    merged: list[AssayRecord] = []
    for key in sorted(grouped):
        rows = grouped[key]
        log10_values = [
            row.binding_affinity_log10_standardized
            for row in rows
            if row.binding_affinity_log10_standardized is not None
        ]
        value_summaries = [
            f"{row.source_database}:{row.binding_affinity_type}={row.binding_affinity_value} {row.binding_affinity_unit}".strip()
            for row in rows
            if row.binding_affinity_value is not None
        ]
        summary_text = "; ".join(value_summaries) if value_summaries else None
        mean_value = round(statistics.mean(log10_values), 6) if log10_values else None

        for row in rows:
            merged.append(row.model_copy(update={
                "reported_measurements_text": summary_text,
                "reported_measurement_mean_log10_standardized": mean_value,
                "reported_measurement_count": len(rows),
            }))
    return merged
