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

from pbdata.pairing import chain_group_key
from pbdata.schemas.canonical_sample import CanonicalBindingSample
from pbdata.schemas.records import AssayRecord

_SOURCE_PRIORITY = {
    "SKEMPI": 1,
    "PDBbind": 2,
    "BioLiP": 3,
    "BindingDB": 4,
    "ChEMBL": 5,
}


def _preferred_source(rows: list[AssayRecord]) -> str | None:
    ordered = sorted(
        {
            row.source_database
            for row in rows
            if row.source_database
        },
        key=lambda name: (_SOURCE_PRIORITY.get(str(name), 99), str(name)),
    )
    return ordered[0] if ordered else None


def _preferred_source_rationale(
    rows: list[AssayRecord],
    preferred_source: str | None,
) -> str | None:
    if not preferred_source:
        return None
    source_names = sorted({str(row.source_database) for row in rows if row.source_database})
    if len(source_names) <= 1:
        return f"single_source:{preferred_source}"
    priority = _SOURCE_PRIORITY.get(preferred_source, 99)
    return (
        f"priority_policy:selected={preferred_source};priority={priority};"
        f"candidates={','.join(source_names)}"
    )


def _conflict_metadata(rows: list[AssayRecord]) -> tuple[bool, str | None, str | None]:
    log10_values = [
        float(row.binding_affinity_log10_standardized)
        for row in rows
        if row.binding_affinity_log10_standardized is not None
    ]
    if len(log10_values) < 2:
        return False, None, None

    spread = max(log10_values) - min(log10_values)
    if spread < 0.3:
        return False, "agreement_within_0.3_log10", "high"
    if spread < 1.0:
        return True, f"moderate_conflict_spread={spread:.3f}", "medium"
    return True, f"high_conflict_spread={spread:.3f}", "low"


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
            chain_group_key(sample.chain_ids_receptor),
            ligand_key,
            mutation_key,
        ])

    if sample.task_type == "protein_protein":
        groups = sorted([
            chain_group_key(sample.chain_ids_receptor),
            chain_group_key(sample.chain_ids_partner),
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
    source_name = sample.source_database
    source_ref = sample.source_record_id
    affinity_confidence = (
        "medium"
        if (sample.wildtype_or_mutant is None and sample.mutation_string is None)
        else "high"
    )
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
        field_provenance={
            "pair_identity_key": {
                "source": source_name,
                "source_record_id": source_ref,
                "method": "pair_identity_key_builder",
                "override_used": bool(provenance.get("pair_grouping_override")),
            },
            "binding_affinity_value": {
                "source": source_name,
                "source_record_id": source_ref,
                "method": "canonical_sample_direct",
            },
            "binding_affinity_type": {
                "source": source_name,
                "source_record_id": source_ref,
                "method": "canonical_sample_direct",
            },
            "binding_affinity_log10_standardized": {
                "source": source_name,
                "source_record_id": source_ref,
                "method": "canonical_sample_direct",
            },
            "binding_affinity_is_mutant_measurement": {
                "source": source_name,
                "source_record_id": source_ref,
                "method": "mutation_annotation_projection",
            },
        },
        field_confidence={
            "pair_identity_key": "medium" if provenance.get("pair_grouping_override") else "high",
            "binding_affinity_value": affinity_confidence,
            "binding_affinity_type": "high" if sample.assay_type else "unknown",
            "binding_affinity_log10_standardized": "high" if sample.assay_value_log10 is not None else "unknown",
            "binding_affinity_is_mutant_measurement": affinity_confidence,
        },
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
        source_refs = sorted(
            {
                row.measurement_source_reference
                for row in rows
                if row.measurement_source_reference
            }
        )
        source_dbs = sorted(
            {
                row.source_database
                for row in rows
                if row.source_database
            }
        )
        conflict_flag, conflict_summary, agreement_band = _conflict_metadata(rows)
        preferred_source = _preferred_source(rows)
        preferred_source_rationale = _preferred_source_rationale(rows, preferred_source)

        for row in rows:
            field_provenance = dict(row.field_provenance or {})
            field_confidence = dict(row.field_confidence or {})
            field_provenance.update({
                "reported_measurements_text": {
                    "sources": source_dbs,
                    "source_record_ids": source_refs,
                    "method": "pair_group_summary_concat",
                },
                "reported_measurement_mean_log10_standardized": {
                    "sources": source_dbs,
                    "source_record_ids": source_refs,
                    "method": "pair_group_summary_mean_log10",
                    "input_count": len(log10_values),
                },
                "reported_measurement_count": {
                    "sources": source_dbs,
                    "source_record_ids": source_refs,
                    "method": "pair_group_summary_count",
                },
                "source_conflict_flag": {
                    "sources": source_dbs,
                    "source_record_ids": source_refs,
                    "method": "pair_group_conflict_detection_log10_spread",
                },
                "source_conflict_summary": {
                    "sources": source_dbs,
                    "source_record_ids": source_refs,
                    "method": "pair_group_conflict_detection_log10_spread",
                },
                "source_agreement_band": {
                    "sources": source_dbs,
                    "source_record_ids": source_refs,
                    "method": "pair_group_conflict_detection_log10_spread",
                },
                "selected_preferred_source": {
                    "sources": source_dbs,
                    "source_record_ids": source_refs,
                    "method": "explicit_source_priority_policy",
                    "priority_order": _SOURCE_PRIORITY,
                },
                "selected_preferred_source_rationale": {
                    "sources": source_dbs,
                    "source_record_ids": source_refs,
                    "method": "explicit_source_priority_policy",
                    "priority_order": _SOURCE_PRIORITY,
                },
            })
            field_confidence.update({
                "reported_measurements_text": "high" if summary_text else "unknown",
                "reported_measurement_mean_log10_standardized": (
                    "high" if len(log10_values) == len(rows) and log10_values else
                    "medium" if log10_values else
                    "unknown"
                ),
                "reported_measurement_count": "high",
                "source_conflict_flag": "high" if len(log10_values) >= 2 else "unknown",
                "source_conflict_summary": "high" if conflict_summary else "unknown",
                "source_agreement_band": "high" if agreement_band else "unknown",
                "selected_preferred_source": "medium" if preferred_source else "unknown",
                "selected_preferred_source_rationale": "high" if preferred_source_rationale else "unknown",
            })
            merged.append(row.model_copy(update={
                "reported_measurements_text": summary_text,
                "reported_measurement_mean_log10_standardized": mean_value,
                "reported_measurement_count": len(rows),
                "source_conflict_flag": conflict_flag,
                "source_conflict_summary": conflict_summary,
                "source_agreement_band": agreement_band,
                "selected_preferred_source": preferred_source,
                "selected_preferred_source_rationale": preferred_source_rationale,
                "field_provenance": field_provenance,
                "field_confidence": field_confidence,
            }))
    return merged
