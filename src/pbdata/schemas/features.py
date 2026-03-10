"""Feature-layer record schema."""

from __future__ import annotations

from typing import TypeAlias

from pydantic import BaseModel, ConfigDict, field_validator

FeatureValue: TypeAlias = int | float | bool | str | list[str] | None

KNOWN_FEATURE_KEYS = {
    "structure_resolution",
    "atom_count_total",
    "heavy_atom_fraction",
    "mean_atomic_weight",
    "mean_covalent_radius",
    "mean_b_factor",
    "mean_occupancy",
    "residue_count_observed",
    "radius_of_gyration_residue_centroids",
    "protein_chain_count",
    "ligand_count",
    "multiligand_entry",
    "interface_residue_count",
    "interface_types",
    "receptor_chain_count",
    "sequence_length",
    "protein_mean_hydropathy",
    "protein_aromatic_fraction",
    "protein_charged_fraction",
    "protein_polar_fraction",
    "assay_source_database",
    "preferred_source_database",
    "binding_affinity_type",
    "binding_affinity_value",
    "binding_affinity_log10_standardized",
    "binding_affinity_is_mutant_measurement",
    "reported_measurement_count",
    "source_conflict_flag",
    "source_agreement_band",
    "assay_temperature_c",
    "assay_ph",
    "network_degree",
    "ppi_degree",
    "pli_degree",
    "pathway_count",
    "ligand_molecular_weight",
    "ligand_component_type",
    "ligand_inchikey",
    "ligand_is_covalent",
    "metal_present",
    "cofactor_present",
    "glycan_present",
    "covalent_binder_present",
    "peptide_partner_present",
    "membrane_vs_soluble",
    "quality_score",
    "microstate_record_count",
    "estimated_net_charge",
    "mean_abs_residue_charge",
    "positive_residue_count",
    "negative_residue_count",
    "same_charge_contact_count",
    "opposite_charge_contact_count",
    "metal_contact_count",
    "acidic_cluster_penalty",
    "local_electrostatic_balance",
}


class FeatureRecord(BaseModel):
    """One materialized feature row for a protein-ligand or protein-protein context."""

    model_config = ConfigDict(frozen=True)

    feature_id: str
    pdb_id: str | None = None
    pair_identity_key: str | None = None
    feature_group: str
    values: dict[str, FeatureValue]
    provenance: dict[str, object]

    @field_validator("values")
    @classmethod
    def _validate_known_feature_keys(cls, values: dict[str, FeatureValue]) -> dict[str, FeatureValue]:
        unknown = sorted(key for key in values if key not in KNOWN_FEATURE_KEYS)
        if unknown:
            raise ValueError(f"Unknown feature keys: {', '.join(unknown)}")
        return values
