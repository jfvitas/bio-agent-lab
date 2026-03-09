"""Training-example layer schemas aligned with bio_agent_full_spec."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict


class StructureFields(BaseModel):
    model_config = ConfigDict(frozen=True)

    pdb_id: str | None = None
    chain_ids: list[str] | None = None
    assembly: str | None = None
    resolution: float | None = None
    atom_count_total: int | None = None
    heavy_atom_fraction: float | None = None
    mean_atomic_weight: float | None = None
    mean_covalent_radius: float | None = None
    mean_b_factor: float | None = None
    mean_occupancy: float | None = None
    residue_count_observed: int | None = None
    radius_of_gyration_residue_centroids: float | None = None


class ProteinFields(BaseModel):
    model_config = ConfigDict(frozen=True)

    uniprot_id: str | None = None
    organism: str | None = None
    gene: str | None = None
    domains: list[str] | None = None
    sequence_length: int | None = None
    mean_hydropathy: float | None = None
    aromatic_fraction: float | None = None
    charged_fraction: float | None = None
    polar_fraction: float | None = None


class LigandFields(BaseModel):
    model_config = ConfigDict(frozen=True)

    ligand_id: str | None = None
    ligand_type: str | None = None
    inchikey: str | None = None
    smiles: str | None = None
    molecular_weight: float | None = None


class InteractionFields(BaseModel):
    model_config = ConfigDict(frozen=True)

    interface_residues: list[str] | None = None
    hydrogen_bonds: int | None = None
    salt_bridges: int | None = None
    interface_residue_count: int | None = None
    microstate_record_count: int | None = None
    estimated_net_charge: float | None = None
    mean_abs_residue_charge: float | None = None
    positive_residue_count: int | None = None
    negative_residue_count: int | None = None
    same_charge_contact_count: int | None = None
    opposite_charge_contact_count: int | None = None
    metal_contact_count: int | None = None
    acidic_cluster_penalty: float | None = None
    local_electrostatic_balance: float | None = None


class ExperimentFields(BaseModel):
    model_config = ConfigDict(frozen=True)

    affinity_type: str | None = None
    affinity_value: float | None = None
    temperature: float | None = None
    ph: float | None = None
    source_database: str | None = None
    preferred_source_database: str | None = None
    reported_measurement_count: int | None = None
    source_conflict_flag: bool | None = None
    source_agreement_band: str | None = None


class GraphFeatureFields(BaseModel):
    model_config = ConfigDict(frozen=True)

    network_degree: int | None = None
    ppi_degree: int | None = None
    pli_degree: int | None = None
    pathway_count: int | None = None


class TrainingExampleRecord(BaseModel):
    """Top-level training example emitted by the final generator stage."""

    model_config = ConfigDict(frozen=True)

    example_id: str
    structure: StructureFields
    protein: ProteinFields
    ligand: LigandFields
    interaction: InteractionFields
    experiment: ExperimentFields
    graph_features: GraphFeatureFields
    labels: dict[str, Any] | None = None
    provenance: dict[str, Any]
