from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


class CanonicalBindingSample(BaseModel):
    """Unified normalized record for protein binding ML datasets.

    Records are immutable after creation (frozen=True) to protect provenance
    integrity. Use model_copy(update={...}) to derive modified records.
    """

    model_config = ConfigDict(frozen=True)

    sample_id: str
    task_type: Literal["protein_ligand", "protein_protein", "mutation_ddg"]
    source_database: str
    source_record_id: str

    pdb_id: str | None = None
    assembly_id: str | None = None

    chain_ids_receptor: list[str] | None = None
    chain_ids_partner: list[str] | None = None
    uniprot_ids: list[str] | None = None

    sequence_receptor: str | None = None
    sequence_partner: str | None = None
    taxonomy_ids: list[int] | None = None

    ligand_id: str | None = None
    ligand_smiles: str | None = None
    ligand_inchi_key: str | None = None

    experimental_method: str | None = None
    structure_resolution: float | None = None

    assay_type: str | None = None
    assay_value: float | None = None
    assay_unit: str | None = None
    assay_value_standardized: float | None = None
    assay_value_log10: float | None = None

    temperature_c: float | None = None
    ph: float | None = None
    buffer: str | None = None
    ionic_strength: float | None = None
    cofactors: list[str] | None = None

    mutation_string: str | None = None
    wildtype_or_mutant: Literal["wildtype", "mutant"] | None = None
    curation_level: str | None = None

    provenance: dict[str, Any]
    quality_flags: list[str]
    quality_score: float

    # ------------------------------------------------------------------
    # Field validators
    # ------------------------------------------------------------------

    @field_validator("quality_score")
    @classmethod
    def _quality_score_range(cls, v: float) -> float:
        if not 0.0 <= v <= 1.0:
            raise ValueError(f"quality_score must be in [0.0, 1.0], got {v}")
        return v

    @field_validator("ph")
    @classmethod
    def _ph_range(cls, v: float | None) -> float | None:
        if v is not None and not 0.0 <= v <= 14.0:
            raise ValueError(f"ph must be in [0.0, 14.0], got {v}")
        return v

    @field_validator("structure_resolution")
    @classmethod
    def _resolution_positive(cls, v: float | None) -> float | None:
        if v is not None and v <= 0.0:
            raise ValueError(f"structure_resolution must be > 0, got {v}")
        return v

    @field_validator("temperature_c")
    @classmethod
    def _temperature_above_absolute_zero(cls, v: float | None) -> float | None:
        if v is not None and v <= -273.15:
            raise ValueError(f"temperature_c must be > -273.15, got {v}")
        return v

    @field_validator("ionic_strength")
    @classmethod
    def _ionic_strength_non_negative(cls, v: float | None) -> float | None:
        if v is not None and v < 0.0:
            raise ValueError(f"ionic_strength must be >= 0, got {v}")
        return v

    @field_validator("provenance")
    @classmethod
    def _provenance_has_ingested_at(cls, v: dict[str, Any]) -> dict[str, Any]:
        if "ingested_at" not in v:
            raise ValueError(
                "provenance must contain 'ingested_at' (ISO timestamp of ingestion)"
            )
        return v
