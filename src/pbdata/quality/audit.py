"""Quality audit logic for CanonicalBindingSample records.

Computes quality_flags and quality_score for each record.

Flags are machine-readable strings — informational only; they do not
remove records from the dataset.  Two categories:

  Data-quality flags
  ------------------
  Warn about missing fields, low resolution, or incomplete coverage.
  These affect quality_score.

  Structural / ambiguity flags
  ----------------------------
  Describe the biological context and highlight edge cases that require
  special handling in ML pipelines.  These do NOT affect quality_score;
  they are diagnostic metadata.

NOTE: alternate_conformer_present and partial_occupancy_present cannot
be detected from GraphQL metadata alone.  They require parsing the
mmCIF/PDB coordinate file (struct_conf / ANISOU records).  These flags
are documented but not yet set automatically.
"""

from __future__ import annotations

from pbdata.schemas.canonical_sample import CanonicalBindingSample

# ---------------------------------------------------------------------------
# Resolution thresholds (Angstroms)
# ---------------------------------------------------------------------------

_RES_LOW      = 3.5
_RES_VERY_LOW = 4.5

# Cofactor comp_ids known to coordinate a metal atom (used for
# metal_present flag even when no free metal ion entity is present).
_METALLO_COFACTOR_COMP_IDS: frozenset[str] = frozenset({
    "HEM", "HEC", "HEA", "HEB", "HDD", "CLN",  # iron-porphyrins
    "FES", "SF4", "F3S", "FEO", "ACO",           # iron-sulfur
    "MGD", "MTE",                                  # molybdenum
    "CLA", "BCL",                                  # chlorophyll (Mg)
    "B12", "CBL",                                  # cobalamin (Co)
})


# ---------------------------------------------------------------------------
# Data-quality flag computation
# ---------------------------------------------------------------------------

def compute_flags(record: CanonicalBindingSample) -> list[str]:
    """Return a deduplicated, deterministically ordered list of flag strings.

    Flags are derived entirely from the canonical record fields so that
    audit_record() is idempotent and produces the same output for the same
    input regardless of call order.

    Structural / ambiguity flags are derived from bound_objects, interfaces,
    and assembly_info when those fields are populated.
    """
    flags: list[str] = []

    # ------------------------------------------------------------------
    # A) Data-quality flags
    # ------------------------------------------------------------------

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

    # ------------------------------------------------------------------
    # B) Structural / ambiguity flags (derived from extended fields)
    # ------------------------------------------------------------------

    bound_objects = _load_bound_objects(record)
    interfaces    = _load_interfaces(record)
    assembly_info = _load_assembly_info(record)

    # --- Interface symmetry / oligomeric type ---
    has_symmetric = any(i.get("is_symmetric") for i in interfaces)
    has_hetero    = any(i.get("is_hetero")    for i in interfaces)

    if has_symmetric:
        flags.append("homomeric_symmetric_interface")
    if has_hetero:
        flags.append("heteromeric_interface")

    # Multimeric (3+ chains in any interface, or 3+ protein entities)
    if record.polymer_entity_count is not None and record.polymer_entity_count >= 3:
        flags.append("multimeric_complex")
    elif len(interfaces) >= 2:
        flags.append("multimeric_complex")

    # --- Bound-object flags ---
    non_artifact = [b for b in bound_objects if b.get("role") != "artifact"]
    if len(non_artifact) > 1:
        flags.append("multiple_bound_objects")

    # Metal: free metal ions OR metallocofactors
    free_metals = [b for b in bound_objects if b.get("binder_type") == "metal_ion"]
    metallo_cofactors = [
        b for b in bound_objects
        if b.get("binder_type") == "cofactor"
        and (b.get("comp_id") or "") in _METALLO_COFACTOR_COMP_IDS
    ]
    if free_metals or metallo_cofactors:
        flags.append("metal_present")

    if free_metals and non_artifact:
        # A free metal co-exists with other bound objects → possible bridging
        flags.append("metal_mediated_binding_possible")

    # Cofactors (biochemical)
    if any(b.get("binder_type") == "cofactor" for b in bound_objects):
        flags.append("cofactor_present")

    # Glycans
    if any(b.get("binder_type") == "glycan" for b in bound_objects):
        flags.append("glycan_present")

    # Covalent binders
    covalent = [
        b for b in bound_objects
        if b.get("is_covalent") is True or b.get("covalent_warhead_flag")
    ]
    if covalent:
        flags.append("covalent_binder")

    # Peptide partners
    if any(b.get("binder_type") == "peptide" for b in bound_objects):
        flags.append("peptide_partner")

    # Possible crystallisation additives
    if any(b.get("role") == "artifact" for b in bound_objects):
        flags.append("possible_crystallization_additive")

    # --- Assembly flags ---
    if assembly_info:
        count = assembly_info.get("assembly_count")
        if count is not None and count > 1:
            flags.append("assembly_ambiguity")

    # --- Membrane context (stored in provenance by rcsb adapter) ---
    if record.provenance.get("membrane_protein_context"):
        flags.append("membrane_protein_context")

    # --- Flags that require coordinate-level data (not yet auto-set) ---
    # alternate_conformer_present — needs ANISOU / altloc parsing
    # partial_occupancy_present   — needs occupancy column from mmCIF
    # interface_incomplete_or_missing_residues — needs struct_conf + seq match

    # Preserve any flags already present (set externally or by a prior audit)
    combined = list(dict.fromkeys(flags))  # deduplicate, preserve order
    return combined


# ---------------------------------------------------------------------------
# Quality score
# ---------------------------------------------------------------------------

def compute_score(record: CanonicalBindingSample) -> float:
    """Return a quality score in [0.0, 1.0] based on data-quality field coverage.

    Only data-quality checks (not structural/ambiguity flags) affect the score,
    so that complex structures are not penalised for biological richness.
    """
    checks = [
        record.experimental_method is not None,
        record.structure_resolution is not None and record.structure_resolution <= _RES_LOW,
        record.sequence_receptor is not None,
        bool(record.chain_ids_receptor),
        bool(record.uniprot_ids),
        bool(record.taxonomy_ids),
        # Task-type-specific checks
        record.task_type != "protein_ligand" or record.ligand_id is not None,
        record.task_type != "protein_protein" or record.sequence_partner is not None,
    ]
    return round(sum(checks) / len(checks), 4)


# ---------------------------------------------------------------------------
# Top-level audit function
# ---------------------------------------------------------------------------

def audit_record(record: CanonicalBindingSample) -> CanonicalBindingSample:
    """Return a new frozen record with updated quality_flags and quality_score."""
    return record.model_copy(update={
        "quality_flags": compute_flags(record),
        "quality_score": compute_score(record),
    })


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _load_bound_objects(record: CanonicalBindingSample) -> list[dict]:
    return list(record.bound_objects or [])


def _load_interfaces(record: CanonicalBindingSample) -> list[dict]:
    return list(record.interfaces or [])


def _load_assembly_info(record: CanonicalBindingSample) -> dict | None:
    return record.assembly_info
