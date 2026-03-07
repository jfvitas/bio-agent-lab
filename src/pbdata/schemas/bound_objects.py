"""Sub-schemas for detailed binder and interface classification.

These models are serialized to dicts (via model_dump()) and stored in
CanonicalBindingSample.bound_objects / .interfaces / .assembly_info.
Using dicts in the parent keeps JSON schema stable while allowing typed
construction here.

BoundObject:  any entity bound to the receptor (ligand, cofactor, metal,
              glycan, peptide partner, protein chain).
InterfaceInfo: a pairwise polymer–polymer interaction in a structure.
AssemblyInfo:  biological-assembly metadata from RCSB.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict

# ---------------------------------------------------------------------------
# Controlled vocabulary type aliases
# ---------------------------------------------------------------------------

BinderType = Literal[
    "small_molecule",   # organic non-cofactor ligand
    "peptide",          # polypeptide ≤ PEPTIDE_MAX_RESIDUES aa
    "protein_chain",    # full-length protein acting as a partner
    "cofactor",         # biochemical cofactor (ATP, heme, FAD, NAD …)
    "metal_ion",        # free metal or halide ion
    "glycan",           # mono/oligosaccharide entity or sugar polymer
    "additive",         # crystallisation agent / buffer / solvent
    "nucleic_acid",     # RNA or DNA polymer entity
    "unknown",
]

BinderRole = Literal[
    "primary_ligand",           # the intended / most prominent bound molecule
    "co_ligand",                # secondary ligand co-bound with the primary
    "cofactor",                 # catalytic or structural cofactor
    "structural_ion",           # ion stabilising fold, not at binding site
    "metal_mediated_contact",   # ion bridging two polymer chains
    "artifact",                 # likely crystallisation additive
    "unknown",
]


# ---------------------------------------------------------------------------
# BoundObject
# ---------------------------------------------------------------------------

class BoundObject(BaseModel):
    """One non-receptor entity associated with a structure.

    Covers nonpolymer entities (small molecules, metals, cofactors) and
    short peptide polymer entities.  Use model_dump() to store inside
    CanonicalBindingSample.bound_objects.
    """

    model_config = ConfigDict(frozen=True)

    comp_id:   str | None = None   # CCD comp_id for nonpolymer; None for polymer binders
    name:      str | None = None   # human-readable name from chem_comp or entity
    entity_id: str | None = None   # RCSB entity ID (e.g. "4HHB_3")
    chain_ids: list[str] | None = None

    binder_type: str = "unknown"   # BinderType
    role:        str = "unknown"   # BinderRole

    smiles:    str | None = None
    inchi_key: str | None = None

    # Covalent-binding flags
    is_covalent:           bool | None = None  # None = not determined from metadata
    covalent_warhead_flag: bool = False         # SMILES contains a known reactive group

    # Polymer-only
    residue_count: int | None = None  # sequence length for peptide partners

    # Provenance
    classification_rationale: str = ""


# ---------------------------------------------------------------------------
# InterfaceInfo
# ---------------------------------------------------------------------------

class InterfaceInfo(BaseModel):
    """Pairwise interaction between two polymer entities in a structure.

    Chain-level geometry (residues, contact counts) is not populated at
    GraphQL-normalisation time; it requires coordinate parsing.
    """

    model_config = ConfigDict(frozen=True)

    entity_id_a: str | None = None
    entity_id_b: str | None = None
    chain_ids_a: list[str]
    chain_ids_b: list[str]

    interface_type: str = "unknown"  # protein_protein | protein_peptide | protein_small_molecule | …
    is_symmetric:   bool = False     # both sides are the same polymer entity (homomeric)
    is_hetero:      bool = False     # two distinct polymer entities

    # Optional descriptors
    entity_name_a: str | None = None
    entity_name_b: str | None = None

    # Filled in later by coordinate-level analysis (mmCIF parsing)
    residue_ids_a: list[str] | None = None
    residue_ids_b: list[str] | None = None
    contact_count: int | None = None


# ---------------------------------------------------------------------------
# AssemblyInfo
# ---------------------------------------------------------------------------

class AssemblyInfo(BaseModel):
    """Biological assembly metadata from the RCSB entry.

    RCSB may report multiple assemblies; we record the preferred one
    (assemblies[0]) plus the total count.
    """

    model_config = ConfigDict(frozen=True)

    assembly_count:  int | None = None   # total assemblies annotated for this entry
    preferred_id:    str | None = None   # e.g. "4HHB-1"
    oligomeric_details: str | None = None  # RCSB text, e.g. "HETERO 21-MER"
    oligomeric_count:   int | None = None  # chain count in preferred assembly
    is_homo_oligomeric: bool | None = None  # True/False/None=unknown

    # From rcsb_assembly_info
    polymer_entity_count:         int | None = None
    polymer_entity_count_protein: int | None = None

    # Derived flag
    asymmetric_unit_is_biological: bool | None = None
    # True  → AU = bio-assembly (no symmetry expansion needed)
    # False → bio-assembly is larger (symmetry mates required)
    # None  → could not determine
