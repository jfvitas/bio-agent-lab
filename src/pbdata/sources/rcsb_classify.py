"""Entity classification logic for RCSB GraphQL records.

This module contains all heuristic classification logic that maps raw
RCSB entity dicts to typed BoundObject / InterfaceInfo / AssemblyInfo
records.  It is intentionally free of I/O and has no dependency on
rcsb.py, keeping the direction clean:

    rcsb.py  →  rcsb_classify.py  →  pbdata.schemas.bound_objects

Design choices
--------------
* Peptide threshold (≤ 30 residues): follows common structural-biology
  convention.  Sequences exactly at the boundary are classified as
  peptide.  Adjust PEPTIDE_MAX_RESIDUES to change globally.

* Metal / cofactor / glycan sets are curated manually.  They are
  intentionally conservative: if a comp_id is not in one of those
  sets, it falls through to "small_molecule".  Unknown molecules
  should be explored, not silently filtered.

* Covalent warhead detection is purely SMILES-pattern-based and does
  not require RDKit.  It flags *possible* covalent binders; the flag
  is informational (is_covalent=None is the default; True requires
  struct_conn from the mmCIF file).

* Oligomeric state is inferred from entity count and chain multiplicity
  of protein entities.  Assembly-level data (from the `assemblies`
  GraphQL field) can override this inference if available.
"""

from __future__ import annotations

import re
from typing import Any

from pbdata.schemas.bound_objects import AssemblyInfo, BoundObject, InterfaceInfo

# ---------------------------------------------------------------------------
# Thresholds
# ---------------------------------------------------------------------------

PEPTIDE_MAX_RESIDUES: int = 30

# ---------------------------------------------------------------------------
# Polymer type sets
# ---------------------------------------------------------------------------

_PROTEIN_POLY_TYPES: frozenset[str] = frozenset({"polypeptide(l)", "polypeptide(d)"})
_NUCLEIC_ACID_POLY_TYPES: frozenset[str] = frozenset({
    "polyribonucleotide",
    "polydeoxyribonucleotide",
    "polyribonucleotide/polydeoxyribonucleotide hybrid",
})
_GLYCAN_POLY_TYPES: frozenset[str] = frozenset({"polysaccharide(d)", "polysaccharide(l)"})

# ---------------------------------------------------------------------------
# Nonpolymer classification sets
# ---------------------------------------------------------------------------

# Free metal / halide ions — should NOT be treated as drug-like ligands
_METAL_COMP_IDS: frozenset[str] = frozenset({
    # Alkali / alkaline-earth
    "LI", "NA", "K", "RB", "CS",
    "MG", "CA", "SR", "BA",
    # Transition metals (common in active sites / crystal contacts)
    "V", "CR", "MN", "FE", "CO", "NI", "CU", "ZN",
    "MO", "TC", "RU", "RH", "PD", "AG", "CD",
    "W", "RE", "OS", "IR", "PT", "AU", "HG",
    # Post-transition / metalloids
    "AL", "GA", "IN", "TL", "SN", "PB", "BI",
    "AS", "SE", "TE",
    # Halogens as ions
    "F", "CL", "BR", "IOD",
    # Lanthanides (used in phasing / crystal contacts)
    "LA", "CE", "PR", "ND", "SM", "EU", "GD", "TB",
    "DY", "HO", "ER", "TM", "YB", "LU",
    # Common oxidation-state variants listed by RCSB
    "FE2", "FE3", "CU1", "MN3",
    # Oxyanions treated as metal-like in crystallography
    "VO4",
})

# Biochemical cofactors — biologically essential but not the primary
# drug-like ligand
_COFACTOR_COMP_IDS: frozenset[str] = frozenset({
    # Adenine nucleotides
    "ATP", "ADP", "AMP", "ANP", "APC", "AGS",
    # Guanine nucleotides
    "GTP", "GDP", "GMP", "GNP",
    # Other nucleotides
    "CTP", "CDP", "UTP", "UDP", "TTP", "TDP",
    # Nicotinamide cofactors
    "NAD", "NADH", "NAP", "NHD",   # NAD+/NADH
    "NDP", "NHE",                   # NADP+/NADPH variants
    # Flavins
    "FAD", "FMN",
    # Coenzyme A / pantetheine
    "COA", "CNA", "3CO",
    # Heme groups
    "HEM", "HEC", "HEA", "HEB", "HDD", "CLN",
    # Pyridoxal phosphate
    "PLP", "PMP",
    # Thiamine pyrophosphate
    "TPP", "TDP",
    # S-Adenosyl methionine / homocysteine
    "SAM", "SAH",
    # Biotin
    "BTN",
    # Lipoic acid
    "LPA",
    # Cobalamin / B12
    "B12", "CBL",
    # Chlorophylls / porphyrins (non-heme)
    "CLA", "BCL",
    # Iron-sulfur clusters
    "SF4", "F3S", "FES", "FEO", "ACO",
    # Molybdenum cofactor
    "MGD", "MTE",
    # Ubiquinone
    "UQ1",
    # Retinal / retinol
    "RET", "RBT",
})

# Monosaccharide / oligosaccharide nonpolymer residues
# (polymer glycan chains are detected via entity_poly.type)
_GLYCAN_SUGAR_COMP_IDS: frozenset[str] = frozenset({
    "NAG", "NDG", "NGA",   # N-acetylglucosamine variants
    "BMA", "MAN", "MAF",   # mannose variants
    "GAL", "GLA", "GLA",   # galactose variants
    "FUC", "FCA",          # fucose
    "SIA", "SLB",          # sialic acid
    "GLC", "BGC",          # glucose variants
    "XYS", "XYL",          # xylose
    "AFL",                 # arabinofuranose
    "GCS",                 # glucosamine
    "RIB", "RHA",          # ribose, rhamnose
})

# Crystallisation agents, solvents, buffer components, and common ions
# that should be flagged as artifacts
_EXCLUDED_COMPS: frozenset[str] = frozenset({
    "HOH", "DOD",                                      # water
    "SO4", "PO4", "HPO", "H2P", "PEP",               # phosphates/sulfates
    "CL", "NA", "MG", "ZN", "CA", "K",               # already in metals, but
    "MN", "FE", "NI", "CU", "CD", "CO", "BR",        # listed here as additives too
    "IOD", "F", "CS", "RB", "SR", "BA",
    "AU", "HG", "PT", "PB",
    "GOL", "EDO", "PEG", "MPD", "PG4", "PGE",        # cryo-protectants
    "FMT", "ACT", "ACE", "ACY", "ETH", "DMS",        # solvents / acetate
    "MES", "TRS", "HEP", "BME", "EPE", "MLI",        # buffer components
    "SUC", "TAR", "AZI", "IMD", "NH2",               # other additives
    "NO3", "CIT", "ACN", "EOH", "IPA", "TFP",
    "DIO", "DMF", "XPE", "P6G",                       # more organics
    "NH4", "OXY",
})

# Metal-containing cofactors: the metal is coordinated/incorporated but the
# entity is still classified as 'cofactor'.  Used to set metal_present flag
# even when no free metal_ion entity is present.
_METALLO_COFACTOR_IDS: frozenset[str] = frozenset({
    "HEM", "HEC", "HEA", "HEB", "HDD", "CLN",  # iron-porphyrins
    "FES", "SF4", "F3S", "FEO", "ACO",           # iron-sulfur clusters
    "MGD", "MTE",                                  # molybdenum cofactor
    "CLA", "BCL",                                  # chlorophylls (Mg)
    "B12", "CBL",                                  # cobalamin (Co)
})

# Keywords that indicate a membrane-protein context.
# Checked against struct_keywords.pdbx_keywords and struct_keywords.text
# (case-insensitive substring match).
_MEMBRANE_KEYWORDS: tuple[str, ...] = (
    "membrane protein",
    "transmembrane",
    "gpcr",
    "g protein-coupled",
    "ion channel",
    "transporter",
    "receptor tyrosine kinase",
    "integrin",
    "aquaporin",
    "abc transporter",
    "membrane receptor",
    "lipid bilayer",
    "detergent",
    "micelle",
    # Photosynthetic membrane complexes (thylakoid-embedded)
    "photosystem",
    "thylakoid",
    "photosynthesis",
    "light harvesting",
    # DHHC-family palmitoyltransferases and lipid-modifying enzymes
    "dhhc",
    "palmitoyl",
    "palmitoyltransferase",
    # Other common membrane-embedded enzyme families
    "cytochrome bc1",
    "cytochrome c oxidase",
    "nadh dehydrogenase",
    "atp synthase",
)

# ---------------------------------------------------------------------------
# Covalent warhead SMILES patterns (no RDKit required)
# Each pattern is a compiled regex against the SMILES string.
# ---------------------------------------------------------------------------

_WARHEAD_PATTERNS: list[re.Pattern[str]] = [
    re.compile(r"C=CC\(=O\)N"),     # acrylamide (Michael acceptor)
    re.compile(r"C=CC\(=O\)O"),     # acrylate
    re.compile(r"C=CS\(=O\)"),      # vinyl sulfone
    re.compile(r"C1CO1"),           # epoxide
    re.compile(r"ClCC[CN]"),        # chloroacetamide
    re.compile(r"BrCC[CN]"),        # bromoacetamide
    re.compile(r"ClC\(=O\)"),       # acyl chloride
    re.compile(r"C=C[^(].*C=O"),   # enone (Knoevenagel-type)
    re.compile(r"\[N\+\].*\[O-\]"), # N-oxide (some boronic acids use this)
]

_COVALENT_TITLE_KEYWORDS: tuple[str, ...] = (
    "covalent inhibitor",
    "covalent complex",
    "acyl-enzyme",
    "acyl enzyme",
    "covalent adduct",
    "irreversible inhibitor",
    "irreversibly inhibited",  # e.g. "irreversibly inhibited by 2-bromopalmitate"
    "irreversibly inactivated",
    "covalently bound",
    "covalently modified",
)

_GLYCAN_CONTEXT_KEYWORDS: tuple[str, ...] = (
    "glycosylated",
    "glycoprotein",
    "glycan",
    "fucosylated",
    "sialylated",
    "oligosaccharide",
)

_METAL_CONTEXT_KEYWORDS: dict[str, str] = {
    "nickel": "NI",
    "zn(": "ZN",
    "zinc": "ZN",
    "cobalt": "CO",
    "manganese": "MN",
    "magnesium": "MG",
    "calcium": "CA",
}


# ---------------------------------------------------------------------------
# Low-level entity helpers
# ---------------------------------------------------------------------------

def _poly_type(entity: dict[str, Any]) -> str:
    return (entity.get("entity_poly") or {}).get("type", "").lower()


def _sequence(entity: dict[str, Any]) -> str | None:
    poly = entity.get("entity_poly") or {}
    seq: str | None = poly.get("pdbx_seq_one_letter_code_can")
    return seq.replace("\n", "").strip() if seq else None


def _polymer_chain_ids(entity: dict[str, Any]) -> list[str] | None:
    ids = (
        (entity.get("rcsb_polymer_entity_container_identifiers") or {})
        .get("auth_asym_ids")
    )
    return list(ids) if ids else None


def _polymer_description(entity: dict[str, Any]) -> str:
    return str(((entity.get("rcsb_polymer_entity") or {}).get("pdbx_description") or ""))


def _entity_suffix(entity: dict[str, Any]) -> str | None:
    entity_id = entity.get("entity_id")
    if entity_id:
        return str(entity_id)
    rcsb_id = str(entity.get("rcsb_id") or "")
    if "_" in rcsb_id:
        return rcsb_id.rsplit("_", 1)[-1]
    return None


def _nonpolymer_chain_ids(entity: dict[str, Any]) -> list[str] | None:
    ids = (
        (entity.get("rcsb_nonpolymer_entity_container_identifiers") or {})
        .get("auth_asym_ids")
    )
    return list(ids) if ids else None


def _apply_mmcif_supplement(
    raw_entry: dict[str, Any],
    poly_entities: list[dict[str, Any]],
    nonpoly_entities: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Augment GraphQL entities with coordinate-derived mmCIF supplement data."""
    supplement = raw_entry.get("mmcif_supplement") or {}
    if not supplement:
        return poly_entities, nonpoly_entities

    polymer_by_id: dict[str, dict[str, Any]] = {
        eid: entity for entity in poly_entities if (eid := _entity_suffix(entity))
    }
    merged_poly = list(poly_entities)
    for extra in supplement.get("polymer_entities") or []:
        entity_id = str(extra.get("entity_id") or "")
        if not entity_id:
            continue
        chains = list(extra.get("chain_ids") or [])
        seq = extra.get("sequence")
        if entity_id in polymer_by_id:
            entity = polymer_by_id[entity_id]
            poly = entity.setdefault("entity_poly", {})
            if seq and not poly.get("pdbx_seq_one_letter_code_can"):
                poly["pdbx_seq_one_letter_code_can"] = seq
            if extra.get("poly_type") and not poly.get("type"):
                poly["type"] = extra["poly_type"]
            ids = entity.setdefault("rcsb_polymer_entity_container_identifiers", {})
            if chains and not ids.get("auth_asym_ids"):
                ids["auth_asym_ids"] = chains
            continue
        merged_poly.append({
            "rcsb_id": f"{raw_entry.get('rcsb_id', 'SUPP')}_{entity_id}",
            "entity_id": entity_id,
            "entity_poly": {
                "type": extra.get("poly_type") or "other",
                "pdbx_seq_one_letter_code_can": seq,
            },
            "rcsb_polymer_entity_container_identifiers": {"auth_asym_ids": chains},
        })

    # Represent branched carbohydrate entities as glycan-like polymers so they survive normalization.
    for extra in supplement.get("branched_entities") or []:
        entity_id = str(extra.get("entity_id") or "")
        if not entity_id or entity_id in polymer_by_id:
            continue
        merged_poly.append({
            "rcsb_id": f"{raw_entry.get('rcsb_id', 'SUPP')}_{entity_id}",
            "entity_id": entity_id,
            "entity_poly": {
                "type": "polysaccharide(D)",
                "pdbx_seq_one_letter_code_can": None,
            },
            "rcsb_polymer_entity_container_identifiers": {
                "auth_asym_ids": list(extra.get("chain_ids") or []),
            },
        })

    supplement_instances = supplement.get("nonpolymer_instances") or []
    if not supplement_instances:
        return merged_poly, nonpoly_entities

    expanded_nonpoly: list[dict[str, Any]] = []
    for inst in supplement_instances:
        comp_id = str(inst.get("comp_id") or "")
        if not comp_id:
            continue
        chain_id = str(inst.get("chain_id") or "")
        residue_id = str(inst.get("residue_id") or "")
        entity_id = str(inst.get("entity_id") or "")
        expanded_nonpoly.append({
            "rcsb_id": "_".join(
                part for part in (raw_entry.get("rcsb_id"), entity_id, comp_id, chain_id, residue_id) if part
            ),
            "entity_id": entity_id,
            "nonpolymer_comp": {"chem_comp": {"id": comp_id, "name": comp_id}},
            "rcsb_nonpolymer_entity_container_identifiers": {"auth_asym_ids": [chain_id] if chain_id else []},
        })
    return merged_poly, expanded_nonpoly or nonpoly_entities


# ---------------------------------------------------------------------------
# Polymer entity classification
# ---------------------------------------------------------------------------

def classify_polymer_entity(entity: dict[str, Any]) -> str:
    """Classify a polymer entity by type and (for proteins) length.

    Returns one of:
        'protein'      — polypeptide > PEPTIDE_MAX_RESIDUES residues
        'peptide'      — polypeptide ≤ PEPTIDE_MAX_RESIDUES residues
        'nucleic_acid' — RNA / DNA / hybrid
        'glycan'       — polysaccharide polymer
        'other_polymer'
    """
    ptype = _poly_type(entity)
    if ptype in _PROTEIN_POLY_TYPES:
        seq = _sequence(entity)
        if seq is not None and len(seq) <= PEPTIDE_MAX_RESIDUES:
            return "peptide"
        return "protein"
    if ptype in _NUCLEIC_ACID_POLY_TYPES:
        return "nucleic_acid"
    if ptype in _GLYCAN_POLY_TYPES:
        return "glycan"
    return "other_polymer"


# ---------------------------------------------------------------------------
# Nonpolymer entity classification
# ---------------------------------------------------------------------------

def classify_nonpolymer_entity(entity: dict[str, Any]) -> tuple[str, str, str]:
    """Classify a nonpolymer entity.

    Returns (binder_type, role, rationale).
    """
    chem = ((entity.get("nonpolymer_comp") or {}).get("chem_comp") or {})
    comp_id = (chem.get("id") or "").upper()

    if comp_id in _METAL_COMP_IDS:
        return "metal_ion", "structural_ion", f"{comp_id!r} in _METAL_COMP_IDS"
    if comp_id in _COFACTOR_COMP_IDS:
        return "cofactor", "cofactor", f"{comp_id!r} in _COFACTOR_COMP_IDS"
    if comp_id in _GLYCAN_SUGAR_COMP_IDS:
        return "glycan", "unknown", f"{comp_id!r} in _GLYCAN_SUGAR_COMP_IDS"
    if comp_id in _EXCLUDED_COMPS:
        return "additive", "artifact", f"{comp_id!r} in _EXCLUDED_COMPS (crystallisation artefact)"
    if not comp_id:
        return "unknown", "unknown", "no comp_id"
    return "small_molecule", "primary_ligand", f"{comp_id!r} not in any exclusion set → small molecule"


# ---------------------------------------------------------------------------
# Covalent-warhead detection
# ---------------------------------------------------------------------------

def has_covalent_warhead(smiles: str) -> bool:
    """Return True if the SMILES string contains a known reactive warhead."""
    for pat in _WARHEAD_PATTERNS:
        if pat.search(smiles):
            return True
    return False


# ---------------------------------------------------------------------------
# Oligomeric state inference
# ---------------------------------------------------------------------------

def infer_oligomeric_state(
    protein_entities: list[dict[str, Any]],
) -> tuple[bool | None, str]:
    """Infer oligomeric state from protein entity count and chain multiplicity.

    Returns (is_homo_oligomeric, description).

    is_homo_oligomeric:
        True  — all copies are the same protein entity
        False — two or more distinct protein entities
        None  — monomer or indeterminate
    """
    n = len(protein_entities)
    if n == 0:
        return None, "no_protein"
    if n == 1:
        chains = _polymer_chain_ids(protein_entities[0]) or []
        nc = len(chains)
        if nc <= 1:
            return None, "monomer"
        _suffix = {2: "homodimer", 3: "homotrimer", 4: "homotetramer"}.get(nc)
        return True, _suffix or f"homo_{nc}mer"
    # n >= 2 protein entities → heteromeric
    if n == 2:
        ca = _polymer_chain_ids(protein_entities[0]) or []
        cb = _polymer_chain_ids(protein_entities[1]) or []
        if len(ca) == 1 and len(cb) == 1:
            return False, "heterodimer"
    return False, f"hetero_complex_{n}_entities"


# ---------------------------------------------------------------------------
# BoundObject construction
# ---------------------------------------------------------------------------

def build_bound_objects(
    nonpoly_entities: list[dict[str, Any]],
    poly_entities: list[dict[str, Any]],
    other_poly_entities: list[dict[str, Any]] | None = None,
    chem_descriptors: dict[str, dict[str, str]] | None = None,
) -> list[BoundObject]:
    """Build a typed BoundObject for every bound entity in the structure.

    Includes:
    - All nonpolymer entities (metals, cofactors, ligands, additives, glycan sugars)
    - Short polypeptide polymer entities (peptide partners)
    - Polymer glycans / nucleic-acid partners as typed bound objects

    Full-length protein entities are NOT included; they appear in
    InterfaceInfo instead.
    """
    objects: list[BoundObject] = []
    other_poly_entities = other_poly_entities or []

    # --- Nonpolymer entities ---
    for ent in nonpoly_entities:
        btype, role, rationale = classify_nonpolymer_entity(ent)
        chem = ((ent.get("nonpolymer_comp") or {}).get("chem_comp") or {})
        comp_id = (chem.get("id") or None)
        name    = (chem.get("name") or None)
        entity_id = ent.get("rcsb_id") or None
        chain_ids = _nonpolymer_chain_ids(ent)

        smiles    = None
        inchi_key = None
        warhead   = False
        if comp_id and chem_descriptors:
            descs = chem_descriptors.get(comp_id, {})
            smiles    = descs.get("SMILES_CANONICAL") or descs.get("SMILES")
            inchi_key = descs.get("InChIKey")
            if smiles:
                warhead = has_covalent_warhead(smiles)

        objects.append(BoundObject(
            comp_id=comp_id,
            name=name,
            entity_id=entity_id,
            chain_ids=chain_ids,
            binder_type=btype,
            role=role,
            smiles=smiles,
            inchi_key=inchi_key,
            is_covalent=True if warhead else None,
            covalent_warhead_flag=warhead,
            classification_rationale=rationale,
        ))

    # --- Short polypeptide polymer entities (peptide partners) ---
    for ent in poly_entities:
        if classify_polymer_entity(ent) != "peptide":
            continue
        seq       = _sequence(ent)
        chain_ids = _polymer_chain_ids(ent)
        rcount    = len(seq) if seq else None
        objects.append(BoundObject(
            entity_id=ent.get("rcsb_id") or None,
            chain_ids=chain_ids,
            binder_type="peptide",
            role="primary_ligand",
            residue_count=rcount,
            classification_rationale=(
                f"Polypeptide with {rcount} residues "
                f"(threshold ≤ {PEPTIDE_MAX_RESIDUES})"
            ),
        ))

    # --- Other polymer binders (glycans / nucleic acids) ---
    for ent in other_poly_entities:
        kind = classify_polymer_entity(ent)
        if kind not in {"glycan", "nucleic_acid"}:
            continue
        seq = _sequence(ent)
        chain_ids = _polymer_chain_ids(ent)
        residue_count = len(seq) if seq else None
        role = "co_ligand" if kind == "glycan" else "unknown"
        rationale = (
            "Polymer glycan preserved as a distinct bound object"
            if kind == "glycan"
            else "Nucleic-acid polymer preserved as a distinct bound object"
        )
        objects.append(BoundObject(
            entity_id=ent.get("rcsb_id") or None,
            chain_ids=chain_ids,
            binder_type=kind,
            role=role,
            residue_count=residue_count,
            classification_rationale=rationale,
        ))

    return objects


def _is_peptide_cofactor_entity(
    raw_entry: dict[str, Any],
    peptide_entity: dict[str, Any],
) -> bool:
    """Return True when a short peptide is acting as a biological cofactor.

    Biological assumption:
    - Small-molecule cofactors are identified by curated CCD IDs.
    - Some short peptides are also bona fide cofactors or activators.
      We only promote these when the entry metadata says so explicitly,
      or for the well-known NS4A cofactor context in HCV NS3/4A protease
      complexes.
    """
    description = _polymer_description(peptide_entity).lower()
    title = str(((raw_entry.get("struct") or {}).get("title") or "")).lower()

    if not description and not title:
        return False

    if "cofactor" in description or "cofactor" in title:
        return True
    if any(keyword in description for keyword in ("co-activator", "coactivator", "activator peptide")):
        return True
    return "ns4a" in description and ("ns3/4a" in title or ("ns3" in title and "protease" in title))


def _apply_peptide_cofactor_context_heuristic(
    raw_entry: dict[str, Any],
    peptide_entities: list[dict[str, Any]],
    bound_objects: list[BoundObject],
) -> list[BoundObject]:
    """Promote explicit peptide cofactors to role='cofactor' without changing type."""
    if not peptide_entities or not bound_objects:
        return bound_objects

    peptide_roles: dict[str, str] = {}
    peptide_descriptions: dict[str, str] = {}
    for entity in peptide_entities:
        entity_id = str(entity.get("rcsb_id") or "")
        if not entity_id or not _is_peptide_cofactor_entity(raw_entry, entity):
            continue
        peptide_roles[entity_id] = "cofactor"
        peptide_descriptions[entity_id] = _polymer_description(entity)

    if not peptide_roles:
        return bound_objects

    updated: list[BoundObject] = []
    for obj in bound_objects:
        if obj.binder_type != "peptide" or not obj.entity_id or obj.entity_id not in peptide_roles:
            updated.append(obj)
            continue
        rationale = obj.classification_rationale
        if rationale:
            rationale += "; "
        rationale += "entry metadata indicates peptide cofactor context"
        updated.append(obj.model_copy(update={
            "role": "cofactor",
            "name": obj.name or peptide_descriptions.get(obj.entity_id) or None,
            "classification_rationale": rationale,
        }))
    return updated


# ---------------------------------------------------------------------------
# Role disambiguation: promote co-ligands and metal-mediated contacts
# ---------------------------------------------------------------------------

def disambiguate_roles(objects: list[BoundObject]) -> list[BoundObject]:
    """Refine BoundObject roles when multiple small-molecule ligands are present.

    Rules applied in order:
    1. If more than one small_molecule is present, keep the first as
       'primary_ligand' and mark the rest as 'co_ligand'.
    2. If a metal_ion shares chain-local context with a small_molecule, flag
       the ion as 'metal_mediated_contact'. Do not blanket-promote every metal
       in entries that merely contain any ligand.

    Returns a new list of (possibly updated) BoundObject instances.
    """
    sm_indices = [i for i, o in enumerate(objects) if o.binder_type == "small_molecule"]
    ligand_chain_ids = {
        chain_id
        for obj in objects
        if obj.binder_type == "small_molecule"
        for chain_id in (obj.chain_ids or [])
        if chain_id
    }

    updated: list[BoundObject] = []
    for i, obj in enumerate(objects):
        if obj.binder_type == "small_molecule" and i in sm_indices[1:]:
            obj = obj.model_copy(update={"role": "co_ligand"})
        if (
            obj.binder_type == "metal_ion"
            and sm_indices
            and ligand_chain_ids
            and set(obj.chain_ids or []).intersection(ligand_chain_ids)
        ):
            rationale = obj.classification_rationale
            if rationale:
                rationale += "; "
            rationale += "shares chain-local context with a small-molecule ligand"
            obj = obj.model_copy(update={
                "role": "metal_mediated_contact",
                "classification_rationale": rationale,
            })
        updated.append(obj)
    return updated


# ---------------------------------------------------------------------------
# InterfaceInfo construction
# ---------------------------------------------------------------------------

def build_interfaces(
    protein_entities: list[dict[str, Any]],
    peptide_entities: list[dict[str, Any]],
) -> list[InterfaceInfo]:
    """Construct InterfaceInfo objects for all biologically relevant pairs.

    Covered cases:
    - Homomeric: 1 protein entity, ≥ 2 chains → symmetric interface
    - Heteromeric: ≥ 2 protein entities → one interface per unique pair
    - Protein–peptide: receptor (first protein entity) × each peptide entity
    """
    interfaces: list[InterfaceInfo] = []

    # Homomeric: one entity, multiple chains
    if len(protein_entities) == 1:
        chains = _polymer_chain_ids(protein_entities[0]) or []
        if len(chains) >= 2:
            interfaces.append(InterfaceInfo(
                entity_id_a=protein_entities[0].get("rcsb_id"),
                entity_id_b=protein_entities[0].get("rcsb_id"),
                chain_ids_a=[chains[0]],
                chain_ids_b=chains[1:],
                interface_type="protein_protein",
                is_symmetric=True,
                is_hetero=False,
            ))

    # Heteromeric: all unique ordered pairs of protein entities
    elif len(protein_entities) >= 2:
        for i, ent_a in enumerate(protein_entities):
            for ent_b in protein_entities[i + 1:]:
                ca = _polymer_chain_ids(ent_a) or []
                cb = _polymer_chain_ids(ent_b) or []
                interfaces.append(InterfaceInfo(
                    entity_id_a=ent_a.get("rcsb_id"),
                    entity_id_b=ent_b.get("rcsb_id"),
                    chain_ids_a=ca,
                    chain_ids_b=cb,
                    interface_type="protein_protein",
                    is_symmetric=False,
                    is_hetero=True,
                ))

    # Protein–peptide (receptor = first protein entity)
    if protein_entities and peptide_entities:
        receptor = protein_entities[0]
        rc = _polymer_chain_ids(receptor) or []
        for pep in peptide_entities:
            pc = _polymer_chain_ids(pep) or []
            interfaces.append(InterfaceInfo(
                entity_id_a=receptor.get("rcsb_id"),
                entity_id_b=pep.get("rcsb_id"),
                chain_ids_a=rc,
                chain_ids_b=pc,
                interface_type="protein_peptide",
                is_symmetric=False,
                is_hetero=True,
            ))

    return interfaces


# ---------------------------------------------------------------------------
# AssemblyInfo construction
# ---------------------------------------------------------------------------

def build_assembly_info(raw_entry: dict[str, Any]) -> AssemblyInfo:
    """Extract assembly metadata from a raw RCSB GraphQL entry dict.

    Uses `assemblies[0]` as the preferred biological assembly.
    Falls back to rcsb_entry_info.assembly_count when assemblies list is absent.
    """
    entry_info = raw_entry.get("rcsb_entry_info") or {}
    assembly_count: int | None = entry_info.get("assembly_count")

    assemblies: list[dict[str, Any]] = raw_entry.get("assemblies") or []
    if not assemblies:
        # No assembly data in query response — use count only
        au_is_bio: bool | None = (
            True if assembly_count == 1
            else (False if assembly_count is not None and assembly_count > 1 else None)
        )
        return AssemblyInfo(
            assembly_count=assembly_count,
            asymmetric_unit_is_biological=au_is_bio,
        )

    pref = assemblies[0]
    struct_asm  = pref.get("pdbx_struct_assembly") or {}
    asm_info    = pref.get("rcsb_assembly_info") or {}

    oligo_details = struct_asm.get("oligomeric_details") or None
    oligo_count   = struct_asm.get("oligomeric_count") or None
    poly_count    = asm_info.get("polymer_entity_count") or None
    poly_prot     = asm_info.get("polymer_entity_count_protein") or None

    # Infer homo/hetero from RCSB text label
    is_homo: bool | None = None
    if oligo_details:
        lower = oligo_details.lower()
        if "homo" in lower:
            is_homo = True
        elif "hetero" in lower:
            is_homo = False

    # AU = biological assembly when only 1 assembly is annotated
    au_is_bio = (assembly_count == 1) if assembly_count is not None else None

    return AssemblyInfo(
        assembly_count=assembly_count,
        preferred_id=pref.get("rcsb_id") or None,
        oligomeric_details=oligo_details,
        oligomeric_count=int(oligo_count) if oligo_count is not None else None,
        is_homo_oligomeric=is_homo,
        polymer_entity_count=int(poly_count) if poly_count is not None else None,
        polymer_entity_count_protein=int(poly_prot) if poly_prot is not None else None,
        asymmetric_unit_is_biological=au_is_bio,
    )


# ---------------------------------------------------------------------------
# Membrane context detection
# ---------------------------------------------------------------------------

def detect_membrane_context(raw_entry: dict[str, Any]) -> bool:
    """Return True if struct_keywords suggest a membrane-protein context.

    Checks pdbx_keywords and the free-text keywords field for membrane-
    related terms (case-insensitive substring match).  This is heuristic:
    a positive result means the flag should be set; a negative does NOT
    guarantee the protein is soluble.
    """
    kw_block = raw_entry.get("struct_keywords") or {}
    fields = [
        (kw_block.get("pdbx_keywords") or "").lower(),
        (kw_block.get("text") or "").lower(),
    ]
    for field in fields:
        for kw in _MEMBRANE_KEYWORDS:
            if kw in field:
                return True
    return False


def _apply_covalent_context_heuristic(
    raw_entry: dict[str, Any],
    bound_objects: list[BoundObject],
) -> list[BoundObject]:
    """Promote a likely bound object to covalent when entry metadata is explicit.

    This is intentionally conservative: it only triggers when the entry title
    contains clear covalent-context language and no object is already marked
    covalent from a warhead/descriptor signal.
    """
    if any(obj.is_covalent is True for obj in bound_objects):
        return bound_objects

    title = ((raw_entry.get("struct") or {}).get("title") or "").lower()
    if not any(keyword in title for keyword in _COVALENT_TITLE_KEYWORDS):
        return bound_objects

    updated: list[BoundObject] = []
    promoted = False
    for obj in bound_objects:
        if not promoted and obj.binder_type in {"small_molecule", "cofactor"}:
            rationale = obj.classification_rationale
            if rationale:
                rationale += "; "
            rationale += "entry title indicates explicit covalent context"
            obj = obj.model_copy(update={
                "is_covalent": True,
                "classification_rationale": rationale,
            })
            promoted = True
        updated.append(obj)
    return updated


def _apply_missing_context_heuristics(
    raw_entry: dict[str, Any],
    protein_entities: list[dict[str, Any]],
    bound_objects: list[BoundObject],
) -> list[BoundObject]:
    """Recover obvious missing glycans/metals from descriptive metadata.

    This is a conservative fallback for entries whose GraphQL payload omits
    branched entities or nonpolymer ions and where coordinate-side parsing is
    unavailable. Inferred objects are kept explicit and rationale-tagged.
    """
    title = ((raw_entry.get("struct") or {}).get("title") or "").lower()
    descriptions = " ".join(
        str(((ent.get("rcsb_polymer_entity") or {}).get("pdbx_description") or "")).lower()
        for ent in protein_entities
    )
    text = " ".join(part for part in (title, descriptions) if part)

    updated = list(bound_objects)

    has_glycan = any(obj.binder_type == "glycan" for obj in updated)
    if not has_glycan and any(keyword in text for keyword in _GLYCAN_CONTEXT_KEYWORDS):
        updated.append(BoundObject(
            binder_type="glycan",
            role="co_ligand",
            classification_rationale=(
                "entry title/entity descriptions indicate glycosylation; "
                "explicit glycan components unavailable from current metadata"
            ),
        ))

    has_metal = any(obj.binder_type == "metal_ion" for obj in updated)
    if not has_metal:
        for keyword, comp_id in _METAL_CONTEXT_KEYWORDS.items():
            if keyword in text:
                updated.append(BoundObject(
                    comp_id=comp_id,
                    name=comp_id,
                    binder_type="metal_ion",
                    role="metal_mediated_contact",
                    classification_rationale=(
                        f"entry title/entity descriptions mention {keyword!r}; "
                        "explicit metal components unavailable from current metadata"
                    ),
                ))
                break

    return updated


# ---------------------------------------------------------------------------
# Top-level per-entry classify function
# ---------------------------------------------------------------------------

def classify_entry(
    raw_entry: dict[str, Any],
    chem_descriptors: dict[str, dict[str, str]] | None = None,
) -> dict[str, Any]:
    """Run the full classification pipeline on one raw RCSB entry dict.

    Returns a dict with keys:
        protein_entities   list[dict]
        peptide_entities   list[dict]
        other_poly         list[dict]
        nonpoly_entities   list[dict]
        bound_objects      list[BoundObject]
        interfaces         list[InterfaceInfo]
        assembly_info      AssemblyInfo
        is_homo_oligomeric bool | None
        oligomeric_state   str
        task_type          str
        membrane_context   bool
    """
    poly_entities:    list[dict[str, Any]] = raw_entry.get("polymer_entities") or []
    nonpoly_entities: list[dict[str, Any]] = raw_entry.get("nonpolymer_entities") or []
    poly_entities, nonpoly_entities = _apply_mmcif_supplement(
        raw_entry,
        poly_entities,
        nonpoly_entities,
    )

    protein_entities: list[dict[str, Any]] = []
    peptide_entities: list[dict[str, Any]] = []
    other_poly:       list[dict[str, Any]] = []

    for ent in poly_entities:
        kind = classify_polymer_entity(ent)
        if kind == "protein":
            protein_entities.append(ent)
        elif kind == "peptide":
            peptide_entities.append(ent)
        else:
            other_poly.append(ent)

    bound_objects = build_bound_objects(
        nonpoly_entities,
        peptide_entities,
        other_poly_entities=other_poly,
        chem_descriptors=chem_descriptors,
    )
    bound_objects = _apply_peptide_cofactor_context_heuristic(
        raw_entry,
        peptide_entities,
        bound_objects,
    )
    bound_objects = disambiguate_roles(bound_objects)
    bound_objects = _apply_covalent_context_heuristic(raw_entry, bound_objects)
    bound_objects = _apply_missing_context_heuristics(
        raw_entry,
        protein_entities,
        bound_objects,
    )

    # Detect fusion-construct peptides: a protein-length entity whose
    # pdbx_description mentions "peptide" alongside another protein name
    # (e.g. "mim2 peptide,HLA class II histocompatibility antigen, DP beta 1
    # chain" in 4P57).  If no peptide was already found, emit a virtual one.
    if not peptide_entities and not any(b.binder_type == "peptide" for b in bound_objects):
        for ent in protein_entities:
            desc = (
                (ent.get("rcsb_polymer_entity") or {}).get("pdbx_description") or ""
            ).lower()
            if not desc:
                continue
            parts = [p.strip() for p in desc.split(",")]
            has_peptide_part = any("peptide" in p for p in parts)
            has_nonpeptide_part = any(p and "peptide" not in p for p in parts)
            if has_peptide_part and has_nonpeptide_part:
                bound_objects.append(BoundObject(
                    entity_id=ent.get("rcsb_id"),
                    binder_type="peptide",
                    role="primary_ligand",
                    classification_rationale=(
                        f"Peptide embedded in fusion construct: {desc}"
                    ),
                ))
                break  # only emit one virtual peptide

    interfaces = build_interfaces(protein_entities, peptide_entities)
    assembly_info = build_assembly_info(raw_entry)
    is_homo, oligo_state = infer_oligomeric_state(protein_entities)

    # Determine task_type (same heuristic as before, now using classify output)
    ligand_objects = [b for b in bound_objects if b.binder_type == "small_molecule"]
    peptide_objects = [b for b in bound_objects if b.binder_type == "peptide"]
    if ligand_objects and protein_entities:
        task_type = "protein_ligand"
    elif len(protein_entities) >= 2 or peptide_objects:
        task_type = "protein_protein"
    else:
        task_type = "protein_ligand"   # fallback

    membrane_context = detect_membrane_context(raw_entry)

    return {
        "protein_entities":   protein_entities,
        "peptide_entities":   peptide_entities,
        "other_poly":         other_poly,
        "nonpoly_entities":   nonpoly_entities,
        "bound_objects":      bound_objects,
        "interfaces":         interfaces,
        "assembly_info":      assembly_info,
        "is_homo_oligomeric": is_homo,
        "oligomeric_state":   oligo_state,
        "task_type":          task_type,
        "membrane_context":   membrane_context,
    }
