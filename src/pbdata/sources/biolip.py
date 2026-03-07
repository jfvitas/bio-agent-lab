"""BioLiP source adapter (stub).

BioLiP (https://zhanggroup.org/BioLiP/) is a semi-manually curated
database of biologically relevant ligand-protein interactions derived
from the PDB.

Status: STUB — not yet implemented.

Implementation notes for future work:
- Data is available as weekly-updated flat files:
    https://zhanggroup.org/BioLiP/weekly/
  Each archive contains receptor.pdb, ligand.pdb, and BioLiP.txt (metadata).
- BioLiP.txt columns (tab-delimited):
    PDB_ID | receptor_chain | ligand_chain | ligand_ID | ligand_serial |
    binding_site_residues | catalytic_site_residues | EC_number |
    GO_terms | Pfam_IDs | ...
- Normalisation approach:
    * task_type = "protein_ligand"
    * Receptor sequence from receptor.pdb (or from RCSB join on PDB ID)
    * Ligand SMILES from RCSB chem-comp (join on ligand_ID)
    * Binding-site residue list to be stored in provenance
- Quality filter: BioLiP already excludes crystallisation artefacts;
  the _EXCLUDED_COMPS list in rcsb.py overlaps significantly.

TODO:
  1. Download and parse BioLiP.txt
  2. Join with RCSB sequences / SMILES (or parse PDB files directly)
  3. Implement normalize_record()
"""

from __future__ import annotations

from typing import Any

from pbdata.schemas.canonical_sample import CanonicalBindingSample
from pbdata.sources.base import BaseAdapter


class BioLiPAdapter(BaseAdapter):
    """Adapter for BioLiP protein-ligand interaction data (not yet implemented)."""

    @property
    def source_name(self) -> str:
        return "BioLiP"

    def fetch_metadata(self, record_id: str) -> dict[str, Any]:
        raise NotImplementedError(
            "BioLiP adapter is not yet implemented.  "
            "See module docstring for implementation notes."
        )

    def normalize_record(self, raw: dict[str, Any]) -> CanonicalBindingSample:
        raise NotImplementedError(
            "BioLiP adapter is not yet implemented.  "
            "See module docstring for implementation notes."
        )
