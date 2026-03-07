"""PDBbind source adapter (stub).

PDBbind (https://www.pdbbind-plus.org.cn/) is a curated database of
experimentally measured binding affinities for protein-ligand, protein-
protein, and protein-nucleic acid complexes taken from the PDB.

Status: STUB — not yet implemented.

Implementation notes for future work:
- PDBbind requires free registration to download the full dataset.
  Direct programmatic download is not publicly available.
  Recommended workflow:
    1. Register at https://www.pdbbind-plus.org.cn/
    2. Download the 'general set' or 'refined set' ZIP archives.
    3. Point this adapter at the extracted local directory via local_dir.
- Directory layout (PDBbind 2020 general set):
    <local_dir>/
      index/INDEX_general_PL_data.2020   — tab-delimited affinity data
      index/INDEX_general_PL_name.2020   — ligand name mapping
      <PDB_ID>/                          — per-complex subdirectories
        <PDB_ID>_protein.pdb
        <PDB_ID>_ligand.mol2
        <PDB_ID>_pocket.pdb
- Normalisation approach:
    * Parse INDEX file for pdb_id, resolution, release_year, and affinity
      (Kd, Ki, or IC50 in nM)
    * task_type = "protein_ligand"
    * Receptor sequence from <PDB_ID>_protein.pdb (or RCSB join)
    * Ligand SMILES from <PDB_ID>_ligand.mol2 or RCSB chem-comp
    * assay_type inferred from affinity label (Kd / Ki / IC50)
    * assay_value_standardized in nM

TODO:
  1. Parse INDEX_general_PL_data files
  2. Parse MOL2 ligand files or join with RCSB for SMILES
  3. Implement normalize_record()
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from pbdata.schemas.canonical_sample import CanonicalBindingSample
from pbdata.sources.base import BaseAdapter


class PDBbindAdapter(BaseAdapter):
    """Adapter for PDBbind binding-affinity data (not yet implemented).

    Args:
        local_dir: Path to an extracted PDBbind dataset directory.
                   Required for any actual data loading.
    """

    def __init__(self, local_dir: Path | None = None) -> None:
        self._local_dir = local_dir

    @property
    def source_name(self) -> str:
        return "PDBbind"

    def fetch_metadata(self, record_id: str) -> dict[str, Any]:
        raise NotImplementedError(
            "PDBbind adapter is not yet implemented.  "
            "PDBbind requires manual download (registration required).  "
            "See module docstring for instructions."
        )

    def normalize_record(self, raw: dict[str, Any]) -> CanonicalBindingSample:
        raise NotImplementedError(
            "PDBbind adapter is not yet implemented.  "
            "See module docstring for implementation notes."
        )
