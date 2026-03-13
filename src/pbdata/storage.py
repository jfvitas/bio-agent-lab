"""Shared storage-layout and cache-validation helpers.

Assumption:
- For immutable identifier-keyed artifacts such as RCSB entry JSON and PDB/mmCIF
  downloads, a locally valid file is treated as current enough to reuse.
- If a file is missing, malformed, incomplete, or clearly mismatched to the
  requested identifier, it is deleted and replaced.
"""

from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

import gemmi


Validator = Callable[[Path], bool]


@dataclass(frozen=True)
class StorageLayout:
    """All data-layer paths rooted under one user-selected parent directory."""

    root: Path

    @property
    def data_dir(self) -> Path:
        return self.root / "data"

    @property
    def data_sources_dir(self) -> Path:
        return self.root / "data_sources"

    @property
    def workspace_structures_dir(self) -> Path:
        return self.root / "structures"

    @property
    def clean_structures_dir(self) -> Path:
        return self.root / "clean_structures"

    @property
    def workspace_features_dir(self) -> Path:
        return self.root / "features"

    @property
    def workspace_graphs_dir(self) -> Path:
        return self.root / "graphs"

    @property
    def workspace_datasets_dir(self) -> Path:
        return self.root / "datasets"

    @property
    def workspace_metadata_dir(self) -> Path:
        return self.root / "metadata"

    @property
    def workspace_logs_dir(self) -> Path:
        return self.root / "logs"

    @property
    def rosetta_outputs_dir(self) -> Path:
        return self.root / "rosetta_outputs"

    @property
    def artifacts_dir(self) -> Path:
        return self.root / "artifacts"

    @property
    def interim_dir(self) -> Path:
        return self.data_dir / "interim"

    @property
    def raw_rcsb_dir(self) -> Path:
        return self.data_dir / "raw" / "rcsb"

    @property
    def raw_skempi_dir(self) -> Path:
        return self.data_dir / "raw" / "skempi"

    @property
    def raw_bindingdb_dir(self) -> Path:
        return self.data_dir / "raw" / "bindingdb"

    @property
    def processed_rcsb_dir(self) -> Path:
        return self.data_dir / "processed" / "rcsb"

    @property
    def audit_dir(self) -> Path:
        return self.data_dir / "audit"

    @property
    def reports_dir(self) -> Path:
        return self.data_dir / "reports"

    @property
    def feature_reports_dir(self) -> Path:
        return self.artifacts_dir / "reports"

    @property
    def splits_dir(self) -> Path:
        return self.data_dir / "splits"

    @property
    def catalog_path(self) -> Path:
        return self.data_dir / "catalog" / "download_manifest.csv"

    @property
    def stage_state_dir(self) -> Path:
        return self.data_dir / "catalog" / "stage_state"

    @property
    def source_state_dir(self) -> Path:
        return self.data_dir / "catalog" / "source_state"

    @property
    def artifact_manifests_dir(self) -> Path:
        return self.artifacts_dir / "manifests"

    @property
    def artifact_logs_dir(self) -> Path:
        return self.artifacts_dir / "logs"

    @property
    def artifact_caches_dir(self) -> Path:
        return self.artifacts_dir / "caches"

    @property
    def canonical_artifacts_dir(self) -> Path:
        return self.artifacts_dir / "canonical"

    @property
    def prepared_structures_artifacts_dir(self) -> Path:
        return self.artifacts_dir / "prepared_structures"

    @property
    def base_features_artifacts_dir(self) -> Path:
        return self.artifacts_dir / "base_features"

    @property
    def site_physics_artifacts_dir(self) -> Path:
        return self.artifacts_dir / "site_physics"

    @property
    def graphs_artifacts_dir(self) -> Path:
        return self.artifacts_dir / "graphs"

    @property
    def training_examples_artifacts_dir(self) -> Path:
        return self.artifacts_dir / "training_examples"

    @property
    def site_envs_artifacts_dir(self) -> Path:
        return self.artifacts_dir / "site_envs"

    @property
    def archetypes_artifacts_dir(self) -> Path:
        return self.artifacts_dir / "archetypes"

    @property
    def external_analysis_artifacts_dir(self) -> Path:
        return self.artifacts_dir / "external_analysis"

    @property
    def physics_targets_artifacts_dir(self) -> Path:
        return self.artifacts_dir / "physics_targets"

    @property
    def surrogate_training_artifacts_dir(self) -> Path:
        return self.artifacts_dir / "surrogate_training"

    @property
    def extracted_dir(self) -> Path:
        return self.data_dir / "extracted"

    @property
    def structures_rcsb_dir(self) -> Path:
        return self.data_dir / "structures" / "rcsb"

    @property
    def graph_dir(self) -> Path:
        return self.data_dir / "graph"

    @property
    def conformations_dir(self) -> Path:
        return self.data_dir / "conformations"

    @property
    def prediction_dir(self) -> Path:
        return self.data_dir / "prediction"

    @property
    def models_dir(self) -> Path:
        return self.data_dir / "models"

    @property
    def features_dir(self) -> Path:
        return self.data_dir / "features"

    @property
    def structural_feature_exports_dir(self) -> Path:
        return self.root / "features" / "structural_features"

    @property
    def ligand_feature_exports_dir(self) -> Path:
        return self.root / "features" / "ligand_features"

    @property
    def interface_feature_exports_dir(self) -> Path:
        return self.root / "features" / "interface_features"

    @property
    def graph_feature_exports_dir(self) -> Path:
        return self.root / "features" / "graph_features"

    @property
    def microstates_dir(self) -> Path:
        return self.features_dir / "microstates"

    @property
    def physics_dir(self) -> Path:
        return self.features_dir / "physics"

    @property
    def microstate_refinement_dir(self) -> Path:
        return self.features_dir / "microstate_refinement"

    @property
    def mm_jobs_dir(self) -> Path:
        return self.features_dir / "mm_jobs"

    @property
    def training_dir(self) -> Path:
        return self.data_dir / "training_examples"

    @property
    def identity_dir(self) -> Path:
        return self.data_dir / "identity"

    @property
    def releases_dir(self) -> Path:
        return self.data_dir / "releases"

    @property
    def qa_dir(self) -> Path:
        return self.data_dir / "qa"

    @property
    def risk_dir(self) -> Path:
        return self.data_dir / "risk"


def resolve_storage_root(storage_root: str | Path | None) -> Path:
    """Resolve a user-supplied storage root to an absolute local path."""
    if storage_root is None:
        return Path.cwd().resolve()
    return Path(storage_root).expanduser().resolve()


def build_storage_layout(storage_root: str | Path | None) -> StorageLayout:
    """Construct the canonical workspace path layout for a storage root."""
    return StorageLayout(root=resolve_storage_root(storage_root))


def validate_rcsb_raw_json(path: Path, *, expected_pdb_id: str | None = None) -> bool:
    """Return True when a cached RCSB JSON payload is structurally usable."""
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return False
    if not isinstance(raw, dict):
        return False
    pdb_id = str(raw.get("rcsb_id") or "").upper()
    if expected_pdb_id and pdb_id != expected_pdb_id.upper():
        return False
    if not pdb_id:
        return False
    required_blocks = ("rcsb_entry_info", "polymer_entities", "nonpolymer_entities")
    return all(key in raw for key in required_blocks)


def validate_skempi_csv(path: Path) -> bool:
    """Return True when a SKEMPI CSV has the required delimiter and headers."""
    try:
        with path.open(encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle, delimiter=";")
            headers = reader.fieldnames or []
    except Exception:
        return False
    required = {"#Pdb", "#Mutation(s)_cleaned"}
    return required.issubset(set(headers))


def validate_bindingdb_raw_json(path: Path, *, expected_pdb_id: str | None = None) -> bool:
    """Return True when a cached BindingDB JSON payload matches the expected shape."""
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return False
    if not isinstance(raw, dict):
        return False
    pdb_id = str(raw.get("pdb_id") or raw.get("pdbID") or "").upper()
    if expected_pdb_id and pdb_id != expected_pdb_id.upper():
        return False
    affinities = raw.get("affinities") or raw.get("monomers") or []
    return bool(pdb_id) and isinstance(affinities, list)


def validate_mmcif_file(path: Path) -> bool:
    """Return True when a local mmCIF file exists and parses with gemmi."""
    try:
        if not path.exists() or path.stat().st_size <= 0:
            return False
        gemmi.cif.read_file(str(path)).sole_block()
        return True
    except Exception:
        return False


def validate_pdb_file(path: Path) -> bool:
    """Return True when a local PDB file looks structurally valid."""
    try:
        if not path.exists() or path.stat().st_size <= 0:
            return False
        with path.open(encoding="utf-8") as handle:
            for _ in range(10):
                line = handle.readline()
                if not line:
                    break
                if line.startswith(("HEADER", "TITLE", "ATOM  ", "HETATM", "MODEL ")):
                    return True
    except Exception:
        return False
    return False


def reuse_existing_file(
    path: Path,
    *,
    validator: Validator,
) -> bool:
    """Return True if a local file is valid for reuse; delete it otherwise."""
    if not path.exists():
        return False
    if validator(path):
        return True
    path.unlink(missing_ok=True)
    return False
