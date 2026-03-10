"""Tkinter GUI for the pbdata pipeline.

Layout
------
Header bar   : Title + subtitle
Left column  : Tabbed notebook (Sources / Search Criteria / Pipeline Options)
Right column : Pipeline stages grouped by phase, with data overview at top
Bottom row   : Live log panel

The ingest stage is handled directly in Python (no subprocess) so that
the GUI can intercept the entry count and show a confirmation dialog
before any data is downloaded.  All other stages are run via subprocess
so their stdout streams naturally to the log.
"""

from __future__ import annotations

import csv
import json
import os
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, TypeVar

import tkinter as tk
from tkinter import filedialog, messagebox, scrolledtext, ttk

import yaml
from pbdata.criteria import (
    EXPERIMENTAL_METHODS,
    RESOLUTION_OPTIONS,
    SearchCriteria,
    load_criteria,
    resolution_label_to_value,
    resolution_value_to_label,
    save_criteria,
)
from pbdata.storage import (
    build_storage_layout,
    reuse_existing_file,
    validate_rcsb_raw_json,
    validate_skempi_csv,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_SOURCES = ["rcsb", "bindingdb", "chembl", "pdbbind", "biolip", "skempi"]
_SOURCE_PATH_FIELDS = {
    "bindingdb": "local_dir",
    "pdbbind": "local_dir",
    "biolip": "local_dir",
    "skempi": "local_path",
}
_STRUCTURE_MIRROR_OPTIONS = ["rcsb", "pdbj"]

_SOURCE_DESCRIPTIONS: dict[str, str] = {
    "rcsb":      "RCSB PDB — structural metadata via Search & GraphQL",
    "chembl":    "ChEMBL — bioactivity data (Kd, Ki, IC50) via REST API",
    "bindingdb": "BindingDB — binding affinity by PDB ID",
    "skempi":    "SKEMPI v2 — protein-protein mutation ddG dataset",
    "pdbbind":   "PDBbind — curated protein-ligand affinities (local)",
    "biolip":    "BioLiP — biologically relevant ligand-protein (local)",
}

_SOURCE_INGEST_NOTES: dict[str, str] = {
    "rcsb":      "Searches RCSB and downloads raw metadata JSON.",
    "skempi":    "Downloads SKEMPI v2 CSV (~3 MB).",
    "chembl":    "Enrichment source — queried during Extract stage.",
    "bindingdb": "Enrichment source — queried during Extract stage; optional local cache dir supported.",
    "pdbbind":   "Local files — parsed during Extract stage.",
    "biolip":    "Local files — parsed during Extract stage.",
}

# All stages that run via subprocess (ingest is special-cased).
# Keep "extract" first — tests assert _SUBPROCESS_STAGES[0] == "extract".
_SUBPROCESS_STAGES = [
    "extract", "normalize", "audit", "report",
    "setup-workspace", "harvest-metadata", "build-structural-graphs", "engineer-dataset",
    "report-bias",
    "build-conformational-states", "build-graph", "build-microstates", "build-physics-features", "build-microstate-refinement", "build-mm-job-manifests", "run-mm-jobs", "run-feature-pipeline", "export-analysis-queue", "ingest-physics-results", "train-site-physics-surrogate", "build-features", "build-training-examples", "build-splits", "train-baseline-model", "evaluate-baseline-model", "build-custom-training-set", "build-release", "run-scenario-tests",
]

# Pipeline groups for the UI
_PIPELINE_GROUPS: list[tuple[str, list[tuple[str, str]]]] = [
    ("Workflow Engine", [
        ("setup-workspace", "Setup Workspace"),
        ("harvest-metadata", "Harvest Metadata"),
    ]),
    ("Data Acquisition", [
        ("ingest", "Ingest Sources"),
    ]),
    ("Processing", [
        ("extract", "Extract Multi-Table"),
        ("normalize", "Normalize Records"),
    ]),
    ("Quality & Analysis", [
        ("audit", "Audit Quality"),
        ("report", "Generate Report"),
        ("report-bias", "Report Bias"),
    ]),
    ("ML Pipeline", [
        ("build-structural-graphs", "Build Structural Graphs"),
        ("build-conformational-states", "Build Conformational States"),
        ("build-graph", "Build Graph"),
        ("build-microstates", "Build Microstates"),
        ("build-physics-features", "Build Physics Features"),
        ("build-microstate-refinement", "Build Microstate Refinement"),
        ("build-mm-job-manifests", "Build MM Job Manifests"),
        ("run-mm-jobs", "Run MM Jobs"),
        ("run-feature-pipeline", "Run Site-Centric Feature Pipeline"),
        ("export-analysis-queue", "Export Analysis Queue"),
        ("ingest-physics-results", "Ingest Physics Results"),
        ("train-site-physics-surrogate", "Train Site-Physics Surrogate"),
        ("build-features", "Build Features"),
        ("build-training-examples", "Build Training Examples"),
        ("build-splits", "Build Splits"),
        ("train-baseline-model", "Train Baseline Model"),
        ("evaluate-baseline-model", "Evaluate Baseline Model"),
        ("build-custom-training-set", "Build Custom Training Set"),
        ("engineer-dataset", "Engineer Dataset"),
        ("build-release", "Build Release Snapshot"),
        ("run-scenario-tests", "Run Scenario Tests"),
    ]),
]

_ALL_STAGE_KEYS = [key for _, stages in _PIPELINE_GROUPS for key, _ in stages]

_CRITERIA_PATH = Path("configs/criteria.yaml")
_SOURCES_CFG   = Path("configs/sources.yaml")

_STATUS_COLORS = {
    "idle":      "#888888",
    "running":   "#e6a817",
    "done":      "#4caf50",
    "error":     "#e53935",
    "skipped":   "#607d8b",
}

_HEADER_BG   = "#1a237e"
_HEADER_FG   = "#ffffff"
_ACCENT_BG   = "#283593"
_SECTION_FG  = "#1a237e"
_LOG_BG      = "#1e1e1e"
_LOG_FG      = "#d4d4d4"
_OVERVIEW_BG = "#f5f5f5"
_APP_BG      = "#f3f6fb"
_CARD_BG     = "#ffffff"
_CARD_BORDER = "#d7deea"
_MUTED_FG    = "#5f6b7a"
_SUCCESS_FG  = "#0f766e"
_WARNING_FG  = "#b45309"
_ERROR_FG    = "#b91c1c"
_REVIEW_ISSUE_OPTIONS = [
    "All",
    "missing_structure_file",
    "no_assay_data",
    "non_high_confidence_fields",
    "missing_ligand_descriptors",
    "no_matched_interface",
    "ambiguous_mutation_context",
    "source_value_conflict",
    "non_high_confidence_assay_fields",
]
_REVIEW_CONFIDENCE_OPTIONS = ["All", "Non-high", "Medium", "Low"]
_FILTERED_REVIEW_CSV_NAME = "master_pdb_review_filtered.csv"
_PIPELINE_EXECUTION_MODES = ["legacy", "site-centric", "hybrid"]

_T = TypeVar("_T")


# ---------------------------------------------------------------------------
# Sources config helpers (read/write sources.yaml)
# ---------------------------------------------------------------------------

def _load_sources_config() -> tuple[dict[str, bool], dict[str, str], str]:
    if not _SOURCES_CFG.exists():
        return (
            {s: False for s in _SOURCES},
            {s: "" for s in _SOURCE_PATH_FIELDS},
            str(Path.cwd()),
        )
    with _SOURCES_CFG.open() as f:
        raw: dict[str, Any] = yaml.safe_load(f) or {}
    sources = raw.get("sources", {})
    enabled = {s: bool(sources.get(s, {}).get("enabled", False)) for s in _SOURCES}
    paths = {
        src: str((sources.get(src, {}).get("extra", {}) or {}).get(field, "") or "")
        for src, field in _SOURCE_PATH_FIELDS.items()
    }
    storage_root = str(raw.get("storage_root") or Path.cwd())
    return enabled, paths, storage_root


def _save_sources_config(
    enabled: dict[str, bool],
    paths: dict[str, str],
    *,
    storage_root: str,
    structure_mirror: str = "rcsb",
) -> None:
    _SOURCES_CFG.parent.mkdir(parents=True, exist_ok=True)
    if _SOURCES_CFG.exists():
        with _SOURCES_CFG.open() as f:
            raw: dict[str, Any] = yaml.safe_load(f) or {}
    else:
        raw = {}
    raw["storage_root"] = storage_root
    sources: dict[str, Any] = raw.setdefault("sources", {})
    for src, val in enabled.items():
        src_cfg = sources.setdefault(src, {})
        src_cfg["enabled"] = val
        if src in _SOURCE_PATH_FIELDS:
            extra = src_cfg.setdefault("extra", {})
            field = _SOURCE_PATH_FIELDS[src]
            path_value = paths.get(src, "").strip()
            if path_value:
                extra[field] = path_value
            else:
                extra.pop(field, None)
    rcsb_extra = sources.setdefault("rcsb", {}).setdefault("extra", {})
    rcsb_extra["structure_mirror"] = (
        structure_mirror if structure_mirror in _STRUCTURE_MIRROR_OPTIONS else "rcsb"
    )
    with _SOURCES_CFG.open("w") as f:
        yaml.safe_dump(raw, f, default_flow_style=False)


def _load_structure_mirror() -> str:
    if not _SOURCES_CFG.exists():
        return "rcsb"
    with _SOURCES_CFG.open() as f:
        raw: dict[str, Any] = yaml.safe_load(f) or {}
    value = str((((raw.get("sources") or {}).get("rcsb") or {}).get("extra") or {}).get("structure_mirror") or "rcsb")
    value = value.strip().lower()
    return value if value in _STRUCTURE_MIRROR_OPTIONS else "rcsb"


def _validate_source_path(src: str, path_value: str) -> tuple[str, str]:
    path_value = path_value.strip()
    if src == "chembl":
        return "ready", "Live API enrichment will be queried during Extract."
    if src == "bindingdb":
        if not path_value:
            return "ready", "No local cache configured. Extract will use the live BindingDB API."
        path = Path(path_value)
        if not path.exists() or not path.is_dir():
            return "error", "Configured BindingDB cache directory does not exist."
        json_count = len(list(path.glob("*.json")))
        return "ready", f"BindingDB cache directory is available ({json_count} JSON file(s) detected)."
    if src == "pdbbind":
        if not path_value:
            return "error", "PDBbind requires a local dataset directory."
        try:
            from pbdata.sources.pdbbind import load_pdbbind_index

            count = len(load_pdbbind_index(Path(path_value)))
            return "ready", f"PDBbind index parsed successfully ({count} row(s))."
        except Exception as exc:
            return "error", f"PDBbind directory is not usable: {exc}"
    if src == "biolip":
        if not path_value:
            return "error", "BioLiP requires a local dataset directory."
        try:
            from pbdata.sources.biolip import load_biolip_rows

            count = len(load_biolip_rows(Path(path_value)))
            return "ready", f"BioLiP file parsed successfully ({count} row(s))."
        except Exception as exc:
            return "error", f"BioLiP directory is not usable: {exc}"
    if src == "skempi":
        if not path_value:
            return "ready", "No local SKEMPI file configured. Ingest will download the official CSV."
        path = Path(path_value)
        if not path.exists():
            return "error", "Configured SKEMPI CSV file does not exist."
        if validate_skempi_csv(path):
            return "ready", "Configured SKEMPI CSV validated successfully."
        return "error", "Configured SKEMPI CSV failed validation."
    return "ready", "No additional validation available."


def _call_on_tk_thread(root: Any, fn: Callable[[], _T]) -> _T:
    """Run a callable on the Tk event thread and return its result."""
    result: dict[str, Any] = {}
    event = threading.Event()

    def _invoke() -> None:
        try:
            result["value"] = fn()
        except Exception as exc:  # pragma: no cover - exercised via re-raise path
            result["error"] = exc
        finally:
            event.set()

    root.after(0, _invoke)
    event.wait()
    if "error" in result:
        raise result["error"]
    return result["value"]


def _count_files(directory: Path, pattern: str = "*.json") -> int:
    """Count files matching a glob pattern in a directory."""
    if not directory.exists():
        return 0
    return sum(1 for _ in directory.glob(pattern))


def _mousewheel_units(event: Any) -> int:
    if getattr(event, "delta", 0):
        return -1 * int(event.delta / 120)
    return -1 if getattr(event, "num", None) == 4 else 1


def _load_csv_dict_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _load_json_dict(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return raw if isinstance(raw, dict) else {}


def _row_has_non_high_confidence(row: dict[str, str]) -> bool:
    values: list[str] = []
    for field in ("field_confidence_json", "assay_field_confidence_json"):
        raw = str(row.get(field) or "").strip()
        if not raw:
            continue
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, dict):
            values.extend(str(value or "").lower() for value in parsed.values())
    return any(value not in {"", "high"} for value in values)


def _row_has_confidence_level(row: dict[str, str], level: str) -> bool:
    raw = str(row.get("assay_field_confidence_json") or row.get("field_confidence_json") or "").strip()
    if not raw:
        return False
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return False
    if not isinstance(parsed, dict):
        return False
    return any(str(value or "").lower() == level for value in parsed.values())


def build_filtered_review_rows(
    master_rows: list[dict[str, str]],
    pair_rows: list[dict[str, str]],
    issue_rows: list[dict[str, str]],
    *,
    pdb_query: str = "",
    pair_query: str = "",
    issue_type: str = "All",
    confidence_filter: str = "All",
    conflict_only: bool = False,
    mutation_ambiguous_only: bool = False,
    metal_only: bool = False,
    cofactor_only: bool = False,
    glycan_only: bool = False,
) -> list[dict[str, str]]:
    entry_by_pdb = {
        str(row.get("pdb_id") or ""): row
        for row in master_rows
        if row.get("pdb_id")
    }
    issues_by_key: dict[tuple[str, str], list[dict[str, str]]] = {}
    issues_by_pdb: dict[str, list[dict[str, str]]] = {}
    for row in issue_rows:
        pdb_id = str(row.get("pdb_id") or "")
        pair_key = str(row.get("pair_identity_key") or "")
        if pdb_id:
            issues_by_pdb.setdefault(pdb_id, []).append(row)
            issues_by_key.setdefault((pdb_id, pair_key), []).append(row)

    review_rows: list[dict[str, str]] = []
    seen_entry_only: set[str] = set()

    for pair_row in pair_rows:
        pdb_id = str(pair_row.get("pdb_id") or "")
        pair_key = str(pair_row.get("pair_identity_key") or "")
        entry = entry_by_pdb.get(pdb_id, {})
        relevant_issues = issues_by_key.get((pdb_id, pair_key), []) + [
            row for row in issues_by_pdb.get(pdb_id, [])
            if not str(row.get("pair_identity_key") or "")
        ]
        review_rows.append({
            "scope": "pair",
            "pdb_id": pdb_id,
            "pair_identity_key": pair_key,
            "title": str(entry.get("title") or ""),
            "issue_types": "; ".join(sorted({str(row.get("issue_type") or "") for row in relevant_issues if row.get("issue_type")})),
            "issue_details": "; ".join(str(row.get("details") or "") for row in relevant_issues if row.get("details")),
            "source_conflict_flag": str(pair_row.get("source_conflict_flag") or ""),
            "source_conflict_summary": str(pair_row.get("source_conflict_summary") or ""),
            "source_agreement_band": str(pair_row.get("source_agreement_band") or ""),
            "selected_preferred_source": str(pair_row.get("selected_preferred_source") or ""),
            "binding_affinity_type": str(pair_row.get("binding_affinity_type") or ""),
            "membrane_vs_soluble": str(entry.get("membrane_vs_soluble") or ""),
            "metal_present": str(entry.get("metal_present") or ""),
            "cofactor_present": str(entry.get("cofactor_present") or ""),
            "glycan_present": str(entry.get("glycan_present") or ""),
            "quality_flags": str(entry.get("quality_flags") or ""),
            "field_confidence_json": str(entry.get("field_confidence_json") or ""),
            "assay_field_confidence_json": str(pair_row.get("assay_field_confidence_json") or ""),
        })
        seen_entry_only.add(pdb_id)

    for pdb_id, entry in entry_by_pdb.items():
        if pdb_id in seen_entry_only:
            continue
        relevant_issues = issues_by_pdb.get(pdb_id, [])
        review_rows.append({
            "scope": "entry",
            "pdb_id": pdb_id,
            "pair_identity_key": "",
            "title": str(entry.get("title") or ""),
            "issue_types": "; ".join(sorted({str(row.get("issue_type") or "") for row in relevant_issues if row.get("issue_type")})),
            "issue_details": "; ".join(str(row.get("details") or "") for row in relevant_issues if row.get("details")),
            "source_conflict_flag": "",
            "source_conflict_summary": "",
            "source_agreement_band": "",
            "selected_preferred_source": "",
            "binding_affinity_type": "",
            "membrane_vs_soluble": str(entry.get("membrane_vs_soluble") or ""),
            "metal_present": str(entry.get("metal_present") or ""),
            "cofactor_present": str(entry.get("cofactor_present") or ""),
            "glycan_present": str(entry.get("glycan_present") or ""),
            "quality_flags": str(entry.get("quality_flags") or ""),
            "field_confidence_json": str(entry.get("field_confidence_json") or ""),
            "assay_field_confidence_json": "",
        })

    def _matches(row: dict[str, str]) -> bool:
        if pdb_query and pdb_query.lower() not in str(row.get("pdb_id") or "").lower():
            return False
        if pair_query and pair_query.lower() not in str(row.get("pair_identity_key") or "").lower():
            return False
        if issue_type != "All":
            issue_types = {item.strip() for item in str(row.get("issue_types") or "").split(";") if item.strip()}
            if issue_type not in issue_types:
                return False
        if conflict_only and str(row.get("source_conflict_flag") or "").lower() != "true":
            return False
        if mutation_ambiguous_only:
            issue_types = str(row.get("issue_types") or "")
            pair_key = str(row.get("pair_identity_key") or "")
            if "ambiguous_mutation_context" not in issue_types and "mutation_unknown" not in pair_key:
                return False
        if metal_only and str(row.get("metal_present") or "").lower() != "true":
            return False
        if cofactor_only and str(row.get("cofactor_present") or "").lower() != "true":
            return False
        if glycan_only and str(row.get("glycan_present") or "").lower() != "true":
            return False
        if confidence_filter == "Non-high" and not _row_has_non_high_confidence(row):
            return False
        if confidence_filter == "Medium" and not _row_has_confidence_level(row, "medium"):
            return False
        if confidence_filter == "Low" and not _row_has_confidence_level(row, "low"):
            return False
        return True

    filtered = [row for row in review_rows if _matches(row)]
    filtered.sort(key=lambda row: (row["pdb_id"], row["pair_identity_key"], row["scope"]))
    return filtered


def build_review_health_summary(coverage: dict[str, Any]) -> dict[str, str]:
    """Build compact review guidance from the coverage summary artifact.

    Assumption:
    - This is an operational summary, not scientific inference. It only
      interprets already-computed counts and release-policy outputs.
    """
    counts = coverage.get("counts") or {}
    release = coverage.get("release") or {}
    issue_types = (coverage.get("coverage") or {}).get("issue_types") or {}

    entry_count = int(counts.get("entry_count") or 0)
    pair_count = int(counts.get("pair_count") or 0)
    model_ready = int(counts.get("model_ready_pair_count") or 0)
    conflicts = int(counts.get("pairs_with_source_conflicts") or 0)
    exclusions = int(release.get("model_ready_exclusion_count") or 0)
    structures = int(counts.get("entries_with_structure_file") or 0)
    missing_structures = int(issue_types.get("missing_structure_file") or 0)
    low_conf = int(issue_types.get("non_high_confidence_fields") or 0) + int(issue_types.get("non_high_confidence_assay_fields") or 0)

    readiness = "Not ready"
    if pair_count > 0 and exclusions == 0 and conflicts == 0:
        readiness = "Release-ready"
    elif model_ready > 0:
        readiness = "Partially ready"
    elif entry_count > 0:
        readiness = "Needs review"

    coverage_text = (
        f"{entry_count:,} entries, {pair_count:,} pairs, {model_ready:,} model-ready, "
        f"{structures:,} with structures"
    )
    quality_text = (
        f"{conflicts:,} conflicted pairs, {low_conf:,} non-high-confidence issues, "
        f"{missing_structures:,} missing structures"
    )

    if conflicts > 0:
        next_action = "Review master_pdb_conflicts.csv and master_pdb_issues.csv before release."
    elif missing_structures > 0:
        next_action = "Repair missing structure files before trusting model-ready outputs."
    elif exclusions > 0:
        next_action = "Review model_ready_exclusions.csv to resolve or accept blocked pairs."
    elif model_ready > 0:
        next_action = "Build or inspect the latest release snapshot."
    else:
        next_action = "Run ingest and extract to populate review artifacts."

    return {
        "readiness": readiness,
        "coverage": coverage_text,
        "quality": quality_text,
        "next_action": next_action,
    }


# ---------------------------------------------------------------------------
# GUI
# ---------------------------------------------------------------------------

class PbdataGUI:
    """Main application window."""

    def __init__(self, root: tk.Tk) -> None:
        self._root = root
        self._root.title("pbdata — Protein Binding Dataset Platform")
        self._set_initial_geometry()
        self._root.resizable(True, True)
        self._root.minsize(760, 520)
        self._root.protocol("WM_DELETE_WINDOW", self._on_close)
        self._root.configure(bg=_APP_BG)
        self._configure_styles()

        # --- Source variables ---
        self._src_enabled: dict[str, tk.BooleanVar] = {
            s: tk.BooleanVar() for s in _SOURCES
        }
        self._src_path_vars: dict[str, tk.StringVar] = {
            s: tk.StringVar(value="") for s in _SOURCE_PATH_FIELDS
        }

        # --- Criteria variables ---
        self._method_vars: dict[str, tk.BooleanVar] = {
            k: tk.BooleanVar() for k in EXPERIMENTAL_METHODS
        }
        self._resolution_var       = tk.StringVar(value="3.0 Å")
        self._task_vars: dict[str, tk.BooleanVar] = {
            "protein_ligand":  tk.BooleanVar(value=True),
            "protein_protein": tk.BooleanVar(value=True),
            "mutation_ddg":    tk.BooleanVar(value=False),
        }
        self._keyword_query_var       = tk.StringVar(value="")
        self._organism_name_var       = tk.StringVar(value="")
        self._taxonomy_id_var         = tk.StringVar(value="")
        self._pdb_ids_var             = tk.StringVar(value="")
        self._membrane_only_var       = tk.BooleanVar(value=False)
        self._require_multimer_var    = tk.BooleanVar(value=False)
        self._require_protein_var     = tk.BooleanVar(value=True)
        self._require_ligand_var      = tk.BooleanVar(value=False)
        self._require_branched_entities_var = tk.BooleanVar(value=False)
        self._min_protein_entities_var = tk.StringVar(value="")
        self._min_nonpolymer_entities_var = tk.StringVar(value="")
        self._max_nonpolymer_entities_var = tk.StringVar(value="")
        self._min_branched_entities_var = tk.StringVar(value="")
        self._max_branched_entities_var = tk.StringVar(value="")
        self._min_assembly_count_var = tk.StringVar(value="")
        self._max_assembly_count_var = tk.StringVar(value="")
        self._max_atom_count_var      = tk.StringVar(value="")
        self._min_year_var            = tk.StringVar(value="")
        self._max_year_var            = tk.StringVar(value="")
        self._review_pdb_query_var    = tk.StringVar(value="")
        self._review_pair_query_var   = tk.StringVar(value="")
        self._review_issue_type_var   = tk.StringVar(value="All")
        self._review_confidence_var   = tk.StringVar(value="All")
        self._review_conflict_only_var = tk.BooleanVar(value=False)
        self._review_mutation_ambiguous_only_var = tk.BooleanVar(value=False)
        self._review_metal_only_var   = tk.BooleanVar(value=False)
        self._review_cofactor_only_var = tk.BooleanVar(value=False)
        self._review_glycan_only_var  = tk.BooleanVar(value=False)
        self._review_filtered_count_var = tk.StringVar(value="--")

        # --- Pipeline option variables ---
        self._storage_root_var        = tk.StringVar(value=str(Path.cwd()))
        self._structure_mirror_var    = tk.StringVar(value="rcsb")
        self._download_structures_var = tk.BooleanVar(value=True)
        self._download_pdb_var        = tk.BooleanVar(value=False)
        self._workers_var             = tk.StringVar(value="1")
        self._pipeline_execution_mode_var = tk.StringVar(value="hybrid")
        self._site_pipeline_degraded_mode_var = tk.BooleanVar(value=True)
        self._site_pipeline_run_id_var = tk.StringVar(value="")
        self._site_physics_batch_id_var = tk.StringVar(value="")
        self._structural_graph_level_var = tk.StringVar(value="residue")
        self._structural_graph_scope_var = tk.StringVar(value="whole_protein")
        self._structural_graph_exports_var = tk.StringVar(value="pyg,networkx")
        self._split_mode_var          = tk.StringVar(value="auto")
        self._train_frac_var          = tk.StringVar(value="0.70")
        self._val_frac_var            = tk.StringVar(value="0.15")
        self._split_seed_var          = tk.StringVar(value="42")
        self._hash_only_var           = tk.BooleanVar(value=False)
        self._jaccard_threshold_var   = tk.StringVar(value="0.30")
        self._release_tag_var         = tk.StringVar(value="")
        self._custom_set_mode_var     = tk.StringVar(value="generalist")
        self._custom_set_target_size_var = tk.StringVar(value="500")
        self._custom_set_seed_var     = tk.StringVar(value="42")
        self._custom_set_cluster_cap_var = tk.StringVar(value="1")
        self._engineered_dataset_name_var = tk.StringVar(value="engineered_dataset")
        self._engineered_dataset_test_frac_var = tk.StringVar(value="0.20")
        self._engineered_dataset_cv_folds_var = tk.StringVar(value="0")
        self._engineered_dataset_cluster_count_var = tk.StringVar(value="8")
        self._engineered_dataset_embedding_backend_var = tk.StringVar(value="auto")
        self._engineered_dataset_strict_family_var = tk.BooleanVar(value=False)

        # --- Pipeline status vars ---
        self._status_vars: dict[str, tk.StringVar] = {
            key: tk.StringVar(value="idle") for key in _ALL_STAGE_KEYS
        }
        self._status_labels: dict[str, tk.Label] = {}

        # --- Data overview labels ---
        self._overview_vars: dict[str, tk.StringVar] = {}
        self._review_export_vars: dict[str, tk.StringVar] = {}
        self._review_health_vars: dict[str, tk.StringVar] = {
            "readiness": tk.StringVar(value="--"),
            "coverage": tk.StringVar(value="--"),
            "quality": tk.StringVar(value="--"),
            "next_action": tk.StringVar(value="--"),
        }

        # Serialise "Run All"
        self._running = threading.Lock()
        self._closing = False
        self._active_processes: set[subprocess.Popen[str]] = set()

        self._build_ui()
        self._load_sources_into_ui()
        self._load_criteria_into_ui()
        self._refresh_overview()

    def _storage_layout(self):
        return build_storage_layout(self._storage_root_var.get().strip() or Path.cwd())

    def _set_initial_geometry(self) -> None:
        screen_w = max(int(self._root.winfo_screenwidth()), 1024)
        screen_h = max(int(self._root.winfo_screenheight()), 768)
        width = min(1280, max(980, screen_w - 120))
        height = min(860, max(640, screen_h - 120))
        self._root.geometry(f"{width}x{height}")

    def _configure_styles(self) -> None:
        style = ttk.Style(self._root)
        try:
            style.theme_use("clam")
        except tk.TclError:
            pass
        style.configure(".", background=_APP_BG, foreground="#0f172a", font=("Segoe UI", 9))
        style.configure("TFrame", background=_APP_BG)
        style.configure("TLabel", background=_APP_BG, foreground="#0f172a")
        style.configure("Muted.TLabel", background=_APP_BG, foreground=_MUTED_FG, font=("Segoe UI", 8))
        style.configure("Section.TLabelframe", background=_CARD_BG, relief="solid", borderwidth=1)
        style.configure("Section.TLabelframe.Label", background=_CARD_BG, foreground="#0f172a", font=("Segoe UI Semibold", 10))
        style.configure("Card.TFrame", background=_CARD_BG, relief="solid", borderwidth=1)
        style.configure("TNotebook", background=_APP_BG, borderwidth=0)
        style.configure("TNotebook.Tab", padding=(12, 8), font=("Segoe UI Semibold", 9))
        style.map("TNotebook.Tab", background=[("selected", _CARD_BG)], foreground=[("selected", "#0f172a")])
        style.configure("Accent.TButton", font=("Segoe UI Semibold", 9))

    # ------------------------------------------------------------------
    # UI construction
    # ------------------------------------------------------------------

    def _build_ui(self) -> None:
        self._root.columnconfigure(0, weight=0, minsize=360)
        self._root.columnconfigure(1, weight=1)
        self._root.rowconfigure(1, weight=1)
        self._root.rowconfigure(2, weight=2)

        self._build_header()
        self._build_left_panel()
        self._build_pipeline_panel()
        self._build_log_panel()

    def _bind_canvas_mousewheel(self, canvas: tk.Canvas, frame: ttk.Frame) -> None:
        def _scroll(event: Any) -> str:
            canvas.yview_scroll(_mousewheel_units(event), "units")
            return "break"

        def _bind_recursive(widget: Any) -> None:
            widget.bind("<MouseWheel>", _scroll, add="+")
            widget.bind("<Button-4>", _scroll, add="+")
            widget.bind("<Button-5>", _scroll, add="+")
            for child in widget.winfo_children():
                _bind_recursive(child)

        for widget in (canvas, frame):
            _bind_recursive(widget)

    def _bind_text_mousewheel(self, widget: Any) -> None:
        def _scroll(event: Any) -> str:
            widget.yview_scroll(_mousewheel_units(event), "units")
            return "break"

        widget.bind("<MouseWheel>", _scroll, add="+")
        widget.bind("<Button-4>", _scroll, add="+")
        widget.bind("<Button-5>", _scroll, add="+")

    def _build_header(self) -> None:
        bar = tk.Frame(self._root, bg=_HEADER_BG, pady=8)
        bar.grid(row=0, column=0, columnspan=2, sticky="ew")
        bar.columnconfigure(1, weight=1)

        left = tk.Frame(bar, bg=_HEADER_BG)
        left.pack(side="left", padx=16)

        tk.Label(
            left,
            text="pbdata",
            fg=_HEADER_FG, bg=_HEADER_BG,
            font=("Helvetica", 15, "bold"),
        ).pack(side="left")

        tk.Label(
            left,
            text="  Protein Binding Dataset Platform",
            fg="#b0bec5", bg=_HEADER_BG,
            font=("Helvetica", 11),
        ).pack(side="left")

        tk.Label(
            bar,
            text="Structure Extraction  |  Assay Ingestion  |  Graph Builder  |  ML Data Generator",
            fg="#7986cb", bg=_HEADER_BG,
            font=("Helvetica", 8),
        ).pack(side="right", padx=16)

    def _build_left_panel(self) -> None:
        left = tk.Frame(self._root, bg=_APP_BG)
        left.grid(row=1, column=0, sticky="nsew", padx=(10, 4), pady=(10, 4))
        left.columnconfigure(0, weight=1)
        left.rowconfigure(0, weight=1)

        notebook = ttk.Notebook(left)
        notebook.grid(row=0, column=0, sticky="nsew")

        self._build_sources_tab(notebook)
        self._build_search_tab(notebook)
        self._build_options_tab(notebook)

    # --- Tab 1: Data Sources ---

    def _build_sources_tab(self, notebook: ttk.Notebook) -> None:
        outer = ttk.Frame(notebook, padding=8)
        notebook.add(outer, text=" Sources ")
        outer.columnconfigure(0, weight=1)
        outer.rowconfigure(0, weight=1)

        canvas = tk.Canvas(outer, highlightthickness=0)
        scrollbar = ttk.Scrollbar(outer, orient="vertical", command=canvas.yview)
        frame = ttk.Frame(canvas, padding=8)
        frame.bind(
            "<Configure>",
            lambda _: canvas.configure(scrollregion=canvas.bbox("all")),
        )
        canvas.bind(
            "<Configure>",
            lambda e: canvas.itemconfigure(wid, width=e.width),
        )
        wid = canvas.create_window((0, 0), window=frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)
        canvas.grid(row=0, column=0, sticky="nsew")
        scrollbar.grid(row=0, column=1, sticky="ns")
        self._bind_canvas_mousewheel(canvas, frame)

        frame.columnconfigure(0, weight=1)
        row = 0

        ttk.Label(
            frame,
            text="Enable the data sources for your pipeline run.",
            font=("Helvetica", 8),
            foreground="#666666",
        ).grid(row=row, column=0, sticky="w", pady=(0, 8))
        row += 1

        for src in _SOURCES:
            src_frame = ttk.Frame(frame)
            src_frame.grid(row=row, column=0, sticky="ew", pady=2)
            src_frame.columnconfigure(1, weight=1)

            ttk.Checkbutton(
                src_frame,
                text=src.upper(),
                variable=self._src_enabled[src],
            ).grid(row=0, column=0, sticky="w")

            ttk.Label(
                src_frame,
                text=_SOURCE_DESCRIPTIONS.get(src, ""),
                font=("Helvetica", 7),
                foreground="#888888",
            ).grid(row=0, column=1, sticky="w", padx=(8, 0))

            note = _SOURCE_INGEST_NOTES.get(src, "")
            if note:
                ttk.Label(
                    src_frame,
                    text=note,
                    font=("Helvetica", 7, "italic"),
                    foreground="#999999",
                ).grid(row=1, column=0, columnspan=2, sticky="w", padx=(24, 0))
            row += 1

        ttk.Separator(frame, orient="horizontal").grid(
            row=row, column=0, sticky="ew", pady=8,
        )
        row += 1

        ttk.Label(
            frame, text="Experimental Structure Mirror:",
            font=("Helvetica", 9, "bold"),
        ).grid(row=row, column=0, sticky="w", pady=(0, 4))
        mirror_box = ttk.Combobox(
            frame,
            state="readonly",
            textvariable=self._structure_mirror_var,
            values=_STRUCTURE_MIRROR_OPTIONS,
            width=12,
        )
        mirror_box.grid(row=row, column=1, sticky="w", padx=(6, 0), pady=(0, 4))
        row += 1

        ttk.Label(
            frame,
            text="Used for experimental mmCIF/PDB downloads during Extract and downstream physics features.",
            font=("Helvetica", 7),
            foreground="#888888",
        ).grid(row=row, column=0, columnspan=2, sticky="w", pady=(0, 8))
        row += 1

        # Path fields for local or cached enrichment sources
        ttk.Label(
            frame, text="Extract-Time Source Paths:",
            font=("Helvetica", 9, "bold"),
        ).grid(row=row, column=0, sticky="w", pady=(0, 4))
        row += 1

        path_labels = {
            "bindingdb": "BindingDB local cache directory (optional)",
            "pdbbind": "PDBbind local dataset directory",
            "biolip":  "BioLiP local dataset directory",
            "skempi":  "SKEMPI CSV file (optional override)",
        }
        for src, label in path_labels.items():
            path_frame = ttk.Frame(frame)
            path_frame.grid(row=row, column=0, sticky="ew", pady=2)
            path_frame.columnconfigure(1, weight=1)

            ttk.Label(path_frame, text=f"{label}:").grid(
                row=0, column=0, sticky="w",
            )
            ttk.Entry(path_frame, textvariable=self._src_path_vars[src]).grid(
                row=0, column=1, sticky="ew", padx=(6, 4),
            )
            is_dir = src != "skempi"
            ttk.Button(
                path_frame, text="...", width=3,
                command=lambda s=src, d=is_dir: self._browse_path(s, d),
            ).grid(row=0, column=2)
            row += 1

        ttk.Separator(frame, orient="horizontal").grid(
            row=row, column=0, sticky="ew", pady=8,
        )
        row += 1

        ttk.Button(
            frame, text="Save Source Config",
            command=self._save_sources,
        ).grid(row=row, column=0, sticky="ew")

    def _browse_path(self, src: str, is_dir: bool) -> None:
        if is_dir:
            path = filedialog.askdirectory(title=f"Select {src} directory")
        else:
            path = filedialog.askopenfilename(
                title=f"Select {src} file",
                filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
            )
        if path:
            self._src_path_vars[src].set(path)

    def _browse_storage_root(self) -> None:
        path = filedialog.askdirectory(title="Select storage root folder")
        if path:
            self._storage_root_var.set(path)
            self._refresh_overview()

    # --- Tab 2: Search Criteria ---

    def _build_search_tab(self, notebook: ttk.Notebook) -> None:
        outer = ttk.Frame(notebook, padding=8)
        notebook.add(outer, text=" Search Criteria ")
        outer.columnconfigure(0, weight=1)
        outer.rowconfigure(0, weight=1)

        canvas = tk.Canvas(outer, highlightthickness=0)
        scrollbar = ttk.Scrollbar(outer, orient="vertical", command=canvas.yview)
        frame = ttk.Frame(canvas, padding=8)
        frame.bind(
            "<Configure>",
            lambda _: canvas.configure(scrollregion=canvas.bbox("all")),
        )
        canvas.bind(
            "<Configure>",
            lambda e: canvas.itemconfigure(wid, width=e.width),
        )
        wid = canvas.create_window((0, 0), window=frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)
        canvas.grid(row=0, column=0, sticky="nsew")
        scrollbar.grid(row=0, column=1, sticky="ns")
        self._bind_canvas_mousewheel(canvas, frame)

        frame.columnconfigure(1, weight=1)
        row = 0

        ttk.Label(
            frame,
            text="RCSB search filters. Also used to scope Extract and Normalize.",
            font=("Helvetica", 8),
            foreground="#666666",
        ).grid(row=row, column=0, columnspan=2, sticky="w", pady=(0, 8))
        row += 1

        # --- Direct PDB IDs ---
        ttk.Label(
            frame, text="Direct PDB IDs:",
            font=("Helvetica", 9, "bold"),
        ).grid(row=row, column=0, columnspan=2, sticky="w")
        row += 1
        ttk.Label(
            frame,
            text="Comma-separated. If set, bypasses RCSB search.",
            font=("Helvetica", 7),
            foreground="#888888",
        ).grid(row=row, column=0, columnspan=2, sticky="w")
        row += 1
        ttk.Entry(frame, textvariable=self._pdb_ids_var).grid(
            row=row, column=0, columnspan=2, sticky="ew", pady=(2, 0),
        )
        row += 1

        ttk.Separator(frame, orient="horizontal").grid(
            row=row, column=0, columnspan=2, sticky="ew", pady=8,
        )
        row += 1

        # --- Text search ---
        ttk.Label(
            frame, text="Text Filters:",
            font=("Helvetica", 9, "bold"),
        ).grid(row=row, column=0, columnspan=2, sticky="w", pady=(0, 4))
        row += 1

        for label, var in [
            ("Keywords / full-text:", self._keyword_query_var),
            ("Organism name:", self._organism_name_var),
            ("NCBI taxonomy ID:", self._taxonomy_id_var),
        ]:
            ttk.Label(frame, text=label).grid(
                row=row, column=0, sticky="w", pady=(4, 0),
            )
            ttk.Entry(frame, textvariable=var).grid(
                row=row, column=1, sticky="ew", padx=(6, 0), pady=(4, 0),
            )
            row += 1

        ttk.Separator(frame, orient="horizontal").grid(
            row=row, column=0, columnspan=2, sticky="ew", pady=8,
        )
        row += 1

        ttk.Checkbutton(
            frame, text="Membrane-related structures only",
            variable=self._membrane_only_var,
        ).grid(row=row, column=0, columnspan=2, sticky="w", pady=(0, 2))
        row += 1

        ttk.Checkbutton(
            frame, text="Require multimeric protein entries",
            variable=self._require_multimer_var,
        ).grid(row=row, column=0, columnspan=2, sticky="w", pady=(0, 2))
        row += 1

        ttk.Separator(frame, orient="horizontal").grid(
            row=row, column=0, columnspan=2, sticky="ew", pady=8,
        )
        row += 1

        # --- Experimental methods ---
        ttk.Label(
            frame, text="Experimental Methods:",
            font=("Helvetica", 9, "bold"),
        ).grid(row=row, column=0, columnspan=2, sticky="w", pady=(0, 4))
        row += 1

        method_labels = {
            "xray":    "X-Ray Diffraction",
            "em":      "Cryo-EM",
            "nmr":     "NMR",
            "neutron": "Neutron Diffraction",
        }
        methods_frame = ttk.Frame(frame)
        methods_frame.grid(row=row, column=0, columnspan=2, sticky="w")
        for i, (key, label) in enumerate(method_labels.items()):
            ttk.Checkbutton(
                methods_frame, text=label, variable=self._method_vars[key],
            ).grid(row=i // 2, column=i % 2, sticky="w", padx=(0, 12), pady=1)
        row += 1

        ttk.Separator(frame, orient="horizontal").grid(
            row=row, column=0, columnspan=2, sticky="ew", pady=8,
        )
        row += 1

        # --- Resolution ---
        ttk.Label(frame, text="Max Resolution:").grid(
            row=row, column=0, sticky="w",
        )
        ttk.Combobox(
            frame,
            textvariable=self._resolution_var,
            values=RESOLUTION_OPTIONS,
            width=10,
            state="readonly",
        ).grid(row=row, column=1, sticky="w", padx=(6, 0))
        row += 1

        ttk.Separator(frame, orient="horizontal").grid(
            row=row, column=0, columnspan=2, sticky="ew", pady=8,
        )
        row += 1

        # --- Task types ---
        ttk.Label(
            frame, text="Interaction Types:",
            font=("Helvetica", 9, "bold"),
        ).grid(row=row, column=0, columnspan=2, sticky="w", pady=(0, 4))
        row += 1

        task_labels = {
            "protein_ligand":  "Protein-Ligand binding",
            "protein_protein": "Protein-Protein interaction",
            "mutation_ddg":    "Mutation ddG",
        }
        for key, label in task_labels.items():
            ttk.Checkbutton(
                frame, text=label, variable=self._task_vars[key],
            ).grid(row=row, column=0, columnspan=2, sticky="w", pady=1)
            row += 1

        ttk.Separator(frame, orient="horizontal").grid(
            row=row, column=0, columnspan=2, sticky="ew", pady=8,
        )
        row += 1

        # --- Structure filters ---
        ttk.Label(
            frame, text="Structure Filters:",
            font=("Helvetica", 9, "bold"),
        ).grid(row=row, column=0, columnspan=2, sticky="w", pady=(0, 4))
        row += 1

        ttk.Checkbutton(
            frame, text="Require protein entity",
            variable=self._require_protein_var,
        ).grid(row=row, column=0, columnspan=2, sticky="w")
        row += 1

        ttk.Checkbutton(
            frame, text="Require ligand / non-polymer",
            variable=self._require_ligand_var,
        ).grid(row=row, column=0, columnspan=2, sticky="w", pady=(2, 0))
        row += 1

        ttk.Checkbutton(
            frame,
            text="Require branched entities (glycan-oriented proxy)",
            variable=self._require_branched_entities_var,
        ).grid(row=row, column=0, columnspan=2, sticky="w", pady=(2, 0))
        row += 1

        for label, var, width in [
            ("Min protein entities:", self._min_protein_entities_var, 8),
            ("Min nonpolymer entities:", self._min_nonpolymer_entities_var, 8),
            ("Max nonpolymer entities:", self._max_nonpolymer_entities_var, 8),
            ("Min branched entities:", self._min_branched_entities_var, 8),
            ("Max branched entities:", self._max_branched_entities_var, 8),
            ("Min biological assemblies:", self._min_assembly_count_var, 8),
            ("Max biological assemblies:", self._max_assembly_count_var, 8),
            ("Max deposited atoms:", self._max_atom_count_var, 12),
        ]:
            ttk.Label(frame, text=label).grid(
                row=row, column=0, sticky="w", pady=(6, 0),
            )
            ttk.Entry(frame, textvariable=var, width=width).grid(
                row=row, column=1, sticky="w", padx=(6, 0), pady=(6, 0),
            )
            row += 1

        ttk.Separator(frame, orient="horizontal").grid(
            row=row, column=0, columnspan=2, sticky="ew", pady=8,
        )
        row += 1

        # --- Date range ---
        ttk.Label(
            frame, text="Release Year Range:",
            font=("Helvetica", 9, "bold"),
        ).grid(row=row, column=0, columnspan=2, sticky="w", pady=(0, 4))
        row += 1

        year_frame = ttk.Frame(frame)
        year_frame.grid(row=row, column=0, columnspan=2, sticky="w")
        ttk.Label(year_frame, text="From:").grid(row=0, column=0, sticky="w")
        ttk.Entry(year_frame, textvariable=self._min_year_var, width=8).grid(
            row=0, column=1, padx=(4, 12),
        )
        ttk.Label(year_frame, text="To:").grid(row=0, column=2, sticky="w")
        ttk.Entry(year_frame, textvariable=self._max_year_var, width=8).grid(
            row=0, column=3, padx=(4, 0),
        )
        row += 1

        ttk.Separator(frame, orient="horizontal").grid(
            row=row, column=0, columnspan=2, sticky="ew", pady=8,
        )
        row += 1

        ttk.Button(
            frame, text="Save Search Criteria",
            command=self._save_criteria,
        ).grid(row=row, column=0, columnspan=2, sticky="ew")
        row += 1

        ttk.Separator(frame, orient="horizontal").grid(
            row=row, column=0, columnspan=2, sticky="ew", pady=8,
        )
        row += 1

        ttk.Label(
            frame,
            text="Local Review Filters",
            font=("Helvetica", 9, "bold"),
        ).grid(row=row, column=0, columnspan=2, sticky="w", pady=(0, 4))
        row += 1
        ttk.Label(
            frame,
            text="Applies to the root review CSVs after extraction. Writes master_pdb_review_filtered.csv in the repo root.",
            font=("Helvetica", 7),
            foreground="#888888",
        ).grid(row=row, column=0, columnspan=2, sticky="w")
        row += 1

        for label, var in [
            ("PDB ID contains:", self._review_pdb_query_var),
            ("Pair key contains:", self._review_pair_query_var),
        ]:
            ttk.Label(frame, text=label).grid(row=row, column=0, sticky="w", pady=(4, 0))
            ttk.Entry(frame, textvariable=var).grid(
                row=row, column=1, sticky="ew", padx=(6, 0), pady=(4, 0),
            )
            row += 1

        ttk.Label(frame, text="Issue type:").grid(row=row, column=0, sticky="w", pady=(4, 0))
        ttk.Combobox(
            frame,
            textvariable=self._review_issue_type_var,
            values=_REVIEW_ISSUE_OPTIONS,
            width=28,
            state="readonly",
        ).grid(row=row, column=1, sticky="w", padx=(6, 0), pady=(4, 0))
        row += 1

        ttk.Label(frame, text="Confidence filter:").grid(row=row, column=0, sticky="w", pady=(4, 0))
        ttk.Combobox(
            frame,
            textvariable=self._review_confidence_var,
            values=_REVIEW_CONFIDENCE_OPTIONS,
            width=18,
            state="readonly",
        ).grid(row=row, column=1, sticky="w", padx=(6, 0), pady=(4, 0))
        row += 1

        for label, var in [
            ("Conflicted pairs only", self._review_conflict_only_var),
            ("Mutation-ambiguous only", self._review_mutation_ambiguous_only_var),
            ("Metal-containing entries only", self._review_metal_only_var),
            ("Cofactor-containing entries only", self._review_cofactor_only_var),
            ("Glycan-containing entries only", self._review_glycan_only_var),
        ]:
            ttk.Checkbutton(frame, text=label, variable=var).grid(
                row=row, column=0, columnspan=2, sticky="w", pady=(2, 0),
            )
            row += 1

        ttk.Label(
            frame,
            textvariable=self._review_filtered_count_var,
            font=("Helvetica", 8, "bold"),
        ).grid(row=row, column=0, columnspan=2, sticky="w", pady=(6, 0))
        row += 1

        review_btns = ttk.Frame(frame)
        review_btns.grid(row=row, column=0, columnspan=2, sticky="ew", pady=(6, 0))
        review_btns.columnconfigure(0, weight=1)
        review_btns.columnconfigure(1, weight=1)
        review_btns.columnconfigure(2, weight=1)
        ttk.Button(
            review_btns,
            text="Apply Review Filter",
            command=self._apply_local_review_filters,
        ).grid(row=0, column=0, sticky="ew", padx=(0, 4))
        ttk.Button(
            review_btns,
            text="Reset Review Filter",
            command=self._reset_local_review_filters,
        ).grid(row=0, column=1, sticky="ew", padx=4)
        ttk.Button(
            review_btns,
            text="Refresh Root Exports",
            command=self._refresh_review_exports,
        ).grid(row=0, column=2, sticky="ew", padx=(4, 0))

    # --- Tab 3: Pipeline Options ---

    def _build_options_tab(self, notebook: ttk.Notebook) -> None:
        outer = ttk.Frame(notebook, padding=8)
        notebook.add(outer, text=" Options ")
        outer.columnconfigure(0, weight=1)

        row = 0

        ttk.Label(
            outer, text="Storage Root",
            font=("Helvetica", 10, "bold"),
        ).grid(row=row, column=0, sticky="w", pady=(0, 6))
        row += 1

        root_frame = ttk.Frame(outer)
        root_frame.grid(row=row, column=0, sticky="ew")
        root_frame.columnconfigure(0, weight=1)
        ttk.Entry(root_frame, textvariable=self._storage_root_var).grid(
            row=0, column=0, sticky="ew", padx=(0, 4),
        )
        ttk.Button(
            root_frame, text="Browse...",
            command=self._browse_storage_root,
        ).grid(row=0, column=1)
        row += 1

        ttk.Label(
            outer,
            text=(
                "All generated files will be stored under <storage root>/data/\n"
                "for raw, processed, extracted, structures, graph, features, reports, and splits."
            ),
            font=("Helvetica", 7),
            foreground="#888888",
        ).grid(row=row, column=0, sticky="w", pady=(2, 0))
        row += 1

        ttk.Separator(outer, orient="horizontal").grid(
            row=row, column=0, sticky="ew", pady=10,
        )
        row += 1

        ttk.Label(
            outer, text="Pipeline Mode",
            font=("Helvetica", 10, "bold"),
        ).grid(row=row, column=0, sticky="w", pady=(0, 6))
        row += 1

        pipeline_mode_frame = ttk.Frame(outer)
        pipeline_mode_frame.grid(row=row, column=0, sticky="ew")
        pipeline_mode_frame.columnconfigure(1, weight=1)
        ttk.Label(pipeline_mode_frame, text="Execution mode:").grid(row=0, column=0, sticky="w", pady=2)
        ttk.Combobox(
            pipeline_mode_frame,
            textvariable=self._pipeline_execution_mode_var,
            values=_PIPELINE_EXECUTION_MODES,
            width=18,
            state="readonly",
        ).grid(row=0, column=1, sticky="w", padx=(6, 0), pady=2)
        ttk.Label(pipeline_mode_frame, text="Site-centric run id:").grid(row=1, column=0, sticky="w", pady=2)
        ttk.Entry(
            pipeline_mode_frame,
            textvariable=self._site_pipeline_run_id_var,
            width=22,
        ).grid(row=1, column=1, sticky="w", padx=(6, 0), pady=2)
        ttk.Label(pipeline_mode_frame, text="Physics batch id:").grid(row=2, column=0, sticky="w", pady=2)
        ttk.Entry(
            pipeline_mode_frame,
            textvariable=self._site_physics_batch_id_var,
            width=22,
        ).grid(row=2, column=1, sticky="w", padx=(6, 0), pady=2)
        ttk.Checkbutton(
            pipeline_mode_frame,
            text="Allow degraded site physics proxies when no surrogate is available",
            variable=self._site_pipeline_degraded_mode_var,
        ).grid(row=3, column=0, columnspan=2, sticky="w", pady=(2, 0))
        row += 1

        ttk.Label(
            outer,
            text=(
                "legacy: current pipeline only\n"
                "site-centric: new artifacts/ pipeline only\n"
                "hybrid: run both, keeping shared extract/canonical inputs"
            ),
            font=("Helvetica", 7),
            foreground="#888888",
        ).grid(row=row, column=0, sticky="w", padx=(24, 0), pady=(2, 0))
        row += 1

        ttk.Separator(outer, orient="horizontal").grid(
            row=row, column=0, sticky="ew", pady=10,
        )
        row += 1

        # --- Extract options ---
        ttk.Label(
            outer, text="Extract Options",
            font=("Helvetica", 10, "bold"),
        ).grid(row=row, column=0, sticky="w", pady=(0, 6))
        row += 1

        ttk.Checkbutton(
            outer, text="Download mmCIF structure files",
            variable=self._download_structures_var,
        ).grid(row=row, column=0, sticky="w")
        row += 1

        ttk.Checkbutton(
            outer, text="Also download PDB format files",
            variable=self._download_pdb_var,
        ).grid(row=row, column=0, sticky="w", pady=(2, 0))
        row += 1

        ttk.Label(
            outer,
            text="mmCIF files are downloaded to <storage root>/data/structures/rcsb/",
            font=("Helvetica", 7),
            foreground="#888888",
        ).grid(row=row, column=0, sticky="w", padx=(24, 0), pady=(2, 0))
        row += 1

        ttk.Separator(outer, orient="horizontal").grid(
            row=row, column=0, sticky="ew", pady=10,
        )
        row += 1

        ttk.Label(
            outer, text="Workflow Engine",
            font=("Helvetica", 10, "bold"),
        ).grid(row=row, column=0, sticky="w", pady=(0, 6))
        row += 1

        ttk.Label(
            outer,
            text=(
                "Use Setup Workspace to create the instruction-pack workspace layout,\n"
                "then Harvest Metadata to build metadata/protein_metadata.csv for graph and dataset steps."
            ),
            font=("Helvetica", 7),
            foreground="#888888",
        ).grid(row=row, column=0, sticky="w", padx=(24, 0), pady=(2, 0))
        row += 1

        ttk.Separator(outer, orient="horizontal").grid(
            row=row, column=0, sticky="ew", pady=10,
        )
        row += 1

        ttk.Label(
            outer, text="Structural Graph Options",
            font=("Helvetica", 10, "bold"),
        ).grid(row=row, column=0, sticky="w", pady=(0, 6))
        row += 1

        graph_frame = ttk.Frame(outer)
        graph_frame.grid(row=row, column=0, sticky="ew")
        graph_frame.columnconfigure(1, weight=1)
        ttk.Label(graph_frame, text="Graph level:").grid(row=0, column=0, sticky="w", pady=2)
        ttk.Combobox(
            graph_frame,
            textvariable=self._structural_graph_level_var,
            values=["residue", "atom"],
            width=18,
            state="readonly",
        ).grid(row=0, column=1, sticky="w", padx=(6, 0), pady=2)
        ttk.Label(graph_frame, text="Scope:").grid(row=1, column=0, sticky="w", pady=2)
        ttk.Combobox(
            graph_frame,
            textvariable=self._structural_graph_scope_var,
            values=["whole_protein", "interface_only", "shell"],
            width=18,
            state="readonly",
        ).grid(row=1, column=1, sticky="w", padx=(6, 0), pady=2)
        ttk.Label(graph_frame, text="Export formats:").grid(row=2, column=0, sticky="w", pady=2)
        ttk.Entry(
            graph_frame,
            textvariable=self._structural_graph_exports_var,
            width=22,
        ).grid(row=2, column=1, sticky="w", padx=(6, 0), pady=2)
        row += 1

        ttk.Label(
            outer,
            text="Comma-separated export formats. Supported: pyg, dgl, networkx.",
            font=("Helvetica", 7),
            foreground="#888888",
        ).grid(row=row, column=0, sticky="w", padx=(24, 0), pady=(2, 0))
        row += 1

        ttk.Separator(outer, orient="horizontal").grid(
            row=row, column=0, sticky="ew", pady=10,
        )
        row += 1

        # --- Split options ---
        ttk.Label(
            outer, text="Split Options",
            font=("Helvetica", 10, "bold"),
        ).grid(row=row, column=0, sticky="w", pady=(0, 6))
        row += 1

        split_frame = ttk.Frame(outer)
        split_frame.grid(row=row, column=0, sticky="ew")
        split_frame.columnconfigure(1, weight=1)

        for i, (label, var) in enumerate([
            ("Workers:", self._workers_var),
            ("Split mode:", self._split_mode_var),
            ("Train fraction:", self._train_frac_var),
            ("Validation fraction:", self._val_frac_var),
            ("Random seed:", self._split_seed_var),
            ("Jaccard threshold:", self._jaccard_threshold_var),
        ]):
            ttk.Label(split_frame, text=label).grid(
                row=i, column=0, sticky="w", pady=2,
            )
            if label == "Split mode:":
                ttk.Combobox(
                    split_frame,
                    textvariable=var,
                    values=["auto", "pair-aware", "legacy-sequence", "hash"],
                    width=16,
                    state="readonly",
                ).grid(row=i, column=1, sticky="w", padx=(6, 0), pady=2)
            else:
                ttk.Entry(split_frame, textvariable=var, width=10).grid(
                    row=i, column=1, sticky="w", padx=(6, 0), pady=2,
                )
        row += 1

        ttk.Checkbutton(
            outer, text="Hash-only split (no sequence clustering)",
            variable=self._hash_only_var,
        ).grid(row=row, column=0, sticky="w", pady=(4, 0))
        row += 1

        ttk.Label(
            outer,
            text="Default uses k-mer Jaccard clustering to prevent\n"
                 "sequence-identity leakage between train/val/test.",
            font=("Helvetica", 7),
            foreground="#888888",
        ).grid(row=row, column=0, sticky="w", padx=(24, 0), pady=(2, 0))
        row += 1

        ttk.Separator(outer, orient="horizontal").grid(
            row=row, column=0, sticky="ew", pady=10,
        )
        row += 1

        ttk.Label(
            outer, text="Dataset Engineering",
            font=("Helvetica", 10, "bold"),
        ).grid(row=row, column=0, sticky="w", pady=(0, 6))
        row += 1

        dataset_frame = ttk.Frame(outer)
        dataset_frame.grid(row=row, column=0, sticky="ew")
        dataset_frame.columnconfigure(1, weight=1)
        dataset_fields: list[tuple[str, tk.Variable, list[str] | None]] = [
            ("Dataset name:", self._engineered_dataset_name_var, None),
            ("Test fraction:", self._engineered_dataset_test_frac_var, None),
            ("CV folds:", self._engineered_dataset_cv_folds_var, None),
            ("Cluster count:", self._engineered_dataset_cluster_count_var, None),
            ("Embedding backend:", self._engineered_dataset_embedding_backend_var, ["auto", "esm", "fallback"]),
        ]
        for i, (label, var, options) in enumerate(dataset_fields):
            ttk.Label(dataset_frame, text=label).grid(row=i, column=0, sticky="w", pady=2)
            if options is not None:
                ttk.Combobox(
                    dataset_frame,
                    textvariable=var,
                    values=options,
                    width=18,
                    state="readonly",
                ).grid(row=i, column=1, sticky="w", padx=(6, 0), pady=2)
            else:
                ttk.Entry(dataset_frame, textvariable=var, width=18).grid(
                    row=i, column=1, sticky="w", padx=(6, 0), pady=2,
                )
        ttk.Checkbutton(
            dataset_frame,
            text="Strict protein-family isolation",
            variable=self._engineered_dataset_strict_family_var,
        ).grid(row=len(dataset_fields), column=0, columnspan=2, sticky="w", pady=(4, 0))
        row += 1

        ttk.Label(
            outer,
            text="Builds train.csv, test.csv, optional cv_folds/, and reproducibility configs from metadata/protein_metadata.csv.",
            font=("Helvetica", 7),
            foreground="#888888",
        ).grid(row=row, column=0, sticky="w", padx=(24, 0), pady=(2, 0))
        row += 1

        ttk.Separator(outer, orient="horizontal").grid(
            row=row, column=0, sticky="ew", pady=10,
        )
        row += 1

        ttk.Label(
            outer, text="Release Options",
            font=("Helvetica", 10, "bold"),
        ).grid(row=row, column=0, sticky="w", pady=(0, 6))
        row += 1

        release_frame = ttk.Frame(outer)
        release_frame.grid(row=row, column=0, sticky="ew")
        release_frame.columnconfigure(1, weight=1)
        ttk.Label(release_frame, text="Release tag:").grid(row=0, column=0, sticky="w")
        ttk.Entry(release_frame, textvariable=self._release_tag_var).grid(
            row=0, column=1, sticky="ew", padx=(6, 0),
        )
        row += 1

        ttk.Label(
            outer,
            text="Optional. If blank, build-release uses the current UTC timestamp.",
            font=("Helvetica", 7),
            foreground="#888888",
        ).grid(row=row, column=0, sticky="w", padx=(24, 0), pady=(2, 0))
        row += 1

        ttk.Separator(outer, orient="horizontal").grid(
            row=row, column=0, sticky="ew", pady=10,
        )
        row += 1

        ttk.Label(
            outer, text="Custom Training Set",
            font=("Helvetica", 10, "bold"),
        ).grid(row=row, column=0, sticky="w", pady=(0, 6))
        row += 1

        custom_frame = ttk.Frame(outer)
        custom_frame.grid(row=row, column=0, sticky="ew")
        custom_frame.columnconfigure(1, weight=1)
        custom_fields: list[tuple[str, tk.StringVar, list[str] | None]] = [
            ("Selection mode:", self._custom_set_mode_var, ["generalist", "protein_ligand", "protein_protein", "mutation_effect", "high_trust"]),
            ("Target size:", self._custom_set_target_size_var, None),
            ("Seed:", self._custom_set_seed_var, None),
            ("Per receptor cluster cap:", self._custom_set_cluster_cap_var, None),
        ]
        for i, (label, var, options) in enumerate(custom_fields):
            ttk.Label(custom_frame, text=label).grid(row=i, column=0, sticky="w", pady=2)
            if options is not None:
                ttk.Combobox(
                    custom_frame,
                    textvariable=var,
                    values=options,
                    state="readonly",
                    width=18,
                ).grid(row=i, column=1, sticky="w", padx=(6, 0), pady=2)
            else:
                ttk.Entry(custom_frame, textvariable=var, width=12).grid(
                    row=i, column=1, sticky="w", padx=(6, 0), pady=2,
                )
        row += 1

        ttk.Label(
            outer,
            text="Builds a diversity-optimized subset from model-ready pairs, emphasizing broad coverage and low redundancy.",
            font=("Helvetica", 7),
            foreground="#888888",
        ).grid(row=row, column=0, sticky="w", padx=(24, 0), pady=(2, 0))

    # --- Pipeline panel (right side) ---

    def _build_pipeline_panel(self) -> None:
        right = tk.Frame(self._root, bg=_APP_BG)
        right.grid(row=1, column=1, sticky="nsew", padx=(4, 10), pady=(10, 4))
        right.columnconfigure(0, weight=1)
        right.rowconfigure(0, weight=1)

        canvas = tk.Canvas(right, highlightthickness=0, bg=_APP_BG)
        scrollbar = ttk.Scrollbar(right, orient="vertical", command=canvas.yview)
        outer = ttk.Frame(canvas)
        outer.bind(
            "<Configure>",
            lambda _: canvas.configure(scrollregion=canvas.bbox("all")),
        )
        canvas.bind(
            "<Configure>",
            lambda e: canvas.itemconfigure(wid, width=e.width),
        )
        wid = canvas.create_window((0, 0), window=outer, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)
        canvas.grid(row=0, column=0, sticky="nsew")
        scrollbar.grid(row=0, column=1, sticky="ns")
        self._bind_canvas_mousewheel(canvas, outer)

        outer.columnconfigure(0, weight=1)

        # Data overview at top
        self._build_overview(outer)

        # Pipeline stages
        pipeline_frame = ttk.LabelFrame(outer, text="Pipeline", padding=12, style="Section.TLabelframe")
        pipeline_frame.grid(row=1, column=0, sticky="nsew", pady=(6, 0))
        pipeline_frame.columnconfigure(1, weight=1)

        prow = 0
        for group_name, stages in _PIPELINE_GROUPS:
            # Group header
            ttk.Label(
                pipeline_frame,
                text=group_name,
                font=("Helvetica", 9, "bold"),
                foreground=_SECTION_FG,
            ).grid(row=prow, column=0, columnspan=3, sticky="w", pady=(8 if prow > 0 else 0, 4))
            prow += 1

            for stage_key, display_name in stages:
                btn_text = f"  {display_name}"
                if stage_key == "ingest":
                    cmd = self._spawn_ingest
                else:
                    cmd = lambda s=stage_key: self._spawn_stage(s)

                ttk.Button(
                    pipeline_frame,
                    text=btn_text,
                    width=28,
                    style="Accent.TButton" if stage_key in {"ingest", "extract", "build-release"} else "TButton",
                    command=cmd,
                ).grid(row=prow, column=0, sticky="w", pady=3, padx=(8, 12))

                lbl = tk.Label(
                    pipeline_frame,
                    textvariable=self._status_vars[stage_key],
                    width=10,
                    anchor="w",
                    fg=_STATUS_COLORS["idle"],
                    font=("Helvetica", 9),
                )
                lbl.grid(row=prow, column=1, sticky="w")
                self._status_labels[stage_key] = lbl
                prow += 1

        # Separator before Run All
        ttk.Separator(pipeline_frame, orient="horizontal").grid(
            row=prow, column=0, columnspan=3, sticky="ew", pady=(10, 8),
        )
        prow += 1

        btn_frame = ttk.Frame(pipeline_frame)
        btn_frame.grid(row=prow, column=0, columnspan=3, sticky="ew")
        btn_frame.columnconfigure(0, weight=1)
        btn_frame.columnconfigure(1, weight=1)

        ttk.Button(
            btn_frame,
            text="Run Full Pipeline",
            style="Accent.TButton",
            command=self._spawn_all,
        ).grid(row=0, column=0, sticky="ew", padx=(0, 4))

        ttk.Button(
            btn_frame,
            text="Refresh Overview",
            command=self._refresh_overview,
        ).grid(row=0, column=1, sticky="ew", padx=(4, 0))

    def _build_overview(self, parent: tk.Frame) -> None:
        overview = ttk.LabelFrame(parent, text="Data Overview", padding=10, style="Section.TLabelframe")
        overview.grid(row=0, column=0, sticky="ew")
        overview.columnconfigure(1, weight=1)
        overview.columnconfigure(3, weight=1)

        ttk.Label(
            overview,
            text="Recommended flow: save sources and search criteria, run ingest -> extract -> review root exports, then graph/features/training/splits/release.",
            font=("Helvetica", 8),
            foreground="#666666",
        ).grid(row=0, column=0, columnspan=4, sticky="w", pady=(0, 6))

        items = [
            ("raw_rcsb",     "Raw RCSB entries:"),
            ("raw_skempi",   "SKEMPI CSV:"),
            ("processed",    "Processed records:"),
            ("extracted",    "Extracted entries:"),
            ("chains",       "Chains:"),
            ("bound_objects", "Bound objects:"),
            ("assays",       "Assay records:"),
            ("graph_nodes",  "Graph nodes:"),
            ("graph_edges",  "Graph edges:"),
            ("splits",       "Split files:"),
        ]

        for i, (key, label) in enumerate(items):
            col_offset = 0 if i < 5 else 2
            r = (i if i < 5 else i - 5) + 1
            self._overview_vars[key] = tk.StringVar(value="--")

            ttk.Label(
                overview, text=label,
                font=("Helvetica", 8),
            ).grid(row=r, column=col_offset, sticky="w", padx=(0, 4), pady=1)

            ttk.Label(
                overview,
                textvariable=self._overview_vars[key],
                font=("Helvetica", 8, "bold"),
            ).grid(row=r, column=col_offset + 1, sticky="w", padx=(0, 16), pady=1)

        review_frame = ttk.LabelFrame(overview, text="Root Review Exports", padding=8, style="Section.TLabelframe")
        review_frame.grid(row=6, column=0, columnspan=4, sticky="ew", pady=(8, 0))
        review_frame.columnconfigure(1, weight=1)

        review_items = [
            ("master_csv", "Entry CSV:"),
            ("pair_csv", "Pair CSV:"),
            ("issue_csv", "Issue CSV:"),
            ("conflict_csv", "Conflict CSV:"),
            ("source_state_csv", "Source State CSV:"),
        ]
        for r, (key, label) in enumerate(review_items):
            self._review_export_vars[key] = tk.StringVar(value="--")
            ttk.Label(
                review_frame,
                text=label,
                font=("Helvetica", 8),
            ).grid(row=r, column=0, sticky="nw", padx=(0, 4), pady=1)
            ttk.Label(
                review_frame,
                textvariable=self._review_export_vars[key],
                font=("Helvetica", 8),
            ).grid(row=r, column=1, sticky="w", pady=1)

        ttk.Button(
            review_frame,
            text="Refresh Root Exports",
            command=self._refresh_review_exports,
        ).grid(row=0, column=2, rowspan=5, sticky="ns", padx=(8, 0))

        ttk.Button(
            review_frame,
            text="Open Repo Root",
            command=lambda: self._open_path(Path.cwd()),
        ).grid(row=0, column=3, rowspan=5, sticky="ns", padx=(8, 0))

        release_frame = ttk.LabelFrame(overview, text="Release Artifacts", padding=8, style="Section.TLabelframe")
        release_frame.grid(row=7, column=0, columnspan=4, sticky="ew", pady=(8, 0))
        release_frame.columnconfigure(1, weight=1)

        release_items = [
            ("model_ready_pairs_csv", "Model-ready CSV:"),
            ("custom_training_set_csv", "Custom training CSV:"),
            ("custom_training_summary_json", "Custom training summary:"),
            ("release_manifest_json", "Release manifest:"),
            ("split_summary_csv", "Split summary:"),
            ("scientific_coverage_json", "Scientific coverage:"),
            ("latest_release_json", "Latest release:"),
        ]
        for r, (key, label) in enumerate(release_items):
            self._review_export_vars[key] = tk.StringVar(value="--")
            ttk.Label(
                release_frame,
                text=label,
                font=("Helvetica", 8),
            ).grid(row=r, column=0, sticky="nw", padx=(0, 4), pady=1)
            ttk.Label(
                release_frame,
                textvariable=self._review_export_vars[key],
                font=("Helvetica", 8),
            ).grid(row=r, column=1, sticky="w", pady=1)

        ttk.Button(
            release_frame,
            text="Open Latest Release",
            command=self._open_latest_release_dir,
        ).grid(row=0, column=2, rowspan=len(release_items), sticky="ns", padx=(8, 0))

        health_frame = ttk.LabelFrame(overview, text="Review Health", padding=8, style="Section.TLabelframe")
        health_frame.grid(row=8, column=0, columnspan=4, sticky="ew", pady=(8, 0))
        health_frame.columnconfigure(1, weight=1)
        health_items = [
            ("readiness", "Release readiness:"),
            ("coverage", "Coverage snapshot:"),
            ("quality", "Quality snapshot:"),
            ("next_action", "Recommended next step:"),
        ]
        for r, (key, label) in enumerate(health_items):
            ttk.Label(
                health_frame,
                text=label,
                font=("Segoe UI Semibold", 8),
            ).grid(row=r, column=0, sticky="nw", padx=(0, 6), pady=1)
            ttk.Label(
                health_frame,
                textvariable=self._review_health_vars[key],
                wraplength=760,
                justify="left",
                font=("Segoe UI", 8),
            ).grid(row=r, column=1, sticky="w", pady=1)

        help_frame = ttk.LabelFrame(overview, text="Interpretation Guide", padding=8, style="Section.TLabelframe")
        help_frame.grid(row=9, column=0, columnspan=4, sticky="ew", pady=(8, 0))
        help_frame.columnconfigure(0, weight=1)
        help_lines = [
            "High confidence means a field came from direct structured data or a deterministic merge with no unresolved ambiguity.",
            "Medium or low confidence usually indicates fallback logic, partial source coverage, or unresolved biological context.",
            "A conflicted pair means multiple sources reported materially divergent values for the same pair and assay type.",
            "Model-ready outputs are conservative by design. Excluded pairs stay out until the blockers are resolved or accepted.",
        ]
        for r, text in enumerate(help_lines):
            ttk.Label(
                help_frame,
                text=f"• {text}",
                wraplength=980,
                justify="left",
                style="Muted.TLabel",
            ).grid(row=r, column=0, sticky="w", pady=1)

        actions_frame = ttk.LabelFrame(overview, text="Quick Actions", padding=8, style="Section.TLabelframe")
        actions_frame.grid(row=10, column=0, columnspan=4, sticky="ew", pady=(8, 0))
        for index in range(5):
            actions_frame.columnconfigure(index, weight=1)
        ttk.Button(
            actions_frame,
            text="Open Filtered Review CSV",
            command=lambda: self._open_path(self._filtered_review_csv_path()),
        ).grid(row=0, column=0, sticky="ew", padx=(0, 4))
        ttk.Button(
            actions_frame,
            text="Open Model-ready CSV",
            command=lambda: self._open_path(self._existing_review_path("model_ready_pairs_csv")),
        ).grid(row=0, column=1, sticky="ew", padx=4)
        ttk.Button(
            actions_frame,
            text="Open Custom Training Set",
            command=lambda: self._open_path(self._existing_review_path("custom_training_set_csv")),
        ).grid(row=0, column=2, sticky="ew", padx=4)
        ttk.Button(
            actions_frame,
            text="Open Coverage Summary",
            command=lambda: self._open_path(self._existing_review_path("scientific_coverage_json")),
        ).grid(row=0, column=3, sticky="ew", padx=4)
        ttk.Button(
            actions_frame,
            text="Open Storage Root",
            command=lambda: self._open_path(self._storage_layout().root),
        ).grid(row=0, column=4, sticky="ew", padx=(4, 0))

    def _refresh_overview(self) -> None:
        layout = self._storage_layout()
        counts = {
            "raw_rcsb":      _count_files(layout.raw_rcsb_dir),
            "raw_skempi":    "Yes" if (layout.raw_skempi_dir / "skempi_v2.csv").exists() else "No",
            "processed":     _count_files(layout.processed_rcsb_dir),
            "extracted":     _count_files(layout.extracted_dir / "entry"),
            "chains":        _count_files(layout.extracted_dir / "chains"),
            "bound_objects": _count_files(layout.extracted_dir / "bound_objects"),
            "assays":        _count_files(layout.extracted_dir / "assays"),
            "graph_nodes":   _count_files(layout.graph_dir, "graph_nodes*"),
            "graph_edges":   _count_files(layout.graph_dir, "graph_edges*"),
            "splits":        _count_files(layout.splits_dir, "*.txt"),
        }
        for key, val in counts.items():
            display = str(val) if isinstance(val, str) else f"{val:,}"
            self._overview_vars[key].set(display)
        for key, path in self._review_export_paths().items():
            self._review_export_vars[key].set(path if path else "--")
        coverage_path = self._existing_review_path("scientific_coverage_json")
        summary = build_review_health_summary(_load_json_dict(coverage_path)) if coverage_path else build_review_health_summary({})
        for key, value in summary.items():
            if key in self._review_health_vars:
                self._review_health_vars[key].set(value)

    def _review_export_paths(self) -> dict[str, str]:
        repo_root = Path.cwd()
        latest_release_path = self._storage_layout().releases_dir / "latest_release.json"
        names = {
            "master_csv": "master_pdb_repository.csv",
            "pair_csv": "master_pdb_pairs.csv",
            "issue_csv": "master_pdb_issues.csv",
            "conflict_csv": "master_pdb_conflicts.csv",
            "source_state_csv": "master_source_state.csv",
            "model_ready_pairs_csv": "model_ready_pairs.csv",
            "custom_training_set_csv": "custom_training_set.csv",
            "custom_training_summary_json": "custom_training_summary.json",
            "release_manifest_json": "dataset_release_manifest.json",
            "split_summary_csv": "split_summary.csv",
            "scientific_coverage_json": "scientific_coverage_summary.json",
        }
        paths: dict[str, str] = {}
        for key, filename in names.items():
            path = repo_root / filename if not Path(filename).is_absolute() else Path(filename)
            paths[key] = str(path) if path.exists() else ""
        paths["latest_release_json"] = str(latest_release_path) if latest_release_path.exists() else ""
        return paths

    def _refresh_review_exports(self) -> None:
        from pbdata.master_export import refresh_master_exports

        layout = self._storage_layout()
        export_status = refresh_master_exports(layout)
        if "master_csv" in export_status:
            self._log_line(f"Root export refreshed: {export_status['master_csv']}")
        if "pair_csv" in export_status:
            self._log_line(f"Root export refreshed: {export_status['pair_csv']}")
        if "issue_csv" in export_status:
            self._log_line(f"Root export refreshed: {export_status['issue_csv']}")
        if "conflict_csv" in export_status:
            self._log_line(f"Root export refreshed: {export_status['conflict_csv']}")
        if "source_state_csv" in export_status:
            self._log_line(f"Root export refreshed: {export_status['source_state_csv']}")
        if "model_ready_pairs_csv" in export_status:
            self._log_line(f"Root export refreshed: {export_status['model_ready_pairs_csv']}")
        if "release_manifest_json" in export_status:
            self._log_line(f"Root export refreshed: {export_status['release_manifest_json']}")
        if "split_summary_csv" in export_status:
            self._log_line(f"Root export refreshed: {export_status['split_summary_csv']}")
        if "scientific_coverage_json" in export_status:
            self._log_line(f"Root export refreshed: {export_status['scientific_coverage_json']}")
        for key in ("master_csv_error", "pair_csv_error", "issue_csv_error", "conflict_csv_error", "source_state_csv_error"):
            if key in export_status:
                self._log_line(f"Root export warning: {export_status[key]}")
        if "release_exports_error" in export_status:
            self._log_line(f"Root export warning: {export_status['release_exports_error']}")
        self._refresh_overview()

    def _filtered_review_csv_path(self) -> Path:
        return Path.cwd() / _FILTERED_REVIEW_CSV_NAME

    def _apply_local_review_filters(self) -> None:
        review_paths = self._review_export_paths()
        if not review_paths.get("master_csv") or not review_paths.get("pair_csv") or not review_paths.get("issue_csv"):
            self._log_line(
                "Local review filter needs the root master, pair, and issue CSVs. Refresh root exports first."
            )
            self._review_filtered_count_var.set("Filtered review rows: unavailable")
            return

        rows = build_filtered_review_rows(
            _load_csv_dict_rows(Path(review_paths["master_csv"])),
            _load_csv_dict_rows(Path(review_paths["pair_csv"])),
            _load_csv_dict_rows(Path(review_paths["issue_csv"])),
            pdb_query=self._review_pdb_query_var.get().strip(),
            pair_query=self._review_pair_query_var.get().strip(),
            issue_type=self._review_issue_type_var.get().strip() or "All",
            confidence_filter=self._review_confidence_var.get().strip() or "All",
            conflict_only=self._review_conflict_only_var.get(),
            mutation_ambiguous_only=self._review_mutation_ambiguous_only_var.get(),
            metal_only=self._review_metal_only_var.get(),
            cofactor_only=self._review_cofactor_only_var.get(),
            glycan_only=self._review_glycan_only_var.get(),
        )
        out_path = self._filtered_review_csv_path()
        columns = [
            "scope",
            "pdb_id",
            "pair_identity_key",
            "title",
            "issue_types",
            "issue_details",
            "source_conflict_flag",
            "source_conflict_summary",
            "source_agreement_band",
            "selected_preferred_source",
            "binding_affinity_type",
            "membrane_vs_soluble",
            "metal_present",
            "cofactor_present",
            "glycan_present",
            "quality_flags",
            "field_confidence_json",
            "assay_field_confidence_json",
        ]
        with out_path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=columns)
            writer.writeheader()
            writer.writerows(rows)
        self._review_filtered_count_var.set(f"Filtered review rows: {len(rows):,}")
        self._log_line(f"Filtered review CSV written to {out_path} ({len(rows):,} rows)")

    def _reset_local_review_filters(self) -> None:
        self._review_pdb_query_var.set("")
        self._review_pair_query_var.set("")
        self._review_issue_type_var.set("All")
        self._review_confidence_var.set("All")
        self._review_conflict_only_var.set(False)
        self._review_mutation_ambiguous_only_var.set(False)
        self._review_metal_only_var.set(False)
        self._review_cofactor_only_var.set(False)
        self._review_glycan_only_var.set(False)
        self._review_filtered_count_var.set("--")

    # --- Log panel ---

    def _build_log_panel(self) -> None:
        frame = ttk.LabelFrame(self._root, text="Log", padding=8)
        frame.grid(row=2, column=0, columnspan=2, sticky="nsew", padx=10, pady=(0, 10))
        frame.columnconfigure(0, weight=1)
        frame.rowconfigure(0, weight=1)

        self._log = scrolledtext.ScrolledText(
            frame,
            state="disabled",
            font=("Courier", 9),
            bg=_LOG_BG, fg=_LOG_FG,
            insertbackground="white",
            relief="flat",
        )
        self._log.grid(row=0, column=0, sticky="nsew")
        self._bind_text_mousewheel(self._log)

        btn_frame = ttk.Frame(frame)
        btn_frame.grid(row=1, column=0, sticky="ew", pady=(5, 0))

        ttk.Button(btn_frame, text="Clear Log", command=self._clear_log).pack(
            side="right",
        )

    # ------------------------------------------------------------------
    # Config persistence
    # ------------------------------------------------------------------

    def _load_sources_into_ui(self) -> None:
        enabled, paths, storage_root = _load_sources_config()
        for src, var in self._src_enabled.items():
            var.set(enabled.get(src, False))
        for src, var in self._src_path_vars.items():
            var.set(paths.get(src, ""))
        self._storage_root_var.set(storage_root)
        self._structure_mirror_var.set(_load_structure_mirror())

    def _save_sources(self) -> None:
        _save_sources_config(
            {s: v.get() for s, v in self._src_enabled.items()},
            {s: v.get() for s, v in self._src_path_vars.items()},
            storage_root=self._storage_root_var.get().strip() or str(Path.cwd()),
            structure_mirror=self._structure_mirror_var.get().strip().lower() or "rcsb",
        )
        self._log_line(
            f"Source config saved to configs/sources.yaml "
            f"(storage root: {self._storage_root_var.get().strip() or Path.cwd()}, "
            f"structure mirror: {self._structure_mirror_var.get().strip().lower() or 'rcsb'})"
        )

    def _load_criteria_into_ui(self) -> None:
        sc = load_criteria(_CRITERIA_PATH)
        self._pdb_ids_var.set(",".join(sc.direct_pdb_ids))
        self._keyword_query_var.set(sc.keyword_query or "")
        self._organism_name_var.set(sc.organism_name_query or "")
        self._taxonomy_id_var.set("" if sc.taxonomy_id is None else str(sc.taxonomy_id))
        for key, var in self._method_vars.items():
            var.set(key in sc.experimental_methods)
        self._resolution_var.set(resolution_value_to_label(sc.max_resolution_angstrom))
        for key, var in self._task_vars.items():
            var.set(key in sc.task_types)
        self._membrane_only_var.set(sc.membrane_only)
        self._require_multimer_var.set(sc.require_multimer)
        self._require_protein_var.set(sc.require_protein)
        self._require_ligand_var.set(sc.require_ligand)
        self._require_branched_entities_var.set(sc.require_branched_entities)
        self._min_protein_entities_var.set(
            "" if sc.min_protein_entities is None else str(sc.min_protein_entities),
        )
        self._min_nonpolymer_entities_var.set(
            "" if sc.min_nonpolymer_entities is None else str(sc.min_nonpolymer_entities),
        )
        self._max_nonpolymer_entities_var.set(
            "" if sc.max_nonpolymer_entities is None else str(sc.max_nonpolymer_entities),
        )
        self._min_branched_entities_var.set(
            "" if sc.min_branched_entities is None else str(sc.min_branched_entities),
        )
        self._max_branched_entities_var.set(
            "" if sc.max_branched_entities is None else str(sc.max_branched_entities),
        )
        self._min_assembly_count_var.set(
            "" if sc.min_assembly_count is None else str(sc.min_assembly_count),
        )
        self._max_assembly_count_var.set(
            "" if sc.max_assembly_count is None else str(sc.max_assembly_count),
        )
        self._max_atom_count_var.set(
            "" if sc.max_deposited_atom_count is None else str(sc.max_deposited_atom_count),
        )
        self._min_year_var.set("" if sc.min_release_year is None else str(sc.min_release_year))
        self._max_year_var.set("" if sc.max_release_year is None else str(sc.max_release_year))

    def _criteria_from_ui(self) -> SearchCriteria:
        methods = [k for k, v in self._method_vars.items() if v.get()]
        task_types = [k for k, v in self._task_vars.items() if v.get()]
        direct_pdb_ids = [
            pid.strip().upper()
            for pid in self._pdb_ids_var.get().replace(";", ",").split(",")
            if pid.strip()
        ]
        keyword_query = self._keyword_query_var.get().strip() or None
        organism_name_query = self._organism_name_var.get().strip() or None
        taxonomy_str = self._taxonomy_id_var.get().strip()
        min_protein_str = self._min_protein_entities_var.get().strip()
        min_nonpolymer_str = self._min_nonpolymer_entities_var.get().strip()
        max_nonpolymer_str = self._max_nonpolymer_entities_var.get().strip()
        min_branched_str = self._min_branched_entities_var.get().strip()
        max_branched_str = self._max_branched_entities_var.get().strip()
        min_assembly_str = self._min_assembly_count_var.get().strip()
        max_assembly_str = self._max_assembly_count_var.get().strip()
        max_atom_str = self._max_atom_count_var.get().strip()
        min_year_str = self._min_year_var.get().strip()
        max_year_str = self._max_year_var.get().strip()
        taxonomy_id: int | None = int(taxonomy_str) if taxonomy_str.isdigit() else None
        min_protein_entities: int | None = int(min_protein_str) if min_protein_str.isdigit() else None
        min_nonpolymer_entities: int | None = int(min_nonpolymer_str) if min_nonpolymer_str.isdigit() else None
        max_nonpolymer_entities: int | None = int(max_nonpolymer_str) if max_nonpolymer_str.isdigit() else None
        min_branched_entities: int | None = int(min_branched_str) if min_branched_str.isdigit() else None
        max_branched_entities: int | None = int(max_branched_str) if max_branched_str.isdigit() else None
        min_assembly_count: int | None = int(min_assembly_str) if min_assembly_str.isdigit() else None
        max_assembly_count: int | None = int(max_assembly_str) if max_assembly_str.isdigit() else None
        max_deposited_atom_count: int | None = int(max_atom_str) if max_atom_str.isdigit() else None
        min_year: int | None = int(min_year_str) if min_year_str.isdigit() else None
        max_year: int | None = int(max_year_str) if max_year_str.isdigit() else None
        return SearchCriteria(
            direct_pdb_ids=direct_pdb_ids,
            keyword_query=keyword_query,
            organism_name_query=organism_name_query,
            taxonomy_id=taxonomy_id,
            experimental_methods=methods,
            max_resolution_angstrom=resolution_label_to_value(self._resolution_var.get()),
            task_types=task_types,
            membrane_only=self._membrane_only_var.get(),
            require_multimer=self._require_multimer_var.get(),
            require_protein=self._require_protein_var.get(),
            require_ligand=self._require_ligand_var.get(),
            require_branched_entities=self._require_branched_entities_var.get(),
            min_protein_entities=min_protein_entities,
            min_nonpolymer_entities=min_nonpolymer_entities,
            max_nonpolymer_entities=max_nonpolymer_entities,
            min_branched_entities=min_branched_entities,
            max_branched_entities=max_branched_entities,
            min_assembly_count=min_assembly_count,
            max_assembly_count=max_assembly_count,
            max_deposited_atom_count=max_deposited_atom_count,
            min_release_year=min_year,
            max_release_year=max_year,
        )

    def _save_criteria(self) -> None:
        sc = self._criteria_from_ui()
        save_criteria(sc, _CRITERIA_PATH)
        self._log_line("Criteria saved to configs/criteria.yaml")

    # ------------------------------------------------------------------
    # Logging helpers
    # ------------------------------------------------------------------

    def _log_line(self, text: str) -> None:
        self._log.configure(state="normal")
        self._log.insert("end", text.rstrip() + "\n")
        self._log.see("end")
        self._log.configure(state="disabled")

    def _clear_log(self) -> None:
        self._log.configure(state="normal")
        self._log.delete("1.0", "end")
        self._log.configure(state="disabled")

    def _existing_review_path(self, key: str) -> Path | None:
        path = self._review_export_paths().get(key, "")
        return Path(path) if path else None

    def _open_path(self, path: Path | None) -> None:
        if path is None:
            self._log_line("Open path failed: no path available.")
            return
        if not path.exists():
            self._log_line(f"Open path failed: {path} does not exist.")
            return
        try:
            os.startfile(str(path))  # type: ignore[attr-defined]
        except AttributeError:
            try:
                subprocess.Popen(["xdg-open", str(path)], cwd=Path.cwd())
            except Exception as exc:
                self._log_line(f"Open path failed: {exc}")
                return
        except Exception as exc:
            self._log_line(f"Open path failed: {exc}")
            return
        self._log_line(f"Opened: {path}")

    def _open_latest_release_dir(self) -> None:
        latest_path = self._existing_review_path("latest_release_json")
        if latest_path is None or not latest_path.exists():
            self._log_line("Latest release pointer not available yet.")
            return
        try:
            payload = json.loads(latest_path.read_text(encoding="utf-8"))
        except Exception as exc:
            self._log_line(f"Latest release pointer is unreadable: {exc}")
            return
        snapshot_dir = Path(str(payload.get("snapshot_dir") or ""))
        if not snapshot_dir.exists():
            self._log_line("Latest release snapshot directory does not exist.")
            return
        self._open_path(snapshot_dir)

    def _on_close(self) -> None:
        self._closing = True
        for proc in list(self._active_processes):
            try:
                if proc.poll() is None:
                    proc.terminate()
            except Exception:
                continue

        deadline = time.time() + 3.0
        while time.time() < deadline:
            remaining = [proc for proc in self._active_processes if proc.poll() is None]
            if not remaining:
                break
            time.sleep(0.1)

        for proc in list(self._active_processes):
            try:
                if proc.poll() is None:
                    proc.kill()
            except Exception:
                continue

        self._root.destroy()

    # ------------------------------------------------------------------
    # Status
    # ------------------------------------------------------------------

    def _set_status(self, stage: str, status: str) -> None:
        self._status_vars[stage].set(status)
        self._status_labels[stage].configure(fg=_STATUS_COLORS.get(status, "#888"))

    # ------------------------------------------------------------------
    # Subprocess stages
    # ------------------------------------------------------------------

    def _build_stage_cmd(self, stage: str) -> list[str]:
        """Build the CLI command for a pipeline stage, including options."""
        cmd = [sys.executable, "-m", "pbdata.cli", stage]
        storage_root = self._storage_root_var.get().strip()
        workers = self._workers_var.get().strip()

        if storage_root:
            cmd[3:3] = ["--storage-root", storage_root]

        if stage in {"extract", "normalize", "audit"} and workers:
            cmd.extend(["--workers", workers])

        if stage == "build-structural-graphs":
            level = self._structural_graph_level_var.get().strip()
            scope = self._structural_graph_scope_var.get().strip()
            exports = [part.strip() for part in self._structural_graph_exports_var.get().split(",") if part.strip()]
            if level:
                cmd.extend(["--graph-level", level])
            if scope:
                cmd.extend(["--scope", scope])
            for export_format in exports:
                cmd.extend(["--export-format", export_format])
        elif stage == "extract":
            if not self._download_structures_var.get():
                cmd.append("--no-download-structures")
            if self._download_pdb_var.get():
                cmd.append("--download-pdb")
        elif stage == "run-feature-pipeline":
            cmd.extend(["--run-mode", "full_build"])
            if workers:
                cmd.extend(["--workers", workers])
            if self._site_pipeline_degraded_mode_var.get():
                cmd.append("--degraded-mode")
            else:
                cmd.append("--no-degraded-mode")
            run_id = self._site_pipeline_run_id_var.get().strip()
            if run_id:
                cmd.extend(["--run-id", run_id])
        elif stage == "export-analysis-queue":
            run_id = self._site_pipeline_run_id_var.get().strip()
            if run_id:
                cmd.extend(["--run-id", run_id])
        elif stage == "ingest-physics-results":
            batch_id = self._site_physics_batch_id_var.get().strip()
            if batch_id:
                cmd.extend(["--batch-id", batch_id])
        elif stage == "train-site-physics-surrogate":
            batch_id = self._site_physics_batch_id_var.get().strip()
            run_id = self._site_pipeline_run_id_var.get().strip()
            if batch_id:
                cmd.extend(["--batch-id", batch_id])
            if run_id:
                cmd.extend(["--source-run-id", run_id])

        elif stage == "build-splits":
            split_mode = self._split_mode_var.get().strip()
            train = self._train_frac_var.get().strip()
            val = self._val_frac_var.get().strip()
            seed = self._split_seed_var.get().strip()
            threshold = self._jaccard_threshold_var.get().strip()
            if split_mode:
                cmd.extend(["--split-mode", split_mode])
            if train:
                cmd.extend(["--train-frac", train])
            if val:
                cmd.extend(["--val-frac", val])
            if seed:
                cmd.extend(["--seed", seed])
            if self._hash_only_var.get():
                cmd.append("--hash-only")
            elif threshold:
                cmd.extend(["--threshold", threshold])
        elif stage == "build-custom-training-set":
            mode = self._custom_set_mode_var.get().strip()
            target_size = self._custom_set_target_size_var.get().strip()
            seed = self._custom_set_seed_var.get().strip()
            cluster_cap = self._custom_set_cluster_cap_var.get().strip()
            if mode:
                cmd.extend(["--mode", mode])
            if target_size:
                cmd.extend(["--target-size", target_size])
            if seed:
                cmd.extend(["--seed", seed])
            if cluster_cap:
                cmd.extend(["--per-receptor-cluster-cap", cluster_cap])
        elif stage == "engineer-dataset":
            dataset_name = self._engineered_dataset_name_var.get().strip()
            test_frac = self._engineered_dataset_test_frac_var.get().strip()
            cv_folds = self._engineered_dataset_cv_folds_var.get().strip()
            cluster_count = self._engineered_dataset_cluster_count_var.get().strip()
            embedding_backend = self._engineered_dataset_embedding_backend_var.get().strip()
            if dataset_name:
                cmd.extend(["--dataset-name", dataset_name])
            if test_frac:
                cmd.extend(["--test-frac", test_frac])
            if cv_folds:
                cmd.extend(["--cv-folds", cv_folds])
            if cluster_count:
                cmd.extend(["--cluster-count", cluster_count])
            if embedding_backend:
                cmd.extend(["--embedding-backend", embedding_backend])
            if self._engineered_dataset_strict_family_var.get():
                cmd.append("--strict-family-isolation")
        elif stage == "build-release":
            release_tag = self._release_tag_var.get().strip()
            if release_tag:
                cmd.extend(["--tag", release_tag])

        return cmd

    def _run_stage(self, stage: str) -> str:
        """Run one CLI stage via subprocess (call from a background thread).

        Returns 'done' or 'error'.
        """
        if self._closing:
            return "cancelled"
        self._root.after(0, self._set_status, stage, "running")
        self._root.after(0, self._log_line, f"\n{'─' * 40}")
        self._root.after(0, self._log_line, f"  {stage}")
        self._root.after(0, self._log_line, f"{'─' * 40}")

        cmd = self._build_stage_cmd(stage)
        try:
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                cwd=Path.cwd(),
            )
            self._active_processes.add(proc)
            for line in proc.stdout:  # type: ignore[union-attr]
                if self._closing:
                    break
                self._root.after(0, self._log_line, line)
            if self._closing and proc.poll() is None:
                proc.terminate()
            proc.wait()
            status = "done" if proc.returncode == 0 else "error"
        except Exception as exc:
            self._root.after(0, self._log_line, f"ERROR: {exc}")
            status = "error"
        finally:
            if 'proc' in locals():
                self._active_processes.discard(proc)

        if self._closing:
            return "cancelled"
        self._root.after(0, self._set_status, stage, status)
        self._root.after(0, self._log_line, f"[{stage}] {status}.")
        self._root.after(0, self._refresh_overview)
        return status

    def _spawn_stage(self, stage: str) -> None:
        threading.Thread(target=self._run_stage, args=(stage,), daemon=True).start()

    # ------------------------------------------------------------------
    # Ingest — multi-source aware
    # ------------------------------------------------------------------

    def _spawn_ingest(self) -> None:
        threading.Thread(target=self._run_ingest, daemon=True).start()

    def _run_ingest(self) -> str:
        """Background thread: ingest from all enabled sources.

        Returns 'done', 'error', or 'cancelled'.
        """
        if self._closing:
            return "cancelled"
        self._root.after(0, self._set_status, "ingest", "running")
        self._root.after(0, self._log_line, f"\n{'═' * 40}")
        self._root.after(0, self._log_line, "  Ingest Sources")
        self._root.after(0, self._log_line, f"{'═' * 40}")
        self._root.after(
            0,
            self._log_line,
            f"  Storage root: {self._storage_root_var.get().strip() or Path.cwd()}",
        )

        # Save current config first
        enabled = _call_on_tk_thread(
            self._root,
            lambda: {s: v.get() for s, v in self._src_enabled.items()},
        )
        self._root.after(0, self._save_sources)

        any_enabled = any(enabled.values())
        if not any_enabled:
            self._root.after(0, self._log_line,
                             "No sources enabled. Enable at least one source in the Sources tab.")
            self._root.after(0, self._set_status, "ingest", "error")
            return "error"

        overall_status = "done"
        source_statuses: list[str] = []

        # --- RCSB ---
        if enabled.get("rcsb"):
            status = self._ingest_rcsb()
            source_statuses.append(f"RCSB: {status}")
            if status == "error":
                overall_status = "error"
            elif status == "cancelled":
                overall_status = "cancelled"

        # --- SKEMPI ---
        if enabled.get("skempi"):
            status = self._ingest_skempi()
            source_statuses.append(f"SKEMPI: {status}")
            if status == "error" and overall_status != "cancelled":
                overall_status = "error"

        # --- Enrichment sources (log status only) ---
        for src in ["chembl", "bindingdb"]:
            if enabled.get(src):
                self._root.after(0, self._log_line,
                                 f"\n  {src.upper()}: Enabled (will be queried during Extract)")
                source_statuses.append(f"{src.upper()}: enabled")

        for src in ["pdbbind", "biolip"]:
            if enabled.get(src):
                path_val = _call_on_tk_thread(
                    self._root,
                    lambda s=src: self._src_path_vars[s].get().strip(),
                )
                if path_val and Path(path_val).exists():
                    self._root.after(0, self._log_line,
                                     f"\n  {src.upper()}: Local path found ({path_val})")
                    source_statuses.append(f"{src.upper()}: ready")
                else:
                    self._root.after(0, self._log_line,
                                     f"\n  {src.upper()}: WARNING — local path not set or not found")
                    source_statuses.append(f"{src.upper()}: path missing")

        for src in ["chembl", "bindingdb", "pdbbind", "biolip"]:
            if not enabled.get(src):
                continue
            path_val = _call_on_tk_thread(
                self._root,
                lambda s=src: self._src_path_vars.get(s, tk.StringVar()).get().strip(),
            )
            status, message = _validate_source_path(src, path_val)
            self._root.after(0, self._log_line, f"    readiness: {src.upper()} -> {message}")
            source_statuses.append(f"{src.upper()} validation: {status}")
            if status == "error" and overall_status != "cancelled":
                overall_status = "error"

        # Summary
        self._root.after(0, self._log_line, "\n  Ingest summary:")
        for s in source_statuses:
            self._root.after(0, self._log_line, f"    {s}")

        self._root.after(0, self._set_status, "ingest", overall_status)
        self._root.after(0, self._refresh_overview)
        return overall_status

    def _ingest_rcsb(self) -> str:
        """Ingest RCSB data: either by direct PDB IDs or search criteria.

        Returns 'done', 'error', or 'cancelled'.
        """
        from pbdata.sources.rcsb_search import (
            count_entries,
            fetch_entries_batch,
            search_and_download,
        )
        layout = _call_on_tk_thread(self._root, self._storage_layout)

        self._root.after(0, self._log_line, "\n  RCSB PDB")
        self._root.after(0, self._log_line, "  " + "-" * 36)

        # Check for direct PDB IDs
        pdb_ids_text = _call_on_tk_thread(
            self._root, lambda: self._pdb_ids_var.get().strip(),
        )
        direct_ids = [
            pid.strip().upper()
            for pid in pdb_ids_text.replace(";", ",").split(",")
            if pid.strip()
        ] if pdb_ids_text else []

        if direct_ids:
            return self._ingest_rcsb_by_ids(direct_ids, fetch_entries_batch)

        # Standard search flow
        sc = _call_on_tk_thread(self._root, self._criteria_from_ui)
        save_criteria(sc, _CRITERIA_PATH)

        self._root.after(0, self._log_line, "  Querying RCSB Search API...")
        try:
            count = count_entries(sc)
        except Exception as exc:
            self._root.after(0, self._log_line, f"  Search failed: {exc}")
            return "error"

        self._root.after(0, self._log_line, f"  Found {count:,} matching entries.")

        # Confirm dialog
        proceed_flag: list[bool] = [False]
        event = threading.Event()

        def _ask_user() -> None:
            size_warning = (
                "\n\nLarge download — this may take a while."
                if count > 5_000 else ""
            )
            proceed_flag[0] = messagebox.askyesno(
                "Confirm RCSB Download",
                f"Found {count:,} entries matching your criteria.\n\n"
                f"Proceed with download?{size_warning}",
            )
            event.set()

        self._root.after(0, _ask_user)
        event.wait()

        if not proceed_flag[0]:
            self._root.after(0, self._log_line, "  RCSB download cancelled by user.")
            return "cancelled"

        raw_dir = layout.raw_rcsb_dir
        self._root.after(0, self._log_line, f"  Downloading to {raw_dir} ...")

        def _log(msg: str) -> None:
            self._root.after(0, self._log_line, f"  {msg}")

        try:
            search_and_download(sc, raw_dir, log_fn=_log, manifest_path=layout.catalog_path)
            return "done"
        except Exception as exc:
            self._root.after(0, self._log_line, f"  Download failed: {exc}")
            return "error"

    def _ingest_rcsb_by_ids(
        self,
        pdb_ids: list[str],
        fetch_entries_batch: Callable[[list[str]], list[dict[str, Any]]],
    ) -> str:
        """Fetch specific PDB IDs directly (bypassing search)."""
        self._root.after(0, self._log_line,
                         f"  Direct PDB ID fetch: {len(pdb_ids)} entries")
        self._root.after(0, self._log_line,
                         f"  IDs: {', '.join(pdb_ids[:20])}"
                         + ("..." if len(pdb_ids) > 20 else ""))

        layout = _call_on_tk_thread(self._root, self._storage_layout)
        raw_dir = layout.raw_rcsb_dir
        raw_dir.mkdir(parents=True, exist_ok=True)

        try:
            # Process in batches of 100
            batch_size = 100
            downloaded = 0
            for start in range(0, len(pdb_ids), batch_size):
                batch = pdb_ids[start:start + batch_size]
                to_fetch = [
                    pid for pid in batch
                    if not reuse_existing_file(
                        raw_dir / f"{pid}.json",
                        validator=lambda path, expected=pid: validate_rcsb_raw_json(
                            path, expected_pdb_id=expected,
                        ),
                    )
                ]
                cached = len(batch) - len(to_fetch)
                if cached:
                    self._root.after(0, self._log_line,
                                     f"  Reusing {cached} valid cached entries")

                if to_fetch:
                    entries = fetch_entries_batch(to_fetch)
                    received_ids: set[str] = set()
                    for entry in entries:
                        pid = str(entry.get("rcsb_id") or "")
                        if pid:
                            received_ids.add(pid)
                            (raw_dir / f"{pid}.json").write_text(
                                json.dumps(entry, indent=2), encoding="utf-8",
                            )
                    missing = [pid for pid in to_fetch if pid not in received_ids]
                    if missing:
                        self._root.after(
                            0,
                            self._log_line,
                            f"  WARNING: {len(missing)} requested IDs were not returned: {', '.join(missing[:10])}",
                        )
                downloaded += len(batch)
                self._root.after(0, self._log_line,
                                 f"  {downloaded}/{len(pdb_ids)} processed")

            self._root.after(0, self._log_line,
                             f"  Direct fetch complete: {len(pdb_ids)} entries")
            return "done"
        except Exception as exc:
            self._root.after(0, self._log_line, f"  Direct fetch failed: {exc}")
            return "error"

    def _ingest_skempi(self) -> str:
        """Download SKEMPI v2 CSV if not already present."""
        self._root.after(0, self._log_line, "\n  SKEMPI v2")
        self._root.after(0, self._log_line, "  " + "-" * 36)

        # Check for custom path
        custom_path = _call_on_tk_thread(
            self._root, lambda: self._src_path_vars.get("skempi", tk.StringVar()).get().strip(),
        )

        layout = _call_on_tk_thread(self._root, self._storage_layout)
        out_dir = layout.raw_skempi_dir
        csv_path = Path(custom_path) if custom_path else out_dir / "skempi_v2.csv"

        if reuse_existing_file(csv_path, validator=validate_skempi_csv):
            self._root.after(0, self._log_line,
                             f"  SKEMPI CSV already present at {csv_path}")
            return "done"

        # Download
        try:
            from pbdata.sources.skempi import _SKEMPI_URL
            import requests
        except ImportError as exc:
            self._root.after(0, self._log_line, f"  Import error: {exc}")
            return "error"

        # Confirm
        proceed_flag: list[bool] = [False]
        event = threading.Event()

        def _ask() -> None:
            proceed_flag[0] = messagebox.askyesno(
                "Confirm SKEMPI Download",
                "Download SKEMPI v2 CSV (~3 MB)?",
            )
            event.set()

        self._root.after(0, _ask)
        event.wait()

        if not proceed_flag[0]:
            self._root.after(0, self._log_line, "  SKEMPI download cancelled.")
            return "cancelled"

        self._root.after(0, self._log_line, "  Downloading SKEMPI v2 CSV...")
        try:
            csv_path.parent.mkdir(parents=True, exist_ok=True)
            resp = requests.get(_SKEMPI_URL, timeout=60)
            resp.raise_for_status()
            csv_path.write_text(resp.text, encoding="utf-8")
            if not validate_skempi_csv(csv_path):
                csv_path.unlink(missing_ok=True)
                raise RuntimeError("Downloaded SKEMPI CSV failed validation and was removed.")
            self._root.after(0, self._log_line, f"  Saved to {csv_path}")
            return "done"
        except Exception as exc:
            self._root.after(0, self._log_line, f"  SKEMPI download failed: {exc}")
            return "error"

    # ------------------------------------------------------------------
    # Run All
    # ------------------------------------------------------------------

    def _spawn_all(self) -> None:
        if not self._running.acquire(blocking=False):
            self._log_line("Pipeline already running — please wait.")
            return
        threading.Thread(target=self._run_all_thread, daemon=True).start()

    def _run_all_thread(self) -> None:
        self._root.after(0, self._log_line, f"\n{'═' * 50}")
        self._root.after(0, self._log_line, "  RUN FULL PIPELINE")
        self._root.after(0, self._log_line, f"{'═' * 50}")
        self._root.after(
            0,
            self._log_line,
            f"  Storage root: {self._storage_root_var.get().strip() or Path.cwd()}",
        )
        try:
            # Ingest (in-process with confirm dialog)
            status = self._run_ingest()
            if status == "error":
                self._root.after(0, self._log_line, "\nPipeline stopped: ingest failed.")
                return
            if status == "cancelled":
                self._root.after(0, self._log_line, "\nPipeline cancelled.")
                return

            mode = self._pipeline_execution_mode_var.get().strip() or "hybrid"
            auto_excluded = {"ingest-physics-results", "train-site-physics-surrogate"}
            if mode == "legacy":
                stages = [
                    stage for stage in _SUBPROCESS_STAGES
                    if stage not in {"run-feature-pipeline", "export-analysis-queue", *auto_excluded}
                ]
            elif mode == "site-centric":
                stages = [
                    "setup-workspace",
                    "extract",
                    "harvest-metadata",
                    "build-structural-graphs",
                    "run-feature-pipeline",
                    "export-analysis-queue",
                    "engineer-dataset",
                ]
            else:
                stages = [stage for stage in _SUBPROCESS_STAGES if stage not in auto_excluded]

            # Remaining stages via subprocess
            for stage in stages:
                status = self._run_stage(stage)
                if status == "error":
                    self._root.after(
                        0, self._log_line,
                        f"\nPipeline stopped at '{stage}'.",
                    )
                    return

            self._root.after(0, self._log_line, f"\n{'═' * 50}")
            self._root.after(0, self._log_line, "  PIPELINE COMPLETE")
            self._root.after(0, self._log_line, f"{'═' * 50}")
            self._root.after(0, self._refresh_overview)
        finally:
            self._running.release()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    """Launch the pbdata GUI."""
    root = tk.Tk()
    PbdataGUI(root)
    root.mainloop()


if __name__ == "__main__":
    main()
