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
import io
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
try:
    import cairosvg  # type: ignore
except Exception:  # pragma: no cover - optional GUI dependency
    cairosvg = None
try:
    from PIL import Image, ImageTk  # type: ignore
except Exception:  # pragma: no cover - optional GUI dependency
    Image = None
    ImageTk = None
from pbdata.config import AppConfig, load_config
from pbdata.demo_model_visuals import architecture_spec_for_selection
from pbdata.demo_modeling import simulate_model_training_run, simulate_saved_model_inference
from pbdata.demo_pipeline import simulate_demo_stage
from pbdata.demo_tutorial import DemoTutorialStep, next_demo_tutorial_step
from pbdata.demo_workspace import is_demo_workspace_seeded, seed_demo_workspace
from pbdata.criteria import (
    EXPERIMENTAL_METHODS,
    RESOLUTION_OPTIONS,
    SearchCriteria,
    load_criteria,
    resolution_label_to_value,
    resolution_value_to_label,
    save_criteria,
)
from pbdata.gui_overview import (
    GUIOverviewSnapshot,
    build_curation_review_summary as _build_curation_review_summary,
    build_gui_overview_snapshot,
    build_review_health_summary as _build_review_health_summary,
    build_training_set_builder_summary as _build_training_set_builder_summary,
    build_training_set_kpis as _build_training_set_kpis,
    build_training_set_workflow_status as _build_training_set_workflow_status,
    count_files as _count_files_impl,
    load_csv_dict_rows as _load_csv_dict_rows_impl,
    load_json_dict as _load_json_dict_impl,
    review_export_paths as _review_export_paths_impl,
)
from pbdata.modeling.studio import (
    build_dataset_profile,
    build_starter_model_config,
    export_starter_model_config,
    recommend_model_architectures,
    validate_model_studio_selection,
    ModelStudioSelection,
    ModelRecommendation,
    DatasetProfile,
)
from pbdata.modeling.runtime import (
    detect_runtime_capabilities,
    export_training_package,
)
from pbdata.modeling.training_runs import (
    build_training_run_report,
    compare_training_runs,
    execute_training_run,
    import_training_run,
    inspect_training_run,
    run_saved_model_inference,
)
from pbdata.storage import (
    build_storage_layout,
    reuse_existing_file,
    validate_rcsb_raw_json,
    validate_skempi_csv,
)
from pbdata.sources.registry import list_source_descriptors

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_SOURCE_DESCRIPTORS = list_source_descriptors()
_SOURCE_DESCRIPTOR_BY_NAME = {descriptor.name: descriptor for descriptor in _SOURCE_DESCRIPTORS}
_SOURCES = [descriptor.name for descriptor in _SOURCE_DESCRIPTORS]
_SOURCE_PATH_FIELDS = {
    descriptor.name: str(descriptor.local_path_field)
    for descriptor in _SOURCE_DESCRIPTORS
    if descriptor.local_path_field
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
    "report-bias", "export-demo-snapshot", "report-source-capabilities", "export-identity-crosswalk",
    "build-conformational-states", "build-graph", "build-microstates", "build-physics-features", "build-microstate-refinement", "build-mm-job-manifests", "run-mm-jobs", "run-feature-pipeline", "export-analysis-queue", "ingest-physics-results", "train-site-physics-surrogate", "build-features", "build-training-examples", "build-splits", "train-baseline-model", "evaluate-baseline-model", "build-custom-training-set", "build-release", "run-scenario-tests",
]

# Pipeline groups for the UI
_PIPELINE_GROUPS: list[tuple[str, list[tuple[str, str]]]] = [
    ("Workflow Engine", [
        ("setup-workspace", "Setup Workspace"),
        ("harvest-metadata", "Harvest Metadata"),
        ("export-demo-snapshot", "Export Demo Snapshot"),
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
        ("build-graph", "Build Graph"),
        ("build-microstates", "Build Microstates"),
        ("build-physics-features", "Build Physics Features"),
        ("run-feature-pipeline", "Run Site-Centric Feature Pipeline"),
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
    ("Experimental & Preview", [
        ("build-conformational-states", "Build Conformational States (Preview)"),
        ("build-microstate-refinement", "Build Microstate Refinement (Experimental)"),
        ("build-mm-job-manifests", "Build MM Job Manifests (Experimental)"),
        ("run-mm-jobs", "Run MM Jobs (Experimental)"),
        ("export-analysis-queue", "Export Analysis Queue (Experimental)"),
        ("ingest-physics-results", "Ingest Physics Results (Experimental)"),
        ("train-site-physics-surrogate", "Train Site-Physics Surrogate (Experimental)"),
    ]),
]

_ALL_STAGE_KEYS = [key for _, stages in _PIPELINE_GROUPS for key, _ in stages]
_STAGE_DISPLAY_NAMES = {
    "ingest": "Ingest Sources",
    **{key: label for _, stages in _PIPELINE_GROUPS for key, label in stages},
}

_CRITERIA_PATH = Path("configs/criteria.yaml")
_SOURCES_CFG   = Path("configs/sources.yaml")

_STATUS_COLORS = {
    "idle":      "#6b7280",
    "running":   "#f59e0b",
    "done":      "#10b981",
    "error":     "#ef4444",
    "skipped":   "#94a3b8",
}

_HEADER_BG   = "#07111f"
_HEADER_FG   = "#f8fafc"
_ACCENT_BG   = "#14b8a6"
_ACCENT_BG_ACTIVE = "#0f766e"
_ACCENT_FG   = "#052e2b"
_SECTION_FG  = "#0f172a"
_LOG_BG      = "#081120"
_LOG_FG      = "#dbe7f5"
_OVERVIEW_BG = "#eef4fb"
_APP_BG      = "#edf3f9"
_CARD_BG     = "#fbfdff"
_CARD_BORDER = "#d6e2f0"
_CARD_ALT_BG = "#f4f8fc"
_MUTED_FG    = "#64748b"
_SUCCESS_FG  = "#0f766e"
_WARNING_FG  = "#b45309"
_ERROR_FG    = "#b91c1c"
_HEADER_SUB_FG = "#9fb3c8"
_HEADER_TAG_FG = "#7dd3fc"
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
_EXPERIMENTAL_STAGE_KEYS = {
    "build-conformational-states",
    "build-microstate-refinement",
    "build-mm-job-manifests",
    "run-mm-jobs",
    "export-analysis-queue",
    "ingest-physics-results",
    "train-site-physics-surrogate",
}

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
    configured_root = Path(str(raw.get("storage_root") or Path.cwd()))
    storage_root = str(configured_root if configured_root.exists() else Path.cwd())
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
    return _count_files_impl(directory, pattern)


def _mousewheel_units(event: Any) -> int:
    if getattr(event, "delta", 0):
        return -1 * int(event.delta / 120)
    return -1 if getattr(event, "num", None) == 4 else 1


def _load_csv_dict_rows(path: Path) -> list[dict[str, str]]:
    return _load_csv_dict_rows_impl(path)


def _load_json_dict(path: Path) -> dict[str, Any]:
    return _load_json_dict_impl(path)


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
    return _build_review_health_summary(coverage)


def build_training_set_builder_summary(
    scorecard: dict[str, Any],
    benchmark_rows: list[dict[str, str]],
) -> dict[str, str]:
    return _build_training_set_builder_summary(scorecard, benchmark_rows)


def build_training_set_kpis(
    scorecard: dict[str, Any],
    benchmark_rows: list[dict[str, str]],
) -> dict[str, str]:
    return _build_training_set_kpis(scorecard, benchmark_rows)


def build_training_set_workflow_status(
    review_paths: dict[str, str],
) -> list[tuple[str, str]]:
    return _build_training_set_workflow_status(review_paths)


def build_curation_review_summary(
    exclusion_rows: list[dict[str, str]],
    conflict_rows: list[dict[str, str]],
    issue_rows: list[dict[str, str]],
) -> dict[str, str]:
    return _build_curation_review_summary(exclusion_rows, conflict_rows, issue_rows)


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
        self._max_results_var         = tk.StringVar(value="")
        self._representative_sampling_var = tk.BooleanVar(value=True)
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
        self._workers_var             = tk.StringVar(value=str(min(max(os.cpu_count() or 1, 1), 4)))
        self._pipeline_execution_mode_var = tk.StringVar(value="hybrid")
        self._skip_experimental_stages_var = tk.BooleanVar(value=True)
        self._site_pipeline_degraded_mode_var = tk.BooleanVar(value=True)
        self._site_pipeline_run_id_var = tk.StringVar(value="")
        self._site_physics_batch_id_var = tk.StringVar(value="")
        self._harvest_uniprot_var = tk.BooleanVar(value=True)
        self._harvest_alphafold_var = tk.BooleanVar(value=False)
        self._harvest_reactome_var = tk.BooleanVar(value=False)
        self._harvest_interpro_var = tk.BooleanVar(value=False)
        self._harvest_pfam_var = tk.BooleanVar(value=False)
        self._harvest_cath_var = tk.BooleanVar(value=False)
        self._harvest_scop_var = tk.BooleanVar(value=False)
        self._harvest_max_proteins_var = tk.StringVar(value="")
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
        self._model_dataset_source_var = tk.StringVar(value="auto")
        self._model_modality_var = tk.StringVar(value="auto")
        self._model_task_var = tk.StringVar(value="auto")
        self._model_family_var = tk.StringVar(value="auto")
        self._model_compute_budget_var = tk.StringVar(value="balanced")
        self._model_interpretability_var = tk.StringVar(value="balanced")
        self._model_runtime_target_var = tk.StringVar(value="local_cpu")
        self._model_profile_summary_var = tk.StringVar(value="No model profile generated yet.")
        self._model_profile_detail_var = tk.StringVar(value="Select a dataset view and refresh recommendations.")
        self._model_warnings_var = tk.StringVar(value="No compatibility warnings.")
        self._model_next_action_var = tk.StringVar(value="Refresh Model Studio to profile the current workspace.")
        self._model_profile_kpi_vars: dict[str, tk.StringVar] = {
            "examples": tk.StringVar(value="0"),
            "train": tk.StringVar(value="0"),
            "val": tk.StringVar(value="0"),
            "test": tk.StringVar(value="0"),
            "modalities": tk.StringVar(value="--"),
            "tasks": tk.StringVar(value="--"),
        }
        self._model_config_preview_var = tk.StringVar(value="No starter config generated yet.")
        self._model_export_status_var = tk.StringVar(value="No starter config exported yet.")
        self._model_runtime_summary_var = tk.StringVar(value="Runtime capabilities not checked yet.")
        self._model_import_path_var = tk.StringVar(value="No imported run yet.")
        self._model_selected_run_var = tk.StringVar(value="")
        self._model_run_comparison_var = tk.StringVar(value="No saved-run comparison yet.")
        self._model_run_detail_var = tk.StringVar(value="No experiment-inspection summary yet.")
        self._model_inference_pdb_var = tk.StringVar(value="")
        self._model_inference_result_var = tk.StringVar(value="No saved-model inference run yet.")
        self._model_selected_run_preview_var = tk.StringVar(value="No selected run preview yet.")
        self._model_chart_preview_status_var = tk.StringVar(value="Chart previews will appear here when a run is selected.")
        self._model_architecture_title_var = tk.StringVar(value="Auto-Selected Model Path")
        self._model_architecture_subtitle_var = tk.StringVar(value="Model Studio will infer a suitable path from modality, task, and runtime choices.")
        self._model_architecture_footer_var = tk.StringVar(value="Use this when you want the app to explain the tradeoffs before committing to a family.")
        self._model_output_highlights_vars: dict[str, tk.StringVar] = {
            "headline": tk.StringVar(value="No simulated run yet."),
            "metric": tk.StringVar(value="--"),
            "artifacts": tk.StringVar(value="--"),
            "status": tk.StringVar(value="Run a demo model to populate charts and predictions."),
        }
        self._model_run_kpi_vars: dict[str, tk.StringVar] = {
            "runs": tk.StringVar(value="0"),
            "curves": tk.StringVar(value="0"),
            "plots": tk.StringVar(value="0"),
            "native": tk.StringVar(value="0"),
            "best": tk.StringVar(value="--"),
        }
        self._model_artifact_vars: dict[str, tk.StringVar] = {
            "run_dir": tk.StringVar(value=""),
            "training_curve": tk.StringVar(value=""),
            "test_performance": tk.StringVar(value=""),
            "metrics": tk.StringVar(value=""),
            "test_predictions": tk.StringVar(value=""),
        }
        self._model_recent_run_vars: list[dict[str, tk.StringVar]] = [
            {
                "title": tk.StringVar(value=f"Recent run {idx}"),
                "detail": tk.StringVar(value="--"),
            }
            for idx in range(1, 4)
        ]
        self._model_run_option_paths: dict[str, str] = {}
        self._model_chart_preview_images: dict[str, object | None] = {"training": None, "test": None}
        self._model_recommendation_vars: list[dict[str, tk.StringVar]] = [
            {
                "title": tk.StringVar(value=f"Recommendation {idx}"),
                "summary": tk.StringVar(value="--"),
                "why": tk.StringVar(value="--"),
                "strengths": tk.StringVar(value="--"),
                "drawbacks": tk.StringVar(value="--"),
                "recipe": tk.StringVar(value="--"),
            }
            for idx in range(1, 4)
        ]

        # --- Pipeline status vars ---
        self._status_vars: dict[str, tk.StringVar] = {
            key: tk.StringVar(value="idle") for key in _ALL_STAGE_KEYS
        }
        self._status_labels: dict[str, tk.Label] = {}
        self._action_buttons: list[tk.Widget] = []
        self._action_targets: dict[str, dict[str, Any]] = {}
        self._demo_highlighted_target_ids: tuple[str, ...] = tuple()
        self._run_state_var = tk.StringVar(value="Idle")
        self._run_current_stage_var = tk.StringVar(value="No active stage")
        self._run_progress_var = tk.StringVar(value="0 / 0 stages complete")
        self._run_next_stage_var = tk.StringVar(value="Nothing queued")
        self._run_last_message_var = tk.StringVar(value="Choose a stage or workflow to begin.")
        self._run_elapsed_var = tk.StringVar(value="0s")
        self._run_plan: list[str] = []
        self._run_completed_count = 0
        self._run_started_at: float | None = None
        self._run_in_progress = False
        self._run_active_label = ""
        self._run_current_stage_key: str | None = None
        self._demo_mode_var = tk.BooleanVar(value=False)
        self._demo_completed_actions: set[str] = set()
        self._demo_tutorial_vars: dict[str, tk.StringVar] = {
            "step": tk.StringVar(value="Turn on Demo Mode to start the guided walkthrough."),
            "detail": tk.StringVar(value="The tutorial will explain what each simulated stage is accomplishing and why it matters."),
            "innovation": tk.StringVar(value="Guided Demo Mode will highlight controls and produce plausible branch-specific results."),
            "instruction": tk.StringVar(value="Enable Demo Mode, then follow the highlighted action."),
            "scroll_hint": tk.StringVar(value=""),
            "progress": tk.StringVar(value="0 of 7 demo steps completed"),
        }
        self._compact_overview_var = tk.BooleanVar(value=True)
        self._overview_sections: dict[str, tk.Widget] = {}
        self._overview_deferred_built = False
        self._overview_deferred_host: ttk.Frame | None = None
        self._left_notebook: ttk.Notebook | None = None
        self._left_panel_host: tk.Frame | None = None
        self._pipeline_panel_host: tk.Frame | None = None
        self._left_tab_frames: dict[str, ttk.Frame] = {}
        self._left_tab_builders: dict[str, Callable[[ttk.Frame], None]] = {}
        self._left_tabs_built: set[str] = set()
        self._model_studio_notebook: ttk.Notebook | None = None
        self._model_studio_pages: dict[str, ttk.Frame] = {}

        # --- Data overview labels ---
        self._overview_vars: dict[str, tk.StringVar] = {}
        self._review_export_vars: dict[str, tk.StringVar] = {}
        self._review_health_vars: dict[str, tk.StringVar] = {
            "readiness": tk.StringVar(value="--"),
            "coverage": tk.StringVar(value="--"),
            "quality": tk.StringVar(value="--"),
            "next_action": tk.StringVar(value="--"),
        }
        self._presenter_banner_vars: dict[str, tk.StringVar] = {
            "headline": tk.StringVar(value="--"),
            "subhead": tk.StringVar(value="--"),
            "state": tk.StringVar(value="--"),
            "next_step": tk.StringVar(value="--"),
        }
        self._completion_summary_vars: dict[str, tk.StringVar] = {
            "status": tk.StringVar(value="--"),
            "headline": tk.StringVar(value="--"),
            "detail": tk.StringVar(value="--"),
            "next_action": tk.StringVar(value="--"),
        }
        self._completion_row_vars: list[dict[str, tk.StringVar]] = [
            {
                "area": tk.StringVar(value="--"),
                "current": tk.StringVar(value="--"),
                "target": tk.StringVar(value="--"),
                "gap": tk.StringVar(value="--"),
                "status": tk.StringVar(value="--"),
            }
            for _ in range(8)
        ]
        self._completion_status_labels: list[tk.Label] = []
        self._artifact_freshness_vars: dict[str, tk.StringVar] = {
            "release_check": tk.StringVar(value="--"),
            "demo_snapshot": tk.StringVar(value="--"),
            "prediction_manifest": tk.StringVar(value="--"),
            "risk_summary": tk.StringVar(value="--"),
            "model_comparison": tk.StringVar(value="--"),
            "training_quality": tk.StringVar(value="--"),
            "release_manifest": tk.StringVar(value="--"),
        }
        self._last_run_vars: dict[str, tk.StringVar] = {
            "status": tk.StringVar(value="--"),
            "summary": tk.StringVar(value="--"),
            "last_stage": tk.StringVar(value="--"),
            "last_result": tk.StringVar(value="--"),
            "next_action": tk.StringVar(value="--"),
        }
        self._training_set_vars: dict[str, tk.StringVar] = {
            "status": tk.StringVar(value="--"),
            "coverage": tk.StringVar(value="--"),
            "quality": tk.StringVar(value="--"),
            "next_action": tk.StringVar(value="--"),
        }
        self._training_kpi_vars: dict[str, tk.StringVar] = {
            "selected": tk.StringVar(value="--"),
            "clusters": tk.StringVar(value="--"),
            "quality": tk.StringVar(value="--"),
            "dominance": tk.StringVar(value="--"),
            "excluded": tk.StringVar(value="--"),
        }
        self._training_workflow_vars: dict[str, tk.StringVar] = {
            "model_ready": tk.StringVar(value="--"),
            "custom_set": tk.StringVar(value="--"),
            "scorecard": tk.StringVar(value="--"),
            "benchmark": tk.StringVar(value="--"),
            "release": tk.StringVar(value="--"),
        }
        self._training_quality_vars: dict[str, tk.StringVar] = {
            "status": tk.StringVar(value="--"),
            "coverage": tk.StringVar(value="--"),
            "quality": tk.StringVar(value="--"),
            "next_action": tk.StringVar(value="--"),
        }
        self._training_quality_kpi_vars: dict[str, tk.StringVar] = {
            "examples": tk.StringVar(value="--"),
            "supervised": tk.StringVar(value="--"),
            "targets": tk.StringVar(value="--"),
            "ligands": tk.StringVar(value="--"),
            "conflicts": tk.StringVar(value="--"),
        }
        self._model_comparison_vars: dict[str, tk.StringVar] = {
            "status": tk.StringVar(value="--"),
            "summary": tk.StringVar(value="--"),
            "next_action": tk.StringVar(value="--"),
        }
        self._model_comparison_kpi_vars: dict[str, tk.StringVar] = {
            "baseline": tk.StringVar(value="--"),
            "tabular": tk.StringVar(value="--"),
            "val_winner": tk.StringVar(value="--"),
            "test_winner": tk.StringVar(value="--"),
            "val_gap": tk.StringVar(value="--"),
        }
        self._split_diagnostics_vars: dict[str, tk.StringVar] = {
            "status": tk.StringVar(value="--"),
            "summary": tk.StringVar(value="--"),
            "next_action": tk.StringVar(value="--"),
        }
        self._split_diagnostics_kpi_vars: dict[str, tk.StringVar] = {
            "strategy": tk.StringVar(value="--"),
            "held_out": tk.StringVar(value="--"),
            "hard_overlap": tk.StringVar(value="--"),
            "family_overlap": tk.StringVar(value="--"),
            "source_overlap": tk.StringVar(value="--"),
            "fold_overlap": tk.StringVar(value="--"),
            "dominance": tk.StringVar(value="--"),
        }
        self._search_preview_vars: dict[str, tk.StringVar] = {
            "status": tk.StringVar(value="--"),
            "summary": tk.StringVar(value="--"),
            "next_action": tk.StringVar(value="--"),
        }
        self._search_preview_kpi_vars: dict[str, tk.StringVar] = {
            "total": tk.StringVar(value="--"),
            "selected": tk.StringVar(value="--"),
            "sample": tk.StringVar(value="--"),
            "mode": tk.StringVar(value="--"),
        }
        self._source_configuration_vars: dict[str, tk.StringVar] = {
            "status": tk.StringVar(value="--"),
            "summary": tk.StringVar(value="--"),
            "next_action": tk.StringVar(value="--"),
        }
        self._source_configuration_kpi_vars: dict[str, tk.StringVar] = {
            "enabled": tk.StringVar(value="--"),
            "implemented": tk.StringVar(value="--"),
            "planned": tk.StringVar(value="--"),
            "misconfigured": tk.StringVar(value="--"),
        }
        self._source_run_vars: dict[str, tk.StringVar] = {
            "status": tk.StringVar(value="--"),
            "summary": tk.StringVar(value="--"),
            "next_action": tk.StringVar(value="--"),
        }
        self._source_run_kpi_vars: dict[str, tk.StringVar] = {
            "sources": tk.StringVar(value="--"),
            "attempts": tk.StringVar(value="--"),
            "records": tk.StringVar(value="--"),
            "mode": tk.StringVar(value="--"),
        }
        self._data_integrity_vars: dict[str, tk.StringVar] = {
            "status": tk.StringVar(value="--"),
            "summary": tk.StringVar(value="--"),
            "next_action": tk.StringVar(value="--"),
            "detail": tk.StringVar(value="--"),
        }
        self._data_integrity_kpi_vars: dict[str, tk.StringVar] = {
            "valid": tk.StringVar(value="--"),
            "issues": tk.StringVar(value="--"),
            "empty": tk.StringVar(value="--"),
            "corrupt": tk.StringVar(value="--"),
            "invalid": tk.StringVar(value="--"),
            "scan": tk.StringVar(value="--"),
        }
        self._active_operations_vars: dict[str, tk.StringVar] = {
            "status": tk.StringVar(value="--"),
            "summary": tk.StringVar(value="--"),
            "active_detail": tk.StringVar(value="--"),
            "failed_detail": tk.StringVar(value="--"),
            "latest_detail": tk.StringVar(value="--"),
            "next_action": tk.StringVar(value="--"),
        }
        self._active_operations_kpi_vars: dict[str, tk.StringVar] = {
            "active": tk.StringVar(value="--"),
            "running": tk.StringVar(value="--"),
            "failed": tk.StringVar(value="--"),
            "stale": tk.StringVar(value="--"),
            "latest": tk.StringVar(value="--"),
        }
        self._identity_crosswalk_vars: dict[str, tk.StringVar] = {
            "status": tk.StringVar(value="--"),
            "summary": tk.StringVar(value="--"),
            "next_action": tk.StringVar(value="--"),
        }
        self._identity_crosswalk_kpi_vars: dict[str, tk.StringVar] = {
            "proteins": tk.StringVar(value="--"),
            "ligands": tk.StringVar(value="--"),
            "pairs": tk.StringVar(value="--"),
            "fallbacks": tk.StringVar(value="--"),
        }
        self._release_readiness_vars: dict[str, tk.StringVar] = {
            "status": tk.StringVar(value="--"),
            "summary": tk.StringVar(value="--"),
            "next_action": tk.StringVar(value="--"),
        }
        self._release_readiness_kpi_vars: dict[str, tk.StringVar] = {
            "entries": tk.StringVar(value="--"),
            "pairs": tk.StringVar(value="--"),
            "model_ready": tk.StringVar(value="--"),
            "held_out": tk.StringVar(value="--"),
            "blockers": tk.StringVar(value="--"),
        }
        self._risk_vars: dict[str, tk.StringVar] = {
            "status": tk.StringVar(value="--"),
            "summary": tk.StringVar(value="--"),
            "next_action": tk.StringVar(value="--"),
        }
        self._risk_kpi_vars: dict[str, tk.StringVar] = {
            "severity": tk.StringVar(value="--"),
            "score": tk.StringVar(value="--"),
            "matches": tk.StringVar(value="--"),
            "pathways": tk.StringVar(value="--"),
        }
        self._prediction_status_vars: dict[str, tk.StringVar] = {
            "status": tk.StringVar(value="--"),
            "method": tk.StringVar(value="--"),
            "preference": tk.StringVar(value="--"),
            "summary": tk.StringVar(value="--"),
        }
        self._prediction_status_kpi_vars: dict[str, tk.StringVar] = {
            "targets": tk.StringVar(value="--"),
            "top_target": tk.StringVar(value="--"),
            "confidence": tk.StringVar(value="--"),
            "query_features": tk.StringVar(value="--"),
        }
        self._workflow_guidance_vars: dict[str, tk.StringVar] = {
            "phase": tk.StringVar(value="--"),
            "summary": tk.StringVar(value="--"),
            "step_1": tk.StringVar(value="--"),
            "step_2": tk.StringVar(value="--"),
            "step_3": tk.StringVar(value="--"),
        }
        self._curation_review_vars: dict[str, tk.StringVar] = {
            "exclusions": tk.StringVar(value="--"),
            "conflicts": tk.StringVar(value="--"),
            "issues": tk.StringVar(value="--"),
            "next_action": tk.StringVar(value="--"),
        }
        self._demo_readiness_vars: dict[str, tk.StringVar] = {
            "readiness": tk.StringVar(value="--"),
            "summary": tk.StringVar(value="--"),
            "customer_message": tk.StringVar(value="--"),
            "walkthrough": tk.StringVar(value="--"),
            "blockers": tk.StringVar(value="--"),
            "warnings": tk.StringVar(value="--"),
        }

        # Serialise "Run All"
        self._running = threading.Lock()
        self._overview_refresh_generation = 0
        self._closing = False
        self._active_processes: set[subprocess.Popen[str]] = set()
        self._pipeline_scroll_canvas: tk.Canvas | None = None
        self._pipeline_scroll_frame: ttk.Frame | None = None
        self._last_model_profile: DatasetProfile | None = None
        self._last_model_recommendations: list[ModelRecommendation] = []

        for watched_var in (
            self._custom_set_mode_var,
            self._custom_set_target_size_var,
            self._model_family_var,
            self._model_modality_var,
            self._model_runtime_target_var,
            self._model_task_var,
        ):
            watched_var.trace_add("write", lambda *_args: (self._update_demo_tutorial(), self._draw_model_architecture_preview()))

        self._build_ui()
        self._update_demo_tutorial()
        self._load_sources_into_ui()
        self._load_criteria_into_ui()
        # Let the window paint before the first overview snapshot runs.
        self._root.after(10, lambda: self._refresh_overview(prefer_cached_status=True))

    def _storage_layout(self):
        return build_storage_layout(self._storage_root_var.get().strip() or Path.cwd())

    def _register_action_target(
        self,
        action_id: str,
        widget: tk.Widget,
        *,
        tab: str | None = None,
        subtab: str | None = None,
        scroll_hint: str = "",
    ) -> tk.Widget:
        style_name = ""
        try:
            style_name = str(widget.cget("style") or "")
        except Exception:
            style_name = ""
        self._action_targets[action_id] = {
            "widget": widget,
            "style": style_name,
            "tab": tab,
            "subtab": subtab,
            "scroll_hint": scroll_hint,
        }
        return widget

    def _tutorial_selection_context(self) -> dict[str, str]:
        return {
            "custom_set_mode": self._custom_set_mode_var.get().strip(),
            "custom_set_target_size": self._custom_set_target_size_var.get().strip(),
            "model_family": self._model_family_var.get().strip(),
            "model_modality": self._model_modality_var.get().strip(),
            "model_runtime_target": self._model_runtime_target_var.get().strip(),
        }

    def _record_demo_action(self, action_id: str) -> None:
        if not bool(self._demo_mode_var.get()):
            return
        self._demo_completed_actions.add(action_id)
        if action_id.startswith("model.run_local"):
            self._demo_completed_actions.add("model.run_local")
        self._update_demo_tutorial()

    def _clear_demo_highlights(self) -> None:
        for action_id in self._demo_highlighted_target_ids:
            target = self._action_targets.get(action_id)
            if not target:
                continue
            widget = target.get("widget")
            original_style = str(target.get("style") or "")
            try:
                if original_style:
                    widget.configure(style=original_style)
                else:
                    widget.configure(style="TButton")
            except Exception:
                continue
        self._demo_highlighted_target_ids = tuple()

    def _highlight_demo_targets(self, target_ids: tuple[str, ...]) -> None:
        self._clear_demo_highlights()
        highlighted: list[str] = []
        for action_id in target_ids:
            target = self._action_targets.get(action_id)
            if not target:
                continue
            widget = target.get("widget")
            style_name = str(target.get("style") or "")
            guide_style = "DemoGuide.Accent.TButton" if style_name == "Accent.TButton" else "DemoGuide.TButton"
            try:
                widget.configure(style=guide_style)
                highlighted.append(action_id)
            except Exception:
                continue
        self._demo_highlighted_target_ids = tuple(highlighted)

    def _focus_demo_tutorial_target(self) -> None:
        if not bool(self._demo_mode_var.get()):
            return
        target_ids = self._demo_highlighted_target_ids
        if not target_ids:
            return
        target = self._action_targets.get(target_ids[0], {})
        widget = target.get("widget")
        tab = str(target.get("tab") or "")
        subtab = str(target.get("subtab") or "")
        if tab and tab in self._left_tab_frames and self._left_notebook is not None:
            self._left_notebook.select(self._left_tab_frames[tab])
        if subtab and self._model_studio_notebook is not None and subtab in self._model_studio_pages:
            self._model_studio_notebook.select(self._model_studio_pages[subtab])
        try:
            widget.focus_set()
        except Exception:
            pass
        if tab == "model_studio" and self._left_notebook is not None:
            self._left_notebook.select(self._left_tab_frames["model_studio"])
        if subtab and self._model_studio_notebook is not None and subtab in self._model_studio_pages:
            self._model_studio_notebook.select(self._model_studio_pages[subtab])

    def _update_demo_tutorial(self) -> None:
        if not bool(self._demo_mode_var.get()):
            self._clear_demo_highlights()
            self._demo_tutorial_vars["step"].set("Turn on Demo Mode to start the guided walkthrough.")
            self._demo_tutorial_vars["detail"].set("The tutorial will explain what each simulated stage is accomplishing and why it matters.")
            self._demo_tutorial_vars["innovation"].set("Guided Demo Mode will highlight controls and produce plausible branch-specific results.")
            self._demo_tutorial_vars["instruction"].set("Enable Demo Mode, then follow the highlighted action.")
            self._demo_tutorial_vars["scroll_hint"].set("")
            self._demo_tutorial_vars["progress"].set("0 of 7 demo steps completed")
            return
        step: DemoTutorialStep = next_demo_tutorial_step(self._tutorial_selection_context(), self._demo_completed_actions)
        self._demo_tutorial_vars["step"].set(step.title)
        self._demo_tutorial_vars["detail"].set(step.detail)
        self._demo_tutorial_vars["innovation"].set(step.innovation)
        self._demo_tutorial_vars["instruction"].set(step.instruction)
        self._demo_tutorial_vars["scroll_hint"].set(step.scroll_hint)
        completed = min(len(self._demo_completed_actions & {
            "search.preview_rcsb",
            "pipeline.run_full",
            "training.build_custom_set",
            "training.run_workflow",
            "model.refresh",
            "model.run_local",
            "model.compare_runs",
            "model.inference",
        }), 7)
        self._demo_tutorial_vars["progress"].set(f"{completed} of 7 demo steps completed")
        self._highlight_demo_targets(step.target_ids)

    def _draw_model_architecture_preview(self) -> None:
        if not hasattr(self, "_model_architecture_canvas"):
            return
        spec = architecture_spec_for_selection(
            self._model_family_var.get().strip() or "auto",
            self._model_modality_var.get().strip() or "auto",
            self._model_task_var.get().strip() or "auto",
        )
        self._model_architecture_title_var.set(spec.title)
        self._model_architecture_subtitle_var.set(spec.subtitle)
        self._model_architecture_footer_var.set(spec.footer)
        canvas: tk.Canvas = self._model_architecture_canvas
        canvas.delete("all")
        width = int(canvas.cget("width"))
        height = int(canvas.cget("height"))
        bg = "#fffdf7"
        canvas.create_rectangle(0, 0, width, height, fill=bg, outline="#f1f5f9")
        boxes = [
            (24, 48, 170, 118, spec.left_label, "#dbeafe", "#1d4ed8"),
            (220, 48, 366, 118, spec.center_label, "#fef3c7", "#b45309"),
            (416, 48, 562, 118, spec.right_label, "#dcfce7", "#15803d"),
        ]
        for x1, y1, x2, y2, label, fill, outline in boxes:
            canvas.create_rectangle(x1, y1, x2, y2, fill=fill, outline=outline, width=2)
            canvas.create_text((x1 + x2) / 2, (y1 + y2) / 2, text=label, width=(x2 - x1 - 18), font=("Segoe UI", 9, "bold"))
        canvas.create_line(170, 83, 220, 83, arrow=tk.LAST, fill="#64748b", width=2)
        canvas.create_line(366, 83, 416, 83, arrow=tk.LAST, fill="#64748b", width=2)
        if spec.family == "hybrid_fusion":
            canvas.create_oval(110, 18, 146, 42, fill="#c4b5fd", outline="#7c3aed", width=2)
            canvas.create_text(128, 30, text="Attr", font=("Segoe UI", 8, "bold"))
            canvas.create_line(128, 42, 260, 48, arrow=tk.LAST, fill="#7c3aed", width=2, smooth=True)
        elif spec.family == "gnn":
            canvas.create_oval(62, 22, 82, 42, fill="#bfdbfe", outline="#2563eb")
            canvas.create_oval(102, 22, 122, 42, fill="#bfdbfe", outline="#2563eb")
            canvas.create_oval(142, 22, 162, 42, fill="#bfdbfe", outline="#2563eb")
            canvas.create_line(82, 32, 102, 32, fill="#2563eb", width=2)
            canvas.create_line(122, 32, 142, 32, fill="#2563eb", width=2)
        elif spec.family in {"xgboost", "random_forest"}:
            canvas.create_line(260, 22, 242, 48, fill="#92400e", width=2)
            canvas.create_line(260, 22, 278, 48, fill="#92400e", width=2)
            canvas.create_line(260, 22, 260, 48, fill="#92400e", width=2)
        elif spec.family in {"clustering", "autoencoder"}:
            canvas.create_oval(248, 18, 272, 42, fill="#fde68a", outline="#d97706")
            canvas.create_oval(292, 18, 316, 42, fill="#fde68a", outline="#d97706")
            canvas.create_oval(336, 18, 360, 42, fill="#fde68a", outline="#d97706")

    def _update_model_output_highlights(self, inspection: Any) -> None:
        metric_name = str(getattr(inspection, "primary_metric_name", None) or "metric").upper()
        metric_value = getattr(inspection, "primary_metric_value", None)
        artifact_count = inspection.artifacts.get("artifact_count", "0") if isinstance(getattr(inspection, "artifacts", None), dict) else "0"
        prediction_count = inspection.artifacts.get("test_prediction_count", "0") if isinstance(getattr(inspection, "artifacts", None), dict) else "0"
        self._model_output_highlights_vars["headline"].set(
            f"Latest highlighted run: {inspection.run_name} ({inspection.family})"
        )
        self._model_output_highlights_vars["metric"].set(
            f"{metric_name} = {metric_value if metric_value is not None else '--'}"
        )
        self._model_output_highlights_vars["artifacts"].set(
            f"{artifact_count} artifacts | {prediction_count} saved predictions"
        )
        chart_text = "Charts are visible below." if getattr(inspection, "chart_ready", False) or getattr(inspection, "test_plot_ready", False) else "Charts are not available for this run yet."
        self._model_output_highlights_vars["status"].set(chart_text)

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
        style.configure("Section.TLabelframe", background=_CARD_BG, relief="solid", borderwidth=1, bordercolor=_CARD_BORDER)
        style.configure(
            "Section.TLabelframe.Label",
            background=_CARD_BG,
            foreground=_SECTION_FG,
            font=("Segoe UI Semibold", 10),
        )
        style.configure("Card.TFrame", background=_CARD_BG, relief="solid", borderwidth=1)
        style.configure("AltCard.TFrame", background=_CARD_ALT_BG, relief="solid", borderwidth=1)
        style.configure("TNotebook", background=_APP_BG, borderwidth=0, tabmargins=(0, 0, 0, 0))
        style.configure(
            "TNotebook.Tab",
            padding=(14, 10),
            font=("Segoe UI Semibold", 9),
            background="#dfe9f3",
            foreground="#475569",
            borderwidth=0,
        )
        style.map(
            "TNotebook.Tab",
            background=[("selected", _CARD_BG), ("active", "#e8f0f7")],
            foreground=[("selected", "#0f172a"), ("active", "#0f172a")],
        )
        style.configure(
            "Accent.TButton",
            font=("Segoe UI Semibold", 9),
            background=_ACCENT_BG,
            foreground=_ACCENT_FG,
            borderwidth=0,
            focusthickness=0,
            padding=(12, 9),
        )
        style.map(
            "Accent.TButton",
            background=[("active", _ACCENT_BG_ACTIVE), ("pressed", _ACCENT_BG_ACTIVE)],
            foreground=[("active", _HEADER_FG), ("pressed", _HEADER_FG)],
        )
        style.configure(
            "TButton",
            padding=(10, 8),
            background=_CARD_BG,
            foreground="#0f172a",
            borderwidth=1,
        )
        style.map(
            "TButton",
            background=[("active", "#f3f7fb"), ("pressed", "#e7eef6")],
            foreground=[("active", "#0f172a"), ("pressed", "#0f172a")],
        )
        style.configure(
            "DemoGuide.TButton",
            padding=(10, 8),
            background="#fde68a",
            foreground="#7c2d12",
            bordercolor="#f59e0b",
            lightcolor="#f59e0b",
            darkcolor="#f59e0b",
            font=("Segoe UI Semibold", 9),
        )
        style.map(
            "DemoGuide.TButton",
            background=[("active", "#fcd34d"), ("pressed", "#f59e0b")],
            foreground=[("active", "#7c2d12"), ("pressed", "#7c2d12")],
        )
        style.configure(
            "DemoGuide.Accent.TButton",
            padding=(10, 8),
            background="#f59e0b",
            foreground="#ffffff",
            bordercolor="#d97706",
            lightcolor="#d97706",
            darkcolor="#d97706",
            font=("Segoe UI Semibold", 9),
        )
        style.map(
            "DemoGuide.Accent.TButton",
            background=[("active", "#d97706"), ("pressed", "#b45309")],
            foreground=[("active", "#ffffff"), ("pressed", "#ffffff")],
        )
        style.configure("TEntry", fieldbackground="#ffffff", bordercolor=_CARD_BORDER, lightcolor=_CARD_BORDER, darkcolor=_CARD_BORDER)
        style.configure("TCombobox", fieldbackground="#ffffff", bordercolor=_CARD_BORDER, lightcolor=_CARD_BORDER, darkcolor=_CARD_BORDER)

    # ------------------------------------------------------------------
    # UI construction
    # ------------------------------------------------------------------

    def _build_ui(self) -> None:
        self._root.columnconfigure(0, weight=1, minsize=420)
        self._root.columnconfigure(1, weight=1)
        self._root.rowconfigure(1, weight=1)
        self._root.rowconfigure(2, weight=2, minsize=220)

        self._build_header()
        self._build_left_panel()
        self._build_pipeline_panel()
        self._build_log_panel()

    def _apply_workspace_focus_layout(self, active_tab: str) -> None:
        left = self._left_panel_host
        right = self._pipeline_panel_host
        if left is None or right is None:
            return
        model_focus = active_tab == "model_studio"
        if model_focus:
            right.grid_remove()
            left.grid_configure(row=1, column=0, columnspan=2, sticky="nsew", padx=(10, 10), pady=(10, 4))
            self._root.columnconfigure(0, weight=1, minsize=720)
            self._root.columnconfigure(1, weight=0, minsize=0)
        else:
            right.grid()
            left.grid_configure(row=1, column=0, columnspan=1, sticky="nsew", padx=(10, 4), pady=(10, 4))
            right.grid_configure(row=1, column=1, sticky="nsew", padx=(4, 10), pady=(10, 4))
            self._root.columnconfigure(0, weight=1, minsize=420)
            self._root.columnconfigure(1, weight=1, minsize=320)

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

    def _make_scrollable_pane(
        self,
        outer: ttk.Frame | tk.Frame,
        *,
        canvas_bg: str = _APP_BG,
        frame_padding: int = 8,
    ) -> tuple[tk.Canvas, ttk.Scrollbar, ttk.Frame]:
        outer.columnconfigure(0, weight=1)
        outer.rowconfigure(0, weight=1)

        canvas = tk.Canvas(outer, highlightthickness=0, bg=canvas_bg)
        scrollbar = ttk.Scrollbar(outer, orient="vertical", command=canvas.yview)
        frame = ttk.Frame(canvas, padding=frame_padding)
        frame.bind(
            "<Configure>",
            lambda _: canvas.configure(scrollregion=canvas.bbox("all")),
        )
        window_id = canvas.create_window((0, 0), window=frame, anchor="nw")
        canvas.bind(
            "<Configure>",
            lambda e: canvas.itemconfigure(window_id, width=e.width),
        )
        canvas.configure(yscrollcommand=scrollbar.set)
        canvas.grid(row=0, column=0, sticky="nsew")
        scrollbar.grid(row=0, column=1, sticky="ns")
        return canvas, scrollbar, frame

    def _add_guidance_card(
        self,
        parent: ttk.Frame,
        *,
        row: int,
        title: str,
        summary: str,
        bullets: list[str] | tuple[str, ...] = (),
        columnspan: int = 1,
        pady: tuple[int, int] = (0, 10),
    ) -> int:
        card = ttk.LabelFrame(parent, text=title, padding=8, style="Section.TLabelframe")
        card.grid(row=row, column=0, columnspan=columnspan, sticky="ew", pady=pady)
        card.columnconfigure(0, weight=1)
        ttk.Label(
            card,
            text=summary,
            wraplength=440,
            justify="left",
            font=("Helvetica", 8),
            foreground="#4b5563",
        ).grid(row=0, column=0, sticky="w")
        if bullets:
            ttk.Label(
                card,
                text="\n".join(f"- {item}" for item in bullets),
                justify="left",
                font=("Helvetica", 8),
                foreground="#0f172a",
            ).grid(row=1, column=0, sticky="w", pady=(6, 0))
        return row + 1

    def _add_metric_cards(
        self,
        parent: ttk.Frame,
        *,
        row: int,
        specs: list[tuple[str, str]],
        value_vars: dict[str, tk.StringVar],
        columns: int = 4,
        value_wraplength: int = 160,
        pady: tuple[int, int] = (0, 0),
    ) -> int:
        host = ttk.Frame(parent)
        host.grid(row=row, column=0, columnspan=4, sticky="ew", pady=pady)
        for column in range(columns):
            host.columnconfigure(column, weight=1)
        for index, (key, label) in enumerate(specs):
            card = ttk.Frame(host, style="Card.TFrame", padding=8)
            card.grid(
                row=index // columns,
                column=index % columns,
                sticky="nsew",
                padx=(0 if index % columns == 0 else 4, 4 if index % columns != columns - 1 else 0),
                pady=(0, 6),
            )
            card.columnconfigure(0, weight=1)
            ttk.Label(
                card,
                text=label,
                style="Muted.TLabel",
                justify="left",
            ).grid(row=0, column=0, sticky="w")
            ttk.Label(
                card,
                textvariable=value_vars[key],
                background=_CARD_BG,
                foreground="#0f172a",
                font=("Segoe UI Semibold", 11),
                justify="left",
                wraplength=value_wraplength,
            ).grid(row=1, column=0, sticky="w", pady=(4, 0))
        return row + ((len(specs) + columns - 1) // columns)

    def _bind_text_mousewheel(self, widget: Any) -> None:
        def _scroll(event: Any) -> str:
            widget.yview_scroll(_mousewheel_units(event), "units")
            return "break"

        widget.bind("<MouseWheel>", _scroll, add="+")
        widget.bind("<Button-4>", _scroll, add="+")
        widget.bind("<Button-5>", _scroll, add="+")

    def _build_header(self) -> None:
        bar = tk.Frame(self._root, bg=_HEADER_BG, pady=10, padx=8)
        bar.grid(row=0, column=0, columnspan=2, sticky="ew")
        bar.columnconfigure(1, weight=1)

        left = tk.Frame(bar, bg=_HEADER_BG)
        left.pack(side="left", padx=12)

        badge = tk.Label(
            left,
            text="DATA PLATFORM",
            fg=_ACCENT_BG,
            bg=_HEADER_BG,
            font=("Segoe UI Semibold", 8),
            padx=8,
            pady=2,
        )
        badge.pack(side="top", anchor="w", pady=(0, 4))

        tk.Label(
            left,
            text="pbdata",
            fg=_HEADER_FG, bg=_HEADER_BG,
            font=("Segoe UI Semibold", 18),
        ).pack(side="left", anchor="s")

        tk.Label(
            left,
            text="  Protein Binding Dataset Platform",
            fg=_HEADER_SUB_FG, bg=_HEADER_BG,
            font=("Segoe UI", 11),
        ).pack(side="left", anchor="s", pady=(0, 1))

        right = tk.Frame(bar, bg=_HEADER_BG)
        right.pack(side="right", padx=12, fill="y")

        tk.Label(
            right,
            text="Structure-aware curation for model-ready biological training sets",
            fg=_HEADER_FG,
            bg=_HEADER_BG,
            font=("Segoe UI Semibold", 9),
            anchor="e",
            justify="right",
        ).pack(side="top", anchor="e")

        tk.Label(
            right,
            text="Extraction   •   Assays   •   Graphs   •   Physics   •   Release",
            fg=_HEADER_TAG_FG,
            bg=_HEADER_BG,
            font=("Segoe UI", 8),
            anchor="e",
            justify="right",
        ).pack(side="top", anchor="e", pady=(3, 0))

        tk.Checkbutton(
            right,
            text="Demo Mode",
            variable=self._demo_mode_var,
            command=self._on_demo_mode_toggle,
            bg=_HEADER_BG,
            fg=_HEADER_FG,
            activebackground=_HEADER_BG,
            activeforeground=_HEADER_FG,
            selectcolor=_HEADER_BG,
            highlightthickness=0,
            font=("Segoe UI Semibold", 8),
        ).pack(side="top", anchor="e", pady=(8, 0))
        tk.Checkbutton(
            right,
            text="Compact Overview",
            variable=self._compact_overview_var,
            command=self._apply_demo_mode,
            bg=_HEADER_BG,
            fg=_HEADER_FG,
            activebackground=_HEADER_BG,
            activeforeground=_HEADER_FG,
            selectcolor=_HEADER_BG,
            highlightthickness=0,
            font=("Segoe UI Semibold", 8),
        ).pack(side="top", anchor="e", pady=(4, 0))

    def _build_left_panel(self) -> None:
        left = tk.Frame(self._root, bg=_APP_BG)
        left.grid(row=1, column=0, sticky="nsew", padx=(10, 4), pady=(10, 4))
        self._left_panel_host = left
        left.columnconfigure(0, weight=1)
        left.rowconfigure(0, weight=1)

        notebook = ttk.Notebook(left)
        notebook.grid(row=0, column=0, sticky="nsew")
        self._left_notebook = notebook
        self._left_tab_builders = {
            "sources": self._build_sources_tab,
            "search": self._build_search_tab,
            "options": self._build_options_tab,
            "model_studio": self._build_model_studio_tab,
        }
        for key, label in [
            ("sources", " Sources "),
            ("search", " Search Criteria "),
            ("options", " Options "),
            ("model_studio", " Model Studio "),
        ]:
            frame = ttk.Frame(notebook, padding=8)
            self._left_tab_frames[key] = frame
            notebook.add(frame, text=label)

        self._build_sources_tab(self._left_tab_frames["sources"])
        self._left_tabs_built.add("sources")
        notebook.bind("<<NotebookTabChanged>>", self._on_left_tab_changed, add="+")
        self._apply_workspace_focus_layout("sources")

    def _on_left_tab_changed(self, _event: tk.Event) -> None:
        notebook = self._left_notebook
        if notebook is None:
            return
        current = notebook.select()
        if not current:
            return
        current_widget = str(notebook.nametowidget(current))
        active_key = "sources"
        for key, frame in self._left_tab_frames.items():
            if str(frame) == current_widget:
                active_key = key
            if str(frame) != current_widget or key in self._left_tabs_built:
                continue
            builder = self._left_tab_builders.get(key)
            if builder is None:
                break
            builder(frame)
            self._left_tabs_built.add(key)
            break
        self._apply_workspace_focus_layout(active_key)

    # --- Tab 1: Data Sources ---

    def _build_sources_tab(self, outer: ttk.Frame) -> None:
        canvas, _scrollbar, frame = self._make_scrollable_pane(outer)

        frame.columnconfigure(0, weight=1)
        frame.columnconfigure(1, weight=1)
        row = 0

        row = self._add_guidance_card(
            frame,
            row=row,
            title="Source Plan",
            summary="Choose which upstream datasets the pipeline should ingest or use for extraction-time enrichment.",
            bullets=[
                "RCSB is the primary structure source for most runs.",
                "BindingDB and ChEMBL enrich extraction; they are not bulk-mirrored by default.",
                "Local dataset paths let you reuse downloaded caches instead of re-fetching data.",
            ],
            columnspan=2,
        )

        sources_card = ttk.LabelFrame(frame, text="Enabled Sources", padding=8, style="Section.TLabelframe")
        sources_card.grid(row=row, column=0, columnspan=2, sticky="ew")
        sources_card.columnconfigure(0, weight=1)
        sources_card.columnconfigure(1, weight=1)
        src_row = 0

        for src in _SOURCES:
            descriptor = _SOURCE_DESCRIPTOR_BY_NAME.get(src)
            src_frame = ttk.Frame(sources_card)
            src_frame.grid(row=src_row, column=0, columnspan=2, sticky="ew", pady=2)
            src_frame.columnconfigure(1, weight=1)

            ttk.Checkbutton(
                src_frame,
                text=(descriptor.label if descriptor is not None else src.upper()),
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
            src_row += 1
        row += 1

        ttk.Separator(frame, orient="horizontal").grid(
            row=row, column=0, columnspan=2, sticky="ew", pady=8,
        )
        row += 1

        mirror_card = ttk.LabelFrame(frame, text="Structure Mirror", padding=8, style="Section.TLabelframe")
        mirror_card.grid(row=row, column=0, columnspan=2, sticky="ew")
        mirror_card.columnconfigure(1, weight=1)
        ttk.Label(
            mirror_card, text="Experimental structure mirror:",
            font=("Helvetica", 9, "bold"),
        ).grid(row=0, column=0, sticky="w", pady=(0, 4))
        mirror_box = ttk.Combobox(
            mirror_card,
            state="readonly",
            textvariable=self._structure_mirror_var,
            values=_STRUCTURE_MIRROR_OPTIONS,
            width=12,
        )
        mirror_box.grid(row=0, column=1, sticky="w", padx=(6, 0), pady=(0, 4))

        ttk.Label(
            mirror_card,
            text="Used for experimental mmCIF/PDB downloads during Extract and downstream physics features.",
            font=("Helvetica", 7),
            foreground="#888888",
        ).grid(row=1, column=0, columnspan=2, sticky="w", pady=(0, 2))
        row += 1

        row = self._add_guidance_card(
            frame,
            row=row,
            title="Cache And Reuse",
            summary="These optional paths let the app reuse local mirrors or cache directories when they already exist.",
            bullets=[
                "Use a shared BindingDB or PDBbind folder to avoid repeated downloads.",
                "Point SKEMPI to a specific CSV if you want to override the default workspace copy.",
            ],
            columnspan=2,
            pady=(8, 10),
        )

        path_card = ttk.LabelFrame(frame, text="Extract-Time Source Paths", padding=8, style="Section.TLabelframe")
        path_card.grid(row=row, column=0, columnspan=2, sticky="ew")
        path_card.columnconfigure(0, weight=1)
        row += 1

        path_labels = {
            "bindingdb": "BindingDB local cache directory (optional)",
            "pdbbind": "PDBbind local dataset directory",
            "biolip":  "BioLiP local dataset directory",
            "skempi":  "SKEMPI CSV file (optional override)",
        }
        path_row = 0
        for src, label in path_labels.items():
            path_frame = ttk.Frame(path_card)
            path_frame.grid(row=path_row, column=0, sticky="ew", pady=2)
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
            path_row += 1

        ttk.Separator(frame, orient="horizontal").grid(
            row=row, column=0, columnspan=2, sticky="ew", pady=8,
        )
        row += 1

        actions = ttk.Frame(frame)
        actions.grid(row=row, column=0, columnspan=2, sticky="ew")
        actions.columnconfigure(0, weight=1)
        ttk.Button(
            actions, text="Save Source Config",
            command=self._save_sources,
        ).grid(row=0, column=0, sticky="ew")
        self._bind_canvas_mousewheel(canvas, frame)

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
            self._refresh_overview_async()

    # --- Tab 2: Search Criteria ---

    def _build_search_tab(self, outer: ttk.Frame) -> None:
        canvas, _scrollbar, frame = self._make_scrollable_pane(outer)

        frame.columnconfigure(1, weight=1)
        row = 0

        row = self._add_guidance_card(
            frame,
            row=row,
            title="Search Strategy",
            summary="Use this tab to define the RCSB subset you want to ingest and the review filters you want to apply after extraction.",
            bullets=[
                "Direct PDB IDs override the live RCSB search query.",
                "Result limits can still aim for broad, representative coverage.",
                "Review filters act on local exports after extraction, not on the upstream download query.",
            ],
            columnspan=2,
        )

        # --- Direct PDB IDs ---
        direct_card = ttk.LabelFrame(frame, text="Query Scope", padding=8, style="Section.TLabelframe")
        direct_card.grid(row=row, column=0, columnspan=2, sticky="ew")
        direct_card.columnconfigure(1, weight=1)
        ttk.Label(
            direct_card, text="Direct PDB IDs:",
            font=("Helvetica", 9, "bold"),
        ).grid(row=0, column=0, columnspan=2, sticky="w")
        ttk.Label(
            direct_card,
            text="Comma-separated. If set, bypasses RCSB search.",
            font=("Helvetica", 7),
            foreground="#888888",
        ).grid(row=1, column=0, columnspan=2, sticky="w")
        ttk.Entry(direct_card, textvariable=self._pdb_ids_var).grid(
            row=2, column=0, columnspan=2, sticky="ew", pady=(2, 0),
        )

        ttk.Label(direct_card, text="Optional result limit:").grid(
            row=3, column=0, sticky="w", pady=(6, 0),
        )
        ttk.Entry(direct_card, textvariable=self._max_results_var, width=10).grid(
            row=3, column=1, sticky="w", padx=(6, 0), pady=(6, 0),
        )
        ttk.Checkbutton(
            direct_card,
            text="Use representative sampling when a result limit is set",
            variable=self._representative_sampling_var,
        ).grid(row=4, column=0, columnspan=2, sticky="w", pady=(2, 0))
        ttk.Label(
            direct_card,
            text="Best-effort diversity pass across task type, method, taxonomy, and resolution buckets before download.",
            font=("Helvetica", 7),
            foreground="#888888",
        ).grid(row=5, column=0, columnspan=2, sticky="w")
        row += 1

        ttk.Separator(frame, orient="horizontal").grid(
            row=row, column=0, columnspan=2, sticky="ew", pady=8,
        )
        row += 1

        # --- Text search ---
        text_card = ttk.LabelFrame(frame, text="Text And Taxonomy Filters", padding=8, style="Section.TLabelframe")
        text_card.grid(row=row, column=0, columnspan=2, sticky="ew")
        text_card.columnconfigure(1, weight=1)

        text_row = 0
        for label, var in [
            ("Keywords / full-text:", self._keyword_query_var),
            ("Organism name:", self._organism_name_var),
            ("NCBI taxonomy ID:", self._taxonomy_id_var),
        ]:
            ttk.Label(text_card, text=label).grid(
                row=text_row, column=0, sticky="w", pady=(4, 0),
            )
            ttk.Entry(text_card, textvariable=var).grid(
                row=text_row, column=1, sticky="ew", padx=(6, 0), pady=(4, 0),
            )
            text_row += 1
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

        actions = ttk.Frame(frame)
        actions.grid(row=row, column=0, columnspan=2, sticky="ew")
        actions.columnconfigure(0, weight=1)
        actions.columnconfigure(1, weight=1)
        ttk.Button(
            actions, text="Save Search Criteria",
            command=self._save_criteria,
        ).grid(row=0, column=0, sticky="ew", padx=(0, 4))

        preview_btn = ttk.Button(
            actions, text="Preview RCSB Search",
            command=self._preview_rcsb_search,
        )
        preview_btn.grid(row=0, column=1, sticky="ew", padx=(4, 0))
        self._register_action_target(
            "search.preview_rcsb",
            preview_btn,
            tab="search",
            scroll_hint="Scroll to the bottom of the Search Criteria tab.",
        )
        row += 1

        ttk.Separator(frame, orient="horizontal").grid(
            row=row, column=0, columnspan=2, sticky="ew", pady=8,
        )
        row += 1

        review_card = ttk.LabelFrame(frame, text="Local Review Filters", padding=8, style="Section.TLabelframe")
        review_card.grid(row=row, column=0, columnspan=2, sticky="ew")
        review_card.columnconfigure(1, weight=1)
        ttk.Label(
            review_card,
            text="Applies to the root review CSVs after extraction. Writes master_pdb_review_filtered.csv in the repo root.",
            font=("Helvetica", 7),
            foreground="#888888",
        ).grid(row=0, column=0, columnspan=2, sticky="w")

        review_row = 1
        for label, var in [
            ("PDB ID contains:", self._review_pdb_query_var),
            ("Pair key contains:", self._review_pair_query_var),
        ]:
            ttk.Label(review_card, text=label).grid(row=review_row, column=0, sticky="w", pady=(4, 0))
            ttk.Entry(review_card, textvariable=var).grid(
                row=review_row, column=1, sticky="ew", padx=(6, 0), pady=(4, 0),
            )
            review_row += 1

        ttk.Label(review_card, text="Issue type:").grid(row=review_row, column=0, sticky="w", pady=(4, 0))
        ttk.Combobox(
            review_card,
            textvariable=self._review_issue_type_var,
            values=_REVIEW_ISSUE_OPTIONS,
            width=28,
            state="readonly",
        ).grid(row=review_row, column=1, sticky="w", padx=(6, 0), pady=(4, 0))
        review_row += 1

        ttk.Label(review_card, text="Confidence filter:").grid(row=review_row, column=0, sticky="w", pady=(4, 0))
        ttk.Combobox(
            review_card,
            textvariable=self._review_confidence_var,
            values=_REVIEW_CONFIDENCE_OPTIONS,
            width=18,
            state="readonly",
        ).grid(row=review_row, column=1, sticky="w", padx=(6, 0), pady=(4, 0))
        review_row += 1

        for label, var in [
            ("Conflicted pairs only", self._review_conflict_only_var),
            ("Mutation-ambiguous only", self._review_mutation_ambiguous_only_var),
            ("Metal-containing entries only", self._review_metal_only_var),
            ("Cofactor-containing entries only", self._review_cofactor_only_var),
            ("Glycan-containing entries only", self._review_glycan_only_var),
        ]:
            ttk.Checkbutton(review_card, text=label, variable=var).grid(
                row=review_row, column=0, columnspan=2, sticky="w", pady=(2, 0),
            )
            review_row += 1

        ttk.Label(
            review_card,
            textvariable=self._review_filtered_count_var,
            font=("Helvetica", 8, "bold"),
        ).grid(row=review_row, column=0, columnspan=2, sticky="w", pady=(6, 0))
        review_row += 1

        review_btns = ttk.Frame(review_card)
        review_btns.grid(row=review_row, column=0, columnspan=2, sticky="ew", pady=(6, 0))
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
        self._bind_canvas_mousewheel(canvas, frame)

    # --- Tab 3: Pipeline Options ---

    def _build_options_tab(self, outer: ttk.Frame) -> None:
        canvas, _scrollbar, frame = self._make_scrollable_pane(outer)
        frame.columnconfigure(0, weight=1)

        row = 0

        row = self._add_guidance_card(
            frame,
            row=row,
            title="Pipeline Setup",
            summary="This tab controls where data is stored, how the pipeline executes, and how downstream graphs, splits, datasets, and releases are built.",
            bullets=[
                "Storage root controls where all managed data lives under data/.",
                "Execution mode changes which stage families Run Full Pipeline will launch.",
                "Workers and split settings affect both throughput and leakage resistance.",
            ],
        )

        storage_card = ttk.LabelFrame(frame, text="Storage Root", padding=8, style="Section.TLabelframe")
        storage_card.grid(row=row, column=0, sticky="ew")
        storage_card.columnconfigure(0, weight=1)
        root_frame = ttk.Frame(storage_card)
        root_frame.grid(row=0, column=0, sticky="ew")
        root_frame.columnconfigure(0, weight=1)
        ttk.Entry(root_frame, textvariable=self._storage_root_var).grid(
            row=0, column=0, sticky="ew", padx=(0, 4),
        )
        ttk.Button(
            root_frame, text="Browse...",
            command=self._browse_storage_root,
        ).grid(row=0, column=1)
        ttk.Label(
            storage_card,
            text=(
                "All generated files will be stored under <storage root>/data/\n"
                "for raw, processed, extracted, structures, graph, features, reports, and splits."
            ),
            font=("Helvetica", 7),
            foreground="#888888",
        ).grid(row=1, column=0, sticky="w", pady=(2, 0))
        row += 1

        ttk.Separator(frame, orient="horizontal").grid(
            row=row, column=0, sticky="ew", pady=10,
        )
        row += 1

        mode_card = ttk.LabelFrame(frame, text="Pipeline Mode", padding=8, style="Section.TLabelframe")
        mode_card.grid(row=row, column=0, sticky="ew")
        pipeline_mode_frame = ttk.Frame(mode_card)
        pipeline_mode_frame.grid(row=0, column=0, sticky="ew")
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
        ttk.Checkbutton(
            pipeline_mode_frame,
            text="Skip Experimental/Preview stages during Run Full Pipeline",
            variable=self._skip_experimental_stages_var,
        ).grid(row=4, column=0, columnspan=2, sticky="w", pady=(2, 0))

        ttk.Label(
            mode_card,
            text=(
                "legacy: current pipeline only\n"
                "site-centric: new artifacts/ pipeline only\n"
                "hybrid: run both, keeping shared extract/canonical inputs"
            ),
            font=("Helvetica", 7),
            foreground="#888888",
        ).grid(row=1, column=0, sticky="w", padx=(24, 0), pady=(2, 0))
        row += 1

        ttk.Separator(frame, orient="horizontal").grid(
            row=row, column=0, sticky="ew", pady=10,
        )
        row += 1

        # --- Extract options ---
        ttk.Label(
            frame, text="Extract Options",
            font=("Helvetica", 10, "bold"),
        ).grid(row=row, column=0, sticky="w", pady=(0, 6))
        row += 1

        ttk.Checkbutton(
            frame, text="Download mmCIF structure files",
            variable=self._download_structures_var,
        ).grid(row=row, column=0, sticky="w")
        row += 1

        ttk.Checkbutton(
            frame, text="Also download PDB format files",
            variable=self._download_pdb_var,
        ).grid(row=row, column=0, sticky="w", pady=(2, 0))
        row += 1

        ttk.Label(
            frame,
            text="mmCIF files are downloaded to <storage root>/data/structures/rcsb/",
            font=("Helvetica", 7),
            foreground="#888888",
        ).grid(row=row, column=0, sticky="w", padx=(24, 0), pady=(2, 0))
        row += 1

        ttk.Separator(frame, orient="horizontal").grid(
            row=row, column=0, sticky="ew", pady=10,
        )
        row += 1

        ttk.Label(
            frame, text="Workflow Engine",
            font=("Helvetica", 10, "bold"),
        ).grid(row=row, column=0, sticky="w", pady=(0, 6))
        row += 1

        workflow_frame = ttk.Frame(frame)
        workflow_frame.grid(row=row, column=0, sticky="ew")
        workflow_frame.columnconfigure(1, weight=1)
        ttk.Checkbutton(
            workflow_frame,
            text="Harvest UniProt annotations",
            variable=self._harvest_uniprot_var,
        ).grid(row=0, column=0, columnspan=2, sticky="w", pady=1)
        ttk.Checkbutton(
            workflow_frame,
            text="Harvest AlphaFold DB metadata",
            variable=self._harvest_alphafold_var,
        ).grid(row=1, column=0, columnspan=2, sticky="w", pady=1)
        ttk.Checkbutton(
            workflow_frame,
            text="Harvest Reactome pathway memberships",
            variable=self._harvest_reactome_var,
        ).grid(row=2, column=0, columnspan=2, sticky="w", pady=1)
        ttk.Checkbutton(
            workflow_frame,
            text="Harvest InterPro domain mappings",
            variable=self._harvest_interpro_var,
        ).grid(row=3, column=0, columnspan=2, sticky="w", pady=1)
        ttk.Checkbutton(
            workflow_frame,
            text="Harvest Pfam domain mappings",
            variable=self._harvest_pfam_var,
        ).grid(row=4, column=0, columnspan=2, sticky="w", pady=1)
        ttk.Checkbutton(
            workflow_frame,
            text="Harvest CATH fold mappings",
            variable=self._harvest_cath_var,
        ).grid(row=5, column=0, columnspan=2, sticky="w", pady=1)
        ttk.Checkbutton(
            workflow_frame,
            text="Harvest SCOP fold mappings",
            variable=self._harvest_scop_var,
        ).grid(row=6, column=0, columnspan=2, sticky="w", pady=1)
        ttk.Label(workflow_frame, text="Max proteins:").grid(row=7, column=0, sticky="w", pady=(4, 0))
        ttk.Entry(
            workflow_frame,
            textvariable=self._harvest_max_proteins_var,
            width=10,
        ).grid(row=7, column=1, sticky="w", padx=(6, 0), pady=(4, 0))
        row += 1

        ttk.Label(
            frame,
            text=(
                "Use Setup Workspace to create the instruction-pack workspace layout,\n"
                "then Harvest Metadata to build metadata/protein_metadata.csv for graph and dataset steps.\n"
                "Optional UniProt / AlphaFold / Reactome enrichment adds annotation columns for downstream grouping."
            ),
            font=("Helvetica", 7),
            foreground="#888888",
        ).grid(row=row, column=0, sticky="w", padx=(24, 0), pady=(2, 0))
        row += 1

        ttk.Separator(frame, orient="horizontal").grid(
            row=row, column=0, sticky="ew", pady=10,
        )
        row += 1

        ttk.Label(
            frame, text="Structural Graph Options",
            font=("Helvetica", 10, "bold"),
        ).grid(row=row, column=0, sticky="w", pady=(0, 6))
        row += 1

        graph_frame = ttk.Frame(frame)
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
            frame,
            text="Comma-separated export formats. Supported: pyg, dgl, networkx.",
            font=("Helvetica", 7),
            foreground="#888888",
        ).grid(row=row, column=0, sticky="w", padx=(24, 0), pady=(2, 0))
        row += 1

        ttk.Separator(frame, orient="horizontal").grid(
            row=row, column=0, sticky="ew", pady=10,
        )
        row += 1

        # --- Split options ---
        ttk.Label(
            frame, text="Split Options",
            font=("Helvetica", 10, "bold"),
        ).grid(row=row, column=0, sticky="w", pady=(0, 6))
        row += 1

        split_frame = ttk.Frame(frame)
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
                    values=["auto", "pair-aware", "legacy-sequence", "hash", "scaffold", "family", "mutation", "source", "time"],
                    width=16,
                    state="readonly",
                ).grid(row=i, column=1, sticky="w", padx=(6, 0), pady=2)
            else:
                ttk.Entry(split_frame, textvariable=var, width=10).grid(
                    row=i, column=1, sticky="w", padx=(6, 0), pady=2,
                )
        row += 1

        ttk.Checkbutton(
            frame, text="Hash-only split (no sequence clustering)",
            variable=self._hash_only_var,
        ).grid(row=row, column=0, sticky="w", pady=(4, 0))
        row += 1

        ttk.Label(
            frame,
            text="Default uses k-mer Jaccard clustering to prevent\n"
                 "sequence-identity leakage between train/val/test.",
            font=("Helvetica", 7),
            foreground="#888888",
        ).grid(row=row, column=0, sticky="w", padx=(24, 0), pady=(2, 0))
        row += 1

        ttk.Separator(frame, orient="horizontal").grid(
            row=row, column=0, sticky="ew", pady=10,
        )
        row += 1

        ttk.Label(
            frame, text="Dataset Engineering",
            font=("Helvetica", 10, "bold"),
        ).grid(row=row, column=0, sticky="w", pady=(0, 6))
        row += 1

        dataset_frame = ttk.Frame(frame)
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
            frame,
            text="Builds train.csv, test.csv, optional cv_folds/, and reproducibility configs from metadata/protein_metadata.csv.",
            font=("Helvetica", 7),
            foreground="#888888",
        ).grid(row=row, column=0, sticky="w", padx=(24, 0), pady=(2, 0))
        row += 1

        ttk.Separator(frame, orient="horizontal").grid(
            row=row, column=0, sticky="ew", pady=10,
        )
        row += 1

        ttk.Label(
            frame, text="Release Options",
            font=("Helvetica", 10, "bold"),
        ).grid(row=row, column=0, sticky="w", pady=(0, 6))
        row += 1

        release_frame = ttk.Frame(frame)
        release_frame.grid(row=row, column=0, sticky="ew")
        release_frame.columnconfigure(1, weight=1)
        ttk.Label(release_frame, text="Release tag:").grid(row=0, column=0, sticky="w")
        ttk.Entry(release_frame, textvariable=self._release_tag_var).grid(
            row=0, column=1, sticky="ew", padx=(6, 0),
        )
        row += 1

        ttk.Label(
            frame,
            text="Optional. If blank, build-release uses the current UTC timestamp.",
            font=("Helvetica", 7),
            foreground="#888888",
        ).grid(row=row, column=0, sticky="w", padx=(24, 0), pady=(2, 0))
        row += 1

        ttk.Separator(frame, orient="horizontal").grid(
            row=row, column=0, sticky="ew", pady=10,
        )
        row += 1

        ttk.Label(
            frame, text="Custom Training Set",
            font=("Helvetica", 10, "bold"),
        ).grid(row=row, column=0, sticky="w", pady=(0, 6))
        row += 1

        custom_frame = ttk.Frame(frame)
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
            frame,
            text="Builds a diversity-optimized subset from model-ready pairs, emphasizing broad coverage and low redundancy.",
            font=("Helvetica", 7),
            foreground="#888888",
        ).grid(row=row, column=0, sticky="w", padx=(24, 0), pady=(2, 0))
        self._bind_canvas_mousewheel(canvas, frame)

    # --- Pipeline panel (right side) ---

    def _model_studio_selection(self) -> ModelStudioSelection:
        return ModelStudioSelection(
            dataset_source=self._model_dataset_source_var.get().strip() or "auto",
            modality=self._model_modality_var.get().strip() or "auto",
            task=self._model_task_var.get().strip() or "auto",
            preferred_family=self._model_family_var.get().strip() or "auto",
            compute_budget=self._model_compute_budget_var.get().strip() or "balanced",
            interpretability_priority=self._model_interpretability_var.get().strip() or "balanced",
        )

    def _refresh_model_runtime(self) -> None:
        runtime = detect_runtime_capabilities()
        self._model_runtime_summary_var.set(runtime.summary)
        if self._model_runtime_target_var.get() not in runtime.supported_targets:
            self._model_runtime_target_var.set(runtime.supported_targets[0] if runtime.supported_targets else "local_cpu")

    def _refresh_model_studio(self, *, record_demo: bool = True) -> None:
        if record_demo:
            self._record_demo_action("model.refresh")
        self._refresh_model_runtime()
        profile = build_dataset_profile(
            self._storage_layout(),
            dataset_source=self._model_dataset_source_var.get().strip() or "auto",
        )
        selection = self._model_studio_selection()
        compatibility = validate_model_studio_selection(profile, selection)
        recommendations = recommend_model_architectures(profile, selection)
        self._last_model_profile = profile
        self._last_model_recommendations = recommendations

        self._model_profile_summary_var.set(profile.summary)
        self._model_profile_detail_var.set(
            " | ".join([
                f"dataset={profile.dataset_source}",
                f"examples={profile.example_count:,}",
                f"train/val/test={profile.train_count:,}/{profile.val_count:,}/{profile.test_count:,}",
                f"labels={', '.join(profile.label_fields) if profile.label_fields else 'none'}",
            ])
        )
        self._model_profile_kpi_vars["examples"].set(f"{profile.example_count:,}")
        self._model_profile_kpi_vars["train"].set(f"{profile.train_count:,}")
        self._model_profile_kpi_vars["val"].set(f"{profile.val_count:,}")
        self._model_profile_kpi_vars["test"].set(f"{profile.test_count:,}")
        self._model_profile_kpi_vars["modalities"].set(", ".join(profile.modalities_available) if profile.modalities_available else "--")
        self._model_profile_kpi_vars["tasks"].set(", ".join(profile.tasks_available) if profile.tasks_available else "--")
        self._model_next_action_var.set(profile.next_action)
        if compatibility:
            self._model_warnings_var.set(
                " ".join(f"[{msg.priority.upper()}] {msg.title}: {msg.body}" for msg in compatibility)
            )
        else:
            self._model_warnings_var.set("No compatibility warnings for the current setup.")

        for payload, vars_for_card in zip(recommendations, self._model_recommendation_vars):
            vars_for_card["title"].set(
                f"#{payload.rank} {payload.label} ({payload.family}, {payload.compute_cost} compute)"
            )
            vars_for_card["summary"].set(
                f"{payload.summary}\nFit score: {payload.fit_score:.2f} | "
                f"Interpretability: {payload.interpretability} | "
                f"Supervision: {payload.supervision}"
            )
            vars_for_card["why"].set(f"Why it fits\n{payload.why_it_fits}")
            vars_for_card["strengths"].set(self._format_model_bullets("Strengths", payload.strengths))
            vars_for_card["drawbacks"].set(self._format_model_bullets("Tradeoffs", payload.drawbacks))
            recipe_parts = list(payload.starter_recipe)
            if payload.warnings:
                recipe_parts.extend(f"Warning: {warning}" for warning in payload.warnings)
            vars_for_card["recipe"].set(self._format_model_bullets("Starter recipe", recipe_parts))
        for vars_for_card in self._model_recommendation_vars[len(recommendations):]:
            vars_for_card["title"].set("No recommendation available")
            vars_for_card["summary"].set("The current workspace does not have enough compatible artifacts for an additional recommendation.")
            vars_for_card["why"].set("Why it fits\n--")
            vars_for_card["strengths"].set("Strengths\n- --")
            vars_for_card["drawbacks"].set("Tradeoffs\n- --")
            vars_for_card["recipe"].set("Starter recipe\n- --")
        if recommendations:
            starter = build_starter_model_config(profile, recommendations[0], selection)
            self._model_config_preview_var.set(json.dumps(starter.config, indent=2))
        else:
            self._model_config_preview_var.set("No starter config available for the current workspace.")

    def _export_model_starter_config(self, recommendation_index: int) -> None:
        if self._last_model_profile is None or recommendation_index >= len(self._last_model_recommendations):
            self._model_export_status_var.set("Refresh Model Studio before exporting a starter config.")
            return
        starter = build_starter_model_config(
            self._last_model_profile,
            self._last_model_recommendations[recommendation_index],
            self._model_studio_selection(),
        )
        out_path = export_starter_model_config(self._storage_layout(), starter)
        self._model_config_preview_var.set(json.dumps(starter.config, indent=2))
        self._model_export_status_var.set(f"Starter config written to {out_path}")
        self._log_line(f"Model Studio starter config written to {out_path}")

    def _export_model_training_package(self, recommendation_index: int) -> None:
        if self._last_model_profile is None or recommendation_index >= len(self._last_model_recommendations):
            self._model_export_status_var.set("Refresh Model Studio before exporting a training package.")
            return
        starter = build_starter_model_config(
            self._last_model_profile,
            self._last_model_recommendations[recommendation_index],
            self._model_studio_selection(),
        )
        target = self._model_runtime_target_var.get().strip() or "local_cpu"
        package_name = f"{starter.model_id}_{target}"
        out_dir = export_training_package(
            self._storage_layout(),
            starter_config=starter.config,
            target=target,
            package_name=package_name,
        )
        self._model_config_preview_var.set(json.dumps(starter.config, indent=2))
        self._model_export_status_var.set(f"Training package written to {out_dir}")
        self._log_line(f"Model Studio training package written to {out_dir}")

    def _run_model_training(self, recommendation_index: int) -> None:
        if self._last_model_profile is None or recommendation_index >= len(self._last_model_recommendations):
            self._model_export_status_var.set("Refresh Model Studio before launching a local run.")
            return
        target = self._model_runtime_target_var.get().strip() or "local_cpu"
        self._record_demo_action("model.run_local")
        if target not in {"local_cpu", "local_gpu"}:
            self._model_export_status_var.set("Local execution is available for local_cpu or local_gpu targets. Use Export Training Package for cluster/Kaggle/Colab.")
            return
        starter = build_starter_model_config(
            self._last_model_profile,
            self._last_model_recommendations[recommendation_index],
            self._model_studio_selection(),
        )
        starter.config.setdefault("compute_budget", self._model_compute_budget_var.get().strip() or "balanced")
        starter.config.setdefault("interpretability", self._model_interpretability_var.get().strip() or "balanced")
        starter.config.setdefault("modality", self._model_modality_var.get().strip() or "auto")
        self._model_config_preview_var.set(json.dumps(starter.config, indent=2))
        self._model_export_status_var.set(f"Launching local training run for {starter.label}...")
        self._log_line(f"Model Studio launching local {target} training run for {starter.model_id}")

        def _worker() -> None:
            try:
                if bool(self._demo_mode_var.get()):
                    layout = self._storage_layout()
                    try:
                        cfg = load_config(_SOURCES_CFG) if _SOURCES_CFG.exists() else AppConfig(storage_root=str(layout.root))
                    except Exception:
                        cfg = AppConfig(storage_root=str(layout.root))
                    result = simulate_model_training_run(
                        layout,
                        cfg,
                        starter_config=starter.config,
                        runtime_target=target,
                        repo_root=Path.cwd(),
                    )
                else:
                    result = execute_training_run(
                        self._storage_layout(),
                        starter_config=starter.config,
                        runtime_target=target,
                    )
            except Exception as exc:
                def _fail() -> None:
                    self._model_export_status_var.set(f"Local training failed: {exc}")
                    self._log_line(f"Model Studio local training failed: {exc}")
                self._root.after(0, _fail)
                return

            def _finish() -> None:
                warning_text = f" Warnings: {' | '.join(result.warnings)}" if result.warnings else ""
                self._model_export_status_var.set(
                    f"{result.summary} Artifacts: {result.run_dir}{warning_text} "
                    "Charts and saved-run details are shown in Latest Run Outputs below."
                )
                self._model_import_path_var.set(str(result.run_dir))
                self._model_config_preview_var.set(json.dumps({
                    "run_name": result.run_name,
                    "run_dir": str(result.run_dir),
                    "family": result.family,
                    "task": result.task,
                    "metrics": result.metrics,
                    "warnings": list(result.warnings),
                }, indent=2))
                self._log_line(f"Model Studio local training finished: {result.run_dir}")
                self._refresh_selected_model_run(result.run_dir)
                self._compare_model_runs(record_demo=False)
                if self._left_notebook is not None and "model_studio" in self._left_tab_frames:
                    self._left_notebook.select(self._left_tab_frames["model_studio"])
            self._root.after(0, _finish)

        threading.Thread(target=_worker, daemon=True).start()

    def _import_model_training_run(self) -> None:
        source_dir = filedialog.askdirectory(
            title="Select completed training run directory",
            initialdir=str(self._storage_layout().models_dir),
        )
        if not source_dir:
            return
        try:
            imported_dir = import_training_run(self._storage_layout(), source_dir=source_dir)
        except Exception as exc:
            self._model_export_status_var.set(f"Run import failed: {exc}")
            self._log_line(f"Model Studio run import failed: {exc}")
            return
        self._model_import_path_var.set(str(imported_dir))
        self._model_export_status_var.set(f"Imported training run into {imported_dir}")
        self._model_config_preview_var.set(json.dumps({
            "imported_run_dir": str(imported_dir),
            "source_dir": source_dir,
        }, indent=2))
        self._log_line(f"Model Studio imported completed run: {imported_dir}")
        self._refresh_selected_model_run(imported_dir)
        self._compare_model_runs(record_demo=False)

    def _compare_model_runs(self, *, record_demo: bool = True) -> None:
        if record_demo:
            self._record_demo_action("model.compare_runs")
        comparisons = compare_training_runs(self._storage_layout())
        if not comparisons:
            self._model_run_comparison_var.set("No saved or imported runs are available yet.")
            self._model_run_detail_var.set("Run a local model or import a completed run to inspect experiment charts and metrics.")
            self._model_config_preview_var.set("No run-comparison report available yet.")
            self._model_run_option_paths = {}
            self._model_selected_run_var.set("")
            if hasattr(self, "_model_selected_run_combo"):
                self._model_selected_run_combo.configure(values=())
            for key, value in {
                "runs": "0",
                "curves": "0",
                "plots": "0",
                "native": "0",
                "best": "--",
            }.items():
                self._model_run_kpi_vars[key].set(value)
            for var in self._model_artifact_vars.values():
                var.set("")
            for index, vars_for_run in enumerate(self._model_recent_run_vars, start=1):
                vars_for_run["title"].set(f"Recent run {index}")
                vars_for_run["detail"].set("--")
            return
        report = build_training_run_report(self._storage_layout())
        top = comparisons[:3]
        option_pairs = [
            (
                f"{item.run_name} | {item.family} | {item.primary_metric_name}="
                f"{item.primary_metric_value if item.primary_metric_value is not None else '--'}",
                str(item.location),
            )
            for item in comparisons
        ]
        self._model_run_option_paths = {label: path for label, path in option_pairs}
        option_labels = [label for label, _path in option_pairs]
        if hasattr(self, "_model_selected_run_combo"):
            self._model_selected_run_combo.configure(values=option_labels)
        self._model_run_comparison_var.set(" | ".join(item.summary for item in top))
        self._model_run_detail_var.set(
            f"{report.get('run_count', 0)} runs tracked | "
            f"{report.get('chart_ready_count', 0)} training curves | "
            f"{report.get('test_plot_ready_count', 0)} test plots | "
            f"{report.get('native_graph_run_count', 0)} native graph runs"
        )
        best = report.get("best_overall") if isinstance(report.get("best_overall"), dict) else {}
        best_text = "--"
        if isinstance(best, dict) and best:
            best_text = str(best.get("run_name") or "--")
        self._model_run_kpi_vars["runs"].set(str(report.get("run_count", 0)))
        self._model_run_kpi_vars["curves"].set(str(report.get("chart_ready_count", 0)))
        self._model_run_kpi_vars["plots"].set(str(report.get("test_plot_ready_count", 0)))
        self._model_run_kpi_vars["native"].set(str(report.get("native_graph_run_count", 0)))
        self._model_run_kpi_vars["best"].set(best_text)
        latest = report.get("recent_runs", [])
        if isinstance(latest, list) and latest and isinstance(latest[0], dict):
            latest_run = latest[0]
            artifacts = latest_run.get("artifacts") if isinstance(latest_run.get("artifacts"), dict) else {}
        for vars_for_run, item in zip(self._model_recent_run_vars, top):
            vars_for_run["title"].set(item.run_name)
            vars_for_run["detail"].set(
                f"{item.family} | {item.primary_metric_name}={item.primary_metric_value if item.primary_metric_value is not None else '--'}\n"
                f"Source: {item.source}"
            )
        for vars_for_run in self._model_recent_run_vars[len(top):]:
            vars_for_run["title"].set("No run")
            vars_for_run["detail"].set("--")
        selected_run_dir = self._model_import_path_var.get().strip()
        if selected_run_dir and selected_run_dir != "No imported run yet.":
            matching_label = next((label for label, path in option_pairs if path == selected_run_dir), "")
            if matching_label:
                self._model_selected_run_var.set(matching_label)
            self._refresh_selected_model_run(selected_run_dir)
        elif isinstance(latest, list) and latest and isinstance(latest[0], dict):
            latest_dir = str(latest[0].get("location") or "")
            matching_label = next((label for label, path in option_pairs if path == latest_dir), "")
            if matching_label:
                self._model_selected_run_var.set(matching_label)
            self._refresh_selected_model_run(latest_dir)
        else:
            if option_labels:
                self._model_selected_run_var.set(option_labels[0])
                self._refresh_selected_model_run(option_pairs[0][1])
            else:
                self._model_selected_run_var.set("")
                for var in self._model_artifact_vars.values():
                    var.set("")
        self._model_config_preview_var.set(self._format_model_report_preview(report))
        self._log_line("Model Studio compared saved runs.")

    def _run_saved_model_inference(self) -> None:
        run_dir = self._model_import_path_var.get().strip()
        pdb_id = self._model_inference_pdb_var.get().strip()
        if not run_dir or run_dir == "No imported run yet.":
            self._model_inference_result_var.set("Choose or create a saved run first.")
            return
        if not pdb_id:
            self._model_inference_result_var.set("Enter a PDB ID to run saved-model inference.")
            return
        self._record_demo_action("model.inference")
        try:
            if bool(self._demo_mode_var.get()):
                layout = self._storage_layout()
                try:
                    cfg = load_config(_SOURCES_CFG) if _SOURCES_CFG.exists() else AppConfig(storage_root=str(layout.root))
                except Exception:
                    cfg = AppConfig(storage_root=str(layout.root))
                result = simulate_saved_model_inference(
                    layout,
                    cfg,
                    run_dir=run_dir,
                    pdb_id=pdb_id,
                    repo_root=Path.cwd(),
                )
            else:
                result = run_saved_model_inference(
                    self._storage_layout(),
                    run_dir=run_dir,
                    pdb_id=pdb_id,
                )
        except Exception as exc:
            self._model_inference_result_var.set(f"Inference failed: {exc}")
            self._log_line(f"Model Studio saved-model inference failed: {exc}")
            return
        summary = f"{result['pdb_id']}: prediction={result['prediction']} truth={result.get('ground_truth')}"
        self._model_inference_result_var.set(summary)
        self._model_config_preview_var.set(json.dumps(result, indent=2))
        self._log_line(f"Model Studio saved-model inference completed for {result['pdb_id']}")

    def _open_model_artifact(self, key: str) -> None:
        path_text = self._model_artifact_vars.get(key, tk.StringVar(value="")).get().strip()
        if not path_text:
            self._log_line(f"Model Studio artifact not available yet: {key}")
            return
        self._open_path(Path(path_text))

    def _refresh_selected_model_run(self, run_dir: str | Path | None) -> None:
        if run_dir is None:
            return
        run_path = Path(str(run_dir)).resolve()
        if not run_path.exists():
            return
        try:
            inspection = inspect_training_run(run_path, source="selected")
        except Exception as exc:
            self._log_line(f"Model Studio selected-run inspection failed: {exc}")
            return
        self._model_artifact_vars["run_dir"].set(str(inspection.location))
        for key in ("training_curve", "test_performance", "metrics", "test_predictions"):
            self._model_artifact_vars[key].set(str(inspection.artifacts.get(key) or ""))
        artifact_count = inspection.artifacts.get("artifact_count", "0")
        prediction_count = inspection.artifacts.get("test_prediction_count", "0")
        self._model_run_detail_var.set(
            f"Selected run: {inspection.run_name} | family={inspection.family} | "
            f"{inspection.primary_metric_name}={inspection.primary_metric_value if inspection.primary_metric_value is not None else '--'} | "
            f"artifacts={artifact_count} | test predictions={prediction_count}"
        )
        self._model_selected_run_preview_var.set(self._format_selected_run_preview(inspection))
        self._update_model_output_highlights(inspection)
        self._update_model_chart_previews(
            training_curve=str(inspection.artifacts.get("training_curve") or ""),
            test_performance=str(inspection.artifacts.get("test_performance") or ""),
        )

    def _load_selected_model_run(self) -> None:
        selected_label = self._model_selected_run_var.get().strip()
        run_path = self._model_run_option_paths.get(selected_label, "")
        if not run_path:
            self._log_line("Model Studio selected-run load skipped: no run is selected.")
            return
        self._model_import_path_var.set(run_path)
        self._refresh_selected_model_run(run_path)
        self._log_line(f"Model Studio loaded selected run: {run_path}")

    def _render_model_chart_preview(self, path: Path, *, key: str, label: ttk.Label) -> bool:
        if cairosvg is None or Image is None or ImageTk is None:
            return False
        try:
            png_bytes = cairosvg.svg2png(url=str(path))
            image = Image.open(io.BytesIO(png_bytes))
            image.thumbnail((520, 300))
            photo = ImageTk.PhotoImage(image)
        except Exception as exc:
            self._log_line(f"Model Studio chart preview failed for {path.name}: {exc}")
            return False
        self._model_chart_preview_images[key] = photo
        label.configure(image=photo, text=path.name, compound="top")
        return True

    def _update_model_chart_previews(self, *, training_curve: str, test_performance: str) -> None:
        if not hasattr(self, "_model_chart_training_label") or not hasattr(self, "_model_chart_test_label"):
            return
        self._model_chart_preview_images["training"] = None
        self._model_chart_preview_images["test"] = None
        training_label: ttk.Label = self._model_chart_training_label
        test_label: ttk.Label = self._model_chart_test_label
        training_label.configure(image="", text="Training curve preview unavailable")
        test_label.configure(image="", text="Test plot preview unavailable")
        if not training_curve and not test_performance:
            self._model_chart_preview_status_var.set("No chart artifacts are available for the selected run.")
            return
        preview_ready = False
        if training_curve:
            preview_ready = self._render_model_chart_preview(Path(training_curve), key="training", label=training_label) or preview_ready
            if not preview_ready:
                training_label.configure(text=f"Training curve: {Path(training_curve).name}")
        if test_performance:
            rendered = self._render_model_chart_preview(Path(test_performance), key="test", label=test_label)
            preview_ready = rendered or preview_ready
            if not rendered:
                test_label.configure(text=f"Test plot: {Path(test_performance).name}")
        if preview_ready:
            self._model_chart_preview_status_var.set("Inline chart previews loaded for the selected run.")
        elif cairosvg is None or Image is None or ImageTk is None:
            self._model_chart_preview_status_var.set(
                "Inline chart previews require Pillow and CairoSVG. Open the artifacts directly if previews are unavailable."
            )
        else:
            self._model_chart_preview_status_var.set("Chart artifacts found, but inline preview rendering was not successful.")

    @staticmethod
    def _format_selected_run_preview(inspection: Any) -> str:
        metrics = inspection.metrics if isinstance(getattr(inspection, "metrics", None), dict) else {}
        split_counts = inspection.split_counts if isinstance(getattr(inspection, "split_counts", None), dict) else {}
        history_summary = inspection.history_summary if isinstance(getattr(inspection, "history_summary", None), dict) else {}
        test_metrics = None
        for key in ("test", "test_metrics", "evaluation", "validation_metrics", "val"):
            value = metrics.get(key)
            if isinstance(value, dict) and value:
                test_metrics = value
                break
        prediction_preview = "No prediction file loaded."
        prediction_path = str(inspection.artifacts.get("test_predictions") or "")
        if prediction_path:
            try:
                payload = json.loads(Path(prediction_path).read_text(encoding="utf-8"))
                if isinstance(payload, list) and payload:
                    sample = payload[0]
                    prediction_preview = f"Sample prediction: {json.dumps(sample, indent=2)}"
            except Exception:
                prediction_preview = f"Predictions available at {prediction_path}"
        return "\n".join([
            f"Run: {inspection.run_name}",
            f"Family: {inspection.family} | Task: {inspection.task} | Backend: {inspection.backend_id}",
            f"Primary metric: {inspection.primary_metric_name}={inspection.primary_metric_value}",
            f"Split counts: train={split_counts.get('train', 0)} val={split_counts.get('val', 0)} test={split_counts.get('test', 0)}",
            f"History: epochs={history_summary.get('epoch_count', 0)} best_val={history_summary.get('best_val_metric', '--')}",
            f"Training curve: {'ready' if inspection.chart_ready else 'not available'}",
            f"Test plot: {'ready' if inspection.test_plot_ready else 'not available'}",
            f"Metric snapshot: {json.dumps(test_metrics, indent=2) if isinstance(test_metrics, dict) else '--'}",
            prediction_preview,
        ])

    @staticmethod
    def _format_model_report_preview(report: dict[str, object]) -> str:
        best = report.get("best_overall") if isinstance(report.get("best_overall"), dict) else {}
        best_text = "No best run yet."
        if isinstance(best, dict) and best:
            metric_name = str(best.get("primary_metric_name") or "metric")
            metric_value = best.get("primary_metric_value")
            best_text = f"{best.get('run_name', '--')} | {best.get('family', '--')} | {metric_name}={metric_value}"
        recent_lines: list[str] = []
        for item in report.get("recent_runs", []) if isinstance(report.get("recent_runs"), list) else []:
            if not isinstance(item, dict):
                continue
            recent_lines.append(
                f"- {item.get('run_name', '--')} | {item.get('family', '--')} | "
                f"{item.get('primary_metric_name', 'metric')}={item.get('primary_metric_value', '--')} | "
                f"charts={'yes' if item.get('chart_ready') else 'no'} | "
                f"plots={'yes' if item.get('test_plot_ready') else 'no'}"
            )
        if not recent_lines:
            recent_lines.append("- No recent runs tracked yet.")
        return "\n".join([
            "Run Snapshot",
            f"Best overall: {best_text}",
            f"Runs tracked: {report.get('run_count', 0)}",
            f"Training curves: {report.get('chart_ready_count', 0)}",
            f"Test plots: {report.get('test_plot_ready_count', 0)}",
            f"Native graph runs: {report.get('native_graph_run_count', 0)}",
            "",
            "Recent runs:",
            *recent_lines[:5],
        ])

    @staticmethod
    def _format_model_bullets(prefix: str, items: list[str] | tuple[str, ...]) -> str:
        clean = [str(item).strip() for item in items if str(item).strip()]
        if not clean:
            return f"{prefix}\n- --"
        return "\n".join([prefix, *[f"- {item}" for item in clean]])

    def _build_model_studio_tab(self, outer: ttk.Frame) -> None:
        canvas, _scrollbar, frame = self._make_scrollable_pane(outer)
        frame.columnconfigure(0, weight=1)
        frame.columnconfigure(1, weight=1)
        row = 0

        ttk.Label(
            frame,
            text="Model Studio",
            font=("Helvetica", 10, "bold"),
        ).grid(row=row, column=0, sticky="w", pady=(0, 4))
        model_refresh_header_btn = ttk.Button(
            frame,
            text="Refresh Recommendations",
            command=self._refresh_model_studio,
        )
        model_refresh_header_btn.grid(row=row, column=1, sticky="e")
        self._register_action_target("model.refresh", model_refresh_header_btn, tab="model_studio")
        row += 1

        ttk.Label(
            frame,
            text="Profiles the current workspace, checks architecture compatibility, and recommends three model strategies.",
            font=("Helvetica", 7),
            foreground="#888888",
        ).grid(row=row, column=0, columnspan=2, sticky="w")
        row += 1

        demo_control = ttk.LabelFrame(frame, text="Demo Control Center", padding=8, style="Section.TLabelframe")
        demo_control.grid(row=row, column=0, columnspan=2, sticky="ew", pady=(8, 0))
        demo_control.columnconfigure(1, weight=1)
        for info_row, (label, key) in enumerate([
            ("Next guided step:", "step"),
            ("What this demonstrates:", "detail"),
            ("Why this is innovative:", "innovation"),
            ("What to click now:", "instruction"),
            ("How to find it:", "scroll_hint"),
        ]):
            ttk.Label(demo_control, text=label, font=("Segoe UI Semibold", 8)).grid(row=info_row, column=0, sticky="nw", padx=(0, 6), pady=1)
            ttk.Label(demo_control, textvariable=self._demo_tutorial_vars[key], wraplength=760, justify="left", font=("Segoe UI", 8)).grid(row=info_row, column=1, sticky="w", pady=1)
        demo_action_row = ttk.Frame(demo_control)
        demo_action_row.grid(row=0, column=2, rowspan=3, sticky="ns", padx=(8, 0))
        ttk.Button(demo_action_row, text="Focus Next Demo Control", command=self._focus_demo_tutorial_target, style="Accent.TButton").grid(row=0, column=0, sticky="ew")
        ttk.Label(demo_action_row, textvariable=self._demo_tutorial_vars["progress"], style="Muted.TLabel").grid(row=1, column=0, sticky="w", pady=(6, 0))
        row += 1

        start_here = ttk.LabelFrame(frame, text="Start Here In Demo Mode", padding=8, style="Section.TLabelframe")
        start_here.grid(row=row, column=0, columnspan=2, sticky="ew", pady=(8, 0))
        start_here.columnconfigure(0, weight=1)
        ttk.Label(
            start_here,
            text=(
                "1. Click the highlighted control.\n"
                "2. Watch the live log and overview update.\n"
                "3. Come back here for the next guided step.\n"
                "4. In Model Studio, train a model and review the charts that appear below."
            ),
            justify="left",
            wraplength=860,
            font=("Segoe UI", 8),
        ).grid(row=0, column=0, sticky="w")
        row += 1

        controls = ttk.LabelFrame(frame, text="Model Design Inputs", padding=8, style="Section.TLabelframe")
        controls.grid(row=row, column=0, columnspan=2, sticky="ew", pady=(8, 0))
        controls.columnconfigure(1, weight=1)
        controls.columnconfigure(3, weight=1)
        control_rows = [
            ("Dataset source:", self._model_dataset_source_var, ["auto", "training_examples", "engineered_dataset", "custom_training_set"]),
            ("Modality:", self._model_modality_var, ["auto", "attributes", "graphs", "graphs+attributes", "unsupervised"]),
            ("Task:", self._model_task_var, ["auto", "regression", "classification", "ranking", "unsupervised"]),
            ("Preferred family:", self._model_family_var, ["auto", "random_forest", "xgboost", "dense_nn", "gnn", "hybrid_fusion", "cnn", "unet", "autoencoder", "clustering"]),
            ("Compute budget:", self._model_compute_budget_var, ["low", "balanced", "high"]),
            ("Interpretability:", self._model_interpretability_var, ["high", "balanced", "low"]),
            ("Runtime target:", self._model_runtime_target_var, ["local_cpu", "local_gpu", "cluster", "kaggle", "colab"]),
        ]
        for idx, (label, var, options) in enumerate(control_rows):
            grid_row = idx // 2
            grid_col = (idx % 2) * 2
            ttk.Label(controls, text=label).grid(row=grid_row, column=grid_col, sticky="w", pady=2)
            combo = ttk.Combobox(
                controls,
                textvariable=var,
                values=options,
                state="readonly",
                width=24,
            )
            combo.grid(row=grid_row, column=grid_col + 1, sticky="ew", padx=(6, 12), pady=2)
        row += 1

        architecture_frame = ttk.LabelFrame(frame, text="Architecture Preview", padding=8, style="Section.TLabelframe")
        architecture_frame.grid(row=row, column=0, columnspan=2, sticky="ew", pady=(8, 0))
        architecture_frame.columnconfigure(0, weight=1)
        ttk.Label(architecture_frame, textvariable=self._model_architecture_title_var, font=("Segoe UI Semibold", 10)).grid(row=0, column=0, sticky="w")
        ttk.Label(architecture_frame, textvariable=self._model_architecture_subtitle_var, wraplength=820, justify="left", style="Muted.TLabel").grid(row=1, column=0, sticky="w", pady=(2, 6))
        architecture_canvas = tk.Canvas(architecture_frame, width=586, height=138, bg="#fffdf7", highlightthickness=0)
        architecture_canvas.grid(row=2, column=0, sticky="ew")
        self._model_architecture_canvas = architecture_canvas
        ttk.Label(architecture_frame, textvariable=self._model_architecture_footer_var, wraplength=820, justify="left", font=("Segoe UI", 8)).grid(row=3, column=0, sticky="w", pady=(6, 0))
        row += 1

        profile_frame = ttk.LabelFrame(frame, text="Workspace Profile", padding=8, style="Section.TLabelframe")
        profile_frame.grid(row=row, column=0, columnspan=2, sticky="nsew", pady=(8, 0))
        profile_frame.columnconfigure(1, weight=1)
        for info_row, (label, var) in enumerate([
            ("Runtime:", self._model_runtime_summary_var),
            ("Summary:", self._model_profile_summary_var),
            ("Detail:", self._model_profile_detail_var),
            ("Compatibility:", self._model_warnings_var),
            ("Next action:", self._model_next_action_var),
            ("Last run/import:", self._model_import_path_var),
            ("Run comparison:", self._model_run_comparison_var),
            ("Experiment detail:", self._model_run_detail_var),
            ("Inference:", self._model_inference_result_var),
        ]):
            ttk.Label(profile_frame, text=label, font=("Segoe UI Semibold", 8)).grid(
                row=info_row, column=0, sticky="nw", padx=(0, 6), pady=1
            )
            ttk.Label(
                profile_frame,
                textvariable=var,
                wraplength=700,
                justify="left",
                font=("Segoe UI", 8),
            ).grid(row=info_row, column=1, sticky="w", pady=1)
        profile_metrics = ttk.Frame(profile_frame)
        profile_metrics.grid(row=len([
            ("Runtime:", self._model_runtime_summary_var),
            ("Summary:", self._model_profile_summary_var),
            ("Detail:", self._model_profile_detail_var),
            ("Compatibility:", self._model_warnings_var),
            ("Next action:", self._model_next_action_var),
            ("Last run/import:", self._model_import_path_var),
            ("Run comparison:", self._model_run_comparison_var),
            ("Experiment detail:", self._model_run_detail_var),
            ("Inference:", self._model_inference_result_var),
        ]), column=0, columnspan=2, sticky="ew", pady=(10, 0))
        for index in range(6):
            profile_metrics.columnconfigure(index, weight=1)
        for index, (key, label) in enumerate([
            ("examples", "Examples"),
            ("train", "Train"),
            ("val", "Val"),
            ("test", "Test"),
            ("modalities", "Modalities"),
            ("tasks", "Tasks"),
        ]):
            card = ttk.Frame(profile_metrics, style="Card.TFrame", padding=8)
            card.grid(row=0, column=index, sticky="ew", padx=(0 if index == 0 else 4, 0))
            ttk.Label(card, text=label, style="Muted.TLabel").grid(row=0, column=0, sticky="w")
            ttk.Label(
                card,
                textvariable=self._model_profile_kpi_vars[key],
                font=("Segoe UI Semibold", 11),
                wraplength=140,
                justify="left",
            ).grid(row=1, column=0, sticky="w", pady=(2, 0))
        row += 1

        run_insights = ttk.LabelFrame(frame, text="Run Insights", padding=8, style="Section.TLabelframe")
        run_insights.grid(row=row, column=0, columnspan=2, sticky="nsew", pady=(8, 0))
        for index in range(3):
            run_insights.columnconfigure(index, weight=1)
        for index, (key, label) in enumerate([
            ("runs", "Tracked runs"),
            ("curves", "Curves"),
            ("plots", "Test plots"),
            ("native", "Native graph"),
            ("best", "Best run"),
        ]):
            card = ttk.Frame(run_insights, style="Card.TFrame", padding=8)
            card.grid(row=index // 3, column=index % 3, sticky="ew", padx=(0 if index % 3 == 0 else 4, 0), pady=(0 if index < 3 else 4, 0))
            ttk.Label(card, text=label, style="Muted.TLabel").grid(row=0, column=0, sticky="w")
            ttk.Label(
                card,
                textvariable=self._model_run_kpi_vars[key],
                font=("Segoe UI Semibold", 11),
                wraplength=150 if key == "best" else 90,
                justify="left",
            ).grid(row=1, column=0, sticky="w", pady=(2, 0))
        artifact_frame = ttk.Frame(run_insights)
        artifact_frame.grid(row=2, column=0, columnspan=3, sticky="ew", pady=(10, 0))
        for index in range(3):
            artifact_frame.columnconfigure(index, weight=1)
        for index, (key, label) in enumerate([
            ("run_dir", "Run Folder"),
            ("training_curve", "Curve"),
            ("test_performance", "Test Plot"),
            ("metrics", "Metrics"),
            ("test_predictions", "Predictions"),
        ]):
            ttk.Button(
                artifact_frame,
                text=label,
                command=lambda artifact_key=key: self._open_model_artifact(artifact_key),
            ).grid(row=index // 3, column=index % 3, sticky="ew", padx=(0 if index % 3 == 0 else 4, 0), pady=(0 if index < 3 else 4, 0))
        selected_run_frame = ttk.Frame(run_insights)
        selected_run_frame.grid(row=3, column=0, columnspan=3, sticky="ew", pady=(10, 0))
        selected_run_frame.columnconfigure(1, weight=1)
        ttk.Label(selected_run_frame, text="Selected run:", style="Muted.TLabel").grid(row=0, column=0, sticky="w")
        selected_run_combo = ttk.Combobox(
            selected_run_frame,
            textvariable=self._model_selected_run_var,
            values=(),
            state="readonly",
            width=48,
        )
        selected_run_combo.grid(row=0, column=1, sticky="ew", padx=(6, 6))
        self._model_selected_run_combo = selected_run_combo
        ttk.Button(
            selected_run_frame,
            text="Load Selected Run",
            command=self._load_selected_model_run,
        ).grid(row=0, column=2, sticky="e")
        ttk.Label(
            run_insights,
            textvariable=self._model_run_detail_var,
            wraplength=760,
            justify="left",
            font=("Segoe UI", 8),
        ).grid(row=4, column=0, columnspan=3, sticky="w", pady=(10, 0))
        recent_runs_frame = ttk.Frame(run_insights)
        recent_runs_frame.grid(row=5, column=0, columnspan=3, sticky="ew", pady=(10, 0))
        recent_runs_frame.columnconfigure(0, weight=1)
        for index, vars_for_run in enumerate(self._model_recent_run_vars):
            card = ttk.Frame(recent_runs_frame, style="Card.TFrame", padding=8)
            card.grid(row=index, column=0, sticky="ew", pady=(0 if index == 0 else 4, 0))
            ttk.Label(card, textvariable=vars_for_run["title"], style="Muted.TLabel").grid(row=0, column=0, sticky="w")
            ttk.Label(
                card,
                textvariable=vars_for_run["detail"],
                wraplength=720,
                justify="left",
                font=("Segoe UI", 8),
            ).grid(row=1, column=0, sticky="w", pady=(4, 0))
        row += 1

        for index, vars_for_card in enumerate(self._model_recommendation_vars):
            card = ttk.LabelFrame(frame, textvariable=vars_for_card["title"], padding=8, style="Section.TLabelframe")
            card.grid(row=row, column=0, columnspan=2, sticky="ew", pady=(8, 0))
            card.columnconfigure(0, weight=1)
            for card_row, key in enumerate(["summary", "why", "strengths", "drawbacks", "recipe"]):
                ttk.Label(
                    card,
                    textvariable=vars_for_card[key],
                    wraplength=760,
                    justify="left",
                    font=("Segoe UI", 8),
                ).grid(row=card_row, column=0, sticky="w", pady=1)
            actions = ttk.Frame(card)
            actions.grid(row=5, column=0, sticky="ew", pady=(8, 0))
            for action_col in range(3):
                actions.columnconfigure(action_col, weight=1)
            ttk.Button(
                actions,
                text="Config",
                command=lambda idx=index: self._export_model_starter_config(idx),
            ).grid(row=0, column=0, sticky="ew", padx=(0, 4))
            ttk.Button(
                actions,
                text="Package",
                command=lambda idx=index: self._export_model_training_package(idx),
            ).grid(row=0, column=1, sticky="ew", padx=4)
            run_locally_btn = ttk.Button(
                actions,
                text="Train",
                command=lambda idx=index: self._run_model_training(idx),
            )
            run_locally_btn.grid(row=0, column=2, sticky="ew", padx=(4, 0))
            if index == 0:
                self._register_action_target(
                    "model.run_local.primary",
                    run_locally_btn,
                    tab="model_studio",
                    scroll_hint="Scroll to the first recommendation card in Model Studio.",
                )
            row += 1

        preview_frame = ttk.LabelFrame(frame, text="Latest Run Outputs", padding=8, style="Section.TLabelframe")
        preview_frame.grid(row=row, column=0, columnspan=2, sticky="ew", pady=(8, 0))
        preview_frame.columnconfigure(0, weight=1)
        highlights = ttk.Frame(preview_frame, style="Card.TFrame", padding=8)
        highlights.grid(row=0, column=0, sticky="ew", pady=(0, 8))
        for index in range(4):
            highlights.columnconfigure(index, weight=1)
        for index, (label, key) in enumerate([
            ("Run highlight", "headline"),
            ("Primary metric", "metric"),
            ("Artifacts", "artifacts"),
            ("Output status", "status"),
        ]):
            card = ttk.Frame(highlights, style="AltCard.TFrame", padding=8)
            card.grid(row=0, column=index, sticky="ew", padx=(0 if index == 0 else 4, 0))
            ttk.Label(card, text=label, style="Muted.TLabel").grid(row=0, column=0, sticky="w")
            ttk.Label(card, textvariable=self._model_output_highlights_vars[key], wraplength=220, justify="left", font=("Segoe UI", 8)).grid(row=1, column=0, sticky="w", pady=(3, 0))
        ttk.Label(
            preview_frame,
            textvariable=self._model_export_status_var,
            wraplength=760,
            justify="left",
            font=("Segoe UI", 8),
        ).grid(row=1, column=0, sticky="w", pady=(0, 4))
        ttk.Label(
            preview_frame,
            textvariable=self._model_config_preview_var,
            wraplength=760,
            justify="left",
            font=("Cascadia Code", 8),
        ).grid(row=2, column=0, sticky="w")
        ttk.Label(
            preview_frame,
            textvariable=self._model_selected_run_preview_var,
            wraplength=760,
            justify="left",
            font=("Segoe UI", 8),
        ).grid(row=3, column=0, sticky="w", pady=(10, 0))
        ttk.Label(
            preview_frame,
            textvariable=self._model_chart_preview_status_var,
            wraplength=760,
            justify="left",
            font=("Segoe UI", 8),
        ).grid(row=4, column=0, sticky="w", pady=(8, 0))
        chart_preview_frame = ttk.Frame(preview_frame)
        chart_preview_frame.grid(row=5, column=0, sticky="ew", pady=(8, 0))
        chart_preview_frame.columnconfigure(0, weight=1)
        chart_preview_frame.columnconfigure(1, weight=1)
        training_chart = ttk.Label(chart_preview_frame, text="Training curve preview unavailable", justify="center")
        training_chart.grid(row=0, column=0, sticky="nsew", padx=(0, 4))
        test_chart = ttk.Label(chart_preview_frame, text="Test plot preview unavailable", justify="center")
        test_chart.grid(row=0, column=1, sticky="nsew", padx=(4, 0))
        self._model_chart_training_label = training_chart
        self._model_chart_test_label = test_chart
        action_row = ttk.Frame(preview_frame)
        action_row.grid(row=6, column=0, sticky="ew", pady=(8, 0))
        action_row.columnconfigure(0, weight=1)
        action_row.columnconfigure(1, weight=1)
        ttk.Button(
            action_row,
            text="Import Run",
            command=self._import_model_training_run,
        ).grid(row=0, column=0, sticky="ew", padx=(0, 4))
        compare_runs_btn = ttk.Button(
            action_row,
            text="Compare",
            command=self._compare_model_runs,
        )
        compare_runs_btn.grid(row=0, column=1, sticky="ew", padx=4)
        self._register_action_target("model.compare_runs", compare_runs_btn, tab="model_studio")
        ttk.Button(
            action_row,
            text="Refresh",
            command=self._refresh_model_studio,
        ).grid(row=1, column=0, columnspan=2, sticky="ew", pady=(6, 0))
        inference_controls = ttk.Frame(preview_frame)
        inference_controls.grid(row=7, column=0, sticky="ew", pady=(8, 0))
        inference_controls.columnconfigure(1, weight=1)
        ttk.Label(inference_controls, text="Saved-model PDB ID:").grid(row=0, column=0, sticky="w")
        ttk.Entry(inference_controls, textvariable=self._model_inference_pdb_var, width=16).grid(row=0, column=1, sticky="ew", padx=(6, 6))
        inference_btn = ttk.Button(
            inference_controls,
            text="Infer",
            command=self._run_saved_model_inference,
        )
        inference_btn.grid(row=0, column=2, sticky="e")
        self._register_action_target(
            "model.saved_inference",
            inference_btn,
            tab="model_studio",
            scroll_hint="Look in the lower inference controls section of Model Studio.",
        )
        row += 1

        ttk.Button(
            frame,
            text="Refresh Model Studio",
            command=self._refresh_model_studio,
            style="Accent.TButton",
        ).grid(row=row, column=0, columnspan=2, sticky="ew", pady=(10, 0))
        self._draw_model_architecture_preview()
        self._refresh_model_studio(record_demo=False)
        self._bind_canvas_mousewheel(canvas, frame)

    def _build_model_studio_tab(self, outer: ttk.Frame) -> None:
        canvas, _scrollbar, frame = self._make_scrollable_pane(outer)
        frame.columnconfigure(0, weight=1)

        header = ttk.Frame(frame)
        header.grid(row=0, column=0, sticky="ew")
        header.columnconfigure(0, weight=1)
        ttk.Label(header, text="Model Studio", font=("Helvetica", 11, "bold")).grid(row=0, column=0, sticky="w")
        ttk.Label(
            header,
            text="A staged workspace for designing a model, understanding the recommendation, and reviewing experiment outputs.",
            font=("Segoe UI", 8),
            foreground="#64748b",
        ).grid(row=1, column=0, sticky="w", pady=(2, 0))
        refresh_btn = ttk.Button(header, text="Refresh Studio", command=self._refresh_model_studio)
        refresh_btn.grid(row=0, column=1, rowspan=2, sticky="e")
        self._register_action_target("model.refresh", refresh_btn, tab="model_studio", subtab="guide")

        hero = ttk.Frame(frame, style="Card.TFrame", padding=10)
        hero.grid(row=1, column=0, sticky="ew", pady=(10, 0))
        for index in range(3):
            hero.columnconfigure(index, weight=1)
        for index, (title, text) in enumerate([
            ("Guide", "Start with the guided next step so Demo Mode tells you exactly what to click."),
            ("Design", "Choose modality, task, family, budget, and runtime in one clean place."),
            ("Runs", "Train, compare, and inspect charts in a dedicated outputs view."),
        ]):
            card = ttk.Frame(hero, style="AltCard.TFrame", padding=8)
            card.grid(row=0, column=index, sticky="ew", padx=(0 if index == 0 else 6, 0))
            ttk.Label(card, text=title, font=("Segoe UI Semibold", 9)).grid(row=0, column=0, sticky="w")
            ttk.Label(card, text=text, wraplength=240, justify="left", style="Muted.TLabel").grid(row=1, column=0, sticky="w", pady=(4, 0))

        studio_nav = ttk.Notebook(frame)
        studio_nav.grid(row=2, column=0, sticky="ew", pady=(10, 0))
        self._model_studio_notebook = studio_nav
        self._model_studio_pages = {}
        for key, label in [
            ("guide", " Guide "),
            ("design", " Design "),
            ("recommendations", " Recommendations "),
            ("runs", " Runs "),
        ]:
            page = ttk.Frame(studio_nav, padding=10)
            page.columnconfigure(0, weight=1)
            self._model_studio_pages[key] = page
            studio_nav.add(page, text=label)

        guide = self._model_studio_pages["guide"]
        demo_control = ttk.LabelFrame(guide, text="Demo Control Center", padding=8, style="Section.TLabelframe")
        demo_control.grid(row=0, column=0, sticky="ew")
        demo_control.columnconfigure(1, weight=1)
        for info_row, (label, key) in enumerate([
            ("Next step:", "step"),
            ("What it demonstrates:", "detail"),
            ("Why it matters:", "innovation"),
            ("Click now:", "instruction"),
            ("Find it here:", "scroll_hint"),
        ]):
            ttk.Label(demo_control, text=label, font=("Segoe UI Semibold", 8)).grid(row=info_row, column=0, sticky="nw", padx=(0, 8), pady=2)
            ttk.Label(demo_control, textvariable=self._demo_tutorial_vars[key], wraplength=760, justify="left", font=("Segoe UI", 8)).grid(row=info_row, column=1, sticky="w", pady=2)
        guide_side = ttk.Frame(demo_control)
        guide_side.grid(row=0, column=2, rowspan=3, sticky="ns", padx=(8, 0))
        ttk.Button(guide_side, text="Focus Next Control", command=self._focus_demo_tutorial_target, style="Accent.TButton").grid(row=0, column=0, sticky="ew")
        ttk.Label(guide_side, textvariable=self._demo_tutorial_vars["progress"], style="Muted.TLabel").grid(row=1, column=0, sticky="w", pady=(6, 0))
        start_here = ttk.LabelFrame(guide, text="Start Here", padding=8, style="Section.TLabelframe")
        start_here.grid(row=1, column=0, sticky="ew", pady=(10, 0))
        ttk.Label(
            start_here,
            text=(
                "1. Follow the highlighted control.\n"
                "2. Watch the live log and the overview change.\n"
                "3. Move to Design to pick a model path.\n"
                "4. Move to Runs to inspect the training outputs and charts."
            ),
            wraplength=860,
            justify="left",
            font=("Segoe UI", 8),
        ).grid(row=0, column=0, sticky="w")
        workspace = ttk.LabelFrame(guide, text="Workspace Snapshot", padding=8, style="Section.TLabelframe")
        workspace.grid(row=2, column=0, sticky="ew", pady=(10, 0))
        workspace.columnconfigure(1, weight=1)
        for info_row, (label, var) in enumerate([
            ("Summary:", self._model_profile_summary_var),
            ("Detail:", self._model_profile_detail_var),
            ("Compatibility:", self._model_warnings_var),
            ("Next action:", self._model_next_action_var),
        ]):
            ttk.Label(workspace, text=label, font=("Segoe UI Semibold", 8)).grid(row=info_row, column=0, sticky="nw", padx=(0, 8), pady=2)
            ttk.Label(workspace, textvariable=var, wraplength=760, justify="left", font=("Segoe UI", 8)).grid(row=info_row, column=1, sticky="w", pady=2)
        workspace_cards = ttk.Frame(workspace)
        workspace_cards.grid(row=4, column=0, columnspan=2, sticky="ew", pady=(8, 0))
        for index in range(3):
            workspace_cards.columnconfigure(index, weight=1)
        for index, (key, label) in enumerate([
            ("examples", "Examples"),
            ("train", "Train"),
            ("val", "Val"),
            ("test", "Test"),
            ("modalities", "Modalities"),
            ("tasks", "Tasks"),
        ]):
            card = ttk.Frame(workspace_cards, style="Card.TFrame", padding=8)
            card.grid(row=index // 3, column=index % 3, sticky="ew", padx=(0 if index % 3 == 0 else 6, 0), pady=(0 if index < 3 else 6, 0))
            ttk.Label(card, text=label, style="Muted.TLabel").grid(row=0, column=0, sticky="w")
            ttk.Label(card, textvariable=self._model_profile_kpi_vars[key], font=("Segoe UI Semibold", 11), wraplength=220, justify="left").grid(row=1, column=0, sticky="w", pady=(3, 0))

        design = self._model_studio_pages["design"]
        controls = ttk.LabelFrame(design, text="Model Design Inputs", padding=8, style="Section.TLabelframe")
        controls.grid(row=0, column=0, sticky="ew")
        controls.columnconfigure(1, weight=1)
        for row, (label, var, options) in enumerate([
            ("Dataset source", self._model_dataset_source_var, ["auto", "training_examples", "engineered_dataset", "custom_training_set"]),
            ("Modality", self._model_modality_var, ["auto", "attributes", "graphs", "graphs+attributes", "unsupervised"]),
            ("Task", self._model_task_var, ["auto", "regression", "classification", "ranking", "unsupervised"]),
            ("Preferred family", self._model_family_var, ["auto", "random_forest", "xgboost", "dense_nn", "gnn", "hybrid_fusion", "cnn", "unet", "autoencoder", "clustering"]),
            ("Compute budget", self._model_compute_budget_var, ["low", "balanced", "high"]),
            ("Interpretability", self._model_interpretability_var, ["high", "balanced", "low"]),
            ("Runtime target", self._model_runtime_target_var, ["local_cpu", "local_gpu", "cluster", "kaggle", "colab"]),
        ]):
            ttk.Label(controls, text=f"{label}:").grid(row=row, column=0, sticky="w", pady=4, padx=(0, 8))
            ttk.Combobox(controls, textvariable=var, values=options, state="readonly", width=28).grid(row=row, column=1, sticky="ew", pady=4)
        architecture = ttk.LabelFrame(design, text="Architecture Preview", padding=8, style="Section.TLabelframe")
        architecture.grid(row=1, column=0, sticky="ew", pady=(10, 0))
        architecture.columnconfigure(0, weight=1)
        ttk.Label(architecture, textvariable=self._model_architecture_title_var, font=("Segoe UI Semibold", 10)).grid(row=0, column=0, sticky="w")
        ttk.Label(architecture, textvariable=self._model_architecture_subtitle_var, wraplength=860, justify="left", style="Muted.TLabel").grid(row=1, column=0, sticky="w", pady=(2, 6))
        architecture_canvas = tk.Canvas(architecture, width=760, height=150, bg="#fffdf7", highlightthickness=0)
        architecture_canvas.grid(row=2, column=0, sticky="ew")
        self._model_architecture_canvas = architecture_canvas
        ttk.Label(architecture, textvariable=self._model_architecture_footer_var, wraplength=860, justify="left", font=("Segoe UI", 8)).grid(row=3, column=0, sticky="w", pady=(6, 0))
        summary = ttk.LabelFrame(design, text="Current Studio Summary", padding=8, style="Section.TLabelframe")
        summary.grid(row=2, column=0, sticky="ew", pady=(10, 0))
        summary.columnconfigure(1, weight=1)
        for info_row, (label, var) in enumerate([
            ("Runtime:", self._model_runtime_summary_var),
            ("Compatibility:", self._model_warnings_var),
            ("Next action:", self._model_next_action_var),
        ]):
            ttk.Label(summary, text=label, font=("Segoe UI Semibold", 8)).grid(row=info_row, column=0, sticky="nw", padx=(0, 8), pady=2)
            ttk.Label(summary, textvariable=var, wraplength=760, justify="left", font=("Segoe UI", 8)).grid(row=info_row, column=1, sticky="w", pady=2)

        recommendations = self._model_studio_pages["recommendations"]
        ttk.Label(
            recommendations,
            text="These cards explain the current best-fit model paths. Start with the first one unless you want to demonstrate a specific tradeoff.",
            wraplength=860,
            justify="left",
            style="Muted.TLabel",
        ).grid(row=0, column=0, sticky="w")
        for index, vars_for_card in enumerate(self._model_recommendation_vars):
            card = ttk.LabelFrame(recommendations, textvariable=vars_for_card["title"], padding=8, style="Section.TLabelframe")
            card.grid(row=index + 1, column=0, sticky="ew", pady=(10 if index == 0 else 8, 0))
            card.columnconfigure(0, weight=1)
            ttk.Label(card, textvariable=vars_for_card["summary"], wraplength=860, justify="left", font=("Segoe UI Semibold", 9)).grid(row=0, column=0, sticky="w")
            ttk.Label(card, textvariable=vars_for_card["why"], wraplength=860, justify="left", font=("Segoe UI", 8)).grid(row=1, column=0, sticky="w", pady=(4, 0))
            detail = ttk.Frame(card)
            detail.grid(row=2, column=0, sticky="ew", pady=(6, 0))
            detail.columnconfigure(0, weight=1)
            detail.columnconfigure(1, weight=1)
            ttk.Label(detail, textvariable=vars_for_card["strengths"], wraplength=420, justify="left", font=("Segoe UI", 8)).grid(row=0, column=0, sticky="nw", padx=(0, 8))
            ttk.Label(detail, textvariable=vars_for_card["drawbacks"], wraplength=420, justify="left", font=("Segoe UI", 8)).grid(row=0, column=1, sticky="nw")
            ttk.Label(card, textvariable=vars_for_card["recipe"], wraplength=860, justify="left", font=("Segoe UI", 8)).grid(row=3, column=0, sticky="w", pady=(6, 0))
            actions = ttk.Frame(card)
            actions.grid(row=4, column=0, sticky="ew", pady=(8, 0))
            for action_col in range(3):
                actions.columnconfigure(action_col, weight=1)
            ttk.Button(actions, text="Config", command=lambda idx=index: self._export_model_starter_config(idx)).grid(row=0, column=0, sticky="ew", padx=(0, 4))
            ttk.Button(actions, text="Package", command=lambda idx=index: self._export_model_training_package(idx)).grid(row=0, column=1, sticky="ew", padx=4)
            train_btn = ttk.Button(actions, text="Train", command=lambda idx=index: self._run_model_training(idx))
            train_btn.grid(row=0, column=2, sticky="ew", padx=(4, 0))
            if index == 0:
                self._register_action_target("model.run_local.primary", train_btn, tab="model_studio", subtab="recommendations", scroll_hint="Open Recommendations and use the first Train button.")

        runs = self._model_studio_pages["runs"]
        insights = ttk.LabelFrame(runs, text="Run Insights", padding=8, style="Section.TLabelframe")
        insights.grid(row=0, column=0, sticky="ew")
        for index in range(3):
            insights.columnconfigure(index, weight=1)
        for index, (key, label) in enumerate([
            ("runs", "Tracked runs"),
            ("curves", "Curves"),
            ("plots", "Test plots"),
            ("native", "Native graph"),
            ("best", "Best run"),
        ]):
            card = ttk.Frame(insights, style="Card.TFrame", padding=8)
            card.grid(row=index // 3, column=index % 3, sticky="ew", padx=(0 if index % 3 == 0 else 6, 0), pady=(0 if index < 3 else 6, 0))
            ttk.Label(card, text=label, style="Muted.TLabel").grid(row=0, column=0, sticky="w")
            ttk.Label(card, textvariable=self._model_run_kpi_vars[key], font=("Segoe UI Semibold", 11), wraplength=220, justify="left").grid(row=1, column=0, sticky="w", pady=(3, 0))
        run_actions = ttk.LabelFrame(runs, text="Run Actions", padding=8, style="Section.TLabelframe")
        run_actions.grid(row=1, column=0, sticky="ew", pady=(10, 0))
        for index in range(3):
            run_actions.columnconfigure(index, weight=1)
        ttk.Button(run_actions, text="Import Run", command=self._import_model_training_run).grid(row=0, column=0, sticky="ew", padx=(0, 4))
        compare_runs_btn = ttk.Button(run_actions, text="Compare Experiments", command=self._compare_model_runs)
        compare_runs_btn.grid(row=0, column=1, sticky="ew", padx=4)
        self._register_action_target("model.compare_runs", compare_runs_btn, tab="model_studio", subtab="runs")
        ttk.Button(run_actions, text="Refresh Studio", command=self._refresh_model_studio).grid(row=0, column=2, sticky="ew", padx=(4, 0))
        selected = ttk.LabelFrame(runs, text="Selected Run", padding=8, style="Section.TLabelframe")
        selected.grid(row=2, column=0, sticky="ew", pady=(10, 0))
        selected.columnconfigure(1, weight=1)
        ttk.Label(selected, text="Saved run:", style="Muted.TLabel").grid(row=0, column=0, sticky="w")
        selected_combo = ttk.Combobox(selected, textvariable=self._model_selected_run_var, values=(), state="readonly", width=48)
        selected_combo.grid(row=0, column=1, sticky="ew", padx=(6, 6))
        self._model_selected_run_combo = selected_combo
        ttk.Button(selected, text="Load", command=self._load_selected_model_run).grid(row=0, column=2, sticky="e")
        ttk.Label(selected, textvariable=self._model_run_detail_var, wraplength=860, justify="left", font=("Segoe UI", 8)).grid(row=1, column=0, columnspan=3, sticky="w", pady=(8, 0))
        outputs = ttk.LabelFrame(runs, text="Latest Run Outputs", padding=8, style="Section.TLabelframe")
        outputs.grid(row=3, column=0, sticky="ew", pady=(10, 0))
        outputs.columnconfigure(0, weight=1)
        highlight_host = ttk.Frame(outputs, style="Card.TFrame", padding=8)
        highlight_host.grid(row=0, column=0, sticky="ew", pady=(0, 8))
        for index in range(2):
            highlight_host.columnconfigure(index, weight=1)
        for index, (label, key) in enumerate([
            ("Run highlight", "headline"),
            ("Primary metric", "metric"),
            ("Artifacts", "artifacts"),
            ("Output status", "status"),
        ]):
            card = ttk.Frame(highlight_host, style="AltCard.TFrame", padding=8)
            card.grid(row=index // 2, column=index % 2, sticky="ew", padx=(0 if index % 2 == 0 else 6, 0), pady=(0 if index < 2 else 6, 0))
            ttk.Label(card, text=label, style="Muted.TLabel").grid(row=0, column=0, sticky="w")
            ttk.Label(card, textvariable=self._model_output_highlights_vars[key], wraplength=380, justify="left", font=("Segoe UI", 8)).grid(row=1, column=0, sticky="w", pady=(3, 0))
        ttk.Label(outputs, textvariable=self._model_export_status_var, wraplength=860, justify="left", font=("Segoe UI", 8)).grid(row=1, column=0, sticky="w")
        ttk.Label(outputs, textvariable=self._model_selected_run_preview_var, wraplength=860, justify="left", font=("Segoe UI", 8)).grid(row=2, column=0, sticky="w", pady=(8, 0))
        ttk.Label(outputs, textvariable=self._model_chart_preview_status_var, wraplength=860, justify="left", font=("Segoe UI", 8)).grid(row=3, column=0, sticky="w", pady=(8, 0))
        chart_frame = ttk.Frame(outputs)
        chart_frame.grid(row=4, column=0, sticky="ew", pady=(8, 0))
        chart_frame.columnconfigure(0, weight=1)
        chart_frame.columnconfigure(1, weight=1)
        training_chart = ttk.Label(chart_frame, text="Training curve preview unavailable", justify="center")
        training_chart.grid(row=0, column=0, sticky="nsew", padx=(0, 6))
        test_chart = ttk.Label(chart_frame, text="Test plot preview unavailable", justify="center")
        test_chart.grid(row=0, column=1, sticky="nsew", padx=(6, 0))
        self._model_chart_training_label = training_chart
        self._model_chart_test_label = test_chart
        artifact_actions = ttk.Frame(outputs)
        artifact_actions.grid(row=5, column=0, sticky="ew", pady=(8, 0))
        for index in range(3):
            artifact_actions.columnconfigure(index, weight=1)
        ttk.Button(artifact_actions, text="Run Folder", command=lambda: self._open_model_artifact("run_dir")).grid(row=0, column=0, sticky="ew", padx=(0, 4))
        ttk.Button(artifact_actions, text="Metrics", command=lambda: self._open_model_artifact("metrics")).grid(row=0, column=1, sticky="ew", padx=4)
        ttk.Button(artifact_actions, text="Predictions", command=lambda: self._open_model_artifact("test_predictions")).grid(row=0, column=2, sticky="ew", padx=(4, 0))
        inference = ttk.LabelFrame(runs, text="Saved-Model Inference", padding=8, style="Section.TLabelframe")
        inference.grid(row=4, column=0, sticky="ew", pady=(10, 0))
        inference.columnconfigure(1, weight=1)
        ttk.Label(inference, text="PDB ID:", style="Muted.TLabel").grid(row=0, column=0, sticky="w")
        ttk.Entry(inference, textvariable=self._model_inference_pdb_var, width=18).grid(row=0, column=1, sticky="ew", padx=(6, 6))
        inference_btn = ttk.Button(inference, text="Run Inference", command=self._run_saved_model_inference)
        inference_btn.grid(row=0, column=2, sticky="e")
        self._register_action_target("model.saved_inference", inference_btn, tab="model_studio", subtab="runs", scroll_hint="Open Runs and use Run Inference.")
        ttk.Label(inference, textvariable=self._model_inference_result_var, wraplength=860, justify="left", font=("Segoe UI", 8)).grid(row=1, column=0, columnspan=3, sticky="w", pady=(8, 0))

        self._model_studio_notebook.select(self._model_studio_pages["guide"])
        self._draw_model_architecture_preview()
        self._refresh_model_studio(record_demo=False)
        self._bind_canvas_mousewheel(canvas, frame)

    def _build_pipeline_panel(self) -> None:
        right = tk.Frame(self._root, bg=_APP_BG)
        right.grid(row=1, column=1, sticky="nsew", padx=(4, 10), pady=(10, 4))
        self._pipeline_panel_host = right
        canvas, _scrollbar, outer = self._make_scrollable_pane(right)
        self._pipeline_scroll_canvas = canvas
        self._pipeline_scroll_frame = outer

        outer.columnconfigure(0, weight=1)

        # Data overview at top
        self._build_overview(outer)

        workflow_card_row = self._add_guidance_card(
            outer,
            row=1,
            title="Run Workflow",
            summary="Use the pipeline panel to run individual stages when you are iterating, or run the full workflow once your sources, search slice, and options look right.",
            bullets=[
                "Refresh Overview to re-scan current workspace artifacts without launching work.",
                "Run Full Pipeline keeps the right-side status panel and live log synchronized as each stage advances.",
            ],
            pady=(6, 6),
        )

        status_frame = ttk.LabelFrame(outer, text="Run Status", padding=10, style="Section.TLabelframe")
        status_frame.grid(row=workflow_card_row, column=0, sticky="ew", pady=(0, 0))
        status_frame.columnconfigure(1, weight=1)
        status_items = [
            ("Workflow state:", self._run_state_var),
            ("Current stage:", self._run_current_stage_var),
            ("Progress:", self._run_progress_var),
            ("Next step:", self._run_next_stage_var),
            ("Last update:", self._run_last_message_var),
            ("Elapsed:", self._run_elapsed_var),
        ]
        for row, (label, var) in enumerate(status_items):
            ttk.Label(
                status_frame,
                text=label,
                font=("Segoe UI Semibold", 8),
            ).grid(row=row, column=0, sticky="nw", padx=(0, 6), pady=1)
            ttk.Label(
                status_frame,
                textvariable=var,
                wraplength=760,
                justify="left",
                font=("Segoe UI", 8),
            ).grid(row=row, column=1, sticky="w", pady=1)

        # Pipeline stages
        pipeline_frame = ttk.LabelFrame(outer, text="Pipeline", padding=12, style="Section.TLabelframe")
        pipeline_frame.grid(row=workflow_card_row + 1, column=0, sticky="nsew", pady=(6, 0))
        pipeline_frame.columnconfigure(0, weight=1)

        prow = 0
        for group_name, stages in _PIPELINE_GROUPS:
            group_card = ttk.LabelFrame(
                pipeline_frame,
                text=group_name,
                padding=8,
                style="Section.TLabelframe",
            )
            group_card.grid(row=prow, column=0, sticky="ew", pady=(8 if prow > 0 else 0, 0))
            group_card.columnconfigure(1, weight=1)
            ttk.Label(
                group_card,
                text=f"{len(stages)} stage{'s' if len(stages) != 1 else ''} in this workflow group",
                style="Muted.TLabel",
            ).grid(row=0, column=0, columnspan=2, sticky="w", pady=(0, 6))
            grow = 1

            for stage_key, display_name in stages:
                btn_text = f"  {display_name}"
                if stage_key == "ingest":
                    cmd = self._spawn_ingest
                else:
                    def cmd(s: str = stage_key) -> None:
                        self._spawn_stage(s)

                button = ttk.Button(
                    group_card,
                    text=btn_text,
                    width=28,
                    style="Accent.TButton" if stage_key in {"ingest", "extract", "build-release", "build-custom-training-set"} else "TButton",
                    command=cmd,
                )
                button.grid(row=grow, column=0, sticky="ew", pady=3, padx=(0, 10))
                self._action_buttons.append(button)
                self._register_action_target(
                    f"stage.{stage_key}",
                    button,
                    scroll_hint="Use the right-side pipeline panel.",
                )

                lbl = tk.Label(
                    group_card,
                    textvariable=self._status_vars[stage_key],
                    width=10,
                    anchor="w",
                    fg=_STATUS_COLORS["idle"],
                    bg=_CARD_BG,
                    font=("Segoe UI", 9),
                )
                lbl.grid(row=grow, column=1, sticky="w")
                self._status_labels[stage_key] = lbl
                grow += 1
            prow += 1

        # Separator before Run All
        ttk.Separator(pipeline_frame, orient="horizontal").grid(
            row=prow, column=0, sticky="ew", pady=(10, 8),
        )
        prow += 1

        btn_frame = ttk.Frame(pipeline_frame)
        btn_frame.grid(row=prow, column=0, sticky="ew")
        btn_frame.columnconfigure(0, weight=1)
        btn_frame.columnconfigure(1, weight=1)

        run_full_btn = ttk.Button(
            btn_frame,
            text="Run Full Pipeline",
            style="Accent.TButton",
            command=self._spawn_all,
        )
        run_full_btn.grid(row=0, column=0, sticky="ew", padx=(0, 4))
        self._action_buttons.append(run_full_btn)
        self._register_action_target("pipeline.run_full", run_full_btn, scroll_hint="Use the right-side pipeline panel just above the stage groups.")

        refresh_btn = ttk.Button(
            btn_frame,
            text="Refresh Overview",
            command=self._refresh_overview,
        )
        refresh_btn.grid(row=0, column=1, sticky="ew", padx=(4, 0))
        self._action_buttons.append(refresh_btn)
        self._bind_canvas_mousewheel(canvas, outer)

    def _build_overview(self, parent: tk.Frame) -> None:
        overview = ttk.LabelFrame(parent, text="Data Overview", padding=10, style="Section.TLabelframe")
        overview.grid(row=0, column=0, sticky="ew")
        for column in range(4):
            overview.columnconfigure(column, weight=1)

        ttk.Label(
            overview,
            text="Recommended flow: configure sources, build a clean dataset slice, review release health, then move into features, training, and demo exports.",
            font=("Helvetica", 8),
            foreground="#666666",
        ).grid(row=0, column=0, columnspan=4, sticky="w", pady=(0, 6))

        items = [
            ("raw_rcsb", "Raw RCSB entries"),
            ("raw_skempi", "SKEMPI CSV"),
            ("processed", "Processed records"),
            ("processed_valid", "Processed valid"),
            ("extracted", "Extracted entries"),
            ("chains", "Chains"),
            ("bound_objects", "Bound objects"),
            ("assays", "Assay records"),
            ("graph_nodes", "Graph nodes"),
            ("graph_edges", "Graph edges"),
            ("splits", "Split files"),
            ("processed_issues", "Processed issues"),
        ]
        for key, _label in items:
            self._overview_vars[key] = tk.StringVar(value="--")

        card_row = self._add_metric_cards(
            overview,
            row=1,
            specs=items,
            value_vars=self._overview_vars,
            columns=4,
            value_wraplength=150,
            pady=(0, 4),
        )

        presenter_frame = ttk.LabelFrame(overview, text="Presenter Banner", padding=8, style="Section.TLabelframe")
        presenter_frame.grid(row=card_row, column=0, columnspan=4, sticky="ew", pady=(8, 0))
        self._overview_sections["presenter_banner"] = presenter_frame
        presenter_frame.columnconfigure(1, weight=1)
        for row, (key, label) in enumerate([
            ("headline", "Headline:"),
            ("subhead", "Story:"),
            ("state", "Workspace state:"),
            ("next_step", "Open with:"),
        ]):
            ttk.Label(presenter_frame, text=label, font=("Segoe UI Semibold", 8)).grid(
                row=row, column=0, sticky="nw", padx=(0, 6), pady=1
            )
            ttk.Label(
                presenter_frame,
                textvariable=self._presenter_banner_vars[key],
                wraplength=860,
                justify="left",
                font=("Segoe UI", 8),
            ).grid(row=row, column=1, sticky="w", pady=1)

        completion_frame = ttk.LabelFrame(overview, text="Completion Status", padding=8, style="Section.TLabelframe")
        completion_frame.grid(row=card_row + 1, column=0, columnspan=4, sticky="ew", pady=(8, 0))
        self._overview_sections["completion"] = completion_frame
        for column in range(5):
            completion_frame.columnconfigure(column, weight=1 if column else 0)

        summary_items = [
            ("status", "Overall status:"),
            ("headline", "Headline:"),
            ("detail", "Progress detail:"),
            ("next_action", "Priority next step:"),
        ]
        for row, (key, label) in enumerate(summary_items):
            ttk.Label(completion_frame, text=label, font=("Segoe UI Semibold", 8)).grid(
                row=row, column=0, sticky="nw", padx=(0, 6), pady=1
            )
            ttk.Label(
                completion_frame,
                textvariable=self._completion_summary_vars[key],
                wraplength=860,
                justify="left",
                font=("Segoe UI", 8),
            ).grid(row=row, column=1, columnspan=4, sticky="w", pady=1)

        header_row = len(summary_items)
        for column, text in enumerate(["Area", "Current state", "Target", "Gap to close", "Status"]):
            ttk.Label(completion_frame, text=text, font=("Segoe UI Semibold", 8)).grid(
                row=header_row, column=column, sticky="w", padx=(0, 8), pady=(8, 2)
            )

        for index, row_vars in enumerate(self._completion_row_vars, start=header_row + 1):
            for column, key in enumerate(["area", "current", "target", "gap", "status"]):
                if key == "status":
                    label = tk.Label(
                        completion_frame,
                        textvariable=row_vars[key],
                        bg=_CARD_BG,
                        fg=_MUTED_FG,
                        justify="left",
                        font=("Segoe UI Semibold", 8),
                    )
                    label.grid(row=index, column=column, sticky="nw", padx=(0, 8), pady=2)
                    self._completion_status_labels.append(label)
                    continue
                ttk.Label(
                    completion_frame,
                    textvariable=row_vars[key],
                    wraplength=190,
                    justify="left",
                    font=("Segoe UI", 8),
                ).grid(row=index, column=column, sticky="nw", padx=(0, 8), pady=2)

        review_frame = ttk.LabelFrame(overview, text="Root Review Exports", padding=8, style="Section.TLabelframe")
        review_frame.grid(row=8, column=0, columnspan=4, sticky="ew", pady=(8, 0))
        self._overview_sections["review_exports"] = review_frame
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
        release_frame.grid(row=9, column=0, columnspan=4, sticky="ew", pady=(8, 0))
        self._overview_sections["release_artifacts"] = release_frame
        release_frame.columnconfigure(1, weight=1)

        release_items = [
            ("model_ready_pairs_csv", "Model-ready CSV:"),
            ("custom_training_set_csv", "Custom training CSV:"),
            ("custom_training_exclusions_csv", "Custom training exclusions:"),
            ("custom_training_summary_json", "Custom training summary:"),
            ("custom_training_scorecard_json", "Training scorecard:"),
            ("custom_training_split_benchmark_csv", "Split benchmark:"),
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

        training_frame = ttk.LabelFrame(overview, text="Training Set Builder", padding=8, style="Section.TLabelframe")
        training_frame.grid(row=10, column=0, columnspan=4, sticky="ew", pady=(8, 0))
        self._overview_sections["training_builder"] = training_frame
        training_frame.columnconfigure(1, weight=1)
        training_items = [
            ("status", "Builder status:"),
            ("coverage", "Coverage snapshot:"),
            ("quality", "Quality snapshot:"),
            ("next_action", "Recommended next step:"),
        ]
        for r, (key, label) in enumerate(training_items):
            ttk.Label(
                training_frame,
                text=label,
                font=("Segoe UI Semibold", 8),
            ).grid(row=r, column=0, sticky="nw", padx=(0, 6), pady=1)
            ttk.Label(
                training_frame,
                textvariable=self._training_set_vars[key],
                wraplength=760,
                justify="left",
                font=("Segoe UI", 8),
            ).grid(row=r, column=1, sticky="w", pady=1)

        metrics_frame = ttk.Frame(training_frame)
        metrics_frame.grid(row=4, column=0, columnspan=2, sticky="ew", pady=(8, 0))
        for index in range(5):
            metrics_frame.columnconfigure(index, weight=1)
        metric_labels = [
            ("selected", "Selected"),
            ("clusters", "Clusters"),
            ("quality", "Mean quality"),
            ("dominance", "Max dominance"),
            ("excluded", "Excluded"),
        ]
        for index, (key, label) in enumerate(metric_labels):
            card = ttk.Frame(metrics_frame, style="Card.TFrame", padding=8)
            card.grid(row=0, column=index, sticky="ew", padx=(0 if index == 0 else 4, 0))
            ttk.Label(
                card,
                text=label,
                style="Muted.TLabel",
            ).grid(row=0, column=0, sticky="w")
            ttk.Label(
                card,
                textvariable=self._training_kpi_vars[key],
                font=("Segoe UI Semibold", 11),
            ).grid(row=1, column=0, sticky="w", pady=(2, 0))

        workflow_frame = ttk.Frame(training_frame)
        workflow_frame.grid(row=5, column=0, columnspan=2, sticky="ew", pady=(8, 0))
        for index in range(5):
            workflow_frame.columnconfigure(index, weight=1)
        workflow_items = [
            ("model_ready", "1. Model-ready"),
            ("custom_set", "2. Custom set"),
            ("scorecard", "3. Scorecard"),
            ("benchmark", "4. Benchmark"),
            ("release", "5. Release"),
        ]
        for index, (key, label) in enumerate(workflow_items):
            pill = ttk.Frame(workflow_frame, style="Card.TFrame", padding=6)
            pill.grid(row=0, column=index, sticky="ew", padx=(0 if index == 0 else 4, 0))
            ttk.Label(
                pill,
                text=label,
                style="Muted.TLabel",
            ).grid(row=0, column=0, sticky="w")
            ttk.Label(
                pill,
                textvariable=self._training_workflow_vars[key],
                font=("Segoe UI Semibold", 9),
            ).grid(row=1, column=0, sticky="w", pady=(2, 0))

        training_actions = ttk.Frame(training_frame)
        training_actions.grid(row=0, column=2, rowspan=6, sticky="ns", padx=(8, 0))
        for index in range(2):
            training_actions.columnconfigure(index, weight=1)
        run_training_btn = ttk.Button(
            training_actions,
            text="Run Training Set Workflow",
            style="Accent.TButton",
            command=self._spawn_training_set_workflow,
        )
        run_training_btn.grid(row=0, column=0, columnspan=2, sticky="ew", pady=(0, 6))
        self._action_buttons.append(run_training_btn)
        self._register_action_target(
            "training.run_workflow",
            run_training_btn,
            scroll_hint="Scroll through the right-side overview to the Training Set Builder actions.",
        )
        custom_set_btn = ttk.Button(
            training_actions,
            text="Build Custom Set",
            command=lambda: self._spawn_stage("build-custom-training-set"),
        )
        custom_set_btn.grid(row=1, column=0, sticky="ew", padx=(0, 4), pady=2)
        self._action_buttons.append(custom_set_btn)
        self._register_action_target(
            "training.build_custom_set",
            custom_set_btn,
            scroll_hint="Scroll through the right-side overview to the Training Set Builder actions.",
        )
        ttk.Button(
            training_actions,
            text="Open Benchmark",
            command=lambda: self._open_path(self._existing_review_path("custom_training_split_benchmark_csv")),
        ).grid(row=1, column=1, sticky="ew", padx=(4, 0), pady=2)
        ttk.Button(
            training_actions,
            text="Open Exclusions",
            command=lambda: self._open_path(self._existing_review_path("custom_training_exclusions_csv")),
        ).grid(row=2, column=0, sticky="ew", padx=(0, 4), pady=2)
        release_btn = ttk.Button(
            training_actions,
            text="Build Release",
            command=lambda: self._spawn_stage("build-release"),
        )
        release_btn.grid(row=2, column=1, sticky="ew", padx=(4, 0), pady=2)
        self._action_buttons.append(release_btn)

        deferred_host = ttk.Frame(overview)
        deferred_host.grid(row=11, column=0, columnspan=4, sticky="ew", pady=(8, 0))
        deferred_host.columnconfigure(0, weight=1)
        self._overview_deferred_host = deferred_host
        ttk.Label(
            deferred_host,
            text="Loading detailed overview panels after startup...",
            style="Muted.TLabel",
        ).grid(row=0, column=0, sticky="w")
        ttk.Button(
            deferred_host,
            text="Load Detailed Panels Now",
            command=self._build_overview_deferred_sections,
        ).grid(row=0, column=1, sticky="e", padx=(8, 0))
        self._root.after(150, self._build_overview_deferred_sections)
        self._apply_demo_mode()
        return

        quality_frame = ttk.LabelFrame(overview, text="Training Example Quality", padding=8, style="Section.TLabelframe")
        quality_frame.grid(row=11, column=0, columnspan=4, sticky="ew", pady=(8, 0))
        self._overview_sections["training_quality"] = quality_frame
        quality_frame.columnconfigure(1, weight=1)
        quality_items = [
            ("status", "Corpus status:"),
            ("coverage", "Coverage snapshot:"),
            ("quality", "Quality snapshot:"),
            ("next_action", "Recommended next step:"),
        ]
        for r, (key, label) in enumerate(quality_items):
            ttk.Label(
                quality_frame,
                text=label,
                font=("Segoe UI Semibold", 8),
            ).grid(row=r, column=0, sticky="nw", padx=(0, 6), pady=1)
            ttk.Label(
                quality_frame,
                textvariable=self._training_quality_vars[key],
                wraplength=760,
                justify="left",
                font=("Segoe UI", 8),
            ).grid(row=r, column=1, sticky="w", pady=1)
        quality_metrics = ttk.Frame(quality_frame)
        quality_metrics.grid(row=4, column=0, columnspan=2, sticky="ew", pady=(8, 0))
        for index in range(5):
            quality_metrics.columnconfigure(index, weight=1)
        quality_metric_labels = [
            ("examples", "Examples"),
            ("supervised", "Supervised"),
            ("targets", "Targets"),
            ("ligands", "Ligands"),
            ("conflicts", "Conflict rate"),
        ]
        for index, (key, label) in enumerate(quality_metric_labels):
            card = ttk.Frame(quality_metrics, style="Card.TFrame", padding=8)
            card.grid(row=0, column=index, sticky="ew", padx=(0 if index == 0 else 4, 0))
            ttk.Label(card, text=label, style="Muted.TLabel").grid(row=0, column=0, sticky="w")
            ttk.Label(
                card,
                textvariable=self._training_quality_kpi_vars[key],
                font=("Segoe UI Semibold", 11),
            ).grid(row=1, column=0, sticky="w", pady=(2, 0))
        ttk.Button(
            quality_frame,
            text="Export Quality Report",
            command=lambda: self._spawn_stage("report-training-set-quality"),
        ).grid(row=0, column=2, rowspan=len(quality_items), sticky="ns", padx=(8, 0))

        comparison_frame = ttk.LabelFrame(overview, text="Model Comparison", padding=8, style="Section.TLabelframe")
        comparison_frame.grid(row=12, column=0, columnspan=4, sticky="ew", pady=(8, 0))
        self._overview_sections["model_comparison"] = comparison_frame
        comparison_frame.columnconfigure(1, weight=1)
        comparison_items = [
            ("status", "Comparison status:"),
            ("summary", "Current summary:"),
            ("next_action", "Recommended next step:"),
        ]
        for r, (key, label) in enumerate(comparison_items):
            ttk.Label(
                comparison_frame,
                text=label,
                font=("Segoe UI Semibold", 8),
            ).grid(row=r, column=0, sticky="nw", padx=(0, 6), pady=1)
            ttk.Label(
                comparison_frame,
                textvariable=self._model_comparison_vars[key],
                wraplength=760,
                justify="left",
                font=("Segoe UI", 8),
            ).grid(row=r, column=1, sticky="w", pady=1)
        comparison_metrics = ttk.Frame(comparison_frame)
        comparison_metrics.grid(row=3, column=0, columnspan=2, sticky="ew", pady=(8, 0))
        for index in range(5):
            comparison_metrics.columnconfigure(index, weight=1)
        comparison_metric_labels = [
            ("baseline", "Baseline"),
            ("tabular", "Tabular"),
            ("val_winner", "Val winner"),
            ("test_winner", "Test winner"),
            ("val_gap", "Val gap"),
        ]
        for index, (key, label) in enumerate(comparison_metric_labels):
            card = ttk.Frame(comparison_metrics, style="Card.TFrame", padding=8)
            card.grid(row=0, column=index, sticky="ew", padx=(0 if index == 0 else 4, 0))
            ttk.Label(card, text=label, style="Muted.TLabel").grid(row=0, column=0, sticky="w")
            ttk.Label(
                card,
                textvariable=self._model_comparison_kpi_vars[key],
                font=("Segoe UI Semibold", 11),
            ).grid(row=1, column=0, sticky="w", pady=(2, 0))
        ttk.Button(
            comparison_frame,
            text="Export Model Comparison",
            command=lambda: self._spawn_stage("report-model-comparison"),
        ).grid(row=0, column=2, rowspan=len(comparison_items), sticky="ns", padx=(8, 0))

        split_diag_frame = ttk.LabelFrame(overview, text="Split Diagnostics", padding=8, style="Section.TLabelframe")
        split_diag_frame.grid(row=13, column=0, columnspan=4, sticky="ew", pady=(8, 0))
        self._overview_sections["split_diagnostics"] = split_diag_frame
        split_diag_frame.columnconfigure(1, weight=1)
        for r, (key, label) in enumerate([
            ("status", "Split status:"),
            ("summary", "Current summary:"),
            ("next_action", "Recommended next step:"),
        ]):
            ttk.Label(split_diag_frame, text=label, font=("Segoe UI Semibold", 8)).grid(
                row=r, column=0, sticky="nw", padx=(0, 6), pady=1
            )
            ttk.Label(
                split_diag_frame,
                textvariable=self._split_diagnostics_vars[key],
                wraplength=760,
                justify="left",
                font=("Segoe UI", 8),
            ).grid(row=r, column=1, sticky="w", pady=1)
        split_diag_metrics = ttk.Frame(split_diag_frame)
        split_diag_metrics.grid(row=3, column=0, columnspan=2, sticky="ew", pady=(8, 0))
        for index in range(5):
            split_diag_metrics.columnconfigure(index, weight=1)
        for index, (key, label) in enumerate([
            ("strategy", "Strategy"),
            ("held_out", "Held-out items"),
            ("hard_overlap", "Hard overlap"),
            ("family_overlap", "Family overlap"),
            ("source_overlap", "Source overlap"),
            ("fold_overlap", "Fold overlap"),
            ("dominance", "Max family share"),
        ]):
            card = ttk.Frame(split_diag_metrics, style="Card.TFrame", padding=8)
            card.grid(row=0, column=index, sticky="ew", padx=(0 if index == 0 else 4, 0))
            ttk.Label(card, text=label, style="Muted.TLabel").grid(row=0, column=0, sticky="w")
            ttk.Label(
                card,
                textvariable=self._split_diagnostics_kpi_vars[key],
                font=("Segoe UI Semibold", 11),
            ).grid(row=1, column=0, sticky="w", pady=(2, 0))
        ttk.Button(
            split_diag_frame,
            text="Open Split Diagnostics",
            command=lambda: self._open_path(self._storage_layout().splits_dir / "split_diagnostics.md"),
        ).grid(row=0, column=2, rowspan=3, sticky="ns", padx=(8, 0))

        search_preview_frame = ttk.LabelFrame(overview, text="Search Preview", padding=8, style="Section.TLabelframe")
        search_preview_frame.grid(row=14, column=0, columnspan=4, sticky="ew", pady=(8, 0))
        self._overview_sections["search_preview"] = search_preview_frame
        search_preview_frame.columnconfigure(1, weight=1)
        for r, (key, label) in enumerate([
            ("status", "Preview status:"),
            ("summary", "Current summary:"),
            ("next_action", "Recommended next step:"),
        ]):
            ttk.Label(search_preview_frame, text=label, font=("Segoe UI Semibold", 8)).grid(
                row=r, column=0, sticky="nw", padx=(0, 6), pady=1
            )
            ttk.Label(
                search_preview_frame,
                textvariable=self._search_preview_vars[key],
                wraplength=760,
                justify="left",
                font=("Segoe UI", 8),
            ).grid(row=r, column=1, sticky="w", pady=1)
        search_preview_metrics = ttk.Frame(search_preview_frame)
        search_preview_metrics.grid(row=3, column=0, columnspan=2, sticky="ew", pady=(8, 0))
        for index in range(4):
            search_preview_metrics.columnconfigure(index, weight=1)
        for index, (key, label) in enumerate([
            ("total", "Total matches"),
            ("selected", "Selected"),
            ("sample", "Preview sample"),
            ("mode", "Selection mode"),
        ]):
            card = ttk.Frame(search_preview_metrics, style="Card.TFrame", padding=8)
            card.grid(row=0, column=index, sticky="ew", padx=(0 if index == 0 else 4, 0))
            ttk.Label(card, text=label, style="Muted.TLabel").grid(row=0, column=0, sticky="w")
            ttk.Label(card, textvariable=self._search_preview_kpi_vars[key], font=("Segoe UI Semibold", 11)).grid(
                row=1, column=0, sticky="w", pady=(2, 0)
            )
        ttk.Button(
            search_preview_frame,
            text="Preview RCSB Search",
            command=self._preview_rcsb_search,
        ).grid(row=0, column=2, rowspan=3, sticky="ns", padx=(8, 0))

        source_config_frame = ttk.LabelFrame(overview, text="Source Configuration", padding=8, style="Section.TLabelframe")
        source_config_frame.grid(row=15, column=0, columnspan=4, sticky="ew", pady=(8, 0))
        self._overview_sections["source_configuration"] = source_config_frame
        source_config_frame.columnconfigure(1, weight=1)
        for r, (key, label) in enumerate([
            ("status", "Configuration status:"),
            ("summary", "Current source surface:"),
            ("next_action", "Recommended next step:"),
        ]):
            ttk.Label(
                source_config_frame,
                text=label,
                font=("Segoe UI Semibold", 8),
            ).grid(row=r, column=0, sticky="nw", padx=(0, 6), pady=1)
            ttk.Label(
                source_config_frame,
                textvariable=self._source_configuration_vars[key],
                wraplength=760,
                justify="left",
                font=("Segoe UI", 8),
            ).grid(row=r, column=1, sticky="w", pady=1)
        source_config_metrics = ttk.Frame(source_config_frame)
        source_config_metrics.grid(row=3, column=0, columnspan=2, sticky="ew", pady=(8, 0))
        for index in range(4):
            source_config_metrics.columnconfigure(index, weight=1)
        for index, (key, label) in enumerate([
            ("enabled", "Enabled"),
            ("implemented", "Implemented"),
            ("planned", "Planned"),
            ("misconfigured", "Needs path"),
        ]):
            card = ttk.Frame(source_config_metrics, style="Card.TFrame", padding=8)
            card.grid(row=0, column=index, sticky="ew", padx=(0 if index == 0 else 4, 0))
            ttk.Label(card, text=label, style="Muted.TLabel").grid(row=0, column=0, sticky="w")
            ttk.Label(
                card,
                textvariable=self._source_configuration_kpi_vars[key],
                font=("Segoe UI Semibold", 11),
            ).grid(row=1, column=0, sticky="w", pady=(2, 0))
        ttk.Button(
            source_config_frame,
            text="Export Source Capabilities",
            command=lambda: self._spawn_stage("report-source-capabilities"),
        ).grid(row=0, column=2, rowspan=3, sticky="ns", padx=(8, 0))

        source_frame = ttk.LabelFrame(overview, text="Source Activity", padding=8, style="Section.TLabelframe")
        source_frame.grid(row=16, column=0, columnspan=4, sticky="ew", pady=(8, 0))
        self._overview_sections["source_activity"] = source_frame
        source_frame.columnconfigure(1, weight=1)
        for r, (key, label) in enumerate([
            ("status", "Run status:"),
            ("summary", "Current summary:"),
            ("next_action", "Recommended next step:"),
        ]):
            ttk.Label(
                source_frame,
                text=label,
                font=("Segoe UI Semibold", 8),
            ).grid(row=r, column=0, sticky="nw", padx=(0, 6), pady=1)
            ttk.Label(
                source_frame,
                textvariable=self._source_run_vars[key],
                wraplength=760,
                justify="left",
                font=("Segoe UI", 8),
            ).grid(row=r, column=1, sticky="w", pady=1)
        source_metrics = ttk.Frame(source_frame)
        source_metrics.grid(row=3, column=0, columnspan=2, sticky="ew", pady=(8, 0))
        for index in range(4):
            source_metrics.columnconfigure(index, weight=1)
        for index, (key, label) in enumerate([
            ("sources", "Sources touched"),
            ("attempts", "Attempts"),
            ("records", "Records observed"),
            ("mode", "Dominant mode"),
        ]):
            card = ttk.Frame(source_metrics, style="Card.TFrame", padding=8)
            card.grid(row=0, column=index, sticky="ew", padx=(0 if index == 0 else 4, 0))
            ttk.Label(card, text=label, style="Muted.TLabel").grid(row=0, column=0, sticky="w")
            ttk.Label(
                card,
                textvariable=self._source_run_kpi_vars[key],
                font=("Segoe UI Semibold", 11),
            ).grid(row=1, column=0, sticky="w", pady=(2, 0))
        ttk.Button(
            source_frame,
            text="Open Source Summary",
            command=lambda: self._open_path(self._storage_layout().reports_dir / "extract_source_run_summary.md"),
        ).grid(row=0, column=2, rowspan=3, sticky="ns", padx=(8, 0))

        data_integrity_frame = ttk.LabelFrame(overview, text="Data Integrity", padding=8, style="Section.TLabelframe")
        data_integrity_frame.grid(row=17, column=0, columnspan=4, sticky="ew", pady=(8, 0))
        self._overview_sections["data_integrity"] = data_integrity_frame
        data_integrity_frame.columnconfigure(1, weight=1)
        for r, (key, label) in enumerate([
            ("status", "Integrity status:"),
            ("summary", "Current summary:"),
            ("detail", "What needs attention:"),
            ("next_action", "Recommended next step:"),
        ]):
            ttk.Label(
                data_integrity_frame,
                text=label,
                font=("Segoe UI Semibold", 8),
            ).grid(row=r, column=0, sticky="nw", padx=(0, 6), pady=1)
            ttk.Label(
                data_integrity_frame,
                textvariable=self._data_integrity_vars[key],
                wraplength=760,
                justify="left",
                font=("Segoe UI", 8),
            ).grid(row=r, column=1, sticky="w", pady=1)
        data_integrity_metrics = ttk.Frame(data_integrity_frame)
        data_integrity_metrics.grid(row=4, column=0, columnspan=2, sticky="ew", pady=(8, 0))
        for index in range(6):
            data_integrity_metrics.columnconfigure(index, weight=1)
        for index, (key, label) in enumerate([
            ("valid", "Valid"),
            ("issues", "Issues"),
            ("empty", "Empty"),
            ("corrupt", "Corrupt"),
            ("invalid", "Schema-invalid"),
            ("scan", "Last scan"),
        ]):
            card = ttk.Frame(data_integrity_metrics, style="Card.TFrame", padding=8)
            card.grid(row=0, column=index, sticky="ew", padx=(0 if index == 0 else 4, 0))
            ttk.Label(card, text=label, style="Muted.TLabel").grid(row=0, column=0, sticky="w")
            ttk.Label(
                card,
                textvariable=self._data_integrity_kpi_vars[key],
                font=("Segoe UI Semibold", 11),
                wraplength=160 if key == "scan" else 120,
                justify="left",
            ).grid(row=1, column=0, sticky="w", pady=(2, 0))
        ttk.Button(
            data_integrity_frame,
            text="Open Health Report",
            command=lambda: self._open_path(self._storage_layout().reports_dir / "processed_json_health.md"),
        ).grid(row=0, column=2, rowspan=2, sticky="ns", padx=(8, 0))
        ttk.Button(
            data_integrity_frame,
            text="Show Clean Command",
            command=lambda: self._log_line(
                "Run `pbdata clean --processed --delete` to remove empty, corrupt, or schema-invalid processed JSON files."
            ),
        ).grid(row=2, column=2, rowspan=2, sticky="ns", padx=(8, 0))

        active_ops_frame = ttk.LabelFrame(overview, text="Active Operations", padding=8, style="Section.TLabelframe")
        active_ops_frame.grid(row=18, column=0, columnspan=4, sticky="ew", pady=(8, 0))
        self._overview_sections["active_operations"] = active_ops_frame
        active_ops_frame.columnconfigure(1, weight=1)
        for r, (key, label) in enumerate([
            ("status", "Operational status:"),
            ("summary", "Current summary:"),
            ("active_detail", "Lock detail:"),
            ("failed_detail", "Failure detail:"),
            ("latest_detail", "Latest manifest:"),
            ("next_action", "Recommended next step:"),
        ]):
            ttk.Label(
                active_ops_frame,
                text=label,
                font=("Segoe UI Semibold", 8),
            ).grid(row=r, column=0, sticky="nw", padx=(0, 6), pady=1)
            ttk.Label(
                active_ops_frame,
                textvariable=self._active_operations_vars[key],
                wraplength=760,
                justify="left",
                font=("Segoe UI", 8),
            ).grid(row=r, column=1, sticky="w", pady=1)
        active_ops_metrics = ttk.Frame(active_ops_frame)
        active_ops_metrics.grid(row=6, column=0, columnspan=2, sticky="ew", pady=(8, 0))
        for index in range(5):
            active_ops_metrics.columnconfigure(index, weight=1)
        for index, (key, label) in enumerate([
            ("active", "Active locks"),
            ("running", "Running states"),
            ("failed", "Failed states"),
            ("stale", "Stale locks"),
            ("latest", "Latest stage"),
        ]):
            card = ttk.Frame(active_ops_metrics, style="Card.TFrame", padding=8)
            card.grid(row=0, column=index, sticky="ew", padx=(0 if index == 0 else 4, 0))
            ttk.Label(card, text=label, style="Muted.TLabel").grid(row=0, column=0, sticky="w")
            ttk.Label(
                card,
                textvariable=self._active_operations_kpi_vars[key],
                font=("Segoe UI Semibold", 11),
            ).grid(row=1, column=0, sticky="w", pady=(2, 0))
        ttk.Button(
            active_ops_frame,
            text="Open Stage State Folder",
            command=lambda: self._open_path(self._storage_layout().stage_state_dir),
        ).grid(row=0, column=2, rowspan=6, sticky="ns", padx=(8, 0))

        identity_frame = ttk.LabelFrame(overview, text="Identity Crosswalk", padding=8, style="Section.TLabelframe")
        identity_frame.grid(row=19, column=0, columnspan=4, sticky="ew", pady=(8, 0))
        self._overview_sections["identity_crosswalk"] = identity_frame
        identity_frame.columnconfigure(1, weight=1)
        for r, (key, label) in enumerate([
            ("status", "Crosswalk status:"),
            ("summary", "Current summary:"),
            ("next_action", "Recommended next step:"),
        ]):
            ttk.Label(
                identity_frame,
                text=label,
                font=("Segoe UI Semibold", 8),
            ).grid(row=r, column=0, sticky="nw", padx=(0, 6), pady=1)
            ttk.Label(
                identity_frame,
                textvariable=self._identity_crosswalk_vars[key],
                wraplength=760,
                justify="left",
                font=("Segoe UI", 8),
            ).grid(row=r, column=1, sticky="w", pady=1)
        identity_metrics = ttk.Frame(identity_frame)
        identity_metrics.grid(row=3, column=0, columnspan=2, sticky="ew", pady=(8, 0))
        for index in range(4):
            identity_metrics.columnconfigure(index, weight=1)
        for index, (key, label) in enumerate([
            ("proteins", "Protein IDs"),
            ("ligands", "Ligand IDs"),
            ("pairs", "Pair IDs"),
            ("fallbacks", "Fallbacks"),
        ]):
            card = ttk.Frame(identity_metrics, style="Card.TFrame", padding=8)
            card.grid(row=0, column=index, sticky="ew", padx=(0 if index == 0 else 4, 0))
            ttk.Label(card, text=label, style="Muted.TLabel").grid(row=0, column=0, sticky="w")
            ttk.Label(
                card,
                textvariable=self._identity_crosswalk_kpi_vars[key],
                font=("Segoe UI Semibold", 11),
            ).grid(row=1, column=0, sticky="w", pady=(2, 0))
        ttk.Button(
            identity_frame,
            text="Export Identity Crosswalk",
            command=lambda: self._spawn_stage("export-identity-crosswalk"),
        ).grid(row=0, column=2, rowspan=3, sticky="ns", padx=(8, 0))

        release_frame = ttk.LabelFrame(overview, text="Release Readiness", padding=8, style="Section.TLabelframe")
        release_frame.grid(row=20, column=0, columnspan=4, sticky="ew", pady=(8, 0))
        self._overview_sections["release_readiness"] = release_frame
        release_frame.columnconfigure(1, weight=1)
        for r, (key, label) in enumerate([
            ("status", "Release status:"),
            ("summary", "Current summary:"),
            ("next_action", "Recommended next step:"),
        ]):
            ttk.Label(release_frame, text=label, font=("Segoe UI Semibold", 8)).grid(
                row=r, column=0, sticky="nw", padx=(0, 6), pady=1
            )
            ttk.Label(
                release_frame,
                textvariable=self._release_readiness_vars[key],
                wraplength=760,
                justify="left",
                font=("Segoe UI", 8),
            ).grid(row=r, column=1, sticky="w", pady=1)
        release_metrics = ttk.Frame(release_frame)
        release_metrics.grid(row=3, column=0, columnspan=2, sticky="ew", pady=(8, 0))
        for index in range(4):
            release_metrics.columnconfigure(index, weight=1)
        for index, (key, label) in enumerate([
            ("entries", "Canonical entries"),
            ("pairs", "Canonical pairs"),
            ("model_ready", "Model-ready"),
            ("held_out", "Held-out"),
            ("blockers", "Blockers"),
        ]):
            card = ttk.Frame(release_metrics, style="Card.TFrame", padding=8)
            card.grid(row=0, column=index, sticky="ew", padx=(0 if index == 0 else 4, 0))
            ttk.Label(card, text=label, style="Muted.TLabel").grid(row=0, column=0, sticky="w")
            ttk.Label(card, textvariable=self._release_readiness_kpi_vars[key], font=("Segoe UI Semibold", 11)).grid(
                row=1, column=0, sticky="w", pady=(2, 0)
            )
        ttk.Button(
            release_frame,
            text="Run Release Check",
            command=lambda: self._spawn_stage("release-check"),
        ).grid(row=0, column=2, rowspan=3, sticky="ns", padx=(8, 0))

        risk_frame = ttk.LabelFrame(overview, text="Pathway Risk Context", padding=8, style="Section.TLabelframe")
        risk_frame.grid(row=21, column=0, columnspan=4, sticky="ew", pady=(8, 0))
        self._overview_sections["risk_context"] = risk_frame
        risk_frame.columnconfigure(1, weight=1)
        for r, (key, label) in enumerate([
            ("status", "Context status:"),
            ("summary", "Interpretation:"),
            ("next_action", "Recommended next step:"),
        ]):
            ttk.Label(
                risk_frame,
                text=label,
                font=("Segoe UI Semibold", 8),
            ).grid(row=r, column=0, sticky="nw", padx=(0, 6), pady=1)
            ttk.Label(
                risk_frame,
                textvariable=self._risk_vars[key],
                wraplength=760,
                justify="left",
                font=("Segoe UI", 8),
            ).grid(row=r, column=1, sticky="w", pady=1)
        risk_metrics = ttk.Frame(risk_frame)
        risk_metrics.grid(row=3, column=0, columnspan=2, sticky="ew", pady=(8, 0))
        for index in range(4):
            risk_metrics.columnconfigure(index, weight=1)
        for index, (key, label) in enumerate([
            ("severity", "Severity"),
            ("score", "Risk score"),
            ("matches", "Matched pairs"),
            ("pathways", "Pathway overlap"),
        ]):
            card = ttk.Frame(risk_metrics, style="Card.TFrame", padding=8)
            card.grid(row=0, column=index, sticky="ew", padx=(0 if index == 0 else 4, 0))
            ttk.Label(card, text=label, style="Muted.TLabel").grid(row=0, column=0, sticky="w")
            ttk.Label(
                card,
                textvariable=self._risk_kpi_vars[key],
                font=("Segoe UI Semibold", 11),
            ).grid(row=1, column=0, sticky="w", pady=(2, 0))
        ttk.Button(
            risk_frame,
            text="Open Risk Summary",
            command=lambda: self._open_path(self._storage_layout().risk_dir / "pathway_risk_summary.json"),
        ).grid(row=0, column=2, rowspan=3, sticky="ns", padx=(8, 0))

        prediction_frame = ttk.LabelFrame(overview, text="Prediction Status", padding=8, style="Section.TLabelframe")
        prediction_frame.grid(row=22, column=0, columnspan=4, sticky="ew", pady=(8, 0))
        self._overview_sections["prediction_status"] = prediction_frame
        prediction_frame.columnconfigure(1, weight=1)
        prediction_items = [
            ("status", "Prediction status:"),
            ("method", "Selected model:"),
            ("preference", "Current preference:"),
            ("summary", "Interpretation:"),
        ]
        for r, (key, label) in enumerate(prediction_items):
            ttk.Label(
                prediction_frame,
                text=label,
                font=("Segoe UI Semibold", 8),
            ).grid(row=r, column=0, sticky="nw", padx=(0, 6), pady=1)
            ttk.Label(
                prediction_frame,
                textvariable=self._prediction_status_vars[key],
                wraplength=760,
                justify="left",
                font=("Segoe UI", 8),
            ).grid(row=r, column=1, sticky="w", pady=1)
        prediction_metrics = ttk.Frame(prediction_frame)
        prediction_metrics.grid(row=4, column=0, columnspan=2, sticky="ew", pady=(8, 0))
        for index in range(4):
            prediction_metrics.columnconfigure(index, weight=1)
        for index, (key, label) in enumerate([
            ("targets", "Targets"),
            ("top_target", "Top target"),
            ("confidence", "Confidence"),
            ("query_features", "Query features"),
        ]):
            card = ttk.Frame(prediction_metrics, style="Card.TFrame", padding=8)
            card.grid(row=0, column=index, sticky="ew", padx=(0 if index == 0 else 4, 0))
            ttk.Label(card, text=label, style="Muted.TLabel").grid(row=0, column=0, sticky="w")
            ttk.Label(
                card,
                textvariable=self._prediction_status_kpi_vars[key],
                font=("Segoe UI Semibold", 11),
            ).grid(row=1, column=0, sticky="w", pady=(2, 0))

        workflow_frame = ttk.LabelFrame(overview, text="Recommended Workflow", padding=8, style="Section.TLabelframe")
        workflow_frame.grid(row=23, column=0, columnspan=4, sticky="ew", pady=(8, 0))
        self._overview_sections["workflow_guidance"] = workflow_frame
        workflow_frame.columnconfigure(1, weight=1)
        workflow_items = [
            ("phase", "Current phase:"),
            ("summary", "Why this is next:"),
            ("step_1", "Step 1:"),
            ("step_2", "Step 2:"),
            ("step_3", "Step 3:"),
        ]
        for r, (key, label) in enumerate(workflow_items):
            ttk.Label(
                workflow_frame,
                text=label,
                font=("Segoe UI Semibold", 8),
            ).grid(row=r, column=0, sticky="nw", padx=(0, 6), pady=1)
            ttk.Label(
                workflow_frame,
                textvariable=self._workflow_guidance_vars[key],
                wraplength=760,
                justify="left",
                font=("Segoe UI", 8),
            ).grid(row=r, column=1, sticky="w", pady=1)

        curation_frame = ttk.LabelFrame(overview, text="Curation Review", padding=8, style="Section.TLabelframe")
        curation_frame.grid(row=24, column=0, columnspan=4, sticky="ew", pady=(8, 0))
        self._overview_sections["curation_review"] = curation_frame
        curation_frame.columnconfigure(1, weight=1)
        curation_items = [
            ("exclusions", "Exclusion review:"),
            ("conflicts", "Conflict review:"),
            ("issues", "Issue review:"),
            ("next_action", "Recommended next step:"),
        ]
        for r, (key, label) in enumerate(curation_items):
            ttk.Label(
                curation_frame,
                text=label,
                font=("Segoe UI Semibold", 8),
            ).grid(row=r, column=0, sticky="nw", padx=(0, 6), pady=1)
            ttk.Label(
                curation_frame,
                textvariable=self._curation_review_vars[key],
                wraplength=760,
                justify="left",
                font=("Segoe UI", 8),
            ).grid(row=r, column=1, sticky="w", pady=1)
        curation_actions = ttk.Frame(curation_frame)
        curation_actions.grid(row=0, column=2, rowspan=len(curation_items), sticky="ns", padx=(8, 0))
        for index in range(2):
            curation_actions.columnconfigure(index, weight=1)
        ttk.Button(
            curation_actions,
            text="Refresh Filtered Review",
            command=self._refresh_filtered_review_csv,
        ).grid(row=0, column=0, columnspan=2, sticky="ew", pady=(0, 6))
        ttk.Button(
            curation_actions,
            text="Open Issues",
            command=lambda: self._open_path(self._existing_review_path("issue_csv")),
        ).grid(row=1, column=0, sticky="ew", padx=(0, 4), pady=2)
        ttk.Button(
            curation_actions,
            text="Open Conflicts",
            command=lambda: self._open_path(self._existing_review_path("conflict_csv")),
        ).grid(row=1, column=1, sticky="ew", padx=(4, 0), pady=2)

        demo_frame = ttk.LabelFrame(overview, text="Demo Readiness", padding=8, style="Section.TLabelframe")
        demo_frame.grid(row=25, column=0, columnspan=4, sticky="ew", pady=(8, 0))
        self._overview_sections["demo_readiness"] = demo_frame
        demo_frame.columnconfigure(1, weight=1)
        demo_items = [
            ("readiness", "Current state:"),
            ("summary", "Summary:"),
            ("customer_message", "Customer-facing note:"),
            ("walkthrough", "Presenter flow:"),
            ("blockers", "Blockers:"),
            ("warnings", "Warnings:"),
        ]
        for r, (key, label) in enumerate(demo_items):
            ttk.Label(
                demo_frame,
                text=label,
                font=("Segoe UI Semibold", 8),
            ).grid(row=r, column=0, sticky="nw", padx=(0, 6), pady=1)
            ttk.Label(
                demo_frame,
                textvariable=self._demo_readiness_vars[key],
                wraplength=760,
                justify="left",
                font=("Segoe UI", 8),
            ).grid(row=r, column=1, sticky="w", pady=1)
        demo_actions = ttk.Frame(demo_frame)
        demo_actions.grid(row=0, column=2, rowspan=len(demo_items), sticky="ns", padx=(8, 0))
        ttk.Button(
            demo_actions,
            text="Export Demo Snapshot",
            command=lambda: self._spawn_stage("export-demo-snapshot"),
        ).grid(row=0, column=0, sticky="ew", pady=(0, 6))
        ttk.Button(
            demo_actions,
            text="Open Demo Walkthrough",
            command=lambda: self._open_path(self._storage_layout().feature_reports_dir / "demo_walkthrough.md"),
        ).grid(row=1, column=0, sticky="ew")

        health_frame = ttk.LabelFrame(overview, text="Review Health", padding=8, style="Section.TLabelframe")
        health_frame.grid(row=26, column=0, columnspan=4, sticky="ew", pady=(8, 0))
        self._overview_sections["review_health"] = health_frame
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
        help_frame.grid(row=27, column=0, columnspan=4, sticky="ew", pady=(8, 0))
        self._overview_sections["interpretation_guide"] = help_frame
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
        actions_frame.grid(row=28, column=0, columnspan=4, sticky="ew", pady=(8, 0))
        self._overview_sections["quick_actions"] = actions_frame
        for index in range(7):
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
            text="Open Split Benchmark",
            command=lambda: self._open_path(self._existing_review_path("custom_training_split_benchmark_csv")),
        ).grid(row=0, column=3, sticky="ew", padx=4)
        ttk.Button(
            actions_frame,
            text="Open Training Scorecard",
            command=lambda: self._open_path(self._existing_review_path("custom_training_scorecard_json")),
        ).grid(row=0, column=4, sticky="ew", padx=4)
        ttk.Button(
            actions_frame,
            text="Open Coverage Summary",
            command=lambda: self._open_path(self._existing_review_path("scientific_coverage_json")),
        ).grid(row=0, column=5, sticky="ew", padx=4)
        ttk.Button(
            actions_frame,
            text="Open Storage Root",
            command=lambda: self._open_path(self._storage_layout().root),
        ).grid(row=0, column=6, sticky="ew", padx=(4, 0))

        last_run_frame = ttk.LabelFrame(overview, text="Last Workflow Run", padding=8, style="Section.TLabelframe")
        last_run_frame.grid(row=29, column=0, columnspan=4, sticky="ew", pady=(8, 0))
        self._overview_sections["last_run"] = last_run_frame
        last_run_frame.columnconfigure(1, weight=1)
        for row, (key, label) in enumerate([
            ("status", "Run state:"),
            ("summary", "Summary:"),
            ("last_stage", "Last stage:"),
            ("last_result", "Last result:"),
            ("next_action", "Next step:"),
        ]):
            ttk.Label(last_run_frame, text=label, font=("Segoe UI Semibold", 8)).grid(
                row=row, column=0, sticky="nw", padx=(0, 6), pady=1
            )
            ttk.Label(
                last_run_frame,
                textvariable=self._last_run_vars[key],
                wraplength=860,
                justify="left",
                font=("Segoe UI", 8),
            ).grid(row=row, column=1, sticky="w", pady=1)

        freshness_frame = ttk.LabelFrame(overview, text="Artifact Freshness", padding=8, style="Section.TLabelframe")
        freshness_frame.grid(row=30, column=0, columnspan=4, sticky="ew", pady=(8, 0))
        self._overview_sections["artifact_freshness"] = freshness_frame
        freshness_frame.columnconfigure(1, weight=1)
        for row, (key, label) in enumerate([
            ("release_check", "Release check:"),
            ("demo_snapshot", "Demo snapshot:"),
            ("prediction_manifest", "Prediction manifest:"),
            ("risk_summary", "Risk summary:"),
            ("model_comparison", "Model comparison:"),
            ("training_quality", "Training quality:"),
            ("release_manifest", "Release manifest:"),
        ]):
            ttk.Label(freshness_frame, text=label, font=("Segoe UI Semibold", 8)).grid(
                row=row, column=0, sticky="nw", padx=(0, 6), pady=1
            )
            ttk.Label(
                freshness_frame,
                textvariable=self._artifact_freshness_vars[key],
                wraplength=860,
                justify="left",
                font=("Segoe UI", 8),
            ).grid(row=row, column=1, sticky="w", pady=1)
        self._apply_demo_mode()

    def _build_overview_deferred_sections(self) -> None:
        if not hasattr(self, "_overview_deferred_built"):
            self._overview_deferred_built = False
        if not hasattr(self, "_overview_deferred_host"):
            self._overview_deferred_host = None
        if self._overview_deferred_built or self._overview_deferred_host is None or self._closing:
            return
        host = self._overview_deferred_host
        for child in host.winfo_children():
            child.destroy()
        host.columnconfigure(0, weight=1)
        self._overview_deferred_built = True

        row = 0

        quality_frame = ttk.LabelFrame(host, text="Training Example Quality", padding=8, style="Section.TLabelframe")
        quality_frame.grid(row=row, column=0, sticky="ew")
        self._overview_sections["training_quality"] = quality_frame
        quality_frame.columnconfigure(1, weight=1)
        for r, (key, label) in enumerate([("status", "Corpus status:"), ("coverage", "Coverage snapshot:"), ("quality", "Quality snapshot:"), ("next_action", "Recommended next step:")]):
            ttk.Label(quality_frame, text=label, font=("Segoe UI Semibold", 8)).grid(row=r, column=0, sticky="nw", padx=(0, 6), pady=1)
            ttk.Label(quality_frame, textvariable=self._training_quality_vars[key], wraplength=760, justify="left", font=("Segoe UI", 8)).grid(row=r, column=1, sticky="w", pady=1)
        quality_metrics = ttk.Frame(quality_frame)
        quality_metrics.grid(row=4, column=0, columnspan=2, sticky="ew", pady=(8, 0))
        for index in range(5):
            quality_metrics.columnconfigure(index, weight=1)
        for index, (key, label) in enumerate([("examples", "Examples"), ("supervised", "Supervised"), ("targets", "Targets"), ("ligands", "Ligands"), ("conflicts", "Conflict rate")]):
            card = ttk.Frame(quality_metrics, style="Card.TFrame", padding=8)
            card.grid(row=0, column=index, sticky="ew", padx=(0 if index == 0 else 4, 0))
            ttk.Label(card, text=label, style="Muted.TLabel").grid(row=0, column=0, sticky="w")
            ttk.Label(card, textvariable=self._training_quality_kpi_vars[key], font=("Segoe UI Semibold", 11)).grid(row=1, column=0, sticky="w", pady=(2, 0))
        ttk.Button(quality_frame, text="Export Quality Report", command=lambda: self._spawn_stage("report-training-set-quality")).grid(row=0, column=2, rowspan=4, sticky="ns", padx=(8, 0))
        row += 1

        comparison_frame = ttk.LabelFrame(host, text="Model Comparison", padding=8, style="Section.TLabelframe")
        comparison_frame.grid(row=row, column=0, sticky="ew", pady=(8, 0))
        self._overview_sections["model_comparison"] = comparison_frame
        comparison_frame.columnconfigure(1, weight=1)
        for r, (key, label) in enumerate([("status", "Comparison status:"), ("summary", "Current summary:"), ("next_action", "Recommended next step:")]):
            ttk.Label(comparison_frame, text=label, font=("Segoe UI Semibold", 8)).grid(row=r, column=0, sticky="nw", padx=(0, 6), pady=1)
            ttk.Label(comparison_frame, textvariable=self._model_comparison_vars[key], wraplength=760, justify="left", font=("Segoe UI", 8)).grid(row=r, column=1, sticky="w", pady=1)
        comparison_metrics = ttk.Frame(comparison_frame)
        comparison_metrics.grid(row=3, column=0, columnspan=2, sticky="ew", pady=(8, 0))
        for index in range(5):
            comparison_metrics.columnconfigure(index, weight=1)
        for index, (key, label) in enumerate([("baseline", "Baseline"), ("tabular", "Tabular"), ("val_winner", "Val winner"), ("test_winner", "Test winner"), ("val_gap", "Val gap")]):
            card = ttk.Frame(comparison_metrics, style="Card.TFrame", padding=8)
            card.grid(row=0, column=index, sticky="ew", padx=(0 if index == 0 else 4, 0))
            ttk.Label(card, text=label, style="Muted.TLabel").grid(row=0, column=0, sticky="w")
            ttk.Label(card, textvariable=self._model_comparison_kpi_vars[key], font=("Segoe UI Semibold", 11)).grid(row=1, column=0, sticky="w", pady=(2, 0))
        ttk.Button(comparison_frame, text="Export Model Comparison", command=lambda: self._spawn_stage("report-model-comparison")).grid(row=0, column=2, rowspan=3, sticky="ns", padx=(8, 0))
        row += 1

        split_diag_frame = ttk.LabelFrame(host, text="Split Diagnostics", padding=8, style="Section.TLabelframe")
        split_diag_frame.grid(row=row, column=0, sticky="ew", pady=(8, 0))
        self._overview_sections["split_diagnostics"] = split_diag_frame
        split_diag_frame.columnconfigure(1, weight=1)
        for r, (key, label) in enumerate([("status", "Split status:"), ("summary", "Current summary:"), ("next_action", "Recommended next step:")]):
            ttk.Label(split_diag_frame, text=label, font=("Segoe UI Semibold", 8)).grid(row=r, column=0, sticky="nw", padx=(0, 6), pady=1)
            ttk.Label(split_diag_frame, textvariable=self._split_diagnostics_vars[key], wraplength=760, justify="left", font=("Segoe UI", 8)).grid(row=r, column=1, sticky="w", pady=1)
        split_diag_metrics = ttk.Frame(split_diag_frame)
        split_diag_metrics.grid(row=3, column=0, columnspan=2, sticky="ew", pady=(8, 0))
        for index in range(7):
            split_diag_metrics.columnconfigure(index, weight=1)
        for index, (key, label) in enumerate([("strategy", "Strategy"), ("held_out", "Held-out items"), ("hard_overlap", "Hard overlap"), ("family_overlap", "Family overlap"), ("source_overlap", "Source overlap"), ("fold_overlap", "Fold overlap"), ("dominance", "Max family share")]):
            card = ttk.Frame(split_diag_metrics, style="Card.TFrame", padding=8)
            card.grid(row=0, column=index, sticky="ew", padx=(0 if index == 0 else 4, 0))
            ttk.Label(card, text=label, style="Muted.TLabel").grid(row=0, column=0, sticky="w")
            ttk.Label(card, textvariable=self._split_diagnostics_kpi_vars[key], font=("Segoe UI Semibold", 11)).grid(row=1, column=0, sticky="w", pady=(2, 0))
        ttk.Button(split_diag_frame, text="Open Split Diagnostics", command=lambda: self._open_path(self._storage_layout().splits_dir / "split_diagnostics.md")).grid(row=0, column=2, rowspan=3, sticky="ns", padx=(8, 0))
        row += 1

        search_preview_frame = ttk.LabelFrame(host, text="Search Preview", padding=8, style="Section.TLabelframe")
        search_preview_frame.grid(row=row, column=0, sticky="ew", pady=(8, 0))
        self._overview_sections["search_preview"] = search_preview_frame
        search_preview_frame.columnconfigure(1, weight=1)
        for r, (key, label) in enumerate([("status", "Preview status:"), ("summary", "Current summary:"), ("next_action", "Recommended next step:")]):
            ttk.Label(search_preview_frame, text=label, font=("Segoe UI Semibold", 8)).grid(row=r, column=0, sticky="nw", padx=(0, 6), pady=1)
            ttk.Label(search_preview_frame, textvariable=self._search_preview_vars[key], wraplength=760, justify="left", font=("Segoe UI", 8)).grid(row=r, column=1, sticky="w", pady=1)
        search_preview_metrics = ttk.Frame(search_preview_frame)
        search_preview_metrics.grid(row=3, column=0, columnspan=2, sticky="ew", pady=(8, 0))
        for index in range(4):
            search_preview_metrics.columnconfigure(index, weight=1)
        for index, (key, label) in enumerate([("total", "Total matches"), ("selected", "Selected"), ("sample", "Preview sample"), ("mode", "Selection mode")]):
            card = ttk.Frame(search_preview_metrics, style="Card.TFrame", padding=8)
            card.grid(row=0, column=index, sticky="ew", padx=(0 if index == 0 else 4, 0))
            ttk.Label(card, text=label, style="Muted.TLabel").grid(row=0, column=0, sticky="w")
            ttk.Label(card, textvariable=self._search_preview_kpi_vars[key], font=("Segoe UI Semibold", 11)).grid(row=1, column=0, sticky="w", pady=(2, 0))
        ttk.Button(search_preview_frame, text="Preview RCSB Search", command=self._preview_rcsb_search).grid(row=0, column=2, rowspan=3, sticky="ns", padx=(8, 0))
        row += 1

        source_config_frame = ttk.LabelFrame(host, text="Source Configuration", padding=8, style="Section.TLabelframe")
        source_config_frame.grid(row=row, column=0, sticky="ew", pady=(8, 0))
        self._overview_sections["source_configuration"] = source_config_frame
        source_config_frame.columnconfigure(1, weight=1)
        for r, (key, label) in enumerate([("status", "Configuration status:"), ("summary", "Current source surface:"), ("next_action", "Recommended next step:")]):
            ttk.Label(source_config_frame, text=label, font=("Segoe UI Semibold", 8)).grid(row=r, column=0, sticky="nw", padx=(0, 6), pady=1)
            ttk.Label(source_config_frame, textvariable=self._source_configuration_vars[key], wraplength=760, justify="left", font=("Segoe UI", 8)).grid(row=r, column=1, sticky="w", pady=1)
        source_config_metrics = ttk.Frame(source_config_frame)
        source_config_metrics.grid(row=3, column=0, columnspan=2, sticky="ew", pady=(8, 0))
        for index in range(4):
            source_config_metrics.columnconfigure(index, weight=1)
        for index, (key, label) in enumerate([("enabled", "Enabled"), ("implemented", "Implemented"), ("planned", "Planned"), ("misconfigured", "Needs path")]):
            card = ttk.Frame(source_config_metrics, style="Card.TFrame", padding=8)
            card.grid(row=0, column=index, sticky="ew", padx=(0 if index == 0 else 4, 0))
            ttk.Label(card, text=label, style="Muted.TLabel").grid(row=0, column=0, sticky="w")
            ttk.Label(card, textvariable=self._source_configuration_kpi_vars[key], font=("Segoe UI Semibold", 11)).grid(row=1, column=0, sticky="w", pady=(2, 0))
        ttk.Button(source_config_frame, text="Export Source Capabilities", command=lambda: self._spawn_stage("report-source-capabilities")).grid(row=0, column=2, rowspan=3, sticky="ns", padx=(8, 0))
        row += 1

        source_frame = ttk.LabelFrame(host, text="Source Activity", padding=8, style="Section.TLabelframe")
        source_frame.grid(row=row, column=0, sticky="ew", pady=(8, 0))
        self._overview_sections["source_activity"] = source_frame
        source_frame.columnconfigure(1, weight=1)
        for r, (key, label) in enumerate([("status", "Run status:"), ("summary", "Current summary:"), ("next_action", "Recommended next step:")]):
            ttk.Label(source_frame, text=label, font=("Segoe UI Semibold", 8)).grid(row=r, column=0, sticky="nw", padx=(0, 6), pady=1)
            ttk.Label(source_frame, textvariable=self._source_run_vars[key], wraplength=760, justify="left", font=("Segoe UI", 8)).grid(row=r, column=1, sticky="w", pady=1)
        source_metrics = ttk.Frame(source_frame)
        source_metrics.grid(row=3, column=0, columnspan=2, sticky="ew", pady=(8, 0))
        for index in range(4):
            source_metrics.columnconfigure(index, weight=1)
        for index, (key, label) in enumerate([("sources", "Sources touched"), ("attempts", "Attempts"), ("records", "Records observed"), ("mode", "Dominant mode")]):
            card = ttk.Frame(source_metrics, style="Card.TFrame", padding=8)
            card.grid(row=0, column=index, sticky="ew", padx=(0 if index == 0 else 4, 0))
            ttk.Label(card, text=label, style="Muted.TLabel").grid(row=0, column=0, sticky="w")
            ttk.Label(card, textvariable=self._source_run_kpi_vars[key], font=("Segoe UI Semibold", 11)).grid(row=1, column=0, sticky="w", pady=(2, 0))
        ttk.Button(source_frame, text="Open Source Summary", command=lambda: self._open_path(self._storage_layout().reports_dir / "extract_source_run_summary.md")).grid(row=0, column=2, rowspan=3, sticky="ns", padx=(8, 0))
        row += 1

        data_integrity_frame = ttk.LabelFrame(host, text="Data Integrity", padding=8, style="Section.TLabelframe")
        data_integrity_frame.grid(row=row, column=0, sticky="ew", pady=(8, 0))
        self._overview_sections["data_integrity"] = data_integrity_frame
        data_integrity_frame.columnconfigure(1, weight=1)
        for r, (key, label) in enumerate([("status", "Integrity status:"), ("summary", "Current summary:"), ("detail", "What needs attention:"), ("next_action", "Recommended next step:")]):
            ttk.Label(data_integrity_frame, text=label, font=("Segoe UI Semibold", 8)).grid(row=r, column=0, sticky="nw", padx=(0, 6), pady=1)
            ttk.Label(data_integrity_frame, textvariable=self._data_integrity_vars[key], wraplength=760, justify="left", font=("Segoe UI", 8)).grid(row=r, column=1, sticky="w", pady=1)
        data_integrity_metrics = ttk.Frame(data_integrity_frame)
        data_integrity_metrics.grid(row=4, column=0, columnspan=2, sticky="ew", pady=(8, 0))
        for index in range(6):
            data_integrity_metrics.columnconfigure(index, weight=1)
        for index, (key, label) in enumerate([("valid", "Valid"), ("issues", "Issues"), ("empty", "Empty"), ("corrupt", "Corrupt"), ("invalid", "Schema-invalid"), ("scan", "Last scan")]):
            card = ttk.Frame(data_integrity_metrics, style="Card.TFrame", padding=8)
            card.grid(row=0, column=index, sticky="ew", padx=(0 if index == 0 else 4, 0))
            ttk.Label(card, text=label, style="Muted.TLabel").grid(row=0, column=0, sticky="w")
            ttk.Label(card, textvariable=self._data_integrity_kpi_vars[key], font=("Segoe UI Semibold", 11), wraplength=160 if key == "scan" else 120, justify="left").grid(row=1, column=0, sticky="w", pady=(2, 0))
        ttk.Button(data_integrity_frame, text="Open Health Report", command=lambda: self._open_path(self._storage_layout().reports_dir / "processed_json_health.md")).grid(row=0, column=2, rowspan=2, sticky="ns", padx=(8, 0))
        row += 1

        active_ops_frame = ttk.LabelFrame(host, text="Active Operations", padding=8, style="Section.TLabelframe")
        active_ops_frame.grid(row=row, column=0, sticky="ew", pady=(8, 0))
        self._overview_sections["active_operations"] = active_ops_frame
        active_ops_frame.columnconfigure(1, weight=1)
        for r, (key, label) in enumerate([("status", "Operational status:"), ("summary", "Current summary:"), ("active_detail", "Lock detail:"), ("failed_detail", "Failure detail:"), ("latest_detail", "Latest manifest:"), ("next_action", "Recommended next step:")]):
            ttk.Label(active_ops_frame, text=label, font=("Segoe UI Semibold", 8)).grid(row=r, column=0, sticky="nw", padx=(0, 6), pady=1)
            ttk.Label(active_ops_frame, textvariable=self._active_operations_vars[key], wraplength=760, justify="left", font=("Segoe UI", 8)).grid(row=r, column=1, sticky="w", pady=1)
        active_ops_metrics = ttk.Frame(active_ops_frame)
        active_ops_metrics.grid(row=6, column=0, columnspan=2, sticky="ew", pady=(8, 0))
        for index in range(5):
            active_ops_metrics.columnconfigure(index, weight=1)
        for index, (key, label) in enumerate([("active", "Active locks"), ("running", "Running states"), ("failed", "Failed states"), ("stale", "Stale locks"), ("latest", "Latest stage")]):
            card = ttk.Frame(active_ops_metrics, style="Card.TFrame", padding=8)
            card.grid(row=0, column=index, sticky="ew", padx=(0 if index == 0 else 4, 0))
            ttk.Label(card, text=label, style="Muted.TLabel").grid(row=0, column=0, sticky="w")
            ttk.Label(card, textvariable=self._active_operations_kpi_vars[key], font=("Segoe UI Semibold", 11)).grid(row=1, column=0, sticky="w", pady=(2, 0))
        ttk.Button(active_ops_frame, text="Open Stage State Folder", command=lambda: self._open_path(self._storage_layout().stage_state_dir)).grid(row=0, column=2, rowspan=6, sticky="ns", padx=(8, 0))
        row += 1

        identity_frame = ttk.LabelFrame(host, text="Identity Crosswalk", padding=8, style="Section.TLabelframe")
        identity_frame.grid(row=row, column=0, sticky="ew", pady=(8, 0))
        self._overview_sections["identity_crosswalk"] = identity_frame
        identity_frame.columnconfigure(1, weight=1)
        for r, (key, label) in enumerate([("status", "Crosswalk status:"), ("summary", "Current summary:"), ("next_action", "Recommended next step:")]):
            ttk.Label(identity_frame, text=label, font=("Segoe UI Semibold", 8)).grid(row=r, column=0, sticky="nw", padx=(0, 6), pady=1)
            ttk.Label(identity_frame, textvariable=self._identity_crosswalk_vars[key], wraplength=760, justify="left", font=("Segoe UI", 8)).grid(row=r, column=1, sticky="w", pady=1)
        identity_metrics = ttk.Frame(identity_frame)
        identity_metrics.grid(row=3, column=0, columnspan=2, sticky="ew", pady=(8, 0))
        for index in range(4):
            identity_metrics.columnconfigure(index, weight=1)
        for index, (key, label) in enumerate([("proteins", "Protein IDs"), ("ligands", "Ligand IDs"), ("pairs", "Pair IDs"), ("fallbacks", "Fallbacks")]):
            card = ttk.Frame(identity_metrics, style="Card.TFrame", padding=8)
            card.grid(row=0, column=index, sticky="ew", padx=(0 if index == 0 else 4, 0))
            ttk.Label(card, text=label, style="Muted.TLabel").grid(row=0, column=0, sticky="w")
            ttk.Label(card, textvariable=self._identity_crosswalk_kpi_vars[key], font=("Segoe UI Semibold", 11)).grid(row=1, column=0, sticky="w", pady=(2, 0))
        ttk.Button(identity_frame, text="Export Identity Crosswalk", command=lambda: self._spawn_stage("export-identity-crosswalk")).grid(row=0, column=2, rowspan=3, sticky="ns", padx=(8, 0))
        row += 1

        release_frame = ttk.LabelFrame(host, text="Release Readiness", padding=8, style="Section.TLabelframe")
        release_frame.grid(row=row, column=0, sticky="ew", pady=(8, 0))
        self._overview_sections["release_readiness"] = release_frame
        release_frame.columnconfigure(1, weight=1)
        for r, (key, label) in enumerate([("status", "Release status:"), ("summary", "Current summary:"), ("next_action", "Recommended next step:")]):
            ttk.Label(release_frame, text=label, font=("Segoe UI Semibold", 8)).grid(row=r, column=0, sticky="nw", padx=(0, 6), pady=1)
            ttk.Label(release_frame, textvariable=self._release_readiness_vars[key], wraplength=760, justify="left", font=("Segoe UI", 8)).grid(row=r, column=1, sticky="w", pady=1)
        release_metrics = ttk.Frame(release_frame)
        release_metrics.grid(row=3, column=0, columnspan=2, sticky="ew", pady=(8, 0))
        for index in range(5):
            release_metrics.columnconfigure(index, weight=1)
        for index, (key, label) in enumerate([("entries", "Canonical entries"), ("pairs", "Canonical pairs"), ("model_ready", "Model-ready"), ("held_out", "Held-out"), ("blockers", "Blockers")]):
            card = ttk.Frame(release_metrics, style="Card.TFrame", padding=8)
            card.grid(row=0, column=index, sticky="ew", padx=(0 if index == 0 else 4, 0))
            ttk.Label(card, text=label, style="Muted.TLabel").grid(row=0, column=0, sticky="w")
            ttk.Label(card, textvariable=self._release_readiness_kpi_vars[key], font=("Segoe UI Semibold", 11)).grid(row=1, column=0, sticky="w", pady=(2, 0))
        ttk.Button(release_frame, text="Run Release Check", command=lambda: self._spawn_stage("release-check")).grid(row=0, column=2, rowspan=3, sticky="ns", padx=(8, 0))
        row += 1

        risk_frame = ttk.LabelFrame(host, text="Pathway Risk Context", padding=8, style="Section.TLabelframe")
        risk_frame.grid(row=row, column=0, sticky="ew", pady=(8, 0))
        self._overview_sections["risk_context"] = risk_frame
        risk_frame.columnconfigure(1, weight=1)
        for r, (key, label) in enumerate([("status", "Context status:"), ("summary", "Interpretation:"), ("next_action", "Recommended next step:")]):
            ttk.Label(risk_frame, text=label, font=("Segoe UI Semibold", 8)).grid(row=r, column=0, sticky="nw", padx=(0, 6), pady=1)
            ttk.Label(risk_frame, textvariable=self._risk_vars[key], wraplength=760, justify="left", font=("Segoe UI", 8)).grid(row=r, column=1, sticky="w", pady=1)
        risk_metrics = ttk.Frame(risk_frame)
        risk_metrics.grid(row=3, column=0, columnspan=2, sticky="ew", pady=(8, 0))
        for index in range(4):
            risk_metrics.columnconfigure(index, weight=1)
        for index, (key, label) in enumerate([("severity", "Severity"), ("score", "Risk score"), ("matches", "Matched pairs"), ("pathways", "Pathway overlap")]):
            card = ttk.Frame(risk_metrics, style="Card.TFrame", padding=8)
            card.grid(row=0, column=index, sticky="ew", padx=(0 if index == 0 else 4, 0))
            ttk.Label(card, text=label, style="Muted.TLabel").grid(row=0, column=0, sticky="w")
            ttk.Label(card, textvariable=self._risk_kpi_vars[key], font=("Segoe UI Semibold", 11)).grid(row=1, column=0, sticky="w", pady=(2, 0))
        ttk.Button(risk_frame, text="Open Risk Summary", command=lambda: self._open_path(self._storage_layout().risk_dir / "pathway_risk_summary.json")).grid(row=0, column=2, rowspan=3, sticky="ns", padx=(8, 0))
        row += 1

        prediction_frame = ttk.LabelFrame(host, text="Prediction Status", padding=8, style="Section.TLabelframe")
        prediction_frame.grid(row=row, column=0, sticky="ew", pady=(8, 0))
        self._overview_sections["prediction_status"] = prediction_frame
        prediction_frame.columnconfigure(1, weight=1)
        for r, (key, label) in enumerate([("status", "Prediction status:"), ("method", "Selected model:"), ("preference", "Current preference:"), ("summary", "Interpretation:")]):
            ttk.Label(prediction_frame, text=label, font=("Segoe UI Semibold", 8)).grid(row=r, column=0, sticky="nw", padx=(0, 6), pady=1)
            ttk.Label(prediction_frame, textvariable=self._prediction_status_vars[key], wraplength=760, justify="left", font=("Segoe UI", 8)).grid(row=r, column=1, sticky="w", pady=1)
        prediction_metrics = ttk.Frame(prediction_frame)
        prediction_metrics.grid(row=4, column=0, columnspan=2, sticky="ew", pady=(8, 0))
        for index in range(4):
            prediction_metrics.columnconfigure(index, weight=1)
        for index, (key, label) in enumerate([("targets", "Targets"), ("top_target", "Top target"), ("confidence", "Confidence"), ("query_features", "Query features")]):
            card = ttk.Frame(prediction_metrics, style="Card.TFrame", padding=8)
            card.grid(row=0, column=index, sticky="ew", padx=(0 if index == 0 else 4, 0))
            ttk.Label(card, text=label, style="Muted.TLabel").grid(row=0, column=0, sticky="w")
            ttk.Label(card, textvariable=self._prediction_status_kpi_vars[key], font=("Segoe UI Semibold", 11)).grid(row=1, column=0, sticky="w", pady=(2, 0))
        row += 1

        workflow_frame = ttk.LabelFrame(host, text="Recommended Workflow", padding=8, style="Section.TLabelframe")
        workflow_frame.grid(row=row, column=0, sticky="ew", pady=(8, 0))
        self._overview_sections["workflow_guidance"] = workflow_frame
        workflow_frame.columnconfigure(1, weight=1)
        for r, (key, label) in enumerate([("phase", "Current phase:"), ("summary", "Why this is next:"), ("step_1", "Step 1:"), ("step_2", "Step 2:"), ("step_3", "Step 3:")]):
            ttk.Label(workflow_frame, text=label, font=("Segoe UI Semibold", 8)).grid(row=r, column=0, sticky="nw", padx=(0, 6), pady=1)
            ttk.Label(workflow_frame, textvariable=self._workflow_guidance_vars[key], wraplength=760, justify="left", font=("Segoe UI", 8)).grid(row=r, column=1, sticky="w", pady=1)
        row += 1

        curation_frame = ttk.LabelFrame(host, text="Curation Review", padding=8, style="Section.TLabelframe")
        curation_frame.grid(row=row, column=0, sticky="ew", pady=(8, 0))
        self._overview_sections["curation_review"] = curation_frame
        curation_frame.columnconfigure(1, weight=1)
        for r, (key, label) in enumerate([("exclusions", "Exclusion review:"), ("conflicts", "Conflict review:"), ("issues", "Issue review:"), ("next_action", "Recommended next step:")]):
            ttk.Label(curation_frame, text=label, font=("Segoe UI Semibold", 8)).grid(row=r, column=0, sticky="nw", padx=(0, 6), pady=1)
            ttk.Label(curation_frame, textvariable=self._curation_review_vars[key], wraplength=760, justify="left", font=("Segoe UI", 8)).grid(row=r, column=1, sticky="w", pady=1)
        curation_actions = ttk.Frame(curation_frame)
        curation_actions.grid(row=0, column=2, rowspan=4, sticky="ns", padx=(8, 0))
        for index in range(2):
            curation_actions.columnconfigure(index, weight=1)
        ttk.Button(curation_actions, text="Refresh Filtered Review", command=self._refresh_filtered_review_csv).grid(row=0, column=0, columnspan=2, sticky="ew", pady=(0, 6))
        ttk.Button(curation_actions, text="Open Issues", command=lambda: self._open_path(self._existing_review_path("issue_csv"))).grid(row=1, column=0, sticky="ew", padx=(0, 4), pady=2)
        ttk.Button(curation_actions, text="Open Conflicts", command=lambda: self._open_path(self._existing_review_path("conflict_csv"))).grid(row=1, column=1, sticky="ew", padx=(4, 0), pady=2)
        row += 1

        demo_frame = ttk.LabelFrame(host, text="Demo Readiness", padding=8, style="Section.TLabelframe")
        demo_frame.grid(row=row, column=0, sticky="ew", pady=(8, 0))
        self._overview_sections["demo_readiness"] = demo_frame
        demo_frame.columnconfigure(1, weight=1)
        for r, (key, label) in enumerate([("readiness", "Current state:"), ("summary", "Summary:"), ("customer_message", "Customer-facing note:"), ("walkthrough", "Presenter flow:"), ("blockers", "Blockers:"), ("warnings", "Warnings:")]):
            ttk.Label(demo_frame, text=label, font=("Segoe UI Semibold", 8)).grid(row=r, column=0, sticky="nw", padx=(0, 6), pady=1)
            ttk.Label(demo_frame, textvariable=self._demo_readiness_vars[key], wraplength=760, justify="left", font=("Segoe UI", 8)).grid(row=r, column=1, sticky="w", pady=1)
        ttk.Button(demo_frame, text="Open Demo Walkthrough", command=lambda: self._open_path(self._storage_layout().feature_reports_dir / "demo_walkthrough.md")).grid(row=0, column=2, rowspan=3, sticky="ns", padx=(8, 0))
        tutorial_frame = ttk.Frame(demo_frame, style="Card.TFrame", padding=8)
        tutorial_frame.grid(row=6, column=0, columnspan=3, sticky="ew", pady=(10, 0))
        tutorial_frame.columnconfigure(1, weight=1)
        for r, (key, label) in enumerate([
            ("step", "Guided step:"),
            ("detail", "What this is doing:"),
            ("innovation", "Why it matters:"),
            ("instruction", "What to click:"),
            ("scroll_hint", "How to find it:"),
            ("progress", "Progress:"),
        ]):
            ttk.Label(tutorial_frame, text=label, font=("Segoe UI Semibold", 8)).grid(row=r, column=0, sticky="nw", padx=(0, 6), pady=1)
            ttk.Label(tutorial_frame, textvariable=self._demo_tutorial_vars[key], wraplength=880, justify="left", font=("Segoe UI", 8)).grid(row=r, column=1, sticky="w", pady=1)
        ttk.Button(
            tutorial_frame,
            text="Focus Guided Control",
            command=self._focus_demo_tutorial_target,
        ).grid(row=0, column=2, rowspan=2, sticky="ns", padx=(8, 0))
        row += 1

        health_frame = ttk.LabelFrame(host, text="Review Health", padding=8, style="Section.TLabelframe")
        health_frame.grid(row=row, column=0, sticky="ew", pady=(8, 0))
        self._overview_sections["review_health"] = health_frame
        health_frame.columnconfigure(1, weight=1)
        for r, (key, label) in enumerate([("readiness", "Release readiness:"), ("coverage", "Coverage snapshot:"), ("quality", "Quality snapshot:"), ("next_action", "Recommended next step:")]):
            ttk.Label(health_frame, text=label, font=("Segoe UI Semibold", 8)).grid(row=r, column=0, sticky="nw", padx=(0, 6), pady=1)
            ttk.Label(health_frame, textvariable=self._review_health_vars[key], wraplength=760, justify="left", font=("Segoe UI", 8)).grid(row=r, column=1, sticky="w", pady=1)
        row += 1

        help_frame = ttk.LabelFrame(host, text="Interpretation Guide", padding=8, style="Section.TLabelframe")
        help_frame.grid(row=row, column=0, sticky="ew", pady=(8, 0))
        self._overview_sections["interpretation_guide"] = help_frame
        help_frame.columnconfigure(0, weight=1)
        for r, text in enumerate([
            "High confidence means a field came from direct structured data or a deterministic merge with no unresolved ambiguity.",
            "Medium or low confidence usually indicates fallback logic, partial source coverage, or unresolved biological context.",
            "A conflicted pair means multiple sources reported materially divergent values for the same pair and assay type.",
            "Model-ready outputs are conservative by design. Excluded pairs stay out until the blockers are resolved or accepted.",
        ]):
            ttk.Label(help_frame, text=f"- {text}", wraplength=980, justify="left", style="Muted.TLabel").grid(row=r, column=0, sticky="w", pady=1)
        row += 1

        last_run_frame = ttk.LabelFrame(host, text="Last Workflow Run", padding=8, style="Section.TLabelframe")
        last_run_frame.grid(row=row, column=0, sticky="ew", pady=(8, 0))
        self._overview_sections["last_run"] = last_run_frame
        last_run_frame.columnconfigure(1, weight=1)
        for r, (key, label) in enumerate([("status", "Run state:"), ("summary", "Summary:"), ("last_stage", "Last stage:"), ("last_result", "Last result:"), ("next_action", "Next step:")]):
            ttk.Label(last_run_frame, text=label, font=("Segoe UI Semibold", 8)).grid(row=r, column=0, sticky="nw", padx=(0, 6), pady=1)
            ttk.Label(last_run_frame, textvariable=self._last_run_vars[key], wraplength=860, justify="left", font=("Segoe UI", 8)).grid(row=r, column=1, sticky="w", pady=1)
        row += 1

        freshness_frame = ttk.LabelFrame(host, text="Artifact Freshness", padding=8, style="Section.TLabelframe")
        freshness_frame.grid(row=row, column=0, sticky="ew", pady=(8, 0))
        self._overview_sections["artifact_freshness"] = freshness_frame
        freshness_frame.columnconfigure(1, weight=1)
        for r, (key, label) in enumerate([("release_check", "Release check:"), ("demo_snapshot", "Demo snapshot:"), ("prediction_manifest", "Prediction manifest:"), ("risk_summary", "Risk summary:"), ("model_comparison", "Model comparison:"), ("training_quality", "Training quality:"), ("release_manifest", "Release manifest:")]):
            ttk.Label(freshness_frame, text=label, font=("Segoe UI Semibold", 8)).grid(row=r, column=0, sticky="nw", padx=(0, 6), pady=1)
            ttk.Label(freshness_frame, textvariable=self._artifact_freshness_vars[key], wraplength=860, justify="left", font=("Segoe UI", 8)).grid(row=r, column=1, sticky="w", pady=1)
        row += 1

        actions_frame = ttk.LabelFrame(host, text="Quick Actions", padding=8, style="Section.TLabelframe")
        actions_frame.grid(row=row, column=0, sticky="ew", pady=(8, 0))
        self._overview_sections["quick_actions"] = actions_frame
        for index in range(7):
            actions_frame.columnconfigure(index, weight=1)
        ttk.Button(actions_frame, text="Open Filtered Review CSV", command=lambda: self._open_path(self._filtered_review_csv_path())).grid(row=0, column=0, sticky="ew", padx=(0, 4))
        ttk.Button(actions_frame, text="Open Model-ready CSV", command=lambda: self._open_path(self._existing_review_path("model_ready_pairs_csv"))).grid(row=0, column=1, sticky="ew", padx=4)
        ttk.Button(actions_frame, text="Open Custom Training Set", command=lambda: self._open_path(self._existing_review_path("custom_training_set_csv"))).grid(row=0, column=2, sticky="ew", padx=4)
        ttk.Button(actions_frame, text="Open Split Benchmark", command=lambda: self._open_path(self._existing_review_path("custom_training_split_benchmark_csv"))).grid(row=0, column=3, sticky="ew", padx=4)
        ttk.Button(actions_frame, text="Open Training Scorecard", command=lambda: self._open_path(self._existing_review_path("custom_training_scorecard_json"))).grid(row=0, column=4, sticky="ew", padx=4)
        ttk.Button(actions_frame, text="Open Coverage Summary", command=lambda: self._open_path(self._existing_review_path("scientific_coverage_json"))).grid(row=0, column=5, sticky="ew", padx=4)
        ttk.Button(actions_frame, text="Open Storage Root", command=lambda: self._open_path(self._storage_layout().root)).grid(row=0, column=6, sticky="ew", padx=(4, 0))
        self._apply_demo_mode()
        if self._pipeline_scroll_canvas is not None and self._pipeline_scroll_frame is not None:
            self._bind_canvas_mousewheel(self._pipeline_scroll_canvas, self._pipeline_scroll_frame)

    @staticmethod
    def _demo_customer_message(readiness: str) -> str:
        if readiness == "ready_for_internal_demo":
            return "Good for a customer-facing baseline walkthrough. Keep claims tied to the artifacts shown on screen."
        if readiness == "technically_reviewable_not_polished":
            return "Good for a technical preview, but call out unfinished areas as roadmap items instead of implied capabilities."
        if readiness == "not_demo_ready":
            return "Finish the blockers before using this workspace for a live customer demo."
        return "Review the current workspace state before presenting it externally."

    @staticmethod
    def _format_demo_walkthrough(steps: list[object]) -> str:
        clean_steps = [str(step).strip() for step in steps if str(step).strip()]
        if not clean_steps:
            return "1. Open the overview and confirm the current workspace state."
        return "\n".join(f"{index}. {step}" for index, step in enumerate(clean_steps, start=1))

    @staticmethod
    def _completion_status_color(status: str) -> str:
        normalized = status.strip().lower()
        if normalized in {"done", "complete"}:
            return _SUCCESS_FG
        if normalized in {"blocked", "error"}:
            return _ERROR_FG
        if normalized in {"partial", "in_progress", "in progress"}:
            return _WARNING_FG
        return _MUTED_FG

    def _apply_demo_mode(self) -> None:
        if not hasattr(self, "_overview_sections"):
            return
        demo_mode = bool(self._demo_mode_var.get())
        compact_mode = bool(self._compact_overview_var.get()) if hasattr(self, "_compact_overview_var") else False
        demo_visible = {
            "presenter_banner",
            "completion",
            "training_quality",
            "model_comparison",
            "search_preview",
            "source_configuration",
            "data_integrity",
            "release_readiness",
            "risk_context",
            "prediction_status",
            "workflow_guidance",
            "demo_readiness",
            "last_run",
            "artifact_freshness",
            "quick_actions",
        }
        compact_visible = {
            "presenter_banner",
            "completion",
            "training_builder",
            "training_quality",
            "model_comparison",
            "split_diagnostics",
            "search_preview",
            "source_configuration",
            "data_integrity",
            "release_readiness",
            "workflow_guidance",
            "last_run",
            "artifact_freshness",
            "quick_actions",
        }
        visible = set(self._overview_sections.keys())
        if compact_mode:
            visible &= compact_visible
        if demo_mode:
            visible &= demo_visible
        for key, widget in self._overview_sections.items():
            if key in visible:
                widget.grid()
            else:
                widget.grid_remove()

    def _on_demo_mode_toggle(self) -> None:
        if not bool(self._demo_mode_var.get()):
            self._apply_demo_mode()
            self._update_demo_tutorial()
            return
        layout = self._storage_layout()
        if not is_demo_workspace_seeded(layout):
            try:
                cfg = load_config(_SOURCES_CFG) if _SOURCES_CFG.exists() else AppConfig(storage_root=str(layout.root))
            except Exception:
                cfg = AppConfig(storage_root=str(layout.root))
            try:
                result = seed_demo_workspace(layout, cfg, repo_root=Path.cwd())
                if result.seeded:
                    self._log_line(
                        f"Demo workspace seeded at {result.manifest_path}. "
                        "Search results, training artifacts, graphs, and model outputs are simulated."
                    )
            except Exception as exc:
                self._demo_mode_var.set(False)
                self._apply_demo_mode()
                self._log_line(f"Demo workspace seeding failed: {exc}")
                messagebox.showerror("Demo Mode", f"Could not seed the demo workspace.\n\n{exc}")
                return
        self._apply_demo_mode()
        self._refresh_overview_async(prefer_cached_status=True)
        self._refresh_model_studio(record_demo=False)
        self._compare_model_runs(record_demo=False)
        self._update_demo_tutorial()
        if self._left_notebook is not None and "model_studio" in self._left_tab_frames:
            self._left_notebook.select(self._left_tab_frames["model_studio"])
        self._root.after(150, self._focus_demo_tutorial_target)
        messagebox.showinfo(
            "Demo Mode",
            "Demo Mode is on.\n\n"
            "Start in Model Studio: read the Demo Control Center, then click the highlighted control.\n\n"
            "After you train a demo model, charts and run outputs will appear in Latest Run Outputs.",
        )

    def _build_overview_snapshot(self, *, prefer_cached_status: bool = False) -> GUIOverviewSnapshot:
        layout = self._storage_layout()
        try:
            cfg = load_config(_SOURCES_CFG) if _SOURCES_CFG.exists() else AppConfig(storage_root=str(layout.root))
        except Exception:
            cfg = AppConfig(storage_root=str(layout.root))
        return build_gui_overview_snapshot(
            layout,
            cfg,
            repo_root=Path.cwd(),
            prefer_cached_status=prefer_cached_status,
        )

    def _ensure_overview_refresh_state(self) -> None:
        if not hasattr(self, "_overview_refresh_generation"):
            self._overview_refresh_generation = 0

    def _refresh_overview(self, *, prefer_cached_status: bool = False) -> None:
        self._ensure_overview_refresh_state()
        self._overview_refresh_generation += 1
        snapshot = self._build_overview_snapshot(prefer_cached_status=prefer_cached_status)
        self._apply_overview_snapshot(snapshot, prefer_cached_status=prefer_cached_status)

    def _refresh_overview_async(self, *, prefer_cached_status: bool = False) -> None:
        if not hasattr(self, "_root"):
            self._refresh_overview(prefer_cached_status=prefer_cached_status)
            return
        self._ensure_overview_refresh_state()
        self._overview_refresh_generation += 1
        generation = self._overview_refresh_generation
        layout = self._storage_layout()
        try:
            cfg = load_config(_SOURCES_CFG) if _SOURCES_CFG.exists() else AppConfig(storage_root=str(layout.root))
        except Exception:
            cfg = AppConfig(storage_root=str(layout.root))

        def _worker() -> None:
            try:
                snapshot = build_gui_overview_snapshot(
                    layout,
                    cfg,
                    repo_root=Path.cwd(),
                    prefer_cached_status=prefer_cached_status,
                )
            except Exception as exc:
                self._root.after(0, self._log_line, f"Overview refresh failed: {exc}")
                return
            self._root.after(
                0,
                self._apply_overview_snapshot_async,
                generation,
                snapshot,
                prefer_cached_status,
            )

        threading.Thread(target=_worker, daemon=True).start()

    def _apply_overview_snapshot_async(
        self,
        generation: int,
        snapshot: GUIOverviewSnapshot,
        prefer_cached_status: bool,
    ) -> None:
        if self._closing or generation != self._overview_refresh_generation:
            return
        self._apply_overview_snapshot(snapshot, prefer_cached_status=prefer_cached_status)

    def _apply_overview_snapshot(
        self,
        snapshot: GUIOverviewSnapshot,
        *,
        prefer_cached_status: bool = False,
    ) -> None:
        for key, value in snapshot.counts.items():
            self._overview_vars[key].set(value)
        for key, path in snapshot.review_paths.items():
            self._review_export_vars[key].set(path if path else "--")
        for key, value in snapshot.review_health.items():
            if key in self._review_health_vars:
                self._review_health_vars[key].set(value)
        for key, value in snapshot.presenter_banner.items():
            if key in self._presenter_banner_vars:
                self._presenter_banner_vars[key].set(value)
        for key, value in snapshot.completion_summary.items():
            if key in self._completion_summary_vars:
                self._completion_summary_vars[key].set(value)
        for index, row in enumerate(snapshot.completion_rows):
            if index >= len(self._completion_row_vars):
                break
            for key, value in row.items():
                if key in self._completion_row_vars[index]:
                    self._completion_row_vars[index][key].set(value)
            if index < len(self._completion_status_labels):
                self._completion_status_labels[index].configure(
                    fg=self._completion_status_color(str(row.get("status") or ""))
                )
        for key, value in snapshot.artifact_freshness.items():
            if key in self._artifact_freshness_vars:
                self._artifact_freshness_vars[key].set(value)
        for key, value in snapshot.last_run_summary.items():
            if key in self._last_run_vars:
                self._last_run_vars[key].set(value)
        for key, value in snapshot.training_summary.items():
            if key in self._training_set_vars:
                self._training_set_vars[key].set(value)
        for key, value in snapshot.training_kpis.items():
            if key in self._training_kpi_vars:
                self._training_kpi_vars[key].set(value)
        for key, value in snapshot.training_workflow.items():
            if key in self._training_workflow_vars:
                self._training_workflow_vars[key].set(value)
        for key, value in snapshot.training_quality_summary.items():
            if key in self._training_quality_vars:
                self._training_quality_vars[key].set(value)
        for key, value in snapshot.training_quality_kpis.items():
            if key in self._training_quality_kpi_vars:
                self._training_quality_kpi_vars[key].set(value)
        for key, value in snapshot.model_comparison_summary.items():
            if key in self._model_comparison_vars:
                self._model_comparison_vars[key].set(value)
        for key, value in snapshot.model_comparison_kpis.items():
            if key in self._model_comparison_kpi_vars:
                self._model_comparison_kpi_vars[key].set(value)
        for key, value in snapshot.split_diagnostics_summary.items():
            if key in self._split_diagnostics_vars:
                self._split_diagnostics_vars[key].set(value)
        for key, value in snapshot.split_diagnostics_kpis.items():
            if key in self._split_diagnostics_kpi_vars:
                self._split_diagnostics_kpi_vars[key].set(value)
        for key, value in snapshot.search_preview_summary.items():
            if key in self._search_preview_vars:
                self._search_preview_vars[key].set(value)
        for key, value in snapshot.search_preview_kpis.items():
            if key in self._search_preview_kpi_vars:
                self._search_preview_kpi_vars[key].set(value)
        for key, value in snapshot.source_configuration_summary.items():
            if key in self._source_configuration_vars:
                self._source_configuration_vars[key].set(value)
        for key, value in snapshot.source_configuration_kpis.items():
            if key in self._source_configuration_kpi_vars:
                self._source_configuration_kpi_vars[key].set(value)
        for key, value in snapshot.source_run_summary.items():
            if key in self._source_run_vars:
                self._source_run_vars[key].set(value)
        for key, value in snapshot.source_run_kpis.items():
            if key in self._source_run_kpi_vars:
                self._source_run_kpi_vars[key].set(value)
        for key, value in snapshot.data_integrity_summary.items():
            if key in self._data_integrity_vars:
                self._data_integrity_vars[key].set(value)
        for key, value in snapshot.data_integrity_kpis.items():
            if key in self._data_integrity_kpi_vars:
                self._data_integrity_kpi_vars[key].set(value)
        for key, value in snapshot.active_operations_summary.items():
            if key in self._active_operations_vars:
                self._active_operations_vars[key].set(value)
        for key, value in snapshot.active_operations_kpis.items():
            if key in self._active_operations_kpi_vars:
                self._active_operations_kpi_vars[key].set(value)
        for key, value in snapshot.identity_crosswalk_summary.items():
            if key in self._identity_crosswalk_vars:
                self._identity_crosswalk_vars[key].set(value)
        for key, value in snapshot.identity_crosswalk_kpis.items():
            if key in self._identity_crosswalk_kpi_vars:
                self._identity_crosswalk_kpi_vars[key].set(value)
        for key, value in snapshot.release_readiness_summary.items():
            if key in self._release_readiness_vars:
                self._release_readiness_vars[key].set(value)
        for key, value in snapshot.release_readiness_kpis.items():
            if key in self._release_readiness_kpi_vars:
                self._release_readiness_kpi_vars[key].set(value)
        for key, value in snapshot.risk_summary.items():
            if key in self._risk_vars:
                self._risk_vars[key].set(value)
        for key, value in snapshot.risk_kpis.items():
            if key in self._risk_kpi_vars:
                self._risk_kpi_vars[key].set(value)
        for key, value in snapshot.prediction_status_summary.items():
            if key in self._prediction_status_vars:
                self._prediction_status_vars[key].set(value)
        for key, value in snapshot.prediction_status_kpis.items():
            if key in self._prediction_status_kpi_vars:
                self._prediction_status_kpi_vars[key].set(value)
        for key, value in snapshot.workflow_guidance.items():
            if key in self._workflow_guidance_vars:
                self._workflow_guidance_vars[key].set(value)
        for key, value in snapshot.curation_summary.items():
            if key in self._curation_review_vars:
                self._curation_review_vars[key].set(value)
        self._demo_readiness_vars["readiness"].set(snapshot.demo_readiness.readiness or "--")
        self._demo_readiness_vars["summary"].set(snapshot.demo_readiness.summary or "--")
        blockers = snapshot.demo_readiness.blockers
        warnings = snapshot.demo_readiness.warnings
        customer_message = self._demo_customer_message(snapshot.demo_readiness.readiness or "")
        if "demo_mode_simulated_outputs" in blockers or "demo_mode_simulated_outputs" in warnings:
            customer_message = (
                "Demo Mode is showing simulated outputs so the intended workflow can be presented instantly. "
                "Do not describe these artifacts as real scientific results."
            )
        self._demo_readiness_vars["customer_message"].set(customer_message)
        self._demo_readiness_vars["walkthrough"].set(
            self._format_demo_walkthrough(snapshot.demo_readiness.recommended_demo_flow)
        )
        self._demo_readiness_vars["blockers"].set(", ".join(str(item) for item in blockers) if blockers else "none")
        self._demo_readiness_vars["warnings"].set(", ".join(str(item) for item in warnings) if warnings else "none")
        self._apply_demo_mode()
        if prefer_cached_status and snapshot.demo_readiness.status_snapshot.processed_health_cache_stale:
            self._root.after(50, self._refresh_overview_async)

    def _review_export_paths(self) -> dict[str, str]:
        return _review_export_paths_impl(self._storage_layout(), repo_root=Path.cwd())

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
        if "custom_training_scorecard_json" in export_status:
            self._log_line(f"Root export refreshed: {export_status['custom_training_scorecard_json']}")
        if "custom_training_split_benchmark_csv" in export_status:
            self._log_line(f"Root export refreshed: {export_status['custom_training_split_benchmark_csv']}")
        if "scientific_coverage_json" in export_status:
            self._log_line(f"Root export refreshed: {export_status['scientific_coverage_json']}")
        for key in ("master_csv_error", "pair_csv_error", "issue_csv_error", "conflict_csv_error", "source_state_csv_error"):
            if key in export_status:
                self._log_line(f"Root export warning: {export_status[key]}")
        if "release_exports_error" in export_status:
            self._log_line(f"Root export warning: {export_status['release_exports_error']}")
        self._refresh_overview_async()

    def _filtered_review_csv_path(self) -> Path:
        return Path.cwd() / _FILTERED_REVIEW_CSV_NAME

    def _refresh_filtered_review_csv(self) -> None:
        self._apply_local_review_filters()

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
        frame = ttk.LabelFrame(self._root, text="Live Run Log", padding=8, style="Section.TLabelframe")
        frame.grid(row=2, column=0, columnspan=2, sticky="nsew", padx=10, pady=(0, 10))
        frame.columnconfigure(0, weight=1)
        frame.rowconfigure(0, weight=1)

        self._log = scrolledtext.ScrolledText(
            frame,
            state="disabled",
            height=12,
            font=("Cascadia Code", 9),
            bg=_LOG_BG, fg=_LOG_FG,
            insertbackground=_HEADER_FG,
            relief="flat",
            padx=10,
            pady=10,
            bd=0,
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
        self._max_results_var.set("" if sc.max_results is None else str(sc.max_results))
        self._representative_sampling_var.set(sc.representative_sampling)
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
        max_results_var = getattr(self, "_max_results_var", None)
        representative_sampling_var = getattr(self, "_representative_sampling_var", None)
        max_results_str = max_results_var.get().strip() if max_results_var is not None else ""
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
        max_results: int | None = int(max_results_str) if max_results_str.isdigit() else None
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
            max_results=max_results,
            representative_sampling=(
                representative_sampling_var.get()
                if representative_sampling_var is not None
                else True
            ),
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

    def _preview_rcsb_search(self) -> None:
        save_criteria(self._criteria_from_ui(), _CRITERIA_PATH)
        self._record_demo_action("search.preview_rcsb")
        self._spawn_stage("preview-rcsb-search")

    # ------------------------------------------------------------------
    # Logging helpers
    # ------------------------------------------------------------------

    def _log_line(self, text: str) -> None:
        if self._run_in_progress:
            normalized = " ".join(text.strip().split())
            if normalized and set(normalized) != {"─"} and set(normalized) != {"═"}:
                self._run_last_message_var.set(normalized)
        self._log.configure(state="normal")
        self._log.insert("end", text.rstrip() + "\n")
        self._log.see("end")
        self._log.configure(state="disabled")

    def _clear_log(self) -> None:
        self._log.configure(state="normal")
        self._log.delete("1.0", "end")
        self._log.configure(state="disabled")

    def _display_stage_name(self, stage: str) -> str:
        return _STAGE_DISPLAY_NAMES.get(stage, stage.replace("-", " ").title())

    def _set_action_buttons_enabled(self, enabled: bool) -> None:
        state = "normal" if enabled else "disabled"
        for button in self._action_buttons:
            try:
                button.configure(state=state)
            except tk.TclError:
                continue

    def _update_run_elapsed(self) -> None:
        if not self._run_in_progress or self._run_started_at is None:
            return
        elapsed_seconds = max(0, int(time.time() - self._run_started_at))
        if elapsed_seconds < 60:
            elapsed = f"{elapsed_seconds}s"
        else:
            minutes, seconds = divmod(elapsed_seconds, 60)
            elapsed = f"{minutes}m {seconds:02d}s"
        self._run_elapsed_var.set(elapsed)
        self._root.after(1000, self._update_run_elapsed)

    def _start_run_tracking(self, run_label: str, planned_stages: list[str]) -> None:
        self._run_active_label = run_label
        self._run_plan = list(planned_stages)
        self._run_completed_count = 0
        self._run_current_stage_key = None
        self._run_started_at = time.time()
        self._run_in_progress = True
        self._run_state_var.set(f"{run_label} running")
        self._run_current_stage_var.set("Preparing workspace")
        self._run_progress_var.set(f"0 / {len(self._run_plan)} stages complete")
        next_stage = self._display_stage_name(self._run_plan[0]) if self._run_plan else "Nothing queued"
        self._run_next_stage_var.set(next_stage)
        self._run_last_message_var.set("Preparing workspace and validating prerequisites.")
        self._run_elapsed_var.set("0s")
        self._update_run_elapsed()

    def _mark_stage_started(self, stage: str) -> None:
        self._run_current_stage_key = stage
        display_name = self._display_stage_name(stage)
        total = len(self._run_plan)
        current_index = self._run_completed_count + 1 if total else 0
        self._run_state_var.set(f"{self._run_active_label or 'Workflow'} running")
        self._run_current_stage_var.set(display_name)
        self._run_progress_var.set(f"Stage {current_index} of {total}" if total else "Single stage run")
        if stage in self._run_plan:
            stage_index = self._run_plan.index(stage)
            next_stage = (
                self._display_stage_name(self._run_plan[stage_index + 1])
                if stage_index + 1 < len(self._run_plan)
                else "Final stage in plan"
            )
        else:
            next_stage = "Not part of current plan"
        self._run_next_stage_var.set(next_stage)
        self._run_last_message_var.set(f"Running {display_name}...")

    def _mark_stage_finished(self, stage: str, status: str) -> None:
        display_name = self._display_stage_name(stage)
        if status == "done":
            self._run_completed_count += 1
            self._run_progress_var.set(
                f"{self._run_completed_count} / {len(self._run_plan)} stages complete"
                if self._run_plan
                else "Single stage complete"
            )
            pending = self._run_plan[self._run_completed_count:] if self._run_plan else []
            self._run_next_stage_var.set(
                self._display_stage_name(pending[0]) if pending else "No remaining stages"
            )
            self._run_last_message_var.set(f"{display_name} completed successfully.")
        elif status == "cancelled":
            self._run_last_message_var.set(f"{display_name} was cancelled.")
        else:
            self._run_state_var.set(f"{self._run_active_label or 'Workflow'} needs attention")
            self._run_next_stage_var.set("Resolve the failing stage before continuing")
            self._run_last_message_var.set(f"{display_name} failed. Check the live log for details.")

    def _finish_run_tracking(self, status: str, summary: str) -> None:
        label = self._run_active_label or "Workflow"
        state_label = {
            "done": f"{label} complete",
            "cancelled": f"{label} cancelled",
            "error": f"{label} needs attention",
        }.get(status, f"{label} finished")
        self._run_state_var.set(state_label)
        if status != "done":
            self._run_current_stage_var.set("No active stage")
        self._run_next_stage_var.set("Choose the next stage or refresh the overview.")
        self._run_last_message_var.set(summary)
        self._run_in_progress = False
        self._run_current_stage_key = None

    def _try_begin_background_run(self, run_label: str, planned_stages: list[str]) -> bool:
        if not self._running.acquire(blocking=False):
            self._log_line("Another workflow is already running. Wait for it to finish before starting a new one.")
            return False
        self._root.after(0, self._set_action_buttons_enabled, False)
        self._root.after(0, self._start_run_tracking, run_label, planned_stages)
        return True

    def _complete_background_run(self, status: str, summary: str) -> None:
        self._root.after(0, self._finish_run_tracking, status, summary)
        self._root.after(0, self._set_action_buttons_enabled, True)
        self._running.release()

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
        if stage in self._status_vars:
            self._status_vars[stage].set(status)
        if stage in self._status_labels:
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
        elif stage == "harvest-metadata":
            max_proteins = self._harvest_max_proteins_var.get().strip()
            if self._harvest_uniprot_var.get():
                cmd.append("--with-uniprot")
            if self._harvest_alphafold_var.get():
                cmd.append("--with-alphafold")
            if self._harvest_reactome_var.get():
                cmd.append("--with-reactome")
            if getattr(self, "_harvest_interpro_var", None) and self._harvest_interpro_var.get():
                cmd.append("--with-interpro")
            if getattr(self, "_harvest_pfam_var", None) and self._harvest_pfam_var.get():
                cmd.append("--with-pfam")
            if getattr(self, "_harvest_cath_var", None) and self._harvest_cath_var.get():
                cmd.append("--with-cath")
            if getattr(self, "_harvest_scop_var", None) and self._harvest_scop_var.get():
                cmd.append("--with-scop")
            if max_proteins:
                cmd.extend(["--max-proteins", max_proteins])

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

        if stage in {"build-graph", "build-features", "build-training-examples"}:
            cmd.append("--strict-prereqs")

        return cmd

    def _pipeline_stages_for_mode(self, mode: str) -> list[str]:
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

        if self._skip_experimental_stages_var.get():
            stages = [stage for stage in stages if stage not in _EXPERIMENTAL_STAGE_KEYS]
        return stages

    def _stage_prerequisite_message(self, stage: str) -> str | None:
        if bool(self._demo_mode_var.get()):
            return None
        layout = self._storage_layout()
        checks: dict[str, list[tuple[Path, str]]] = {
            "engineer-dataset": [
                (
                    layout.workspace_metadata_dir / "protein_metadata.csv",
                    "Run 'harvest-metadata' first.",
                ),
            ],
            "build-custom-training-set": [
                (
                    layout.root / "model_ready_pairs.csv",
                    "Run 'report' and upstream graph/feature/training stages first so model-ready pairs exist.",
                ),
            ],
            "build-release": [
                (
                    layout.root / "model_ready_pairs.csv",
                    "Run 'report' and upstream graph/feature/training stages first so model-ready pairs exist.",
                ),
            ],
            "build-physics-features": [
                (layout.microstates_dir / "microstate_records.json", "Run 'build-microstates' first."),
            ],
            "build-microstate-refinement": [
                (layout.microstates_dir / "microstate_records.json", "Run 'build-microstates' first."),
            ],
            "build-mm-job-manifests": [
                (
                    layout.microstate_refinement_dir / "microstate_refinement_records.json",
                    "Run 'build-microstate-refinement' first.",
                ),
            ],
            "build-features": [
                (layout.extracted_dir / "assays", "Run 'extract' first."),
                (layout.graph_dir / "graph_edges.json", "Run 'build-graph' first."),
            ],
            "build-graph": [
                (layout.extracted_dir / "entry", "Run 'extract' first."),
            ],
            "build-training-examples": [
                (layout.extracted_dir / "assays", "Run 'extract' first."),
                (layout.features_dir / "feature_records.json", "Run 'build-features' first."),
                (layout.graph_dir / "graph_nodes.json", "Run 'build-graph' first."),
            ],
        }
        missing = [message for path, message in checks.get(stage, []) if not path.exists()]
        if not missing:
            return None
        return "Missing prerequisites: " + " ".join(missing)

    def _run_stage(self, stage: str) -> str:
        """Run one CLI stage via subprocess (call from a background thread).

        Returns 'done' or 'error'.
        """
        if self._closing:
            return "cancelled"
        if bool(self._demo_mode_var.get()):
            return self._run_demo_stage(stage)
        self._root.after(0, self._mark_stage_started, stage)
        self._root.after(0, self._set_status, stage, "running")
        self._root.after(0, self._log_line, f"\n{'─' * 40}")
        self._root.after(0, self._log_line, f"  {stage}")
        self._root.after(0, self._log_line, f"{'─' * 40}")

        prereq_message = self._stage_prerequisite_message(stage)
        if prereq_message:
            self._root.after(0, self._log_line, prereq_message)
            self._root.after(0, self._set_status, stage, "error")
            return "error"

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
        self._root.after(0, self._mark_stage_finished, stage, status)
        self._root.after(0, self._log_line, f"[{stage}] {status}.")
        self._root.after(0, self._refresh_overview)
        return status

    def _run_demo_stage(self, stage: str) -> str:
        if self._closing:
            return "cancelled"
        self._root.after(0, self._mark_stage_started, stage)
        self._root.after(0, self._set_status, stage, "running")
        self._root.after(0, self._log_line, f"\n{'─' * 40}")
        self._root.after(0, self._log_line, f"  {self._display_stage_name(stage)} [Demo Mode]")
        self._root.after(0, self._log_line, f"{'─' * 40}")
        try:
            layout = self._storage_layout()
            try:
                cfg = load_config(_SOURCES_CFG) if _SOURCES_CFG.exists() else AppConfig(storage_root=str(layout.root))
            except Exception:
                cfg = AppConfig(storage_root=str(layout.root))
            simulation = simulate_demo_stage(
                layout,
                cfg,
                stage=stage,
                repo_root=Path.cwd(),
                context=self._tutorial_selection_context(),
            )
            for line in simulation.lines:
                self._root.after(0, self._log_line, line)
                time.sleep(0.06)
            if simulation.artifacts:
                self._root.after(
                    0,
                    self._log_line,
                    "Demo artifacts refreshed: " + ", ".join(simulation.artifacts),
                )
            status = simulation.status
        except Exception as exc:
            self._root.after(0, self._log_line, f"DEMO ERROR: {exc}")
            status = "error"
        self._root.after(0, self._set_status, stage, status)
        self._root.after(0, self._mark_stage_finished, stage, status)
        self._root.after(0, self._log_line, f"[{stage}] {status}.")
        self._root.after(0, self._refresh_overview)
        if stage in {"build-features", "build-training-examples", "build-splits", "train-baseline-model", "evaluate-baseline-model"}:
            self._root.after(0, self._refresh_model_studio)
            self._root.after(0, self._compare_model_runs)
        return status

    def _spawn_stage(self, stage: str) -> None:
        if stage == "build-custom-training-set":
            self._record_demo_action("training.build_custom_set")
        if not self._try_begin_background_run(self._display_stage_name(stage), [stage]):
            return

        def _thread_target() -> None:
            status = "error"
            summary = f"{self._display_stage_name(stage)} failed."
            try:
                status = self._run_stage(stage)
                summary = (
                    f"{self._display_stage_name(stage)} completed."
                    if status == "done"
                    else f"{self._display_stage_name(stage)} was cancelled."
                    if status == "cancelled"
                    else f"{self._display_stage_name(stage)} failed."
                )
            finally:
                self._complete_background_run(status, summary)

        threading.Thread(target=_thread_target, daemon=True).start()

    def _spawn_training_set_workflow(self) -> None:
        stages = ["build-splits", "build-custom-training-set", "build-release"]
        self._record_demo_action("training.run_workflow")
        if not self._try_begin_background_run("Training set workflow", stages):
            return
        threading.Thread(target=self._run_training_set_workflow_thread, daemon=True).start()

    def _run_training_set_workflow_thread(self) -> None:
        self._root.after(0, self._log_line, f"\n{'═' * 50}")
        self._root.after(0, self._log_line, "  RUN TRAINING SET WORKFLOW")
        self._root.after(0, self._log_line, f"{'═' * 50}")
        self._root.after(
            0,
            self._log_line,
            "  Workflow: build-splits -> build-custom-training-set -> build-release",
        )
        try:
            for stage in ("build-splits", "build-custom-training-set", "build-release"):
                status = self._run_stage(stage)
                if status == "error":
                    self._root.after(0, self._log_line, f"\nTraining set workflow stopped at '{stage}'.")
                    self._complete_background_run("error", f"Training set workflow stopped at {self._display_stage_name(stage)}.")
                    return
                if status == "cancelled":
                    self._complete_background_run("cancelled", "Training set workflow was cancelled.")
                    return
            self._root.after(0, self._log_line, f"\n{'═' * 50}")
            self._root.after(0, self._log_line, "  TRAINING SET WORKFLOW COMPLETE")
            self._root.after(0, self._log_line, f"{'═' * 50}")
            self._root.after(
                0,
                self._log_line,
                "  Review custom_training_scorecard.json and custom_training_split_benchmark.csv before shipping the set.",
            )
            self._root.after(0, self._refresh_overview)
            self._complete_background_run("done", "Training set workflow completed.")
        finally:
            pass

    # ------------------------------------------------------------------
    # Ingest — multi-source aware
    # ------------------------------------------------------------------

    def _spawn_ingest(self) -> None:
        if not self._try_begin_background_run("Ingest", ["ingest"]):
            return
        threading.Thread(target=self._run_ingest, kwargs={"finalize_background_run": True}, daemon=True).start()

    def _run_ingest(self, *, finalize_background_run: bool = False) -> str:
        """Background thread: ingest from all enabled sources.

        Returns 'done', 'error', or 'cancelled'.
        """
        if self._closing:
            return "cancelled"
        if bool(self._demo_mode_var.get()):
            status = self._run_demo_stage("ingest")
            if finalize_background_run:
                self._complete_background_run(
                    status,
                    "Ingest finished successfully." if status == "done"
                    else "Ingest was cancelled." if status == "cancelled"
                    else "Ingest finished with errors.",
                )
            return status
        self._root.after(0, self._mark_stage_started, "ingest")
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
        self._root.after(0, self._mark_stage_finished, "ingest", overall_status)
        self._root.after(0, self._refresh_overview)
        if finalize_background_run:
            self._complete_background_run(
                overall_status,
                "Ingest finished successfully." if overall_status == "done"
                else "Ingest was cancelled." if overall_status == "cancelled"
                else "Ingest finished with errors.",
            )
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
        mode = self._pipeline_execution_mode_var.get().strip() or "hybrid"
        planned_stages = ["ingest", *self._pipeline_stages_for_mode(mode)]
        self._record_demo_action("pipeline.run_full")
        if not self._try_begin_background_run("Full pipeline", planned_stages):
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
            status = self._run_ingest(finalize_background_run=False)
            if status == "error":
                self._root.after(0, self._log_line, "\nPipeline stopped: ingest failed.")
                self._complete_background_run("error", "Full pipeline stopped during Ingest Sources.")
                return
            if status == "cancelled":
                self._root.after(0, self._log_line, "\nPipeline cancelled.")
                self._complete_background_run("cancelled", "Full pipeline was cancelled during ingest.")
                return

            mode = self._pipeline_execution_mode_var.get().strip() or "hybrid"
            stages = self._pipeline_stages_for_mode(mode)
            if self._skip_experimental_stages_var.get():
                skipped = [stage for stage in _EXPERIMENTAL_STAGE_KEYS if stage in _SUBPROCESS_STAGES and stage not in stages]
                if skipped:
                    self._root.after(
                        0,
                        self._log_line,
                        f"  Skipping experimental/preview stages: {', '.join(sorted(skipped))}",
                    )

            # Remaining stages via subprocess
            for stage in stages:
                status = self._run_stage(stage)
                if status == "error":
                    self._root.after(
                        0, self._log_line,
                        f"\nPipeline stopped at '{stage}'.",
                    )
                    self._complete_background_run("error", f"Full pipeline stopped at {self._display_stage_name(stage)}.")
                    return
                if status == "cancelled":
                    self._complete_background_run("cancelled", f"Full pipeline was cancelled at {self._display_stage_name(stage)}.")
                    return

            self._root.after(0, self._log_line, f"\n{'═' * 50}")
            self._root.after(0, self._log_line, "  PIPELINE COMPLETE")
            self._root.after(0, self._log_line, f"{'═' * 50}")
            self._root.after(0, self._refresh_overview)
            self._complete_background_run("done", "Full pipeline completed.")
        finally:
            pass


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
