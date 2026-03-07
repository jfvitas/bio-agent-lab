"""Tkinter GUI for the pbdata pipeline.

Layout
------
Left column  : Data Sources panel + Search Criteria panel
Right column : Pipeline panel (ingest has a search→confirm→download flow)
Bottom row   : Live log panel

The ingest stage is handled directly in Python (no subprocess) so that
the GUI can intercept the entry count and show a confirmation dialog
before any data is downloaded.  All other stages are run via subprocess
so their stdout streams naturally to the log.
"""

from __future__ import annotations

import subprocess
import sys
import threading
from pathlib import Path
from typing import Any, Callable

import tkinter as tk
from tkinter import messagebox, scrolledtext, ttk

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

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_SOURCES = ["rcsb", "bindingdb", "pdbbind", "biolip", "skempi"]

# Stages that run via subprocess (ingest is special-cased)
_SUBPROCESS_STAGES = ["normalize", "audit", "report", "build-splits"]

_CRITERIA_PATH = Path("configs/criteria.yaml")
_SOURCES_CFG   = Path("configs/sources.yaml")

_STATUS_COLORS = {
    "idle":    "#888888",
    "running": "#e6a817",
    "done":    "#4caf50",
    "error":   "#e53935",
}

_HEADER_BG = "#1a237e"
_HEADER_FG = "#ffffff"
_LOG_BG    = "#1e1e1e"
_LOG_FG    = "#d4d4d4"


# ---------------------------------------------------------------------------
# Sources config helpers (read/write sources.yaml)
# ---------------------------------------------------------------------------

def _load_sources_enabled() -> dict[str, bool]:
    if not _SOURCES_CFG.exists():
        return {s: False for s in _SOURCES}
    with _SOURCES_CFG.open() as f:
        raw: dict[str, Any] = yaml.safe_load(f) or {}
    sources = raw.get("sources", {})
    return {s: bool(sources.get(s, {}).get("enabled", False)) for s in _SOURCES}


def _save_sources_enabled(enabled: dict[str, bool]) -> None:
    _SOURCES_CFG.parent.mkdir(parents=True, exist_ok=True)
    if _SOURCES_CFG.exists():
        with _SOURCES_CFG.open() as f:
            raw: dict[str, Any] = yaml.safe_load(f) or {}
    else:
        raw = {}
    sources: dict[str, Any] = raw.setdefault("sources", {})
    for src, val in enabled.items():
        sources.setdefault(src, {})["enabled"] = val
    with _SOURCES_CFG.open("w") as f:
        yaml.safe_dump(raw, f, default_flow_style=False)


# ---------------------------------------------------------------------------
# GUI
# ---------------------------------------------------------------------------

class PbdataGUI:
    """Main application window."""

    def __init__(self, root: tk.Tk) -> None:
        self._root = root
        self._root.title("pbdata — Protein Binding Dataset Pipeline")
        self._root.geometry("1100x760")
        self._root.resizable(True, True)

        # Source enable/disable vars
        self._src_enabled: dict[str, tk.BooleanVar] = {
            s: tk.BooleanVar() for s in _SOURCES
        }

        # Criteria vars
        self._method_vars: dict[str, tk.BooleanVar] = {
            k: tk.BooleanVar() for k in EXPERIMENTAL_METHODS
        }
        self._resolution_var  = tk.StringVar(value="3.0 Å")
        self._task_vars: dict[str, tk.BooleanVar] = {
            "protein_ligand":  tk.BooleanVar(value=True),
            "protein_protein": tk.BooleanVar(value=True),
            "mutation_ddg":    tk.BooleanVar(value=False),
        }
        self._keyword_query_var    = tk.StringVar(value="")
        self._require_protein_var = tk.BooleanVar(value=True)
        self._require_ligand_var  = tk.BooleanVar(value=False)
        self._min_protein_entities_var = tk.StringVar(value="")
        self._max_atom_count_var       = tk.StringVar(value="")
        self._min_year_var        = tk.StringVar(value="")
        self._max_year_var        = tk.StringVar(value="")

        # Pipeline status vars
        all_stages = ["ingest"] + _SUBPROCESS_STAGES
        self._status_vars: dict[str, tk.StringVar] = {
            s: tk.StringVar(value="idle") for s in all_stages
        }
        self._status_labels: dict[str, tk.Label] = {}

        # Serialise "Run All"
        self._running = threading.Lock()

        self._build_ui()
        self._load_sources_into_ui()
        self._load_criteria_into_ui()

    # ------------------------------------------------------------------
    # UI construction
    # ------------------------------------------------------------------

    def _build_ui(self) -> None:
        self._root.columnconfigure(0, weight=0, minsize=235)
        self._root.columnconfigure(1, weight=1)
        self._root.rowconfigure(1, weight=1)
        self._root.rowconfigure(2, weight=2)

        self._build_header()
        self._build_left_column()
        self._build_pipeline_panel()
        self._build_log_panel()

    def _build_header(self) -> None:
        bar = tk.Frame(self._root, bg=_HEADER_BG, pady=10)
        bar.grid(row=0, column=0, columnspan=2, sticky="ew")
        tk.Label(
            bar,
            text="pbdata  ·  Protein Binding Dataset Pipeline",
            fg=_HEADER_FG, bg=_HEADER_BG,
            font=("Helvetica", 13, "bold"),
        ).pack(side="left", padx=16)

    def _build_left_column(self) -> None:
        left = tk.Frame(self._root)
        left.grid(row=1, column=0, sticky="nsew", padx=(10, 4), pady=10)
        left.columnconfigure(0, weight=1)
        left.rowconfigure(0, weight=0)
        left.rowconfigure(1, weight=1)

        self._build_sources_panel(left)
        self._build_criteria_panel(left)

    def _build_sources_panel(self, parent: tk.Frame) -> None:
        frame = ttk.LabelFrame(parent, text="Data Sources", padding=12)
        frame.grid(row=0, column=0, sticky="ew", pady=(0, 6))

        cols = 2
        for i, src in enumerate(_SOURCES):
            ttk.Checkbutton(
                frame, text=src.upper(), variable=self._src_enabled[src]
            ).grid(row=i // cols, column=i % cols, sticky="w", padx=4, pady=2)

        ttk.Button(
            frame, text="Save Config", command=self._save_sources
        ).grid(row=(len(_SOURCES) + 1) // cols + 1, column=0, columnspan=2,
               sticky="ew", pady=(8, 0))

    def _build_criteria_panel(self, parent: tk.Frame) -> None:
        outer = ttk.LabelFrame(parent, text="Search Criteria", padding=8)
        outer.grid(row=1, column=0, sticky="nsew", pady=(6, 0))
        outer.columnconfigure(0, weight=1)
        outer.rowconfigure(0, weight=1)

        canvas = tk.Canvas(outer, highlightthickness=0)
        scrollbar = ttk.Scrollbar(outer, orient="vertical", command=canvas.yview)
        frame = ttk.Frame(canvas, padding=4)

        frame.bind(
            "<Configure>",
            lambda _event: canvas.configure(scrollregion=canvas.bbox("all")),
        )
        canvas.bind(
            "<Configure>",
            lambda event: canvas.itemconfigure(window_id, width=event.width),
        )
        window_id = canvas.create_window((0, 0), window=frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)

        canvas.grid(row=0, column=0, sticky="nsew")
        scrollbar.grid(row=0, column=1, sticky="ns")

        frame.columnconfigure(1, weight=1)
        row = 0

        ttk.Label(frame, text="Keywords / text search:").grid(
            row=row, column=0, sticky="w"
        )
        ttk.Entry(frame, textvariable=self._keyword_query_var).grid(
            row=row, column=1, sticky="ew", padx=(6, 0)
        )
        row += 1

        ttk.Separator(frame, orient="horizontal").grid(
            row=row, column=0, columnspan=2, sticky="ew", pady=6
        )
        row += 1

        # --- Experimental methods ---
        ttk.Label(frame, text="Methods:", font=("Helvetica", 9, "bold")).grid(
            row=row, column=0, columnspan=2, sticky="w", pady=(0, 2)
        )
        row += 1
        method_labels = {
            "xray":    "X-Ray",
            "em":      "Cryo-EM",
            "nmr":     "NMR",
            "neutron": "Neutron",
        }
        for i, (key, label) in enumerate(method_labels.items()):
            ttk.Checkbutton(
                frame, text=label, variable=self._method_vars[key]
            ).grid(row=row + i // 2, column=i % 2, sticky="w", pady=1)
        row += 2

        ttk.Separator(frame, orient="horizontal").grid(
            row=row, column=0, columnspan=2, sticky="ew", pady=6
        )
        row += 1

        # --- Resolution ---
        ttk.Label(frame, text="Max Resolution:").grid(
            row=row, column=0, sticky="w"
        )
        res_combo = ttk.Combobox(
            frame,
            textvariable=self._resolution_var,
            values=RESOLUTION_OPTIONS,
            width=10,
            state="readonly",
        )
        res_combo.grid(row=row, column=1, sticky="w", padx=(6, 0))
        row += 1

        ttk.Separator(frame, orient="horizontal").grid(
            row=row, column=0, columnspan=2, sticky="ew", pady=6
        )
        row += 1

        # --- Task types ---
        ttk.Label(frame, text="Task Types:", font=("Helvetica", 9, "bold")).grid(
            row=row, column=0, columnspan=2, sticky="w", pady=(0, 2)
        )
        row += 1
        task_labels = {
            "protein_ligand":  "Protein + Ligand",
            "protein_protein": "Protein + Protein",
            "mutation_ddg":    "Mutation ΔΔG",
        }
        for key, label in task_labels.items():
            ttk.Checkbutton(
                frame, text=label, variable=self._task_vars[key]
            ).grid(row=row, column=0, columnspan=2, sticky="w", pady=1)
            row += 1

        ttk.Separator(frame, orient="horizontal").grid(
            row=row, column=0, columnspan=2, sticky="ew", pady=6
        )
        row += 1

        # --- Other filters ---
        ttk.Checkbutton(
            frame, text="Require protein entity", variable=self._require_protein_var
        ).grid(row=row, column=0, columnspan=2, sticky="w")
        row += 1

        ttk.Checkbutton(
            frame, text="Require ligand / non-polymer", variable=self._require_ligand_var
        ).grid(row=row, column=0, columnspan=2, sticky="w", pady=(4, 0))
        row += 1

        ttk.Label(frame, text="Min protein entities:").grid(
            row=row, column=0, sticky="w", pady=(6, 0)
        )
        ttk.Entry(frame, textvariable=self._min_protein_entities_var, width=8).grid(
            row=row, column=1, sticky="w", padx=(6, 0), pady=(6, 0)
        )
        row += 1

        ttk.Label(frame, text="Max deposited atoms:").grid(
            row=row, column=0, sticky="w", pady=(6, 0)
        )
        ttk.Entry(frame, textvariable=self._max_atom_count_var, width=12).grid(
            row=row, column=1, sticky="w", padx=(6, 0), pady=(6, 0)
        )
        row += 1

        ttk.Label(frame, text="Min release year:").grid(
            row=row, column=0, sticky="w", pady=(6, 0)
        )
        ttk.Entry(frame, textvariable=self._min_year_var, width=8).grid(
            row=row, column=1, sticky="w", padx=(6, 0), pady=(6, 0)
        )
        row += 1

        ttk.Label(frame, text="Max release year:").grid(
            row=row, column=0, sticky="w", pady=(6, 0)
        )
        ttk.Entry(frame, textvariable=self._max_year_var, width=8).grid(
            row=row, column=1, sticky="w", padx=(6, 0), pady=(6, 0)
        )
        row += 1

        ttk.Button(
            frame, text="Save Criteria", command=self._save_criteria
        ).grid(row=row, column=0, columnspan=2, sticky="ew", pady=(12, 0))

    def _build_pipeline_panel(self) -> None:
        frame = ttk.LabelFrame(self._root, text="Pipeline", padding=16)
        frame.grid(row=1, column=1, sticky="nsew", padx=(4, 10), pady=10)
        frame.columnconfigure(1, weight=1)
        row = 0

        # --- Ingest (special: search → confirm → download) ---
        ttk.Label(
            frame,
            text="Step 1 — Find & Download",
            font=("Helvetica", 10, "bold"),
        ).grid(row=row, column=0, columnspan=3, sticky="w", pady=(0, 6))
        row += 1

        ttk.Button(
            frame,
            text="🔍  Search & Download (RCSB)",
            width=30,
            command=self._spawn_ingest,
        ).grid(row=row, column=0, sticky="w", pady=4, padx=(0, 12))
        ingest_lbl = tk.Label(
            frame,
            textvariable=self._status_vars["ingest"],
            width=12,
            anchor="w",
            fg=_STATUS_COLORS["idle"],
            font=("Helvetica", 9),
        )
        ingest_lbl.grid(row=row, column=1, sticky="w")
        self._status_labels["ingest"] = ingest_lbl
        row += 1

        ttk.Separator(frame, orient="horizontal").grid(
            row=row, column=0, columnspan=3, sticky="ew", pady=(10, 8)
        )
        row += 1

        # --- Remaining pipeline stages ---
        ttk.Label(
            frame,
            text="Step 2 — Process & Analyse",
            font=("Helvetica", 10, "bold"),
        ).grid(row=row, column=0, columnspan=3, sticky="w", pady=(0, 6))
        row += 1

        for stage in _SUBPROCESS_STAGES:
            ttk.Button(
                frame,
                text=f"▶  {stage}",
                width=22,
                command=lambda s=stage: self._spawn_stage(s),
            ).grid(row=row, column=0, sticky="w", pady=4, padx=(0, 12))

            lbl = tk.Label(
                frame,
                textvariable=self._status_vars[stage],
                width=12,
                anchor="w",
                fg=_STATUS_COLORS["idle"],
                font=("Helvetica", 9),
            )
            lbl.grid(row=row, column=1, sticky="w")
            self._status_labels[stage] = lbl
            row += 1

        ttk.Separator(frame, orient="horizontal").grid(
            row=row, column=0, columnspan=3, sticky="ew", pady=(10, 8)
        )
        row += 1

        ttk.Button(
            frame,
            text="▶▶  Run Full Pipeline",
            command=self._spawn_all,
        ).grid(row=row, column=0, columnspan=2, sticky="ew")

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

        ttk.Button(frame, text="Clear", command=self._clear_log).grid(
            row=1, column=0, sticky="e", pady=(5, 0)
        )

    # ------------------------------------------------------------------
    # Config persistence
    # ------------------------------------------------------------------

    def _load_sources_into_ui(self) -> None:
        enabled = _load_sources_enabled()
        for src, var in self._src_enabled.items():
            var.set(enabled.get(src, False))

    def _save_sources(self) -> None:
        _save_sources_enabled({s: v.get() for s, v in self._src_enabled.items()})
        self._log_line("Source config saved.")

    def _load_criteria_into_ui(self) -> None:
        sc = load_criteria(_CRITERIA_PATH)
        self._keyword_query_var.set(sc.keyword_query or "")
        for key, var in self._method_vars.items():
            var.set(key in sc.experimental_methods)
        self._resolution_var.set(resolution_value_to_label(sc.max_resolution_angstrom))
        for key, var in self._task_vars.items():
            var.set(key in sc.task_types)
        self._require_protein_var.set(sc.require_protein)
        self._require_ligand_var.set(sc.require_ligand)
        self._min_protein_entities_var.set(
            "" if sc.min_protein_entities is None else str(sc.min_protein_entities)
        )
        self._max_atom_count_var.set(
            "" if sc.max_deposited_atom_count is None else str(sc.max_deposited_atom_count)
        )
        self._min_year_var.set("" if sc.min_release_year is None else str(sc.min_release_year))
        self._max_year_var.set("" if sc.max_release_year is None else str(sc.max_release_year))

    def _criteria_from_ui(self) -> SearchCriteria:
        methods = [k for k, v in self._method_vars.items() if v.get()]
        task_types = [k for k, v in self._task_vars.items() if v.get()]
        keyword_query = self._keyword_query_var.get().strip() or None
        min_protein_str = self._min_protein_entities_var.get().strip()
        max_atom_str = self._max_atom_count_var.get().strip()
        min_year_str = self._min_year_var.get().strip()
        max_year_str = self._max_year_var.get().strip()
        min_protein_entities: int | None = int(min_protein_str) if min_protein_str.isdigit() else None
        max_deposited_atom_count: int | None = int(max_atom_str) if max_atom_str.isdigit() else None
        min_year: int | None = int(min_year_str) if min_year_str.isdigit() else None
        max_year: int | None = int(max_year_str) if max_year_str.isdigit() else None
        return SearchCriteria(
            keyword_query=keyword_query,
            experimental_methods=methods,
            max_resolution_angstrom=resolution_label_to_value(self._resolution_var.get()),
            task_types=task_types,
            require_protein=self._require_protein_var.get(),
            require_ligand=self._require_ligand_var.get(),
            min_protein_entities=min_protein_entities,
            max_deposited_atom_count=max_deposited_atom_count,
            min_release_year=min_year,
            max_release_year=max_year,
        )

    def _save_criteria(self) -> None:
        sc = self._criteria_from_ui()
        save_criteria(sc, _CRITERIA_PATH)
        self._log_line("Criteria saved to configs/criteria.yaml")

    # ------------------------------------------------------------------
    # Logging
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

    # ------------------------------------------------------------------
    # Status
    # ------------------------------------------------------------------

    def _set_status(self, stage: str, status: str) -> None:
        self._status_vars[stage].set(status)
        self._status_labels[stage].configure(fg=_STATUS_COLORS.get(status, "#888"))

    # ------------------------------------------------------------------
    # Subprocess stages (normalize / audit / report / build-splits)
    # ------------------------------------------------------------------

    def _run_stage(self, stage: str) -> str:
        """Run one CLI stage via subprocess (call from a background thread).

        Returns 'done' or 'error'.
        """
        self._root.after(0, self._set_status, stage, "running")
        self._root.after(0, self._log_line, f"\n─── {stage} ───")

        cmd = [sys.executable, "-m", "pbdata.cli", stage]
        try:
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                cwd=Path.cwd(),
            )
            for line in proc.stdout:  # type: ignore[union-attr]
                self._root.after(0, self._log_line, line)
            proc.wait()
            status = "done" if proc.returncode == 0 else "error"
        except Exception as exc:
            self._root.after(0, self._log_line, f"ERROR: {exc}")
            status = "error"

        self._root.after(0, self._set_status, stage, status)
        self._root.after(0, self._log_line, f"[{stage}] {status}.")
        return status

    def _spawn_stage(self, stage: str) -> None:
        threading.Thread(target=self._run_stage, args=(stage,), daemon=True).start()

    # ------------------------------------------------------------------
    # Ingest — search → confirm → download (runs in-process, not subprocess)
    # ------------------------------------------------------------------

    def _spawn_ingest(self) -> None:
        threading.Thread(target=self._run_ingest, daemon=True).start()

    def _run_ingest(self) -> str:
        """Background thread: count → confirm dialog → download.

        Returns 'done', 'error', or 'cancelled'.
        """
        from pbdata.sources.rcsb_search import count_entries, search_and_download

        self._root.after(0, self._set_status, "ingest", "running")
        self._root.after(0, self._log_line, "\n─── Search & Download (RCSB) ───")

        # Save current criteria first
        sc = self._criteria_from_ui()
        save_criteria(sc, _CRITERIA_PATH)

        # -- Step 1: count --
        self._root.after(0, self._log_line, "Querying RCSB Search API...")
        try:
            count = count_entries(sc)
        except Exception as exc:
            self._root.after(0, self._log_line, f"Search failed: {exc}")
            self._root.after(0, self._set_status, "ingest", "error")
            return "error"

        self._root.after(0, self._log_line, f"Found {count:,} matching entries.")

        # -- Step 2: confirm (must run on main thread via threading.Event) --
        proceed_flag: list[bool] = [False]
        event = threading.Event()

        def _ask_user() -> None:
            size_warning = (
                "\n\n⚠  Large download — this may take a while."
                if count > 5_000
                else ""
            )
            proceed_flag[0] = messagebox.askyesno(
                "Confirm Download",
                f"Found {count:,} entries matching your criteria.\n\n"
                f"Proceed with download?{size_warning}",
            )
            event.set()

        self._root.after(0, _ask_user)
        event.wait()  # block the background thread until the user responds

        if not proceed_flag[0]:
            self._root.after(0, self._log_line, "Download cancelled by user.")
            self._root.after(0, self._set_status, "ingest", "idle")
            return "cancelled"

        # -- Step 3: download --
        raw_dir = Path("data/raw/rcsb")
        self._root.after(0, self._log_line, f"Downloading to {raw_dir} ...")

        def _log(msg: str) -> None:
            self._root.after(0, self._log_line, msg)

        try:
            search_and_download(sc, raw_dir, log_fn=_log)
            self._root.after(0, self._set_status, "ingest", "done")
            return "done"
        except Exception as exc:
            self._root.after(0, self._log_line, f"Download failed: {exc}")
            self._root.after(0, self._set_status, "ingest", "error")
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
        self._root.after(0, self._log_line, "\n═══ Run Full Pipeline ═══")
        try:
            # Ingest (in-process with confirm dialog)
            status = self._run_ingest()
            if status == "error":
                self._root.after(0, self._log_line, "Pipeline stopped: ingest failed.")
                return
            if status == "cancelled":
                self._root.after(0, self._log_line, "Pipeline cancelled.")
                return

            # Remaining stages via subprocess
            for stage in _SUBPROCESS_STAGES:
                status = self._run_stage(stage)
                if status == "error":
                    self._root.after(
                        0, self._log_line,
                        f"Pipeline stopped at '{stage}'.",
                    )
                    return

            self._root.after(0, self._log_line, "\n═══ Pipeline complete. ═══")
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
