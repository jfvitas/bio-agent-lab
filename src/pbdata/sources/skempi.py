"""SKEMPI v2 source adapter.

SKEMPI (Structural Kinetic and Energetic database of Mutant Protein Interactions)
provides thermodynamic data (ddG, Kd, Ka) for point mutations at protein-protein
interfaces.

Data source:
  https://life.bsc.es/pid/skempi2/database/download/skempi_v2.csv
  (CSV, semicolon-delimited, ~7700 rows as of 2024-01)

Each row in the CSV maps to a single CanonicalBindingSample with:
  - task_type = "mutation_ddg"
  - wildtype_or_mutant = "mutant"
  - mutation_string populated from the '#Mutation(s)_cleaned' column
  - assay_value = ddG in kcal/mol (computed from dKd or provided directly)
  - assay_unit = "kcal/mol"

ddG calculation:
  If 'affinity_mut (M)' and 'affinity_wt (M)' are both present:
    ddG = RT * ln(Kd_mut / Kd_wt)   (R = 1.987e-3 kcal/(mol·K))
  If 'ddG (kcal/mol)' is present, use it directly.
  Temperature defaults to 25°C (298.15 K) when 'Temperature' is missing.
"""

from __future__ import annotations

import csv
import io
import math
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

from pbdata.schemas.canonical_sample import CanonicalBindingSample
from pbdata.sources.base import BaseAdapter

_ADAPTER_VERSION = "0.1.0"
_SKEMPI_URL = (
    "https://life.bsc.es/pid/skempi2/database/download/skempi_v2.csv"
)
_TIMEOUT = 60  # seconds
_R = 1.987e-3  # kcal/(mol·K)
_DEFAULT_TEMP_K = 298.15  # 25 °C


# ---------------------------------------------------------------------------
# Row-level parsing helpers
# ---------------------------------------------------------------------------

def _parse_kd(value: str) -> float | None:
    """Parse a Kd string (in molar) to a float.  Returns None on failure."""
    cleaned = value.strip().lstrip("<>=~").strip()
    try:
        return float(cleaned)
    except (ValueError, TypeError):
        return None


def _compute_ddg(row: dict[str, str]) -> tuple[float | None, float | None]:
    """Compute ddG (kcal/mol) and temperature (°C) from a SKEMPI row.

    Returns (ddG_kcal_mol, temperature_celsius).
    """
    temp_raw = row.get("Temperature", "").strip()
    try:
        temp_k = float(temp_raw)
    except (ValueError, TypeError):
        temp_k = _DEFAULT_TEMP_K
    temp_c = temp_k - 273.15

    # Direct ddG column (some rows supply it)
    ddg_raw = row.get("ddG (kcal/mol)", "").strip()
    try:
        return float(ddg_raw), temp_c
    except (ValueError, TypeError):
        pass

    # Compute from Kd ratio
    kd_mut = _parse_kd(row.get("affinity_mut (M)", ""))
    kd_wt  = _parse_kd(row.get("affinity_wt (M)", ""))
    if kd_mut is not None and kd_wt is not None and kd_wt > 0 and kd_mut > 0:
        ddg = _R * temp_k * math.log(kd_mut / kd_wt)
        return round(ddg, 6), temp_c

    return None, temp_c


def _parse_row(row: dict[str, str], row_index: int) -> CanonicalBindingSample | None:
    """Convert one SKEMPI CSV row to a CanonicalBindingSample.

    Returns None for rows that cannot be meaningfully represented.
    """
    pdb_id       = (row.get("#Pdb") or row.get("Pdb") or "").strip().upper()[:4]
    mutation_raw = (row.get("#Mutation(s)_cleaned") or row.get("Mutation(s)_cleaned") or "").strip()
    protein_1    = row.get("Protein 1", "").strip()
    protein_2    = row.get("Protein 2", "").strip()

    if not pdb_id or not mutation_raw:
        return None

    ddg, temp_c = _compute_ddg(row)
    log10: float | None = None  # ddG can be negative, log10 is not meaningful

    # Unique sample ID: PDB + sanitised mutation string + row index (tie-break)
    mut_slug = mutation_raw.replace(",", "_").replace(" ", "")[:60]
    sample_id = f"SKEMPI_{pdb_id}_{mut_slug}_{row_index}"

    provenance: dict[str, Any] = {
        "source_database": "SKEMPI",
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "adapter_version": _ADAPTER_VERSION,
        "protein_1": protein_1,
        "protein_2": protein_2,
        "skempi_row": row_index,
    }

    return CanonicalBindingSample(
        sample_id=sample_id,
        task_type="mutation_ddg",
        source_database="SKEMPI",
        source_record_id=f"{pdb_id}:{mutation_raw}",
        pdb_id=pdb_id,
        mutation_string=mutation_raw,
        wildtype_or_mutant="mutant",
        assay_type="ddG",
        assay_value=ddg,
        assay_unit="kcal/mol",
        assay_value_standardized=ddg,   # already in canonical unit for ddG
        assay_value_log10=log10,
        temperature_c=round(temp_c, 2) if temp_c is not None else None,
        provenance=provenance,
        quality_flags=[],
        quality_score=0.0,
    )


# ---------------------------------------------------------------------------
# CSV loading helpers
# ---------------------------------------------------------------------------

def _iter_rows(text: str):
    """Yield dicts from a SKEMPI v2 CSV (semicolon-delimited, first col header)."""
    reader = csv.DictReader(io.StringIO(text), delimiter=";")
    yield from reader


def load_skempi_csv(
    path: Path | None = None,
    *,
    download: bool = True,
) -> list[CanonicalBindingSample]:
    """Load SKEMPI v2 CSV and return all parseable records.

    Args:
        path:     Optional local path to an already-downloaded CSV file.
                  If None and download=True, the file is fetched from the
                  canonical SKEMPI URL.
        download: Whether to download if path is absent.

    Returns:
        List of CanonicalBindingSample records.
    """
    if path is not None and Path(path).exists():
        text = Path(path).read_text(encoding="utf-8")
    elif download:
        resp = requests.get(_SKEMPI_URL, timeout=_TIMEOUT)
        resp.raise_for_status()
        text = resp.text
    else:
        raise FileNotFoundError(
            f"SKEMPI CSV not found at {path!r} and download=False."
        )

    records: list[CanonicalBindingSample] = []
    for i, row in enumerate(_iter_rows(text)):
        rec = _parse_row(row, i)
        if rec is not None:
            records.append(rec)
    return records


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------

class SKEMPIAdapter(BaseAdapter):
    """Adapter for SKEMPI v2 mutation ddG data.

    fetch_metadata() downloads the full CSV (cached to local_path if given).
    normalize_record() converts a single CSV row dict to a sample.
    Use load_skempi_csv() for bulk loading.
    """

    def __init__(self, local_path: Path | None = None) -> None:
        self._local_path = local_path

    @property
    def source_name(self) -> str:
        return "SKEMPI"

    def fetch_metadata(self, record_id: str) -> dict[str, Any]:
        """Fetch the full SKEMPI CSV and return it as a dict with key 'csv_text'.

        record_id is ignored — SKEMPI is delivered as a single bulk CSV.
        """
        if self._local_path and Path(self._local_path).exists():
            text = Path(self._local_path).read_text(encoding="utf-8")
        else:
            resp = requests.get(_SKEMPI_URL, timeout=_TIMEOUT)
            resp.raise_for_status()
            text = resp.text
            if self._local_path:
                Path(self._local_path).parent.mkdir(parents=True, exist_ok=True)
                Path(self._local_path).write_text(text, encoding="utf-8")
        return {"csv_text": text}

    def normalize_record(self, raw: dict[str, Any]) -> CanonicalBindingSample:
        """Expect raw = {'row': <dict>, 'row_index': <int>}.

        For bulk use, call load_skempi_csv() directly.
        """
        row = raw.get("row") or {}
        idx = raw.get("row_index", 0)
        rec = _parse_row(row, idx)
        if rec is None:
            raise ValueError(f"Could not parse SKEMPI row {idx}: {row}")
        return rec

    def fetch_all(self) -> list[CanonicalBindingSample]:
        """Download (or load from local_path) and return all SKEMPI records."""
        return load_skempi_csv(self._local_path, download=True)
