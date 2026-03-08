"""BioLiP source adapter.

BioLiP (https://zhanggroup.org/BioLiP/) is a semi-manually curated
database of biologically relevant ligand-protein interactions derived
from the PDB.

This adapter supports the local flat-file workflow using BioLiP.txt from
the official weekly release. The file format has varied over time, so the
parser accepts either a header row or the common legacy positional layout.
Only stable, high-confidence fields are extracted here.
"""

from __future__ import annotations

import csv
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pbdata.schemas.canonical_sample import CanonicalBindingSample
from pbdata.sources.base import BaseAdapter

_ADAPTER_VERSION = "0.2.0"
_LEGACY_COLUMNS = [
    "pdb_id",
    "receptor_chain",
    "resolution",
    "binding_site_residues",
    "ligand_chain",
    "ligand_id",
    "ligand_serial",
    "binding_affinity",
    "binding_affinity_manual",
    "catalytic_site_residues",
    "ec_number",
    "go_terms",
    "pubmed_id",
]
_AFFINITY_RE = re.compile(
    r"(?P<kind>Kd|Ki|IC50|EC50|dG|ΔG)\s*[:=]?\s*(?P<value>[<>~]?\d+(?:\.\d+)?)\s*(?P<unit>fM|pM|nM|uM|mM|M|kcal/mol)?",
    re.IGNORECASE,
)
_TO_NM = {
    "fm": 0.000001,
    "pm": 0.001,
    "nm": 1.0,
    "um": 1000.0,
    "mm": 1000000.0,
    "m": 1000000000.0,
}


def _normalize_key(name: str) -> str:
    return (
        name.strip()
        .lower()
        .replace(" ", "_")
        .replace("-", "_")
        .replace("(", "")
        .replace(")", "")
        .replace("/", "_")
    )


def _parse_affinity(text: str) -> tuple[str | None, float | None, str | None, float | None]:
    match = _AFFINITY_RE.search((text or "").strip())
    if not match:
        return None, None, None, None

    kind = match.group("kind")
    if kind == "ΔG":
        kind = "dG"
    unit = match.group("unit")
    value_text = match.group("value").lstrip("<>~")
    try:
        value = float(value_text)
    except ValueError:
        return kind, None, unit, None

    standardized = None
    if unit and unit.lower() in _TO_NM:
        standardized = round(value * _TO_NM[unit.lower()], 6)
    elif kind == "dG" and unit:
        standardized = value
    return kind, value, unit, standardized


def _parse_site_residues(text: str) -> tuple[list[str], list[str]]:
    residues: list[str] = []
    residue_names: list[str] = []
    for token in re.split(r"[,\s;]+", (text or "").strip()):
        if not token:
            continue
        cleaned = token.strip()
        residues.append(cleaned)
        letters = "".join(ch for ch in cleaned if ch.isalpha())
        if letters:
            residue_names.append(letters[:3].upper())
    return residues, residue_names


def _read_biolip_rows(path: Path) -> list[dict[str, str]]:
    lines = [
        line for line in path.read_text(encoding="utf-8", errors="ignore").splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    ]
    if not lines:
        return []

    first_parts = lines[0].split("\t")
    has_header = any("pdb" in _normalize_key(part) for part in first_parts)

    rows: list[dict[str, str]] = []
    if has_header:
        reader = csv.DictReader(lines, delimiter="\t")
        for row in reader:
            rows.append({_normalize_key(k): (v or "").strip() for k, v in row.items() if k is not None})
        return rows

    for line in lines:
        parts = line.split("\t")
        row = {
            _LEGACY_COLUMNS[idx]: parts[idx].strip()
            for idx in range(min(len(parts), len(_LEGACY_COLUMNS)))
        }
        rows.append(row)
    return rows


def load_biolip_rows(local_dir: Path) -> list[dict[str, str]]:
    txt_path = local_dir / "BioLiP.txt"
    if not txt_path.exists():
        raise FileNotFoundError(f"BioLiP.txt not found under {local_dir}")
    return _read_biolip_rows(txt_path)


def _first_nonempty(row: dict[str, str], *keys: str) -> str:
    for key in keys:
        value = row.get(key, "").strip()
        if value:
            return value
    return ""


class BioLiPAdapter(BaseAdapter):
    """Adapter for local BioLiP flat-file data."""

    def __init__(self, local_dir: Path | None = None) -> None:
        self._local_dir = local_dir

    @property
    def source_name(self) -> str:
        return "BioLiP"

    def fetch_metadata(self, record_id: str) -> dict[str, Any]:
        if self._local_dir is None:
            raise FileNotFoundError("BioLiP local_dir is required.")
        pdb_id = record_id.upper()
        for row in load_biolip_rows(self._local_dir):
            if _first_nonempty(row, "pdb_id", "pdb")[:4].upper() == pdb_id:
                return row
        raise KeyError(f"BioLiP entry not found for {pdb_id}")

    def normalize_record(self, raw: dict[str, Any]) -> CanonicalBindingSample:
        pdb_id = _first_nonempty(raw, "pdb_id", "pdb")[:4].upper()
        receptor_chain = _first_nonempty(raw, "receptor_chain", "protein_chain", "chain")
        ligand_chain = _first_nonempty(raw, "ligand_chain")
        ligand_id = _first_nonempty(raw, "ligand_id", "ligand_id_ligand")
        ligand_serial = _first_nonempty(raw, "ligand_serial", "ligand_serial_number")
        affinity_text = _first_nonempty(raw, "binding_affinity", "binding_affinity_manual", "affinity")
        affinity_type, affinity_value, affinity_unit, affinity_standardized = _parse_affinity(affinity_text)
        residue_ids, residue_names = _parse_site_residues(_first_nonempty(raw, "binding_site_residues"))
        catalytic_ids, _ = _parse_site_residues(_first_nonempty(raw, "catalytic_site_residues"))

        log10_value = None
        if affinity_standardized is not None and affinity_standardized > 0 and affinity_type != "dG":
            import math
            log10_value = round(math.log10(affinity_standardized), 6)

        sample_slug = ligand_serial or ligand_chain or ligand_id or "UNK"
        return CanonicalBindingSample(
            sample_id=f"BIOLIP_{pdb_id}_{receptor_chain or '-'}_{sample_slug}",
            task_type="protein_ligand",
            source_database="BioLiP",
            source_record_id=f"{pdb_id}:{receptor_chain}:{ligand_chain}:{ligand_id}",
            pdb_id=pdb_id,
            chain_ids_receptor=[receptor_chain] if receptor_chain else None,
            ligand_id=ligand_id or None,
            assay_type=affinity_type,
            assay_value=affinity_value,
            assay_unit=affinity_unit,
            assay_value_standardized=affinity_standardized,
            assay_value_log10=log10_value,
            provenance={
                "source_database": "BioLiP",
                "ingested_at": datetime.now(timezone.utc).isoformat(),
                "adapter_version": _ADAPTER_VERSION,
                "ligand_chain": ligand_chain or None,
                "ligand_serial": ligand_serial or None,
                "binding_site_residue_ids": residue_ids or None,
                "binding_site_residue_names": residue_names or None,
                "catalytic_site_residue_ids": catalytic_ids or None,
                "ec_number": _first_nonempty(raw, "ec_number") or None,
                "go_terms": _first_nonempty(raw, "go_terms") or None,
                "pubmed_id": _first_nonempty(raw, "pubmed_id", "pmid") or None,
                "raw_affinity_text": affinity_text or None,
            },
            quality_flags=[],
            quality_score=0.0,
        )

    def fetch_all(self) -> list[CanonicalBindingSample]:
        if self._local_dir is None:
            raise FileNotFoundError("BioLiP local_dir is required.")
        return [self.normalize_record(row) for row in load_biolip_rows(self._local_dir)]
