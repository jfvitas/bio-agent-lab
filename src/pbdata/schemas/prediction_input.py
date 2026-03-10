"""Prediction-input schema for workflow normalization."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, model_validator

_SMILES_ALLOWED = re.compile(r"^[A-Za-z0-9@+\-\[\]\(\)=#$:/\\.%,]+$")
_SMILES_ATOM = re.compile(r"(Cl|Br|[BCNOPSFIKH]|[cnops])")


def _balanced(text: str, opening: str, closing: str) -> bool:
    depth = 0
    for char in text:
        if char == opening:
            depth += 1
        elif char == closing:
            depth -= 1
            if depth < 0:
                return False
    return depth == 0


class PredictionInputRecord(BaseModel):
    model_config = ConfigDict(frozen=True)

    input_type: Literal["smiles", "sdf", "pdb", "mmcif", "fasta"]
    input_value: str
    target_id: str | None = None

    @model_validator(mode="after")
    def _validate_input(self) -> "PredictionInputRecord":
        value = self.input_value.strip()
        if not value:
            raise ValueError("input_value must not be empty")

        if self.input_type == "smiles":
            if any(ch in value for ch in "\r\n"):
                raise ValueError("SMILES input must be a single-line string")
            # Conservative validation only: reject obviously non-SMILES tokens
            # without pretending to do full cheminformatics parsing.
            if not _SMILES_ALLOWED.fullmatch(value):
                raise ValueError("SMILES input contains invalid characters")
            if not _balanced(value, "(", ")") or not _balanced(value, "[", "]"):
                raise ValueError("SMILES input has unbalanced parentheses or brackets")
            if not _SMILES_ATOM.search(value):
                raise ValueError("SMILES input must contain at least one recognizable atom token")
            return self

        path = Path(value)
        if self.input_type == "sdf":
            if path.exists() and path.is_file():
                return self
            if path.suffix.lower() == ".sdf":
                raise ValueError(f"SDF file not found: {value}")
            if "M END" not in value and "$$$$" not in value:
                raise ValueError("SDF input must be a readable file path or contain SDF content")
            return self

        if self.input_type == "pdb":
            if path.exists() and path.is_file() and path.suffix.lower() == ".pdb":
                return self
            if path.suffix.lower() == ".pdb":
                raise ValueError(f"PDB structure file not found: {value}")
            if not any(token in value for token in ("ATOM", "HETATM", "HEADER")):
                raise ValueError("PDB input must be a .pdb path or contain PDB-style records")
            return self

        if self.input_type == "mmcif":
            if path.exists() and path.is_file() and path.suffix.lower() in {".cif", ".mmcif"}:
                return self
            if path.suffix.lower() in {".cif", ".mmcif"}:
                raise ValueError(f"mmCIF structure file not found: {value}")
            if "data_" not in value:
                raise ValueError("mmCIF input must be a .cif/.mmcif path or contain mmCIF text")
            return self

        if self.input_type == "fasta":
            if path.exists() and path.is_file() and path.suffix.lower() in {".fa", ".fasta", ".faa"}:
                return self
            if path.suffix.lower() in {".fa", ".fasta", ".faa"}:
                raise ValueError(f"FASTA file not found: {value}")
            compact = value.replace("\r", "").strip()
            if not compact.startswith(">") and not compact.isalpha():
                raise ValueError("FASTA input must be a FASTA path, FASTA text, or raw sequence")
            return self

        return self
