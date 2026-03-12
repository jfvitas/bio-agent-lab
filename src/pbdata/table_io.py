"""Small dataframe I/O wrapper with a parquet-first, JSON-lines fallback."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd


def write_dataframe(df: pd.DataFrame, path: Path) -> Path:
    """Write a dataframe to parquet, or JSON-lines when parquet support is unavailable."""
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        df.to_parquet(path, index=False)
    except (ImportError, ModuleNotFoundError, ValueError):
        rows = df.to_dict(orient="records")
        with path.open("w", encoding="utf-8") as handle:
            for row in rows:
                handle.write(json.dumps(row) + "\n")
    return path


def read_dataframe(path: Path) -> pd.DataFrame:
    """Read a dataframe from parquet, or JSON-lines fallback if needed."""
    try:
        return pd.read_parquet(path)
    except Exception:
        rows: list[dict[str, Any]] = []
        with path.open(encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                rows.append(json.loads(line))
        return pd.DataFrame(rows)
