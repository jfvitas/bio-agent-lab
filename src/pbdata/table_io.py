"""Shared dataframe and JSON row I/O helpers."""

from __future__ import annotations

import json
import logging
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


def load_json_rows(
    path: Path,
    *,
    logger: logging.Logger | None = None,
    warning_prefix: str = "Skipping unreadable JSON input",
) -> list[dict[str, Any]]:
    """Return JSON object/list content as a normalized list of dict rows."""
    if not path.exists():
        return []
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        if logger is not None:
            logger.warning("%s %s: %s", warning_prefix, path.name, exc)
        return []
    if isinstance(raw, list):
        return [item for item in raw if isinstance(item, dict)]
    return [raw] if isinstance(raw, dict) else []


def load_table_json(
    table_dir: Path,
    *,
    logger: logging.Logger | None = None,
    warning_prefix: str = "Skipping unreadable JSON input",
) -> list[dict[str, Any]]:
    """Read all `*.json` files in a table directory into one list of dict rows."""
    rows: list[dict[str, Any]] = []
    if not table_dir.exists():
        return rows
    for path in sorted(table_dir.glob("*.json")):
        rows.extend(load_json_rows(path, logger=logger, warning_prefix=warning_prefix))
    return rows
