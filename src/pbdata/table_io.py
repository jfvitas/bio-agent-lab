"""Shared dataframe and JSON row I/O helpers."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import pandas as pd

_JSON_ROWS_CACHE: dict[str, tuple[tuple[int, int, int], list[dict[str, Any]]]] = {}
_TABLE_ROWS_CACHE: dict[str, tuple[tuple[tuple[str, tuple[int, int, int]], ...], list[dict[str, Any]]]] = {}


def _file_signature(path: Path) -> tuple[int, int, int] | None:
    try:
        stat = path.stat()
    except FileNotFoundError:
        return None
    return (stat.st_mtime_ns, stat.st_ctime_ns, stat.st_size)


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
    signature = _file_signature(path)
    if signature is None:
        return []
    cache_key = str(path.resolve())
    cached = _JSON_ROWS_CACHE.get(cache_key)
    if cached and cached[0] == signature:
        return [dict(row) for row in cached[1]]
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        if logger is not None:
            logger.warning("%s %s: %s", warning_prefix, path.name, exc)
        return []
    if isinstance(raw, list):
        rows = [dict(item) for item in raw if isinstance(item, dict)]
    else:
        rows = [dict(raw)] if isinstance(raw, dict) else []
    _JSON_ROWS_CACHE[cache_key] = (signature, rows)
    return [dict(row) for row in rows]


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
    file_signatures = tuple(
        (str(path.resolve()), signature)
        for path in sorted(table_dir.glob("*.json"))
        if (signature := _file_signature(path)) is not None
    )
    cache_key = str(table_dir.resolve())
    cached = _TABLE_ROWS_CACHE.get(cache_key)
    if cached and cached[0] == file_signatures:
        return [dict(row) for row in cached[1]]
    for path_str, _ in file_signatures:
        rows.extend(load_json_rows(Path(path_str), logger=logger, warning_prefix=warning_prefix))
    _TABLE_ROWS_CACHE[cache_key] = (file_signatures, rows)
    return [dict(row) for row in rows]


def clear_table_io_cache() -> None:
    """Clear cached JSON row and table loads.

    Useful for long-lived GUI sessions, tests, and explicit memory trimming.
    """
    _JSON_ROWS_CACHE.clear()
    _TABLE_ROWS_CACHE.clear()
