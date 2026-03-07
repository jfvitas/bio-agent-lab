"""Logging setup for pbdata.

Configures Python logging from a YAML dictConfig file or falls back to
a sensible default when no config path is provided.
"""

import logging
import logging.config
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, ConfigDict

_DEFAULT_FORMAT = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"


class LoggingConfig(BaseModel):
    """Minimal validation wrapper for Python logging dictConfig dicts.

    Only 'version' is structurally required by the dictConfig spec.
    All other keys are passed through unchanged.
    """

    model_config = ConfigDict(extra="allow")

    version: Literal[1]
    disable_existing_loggers: bool = False


def setup_logging(config_path: str | Path | None = None) -> None:
    """Configure logging for the application.

    Args:
        config_path: Path to a YAML logging config (dictConfig format).
            If None, a default INFO-level console handler is used.

    Raises:
        FileNotFoundError: If config_path is given but does not exist.
        ValueError: If the YAML content is not a valid dictConfig structure.
    """
    if config_path is None:
        logging.basicConfig(level=logging.INFO, format=_DEFAULT_FORMAT)
        return

    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Logging config not found: {path}")
    with path.open() as f:
        raw: dict[str, Any] = yaml.safe_load(f) or {}

    # Validate structure before handing off to dictConfig.
    LoggingConfig.model_validate(raw)
    logging.config.dictConfig(raw)
