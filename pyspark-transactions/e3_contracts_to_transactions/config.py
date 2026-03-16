"""Load pipeline parameters from YAML config file."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml

_DEFAULT_CONFIG_PATH = Path(__file__).resolve().parent.parent / "config" / "parameters.yaml"


def load_parameters(path: str | Path | None = None) -> dict[str, Any]:
    """Read *parameters.yaml* and return it as a plain dict.

    Parameters
    ----------
    path : str | Path | None
        Explicit path to the YAML file.  When *None*, uses the
        ``PIPELINE_CONFIG`` environment variable or falls back to the
        default location ``config/parameters.yaml`` relative to the
        project root.

    Returns
    -------
    dict[str, Any]
        Parsed configuration dictionary.

    Raises
    ------
    FileNotFoundError
        If the resolved path does not exist.
    """
    if path is None:
        path = os.environ.get("PIPELINE_CONFIG", str(_DEFAULT_CONFIG_PATH))
    path = Path(path)

    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")

    with open(path, encoding="utf-8") as fh:
        config: dict[str, Any] = yaml.safe_load(fh)

    _validate(config)
    return config


def _validate(config: dict[str, Any]) -> None:
    """Minimal sanity checks on required keys."""
    required = [
        "source_system",
        "transaction_type_mapping",
        "transaction_type_default",
        "transaction_direction_mapping",
        "date_of_loss_format",
        "creation_date_format",
    ]
    missing = [k for k in required if k not in config]
    if missing:
        raise ValueError(f"Config is missing required keys: {missing}")
