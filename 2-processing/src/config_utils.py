"""Shared helpers for loading the processing configuration and its paths."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml


def load_project_config(config_path: Path) -> dict[str, Any]:
    """Load YAML and resolve configured paths relative to that config file."""
    config_path = Path(config_path).expanduser().resolve()
    with config_path.open(encoding="utf-8") as config_file:
        config = yaml.safe_load(config_file)
    if not isinstance(config, dict):
        raise ValueError(f"Configuration must be a YAML mapping: {config_path}")

    paths = config.get("paths")
    if not isinstance(paths, dict):
        raise ValueError(f"Configuration is missing the 'paths' mapping: {config_path}")

    for key, value in paths.items():
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"Configuration path '{key}' must be a non-empty string")
        resolved = Path(value).expanduser()
        if not resolved.is_absolute():
            resolved = config_path.parent / resolved
        paths[key] = str(resolved.resolve())
    return config
