"""Creation of project-owned output directories only."""

from __future__ import annotations

from pathlib import Path

from dineiq.config import Settings


def ensure_runtime_dirs(settings: Settings) -> tuple[Path, ...]:
    """Create runtime destinations; never create or mutate directories in data_root."""
    destinations = (settings.artifacts_root, settings.models_dir, settings.logs_dir,
                    settings.reports_root)
    for path in destinations:
        path.mkdir(parents=True, exist_ok=True)
    return destinations
