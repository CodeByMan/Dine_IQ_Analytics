"""Environment-backed project configuration; contains no analytics behavior."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _path_from_value(value: str | None, default: Path, project_root: Path) -> Path:
    if not value:
        return default.resolve()
    path = Path(value).expanduser()
    if not path.is_absolute():
        path = project_root / path
    return path.resolve()


@dataclass(frozen=True)
class Settings:
    project_root: Path
    data_root: Path
    artifacts_root: Path
    reports_root: Path

    @property
    def raw_data_dir(self) -> Path:
        return self.data_root / "data"

    @property
    def processed_data_dir(self) -> Path:
        return self.data_root / "processed_data"

    @property
    def parquet_dir(self) -> Path:
        return self.data_root / "parquet"

    @property
    def splits_dir(self) -> Path:
        return self.data_root / "splits"

    @property
    def fixtures_dir(self) -> Path:
        return self.data_root / "fixtures"

    @property
    def models_dir(self) -> Path:
        return self.artifacts_root / "models"

    @property
    def logs_dir(self) -> Path:
        return self.artifacts_root / "logs"

    @property
    def database_path(self) -> Path:
        return self.artifacts_root / "dineiq.sqlite3"


def load_settings(environ: Mapping[str, str] | None = None) -> Settings:
    """Resolve paths from environment variables without touching input data."""
    env = os.environ if environ is None else environ
    root = PROJECT_ROOT.resolve()
    named_dataset_sibling = root.parent / "dataset"
    legacy_dataset_sibling = root.parent / "restaurant_hackathon_dataset"
    packaged_dataset = root / "data" / "source_dataset"
    default_data_root = (
        named_dataset_sibling
        if named_dataset_sibling.is_dir()
        else packaged_dataset
        if (packaged_dataset / "data").is_dir()
        else legacy_dataset_sibling
    )
    return Settings(
        project_root=root,
        data_root=_path_from_value(
            env.get("DINEIQ_DATA_DIR"), default_data_root, root
        ),
        artifacts_root=_path_from_value(
            env.get("DINEIQ_ARTIFACTS_DIR"), root / "artifacts", root
        ),
        reports_root=_path_from_value(
            env.get("DINEIQ_REPORTS_DIR"), root / "reports", root
        ),
    )
