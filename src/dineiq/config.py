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
    processed_root: Path | None = None

    @property
    def raw_data_dir(self) -> Path:
        return self.data_root

    @property
    def processed_data_dir(self) -> Path:
        return self.processed_root or (self.project_root / "processed_data")

    @property
    def parquet_dir(self) -> Path:
        return self.artifacts_root

    @property
    def splits_dir(self) -> Path:
        return self.processed_data_dir / "splits"

    @property
    def fixtures_dir(self) -> Path:
        return self.project_root / "tests" / "fixtures"

    @property
    def models_dir(self) -> Path:
        return self.project_root / "models"

    @property
    def logs_dir(self) -> Path:
        return self.reports_root / "spark" / "logs"

    @property
    def database_path(self) -> Path:
        return self.project_root / "database" / "dineiq.sqlite3"


def load_settings(environ: Mapping[str, str] | None = None) -> Settings:
    """Resolve paths from environment variables without touching input data."""
    env = os.environ if environ is None else environ
    root = PROJECT_ROOT.resolve()
    return Settings(
        project_root=root,
        data_root=_path_from_value(
            env.get("DINEIQ_DATA_DIR"), root / "raw_data", root
        ),
        artifacts_root=_path_from_value(
            env.get("DINEIQ_ARTIFACTS_DIR"), root / "parquet_data", root
        ),
        reports_root=_path_from_value(
            env.get("DINEIQ_REPORTS_DIR"), root / "reports", root
        ),
        processed_root=_path_from_value(
            env.get("DINEIQ_PROCESSED_DIR"), root / "processed_data", root
        ),
    )
