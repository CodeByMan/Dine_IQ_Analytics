"""Tests for configuration and non-destructive initialization only."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from dineiq.config import PROJECT_ROOT, Settings, load_settings
from dineiq.paths import ensure_runtime_dirs


class BootstrapTests(unittest.TestCase):
    def test_default_dataset_uses_named_sibling_when_available(self) -> None:
        settings = load_settings(environ={})
        parent = PROJECT_ROOT.parent
        named = parent / "dataset"
        legacy = parent / "restaurant_hackathon_dataset"
        packaged = PROJECT_ROOT / "data" / "source_dataset"
        expected = named if named.is_dir() else packaged if (packaged / "data").is_dir() else legacy
        self.assertEqual(settings.data_root, expected.resolve())

    def test_named_dataset_sibling_is_preferred_when_present(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            parent = Path(tmp)
            project = parent / "DineIQ_Analytics"
            project.mkdir()
            (parent / "dataset").mkdir()
            with patch("dineiq.config.PROJECT_ROOT", project):
                settings = load_settings(environ={})
            self.assertEqual(settings.data_root, (parent / "dataset").resolve())

    def test_relative_environment_paths_resolve_from_project_root(self) -> None:
        settings = load_settings(environ={
            "DINEIQ_DATA_DIR": "external/data",
            "DINEIQ_ARTIFACTS_DIR": "build/artifacts",
            "DINEIQ_REPORTS_DIR": "build/reports",
        })
        self.assertEqual(settings.data_root, (PROJECT_ROOT / "external/data").resolve())
        self.assertEqual(settings.artifacts_root, (PROJECT_ROOT / "build/artifacts").resolve())
        self.assertEqual(settings.reports_root, (PROJECT_ROOT / "build/reports").resolve())

    def test_initialization_creates_only_project_output_directories(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            data = root / "source_dataset"
            data.mkdir()
            outputs = root / "app_outputs"
            reports = root / "reports"
            settings = Settings(project_root=root, data_root=data,
                                artifacts_root=outputs, reports_root=reports)
            created = ensure_runtime_dirs(settings)
            self.assertEqual(len(created), 4)
            self.assertTrue(all(path.is_dir() for path in created))
            self.assertEqual(list(data.iterdir()), [])


if __name__ == "__main__":
    unittest.main()
