from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from dineiq.config import Settings
from dineiq.ui.catalog import OUTPUT_PATHS, available_output_keys, load_report, output_path


class UICatalogTests(unittest.TestCase):
    def test_catalog_contains_only_current_pipeline_paths(self):
        self.assertTrue(OUTPUT_PATHS)
        self.assertTrue(all("batch" not in str(path).lower() for path in OUTPUT_PATHS.values()))

    def test_missing_outputs_are_reported_as_empty_not_fabricated(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            settings = Settings(root, root / "data", root / "artifacts", root / "reports")
            self.assertEqual(available_output_keys(settings), ())
            report = load_report(settings, "demand_models")
            self.assertFalse(report["available"])
            self.assertFalse(report["passed"])

    def test_output_path_is_project_artifact_path(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            settings = Settings(root, root / "data", root / "artifacts", root / "reports")
            self.assertEqual(output_path(settings, "menu"), root / "artifacts" / "descriptive_analytics" / "menu_profitability_classification")


if __name__ == "__main__":
    unittest.main()
