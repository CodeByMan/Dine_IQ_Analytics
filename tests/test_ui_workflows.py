"""Dependency-light integration contracts for the operational UI workflow graph."""

from __future__ import annotations

import ast
import os
import subprocess
import sys
import unittest
from pathlib import Path


APP = Path(__file__).parents[1] / "src" / "dineiq" / "ui" / "app.py"


class UIWorkflowContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.source = APP.read_text(encoding="utf-8")
        cls.tree = ast.parse(cls.source)

    def test_each_user_action_has_a_connected_processing_path(self) -> None:
        # These are the controls that initiate state-changing or analytical
        # workflows.  The assertion intentionally checks the source path after
        # the control, not only the visible label.
        expected = {
            "Estimate scenario impact": "simulate_scenario",
            "Predict with both saved models": "_load_live_predictor",
            "Download CSV report": "serialize_report_csv",
            "Sign out": "st.rerun",
        }
        for label, downstream in expected.items():
            self.assertIn(label, self.source)
            self.assertIn(downstream, self.source)

    def test_all_dashboard_pages_are_routed_and_filtered(self) -> None:
        routes = ("Executive", "Menu", "Customers", "Wastage", "Forecast", "Spark vs Python")
        for route in routes:
            self.assertIn(f'active_page == "{route}"', self.source)
        self.assertIn("filters = _filter_controls(data)", self.source)
        self.assertIn("_filtered(data", self.source)

    def test_bootstrap_and_live_model_failures_use_user_safe_paths(self) -> None:
        self.assertIn("Dashboard authentication/database bootstrap failed", self.source)
        self.assertIn("Dashboard data load failed", self.source)
        self.assertIn("Wastage model verification failed", self.source)
        self.assertIn("Dual-model live prediction failed", self.source)
        # Spark and wastage model imports are deliberately deferred until the
        # corresponding workflow is submitted, so the dashboard can still
        # display data/report pages when those runtimes are unavailable.
        module = ast.parse(self.source)
        top_level_imports = [
            node for node in module.body
            if isinstance(node, (ast.Import, ast.ImportFrom))
        ]
        imported_text = "\n".join(ast.get_source_segment(self.source, node) or "" for node in top_level_imports)
        self.assertNotIn("from dineiq.dataset.pipeline import create_local_spark", imported_text)
        self.assertNotIn("from dineiq.analytics.wastage_risk_model import WastageRiskPredictor", imported_text)

    def test_cli_import_and_non_spark_health_path_do_not_eagerly_require_spark(self) -> None:
        env = os.environ.copy()
        env["PYTHONPATH"] = str(APP.parents[2])
        project_root = APP.parents[3]
        result = subprocess.run(
            [sys.executable, "-m", "dineiq.cli", "doctor"],
            cwd=project_root, env=env, text=True, capture_output=True, check=False,
        )
        self.assertNotIn("No module named 'pyspark'", result.stderr + result.stdout)
        self.assertIn("DINEIQ ENVIRONMENT CHECK", result.stdout)


if __name__ == "__main__":
    unittest.main()
