"""Source-level dashboard smoke contracts that run without launching a browser."""

from __future__ import annotations

import unittest
from pathlib import Path


class DashboardContractTests(unittest.TestCase):
    def test_all_srs_dashboard_routes_and_chart_helpers_are_connected(self) -> None:
        app = (Path(__file__).parents[1] / "src" / "dineiq" / "ui" / "app.py").read_text(encoding="utf-8")
        for route in ("Executive", "Menu", "Customers", "Wastage", "Forecast", "Spark vs Python"):
            self.assertIn(f'active_page == "{route}"', app)
        for helper in ("executive_kpis", "menu_kpis", "customer_kpis", "wastage_kpis", "forecast_kpis", "dual_kpis"):
            self.assertIn(helper, app)
        for chart in ("_render_bar", "_render_line", "_render_scatter", "_render_gauge", "st.area_chart", "_plotly_pie", "Plotly pie chart", "Plotly doughnut chart"):
            self.assertIn(chart, app)

    def test_forecast_history_and_saved_wastage_model_paths_are_present(self) -> None:
        app = (Path(__file__).parents[1] / "src" / "dineiq" / "ui" / "app.py").read_text(encoding="utf-8")
        self.assertIn('"forecast_test"', app)
        self.assertIn("_load_wastage_predictor", app)
        self.assertIn("model_risk_probability", app)


if __name__ == "__main__":
    unittest.main()
