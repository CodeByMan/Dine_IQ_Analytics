"""Pure dashboard KPI/chart contracts for Phase 7G."""

from __future__ import annotations

import unittest

import pandas as pd

from dineiq.ui.dashboard_metrics import (
    customer_kpis,
    dated_sum,
    dual_kpis,
    executive_kpis,
    forecast_kpis,
    grouped_count,
    grouped_mean,
    grouped_sum,
    menu_kpis,
    wastage_kpis,
)


class DashboardMetricsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.menu = pd.DataFrame({
            "Item_ID": ["I1", "I2", "I3", "I4"],
            "performance_class": ["Profit Driver", "Volume Driver", "Hidden Opportunity", "Low Performer"],
            "item_revenue": [100.0, 80.0, 60.0, 20.0],
            "contribution_margin": [40.0, 12.0, 30.0, -2.0],
            "order_frequency": [5, 2, 1, 0],
            "average_rating": [4.5, 4.0, 4.2, 2.0],
            "wastage_percentage": [2.0, 8.0, 5.0, 20.0],
        })
        self.customers = pd.DataFrame({
            "Customer_ID": ["C1", "C2", "C3"],
            "customer_segment": ["High-Value Loyal", "At-Risk", "Promotion-Driven"],
            "repeat_behavior": ["Repeat", "Repeat", "Single/No repeat"],
        })
        self.location = pd.DataFrame({
            "Location_ID": ["L1", "L2"], "revenue": [180.0, 80.0],
            "contribution_margin": [60.0, 20.0], "order_count": [10, 4],
            "average_order_value": [18.0, 20.0],
        })

    def test_dashboard_kpis_are_derived_from_input_rows(self) -> None:
        executive = executive_kpis(
            self.menu, self.location, self.customers,
            pd.DataFrame({"wastage_cost": [5.0]}),
            pd.DataFrame({"forecast_quantity": [9.0, 11.0]}),
            pd.DataFrame({"priority": ["Critical", "High"]}),
            pd.DataFrame({"anomaly_type": ["Sudden sales drop"]}),
        )
        self.assertEqual(executive["total_revenue"], 260.0)
        self.assertEqual(executive["total_profit"], 80.0)
        self.assertEqual(executive["total_orders"], 14.0)
        self.assertEqual(executive["active_customers"], 3)
        self.assertEqual(executive["repeat_customers"], 2)
        self.assertEqual(executive["critical_recommendations"], 1)

    def test_menu_customer_and_wastage_contracts(self) -> None:
        menu = menu_kpis(self.menu)
        self.assertEqual(menu["profit_drivers"], 1)
        self.assertEqual(menu["low_performers"], 1)
        self.assertEqual(menu["slow_movers"], 2)
        customers = customer_kpis(self.customers, pd.DataFrame({"frequency": [2, 3], "monetary_value": [10.0, 20.0]}))
        self.assertEqual(customers["high_value"], 1)
        self.assertEqual(customers["at_risk"], 1)
        waste = wastage_kpis(
            pd.DataFrame({"quantity_wasted": [2, 3], "wastage_cost": [5.0, 7.0]}),
            pd.DataFrame({"Item_ID": ["I1", "I2"], "Location_ID": ["L1", "L2"],
                          "high_wastage_risk": [True, False], "model_risk_probability": [0.8, 0.2]}),
        )
        self.assertEqual(waste["quantity"], 5.0)
        self.assertEqual(waste["high_risk_items"], 1)
        self.assertAlmostEqual(waste["mean_model_probability"], 0.5)

    def test_forecast_and_dual_metrics_are_empty_safe(self) -> None:
        forecast = forecast_kpis(
            pd.DataFrame({"forecast_quantity": [10.0, 20.0]}),
            pd.DataFrame({"metric": ["MAE", "R2"], "value": [2.0, 0.5]}),
        )
        self.assertEqual(forecast["forecast_demand"], 30.0)
        self.assertEqual(forecast["high_risk_periods"], 1)
        self.assertEqual(forecast["mae"], 2.0)
        self.assertEqual(forecast["r2"], 0.5)
        dual = dual_kpis(None, pd.DataFrame({"agreement": [True, False], "numerical_difference": [1.0, -3.0]}))
        self.assertEqual(dual["agreement_percentage"], 50.0)
        self.assertEqual(dual["disagreement_count"], 1)
        self.assertEqual(forecast_kpis(pd.DataFrame(), pd.DataFrame())["high_risk_periods"], 0)

    def test_chart_frames_are_real_grouped_or_dated_values(self) -> None:
        self.assertEqual(grouped_sum(self.menu, "performance_class", "item_revenue").iloc[0]["item_revenue"], 100.0)
        self.assertEqual(grouped_mean(self.menu, "performance_class", "average_rating").iloc[0]["average_rating"], 4.5)
        counts = grouped_count(self.customers, "customer_segment")
        self.assertEqual(int(counts["count"].sum()), 3)
        dated = dated_sum(pd.DataFrame({"Date": ["2025-01-01", "2025-01-01"], "value": [1, 2]}), "Date", "value")
        self.assertEqual(dated.iloc[0]["value"], 3)
        self.assertTrue(grouped_sum(pd.DataFrame(), "x", "y").empty)


if __name__ == "__main__":
    unittest.main()
