"""dashboard acceptance tests for scenario logic, dashboard data, and SRS UI contracts."""

from __future__ import annotations

import sys
import types
import unittest
from pathlib import Path

import pandas as pd

# These tests exercise pure dashboard data helpers. PyArrow is only stubbed for
# this module import when running helper tests in a minimal development environment.
if "pyarrow.dataset" not in sys.modules:
    try:
        import pyarrow.dataset  # noqa: F401
    except ImportError:
        pyarrow = types.ModuleType("pyarrow")
        pyarrow.__version__ = "18.0.0"
        dataset = types.ModuleType("pyarrow.dataset")
        pyarrow.dataset = dataset
        sys.modules["pyarrow"] = pyarrow
        sys.modules["pyarrow.dataset"] = dataset

from dineiq.analytics.scenario_analysis import (  # noqa: E402
    INDICATORS, SCENARIOS, ScenarioBaseline, simulate_scenario,
)
from dineiq.ui.data import (  # noqa: E402
    DASHBOARD_LABELS, DASHBOARD_REQUIRED_CONTENT, FILTER_LABELS,
    RECOMMENDATION_REQUIREMENT_TYPES, apply_filters, build_scenario_baselines,
    high_value_customer_count, missing_recommendation_requirement_types,
    recency_bucket_counts, repeat_customer_count,
)


class DashboardScenarioTests(unittest.TestCase):
    def setUp(self) -> None:
        self.baseline = ScenarioBaseline(
            forecast_demand=100, selling_price=20, unit_cost=8,
            wastage_rate=0.1, wastage_unit_cost=5, preparation_quantity=110,
            current_discount_pct=5, observed_price_elasticity=-0.5,
        )

    def test_all_eight_scenarios_return_the_five_labeled_indicators(self) -> None:
        inputs = {
            "increase_price": {"change_pct": 10},
            "reduce_price": {"change_pct": 10},
            "change_discount": {"new_discount_pct": 10},
            "promotion_frequency": {"demand_response_pct": 8},
            "remove_item": {},
            "preparation_quantity": {"change_pct": 10},
            "increase_demand": {"change_pct": 10},
            "wastage_assumption": {"new_wastage_rate_pct": 8},
        }
        self.assertEqual(len(SCENARIOS), 8)
        for scenario, kwargs in inputs.items():
            with self.subTest(scenario=scenario):
                result = simulate_scenario(scenario, self.baseline, **kwargs)
                self.assertEqual(result["indicators"], list(INDICATORS))
                self.assertEqual(set(result["estimate"]), set(INDICATORS))
                self.assertEqual(set(result["change"]), set(INDICATORS))
                self.assertIn("ESTIMATE", result["status"])
                self.assertTrue(result["assumptions"])

    def test_scenarios_require_explicit_assumptions_and_validate_bounds(self) -> None:
        no_elasticity = ScenarioBaseline(100, 20, 8, 0.1, 5, 110)
        # Missing elasticity uses the documented default zero response and
        # clearly discloses it; campaign lift requires an explicit assumption.
        result = simulate_scenario("increase_price", no_elasticity, change_pct=10)
        self.assertIn("user-entered demand response", result["assumptions"])
        with self.assertRaises(ValueError):
            simulate_scenario("promotion_frequency", self.baseline)
        with self.assertRaises(ValueError):
            simulate_scenario("wastage_assumption", self.baseline, new_wastage_rate_pct=101)
        with self.assertRaises(ValueError):
            simulate_scenario("increase_demand", self.baseline, change_pct=-100)

    def test_all_eleven_srs_filter_dimensions_reduce_rows(self) -> None:
        frame = pd.DataFrame({
            "Order_Date": ["2025-01-01", "2025-02-01"],
            "Location_ID": ["L1", "L2"], "Item_ID": ["I1", "I2"],
            "Category_ID": ["C1", "C2"], "customer_segment": ["Loyal", "At-Risk"],
            "Channel_ID": ["CH1", "CH2"], "Promotion_ID": ["P1", "P2"],
            "performance_class": ["Profit Driver", "Low Performer"],
            "Selling_Price": [10.0, 30.0], "average_rating": [4.0, 2.0],
            "wastage_percentage": [5.0, 25.0],
        })
        selections = {
            "dates": (pd.Timestamp("2025-01-01").date(), pd.Timestamp("2025-01-31").date()),
            "location": ["L1"], "item": ["I1"], "category": ["C1"],
            "segment": ["Loyal"], "channel": ["CH1"], "promotion": ["P1"],
            "performance": ["Profit Driver"], "price": (0, 20),
            "rating": (3, 5), "wastage": (0, 10),
        }
        self.assertEqual(len(FILTER_LABELS), 11)
        self.assertEqual(len(apply_filters(frame, {"search": "at-risk"})), 1)
        for dimension, selected in selections.items():
            with self.subTest(dimension=dimension):
                filtered = apply_filters(frame, {dimension: selected})
                self.assertEqual(len(filtered), 1)
                self.assertEqual(filtered.iloc[0]["Item_ID"], "I1")
        self.assertEqual(len(apply_filters(frame, selections)), 1)

    def test_required_dashboard_and_kpi_contract_is_present(self) -> None:
        expected_dashboards = {"Executive", "Menu", "Customers", "Wastage", "Forecast", "Spark vs Python"}
        self.assertEqual(set(DASHBOARD_LABELS), expected_dashboards)
        self.assertEqual(len(DASHBOARD_REQUIRED_CONTENT), 6)
        self.assertTrue(all(content for content in DASHBOARD_REQUIRED_CONTENT.values()))
        executive = DASHBOARD_REQUIRED_CONTENT["FR-EX-89 Executive"]
        for required in ("total revenue", "total profit", "total orders", "average order value",
                         "active customers", "repeat customers", "wastage", "forecast demand",
                         "critical recommendations", "anomalies"):
            self.assertIn(required, executive)
        app = Path(__file__).parents[1] / "src/dineiq/ui/app.py"
        self.assertTrue(app.is_file())
        application = app.read_text(encoding="utf-8")
        self.assertIn("st.sidebar.radio", application)
        for label in ("Recommendations", "What-if", "Search and filters", "Spark vs Python"):
            self.assertIn(label, application)

    def test_recommendation_families_cover_menu_inventory_and_customer_targeting(self) -> None:
        all_types = set().union(*RECOMMENDATION_REQUIREMENT_TYPES.values())
        recommendations = pd.DataFrame({"recommendation_type": sorted(all_types)})
        self.assertEqual(missing_recommendation_requirement_types(recommendations), {})
        self.assertEqual(set(RECOMMENDATION_REQUIREMENT_TYPES), {"FR-EX-48", "FR-EX-49", "FR-EX-50"})

    def test_scenario_baselines_join_item_location_and_historical_evidence(self) -> None:
        menu = pd.DataFrame({"Item_ID": ["I1"], "Selling_Price": [20.0], "Ingredient_Cost": [8.0]})
        features = pd.DataFrame({"Item_ID": ["I1"], "discount_percentage": [5.0]})
        forecast = pd.DataFrame({"Item_ID": ["I1"], "Location_ID": ["L1"], "forecast_quantity": [100.0]})
        risk = pd.DataFrame({"Item_ID": ["I1"], "Location_ID": ["L1"],
                             "historical_wastage_percentage": [10.0],
                             "historical_wastage_cost": [50.0],
                             "historical_quantity_wasted": [10.0]})
        sensitivity = pd.DataFrame({"Item_ID": ["I1"], "observed_elasticity": [-0.5]})
        rows = build_scenario_baselines(menu, features, forecast, risk, sensitivity)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows.iloc[0]["Location_ID"], "L1")
        self.assertAlmostEqual(rows.iloc[0]["scenario_wastage_rate"], 0.1)
        self.assertAlmostEqual(rows.iloc[0]["scenario_wastage_unit_cost"], 5.0)
        self.assertAlmostEqual(rows.iloc[0]["observed_elasticity"], -0.5)

    def test_high_value_segment_count_uses_actual_analytics_label(self) -> None:
        segments = pd.DataFrame({"customer_segment": ["High-Value Loyal", "At-Risk", "High-Value Loyal Customers"]})
        self.assertEqual(high_value_customer_count(segments), 2)

    def test_repeat_customer_count_counts_explicit_repeat_labels(self) -> None:
        segments = pd.DataFrame({"repeat_behavior": ["Repeat", "Single/No repeat", "Repeat", None]})
        self.assertEqual(repeat_customer_count(segments), 2)

    def test_customer_recency_chart_buckets_have_altair_safe_numeric_indexes(self) -> None:
        customers = pd.DataFrame({"recency_days": [-1, 10, 73, 146, 219, None]})
        counts = recency_bucket_counts(customers, bins=3)
        self.assertEqual(sum(counts), 5)
        self.assertTrue(all(isinstance(value, int) for value in counts.index))
        self.assertEqual(counts.index.name, "Recency bucket")


if __name__ == "__main__":
    unittest.main()
