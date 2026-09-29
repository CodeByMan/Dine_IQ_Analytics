"""Acceptance tests for demand planning SRS analytics using compact Spark fixtures."""

from __future__ import annotations

import unittest
from datetime import date, timedelta
import tempfile
from pathlib import Path

from pyspark.sql import SparkSession

from dineiq.analytics.demand_forecast import build_demand_forecasts
from dineiq.analytics.market_basket import build_market_basket
from dineiq.analytics.price_sensitivity import build_price_sensitivity
from dineiq.analytics.promotion_effectiveness import build_promotion_effectiveness
from dineiq.analytics.wastage_risk import build_wastage_risk
from dineiq.analytics.wastage_risk_model import MODEL_VERSION, WastageRiskPredictor, train_wastage_risk_model


class DemandPlanningTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = (SparkSession.builder.master("local[1]").appName("DineIQ-DemandPlanning-Tests")
                     .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1").getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()

    def test_market_basket_metrics_and_directed_bundle_candidates(self) -> None:
        orders = self.spark.createDataFrame([
            (1, "Completed"), (2, "Completed"), (3, "Completed"), (4, "Cancelled"),
        ], ["Order_ID", "Order_Status"])
        lines = self.spark.createDataFrame([
            (1, 1, "A", 1), (2, 1, "B", 1), (3, 2, "A", 1), (4, 2, "B", 1),
            (5, 3, "A", 1), (6, 3, "C", 1), (7, 4, "B", 1), (8, 4, "C", 1),
        ], ["Order_Item_ID", "Order_ID", "Item_ID", "Quantity"])
        menu = self.spark.createDataFrame([
            ("A", "Dish A", "C1", 10.0), ("B", "Dish B", "C1", 20.0),
            ("C", "Dish C", "C2", 5.0),
        ], ["Item_ID", "Item_Name", "Category_ID", "Selling_Price"])
        out = build_market_basket({"orders": orders, "order_items": lines, "menu_items": menu}, min_pair_orders=1)
        ab = out["association_rules"].where("antecedent_item_id='A' AND consequent_item_id='B'").first()
        self.assertEqual(ab["pair_order_count"], 2)
        self.assertAlmostEqual(ab["support"], 2 / 3)
        self.assertAlmostEqual(ab["confidence"], 2 / 3)
        self.assertAlmostEqual(ab["lift"], 1.0)
        self.assertTrue(out["bundle_recommendations"].count() > 0)
        kinds = {r["recommendation_type"] for r in out["bundle_recommendations"].collect()}
        self.assertEqual(kinds, {"Upsell opportunity", "Combo meal / frequently paired dish", "Cross-sell opportunity"})

    def test_demand_forecast_is_chronological_configurable_and_reports_metrics(self) -> None:
        start = date(2025, 1, 1)
        data = [(start + timedelta(days=i), "I1", "L1", float((i % 7) + 1)) for i in range(21)]
        schema = ["Date", "Item_ID", "Location_ID", "Target_Quantity"]
        train = self.spark.createDataFrame(data[:10], schema)
        validation = self.spark.createDataFrame(data[10:15], schema)
        test = self.spark.createDataFrame(data[15:], schema)
        items = self.spark.createDataFrame([("I1", "C1")], ["Item_ID", "Category_ID"])
        result = build_demand_forecasts(train, validation, test, items, horizon_days=4)
        self.assertEqual(result["forecast_evaluation"].count(), 8)
        self.assertEqual(result["forecast_item_location_daily"].count(), 4)
        self.assertEqual(result["forecast_category_daily"].count(), 4)
        self.assertEqual(result["forecast_location_daily"].count(), 4)
        metrics = {r["metric"] for r in result["forecast_evaluation"].select("metric").distinct().collect()}
        self.assertEqual(metrics, {"MAE", "RMSE", "MAPE_nonzero_actuals", "R2"})
        with self.assertRaisesRegex(ValueError, "chronological"):
            build_demand_forecasts(validation, train, test, items, horizon_days=4)

    def test_wastage_risk_uses_historical_rates_and_marks_risk_population(self) -> None:
        waste = self.spark.createDataFrame([
            (1, "I1", "L1", "2025-01-01", 1.0, 100.0, 10.0),
            (2, "I2", "L1", "2025-01-01", 10.0, 100.0, 20.0),
            (3, "I3", "L1", "2025-01-01", 20.0, 100.0, 30.0),
            (4, "I4", "L1", "2025-01-01", 40.0, 100.0, 40.0),
        ], ["Wastage_ID", "Item_ID", "Location_ID", "Wastage_Date", "Quantity_Wasted", "Preparation_Quantity", "Total_Wastage_Cost"])
        features = self.spark.createDataFrame([(f"I{i}", float(i * 10)) for i in range(1, 5)], ["Item_ID", "item_popularity"])
        result = build_wastage_risk({"wastage": waste}, features)
        self.assertEqual(result.count(), 4)
        high = result.where("high_wastage_risk").collect()
        self.assertTrue(high)
        self.assertIn("I4", {row["Item_ID"] for row in high})

    def test_wastage_model_persists_reloads_and_predicts_latest_item_location_rows(self) -> None:
        rows = []
        record_id = 0
        start = date(2025, 1, 1)
        for day in range(18):
            for item, offset in (("I1", 0.00), ("I2", 0.10), ("I3", 0.20)):
                record_id += 1
                rate = 0.05 + offset + (day % 4) * 0.04
                rows.append((record_id, item, "L1", (start + timedelta(days=day)).isoformat(), rate * 100.0, 100.0, rate * 100.0))
        waste = self.spark.createDataFrame(rows, [
            "Wastage_ID", "Item_ID", "Location_ID", "Wastage_Date", "Quantity_Wasted",
            "Preparation_Quantity", "Total_Wastage_Cost",
        ])
        menu_features = self.spark.createDataFrame(
            [("I1", 10.0), ("I2", 20.0), ("I3", 30.0)], ["Item_ID", "item_popularity"]
        )
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            result = train_wastage_risk_model(
                {"wastage": waste}, menu_features,
                root / "wastage_risk.joblib", root / "wastage_model.json",
            )
            self.assertEqual(result["summary"]["model_version"], MODEL_VERSION)
            self.assertTrue(result["summary"]["loaded_persisted_model_for_inference"])
            self.assertTrue((root / "wastage_risk.joblib").is_file())
            self.assertTrue((root / "wastage_model.json").is_file())
            self.assertEqual(result["predictions"].count(), 3)
            self.assertIn("model_risk_probability", result["predictions"].columns)
            self.assertIn("model_high_wastage_risk", result["predictions"].columns)
            persisted_rows = result["predictions"].toPandas()
            predictor = WastageRiskPredictor.load(root / "wastage_risk.joblib")
            live = predictor.predict(persisted_rows)
            self.assertEqual(set(live["model_version"]), {MODEL_VERSION})
            self.assertTrue(
                (abs(live["model_risk_probability"] - persisted_rows["model_risk_probability"]) < 1e-8).all()
            )

    def test_price_sensitivity_uses_before_after_windows_and_three_classes(self) -> None:
        event_date = date(2025, 2, 1)
        order_rows, line_rows = [], []
        order_id = line_id = 0
        for offset in range(-30, 30):
            for item_id, daily_units in (("I1", 10 if offset < 0 else 5),
                                         ("I2", 10 if offset < 0 else 9),
                                         ("I3", 10 if offset < 0 else 9),
                                         ("I4", 10)):
                order_id += 1
                order_rows.append((order_id, f"C{order_id % 5}", (event_date + timedelta(days=offset)).isoformat(), "Completed"))
                line_id += 1
                line_rows.append((line_id, order_id, item_id, daily_units, daily_units * 12.0, daily_units * 5.0, daily_units * 0.5, (event_date + timedelta(days=offset)).isoformat()))
        tables = {
            "orders": self.spark.createDataFrame(order_rows, ["Order_ID", "Customer_ID", "Order_Date", "Order_Status"]),
            "order_items": self.spark.createDataFrame(line_rows,
                ["Order_Item_ID", "Order_ID", "Item_ID", "Quantity", "Line_Total", "Gross_Profit", "Discount_Amount", "Order_Date"]),
            "pricing_history": self.spark.createDataFrame([
                ("P1", "I1", event_date.isoformat(), 10.0, 12.0),
                ("P2", "I2", event_date.isoformat(), 10.0, 12.0),
                ("P3", "I3", event_date.isoformat(), 10.0, 11.0),
                ("P4", "I4", event_date.isoformat(), 10.0, 10.0),
            ],
                ["Price_History_ID", "Item_ID", "Effective_From", "Previous_Price", "New_Price"]),
            "ratings": self.spark.createDataFrame([("I1", 4.0), ("I2", 3.0), ("I3", 5.0), ("I4", 4.0)],
                ["Item_ID", "Rating"]),
        }
        result = build_price_sensitivity(tables)
        rows = {row["Item_ID"]: row.asDict() for row in result.collect()}
        self.assertAlmostEqual(rows["I1"]["observed_elasticity"], -2.5)
        self.assertEqual(rows["I1"]["price_sensitivity_class"], "Highly Price Sensitive")
        self.assertEqual(rows["I2"]["price_sensitivity_class"], "Low Price Sensitivity")
        self.assertEqual(rows["I3"]["price_sensitivity_class"], "Moderately Price Sensitive")
        self.assertEqual(rows["I4"]["price_sensitivity_class"], "Insufficient evidence")
        self.assertEqual(rows["I1"]["average_rating"], 4.0)
        self.assertIn("revenue_change_pct", rows["I1"])
        self.assertIn("contribution_margin_change_pct", rows["I1"])
        self.assertIn("discount_amount_change", rows["I1"])
        self.assertIn("repeat_purchase_rate", rows["I1"])

    def test_promotion_effectiveness_uses_multiple_kpis_and_campaign_population(self) -> None:
        orders = self.spark.createDataFrame([
            (1, "C1", "L1", None, "2025-01-05", "Completed", 50.0),
            (2, "C1", "L1", "P1", "2025-02-05", "Completed", 100.0),
            (3, "C2", "L1", "P1", "2025-02-06", "Completed", 100.0),
            (4, "C1", "L1", None, "2025-03-10", "Completed", 80.0),
        ], ["Order_ID", "Customer_ID", "Location_ID", "Promotion_ID", "Order_Date", "Order_Status", "Total_Amount"])
        promotions = self.spark.createDataFrame([("P1", "Campaign", "2025-02-01", "2025-02-28")],
            ["Promotion_ID", "Promotion_Name", "Start_Date", "End_Date"])
        lines = self.spark.createDataFrame([(1, 1, 1, 20.0), (2, 2, 1, 5.0), (3, 3, 1, 5.0), (4, 4, 1, 20.0)],
            ["Order_Item_ID", "Order_ID", "Quantity", "Gross_Profit"])
        wastage = self.spark.createDataFrame([
            (1, "2025-01-05", 1.0, 5.0), (2, "2025-02-05", 2.0, 10.0),
        ],
            ["Wastage_ID", "Wastage_Date", "Quantity_Wasted", "Total_Wastage_Cost"])
        result = build_promotion_effectiveness({"orders": orders, "promotions": promotions, "order_items": lines, "wastage": wastage})
        self.assertEqual(result.count(), 1)
        row = result.first()
        self.assertEqual(row["promotion_order_volume"], 2)
        self.assertEqual(row["promotion_revenue"], 200.0)
        self.assertEqual(row["new_customers_during_promotion"], 1)
        self.assertEqual(row["post_repeat_customers"], 1)
        self.assertEqual(row["promotion_wastage_cost"], 10.0)
        self.assertAlmostEqual(row["wastage_cost_change_pct"], 100.0)
        self.assertIn("trade-off", row["multi_kpi_assessment"])
        self.assertFalse(row["causal_effect_claimed"])


if __name__ == "__main__":
    unittest.main()
