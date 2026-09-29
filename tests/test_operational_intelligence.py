"""Focused Spark acceptance tests for operational intelligence requirements."""

from __future__ import annotations

import unittest
from datetime import date, timedelta

from pyspark.sql import SparkSession

from dineiq.analytics.anomaly_detection import build_rating_anomalies, build_sales_anomalies
from dineiq.analytics.channel_analysis import build_channel_analysis
from dineiq.analytics.churn_risk import CHURN_FACTORS, build_customer_churn_risk
from dineiq.analytics.location_intelligence import MENU_CLASSES, build_location_comparison, build_location_menu_performance
from dineiq.analytics.promotion_traps import build_promotion_traps
from dineiq.analytics.recommendations import RECOMMENDATION_TYPES, build_recommendations


class OperationalIntelligenceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = (SparkSession.builder.master("local[1]").appName("DineIQ-OperationalIntelligence-Tests")
                     .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1").getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()

    def test_all_five_promotion_traps_are_detected(self) -> None:
        start, end = date(2025, 2, 1), date(2025, 2, 28)
        orders = []
        lines = []
        oid = lid = 0
        for day in range(2, 12):
            oid += 1
            orders.append((oid, f"BASE{day}", None, (start - timedelta(days=day)).isoformat(), "Completed"))
            lid += 1
            lines.append((lid, oid, "HIGH", 10, 90.0))
        for day in range(10):
            oid += 1
            order_date = (start + timedelta(days=day)).isoformat()
            orders.append((oid, f"DUR{day}", None, order_date, "Completed"))
            lid += 1
            lines.append((lid, oid, "HIGH", 2, 18.0))
        for index in range(5):
            oid += 1
            orders.append((oid, f"PROMO{index}", "P1", (start + timedelta(days=4)).isoformat(), "Completed"))
            lid += 1
            lines.append((lid, oid, "LOW", 1, 1.0))
        tables = {
            "orders": self.spark.createDataFrame(orders, ["Order_ID", "Customer_ID", "Promotion_ID", "Order_Date", "Order_Status"]),
            "order_items": self.spark.createDataFrame(lines, ["Order_Item_ID", "Order_ID", "Item_ID", "Quantity", "Gross_Profit"]),
            "menu_items": self.spark.createDataFrame([("HIGH", "CAT"), ("LOW", "CAT")], ["Item_ID", "Category_ID"]),
        }
        effectiveness = self.spark.createDataFrame([(
            "P1", "Campaign", start, end, 20.0, -10.0, 10, 5, 10, 10, 50.0, 1000.0,
            25.0, 200.0, 100.0, 40.0, 10.0,
        )], [
            "Promotion_ID", "Promotion_Name", "start_date", "end_date", "revenue_change_pct", "margin_change_pct",
            "promotion_customers", "pre_customer_count", "pre_order_volume", "promotion_order_volume",
            "promotion_contribution_margin", "pre_contribution_margin", "wastage_cost_change_pct",
            "promotion_revenue", "pre_revenue", "promotion_wastage_cost", "pre_wastage_cost",
        ])
        result = build_promotion_traps(tables, effectiveness)
        row = result.first()
        self.assertTrue(row["promotion_sales_up_profit_down"])
        self.assertTrue(row["promotion_customer_up_margin_collapse"])
        self.assertTrue(row["promotion_increased_wastage"])
        self.assertTrue(row["purchases_only_during_discount_window"])
        self.assertTrue(row["sales_shift_from_more_profitable_product"])

    def test_all_five_rating_anomaly_patterns_are_detected(self) -> None:
        rows = []
        rid = 0

        def add(item: str, day: int, rating: int, count: int, prefix: str) -> None:
            nonlocal rid
            for n in range(count):
                rid += 1
                rows.append((rid, None, None, item, "L1", rating, (date(2025, 1, 1) + timedelta(days=day)).isoformat()))

        for day in range(10):
            add("SPIKE", day, 2, 3, "s")
            add("DROP", day, 5, 3, "d")
        add("SPIKE", 10, 5, 3, "s")
        add("DROP", 10, 1, 3, "d")
        add("IDENTICAL", 1, 4, 10, "i")
        for day in range(8):
            add("BURST", day, 3, 1, "b")
        add("BURST", 8, 3, 10, "b")
        rid += 1
        rows.append((rid, "C1", 900, "BAD_ITEM", "L1", 1, "2025-02-01"))
        ratings = self.spark.createDataFrame(rows,
            ["Rating_ID", "Customer_ID", "Order_ID", "Item_ID", "Location_ID", "Rating", "Review_Date"])
        tables = {
            "ratings": ratings,
            "raw_ratings": ratings,
            "orders": self.spark.createDataFrame([(900, "C1", "Completed")], ["Order_ID", "Customer_ID", "Order_Status"]),
            "order_items": self.spark.createDataFrame([(1, 900, "PURCHASED")], ["Order_Item_ID", "Order_ID", "Item_ID"]),
        }
        result = build_rating_anomalies(tables)
        found = {row["anomaly_type"] for row in result.select("anomaly_type").distinct().collect()}
        self.assertEqual(found, {
            "Sudden rating spike", "Sudden rating drop", "Excessive identical ratings",
            "High rating volume in a short period", "Rating inconsistent with purchasing pattern",
        })

    def test_all_six_sales_anomaly_patterns_are_detected(self) -> None:
        orders = []
        lines = []
        oid = lid = 0
        first = date(2025, 1, 1)

        def sale(item: str, day: int, quantity: int, revenue: float = 100.0, profit: float = 20.0) -> None:
            nonlocal oid, lid
            oid += 1
            order_date = (first + timedelta(days=day)).isoformat()
            orders.append((oid, f"CUS{oid}", "L1", "", order_date, "12:00:00", revenue, "Cash", False, "Completed"))
            lid += 1
            discount = 60.0 if item == "DISCOUNT" else 0.0
            lines.append((lid, oid, item, quantity, 100.0, revenue, profit, discount))

        for day in range(20):
            sale("SPIKE", day, 10)
            sale("DROP", day, 10)
            sale("UNEXPECTED", day, 5 if day % 2 == 0 else 15)
        sale("SPIKE", 20, 100)
        sale("UNEXPECTED", 20, 25)
        sale("DROP", 21, 10)  # Keeps the intervening zero-sales day in the date spine.
        sale("DISCOUNT", 22, 1)
        headers = ["Order_ID", "Customer_ID", "Location_ID", "Promotion_ID", "Order_Date", "Order_Time",
                   "Total_Amount", "Payment_Method", "Is_Duplicate", "Order_Status"]
        for n in range(501):
            oid += 1
            value = 10000.0 if n == 500 else 20.0
            orders.append((oid, f"HIGH{n}", "L1", "", "2025-03-01", f"10:{n % 60:02d}:00", value, "Cash", False, "Completed"))
        # Repeated order-header signature and an explicit raw duplicate flag.
        for n in range(2):
            oid += 1
            orders.append((oid, "DUP_C", "L1", "", "2025-03-02", "15:30:00", 50.0, "Cash", n == 1, "Completed"))
        tables = {
            "orders": self.spark.createDataFrame(orders, headers),
            "raw_orders": self.spark.createDataFrame(orders, headers),
            "order_items": self.spark.createDataFrame(lines,
                ["Order_Item_ID", "Order_ID", "Item_ID", "Quantity", "Unit_Price", "Line_Total",
                 "Gross_Profit", "Discount_Amount"]),
        }
        result = build_sales_anomalies(tables)
        found = {row["anomaly_type"] for row in result.select("anomaly_type").distinct().collect()}
        self.assertEqual(found, {
            "Sudden sales spike", "Sudden sales drop", "Abnormally high order value",
            "Unusual discount", "Unexpected demand", "Duplicate transaction",
        })

    def test_location_channel_and_churn_outputs_match_expected_populations(self) -> None:
        orders = [
            (1, "C1", "L1", None, "2025-05-01", "12:10:00", 40.0, "Completed", 1),
            (2, "C1", "L1", None, "2025-05-15", "13:10:00", 40.0, "Completed", 1),
            (3, "C1", "L1", None, "2025-06-01", "13:20:00", 40.0, "Completed", 1),
            (4, "C2", "L2", "P1", "2025-08-10", "19:15:00", 60.0, "Completed", 2),
            (5, "C2", "L2", None, "2025-09-10", "20:20:00", 60.0, "Completed", 2),
        ]
        order_df = self.spark.createDataFrame(orders,
            ["Order_ID", "Customer_ID", "Location_ID", "Promotion_ID", "Order_Date", "Order_Time",
             "Total_Amount", "Order_Status", "Channel_ID"])
        lines = self.spark.createDataFrame([
            (1, 1, "I1", 2, 40.0, 20.0, 0.0), (2, 2, "I1", 1, 20.0, 8.0, 0.0),
            (3, 3, "I2", 2, 40.0, 15.0, 0.0), (4, 4, "I1", 1, 30.0, 15.0, 2.0),
            (5, 5, "I2", 2, 60.0, 25.0, 0.0),
        ], ["Order_Item_ID", "Order_ID", "Item_ID", "Quantity", "Line_Total", "Gross_Profit", "Discount_Amount"])
        tables = {
            "orders": order_df,
            "order_items": lines,
            "customers": self.spark.createDataFrame([("C1", "2024-01-01"), ("C2", "2025-08-01")], ["Customer_ID", "Signup_Date"]),
            "menu_items": self.spark.createDataFrame([("I1", "Dish 1", "CAT1"), ("I2", "Dish 2", "CAT2")], ["Item_ID", "Item_Name", "Category_ID"]),
            "menu_categories": self.spark.createDataFrame([("CAT1", "Meals"), ("CAT2", "Drinks")], ["Category_ID", "Category_Name"]),
            "ordering_channels": self.spark.createDataFrame([(1, "Dine-in", "at table"), (2, "Delivery", "courier")], ["Channel_ID", "Channel_Name", "Description"]),
            "restaurants": self.spark.createDataFrame([("L1", "One", "A", "P"), ("L2", "Two", "B", "P")], ["Location_ID", "Restaurant_Name", "City", "Province"]),
            "ratings": self.spark.createDataFrame([(1, "I1", "L1", 4), (2, "I2", "L2", 3)], ["Rating_ID", "Item_ID", "Location_ID", "Rating"]),
            "wastage": self.spark.createDataFrame([(1, "I1", "L1", 2.0, 10.0, 5.0), (2, "I2", "L2", 4.0, 10.0, 8.0)], ["Wastage_ID", "Item_ID", "Location_ID", "Quantity_Wasted", "Preparation_Quantity", "Total_Wastage_Cost"]),
        }
        location_result = build_location_comparison(tables)
        menu_result = build_location_menu_performance(tables)
        channel_result = build_channel_analysis(tables)
        churn_result = build_customer_churn_risk(tables)
        self.assertEqual(location_result.count(), 2)
        self.assertEqual(menu_result.count(), 4)
        self.assertTrue(set(row["performance_class"] for row in menu_result.select("performance_class").distinct().collect()).issubset(set(MENU_CLASSES)))
        self.assertEqual(channel_result["channel_summary"].count(), 2)
        self.assertIn("preference_rank", channel_result["channel_category_preferences"].columns)
        self.assertIn("peak_rank", channel_result["channel_peak_periods"].columns)
        self.assertEqual(churn_result.count(), 2)
        self.assertTrue(set(CHURN_FACTORS).issubset(set(churn_result.columns)))
        c1 = churn_result.where("Customer_ID='C1'").first()
        self.assertGreaterEqual(c1["churn_risk_factor_count"], 3)
        self.assertEqual(c1["churn_risk_level"], "High")

    def test_recommendations_cover_all_nine_types_with_evidence_and_priorities(self) -> None:
        menu_rows = [(f"I{i}", f"Dish{i}", "Hidden Opportunity", 100.0, float(i), float(i * 2), i) for i in range(1, 6)]
        menu_rows.append(("LOW", "Low Dish", "Low Performer", 10.0, -5.0, -50.0, 1))
        menu = self.spark.createDataFrame(menu_rows,
            ["Item_ID", "Item_Name", "performance_class", "item_revenue", "contribution_margin", "profit_percentage", "order_frequency"])
        waste = self.spark.createDataFrame([(True, "I1", "L1", 20.0, 25.0, 50.0, 10.0)],
            ["high_wastage_risk", "Item_ID", "Location_ID", "historical_wastage_cost", "historical_wastage_percentage", "historical_quantity_wasted", "risk_cutoff_value"])
        price = self.spark.createDataFrame([("I1", "Highly Price Sensitive", 100.0, -20.0, -1.5, -30.0)],
            ["Item_ID", "price_sensitivity_class", "revenue_before", "revenue_change_pct", "observed_elasticity", "demand_change_pct"])
        bundle = self.spark.createDataFrame([("I1", "I2", 10, 1.5, 0.2, 0.5)],
            ["antecedent_item_id", "consequent_item_id", "pair_order_count", "lift", "support", "confidence"])
        forecast = self.spark.createDataFrame([("I1", "L1", date(2025, 10, 1), 12.0)],
            ["Item_ID", "Location_ID", "Date", "forecast_quantity"])
        segments = self.spark.createDataFrame([("At-Risk", "C1", 100.0, 4), ("Frequent", "C2", 50.0, 6)],
            ["customer_segment", "Customer_ID", "monetary_value", "frequency"])
        traps = self.spark.createDataFrame([("P1", "Campaign", True, False, False, False, False,
            200.0, 100.0, 50.0, 10.0, 20.0, 50.0, 1)],
            ["Promotion_ID", "Promotion_Name", "promotion_sales_up_profit_down", "promotion_customer_up_margin_collapse",
             "promotion_increased_wastage", "purchases_only_during_discount_window", "sales_shift_from_more_profitable_product",
             "promotion_revenue", "pre_revenue", "promotion_wastage_cost", "pre_wastage_cost", "discount_window_only_share",
             "wastage_cost_change_pct", "higher_margin_items_with_declining_sales"])
        locations = self.spark.createDataFrame([("L1", 1000.0, 20.0, -10.0)],
            ["Location_ID", "revenue", "wastage_cost", "promotion_margin_per_order_change_pct"])
        anomalies = self.spark.createDataFrame([("L1", "Sudden sales spike", "I1", date(2025, 9, 1))],
            ["location_id", "anomaly_type", "subject_id", "event_date"])
        result = build_recommendations(menu, waste, price, bundle, forecast, segments, traps, locations, anomalies)
        found = {row["recommendation_type"] for row in result.select("recommendation_type").distinct().collect()}
        self.assertEqual(found, set(RECOMMENDATION_TYPES))
        self.assertEqual(result.where("supporting_evidence IS NULL OR length(trim(supporting_evidence))=0").count(), 0)
        self.assertEqual(result.where("priority NOT IN ('Low','Medium','High','Critical')").count(), 0)
        priorities = {row["priority"] for row in result.where("recommendation_type='Promote high-margin Hidden Opportunities'").select("priority").distinct().collect()}
        self.assertEqual(priorities, {"Low", "Medium", "High", "Critical"})


if __name__ == "__main__":
    unittest.main()
