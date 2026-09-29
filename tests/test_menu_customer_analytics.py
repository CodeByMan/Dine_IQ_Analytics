"""Small Spark acceptance tests for the descriptive analytics SRS analytics scope."""

from __future__ import annotations

import unittest
from datetime import date, datetime

from pyspark.sql import SparkSession, functions as F

from dineiq.analytics.customer_segments import CUSTOMER_SEGMENTS, build_customer_rfm_and_segments
from dineiq.analytics.menu_performance import MENU_CLASSES, TRICKY_CASE_FLAGS, add_performance_class, add_tricky_case_flags, build_menu_performance
from dineiq.analytics.rating_analysis import build_rating_analysis
from dineiq.analytics.time_patterns import build_time_patterns
from dineiq.analytics.wastage_analysis import build_wastage_analysis


class DescriptiveAnalyticsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = (SparkSession.builder.master("local[1]").appName("DineIQ-DescriptiveAnalytics-Tests")
                     .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1").getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()

    def _tables(self):
        orders = self.spark.createDataFrame([
            (1, 1, 1, "", 1, date(2025, 4, 30), "10:00:00", datetime(2025, 4, 30, 10), "Completed", 100.0),
            (2, 1, 1, "5", 2, date(2025, 5, 1), "18:00:00", datetime(2025, 5, 1, 18), "Completed", 200.0),
            (3, 2, 2, "", 1, date(2025, 5, 1), "13:00:00", datetime(2025, 5, 1, 13), "Completed", 50.0),
        ], ["Order_ID", "Customer_ID", "Location_ID", "Promotion_ID", "Channel_ID", "Order_Date", "Order_Time", "Order_DateTime", "Order_Status", "Total_Amount"])
        lines = self.spark.createDataFrame([
            (1, 1, 10, 2, 100.0, 40.0, date(2025, 4, 30)),
            (2, 2, 20, 3, 150.0, 45.0, date(2025, 5, 1)),
            (3, 3, 30, 1, 50.0, 10.0, date(2025, 5, 1)),
        ], ["Order_Item_ID", "Order_ID", "Item_ID", "Quantity", "Line_Total", "Gross_Profit", "Order_Date"])
        items = self.spark.createDataFrame([
            (10, "Dish A", 1, 500.0, 200.0, date(2024, 1, 1)),
            (20, "Dish B", 1, 300.0, 150.0, date(2024, 1, 1)),
            (30, "Dish C", 2, 250.0, 100.0, date(2024, 1, 1)),
            (40, "Dish D", 2, 100.0, 90.0, date(2024, 1, 1)),
        ], ["Item_ID", "Item_Name", "Category_ID", "Selling_Price", "Ingredient_Cost", "Launch_Date"])
        customers = self.spark.createDataFrame([
            (1, date(2024, 1, 1)), (2, date(2025, 4, 30)), (3, date(2024, 1, 1)),
        ], ["Customer_ID", "Signup_Date"])
        ratings = self.spark.createDataFrame([
            (10, 1, 5), (20, 1, 3), (30, 2, 4),
        ], ["Item_ID", "Location_ID", "Rating"])
        wastage = self.spark.createDataFrame([
            (1, 10, 1, date(2025, 4, 1), 2.0, 20.0, 10.0, "Overproduction", "Evening"),
            (2, 20, 2, date(2025, 5, 1), 1.0, 15.0, 10.0, "Expiry", "Morning"),
        ], ["Wastage_ID", "Item_ID", "Location_ID", "Wastage_Date", "Quantity_Wasted", "Total_Wastage_Cost", "Preparation_Quantity", "Wastage_Reason", "Shift"])
        restaurants = self.spark.createDataFrame([
            (1, "Location One", 4.0), (2, "Location Two", 3.5),
        ], ["Location_ID", "Restaurant_Name", "Average_Rating"])
        return {
            "orders": orders, "order_items": lines, "menu_items": items,
            "customers": customers, "ratings": ratings, "wastage": wastage,
            "restaurants": restaurants,
        }

    def _menu_features(self):
        values = [
            (10, 1000.0, 200.0, 800.0, 40.0, 20, 100, 4.5, 0.8, 10.0, 0.2, 5.0),
            (20, 1000.0, 700.0, 300.0, 10.0, 16, 90, 3.4, 0.4, 20.0, 0.2, 10.0),
            (30, 500.0, 300.0, 200.0, 30.0, 5, 30, 5.0, 0.9, 15.0, 0.2, 5.0),
            (40, 0.0, 0.0, 0.0, 0.0, 0, 0, None, None, 0.0, 0.0, None),
        ]
        columns = ["Item_ID", "item_revenue", "cost", "contribution_margin", "profit_percentage", "order_frequency", "item_popularity", "average_rating", "repeat_purchase_rate", "promotion_dependency", "weekend_order_ratio", "wastage_percentage"]
        return self.spark.createDataFrame(values, columns)

    def test_menu_outputs_every_item_with_one_of_four_classes_and_profit_metrics(self) -> None:
        tables = self._tables()
        location_features = self.spark.createDataFrame([
            (10, 1, 400.0), (10, 2, 600.0), (20, 1, 800.0), (20, 2, 200.0),
            (30, 1, 450.0), (30, 2, 50.0), (40, 1, 20.0), (40, 2, 10.0),
        ], ["Item_ID", "Location_ID", "item_location_revenue"])
        result = build_menu_performance(tables, self._menu_features(), location_features)
        classified = result["menu_profitability_classification"]
        self.assertEqual(classified.count(), 4)
        rows = {row["Item_ID"]: row.asDict() for row in classified.collect()}
        self.assertEqual(rows[10]["performance_class"], "Profit Driver")
        self.assertEqual(rows[20]["performance_class"], "Volume Driver")
        self.assertEqual(rows[30]["performance_class"], "Hidden Opportunity")
        self.assertEqual(rows[40]["performance_class"], "Low Performer")
        self.assertTrue(set(row["performance_class"] for row in rows.values()).issubset(set(MENU_CLASSES)))
        self.assertEqual(rows[10]["cost"], 200.0)
        self.assertEqual(set(result["tricky_menu_cases"].columns[2:]), set(TRICKY_CASE_FLAGS))

    def test_performance_class_rule_supports_all_four_named_classes(self) -> None:
        rows = [
            (1, 0.9, 0.8, 0.3, 10.0),
            (2, 0.1, 0.8, 0.2, 2.0),
            (3, 0.7, 0.2, 0.8, 5.0),
            (4, 0.2, 0.2, 0.1, 1.0),
        ]
        frame = self.spark.createDataFrame(rows, ["Item_ID", "profitability_score", "demand_score", "opportunity_score", "contribution_margin"])
        result = {row["Item_ID"]: row["performance_class"] for row in add_performance_class(frame).collect()}
        self.assertEqual(set(result.values()), set(MENU_CLASSES))

    def test_all_ten_named_tricky_menu_cases_have_executable_flags(self) -> None:
        fields = ["demand_score", "contribution_margin", "profitability_score", "order_frequency", "wastage_percentile", "average_rating", "promotion_dependency", "location_cv_percentile", "location_count", "weekend_order_ratio", "active_month_count", "top_three_month_share", "item_age_days"]
        base = [0.1, 1.0, 0.1, 5, 0.1, 3.0, 0.0, 0.1, 1, 0.2, 2, 0.4, 500]
        changes = [
            {0: 0.9, 1: -1.0}, {2: 0.9, 3: 1}, {0: 0.9, 4: 0.9},
            {5: 4.5, 2: 0.2}, {5: 2.0, 0: 0.9}, {6: 60.0},
            {7: 0.9, 8: 2}, {9: 1.0}, {10: 6, 11: 0.8}, {12: 30},
        ]
        records = []
        for change in changes:
            row = base.copy()
            for index, value in change.items(): row[index] = value
            records.append(tuple(row))
        output = add_tricky_case_flags(self.spark.createDataFrame(records, fields))
        row = output.agg(*[F.max(F.col(name).cast("int")).alias(name) for name in TRICKY_CASE_FLAGS]).first().asDict()
        self.assertEqual(set(TRICKY_CASE_FLAGS), set(row))
        self.assertTrue(all(bool(row[name]) for name in TRICKY_CASE_FLAGS))

    def test_customer_rfm_and_all_ten_behavior_factors(self) -> None:
        outputs = build_customer_rfm_and_segments(self._tables())
        rfm = outputs["customer_rfm"].where("Customer_ID = 1").first().asDict()
        self.assertEqual(rfm["recency_days"], 0)
        self.assertEqual(rfm["frequency"], 2)
        self.assertEqual(rfm["monetary_value"], 300.0)
        factors = outputs["customer_segments"]
        self.assertEqual(factors.count(), 3)
        required = {
            "recency_days", "frequency", "monetary_value", "average_order_value", "visit_frequency",
            "favorite_category_id", "promotion_sensitivity", "preferred_channel_id",
            "preferred_time_of_day", "repeat_behavior",
        }
        self.assertTrue(required.issubset(set(factors.columns)))
        self.assertTrue(set(row[0] for row in factors.select("customer_segment").distinct().collect()).issubset(set(CUSTOMER_SEGMENTS)))
        customer_one = factors.where("Customer_ID = 1").first().asDict()
        self.assertEqual(customer_one["average_order_value"], 150.0)
        self.assertEqual(customer_one["repeat_behavior"], "Repeat")

    def test_hour_weekday_and_seasonality_outputs_execute(self) -> None:
        result = build_time_patterns(self._tables())
        self.assertEqual(set(result), {"hourly_order_patterns", "weekday_order_patterns", "seasonal_order_patterns", "monthly_order_trend", "peak_period_summary"})
        self.assertGreaterEqual(result["peak_period_summary"].count(), 3)
        self.assertTrue({"weekend", "restaurant_location", "ordering_channel"}.issubset(
            {row["period_type"] for row in result["peak_period_summary"].collect()}
        ))
        self.assertTrue(result["seasonal_order_patterns"].count() > 0)

    def test_wastage_trends_report_quantity_cost_and_denominator(self) -> None:
        result = build_wastage_analysis(self._tables())
        by_item = result["wastage_by_item"].where("Item_ID = 10").first().asDict()
        self.assertEqual(by_item["quantity_wasted"], 2.0)
        self.assertEqual(by_item["wastage_cost"], 20.0)
        self.assertEqual(by_item["preparation_quantity"], 10.0)
        self.assertAlmostEqual(by_item["wastage_percentage"], 20.0)
        self.assertTrue({"wastage_monthly_trend", "wastage_by_item", "wastage_by_location", "wastage_by_reason_shift",
                         "wastage_by_category", "wastage_by_day", "wastage_by_demand",
                         "wastage_by_inventory", "wastage_by_promotion"}.issubset(set(result)))

    def test_ratings_are_compared_with_item_and_location_sales(self) -> None:
        result = build_rating_analysis(self._tables())
        self.assertEqual(set(result), {"item_rating_performance", "location_rating_performance", "item_location_rating_performance"})
        item = result["item_rating_performance"].where("Item_ID = 10").first().asDict()
        self.assertEqual(item["average_customer_rating"], 5.0)
        self.assertEqual(item["item_revenue"], 100.0)
        location = result["location_rating_performance"].where("Location_ID = 1").first().asDict()
        self.assertEqual(location["location_order_count"], 2)
        self.assertEqual(result["item_location_rating_performance"].count(), 3)


if __name__ == "__main__":
    unittest.main()
