from __future__ import annotations

import csv
import json
import tempfile
import unittest
from datetime import date, datetime
from pathlib import Path

from pyspark.sql import SparkSession

from dineiq.analytics.eda import EDA_OUTPUTS, build_eda_outputs
from dineiq.dataset.cleaning import clean_tables
from dineiq.dataset.ingest import read_table, validate_schema
from dineiq.dataset.integrate import build_required_joins
from dineiq.dataset.quality import ISSUE_TYPES, detect_fixture_record, fixture_coverage
from dineiq.dataset.schema import TABLE_SCHEMAS, expected_schema
from dineiq.features.engineering import SRS_FEATURES, build_feature_tables


class DataFoundationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = (SparkSession.builder.master("local[1]").appName("DineIQ-DataFoundation-Tests")
                     .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1").getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()

    def test_schema_catalog_covers_all_twelve_dataset_1b_source_tables(self) -> None:
        self.assertEqual(len(TABLE_SCHEMAS), 12)
        self.assertNotIn("order_channel", TABLE_SCHEMAS)
        self.assertIn("Channel", TABLE_SCHEMAS["orders"].columns)
        for table, spec in TABLE_SCHEMAS.items():
            self.assertIn(spec.primary_key, spec.columns)
            self.assertEqual(len(spec.columns), len(set(spec.columns)))
            self.assertEqual(expected_schema(table).fieldNames(), list(spec.columns))

    def test_schema_validator_detects_missing_column(self) -> None:
        good = self.spark.createDataFrame([("C01", "Soup")], ["Category_ID", "Category_Name"])
        self.assertTrue(validate_schema("menu_categories", good).passed)
        bad = self.spark.createDataFrame([("C01",)], ["Category_ID"])
        check = validate_schema("menu_categories", bad)
        self.assertFalse(check.passed)
        self.assertEqual(check.missing_columns, ("Category_Name",))

    def test_documented_string_identifiers_remain_strings_during_ingestion(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            data_dir = Path(temporary) / "data"
            data_dir.mkdir()
            expected_values = {
                "inventory": ("Inventory_ID", "INV0000001", "Restaurant_ID", "R001"),
                "restaurants": ("Restaurant_ID", "R001", "Restaurant_ID", "R001"),
            }
            for table, (key_column, key_value, id_column, id_value) in expected_values.items():
                spec = TABLE_SCHEMAS[table]
                values = {column: "" for column in spec.columns}
                values.update({key_column: key_value, id_column: id_value})
                with (data_dir / spec.filename).open("w", newline="", encoding="utf-8") as stream:
                    writer = csv.DictWriter(stream, fieldnames=spec.columns)
                    writer.writeheader()
                    writer.writerow(values)
            inventory = read_table(self.spark, Path(temporary), "inventory")
            restaurants = read_table(self.spark, Path(temporary), "restaurants")
            self.assertEqual(inventory.schema["Inventory_ID"].dataType.simpleString(), "string")
            self.assertEqual(inventory.select("Inventory_ID").first()[0], "INV0000001")
            self.assertEqual(restaurants.schema["Restaurant_ID"].dataType.simpleString(), "string")
            self.assertEqual(restaurants.select("Restaurant_ID").first()[0], "R001")

    def test_all_fifteen_quality_detectors_flag_their_cases(self) -> None:
        cases = {
            "missing_values": {"Age": None},
            "duplicate_orders": {"Order_ID": 1, "duplicate_of": 1},
            "duplicate_order_lines": {"Order_Item_ID": 1, "duplicate_of": 1},
            "invalid_menu_prices": {"Selling_Price": -1},
            "negative_quantities": {"Quantity": -1},
            "invalid_dates": {"Order_Date": "2026-02-30"},
            "invalid_ratings": {"Rating": 6},
            "missing_customer_ids": {"Customer_ID": None},
            "missing_menu_ids": {"Item_ID": None},
            "invalid_restaurant_ids": {"Location_ID": 999999},
            "impossible_wastage_quantities": {"Preparation_Quantity": 20, "Quantity_Wasted": 1000},
            "incorrect_discounts": {"Discount_Amount": 125, "Gross_Line_Value": 100},
            "cancelled_transactions": {"Order_Status": "Cancelled"},
            "inconsistent_units": {"Expected_Unit": "portion", "Quantity_Unit": "kg"},
            "invalid_location_references": {"Location_ID": 999999},
        }
        self.assertEqual(set(cases), set(ISSUE_TYPES))
        for name, record in cases.items():
            self.assertTrue(detect_fixture_record(name, record, {"1"}), name)

    def test_fixture_report_requires_all_fifteen_named_cases(self) -> None:
        records = [
            ("missing_values", {"Age": None}), ("duplicate_orders", {"Order_ID": 1, "duplicate_of": 1}),
            ("duplicate_order_lines", {"Order_Item_ID": 1, "duplicate_of": 1}),
            ("invalid_menu_prices", {"Selling_Price": -1}), ("negative_quantities", {"Quantity": -1}),
            ("invalid_dates", {"Order_Date": "2026-02-30"}), ("invalid_ratings", {"Rating": 6}),
            ("missing_customer_ids", {"Customer_ID": None}), ("missing_menu_ids", {"Item_ID": None}),
            ("invalid_restaurant_ids", {"Location_ID": 999999}),
            ("impossible_wastage_quantities", {"Preparation_Quantity": 20, "Quantity_Wasted": 1000}),
            ("incorrect_discounts", {"Discount_Amount": 125, "Gross_Line_Value": 100}),
            ("cancelled_transactions", {"Order_Status": "Cancelled"}),
            ("inconsistent_units", {"Expected_Unit": "portion", "Quantity_Unit": "kg"}),
            ("invalid_location_references", {"Location_ID": 999999}),
        ]
        with tempfile.TemporaryDirectory() as temporary:
            folder = Path(temporary) / "data_quality"
            folder.mkdir()
            with (folder / "quality_issue_cases.csv").open("w", newline="", encoding="utf-8") as stream:
                writer = csv.DictWriter(stream, fieldnames=["Issue_Type", "Fixture_Record_JSON"])
                writer.writeheader()
                for issue, record in records:
                    writer.writerow({"Issue_Type": issue, "Fixture_Record_JSON": json.dumps(record)})
            result = fixture_coverage(Path(temporary), {"1"})
        self.assertTrue(result["passed"])
        self.assertEqual(result["fixture_types_detected"], 15)

    def test_clean_views_follow_rules_and_leave_source_frames_unchanged(self) -> None:
        tables = {name: self.spark.createDataFrame([(1,)], ["value"]) for name in TABLE_SCHEMAS}
        tables["orders"] = self.spark.createDataFrame([
            (1, "Completed", False, False), (2, "Cancelled", False, False),
            (3, "Completed", True, False), (4, "Completed", False, True),
        ], ["Order_ID", "Order_Status", "Is_Anomaly", "Is_Duplicate"])
        tables["order_items"] = self.spark.createDataFrame([
            (11, 1, 10, 1, False, False), (12, 2, 10, 1, False, False),
            (13, 3, 10, 1, False, False), (14, 1, 10, 0, False, False),
        ], ["Order_Item_ID", "Order_ID", "Item_ID", "Quantity", "Is_Anomaly", "Is_Duplicate"])
        tables["inventory"] = self.spark.createDataFrame([(1, False), (2, True)], ["Inventory_ID", "Is_Anomaly"])
        tables["wastage"] = self.spark.createDataFrame([(1, 1.0, False), (2, -1.0, False)], ["Wastage_ID", "Quantity_Wasted", "Is_Anomaly"])
        tables["ratings"] = self.spark.createDataFrame([(1, 5, False, False), (2, 6, False, False)], ["Rating_ID", "Rating", "Is_Anomaly", "Is_Duplicate"])
        before = tables["order_items"].count()
        cleaned = clean_tables(tables)
        self.assertEqual(cleaned["order_items"].count(), 1)
        self.assertEqual(cleaned["orders"].count(), 2)
        self.assertEqual(cleaned["inventory"].count(), 1)
        self.assertEqual(cleaned["wastage"].count(), 1)
        self.assertEqual(cleaned["ratings"].count(), 1)
        self.assertEqual(tables["order_items"].count(), before)

    def test_ratings_for_quarantined_orders_are_excluded(self) -> None:
        tables = {name: self.spark.createDataFrame([(1,)], ["value"]) for name in TABLE_SCHEMAS}
        tables["customers"] = self.spark.createDataFrame([("C1",)], ["Customer_ID"])
        tables["restaurants"] = self.spark.createDataFrame([("R1",)], ["Restaurant_ID"])
        tables["promotions"] = self.spark.createDataFrame(
            [("P1", date(2025, 1, 1), date(2025, 12, 31))],
            ["Promotion_ID", "Start_Date", "End_Date"],
        )
        tables["menu_items"] = self.spark.createDataFrame(
            [("I1", 5.0)], ["Item_ID", "Base_Cost"]
        )
        tables["orders"] = self.spark.createDataFrame([
            ("O1", date(2025, 1, 1), "C1", "R1", "P1", "Dine-in", "Cash", 0.0, "Completed"),
            ("O2", date(2025, 1, 1), "MISSING", "R1", "P1", "Dine-in", "Cash", 0.0, "Completed"),
        ], ["Order_ID", "Order_Date", "Customer_ID", "Restaurant_ID", "Promotion_ID", "Channel",
            "Payment_Method", "Delivery_Charge", "Status"])
        tables["order_items"] = self.spark.createDataFrame(
            [("L1", "O1", "I1", 1, 10.0, 0.0, 10.0, 10.0)],
            ["Order_Item_ID", "Order_ID", "Item_ID", "Quantity", "Unit_Price", "Discount_Percent",
             "Line_Subtotal", "Line_Total"],
        )
        tables["ratings"] = self.spark.createDataFrame([
            ("RT1", "O1", "C1", "I1", "R1", 5),
            ("RT2", "O2", "C1", "I1", "R1", 4),
        ], ["Rating_ID", "Order_ID", "Customer_ID", "Item_ID", "Restaurant_ID", "Rating_Value"])
        tables["wastage"] = self.spark.createDataFrame(
            [("I1", "kg", 0.0, 1.0)],
            ["Item_ID", "Unit", "Wastage_Quantity", "Prepared_Quantity"],
        )
        tables["inventory"] = self.spark.createDataFrame(
            [("kg", 1.0, "R1", "I1", 0.0)],
            ["Unit", "Consumed_Stock", "Restaurant_ID", "Item_ID", "Received_Stock"],
        )

        cleaned = clean_tables(tables)

        self.assertEqual(
            [row.Rating_ID for row in cleaned["ratings"].select("Rating_ID").collect()],
            ["RT1"],
        )

    def test_ten_named_joins_execute_independently(self) -> None:
        tables = {
            "orders": self.spark.createDataFrame([("ORD1", "CUS1", "R001", "PROMO1")], ["Order_ID", "Customer_ID", "Restaurant_ID", "Promotion_ID"]),
            "customers": self.spark.createDataFrame([("CUS1", "Customer")], ["Customer_ID", "First_Name"]),
            "order_items": self.spark.createDataFrame([("ORD1", "M1")], ["Order_ID", "Item_ID"]),
            "menu_items": self.spark.createDataFrame([("M1", "C1")], ["Item_ID", "Category_ID"]),
            "menu_categories": self.spark.createDataFrame([("C1", "Category")], ["Category_ID", "Category_Name"]),
            "restaurants": self.spark.createDataFrame([("R001", "Branch")], ["Restaurant_ID", "Restaurant_Name"]),
            "promotions": self.spark.createDataFrame([("PROMO1", "Promotion")], ["Promotion_ID", "Promotion_Name"]),
            "pricing_history": self.spark.createDataFrame([(40,)], ["Item_ID"]),
            "ratings": self.spark.createDataFrame([(40,)], ["Item_ID"]),
            "inventory": self.spark.createDataFrame([(40,)], ["Item_ID"]),
            "wastage": self.spark.createDataFrame([(40,)], ["Item_ID"]),
        }
        joined = build_required_joins(tables)
        self.assertEqual(len(joined), 10)
        self.assertTrue(all(frame.count() == 1 for frame in joined.values()))

    def _small_feature_tables(self):
        orders = self.spark.createDataFrame([
            (1, 7, 1, 2, "5", date(2025, 1, 4), datetime(2025, 1, 4, 10), "10:00:00", 100.0, "Completed"),
            (2, 7, 1, 1, "", date(2025, 1, 5), datetime(2025, 1, 5, 10), "10:00:00", 200.0, "Completed"),
        ], ["Order_ID", "Customer_ID", "Location_ID", "Channel_ID", "Promotion_ID", "Order_Date", "Order_DateTime", "Order_Time", "Total_Amount", "Order_Status"])
        lines = self.spark.createDataFrame([
            (1, 1, 10, 1, 10.0, 0.0, 10.0, 4.0), (2, 2, 10, 1, 10.0, 0.0, 10.0, 4.0),
        ], ["Order_Item_ID", "Order_ID", "Item_ID", "Quantity", "Unit_Price", "Discount_Amount", "Line_Total", "Gross_Profit"])
        ratings = self.spark.createDataFrame([(10, 5, date(2025, 1, 4)), (10, 3, date(2025, 1, 5))], ["Item_ID", "Rating", "Review_Date"])
        wastage = self.spark.createDataFrame([(10, 1.0, 10.0)], ["Item_ID", "Quantity_Wasted", "Preparation_Quantity"])
        prices = self.spark.createDataFrame([(10, 5.0)], ["Item_ID", "Price_Change_Percentage"])
        return {"orders": orders, "order_items": lines, "ratings": ratings, "wastage": wastage, "pricing_history": prices}

    def test_twenty_two_srs_features_have_expected_values(self) -> None:
        outputs = build_feature_tables(self._small_feature_tables())
        self.assertEqual(len(SRS_FEATURES), 22)
        row = outputs["menu_features"].where("Item_ID = 10").first().asDict()
        self.assertEqual(row["item_revenue"], 20.0)
        self.assertEqual(row["cost"], 12.0)
        self.assertEqual(row["contribution_margin"], 8.0)
        self.assertAlmostEqual(row["profit_percentage"], 40.0)
        self.assertEqual(row["order_frequency"], 2)
        self.assertEqual(row["item_popularity"], 2)
        self.assertAlmostEqual(row["repeat_purchase_rate"], 1.0)
        self.assertAlmostEqual(row["average_rating"], 4.0)
        self.assertAlmostEqual(row["rating_trend"], -2.0)
        self.assertAlmostEqual(row["wastage_percentage"], 10.0)
        self.assertAlmostEqual(row["promotion_dependency"], 50.0)
        self.assertEqual(row["peak_hour_frequency"], 2)
        self.assertAlmostEqual(row["weekend_order_ratio"], 1.0)
        self.assertAlmostEqual(row["basket_size"], 1.0)
        self.assertAlmostEqual(row["price_change_percentage"], 5.0)
        customer = outputs["customer_features"].where("Customer_ID = 7").first().asDict()
        self.assertEqual(customer["customer_recency"], 0)
        self.assertEqual(customer["customer_frequency"], 2)
        self.assertEqual(customer["customer_monetary_value"], 300.0)
        self.assertEqual(customer["average_order_value"], 150.0)
        self.assertEqual(customer["channel_preference"], 1)

    def test_thirteen_eda_outputs_exist_and_execute(self) -> None:
        source = self._small_feature_tables()
        cleaned = source | {
            "menu_items": self.spark.createDataFrame([(10, "Dish", 2)], ["Item_ID", "Item_Name", "Category_ID"]),
            "menu_categories": self.spark.createDataFrame([(2, "Main")], ["Category_ID", "Category_Name"]),
        }
        outputs = build_eda_outputs(source, cleaned)
        self.assertEqual(set(outputs), set(EDA_OUTPUTS))
        self.assertTrue(all(frame.count() >= 0 for frame in outputs.values()))
        self.assertEqual(outputs["top_selling_dishes"].first()["Item_ID"], 10)


if __name__ == "__main__":
    unittest.main()
