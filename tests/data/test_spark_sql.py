from __future__ import annotations

import unittest
from datetime import date
from unittest.mock import patch

from pyspark.sql import SparkSession

from dineiq.dataset.integrate import build_sql_aggregates


class SparkSQLTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = (
            SparkSession.builder.master("local[1]")
            .appName("DineIQ-Spark-SQL-Tests")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "1")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()

    def test_required_business_aggregates_execute_through_spark_sql(self) -> None:
        order_lines = self.spark.createDataFrame(
            [
                ("L1", "Dine-in", "O1", date(2025, 1, 6), 100.0, 2),
                ("L1", "Dine-in", "O1", date(2025, 1, 6), 50.0, 1),
                ("L2", "Delivery", "O2", date(2025, 1, 7), 80.0, 4),
            ],
            [
                "Location_ID",
                "Channel",
                "Order_ID",
                "Order_Date",
                "order_items__Line_Total",
                "order_items__Quantity",
            ],
        )

        with patch.object(self.spark, "sql", wraps=self.spark.sql) as sql_call:
            outputs = build_sql_aggregates(
                self.spark,
                order_lines,
                view_name="test_order_line_join",
            )

        self.assertGreaterEqual(sql_call.call_count, 3)

        self.assertEqual(set(outputs), {"location", "channel", "weekday"})

        location = {
            row["Location_ID"]: (
                row["revenue"],
                row["order_count"],
                row["units_sold"],
            )
            for row in outputs["location"].collect()
        }
        self.assertEqual(location, {"L1": (150.0, 1, 3), "L2": (80.0, 1, 4)})

        channel = {
            row["Channel"]: (row["revenue"], row["order_count"])
            for row in outputs["channel"].collect()
        }
        self.assertEqual(channel, {"Dine-in": (150.0, 1), "Delivery": (80.0, 1)})

        weekday = {
            row["weekday"]: (row["revenue"], row["order_count"])
            for row in outputs["weekday"].collect()
        }
        self.assertEqual(weekday, {"Monday": (150.0, 1), "Tuesday": (80.0, 1)})


if __name__ == "__main__":
    unittest.main()
