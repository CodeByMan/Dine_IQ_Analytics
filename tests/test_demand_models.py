"""Focused acceptance tests for the independent demand models model pipelines."""

from __future__ import annotations

import csv
import tempfile
import unittest
from datetime import date, timedelta
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.types import DateType, DoubleType, IntegerType, StringType, StructField, StructType

from dineiq.analytics.python_demand_model import (
    HASH_FEATURES, _CausalHistory, _encode, train_python_demand_model,
)
from sklearn.feature_extraction import FeatureHasher
from dineiq.analytics.spark_models import train_spark_models
from dineiq.dataset.demand_models_pipeline import _compare_predictions


class DemandModelsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = (SparkSession.builder.master("local[1]").appName("DineIQ-DemandModels-Tests")
                     .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1").getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()

    def test_three_spark_models_train_select_on_validation_and_save(self) -> None:
        origin = date(2025, 1, 1)
        rows = [(origin + timedelta(days=i), item, "L1", 1, float(4 + (i % 7) + (item == "I2") * 2))
                for i in range(63) for item in ("I1", "I2")]
        names = ["Date", "Item_ID", "Location_ID", "Is_Available", "Target_Quantity"]
        train = self.spark.createDataFrame(rows[:70], names)
        validation = self.spark.createDataFrame(rows[70:98], names)
        test = self.spark.createDataFrame(rows[98:], names)
        with tempfile.TemporaryDirectory() as directory:
            result = train_spark_models(train, validation, test, Path(directory) / "models")
            self.assertEqual(len(result["models_trained"]), 3)
            self.assertIn(result["selected_model"], result["models_trained"])
            self.assertEqual(result["test_rows"], 28)
            self.assertEqual(len(list((Path(directory) / "models").iterdir())), 4)
            self.assertEqual(result["chronological_boundaries"]["train"]["max"] <
                             result["chronological_boundaries"]["validation"]["min"], True)

    def test_python_encoder_accepts_chunk_without_prior_history(self) -> None:
        rows = [{"Date": f"2025-01-{day:02d}", "Item_ID": "I1", "Location_ID": "L1",
                 "Is_Available": 1, "Target_Quantity": float(day)} for day in range(1, 7)]
        import pandas as pd
        matrix, target, indices = _encode(
            pd.DataFrame(rows), FeatureHasher(n_features=HASH_FEATURES, input_type="dict"), _CausalHistory()
        )
        self.assertEqual(matrix.shape, (0, HASH_FEATURES))
        self.assertEqual(len(target), 0)
        self.assertEqual(indices, [])

    def test_python_model_trains_independently_and_persists_test_predictions(self) -> None:
        origin = date(2025, 1, 1)
        rows = [(origin + timedelta(days=i), f"I{i % 3}", f"L{i % 2}", 1, float(1 + i % 7)) for i in range(90)]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            paths = [root / f"{name}.csv" for name in ("train", "validation", "test")]
            with paths[0].open("w", newline="", encoding="utf-8") as stream:
                writer = csv.writer(stream); writer.writerow(["Date", "Item_ID", "Location_ID", "Is_Available", "Target_Quantity"]); writer.writerows(rows[:54])
            with paths[1].open("w", newline="", encoding="utf-8") as stream:
                writer = csv.writer(stream); writer.writerow(["Date", "Item_ID", "Location_ID", "Is_Available", "Target_Quantity"]); writer.writerows(rows[54:72])
            with paths[2].open("w", newline="", encoding="utf-8") as stream:
                writer = csv.writer(stream); writer.writerow(["Date", "Item_ID", "Location_ID", "Is_Available", "Target_Quantity"]); writer.writerows(rows[72:])
            result = train_python_demand_model(paths[0], paths[1], paths[2], root / "model.joblib", root / "test_predictions.csv")
            self.assertEqual(result["selected_model"], "SGDRegressor")
            self.assertEqual(result["test_metrics"]["records"], 18)
            self.assertTrue((root / "model.joblib").is_file())
            with (root / "test_predictions.csv").open(encoding="utf-8") as stream:
                self.assertEqual(sum(1 for _ in csv.DictReader(stream)), 18)

    def test_dual_pipeline_report_keeps_actuals_differences_and_reason(self) -> None:
        schema = StructType([
            StructField("Date", DateType(), False), StructField("Item_ID", StringType(), False),
            StructField("Location_ID", StringType(), False), StructField("Is_Available", DoubleType(), False),
            StructField("Target_Quantity", DoubleType(), False), StructField("spark_prediction", DoubleType(), False),
        ])
        origin = date(2025, 1, 1)
        spark_rows = [(origin + timedelta(days=i), "I1", "L1", 1.0, float(i + 1), float(i + 1)) for i in range(120)]
        spark_predictions = self.spark.createDataFrame(spark_rows, schema)
        baseline_schema = StructType(schema.fields[:4] + [
            StructField("Target_Quantity", DoubleType(), False),
            StructField("seasonal_naive_prediction", DoubleType(), False),
        ])
        baseline_rows = [(origin + timedelta(days=i), "I1", "L1", 1.0, float(i + 1), float(i + 1)) for i in range(120)]
        baseline_predictions = self.spark.createDataFrame(baseline_rows, baseline_schema)
        with tempfile.TemporaryDirectory() as directory:
            python_csv = Path(directory) / "python.csv"
            with python_csv.open("w", newline="", encoding="utf-8") as stream:
                writer = csv.writer(stream)
                writer.writerow(["Date", "Item_ID", "Location_ID", "Is_Available", "Target_Quantity", "python_prediction"])
                for i in range(120):
                    writer.writerow([origin + timedelta(days=i), "I1", "L1", 1, i + 1, i + 1])
            report = _compare_predictions(self.spark, spark_predictions, baseline_predictions, python_csv)
            self.assertEqual(report.count(), 120)
            self.assertEqual(report.where("match_status != 'MATCH'").count(), 0)
            self.assertIsNotNone(report.first()["disagreement_explanation"])
            self.assertIn("actual", " ".join(report.columns).lower())
            self.assertIn("prediction_difference", report.columns)


if __name__ == "__main__":
    unittest.main()
