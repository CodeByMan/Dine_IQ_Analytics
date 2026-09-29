"""demand models independent Spark/Python demand-model and comparison pipeline."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

from dineiq.analytics.python_demand_model import train_python_demand_model
from dineiq.analytics.spark_models import KEY_COLUMNS, train_spark_models
from dineiq.config import Settings
from dineiq.db.operations import record_model_run
from dineiq.db.schema import initialize_database


DEMAND_MODELS_REQUIREMENTS = (
    "FR-EX-42", "FR-EX-43", "FR-EX-44", "FR-EX-45", "FR-EX-46",
    "FR-EX-67", "FR-EX-68", "FR-EX-69", "NFR-EX-04", "DS-EX-08", "DS-EX-09",
)
COMPARISON_SCHEMA = StructType([
    StructField("Date", StringType(), True), StructField("Item_ID", StringType(), True),
    StructField("Location_ID", StringType(), True), StructField("Is_Available", IntegerType(), True),
    StructField("Target_Quantity", DoubleType(), True), StructField("python_prediction", DoubleType(), True),
])
COMPARISON_KEYS = list(KEY_COLUMNS)
MIN_COMPARISON_RECORDS = 100
AGREEMENT_TOLERANCE_FRACTION = 0.10


def _load_split(spark: SparkSession, path: Path) -> DataFrame:
    schema = StructType([
        StructField("Date", StringType(), True), StructField("Item_ID", StringType(), True),
        StructField("Location_ID", StringType(), True), StructField("Is_Available", IntegerType(), True),
        StructField("Target_Quantity", DoubleType(), True),
    ])
    return spark.read.schema(schema).option("header", "true").csv(str(path))


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _require_prior_evidence(settings: Settings) -> None:
    for stage in ("data_foundation", "descriptive_analytics", "demand_planning", "operational_intelligence"):
        report = settings.reports_root / stage / "build_summary.json"
        if not report.is_file() or not _read_json(report).get("passed"):
            raise RuntimeError(f"Verified {stage} build report is required before demand models.")
        trace = settings.project_root / "docs" / f"{stage}_traceability.csv"
        if not trace.is_file():
            raise FileNotFoundError(f"Verified {stage} traceability is missing: {trace}")
        with trace.open("r", newline="", encoding="utf-8-sig") as stream:
            rows = list(csv.DictReader(stream))
        if not rows or any(row.get("Status") != "VERIFIED" for row in rows):
            raise RuntimeError(f"{stage} traceability is not fully VERIFIED.")


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True, allow_nan=False) + "\n", encoding="utf-8")


def _compare_predictions(
    spark: SparkSession, spark_predictions: DataFrame, baseline_predictions: DataFrame, python_csv: Path
) -> DataFrame:
    python_predictions = spark.read.schema(COMPARISON_SCHEMA).option("header", "true").csv(str(python_csv)).withColumn(
        "Date", F.to_date("Date")
    ).withColumn("Is_Available", F.col("Is_Available").cast("double"))
    spark_keys = spark_predictions.groupBy(*COMPARISON_KEYS).count().where(F.col("count") != 1).limit(1).count()
    python_keys = python_predictions.groupBy(*COMPARISON_KEYS).count().where(F.col("count") != 1).limit(1).count()
    if spark_keys or python_keys:
        raise ValueError("Spark/Python test predictions contain duplicate comparison keys")
    left = spark_predictions.select(*COMPARISON_KEYS, "Target_Quantity", "spark_prediction")
    right = python_predictions.select(*COMPARISON_KEYS, F.col("Target_Quantity").alias("python_actual"), "python_prediction")
    joined = left.join(right, COMPARISON_KEYS, "full_outer")
    baseline = baseline_predictions.select(*COMPARISON_KEYS, "seasonal_naive_prediction")
    joined = joined.join(baseline, COMPARISON_KEYS, "left")
    unmatched = joined.where(
        F.col("Target_Quantity").isNull() | F.col("python_actual").isNull()
        | F.col("spark_prediction").isNull() | F.col("python_prediction").isNull()
        | F.col("seasonal_naive_prediction").isNull()
    ).limit(1).count()
    if unmatched:
        raise ValueError("Spark/Python pipelines did not produce predictions for identical test records")
    actual_mismatch = joined.where(F.abs(F.col("Target_Quantity") - F.col("python_actual")) > 1e-8).limit(1).count()
    if actual_mismatch:
        raise ValueError("Spark/Python test records have mismatching actual target values")
    tolerance = F.greatest(F.lit(1.0), F.abs(F.col("Target_Quantity"))) * F.lit(AGREEMENT_TOLERANCE_FRACTION)
    difference = F.col("python_prediction") - F.col("spark_prediction")
    return joined.withColumn("prediction_difference", difference).withColumn(
        "absolute_prediction_difference", F.abs(difference)
    ).withColumn("agreement_tolerance", tolerance).withColumn(
        "match_status", F.when(F.abs(difference) <= tolerance, F.lit("MATCH")).otherwise(F.lit("MISMATCH"))
    ).withColumn(
        "disagreement_explanation",
        F.when(F.abs(difference) <= tolerance, F.lit("Independent predictions are within the documented 10% of actual demand tolerance"))
        .when(difference > 0, F.lit("Python model predicted higher demand than the selected Spark model beyond tolerance"))
        .otherwise(F.lit("Python model predicted lower demand than the selected Spark model beyond tolerance")),
    ).withColumn("spark_model_version", F.lit("selected-spark-mllib-demand_models-v1")).withColumn(
        "python_model_version", F.lit("sklearn-sgdregressor-demand_models-v1")
    ).withColumn("actual_demand", F.col("Target_Quantity")).drop("python_actual")


def build_demand_models(spark: SparkSession, settings: Settings) -> dict[str, Any]:
    """Train/evaluate MLlib and independent Python models and compare unseen rows."""
    _require_prior_evidence(settings)
    split_root = settings.splits_dir / "demand_forecast"
    split_paths = {name: split_root / f"{name}.csv" for name in ("train", "validation", "test")}
    missing = [str(path) for path in split_paths.values() if not path.is_file()]
    if missing:
        raise FileNotFoundError("Required chronological demand split(s) missing: " + ", ".join(missing))
    train, validation, test = (_load_split(spark, split_paths[name]) for name in ("train", "validation", "test"))
    models_root = settings.models_dir / "demand_models"
    artifacts_root = settings.artifacts_root / "demand_models"
    reports_root = settings.reports_root / "demand_models"
    spark_result = train_spark_models(train, validation, test, models_root / "spark")
    python_test_csv = artifacts_root / "python_test_predictions.csv"
    python_result = train_python_demand_model(
        split_paths["train"], split_paths["validation"], split_paths["test"],
        models_root / "python" / "selected_model.joblib", python_test_csv,
    )
    spark_predictions = spark_result["predictions"].persist()
    comparison = _compare_predictions(
        spark, spark_predictions, spark_result["baseline_predictions"], python_test_csv
    ).persist()
    comparison_count = comparison.count()
    if comparison_count < MIN_COMPARISON_RECORDS:
        raise ValueError(f"Dual-pipeline comparison needs at least {MIN_COMPARISON_RECORDS} unseen records; got {comparison_count}")
    output_root = artifacts_root / "dual_pipeline_comparison"
    comparison.write.mode("overwrite").option("compression", "snappy").parquet(str(output_root))
    readback = spark.read.parquet(str(output_root))
    readback_count = readback.count()
    if comparison_count != readback_count:
        raise RuntimeError("Dual-pipeline Parquet read-back row count mismatch")
    agreement = readback.agg(
        F.sum(F.when(F.col("match_status") == "MATCH", 1).otherwise(0)).alias("matches"),
        F.avg("absolute_prediction_difference").alias("mean_absolute_prediction_difference"),
        F.max("absolute_prediction_difference").alias("maximum_absolute_prediction_difference"),
    ).first()
    match_count = int(agreement["matches"] or 0)
    agreement_pct = match_count / comparison_count * 100.0
    baseline_rmse = spark_result["baseline_test_metrics"]["rmse"]
    spark_rmse = spark_result["test_metrics"]["rmse"]
    baseline_improvement_pct = ((baseline_rmse - spark_rmse) / baseline_rmse * 100.0) if baseline_rmse > 0 else None
    python_rmse = python_result["test_metrics"]["rmse"]
    python_baseline_improvement_pct = ((baseline_rmse - python_rmse) / baseline_rmse * 100.0) if baseline_rmse > 0 else None
    baseline_improvement_verified = bool(
        baseline_improvement_pct is not None and baseline_improvement_pct > 0
        and python_baseline_improvement_pct is not None and python_baseline_improvement_pct > 0
    )
    summary = {
        "requirement_ids": list(DEMAND_MODELS_REQUIREMENTS),
        "spark_algorithms_trained": spark_result["models_trained"],
        "spark_selected_model": spark_result["selected_model"],
        "spark_validation_metrics": spark_result["validation_metrics"],
        "spark_test_metrics": spark_result["test_metrics"],
        "python_selected_model": python_result["selected_model"],
        "python_model_version": python_result["model_version"],
        "python_best_epoch": python_result["best_epoch"],
        "python_validation_metrics": python_result["validation_metrics"],
        "python_test_metrics": python_result["test_metrics"],
        "chronological_boundaries": spark_result["chronological_boundaries"],
        "same_underlying_test_records": True,
        "comparison_records": comparison_count, "comparison_parquet_readback_records": readback_count,
        "agreement_tolerance": "absolute Spark/Python prediction difference <= 10% of max(1, absolute actual demand)",
        "agreement_count": match_count, "disagreement_count": comparison_count - match_count,
        "agreement_percentage": agreement_pct,
        "mean_absolute_prediction_difference": float(agreement["mean_absolute_prediction_difference"] or 0.0),
        "maximum_absolute_prediction_difference": float(agreement["maximum_absolute_prediction_difference"] or 0.0),
        "seasonal_naive_test_metrics": spark_result["baseline_test_metrics"],
        "spark_rmse_improvement_over_seasonal_naive_pct": baseline_improvement_pct,
        "python_rmse_improvement_over_seasonal_naive_pct": python_baseline_improvement_pct,
        "nfr_ex_04_forecast_baseline_improvement_verified": baseline_improvement_verified,
        "comparison_artifact": str(output_root),
        "spark_models_dir": str(models_root / "spark"),
        "python_model_file": str(models_root / "python" / "selected_model.joblib"),
        "source_data_modified": False,
        "passed": bool(len(spark_result["models_trained"]) >= 3 and comparison_count == readback_count
                        and comparison_count >= MIN_COMPARISON_RECORDS
                        and spark_result["selected_model"] in spark_result["models_trained"]
                        and baseline_improvement_verified),
    }
    _write_json(reports_root / "build_summary.json", summary)
    initialize_database(settings.database_path)
    record_model_run(
        str(settings.database_path), pipeline="Spark MLlib",
        model_name=str(summary["spark_selected_model"]),
        model_version="selected-spark-mllib-demand_models-v1",
        artifact_path=str(models_root / "spark"),
        metrics=summary["spark_test_metrics"], record_count=comparison_count,
    )
    record_model_run(
        str(settings.database_path), pipeline="Python scikit-learn",
        model_name=str(summary["python_selected_model"]),
        model_version=str(summary["python_model_version"]),
        artifact_path=str(models_root / "python" / "selected_model.joblib"),
        metrics=summary["python_test_metrics"], record_count=comparison_count,
    )
    comparison.unpersist()
    spark_predictions.unpersist()
    return summary
