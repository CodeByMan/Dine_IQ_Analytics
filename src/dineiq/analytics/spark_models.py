"""Resource-bounded Spark MLlib demand models and chronological evaluation."""

from __future__ import annotations

import math
from pathlib import Path
from typing import Any

from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml.regression import DecisionTreeRegressor, LinearRegression, RandomForestRegressor
from pyspark.sql import DataFrame, Window, functions as F


MODEL_SPECS = (
    ("LinearRegression", lambda: LinearRegression(featuresCol="features", labelCol="Target_Quantity", predictionCol="prediction", maxIter=30, regParam=0.05)),
    ("DecisionTreeRegressor", lambda: DecisionTreeRegressor(featuresCol="features", labelCol="Target_Quantity", predictionCol="prediction", maxDepth=8, minInstancesPerNode=20)),
    ("RandomForestRegressor", lambda: RandomForestRegressor(featuresCol="features", labelCol="Target_Quantity", predictionCol="prediction", numTrees=24, maxDepth=8, maxBins=64, seed=42)),
)

KEY_COLUMNS = ("Date", "Item_ID", "Location_ID", "Is_Available")


def normalize_split(frame: DataFrame, name: str) -> DataFrame:
    required = set(KEY_COLUMNS) | {"Target_Quantity"}
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"{name} demand split missing columns: {missing}")
    result = frame.select(
        F.to_date("Date").alias("Date"), F.col("Item_ID").cast("string").alias("Item_ID"),
        F.col("Location_ID").cast("string").alias("Location_ID"),
        F.col("Is_Available").cast("double").alias("Is_Available"),
        F.col("Target_Quantity").cast("double").alias("Target_Quantity"),
    )
    invalid = result.where(
        F.col("Date").isNull() | F.col("Item_ID").isNull() | F.col("Location_ID").isNull()
        | F.col("Is_Available").isNull() | F.col("Target_Quantity").isNull()
        | (F.col("Target_Quantity") < 0)
    ).limit(1).count()
    if invalid:
        raise ValueError(f"{name} demand split contains invalid dates, features, or negative targets")
    duplicate = result.groupBy(*KEY_COLUMNS).count().where(F.col("count") > 1).limit(1).count()
    if duplicate:
        raise ValueError(f"{name} demand split contains duplicate item/location/date records")
    return result.withColumn("split", F.lit(name))


def _with_features(frame: DataFrame) -> DataFrame:
    return frame.withColumn("day_of_week", F.dayofweek("Date").cast("double")).withColumn(
        "month", F.month("Date").cast("double")
    ).withColumn("year", F.year("Date").cast("double")).withColumn(
        "day_sin", F.sin(F.lit(2.0 * math.pi / 365.25) * F.dayofyear("Date"))
    ).withColumn("day_cos", F.cos(F.lit(2.0 * math.pi / 365.25) * F.dayofyear("Date")))


def _metric_record(frame: DataFrame, model_name: str, split: str) -> dict[str, Any]:
    prediction = F.greatest(F.lit(0.0), F.col("prediction").cast("double"))
    actual = F.col("Target_Quantity").cast("double")
    stats = frame.select(actual.alias("actual"), prediction.alias("prediction")).agg(
        F.count(F.lit(1)).alias("records"), F.avg(F.abs(F.col("actual") - F.col("prediction"))).alias("mae"),
        F.sqrt(F.avg(F.pow(F.col("actual") - F.col("prediction"), 2))).alias("rmse"),
        F.avg(F.when(F.col("actual") != 0, F.abs(F.col("actual") - F.col("prediction")) / F.abs("actual") * 100)).alias("mape_nonzero_actuals"),
        F.sum(F.pow(F.col("actual") - F.col("prediction"), 2)).alias("sse"),
        F.var_pop("actual").alias("variance"),
    ).first()
    records = int(stats["records"])
    if records == 0:
        raise ValueError(f"{split} split produced no evaluation records")
    variance_sum = float(stats["variance"] or 0.0) * records
    return {
        "model": model_name, "split": split, "records": records,
        "mae": float(stats["mae"]), "rmse": float(stats["rmse"]),
        "mape_nonzero_actuals": float(stats["mape_nonzero_actuals"]) if stats["mape_nonzero_actuals"] is not None else None,
        "r2": 1.0 - float(stats["sse"] or 0.0) / variance_sum if variance_sum > 0 else None,
    }


def _seasonal_naive(train: DataFrame, validation: DataFrame, test: DataFrame) -> DataFrame:
    history = train.unionByName(validation).unionByName(test)
    order = Window.partitionBy("Item_ID", "Location_ID").orderBy("Date")
    return history.withColumn("seasonal_naive_prediction", F.lag("Target_Quantity", 7).over(order)).where(
        F.col("split") == "test"
    ).select(*KEY_COLUMNS, "Target_Quantity", "seasonal_naive_prediction")


def train_spark_models(
    train: DataFrame, validation: DataFrame, test: DataFrame, model_root: Path
) -> dict[str, Any]:
    """Fit 3 MLlib regressors, select on validation RMSE, and score unseen test rows."""
    tr, va, te = (normalize_split(train, "train"), normalize_split(validation, "validation"), normalize_split(test, "test"))
    bounds = [f.agg(F.min("Date").alias("min"), F.max("Date").alias("max")).first() for f in (tr, va, te)]
    if not (bounds[0]["max"] < bounds[1]["min"] and bounds[1]["max"] < bounds[2]["min"]):
        raise ValueError("model splits must be strictly chronological and non-overlapping")
    # Construct strictly past-only lags across the chronological splits. Each
    # validation/test row can use only previously observed demand, never its own
    # target or a later row's value.
    history = tr.unionByName(va).unionByName(te)
    series_order = Window.partitionBy("Item_ID", "Location_ID").orderBy("Date")
    prior_week = series_order.rowsBetween(-7, -1)
    history = history.withColumn("lag_1", F.lag("Target_Quantity", 1).over(series_order)).withColumn(
        "lag_7", F.lag("Target_Quantity", 7).over(series_order)
    ).withColumn("trailing_7_mean", F.avg("Target_Quantity").over(prior_week))
    prepared = _with_features(history).where(F.col("lag_7").isNotNull()).persist()
    tr = prepared.where(F.col("split") == "train")
    va = prepared.where(F.col("split") == "validation")
    te = prepared.where(F.col("split") == "test")
    model_root.mkdir(parents=True, exist_ok=True)
    validation_metrics: list[dict[str, Any]] = []
    fitted = {}
    try:
        for name, make_estimator in MODEL_SPECS:
            item_index = StringIndexer(inputCol="Item_ID", outputCol="item_index", handleInvalid="keep")
            location_index = StringIndexer(inputCol="Location_ID", outputCol="location_index", handleInvalid="keep")
            encoder = OneHotEncoder(inputCols=["item_index", "location_index"], outputCols=["item_vector", "location_vector"], handleInvalid="keep")
            assembler = VectorAssembler(inputCols=["item_vector", "location_vector", "Is_Available", "day_of_week", "month", "year", "day_sin", "day_cos", "lag_1", "lag_7", "trailing_7_mean"], outputCol="features", handleInvalid="keep")
            pipeline = Pipeline(stages=[item_index, location_index, encoder, assembler, make_estimator()])
            model = pipeline.fit(tr)
            predicted_validation = model.transform(va)
            metrics = _metric_record(predicted_validation, name, "validation")
            validation_metrics.append(metrics)
            fitted[name] = model
            model.write().overwrite().save(str(model_root / name))
        selected = min(validation_metrics, key=lambda row: (row["rmse"], row["mae"], row["model"]))["model"]
        selected_model = fitted[selected]
        test_predictions = selected_model.transform(te).select(
            *KEY_COLUMNS, F.col("Target_Quantity"), F.greatest(F.lit(0.0), F.col("prediction")).alias("spark_prediction")
        )
        test_metrics = _metric_record(test_predictions.select(
            F.col("Target_Quantity"), F.col("spark_prediction").alias("prediction")
        ), selected, "test")
        baseline = _seasonal_naive(tr, va, te)
        baseline_with_predictions = baseline.where(F.col("seasonal_naive_prediction").isNotNull()).select(
            *KEY_COLUMNS, "Target_Quantity", "seasonal_naive_prediction"
        )
        baseline_metrics = _metric_record(baseline_with_predictions.select(
            F.col("Target_Quantity"), F.col("seasonal_naive_prediction").alias("prediction")
        ), "SeasonalNaiveLag7", "test")
        selected_model.write().overwrite().save(str(model_root / "selected_model"))
        return {
            "selected_model": selected, "models_trained": [name for name, _ in MODEL_SPECS],
            "validation_metrics": validation_metrics, "test_metrics": test_metrics,
            "baseline_test_metrics": baseline_metrics, "predictions": test_predictions,
            "baseline_predictions": baseline_with_predictions,
            "chronological_boundaries": {name: {"min": str(row["min"]), "max": str(row["max"])}
                                         for name, row in zip(("train", "validation", "test"), bounds)},
            "test_rows": int(test_predictions.count()),
        }
    finally:
        prepared.unpersist()
